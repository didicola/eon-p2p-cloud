import { WorkflowEntrypoint } from "cloudflare:workers";
import { type Env, type RegionMetrics } from "../types";
import { SWARM_SHARDS, getSwarmDO, getModelFamily } from "../models";
import { broadcastToPeers } from "../peer-protocol";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** The event payload that triggers the workflow. */
interface ScalerEvent {
  /** Optional target peer count override.  If absent the workflow calculates it. */
  targetPeers?: number;
  /** The model shard to evaluate; if omitted all shards are scanned. */
  shard?: string;
}

/** Aggregated metrics snapshot from across the swarm. */
interface SwarmMetrics {
  totalQueueDepth: number;
  totalPeers: number;
  shardMetrics: {
    shard: string;
    queueDepth: number;
    peerCount: number;
    avgLatency: number;
  }[];
  regionMetrics?: Record<string, RegionMetrics>;
}

/** Result of the scaling decision. */
interface ScaleDecision {
  currentPeers: number;
  targetPeers: number;
  deficit: number;
  reason: string;
  shouldRecruit: boolean;
}

// ---------------------------------------------------------------------------
// SwarmScaler — Cloudflare Workflow for autoscaling the P2P swarm
// ---------------------------------------------------------------------------

/**
 * Periodic autoscaling workflow.
 *
 * Runs in four steps:
 *   1. **getSwarmMetrics** — queries every shard DO for queue depth and peer count.
 *   2. **calculateTarget** — determines how many peers are needed (simple heuristic:
 *      if total queue depth > 2x active peers, recruit more).
 *   3. **sendRecruitmentSignal** — if a deficit exists, enqueues a recruitment
 *      notification to the TASK_QUEUE so external listeners can spin up new peers.
 *   4. **logState** — records the metrics and decision for observability.
 *
 * Triggered externally via the Cloudflare Workflows API (not a cron binding).
 */
export class SwarmScaler extends WorkflowEntrypoint<Env, ScalerEvent> {
  /**
   * Main workflow run method.
   *
   * @param event  The trigger payload (can specify a target peer count or shard)
   * @param step   Step controller for durable execution
   * @returns A summary of the scaling action taken.
   */
  async run(
    event: ScalerEvent,
    step: WorkflowStep,
  ): Promise<{
    action: string;
    decision: ScaleDecision;
    metrics: SwarmMetrics;
  }> {
    let metrics: SwarmMetrics;

    // -----------------------------------------------------------------------
    // Step 1 — Gather swarm metrics
    // -----------------------------------------------------------------------
    metrics = await step.do("getSwarmMetrics", async () => {
      return this.collectSwarmMetrics(event.shard);
    });

    // -----------------------------------------------------------------------
    // Step 2 — Calculate target peer count
    // -----------------------------------------------------------------------
    const decision = await step.do("calculateTarget", async () => {
      return this.calculateTarget(metrics, event.targetPeers);
    });

    // -----------------------------------------------------------------------
    // Step 3 — Send recruitment signal (if needed)
    // -----------------------------------------------------------------------
    await step.do("sendRecruitmentSignal", async () => {
      if (decision.shouldRecruit && decision.deficit > 0) {
        await this.signalRecruitment(decision, metrics);
      }
    });

    // -----------------------------------------------------------------------
    // Step 4 — Log state
    // -----------------------------------------------------------------------
    await step.do("logState", async () => {
      console.log(
        JSON.stringify({
          ts: Date.now(),
          workflow: "swarm-scaler",
          metrics: {
            totalQueueDepth: metrics.totalQueueDepth,
            totalPeers: metrics.totalPeers,
          },
          decision,
        }),
      );
    });

    return {
      action: decision.shouldRecruit ? "recruited" : "noop",
      decision,
      metrics,
    };
  }

  // -------------------------------------------------------------------------
  // Step implementations
  // -------------------------------------------------------------------------

  /**
   * Query every model-family shard DO for its current queue depth and peer
   * count.
   */
  private async collectSwarmMetrics(shard?: string): Promise<SwarmMetrics> {
    const shardsToCheck = shard ? [shard] : SWARM_SHARDS;
    const shardMetrics: SwarmMetrics["shardMetrics"] = [];

    for (const s of shardsToCheck) {
      try {
        const stub = getSwarmDO(this.env as unknown as Env, s);

        const [queueResp, peerCount] = await Promise.all([
          stub.fetch("http://internal/queue/depth", { method: "GET" }),
          stub.fetch("http://internal/peer-count", { method: "GET" }),
        ]);

        const queueData = queueResp.ok
          ? ((await queueResp.json()) as { depth: number })
          : { depth: 0 };
        const peerData = peerCount.ok
          ? ((await peerCount.json()) as { count: number })
          : { count: 0 };

        // Determine average latency from capabilities
        const peersResp = await stub.fetch("http://internal/peers", {
          method: "GET",
        });
        let avgLatency = 0;
        if (peersResp.ok) {
          const peersData = (await peersResp.json()) as {
            peers: { avgLatency: number }[];
          };
          const latencies = peersData.peers.map((p) => p.avgLatency);
          avgLatency =
            latencies.length > 0
              ? latencies.reduce((s, v) => s + v, 0) / latencies.length
              : 0;
        }

        shardMetrics.push({
          shard: s,
          queueDepth: queueData.depth,
          peerCount: peerData.count,
          avgLatency,
        });
      } catch (err) {
        console.warn(`swarm-scaler: failed to query shard ${s}:`, err);
        shardMetrics.push({
          shard: s,
          queueDepth: 0,
          peerCount: 0,
          avgLatency: 0,
        });
      }
    }

    const totalQueueDepth = shardMetrics.reduce((s, m) => s + m.queueDepth, 0);
    const totalPeers = shardMetrics.reduce((s, m) => s + m.peerCount, 0);

    return {
      totalQueueDepth,
      totalPeers,
      shardMetrics,
    };
  }

  /**
   * Determine whether the swarm needs more peers.
   *
   * Simple heuristic:
   *   - If total queue depth > totalPeers * 2, we need more peers.
   *   - Target = ceil(queueDepth / 2) — each peer should handle ~2 queued tasks.
   *   - If `overrideTarget` is provided, use that instead.
   */
  private calculateTarget(
    metrics: SwarmMetrics,
    overrideTarget?: number,
  ): ScaleDecision {
    const currentPeers = metrics.totalPeers || 1; // avoid div by zero

    if (overrideTarget !== undefined && overrideTarget >= 0) {
      return {
        currentPeers,
        targetPeers: overrideTarget,
        deficit: Math.max(0, overrideTarget - currentPeers),
        reason: "override",
        shouldRecruit: overrideTarget > currentPeers,
      };
    }

    const targetPeers = Math.max(
      currentPeers,
      Math.ceil(metrics.totalQueueDepth / 2),
    );
    const deficit = targetPeers - currentPeers;

    return {
      currentPeers,
      targetPeers,
      deficit: Math.max(0, deficit),
      reason:
        deficit > 0
          ? `queue_depth (${metrics.totalQueueDepth}) > 2x peers (${currentPeers})`
          : "adequate capacity",
      shouldRecruit: deficit > 0,
    };
  }

  /**
   * Send a recruitment notification via the TASK_QUEUE so external systems
   * (Cloudflare Workers, webhook listeners, etc.) can spin up new peers.
   */
  private async signalRecruitment(
    decision: ScaleDecision,
    metrics: SwarmMetrics,
  ): Promise<void> {
    const env = this.env as unknown as Env;

    const recruitmentMessage = {
      type: "recruitment",
      targetPeers: decision.targetPeers,
      deficit: decision.deficit,
      currentPeers: decision.currentPeers,
      totalQueueDepth: metrics.totalQueueDepth,
      shardBreakdown: metrics.shardMetrics,
      timestamp: Date.now(),
    };

    // 1. Dispatch via the TASK_QUEUE (or a dedicated queue binding if present)
    try {
      await env.TASK_QUEUE.send({
        type: "peer_heartbeat",
        peerId: "swarm-scaler",
        prompt: JSON.stringify(recruitmentMessage),
      });
    } catch (err) {
      console.warn("swarm-scaler: TASK_QUEUE send failed:", err);
    }

    // 2. Broadcast to all connected peers so external listeners see the signal
    for (const s of SWARM_SHARDS) {
      try {
        await broadcastToPeers(env, s, {
          type: "recruitment_signal",
          ...recruitmentMessage,
        });
      } catch {
        // best-effort per shard
      }
    }
  }
}

// ---------------------------------------------------------------------------
// WorkflowStep type (re-declared so the import is clean)
// ---------------------------------------------------------------------------

/**
 * Minimal interface for the `step` parameter provided by the Workflows runtime.
 * The full type is defined in `@cloudflare/workers-types` under `WorkflowStep`.
 */
interface WorkflowStep {
  do: <T>(name: string, handler: () => Promise<T>) => Promise<T>;
  sleep: (name: string, duration: string | number) => Promise<void>;
}
