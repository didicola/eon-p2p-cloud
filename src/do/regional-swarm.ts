import { DurableObject } from "cloudflare:workers";
import { VaultStorageDO } from "./vault-storage";
import { type Env, type PeerCapability, type RegionMetrics, type Task } from "../types";

/**
 * RegionalSwarmDO — per-continent swarm coordinator (Phase 3).
 *
 * One instance per continent-sized region (e.g. "na", "eu", "asia", "sa",
 * "af", "oc").  Maintains a full capability registry for all peers in that
 * region and provides latency-optimised task routing within the region.
 */
export class RegionalSwarmDO extends VaultStorageDO<Env> {
  /** In-memory cache — reloaded from storage on first access. */
  private peers = new Map<string, PeerCapability>();
  private taskQueue: Task[] = [];
  private regionName: string;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    const id = ctx.id.toString();
    this.regionName = id.includes("region-")
      ? id.split("region-")[1]
      : "unknown";
  }

  // -----------------------------------------------------------------------
  // Peer management
  // -----------------------------------------------------------------------

  /**
   * Register or update a peer's capabilities in this region.
   */
  async registerPeer(cap: PeerCapability): Promise<void> {
    cap.lastHeartbeat = Date.now();
    cap.region = this.regionName;
    this.peers.set(cap.peerId, cap);
    await this.encPut(peerKey(cap.peerId), cap);
  }

  /**
   * Remove a peer from the region.
   */
  async unregisterPeer(peerId: string): Promise<void> {
    this.peers.delete(peerId);
    await this.ctx.storage.delete(peerKey(peerId));
  }

  /**
   * Get all active peers in this region (with stale eviction).
   */
  async getActivePeers(): Promise<PeerCapability[]> {
    await this.rehydrateIfEmpty();
    this.evictStale();
    return Array.from(this.peers.values());
  }

  // -----------------------------------------------------------------------
  // Task routing with latency optimisation
  // -----------------------------------------------------------------------

  /**
   * Find the best peer for a model within this region using latency as the
   * primary sort key.
   */
  async findBestPeerInRegion(
    model: string,
  ): Promise<PeerCapability | null> {
    await this.rehydrateIfEmpty();
    this.evictStale();

    const candidates = Array.from(this.peers.values())
      .filter(
        (p) =>
          p.models.includes(model) &&
          p.currentLoad < p.maxLoad &&
          p.reliability > 0,
      )
      .sort(
        (a, b) =>
          a.avgLatency - b.avgLatency ||
          b.reliability - a.reliability ||
          a.currentLoad / a.maxLoad - b.currentLoad / b.maxLoad,
      );

    return candidates[0] ?? null;
  }

  /**
   * Enqueue a task for processing within the region.  Returns the task id.
   */
  async enqueueTask(
    model: string,
    prompt: string,
    messages?: { role: string; content: string }[],
  ): Promise<string> {
    const task: Task = {
      id: crypto.randomUUID(),
      model,
      prompt,
      messages,
      created: Date.now(),
      done: false,
      attempts: 0,
      routingHint: this.regionName,
    };

    this.taskQueue.push(task);
    await this.encPut(`regtask:${task.id}`, task);
    return task.id;
  }

  /**
   * Claim a pending task.  Tasks are returned in FIFO order.
   */
  async claimTask(peerId: string): Promise<Task | null> {
    if (this.taskQueue.length === 0) {
      // Rehydrate from storage
      const stored = await this.encList<Task>({
        prefix: "regtask:",
      });
      for (const [, t] of stored) {
        if (!t.claimed && !t.done) {
          this.taskQueue.push(t);
        }
      }
    }

    const task = this.taskQueue.find((t) => !t.claimed && !t.done);
    if (task) {
      task.claimed = peerId;
      task.attempts++;
      await this.encPut(`regtask:${task.id}`, task);
      return task;
    }
    return null;
  }

  /**
   * Mark a task as completed with the given result.
   */
  async submitResult(taskId: string, result: string): Promise<boolean> {
    const task =
      this.taskQueue.find((t) => t.id === taskId) ??
      (await encGet<Task>(this.ctx.storage, `regtask:${taskId}`));

    if (task) {
      task.result = result;
      task.done = true;
      await this.encPut(`regtask:${task.id}`, task);
      return true;
    }
    return false;
  }

  // -----------------------------------------------------------------------
  // Metrics
  // -----------------------------------------------------------------------

  /**
   * Return aggregated metrics for this region.
   */
  async getMetrics(): Promise<RegionMetrics> {
    await this.rehydrateIfEmpty();
    this.evictStale();

    const peers = Array.from(this.peers.values());
    const pending = this.taskQueue.filter((t) => !t.done);
    const avgLatency =
      peers.length > 0
        ? peers.reduce((s, p) => s + p.avgLatency, 0) / peers.length
        : 0;
    const errorRate =
      peers.length > 0
        ? peers.reduce(
            (s, p) => s + (1 - (p.reliability ?? 1.0)),
            0,
          ) / peers.length
        : 0;

    return {
      region: this.regionName,
      peerCount: peers.length,
      queueDepth: pending.length,
      avgLatency,
      requestsPerMin: 0, // calculated externally if needed
      errorRate,
    };
  }

  // -----------------------------------------------------------------------
  // Internal helpers
  // -----------------------------------------------------------------------

  private async rehydrateIfEmpty(): Promise<void> {
    if (this.peers.size > 0) return;
    const stored = await this.encList<PeerCapability>({
      prefix: "regpeer:",
    });
    for (const [key, cap] of stored) {
      this.peers.set(cap.peerId, cap);
    }
  }

  private evictStale(): void {
    const now = Date.now();
    for (const [id, cap] of this.peers) {
      if (now - cap.lastHeartbeat > 300_000) {
        this.peers.delete(id);
        this.ctx.storage.delete(peerKey(id)).catch(() => {});
      }
    }
  }

  // -----------------------------------------------------------------------
  // Fetch handler
  // -----------------------------------------------------------------------

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    try {
      // Peer management
      if (path === "/register" && request.method === "POST") {
        const body = (await request.json()) as PeerCapability;
        await this.registerPeer(body);
        return new Response(JSON.stringify({ ok: true }), {
          headers: { "Content-Type": "application/json" },
        });
      }

      if (path === "/unregister" && request.method === "POST") {
        const body = (await request.json()) as { peerId: string };
        await this.unregisterPeer(body.peerId);
        return new Response(JSON.stringify({ ok: true }), {
          headers: { "Content-Type": "application/json" },
        });
      }

      if (path === "/peers") {
        const peers = await this.getActivePeers();
        return new Response(
          JSON.stringify({ region: this.regionName, peers }),
          { headers: { "Content-Type": "application/json" } },
        );
      }

      if (path === "/find-peer") {
        const model = url.searchParams.get("model") || "";
        const peer = await this.findBestPeerInRegion(model);
        return new Response(JSON.stringify({ peer }), {
          headers: { "Content-Type": "application/json" },
        });
      }

      // Task routing
      if (path === "/task" && request.method === "POST") {
        const body = (await request.json()) as {
          model: string;
          prompt: string;
          messages?: { role: string; content: string }[];
        };
        const taskId = await this.enqueueTask(
          body.model,
          body.prompt,
          body.messages,
        );
        return new Response(JSON.stringify({ taskId }), {
          headers: { "Content-Type": "application/json" },
        });
      }

      if (path === "/task/claim" && request.method === "POST") {
        const body = (await request.json()) as { peerId: string };
        const task = await this.claimTask(body.peerId);
        return new Response(JSON.stringify({ task }), {
          headers: { "Content-Type": "application/json" },
        });
      }

      if (path.startsWith("/task/") && request.method === "POST") {
        const taskId = path.split("/").pop()!;
        const body = (await request.json()) as { result: string };
        const ok = await this.submitResult(taskId, body.result);
        return new Response(JSON.stringify({ ok }), {
          headers: { "Content-Type": "application/json" },
        });
      }

      // Metrics
      if (path === "/metrics") {
        const metrics = await this.getMetrics();
        return new Response(JSON.stringify(metrics), {
          headers: { "Content-Type": "application/json" },
        });
      }

      return new Response(
        `RegionalSwarmDO [${this.regionName}] — /register, /peers, /find-peer, /task/*, /metrics`,
        { status: 200 },
      );
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      console.error(`RegionalSwarmDO [${this.regionName}] error:`, msg);
      return new Response(
        JSON.stringify({ error: "internal_error", message: msg }),
        {
          status: 500,
          headers: { "Content-Type": "application/json" },
        },
      );
    }
  }
}

// ---------------------------------------------------------------------------
// Storage key helper
// ---------------------------------------------------------------------------

function peerKey(peerId: string): string {
  return `regpeer:${peerId}`;
}
