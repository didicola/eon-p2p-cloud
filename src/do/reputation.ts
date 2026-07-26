import { DurableObject } from "cloudflare:workers";
import { type Env } from "../types";

// ---------------------------------------------------------------------------
// Internal types for the sliding-window result store
// ---------------------------------------------------------------------------

interface RepResult {
  taskId: string;
  success: boolean;
  latencyMs: number;
  score: number;
  timestamp: number;
}

export interface PeerReputation {
  reliability: number;
  avgLatency: number;
  samples: number;
  totalScore: number;
}

const WINDOW_SIZE = 1000;
const STORAGE_PREFIX = "results:";

/**
 * ReputationDO — sliding-window peer reputation tracker.
 *
 * Keeps the last 1000 results per peer.  Results beyond the window are
 * silently dropped.  Every write is durably persisted through ctx.storage.
 *
 * Penalty logic:
 *   - A failed task reduces reliability proportionally.
 *   - Latency is a simple moving average over the window.
 */
export class ReputationDO extends DurableObject<Env> {
  /** In-memory cache of the sliding window per peer.  Populated lazily. */
  private results = new Map<string, RepResult[]>();

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
  }

  // -----------------------------------------------------------------------
  // Public API
  // -----------------------------------------------------------------------

  /**
   * Record a completed (or failed) task result for a peer.
   *
   * @param peerId   The reporting peer
   * @param taskId   The task that was executed
   * @param success  true if the task completed without error
   * @param latencyMs  Wall-clock duration of the inference
   * @param score    Quality score (0..1) from an optional verification step
   */
  async recordResult(
    peerId: string,
    taskId: string,
    success: boolean,
    latencyMs: number,
    score: number,
  ): Promise<void> {
    const entry: RepResult = {
      taskId,
      success,
      latencyMs,
      score,
      timestamp: Date.now(),
    };

    let entries = this.results.get(peerId);
    if (!entries) {
      entries = await this.loadFromStorage(peerId);
    }

    entries.push(entry);

    // Trim to sliding window
    if (entries.length > WINDOW_SIZE) {
      entries = entries.slice(entries.length - WINDOW_SIZE);
    }

    this.results.set(peerId, entries);
    await this.ctx.storage.put<RepResult[]>(
      `${STORAGE_PREFIX}${peerId}`,
      entries,
    );
  }

  /**
   * Get the current reputation snapshot for a peer.
   *
   * Returns a default (reliability=1.0, samples=0) for unknown peers so
   * callers never need to null-check.
   */
  async getReputation(peerId: string): Promise<PeerReputation> {
    let entries = this.results.get(peerId);
    if (!entries) {
      entries = await this.loadFromStorage(peerId);
    }

    if (entries.length === 0) {
      return { reliability: 1.0, avgLatency: 0, samples: 0, totalScore: 0 };
    }

    const successes = entries.filter((e) => e.success).length;
    const totalLatency = entries.reduce((sum, e) => sum + e.latencyMs, 0);
    const totalScore = entries.reduce((sum, e) => sum + e.score, 0);

    // Exponentially decay reliability for failed tasks: each failure reduces
    // the ratio faster than a simple average would suggest.
    const failures = entries.length - successes;
    const penalty = failures > 0 ? Math.pow(0.85, failures) : 1.0;
    const reliability = Math.min(1.0, (successes / entries.length) * penalty);

    return {
      reliability,
      avgLatency: totalLatency / entries.length,
      samples: entries.length,
      totalScore,
    };
  }

  /**
   * Get reputations for every peer that has recorded results.
   */
  async getAllReputations(): Promise<Record<string, PeerReputation>> {
    const result: Record<string, PeerReputation> = {};
    const stored = await this.ctx.storage.list<RepResult[]>({
      prefix: STORAGE_PREFIX,
    });
    for (const [key, entries] of stored) {
      const peerId = key.slice(STORAGE_PREFIX.length);
      const successes = entries.filter((e) => e.success).length;
      const failures = entries.length - successes;
      const totalLatency = entries.reduce((sum, e) => sum + e.latencyMs, 0);
      const totalScore = entries.reduce((sum, e) => sum + e.score, 0);
      const penalty = failures > 0 ? Math.pow(0.85, failures) : 1.0;
      const reliability = Math.min(
        1.0,
        (successes / entries.length) * penalty,
      );
      result[peerId] = {
        reliability,
        avgLatency: entries.length > 0 ? totalLatency / entries.length : 0,
        samples: entries.length,
        totalScore,
      };
    }
    return result;
  }

  /**
   * Penalize a peer for an unreliable result — shortcut to recordResult
   * with success=false, latencyMs=0, score=0.
   */
  async penalizePeer(peerId: string, taskId: string): Promise<void> {
    await this.recordResult(peerId, taskId, false, 0, 0);
  }

  // -----------------------------------------------------------------------
  // Internal helpers
  // -----------------------------------------------------------------------

  private async loadFromStorage(peerId: string): Promise<RepResult[]> {
    const stored = await this.ctx.storage.get<RepResult[]>(
      `${STORAGE_PREFIX}${peerId}`,
    );
    const entries = stored ?? [];
    this.results.set(peerId, entries);
    return entries;
  }

  // -----------------------------------------------------------------------
  // Fetch handler — inter-DO HTTP routing
  // -----------------------------------------------------------------------

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    try {
      if (path === "/record" && request.method === "POST") {
        const body = (await request.json()) as {
          peerId: string;
          taskId: string;
          success: boolean;
          latencyMs: number;
          score: number;
        };
        await this.recordResult(
          body.peerId,
          body.taskId,
          body.success,
          body.latencyMs,
          body.score,
        );
        return new Response(JSON.stringify({ ok: true }), {
          headers: { "Content-Type": "application/json" },
        });
      }

      if (path.startsWith("/reputation/")) {
        const peerId = path.split("/").pop()!;
        const rep = await this.getReputation(peerId);
        return new Response(JSON.stringify({ peerId, ...rep }), {
          headers: { "Content-Type": "application/json" },
        });
      }

      if (path === "/reputations") {
        const reps = await this.getAllReputations();
        return new Response(JSON.stringify({ reputations: reps }), {
          headers: { "Content-Type": "application/json" },
        });
      }

      return new Response("ReputationDO — /record, /reputation/:peerId, /reputations", {
        status: 200,
      });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      console.error("ReputationDO error:", msg);
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
