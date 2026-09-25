import { DurableObject } from "cloudflare:workers";
import { VaultStorageDO } from "./vault-storage";
import { type Env, type PeerCapability } from "../types";

/**
 * EdgeSwarmDO — lightweight per-colo swarm registry (Phase 3).
 *
 * Each Cloudflare colo (point-of-presence) can have its own EdgeSwarmDO
 * instance, maintaining only the peers physically located in that colo.
 *
 * Characteristics:
 *   - Minimal state: a map of peerId -> PeerCapability per colo.
 *   - Crash-recoverable: all mutations are persisted through ctx.storage.
 *   - `getClosestPeer(colo, model)` returns the peer with the lowest latency
 *     inside the requested colo, falling back to any peer in the same colo
 *     if no model match.
 */
export class EdgeSwarmDO extends VaultStorageDO<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
  }

  // -----------------------------------------------------------------------
  // Peer registration
  // -----------------------------------------------------------------------

  /**
   * Register a peer that is physically located in this colo.
   */
  async registerLocalPeer(
    peerId: string,
    colo: string,
    capabilities: Partial<PeerCapability>,
  ): Promise<void> {
    const now = Date.now();
    const peer: PeerCapability = {
      peerId,
      models: capabilities.models ?? [],
      maxTokens: capabilities.maxTokens ?? 4096,
      avgLatency: capabilities.avgLatency ?? 0,
      reliability: capabilities.reliability ?? 1.0,
      currentLoad: capabilities.currentLoad ?? 0,
      maxLoad: capabilities.maxLoad ?? 5,
      region: capabilities.region ?? "unknown",
      colo,
      lastHeartbeat: now,
      version: capabilities.version ?? "1.0",
      protocol: capabilities.protocol ?? "http",
      endpoint: capabilities.endpoint,
    };

    await this.encPut(edgePeerKey(peerId), peer);
  }

  /**
   * Remove a peer from this colo.
   */
  async unregisterPeer(peerId: string): Promise<void> {
    await this.ctx.storage.delete(edgePeerKey(peerId));
  }

  /**
   * Get every peer registered in this colo.
   */
  async getLocalPeers(): Promise<PeerCapability[]> {
    const stored = await this.encList<PeerCapability>({
      prefix: "edgepeer:",
    });
    return Array.from(stored.values());
  }

  /**
   * Find the closest peer in this colo that can serve a model.
   *
   * Strategy:
   *   1. Filter for peers that have the model.
   *   2. Among them, pick the one with the lowest avgLatency.
   *   3. If none match the model, return the lowest-latency peer overall
   *      (the caller can decide whether to use it).
   *
   * @param colo   The colo code (e.g. "LHR", "HKG")
   * @param model  The model family or full model id
   * @returns The best matching PeerCapability, or null.
   */
  async getClosestPeer(
    colo: string,
    model: string,
  ): Promise<PeerCapability | null> {
    const peers = await this.getLocalPeers();

    // First pass: peers that have this model
    const withModel = peers
      .filter(
        (p) =>
          (p.colo === colo || !colo) &&
          p.currentLoad < p.maxLoad &&
          p.models.some(
            (m) => m === model || m.startsWith(model.split("-")[0]),
          ),
      )
      .sort((a, b) => {
        // Prefer same-colo, then lowest latency
        if (a.colo === colo && b.colo !== colo) return -1;
        if (a.colo !== colo && b.colo === colo) return 1;
        return a.avgLatency - b.avgLatency;
      });

    if (withModel.length > 0) return withModel[0];

    // Second pass: any peer in the requested colo (no model filter)
    const inColo = peers
      .filter((p) => p.colo === colo && p.currentLoad < p.maxLoad)
      .sort((a, b) => a.avgLatency - b.avgLatency);

    return inColo[0] ?? null;
  }

  /**
   * Heartbeat update — lightweight ping to keep the entry fresh.
   */
  async heartbeat(
    peerId: string,
    load?: number,
    latency?: number,
  ): Promise<void> {
    const key = edgePeerKey(peerId);
    const peer = await encGet<PeerCapability>(this.ctx.storage, key);
    if (!peer) return;

    peer.lastHeartbeat = Date.now();
    if (load !== undefined) peer.currentLoad = load;
    if (latency !== undefined) peer.avgLatency = latency;

    await this.encPut(key, peer);
  }

  // -----------------------------------------------------------------------
  // Fetch handler
  // -----------------------------------------------------------------------

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    try {
      if (path === "/register" && request.method === "POST") {
        const body = (await request.json()) as {
          peerId: string;
          colo: string;
          capabilities: Partial<PeerCapability>;
        };
        await this.registerLocalPeer(
          body.peerId,
          body.colo,
          body.capabilities,
        );
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
        const peers = await this.getLocalPeers();
        return new Response(JSON.stringify({ peers }), {
          headers: { "Content-Type": "application/json" },
        });
      }

      if (path === "/closest") {
        const colo = url.searchParams.get("colo") || "";
        const model = url.searchParams.get("model") || "";
        const peer = await this.getClosestPeer(colo, model);
        return new Response(JSON.stringify({ peer }), {
          headers: { "Content-Type": "application/json" },
        });
      }

      if (path === "/heartbeat" && request.method === "POST") {
        const body = (await request.json()) as {
          peerId: string;
          load?: number;
          latency?: number;
        };
        await this.heartbeat(body.peerId, body.load, body.latency);
        return new Response("OK");
      }

      return new Response(
        "EdgeSwarmDO — /register, /unregister, /peers, /closest, /heartbeat",
        { status: 200 },
      );
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      console.error("EdgeSwarmDO error:", msg);
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

function edgePeerKey(peerId: string): string {
  return `edgepeer:${peerId}`;
}
