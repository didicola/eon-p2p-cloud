import { DurableObject } from "cloudflare:workers";
import { VaultStorageDO } from "./vault-storage";
import { type PeerCapability, type Task, type Env } from "../types";
import { SWARM_SHARDS, getModelFamily } from "../models";

/**
 * P2PSwarmDO — sharded peer-to-peer swarm Durable Object.
 *
 * One instance per model-family shard (llama, qwen, mistral, gemma, deepseek, general).
 *
 * Responsibilities:
 *   - Peer capability registry (PeerCapability with model lists, load, latency, reliability)
 *   - Smart peer routing via findBestPeer(priority)
 *   - Priority-aware task queue
 *   - WebSocket-based push delivery for real-time task dispatch
 *   - Heartbeat-based eviction (5 min stale expiry)
 *   - All durable state through ctx.storage
 */
export class P2PSwarmDO extends VaultStorageDO<Env> {
  private capabilities = new Map<string, PeerCapability>();
  private taskQueue: Task[] = [];
  private connections = new Map<string, WebSocket>();
  private shardName: string;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    const id = ctx.id.toString();
    this.shardName = id.includes("eon-swarm-")
      ? id.split("eon-swarm-")[1]
      : "general";
  }

  // ---------------------------------------------------------------------------
  // Peer capability registry
  // ---------------------------------------------------------------------------

  /**
   * Register or update a peer's basic announcement.
   * Legacy compat — prefer registerCapability().
   */
  async announce(
    peerId: string,
    models: string[],
    partial?: Partial<PeerCapability>,
  ): Promise<void> {
    const now = Date.now();
    const existing = this.capabilities.get(peerId);

    const cap: PeerCapability = {
      peerId,
      models,
      maxTokens: partial?.maxTokens ?? existing?.maxTokens ?? 4096,
      avgLatency: partial?.avgLatency ?? existing?.avgLatency ?? 0,
      reliability: partial?.reliability ?? existing?.reliability ?? 1.0,
      currentLoad: partial?.currentLoad ?? existing?.currentLoad ?? 0,
      maxLoad: partial?.maxLoad ?? existing?.maxLoad ?? 5,
      region: partial?.region ?? existing?.region ?? "unknown",
      colo: partial?.colo ?? existing?.colo,
      lastHeartbeat: now,
      version: partial?.version ?? existing?.version ?? "1.0",
      protocol: partial?.protocol ?? existing?.protocol ?? "http",
      endpoint: partial?.endpoint ?? existing?.endpoint,
    };

    this.capabilities.set(peerId, cap);
    await this.persistCapability(cap);
  }

  /**
   * Register a full PeerCapability object.
   */
  async registerCapability(cap: PeerCapability): Promise<void> {
    cap.lastHeartbeat = Date.now();
    this.capabilities.set(cap.peerId, cap);
    await this.persistCapability(cap);
  }

  /**
   * Find the best peer for a given model, ranked by the requested priority axis.
   *
   * @param model   Model id string (e.g. "llama-3.3-70b")
   * @param priority  "latency" — fastest avg response
   *                  "cost"    — lowest utilisation ratio
   *                  "reliability" — highest historical success rate
   *
   * @returns The best matching PeerCapability, or null if none available.
   */
  findBestPeer(
    model: string,
    priority: "latency" | "cost" | "reliability" = "latency",
  ): PeerCapability | null {
    this.evictStalePeers();

    const candidates = Array.from(this.capabilities.values()).filter(
      (p) =>
        p.models.includes(model) &&
        p.currentLoad < p.maxLoad &&
        p.reliability > 0,
    );

    if (candidates.length === 0) return null;

    switch (priority) {
      case "latency":
        candidates.sort((a, b) => a.avgLatency - b.avgLatency);
        break;
      case "cost":
        candidates.sort(
          (a, b) =>
            a.currentLoad / a.maxLoad - b.currentLoad / b.maxLoad,
        );
        break;
      case "reliability":
        candidates.sort((a, b) => b.reliability - a.reliability);
        break;
    }

    return candidates[0];
  }

  // ---------------------------------------------------------------------------
  // Peer inspection & heartbeat eviction
  // ---------------------------------------------------------------------------

  async getPeers(): Promise<PeerCapability[]> {
    this.evictStalePeers();
    return Array.from(this.capabilities.values());
  }

  async getPeerCount(): Promise<number> {
    this.evictStalePeers();
    return this.capabilities.size;
  }

  private evictStalePeers(): void {
    const now = Date.now();
    const stale: string[] = [];
    for (const [id, cap] of this.capabilities) {
      if (now - cap.lastHeartbeat > 300_000) {
        stale.push(id);
        this.capabilities.delete(id);
      }
    }
    if (stale.length > 0) {
      this.ctx.storage.delete(stale.map((id) => `cap:${id}`)).catch(() => {});
    }
  }

  private async persistCapability(cap: PeerCapability): Promise<void> {
    await this.encPut(`cap:${cap.peerId}`, cap);
  }

  // ---------------------------------------------------------------------------
  // Priority-aware task queue
  // ---------------------------------------------------------------------------

  async getQueueDepth(): Promise<number> {
    return this.taskQueue.filter((t) => !t.done).length;
  }

  async enqueueTask(
    model: string,
    prompt: string,
    priority: "latency" | "cost" | "reliability" = "latency",
    messages?: { role: string; content: string }[],
    routingHint?: string,
  ): Promise<string> {
    const task: Task = {
      id: crypto.randomUUID(),
      model,
      prompt,
      messages,
      created: Date.now(),
      done: false,
      attempts: 0,
      priority,
      routingHint,
    };

    this.taskQueue.push(task);
    await this.encPut(`task:${task.id}`, task);
    this.broadcastToPeers({
      type: "new_task",
      taskId: task.id,
      model,
      priority,
    });

    return task.id;
  }

  async claimTask(peerId: string): Promise<Task | null> {
    // Restore from storage if in-memory queue is empty
    if (this.taskQueue.length === 0) {
      const stored = await encList<Task>(this.ctx.storage, { prefix: "task:" });
      if (stored) {
        for (const [, val] of stored) {
          if (
            !val.claimed &&
            !val.done &&
            !this.taskQueue.find((t) => t.id === val.id)
          ) {
            this.taskQueue.push(val);
          }
        }
      }
    }

    // Sort: higher priority tasks claimed first
    this.sortQueueByPriority();

    const task = this.taskQueue.find((t) => !t.claimed && !t.done);
    if (task) {
      task.claimed = peerId;
      task.attempts++;
      await this.encPut(`task:${task.id}`, task);
      return task;
    }

    return null;
  }

  async submitResult(taskId: string, result: string): Promise<boolean> {
    let task = this.taskQueue.find((t) => t.id === taskId);
    if (!task) {
      task = (await encGet<Task>(this.ctx.storage, `task:${taskId}`)) ?? null;
    }
    if (task) {
      task.result = result;
      task.done = true;
      if (!this.taskQueue.find((t) => t.id === taskId)) {
        this.taskQueue.push(task);
      }
      await this.encPut(`task:${task.id}`, task);
      this.broadcastToPeers({ type: "task_done", taskId });
      return true;
    }
    return false;
  }

  async getTaskResult(taskId: string): Promise<Task | null> {
    const inMem = this.taskQueue.find((t) => t.id === taskId);
    if (inMem) return inMem;
    return (await encGet<Task>(this.ctx.storage, `task:${taskId}`)) ?? null;
  }

  private sortQueueByPriority(): void {
    const rank: Record<string, number> = {
      latency: 3,
      cost: 2,
      reliability: 1,
    };
    this.taskQueue.sort((a, b) => {
      const pa = rank[a.priority ?? "reliability"] ?? 0;
      const pb = rank[b.priority ?? "reliability"] ?? 0;
      return pb - pa;
    });
  }

  // ---------------------------------------------------------------------------
  // WebSocket connection management
  // ---------------------------------------------------------------------------

  async registerConnection(
    peerId: string,
    webSocket: WebSocket,
  ): Promise<void> {
    this.connections.set(peerId, webSocket);

    webSocket.addEventListener("close", () => {
      this.connections.delete(peerId);
    });

    webSocket.addEventListener("message", async (msg: MessageEvent) => {
      try {
        const data = JSON.parse(msg.data as string);
        if (data.type === "heartbeat") {
          const cap = this.capabilities.get(peerId);
          if (cap) {
            cap.lastHeartbeat = Date.now();
            cap.currentLoad = data.load ?? cap.currentLoad;
            cap.avgLatency = data.latency ?? cap.avgLatency;
            await this.encPut(`cap:${peerId}`, cap);
          }
        }
      } catch {
        // ignore malformed frames
      }
    });
  }

  private broadcastToPeers(msg: object): void {
    const payload = JSON.stringify(msg);
    for (const [id, ws] of this.connections) {
      try {
        ws.send(payload);
      } catch {
        this.connections.delete(id);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Raw storage access (for admin / debugging)
  // ---------------------------------------------------------------------------

  async getStorage(key: string): Promise<unknown> {
    return this.encGet(key);
  }

  async listStorage(prefix: string) {
    return encList<any>(this.ctx.storage, { prefix });
  }

  // ---------------------------------------------------------------------------
  // Fetch handler — internal DO-to-DO and HTTP-ingress routing
  // ---------------------------------------------------------------------------

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    try {
      // Peer management
      if (path === "/announce" && request.method === "POST") {
        const body = (await request.json()) as {
          peerId: string;
          models: string[];
          capabilities?: Partial<PeerCapability>;
        };
        await this.announce(
          body.peerId,
          body.models,
          body.capabilities,
        );
        return new Response("OK");
      }

      if (path === "/register-capability" && request.method === "POST") {
        const body = (await request.json()) as PeerCapability;
        await this.registerCapability(body);
        return new Response("OK");
      }

      if (path === "/peers") {
        const peers = await this.getPeers();
        return new Response(
          JSON.stringify({ shard: this.shardName, peers }),
        );
      }

      if (path === "/find-peer") {
        const model = url.searchParams.get("model") || "";
        const priority = (url.searchParams.get("priority") as
          | "latency"
          | "cost"
          | "reliability") || "latency";
        const peer = this.findBestPeer(model, priority);
        return new Response(JSON.stringify({ peer }));
      }

      // Task queue
      if (path === "/queue/depth") {
        const depth = await this.getQueueDepth();
        return new Response(JSON.stringify({ depth }));
      }

      if (path === "/task" && request.method === "POST") {
        const body = (await request.json()) as {
          model: string;
          prompt: string;
          priority?: "latency" | "cost" | "reliability";
          messages?: { role: string; content: string }[];
          routingHint?: string;
        };
        const taskId = await this.enqueueTask(
          body.model,
          body.prompt,
          body.priority,
          body.messages,
          body.routingHint,
        );
        return new Response(JSON.stringify({ taskId }));
      }

      if (path === "/task/claim" && request.method === "POST") {
        const body = (await request.json()) as { peerId: string };
        const task = await this.claimTask(body.peerId);
        return new Response(
          JSON.stringify({ task }),
          {
            headers: { "Content-Type": "application/json" },
          },
        );
      }

      if (path.startsWith("/task/") && request.method === "POST") {
        const taskId = path.split("/").pop()!;
        const body = (await request.json()) as { result: string };
        const ok = await this.submitResult(taskId, body.result);
        return new Response(JSON.stringify({ ok }));
      }

      if (path.startsWith("/task/")) {
        const taskId = path.split("/").pop()!;
        const task = await this.getTaskResult(taskId);
        if (!task) {
          return new Response(
            JSON.stringify({ error: "not_found" }),
            { status: 404, headers: { "Content-Type": "application/json" } },
          );
        }
        return new Response(JSON.stringify(task), {
          headers: { "Content-Type": "application/json" },
        });
      }

      // WebSocket upgrade
      if (path === "/connect" && request.headers.get("Upgrade") === "websocket") {
        const peerId =
          url.searchParams.get("peer_id") || `ws:${crypto.randomUUID()}`;
        const pair = new WebSocketPair();
        const [client, server] = [pair[0], pair[1]];
        server.accept();
        await this.registerConnection(peerId, server);
        server.send(
          JSON.stringify({
            type: "connected",
            peerId,
            shard: this.shardName,
          }),
        );
        return new Response(null, { status: 101, webSocket: client });
      }

      return new Response(
        `P2PSwarmDO [${this.shardName}] — use /announce, /peers, /find-peer, /task/*, /connect`,
        { status: 200 },
      );
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      console.error(`P2PSwarmDO [${this.shardName}] error:`, msg);
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
