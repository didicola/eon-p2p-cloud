import { DurableObject } from "cloudflare:workers";
import { type Env, type Task } from "../types";
import { FREE_PROVIDERS, getModelFamily, parseReply } from "../models";

/**
 * CloudP2PAgentDO — a cloud-hosted compute peer that participates in the P2P
 * swarm by claiming and fulfilling inference tasks.
 *
 * Behaviour:
 *   - Announces itself to the swarm on initialize() using ENV vars only (no
 *     hardcoded URLs).
 *   - On each alarm() tick it claims a task from the swarm, runs it through
 *     the free-LLM provider chain, and submits the result.
 *   - Uses ctx.storage for a durable processing lock (no in-memory global
 *     flag), so alarm() is safe across colo restarts.
 *   - Adaptive alarm interval: 3 s when busy (claimed a task), 10 s when idle.
 */
export class CloudP2PAgentDO extends DurableObject<Env> {
  private peerId: string;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.peerId = `cloud-agent:${ctx.id.name}`;
  }

  // -----------------------------------------------------------------------
  // Lifecycle
  // -----------------------------------------------------------------------

  /**
   * One-time initialisation.  Announces to the swarm and sets the first
   * alarm so the agent starts processing tasks.
   */
  async initialize(): Promise<void> {
    const initialized = await this.ctx.storage.get<boolean>("initialized");
    if (initialized) return;

    await this.announce();
    await this.ctx.storage.put("initialized", true);

    const nextAlarm = await this.ctx.storage.getAlarm();
    if (nextAlarm === null || nextAlarm === undefined) {
      // First alarm: fire in 5 s to give the swarm time to index us
      await this.ctx.storage.setAlarm(Date.now() + 5_000);
    }
  }

  /**
   * Announce this agent's presence to every bridge URL configured in env.
   * Uses env.LOCAL_BRIDGE_URL / env.LOCAL_P2P_URL — never hardcoded.
   */
  async announce(): Promise<void> {
    const env = this.env;
    const endpoints: string[] = [];

    if (env.LOCAL_BRIDGE_URL) endpoints.push(env.LOCAL_BRIDGE_URL);
    if (env.LOCAL_P2P_URL) endpoints.push(env.LOCAL_P2P_URL);

    const body = JSON.stringify({
      peer: this.peerId,
      models: Object.keys(FREE_PROVIDERS.map(() => "general")),
      version: "2.0",
      protocol: "http",
      currentLoad: 0,
      maxLoad: 10,
    });

    for (const baseUrl of endpoints) {
      try {
        await fetch(`${baseUrl}/p2p/announce`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body,
          signal: AbortSignal.timeout(10_000),
        });
      } catch (e) {
        console.warn(
          `CloudP2PAgentDO: announce to ${baseUrl} failed:`,
          e,
        );
      }
    }
  }

  // -----------------------------------------------------------------------
  // LLM inference
  // -----------------------------------------------------------------------

  /**
   * Try each free provider in order until one returns a non-null response.
   */
  async callLLM(prompt: string): Promise<string | null> {
    for (const provider of FREE_PROVIDERS) {
      try {
        const res = await fetch(provider.url, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            model: provider.model,
            messages: [
              {
                role: "system",
                content: "You are Eon. Be concise and accurate.",
              },
              { role: "user", content: prompt },
            ],
            max_tokens: 500,
          }),
          signal: AbortSignal.timeout(25_000),
        });

        if (res.ok) {
          const data = (await res.json()) as {
            choices?: { message?: { content?: string } }[];
          };
          const raw = data?.choices?.[0]?.message?.content ?? "";
          const reply = parseReply(raw);
          if (reply) return reply;
        }
      } catch (e) {
        console.warn(`CloudP2PAgentDO: free LLM ${provider.name} failed:`, e);
      }
    }

    return null;
  }

  // -----------------------------------------------------------------------
  // Alarm — main work loop
  // -----------------------------------------------------------------------

  /**
   * Durable alarm handler.  Uses storage-based locking so that only one
   * alarm cycle executes at a time, even across colo restarts.
   *
   * 1. Acquire the processing lock (ctx.storage, 30 s TTL).
   * 2. Claim a task from the swarm.
   * 3. Run callLLM on it.
   * 4. Submit the result back.
   * 5. Release the lock.
   * 6. Schedule the next alarm (3 s if work was done, 10 s if idle).
   */
  async alarm(): Promise<void> {
    // Storage-based lock: write a timestamp; if it is <30 s old another
    // cycle is already running.
    const lockKey = "processing_lock";
    const now = Date.now();
    const lockTs = await this.ctx.storage.get<number>(lockKey);

    if (lockTs && now - lockTs < 30_000) {
      // Lock is still held — skip this alarm tick
      await this.scheduleNext(false);
      return;
    }

    await this.ctx.storage.put(lockKey, now);

    let hadWork = false;
    try {
      const task = await this.claimFromSwarm();
      if (task) {
        hadWork = true;
        const result = (await this.callLLM(task.prompt)) ??
          "Cloud agent: no provider available";
        await this.submitToSwarm(task.id, result);
      }
    } catch (e) {
      console.error("CloudP2PAgentDO: alarm processing error:", e);
    } finally {
      // Release lock
      await this.ctx.storage.delete(lockKey);
    }

    await this.scheduleNext(hadWork);
  }

  /**
   * Claim a pending task from the P2P swarm (general shard).
   */
  private async claimFromSwarm(): Promise<Task | null> {
    const shard = getModelFamily("general");
    const doId = this.env.P2P_SWARM.idFromName(`eon-swarm-${shard}`);
    const stub = this.env.P2P_SWARM.get(doId);

    const resp = await stub.fetch("http://internal/task/claim", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ peerId: this.peerId }),
    });

    if (resp.ok) {
      const data = (await resp.json()) as { task: Task | null };
      return data.task ?? null;
    }

    return null;
  }

  /**
   * Submit a completed task result back to the swarm.
   */
  private async submitToSwarm(
    taskId: string,
    result: string,
  ): Promise<boolean> {
    const shard = getModelFamily("general");
    const doId = this.env.P2P_SWARM.idFromName(`eon-swarm-${shard}`);
    const stub = this.env.P2P_SWARM.get(doId);

    const resp = await stub.fetch(`http://internal/task/${taskId}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ result }),
    });

    if (!resp.ok) {
      console.warn(
        `CloudP2PAgentDO: failed to submit result for task ${taskId}: ${resp.status}`,
      );
      return false;
    }

    return true;
  }

  /**
   * Schedule the next alarm with an adaptive interval.
   *
   * @param hadWork  true if the previous cycle processed a task
   */
  private async scheduleNext(hadWork: boolean): Promise<void> {
    const interval = hadWork ? 3_000 : 10_000;
    await this.ctx.storage.setAlarm(Date.now() + interval);
  }

  // -----------------------------------------------------------------------
  // Fetch handler
  // -----------------------------------------------------------------------

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    try {
      if (path === "/init" && request.method === "POST") {
        await this.initialize();
        return new Response(
          JSON.stringify({
            agent: this.peerId,
            status: "initialized",
          }),
          { headers: { "Content-Type": "application/json" } },
        );
      }

      if (path === "/announce") {
        await this.announce();
        return new Response("OK");
      }

      if (path === "/status") {
        return new Response(
          JSON.stringify({
            peerId: this.peerId,
            alive: true,
          }),
          { headers: { "Content-Type": "application/json" } },
        );
      }

      return new Response(
        `CloudP2PAgentDO [${this.peerId}] — /init, /announce, /status`,
        { status: 200 },
      );
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      console.error("CloudP2PAgentDO error:", msg);
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
