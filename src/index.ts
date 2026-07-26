// =============================================================================
// EON P2P Cloud — Entry Point
// =============================================================================
// Re-exports all Durable Object classes so wrangler can find them by class_name.
// Default export: fetch (HTTP), queue (consumer), scheduled (cron).
// =============================================================================

// -----------------------------------------------------------------------------
// DO Re-exports (wrangler bindings)
// -----------------------------------------------------------------------------
export { P2PSwarmDO } from "./do/p2p-swarm";
export { ReputationDO } from "./do/reputation";
export { CloudOpencodeDO } from "./do/cloud-opencode";
export { CloudP2PAgentDO } from "./do/cloud-p2p-agent";
export { ModelRegistryDO } from "./do/model-registry";
export { IncentiveDO } from "./do/incentives";
export { EdgeSwarmDO } from "./do/edge-swarm";
export { RegionalSwarmDO } from "./do/regional-swarm";
export { GlobalSwarmDO } from "./do/global-swarm";

// -----------------------------------------------------------------------------
// Module imports
// -----------------------------------------------------------------------------
import type {
  Env,
  QueueTask,
  ChatRequest,
  PeerCapability,
  ProviderRegistration,
  VerificationRequest,
  VerificationResponse,
} from "./types";

import {
  MODELS,
  SWARM_SHARDS,
  FREE_PROVIDERS,
  getModelFamily,
  parseReply,
  getSwarmDO,
  shardForKey,
} from "./models";

import { AGENT_ROUTES } from "./agents";
import { handleWebSocketUpgrade } from "./peer-protocol";
import { cachedInference } from "./cache";
import { verifyTask } from "./verification";

export type { Env } from "./types";

// -----------------------------------------------------------------------------
// Log helper
// -----------------------------------------------------------------------------
interface LogEntry {
  level: "info" | "warn" | "error";
  msg: string;
  data?: unknown;
  timestamp: number;
}

async function log(
  env: Env,
  ctx: ExecutionContext,
  level: LogEntry["level"],
  msg: string,
  data?: unknown,
): Promise<void> {
  const entry: LogEntry = { level, msg, data, timestamp: Date.now() };
  if (env.LOG_BUCKET) {
    ctx.waitUntil(
      env.LOG_BUCKET
        .put(
          `logs/${Date.now()}-${crypto.randomUUID()}.json`,
          JSON.stringify(entry),
        )
        .catch(() => {}),
    );
  }
  if (level === "error") console.error(msg, data);
  else if (level === "warn") console.warn(msg, data);
}

// -----------------------------------------------------------------------------
// Rate limiting (sliding window, per IP)
// -----------------------------------------------------------------------------
function getRateLimitKey(request: Request): string | null {
  return (
    request.headers.get("CF-Connecting-IP") ||
    request.headers.get("X-Forwarded-For") ||
    null
  );
}

async function checkRateLimit(
  env: Env,
  key: string,
  limit: number,
  windowMs: number,
): Promise<boolean> {
  if (!env.RATE_LIMIT_KV) return true;
  try {
    const now = Date.now();
    const bucket = Math.floor(now / windowMs);
    const windowKey = `ratelimit:${key}:${bucket}`;
    const count = parseInt((await env.RATE_LIMIT_KV.get(windowKey)) || "0", 10);
    if (count >= limit) return false;
    await env.RATE_LIMIT_KV.put(windowKey, String(count + 1), {
      expirationTtl: Math.ceil(windowMs / 1000),
    });
    return true;
  } catch (e) {
    console.error("Rate limit check error:", e);
    return true; // allow on failure
  }
}

// -----------------------------------------------------------------------------
// Workers AI call
// -----------------------------------------------------------------------------
async function callWorkersAI(
  ai: Ai,
  model: string,
  messages: { role: string; content: string }[],
  maxTokens: number,
): Promise<string | null> {
  try {
    const res = await ai.run(model, {
      messages,
      max_tokens: maxTokens,
    });
    const d = res as {
      response?: string;
      choices?: { message?: { content?: string } }[];
    };
    const reply = parseReply(
      d?.response || d?.choices?.[0]?.message?.content || "",
    );
    return reply || null;
  } catch (e) {
    console.error(`Workers AI error for ${model}:`, e);
    return null;
  }
}

// -----------------------------------------------------------------------------
// External API call (free providers, blind proxy, etc.)
// -----------------------------------------------------------------------------
async function callExternalAPI(
  url: string,
  model: string,
  messages: { role: string; content: string }[],
): Promise<string | null> {
  try {
    const endpoint = url.endsWith("/v1/chat/completions")
      ? url
      : `${url.replace(/\/+$/, "")}/v1/chat/completions`;
    const res = await fetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ model: model || "auto", messages, max_tokens: 800 }),
      signal: AbortSignal.timeout(30000),
    });
    if (!res.ok) {
      console.warn(`External API ${url} returned ${res.status}`);
      return null;
    }
    const data = (await res.json()) as {
      choices?: { message?: { content?: string } }[];
    };
    const reply = parseReply(data?.choices?.[0]?.message?.content || "");
    return reply || null;
  } catch (e) {
    console.error(`External API error for ${url}:`, e);
    return null;
  }
}

// -----------------------------------------------------------------------------
// Streaming chat helper (SSE via Workers AI stream mode)
// -----------------------------------------------------------------------------
async function handleStreamingChat(
  env: Env,
  ctx: ExecutionContext,
  cfModel: string | undefined,
  modelName: string,
  body: ChatRequest,
  corsHeaders: Record<string, string>,
): Promise<Response> {
  if (!cfModel) {
    return new Response(
      JSON.stringify({ error: "Streaming not available for this model" }),
      {
        status: 400,
        headers: { "Content-Type": "application/json", ...corsHeaders },
      },
    );
  }

  try {
    const stream = (await env.AI.run(cfModel, {
      messages: body.messages,
      max_tokens: body.max_tokens || 800,
      stream: true,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } as any)) as ReadableStream;

    // Tee so we can drain in the background while sending SSE
    const tee = stream.tee();

    // Background drain (keeps the stream alive on Workers AI side)
    ctx.waitUntil(
      (async () => {
        const reader = tee[1].getReader();
        const decoder = new TextDecoder();
        try {
          while (true) {
            const { done } = await reader.read();
            if (done) break;
          }
        } catch {
          // drain error is non-fatal
        }
      })(),
    );

    const encoder = new TextEncoder();
    const sseStream = new ReadableStream({
      async start(controller) {
        const reader = tee[0].getReader();
        const decoder = new TextDecoder();
        try {
          while (true) {
            const { done, value } = await reader.read();
            if (done) {
              controller.enqueue(encoder.encode("data: [DONE]\n\n"));
              break;
            }
            const text = decoder.decode(value, { stream: true });
            const lines = text.split("\n").filter((l) => l.trim());
            for (const line of lines) {
              controller.enqueue(encoder.encode(`data: ${line}\n\n`));
            }
          }
        } catch (e) {
          console.error("SSE streaming error:", e);
        } finally {
          try {
            controller.close();
          } catch {
            // already closed
          }
        }
      },
    });

    return new Response(sseStream, {
      headers: {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        Connection: "keep-alive",
        ...corsHeaders,
      },
    });
  } catch (e) {
    console.error("Streaming init error:", e);
    return new Response(JSON.stringify({ error: "streaming_failed" }), {
      status: 500,
      headers: { "Content-Type": "application/json", ...corsHeaders },
    });
  }
}

// -----------------------------------------------------------------------------
// CORS headers
// -----------------------------------------------------------------------------
const CORS_HEADERS: Record<string, string> = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS, PUT, DELETE",
  "Access-Control-Allow-Headers":
    "Content-Type, Authorization, X-Requested-With",
};

/** Build a JSON error response with CORS. */
function jsonError(
  status: number,
  code: string,
  message: string,
  extra?: Record<string, unknown>,
): Response {
  return new Response(
    JSON.stringify({ error: code, message, ...extra }),
    {
      status,
      headers: { "Content-Type": "application/json", ...CORS_HEADERS },
    },
  );
}

// -----------------------------------------------------------------------------
// Default export — the Workers module
// -----------------------------------------------------------------------------
export default {
  // ═══════════════════════════════════════════════════════════════════════════
  // HTTP fetch handler
  // ═══════════════════════════════════════════════════════════════════════════
  async fetch(
    request: Request,
    env: Env,
    ctx: ExecutionContext,
  ): Promise<Response> {
    const url = new URL(request.url);
    const method = request.method;

    // ---- Rate limiting (60 req/min per IP, sliding window) ----
    const rateLimitKey = getRateLimitKey(request);
    if (rateLimitKey) {
      const allowed = await checkRateLimit(env, rateLimitKey, 60, 60000);
      if (!allowed) {
        return jsonError(429, "rate_limit_exceeded", "Too many requests");
      }
    }

    // ---- CORS preflight ----
    if (method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }

    try {
      // ─────────────────────────────────────────────────────────────
      // GET /status — full health
      // ─────────────────────────────────────────────────────────────
      if (url.pathname === "/status" && method === "GET") {
        const peerId = `cloud:${url.hostname}`;

        // Gather shard-level details in parallel
        const shardResults = await Promise.all(
          SWARM_SHARDS.map(async (shard) => {
            try {
              const doId = env.P2P_SWARM.idFromName(`eon-swarm-${shard}`);
              const stub = env.P2P_SWARM.get(doId);
              const [peerCount, queueDepth] = await Promise.all([
                stub.getPeerCount().catch(() => 0),
                stub.getQueueDepth().catch(() => -1),
              ]);
              return { shard, peers: peerCount, queueDepth };
            } catch (e) {
              console.error(`Status check failed for shard ${shard}:`, e);
              return { shard, peers: 0, queueDepth: -1, error: String(e) };
            }
          }),
        );

        // Aggregate counts
        const totalPeers = shardResults.reduce((s, r) => s + r.peers, 0);
        const totalQueueDepth = shardResults.reduce(
          (s, r) => s + Math.max(0, r.queueDepth),
          0,
        );

        // Try for global metrics too
        let globalMetrics: Record<string, unknown> | undefined;
        try {
          const globalId = env.GLOBAL_SWARM.idFromName("global");
          const globalStub = env.GLOBAL_SWARM.get(globalId);
          const gm = await globalStub.fetch("http://internal/metrics", {
            method: "GET",
            signal: AbortSignal.timeout(5000),
          });
          if (gm.ok) globalMetrics = await gm.json<any>();
        } catch {
          // GlobalSwarmDO may not be deployed yet — that is OK
        }

        return new Response(
          JSON.stringify({
            status: "operational",
            architecture: "cloud-p2p",
            version: "3.0",
            peer: peerId,
            models: Object.keys(MODELS).length,
            shardCount: SWARM_SHARDS.length,
            totalPeers,
            totalQueueDepth,
            shards: shardResults,
            globalMetrics,
          }),
          {
            headers: { "Content-Type": "application/json", ...CORS_HEADERS },
          },
        );
      }

      // ─────────────────────────────────────────────────────────────
      // GET /v1/models — list available models
      // ─────────────────────────────────────────────────────────────
      if (url.pathname === "/v1/models" && method === "GET") {
        return new Response(
          JSON.stringify({
            object: "list",
            data: Object.keys(MODELS).map((id) => ({
              id,
              object: "model",
              created: Date.now(),
              owned_by: "eon-p2p-cloud",
            })),
          }),
          {
            headers: { "Content-Type": "application/json", ...CORS_HEADERS },
          },
        );
      }

      // ─────────────────────────────────────────────────────────────
      // POST /v1/chat/completions — 3-tier chat + swarm enqueue
      // ─────────────────────────────────────────────────────────────
      if (
        (url.pathname === "/v1/chat/completions" || url.pathname === "/chat") &&
        method === "POST"
      ) {
        const body = (await request.json()) as ChatRequest;
        const modelName = body.model || "llama-3.3-70b";
        const cfModel = MODELS[modelName];

        // --- Streaming path ---
        if (body.stream) {
          // If we have a Workers AI model, stream it directly
          if (cfModel) {
            return handleStreamingChat(
              env,
              ctx,
              cfModel,
              modelName,
              body,
              CORS_HEADERS,
            );
          }
          return jsonError(
            400,
            "streaming_unavailable",
            `Model "${modelName}" does not support Workers AI streaming`,
          );
        }

        // --- Tier 1: Workers AI ---
        if (cfModel) {
          const response = await callWorkersAI(
            env.AI,
            cfModel,
            body.messages,
            body.max_tokens || 800,
          );
          if (response) {
            return new Response(
              JSON.stringify({
                id: `chatcmpl-${crypto.randomUUID()}`,
                object: "chat.completion",
                created: Math.floor(Date.now() / 1000),
                model: modelName,
                provider: "workers-ai",
                choices: [
                  {
                    index: 0,
                    message: { role: "assistant", content: response },
                    finish_reason: "stop",
                  },
                ],
                usage: {
                  prompt_tokens: body.messages.reduce(
                    (s, m) => s + m.content.length,
                    0,
                  ),
                  completion_tokens: response.length,
                  total_tokens:
                    body.messages.reduce((s, m) => s + m.content.length, 0) +
                    response.length,
                },
              }),
              {
                headers: {
                  "Content-Type": "application/json",
                  ...CORS_HEADERS,
                },
              },
            );
          }
          console.warn(`Workers AI returned no response for model ${modelName}`);
        }

        // --- Tier 2: Cached inference (via CACHE_KV) ---
        if (env.CACHE_KV) {
          try {
            const cached = await cachedInference(
              env, ctx, modelName, body.messages, body.max_tokens || 800,
              () => callWorkersAI(env.AI, MODELS[modelName]!, body.messages, body.max_tokens || 800).then(r => r || ""),
            );
            if (cached) {
              return new Response(
                JSON.stringify({
                  id: `chatcmpl-${crypto.randomUUID()}`,
                  object: "chat.completion",
                  created: Math.floor(Date.now() / 1000),
                  model: modelName,
                  provider: "cache",
                  choices: [
                    {
                      index: 0,
                      message: { role: "assistant", content: cached },
                      finish_reason: "stop",
                    },
                  ],
                }),
                {
                  headers: {
                    "Content-Type": "application/json",
                    ...CORS_HEADERS,
                  },
                },
              );
            }
          } catch (e) {
            console.error("Cache lookup error:", e);
          }
        }

        // --- Tier 3: Local blind proxy ---
        if (env.LOCAL_BLIND_PROXY) {
          const localResponse = await callExternalAPI(
            env.LOCAL_BLIND_PROXY,
            modelName,
            body.messages,
          );
          if (localResponse) {
            return new Response(
              JSON.stringify({
                id: `chatcmpl-${crypto.randomUUID()}`,
                object: "chat.completion",
                created: Math.floor(Date.now() / 1000),
                model: modelName,
                provider: "local-blind-proxy",
                choices: [
                  {
                    index: 0,
                    message: { role: "assistant", content: localResponse },
                    finish_reason: "stop",
                  },
                ],
              }),
              {
                headers: {
                  "Content-Type": "application/json",
                  ...CORS_HEADERS,
                },
              },
            );
          }
          console.warn(
            `Local blind proxy returned no response for model ${modelName}`,
          );
        }

        // --- Tier 4: Enqueue to swarm (fallback) ---
        const promptText = body.messages.map((m) => m.content).join("\n");
        const swarmStub = getSwarmDO(env, modelName);
        const taskId = await swarmStub.enqueueTask(
          modelName,
          promptText,
          "latency",
          body.messages,
        );

        // Fire-and-forget the queue message so swarm peers can pick it up
        ctx.waitUntil(
          env.TASK_QUEUE
            .send({
              type: "inference",
              model: modelName,
              messages: body.messages,
              taskId,
              requestId: crypto.randomUUID(),
            })
            .catch((e) => console.error("Queue send failed:", e)),
        );

        await log(env, ctx, "info", "chat_enqueued", {
          model: modelName,
          taskId,
        });

        return new Response(
          JSON.stringify({
            task_id: taskId,
            status: "queued",
            shard: getModelFamily(modelName),
            message:
              "All inference tiers exhausted; task routed to P2P swarm. Poll /p2p/task/:id for result.",
          }),
          {
            status: 202,
            headers: { "Content-Type": "application/json", ...CORS_HEADERS },
          },
        );
      }

      // ─────────────────────────────────────────────────────────────
      // POST /opencode/dispatch — dispatch to CloudOpencodeDO
      // ─────────────────────────────────────────────────────────────
      if (url.pathname === "/opencode/dispatch" && method === "POST") {
        const body = (await request.json()) as {
          agent: string;
          prompt: string;
        };
        const doId = env.OPENCODE.idFromName("opencode");
        const stub = env.OPENCODE.get(doId);
        const result = await stub.fetch("http://internal/dispatch", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        });
        const data = await result.json();
        await log(env, ctx, "info", "opencode_dispatch", {
          agent: body.agent,
        });
        return new Response(JSON.stringify(data), {
          headers: { "Content-Type": "application/json", ...CORS_HEADERS },
        });
      }

      // ─────────────────────────────────────────────────────────────
      // GET /opencode/agents — list agent routes
      // ─────────────────────────────────────────────────────────────
      if (url.pathname === "/opencode/agents" && method === "GET") {
        return new Response(
          JSON.stringify({
            agents: Object.keys(AGENT_ROUTES),
            count: Object.keys(AGENT_ROUTES).length,
            routes: AGENT_ROUTES,
          }),
          {
            headers: { "Content-Type": "application/json", ...CORS_HEADERS },
          },
        );
      }

      // ─────────────────────────────────────────────────────────────
      // POST /opencode/chain — run agent chain
      // ─────────────────────────────────────────────────────────────
      if (url.pathname === "/opencode/chain" && method === "POST") {
        const body = (await request.json()) as {
          steps: { agent: string; prompt: string }[];
        };
        const doId = env.OPENCODE.idFromName("opencode-chain");
        const stub = env.OPENCODE.get(doId);
        const resp = await stub.fetch("http://internal/chain", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        });
        const data = await resp.json();
        return new Response(JSON.stringify(data), {
          headers: { "Content-Type": "application/json", ...CORS_HEADERS },
        });
      }

      // ─────────────────────────────────────────────────────────────
      // GET /spawn-agent — spawn a CloudP2PAgentDO instance
      // ─────────────────────────────────────────────────────────────
      if (url.pathname === "/spawn-agent" && method === "GET") {
        const name = url.searchParams.get("name") || "default";
        const doId = env.CLOUD_AGENT.idFromName(name);
        const stub = env.CLOUD_AGENT.get(doId);
        const resp = await stub.fetch("http://internal/init", {
          method: "POST",
        });
        const status = resp.ok ? "active" : "failed";
        return new Response(
          JSON.stringify({
            agent: name,
            peer: `cloud-agent:${name}`,
            status,
          }),
          {
            headers: { "Content-Type": "application/json", ...CORS_HEADERS },
          },
        );
      }

      // ─────────────────────────────────────────────────────────────
      // POST /p2p/announce — register peer
      // ─────────────────────────────────────────────────────────────
      if (url.pathname === "/p2p/announce" && method === "POST") {
        const body = (await request.json()) as {
          peer: string;
          models?: string[];
          model?: string; // legacy single-model support
          version?: string;
          region?: string;
          colo?: string;
          load?: number;
          maxLoad?: number;
          latency?: number;
          protocol?: string;
          endpoint?: string;
        };

        const models = body.models || (body.model ? [body.model] : ["general"]);
        const shard = getModelFamily(models[0]);
        const doId = env.P2P_SWARM.idFromName(`eon-swarm-${shard}`);
        const stub = env.P2P_SWARM.get(doId);

        await stub.announce(body.peer, models, {
          region: body.region,
          version: body.version,
          currentLoad: body.load,
          maxLoad: body.maxLoad,
          avgLatency: body.latency,
          protocol: body.protocol as any,
          endpoint: body.endpoint,
          colo: body.colo,
        });

        // Also register in the per-colo EdgeSwarmDO if colo is known
        if (body.colo) {
          try {
            const edgeId = env.EDGE_SWARM.idFromName(`edge-${body.colo}`);
            const edgeStub = env.EDGE_SWARM.get(edgeId);
            await edgeStub.fetch("http://internal/register", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({
                peerId: body.peer,
                colo: body.colo,
                capabilities: {
                  models,
                  region: body.region || "unknown",
                  version: body.version || "1.0",
                  currentLoad: body.load || 0,
                  maxLoad: body.maxLoad || 5,
                  avgLatency: body.latency || 0,
                  protocol: body.protocol || "http",
                  endpoint: body.endpoint,
                },
              }),
            });
          } catch (e) {
            console.warn("EdgeSwarmDO registration failed:", e);
          }
        }

        return new Response(
          JSON.stringify({ ok: true, peer: body.peer, shard }),
          {
            status: 200,
            headers: { "Content-Type": "application/json", ...CORS_HEADERS },
          },
        );
      }

      // ─────────────────────────────────────────────────────────────
      // GET /p2p/peers — list peers, optional ?model= shard filter
      // ─────────────────────────────────────────────────────────────
      if (url.pathname === "/p2p/peers" && method === "GET") {
        const model = url.searchParams.get("model") || undefined;
        const shard = model ? getModelFamily(model) : undefined;

        // If shard specified, query only that shard; else query all
        const shardsToQuery = shard ? [shard] : SWARM_SHARDS;

        const results = await Promise.all(
          shardsToQuery.map(async (s) => {
            try {
              const doId = env.P2P_SWARM.idFromName(`eon-swarm-${s}`);
              const stub = env.P2P_SWARM.get(doId);
              const peers: PeerCapability[] = await stub.getPeers();
              return { shard: s, peers };
            } catch (e) {
              console.error(`Failed to get peers for shard ${s}:`, e);
              return { shard: s, peers: [], error: String(e) };
            }
          }),
        );

        return new Response(
          JSON.stringify({
            shard: shard || "all",
            totalPeers: results.reduce((s, r) => s + r.peers.length, 0),
            shards: results,
          }),
          {
            headers: { "Content-Type": "application/json", ...CORS_HEADERS },
          },
        );
      }

      // ─────────────────────────────────────────────────────────────
      // POST /p2p/tasks — claim a task
      // ─────────────────────────────────────────────────────────────
      if (url.pathname === "/p2p/tasks" && method === "POST") {
        const body = (await request.json()) as {
          peer: string;
          model?: string;
        };
        const stub = getSwarmDO(env, body.model || "general");
        const task = await stub.claimTask(body.peer);

        return new Response(
          JSON.stringify({ task }),
          {
            status: task ? 200 : 204,
            headers: { "Content-Type": "application/json", ...CORS_HEADERS },
          },
        );
      }

      // ─────────────────────────────────────────────────────────────
      // POST /p2p/task/:id — submit result
      // ─────────────────────────────────────────────────────────────
      if (
        url.pathname.startsWith("/p2p/task/") &&
        method === "POST" &&
        url.pathname.split("/").length === 4
      ) {
        const taskId = url.pathname.split("/").pop()!;
        const body = (await request.json()) as {
          result: string;
          peerId?: string;
          tokensGenerated?: number;
          latencyMs?: number;
        };

        const stub = getSwarmDO(env, "general");
        await stub.submitResult(taskId, body.result);

        // Record reputation if peerId provided
        if (body.peerId && env.REPUTATION) {
          try {
            const repId = env.REPUTATION.idFromName("reputation-global");
            const repStub = env.REPUTATION.get(repId);
            await repStub.fetch("http://internal/record", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({
                peerId: body.peerId,
                taskId,
                success: true,
                latencyMs: body.latencyMs || 0,
                score: 1.0,
              }),
            });
          } catch (e) {
            console.warn("Reputation recording failed:", e);
          }
        }

        // Credit the peer if tokensGenerated provided and incentives are active
        if (body.peerId && body.tokensGenerated && env.INCENTIVES) {
          try {
            const incId = env.INCENTIVES.idFromName("incentives-global");
            const incStub = env.INCENTIVES.get(incId);
            await incStub.fetch("http://internal/credit", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({
                peerId: body.peerId,
                taskId,
                tokensGenerated: body.tokensGenerated,
              }),
            });
          } catch (e) {
            console.warn("Incentive credit failed:", e);
          }
        }

        return new Response(
          JSON.stringify({ ok: true, taskId }),
          {
            status: 200,
            headers: { "Content-Type": "application/json", ...CORS_HEADERS },
          },
        );
      }

      // ─────────────────────────────────────────────────────────────
      // GET /p2p/task/:id — get task result
      // ─────────────────────────────────────────────────────────────
      if (
        url.pathname.startsWith("/p2p/task/") &&
        method === "GET" &&
        url.pathname.split("/").length === 4
      ) {
        const taskId = url.pathname.split("/").pop()!;
        const stub = getSwarmDO(env, "general");
        const task = await stub.getTaskResult(taskId);

        if (!task) {
          return jsonError(404, "task_not_found", `Task "${taskId}" not found`);
        }

        return new Response(JSON.stringify(task), {
          status: 200,
          headers: { "Content-Type": "application/json", ...CORS_HEADERS },
        });
      }

      // ─────────────────────────────────────────────────────────────
      // GET /p2p/connect — WebSocket upgrade
      // ─────────────────────────────────────────────────────────────
      if (
        url.pathname === "/p2p/connect" &&
        request.headers.get("Upgrade")?.toLowerCase() === "websocket"
      ) {
        return handleWebSocketUpgrade(request, env, ctx);
        const peerId =
          url.searchParams.get("peer_id") || `ws:${crypto.randomUUID()}`;
        const model = url.searchParams.get("model") || "general";
        const pair = new WebSocketPair();
        const [client, server] = [pair[0], pair[1]];
        server.accept();

        const stub = getSwarmDO(env, model);
        await stub.registerConnection(peerId, server);
        server.send(
          JSON.stringify({ type: "connected", peerId, shard: getModelFamily(model) }),
        );

        return new Response(null, { status: 101, webSocket: client });
      }

      // ─────────────────────────────────────────────────────────────
      // POST /providers/register — register model provider (Phase 3)
      // ─────────────────────────────────────────────────────────────
      if (url.pathname === "/providers/register" && method === "POST") {
        const body = (await request.json()) as ProviderRegistration;
        const regId = env.MODEL_REGISTRY.idFromName("global");
        const stub = env.MODEL_REGISTRY.get(regId);
        const resp = await stub.fetch("http://internal/register", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        });
        const data = await resp.json();
        return new Response(JSON.stringify(data), {
          status: resp.status,
          headers: { "Content-Type": "application/json", ...CORS_HEADERS },
        });
      }

      // ─────────────────────────────────────────────────────────────
      // GET /providers/models — list all models from registry
      // ─────────────────────────────────────────────────────────────
      if (url.pathname === "/providers/models" && method === "GET") {
        const regId = env.MODEL_REGISTRY.idFromName("global");
        const stub = env.MODEL_REGISTRY.get(regId);
        const resp = await stub.fetch("http://internal/models", {
          method: "GET",
          signal: AbortSignal.timeout(10000),
        });
        const data = await resp.json();
        return new Response(JSON.stringify(data), {
          headers: { "Content-Type": "application/json", ...CORS_HEADERS },
        });
      }

      // ─────────────────────────────────────────────────────────────
      // POST /incentives/balance — get peer balance (Phase 2)
      // ─────────────────────────────────────────────────────────────
      if (url.pathname === "/incentives/balance" && method === "POST") {
        const body = (await request.json()) as { peerId: string };
        if (!body.peerId) {
          return jsonError(400, "missing_peer_id", "peerId is required");
        }
        const incId = env.INCENTIVES.idFromName("incentives-global");
        const stub = env.INCENTIVES.get(incId);
        const resp = await stub.fetch(
          `http://internal/balance/${encodeURIComponent(body.peerId)}`,
          { method: "GET" },
        );
        const data = await resp.json();
        return new Response(JSON.stringify(data), {
          headers: { "Content-Type": "application/json", ...CORS_HEADERS },
        });
      }

      // ─────────────────────────────────────────────────────────────
      // POST /incentives/redeem — redeem credits
      // ─────────────────────────────────────────────────────────────
      if (url.pathname === "/incentives/redeem" && method === "POST") {
        const body = (await request.json()) as { peerId: string };
        if (!body.peerId) {
          return jsonError(400, "missing_peer_id", "peerId is required");
        }
        const incId = env.INCENTIVES.idFromName("incentives-global");
        const stub = env.INCENTIVES.get(incId);
        const resp = await stub.fetch(
          `http://internal/redeem/${encodeURIComponent(body.peerId)}`,
          { method: "POST" },
        );
        const data = await resp.json();
        return new Response(JSON.stringify(data), {
          headers: { "Content-Type": "application/json", ...CORS_HEADERS },
        });
      }

      // ─────────────────────────────────────────────────────────────
      // POST /admin/verify — trigger verification (Phase 2)
      // ─────────────────────────────────────────────────────────────
      if (url.pathname === "/admin/verify" && method === "POST") {
        const body = (await request.json()) as { prompt: string; model: string; count?: number };
        const swarmStub = getSwarmDO(env, body.model || "general");
        const result = await verifyTask(env, swarmStub, body.prompt, body.model || "general", body.count || 3);
        return new Response(JSON.stringify(result), {
          headers: { "Content-Type": "application/json", ...CORS_HEADERS },
        });
      }

      // ─────────────────────────────────────────────────────────────
      // GET /region/:region/metrics — get regional metrics (Phase 3)
      // ─────────────────────────────────────────────────────────────
      const regionMatch = url.pathname.match(
        /^\/region\/([a-z]{2})\/metrics$/,
      );
      if (regionMatch && method === "GET") {
        const region = regionMatch[1];
        if (!["na", "eu", "asia", "sa", "af", "oc"].includes(region)) {
          return jsonError(400, "invalid_region", `Unknown region "${region}"`);
        }

        try {
          // Try GlobalSwarmDO first (has cached snapshots)
          const globalId = env.GLOBAL_SWARM.idFromName("global");
          const globalStub = env.GLOBAL_SWARM.get(globalId);
          const resp = await globalStub.fetch(
            `http://internal/region-metrics?region=${region}`,
            { method: "GET", signal: AbortSignal.timeout(5000) },
          );
          if (resp.ok) {
            const data = await resp.json();
            return new Response(JSON.stringify(data), {
              headers: {
                "Content-Type": "application/json",
                ...CORS_HEADERS,
              },
            });
          }
        } catch (e) {
          console.warn(
            `GlobalSwarmDO metrics lookup failed for ${region}:`,
            e,
          );
        }

        // Fallback: query RegionalSwarmDO directly
        try {
          const regId = env.REGIONAL_SWARM.idFromName(`region-${region}`);
          const regStub = env.REGIONAL_SWARM.get(regId);
          const resp = await regStub.fetch("http://internal/metrics", {
            method: "GET",
            signal: AbortSignal.timeout(5000),
          });
          if (resp.ok) {
            const data = await resp.json();
            return new Response(JSON.stringify({ region, metrics: data }), {
              headers: {
                "Content-Type": "application/json",
                ...CORS_HEADERS,
              },
            });
          }
        } catch (e) {
          console.warn(
            `RegionalSwarmDO metrics lookup failed for ${region}:`,
            e,
          );
        }

        return jsonError(
          503,
          "metrics_unavailable",
          `Metrics for region "${region}" are not available`,
        );
      }

      // ─────────────────────────────────────────────────────────────
      // POST /admin/migrate — migrate task between regions (Phase 3)
      // ─────────────────────────────────────────────────────────────
      if (url.pathname === "/admin/migrate" && method === "POST") {
        const body = (await request.json()) as {
          taskId: string;
          fromRegion: string;
          toRegion: string;
        };

        if (!body.taskId || !body.fromRegion || !body.toRegion) {
          return jsonError(
            400,
            "missing_fields",
            "taskId, fromRegion, and toRegion are required",
          );
        }

        try {
          const globalId = env.GLOBAL_SWARM.idFromName("global");
          const globalStub = env.GLOBAL_SWARM.get(globalId);
          const resp = await globalStub.fetch("http://internal/migrate", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body),
            signal: AbortSignal.timeout(15000),
          });
          const data = await resp.json();
          return new Response(JSON.stringify(data), {
            status: resp.status,
            headers: { "Content-Type": "application/json", ...CORS_HEADERS },
          });
        } catch (e) {
          console.error("Migration error:", e);
          return jsonError(500, "migration_failed", String(e));
        }
      }

      // ─────────────────────────────────────────────────────────────
      // Fallback — informational message
      // ─────────────────────────────────────────────────────────────
      return new Response(
        JSON.stringify({
          service: "EON P2P Cloud",
          version: "3.0",
          endpoints: {
            status: "GET /status",
            models: "GET /v1/models",
            chat: "POST /v1/chat/completions",
            opencode_dispatch: "POST /opencode/dispatch",
            opencode_agents: "GET /opencode/agents",
            opencode_chain: "POST /opencode/chain",
            spawn_agent: "GET /spawn-agent",
            p2p_announce: "POST /p2p/announce",
            p2p_peers: "GET /p2p/peers",
            p2p_tasks: "POST /p2p/tasks",
            p2p_task_result: "POST /p2p/task/:id",
            p2p_task_get: "GET /p2p/task/:id",
            p2p_connect: "GET /p2p/connect (WebSocket)",
            providers_register: "POST /providers/register",
            providers_models: "GET /providers/models",
            incentives_balance: "POST /incentives/balance",
            incentives_redeem: "POST /incentives/redeem",
            admin_verify: "POST /admin/verify",
            region_metrics: "GET /region/:region/metrics",
            admin_migrate: "POST /admin/migrate",
          },
        }),
        {
          status: 200,
          headers: { "Content-Type": "application/json", ...CORS_HEADERS },
        },
      );
    } catch (e) {
      const errMsg = e instanceof Error ? e.message : String(e);
      console.error("Unhandled fetch error:", e);
      await log(env, ctx, "error", "unhandled_request_error", {
        path: url.pathname,
        error: errMsg,
      }).catch(() => {});
      return jsonError(500, "internal_error", errMsg);
    }
  },

  // ═══════════════════════════════════════════════════════════════════════════
  // Queue consumer
  // ═══════════════════════════════════════════════════════════════════════════
  async queue(
    batch: MessageBatch<QueueTask>,
    env: Env,
    ctx: ExecutionContext,
  ): Promise<void> {
    for (const msg of batch.messages) {
      const task = msg.body;
      const taskId = task.taskId;
      const requestId = task.requestId;

      try {
        if (task.type === "inference" && taskId && task.messages) {
          // Check if task is already done
          const stub = getSwarmDO(env, task.model || "general");
          const existing = await stub.getTaskResult(taskId);
          if (existing && existing.done) {
            msg.ack();
            continue;
          }

          // Try free providers
          let result: string | null = null;
          for (const provider of FREE_PROVIDERS) {
            result = await callExternalAPI(
              provider.url,
              provider.model,
              task.messages,
            );
            if (result) break;
          }

          // If local blind proxy configured, try it too
          if (!result && env.LOCAL_BLIND_PROXY) {
            result = await callExternalAPI(
              env.LOCAL_BLIND_PROXY,
              task.model || "auto",
              task.messages,
            );
          }

          if (result) {
            await stub.submitResult(taskId, result);
            // Cache the result if CACHE_KV is available
            if (env.CACHE_KV && task.model) {
              ctx.waitUntil(
                env.CACHE_KV!
                  .put(
                    `cache:${task.model}:${hashMessages(task.messages)}`,
                    JSON.stringify({
                      result,
                      model: task.model,
                      created: Date.now(),
                      ttl: 300_000, // 5 min
                      hits: 1,
                    }),
                    { expirationTtl: 300 },
                  )
                  .catch(() => {}),
              );
            }
            await log(env, ctx, "info", "queue_inference_completed", {
              taskId,
              model: task.model,
              requestId,
            });
          } else {
            console.warn(
              `Queue: all providers exhausted for task ${taskId}`,
            );
          }
        }

        if (task.type === "agent_dispatch" && task.agent && task.prompt) {
          const doId = env.OPENCODE.idFromName("opencode-queue");
          const stub = env.OPENCODE.get(doId);
          await stub.dispatch(task.agent, task.prompt);
          await log(env, ctx, "info", "queue_agent_dispatch_completed", {
            agent: task.agent,
            requestId,
          });
        }

        msg.ack();
      } catch (e) {
        console.error(
          `Queue processing error for task ${taskId || "unknown"}:`,
          e,
        );
        // Retry with backoff — max 3 retries (0, 5s, 30s via the
        // MessageBatch retry count built into Workers Queues).
        msg.retry({ delaySeconds: 5 });
      }
    }
  },

  // ═══════════════════════════════════════════════════════════════════════════
  // Scheduled (cron) handler — triggers SwarmScaler workflow
  // ═══════════════════════════════════════════════════════════════════════════
  async scheduled(
    event: ScheduledEvent,
    env: Env,
    ctx: ExecutionContext,
  ): Promise<void> {
    const cron = event.cron;

    await log(env, ctx, "info", "scheduled_trigger", { cron });

    try {
      // Sync regional metrics via GlobalSwarmDO
      const globalId = env.GLOBAL_SWARM.idFromName("global");
      const globalStub = env.GLOBAL_SWARM.get(globalId);
      const syncResp = await globalStub.fetch("http://internal/sync-regions", {
        method: "POST",
        signal: AbortSignal.timeout(30000),
      });

      if (syncResp.ok) {
        const syncData = (await syncResp.json()) as {
          regions: Record<string, unknown>;
        };
        const regionCount = Object.keys(syncData.regions || {}).length;
        await log(env, ctx, "info", "regional_sync_completed", {
          regions: regionCount,
        });
      }

      // Collect per-shard queue depths and decide if more agents are needed
      const shardDepths: Record<string, number> = {};
      for (const shard of SWARM_SHARDS) {
        try {
          const doId = env.P2P_SWARM.idFromName(`eon-swarm-${shard}`);
          const stub = env.P2P_SWARM.get(doId);
          shardDepths[shard] = (await stub.getQueueDepth()) || 0;
        } catch (e) {
          console.error(`Scheduled: failed to get depth for shard ${shard}:`, e);
          shardDepths[shard] = -1;
        }
      }

      const totalDepth = Object.values(shardDepths).reduce(
        (s, d) => s + Math.max(0, d),
        0,
      );

      // Spawn extra agents if queue is deep (> 20 pending tasks)
      if (totalDepth > 20) {
        const extraAgents = Math.min(Math.ceil(totalDepth / 10), 5);
        for (let i = 0; i < extraAgents; i++) {
          const name = `scaler-${Date.now()}-${i}`;
          try {
            const doId = env.CLOUD_AGENT.idFromName(name);
            const stub = env.CLOUD_AGENT.get(doId);
            await stub.fetch("http://internal/init", { method: "POST" });
          } catch (e) {
            console.error(`Scheduled: failed to spawn agent ${name}:`, e);
          }
        }
        await log(env, ctx, "info", "scaler_spawned_agents", {
          count: extraAgents,
          totalDepth,
          shardDepths,
        });
      } else {
        await log(env, ctx, "info", "scaler_no_action_needed", {
          totalDepth,
          shardDepths,
        });
      }
    } catch (e) {
      console.error("Scheduled handler error:", e);
      await log(env, ctx, "error", "scheduled_handler_error", {
        cron,
        error: String(e),
      });
    }
  },
};

// -----------------------------------------------------------------------------
// Utility: hash messages for cache key
// -----------------------------------------------------------------------------
function hashMessages(
  messages: { role: string; content: string }[],
): string {
  const joined = messages
    .map((m) => `${m.role}:${m.content}`)
    .join("|");
  let hash = 0;
  for (let i = 0; i < joined.length; i++) {
    hash = ((hash << 5) - hash + joined.charCodeAt(i)) | 0;
  }
  return Math.abs(hash).toString(16);
}
