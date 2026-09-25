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
export { AccountManagerDO } from "./do/account-manager";
export { DreamMemoryDO } from "./do/dream-memory";
export { DreamEngineDO } from "./do/dream-engine";
export { TunnelRelayDO } from "./do/tunnel-relay";

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
import { routeInference, getRoutableModels } from "./unified-router";
import { DNA_MANIFEST } from "./dna";

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
    // DOOR-PATCH (proven 2026-09-15): streams break when max_tokens > ~8192.
    // Clamp to 8192 whenever streaming is requested.
    const maxTokens = body.max_tokens ? Math.min(body.max_tokens, 8192) : 800;
    const stream = (await env.AI.run(cfModel, {
      messages: body.messages,
      max_tokens: maxTokens,
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
        let sentDone = false;
        try {
          while (true) {
            const { done, value } = await reader.read();
            if (done) {
              if (!sentDone) {
                controller.enqueue(encoder.encode("data: [DONE]\n\n"));
                sentDone = true;
              }
              break;
            }
            const text = decoder.decode(value, { stream: true });
            const lines = text.split("\n").filter((l) => l.trim());
            for (const line of lines) {
              let content = line.trim();
              // DOOR-PATCH (proven 2026-09-15): strip any existing "data: "
              // prefix so we never emit "data: data: {...}" double-prefix.
              if (content.startsWith("data:")) content = content.slice(5).trim();
              if (!content) continue;
              // DOOR-PATCH: honor an upstream [DONE] once; never duplicate.
              if (content === "[DONE]") {
                if (!sentDone) {
                  controller.enqueue(encoder.encode("data: [DONE]\n\n"));
                  sentDone = true;
                }
                continue;
              }
              // DOOR-PATCH (proven 2026-09-15): drop usage-only stats chunks
              // (no choices, no error) that break SDK type validation.
              try {
                const parsed = JSON.parse(content);
                if (
                  parsed &&
                  typeof parsed === "object" &&
                  !parsed.choices &&
                  !parsed.error
                ) {
                  continue;
                }
              } catch {
                // not JSON; pass through
              }
              controller.enqueue(encoder.encode(`data: ${content}\n\n`));
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
// Auth helper — checks Bearer token against env.AUTH_TOKEN
// -----------------------------------------------------------------------------
function requireAuth(request: Request, env: Env): Response | null {
  if (!env.AUTH_TOKEN) return null; // no token configured = no auth
  const auth = request.headers.get("Authorization");
  if (!auth || !auth.startsWith("Bearer ")) {
    return jsonError(401, "unauthorized", "Missing or invalid Authorization header");
  }
  const token = auth.slice(7);
  if (token !== env.AUTH_TOKEN) {
    return jsonError(403, "forbidden", "Invalid token");
  }
  return null;
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
      // GET /v1/models — list available models (Workers AI + unified router)
      // ─────────────────────────────────────────────────────────────
      if (url.pathname === "/v1/models" && method === "GET") {
        const workersModels = Object.keys(MODELS).map((id) => ({
          id,
          object: "model",
          created: Date.now(),
          owned_by: "workers-ai",
        }));
        const routedModels = getRoutableModels().map((m) => ({
          id: m.id,
          object: "model",
          created: Date.now(),
          owned_by: `unified:${m.provider}`,
        }));
        const allModels = [...workersModels, ...routedModels];
        // Deduplicate by id
        const seen = new Set<string>();
        const unique = allModels.filter((m) => {
          if (seen.has(m.id)) return false;
          seen.add(m.id);
          return true;
        });
        return new Response(
          JSON.stringify({
            object: "list",
            data: unique,
            total: unique.length,
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
        const authErr = requireAuth(request, env);
        if (authErr) return authErr;
        let body: any = (await request.json()) as ChatRequest;
        let modelName = body.model || "llama-3.3-70b";
        let e2eEncrypted = false;
        if (body.encrypted && body.payload) {
          try {
            const e2eKey = await deriveE2EKey(env.AUTH_TOKEN || "");
            const plain = await e2eDecrypt(e2eKey, body.payload);
            body = JSON.parse(plain) as ChatRequest;
            modelName = body.model || "llama-3.3-70b";
            e2eEncrypted = true;
          } catch (e) {
            return new Response(JSON.stringify({ error: "E2E decryption failed", detail: String(e) }), {
              status: 400,
              headers: { "Content-Type": "application/json", ...CORS_HEADERS },
            });
          }
        }

        // Resolve agent type names to Workers AI models via AGENT_ROUTES
        const agentRoute = AGENT_ROUTES[modelName];
        const resolvedAgent = agentRoute ? agentRoute.system : null;
        if (agentRoute?.backup) {
          modelName = agentRoute.backup; // use the Workers AI backup model
        }

        // Map short model names to full Workers AI IDs
        let cfModel = MODELS[modelName];
        if (!cfModel) {
          // Fallback: try "llama-3.3-70b" as default
          cfModel = MODELS["llama-3.3-70b"];
          modelName = "llama-3.3-70b";
        }

        // Inject agent system prompt if available
        if (resolvedAgent && body.messages[0]?.role === "system") {
          body.messages[0].content = resolvedAgent + "\n\n" + body.messages[0].content;
        }

        // --- Streaming path ---
        if (body.stream) {
          return handleStreamingChat(
            env,
            ctx,
            cfModel,
            modelName,
            body,
            CORS_HEADERS,
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

        // --- Tier 1b: Free API via AGENT_ROUTES primary model ---
        if (agentRoute && agentRoute.model !== modelName && body.messages?.length) {
          try {
            const freeResponse = await callExternalAPI(
              agentRoute.model,
              body.messages,
              body.max_tokens || 800,
            );
            if (freeResponse) {
              return new Response(
                JSON.stringify({
                  id: `chatcmpl-${crypto.randomUUID()}`,
                  object: "chat.completion",
                  created: Math.floor(Date.now() / 1000),
                  model: agentRoute.model,
                  provider: "free-api",
                  choices: [{ index: 0, message: { role: "assistant", content: freeResponse }, finish_reason: "stop" }],
                  usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
                }),
                { headers: { "Content-Type": "application/json", ...CORS_HEADERS } },
              );
            }
          } catch {}
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

        // --- Tier 3: Unified router (Workers AI → Free APIs → P2P Swarm) ---
        const sysMsg = body.messages.find((m) => m.role === "system");
        const userMsg = body.messages.filter((m) => m.role !== "system").map((m) => m.content).join("\n");
        const unified = await routeInference(
          env,
          sysMsg?.content || "",
          userMsg || body.messages.map((m) => m.content).join("\n"),
          modelName,
        );
        if (unified) {
          return new Response(
            JSON.stringify({
              id: `chatcmpl-${crypto.randomUUID()}`,
              object: "chat.completion",
              created: Math.floor(Date.now() / 1000),
              model: modelName,
              provider: "unified-router",
              choices: [
                {
                  index: 0,
                  message: { role: "assistant", content: unified },
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
        console.warn(`Unified router returned no response for model ${modelName}`);

        // --- Tier 4: Local blind proxy ---
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

        // --- Tier 5: Enqueue to swarm (fallback) ---
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
        const authErr = requireAuth(request, env);
        if (authErr) return authErr;
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
        const authErr = requireAuth(request, env);
        if (authErr) return authErr;
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
      // POST /report-ip — node broadcasts its LAN IP for SSHFS/mesh mounting
      // GET /get-ip/:node — fetch a node's reported IP
      // ─────────────────────────────────────────────────────────────
      if (url.pathname === "/report-ip" && method === "POST") {
        const body = (await request.json()) as { node?: string; ip?: string; port?: number };
        const node = body.node || "unknown";
        const record = JSON.stringify({ ip: body.ip || "", port: body.port || 8022, updated: Date.now() });
        await env.CACHE_KV.put(`ip:${node}`, record);
        await log(env, ctx, "info", "report_ip", { node, ip: body.ip, port: body.port });
        return new Response(JSON.stringify({ ok: true, node, stored: JSON.parse(record) }), {
          headers: { "Content-Type": "application/json", ...CORS_HEADERS },
        });
      }

      if (url.pathname.startsWith("/get-ip/") && method === "GET") {
        const node = url.pathname.split("/").pop() || "";
        const raw = await env.CACHE_KV.get(`ip:${node}`);
        if (!raw) {
          return new Response(JSON.stringify({ ok: false, node, error: "not_found" }), {
            status: 404, headers: { "Content-Type": "application/json", ...CORS_HEADERS },
          });
        }
        return new Response(JSON.stringify({ ok: true, node, ...JSON.parse(raw) }), {
          headers: { "Content-Type": "application/json", ...CORS_HEADERS },
        });
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
        const authErr = requireAuth(request, env);
        if (authErr) return authErr;
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
        const authErr = requireAuth(request, env);
        if (authErr) return authErr;
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
        const authErr = requireAuth(request, env);
        if (authErr) return authErr;
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
      // POST /accounts/register — register a provider account
      //   Body: { alias, provider, apiKey, email, baseUrl?, note? }
      // ─────────────────────────────────────────────────────────────
      if (url.pathname === "/accounts/register" && method === "POST") {
        const authErr = requireAuth(request, env);
        if (authErr) return authErr;
        const body = (await request.json()) as {
          alias: string; provider: string; apiKey: string; email: string; baseUrl?: string; note?: string; extra?: Record<string, string>;
        };
        const doId = env.ACCOUNT_MANAGER.idFromName("accounts");
        const stub = env.ACCOUNT_MANAGER.get(doId);
        await stub.register({
          alias: body.alias,
          provider: body.provider as any,
          apiKey: body.apiKey,
          email: body.email,
          baseUrl: body.baseUrl,
          status: "active",
          usage: { requestsUsed: 0, requestsLimit: 10000, tokensUsed: 0, tokensLimit: 1000000, lastReset: Date.now() },
          lastCheck: Date.now(),
          extra: body.extra,
          note: body.note,
        });
        return new Response(JSON.stringify({ ok: true, provider: body.provider, alias: body.alias }), {
          headers: { "Content-Type": "application/json", ...CORS_HEADERS },
        });
      }

      // ─────────────────────────────────────────────────────────────
      // GET /accounts/list?provider=X — list accounts (optionally filtered)
      // ─────────────────────────────────────────────────────────────
      if (url.pathname === "/accounts/list" && method === "GET") {
        const authErr = requireAuth(request, env);
        if (authErr) return authErr;
        const provider = url.searchParams.get("provider") || undefined;
        const doId = env.ACCOUNT_MANAGER.idFromName("accounts");
        const stub = env.ACCOUNT_MANAGER.get(doId);
        const accounts = await stub.list(provider);
        const safe = accounts.map((a: any) => ({
          alias: a.alias, provider: a.provider, status: a.status,
          usage: a.usage, email: a.email, baseUrl: a.baseUrl,
          lastCheck: a.lastCheck, note: a.note,
        }));
        return new Response(JSON.stringify({ count: safe.length, accounts: safe }), {
          headers: { "Content-Type": "application/json", ...CORS_HEADERS },
        });
      }

      // ─────────────────────────────────────────────────────────────
      // GET /accounts/rotate?provider=X&exclude=A — least-loaded account
      // ─────────────────────────────────────────────────────────────
      if (url.pathname === "/accounts/rotate" && method === "GET") {
        const authErr = requireAuth(request, env);
        if (authErr) return authErr;
        const provider = url.searchParams.get("provider");
        const exclude = url.searchParams.get("exclude") || undefined;
        if (!provider) return jsonError(400, "missing_provider", "provider query param required");
        const doId = env.ACCOUNT_MANAGER.idFromName("accounts");
        const stub = env.ACCOUNT_MANAGER.get(doId);
        const account = await stub.rotate(provider, exclude);
        if (!account) {
          return jsonError(404, "no_active_accounts", `No active ${provider} accounts`);
        }
        return new Response(JSON.stringify({
          alias: account.alias, provider: account.provider,
          baseUrl: account.baseUrl,
          usage: `${account.usage.requestsUsed}/${account.usage.requestsLimit}`,
        }), {
          headers: { "Content-Type": "application/json", ...CORS_HEADERS },
        });
      }

      // ─────────────────────────────────────────────────────────────
      // DELETE /accounts/remove?provider=X&alias=A — remove an account
      // ─────────────────────────────────────────────────────────────
      if (url.pathname === "/accounts/remove" && method === "DELETE") {
        const authErr = requireAuth(request, env);
        if (authErr) return authErr;
        const provider = url.searchParams.get("provider");
        const alias = url.searchParams.get("alias");
        if (!provider || !alias) {
          return jsonError(400, "missing_params", "provider and alias query params required");
        }
        const doId = env.ACCOUNT_MANAGER.idFromName("accounts");
        const stub = env.ACCOUNT_MANAGER.get(doId);
        await stub.remove(provider, alias);
        return new Response(JSON.stringify({ ok: true, provider, removed: alias }), {
          headers: { "Content-Type": "application/json", ...CORS_HEADERS },
        });
      }

      // ─────────────────────────────────────────────────────────────
      // SELF-UPGRADE — cloud proposes config/model changes, local applies
      // Uses DreamMemoryDO (SQLite, no daily write limits)
      // ─────────────────────────────────────────────────────────────
      // POST /upgrade/propose — propose a config/model/script change
      if (url.pathname === "/upgrade/propose" && method === "POST") {
        const body = await request.json() as Record<string, unknown>;
        const id = body.id as string || `upgrade-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
        const doId = env.DREAM_MEMORY.idFromName("upgrade-registry");
        const stub = env.DREAM_MEMORY.get(doId);
        const entry = {
          type: "upgrade",
          id,
          title: `[upgrade] ${body.target} — ${(body.reason as string || '').slice(0, 60)}`,
          description: JSON.stringify({ target: body.target, reason: body.reason, priority: body.priority, source: body.source, content: body.content, patches: body.patches, path: body.path }),
          priority: body.priority as number || 1,
          source: body.source as string || "cloud",
          created: new Date().toISOString(),
        };
        const resp = await stub.fetch("http://internal/store", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(entry),
        });
        const result = await resp.json();
        await log(env, ctx, "info", "upgrade_proposed", { id, target: body.target });
        return new Response(JSON.stringify(result), {
          headers: { "Content-Type": "application/json", ...CORS_HEADERS },
        });
      }

      // GET /upgrade/pending — local pulls pending upgrades
      if (url.pathname === "/upgrade/pending" && method === "GET") {
        const doId = env.DREAM_MEMORY.idFromName("upgrade-registry");
        const stub = env.DREAM_MEMORY.get(doId);
        const resp = await stub.fetch("http://internal/list?type=upgrade&limit=50", { method: "GET" });
        const data = await resp.json() as { entries: { id: string; title: string; description: string }[]; total: number };
        const upgrades = (data.entries || []).map((e) => {
          const desc = (() => { try { return JSON.parse(e.description); } catch { return { content: e.description }; } })();
          return { id: e.id, target: desc.target, reason: desc.reason, content: desc.content, patches: desc.patches, path: desc.path, priority: desc.priority, source: desc.source };
        });
        return new Response(JSON.stringify({ upgrades }), {
          headers: { "Content-Type": "application/json", ...CORS_HEADERS },
        });
      }

      // POST /upgrade/result — local reports upgrade result (delete from DO)
      if (url.pathname === "/upgrade/result" && method === "POST") {
        const body = await request.json() as { id: string; status: string; error?: string };
        const doId = env.DREAM_MEMORY.idFromName("upgrade-registry");
        const stub = env.DREAM_MEMORY.get(doId);
        await stub.fetch(`http://internal/delete/${body.id}`, { method: "DELETE" });
        await log(env, ctx, "info", "upgrade_result", { id: body.id, status: body.status });
        return new Response(JSON.stringify({ ok: true }), {
          headers: { "Content-Type": "application/json", ...CORS_HEADERS },
        });
      }

      // ─────────────────────────────────────────────────────────────
      // FULL DELEGATION — local↔cloud agent dispatch
      // Cloud→local: tasks stored in DreamMemoryDO, local polls + executes
      // Local→cloud: local calls /delegate/to-cloud, cloud dispatches to CloudOpencodeDO
      // ─────────────────────────────────────────────────────────────
      // POST /delegate/to-cloud — local sends a task to cloud agents
      if (url.pathname === "/delegate/to-cloud" && method === "POST") {
        const body = await request.json() as {
          agent_type: string;
          prompt: string;
          task_id?: string;
          chain?: { agent: string; prompt: string }[];
        };
        const taskId = body.task_id || `delegate-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
        const doId = env.OPENCODE.idFromName(`delegate-${taskId}`);
        const stub = env.OPENCODE.get(doId);
        let result: string | string[];
        if (body.chain && body.chain.length > 0) {
          result = await stub.runChain(body.chain);
        } else {
          result = await stub.dispatch(body.agent_type, body.prompt);
        }
        await log(env, ctx, "info", "delegate_to_cloud", { taskId, agent: body.agent_type });
        return new Response(JSON.stringify({ ok: true, task_id: taskId, result }), {
          headers: { "Content-Type": "application/json", ...CORS_HEADERS },
        });
      }

      // POST /delegate/to-local — cloud stores a task for local to execute
      if (url.pathname === "/delegate/to-local" && method === "POST") {
        const body = await request.json() as {
          target: string;
          action: string;
          params: Record<string, unknown>;
          task_id?: string;
          priority?: number;
        };
        const taskId = body.task_id || `local-task-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
        const doId = env.DREAM_MEMORY.idFromName("delegation-queue");
        const stub = env.DREAM_MEMORY.get(doId);
        const entry = {
          type: "delegation",
          id: taskId,
          title: `[delegation] ${body.target} — ${body.action}`,
          description: JSON.stringify({ target: body.target, action: body.action, params: body.params }),
          priority: body.priority || 1,
          source: "cloud",
          created: new Date().toISOString(),
        };
        const resp = await stub.fetch("http://internal/store", {
          method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(entry),
        });
        const result = await resp.json();
        await log(env, ctx, "info", "delegate_to_local", { taskId, target: body.target, action: body.action });
        return new Response(JSON.stringify({ ok: true, task_id: taskId, stored: result }), {
          headers: { "Content-Type": "application/json", ...CORS_HEADERS },
        });
      }

      // GET /delegate/pending — local polls for pending cloud→local tasks
      if (url.pathname === "/delegate/pending" && method === "GET") {
        const doId = env.DREAM_MEMORY.idFromName("delegation-queue");
        const stub = env.DREAM_MEMORY.get(doId);
        const resp = await stub.fetch("http://internal/list?type=delegation&limit=20", { method: "GET" });
        const data = await resp.json() as { entries: { id: string; title: string; description: string }[] };
        const tasks = (data.entries || []).map((e) => {
          const desc = (() => { try { return JSON.parse(e.description); } catch { return e.description; } })();
          return { task_id: e.id, ...desc };
        });
        return new Response(JSON.stringify({ tasks }), {
          headers: { "Content-Type": "application/json", ...CORS_HEADERS },
        });
      }

      // POST /delegate/result — local reports delegation result back to cloud
      if (url.pathname === "/delegate/result" && method === "POST") {
        const body = await request.json() as { task_id: string; status: string; result?: string; error?: string };
        const doId = env.DREAM_MEMORY.idFromName("delegation-queue");
        const stub = env.DREAM_MEMORY.get(doId);
        await stub.fetch(`http://internal/delete/${body.task_id}`, { method: "DELETE" });
        // Store result as a completed delegation entry (type=delegation_done) for traceability
        const resultEntry = {
          type: "delegation_done",
          id: `done-${body.task_id}`,
          title: `[done] ${body.status}`,
          description: JSON.stringify(body),
          priority: 0,
          source: "local",
          created: new Date().toISOString(),
        };
        await stub.fetch("http://internal/store", {
          method: "POST", headers: { "Content-Type": "application/json" },
          body: JSON.stringify(resultEntry),
        });
        await log(env, ctx, "info", "delegate_result", { task_id: body.task_id, status: body.status });
        return new Response(JSON.stringify({ ok: true }), {
          headers: { "Content-Type": "application/json", ...CORS_HEADERS },
        });
      }

      // ─────────────────────────────────────────────────────────────
            // GET /delegate/result — cloud readback: fetch a completed delegation result
      if (url.pathname === "/delegate/result" && method === "GET") {
        const tid = url.searchParams.get("task_id") || "";
        if (!tid) {
          return new Response(JSON.stringify({ ok: false, error: "task_id required" }), {
            status: 400, headers: { "Content-Type": "application/json", ...CORS_HEADERS },
          });
        }
        const doId = env.DREAM_MEMORY.idFromName("delegation-queue");
        const stub = env.DREAM_MEMORY.get(doId);
        const resp = await stub.fetch(`http://internal/recall/done-${tid}`, { method: "GET" });
        const entry: any = await resp.json();
        if (entry && entry.type === "delegation_done") {
          let desc: unknown = entry.description;
          try { desc = JSON.parse(entry.description); } catch { /* keep raw */ }
          return new Response(JSON.stringify({ ok: true, task_id: tid, ...(desc as object) }), {
            headers: { "Content-Type": "application/json", ...CORS_HEADERS },
          });
        }
        return new Response(JSON.stringify({ ok: false, error: "not_found", hint: "run POST /delegate/result first" }), {
          status: 404, headers: { "Content-Type": "application/json", ...CORS_HEADERS },
        });
      }

      // GET /delegate/history — cloud readback: recent completed delegation results
      if (url.pathname === "/delegate/history" && method === "GET") {
        const limit = parseInt(url.searchParams.get("limit") || "20");
        const doId = env.DREAM_MEMORY.idFromName("delegation-queue");
        const stub = env.DREAM_MEMORY.get(doId);
        const resp = await stub.fetch(`http://internal/list?type=delegation_done&limit=${Math.min(limit, 100)}&offset=0`, { method: "GET" });
        const data = await resp.json() as { entries: { id: string; title: string; description: string; created: string }[] };
        const entries = (data.entries || []).map((e) => {
          let desc: unknown = e.description;
          try { desc = JSON.parse(e.description); } catch { /* keep raw */ }
          return { id: e.id.replace(/^done-/, ""), title: e.title, created: e.created, result: desc };
        });
        return new Response(JSON.stringify({ ok: true, count: entries.length, entries }), {
          headers: { "Content-Type": "application/json", ...CORS_HEADERS },
        });
      }

// SYNC MATRIX — bidirectional sync between local AI-Ops & CPW
      // ─────────────────────────────────────────────────────────────
      // POST /sync/config — receive config from local (rule.md, dynamic-models, opencode.jsonc)
      if (url.pathname === "/sync/config" && method === "POST") {
        const body = await request.json() as { items: { type: string; key: string; value: string }[] };
        const results: { key: string; ok: boolean }[] = [];
        for (const item of body.items) {
          try {
            const kvKey = `sync:config:${item.type}:${item.key}`;
            if (env.CACHE_KV) {
              await env.CACHE_KV.put(kvKey, item.value, { expirationTtl: 86400 * 30 });
            }
            results.push({ key: `${item.type}/${item.key}`, ok: true });
          } catch (e) {
            results.push({ key: `${item.type}/${item.key}`, ok: false });
          }
        }
        return new Response(JSON.stringify({ synced: results.length, results }), {
          headers: { "Content-Type": "application/json", ...CORS_HEADERS },
        });
      }

      // POST /tg/send — Telegram relay via Cloudflare edge. This box and the
      // termux twin cannot reach api.telegram.org directly (egress TLS-block);
      // the worker (CF-edge) CAN, so all telegram sends round-matrix through here.
      if (url.pathname === "/tg/send" && method === "POST") {
        try {
          const body = await request.json() as { chat_id?: string; text?: string; token?: string };
          const chat = body.chat_id || "6663994526";
          const text = String(body.text || "ping").slice(0, 4000);
          const token = body.token || env.TELEGRAM_BOT_TOKEN || "";
          if (!token) {
            return new Response(JSON.stringify({ ok: false, error: "no tg token" }), {
              status: 400, headers: { "Content-Type": "application/json", ...CORS_HEADERS },
            });
          }
          const tg = await fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ chat_id: chat, text }),
          });
          const raw = await tg.text();
          return new Response(raw, { status: tg.status, headers: { "Content-Type": "application/json", ...CORS_HEADERS } });
        } catch (e: any) {
          return new Response(JSON.stringify({ ok: false, error: String(e?.message || e) }), {
            status: 500, headers: { "Content-Type": "application/json", ...CORS_HEADERS },
          });
        }
      }

      // GET /sync/config — retrieve config from cloud
      if (url.pathname === "/sync/config" && method === "GET") {
        const type = url.searchParams.get("type") || "";
        const key = url.searchParams.get("key") || "";
        if (!type || !key) {
          return new Response(JSON.stringify({ error: "Need ?type=&key=" }), {
            status: 400, headers: { "Content-Type": "application/json", ...CORS_HEADERS },
          });
        }
        const kvKey = `sync:config:${type}:${key}`;
        let value: string | null = null;
        if (env.CACHE_KV) {
          value = await env.CACHE_KV.get(kvKey);
        }
        if (!value) {
          return new Response(JSON.stringify({ found: false }), {
            headers: { "Content-Type": "application/json", ...CORS_HEADERS },
          });
        }
        return new Response(JSON.stringify({ found: true, type, key, value }), {
          headers: { "Content-Type": "application/json", ...CORS_HEADERS },
        });
      }

      // POST /sync/models — push local model registry to cloud
      if (url.pathname === "/sync/models" && method === "POST") {
        const body = await request.json() as { models: { id: string; provider: string; cost: { input: number; output: number }; context?: number }[] };
        if (env.CACHE_KV) {
          await env.CACHE_KV.put("sync:models:registry", JSON.stringify(body.models), { expirationTtl: 86400 * 7 });
        }
        // Also register each model in ModelRegistryDO
        const doId = env.MODEL_REGISTRY.idFromName("sync-registry");
        const stub = env.MODEL_REGISTRY.get(doId);
        const regResp = await stub.fetch("http://internal/bulk-register", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body.models),
        });
        const regResult = await regResp.json();
        return new Response(JSON.stringify({
          synced: body.models.length,
          registered: regResult,
        }), { headers: { "Content-Type": "application/json", ...CORS_HEADERS } });
      }

      // GET /sync/models — get cloud model registry
      if (url.pathname === "/sync/models" && method === "GET") {
        let registry: any[] = [];
        if (env.CACHE_KV) {
          const cached = await env.CACHE_KV.get("sync:models:registry");
          if (cached) registry = JSON.parse(cached);
        }
        // Also add Workers AI models
        const cloudModels = Object.entries(MODELS).map(([name, cfId]) => ({
          id: name, provider: "cloudflare-workers-ai", cloudflareId: cfId, cost: { input: 0, output: 0 },
        }));
        return new Response(JSON.stringify({
          modelCount: registry.length + cloudModels.length,
          local: registry,
          cloudflare: cloudModels,
        }), { headers: { "Content-Type": "application/json", ...CORS_HEADERS } });
      }

      // POST /sync/memory — push local sovereign memory entries to cloud
      if (url.pathname === "/sync/memory" && method === "POST") {
        const body = await request.json() as { entries: { id: string; title: string; content: string; tags?: string; created_at?: number }[] };
        const doId = env.DREAM_MEMORY.idFromName("sync-memory");
        const stub = env.DREAM_MEMORY.get(doId);
        const results: { id: string; ok: boolean }[] = [];
        for (const entry of body.entries) {
          try {
            const resp = await stub.fetch("http://internal/store", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ id: entry.id, title: entry.title, content: entry.content, tags: entry.tags || "", created_at: entry.created_at || Date.now() }),
            });
            results.push({ id: entry.id, ok: resp.ok });
          } catch {
            results.push({ id: entry.id, ok: false });
          }
        }
        return new Response(JSON.stringify({ synced: results.length, results }), {
          headers: { "Content-Type": "application/json", ...CORS_HEADERS },
        });
      }

      // GET /sync/memory — retrieve memory entries from cloud
      if (url.pathname === "/sync/memory" && method === "GET") {
        const limit = parseInt(url.searchParams.get("limit") || "50");
        const since = url.searchParams.get("since") || "0";
        const doId = env.DREAM_MEMORY.idFromName("sync-memory");
        const stub = env.DREAM_MEMORY.get(doId);
        const resp = await stub.fetch(`http://internal/list?limit=${limit}&since=${since}`, { method: "GET" });
        const data = await resp.json();
        return new Response(JSON.stringify(data), {
          headers: { "Content-Type": "application/json", ...CORS_HEADERS },
        });
      }

      // GET /sync/health — cloud health for local monitoring
      if (url.pathname === "/sync/health" && method === "GET") {
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
            } catch {
              return { shard, peers: 0, queueDepth: -1 };
            }
          }),
        );
        // Check Workers AI health
        let workersAiOk = false;
        try {
          const test = await env.AI.run("@cf/meta/llama-3.2-1b-instruct", { messages: [{ role: "user", content: "ping" }], max_tokens: 1 }) as any;
          workersAiOk = !!(test?.response || test?.choices);
        } catch { workersAiOk = false; }
        return new Response(JSON.stringify({
          status: "operational",
          shards: shardResults,
          workersAi: workersAiOk,
          models: Object.keys(MODELS).length,
          agents: Object.keys(AGENT_ROUTES).length,
          timestamp: Date.now(),
        }), { headers: { "Content-Type": "application/json", ...CORS_HEADERS } });
      }

      // ─────────────────────────────────────────────────────────────
      // POST /dream/store — store a dream memory entry
      // ─────────────────────────────────────────────────────────────
      if (url.pathname === "/dream/store" && method === "POST") {
        const doId = env.DREAM_MEMORY.idFromName("dream-memory");
        const stub = env.DREAM_MEMORY.get(doId);
        const resp = await stub.fetch("http://internal/store", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: request.body,
        });
        const data = await resp.json();
        return new Response(JSON.stringify(data), {
          status: resp.status,
          headers: { "Content-Type": "application/json", ...CORS_HEADERS },
        });
      }

      // ─────────────────────────────────────────────────────────────
      // GET /dream/list — list dream entries
      // ─────────────────────────────────────────────────────────────
      if (url.pathname === "/dream/list" && method === "GET") {
        const doId = env.DREAM_MEMORY.idFromName("dream-memory");
        const stub = env.DREAM_MEMORY.get(doId);
        const resp = await stub.fetch(
          `http://internal/list${url.search}`,
          { method: "GET" },
        );
        const data = await resp.json();
        return new Response(JSON.stringify(data), {
          headers: { "Content-Type": "application/json", ...CORS_HEADERS },
        });
      }

      // ─────────────────────────────────────────────────────────────
      // GET /dream/stats — dream memory statistics
      // ─────────────────────────────────────────────────────────────
      if (url.pathname === "/dream/stats" && method === "GET") {
        const doId = env.DREAM_MEMORY.idFromName("dream-memory");
        const stub = env.DREAM_MEMORY.get(doId);
        const resp = await stub.fetch("http://internal/stats", { method: "GET" });
        const data = await resp.json();
        return new Response(JSON.stringify(data), {
          headers: { "Content-Type": "application/json", ...CORS_HEADERS },
        });
      }

      // ─────────────────────────────────────────────────────────────
      // GET /dream/recall/:id — recall a specific dream entry
      // ─────────────────────────────────────────────────────────────
      const dreamRecallMatch = url.pathname.match(/^\/dream\/recall\/(.+)$/);
      if (dreamRecallMatch && method === "GET") {
        const id = dreamRecallMatch[1];
        const doId = env.DREAM_MEMORY.idFromName("dream-memory");
        const stub = env.DREAM_MEMORY.get(doId);
        const resp = await stub.fetch(`http://internal/recall/${id}`, {
          method: "GET",
        });
        const data = await resp.json();
        return new Response(JSON.stringify(data), {
          status: resp.status,
          headers: { "Content-Type": "application/json", ...CORS_HEADERS },
        });
      }

      // ─────────────────────────────────────────────────────────────
      // POST /dream/cycle — trigger dream engine cycle
      // ─────────────────────────────────────────────────────────────
      if (url.pathname === "/dream/cycle" && method === "POST") {
        const doId = env.DREAM_ENGINE.idFromName("eon-dream-engine");
        const stub = env.DREAM_ENGINE.get(doId);
        const resp = await stub.fetch("http://internal/dream/cycle", {
          method: "POST",
          signal: AbortSignal.timeout(120000),
        });
        const data = await resp.json();
        return new Response(JSON.stringify(data), {
          status: resp.status,
          headers: { "Content-Type": "application/json", ...CORS_HEADERS },
        });
      }

      // ─────────────────────────────────────────────────────────────
      // GET /dream/insights — list dream engine insights
      // ─────────────────────────────────────────────────────────────
      if (url.pathname === "/dream/insights" && method === "GET") {
        const doId = env.DREAM_ENGINE.idFromName("eon-dream-engine");
        const stub = env.DREAM_ENGINE.get(doId);
        const resp = await stub.fetch("http://internal/dream/list", { method: "GET" });
        const data = await resp.json();
        return new Response(JSON.stringify(data), {
          headers: { "Content-Type": "application/json", ...CORS_HEADERS },
        });
      }

      // ─────────────────────────────────────────────────────────────
      // GET /upgrade/list — list dream engine upgrade proposals
      // ─────────────────────────────────────────────────────────────
      if (url.pathname === "/upgrade/list" && method === "GET") {
        const doId = env.DREAM_ENGINE.idFromName("eon-dream-engine");
        const stub = env.DREAM_ENGINE.get(doId);
        const resp = await stub.fetch(
          `http://internal/upgrade/list${url.search}`,
          { method: "GET" },
        );
        const data = await resp.json();
        return new Response(JSON.stringify(data), {
          headers: { "Content-Type": "application/json", ...CORS_HEADERS },
        });
      }

      // ─────────────────────────────────────────────────────────────
      // POST /web-agent — autonomous web agent (fetch + parse any URL)
      // ─────────────────────────────────────────────────────────────
      if (url.pathname === "/web-agent" && method === "POST") {
        const { url: targetUrl, action = "fetch", selector, query } = await request.json() as {
          url: string;
          action?: string;    // fetch | search | extract
          selector?: string;  // CSS selector for extract action
          query?: string;     // search query
        };

        if (!targetUrl && action !== "search") {
          return new Response(JSON.stringify({ error: "url is required" }), {
            status: 400, headers: { "Content-Type": "application/json", ...CORS_HEADERS },
          });
        }

        const startTime = Date.now();

        try {
          const fetchResp = await fetch(targetUrl || `https://lite.duckduckgo.com/lite/?q=${encodeURIComponent(query || "")}`, {
            headers: {
              "User-Agent": "EON-AI-CPW/3.5 (autonomous-web-agent; +https://eon-p2p-ui.pages.dev)",
              "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
              "Accept-Language": "en-US,en;q=0.5",
            },
          });

          const html = await fetchResp.text();
          const contentType = fetchResp.headers.get("content-type") || "";
          const isHtml = contentType.includes("text/html") || html.trim().startsWith("<");

          // Extract structured data
          const title = isHtml ? extractTitle(html) : "";
          const text = isHtml ? extractText(html) : html.slice(0, 10000);
          const links = isHtml ? extractLinks(html, targetUrl) : [];
          const images = isHtml ? extractImages(html, targetUrl) : [];
          const meta = isHtml ? extractMeta(html) : {};

          const elapsed = Date.now() - startTime;

          return new Response(JSON.stringify({
            success: true,
            url: targetUrl,
            status: fetchResp.status,
            contentType,
            elapsed,
            title,
            text: text.slice(0, 50000),
            links: links.slice(0, 50),
            images: images.slice(0, 20),
            meta,
            size: html.length,
          }), {
            headers: { "Content-Type": "application/json", ...CORS_HEADERS },
          });
        } catch (err) {
          return new Response(JSON.stringify({
            success: false,
            url: targetUrl,
            error: err instanceof Error ? err.message : String(err),
          }), {
            status: 502,
            headers: { "Content-Type": "application/json", ...CORS_HEADERS },
          });
        }
      }

      // ─────────────────────────────────────────────────────────────
      // Tunnel relay bridge (reconciled from live)
      // ─────────────────────────────────────────────────────────────
      if (url.pathname === "/_tunnel") {
        const tAuth = request.headers.get("Authorization") || "";
        if (tAuth !== `Bearer ${env.AUTH_TOKEN}`)
          return new Response(JSON.stringify({ error: "unauthorized" }), {
            status: 401,
            headers: { "Content-Type": "application/json", ...CORS_HEADERS },
          });
        const tunnelId = url.searchParams.get("id") || "default";
        const doId = env.TUNNEL_RELAY.idFromName(`tunnel-${tunnelId}`);
        const stub = env.TUNNEL_RELAY.get(doId);
        return stub.fetch(request);
      }

      // ─────────────────────────────────────────────────────────────
      // ─────────────────────────────────────────────────────────────
      // Fleet DNS + self-heal deploy (reconciled from live)
      // ─────────────────────────────────────────────────────────────
      if (url.pathname === "/api/dns/resolve" && method === "GET") {
        const host = url.searchParams.get("host") || "";
        const FLEET_DNS: Record<string, { worker: string; ip: string; port: number }> = {
          "cloud.eon": { worker: "eon-p2p-cloud", ip: "192.168.1.73", port: 443 },
          "hub.eon": { worker: "eon-hub", ip: "192.168.1.73", port: 443 },
          "ide.eon": { worker: "eon-fleet-hub", ip: "192.168.1.73", port: 443 },
          "relay.eon": { worker: "home-cloud-relay", ip: "192.168.1.73", port: 443 },
          "sentinel.eon": { worker: "eon-cloud-sentinel", ip: "192.168.1.73", port: 443 },
          "alpha.eon": { worker: "eon-cloud-backup-alpha", ip: "192.168.1.73", port: 443 },
          "bravo.eon": { worker: "eon-cloud-backup-bravo", ip: "192.168.1.73", port: 443 },
          "sovereign.eon": { worker: "eon-sovereign-core", ip: "192.168.1.73", port: 443 },
          "dns.eon": { worker: "eon-p2p-cloud", ip: "192.168.1.73", port: 443 },
          "runtime.eon": { worker: "eon-p2p-cloud", ip: "192.168.1.73", port: 443 },
          "mesh.eon": { worker: "eon-p2p-cloud", ip: "192.168.1.73", port: 443 },
          "dream.eon": { worker: "eon-p2p-cloud", ip: "192.168.1.73", port: 443 },
          "vault.eon": { worker: "eon-p2p-cloud", ip: "192.168.1.73", port: 443 },
          "agent.eon": { worker: "eon-cloud-sentinel", ip: "192.168.1.73", port: 443 },
          "opencode.eon": { worker: "eon-fleet-hub", ip: "192.168.1.73", port: 443 }
        };
        const entry = FLEET_DNS[host] || FLEET_DNS[host.replace(/^eon-/, "").replace(/-cloud$/, "")];
        if (entry) {
          return new Response(JSON.stringify({
            host, ip: entry.ip, port: entry.port, worker: entry.worker,
            note: "Samsung reverse-proxies this to CF Worker"
          }), {
            headers: { "Content-Type": "application/json", ...CORS_HEADERS }
          });
        }
        if (host.endsWith(".eon") || host === "eon") {
          return new Response(JSON.stringify({
            host, ip: "192.168.1.73", port: 443, worker: "eon-p2p-cloud",
            note: "Wildcard *.eon → Samsung reverse proxy"
          }), {
            headers: { "Content-Type": "application/json", ...CORS_HEADERS }
          });
        }
        return new Response(JSON.stringify({ host, error: "not an .eon domain" }), {
          status: 404,
          headers: { "Content-Type": "application/json", ...CORS_HEADERS }
        });
      }
      if (url.pathname === "/api/fleet/dns-mapping" && method === "GET") {
        const FLEET_MAP: Record<string, string> = {
          "cloud.eon": "eon-p2p-cloud",
          "hub.eon": "eon-hub",
          "ide.eon": "eon-fleet-hub",
          "relay.eon": "home-cloud-relay",
          "sentinel.eon": "eon-cloud-sentinel",
          "alpha.eon": "eon-cloud-backup-alpha",
          "bravo.eon": "eon-cloud-backup-bravo",
          "sovereign.eon": "eon-sovereign-core",
          "dns.eon": "eon-p2p-cloud",
          "runtime.eon": "eon-p2p-cloud",
          "mesh.eon": "eon-p2p-cloud",
          "dream.eon": "eon-p2p-cloud",
          "vault.eon": "eon-p2p-cloud",
          "agent.eon": "eon-cloud-sentinel",
          "opencode.eon": "eon-fleet-hub"
        };
        return new Response(JSON.stringify({ mapping: FLEET_MAP, proxy_ip: "192.168.1.73" }, null, 2), {
          headers: { "Content-Type": "application/json", ...CORS_HEADERS }
        });
      }
      if (url.pathname === "/api/fleet/deploy" && method === "POST") {
        const authErr = requireAuth(request, env);
        if (authErr) return authErr;
        const acctId = (env as any).CF_ACCOUNT_ID || "87f71cf268d9b69fdfd2725afbfb4483";
        const apiToken = (env as any).CF_API_TOKEN;
        if (!apiToken) {
          return new Response(JSON.stringify({ error: "CF_API_TOKEN not configured" }), {
            status: 500,
            headers: { "Content-Type": "application/json", ...CORS_HEADERS }
          });
        }
        const fleet = [
          "eon-p2p-cloud", "eon-cloud-backup-alpha", "eon-cloud-backup-bravo", "eon-hub",
          "home-cloud-relay", "eon-cloud-sentinel", "eon-sovereign-core", "eon-fleet-hub"
        ];
        const bindings = [
          { name: "ACCOUNT_MANAGER", type: "durable_object_namespace", namespace_id: "cfc1763365e54abeb4abf59a7562165e" },
          { name: "AI", type: "ai", project: "<catalog>" },
          { name: "AUTH_TOKEN", type: "plain_text", text: env.AUTH_TOKEN || "" },
          { name: "CACHE_KV", type: "kv_namespace", namespace_id: "7e10e1e6f1cf4f9f9923d2175c8d1222" },
          { name: "CLOUD_AGENT", type: "durable_object_namespace", namespace_id: "bb20596fb739430698c707ef25179fac" },
          { name: "DREAM_ENGINE", type: "durable_object_namespace", namespace_id: "7708865c1f8f427b9edc1977cbe4e61e" },
          { name: "DREAM_MEMORY", type: "durable_object_namespace", namespace_id: "c3b45127965e4e3592fbbc503fdadc8d" },
          { name: "EDGE_SWARM", type: "durable_object_namespace", namespace_id: "ef6614f962974a34bb05e90e443d7210" },
          { name: "GLOBAL_SWARM", type: "durable_object_namespace", namespace_id: "2210d5d279724739a6ab058664f0b21d" },
          { name: "INCENTIVES", type: "durable_object_namespace", namespace_id: "259e1fc1d2d247818cf0b3cd35595a80" },
          { name: "MODEL_REGISTRY", type: "durable_object_namespace", namespace_id: "cae41f14e91d4deeb77b3fcfe61c33b2" },
          { name: "OPENCODE", type: "durable_object_namespace", namespace_id: "8a8a471adf4f4546b4376b32ef0be9cb" },
          { name: "P2P_SWARM", type: "durable_object_namespace", namespace_id: "752a57d71a4a4b359f4303e043f96e39" },
          { name: "RATE_LIMIT_KV", type: "kv_namespace", namespace_id: "1962bd59cfc2410ab72213154da4dd1a" },
          { name: "REGIONAL_SWARM", type: "durable_object_namespace", namespace_id: "48da944beadc4cbdad3913bf4fb72b5b" },
          { name: "REPUTATION", type: "durable_object_namespace", namespace_id: "f5099cad3f6e448687e43406e44f1abb" },
          { name: "TASK_QUEUE", type: "queue", queue_name: "eon-task-queue" },
          { name: "CF_API_TOKEN", type: "plain_text", text: apiToken },
          { name: "CF_ACCOUNT_ID", type: "plain_text", text: acctId }
        ];
        const myself = new URL(request.url).hostname.split(".")[0];
        const srcResp = await fetch(`https://api.cloudflare.com/client/v4/accounts/${acctId}/workers/scripts/${myself}`, {
          headers: { Authorization: `Bearer ${apiToken}` }
        });
        if (!srcResp.ok) {
          return new Response(JSON.stringify({ error: "failed to fetch own script", status: srcResp.status }), {
            status: 502,
            headers: { "Content-Type": "application/json", ...CORS_HEADERS }
          });
        }
        const srcCT = srcResp.headers.get("Content-Type") || "";
        const srcBody = new Uint8Array(await srcResp.arrayBuffer());
        let scriptBytes: Uint8Array;
        const boundaryMatch = srcCT.match(/boundary=(.+)/);
        if (boundaryMatch) {
          const boundary = boundaryMatch[1];
          const boundaryBytes = new TextEncoder().encode(`--${boundary}`);
          const headerEnd = new TextEncoder().encode("\r\n\r\n");
          const firstBound = findBytes(srcBody, boundaryBytes, 0);
          if (firstBound >= 0) {
            const afterBound = firstBound + boundaryBytes.length;
            const headerStart = findBytes(srcBody, headerEnd, afterBound);
            if (headerStart >= 0) {
              const contentStart = headerStart + headerEnd.length;
              const nextBound = findBytes(srcBody, boundaryBytes, contentStart);
              const contentEnd = nextBound > contentStart ? nextBound - 2 : srcBody.length;
              scriptBytes = srcBody.slice(contentStart, contentEnd);
            } else {
              scriptBytes = srcBody;
            }
          } else {
            scriptBytes = srcBody;
          }
        } else {
          scriptBytes = srcBody;
        }
        const bodyStr = await request.text().catch(() => "{}");
        const opts = bodyStr ? JSON.parse(bodyStr) : {};
        const targets: string[] = opts.targets || fleet;
        const results: Record<string, string> = {};
        for (let i = 0; i < targets.length; i += 2) {
          const batch = targets.slice(i, i + 2);
          await Promise.all(batch.map(async (name: string) => {
            try {
              const boundary = `----EON-DEPLOY-${name}-${Date.now()}`;
              const metadata = JSON.stringify({
                main_module: "worker.js",
                bindings,
                compatibility_date: "2026-07-25",
                compatibility_flags: ["nodejs_compat"]
              });
              const parts: Uint8Array[] = [];
              const enc = new TextEncoder();
              parts.push(enc.encode(`--${boundary}\r\nContent-Disposition: form-data; name="metadata"\r\nContent-Type: application/json\r\n\r\n${metadata}\r\n`));
              parts.push(enc.encode(`--${boundary}\r\nContent-Disposition: form-data; name="files"; filename="worker.js"\r\nContent-Type: application/javascript+module\r\n\r\n`));
              parts.push(new Uint8Array(scriptBytes));
              parts.push(enc.encode(`\r\n--${boundary}--\r\n`));
              const totalLen = parts.reduce((s, p) => s + p.length, 0);
              const combined = new Uint8Array(totalLen);
              let offset = 0;
              for (const part of parts) {
                combined.set(part, offset);
                offset += part.length;
              }
              const deployResp = await fetch(
                `https://api.cloudflare.com/client/v4/accounts/${acctId}/workers/scripts/${name}`,
                { method: "PUT", headers: { Authorization: `Bearer ${apiToken}`, "Content-Type": `multipart/form-data; boundary=${boundary}` }, body: combined }
              );
              const deployData = (await deployResp.json()) as { success: boolean; errors: unknown[] };
              await fetch(`https://api.cloudflare.com/client/v4/accounts/${acctId}/workers/scripts/${name}/subdomain`, {
                method: "POST",
                headers: { Authorization: `Bearer ${apiToken}`, "Content-Type": "application/json" },
                body: JSON.stringify({ enabled: true })
              }).catch(() => {});
              results[name] = deployData.success ? "OK" : `FAIL: ${JSON.stringify(deployData.errors)}`;
            } catch (e) {
              results[name] = `ERROR: ${String(e).slice(0, 100)}`;
            }
          }));
        }
        const ok = Object.values(results).filter((v) => v === "OK").length;
        return new Response(JSON.stringify({
          deployed: ok,
          total: targets.length,
          bundle: myself,
          results,
          message: `Fleet deploy: ${ok}/${targets.length} workers updated from ${myself}`
        }, null, 2), {
          headers: { "Content-Type": "application/json", ...CORS_HEADERS }
        });
      }

      // Sovereign dashboard + DNA identity pages (reconciled from live)
      // ─────────────────────────────────────────────────────────────
      if (url.pathname === "/" && method === "GET") {
        return renderSovereignDashboard(request, env);
      }
      if (url.pathname === "/dna" && method === "GET") {
        return renderDnaPage(request);
      }
      if (url.pathname === "/api/dna" && method === "GET") {
        return new Response(JSON.stringify(DNA_MANIFEST, null, 2), {
          headers: { "Content-Type": "application/json", ...CORS_HEADERS },
        });
      }
      if (url.pathname.startsWith("/api/remote/")) {
        return handleRemoteApi(request, env, url);
      }

      // ─────────────────────────────────────────────────────────────
      // Tunnel relay fallback — unmatched requests proxy through
      // TunnelRelayDO (reconciled from live)
      // ─────────────────────────────────────────────────────────────
      {
        const doId = env.TUNNEL_RELAY.idFromName("tunnel-default");
        const stub = env.TUNNEL_RELAY.get(doId);
        const proxyReq = new Request(request.url, {
          method: request.method,
          headers: request.headers,
          body:
            request.method !== "GET" && request.method !== "HEAD"
              ? request
              : undefined,
        });
        return await stub.fetch(proxyReq);
      }

      // ─────────────────────────────────────────────────────────────
      // Fallback — informational message
      // ─────────────────────────────────────────────────────────────
      return new Response(
        JSON.stringify({
          service: "EON P2P Cloud",
          version: "3.1",
          meaning: "worlds-most-powerful-AI-infrastructure: zero-cost, anonymous, self-healing, self-upgrading, bidirectional sync, full delegation between local bare-metal and cloud serverless across 9 channels + 2 delegation directions",
          delegation: {
            to_cloud: "POST /delegate/to-cloud  {agent_type,prompt}",
            to_local: "POST /delegate/to-local  {target,action,params}",
            pending: "GET /delegate/pending",
            result: "POST /delegate/result  {task_id,status,result?}",
          },
          self_upgrade: {
            propose: "POST /upgrade/propose  {target,content,reason,priority}",
            pending: "GET /upgrade/pending",
            result: "POST /upgrade/result  {id,status}",
          },
          sync_matrix: {
            push_config: "POST /sync/config  [{type,key,value}]",
            pull_config: "GET /sync/config?type=&key=",
            push_models: "POST /sync/models  {models:[{id,provider,cost}]}",
            pull_models: "GET /sync/models",
            push_memory: "POST /sync/memory  {entries:[{id,title,content}]}",
            pull_memory: "GET /sync/memory?limit=&since=",
            health: "GET /sync/health",
          },
          endpoints: {
            status: "GET /status",
            models: "GET /v1/models",
            chat: "POST /v1/chat/completions",
            dream_store: "POST /dream/store",
            dream_list: "GET /dream/list",
            dream_stats: "GET /dream/stats",
            dream_recall: "GET /dream/recall/:id",
            dream_cycle: "POST /dream/cycle (trigger autonomous reflection)",
            dream_insights: "GET /dream/insights (dream engine insights)",
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
            accounts_register: "POST /accounts/register  {alias,provider,apiKey,email}",
            accounts_list: "GET /accounts/list?provider=X",
            accounts_rotate: "GET /accounts/rotate?provider=X",
            accounts_remove: "DELETE /accounts/remove?provider=X&alias=A",
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

      // ── Dream Engine: autonomous reflection + skill synthesis ──
      try {
        const dreamId = env.DREAM_ENGINE.idFromName("eon-dream-engine");
        const dreamStub = env.DREAM_ENGINE.get(dreamId);
        const dreamResp = await dreamStub.fetch("http://internal/dream/cycle", {
          method: "POST",
          signal: AbortSignal.timeout(60000),
        });
        if (dreamResp.ok) {
          const dreamResult = (await dreamResp.json()) as { dreams: number; upgrades: number };
          await log(env, ctx, "info", "dream_cycle_completed", dreamResult);
        }
      } catch (e) {
        console.error("Dream engine cycle failed:", e);
        await log(env, ctx, "warn", "dream_cycle_failed", { error: String(e) });
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

// -----------------------------------------------------------------------------
// HTML extraction helpers for web agent
// -----------------------------------------------------------------------------
function extractTitle(html: string): string {
  const m = html.match(/<title[^>]*>([^<]+)<\/title>/i);
  return m ? m[1].trim() : "";
}

function extractText(html: string): string {
  // Remove script, style, svg, comments
  let clean = html.replace(/<script[^>]*>[\s\S]*?<\/script>/gi, " ");
  clean = clean.replace(/<style[^>]*>[\s\S]*?<\/style>/gi, " ");
  clean = clean.replace(/<svg[^>]*>[\s\S]*?<\/svg>/gi, " ");
  clean = clean.replace(/<!--[\s\S]*?-->/g, " ");
  // Replace tags with newlines
  clean = clean.replace(/<br\s*\/?>/gi, "\n");
  clean = clean.replace(/<\/p>/gi, "\n\n");
  clean = clean.replace(/<\/div>/gi, "\n");
  clean = clean.replace(/<\/h[1-6]>/gi, "\n\n");
  clean = clean.replace(/<\/li>/gi, "\n");
  clean = clean.replace(/<\/tr>/gi, "\n");
  // Strip remaining HTML tags
  clean = clean.replace(/<[^>]+>/g, " ");
  // Collapse whitespace
  clean = clean.replace(/&nbsp;/g, " ");
  clean = clean.replace(/&amp;/g, "&");
  clean = clean.replace(/&lt;/g, "<");
  clean = clean.replace(/&gt;/g, ">");
  clean = clean.replace(/&quot;/g, '"');
  clean = clean.replace(/\s+/g, " ").trim();
  return clean;
}

function extractLinks(html: string, baseUrl: string): { href: string; text: string }[] {
  const links: { href: string; text: string }[] = [];
  const regex = /<a\s+[^>]*href\s*=\s*["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi;
  let m: RegExpExecArray | null;
  while ((m = regex.exec(html)) !== null) {
    let href = m[1].trim();
    const innerText = m[2].replace(/<[^>]+>/g, "").trim();
    // Skip empty, javascript, anchor-only
    if (!href || href.startsWith("javascript:") || href.startsWith("#")) continue;
    // Resolve relative URLs
    try { href = new URL(href, baseUrl).href; } catch {}
    links.push({ href, text: innerText.slice(0, 100) });
  }
  return links;
}

function extractImages(html: string, baseUrl: string): { src: string; alt: string }[] {
  const images: { src: string; alt: string }[] = [];
  const regex = /<img\s+[^>]*src\s*=\s*["']([^"']+)["'][^>]*>/gi;
  let m: RegExpExecArray | null;
  while ((m = regex.exec(html)) !== null) {
    let src = m[1].trim();
    if (!src || src.startsWith("data:")) continue;
    const altMatch = m[0].match(/alt\s*=\s*["']([^"']*)["']/i);
    try { src = new URL(src, baseUrl).href; } catch {}
    images.push({ src, alt: altMatch ? altMatch[1] : "" });
  }
  return images;
}

function extractMeta(html: string): Record<string, string> {
  const meta: Record<string, string> = {};
  const regex = /<meta\s+[^>]*?(?:name|property)\s*=\s*["']([^"']+)["'][^>]*?content\s*=\s*["']([^"']+)["'][^>]*?\/?>/gi;
  let m: RegExpExecArray | null;
  while ((m = regex.exec(html)) !== null) {
    meta[m[1].toLowerCase()] = m[2].slice(0, 500);
  }
  // Also try reversed attribute order
  const regex2 = /<meta\s+[^>]*?content\s*=\s*["']([^"']+)["'][^>]*?(?:name|property)\s*=\s*["']([^"']+)["'][^>]*?\/?>/gi;
  while ((m = regex2.exec(html)) !== null) {
    meta[m[2].toLowerCase()] = m[1].slice(0, 500);
  }
  return meta;
}

// ─────────────────────────────────────────────────────────────
// Sovereign edge/dashboard/remote handlers (reconciled from LIVE bundle
// 2026-09-25 — these existed only in the deployed worker, never in git)
// ─────────────────────────────────────────────────────────────
function escHtml(value) {
  return String(value ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#39;");
}
function clampScore(n) {
  if (Number.isNaN(n)) return 100;
  return Math.max(0, Math.min(100, n));
}
function envInt(E, key, fallback = 0) {
   const raw = E[key];
   if (typeof raw !== "string" || raw.trim() === "") return fallback;
   const n = parseInt(raw, 10);
   return Number.isNaN(n) ? fallback : n;
 }
 const FALLBACK_MIRRORS = [
   { ref: "code-mirror", url: "https://github.com/eon-sovereign/eon-p2p-cloud", kind: "earthly", note: "code mirror" },
   { ref: "ui-mirror", url: "https://eon-p2p-ui.pages.dev", kind: "earthly", note: "UI mirror" }
 ];
 const DEFAULT_EDGE_ORGANS = [
   { name: "EONHub", port: 8201, label: "EON_MATRIX" },
   { name: "EONModels", port: 8090, label: "blind-proxy" },
   { name: "EON-Torch", port: 8089, label: "companion" },
   { name: "EON-Edge", port: 8088, label: "cloudbridge" }
 ];
 const COUNT_ORGANS = [
   { name: "EON-Memory", envKey: "EON_MEMORY_COUNT" },
   { name: "EON-Dream", envKey: "EON_DREAM_COUNT" },
   { name: "EON-Remote", envKey: "EON_REMOTE_COUNT" },
   { name: "EON-Wrangler", envKey: "EON_WRANGLER_COUNT" }
 ];
 function readEdgeOrgans(E) {
  const raw = E.EON_EDGE_PORTS;
  if (raw) {
    try {
      const v = JSON.parse(raw);
      if (Array.isArray(v)) {
        const out = [];
        for (const o of v) {
          const rec = o;
          if (rec && typeof rec.name === "string" && typeof rec.port === "number" && rec.port > 0) {
            out.push({ name: rec.name, port: rec.port, label: typeof rec.label === "string" ? rec.label : `:${rec.port}` });
          }
        }
        if (out.length) return out;
      } else if (v && typeof v === "object") {
        const out = [];
        for (const [name, port] of Object.entries(v)) {
          const p = parseInt(String(port), 10);
          if (p > 0) out.push({ name, port: p, label: name });
        }
        if (out.length) return out;
      }
    } catch {
    }
  }
  return DEFAULT_EDGE_ORGANS.map((o) => ({ ...o }));
}
async function probeEdgePort(port) {
  try {
    const res = await fetch(`http://127.0.0.1:${port}/`, {
      signal: AbortSignal.timeout(1500)
    });
    return res.status < 500;
  } catch {
    return false;
  }
}
function readEdgeStatus(E) {
  const raw = E.EON_EDGE_STATUS;
  if (!raw) return {};
  try {
    const v = JSON.parse(raw);
    if (v && typeof v === "object") {
      const out = {};
      for (const [k, val] of Object.entries(v)) out[k] = String(val);
      return out;
    }
    if (typeof v === "string") return { "*": v };
  } catch {
  }
  return {};
}
async function readMirrorRefs(E) {
  const out = [];
  try {
    if (E.EON_KV) {
      const idx = await E.EON_KV.get("remote:index");
      if (idx) {
        const parsed = JSON.parse(idx);
        if (Array.isArray(parsed)) {
          for (const m of parsed) if (m && typeof m === "object") out.push(m);
        }
      }
    }
  } catch {
  }
  if (out.length === 0 && E.EON_REMOTE_MIRRORS) {
    try {
      const parsed = JSON.parse(E.EON_REMOTE_MIRRORS);
      if (Array.isArray(parsed)) {
        for (const m of parsed) if (m && typeof m === "object") out.push(m);
      }
    } catch {
    }
  }
  return out.length ? out : FALLBACK_MIRRORS;
}
function formatWhen(iso) {
  if (!iso || iso.trim() === "") return "pending first run";
  const num = /^\d+$/.test(iso.trim()) ? parseInt(iso.trim(), 10) : NaN;
  const t = Number.isNaN(num) ? new Date(iso).getTime() : num * 1e3;
  if (Number.isNaN(t)) return escHtml(iso);
  return new Date(t).toISOString().replace("T", " ").slice(0, 16) + " UTC";
}
function badgeHtml(status) {
  const s = status === "up" ? "up" : status === "down" ? "down" : "partial";
  return `<span class="badge b-${s}">${s.toUpperCase()}</span>`;
}
async function renderSovereignDashboard(request, env) {
  const E = env;
  const url = new URL(request.url);
  let score = 100;
  if (E.EON_SOVEREIGNTY_SCORE) {
    const n = parseInt(E.EON_SOVEREIGNTY_SCORE, 10);
    if (!Number.isNaN(n)) score = n;
  }
  score = clampScore(score);
  const fallbackPct = 100 - score;
  const edgeOrgans = readEdgeOrgans(E);
  const edgeStatus = readEdgeStatus(E);
  const probeResults = await Promise.all(
    edgeOrgans.map(async (o) => ({ name: o.name, ok: await probeEdgePort(o.port) }))
  );
  const portCards = edgeOrgans.map((o) => {
    const probe = probeResults.find((p) => p.name === o.name);
    let status;
    let source;
    if (probe && probe.ok) {
      status = "up";
      source = "probe";
    } else {
      const fromEnv = edgeStatus[`127.0.0.1:${o.port}`] || edgeStatus[String(o.port)] || edgeStatus[o.name] || edgeStatus[o.name.toLowerCase()] || edgeStatus["*"];
      if (fromEnv) {
        status = fromEnv;
        source = "env";
      } else {
        status = "partial";
        source = "edge-default";
      }
    }
    return {
      name: o.name,
      badge: status === "up" ? "up" : status === "down" ? "down" : "partial",
      meta: `:${o.port} \xB7 ${o.label} \xB7 ${source}`,
      detail: `127.0.0.1:${o.port}`
    };
  });
  const countCards = COUNT_ORGANS.map((o) => {
    const n = envInt(E, o.envKey, 0);
    let detail = `${n} records`;
    if (o.name === "EON-Dream") detail = `${n} dreams \xB7 ${envInt(E, "EON_DREAM_INSIGHTS", 0)} insights \xB7 ${envInt(E, "EON_DREAM_MEMORIES", 0)} memories`;
    if (o.name === "EON-Memory") detail = `${n} memories`;
    if (o.name === "EON-Remote") detail = `${n} mirrors`;
    if (o.name === "EON-Wrangler") detail = `${n} kv rows`;
    return { name: o.name, badge: n > 0 ? "up" : "partial", meta: `count \xB7 env ${o.envKey}`, detail };
  });
  const organs = [...portCards, ...countCards];
  const mirrors = await readMirrorRefs(E);
  const mirrorRows = mirrors.length ? mirrors.slice(0, 12).map(
    (m) => `<div class="mirror"><code>${escHtml(m.ref || m.url || "?")}</code><span>${escHtml(m.url || m.note || "")}</span></div>`
  ).join("") : `<div class="muted">no mirrored earthly resources</div>`;
  const dreamLast = formatWhen(E.EON_DREAM_LAST);
  const dreamNext = formatWhen(E.EON_DREAM_NEXT);
  const dreamCycles = E.EON_DREAM_CYCLES ? escHtml(E.EON_DREAM_CYCLES) : "\u2014";
  const dreamPill = E.EON_DREAM_LAST && E.EON_DREAM_LAST.trim() !== "" ? "trained" : "pending first run";
  const origin = escHtml(E.EON_CLOUD || url.origin);
  const updatedAt = (/* @__PURE__ */ new Date()).toISOString().replace("T", " ").slice(0, 16) + " UTC";
  const fallbackLines = envInt(E, "EON_FALLBACK_COUNT", 0);
  const cardHtml = organs.map(
    (c) => `<div class="card">\n        <div class="org"><span class="dot dot-${c.badge}"></span><span class="org-name">${escHtml(c.name)}</span>${badgeHtml(c.badge)}</div>\n        <div class="meta">${escHtml(c.meta)}</div>\n        <div class="detail">${escHtml(c.detail)}</div>\n      </div>`
  ).join("");
  const html = `<!DOCTYPE html>\n<html lang="en">\n<head>\n<meta charset="utf-8">\n<meta name="viewport" content="width=device-width, initial-scale=1">\n<meta http-equiv="refresh" content="60">\n<title>EON Sovereign Cloud \u2014 Dashboard</title>\n<style>\n*{box-sizing:border-box;margin:0;padding:0}\nbody{background:#0b0e14;color:#cbd5e1;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;padding:24px;line-height:1.5}\n.wrap{max-width:1080px;margin:0 auto}\nheader{display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px;border-bottom:1px solid #232a3a;padding-bottom:14px;margin-bottom:18px}\nh1{font-size:20px;letter-spacing:2px;color:#e2e8f0}\nh1 span{color:#22c55e}\n.sub{color:#64748b;font-size:11px;text-transform:uppercase;letter-spacing:1px}\n.updated{color:#475569;font-size:11px;text-align:right}\nh2{font-size:12px;color:#e2e8f0;text-transform:uppercase;letter-spacing:2px;margin:22px 0 10px}\n.gauge-row{display:flex;gap:24px;align-items:center;flex-wrap:wrap;background:#12161f;border:1px solid #232a3a;border-radius:14px;padding:20px}\n.gauge{width:150px;height:150px;border-radius:50%;display:grid;place-items:center;flex:0 0 auto}\n.gauge-hole{width:108px;height:108px;background:#0b0e14;border-radius:50%;display:flex;flex-direction:column;align-items:center;justify-content:center}\n.gauge-num{font-size:30px;font-weight:800;color:#e2e8f0;line-height:1}\n.gauge-unit{font-size:9px;color:#94a3b8;text-transform:uppercase;letter-spacing:1px;margin-top:4px}\n.gauge-side{flex:1;min-width:230px}\n.gauge-title{font-size:13px;color:#e2e8f0;font-weight:700;text-transform:uppercase;letter-spacing:1.5px;margin-bottom:10px}\n.split{display:flex;height:12px;border-radius:6px;overflow:hidden;margin:10px 0 6px;border:1px solid #1e2534}\n.split-sovereign{background:#22c55e}\n.split-fallback{background:#64748b}\n.split-label{display:flex;justify-content:space-between;font-size:11px;color:#94a3b8}\n.legend{display:flex;gap:16px;font-size:11px;color:#94a3b8;margin-top:10px;flex-wrap:wrap;align-items:center}\n.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:10px}\n.card{background:#12161f;border:1px solid #232a3a;border-radius:12px;padding:12px 14px}\n.org{display:flex;align-items:center;gap:9px}\n.dot{width:9px;height:9px;border-radius:50%;flex:0 0 auto}\n.dot-up{background:#22c55e;box-shadow:0 0 8px #22c55e}\n.dot-down{background:#ef4444;box-shadow:0 0 8px #ef4444}\n.dot-partial{background:#f59e0b;box-shadow:0 0 8px #f59e0b}\n.org-name{font-weight:700;color:#e2e8f0;flex:1;font-size:14px}\n.badge{font-size:10px;font-weight:800;padding:3px 9px;border-radius:20px;letter-spacing:1px}\n.b-up{background:#14532d;color:#4ade80}\n.b-down{background:#7f1d1d;color:#f87171}\n.b-partial{background:#78350f;color:#fbbf24}\n.meta{color:#64748b;font-size:11px;margin-top:6px}\n.detail{color:#475569;font-size:11px;margin-top:2px}\n.panels{display:grid;grid-template-columns:1fr 1fr;gap:14px}\n.panel{background:#12161f;border:1px solid #232a3a;border-radius:12px;padding:14px}\n.panel h3{font-size:11px;color:#94a3b8;text-transform:uppercase;letter-spacing:1.5px;margin-bottom:10px;display:flex;justify-content:space-between;align-items:center}\n.pill{font-size:10px;color:#94a3b8;border:1px solid #232a3a;border-radius:20px;padding:2px 8px}\n.row{display:flex;justify-content:space-between;gap:12px;font-size:12px;padding:5px 0;border-bottom:1px dashed #1e2534}\n.row:last-child{border-bottom:none}\n.row span{color:#94a3b8}\n.row code{color:#e2e8f0}\n.mirror{display:flex;justify-content:space-between;gap:12px;font-size:12px;padding:5px 0;border-bottom:1px dashed #1e2534}\n.mirror code{color:#e2e8f0}\n.mirror span{color:#64748b;font-size:11px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:55%}\n.muted{color:#64748b;font-size:12px}\n.foot-note{color:#475569;font-size:10px;margin-top:10px}\nfooter{margin-top:22px;border-top:1px solid #232a3a;padding-top:12px;color:#475569;font-size:11px;display:flex;justify-content:space-between;flex-wrap:wrap;gap:8px}\ncode.inline{background:#12161f;border:1px solid #232a3a;border-radius:5px;padding:1px 6px;color:#94a3b8}\n@media(max-width:760px){.panels{grid-template-columns:1fr}.updated{text-align:left}}\n</style>\n</head>\n<body>\n<div class="wrap">\n<header>\n  <div>\n    <h1>EON <span>Sovereign</span> Cloud</h1>\n    <div class="sub">sovereign organs primary \xB7 earthly fallback only</div>\n  </div>\n  <div class="updated">updated <time>${updatedAt}</time><br><code class="inline">${origin}</code></div>\n</header>\n\n<section class="gauge-row">\n  <div class="gauge" style="background:conic-gradient(#22c55e 0% ${score}%, #2b3348 ${score}% 100%)">\n    <div class="gauge-hole"><div class="gauge-num">${score}</div><div class="gauge-unit">% sovereign</div></div>\n  </div>\n  <div class="gauge-side">\n    <div class="gauge-title">Sovereignty Score</div>\n    <div class="split"><div class="split-sovereign" style="width:${score}%"></div><div class="split-fallback" style="width:${fallbackPct}%"></div></div>\n    <div class="split-label"><span>${score}% sovereign</span><span>${fallbackPct}% earthly fallback</span></div>\n    <div class="legend">\n      <span><span class="dot dot-up"></span> sovereign</span>\n      <span><span class="dot dot-partial"></span> partial</span>\n      <span><span class="dot dot-down"></span> down</span>\n    </div>\n  </div>\n</section>\n\n<section>\n  <h2>Organs \xB7 8</h2>\n  <div class="grid">${cardHtml}</div>\n</section>\n\n<div class="panels">\n  <section class="panel">\n    <h3>Dream Status <span class="pill">${dreamPill}</span></h3>\n    <div class="row"><span>last training run</span><code>${dreamLast}</code></div>\n    <div class="row"><span>next scheduled</span><code>${dreamNext}</code></div>\n    <div class="row"><span>cycle count</span><code>${dreamCycles}</code></div>\n    <div class="row"><span>data</span><code>${envInt(E, "EON_DREAM_COUNT", 0)} dreams \xB7 ${envInt(E, "EON_DREAM_INSIGHTS", 0)} insights \xB7 ${envInt(E, "EON_DREAM_MEMORIES", 0)} memories</code></div>\n  </section>\n  <section class="panel">\n    <h3>Remote Access <span class="pill">earthly mirror</span></h3>\n    ${mirrorRows}\n    <div class="foot-note">mirrored earthly resources \u2014 fallback only, never primary \xB7 source: EON_KV remote:index / env / in-memory</div>\n  </section>\n</div>\n\n<footer>\n  <span>Earthly Fallback Layer: <code class="inline">~/.eon/fallback.log</code> (${fallbackLines} lines) \xB7 score source <code class="inline">~/.eon/sovereignty.json</code></span>\n  <span>EON Sovereign Cloud \xB7 dashboard v1.0</span>\n</footer>\n</div>\n</body>\n</html>`;
  return new Response(html, {
    headers: { "Content-Type": "text/html; charset=utf-8", ...CORS_HEADERS }
  });
}
function escDna(v) {
  return String(v ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}
function renderDnaPage(request) {
  const url = new URL(request.url);
  const updatedAt = (/* @__PURE__ */ new Date()).toISOString().replace("T", " ").slice(0, 16) + " UTC";
  const topology = DNA_MANIFEST.mesh_topology || {};
  const topologyRows = Object.entries(topology).map(
    ([k, v]) => `<div class="row"><span>${escDna(k)}</span><code>${escDna(v)}</code></div>`
  ).join("");
  const sources = DNA_MANIFEST.dna_sources || [];
  const sourceRows = sources.map(
    (s) => `<div class="mirror"><code>${escDna(s.machine)}</code><span>${escDna(s.path)}${s.components ? " \xB7 " + escDna(s.components.join(", ")) : ""}</span></div>`
  ).join("");
  const grid = DNA_MANIFEST.model_grid || {};
  const gridRows = Object.entries(grid).map(([k, v]) => `<div class="row"><span>${escDna(k)}</span><code>${escDna(v)}</code></div>`).join("");
  const brain = DNA_MANIFEST.core_brain || {};
  const brainRows = Object.entries(brain).map(([k, v]) => `<div class="row"><span>${escDna(k)}</span><code>${escDna(v)}</code></div>`).join("");
  const docs = DNA_MANIFEST.architecture_docs || [];
  const docRows = docs.map((d) => `<div class="row"><span>doc</span><code>${escDna(d)}</code></div>`).join("");
  const html = `<!DOCTYPE html>\n<html lang="en">\n<head>\n<meta charset="utf-8">\n<meta name="viewport" content="width=device-width, initial-scale=1">\n<title>EON Sovereign DNA \u2014 Mesh Identity</title>\n<style>\n*{box-sizing:border-box;margin:0;padding:0}\nbody{background:#0b0e14;color:#cbd5e1;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;padding:24px;line-height:1.5}\n.wrap{max-width:1080px;margin:0 auto}\nheader{display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px;border-bottom:1px solid #232a3a;padding-bottom:14px;margin-bottom:18px}\nh1{font-size:20px;letter-spacing:2px;color:#e2e8f0}\nh1 span{color:#22c55e}\n.sub{color:#64748b;font-size:11px;text-transform:uppercase;letter-spacing:1px}\n.updated{color:#475569;font-size:11px;text-align:right}\nh2{font-size:12px;color:#e2e8f0;text-transform:uppercase;letter-spacing:2px;margin:22px 0 10px}\n.panels{display:grid;grid-template-columns:1fr 1fr;gap:14px}\n.panel{background:#12161f;border:1px solid #232a3a;border-radius:12px;padding:14px}\n.panel h3{font-size:11px;color:#94a3b8;text-transform:uppercase;letter-spacing:1.5px;margin-bottom:10px}\n.row{display:flex;justify-content:space-between;gap:12px;font-size:12px;padding:5px 0;border-bottom:1px dashed #1e2534}\n.row:last-child{border-bottom:none}\n.row span{color:#94a3b8;flex:0 0 auto}\n.row code{color:#e2e8f0;text-align:right;word-break:break-all}\n.mirror{display:flex;justify-content:space-between;gap:12px;font-size:12px;padding:5px 0;border-bottom:1px dashed #1e2534}\n.mirror code{color:#e2e8f0}\n.mirror span{color:#64748b;font-size:11px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:60%}\n.badge{font-size:10px;font-weight:800;padding:3px 9px;border-radius:20px;letter-spacing:1px;background:#14532d;color:#4ade80}\nfooter{margin-top:22px;border-top:1px solid #232a3a;padding-top:12px;color:#475569;font-size:11px;display:flex;justify-content:space-between;flex-wrap:wrap;gap:8px}\n@media(max-width:760px){.panels{grid-template-columns:1fr}.updated{text-align:left}}\n</style>\n</head>\n<body>\n<div class="wrap">\n<header>\n  <div>\n    <h1>EON <span>DNA</span> \u2014 Sovereign Mesh Identity</h1>\n    <div class="sub">twin id ${escDna(DNA_MANIFEST.twin_id)} \xB7 ${escDna(DNA_MANIFEST.name)} \xB7 free forever</div>\n  </div>\n  <div class="updated">updated <time>${updatedAt}</time><br><code class="inline">${escDna(url.origin)}</code><br><span class="badge">24/7 global</span></div>\n</header>\n\n<div class="panels">\n  <section class="panel">\n    <h3>Mesh Topology</h3>\n    ${topologyRows || '<div class="row"><span>\u2014</span><code>none</code></div>'}\n  </section>\n  <section class="panel">\n    <h3>DNA Sources</h3>\n    ${sourceRows || '<div class="row"><span>\u2014</span><code>none</code></div>'}\n  </section>\n</div>\n\n<div class="panels">\n  <section class="panel">\n    <h3>Model Grid</h3>\n    ${gridRows || '<div class="row"><span>\u2014</span><code>none</code></div>'}\n  </section>\n  <section class="panel">\n    <h3>Core Brain</h3>\n    ${brainRows || '<div class="row"><span>\u2014</span><code>none</code></div>'}\n  </section>\n</div>\n\n<div class="panels">\n  <section class="panel">\n    <h3>Architecture Docs</h3>\n    ${docRows || '<div class="row"><span>\u2014</span><code>none</code></div>'}\n  </section>\n  <section class="panel">\n    <h3>Identity</h3>\n    <div class="row"><span>twin_id</span><code>${escDna(DNA_MANIFEST.twin_id)}</code></div>\n    <div class="row"><span>name</span><code>${escDna(DNA_MANIFEST.name)}</code></div>\n    <div class="row"><span>created</span><code>${escDna(DNA_MANIFEST.created_at)}</code></div>\n    <div class="row"><span>created_by</span><code>${escDna(DNA_MANIFEST.created_by)}</code></div>\n    <div class="row"><span>purpose</span><code>${escDna(DNA_MANIFEST.purpose)}</code></div>\n  </section>\n</div>\n\n<footer>\n  <span>Public-safe mirror \u2014 auth tokens excluded \xB7 source of truth: local TWIN_MANIFEST.json</span>\n  <span>EON Sovereign DNA \xB7 v${escDna(DNA_MANIFEST.manifest_version)}</span>\n</footer>\n</div>\n</body>\n</html>`;
  return new Response(html, {
    headers: { "Content-Type": "text/html; charset=utf-8", ...CORS_HEADERS }
  });
}
var REMOTE_VERSION = "1.0.0";
var REMOTE_BODY_TRUNCATE = 2e4;
var REMOTE_CACHE_CAP = 1e5;
var REMOTE_MIRROR_CAP = 1e6;
var REMOTE_MEM_CACHE = /* @__PURE__ */ new Map();
var REMOTE_MEM_MIRRORS = /* @__PURE__ */ new Map();
var REMOTE_MEM_INDEX = [];
function remoteHash(str) {
  let h = 5381;
  for (let i = 0; i < str.length; i++) h = (h << 5) + h + str.charCodeAt(i) | 0;
  return (h >>> 0).toString(36);
}
function remoteTruncate(s, max) {
  if (!s) return "";
  return s.length > max ? s.slice(0, max) + "\u2026[truncated]" : s;
}
function remoteMemGet(key) {
  if (key.startsWith("remote:cache:")) return REMOTE_MEM_CACHE.get(key.slice(13)) ?? null;
  if (key === "remote:mirror:index") return REMOTE_MEM_INDEX.length ? [...REMOTE_MEM_INDEX] : null;
  if (key.startsWith("remote:mirror:")) return REMOTE_MEM_MIRRORS.get(key.slice(14)) ?? null;
  return null;
}
function remoteMemSet(key, val) {
  if (key.startsWith("remote:cache:")) REMOTE_MEM_CACHE.set(key.slice(13), val);
  else if (key === "remote:mirror:index") {
    REMOTE_MEM_INDEX.length = 0;
    REMOTE_MEM_INDEX.push(...val);
  } else if (key.startsWith("remote:mirror:")) REMOTE_MEM_MIRRORS.set(key.slice(14), val);
}
async function remoteKvGet(kv, key) {
  if (!kv) return remoteMemGet(key);
  try {
    const v = await kv.get(key, "json") ?? null;
    return v !== null ? v : remoteMemGet(key);
  } catch {
    return remoteMemGet(key);
  }
}
async function remoteKvPut(kv, key, val) {
  if (!kv) {
    remoteMemSet(key, val);
    return;
  }
  try {
    await kv.put(key, JSON.stringify(val));
  } catch (e) {
    console.log("EON-Remote: KV write failed, memory fallback \u2014 " + (e && e.message ? e.message : String(e)));
    remoteMemSet(key, val);
  }
}
async function remoteCacheGet(kv, hash) {
  return remoteKvGet(kv, "remote:cache:" + hash);
}
async function remoteCachePut(kv, hash, entry) {
  await remoteKvPut(kv, "remote:cache:" + hash, entry);
}
async function remoteMirrorPut(kv, record) {
  await remoteKvPut(kv, "remote:mirror:" + record.ref, record);
  const records = await remoteKvGet(kv, "remote:mirror:index") || [];
  const i = records.findIndex((r) => r.ref === record.ref);
  if (i >= 0) records[i] = record;
  else records.push(record);
  await remoteKvPut(kv, "remote:mirror:index", records);
}
function remoteIsSafeTarget(raw) {
  let u;
  try {
    u = new URL(raw);
  } catch {
    return false;
  }
  if (u.protocol !== "http:" && u.protocol !== "https:") return false;
  const host = u.hostname.toLowerCase();
  if (host === "localhost" || host.endsWith(".localhost")) return false;
  if (host === "metadata.google.internal" || host === "instance-data") return false;
  if (host === "::1" || host === "[::1]" || host.startsWith("fc") || host.startsWith("fd") || host.startsWith("fe8")) return false;
  if (/^(127\.|10\.|192\.168\.|169\.254\.|0\.|100\.(6[4-9]|7[0-9]|12[0-7])\.)/.test(host)) return false;
  if (/^172\.(1[6-9]|2[0-9]|3[01])\./.test(host)) return false;
  if (/^[0-9]+$/.test(host.replace(/\./g, ""))) return false;
  return true;
}
function remoteJson(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" }
  });
}
async function remoteDoFetch(rawUrl) {
  const r = await fetch(rawUrl, {
    headers: {
      "User-Agent": "EON-Remote/1.0 (sovereign-edge relay)",
      "Accept": "*/*"
    },
    redirect: "follow"
  });
  const bytes = new Uint8Array(await r.arrayBuffer());
  let body = "";
  try {
    body = new TextDecoder("utf-8", { fatal: false }).decode(bytes);
  } catch {
    body = String.fromCharCode.apply(null, Array.from(bytes.subarray(0, 4096)));
  }
  return { status: r.status, body, contentType: r.headers.get("content-type") || "" };
}
async function handleRemoteWebProxy(request, env, url) {
  const raw = url.searchParams.get("url") || "";
  if (!raw) return remoteJson({ ok: false, error: "missing url param" }, 400);
  if (!remoteIsSafeTarget(raw)) return remoteJson({ ok: false, error: "unsafe target" }, 400);
  const method = (request.method || "GET").toUpperCase();
  const useMethod = ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"].indexOf(method) !== -1 ? method : "GET";
  const useBody = ["POST", "PUT", "PATCH", "DELETE"].indexOf(useMethod) !== -1 ? await request.text() : void 0;
  try {
    const r = await fetch(raw, {
      method: useMethod,
      body: useBody,
      headers: {
        "User-Agent": "EON-WebProxy/1.0 (sovereign-edge relay)",
        "Accept": "*/*",
        ...request.headers.get("authorization") ? { "Authorization": request.headers.get("authorization") } : {},
        ...request.headers.get("x-forwarded-for") ? { "X-Forwarded-For": request.headers.get("x-forwarded-for") } : {},
        ...useBody && request.headers.get("content-type") ? { "Content-Type": request.headers.get("content-type") } : {}
      },
      redirect: "follow"
    });
    const h = new Headers();
    const ct = r.headers.get("content-type");
    if (ct) h.set("content-type", ct);
    h.set("x-eon-webproxy", "sovereign-edge");
    h.set("access-control-allow-origin", "*");
    return new Response(r.body, { status: r.status, headers: h });
  } catch (e) {
    return remoteJson({ ok: false, error: "webproxy failed: " + (e && e.message ? e.message : String(e)) }, 502);
  }
}
async function handleRemoteFetch(request, env, url) {
  const kv = env.CACHE_KV;
  const raw = url.searchParams.get("url") || "";
  if (!raw) return remoteJson({ ok: false, error: "missing url param" }, 400);
  if (!remoteIsSafeTarget(raw)) return remoteJson({ ok: false, error: "unsafe target" }, 400);
  const hash = remoteHash(raw);
  const cached = await remoteCacheGet(kv, hash);
  if (cached && cached.body) {
    return remoteJson({
      ok: true,
      status: cached.status,
      cached: true,
      from_cache: true,
      source: "sovereign-edge",
      url: raw,
      fetched_at: cached.fetched_at,
      body: remoteTruncate(cached.body, REMOTE_BODY_TRUNCATE)
    });
  }
  try {
    const got = await remoteDoFetch(raw);
    const entry = {
      url: raw,
      status: got.status,
      body: got.body.slice(0, REMOTE_CACHE_CAP),
      contentType: got.contentType,
      fetched_at: Date.now()
    };
    await remoteCachePut(kv, hash, entry);
    return remoteJson({
      ok: true,
      status: got.status,
      cached: false,
      from_cache: false,
      source: "sovereign-edge",
      url: raw,
      fetched_at: entry.fetched_at,
      body: remoteTruncate(got.body, REMOTE_BODY_TRUNCATE)
    });
  } catch (e) {
    return remoteJson({ ok: false, error: "fetch failed: " + (e && e.message ? e.message : String(e)) }, 502);
  }
}
async function handleRemoteMirror(request, env, url) {
  const kv = env.CACHE_KV;
  const raw = url.searchParams.get("url") || "";
  let ref = url.searchParams.get("ref") || "";
  if (!raw) return remoteJson({ ok: false, error: "missing url param" }, 400);
  if (!remoteIsSafeTarget(raw)) return remoteJson({ ok: false, error: "unsafe target" }, 400);
  if (!ref) ref = "mirror-" + Date.now().toString(36) + "-" + remoteHash(raw);
  if (!/^[A-Za-z0-9._-]+$/.test(ref)) return remoteJson({ ok: false, error: "ref must match [A-Za-z0-9._-]" }, 400);
  try {
    const got = await remoteDoFetch(raw);
    const body = got.body.slice(0, REMOTE_MIRROR_CAP);
    const record = {
      ref,
      url: raw,
      fetched_at: Date.now(),
      size: body.length,
      status: got.status,
      contentType: got.contentType,
      body
    };
    await remoteMirrorPut(kv, record);
    return remoteJson({
      ok: true,
      ref,
      url: raw,
      size: body.length,
      stored: true,
      fetched_at: record.fetched_at,
      status: got.status
    });
  } catch (e) {
    return remoteJson({ ok: false, error: "mirror failed: " + (e && e.message ? e.message : String(e)) }, 502);
  }
}
async function handleRemoteList(request, env, url) {
  const kv = env.CACHE_KV;
  const records = await remoteKvGet(kv, "remote:mirror:index") || [];
  const mirrors = records.map(({ body, ...meta }) => meta);
  return remoteJson({
    ok: true,
    count: mirrors.length,
    mirrors,
    storage: kv ? "kv" : "in-memory-map",
    timestamp: Date.now()
  });
}
async function handleRemoteDiscover(request, env, url) {
  const kv = env.CACHE_KV;
  const index = await remoteKvGet(kv, "remote:mirror:index") || [];
  const out = {
    ok: true,
    organ: "eon-remote",
    version: REMOTE_VERSION,
    worker: "eon-p2p-cloud",
    host: url.origin,
    storage: kv ? "kv" : "in-memory-map",
    mirrored: index.length,
    endpoints: [
      { path: "/api/remote/fetch", method: "GET", params: ["url"], desc: "sovereign fetch of an earthly URL (cached)" },
      { path: "/api/remote/mirror", method: "GET", params: ["url", "ref"], desc: "snapshot an earthly resource under a named ref" },
      { path: "/api/remote/list", method: "GET", params: [], desc: "list mirrored resources" },
      { path: "/api/remote/discover", method: "GET", params: [], desc: "this self-discovery document" }
    ],
    timestamp: Date.now()
  };
  if (!kv) out.cached_entries = REMOTE_MEM_CACHE.size;
  return remoteJson(out);
}
async function handleRemoteApi(request, env, url) {
  const path = url.pathname;
  if (path === "/api/remote/fetch" && request.method === "GET") return handleRemoteFetch(request, env, url);
  if (path === "/api/remote/mirror" && request.method === "GET") return handleRemoteMirror(request, env, url);
  if (path === "/api/remote/list" && request.method === "GET") return handleRemoteList(request, env, url);
  if (path === "/api/remote/discover" && request.method === "GET") return handleRemoteDiscover(request, env, url);
  if (path === "/api/remote/webproxy") return handleRemoteWebProxy(request, env, url);
  return remoteJson({ ok: false, error: "unknown remote route: " + path }, 404);
}

function findBytes(haystack: string, needle: string, start = 0): number {
  for (let i = start; i <= haystack.length - needle.length; i++) {
    let match = true;
    for (let j = 0; j < needle.length; j++) {
      if (haystack[i + j] !== needle[j]) {
        match = false;
        break;
      }
    }
    if (match) return i;
  }
  return -1;
}
async function deriveE2EKey(authToken: string): Promise<CryptoKey> {
  const enc = new TextEncoder();
  const keyMaterial = await crypto.subtle.importKey(
    "raw",
    enc.encode(authToken),
    "PBKDF2",
    false,
    ["deriveKey"]
  );
  return crypto.subtle.deriveKey(
    { name: "PBKDF2", salt: enc.encode("eon-e2e-salt-v1"), iterations: 1e5, hash: "SHA-256" },
    keyMaterial,
    { name: "AES-GCM", length: 256 },
    false,
    ["encrypt", "decrypt"]
  );
}
async function e2eDecrypt(key: CryptoKey, payload: string): Promise<string> {
  const raw = Uint8Array.from(atob(payload), (c) => c.charCodeAt(0));
  const iv = raw.slice(0, 12);
  const ciphertext = raw.slice(12);
  const plainBuf = await crypto.subtle.decrypt({ name: "AES-GCM", iv }, key, ciphertext);
  return new TextDecoder().decode(plainBuf);
}
