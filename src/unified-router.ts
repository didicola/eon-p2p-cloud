// Unified AI Router — replaces blind proxy for cloud-native inference
// Tier 1: Cloudflare Workers AI (always free, always available, 16 models)
// Tier 2: Direct free API calls (pollinations, deepinfra, siliconflow — no local deps)
// Tier 3: P2P Swarm (GPU peers when connected)
// Tier 4: Legacy blind proxy via Tor (when available)

import type { Env } from "./types";

// ── Model family → Workers AI model mapping ────────────────────────────
// This mirrors the blind proxy's 180+ regex rules but simplified:
// any model in a family routes to the best Workers AI model for that family.
const FAMILY_ROUTES: Record<string, string> = {
  llama:     "@cf/meta/llama-3.3-70b-instruct-fp8-fast",
  "llama-4": "@cf/meta/llama-4-scout-17b-16e-instruct",
  qwen:      "@cf/qwen/qwen2.5-coder-32b-instruct",
  qwq:       "@cf/qwen/qwq-32b",
  mistral:   "@cf/mistralai/mistral-small-3.1-24b-instruct",
  codestral: "@cf/mistralai/mistral-small-3.1-24b-instruct",
  gemma:     "@cf/google/gemma-4-26b-a4b-it",
  nemotron:  "@cf/nvidia/nemotron-3-120b-a12b",
  deepseek:  "@cf/deepseek-ai/deepseek-r1-distill-qwen-32b",
  glm:       "@cf/zai-org/glm-5.2",
  kimi:      "@cf/moonshotai/kimi-k2.7-code",
  "gpt-oss": "@cf/openai/gpt-oss-120b",
  hermes:    "@cf/meta/llama-3.3-70b-instruct-fp8-fast",
  dolphin:   "@cf/meta/llama-3.3-70b-instruct-fp8-fast",
  phi:       "@cf/microsoft/phi-4",
  poolside:  "@cf/meta/llama-3.3-70b-instruct-fp8-fast",
  liquid:    "@cf/meta/llama-3.3-70b-instruct-fp8-fast",
};

const DEFAULT_MODEL = "@cf/meta/llama-3.3-70b-instruct-fp8-fast";

// ── Free API providers (no local deps, direct HTTP calls) ──────────────
interface FreeAPIProvider {
  name: string;
  url: string;
  // Map model family to this provider's model name
  modelMap: Record<string, string>;
  defaultModel: string;
  // Authorization header template (empty = no auth)
  auth?: string;
}

const FREE_API_PROVIDERS: FreeAPIProvider[] = [
  {
    name: "pollinations",
    url: "https://text.pollinations.ai/openai/v1/chat/completions",
    modelMap: {
      openai: "openai",
      mistral: "mistral",
      llama: "llama",
      qwen: "qwen-coder",
    },
    defaultModel: "openai",
  },
  {
    name: "deepinfra",
    url: "https://api.deepinfra.com/v1/openai/chat/completions",
    modelMap: {
      deepseek: "deepseek-ai/DeepSeek-V3-0324",
      llama: "meta-llama/Llama-3.3-70B-Instruct",
      qwen: "Qwen/Qwen3-235B-A35B",
      mistral: "mistralai/Mistral-Small-3.1-24B-Instruct",
      gemma: "google/gemma-4-31b-it",
    },
    defaultModel: "meta-llama/Llama-3.3-70B-Instruct",
  },
  {
    name: "siliconflow",
    url: "https://api.siliconflow.cn/v1/chat/completions",
    modelMap: {
      deepseek: "deepseek-ai/DeepSeek-V3-0324",
      llama: "meta-llama/Llama-3.3-70B-Instruct",
      qwen: "Qwen/Qwen3-235B-A35B",
    },
    defaultModel: "meta-llama/Llama-3.3-70B-Instruct",
  },
];

// ── Detect model family from model name ───────────────────────────────
function detectFamily(model: string): string {
  const m = model.toLowerCase();
  if (m.startsWith("llama-4")) return "llama-4";
  if (m.startsWith("llama")) return "llama";
  if (m.startsWith("qwen") || m.startsWith("qwq")) return m.startsWith("qwq") ? "qwq" : "qwen";
  if (m.startsWith("mistral") || m.startsWith("codestral")) return m.includes("codestral") ? "codestral" : "mistral";
  if (m.startsWith("gemma")) return "gemma";
  if (m.startsWith("nemotron") || m.includes("nvidia")) return "nemotron";
  if (m.startsWith("deepseek")) return "deepseek";
  if (m.startsWith("glm")) return "glm";
  if (m.startsWith("kimi")) return "kimi";
  if (m.includes("gpt-oss")) return "gpt-oss";
  if (m.includes("hermes")) return "hermes";
  if (m.includes("dolphin")) return "dolphin";
  if (m.includes("phi")) return "phi";
  if (m.includes("poolside")) return "poolside";
  if (m.includes("liquid")) return "liquid";
  // If model starts with @cf/, use directly (Workers AI model ID)
  if (m.startsWith("@cf/")) return "workers-ai-direct";
  return "default";
}

// ── Try Workers AI inference ──────────────────────────────────────────
async function tryWorkersAI(
  ai: Ai, system: string, prompt: string, model?: string
): Promise<string | null> {
  const family = model ? detectFamily(model) : "default";
  const workersModel = FAMILY_ROUTES[family] || DEFAULT_MODEL;

  const modelsToTry = [
    ...(model?.startsWith("@cf/") ? [model] : [workersModel]),
    DEFAULT_MODEL,
    "@cf/meta/llama-4-scout-17b-16e-instruct",
    "@cf/qwen/qwen2.5-coder-32b-instruct",
    "@cf/nvidia/nemotron-3-120b-a12b",
  ];

  const uniqueModels = [...new Set(modelsToTry)];
  for (const m of uniqueModels) {
    try {
      const res = await ai.run(m, {
        messages: [
          { role: "system", content: system || "You are a helpful assistant." },
          { role: "user", content: prompt },
        ],
        max_tokens: 2000,
      }) as { response?: string; choices?: { message?: { content?: string } }[] };

      const raw = res?.response ?? res?.choices?.[0]?.message?.content ?? "";
      if (raw) return raw;
    } catch (e) {
      console.warn(`[unified-router] Workers AI ${m} failed:`, e);
    }
  }
  return null;
}

// ── Try free API providers ────────────────────────────────────────────
async function tryFreeAPI(
  system: string, prompt: string, model?: string
): Promise<string | null> {
  const family = model ? detectFamily(model) : "default";

  for (const provider of FREE_API_PROVIDERS) {
    const providerModel = provider.modelMap[family] || provider.defaultModel;
    try {
      const headers: Record<string, string> = {
        "Content-Type": "application/json",
      };
      if (provider.auth) headers["Authorization"] = provider.auth;

      const res = await fetch(provider.url, {
        method: "POST",
        headers,
        body: JSON.stringify({
          model: providerModel,
          messages: [
            { role: "system", content: system || "You are a helpful assistant." },
            { role: "user", content: prompt },
          ],
          max_tokens: 2000,
        }),
        signal: AbortSignal.timeout(30_000),
      });

      if (res.ok) {
        const data = await res.json() as {
          choices?: { message?: { content?: string } }[];
        };
        const raw = data?.choices?.[0]?.message?.content ?? "";
        if (raw) return raw;
      }
    } catch (e) {
      console.warn(`[unified-router] Free API ${provider.name} failed:`, e);
    }
  }
  return null;
}

// ── Try P2P swarm peers ───────────────────────────────────────────────
async function tryP2PSwarm(
  env: Env, system: string, prompt: string, model?: string
): Promise<string | null> {
  try {
    const family = model ? detectFamily(model) : "default";
    const shard = ["llama", "qwen", "mistral", "gemma", "deepseek"].includes(family) ? family : "general";
    const doId = env.P2P_SWARM.idFromName(`eon-swarm-${shard}`);
    const stub = env.P2P_SWARM.get(doId);

    const resp = await stub.fetch("http://internal/dispatch", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        type: "inference",
        model: model || "llama-3.3-70b",
        messages: [
          { role: "system", content: system || "You are a helpful assistant." },
          { role: "user", content: prompt },
        ],
        max_tokens: 2000,
      }),
    });

    if (resp.ok) {
      const data = await resp.json() as { result?: string; content?: string };
      return data.result || data.content || null;
    }
  } catch (e) {
    console.warn(`[unified-router] P2P swarm failed:`, e);
  }
  return null;
}

// ── Main unified route function ───────────────────────────────────────
// Tiers: Workers AI → Free APIs → P2P Swarm → null
export async function routeInference(
  env: Env,
  system: string,
  prompt: string,
  model?: string,
): Promise<string | null> {
  // Tier 1: Workers AI (always free, always available)
  const t1 = await tryWorkersAI(env.AI, system, prompt, model);
  if (t1) return t1;

  // Tier 2: Free API providers (direct HTTP, no local deps)
  const t2 = await tryFreeAPI(system, prompt, model);
  if (t2) return t2;

  // Tier 3: P2P swarm (GPU peers)
  const t3 = await tryP2PSwarm(env, system, prompt, model);
  if (t3) return t3;

  return null;
}

// ── List all routable models ─────────────────────────────────────────
export function getRoutableModels(): { id: string; provider: string }[] {
  const models: { id: string; provider: string }[] = [];

  // Workers AI models (always available)
  models.push(
    { id: "llama-3.3-70b", provider: "workers-ai" },
    { id: "llama-4-scout", provider: "workers-ai" },
    { id: "qwen-coder-32b", provider: "workers-ai" },
    { id: "qwq-32b", provider: "workers-ai" },
    { id: "mistral-small-24b", provider: "workers-ai" },
    { id: "codestral", provider: "workers-ai" },
    { id: "gemma-4-26b", provider: "workers-ai" },
    { id: "nemotron-3-120b", provider: "workers-ai" },
    { id: "deepseek-r1-32b", provider: "workers-ai" },
    { id: "glm-5.2", provider: "workers-ai" },
    { id: "kimi-k2.7", provider: "workers-ai" },
    { id: "gpt-oss-120b", provider: "workers-ai" },
    { id: "phi-4", provider: "workers-ai" },
  );

  // Model families (routed to Workers AI via family map)
  for (const family of Object.keys(FAMILY_ROUTES)) {
    if (!models.find(m => m.id.startsWith(family))) {
      models.push({ id: `${family}*`, provider: "workers-ai" });
    }
  }

  // Free API models
  for (const provider of FREE_API_PROVIDERS) {
    for (const [family, modelName] of Object.entries(provider.modelMap)) {
      models.push({ id: `${modelName} (via ${provider.name})`, provider: provider.name });
    }
  }

  return models;
}
