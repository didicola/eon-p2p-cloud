import { type Env, type PeerCapability } from "./types";

export const MODELS: Record<string, string> = {
  "llama-3.3-70b": "@cf/meta/llama-3.3-70b-instruct-fp8-fast",
  "gpt-oss-120b": "@cf/openai/gpt-oss-120b",
  "mistral-small": "@cf/mistralai/mistral-small-3.1-24b-instruct",
  "gemma-4": "@cf/google/gemma-4-26b-a4b-it",
  "nemotron-3": "@cf/nvidia/nemotron-3-120b-a12b",
  "deepseek-r1": "@cf/deepseek-ai/deepseek-r1-distill-qwen-32b",
  "glm-5.2": "@cf/zai-org/glm-5.2",
  "qwen-coder": "@cf/qwen/qwen2.5-coder-32b-instruct",
  "kimi-k2.7": "@cf/moonshotai/kimi-k2.7-code",
  "qwq-32b": "@cf/qwen/qwq-32b",
  "llama-4-scout": "@cf/meta/llama-4-scout-17b-16e-instruct",
  "gpt-oss-20b": "@cf/openai/gpt-oss-20b",
};

export const SWARM_SHARDS = ["llama", "qwen", "mistral", "gemma", "deepseek", "general"];

export interface FreeProvider {
  name: string;
  url: string;
  model: string;
}

export const FREE_PROVIDERS: FreeProvider[] = [
  { name: "pollinations", url: "https://text.pollinations.ai/openai/v1/chat/completions", model: "openai" },
  { name: "pollinations-2", url: "https://text.pollinations.ai/openai/v1/chat/completions", model: "mistral" },
  { name: "aiand-free", url: "https://api.aiand.com/v1/chat/completions", model: "qwen/qwen3.6-27b" },
];

export function getModelFamily(model: string): string {
  const family = model.split("-")[0] || "general";
  return SWARM_SHARDS.includes(family) ? family : "general";
}

export function getSwarmDO(env: Env, model: string): DurableObjectStub {
  const shard = getModelFamily(model);
  const doId = env.P2P_SWARM.idFromName(`eon-swarm-${shard}`);
  return env.P2P_SWARM.get(doId);
}

export function parseReply(raw: string): string {
  let reply = raw;
  if (reply.includes("</think>")) reply = reply.split("</think>")[1].trim();
  return reply;
}

export function shardForKey(key: string): string {
  let hash = 0;
  for (let i = 0; i < key.length; i++) {
    hash = ((hash << 5) - hash + key.charCodeAt(i)) | 0;
  }
  return SWARM_SHARDS[Math.abs(hash) % SWARM_SHARDS.length];
}
