export interface QueueTask {
  type: "inference" | "agent_dispatch" | "peer_heartbeat" | "verification";
  model?: string;
  messages?: { role: string; content: string }[];
  agent?: string;
  prompt?: string;
  peerId?: string;
  taskId?: string;
  requestId?: string;
  priority?: number;
}

export interface ChatRequest {
  model?: string;
  messages: { role: string; content: string }[];
  max_tokens?: number;
  stream?: boolean;
}

export interface Task {
  id: string;
  model: string;
  prompt: string;
  messages?: { role: string; content: string }[];
  created: number;
  claimed?: string;
  result?: string;
  done: boolean;
  attempts: number;
  priority?: "latency" | "cost" | "reliability";
  routingHint?: string;
}

export interface PeerCapability {
  peerId: string;
  models: string[];
  maxTokens: number;
  avgLatency: number;
  reliability: number;
  currentLoad: number;
  maxLoad: number;
  region: string;
  colo?: string;
  lastHeartbeat: number;
  version: string;
  protocol: "websocket" | "http";
  endpoint?: string;
}

export interface LogEntry {
  level: "info" | "warn" | "error";
  msg: string;
  data?: unknown;
  timestamp: number;
}

export interface ProviderRegistration {
  name: string;
  baseUrl: string;
  apiKey?: string;
  models: string[];
  pricing: { input: number; output: number };
  region: string;
  status: "active" | "inactive";
  reliability: number;
  totalRequests: number;
}

export interface CreditEntry {
  peerId: string;
  amount: number;
  reason: string;
  taskId: string;
  timestamp: number;
}

export interface CacheEntry {
  result: string;
  model: string;
  created: number;
  ttl: number;
  hits: number;
}

export interface RegionMetrics {
  region: string;
  peerCount: number;
  queueDepth: number;
  avgLatency: number;
  requestsPerMin: number;
  errorRate: number;
}

export interface VerificationRequest {
  taskId: string;
  prompt: string;
  peerResult: string;
  peerId: string;
  model: string;
}

export type ProviderType = "cloudflare" | "github" | "huggingface" | "openrouter" | "mistral" | "groq" | "cerebras" | "sambanova" | "gemini" | "anthropic" | "keylessai" | "pollinations" | "bazaarlink" | "nvidia" | "elevenlabs" | "replicate" | "together" | "deepinfra" | "fireworks" | "novita";

export interface ProviderUsage {
  requestsUsed: number;
  requestsLimit: number;
  tokensUsed: number;
  tokensLimit: number;
  lastReset: number;
}

export interface AccountRecord {
  alias: string;
  provider: ProviderType;
  apiKey: string;
  email: string;
  baseUrl?: string;
  status: "active" | "rate_limited" | "error";
  usage: ProviderUsage;
  lastCheck: number;
  extra?: Record<string, string>;
  note?: string;
}

export interface VerificationResponse {
  taskId: string;
  score: number;
  verified: boolean;
  confidence: number;
  method: "redundant" | "sanity_check";
}

export interface Env {
  AI: Ai;
  P2P_SWARM: DurableObjectNamespace;
  CLOUD_AGENT: DurableObjectNamespace;
  OPENCODE: DurableObjectNamespace;
  REPUTATION: DurableObjectNamespace;
  MODEL_REGISTRY: DurableObjectNamespace;
  INCENTIVES: DurableObjectNamespace;
  EDGE_SWARM: DurableObjectNamespace;
  REGIONAL_SWARM: DurableObjectNamespace;
  GLOBAL_SWARM: DurableObjectNamespace;
  ACCOUNT_MANAGER: DurableObjectNamespace;
  DREAM_MEMORY: DurableObjectNamespace;
  DREAM_ENGINE: DurableObjectNamespace;
  TASK_QUEUE: Queue<QueueTask>;
  LOG_BUCKET?: R2Bucket;
  CACHE_KV?: KVNamespace;
  RATE_LIMIT_KV?: KVNamespace;
  LOCAL_BLIND_PROXY?: string;
  LOCAL_BRIDGE_URL?: string;
  LOCAL_P2P_URL?: string;
  PAYMENT_PROVIDER?: Fetcher;
  TELEGRAM_BOT_TOKEN?: string;
  GH_TOKEN?: string;
  AUTH_TOKEN?: string;
}
