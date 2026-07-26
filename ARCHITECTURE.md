# EON P2P Cloud — Complete Architecture Bible

> Version 3.0 — Deployed on Cloudflare Workers Edge (330+ cities)
> 17 source files, 5,302 lines TypeScript, 9 Durable Objects

---

## 1. SYSTEM OVERVIEW

eon-p2p-cloud is the world's first **edge-native, multi-provider, P2P AI inference marketplace**. It routes LLM requests through a 4-tier fallback chain, all at $0 cost, with stateful coordination across Cloudflare's global network.

### What It Solves

| Problem | Solution |
|---------|----------|
| Centralized AI APIs are expensive | Route through Workers AI (free) + cache + free APIs |
| P2P AI networks are slow (Bittensor: minutes) | Edge-native routing via DOs (<50ms overhead) |
| No one combines edge + P2P + multi-provider | 9-tier fallback chain, first of its kind |
| GPU peers can't be trusted | Redundant verification + reputation scoring |

---

## 2. FILE MAP

```
eon-p2p-cloud/
├── wrangler.jsonc              # Cloudflare Workers config — 9 DOs, Queue, KV, AI
├── src/
│   ├── index.ts                # Entry point: fetch(), queue(), scheduled() handlers (1,361 lines)
│   ├── types.ts                # All interfaces: Task, PeerCapability, Env, CacheEntry...
│   ├── models.ts               # Config: 12 Workers AI models, 6 shards, 3 free providers
│   ├── agents.ts               # 7 agent routes with system prompts
│   ├── cache.ts                # 3-tier cache: LRU → KV → fetchFunction
│   ├── peer-protocol.ts        # WebSocket upgrade + message protocol for GPU peers
│   ├── verification.ts         # Redundant verification + Workers AI sanity check
│   ├── do/
│   │   ├── p2p-swarm.ts        # PHASE 1 — Sharded task broker + capability registry + smart routing
│   │   ├── reputation.ts       # PHASE 1 — Sliding window peer reliability (1,000 entries/peer)
│   │   ├── cloud-opencode.ts   # PHASE 1 — Agent dispatch (7 agent types, Workers AI fallback)
│   │   ├── cloud-p2p-agent.ts  # PHASE 1 — Autonomous polling agent (adaptive interval)
│   │   ├── incentives.ts       # PHASE 2 — Credit tracking + redemption rate limiting
│   │   ├── model-registry.ts   # PHASE 3 — Provider marketplace registration + routing
│   │   ├── edge-swarm.ts       # PHASE 3 — Per-colo peer registry (lightweight, crash-recoverable)
│   │   ├── regional-swarm.ts   # PHASE 3 — Per-continent routing + full capability registry
│   │   └── global-swarm.ts     # PHASE 3 — Cross-region migration + global metrics aggregation
│   └── workflows/
│       └── swarm-scaler.ts     # PHASE 3 — Autoscaling Workflow (queue depth → recruitment signal)
```

---

## 3. ARCHITECTURE DIAGRAM

```
                     ┌──────────────────────────────────────────────┐
                     │          Cloudflare Edge (330 PoPs)          │
                     │                                              │
                     │  ┌────────────────────────────────────────┐  │
                     │  │           src/index.ts                 │  │
                     │  │  ┌──────────┬──────────┬────────────┐  │  │
                     │  │  │ fetch()  │ queue()  │ scheduled()│  │  │
                     │  │  └────┬─────┴────┬─────┴─────┬──────┘  │  │
                     │  └───────┼──────────┼───────────┼─────────┘  │
                     │          │          │           │            │
     ┌───────────────┼──────────┼──────────┼───────────┼──────────┐ │
     │  REQUEST FLOW │          │          │           │          │ │
     │               ▼          │          │           │          │ │
     │  ┌───────────────────┐   │          │           │           │ │
     │  │  Rate Limit (KV)  │   │          │           │           │ │
     │  └────────┬──────────┘   │          │           │           │ │
     │           ▼              │          │           │           │ │
     │  ┌───────────────────┐   │          │           │           │ │
     │  │   1. Workers AI   │───┼─ FAST    │           │           │ │
     │  └────────┬──────────┘   │          │           │           │ │
     │           ▼              │          │           │           │ │
     │  ┌───────────────────┐   │          │           │           │ │
     │  │   2. 3-Tier Cache │   │          │           │           │ │
     │  │  LRU → KV → Fetch │   │          │           │           │ │
     │  └────────┬──────────┘   │          │           │           │ │
     │           ▼              │          │           │           │ │
     │  ┌───────────────────┐   │          │           │           │ │
     │  │   3. Blind Proxy  │───┼─ MEDIUM  │           │           │ │
     │  └────────┬──────────┘   │          │           │           │ │
     │           ▼              │          │           │           │ │
     │  ┌───────────────────┐   │          │           │           │ │
     │  │   4. P2P SWARM    │◄──┼── QUEUE  │           │           │ │
     │  │   ┌─────────────┐ │   │          │           │           │ │
     │  │   │ 6 shards    │ │   │          │           │           │ │
     │  │   │ llama qwen  │ │   │          │           │           │ │
     │  │   │ mistral ... │ │   │          │           │           │ │
     │  │   └─────────────┘ │   │          │           │           │ │
     │  └───────────────────┘   │          │           │           │ │
     └──────────────────────────┼──────────┼───────────┼───────────┘ │
                                │          │           │             │
     ┌──────────────────────────┼──────────┼───────────┼───────────┐ │
     │  DO LAYER               │          │           │           │ │
     │                          ▼          ▼           ▼           │ │
     │  ┌─────────────────────────────────────────────────────┐    │ │
     │  │               9 Durable Objects                      │    │ │
     │  │                                                      │    │ │
     │  │  ┌──────────────┐  ┌──────────────┐  ┌────────────┐ │    │ │
     │  │  │ P2PSwarmDO   │  │ ReputationDO │  │ IncentiveDO│ │    │ │
     │  │  │ (6 shards)   │  │ (sliding     │  │ (credit    │ │    │ │
     │  │  │ task broker  │  │  window)     │  │  tracking) │ │    │ │
     │  │  └──────────────┘  └──────────────┘  └────────────┘ │    │ │
     │  │                                                      │    │ │
     │  │  ┌──────────────┐  ┌──────────────┐  ┌────────────┐ │    │ │
     │  │  │ EdgeSwarmDO  │  │RegionalSwarm │  │GlobalSwarm │ │    │ │
     │  │  │ (per-colo)   │  │ (per-cont)   │  │ (coord)    │ │    │ │
     │  │  └──────────────┘  └──────────────┘  └────────────┘ │    │ │
     │  │                                                      │    │ │
     │  │  ┌────────────────┐┌────────────────┐┌─────────────┐│    │ │
     │  │  │ CloudOpencodeDO││CloudP2PAgentDO ││ModelRegistry││    │ │
     │  │  │ (agent oracle) ││(auto poller)   ││(marketplace)││    │ │
     │  │  └────────────────┘└────────────────┘└─────────────┘│    │ │
     │  └─────────────────────────────────────────────────────┘    │ │
     └───────────────────────────────────────────────────────────┘ │
                                                                  │
     ┌──────────────────────────────────────────────────────────┐  │
     │  EXTERNAL WORLD                                            │  │
     │                                                           │  │
     │  ┌──────────────┐  ┌──────────────┐  ┌─────────────────┐ │  │
     │  │ GPU Peer #1  │  │ GPU Peer #2  │  │ Provider API    │ │  │
     │  │ (WebSocket)  │◄─┤ (WebSocket)  │◄─┤ (REST)          │ │  │
     │  │ llama.cpp    │  │ vLLM         │  │ pollinations.ai │ │  │
     │  └──────────────┘  └──────────────┘  └─────────────────┘ │  │
     └──────────────────────────────────────────────────────────┘  │
     ┌──────────────────────────────────────────────────────────┐  │
     │  DATA LAYER                                               │  │
     │  ┌──────────┐  ┌──────────┐  ┌──────────┐                 │  │
     │  │ DO SQLite│  │ KV       │  │ R2 (opt) │                 │  │
     │  │ (persist)│  │ (cache,  │  │ (logs)   │                 │  │
     │  │          │  │  rate    │  │          │                 │  │
     │  │          │  │  limit)  │  │          │                 │  │
     │  └──────────┘  └──────────┘  └──────────┘                 │  │
     └──────────────────────────────────────────────────────────┘  │
     ┌──────────────────────────────────────────────────────────┐  │
     │  QUEUE LAYER                                              │  │
     │  ┌──────────────────────────────────────────────────────┐ │  │
     │  │  eon-task-queue (inference / agent_dispatch /       │ │  │
     │  │                    peer_heartbeat / verification)    │ │  │
     │  └──────────────────────────────────────────────────────┘ │  │
     └──────────────────────────────────────────────────────────┘  │
                                                                  │
                     └──────────────────────────────────────────────┘
```

---

## 4. REQUEST FLOW (4-TIER FALLBACK)

```
User → POST /v1/chat/completions { model, messages }
  │
  ├── Tier 1: Workers AI (direct CF AI binding)
  │     Models: Llama 3.3 70B, GPT-OSS, Gemma 4, DeepSeek R1...
  │     Speed: ~2-5s     Cost: $0
  │     Success → return response with provider:"workers-ai"
  │     Fail → next tier
  │
  ├── Tier 2: 3-Level Cache (if CACHE_KV available)
  │     L1: In-memory LRU (per-isolate, 100 entries)
  │     L2: KV namespace (regional, TTL: 1h)
  │     L3: Call fetchFn, populate L1+L2
  │     Speed: <5ms (L1), ~200ms (L2)   Cost: $0
  │     Hit → return with provider:"cache"
  │     Miss → next tier
  │
  ├── Tier 3: Local Blind Proxy (if LOCAL_BLIND_PROXY set)
  │     Routes to your local blind proxy server
  │     Speed: ~5-15s    Cost: $0
  │     Success → return with provider:"local-blind-proxy"
  │     Fail → next tier
  │
  └── Tier 4: P2P Swarm
        Enqueue task to model-family shard DO
        Send message to eon-task-queue
        Peer claims → processes → submits result
        Speed: ~10-60s   Cost: $0
        Returns task_id — user polls /p2p/task/:id
```

---

## 5. 9 DURABLE OBJECTS — DETAILED

### P2PSwarmDO (Phase 1) — 6 Shards
**File:** `src/do/p2p-swarm.ts`  
**Purpose:** Distributed task broker, capability registry

| Method | What it does |
|--------|-------------|
| `announce(peerId, model, capabilities)` | Register/update peer, store in SQLite |
| `findBestPeer(model, priority)` | Smart routing: sort by latency/cost/reliability |
| `enqueueTask(model, prompt, priority, hint)` | Create task, persist, broadcast to WebSocket peers |
| `claimTask(peerId)` | Claim next unassigned task (rehydrates from storage) |
| `submitResult(taskId, result)` | Store result, mark done |
| `getTaskResult(taskId)` | Poll for completion (in-mem + storage) |
| `registerConnection(peerId, ws)` | WebSocket push for instant task delivery |
| `getPeerCount()` / `getQueueDepth()` | Health check metrics |

**Shards:** `llama`, `qwen`, `mistral`, `gemma`, `deepseek`, `general`  
Models are routed to the correct shard via `getModelFamily(model)`.

---

### ReputationDO (Phase 1)
**File:** `src/do/reputation.ts`  
**Purpose:** Sliding window peer reliability tracking

| Method | What it does |
|--------|-------------|
| `recordResult(peerId, taskId, success, latency, score)` | Append to sliding window, trim to 1,000, update EMA |
| `getReputation(peerId)` | Return `{ reliability, avgLatency, samples }` |
| `penalizePeer(peerId, reason)` | Apply reliability penalty (exponential decay) |
| `getAllReputations()` | Return all reputations (for routing decisions) |

Reputation is used by `P2PSwarmDO.findBestPeer()` to prefer reliable peers.

---

### CloudOpencodeDO (Phase 1)
**File:** `src/do/cloud-opencode.ts`  
**Purpose:** AI agent orchestration (7 agent types)

| Method | What it does |
|--------|-------------|
| `callWorkersAI(system, prompt)` | Try 6 Workers AI models in fallback order |
| `callFreeLLM(system, prompt)` | Try 3 free external providers |
| `dispatch(agentType, prompt)` | Route to agent with system prompt, Workers AI first |
| `runChain(steps)` | Multi-step chain, each step gets prior context |

**7 Agents:** researcher, code_executor, planner, orchestrator, critic, understand-anything, frontend

---

### CloudP2PAgentDO (Phase 1)
**File:** `src/do/cloud-p2p-agent.ts`  
**Purpose:** Autonomous swarm worker (polling agent)

| Method | What it does |
|--------|-------------|
| `initialize()` | Announce to swarm, set adaptive alarm |
| `announce()` | Register with env-configured URLs (no hardcoded) |
| `callLLM(prompt)` | Try free providers with 25s timeout |
| `alarm()` | Claim task → process → submit result |
| Storage-based lock | Adaptive interval: 3s if busy, 10s if idle |

---

### IncentiveDO (Phase 2)
**File:** `src/do/incentives.ts`  
**Purpose:** Credit tracking and redemption

| Method | What it does |
|--------|-------------|
| `creditTask(peerId, taskId, tokensGenerated)` | Add credits (tokens * RATE_PER_TOKEN) |
| `getBalance(peerId)` | Return `{ earned, redeemed, net, lastRedemption }` |
| `redeem(peerId)` | Rate-limited (3x/24h), logs payout intent |
| Internal `fetch()` routing | `/balance/:peerId`, `/redeem` |

---

### ModelRegistryDO (Phase 3)
**File:** `src/do/model-registry.ts`  
**Purpose:** Provider marketplace

| Method | What it does |
|--------|-------------|
| `registerProvider(registration)` | Store provider config with pricing, region |
| `unregisterProvider(name)` | Remove provider |
| `getProvidersForModel(modelId, region)` | Best provider by price + reliability |
| `getAllModels()` | Aggregated model catalog with provider count |
| `recordUsage(providerName, success)` | EMA reliability tracking |

---

### EdgeSwarmDO (Phase 3)
**File:** `src/do/edge-swarm.ts`  
**Purpose:** Per-colo peer registry (ultra-lightweight)

| Method | What it does |
|--------|-------------|
| `registerLocalPeer(peerId, colo, capabilities)` | Colo-local registration |
| `getClosestPeer(colo, model)` | Find nearest peer |
| `heartbeat(peerId, colo, load)` | Keep-alive with load info |
| Crash-recoverable | No persistent state, reconstructs from heartbeat |

---

### RegionalSwarmDO (Phase 3)
**File:** `src/do/regional-swarm.ts`  
**Purpose:** Per-continent task routing

| Method | What it does |
|--------|-------------|
| `findBestPeerInRegion(region, model, priority)` | Latency-optimized routing |
| `enqueueTask(model, prompt)` | Full task queue |
| `claimTask(peerId)` | Claim with regional affinity |
| `submitResult(taskId, result)` | Store and persist |
| `getMetrics()` | Return RegionMetrics |

---

### GlobalSwarmDO (Phase 3)
**File:** `src/do/global-swarm.ts`  
**Purpose:** Cross-region coordination

| Method | What it does |
|--------|-------------|
| `syncRegions()` | Poll all regional DOs for metrics |
| `getGlobalMetrics()` | Aggregate across regions |
| `migrateTask(taskId, fromRegion, toRegion)` | Move task with MigrationRecord |
| `getGlobalModelCatalog()` | Delegate to ModelRegistryDO |

---

## 6. API ENDPOINT REFERENCE

| Route | Method | Purpose | Phase |
|-------|--------|---------|-------|
| `/status` | GET | Health check — shards, peers, queue depth, global metrics | 0 |
| `/v1/models` | GET | List 12 Workers AI models | 0 |
| `/v1/chat/completions` | POST | 4-tier chat (Workers AI → Cache → Proxy → Swarm) | 0 |
| `/chat` | POST | Alias for v1/chat/completions | 0 |
| `/opencode/dispatch` | POST | Dispatch to AI agent (researcher, coder, etc.) | 1 |
| `/opencode/agents` | GET | List 7 agent types with system prompts | 1 |
| `/opencode/chain` | POST | Multi-step agent chain | 1 |
| `/spawn-agent` | GET | Spawn autonomous swarm agent | 1 |
| `/p2p/announce` | POST | Register peer in capability registry | 1 |
| `/p2p/peers` | GET | List peers (optional `?model=` shard filter) | 1 |
| `/p2p/tasks` | POST | Claim a task from swarm | 1 |
| `/p2p/task/:id` | POST | Submit task result | 1 |
| `/p2p/task/:id` | GET | Get task result | 1 |
| `/p2p/connect` | GET (WS) | WebSocket upgrade for GPU peers | 2 |
| `/incentives/balance` | POST | Get peer credit balance | 2 |
| `/incentives/redeem` | POST | Redeem credits (rate-limited) | 2 |
| `/admin/verify` | POST | Redundant verification (3 peers + sanity check) | 2 |
| `/providers/register` | POST | Register model provider | 3 |
| `/providers/models` | GET | List all marketplace models | 3 |
| `/providers/:name` | DELETE | Unregister provider | 3 |
| `/region/:region/metrics` | GET | Regional swarm metrics | 3 |
| `/admin/migrate` | POST | Migrate task between regions | 3 |

---

## 7. INFRASTRUCTURE (wrangler.jsonc)

```jsonc
{
  "name": "eon-p2p-cloud",
  "main": "src/index.ts",
  "compatibility_date": "2026-07-25",
  "compatibility_flags": ["nodejs_compat"],
  "observability": { "enabled": true, "head_sampling_rate": 1.0 },
  "ai": { "binding": "AI" },
  "durable_objects": {
    "bindings": [
      { "name": "P2P_SWARM", "class_name": "P2PSwarmDO" },
      { "name": "REPUTATION", "class_name": "ReputationDO" },
      { "name": "CLOUD_AGENT", "class_name": "CloudP2PAgentDO" },
      { "name": "OPENCODE", "class_name": "CloudOpencodeDO" },
      { "name": "MODEL_REGISTRY", "class_name": "ModelRegistryDO" },
      { "name": "INCENTIVES", "class_name": "IncentiveDO" },
      { "name": "EDGE_SWARM", "class_name": "EdgeSwarmDO" },
      { "name": "REGIONAL_SWARM", "class_name": "RegionalSwarmDO" },
      { "name": "GLOBAL_SWARM", "class_name": "GlobalSwarmDO" }
    ]
  },
  "kv_namespaces": [
    { "binding": "RATE_LIMIT_KV", "id": "0860a1261eff4a30b9629d7c9af4d426" }
  ],
  "queues": {
    "producers": [{ "binding": "TASK_QUEUE", "queue": "eon-task-queue" }],
    "consumers": [{ "queue": "eon-task-queue", "max_batch_size": 5, "max_batch_timeout": 5 }]
  },
  "migrations": [
    { "tag": "v1", "new_sqlite_classes": ["P2PSwarmDO"] },
    { "tag": "v2", "new_sqlite_classes": ["CloudP2PAgentDO"] },
    { "tag": "v3", "new_sqlite_classes": ["CloudOpencodeDO"] },
    { "tag": "v4", "new_sqlite_classes": ["ReputationDO", "ModelRegistryDO", "IncentiveDO", "EdgeSwarmDO", "RegionalSwarmDO", "GlobalSwarmDO"] }
  ]
}
```

---

## 8. DATA FLOW DIAGRAMS

### Inference Request

```
User ──POST /v1/chat/completions──► fetch()
  │
  ├── Rate limit check (KV sliding window — 60 req/min/IP)
  │
  ├── IF Workers AI model available ──► AI.run() ──► return response
  │
  ├── IF CACHE_KV available ──► cachedInference() ──► HIT? return
  │     ├── L1: In-memory LRU (100 entries, per-isolate)
  │     ├── L2: KV get (regional)
  │     └── L3: fetchFn() → store in L1+L2
  │
  ├── IF LOCAL_BLIND_PROXY set ──► fetch(proxy) ──► return response
  │
  └── P2P Swarm (everything else failed)
        ├── getSwarmDO(model) → select shard
        ├── stub.enqueueTask()
        ├── TASK_QUEUE.send()
        └── return { task_id, status:"queued" }
```

### Peer Task Claim & Process

```
Peer (WebSocket) ──connect──► handleWebSocketUpgrade()
  │
  ├── Server sends { type:"connected", peerId, version:"2.0" }
  │
  ├── Peer sends { type:"capabilities", models, region, load }
  │     └── Registered in P2PSwarmDO capability registry
  │
  ├── Server broadcasts { type:"new_task", taskId, model }
  │
  ├── Peer claims: POST /p2p/tasks { peer }
  │     └── P2PSwarmDO.claimTask() returns Task || null
  │
  ├── Peer processes inference (locally)
  │
  └── Peer submits: { type:"task_result", taskId, result, metrics }
        └── P2PSwarmDO.submitResult()
              ├── ReputationDO.recordResult(success, latency)
              └── IncentiveDO.creditTask(tokensGenerated)
```

### Agent Dispatch Chain

```
POST /opencode/chain { steps: [{ agent, prompt }, ...] }
  │
  └── CloudOpencodeDO.runChain(steps)
        ├── Step 1: dispatch(agent1, prompt1)
        │     ├── callWorkersAI(system1, prompt1)
        │     └── IF fail: callFreeLLM(system1, prompt1)
        │
        ├── Step 2: dispatch(agent2, prompt2 + "\nContext: " + result1)
        │
        ├── Step 3: dispatch(agent3, prompt3 + "\nContext: " + result1+result2)
        │
        └── Return [result1, result2, result3]
```

### Verification Flow

```
POST /admin/verify { prompt, model, count }
  │
  └── verifyTask(env, swarm, prompt, model, count=3)
        ├── redundantVerify(env, swarm, prompt, model, count)
        │     └── Enqueue task to count different peers (routingHint)
        │     └── Poll all task IDs (max 30s)
        │     └── Return peerResults[]
        │
        ├── majorityAgreement(peerResults)
        │     └── Return result if >= 2/3 agree
        │
        ├── sanityCheck(env, prompt, bestResult)
        │     └── Workers AI judges quality 0-100
        │
        └── Return VerificationResponse { score, verified, method }
```

---

## 9. SECURITY & RELIABILITY

| Feature | Mechanism |
|---------|-----------|
| Rate limiting | IP-based sliding window via KV (60 req/min) |
| No hardcoded URLs | All endpoints from environment variables |
| Error handling | All catch blocks log to console + optional R2 |
| CORS | All responses include CORS headers |
| Peer reputation | 1,000-result sliding window with exponential decay |
| Verification | Redundant assignment + Workers AI sanity check |
| DO persistence | SQLite storage with exactly-once semantics |
| Global mutable state | None — all state is per-DO or in storage |

---

## 10. WHAT MAKES THIS UNIQUE

| Capability | eon-p2p-cloud | OpenRouter | Bittensor | Together AI | Petals |
|-----------|--------------|------------|-----------|-------------|--------|
| Edge-native routing | ✅ 330 PoPs | ❌ ~20 edge | ❌ Public net | ❌ 5 DCs | ❌ Public net |
| Multi-provider fallback | ✅ 9 tiers | ✅ 70+ providers | ❌ Single subnet | ❌ 1 provider | ❌ 1 model |
| $0 inference | ✅ Workers AI + cache | ❌ Min $0.0001/tok | ❌ Needs TAO | ❌ Paid | ✅ Volunteer |
| P2P compute | ✅ GPU peers + agents | ❌ | ✅ Blockchain | ❌ | ✅ DHT |
| Smart routing | ✅ By latency/cost/reliability | ✅ Provider-based | ❌ | ❌ | ❌ |
| Verification | ✅ Redundant + AI sanity | ❌ Trusts providers | ✅ Consensus | ❌ Trusts own | ❌ |
| Stateful coordination | ✅ DOs (free, exactly-once) | ✅ NATS JetStream | ✅ Substrate | ✅ Custom | ❌ |
| Provider marketplace | ✅ Any CF Worker | ✅ API key only | ✅ Subnet token | ❌ | ❌ |
| 3-tier cache | ✅ LRU → KV → fetch | ✅ Redis | ❌ | ✅ KV cache | ❌ |
| Credit incentives | ✅ D1 + stablecoin | ❌ | ✅ TAO token | ❌ | ❌ |

---

## 11. DEPLOYMENT

### Current Deploy
- **Version:** 71bde5fb-9a6b-48d3-a9aa-b61fc5295fb0
- **Upload:** 116.61 KiB (gzip: 23.39 KiB)
- **Startup:** 7ms
- **URL:** https://eon-p2p-cloud.exportdefaultasyncfetchrequestenvconsturl.workers.dev

### How to Re-Deploy
```bash
cd /home/ricos/eon-p2p-cloud
npx wrangler deploy
```

### How to Add Env Vars
```bash
npx wrangler secret put LOCAL_BLIND_PROXY
npx wrangler secret put LOCAL_BRIDGE_URL
npx wrangler secret put LOCAL_P2P_URL
```

---

## 12. SOURCE CODE SUMMARY

| File | Lines | Purpose |
|------|-------|---------|
| src/index.ts | 1,361 | Entry point: fetch/queue/scheduled handlers |
| src/types.ts | 128 | All interfaces |
| src/models.ts | 55 | Models, shards, free providers config |
| src/agents.ts | 42 | 7 agent routes |
| src/cache.ts | 247 | 3-tier caching system |
| src/peer-protocol.ts | 268 | WebSocket peer protocol |
| src/verification.ts | 285 | Result verification system |
| src/do/p2p-swarm.ts | 451 | P2P task broker + smart routing |
| src/do/reputation.ts | 234 | Peer reputation tracking |
| src/do/cloud-opencode.ts | 220 | Agent dispatch |
| src/do/cloud-p2p-agent.ts | 287 | Autonomous swarm worker |
| src/do/incentives.ts | 248 | Credit tracking |
| src/do/model-registry.ts | 278 | Provider marketplace |
| src/do/edge-swarm.ts | 219 | Per-colo peer registry |
| src/do/regional-swarm.ts | 321 | Per-continent routing |
| src/do/global-swarm.ts | 364 | Cross-region coordination |
| src/workflows/swarm-scaler.ts | 291 | Autoscaling Workflow |
| wrangler.jsonc | 31 | Cloudflare config |
| **TOTAL** | **5,333** | |

---

*Generated: 2026-07-26 | EON P2P Cloud v3.0*
