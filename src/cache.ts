import { type Env, type CacheEntry } from "./types";

// ---------------------------------------------------------------------------
// LRU cache implementation
// ---------------------------------------------------------------------------

/**
 * A simple Map-backed LRU cache with a maximum entry count.
 *
 * On every `get` the accessed entry is moved to the end (most-recently-used).
 * When the cache exceeds `maxSize` the oldest (first) entries are evicted.
 */
export class LRUCache<V = string> {
  private readonly store = new Map<string, V>();
  private readonly maxSize: number;

  constructor(maxSize: number = 100) {
    this.maxSize = maxSize;
  }

  /** Retrieve a value and promote it to most-recently-used. */
  get(key: string): V | undefined {
    if (!this.store.has(key)) return undefined;
    const value = this.store.get(key)!;
    // Move to end (most-recently-used)
    this.store.delete(key);
    this.store.set(key, value);
    return value;
  }

  /** Insert or update a value.  Evicts oldest entries if over capacity. */
  set(key: string, value: V): void {
    if (this.store.has(key)) {
      this.store.delete(key);
    } else if (this.store.size >= this.maxSize) {
      // Evict the least-recently-used (first inserted) entry
      const oldest = this.store.keys().next();
      if (!oldest.done) {
        this.store.delete(oldest.value);
      }
    }
    this.store.set(key, value);
  }

  /** Check existence without promoting. */
  has(key: string): boolean {
    return this.store.has(key);
  }

  /** Remove a single entry. */
  delete(key: string): void {
    this.store.delete(key);
  }

  /** Remove all entries. */
  clear(): void {
    this.store.clear();
  }

  /** Current number of entries. */
  get size(): number {
    return this.store.size;
  }
}

// ---------------------------------------------------------------------------
// Module-level LRU singleton (per-isolate)
// ---------------------------------------------------------------------------

let _lru: LRUCache<string> | null = null;

/**
 * Return (or create) the per-isolate LRU cache instance.
 *
 * In Cloudflare Workers each isolate is long-lived but not shared across
 * requests — a module-level singleton is safe.
 */
export function initLRU(): LRUCache<string> {
  if (!_lru) {
    _lru = new LRUCache<string>(100);
  }
  return _lru;
}

// ---------------------------------------------------------------------------
// Cache-key generation
// ---------------------------------------------------------------------------

/**
 * Produce a deterministic cache key from a model id and a message list.
 *
 * The hash is a FNV-1a 64-bit digest (safe for KV key length limits).
 * Two identical model+messages arrays always produce the same key.
 *
 * @param model     Model identifier (e.g. "llama-3.3-70b")
 * @param messages  Array of `{ role, content }` messages
 * @returns A hex string suitable as a KV key.
 */
export function getCacheKey(
  model: string,
  messages: { role: string; content: string }[],
): string {
  const input = `${model}::${messages.map((m) => `${m.role}:${m.content}`).join("||")}`;
  return fnv1a64(input);
}

// ---------------------------------------------------------------------------
// 3-tier cached inference
// ---------------------------------------------------------------------------

/**
 * Perform inference with a 3-tier caching strategy.
 *
 * **Tier 1** — In-memory LRU cache (per-isolate, ~100 entries).
 * **Tier 2** — KV namespace (`CACHE_KV`) with configurable TTL.
 * **Tier 3** — Execute `fetchFn`, then populate Tier 1 and Tier 2.
 *
 * @param env        Workers environment bindings (expects optional `env.CACHE_KV`)
 * @param ctx        Execution context for `waitUntil` (async KV write)
 * @param model      Model identifier
 * @param messages   Input messages
 * @param maxTokens  Maximum tokens for the inference call (used for cache-key scoping)
 * @param fetchFn    Async function that performs the actual model inference.
 *                   Called only on a cache miss at all three tiers.
 * @param ttlSec     KV TTL in seconds (default 3600 = 1 hour)
 * @returns The inference result string.
 */
export async function cachedInference(
  env: Env,
  ctx: ExecutionContext,
  model: string,
  messages: { role: string; content: string }[],
  maxTokens: number,
  fetchFn: () => Promise<string>,
  ttlSec: number = 3600,
): Promise<string> {
  const cacheKey = getCacheKey(model, messages);

  // -- Tier 1: In-memory LRU ------------------------------------------------
  const lru = initLRU();
  const memCached = lru.get(cacheKey);
  if (memCached !== undefined) {
    return memCached;
  }

  // -- Tier 2: KV namespace -------------------------------------------------
  const kv = getCacheKV(env);
  if (kv) {
    try {
      const kvRaw = await kv.get(cacheKey);
      if (kvRaw !== null) {
        // Promote to LRU before returning
        lru.set(cacheKey, kvRaw);
        return kvRaw;
      }
    } catch (err) {
      console.warn("cache: KV read failed:", err);
    }
  }

  // -- Tier 3: Execute fetchFn ----------------------------------------------
  const result = await fetchFn();

  // Store in Tier 1 (LRU)
  lru.set(cacheKey, result);

  // Store in Tier 2 (KV) — fire-and-forget to avoid blocking the response
  const kvForWrite = getCacheKV(env);
  if (kvForWrite) {
    ctx.waitUntil(
      kvForWrite.put(cacheKey, result, {
        expirationTtl: ttlSec,
      }).catch((err) => {
        console.warn("cache: KV write failed:", err);
      }),
    );
  }

  return result;
}

// ---------------------------------------------------------------------------
// Cache invalidation
// ---------------------------------------------------------------------------

/**
 * Remove a cached entry from all tiers.
 *
 * @param env       Workers environment bindings
 * @param model     Model identifier
 * @param messages  Input messages (re-hashed to compute the key)
 */
export async function invalidateCache(
  env: Env,
  model: string,
  messages: { role: string; content: string }[],
): Promise<void> {
  const cacheKey = getCacheKey(model, messages);

  // Tier 1: LRU
  initLRU().delete(cacheKey);

  // Tier 2: KV
  const kv = getCacheKV(env);
  if (kv) {
    try {
      await kv.delete(cacheKey);
    } catch (err) {
      console.warn("cache: KV delete failed:", err);
    }
  }
}

// ---------------------------------------------------------------------------
// Internal: FNV-1a 64-bit hash (no external deps)
// ---------------------------------------------------------------------------

/**
 * Compute a FNV-1a 64-bit hash from a string using BigInt.
 * Returns a 16-character hex string suitable as a KV key.
 */
function fnv1a64(input: string): string {
  const FNV_OFFSET = 0xcbf29ce484222325n;
  const FNV_PRIME = 0x100000001b3n;

  let hash = FNV_OFFSET;
  for (let i = 0; i < input.length; i++) {
    hash ^= BigInt(input.charCodeAt(i));
    hash = (hash * FNV_PRIME) & 0xffffffffffffffffn;
  }

  return hash.toString(16).padStart(16, "0");
}

// ---------------------------------------------------------------------------
// KV namespace accessor (CACHE_KV is optional)
// ---------------------------------------------------------------------------

/**
 * Safely retrieve the CACHE_KV binding, which must be declared in wrangler.toml
 * as `{ binding = "CACHE_KV", type = "kv_namespace" }` when the cache module
 * is used.  Returns undefined if the binding is not configured — all callers
 * check for this before reading or writing.
 */
function getCacheKV(env: Env): KVNamespace | undefined {
  return (env as Record<string, unknown>).CACHE_KV as KVNamespace | undefined;
}
