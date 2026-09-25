import { DurableObject } from "cloudflare:workers";
import { VaultStorageDO } from "./vault-storage";
import { type Env, type ProviderRegistration } from "../types";

/**
 * ModelRegistryDO — global model-and-provider registry (Phase 3).
 *
 * Maintains a durable catalogue of LLM providers and their supported models,
 * along with usage-based reliability tracking.  Queries support filtering by
 * model ID and region and return results sorted by price+reliability.
 *
 * All state is persisted through ctx.storage for crash recovery.
 */
export class ModelRegistryDO extends VaultStorageDO<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
  }

  // -----------------------------------------------------------------------
  // Provider management
  // -----------------------------------------------------------------------

  /**
   * Register (or update) an LLM provider.
   *
   * If a provider with the same `name` already exists it is overwritten.
   */
  async registerProvider(
    registration: ProviderRegistration,
  ): Promise<void> {
    const key = providerKey(registration.name);

    // Merge with existing so we don't lose reliability counters on re-register
    const existing = await encGet<ProviderRegistration>(this.ctx.storage, key);
    if (existing) {
      registration.reliability =
        registration.reliability ?? existing.reliability;
      registration.totalRequests =
        registration.totalRequests ?? existing.totalRequests;
    }

    await this.encPut(key, registration);
    await this.indexProviderModels(registration);
  }

  /**
   * Remove a provider from the registry.
   */
  async unregisterProvider(name: string): Promise<void> {
    const key = providerKey(name);
    const existing = await encGet<ProviderRegistration>(this.ctx.storage, key);
    if (existing) {
      // Remove from model indexes
      for (const modelId of existing.models) {
        const idxKey = modelIndexKey(modelId);
        const list =
          (await encGet<string[]>(this.ctx.storage, idxKey)) ?? [];
        const filtered = list.filter((n) => n !== name);
        if (filtered.length > 0) {
          await this.encPut(idxKey, filtered);
        } else {
          await this.ctx.storage.delete(idxKey);
        }
      }
    }
    await this.ctx.storage.delete(key);
  }

  /**
   * Get a single provider registration by name.
   */
  async getProvider(
    name: string,
  ): Promise<ProviderRegistration | null> {
    return (
      (await this.encGet<ProviderRegistration>(
        providerKey(name),
      )) ?? null
    );
  }

  /**
   * Find the best providers for a given model, optionally filtered by region.
   * Results are sorted by cost (ascending), then reliability (descending).
   *
   * @param modelId  Model identifier (e.g. "llama-3.3-70b")
   * @param region   Optional region filter (ISO 3166-1 alpha-2 code)
   */
  async getProvidersForModel(
    modelId: string,
    region?: string,
  ): Promise<ProviderRegistration[]> {
    const idxKey = modelIndexKey(modelId);
    const names = await encGet<string[]>(this.ctx.storage, idxKey);
    if (!names || names.length === 0) return [];

    const providers: ProviderRegistration[] = [];
    for (const name of names) {
      const reg = await this.encGet<ProviderRegistration>(
        providerKey(name),
      );
      if (reg) {
        if (region && reg.region !== region && reg.region !== "global") {
          continue; // skip mismatched region
        }
        providers.push(reg);
      }
    }

    // Sort: cheapest input price first, then most reliable
    providers.sort((a, b) => {
      const priceDiff = a.pricing.input - b.pricing.input;
      if (priceDiff !== 0) return priceDiff;
      return (b.reliability ?? 0) - (a.reliability ?? 0);
    });

    return providers;
  }

  /**
   * Return every registered model (deduplicated) with a count of providers.
   */
  async getAllModels(): Promise<
    { modelId: string; providerCount: number }[]
  > {
    const list = await this.encList<string[]>({
      prefix: "idx:model:",
    });
    const models: { modelId: string; providerCount: number }[] = [];
    for (const [key, names] of list) {
      const modelId = key.slice("idx:model:".length);
      models.push({ modelId, providerCount: names.length });
    }
    return models.sort((a, b) => a.modelId.localeCompare(b.modelId));
  }

  /**
   * Record a usage outcome for a provider so we can track reliability.
   *
   * @param providerName  The registered provider name
   * @param success       true if the inference completed without error
   */
  async recordUsage(providerName: string, success: boolean): Promise<void> {
    const key = providerKey(providerName);
    const reg = await encGet<ProviderRegistration>(this.ctx.storage, key);
    if (!reg) return;

    reg.totalRequests = (reg.totalRequests ?? 0) + 1;
    // Exponential moving average for reliability
    const alpha = 0.01;
    const current = reg.reliability ?? 1.0;
    reg.reliability = success
      ? current + alpha * (1.0 - current)
      : current * (1.0 - alpha);

    await this.encPut(key, reg);
  }

  // -----------------------------------------------------------------------
  // Internal helpers
  // -----------------------------------------------------------------------

  /**
   * Maintain a reverse index: modelId -> [providerName, ...]
   */
  private async indexProviderModels(
    registration: ProviderRegistration,
  ): Promise<void> {
    for (const modelId of registration.models) {
      const idxKey = modelIndexKey(modelId);
      const list =
        (await encGet<string[]>(this.ctx.storage, idxKey)) ?? [];
      if (!list.includes(registration.name)) {
        list.push(registration.name);
        await this.encPut(idxKey, list);
      }
    }
  }

  // -----------------------------------------------------------------------
  // Fetch handler
  // -----------------------------------------------------------------------

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    try {
      if (path === "/register" && request.method === "POST") {
        const body = (await request.json()) as ProviderRegistration;
        await this.registerProvider(body);
        return new Response(
          JSON.stringify({ ok: true, provider: body.name }),
          { headers: { "Content-Type": "application/json" } },
        );
      }

      if (path === "/unregister" && request.method === "POST") {
        const body = (await request.json()) as { name: string };
        await this.unregisterProvider(body.name);
        return new Response(
          JSON.stringify({ ok: true }),
          { headers: { "Content-Type": "application/json" } },
        );
      }

      if (path.startsWith("/provider/")) {
        const name = path.split("/").pop()!;
        const reg = await this.getProvider(name);
        if (!reg) {
          return new Response(
            JSON.stringify({ error: "provider_not_found" }),
            { status: 404, headers: { "Content-Type": "application/json" } },
          );
        }
        return new Response(JSON.stringify(reg), {
          headers: { "Content-Type": "application/json" },
        });
      }

      if (path === "/models") {
        const models = await this.getAllModels();
        return new Response(
          JSON.stringify({ models }),
          { headers: { "Content-Type": "application/json" } },
        );
      }

      if (path.startsWith("/providers-for-model/")) {
        const modelId = path.split("/").pop()!;
        const region = url.searchParams.get("region") || undefined;
        const providers = await this.getProvidersForModel(modelId, region);
        return new Response(
          JSON.stringify({ model: modelId, providers }),
          { headers: { "Content-Type": "application/json" } },
        );
      }

      if (path === "/record-usage" && request.method === "POST") {
        const body = (await request.json()) as {
          providerName: string;
          success: boolean;
        };
        await this.recordUsage(body.providerName, body.success);
        return new Response(
          JSON.stringify({ ok: true }),
          { headers: { "Content-Type": "application/json" } },
        );
      }

      return new Response(
        "ModelRegistryDO — /register, /unregister, /provider/:name, /models, /providers-for-model/:id, /record-usage",
        { status: 200 },
      );
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      console.error("ModelRegistryDO error:", msg);
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

// -----------------------------------------------------------------------
// Storage key helpers (kept local to this module)
// -----------------------------------------------------------------------

function providerKey(name: string): string {
  return `provider:${name}`;
}

function modelIndexKey(modelId: string): string {
  return `idx:model:${modelId}`;
}
