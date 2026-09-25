import { DurableObject } from "cloudflare:workers";
import { VaultStorageDO } from "./vault-storage";
import { type Env, type RegionMetrics, type Task } from "../types";

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

interface RegionSnapshot {
  region: string;
  metrics: RegionMetrics;
  syncedAt: number;
}

interface MigrationRecord {
  taskId: string;
  fromRegion: string;
  toRegion: string;
  migratedAt: number;
  success: boolean;
}

const REGION_NAMES = ["na", "eu", "asia", "sa", "af", "oc"];

/**
 * GlobalSwarmDO — single global coordinator Durable Object (Phase 3).
 *
 * Responsibilities:
 *   1. Periodically sync metrics from every RegionalSwarmDO.
 *   2. Provide a global model catalogue (delegating to ModelRegistryDO).
 *   3. Support cross-region task migration.
 *
 * This DO is a singleton — only one instance should ever be created.
 */
export class GlobalSwarmDO extends VaultStorageDO<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
  }

  // -----------------------------------------------------------------------
  // Regional sync
  // -----------------------------------------------------------------------

  /**
   * Poll every RegionalSwarmDO instance and aggregate their metrics.
   * Returns a map of region -> RegionMetrics.
   */
  async syncRegions(): Promise<Record<string, RegionMetrics>> {
    const snapshots: Record<string, RegionMetrics> = {};

    for (const region of REGION_NAMES) {
      try {
        const id = this.env.REGIONAL_SWARM.idFromName(`region-${region}`);
        const stub = this.env.REGIONAL_SWARM.get(id);
        const resp = await stub.fetch("http://internal/metrics", {
          method: "GET",
          signal: AbortSignal.timeout(10_000),
        });

        if (resp.ok) {
          const metrics = (await resp.json()) as RegionMetrics;
          snapshots[region] = metrics;

          // Persist the snapshot for crash recovery
          await this.encPut(
            snapshotKey(region),
            {
              region,
              metrics,
              syncedAt: Date.now(),
            } satisfies RegionSnapshot,
          );
        } else {
          console.warn(
            `GlobalSwarmDO: syncRegions region ${region} returned ${resp.status}`,
          );
        }
      } catch (e) {
        console.error(
          `GlobalSwarmDO: syncRegions failed for region ${region}:`,
          e,
        );
      }
    }

    return snapshots;
  }

  /**
   * Get the last synced metrics for a specific region, or all regions.
   */
  async getRegionMetrics(
    region?: string,
  ): Promise<Record<string, RegionMetrics>> {
    const result: Record<string, RegionMetrics> = {};

    if (region) {
      const snap = await this.encGet<RegionSnapshot>(
        snapshotKey(region),
      );
      if (snap) result[region] = snap.metrics;
    } else {
      const stored = await this.encList<RegionSnapshot>({
        prefix: "snapshot:",
      });
      for (const [, snap] of stored) {
        result[snap.region] = snap.metrics;
      }
    }

    return result;
  }

  /**
   * Get aggregated global metrics derived from all regional snapshots.
   */
  async getGlobalMetrics(): Promise<{
    totalPeers: number;
    totalQueueDepth: number;
    avgLatency: number;
    regions: Record<string, RegionMetrics>;
  }> {
    const regions = await this.getRegionMetrics();
    const values = Object.values(regions);
    const totalPeers = values.reduce((s, m) => s + m.peerCount, 0);
    const totalQueueDepth = values.reduce((s, m) => s + m.queueDepth, 0);
    const avgLatency =
      values.length > 0
        ? values.reduce((s, m) => s + m.avgLatency, 0) / values.length
        : 0;

    return { totalPeers, totalQueueDepth, avgLatency, regions };
  }

  // -----------------------------------------------------------------------
  // Cross-region task migration
  // -----------------------------------------------------------------------

  /**
   * Migrate a task from one region to another.  The task is re-enqueued in
   * the target region's RegionalSwarmDO.  The origin region's task is
   * marked done (the peer that claimed it will retry).
   *
   * @param taskId     The id of the task to migrate
   * @param fromRegion Source region code
   * @param toRegion   Destination region code
   * @returns true if the migration was accepted
   */
  async migrateTask(
    taskId: string,
    fromRegion: string,
    toRegion: string,
  ): Promise<boolean> {
    if (!REGION_NAMES.includes(fromRegion) || !REGION_NAMES.includes(toRegion)) {
      console.warn(
        `GlobalSwarmDO: migrateTask invalid region ${fromRegion} -> ${toRegion}`,
      );
      return false;
    }

    try {
      // 1. Fetch the task from the source region
      const fromId = this.env.REGIONAL_SWARM.idFromName(
        `region-${fromRegion}`,
      );
      const fromStub = this.env.REGIONAL_SWARM.get(fromId);

      // The regional DO exposes task fetch via /task/:id (GET)
      const getResp = await fromStub.fetch(
        `http://internal/task/${taskId}`,
        { method: "GET" },
      );
      if (!getResp.ok) {
        console.warn(
          `GlobalSwarmDO: migrateTask task ${taskId} not found in ${fromRegion}`,
        );
        return false;
      }

      const task = (await getResp.json()) as Task | null;
      if (!task || task.done) return false;

      // 2. Re-enqueue in the target region
      const toId = this.env.REGIONAL_SWARM.idFromName(`region-${toRegion}`);
      const toStub = this.env.REGIONAL_SWARM.get(toId);

      const newTaskId = await (
        await toStub.fetch("http://internal/task", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            model: task.model,
            prompt: task.prompt,
            messages: task.messages,
          }),
        })
      ).json<any>();

      // 3. Mark the original task as done (migrated) in the source
      await fromStub.fetch(`http://internal/task/${taskId}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          result: `[migrated to ${toRegion} as ${newTaskId.taskId}]`,
        }),
      });

      // 4. Record the migration
      const record: MigrationRecord = {
        taskId,
        fromRegion,
        toRegion,
        migratedAt: Date.now(),
        success: true,
      };
      await this.encPut(migrationKey(taskId), record);

      return true;
    } catch (e) {
      console.error(
        `GlobalSwarmDO: migrateTask ${taskId} ${fromRegion}->${toRegion} failed:`,
        e,
      );
      return false;
    }
  }

  /**
   * Get migration history for a task.
   */
  async getMigrationHistory(
    taskId: string,
  ): Promise<MigrationRecord | null> {
    return (
      (await this.encGet<MigrationRecord>(
        migrationKey(taskId),
      )) ?? null
    );
  }

  // -----------------------------------------------------------------------
  // Global model catalogue (delegates to ModelRegistryDO)
  // -----------------------------------------------------------------------

  /**
   * Get the aggregated model catalogue from the ModelRegistryDO.
   */
  async getGlobalModelCatalog(): Promise<
    { modelId: string; providerCount: number }[]
  > {
    try {
      const id = this.env.MODEL_REGISTRY.idFromName("global");
      const stub = this.env.MODEL_REGISTRY.get(id);
      const resp = await stub.fetch("http://internal/models", {
        method: "GET",
        signal: AbortSignal.timeout(10_000),
      });
      if (resp.ok) {
        const data = (await resp.json()) as {
          models: { modelId: string; providerCount: number }[];
        };
        return data.models;
      }
    } catch (e) {
      console.error("GlobalSwarmDO: getGlobalModelCatalog error:", e);
    }
    return [];
  }

  // -----------------------------------------------------------------------
  // Fetch handler
  // -----------------------------------------------------------------------

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    try {
      if (path === "/sync-regions" && request.method === "POST") {
        const metrics = await this.syncRegions();
        return new Response(JSON.stringify({ regions: metrics }), {
          headers: { "Content-Type": "application/json" },
        });
      }

      if (path === "/metrics") {
        const global = await this.getGlobalMetrics();
        return new Response(JSON.stringify(global), {
          headers: { "Content-Type": "application/json" },
        });
      }

      if (path === "/region-metrics") {
        const region = url.searchParams.get("region") || undefined;
        const metrics = await this.getRegionMetrics(region);
        return new Response(JSON.stringify({ metrics }), {
          headers: { "Content-Type": "application/json" },
        });
      }

      if (path === "/migrate" && request.method === "POST") {
        const body = (await request.json()) as {
          taskId: string;
          fromRegion: string;
          toRegion: string;
        };
        const ok = await this.migrateTask(
          body.taskId,
          body.fromRegion,
          body.toRegion,
        );
        return new Response(JSON.stringify({ ok }), {
          headers: { "Content-Type": "application/json" },
        });
      }

      if (path.startsWith("/migration/")) {
        const taskId = path.split("/").pop()!;
        const history = await this.getMigrationHistory(taskId);
        return new Response(
          JSON.stringify(history ?? { error: "not_found" }),
          {
            status: history ? 200 : 404,
            headers: { "Content-Type": "application/json" },
          },
        );
      }

      if (path === "/models") {
        const catalog = await this.getGlobalModelCatalog();
        return new Response(
          JSON.stringify({ models: catalog }),
          { headers: { "Content-Type": "application/json" } },
        );
      }

      return new Response(
        "GlobalSwarmDO — /sync-regions, /metrics, /region-metrics, /migrate, /migration/:taskId, /models",
        { status: 200 },
      );
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      console.error("GlobalSwarmDO error:", msg);
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

// ---------------------------------------------------------------------------
// Storage key helpers
// ---------------------------------------------------------------------------

function snapshotKey(region: string): string {
  return `snapshot:${region}`;
}

function migrationKey(taskId: string): string {
  return `migration:${taskId}`;
}
