import { DurableObject } from "cloudflare:workers";
import type { Env } from "../types";

interface DreamEntry {
  id: string;
  type: "skill" | "pattern" | "insight" | "sync";
  source: "dream-engine" | "companion" | "cloud";
  slug?: string;
  title: string;
  description: string;
  priority: number;
  data?: string;
  created: string;
  syncedAt?: string;
}

export class DreamMemoryDO extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
  }

  async store(entry: DreamEntry): Promise<{ ok: boolean; id: string }> {
    const id = entry.id || crypto.randomUUID();
    const key = `dream:${id}`;
    const stored = { ...entry, id, syncedAt: new Date().toISOString() };
    await this.ctx.storage.put(key, stored);

    const index = (await this.ctx.storage.get<string[]>("dream:index")) || [];
    if (!index.includes(id)) {
      index.unshift(id);
      if (index.length > 1000) index.length = 1000;
      await this.ctx.storage.put("dream:index", index);
    }

    return { ok: true, id };
  }

  async recall(id: string): Promise<DreamEntry | null> {
    return (await this.ctx.storage.get<DreamEntry>(`dream:${id}`)) || null;
  }

  async list(
    type?: string,
    limit = 50,
    offset = 0,
  ): Promise<{ entries: DreamEntry[]; total: number }> {
    const index = (await this.ctx.storage.get<string[]>("dream:index")) || [];
    let ids = index;
    if (type) {
      const all = await Promise.all(
        index.map(async (id) => {
          const entry = await this.ctx.storage.get<DreamEntry>(`dream:${id}`);
          return entry && entry.type === type ? entry : null;
        }),
      );
      ids = all.filter((e): e is DreamEntry => e !== null).map((e) => e.id);
    }
    const total = ids.length;
    const page = ids.slice(offset, offset + limit);
    const entries = (
      await Promise.all(
        page.map(async (id) => this.ctx.storage.get<DreamEntry>(`dream:${id}`)),
      )
    ).filter((e): e is DreamEntry => e !== null);
    return { entries, total };
  }

  async delete(id: string): Promise<{ ok: boolean }> {
    await this.ctx.storage.delete(`dream:${id}`);
    const index = (await this.ctx.storage.get<string[]>("dream:index")) || [];
    const filtered = index.filter((i) => i !== id);
    await this.ctx.storage.put("dream:index", filtered);
    return { ok: true };
  }

  async stats(): Promise<{ total: number; byType: Record<string, number> }> {
    const index = (await this.ctx.storage.get<string[]>("dream:index")) || [];
    const entries = (
      await Promise.all(
        index.map(async (id) =>
          this.ctx.storage.get<DreamEntry>(`dream:${id}`),
        ),
      )
    ).filter((e): e is DreamEntry => e !== null);
    const byType: Record<string, number> = {};
    for (const e of entries) {
      byType[e.type] = (byType[e.type] || 0) + 1;
    }
    return { total: entries.length, byType };
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;
    try {
      if (path === "/store" && request.method === "POST") {
        const body = (await request.json()) as DreamEntry;
        const result = await this.store(body);
        return new Response(JSON.stringify(result), {
          headers: { "Content-Type": "application/json" },
        });
      }
      if (path.startsWith("/recall/")) {
        const id = path.split("/").pop()!;
        const entry = await this.recall(id);
        return new Response(
          JSON.stringify(entry || { error: "not_found" }),
          { status: entry ? 200 : 404, headers: { "Content-Type": "application/json" } },
        );
      }
      if (path === "/list" && request.method === "GET") {
        const type = url.searchParams.get("type") || undefined;
        const limit = parseInt(url.searchParams.get("limit") || "50");
        const offset = parseInt(url.searchParams.get("offset") || "0");
        const result = await this.list(type, limit, offset);
        return new Response(JSON.stringify(result), {
          headers: { "Content-Type": "application/json" },
        });
      }
      if (path.startsWith("/delete/") && request.method === "DELETE") {
        const id = path.split("/").pop()!;
        const result = await this.delete(id);
        return new Response(JSON.stringify(result), {
          headers: { "Content-Type": "application/json" },
        });
      }
      if (path === "/stats") {
        const result = await this.stats();
        return new Response(JSON.stringify(result), {
          headers: { "Content-Type": "application/json" },
        });
      }
      return new Response(
        JSON.stringify({
          endpoints: {
            store: "POST /store",
            recall: "GET /recall/:id",
            list: "GET /list?type=&limit=&offset=",
            delete: "DELETE /delete/:id",
            stats: "GET /stats",
          },
        }),
        { headers: { "Content-Type": "application/json" } },
      );
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      return new Response(JSON.stringify({ error: msg }), {
        status: 500,
        headers: { "Content-Type": "application/json" },
      });
    }
  }
}
