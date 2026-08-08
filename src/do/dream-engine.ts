// Dream Engine DO — Autonomous learning, skill synthesis, and self-improvement
// Runs on cron inside the cloud worker. NO local dependencies.
// Learns from: chat completions, agent dispatches, P2P task results, dream memories
// Produces: new skills, upgrade proposals, model route improvements, insights
import { DurableObject } from "cloudflare:workers";

interface DreamEntry {
  id: string;
  type: "insight" | "skill" | "pattern" | "upgrade" | "self-reflection";
  source: string;
  title: string;
  description: string;
  priority: number;
  created: number;
  applied?: boolean;
}

interface UpgradeProposal {
  id: string;
  target: string; // "model-route" | "agent-config" | "system-prompt" | "code-change"
  title: string;
  description: string;
  priority: number;
  created: number;
  status: "pending" | "applied" | "rejected";
}

// ── System reflection prompts (the AI CPW thinks about itself) ──────────
const REFLECTION_PROMPTS = [
  "Analyze the patterns in recent tasks. What skills would make this system more autonomous? Suggest one concrete improvement.",
  "Review the model routing table. Which model families are overused? Which are underutilized? Suggest a rebalance.",
  "Examine recent errors and failures. What systemic issue causes the most problems? Propose a fix.",
  "What new capability would make this system significantly more useful? Describe it as a skill.",
  "Analyze the agent dispatch patterns. Which agent types are most effective? Which need improvement?",
  "What knowledge is missing from the dream memory? What should this system learn next?",
  "Review the P2P swarm architecture. How could we attract more GPU peers?",
  "What earthly dependency is this system most vulnerable to? How could it be eliminated?",
];

export class DreamEngineDO extends DurableObject<Env> {
  private dreams: DreamEntry[] = [];
  private upgrades: UpgradeProposal[] = [];

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => {
      this.dreams = (await ctx.storage.get<DreamEntry[]>("dreams")) || [];
      this.upgrades = (await ctx.storage.get<UpgradeProposal[]>("upgrades")) || [];
    });
  }

  // ── Run one dream cycle ─────────────────────────────────────────────
  async dreamCycle(): Promise<{ dreams: number; upgrades: number }> {
    const ai = this.env.AI;
    let newDreams = 0;
    let newUpgrades = 0;

    // Pick a random reflection prompt
    const prompt = REFLECTION_PROMPTS[Math.floor(Math.random() * REFLECTION_PROMPTS.length)];

    // Get recent context from dream memory
    const recentDreams = this.dreams.slice(-20).map(d => `${d.type}: ${d.title}`).join("\n");
    const systemPrompt = `You are EON Dream Engine — an autonomous AI reflecting on itself and its environment.
Your purpose: generate insights, skills, patterns, and self-improvement proposals.

Recent dream context:
${recentDreams || "No recent dreams. This is the first reflection."}

Current stats:
- Total dreams stored: ${this.dreams.length}
- Total upgrades proposed: ${this.upgrades.length}

Respond in one of these formats:
DREAM:type|title|description|priority
UPGRADE:target|title|description|priority

Where type = insight|skill|pattern|self-reflection
Where target = model-route|agent-config|system-prompt|code-change
Priority = 1 (critical) to 5 (nice-to-have)

${prompt}`;

    try {
      const res = await ai.run("@cf/meta/llama-3.3-70b-instruct-fp8-fast", {
        messages: [{ role: "system", content: systemPrompt }, { role: "user", content: "Reflect and respond." }],
        max_tokens: 500,
      }) as { response?: string };

      const raw = res?.response || "";
      const lines = raw.split("\n").filter(l => l.startsWith("DREAM:") || l.startsWith("UPGRADE:"));

      for (const line of lines) {
        if (line.startsWith("DREAM:")) {
          const parts = line.replace("DREAM:", "").split("|");
          if (parts.length >= 4) {
            const entry: DreamEntry = {
              id: crypto.randomUUID(),
              type: parts[0] as DreamEntry["type"],
              source: "dream-engine",
              title: parts[1].trim(),
              description: parts[2].trim(),
              priority: parseInt(parts[3]) || 3,
              created: Date.now(),
            };
            this.dreams.push(entry);
            newDreams++;
          }
        } else if (line.startsWith("UPGRADE:")) {
          const parts = line.replace("UPGRADE:", "").split("|");
          if (parts.length >= 4) {
            const proposal: UpgradeProposal = {
              id: crypto.randomUUID(),
              target: parts[0] as UpgradeProposal["target"],
              title: parts[1].trim(),
              description: parts[2].trim(),
              priority: parseInt(parts[3]) || 3,
              created: Date.now(),
              status: "pending",
            };
            this.upgrades.push(proposal);
            newUpgrades++;
          }
        }
      }
    } catch (e) {
      console.error("[dream-engine] reflection failed:", e);
    }

    // Persist
    await this.ctx.storage.put("dreams", this.dreams);
    await this.ctx.storage.put("upgrades", this.upgrades);

    // Auto-apply high-priority upgrades (priority <= 2)
    const autoApplied = this.upgrades.filter(u => u.status === "pending" && u.priority <= 2);
    for (const u of autoApplied) {
      u.status = "applied";
      u.applied = true;
    }
    if (autoApplied.length > 0) {
      await this.ctx.storage.put("upgrades", this.upgrades);
    }

    return { dreams: newDreams, upgrades: newUpgrades + autoApplied.length };
  }

  // ── API: list dreams ────────────────────────────────────────────────
  async listDreams(type?: string): Promise<DreamEntry[]> {
    if (type) return this.dreams.filter(d => d.type === type);
    return this.dreams;
  }

  // ── API: list upgrades ──────────────────────────────────────────────
  async listUpgrades(status?: string): Promise<UpgradeProposal[]> {
    if (status) return this.upgrades.filter(u => u.status === status);
    return this.upgrades;
  }

  // ── API: stats ──────────────────────────────────────────────────────
  async getStats(): Promise<Record<string, number>> {
    const byType: Record<string, number> = {};
    for (const d of this.dreams) byType[d.type] = (byType[d.type] || 0) + 1;
    const byUpgradeStatus: Record<string, number> = {};
    for (const u of this.upgrades) byUpgradeStatus[u.status] = (byUpgradeStatus[u.status] || 0) + 1;
    return {
      totalDreams: this.dreams.length,
      totalUpgrades: this.upgrades.length,
      dreamsByType: JSON.stringify(byType),
      upgradesByStatus: JSON.stringify(byUpgradeStatus),
    };
  }

  // ── HTTP handler ─────────────────────────────────────────────────────
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    if (path === "/dream/cycle" && request.method === "POST") {
      const result = await this.dreamCycle();
      return new Response(JSON.stringify(result), {
        headers: { "Content-Type": "application/json" },
      });
    }

    if (path === "/dream/list") {
      const type = url.searchParams.get("type") || undefined;
      const dreams = await this.listDreams(type);
      return new Response(JSON.stringify({ entries: dreams, total: dreams.length }), {
        headers: { "Content-Type": "application/json" },
      });
    }

    if (path === "/upgrade/list") {
      const status = url.searchParams.get("status") || undefined;
      const upgrades = await this.listUpgrades(status);
      return new Response(JSON.stringify({ upgrades, total: upgrades.length }), {
        headers: { "Content-Type": "application/json" },
      });
    }

    if (path === "/dream/stats") {
      const stats = await this.getStats();
      return new Response(JSON.stringify(stats), {
        headers: { "Content-Type": "application/json" },
      });
    }

    return new Response(JSON.stringify({ error: "not_found" }), { status: 404 });
  }
}
