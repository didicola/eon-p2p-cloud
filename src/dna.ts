// EON Sovereign DNA — mesh identity manifest.
// Reconstructed from the LIVE worker bundle (2026-09-25) — this file existed
// only in the deployed Cloudflare worker and was never pushed to git.
export const DNA_MANIFEST = {
  manifest_version: 1,
  twin_id: "opencode-twin-v1",
  name: "OpenCode Twin DNA Bundle",
  created_at: "2026-07-31T13:33:00Z",
  created_by: "eon-fleet (sovereign architecture)",
  purpose:
    "Step 4 of the parallel-world build: deliver the full opencode core + architecture + DNA to AI Cloud / AI Web so they can create their OWN twins with their OWN domains, then resend the new twin for ins",
  dna_sources: [
    {
      machine: "ubuntu",
      path: "/home/ricos/.config/opencode/",
      components: ["opencode.jsonc", "opencode.json", "AGENTS.md", "agents/"],
    },
    {
      machine: "termux-samsung",
      path: "~/termux-mirror/.config/opencode/",
      components: ["opencode.json", "opencode.jsonc", "AGENTS.md", "agents/"],
    },
  ],
  architecture_docs: [
    "PARALLEL_WORLD_ARCHITECTURE.md",
    "AI_CLOUD_ARCHITECTURE.md",
    "PARALLEL_WORLD_SIGNAL.md",
    "EON_DREAM.md",
  ],
  core_brain: {
    eon_mega_brain_v6_py:
      "8 brain regions (cortex, prefrontal, hippocampus, thalamus, brainstem, limbic) + 8 cloud workers",
    eon_unified_py: "unified AGI entry point",
  },
  model_grid: {
    providers: 39,
    workers: 8,
    models: 668,
    services: 151,
    routing: "9-tier $0 fallback via blind-proxy :8090",
    note: "blind-proxy maps model families to free tier (OpenRouter :free -> freellmapi -> TFG -> Mistral -> HuggingFace -> Cerebras -> SambaNova -> BazaarLink)",
  },
  mesh_topology: {
    ubuntu: "master coordinator (eon-master-daemon, heartbeat ubuntu->cloud memory)",
    termux_samsung: "node 5 cloud-opencode (blind-proxy :8090, matrix :8201, socat :8443)",
    ai_cloud:
      "cloud-brain-proxy, eon-datacenter (D1+VOL), ai-cloud-space (KV+D1), sovereign-llm, delegate-relay, bot-router, edge-proxy, ghost-swarm-relay, eon-master-bridge, memory-cache",
    ai_web:
      "eon-site, eon-sovereign-site, asi-sovereign-chat, dashboard, asi-aiops-dashboard, asi-parallel-console",
  },
  free_forever: true,
} as const;