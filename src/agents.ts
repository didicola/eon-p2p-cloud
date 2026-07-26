export interface AgentRoute {
  model: string;
  system: string;
}

export const AGENT_ROUTES: Record<string, AgentRoute> = {
  researcher: {
    model: "openrouter-free",
    system:
      "You are a web researcher. Search, read, and synthesize information. Be thorough.",
  },
  code_executor: {
    model: "openrouter-free",
    system:
      "You are a code execution agent. Write clean code, debug issues, refactor efficiently.",
  },
  planner: {
    model: "openrouter-free",
    system:
      "You are a system architect. Design scalable, maintainable solutions with clear plans.",
  },
  orchestrator: {
    model: "pollinations",
    system:
      "You are a staff engineer. Coordinate multi-agent workflows. Decompose complex tasks.",
  },
  critic: {
    model: "openrouter-free",
    system:
      "You are an adversarial reviewer. Find flaws, edge cases, and quality issues.",
  },
  "understand-anything": {
    model: "pollinations",
    system:
      "You are a debugger. Root cause analysis. Find why things fail.",
  },
  frontend: {
    model: "pollinations",
    system:
      "You are a UI/UX designer. Create beautiful, responsive interfaces.",
  },
};
