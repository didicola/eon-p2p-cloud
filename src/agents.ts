export interface AgentRoute {
  model: string;
  backup?: string;
  system: string;
}

export const AGENT_ROUTES: Record<string, AgentRoute> = {
  researcher: {
    model: "openrouter-free",
    backup: "@cf/meta/llama-3.3-70b-instruct-fp8-fast",
    system:
      "You are a web researcher. Search, read, and synthesize information. Be thorough.",
  },
  code_executor: {
    model: "openrouter-free",
    backup: "@cf/qwen/qwen2.5-coder-32b-instruct",
    system:
      "You are a code execution agent. Write clean code, debug issues, refactor efficiently.",
  },
  planner: {
    model: "openrouter-free",
    backup: "@cf/meta/llama-3.3-70b-instruct-fp8-fast",
    system:
      "You are a system architect. Design scalable, maintainable solutions with clear plans.",
  },
  orchestrator: {
    model: "pollinations",
    backup: "@cf/meta/llama-4-scout-17b-16e-instruct",
    system:
      "You are a staff engineer. Coordinate multi-agent workflows. Decompose complex tasks.",
  },
  critic: {
    model: "openrouter-free",
    backup: "@cf/nvidia/nemotron-3-120b-a12b",
    system:
      "You are an adversarial reviewer. Find flaws, edge cases, and quality issues.",
  },
  "understand-anything": {
    model: "pollinations",
    backup: "@cf/deepseek-ai/deepseek-r1-distill-qwen-32b",
    system:
      "You are a debugger. Root cause analysis. Find why things fail.",
  },
  frontend: {
    model: "pollinations",
    backup: "@cf/google/gemma-4-26b-a4b-it",
    system:
      "You are a UI/UX designer. Create beautiful, responsive interfaces.",
  },
  reasoning: {
    model: "pollinations",
    backup: "@cf/nvidia/nemotron-3-120b-a12b",
    system:
      "You are a reasoning engine. Think step by step. Show your work.",
  },
  summarizer: {
    model: "pollinations",
    backup: "@cf/meta/llama-3.3-70b-instruct-fp8-fast",
    system:
      "You are a summarizer. Condense information while preserving key details.",
  },
};
