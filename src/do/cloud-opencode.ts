import { DurableObject } from "cloudflare:workers";
import { type Env } from "../types";
import { FREE_PROVIDERS, parseReply } from "../models";
import { AGENT_ROUTES } from "../agents";

/**
 * CloudOpencodeDO — multi-model LLM dispatch Durable Object.
 *
 * Provides a unified interface over:
 *   1. Workers AI models (Cloudflare's @cf/... models, tried in priority order)
 *   2. Free third-party LLM providers (pollinations, aiand, ...)
 *
 * Routes agent-type requests to the correct system prompt + model via AGENT_ROUTES.
 * Supports single-step dispatch and multi-step chain execution.
 */
export class CloudOpencodeDO extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
  }

  // -----------------------------------------------------------------------
  // Workers AI — try multiple models in priority order
  // -----------------------------------------------------------------------

  /**
   * Call Workers AI with a system+user prompt pair.  Tries models in
   * configured order until one returns a non-null result.
   *
   * @param system  System-level instruction
   * @param prompt  User query
   * @returns The model's text response, or null if all models failed.
   */
  async callWorkersAI(system: string, prompt: string, preferred?: string): Promise<string | null> {
    const modelsToTry = [
      ...(preferred ? [preferred] : []),
      "@cf/meta/llama-3.3-70b-instruct-fp8-fast",
      "@cf/meta/llama-4-scout-17b-16e-instruct",
      "@cf/qwen/qwen2.5-coder-32b-instruct",
      "@cf/mistralai/mistral-small-3.1-24b-instruct",
      "@cf/google/gemma-4-26b-a4b-it",
      "@cf/nvidia/nemotron-3-120b-a12b",
    ];

    for (const model of modelsToTry) {
      try {
        const res = (await this.env.AI.run(model, {
          messages: [
            { role: "system", content: system },
            { role: "user", content: prompt },
          ],
          max_tokens: 1000,
          // The streaming param is optional; include it for models that support it
          // stream: false,
        })) as { response?: string; choices?: { message?: { content?: string } }[] };

        const raw =
          res?.response ?? res?.choices?.[0]?.message?.content ?? "";
        const reply = parseReply(raw);
        if (reply) return reply;
      } catch (e) {
        console.warn(`Workers AI model ${model} failed:`, e);
      }
    }

    return null;
  }

  // -----------------------------------------------------------------------
  // Free LLM providers — try each in order
  // -----------------------------------------------------------------------

  /**
   * Call free third-party LLM providers (pollinations, aiand, etc).
   * Each provider is tried in order until one returns a non-null result.
   */
  async callFreeLLM(system: string, prompt: string): Promise<string | null> {
    for (const provider of FREE_PROVIDERS) {
      try {
        const res = await fetch(provider.url, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            model: provider.model,
            messages: [
              { role: "system", content: system },
              { role: "user", content: prompt },
            ],
            max_tokens: 1000,
          }),
          signal: AbortSignal.timeout(25_000),
        });

        if (res.ok) {
          const data = (await res.json()) as {
            choices?: { message?: { content?: string } }[];
          };
          const raw = data?.choices?.[0]?.message?.content ?? "";
          const reply = parseReply(raw);
          if (reply) return reply;
        }
      } catch (e) {
        console.warn(`Free LLM ${provider.name} failed:`, e);
      }
    }

    return null;
  }

  // -----------------------------------------------------------------------
  // Agent dispatch
  // -----------------------------------------------------------------------

  /**
   * Dispatch a prompt to a named agent type.  The agent's system prompt and
   * preferred model are read from AGENT_ROUTES.  Falls back: Workers AI
   * first, then free providers.
   *
   * @param agentType  e.g. "researcher", "code_executor", "critic", ...
   * @param prompt     The user's instruction for that agent
   * @returns The agent's response text, or an error message string.
   */
  async dispatch(agentType: string, prompt: string): Promise<string> {
    const route = AGENT_ROUTES[agentType];
    if (!route) {
      return `Unknown agent type: "${agentType}". Available: ${Object.keys(AGENT_ROUTES).join(", ")}`;
    }

    const result =
      (await this.callWorkersAI(route.system, prompt, route.backup)) ??
      (await this.callFreeLLM(route.system, prompt)) ??
      (await this.callWorkersAI(route.system, prompt));

    return result ?? `Cloud agent "${agentType}": all providers exhausted`;
  }

  // -----------------------------------------------------------------------
  // Multi-step chain
  // -----------------------------------------------------------------------

  /**
   * Run a multi-step chain where each step is dispatched to an agent and the
   * accumulated context is appended to each subsequent prompt.
   *
   * @param steps  Array of { agent, prompt } — executed sequentially.
   * @returns Array of response texts, one per step.
   */
  async runChain(
    steps: { agent: string; prompt: string }[],
  ): Promise<string[]> {
    const results: string[] = [];

    for (const step of steps) {
      const context =
        results.length > 0
          ? `\n\nContext so far:\n${results.join("\n---\n")}`
          : "";
      const result = await this.dispatch(step.agent, step.prompt + context);
      results.push(result);
    }

    return results;
  }

  // -----------------------------------------------------------------------
  // Fetch handler
  // -----------------------------------------------------------------------

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    try {
      if (path === "/dispatch" && request.method === "POST") {
        const body = (await request.json()) as {
          agent: string;
          prompt: string;
        };
        const result = await this.dispatch(body.agent, body.prompt);
        return new Response(
          JSON.stringify({ agent: body.agent, result }),
          { headers: { "Content-Type": "application/json" } },
        );
      }

      if (path === "/chain" && request.method === "POST") {
        const body = (await request.json()) as {
          steps: { agent: string; prompt: string }[];
        };
        const results = await this.runChain(body.steps);
        return new Response(
          JSON.stringify({ steps: body.steps.length, results }),
          { headers: { "Content-Type": "application/json" } },
        );
      }

      if (path === "/agents") {
        return new Response(
          JSON.stringify({
            agents: Object.keys(AGENT_ROUTES),
            count: Object.keys(AGENT_ROUTES).length,
          }),
          { headers: { "Content-Type": "application/json" } },
        );
      }

      return new Response(
        "CloudOpencodeDO — /dispatch, /chain, /agents",
        { status: 200 },
      );
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      console.error("CloudOpencodeDO error:", msg);
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
