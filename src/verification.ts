import { type Env, type VerificationRequest, type VerificationResponse } from "./types";
import { getSwarmDO } from "./models";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/**
 * Workers AI models to try for sanity-check scoring, in priority order.
 * The first model that responds is used; the list provides resilience when
 * a particular model is rate-limited or unavailable.
 */
const SANITY_MODELS: string[] = [
  "@cf/meta/llama-3.3-70b-instruct-fp8-fast",
  "@cf/meta/llama-4-scout-17b-16e-instruct",
  "@cf/qwen/qwen2.5-coder-32b-instruct",
  "@cf/mistralai/mistral-small-3.1-24b-instruct",
  "@cf/google/gemma-4-26b-a4b-it",
];

// ---------------------------------------------------------------------------
// Sanity check
// ---------------------------------------------------------------------------

/**
 * Rate a (prompt, claimed result) pair on a quality scale from 0 to 100.
 *
 * Uses Workers AI with a small scoring-oriented system prompt.  Tries
 * multiple models in `SANITY_MODELS` order until one returns a parseable
 * numeric score.  Returns 0 when all models fail.
 *
 * @param env            Workers environment bindings (must have `AI`)
 * @param prompt         The original inference prompt
 * @param claimedResult  The result submitted by a peer
 * @returns A quality score between 0 and 100.
 */
export async function sanityCheck(
  env: Env,
  prompt: string,
  claimedResult: string,
): Promise<number> {
  const systemPrompt = [
    "You are a strict quality evaluator. Rate the following response on a scale of 0 to 100.",
    "Consider: relevance to the prompt, factual accuracy, coherence, and completeness.",
    "Respond with ONLY a single integer between 0 and 100. No explanation, no punctuation.",
  ].join(" ");

  const userPrompt = [
    `Prompt: "${prompt.slice(0, 2000)}"`,
    `Response: "${claimedResult.slice(0, 4000)}"`,
    "Score (0-100):",
  ].join("\n");

  for (const model of SANITY_MODELS) {
    try {
      const res = (await env.AI.run(model, {
        messages: [
          { role: "system", content: systemPrompt },
          { role: "user", content: userPrompt },
        ],
        max_tokens: 10,
      })) as { response?: string; choices?: { message?: { content?: string } }[] };

      const raw =
        res?.response ?? res?.choices?.[0]?.message?.content ?? "";
      const parsed = parseInt(raw.trim(), 10);

      if (!isNaN(parsed) && parsed >= 0 && parsed <= 100) {
        return parsed;
      }
    } catch (err) {
      console.warn(`verification: sanityCheck model ${model} failed:`, err);
    }
  }

  return 0;
}

// ---------------------------------------------------------------------------
// Redundant verification
// ---------------------------------------------------------------------------

/**
 * Enqueue the same inference task to `count` different peers and return
 * their results for comparison.
 *
 * Each peer receives an identical prompt.  The caller can use
 * `majorityAgreement()` or custom logic to determine consensus.
 *
 * @param env     Workers environment bindings
 * @param swarm   The swarm DO stub to enqueue tasks on
 * @param prompt  The prompt to dispatch
 * @param model   The model id for routing
 * @param count   Number of redundant copies to enqueue (default 3)
 * @returns Array of `{ peerId, result }` for completed tasks.
 */
export async function redundantVerify(
  env: Env,
  swarm: DurableObjectStub,
  prompt: string,
  model: string,
  count: number = 3,
): Promise<{ peerId: string; result: string }[]> {
  const taskIds: string[] = [];

  // Enqueue count copies of the same task
  for (let i = 0; i < count; i++) {
    const resp = await swarm.fetch("http://internal/task", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ model, prompt }),
    });
    if (resp.ok) {
      const body = (await resp.json()) as { taskId: string };
      taskIds.push(body.taskId);
    }
  }

  if (taskIds.length === 0) return [];

  // Poll for completion (up to ~30 s)
  const results: { peerId: string; result: string }[] = [];
  const deadline = Date.now() + 30_000;

  while (Date.now() < deadline && results.length < taskIds.length) {
    for (const taskId of taskIds) {
      // Skip already-collected
      if (results.some((r) => r.peerId === taskId)) continue;

      const resp = await swarm.fetch(`http://internal/task/${taskId}`, {
        method: "GET",
      });

      if (resp.ok) {
        const task = (await resp.json()) as {
          id: string;
          result?: string;
          done: boolean;
          claimed?: string;
        };
        if (task.done && task.result) {
          results.push({
            peerId: task.claimed ?? task.id,
            result: task.result,
          });
        }
      }
    }

    if (results.length < taskIds.length) {
      await sleep(500);
    }
  }

  return results;
}

// ---------------------------------------------------------------------------
// Response scoring
// ---------------------------------------------------------------------------

/**
 * Score a single response against its prompt using Workers AI.
 *
 * Returns a value between 0 and 100 where higher is better.
 * Falls back through multiple models if needed.
 *
 * @param env       Workers environment bindings
 * @param prompt    The original prompt
 * @param response  The response text to evaluate
 * @returns A score 0-100, or 0 if all models fail.
 */
export async function scoreResponse(
  env: Env,
  prompt: string,
  response: string,
): Promise<number> {
  return sanityCheck(env, prompt, response);
}

// ---------------------------------------------------------------------------
// Majority agreement
// ---------------------------------------------------------------------------

/**
 * Determine majority agreement among a set of results.
 *
 * If at least 2/3 of the results share the same answer, that answer is
 * returned.  Otherwise returns null (no consensus).
 *
 * Comparison is done on the full result string after trimming whitespace.
 *
 * @param results  Array of result strings from redundant peers
 * @returns The majority result if consensus exists, otherwise null.
 */
export function majorityAgreement(
  results: string[],
): string | null {
  if (results.length === 0) return null;

  const threshold = Math.ceil((2 / 3) * results.length);

  // Count occurrences of each result
  const counts = new Map<string, number>();
  for (const r of results) {
    const key = r.trim();
    counts.set(key, (counts.get(key) ?? 0) + 1);
  }

  // Find the first result that meets the threshold
  let best: string | null = null;
  for (const [result, count] of counts) {
    if (count >= threshold) {
      // If multiple meet the threshold, pick the one with the most votes
      if (best === null || count > (counts.get(best) ?? 0)) {
        best = result;
      }
    }
  }

  return best;
}

// ---------------------------------------------------------------------------
// Convenience: full verification flow
// ---------------------------------------------------------------------------

/**
 * Run a full verification flow: redundant dispatch + sanity check.
 *
 * Dispatches the prompt to `count` peers, polls for results, runs a sanity
 * check on each, and returns a `VerificationResponse`.
 *
 * @param env     Workers environment bindings
 * @param swarm   The swarm DO stub
 * @param prompt  The prompt to verify
 * @param model   Model id for routing
 * @param count   Number of redundant peers (default 3)
 * @returns A VerificationResponse summarising the outcome.
 */
export async function verifyTask(
  env: Env,
  swarm: DurableObjectStub,
  prompt: string,
  model: string,
  count: number = 3,
): Promise<VerificationResponse> {
  const peerResults = await redundantVerify(env, swarm, prompt, model, count);

  if (peerResults.length === 0) {
    return {
      taskId: "",
      score: 0,
      verified: false,
      confidence: 0,
      method: "redundant",
    };
  }

  // Majority agreement
  const results = peerResults.map((r) => r.result);
  const consensus = majorityAgreement(results);

  // Sanity-check the consensus (or the first result if no consensus)
  const bestResult = consensus ?? results[0];
  const score = await sanityCheck(env, prompt, bestResult);

  const confidence = results.length / count;

  return {
    taskId: "",
    score,
    verified: score >= 50 && consensus !== null,
    confidence,
    method: consensus ? "redundant" : "sanity_check",
  };
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
