import { DurableObject } from "cloudflare:workers";
import { type Env, type CreditEntry } from "../types";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const REDEMPTION_WINDOW_MS = 86_400_000; // 24 h
const MAX_REDEMPTIONS_PER_WINDOW = 3;

/**
 * IncentiveDO — credit and reward accounting for P2P compute peers (Phase 2).
 *
 * Each peer earns credits for every task it completes.  Credits can be
 * redeemed (stub) to trigger a payout.
 *
 * Rate limits:
 *   - Redemption is limited to MAX_REDEMPTIONS_PER_WINDOW per sliding 24 h
 *     window.
 */
export class IncentiveDO extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
  }

  // -----------------------------------------------------------------------
  // Credits
  // -----------------------------------------------------------------------

  /**
   * Add credits for a completed task.
   *
   * @param peerId          The peer that performed the work
   * @param taskId          The completed task identifier
   * @param tokensGenerated Number of output tokens (used as a proxy for work)
   */
  async creditTask(
    peerId: string,
    taskId: string,
    tokensGenerated: number,
  ): Promise<CreditEntry> {
    const entry: CreditEntry = {
      peerId,
      amount: this.computeCreditAmount(tokensGenerated),
      reason: "task_completed",
      taskId,
      timestamp: Date.now(),
    };

    // Append to the peer's credit ledger
    const ledger = await this.getLedger(peerId);
    ledger.push(entry);
    await this.ctx.storage.put(ledgerKey(peerId), ledger);

    // Update aggregate balance
    const balance = await this.getBalance(peerId);
    balance.earned += entry.amount;
    balance.net += entry.amount;
    await this.ctx.storage.put(balanceKey(peerId), balance);

    return entry;
  }

  /**
   * Get the current balance for a peer.
   */
  async getBalance(peerId: string): Promise<PeerBalance> {
    const stored = await this.ctx.storage.get<PeerBalance>(
      balanceKey(peerId),
    );
    if (stored) return stored;

    const initial: PeerBalance = {
      earned: 0,
      redeemed: 0,
      net: 0,
      lastRedemption: 0,
    };
    return initial;
  }

  /**
   * Initiate a payout (stub).  Logs intent and records the redemption.
   *
   * Rate-limited to MAX_REDEMPTIONS_PER_WINDOW per sliding 24 h window.
   *
   * @returns { ok, reason, amount } describing the result.
   */
  async redeem(peerId: string): Promise<{
    ok: boolean;
    reason: string;
    amount: number;
  }> {
    const balance = await this.getBalance(peerId);
    if (balance.net <= 0) {
      return { ok: false, reason: "no_balance", amount: 0 };
    }

    // Sliding window rate limit check
    const redemptions = await this.getRedemptionTimestamps(peerId);
    const windowStart = Date.now() - REDEMPTION_WINDOW_MS;
    const recent = redemptions.filter((ts) => ts > windowStart);

    if (recent.length >= MAX_REDEMPTIONS_PER_WINDOW) {
      const oldestAllowed = recent[0] + REDEMPTION_WINDOW_MS;
      return {
        ok: false,
        reason: "rate_limited",
        amount: 0,
      };
    }

    // Stub payout — log intent instead of executing an on-chain tx
    const amount = Math.floor(balance.net);
    console.log(
      `IncentiveDO: PAYOUT INTENT peer=${peerId} amount=${amount} (stub)`,
    );

    // Record the redemption
    redemptions.push(Date.now());
    await this.ctx.storage.put(redemptionKey(peerId), redemptions);

    balance.redeemed += amount;
    balance.net -= amount;
    balance.lastRedemption = Date.now();
    await this.ctx.storage.put(balanceKey(peerId), balance);

    return { ok: true, reason: "payout_initiated", amount };
  }

  /**
   * Get the full credit ledger for a peer.
   */
  async getLedger(peerId: string): Promise<CreditEntry[]> {
    return (await this.ctx.storage.get<CreditEntry[]>(ledgerKey(peerId))) ?? [];
  }

  // -----------------------------------------------------------------------
  // Internal helpers
  // -----------------------------------------------------------------------

  /**
   * Compute credits from token count.  Currently 1 token = 1 credit, but
   * this can be tuned later to account for model size or execution time.
   */
  private computeCreditAmount(tokensGenerated: number): number {
    return Math.max(1, Math.floor(tokensGenerated));
  }

  private async getRedemptionTimestamps(
    peerId: string,
  ): Promise<number[]> {
    return (
      (await this.ctx.storage.get<number[]>(redemptionKey(peerId))) ?? []
    );
  }

  // -----------------------------------------------------------------------
  // Fetch handler
  // -----------------------------------------------------------------------

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    try {
      if (path === "/credit" && request.method === "POST") {
        const body = (await request.json()) as {
          peerId: string;
          taskId: string;
          tokensGenerated: number;
        };
        const entry = await this.creditTask(
          body.peerId,
          body.taskId,
          body.tokensGenerated,
        );
        return new Response(JSON.stringify(entry), {
          headers: { "Content-Type": "application/json" },
        });
      }

      if (path.startsWith("/balance/")) {
        const peerId = path.split("/").pop()!;
        const balance = await this.getBalance(peerId);
        return new Response(
          JSON.stringify({ peerId, ...balance }),
          { headers: { "Content-Type": "application/json" } },
        );
      }

      if (path.startsWith("/redeem/") && request.method === "POST") {
        const peerId = path.split("/").pop()!;
        const result = await this.redeem(peerId);
        return new Response(JSON.stringify(result), {
          headers: { "Content-Type": "application/json" },
        });
      }

      if (path.startsWith("/ledger/")) {
        const peerId = path.split("/").pop()!;
        const ledger = await this.getLedger(peerId);
        return new Response(
          JSON.stringify({ peerId, entries: ledger }),
          { headers: { "Content-Type": "application/json" } },
        );
      }

      return new Response(
        "IncentiveDO — /credit, /balance/:peerId, /redeem/:peerId, /ledger/:peerId",
        { status: 200 },
      );
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      console.error("IncentiveDO error:", msg);
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
// Internal types & storage key helpers
// ---------------------------------------------------------------------------

interface PeerBalance {
  earned: number;
  redeemed: number;
  net: number;
  lastRedemption: number;
}

function balanceKey(peerId: string): string {
  return `balance:${peerId}`;
}

function ledgerKey(peerId: string): string {
  return `ledger:${peerId}`;
}

function redemptionKey(peerId: string): string {
  return `redemption:${peerId}`;
}
