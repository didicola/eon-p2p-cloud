import { DurableObject } from "cloudflare:workers";
import { VaultStorageDO } from "./vault-storage";
import { type Env, type AccountRecord, type ProviderType } from "../types";

const STORAGE_KEY = "accounts";

export class AccountManagerDO extends VaultStorageDO<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
  }

  async register(record: AccountRecord): Promise<string> {
    const accounts = await encGet<AccountRecord[]>(this.ctx.storage, STORAGE_KEY) || [];
    const existing = accounts.find(a => a.alias === record.alias && a.provider === record.provider);
    if (existing) {
      Object.assign(existing, record);
    } else {
      accounts.push(record);
    }
    await this.encPut(STORAGE_KEY, accounts);
    return `${record.provider}:${record.alias} registered`;
  }

  async list(provider?: string): Promise<AccountRecord[]> {
    const accounts = await encGet<AccountRecord[]>(this.ctx.storage, STORAGE_KEY) || [];
    if (provider) return accounts.filter(a => a.provider === provider);
    return accounts;
  }

  async get(provider: string, alias: string): Promise<AccountRecord | null> {
    const accounts = await encGet<AccountRecord[]>(this.ctx.storage, STORAGE_KEY) || [];
    return accounts.find(a => a.provider === provider && a.alias === alias) || null;
  }

  async remove(provider: string, alias: string): Promise<string> {
    const accounts = await encGet<AccountRecord[]>(this.ctx.storage, STORAGE_KEY) || [];
    const remaining = accounts.filter(a => !(a.provider === provider && a.alias === alias));
    await this.encPut(STORAGE_KEY, remaining);
    return `${provider}:${alias} removed`;
  }

  async rotate(provider: string, excludeAlias?: string): Promise<AccountRecord | null> {
    const accounts = await encGet<AccountRecord[]>(this.ctx.storage, STORAGE_KEY) || [];
    const active = accounts
      .filter(a => a.provider === provider && a.status === "active" && a.alias !== excludeAlias)
      .sort((a, b) => {
        const aRatio = a.usage.requestsLimit > 0 ? a.usage.requestsUsed / a.usage.requestsLimit : 0;
        const bRatio = b.usage.requestsLimit > 0 ? b.usage.requestsUsed / b.usage.requestsLimit : 0;
        return aRatio - bRatio;
      });
    return active[0] || null;
  }

  async trackUsage(provider: string, alias: string, tokensUsed: number): Promise<void> {
    const accounts = await encGet<AccountRecord[]>(this.ctx.storage, STORAGE_KEY) || [];
    const account = accounts.find(a => a.provider === provider && a.alias === alias);
    if (account) {
      account.usage.requestsUsed++;
      account.usage.tokensUsed += tokensUsed;
      account.lastCheck = Date.now();
      await this.encPut(STORAGE_KEY, accounts);
    }
  }
}
