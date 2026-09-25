import { DurableObject } from "cloudflare:workers";
import { vaultEncryptObj, vaultDecryptObj } from "../eon-crypto";

/**
 * VaultStorageDO — Durable Object base with vault-encrypted storage helpers.
 *
 * Mirrors the LIVE worker's per-DO encPut/encGet/encList layer (reconciled
 * 2026-09-25). Live has been writing ALL durable state through
 * vaultEncryptObj; DO classes that read/write ctx.storage MUST extend this
 * base (or otherwise wrap storage) or they will read live-written ciphertext
 * as garbage.
 *
 * encGet/encList intentionally fall back to the raw value when decryption
 * fails (same as live), so mixed/legacy plaintext keys still survive.
 */
export class VaultStorageDO<Env = unknown> extends DurableObject<Env> {
  async encPut(key: string, value: unknown): Promise<void> {
    await this.ctx.storage.put(key, await vaultEncryptObj(value));
  }

  async encGet<T = unknown>(key: string): Promise<T | undefined> {
    const raw = (await this.ctx.storage.get(key)) as string | undefined;
    if (raw === undefined) return undefined;
    try {
      return (await vaultDecryptObj<T>(raw)) as T;
    } catch {
      return raw as unknown as T;
    }
  }

  async encList<T = unknown>(opts?: {
    prefix?: string;
    limit?: number;
  }): Promise<Map<string, T>> {
    const raw = await this.ctx.storage.list<unknown>(opts);
    const result = new Map<string, T>();
    for (const [k, v] of raw) {
      try {
        result.set(k, (await vaultDecryptObj<T>(v as string)) as T);
      } catch {
        result.set(k, v as T);
      }
    }
    return result;
  }
}