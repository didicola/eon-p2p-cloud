// EON vault — AES-GCM / PBKDF2 encryption helpers.
// Reconstructed from the LIVE worker bundle (2026-09-25) — this file existed
// only in the deployed Cloudflare worker and was never pushed to git.
const ALGORITHM = "AES-GCM";
const KEY_LENGTH = 256;
const IV_LENGTH = 12;
const ITERATIONS = 1e5;
const encoder = new TextEncoder();
const decoder = new TextDecoder();

let _cachedKey: CryptoKey | null = null;
let _keyMaterial: string | null = null;

function getKeyMaterial(): string {
  if (_keyMaterial) return _keyMaterial;
  _keyMaterial =
    (globalThis as any).__EON_VAULT_KEY__ ||
    "eon-vault-v3-sovereign-fleet-2026-quantum-resistant-aes256gcm";
  return _keyMaterial;
}

async function deriveKey(salt: Uint8Array): Promise<CryptoKey> {
  const keyMaterial = await crypto.subtle.importKey(
    "raw",
    encoder.encode(getKeyMaterial()),
    "PBKDF2",
    false,
    ["deriveKey"]
  );
  return crypto.subtle.deriveKey(
    {
      name: "PBKDF2",
      salt,
      iterations: ITERATIONS,
      hash: "SHA-256",
    },
    keyMaterial,
    { name: ALGORITHM, length: KEY_LENGTH },
    false,
    ["encrypt", "decrypt"]
  );
}

async function getKey(): Promise<CryptoKey> {
  if (_cachedKey) return _cachedKey;
  const salt = encoder.encode("eon-vault-static-salt-v3");
  _cachedKey = await deriveKey(new Uint8Array(salt));
  return _cachedKey;
}

export async function vaultEncrypt(plaintext: string): Promise<string> {
  const key = await getKey();
  const iv = crypto.getRandomValues(new Uint8Array(IV_LENGTH));
  const encrypted = await crypto.subtle.encrypt(
    { name: ALGORITHM, iv },
    key,
    encoder.encode(plaintext)
  );
  const combined = new Uint8Array(iv.length + encrypted.byteLength);
  combined.set(iv, 0);
  combined.set(new Uint8Array(encrypted), iv.length);
  return btoa(String.fromCharCode(...combined));
}

export async function vaultDecrypt(ciphertext: string): Promise<string> {
  try {
    const key = await getKey();
    const raw = Uint8Array.from(atob(ciphertext), (c) => c.charCodeAt(0));
    const iv = raw.slice(0, IV_LENGTH);
    const data = raw.slice(IV_LENGTH);
    const decrypted = await crypto.subtle.decrypt(
      { name: ALGORITHM, iv },
      key,
      data
    );
    return decoder.decode(decrypted);
  } catch {
    return ciphertext;
  }
}

export async function vaultEncryptObj(obj: unknown): Promise<string> {
  return vaultEncrypt(JSON.stringify(obj));
}

export async function vaultDecryptObj<T = unknown>(blob: string): Promise<T | string> {
  const raw = await vaultDecrypt(blob);
  try {
    return JSON.parse(raw) as T;
  } catch {
    return raw;
  }
}

// ─────────────────────────────────────────────────────────────
// Durable Object storage helpers (encPut/encGet/encList).
// Mirrors the LIVE worker's per-DO enc* layer (reconciled 2026-09-25):
// every DO in the deployed worker wraps ctx.storage with vault
// encryption. The repo's DO classes must do the same or they will
// read live-written ciphertext as garbage.
// ─────────────────────────────────────────────────────────────
type StorageLike = {
  put(key: string, value: unknown): Promise<void>;
  get(key: string): Promise<unknown>;
  list<T = unknown>(opts?: { prefix?: string; limit?: number }): Promise<Map<string, T>>;
  delete(key: string): Promise<void>;
};

export async function encPut(storage: StorageLike, key: string, value: unknown): Promise<void> {
  await storage.put(key, await vaultEncryptObj(value));
}

export async function encGet<T = unknown>(storage: StorageLike, key: string): Promise<T | undefined> {
  const raw = (await storage.get(key)) as string | undefined;
  if (raw === undefined) return undefined;
  try {
    return (await vaultDecryptObj<T>(raw)) as T;
  } catch {
    return raw as unknown as T;
  }
}

export async function encList<T = unknown>(storage: StorageLike, opts?: { prefix?: string; limit?: number }): Promise<Map<string, T>> {
  const raw = await storage.list<unknown>(opts);
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