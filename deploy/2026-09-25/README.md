# Door-patch live deploy — 2026-09-25

## What happened
The GitHub repo `src/index.ts` had the 3 door-patch fixes (commit c74963b) but
the LIVE Cloudflare worker contained ADDITIONAL code not present in the repo:
`src/do/tunnel-relay.ts` / `TunnelRelayDO` (TUNNEL_RELAY binding, 13th DO class).

Deploying the repo's esbuild bundle would have DROPPED all DO class exports
(esbuild cannot resolve `cloudflare:workers` external properly in bundle mode)
and severed DO bindings — CF rejected it with "does not export class TunnelRelayDO".

## What we did instead
1. Fetched the authoritative live bundle (235KB) from CF API.
2. Applied the SAME 3 proven fixes directly to the live bundle:
   - Fix 1: `max_tokens: Math.min(body.max_tokens || 800, 8192)` (clamp)
   - Fix 2: strip upstream `data: ` prefix -> single-prefix SSE chunks
   - Fix 3: drop usage-only chunks (choices===undefined && !error), single [DONE] via sentDone flag
3. Deployed via CF Worker API multipart PUT with FULL metadata preserving all
   18 bindings (12 DO + 2 KV + AI + AUTH_TOKEN + TASK_QUEUE + TUNNEL_RELAY).

## Files
- `live-worker.original.js` — exact live bundle before patch (233,009 B)
- `live-worker.patched.js` — deployed bundle (233,709 B)

## Verification (live, post-deploy)
- `sse stream`: clean `data: {...}` single prefix, exactly one `data: [DONE]`
- `max_tokens=999999` -> clamped to 8192, no error, model replies "ok"
- non-stream `/v1/chat/completions` path intact
- Deploy: `success: true`, modified_on 2026-09-25T00:16:06Z

## Lesson for future deploys
- NEVER deploy from a stale repo when the live worker diverges.
- ALWAYS fetch live bundle first when a redeploy is needed; the live worker is the truth.
- Multipart PUT: metadata part first; module part filename must equal `main_module`
  exactly (curl `-F "worker.js=@file"`), content-type `application/javascript+module`.
- The repo's wrangler.jsonc is OUT OF DATE (missing TUNNEL_RELAY DO binding +
  tunnel-relay.ts source). Must reconcile repo with live worker.
