# EON P2P Cloud

P2P cloud computing platform with multi-provider AI inference, auto-rotation matrix, and system upgrade.

## Architecture

- **Primary**: https://eon-p2p-cloud.exportdefaultasyncfetchrequestenvconsturl.workers.dev
- **Companion**: http://127.0.0.1:8089 (v3.5 — 13 matrices for memory, truth, thought, search, research, web, storage, mcp, files, code, config, project, data)
- **Blind proxy**: :8090 (523 models, 9-tier fallback)
- **OmniRoute**: :20128 (99 models)

## Deploy

```bash
cd eon-p2p-cloud && npx wrangler deploy
```

## System Matrix

See `http://127.0.0.1:8089/system` for all endpoints.
