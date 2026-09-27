# Fleet link (sealed D2573)

p2p-cloud (mission control) ↔ eon-fleet-cloud (ghost fleet pipeline):

- Fleet pipeline: every 10 min, health-checks ghost workers, pushes registry to
  `https://ghost.eon-sovereign.workers.dev`, broadcasts to Telegram.
- Mint machinery lives there: `scripts/fleet-mint.py` (mint) + `scripts/fleet-claim.py`
  (headless claim) + `scripts/cloud_rounds.json` (41 rounds, all dead as of Sep-02).
- Status: pipeline green; mint lanes die unclaimed (bot-wall) — pursuit work continues there.
- This repo consumes fleet state; it does not duplicate mint logic (D2540 keep-both).
