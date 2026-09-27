#!/data/data/com.termux/files/usr/bin/bash
# EON TWIN RECOVERY — one-shot, safe, idempotent.
# Run:  curl -sL https://raw.githubusercontent.com/didicola/eon-p2p-cloud/master/ops/twin-recover.sh | bash
#
# HARD RULE: never kills opencode / eon-opencode-loop. Only touches EON daemons.
# Fixes the SIGKILL feedback loop found 2026-09-27:
#   1) boot script had no singleton guard -> one keepalive per Termux:Boot trigger
#   2) keepalive used `setsid nohup X &` -> leaks 2 subshells stuck in do_wait per resurrect
#   3) ~40 Termux procs > Android's 32-process phantom-kill threshold
#   4) two sshd supervisors + two runit generations

H=/data/data/com.termux/files/home
PREFIX=/data/data/com.termux/files/usr
LOG=$H/eon-keepalive.log
say() { echo "[recover] $*"; }

# ---------- 0. preflight ----------
if pgrep -f "eon-stack-keepalive" >/dev/null 2>&1; then
  say "keepalive already running — leaving it (idempotent)"
else
  say "no keepalive running"
fi
say "procs before: $(pgrep -u 10370 2>/dev/null | wc -l) | opencode: $(pgrep -x opencode 2>/dev/null | tr '\n' ',' )"

# ---------- 1. singleton guard + no subshell leak in keepalive ----------
if [ -f "$H/eon-stack-keepalive.sh" ]; then
  cp -n "$H/eon-stack-keepalive.sh" "$H/eon-stack-keepalive.sh.bak-$(date +%Y%m%d-%H%M%S)" 2>/dev/null
  # inject flock singleton + spawn() helper + full PATH if not already present
  grep -q "flock -n 9" "$H/eon-stack-keepalive.sh" || python3 - "$H/eon-stack-keepalive.sh" <<'PY'
import sys
p = sys.argv[1]
src = open(p).read()
anchor = 'LOG=$H/eon-keepalive.log'
add = anchor + '''
export PATH="/data/data/com.termux/files/usr/bin:$PATH"
# singleton: only one supervisor may run (Android re-fires Termux:Boot often)
PIDF=$H/.eon-keepalive.lock
exec 9>"$PIDF" || exit 0
flock -n 9 || exit 0
# spawn: setsid -f force-forks so no bash subshell is left waiting in do_wait
spawn() { ( setsid -f "$@" </dev/null >>"$LOG" 2>&1 & ) ; }'''
if "flock -n 9" not in src:
    src = src.replace(anchor, add, 1)
    # replace the leaking spawn form with the helper
    src = src.replace('setsid nohup python3 "$H/coord_poller.py" >>"$H/coord_poller.log" 2>&1 &',
                      'spawn python3 "$H/coord_poller.py" >>"$H/coord_poller.log"')
    src = src.replace('setsid nohup python3 "$H/sfx7.py" >>"$LOG" 2>&1 &',
                      'spawn python3 "$H/sfx7.py" >>"$LOG"')
    src = src.replace('setsid nohup bash "$H/eon-e2e-loop.sh" >>"$H/eon-e2e-loop.log" 2>&1 &',
                      'spawn bash "$H/eon-e2e-loop.sh" >>"$H/eon-e2e-loop.log"')
    src = src.replace('setsid nohup bash "$H/eon-swarm-hb.sh" >>"$H/eon-swarm-hb.log" 2>&1 &',
                      'spawn bash "$H/eon-swarm-hb.sh" >>"$H/eon-swarm-hb.log"')
    open(p, "w").write(src)
    print("[recover] keepalive patched: flock singleton + spawn() helper + full PATH")
else:
    print("[recover] keepalive already patched")
PY
  bash -n "$H/eon-stack-keepalive.sh" && say "keepalive syntax OK" || say "keepalive SYNTAX ERROR"
fi

# ---------- 2. boot script singleton (prevents future multiplication) ----------
BOOT=$H/.termux/boot/00-eon-keepalive.sh
if [ -f "$BOOT" ] && ! grep -q "flock" "$BOOT"; then
  cp -n "$BOOT" "$BOOT.bak-$(date +%Y%m%d-%H%M%S)" 2>/dev/null
  sed -i '2i exec 9>/data/data/com.termux/files/home/.eon-boot.lock; flock -n 9 || exit 0' "$BOOT"
  say "boot script guarded with flock"
fi

# ---------- 3. stop duplicate/stale keepalives (NOT opencode, NOT daemons) ----------
KA=$(pgrep -f "eon-stack-keepalive" | tr '\n' ' ')
if [ -n "$KA" ]; then
  KEEP=$(echo "$KA" | tr ' ' '\n' | tail -1)
  for p in $KA; do
    [ "$p" = "$KEEP" ] && continue
    [ "$p" = "$$" ] && continue
    kill "$p" 2>/dev/null && say "stopped duplicate keepalive pid=$p (kept $KEEP)"
  done
fi

# ---------- 4. reap leaked subshells (bash procs stuck in do_wait, not opencode) ----------
LEAK=0
for p in $(pgrep -f "eon-stack-keepalive" 2>/dev/null); do
  for c in $(pgrep -P "$p" 2>/dev/null); do
    W=$(cat /proc/$c/wchan 2>/dev/null)
    if [ "$W" = "do_wait" ]; then kill "$c" 2>/dev/null && LEAK=$((LEAK+1)); fi
  done
done
say "reaped $LEAK stuck subshell(es)"

# ---------- 5. get under the 32-process threshold: park unused legacy services ----------
for s in ftpd telnetd busybox-httpd cloudflared eon-sovereign; do
  d="$PREFIX/var/service/$s"
  [ -d "$d" ] && [ ! -d "$d.disabled" ] && mv "$d" "$d.disabled" && say "parked $s (-2 procs)"
done

# ---------- 6. one sshd owner ----------
if [ -f "$PREFIX/var/service/sshd/run" ]; then
  mkdir -p "$PREFIX/var/service/sshd.disabled"
  mv "$PREFIX/var/service/sshd/run" "$PREFIX/var/service/sshd.disabled/run" 2>/dev/null \
    && say "parked runsv sshd (keepalive owns sshd via port check)"
fi
pkill -f "sshd.*8022" 2>/dev/null; sleep 1
command -v sshd >/dev/null 2>&1 && sshd && say "sshd started (key-only, port 8022)"

# ---------- 7. start the stack ----------
if ! pgrep -f "eon-stack-keepalive" >/dev/null 2>&1; then
  ( setsid -f bash "$H/eon-stack-keepalive.sh" </dev/null >>"$LOG" 2>&1 & )
  say "keepalive started (singleton-locked)"
fi
for f in coord_poller.py eon-e2e-loop.sh eon-swarm-hb.sh; do
  grep -q "$f" "$H/eon-stack-keepalive.sh" 2>/dev/null || say "note: $f not in resurrect list"
done
sleep 20
say "procs after: $(pgrep -u 10370 2>/dev/null | wc -l) | keepalives: $(pgrep -f 'eon-stack-keepalive' | wc -l)"
say "sshd: $(pgrep -x sshd | wc -l) | opencode: $(pgrep -x opencode | wc -l) (untouched)"
say "DONE — if sshd is up you can reconnect. opencode was never touched."
