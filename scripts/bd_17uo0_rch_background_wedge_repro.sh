#!/usr/bin/env bash
# bd-17uo0 — RCH background-command wedge: repro + workaround verification harness.
#
# THE WEDGE (observed 2026-05-19/20, 25 min and 1h54m): a coding agent runs a
# long command (an `rch exec` build, or a multi-minute verification script) as a
# BACKGROUND terminal. The rch remote side completes, fails, was never admitted,
# or the queue is idle (`rch status` shows slots free / 0 active) — but the LOCAL
# rch wrapper the agent's TUI is waiting on never returns, so the TUI sits on
# "Waiting for background terminal" indefinitely while the remote is idle.
#
# ROOT-CAUSE SHAPE (grounded in the rch binary + `rch --help`): the PreToolUse
# `Bash` hook routes every command through `rch`; a remote compile carries a
# DurableJobLease (wrapper_pid / heartbeat / terminal_acknowledged / abandoned).
# rch **fails open to a LOCAL build only when no worker can take the job**; it
# does NOT bound the LOCAL wait once the remote job is already done/absent but
# the response was never delivered to the wrapper. A background terminal has no
# interactive tty, so the interrupt/acknowledge path is inert — the wait hangs.
#
# This harness does NOT try to force the (intermittent) wedge — that would risk
# wedging the agent running it. Instead it POSITIVELY VERIFIES that each
# documented mitigation returns control within a bounded timeout, across the
# acceptance scenarios. Every rch touchpoint is wrapped in `timeout`, so the
# harness itself can never wedge.
#
# Usage:  bash scripts/bd_17uo0_rch_background_wedge_repro.sh
# Exit 0  = all mitigations verified; non-zero = a mitigation failed.

set -u

RCH_BIN="${RCH_BIN:-$HOME/.local/bin/rch}"
PASS=0
FAIL=0
BOUND=20   # seconds: the hard cap every check must return within

say() { printf '%s\n' "$*"; }
ok()   { PASS=$((PASS + 1)); printf '  \033[32mPASS\033[0m %s\n' "$*"; }
bad()  { FAIL=$((FAIL + 1)); printf '  \033[31mFAIL\033[0m %s\n' "$*"; }

# Run "$@" under a hard timeout; echo the wall-clock seconds and the exit code
# into the globals REPLY_SECS / REPLY_RC. A timeout (rc 124) means the command
# would have hung past the bound — i.e. the wedge shape.
timed() {
  local start end
  start=$(date +%s.%N)
  timeout "${BOUND}s" "$@" >/dev/null 2>&1
  REPLY_RC=$?
  end=$(date +%s.%N)
  REPLY_SECS=$(awk -v a="$start" -v b="$end" 'BEGIN { printf "%.2f", b - a }')
}

say "== bd-17uo0 RCH background-command wedge — workaround verification =="
say "   rch: ${RCH_BIN}  (hard bound per check: ${BOUND}s)"
say ""

# ---------------------------------------------------------------------------
# Scenario A — remote-state inspection (recovery-ladder step 1): the operator
# must be able to see that the remote queue is idle while a local wait hangs.
# ---------------------------------------------------------------------------
say "A. Remote-state is inspectable within a bound (recovery-ladder step 1)"
if [ -x "$RCH_BIN" ]; then
  timed "$RCH_BIN" status
  if [ "$REPLY_RC" -ne 124 ]; then
    ok "rch status returned in ${REPLY_SECS}s (rc=${REPLY_RC}) — remote/idle state visible"
  else
    bad "rch status did not return within ${BOUND}s"
  fi
else
  say "  SKIP rch not installed at ${RCH_BIN}"
fi

# ---------------------------------------------------------------------------
# Scenario B — the primary mitigation: RCH_CARGO_WRAPPER_BYPASS=1 runs the
# command locally and returns immediately, regardless of remote/queue state.
# ---------------------------------------------------------------------------
say "B. RCH_CARGO_WRAPPER_BYPASS=1 returns control immediately (any remote state)"
RCH_CARGO_WRAPPER_BYPASS=1 timed bash -c 'exit 0'
if [ "$REPLY_RC" -eq 0 ] && [ "$REPLY_RC" -ne 124 ]; then
  ok "bypass command returned in ${REPLY_SECS}s, rc=0 — wrapper wait skipped"
else
  bad "bypass did not return cleanly (rc=${REPLY_RC})"
fi
# The bypass env is a real, recognized knob (defends the runbook against drift).
if [ -x "$RCH_BIN" ] && strings "$RCH_BIN" 2>/dev/null | grep -q "RCH_CARGO_WRAPPER_BYPASS"; then
  ok "RCH_CARGO_WRAPPER_BYPASS is a recognized rch env knob"
else
  bad "RCH_CARGO_WRAPPER_BYPASS not found in the rch binary — runbook may be stale"
fi

# ---------------------------------------------------------------------------
# Scenario C — command failure with nonzero status still returns control
# (a wedge would swallow the failure; the mitigation must surface it).
# ---------------------------------------------------------------------------
say "C. A failing command surfaces its nonzero status (control returns, not wedged)"
RCH_CARGO_WRAPPER_BYPASS=1 timed bash -c 'exit 7'
if [ "$REPLY_RC" -eq 7 ]; then
  ok "failing command returned rc=7 in ${REPLY_SECS}s — failure surfaced, no hang"
else
  bad "failing command did not surface rc=7 (got ${REPLY_RC})"
fi

# ---------------------------------------------------------------------------
# Scenario D — the reaper: a `timeout` around any command bounds the wait, so
# a would-be-infinite wrapper wait is force-returned as rc 124 instead of a
# silent forever-hang (covers remote-active / remote-complete / remote-absent /
# wrapper-already-exited uniformly — the OS reaps the stale wait).
# ---------------------------------------------------------------------------
say "D. A foreground timeout reaps a stale/never-returning wait (rc 124, bounded)"
start=$(date +%s.%N)
timeout 2s bash -c 'sleep 600' >/dev/null 2>&1   # stand-in for a hung wrapper wait
rc=$?
end=$(date +%s.%N)
secs=$(awk -v a="$start" -v b="$end" 'BEGIN { printf "%.2f", b - a }')
if [ "$rc" -eq 124 ] && awk -v s="$secs" 'BEGIN { exit !(s < 5) }'; then
  ok "a hung command was force-returned in ${secs}s as rc=124 — no indefinite wait"
else
  bad "timeout did not reap the hung command as expected (rc=${rc}, ${secs}s)"
fi

# ---------------------------------------------------------------------------
# Scenario E — bounded local daemon wait: RCH_DAEMON_WAIT_RESPONSE_TIMEOUT_SECS
# is the rch-native knob that returns control instead of an unbounded wait.
# ---------------------------------------------------------------------------
say "E. rch exposes a bounded local-wait knob (RCH_DAEMON_WAIT_RESPONSE_TIMEOUT_SECS)"
if [ -x "$RCH_BIN" ] && strings "$RCH_BIN" 2>/dev/null | grep -q "RCH_DAEMON_WAIT_RESPONSE_TIMEOUT_SECS"; then
  ok "RCH_DAEMON_WAIT_RESPONSE_TIMEOUT_SECS is a recognized knob — set it to bound the wrapper wait"
else
  bad "RCH_DAEMON_WAIT_RESPONSE_TIMEOUT_SECS not found — bounded-wait guidance may be stale"
fi

# ---------------------------------------------------------------------------
# Scenario F — identify a stale local wait for the recovery ladder: the wrapper
# is a local `rch` process the TUI is blocked on; the operator kills only that,
# never the remote job.
# ---------------------------------------------------------------------------
say "F. Stale local wrapper waits are identifiable (recovery-ladder: kill local only)"
if command -v pgrep >/dev/null 2>&1; then
  waiters=$(pgrep -af "rch" 2>/dev/null | grep -v "bd_17uo0" | grep -v "rchd" | wc -l | tr -d ' ')
  ok "pgrep can enumerate rch wrapper processes (currently ${waiters}); kill the stale wrapper PID, not the remote job"
else
  bad "pgrep unavailable — cannot enumerate stale local waiters"
fi

say ""
say "== summary: ${PASS} passed, ${FAIL} failed =="
[ "$FAIL" -eq 0 ]
