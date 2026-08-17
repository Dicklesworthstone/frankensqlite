# RCH background-command wedge — diagnosis, workaround, and recovery runbook

**Bead:** bd-17uo0 · **Class:** agent/tool operability · **Related:** bd-yxsqo (UBS full-file scan hang)

## Symptom

A coding agent (observed with Codex/gpt-5.5) runs a long command — an `rch exec`
of a perf build, or a multi-minute verification script — as a **background
terminal**. The remote side completes, fails, was never admitted, or the queue
is idle (`rch status` shows slots free / `0 active`), yet the agent's TUI sits on
**"Waiting for background terminal (Xh Ym • esc to interrupt)"** indefinitely.
Observed durations: **25 min** (cod_3) and **1h 54m** (cod_4) during the
2026-05-19/20 swarm session, with zero progress.

## Root-cause shape

Grounded in the `rch` binary (v1.0.57) and `rch --help`:

- The PreToolUse `Bash` hook routes **every** shell command through `rch`
  (`~/.claude/settings.json`). For a compile it may offload to a remote worker.
- A remote job carries a **DurableJobLease** (`wrapper_pid`, `heartbeat_unix_ms`,
  `terminal_acknowledged`, `abandoned`). The **local wrapper** — the process the
  agent's TUI is actually blocked on — waits for the remote response.
- rch's fail-open covers **admission**: "always attempt offload but fail open to
  a LOCAL build if no worker can take it." It does **not** bound the **local
  wait** once a job is already *done/absent* but its response was never delivered
  to the wrapper.
- A **background terminal has no interactive tty**, so the interrupt / acknowledge
  path (`terminal_acknowledged`) is inert. The wrapper wait then has nothing to
  wake or cancel it — it hangs while the remote queue is idle.

Net: this is a **stale-local-wrapper / fail-open gap**, aggravated by the
no-tty background context. It is *not* the remote job hanging (the remote is
idle); it is the local wait never returning.

## Workaround (proven; keep foreground/synchronous rch as the default)

Use any of these — B is the one used continuously in day-to-day work:

- **A. Never background a long rch/script command.** Run it in the **foreground**
  with an explicit reaper: `timeout <N>s <command>`. The OS force-returns a stale
  wait as exit `124` instead of a silent forever-wait.
- **B. Bypass the wrapper** for long or background compiles:
  `RCH_CARGO_WRAPPER_BYPASS=1 <command>` runs locally and returns immediately —
  no wrapper wait to strand (verified below at 0.11 s). Pair with `timeout`.
- **C. Bound the local wait** with the rch-native knob:
  `RCH_DAEMON_WAIT_RESPONSE_TIMEOUT_SECS=<N> <command>` returns control instead of
  waiting unbounded. (`RCH_DAEMON_TIMEOUT_MS`, default 5000, bounds daemon comms
  but not the job-response wait.)

Foreground/synchronous rch remains the recommended path until a
background-terminal watchdog is proven in the client.

## Recovery ladder — for a live wedge (Beads / Agent Mail handoff)

1. **Identify the session/pane** (agent name + terminal). Note the elapsed
   timer is *not* liveness — the "Working Xm" counter fooled the smart-restart
   activity check on cod_3.
2. **Confirm the remote is idle:** `rch status` and `rch queue`. Slots free /
   `0 active` builds ⇒ the remote is done; the wait is a *stale local wrapper*.
3. **Return control, least-invasive first:**
   - Responsive TUI: `Esc` → surface `/stop` → stop the background terminal →
     rerun in the **foreground** with `RCH_CARGO_WRAPPER_BYPASS=1` + `timeout`.
   - Unresponsive TUI: identify the stale local wrapper (`pgrep -af rch`, exclude
     `rchd` and this repro script) and kill **only that local wait** — never the
     remote job (avoids orphaned remote builds). Then relaunch and re-send the
     directive as a **single** message (a multi-line prompt into bare `zsh`
     leaves `quote>` artifacts).
4. **Do not** SIGKILL the remote worker job; let it complete or use
   `rch` cancellation so slots are released cleanly.

## Acceptance-scenario coverage

| Scenario | Mitigation that returns control | Verified by |
|---|---|---|
| Remote job still active | `timeout` reaper (foreground); or let it finish foreground | harness D |
| Remote job complete | wrapper wait bounded by `RCH_DAEMON_WAIT_RESPONSE_TIMEOUT_SECS`; `timeout` reaper | harness D, E |
| Remote job absent / never admitted | fail-open-to-local + `timeout`; or bypass | harness B, D |
| Local wrapper already exited | nothing to wait on; `timeout` reaper is a no-op safety net | harness D |
| Command failed (nonzero) | failure surfaces as its own rc, no hang | harness C |

## Evidence

Run `bash scripts/bd_17uo0_rch_background_wedge_repro.sh` — a safe harness (every
rch touchpoint wrapped in `timeout`, so it can never wedge the caller). It
positively verifies each mitigation returns control within a hard 20 s bound
(forcing the intermittent wedge itself would risk stranding the runner).

Observed 2026-08-17 (rch 1.0.57): **7 passed, 0 failed** —
`rch status` 0.11 s; `RCH_CARGO_WRAPPER_BYPASS=1` command 0.11 s / rc 0;
failing command surfaced rc 7; a hung command force-returned in 2.01 s as rc 124;
`RCH_CARGO_WRAPPER_BYPASS` and `RCH_DAEMON_WAIT_RESPONSE_TIMEOUT_SECS` confirmed as
recognized knobs in the binary; `pgrep` enumerated the local rch wrappers.

## Upstream fix (out of scope here — client/agent surfaces)

The durable fix lives in the `rch` client and/or the Codex background-terminal
watchdog (both external to this repo): bound the local job-response wait, detect
a done/absent remote job and return control, and surface a "still running Xm —
kill?" prompt for a background terminal rather than a silent indefinite wait.
This runbook + harness is the operability mitigation until that lands.
