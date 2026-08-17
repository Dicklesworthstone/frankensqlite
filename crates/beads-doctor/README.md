# beads-doctor

A single cross-platform Rust binary that periodically (every 30 minutes, via an
OS scheduler) runs `br doctor --quick --json` against each configured beads
workspace, parses the structured anomalies, **notifies only on error-severity
findings** (WARN/`degraded` are logged, not notified), and keeps a rolling
per-workspace JSONL log.

It is asupersync-native: the binary is the top-level runtime owner (it builds
the `asupersync` runtime in `main` and threads the resulting `&Cx` through every
async fn), per-workspace probes run under one structured
`asupersync::combinator::JoinSet` region (no orphan tasks), and timestamps come
from `asupersync::time::wall_now()`. No tokio/reqwest — HTTP is a `curl`
subprocess, the doctor is a `br` subprocess. `#![forbid(unsafe_code)]`.

## Configuration

TOML at `~/.config/beads-doctor/workspaces.toml` (respects `XDG_CONFIG_HOME`):

```toml
[[workspace]]
name = "fsqlite"
path = "/data/projects/frankensqlite"
ntfy_topic = "beads-fsqlite"
# optional; defaults shown:
ntfy_server = "https://ntfy.sh"
severity_threshold = "error"
```

Config-driven: adding a new `[[workspace]]` block is picked up on the next `run`
with no code change.

## Commands

```
beads-doctor run [--once] [--workspace <name>] [--config <path>]
beads-doctor install [--workspace <name>] [--config <path>] [--exec <path>]
beads-doctor uninstall [--workspace <name>] [--config <path>]
beads-doctor --help | --version
```

`run` probes every configured workspace once — this is what the timer invokes.
`--once` is an accepted alias (one cycle is always the unit of work; the OS
scheduler owns the 30-minute cadence).

## Severity policy

A finding notifies when its severity rank meets or exceeds the workspace
threshold (`degraded`(1) < `warn`(2) < `error`(3) < `critical`(4)), **or** its
code is on the always-notify list (`database_corrupt`, `sqlite_integrity_failed`)
regardless of severity. With the default `"error"` threshold, `degraded`/`warn`
findings (e.g. an orphaned `write_lock`) are logged only. Every finding is always
written to the rolling log; only notify-worthy ones are posted to ntfy.

## Notification payload (stable schema — `schema_version = 1`)

Downstream PagerDuty/Slack bridges pin to this shape:

```json
{
  "schema_version": 1,
  "workspace": "fsqlite",
  "code": "database_corrupt",
  "severity": "error",
  "message": "beads.db failed integrity check",
  "remediation": "Restore `.beads/beads.db` from the JSONL store and rebuild (`br doctor --repair`).",
  "timestamp": 1128580
}
```

`timestamp` is `asupersync::time::wall_now()` nanoseconds (process-monotonic).
`remediation` is always present (empty string when none is known; a canned hint
for the always-notify corruption codes).

## Rolling logs and rotation

One JSON line per finding and per probe tick to
`~/.local/state/beads-doctor/<workspace>.jsonl` (respects `XDG_STATE_HOME`). The
active file rotates to `<file>.<nanos>` when it exceeds **50 MiB** or its mtime is
older than **30 days** (size wins when both trip). The rotation *decision* is a
pure function (`log::rotation_reason`) that touches no clock, and the clock is
injected behind the `log::Clock` trait, so rotation is unit-tested with synthetic
times in a tempdir.

## Installers

Each generator is a pure function returning file content (unit-tested on any
host). `install` writes them for the current OS; **activating** the schedule is a
platform step the tool prints but does not perform:

- **Linux (systemd user):** `~/.config/systemd/user/beads-doctor@.{service,timer}`
  template units (`%i` = workspace), 30-minute `OnUnitActiveSec`/`OnCalendar`.
  Activate: `systemctl --user enable --now beads-doctor@<name>.timer`.
- **macOS (launchd):** `~/Library/LaunchAgents/com.beads.doctor.<name>.plist`,
  `StartInterval` 1800s plus a 30-minute `StartCalendarInterval`.
  Activate: `launchctl load ~/Library/LaunchAgents/com.beads.doctor.<name>.plist`.
- **Windows (Task Scheduler):** a task XML with a `PT30M` repetition.
  Activate: `schtasks /create /tn beads-doctor-<name> /xml <file>.xml`.

## Verification split (honest)

### Sandbox-verified here (unit/integration tests, all green)

- Anomaly parse from a `br doctor --json` fixture → `[{code,severity,message}]`.
- Severity filter: `database_corrupt`/`error` → stub Notifier records a
  notification; `warn`/`degraded` (e.g. orphaned write_lock) → logged, Notifier
  not called.
- Notification payload schema stability (exact key set + values).
- Config parse and config-driven pickup (adding a `[[workspace]]` and re-running
  `run` probes the new one).
- Log rotation: >50 MiB size rotation in a tempdir, and age rotation via an
  injected clock.
- Installer filegen: systemd/launchd/Task-Scheduler content carries the
  workspace name (or its enabled instance unit) and the 30-minute interval.
- End-to-end `run` against the real `br` on the fsqlite workspace: the live
  `degraded` finding was logged with `notified:false` and produced no
  notification (exercises the real subprocess runner).

### Platform-verification-required (NOT run here — one command away)

These are system-level and cannot be proven in this sandbox. The generators and
`install` command make each a single step, but they were **not** executed:

1. **Linux:** `systemctl --user enable --now beads-doctor@<name>.timer` then
   `journalctl --user -u 'beads-doctor@<name>.service'` showing ≥3 ticks.
2. **macOS** (the `ssh mmini` box): `launchctl list | grep beads-doctor` after
   `launchctl load`.
3. **Windows CI:** `schtasks /create ... /xml` registration + a triggered run.

## Design / dependency notes

- **asupersync structured concurrency.** Per-workspace probes are spawned into a
  `JoinSet::in_cx(cx)` region and collected with `join_all` — the blessed
  no-orphan primitive. The root task is spawned via `runtime.handle().spawn(...)`
  (not a bare `block_on`) so its `Cx` carries a spawn gateway. `DoctorRunner` and
  `Notifier` are `&Cx`-first synchronous traits (blocking subprocess calls are
  fine at a 30-minute cadence); tests inject in-memory stubs so no real `br` or
  network is required.
- **The one `SystemTime::now`.** `wall_now()` is a process-monotonic clock, not a
  calendar clock, so it cannot tell whether a file on disk is "older than 30
  days" across process restarts. Age-based log rotation needs a calendar instant,
  which asupersync does not vend; `SystemClock::wall_clock()` supplies it via
  `SystemTime::now`, quarantined to that single filesystem-age comparison and
  injected behind the `Clock` trait. Every payload/log `timestamp` still uses
  `wall_now()`.
