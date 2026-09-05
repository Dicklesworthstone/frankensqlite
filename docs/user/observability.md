# Observability: Production Metrics

FrankenSQLite exposes a small, **privacy-safe** set of SLO-relevant metrics so
you can answer *"is the engine healthy?"* under production load without parsing
logs. The surface lives in the `fsqlite-observability` crate
(`metrics` module) and is tracked by bead `bd-zywqc.11`.

> **Privacy guarantee (by construction).** Every metric is a *structural*
> counter, gauge, or latency histogram with a **fixed label set**
> (`response`, `result`, and Prometheus's `le`). No query text, table name,
> column name, or row count is ever recorded or exposed. This is enforced by a
> label-key audit test that fails the build if any non-fixed label key appears.

---

## Enabling and disabling

Metrics recording is **on by default** and effectively free when idle (the
counters are lock-free atomics).

| Control | Effect |
|---------|--------|
| *(default)* | Recording enabled; poll via the callable API below. |
| `FRANKENSQLITE_METRICS_DISABLE=1` | **Hard opt-out.** Every `inc`/`observe`/`set` becomes a branch-guarded no-op and the exposition functions return an empty string. Read once at process start via `LazyLock`, so the disable check adds a single predictable branch. |
| `FRANKENSQLITE_STATSD_ADDR=host:port` | **Opt-in StatsD push.** Names the StatsD/DogStatsD server (e.g. `127.0.0.1:8125`) that `StatsdPusher::from_env()` sends to. Unset ⇒ no StatsD push. |
| `FRANKENSQLITE_METRICS_BIND=host:port` | **Opt-in HTTP `/metrics`.** The first successful native `Connection::open` calls `metrics_net::autostart_from_env()` to bind here (e.g. `127.0.0.1:9009`). Unset ⇒ no HTTP endpoint. Also gated by the hard opt-out above. |

Set the environment variable before the process starts:

```bash
FRANKENSQLITE_METRICS_DISABLE=1 ./your-app
```

---

## Exposition formats

Three ways to read the metrics. The **callable API**, both **serializers**, the
**StatsD UDP push** transport, and the **HTTP `/metrics` endpoint** all ship
today. The SQL engine records `commits_total` at its commit success boundaries,
`page_lock_acquire_duration_seconds` when a contended wait finishes, and
`fsync_duration_seconds` after successful WAL durability calls.
Native database opens start the HTTP endpoint when the bind environment variable
is set. `PRAGMA enable_metrics_http` can also start the shared endpoint. Most
other engine recording sites still need wiring (see [Roadmap](#roadmap)).

The bind setting is checked once per process at the first successful native
open; the hard-disable flag is cached on first metrics use. Subsequent
connections share the endpoint, which lives until process exit.
A bind failure during open leaves database operations usable. Set the bind
address before starting the process; later opens do not retry or move the
listener. SQL control can explicitly retry a failed start.

### 1. Callable API (embedded scrape) — available now

Embedded users poll the process-wide registry directly:

```rust
use fsqlite_observability::metrics;

// Prometheus text exposition (v0.0.4) for the global registry:
let body: String = metrics::render_prometheus();

// Or hold your own registry (e.g. per subsystem) and render it:
let reg = metrics::MetricsRegistry::default();
reg.commits_total.inc();
let body = reg.render_prometheus();
```

### 2. Prometheus (pull) — available now

`render_prometheus()` produces standard Prometheus text exposition. Serve it
from any HTTP handler you already run, or use the built-in `std::net` endpoint
(`metrics_net` module; no tokio/hyper/axum). It serves `GET /metrics` from a
daemon thread with `Content-Type: text/plain; version=0.0.4`, and `404`s any
other path:

```rust
use fsqlite_observability::metrics_net;

// Bind an explicit address (":0" picks an ephemeral port); returns the bound addr.
let addr = metrics_net::start_metrics_http("127.0.0.1:9009")?;
// ... now `curl http://127.0.0.1:9009/metrics` returns the exposition.

// Or opt in by environment — a no-op unless FRANKENSQLITE_METRICS_BIND is set
// (and a no-op under FRANKENSQLITE_METRICS_DISABLE=1). Idempotent; binds once.
metrics_net::autostart_from_env(); // honors FRANKENSQLITE_METRICS_BIND=127.0.0.1:9009
```

Native `Connection::open` calls `autostart_from_env` automatically. SQL can
query or enable the same process-wide listener:

```sql
PRAGMA enable_metrics_http;                    -- 0 before start, 1 after success
PRAGMA enable_metrics_http=1;                  -- env override or 127.0.0.1:9009
PRAGMA enable_metrics_http='127.0.0.1:9009';    -- explicit address
```

An explicit enable reports bind errors to its caller, and a later enable can
retry. Repeated enables reuse the successful listener; supplying another
address does not move it or create a second listener. `=0` is a no-op: this
control has no runtime shutdown, and the listener lives until process exit.
The hard-disable environment flag prevents both startup paths. HTTP control
requires a native target.

Example output:

```text
# HELP fsqlite_commits_total Committed transactions.
# TYPE fsqlite_commits_total counter
fsqlite_commits_total 42
# HELP fsqlite_conflicts_total Write conflicts by resolution.
# TYPE fsqlite_conflicts_total counter
fsqlite_conflicts_total{response="busy_snapshot"} 3
fsqlite_conflicts_total{response="rebased"} 11
# TYPE fsqlite_fsync_duration_seconds histogram
fsqlite_fsync_duration_seconds_bucket{le="0.001"} 500
fsqlite_fsync_duration_seconds_bucket{le="+Inf"} 512
fsqlite_fsync_duration_seconds_sum 0.734
fsqlite_fsync_duration_seconds_count 512
```

### 3. StatsD (push) — available now

StatsD is a *push* protocol: a client periodically emits UDP datagrams that the
server aggregates over each flush interval. Because a StatsD counter sample
(`|c`) is **added** to the server's running total each flush, pushing a
*cumulative* total every interval would double-count. FrankenSQLite's
[`StatsdEncoder`] is therefore **stateful** — it holds the previously-encoded
cumulative snapshot and emits:

- the **delta** since the last encode for counter-typed series, and
- the **absolute** value for gauges.

Uses the DogStatsD tagged dialect (the only widely-supported StatsD variant that
can carry the `response`/`result` labels).

The `StatsdPusher` wraps the encoder over a UDP socket (fire-and-forget, no async
runtime). Point it at your server with `FRANKENSQLITE_STATSD_ADDR` and call
`push_once` on your own interval — datagrams larger than a safe UDP payload
(1432 bytes) are split on line boundaries automatically:

```rust
use fsqlite_observability::metrics::{self, StatsdPusher};

// Opt-in: Ok(None) unless FRANKENSQLITE_STATSD_ADDR is set.
if let Ok(Some(mut pusher)) = StatsdPusher::from_env() {
    // once per flush interval, from your scheduler / a dedicated thread:
    let _ = pusher.push_once(metrics::global());
}
```

To render the datagram without sending it (e.g. a custom transport), use the
encoder directly:

```rust
use fsqlite_observability::metrics::{self, StatsdEncoder};

let mut enc = StatsdEncoder::new();
let datagram: String = enc.encode(metrics::global());
```

Example datagram:

```text
fsqlite.commits_total:5|c
fsqlite.conflicts_total:2|c|#response:busy_snapshot
fsqlite.fsync_duration_seconds.count:12|c
fsqlite.fsync_duration_seconds.sum:0.0183|c
fsqlite.active_writers:4|g
```

Each latency histogram maps to two StatsD counters — `<name>.count`
(observations) and `<name>.sum` (seconds) — so the server derives average
latency over an interval as `rate(sum) / rate(count)`.

---

## Metric catalog

Prometheus names use the `fsqlite_` prefix; the StatsD equivalents use the
dotted `fsqlite.` form shown in parentheses. "Δ (StatsD)" marks series that push
as per-flush deltas.

### Counters (monotonic)

| Metric | Labels | Meaning | SLO recommendation |
|--------|--------|---------|--------------------|
| `fsqlite_commits_total` (`fsqlite.commits_total`) | — | Completed explicit transactions, including read-only COMMIT, plus committed autocommit write transactions. | Base rate for transaction completion; a sudden drop with steady request load signals stalls. |
| `fsqlite_conflicts_total` (`fsqlite.conflicts_total`) | `response=busy_snapshot` \| `rebased` | Rejected MVCC commit attempts, or successfully committed rebased transactions. | Compare rejected attempts with committed transactions to measure retry pressure. The `rebased` series reports successful merges in the MVCC transaction-manager API. |
| `fsqlite_sweeper_clears_total` (`fsqlite.sweeper_clears_total`) | — | Completed version-store GC passes, including empty passes and passes blocked from reclaiming pinned versions. | Use to check GC activity; an increase does not prove that versions or bytes were reclaimed. |
| `fsqlite_historical_snapshots_opened_total` (`fsqlite.historical_snapshots_opened_total`) | — | Time-travel / historical snapshots opened. | Informational; correlate with `historical_pins_active`. |
| `fsqlite_schema_epoch_bumps_total` (`fsqlite.schema_epoch_bumps_total`) | — | Local main or TEMP schema-epoch increments that invalidate cached plans, including DDL later rolled back. | Track local schema churn; this is not a count of committed DDL statements. |
| `fsqlite_integrity_check_runs_total` (`fsqlite.integrity_check_runs_total`) | `result=ok` \| `fail` | Completed public `integrity_check` and `quick_check` calls by validation outcome. | Investigate any failure: its diagnostic distinguishes corruption from an operational error. |

The commit counter increments once after successful commit resolution, including
busy retries. It excludes rollbacks, rejected commits, failed autocommit writes,
implicit reads, and uncommitted retained batches. A retained batch counts once
when flushed, regardless of its statement count. Post-commit cleanup errors do
not undo an already counted commit. These are SQL engine commit counts; they do
not count individual rows or internal pager maintenance transactions.

The conflict counter records each commit attempt rejected with `BusySnapshot`,
including SSI, stale-schema and first-committer-wins validation failures.
Autocommit retries start new transactions: a rejected attempt still counts even
if a later attempt succeeds. SQL validation and the separate MVCC
`TransactionManager` API each record their own terminal outcome; neither
requires hot-path profiling. The MVCC API counts a successful rebase once per
committed transaction, regardless of page count. Partial rebases followed by an
abort, invalid-state calls, and ordinary successful commits do not increment
that series. This does not establish a public SQL rebase path.

The sweeper counter increments after `VersionStore::gc_tick` completes its
pruning and retirement work. It counts passes, not freed versions or bytes:
reader guards can keep retired versions pinned across multiple passes. Calls
that return before invoking the version-store collector do not increment it.

The schema counter follows local DDL epochs, including explicit schema-cookie
updates. Loading a peer's schema does not count as a new local invalidation.
The integrity counter records the validator's result before formatting its
diagnostic. Informational notes appended after an `ok` verdict do not turn that
check into a failure. An open failure or a request that never reaches the
validator is not a completed check.

### Histograms (latency, seconds)

Bucket bounds (`le`, seconds): `0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1,
0.5, 1.0, 5.0`, plus `+Inf`. Each exposes `_bucket`, `_sum`, and `_count`.

| Metric | Meaning | SLO recommendation |
|--------|---------|--------------------|
| `fsqlite_fsync_duration_seconds` (`fsqlite.fsync_duration_seconds`) | Elapsed time for successful WAL VFS sync calls, including header creation/reset and frame durability. Failed or cancelled calls are excluded; an in-memory VFS has no physical disk flush. | Track the successful durability latency distribution; failures require separate error diagnostics. |
| `fsqlite_commit_duration_seconds` (`fsqlite.commit_duration_seconds`) | Successful SQL commit-resolution latency, including validation and durability waits. One sample per counted commit, including explicit read-only commits. | Compare its distribution with your transaction budget and WAL sync latency. These measure different populations, so subtracting their percentiles does not isolate a component's cost. |
| `fsqlite_page_lock_acquire_duration_seconds` (`fsqlite.page_lock_acquire_duration_seconds`) | Elapsed time per contended VDBE page-lock wait attempt, including holder change, timeout, cancellation, and deadlock-victim exits. | Compare tails with the workload's busy-timeout budget to locate page contention. |

The page-lock histogram excludes uncontended acquisitions, zero-budget probes,
and cancellation detected before entering the wait. It measures each completed
wait attempt, not a whole statement or a successful lock acquisition. Recording
uses the wait loop's existing start time. When metrics are disabled, the added
recording path skips the elapsed-time read and registry access; the wait loop
still reads the clock to enforce its timeout. The separate VDBE counters do not
control this histogram.

Commit timing starts when an explicit COMMIT, retained-batch flush, or writable
autocommit resolution enters its commit work. It ends at the successful outcome
counted by `commits_total`, before later checkpointing and connection cleanup.
Time spent executing earlier SQL or accumulating a retained batch is excluded.
Internal durability retries are included in the successful attempt; rejected
transactions and rollbacks have no latency sample. When metrics are disabled,
this timing path skips clock reads and registry access.

### Gauges (point-in-time)

| Metric | Meaning | SLO recommendation |
|--------|---------|--------------------|
| `fsqlite_active_writers` (`fsqlite.active_writers`) | Concurrent transactions registered in this process, summed across databases; includes a concurrent `BEGIN` before its first write. | Capacity signal for concurrent writer admission; not a count of connections or serialized-mode transactions. |
| `fsqlite_active_readers` (`fsqlite.active_readers`) | Active reader snapshots. | Capacity signal. |
| `fsqlite_historical_pins_active` (`fsqlite.historical_pins_active`) | Historical snapshots currently pinned (holding old versions alive). | A steadily rising value pins version history and blocks the sweeper — alert if it grows unbounded. |
| `fsqlite_wal_frames_pending_checkpoint` (`fsqlite.wal_frames_pending_checkpoint`) | WAL frames not yet checkpointed. | Alert when it grows without bound — checkpointing is falling behind writes. |
| `fsqlite_history_records_count` (`fsqlite.history_records_count`) | Records retained in the history sidecar. | Watch alongside `historical_pins_active`; unbounded growth means retained history is not being reclaimed. |
| `fsqlite_history_bytes` (`fsqlite.history_bytes`) | Bytes retained in the history sidecar. | Disk-budget signal for time-travel retention. |

The writer gauge follows successful concurrent-registry admission and removal.
Rejected admission and duplicate removal leave it unchanged. Recycling a
removed handle does not keep it active, and dropping a registry releases its
remaining contribution without resetting other databases' counts.

---

## Recommended alerts (starting point)

```text
# Writers repeatedly turned away (retry pressure)
rate(fsqlite_conflicts_total{response="busy_snapshot"}[5m])
  / rate(fsqlite_commits_total[5m]) > 0.05

# Any integrity failure is a page.
increase(fsqlite_integrity_check_runs_total{result="fail"}[15m]) > 0

# Checkpointing falling behind.
fsqlite_wal_frames_pending_checkpoint > <your_wal_frame_budget>

# Commit latency SLO breach.
histogram_quantile(0.99, rate(fsqlite_commit_duration_seconds_bucket[5m]))
  > <your_write_latency_budget_seconds>
```

Tune every threshold to your workload — the values above are illustrative
defaults, not measured guarantees.

---

## Roadmap

The recording registry, both serializers, the StatsD UDP push transport, and the
HTTP `/metrics` endpoint all ship today. Remaining increments (tracked on
`bd-zywqc.11`):

- **Engine hot-path wiring** — remaining history/snapshot and active-state gauge
  producers, plus accurate WAL checkpoint backlog accounting. Commit counts and successful commit latency are wired;
  its public SQL keeper is
  `crates/fsqlite-core/tests/agent_swarm_explain_concurrency_contract.rs::real_commit_metrics_count_public_durable_outcomes`
  (bd-zywqc.11.1.3). The page-lock wait histogram is wired at
  `fsqlite-vdbe::engine::wait_for_page_lock_holder_change`; its isolated
  `page_lock_wait_histogram_tracks_real_outcomes` keeper covers enabled and
  disabled recording (bd-zywqc.11.1.1). WAL fsync timing covers `WalFile` creation,
  reset, `sync`, and `durable_sync`; the real-file
  `real_wal_barriers_record_successful_fsync_latency` keeper checks successful
  counts, cancellation, reopen, and disabled mode. Clock reads are skipped when
  metrics are disabled. Conflict outcomes are checked by the public SQL keeper
  and `fsqlite-mvcc::lifecycle::tests::real_conflict_metrics_count_terminal_transaction_outcomes`,
  including a multi-page merge followed by an abort. Completed GC passes are
  recorded by `VersionStore::gc_tick`; the real
  `real_sweeper_metrics_follow_completed_gc_passes` keeper checks pinned-version
  protection, recycling after guard release, empty passes, and disabled mode.
  Schema epochs are checked by the public
  `real_schema_metrics_follow_public_cache_invalidations` keeper, including
  prepared-statement refresh, TEMP changes, rollback, and peer reload.
  `adversarial_bit_flip::real_integrity_metrics_count_clean_and_corrupt_public_checks`
  checks both public validation PRAGMAs against clean and structurally corrupted
  files also checked by stock SQLite. Both keepers cover disabled recording.
  The concurrent-writer gauge is checked by
  `real_active_writer_gauge_tracks_registry_lifetimes` and by live HTTP scrapes
  around a public concurrent transaction.
  Other series being present in an exposition is not proof that their engine
  producers run.
- **Overhead gate** — verify opt-in metrics add < 2% on the 8-writer soak.

[`StatsdEncoder`]: ../../crates/fsqlite-observability/src/metrics.rs
