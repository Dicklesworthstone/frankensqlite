//! Production telemetry registry (bd-zywqc.11): a dependency-light, pollable
//! Prometheus metrics surface for SLO-relevant structural counters.
//!
//! Deliberate deviation from the bead's "new crate `fsqlite-metrics`" wording:
//! this crate (`fsqlite-observability`) is already the metrics/tracing home
//! (`TraceMetricsSnapshot`, `IoUringLatencyMetrics`), so per AGENTS.md
//! ("revise existing code in place; the bar for a new crate is incredibly
//! high") the registry lives here as a module rather than a redundant crate.
//!
//! **Privacy (AC#3), by construction:** every metric is a structural counter,
//! gauge, or latency histogram with a *fixed* label set (`response`, `result`).
//! No query text, table name, or row count is ever recorded or exposed.
//!
//! **Opt-out (AC#4):** `FRANKENSQLITE_METRICS_DISABLE=1` makes every `inc`/
//! `observe`/`set` a cheap branch-guarded no-op (read once via `LazyLock`).
//!
//! This module is the self-contained foundation (registry + exposition + tests).
//! Exposition today: Prometheus pull ([`MetricsRegistry::render_prometheus`])
//! and StatsD push datagrams ([`StatsdEncoder`], AC#6). Follow-on increments
//! (tracked on the bead): the network transport — HTTP `/metrics` server and
//! the StatsD UDP push loop (both `std::net`/asupersync, since tokio is
//! forbidden) — the engine hot-path wiring that increments these metrics, and
//! the perf/soak gate.

use std::sync::LazyLock;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

/// Prometheus `le` bucket bounds (seconds) for the latency histograms — from
/// 100µs to 5s, covering fsync / commit / page-lock acquisition latencies.
const DURATION_BUCKETS_SECONDS: [f64; 10] =
    [0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0];

/// Whether the entire metrics subsystem is disabled (`FRANKENSQLITE_METRICS_DISABLE=1`).
/// Read exactly once; when true, every recording call is a branch-guarded no-op.
static METRICS_DISABLED: LazyLock<bool> = LazyLock::new(|| {
    std::env::var("FRANKENSQLITE_METRICS_DISABLE")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
});

/// Returns `true` when the metrics subsystem is disabled by the environment.
#[must_use]
pub fn metrics_disabled() -> bool {
    *METRICS_DISABLED
}

/// A monotonically increasing counter.
#[derive(Debug, Default)]
pub struct Counter(AtomicU64);

impl Counter {
    /// Increment by one (no-op when metrics are disabled).
    pub fn inc(&self) {
        self.inc_by(1);
    }

    /// Increment by `n` (no-op when metrics are disabled).
    pub fn inc_by(&self, n: u64) {
        if metrics_disabled() {
            return;
        }
        self.0.fetch_add(n, Ordering::Relaxed);
    }

    /// Current value.
    #[must_use]
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// A gauge that can go up or down (integer-valued: counts, bytes, frames).
#[derive(Debug, Default)]
pub struct Gauge(AtomicI64);

impl Gauge {
    /// Set the gauge to `v` (no-op when metrics are disabled).
    pub fn set(&self, v: i64) {
        if metrics_disabled() {
            return;
        }
        self.0.store(v, Ordering::Relaxed);
    }

    /// Add `delta` (may be negative). No-op when metrics are disabled.
    pub fn add(&self, delta: i64) {
        if metrics_disabled() {
            return;
        }
        self.0.fetch_add(delta, Ordering::Relaxed);
    }

    /// Increment by one.
    pub fn inc(&self) {
        self.add(1);
    }

    /// Decrement by one.
    pub fn dec(&self) {
        self.add(-1);
    }

    /// Current value.
    #[must_use]
    pub fn get(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// A cumulative latency histogram with fixed [`DURATION_BUCKETS_SECONDS`] buckets
/// plus `_sum` and `_count`, rendered in Prometheus exposition form.
#[derive(Debug)]
pub struct Histogram {
    buckets: [AtomicU64; DURATION_BUCKETS_SECONDS.len()],
    /// Cumulative sum of observed values, stored as `f64::to_bits` for lock-free add.
    sum_bits: AtomicU64,
    count: AtomicU64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self {
            buckets: Default::default(),
            sum_bits: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }
}

impl Histogram {
    /// Record an observation in seconds (no-op when metrics are disabled).
    pub fn observe(&self, seconds: f64) {
        if metrics_disabled() || !seconds.is_finite() || seconds < 0.0 {
            return;
        }
        for (i, bound) in DURATION_BUCKETS_SECONDS.iter().enumerate() {
            if seconds <= *bound {
                self.buckets[i].fetch_add(1, Ordering::Relaxed);
            }
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        // Lock-free f64 sum via CAS on the bit pattern.
        let mut cur = self.sum_bits.load(Ordering::Relaxed);
        loop {
            let next = f64::from_bits(cur) + seconds;
            match self.sum_bits.compare_exchange_weak(
                cur,
                next.to_bits(),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => cur = observed,
            }
        }
    }

    /// Total observation count.
    #[must_use]
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Cumulative sum of observations.
    #[must_use]
    pub fn sum(&self) -> f64 {
        f64::from_bits(self.sum_bits.load(Ordering::Relaxed))
    }

    /// Cumulative bucket counts (`le` semantics: count of observations ≤ bound).
    #[must_use]
    fn bucket_counts(&self) -> [u64; DURATION_BUCKETS_SECONDS.len()] {
        let mut out = [0u64; DURATION_BUCKETS_SECONDS.len()];
        for (i, b) in self.buckets.iter().enumerate() {
            out[i] = b.load(Ordering::Relaxed);
        }
        out
    }
}

/// The full SLO-relevant metrics registry. Fixed label sets only.
#[derive(Debug, Default)]
pub struct MetricsRegistry {
    // ── counters ────────────────────────────────────────────────
    pub commits_total: Counter,
    pub sweeper_clears_total: Counter,
    pub historical_snapshots_opened_total: Counter,
    pub schema_epoch_bumps_total: Counter,
    // conflicts_total{response=...}
    pub conflicts_busy_snapshot_total: Counter,
    pub conflicts_rebased_total: Counter,
    // integrity_check_runs_total{result=...}
    pub integrity_check_ok_total: Counter,
    pub integrity_check_fail_total: Counter,
    // ── histograms ──────────────────────────────────────────────
    pub fsync_duration_seconds: Histogram,
    pub commit_duration_seconds: Histogram,
    pub page_lock_acquire_duration_seconds: Histogram,
    // ── gauges ──────────────────────────────────────────────────
    pub active_writers: Gauge,
    pub active_readers: Gauge,
    pub historical_pins_active: Gauge,
    pub wal_frames_pending_checkpoint: Gauge,
    pub history_records_count: Gauge,
    pub history_bytes: Gauge,
}

/// Global registry singleton. Hot paths call `global().commits_total.inc()` etc.
static GLOBAL: LazyLock<MetricsRegistry> = LazyLock::new(MetricsRegistry::default);

/// The process-wide metrics registry.
#[must_use]
pub fn global() -> &'static MetricsRegistry {
    &GLOBAL
}

impl MetricsRegistry {
    /// Render the full registry in Prometheus text exposition format (v0.0.4).
    /// Returns an empty string when metrics are disabled.
    #[must_use]
    pub fn render_prometheus(&self) -> String {
        if metrics_disabled() {
            return String::new();
        }
        let mut o = String::with_capacity(4096);
        let counter = |o: &mut String, name: &str, help: &str, labels: &str, v: u64| {
            o.push_str(&format!("# HELP {name} {help}\n# TYPE {name} counter\n"));
            o.push_str(&format!("{name}{labels} {v}\n"));
        };
        // unlabeled counters
        counter(
            &mut o,
            "fsqlite_commits_total",
            "Committed transactions.",
            "",
            self.commits_total.get(),
        );
        counter(
            &mut o,
            "fsqlite_sweeper_clears_total",
            "Sweeper clear events.",
            "",
            self.sweeper_clears_total.get(),
        );
        counter(
            &mut o,
            "fsqlite_historical_snapshots_opened_total",
            "Historical snapshots opened.",
            "",
            self.historical_snapshots_opened_total.get(),
        );
        counter(
            &mut o,
            "fsqlite_schema_epoch_bumps_total",
            "Schema-epoch bumps.",
            "",
            self.schema_epoch_bumps_total.get(),
        );
        // labeled counters (single HELP/TYPE, multiple label series)
        o.push_str("# HELP fsqlite_conflicts_total Write conflicts by resolution.\n# TYPE fsqlite_conflicts_total counter\n");
        o.push_str(&format!(
            "fsqlite_conflicts_total{{response=\"busy_snapshot\"}} {}\n",
            self.conflicts_busy_snapshot_total.get()
        ));
        o.push_str(&format!(
            "fsqlite_conflicts_total{{response=\"rebased\"}} {}\n",
            self.conflicts_rebased_total.get()
        ));
        o.push_str("# HELP fsqlite_integrity_check_runs_total Integrity checks by result.\n# TYPE fsqlite_integrity_check_runs_total counter\n");
        o.push_str(&format!(
            "fsqlite_integrity_check_runs_total{{result=\"ok\"}} {}\n",
            self.integrity_check_ok_total.get()
        ));
        o.push_str(&format!(
            "fsqlite_integrity_check_runs_total{{result=\"fail\"}} {}\n",
            self.integrity_check_fail_total.get()
        ));
        // histograms
        render_histogram(
            &mut o,
            "fsqlite_fsync_duration_seconds",
            "fsync latency.",
            &self.fsync_duration_seconds,
        );
        render_histogram(
            &mut o,
            "fsqlite_commit_duration_seconds",
            "Commit latency.",
            &self.commit_duration_seconds,
        );
        render_histogram(
            &mut o,
            "fsqlite_page_lock_acquire_duration_seconds",
            "Page-lock acquire latency.",
            &self.page_lock_acquire_duration_seconds,
        );
        // gauges
        let gauge = |o: &mut String, name: &str, help: &str, v: i64| {
            o.push_str(&format!(
                "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {v}\n"
            ));
        };
        gauge(
            &mut o,
            "fsqlite_active_writers",
            "Active writer lanes.",
            self.active_writers.get(),
        );
        gauge(
            &mut o,
            "fsqlite_active_readers",
            "Active reader snapshots.",
            self.active_readers.get(),
        );
        gauge(
            &mut o,
            "fsqlite_historical_pins_active",
            "Active historical-snapshot pins.",
            self.historical_pins_active.get(),
        );
        gauge(
            &mut o,
            "fsqlite_wal_frames_pending_checkpoint",
            "WAL frames pending checkpoint.",
            self.wal_frames_pending_checkpoint.get(),
        );
        gauge(
            &mut o,
            "fsqlite_history_records_count",
            "History records.",
            self.history_records_count.get(),
        );
        gauge(
            &mut o,
            "fsqlite_history_bytes",
            "History bytes.",
            self.history_bytes.get(),
        );
        o
    }
}

fn render_histogram(o: &mut String, name: &str, help: &str, h: &Histogram) {
    o.push_str(&format!("# HELP {name} {help}\n# TYPE {name} histogram\n"));
    let counts = h.bucket_counts();
    for (i, bound) in DURATION_BUCKETS_SECONDS.iter().enumerate() {
        o.push_str(&format!("{name}_bucket{{le=\"{bound}\"}} {}\n", counts[i]));
    }
    o.push_str(&format!("{name}_bucket{{le=\"+Inf\"}} {}\n", h.count()));
    o.push_str(&format!("{name}_sum {}\n", h.sum()));
    o.push_str(&format!("{name}_count {}\n", h.count()));
}

/// Convenience: render the global registry.
#[must_use]
pub fn render_prometheus() -> String {
    global().render_prometheus()
}

// ── StatsD push exposition (AC#6) ───────────────────────────────────────────
//
// The push-protocol counterpart to `render_prometheus`. StatsD (specifically
// the DogStatsD tagged dialect — the only StatsD variant that can carry our
// `response`/`result` labels) is a *push* protocol: a client periodically emits
// UDP datagrams that the server aggregates over each flush interval. Because a
// StatsD counter sample (`|c`) is **added** to the server's running total and
// reset each flush, pushing a *cumulative* registry total every interval would
// double-count. [`StatsdEncoder`] therefore holds the previously-encoded
// cumulative values and emits the **delta** since the last encode for
// counter-typed series, and the **absolute** value for gauges.
//
// As with Prometheus (pure `render_prometheus` string + separate HTTP serving),
// the UDP transport (`send_to` on a `std::net::UdpSocket` bound from
// `FRANKENSQLITE_STATSD_ADDR`) is split into the socket-I/O follow-on that also
// brings the HTTP `/metrics` endpoint (AC#2), keeping this module free of
// network I/O and cx side effects. The datagram *content* — privacy-audited and
// unit-testable — is the reusable core delivered here.

/// One cumulative StatsD counter series: metric name, DogStatsD tag suffix
/// (`"|#key:value"`, or `""` when untagged), and the current cumulative value.
struct StatsdCounterSample {
    name: &'static str,
    tag_suffix: &'static str,
    value: u64,
}

/// Round a seconds value to microsecond precision so the summed-latency deltas
/// render cleanly (e.g. `0.0183`) instead of exposing f64 subtraction noise.
/// Microseconds are far finer than any fsync/commit latency SLO.
fn round_micros(seconds: f64) -> f64 {
    (seconds * 1e6).round() / 1e6
}

impl MetricsRegistry {
    /// Integer cumulative counter series, in a fixed positional order (the
    /// [`StatsdEncoder`] diffs against this order). Includes the eight
    /// structural counters plus each latency histogram's observation `.count`.
    fn statsd_counter_series(&self) -> [StatsdCounterSample; 11] {
        [
            StatsdCounterSample {
                name: "fsqlite.commits_total",
                tag_suffix: "",
                value: self.commits_total.get(),
            },
            StatsdCounterSample {
                name: "fsqlite.sweeper_clears_total",
                tag_suffix: "",
                value: self.sweeper_clears_total.get(),
            },
            StatsdCounterSample {
                name: "fsqlite.historical_snapshots_opened_total",
                tag_suffix: "",
                value: self.historical_snapshots_opened_total.get(),
            },
            StatsdCounterSample {
                name: "fsqlite.schema_epoch_bumps_total",
                tag_suffix: "",
                value: self.schema_epoch_bumps_total.get(),
            },
            StatsdCounterSample {
                name: "fsqlite.conflicts_total",
                tag_suffix: "|#response:busy_snapshot",
                value: self.conflicts_busy_snapshot_total.get(),
            },
            StatsdCounterSample {
                name: "fsqlite.conflicts_total",
                tag_suffix: "|#response:rebased",
                value: self.conflicts_rebased_total.get(),
            },
            StatsdCounterSample {
                name: "fsqlite.integrity_check_runs_total",
                tag_suffix: "|#result:ok",
                value: self.integrity_check_ok_total.get(),
            },
            StatsdCounterSample {
                name: "fsqlite.integrity_check_runs_total",
                tag_suffix: "|#result:fail",
                value: self.integrity_check_fail_total.get(),
            },
            StatsdCounterSample {
                name: "fsqlite.fsync_duration_seconds.count",
                tag_suffix: "",
                value: self.fsync_duration_seconds.count(),
            },
            StatsdCounterSample {
                name: "fsqlite.commit_duration_seconds.count",
                tag_suffix: "",
                value: self.commit_duration_seconds.count(),
            },
            StatsdCounterSample {
                name: "fsqlite.page_lock_acquire_duration_seconds.count",
                tag_suffix: "",
                value: self.page_lock_acquire_duration_seconds.count(),
            },
        ]
    }

    /// Cumulative summed-latency series (seconds), positionally ordered. Each
    /// histogram's `.sum` is a monotonically increasing counter; the encoder
    /// emits its per-flush delta so the server can derive average latency as
    /// `rate(sum) / rate(count)`.
    fn statsd_sum_series(&self) -> [(&'static str, f64); 3] {
        [
            (
                "fsqlite.fsync_duration_seconds.sum",
                self.fsync_duration_seconds.sum(),
            ),
            (
                "fsqlite.commit_duration_seconds.sum",
                self.commit_duration_seconds.sum(),
            ),
            (
                "fsqlite.page_lock_acquire_duration_seconds.sum",
                self.page_lock_acquire_duration_seconds.sum(),
            ),
        ]
    }

    /// Gauge series (absolute values, pushed verbatim each flush).
    fn statsd_gauge_series(&self) -> [(&'static str, i64); 6] {
        [
            ("fsqlite.active_writers", self.active_writers.get()),
            ("fsqlite.active_readers", self.active_readers.get()),
            (
                "fsqlite.historical_pins_active",
                self.historical_pins_active.get(),
            ),
            (
                "fsqlite.wal_frames_pending_checkpoint",
                self.wal_frames_pending_checkpoint.get(),
            ),
            (
                "fsqlite.history_records_count",
                self.history_records_count.get(),
            ),
            ("fsqlite.history_bytes", self.history_bytes.get()),
        ]
    }
}

/// Stateful StatsD/DogStatsD datagram encoder (AC#6).
///
/// Owns the last-encoded cumulative snapshot so counter-typed series are emitted
/// as per-flush deltas (StatsD counters are additive per flush) while gauges are
/// absolute. A single long-lived encoder is driven by the (follow-on) UDP push
/// loop.
#[derive(Debug, Default)]
pub struct StatsdEncoder {
    /// Previous cumulative counter values, positionally aligned with
    /// [`MetricsRegistry::statsd_counter_series`]. Empty before the first encode
    /// (so the first datagram carries everything accumulated since start-up).
    last_counters: Vec<u64>,
    /// Previous cumulative summed-latency values, aligned with
    /// [`MetricsRegistry::statsd_sum_series`].
    last_sums: Vec<f64>,
}

impl StatsdEncoder {
    /// Create a fresh encoder with no prior snapshot.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Encode a newline-separated StatsD datagram for the current `reg` state
    /// and advance the internal snapshot. Counter series contribute a `|c` line
    /// carrying the delta since the previous call (zero-deltas omitted); gauges
    /// contribute a `|g` line with the absolute value. Returns an empty string
    /// when metrics are disabled (`FRANKENSQLITE_METRICS_DISABLE=1`).
    #[must_use]
    pub fn encode(&mut self, reg: &MetricsRegistry) -> String {
        if metrics_disabled() {
            return String::new();
        }
        let counters = reg.statsd_counter_series();
        let sums = reg.statsd_sum_series();
        let mut o = String::with_capacity(1024);

        // Integer counters → delta since last encode.
        for (i, c) in counters.iter().enumerate() {
            let prev = self.last_counters.get(i).copied().unwrap_or(0);
            let delta = c.value.saturating_sub(prev);
            if delta != 0 {
                o.push_str(&format!("{}:{delta}|c{}\n", c.name, c.tag_suffix));
            }
        }
        // Summed-latency counters → microsecond-rounded delta since last encode.
        for (i, (name, sum)) in sums.iter().enumerate() {
            let prev = self.last_sums.get(i).copied().unwrap_or(0.0);
            let delta = round_micros(sum - prev);
            if delta > 0.0 {
                o.push_str(&format!("{name}:{delta}|c\n"));
            }
        }
        // Gauges → absolute value every flush.
        for (name, v) in reg.statsd_gauge_series() {
            o.push_str(&format!("{name}:{v}|g\n"));
        }

        self.last_counters = counters.iter().map(|c| c.value).collect();
        self.last_sums = sums.iter().map(|(_, s)| *s).collect();
        o
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counter_increments() {
        let c = Counter::default();
        assert_eq!(c.get(), 0);
        c.inc();
        c.inc_by(4);
        assert_eq!(c.get(), 5);
    }

    #[test]
    fn gauge_up_and_down() {
        let g = Gauge::default();
        g.set(10);
        g.inc();
        g.dec();
        g.add(-3);
        assert_eq!(g.get(), 7);
    }

    #[test]
    fn histogram_buckets_sum_count() {
        let h = Histogram::default();
        h.observe(0.0003); // ≤ 0.0005, 0.001, ...
        h.observe(0.02); // ≤ 0.05, 0.1, ...
        h.observe(2.0); // ≤ 5.0
        assert_eq!(h.count(), 3);
        assert!((h.sum() - 2.0203).abs() < 1e-9);
        let counts = h.bucket_counts();
        // le=0.0001 bucket catches nothing; le=0.0005 catches the 0.0003 obs.
        assert_eq!(counts[0], 0);
        assert_eq!(counts[1], 1);
        // le=0.05 catches 0.0003 and 0.02.
        assert_eq!(counts[5], 2);
        // le=5.0 (last) catches all three.
        assert_eq!(counts[DURATION_BUCKETS_SECONDS.len() - 1], 3);
    }

    #[test]
    fn prometheus_exposition_is_well_formed_and_private() {
        let r = MetricsRegistry::default();
        r.commits_total.inc();
        r.conflicts_busy_snapshot_total.inc_by(2);
        r.integrity_check_ok_total.inc();
        r.fsync_duration_seconds.observe(0.002);
        r.active_writers.set(3);
        let text = r.render_prometheus();
        // structural samples present
        assert!(text.contains("# TYPE fsqlite_commits_total counter"));
        assert!(text.contains("fsqlite_commits_total 1"));
        assert!(text.contains("fsqlite_conflicts_total{response=\"busy_snapshot\"} 2"));
        assert!(text.contains("fsqlite_integrity_check_runs_total{result=\"ok\"} 1"));
        assert!(text.contains("# TYPE fsqlite_fsync_duration_seconds histogram"));
        assert!(text.contains("fsqlite_fsync_duration_seconds_bucket{le=\"+Inf\"} 1"));
        assert!(text.contains("fsqlite_active_writers 3"));
        // privacy (AC#3): only fixed structural labels; never query/table content.
        assert!(!text.to_lowercase().contains("select"));
        assert!(!text.contains("table"));
        // every label key is one of the allowed fixed set.
        for line in text.lines().filter(|l| l.contains('{')) {
            let inside = &line[line.find('{').unwrap() + 1..line.find('}').unwrap()];
            for kv in inside.split(',') {
                let key = kv.split('=').next().unwrap().trim();
                assert!(
                    matches!(key, "response" | "result" | "le"),
                    "unexpected label key {key:?} — privacy audit failed"
                );
            }
        }
    }

    #[test]
    fn statsd_first_encode_emits_cumulative_then_deltas() {
        let r = MetricsRegistry::default();
        r.commits_total.inc_by(3);
        r.conflicts_busy_snapshot_total.inc_by(2);
        r.fsync_duration_seconds.observe(0.002);
        r.active_writers.set(4);

        let mut enc = StatsdEncoder::new();
        let first = enc.encode(&r);
        // Counter: absolute cumulative on the first flush (prev == 0).
        assert!(first.contains("fsqlite.commits_total:3|c\n"), "{first}");
        // Tagged counter carries the DogStatsD suffix after the type.
        assert!(
            first.contains("fsqlite.conflicts_total:2|c|#response:busy_snapshot\n"),
            "{first}"
        );
        // Histogram → observation-count counter + summed-latency counter.
        assert!(
            first.contains("fsqlite.fsync_duration_seconds.count:1|c\n"),
            "{first}"
        );
        assert!(
            first.contains("fsqlite.fsync_duration_seconds.sum:0.002|c\n"),
            "{first}"
        );
        // Gauge: absolute value.
        assert!(first.contains("fsqlite.active_writers:4|g\n"), "{first}");

        // Second flush with no new counter activity: counters/sums drop out
        // (zero delta), gauges are still pushed at their absolute value.
        let second = enc.encode(&r);
        assert!(!second.contains("fsqlite.commits_total"), "{second}");
        assert!(
            !second.contains("fsqlite.fsync_duration_seconds.count"),
            "{second}"
        );
        assert!(
            !second.contains("fsqlite.fsync_duration_seconds.sum"),
            "{second}"
        );
        assert!(second.contains("fsqlite.active_writers:4|g\n"), "{second}");

        // Third flush after more activity: counter emits only the *delta*.
        r.commits_total.inc_by(5);
        r.fsync_duration_seconds.observe(0.010);
        r.active_writers.set(2);
        let third = enc.encode(&r);
        assert!(third.contains("fsqlite.commits_total:5|c\n"), "{third}");
        assert!(
            third.contains("fsqlite.fsync_duration_seconds.count:1|c\n"),
            "{third}"
        );
        assert!(
            third.contains("fsqlite.fsync_duration_seconds.sum:0.01|c\n"),
            "{third}"
        );
        assert!(third.contains("fsqlite.active_writers:2|g\n"), "{third}");
    }

    #[test]
    fn statsd_datagram_is_private_and_well_typed() {
        let r = MetricsRegistry::default();
        r.commits_total.inc();
        r.conflicts_rebased_total.inc();
        r.integrity_check_fail_total.inc();
        r.commit_duration_seconds.observe(0.05);
        r.history_bytes.set(4096);
        let datagram = StatsdEncoder::new().encode(&r);

        for line in datagram.lines() {
            // Every line is `name:value|type[|#tags]`.
            let (name_val, rest) = line.split_once('|').expect("statsd type field");
            assert!(name_val.contains(':'), "malformed sample: {line:?}");
            // Type is a known StatsD type; tags (if any) use only fixed keys.
            let mut parts = rest.splitn(2, "|#");
            let ty = parts.next().unwrap();
            assert!(matches!(ty, "c" | "g"), "unexpected statsd type {ty:?}");
            if let Some(tags) = parts.next() {
                for tag in tags.split(',') {
                    let key = tag.split(':').next().unwrap();
                    assert!(
                        matches!(key, "response" | "result"),
                        "unexpected tag key {key:?} — statsd privacy audit failed"
                    );
                }
            }
        }
        // Privacy (AC#3): no query/table content leaks into the datagram.
        assert!(!datagram.to_lowercase().contains("select"));
        assert!(!datagram.contains("table"));
    }

    #[test]
    fn statsd_disabled_yields_empty() {
        // Only asserts the disabled-path shape when the env var is actually set
        // (tests must not mutate process env); otherwise the encoder is exercised
        // by the other StatsD tests. This keeps the assertion deterministic under
        // parallel test execution.
        if metrics_disabled() {
            let r = MetricsRegistry::default();
            r.commits_total.inc();
            assert!(StatsdEncoder::new().encode(&r).is_empty());
        }
    }
}
