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
//! Follow-on increments (tracked on the bead): the HTTP `/metrics` endpoint
//! (asupersync/`std::net`, since tokio is forbidden), the StatsD push path, the
//! engine hot-path wiring that increments these metrics, and the perf/soak gate.

use std::sync::LazyLock;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

/// Prometheus `le` bucket bounds (seconds) for the latency histograms — from
/// 100µs to 5s, covering fsync / commit / page-lock acquisition latencies.
const DURATION_BUCKETS_SECONDS: [f64; 10] = [
    0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0,
];

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
        counter(&mut o, "fsqlite_commits_total", "Committed transactions.", "", self.commits_total.get());
        counter(&mut o, "fsqlite_sweeper_clears_total", "Sweeper clear events.", "", self.sweeper_clears_total.get());
        counter(&mut o, "fsqlite_historical_snapshots_opened_total", "Historical snapshots opened.", "", self.historical_snapshots_opened_total.get());
        counter(&mut o, "fsqlite_schema_epoch_bumps_total", "Schema-epoch bumps.", "", self.schema_epoch_bumps_total.get());
        // labeled counters (single HELP/TYPE, multiple label series)
        o.push_str("# HELP fsqlite_conflicts_total Write conflicts by resolution.\n# TYPE fsqlite_conflicts_total counter\n");
        o.push_str(&format!("fsqlite_conflicts_total{{response=\"busy_snapshot\"}} {}\n", self.conflicts_busy_snapshot_total.get()));
        o.push_str(&format!("fsqlite_conflicts_total{{response=\"rebased\"}} {}\n", self.conflicts_rebased_total.get()));
        o.push_str("# HELP fsqlite_integrity_check_runs_total Integrity checks by result.\n# TYPE fsqlite_integrity_check_runs_total counter\n");
        o.push_str(&format!("fsqlite_integrity_check_runs_total{{result=\"ok\"}} {}\n", self.integrity_check_ok_total.get()));
        o.push_str(&format!("fsqlite_integrity_check_runs_total{{result=\"fail\"}} {}\n", self.integrity_check_fail_total.get()));
        // histograms
        render_histogram(&mut o, "fsqlite_fsync_duration_seconds", "fsync latency.", &self.fsync_duration_seconds);
        render_histogram(&mut o, "fsqlite_commit_duration_seconds", "Commit latency.", &self.commit_duration_seconds);
        render_histogram(&mut o, "fsqlite_page_lock_acquire_duration_seconds", "Page-lock acquire latency.", &self.page_lock_acquire_duration_seconds);
        // gauges
        let gauge = |o: &mut String, name: &str, help: &str, v: i64| {
            o.push_str(&format!("# HELP {name} {help}\n# TYPE {name} gauge\n{name} {v}\n"));
        };
        gauge(&mut o, "fsqlite_active_writers", "Active writer lanes.", self.active_writers.get());
        gauge(&mut o, "fsqlite_active_readers", "Active reader snapshots.", self.active_readers.get());
        gauge(&mut o, "fsqlite_historical_pins_active", "Active historical-snapshot pins.", self.historical_pins_active.get());
        gauge(&mut o, "fsqlite_wal_frames_pending_checkpoint", "WAL frames pending checkpoint.", self.wal_frames_pending_checkpoint.get());
        gauge(&mut o, "fsqlite_history_records_count", "History records.", self.history_records_count.get());
        gauge(&mut o, "fsqlite_history_bytes", "History bytes.", self.history_bytes.get());
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
}
