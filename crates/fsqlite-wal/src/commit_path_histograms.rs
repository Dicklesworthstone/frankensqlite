//! Supplementary edge-case tests for commit-path histograms and wake-reason
//! accounting (bd-db300.3.8.1).
//!
//! The canonical types (`PhaseHistogram`, `PhasePercentiles`,
//! `WakeReasonCounters`, `WakeReasonSnapshot`) live in `group_commit.rs`.
//! This module adds edge-case coverage via the global singleton.

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use crate::group_commit::{
        GLOBAL_CONSOLIDATION_METRICS, GLOBAL_CONSOLIDATION_METRICS_TEST_LOCK,
    };

    struct ResetGlobalMetrics;

    impl Drop for ResetGlobalMetrics {
        fn drop(&mut self) {
            GLOBAL_CONSOLIDATION_METRICS.reset();
        }
    }

    fn with_global_metrics<T>(body: impl FnOnce() -> T) -> T {
        let _guard = GLOBAL_CONSOLIDATION_METRICS_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _reset = ResetGlobalMetrics;
        GLOBAL_CONSOLIDATION_METRICS.reset();
        body()
    }

    // ── Global histogram recording and snapshot ─────────────────────

    #[test]
    fn global_hist_phase_b_records_and_snapshots() {
        with_global_metrics(|| {
            GLOBAL_CONSOLIDATION_METRICS.hist_phase_b.record(100);
            GLOBAL_CONSOLIDATION_METRICS.hist_phase_b.record(200);
            let s = GLOBAL_CONSOLIDATION_METRICS.snapshot();
            assert_eq!(s.hist_phase_b.count, 2);
            assert_eq!(s.hist_phase_b.max, 200);
        });
    }

    #[test]
    fn global_hist_wal_append_records() {
        with_global_metrics(|| {
            GLOBAL_CONSOLIDATION_METRICS.hist_wal_append.record(50);
            let s = GLOBAL_CONSOLIDATION_METRICS.snapshot();
            assert_eq!(s.hist_wal_append.count, 1);
            assert_eq!(s.hist_wal_append.max, 50);
        });
    }

    #[test]
    fn global_hist_exclusive_lock_records() {
        with_global_metrics(|| {
            GLOBAL_CONSOLIDATION_METRICS.hist_exclusive_lock.record(10);
            let s = GLOBAL_CONSOLIDATION_METRICS.snapshot();
            assert_eq!(s.hist_exclusive_lock.count, 1);
            assert_eq!(s.hist_exclusive_lock.max, 10);
        });
    }

    #[test]
    fn global_hist_consolidator_lock_wait_records() {
        with_global_metrics(|| {
            GLOBAL_CONSOLIDATION_METRICS
                .hist_consolidator_lock_wait
                .record(5);
            let s = GLOBAL_CONSOLIDATION_METRICS.snapshot();
            assert_eq!(s.hist_consolidator_lock_wait.count, 1);
            assert_eq!(s.hist_consolidator_lock_wait.max, 5);
        });
    }

    #[test]
    fn global_hist_waiter_epoch_wait_records() {
        with_global_metrics(|| {
            GLOBAL_CONSOLIDATION_METRICS
                .hist_waiter_epoch_wait
                .record(200);
            let s = GLOBAL_CONSOLIDATION_METRICS.snapshot();
            assert_eq!(s.hist_waiter_epoch_wait.count, 1);
            assert_eq!(s.hist_waiter_epoch_wait.max, 200);
        });
    }

    #[test]
    fn global_hist_arrival_wait_records() {
        with_global_metrics(|| {
            GLOBAL_CONSOLIDATION_METRICS.hist_arrival_wait.record(15);
            let s = GLOBAL_CONSOLIDATION_METRICS.snapshot();
            assert_eq!(s.hist_arrival_wait.count, 1);
            assert_eq!(s.hist_arrival_wait.max, 15);
        });
    }

    #[test]
    fn global_hist_wal_sync_records() {
        with_global_metrics(|| {
            GLOBAL_CONSOLIDATION_METRICS.hist_wal_sync.record(80);
            let s = GLOBAL_CONSOLIDATION_METRICS.snapshot();
            assert_eq!(s.hist_wal_sync.count, 1);
            assert_eq!(s.hist_wal_sync.max, 80);
        });
    }

    #[test]
    fn global_hist_full_commit_records() {
        with_global_metrics(|| {
            GLOBAL_CONSOLIDATION_METRICS.hist_full_commit.record(300);
            let s = GLOBAL_CONSOLIDATION_METRICS.snapshot();
            assert_eq!(s.hist_full_commit.count, 1);
            assert_eq!(s.hist_full_commit.max, 300);
        });
    }

    // ── Wake-reason global counters ─────────────────────────────────

    #[test]
    fn global_wake_reason_notify_increments() {
        with_global_metrics(|| {
            GLOBAL_CONSOLIDATION_METRICS
                .wake_reasons
                .notify
                .fetch_add(1, Ordering::Relaxed);
            let s = GLOBAL_CONSOLIDATION_METRICS.snapshot();
            assert_eq!(s.wake_reasons.notify, 1);
        });
    }

    #[test]
    fn global_wake_reason_timeout_increments() {
        with_global_metrics(|| {
            GLOBAL_CONSOLIDATION_METRICS
                .wake_reasons
                .timeout
                .fetch_add(1, Ordering::Relaxed);
            let s = GLOBAL_CONSOLIDATION_METRICS.snapshot();
            assert_eq!(s.wake_reasons.timeout, 1);
        });
    }

    #[test]
    fn global_wake_reason_flusher_takeover_increments() {
        with_global_metrics(|| {
            GLOBAL_CONSOLIDATION_METRICS
                .wake_reasons
                .flusher_takeover
                .fetch_add(1, Ordering::Relaxed);
            let s = GLOBAL_CONSOLIDATION_METRICS.snapshot();
            assert_eq!(s.wake_reasons.flusher_takeover, 1);
        });
    }

    #[test]
    fn global_wake_reason_failed_epoch_increments() {
        with_global_metrics(|| {
            GLOBAL_CONSOLIDATION_METRICS
                .wake_reasons
                .failed_epoch
                .fetch_add(1, Ordering::Relaxed);
            let s = GLOBAL_CONSOLIDATION_METRICS.snapshot();
            assert_eq!(s.wake_reasons.failed_epoch, 1);
        });
    }

    #[test]
    fn global_wake_reason_busy_retry_increments() {
        with_global_metrics(|| {
            GLOBAL_CONSOLIDATION_METRICS
                .wake_reasons
                .busy_retry
                .fetch_add(1, Ordering::Relaxed);
            let s = GLOBAL_CONSOLIDATION_METRICS.snapshot();
            assert_eq!(s.wake_reasons.busy_retry, 1);
        });
    }

    #[test]
    fn global_wake_reason_total_is_nonnegative() {
        with_global_metrics(|| {
            let s = GLOBAL_CONSOLIDATION_METRICS.snapshot();
            assert_eq!(s.wake_reasons.total(), 0);
        });
    }

    // ── Histogram percentile structure ──────────────────────────────

    #[test]
    fn global_hist_percentiles_are_ordered() {
        with_global_metrics(|| {
            for i in 1..=100u64 {
                GLOBAL_CONSOLIDATION_METRICS
                    .hist_wal_backend_lock_wait
                    .record(i);
            }
            let s = GLOBAL_CONSOLIDATION_METRICS.snapshot();
            let p = s.hist_wal_backend_lock_wait;
            assert_eq!(p.count, 100);
            assert!(p.p50 <= p.p95, "p50 <= p95");
            assert!(p.p95 <= p.p99, "p95 <= p99");
        });
    }

    #[test]
    fn global_reset_zeroes_all_histograms_and_counters() {
        with_global_metrics(|| {
            GLOBAL_CONSOLIDATION_METRICS.hist_phase_b.record(999);
            GLOBAL_CONSOLIDATION_METRICS.hist_full_commit.record(888);
            GLOBAL_CONSOLIDATION_METRICS
                .wake_reasons
                .notify
                .fetch_add(5, Ordering::Relaxed);
            GLOBAL_CONSOLIDATION_METRICS.reset();
            let s = GLOBAL_CONSOLIDATION_METRICS.snapshot();
            assert_eq!(s.hist_phase_b.count, 0);
            assert_eq!(s.hist_full_commit.count, 0);
            assert_eq!(s.wake_reasons.notify, 0);
            assert_eq!(s.wake_reasons.total(), 0);
        });
    }

    #[test]
    fn global_hist_accumulates_multiple_records() {
        with_global_metrics(|| {
            for v in [10, 20, 30, 40, 50] {
                GLOBAL_CONSOLIDATION_METRICS.hist_phase_b.record(v);
            }
            let s = GLOBAL_CONSOLIDATION_METRICS.snapshot();
            assert_eq!(s.hist_phase_b.count, 5);
            assert_eq!(s.hist_phase_b.max, 50);
        });
    }

    #[test]
    fn global_wake_reasons_total_sums_all_fields() {
        with_global_metrics(|| {
            GLOBAL_CONSOLIDATION_METRICS
                .wake_reasons
                .notify
                .fetch_add(2, Ordering::Relaxed);
            GLOBAL_CONSOLIDATION_METRICS
                .wake_reasons
                .timeout
                .fetch_add(3, Ordering::Relaxed);
            GLOBAL_CONSOLIDATION_METRICS
                .wake_reasons
                .flusher_takeover
                .fetch_add(1, Ordering::Relaxed);
            GLOBAL_CONSOLIDATION_METRICS
                .wake_reasons
                .failed_epoch
                .fetch_add(4, Ordering::Relaxed);
            GLOBAL_CONSOLIDATION_METRICS
                .wake_reasons
                .busy_retry
                .fetch_add(5, Ordering::Relaxed);
            let s = GLOBAL_CONSOLIDATION_METRICS.snapshot();
            assert_eq!(s.wake_reasons.total(), 2 + 3 + 1 + 4 + 5);
        });
    }

    #[test]
    fn global_hist_single_value_all_percentiles_equal() {
        with_global_metrics(|| {
            GLOBAL_CONSOLIDATION_METRICS
                .hist_wal_backend_lock_wait
                .record(42);
            let s = GLOBAL_CONSOLIDATION_METRICS.snapshot();
            let p = s.hist_wal_backend_lock_wait;
            assert_eq!(p.count, 1);
            assert_eq!(p.max, 42);
            assert_eq!(p.p50, 42);
            assert_eq!(p.p95, 42);
            assert_eq!(p.p99, 42);
        });
    }

    #[test]
    fn recent_tail_us_tracks_spike_then_decays() {
        with_global_metrics(|| {
            GLOBAL_CONSOLIDATION_METRICS.hist_phase_b.record(1000);
            let after_spike = GLOBAL_CONSOLIDATION_METRICS.hist_phase_b.recent_tail_us();
            assert!(after_spike >= 1000, "tail must capture spike");
            for _ in 0..20 {
                GLOBAL_CONSOLIDATION_METRICS.hist_phase_b.record(1);
            }
            let after_decay = GLOBAL_CONSOLIDATION_METRICS.hist_phase_b.recent_tail_us();
            assert!(
                after_decay < after_spike,
                "tail should decay toward small values"
            );
        });
    }

    #[test]
    fn phase_percentiles_mean_us_computed_correctly() {
        with_global_metrics(|| {
            for v in [10, 20, 30] {
                GLOBAL_CONSOLIDATION_METRICS.hist_wal_sync.record(v);
            }
            let s = GLOBAL_CONSOLIDATION_METRICS.snapshot();
            assert_eq!(s.hist_wal_sync.mean_us, 20);
        });
    }

    #[test]
    fn phase_percentiles_derive_debug_clone_copy_default() {
        use crate::group_commit::PhasePercentiles;
        let p = PhasePercentiles {
            p50: 1,
            p95: 2,
            p99: 3,
            max: 4,
            count: 5,
            mean_us: 6,
        };
        let dbg = format!("{p:?}");
        assert!(dbg.contains("PhasePercentiles"));
        let cloned = p;
        assert_eq!(p, cloned);
        let def = PhasePercentiles::default();
        assert_eq!(def.count, 0);
    }

    #[test]
    fn wake_reason_snapshot_derive_debug_clone_serialize() {
        use crate::group_commit::WakeReasonSnapshot;
        let w = WakeReasonSnapshot {
            notify: 1,
            timeout: 2,
            flusher_takeover: 3,
            failed_epoch: 4,
            busy_retry: 5,
        };
        let dbg = format!("{w:?}");
        assert!(dbg.contains("WakeReasonSnapshot"));
        let json = serde_json::to_string(&w).expect("serialize");
        assert!(json.contains("\"flusher_takeover\":3"));
        let cloned = w;
        assert_eq!(w, cloned);
    }
}
