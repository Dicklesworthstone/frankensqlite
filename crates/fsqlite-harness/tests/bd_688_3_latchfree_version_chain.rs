//! bd-688.3: Latch-free MVCC Version Chain (Hekaton-style)
//!
//! Tests for the sharded, CAS-based `ChainHeadTable` that replaces the
//! `RwLock<HashMap>` chain head map in `VersionStore`.

use std::sync::Arc;

use fsqlite_mvcc::observability::set_mvcc_cas_metrics_enabled;
use fsqlite_mvcc::{
    CHAIN_HEAD_EMPTY, CHAIN_HEAD_SHARDS, VersionStore, cas_metrics_snapshot, idx_to_version_pointer,
};
use fsqlite_types::{
    CommitSeq, PageData, PageNumber, PageSize, PageVersion, SchemaEpoch, Snapshot, TxnEpoch, TxnId,
    TxnToken,
};

const BEAD: &str = "bd-688.3";

fn make_snapshot(high: u64) -> Snapshot {
    Snapshot::new(CommitSeq::new(high), SchemaEpoch::ZERO)
}

fn make_version(
    pgno: u32,
    commit_seq: u64,
    prev: Option<fsqlite_types::VersionPointer>,
) -> PageVersion {
    PageVersion {
        pgno: PageNumber::new(pgno).unwrap(),
        commit_seq: CommitSeq::new(commit_seq),
        created_by: TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0)),
        data: PageData::zeroed(PageSize::DEFAULT),
        prev,
    }
}

// ---------------------------------------------------------------------------
// Unit tests: basic ChainHeadTable operations via VersionStore
// ---------------------------------------------------------------------------

#[test]
fn chain_head_basic_get_and_publish() {
    let store = VersionStore::new(PageSize::DEFAULT);
    let pgno = PageNumber::new(1).unwrap();

    // Initially empty.
    assert!(
        store.chain_head(pgno).is_none(),
        "bead_id={BEAD} empty store should return None for chain head"
    );

    // Publish first version.
    let v1 = make_version(1, 1, None);
    let idx1 = store.publish(v1);

    // Read back.
    assert_eq!(
        store.chain_head(pgno),
        Some(idx1),
        "bead_id={BEAD} chain_head should return published idx"
    );
}

#[test]
fn chain_head_update_overwrites() {
    let store = VersionStore::new(PageSize::DEFAULT);
    let pgno = PageNumber::new(42).unwrap();

    let v1 = make_version(42, 1, None);
    let idx1 = store.publish(v1);
    assert_eq!(store.chain_head(pgno), Some(idx1));

    // Overwrite with v2.
    let v2 = make_version(42, 5, Some(idx_to_version_pointer(idx1)));
    let idx2 = store.publish(v2);
    assert_eq!(
        store.chain_head(pgno),
        Some(idx2),
        "bead_id={BEAD} chain head should be updated to latest publish"
    );
}

#[test]
fn chain_head_empty_sentinel_constant() {
    assert_eq!(
        CHAIN_HEAD_EMPTY,
        u64::MAX,
        "bead_id={BEAD} empty sentinel should be u64::MAX"
    );
}

#[test]
fn chain_head_sharding_constant() {
    // 128 matches MAX_CONCURRENT_WRITERS so a full complement of concurrent
    // writers averages <=1 per shard on 128-thread hosts.
    assert_eq!(
        CHAIN_HEAD_SHARDS, 128,
        "bead_id={BEAD} shard count should be 128"
    );
}

// ---------------------------------------------------------------------------
// VersionStore integration: prev pointer correctness
// ---------------------------------------------------------------------------

#[test]
fn version_store_publish_sets_prev_pointer() {
    let store = VersionStore::new(PageSize::DEFAULT);
    let pgno = PageNumber::new(1).unwrap();

    let v1 = make_version(1, 1, None);
    let idx1 = store.publish(v1);

    let v2 = make_version(1, 5, Some(idx_to_version_pointer(idx1)));
    let idx2 = store.publish(v2);

    // Chain head should be idx2.
    assert_eq!(store.chain_head(pgno), Some(idx2));

    // Walk chain: [5, 1].
    let chain = store.walk_chain(pgno);
    assert_eq!(chain.len(), 2);
    assert_eq!(chain[0].commit_seq, CommitSeq::new(5));
    assert_eq!(chain[1].commit_seq, CommitSeq::new(1));
}

#[test]
fn version_store_resolve_still_works() {
    let store = VersionStore::new(PageSize::DEFAULT);
    let pgno = PageNumber::new(1).unwrap();

    let v1 = make_version(1, 1, None);
    let idx1 = store.publish(v1);
    let v2 = make_version(1, 5, Some(idx_to_version_pointer(idx1)));
    let idx2 = store.publish(v2);
    let v3 = make_version(1, 10, Some(idx_to_version_pointer(idx2)));
    store.publish(v3);

    // Snapshot at 7: should see v2 (seq=5).
    let snap = make_snapshot(7);
    let resolved = store.resolve(pgno, &snap).unwrap();
    let version = store.get_version(resolved).unwrap();
    assert_eq!(version.commit_seq, CommitSeq::new(5));

    // Snapshot at 10: should see v3.
    let snap10 = make_snapshot(10);
    let resolved10 = store.resolve(pgno, &snap10).unwrap();
    let version10 = store.get_version(resolved10).unwrap();
    assert_eq!(version10.commit_seq, CommitSeq::new(10));

    // Snapshot at 0: nothing visible.
    let snap0 = make_snapshot(0);
    assert!(store.resolve(pgno, &snap0).is_none());
}

#[test]
fn version_store_multiple_pages_independent() {
    let store = VersionStore::new(PageSize::DEFAULT);

    // Publish to 10 different pages.
    for i in 1..=10_u32 {
        let v = make_version(i, u64::from(i), None);
        store.publish(v);
    }

    // Each page should have exactly one version.
    for i in 1..=10_u32 {
        let pgno = PageNumber::new(i).unwrap();
        assert!(store.chain_head(pgno).is_some());
        let chain = store.walk_chain(pgno);
        assert_eq!(chain.len(), 1, "page {} should have 1 version", i);
    }
}

// ---------------------------------------------------------------------------
// Stress tests: concurrent publish to different pages
// ---------------------------------------------------------------------------

#[test]
fn stress_concurrent_publish_different_pages() {
    let store = Arc::new(VersionStore::new(PageSize::DEFAULT));
    let num_threads = 64;
    let pages_per_thread = 100;

    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let store = Arc::clone(&store);
            std::thread::spawn(move || {
                for i in 0..pages_per_thread {
                    #[allow(clippy::cast_possible_truncation)]
                    let pgno = (t * pages_per_thread + i + 1) as u32;
                    let version = make_version(pgno, (t as u64) * 1000 + i as u64 + 1, None);
                    store.publish(version);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // All pages should be resolvable.
    let total_pages = num_threads * pages_per_thread;
    for pgno_raw in 1..=total_pages {
        #[allow(clippy::cast_possible_truncation)]
        let pgno = PageNumber::new(pgno_raw as u32).unwrap();
        assert!(
            store.chain_head(pgno).is_some(),
            "bead_id={BEAD} page {} should have a chain head after concurrent publish",
            pgno_raw
        );
    }
}

#[test]
fn stress_concurrent_resolve_while_publishing() {
    let store = Arc::new(VersionStore::new(PageSize::DEFAULT));

    // Pre-publish some versions.
    for i in 1..=100_u32 {
        let version = make_version(i, 1, None);
        store.publish(version);
    }

    let num_writer_threads = 8;
    let num_reader_threads = 16;

    let mut handles: Vec<_> = (0..num_writer_threads)
        .map(|t| {
            let store = Arc::clone(&store);
            std::thread::spawn(move || {
                for round in 0..50 {
                    #[allow(clippy::cast_possible_truncation)]
                    let pgno = (t * 12 + round % 12 + 1) as u32;
                    let prev = store.chain_head(PageNumber::new(pgno).unwrap());
                    let prev_ptr = prev.map(idx_to_version_pointer);
                    let seq = 100 + t as u64 * 50 + round as u64;
                    let version = make_version(pgno, seq, prev_ptr);
                    store.publish(version);
                }
            })
        })
        .collect();

    let reader_handles: Vec<_> = (0..num_reader_threads)
        .map(|t| {
            let store = Arc::clone(&store);
            std::thread::spawn(move || {
                let snap = make_snapshot(u64::MAX);
                for round in 0..100 {
                    #[allow(clippy::cast_possible_truncation)]
                    let pgno = PageNumber::new((t * 6 + round % 100 + 1) as u32).unwrap();
                    // Resolve may return None if the page hasn't been published yet.
                    let _ = store.resolve(pgno, &snap);
                }
            })
        })
        .collect();

    handles.extend(reader_handles);

    for h in handles {
        h.join().unwrap();
    }
}

// ---------------------------------------------------------------------------
// CAS success rate validation
// ---------------------------------------------------------------------------

#[test]
fn cas_success_rate_under_moderate_contention() {
    // 8 threads, 100 pages, measure first-attempt CAS success rate.
    // Use snapshot-delta pattern to avoid interference from parallel tests.
    set_mvcc_cas_metrics_enabled(true);
    let before = cas_metrics_snapshot();

    let store = Arc::new(VersionStore::new(PageSize::DEFAULT));
    let num_threads = 8;
    let ops_per_thread = 200;

    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let store = Arc::clone(&store);
            std::thread::spawn(move || {
                for i in 0..ops_per_thread {
                    // Spread across 100 pages to create moderate contention.
                    #[allow(clippy::cast_possible_truncation)]
                    let pgno = ((t * 13 + i * 7) % 100 + 1) as u32;
                    let prev = store.chain_head(PageNumber::new(pgno).unwrap());
                    let prev_ptr = prev.map(idx_to_version_pointer);
                    let seq = t as u64 * 1000 + i as u64 + 1;
                    let version = make_version(pgno, seq, prev_ptr);
                    store.publish(version);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let after = cas_metrics_snapshot();
    let total_ops = num_threads * ops_per_thread;
    let delta_attempts = after.attempts_total - before.attempts_total;
    let delta_le_1 = after.retries.le_1 - before.retries.le_1;
    assert!(
        delta_attempts >= total_ops as u64,
        "bead_id={BEAD} CAS attempt delta {delta_attempts} should be >= total publish count {total_ops}"
    );

    // >95% first-attempt success under moderate contention.
    #[allow(clippy::cast_precision_loss)]
    let first_attempt_pct = delta_le_1 as f64 / delta_attempts as f64 * 100.0;
    assert!(
        first_attempt_pct > 95.0,
        "bead_id={BEAD} first-attempt CAS success rate {first_attempt_pct:.1}% should be >95%"
    );
}

// ---------------------------------------------------------------------------
// ABA protection test
// ---------------------------------------------------------------------------

#[test]
fn aba_protection_interleave_gc_and_publish() {
    // This test verifies that interleaving GC (which can remove chain heads)
    // and publish (which installs new heads) doesn't cause data loss.
    let store = VersionStore::new(PageSize::DEFAULT);
    let pgno = PageNumber::new(1).unwrap();

    // Build initial chain: V1(seq=1) <- V2(seq=5) <- V3(seq=10).
    let v1 = make_version(1, 1, None);
    let idx1 = store.publish(v1);
    let v2 = make_version(1, 5, Some(idx_to_version_pointer(idx1)));
    let idx2 = store.publish(v2);
    let v3 = make_version(1, 10, Some(idx_to_version_pointer(idx2)));
    let idx3 = store.publish(v3);

    // GC at horizon=10 should prune versions 1 and 2.
    let mut todo = fsqlite_mvcc::GcTodo::new();
    todo.enqueue(pgno);
    let gc_result = store.gc_tick(&mut todo, CommitSeq::new(10));
    assert!(gc_result.versions_freed >= 2, "should prune old versions");

    // Now publish V4 after GC.
    let v4 = make_version(1, 15, Some(idx_to_version_pointer(idx3)));
    let idx4 = store.publish(v4);

    // Chain head should be V4.
    assert_eq!(store.chain_head(pgno), Some(idx4));

    // Resolve at snapshot 15 should see V4.
    let snap = make_snapshot(15);
    let resolved = store.resolve(pgno, &snap).unwrap();
    let version = store.get_version(resolved).unwrap();
    assert_eq!(
        version.commit_seq,
        CommitSeq::new(15),
        "bead_id={BEAD} publish after GC should install correctly"
    );
}

// ---------------------------------------------------------------------------
// Metrics and tracing tests
// ---------------------------------------------------------------------------

#[test]
fn cas_metrics_reset_and_increment() {
    // Use snapshot-delta pattern to avoid interference from parallel tests.
    let store = VersionStore::new(PageSize::DEFAULT);
    set_mvcc_cas_metrics_enabled(true);
    let before = cas_metrics_snapshot();

    for i in 1..=10_u32 {
        let version = make_version(i, u64::from(i), None);
        store.publish(version);
    }

    let after = cas_metrics_snapshot();
    let delta_attempts = after.attempts_total - before.attempts_total;
    let delta_le_1 = after.retries.le_1 - before.retries.le_1;
    assert!(
        delta_attempts >= 10,
        "bead_id={BEAD} CAS attempt delta {delta_attempts} should be >= 10"
    );
    assert!(
        delta_le_1 >= 10,
        "bead_id={BEAD} uncontended publishes should all be first-attempt (delta_le_1={delta_le_1})"
    );
}

// ---------------------------------------------------------------------------
// Property tests
// ---------------------------------------------------------------------------

mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn prop_publish_resolve_roundtrip(
            pgno_raw in 1_u32..1000,
            commit_seq in 1_u64..10000,
        ) {
            let store = VersionStore::new(PageSize::DEFAULT);
            let version = make_version(pgno_raw, commit_seq, None);
            store.publish(version);

            let pgno = PageNumber::new(pgno_raw).unwrap();
            let snap = make_snapshot(commit_seq);
            let resolved = store.resolve(pgno, &snap);
            prop_assert!(resolved.is_some(), "published version must be resolvable");

            let v = store.get_version(resolved.unwrap()).unwrap();
            prop_assert_eq!(v.commit_seq, CommitSeq::new(commit_seq));
        }

        #[test]
        fn prop_chain_head_has_highest_commit_seq(
            num_versions in 2_usize..20,
        ) {
            let store = VersionStore::new(PageSize::DEFAULT);
            let pgno = PageNumber::new(1).unwrap();
            let mut prev: Option<fsqlite_types::VersionPointer> = None;

            for seq in 1..=num_versions as u64 {
                let version = make_version(1, seq, prev);
                let idx = store.publish(version);
                prev = Some(idx_to_version_pointer(idx));
            }

            // Chain head should have the highest commit_seq.
            let head_idx = store.chain_head(pgno).unwrap();
            let head_version = store.get_version(head_idx).unwrap();
            prop_assert_eq!(head_version.commit_seq, CommitSeq::new(num_versions as u64));
        }

        #[test]
        fn prop_metrics_match_publish_count(
            count in 1_usize..50,
        ) {
            // Use snapshot-delta pattern to avoid interference from parallel tests.
            set_mvcc_cas_metrics_enabled(true);
            let before = cas_metrics_snapshot();
            let store = VersionStore::new(PageSize::DEFAULT);

            for i in 1..=count {
                #[allow(clippy::cast_possible_truncation)]
                let version = make_version(i as u32, i as u64, None);
                store.publish(version);
            }

            let after = cas_metrics_snapshot();
            let delta = after.attempts_total - before.attempts_total;
            prop_assert!(
                delta >= count as u64,
                "CAS attempt delta {} must be >= publish count {}",
                delta,
                count
            );
        }
    }
}
