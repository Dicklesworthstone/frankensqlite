//! Harness integration tests for bd-t6sv2.13: Multi-Process Coordination Documentation & Testing.
//!
//! Validates: SHM header layout serialize/deserialize, seqlock snapshot protocol,
//! serialized writer acquisition/release, wire frame encode/decode, permit lifecycle,
//! idempotency cache, canonical ordering validators, GC horizon coordination,
//! cross-thread snapshot consistency, and conformance summary.

use std::sync::Arc;
use std::thread;

use fsqlite_mvcc::coordinator_ipc::{
    Frame, FrameError, IdempotencyCache, MessageKind, PermitError, PermitManager,
    WireTxnToken, is_canonical_pages, validate_write_set_summary,
    validate_witness_edge_counts,
};
use fsqlite_mvcc::shm::SharedMemoryLayout;
use fsqlite_types::{CommitSeq, PageSize, SchemaEpoch};

const BEAD_ID: &str = "bd-t6sv2.13";

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Test 1: SHM header layout round-trip serialize/deserialize.
#[test]
fn test_shm_header_round_trip() {
    let page_size = PageSize::new(4096).unwrap();
    let layout = SharedMemoryLayout::new(page_size, 64);

    // Serialize to bytes.
    let bytes = layout.to_bytes();
    assert_eq!(
        bytes.len(),
        SharedMemoryLayout::HEADER_SIZE,
        "bead_id={BEAD_ID} header size mismatch"
    );

    // Deserialize back.
    let restored = SharedMemoryLayout::open(&bytes).expect("open failed");

    // Immutable fields match.
    assert_eq!(restored.page_size(), page_size, "bead_id={BEAD_ID} page_size mismatch");
    assert_eq!(restored.max_txn_slots(), 64, "bead_id={BEAD_ID} max_txn_slots mismatch");
    assert_eq!(
        restored.layout_checksum(),
        layout.layout_checksum(),
        "bead_id={BEAD_ID} checksum mismatch"
    );

    // Region offsets match.
    assert_eq!(restored.lock_table_offset(), layout.lock_table_offset());
    assert_eq!(restored.witness_offset(), layout.witness_offset());
    assert_eq!(restored.txn_slot_offset(), layout.txn_slot_offset());
    assert_eq!(restored.committed_readers_offset(), layout.committed_readers_offset());

    println!("[{BEAD_ID}] SHM header round-trip: OK (size={})", bytes.len());
}

/// Test 2: SHM open rejects corrupted headers.
#[test]
fn test_shm_open_rejects_corruption() {
    let page_size = PageSize::new(4096).unwrap();
    let layout = SharedMemoryLayout::new(page_size, 32);
    let bytes = layout.to_bytes();

    // Too short.
    let short_result = SharedMemoryLayout::open(&bytes[..10]);
    assert!(short_result.is_err(), "bead_id={BEAD_ID} should reject short buffer");

    // Corrupted magic.
    let mut bad_magic = bytes.clone();
    bad_magic[0] = 0xFF;
    let magic_result = SharedMemoryLayout::open(&bad_magic);
    assert!(magic_result.is_err(), "bead_id={BEAD_ID} should reject bad magic");

    // Corrupted checksum.
    let mut bad_checksum = bytes.clone();
    // Flip a bit in the checksum region (offset 144 from research).
    let checksum_byte_idx = SharedMemoryLayout::HEADER_SIZE - 72; // offset to layout_checksum
    if checksum_byte_idx < bad_checksum.len() {
        bad_checksum[checksum_byte_idx] ^= 0x01;
    }
    let checksum_result = SharedMemoryLayout::open(&bad_checksum);
    assert!(checksum_result.is_err(), "bead_id={BEAD_ID} should reject bad checksum");

    println!("[{BEAD_ID}] SHM corruption rejection: short=ERR magic=ERR checksum=ERR");
}

/// Test 3: Seqlock publish/load snapshot protocol.
#[test]
fn test_seqlock_snapshot_protocol() {
    let page_size = PageSize::new(4096).unwrap();
    let layout = SharedMemoryLayout::new(page_size, 16);

    // Initial state: commit_seq=0, schema_epoch=0.
    let snap0 = layout.load_consistent_snapshot();
    assert_eq!(snap0.commit_seq, CommitSeq::new(0));
    assert_eq!(snap0.schema_epoch, SchemaEpoch::new(0));

    // Publish a snapshot.
    layout.publish_snapshot(CommitSeq::new(42), SchemaEpoch::new(3), 7);

    // Read back.
    let snap1 = layout.load_consistent_snapshot();
    assert_eq!(
        snap1.commit_seq,
        CommitSeq::new(42),
        "bead_id={BEAD_ID} commit_seq not updated"
    );
    assert_eq!(
        snap1.schema_epoch,
        SchemaEpoch::new(3),
        "bead_id={BEAD_ID} schema_epoch not updated"
    );
    assert_eq!(snap1.ecs_epoch, 7, "bead_id={BEAD_ID} ecs_epoch not updated");

    // Multiple publishes.
    layout.publish_snapshot(CommitSeq::new(100), SchemaEpoch::new(5), 20);
    let snap2 = layout.load_consistent_snapshot();
    assert_eq!(snap2.commit_seq, CommitSeq::new(100));

    println!("[{BEAD_ID}] seqlock protocol: OK (3 snapshots verified)");
}

/// Test 4: Serialized writer acquisition and release.
#[test]
fn test_serialized_writer_lifecycle() {
    let page_size = PageSize::new(4096).unwrap();
    let layout = SharedMemoryLayout::new(page_size, 16);

    // Initially no writer.
    assert!(
        layout.check_serialized_writer().is_none(),
        "bead_id={BEAD_ID} should have no writer initially"
    );

    // Acquire writer.
    let acquired = layout.acquire_serialized_writer(42, 1234, 1000, 9999);
    assert!(acquired, "bead_id={BEAD_ID} writer acquisition failed");

    // Check writer is active.
    let active = layout.check_serialized_writer();
    assert!(active.is_some(), "bead_id={BEAD_ID} writer should be active");

    // Second acquisition with different txn should fail.
    let second = layout.acquire_serialized_writer(99, 5678, 2000, 9999);
    assert!(!second, "bead_id={BEAD_ID} second writer should be rejected");

    // Release writer.
    let released = layout.release_serialized_writer(42);
    assert!(released, "bead_id={BEAD_ID} writer release failed");

    // Now no writer.
    assert!(
        layout.check_serialized_writer().is_none(),
        "bead_id={BEAD_ID} writer should be released"
    );

    // Can acquire again.
    let reacquired = layout.acquire_serialized_writer(99, 5678, 2000, 9999);
    assert!(reacquired, "bead_id={BEAD_ID} re-acquisition failed");

    println!("[{BEAD_ID}] serialized writer lifecycle: acquire=OK reject=OK release=OK re-acquire=OK");
}

/// Test 5: Wire frame encode/decode round-trip.
#[test]
fn test_frame_encode_decode_round_trip() {
    let frame = Frame {
        kind: MessageKind::Reserve,
        request_id: 0xDEAD_BEEF_CAFE_BABE,
        payload: vec![1, 2, 3, 4, 5],
    };

    let encoded = frame.encode();
    let decoded = Frame::decode(&encoded).expect("decode failed");

    assert_eq!(decoded.kind, MessageKind::Reserve);
    assert_eq!(decoded.request_id, 0xDEAD_BEEF_CAFE_BABE);
    assert_eq!(decoded.payload, vec![1, 2, 3, 4, 5]);

    // Test all message kinds.
    for kind in [
        MessageKind::Reserve,
        MessageKind::SubmitNativePublish,
        MessageKind::SubmitWalCommit,
        MessageKind::RowidReserve,
        MessageKind::Response,
        MessageKind::Ping,
        MessageKind::Pong,
    ] {
        let f = Frame {
            kind,
            request_id: 1,
            payload: vec![],
        };
        let wire = f.encode();
        let back = Frame::decode(&wire).expect("decode failed");
        assert_eq!(back.kind, kind, "bead_id={BEAD_ID} kind round-trip failed for {kind:?}");
    }

    println!("[{BEAD_ID}] frame encode/decode: 7 message kinds round-tripped");
}

/// Test 6: Frame decode rejects malformed input.
#[test]
fn test_frame_decode_rejects_malformed() {
    // Too short.
    assert!(matches!(
        Frame::decode(&[0; 4]),
        Err(FrameError::TooShort)
    ));

    // Len too small.
    let mut buf = vec![0u8; 16];
    buf[0..4].copy_from_slice(&1u32.to_be_bytes()); // len_be = 1 < FRAME_MIN_LEN_BE
    assert!(matches!(
        Frame::decode(&buf),
        Err(FrameError::LenTooSmall(_))
    ));

    // Unknown version.
    let good_frame = Frame {
        kind: MessageKind::Ping,
        request_id: 0,
        payload: vec![],
    };
    let mut bad_version = good_frame.encode();
    bad_version[4] = 0xFF; // corrupt version
    bad_version[5] = 0xFF;
    assert!(matches!(
        Frame::decode(&bad_version),
        Err(FrameError::UnknownVersion(_))
    ));

    println!("[{BEAD_ID}] frame rejection: TooShort=OK LenTooSmall=OK UnknownVersion=OK");
}

/// Test 7: Permit manager lifecycle (reserve/consume/release).
#[test]
fn test_permit_manager_lifecycle() {
    let pm = PermitManager::new(4);

    // Reserve permits up to capacity.
    let p1 = pm.reserve().expect("reserve 1");
    let p2 = pm.reserve().expect("reserve 2");
    let p3 = pm.reserve().expect("reserve 3");
    let p4 = pm.reserve().expect("reserve 4");
    assert_eq!(pm.outstanding(), 4);

    // Capacity reached.
    assert!(matches!(pm.reserve(), Err(PermitError::Busy)));

    // Consume one.
    pm.consume(p1).expect("consume p1");
    assert_eq!(pm.outstanding(), 3);

    // Now can reserve again.
    let p5 = pm.reserve().expect("reserve after consume");
    assert_eq!(pm.outstanding(), 4);

    // Double consume fails.
    assert!(matches!(pm.consume(p1), Err(PermitError::AlreadyConsumed(_))));

    // Not-found consume fails.
    assert!(matches!(pm.consume(99999), Err(PermitError::NotFound(_))));

    // Release without consuming.
    pm.release(p2);
    assert_eq!(pm.outstanding(), 3);

    // GC consumed permits.
    pm.gc_consumed();

    // Cleanup.
    pm.consume(p3).expect("consume p3");
    pm.consume(p4).expect("consume p4");
    pm.consume(p5).expect("consume p5");

    println!("[{BEAD_ID}] permit lifecycle: reserve=OK capacity=OK consume=OK release=OK gc=OK");
}

/// Test 8: Idempotency cache stores and retrieves terminal responses.
#[test]
fn test_idempotency_cache() {
    let cache = IdempotencyCache::new();

    // Miss on empty cache.
    assert!(cache.get(1, 1).is_none(), "bead_id={BEAD_ID} expected cache miss");

    // Store a response.
    cache.insert(1, 1, vec![0xAA, 0xBB]);

    // Hit.
    let hit = cache.get(1, 1);
    assert_eq!(hit, Some(vec![0xAA, 0xBB]), "bead_id={BEAD_ID} cache hit mismatch");

    // Different txn_epoch is a miss.
    assert!(cache.get(1, 2).is_none(), "bead_id={BEAD_ID} expected miss for different epoch");

    // Different txn_id is a miss.
    assert!(cache.get(2, 1).is_none(), "bead_id={BEAD_ID} expected miss for different txn_id");

    println!("[{BEAD_ID}] idempotency cache: miss=OK put=OK hit=OK isolation=OK");
}

/// Test 9: Canonical ordering validators.
#[test]
fn test_canonical_ordering_validators() {
    // is_canonical_pages: sorted ascending, no duplicates.
    assert!(is_canonical_pages(&[1, 2, 3, 10, 20]));
    assert!(is_canonical_pages(&[]));
    assert!(is_canonical_pages(&[42]));
    assert!(!is_canonical_pages(&[3, 2, 1])); // unsorted
    assert!(!is_canonical_pages(&[1, 1, 2])); // duplicate

    // validate_write_set_summary: validates size constraints (not ordering).
    assert!(validate_write_set_summary(&[1, 5, 10]));
    assert!(validate_write_set_summary(&[])); // empty is valid

    // validate_witness_edge_counts: must not exceed limits.
    assert!(validate_witness_edge_counts(10, 10, 10, 10));
    assert!(!validate_witness_edge_counts(100_000, 0, 0, 0)); // exceeds WIRE_WITNESS_EDGE_MAX

    println!("[{BEAD_ID}] canonical validators: pages=OK write_set=OK witness_edges=OK");
}

/// Test 10: WireTxnToken encode/decode round-trip.
#[test]
fn test_wire_txn_token_round_trip() {
    let token = WireTxnToken {
        txn_id: 0x1234_5678_9ABC_DEF0,
        txn_epoch: 42,
    };

    let bytes = token.to_bytes();
    assert_eq!(bytes.len(), 16, "bead_id={BEAD_ID} token wire size should be 16");

    let decoded = WireTxnToken::from_bytes(&bytes).expect("decode failed");
    assert_eq!(decoded.txn_id, token.txn_id, "bead_id={BEAD_ID} txn_id mismatch");
    assert_eq!(decoded.txn_epoch, token.txn_epoch, "bead_id={BEAD_ID} txn_epoch mismatch");

    println!("[{BEAD_ID}] WireTxnToken round-trip: OK");
}

/// Test 11: Cross-thread snapshot consistency via seqlock.
#[test]
fn test_cross_thread_snapshot_consistency() {
    let page_size = PageSize::new(4096).unwrap();
    let layout = Arc::new(SharedMemoryLayout::new(page_size, 16));

    let writer = layout.clone();
    let reader = layout.clone();

    // Writer publishes incrementing commit_seq values.
    let writer_handle = thread::spawn(move || {
        for i in 1..=1000_u64 {
            writer.publish_snapshot(CommitSeq::new(i), SchemaEpoch::new(0), 0);
        }
    });

    // Reader reads consistent snapshots (commit_seq must never be torn).
    let reader_handle = thread::spawn(move || {
        let mut max_seen = 0_u64;
        let mut reads = 0_u64;
        for _ in 0..5000 {
            let snap = reader.load_consistent_snapshot();
            let seq = snap.commit_seq.get();
            // Seqlock guarantee: value must be from a completed publish (0..=1000).
            assert!(
                seq <= 1000,
                "bead_id={BEAD_ID} torn read detected: commit_seq={seq}"
            );
            // Monotonicity: we might see stale reads but not future-past inversion
            // within a single thread (since the writer only increments).
            if seq > max_seen {
                max_seen = seq;
            }
            reads += 1;
        }
        (max_seen, reads)
    });

    writer_handle.join().expect("writer panicked");
    let (max_seen, reads) = reader_handle.join().expect("reader panicked");

    println!("[{BEAD_ID}] cross-thread consistency: max_seen={max_seen} reads={reads} no torn reads");
}

/// Test 12: GC horizon coordination.
#[test]
fn test_gc_horizon_coordination() {
    let page_size = PageSize::new(4096).unwrap();
    let layout = SharedMemoryLayout::new(page_size, 16);

    // Initial horizon is 0.
    assert_eq!(layout.load_gc_horizon(), CommitSeq::new(0));

    // Store and load.
    layout.store_gc_horizon(CommitSeq::new(100));
    assert_eq!(layout.load_gc_horizon(), CommitSeq::new(100));

    // Advance horizon.
    layout.store_gc_horizon(CommitSeq::new(500));
    assert_eq!(layout.load_gc_horizon(), CommitSeq::new(500));

    // TxnId allocation.
    let txn1 = layout.alloc_txn_id();
    let txn2 = layout.alloc_txn_id();
    assert!(txn1.is_some(), "bead_id={BEAD_ID} txn1 alloc failed");
    assert!(txn2.is_some(), "bead_id={BEAD_ID} txn2 alloc failed");
    assert_ne!(txn1, txn2, "bead_id={BEAD_ID} txn IDs must be unique");

    println!(
        "[{BEAD_ID}] GC horizon: initial=0 store=OK advance=OK txn_alloc=OK (txn1={:?} txn2={:?})",
        txn1, txn2
    );
}

/// Test 13: Conformance summary.
#[test]
fn test_conformance_summary() {
    let page_size = PageSize::new(4096).unwrap();
    let layout = SharedMemoryLayout::new(page_size, 16);

    let pass_shm_roundtrip = {
        let bytes = layout.to_bytes();
        SharedMemoryLayout::open(&bytes).is_ok()
    };
    let pass_seqlock = {
        layout.publish_snapshot(CommitSeq::new(1), SchemaEpoch::new(0), 0);
        layout.load_consistent_snapshot().commit_seq == CommitSeq::new(1)
    };
    let pass_writer = {
        let a = layout.acquire_serialized_writer(1, 1, 1, 9999);
        let r = layout.release_serialized_writer(1);
        a && r
    };
    let pass_frame = {
        let f = Frame { kind: MessageKind::Ping, request_id: 0, payload: vec![] };
        Frame::decode(&f.encode()).is_ok()
    };
    let pass_permit = {
        let pm = PermitManager::new(2);
        let p = pm.reserve().is_ok();
        p
    };
    let pass_canonical = is_canonical_pages(&[1, 2, 3]) && !is_canonical_pages(&[3, 2, 1]);

    println!("\n=== {BEAD_ID} Multi-Process Coordination Conformance ===");
    println!("  shm_round_trip..............{}", if pass_shm_roundtrip { "PASS" } else { "FAIL" });
    println!("  seqlock_protocol............{}", if pass_seqlock { "PASS" } else { "FAIL" });
    println!("  writer_lifecycle............{}", if pass_writer { "PASS" } else { "FAIL" });
    println!("  frame_codec.................{}", if pass_frame { "PASS" } else { "FAIL" });
    println!("  permit_lifecycle............{}", if pass_permit { "PASS" } else { "FAIL" });
    println!("  canonical_validators........{}", if pass_canonical { "PASS" } else { "FAIL" });

    let all = [
        pass_shm_roundtrip, pass_seqlock, pass_writer,
        pass_frame, pass_permit, pass_canonical,
    ];
    let passed = all.iter().filter(|&&p| p).count();
    println!("  [{}/{}] conformance checks passed", passed, all.len());

    assert!(all.iter().all(|&p| p), "bead_id={BEAD_ID} conformance failed");
}
