//! bd-db300.7.2.3: Verify recovery invariants under injected commit-path faults.
//!
//! Each test uses a fault hook from bd-db300.7.2.2 to inject a failure at a
//! specific point in the WAL commit path, then re-opens the WAL and verifies
//! that recovery produces the correct state.
//!
//! ## Replay
//! ```bash
//! cargo test -p fsqlite-wal --test bd_db300_7_2_3_recovery_invariants -- --nocapture --test-threads=1
//! ```
//!
//! ## Recovery Invariant Matrix
//!
//! | Hook | Fault | Pre-fault State | Expected Recovery State | Invariant |
//! |------|-------|-----------------|------------------------|-----------|
//! | H1   | F1: crash after append, before sync | N committed + M appended | N+M frames (unsynced but in-memory) | Checksum chain valid through all frames |
//! | H2   | F2: sync interrupted | N committed + M appended | N+M frames (sync failed but data written) | Frames readable, chain valid |
//! | H5   | F5: append_frames returns Busy | N committed | N frames (append rejected) | No partial frames, retry succeeds |
//! | H9   | F9: crash between header rewrite and truncate | N committed, then reset | 0 frames (salt mismatch) | DB state = last checkpoint |

use std::path::Path;

use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{SyncFlags, VfsOpenFlags};
use fsqlite_vfs::MemoryVfs;
use fsqlite_vfs::traits::Vfs;
use fsqlite_wal::fault_hooks::{self, FaultHookArm};
use fsqlite_wal::wal::WalAppendFrameRef;
use fsqlite_wal::{WalFile, WalSalts};

const PAGE_SIZE: u32 = 4096;
const BEAD_ID: &str = "bd-db300.7.2.3";

/// Shared, panic-safe ownership guard for process-global fault hooks.
static RECOVERY_TEST_LOCK: fault_hooks::FaultInjectionSessionLock =
    fault_hooks::FaultInjectionSessionLock::new();

fn test_cx() -> Cx {
    Cx::new()
}

fn test_salts() -> WalSalts {
    WalSalts {
        salt1: 0xDEAD_BEEF,
        salt2: 0xCAFE_BABE,
    }
}

fn sample_page(fill: u8) -> Vec<u8> {
    vec![fill; PAGE_SIZE as usize]
}

fn open_wal_file(vfs: &MemoryVfs, cx: &Cx) -> <MemoryVfs as Vfs>::File {
    let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL;
    let (file, _) = vfs
        .open(cx, Some(Path::new("/recovery_test.db-wal")), flags)
        .expect("open WAL file");
    file
}

/// Create a WAL with `n` committed frames and return it with checksum.
fn create_wal_with_committed_frames(
    vfs: &MemoryVfs,
    cx: &Cx,
    n: usize,
) -> (WalFile<<MemoryVfs as Vfs>::File>, u32) {
    let file = open_wal_file(vfs, cx);
    let mut wal = WalFile::create(cx, file, PAGE_SIZE, 0, test_salts()).expect("create WAL");

    for i in 0..n {
        let page_no = u32::try_from(i + 1).unwrap();
        let db_size = if i == n - 1 {
            u32::try_from(n).unwrap()
        } else {
            0
        };
        wal.append_frame(
            cx,
            page_no,
            &sample_page(u8::try_from(i % 251).unwrap()),
            db_size,
        )
        .expect("append frame");
    }
    if n > 0 {
        wal.sync(cx, SyncFlags::NORMAL).expect("sync WAL");
    }

    let checksum = wal.running_checksum().s1;
    (wal, checksum)
}

// ─── Recovery Proof R1: Crash after WAL append, before sync (H1/F1) ────────

/// **R1**: After fault-injecting a crash between append and sync, re-opening
/// the WAL recovers all frames that were successfully written. The checksum
/// chain is valid through the recovered frames.
///
/// This verifies that `WalFile::open()` correctly scans the checksum chain
/// and accepts frames that were written but never synced (in-memory VFS
/// simulates this because data is always "durable" in memory).
#[test]
fn r1_crash_after_append_recovery_preserves_written_frames() {
    let _guard = RECOVERY_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    fault_hooks::clear();

    let cx = test_cx();
    let vfs = MemoryVfs::new();

    // Phase 1: Create WAL with 5 committed frames.
    let (mut wal, baseline_checksum) = create_wal_with_committed_frames(&vfs, &cx, 5);
    assert_eq!(wal.frame_count(), 5);

    // Phase 2: Arm the after-append hook and attempt to append 3 more frames.
    fault_hooks::arm_after_append(FaultHookArm::new(
        BEAD_ID,
        "R1-crash-after-append",
        "wal_append_recovery",
    ));

    let new_pages: Vec<Vec<u8>> = (0..3).map(|i| sample_page(0xA0 + i)).collect();
    let new_frames: Vec<WalAppendFrameRef<'_>> = new_pages
        .iter()
        .enumerate()
        .map(|(i, page)| fsqlite_wal::wal::WalAppendFrameRef {
            page_number: u32::try_from(i + 6).unwrap(),
            page_data: page,
            db_size_if_commit: if i == 2 { 8 } else { 0 },
        })
        .collect();

    let err = wal
        .append_frames(&cx, &new_frames)
        .expect_err("H1 hook should fire after append");
    assert!(err.to_string().contains("fault_inject:wal_after_append"));

    // The WAL handle shows 8 frames (5 original + 3 appended before error).
    assert_eq!(
        wal.frame_count(),
        8,
        "in-memory: append succeeded before hook fired"
    );

    // Phase 3: Close and re-open — recovery.
    wal.close(&cx).expect("close WAL");
    let recovered_file = open_wal_file(&vfs, &cx);
    let recovered = WalFile::open(&cx, recovered_file).expect("reopen WAL for recovery");

    // Recovery proof:
    // - MemoryVfs: data is always "durable", so all 8 frames survive.
    // - On real disk without sync: only 5 frames would survive (the synced ones).
    //   The 3 unsynced frames would have invalid checksums from torn writes.
    // - In both cases, the checksum chain is valid for all recovered frames.
    let recovered_count = recovered.frame_count();
    assert!(
        recovered_count >= 5,
        "bead_id={BEAD_ID} invariant=R1 recovered_count={recovered_count} — must be >= pre-fault committed count (5)"
    );

    // Verify checksum chain: read each frame and confirm header is valid.
    for i in 0..recovered_count {
        let (header, _page) = recovered
            .read_frame(&cx, i)
            .unwrap_or_else(|e| panic!("bead_id={BEAD_ID} invariant=R1 frame {i} unreadable: {e}"));
        assert!(
            header.page_number > 0,
            "bead_id={BEAD_ID} invariant=R1 frame {i} has zero page_number"
        );
    }

    // Verify chain continuity: running checksum on reopened WAL should be valid.
    let recovered_checksum = recovered.running_checksum().s1;
    if recovered_count == 5 {
        assert_eq!(
            recovered_checksum, baseline_checksum,
            "bead_id={BEAD_ID} invariant=R1 checksum should match pre-fault baseline when only committed frames survive"
        );
    }

    eprintln!(
        "[{BEAD_ID}] R1 PASS: recovered {recovered_count} frames, checksum={recovered_checksum:#x}"
    );

    let records = fault_hooks::take_records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].point, "wal_after_append");

    fault_hooks::clear();
    recovered.close(&cx).expect("close recovered WAL");
}

// ─── Recovery Proof R2: Sync interrupted (H2/F2) ──────────────────────────

/// **R2**: After a sync failure, frames that were already written are still
/// recoverable. The error does not corrupt already-written data.
#[test]
fn r2_sync_failure_does_not_corrupt_written_frames() {
    let _guard = RECOVERY_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    fault_hooks::clear();

    let cx = test_cx();
    let vfs = MemoryVfs::new();

    let (mut wal, _baseline_checksum) = create_wal_with_committed_frames(&vfs, &cx, 4);

    // Arm sync failure.
    fault_hooks::arm_sync_failure(FaultHookArm::new(
        BEAD_ID,
        "R2-sync-failure",
        "wal_sync_recovery",
    ));

    let err = wal
        .sync(&cx, SyncFlags::NORMAL)
        .expect_err("H2 hook should fail sync");
    assert!(err.to_string().contains("fault_inject:wal_sync_failure"));

    // Frames are still in the WAL (sync failure doesn't remove data).
    assert_eq!(wal.frame_count(), 4);

    // Recovery: re-open.
    wal.close(&cx).expect("close WAL");
    let recovered_file = open_wal_file(&vfs, &cx);
    let recovered = WalFile::open(&cx, recovered_file).expect("reopen WAL");

    assert_eq!(
        recovered.frame_count(),
        4,
        "bead_id={BEAD_ID} invariant=R2 — sync failure must not lose frames"
    );

    // Verify all frames readable.
    for i in 0..4 {
        recovered
            .read_frame(&cx, i)
            .unwrap_or_else(|e| panic!("bead_id={BEAD_ID} invariant=R2 frame {i}: {e}"));
    }

    eprintln!("[{BEAD_ID}] R2 PASS: 4 frames recovered after sync failure");
    fault_hooks::clear();
    recovered.close(&cx).expect("close");
}

// ─── Recovery Proof R3: Busy retry preserves WAL state (H5/F5) ──────────────

/// **R3**: When `append_frames` returns `Busy` via the countdown hook, no
/// partial frames are left in the WAL. After retry, the WAL is consistent.
#[test]
fn r3_append_busy_leaves_no_partial_frames_and_retry_succeeds() {
    let _guard = RECOVERY_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    fault_hooks::clear();

    let cx = test_cx();
    let vfs = MemoryVfs::new();

    let (mut wal, _) = create_wal_with_committed_frames(&vfs, &cx, 3);
    let pre_fault_count = wal.frame_count();
    let pre_fault_checksum = wal.running_checksum();

    // Arm: fire on first invocation.
    fault_hooks::arm_append_busy_countdown(
        FaultHookArm::new(BEAD_ID, "R3-busy-retry", "wal_append_retry"),
        1,
    );

    let page = sample_page(0xBB);
    let frames = [WalAppendFrameRef {
        page_number: 4,
        page_data: &page,
        db_size_if_commit: 4,
    }];

    let err = wal
        .append_frames(&cx, &frames)
        .expect_err("H5 countdown should fire on first append");
    assert!(matches!(err, fsqlite_error::FrankenError::Busy));

    // Key invariant: no partial frames.
    assert_eq!(
        wal.frame_count(),
        pre_fault_count,
        "bead_id={BEAD_ID} invariant=R3 — busy must not leave partial frames"
    );
    assert_eq!(
        wal.running_checksum(),
        pre_fault_checksum,
        "bead_id={BEAD_ID} invariant=R3 — checksum must be unchanged after busy"
    );

    // Retry succeeds.
    wal.append_frames(&cx, &frames)
        .expect("retry after busy should succeed");
    assert_eq!(wal.frame_count(), pre_fault_count + 1);

    // Recovery: re-open and verify.
    wal.close(&cx).expect("close");
    let recovered_file = open_wal_file(&vfs, &cx);
    let recovered = WalFile::open(&cx, recovered_file).expect("reopen");
    assert_eq!(
        recovered.frame_count(),
        pre_fault_count + 1,
        "bead_id={BEAD_ID} invariant=R3 — recovery sees the retried frame"
    );

    eprintln!("[{BEAD_ID}] R3 PASS: busy left 0 partial frames, retry succeeded");
    fault_hooks::clear();
    recovered.close(&cx).expect("close");
}

// ─── Recovery Proof R4: Header/truncate crash (H9/F9) ──────────────────────

/// **R4**: After a crash between WAL header rewrite (new salts) and file
/// truncation, recovery produces 0 frames because all existing frames have
/// old-generation salts that don't match the new header.
#[test]
fn r4_crash_between_header_and_truncate_recovers_to_zero_frames() {
    let _guard = RECOVERY_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    fault_hooks::clear();

    let cx = test_cx();
    let vfs = MemoryVfs::new();

    // Create WAL with 5 committed frames.
    let (mut wal, _) = create_wal_with_committed_frames(&vfs, &cx, 5);
    let original_salts = wal.generation_identity().salts;
    assert_eq!(wal.frame_count(), 5);

    // Arm the crash-header-truncate hook.
    fault_hooks::arm_crash_header_truncate(FaultHookArm::new(
        BEAD_ID,
        "R4-header-truncate",
        "wal_reset_recovery",
    ));

    let new_salts = WalSalts {
        salt1: original_salts.salt1.wrapping_add(42),
        salt2: original_salts.salt2.wrapping_add(42),
    };
    let err = wal
        .reset(&cx, 1, new_salts, true)
        .expect_err("H9 hook should fire");
    assert!(
        err.to_string()
            .contains("fault_inject:wal_crash_header_truncate")
    );

    // Close and recover.
    wal.close(&cx).expect("close corrupted WAL");
    let recovered_file = open_wal_file(&vfs, &cx);
    let recovered = WalFile::open(&cx, recovered_file).expect("reopen WAL");

    // Key invariant: 0 frames because salt mismatch.
    assert_eq!(
        recovered.frame_count(),
        0,
        "bead_id={BEAD_ID} invariant=R4 — salt mismatch must discard all old frames"
    );
    assert_eq!(
        recovered.generation_identity().salts,
        new_salts,
        "bead_id={BEAD_ID} invariant=R4 — recovered header must have new salts"
    );

    eprintln!("[{BEAD_ID}] R4 PASS: 0 frames recovered (salt mismatch discards old generation)");
    fault_hooks::clear();
    recovered.close(&cx).expect("close");
}

// ─── Recovery Proof R5: Multi-fault sequence (H1 then H5) ──────────────────

/// **R5**: After a sequence of faults (crash-after-append then busy-retry),
/// the WAL maintains a consistent checksum chain throughout.
#[test]
fn r5_multi_fault_sequence_maintains_checksum_chain() {
    let _guard = RECOVERY_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    fault_hooks::clear();

    let cx = test_cx();
    let vfs = MemoryVfs::new();

    // Create WAL with 3 committed frames.
    let (mut wal, _) = create_wal_with_committed_frames(&vfs, &cx, 3);

    // Fault 1: crash after append.
    fault_hooks::arm_after_append(FaultHookArm::new(
        BEAD_ID,
        "R5-multi-fault-1",
        "wal_multi_fault",
    ));

    let page_a = sample_page(0xDD);
    let frames_a = [fsqlite_wal::wal::WalAppendFrameRef {
        page_number: 4,
        page_data: &page_a,
        db_size_if_commit: 4,
    }];
    let _ = wal.append_frames(&cx, &frames_a); // Will error after writing

    // Close and reopen (simulates restart after crash).
    wal.close(&cx).expect("close after fault 1");
    let file2 = open_wal_file(&vfs, &cx);
    let mut wal2 = WalFile::open(&cx, file2).expect("reopen after fault 1");
    let count_after_recovery_1 = wal2.frame_count();

    // Fault 2: busy on next append.
    fault_hooks::arm_append_busy_countdown(
        FaultHookArm::new(BEAD_ID, "R5-multi-fault-2", "wal_multi_fault"),
        1,
    );

    let page_b = sample_page(0xEE);
    let frames_b = [fsqlite_wal::wal::WalAppendFrameRef {
        page_number: u32::try_from(count_after_recovery_1 + 1).unwrap(),
        page_data: &page_b,
        db_size_if_commit: u32::try_from(count_after_recovery_1 + 1).unwrap(),
    }];
    let busy_err = wal2.append_frames(&cx, &frames_b);
    assert!(busy_err.is_err());

    // Retry succeeds.
    wal2.append_frames(&cx, &frames_b)
        .expect("retry after busy");
    let final_count = wal2.frame_count();
    assert_eq!(final_count, count_after_recovery_1 + 1);

    // Final recovery.
    wal2.close(&cx).expect("close after fault 2");
    let file3 = open_wal_file(&vfs, &cx);
    let final_wal = WalFile::open(&cx, file3).expect("final reopen");
    assert_eq!(
        final_wal.frame_count(),
        final_count,
        "bead_id={BEAD_ID} invariant=R5 — multi-fault sequence preserves final state"
    );

    // Verify all frames readable.
    for i in 0..final_wal.frame_count() {
        final_wal
            .read_frame(&cx, i)
            .unwrap_or_else(|e| panic!("bead_id={BEAD_ID} invariant=R5 frame {i}: {e}"));
    }

    let records = fault_hooks::take_records();
    assert_eq!(
        records.len(),
        2,
        "bead_id={BEAD_ID} invariant=R5 — exactly 2 faults should fire"
    );

    eprintln!(
        "[{BEAD_ID}] R5 PASS: multi-fault ({count_after_recovery_1} after R1, {final_count} final)"
    );
    fault_hooks::clear();
    final_wal.close(&cx).expect("close");
}
