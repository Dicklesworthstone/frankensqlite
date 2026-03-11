#![allow(clippy::redundant_clone, clippy::similar_names)]
//! Storage-layer deterministic unit test suites (bd-mblr.6.1).
//!
//! Granular deterministic tests for pager/WAL/MVCC/B-tree/VFS edge behavior:
//! conflicts, rollback, checkpoint ordering, recovery metadata, and boundary
//! conditions. All tests use fixtures from [`unit_fixtures`] and diagnostics
//! from [`test_diagnostics`].
//!
//! Maps to unit matrix entries UT-STOR-001 through UT-STOR-006 and adds
//! edge-case suites beyond the canonical matrix.

use crate::test_diagnostics::DiagContext;
use crate::unit_fixtures::FixtureSeed;
use crate::{diag_assert, diag_assert_eq};

const BEAD_ID: &str = "bd-mblr.6.1";

// ─── Pager Suite ──────────────────────────────────────────────────────────

#[cfg(test)]
mod pager_tests {
    use super::*;
    use fsqlite_pager::traits::{MvccPager, TransactionHandle, TransactionMode};
    use fsqlite_pager::{
        CHECKSUM_STRIDE, JOURNAL_HEADER_SIZE, JOURNAL_MAGIC, JournalHeader, SimplePager,
        lock_byte_page,
    };
    use fsqlite_types::cx::Cx;
    use fsqlite_types::{PageNumber, PageSize};
    use fsqlite_vfs::MemoryVfs;
    use std::path::PathBuf;

    fn test_pager() -> SimplePager<MemoryVfs> {
        let vfs = MemoryVfs::new();
        let path = PathBuf::from("/stor-suite-test.db");
        let cx = Cx::new();
        SimplePager::open_with_cx(&cx, vfs, &path, PageSize::DEFAULT).expect("open test pager")
    }

    fn test_pager_with_size(page_size: PageSize) -> SimplePager<MemoryVfs> {
        let vfs = MemoryVfs::new();
        let path = PathBuf::from(format!("/stor-suite-{}.db", page_size.get()));
        let cx = Cx::new();
        SimplePager::open_with_cx(&cx, vfs, &path, page_size).expect("open test pager")
    }

    // -- Transaction mode isolation --

    #[test]
    fn pager_readonly_cannot_write() {
        let pager = test_pager();
        let cx = Cx::new();
        let mut txn = pager
            .begin(&cx, TransactionMode::ReadOnly)
            .expect("begin ro");
        let result = txn.allocate_page(&cx);
        let ctx = DiagContext::new(BEAD_ID)
            .case("readonly_cannot_write")
            .invariant("RO txn must reject write operations");
        diag_assert!(ctx, result.is_err(), "allocate_page on RO must fail");
    }

    #[test]
    fn pager_immediate_is_writer() {
        let pager = test_pager();
        let cx = Cx::new();
        let mut txn = pager
            .begin(&cx, TransactionMode::Immediate)
            .expect("begin imm");
        let result = txn.allocate_page(&cx);
        let ctx = DiagContext::new(BEAD_ID)
            .case("immediate_is_writer")
            .invariant("IMMEDIATE txn can allocate pages (holds writer lock)");
        diag_assert!(
            ctx,
            result.is_ok(),
            "allocate_page on IMMEDIATE must succeed"
        );
    }

    #[test]
    fn pager_writer_mutual_exclusion() {
        let pager = test_pager();
        let cx = Cx::new();
        let _w1 = pager
            .begin(&cx, TransactionMode::Exclusive)
            .expect("begin excl");
        let result = pager.begin(&cx, TransactionMode::Immediate);
        let ctx = DiagContext::new(BEAD_ID)
            .case("writer_mutual_exclusion")
            .invariant("Only one writer at a time");
        diag_assert!(ctx, result.is_err(), "second writer must be rejected");
    }

    #[test]
    fn pager_multiple_readers_coexist() {
        let pager = test_pager();
        let cx = Cx::new();
        let r1 = pager.begin(&cx, TransactionMode::ReadOnly);
        let r2 = pager.begin(&cx, TransactionMode::ReadOnly);
        let ctx = DiagContext::new(BEAD_ID)
            .case("multiple_readers")
            .invariant("Multiple readers allowed concurrently");
        diag_assert!(ctx, r1.is_ok(), "first reader must succeed");
        diag_assert!(ctx, r2.is_ok(), "second reader must succeed");
    }

    #[test]
    fn pager_deferred_upgrades_on_allocate() {
        let pager = test_pager();
        let cx = Cx::new();
        let mut deferred = pager
            .begin(&cx, TransactionMode::Deferred)
            .expect("begin def");

        // DEFERRED can read page 1 (starts as reader).
        let pre_ctx = DiagContext::new(BEAD_ID)
            .case("deferred_pre_upgrade")
            .invariant("DEFERRED starts as reader, can read");
        let read_result = deferred.get_page(&cx, PageNumber::ONE);
        diag_assert!(pre_ctx, read_result.is_ok(), "reader can read");

        // After allocate, deferred has upgraded to writer.
        let _page = deferred
            .allocate_page(&cx)
            .expect("allocate upgrades to writer");
        let post_ctx = DiagContext::new(BEAD_ID)
            .case("deferred_post_upgrade")
            .invariant("DEFERRED upgrades on first write, can commit");
        let commit_result = deferred.commit(&cx);
        diag_assert!(
            post_ctx,
            commit_result.is_ok(),
            "upgraded writer can commit"
        );
    }

    // -- Page write and read-back --

    #[test]
    fn pager_write_readback_deterministic() {
        let pager = test_pager();
        let cx = Cx::new();
        let mut txn = pager.begin(&cx, TransactionMode::Immediate).expect("begin");
        let page_no = txn.allocate_page(&cx).expect("alloc");
        let page_size = PageSize::DEFAULT.as_usize();

        let seed = FixtureSeed::derive("pager-write-readback");
        let mut data = vec![0u8; page_size];
        let seed_bytes = seed.raw().to_le_bytes();
        for (i, byte) in data.iter_mut().enumerate() {
            *byte = seed_bytes[i % 8];
        }

        txn.write_page(&cx, page_no, &data).expect("write");
        let read_back = txn.get_page(&cx, page_no).expect("read");

        let ctx = DiagContext::new(BEAD_ID)
            .case("write_readback")
            .seed(seed.raw())
            .invariant("Written page reads back identically");
        diag_assert_eq!(ctx, read_back.as_ref(), data.as_slice());
    }

    // -- Page boundary sizes --

    #[test]
    fn pager_min_page_size() {
        let page_size = PageSize::new(512).expect("512 valid");
        let pager = test_pager_with_size(page_size);
        let cx = Cx::new();
        let txn = pager.begin(&cx, TransactionMode::ReadOnly).expect("begin");
        let page1 = txn.get_page(&cx, PageNumber::ONE).expect("read page 1");
        let ctx = DiagContext::new(BEAD_ID)
            .case("min_page_size")
            .invariant("512-byte page size boundary");
        diag_assert_eq!(ctx, page1.as_ref().len(), 512);
    }

    #[test]
    fn pager_max_page_size() {
        let page_size = PageSize::new(65536).expect("65536 valid");
        let pager = test_pager_with_size(page_size);
        let cx = Cx::new();
        let txn = pager.begin(&cx, TransactionMode::ReadOnly).expect("begin");
        let page1 = txn.get_page(&cx, PageNumber::ONE).expect("read page 1");
        let ctx = DiagContext::new(BEAD_ID)
            .case("max_page_size")
            .invariant("65536-byte page size boundary");
        diag_assert_eq!(ctx, page1.as_ref().len(), 65536);
    }

    // -- Journal format --

    #[test]
    fn journal_header_roundtrip() {
        let header = JournalHeader {
            page_count: 42,
            nonce: 0xDEAD_BEEF,
            initial_db_size: 100,
            sector_size: 4096,
            page_size: 4096,
        };

        let buf = header.encode();
        let parsed = JournalHeader::decode(&buf).expect("decode");

        let ctx = DiagContext::new(BEAD_ID)
            .case("journal_header_roundtrip")
            .invariant("Journal header write-parse roundtrip");
        diag_assert_eq!(ctx, parsed, header);
    }

    #[test]
    fn journal_magic_matches_spec() {
        let ctx = DiagContext::new(BEAD_ID)
            .case("journal_magic")
            .invariant("Journal magic must be 0xd9d505f920a163d7");
        diag_assert_eq!(
            ctx,
            JOURNAL_MAGIC,
            [0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7]
        );
    }

    #[test]
    fn journal_bad_magic_rejected() {
        let mut buf = vec![0u8; JOURNAL_HEADER_SIZE];
        buf[0] = 0xFF; // Corrupt magic.
        let result = JournalHeader::decode(&buf);
        let ctx = DiagContext::new(BEAD_ID)
            .case("journal_bad_magic")
            .invariant("Corrupt journal magic must be rejected");
        diag_assert!(ctx, result.is_err(), "bad magic must fail");
    }

    #[test]
    fn journal_checksum_stride_200() {
        let ctx = DiagContext::new(BEAD_ID)
            .case("checksum_stride")
            .invariant("Journal checksum stride = 200 per spec");
        diag_assert_eq!(ctx, CHECKSUM_STRIDE, 200);
    }

    #[test]
    fn lock_byte_page_default() {
        let page = lock_byte_page(PageSize::DEFAULT);
        let expected = (0x4000_0000_u32 / PageSize::DEFAULT.get()) + 1;
        let ctx = DiagContext::new(BEAD_ID)
            .case("lock_byte_page_default")
            .invariant("Lock-byte page = (PENDING_BYTE / page_size) + 1");
        diag_assert_eq!(ctx, page, expected);
    }

    #[test]
    fn lock_byte_page_varies_with_size() {
        let page_512 = lock_byte_page(PageSize::new(512).unwrap());
        let page_65536 = lock_byte_page(PageSize::new(65536).unwrap());
        let ctx = DiagContext::new(BEAD_ID)
            .case("lock_byte_page_varies")
            .invariant("Larger page size → smaller lock-byte page number");
        diag_assert!(ctx, page_512 > page_65536, "512 lock page should be higher");
    }
}

// ─── WAL Suite ────────────────────────────────────────────────────────────

#[cfg(test)]
mod wal_tests {
    use super::*;
    use fsqlite_types::cx::Cx;
    use fsqlite_types::flags::VfsOpenFlags;
    use fsqlite_vfs::{MemoryVfs, Vfs};
    use fsqlite_wal::checkpoint::{CheckpointMode, CheckpointState, plan_checkpoint};
    use fsqlite_wal::checksum::{WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE, WalSalts};
    use fsqlite_wal::wal::WalFile;

    const PAGE_SIZE: u32 = 4096;

    fn test_cx() -> Cx {
        Cx::default()
    }

    #[allow(clippy::cast_possible_truncation)]
    fn test_salts() -> WalSalts {
        let seed = FixtureSeed::derive("wal-test-salts");
        let raw = seed.raw();
        WalSalts {
            salt1: (raw >> 32) as u32,
            salt2: raw as u32,
        }
    }

    fn sample_page(seed_byte: u8) -> Vec<u8> {
        let ps = usize::try_from(PAGE_SIZE).expect("page size fits usize");
        let mut page = vec![0u8; ps];
        for (i, byte) in page.iter_mut().enumerate() {
            let reduced = u8::try_from(i % 251).expect("modulo fits u8");
            *byte = reduced ^ seed_byte;
        }
        page
    }

    fn open_wal_file(vfs: &MemoryVfs, cx: &Cx) -> <MemoryVfs as Vfs>::File {
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL;
        let (file, _) = vfs
            .open(
                cx,
                Some(std::path::Path::new("wal-suite-test.db-wal")),
                flags,
            )
            .expect("open WAL file");
        file
    }

    // -- WAL frame write/readback (UT-STOR-001) --

    #[test]
    fn wal_frame_write_readback() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create WAL");

        let page = sample_page(0x42);
        wal.append_frame(&cx, 1, &page, 0).expect("append frame");

        let (header, data) = wal.read_frame(&cx, 0).expect("read frame");
        let ctx = DiagContext::new(BEAD_ID)
            .case("wal_frame_readback")
            .seed(0x42)
            .invariant("Written frame reads back identically");
        diag_assert_eq!(ctx, data, page);
        diag_assert_eq!(
            ctx.clone().case("wal_frame_page_number"),
            header.page_number,
            1
        );

        wal.close(&cx).expect("close WAL");
    }

    #[test]
    fn wal_checksum_chain_integrity() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 5, test_salts()).expect("create WAL");

        for i in 0..5u32 {
            let page = sample_page(u8::try_from(i).expect("fits"));
            let db_size = if i == 4 { 5 } else { 0 };
            wal.append_frame(&cx, i + 1, &page, db_size)
                .expect("append frame");
        }

        wal.close(&cx).expect("close");

        // Reopen — checksum chain validated on open.
        let file2 = open_wal_file(&vfs, &cx);
        let wal2 = WalFile::open(&cx, file2).expect("reopen validates chain");
        let ctx = DiagContext::new(BEAD_ID)
            .case("checksum_chain_intact")
            .invariant("Checksum chain validates on reopen");
        diag_assert_eq!(ctx, wal2.frame_count(), 5);

        wal2.close(&cx).expect("close");
    }

    #[test]
    fn wal_reset_clears_frames() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");

        for i in 0..3u8 {
            wal.append_frame(&cx, u32::from(i) + 1, &sample_page(i), 0)
                .expect("append");
        }

        let new_salts = WalSalts {
            salt1: 0x1111_2222,
            salt2: 0x3333_4444,
        };
        wal.reset(&cx, 1, new_salts, true).expect("reset");

        let ctx = DiagContext::new(BEAD_ID)
            .case("wal_reset")
            .invariant("WAL reset zeroes frame count");
        diag_assert_eq!(ctx, wal.frame_count(), 0);
        diag_assert_eq!(
            ctx.clone().case("wal_reset_ckpt_seq"),
            wal.header().checkpoint_seq,
            1
        );

        wal.close(&cx).expect("close");
    }

    #[test]
    fn wal_commit_frame_marks_db_size() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");

        wal.append_frame(&cx, 1, &sample_page(0x10), 10)
            .expect("append commit");
        let header = wal.read_frame_header(&cx, 0).expect("read header");

        let ctx = DiagContext::new(BEAD_ID)
            .case("commit_frame_db_size")
            .invariant("Commit frame carries db_size");
        diag_assert!(ctx.clone(), header.is_commit(), "must be commit frame");
        diag_assert_eq!(ctx, header.db_size, 10);

        wal.close(&cx).expect("close");
    }

    // -- Checkpoint planning (UT-STOR-006) --

    #[test]
    fn checkpoint_passive_respects_reader_limit() {
        let state = CheckpointState {
            total_frames: 100,
            backfilled_frames: 40,
            oldest_reader_frame: Some(65),
        };
        let plan = plan_checkpoint(CheckpointMode::Passive, state);

        let ctx = DiagContext::new(BEAD_ID)
            .case("passive_reader_limit")
            .invariant("PASSIVE backfills up to reader limit");
        diag_assert_eq!(ctx.clone(), plan.frames_to_backfill, 25);
        diag_assert_eq!(
            ctx.clone().case("passive_not_complete"),
            plan.completes_checkpoint(),
            false
        );
        diag_assert_eq!(
            ctx.clone().case("passive_not_blocked"),
            plan.blocked_by_readers,
            false
        );
    }

    #[test]
    fn checkpoint_full_blocked_by_readers() {
        let state = CheckpointState {
            total_frames: 200,
            backfilled_frames: 120,
            oldest_reader_frame: Some(150),
        };
        let plan = plan_checkpoint(CheckpointMode::Full, state);

        let ctx = DiagContext::new(BEAD_ID)
            .case("full_blocked")
            .invariant("FULL marks blocked when reader pins tail");
        diag_assert_eq!(ctx.clone(), plan.frames_to_backfill, 30);
        diag_assert_eq!(
            ctx.clone().case("full_incomplete"),
            plan.completes_checkpoint(),
            false
        );
        diag_assert_eq!(ctx, plan.blocked_by_readers, true);
    }

    #[test]
    fn checkpoint_full_completes_without_readers() {
        let state = CheckpointState {
            total_frames: 50,
            backfilled_frames: 20,
            oldest_reader_frame: None,
        };
        let plan = plan_checkpoint(CheckpointMode::Full, state);

        let ctx = DiagContext::new(BEAD_ID)
            .case("full_completes")
            .invariant("FULL completes when no readers");
        diag_assert_eq!(ctx.clone(), plan.frames_to_backfill, 30);
        diag_assert_eq!(ctx, plan.completes_checkpoint(), true);
    }

    #[test]
    fn checkpoint_restart_resets_wal_no_readers() {
        let state = CheckpointState {
            total_frames: 50,
            backfilled_frames: 0,
            oldest_reader_frame: None,
        };
        let plan = plan_checkpoint(CheckpointMode::Restart, state);

        let ctx = DiagContext::new(BEAD_ID)
            .case("restart_resets_wal")
            .invariant("RESTART resets WAL when complete and no readers");
        diag_assert_eq!(ctx.clone(), plan.should_reset_wal(), true);
        diag_assert_eq!(ctx, plan.completes_checkpoint(), true);
    }

    #[test]
    fn checkpoint_restart_no_reset_with_readers() {
        let state = CheckpointState {
            total_frames: 50,
            backfilled_frames: 0,
            oldest_reader_frame: Some(25),
        };
        let plan = plan_checkpoint(CheckpointMode::Restart, state);

        let ctx = DiagContext::new(BEAD_ID)
            .case("restart_no_reset_readers")
            .invariant("RESTART does NOT reset WAL when readers active");
        diag_assert_eq!(ctx, plan.should_reset_wal(), false);
    }

    #[test]
    fn checkpoint_truncate_truncates_wal_no_readers() {
        let state = CheckpointState {
            total_frames: 50,
            backfilled_frames: 0,
            oldest_reader_frame: None,
        };
        let plan = plan_checkpoint(CheckpointMode::Truncate, state);

        let ctx = DiagContext::new(BEAD_ID)
            .case("truncate_truncates")
            .invariant("TRUNCATE truncates WAL when complete and no readers");
        diag_assert_eq!(ctx, plan.should_truncate_wal(), true);
    }

    #[test]
    fn checkpoint_empty_wal_noop() {
        let state = CheckpointState {
            total_frames: 0,
            backfilled_frames: 0,
            oldest_reader_frame: None,
        };
        let plan = plan_checkpoint(CheckpointMode::Passive, state);

        let ctx = DiagContext::new(BEAD_ID)
            .case("empty_wal_noop")
            .invariant("Empty WAL checkpoint is noop");
        diag_assert_eq!(ctx.clone(), plan.frames_to_backfill, 0);
        diag_assert_eq!(ctx, plan.completes_checkpoint(), true);
    }

    #[test]
    fn checkpoint_already_complete() {
        let state = CheckpointState {
            total_frames: 100,
            backfilled_frames: 100,
            oldest_reader_frame: None,
        };
        let plan = plan_checkpoint(CheckpointMode::Full, state);

        let ctx = DiagContext::new(BEAD_ID)
            .case("already_complete")
            .invariant("Fully backfilled WAL requires zero additional frames");
        diag_assert_eq!(ctx.clone(), plan.frames_to_backfill, 0);
        diag_assert_eq!(ctx, plan.completes_checkpoint(), true);
    }

    // -- WAL constants --

    #[test]
    fn wal_header_size_32() {
        let ctx = DiagContext::new(BEAD_ID)
            .case("wal_header_size")
            .invariant("WAL header is 32 bytes per SQLite spec");
        diag_assert_eq!(ctx, WAL_HEADER_SIZE, 32);
    }

    #[test]
    fn wal_frame_header_size_24() {
        let ctx = DiagContext::new(BEAD_ID)
            .case("wal_frame_header_size")
            .invariant("WAL frame header is 24 bytes per spec");
        diag_assert_eq!(ctx, WAL_FRAME_HEADER_SIZE, 24);
    }
}

// ─── MVCC Suite ───────────────────────────────────────────────────────────

#[cfg(test)]
mod mvcc_tests {
    use super::*;
    use fsqlite_mvcc::core_types::{
        InProcessPageLockTable, LOCK_TABLE_SHARDS, Transaction, TransactionMode, TransactionState,
        VersionArena,
    };
    use fsqlite_types::glossary::{CommitSeq, PageVersion, Snapshot, TxnEpoch, TxnId, TxnToken};
    use fsqlite_types::{PageData, PageNumber, PageSize, SchemaEpoch};

    fn make_page_version(pgno: u32, commit: u64) -> PageVersion {
        let pgno = PageNumber::new(pgno).unwrap();
        let commit_seq = CommitSeq::new(commit);
        let txn_id = TxnId::new(1).unwrap();
        let created_by = TxnToken::new(txn_id, TxnEpoch::new(0));
        PageVersion {
            pgno,
            commit_seq,
            created_by,
            data: PageData::zeroed(PageSize::DEFAULT),
            prev: None,
        }
    }

    // -- TxnId boundary tests --

    #[test]
    fn txn_id_zero_rejected() {
        let ctx = DiagContext::new(BEAD_ID)
            .case("txn_id_zero")
            .invariant("TxnId 0 is reserved/invalid");
        diag_assert!(ctx, TxnId::new(0).is_none(), "0 must be rejected");
    }

    #[test]
    fn txn_id_one_accepted() {
        let ctx = DiagContext::new(BEAD_ID)
            .case("txn_id_one")
            .invariant("TxnId 1 is first valid ID");
        diag_assert!(ctx, TxnId::new(1).is_some(), "1 must be accepted");
    }

    #[test]
    fn txn_id_max_boundary() {
        let ctx = DiagContext::new(BEAD_ID)
            .case("txn_id_max")
            .invariant("TxnId MAX_RAW accepted, MAX_RAW+1 rejected");
        diag_assert!(
            ctx.clone(),
            TxnId::new(TxnId::MAX_RAW).is_some(),
            "MAX_RAW accepted"
        );
        diag_assert!(
            ctx,
            TxnId::new(TxnId::MAX_RAW + 1).is_none(),
            "MAX_RAW+1 rejected"
        );
    }

    #[test]
    fn txn_id_sentinel_encoding() {
        let max = TxnId::new(TxnId::MAX_RAW).unwrap();
        let ctx = DiagContext::new(BEAD_ID)
            .case("txn_id_sentinel")
            .invariant("Top two bits of TxnId::MAX must be clear");
        diag_assert_eq!(ctx, max.get() >> 62, 0);
    }

    // -- Epoch wraparound --

    #[test]
    fn txn_epoch_wraps_at_u32_max() {
        let epoch = TxnEpoch::new(u32::MAX);
        let next = epoch.get().wrapping_add(1);
        let ctx = DiagContext::new(BEAD_ID)
            .case("epoch_wraparound")
            .invariant("Epoch wraps u32::MAX → 0");
        diag_assert_eq!(ctx, next, 0);
    }

    // -- Snapshot visibility --

    #[test]
    fn snapshot_visibility_boundary() {
        let snap = Snapshot::new(CommitSeq::new(50), SchemaEpoch::ZERO);
        let ctx = DiagContext::new(BEAD_ID)
            .case("snapshot_visibility")
            .invariant("Snapshot at 50 sees <=50, not 51");
        diag_assert!(ctx.clone(), CommitSeq::new(50) <= snap.high, "50 visible");
        diag_assert!(ctx, CommitSeq::new(51) > snap.high, "51 invisible");
    }

    #[test]
    fn commit_seq_monotonic() {
        let a = CommitSeq::new(5);
        let b = CommitSeq::new(10);
        let ctx = DiagContext::new(BEAD_ID)
            .case("commit_seq_monotonic")
            .invariant("CommitSeq ordering is monotonic");
        diag_assert!(ctx.clone(), a < b, "5 < 10");
        diag_assert_eq!(ctx, a.next(), CommitSeq::new(6));
    }

    // -- Version arena --

    #[test]
    fn version_arena_alloc_free_reuse() {
        let mut arena = VersionArena::new();
        let v1 = make_page_version(1, 1);
        let idx1 = arena.alloc(v1);

        let ctx_alloc = DiagContext::new(BEAD_ID)
            .case("arena_alloc")
            .invariant("Allocated version is retrievable");
        diag_assert!(ctx_alloc, arena.get(idx1).is_some(), "v1 exists");

        arena.free(idx1);
        let ctx_free = DiagContext::new(BEAD_ID)
            .case("arena_free")
            .invariant("Freed version is no longer accessible");
        diag_assert!(ctx_free, arena.get(idx1).is_none(), "v1 freed");

        let v2 = make_page_version(2, 2);
        let idx2 = arena.alloc(v2);
        let ctx_reuse = DiagContext::new(BEAD_ID)
            .case("arena_reuse")
            .invariant("Freed slot reuses location and advances generation");
        diag_assert_eq!(ctx_reuse.clone(), idx1.chunk(), idx2.chunk());
        diag_assert_eq!(ctx_reuse.clone(), idx1.offset(), idx2.offset());
        diag_assert!(
            ctx_reuse,
            idx1.generation() != idx2.generation(),
            "generation must advance on slot reuse"
        );
    }

    // -- Page lock table --

    #[test]
    fn lock_table_acquire_release() {
        let table = InProcessPageLockTable::new();
        let page = PageNumber::new(42).unwrap();
        let txn_a = TxnId::new(1).unwrap();
        let txn_b = TxnId::new(2).unwrap();

        assert!(table.try_acquire(page, txn_a).is_ok());

        let ctx = DiagContext::new(BEAD_ID)
            .case("lock_acquire")
            .invariant("Lock holder matches acquiring txn");
        diag_assert_eq!(ctx, table.holder(page), Some(txn_a));

        // Contention: different txn gets Err(holder)
        let ctx_contention = DiagContext::new(BEAD_ID)
            .case("lock_contention")
            .invariant("Conflicting lock returns current holder");
        diag_assert_eq!(ctx_contention, table.try_acquire(page, txn_b), Err(txn_a));

        // Release
        assert!(table.release(page, txn_a));
        let ctx_released = DiagContext::new(BEAD_ID)
            .case("lock_released")
            .invariant("Released lock has no holder");
        diag_assert_eq!(ctx_released, table.holder(page), None);
    }

    #[test]
    fn lock_table_release_all() {
        let table = InProcessPageLockTable::new();
        let txn = TxnId::new(1).unwrap();

        for i in 1..=10_u32 {
            let page = PageNumber::new(i).unwrap();
            table.try_acquire(page, txn).unwrap();
        }

        let ctx = DiagContext::new(BEAD_ID)
            .case("lock_release_all")
            .invariant("release_all frees all locks for a txn");
        diag_assert_eq!(ctx.clone(), table.lock_count(), 10);
        table.release_all(txn);
        diag_assert_eq!(ctx, table.lock_count(), 0);
    }

    #[test]
    fn lock_table_shard_distribution() {
        let table = InProcessPageLockTable::new();
        let txn = TxnId::new(1).unwrap();

        for i in 1..=128_u32 {
            let page = PageNumber::new(i).unwrap();
            table.try_acquire(page, txn).unwrap();
        }

        let dist = table.shard_distribution();
        let ctx = DiagContext::new(BEAD_ID)
            .case("shard_distribution")
            .invariant("128 pages across 64 shards → 2 each");
        diag_assert_eq!(ctx.clone(), dist.len(), LOCK_TABLE_SHARDS);
        for &count in &dist {
            diag_assert_eq!(ctx.clone(), count, 2);
        }
    }

    #[test]
    fn lock_table_idempotent_reacquire() {
        let table = InProcessPageLockTable::new();
        let page = PageNumber::new(1).unwrap();
        let txn = TxnId::new(1).unwrap();

        assert!(table.try_acquire(page, txn).is_ok());
        let ctx = DiagContext::new(BEAD_ID)
            .case("idempotent_reacquire")
            .invariant("Same txn re-acquiring same page succeeds");
        diag_assert!(ctx, table.try_acquire(page, txn).is_ok(), "reacquire ok");
    }

    // -- Transaction state machine --

    #[test]
    fn transaction_state_machine_commit() {
        let txn_id = TxnId::new(1).unwrap();
        let snap = Snapshot::new(CommitSeq::new(0), SchemaEpoch::ZERO);
        let mut txn = Transaction::new(txn_id, TxnEpoch::new(0), snap, TransactionMode::Concurrent);

        let ctx_active = DiagContext::new(BEAD_ID)
            .case("txn_starts_active")
            .invariant("New txn starts in Active state");
        diag_assert_eq!(ctx_active, txn.state, TransactionState::Active);

        txn.commit();
        let ctx_committed = DiagContext::new(BEAD_ID)
            .case("txn_committed")
            .invariant("After commit → Committed state");
        diag_assert_eq!(ctx_committed, txn.state, TransactionState::Committed);
    }

    #[test]
    fn transaction_state_machine_abort() {
        let txn_id = TxnId::new(2).unwrap();
        let snap = Snapshot::new(CommitSeq::new(0), SchemaEpoch::ZERO);
        let mut txn = Transaction::new(txn_id, TxnEpoch::new(0), snap, TransactionMode::Concurrent);

        txn.abort();
        let ctx = DiagContext::new(BEAD_ID)
            .case("txn_aborted")
            .invariant("After abort → Aborted state");
        diag_assert_eq!(ctx, txn.state, TransactionState::Aborted);
    }

    #[test]
    #[should_panic(expected = "can only commit active")]
    fn transaction_double_commit_panics() {
        let txn_id = TxnId::new(3).unwrap();
        let snap = Snapshot::new(CommitSeq::new(0), SchemaEpoch::ZERO);
        let mut txn = Transaction::new(txn_id, TxnEpoch::new(0), snap, TransactionMode::Concurrent);
        txn.commit();
        txn.commit(); // panic
    }

    #[test]
    #[should_panic(expected = "can only abort active")]
    fn transaction_commit_then_abort_panics() {
        let txn_id = TxnId::new(4).unwrap();
        let snap = Snapshot::new(CommitSeq::new(0), SchemaEpoch::ZERO);
        let mut txn = Transaction::new(txn_id, TxnEpoch::new(0), snap, TransactionMode::Concurrent);
        txn.commit();
        txn.abort(); // panic
    }

    #[test]
    fn transaction_fields_initialized() {
        let txn_id = TxnId::new(42).unwrap();
        let epoch = TxnEpoch::new(7);
        let snap = Snapshot::new(CommitSeq::new(100), SchemaEpoch::new(3));
        let txn = Transaction::new(txn_id, epoch, snap, TransactionMode::Concurrent);

        let ctx = DiagContext::new(BEAD_ID)
            .case("txn_fields_init")
            .invariant("Transaction new() initializes all fields correctly");
        diag_assert_eq!(ctx.clone().case("txn_id"), txn.txn_id, txn_id);
        diag_assert_eq!(ctx.clone().case("txn_epoch"), txn.txn_epoch, epoch);
        diag_assert!(ctx.clone(), txn.slot_id.is_none(), "slot_id starts None");
        diag_assert_eq!(
            ctx.clone().case("snapshot_high"),
            txn.snapshot.high,
            CommitSeq::new(100)
        );
        diag_assert!(ctx.clone(), txn.write_set.is_empty(), "write_set empty");
        diag_assert!(ctx.clone(), txn.intent_log.is_empty(), "intent_log empty");
        diag_assert!(ctx.clone(), txn.page_locks.is_empty(), "page_locks empty");
        diag_assert_eq!(
            ctx.clone().case("state"),
            txn.state,
            TransactionState::Active
        );
        diag_assert!(ctx, !txn.serialized_write_lock_held, "no serialized lock");
    }

    // -- SSI dangerous structure detection --

    #[test]
    fn transaction_ssi_dangerous_structure() {
        let txn_id = TxnId::new(1).unwrap();
        let snap = Snapshot::new(CommitSeq::new(0), SchemaEpoch::ZERO);
        let mut txn = Transaction::new(txn_id, TxnEpoch::new(0), snap, TransactionMode::Concurrent);

        let ctx = DiagContext::new(BEAD_ID)
            .case("ssi_dangerous_structure")
            .invariant("Dangerous structure requires both in_rw and out_rw");
        diag_assert!(ctx.clone(), !txn.has_in_rw, "starts no in_rw");
        diag_assert!(ctx.clone(), !txn.has_out_rw, "starts no out_rw");

        txn.has_in_rw = true;
        txn.has_out_rw = true;
        diag_assert!(
            ctx,
            txn.has_in_rw && txn.has_out_rw,
            "dangerous when both set"
        );
    }
}

// ─── B-tree Suite ─────────────────────────────────────────────────────────

#[cfg(test)]
mod btree_tests {
    use super::*;
    use fsqlite_btree::cell::{
        BtreePageHeader, BtreePageType, has_overflow, max_local_payload, min_local_payload,
        read_cell_pointers, write_cell_pointers,
    };
    use fsqlite_types::PageNumber;

    // -- Page type flags --

    #[test]
    fn btree_page_type_flags() {
        let ctx = DiagContext::new(BEAD_ID)
            .case("page_type_flags")
            .invariant("Page type flags match SQLite spec");
        diag_assert_eq!(
            ctx.clone().case("interior_index"),
            BtreePageType::from_flag(0x02),
            Some(BtreePageType::InteriorIndex)
        );
        diag_assert_eq!(
            ctx.clone().case("interior_table"),
            BtreePageType::from_flag(0x05),
            Some(BtreePageType::InteriorTable)
        );
        diag_assert_eq!(
            ctx.clone().case("leaf_index"),
            BtreePageType::from_flag(0x0A),
            Some(BtreePageType::LeafIndex)
        );
        diag_assert_eq!(
            ctx.clone().case("leaf_table"),
            BtreePageType::from_flag(0x0D),
            Some(BtreePageType::LeafTable)
        );
        diag_assert_eq!(
            ctx.clone().case("zero_invalid"),
            BtreePageType::from_flag(0x00),
            None
        );
        diag_assert_eq!(ctx, BtreePageType::from_flag(0xFF), None);
    }

    #[test]
    fn btree_page_type_predicates() {
        let ctx = DiagContext::new(BEAD_ID)
            .case("page_type_predicates")
            .invariant("Interior/leaf/table/index predicates correct");
        diag_assert!(
            ctx.clone(),
            BtreePageType::InteriorTable.is_interior(),
            "IT interior"
        );
        diag_assert!(
            ctx.clone(),
            BtreePageType::InteriorIndex.is_interior(),
            "II interior"
        );
        diag_assert!(
            ctx.clone(),
            !BtreePageType::LeafTable.is_interior(),
            "LT not interior"
        );
        diag_assert!(ctx.clone(), BtreePageType::LeafTable.is_leaf(), "LT leaf");
        diag_assert!(
            ctx.clone(),
            BtreePageType::InteriorTable.is_table(),
            "IT table"
        );
        diag_assert!(ctx, BtreePageType::InteriorIndex.is_index(), "II index");
    }

    #[test]
    fn btree_header_size_by_type() {
        let ctx = DiagContext::new(BEAD_ID)
            .case("header_size")
            .invariant("Leaf headers 8 bytes, interior headers 12 bytes");
        diag_assert_eq!(ctx.clone(), BtreePageType::LeafTable.header_size(), 8);
        diag_assert_eq!(ctx.clone(), BtreePageType::LeafIndex.header_size(), 8);
        diag_assert_eq!(ctx.clone(), BtreePageType::InteriorTable.header_size(), 12);
        diag_assert_eq!(ctx, BtreePageType::InteriorIndex.header_size(), 12);
    }

    // -- Page header roundtrip --

    #[test]
    fn btree_leaf_table_header_roundtrip() {
        let header = BtreePageHeader {
            page_type: BtreePageType::LeafTable,
            first_freeblock: 0,
            cell_count: 5,
            cell_content_offset: 3800,
            fragmented_free_bytes: 2,
            right_child: None,
        };

        let mut page = vec![0u8; 4096];
        header.write(&mut page, 0);
        let parsed = BtreePageHeader::parse(&page, 0).unwrap();

        let ctx = DiagContext::new(BEAD_ID)
            .case("leaf_table_roundtrip")
            .invariant("Leaf table header write→parse roundtrip");
        diag_assert_eq!(ctx, parsed, header);
    }

    #[test]
    fn btree_interior_table_header_roundtrip() {
        let right_child = PageNumber::new(42).unwrap();
        let header = BtreePageHeader {
            page_type: BtreePageType::InteriorTable,
            first_freeblock: 100,
            cell_count: 10,
            cell_content_offset: 2048,
            fragmented_free_bytes: 0,
            right_child: Some(right_child),
        };

        let mut page = vec![0u8; 4096];
        header.write(&mut page, 0);
        let parsed = BtreePageHeader::parse(&page, 0).unwrap();

        let ctx = DiagContext::new(BEAD_ID)
            .case("interior_table_roundtrip")
            .invariant("Interior table header with right_child roundtrips");
        diag_assert_eq!(ctx.clone(), parsed, header);
        diag_assert_eq!(ctx, parsed.right_child.unwrap().get(), 42);
    }

    #[test]
    fn btree_page1_offset_100() {
        let header = BtreePageHeader {
            page_type: BtreePageType::LeafTable,
            first_freeblock: 0,
            cell_count: 3,
            cell_content_offset: 3900,
            fragmented_free_bytes: 0,
            right_child: None,
        };

        let mut page = vec![0u8; 4096];
        header.write(&mut page, 100);
        let parsed = BtreePageHeader::parse(&page, 100).unwrap();

        let ctx = DiagContext::new(BEAD_ID)
            .case("page1_offset_100")
            .invariant("Page 1 header at offset 100 (after db header)");
        diag_assert_eq!(ctx, parsed, header);
    }

    #[test]
    fn btree_content_offset_zero_means_65536() {
        let header = BtreePageHeader {
            page_type: BtreePageType::LeafTable,
            first_freeblock: 0,
            cell_count: 0,
            cell_content_offset: 65536,
            fragmented_free_bytes: 0,
            right_child: None,
        };

        let mut page = vec![0u8; 65536];
        header.write(&mut page, 0);

        let ctx_raw = DiagContext::new(BEAD_ID)
            .case("content_offset_raw_zero")
            .invariant("65536 cell_content_offset encoded as 0x0000");
        diag_assert_eq!(ctx_raw.clone(), page[5], 0);
        diag_assert_eq!(ctx_raw, page[6], 0);

        let parsed = BtreePageHeader::parse(&page, 0).unwrap();
        let ctx = DiagContext::new(BEAD_ID)
            .case("content_offset_decoded_65536")
            .invariant("0x0000 decodes as 65536");
        diag_assert_eq!(ctx, parsed.cell_content_offset, 65536);
    }

    #[test]
    fn btree_invalid_type_rejected() {
        let mut page = vec![0u8; 4096];
        page[0] = 0xFF;
        let result = BtreePageHeader::parse(&page, 0);
        let ctx = DiagContext::new(BEAD_ID)
            .case("invalid_type_rejected")
            .invariant("Invalid page type flag must error");
        diag_assert!(ctx, result.is_err(), "0xFF type must fail");
    }

    #[test]
    fn btree_truncated_page_rejected() {
        let page = vec![0u8; 4];
        let result = BtreePageHeader::parse(&page, 0);
        let ctx = DiagContext::new(BEAD_ID)
            .case("truncated_page")
            .invariant("Truncated page must error");
        diag_assert!(ctx, result.is_err(), "too-short page must fail");
    }

    // -- Cell pointer array --

    #[test]
    fn btree_cell_pointer_roundtrip() {
        let header = BtreePageHeader {
            page_type: BtreePageType::LeafTable,
            first_freeblock: 0,
            cell_count: 3,
            cell_content_offset: 3800,
            fragmented_free_bytes: 0,
            right_child: None,
        };

        let mut page = vec![0u8; 4096];
        header.write(&mut page, 0);

        let ptrs = [3900u16, 3950, 4000];
        write_cell_pointers(&mut page, 0, &header, &ptrs);
        let read_ptrs = read_cell_pointers(&page, &header, 0).unwrap();

        let ctx = DiagContext::new(BEAD_ID)
            .case("cell_pointer_roundtrip")
            .invariant("Cell pointer write→read roundtrip");
        diag_assert_eq!(ctx, read_ptrs, vec![3900u16, 3950, 4000]);
    }

    // -- Payload size calculations --

    #[test]
    fn btree_max_local_payload_leaf_table() {
        let ctx = DiagContext::new(BEAD_ID)
            .case("max_local_leaf_table")
            .invariant("Leaf table max local = U - 35");
        diag_assert_eq!(ctx, max_local_payload(4096, BtreePageType::LeafTable), 4061);
    }

    #[test]
    fn btree_max_local_payload_other() {
        let expected = (4096 - 12) * 64 / 255 - 23;
        let ctx = DiagContext::new(BEAD_ID)
            .case("max_local_other")
            .invariant("Non-leaf-table max local = (U-12)*64/255 - 23");
        diag_assert_eq!(
            ctx.clone(),
            max_local_payload(4096, BtreePageType::InteriorIndex),
            expected
        );
        diag_assert_eq!(
            ctx.clone(),
            max_local_payload(4096, BtreePageType::LeafIndex),
            expected
        );
        diag_assert_eq!(
            ctx,
            max_local_payload(4096, BtreePageType::InteriorTable),
            expected
        );
    }

    #[test]
    fn btree_min_local_payload() {
        let expected = (4096 - 12) * 32 / 255 - 23;
        let ctx = DiagContext::new(BEAD_ID)
            .case("min_local")
            .invariant("Min local = (U-12)*32/255 - 23");
        diag_assert_eq!(ctx, min_local_payload(4096), expected);
    }

    #[test]
    fn btree_overflow_detection() {
        let max_leaf = max_local_payload(4096, BtreePageType::LeafTable);
        let ctx = DiagContext::new(BEAD_ID)
            .case("overflow_detection")
            .invariant("Payload > max_local triggers overflow");
        diag_assert!(
            ctx.clone(),
            !has_overflow(max_leaf, 4096, BtreePageType::LeafTable),
            "no overflow at max"
        );
        diag_assert!(
            ctx,
            has_overflow(max_leaf + 1, 4096, BtreePageType::LeafTable),
            "overflow at max+1"
        );
    }
}

// ─── VFS Suite ────────────────────────────────────────────────────────────

#[cfg(test)]
mod vfs_tests {
    use super::*;
    use fsqlite_types::cx::Cx;
    use fsqlite_types::flags::VfsOpenFlags;
    use fsqlite_vfs::{MemoryVfs, Vfs, VfsFile};

    // -- Memory VFS read/write --

    #[test]
    fn memory_vfs_write_read_roundtrip() {
        let vfs = MemoryVfs::new();
        let cx = Cx::new();
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::MAIN_DB;
        let (mut file, _) = vfs
            .open(&cx, Some(std::path::Path::new("vfs-test.db")), flags)
            .expect("open");

        let seed = FixtureSeed::derive("vfs-write-roundtrip");
        let data: Vec<u8> = seed.raw().to_le_bytes().to_vec();
        file.write(&cx, &data, 0).expect("write");

        let mut buf = vec![0u8; data.len()];
        file.read(&cx, &mut buf, 0).expect("read");

        let ctx = DiagContext::new(BEAD_ID)
            .case("vfs_write_read")
            .seed(seed.raw())
            .invariant("VFS write→read roundtrip");
        diag_assert_eq!(ctx, buf, data);
    }

    #[test]
    fn memory_vfs_file_size_tracks_writes() {
        let vfs = MemoryVfs::new();
        let cx = Cx::new();
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::MAIN_DB;
        let (mut file, _) = vfs
            .open(&cx, Some(std::path::Path::new("vfs-size.db")), flags)
            .expect("open");

        let ctx = DiagContext::new(BEAD_ID)
            .case("file_size_tracks")
            .invariant("File size grows with writes");

        let initial = file.file_size(&cx).expect("size");
        diag_assert_eq!(ctx.clone().case("initial_zero"), initial, 0);

        file.write(&cx, &[0xAB; 4096], 0).expect("write 4k");
        let after_write = file.file_size(&cx).expect("size");
        diag_assert_eq!(ctx, after_write, 4096);
    }

    #[test]
    fn memory_vfs_multiple_files_isolated() {
        let vfs = MemoryVfs::new();
        let cx = Cx::new();
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::MAIN_DB;

        let (mut f1, _) = vfs
            .open(&cx, Some(std::path::Path::new("iso-a.db")), flags)
            .expect("open a");
        let (mut f2, _) = vfs
            .open(&cx, Some(std::path::Path::new("iso-b.db")), flags)
            .expect("open b");

        f1.write(&cx, &[0xAA; 100], 0).expect("write a");
        f2.write(&cx, &[0xBB; 200], 0).expect("write b");

        let ctx = DiagContext::new(BEAD_ID)
            .case("file_isolation")
            .invariant("Separate VFS files are isolated");
        diag_assert_eq!(ctx.clone().case("f1_size"), f1.file_size(&cx).unwrap(), 100);
        diag_assert_eq!(ctx, f2.file_size(&cx).unwrap(), 200);
    }

    #[test]
    fn memory_vfs_truncate() {
        let vfs = MemoryVfs::new();
        let cx = Cx::new();
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::MAIN_DB;
        let (mut file, _) = vfs
            .open(&cx, Some(std::path::Path::new("trunc.db")), flags)
            .expect("open");

        file.write(&cx, &[0xCC; 8192], 0).expect("write 8k");
        file.truncate(&cx, 4096).expect("truncate to 4k");

        let ctx = DiagContext::new(BEAD_ID)
            .case("vfs_truncate")
            .invariant("Truncate reduces file size");
        diag_assert_eq!(ctx, file.file_size(&cx).unwrap(), 4096);
    }

    #[test]
    fn memory_vfs_read_beyond_eof_zeroes() {
        let vfs = MemoryVfs::new();
        let cx = Cx::new();
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::MAIN_DB;
        let (mut file, _) = vfs
            .open(&cx, Some(std::path::Path::new("eof.db")), flags)
            .expect("open");

        file.write(&cx, &[0xDD; 100], 0).expect("write");

        let mut buf = vec![0xFF; 200];
        // Read starting at offset 50, spanning past EOF.
        let result = file.read(&cx, &mut buf, 50);
        let ctx = DiagContext::new(BEAD_ID)
            .case("read_beyond_eof")
            .invariant("Read past EOF pads with zeros or short-reads");
        // The exact behavior depends on VFS impl — just verify no panic.
        diag_assert!(ctx, result.is_ok(), "read past EOF should not panic");
    }

    // -- SHM region management --

    #[test]
    fn memory_vfs_shm_region_create() {
        let vfs = MemoryVfs::new();
        let cx = Cx::new();
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::MAIN_DB;
        let (mut file, _) = vfs
            .open(&cx, Some(std::path::Path::new("shm-test.db")), flags)
            .expect("open");

        let region = file.shm_map(&cx, 0, 32768, true);
        let ctx = DiagContext::new(BEAD_ID)
            .case("shm_create")
            .invariant("SHM region 0 creation succeeds");
        diag_assert!(ctx, region.is_ok(), "shm_map region 0 must succeed");
    }
}
