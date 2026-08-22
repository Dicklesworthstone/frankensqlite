//! bd-v0t32 adversarial corpus — full-disk / ENOSPC.
//!
//! A disk-full condition during a write must surface as the SQLite-compatible
//! `SQLITE_FULL` (FrankenError::DatabaseFull, error code 13) — never a silent
//! success and never corruption of the last committed image — and the database
//! must remain usable once space is available again.
//!
//! The fault is injected at the pager/VFS layer via the harness
//! `FaultInjectingVfs` (the ENOSPC surface is below the SQL/schema layer, so the
//! oracle here is a fresh pager reading the committed pages back, not stock C
//! SQLite). This extends the disk-full coverage in
//! `bd_mblr_2_3_fault_injection_reliability` with the explicit SQLITE_FULL error
//! *code* and transient-full *recovery* assertions.

use std::path::{Path, PathBuf};

use fsqlite_e2e::block_on;
use fsqlite_error::{ErrorCode, FrankenError};
use fsqlite_harness::fault_vfs::{FaultInjectingVfs, FaultSpec};
use fsqlite_pager::{MvccPager, SimplePager, TransactionHandle, TransactionMode};
use fsqlite_types::cx::Cx;
use fsqlite_types::{PageNumber, PageSize};
use fsqlite_vfs::MemoryVfs;

const SEED: u64 = 0x0056_3074_3332_454E; // "V0t32EN"

fn page(fill: u8) -> Vec<u8> {
    vec![fill; PageSize::DEFAULT.as_usize()]
}

fn seed_committed_page(backing: &MemoryVfs, path: &Path, fill: u8) -> (PageNumber, Vec<u8>) {
    let cx = Cx::new();
    let pager = block_on(SimplePager::open_with_cx(
        &cx,
        backing.clone(),
        path,
        PageSize::DEFAULT,
    ))
    .expect("open seed pager");
    let original = page(fill);
    let page_no = {
        let mut txn = block_on(pager.begin(&cx, TransactionMode::Immediate)).expect("begin seed");
        let page_no = block_on(txn.allocate_page(&cx)).expect("allocate");
        block_on(txn.write_page(&cx, page_no, &original)).expect("write seed");
        block_on(txn.commit(&cx)).expect("commit seed");
        page_no
    };
    drop(pager);
    (page_no, original)
}

fn read_page(backing: &MemoryVfs, path: &Path, page_no: PageNumber) -> Vec<u8> {
    let cx = Cx::new();
    let pager = block_on(SimplePager::open_with_cx(
        &cx,
        backing.clone(),
        path,
        PageSize::DEFAULT,
    ))
    .expect("open reader");
    let reader = block_on(pager.begin(&cx, TransactionMode::ReadOnly)).expect("begin readonly");
    let bytes = block_on(reader.get_page(&cx, page_no))
        .expect("read page")
        .as_ref()
        .to_vec();
    drop(reader);
    bytes
}

/// A disk-full commit surfaces SQLITE_FULL (code 13) and never mutates the last
/// committed page image.
#[test]
fn disk_full_commit_surfaces_sqlite_full_code_and_preserves_page() {
    let path = PathBuf::from("/v0t32_enospc_code.db");
    let backing = MemoryVfs::new();
    let (page_no, original) = seed_committed_page(&backing, &path, 0x11);
    let cx = Cx::new();

    let fault_vfs = FaultInjectingVfs::with_seed(backing.clone(), SEED);
    fault_vfs.inject_fault(FaultSpec::disk_full("*.db-journal").build());
    let pager = block_on(SimplePager::open_with_cx(
        &cx,
        fault_vfs,
        &path,
        PageSize::DEFAULT,
    ))
    .expect("open");

    let err = {
        let mut txn = block_on(pager.begin(&cx, TransactionMode::Immediate)).expect("begin");
        block_on(txn.write_page(&cx, page_no, &page(0x7A))).expect("stage");
        block_on(txn.commit(&cx)).expect_err("commit into a full disk must fail")
    };
    assert!(
        matches!(err, FrankenError::DatabaseFull),
        "ENOSPC must surface DatabaseFull, got {err:?}"
    );
    assert_eq!(
        err.error_code(),
        ErrorCode::Full,
        "DatabaseFull must map to SQLITE_FULL"
    );
    assert_eq!(ErrorCode::Full as i32, 13, "SQLITE_FULL is 13");
    drop(pager);

    assert_eq!(
        read_page(&backing, &path, page_no),
        original,
        "the last committed page image must survive the failed disk-full commit"
    );
}

/// Once space is available again (a fresh pager with no fault), the database is
/// fully usable — the disk-full condition is transient, not wedging.
#[test]
fn database_recovers_after_transient_disk_full() {
    let path = PathBuf::from("/v0t32_enospc_recover.db");
    let backing = MemoryVfs::new();
    let (page_no, original) = seed_committed_page(&backing, &path, 0x22);
    let cx = Cx::new();

    // Full disk: the commit fails.
    {
        let fault_vfs = FaultInjectingVfs::with_seed(backing.clone(), SEED ^ 0x0F);
        fault_vfs.inject_fault(FaultSpec::disk_full("*.db-journal").build());
        let pager = block_on(SimplePager::open_with_cx(
            &cx,
            fault_vfs,
            &path,
            PageSize::DEFAULT,
        ))
        .expect("open fault pager");
        let mut txn = block_on(pager.begin(&cx, TransactionMode::Immediate)).expect("begin");
        block_on(txn.write_page(&cx, page_no, &page(0x99))).expect("stage");
        assert!(
            matches!(block_on(txn.commit(&cx)), Err(FrankenError::DatabaseFull)),
            "the disk-full commit must fail"
        );
        drop(pager);
    }

    // The old image is intact after the failure.
    assert_eq!(
        read_page(&backing, &path, page_no),
        original,
        "pre-full image intact"
    );

    // Space is back: a fresh pager commits new data, which reads back correctly.
    {
        let pager = block_on(SimplePager::open_with_cx(
            &cx,
            backing.clone(),
            &path,
            PageSize::DEFAULT,
        ))
        .expect("open clean pager");
        let mut txn = block_on(pager.begin(&cx, TransactionMode::Immediate)).expect("begin retry");
        block_on(txn.write_page(&cx, page_no, &page(0x5C))).expect("stage retry");
        block_on(txn.commit(&cx)).expect("retry commit must succeed once space is back");
        drop(pager);
    }
    assert_eq!(
        read_page(&backing, &path, page_no),
        page(0x5C),
        "the post-recovery commit must be durable and readable"
    );
}
