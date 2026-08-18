//! bd-v0t32 adversarial corpus — slow disk (injected write/sync latency).
//!
//! Injected I/O latency must not cause a spurious failure or corruption: a
//! commit whose writes and syncs are delayed still succeeds and its data reads
//! back byte-for-byte. This is the pager/VFS-level correctness invariant for a
//! slow disk (the fault is injected via the harness `FaultInjectingVfs`).
//!
//! Note: the higher-level "a slow writer must not be misjudged Dead by the
//! MVCC lease layer" property needs the concurrent-writer/lease machinery and is
//! out of scope for this single-pager module; it is tracked with the remaining
//! corpus work (bd-v0t32).

use std::path::{Path, PathBuf};

use fsqlite_e2e::block_on;
use fsqlite_harness::fault_vfs::FaultInjectingVfs;
use fsqlite_harness::fault_vfs::FaultSpec;
use fsqlite_pager::{MvccPager, SimplePager, TransactionHandle, TransactionMode};
use fsqlite_types::cx::Cx;
use fsqlite_types::{PageNumber, PageSize};
use fsqlite_vfs::MemoryVfs;

const SEED: u64 = 0x0056_3074_3332_534C; // "V0t32SL"

fn page(fill: u8) -> Vec<u8> {
    vec![fill; PageSize::DEFAULT.as_usize()]
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

/// A commit whose writes are delayed still succeeds and is durable.
#[test]
fn write_latency_does_not_fail_or_corrupt_commit() {
    let path = PathBuf::from("/v0t32_slow_write.db");
    let backing = MemoryVfs::new();
    let cx = Cx::new();

    let fault_vfs = FaultInjectingVfs::with_seed(backing.clone(), SEED);
    // Delay every matching write for the whole commit (not one-shot).
    fault_vfs.inject_fault(
        FaultSpec::latency("*")
            .latency_millis(2)
            .trigger_count(256)
            .build(),
    );
    let pager = block_on(SimplePager::open_with_cx(
        &cx,
        fault_vfs,
        &path,
        PageSize::DEFAULT,
    ))
    .expect("open");

    let page_no = {
        let mut txn = block_on(pager.begin(&cx, TransactionMode::Immediate)).expect("begin");
        let page_no = block_on(txn.allocate_page(&cx)).expect("allocate");
        block_on(txn.write_page(&cx, page_no, &page(0x3B))).expect("write under latency");
        block_on(txn.commit(&cx)).expect("commit must succeed despite write latency");
        page_no
    };
    drop(pager);

    assert_eq!(
        read_page(&backing, &path, page_no),
        page(0x3B),
        "data committed under injected write latency must be durable and intact"
    );
}

/// Multiple committed pages under sustained latency all survive intact and in
/// order — latency perturbs timing, never correctness.
#[test]
fn sustained_latency_preserves_all_committed_pages() {
    let path = PathBuf::from("/v0t32_slow_many.db");
    let backing = MemoryVfs::new();
    let cx = Cx::new();

    let fault_vfs = FaultInjectingVfs::with_seed(backing.clone(), SEED ^ 0x11);
    fault_vfs.inject_fault(
        FaultSpec::latency("*")
            .latency_millis(1)
            .trigger_count(1024)
            .build(),
    );
    let pager = block_on(SimplePager::open_with_cx(
        &cx,
        fault_vfs,
        &path,
        PageSize::DEFAULT,
    ))
    .expect("open");

    let mut pages = Vec::new();
    for i in 0..8u8 {
        let mut txn = block_on(pager.begin(&cx, TransactionMode::Immediate)).expect("begin");
        let page_no = block_on(txn.allocate_page(&cx)).expect("allocate");
        block_on(txn.write_page(&cx, page_no, &page(0x40 | i))).expect("write");
        block_on(txn.commit(&cx)).expect("commit under latency");
        pages.push((page_no, 0x40 | i));
    }
    drop(pager);

    for (page_no, fill) in pages {
        assert_eq!(
            read_page(&backing, &path, page_no),
            page(fill),
            "every page committed under latency must be intact"
        );
    }
}
