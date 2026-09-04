//! GH #410 keepers — the reserved lock-byte page never reaches the freelist,
//! and an archive whose durable freelist already names it is repairable.
//!
//! SQLite reserves the page containing byte offset `0x4000_0000` (1 GiB): the
//! "lock-byte page". It is never allocated and never freed. A 10 GB archive
//! written by fsqlite 0.3.13/0.3.14 was found with page 262145 (the lock-byte
//! page at `page_size = 4096`) sitting on a freelist leaf, which
//! `PRAGMA integrity_check` reports as
//! `page 262145 referenced by freelist trunk[…] leaf[…] is the reserved
//! lock-byte page`.
//!
//! These keepers pin both halves of the fix:
//!
//! 1. *writer* — every path that can put a page on the freelist funnels
//!    through `normalize_freelist`, which drops the reserved page, and
//!    `free_page` refuses it outright, so a database whose freelist straddles
//!    the boundary allocates right past the reserved page instead of handing
//!    it out;
//! 2. *recovery* — an EXISTING file whose durable trunk chain names it is
//!    repaired in place by `PRAGMA fsqlite.repair_freelist`, after which
//!    `integrity_check` stops reporting it and stock SQLite's own
//!    `quick_check` agrees.
//!
//! The fixture is a SPARSE file: it declares 262 146 pages of 4 KiB (just over
//! 1 GiB) but only page 1 and the freelist trunk chain are ever written, so it
//! costs about 1 MB on disk on any filesystem with sparse-file support.

#![allow(clippy::cast_possible_truncation)]

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

const PAGE_SIZE: u64 = 4096;
/// `0x4000_0000 / 4096 + 1`.
const LOCK_BYTE_PAGE: u32 = 262_145;
/// One page past the lock-byte page, so the declared image straddles it.
const PAGE_COUNT: u32 = 262_146;
/// `page_size / 4 - 2`, matching the on-disk trunk layout.
const MAX_LEAF_ENTRIES: usize = 1022;

fn page_offset(page: u32) -> u64 {
    u64::from(page - 1) * PAGE_SIZE
}

fn texts(rows: &[Row]) -> Vec<String> {
    rows.iter()
        .map(|row| match row.values()[0] {
            SqliteValue::Text(ref text) => text.to_string(),
            ref other => panic!("expected text, got {other:?}"),
        })
        .collect()
}

fn scalar_i64(rows: &[Row]) -> i64 {
    match rows[0].values()[0] {
        SqliteValue::Integer(n) => n,
        ref other => panic!("expected an integer, got {other:?}"),
    }
}

/// Build a sparse database that declares `PAGE_COUNT` pages and whose durable
/// freelist is exactly `free` (which must be strictly descending).
///
/// Starts from a real, empty SQLite file (so page 1 is a genuine header plus
/// an empty `sqlite_master` leaf) and then rewrites the page-count and
/// freelist header fields and lays down the trunk chain, byte for byte the way
/// the engine's own serializer does.
fn build_sparse_archive(path: &Path, free: &[u32]) {
    {
        let conn = rusqlite::Connection::open(path).expect("create fixture");
        conn.pragma_update(None, "page_size", 4096_i64)
            .expect("page_size");
        conn.pragma_update(None, "journal_mode", "delete")
            .expect("journal_mode");
        // A real table keeps the header fully initialized (schema format,
        // cookie, text encoding); it owns page 2, and every page above it is
        // what the fixture declares free.
        conn.execute_batch("CREATE TABLE seed(x); VACUUM;")
            .expect("seed");
    }
    assert!(
        free.windows(2).all(|w| w[0] > w[1]),
        "the fixture freelist must be strictly descending"
    );

    let trunk_count = free.len().div_ceil(MAX_LEAF_ENTRIES + 1);
    let trunks: Vec<u32> = free.iter().copied().take(trunk_count).collect();

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .expect("open fixture");
    // Declare the sparse extent first; the trunk writes below land inside it.
    file.set_len(u64::from(PAGE_COUNT) * PAGE_SIZE)
        .expect("extend fixture");

    let mut header = [0_u8; 100];
    file.read_exact(&mut header).expect("read header");
    assert_eq!(&header[..16], b"SQLite format 3\0", "fixture is a SQLite file");
    header[28..32].copy_from_slice(&PAGE_COUNT.to_be_bytes());
    header[32..36].copy_from_slice(&trunks[0].to_be_bytes());
    header[36..40].copy_from_slice(&(free.len() as u32).to_be_bytes());
    // Bump the change counter and keep `version_valid_for` in step so readers
    // do not mistake the rewritten header for a stale one.
    let change_counter = u32::from_be_bytes([header[24], header[25], header[26], header[27]]) + 1;
    header[24..28].copy_from_slice(&change_counter.to_be_bytes());
    header[92..96].copy_from_slice(&change_counter.to_be_bytes());
    file.seek(SeekFrom::Start(0)).expect("seek header");
    file.write_all(&header).expect("write header");

    let mut leaf_index = trunks.len();
    for (idx, trunk) in trunks.iter().enumerate() {
        let next = trunks.get(idx + 1).copied().unwrap_or(0);
        let take = free.len().saturating_sub(leaf_index).min(MAX_LEAF_ENTRIES);
        let mut page = vec![0_u8; PAGE_SIZE as usize];
        page[0..4].copy_from_slice(&next.to_be_bytes());
        page[4..8].copy_from_slice(&(take as u32).to_be_bytes());
        for i in 0..take {
            let base = 8 + i * 4;
            page[base..base + 4].copy_from_slice(&free[leaf_index + i].to_be_bytes());
        }
        leaf_index += take;
        file.seek(SeekFrom::Start(page_offset(*trunk)))
            .expect("seek trunk");
        file.write_all(&page).expect("write trunk");
    }
    assert_eq!(
        leaf_index,
        free.len(),
        "every free page is named by the chain"
    );
    file.sync_all().expect("sync fixture");
}

/// Read the durable freelist straight out of the file (header head/count plus
/// the trunk chain), independent of anything the engine believes.
fn durable_freelist(path: &Path) -> Vec<u32> {
    let mut file = std::fs::File::open(path).expect("open for freelist read");
    let mut header = [0_u8; 100];
    file.read_exact(&mut header).expect("read header");
    let mut trunk = u32::from_be_bytes([header[32], header[33], header[34], header[35]]);
    let count = u32::from_be_bytes([header[36], header[37], header[38], header[39]]) as usize;

    let mut pages = Vec::new();
    let mut seen = std::collections::HashSet::new();
    while trunk != 0 && pages.len() <= count {
        assert!(seen.insert(trunk), "freelist trunk loop at page {trunk}");
        let mut buf = vec![0_u8; PAGE_SIZE as usize];
        file.seek(SeekFrom::Start(page_offset(trunk)))
            .expect("seek trunk");
        file.read_exact(&mut buf).expect("read trunk");
        pages.push(trunk);
        let next = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        let leaves = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize;
        assert!(leaves <= MAX_LEAF_ENTRIES, "trunk {trunk} leaf count {leaves}");
        for i in 0..leaves {
            let base = 8 + i * 4;
            pages.push(u32::from_be_bytes([
                buf[base],
                buf[base + 1],
                buf[base + 2],
                buf[base + 3],
            ]));
        }
        trunk = next;
    }
    pages
}

#[test]
fn gh410_durable_freelist_naming_the_lock_byte_page_is_detected_and_repairable() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("gh410_lock_byte.db");
        // Every page above the seed table's root is free, INCLUDING the
        // reserved lock-byte page — the shape observed on the damaged archive.
        let free: Vec<u32> = (3..=PAGE_COUNT).rev().collect();
        build_sparse_archive(&db_path, &free);
        assert!(durable_freelist(&db_path).contains(&LOCK_BYTE_PAGE));
        let db_str = db_path.to_string_lossy().into_owned();

        // Opening the archive runs the migration repair pass, which now
        // republishes the durable freelist without the reserved page. Before
        // that pass this exact image made `integrity_check` fail with
        // "page 262145 referenced by freelist trunk[1] is the reserved
        // lock-byte page", and `repair_orphaned_pages` refused to run.
        let conn = Connection::open(&db_str).await.unwrap();

        let after = texts(&conn.query("PRAGMA integrity_check;").await.unwrap());
        assert!(
            !after
                .iter()
                .any(|row| row.contains("reserved lock-byte page")),
            "repair must drop the reserved page from the freelist, got {after:?}"
        );

        let count = scalar_i64(&conn.query("PRAGMA freelist_count;").await.unwrap());
        assert_eq!(
            count,
            i64::from(PAGE_COUNT - 3),
            "the repaired header counts every free page except the reserved one"
        );

        // The explicit repair entry point is idempotent: the on-open pass
        // already dropped the only illegal entry.
        let again = scalar_i64(&conn.query("PRAGMA fsqlite.repair_freelist;").await.unwrap());
        assert_eq!(again, 0, "repair is idempotent");
        conn.close().await.unwrap();

        let durable = durable_freelist(&db_path);
        assert!(
            !durable.contains(&LOCK_BYTE_PAGE),
            "the repaired durable chain must not name the reserved page"
        );
        assert_eq!(
            durable.len(),
            (PAGE_COUNT - 3) as usize,
            "the repaired chain names every other free page exactly once"
        );
    });
}

#[test]
fn gh410_orphan_repair_and_write_churn_never_free_the_reserved_page() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("gh410_alloc.db");
        // The clean shape this time: every page above the seed root is free
        // and the reserved page is NOT on the durable list. The on-open
        // migration pass and the write churn below must both leave it that
        // way — before the fix, `repair_orphaned_pages` enumerated the
        // reserved page as an orphan (it is owned by nobody by design) and
        // freed it, which is how page 262145 reached the archive's freelist.
        let free: Vec<u32> = (3..=PAGE_COUNT).rev().filter(|p| *p != LOCK_BYTE_PAGE).collect();
        build_sparse_archive(&db_path, &free);
        assert!(!durable_freelist(&db_path).contains(&LOCK_BYTE_PAGE));
        let db_str = db_path.to_string_lossy().into_owned();

        let conn = Connection::open(&db_str).await.unwrap();
        conn.execute("CREATE TABLE t(k INTEGER PRIMARY KEY, v TEXT);")
            .await
            .unwrap();
        let payload = "x".repeat(3000);
        for k in 0..40_i64 {
            conn.execute(&format!("INSERT INTO t(k, v) VALUES ({k}, '{payload}');"))
                .await
                .unwrap();
        }
        // Free a run of pages and take them again, so the free/allocate round
        // trip runs against a freelist that spans the reserved page.
        conn.execute("DELETE FROM t WHERE k % 2 = 0;").await.unwrap();
        for k in 100..140_i64 {
            conn.execute(&format!("INSERT INTO t(k, v) VALUES ({k}, '{payload}');"))
                .await
                .unwrap();
        }
        assert_eq!(
            scalar_i64(&conn.query("SELECT COUNT(*) FROM t;").await.unwrap()),
            60,
            "the workload's rows all survived"
        );

        let rows = texts(&conn.query("PRAGMA integrity_check;").await.unwrap());
        assert_eq!(
            rows,
            vec!["ok".to_owned()],
            "the churned image must stay integrity-clean"
        );
        assert_eq!(
            scalar_i64(&conn.query("PRAGMA fsqlite.repair_freelist;").await.unwrap()),
            0,
            "nothing needed repairing: no path put the reserved page back"
        );
        conn.close().await.unwrap();

        let durable = durable_freelist(&db_path);
        assert!(
            !durable.contains(&LOCK_BYTE_PAGE),
            "the reserved page must not appear on the durable freelist"
        );

        // Stock SQLite reads the churned image and agrees.
        let stock = rusqlite::Connection::open(&db_path).unwrap();
        let verdict: String = stock
            .query_row("PRAGMA quick_check(20);", [], |row| row.get(0))
            .unwrap();
        assert_eq!(verdict, "ok", "stock quick_check must accept the image");
    });
}
