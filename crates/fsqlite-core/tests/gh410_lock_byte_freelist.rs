#![recursion_limit = "512"]

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
/// freelist is exactly `free` (which must contain unique page numbers).
///
/// Starts from a real, empty SQLite file (so page 1 is a genuine header plus
/// an empty `sqlite_master` leaf) and then rewrites the page-count and
/// freelist header fields and lays down the trunk chain, byte for byte the way
/// the engine's own serializer does.
fn build_sparse_archive(path: &Path, free: &[u32], journal_version: u8) {
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
    extend_sparse_archive(path, free, journal_version);
}

/// Preserve an existing two-page seed while installing the synthetic chain.
fn extend_sparse_archive(path: &Path, free: &[u32], journal_version: u8) {
    assert_eq!(std::fs::metadata(path).unwrap().len(), 2 * PAGE_SIZE);
    assert_eq!(
        free.iter()
            .copied()
            .collect::<std::collections::HashSet<_>>()
            .len(),
        free.len(),
        "the fixture freelist must contain unique pages"
    );
    assert!(matches!(journal_version, 1 | 2));

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
    assert_eq!(
        &header[..16],
        b"SQLite format 3\0",
        "fixture is a SQLite file"
    );
    // A checkpointed WAL database can have no WAL sidecar. Persist the mode
    // before the first FrankenSQLite open so its migration repair uses WAL.
    header[18] = journal_version;
    header[19] = journal_version;
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
        assert!(
            leaves <= MAX_LEAF_ENTRIES,
            "trunk {trunk} leaf count {leaves}"
        );
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
        build_sparse_archive(&db_path, &free, 1);
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
fn gh462_wal_open_repairs_reserved_freelist_trunks_and_leaves() {
    asupersync::test_utils::run_test(|| async {
        for reserved_is_leaf in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("gh462_wal_lock_byte.db");
            let mut free: Vec<u32> = (3..=PAGE_COUNT).rev().collect();
            if reserved_is_leaf {
                let last = free.len() - 1;
                free.swap(1, last);
            }
            build_sparse_archive(&db_path, &free, 2);
            assert!(durable_freelist(&db_path).contains(&LOCK_BYTE_PAGE));

            let db_str = db_path.to_string_lossy().into_owned();
            let conn = Connection::open(&db_str)
                .await
                .expect("WAL migration must publish reserved-page repair without BusySnapshot");
            assert_eq!(
                texts(&conn.query("PRAGMA journal_mode;").await.unwrap()),
                vec!["wal".to_owned()]
            );
            assert!(
                std::fs::metadata(format!("{db_str}-wal")).unwrap().len() > 32,
                "repair must traverse the real WAL publication path"
            );
            assert_eq!(
                scalar_i64(&conn.query("PRAGMA freelist_count;").await.unwrap()),
                i64::from(PAGE_COUNT - 3)
            );
            assert_eq!(
                scalar_i64(&conn.query("PRAGMA fsqlite.repair_freelist;").await.unwrap()),
                0,
                "the on-open repair must be idempotent"
            );
            conn.execute("BEGIN IMMEDIATE;").await.unwrap();
            conn.execute("COMMIT;").await.unwrap();
            conn.execute("INSERT INTO seed VALUES (462);")
                .await
                .unwrap();
            assert_eq!(
                texts(&conn.query("PRAGMA integrity_check;").await.unwrap()),
                vec!["ok".to_owned()]
            );
            conn.close().await.unwrap();

            let mut durable = durable_freelist(&db_path);
            durable.sort_unstable();
            let expected: Vec<u32> = (3..=PAGE_COUNT)
                .filter(|page| *page != LOCK_BYTE_PAGE)
                .collect();
            assert_eq!(
                durable, expected,
                "repair must preserve every legal free page"
            );

            let reopened = Connection::open(&db_str).await.unwrap();
            assert_eq!(
                scalar_i64(&reopened.query("SELECT x FROM seed;").await.unwrap()),
                462
            );
            assert_eq!(
                scalar_i64(
                    &reopened
                        .query("PRAGMA fsqlite.repair_freelist;")
                        .await
                        .unwrap()
                ),
                0
            );
            reopened.close().await.unwrap();
        }
    });
}

#[test]
fn gh462_schema_only_readers_leave_repair_for_a_real_writer() {
    fn durable_digest(path: &Path) -> Vec<Option<(u64, blake3::Hash)>> {
        ["", "-wal", "-journal"]
            .into_iter()
            .map(|suffix| {
                let mut name = path.as_os_str().to_owned();
                name.push(suffix);
                let mut file = match std::fs::File::open(Path::new(&name)) {
                    Ok(file) => file,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => return None,
                    Err(error) => panic!("open durable fixture: {error}"),
                };
                let size = file.metadata().unwrap().len();
                let mut hasher = blake3::Hasher::new();
                let mut buffer = vec![0_u8; 65536];
                loop {
                    let count = file.read(&mut buffer).unwrap();
                    if count == 0 {
                        break;
                    }
                    hasher.update(&buffer[..count]);
                }
                Some((size, hasher.finalize()))
            })
            .collect()
    }

    asupersync::test_utils::run_test(|| async {
        for reserved_is_leaf in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("gh462_schema_only.db");
            let db_str = db_path.to_string_lossy().into_owned();
            let seed = Connection::open(&db_str).await.unwrap();
            seed.execute("PRAGMA journal_mode=DELETE;").await.unwrap();
            seed.execute("CREATE TABLE seed(x);").await.unwrap();
            seed.execute("INSERT INTO seed VALUES (462), (463);")
                .await
                .unwrap();
            seed.close().await.unwrap();
            let mut free: Vec<u32> = (3..=PAGE_COUNT).rev().collect();
            if reserved_is_leaf {
                let last = free.len() - 1;
                free.swap(1, last);
            }
            extend_sparse_archive(&db_path, &free, 2);
            let before_open = durable_digest(&db_path);
            let damaged_free = durable_freelist(&db_path);
            assert!(damaged_free.contains(&LOCK_BYTE_PAGE));

            let conn = Connection::open_existing_schema_only(&db_str)
                .await
                .unwrap();
            // Writable schema-only open may initialize the WAL header, but
            // must preserve the main database and leave its repair pending.
            let before = durable_digest(&db_path);
            assert_eq!(
                before[0], before_open[0],
                "open must preserve the main file"
            );
            assert_eq!(durable_freelist(&db_path), damaged_free);
            let rows_before = conn.query("SELECT x FROM seed ORDER BY x;").await.unwrap();
            assert_eq!(rows_before.len(), 2);
            assert_eq!(rows_before[0].values(), &[SqliteValue::Integer(462)]);
            assert_eq!(rows_before[1].values(), &[SqliteValue::Integer(463)]);
            conn.execute("CREATE TABLE IF NOT EXISTS seed(x);")
                .await
                .unwrap();
            conn.execute("BEGIN CONCURRENT;").await.unwrap();
            let initial_seq = conn.current_concurrent_snapshot_seq().unwrap();
            assert_eq!(
                scalar_i64(&conn.query("SELECT COUNT(*) FROM seed;").await.unwrap()),
                2
            );
            conn.execute("COMMIT;").await.unwrap();
            assert_eq!(
                durable_digest(&db_path),
                before,
                "read/no-op paths must not publish a repair"
            );
            assert_eq!(
                durable_freelist(&db_path),
                damaged_free,
                "repair must remain pending"
            );

            // This eager writer really publishes the pending metadata, even
            // though the SQL transaction contains no row mutation.
            conn.execute("BEGIN IMMEDIATE;").await.unwrap();
            conn.execute("COMMIT;").await.unwrap();
            let repaired_seq = conn
                .last_local_commit_seq()
                .expect("repair commit sequence");
            assert!(
                repaired_seq > initial_seq,
                "repair must advance durable publication"
            );
            assert!(std::fs::metadata(format!("{db_str}-wal")).unwrap().len() > 32);
            assert_eq!(
                scalar_i64(&conn.query("PRAGMA fsqlite.repair_freelist;").await.unwrap()),
                0,
                "the eager writer must consume the pending repair"
            );
            let rows_after = conn.query("SELECT x FROM seed ORDER BY x;").await.unwrap();
            assert_eq!(
                rows_after.iter().map(Row::values).collect::<Vec<_>>(),
                rows_before.iter().map(Row::values).collect::<Vec<_>>()
            );
            conn.execute("BEGIN CONCURRENT;").await.unwrap();
            assert!(conn.current_concurrent_snapshot_seq().unwrap() >= repaired_seq);
            conn.execute("COMMIT;").await.unwrap();
            assert_eq!(
                texts(&conn.query("PRAGMA integrity_check;").await.unwrap()),
                vec!["ok".to_owned()]
            );
            conn.close().await.unwrap();
            let mut actual = durable_freelist(&db_path);
            actual.sort_unstable();
            let expected: Vec<_> = (3..=PAGE_COUNT)
                .filter(|page| *page != LOCK_BYTE_PAGE)
                .collect();
            assert_eq!(actual, expected);
            assert_eq!(
                std::fs::metadata(&db_path).unwrap().len(),
                u64::from(PAGE_COUNT) * PAGE_SIZE
            );
        }
    });
}

/// The GH#462 shape for the bd-70qrk negatives: a two-row seed whose sparse
/// archive's freelist names the reserved page, opened schema-only (the WAL
/// stays header-less and the repair pending), with a repairing transaction
/// open: BEGIN CONCURRENT and an INSERT.
async fn gh462_open_repairing_transaction(
    dir: &tempfile::TempDir,
) -> (std::path::PathBuf, Connection) {
    let db_path = dir.path().join("gh462_window.db");
    let db_str = db_path.to_string_lossy().into_owned();
    let seed = Connection::open(&db_str).await.unwrap();
    seed.execute("PRAGMA journal_mode=DELETE;").await.unwrap();
    seed.execute("CREATE TABLE seed(x);").await.unwrap();
    seed.execute("INSERT INTO seed VALUES (462), (463);")
        .await
        .unwrap();
    seed.close().await.unwrap();
    let free: Vec<u32> = (3..=PAGE_COUNT).rev().collect();
    extend_sparse_archive(&db_path, &free, 2);
    let conn = Connection::open_existing_schema_only(&db_str)
        .await
        .unwrap();
    conn.execute("BEGIN CONCURRENT;").await.unwrap();
    conn.execute("INSERT INTO seed VALUES (464);").await.unwrap();
    (db_path, conn)
}

/// The repairing COMMIT after a peer acted inside its window: refused with
/// BusySnapshot, nothing of it applied, the repair still pending for the next
/// writer, which then publishes it on top of the peer's state.
async fn gh462_assert_refused_then_repaired(
    conn: Connection,
    db_path: &Path,
    user_version: i64,
    why: &str,
) {
    let refused = conn.execute("COMMIT;").await;
    assert!(
        matches!(
            refused,
            Err(fsqlite_error::FrankenError::BusySnapshot { .. })
        ),
        "{why}: the repairing COMMIT must be refused: {refused:?}"
    );
    if conn.in_transaction() {
        conn.execute("ROLLBACK;").await.unwrap();
    }
    assert_eq!(
        scalar_i64(&conn.query("PRAGMA user_version;").await.unwrap()),
        user_version,
        "{why}: the refused commit must not overwrite the peer's page 1"
    );
    assert_eq!(
        scalar_i64(&conn.query("SELECT COUNT(*) FROM seed;").await.unwrap()),
        2,
        "{why}"
    );
    conn.execute("BEGIN IMMEDIATE;").await.unwrap();
    conn.execute("COMMIT;").await.unwrap();
    assert_eq!(
        scalar_i64(&conn.query("PRAGMA fsqlite.repair_freelist;").await.unwrap()),
        0,
        "{why}: the eager writer must consume the pending repair"
    );
    assert_eq!(
        scalar_i64(&conn.query("PRAGMA user_version;").await.unwrap()),
        user_version,
        "{why}"
    );
    assert_eq!(
        texts(&conn.query("PRAGMA integrity_check;").await.unwrap()),
        vec!["ok".to_owned()],
        "{why}"
    );
    conn.close().await.unwrap();
    let stock = rusqlite::Connection::open(db_path).unwrap();
    let stock_user_version: i64 = stock
        .query_row("PRAGMA user_version;", [], |row| row.get(0))
        .unwrap();
    assert_eq!(stock_user_version, user_version, "{why}");
    let quick: String = stock
        .query_row("PRAGMA quick_check;", [], |row| row.get(0))
        .unwrap();
    assert_eq!(quick, "ok", "{why}");
}

/// bd-70qrk negative: a COMMIT that writes the first header of a header-less
/// WAL is not refused for that header alone (the GH#462 keeper above), but a
/// real peer's commit in between still refuses it. Stock SQLite writes the
/// header itself and commits page 1 (`PRAGMA user_version`, which changes no
/// freelist byte) while the repairing transaction is open.
#[test]
fn gh462_a_peer_page_one_commit_still_refuses_the_repairing_commit() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let (db_path, conn) = gh462_open_repairing_transaction(&dir).await;
        {
            let stock = rusqlite::Connection::open(&db_path).unwrap();
            stock.pragma_update(None, "user_version", 7_i64).unwrap();
        }
        gh462_assert_refused_then_repaired(conn, &db_path, 7, "stock page-1 commit").await;
    });
}

/// bd-70qrk: a stock `wal_checkpoint(TRUNCATE)` inside the repairing
/// transaction's window cannot restart the shared WAL index. The
/// transaction's read holds its read mark, so stock answers busy and nothing
/// moves: the index header the read bound is still the one at the repair's
/// own header write, which is exactly what the fix checks. The COMMIT then
/// publishes the repair. Red on a build without the fix (the GH#462
/// refusal), green with it. It need not be red on the skip-check mutant: no
/// foreign generation change can happen here, and the two peer-commit cases
/// above and below cover the mutant.
#[test]
fn gh462_a_stock_wal_restart_cannot_run_inside_the_window() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let (db_path, conn) = gh462_open_repairing_transaction(&dir).await;
        let stock = rusqlite::Connection::open(&db_path).unwrap();
        stock.busy_timeout(std::time::Duration::ZERO).unwrap();
        let (busy, log, copied): (i64, i64, i64) = stock
            .query_row("PRAGMA wal_checkpoint(TRUNCATE);", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .unwrap();
        assert_eq!(
            busy, 1,
            "a stock restart must be refused while the repairing transaction reads \
             (busy={busy} log={log} copied={copied})"
        );
        drop(stock);
        conn.execute("COMMIT;").await.unwrap();
        assert_eq!(
            scalar_i64(&conn.query("PRAGMA fsqlite.repair_freelist;").await.unwrap()),
            0,
            "the commit published the pending repair"
        );
        assert_eq!(
            scalar_i64(&conn.query("SELECT COUNT(*) FROM seed;").await.unwrap()),
            3
        );
        assert_eq!(
            texts(&conn.query("PRAGMA integrity_check;").await.unwrap()),
            vec!["ok".to_owned()]
        );
        conn.close().await.unwrap();
        let stock = rusqlite::Connection::open(&db_path).unwrap();
        let quick: String = stock
            .query_row("PRAGMA quick_check;", [], |row| row.get(0))
            .unwrap();
        assert_eq!(quick, "ok");
    });
}

/// bd-70qrk negative: an fsqlite connection in the same process commits page
/// 1 inside the repairing transaction's window. The in-process publication
/// path must refuse the repairing COMMIT the same way.
#[test]
fn gh462_an_in_process_peer_commit_inside_the_window_still_refuses_the_repairing_commit() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let (db_path, conn) = gh462_open_repairing_transaction(&dir).await;
        let peer = Connection::open_existing_schema_only(db_path.to_string_lossy().into_owned())
            .await
            .unwrap();
        peer.execute("PRAGMA user_version = 7;").await.unwrap();
        peer.close().await.unwrap();
        gh462_assert_refused_then_repaired(conn, &db_path, 7, "in-process fsqlite peer").await;
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
        let free: Vec<u32> = (3..=PAGE_COUNT)
            .rev()
            .filter(|p| *p != LOCK_BYTE_PAGE)
            .collect();
        build_sparse_archive(&db_path, &free, 1);
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
        conn.execute("DELETE FROM t WHERE k % 2 = 0;")
            .await
            .unwrap();
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
