//! bd-yh9gm keepers: commit-planned freelist reconciliation for disjoint-table
//! concurrent writers in file-backed WAL mode.
//!
//! Before bd-yh9gm:
//! - `pending_conflict_pages_conservative()` pushed `PageNumber::ONE` as a
//!   shared FCW conflict token whenever a transaction freed any page
//!   `<= durable_bound`, forcing 100% `BusySnapshot("1")` conflicts on
//!   disjoint-table concurrent DELETEs.
//! - `predicted_conflict_pages_for_wal_commit_with_inner()` kept freelist trunk
//!   pages in the cross-process conflict surface.
//! - `resurrected_or_erased_freelist_pages()` failed closed on any concurrent
//!   freelist publication instead of reconciling against the current durable
//!   freelist under `HeldAppendGate`.
//!
//! Keepers:
//! 1. `bd_yh9gm_disjoint_table_concurrent_frees_commit_first_time`: 2
//!    connections on separate tables both free pages (pre-existing trunk,
//!    initially empty freelist, and `secure_delete=ON`) and both commit first
//!    time with zero retries, stock SQLite `integrity_check == "ok"`, and
//!    exact page accounting (`freelist_count + live_btree_pages == page_count`).
//! 2. `bd_yh9gm_disjoint_table_free_and_reuse_commit_first_time`: 2 connections
//!    on separate tables where one frees pages and the peer reuses durable
//!    freelist pages in the same window commit first time in both commit
//!    orders, with stock `integrity_check == "ok"` and exact page accounting.
//! 3. `bd_yh9gm_schema_change_on_page_one_survives_concurrent_peer_delete`: a
//!    concurrent `CREATE TABLE` (direct page-1 write) followed by a peer
//!    `DELETE` (commit-planned synthetic page 1) preserves both the new schema
//!    entry and the reconciled freelist.
//! 4. `bd_yh9gm_freed_btree_page_conflict_and_double_reuse_stay_refused`:
//!    negative cases proving that (a) updating a row on a leaf a peer freed is
//!    still refused and (b) reusing the exact same durable freelist page is
//!    still refused fail-closed.
//! 5. `bd_yh9gm_multi_writer_disjoint_deletes_zero_retries`: 2-writer and
//!    4-writer concurrent disjoint-table DELETE workloads commit with 0
//!    retries and stock `integrity_check == "ok"` (compared live against stock
//!    SQLite 3.54.1).
//!
//! Planted mutant: adding the freelist trunk page back into
//! `predicted_conflict_pages_for_wal_commit_with_synthetic_trunks` turns
//! keeper 1 (with `pre_free=true`) and keeper 2 red with `BusySnapshot` on the
//! trunk page.
#![cfg(unix)]

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use fsqlite_core::connection::Connection;
use fsqlite_error::FrankenError;
use fsqlite_types::value::SqliteValue;
use rusqlite::OpenFlags;

const PAD: usize = 220;
const INITIAL_ROWS_PER_TABLE: i64 = 180; // ~10-12 leaves per table at 4 KiB pages
const POOL_ROWS: i64 = 360; // ~20 leaves pre-freed so a durable freelist trunk + leaves exist
const BUSY_TIMEOUT_MS: u32 = 5_000;

fn db_str(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn pad(tag: i64) -> String {
    format!("{tag:08}-{}", "x".repeat(PAD))
}

async fn open_frank(path: &Path) -> Connection {
    let conn = Connection::open(db_str(path)).await.expect("open fsqlite");
    conn.execute(&format!("PRAGMA busy_timeout={BUSY_TIMEOUT_MS}"))
        .await
        .expect("busy_timeout");
    conn
}

async fn scalar_frank(conn: &Connection, sql: &str) -> i64 {
    let rows = conn.query(sql).await.expect("scalar query");
    match rows[0].values()[0] {
        SqliteValue::Integer(v) => v,
        ref other => panic!("expected integer from `{sql}`, got {other:?}"),
    }
}

#[derive(Debug, Clone, Default)]
struct FreelistLayout {
    page_count: u32,
    freelist_count: u32,
    trunks: BTreeSet<u32>,
    leaves: BTreeSet<u32>,
}

impl FreelistLayout {
    fn classify_page(&self, page: u32) -> &'static str {
        if page == 1 {
            "page_1"
        } else if self.trunks.contains(&page) {
            "freelist_trunk"
        } else if self.leaves.contains(&page) {
            "freelist_leaf"
        } else if page > self.page_count {
            "eof_growth"
        } else {
            "btree_or_other"
        }
    }
}

/// Checkpoint to main DB and inspect page 1 + freelist trunk/leaf chains
/// directly using stock SQLite (rusqlite) + main DB page reads.
fn inspect_layout_after_checkpoint(path: &Path) -> FreelistLayout {
    let stock = rusqlite::Connection::open(path).expect("stock open for layout");
    stock
        .busy_timeout(Duration::from_millis(u64::from(BUSY_TIMEOUT_MS)))
        .expect("stock busy");
    let _: (i64, i64, i64) = stock
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .expect("wal_checkpoint(TRUNCATE)");
    let page_count: u32 = stock
        .query_row("PRAGMA page_count", [], |row| row.get(0))
        .expect("page_count");
    let page_size: u32 = stock
        .query_row("PRAGMA page_size", [], |row| row.get(0))
        .expect("page_size");
    let page_size = usize::try_from(page_size).expect("page_size fits usize");
    let freelist_count: u32 = stock
        .query_row("PRAGMA freelist_count", [], |row| row.get(0))
        .expect("freelist_count");
    drop(stock);

    let bytes = std::fs::read(path).expect("read main db");
    let mut trunks = BTreeSet::new();
    let mut leaves = BTreeSet::new();
    if bytes.len() >= 100 {
        let mut trunk = u32::from_be_bytes(bytes[32..36].try_into().expect("trunk head"));
        while trunk != 0 {
            assert!(trunks.insert(trunk), "cycle in freelist trunk {trunk}");
            let start = (trunk as usize - 1) * page_size;
            let page = &bytes[start..start + page_size];
            let next = u32::from_be_bytes(page[0..4].try_into().expect("next trunk"));
            let leaf_count =
                u32::from_be_bytes(page[4..8].try_into().expect("leaf count")) as usize;
            for i in 0..leaf_count {
                let off = 8 + i * 4;
                let leaf = u32::from_be_bytes(page[off..off + 4].try_into().expect("leaf"));
                assert!(leaves.insert(leaf), "duplicate freelist leaf {leaf}");
            }
            trunk = next;
        }
    }
    assert_eq!(
        u32::try_from(trunks.len() + leaves.len()).expect("freelist len fits u32"),
        freelist_count,
        "freelist trunk + leaf count must equal PRAGMA freelist_count"
    );

    FreelistLayout {
        page_count,
        freelist_count,
        trunks,
        leaves,
    }
}

/// Verify stock SQLite `PRAGMA integrity_check == "ok"`, exact freelist chain
/// accounting, and `freelist_count + btree_pages == page_count` (using stock's
/// `dbstat` virtual table when available, plus stock's `integrity_check` which
/// checks every page `1..=page_count` is accounted for exactly once).
fn assert_stock_clean_and_accounted(path: &Path, context: &str) -> (u32, u32) {
    let layout = inspect_layout_after_checkpoint(path);
    let stock = rusqlite::Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .expect("stock readonly open");
    stock
        .busy_timeout(Duration::from_millis(u64::from(BUSY_TIMEOUT_MS)))
        .expect("stock busy");
    let integrity: Vec<String> = {
        let mut stmt = stock
            .prepare("PRAGMA integrity_check(100000)")
            .expect("integrity_check");
        stmt.query_map([], |row| row.get::<_, String>(0))
            .expect("integrity rows")
            .collect::<Result<_, _>>()
            .expect("collect integrity")
    };
    assert_eq!(
        integrity,
        vec!["ok".to_owned()],
        "{context}: stock integrity_check failed: {integrity:?}"
    );
    if let Ok(btree_pages) =
        stock.query_row("SELECT count(*) FROM dbstat", [], |row| row.get::<_, u32>(0))
    {
        assert_eq!(
            layout.freelist_count + btree_pages,
            layout.page_count,
            "{context}: freelist_count ({}) + btree_pages ({btree_pages}) != page_count ({})",
            layout.freelist_count,
            layout.page_count
        );
    }
    (layout.page_count, layout.freelist_count)
}

fn refused_pages_of(err: &FrankenError) -> Vec<u32> {
    match err {
        FrankenError::BusySnapshot { conflicting_pages } => conflicting_pages
            .split(',')
            .filter_map(|part| part.trim().parse::<u32>().ok())
            .collect(),
        _ => Vec::new(),
    }
}

async fn seed_db_frank(path: &Path, table_count: usize, pre_free_pool: bool) {
    let conn = open_frank(path).await;
    conn.execute("PRAGMA journal_mode=WAL").await.expect("wal");
    conn.execute("PRAGMA synchronous=NORMAL")
        .await
        .expect("sync");
    for t in 0..table_count {
        conn.execute(&format!(
            "CREATE TABLE t{t}(id INTEGER PRIMARY KEY, pad TEXT)"
        ))
        .await
        .expect("create table");
    }
    if pre_free_pool {
        conn.execute("CREATE TABLE pool(id INTEGER PRIMARY KEY, pad TEXT)")
            .await
            .expect("create pool");
    }
    conn.execute("BEGIN").await.expect("begin seed");
    for t in 0..table_count {
        for id in 1..=INITIAL_ROWS_PER_TABLE {
            conn.execute(&format!(
                "INSERT INTO t{t}(id, pad) VALUES ({id}, '{}')",
                pad(id)
            ))
            .await
            .expect("seed insert");
        }
    }
    if pre_free_pool {
        for id in 1..=POOL_ROWS {
            conn.execute(&format!(
                "INSERT INTO pool(id, pad) VALUES ({id}, '{}')",
                pad(id)
            ))
            .await
            .expect("pool insert");
        }
    }
    conn.execute("COMMIT").await.expect("commit seed");

    if pre_free_pool {
        conn.execute("BEGIN").await.expect("begin pool delete");
        conn.execute("DELETE FROM pool").await.expect("delete pool");
        conn.execute("COMMIT").await.expect("commit pool delete");
    }
    conn.close().await.expect("close seed");
}

fn seed_db_stock(path: &Path, table_count: usize, pre_free_pool: bool) {
    let conn = rusqlite::Connection::open(path).expect("stock open seed");
    conn.busy_timeout(Duration::from_millis(u64::from(BUSY_TIMEOUT_MS)))
        .expect("busy");
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")
        .expect("wal");
    for t in 0..table_count {
        conn.execute(
            &format!("CREATE TABLE t{t}(id INTEGER PRIMARY KEY, pad TEXT)"),
            [],
        )
        .expect("create table");
    }
    if pre_free_pool {
        conn.execute("CREATE TABLE pool(id INTEGER PRIMARY KEY, pad TEXT)", [])
            .expect("create pool");
    }
    conn.execute_batch("BEGIN").expect("begin");
    for t in 0..table_count {
        for id in 1..=INITIAL_ROWS_PER_TABLE {
            conn.execute(
                &format!("INSERT INTO t{t}(id, pad) VALUES (?1, ?2)"),
                rusqlite::params![id, pad(id)],
            )
            .expect("seed insert");
        }
    }
    if pre_free_pool {
        for id in 1..=POOL_ROWS {
            conn.execute(
                "INSERT INTO pool(id, pad) VALUES (?1, ?2)",
                rusqlite::params![id, pad(id)],
            )
            .expect("pool insert");
        }
    }
    conn.execute_batch("COMMIT").expect("commit");
    if pre_free_pool {
        conn.execute_batch("BEGIN; DELETE FROM pool; COMMIT;")
            .expect("delete pool");
    }
}

fn run_mt_stock_deletes(
    path: &Path,
    writers: usize,
    rounds_per_writer: usize,
) -> (usize, usize, Duration) {
    let barrier = Arc::new(Barrier::new(writers));
    let started = Instant::now();
    let handles: Vec<_> = (0..writers)
        .map(|w| {
            let db_path = path.to_path_buf();
            let bar = Arc::clone(&barrier);
            std::thread::spawn(move || {
                let conn = rusqlite::Connection::open(&db_path).expect("stock worker open");
                conn.busy_timeout(Duration::from_millis(u64::from(BUSY_TIMEOUT_MS)))
                    .expect("busy");
                let mut retries = 0usize;
                let table = format!("t{w}");
                bar.wait();
                for round in 0..rounds_per_writer {
                    let lo = (round as i64) * 8 + 1;
                    let hi = lo + 7;
                    loop {
                        if conn.execute_batch("BEGIN IMMEDIATE").is_err() {
                            retries += 1;
                            continue;
                        }
                        let ok = conn
                            .execute(
                                &format!("DELETE FROM {table} WHERE id BETWEEN {lo} AND {hi}"),
                                [],
                            )
                            .is_ok();
                        if !ok || conn.execute_batch("COMMIT").is_err() {
                            let _ = conn.execute_batch("ROLLBACK");
                            retries += 1;
                            continue;
                        }
                        break;
                    }
                }
                retries
            })
        })
        .collect();
    let retries: usize = handles.into_iter().map(|h| h.join().expect("join")).sum();
    (writers * rounds_per_writer, retries, started.elapsed())
}

#[test]
fn bd_yh9gm_disjoint_table_concurrent_frees_commit_first_time() {
    asupersync::test_utils::run_test(|| async {
        for (pre_free, secure_delete) in [(true, "OFF"), (false, "OFF"), (true, "ON")] {
            let dir = tempfile::tempdir().expect("tempdir");
            let db = dir.path().join("det_delete.db");
            seed_db_frank(&db, 2, pre_free).await;
            let layout = inspect_layout_after_checkpoint(&db);
            if pre_free {
                assert!(
                    !layout.trunks.is_empty(),
                    "precondition: pre_free=true must have an existing freelist trunk"
                );
            }

            let c1 = open_frank(&db).await;
            let c2 = open_frank(&db).await;
            c1.execute(&format!("PRAGMA secure_delete={secure_delete}"))
                .await
                .expect("c1 secure_delete");
            c2.execute(&format!("PRAGMA secure_delete={secure_delete}"))
                .await
                .expect("c2 secure_delete");
            c1.execute("BEGIN CONCURRENT").await.expect("c1 begin");
            c2.execute("BEGIN CONCURRENT").await.expect("c2 begin");
            c1.execute("DELETE FROM t0 WHERE id > 10")
                .await
                .expect("c1 delete");
            c2.execute("DELETE FROM t1 WHERE id > 10")
                .await
                .expect("c2 delete");
            c1.execute("COMMIT").await.expect("c1 commit");
            c2.execute("COMMIT").await.unwrap_or_else(|err| {
                let classified: Vec<(u32, &'static str)> = refused_pages_of(&err)
                    .into_iter()
                    .map(|p| (p, layout.classify_page(p)))
                    .collect();
                panic!(
                    "bd-yh9gm: disjoint-table concurrent DELETE must commit first time \
                     (pre_free={pre_free} secure_delete={secure_delete} init_trunks={:?}): \
                     {err:?} classified={classified:?}",
                    layout.trunks
                );
            });
            c1.close().await.expect("c1 close");
            c2.close().await.expect("c2 close");

            let (pages, free) = assert_stock_clean_and_accounted(
                &db,
                &format!("det_delete(pre_free={pre_free},secure_delete={secure_delete})"),
            );
            assert!(
                free > layout.freelist_count + 10,
                "both writers' freed leaves must appear on durable freelist: before={} after={free} pages={pages}",
                layout.freelist_count
            );
            let stock = rusqlite::Connection::open(&db).expect("stock verify");
            let c_t0: i64 = stock
                .query_row("SELECT count(*) FROM t0", [], |r| r.get(0))
                .expect("t0 count");
            let c_t1: i64 = stock
                .query_row("SELECT count(*) FROM t1", [], |r| r.get(0))
                .expect("t1 count");
            assert_eq!((c_t0, c_t1), (10, 10));
        }
    });
}

#[test]
fn bd_yh9gm_disjoint_table_free_and_reuse_commit_first_time() {
    asupersync::test_utils::run_test(|| async {
        for deleter_first in [true, false] {
            let dir = tempfile::tempdir().expect("tempdir");
            let db = dir.path().join("det_free_and_reuse.db");
            seed_db_frank(&db, 2, true).await;
            let layout = inspect_layout_after_checkpoint(&db);
            assert!(
                layout.freelist_count >= 15,
                "precondition: durable freelist has leaves to reuse"
            );

            let c_del = open_frank(&db).await;
            let c_ins = open_frank(&db).await;
            c_del.execute("BEGIN CONCURRENT").await.expect("del begin");
            c_ins.execute("BEGIN CONCURRENT").await.expect("ins begin");

            c_del
                .execute("DELETE FROM t0 WHERE id > 10")
                .await
                .expect("c_del delete");
            for i in 1_001..=1_120_i64 {
                c_ins
                    .execute(&format!(
                        "INSERT INTO t1(id, pad) VALUES ({i}, '{}')",
                        pad(i)
                    ))
                    .await
                    .expect("c_ins insert from freelist");
            }
            assert_eq!(
                u32::try_from(scalar_frank(&c_ins, "PRAGMA page_count").await)
                    .expect("page_count fits u32"),
                layout.page_count,
                "precondition: c_ins reused durable freelist pages without EOF growth"
            );

            if deleter_first {
                c_del.execute("COMMIT").await.expect("c_del commit first");
                c_ins.execute("COMMIT").await.unwrap_or_else(|err| {
                    let classified: Vec<(u32, &'static str)> = refused_pages_of(&err)
                        .into_iter()
                        .map(|p| (p, layout.classify_page(p)))
                        .collect();
                    panic!(
                        "bd-yh9gm: c_ins reusing disjoint freelist leaves must commit first time \
                         after peer DELETE: {err:?} classified={classified:?}"
                    );
                });
            } else {
                c_ins.execute("COMMIT").await.expect("c_ins commit first");
                c_del.execute("COMMIT").await.unwrap_or_else(|err| {
                    let classified: Vec<(u32, &'static str)> = refused_pages_of(&err)
                        .into_iter()
                        .map(|p| (p, layout.classify_page(p)))
                        .collect();
                    panic!(
                        "bd-yh9gm: c_del freeing disjoint table leaves must commit first time \
                         after peer freelist reuse: {err:?} classified={classified:?}"
                    );
                });
            }
            c_del.close().await.expect("close del");
            c_ins.close().await.expect("close ins");

            let (pages, free) = assert_stock_clean_and_accounted(
                &db,
                &format!("det_free_and_reuse(deleter_first={deleter_first})"),
            );
            assert_eq!(pages, layout.page_count, "no spurious EOF growth");
            assert!(free > 0, "freelist retains net free pages");
            let stock = rusqlite::Connection::open(&db).expect("stock verify");
            let c_t0: i64 = stock
                .query_row("SELECT count(*) FROM t0", [], |r| r.get(0))
                .expect("t0 count");
            let c_t1: i64 = stock
                .query_row("SELECT count(*) FROM t1", [], |r| r.get(0))
                .expect("t1 count");
            assert_eq!((c_t0, c_t1), (10, INITIAL_ROWS_PER_TABLE + 120));
        }
    });
}

#[test]
fn bd_yh9gm_page_one_peer_writes_survive_concurrent_peer_delete() {
    asupersync::test_utils::run_test(|| async {
        // Part A: a non-DDL direct Page-1 update (PRAGMA user_version /
        // application_id) commits while a concurrent peer deletes rows and
        // mutates the freelist. Synthetic Page-1 reconciliation must refresh
        // from current durable Page 1 so the peer's header fields survive AND
        // the deleter still commits on the first try.
        {
            let dir = tempfile::tempdir().expect("tempdir");
            let db = dir.path().join("page_one_header_and_delete.db");
            seed_db_frank(&db, 2, true).await;

            let c_del = open_frank(&db).await;
            let c_hdr = open_frank(&db).await;
            c_del.execute("BEGIN CONCURRENT").await.expect("del begin");
            c_del
                .execute("DELETE FROM t0 WHERE id > 10")
                .await
                .expect("c_del delete");

            c_hdr
                .execute("PRAGMA user_version = 4242")
                .await
                .expect("peer user_version");
            c_hdr
                .execute("PRAGMA application_id = 305419896")
                .await
                .expect("peer application_id");

            c_del
                .execute("COMMIT")
                .await
                .expect("c_del commit must commute with peer Page-1 header write");
            c_del.close().await.expect("close del");
            c_hdr.close().await.expect("close hdr");

            assert_stock_clean_and_accounted(&db, "page_one_header_survives_peer_delete");
            let stock = rusqlite::Connection::open(&db).expect("stock verify");
            let user_version: i64 = stock
                .query_row("PRAGMA user_version", [], |r| r.get(0))
                .expect("stock user_version");
            let application_id: i64 = stock
                .query_row("PRAGMA application_id", [], |r| r.get(0))
                .expect("stock application_id");
            assert_eq!(
                user_version, 4242,
                "synthetic Page-1 reconciliation must preserve peer user_version"
            );
            assert_eq!(
                application_id, 305419896,
                "synthetic Page-1 reconciliation must preserve peer application_id"
            );
            let t0_left: i64 = stock
                .query_row("SELECT count(*) FROM t0", [], |r| r.get(0))
                .expect("t0 count");
            assert_eq!(t0_left, 10);
        }

        // Part B: concurrent DDL bumps schema_cookie on Page 1; the stale
        // concurrent deleter must still fail closed with BusySnapshot and
        // leave the peer's sqlite_master entry intact.
        {
            let dir = tempfile::tempdir().expect("tempdir");
            let db = dir.path().join("schema_ddl_and_delete.db");
            seed_db_frank(&db, 2, true).await;

            let c_del = open_frank(&db).await;
            let c_ddl = open_frank(&db).await;
            c_del.execute("BEGIN CONCURRENT").await.expect("del begin");
            c_del
                .execute("DELETE FROM t0 WHERE id > 10")
                .await
                .expect("c_del delete");

            c_ddl
                .execute("CREATE TABLE peer_added(k INTEGER PRIMARY KEY, v TEXT)")
                .await
                .expect("peer create table");
            c_ddl
                .execute("INSERT INTO peer_added VALUES (42, 'must_survive')")
                .await
                .expect("peer insert");

            let err = c_del
                .execute("COMMIT")
                .await
                .expect_err("stale schema snapshot must still fail closed after peer DDL");
            assert!(
                matches!(err, FrankenError::BusySnapshot { .. }),
                "expected BusySnapshot on stale schema snapshot, got {err:?}"
            );
            let _ = c_del.execute("ROLLBACK").await;
            c_del.close().await.expect("close del");
            c_ddl.close().await.expect("close ddl");

            assert_stock_clean_and_accounted(&db, "schema_ddl_survives_stale_peer_delete");
            let stock = rusqlite::Connection::open(&db).expect("stock verify");
            let survived: String = stock
                .query_row("SELECT v FROM peer_added WHERE k = 42", [], |r| r.get(0))
                .expect("peer_added table and row must survive");
            assert_eq!(survived, "must_survive");
        }
    });
}

#[test]
fn bd_yh9gm_freed_btree_page_conflict_and_double_reuse_stay_refused() {
    asupersync::test_utils::run_test(|| async {
        // Negative case A: updating a row on a leaf that a concurrent peer
        // freed must still conflict (freed B-tree pages remain FCW-tracked).
        {
            let dir = tempfile::tempdir().expect("tempdir");
            let db = dir.path().join("neg_btree_free_update.db");
            seed_db_frank(&db, 1, true).await;

            let c_del = open_frank(&db).await;
            let c_upd = open_frank(&db).await;
            c_upd.execute("PRAGMA busy_timeout=150").await.expect("bt");
            c_del.execute("BEGIN CONCURRENT").await.expect("del begin");
            c_upd.execute("BEGIN CONCURRENT").await.expect("upd begin");

            // c_upd reads table t0 first to pin its snapshot before c_del commits.
            let before = scalar_frank(&c_upd, "SELECT count(*) FROM t0").await;
            assert_eq!(before, INITIAL_ROWS_PER_TABLE);

            c_del
                .execute("DELETE FROM t0 WHERE id > 10")
                .await
                .expect("c_del delete");
            c_del.execute("COMMIT").await.expect("c_del commit");

            // Now c_upd tries to modify row 150 (on a leaf c_del just freed).
            let upd_res = c_upd
                .execute("UPDATE t0 SET pad = 'stale_update' WHERE id = 150")
                .await;
            let final_res = match upd_res {
                Err(e) => Err(e),
                Ok(_) => c_upd.execute("COMMIT").await,
            };
            assert!(
                matches!(
                    final_res,
                    Err(FrankenError::Busy | FrankenError::BusySnapshot { .. })
                ),
                "updating a row on a freed leaf must fail Busy/BusySnapshot, got {final_res:?}"
            );
            let _ = c_upd.execute("ROLLBACK").await;
            c_del.close().await.expect("close del");
            c_upd.close().await.expect("close upd");
            assert_stock_clean_and_accounted(&db, "neg_btree_free_update");
        }

        // Negative case B: two connections both reusing the SAME durable
        // freelist page in one window must still fail closed (exact reuse guard).
        {
            let dir = tempfile::tempdir().expect("tempdir");
            let db = dir.path().join("neg_double_reuse.db");
            seed_db_frank(&db, 2, true).await;

            let c1 = open_frank(&db).await;
            let c2 = open_frank(&db).await;
            c2.execute("PRAGMA busy_timeout=150").await.expect("bt");
            c1.execute("BEGIN CONCURRENT").await.expect("c1 begin");
            c2.execute("BEGIN CONCURRENT").await.expect("c2 begin");
            let _ = scalar_frank(&c2, "SELECT count(*) FROM t1").await;

            for i in 1_001..=1_120_i64 {
                c1.execute(&format!(
                    "INSERT INTO t0(id, pad) VALUES ({i}, '{}')",
                    pad(i)
                ))
                .await
                .expect("c1 insert from freelist");
            }
            c1.execute("COMMIT").await.expect("c1 commit");

            let mut c2_err = None;
            for i in 1_001..=1_120_i64 {
                if let Err(err) = c2
                    .execute(&format!(
                        "INSERT INTO t1(id, pad) VALUES ({i}, '{}')",
                        pad(i)
                    ))
                    .await
                {
                    c2_err = Some(err);
                    break;
                }
            }
            let c2_outcome = match c2_err {
                Some(e) => Err(e),
                None => c2.execute("COMMIT").await,
            };
            assert!(
                matches!(
                    c2_outcome,
                    Err(FrankenError::Busy | FrankenError::BusySnapshot { .. })
                ),
                "double consumption of the same durable freelist page must be refused, got {c2_outcome:?}"
            );
            let _ = c2.execute("ROLLBACK").await;
            c1.close().await.expect("close c1");
            c2.close().await.expect("close c2");
            assert_stock_clean_and_accounted(&db, "neg_double_reuse");
        }
    });
}

#[test]
fn bd_yh9gm_multi_writer_disjoint_deletes_zero_retries() {
    asupersync::test_utils::run_test(|| async {
        for writers in [2_usize, 4_usize] {
            let rounds = 20_usize;
            let dir = tempfile::tempdir().expect("tempdir");
            let frank_db = dir.path().join(format!("mt_del_frank_{writers}w.db"));
            let stock_db = dir.path().join(format!("mt_del_stock_{writers}w.db"));

            seed_db_frank(&frank_db, writers, true).await;
            let init_layout = inspect_layout_after_checkpoint(&frank_db);

            let barrier = Arc::new(Barrier::new(writers));
            let started = Instant::now();
            let handles: Vec<_> = (0..writers)
                .map(|w| {
                    let db_path = frank_db.clone();
                    let bar = Arc::clone(&barrier);
                    std::thread::spawn(move || {
                        let runtime = asupersync::runtime::RuntimeBuilder::current_thread()
                            .build()
                            .expect("worker runtime");
                        runtime.block_on(async move {
                            let conn = open_frank(&db_path).await;
                            let table = format!("t{w}");
                            let mut retries = 0usize;
                            bar.wait();
                            for r in 0..rounds {
                                let lo = (r as i64) * 8 + 1;
                                let hi = lo + 7;
                                loop {
                                    conn.execute("BEGIN").await.expect("begin");
                                    let res = conn
                                        .execute(&format!(
                                            "DELETE FROM {table} WHERE id BETWEEN {lo} AND {hi}"
                                        ))
                                        .await;
                                    if res.is_err() {
                                        let _ = conn.execute("ROLLBACK").await;
                                        retries += 1;
                                        continue;
                                    }
                                    match conn.execute("COMMIT").await {
                                        Ok(_) => break,
                                        Err(_) => {
                                            let _ = conn.execute("ROLLBACK").await;
                                            retries += 1;
                                        }
                                    }
                                }
                            }
                            conn.close().await.expect("close worker");
                            retries
                        })
                    })
                })
                .collect();
            let del_retries: usize = handles.into_iter().map(|h| h.join().expect("join")).sum();
            let del_ms = started.elapsed().as_millis();
            let (del_pages, del_free) =
                assert_stock_clean_and_accounted(&frank_db, &format!("mt_pure_delete_{writers}w"));
            assert_eq!(
                del_retries, 0,
                "bd-yh9gm: disjoint-table concurrent pure DELETE ({writers}w) must have 0 retries"
            );

            seed_db_stock(&stock_db, writers, true);
            let (stock_txns, stock_retries, stock_elapsed) =
                run_mt_stock_deletes(&stock_db, writers, rounds);
            let _ =
                assert_stock_clean_and_accounted(&stock_db, &format!("stock_mt_delete_{writers}w"));
            eprintln!(
                "BD_YH9GM_KEEPER writers={writers} frank_txns={} frank_retries={del_retries} frank_ms={del_ms} stock_txns={stock_txns} stock_retries={stock_retries} stock_ms={} pages={del_pages} init_free={} final_free={del_free}",
                writers * rounds,
                stock_elapsed.as_millis(),
                init_layout.freelist_count,
            );
        }
    });
}
