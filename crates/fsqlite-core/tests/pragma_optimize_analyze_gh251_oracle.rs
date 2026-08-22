//! GH #251 (bd-gh-pragma-optimize-analyze): `PRAGMA optimize` was unimplemented
//! — it fell through to the unknown-pragma no-op, so it never ran ANALYZE and
//! `sqlite_stat1` was never created, making a follow-up `SELECT * FROM
//! sqlite_stat1` fail with "no such table". The statistics machinery itself
//! (plain `ANALYZE`) was already complete and byte-identical to stock.
//!
//! The fix routes `PRAGMA optimize(MASK)` through the existing `execute_analyze`
//! machinery for the tables that would benefit (those holding at least one
//! row), honoring the SQLite >= 3.46 mask bits: 0x0001 = debug (list the
//! ANALYZE statements that would run without executing them), 0x0002 = run
//! ANALYZE, default (no argument) = 0xfffe.
//!
//! Oracle: bundled rusqlite (the parity target named in the issue, SQLite
//! 3.5x), NOT the system sqlite3 CLI. Rows are populated with a recursive CTE
//! rather than `generate_series` because bundled rusqlite lacks that TVF.

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

const POPULATE: &str = "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT);\
                        CREATE INDEX idx_b ON t(b);\
                        INSERT INTO t(b) WITH RECURSIVE c(x) AS (\
                            SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 100\
                        ) SELECT 'row' || x FROM c;";

fn cell_text(v: &SqliteValue) -> Option<String> {
    match v {
        SqliteValue::Text(s) => Some(s.as_ref().to_owned()),
        SqliteValue::Null => None,
        SqliteValue::Integer(n) => Some(n.to_string()),
        other => panic!("unexpected cell {other:?}"),
    }
}

async fn frank_rows(conn: &Connection, sql: &str) -> Vec<Row> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("frank error on `{sql}`: {e:?}"))
}

/// Sorted `(tbl, idx, stat)` tuples of frank's `sqlite_stat1`.
async fn frank_stat1(conn: &Connection) -> Vec<(String, Option<String>, String)> {
    let mut out: Vec<(String, Option<String>, String)> =
        frank_rows(conn, "SELECT tbl, idx, stat FROM sqlite_stat1")
            .await
            .iter()
            .map(|row| {
                let v = row.values();
                (
                    cell_text(&v[0]).expect("tbl is text"),
                    cell_text(&v[1]),
                    cell_text(&v[2]).expect("stat is text"),
                )
            })
            .collect();
    out.sort();
    out
}

async fn frank_stat1_exists(conn: &Connection) -> bool {
    let rows = frank_rows(
        conn,
        "SELECT count(*) FROM sqlite_master WHERE name = 'sqlite_stat1'",
    )
    .await;
    matches!(rows[0].values()[0], SqliteValue::Integer(n) if n > 0)
}

fn rusqlite_stat1(conn: &rusqlite::Connection) -> Vec<(String, Option<String>, String)> {
    let mut stmt = conn
        .prepare("SELECT tbl, idx, stat FROM sqlite_stat1")
        .unwrap();
    let mut out: Vec<(String, Option<String>, String)> = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .unwrap()
        .map(Result::unwrap)
        .collect();
    out.sort();
    out
}

fn rusqlite_stat1_exists(conn: &rusqlite::Connection) -> bool {
    conn.query_row(
        "SELECT count(*) FROM sqlite_master WHERE name = 'sqlite_stat1'",
        [],
        |row| row.get::<_, i64>(0),
    )
    .unwrap()
        > 0
}

/// The headline fix: `PRAGMA optimize(0x10002)` on a populated database
/// materializes `sqlite_stat1` exactly as stock SQLite does.
#[test]
fn optimize_mask_materializes_stat1_matching_rusqlite() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute(POPULATE).await.unwrap();
        // Before: sqlite_stat1 does not exist.
        assert!(!frank_stat1_exists(&conn).await);
        frank_rows(&conn, "PRAGMA optimize(0x10002)").await;
        assert!(
            frank_stat1_exists(&conn).await,
            "optimize(0x10002) must create sqlite_stat1"
        );
        let frank = frank_stat1(&conn).await;

        let r = rusqlite::Connection::open_in_memory().unwrap();
        r.execute_batch(POPULATE).unwrap();
        r.execute_batch("PRAGMA optimize(0x10002)").unwrap();
        let sqlite = rusqlite_stat1(&r);

        assert_eq!(
            frank, sqlite,
            "sqlite_stat1 must match stock after optimize"
        );
        assert!(!frank.is_empty(), "sqlite_stat1 must be non-empty");
    });
}

/// The no-argument default mask (0xfffe) also analyzes and matches stock, and
/// the result is identical to what plain `ANALYZE` produces (frank-internal).
#[test]
fn optimize_default_mask_matches_analyze_and_rusqlite() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute(POPULATE).await.unwrap();
        frank_rows(&conn, "PRAGMA optimize").await;
        let via_optimize = frank_stat1(&conn).await;

        // Plain ANALYZE on a fresh identical DB yields the same stat1 content.
        let conn2 = Connection::open(":memory:").await.unwrap();
        conn2.execute(POPULATE).await.unwrap();
        conn2.execute("ANALYZE").await.unwrap();
        let via_analyze = frank_stat1(&conn2).await;
        assert_eq!(
            via_optimize, via_analyze,
            "optimize must match plain ANALYZE"
        );

        let r = rusqlite::Connection::open_in_memory().unwrap();
        r.execute_batch(POPULATE).unwrap();
        r.execute_batch("PRAGMA optimize").unwrap();
        assert_eq!(
            via_optimize,
            rusqlite_stat1(&r),
            "default optimize vs stock"
        );
    });
}

/// `PRAGMA optimize(0)` (analyze bit clear) analyzes nothing and must NOT create
/// `sqlite_stat1`, matching stock.
#[test]
fn optimize_zero_mask_does_not_create_stat1() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute(POPULATE).await.unwrap();
        frank_rows(&conn, "PRAGMA optimize(0)").await;
        assert!(
            !frank_stat1_exists(&conn).await,
            "optimize(0) must not create sqlite_stat1"
        );

        let r = rusqlite::Connection::open_in_memory().unwrap();
        r.execute_batch(POPULATE).unwrap();
        r.execute_batch("PRAGMA optimize(0)").unwrap();
        assert!(
            !rusqlite_stat1_exists(&r),
            "stock optimize(0) creates nothing"
        );
    });
}

/// On an all-empty database `PRAGMA optimize` analyzes nothing worth analyzing,
/// so `sqlite_stat1` is never created — matching stock (and frank's prior
/// silent no-op did NOT regress on this edge).
#[test]
fn optimize_on_empty_db_creates_no_stat1() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT); CREATE INDEX idx_b ON t(b);")
            .await
            .unwrap();
        frank_rows(&conn, "PRAGMA optimize").await;
        assert!(
            !frank_stat1_exists(&conn).await,
            "optimize on empty DB must not create sqlite_stat1"
        );

        let r = rusqlite::Connection::open_in_memory().unwrap();
        r.execute_batch(
            "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT); CREATE INDEX idx_b ON t(b);",
        )
        .unwrap();
        r.execute_batch("PRAGMA optimize").unwrap();
        assert!(
            !rusqlite_stat1_exists(&r),
            "stock optimize on empty DB creates nothing"
        );
    });
}

/// Debug mode (`mask & 0x0001`) lists the ANALYZE statements that WOULD run,
/// one per beneficial table, WITHOUT modifying the database. `PRAGMA
/// optimize(-1)` sets every bit including debug and analyze.
#[test]
fn optimize_debug_mode_lists_without_executing() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute(POPULATE).await.unwrap();
        conn.execute(
            "CREATE TABLE t2(a INTEGER PRIMARY KEY, b TEXT); INSERT INTO t2(b) VALUES('x');",
        )
        .await
        .unwrap();

        let mut frank: Vec<String> = frank_rows(&conn, "PRAGMA optimize(-1)")
            .await
            .iter()
            .map(|row| cell_text(&row.values()[0]).expect("debug row is text"))
            .collect();
        frank.sort();
        // Debug mode must NOT have created sqlite_stat1.
        assert!(
            !frank_stat1_exists(&conn).await,
            "debug optimize must not modify the database"
        );

        let r = rusqlite::Connection::open_in_memory().unwrap();
        r.execute_batch(POPULATE).unwrap();
        r.execute_batch(
            "CREATE TABLE t2(a INTEGER PRIMARY KEY, b TEXT); INSERT INTO t2(b) VALUES('x');",
        )
        .unwrap();
        let mut sqlite: Vec<String> = {
            let mut stmt = r.prepare("PRAGMA optimize(-1)").unwrap();
            let v = stmt
                .query_map([], |row| row.get::<_, String>(0))
                .unwrap()
                .map(Result::unwrap)
                .collect::<Vec<_>>();
            v
        };
        sqlite.sort();

        assert_eq!(frank, sqlite, "debug-mode ANALYZE listing must match stock");
        assert_eq!(
            frank,
            vec![
                "ANALYZE \"main\".\"t\"".to_owned(),
                "ANALYZE \"main\".\"t2\"".to_owned(),
            ],
            "both non-empty tables must be listed"
        );
        assert!(
            !rusqlite_stat1_exists(&r),
            "stock debug optimize creates nothing"
        );
    });
}

/// Control: `PRAGMA optimize(1)` is debug-only (analyze bit clear) — it lists
/// nothing and creates nothing, and plain `ANALYZE` still works unchanged.
#[test]
fn optimize_debug_only_bit_and_plain_analyze_control() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute(POPULATE).await.unwrap();

        let debug_only = frank_rows(&conn, "PRAGMA optimize(1)").await;
        assert!(
            debug_only.is_empty(),
            "optimize(1) lists nothing (analyze bit clear)"
        );
        assert!(
            !frank_stat1_exists(&conn).await,
            "optimize(1) must not create sqlite_stat1"
        );

        // Plain ANALYZE still creates sqlite_stat1 (unchanged machinery).
        conn.execute("ANALYZE").await.unwrap();
        assert!(frank_stat1_exists(&conn).await, "plain ANALYZE still works");
    });
}
