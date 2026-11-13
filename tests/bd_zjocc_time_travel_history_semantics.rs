#![recursion_limit = "512"]

//! bd-zjocc: a `FOR SYSTEM_TIME AS OF` query must answer exactly what stock
//! SQLite answers on the database frozen at that commit.
//!
//! A shadow stock SQLite replays the same statements; at every commit
//! boundary that makes a snapshot it records the answers to a fixed set of
//! queries (rows, or the fact that the query fails). After the whole history
//! is written, fsqlite
//! answers every query against every recorded commit, and each answer must
//! match the shadow's. The queries cover what the time-travel executor got
//! wrong:
//! - aggregates (`count(*)` over an empty table returned no row; over a
//!   table, a TEMP table or a time-travel subquery, one NULL row per input
//!   row);
//! - a table created after the snapshot (read the live table's rows);
//! - a TEMP table that later shadows a `main` table (the main table's history
//!   was unreachable) and `main.`/`temp.` qualified names.

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

fn render_fsqlite(rows: &[Row]) -> Vec<String> {
    rows.iter()
        .map(|row| {
            row.values()
                .iter()
                .map(|value| match value {
                    SqliteValue::Null => "null".to_owned(),
                    SqliteValue::Integer(i) => format!("i:{i}"),
                    SqliteValue::Float(f) => format!("r:{f}"),
                    SqliteValue::Text(t) => format!("t:{t}"),
                    SqliteValue::Blob(b) => format!("b:{b:?}"),
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

fn render_stock(conn: &rusqlite::Connection, sql: &str) -> Option<Vec<String>> {
    let mut stmt = conn.prepare(sql).ok()?;
    let columns = stmt.column_count();
    let rows = stmt
        .query_map([], |row| {
            let mut cells = Vec::with_capacity(columns);
            for i in 0..columns {
                cells.push(match row.get_ref(i)? {
                    rusqlite::types::ValueRef::Null => "null".to_owned(),
                    rusqlite::types::ValueRef::Integer(v) => format!("i:{v}"),
                    rusqlite::types::ValueRef::Real(v) => format!("r:{v}"),
                    rusqlite::types::ValueRef::Text(v) => {
                        format!("t:{}", String::from_utf8_lossy(v))
                    }
                    rusqlite::types::ValueRef::Blob(v) => format!("b:{v:?}"),
                });
            }
            Ok(cells.join("|"))
        })
        .ok()?
        .collect::<Result<Vec<_>, _>>()
        .ok()?;
    Some(rows)
}

/// Statements that each end at a commit boundary (DDL, or an explicit
/// transaction ending in COMMIT), and whether that boundary makes a
/// snapshot. A transaction that only writes TEMP tables makes no pager
/// commit and so no snapshot; its rows first appear in the next commit's.
const BOUNDARIES: &[(bool, &[&str])] = &[
    (true, &["CREATE TABLE e(x)"]),
    (true, &["CREATE TABLE t(a INTEGER, b TEXT)"]),
    (true, &["CREATE TABLE u(k INTEGER, w TEXT)"]),
    (
        true,
        &[
            "BEGIN",
            "INSERT INTO t VALUES (1, 'x'), (2, 'y'), (3, 'x')",
            "INSERT INTO u VALUES (1, 'one'), (3, 'three')",
            "COMMIT",
        ],
    ),
    (true, &["CREATE TEMP TABLE tt(v)"]),
    (false, &["BEGIN", "INSERT INTO tt VALUES (10), (20)", "COMMIT"]),
    (
        true,
        &[
            "BEGIN",
            "INSERT INTO t VALUES (4, 'y')",
            "INSERT INTO u VALUES (4, 'four')",
            "COMMIT",
        ],
    ),
    (true, &["CREATE TEMP TABLE t(a INTEGER, b TEXT)"]),
    (false, &["BEGIN", "INSERT INTO temp.t VALUES (100, 'temp')", "COMMIT"]),
    (true, &["BEGIN", "INSERT INTO u VALUES (5, 'five')", "COMMIT"]),
];

/// Historical queries, written without the temporal clause; `{AS_OF}` marks
/// where it goes.
const QUERIES: &[&str] = &[
    "SELECT count(*) FROM e {AS_OF}",
    "SELECT count(*), sum(a), max(b) FROM t {AS_OF}",
    "SELECT b, count(*), sum(a) FROM t {AS_OF} GROUP BY b ORDER BY b",
    "SELECT b, count(*) FROM t {AS_OF} GROUP BY b ORDER BY b DESC LIMIT 1",
    "SELECT DISTINCT b FROM t {AS_OF} ORDER BY b",
    "SELECT a, b FROM t {AS_OF} ORDER BY a",
    "SELECT a FROM main.t {AS_OF} ORDER BY a",
    "SELECT count(*), sum(a) FROM main.t {AS_OF}",
    "SELECT a FROM temp.t {AS_OF} ORDER BY a",
    "SELECT count(*), sum(v) FROM tt {AS_OF}",
    "SELECT v FROM tt {AS_OF} ORDER BY v",
    "SELECT t.a, u.w FROM t {AS_OF} JOIN u ON u.k = t.a ORDER BY t.a",
    "SELECT count(*), sum(t.a) FROM t {AS_OF} JOIN u ON u.k = t.a",
];

/// The shadow is stock SQLite; it answers each query without a temporal
/// clause at the moment the boundary is reached.
fn stock_answer(stock: &rusqlite::Connection, query: &str) -> Option<Vec<String>> {
    render_stock(stock, &query.replace(" {AS_OF}", ""))
}

/// Every probe query names `e`, which exists from the first boundary on, so
/// a commit seq resolves to a snapshot exactly when this succeeds.
async fn snapshot_exists(conn: &Connection, seq: u64) -> bool {
    conn.query(&format!("SELECT x FROM e FOR SYSTEM_TIME AS OF COMMITSEQ {seq}"))
        .await
        .is_ok()
}

#[test]
fn historical_queries_match_stock_frozen_at_each_commit() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.expect("open :memory:");
        let stock = rusqlite::Connection::open_in_memory().expect("stock shadow");

        // (seq, per-query stock answer) for every boundary.
        let mut history: Vec<(u64, Vec<Option<Vec<String>>>)> = Vec::new();
        let mut next_probe = 1_u64;
        for (makes_snapshot, statements) in BOUNDARIES {
            for sql in *statements {
                conn.execute(sql)
                    .await
                    .unwrap_or_else(|e| panic!("fsqlite {sql}: {e}"));
                stock
                    .execute_batch(sql)
                    .unwrap_or_else(|e| panic!("stock {sql}: {e}"));
            }
            // Search a short window without consuming the seqs: a boundary
            // that makes no snapshot must not skip past the next one's.
            let mut found = None;
            for candidate in next_probe..next_probe + 16 {
                if snapshot_exists(&conn, candidate).await {
                    found = Some(candidate);
                    break;
                }
            }
            assert_eq!(
                found.is_some(),
                *makes_snapshot,
                "snapshot after {statements:?}: found {found:?}"
            );
            let Some(seq) = found else {
                continue;
            };
            next_probe = seq + 1;
            let answers = QUERIES
                .iter()
                .map(|query| stock_answer(&stock, query))
                .collect();
            history.push((seq, answers));
        }

        let mut mismatches = Vec::new();
        for (seq, answers) in &history {
            for (query, expected) in QUERIES.iter().zip(answers) {
                let sql = query.replace("{AS_OF}", &format!("FOR SYSTEM_TIME AS OF COMMITSEQ {seq}"));
                let actual = conn.query(&sql).await.ok().map(|rows| render_fsqlite(&rows));
                if &actual != expected {
                    mismatches.push(format!(
                        "seq {seq}: {sql}\n    stock:   {expected:?}\n    fsqlite: {actual:?}"
                    ));
                }
            }
        }
        assert!(
            mismatches.is_empty(),
            "{} historical answers differ from stock frozen at the commit:\n{}",
            mismatches.len(),
            mismatches.join("\n")
        );

        // A historical query leaves the live catalog and rows untouched.
        assert_eq!(
            render_fsqlite(&conn.query("SELECT a FROM t ORDER BY a").await.expect("live temp.t")),
            vec!["i:100".to_owned()]
        );
        assert_eq!(
            render_fsqlite(&conn.query("SELECT a FROM main.t ORDER BY a").await.expect("live main.t")),
            vec!["i:1", "i:2", "i:3", "i:4"]
        );
    });
}

#[test]
fn aggregate_over_a_time_travel_subquery_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.expect("open :memory:");
        let stock = rusqlite::Connection::open_in_memory().expect("stock shadow");
        for sql in [
            "CREATE TABLE t(a INTEGER)",
            "BEGIN",
            "INSERT INTO t VALUES (1), (2), (3)",
            "COMMIT",
        ] {
            conn.execute(sql).await.expect("fsqlite setup");
            stock.execute_batch(sql).expect("stock setup");
        }
        let mut seq = 1_u64;
        while !conn
            .query(&format!("SELECT a FROM t FOR SYSTEM_TIME AS OF COMMITSEQ {seq}"))
            .await
            .is_ok_and(|rows| rows.len() == 3)
        {
            seq += 1;
            assert!(seq < 1_000, "no snapshot holds the three rows");
        }
        let expected = render_stock(&stock, "SELECT count(*), sum(a) FROM (SELECT a FROM t)");
        conn.execute("INSERT INTO t VALUES (4)").await.expect("later write");
        let actual = conn
            .query(&format!(
                "SELECT count(*), sum(a) FROM (SELECT a FROM t FOR SYSTEM_TIME AS OF COMMITSEQ {seq})"
            ))
            .await
            .ok()
            .map(|rows| render_fsqlite(&rows));
        assert_eq!(actual, expected);
    });
}

/// `main.<name>` reaches the main table while a TEMP table of the same name
/// shadows it, both live and as of a commit taken under the shadow. The
/// interpreted join executor scanned the visible (TEMP) table for it, so a
/// join over `main.t` answered with the TEMP table's rows.
#[test]
fn main_qualified_reads_under_a_temp_shadow_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.expect("open :memory:");
        let stock = rusqlite::Connection::open_in_memory().expect("stock shadow");
        for sql in [
            "CREATE TABLE e(x)",
            "CREATE TABLE t(a INTEGER, b TEXT)",
            "INSERT INTO t VALUES (1, 'x'), (2, 'y'), (3, 'x'), (4, 'y')",
            "CREATE TABLE u(k INTEGER, w TEXT)",
            "INSERT INTO u VALUES (1, 'one'), (3, 'three'), (4, 'four')",
            "CREATE TEMP TABLE t(a INTEGER, b TEXT)",
            "INSERT INTO temp.t VALUES (100, 'temp')",
            "INSERT INTO u VALUES (5, 'five')",
        ] {
            conn.execute(sql)
                .await
                .unwrap_or_else(|e| panic!("fsqlite {sql}: {e}"));
            stock
                .execute_batch(sql)
                .unwrap_or_else(|e| panic!("stock {sql}: {e}"));
        }
        // The newest snapshot is the last commit's, taken under the shadow.
        let mut newest = None;
        for candidate in 1..64 {
            if snapshot_exists(&conn, candidate).await {
                newest = Some(candidate);
            }
        }
        let seq = newest.expect("a snapshot under the shadow");
        let as_of = format!("FOR SYSTEM_TIME AS OF COMMITSEQ {seq}");

        // (query, also checked live). A live GROUP BY over `main.t` compiles
        // to a program and is not this path, so it is checked as of the
        // snapshot only.
        let queries: &[(&str, bool)] = &[
            ("SELECT t.a, u.w FROM main.t {AS_OF} JOIN u ON u.k = t.a ORDER BY t.a", true),
            ("SELECT count(*), sum(t.a) FROM main.t {AS_OF} JOIN u ON u.k = t.a", true),
            ("SELECT b, count(*) FROM main.t {AS_OF} GROUP BY b ORDER BY b", false),
            ("SELECT a FROM main.t {AS_OF} ORDER BY a", true),
        ];
        let mut mismatches = Vec::new();
        let mut check = |sql: String, expected: &Option<Vec<String>>, actual: Option<Vec<String>>| {
            if &actual != expected {
                mismatches.push(format!(
                    "{sql}\n    stock:   {expected:?}\n    fsqlite: {actual:?}"
                ));
            }
        };
        for (query, live_too) in queries {
            let expected = stock_answer(&stock, query);
            let historical = query.replace("{AS_OF}", &as_of);
            let actual = conn.query(&historical).await.ok().map(|rows| render_fsqlite(&rows));
            check(historical, &expected, actual);
            if *live_too {
                let live = query.replace(" {AS_OF}", "");
                let actual = conn.query(&live).await.ok().map(|rows| render_fsqlite(&rows));
                check(live, &expected, actual);
            }
        }
        let aliased = "SELECT m.a, tt.a FROM main.t AS m JOIN temp.t AS tt ORDER BY m.a";
        let actual = conn.query(aliased).await.ok().map(|rows| render_fsqlite(&rows));
        check(aliased.to_owned(), &render_stock(&stock, aliased), actual);
        assert!(
            mismatches.is_empty(),
            "{} `main.t` reads under a TEMP shadow differ from stock:\n{}",
            mismatches.len(),
            mismatches.join("\n")
        );
    });
}
