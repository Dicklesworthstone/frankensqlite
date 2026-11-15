//! bd-agg-composite-prefix-range: `SELECT COUNT(*)/SUM(...) FROM t WHERE a = v AND b <range>` on a
//! composite `index(a, b)` now seeks the `a = v` block bounded by the range on `b` instead of
//! full-scanning — for SUM (via `codegen_select_aggregate`) and COUNT(*) (which yields out of
//! `codegen_select_count_star`). HARD GATE: byte-identical to C SQLite (rusqlite) across all range
//! operators on `b`, both aggregates, NULL `b` (excluded), empty ranges (→ COUNT=0 / SUM=NULL),
//! COALESCE, mixed storage in `b`, and a residual-predicate DECLINE (`... AND c = k` must not be
//! dropped — the bd-zqkrp-residual-drop guard). Opcode gate: the seek emits SeekGE + IdxGT; COUNT(*)
//! and SUM(a) are covering (no SeekRowid), SUM(b) is not.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn render(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

async fn frank_rows(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("frank `{sql}`: {e}"))
        .iter()
        .map(|row| row.values().iter().map(render).collect())
        .collect()
}

fn sqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = conn.prepare(sql).unwrap();
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            out.push(match row.get_unwrap::<_, rusqlite::types::Value>(i) {
                rusqlite::types::Value::Null => "NULL".to_owned(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f:?}"),
                rusqlite::types::Value::Text(s) => format!("'{s}'"),
                rusqlite::types::Value::Blob(b) => format!(
                    "X'{}'",
                    b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                ),
            });
        }
        Ok(out)
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

async fn has_op(conn: &Connection, sql: &str, want: &str) -> bool {
    conn.query(&format!("EXPLAIN {sql}"))
        .await
        .unwrap()
        .iter()
        .any(
            |row| matches!(row.values().get(1), Some(SqliteValue::Text(op)) if op.to_string() == want),
        )
}

#[test]
fn agg_composite_prefix_range_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.expect("frank");
        let r = rusqlite::Connection::open_in_memory().expect("sqlite");
        for stmt in [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER);",
            "CREATE INDEX idx_ab ON t(a, b);",
        ] {
            f.execute(stmt).await.unwrap();
            r.execute_batch(stmt).unwrap();
        }
        for i in 1..=3000_i64 {
            let a = i % 12;
            // b spread over a range with some NULLs.
            let b = if i % 13 == 0 {
                "NULL".to_owned()
            } else {
                format!("{}", ((i.wrapping_mul(7)) % 200) - 50)
            };
            let c = i % 5;
            let stmt = format!("INSERT INTO t VALUES ({i}, {a}, {b}, {c});");
            f.execute(&stmt).await.unwrap();
            r.execute_batch(&stmt).unwrap();
        }
        // Mixed storage in b under INTEGER affinity.
        for (id, aval, bval) in [(9001, 5, "7.5"), (9002, 5, "'apple'")] {
            let stmt = format!("INSERT INTO t VALUES ({id}, {aval}, {bval}, 0);");
            f.execute(&stmt).await.unwrap();
            r.execute_batch(&stmt).unwrap();
        }

        for sql in [
            // All range operators on b within a = v, both aggregates.
            "SELECT COUNT(*) FROM t WHERE a = 5 AND b > 10",
            "SELECT SUM(c) FROM t WHERE a = 5 AND b > 10",
            "SELECT COUNT(*) FROM t WHERE a = 5 AND b >= 10",
            "SELECT COUNT(*) FROM t WHERE a = 5 AND b < 10",
            "SELECT SUM(c) FROM t WHERE a = 5 AND b <= 10",
            "SELECT COUNT(*) FROM t WHERE a = 5 AND b BETWEEN -20 AND 40",
            "SELECT SUM(c) FROM t WHERE a = 5 AND b > -20 AND b < 40",
            // Reversed operand order in the range.
            "SELECT COUNT(*) FROM t WHERE a = 5 AND 10 < b",
            // Absent prefix / empty range -> COUNT=0 / SUM=NULL, and COALESCE.
            "SELECT COUNT(*) FROM t WHERE a = 999 AND b > 0",
            "SELECT SUM(c) FROM t WHERE a = 5 AND b > 100000",
            "SELECT COALESCE(SUM(c), -1) FROM t WHERE a = 5 AND b > 100000",
            // Covering SUM of the pinned leading column, and of the range column (both are key columns).
            "SELECT SUM(a) FROM t WHERE a = 5 AND b > 10",
            "SELECT SUM(b) FROM t WHERE a = 5 AND b > 10",
            "SELECT MIN(b) FROM t WHERE a = 5 AND b > 10",
            "SELECT MAX(b) FROM t WHERE a = 5 AND b BETWEEN 0 AND 40",
            // Residual on a NON-key column: `c = 1` must NOT be dropped (declines to a correct scan).
            "SELECT COUNT(*) FROM t WHERE a = 5 AND b > 10 AND c = 1",
            "SELECT SUM(c) FROM t WHERE a = 3 AND b BETWEEN 0 AND 50 AND c = 2",
        ] {
            assert_eq!(
                frank_rows(&f, sql).await,
                sqlite_rows(&r, sql),
                "diverged: `{sql}`"
            );
        }

        // Opcode gate: the composite prefix+range seek fires (SeekGE anchor + IdxGT prefix stop).
        assert!(
            has_op(
                &f,
                "SELECT COUNT(*) FROM t WHERE a = 5 AND b > 10",
                "SeekGE"
            )
            .await
                && has_op(&f, "SELECT COUNT(*) FROM t WHERE a = 5 AND b > 10", "IdxGT").await,
            "COUNT(*) WHERE a=v AND b>c must seek (SeekGE + IdxGT)"
        );
        assert!(
            has_op(&f, "SELECT SUM(c) FROM t WHERE a = 5 AND b > 10", "IdxGT").await,
            "SUM WHERE a=v AND b>c must seek the composite block"
        );
        // Covering gate: COUNT(*)/SUM(leading col a) read straight from the index; SUM(b) needs the table.
        assert!(
            !has_op(
                &f,
                "SELECT COUNT(*) FROM t WHERE a = 5 AND b > 10",
                "SeekRowid"
            )
            .await,
            "COUNT(*) composite prefix+range must be covering (no SeekRowid)"
        );
        assert!(
            !has_op(
                &f,
                "SELECT SUM(a) FROM t WHERE a = 5 AND b > 10",
                "SeekRowid"
            )
            .await,
            "SUM(leading col) composite prefix+range must be covering (no SeekRowid)"
        );
        assert!(
            !has_op(
                &f,
                "SELECT SUM(b) FROM t WHERE a = 5 AND b > 10",
                "SeekRowid"
            )
            .await,
            "SUM(range col b) is a KEY column -> covering (no SeekRowid)"
        );
        // A SUM of a NON-key column still needs the table lookup.
        assert!(
            has_op(
                &f,
                "SELECT SUM(c) FROM t WHERE a = 5 AND b > 10",
                "SeekRowid"
            )
            .await,
            "SUM(non-key col c) must open the table (SeekRowid)"
        );
        // Residual declines the seek (no IdxGT), falling to a scan that enforces c.
        assert!(
            !has_op(
                &f,
                "SELECT COUNT(*) FROM t WHERE a = 5 AND b > 10 AND c = 1",
                "IdxGT"
            )
            .await,
            "residual `c = 1` must decline the composite prefix+range seek"
        );
    });
}

/// bd-bn45n follow-up: aggregates over a PURE range on the LEADING key term of a composite-only index
/// (`SELECT COUNT(*)/SUM(...) FROM t WHERE a <range>`, no equality prefix) reuse the same
/// `composite_index_prefix_range_target` (now empty-prefix-capable) and the aggregate composite-range
/// emitter. The emitter must SKIP the prefix `IdxGT` when `prefix_len==0` (the range bounds terminate
/// the walk); this asserts byte-identical results and that the seek fires with no prefix IdxGT.
#[test]
fn agg_composite_pure_leading_range_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.expect("frank");
        let r = rusqlite::Connection::open_in_memory().expect("sqlite");
        for stmt in [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER);",
            "CREATE INDEX idx_ab ON t(a, b);",
        ] {
            f.execute(stmt).await.unwrap();
            r.execute_batch(stmt).unwrap();
        }
        for i in 1..=3000_i64 {
            // NULL run in the LEADING term `a` (excluded by any `a <range>`, matching C SQLite).
            let a = if i % 47 == 0 {
                "NULL".to_owned()
            } else {
                format!("{}", i % 12)
            };
            let stmt = format!("INSERT INTO t VALUES ({i}, {a}, {}, {});", i % 25, i % 5);
            f.execute(&stmt).await.unwrap();
            r.execute_batch(&stmt).unwrap();
        }
        for sql in [
            "SELECT COUNT(*) FROM t WHERE a < 5",
            "SELECT COUNT(*) FROM t WHERE a <= 4",
            "SELECT COUNT(*) FROM t WHERE a > 7",
            "SELECT COUNT(*) FROM t WHERE a >= 6",
            "SELECT COUNT(*) FROM t WHERE a BETWEEN 2 AND 8",
            "SELECT SUM(c) FROM t WHERE a < 5",
            "SELECT SUM(a) FROM t WHERE a < 5",
            "SELECT MIN(b) FROM t WHERE a < 5",
            "SELECT MAX(c) FROM t WHERE a >= 6",
            "SELECT COALESCE(SUM(c), -1) FROM t WHERE a > 100000",
            // Residual on a non-key column must NOT be dropped (declines to a scan enforcing c).
            "SELECT COUNT(*) FROM t WHERE a < 5 AND c = 1",
        ] {
            assert_eq!(
                frank_rows(&f, sql).await,
                sqlite_rows(&r, sql),
                "diverged: `{sql}`"
            );
        }

        // Pure leading range seeks (SeekGE) with NO prefix IdxGT (empty prefix); residual declines.
        assert!(
            has_op(&f, "SELECT COUNT(*) FROM t WHERE a < 5", "SeekGE").await,
            "COUNT(*) pure leading range must seek idx_ab (SeekGE)"
        );
        assert!(
            !has_op(&f, "SELECT COUNT(*) FROM t WHERE a < 5", "IdxGT").await,
            "empty-prefix aggregate leading-range seek must not emit a prefix IdxGT"
        );
        assert!(
            !has_op(&f, "SELECT COUNT(*) FROM t WHERE a < 5 AND c = 1", "SeekGE").await,
            "residual `c = 1` must decline the aggregate leading-range seek"
        );
    });
}

/// The tables and indexes the program opens with `OpenRead`, by their P4 names.
async fn index_opened(conn: &Connection, sql: &str) -> Vec<String> {
    conn.query(&format!("EXPLAIN {sql}"))
        .await
        .unwrap()
        .iter()
        .filter(|row| {
            matches!(row.values().get(1), Some(SqliteValue::Text(op)) if op.as_str() == "OpenRead")
        })
        .filter_map(|row| match row.values().get(5) {
            Some(SqliteValue::Text(p4)) => Some(p4.to_string()),
            _ => None,
        })
        .collect()
}

/// bd-k96op: an aggregate over a range on an index's leading key with a residual that a
/// composite index holds (`a > 5 AND b = 3` over `idx_ab(a, b)`) walks that index, preferred over
/// the single-column `idx_a`, and applies the residual from the entry: no row is looked up unless
/// an aggregate reads a column the index lacks. A NULL second key at an inclusive lower bound is
/// counted (the seek probe is the bound alone). A residual the index lacks (`c = 1`) keeps the
/// single-column walk and its lookups. Answers match stock in memory and on a file database in
/// strict mode.
#[test]
fn agg_leading_range_with_a_residual_the_index_holds_reads_only_the_index() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("agg_range_covered_residual.db");
        for path in [":memory:", file.to_str().unwrap()] {
            let f = Connection::open(path).await.expect("frank");
            let r = rusqlite::Connection::open_in_memory().expect("sqlite");
            for stmt in [
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER);",
                "CREATE INDEX idx_a ON t(a);",
                "CREATE INDEX idx_ab ON t(a, b);",
            ] {
                f.execute(stmt).await.unwrap();
                r.execute_batch(stmt).unwrap();
            }
            for i in 1..=3000_i64 {
                let a = if i % 47 == 0 {
                    "NULL".to_owned()
                } else {
                    format!("{}", i % 12)
                };
                let b = if i % 9 == 0 {
                    "NULL".to_owned()
                } else {
                    format!("{}", i % 25)
                };
                let stmt = format!("INSERT INTO t VALUES ({i}, {a}, {b}, {});", i % 5);
                f.execute(&stmt).await.unwrap();
                r.execute_batch(&stmt).unwrap();
            }
            if path != ":memory:" {
                f.execute("PRAGMA fsqlite.parity_cert_strict = ON;")
                    .await
                    .unwrap();
            }
            let covered = [
                "SELECT COUNT(*) FROM t WHERE a > 5 AND b = 3",
                "SELECT COUNT(*) FROM t WHERE a >= 5 AND b IS NULL",
                "SELECT COUNT(*) FROM t WHERE a BETWEEN 2 AND 8 AND b < 10",
                "SELECT SUM(b) FROM t WHERE a < 5 AND b > 3",
                "SELECT MIN(b), MAX(b) FROM t WHERE a > 3 AND b <> 7",
                "SELECT COUNT(*) FROM t WHERE a > 5 AND (b = 1 OR b = 2)",
                "SELECT COUNT(*) FROM t WHERE a > 100 AND b = 1",
            ];
            for sql in covered {
                assert_eq!(
                    frank_rows(&f, sql).await,
                    sqlite_rows(&r, sql),
                    "diverged: `{sql}` at {path}"
                );
                assert_eq!(
                    index_opened(&f, sql).await,
                    ["idx_ab"],
                    "`{sql}` must read only idx_ab at {path}"
                );
                assert!(
                    !has_op(&f, sql, "SeekRowid").await,
                    "`{sql}` must not look rows up at {path}"
                );
            }
            // The residual still comes from the entry; the uncovered aggregate reads the row.
            let sum_c = "SELECT SUM(c) FROM t WHERE a > 5 AND b = 3";
            assert_eq!(
                frank_rows(&f, sum_c).await,
                sqlite_rows(&r, sum_c),
                "{path}"
            );
            assert!(
                index_opened(&f, sum_c).await.contains(&"idx_ab".to_owned())
                    && has_op(&f, sum_c, "SeekRowid").await,
                "`{sum_c}` filters on idx_ab and reads c from the row at {path}"
            );
            // A residual the index lacks keeps the single-column walk and its lookups.
            let uncovered = "SELECT COUNT(*) FROM t WHERE a > 5 AND c = 1";
            assert_eq!(
                frank_rows(&f, uncovered).await,
                sqlite_rows(&r, uncovered),
                "{path}"
            );
            assert!(
                has_op(&f, uncovered, "SeekRowid").await,
                "`{uncovered}` must read c from the row at {path}"
            );
            f.close().await.unwrap();
        }
    });
}

/// The schema and rows of `agg_composite_range_prefers_a_later_index_that_covers`.
fn covering_choice_fixture() -> Vec<String> {
    let mut stmts: Vec<String> = [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, r REAL, c INTEGER);",
        "CREATE INDEX idx_ab ON t(a, b);",
        "CREATE INDEX idx_ar ON t(a, r);",
        "CREATE TABLE w (a INTEGER NOT NULL, id INTEGER NOT NULL, b INTEGER, r REAL, \
         PRIMARY KEY (a, id)) WITHOUT ROWID;",
        "CREATE INDEX w_ab ON w(a, b);",
        "CREATE INDEX w_ar ON w(a, r);",
    ]
    .map(str::to_owned)
    .to_vec();
    for i in 1..=3000_i64 {
        let a = if i % 47 == 0 {
            "NULL".to_owned()
        } else {
            format!("{}", i % 12)
        };
        let b = if i % 9 == 0 {
            "NULL".to_owned()
        } else {
            format!("{}", i % 25)
        };
        // Whole and half values and NULLs. Stock stores a whole REAL as an integer in its
        // records; read back, it must still be REAL.
        let r = if i % 11 == 0 {
            "NULL".to_owned()
        } else {
            format!("{}.{}", (i * 7) % 40, if i % 2 == 0 { 0 } else { 5 })
        };
        stmts.push(format!(
            "INSERT INTO t VALUES ({i}, {a}, {b}, {r}, {});",
            i % 5
        ));
        stmts.push(format!("INSERT INTO w VALUES ({}, {i}, {b}, {r});", i % 12));
    }
    stmts
}

/// bd-w4pr1.13.18 (read/seek round 3, lever R1): two composite indexes lead with `a`. `idx_ab(a,
/// b)` is declared first and lacks `r`; `idx_ar(a, r)` holds it. An aggregate over a range on `a`
/// that reads only `r` walks `idx_ar` and looks no row up, as stock does (`SEARCH t USING
/// COVERING INDEX idx_ar`). Every index the composite range target admits enforces the whole
/// WHERE, so the choice changes no answer; `group_concat(r)` checks that the entries come in
/// stock's order. Negative cases: a WHERE only `idx_ab` pins keeps it with its lookups; a first
/// index that covers stays chosen; when no index covers, the first one is kept. A WITHOUT ROWID
/// twin, which this target declines, answers as before. Checked in memory, on a file fsqlite
/// wrote and on a file stock wrote, in strict mode on the files.
#[test]
fn agg_composite_range_prefers_a_later_index_that_covers() {
    asupersync::test_utils::run_test(|| async {
        let fixture = covering_choice_fixture();
        let dir = tempfile::tempdir().unwrap();
        let ours = dir.path().join("covering_choice_ours.db");
        let theirs = dir.path().join("covering_choice_stock.db");
        {
            let stock_writer = rusqlite::Connection::open(&theirs).unwrap();
            for stmt in &fixture {
                stock_writer.execute_batch(stmt).unwrap();
            }
        }
        for path in [":memory:", ours.to_str().unwrap(), theirs.to_str().unwrap()] {
            let stock_file = path == theirs.to_str().unwrap();
            let f = Connection::open(path).await.expect("frank");
            let r = if stock_file {
                rusqlite::Connection::open(&theirs).expect("sqlite")
            } else {
                let r = rusqlite::Connection::open_in_memory().expect("sqlite");
                for stmt in &fixture {
                    f.execute(stmt).await.unwrap();
                    r.execute_batch(stmt).unwrap();
                }
                r
            };
            if path != ":memory:" {
                f.execute("PRAGMA fsqlite.parity_cert_strict = ON;")
                    .await
                    .unwrap();
            }
            // Covered by the later idx_ar only.
            let covered = [
                "SELECT SUM(r) FROM t WHERE a BETWEEN 2 AND 8",
                "SELECT COUNT(*), SUM(r), TOTAL(r), AVG(r) FROM t WHERE a > 5",
                "SELECT COUNT(r), SUM(a), SUM(r) FROM t WHERE a <= 3",
                "SELECT group_concat(r) FROM t WHERE a >= 9",
                "SELECT SUM(r) FROM t WHERE a > 100",
                "SELECT SUM(r) FROM t WHERE a = 5 AND r > 12.0",
            ];
            for sql in covered {
                assert_eq!(
                    frank_rows(&f, sql).await,
                    sqlite_rows(&r, sql),
                    "diverged: `{sql}` at {path}"
                );
                assert_eq!(
                    index_opened(&f, sql).await,
                    ["idx_ar"],
                    "`{sql}` must read only idx_ar at {path}"
                );
                assert!(
                    !has_op(&f, sql, "SeekRowid").await,
                    "`{sql}` must not look rows up at {path}"
                );
            }
            // Only idx_ab can pin `b > 3`: it stays, and r is read from the row.
            let pinned_by_ab = "SELECT SUM(r) FROM t WHERE a = 5 AND b > 3";
            // idx_ab covers SUM(b) and is first: it stays.
            let first_covers = "SELECT SUM(b) FROM t WHERE a > 5";
            // Neither index holds c: the first one is kept, with lookups.
            let none_covers = "SELECT SUM(c) FROM t WHERE a > 5";
            for (sql, lookups) in [
                (pinned_by_ab, true),
                (first_covers, false),
                (none_covers, true),
            ] {
                assert_eq!(
                    frank_rows(&f, sql).await,
                    sqlite_rows(&r, sql),
                    "diverged: `{sql}` at {path}"
                );
                let opened = index_opened(&f, sql).await;
                assert!(
                    opened.contains(&"idx_ab".to_owned()) && !opened.contains(&"idx_ar".to_owned()),
                    "`{sql}` must walk idx_ab at {path}: {opened:?}"
                );
                assert_eq!(
                    has_op(&f, sql, "SeekRowid").await,
                    lookups,
                    "`{sql}` row lookups at {path}"
                );
            }
            for sql in [
                "SELECT SUM(r), COUNT(*) FROM w WHERE a BETWEEN 2 AND 8",
                "SELECT COUNT(r), TOTAL(r) FROM w WHERE a >= 9",
            ] {
                assert_eq!(
                    frank_rows(&f, sql).await,
                    sqlite_rows(&r, sql),
                    "diverged: `{sql}` at {path}"
                );
            }
            f.close().await.unwrap();
        }
    });
}

/// bd-w4pr1.13.18 (read/seek round 3, lever R1b): the same choice for a plain SELECT over a
/// range on `a` with no ORDER BY. A projection only `idx_ar` holds walks it and looks no row
/// up, and the rows come out in stock's order (compared unsorted). Negative cases: a
/// projection the first index holds keeps it; a WHERE only `idx_ab` pins keeps it with its
/// lookups; with an ORDER BY the choice is left as it was (answers only); a projection no index
/// holds is compared as a set, since either index's order is legal there.
#[test]
fn nonagg_composite_range_prefers_a_later_index_that_covers() {
    asupersync::test_utils::run_test(|| async {
        let fixture = covering_choice_fixture();
        let dir = tempfile::tempdir().unwrap();
        let ours = dir.path().join("nonagg_covering_choice_ours.db");
        let theirs = dir.path().join("nonagg_covering_choice_stock.db");
        {
            let stock_writer = rusqlite::Connection::open(&theirs).unwrap();
            for stmt in &fixture {
                stock_writer.execute_batch(stmt).unwrap();
            }
        }
        for path in [":memory:", ours.to_str().unwrap(), theirs.to_str().unwrap()] {
            let stock_file = path == theirs.to_str().unwrap();
            let f = Connection::open(path).await.expect("frank");
            let r = if stock_file {
                rusqlite::Connection::open(&theirs).expect("sqlite")
            } else {
                let r = rusqlite::Connection::open_in_memory().expect("sqlite");
                for stmt in &fixture {
                    f.execute(stmt).await.unwrap();
                    r.execute_batch(stmt).unwrap();
                }
                r
            };
            if path != ":memory:" {
                f.execute("PRAGMA fsqlite.parity_cert_strict = ON;")
                    .await
                    .unwrap();
            }
            for sql in [
                "SELECT a, r FROM t WHERE a BETWEEN 2 AND 8",
                "SELECT id, r FROM t WHERE a <= 3",
                "SELECT r FROM t WHERE a > 9 LIMIT 7",
                "SELECT r, a FROM t WHERE a = 5 AND r > 12.0",
            ] {
                assert_eq!(
                    frank_rows(&f, sql).await,
                    sqlite_rows(&r, sql),
                    "diverged: `{sql}` at {path}"
                );
                assert_eq!(
                    index_opened(&f, sql).await,
                    ["idx_ar"],
                    "`{sql}` must read only idx_ar at {path}"
                );
                assert!(
                    !has_op(&f, sql, "SeekRowid").await,
                    "`{sql}` must not look rows up at {path}"
                );
            }
            // The first index holds the projection: it stays, covered.
            let first_covers = "SELECT a, b FROM t WHERE a > 5";
            // Only idx_ab can pin `b > 3`: it stays, and r is read from the row.
            let pinned_by_ab = "SELECT r FROM t WHERE a = 5 AND b > 3";
            for (sql, lookups) in [(first_covers, false), (pinned_by_ab, true)] {
                assert_eq!(
                    frank_rows(&f, sql).await,
                    sqlite_rows(&r, sql),
                    "diverged: `{sql}` at {path}"
                );
                let opened = index_opened(&f, sql).await;
                assert!(
                    opened.contains(&"idx_ab".to_owned()) && !opened.contains(&"idx_ar".to_owned()),
                    "`{sql}` must walk idx_ab at {path}: {opened:?}"
                );
                assert_eq!(
                    has_op(&f, sql, "SeekRowid").await,
                    lookups,
                    "`{sql}` row lookups at {path}"
                );
            }
            let ordered = "SELECT a, r FROM t WHERE a BETWEEN 2 AND 8 ORDER BY a, r, id";
            assert_eq!(
                frank_rows(&f, ordered).await,
                sqlite_rows(&r, ordered),
                "diverged: `{ordered}` at {path}"
            );
            let uncovered = "SELECT a, c FROM t WHERE a > 5";
            let mut ours_rows = frank_rows(&f, uncovered).await;
            let mut stock_rows = sqlite_rows(&r, uncovered);
            ours_rows.sort();
            stock_rows.sort();
            assert_eq!(ours_rows, stock_rows, "diverged: `{uncovered}` at {path}");
            f.close().await.unwrap();
        }
    });
}
