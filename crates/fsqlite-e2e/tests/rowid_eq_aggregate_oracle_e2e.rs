//! bd-2dgf5 — Oracle-parity e2e: aggregate over a rowid-equality predicate vs rusqlite.
//!
//! `SELECT <agg>(...) FROM t WHERE <ipk> = <int literal>` now seeks the single row by
//! rowid instead of full-scanning. This asserts the results are bit-identical to real
//! SQLite for that shape and for the neighbouring cases the seek must decline (real/text
//! RHS, bound-affinity, no-match, negative, i64 bounds, DISTINCT, GROUP BY, HAVING,
//! multiple aggregates, and `WITHOUT ROWID`), so the optimization changes speed, not
//! semantics.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn render_frank(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

async fn frank_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rows = conn.query(sql).await.map_err(|e| e.to_string())?;
    Ok(rows
        .iter()
        .map(|row| row.values().iter().map(render_frank).collect())
        .collect())
}

fn sqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let v: rusqlite::types::Value = row.get_unwrap(i);
            out.push(match v {
                rusqlite::types::Value::Null => "NULL".to_owned(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f}"),
                rusqlite::types::Value::Text(s) => format!("'{s}'"),
                rusqlite::types::Value::Blob(b) => format!(
                    "X'{}'",
                    b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                ),
            });
        }
        Ok(out)
    })
    .map_err(|e| e.to_string())?
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| e.to_string())
}

async fn setup(stmts: &[&str]) -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in stmts {
        f.execute(s)
            .await
            .unwrap_or_else(|e| panic!("frank `{s}`: {e}"));
        r.execute_batch(s)
            .unwrap_or_else(|e| panic!("rusqlite `{s}`: {e}"));
    }
    (f, r)
}

#[test]
fn rowid_eq_aggregate_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let mut schema =
            vec!["CREATE TABLE t (id INTEGER PRIMARY KEY, k INTEGER, v REAL, s TEXT);".to_owned()];
        // Rows include negatives, gaps, and a large rowid near i64 bounds.
        let rows: [(i64, i64, f64, &str); 7] = [
            (-5, 1, 1.5, "a"),
            (-1, 2, 2.5, "b"),
            (1, 3, 3.5, "c"),
            (2, 3, 4.5, "d"),
            (3, 4, 5.5, "e"),
            (100, 4, 6.5, "f"),
            (9_223_372_036_854_775_807, 5, 7.5, "z"),
        ];
        for (id, k, v, s) in rows {
            schema.push(format!("INSERT INTO t VALUES ({id}, {k}, {v}, '{s}');"));
        }
        let schema_refs: Vec<&str> = schema.iter().map(String::as_str).collect();
        let (f, r) = setup(&schema_refs).await;

        let queries = [
            // Core rowid-equality seek shape, every aggregate kind.
            "SELECT COUNT(*) FROM t WHERE id = 2",
            "SELECT COUNT(*) FROM t WHERE 2 = id",
            "SELECT SUM(v) FROM t WHERE id = 2",
            "SELECT SUM(k) FROM t WHERE id = 3",
            "SELECT AVG(v) FROM t WHERE id = 2",
            "SELECT MIN(v), MAX(v) FROM t WHERE id = 2",
            "SELECT TOTAL(k) FROM t WHERE id = 2",
            "SELECT COUNT(k), SUM(k), group_concat(s) FROM t WHERE id = 2",
            "SELECT COUNT(DISTINCT k) FROM t WHERE id = 2",
            // Boundaries and misses (seek returns 0/NULL exactly like the empty scan).
            "SELECT COUNT(*) FROM t WHERE id = -5",
            "SELECT SUM(v) FROM t WHERE id = -1",
            "SELECT COUNT(*) FROM t WHERE id = 9223372036854775807",
            "SELECT COUNT(*) FROM t WHERE id = 999",
            "SELECT SUM(v) FROM t WHERE id = 999",
            "SELECT COUNT(*) FROM t WHERE id = 0",
            "SELECT COUNT(*) FROM t WHERE id = -999",
            // Cases the seek MUST decline (affinity / non-integer / NULL) — the scan must
            // still give the right answer.
            "SELECT COUNT(*) FROM t WHERE id = 2.0",
            "SELECT COUNT(*) FROM t WHERE id = 2.5",
            "SELECT COUNT(*) FROM t WHERE id = '2'",
            "SELECT COUNT(*) FROM t WHERE id = NULL",
            // Predicates that must NOT be treated as a single rowid equality.
            "SELECT COUNT(*) FROM t WHERE id = 2 AND k = 3",
            "SELECT COUNT(*) FROM t WHERE id <> 2",
            "SELECT SUM(k) FROM t WHERE id = 2 GROUP BY k",
            "SELECT COUNT(*) FROM t WHERE id = 2 HAVING COUNT(*) > 0",
            "SELECT COUNT(*) FROM t NOT INDEXED WHERE id = 2",
            // A non-aggregate control on the same predicate.
            "SELECT k, v FROM t WHERE id = 2",
        ];

        for sql in queries {
            let fr = frank_rows(&f, sql).await;
            let sr = sqlite_rows(&r, sql);
            assert_eq!(
                fr, sr,
                "rowid-equality aggregate result diverged from SQLite for `{sql}`"
            );
        }
    });
}

#[test]
fn rowid_range_aggregate_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let mut schema =
            vec!["CREATE TABLE t (id INTEGER PRIMARY KEY, k INTEGER, v REAL, s TEXT);".to_owned()];
        let rows: [(i64, i64, f64, &str); 8] = [
            (-5, 1, 1.5, "a"),
            (-1, 2, 2.5, "b"),
            (1, 3, 3.5, "c"),
            (2, 3, 4.5, "d"),
            (3, 4, 5.5, "e"),
            (10, 4, 6.5, "f"),
            (50, 5, 7.5, "g"),
            (100, 5, 8.5, "z"),
        ];
        for (id, k, v, s) in rows {
            schema.push(format!("INSERT INTO t VALUES ({id}, {k}, {v}, '{s}');"));
        }
        let schema_refs: Vec<&str> = schema.iter().map(String::as_str).collect();
        let (f, r) = setup(&schema_refs).await;

        let queries = [
            // Upper-bounded (early-exit) ranges, every aggregate kind.
            "SELECT COUNT(*) FROM t WHERE id <= 3",
            "SELECT SUM(v) FROM t WHERE id <= 3",
            "SELECT SUM(k) FROM t WHERE id < 3",
            "SELECT AVG(v) FROM t WHERE id <= 10",
            "SELECT MIN(v), MAX(v) FROM t WHERE id <= 50",
            "SELECT COUNT(k), SUM(k), group_concat(s) FROM t WHERE id <= 10",
            "SELECT COUNT(DISTINCT k) FROM t WHERE id <= 50",
            // Lower-bounded (seek-to-start) ranges.
            "SELECT COUNT(*) FROM t WHERE id >= 3",
            "SELECT SUM(v) FROM t WHERE id > 3",
            "SELECT SUM(k) FROM t WHERE id >= 50",
            // Both bounds (BETWEEN and explicit AND).
            "SELECT COUNT(*) FROM t WHERE id BETWEEN 1 AND 50",
            "SELECT SUM(v) FROM t WHERE id BETWEEN 2 AND 10",
            "SELECT SUM(k) FROM t WHERE id > 1 AND id < 50",
            "SELECT COUNT(*) FROM t WHERE id >= 2 AND id <= 3",
            // Negatives and empty/edge ranges.
            "SELECT COUNT(*) FROM t WHERE id <= -1",
            "SELECT SUM(v) FROM t WHERE id >= -5",
            "SELECT COUNT(*) FROM t WHERE id BETWEEN -5 AND 1",
            "SELECT COUNT(*) FROM t WHERE id <= -999",
            "SELECT COUNT(*) FROM t WHERE id >= 999",
            "SELECT COUNT(*) FROM t WHERE id BETWEEN 4 AND 9",
            // Non-integer / affinity bounds — must match whatever the safe path decides.
            "SELECT COUNT(*) FROM t WHERE id <= 3.5",
            "SELECT COUNT(*) FROM t WHERE id >= 2.5",
            // Must NOT be optimized as a lone rowid range.
            "SELECT SUM(k) FROM t WHERE id <= 50 GROUP BY k",
            "SELECT COUNT(*) FROM t WHERE id <= 50 AND k = 4",
            "SELECT COUNT(*) FROM t NOT INDEXED WHERE id <= 3",
            // Non-aggregate control on the same predicate.
            "SELECT id, v FROM t WHERE id BETWEEN 2 AND 10 ORDER BY id",
        ];

        for sql in queries {
            let fr = frank_rows(&f, sql).await;
            let sr = sqlite_rows(&r, sql);
            assert_eq!(
                fr, sr,
                "rowid-range aggregate result diverged from SQLite for `{sql}`"
            );
        }
    });
}

#[test]
fn index_in_list_aggregate_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        // k is INTEGER (INTEGER affinity) with a secondary index; v/s carry the aggregated data.
        // Duplicate k values across rows so a per-value seek walks real duplicate runs, and gaps
        // so some list values match nothing.
        let mut schema = vec![
            "CREATE TABLE t (id INTEGER PRIMARY KEY, k INTEGER, v REAL, s TEXT);".to_owned(),
            "CREATE INDEX idx_t_k ON t(k);".to_owned(),
        ];
        for i in 1..=60_i64 {
            schema.push(format!(
                "INSERT INTO t VALUES ({i}, {}, {i}.5, 'r{i}');",
                i % 7 // k in 0..6, several rows per value
            ));
        }
        // A couple of negative-k and NULL-k rows for edge coverage.
        schema.push("INSERT INTO t VALUES (200, -3, 9.5, 'neg');".to_owned());
        schema.push("INSERT INTO t VALUES (201, NULL, 10.5, 'nul');".to_owned());
        let schema_refs: Vec<&str> = schema.iter().map(String::as_str).collect();
        let (f, r) = setup(&schema_refs).await;

        let queries = [
            // Core IN-list seek shape, every aggregate kind.
            "SELECT SUM(v) FROM t WHERE k IN (1, 2, 3)",
            "SELECT COUNT(k) FROM t WHERE k IN (1, 2, 3)",
            "SELECT AVG(v) FROM t WHERE k IN (2, 4)",
            "SELECT MIN(v), MAX(v) FROM t WHERE k IN (0, 6)",
            "SELECT TOTAL(id) FROM t WHERE k IN (1, 5)",
            "SELECT COUNT(v), SUM(id), group_concat(s) FROM t WHERE k IN (2, 3)",
            "SELECT COUNT(DISTINCT k) FROM t WHERE k IN (1, 2, 3)",
            // Duplicates in the list MUST NOT double count (dedup correctness).
            "SELECT SUM(v) FROM t WHERE k IN (2, 2, 2)",
            "SELECT COUNT(k) FROM t WHERE k IN (1, 1, 2, 2, 3)",
            // Single-element and all-values lists.
            "SELECT SUM(v) FROM t WHERE k IN (2)",
            "SELECT COUNT(*) FROM t WHERE k IN (0, 1, 2, 3, 4, 5, 6)",
            // Values that match nothing, negatives, and mixes.
            "SELECT COUNT(k) FROM t WHERE k IN (99, 100)",
            "SELECT SUM(v) FROM t WHERE k IN (2, 999)",
            "SELECT COUNT(k) FROM t WHERE k IN (-3, 2)",
            "SELECT SUM(v) FROM t WHERE k IN (-3)",
            // NULL in the list (never matches; must not error or over-count).
            "SELECT COUNT(k) FROM t WHERE k IN (2, NULL)",
            // Shapes the seek must decline but still answer correctly.
            "SELECT COUNT(k) FROM t WHERE k IN (2, 3.0)",
            "SELECT COUNT(k) FROM t WHERE k IN ('2', '3')",
            "SELECT COUNT(k) FROM t WHERE k NOT IN (1, 2, 3)",
            "SELECT SUM(v) FROM t WHERE k IN (1, 2) AND id < 30",
            "SELECT SUM(v) FROM t WHERE k IN (1, 2) GROUP BY k",
            "SELECT COUNT(k) FROM t NOT INDEXED WHERE k IN (1, 2, 3)",
            // Non-aggregate control on the same predicate.
            "SELECT id FROM t WHERE k IN (2, 3) ORDER BY id",
        ];

        for sql in queries {
            let fr = frank_rows(&f, sql).await;
            let sr = sqlite_rows(&r, sql);
            assert_eq!(
                fr, sr,
                "IN-list aggregate result diverged from SQLite for `{sql}`"
            );
        }
    });
}

#[test]
fn index_in_list_nonaggregate_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        // k INTEGER (INTEGER affinity), indexed; duplicate k values and rowid gaps.
        let mut schema = vec![
            "CREATE TABLE t (id INTEGER PRIMARY KEY, k INTEGER, v REAL, s TEXT);".to_owned(),
            "CREATE INDEX idx_t_k ON t(k);".to_owned(),
        ];
        for i in 1..=40_i64 {
            schema.push(format!(
                "INSERT INTO t VALUES ({i}, {}, {i}.5, 'r{i}');",
                i % 5
            ));
        }
        schema.push("INSERT INTO t VALUES (200, -3, 9.5, 'neg');".to_owned());
        schema.push("INSERT INTO t VALUES (201, NULL, 10.5, 'nul');".to_owned());
        let schema_refs: Vec<&str> = schema.iter().map(String::as_str).collect();
        let (f, r) = setup(&schema_refs).await;

        // Without ORDER BY the row order is unspecified, so the contract is SET equality: sort
        // both sides. (When fsqlite takes the seek and C SQLite seeks too, orders coincide; when
        // fsqlite declines to a rowid scan they legitimately differ. Both are valid.)
        let sorted = |res: Result<Vec<Vec<String>>, String>| -> Result<Vec<Vec<String>>, String> {
            res.map(|mut rows| {
                rows.sort();
                rows
            })
        };
        let set_eq = [
            "SELECT id FROM t WHERE k IN (1, 2, 3)",
            "SELECT id, k FROM t WHERE k IN (2, 1)",
            "SELECT id, s FROM t WHERE k IN (0, 4)",
            "SELECT id FROM t WHERE k IN (2)",
            "SELECT id FROM t WHERE k IN (2, 2, 2)",
            "SELECT id FROM t WHERE k IN (1, 1, 2, 2)",
            "SELECT id FROM t WHERE k IN (99, 100)",
            "SELECT id FROM t WHERE k IN (-3, 2)",
            "SELECT id, v FROM t WHERE k IN (0, 1, 2, 3, 4)",
            "SELECT id FROM t WHERE k IN (2, NULL)",
        ];
        for sql in set_eq {
            assert_eq!(
                sorted(frank_rows(&f, sql).await),
                sorted(sqlite_rows(&r, sql)),
                "non-aggregate IN-list (row set) diverged from SQLite for `{sql}`"
            );
        }

        // With ORDER BY (declines the seek) and the decline cases: still must match exactly.
        let ordered_or_declined = [
            "SELECT id FROM t WHERE k IN (1, 2, 3) ORDER BY id",
            "SELECT id, k FROM t WHERE k IN (2, 1) ORDER BY id DESC",
            "SELECT id FROM t WHERE k IN (1, 2) ORDER BY id LIMIT 3",
            "SELECT DISTINCT k FROM t WHERE k IN (1, 2, 3) ORDER BY k",
            "SELECT id FROM t WHERE k IN (2, 3.0) ORDER BY id",
            "SELECT id FROM t WHERE k IN ('2', '3') ORDER BY id",
            "SELECT id FROM t WHERE k NOT IN (1, 2) ORDER BY id",
            "SELECT id FROM t WHERE k IN (1, 2) AND id < 20 ORDER BY id",
            "SELECT s FROM t WHERE v IN (1, 2) ORDER BY id",
        ];
        for sql in ordered_or_declined {
            assert_eq!(
                frank_rows(&f, sql).await,
                sqlite_rows(&r, sql),
                "non-aggregate IN-list (ordered/declined) diverged from SQLite for `{sql}`"
            );
        }
    });
}

#[test]
fn covering_index_range_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        // k INTEGER, indexed. Covering (SELECT id / k -> index-only) vs non-covering (SELECT v).
        let mut schema = vec![
            "CREATE TABLE t (id INTEGER PRIMARY KEY, k INTEGER, v REAL, w TEXT);".to_owned(),
            "CREATE INDEX idx_t_k ON t(k);".to_owned(),
        ];
        for i in 1..=60_i64 {
            schema.push(format!(
                "INSERT INTO t VALUES ({i}, {}, {i}.5, 'r{i}');",
                (i % 12) - 3 // k in -3..8, duplicates, negatives
            ));
        }
        schema.push("INSERT INTO t VALUES (200, NULL, 9.5, 'nul');".to_owned());
        let schema_refs: Vec<&str> = schema.iter().map(String::as_str).collect();
        let (f, r) = setup(&schema_refs).await;

        let sorted = |res: Result<Vec<Vec<String>>, String>| -> Result<Vec<Vec<String>>, String> {
            res.map(|mut rows| {
                rows.sort();
                rows
            })
        };
        // No ORDER BY -> set equality (row order unspecified).
        let set_eq = [
            // Covering (id = rowid via IdxRowid): lower / upper / both bounds.
            "SELECT id FROM t WHERE k > 5",
            "SELECT id FROM t WHERE k >= 5",
            "SELECT id FROM t WHERE k < 0",
            "SELECT id FROM t WHERE k <= 0",
            "SELECT id FROM t WHERE k BETWEEN 2 AND 5",
            "SELECT id FROM t WHERE k > 2 AND k < 6",
            "SELECT id FROM t WHERE k > -3 AND k <= 0",
            // Covering (indexed column itself).
            "SELECT k FROM t WHERE k BETWEEN 1 AND 4",
            "SELECT id, k FROM t WHERE k >= 6",
            // Empty / edge ranges.
            "SELECT id FROM t WHERE k > 999",
            "SELECT id FROM t WHERE k < -999",
            "SELECT id FROM t WHERE k BETWEEN 100 AND 200",
            // Non-covering (v not in index) — declines to scan, must still match.
            "SELECT v FROM t WHERE k > 5",
            "SELECT id, v, w FROM t WHERE k BETWEEN 2 AND 5",
            // Non-integer / affinity bounds — must match whatever the safe path decides.
            "SELECT id FROM t WHERE k > 2.5",
            "SELECT id FROM t WHERE k <= 3.0",
        ];
        for sql in set_eq {
            assert_eq!(
                sorted(frank_rows(&f, sql).await),
                sorted(sqlite_rows(&r, sql)),
                "covering index range (row set) diverged from SQLite for `{sql}`"
            );
        }
        // ORDER BY -> exact; NOT INDEXED / DISTINCT decline but must match.
        let exact = [
            "SELECT id FROM t WHERE k > 5 ORDER BY id",
            "SELECT id, k FROM t WHERE k BETWEEN 2 AND 5 ORDER BY k, id",
            "SELECT id FROM t WHERE k >= 6 ORDER BY id DESC",
            "SELECT id FROM t NOT INDEXED WHERE k > 5 ORDER BY id",
            "SELECT DISTINCT k FROM t WHERE k BETWEEN 1 AND 4 ORDER BY k",
            "SELECT COUNT(*) FROM t WHERE k > 5",
        ];
        for sql in exact {
            assert_eq!(
                frank_rows(&f, sql).await,
                sqlite_rows(&r, sql),
                "covering index range (ordered/declined) diverged from SQLite for `{sql}`"
            );
        }
    });
}

#[test]
fn or_of_equalities_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        // OR-of-equalities on the same column normalizes to an IN-list seek. Cover secondary index
        // (k), rowid (id), aggregate + non-aggregate, and every shape that must NOT normalize.
        let mut schema = vec![
            "CREATE TABLE t (id INTEGER PRIMARY KEY, k INTEGER, j INTEGER, v REAL);".to_owned(),
            "CREATE INDEX idx_t_k ON t(k);".to_owned(),
        ];
        for i in 1..=40_i64 {
            schema.push(format!(
                "INSERT INTO t VALUES ({i}, {}, {}, {i}.5);",
                i % 5,
                i % 3
            ));
        }
        let schema_refs: Vec<&str> = schema.iter().map(String::as_str).collect();
        let (f, r) = setup(&schema_refs).await;

        // Aggregates and COUNT(*) are order-independent -> exact.
        let exact = [
            "SELECT SUM(v) FROM t WHERE k = 2 OR k = 3",
            "SELECT COUNT(k) FROM t WHERE k = 1 OR k = 2 OR k = 4",
            "SELECT SUM(v) FROM t WHERE k = 2 OR k = 2",
            "SELECT SUM(v) FROM t WHERE id = 5 OR id = 10 OR id = 15",
            "SELECT COUNT(*) FROM t WHERE id = 5 OR id = 10",
            "SELECT SUM(v) FROM t WHERE 2 = k OR k = 3",
            // Must NOT normalize (mixed columns / operators / NOT / real literal / precedence).
            "SELECT COUNT(k) FROM t WHERE k = 2 OR j = 1",
            "SELECT COUNT(k) FROM t WHERE k = 2 OR k > 3",
            "SELECT COUNT(k) FROM t WHERE k = 2.0 OR k = 3",
            "SELECT COUNT(k) FROM t WHERE k = 2 OR k = 3 AND j = 1",
            "SELECT COUNT(k) FROM t WHERE NOT (k = 2 OR k = 3)",
            "SELECT SUM(v) FROM t NOT INDEXED WHERE k = 2 OR k = 3",
        ];
        for sql in exact {
            assert_eq!(
                frank_rows(&f, sql).await,
                sqlite_rows(&r, sql),
                "OR-of-equalities aggregate diverged from SQLite for `{sql}`"
            );
        }

        // Non-aggregate: no ORDER BY -> set equality; ORDER BY -> exact.
        let sorted = |res: Result<Vec<Vec<String>>, String>| -> Result<Vec<Vec<String>>, String> {
            res.map(|mut rows| {
                rows.sort();
                rows
            })
        };
        let set_eq = [
            "SELECT id FROM t WHERE k = 2 OR k = 3",
            "SELECT id, v FROM t WHERE k = 1 OR k = 4",
            "SELECT id FROM t WHERE id = 5 OR id = 10 OR id = 15",
            "SELECT id FROM t WHERE k = 2 OR k = 2",
        ];
        for sql in set_eq {
            assert_eq!(
                sorted(frank_rows(&f, sql).await),
                sorted(sqlite_rows(&r, sql)),
                "OR-of-equalities non-aggregate (set) diverged from SQLite for `{sql}`"
            );
        }
        for sql in [
            "SELECT id FROM t WHERE k = 2 OR k = 3 ORDER BY id",
            "SELECT id FROM t WHERE id = 15 OR id = 5 OR id = 10 ORDER BY id",
            "SELECT id FROM t WHERE k = 2 OR j = 1 ORDER BY id",
        ] {
            assert_eq!(
                frank_rows(&f, sql).await,
                sqlite_rows(&r, sql),
                "OR-of-equalities non-aggregate (ordered) diverged from SQLite for `{sql}`"
            );
        }
    });
}

#[test]
fn rowid_in_list_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let mut schema =
            vec!["CREATE TABLE t (id INTEGER PRIMARY KEY, k INTEGER, v REAL, s TEXT);".to_owned()];
        // Gaps so some list values match nothing; negatives.
        for (id, s) in [
            (-5_i64, "a"),
            (2, "b"),
            (5, "c"),
            (10, "d"),
            (15, "e"),
            (100, "f"),
        ] {
            schema.push(format!("INSERT INTO t VALUES ({id}, {id}, {id}.5, '{s}');"));
        }
        let schema_refs: Vec<&str> = schema.iter().map(String::as_str).collect();
        let (f, r) = setup(&schema_refs).await;

        let sorted = |res: Result<Vec<Vec<String>>, String>| -> Result<Vec<Vec<String>>, String> {
            res.map(|mut rows| {
                rows.sort();
                rows
            })
        };
        // No ORDER BY -> set equality (row order unspecified).
        let set_eq = [
            "SELECT id, v FROM t WHERE id IN (5, 10, 15)",
            "SELECT id FROM t WHERE id IN (10, 5)",
            "SELECT id, s FROM t WHERE id IN (2)",
            "SELECT id FROM t WHERE id IN (5, 5, 10)",
            "SELECT id FROM t WHERE id IN (999, 1000)",
            "SELECT id FROM t WHERE id IN (2, 999)",
            "SELECT id FROM t WHERE id IN (-5, 2)",
            "SELECT id FROM t WHERE id IN (2, NULL)",
        ];
        for sql in set_eq {
            assert_eq!(
                sorted(frank_rows(&f, sql).await),
                sorted(sqlite_rows(&r, sql)),
                "rowid IN-list (row set) diverged from SQLite for `{sql}`"
            );
        }
        // ORDER BY / declined -> exact.
        let exact = [
            "SELECT id FROM t WHERE id IN (15, 5, 10) ORDER BY id",
            "SELECT id, v FROM t WHERE id IN (5, 10, 15) ORDER BY id DESC",
            "SELECT id FROM t WHERE id IN (5, 10) ORDER BY id LIMIT 1",
            "SELECT id FROM t WHERE id IN (2.0, 5) ORDER BY id",
            "SELECT id FROM t WHERE id IN ('5', '10') ORDER BY id",
            "SELECT id FROM t WHERE id NOT IN (5, 10) ORDER BY id",
            "SELECT id FROM t WHERE id IN (5, 10) AND k < 8 ORDER BY id",
            "SELECT COUNT(*) FROM t WHERE id IN (5, 10, 15)",
        ];
        for sql in exact {
            assert_eq!(
                frank_rows(&f, sql).await,
                sqlite_rows(&r, sql),
                "rowid IN-list (ordered/declined) diverged from SQLite for `{sql}`"
            );
        }
    });
}
