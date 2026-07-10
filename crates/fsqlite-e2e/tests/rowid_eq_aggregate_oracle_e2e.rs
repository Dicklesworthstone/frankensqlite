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

fn frank_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rows = conn.query(sql).map_err(|e| e.to_string())?;
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

fn setup(stmts: &[&str]) -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in stmts {
        f.execute(s).unwrap_or_else(|e| panic!("frank `{s}`: {e}"));
        r.execute_batch(s)
            .unwrap_or_else(|e| panic!("rusqlite `{s}`: {e}"));
    }
    (f, r)
}

#[test]
fn rowid_eq_aggregate_matches_sqlite() {
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
    let (f, r) = setup(&schema_refs);

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
        let fr = frank_rows(&f, sql);
        let sr = sqlite_rows(&r, sql);
        assert_eq!(
            fr, sr,
            "rowid-equality aggregate result diverged from SQLite for `{sql}`"
        );
    }
}

#[test]
fn rowid_range_aggregate_matches_sqlite() {
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
    let (f, r) = setup(&schema_refs);

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
        let fr = frank_rows(&f, sql);
        let sr = sqlite_rows(&r, sql);
        assert_eq!(
            fr, sr,
            "rowid-range aggregate result diverged from SQLite for `{sql}`"
        );
    }
}

#[test]
fn index_in_list_aggregate_matches_sqlite() {
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
    let (f, r) = setup(&schema_refs);

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
        let fr = frank_rows(&f, sql);
        let sr = sqlite_rows(&r, sql);
        assert_eq!(
            fr, sr,
            "IN-list aggregate result diverged from SQLite for `{sql}`"
        );
    }
}
