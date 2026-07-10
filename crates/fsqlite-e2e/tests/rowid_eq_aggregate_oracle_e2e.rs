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
