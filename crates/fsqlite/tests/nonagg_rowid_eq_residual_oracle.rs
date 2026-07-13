//! bd-nonagg-rowid-eq-residual: `SELECT ... FROM t WHERE <rowid> = <const> AND <residual>` does one
//! SeekRowid on the target row and filters the residual, instead of full-scanning. The planner emits a
//! RowidLookup directive but codegen's bare rowid extraction declines the conjunction, so the shape
//! otherwise falls through to a full scan; this hoists a residual-aware seek before the directive. The
//! emitter always opens the table, so the residual reads any column and ALL outputs work; bare
//! `rowid = const` still seeks via the existing directive. Byte-set-identical to C SQLite.
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
fn frank(c: &Connection, sql: &str) -> Vec<Vec<String>> {
    let mut r: Vec<Vec<String>> = c
        .query(sql)
        .unwrap_or_else(|e| panic!("frank `{sql}`: {e}"))
        .iter()
        .map(|row| row.values().iter().map(render).collect())
        .collect();
    r.sort();
    r
}
fn sqlite(c: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = c.prepare(sql).unwrap();
    let n = stmt.column_count();
    let mut r: Vec<Vec<String>> = stmt
        .query_map([], |row| {
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
        .collect();
    r.sort();
    r
}
fn has_seek(c: &Connection, sql: &str) -> bool {
    c.query(&format!("EXPLAIN {sql}")).unwrap().iter().any(|row| matches!(row.values().get(1), Some(SqliteValue::Text(o)) if o.to_string().starts_with("Seek")))
}
fn setup(ddl: &[&str]) -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in ddl {
        f.execute(s).unwrap();
        r.execute_batch(s).unwrap();
    }
    (f, r)
}
fn ins(f: &Connection, r: &rusqlite::Connection, s: &str) {
    f.execute(s).unwrap();
    r.execute_batch(s).unwrap();
}
fn cmp(f: &Connection, r: &rusqlite::Connection, sql: &str, l: &str) {
    assert_eq!(frank(f, sql), sqlite(r, sql), "[{l}] diverged: `{sql}`");
}
#[test]
fn nonagg_rowid_eq_residual_matches_sqlite() {
    let (f, r) = setup(&[
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, c INTEGER, x TEXT);",
        "CREATE INDEX idx_c ON t(c);",
    ]);
    for i in 1..=600_i64 {
        let a = if i % 17 == 0 {
            "NULL".to_owned()
        } else {
            format!("{}", i % 20)
        };
        let c = if i % 19 == 0 {
            "NULL".to_owned()
        } else {
            format!("{}", i % 12)
        };
        ins(
            &f,
            &r,
            &format!("INSERT INTO t VALUES ({i}, {a}, {c}, 'v{}');", i % 7),
        );
    }
    let seeks = [
        "SELECT x FROM t WHERE id = 25 AND c = 1",  // match
        "SELECT id FROM t WHERE id = 20 AND c = 8", // match
        "SELECT * FROM t WHERE id = 40 AND c > 3",
        "SELECT c FROM t WHERE id = 200 AND c != 5 AND x = 'v3'",
        "SELECT a, c FROM t WHERE id = 51 AND c BETWEEN 2 AND 8",
        "SELECT id FROM t WHERE id = 5 AND c = 999", // row exists, residual FALSE -> empty
        "SELECT id FROM t WHERE id = 99999 AND c = 3", // rowid miss -> empty
        "SELECT id FROM t WHERE id = 17 AND a IS NULL", // NULL residual (17 % 17 == 0)
        "SELECT x FROM t WHERE 8 = id AND c = 8",    // rowid on the RHS
    ];
    for sql in seeks {
        cmp(&f, &r, sql, "rowid-eq-resid");
        assert!(has_seek(&f, sql), "rowid=const+residual must seek: `{sql}`");
    }
    // Bare `rowid = const` (no residual) still seeks via the existing RowidLookup directive.
    for sql in [
        "SELECT id FROM t WHERE id = 3",
        "SELECT x FROM t WHERE id = 300",
    ] {
        cmp(&f, &r, sql, "bare");
        assert!(has_seek(&f, sql), "bare rowid = const must seek: `{sql}`");
    }
}
