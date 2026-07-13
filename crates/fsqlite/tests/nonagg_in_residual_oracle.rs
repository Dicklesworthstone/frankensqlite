//! bd-nonagg-in-list-residual: `SELECT ... FROM t WHERE <int-indexed col> IN (<ints>) AND <residual>`
//! seeks each IN-list value's run and filters the residual per row, instead of full-scanning. The IN
//! emitter always opens the table, so the residual reads any column and ALL outputs (covering too) work;
//! IN is not a single-eq prefix, so no composite-prefix-range interaction. Exact IN (no residual) still
//! seeks byte-identically. Byte-set-identical to C SQLite.
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
fn check(label: &str, ddl: &[&str]) {
    let (f, r) = setup(ddl);
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
            &format!(
                "INSERT INTO t VALUES ({i}, {a}, {c}, 'v{}', 'k{}');",
                i % 7,
                i % 9
            ),
        );
    }
    // IN + residual: seek the IN runs, filter the residual. Covering (id/a) and non-covering (x/c/*).
    let seeks = [
        "SELECT x FROM t WHERE a IN (1,3,5) AND c = 5",
        "SELECT id FROM t WHERE a IN (5,6,7) AND c = 3",
        "SELECT * FROM t WHERE a IN (1,2) AND c > 5",
        "SELECT c FROM t WHERE a IN (0,1,2,3) AND c != 5 AND x = 'v3'",
        "SELECT id, x FROM t WHERE a IN (10,11,12) AND c BETWEEN 2 AND 8",
        "SELECT id FROM t WHERE a IN (999) AND c = 3", // zero-match value
        "SELECT a FROM t WHERE a IN (5,6) AND c = 3",
    ];
    for sql in seeks {
        cmp(&f, &r, sql, label);
        assert!(
            has_seek(&f, sql),
            "[{label}] IN+residual must seek: `{sql}`"
        );
    }
    // Exact IN (no residual) must STILL seek (byte-identical to before this change).
    for sql in [
        "SELECT id FROM t WHERE a IN (1,2,3)",
        "SELECT x FROM t WHERE a IN (5,6,7)",
    ] {
        cmp(&f, &r, sql, label);
        assert!(
            has_seek(&f, sql),
            "[{label}] exact IN must still seek: `{sql}`"
        );
    }
}
#[test]
fn nonagg_in_residual_matches_sqlite() {
    check(
        "single",
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, c INTEGER, x TEXT, s TEXT);",
            "CREATE INDEX idx_a ON t(a);",
            "CREATE INDEX idx_c ON t(c);",
        ],
    );
    check(
        "shadowed",
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, c INTEGER, x TEXT, s TEXT);",
            "CREATE INDEX idx_ax ON t(a, x);",
            "CREATE INDEX idx_a ON t(a);",
        ],
    );
}
