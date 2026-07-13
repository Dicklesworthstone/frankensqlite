//! bd-nonagg-rowid-range-residual: `SELECT ... FROM t WHERE <rowid> <range> AND <residual>` walks only
//! the [lower, upper] rowid slice (rowid order = full-scan order) and filters the residual per row,
//! instead of full-scanning. The planner emits a FullTableScan directive (it models neither the rowid
//! range nor a non-indexed residual as seekable), which this hoists a residual-aware range scan ahead
//! of. Byte-identical to the full scan: same rows, same order, fewer visited. No secondary index here,
//! so every residual is non-seekable and the directive stays FullTableScan. Byte-set-identical to
//! C SQLite.
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
fn nonagg_rowid_range_residual_matches_sqlite() {
    // No secondary index: every residual predicate is non-seekable -> FullTableScan directive.
    let (f, r) = setup(&["CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, c INTEGER, x TEXT);"]);
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
    // Every case has a LOWER bound -> SeekGE/SeekGT is emitted (has_seek proves the hoist fired).
    let seeks = [
        "SELECT x FROM t WHERE id BETWEEN 50 AND 120 AND c = 5",
        "SELECT id FROM t WHERE id > 400 AND id < 450 AND a > 10",
        "SELECT * FROM t WHERE id >= 100 AND id <= 200 AND x = 'v3'",
        "SELECT c FROM t WHERE id BETWEEN 1 AND 50 AND c IS NULL",
        "SELECT id FROM t WHERE id BETWEEN 5 AND 10 AND c = 999", // residual FALSE for all -> empty
        "SELECT id, a FROM t WHERE id BETWEEN 300 AND 320 AND a = 6",
        "SELECT id FROM t WHERE id > 590 AND c = 3", // open upper, lower-bounded
        "SELECT x FROM t WHERE id BETWEEN 100 AND 300 AND c > 3 AND x = 'v2'", // multi-conjunct residual
        "SELECT id FROM t WHERE id >= 17 AND id <= 40 AND a IS NULL", // NULL residual (34 % 17 == 0)
    ];
    for sql in seeks {
        cmp(&f, &r, sql, "rowid-range-resid");
        assert!(has_seek(&f, sql), "rowid range+residual must seek: `{sql}`");
    }
    // Bare rowid range (no residual) still seeks byte-identically.
    for sql in [
        "SELECT id FROM t WHERE id BETWEEN 10 AND 20",
        "SELECT x FROM t WHERE id > 595",
    ] {
        cmp(&f, &r, sql, "bare");
        assert!(has_seek(&f, sql), "bare rowid range must seek: `{sql}`");
    }
}
