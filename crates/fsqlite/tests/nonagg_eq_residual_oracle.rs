//! bd-nonagg-eq-residual: `SELECT <non-covering cols> FROM t WHERE <indexed col> = <lit> AND <residual>`
//! seeks the exact `col = lit` block on its index and filters the residual per row (heuristic-chain
//! fallback, after rowid/range/index_eq) via codegen_select_index_equality_scan with residual_filter=true,
//! instead of full-scanning. The eq block is emitted in rowid order (= the full scan's order within it),
//! so byte-identical. Gated to a NON-covering output so the emitter opens the table and the residual can
//! read any column; covering outputs (SELECT id / SELECT a) correctly DECLINE to the full scan.
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
fn frank_rows(c: &Connection, sql: &str) -> Vec<Vec<String>> {
    let mut r: Vec<Vec<String>> = c
        .query(sql)
        .unwrap_or_else(|e| panic!("frank `{sql}`: {e}"))
        .iter()
        .map(|row| row.values().iter().map(render).collect())
        .collect();
    r.sort();
    r
}
fn sqlite_rows(c: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
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
    c.query(&format!("EXPLAIN {sql}")).unwrap().iter().any(|row| matches!(row.values().get(1), Some(SqliteValue::Text(op)) if op.to_string().starts_with("Seek")))
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
    assert_eq!(
        frank_rows(f, sql),
        sqlite_rows(r, sql),
        "[{l}] diverged: `{sql}`"
    );
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
    // Output includes a column (`c` / `s` / `*`) not in idx_a NOR the composite idx_ax used by the
    // shadowed schema, so it is non-covering on BOTH schemas and must seek.
    let seeks = [
        "SELECT * FROM t WHERE a = 5 AND c = 5",
        "SELECT c FROM t WHERE a = 5 AND c = 5",
        "SELECT c, id FROM t WHERE a = 5 AND c > 6",
        "SELECT c FROM t WHERE a = 5 AND c = 5 AND s = 'k3'",
        "SELECT c FROM t WHERE a = 5 AND c != 5",
        "SELECT * FROM t WHERE a = 999 AND c = 5",
        "SELECT c FROM t WHERE a = 0 AND c = 0",
        "SELECT c, s FROM t WHERE a = 3 AND c BETWEEN 2 AND 8",
    ];
    for sql in seeks {
        cmp(&f, &r, sql, label);
        assert!(
            has_seek(&f, sql),
            "[{label}] non-covering eq+residual must seek: `{sql}`"
        );
    }
    // Covering outputs correctly DECLINE to the full scan (results still correct): the rowid (id) and
    // the indexed column (a) are covering on idx_a; `x` is covering on the shadowed idx_ax.
    for sql in [
        "SELECT id FROM t WHERE a = 5 AND c = 5",
        "SELECT a FROM t WHERE a = 5 AND c = 5",
        "SELECT x FROM t WHERE a = 5 AND c = 5",
    ] {
        cmp(&f, &r, sql, label);
    }
    cmp(&f, &r, "SELECT id FROM t WHERE a = 5", label);
}
#[test]
fn nonagg_eq_residual_matches_sqlite() {
    check(
        "single idx_a",
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, c INTEGER, x TEXT, s TEXT);",
            "CREATE INDEX idx_a ON t(a);",
            "CREATE INDEX idx_c ON t(c);",
        ],
    );
    check(
        "shadowed idx_a",
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, c INTEGER, x TEXT, s TEXT);",
            "CREATE INDEX idx_ax ON t(a, x);",
            "CREATE INDEX idx_a ON t(a);",
        ],
    );
}
#[test]
fn nonagg_eq_residual_text_prefix() {
    let (f, r) = setup(&[
        "CREATE TABLE t (id INTEGER PRIMARY KEY, s TEXT, c INTEGER, x TEXT);",
        "CREATE INDEX idx_s ON t(s);",
    ]);
    for i in 1..=300_i64 {
        ins(
            &f,
            &r,
            &format!(
                "INSERT INTO t VALUES ({i}, 'k{}', {}, 'p{}');",
                i % 8,
                i % 10,
                i % 4
            ),
        );
    }
    for sql in [
        "SELECT c FROM t WHERE s = 'k3' AND c = 4",
        "SELECT * FROM t WHERE s = 'k3' AND c > 5",
    ] {
        cmp(&f, &r, sql, "text-prefix");
        assert!(has_seek(&f, sql), "text eq+residual must seek: `{sql}`");
    }
}
