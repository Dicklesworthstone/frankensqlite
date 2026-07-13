//! bd-nonagg-rowid-in-order: `SELECT ... FROM t WHERE <rowid> IN (<ints>) ORDER BY <rowid>` is served
//! by the rowid IN seek (one SeekRowid per listed value) WITHOUT a sorter — the seek emits in ascending
//! rowid order (values are sorted+deduped), and DESC reverses the emit order. Compared IN ORDER against
//! C SQLite (order is the point, so results are NOT sorted before comparison). Negative cases confirm a
//! non-rowid or multi-term ORDER BY declines to the sorter.
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
// ORDERED: preserve row order (do NOT sort) — the lever's correctness is about output order.
fn frank_ord(c: &Connection, sql: &str) -> Vec<Vec<String>> {
    c.query(sql)
        .unwrap_or_else(|e| panic!("frank `{sql}`: {e}"))
        .iter()
        .map(|row| row.values().iter().map(render).collect())
        .collect()
}
fn sqlite_ord(c: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = c.prepare(sql).unwrap();
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
fn cmp_ord(f: &Connection, r: &rusqlite::Connection, sql: &str, l: &str) {
    assert_eq!(
        frank_ord(f, sql),
        sqlite_ord(r, sql),
        "[{l}] order diverged: `{sql}`"
    );
}
#[test]
fn nonagg_rowid_in_order_matches_sqlite() {
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
    // Served by the seek WITHOUT a sorter — verified in output order + Seek present.
    let seeks = [
        "SELECT * FROM t WHERE id IN (5,25,45,60) ORDER BY id", // ASC (default)
        "SELECT id, x FROM t WHERE id IN (60,45,25,5) ORDER BY id ASC", // SQL values unsorted -> ascending
        "SELECT * FROM t WHERE id IN (5,25,45,60) ORDER BY id DESC",    // DESC
        "SELECT x FROM t WHERE id IN (10,20,30,40) AND c = 3 ORDER BY id", // residual + ASC
        "SELECT id FROM t WHERE id IN (99999, 5, 250) ORDER BY id",     // zero-match -> [5,250]
        "SELECT id FROM t WHERE id IN (7,8,9) ORDER BY id DESC",        // -> [9,8,7]
        "SELECT c FROM t WHERE id IN (100,200,300) AND c != 5 ORDER BY id DESC", // residual + DESC
        "SELECT id FROM t WHERE id IN (5,5,25,25,45) ORDER BY id",      // duplicate values -> dedup
    ];
    for sql in seeks {
        cmp_ord(&f, &r, sql, "rowid-in-order");
        assert!(
            has_seek(&f, sql),
            "rowid IN + ORDER BY rowid must seek: `{sql}`"
        );
    }
    // Negative: a non-rowid or multi-term ORDER BY must NOT take the seek (falls to the sorter) — still correct.
    let declines = [
        "SELECT * FROM t WHERE id IN (5,25,45) ORDER BY x", // ORDER BY non-rowid
        "SELECT * FROM t WHERE id IN (5,25,45) ORDER BY id, x", // two-term ORDER BY
        "SELECT * FROM t WHERE id IN (5,25,45) ORDER BY x DESC",
    ];
    for sql in declines {
        cmp_ord(&f, &r, sql, "declines");
        assert!(
            !has_seek(&f, sql),
            "non-rowid/multi-term ORDER BY must NOT seek: `{sql}`"
        );
    }
    // No ORDER BY still seeks (byte-identical to before this lever).
    assert!(
        has_seek(&f, "SELECT id FROM t WHERE id IN (1,2,3)"),
        "bare rowid IN must still seek"
    );
}
