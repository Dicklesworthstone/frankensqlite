//! bd-z3a05: `SELECT ... FROM t WHERE <indexed col> = <literal> AND <rowid range>` — keyset
//! pagination (`a = 5 AND id > 100 ORDER BY id LIMIT 10`) — seeks the `a = 5` block of a
//! single-column index bounded by the rowid range, as stock's
//! `SEARCH t USING INDEX t_a (a=? AND rowid>?)` does, instead of walking the rowid slice (usually
//! most of the table) and filtering on `a`. The block is in rowid order, so the seek also serves
//! `ORDER BY <rowid>` ASC and DESC. Every case compares rows in the order each engine returns them
//! against C SQLite; the plan check proves the index is opened for the bounded shapes, and a
//! composite-only table proves the lane declines when its entries are not in rowid order.
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

async fn frank_rows(c: &Connection, sql: &str) -> Vec<Vec<String>> {
    c.query(sql)
        .await
        .unwrap_or_else(|e| panic!("frank `{sql}`: {e}"))
        .iter()
        .map(|row| row.values().iter().map(render).collect())
        .collect()
}

fn sqlite_rows(c: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = c.prepare(sql).unwrap();
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        Ok((0..n)
            .map(|i| match row.get_unwrap::<_, rusqlite::types::Value>(i) {
                rusqlite::types::Value::Null => "NULL".to_owned(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f:?}"),
                rusqlite::types::Value::Text(s) => format!("'{s}'"),
                rusqlite::types::Value::Blob(b) => format!(
                    "X'{}'",
                    b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                ),
            })
            .collect())
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

/// Whether the program opens the index `index` (a read cursor on it).
async fn opens_index(c: &Connection, sql: &str, index: &str) -> bool {
    c.query(&format!("EXPLAIN {sql}"))
        .await
        .unwrap()
        .iter()
        .any(|row| {
            let values = row.values();
            matches!(values.get(1), Some(SqliteValue::Text(op)) if op.to_string() == "OpenRead")
                && values.iter().any(|value| {
                    matches!(value, SqliteValue::Text(text)
                        if text.to_string().eq_ignore_ascii_case(index))
                })
        })
}

async fn setup() -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    let mut ddl = vec![
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, c INTEGER, x TEXT);".to_owned(),
        "CREATE INDEX t_a ON t(a);".to_owned(),
        "CREATE INDEX t_x ON t(x);".to_owned(),
        // Only a composite index on (a, c): its `a = 5` entries are ordered by c, not rowid.
        "CREATE TABLE u (id INTEGER PRIMARY KEY, a INTEGER, c INTEGER);".to_owned(),
        "CREATE INDEX u_ac ON u(a, c);".to_owned(),
    ];
    for i in 1..=1500_i64 {
        let a = if i % 37 == 0 {
            "NULL".to_owned()
        } else {
            format!("{}", i % 20)
        };
        ddl.push(format!(
            "INSERT INTO t VALUES ({i}, {a}, {}, 'v{}');",
            i % 12,
            i % 7
        ));
        ddl.push(format!(
            "INSERT INTO u VALUES ({i}, {a}, {});",
            (1500 - i) % 9
        ));
    }
    for statement in &ddl {
        f.execute(statement).await.unwrap();
        r.execute_batch(statement).unwrap();
    }
    (f, r)
}

#[test]
fn eq_rowid_bounded_seek_matches_sqlite_in_order() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup().await;
        // Bounded shapes on a single-column index: the lane must take the index.
        let bounded = [
            "SELECT id, c FROM t WHERE a = 5 AND id > 100 ORDER BY id LIMIT 10",
            "SELECT id, c FROM t WHERE a = 5 AND id > 100 ORDER BY id DESC LIMIT 10",
            "SELECT id FROM t WHERE a = 5 AND id >= 100 AND id < 400 ORDER BY id",
            "SELECT id FROM t WHERE a = 5 AND id > 100 AND id <= 400 ORDER BY id DESC",
            "SELECT id FROM t WHERE a = 5 AND id BETWEEN 50 AND 300 ORDER BY id DESC",
            "SELECT id FROM t WHERE a = 5 AND id BETWEEN 300 AND 50 ORDER BY id",
            "SELECT id, c FROM t WHERE a = 5 AND id > 100 ORDER BY id LIMIT 5 OFFSET 3",
            "SELECT id, c FROM t WHERE a = 5 AND id < 900 ORDER BY id DESC LIMIT 4 OFFSET 2",
            "SELECT id, x FROM t WHERE x = 'v3' AND id < 200 ORDER BY id DESC LIMIT 7",
            "SELECT id, x FROM t WHERE x = 'v3' AND id >= 1400 ORDER BY id",
            "SELECT id FROM t WHERE a = 5 AND id > 100 AND c = 3 ORDER BY id",
            "SELECT id FROM t WHERE a = 99 AND id > 0 ORDER BY id",
            "SELECT id FROM t WHERE a = 5 AND id > 100000 ORDER BY id",
            "SELECT id FROM t WHERE a = 5 AND id < 0 ORDER BY id DESC",
            "SELECT id FROM t WHERE a = 5 AND rowid > 700 ORDER BY rowid LIMIT 3",
            "SELECT id FROM t WHERE id > 1000 AND a = 7 ORDER BY id",
            "SELECT * FROM t WHERE a = 0 AND id > 1460 ORDER BY id",
        ];
        for sql in bounded {
            assert_eq!(frank_rows(&f, sql).await, sqlite_rows(&r, sql), "`{sql}`");
            assert!(
                opens_index(&f, sql, "t_a").await || opens_index(&f, sql, "t_x").await,
                "bd-z3a05: `{sql}` must seek the equality index, not walk the rowid slice"
            );
        }
        // Shapes outside the lane must stay correct: a composite index (entries not in rowid
        // order within `a`), a bound that is not an integer literal, no rowid bound at all.
        let other = [
            "SELECT id, c FROM u WHERE a = 5 AND id > 100 ORDER BY id LIMIT 10",
            "SELECT id, c FROM u WHERE a = 5 AND id > 100 ORDER BY id DESC LIMIT 10",
            "SELECT id FROM t WHERE a = 5 AND id > -5 ORDER BY id LIMIT 6",
            "SELECT id FROM t WHERE a = 5 AND id > 10.5 ORDER BY id LIMIT 6",
            "SELECT id FROM t WHERE a = 5 ORDER BY id LIMIT 6",
            "SELECT id FROM t WHERE a = '5' AND id > 100 ORDER BY id LIMIT 6",
            "SELECT id FROM t WHERE a IS NULL AND id > 100 ORDER BY id LIMIT 6",
        ];
        for sql in other {
            assert_eq!(frank_rows(&f, sql).await, sqlite_rows(&r, sql), "`{sql}`");
        }
    });
}
