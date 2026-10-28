#![recursion_limit = "512"]

//! bd-bymr0: an UPDATE on a WITHOUT ROWID table, and an upsert's DO UPDATE on
//! any table, evaluated their SET terms against the registers the terms were
//! rewriting. `SET a = b, b = a` stored b twice, `SET a = 'z' || a` read its
//! own partial result, and a correlated subquery reading an assigned column
//! read NULL. Stock evaluates every SET term against the row as it was.
//!
//! Every statement runs alone on a fresh target and is compared with
//! rusqlite's bundled SQLite: the change count and the target's rows. In
//! memory and in a file, ad hoc and prepared.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => format!("int:{n}"),
        SqliteValue::Float(f) => format!("real:{f}"),
        SqliteValue::Text(s) => format!("text:{s}"),
        SqliteValue::Blob(b) => format!("blob:{b:?}"),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => format!("int:{n}"),
        rusqlite::types::Value::Real(f) => format!("real:{f}"),
        rusqlite::types::Value::Text(s) => format!("text:{s}"),
        rusqlite::types::Value::Blob(b) => format!("blob:{b:?}"),
    }
}

const SCHEMA: &str = "CREATE TABLE s(n INTEGER); INSERT INTO s VALUES (1), (2), (3), (4), (5);";

const TARGETS: &[(&str, &str)] = &[
    (
        "rowid",
        "CREATE TABLE w(id INTEGER PRIMARY KEY, a, b, n INTEGER);",
    ),
    (
        "text primary key",
        "CREATE TABLE w(id TEXT PRIMARY KEY, a, b, n INTEGER);",
    ),
    (
        "without rowid",
        "CREATE TABLE w(id INTEGER PRIMARY KEY, a, b, n INTEGER) WITHOUT ROWID;",
    ),
    (
        "without rowid, composite key last, index",
        "CREATE TABLE w(a, b, n INTEGER, id INTEGER, PRIMARY KEY (id, n)) WITHOUT ROWID;\
         CREATE INDEX w_b ON w(b);",
    ),
];

const RESET: &str = "DROP TABLE IF EXISTS w;";
const FILL: &str =
    "INSERT INTO w(id, a, b, n) VALUES (1, 'x', 'y', 1), (2, 'p', 'q', 2), (3, 'r', 's', 3);";
const ROWS: &str = "SELECT id, a, b, n FROM w ORDER BY id, n";

const STATEMENTS: &[&str] = &[
    "UPDATE w SET a = b, b = a",
    "UPDATE w SET a = 'z' || a",
    "UPDATE w SET b = a || '!', a = b",
    "UPDATE w SET n = n + 1, a = n",
    "UPDATE w SET (a, b) = (b, a)",
    "UPDATE w SET a = b, b = a WHERE n >= 2",
    "UPDATE w SET id = id + 10, a = id",
    "UPDATE w SET n = (SELECT count(*) FROM s WHERE s.n <= w.n)",
    "UPDATE w SET n = (SELECT max(s.n) FROM s WHERE s.n < w.n), a = n",
    "UPDATE w SET a = (SELECT s.n FROM s WHERE s.n = w.n + 1)",
    "UPDATE w SET n = EXISTS (SELECT 1 FROM s WHERE s.n = w.n + 3)",
    "UPDATE w SET a = b, b = s.n FROM s WHERE s.n = w.n",
    "INSERT INTO w(id, a, b, n) VALUES (1, 'm', 'n', 1) ON CONFLICT DO UPDATE SET a = b, b = a",
    "INSERT INTO w(id, a, b, n) VALUES (1, 'm', 'n', 1) \
     ON CONFLICT DO UPDATE SET a = 'z' || a, b = a",
    "INSERT INTO w(id, a, b, n) VALUES (2, 'm', 'n', 2) \
     ON CONFLICT DO UPDATE SET a = excluded.a || a, b = a WHERE b <> 'zzz'",
    "INSERT INTO w(id, a, b, n) VALUES (2, 'm', 'n', 2) \
     ON CONFLICT DO UPDATE SET n = n + 10, a = n",
    "INSERT INTO w(id, a, b, n) VALUES (1, 'm', 'n', 1) \
     ON CONFLICT DO UPDATE SET id = id + 10, a = id",
    "INSERT INTO w(id, a, b, n) VALUES (3, 'm', 'n', 3) \
     ON CONFLICT DO UPDATE SET a = (SELECT count(*) FROM s WHERE s.n <= w.n), n = a",
];

async fn compare(file_backed: bool) -> (Vec<String>, usize) {
    let dir = tempfile::tempdir().unwrap();
    let (f, r) = if file_backed {
        (
            Connection::open(dir.path().join("f.db").to_str().unwrap())
                .await
                .unwrap(),
            rusqlite::Connection::open(dir.path().join("r.db")).unwrap(),
        )
    } else {
        (
            Connection::open(":memory:").await.unwrap(),
            rusqlite::Connection::open_in_memory().unwrap(),
        )
    };
    f.execute_batch(SCHEMA).await.unwrap();
    r.execute_batch(SCHEMA).unwrap();
    let db = if file_backed { "file" } else { "memory" };
    let mut diffs = Vec::new();
    let mut runs = 0usize;
    for (target, ddl) in TARGETS {
        for sql in STATEMENTS {
            for prepared in [false, true] {
                runs += 1;
                for setup in [RESET, *ddl, FILL] {
                    f.execute_batch(setup).await.unwrap();
                    r.execute_batch(setup).unwrap();
                }
                let stock = r.execute(sql, []).map_err(|e| e.to_string());
                let frank = if prepared {
                    match f.prepare(sql).await {
                        Ok(stmt) => stmt.execute().await,
                        Err(e) => Err(e),
                    }
                } else {
                    f.execute(sql).await
                }
                .map_err(|e| e.to_string());
                let path = format!("[{db}, {target}, prepared={prepared}] `{sql}`");
                let changes_match = match (&frank, &stock) {
                    (Ok(frank), Ok(stock)) => frank == stock,
                    (Err(_), Err(_)) => true,
                    _ => false,
                };
                if !changes_match {
                    diffs.push(format!(
                        "  {path}\n     changes: frank= {frank:?} stock= {stock:?}"
                    ));
                }
                let fr: Vec<Vec<String>> = f
                    .query(ROWS)
                    .await
                    .unwrap()
                    .iter()
                    .map(|row| row.values().iter().map(tag_f).collect())
                    .collect();
                let mut st = r.prepare(ROWS).unwrap();
                let rr: Vec<Vec<String>> = st
                    .query_map([], |row| {
                        Ok((0..4)
                            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                            .collect())
                    })
                    .unwrap()
                    .collect::<Result<_, _>>()
                    .unwrap();
                if fr != rr {
                    diffs.push(format!("  {path}\n     frank= {fr:?}\n     stock= {rr:?}"));
                }
            }
        }
    }
    f.close().await.unwrap();
    (diffs, runs)
}

#[test]
fn set_terms_read_the_old_row() {
    asupersync::test_utils::run_test(|| async {
        let (mut diffs, mut runs) = compare(false).await;
        let (file_diffs, file_runs) = compare(true).await;
        diffs.extend(file_diffs);
        runs += file_runs;
        assert!(
            diffs.is_empty(),
            "{} divergence(s) over {runs} statement run(s) from rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
