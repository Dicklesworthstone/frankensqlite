#![recursion_limit = "512"]

//! bd-mw9m3: `PRAGMA table_info` / `table_xinfo` / `pragma_table_info()` on
//! a view resolve the view's columns first, as stock's
//! sqlite3ViewGetColumnNames does. A circular view is "view X is circularly
//! defined" (X the first view met twice) and a view over a dropped table is
//! "no such table: main.t", as when the view is selected from. Main at
//! 7d09e6417 returned one bogus column named `*` for the circular views and
//! the stale column list for the broken one.
//!
//! Oracle: bundled stock SQLite runs the same script, in memory and on a
//! file. The good views use typed columns, so the comparison does not depend
//! on the declared type stock reports for an untyped column (BLOB).

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const SCRIPT: &[&str] = &[
    "CREATE VIEW c AS SELECT * FROM c",
    "CREATE VIEW ma AS SELECT * FROM mb",
    "CREATE VIEW mb AS SELECT * FROM ma",
    "CREATE VIEW top AS SELECT * FROM ma",
    "CREATE TABLE t(a INT, b TEXT)",
    "CREATE VIEW v AS SELECT a FROM t",
    "CREATE VIEW vgood AS SELECT a, b AS bee FROM t",
    "CREATE VIEW vsub AS SELECT * FROM (SELECT * FROM c)",
    "CREATE VIEW vcol(p) AS SELECT * FROM c",
    "CREATE TEMP VIEW tc AS SELECT * FROM tc",
    "PRAGMA table_info(c)",
    "PRAGMA table_info(ma)",
    "PRAGMA table_info(top)",
    "PRAGMA table_xinfo(mb)",
    "PRAGMA main.table_info(c)",
    "PRAGMA table_info(vsub)",
    "PRAGMA table_info(vcol)",
    "PRAGMA table_info(tc)",
    "PRAGMA temp.table_info(tc)",
    "SELECT * FROM pragma_table_info('c')",
    "SELECT count(*) FROM pragma_table_info('ma')",
    "SELECT * FROM c",
    "PRAGMA table_info(vgood)",
    "PRAGMA table_xinfo(vgood)",
    "SELECT name, type FROM pragma_table_info('v')",
    "PRAGMA table_info(nosuch)",
    "DROP TABLE t",
    "PRAGMA table_info(v)",
    "SELECT * FROM pragma_table_info('v')",
    "PRAGMA table_info(vgood)",
    "SELECT * FROM v",
    "CREATE TABLE t(a INT, b TEXT)",
    "PRAGMA table_info(vgood)",
];

fn stock_value(value: rusqlite::types::ValueRef<'_>) -> String {
    use rusqlite::types::ValueRef;
    match value {
        ValueRef::Null => "null".to_owned(),
        ValueRef::Integer(n) => format!("i:{n}"),
        ValueRef::Real(r) => format!("r:{r}"),
        ValueRef::Text(text) => format!("t:{}", String::from_utf8_lossy(text)),
        ValueRef::Blob(blob) => format!("b:{blob:?}"),
    }
}

fn fsqlite_value(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "null".to_owned(),
        SqliteValue::Integer(n) => format!("i:{n}"),
        SqliteValue::Float(r) => format!("r:{r}"),
        SqliteValue::Text(text) => format!("t:{}", text.as_str()),
        SqliteValue::Blob(blob) => format!("b:{blob:?}"),
    }
}

fn stock_run(path: &str) -> Vec<String> {
    let db = rusqlite::Connection::open(path).expect("stock open");
    SCRIPT
        .iter()
        .map(|sql| {
            let run = || -> rusqlite::Result<Vec<String>> {
                let mut statement = db.prepare(sql)?;
                let columns = statement.column_count();
                let mut rows = statement.query([])?;
                let mut rendered = Vec::new();
                while let Some(row) = rows.next()? {
                    let values: Vec<String> = (0..columns)
                        .map(|index| row.get_ref(index).map(stock_value))
                        .collect::<rusqlite::Result<_>>()?;
                    rendered.push(values.join("|"));
                }
                Ok(rendered)
            };
            match run() {
                Ok(rows) => format!("{sql} => ok {rows:?}"),
                Err(rusqlite::Error::SqliteFailure(_, Some(message))) => {
                    format!("{sql} => error: {message}")
                }
                Err(other) => format!("{sql} => error: {other}"),
            }
        })
        .collect()
}

async fn fsqlite_run(path: &str) -> Vec<String> {
    let conn = Connection::open(path).await.expect("open");
    let mut results = Vec::new();
    for sql in SCRIPT {
        results.push(match conn.query(sql).await {
            Ok(rows) => {
                let rendered: Vec<String> = rows
                    .iter()
                    .map(|row| {
                        row.values()
                            .iter()
                            .map(fsqlite_value)
                            .collect::<Vec<_>>()
                            .join("|")
                    })
                    .collect();
                format!("{sql} => ok {rendered:?}")
            }
            Err(error) => format!("{sql} => error: {error}"),
        });
    }
    conn.close().await.expect("close");
    results
}

#[test]
fn view_table_info_resolves_the_view_like_stock() {
    let dir = tempfile::tempdir().expect("tempdir");
    asupersync::test_utils::run_test(|| async move {
        let mut mismatches = Vec::new();
        for (index, store) in ["memory", "file"].into_iter().enumerate() {
            let (stock_path, fsqlite_path) = if store == "memory" {
                (":memory:".to_owned(), ":memory:".to_owned())
            } else {
                let stock = dir.path().join(format!("stock{index}.db"));
                let fsqlite = dir.path().join(format!("fsqlite{index}.db"));
                (
                    stock.to_str().expect("utf-8 path").to_owned(),
                    fsqlite.to_str().expect("utf-8 path").to_owned(),
                )
            };
            let want = stock_run(&stock_path);
            let got = fsqlite_run(&fsqlite_path).await;
            for (got, want) in got.iter().zip(&want) {
                if got != want {
                    mismatches.push(format!("{store}\n  fsqlite: {got}\n  stock:   {want}"));
                }
            }
            assert_eq!(got.len(), want.len());
        }
        for mismatch in &mismatches {
            eprintln!("event=view_table_info_mismatch {mismatch}");
        }
        assert!(
            mismatches.is_empty(),
            "{} mismatches; first: {}",
            mismatches.len(),
            mismatches[0]
        );
    });
}
