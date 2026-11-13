#![recursion_limit = "512"]

//! bd-i95tk: the schema text stock SQLite stores is the statement's own text.
//!
//! - `sqlite_master.sql` holds `CREATE [UNIQUE ]<KIND> ` followed by the
//!   source from the object name onward, verbatim (whitespace and comments
//!   kept); `TEMP`, a schema prefix and `IF NOT EXISTS` are dropped. Tables
//!   already did this; indexes, views and triggers kept `IF NOT EXISTS` (or
//!   fell back to a re-rendered AST when schema-qualified).
//! - TEMP views and triggers follow the same rule in `sqlite_temp_master`
//!   (a TEMP view's text was truncated to `CREATE VIEW <name>`).
//! - ALTER TABLE ADD COLUMN splices the column definition exactly as written.
//! - All of this holds inside a multi-statement batch as well, which used to
//!   store AST-rendered text.
//!
//! Every statement runs on FrankenSQLite and stock SQLite (rusqlite), one at a
//! time on one pair of connections and as a single batch on another.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("X'{}'", b.len()),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f:?}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("X'{}'", b.len()),
    }
}

async fn frank(f: &Connection, sql: &str) -> Vec<Vec<String>> {
    match f.query(sql).await {
        Ok(rows) => rows
            .iter()
            .map(|r| r.values().iter().map(tag_f).collect())
            .collect(),
        Err(e) => vec![vec![format!("<ERR {e}>")]],
    }
}

fn stock(r: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut st = r.prepare(sql).expect("stock prepare");
    let n = st.column_count();
    st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

const SETUP: &[&str] = &[
    "CREATE TEMP TABLE  IF NOT EXISTS  tt ( x  INTEGER,  y text /* c */ )",
    "CREATE TEMPORARY VIEW IF NOT EXISTS tv AS SELECT  x+1  FROM tt",
    "CREATE TEMP TRIGGER IF NOT EXISTS ttr AFTER INSERT ON tt BEGIN SELECT 1; END",
    "CREATE TABLE IF NOT EXISTS m ( a ,b )",
    "CREATE UNIQUE INDEX IF NOT EXISTS mi ON m(a)",
    "CREATE INDEX IF NOT EXISTS main.mj ON m(b) WHERE b > 0",
    "CREATE VIEW IF NOT EXISTS mv AS SELECT a FROM m",
    "CREATE VIEW main.mv2 AS SELECT  b  FROM m",
    "CREATE TRIGGER IF NOT EXISTS mtr AFTER INSERT ON m BEGIN SELECT 1; END",
    // Trailing text after the last token: an index keeps it, a view keeps the
    // comment but not the whitespace, a trigger and a table keep neither.
    "CREATE INDEX mk ON m(a, b) /* index tail */  ",
    "CREATE VIEW mv3 AS SELECT a FROM m /* view tail */  ",
    "CREATE TRIGGER mtr2 AFTER DELETE ON m BEGIN SELECT 2; END /* trigger tail */ ",
    "CREATE TABLE n ( z ) /* table tail */ ",
    "ALTER TABLE m ADD COLUMN  c   INTEGER   DEFAULT ( 1 +  2 ) ",
    "ALTER TABLE m ADD d DEFAULT 'x' /* trailing */",
    "ALTER TABLE main.m ADD COLUMN e TEXT  COLLATE nocase ;",
];

const QUERIES: &[&str] = &[
    "SELECT type, name, tbl_name, sql FROM sqlite_temp_master \
     WHERE type IN ('view', 'trigger') ORDER BY name",
    "SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY name",
    "SELECT name, dflt_value FROM pragma_table_info('m') ORDER BY cid",
];

async fn compare(f: &Connection, r: &rusqlite::Connection, label: &str, failures: &mut Vec<String>) {
    for sql in QUERIES {
        let (fv, sv) = (frank(f, sql).await, stock(r, sql));
        if fv != sv {
            failures.push(format!("{label} `{sql}`:\n  frank {fv:?}\n  stock {sv:?}"));
        }
    }
}

#[test]
fn stored_schema_text_is_the_statement_text_as_stock_stores_it() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut failures = Vec::new();

        // One statement per call.
        let path = dir.path().join("each.db");
        let f = Connection::open(path.to_str().expect("utf-8 path"))
            .await
            .expect("open");
        let r = rusqlite::Connection::open_in_memory().expect("stock open");
        for sql in SETUP {
            let fo = f.execute(sql).await.map(|_| ()).map_err(|e| e.to_string());
            let so = r.execute_batch(sql).map_err(|e| e.to_string());
            assert_eq!(fo.is_ok(), so.is_ok(), "`{sql}`: frank {fo:?} vs stock {so:?}");
        }
        compare(&f, &r, "[statement by statement]", &mut failures).await;
        f.close().await.expect("close");

        // Statement by statement through `query`, as the CLI runs them.
        let path = dir.path().join("query.db");
        let f = Connection::open(path.to_str().expect("utf-8 path"))
            .await
            .expect("open");
        let r = rusqlite::Connection::open_in_memory().expect("stock open");
        for sql in SETUP {
            f.query(sql).await.expect("frank query");
            r.execute_batch(sql).expect("stock");
        }
        compare(&f, &r, "[through query]", &mut failures).await;
        f.close().await.expect("close");

        // The same DDL as one batch.
        let batch = SETUP.join(";\n");
        let path = dir.path().join("batch.db");
        let f = Connection::open(path.to_str().expect("utf-8 path"))
            .await
            .expect("open");
        let r = rusqlite::Connection::open_in_memory().expect("stock open");
        f.execute_batch(&batch).await.expect("frank batch");
        r.execute_batch(&batch).expect("stock batch");
        compare(&f, &r, "[one batch]", &mut failures).await;
        f.close().await.expect("close");

        assert!(
            failures.is_empty(),
            "{} differences from SQLite:\n{}",
            failures.len(),
            failures.join("\n")
        );
    });
}

/// A CREATE that fails before storing its text must not leave that text for
/// the next CREATE: one run through `query` used to store whatever text an
/// earlier, failed `execute` had left behind.
#[test]
fn a_failed_create_does_not_lend_its_text_to_a_later_create() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("stale.db");
        let f = Connection::open(path.to_str().expect("utf-8 path"))
            .await
            .expect("open");
        f.execute("CREATE TABLE t(a)").await.expect("create t");
        assert!(
            f.execute("CREATE TABLE t(a, b)").await.is_err(),
            "t already exists"
        );
        f.query("CREATE TABLE u(x INTEGER)")
            .await
            .expect("create u");
        assert_eq!(
            frank(&f, "SELECT name, sql FROM sqlite_master ORDER BY name").await,
            [
                ["'t'", "'CREATE TABLE t(a)'"],
                ["'u'", "'CREATE TABLE u(x INTEGER)'"]
            ]
        );
        f.close().await.expect("close");

        let r = rusqlite::Connection::open(&path).expect("stock open");
        assert_eq!(
            stock(&r, "PRAGMA integrity_check"),
            [["'ok'"]],
            "stock integrity_check"
        );
        assert_eq!(
            stock(&r, "SELECT name FROM pragma_table_info('u')"),
            [["'x'"]],
            "stock reads u with its own columns"
        );
    });
}

/// Stock ends the stored text differently per object kind. An index keeps
/// everything up to the terminating `;` (or the end of the input), trailing
/// whitespace and comments included; a view does the same but trims trailing
/// whitespace; a table and a trigger end at their last token. Text after the
/// `;` is never stored. A trailing `--` comment stored in an index or view must
/// not break later reads of the schema: reopen, VACUUM, a RENAME rewrite.
#[test]
fn stored_text_ends_where_stock_ends_it_for_each_kind() {
    const STATEMENTS: &[&str] = &[
        "CREATE TABLE t(c)",
        "CREATE INDEX i1 ON t(c) /* t */ ;",
        "CREATE INDEX i2 ON t(c)   ;   -- after the terminator",
        "CREATE INDEX i3 ON t(c) WHERE c > 0 /* t */",
        "CREATE INDEX i4 ON t(c) -- t",
        "CREATE INDEX i5 ON t(c) WHERE c <> ';' /* ; */ ;",
        "CREATE VIEW v1 AS SELECT 1 AS one /* t */   ;",
        "CREATE VIEW v2 AS SELECT 2 AS two   ",
        "CREATE VIEW v3 AS SELECT ';' AS semi -- t",
        "CREATE TRIGGER g1 AFTER INSERT ON t BEGIN SELECT 1; END /* t */ ;",
        "CREATE TABLE t2(a) /* t */ ;",
    ];
    const MASTER: &str = "SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY name";
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("tail.db");
        let f = Connection::open(path.to_str().expect("utf-8 path"))
            .await
            .expect("open");
        let r = rusqlite::Connection::open_in_memory().expect("stock open");
        for sql in STATEMENTS {
            f.execute(sql).await.expect("frank execute");
            r.execute_batch(sql).expect("stock execute");
        }
        assert_eq!(
            frank(&f, MASTER).await,
            stock(&r, MASTER),
            "statement by statement"
        );
        f.close().await.expect("close");

        // The stored tails survive a reopen, a VACUUM and a RENAME rewrite.
        let f = Connection::open(path.to_str().expect("utf-8 path"))
            .await
            .expect("reopen");
        for sql in [
            "INSERT INTO t VALUES (1), (2), (';')",
            "VACUUM",
            "ALTER TABLE t RENAME TO t_renamed",
        ] {
            f.execute(sql).await.expect(sql);
        }
        assert_eq!(
            frank(&f, "SELECT one, two, semi FROM v1, v2, v3").await,
            [["1", "2", "';'"]]
        );
        assert_eq!(
            frank(
                &f,
                "SELECT count(*) FROM t_renamed INDEXED BY i4 WHERE c > 0"
            )
            .await,
            [["3"]]
        );
        f.close().await.expect("close");
        let r = rusqlite::Connection::open(&path).expect("stock open");
        assert_eq!(
            stock(&r, "PRAGMA integrity_check"),
            [["'ok'"]],
            "stock integrity_check"
        );
        assert_eq!(
            stock(&r, "SELECT count(*) FROM t_renamed WHERE c > 0"),
            [["3"]],
            "stock reads the renamed table"
        );
    });
}
