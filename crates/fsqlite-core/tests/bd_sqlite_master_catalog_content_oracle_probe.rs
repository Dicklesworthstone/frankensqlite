#![recursion_limit = "512"]

//! sqlite_master catalog-content leaf-hunt (pane af49, 2026-08-21): frank vs
//! rusqlite over the schema catalog rows (type,name,tbl_name) — extending the
//! bd-y4yjq catalog-listing bug into the MAIN schema. Covers: a table plus the
//! auto-index created by a UNIQUE constraint (sqlite_autoindex_*), explicit
//! indexes, a view, a trigger (whose tbl_name is the table it's ON), the
//! sqlite_sequence bookkeeping table appearing once AUTOINCREMENT is used,
//! catalog rows disappearing after DROP, and name/tbl_name rewrites after
//! ALTER TABLE RENAME. The opaque `sql`/`rootpage` columns are NOT compared
//! (version/whitespace-specific); only type/name/tbl_name. Pass = keeper.

use fsqlite_core::connection::Connection;
use fsqlite_error::ErrorCode;
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

async fn fq(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    match conn.query(sql).await {
        Ok(rows) => rows
            .iter()
            .map(|r| r.values().iter().map(tag_f).collect())
            .collect(),
        Err(_) => vec![vec!["ERR".to_owned()]],
    }
}
fn rq(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let Ok(mut st) = conn.prepare(sql) else {
        return vec![vec!["ERR".to_owned()]];
    };
    let n = st.column_count();
    match st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect::<Vec<_>>())
    }) {
        Ok(rows) => rows
            .collect::<Result<Vec<_>, _>>()
            .unwrap_or_else(|_| vec![vec!["ERR".to_owned()]]),
        Err(_) => vec![vec!["ERR".to_owned()]],
    }
}
async fn ex(f: &Connection, r: &rusqlite::Connection, sql: &str) {
    let _ = f.execute(sql).await;
    let _ = r.execute(sql, []);
}

/// bd-2dtww: catalog SQL is untrusted persisted input, even when it parses.
/// Build the corruptions through stock's writable_schema so the table pages
/// remain valid and only the schema semantics differ from a healthy file.
#[test]
fn malformed_catalogs_are_rejected_before_schema_publication() {
    asupersync::test_utils::run_test(|| async {
        for (name, setup, corruption) in [
            (
                "duplicate_columns",
                "CREATE TABLE t(a INTEGER, b TEXT); INSERT INTO t VALUES(1, 'kept')",
                "UPDATE sqlite_schema SET sql='CREATE TABLE t(a INTEGER, A TEXT)' WHERE name='t'",
            ),
            (
                "quoted_duplicate_columns",
                "CREATE TABLE t(\"a b\" INTEGER, c TEXT)",
                "UPDATE sqlite_schema SET sql='CREATE TABLE t(\"a b\" INTEGER, [A B] TEXT)' WHERE name='t'",
            ),
            (
                "before_trigger_on_virtual_table",
                "CREATE VIRTUAL TABLE ft USING fts5(body)",
                "INSERT INTO sqlite_schema VALUES('trigger', 'tr', 'ft', 0, \
                 'CREATE TRIGGER tr BEFORE INSERT ON ft BEGIN SELECT 1; END')",
            ),
            (
                "after_trigger_on_virtual_table",
                "CREATE VIRTUAL TABLE ft USING fts5(body)",
                "INSERT INTO sqlite_schema VALUES('trigger', 'tr', 'ft', 0, \
                 'CREATE TRIGGER tr AFTER DELETE ON ft BEGIN SELECT 1; END')",
            ),
            (
                "instead_of_trigger_on_virtual_table",
                "CREATE VIRTUAL TABLE ft USING fts5(body)",
                "INSERT INTO sqlite_schema VALUES('trigger', 'tr', 'ft', 0, \
                 'CREATE TRIGGER tr INSTEAD OF UPDATE ON ft BEGIN SELECT 1; END')",
            ),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join(format!("{name}.db"));
            {
                let stock = rusqlite::Connection::open(&path).unwrap();
                stock.execute_batch(setup).unwrap();
                stock.execute_batch("PRAGMA writable_schema=ON").unwrap();
                stock.execute_batch(corruption).unwrap();
            }
            let original = std::fs::read(&path).unwrap();
            {
                // SQLite loads its catalog lazily, so force the read before
                // comparing its error class with FrankenSQLite's eager open.
                let stock = rusqlite::Connection::open(&path).unwrap();
                let error = stock
                    .prepare("SELECT name FROM sqlite_schema")
                    .expect_err("stock must reject the malformed catalog");
                assert_eq!(
                    error.sqlite_error_code(),
                    Some(rusqlite::ErrorCode::DatabaseCorrupt),
                    "{name}: {error}"
                );
            }
            for mode in ["existing", "schema_only", "import"] {
                let result = match mode {
                    "existing" => Connection::open_existing(path.to_str().unwrap()).await,
                    "schema_only" => Connection::open_schema_only(path.to_str().unwrap()).await,
                    "import" => Connection::import_bytes(&original).await,
                    _ => unreachable!(),
                };
                match result {
                    Ok(conn) => {
                        conn.close().await.unwrap();
                        panic!("{name}/{mode}: malformed catalog was accepted");
                    }
                    Err(error) => assert_eq!(
                        error.error_code(),
                        ErrorCode::Corrupt,
                        "{name}/{mode}: {error}"
                    ),
                }
                assert_eq!(
                    std::fs::read(&path).unwrap(),
                    original,
                    "{name}/{mode}: rejecting a malformed catalog must preserve its bytes"
                );
            }
        }
    });
}

#[test]
fn valid_catalog_semantics_remain_available() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("valid.db");
        {
            let stock = rusqlite::Connection::open(&path).unwrap();
            stock
                .execute_batch(
                    "CREATE TABLE t(\"ä\" INTEGER, \"Ä\" TEXT); \
                     INSERT INTO t VALUES(1, 'kept'); \
                     CREATE VIEW v AS SELECT * FROM t; \
                     CREATE TRIGGER t AFTER INSERT ON t BEGIN SELECT 1; END; \
                     CREATE TRIGGER v INSTEAD OF INSERT ON v BEGIN SELECT 1; END;",
                )
                .unwrap();
            #[cfg(feature = "ext-fts5")]
            stock
                .execute_batch(
                    "CREATE VIRTUAL TABLE ft USING fts5(body); \
                     INSERT INTO ft VALUES('preserved document');",
                )
                .unwrap();
        }
        let original = std::fs::read(&path).unwrap();
        let conn = Connection::open_schema_only(path.to_str().unwrap())
            .await
            .expect("valid tables, views, triggers and FTS5 must still open");
        assert_eq!(
            conn.query("SELECT * FROM t").await.unwrap()[0].values(),
            &[SqliteValue::Integer(1), SqliteValue::Text("kept".into())]
        );
        #[cfg(feature = "ext-fts5")]
        assert_eq!(
            conn.query("SELECT body FROM ft WHERE ft MATCH 'preserved'")
                .await
                .unwrap()[0]
                .values(),
            &[SqliteValue::Text("preserved document".into())]
        );
        conn.close().await.unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), original);
    });
}

#[test]
fn sqlite_master_catalog_content_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE users(id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT)",
            "CREATE TABLE posts(id INTEGER PRIMARY KEY, uid INTEGER, title TEXT)",
            "CREATE INDEX ix_posts_uid ON posts(uid)",
            "CREATE UNIQUE INDEX ux_posts_title ON posts(title)",
            "CREATE VIEW active_users AS SELECT id, name FROM users WHERE name IS NOT NULL",
            "CREATE TRIGGER trg_posts AFTER INSERT ON posts BEGIN UPDATE users SET name=name WHERE id=NEW.uid; END",
        ] {
            ex(&f, &r, s).await;
        }
        let mut diffs = Vec::new();
        let check =
            |label: &str, fr: Vec<Vec<String>>, rr: Vec<Vec<String>>, d: &mut Vec<String>| {
                if fr != rr {
                    d.push(format!(
                        "  [{label}]\n     frank= {fr:?}\n     stock= {rr:?}"
                    ));
                }
            };

        // all objects by (type, name)
        check(
            "all objects",
            fq(&f, "SELECT type,name FROM sqlite_master ORDER BY type,name").await,
            rq(&r, "SELECT type,name FROM sqlite_master ORDER BY type,name"),
            &mut diffs,
        );
        // tables only
        check(
            "tables",
            fq(
                &f,
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
            )
            .await,
            rq(
                &r,
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
            ),
            &mut diffs,
        );
        // indexes with their tbl_name (includes the UNIQUE auto-index sqlite_autoindex_users_1)
        check(
            "indexes + tbl_name",
            fq(
                &f,
                "SELECT type,name,tbl_name FROM sqlite_master WHERE type='index' ORDER BY name",
            )
            .await,
            rq(
                &r,
                "SELECT type,name,tbl_name FROM sqlite_master WHERE type='index' ORDER BY name",
            ),
            &mut diffs,
        );
        // the UNIQUE-constraint auto-index exists (by name prefix)
        check("autoindex present", fq(&f, "SELECT count(*) FROM sqlite_master WHERE type='index' AND name LIKE 'sqlite_autoindex_%'").await,
              rq(&r, "SELECT count(*) FROM sqlite_master WHERE type='index' AND name LIKE 'sqlite_autoindex_%'"), &mut diffs);
        // view + trigger with tbl_name
        check(
            "view row",
            fq(
                &f,
                "SELECT type,name,tbl_name FROM sqlite_master WHERE type='view'",
            )
            .await,
            rq(
                &r,
                "SELECT type,name,tbl_name FROM sqlite_master WHERE type='view'",
            ),
            &mut diffs,
        );
        check(
            "trigger row",
            fq(
                &f,
                "SELECT type,name,tbl_name FROM sqlite_master WHERE type='trigger'",
            )
            .await,
            rq(
                &r,
                "SELECT type,name,tbl_name FROM sqlite_master WHERE type='trigger'",
            ),
            &mut diffs,
        );
        // an index's tbl_name points at its owning table
        check("index tbl_name mapping", fq(&f, "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='posts' ORDER BY name").await,
              rq(&r, "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='posts' ORDER BY name"), &mut diffs);

        // ── AUTOINCREMENT -> sqlite_sequence appears ──
        ex(
            &f,
            &r,
            "CREATE TABLE ai(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)",
        )
        .await;
        ex(&f, &r, "INSERT INTO ai(v) VALUES ('a'),('b')").await;
        check(
            "sqlite_sequence present",
            fq(
                &f,
                "SELECT name FROM sqlite_master WHERE name='sqlite_sequence'",
            )
            .await,
            rq(
                &r,
                "SELECT name FROM sqlite_master WHERE name='sqlite_sequence'",
            ),
            &mut diffs,
        );
        check(
            "sqlite_sequence row",
            fq(&f, "SELECT name,seq FROM sqlite_sequence WHERE name='ai'").await,
            rq(&r, "SELECT name,seq FROM sqlite_sequence WHERE name='ai'"),
            &mut diffs,
        );

        // ── DROP removes catalog rows (table + its indexes) ──
        ex(&f, &r, "DROP INDEX ix_posts_uid").await;
        check(
            "after drop index",
            fq(
                &f,
                "SELECT name FROM sqlite_master WHERE type='index' ORDER BY name",
            )
            .await,
            rq(
                &r,
                "SELECT name FROM sqlite_master WHERE type='index' ORDER BY name",
            ),
            &mut diffs,
        );
        ex(&f, &r, "DROP TABLE posts").await;
        check(
            "after drop table",
            fq(&f, "SELECT type,name FROM sqlite_master ORDER BY type,name").await,
            rq(&r, "SELECT type,name FROM sqlite_master ORDER BY type,name"),
            &mut diffs,
        );

        // ── ALTER RENAME updates name (and tbl_name of dependents) ──
        ex(&f, &r, "ALTER TABLE users RENAME TO members").await;
        check("after rename table", fq(&f, "SELECT type,name,tbl_name FROM sqlite_master WHERE type IN('table','index','view') ORDER BY type,name").await,
              rq(&r, "SELECT type,name,tbl_name FROM sqlite_master WHERE type IN('table','index','view') ORDER BY type,name"), &mut diffs);

        // total object count
        check(
            "total count",
            fq(&f, "SELECT count(*) FROM sqlite_master").await,
            rq(&r, "SELECT count(*) FROM sqlite_master"),
            &mut diffs,
        );

        assert!(
            diffs.is_empty(),
            "{} sqlite_master catalog divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
