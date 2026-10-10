#![recursion_limit = "512"]

//! STRICT-table type-enforcement leaf-hunt (pane af49, 2026-08-21): frank vs
//! rusqlite over STRICT tables — declared-type coercion (INT<->INTEGER, REAL
//! widening, TEXT), lossless-integer acceptance (1.0 -> 1) vs rejection (1.5,
//! 'abc'), ANY columns preserving storage class, and typeof of stored values.
//! Insert success is compared via row count; stored types via typeof. Pass =
//! coverage keeper; a mismatch is a leaf divergence.

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

#[test]
fn strict_tables_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let mut diffs = Vec::new();
        let check =
            |label: &str, fr: Vec<Vec<String>>, rr: Vec<Vec<String>>, d: &mut Vec<String>| {
                if fr != rr {
                    d.push(format!(
                        "  [{label}]\n     frank= {fr:?}\n     stock= {rr:?}"
                    ));
                }
            };

        ex(
            &f,
            &r,
            "CREATE TABLE s(i INTEGER, r REAL, t TEXT, b BLOB, a ANY) STRICT",
        )
        .await;

        // valid rows
        ex(
            &f,
            &r,
            "INSERT INTO s VALUES (1, 2.5, 'x', x'0102', 'anything')",
        )
        .await;
        ex(&f, &r, "INSERT INTO s VALUES (10, 3, 'y', x'03', 42)").await; // r=3 widened, a=int 42
        ex(&f, &r, "INSERT INTO s VALUES ('50', 4, 'z', x'04', 2.5)").await; // i='50' -> lossless int?
        ex(&f, &r, "INSERT INTO s VALUES (7.0, 5, 'w', x'05', x'aa')").await; // i=7.0 -> lossless 7
        check(
            "valid rows typeof",
            fq(
                &f,
                "SELECT typeof(i),typeof(r),typeof(t),typeof(b),typeof(a) FROM s ORDER BY rowid",
            )
            .await,
            rq(
                &r,
                "SELECT typeof(i),typeof(r),typeof(t),typeof(b),typeof(a) FROM s ORDER BY rowid",
            ),
            &mut diffs,
        );
        check(
            "valid rows values",
            fq(&f, "SELECT i,r,a FROM s ORDER BY rowid").await,
            rq(&r, "SELECT i,r,a FROM s ORDER BY rowid"),
            &mut diffs,
        );

        // rejections (each must fail on both -> row count unchanged at 4)
        ex(&f, &r, "INSERT INTO s VALUES ('abc', 1.0, 't', x'00', 1)").await; // 'abc' not int
        ex(&f, &r, "INSERT INTO s VALUES (1.5, 1.0, 't', x'00', 1)").await; // 1.5 not lossless int
        ex(&f, &r, "INSERT INTO s VALUES (1, 'nope', 't', x'00', 1)").await; // 'nope' not real
        ex(&f, &r, "INSERT INTO s VALUES (1, 1.0, 1, x'00', 1)").await; // int into TEXT (strict)
        ex(&f, &r, "INSERT INTO s VALUES (1, 1.0, 't', 'notblob', 1)").await; // text into BLOB
        check(
            "rejections count",
            fq(&f, "SELECT count(*) FROM s").await,
            rq(&r, "SELECT count(*) FROM s"),
            &mut diffs,
        );

        // STRICT with NULL (allowed unless NOT NULL)
        ex(
            &f,
            &r,
            "INSERT INTO s VALUES (NULL, NULL, NULL, NULL, NULL)",
        )
        .await;
        check(
            "nulls allowed",
            fq(&f, "SELECT count(*) FROM s WHERE i IS NULL").await,
            rq(&r, "SELECT count(*) FROM s WHERE i IS NULL"),
            &mut diffs,
        );

        // STRICT table with a bare (no declared type) column must be rejected at CREATE
        ex(&f, &r, "CREATE TABLE bad(x, y INTEGER) STRICT").await;
        check(
            "bare-column strict rejected",
            fq(&f, "SELECT count(*) FROM sqlite_master WHERE name='bad'").await,
            rq(&r, "SELECT count(*) FROM sqlite_master WHERE name='bad'"),
            &mut diffs,
        );

        assert!(
            diffs.is_empty(),
            "{} STRICT-table divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}

async fn assert_pk_query(f: &Connection, r: &rusqlite::Connection, sql: &str) {
    let frank = f
        .query(sql)
        .await
        .unwrap_or_else(|error| panic!("FrankenSQLite {sql}: {error}"))
        .iter()
        .map(|row| row.values().iter().map(tag_f).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    let mut statement = r.prepare(sql).expect("stock query prepares");
    let columns = statement.column_count();
    let stock = statement
        .query_map([], |row| {
            (0..columns)
                .map(|index| {
                    row.get::<_, rusqlite::types::Value>(index)
                        .map(|value| tag_r(&value))
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .expect("stock query executes")
        .collect::<Result<Vec<_>, _>>()
        .expect("stock query returns rows");
    assert_eq!(frank, stock, "{sql}");
}

async fn assert_pk_statement(
    f: &Connection,
    r: &rusqlite::Connection,
    sql: &str,
    table: &str,
    prepared: bool,
    rejects_null: bool,
) {
    let frank = if prepared {
        f.prepare(sql)
            .await
            .expect("prepare PK statement")
            .execute()
            .await
    } else {
        f.execute(sql).await
    }
    .map_err(|error| error.to_string());
    let stock = r.execute(sql, []).map_err(|error| error.to_string());
    assert_eq!(stock.is_err(), rejects_null, "stock premise: {sql}");
    if let Err(error) = &stock {
        assert!(
            error.starts_with("NOT NULL constraint failed:"),
            "{sql}: {error}"
        );
    }
    assert_eq!(frank, stock, "prepared={prepared}: {sql}");
    assert_pk_query(f, r, &format!("SELECT * FROM {table} ORDER BY v")).await;
    let schema = table.split_once('.').map_or("main", |(schema, _)| schema);
    let integrity = format!("PRAGMA {schema}.integrity_check");
    assert_eq!(
        r.query_row(&integrity, [], |row| row.get::<_, String>(0))
            .unwrap(),
        "ok",
        "stock integrity premise: {sql}"
    );
    assert_pk_query(f, r, &integrity).await;
}

#[test]
fn strict_primary_key_null_writes_match_stock_bd_9y1gr() {
    asupersync::test_utils::run_test(|| async {
        // INT and an inline INTEGER PRIMARY KEY DESC are not rowid aliases.
        // The table-level case also checks quoted identifiers, case folding,
        // collation and descending order in the primary-key declaration.
        for (definition, seed) in [
            ("k TEXT PRIMARY KEY, v TEXT UNIQUE", "'seed'"),
            ("k INT PRIMARY KEY, v TEXT UNIQUE", "7"),
            ("k INTEGER PRIMARY KEY DESC, v TEXT UNIQUE", "7"),
            ("k REAL PRIMARY KEY, v TEXT UNIQUE", "1.5"),
            ("k BLOB PRIMARY KEY, v TEXT UNIQUE", "x'00ff'"),
            ("k ANY PRIMARY KEY, v TEXT UNIQUE", "'000123'"),
            (
                "k TEXT, v TEXT UNIQUE, PRIMARY KEY('K' COLLATE NOCASE DESC)",
                "'seed'",
            ),
        ] {
            for prepared in [false, true] {
                let f = Connection::open(":memory:").await.unwrap();
                let r = rusqlite::Connection::open_in_memory().unwrap();
                for sql in [
                    format!("CREATE TABLE t({definition}) STRICT"),
                    format!("INSERT INTO t VALUES({seed}, 'base')"),
                ] {
                    f.execute(&sql).await.unwrap();
                    r.execute(&sql, []).unwrap();
                }
                assert_pk_query(&f, &r, "PRAGMA table_info(t)").await;
                assert_pk_query(&f, &r, "PRAGMA table_xinfo(t)").await;
                for sql in [
                    "INSERT INTO t VALUES(NULL, 'explicit')".to_owned(),
                    "INSERT INTO t(v) VALUES('omitted')".to_owned(),
                    "INSERT INTO t DEFAULT VALUES".to_owned(),
                    "INSERT INTO t SELECT NULL, 'selected'".to_owned(),
                    "UPDATE t SET k=NULL WHERE v='base'".to_owned(),
                    "UPDATE OR REPLACE t SET k=NULL WHERE v='base'".to_owned(),
                    format!(
                        "INSERT INTO t VALUES({seed}, 'base') \
                         ON CONFLICT(v) DO UPDATE SET k=NULL"
                    ),
                    "INSERT OR REPLACE INTO t VALUES(NULL, 'replace')".to_owned(),
                ] {
                    assert_pk_statement(&f, &r, &sql, "t", prepared, true).await;
                }
                for sql in [
                    "INSERT OR IGNORE INTO t VALUES(NULL, 'ignored')",
                    "UPDATE OR IGNORE t SET k=NULL WHERE v='base'",
                ] {
                    assert_pk_statement(&f, &r, sql, "t", prepared, false).await;
                }
                f.close().await.unwrap();
            }
        }
    });
}

#[test]
fn strict_primary_key_conflict_actions_and_aliases_match_stock_bd_9y1gr() {
    asupersync::test_utils::run_test(|| async {
        for prepared in [false, true] {
            let f = Connection::open(":memory:").await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            // A PRIMARY KEY's IGNORE does not override its implicit NOT NULL's
            // ABORT. An explicit NOT NULL clause retains its own algorithm.
            for (table, declaration, rejects_null) in [
                ("implicit", "k TEXT PRIMARY KEY ON CONFLICT IGNORE", true),
                (
                    "explicit",
                    "k TEXT PRIMARY KEY ON CONFLICT REPLACE NOT NULL ON CONFLICT IGNORE",
                    false,
                ),
            ] {
                let sql = format!("CREATE TABLE {table}({declaration}, v TEXT) STRICT");
                f.execute(&sql).await.unwrap();
                r.execute(&sql, []).unwrap();
                assert_pk_statement(
                    &f,
                    &r,
                    &format!("INSERT INTO {table} VALUES(NULL, 'null')"),
                    table,
                    prepared,
                    rejects_null,
                )
                .await;
            }
            for (table, declaration, suffix) in [
                ("ordinary", "k TEXT PRIMARY KEY", ""),
                ("alias", "k INTEGER PRIMARY KEY", "STRICT"),
                ("table_alias", "k INTEGER, PRIMARY KEY(k DESC)", "STRICT"),
            ] {
                // Put v before the declaration so a table-level PK remains last.
                let sql = format!("CREATE TABLE {table}(v TEXT, {declaration}) {suffix}");
                f.execute(&sql).await.unwrap();
                r.execute(&sql, []).unwrap();
                assert_pk_query(&f, &r, &format!("PRAGMA table_info({table})")).await;
                assert_pk_statement(
                    &f,
                    &r,
                    &format!("INSERT INTO {table}(k, v) VALUES(NULL, 'allocated-or-null')"),
                    table,
                    prepared,
                    false,
                )
                .await;
            }
            for sql in [
                "CREATE TABLE t(k TEXT PRIMARY KEY DEFAULT 'fallback', v TEXT) STRICT",
                "INSERT INTO t VALUES('seed', 'base')",
            ] {
                f.execute(sql).await.unwrap();
                r.execute(sql, []).unwrap();
            }
            for sql in [
                "INSERT OR REPLACE INTO t VALUES(NULL, 'insert-default')",
                "UPDATE OR REPLACE t SET k=NULL WHERE v='base'",
            ] {
                assert_pk_statement(&f, &r, sql, "t", prepared, false).await;
            }
            f.close().await.unwrap();
        }
    });
}

#[test]
fn strict_composite_primary_key_survives_catalog_reload_bd_9y1gr() {
    asupersync::test_utils::run_test(|| async {
        let directory = tempfile::tempdir().unwrap();
        let frank_path = directory.path().join("strict-frank.db");
        let stock_path = directory.path().join("strict-stock.db");
        for phase in ["created", "reopened", "attached"] {
            let attached = phase == "attached";
            let f = Connection::open(if attached {
                ":memory:"
            } else {
                frank_path.to_str().unwrap()
            })
            .await
            .unwrap();
            let r = if attached {
                rusqlite::Connection::open_in_memory().unwrap()
            } else {
                rusqlite::Connection::open(&stock_path).unwrap()
            };
            if phase == "created" {
                for sql in [
                    "CREATE TABLE t(k TEXT, n INT, v TEXT UNIQUE, \
                     PRIMARY KEY('K' COLLATE NOCASE DESC, n)) STRICT",
                    "INSERT INTO t VALUES('seed', 7, 'base')",
                ] {
                    f.execute(sql).await.unwrap();
                    r.execute(sql, []).unwrap();
                }
            } else if attached {
                f.execute(&format!(
                    "ATTACH '{}' AS aux",
                    frank_path.to_string_lossy().replace('\'', "''")
                ))
                .await
                .unwrap();
                r.execute(
                    &format!(
                        "ATTACH '{}' AS aux",
                        stock_path.to_string_lossy().replace('\'', "''")
                    ),
                    [],
                )
                .unwrap();
            }
            let schema = if attached { "aux" } else { "main" };
            let table = format!("{schema}.t");
            assert_pk_query(&f, &r, &format!("PRAGMA {schema}.table_info(t)")).await;
            for prepared in [false, true] {
                for sql in [
                    format!("INSERT INTO {table} VALUES(NULL, 8, 'null-text')"),
                    format!("INSERT INTO {table} VALUES('new', NULL, 'null-int')"),
                    format!("INSERT INTO {table}(k, v) VALUES('new', 'omitted-int')"),
                    format!("UPDATE {table} SET n=NULL WHERE v='base'"),
                ] {
                    assert_pk_statement(&f, &r, &sql, &table, prepared, true).await;
                }
            }
            f.close().await.unwrap();
            r.close().unwrap();
        }
    });
}

#[test]
fn strict_legacy_null_primary_key_remains_readable_bd_9y1gr() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("strict-legacy.db");
    let frank_path = directory.path().join("strict-legacy-frank.db");
    {
        let stock = rusqlite::Connection::open(&path).unwrap();
        // Model a NULL row written by an older FrankenSQLite build without
        // disabling constraints or corrupting B-tree structure in the candidate.
        stock
            .execute_batch(
                "CREATE TABLE t(k TEXT PRIMARY KEY, v TEXT);
             INSERT INTO t VALUES(NULL, 'legacy'), ('seed', 'healthy');
             PRAGMA writable_schema=ON;
             UPDATE sqlite_schema SET sql=sql || ' STRICT' WHERE name='t';",
            )
            .unwrap();
    }
    std::fs::copy(&path, &frank_path).unwrap();
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(frank_path.to_str().unwrap())
            .await
            .unwrap();
        let r = rusqlite::Connection::open(&path).unwrap();
        assert_eq!(
            r.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "NULL value in t.k"
        );
        assert_pk_query(&f, &r, "SELECT * FROM t ORDER BY v").await;
        assert_pk_query(&f, &r, "PRAGMA table_info(t)").await;
        assert_pk_query(&f, &r, "PRAGMA integrity_check").await;
        let sql = "INSERT INTO t VALUES(NULL, 'new-null')";
        let frank_error = f.prepare(sql).await.unwrap().execute().await.unwrap_err();
        let stock_error = r.execute(sql, []).unwrap_err();
        assert_eq!(frank_error.to_string(), stock_error.to_string());
        assert_pk_query(&f, &r, "SELECT * FROM t ORDER BY v").await;
        assert_pk_query(&f, &r, "PRAGMA integrity_check").await;
        f.close().await.unwrap();
    });
}
