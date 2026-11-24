#![recursion_limit = "512"]

//! Differential oracle: collation (BINARY / NOCASE / RTRIM) vs rusqlite
//! (bundled SQLite 3.53). A probe sweep found this surface stock-correct across
//! 15 cases; this keeper locks it in.
//!
//! Covers a NOCASE-declared column driving equality, ORDER BY, DISTINCT,
//! GROUP BY, IN, `<`, and min/max; explicit `COLLATE` on a BINARY column and in
//! ORDER BY/GROUP BY; explicit-COLLATE precedence over the column collation;
//! BINARY (default) uppercase-before-lowercase ordering; and RTRIM
//! (trailing-space-insensitive) equality and ordering.

use fsqlite_core::connection::Connection;
use fsqlite_func::collation::CollationFunction;
use fsqlite_types::value::SqliteValue;

struct BuiltinNameOverride {
    name: &'static str,
}

impl CollationFunction for BuiltinNameOverride {
    fn name(&self) -> &str {
        self.name
    }

    fn compare(&self, left: &[u8], right: &[u8]) -> std::cmp::Ordering {
        if self.name == "BINARY" {
            left.to_ascii_lowercase().cmp(&right.to_ascii_lowercase())
        } else {
            left.cmp(right)
        }
    }
}

fn override_builtin(frank: &Connection, stock: &rusqlite::Connection, name: &'static str) {
    frank.register_collation_function(BuiltinNameOverride { name });
    stock
        .create_collation(name, move |left, right| {
            BuiltinNameOverride { name }.compare(left.as_bytes(), right.as_bytes())
        })
        .unwrap();
}

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}
fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

async fn fq(f: &Connection, sql: &str) -> Vec<Vec<String>> {
    match f.query_with_params(sql, &[]).await {
        Ok(rows) => rows
            .iter()
            .map(|r| r.values().iter().map(tag_f).collect())
            .collect(),
        Err(e) => vec![vec![format!("<ERR {e:?}>")]],
    }
}
fn rq(r: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut st = match r.prepare(sql) {
        Ok(st) => st,
        Err(e) => return vec![vec![format!("<ERR {e}>")]],
    };
    let n = st.column_count();
    st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

async fn agree(setup: &[&str], sql: &str, msg: &str) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in setup {
        let _ = f.execute(s).await;
        let _ = r.execute_batch(s);
    }
    let fr = fq(&f, sql).await;
    let rr = rq(&r, sql);
    assert_eq!(
        fr, rr,
        "{msg}\n  sql   ={sql}\n  frank ={fr:?}\n  sqlite={rr:?}"
    );
}

/// A NOCASE-declared column with case-variant rows.
const N: &[&str] = &[
    "CREATE TABLE t(s TEXT COLLATE NOCASE)",
    "INSERT INTO t VALUES ('Apple'),('apple'),('BANANA'),('banana'),('Cherry')",
];

#[test]
fn nocase_column() {
    asupersync::test_utils::run_test(|| async {
        agree(
            N,
            "SELECT s FROM t WHERE s = 'apple' ORDER BY rowid",
            "NOCASE equality",
        )
        .await;
        agree(N, "SELECT s FROM t ORDER BY s", "NOCASE ORDER BY").await;
        agree(N, "SELECT DISTINCT s FROM t ORDER BY s", "NOCASE DISTINCT").await;
        agree(
            N,
            "SELECT count(*) FROM t GROUP BY s ORDER BY s",
            "NOCASE GROUP BY",
        )
        .await;
        agree(
            N,
            "SELECT s FROM t WHERE s < 'c' ORDER BY s",
            "NOCASE < comparison",
        )
        .await;
        agree(
            N,
            "SELECT s FROM t WHERE s IN ('APPLE','banana') ORDER BY rowid",
            "NOCASE IN list",
        )
        .await;
        agree(N, "SELECT min(s), max(s) FROM t", "NOCASE min/max").await;
    });
}

#[test]
fn explicit_collate() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE b(s TEXT)",
                "INSERT INTO b VALUES ('ABC'),('abc'),('xyz')",
            ],
            "SELECT s FROM b WHERE s = 'abc' COLLATE NOCASE ORDER BY rowid",
            "explicit COLLATE NOCASE on a BINARY column",
        )
        .await;
        agree(
            &[
                "CREATE TABLE b(s TEXT)",
                "INSERT INTO b VALUES ('b'),('A'),('c'),('B'),('a')",
            ],
            "SELECT s FROM b ORDER BY s COLLATE NOCASE, s",
            "explicit COLLATE NOCASE in ORDER BY",
        )
        .await;
        agree(
            &[
                "CREATE TABLE b(s TEXT)",
                "INSERT INTO b VALUES ('X'),('x'),('y'),('Y')",
            ],
            "SELECT count(*) FROM b GROUP BY s COLLATE NOCASE ORDER BY s COLLATE NOCASE",
            "explicit COLLATE in GROUP BY",
        )
        .await;
        agree(
            N,
            "SELECT s FROM t WHERE s = 'apple' COLLATE BINARY ORDER BY rowid",
            "explicit COLLATE BINARY overrides the column's NOCASE",
        )
        .await;
    });
}

#[test]
fn binary_and_rtrim() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE b(s TEXT)",
                "INSERT INTO b VALUES ('b'),('A'),('a'),('B')",
            ],
            "SELECT s FROM b ORDER BY s",
            "BINARY default: uppercase sorts before lowercase",
        )
        .await;
        agree(
            &[
                "CREATE TABLE b(s TEXT)",
                "INSERT INTO b VALUES ('Z'),('a'),('M')",
            ],
            "SELECT s FROM b ORDER BY s COLLATE NOCASE",
            "NOCASE affects ASCII letters only",
        )
        .await;
        agree(
            &[
                "CREATE TABLE t(s TEXT COLLATE RTRIM)",
                "INSERT INTO t VALUES ('hi'),('hi   '),('hix')",
            ],
            "SELECT s FROM t WHERE s = 'hi' ORDER BY rowid",
            "RTRIM ignores trailing spaces in equality",
        )
        .await;
        agree(
            &[
                "CREATE TABLE t(s TEXT COLLATE RTRIM)",
                "INSERT INTO t VALUES ('b '),('a'),('a  '),('c')",
            ],
            "SELECT quote(s) FROM t ORDER BY s",
            "RTRIM ORDER BY treats trailing-space variants as equal",
        )
        .await;
    });
}

/// bd-puv2t: canonical TEMP UNIQUE keys are valid only while the registry
/// still supplies the built-in comparator. Application replacements can
/// make two formerly equal keys distinct, or make distinct keys equal.
#[test]
fn builtin_name_overrides_govern_unique_writes() {
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            for storage in ["", "TEMP "] {
                for constraint in ["UNIQUE", "PRIMARY KEY"] {
                    for (declaration, name, first, second) in [
                        ("COLLATE RTRIM", "RTRIM", "key", "key "),
                        ("COLLATE NOCASE", "NOCASE", "key", "KEY"),
                        ("COLLATE BINARY", "BINARY", "key", "KEY"),
                        ("", "BINARY", "key", "KEY"),
                    ] {
                        // None keeps the built-in behavior. The other cases
                        // register before DDL or after the first indexed row.
                        for registration in [None, Some(false), Some(true)] {
                            let dir = tempfile::tempdir().unwrap();
                            let path = dir.path().join("collations.db");
                            let f = Connection::open(if file_backed {
                                path.to_str().unwrap()
                            } else {
                                ":memory:"
                            })
                            .await
                            .unwrap();
                            let r = rusqlite::Connection::open_in_memory().unwrap();
                            if registration == Some(false) {
                                override_builtin(&f, &r, name);
                            }
                            let ddl = format!(
                                "CREATE {storage}TABLE t(k TEXT {declaration} {constraint}, v INTEGER)"
                            );
                            f.execute(&ddl).await.unwrap();
                            r.execute_batch(&ddl).unwrap();
                            let insert = format!("INSERT INTO t VALUES ('{first}',1)");
                            f.execute(&insert).await.unwrap();
                            r.execute(&insert, []).unwrap();
                            if registration == Some(true) {
                                override_builtin(&f, &r, name);
                            }
                            for sql in [
                                format!("INSERT OR IGNORE INTO t VALUES ('{second}',2)"),
                                format!("INSERT OR REPLACE INTO t VALUES ('{second}',3)"),
                                "ALTER TABLE t ADD COLUMN extra INTEGER DEFAULT 7".to_owned(),
                                format!("UPDATE OR IGNORE t SET k='{first}' WHERE v=3"),
                                format!("UPDATE OR REPLACE t SET k='{first}' WHERE v=3"),
                                "DELETE FROM t WHERE v=3".to_owned(),
                                format!("INSERT OR IGNORE INTO t(k,v) VALUES ('{second}',4)"),
                            ] {
                                let actual = f.execute(&sql).await.unwrap();
                                let expected = r.execute(&sql, []).unwrap();
                                if !sql.starts_with("ALTER") {
                                    assert_eq!(actual, expected, "{ddl}; {registration:?}; {sql}");
                                }
                                assert_eq!(
                                    fq(&f, "SELECT k,v FROM t ORDER BY v").await,
                                    rq(&r, "SELECT k,v FROM t ORDER BY v"),
                                    "file={file_backed}; {ddl}; {registration:?}; {sql}"
                                );
                            }
                            f.close().await.unwrap();
                        }
                    }
                }
            }
        }
    });
}

/// CREATE UNIQUE INDEX validates already stored rows with the registered
/// comparator, including an override installed after the rows were inserted.
#[test]
fn builtin_name_overrides_govern_unique_index_creation() {
    asupersync::test_utils::run_test(|| async {
        for storage in ["", "TEMP "] {
            for (declaration, name, second) in [
                ("COLLATE RTRIM", "RTRIM", "key "),
                ("COLLATE NOCASE", "NOCASE", "KEY"),
                ("COLLATE BINARY", "BINARY", "KEY"),
                ("", "BINARY", "KEY"),
            ] {
                let f = Connection::open(":memory:").await.unwrap();
                let r = rusqlite::Connection::open_in_memory().unwrap();
                let setup = format!(
                    "CREATE {storage}TABLE t(k TEXT {declaration}, v INTEGER); \
                     INSERT INTO t VALUES ('key',1),('{second}',2)"
                );
                f.execute_batch(&setup).await.unwrap();
                r.execute_batch(&setup).unwrap();
                override_builtin(&f, &r, name);
                let sql = "CREATE UNIQUE INDEX t_k ON t(k)";
                let actual = f.execute(sql).await;
                let expected = r.execute(sql, []);
                assert_eq!(actual.is_ok(), expected.is_ok(), "{setup}");
                assert_eq!(
                    fq(&f, "SELECT k,v FROM t ORDER BY v").await,
                    rq(&r, "SELECT k,v FROM t ORDER BY v"),
                    "{setup}"
                );
                if expected.is_ok() {
                    for sql in [
                        "INSERT OR IGNORE INTO t VALUES ('key',3)",
                        "INSERT OR REPLACE INTO t VALUES ('key',4)",
                    ] {
                        assert_eq!(f.execute(sql).await.unwrap(), r.execute(sql, []).unwrap());
                        assert_eq!(
                            fq(&f, "SELECT k,v FROM t ORDER BY v").await,
                            rq(&r, "SELECT k,v FROM t ORDER BY v"),
                            "{setup}; {sql}"
                        );
                    }
                }
                f.close().await.unwrap();
            }
        }
    });
}
