#![recursion_limit = "512"]

//! Non-recursive CTE composition leaf-hunt (pane af49, 2026-08-21): frank vs
//! rusqlite over WITH-clause corners that aren't recursion — several CTEs in one
//! WITH where a later CTE references an earlier one, a CTE whose name SHADOWS a
//! real table (the CTE must win inside its query), a CTE self-joined / reused
//! twice, explicit column-list renaming on the CTE, a CTE feeding an aggregate,
//! WITH nested inside a subquery / inside a FROM subquery, and a CTE referenced
//! from a DML statement. Ordered result sets compared. Pass = coverage keeper.

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

async fn assert_materialized_cte_query(
    f: &Connection,
    r: &rusqlite::Connection,
    sql: &str,
    database: &str,
    expected_rows: usize,
    ordered: bool,
) {
    let mut statement = r.prepare(sql).expect("stock CTE prepares");
    let width = statement.column_count();
    let mut expected = statement
        .query_map([], |row| {
            (0..width)
                .map(|index| {
                    row.get::<_, rusqlite::types::Value>(index)
                        .map(|value| tag_r(&value))
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .expect("stock CTE executes")
        .collect::<Result<Vec<_>, _>>()
        .expect("stock CTE rows");
    assert_eq!(expected.len(), expected_rows, "stock fixture: {sql}");
    if !ordered {
        expected.sort();
    }
    for prepared in [false, true] {
        let actual = if prepared {
            f.prepare(sql).await.unwrap().query().await.unwrap()
        } else {
            f.query(sql).await.unwrap()
        };
        let mut actual = actual
            .iter()
            .map(|row| row.values().iter().map(tag_f).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        if !ordered {
            actual.sort();
        }
        assert_eq!(actual, expected, "{database}, prepared={prepared}: {sql}");
    }
}

// bd-r5mmz / bd-6hy0g / bd-n2ehh: a materialized CTE remains visible to
// UPSERT and RETURNING for the whole statement, including after trigger DML
// refreshes the persistent row image. Trigger programs have their own scope.
// These helpers require successful queries; equal errors are never a pass.
async fn cte_dml_frank_rows(
    conn: &Connection,
    sql: &str,
    params: &[i64],
    prepared: bool,
) -> Vec<Vec<String>> {
    let params: Vec<_> = params.iter().copied().map(SqliteValue::Integer).collect();
    let rows = if prepared {
        let statement = conn
            .prepare(sql)
            .await
            .unwrap_or_else(|error| panic!("FrankenSQLite prepare {sql}: {error}"));
        if statement.column_count() == 0 {
            statement
                .execute_with_params(&params)
                .await
                .map(|_| Vec::new())
        } else {
            statement.query_with_params(&params).await
        }
    } else {
        conn.query_with_params(sql, &params).await
    }
    .unwrap_or_else(|error| panic!("FrankenSQLite query {sql}: {error}"));
    rows.iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect()
}

fn cte_dml_stock_rows(
    conn: &rusqlite::Connection,
    sql: &str,
    params: &[i64],
) -> Vec<Vec<String>> {
    let mut statement = conn
        .prepare(sql)
        .unwrap_or_else(|error| panic!("stock prepare {sql}: {error}"));
    let width = statement.column_count();
    statement
        .query_map(rusqlite::params_from_iter(params), |row| {
            (0..width)
                .map(|index| {
                    row.get::<_, rusqlite::types::Value>(index)
                        .map(|value| tag_r(&value))
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .unwrap_or_else(|error| panic!("stock query {sql}: {error}"))
        .collect::<Result<Vec<_>, _>>()
        .unwrap_or_else(|error| panic!("stock rows {sql}: {error}"))
}

async fn cte_dml_setup(f: &Connection, r: &rusqlite::Connection, triggers: bool) {
    for sql in [
        "CREATE TABLE t(k INTEGER PRIMARY KEY, v INTEGER, note TEXT)",
        "CREATE TABLE src(x INTEGER)",
        "CREATE TABLE log(id INTEGER PRIMARY KEY, msg TEXT)",
        "INSERT INTO src VALUES (1),(2),(3)",
        "INSERT INTO t VALUES (1,10,'old'),(2,20,'old')",
    ] {
        f.execute(sql)
            .await
            .unwrap_or_else(|error| panic!("FrankenSQLite fixture {sql}: {error}"));
        r.execute_batch(sql)
            .unwrap_or_else(|error| panic!("stock fixture {sql}: {error}"));
    }
    if triggers {
        for sql in [
            "CREATE TRIGGER t_ins BEFORE INSERT ON t \
             WHEN (SELECT count(*) FROM log) >= 0 BEGIN \
             INSERT INTO log(msg) VALUES \
               ('ins ' || NEW.k || ' seen ' || (SELECT count(*) FROM log)); END",
            "CREATE TRIGGER t_upd AFTER UPDATE ON t BEGIN \
             INSERT INTO log(msg) VALUES ('upd ' || NEW.k || ' ' || NEW.v); END",
        ] {
            f.execute(sql).await.expect("FrankenSQLite trigger fixture");
            r.execute_batch(sql).expect("stock trigger fixture");
        }
    }
}

async fn assert_cte_dml_state(f: &Connection, r: &rusqlite::Connection, label: &str) {
    for sql in [
        "SELECT changes()",
        "SELECT k,v,note FROM t ORDER BY k",
        "SELECT x FROM src ORDER BY x",
        "SELECT id,msg FROM log ORDER BY id",
    ] {
        assert_eq!(
            cte_dml_frank_rows(f, sql, &[], false).await,
            cte_dml_stock_rows(r, sql, &[]),
            "{label}: {sql}",
        );
    }
}

#[test]
fn with_insert_upsert_cte_reads_survive_trigger_replay() {
    asupersync::test_utils::run_test(|| async {
        let directory = tempfile::tempdir().unwrap();
        let sql = "WITH c(x) AS (SELECT x FROM src) \
                   INSERT INTO t(k,v) SELECT x,x*100 FROM c WHERE true \
                   ON CONFLICT(k) DO UPDATE SET v=(SELECT max(x) FROM c)";
        for file_backed in [false, true] {
            for prepared in [false, true] {
                // Exercise the silent wrong-write report before the sibling
                // no-trigger cursor error when this keeper is run on a base.
                for triggers in [true, false] {
                    let label = format!(
                        "upsert-file-{file_backed}-prepared-{prepared}-triggers-{triggers}"
                    );
                    let path = directory.path().join(format!("{label}.db"));
                    let f = Connection::open(if file_backed {
                        path.to_str().unwrap()
                    } else {
                        ":memory:"
                    })
                    .await
                    .unwrap();
                    let r = rusqlite::Connection::open_in_memory().unwrap();
                    cte_dml_setup(&f, &r, triggers).await;
                    // The second execution conflicts on all three keys. It also
                    // catches cached programs retaining a cleaned-up CTE root.
                    for execution in 0..2 {
                        let expected = cte_dml_stock_rows(&r, sql, &[]);
                        let actual = cte_dml_frank_rows(&f, sql, &[], prepared).await;
                        assert!(expected.is_empty(), "stock UPSERT has no RETURNING");
                        assert_eq!(actual, expected, "{label}, execution={execution}");
                        assert_cte_dml_state(&f, &r, &label).await;
                        let rows = cte_dml_stock_rows(&r, "SELECT k,v FROM t ORDER BY k", &[]);
                        assert_eq!(rows[0], vec!["int:1", "int:3"]);
                        assert_eq!(rows[1], vec!["int:2", "int:3"]);
                        let error = f.query("SELECT x FROM c").await.expect_err("CTE is scoped");
                        assert!(error.to_string().contains("no such table"), "{error}");
                    }
                    f.close().await.unwrap();
                    if file_backed {
                        let reopened = Connection::open(path.to_str().unwrap()).await.unwrap();
                        for query in [
                            "SELECT k,v,note FROM t ORDER BY k",
                            "SELECT id,msg FROM log ORDER BY id",
                        ] {
                            assert_eq!(
                                cte_dml_frank_rows(&reopened, query, &[], false).await,
                                cte_dml_stock_rows(&r, query, &[]),
                                "{label} after reopen: {query}"
                            );
                        }
                        reopened.close().await.unwrap();
                    }
                }
            }
        }
    });
}

#[test]
fn with_insert_returning_cte_reads_preserve_parameters() {
    asupersync::test_utils::run_test(|| async {
        let directory = tempfile::tempdir().unwrap();
        let sql = "WITH c(x) AS (SELECT x FROM src WHERE x<=?1) \
                   INSERT INTO t(k,v) SELECT x+?2,x*?3 FROM c \
                   RETURNING k,v,(SELECT count(*) FROM c),(SELECT max(x)+?4 FROM c)";
        for file_backed in [false, true] {
            for prepared in [false, true] {
                for triggers in [false, true] {
                    let label = format!(
                        "returning-file-{file_backed}-prepared-{prepared}-triggers-{triggers}"
                    );
                    let path = directory.path().join(format!("{label}.db"));
                    let f = Connection::open(if file_backed {
                        path.to_str().unwrap()
                    } else {
                        ":memory:"
                    })
                    .await
                    .unwrap();
                    let r = rusqlite::Connection::open_in_memory().unwrap();
                    cte_dml_setup(&f, &r, triggers).await;
                    let params = [3, 10, 7, 20];
                    let expected = cte_dml_stock_rows(&r, sql, &params);
                    assert_eq!(
                        expected,
                        vec![
                            vec!["int:11", "int:7", "int:3", "int:23"],
                            vec!["int:12", "int:14", "int:3", "int:23"],
                            vec!["int:13", "int:21", "int:3", "int:23"],
                        ],
                        "stock RETURNING fixture"
                    );
                    assert_eq!(
                        cte_dml_frank_rows(&f, sql, &params, prepared).await,
                        expected,
                        "{label}"
                    );
                    assert_cte_dml_state(&f, &r, &label).await;
                    f.close().await.unwrap();
                }
            }
        }
    });
}

#[test]
fn with_insert_unbound_parameters_do_not_alias_replayed_source_values() {
    asupersync::test_utils::run_test(|| async {
        let directory = tempfile::tempdir().unwrap();
        let sql = "WITH c(x) AS (VALUES(1),(2)) \
                   INSERT INTO t(k,v) SELECT x,x*100 FROM c WHERE true \
                   ON CONFLICT(k) DO UPDATE SET v=coalesce(?1,excluded.v),note=:note \
                   RETURNING k,v,?1,?4,?,:note,(SELECT count(*) FROM c)";
        for file_backed in [false, true] {
            for prepared in [false, true] {
                for triggers in [false, true] {
                    let label = format!(
                        "unbound-file-{file_backed}-prepared-{prepared}-triggers-{triggers}"
                    );
                    let path = directory.path().join(format!("{label}.db"));
                    let f = Connection::open(if file_backed {
                        path.to_str().unwrap()
                    } else {
                        ":memory:"
                    })
                    .await
                    .unwrap();
                    let r = rusqlite::Connection::open_in_memory().unwrap();
                    cte_dml_setup(&f, &r, triggers).await;
                    // Native query() leaves parameters unbound. rusqlite's
                    // raw_query mirrors SQLite's NULL bindings; query_map([])
                    // rejects the parameter count before executing the SQL.
                    let expected = {
                        let mut statement = r.prepare(sql).unwrap();
                        let width = statement.column_count();
                        let mut rows = statement.raw_query();
                        let mut result = Vec::new();
                        while let Some(row) = rows.next().unwrap() {
                            result.push(
                                (0..width)
                                    .map(|index| {
                                        tag_r(&row.get::<_, rusqlite::types::Value>(index).unwrap())
                                    })
                                    .collect::<Vec<_>>(),
                            );
                        }
                        result
                    };
                    assert_eq!(
                        expected,
                        vec![
                            vec!["int:1", "int:100", "NULL", "NULL", "NULL", "NULL", "int:2"],
                            vec!["int:2", "int:200", "NULL", "NULL", "NULL", "NULL", "int:2"],
                        ],
                        "stock unbound-parameter fixture"
                    );
                    let actual = if prepared {
                        f.prepare(sql).await.unwrap().query().await
                    } else {
                        f.query(sql).await
                    }
                    .unwrap_or_else(|error| panic!("{label}: {error}"));
                    assert_eq!(
                        actual
                            .iter()
                            .map(|row| row.values().iter().map(tag_f).collect::<Vec<_>>())
                            .collect::<Vec<_>>(),
                        expected,
                        "{label}: original slots must remain NULL"
                    );
                    assert_cte_dml_state(&f, &r, &label).await;
                    f.close().await.unwrap();
                }
            }
        }
    });
}

#[test]
fn with_insert_upsert_where_cte_reads_keep_bound_parameters() {
    asupersync::test_utils::run_test(|| async {
        let directory = tempfile::tempdir().unwrap();
        let sql = "WITH c(x) AS (SELECT x FROM src) \
                   INSERT INTO t(k,v) SELECT x,x*?1 FROM c WHERE true \
                   ON CONFLICT(k) DO UPDATE SET v=excluded.v \
                   WHERE EXISTS(SELECT 1 FROM c WHERE x=?2)";
        for file_backed in [false, true] {
            for prepared in [false, true] {
                for triggers in [false, true] {
                    let label = format!(
                        "where-file-{file_backed}-prepared-{prepared}-triggers-{triggers}"
                    );
                    let path = directory.path().join(format!("{label}.db"));
                    let f = Connection::open(if file_backed {
                        path.to_str().unwrap()
                    } else {
                        ":memory:"
                    })
                    .await
                    .unwrap();
                    let r = rusqlite::Connection::open_in_memory().unwrap();
                    cte_dml_setup(&f, &r, triggers).await;
                    // A successful DO UPDATE followed by a false condition
                    // proves the replay retained the predicate's bind slot.
                    for (params, changes) in [([100, 3], 3), ([1000, 0], 0)] {
                        let expected = cte_dml_stock_rows(&r, sql, &params);
                        assert!(expected.is_empty());
                        assert_eq!(
                            cte_dml_frank_rows(&f, sql, &params, prepared).await,
                            expected,
                            "{label}: {params:?}"
                        );
                        assert_eq!(r.changes(), changes);
                        assert_cte_dml_state(&f, &r, &label).await;
                    }
                    f.close().await.unwrap();
                }
            }
        }
    });
}

#[test]
fn with_insert_cte_is_hidden_from_trigger_body_and_when() {
    asupersync::test_utils::run_test(|| async {
        let directory = tempfile::tempdir().unwrap();
        let sql = "WITH log(x) AS (SELECT x FROM src) \
                   INSERT INTO t(k,v) SELECT x+20,x FROM log";
        for file_backed in [false, true] {
            for prepared in [false, true] {
                for temporary_trigger in [false, true] {
                    let label = format!(
                        "scope-file-{file_backed}-prepared-{prepared}-temp-{temporary_trigger}"
                    );
                    let path = directory.path().join(format!("{label}.db"));
                    let f = Connection::open(if file_backed {
                        path.to_str().unwrap()
                    } else {
                        ":memory:"
                    })
                    .await
                    .unwrap();
                    let r = rusqlite::Connection::open_in_memory().unwrap();
                    cte_dml_setup(&f, &r, false).await;
                    let temp = if temporary_trigger { "TEMP " } else { "" };
                    let trigger = format!(
                        "CREATE {temp}TRIGGER t_ins BEFORE INSERT ON t \
                         WHEN (SELECT count(*) FROM log WHERE msg LIKE 'ins%')<2 BEGIN \
                         INSERT INTO log(msg) VALUES \
                         ('ins ' || NEW.k || ' seen ' || (SELECT count(*) FROM log)); END"
                    );
                    f.execute(&trigger).await.unwrap();
                    r.execute_batch(&trigger).unwrap();
                    let expected = cte_dml_stock_rows(&r, sql, &[]);
                    assert!(expected.is_empty());
                    assert_eq!(
                        cte_dml_frank_rows(&f, sql, &[], prepared).await,
                        expected,
                        "{label}"
                    );
                    assert_cte_dml_state(&f, &r, &label).await;
                    assert_eq!(
                        cte_dml_stock_rows(&r, "SELECT msg FROM log ORDER BY id", &[]),
                        vec![vec!["text:ins 21 seen 0"], vec!["text:ins 22 seen 1"]]
                    );
                    // The persistent binding is restored after the WITH scope.
                    f.execute("INSERT INTO log(msg) VALUES('outside')").await.unwrap();
                    r.execute_batch("INSERT INTO log(msg) VALUES('outside')").unwrap();
                    assert_cte_dml_state(&f, &r, &label).await;
                    f.close().await.unwrap();
                }
            }
        }
    });
}

#[test]
fn with_insert_cte_shadowing_target_writes_the_persistent_table() {
    asupersync::test_utils::run_test(|| async {
        let directory = tempfile::tempdir().unwrap();
        // The FROM binding is the CTE, but an INSERT target is always a real
        // table. Writing the materialized root would lose successful writes
        // when the WITH cleanup runs.
        let sql = "WITH t(k,v) AS (VALUES(7,70),(19,190)) \
                   INSERT INTO t(k,v) SELECT k,v FROM t";
        for file_backed in [false, true] {
            for prepared in [false, true] {
                for triggers in [false, true] {
                    let label = format!(
                        "target-file-{file_backed}-prepared-{prepared}-triggers-{triggers}"
                    );
                    let path = directory.path().join(format!("{label}.db"));
                    let f = Connection::open(if file_backed {
                        path.to_str().unwrap()
                    } else {
                        ":memory:"
                    })
                    .await
                    .unwrap();
                    let r = rusqlite::Connection::open_in_memory().unwrap();
                    cte_dml_setup(&f, &r, triggers).await;
                    let expected = cte_dml_stock_rows(&r, sql, &[]);
                    assert!(expected.is_empty());
                    assert_eq!(
                        cte_dml_frank_rows(&f, sql, &[], prepared).await,
                        expected,
                        "{label}"
                    );
                    assert_cte_dml_state(&f, &r, &label).await;
                    assert_eq!(
                        cte_dml_stock_rows(&r, "SELECT k,v,note FROM t ORDER BY k", &[]),
                        vec![
                            vec!["int:1", "int:10", "text:old"],
                            vec!["int:2", "int:20", "text:old"],
                            vec!["int:7", "int:70", "NULL"],
                            vec!["int:19", "int:190", "NULL"],
                        ]
                    );
                    f.close().await.unwrap();
                }
            }
        }
    });
}

#[test]
fn with_insert_target_shadow_upsert_returning_respects_nested_scopes() {
    asupersync::test_utils::run_test(|| async {
        let directory = tempfile::tempdir().unwrap();
        for file_backed in [false, true] {
            for prepared in [false, true] {
                for (target, temporary_target) in [("t", false), ("main.t", false), ("t", true)] {
                    for triggers in [false, true] {
                        if temporary_target && triggers {
                            continue;
                        }
                        let label = format!(
                            "nested-file-{file_backed}-prepared-{prepared}-{target}-temp-{temporary_target}-triggers-{triggers}"
                        );
                        let path = directory.path().join(format!("{label}.db"));
                        let f = Connection::open(if file_backed {
                            path.to_str().unwrap()
                        } else {
                            ":memory:"
                        })
                        .await
                        .unwrap();
                        let r = rusqlite::Connection::open_in_memory().unwrap();
                        cte_dml_setup(&f, &r, triggers).await;
                        let anchor = "INSERT INTO main.t VALUES(99,999,'anchor')";
                        f.execute(anchor).await.unwrap();
                        r.execute_batch(anchor).unwrap();
                        if temporary_target {
                            for setup in [
                                "CREATE TEMP TABLE t(k INTEGER PRIMARY KEY,v INTEGER,note TEXT)",
                                "INSERT INTO temp.t VALUES(1,10,'temp-old'),(2,20,'temp-old')",
                            ] {
                                f.execute(setup).await.unwrap();
                                r.execute_batch(setup).unwrap();
                            }
                        }
                        // Read scopes differ in the same statement: INSERT
                        // targets the real table, the SET and outer RETURNING
                        // subqueries read the outer CTE, and main.t bypasses
                        // it. A RETURNING subquery with its own WITH is
                        // bd-d8fxs (see the ignored keeper below).
                        let sql = format!(
                            "WITH t(k,v) AS (VALUES(1,100),(2,200),(3,300)) \
                             INSERT INTO {target}(k,v) SELECT k,v FROM t WHERE true \
                             ON CONFLICT(k) DO UPDATE SET v=(SELECT max(v) FROM t) \
                             RETURNING k,t.v,(SELECT count(*) FROM t), \
                               (SELECT v FROM main.t WHERE k=99)"
                        );
                        let expected = cte_dml_stock_rows(&r, &sql, &[]);
                        assert_eq!(
                            expected,
                            vec![
                                vec!["int:1", "int:300", "int:3", "int:999"],
                                vec!["int:2", "int:300", "int:3", "int:999"],
                                vec!["int:3", "int:300", "int:3", "int:999"],
                            ],
                            "stock nested-scope fixture"
                        );
                        assert_eq!(
                            cte_dml_frank_rows(&f, &sql, &[], prepared).await,
                            expected,
                            "{label}"
                        );
                        assert_cte_dml_state(&f, &r, &label).await;
                        let query = "SELECT k,v,note FROM main.t ORDER BY k";
                        assert_eq!(
                            cte_dml_frank_rows(&f, query, &[], false).await,
                            cte_dml_stock_rows(&r, query, &[]),
                            "{label}: persistent main table"
                        );
                        f.close().await.unwrap();
                    }
                }
            }
        }
    });
}

/// bd-d8fxs keeper: a RETURNING subquery with its own WITH sees that scope.
/// A nested `WITH t` shadows the statement's CTE `t`, and `q` built over the
/// outer `t` still reads the CTE. The changed-row projection must preserve
/// those scopes instead of compiling an unsupported subquery as NULL.
#[test]
fn with_insert_returning_nested_with_scopes_match_stock() {
    asupersync::test_utils::run_test(|| async {
        for prepared in [false, true] {
            let label = format!("nested-with-returning-prepared-{prepared}");
            let f = Connection::open(":memory:").await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            cte_dml_setup(&f, &r, false).await;
            let anchor = "INSERT INTO main.t VALUES(99,999,'anchor')";
            f.execute(anchor).await.unwrap();
            r.execute_batch(anchor).unwrap();
            let sql = "WITH t(k,v) AS (VALUES(1,100),(2,200),(3,300)) \
                       INSERT INTO t(k,v) SELECT k,v FROM t WHERE true \
                       ON CONFLICT(k) DO UPDATE SET v=(SELECT max(v) FROM t) \
                       RETURNING k,t.v,(SELECT count(*) FROM t), \
                         (WITH t(v) AS (VALUES(99)) SELECT v FROM t), \
                         (WITH q(n) AS (SELECT max(v) FROM t) SELECT n FROM q), \
                         (SELECT v FROM main.t WHERE k=99)";
            let expected = cte_dml_stock_rows(&r, sql, &[]);
            assert_eq!(
                expected,
                vec![
                    vec!["int:1", "int:300", "int:3", "int:99", "int:300", "int:999"],
                    vec!["int:2", "int:300", "int:3", "int:99", "int:300", "int:999"],
                    vec!["int:3", "int:300", "int:3", "int:99", "int:300", "int:999"],
                ],
                "stock nested-scope fixture"
            );
            assert_eq!(
                cte_dml_frank_rows(&f, sql, &[], prepared).await,
                expected,
                "{label}"
            );
            assert_cte_dml_state(&f, &r, &label).await;
            f.close().await.unwrap();
        }
    });
}

#[test]
fn insert_returning_subquery_projection_observes_each_row_before_after_triggers() {
    asupersync::test_utils::run_test(|| async {
        let directory = tempfile::tempdir().unwrap();
        let cases = [
            ("values", "VALUES(?1,?2),(?1+1,?2+1)", [11, 20, 99], 2),
            (
                "select",
                "SELECT ?1,?2 UNION ALL SELECT ?1+1,?2+1",
                [11, 20, 99],
                2,
            ),
            ("defaults", "DEFAULT VALUES", [11, 20, 99], 1),
            (
                "upsert",
                "VALUES(?1,?2) ON CONFLICT(k) DO UPDATE SET k=excluded.k+10,v=excluded.v+1",
                [1, 20, 99],
                1,
            ),
        ];
        for file_backed in [false, true] {
            for prepared in [false, true] {
                for triggers in [false, true] {
                    for (case, source, params, expected_count) in cases {
                        let label = format!(
                            "projection-{case}-file-{file_backed}-prepared-{prepared}-triggers-{triggers}"
                        );
                        let path = directory.path().join(format!("{label}.db"));
                        let f = Connection::open(if file_backed {
                            path.to_str().unwrap()
                        } else {
                            ":memory:"
                        })
                        .await
                        .unwrap();
                        let r = rusqlite::Connection::open_in_memory().unwrap();
                        for setup in [
                            "CREATE TABLE t(k INTEGER PRIMARY KEY, v INTEGER DEFAULT 7)",
                            "CREATE TABLE audit(n INTEGER)",
                            "INSERT INTO t VALUES(1,5)",
                        ] {
                            f.execute(setup).await.unwrap();
                            r.execute_batch(setup).unwrap();
                        }
                        if triggers {
                            for setup in [
                                "CREATE TRIGGER t_before BEFORE INSERT ON t BEGIN \
                                 INSERT INTO audit VALUES(1); END",
                                "CREATE TRIGGER t_after AFTER INSERT ON t BEGIN \
                                 UPDATE t SET v=v+1000 WHERE k=NEW.k; \
                                 INSERT INTO audit VALUES(2); END",
                                "CREATE TRIGGER t_updated AFTER UPDATE ON t BEGIN \
                                 INSERT INTO audit VALUES(3); END",
                            ] {
                                f.execute(setup).await.unwrap();
                                r.execute_batch(setup).unwrap();
                            }
                        }
                        let columns = if case == "defaults" { "" } else { "(k,v)" };
                        let sql = format!(
                            "INSERT INTO t{columns} {source} RETURNING k,v, \
                             (SELECT n FROM (SELECT t.v AS n)), \
                             (WITH w(n) AS (VALUES(?3)) SELECT n FROM w), \
                             (SELECT count(*) FROM (SELECT k FROM t) AS q WHERE q.k<=t.k), \
                             (SELECT count(*) FROM audit WHERE t.k>0)"
                        );
                        let expected = cte_dml_stock_rows(&r, &sql, &params);
                        assert_eq!(expected.len(), expected_count, "stock fixture: {label}");
                        assert_eq!(
                            cte_dml_frank_rows(&f, &sql, &params, prepared).await,
                            expected,
                            "{label}"
                        );
                        for check in [
                            "SELECT k,v FROM t ORDER BY k",
                            "SELECT n FROM audit ORDER BY rowid",
                            "SELECT changes(),total_changes(),last_insert_rowid()",
                            "PRAGMA integrity_check",
                        ] {
                            assert_eq!(
                                cte_dml_frank_rows(&f, check, &[], false).await,
                                cte_dml_stock_rows(&r, check, &[]),
                                "{label}: {check}"
                            );
                        }
                        f.close().await.unwrap();
                    }
                }
            }
        }
    });
}

#[test]
fn insert_returning_projection_error_rolls_back_rows_and_trigger_effects() {
    asupersync::test_utils::run_test(|| async {
        let directory = tempfile::tempdir().unwrap();
        for file_backed in [false, true] {
            for prepared in [false, true] {
                for (explicit_transaction, with_triggers) in
                    [(false, false), (false, true), (true, false), (true, true)]
                {
                    for (case, source) in [
                        ("one", "VALUES(11,?1)"),
                        ("many", "VALUES(11,20),(12,?1)"),
                    ] {
                        let label = format!(
                            "projection-error-{case}-file-{file_backed}-prepared-{prepared}-explicit-{explicit_transaction}-triggers-{with_triggers}"
                        );
                        let path = directory.path().join(format!("{label}.db"));
                        let f = Connection::open(if file_backed {
                            path.to_str().unwrap()
                        } else {
                            ":memory:"
                        })
                        .await
                        .unwrap();
                        let r = rusqlite::Connection::open_in_memory().unwrap();
                        for setup in [
                            "CREATE TABLE t(k INTEGER PRIMARY KEY,v INTEGER)",
                            "CREATE TABLE audit(n INTEGER)",
                        ] {
                            f.execute(setup).await.unwrap();
                            r.execute_batch(setup).unwrap();
                        }
                        if with_triggers {
                            for setup in [
                                "CREATE TRIGGER t_before BEFORE INSERT ON t BEGIN \
                                 INSERT INTO audit VALUES(NEW.k); END",
                                "CREATE TRIGGER t_after AFTER INSERT ON t BEGIN \
                                 INSERT INTO audit VALUES(-NEW.k); END",
                            ] {
                                f.execute(setup).await.unwrap();
                                r.execute_batch(setup).unwrap();
                            }
                        }
                        if explicit_transaction {
                            f.execute("BEGIN").await.unwrap();
                            r.execute_batch("BEGIN").unwrap();
                        }
                        f.execute("INSERT INTO t VALUES(1,5)").await.unwrap();
                        r.execute_batch("INSERT INTO t VALUES(1,5)").unwrap();
                        let sql = format!(
                            "INSERT INTO t(k,v) {source} RETURNING k, \
                             (SELECT abs(n) FROM (SELECT t.v AS n))"
                        );
                        let stock_error = {
                            let mut statement = r.prepare(&sql).unwrap();
                            let mut rows = statement.query([i64::MIN]).unwrap();
                            match rows.next() {
                                Err(error) => error,
                                Ok(_) => panic!("stock projection must overflow: {label}"),
                            }
                        };
                        assert!(stock_error.to_string().contains("overflow"), "{label}");
                        let params = [SqliteValue::Integer(i64::MIN)];
                        let frank_error = if prepared {
                            f.prepare(&sql)
                                .await
                                .unwrap()
                                .query_with_params(&params)
                                .await
                                .unwrap_err()
                        } else {
                            f.query_with_params(&sql, &params).await.unwrap_err()
                        };
                        assert!(
                            frank_error.to_string().contains("overflow"),
                            "{label}: {frank_error}"
                        );
                        for check in [
                            "SELECT k,v FROM t ORDER BY k",
                            "SELECT n FROM audit ORDER BY rowid",
                            "PRAGMA integrity_check",
                        ] {
                            assert_eq!(
                                cte_dml_frank_rows(&f, check, &[], false).await,
                                cte_dml_stock_rows(&r, check, &[]),
                                "{label}: {check}"
                            );
                        }
                        let later = "INSERT INTO t VALUES(90,90)";
                        f.execute(later).await.unwrap();
                        r.execute_batch(later).unwrap();
                        if explicit_transaction {
                            f.execute("COMMIT").await.unwrap();
                            r.execute_batch("COMMIT").unwrap();
                        }
                        for check in [
                            "SELECT k,v FROM t ORDER BY k",
                            "SELECT n FROM audit ORDER BY rowid",
                        ] {
                            assert_eq!(
                                cte_dml_frank_rows(&f, check, &[], false).await,
                                cte_dml_stock_rows(&r, check, &[]),
                                "{label}: after retry: {check}"
                            );
                        }
                        f.close().await.unwrap();
                    }
                }
            }
        }
    });
}

#[test]
fn with_insert_cte_abort_restores_rows_and_shadowed_names() {
    asupersync::test_utils::run_test(|| async {
        let directory = tempfile::tempdir().unwrap();
        let sql = "WITH log(x) AS (VALUES(21),(22),(23)) \
                   INSERT INTO t(k,v) SELECT x,x FROM log";
        for file_backed in [false, true] {
            for prepared in [false, true] {
                let label = format!("abort-file-{file_backed}-prepared-{prepared}");
                let path = directory.path().join(format!("{label}.db"));
                let f = Connection::open(if file_backed {
                    path.to_str().unwrap()
                } else {
                    ":memory:"
                })
                .await
                .unwrap();
                let r = rusqlite::Connection::open_in_memory().unwrap();
                cte_dml_setup(&f, &r, true).await;
                for setup in [
                    "CREATE TRIGGER t_stop BEFORE INSERT ON t WHEN NEW.k=22 \
                     BEGIN SELECT RAISE(ABORT,'cte rollback'); END",
                    "BEGIN",
                    "INSERT INTO log(msg) VALUES('before')",
                ] {
                    f.execute(setup).await.unwrap();
                    r.execute_batch(setup).unwrap();
                }
                let expected_error = r.execute(sql, []).expect_err("stock trigger aborts");
                assert!(expected_error.to_string().contains("cte rollback"));
                let actual_error = if prepared {
                    f.prepare(sql).await.unwrap().execute().await.map(|_| ())
                } else {
                    f.query(sql).await.map(|_| ())
                }
                .expect_err("FrankenSQLite trigger aborts");
                assert!(
                    actual_error.to_string().contains("cte rollback"),
                    "{label}: {actual_error}"
                );
                assert_cte_dml_state(&f, &r, &label).await;
                // A later statement still reaches the persistent table and
                // trigger, and the outer transaction remains usable.
                f.execute("INSERT INTO t(k,v) VALUES(24,24)").await.unwrap();
                r.execute_batch("INSERT INTO t(k,v) VALUES(24,24)").unwrap();
                assert_cte_dml_state(&f, &r, &label).await;
                f.execute("ROLLBACK").await.unwrap();
                r.execute_batch("ROLLBACK").unwrap();
                assert_cte_dml_state(&f, &r, &label).await;
                f.close().await.unwrap();
            }
        }
    });
}

#[test]
fn materialized_cte_shadow_preserves_values_instead_of_inheriting_table_rowid() {
    asupersync::test_utils::run_test(|| async {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cte-shadow.db");
        for database in [":memory:", path.to_str().unwrap()] {
            let f = Connection::open(database).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            for sql in [
                "CREATE TABLE nodes(id INTEGER PRIMARY KEY, label TEXT)",
                "INSERT INTO nodes VALUES(7,'persistent'),(19,'other')",
                "CREATE TABLE marker(k INTEGER PRIMARY KEY, label TEXT, v INTEGER)",
                "INSERT INTO marker VALUES(1,'left',9),(2,'right',9)",
            ] {
                f.execute(sql).await.unwrap();
                r.execute_batch(sql).unwrap();
            }
            for sql in [
                "WITH nodes(id,label) AS MATERIALIZED (VALUES(101,'left'),(205,'right')) \
                 SELECT c.id,c.label,m.v FROM nodes c JOIN marker m ON m.label=c.label ORDER BY c.id",
                "WITH nodes(label,id) AS MATERIALIZED (VALUES('left',101),('right',205)) \
                 SELECT c.id,c.label,m.v FROM nodes c JOIN marker m ON m.label=c.label ORDER BY c.id",
                "WITH nodes(id,label) AS MATERIALIZED (VALUES('00101','left'),('00205','right')) \
                 SELECT c.id,c.label,m.v FROM nodes c JOIN marker m ON m.label=c.label ORDER BY c.id",
                "WITH nodes(id,label) AS MATERIALIZED (VALUES(101,'left'),(205,'right')) \
                 SELECT c.id,c.label,m.v FROM nodes c JOIN marker m ON m.label=c.label \
                 WHERE c.id>100 ORDER BY c.id LIMIT 2",
                "WITH nodes(id,v) AS MATERIALIZED (VALUES(101,10),(205,20),(209,30)) \
                 SELECT id/100,sum(v) FROM nodes GROUP BY id/100 ORDER BY 1",
                "WITH nodes(id,label) AS MATERIALIZED (VALUES(101,'left'),(205,'right')) \
                 SELECT m.k,EXISTS(SELECT 1 FROM nodes c \
                 WHERE (c.id=m.k+100 AND c.label='left') OR c.id=m.k+203) \
                 FROM marker m ORDER BY m.k",
            ] {
                assert_materialized_cte_query(&f, &r, sql, database, 2, true).await;
                let restored = f
                    .query("SELECT id,label FROM nodes ORDER BY id")
                    .await
                    .unwrap();
                assert_eq!(
                    restored
                        .iter()
                        .map(|row| row.values().iter().map(tag_f).collect::<Vec<_>>())
                        .collect::<Vec<_>>(),
                    vec![
                        vec!["int:7", "text:persistent"],
                        vec!["int:19", "text:other"],
                    ],
                    "persistent table after {sql}"
                );
            }
            f.close().await.unwrap();
        }
    });
}

#[test]
fn materialized_cte_shadow_nonleading_primary_key_projects_fewer_columns() {
    asupersync::test_utils::run_test(|| async {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cte-narrow-shadow.db");
        for database in [":memory:", path.to_str().unwrap()] {
            let f = Connection::open(database).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            for sql in [
                "CREATE TABLE nodes(label TEXT, id INTEGER PRIMARY KEY, payload BLOB)",
                "INSERT INTO nodes VALUES('persistent',19,x'CAFE')",
            ] {
                f.execute(sql).await.unwrap();
                r.execute_batch(sql).unwrap();
            }
            for (sql, rows) in [
                (
                    "WITH nodes(v) AS MATERIALIZED (VALUES(101),(205)) \
                     SELECT sum(v),count(*) FROM nodes GROUP BY v/100 ORDER BY 1",
                    2,
                ),
                (
                    "WITH nodes(v) AS MATERIALIZED (VALUES(101),(205)) \
                     SELECT min(v),max(v) FROM nodes",
                    1,
                ),
                (
                    "SELECT id/10,sum(id) FROM nodes GROUP BY id/10 ORDER BY 1",
                    1,
                ),
                (
                    "SELECT rowid,sum(id) FROM nodes GROUP BY rowid ORDER BY rowid",
                    1,
                ),
            ] {
                assert_materialized_cte_query(&f, &r, sql, database, rows, true).await;
            }
            f.close().await.unwrap();
        }
    });
}

#[test]
fn materialized_cte_shadow_does_not_reuse_warmed_group_by_memo() {
    asupersync::test_utils::run_test(|| async {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cte-memo-shadow.db");
        for database in [":memory:", path.to_str().unwrap()] {
            let f = Connection::open(database).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            for sql in [
                "CREATE TABLE nodes(id INTEGER PRIMARY KEY, v INTEGER)",
                "INSERT INTO nodes VALUES(7,90),(19,91)",
            ] {
                f.execute(sql).await.unwrap();
                r.execute_batch(sql).unwrap();
            }
            let consumer = "SELECT id/100,sum(v) FROM nodes GROUP BY id/100";
            let prefix =
                "WITH nodes(id,v) AS MATERIALIZED (VALUES(101,10),(205,20),(209,30)) ";
            // Preserve expression spans as well as the stripped SELECT shape,
            // so the ordinary query can warm exactly the CTE consumer's memo.
            // Omitting ORDER BY admits the rowid-bucket fast lane; compare
            // unordered rows because SQL does not order these groups.
            let ordinary = format!("{}{consumer}", " ".repeat(prefix.len()));
            let shadow = format!("{prefix}{consumer}");
            for _ in 0..2 {
                assert_materialized_cte_query(&f, &r, &ordinary, database, 1, false).await;
                assert_materialized_cte_query(&f, &r, &shadow, database, 2, false).await;
            }
            assert_materialized_cte_query(&f, &r, &ordinary, database, 1, false).await;
            f.close().await.unwrap();
        }
    });
}

#[test]
fn dysy4_forward_cte_shadows_outer_table() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in ["CREATE TABLE a(v INTEGER)", "INSERT INTO a VALUES (1)"] {
            f.execute(sql).await.unwrap();
            r.execute_batch(sql).unwrap();
        }
        for (sql, expected) in [
            ("WITH q AS (SELECT v FROM a), a(v) AS (VALUES(2)) SELECT v FROM q", 2),
            ("WITH q AS (SELECT v FROM later), later(v) AS (VALUES(3)) SELECT v FROM q", 3),
            ("WITH q AS (SELECT v FROM main.a) SELECT v FROM q", 1),
            ("WITH q AS (WITH a(v) AS (VALUES(4)) SELECT v FROM a), a(v) AS (VALUES(2)) SELECT v FROM q", 4),
            ("WITH q AS (SELECT (SELECT v FROM a) AS v), a(v) AS (VALUES(2)) SELECT v FROM q", 2),
            ("WITH q AS (SELECT v FROM b), b AS (SELECT v+1 AS v FROM a), a(v) AS (VALUES(2)) SELECT v FROM q", 3),
            ("WITH q AS (WITH a(v) AS (VALUES(4)) SELECT v FROM a), a AS (SELECT v FROM q) SELECT v FROM a", 4),
            ("WITH q AS (WITH unused AS (SELECT v FROM a) SELECT 5 AS v), a AS (SELECT v FROM q) SELECT v FROM a", 5),
            ("WITH q AS (SELECT 2 IN a AS v), a(v) AS (VALUES(2)) SELECT v FROM q", 1),
            ("WITH q AS (SELECT v FROM a), a(v) AS (SELECT 2 WHERE 0) SELECT count(*) FROM q", 0),
            ("WITH q AS (SELECT v FROM a), a(v) AS (SELECT CAST(2 AS TEXT)) SELECT v=2 FROM q", 1),
        ] {
            let stock: i64 = r.query_row(sql, [], |row| row.get(0)).unwrap();
            assert_eq!(stock, expected, "stock control: {sql}");
            let actual = f.query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
            assert_eq!(actual.len(), 1, "{sql}");
            assert_eq!(actual[0].values(), &[SqliteValue::Integer(stock)], "{sql}");
            // Materialization must restore the real table after every statement.
            let restored = f.query("SELECT v FROM a").await
                .unwrap_or_else(|e| panic!("persistent table after {sql}: {e}"));
            assert_eq!(restored.len(), 1, "persistent table after {sql}");
            assert_eq!(restored[0].values(), &[SqliteValue::Integer(1)], "persistent table after {sql}");
        }
    });
}

#[test]
fn dysy4_qualified_cte_collision_keeps_existing_explicit_refusal() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in ["CREATE TABLE a(v INTEGER)", "INSERT INTO a VALUES(1)"] {
            f.execute(sql).await.unwrap();
            r.execute_batch(sql).unwrap();
        }
        // This pre-existing namespace limitation is outside the unqualified
        // sibling-resolution fix. Do not silently read the CTE as main.a.
        let sql = "WITH q AS (SELECT v FROM main.a), a(v) AS (VALUES(2)) SELECT v FROM q";
        let stock: i64 = r.query_row(sql, [], |row| row.get(0)).unwrap();
        assert_eq!(stock, 1);
        let error = f.query(sql).await.expect_err("qualified collision remains unsupported");
        assert!(matches!(error, fsqlite_error::FrankenError::NotImplemented(_)), "{error}");
        let rows = f.query("SELECT v FROM main.a").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values(), &[SqliteValue::Integer(stock)]);
    });
}

#[test]
fn dysy4_cte_cycles_fail_without_leaking_materialized_tables() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in ["CREATE TABLE a(v)", "INSERT INTO a VALUES(1)"] {
            f.execute(sql).await.unwrap();
            r.execute_batch(sql).unwrap();
        }
        for cycle in [
            "WITH q AS (SELECT v FROM a), a AS (SELECT v FROM q) SELECT v FROM q",
            "WITH x AS (SELECT v FROM q), q AS (SELECT v FROM a), a AS (SELECT v FROM q) SELECT v FROM x",
        ] {
            let stock = r.prepare(cycle).expect_err("stock detects sibling cycle");
            assert!(stock.to_string().contains("circular reference: q"), "{stock}");
            let actual = f.query(cycle).await.expect_err("sibling cycle must fail");
            assert!(actual.to_string().contains("circular reference: q"), "{actual}");
            assert_eq!(fq(&f, "SELECT v FROM a").await, vec![vec!["int:1"]]);
        }
        for sql in [
            "WITH q AS (SELECT * FROM z), z AS (SELECT * FROM q) SELECT 1",
            "WITH RECURSIVE q(v) AS (VALUES(1) UNION ALL SELECT v+1 FROM q WHERE v<3) SELECT sum(v) FROM q",
        ] {
            let stock: i64 = r.query_row(sql, [], |row| row.get(0)).unwrap();
            let actual = f.query(sql).await.unwrap();
            assert_eq!(actual.len(), 1, "{sql}");
            assert_eq!(actual[0].values(), &[SqliteValue::Integer(stock)], "{sql}");
        }
    });
}

#[test]
fn cte_composition_edges_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE base(id INTEGER PRIMARY KEY, g TEXT, v INTEGER)",
            "INSERT INTO base VALUES (1,'x',10),(2,'x',20),(3,'y',30),(4,'y',40),(5,'z',50)",
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

        // later CTE references an earlier CTE
        check(
            "chained ctes",
            fq(
                &f,
                "WITH a AS (SELECT id,v FROM base WHERE v>=20), \
                  b AS (SELECT id, v*2 AS w FROM a WHERE id<5) \
             SELECT id,w FROM b ORDER BY id",
            )
            .await,
            rq(
                &r,
                "WITH a AS (SELECT id,v FROM base WHERE v>=20), \
                  b AS (SELECT id, v*2 AS w FROM a WHERE id<5) \
             SELECT id,w FROM b ORDER BY id",
            ),
            &mut diffs,
        );

        // CTE name shadows a real table -> the CTE wins inside its query
        check(
            "cte shadows table",
            fq(
                &f,
                "WITH base AS (SELECT 99 AS id, 'shadow' AS g, 0 AS v) \
             SELECT id,g,v FROM base",
            )
            .await,
            rq(
                &r,
                "WITH base AS (SELECT 99 AS id, 'shadow' AS g, 0 AS v) \
             SELECT id,g,v FROM base",
            ),
            &mut diffs,
        );

        // CTE self-joined (reused twice under different aliases)
        check("cte reused twice", fq(&f,
            "WITH c AS (SELECT id,g,v FROM base) \
             SELECT c1.id, c2.id FROM c c1 JOIN c c2 ON c1.g=c2.g AND c1.id<c2.id ORDER BY c1.id,c2.id").await,
            rq(&r,
            "WITH c AS (SELECT id,g,v FROM base) \
             SELECT c1.id, c2.id FROM c c1 JOIN c c2 ON c1.g=c2.g AND c1.id<c2.id ORDER BY c1.id,c2.id"), &mut diffs);

        // explicit column-list renaming on the CTE
        check(
            "cte explicit col names",
            fq(
                &f,
                "WITH renamed(pk, grp, val) AS (SELECT id,g,v FROM base) \
             SELECT pk, grp, val FROM renamed WHERE val>25 ORDER BY pk",
            )
            .await,
            rq(
                &r,
                "WITH renamed(pk, grp, val) AS (SELECT id,g,v FROM base) \
             SELECT pk, grp, val FROM renamed WHERE val>25 ORDER BY pk",
            ),
            &mut diffs,
        );

        // CTE feeding an aggregate + GROUP BY
        check(
            "cte aggregate",
            fq(
                &f,
                "WITH g AS (SELECT g AS grp, v FROM base) \
             SELECT grp, count(*), sum(v) FROM g GROUP BY grp ORDER BY grp",
            )
            .await,
            rq(
                &r,
                "WITH g AS (SELECT g AS grp, v FROM base) \
             SELECT grp, count(*), sum(v) FROM g GROUP BY grp ORDER BY grp",
            ),
            &mut diffs,
        );

        // WITH nested inside a FROM subquery
        check("with inside from subquery", fq(&f,
            "SELECT total FROM (WITH s AS (SELECT sum(v) AS total FROM base) SELECT total FROM s)").await,
            rq(&r,
            "SELECT total FROM (WITH s AS (SELECT sum(v) AS total FROM base) SELECT total FROM s)"), &mut diffs);

        // WITH inside a scalar subquery in the SELECT list
        check("with inside scalar subquery", fq(&f,
            "SELECT id, (WITH m AS (SELECT max(v) AS mx FROM base) SELECT mx FROM m) AS mx FROM base WHERE id<=2 ORDER BY id").await,
            rq(&r,
            "SELECT id, (WITH m AS (SELECT max(v) AS mx FROM base) SELECT mx FROM m) AS mx FROM base WHERE id<=2 ORDER BY id"), &mut diffs);

        // CTE referenced inside an IN subquery
        check(
            "cte in-subquery",
            fq(
                &f,
                "WITH hi AS (SELECT id FROM base WHERE v>=30) \
             SELECT id,g FROM base WHERE id IN (SELECT id FROM hi) ORDER BY id",
            )
            .await,
            rq(
                &r,
                "WITH hi AS (SELECT id FROM base WHERE v>=30) \
             SELECT id,g FROM base WHERE id IN (SELECT id FROM hi) ORDER BY id",
            ),
            &mut diffs,
        );

        // compound (UNION) query as a CTE body
        check(
            "compound cte body",
            fq(
                &f,
                "WITH u AS (SELECT id FROM base WHERE g='x' UNION SELECT id FROM base WHERE g='z') \
             SELECT id FROM u ORDER BY id",
            )
            .await,
            rq(
                &r,
                "WITH u AS (SELECT id FROM base WHERE g='x' UNION SELECT id FROM base WHERE g='z') \
             SELECT id FROM u ORDER BY id",
            ),
            &mut diffs,
        );

        // CTE feeding a DML statement (INSERT ... SELECT with a leading WITH)
        ex(&f, &r, "CREATE TABLE sink(id INTEGER, doubled INTEGER)").await;
        ex(&f, &r, "WITH d AS (SELECT id, v*2 AS w FROM base WHERE g='y') INSERT INTO sink(id,doubled) SELECT id,w FROM d").await;
        check(
            "after cte-fed insert",
            fq(&f, "SELECT id,doubled FROM sink ORDER BY id").await,
            rq(&r, "SELECT id,doubled FROM sink ORDER BY id"),
            &mut diffs,
        );

        assert!(
            diffs.is_empty(),
            "{} CTE-composition divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
