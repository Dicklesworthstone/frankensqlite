#![recursion_limit = "512"]
//! GH#516: RETURNING stays part of the outer triggered INSERT.
//!
//! The conformance cases run on the unchanged engine too. The explicitly
//! ignored dispatch probe is an acceptance test for the compiled-row candidate,
//! not a requirement that the unmodified main branch already has that candidate.
//! All SQLite oracle connections use separate databases from FrankenSQLite.
//!
//! The correlated UPDATE/DELETE cases also cover the frozen RETURNING row:
//! hidden rowids, declared comparison metadata, changed primary keys, and
//! statement rollback when evaluating the projection fails.

use std::sync::{Arc, Mutex};

use fsqlite_core::connection::{Connection, Row, TraceEvent, TraceMask};
use fsqlite_types::value::SqliteValue;
use rusqlite::types::ValueRef;
use serde::Deserialize;

fn stock_value(value: ValueRef<'_>) -> SqliteValue {
    match value {
        ValueRef::Null => SqliteValue::Null,
        ValueRef::Integer(value) => SqliteValue::Integer(value),
        ValueRef::Real(value) => SqliteValue::Float(value),
        ValueRef::Text(value) => {
            SqliteValue::Text(std::str::from_utf8(value).expect("UTF-8 fixture").into())
        }
        ValueRef::Blob(value) => SqliteValue::Blob(value.to_vec().into()),
    }
}

fn stock_rows(db: &rusqlite::Connection, sql: &str) -> rusqlite::Result<Vec<Vec<SqliteValue>>> {
    let mut statement = db.prepare(sql)?;
    let width = statement.column_count();
    statement
        .query_map([], |row| {
            (0..width).map(|i| row.get_ref(i).map(stock_value)).collect()
        })?
        .collect()
}

fn stock_unbound_rows(
    db: &rusqlite::Connection,
    sql: &str,
) -> rusqlite::Result<Vec<Vec<SqliteValue>>> {
    let mut statement = db.prepare(sql)?;
    let width = statement.column_count();
    // raw_query leaves placeholders unbound, matching native query()'s NULL
    // bindings. query_map([]) would reject a parameterized statement itself.
    let mut rows = statement.raw_query();
    let mut result = Vec::new();
    while let Some(row) = rows.next()? {
        result.push(
            (0..width)
                .map(|i| row.get_ref(i).map(stock_value))
                .collect::<rusqlite::Result<_>>()?,
        );
    }
    Ok(result)
}

fn assert_rows(actual: &[Row], expected: &[Vec<SqliteValue>], label: &str) {
    let actual: Vec<_> = actual.iter().map(|row| row.values().to_vec()).collect();
    let types = |rows: &[Vec<SqliteValue>]| {
        rows.iter()
            .map(|row| row.iter().map(SqliteValue::typeof_str).collect::<Vec<_>>())
            .collect::<Vec<_>>()
    };
    assert_eq!(types(&actual), types(expected), "storage classes: {label}");
    assert_eq!(actual, expected, "values and order: {label}");
}

#[derive(Deserialize)]
struct Case {
    name: String,
    setup: Vec<String>,
    insert: String,
    succeeds: bool,
    after: Vec<String>,
}

fn cases() -> Vec<Case> {
    serde_json::from_str(include_str!("gh516_returning_cases.json"))
        .expect("RETURNING conformance cases")
}

async fn snapshots(native: &Connection, stock: &rusqlite::Connection, label: &str) {
    for sql in [
        "SELECT changes(),total_changes(),last_insert_rowid()",
        "SELECT * FROM t ORDER BY id",
        "SELECT * FROM audit ORDER BY rowid",
    ] {
        assert_rows(
            &native.query(sql).await.expect("native snapshot"),
            &stock_rows(stock, sql).expect("stock snapshot"),
            &format!("{label}: {sql}"),
        );
    }
}

async fn differential_returning(wal: bool) {
    for case in cases() {
        let directory = tempfile::tempdir().expect("separate database directory");
        let path = if wal {
            directory.path().join("native.db").to_str().unwrap().to_owned()
        } else {
            ":memory:".to_owned()
        };
        let native = Connection::open(&path).await.expect("native open");
        let stock = if wal {
            rusqlite::Connection::open(directory.path().join("stock.db")).unwrap()
        } else {
            rusqlite::Connection::open_in_memory().unwrap()
        };
        if wal {
            native.execute("PRAGMA journal_mode=WAL").await.unwrap();
            stock.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        }
        for sql in &case.setup {
            stock.execute_batch(sql).unwrap_or_else(|e| panic!("{} stock setup: {sql}: {e}", case.name));
            native.execute(sql).await.unwrap_or_else(|e| panic!("{} native setup: {sql}: {e}", case.name));
        }
        let expected = stock_rows(&stock, &case.insert);
        let actual = native.query(&case.insert).await;
        assert_eq!(expected.is_ok(), case.succeeds, "invalid fixture {}: {expected:?}", case.name);
        assert_eq!(actual.is_ok(), expected.is_ok(), "{} wal={wal}: native={actual:?}, stock={expected:?}", case.name);
        match (actual, expected) {
            (Ok(actual), Ok(expected)) => assert_rows(&actual, &expected, &case.name),
            (Err(actual), Err(expected)) => {
                if case.name.starts_with("raise/") {
                    let expected_msg = expected.to_string();
                    let actual_reason = match &actual {
                        fsqlite_error::FrankenError::TriggerRaise(msg)
                        | fsqlite_error::FrankenError::FunctionError(msg)
                        | fsqlite_error::FrankenError::RaiseFail(msg)
                        | fsqlite_error::FrankenError::TransactionRolledBack { reason: msg } => {
                            msg.as_str()
                        }
                        other => panic!("{} unexpected raise error variant: {other:?}", case.name),
                    };
                    assert!(
                        expected_msg.ends_with(actual_reason),
                        "{} raise message mismatch: actual={actual:?}, expected={expected_msg}",
                        case.name
                    );
                } else {
                    let code = expected.sqlite_error().expect("SQLite constraint error");
                    assert_eq!(
                        actual.error_code() as i32,
                        code.extended_code & 0xff,
                        "{} error code",
                        case.name
                    );
                    assert_eq!(
                        actual.to_string(),
                        expected.to_string(),
                        "{} error message",
                        case.name
                    );
                }
            }
            _ => unreachable!(),
        }
        snapshots(&native, &stock, &case.name).await;
        for sql in &case.after {
            stock.execute_batch(sql).unwrap();
            native.execute(sql).await.unwrap();
            snapshots(&native, &stock, &format!("{} after {sql}", case.name)).await;
        }
        let contents = stock_rows(&stock, "SELECT * FROM t ORDER BY id").unwrap();
        let audit = stock_rows(&stock, "SELECT * FROM audit ORDER BY rowid").unwrap();
        native.close().await.unwrap();
        drop(stock);
        if wal {
            // Stock reads the actual candidate file only after native close.
            let persisted = rusqlite::Connection::open(&path).unwrap();
            assert_eq!(stock_rows(&persisted, "SELECT * FROM t ORDER BY id").unwrap(), contents, "{} persisted t", case.name);
            assert_eq!(stock_rows(&persisted, "SELECT * FROM audit ORDER BY rowid").unwrap(), audit, "{} persisted audit", case.name);
            let integrity: String = persisted.query_row("PRAGMA integrity_check", [], |row| row.get(0)).unwrap();
            assert_eq!(integrity, "ok", "{} persisted integrity", case.name);
        }
    }
}

#[test]
fn returning_conformance_memory() {
    asupersync::test_utils::run_test(|| async { differential_returning(false).await; });
}

#[test]
fn returning_conformance_wal() {
    asupersync::test_utils::run_test(|| async { differential_returning(true).await; });
}

#[test]
fn returning_original_parameters_survive_repeated_preparation() {
    asupersync::test_utils::run_test(|| async {
        let native = Connection::open(":memory:").await.unwrap();
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT)",
            "CREATE TABLE audit(id INTEGER,v TEXT)",
            "CREATE TRIGGER ai AFTER INSERT ON t BEGIN INSERT INTO audit VALUES(NEW.id,NEW.v); END",
        ] {
            native.execute(sql).await.unwrap();
            stock.execute_batch(sql).unwrap();
        }
        // RETURNING's anonymous placeholder is slot 5 in the original SQL,
        // not a slot accidentally reused for the synthetic single-row VALUES.
        let sql = "INSERT INTO t VALUES(?2,?1),(?4,?3) RETURNING id,v,?";
        let prepared = native.prepare(sql).await.unwrap();
        for offset in [0, 100] {
            let parameters = [
                SqliteValue::Text("a".into()), SqliteValue::Integer(offset + 1),
                SqliteValue::Text("b".into()), SqliteValue::Integer(offset + 2),
                SqliteValue::Text(format!("marker-{offset}").into()),
            ];
            let actual = prepared.query_with_params(&parameters).await.unwrap();
            let mut statement = stock.prepare(sql).unwrap();
            let expected: Vec<Vec<SqliteValue>> = statement.query_map(
                rusqlite::params!["a", offset + 1, "b", offset + 2, format!("marker-{offset}")],
                |row| (0..3).map(|i| row.get_ref(i).map(stock_value)).collect(),
            ).unwrap().collect::<rusqlite::Result<_>>().unwrap();
            assert_rows(&actual, &expected, "original parameter numbering");
        }
        drop(prepared);
        snapshots(&native, &stock, "bound parameters").await;
        native.close().await.unwrap();
    });
}

#[test]
fn returning_compiled_candidate_avoids_row_redispatch() {
    asupersync::test_utils::run_test(|| async {
        for timing in ["BEFORE", "AFTER"] {
            for projection in ["*", "id,v,id+1,CASE WHEN v IS NULL THEN 'missing' ELSE v END"] {
                let native = Connection::open(":memory:").await.unwrap();
                native.execute("CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT)").await.unwrap();
                native.execute("CREATE TABLE audit(v TEXT)").await.unwrap();
                native.execute(&format!("CREATE TRIGGER tr {timing} INSERT ON t WHEN NEW.v IS NULL BEGIN INSERT INTO audit VALUES(NEW.v); END")).await.unwrap();
                let events = Arc::new(Mutex::new(Vec::new()));
                let captured = Arc::clone(&events);
                native.trace_v2(TraceMask::STMT, Some(Arc::new(move |event| {
                    if let TraceEvent::Statement { sql } = event {
                        captured.lock().unwrap().push(sql);
                    }
                })));
                let sql = format!("INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c') RETURNING {projection}");
                let rows = native.query(&sql).await.unwrap();
                native.trace_v2(TraceMask::STMT, None);
                assert_eq!(rows.len(), 3);
                assert_eq!(events.lock().unwrap().len(), 1, "{timing}: synthetic complete row INSERTs remain");
                assert_eq!(native.query("SELECT count(*) FROM audit").await.unwrap()[0].values()[0], SqliteValue::Integer(0));
                native.close().await.unwrap();
            }
        }
    });
}

async fn compare_correlated_dml_returning(
    label: &str,
    setup: &str,
    sql: &str,
    succeeds: bool,
    checks: &[&str],
) {
    for file_backed in [false, true] {
        for prepared in [false, true] {
            let directory = tempfile::tempdir().expect("separate RETURNING databases");
            let native_path = if file_backed {
                directory
                    .path()
                    .join("native.db")
                    .to_str()
                    .unwrap()
                    .to_owned()
            } else {
                ":memory:".to_owned()
            };
            let native = Connection::open(&native_path).await.unwrap();
            let stock = if file_backed {
                rusqlite::Connection::open(directory.path().join("stock.db")).unwrap()
            } else {
                rusqlite::Connection::open_in_memory().unwrap()
            };
            if file_backed {
                native.execute("PRAGMA journal_mode=WAL").await.unwrap();
                stock.execute_batch("PRAGMA journal_mode=WAL").unwrap();
            }
            let context = format!("{label}, file_backed={file_backed}, prepared={prepared}");
            native.execute_batch(setup).await.unwrap_or_else(|error| {
                panic!("native setup: {context}: {error}");
            });
            stock.execute_batch(setup).unwrap_or_else(|error| {
                panic!("stock setup: {context}: {error}");
            });

            let expected = stock_unbound_rows(&stock, sql);
            let actual = if prepared {
                match native.prepare(sql).await {
                    Ok(statement) => statement.query().await,
                    Err(error) => Err(error),
                }
            } else {
                native.query(sql).await
            };
            assert_eq!(
                expected.is_ok(),
                succeeds,
                "invalid fixture: {context}: {expected:?}"
            );
            assert_eq!(
                actual.is_ok(),
                expected.is_ok(),
                "{context}: native={actual:?}, stock={expected:?}",
            );
            if let (Ok(actual), Ok(expected)) = (actual, expected) {
                assert_rows(&actual, &expected, &context);
            }
            for check in checks {
                let actual = native.query(check).await.unwrap_or_else(|error| {
                    panic!("native snapshot: {context}: {check}: {error}");
                });
                let expected = stock_rows(&stock, check).unwrap_or_else(|error| {
                    panic!("stock snapshot: {context}: {check}: {error}");
                });
                assert_rows(&actual, &expected, &format!("{context}: {check}"));
            }
            native.close().await.unwrap();
        }
    }
}

#[test]
fn correlated_returning_frozen_rowid_aliases_match_stock() {
    asupersync::test_utils::run_test(|| async {
        compare_correlated_dml_returning(
            "unshadowed hidden rowid aliases",
            "CREATE TABLE t(v TEXT);\
             INSERT INTO t(rowid,v) VALUES(11,'a'),(23,'b'),(37,'c');\
             CREATE TABLE ids(id INTEGER);\
             INSERT INTO ids VALUES(11),(23),(37);\
             CREATE VIEW id_view AS SELECT id FROM ids;",
            "DELETE FROM t WHERE rowid < 30 RETURNING *,rowid,_rowid_,oid,\
               (SELECT count(*) FROM id_view WHERE id_view.id=t.oid)",
            true,
            &["SELECT changes()", "SELECT rowid,* FROM t ORDER BY rowid"],
        )
        .await;
        compare_correlated_dml_returning(
            "declared rowid and _rowid_ shadow only their own names",
            "CREATE TABLE t(rowid TEXT,_rowid_ TEXT,v TEXT);\
             INSERT INTO t(oid,rowid,_rowid_,v) VALUES\
               (11,'declared-a','other-a','a'),(23,'declared-b','other-b','b');\
             CREATE TABLE ids(id INTEGER,label TEXT);\
             INSERT INTO ids VALUES(11,'declared-a'),(23,'declared-b');\
             CREATE VIEW id_view AS SELECT id,label FROM ids;",
            "DELETE FROM t RETURNING *,rowid,_rowid_,oid,\
               (SELECT count(*) FROM id_view WHERE id_view.id=t.oid \
                  AND id_view.label=t.rowid)",
            true,
            &["SELECT changes()", "SELECT oid,* FROM t ORDER BY oid"],
        )
        .await;
    });
}

#[test]
fn correlated_returning_preserves_outer_column_metadata() {
    asupersync::test_utils::run_test(|| async {
        compare_correlated_dml_returning(
            "frozen columns retain affinity and collation",
            "CREATE TABLE t(id INTEGER PRIMARY KEY,word TEXT COLLATE NOCASE,\
               number INTEGER,text_number TEXT,padded TEXT COLLATE RTRIM);\
             INSERT INTO t VALUES(1,'Alpha',7,'5','tail '),(2,'bETA',9,'10','tail  ');\
             CREATE TABLE keys(word TEXT COLLATE BINARY,number_text TEXT,raw_number);\
             INSERT INTO keys VALUES('ALPHA','8',5),('BETA','10',10);\
             CREATE VIEW key_view AS SELECT word,number_text,raw_number FROM keys;",
            "UPDATE t SET number=number+1 RETURNING id,word='ALPHA',number='8',\
               text_number=5,padded='tail',\
               (SELECT count(*) FROM key_view AS k WHERE t.word=k.word),\
               (SELECT count(*) FROM key_view AS k WHERE t.number=k.number_text),\
               (SELECT count(*) FROM key_view AS k WHERE t.text_number=k.raw_number)",
            true,
            &["SELECT changes()", "SELECT * FROM t ORDER BY id"],
        )
        .await;
    });
}

#[test]
fn correlated_returning_uses_new_primary_keys() {
    asupersync::test_utils::run_test(|| async {
        compare_correlated_dml_returning(
            "changed INTEGER PRIMARY KEY supplies every rowid alias",
            "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT);\
             INSERT INTO t VALUES(1,'a'),(2,'b');\
             CREATE TABLE ids(id INTEGER);\
             INSERT INTO ids VALUES(11),(12);\
             CREATE VIEW id_view AS SELECT id FROM ids;",
            "UPDATE t SET id=id+10,v=v||'!' WHERE id<3 \
             RETURNING *,rowid,_rowid_,oid,\
               (SELECT count(*) FROM id_view WHERE id_view.id=t.id)",
            true,
            &["SELECT changes()", "SELECT * FROM t ORDER BY id"],
        )
        .await;
        compare_correlated_dml_returning(
            "changed WITHOUT ROWID composite key precedes AFTER trigger",
            "CREATE TABLE t(a TEXT,b INTEGER,v TEXT,PRIMARY KEY(a,b)) WITHOUT ROWID;\
             INSERT INTO t VALUES('a',1,'x'),('b',2,'y');\
             CREATE TABLE ids(a TEXT,b INTEGER);\
             INSERT INTO ids VALUES('a!',11),('b!',12);\
             CREATE VIEW id_view AS SELECT a,b FROM ids;\
             CREATE TABLE audit(a TEXT,b INTEGER);\
             CREATE VIEW audit_view AS SELECT a,b FROM audit;\
             CREATE TRIGGER au AFTER UPDATE ON t BEGIN \
               INSERT INTO audit VALUES(NEW.a,NEW.b); END;",
            "UPDATE t SET a=a||'!',b=b+10 WHERE b<3 RETURNING *,\
               (SELECT count(*) FROM id_view AS k WHERE k.a=t.a AND k.b=t.b),\
               (SELECT count(*) FROM audit_view AS l WHERE l.b<=t.b)",
            true,
            &[
                "SELECT changes()",
                "SELECT * FROM t ORDER BY a,b",
                "SELECT * FROM audit ORDER BY rowid",
            ],
        )
        .await;
    });
}

#[test]
fn correlated_returning_projection_error_rolls_back_statement() {
    asupersync::test_utils::run_test(|| async {
        compare_correlated_dml_returning(
            "second projection error rolls back earlier rows and AFTER triggers",
            "CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER);\
             INSERT INTO t VALUES(1,10),(2,20),(3,30);\
             CREATE TABLE docs(id INTEGER,payload TEXT);\
             INSERT INTO docs VALUES(1,'{\"n\":1}'),(2,'broken'),(3,'{\"n\":3}');\
             CREATE VIEW doc_view AS SELECT id,payload FROM docs;\
             CREATE TABLE audit(id INTEGER,v INTEGER);\
             CREATE TRIGGER au AFTER UPDATE ON t BEGIN \
               INSERT INTO audit VALUES(NEW.id,NEW.v); END;",
            "UPDATE t SET v=v+1 RETURNING id,\
               (SELECT json_extract(d.payload,'$.n') FROM doc_view AS d WHERE d.id=t.id)",
            false,
            &[
                "SELECT changes()",
                "SELECT * FROM t ORDER BY id",
                "SELECT * FROM audit ORDER BY rowid",
            ],
        )
        .await;
    });
}

#[test]
fn correlated_returning_ignore_skips_projection_and_after_trigger() {
    asupersync::test_utils::run_test(|| async {
        compare_correlated_dml_returning(
            "ignored UNIQUE conflict cannot evaluate an invalid projection",
            "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT UNIQUE);\
             INSERT INTO t VALUES(1,'free'),(2,'keep'),(3,'taken');\
             CREATE TABLE docs(id INTEGER,payload TEXT);\
             INSERT INTO docs VALUES(1,'{\"n\":1}'),(2,'broken');\
             CREATE VIEW doc_view AS SELECT id,payload FROM docs;\
             CREATE TABLE audit(phase TEXT,id INTEGER);\
             CREATE TRIGGER bu BEFORE UPDATE ON t BEGIN \
               INSERT INTO audit VALUES('before',OLD.id); END;\
             CREATE TRIGGER au AFTER UPDATE ON t BEGIN \
               INSERT INTO audit VALUES('after',NEW.id); END;",
            "UPDATE OR IGNORE t SET v=CASE id WHEN 1 THEN 'changed' ELSE 'taken' END \
             WHERE id<3 RETURNING id,v,\
               (SELECT json_extract(d.payload,'$.n') FROM doc_view AS d WHERE d.id=t.id)",
            true,
            &[
                "SELECT changes()",
                "SELECT * FROM t ORDER BY id",
                "SELECT * FROM audit ORDER BY rowid",
            ],
        )
        .await;
    });
}

#[test]
fn correlated_returning_unbound_parameters_do_not_alias_frozen_assignments() {
    asupersync::test_utils::run_test(|| async {
        compare_correlated_dml_returning(
            "BEFORE-trigger SET freezing cannot occupy original RETURNING slots",
            "CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER);\
             INSERT INTO t VALUES(1,10);\
             CREATE TABLE ids(id INTEGER);\
             INSERT INTO ids VALUES(1);\
             CREATE VIEW id_view AS SELECT id FROM ids;\
             CREATE TABLE audit(phase TEXT,v INTEGER);\
             CREATE TRIGGER bu BEFORE UPDATE ON t BEGIN \
               INSERT INTO audit VALUES('before',NEW.v); END;\
             CREATE TRIGGER au AFTER UPDATE ON t BEGIN \
               INSERT INTO audit VALUES('after',NEW.v); END;",
            // The unique locator bypasses replay, so no replay-generated
            // padding masks a collision with the unbound RETURNING slots.
            "UPDATE t SET v=v+1 WHERE id=1 RETURNING ?1,?4,?,\
               (SELECT count(*) FROM id_view WHERE id_view.id=t.id)",
            true,
            &[
                "SELECT changes()",
                "SELECT * FROM t ORDER BY id",
                "SELECT * FROM audit ORDER BY rowid",
            ],
        )
        .await;
    });
}

#[test]
fn correlated_returning_external_probe_with_all_rowid_aliases_shadowed() {
    asupersync::test_utils::run_test(|| async {
        compare_correlated_dml_returning(
            "an unaffected external probe does not require a SQL-visible rowid",
            "PRAGMA foreign_keys=OFF;\
             CREATE TABLE t(rowid INTEGER,_rowid_ TEXT,oid TEXT,v TEXT);\
             INSERT INTO t VALUES(1,'a','oa','va'),(2,'b','ob','vb'),(3,'c','oc','vc');\
             CREATE TABLE external_keys(id INTEGER PRIMARY KEY,label TEXT);\
             INSERT INTO external_keys VALUES(1,'first'),(2,'second'),(3,'third');",
            "DELETE FROM t WHERE rowid<3 RETURNING *,\
               (SELECT k.label FROM external_keys AS k WHERE k.id=t.rowid)",
            true,
            &["SELECT changes()", "SELECT * FROM t ORDER BY rowid"],
        )
        .await;
    });
}

#[test]
fn insert_replay_unbound_returning_parameters_do_not_alias_source_values() {
    asupersync::test_utils::run_test(|| async {
        for timing in ["BEFORE", "AFTER"] {
            let setup = format!(
                "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT);\
                 CREATE TABLE ids(id INTEGER);\
                 INSERT INTO ids VALUES(1),(2);\
                 CREATE VIEW id_view AS SELECT id FROM ids;\
                 CREATE TABLE audit(id INTEGER,v TEXT);\
                 CREATE TRIGGER tr {timing} INSERT ON t BEGIN \
                   INSERT INTO audit VALUES(NEW.id,NEW.v); END;"
            );
            for projection in [
                "id,v,?1,?2",
                "id,v,?1,?4,?,:marker,:marker",
                "id,v,?1,?4,(SELECT count(*) FROM id_view WHERE id_view.id=t.id)",
            ] {
                let sql = format!(
                    "INSERT INTO t VALUES(1,'first'),(2,'second') RETURNING {projection}"
                );
                compare_correlated_dml_returning(
                    &format!("{timing} INSERT preserves unbound caller slots: {projection}"),
                    &setup,
                    &sql,
                    true,
                    &[
                        "SELECT changes()",
                        "SELECT * FROM t ORDER BY id",
                        "SELECT * FROM audit ORDER BY rowid",
                    ],
                )
                .await;
            }
        }
    });
}

#[test]
fn insert_replay_unbound_upsert_assignments_remain_null() {
    asupersync::test_utils::run_test(|| async {
        for returning in ["", " RETURNING id,v,?1,?4,?"] {
            let sql = format!(
                "INSERT INTO t VALUES(1,'first'),(2,'second') \
                 ON CONFLICT(id) DO UPDATE SET v=?2{returning}"
            );
            compare_correlated_dml_returning(
                &format!("UPSERT preserves unbound caller slots: {returning}"),
                "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT);\
                 INSERT INTO t VALUES(1,'old-first'),(2,'old-second');\
                 CREATE TABLE audit(id INTEGER,v TEXT);\
                 CREATE TRIGGER au AFTER UPDATE ON t BEGIN \
                   INSERT INTO audit VALUES(NEW.id,NEW.v); END;",
                &sql,
                true,
                &[
                    "SELECT changes()",
                    "SELECT * FROM t ORDER BY id",
                    "SELECT * FROM audit ORDER BY rowid",
                ],
            )
            .await;
        }
    });
}

#[test]
fn insert_replay_explicit_short_bindings_are_rejected_before_writes() {
    asupersync::test_utils::run_test(|| async {
        for prepared in [false, true] {
            let native = Connection::open(":memory:").await.unwrap();
            let stock = rusqlite::Connection::open_in_memory().unwrap();
            let setup = "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT);\
                         CREATE TABLE audit(id INTEGER);\
                         CREATE TRIGGER ai AFTER INSERT ON t BEGIN \
                           INSERT INTO audit VALUES(NEW.id); END;";
            native.execute_batch(setup).await.unwrap();
            stock.execute_batch(setup).unwrap();
            let sql = "INSERT INTO t VALUES(1,'first'),(2,'second') RETURNING id,v,?2";
            for parameter_count in [0, 1] {
                let parameters = vec![SqliteValue::Integer(99); parameter_count];
                let actual = if prepared {
                    native
                        .prepare(sql)
                        .await
                        .unwrap()
                        .query_with_params(&parameters)
                        .await
                } else {
                    native.query_with_params(sql, &parameters).await
                };
                let mut reference = stock.prepare(sql).unwrap();
                let stock_parameters = vec![99_i64; parameter_count];
                assert!(
                    reference
                        .query(rusqlite::params_from_iter(stock_parameters))
                        .is_err(),
                    "stock must reject an explicit short parameter slice"
                );
                assert!(
                    actual.is_err(),
                    "prepared={prepared}, parameter_count={parameter_count}: {actual:?}"
                );
                for check in ["SELECT count(*) FROM t", "SELECT count(*) FROM audit"] {
                    let actual = native.query(check).await.unwrap();
                    let expected = stock_rows(&stock, check).unwrap();
                    assert_rows(
                        &actual,
                        &expected,
                        "a binding error cannot write or fire triggers",
                    );
                }
            }
            native.close().await.unwrap();
        }
    });
}
