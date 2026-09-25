use super::*;
use crate::compat::changeset::apply_changeset;
use fsqlite_ext_session::Changeset;

async fn database(schema: &str) -> Connection {
    let connection = Connection::open(":memory:").await.unwrap();
    connection.execute_batch(schema).await.unwrap();
    connection
        .execute("PRAGMA recursive_triggers=ON")
        .await
        .unwrap();
    connection
}

async fn values(connection: &Connection, sql: &str, columns: usize) -> Vec<Vec<SqliteValue>> {
    connection
        .query(sql)
        .await
        .unwrap()
        .iter()
        .map(|row| {
            (0..columns)
                .map(|column| row.get(column).unwrap().clone())
                .collect()
        })
        .collect()
}

#[test]
fn live_capture_coalesces_and_applies_the_encoded_changeset() {
    asupersync::test_utils::run_test(|| async {
        let schema = "CREATE TABLE items(id INTEGER PRIMARY KEY,value TEXT,payload BLOB); INSERT INTO items VALUES(1,'old',x'00ff'),(2,'gone',NULL),(3,'stable',x'80');";
        let mut source = database(schema).await;
        let mut replica = database(schema).await;
        let cx = Cx::new();
        let mut capture = ChangesetCapture::begin(&mut source, &cx, CaptureOptions::new(["items"]))
            .await
            .unwrap();
        capture.execute_batch("UPDATE items SET value='middle' WHERE id=1; UPDATE items SET value='final' WHERE id=1; INSERT INTO items VALUES(4,'temporary',NULL); DELETE FROM items WHERE id=4; UPDATE items SET value='changed' WHERE id=3; UPDATE items SET value='stable' WHERE id=3;").await.unwrap();
        capture
            .execute(
                "UPDATE items SET id=11,value=?1 WHERE id=1",
                &[SqliteValue::Text("embedded\0text".into())],
            )
            .await
            .unwrap();
        capture
            .execute_batch(
                "DELETE FROM items WHERE id=2; INSERT INTO items VALUES(5,'added',x'010203');",
            )
            .await
            .unwrap();
        let captured = capture.commit().await.unwrap();
        assert_eq!(captured.changes, 4);
        assert_eq!(captured.touched_rows, 6);
        let decoded = Changeset::decode(&captured.bytes).unwrap();
        assert!(
            decoded.tables[0].rows[..2]
                .iter()
                .all(|row| row.op == ChangeOp::Delete)
        );
        apply_changeset(&mut replica, &decoded).await.unwrap();
        assert_eq!(
            values(&source, "SELECT * FROM items ORDER BY id", 3).await,
            values(&replica, "SELECT * FROM items ORDER BY id", 3).await
        );
        assert!(
            source
                .query("SELECT name FROM temp.sqlite_schema WHERE name GLOB '__fsqlite_capture_*'")
                .await
                .unwrap()
                .is_empty()
        );
        // Opening another scope proves normal SQL/capture remains usable.
        let capture = ChangesetCapture::begin(&mut source, &cx, CaptureOptions::new(["items"]))
            .await
            .unwrap();
        assert!(capture.commit().await.unwrap().bytes.is_empty());
    });
}

#[test]
fn replace_deletions_and_savepoint_rollback_share_the_application_transaction() {
    asupersync::test_utils::run_test(|| async {
        let schema = "CREATE TABLE t(id INTEGER PRIMARY KEY,u TEXT UNIQUE,v INTEGER); INSERT INTO t VALUES(1,'one',10),(2,'two',20);";
        let mut source = database(schema).await;
        let mut replica = database(schema).await;
        let cx = Cx::new();
        let mut capture = ChangesetCapture::begin(&mut source, &cx, CaptureOptions::new(["t"]))
            .await
            .unwrap();
        capture.savepoint("nested \" scope").await.unwrap();
        capture
            .execute_batch("UPDATE t SET v=999; INSERT INTO t VALUES(3,'three',30);")
            .await
            .unwrap();
        capture.rollback_to("nested \" scope").await.unwrap();
        capture.release("nested \" scope").await.unwrap();
        capture
            .execute("INSERT OR REPLACE INTO t VALUES(4,'one',40)", &[])
            .await
            .unwrap();
        let captured = capture.commit().await.unwrap();
        assert_eq!(captured.touched_rows, 2);
        assert_eq!(captured.changes, 2);
        apply_changeset(&mut replica, &Changeset::decode(&captured.bytes).unwrap())
            .await
            .unwrap();
        assert_eq!(
            values(&source, "SELECT * FROM t ORDER BY id", 3).await,
            values(&replica, "SELECT * FROM t ORDER BY id", 3).await
        );
    });
}

#[test]
fn composite_without_rowid_keys_quoted_names_and_binary_values_roundtrip() {
    asupersync::test_utils::run_test(|| async {
        let schema = "CREATE TABLE \"quoted table\"(v BLOB,b TEXT COLLATE NOCASE,a INTEGER,PRIMARY KEY(a,b)) WITHOUT ROWID; INSERT INTO \"quoted table\" VALUES(x'ff00','Alpha',9223372036854775807);";
        let mut source = database(schema).await;
        let mut replica = database(schema).await;
        let cx = Cx::new();
        let mut options = CaptureOptions::new(["quoted table"]);
        options.indirect = true;
        let mut capture = ChangesetCapture::begin(&mut source, &cx, options)
            .await
            .unwrap();
        capture
            .execute_batch("UPDATE \"quoted table\" SET b='alpha',v=x'0001';")
            .await
            .unwrap();
        let captured = capture.commit().await.unwrap();
        assert_eq!(
            &captured.bytes[..5],
            &[b'T', 3, 0, 2, 1],
            "reordered composite PK ordinals must survive on the wire"
        );
        let decoded = Changeset::decode(&captured.bytes).unwrap();
        assert_eq!(
            captured.changes, 2,
            "binary key rename is DELETE plus INSERT"
        );
        assert!(decoded.tables[0].rows.iter().all(|row| row.indirect));
        apply_changeset(&mut replica, &decoded).await.unwrap();
        assert_eq!(
            values(&source, "SELECT * FROM \"quoted table\"", 3).await,
            values(&replica, "SELECT * FROM \"quoted table\"", 3).await
        );
    });
}

#[test]
fn null_keys_are_ignored_but_crossing_into_or_out_of_null_is_captured() {
    asupersync::test_utils::run_test(|| async {
        let mut source = database("CREATE TABLE t(k TEXT PRIMARY KEY,v TEXT); INSERT INTO t VALUES(NULL,'initial'),('old','retired');").await;
        let cx = Cx::new();
        let mut capture = ChangesetCapture::begin(&mut source, &cx, CaptureOptions::new(["t"]))
            .await
            .unwrap();
        capture.execute_batch("UPDATE t SET k='new' WHERE k IS NULL; UPDATE t SET k=NULL WHERE k='old'; INSERT INTO t VALUES(NULL,'ignored');").await.unwrap();
        let captured = capture.commit().await.unwrap();
        assert_eq!(captured.changes, 2);
        let decoded = Changeset::decode(&captured.bytes).unwrap();
        assert!(decoded.tables[0].rows.iter().all(|row| {
            let key = if row.op == ChangeOp::Insert {
                &row.new_values[0]
            } else {
                &row.old_values[0]
            };
            *key != ChangesetValue::Null
        }));
    });
}

#[test]
fn errors_and_abandonment_cannot_commit_uncaptured_writes() {
    asupersync::test_utils::run_test(|| async {
        let mut source = database("CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT);").await;
        let cx = Cx::new();
        for disallowed in [
            "COMMIT",
            "INSERT INTO t VALUES(2,'prefix'); COMMIT; BEGIN",
            "DROP TABLE t",
            "PRAGMA recursive_triggers=OFF",
            "INSERT INTO temp.__fsqlite_capture_budget VALUES(0,0,0)",
        ] {
            let mut capture = ChangesetCapture::begin(&mut source, &cx, CaptureOptions::new(["t"]))
                .await
                .unwrap();
            capture
                .execute("INSERT INTO t VALUES(1,'owned')", &[])
                .await
                .unwrap();
            assert!(capture.execute_batch(disallowed).await.is_err());
            assert!(matches!(
                capture.commit().await,
                Err(CaptureError::Poisoned)
            ));
            assert!(source.query("SELECT * FROM t").await.unwrap().is_empty());
        }
        let mut capture = ChangesetCapture::begin(&mut source, &cx, CaptureOptions::new(["t"]))
            .await
            .unwrap();
        capture
            .execute("INSERT INTO t VALUES(1,'abandoned')", &[])
            .await
            .unwrap();
        drop(capture);
        assert!(source.query("SELECT * FROM t").await.unwrap().is_empty());
        let mut capture = ChangesetCapture::begin(&mut source, &cx, CaptureOptions::new(["t"]))
            .await
            .unwrap();
        capture
            .execute("INSERT INTO t VALUES(1,'first')", &[])
            .await
            .unwrap();
        assert!(
            capture
                .execute("INSERT INTO t VALUES(1,'duplicate')", &[])
                .await
                .is_err()
        );
        assert!(matches!(
            capture.commit().await,
            Err(CaptureError::Poisoned)
        ));
        assert!(source.query("SELECT * FROM t").await.unwrap().is_empty());
    });
}

#[test]
fn every_capture_budget_refuses_before_application_commit() {
    asupersync::test_utils::run_test(|| async {
        for budget in 0..4 {
            let mut source = database("CREATE TABLE t(id INTEGER PRIMARY KEY,v BLOB);").await;
            let cx = Cx::new();
            let mut options = CaptureOptions::new(["t"]);
            match budget {
                0 => options.max_touched_rows = 1,
                1 => options.max_cells = 1,
                2 => options.max_image_bytes = 1024,
                _ => options.max_changeset_bytes = 16,
            }
            let mut capture = ChangesetCapture::begin(&mut source, &cx, options)
                .await
                .unwrap();
            let result = capture
                .execute("INSERT INTO t VALUES(1,zeroblob(2048)),(2,x'00')", &[])
                .await;
            // A limit may fire in a trigger or before collecting the final image.
            let committed = capture.commit().await;
            assert!(committed.is_err(), "budget {budget}; DML result {result:?}");
            assert!(source.query("SELECT * FROM t").await.unwrap().is_empty());
        }
    });
}

#[test]
fn schema_admission_and_nested_transaction_refusals_preserve_the_source() {
    asupersync::test_utils::run_test(|| async {
        let cx = Cx::new();
        for schema in [
            "CREATE TABLE t(v TEXT);",
            "CREATE TABLE t(id INTEGER PRIMARY KEY,v INT GENERATED ALWAYS AS(id+1));",
            "CREATE TABLE t(id INTEGER PRIMARY KEY); CREATE TEMP TABLE t(id INTEGER);",
            "CREATE TABLE t(id INTEGER PRIMARY KEY); CREATE TRIGGER app AFTER INSERT ON t BEGIN SELECT 1; END;",
        ] {
            let mut source = database(schema).await;
            assert!(
                ChangesetCapture::begin(&mut source, &cx, CaptureOptions::new(["t"]))
                    .await
                    .is_err()
            );
        }
        let mut source = database("CREATE TABLE t(id INTEGER PRIMARY KEY);").await;
        source.execute("BEGIN").await.unwrap();
        source.execute("INSERT INTO t VALUES(17)").await.unwrap();
        assert!(
            ChangesetCapture::begin(&mut source, &cx, CaptureOptions::new(["t"]))
                .await
                .is_err()
        );
        assert!(source.in_transaction());
        source.execute("COMMIT").await.unwrap();
        assert_eq!(
            values(&source, "SELECT * FROM t", 1).await,
            vec![vec![SqliteValue::Integer(17)]]
        );
    });
}

#[test]
fn cancellation_and_unpolled_begin_have_no_application_effect() {
    asupersync::test_utils::run_test(|| async {
        let mut source = database("CREATE TABLE t(id INTEGER PRIMARY KEY);").await;
        let cx = Cx::new();
        drop(ChangesetCapture::begin(
            &mut source,
            &cx,
            CaptureOptions::new(["t"]),
        ));
        assert!(!source.in_transaction());
        let mut capture = ChangesetCapture::begin(&mut source, &cx, CaptureOptions::new(["t"]))
            .await
            .unwrap();
        capture
            .execute("INSERT INTO t VALUES(1)", &[])
            .await
            .unwrap();
        cx.cancel();
        assert!(capture.commit().await.is_err());
        assert!(source.query("SELECT * FROM t").await.unwrap().is_empty());
    });
}

#[test]
fn wire_accounting_matches_session_encoding_at_varint_boundaries() {
    for bytes in [0, 1, 127, 128, 16383, 16384] {
        let info = TableInfo {
            name: "t".into(),
            column_count: 3,
            pk_flags: vec![true, false, false],
        };
        let row = ChangesetRow {
            op: ChangeOp::Insert,
            indirect: false,
            old_values: Vec::new(),
            new_values: vec![
                ChangesetValue::Integer(i64::MIN),
                ChangesetValue::Text("x".repeat(bytes)),
                ChangesetValue::Blob(vec![0; bytes]),
            ],
        };
        let expected = table_wire_size(&info, std::slice::from_ref(&row)).unwrap();
        let mut encoded = Vec::new();
        info.encode(&mut encoded);
        row.encode_changeset(&mut encoded);
        assert_eq!(encoded.len(), expected);
    }
}

#[test]
fn production_journal_sql_executes_on_stock_sqlite_and_rolls_back_first_touches() {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    connection.execute_batch("PRAGMA recursive_triggers=ON; CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'old'); BEGIN; CREATE TEMP TABLE __fsqlite_capture_budget(n INTEGER NOT NULL,bytes INTEGER NOT NULL,cells INTEGER NOT NULL); INSERT INTO __fsqlite_capture_budget VALUES(0,0,0);").unwrap();
    let plan = TablePlan {
        info: TableInfo {
            name: "t".into(),
            column_count: 2,
            pk_flags: vec![true, false],
        },
        pk_ordinals: vec![1, 0],
        columns: vec!["id".into(), "v".into()],
        keys: vec![0],
        journal: format!("{PREFIX}0"),
    };
    for sql in journal_statements(&plan, &CaptureOptions::new(["t"])) {
        connection.execute_batch(&sql).unwrap();
    }
    connection
        .execute_batch("SAVEPOINT s; UPDATE t SET v='rolled back'; ROLLBACK TO s; RELEASE s;")
        .unwrap();
    assert_eq!(
        connection
            .query_row("SELECT n FROM __fsqlite_capture_budget", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    connection
        .execute_batch("UPDATE t SET v='middle'; UPDATE t SET v='final';")
        .unwrap();
    assert_eq!(
        connection
            .query_row("SELECT n FROM __fsqlite_capture_budget", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        connection
            .query_row("SELECT v1 FROM __fsqlite_capture_0", [], |row| row
                .get::<_, String>(0))
            .unwrap(),
        "old"
    );
    connection.execute_batch("ROLLBACK").unwrap();
    assert_eq!(
        connection
            .query_row("SELECT v FROM t", [], |row| row.get::<_, String>(0))
            .unwrap(),
        "old"
    );
}
