//! bd-xoixz: `UPDATE OR IGNORE <t> SET ... FROM <src> WHERE ...` whose new row
//! fails a CHECK or NOT NULL constraint must silently SKIP that row (matching
//! C SQLite), not abort the whole statement. `codegen_update_from` previously
//! passed a `None` ignore-label to the constraint emitters, so a violation
//! aborted; the fix routes the skip to the innermost loop's Next when
//! `or_conflict` is Ignore, mirroring the plain `codegen_update` path.
//! Verified against C SQLite via the same statement on both engines.
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
async fn frank_state(c: &Connection, sql: &str, ncols: usize) -> Vec<Vec<String>> {
    let mut r: Vec<Vec<String>> = c
        .query(sql)
        .await
        .unwrap()
        .iter()
        .map(|row| row.values().iter().take(ncols).map(render).collect())
        .collect();
    r.sort();
    r
}
fn sqlite_state(c: &rusqlite::Connection, sql: &str, ncols: usize) -> Vec<Vec<String>> {
    let mut stmt = c.prepare(sql).unwrap();
    let mut r: Vec<Vec<String>> = stmt
        .query_map([], |row| {
            Ok((0..ncols)
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
                .collect::<Vec<_>>())
        })
        .unwrap()
        .map(Result::unwrap)
        .collect();
    r.sort();
    r
}

/// Run the DDL/seed/DML on both engines and diff the resulting target state.
async fn check(ddl: &[&str], dml: &str, select_state: &str, ncols: usize) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in ddl {
        f.execute(s)
            .await
            .unwrap_or_else(|e| panic!("frank ddl `{s}`: {e}"));
        r.execute_batch(s).unwrap();
    }
    // Both engines run the same UPDATE OR IGNORE ... FROM ...; neither should
    // error (the violating rows are skipped, not aborted).
    f.execute(dml)
        .await
        .unwrap_or_else(|e| panic!("frank `{dml}` must not abort under OR IGNORE: {e}"));
    r.execute_batch(dml).unwrap();
    assert_eq!(
        frank_state(&f, select_state, ncols).await,
        sqlite_state(&r, select_state, ncols),
        "target state diverged after `{dml}`"
    );
}

#[test]
fn update_from_or_ignore_check_violation_skips_row() {
    asupersync::test_utils::run_test(|| async {
        // v has CHECK(v < 100). src row 2 pushes v to 500 (violates) -> that row
        // is skipped; rows 1 and 3 update. Row 4 has no src match (unchanged).
        check(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER CHECK(v < 100));",
                "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40);",
                "CREATE TABLE src (id INTEGER, newv INTEGER);",
                "INSERT INTO src VALUES (1, 11), (2, 500), (3, 33);",
            ],
            "UPDATE OR IGNORE t SET v = src.newv FROM src WHERE t.id = src.id",
            "SELECT id, v FROM t",
            2,
        )
        .await;
    });
}

#[test]
fn update_from_or_ignore_not_null_violation_skips_row() {
    asupersync::test_utils::run_test(|| async {
        // n is NOT NULL. src row 2 sets n to NULL (violates) -> skipped; rows 1
        // and 3 update.
        check(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER NOT NULL);",
                "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);",
                "CREATE TABLE src (id INTEGER, newn INTEGER);",
                "INSERT INTO src VALUES (1, 11), (2, NULL), (3, 33);",
            ],
            "UPDATE OR IGNORE t SET n = src.newn FROM src WHERE t.id = src.id",
            "SELECT id, n FROM t",
            2,
        )
        .await;
    });
}

#[test]
fn update_from_or_ignore_all_rows_violate_leaves_table_unchanged() {
    asupersync::test_utils::run_test(|| async {
        // Every matched row violates the CHECK -> all skipped, table unchanged,
        // still no abort.
        check(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER CHECK(v < 100));",
                "INSERT INTO t VALUES (1, 10), (2, 20);",
                "CREATE TABLE src (id INTEGER, newv INTEGER);",
                "INSERT INTO src VALUES (1, 200), (2, 300);",
            ],
            "UPDATE OR IGNORE t SET v = src.newv FROM src WHERE t.id = src.id",
            "SELECT id, v FROM t",
            2,
        )
        .await;
    });
}

#[test]
fn update_from_without_or_ignore_still_aborts_on_check() {
    asupersync::test_utils::run_test(|| async {
        // Control: WITHOUT OR IGNORE, a CHECK violation must ABORT on both
        // engines and leave the table unchanged (statement atomicity).
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER CHECK(v < 100));",
            "INSERT INTO t VALUES (1, 10), (2, 20);",
            "CREATE TABLE src (id INTEGER, newv INTEGER);",
            "INSERT INTO src VALUES (1, 11), (2, 500);",
        ] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        let dml = "UPDATE t SET v = src.newv FROM src WHERE t.id = src.id";
        let f_err = f.execute(dml).await.is_err();
        let r_err = r.execute_batch(dml).is_err();
        assert!(
            f_err,
            "frank must abort a plain UPDATE..FROM CHECK violation"
        );
        assert_eq!(f_err, r_err, "abort parity for plain UPDATE..FROM");
        assert_eq!(
            frank_state(&f, "SELECT id, v FROM t", 2).await,
            sqlite_state(&r, "SELECT id, v FROM t", 2),
            "aborted statement must leave the table unchanged on both engines"
        );
    });
}
