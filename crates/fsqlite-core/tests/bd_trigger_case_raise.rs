//! Keeper: the CASE-wrapped conditional-RAISE trigger idiom
//! `SELECT CASE WHEN <cond> THEN RAISE(<action>, <msg>) END` — the canonical
//! alternative to `SELECT RAISE(...) WHERE <cond>`. Previously frank rejected it
//! with "expression form is not supported in this connection path: Raise {..}"
//! (the recognizer only matched the direct/WHERE form). Oracle: sqlite3 3.46.1.
use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn as_text(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Text(s) => s.to_string(),
        other => panic!("expected TEXT, got {other:?}"),
    }
}

async fn state(conn: &Connection) -> String {
    let rows = conn
        .query_with_params(
            "SELECT 'STATE:'||IFNULL(group_concat(x ORDER BY x),'') FROM t",
            &[],
        )
        .await
        .unwrap();
    as_text(&rows[0].values()[0])
}

async fn mk(trigger_body: &str) -> Connection {
    let c = Connection::open(":memory:").await.unwrap();
    c.execute("CREATE TABLE t(x INTEGER)").await.unwrap();
    c.execute(&format!(
        "CREATE TRIGGER tr BEFORE INSERT ON t BEGIN {trigger_body} END"
    ))
    .await
    .unwrap();
    c
}

#[test]
fn case_raise_abort_rolls_back_statement_and_carries_message() {
    asupersync::test_utils::run_test(|| async {
        let c = mk("SELECT CASE WHEN NEW.x=2 THEN RAISE(ABORT,'abort2') END;").await;
        let err = c
            .execute("INSERT INTO t VALUES (1),(2),(3)")
            .await
            .expect_err("RAISE(ABORT) must error");
        assert!(err.to_string().contains("abort2"), "message: {err}");
        // ABORT rolls back the whole statement's rows.
        assert_eq!(state(&c).await, "STATE:");
    });
}

#[test]
fn case_raise_ignore_skips_offending_row_no_error() {
    asupersync::test_utils::run_test(|| async {
        let c = mk("SELECT CASE WHEN NEW.x=2 THEN RAISE(IGNORE) END;").await;
        c.execute("INSERT INTO t VALUES (1),(2),(3)")
            .await
            .expect("RAISE(IGNORE) must not error");
        assert_eq!(state(&c).await, "STATE:1,3");
    });
}

#[test]
fn case_raise_fail_keeps_rows_before_failure() {
    asupersync::test_utils::run_test(|| async {
        let c = mk("SELECT CASE WHEN NEW.x=2 THEN RAISE(FAIL,'fail2') END;").await;
        let err = c
            .execute("INSERT INTO t VALUES (1),(2),(3)")
            .await
            .expect_err("RAISE(FAIL) must error");
        assert!(err.to_string().contains("fail2"), "message: {err}");
        // FAIL keeps rows inserted before the failing row; row 3 is not reached.
        assert_eq!(state(&c).await, "STATE:1");
    });
}

#[test]
fn case_raise_predicate_false_does_not_fire() {
    asupersync::test_utils::run_test(|| async {
        // WHEN never true -> RAISE never fires -> all rows insert.
        let c = mk("SELECT CASE WHEN NEW.x=999 THEN RAISE(ABORT,'nope') END;").await;
        c.execute("INSERT INTO t VALUES (1),(2),(3)")
            .await
            .expect("no RAISE should fire");
        assert_eq!(state(&c).await, "STATE:1,2,3");
    });
}
