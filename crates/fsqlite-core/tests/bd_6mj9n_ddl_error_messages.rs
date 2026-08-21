// Keeper for bd-audit-internal-wrapped-stock-messages-6mj9n: DDL create/drop
// conflict errors must match SQLite's messages VERBATIM under SQLITE_ERROR —
// never wrapped as an Internal error ("internal error:" prefix / SQLITE_INTERNAL).
// Oracle: sqlite3 3.46.1. Each case was confirmed divergent (DIFF-INTERNAL) by
// the audit probe before the FrankenError::Internal -> FunctionError swap.
use fsqlite_core::connection::Connection;

async fn err_of(setup: &[&str], sql: &str) -> String {
    let c = Connection::open(":memory:").await.unwrap();
    for s in setup {
        c.execute(s).await.unwrap();
    }
    c.execute(sql)
        .await
        .expect_err("statement should have been rejected")
        .to_string()
}

fn assert_stock(msg: &str, expected: &str) {
    assert_eq!(msg, expected, "message must match stock verbatim");
    assert!(
        !msg.starts_with("internal error:"),
        "must not be wrapped as an Internal error (was: {msg:?})"
    );
}

#[test]
fn ddl_conflict_error_messages_6mj9n() {
    asupersync::test_utils::run_test(|| async {
        // CREATE ... already exists (dup + cross-namespace table/view clash).
        assert_stock(
            &err_of(&["CREATE TABLE t(a)"], "CREATE TABLE t(b)").await,
            "table t already exists",
        );
        assert_stock(
            &err_of(&["CREATE VIEW v AS SELECT 1"], "CREATE VIEW v AS SELECT 2").await,
            "view v already exists",
        );
        assert_stock(
            &err_of(&["CREATE VIEW v AS SELECT 1"], "CREATE TABLE v(a)").await,
            "view v already exists",
        );
        assert_stock(
            &err_of(&["CREATE TABLE t(a)"], "CREATE VIEW t AS SELECT 1").await,
            "table t already exists",
        );
        assert_stock(
            &err_of(&["CREATE TABLE t(a)", "CREATE INDEX ix ON t(a)"], "CREATE INDEX ix ON t(a)")
                .await,
            "index ix already exists",
        );
        assert_stock(
            &err_of(
                &["CREATE TABLE t(a)", "CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1; END"],
                "CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT 2; END",
            )
            .await,
            "trigger tr already exists",
        );
        // CREATE TABLE ... AS SELECT onto an existing name.
        assert_stock(
            &err_of(&["CREATE TABLE t(a)"], "CREATE TABLE t AS SELECT 1").await,
            "table t already exists",
        );
        // CREATE TABLE in an attached database onto an existing name.
        assert_stock(
            &err_of(
                &["ATTACH ':memory:' AS aux", "CREATE TABLE aux.t(a)"],
                "CREATE TABLE aux.t(b)",
            )
            .await,
            "table t already exists",
        );

        // DROP <object> that does not exist.
        assert_stock(&err_of(&[], "DROP INDEX nope").await, "no such index: nope");
        assert_stock(&err_of(&[], "DROP VIEW nope").await, "no such view: nope");
        assert_stock(&err_of(&[], "DROP TRIGGER nope").await, "no such trigger: nope");
    });
}

#[test]
fn query_and_vtab_error_messages_6mj9n() {
    asupersync::test_utils::run_test(|| async {
        // INSERT into a missing table (SELECT/UPDATE/DELETE already used the
        // correct NoSuchTable path; INSERT wrapped it in Internal).
        assert_stock(&err_of(&[], "INSERT INTO nope VALUES(1)").await, "no such table: nope");
        // A missing column reports just the reference — never " in table T".
        assert_stock(
            &err_of(&["CREATE TABLE t(a)"], "SELECT nope FROM t").await,
            "no such column: nope",
        );
        // `table.*` where the table is not a FROM source.
        assert_stock(
            &err_of(&["CREATE TABLE t(a)"], "SELECT x.* FROM t").await,
            "no such table: x",
        );
        // A bad column reference in an INSERT ... VALUES list.
        assert_stock(
            &err_of(&["CREATE TABLE t(a)"], "INSERT INTO t(a) VALUES(nope)").await,
            "no such column: nope",
        );
        // Column/table DDL on a virtual table.
        assert_stock(
            &err_of(
                &["CREATE VIRTUAL TABLE ft USING fts5(x)"],
                "ALTER TABLE ft RENAME COLUMN x TO y",
            )
            .await,
            "cannot rename columns of virtual table \"ft\"",
        );
        assert_stock(
            &err_of(
                &["CREATE VIRTUAL TABLE ft USING fts5(x, z)"],
                "ALTER TABLE ft DROP COLUMN z",
            )
            .await,
            "cannot drop column from virtual table \"ft\"",
        );
        assert_stock(
            &err_of(&["CREATE VIRTUAL TABLE ft USING fts5(x)"], "ALTER TABLE ft ADD COLUMN z").await,
            "virtual tables may not be altered",
        );
    });
}

#[test]
fn schema_and_generated_column_error_messages_6mj9n() {
    asupersync::test_utils::run_test(|| async {
        // DETACH of a database that is not attached / reserved.
        assert_stock(&err_of(&[], "DETACH nodb").await, "no such database: nodb");
        assert_stock(&err_of(&[], "DETACH main").await, "cannot detach database main");
        assert_stock(&err_of(&[], "DETACH temp").await, "no such database: temp");
        // ATTACH a schema name that is already in use.
        assert_stock(
            &err_of(&["ATTACH ':memory:' AS aux"], "ATTACH ':memory:' AS aux").await,
            "database aux is already in use",
        );
        // A PRAGMA qualified by an unknown schema (distinct wording from DETACH).
        assert_stock(&err_of(&[], "PRAGMA nodb.user_version").await, "unknown database nodb");
        // A table whose every column is GENERATED is rejected at CREATE time.
        assert_stock(
            &err_of(&[], "CREATE TABLE t(a INTEGER GENERATED ALWAYS AS (1) VIRTUAL)").await,
            "must have at least one non-generated column",
        );
        assert_stock(
            &err_of(&[], "CREATE TABLE t(a AS (1), b AS (2))").await,
            "must have at least one non-generated column",
        );
        // INSERT into / UPDATE of a generated column.
        assert_stock(
            &err_of(
                &["CREATE TABLE t(a INTEGER, b INTEGER GENERATED ALWAYS AS (a+1) VIRTUAL)"],
                "INSERT INTO t(a,b) VALUES(1,2)",
            )
            .await,
            "cannot INSERT into generated column \"b\"",
        );
        assert_stock(
            &err_of(
                &[
                    "CREATE TABLE t(a INTEGER, b INTEGER GENERATED ALWAYS AS (a+1) VIRTUAL)",
                    "INSERT INTO t(a) VALUES(1)",
                ],
                "UPDATE t SET b=5",
            )
            .await,
            "cannot UPDATE generated column \"b\"",
        );
    });
}

#[test]
fn unknown_attached_schema_create_messages_zh5kl() {
    asupersync::test_utils::run_test(|| async {
        // A CREATE-family statement qualified by an unknown attached schema
        // reports "unknown database <schema>" (routed through
        // execute_statement_dispatch_with_fk_scope). DML/DROP take a separate
        // fast-path and are tracked as a follow-up on bd-...-zh5kl.
        assert_stock(&err_of(&[], "CREATE TABLE nodb.t(a)").await, "unknown database nodb");
        assert_stock(&err_of(&[], "CREATE VIEW nodb.v AS SELECT 1").await, "unknown database nodb");
    });
}
