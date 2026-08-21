//! Error-message parity keeper: frank's error text must match stock SQLite
//! (3.53) VERBATIM for the common error conditions below. A differential probe
//! (frank vs rusqlite) confirmed these match; asserted here against the exact
//! expected string so a regression (e.g. an `Internal("internal error: …")`
//! wrapping, or reworded text) is caught.
//!
//! Scope note (bd-ttof2): the " in <SQL> at offset N" suffix feature has begun
//! — aggregate-in-WHERE misuse is asserted verbatim below (name + two spellings
//! + offset). Still NOT asserted (suffix not yet wired at those sites):
//! no-such-column in SELECT, no-such-function, wrong-arg-count, ambiguous-column,
//! table-already-exists; plus the open bugs (INTEGER-PK dup wording, INSERT
//! column-count `not implemented:` prefix). Those are tracked in bd-ttof2.

use fsqlite_core::connection::Connection;

/// Run `setup` then `sql`; assert `sql` fails with EXACTLY `expected` message.
async fn err_is(setup: &[&str], sql: &str, expected: &str) {
    let f = Connection::open(":memory:").await.unwrap();
    let _ = f.execute("PRAGMA foreign_keys=ON").await;
    for s in setup {
        let _ = f.execute(s).await;
    }
    match f.execute(sql).await {
        Ok(_) => panic!("expected an error for `{sql}`, but it succeeded"),
        Err(e) => assert_eq!(e.to_string(), expected, "sql=`{sql}`"),
    }
}

/// Like [`err_is`] but drives `query` (the row-returning path). SELECT analysis
/// errors are asserted through `query`, since `execute` of a SELECT currently
/// discards rows via a fast path that can bypass some prepare-time validation
/// (tracked separately in bd-ttof2).
async fn query_err_is(setup: &[&str], sql: &str, expected: &str) {
    let f = Connection::open(":memory:").await.unwrap();
    for s in setup {
        let _ = f.execute(s).await;
    }
    match f.query(sql).await {
        Ok(_) => panic!("expected an error for `{sql}`, but it succeeded"),
        Err(e) => assert_eq!(e.to_string(), expected, "sql=`{sql}`"),
    }
}

#[test]
fn constraint_violations() {
    asupersync::test_utils::run_test(|| async {
        err_is(&["CREATE TABLE t(x INT UNIQUE)", "INSERT INTO t VALUES (1)"],
               "INSERT INTO t VALUES (1)", "UNIQUE constraint failed: t.x").await;
        err_is(&["CREATE TABLE t(x INT NOT NULL)"],
               "INSERT INTO t VALUES (NULL)", "NOT NULL constraint failed: t.x").await;
        err_is(&["CREATE TABLE t(x INT CHECK (x > 0))"],
               "INSERT INTO t VALUES (-1)", "CHECK constraint failed: x > 0").await;
        err_is(&["CREATE TABLE t(x INT CONSTRAINT pos CHECK (x > 0))"],
               "INSERT INTO t VALUES (-1)", "CHECK constraint failed: pos").await;
        err_is(&["CREATE TABLE p(id INT PRIMARY KEY)", "CREATE TABLE c(pid INT REFERENCES p(id))"],
               "INSERT INTO c VALUES (99)", "FOREIGN KEY constraint failed").await;
    });
}

#[test]
fn strict_type_errors() {
    asupersync::test_utils::run_test(|| async {
        err_is(&["CREATE TABLE t(x INTEGER) STRICT"],
               "INSERT INTO t VALUES ('notanint')", "cannot store TEXT value in INTEGER column t.x").await;
    });
}

#[test]
fn ddl_and_ordinal_errors() {
    asupersync::test_utils::run_test(|| async {
        // bd-ttof2 fix: ALTER TABLE ADD a duplicate column now reports verbatim
        // (was Internal "internal error: duplicate column name: b").
        err_is(&["CREATE TABLE t(a INT, b INT)"],
               "ALTER TABLE t ADD COLUMN b INT", "duplicate column name: b").await;
        // bd-ttof2 fix: a GROUP BY ordinal out of range now carries the 1-based
        // term-position prefix ("1st GROUP BY term ..."), matching ORDER BY.
        err_is(&["CREATE TABLE t(a INT)"],
               "SELECT a FROM t GROUP BY 9", "1st GROUP BY term out of range - should be between 1 and 1").await;
        err_is(&["CREATE TABLE t(a INT)"],
               "SELECT a FROM t ORDER BY 5", "1st ORDER BY term out of range - should be between 1 and 1").await;
    });
}

#[test]
fn name_resolution_and_txn_errors() {
    asupersync::test_utils::run_test(|| async {
        err_is(&[], "SELECT * FROM nonexistent", "no such table: nonexistent").await;
        // bd-ttof2 fix: an unknown INSERT column now matches stock verbatim
        // (was Internal("internal error: column '…' not found in table '…'")).
        err_is(&["CREATE TABLE t(a INT)"],
               "INSERT INTO t(nope) VALUES (1)", "table t has no column named nope").await;
        err_is(&[], "RELEASE nonexistent_sp", "no such savepoint: nonexistent_sp").await;
        err_is(&[], "COMMIT", "cannot commit - no transaction is active").await;
    });
}

#[test]
fn view_modify_errors() {
    asupersync::test_utils::run_test(|| async {
        // bd-ttof2 fix: DML against a plain view (no INSTEAD OF trigger) reports
        // "cannot modify V because it is a view" verbatim, matching stock — not
        // the "no such table: V" a table-DML fallthrough raised before, which
        // leaked because the :memory: INSERT/UPDATE/DELETE prepared fast-path
        // bypassed the view-DML dispatch check. Covers all three DML verbs.
        let setup: &[&str] = &["CREATE TABLE t(x INT)", "CREATE VIEW v AS SELECT x FROM t"];
        err_is(setup, "INSERT INTO v VALUES (1)", "cannot modify v because it is a view").await;
        err_is(setup, "DELETE FROM v", "cannot modify v because it is a view").await;
        err_is(setup, "UPDATE v SET x = 1", "cannot modify v because it is a view").await;
    });
}

#[test]
fn aggregate_in_where_misuse_offset() {
    asupersync::test_utils::run_test(|| async {
        // bd-ttof2 (offset-suffix feature, first slice): an aggregate in WHERE
        // now (a) names the offending aggregate, (b) uses stock's two spellings
        // ("misuse of aggregate: NAME()" when the SELECT is itself an aggregate
        // query, else "misuse of aggregate function NAME()"), and (c) carries
        // SQLite's " in <SQL> at offset N" suffix — N is the byte offset of the
        // offending call (the rightmost aggregate, per stock). Was the bare,
        // offsetless "misuse of aggregate: ".
        let t: &[&str] = &["CREATE TABLE t(x INT)"];
        query_err_is(t, "SELECT max(x) FROM t WHERE max(x) > 0",
               "misuse of aggregate: max() in SELECT max(x) FROM t WHERE max(x) > 0 at offset 27").await;
        query_err_is(t, "SELECT * FROM t WHERE sum(x) > 0",
               "misuse of aggregate function sum() in SELECT * FROM t WHERE sum(x) > 0 at offset 22").await;
        // offset points at the aggregate even when it is not the leading token
        query_err_is(t, "SELECT * FROM t WHERE x > sum(x)",
               "misuse of aggregate function sum() in SELECT * FROM t WHERE x > sum(x) at offset 26").await;
        // two aggregates: stock names (and offsets) the RIGHTMOST
        query_err_is(t, "SELECT * FROM t WHERE sum(x) + count(*) > 0",
               "misuse of aggregate function count() in SELECT * FROM t WHERE sum(x) + count(*) > 0 at offset 31").await;
    });
}

#[test]
fn window_misuse_offset() {
    asupersync::test_utils::run_test(|| async {
        // bd-ttof2 offset-suffix (slice 2): a window function where one is not
        // allowed (WHERE / GROUP BY / HAVING) names the offending call and
        // carries the offset: "misuse of window function NAME() in <SQL> at
        // offset N". Was the bare, offsetless "misuse of window function".
        let t: &[&str] = &["CREATE TABLE t(x INT, g TEXT)"];
        query_err_is(t, "SELECT x FROM t WHERE row_number() OVER () = 1",
               "misuse of window function row_number() in SELECT x FROM t WHERE row_number() OVER () = 1 at offset 22").await;
        query_err_is(t, "SELECT g FROM t GROUP BY row_number() OVER ()",
               "misuse of window function row_number() in SELECT g FROM t GROUP BY row_number() OVER () at offset 25").await;
        query_err_is(t, "SELECT g FROM t GROUP BY g HAVING row_number() OVER () > 0",
               "misuse of window function row_number() in SELECT g FROM t GROUP BY g HAVING row_number() OVER () > 0 at offset 34").await;
        // offset points at the window call even when it is not the leading token
        query_err_is(t, "SELECT x FROM t WHERE x > sum(x) OVER ()",
               "misuse of window function sum() in SELECT x FROM t WHERE x > sum(x) OVER () at offset 26").await;
    });
}

#[test]
fn nested_aggregate_misuse_offset() {
    asupersync::test_utils::run_test(|| async {
        // bd-ttof2 offset-suffix (slice 3): an aggregate nested inside another
        // aggregate names the INNER call and carries its offset: "misuse of
        // aggregate function NAME() in <SQL> at offset N". Was the bare,
        // offsetless "misuse of aggregate function".
        let t: &[&str] = &["CREATE TABLE t(x INT, g TEXT)"];
        query_err_is(t, "SELECT sum(count(*)) FROM t",
               "misuse of aggregate function count() in SELECT sum(count(*)) FROM t at offset 11").await;
        query_err_is(t, "SELECT max(avg(x)) FROM t GROUP BY g",
               "misuse of aggregate function avg() in SELECT max(avg(x)) FROM t GROUP BY g at offset 11").await;
        query_err_is(t, "SELECT g FROM t GROUP BY g HAVING sum(count(*)) > 0",
               "misuse of aggregate function count() in SELECT g FROM t GROUP BY g HAVING sum(count(*)) > 0 at offset 38").await;
        // a non-nested aggregate precedes the nested one: still names the inner
        query_err_is(t, "SELECT avg(x) + sum(count(*)) FROM t",
               "misuse of aggregate function count() in SELECT avg(x) + sum(count(*)) FROM t at offset 20").await;
        // triple nesting reports the innermost
        query_err_is(t, "SELECT sum(avg(count(*))) FROM t",
               "misuse of aggregate function count() in SELECT sum(avg(count(*))) FROM t at offset 15").await;
    });
}
