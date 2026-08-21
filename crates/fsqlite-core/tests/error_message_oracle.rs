//! Error-message parity keeper: frank's error text must match stock SQLite
//! (3.53) VERBATIM for the common error conditions below. A differential probe
//! (frank vs rusqlite) confirmed these match; asserted here against the exact
//! expected string so a regression (e.g. an `Internal("internal error: …")`
//! wrapping, or reworded text) is caught.
//!
//! Scope note (bd-ttof2, FIX-FORWARD): SQLite does NOT append a
//! " in <SQL> at offset N" suffix to any error MESSAGE — the byte offset is
//! exposed only via the separate sqlite3_error_offset() C API. An earlier
//! bd-ttof2 slice misread rusqlite's Display wrapper (which formats the offset
//! into the string on the prepare path) as stock behavior; verified against
//! sqlite3 CLI 3.46.1 and rusqlite 3.53's bare `SqlInputError.msg` that neither
//! the CLI nor the C API carries the suffix. The `_offset`-named tests below
//! therefore assert the BARE stock message and guard against the suffix
//! regressing back in. Regression tracked in
//! bd-ttof2-offset-suffix-regression-npx41.

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
        // An aggregate in WHERE names the offending aggregate and uses stock's
        // two spellings: "misuse of aggregate: NAME()" when the SELECT is itself
        // an aggregate query, else "misuse of aggregate function NAME()"; the
        // two-aggregate case names the RIGHTMOST. bd-ttof2 FIX-FORWARD: there is
        // NO " in <SQL> at offset N" suffix — stock never puts the byte offset in
        // the message (verified vs rusqlite 3.53 + sqlite3 CLI 3.46.1).
        let t: &[&str] = &["CREATE TABLE t(x INT)"];
        query_err_is(t, "SELECT max(x) FROM t WHERE max(x) > 0",
               "misuse of aggregate: max()").await;
        query_err_is(t, "SELECT * FROM t WHERE sum(x) > 0",
               "misuse of aggregate function sum()").await;
        query_err_is(t, "SELECT * FROM t WHERE x > sum(x)",
               "misuse of aggregate function sum()").await;
        query_err_is(t, "SELECT * FROM t WHERE sum(x) + count(*) > 0",
               "misuse of aggregate function count()").await;
    });
}

#[test]
fn window_misuse_offset() {
    asupersync::test_utils::run_test(|| async {
        // A window function where one is not allowed (WHERE / GROUP BY / HAVING)
        // names the offending call: "misuse of window function NAME()".
        // bd-ttof2 FIX-FORWARD: NO " in <SQL> at offset N" suffix (stock never
        // puts the byte offset in the message).
        let t: &[&str] = &["CREATE TABLE t(x INT, g TEXT)"];
        query_err_is(t, "SELECT x FROM t WHERE row_number() OVER () = 1",
               "misuse of window function row_number()").await;
        query_err_is(t, "SELECT g FROM t GROUP BY row_number() OVER ()",
               "misuse of window function row_number()").await;
        query_err_is(t, "SELECT g FROM t GROUP BY g HAVING row_number() OVER () > 0",
               "misuse of window function row_number()").await;
        query_err_is(t, "SELECT x FROM t WHERE x > sum(x) OVER ()",
               "misuse of window function sum()").await;
    });
}

#[test]
fn nested_aggregate_misuse_offset() {
    asupersync::test_utils::run_test(|| async {
        // An aggregate nested inside another aggregate names the INNER call:
        // "misuse of aggregate function NAME()". bd-ttof2 FIX-FORWARD: NO
        // " in <SQL> at offset N" suffix (stock never puts the byte offset in
        // the message; verified vs sqlite3 CLI 3.46.1).
        let t: &[&str] = &["CREATE TABLE t(x INT, g TEXT)"];
        query_err_is(t, "SELECT sum(count(*)) FROM t",
               "misuse of aggregate function count()").await;
        query_err_is(t, "SELECT max(avg(x)) FROM t GROUP BY g",
               "misuse of aggregate function avg()").await;
        query_err_is(t, "SELECT g FROM t GROUP BY g HAVING sum(count(*)) > 0",
               "misuse of aggregate function count()").await;
        // a non-nested aggregate precedes the nested one: still names the inner
        query_err_is(t, "SELECT avg(x) + sum(count(*)) FROM t",
               "misuse of aggregate function count()").await;
        // triple nesting reports the innermost
        query_err_is(t, "SELECT sum(avg(count(*))) FROM t",
               "misuse of aggregate function count()").await;
    });
}

#[test]
fn function_resolution_offset() {
    asupersync::test_utils::run_test(|| async {
        // Function-resolution and function-modifier errors. bd-ttof2
        // FIX-FORWARD: NO " in <SQL> at offset N" suffix — stock reports the
        // bare message (verified vs sqlite3 CLI 3.46.1); the byte offset is
        // only reachable via a separate sqlite3_error_offset()-style accessor.
        let t: &[&str] = &["CREATE TABLE t(a INT, b INT)"];
        query_err_is(t, "SELECT nosuchfn(a) FROM t",
               "no such function: nosuchfn").await;
        // nested inner call resolves the same bare message
        query_err_is(t, "SELECT abs(nosuchfn(a)) FROM t",
               "no such function: nosuchfn").await;
        query_err_is(&[], "SELECT abs(1, 2)",
               "wrong number of arguments to function abs()").await;
        query_err_is(t, "SELECT a + abs(1, 2) FROM t",
               "wrong number of arguments to function abs()").await;
        query_err_is(t, "SELECT abs(a) FILTER (WHERE a > 0) FROM t",
               "FILTER may not be used with non-aggregate abs()").await;
        query_err_is(t, "SELECT abs(a ORDER BY a) FROM t",
               "ORDER BY may not be used with non-aggregate abs()").await;
        // DISTINCT-on-window: base message only (no offset), matching stock.
        query_err_is(t, "SELECT count(DISTINCT a) OVER () FROM t",
               "DISTINCT is not supported for window functions").await;
    });
}

#[test]
fn offset_suffix_on_parameterized_entrypoint() {
    asupersync::test_utils::run_test(|| async {
        // The parameterized entry points (query_with_params/execute_with_params)
        // surface the same bare stock error message as query/execute. A
        // function-resolution error raised through query_with_params reports the
        // stock message with NO offset suffix (bd-ttof2 FIX-FORWARD). (The
        // dispatch-level *misuse* validation is skipped on the parameterized fast
        // path — a separate pre-existing gap tracked in bd-ttof2 — so this
        // asserts a function-resolution error, which does fire.)
        let f = Connection::open(":memory:").await.unwrap();
        let _ = f.execute("CREATE TABLE t(x INT)").await;
        match f.query_with_params("SELECT nosuchfn(x) FROM t", &[]).await {
            Ok(_) => panic!("expected an error"),
            Err(e) => assert_eq!(e.to_string(), "no such function: nosuchfn"),
        }
    });
}

#[test]
fn distinct_aggregate_arg_count_precedence() {
    asupersync::test_utils::run_test(|| async {
        // For a DISTINCT aggregate with more than one argument, stock reports the
        // DISTINCT-single-argument error ONLY when the function legitimately
        // accepts that arity (group_concat takes 1 or 2). When the arity is
        // invalid for the function itself (count/sum/avg take 1), the arg-count
        // error takes precedence. bd-errmsg-parity-batch4-deqcb.
        let t: &[&str] = &["CREATE TABLE t(a INT, b INT)"];
        query_err_is(t, "SELECT count(DISTINCT a, b) FROM t",
               "wrong number of arguments to function count()").await;
        query_err_is(t, "SELECT sum(DISTINCT a, b) FROM t",
               "wrong number of arguments to function sum()").await;
        query_err_is(t, "SELECT avg(DISTINCT a, b) FROM t",
               "wrong number of arguments to function avg()").await;
        // group_concat legitimately takes 2 args, so the arg count is valid and
        // the DISTINCT-single-argument rule is what fires.
        query_err_is(t, "SELECT group_concat(DISTINCT a, b) FROM t",
               "DISTINCT aggregates must have exactly one argument").await;
        // control: a valid single-argument DISTINCT aggregate is accepted.
        let f = Connection::open(":memory:").await.unwrap();
        let _ = f.execute("CREATE TABLE t(a INT, b INT)").await;
        assert!(
            f.query("SELECT count(DISTINCT a) FROM t").await.is_ok(),
            "count(DISTINCT a) should be accepted"
        );
    });
}

#[test]
fn multicolumn_subquery_comparison_operand_row_value_misused() {
    asupersync::test_utils::run_test(|| async {
        // A multi-column subquery as the IMMEDIATE operand of a comparison operator
        // (=, <>, <, <=, >, >=, IS, IS NOT) against a scalar is "row value misused",
        // NOT the generic "sub-select returns N columns - expected 1". Verified vs
        // sqlite3 CLI 3.46.1 (bd-errmsg-parity-batch4-deqcb).
        let t: &[&str] = &["CREATE TABLE t(a INT)"];
        query_err_is(t, "SELECT * FROM t WHERE a = (SELECT 1, 2)", "row value misused").await;
        query_err_is(t, "SELECT * FROM t WHERE a < (SELECT 1, 2)", "row value misused").await;
        query_err_is(t, "SELECT * FROM t WHERE a IS (SELECT 1, 2)", "row value misused").await;
        query_err_is(t, "SELECT * FROM t WHERE a = (SELECT 1, 2, 3)", "row value misused").await;
        // symmetric: subquery on the left
        query_err_is(t, "SELECT * FROM t WHERE (SELECT 1, 2) = a", "row value misused").await;
        // the row-value diagnostic precedes name validation inside the subquery
        query_err_is(t, "SELECT * FROM t WHERE a = (SELECT nope, 2)", "row value misused").await;
        // still fires on the `=` operand when nested inside a larger boolean
        query_err_is(t, "SELECT * FROM t WHERE a = (SELECT 1, 2) AND a > 0", "row value misused")
            .await;

        // NEGATIVE: a multi-column subquery under a NON-comparison operator keeps
        // the generic arity error (the subquery is an arithmetic/list/bare operand,
        // not an immediate comparison operand).
        query_err_is(
            t,
            "SELECT * FROM t WHERE a = (1 + (SELECT 1, 2))",
            "sub-select returns 2 columns - expected 1",
        )
        .await;
        query_err_is(&[], "SELECT (SELECT 1, 2)", "sub-select returns 2 columns - expected 1").await;
        query_err_is(
            t,
            "SELECT * FROM t WHERE (SELECT 1, 2)",
            "sub-select returns 2 columns - expected 1",
        )
        .await;

        // NEGATIVE: a single-column subquery comparison is valid.
        let f = Connection::open(":memory:").await.unwrap();
        f.execute("CREATE TABLE t(a INT)").await.unwrap();
        f.execute("INSERT INTO t VALUES (1)").await.unwrap();
        assert!(
            f.query("SELECT * FROM t WHERE a = (SELECT 1)").await.is_ok(),
            "single-column subquery comparison is valid"
        );
    });
}
