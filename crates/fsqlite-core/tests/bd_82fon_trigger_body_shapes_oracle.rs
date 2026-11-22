//! bd-82fon: trigger-body and recursive-CTE shapes fsqlite refused where
//! stock SQLite 3.54.1 answers.
//!
//! Shape 1: stock refuses a schema-qualified INSERT, UPDATE or DELETE target
//! only in the body of a trigger outside the TEMP schema (triggerStepAllocate
//! checks the schema sqlite3BeginTrigger placed the trigger in). A TEMP
//! trigger, whether created with TEMP, named `temp.x`, or unqualified on a TEMP
//! table, may write `main.t`. fsqlite refused every qualified target at parse.
//!
//! Shape 2: a recursive arm `FROM (c JOIN o)` was "not implemented" in
//! fsqlite; stock's grammar makes a leading parenthesized join the FROM list.
//!
//! Shape 3: under double-quoted strings, `INSERT INTO t VALUES ("hello")` in
//! a trigger body inserts the string; fsqlite failed with "no such column:
//! hello" when the trigger fired.
//!
//! Every statement runs in order on bundled SQLite 3.54.1 and on fsqlite, on
//! `:memory:` and on a file, executed directly and prepared; each one's rows,
//! or its error text, must equal stock's, and is pinned to stock's.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn render_fsqlite(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "NULL".into(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("x{b:02x?}"),
    }
}

fn render_stock(value: &rusqlite::types::Value) -> String {
    match value {
        rusqlite::types::Value::Null => "NULL".into(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f:?}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("x{b:02x?}"),
    }
}

fn stock_outcome(conn: &rusqlite::Connection, sql: &str) -> String {
    let result = (|| -> rusqlite::Result<Vec<Vec<String>>> {
        let mut statement = conn.prepare(sql)?;
        let columns = statement.column_count();
        let mut rows = Vec::new();
        let mut cursor = statement.query([])?;
        while let Some(row) = cursor.next()? {
            rows.push(
                (0..columns)
                    .map(|i| render_stock(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                    .collect(),
            );
        }
        Ok(rows)
    })();
    match result {
        Ok(rows) => format!("rows={rows:?}"),
        Err(
            rusqlite::Error::SqliteFailure(_, Some(message))
            | rusqlite::Error::SqlInputError { msg: message, .. },
        ) => format!("error: {message}"),
        Err(error) => format!("error: {error}"),
    }
}

/// `sql` on fsqlite, run directly or prepared first: a SELECT is queried,
/// any other statement executed. fsqlite's `prepare()` takes SELECT, INSERT,
/// UPDATE and DELETE (and PRAGMA), so a CREATE runs directly in both modes.
async fn fsqlite_outcome(conn: &Connection, sql: &str, prepared: bool) -> String {
    let query = sql.starts_with("SELECT") || sql.starts_with("WITH");
    let preparable = query
        || ["INSERT", "UPDATE", "DELETE"]
            .iter()
            .any(|verb| sql.starts_with(verb));
    let result = match (prepared && preparable, query) {
        (true, _) => match conn.prepare(sql).await {
            Ok(statement) if query => statement.query().await,
            Ok(statement) => statement.execute().await.map(|_| Vec::new()),
            Err(error) => Err(error),
        },
        (false, true) => conn.query(sql).await,
        (false, false) => conn.execute(sql).await.map(|_| Vec::new()),
    };
    match result {
        Ok(rows) => {
            let rows: Vec<Vec<String>> = rows
                .iter()
                .map(|row| row.values().iter().map(render_fsqlite).collect())
                .collect();
            format!("rows={rows:?}")
        }
        Err(error) => format!("error: {error}"),
    }
}

/// Runs `script` in order on stock (checking each pinned outcome, so the
/// keeper is not vacuous) and then on fsqlite in all four modes; every
/// difference is listed before the test fails.
fn assert_script_matches_stock(script: &[(&str, &str)]) {
    let stock = rusqlite::Connection::open_in_memory().unwrap();
    let expected: Vec<String> = script
        .iter()
        .map(|(sql, _)| stock_outcome(&stock, sql))
        .collect();
    for ((sql, pinned), stock_gave) in script.iter().zip(&expected) {
        assert_eq!(stock_gave, pinned, "stock on `{sql}`");
    }
    asupersync::test_utils::run_test(|| async {
        let mut differences = Vec::new();
        for file_backed in [false, true] {
            for prepared in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = if file_backed {
                    dir.path().join("bd_82fon.db").to_str().unwrap().to_owned()
                } else {
                    ":memory:".to_owned()
                };
                let conn = Connection::open(&path).await.unwrap();
                for ((sql, _), expected) in script.iter().zip(&expected) {
                    let got = fsqlite_outcome(&conn, sql, prepared).await;
                    if &got != expected {
                        differences.push(format!(
                            "file_backed={file_backed} prepared={prepared}: `{sql}`\n  \
                             fsqlite: {got}\n  stock:   {expected}"
                        ));
                    }
                }
            }
        }
        assert!(
            differences.is_empty(),
            "{} differences from stock:\n{}",
            differences.len(),
            differences.join("\n")
        );
    });
}

const THREE: &str = "rows=[[\"1\"], [\"2\"], [\"3\"]]";

/// Shape 2: a recursive arm whose FROM clause opens with a parenthesized
/// join holding the recursive reference, which stock's grammar makes the
/// FROM list itself. fsqlite's join executor refused the parenthesized
/// source ("not implemented").
#[test]
fn a_recursive_arm_may_open_with_a_parenthesized_join_like_stock() {
    let recursive = |arm: &str| {
        format!("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT {arm}) SELECT x FROM c")
    };
    let statements = [
        recursive("x + 1 FROM (c JOIN o) WHERE x < 3"),
        recursive("x + 1 FROM (o JOIN c) WHERE x < 3"),
        recursive("x + 1 FROM ((c JOIN o)) WHERE x < 3"),
        recursive("x + z FROM (c JOIN o ON x < 3)"),
        recursive("x + 1 FROM (c JOIN o) JOIN o AS q WHERE x < 3"),
        recursive("x + y FROM (c LEFT JOIN p ON y > x) WHERE x < 30"),
        // Negatives: the recursive reference again in a subquery of an ON
        // clause; two references inside the parentheses. (A parenthesized
        // join that does not open the clause, `FROM o JOIN (c JOIN p)`, is
        // stock's "circular reference: c" and still "not implemented" here: a
        // separate defect, reported on bd-82fon.)
        recursive("x + 1 FROM o JOIN c ON (SELECT count(*) FROM c) > 0 WHERE x < 3"),
        recursive("x + 1 FROM (c JOIN c AS d) WHERE x < 3"),
    ];
    let pinned = [
        THREE,
        THREE,
        THREE,
        THREE,
        THREE,
        "rows=[[\"1\"], [\"11\"], [\"21\"], [\"31\"], [\"NULL\"]]",
        "error: multiple recursive references: c",
        "error: multiple references to recursive table: c",
    ];
    let mut script = vec![
        ("CREATE TABLE o(z)", OK),
        ("INSERT INTO o VALUES (1)", OK),
        ("CREATE TABLE p(y)", OK),
        ("INSERT INTO p VALUES (10), (20)", OK),
        // Outside a recursive arm.
        (
            "SELECT z, y FROM (o JOIN p) ORDER BY y",
            "rows=[[\"1\", \"10\"], [\"1\", \"20\"]]",
        ),
        (
            "SELECT z, y FROM (o LEFT JOIN p ON y > 15) ORDER BY y",
            "rows=[[\"1\", \"20\"]]",
        ),
    ];
    script.extend(
        statements
            .iter()
            .map(String::as_str)
            .zip(pinned.iter().copied()),
    );
    assert_script_matches_stock(&script);
}

const OK: &str = "rows=[]";
const QUALIFIED: &str = "error: qualified table names are not allowed on INSERT, UPDATE, \
                         and DELETE statements within triggers";

#[test]
fn a_temp_trigger_may_write_a_qualified_table_like_stock() {
    assert_script_matches_stock(&[
        ("CREATE TABLE t(a DEFAULT 1)", OK),
        ("CREATE TABLE u(b)", OK),
        ("CREATE TEMP TABLE tu(c)", OK),
        // TEMP keyword: INSERT, UPDATE and DELETE on main.t.
        (
            "CREATE TEMP TRIGGER t1 AFTER INSERT ON u BEGIN \
             INSERT INTO main.t VALUES (new.b); \
             UPDATE main.t SET a = a + 10 WHERE a = 2; \
             DELETE FROM main.t WHERE a = 3; END",
            OK,
        ),
        ("INSERT INTO u VALUES (1)", OK),
        ("INSERT INTO u VALUES (2)", OK),
        ("INSERT INTO u VALUES (3)", OK),
        ("SELECT a FROM t ORDER BY a", "rows=[[\"1\"], [\"12\"]]"),
        // A `temp.` trigger name.
        (
            "CREATE TRIGGER temp.t2 AFTER INSERT ON u BEGIN \
             INSERT INTO main.t VALUES (100); END",
            OK,
        ),
        ("INSERT INTO u VALUES (4)", OK),
        (
            "SELECT a FROM t ORDER BY a",
            "rows=[[\"1\"], [\"4\"], [\"12\"], [\"100\"]]",
        ),
        // An unqualified trigger on a TEMP table is a TEMP trigger.
        (
            "CREATE TRIGGER t3 AFTER INSERT ON tu BEGIN \
             INSERT INTO main.t VALUES (new.c); END",
            OK,
        ),
        ("INSERT INTO tu VALUES (7)", OK),
        (
            "SELECT a FROM t ORDER BY a",
            "rows=[[\"1\"], [\"4\"], [\"7\"], [\"12\"], [\"100\"]]",
        ),
        // Negatives: outside TEMP each qualified target is refused, with the
        // trigger named, its table qualified, or a TEMP target.
        (
            "CREATE TRIGGER n1 AFTER INSERT ON u BEGIN INSERT INTO main.t VALUES (5); END",
            QUALIFIED,
        ),
        (
            "CREATE TRIGGER n2 AFTER INSERT ON u BEGIN UPDATE main.t SET a = 1; END",
            QUALIFIED,
        ),
        (
            "CREATE TRIGGER n3 AFTER INSERT ON u BEGIN DELETE FROM main.t; END",
            QUALIFIED,
        ),
        (
            "CREATE TRIGGER main.n4 AFTER INSERT ON u BEGIN INSERT INTO main.t VALUES (5); END",
            QUALIFIED,
        ),
        (
            "CREATE TRIGGER n5 AFTER INSERT ON main.u BEGIN INSERT INTO temp.tu VALUES (5); END",
            QUALIFIED,
        ),
        // IF NOT EXISTS over an existing trigger stops before the body check.
        (
            "CREATE TRIGGER n6 AFTER INSERT ON u BEGIN INSERT INTO t VALUES (5); END",
            OK,
        ),
        (
            "CREATE TRIGGER IF NOT EXISTS n6 AFTER INSERT ON u BEGIN \
             INSERT INTO main.t VALUES (5); END",
            OK,
        ),
        // Errors stock reports first: the trigger's table, then a clause
        // inside the same step.
        (
            "CREATE TRIGGER n7 AFTER INSERT ON nosuch BEGIN INSERT INTO main.t VALUES (5); END",
            "error: no such table: main.nosuch",
        ),
        (
            "CREATE TRIGGER n8 AFTER INSERT ON u BEGIN UPDATE main.t INDEXED BY nx SET a = 1; END",
            "error: the INDEXED BY clause is not allowed on UPDATE or DELETE statements \
             within triggers",
        ),
        (
            "SELECT name FROM sqlite_master WHERE type = 'trigger' ORDER BY name",
            "rows=[[\"'n6'\"]]",
        ),
    ]);
}

/// Shape 3's trigger: a double-quoted VALUES value (top level and inside an
/// expression) beside a double-quoted name that is a column of a SELECT step,
/// or of a subquery in VALUES.
const DQS_TRIGGER: &str = "CREATE TRIGGER tq AFTER INSERT ON o BEGIN \
     INSERT INTO t VALUES (\"hello\"), (new.z), (\"x\" || new.z); \
     INSERT INTO t SELECT \"z\" FROM o WHERE z = new.z; \
     INSERT INTO t VALUES ((SELECT \"z\" FROM o WHERE z = new.z) + 100); END";

#[test]
fn a_double_quoted_value_in_a_trigger_body_is_a_string_like_stock() {
    assert_script_matches_stock(&[
        ("CREATE TABLE t(a)", OK),
        ("CREATE TABLE o(z)", OK),
        ("CREATE TABLE p(w)", OK),
        (DQS_TRIGGER, OK),
        ("INSERT INTO o VALUES (2)", OK),
        (
            "SELECT a, typeof(a) FROM t ORDER BY rowid",
            "rows=[[\"'hello'\", \"'text'\"], [\"2\", \"'integer'\"], [\"'x2'\", \"'text'\"], \
             [\"2\", \"'integer'\"], [\"102\", \"'integer'\"]]",
        ),
        // The schema keeps the text as written.
        (
            "SELECT sql = 'CREATE TRIGGER tq AFTER INSERT ON o BEGIN \
             INSERT INTO t VALUES (\"hello\"), (new.z), (\"x\" || new.z); \
             INSERT INTO t SELECT \"z\" FROM o WHERE z = new.z; \
             INSERT INTO t VALUES ((SELECT \"z\" FROM o WHERE z = new.z) + 100); END' \
             FROM sqlite_master WHERE name = 'tq'",
            "rows=[[\"1\"]]",
        ),
        // Negative: an unquoted name is a column, and there is none.
        (
            "CREATE TRIGGER tb AFTER INSERT ON p BEGIN INSERT INTO t VALUES (hello); END",
            OK,
        ),
        ("INSERT INTO p VALUES (1)", "error: no such column: hello"),
        ("SELECT count(*) FROM p", "rows=[[\"0\"]]"),
    ]);
}

/// The trigger a reopened file reads back from its schema fires the same way.
#[test]
fn a_reopened_trigger_body_keeps_its_double_quoted_strings_like_stock() {
    const READ: &str = "SELECT a, typeof(a) FROM t ORDER BY rowid";
    let dir = tempfile::tempdir().unwrap();
    let stock_path = dir.path().join("stock.db");
    {
        let stock = rusqlite::Connection::open(&stock_path).unwrap();
        stock
            .execute_batch(&format!(
                "CREATE TABLE t(a); CREATE TABLE o(z); {DQS_TRIGGER};"
            ))
            .unwrap();
    }
    let stock = rusqlite::Connection::open(&stock_path).unwrap();
    stock.execute_batch("INSERT INTO o VALUES (3)").unwrap();
    let expected = stock_outcome(&stock, READ);
    assert_eq!(
        expected,
        "rows=[[\"'hello'\", \"'text'\"], [\"3\", \"'integer'\"], [\"'x3'\", \"'text'\"], \
         [\"3\", \"'integer'\"], [\"103\", \"'integer'\"]]",
        "stock"
    );
    asupersync::test_utils::run_test(|| async {
        for prepared in [false, true] {
            let path = dir.path().join(format!("fsqlite_{prepared}.db"));
            let path = path.to_str().unwrap();
            {
                let conn = Connection::open(path).await.unwrap();
                for sql in ["CREATE TABLE t(a)", "CREATE TABLE o(z)", DQS_TRIGGER] {
                    conn.execute(sql).await.unwrap();
                }
            }
            let conn = Connection::open(path).await.unwrap();
            assert_eq!(
                fsqlite_outcome(&conn, "INSERT INTO o VALUES (3)", prepared).await,
                OK,
                "prepared={prepared}"
            );
            assert_eq!(
                fsqlite_outcome(&conn, READ, prepared).await,
                expected,
                "prepared={prepared}"
            );
        }
    });
}
