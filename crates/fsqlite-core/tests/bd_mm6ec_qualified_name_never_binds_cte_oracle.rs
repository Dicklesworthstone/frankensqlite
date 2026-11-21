//! bd-mm6ec: SQLite looks a relation name up among the CTEs in scope only
//! when it is unqualified and is not the target of an INSERT, UPDATE or
//! DELETE. With a table `c` holding 42,
//!
//! ```sql
//! WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c
//!   WHERE x < (SELECT x FROM main.c) - 39) SELECT x FROM c ORDER BY x;
//! ```
//!
//! gives 1, 2, 3 in stock SQLite 3.54.1: `main.c` is the table, so 42 - 39
//! bounds the recursion. fsqlite gave 1. It installs a materialized CTE in
//! front of a catalog that codegen searches by bare name, so the recursive
//! arm's `main.c` read its own working table. Outside a CTE's own body it
//! refused the shape ("not implemented: qualified relation main.c is not
//! supported while a same-name CTE is active"), and the WITH of an UPDATE,
//! DELETE or INSERT, which that check never saw, wrote wrong rows: an UPDATE
//! whose `IN (SELECT x FROM main.c)` read the CTE changed 7 where stock
//! changed 42, and a DELETE or UPDATE whose target shares a CTE's name failed
//! with "write cursor targets read-only materialized root".
//!
//! Every statement runs on bundled SQLite 3.54.1 and on fsqlite, on `:memory:`
//! and on a file, executed directly and prepared. Each statement's rows, or
//! its error text, must equal stock's; most are also pinned to the outcome
//! stock gives, so a comparison cannot pass vacuously.

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

/// `sql` on fsqlite, run directly (`Connection::query`) or prepared first: a
/// prepared query is queried, a prepared DML statement executed. A statement
/// with RETURNING always runs directly, as a prepared DML statement returns
/// no rows.
async fn fsqlite_outcome(conn: &Connection, sql: &str, prepared: bool) -> String {
    let query = sql.starts_with("SELECT") || (sql.starts_with("WITH") && !is_dml(sql));
    let result = if prepared && (query || !sql.contains("RETURNING")) {
        match conn.prepare(sql).await {
            Ok(statement) if query => statement.query().await,
            Ok(statement) => statement.execute().await.map(|_| Vec::new()),
            Err(error) => Err(error),
        }
    } else {
        conn.query(sql).await
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

/// Whether a statement starting with WITH is an INSERT, UPDATE or DELETE: its
/// WITH clause closes before the statement's own keyword.
fn is_dml(sql: &str) -> bool {
    [") INSERT ", ") UPDATE ", ") DELETE "]
        .iter()
        .any(|keyword| sql.contains(keyword))
}

/// A script: setup statements, then statements whose outcomes are compared
/// in order, each with the outcome stock must give where it is pinned.
struct Script {
    name: &'static str,
    setup: &'static [&'static str],
    statements: &'static [(&'static str, Option<&'static str>)],
}

const MAIN_ONLY: Script = Script {
    name: "main_only",
    setup: &[
        "CREATE TABLE c(x)",
        "INSERT INTO c VALUES (42)",
        "CREATE TABLE w(a, b)",
        "INSERT INTO w VALUES (1, 2)",
        "CREATE VIEW v AS SELECT x FROM main.c",
    ],
    statements: &[
        // The bead's shape: the recursion is bounded by the table, 42 - 39.
        (
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c \
             WHERE x < (SELECT x FROM main.c) - 39) SELECT x FROM c ORDER BY x",
            Some("rows=[[\"1\"], [\"2\"], [\"3\"]]"),
        ),
        (
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c \
             WHERE x < (SELECT x FROM main.c) - 39) SELECT sum(x) FROM c",
            Some("rows=[[\"6\"]]"),
        ),
        (
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c \
             WHERE x < (SELECT x FROM main.c) - 39) SELECT x FROM c LIMIT 2",
            Some("rows=[[\"1\"], [\"2\"]]"),
        ),
        (
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + (SELECT x FROM main.c) \
             FROM c WHERE x < 100) SELECT x FROM c",
            Some("rows=[[\"1\"], [\"43\"], [\"85\"], [\"127\"]]"),
        ),
        // A qualified name in a join inside the recursive part.
        (
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT c.x + 1 FROM c \
             JOIN main.c AS m ON m.x > c.x + 38 WHERE c.x < 10) SELECT x FROM c ORDER BY x",
            Some("rows=[[\"1\"], [\"2\"], [\"3\"], [\"4\"]]"),
        ),
        // Unaliased, the table and the CTE expose the same name c, so `c.x`
        // is ambiguous. (In a recursive arm stock says so and fsqlite runs
        // it; that arm-ambiguity residual is outside this bead.)
        (
            "WITH c(x) AS (SELECT 1) SELECT c.x FROM c, main.c",
            Some("error: ambiguous column name: c.x"),
        ),
        // The body's own WITH of the name feeds the anchor (bd-gxbof); the
        // arm still reads the recursive table and `main.c` the table.
        (
            "WITH RECURSIVE c(x) AS (WITH c(x) AS (SELECT 1) SELECT x FROM c \
             UNION ALL SELECT x + 1 FROM c WHERE x < (SELECT x FROM main.c) - 39) \
             SELECT x FROM c ORDER BY x",
            Some("rows=[[\"1\"], [\"2\"], [\"3\"]]"),
        ),
        (
            "WITH RECURSIVE w(z) AS (SELECT 1 UNION ALL SELECT z + 1 FROM w \
             WHERE z < (SELECT b FROM main.w) + 1) SELECT z FROM w",
            Some("rows=[[\"1\"], [\"2\"], [\"3\"]]"),
        ),
        // Non-recursive CTEs: the consumer, a sibling body, the CTE's body.
        (
            "WITH c(x) AS (SELECT 7) SELECT x FROM main.c",
            Some("rows=[[\"42\"]]"),
        ),
        (
            "WITH c(x) AS (SELECT 7) SELECT a.x, b.x FROM c AS a, main.c AS b",
            Some("rows=[[\"7\", \"42\"]]"),
        ),
        (
            "WITH c(x) AS (SELECT 7) SELECT c.x, (SELECT x FROM main.c) FROM c",
            Some("rows=[[\"7\", \"42\"]]"),
        ),
        (
            "WITH c(x) AS (SELECT 7) SELECT * FROM c JOIN main.c AS m ON m.x > c.x",
            Some("rows=[[\"7\", \"42\"]]"),
        ),
        (
            "WITH c(x) AS (SELECT 7) SELECT c.* FROM c, main.c AS m",
            Some("rows=[[\"7\"]]"),
        ),
        (
            "WITH c(x) AS (SELECT 7) SELECT m.* FROM c, main.c AS m",
            Some("rows=[[\"42\"]]"),
        ),
        (
            "WITH c(x) AS (SELECT 7) SELECT x FROM main.c WHERE x > (SELECT x FROM c)",
            Some("rows=[[\"42\"]]"),
        ),
        (
            "WITH c(x) AS (SELECT 7) SELECT x FROM main.c UNION ALL SELECT x FROM c ORDER BY 1",
            Some("rows=[[\"7\"], [\"42\"]]"),
        ),
        (
            "WITH c(x) AS (SELECT 7) \
             SELECT count(*), sum(x) FROM (SELECT x FROM main.c UNION ALL SELECT x FROM c)",
            Some("rows=[[\"2\", \"49\"]]"),
        ),
        (
            "WITH c AS MATERIALIZED (SELECT 7 AS x) SELECT c.x, m.x FROM c, main.c AS m",
            Some("rows=[[\"7\", \"42\"]]"),
        ),
        (
            "WITH c AS NOT MATERIALIZED (SELECT 7 AS x) SELECT c.x, m.x FROM c, main.c AS m",
            Some("rows=[[\"7\", \"42\"]]"),
        ),
        (
            "WITH c(x) AS (SELECT 7), d(y) AS (SELECT x FROM main.c) SELECT y FROM d",
            Some("rows=[[\"42\"]]"),
        ),
        (
            "WITH c(x) AS (SELECT 7), d(y) AS (SELECT c.x + m.x FROM c, main.c AS m) \
             SELECT y FROM d",
            Some("rows=[[\"49\"]]"),
        ),
        (
            "WITH c(x) AS (SELECT (SELECT x FROM main.c) + 1) SELECT x FROM c",
            Some("rows=[[\"43\"]]"),
        ),
        (
            "WITH c(x) AS (SELECT 7) \
             SELECT x FROM c WHERE EXISTS (SELECT 1 FROM main.c WHERE x = 42)",
            Some("rows=[[\"7\"]]"),
        ),
        // `IN main.c` reads the table: 42 is in it and 7 is not.
        (
            "WITH c(x) AS (SELECT 42) SELECT x FROM c WHERE x IN main.c",
            Some("rows=[[\"42\"]]"),
        ),
        (
            "WITH c(x) AS (SELECT 7) SELECT x FROM c WHERE x IN main.c",
            Some("rows=[]"),
        ),
        // A WITH nested in a subquery.
        (
            "SELECT (WITH c(x) AS (SELECT 7) SELECT x FROM main.c)",
            Some("rows=[[\"42\"]]"),
        ),
        (
            "SELECT * FROM (WITH c(x) AS (SELECT 7) SELECT x FROM main.c)",
            Some("rows=[[\"42\"]]"),
        ),
        // The table's own columns, not the CTE's.
        (
            "WITH w(z) AS (SELECT 9) SELECT a, b FROM main.w",
            Some("rows=[[\"1\", \"2\"]]"),
        ),
        (
            "WITH w(z) AS (SELECT 9) SELECT z FROM main.w",
            Some("error: no such column: z"),
        ),
        (
            "WITH w(z) AS (SELECT 9) SELECT w.z, m.a FROM w, main.w AS m",
            Some("rows=[[\"9\", \"1\"]]"),
        ),
        // A view's body is bound in its own scope.
        (
            "WITH c(x) AS (SELECT 7) SELECT x FROM v",
            Some("rows=[[\"42\"]]"),
        ),
        // Negatives: an unqualified c still names the CTE ...
        (
            "WITH c(x) AS (SELECT 7) SELECT (SELECT x FROM c), (SELECT x FROM main.c)",
            Some("rows=[[\"7\", \"42\"]]"),
        ),
        (
            "WITH c(x) AS (SELECT 7) SELECT x FROM c",
            Some("rows=[[\"7\"]]"),
        ),
        // ... a qualified name that does not exist is stock's error ...
        (
            "WITH RECURSIVE d(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM d \
             WHERE x < (SELECT x FROM main.d)) SELECT x FROM d",
            Some("error: no such table: main.d"),
        ),
        (
            "WITH d(x) AS (SELECT 1) SELECT * FROM main.d",
            Some("error: no such table: main.d"),
        ),
        (
            "WITH c(x) AS (SELECT 1) SELECT * FROM temp.c",
            Some("error: no such table: temp.c"),
        ),
        (
            "WITH c(x) AS (SELECT (SELECT x FROM temp.c)) SELECT x FROM c",
            Some("error: no such table: temp.c"),
        ),
        // ... and the CTE's own refusals keep its name.
        (
            "WITH c(x) AS (SELECT x FROM c) SELECT * FROM main.c",
            Some("rows=[[\"42\"]]"),
        ),
        (
            "WITH c(x) AS (SELECT x FROM c) SELECT c.x FROM c, main.c AS m",
            Some("error: circular reference: c"),
        ),
        (
            "WITH c(x, y) AS (SELECT 1) SELECT * FROM c, main.c AS m",
            Some("error: table c has 1 values for 2 columns"),
        ),
    ],
};

const WITH_TEMP: Script = Script {
    name: "with_temp",
    setup: &[
        "CREATE TABLE c(x)",
        "INSERT INTO c VALUES (42)",
        "CREATE TEMP TABLE c(x)",
        "INSERT INTO temp.c VALUES (45)",
    ],
    statements: &[
        (
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c \
             WHERE x < (SELECT x FROM temp.c) - 40) SELECT x FROM c ORDER BY x",
            Some("rows=[[\"1\"], [\"2\"], [\"3\"], [\"4\"], [\"5\"]]"),
        ),
        (
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c \
             WHERE x < (SELECT x FROM main.c) - 39) SELECT x FROM c ORDER BY x",
            Some("rows=[[\"1\"], [\"2\"], [\"3\"]]"),
        ),
        (
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT c.x + 1 FROM c \
             JOIN temp.c AS t ON t.x > c.x + 41) SELECT x FROM c ORDER BY x",
            Some("rows=[[\"1\"], [\"2\"], [\"3\"], [\"4\"]]"),
        ),
        (
            "WITH c(x) AS (SELECT 7) \
             SELECT (SELECT x FROM c), (SELECT x FROM main.c), (SELECT x FROM temp.c)",
            Some("rows=[[\"7\", \"42\", \"45\"]]"),
        ),
        (
            "WITH c(x) AS (SELECT 7) SELECT x FROM temp.c",
            Some("rows=[[\"45\"]]"),
        ),
        (
            "WITH c(x) AS (SELECT 7) SELECT m.x, t.x FROM main.c AS m, temp.c AS t",
            Some("rows=[[\"42\", \"45\"]]"),
        ),
        (
            "WITH c(x) AS (SELECT 7) SELECT c.x, t.x, m.x FROM c, temp.c AS t, main.c AS m",
            Some("rows=[[\"7\", \"45\", \"42\"]]"),
        ),
        (
            "WITH c(x) AS (SELECT (SELECT x FROM temp.c) + 1) SELECT x FROM c",
            Some("rows=[[\"46\"]]"),
        ),
        // Control: without a CTE, TEMP shadows MAIN.
        ("SELECT x FROM c", Some("rows=[[\"45\"]]")),
    ],
};

const WITH_ATTACHED: Script = Script {
    name: "with_attached",
    setup: &[
        "CREATE TABLE c(x)",
        "INSERT INTO c VALUES (42)",
        "ATTACH ':memory:' AS aux",
        "CREATE TABLE aux.c(x)",
        "INSERT INTO aux.c VALUES (99)",
    ],
    statements: &[
        (
            "WITH c(x) AS (SELECT 7) SELECT x FROM aux.c",
            Some("rows=[[\"99\"]]"),
        ),
        (
            "WITH c(x) AS (SELECT 7) SELECT c.x, a.x FROM c, aux.c AS a",
            Some("rows=[[\"7\", \"99\"]]"),
        ),
        (
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c \
             WHERE x < (SELECT x FROM aux.c) - 96) SELECT x FROM c ORDER BY x",
            Some("rows=[[\"1\"], [\"2\"], [\"3\"]]"),
        ),
        // Not here: main.c and aux.c in one query ("queries mixing attached
        // schemas with main/temp or unqualified tables are not yet
        // supported"), and a CTE beside a scalar subquery on an attached
        // table ("JOIN on non-SELECT core"), which fsqlite refuses whatever
        // the names.
    ],
};

/// The WITH of an UPDATE, DELETE or INSERT: each write is followed by a read
/// of the table it changed.
const DML: Script = Script {
    name: "dml",
    setup: &[
        "CREATE TABLE c(x)",
        "INSERT INTO c VALUES (42), (7), (1)",
        "CREATE TABLE t(y)",
        "INSERT INTO t VALUES (1), (7), (42)",
        "CREATE TABLE u(k, v)",
        "INSERT INTO u VALUES (42, 'a'), (5, 'b')",
    ],
    statements: &[
        // Reading the CTE (7) instead of the table would change 7, not 42.
        (
            "WITH c(x) AS (SELECT 7) UPDATE t SET y = y + 100 \
             WHERE y IN (SELECT x FROM main.c WHERE x > 10)",
            Some("rows=[]"),
        ),
        (
            "SELECT y FROM t ORDER BY y",
            Some("rows=[[\"1\"], [\"7\"], [\"142\"]]"),
        ),
        (
            "WITH c(x) AS (SELECT 1) DELETE FROM t \
             WHERE y IN (SELECT x FROM c) OR y = (SELECT max(x) + 100 FROM main.c)",
            Some("rows=[]"),
        ),
        ("SELECT y FROM t ORDER BY y", Some("rows=[[\"7\"]]")),
        (
            "WITH c(x) AS (SELECT 5) UPDATE t SET y = (SELECT max(x) FROM main.c) WHERE y = 7",
            Some("rows=[]"),
        ),
        ("SELECT y FROM t ORDER BY y", Some("rows=[[\"42\"]]")),
        // A target named like the CTE is the table, qualified or not, while
        // the unqualified reads name the CTE.
        (
            "WITH c(x) AS (SELECT 7) DELETE FROM main.c WHERE x IN (SELECT x FROM c)",
            Some("rows=[]"),
        ),
        (
            "SELECT x FROM c ORDER BY x",
            Some("rows=[[\"1\"], [\"42\"]]"),
        ),
        (
            "WITH c(x) AS (SELECT 1) UPDATE c SET x = x + 100 WHERE x IN (SELECT x FROM c)",
            Some("rows=[]"),
        ),
        (
            "SELECT x FROM c ORDER BY x",
            Some("rows=[[\"42\"], [\"101\"]]"),
        ),
        (
            "WITH c(x) AS (SELECT 42) DELETE FROM c WHERE x IN (SELECT x FROM c)",
            Some("rows=[]"),
        ),
        ("SELECT x FROM c ORDER BY x", Some("rows=[[\"101\"]]")),
        (
            "WITH c(x) AS (SELECT 5) INSERT INTO c SELECT x FROM c",
            Some("rows=[]"),
        ),
        // Reading the CTE as main.c would insert nothing (5 > 5 is false).
        (
            "WITH c(x) AS (SELECT 5) INSERT INTO c \
             SELECT x + 1 FROM main.c WHERE x > (SELECT x FROM c)",
            Some("rows=[]"),
        ),
        (
            "SELECT x FROM c ORDER BY x",
            Some("rows=[[\"5\"], [\"101\"], [\"102\"]]"),
        ),
        (
            "WITH c(x) AS (SELECT 101) UPDATE main.c SET x = x + 1000 \
             WHERE x IN (SELECT x FROM c) RETURNING x",
            Some("rows=[[\"1101\"]]"),
        ),
        (
            "WITH c(x) AS (SELECT 0) INSERT INTO main.c SELECT x FROM c",
            Some("rows=[]"),
        ),
        (
            "SELECT x FROM c ORDER BY x",
            Some("rows=[[\"0\"], [\"5\"], [\"102\"], [\"1101\"]]"),
        ),
        // UPDATE ... FROM, RETURNING and VALUES subqueries.
        (
            "WITH c(x) AS (SELECT 5) UPDATE u SET v = v || '!' FROM main.c AS m WHERE u.k = m.x",
            Some("rows=[]"),
        ),
        (
            "SELECT k, v FROM u ORDER BY k",
            Some("rows=[[\"5\", \"'b!'\"], [\"42\", \"'a'\"]]"),
        ),
        (
            "WITH c(x) AS (SELECT 42) UPDATE u SET v = v || '?' FROM c WHERE u.k = c.x",
            Some("rows=[]"),
        ),
        (
            "SELECT k, v FROM u ORDER BY k",
            Some("rows=[[\"5\", \"'b!'\"], [\"42\", \"'a?'\"]]"),
        ),
        (
            "WITH c(x) AS (SELECT 5) DELETE FROM u WHERE k = 5 \
             RETURNING k, (SELECT max(x) FROM main.c), (SELECT max(x) FROM c)",
            Some("rows=[[\"5\", \"1101\", \"5\"]]"),
        ),
        (
            "WITH c(x) AS (SELECT 5) \
             INSERT INTO u VALUES ((SELECT max(x) FROM main.c), (SELECT max(x) FROM c)) \
             RETURNING k, v",
            Some("rows=[[\"1101\", \"5\"]]"),
        ),
        (
            "SELECT k, v FROM u ORDER BY k",
            Some("rows=[[\"42\", \"'a?'\"], [\"1101\", \"5\"]]"),
        ),
    ],
};

fn run_script(script: &Script) {
    let stock = rusqlite::Connection::open_in_memory().unwrap();
    for sql in script.setup {
        stock.execute_batch(sql).unwrap();
    }
    let expected: Vec<String> = script
        .statements
        .iter()
        .map(|(sql, _)| stock_outcome(&stock, sql))
        .collect();
    // Not vacuous: stock gives each pinned outcome.
    for ((sql, pinned), stock_gave) in script.statements.iter().zip(&expected) {
        if let Some(pinned) = pinned {
            assert_eq!(stock_gave, pinned, "{}: stock on `{sql}`", script.name);
        }
    }
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            for prepared in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = if file_backed {
                    dir.path().join("bd_mm6ec.db").to_str().unwrap().to_owned()
                } else {
                    ":memory:".to_owned()
                };
                let conn = Connection::open(&path).await.unwrap();
                for sql in script.setup {
                    conn.execute(sql).await.unwrap();
                }
                for ((sql, _), expected) in script.statements.iter().zip(&expected) {
                    let got = fsqlite_outcome(&conn, sql, prepared).await;
                    assert_eq!(
                        &got, expected,
                        "{}: file_backed={file_backed} prepared={prepared}: `{sql}`",
                        script.name
                    );
                }
            }
        }
    });
}

#[test]
fn qualified_names_read_the_table_beside_a_same_name_cte() {
    run_script(&MAIN_ONLY);
}

#[test]
fn qualified_names_read_temp_and_main_beside_a_same_name_cte() {
    run_script(&WITH_TEMP);
}

#[test]
fn attached_qualified_names_read_the_attached_table_beside_a_same_name_cte() {
    run_script(&WITH_ATTACHED);
}

#[test]
fn dml_targets_and_qualified_reads_never_bind_a_cte() {
    run_script(&DML);
}
