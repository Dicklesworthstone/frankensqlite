//! bd-x5eqm: three divergences from stock SQLite 3.54.1.
//!
//! 1. A prepared INSERT, UPDATE or DELETE on a view failed with "no such
//!    table: vt" where the same statement run directly fires the view's
//!    INSTEAD OF trigger. The prepare path compiled the statement as table
//!    bytecode, which finds no table of the view's name; it now defers such a
//!    statement to the dispatcher. A view without an INSTEAD OF trigger still
//!    gives stock's "cannot modify vn because it is a view".
//! 2. `WITH d(x) AS (SELECT 7) SELECT (SELECT x FROM d), (SELECT y FROM
//!    aux.w)` gave "not implemented: JOIN on non-SELECT core": a WITH whose
//!    body reads an attached table runs on the memdb fallback, whose join
//!    evaluator refuses a FROM-less body. Stock answers 7|9.
//! 3. In a recursive CTE's recursive arm, `FROM c, main.c` exposes the name
//!    c twice, so `c.x` is ambiguous; stock refuses the statement, fsqlite
//!    ran it. The qualified-column binder took a CTE to have unknown columns,
//!    so `c.x` bound to the table alone; it now knows a CTE's columns when
//!    its column list or result aliases name them. A CTE without `x` still
//!    leaves `c.x` to the table.
//!
//! Every statement runs on bundled SQLite 3.54.1 and on fsqlite, on `:memory:`
//! and on a file, executed directly and prepared. Each statement's rows, or
//! its error text, must equal stock's, and is pinned to stock's.

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

/// `sql` on fsqlite, run directly or prepared first: a prepared query is
/// queried, a prepared INSERT, UPDATE or DELETE executed.
async fn fsqlite_outcome(conn: &Connection, sql: &str, prepared: bool) -> String {
    let dml = ["INSERT ", "UPDATE ", "DELETE "]
        .iter()
        .any(|keyword| sql.starts_with(keyword) || sql.contains(&format!(") {keyword}")));
    let query = !dml;
    let result = if prepared {
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

struct Script {
    name: &'static str,
    setup: &'static [&'static str],
    statements: &'static [(&'static str, &'static str)],
}

const NO_ROWS: &str = "rows=[]";
const NOT_A_TABLE: &str = "error: cannot modify vn because it is a view";
const ONE_TO_THREE: &str = "rows=[[\"1\"], [\"2\"], [\"3\"]]";
const AMBIGUOUS_C_X: &str = "error: ambiguous column name: c.x";

const VIEWS: Script = Script {
    name: "views",
    setup: &[
        "CREATE TABLE t(a, b)",
        "CREATE VIEW vt AS SELECT a, b FROM t",
        "CREATE VIEW vn AS SELECT a FROM t",
        "CREATE TRIGGER ti INSTEAD OF INSERT ON vt BEGIN INSERT INTO t VALUES (NEW.a, NEW.b); END",
        "CREATE TRIGGER tu INSTEAD OF UPDATE ON vt \
         BEGIN UPDATE t SET b = NEW.b WHERE a = OLD.a; END",
        "CREATE TRIGGER td INSTEAD OF DELETE ON vt BEGIN DELETE FROM t WHERE a = OLD.a; END",
    ],
    statements: &[
        ("INSERT INTO vt VALUES (3, 3), (4, 4)", NO_ROWS),
        // An INSTEAD OF trigger's writes are not the statement's changes.
        ("SELECT changes()", "rows=[[\"0\"]]"),
        ("UPDATE vt SET b = 9 WHERE a = 3", NO_ROWS),
        (
            "SELECT a, b FROM t ORDER BY a",
            "rows=[[\"3\", \"9\"], [\"4\", \"4\"]]",
        ),
        ("DELETE FROM vt WHERE a = 4", NO_ROWS),
        ("SELECT a, b FROM t ORDER BY a", "rows=[[\"3\", \"9\"]]"),
        // Negatives: a view without INSTEAD OF triggers is still refused.
        ("INSERT INTO vn VALUES (1)", NOT_A_TABLE),
        ("UPDATE vn SET a = 2", NOT_A_TABLE),
        ("DELETE FROM vn", NOT_A_TABLE),
        ("SELECT count(*) FROM t", "rows=[[\"1\"]]"),
    ],
};

const FROMLESS_WITH_ATTACHED: Script = Script {
    name: "fromless_with_attached",
    setup: &[
        "ATTACH ':memory:' AS aux",
        "CREATE TABLE aux.w(y)",
        "INSERT INTO aux.w VALUES (9)",
        "CREATE TABLE c(x)",
        "INSERT INTO c VALUES (100)",
        "CREATE TABLE t2(a, b)",
    ],
    statements: &[
        // The bead's shape, and without the name collision.
        (
            "WITH c AS (SELECT 7 AS x) SELECT (SELECT x FROM c), (SELECT y FROM aux.w)",
            "rows=[[\"7\", \"9\"]]",
        ),
        (
            "WITH d(x) AS (SELECT 7) SELECT (SELECT x FROM d), (SELECT y FROM aux.w)",
            "rows=[[\"7\", \"9\"]]",
        ),
        // A VALUES body, an expression, an aggregate, a WHERE that keeps no
        // row, and an INSERT source of the same shape.
        (
            "WITH d(x) AS (SELECT 7) VALUES ((SELECT x FROM d), (SELECT y FROM aux.w)), (1, 2)",
            "rows=[[\"7\", \"9\"], [\"1\", \"2\"]]",
        ),
        (
            "WITH d(x) AS (SELECT 7) SELECT (SELECT max(y) FROM aux.w) + (SELECT x FROM d)",
            "rows=[[\"16\"]]",
        ),
        (
            "WITH d(x) AS (SELECT 7) SELECT count(*), (SELECT y FROM aux.w)",
            "rows=[[\"1\", \"9\"]]",
        ),
        (
            "WITH d(x) AS (SELECT 7) SELECT (SELECT y FROM aux.w) WHERE (SELECT x FROM d) > 10",
            NO_ROWS,
        ),
        (
            "WITH d(x) AS (SELECT 7) INSERT INTO t2 SELECT (SELECT x FROM d), (SELECT y FROM aux.w)",
            NO_ROWS,
        ),
        ("SELECT a, b FROM t2", "rows=[[\"7\", \"9\"]]"),
        // Control: a FROM clause, as before.
        (
            "WITH c(x) AS (SELECT 7) SELECT (SELECT y FROM aux.w), x FROM c",
            "rows=[[\"9\", \"7\"]]",
        ),
    ],
};

const SAME_NAME_CTE_AND_TABLE: Script = Script {
    name: "same_name_cte_and_table",
    setup: &["CREATE TABLE c(x)", "INSERT INTO c VALUES (100)"],
    statements: &[
        // The bead's shape; without a column list; the table first; a join;
        // the reference outside the result; under a false WHERE (stock
        // refuses at prepare).
        (
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL \
             SELECT c.x + 1 FROM c, main.c WHERE c.x < 3) SELECT x FROM c",
            AMBIGUOUS_C_X,
        ),
        (
            "WITH RECURSIVE c AS (SELECT 1 AS x UNION ALL \
             SELECT c.x + 1 FROM c, main.c WHERE c.x < 3) SELECT x FROM c",
            AMBIGUOUS_C_X,
        ),
        (
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL \
             SELECT c.x + 1 FROM main.c, c WHERE c.x < 3) SELECT x FROM c",
            AMBIGUOUS_C_X,
        ),
        (
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL \
             SELECT c.x + 1 FROM c JOIN main.c ON 1 WHERE c.x < 3) SELECT x FROM c",
            AMBIGUOUS_C_X,
        ),
        (
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL \
             SELECT 2 FROM c, main.c WHERE c.x < 3) SELECT x FROM c",
            AMBIGUOUS_C_X,
        ),
        (
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL \
             SELECT c.x + 1 FROM c, main.c WHERE c.x < 3 AND 0) SELECT x FROM c",
            AMBIGUOUS_C_X,
        ),
        (
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL \
             SELECT x + 1 FROM c, main.c WHERE x < 3) SELECT x FROM c",
            "error: ambiguous column name: x",
        ),
        // Negatives: an alias on either side gives each item its own name.
        (
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL \
             SELECT c.x + 1 FROM c, main.c AS m WHERE c.x < 3) SELECT x FROM c",
            ONE_TO_THREE,
        ),
        (
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL \
             SELECT c.x + 1 FROM main.c AS k, c WHERE c.x < 3) SELECT x FROM c",
            ONE_TO_THREE,
        ),
        (
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL \
             SELECT c.x + m.x FROM c, main.c AS m WHERE c.x < 3) SELECT x FROM c",
            "rows=[[\"1\"], [\"101\"]]",
        ),
        (
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL \
             SELECT k.x + 1 FROM c AS k, main.c WHERE k.x < 3) SELECT x FROM c",
            ONE_TO_THREE,
        ),
        // Outside a recursive arm: a CTE whose columns include x shares the
        // qualifier (an alias names them; an UPDATE's FROM); one without x
        // leaves it to the table. (Not here, separate gaps: a CTE without x in
        // a subquery, `WITH c(y) AS (SELECT 5) SELECT (SELECT c.x FROM c) FROM
        // main.c`, where stock reads the enclosing table's x and fsqlite says
        // "no such column: c.x", as for any inner item without the column;
        // and `WITH c(y) AS (SELECT 5) UPDATE main.c SET x = c.y FROM c`,
        // which stock runs (x = 5) and fsqlite refuses as "no such column:
        // c.y", the UPDATE resolving c. against its target.)
        (
            "WITH c AS (SELECT 5 AS y, 6 AS x) SELECT c.x FROM main.c, c",
            AMBIGUOUS_C_X,
        ),
        (
            "WITH c(y) AS (SELECT 5) SELECT c.x FROM c, main.c",
            "rows=[[\"100\"]]",
        ),
        (
            "WITH c(x) AS (SELECT 5) UPDATE main.c SET x = c.x FROM c",
            AMBIGUOUS_C_X,
        ),
        ("SELECT x FROM main.c", "rows=[[\"100\"]]"),
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
        assert_eq!(stock_gave, pinned, "{}: stock on `{sql}`", script.name);
    }
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            for prepared in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = if file_backed {
                    dir.path().join("bd_x5eqm.db").to_str().unwrap().to_owned()
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
fn prepared_view_dml_fires_instead_of_triggers_like_stock() {
    run_script(&VIEWS);
}

#[test]
fn fromless_with_body_reading_an_attached_table_answers_like_stock() {
    run_script(&FROMLESS_WITH_ATTACHED);
}

#[test]
fn a_qualifier_over_a_cte_and_a_same_named_table_binds_like_stock() {
    run_script(&SAME_NAME_CTE_AND_TABLE);
}
