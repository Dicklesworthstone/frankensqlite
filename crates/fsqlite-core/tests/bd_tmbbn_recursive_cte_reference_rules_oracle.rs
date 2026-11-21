//! bd-tmbbn: stock SQLite's rules for how a recursive CTE's body may read the
//! CTE (select.c withExpand). The recursive arms are the trailing compound
//! arms, joined by the body's last UNION or UNION ALL, whose own FROM items
//! name the CTE. Each names it once ("multiple references to recursive
//! table: c"), and nowhere else: a reference in a subquery of a recursive arm
//! is "multiple recursive references: c". A reference outside the recursive
//! arms, a derived table's in an arm with no FROM reference of its own
//! included, has no base case: "circular reference: c".
//!
//! fsqlite ran `SELECT x+1 FROM c WHERE x < (SELECT 3 FROM c LIMIT 1)` and
//! `SELECT x+1 FROM (SELECT x FROM c) WHERE x < 3` as recursive arms (1, 2,
//! 3), and refused `FROM c, c AS d` as "ambiguous column name: x".
//!
//! Every statement runs on bundled SQLite 3.54.1 and on fsqlite, on `:memory:`
//! and on a file, executed directly and prepared, with a table `c` holding 42
//! and a table `o` holding 2. Each statement's rows, or its error text, must
//! equal stock's, and is pinned to stock's so the comparison cannot pass
//! vacuously.

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

async fn fsqlite_outcome(conn: &Connection, sql: &str, prepared: bool) -> String {
    let result = if prepared {
        match conn.prepare(sql).await {
            Ok(statement) => statement.query().await,
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

const SETUP: &[&str] = &[
    "CREATE TABLE c(x)",
    "INSERT INTO c VALUES (42)",
    "CREATE TABLE o(y)",
    "INSERT INTO o VALUES (2)",
];

const ONE_TWO_THREE: &str = "rows=[[\"1\"], [\"2\"], [\"3\"]]";
const MULTIPLE_RECURSIVE: &str = "error: multiple recursive references: c";
const MULTIPLE_TABLE: &str = "error: multiple references to recursive table: c";
const CIRCULAR: &str = "error: circular reference: c";

/// Each query with the outcome stock gives.
const QUERIES: &[(&str, &str)] = &[
    // The bead's three shapes.
    (
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c \
         WHERE x < (SELECT 3 FROM c LIMIT 1)) SELECT x FROM c ORDER BY x",
        MULTIPLE_RECURSIVE,
    ),
    (
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c, c AS d WHERE x < 3) \
         SELECT x FROM c ORDER BY x",
        MULTIPLE_TABLE,
    ),
    (
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM (SELECT x FROM c) \
         WHERE x < 3) SELECT x FROM c ORDER BY x",
        CIRCULAR,
    ),
    // The same through a consumer the sum fast path could take.
    (
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c \
         WHERE x < (SELECT 3 FROM c LIMIT 1)) SELECT sum(x) FROM c",
        MULTIPLE_RECURSIVE,
    ),
    (
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c, c AS d WHERE x < 3) \
         SELECT sum(x) FROM c",
        MULTIPLE_TABLE,
    ),
    (
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM (SELECT x FROM c) \
         WHERE x < 3) SELECT sum(x) FROM c",
        CIRCULAR,
    ),
    // Without RECURSIVE the rules are the same.
    (
        "WITH c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM (SELECT x FROM c) WHERE x < 3) \
         SELECT x FROM c ORDER BY x",
        CIRCULAR,
    ),
    // A second reference in EXISTS, IN, a join's ON, the result list, the
    // body's ORDER BY or its LIMIT.
    (
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c \
         WHERE EXISTS (SELECT 1 FROM c) AND x < 3) SELECT x FROM c ORDER BY x",
        MULTIPLE_RECURSIVE,
    ),
    (
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x IN c AND x < 3) \
         SELECT x FROM c ORDER BY x",
        MULTIPLE_RECURSIVE,
    ),
    (
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT c.x+1 FROM c \
         JOIN o ON o.y > (SELECT count(*) FROM c) WHERE c.x < 3) SELECT x FROM c ORDER BY x",
        MULTIPLE_RECURSIVE,
    ),
    (
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT (SELECT max(x) FROM c) + x FROM c \
         WHERE x < 3) SELECT x FROM c ORDER BY x",
        MULTIPLE_RECURSIVE,
    ),
    (
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 3 \
         ORDER BY (SELECT count(*) FROM c)) SELECT x FROM c",
        MULTIPLE_RECURSIVE,
    ),
    (
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 3 \
         LIMIT (SELECT 2 FROM c)) SELECT x FROM c",
        MULTIPLE_RECURSIVE,
    ),
    // The recursive arms are the trailing ones of the last operator.
    (
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 3 \
         UNION ALL SELECT x+10 FROM c WHERE x < 2) SELECT x FROM c ORDER BY x",
        "rows=[[\"1\"], [\"2\"], [\"3\"], [\"11\"]]",
    ),
    (
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 3 \
         UNION ALL SELECT 5) SELECT x FROM c ORDER BY x",
        CIRCULAR,
    ),
    (
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION SELECT x+1 FROM c WHERE x < 3 \
         UNION ALL SELECT x+10 FROM c WHERE x < 2) SELECT x FROM c ORDER BY x",
        CIRCULAR,
    ),
    (
        "WITH RECURSIVE c(x) AS (SELECT 1 INTERSECT SELECT x+1 FROM c) SELECT x FROM c",
        CIRCULAR,
    ),
    // A schema-qualified name is never the CTE (bd-mm6ec), and the refusal
    // names the CTE beside one.
    (
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c \
         WHERE x < (SELECT x FROM main.c LIMIT 1) - 39) SELECT x FROM c ORDER BY x",
        ONE_TWO_THREE,
    ),
    (
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c, c AS d WHERE x < 3) \
         SELECT c.x FROM c, main.c AS m",
        MULTIPLE_TABLE,
    ),
    // Negatives: one reference, a subquery naming another table or a nested
    // WITH, a sibling reading the CTE, a LEFT JOIN, VALUES as the anchor, an
    // inner CTE of the name in a subquery, and an unused CTE.
    (
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 3) \
         SELECT x FROM c ORDER BY x",
        ONE_TWO_THREE,
    ),
    (
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c \
         WHERE x < (SELECT y FROM o) + 1) SELECT x FROM c ORDER BY x",
        ONE_TWO_THREE,
    ),
    (
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c \
         WHERE x < (WITH d AS (SELECT 3 AS z) SELECT z FROM d)) SELECT x FROM c ORDER BY x",
        ONE_TWO_THREE,
    ),
    (
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 3), \
         d(y) AS (SELECT x FROM c) SELECT y FROM d ORDER BY y",
        ONE_TWO_THREE,
    ),
    (
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 3), \
         d(y) AS (SELECT 1 UNION ALL SELECT y+1 FROM d, c WHERE y < 2) \
         SELECT y FROM d ORDER BY y",
        "rows=[[\"1\"], [\"2\"], [\"2\"], [\"2\"]]",
    ),
    (
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT c.x+1 FROM c \
         LEFT JOIN o ON o.y = c.x WHERE c.x < 3) SELECT x FROM c ORDER BY x",
        ONE_TWO_THREE,
    ),
    (
        "WITH RECURSIVE c(x) AS (VALUES (1) UNION ALL SELECT x+1 FROM c WHERE x < 3) \
         SELECT x FROM c ORDER BY x",
        ONE_TWO_THREE,
    ),
    (
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c \
         WHERE x < (WITH c(z) AS (SELECT 3) SELECT z FROM c)) SELECT x FROM c ORDER BY x",
        ONE_TWO_THREE,
    ),
    (
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 3) \
         SELECT x FROM c WHERE x IN (SELECT x FROM c) ORDER BY x",
        ONE_TWO_THREE,
    ),
    (
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c, c AS d) SELECT 9",
        "rows=[[\"9\"]]",
    ),
];

#[test]
fn recursive_cte_reference_rules_match_stock() {
    let stock = rusqlite::Connection::open_in_memory().unwrap();
    for sql in SETUP {
        stock.execute_batch(sql).unwrap();
    }
    let expected: Vec<String> = QUERIES
        .iter()
        .map(|(sql, _)| stock_outcome(&stock, sql))
        .collect();
    // Not vacuous: stock gives each pinned outcome.
    for ((sql, pinned), stock_gave) in QUERIES.iter().zip(&expected) {
        assert_eq!(stock_gave, pinned, "stock on `{sql}`");
    }
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            for prepared in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = if file_backed {
                    dir.path().join("bd_tmbbn.db").to_str().unwrap().to_owned()
                } else {
                    ":memory:".to_owned()
                };
                let conn = Connection::open(&path).await.unwrap();
                for sql in SETUP {
                    conn.execute(sql).await.unwrap();
                }
                for ((sql, _), expected) in QUERIES.iter().zip(&expected) {
                    let got = fsqlite_outcome(&conn, sql, prepared).await;
                    assert_eq!(
                        &got, expected,
                        "file_backed={file_backed} prepared={prepared}: `{sql}`"
                    );
                }
            }
        }
    });
}
