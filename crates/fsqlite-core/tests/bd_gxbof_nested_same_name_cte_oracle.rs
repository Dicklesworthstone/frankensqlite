//! bd-gxbof: a CTE whose body defines a nested CTE of the same name reads
//! that nested CTE. `WITH c AS (WITH c AS (SELECT 1 AS x) SELECT x FROM c)
//! SELECT x FROM c` gives 1 in stock SQLite 3.54.1; fsqlite refused it with
//! "circular reference: c", because its anchor-position self-reference check
//! did not see the inner WITH's shadowing name, nor a derived table's.
//!
//! Stock resolves a CTE's body with the CTE's own name bound to the CTE, so a
//! reference the body's WITHs do not shadow is "circular reference" wherever
//! it sits: a scalar, EXISTS or IN subquery, or a nested CTE body. fsqlite
//! answered several of those from the table the CTE shadows (42 where stock
//! refuses). A recursive CTE's body with its own WITH lends that WITH to its
//! anchor and arms, as in stock; only a recursive arm's own FROM reference to
//! the name reads the recursive table.
//!
//! Every statement runs on bundled SQLite 3.54.1 and on fsqlite, on `:memory:`
//! and on a file, executed directly and prepared, with a table `c` holding 42.
//! Each statement's rows, or its error text, must equal stock's.

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
        Err(rusqlite::Error::SqliteFailure(_, Some(message))) => format!("error: {message}"),
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

const SETUP: &[&str] = &["CREATE TABLE c(x)", "INSERT INTO c VALUES (42)"];

/// `levels` WITHs of the same name nested inside each other over `SELECT 10
/// AS x`, each body reading the one inside it through `projection`.
fn same_name_nest(levels: usize, projection: &str) -> String {
    (0..levels).fold("SELECT 10 AS x".to_owned(), |inner, _| {
        format!("WITH c AS ({inner}) SELECT {projection} FROM c")
    })
}

/// Each query with the outcome stock must give, where pinning it keeps the
/// comparison from passing vacuously (both engines refusing alike, say).
fn queries() -> Vec<(String, Option<&'static str>)> {
    let fixed: &[(&str, Option<&'static str>)] = &[
        // The inner WITH's c shadows the outer name inside the outer body.
        (
            "WITH c AS (WITH c AS (SELECT 1 AS x) SELECT x FROM c) SELECT x FROM c",
            Some("rows=[[\"1\"]]"),
        ),
        (
            "WITH c AS (WITH c AS (SELECT 2 AS x) SELECT x + 10 AS x FROM c) SELECT x FROM c",
            Some("rows=[[\"12\"]]"),
        ),
        (
            "WITH c AS (WITH c AS (WITH c AS (SELECT 3 AS x) SELECT x FROM c) SELECT x FROM c) \
             SELECT x FROM c",
            Some("rows=[[\"3\"]]"),
        ),
        // A derived table's own WITH shadows it too.
        (
            "WITH c AS (SELECT * FROM (WITH c AS (SELECT 7 AS x) SELECT x FROM c)) \
             SELECT x FROM c",
            Some("rows=[[\"7\"]]"),
        ),
        // The innermost body reads the table c by its schema.
        (
            "WITH c AS (WITH c AS (SELECT x + 1 AS x FROM main.c) SELECT x FROM c) \
             SELECT x FROM c",
            Some("rows=[[\"43\"]]"),
        ),
        // Siblings. d's inner c reads itself, which is circular in stock.
        (
            "WITH c AS (SELECT 5 AS x), d AS (WITH c AS (SELECT x + 1 AS x FROM c) \
             SELECT x FROM c) SELECT x FROM d",
            Some("error: circular reference: c"),
        ),
        (
            "WITH c AS (SELECT x FROM d), d AS (WITH c AS (SELECT 1 AS x) SELECT x FROM c) \
             SELECT x FROM c",
            Some("rows=[[\"1\"]]"),
        ),
        (
            "WITH c AS (SELECT (WITH c AS (SELECT 9 AS x) SELECT x FROM c) AS x) SELECT x FROM c",
            Some("rows=[[\"9\"]]"),
        ),
        // Outer references beside a nest still read the outer c.
        (
            "WITH c AS (SELECT 5 AS x), d AS (WITH c AS (SELECT 1 AS x) SELECT x FROM c) \
             SELECT c.x, d.x FROM c, d",
            Some("rows=[[\"5\", \"1\"]]"),
        ),
        (
            "WITH c AS (SELECT 5 AS x) \
             SELECT (WITH c AS (SELECT 1 AS x) SELECT x FROM c) AS i, (SELECT x FROM c) AS o",
            Some("rows=[[\"1\", \"5\"]]"),
        ),
        // An inner body reading the outer c through a scalar subquery: under
        // another name it reads the outer c; under c it reads itself.
        (
            "WITH c AS (SELECT 5 AS x), \
             d AS (WITH e AS (SELECT (SELECT x FROM c) + 1 AS x) SELECT x FROM e) \
             SELECT x FROM d",
            Some("rows=[[\"6\"]]"),
        ),
        (
            "WITH c AS (SELECT 5 AS x), \
             d AS (WITH c AS (SELECT (SELECT x FROM c) + 1 AS x) SELECT x FROM c) \
             SELECT x FROM d",
            Some("error: circular reference: c"),
        ),
        (
            "WITH c AS (SELECT 5 AS x), \
             d AS (WITH c AS (SELECT 1 AS x) SELECT (SELECT x FROM c) AS x) SELECT x FROM d",
            Some("rows=[[\"1\"]]"),
        ),
        // A body reading itself anywhere is circular, not the table c.
        (
            "WITH c AS (SELECT x FROM c) SELECT x FROM c",
            Some("error: circular reference: c"),
        ),
        (
            "WITH c AS (WITH d AS (SELECT 1 AS x) SELECT x FROM c) SELECT x FROM c",
            Some("error: circular reference: c"),
        ),
        (
            "WITH c AS (SELECT * FROM (SELECT x FROM c)) SELECT x FROM c",
            Some("error: circular reference: c"),
        ),
        (
            "WITH c AS (SELECT (SELECT x FROM c) AS x) SELECT x FROM c",
            Some("error: circular reference: c"),
        ),
        (
            "WITH c AS (SELECT 1 AS x WHERE EXISTS (SELECT 1 FROM c)) SELECT x FROM c",
            Some("error: circular reference: c"),
        ),
        (
            "WITH c AS (SELECT 1 AS x WHERE 1 IN (SELECT x FROM c)) SELECT x FROM c",
            Some("error: circular reference: c"),
        ),
        (
            "WITH c AS (SELECT 1 AS x WHERE 1 IN c) SELECT x FROM c",
            Some("error: circular reference: c"),
        ),
        (
            "WITH b AS (WITH a AS (SELECT x FROM b) SELECT x FROM a) SELECT x FROM b",
            Some("error: circular reference: b"),
        ),
        (
            "WITH a AS (SELECT 1 AS x), b AS (WITH a AS (SELECT x FROM b) SELECT x FROM a) \
             SELECT x FROM b",
            Some("error: circular reference: b"),
        ),
        // A genuinely circular pair stays refused.
        (
            "WITH a AS (SELECT x FROM b), b AS (SELECT x FROM a) SELECT x FROM a",
            Some("error: circular reference: a"),
        ),
        // Control: the schema-qualified table is no self-reference.
        (
            "WITH c AS (SELECT (SELECT x FROM main.c) AS x) SELECT x FROM c",
            Some("rows=[[\"42\"]]"),
        ),
        // Recursive CTEs whose body has its own WITH: the anchor reads it,
        // and so do the arms, except a recursive arm's own FROM reference.
        (
            "WITH RECURSIVE c(x) AS (WITH c(x) AS (SELECT 1) SELECT x FROM c \
             UNION ALL SELECT x + 1 FROM c WHERE x < 3) SELECT x FROM c ORDER BY x",
            Some("rows=[[\"1\"], [\"2\"], [\"3\"]]"),
        ),
        (
            "WITH c(x) AS (WITH c(x) AS (SELECT 1) SELECT x FROM c \
             UNION ALL SELECT x + 1 FROM c WHERE x < 3) SELECT x FROM c ORDER BY x",
            Some("rows=[[\"1\"], [\"2\"], [\"3\"]]"),
        ),
        (
            "WITH RECURSIVE c(x) AS (WITH c(x) AS (SELECT 1) SELECT x FROM c \
             UNION SELECT x + 1 FROM c WHERE x < 3) SELECT x FROM c ORDER BY x",
            Some("rows=[[\"1\"], [\"2\"], [\"3\"]]"),
        ),
        (
            "WITH RECURSIVE c(x) AS (WITH c(x) AS (SELECT 1) SELECT x FROM c \
             UNION ALL SELECT x + 1 FROM c WHERE x < 3 ORDER BY 1 LIMIT 2) \
             SELECT x FROM c ORDER BY x",
            Some("rows=[[\"1\"], [\"2\"]]"),
        ),
        (
            "WITH RECURSIVE c(x) AS (WITH c(x) AS (SELECT 1) SELECT x FROM c) SELECT x FROM c",
            Some("rows=[[\"1\"]]"),
        ),
        (
            "WITH RECURSIVE c(x) AS (WITH c(x) AS (SELECT 1) SELECT x FROM c \
             UNION ALL SELECT x + 1 FROM c WHERE x < 3) SELECT sum(x) FROM c",
            Some("rows=[[\"6\"]]"),
        ),
        (
            "WITH RECURSIVE c(x) AS (WITH c(x) AS (SELECT 4) SELECT x FROM c \
             UNION ALL SELECT x + 1 FROM c WHERE x < 6) SELECT count(*), sum(x) FROM c",
            Some("rows=[[\"3\", \"15\"]]"),
        ),
        (
            "WITH RECURSIVE c(x) AS (WITH d(y) AS (SELECT 1) SELECT y FROM d \
             UNION ALL SELECT x + 1 FROM c WHERE x < 3) SELECT x FROM c ORDER BY x",
            Some("rows=[[\"1\"], [\"2\"], [\"3\"]]"),
        ),
        (
            "WITH RECURSIVE c(x) AS (WITH d(y) AS (SELECT 10) SELECT 1 \
             UNION ALL SELECT x + y FROM c, d WHERE x < 30) SELECT x FROM c ORDER BY x",
            Some("rows=[[\"1\"], [\"11\"], [\"21\"], [\"31\"]]"),
        ),
        (
            "WITH RECURSIVE c(x) AS (WITH d(y) AS (SELECT 2) SELECT y FROM d \
             UNION ALL SELECT x + (SELECT y FROM d) FROM c WHERE x < 8) SELECT x FROM c ORDER BY x",
            Some("rows=[[\"2\"], [\"4\"], [\"6\"], [\"8\"]]"),
        ),
        (
            "WITH RECURSIVE c(x) AS (WITH c(x) AS (SELECT 1), d(y) AS (SELECT 100) \
             SELECT x FROM c UNION ALL SELECT x + y FROM c, d WHERE x < 200) \
             SELECT x FROM c ORDER BY x",
            Some("rows=[[\"1\"], [\"101\"], [\"201\"]]"),
        ),
        // The inner c a sibling inner CTE reads stays the inner c in the arm.
        (
            "WITH RECURSIVE c(x) AS (WITH c(x) AS (SELECT 1), d(y) AS (SELECT x * 10 FROM c) \
             SELECT x FROM c UNION ALL SELECT x + y FROM c, d WHERE x < 30) \
             SELECT x FROM c ORDER BY x",
            Some("rows=[[\"1\"], [\"11\"], [\"21\"], [\"31\"]]"),
        ),
        (
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 3), \
             d AS (WITH c AS (SELECT 100 AS x) SELECT x FROM c) \
             SELECT x FROM c UNION ALL SELECT x FROM d",
            Some("rows=[[\"1\"], [\"2\"], [\"3\"], [\"100\"]]"),
        ),
        // A recursive CTE's anchor reading itself is circular; so is a body
        // that cannot recurse (EXCEPT).
        (
            "WITH RECURSIVE c(x) AS (SELECT (SELECT 1 FROM c) \
             UNION ALL SELECT x + 1 FROM c WHERE x < 3) SELECT x FROM c ORDER BY x",
            Some("error: circular reference: c"),
        ),
        (
            "WITH RECURSIVE c(x) AS (SELECT 1 EXCEPT SELECT x FROM c) SELECT x FROM c",
            Some("error: circular reference: c"),
        ),
        // Control: a plain recursive CTE.
        (
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 3) \
             SELECT x FROM c ORDER BY x",
            Some("rows=[[\"1\"], [\"2\"], [\"3\"]]"),
        ),
    ];
    let mut queries: Vec<(String, Option<&'static str>)> = fixed
        .iter()
        .map(|(sql, pinned)| ((*sql).to_owned(), *pinned))
        .collect();
    // Ten levels, as stock nests them (to 498 before "Recursion limit").
    queries.push((same_name_nest(10, "x"), Some("rows=[[\"10\"]]")));
    queries.push((same_name_nest(10, "x + 1 AS x"), Some("rows=[[\"20\"]]")));
    queries
}

#[test]
fn nested_same_name_ctes_match_stock() {
    let stock = rusqlite::Connection::open_in_memory().unwrap();
    for sql in SETUP {
        stock.execute_batch(sql).unwrap();
    }
    let queries = queries();
    let expected: Vec<String> = queries
        .iter()
        .map(|(sql, _)| stock_outcome(&stock, sql))
        .collect();
    // Not vacuous: stock gives each pinned outcome.
    for ((sql, pinned), stock_gave) in queries.iter().zip(&expected) {
        if let Some(pinned) = pinned {
            assert_eq!(stock_gave, pinned, "stock on `{sql}`");
        }
    }
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            for prepared in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = if file_backed {
                    dir.path().join("bd_gxbof.db").to_str().unwrap().to_owned()
                } else {
                    ":memory:".to_owned()
                };
                let conn = Connection::open(&path).await.unwrap();
                for sql in SETUP {
                    conn.execute(sql).await.unwrap();
                }
                for ((sql, _), expected) in queries.iter().zip(&expected) {
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
