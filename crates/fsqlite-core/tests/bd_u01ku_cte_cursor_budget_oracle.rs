//! bd-u01ku (item 1): stock SQLite refuses a CTE expansion once the
//! statement has assigned 1000 cursors ("VIEWs and/or subqueries nested too
//! deep"), and each reference to a CTE expands its body again. A chain of 999
//! CTEs read through a derived table, a chain of 500 CTEs whose bodies read
//! through a derived table, and nine levels of a CTE joined to itself are
//! refused by stock SQLite 3.54.1; fsqlite answered them.
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

/// `n` CTEs c0..c{n-1}, each reading the one before (through a derived table
/// when `derived`), read by `outer` (`{last}` names c{n-1}).
fn chain(n: usize, derived: bool, outer: &str) -> String {
    let mut ctes = vec!["c0 AS (SELECT 1 AS x)".to_owned()];
    for i in 1..n {
        let source = if derived {
            format!("(SELECT x FROM c{})", i - 1)
        } else {
            format!("c{}", i - 1)
        };
        ctes.push(format!("c{i} AS (SELECT x FROM {source})"));
    }
    format!(
        "WITH {} {}",
        ctes.join(", "),
        outer.replace("{last}", &format!("c{}", n - 1))
    )
}

/// c0 and k levels of `ci AS (SELECT a.x FROM c{i-1} AS a, c{i-1} AS b)`.
fn doubling(k: usize) -> String {
    let mut ctes = vec!["c0 AS (SELECT 1 AS x)".to_owned()];
    for i in 1..=k {
        ctes.push(format!(
            "c{i} AS (SELECT a.x FROM c{0} AS a, c{0} AS b)",
            i - 1
        ));
    }
    format!("WITH {} SELECT x FROM c{k}", ctes.join(", "))
}

const TOO_DEEP: &str = "error: VIEWs and/or subqueries nested too deep";
const ONE: &str = "rows=[[\"1\"]]";
const THROUGH_DERIVED: &str = "SELECT x FROM (SELECT x FROM {last})";

#[test]
fn cte_expansion_cursor_budget_matches_stock() {
    let statements: Vec<(String, &str)> = vec![
        // The bead's three shapes at stock's boundaries.
        (chain(998, false, THROUGH_DERIVED), ONE),
        (chain(999, false, THROUGH_DERIVED), TOO_DEEP),
        (chain(499, true, THROUGH_DERIVED), ONE),
        (chain(500, true, THROUGH_DERIVED), TOO_DEEP),
        (doubling(9), TOO_DEEP),
        (doubling(10), TOO_DEEP),
        // Negatives: 999 CTEs read directly assign 999 cursors (bd-6tmuj's
        // boundary does not move), a small doubling chain, a long recursive
        // CTE (its self-reference is not expanded).
        (chain(999, false, "SELECT x FROM {last}"), ONE),
        (doubling(5), ONE),
        (
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 2000) \
             SELECT count(*) FROM c"
                .to_owned(),
            "rows=[[\"2000\"]]",
        ),
    ];
    let stock = rusqlite::Connection::open_in_memory().unwrap();
    let expected: Vec<String> = statements
        .iter()
        .map(|(sql, _)| stock_outcome(&stock, sql))
        .collect();
    // Not vacuous: stock gives each pinned outcome.
    for ((sql, pinned), stock_gave) in statements.iter().zip(&expected) {
        assert_eq!(
            stock_gave,
            pinned,
            "stock on `{}...`",
            &sql[..sql.len().min(80)]
        );
    }
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            for prepared in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = if file_backed {
                    dir.path().join("bd_u01ku.db").to_str().unwrap().to_owned()
                } else {
                    ":memory:".to_owned()
                };
                let conn = Connection::open(&path).await.unwrap();
                for ((sql, _), expected) in statements.iter().zip(&expected) {
                    let got = fsqlite_outcome(&conn, sql, prepared).await;
                    assert_eq!(
                        &got,
                        expected,
                        "file_backed={file_backed} prepared={prepared}: `{}...`",
                        &sql[..sql.len().min(80)]
                    );
                }
            }
        }
    });
}
