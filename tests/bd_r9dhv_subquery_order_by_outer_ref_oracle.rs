#![recursion_limit = "512"]

//! bd-r9dhv: SQLite 3.53 (the bundled oracle) resolves a name in a subquery's
//! ORDER BY or GROUP BY against the enclosing queries too, after the
//! subquery's own result aliases and FROM columns, so
//! `(SELECT y FROM h ORDER BY abs(y - x.k) LIMIT 1)` is a correlated subquery.
//! fsqlite followed SQLite 3.51 and earlier, which report "no such column"
//! for such a name. LIMIT and OFFSET still see no outer names, and a compound
//! subquery's ORDER BY still has to match a result column.
//!
//! Scalar, EXISTS, IN and FROM-subquery forms and writes are compared with
//! rusqlite (rows, or failure), ad hoc and prepared, in memory and
//! file-backed.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("blob{b:?}"),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("blob{b:?}"),
    }
}

const SETUP: &[&str] = &[
    "CREATE TABLE g(k INTEGER PRIMARY KEY, grp, n)",
    "INSERT INTO g VALUES (1,1,1),(2,1,2),(3,2,1),(4,2,2),(6,3,1),(7,3,3)",
    "CREATE TABLE h(y INTEGER PRIMARY KEY, z TEXT)",
    "INSERT INTO h VALUES (1,'a'),(5,'B'),(9,'c'),(12,'D')",
    "CREATE TABLE m(id INTEGER PRIMARY KEY, gk INTEGER, w)",
    "CREATE INDEX m_gk ON m(gk)",
    "INSERT INTO m VALUES (1,1,10),(2,1,20),(3,2,30),(4,4,40),(5,4,50),(6,7,60)",
];

const QUERIES: &[&str] = &[
    // Scalar subqueries: qualified, unqualified and expression outer names.
    "SELECT x.k, (SELECT y FROM h ORDER BY abs(y - x.k), y LIMIT 1) FROM g AS x ORDER BY x.k",
    "SELECT k, (SELECT y FROM h ORDER BY abs(y - k), y LIMIT 1) FROM g ORDER BY k",
    "SELECT g.k, (SELECT z FROM h ORDER BY abs(y - g.n * 4) DESC, z COLLATE NOCASE LIMIT 1) \
     FROM g ORDER BY g.k",
    "SELECT x.k, (SELECT y FROM h ORDER BY abs(y - x.k), y LIMIT 1 OFFSET 1) FROM g AS x ORDER BY x.k",
    "SELECT x.k, (SELECT (SELECT y FROM h ORDER BY abs(y - x.k), y LIMIT 1) FROM g AS g2 \
     WHERE g2.k = 1) FROM g AS x ORDER BY x.k",
    // EXISTS / NOT EXISTS, directly and through a FROM subquery.
    "SELECT x.k FROM g AS x WHERE EXISTS (SELECT 1 FROM (SELECT k FROM g \
     ORDER BY abs(k - x.k), k LIMIT 2) AS s WHERE s.k = x.k + 1) ORDER BY x.k",
    "SELECT x.k FROM g AS x WHERE NOT EXISTS (SELECT 1 FROM (SELECT k FROM g \
     ORDER BY abs(k - x.k), k LIMIT 2) AS s WHERE s.k = x.k + 1) ORDER BY x.k",
    "SELECT x.k FROM g AS x WHERE EXISTS (SELECT 1 FROM h WHERE y > x.k \
     ORDER BY abs(y - x.k) LIMIT 1) ORDER BY x.k",
    // IN / NOT IN.
    "SELECT x.k FROM g AS x WHERE x.k + 1 IN (SELECT k FROM g ORDER BY abs(k - x.k), k LIMIT 2) \
     ORDER BY x.k",
    "SELECT x.k FROM g AS x WHERE x.k NOT IN (SELECT y FROM h ORDER BY abs(y - x.k), y LIMIT 1) \
     ORDER BY x.k",
    // FROM subqueries inside a scalar subquery.
    "SELECT x.k, (SELECT max(s.k) FROM (SELECT k FROM g ORDER BY abs(k - x.k), k LIMIT 2) AS s) \
     FROM g AS x ORDER BY x.k",
    "SELECT x.k, (SELECT count(*) FROM (SELECT k FROM g ORDER BY abs(k - x.k)) AS s \
     WHERE s.k > x.k) FROM g AS x ORDER BY x.k",
    // GROUP BY.
    "SELECT x.k, (SELECT count(*) FROM (SELECT y FROM h GROUP BY y / (x.k + 1))) FROM g AS x \
     ORDER BY x.k",
    "SELECT x.grp, (SELECT count(*) FROM (SELECT y FROM h GROUP BY y / (x.grp + 1) \
     HAVING count(*) > 0)) FROM g AS x GROUP BY x.grp ORDER BY x.grp",
    // Correlated index probes, the shapes the compiled subquery lanes admit.
    "SELECT x.k, (SELECT m.w FROM m WHERE m.gk = x.k ORDER BY abs(m.w - x.n * 15), m.w LIMIT 1) \
     FROM g AS x ORDER BY x.k",
    "SELECT x.k FROM g AS x WHERE EXISTS (SELECT 1 FROM m WHERE m.gk = x.k \
     ORDER BY m.w * x.n LIMIT 1) ORDER BY x.k",
    "SELECT x.k, (SELECT count(*) FROM m WHERE m.gk = x.k ORDER BY x.n) FROM g AS x ORDER BY x.k",
    "SELECT x.k FROM g AS x WHERE x.k IN (SELECT m.gk FROM m WHERE m.gk = x.k \
     ORDER BY m.w - x.n LIMIT 1) ORDER BY x.k",
    // A term that is just an outer column is a value, not a column ordinal.
    "SELECT x.k, (SELECT y FROM h ORDER BY x.k, y DESC LIMIT 1) FROM g AS x ORDER BY x.k",
    "SELECT k, (SELECT y FROM h ORDER BY k, y DESC LIMIT 1) FROM g ORDER BY k",
    "SELECT x.k FROM g AS x WHERE (SELECT y FROM h ORDER BY x.n DESC, y LIMIT 1) = 1 ORDER BY x.k",
    "SELECT x.k, (SELECT count(*) FROM (SELECT y FROM h GROUP BY x.k)) FROM g AS x ORDER BY x.k",
    // The subquery's own aliases and columns still come first.
    "SELECT k, (SELECT y AS k FROM h ORDER BY k DESC LIMIT 1) FROM g ORDER BY k",
    "SELECT k, (SELECT k FROM g AS g2 ORDER BY k DESC LIMIT 1) FROM g ORDER BY k",
    "SELECT x.k, (SELECT y FROM h ORDER BY (SELECT abs(y - x.k)), y LIMIT 1) FROM g AS x \
     ORDER BY x.k",
    // Still errors.
    "SELECT x.k, (SELECT y FROM h LIMIT x.n) FROM g AS x",
    "SELECT x.k, (SELECT y FROM h LIMIT 1 OFFSET x.n) FROM g AS x",
    "SELECT x.k, (SELECT y FROM h UNION SELECT y + 1 FROM h ORDER BY abs(y - x.k) LIMIT 1) \
     FROM g AS x",
    "SELECT k FROM g ORDER BY x.k",
];

/// Writes whose subqueries order by an outer column, checked through the rows
/// they leave behind.
const WRITES: &[(&str, &str)] = &[
    (
        "UPDATE g SET n = (SELECT y FROM h ORDER BY abs(y - g.k), y LIMIT 1) WHERE k > 2",
        "SELECT k, n FROM g ORDER BY k",
    ),
    (
        "INSERT INTO h(y, z) SELECT k + 100, (SELECT z FROM h ORDER BY abs(y - g.k), y LIMIT 1) \
         FROM g",
        "SELECT y, z FROM h ORDER BY y",
    ),
    (
        "DELETE FROM g WHERE k IN (SELECT y FROM h ORDER BY abs(y - g.k), y LIMIT 1)",
        "SELECT k, n FROM g ORDER BY k",
    ),
];

fn rows_r(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let ncol = stmt.column_count();
    let rows = stmt
        .query_map([], |row| {
            Ok((0..ncol)
                .map(|i| tag_r(&row.get::<_, rusqlite::types::Value>(i).unwrap()))
                .collect())
        })
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<Vec<String>>, _>>()
        .map_err(|e| e.to_string())?;
    Ok(rows)
}

fn same_outcome(
    franken: Result<Vec<fsqlite_core::connection::Row>, fsqlite_error::FrankenError>,
    stock: &Result<Vec<Vec<String>>, String>,
    what: &str,
) {
    match (franken, stock) {
        (Ok(rows), Ok(expected)) => {
            let rows: Vec<Vec<String>> = rows
                .iter()
                .map(|row| row.values().iter().map(tag_f).collect())
                .collect();
            assert_eq!(&rows, expected, "{what}");
        }
        // rusqlite appends " in <sql> at offset <n>" to a prepare error.
        (Err(error), Err(expected)) => assert!(
            error
                .to_string()
                .contains(expected.split(" in ").next().unwrap_or(expected)),
            "{what}: franken `{error}`, stock `{expected}`"
        ),
        (franken, stock) => panic!("{what}: franken {franken:?}, stock {stock:?}"),
    }
}

async fn open_pair(target: &str) -> (Connection, rusqlite::Connection) {
    let f = Connection::open(target).await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for sql in SETUP {
        f.execute(sql).await.unwrap();
        r.execute(sql, []).unwrap();
    }
    (f, r)
}

#[test]
fn subquery_order_by_and_group_by_see_outer_columns() {
    for file_backed in [false, true] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let target = |name: &str| {
                if file_backed {
                    dir.path().join(name).to_string_lossy().into_owned()
                } else {
                    ":memory:".to_owned()
                }
            };
            let (f, r) = open_pair(&target("bd_r9dhv.db")).await;
            for sql in QUERIES {
                let stock = rows_r(&r, sql);
                same_outcome(f.query(sql).await, &stock, &format!("query `{sql}`"));
                let prepared = match f.prepare(sql).await {
                    Ok(stmt) => stmt.query().await,
                    Err(error) => Err(error),
                };
                same_outcome(prepared, &stock, &format!("prepared `{sql}`"));
            }

            for (index, (write, check)) in WRITES.iter().enumerate() {
                for prepared in [false, true] {
                    let name = format!("bd_r9dhv_w{index}_{prepared}.db");
                    let (f, r) = open_pair(&target(&name)).await;
                    r.execute(write, []).unwrap();
                    let outcome = if prepared {
                        match f.prepare(write).await {
                            Ok(stmt) => stmt.execute().await.map(|_| ()),
                            Err(error) => Err(error),
                        }
                    } else {
                        f.execute(write).await.map(|_| ())
                    };
                    outcome.unwrap_or_else(|e| panic!("`{write}` (prepared {prepared}): {e}"));
                    same_outcome(
                        f.query(check).await,
                        &rows_r(&r, check),
                        &format!("`{write}` (prepared {prepared}) then `{check}`"),
                    );
                }
            }
        });
    }
}
