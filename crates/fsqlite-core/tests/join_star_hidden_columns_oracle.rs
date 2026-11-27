//! `*` and `<source>.*` over a join of table functions, against stock.
//!
//! `join_star_matches_stock` compares names, width and rows with rusqlite's
//! bundled SQLite, ad hoc and prepared, across `*`, `a.*`, `b.*`, USING,
//! NATURAL, ON, outer joins and the forms that consume a join's `*`.
//! `stored_join_star_matches_stock` does the same through a view, CREATE TABLE
//! AS and INSERT ... SELECT.
//!
//! b56e7bc7c made `json` and `root` real hidden columns of `json_each` and
//! `json_tree`. The names of a join's `*` skipped them but its row values did
//! not, so a USING or NATURAL join gave rows wider than their header, even when
//! the join column was an ordinary visible one. `bd-star-hidden-reland-nxl8a`
//! re-landed hidden columns through one unified star-expansion authority so
//! column names, values, affinities, collations, and views/CTAS/INSERT-SELECT
//! all agree on [`HIDDEN_COLUMN_SHAPES`] and `generate_series`'s hidden
//! `start`, `stop`, and `step` columns.
//!
//! `generate_series` has no bundled oracle, since it is an extension of the
//! sqlite3 shell. Its expected values are what that shell prints (3.46.1).
//!
//! `planted_wide_row_is_reported` feeds the comparison the rows that bug
//! produced, so a comparison that cannot fail cannot pass.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const SETUP: &[&str] = &[
    "CREATE TABLE t(key INTEGER, note TEXT)",
    "INSERT INTO t VALUES (0,'zero'),(1,'one'),(9,'nine')",
];

const SOURCES: &[&str] = &["json_each('[10,20]')", "json_tree('[10,20]')"];

/// Each runs with `{f}` as every entry of [`SOURCES`].
const SHAPES: &[&str] = &[
    "SELECT * FROM {f} a JOIN (SELECT 0 AS key) b USING(key)",
    "SELECT a.* FROM {f} a JOIN (SELECT 0 AS key) b USING(key)",
    "SELECT b.* FROM {f} a JOIN (SELECT 0 AS key) b USING(key)",
    "SELECT * FROM {f} a NATURAL JOIN (SELECT 0 AS key) b",
    "SELECT a.* FROM {f} a NATURAL JOIN (SELECT 0 AS key) b",
    "SELECT b.* FROM {f} a NATURAL JOIN (SELECT 0 AS key) b",
    "SELECT * FROM (SELECT 0 AS key) b LEFT JOIN {f} a USING(key)",
    "SELECT a.* FROM (SELECT 0 AS key) b LEFT JOIN {f} a USING(key)",
    "SELECT b.* FROM (SELECT 0 AS key) b LEFT JOIN {f} a USING(key)",
    "SELECT * FROM (SELECT 5 AS key) b LEFT JOIN {f} a USING(key)",
    "SELECT * FROM {f} a LEFT JOIN (SELECT 0 AS key) b USING(key)",
    "SELECT * FROM {f} a RIGHT JOIN (SELECT 0 AS key) b USING(key)",
    "SELECT * FROM {f} a FULL JOIN (SELECT 5 AS key) b USING(key)",
    "SELECT * FROM {f} a JOIN {f} b ON a.key = b.key",
    "SELECT a.* FROM {f} a JOIN {f} b ON a.key = b.key",
    "SELECT b.* FROM {f} a JOIN {f} b ON a.key = b.key",
    "SELECT * FROM {f} a JOIN {f} b USING(key)",
    "SELECT a.* FROM {f} a JOIN {f} b USING(key)",
    "SELECT b.* FROM {f} a JOIN {f} b USING(key)",
    "SELECT * FROM {f} a NATURAL JOIN (SELECT 'x' AS json, 'y' AS root) b",
    "SELECT * FROM {f} a JOIN t USING(key)",
    "SELECT * FROM {f} a JOIN t ON a.key = t.key",
    "SELECT * FROM t JOIN {f} a USING(key)",
    "SELECT * FROM {f} a NATURAL JOIN t",
    "SELECT a.*, t.* FROM {f} a JOIN t USING(key)",
    "SELECT t.*, a.* FROM {f} a JOIN t USING(key)",
    // bd-literal-star-name-qzqb4: an unaliased literal beside `*` is named by
    // its text, as stock names it.
    "SELECT *, 1 FROM {f} a JOIN t USING(key)",
    "SELECT 1, *, note FROM {f} a JOIN t USING(key)",
    "SELECT *, 1 AS one FROM {f} a JOIN t USING(key)",
    "SELECT a.*, key + 1, t.* FROM {f} a JOIN t USING(key)",
    "SELECT * FROM {f} a JOIN t USING(key) WHERE a.key = 1",
    "SELECT * FROM {f} a JOIN t USING(key) ORDER BY 1 DESC",
    "SELECT * FROM {f} a JOIN t USING(key) ORDER BY note",
    "SELECT * FROM {f} a JOIN t USING(key) ORDER BY 1 LIMIT 1",
    "SELECT DISTINCT * FROM {f} a JOIN t USING(key)",
    "SELECT * FROM {f} a JOIN t USING(key) UNION ALL SELECT * FROM {f} a JOIN t USING(key)",
    "SELECT * FROM {f} a JOIN t USING(key) JOIN (SELECT 1 AS key) c USING(key)",
    "WITH q AS (SELECT * FROM {f} a JOIN t USING(key)) SELECT * FROM q",
    "WITH q AS (SELECT * FROM {f} a JOIN t USING(key)) SELECT note FROM q",
    "WITH q AS (SELECT * FROM {f} a JOIN t USING(key)) SELECT key, note FROM q ORDER BY key",
    "WITH q AS (SELECT a.* FROM {f} a JOIN t USING(key)) SELECT * FROM q",
    "WITH q AS (SELECT t.*, a.* FROM {f} a JOIN t USING(key)) SELECT * FROM q",
    "WITH q AS (SELECT * FROM {f} a JOIN t ON a.key = t.key) SELECT * FROM q",
    "WITH q AS (SELECT * FROM {f} a NATURAL JOIN t) SELECT * FROM q",
    "WITH q AS (SELECT * FROM {f} a LEFT JOIN t USING(key)) SELECT * FROM q",
    "WITH q AS (SELECT * FROM t JOIN {f} a USING(key)) SELECT * FROM q",
    "WITH q AS (SELECT * FROM {f} a) SELECT * FROM q",
    "WITH q AS MATERIALIZED (SELECT * FROM {f} a JOIN t USING(key)) SELECT * FROM q",
    "WITH q AS NOT MATERIALIZED (SELECT * FROM {f} a JOIN t USING(key)) SELECT * FROM q",
    "WITH q(a1,a2,a3,a4,a5,a6,a7,a8,a9) AS (SELECT * FROM {f} a JOIN t USING(key)) \
     SELECT a9 FROM q",
    "SELECT * FROM (SELECT a.* FROM {f} a JOIN t USING(key))",
    "SELECT * FROM (SELECT * FROM {f} a JOIN t USING(key))",
    "SELECT note FROM (SELECT * FROM {f} a JOIN t USING(key))",
    "SELECT * FROM (SELECT * FROM {f} a JOIN t USING(key)) WHERE note = 'one'",
    "SELECT count(*) FROM (SELECT * FROM {f} a JOIN t USING(key))",
];

/// Shapes that name `json` or `root` as columns, which needs the backed-out
/// hidden columns. Stock returns rows for all of them.
const HIDDEN_COLUMN_SHAPES: &[&str] = &[
    "SELECT * FROM {f} a JOIN {f} b USING(json)",
    "SELECT a.* FROM {f} a JOIN {f} b USING(json)",
    "SELECT b.* FROM {f} a JOIN {f} b USING(json)",
    "SELECT a.json, a.root FROM {f} a JOIN t USING(key)",
    "SELECT * FROM {f} a JOIN (SELECT '[10,20]' AS json, 7 AS z) b USING(json)",
    "WITH q AS (SELECT a.json, a.root, note FROM {f} a JOIN t USING(key)) SELECT * FROM q",
    "WITH q AS (SELECT * FROM {f} a JOIN (SELECT '[10,20]' AS json, 7 AS z) b USING(json)) \
     SELECT * FROM q",
];

/// Forms that store or re-read a join's `*`. `{o}` is a name unique to the
/// statement, and `{j}` the join they consume.
const STORED_SHAPES: &[(&str, &str)] = &[
    ("CREATE VIEW {o} AS {j}", "SELECT * FROM {o}"),
    ("CREATE VIEW {o} AS {j}", "SELECT note FROM {o}"),
    ("CREATE TABLE {o} AS {j}", "SELECT * FROM {o}"),
    ("CREATE TABLE {o} AS {j}", "SELECT note FROM {o}"),
    (
        "CREATE TABLE {o}(key, value, type, atom, id, parent, fullkey, path, note)",
        "INSERT INTO {o} {j}",
    ),
    (
        "CREATE TABLE {o}(key, value, type, atom, id, parent, fullkey, path, note)",
        "INSERT INTO {o} WITH q AS ({j}) SELECT * FROM q",
    ),
];

/// Column names and rows, each value rendered with its storage class.
type Table = (Vec<String>, Vec<Vec<String>>);

fn tag_f(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => format!("i{n}"),
        SqliteValue::Float(f) => format!("r{f}"),
        SqliteValue::Text(s) => format!("t{s}"),
        SqliteValue::Blob(b) => format!("b{}", b.len()),
    }
}

fn tag_r(value: rusqlite::types::Value) -> String {
    use rusqlite::types::Value;
    match value {
        Value::Null => "NULL".to_owned(),
        Value::Integer(n) => format!("i{n}"),
        Value::Real(f) => format!("r{f}"),
        Value::Text(s) => format!("t{s}"),
        Value::Blob(b) => format!("b{}", b.len()),
    }
}

fn stock_table(conn: &rusqlite::Connection, sql: &str) -> Result<Table, String> {
    let mut statement = conn.prepare(sql).map_err(|error| error.to_string())?;
    let names: Vec<String> = statement
        .column_names()
        .map(str::to_owned)
        .collect();
    let width = names.len();
    let rows = statement
        .query_map([], |row| {
            (0..width)
                .map(|index| row.get::<_, rusqlite::types::Value>(index).map(tag_r))
                .collect::<rusqlite::Result<Vec<_>>>()
        })
        .map_err(|error| error.to_string())?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|error| error.to_string())?;
    Ok((names, rows))
}

fn rendered(rows: &[fsqlite_core::connection::Row]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect()
}

/// The result of `sql` ad hoc and prepared.
async fn frank_tables(conn: &Connection, sql: &str) -> Result<[Table; 2], String> {
    let adhoc = conn.query(sql).await.map_err(|error| error.to_string())?;
    let statement = conn.prepare(sql).await.map_err(|error| error.to_string())?;
    let names = statement.column_names();
    let prepared = statement.query().await.map_err(|error| error.to_string())?;
    Ok([(names.clone(), rendered(&adhoc)), (names, rendered(&prepared))])
}

/// JSON1's `id` and `parent` number a private traversal, so they are compared
/// by width only.
fn comparable(names: &[String], rows: &[Vec<String>], ordered: bool) -> Vec<Vec<String>> {
    let mut kept: Vec<Vec<String>> = rows
        .iter()
        .map(|row| {
            row.iter()
                .enumerate()
                .filter(|(index, _)| {
                    !names
                        .get(*index)
                        .is_some_and(|name| name == "id" || name == "parent")
                })
                .map(|(_, value)| value.clone())
                .collect()
        })
        .collect();
    if !ordered {
        kept.sort();
    }
    kept
}

/// Why `frank` differs from `stock`, or `None` when it matches.
fn difference(sql: &str, stock: &Table, frank: &Table) -> Option<String> {
    let (stock_names, stock_rows) = stock;
    let (frank_names, frank_rows) = frank;
    if frank_names != stock_names {
        return Some(format!("names {frank_names:?}, stock {stock_names:?}"));
    }
    if let Some(row) = frank_rows.iter().find(|row| row.len() != stock_names.len()) {
        return Some(format!(
            "row of {} values under {} names: {row:?}",
            row.len(),
            stock_names.len()
        ));
    }
    let ordered = sql.contains("ORDER BY");
    let frank_kept = comparable(frank_names, frank_rows, ordered);
    let stock_kept = comparable(stock_names, stock_rows, ordered);
    (frank_kept != stock_kept).then(|| format!("rows {frank_kept:?}, stock {stock_kept:?}"))
}

async fn assert_shapes_match_stock(shapes: &[&str]) {
    let frank = Connection::open(":memory:").await.unwrap();
    let stock = rusqlite::Connection::open_in_memory().unwrap();
    for sql in SETUP {
        frank.execute(sql).await.unwrap();
        stock.execute_batch(sql).unwrap();
    }
    let mut failures = Vec::new();
    let mut returned_rows = 0;
    for source in SOURCES {
        for shape in shapes {
            let sql = shape.replace("{f}", source);
            let expected = stock_table(&stock, &sql);
            match (&expected, frank_tables(&frank, &sql).await) {
                (Ok(expected), Ok(tables)) => {
                    returned_rows += expected.1.len();
                    for (form, table) in ["ad hoc", "prepared"].iter().zip(&tables) {
                        if let Some(reason) = difference(&sql, expected, table) {
                            failures.push(format!("{sql} [{form}]: {reason}"));
                        }
                    }
                }
                (Err(_), Err(_)) => {}
                (Ok(_), Err(error)) => {
                    failures.push(format!("{sql}: stock returned rows, fsqlite: {error}"));
                }
                (Err(error), Ok(_)) => {
                    failures.push(format!("{sql}: fsqlite returned rows, stock: {error}"));
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} statements differ:\n{}",
        failures.len(),
        SOURCES.len() * shapes.len(),
        failures.join("\n")
    );
    assert!(returned_rows > shapes.len(), "the shapes must return rows");
}

#[test]
fn join_star_matches_stock() {
    asupersync::test_utils::run_test(|| assert_shapes_match_stock(SHAPES));
}

/// bd-window-using-column-guy4l: window functions over USING / NATURAL joins
/// (including `SELECT *`, `ORDER BY`, `PARTITION BY`, `FILTER`, wrapped
/// window expressions, outer-join USING coalescing, 3-way USING, and
/// `GROUP BY` + window) must resolve unqualified USING columns and deduplicate
/// `*` identically to stock SQLite, while an ambiguous `ON` join stays an error.
#[test]
fn bd_window_using_column_guy4l_matches_stock() {
    const WINDOW_USING_SHAPES: &[&str] = &[
        "SELECT *, sum(key) OVER (ORDER BY key) AS s FROM {f} a JOIN t USING(key)",
        "SELECT *, row_number() OVER (ORDER BY key) AS r FROM {f} a JOIN t USING(key)",
        "SELECT *, row_number() OVER () AS r FROM {f} a JOIN t USING(key)",
        "SELECT *, count(*) OVER () AS n FROM {f} a NATURAL JOIN t",
        "SELECT *, sum(key) OVER (PARTITION BY key ORDER BY key) AS s FROM {f} a JOIN t USING(key)",
        "SELECT key, sum(key) OVER (ORDER BY key) AS s, note FROM {f} a JOIN t USING(key) ORDER BY key",
        "SELECT *, sum(key) OVER (ORDER BY key) + key AS s FROM {f} a JOIN t USING(key)",
        "SELECT *, sum(value) FILTER (WHERE key >= 0) OVER (ORDER BY key) AS s FROM {f} a JOIN t USING(key)",
        "SELECT *, row_number() OVER (ORDER BY key) AS r FROM {f} a LEFT JOIN t USING(key)",
        "SELECT *, row_number() OVER (ORDER BY key) AS r FROM {f} a RIGHT JOIN t USING(key)",
        "SELECT *, row_number() OVER (ORDER BY key) AS r FROM {f} a FULL JOIN t USING(key)",
        "SELECT *, sum(key) OVER (ORDER BY key) AS s FROM {f} a JOIN t USING(key) JOIN (SELECT 1 AS key) c USING(key)",
        "SELECT a.*, sum(key) OVER (ORDER BY key) AS s FROM {f} a JOIN t USING(key)",
        "SELECT *, count(*) OVER (ORDER BY key) AS c FROM {f} a JOIN t USING(key) GROUP BY key",
    ];
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let (frank, stock) = if file_backed {
                (
                    Connection::open(dir.path().join("f.db").to_str().unwrap())
                        .await
                        .unwrap(),
                    rusqlite::Connection::open(dir.path().join("r.db")).unwrap(),
                )
            } else {
                (
                    Connection::open(":memory:").await.unwrap(),
                    rusqlite::Connection::open_in_memory().unwrap(),
                )
            };
            let db = if file_backed { "file" } else { "memory" };
            for sql in SETUP {
                frank.execute(sql).await.unwrap();
                stock.execute_batch(sql).unwrap();
            }
            let mut failures = Vec::new();
            let mut returned_rows = 0usize;
            for source in SOURCES {
                for shape in WINDOW_USING_SHAPES {
                    let sql = shape.replace("{f}", source);
                    let expected = stock_table(&stock, &sql);
                    match (&expected, frank_tables(&frank, &sql).await) {
                        (Ok(expected), Ok(tables)) => {
                            returned_rows += expected.1.len();
                            for (form, table) in ["ad hoc", "prepared"].iter().zip(&tables) {
                                if let Some(reason) = difference(&sql, expected, table) {
                                    failures.push(format!("[{db}] {sql} [{form}]: {reason}"));
                                }
                            }
                        }
                        (Ok(_), Err(error)) => {
                            failures.push(format!(
                                "[{db}] {sql}: stock returned rows, fsqlite: {error}"
                            ));
                        }
                        (Err(error), Ok(_)) => {
                            failures.push(format!(
                                "[{db}] {sql}: fsqlite returned rows, stock: {error}"
                            ));
                        }
                        (Err(_), Err(_)) => {}
                    }
                }
                // Negative control: under `ON`, unqualified `key` is ambiguous
                // in both engines and must NOT be coalesced.
                let neg_sql = format!(
                    "SELECT *, sum(key) OVER (ORDER BY key) AS s FROM {source} a JOIN t ON a.key = t.key"
                );
                assert!(
                    stock_table(&stock, &neg_sql).is_err(),
                    "[{db}] stock must reject ambiguous ON query: {neg_sql}"
                );
                assert!(
                    frank_tables(&frank, &neg_sql).await.is_err(),
                    "[{db}] fsqlite must reject ambiguous ON query: {neg_sql}"
                );
            }
            frank.close().await.unwrap();
            assert!(
                failures.is_empty(),
                "[{db}] {} divergence(s):\n{}",
                failures.len(),
                failures.join("\n")
            );
            assert!(
                returned_rows > WINDOW_USING_SHAPES.len(),
                "[{db}] window USING shapes must return rows"
            );
        }
    });
}

#[test]
fn hidden_column_join_shapes_match_stock() {
    asupersync::test_utils::run_test(|| assert_shapes_match_stock(HIDDEN_COLUMN_SHAPES));
}

#[test]
fn hidden_using_paren_join_natural_and_metadata_match_stock() {
    const EXTRA_SETUP: &[&str] = &[
        "CREATE TABLE tj(json TEXT, z INTEGER, note TEXT COLLATE NOCASE)",
        "INSERT INTO tj VALUES ('[10,20]', 5, 'ZERO')",
        "CREATE TABLE tj2(json TEXT, w INTEGER)",
        "INSERT INTO tj2 VALUES ('[10,20]', 77)",
    ];
    const EXTRA_SHAPES: &[&str] = &[
        "SELECT * FROM ({f} a JOIN tj USING(json))",
        "SELECT * FROM tj t1 JOIN ({f} a JOIN tj t2 USING(json))",
        "SELECT * FROM {f} a JOIN tj USING(json) NATURAL JOIN tj2",
        "SELECT z FROM (SELECT * FROM {f} a JOIN tj USING(json)) WHERE z = '5'",
        "SELECT note FROM (SELECT * FROM {f} a JOIN tj USING(json)) WHERE note = 'zero'",
        "WITH q AS (SELECT * FROM {f} a JOIN tj USING(json)) SELECT z, note FROM q WHERE z = '5' AND note = 'zero'",
    ];
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let (frank, stock) = if file_backed {
                (
                    Connection::open(dir.path().join("f.db").to_str().unwrap())
                        .await
                        .unwrap(),
                    rusqlite::Connection::open(dir.path().join("r.db")).unwrap(),
                )
            } else {
                (
                    Connection::open(":memory:").await.unwrap(),
                    rusqlite::Connection::open_in_memory().unwrap(),
                )
            };
            let db = if file_backed { "file" } else { "memory" };
            for sql in EXTRA_SETUP {
                frank.execute(sql).await.unwrap();
                stock.execute_batch(sql).unwrap();
            }
            let mut failures = Vec::new();
            let mut returned_rows = 0usize;
            for source in SOURCES {
                for shape in EXTRA_SHAPES {
                    let sql = shape.replace("{f}", source);
                    let expected = stock_table(&stock, &sql).unwrap();
                    returned_rows += expected.1.len();
                    for (form, table) in ["ad hoc", "prepared"]
                        .iter()
                        .zip(&frank_tables(&frank, &sql).await.unwrap())
                    {
                        if let Some(reason) = difference(&sql, &expected, table) {
                            failures.push(format!("[{db}] {sql} [{form}]: {reason}"));
                        }
                    }
                }
            }
            frank.close().await.unwrap();
            assert!(
                failures.is_empty(),
                "[{db}] {} divergence(s):\n{}",
                failures.len(),
                failures.join("\n")
            );
            assert!(
                returned_rows > EXTRA_SHAPES.len(),
                "[{db}] extra hidden-column shapes must return rows"
            );
        }
    });
}

#[test]
fn stored_join_star_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let frank = Connection::open(":memory:").await.unwrap();
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        for sql in SETUP {
            frank.execute(sql).await.unwrap();
            stock.execute_batch(sql).unwrap();
        }
        let mut failures = Vec::new();
        let mut returned_rows = 0;
        for (source_index, source) in SOURCES.iter().enumerate() {
            let join = format!("SELECT * FROM {source} a JOIN t USING(key)");
            for (shape_index, (setup, read)) in STORED_SHAPES.iter().enumerate() {
                let object = format!("o{source_index}_{shape_index}");
                let fill = |sql: &str| sql.replace("{o}", &object).replace("{j}", &join);
                let setup = fill(setup);
                let read = fill(read);
                frank.execute(&setup).await.unwrap();
                stock.execute_batch(&setup).unwrap();
                let read = if read.starts_with("INSERT") {
                    frank.execute(&read).await.unwrap();
                    stock.execute_batch(&read).unwrap();
                    format!("SELECT * FROM {object}")
                } else {
                    read
                };
                let expected = stock_table(&stock, &read).unwrap();
                returned_rows += expected.1.len();
                for (form, table) in ["ad hoc", "prepared"]
                    .iter()
                    .zip(&frank_tables(&frank, &read).await.unwrap())
                {
                    if let Some(reason) = difference(&read, &expected, table) {
                        failures.push(format!("{setup}; {read} [{form}]: {reason}"));
                    }
                }
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n"));
        assert!(returned_rows > STORED_SHAPES.len(), "the shapes must return rows");
    });
}

async fn assert_series_shows_only_value(statements: &[&str]) {
    let frank = Connection::open(":memory:").await.unwrap();
    for sql in statements {
        let expected = (vec!["value".to_owned()], vec![vec!["i1".to_owned()]]);
        for table in frank_tables(&frank, sql).await.unwrap() {
            assert_eq!(difference(sql, &expected, &table), None, "{sql}");
        }
    }
}

#[test]
fn generate_series_other_side_star_shows_only_value() {
    asupersync::test_utils::run_test(|| {
        assert_series_shows_only_value(&[
            "SELECT b.* FROM generate_series(1,2) a JOIN (SELECT 1 AS value) b USING(value)",
        ])
    });
}

#[test]
fn generate_series_join_star_shows_only_value() {
    asupersync::test_utils::run_test(|| {
        assert_series_shows_only_value(&[
            "SELECT * FROM generate_series(1,2) a JOIN (SELECT 1 AS value) b USING(value)",
            "SELECT a.* FROM generate_series(1,2) a JOIN (SELECT 1 AS value) b USING(value)",
            "SELECT * FROM generate_series(1,2) a NATURAL JOIN (SELECT 1 AS value) b",
            "SELECT * FROM (SELECT 1 AS value) b JOIN generate_series(1,2) a USING(value)",
        ])
    });
}

#[test]
fn planted_wide_row_is_reported() {
    let sql = "SELECT * FROM json_each('[1]') a JOIN (SELECT 0 AS key) b USING(key)";
    let names: Vec<String> = ["key", "value", "type", "atom", "id", "parent", "fullkey", "path"]
        .map(str::to_owned)
        .to_vec();
    let row: Vec<String> = ["i0", "i1", "tinteger", "i1", "i1", "NULL", "t$[0]", "t$"]
        .map(str::to_owned)
        .to_vec();
    let stock = (names.clone(), vec![row.clone()]);
    assert_eq!(difference(sql, &stock, &stock), None);
    let mut wide = row.clone();
    wide.extend(["t[1]".to_owned(), "t$".to_owned()]);
    let reason = difference(sql, &stock, &(names.clone(), vec![wide])).expect("wide row");
    assert!(reason.contains("10 values under 8 names"), "{reason}");
    let mut other = row;
    other[0] = "i7".to_owned();
    let reason = difference(sql, &stock, &(names, vec![other])).expect("different row");
    assert!(reason.starts_with("rows "), "{reason}");
}
