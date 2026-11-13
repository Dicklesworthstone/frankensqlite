#![recursion_limit = "512"]

//! An index that serves `WHERE col = value ORDER BY <next key column>` seeks its
//! equality prefix with SQLite's comparison affinity and collation.
//!
//! The ORDER BY index lane (`codegen_select_index_ordered_scan`) seeked the
//! equality prefix with the raw value and walked only that block, with no
//! fallback scan. So `nu = '1' ORDER BY id` over `INDEX(nu, id)` on a NUMERIC,
//! INTEGER or REAL column returned no rows: the TEXT probe '1' lands among the
//! TEXT keys, while SQLite converts it to the number 1 first. The same held for
//! a bound TEXT parameter, and `k = 'v' COLLATE NOCASE` seeked a BINARY index
//! for 'v' only. Found by the fresh-eyes review of 71556d531 (the lane predates
//! it). Results are compared with rusqlite, in memory and file-backed, as
//! literals and as parameters.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => format!("i{n}"),
        SqliteValue::Float(f) => format!("r{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("x{b:?}"),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => format!("i{n}"),
        rusqlite::types::Value::Real(f) => format!("r{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("x{b:?}"),
    }
}

fn to_r(v: &SqliteValue) -> rusqlite::types::Value {
    match v {
        SqliteValue::Null => rusqlite::types::Value::Null,
        SqliteValue::Integer(n) => rusqlite::types::Value::Integer(*n),
        SqliteValue::Float(f) => rusqlite::types::Value::Real(*f),
        SqliteValue::Text(s) => rusqlite::types::Value::Text(s.to_string()),
        SqliteValue::Blob(b) => rusqlite::types::Value::Blob(b.to_vec()),
    }
}

const SETUP: &[&str] = &[
    "CREATE TABLE p(id INTEGER PRIMARY KEY, a TEXT, k TEXT, n INTEGER, r REAL, nu NUMERIC, x, \
     kn TEXT COLLATE NOCASE)",
    "INSERT INTO p VALUES (1,'A','v',1,1,1,1,'v'),(2,'A','V','1','1','1','1','V'),\
     (3,'a','1',1.0,1.0,1.0,1.0,'1'),(4,'A',NULL,NULL,NULL,NULL,NULL,NULL),\
     (5,'B','abc','2.5','2.5','2.5','2.5','ABC'),(6,'A',' 1',' 1',' 1',' 1',' 1',' 1'),\
     (7,'A','1.0','1.0','1.0','1.0','1.0','1.0'),(8,'A',x'31',x'31',x'31',x'31',x'31',x'31'),\
     (9,'A','2',2,2,2,2,'2'),(10,'A',2.5,2.5,2.5,2.5,2.5,2.5),(11,'A','abc','abc','abc',\
     'abc','abc','abc')",
    "CREATE INDEX p_kid ON p(k, id)",
    "CREATE INDEX p_nid ON p(n, id)",
    "CREATE INDEX p_rid ON p(r, id)",
    "CREATE INDEX p_nuid ON p(nu, id)",
    "CREATE INDEX p_xid ON p(x, id)",
    "CREATE INDEX p_knid ON p(kn, id)",
    "CREATE INDEX p_akid ON p(a, k, id)",
];

const VALUES: &[&str] = &[
    "1", "'1'", "1.0", "'1.0'", "' 1'", "2.5", "'2.5'", "'v'", "'V'", "'abc'", "x'31'", "NULL",
];

fn params() -> Vec<SqliteValue> {
    vec![
        SqliteValue::Integer(1),
        SqliteValue::Float(1.0),
        SqliteValue::Float(2.5),
        SqliteValue::Text("1".into()),
        SqliteValue::Text(" 1".into()),
        SqliteValue::Text("1.0".into()),
        SqliteValue::Text("2.5".into()),
        SqliteValue::Text("v".into()),
        SqliteValue::Text("V".into()),
        SqliteValue::Blob(b"1".to_vec().into()),
        SqliteValue::Null,
    ]
}

fn queries() -> Vec<String> {
    let mut queries = Vec::new();
    for column in ["k", "n", "r", "nu", "x", "kn"] {
        for value in VALUES.iter().copied().chain(["?1"]) {
            for suffix in ["", " COLLATE NOCASE", " COLLATE BINARY"] {
                for order in ["id", "id DESC"] {
                    queries.push(format!(
                        "SELECT id FROM p WHERE {column} = {value}{suffix} ORDER BY {order}"
                    ));
                }
            }
            queries.push(format!(
                "SELECT id FROM p WHERE {value} = {column} ORDER BY id"
            ));
        }
    }
    for value in VALUES.iter().copied().chain(["?1"]) {
        queries.push(format!(
            "SELECT id FROM p WHERE a = 'A' AND k = {value} ORDER BY id"
        ));
        queries.push(format!(
            "SELECT id FROM p WHERE a = 'a' COLLATE NOCASE AND k = {value} ORDER BY id DESC"
        ));
    }
    queries
}

async fn franken_rows(
    conn: &Connection,
    sql: &str,
    params: &[SqliteValue],
) -> Result<Vec<String>, String> {
    conn.query_with_params(sql, params)
        .await
        .map(|rows| {
            rows.iter()
                .map(|row| row.values().iter().map(tag_f).collect::<Vec<_>>().join("|"))
                .collect()
        })
        .map_err(|error| error.to_string())
}

fn stock_rows(
    conn: &rusqlite::Connection,
    sql: &str,
    params: &[SqliteValue],
) -> Result<Vec<String>, String> {
    let mut stmt = conn.prepare(sql).map_err(|error| error.to_string())?;
    let ncol = stmt.column_count();
    let params: Vec<rusqlite::types::Value> = params.iter().map(to_r).collect();
    stmt.query_map(rusqlite::params_from_iter(params.iter()), |row| {
        Ok((0..ncol)
            .map(|i| tag_r(&row.get::<_, rusqlite::types::Value>(i).unwrap()))
            .collect::<Vec<_>>()
            .join("|"))
    })
    .map_err(|error| error.to_string())?
    .collect::<Result<_, _>>()
    .map_err(|error| error.to_string())
}

#[test]
fn ordered_index_equality_prefix_matches_sqlite() {
    for file_backed in [false, true] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("ordered_index_prefix.db");
            let f = if file_backed {
                Connection::open(path.to_str().unwrap()).await.unwrap()
            } else {
                Connection::open(":memory:").await.unwrap()
            };
            let r = rusqlite::Connection::open_in_memory().unwrap();
            for sql in SETUP {
                f.execute(sql).await.unwrap();
                r.execute_batch(sql).unwrap();
            }
            let mut found = Vec::new();
            for sql in queries() {
                let param_sets: Vec<Vec<SqliteValue>> = if sql.contains("?1") {
                    params().into_iter().map(|value| vec![value]).collect()
                } else {
                    vec![Vec::new()]
                };
                for params in param_sets {
                    let ff = franken_rows(&f, &sql, &params).await;
                    let rr = stock_rows(&r, &sql, &params);
                    if ff != rr {
                        found.push(format!(
                            "[file_backed={file_backed}] {sql} params={params:?}\n  \
                             fsqlite: {ff:?}\n  stock:   {rr:?}"
                        ));
                    }
                }
            }
            assert!(
                found.is_empty(),
                "{} mismatches vs stock:\n{}",
                found.len(),
                found.join("\n")
            );
        });
    }
}

/// The ORDER BY index lane still serves the shape after the fix: the prefix
/// seek remains (no table scan, no sorter) and the TEXT literal is converted
/// before it.
#[test]
fn ordered_index_prefix_keeps_the_seek_and_converts_the_probe() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ordered_index_prefix_plan.db");
        let f = Connection::open(path.to_str().unwrap()).await.unwrap();
        for sql in SETUP {
            f.execute(sql).await.unwrap();
        }
        let rows = f
            .query("EXPLAIN SELECT id FROM p WHERE nu = '1' ORDER BY id")
            .await
            .unwrap();
        let opcodes: Vec<String> = rows
            .iter()
            .map(|row| match &row.values()[1] {
                SqliteValue::Text(op) => op.to_string(),
                other => format!("{other:?}"),
            })
            .collect();
        for expected in ["Affinity", "SeekGE", "IdxGT"] {
            assert!(
                opcodes.iter().any(|op| op == expected),
                "expected {expected} in {opcodes:?}"
            );
        }
        for absent in ["SorterOpen", "Rewind"] {
            assert!(
                !opcodes.iter().any(|op| op == absent),
                "unexpected {absent} in {opcodes:?}"
            );
        }
    });
}
