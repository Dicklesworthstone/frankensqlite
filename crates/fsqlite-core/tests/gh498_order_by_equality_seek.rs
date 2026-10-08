#![recursion_limit = "512"]

//! GH#498 (bd-kyh7x): `SELECT v FROM r9_t WHERE k = 'k1' ORDER BY id`, with an index on `k` and
//! `id TEXT PRIMARY KEY`, walked the whole PK autoindex in `id` order and tested `k` on every row.
//! Stock seeks the `k` index and sorts the few matches. Results are compared with stock SQLite
//! (rusqlite, bundled) in order -- every ORDER BY here is total -- on `:memory:` and on a file,
//! through `query()`, `prepare().query()` and bound parameters; the EXPLAIN of the issue's shape
//! must seek `r9_t_k` and sort, not rewind a table or index.
//!
//! Also covered: probes whose storage class the column affinity would convert (an integer
//! against TEXT, text against INTEGER), which must keep the scan, NULL probes, a NOCASE column, a
//! composite index whose trailing key holds NULL and a REAL below every integer (the seek is a
//! one-field prefix there), DISTINCT, LIMIT / OFFSET, and the same composite index as the outer
//! table of a LEFT JOIN, whose equality seek had the same two-field-probe bug.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const SETUP: &[&str] = &[
    "CREATE TABLE r9_t (id TEXT PRIMARY KEY, k TEXT NOT NULL, v TEXT)",
    "CREATE INDEX r9_t_k ON r9_t (k)",
    "WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM n WHERE i < 199) \
     INSERT INTO r9_t SELECT 'id' || i, 'k' || (i % 20), 'v' || i FROM n",
    "INSERT INTO r9_t VALUES ('e5', 5, 'five'), ('e6', '5', 'five again'), ('u1', 'ключ', 'unicode')",
    "CREATE TABLE n (a INTEGER, b INTEGER, c TEXT)",
    "CREATE INDEX n_a ON n (a)",
    "CREATE INDEX n_b ON n (b)",
    "WITH RECURSIVE s(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM s WHERE i < 89) \
     INSERT INTO n SELECT i % 7, i % 13, 'c' || i FROM s",
    "INSERT INTO n VALUES ('3', 100, 'text three'), (3.5, 101, 'three and a half'), ('x', 102, 'ex')",
    "CREATE TABLE comp (a INT, b INT, v TEXT)",
    "CREATE INDEX comp_ab ON comp (a, b)",
    "INSERT INTO comp VALUES (1, NULL, 'n1'), (1, 2, 'x'), (2, NULL, 'n2'), (3, 4, 'y'), \
     (1, NULL, 'n3'), (1, -5, 'neg'), (1, 'txt', 'text'), (1, -1e300, 'huge')",
    "CREATE TABLE nc (k TEXT COLLATE NOCASE, v TEXT)",
    "CREATE INDEX nc_k ON nc (k)",
    "INSERT INTO nc VALUES ('A', 'upper'), ('a', 'lower'), ('b', 'other')",
];

const QUERIES: &[&str] = &[
    "SELECT v FROM r9_t WHERE k = 'k1' ORDER BY id",
    "SELECT v FROM r9_t WHERE k = 'k1' ORDER BY id DESC",
    "SELECT id, v FROM r9_t WHERE k = 'k1' ORDER BY v DESC LIMIT 3",
    "SELECT id FROM r9_t WHERE k = 'k1' AND v > 'v5' ORDER BY id LIMIT 2 OFFSET 1",
    "SELECT id FROM r9_t WHERE 'k2' = k ORDER BY id",
    "SELECT x.id FROM r9_t AS x WHERE x.k = 'k3' ORDER BY x.v",
    "SELECT id FROM r9_t WHERE k = 'k4' ORDER BY rowid DESC",
    "SELECT id FROM r9_t WHERE k = 'k4' AND v <> 'v4' ORDER BY rowid",
    "SELECT v FROM r9_t WHERE k = 'absent' ORDER BY id",
    "SELECT v FROM r9_t WHERE k = NULL ORDER BY id",
    "SELECT id FROM r9_t WHERE k = 5 ORDER BY id",
    "SELECT id FROM r9_t WHERE k = 'ключ' ORDER BY id",
    "SELECT DISTINCT substr(v, 1, 2) FROM r9_t WHERE k = 'k5' ORDER BY 1",
    "SELECT c FROM n WHERE a = 3 ORDER BY b, c",
    "SELECT c FROM n WHERE a = 3 ORDER BY c DESC LIMIT 4",
    "SELECT c FROM n WHERE a = '3' ORDER BY c",
    "SELECT c FROM n WHERE b = 4 AND a > 1 ORDER BY a, c",
    "SELECT v FROM comp WHERE a = 1 ORDER BY v",
    "SELECT v, b FROM comp WHERE a = 1 AND b IS NULL ORDER BY v DESC",
    "SELECT v FROM nc WHERE k = 'a' ORDER BY v",
    "SELECT comp.v, n.c FROM comp LEFT JOIN n ON n.a = comp.b WHERE comp.a = 1 ORDER BY comp.v, n.c",
];

const PARAM_QUERIES: &[(&str, &[SqliteValue])] = &[
    ("SELECT v FROM r9_t WHERE k = ?1 ORDER BY id", &[SqliteValue::Null]),
    ("SELECT v FROM r9_t WHERE k = ?1 ORDER BY id", &[SqliteValue::Integer(5)]),
    ("SELECT v FROM r9_t WHERE k = ?1 ORDER BY id", &[SqliteValue::Float(5.0)]),
    ("SELECT c FROM n WHERE a = ?1 ORDER BY b DESC, c", &[SqliteValue::Integer(3)]),
    ("SELECT c FROM n WHERE a = ?1 ORDER BY b DESC, c", &[SqliteValue::Float(3.0)]),
    ("SELECT c FROM n WHERE a = ?1 ORDER BY b DESC, c", &[SqliteValue::Float(3.5)]),
    ("SELECT c FROM n WHERE a = ?1 ORDER BY b DESC, c", &[SqliteValue::Null]),
    ("SELECT v FROM comp WHERE a = ?1 ORDER BY v", &[SqliteValue::Integer(1)]),
];

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("X'{}'", b.len()),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f:?}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("X'{}'", b.len()),
    }
}

fn to_rusqlite(v: &SqliteValue) -> rusqlite::types::Value {
    match v {
        SqliteValue::Null => rusqlite::types::Value::Null,
        SqliteValue::Integer(n) => rusqlite::types::Value::Integer(*n),
        SqliteValue::Float(f) => rusqlite::types::Value::Real(*f),
        SqliteValue::Text(s) => rusqlite::types::Value::Text(s.to_string()),
        SqliteValue::Blob(b) => rusqlite::types::Value::Blob(b.to_vec()),
    }
}

fn stock_rows(r: &rusqlite::Connection, sql: &str, params: &[SqliteValue]) -> Vec<Vec<String>> {
    let mut statement = r.prepare(sql).expect("stock prepare");
    let n = statement.column_count();
    let params: Vec<rusqlite::types::Value> = params.iter().map(to_rusqlite).collect();
    statement
        .query_map(rusqlite::params_from_iter(params), |row| {
            Ok((0..n)
                .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect::<Vec<_>>())
        })
        .and_then(Iterator::collect)
        .expect("stock query")
}

fn frank_rows(rows: &[fsqlite_core::connection::Row]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect()
}

async fn explain_rows(f: &Connection, sql: &str) -> Vec<(String, String)> {
    f.query(sql)
        .await
        .expect("explain")
        .iter()
        .map(|row| {
            let values = row.values();
            let opcode = match values.get(1) {
                Some(SqliteValue::Text(op)) => op.to_string(),
                _ => String::new(),
            };
            let p4 = match values.get(5) {
                Some(SqliteValue::Text(p4)) => p4.to_string(),
                _ => String::new(),
            };
            (opcode, p4)
        })
        .collect()
}

async fn run(file_backed: bool) -> Vec<String> {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = if file_backed {
        dir.path().join("gh498.db").to_str().expect("utf-8 path").to_owned()
    } else {
        ":memory:".to_owned()
    };
    let f = Connection::open(&path).await.expect("open");
    let r = rusqlite::Connection::open_in_memory().expect("stock open");
    for sql in SETUP {
        f.execute(sql).await.expect("frank setup");
        r.execute(sql, []).expect("stock setup");
    }
    let mut failures = Vec::new();
    for sql in QUERIES {
        let stock = stock_rows(&r, sql, &[]);
        let direct = f.query(sql).await.map(|rows| frank_rows(&rows));
        if direct.as_ref().ok() != Some(&stock) {
            failures.push(format!(
                "[file_backed={file_backed}] `{sql}` (query): frank {direct:?} vs stock {stock:?}"
            ));
        }
        let prepared = match f.prepare(sql).await {
            Ok(statement) => statement.query().await.map(|rows| frank_rows(&rows)),
            Err(e) => Err(e),
        };
        if prepared.as_ref().ok() != Some(&stock) {
            failures.push(format!(
                "[file_backed={file_backed}] `{sql}` (prepare): frank {prepared:?} vs stock {stock:?}"
            ));
        }
    }
    for (sql, params) in PARAM_QUERIES {
        let stock = stock_rows(&r, sql, params);
        let direct = f
            .query_with_params(sql, params)
            .await
            .map(|rows| frank_rows(&rows));
        if direct.as_ref().ok() != Some(&stock) {
            failures.push(format!(
                "[file_backed={file_backed}] `{sql}` {params:?} (query_with_params): frank \
                 {direct:?} vs stock {stock:?}"
            ));
        }
        let prepared = match f.prepare(sql).await {
            Ok(statement) => statement
                .query_with_params(params)
                .await
                .map(|rows| frank_rows(&rows)),
            Err(e) => Err(e),
        };
        if prepared.as_ref().ok() != Some(&stock) {
            failures.push(format!(
                "[file_backed={file_backed}] `{sql}` {params:?} (prepare): frank {prepared:?} vs \
                 stock {stock:?}"
            ));
        }
    }
    // The issue's shape opens and seeks `r9_t_k`, then sorts: no Rewind of the table or of the
    // PK autoindex. A parameter keeps a Rewind for the scan a mismatched storage class takes,
    // but still seeks the index first.
    for (sql, rewind_allowed) in [
        (
            "EXPLAIN SELECT v FROM r9_t WHERE k = 'k1' ORDER BY id",
            false,
        ),
        ("EXPLAIN SELECT v FROM r9_t WHERE k = ?1 ORDER BY id", true),
    ] {
        let program = explain_rows(&f, sql).await;
        let opens_k_index = program
            .iter()
            .any(|(op, p4)| op == "OpenRead" && p4.contains("r9_t_k"));
        let seeks = program.iter().any(|(op, _)| op == "SeekGE");
        let sorts = program.iter().any(|(op, _)| op == "SorterSort");
        let rewinds = program.iter().any(|(op, _)| op == "Rewind");
        if !opens_k_index || !seeks || !sorts || (rewinds && !rewind_allowed) {
            failures.push(format!(
                "[file_backed={file_backed}] `{sql}` does not seek r9_t_k and sort: {program:?}"
            ));
        }
    }
    f.close().await.expect("close");
    failures
}

#[test]
fn order_by_with_an_indexed_equality_seeks_and_sorts_as_stock() {
    asupersync::test_utils::run_test(|| async {
        let mut failures = Vec::new();
        for file_backed in [false, true] {
            failures.extend(run(file_backed).await);
        }
        assert!(
            failures.is_empty(),
            "{} failures:\n{}",
            failures.len(),
            failures.join("\n")
        );
    });
}
