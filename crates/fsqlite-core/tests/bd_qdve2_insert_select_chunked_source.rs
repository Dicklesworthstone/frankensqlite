#![recursion_limit = "512"]

//! bd-qdve2: INSERT ... SELECT reading one rowid table whole
//! (`SELECT cols FROM s`, optionally `ORDER BY s.rowid`) reads its source in
//! rowid chunks of 10,000 rows and inserts each chunk before reading the
//! next, where nothing the INSERT does can write the source. Each scenario
//! runs on a fresh connection pair, in memory and in a file: every
//! statement's result or error text, then `changes()`, `total_changes()`,
//! `last_insert_rowid()`, and the checks are compared with rusqlite's bundled
//! SQLite. Sources straddle the chunk size, the rowids have gaps and negative
//! values, a constraint error lands in a later chunk, and automatic target
//! rowids make the insertion order visible. The shapes that must read the
//! whole source first are checked too: a trigger on the target that writes
//! the source, and a self-referencing INSERT. So are filtered and computed
//! sources, which run in chunks of 10,000 rows: by rowid where the scan is in
//! rowid order, else by LIMIT/OFFSET (a compound SELECT, or a filter an
//! index can serve). Either way `changes()` is the whole statement's count,
//! not the last chunk's.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => format!("int:{n}"),
        SqliteValue::Float(f) => format!("real:{f}"),
        SqliteValue::Text(s) => format!("text:{s}"),
        SqliteValue::Blob(b) => format!("blob:{b:?}"),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => format!("int:{n}"),
        rusqlite::types::Value::Real(f) => format!("real:{f}"),
        rusqlite::types::Value::Text(s) => format!("text:{s}"),
        rusqlite::types::Value::Blob(b) => format!("blob:{b:?}"),
    }
}

async fn fq(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    conn.query(sql)
        .await
        .map(|rows| {
            rows.iter()
                .map(|r| r.values().iter().map(tag_f).collect())
                .collect()
        })
        .map_err(|e| e.to_string())
}

/// Stock's error message. For an error found while preparing, rusqlite's
/// Display appends the SQL text and the error offset; keep the message alone.
fn stock_err(e: rusqlite::Error) -> String {
    match e {
        rusqlite::Error::SqlInputError { msg, .. }
        | rusqlite::Error::SqliteFailure(_, Some(msg)) => msg,
        other => other.to_string(),
    }
}

fn rq(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut st = conn.prepare(sql).map_err(stock_err)?;
    let n = st.column_count();
    st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .map_err(stock_err)?
    .collect::<Result<Vec<_>, _>>()
    .map_err(stock_err)
}

/// rusqlite's bundled SQLite has no generate_series, so stock reads the same
/// rows from a recursive CTE: `generate_series(a, b)` becomes a subquery
/// yielding `value` = a..=b in order.
fn stock_sql(sql: &str) -> String {
    const CALL: &str = "generate_series(";
    let Some(start) = sql.find(CALL) else {
        return sql.to_owned();
    };
    let args_start = start + CALL.len();
    let comma = args_start + sql[args_start..].find(',').expect("two arguments");
    let close = comma + sql[comma..].find(')').expect("closing paren");
    let lo = sql[args_start..comma].trim();
    let hi = sql[comma + 1..close].trim();
    format!(
        "{}(WITH RECURSIVE gs(value) AS (SELECT {lo} WHERE {lo} <= {hi} \
         UNION ALL SELECT value + 1 FROM gs WHERE value < {hi}) SELECT value FROM gs){}",
        &sql[..start],
        stock_sql(&sql[close + 1..])
    )
}

struct Scenario {
    name: &'static str,
    /// Run in order; each statement's outcome is compared.
    steps: &'static [&'static str],
    /// Compared after the steps (besides the change counters).
    checks: &'static [&'static str],
}

const SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "rowid alias with gaps and negative rowids; automatic target rowids",
        steps: &[
            "CREATE TABLE src(id INTEGER PRIMARY KEY, v TEXT, n REAL)",
            "INSERT INTO src SELECT value * 3 - 40000, 'v' || value, value / 7.0 \
             FROM generate_series(1, 25000)",
            "DELETE FROM src WHERE id % 11 = 0",
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, n REAL)",
            "INSERT INTO t SELECT id, v, n FROM src",
            "CREATE TABLE t2(v TEXT, n REAL)",
            "INSERT INTO t2 SELECT v, n FROM src",
            "INSERT INTO t2(v) SELECT v FROM src ORDER BY src.rowid",
            "INSERT INTO t2(n) SELECT n FROM src ORDER BY src.rowid DESC",
        ],
        checks: &[
            "SELECT count(*), sum(id), min(id), max(id), sum(length(v)), sum(n) FROM t",
            "SELECT count(*), sum(rowid * length(v)), sum(rowid * n) FROM t2",
            "SELECT rowid, v, n FROM t2 WHERE rowid % 4999 = 0 ORDER BY rowid",
        ],
    },
    Scenario {
        name: "chunk boundaries: empty, exactly 10,000 and 20,000 rows, one more",
        steps: &[
            "CREATE TABLE s0(a INT, b TEXT)",
            "CREATE TABLE s1(a INT, b TEXT)",
            "INSERT INTO s1 SELECT value, 'b' || value FROM generate_series(1, 10000)",
            "CREATE TABLE s2(a INT, b TEXT)",
            "INSERT INTO s2 SELECT value, 'c' || value FROM generate_series(1, 20001)",
            "CREATE TABLE t(a INT, b TEXT)",
            "INSERT INTO t SELECT a, b FROM s0",
            "INSERT INTO t SELECT a, b FROM s1",
            "DELETE FROM s2 WHERE a = 20001",
            "INSERT INTO t SELECT a, b FROM s2",
            "INSERT INTO s2 VALUES (20001, 'last')",
            "INSERT INTO t SELECT b, a FROM s2",
        ],
        checks: &["SELECT count(*), sum(rowid * length(b)), max(rowid) FROM t"],
    },
    Scenario {
        name: "a source column named rowid (the scan keys on _rowid_)",
        steps: &[
            "CREATE TABLE src(rowid INT, v TEXT)",
            "INSERT INTO src(rowid, v) SELECT 30000 - value, 'v' || value \
             FROM generate_series(1, 21000)",
            "CREATE TABLE t(r INT, v TEXT)",
            "INSERT INTO t SELECT rowid, v FROM src",
            "INSERT INTO t SELECT src.rowid, v FROM src ORDER BY src._rowid_",
        ],
        checks: &["SELECT count(*), sum(rowid * r), sum(length(v)) FROM t"],
    },
    Scenario {
        name: "a constraint error in a later chunk; OR IGNORE and OR REPLACE across chunks",
        steps: &[
            "CREATE TABLE src(a INT, b TEXT)",
            "INSERT INTO src SELECT value, 'b' || value FROM generate_series(1, 25000)",
            "CREATE TABLE t(a INT UNIQUE, b TEXT)",
            "CREATE TABLE t2(a INT, b TEXT)",
            "INSERT INTO t VALUES (22222, 'pre')",
            "INSERT INTO t SELECT a, b FROM src",
            "BEGIN",
            "INSERT INTO t2 SELECT a, b FROM src",
            "INSERT INTO t SELECT a, b FROM src",
            "COMMIT",
            "INSERT OR IGNORE INTO t SELECT a, b FROM src",
            "UPDATE src SET b = 'new' || a WHERE a % 1000 = 7",
            "INSERT OR REPLACE INTO t SELECT a, b FROM src",
        ],
        checks: &[
            "SELECT count(*), sum(a), sum(b LIKE 'new%'), sum(b = 'pre') FROM t",
            "SELECT count(*), sum(rowid * a) FROM t2",
        ],
    },
    Scenario {
        name: "explicit transaction: the source sees the transaction's own writes",
        steps: &[
            "CREATE TABLE src(a INTEGER PRIMARY KEY, b TEXT)",
            "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)",
            "BEGIN",
            "INSERT INTO src SELECT value, 'b' || value FROM generate_series(1, 15000)",
            "DELETE FROM src WHERE a BETWEEN 9000 AND 11000",
            "INSERT INTO t SELECT a, b FROM src",
            "ROLLBACK",
            "INSERT INTO src SELECT value, 'c' || value FROM generate_series(1, 12000)",
            "BEGIN",
            "INSERT INTO t SELECT a, b FROM src",
            "COMMIT",
        ],
        checks: &["SELECT count(*), sum(a), max(b) FROM t"],
    },
    Scenario {
        name: "read whole first: a trigger on the target writes the source",
        steps: &[
            "CREATE TABLE src(a INT, b TEXT)",
            "INSERT INTO src SELECT value, 'b' || value FROM generate_series(1, 21000)",
            "CREATE TABLE t(a INT, b TEXT)",
            "CREATE TRIGGER tr AFTER INSERT ON t WHEN NEW.a <= 5 BEGIN \
             INSERT INTO src VALUES (NEW.a + 100000, 'trig'); END",
            "INSERT INTO t SELECT a, b FROM src",
        ],
        checks: &[
            "SELECT count(*), sum(a), sum(b = 'trig') FROM t",
            "SELECT count(*), sum(a) FROM src",
        ],
    },
    Scenario {
        name: "read whole first: the target is the source",
        steps: &[
            "CREATE TABLE t(a INT, b TEXT)",
            "INSERT INTO t SELECT value, 'b' || value FROM generate_series(1, 12000)",
            "INSERT INTO t SELECT a, b FROM t",
        ],
        checks: &["SELECT count(*), sum(rowid * a) FROM t"],
    },
    Scenario {
        name: "LIMIT/OFFSET chunks (filtered or computed source): changes() is the statement's",
        steps: &[
            "CREATE TABLE src(a INT, b TEXT)",
            "INSERT INTO src SELECT value, 'b' || value FROM generate_series(1, 25000)",
            "CREATE TABLE t(a INT UNIQUE, b TEXT)",
            "INSERT INTO t SELECT a, b FROM src WHERE a > 100",
            "INSERT INTO t SELECT a - 25000, b FROM src",
            "INSERT INTO t SELECT a + 25000, b FROM src WHERE a <> 22222 \
             UNION ALL SELECT 24000, 'dup'",
            "INSERT INTO t SELECT a + 50000, b FROM src WHERE a < 22223 \
             UNION ALL SELECT 50001, 'dup'",
            "BEGIN",
            "INSERT INTO t SELECT a + 100000, b FROM src WHERE a % 2 = 0",
            "INSERT INTO t SELECT a + 200000, b FROM src WHERE a % 2 = 1 \
             UNION ALL SELECT 100002, 'dup'",
            "COMMIT",
        ],
        checks: &["SELECT count(*), sum(a), sum(rowid * length(b)) FROM t"],
    },
    Scenario {
        name: "filtered sources in rowid chunks, and an index the filter can use",
        steps: &[
            "CREATE TABLE src(id INTEGER PRIMARY KEY, a INT, b TEXT, c INT)",
            "INSERT INTO src SELECT value * 2 - 30000, value % 977, 'b' || (25000 - value), \
             value FROM generate_series(1, 25000)",
            "CREATE INDEX src_b ON src(b)",
            "CREATE TABLE t(x, y, z)",
            "INSERT INTO t SELECT a, b, c FROM src WHERE c % 3 <> 0",
            "INSERT INTO t SELECT x.c * 2, upper(x.b), x.id FROM src AS x WHERE x.a < 900",
            "INSERT INTO t SELECT a, b, id FROM src WHERE id BETWEEN -20000 AND 15000",
            "INSERT INTO t(z) SELECT c FROM src WHERE b > 'b2'",
            "CREATE TABLE t4(p, q, r, s)",
            "INSERT INTO t4 SELECT * FROM src WHERE a IN (1, 2, 3) OR c > 24000",
        ],
        checks: &[
            "SELECT count(*), sum(rowid * coalesce(x, 1)), sum(rowid * length(y)), \
             sum(rowid * z) FROM t",
            "SELECT count(*), sum(rowid * p), sum(rowid * q), sum(rowid * s) FROM t4",
        ],
    },
    Scenario {
        name: "WITHOUT ROWID and TEMP sources",
        steps: &[
            "CREATE TABLE wr(k INT PRIMARY KEY, v TEXT) WITHOUT ROWID",
            "INSERT INTO wr SELECT 15000 - value, 'v' || value FROM generate_series(1, 12000)",
            "CREATE TEMP TABLE tmp(a INT, b TEXT)",
            "INSERT INTO tmp SELECT value, 'tmp' || value FROM generate_series(1, 11000)",
            "CREATE TABLE t(a INT, b TEXT)",
            "INSERT INTO t SELECT k, v FROM wr",
            "INSERT INTO t SELECT a, b FROM tmp",
        ],
        checks: &["SELECT count(*), sum(rowid * a), sum(length(b)) FROM t"],
    },
];

const COUNTERS: &str = "SELECT changes(), total_changes(), last_insert_rowid()";

/// Runs every scenario on fresh connection pairs opened by `open`, comparing
/// each step's outcome, the counters after it, and the checks. Returns the
/// divergences and the number of statements compared.
async fn compare_scenarios(file_backed: bool) -> (Vec<String>, usize) {
    let mut diffs = Vec::new();
    let mut statements = 0usize;
    for (index, scenario) in SCENARIOS.iter().enumerate() {
        let dir = tempfile::tempdir().unwrap();
        let (f, r) = if file_backed {
            let fpath = dir.path().join(format!("f{index}.db"));
            let rpath = dir.path().join(format!("r{index}.db"));
            (
                Connection::open(fpath.to_str().unwrap()).await.unwrap(),
                rusqlite::Connection::open(rpath).unwrap(),
            )
        } else {
            (
                Connection::open(":memory:").await.unwrap(),
                rusqlite::Connection::open_in_memory().unwrap(),
            )
        };
        // Bundled rusqlite enables foreign keys by default; stock's default,
        // and fsqlite's, is off.
        r.execute_batch("PRAGMA foreign_keys = OFF").unwrap();
        let label = if file_backed { "file" } else { "memory" };
        let mut compare = async |sql: &str| {
            statements += 1;
            let fr = fq(&f, sql).await;
            let rr = rq(&r, &stock_sql(sql));
            if fr != rr {
                diffs.push(format!(
                    "  [{label}: {}] `{sql}`\n     frank= {fr:?}\n     stock= {rr:?}",
                    scenario.name
                ));
            }
        };
        for step in scenario.steps {
            compare(step).await;
            compare(COUNTERS).await;
        }
        for check in scenario.checks {
            compare(check).await;
        }
        drop(compare);
        f.close().await.unwrap();
    }
    (diffs, statements)
}

#[test]
fn insert_select_chunked_source_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let (mut diffs, mut statements) = compare_scenarios(false).await;
        let (file_diffs, file_statements) = compare_scenarios(true).await;
        diffs.extend(file_diffs);
        statements += file_statements;
        assert!(
            diffs.is_empty(),
            "{} of {statements} statement(s) diverge from rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
