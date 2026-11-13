#![recursion_limit = "512"]

//! bd-i3csr (GH#491): INSERT ... SELECT replays its source rows in batches,
//! several rows per prepared multi-row INSERT ... VALUES, where that keeps the
//! per-row replay's semantics. Each scenario runs on a fresh connection pair:
//! every statement's result or error text, then `changes()`,
//! `total_changes()`, `last_insert_rowid()` and the target's full contents
//! are compared with rusqlite's bundled SQLite. Row counts straddle the batch
//! size (32 rows, fewer for wide rows); conflicts, constraint errors and a
//! source error land inside and between batches; the shapes that must keep
//! per-row replay (triggers, foreign keys, RETURNING, UPSERT, OR FAIL) are
//! checked alongside. Not covered, as they differ from stock with or without
//! batching: STRICT error texts (bd-8djon) and failures part way through the
//! source (bd-qdve2, see SOURCE_ERROR).

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
/// rows from a recursive CTE: each `generate_series(a, b)` becomes a subquery
/// yielding `value` = a..=b in order (none when a > b). fsqlite keeps the
/// table-valued function, the GH#491 source.
fn stock_sql(sql: &str) -> String {
    const CALL: &str = "generate_series(";
    let mut out = String::with_capacity(sql.len());
    let mut rest = sql;
    while let Some(start) = rest.find(CALL) {
        out.push_str(&rest[..start]);
        let args_start = start + CALL.len();
        let mut depth = 0usize;
        let mut comma = None;
        let mut close = None;
        for (i, ch) in rest[args_start..].char_indices() {
            match ch {
                '(' => depth += 1,
                ')' if depth == 0 => {
                    close = Some(args_start + i);
                    break;
                }
                ')' => depth -= 1,
                ',' if depth == 0 => comma = Some(args_start + i),
                _ => {}
            }
        }
        let (comma, close) = (comma.expect("two arguments"), close.expect("closing paren"));
        let lo = rest[args_start..comma].trim();
        let hi = rest[comma + 1..close].trim();
        out.push_str(&format!(
            "(WITH RECURSIVE gs(value) AS (SELECT {lo} WHERE {lo} <= {hi} \
             UNION ALL SELECT value + 1 FROM gs WHERE value < {hi}) SELECT value FROM gs)"
        ));
        rest = &rest[close + 1..];
    }
    out.push_str(rest);
    out
}

struct Scenario {
    name: &'static str,
    /// Run in order; each statement's outcome is compared.
    steps: &'static [&'static str],
    /// Compared after the steps (besides the change counters).
    checks: &'static [&'static str],
}

/// `n` rows from generate_series: `a` = 1..n, `b` = 'r' || a.
const SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "row counts around the batch size, plain index",
        steps: &[
            "CREATE TABLE t(a INT, b TEXT)",
            "CREATE INDEX ta ON t(a)",
            "INSERT INTO t SELECT value, 'r' || value FROM generate_series(1, 0)",
            "INSERT INTO t SELECT value, 'r' || value FROM generate_series(1, 1)",
            "INSERT INTO t SELECT value, 'r' || value FROM generate_series(2, 32)",
            "INSERT INTO t SELECT value, 'r' || value FROM generate_series(33, 65)",
            "INSERT INTO t SELECT value, 'r' || value FROM generate_series(66, 1000)",
        ],
        checks: &[
            "SELECT count(*), sum(a), min(b), max(b) FROM t",
            "SELECT a FROM t WHERE a BETWEEN 30 AND 35 ORDER BY a",
        ],
    },
    Scenario {
        name: "int PRIMARY KEY (GH#491 shape) and UNIQUE",
        steps: &[
            "CREATE TABLE t(a INT PRIMARY KEY, b VARCHAR, c TEXT UNIQUE)",
            "INSERT INTO t SELECT value, 'name' || value, 'c' || value \
             FROM generate_series(1, 200)",
            "INSERT INTO t SELECT value, 'again', 'd' || value FROM generate_series(150, 160)",
        ],
        checks: &["SELECT count(*), sum(a), count(DISTINCT c) FROM t"],
    },
    Scenario {
        name: "ABORT: a duplicate in a later batch rolls back the whole statement",
        steps: &[
            "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT UNIQUE)",
            "INSERT INTO t VALUES (500, 'r70')",
            "INSERT INTO t SELECT value, 'r' || value FROM generate_series(1, 100)",
            "INSERT INTO t SELECT value, 'x' || value FROM generate_series(1, 100)",
        ],
        checks: &["SELECT count(*), sum(a) FROM t"],
    },
    Scenario {
        name: "explicit transaction: a failed statement keeps the earlier ones",
        steps: &[
            "CREATE TABLE t(a INT UNIQUE, b TEXT)",
            "BEGIN",
            "INSERT INTO t SELECT value, 'r' || value FROM generate_series(1, 40)",
            "INSERT INTO t SELECT value + 35, 'r' FROM generate_series(1, 40)",
            "INSERT INTO t SELECT value + 1000, 'k' FROM generate_series(1, 33)",
            "COMMIT",
        ],
        checks: &["SELECT count(*), sum(a), max(a) FROM t"],
    },
    Scenario {
        name: "OR IGNORE: duplicates within a batch, across batches, and existing rows",
        steps: &[
            "CREATE TABLE t(a INT UNIQUE, b TEXT)",
            "INSERT INTO t VALUES (7, 'old'), (40, 'old')",
            "INSERT OR IGNORE INTO t SELECT value % 50, 'n' || value \
             FROM generate_series(1, 120)",
        ],
        checks: &[
            "SELECT count(*), sum(a) FROM t",
            "SELECT a, b FROM t WHERE a IN (0, 1, 7, 40, 49) ORDER BY a",
        ],
    },
    Scenario {
        name: "OR REPLACE: duplicates within and across batches",
        steps: &[
            "CREATE TABLE t(a INT PRIMARY KEY, b TEXT)",
            "INSERT INTO t VALUES (3, 'old')",
            "INSERT OR REPLACE INTO t SELECT value % 20, 'n' || value \
             FROM generate_series(1, 90)",
        ],
        checks: &["SELECT a, b FROM t ORDER BY a"],
    },
    Scenario {
        name: "rowid tables: last_insert_rowid and automatic rowids",
        steps: &[
            "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)",
            "INSERT INTO t(b) SELECT 'r' || value FROM generate_series(1, 77)",
            "INSERT INTO t SELECT value * 10, 'x' FROM generate_series(1, 40)",
            "INSERT INTO t(b) SELECT 'y' FROM generate_series(1, 35)",
        ],
        checks: &["SELECT count(*), sum(a), max(a) FROM t"],
    },
    Scenario {
        name: "AUTOINCREMENT",
        steps: &[
            "CREATE TABLE t(a INTEGER PRIMARY KEY AUTOINCREMENT, b TEXT)",
            "INSERT INTO t(b) SELECT 'r' || value FROM generate_series(1, 70)",
            "DELETE FROM t WHERE a > 60",
            "INSERT INTO t(b) SELECT 's' || value FROM generate_series(1, 5)",
        ],
        checks: &[
            "SELECT seq FROM sqlite_sequence WHERE name = 't'",
            "SELECT max(a), count(*) FROM t",
        ],
    },
    Scenario {
        name: "column list, defaults, and a reordered target",
        steps: &[
            "CREATE TABLE t(a INT, b TEXT DEFAULT 'dflt', c REAL DEFAULT 1.5, d INT)",
            "CREATE INDEX td ON t(d, a)",
            "INSERT INTO t(d, a) SELECT value * 2, value FROM generate_series(1, 50)",
            "INSERT INTO t(c, b, a) SELECT value / 2.0, 'x', -value FROM generate_series(1, 33)",
        ],
        checks: &["SELECT * FROM t ORDER BY rowid"],
    },
    Scenario {
        name: "wide rows (smaller batches)",
        steps: &[
            "CREATE TABLE w(c01, c02, c03, c04, c05, c06, c07, c08, c09, c10, \
             c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, \
             c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, \
             c31, c32, c33, c34, c35, c36, c37, c38, c39, c40)",
            "CREATE INDEX wi ON w(c40)",
            "INSERT INTO w SELECT value, value + 1, value + 2, value + 3, value + 4, \
             value + 5, value + 6, value + 7, value + 8, value + 9, \
             value + 10, value + 11, value + 12, value + 13, value + 14, \
             value + 15, value + 16, value + 17, value + 18, value + 19, \
             value + 20, value + 21, value + 22, value + 23, value + 24, \
             value + 25, value + 26, value + 27, value + 28, value + 29, \
             value + 30, value + 31, value + 32, value + 33, value + 34, \
             value + 35, value + 36, value + 37, value + 38, 'last' || value \
             FROM generate_series(1, 103)",
        ],
        checks: &["SELECT count(*), sum(c01), sum(c39), max(c40) FROM w"],
    },
    Scenario {
        name: "constraint errors inside a batch",
        steps: &[
            "CREATE TABLE t(a INT NOT NULL, b INT CHECK (b < 60))",
            "CREATE TABLE s(a INTEGER PRIMARY KEY, b TEXT) STRICT",
            "INSERT INTO t SELECT value, value FROM generate_series(1, 59)",
            "INSERT INTO t SELECT value, value FROM generate_series(1, 70)",
            "INSERT INTO t SELECT CASE WHEN value = 45 THEN NULL ELSE value END, 1 \
             FROM generate_series(1, 50)",
            "INSERT INTO s SELECT value, CASE WHEN value = 40 THEN 1 ELSE 'ok' END \
             FROM generate_series(1, 50)",
        ],
        checks: &["SELECT count(*), sum(a) FROM t", "SELECT count(*) FROM s"],
    },
    Scenario {
        name: "from a base table (streaming source), filtered and self-referencing",
        steps: &[
            "CREATE TABLE src(x INT, y TEXT)",
            "INSERT INTO src SELECT value, 'y' || value FROM generate_series(1, 150)",
            "CREATE TABLE t(a INT PRIMARY KEY, b TEXT)",
            "INSERT INTO t SELECT x, y FROM src WHERE x % 3 <> 0",
            "INSERT INTO t SELECT x + 1000, y FROM src JOIN t ON t.a = src.x WHERE x < 80",
            "INSERT INTO t SELECT a + 5000, b FROM t",
        ],
        checks: &["SELECT count(*), sum(a), max(b) FROM t"],
    },
    Scenario {
        name: "generated columns and WITHOUT ROWID",
        steps: &[
            "CREATE TABLE g(a INT, b INT GENERATED ALWAYS AS (a * 2) VIRTUAL, \
             c INT GENERATED ALWAYS AS (a + 1) STORED, d TEXT)",
            "CREATE INDEX gb ON g(b)",
            "INSERT INTO g(a, d) SELECT value, 'd' FROM generate_series(1, 45)",
            "CREATE TABLE wr(k TEXT PRIMARY KEY, v INT) WITHOUT ROWID",
            "INSERT INTO wr SELECT 'k' || value, value FROM generate_series(1, 70)",
            "INSERT OR IGNORE INTO wr SELECT 'k' || (value * 2), -value \
             FROM generate_series(1, 50)",
        ],
        checks: &[
            "SELECT count(*), sum(b), sum(c) FROM g",
            "SELECT count(*), sum(v) FROM wr",
        ],
    },
    Scenario {
        name: "shapes that keep per-row replay: triggers, FK, RETURNING, UPSERT, OR FAIL",
        steps: &[
            "CREATE TABLE log(n INT)",
            "CREATE TABLE t(a INT UNIQUE, b TEXT)",
            "CREATE TRIGGER tr AFTER INSERT ON t BEGIN \
             INSERT INTO log VALUES ((SELECT count(*) FROM t)); END",
            "INSERT INTO t SELECT value, 'r' FROM generate_series(1, 40)",
            "DROP TRIGGER tr",
            "INSERT INTO t SELECT value, 'u' FROM generate_series(30, 70) \
             WHERE true ON CONFLICT(a) DO UPDATE SET b = 'up'",
            "INSERT OR FAIL INTO t SELECT value + 100, 'f' FROM generate_series(1, 50) \
             UNION ALL SELECT 5, 'dup'",
            "PRAGMA foreign_keys = ON",
            "CREATE TABLE child(x INT REFERENCES t(a))",
            "INSERT INTO child SELECT value FROM generate_series(1, 40)",
            "INSERT INTO child SELECT value FROM generate_series(60, 75)",
        ],
        checks: &[
            "SELECT count(*), sum(n) FROM log",
            "SELECT count(*), sum(a), sum(b = 'up') FROM t",
            "SELECT count(*) FROM child",
        ],
    },
];

/// A source whose evaluation fails part way. fsqlite evaluates such a source
/// before inserting any row, so the error is reported with nothing inserted
/// and `last_insert_rowid()` unchanged; stock inserts the rows before the
/// failing one first and leaves `last_insert_rowid()` at the last of them.
/// That difference predates batching (bd-qdve2), so this scenario compares
/// the error, the contents and the change counts, not `last_insert_rowid()`.
const SOURCE_ERROR: &[Scenario] = &[Scenario {
    name: "a source error",
    steps: &[
        "CREATE TABLE src(x INT)",
        "INSERT INTO src SELECT value FROM generate_series(1, 60)",
        "CREATE TABLE t(a INT UNIQUE, b INT)",
        "INSERT INTO t SELECT value, CASE WHEN value = 50 \
         THEN abs(-9223372036854775807 - 1) ELSE value END FROM generate_series(1, 60)",
        "INSERT INTO t SELECT x, CASE WHEN x = 50 \
         THEN abs(-9223372036854775807 - 1) ELSE x END FROM src",
        "INSERT INTO t SELECT x, x FROM src WHERE x <= 40",
    ],
    checks: &["SELECT count(*), sum(a) FROM t"],
}];

const COUNTERS: &str = "SELECT changes(), total_changes(), last_insert_rowid()";

/// Runs `scenarios` on fresh connection pairs, comparing each step's outcome
/// and then `counters` after it, and finally the checks. Returns the
/// divergences and the number of statements compared.
async fn compare_scenarios(scenarios: &[Scenario], counters: &str) -> (Vec<String>, usize) {
    let mut diffs = Vec::new();
    let mut statements = 0usize;
    for scenario in scenarios {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        // Bundled rusqlite enables foreign keys by default; stock's default,
        // and fsqlite's, is off. Scenarios turn them on.
        r.execute_batch("PRAGMA foreign_keys = OFF").unwrap();
        let mut compare = async |sql: &str| {
            statements += 1;
            let fr = fq(&f, sql).await;
            let rr = rq(&r, &stock_sql(sql));
            if fr != rr {
                diffs.push(format!(
                    "  [{}] `{sql}`\n     frank= {fr:?}\n     stock= {rr:?}",
                    scenario.name
                ));
            }
        };
        for step in scenario.steps {
            compare(step).await;
            compare(counters).await;
        }
        for check in scenario.checks {
            compare(check).await;
        }
    }
    (diffs, statements)
}

#[test]
fn insert_select_batched_replay_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let (mut diffs, mut statements) = compare_scenarios(SCENARIOS, COUNTERS).await;
        let (source_diffs, source_statements) =
            compare_scenarios(SOURCE_ERROR, "SELECT changes(), total_changes()").await;
        diffs.extend(source_diffs);
        statements += source_statements;
        assert!(
            diffs.is_empty(),
            "{} of {statements} statement(s) diverge from rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
