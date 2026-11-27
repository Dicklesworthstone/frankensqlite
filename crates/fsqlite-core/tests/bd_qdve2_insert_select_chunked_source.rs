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
//! not the last chunk's. A `generate_series` source runs in sub-ranges of its
//! series, unless the SELECT names the hidden start/stop/step columns or must
//! see the series whole (ORDER BY, LIMIT, DISTINCT, a subquery on the target).

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
/// rows from a recursive CTE: `generate_series(a, b[, s])` (s >= 1) becomes a
/// subquery yielding `value` = a, a + s, ... up to b in order, beside the
/// hidden `start`, `stop` and `step` columns. (`SELECT *` would show those, so
/// no step selects `*` from a series.)
fn stock_sql(sql: &str) -> String {
    const CALL: &str = "generate_series(";
    let Some(start) = sql.find(CALL) else {
        return sql.to_owned();
    };
    let args_start = start + CALL.len();
    let close = args_start + sql[args_start..].find(')').expect("closing paren");
    let args: Vec<&str> = sql[args_start..close].split(',').map(str::trim).collect();
    let (lo, hi) = (args[0], args[1]);
    let step = args.get(2).copied().unwrap_or("1");
    format!(
        "{}(WITH RECURSIVE gs(value, start, stop, step) AS \
         (SELECT {lo}, {lo}, {hi}, {step} WHERE {lo} <= {hi} \
         UNION ALL SELECT value + {step}, start, stop, step FROM gs \
         WHERE value + {step} <= {hi}) SELECT value, start, stop, step FROM gs){}",
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
    Scenario {
        name: "generate_series sources in sub-ranges: steps, filters, ragged and exact ends",
        steps: &[
            "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)",
            "INSERT INTO t SELECT value, 'n' || value FROM generate_series(1, 25000)",
            "CREATE TABLE t2(x, y)",
            "INSERT INTO t2 SELECT value * 2, value % 7 FROM generate_series(-30000, 30001, 2)",
            "INSERT INTO t2 SELECT value, NULL FROM generate_series(1, 30000) WHERE value % 3 = 0",
            "INSERT INTO t2 SELECT g.value, upper('x' || g.value) \
             FROM generate_series(5, 20004) AS g",
            "INSERT INTO t2(x) SELECT value FROM generate_series(1, 10000)",
            "INSERT INTO t2(x) SELECT value FROM generate_series(1, 10001)",
            "INSERT INTO t2(x) SELECT value FROM generate_series(100, 100)",
            "INSERT INTO t2(x) SELECT value FROM generate_series(5, 1)",
            "INSERT INTO t2(x) SELECT value FROM generate_series(1, 123456, 7)",
            "INSERT INTO t2 SELECT value, value FROM generate_series(1, 20000) WHERE value > 25000",
            "BEGIN",
            "INSERT INTO t2(y) SELECT 'txn' || value FROM generate_series(1, 12000)",
            "COMMIT",
        ],
        checks: &[
            "SELECT count(*), sum(a), sum(length(b)) FROM t",
            "SELECT count(*), sum(rowid * coalesce(x, 1)), sum(rowid * length(y)), \
             sum(y IS NULL) FROM t2",
        ],
    },
    Scenario {
        name: "generate_series: errors across sub-ranges, and shapes that read the series whole",
        steps: &[
            "CREATE TABLE u(a INT UNIQUE, b TEXT)",
            "INSERT INTO u VALUES (22222, 'pre')",
            "INSERT INTO u SELECT value, 'b' || value FROM generate_series(1, 25000)",
            "INSERT OR IGNORE INTO u SELECT value, 'i' || value FROM generate_series(1, 25000, 3)",
            "CREATE TABLE h(v, s, e, st)",
            "INSERT INTO h SELECT value, start, stop, step FROM generate_series(1, 25000, 2)",
            "INSERT INTO h SELECT value, NULL, NULL, NULL FROM generate_series(1, 25000) \
             WHERE stop = 25000 AND value > 24990",
            "INSERT INTO h(v) SELECT value FROM generate_series(1, 21000) ORDER BY value DESC",
            "INSERT INTO h(v) SELECT value FROM generate_series(1, 21000) LIMIT 15000 OFFSET 3",
            "INSERT INTO h(v) SELECT DISTINCT value % 5 FROM generate_series(1, 21000)",
            "INSERT INTO h(v, s) SELECT value, (SELECT count(*) FROM h) \
             FROM generate_series(1, 12000)",
        ],
        checks: &[
            "SELECT count(*), sum(a), sum(b LIKE 'i%'), sum(b = 'pre') FROM u",
            "SELECT count(*), sum(rowid * v), sum(rowid * coalesce(s, 0)), sum(e), sum(st) FROM h",
        ],
    },
];

const COUNTERS: &str = "SELECT changes(), total_changes(), last_insert_rowid()";

#[test]
fn insert_select_reads_the_callers_change_counters() {
    asupersync::test_utils::run_test(|| async {
        for file in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("frank.db");
            let f = Connection::open(if file { path.to_str().unwrap() } else { ":memory:" })
                .await
                .unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            let setup = "CREATE TABLE src(n INTEGER PRIMARY KEY); CREATE TABLE dst(c,t,r); \
                         INSERT INTO src VALUES (11),(22),(33)";
            f.execute_batch(setup).await.unwrap();
            r.execute_batch(setup).unwrap();
            for sql in [
                "INSERT INTO dst SELECT changes(),total_changes(),last_insert_rowid()",
                "INSERT INTO dst SELECT changes(),total_changes(),last_insert_rowid() FROM src WHERE n=22",
                "WITH s AS (SELECT n FROM src WHERE n=22) INSERT INTO dst SELECT changes(),total_changes(),last_insert_rowid() FROM s",
                "INSERT INTO dst SELECT changes(),total_changes(),last_insert_rowid() FROM src LIMIT 1",
                "INSERT INTO dst SELECT changes(),total_changes(),last_insert_rowid() FROM src WHERE n=99",
                "INSERT INTO dst SELECT n,n,n FROM src WHERE 0",
            ] {
                let seed = "UPDATE src SET n=n WHERE n IN (11,22)";
                assert_eq!(f.execute(seed).await.unwrap(), 2);
                assert_eq!(r.execute(seed, []).unwrap(), 2);
                let stored = "SELECT c,t,r FROM dst ORDER BY rowid";
                assert_eq!(
                    f.execute(sql).await.unwrap(),
                    r.execute(sql, []).unwrap(),
                    "{sql}"
                );
                for check in [stored, COUNTERS] {
                    assert_eq!(
                        fq(&f, check).await.unwrap(),
                        rq(&r, check).unwrap(),
                        "file={file}, {sql}, {check}"
                    );
                }
            }
            f.close().await.unwrap();
        }
    });
}

#[test]
fn insert_select_change_counts_stay_stable_across_source_chunks() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let setup = "CREATE TABLE src(n INTEGER PRIMARY KEY); CREATE TABLE dst(c,t); \
                     INSERT INTO src SELECT value FROM generate_series(1,10003); \
                     UPDATE src SET n=n WHERE n<4";
        f.execute_batch(setup).await.unwrap();
        r.execute_batch(&stock_sql(setup)).unwrap();
        let sql = "INSERT INTO dst SELECT changes(), total_changes() FROM src";
        assert_eq!(f.execute(sql).await.unwrap(), 10003);
        assert_eq!(r.execute(sql, []).unwrap(), 10003);
        let groups = "SELECT c,t,count(*) FROM dst GROUP BY c,t";
        let expected = rq(&r, groups).unwrap();
        assert_eq!(expected, [["int:3", "int:10006", "int:10003"]]);
        assert_eq!(fq(&f, groups).await.unwrap(), expected);
        assert_eq!(fq(&f, COUNTERS).await.unwrap(), rq(&r, COUNTERS).unwrap());
    });
}

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

/// A generate_series source whose arguments are parameters is split into
/// sub-ranges once they are bound, anonymous `?` numbered in text order across
/// the result columns, the series arguments and the WHERE clause. Stock's text
/// is spelled out with numbered parameters, since its CTE repeats each bound.
#[test]
fn insert_select_series_with_parameters_matches_stock() {
    const SERIES: &str = "(WITH RECURSIVE gs(value) AS (SELECT ?2 WHERE ?2 <= ?3 \
                          UNION ALL SELECT value + ?4 FROM gs WHERE value + ?4 <= ?3) \
                          SELECT value FROM gs)";
    let cases: [(&str, String, &[i64]); 3] = [
        (
            "INSERT INTO t SELECT value + ?1, 'p' || value FROM generate_series(?2, ?3, ?4)",
            format!("INSERT INTO t SELECT value + ?1, 'p' || value FROM {SERIES}"),
            &[0, -5, 24990, 1],
        ),
        (
            "INSERT INTO t(a) SELECT value + ? FROM generate_series(?, ?, ?) WHERE value % ? = 0",
            format!("INSERT INTO t(a) SELECT value + ?1 FROM {SERIES} WHERE value % ?5 = 0"),
            &[1_000_000, 1, 60000, 3, 2],
        ),
        (
            "INSERT INTO t(b) SELECT ?1 || value FROM generate_series(?2, ?3, ?4)",
            format!("INSERT INTO t(b) SELECT ?1 || value FROM {SERIES}"),
            &[7, 40001, 40000, 1],
        ),
    ];
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        r.execute_batch("PRAGMA foreign_keys = OFF").unwrap();
        f.execute("CREATE TABLE t(a INT, b TEXT)").await.unwrap();
        r.execute_batch("CREATE TABLE t(a INT, b TEXT)").unwrap();
        let mut diffs = Vec::new();
        for (sql, stock, params) in &cases {
            let values: Vec<SqliteValue> =
                params.iter().copied().map(SqliteValue::Integer).collect();
            let fr = f
                .execute_with_params(sql, &values)
                .await
                .map_err(|e| e.to_string());
            let rr = r
                .execute(stock, rusqlite::params_from_iter(params.iter()))
                .map_err(stock_err);
            if fr != rr {
                diffs.push(format!("  `{sql}`\n     frank= {fr:?}\n     stock= {rr:?}"));
            }
            for check in [
                COUNTERS,
                "SELECT count(*), sum(rowid * coalesce(a, 1)), sum(rowid * length(b)) FROM t",
            ] {
                let fr = fq(&f, check).await;
                let rr = rq(&r, check);
                if fr != rr {
                    diffs.push(format!(
                        "  after `{sql}`: `{check}`\n     frank= {fr:?}\n     stock= {rr:?}"
                    ));
                }
            }
        }
        f.close().await.unwrap();
        assert!(
            diffs.is_empty(),
            "{} divergence(s) from rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}

/// bd-1bs3h: unordered full scans and `INSERT ... SELECT` must pick the same
/// strictly narrower covering index (estimated width, tie-broken by most
/// recently created) as stock SQLite across `:memory:` and file-backed,
/// direct and prepared execution, including EQP wording, `REAL`/`STRICT`
/// storage classes, `VIRTUAL` generated columns, `DESC`, `NOT INDEXED`,
/// `SELECT DISTINCT`, `PRAGMA reverse_unordered_selects`, `INSERT OR REPLACE`,
/// and multi-chunk (>10,000 rows) `INSERT ... SELECT` sources.
#[test]
fn bd_1bs3h_covering_full_scan_and_insert_select_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let setup_sqls: &[&str] = &[
            // Exact bd-1bs3h repro table: width(s) = 5+1 = 6, width(ib) = 5 < 6.
            "CREATE TABLE s(a INTEGER PRIMARY KEY, b TEXT, c INT)",
            "CREATE INDEX ib ON s(b)",
            "INSERT INTO s VALUES (1,'z',10),(2,'y',20),(3,'x',30),(4,'w',40)",
            "CREATE TABLE t2(x, y)",
            "CREATE TABLE t_rep(k INT PRIMARY KEY, v TEXT)",
            // Equal width (5 == 5) and full-row index (6 == 6) must keep table scan.
            "CREATE TABLE s2(a INTEGER PRIMARY KEY, b TEXT)",
            "CREATE INDEX s2_b ON s2(b)",
            "INSERT INTO s2 VALUES (1,'z'),(2,'a')",
            "CREATE TABLE s3(x INT, y TEXT)",
            "CREATE INDEX s3_xy ON s3(x, y)",
            "INSERT INTO s3 VALUES (2,'z'),(1,'a')",
            // Tie-breaking (most recently created wins) and TEXT(5) vs INT(1) vs VARCHAR(100)(25).
            "CREATE TABLE s4(id INTEGER PRIMARY KEY, a INT, b INT, c INT, t TEXT, v VARCHAR(100))",
            "CREATE INDEX i_a ON s4(a)",
            "CREATE INDEX i_c ON s4(c)",
            "CREATE INDEX i_t ON s4(t)",
            "CREATE INDEX i_v ON s4(v)",
            "INSERT INTO s4 VALUES (1,30,1,20,'z','a'),(2,10,2,30,'y','b'),(3,20,3,10,'x','c')",
            // DESC, partial, REAL/STRICT, VIRTUAL generated column.
            "CREATE TABLE s5(id INTEGER PRIMARY KEY, a INT, b TEXT)",
            "CREATE INDEX i5_desc ON s5(a DESC)",
            "CREATE INDEX i5_part ON s5(a) WHERE a > 0",
            "INSERT INTO s5 VALUES (1,10,'a'),(2,30,'b'),(3,20,'c')",
            "CREATE TABLE s6(id INTEGER PRIMARY KEY, r REAL, x INT) STRICT",
            "CREATE INDEX i6_r ON s6(r)",
            "INSERT INTO s6 VALUES (1, 3, 10), (2, 1.5, 20), (3, 2, 30)",
            "CREATE TABLE g1(id INTEGER PRIMARY KEY, a INT, b INT GENERATED ALWAYS AS (a + 1) VIRTUAL)",
            "CREATE INDEX g1_a ON g1(a)",
            "INSERT INTO g1(id, a) VALUES (1, 30), (2, 10), (3, 20)",
            // Multi-chunk (>10,000 rows) INSERT ... SELECT with a narrower covering index.
            "CREATE TABLE big_s(id INTEGER PRIMARY KEY, k TEXT, pad TEXT)",
            "INSERT INTO big_s SELECT value, printf('%06d', 12005 - value), 'pad' || value \
             FROM generate_series(1, 12005)",
            "CREATE INDEX big_s_k ON big_s(k)",
            "CREATE TABLE big_dst(src_id INT, k TEXT)",
        ];

        let queries: &[&str] = &[
            "SELECT a, b FROM s",
            "SELECT a, b FROM s WHERE length(b) > 0",
            "SELECT a, upper(b) FROM s",
            "SELECT a, b FROM s LIMIT 2 OFFSET 1",
            "SELECT a, b FROM s NOT INDEXED",
            "SELECT a, c FROM s",
            "SELECT * FROM s",
            "SELECT DISTINCT a, b FROM s",
            "SELECT DISTINCT b FROM s",
            "SELECT DISTINCT b, length(b) FROM s",
            "SELECT DISTINCT length(b) FROM s",
            "SELECT 1, a FROM s",
            "SELECT a, count(*) FROM s",
            "SELECT a, sum(length(b)) FROM s WHERE length(b) > 0",
            "SELECT a, b FROM s2",
            "SELECT x, y FROM s3",
            "SELECT id FROM s4",
            "SELECT id, a FROM s4",
            "SELECT id, t FROM s4",
            "SELECT id, a FROM s5",
            "SELECT id, r, typeof(r) FROM s6",
            "SELECT id, a FROM g1",
            "SELECT id, b FROM g1",
        ];

        let mut diffs = Vec::new();
        for file_backed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let (f, r) = if file_backed {
                let fpath = dir.path().join("f_1bs3h.db");
                let rpath = dir.path().join("r_1bs3h.db");
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
            r.execute_batch("PRAGMA foreign_keys = OFF").unwrap();
            let label = if file_backed { "file" } else { "memory" };

            for sql in setup_sqls {
                f.execute(sql).await.unwrap();
                r.execute_batch(&stock_sql(sql)).unwrap();
            }

            // 1. Direct & prepared SELECT rows + exact EXPLAIN QUERY PLAN parity.
            for sql in queries {
                let stock_rows = rq(&r, sql);
                let direct_rows = fq(&f, sql).await;
                if direct_rows != stock_rows {
                    diffs.push(format!(
                        "[{label} direct] `{sql}`\n  frank={direct_rows:?}\n  stock={stock_rows:?}"
                    ));
                }
                let prep_rows = match f.prepare(sql).await.map_err(|e| e.to_string()) {
                    Ok(st) => st
                        .query()
                        .await
                        .map(|rows| {
                            rows.iter()
                                .map(|row| row.values().iter().map(tag_f).collect::<Vec<_>>())
                                .collect::<Vec<_>>()
                        })
                        .map_err(|e| e.to_string()),
                    Err(e) => Err(e),
                };
                if prep_rows != stock_rows {
                    diffs.push(format!(
                        "[{label} prepared] `{sql}`\n  frank={prep_rows:?}\n  stock={stock_rows:?}"
                    ));
                }

                let eqp_sql = format!("EXPLAIN QUERY PLAN {sql}");
                let frank_eqp: Vec<String> = f
                    .query(&eqp_sql)
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|row| match &row.values()[3] {
                        SqliteValue::Text(s) => s.to_string(),
                        other => panic!("unexpected eqp detail {other:?}"),
                    })
                    .collect();
                let stock_eqp: Vec<String> = r
                    .prepare(&eqp_sql)
                    .unwrap()
                    .query_map([], |row| row.get::<_, String>(3))
                    .unwrap()
                    .map(Result::unwrap)
                    .collect();
                if frank_eqp != stock_eqp {
                    diffs.push(format!(
                        "[{label} EQP] `{sql}`\n  frank={frank_eqp:?}\n  stock={stock_eqp:?}"
                    ));
                }
            }

            // 2. Exact bd-1bs3h INSERT ... SELECT (target rowid order & INSERT OR REPLACE winner).
            for dml in [
                "INSERT INTO t2 SELECT a, b FROM s",
                "INSERT INTO t2 SELECT a, b FROM s WHERE length(b) > 0",
                "INSERT INTO t2 SELECT a, b FROM s NOT INDEXED",
                "INSERT OR REPLACE INTO t_rep SELECT 1, b FROM s",
                // Multi-chunk (>10,000 rows) INSERT ... SELECT must decline rowid keyset/streaming
                // and preserve covering-index order across all 12,005 rows.
                "INSERT INTO big_dst SELECT id, k FROM big_s",
            ] {
                let fr = f.execute(dml).await.map_err(|e| e.to_string());
                let rr = r.execute(dml, []).map_err(stock_err);
                if fr != rr {
                    diffs.push(format!("[{label} DML] `{dml}` frank={fr:?} stock={rr:?}"));
                }
            }
            for check in [
                "SELECT rowid, x, y FROM t2 ORDER BY rowid",
                "SELECT k, v FROM t_rep",
                "SELECT count(*), sum(rowid * src_id) FROM big_dst",
                "SELECT rowid, src_id, k FROM big_dst WHERE rowid IN (1, 2, 10000, 10001, 12005) ORDER BY rowid",
            ] {
                let fr = fq(&f, check).await;
                let rr = rq(&r, check);
                if fr != rr {
                    diffs.push(format!(
                        "[{label} check] `{check}`\n  frank={fr:?}\n  stock={rr:?}"
                    ));
                }
            }

            // 3. PRAGMA reverse_unordered_selects = ON still uses `ib`, walked in reverse.
            f.execute("PRAGMA reverse_unordered_selects = ON").await.unwrap();
            r.execute_batch("PRAGMA reverse_unordered_selects = ON").unwrap();
            let rev_sql = "SELECT a, b FROM s";
            let fr = fq(&f, rev_sql).await;
            let rr = rq(&r, rev_sql);
            if fr != rr {
                diffs.push(format!(
                    "[{label} reverse_unordered_selects] `{rev_sql}`\n  frank={fr:?}\n  stock={rr:?}"
                ));
            }
            f.close().await.unwrap();
        }
        assert!(
            diffs.is_empty(),
            "bd-1bs3h {} divergence(s) from rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
