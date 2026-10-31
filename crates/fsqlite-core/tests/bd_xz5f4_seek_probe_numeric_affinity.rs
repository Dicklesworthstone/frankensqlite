//! bd-xz5f4: a seek probe on a REAL index column took REAL affinity, so an
//! integer past 2^53 rounded and the seek started at the wrong key. Stock caps
//! the affinity of a probe at NUMERIC (`sqlite3IndexAffinityStr`), which keeps
//! the integer exact. Constant expressions became seek keys in bd-aavtp, which
//! made `c >= '9007199254740993' || ''` a wrong-result regression from v0.4.12.
//!
//! `seek_probes_match_stock` compares each statement with rusqlite's bundled
//! SQLite. It covers:
//! - REAL, INTEGER and NUMERIC columns holding integers and reals near 2^53
//!   and the i64 limits;
//! - literal, text, constant-expression and bound probes;
//! - equality, ranges, BETWEEN, IN, composite keys, aggregates, min/max,
//!   UPDATE and DELETE.
//!
//! Each runs in memory and in a file, ad hoc and prepared.
//!
//! `planted_rounded_probe_is_reported` feeds the comparison the probe REAL
//! affinity rounds to, so a comparison that cannot see the defect fails.

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

/// The first `n` of the parameters statements bind: `?1` and `?2` the integer
/// past 2^53 and its text, `?3` the real it rounds to, `?4` and `?5` the i64
/// maximum and its text.
fn f_params(n: usize) -> Vec<SqliteValue> {
    let mut params = vec![
        SqliteValue::Integer(9_007_199_254_740_993),
        SqliteValue::Text("9007199254740993".into()),
        SqliteValue::Float(9_007_199_254_740_992.0),
        SqliteValue::Integer(i64::MAX),
        SqliteValue::Text("9223372036854775807".into()),
    ];
    params.truncate(n);
    params
}

fn r_params(n: usize) -> Vec<rusqlite::types::Value> {
    let mut params = vec![
        rusqlite::types::Value::Integer(9_007_199_254_740_993),
        rusqlite::types::Value::Text("9007199254740993".to_owned()),
        rusqlite::types::Value::Real(9_007_199_254_740_992.0),
        rusqlite::types::Value::Integer(i64::MAX),
        rusqlite::types::Value::Text("9223372036854775807".to_owned()),
    ];
    params.truncate(n);
    params
}

fn rq(conn: &rusqlite::Connection, sql: &str, bind: usize) -> Result<Vec<Vec<String>>, ()> {
    let mut st = conn.prepare(sql).map_err(|_| ())?;
    let n = st.column_count();
    st.query_map(rusqlite::params_from_iter(r_params(bind)), |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .map_err(|_| ())?
    .collect::<Result<Vec<_>, _>>()
    .map_err(|_| ())
}

async fn fq(
    conn: &Connection,
    sql: &str,
    bind: usize,
    prepared: bool,
) -> Result<Vec<Vec<String>>, ()> {
    let params = f_params(bind);
    let rows = match (prepared, bind > 0) {
        (false, false) => conn.query(sql).await,
        (false, true) => conn.query_with_params(sql, &params).await,
        (true, _) => match conn.prepare(sql).await {
            Ok(stmt) if bind == 0 => stmt.query().await,
            Ok(stmt) => stmt.query_with_params(&params).await,
            Err(e) => Err(e),
        },
    };
    // An error compares by its presence: the messages differ between engines.
    rows.map(|rows| {
        rows.iter()
            .map(|r| r.values().iter().map(tag_f).collect())
            .collect()
    })
    .map_err(|_| ())
}

/// `c`, `i` and `d` take REAL, INTEGER and NUMERIC affinity. Their rows sit
/// on both sides of the probes, each column has an index of its own and one
/// behind `k`, and `n` counts the updates.
const SCHEMA: &str = "\
CREATE TABLE t(id INTEGER PRIMARY KEY, c REAL, i INTEGER, d NUMERIC, k INTEGER, n INTEGER);\
INSERT INTO t VALUES \
  (1, 9007199254740992.0, 9007199254740992, 9007199254740992, 1, 0), \
  (2, 9007199254740994.0, 9007199254740993, 9007199254740993, 1, 0), \
  (3, 9007199254740996.0, 9007199254740994, 9007199254740994.0, 1, 0), \
  (4, -9007199254740992.0, -9007199254740993, -9007199254740993, 1, 0), \
  (5, 9223372036854775807.0, 9223372036854775807, 9223372036854775807, 2, 0), \
  (6, 9223372036854774784.0, 9223372036854775806, 9223372036854774784.0, 2, 0), \
  (7, -9223372036854775808.0, -9223372036854775808, -9223372036854775808, 2, 0), \
  (8, 5, 5, 5, 3, 0), \
  (9, 5.5, 5.5, 5.5, 3, 0);\
CREATE INDEX t_c ON t(c);\
CREATE INDEX t_i ON t(i);\
CREATE INDEX t_d ON t(d);\
CREATE INDEX t_kc ON t(k, c);\
CREATE INDEX t_ki ON t(k, i);\
CREATE INDEX t_kd ON t(k, d);";

const COLUMNS: &[&str] = &["c", "i", "d"];

const PROBES: &[&str] = &[
    "9007199254740993",
    "9007199254740992.0",
    "'9007199254740993'",
    "'9007199254740992.0'",
    "' 9007199254740993'",
    "9223372036854775807",
    "'9223372036854775807'",
    "'9223372036854775808'",
    "-9223372036854775808",
    "9223372036854775808.0",
    "'5'",
    "'5.5'",
    "'abc'",
    "'9007199254740993' || ''",
    "'9223372036854775807' || ''",
    "9007199254740992 + 1",
    "abs(-9007199254740993)",
    "'5' || ''",
    "?1",
    "?2",
    "?3",
    "?4",
    "?5",
];

const SELECT_FORMS: &[&str] = &[
    "SELECT id FROM t WHERE {c} = {p} ORDER BY id",
    "SELECT id FROM t WHERE {c} > {p} ORDER BY id",
    "SELECT id FROM t WHERE {c} >= {p} ORDER BY id",
    "SELECT id FROM t WHERE {c} < {p} ORDER BY id",
    "SELECT id FROM t WHERE {c} <= {p} ORDER BY id",
    "SELECT id FROM t WHERE {c} >= {p} ORDER BY {c} DESC, id",
    "SELECT id FROM t WHERE {c} <= {p} ORDER BY {c} DESC, id",
    "SELECT id FROM t WHERE {c} BETWEEN {p} AND {p} ORDER BY id",
    "SELECT id FROM t WHERE {c} IN ({p}) ORDER BY id",
    "SELECT id FROM t WHERE {c} IN ({p}, 5) ORDER BY id",
    "SELECT id FROM t WHERE k = 1 AND {c} = {p} ORDER BY id",
    "SELECT id FROM t WHERE k = 1 AND {c} >= {p} ORDER BY id",
    "SELECT id FROM t WHERE k = 1 AND {c} < {p} ORDER BY id",
    "SELECT id FROM t WHERE k = 1 AND {c} BETWEEN {p} AND {p} ORDER BY id",
    "SELECT count(*) FROM t WHERE {c} = {p}",
    "SELECT count(*) FROM t WHERE {c} >= {p}",
    "SELECT min({c}) FROM t WHERE {c} >= {p}",
    "SELECT max({c}) FROM t WHERE {c} <= {p}",
    "SELECT count(*), sum(n) FROM t WHERE k = 1 AND {c} >= {p}",
    "SELECT min({c}) FROM t WHERE k = 1 AND {c} >= {p}",
    "SELECT max({c}) FROM t WHERE k = 1 AND {c} <= {p}",
    "SELECT EXISTS(SELECT 1 FROM t WHERE {c} = {p})",
];

const DML_FORMS: &[&str] = &[
    "UPDATE t SET n = n + 1 WHERE {c} = {p}",
    "UPDATE t SET n = n + 1 WHERE {c} >= {p}",
    "UPDATE t SET n = n + 1 WHERE {c} IN ({p})",
    "UPDATE t SET n = n + 1 WHERE k = 1 AND {c} = {p}",
    "DELETE FROM t WHERE {c} = {p}",
    "DELETE FROM t WHERE {c} >= {p}",
    "DELETE FROM t WHERE k = 1 AND {c} < {p}",
];

const DML_CHECK: &str = "SELECT id, c, i, d, k, n FROM t ORDER BY id";

/// A row-level difference between `frank_sql` here and `stock_sql` in stock.
async fn select_diff(
    f: &Connection,
    r: &rusqlite::Connection,
    frank_sql: &str,
    stock_sql: &str,
    bind: usize,
    prepared: bool,
) -> Option<String> {
    let frank = fq(f, frank_sql, bind, prepared).await;
    let stock = rq(r, stock_sql, bind);
    (frank != stock).then(|| format!("frank= {frank:?}\n     stock= {stock:?}"))
}

async fn dml_diff(
    f: &Connection,
    r: &rusqlite::Connection,
    sql: &str,
    bind: usize,
    prepared: bool,
) -> Option<String> {
    f.execute("BEGIN").await.unwrap();
    r.execute_batch("BEGIN").unwrap();
    let stock = r
        .execute(sql, rusqlite::params_from_iter(r_params(bind)))
        .map_err(|_| ());
    let values = f_params(bind);
    let frank = if prepared {
        match f.prepare(sql).await {
            Ok(stmt) if bind > 0 => stmt.execute_with_params(&values).await,
            Ok(stmt) => stmt.execute().await,
            Err(e) => Err(e),
        }
    } else if bind > 0 {
        f.execute_with_params(sql, &values).await
    } else {
        f.execute(sql).await
    }
    .map_err(|_| ());
    let mut diff = (frank != stock).then(|| format!("changes: frank= {frank:?} stock= {stock:?}"));
    if diff.is_none() {
        diff = select_diff(f, r, DML_CHECK, DML_CHECK, 0, false).await;
    }
    f.execute("ROLLBACK").await.unwrap();
    r.execute_batch("ROLLBACK").unwrap();
    diff
}

async fn compare(file_backed: bool) -> (Vec<String>, usize) {
    let dir = tempfile::tempdir().unwrap();
    let (f, r) = if file_backed {
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
    f.execute_batch(SCHEMA).await.unwrap();
    r.execute_batch(SCHEMA).unwrap();
    let db = if file_backed { "file" } else { "memory" };
    let mut sqls = Vec::new();
    for probe in PROBES {
        for column in COLUMNS {
            for form in SELECT_FORMS.iter().chain(DML_FORMS) {
                sqls.push(form.replace("{c}", column).replace("{p}", probe));
            }
        }
    }
    let mut diffs = Vec::new();
    let mut runs = 0usize;
    for sql in &sqls {
        // Bind as many parameters as the statement declares, as rusqlite requires.
        let bind = r.prepare(sql).map_or(0, |st| st.parameter_count());
        for prepared in [false, true] {
            runs += 1;
            let diff = if sql.starts_with("SELECT") {
                select_diff(&f, &r, sql, sql, bind, prepared).await
            } else {
                dml_diff(&f, &r, sql, bind, prepared).await
            };
            if let Some(diff) = diff {
                diffs.push(format!("  [{db}, prepared={prepared}] `{sql}`\n     {diff}"));
            }
        }
    }
    f.close().await.unwrap();
    (diffs, runs)
}

#[test]
fn seek_probes_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let (mut diffs, mut runs) = compare(false).await;
        let (file_diffs, file_runs) = compare(true).await;
        diffs.extend(file_diffs);
        runs += file_runs;
        assert!(
            diffs.is_empty(),
            "{} divergence(s) over {runs} statement run(s) from rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}

#[test]
fn planted_rounded_probe_is_reported() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        f.execute_batch(SCHEMA).await.unwrap();
        r.execute_batch(SCHEMA).unwrap();
        let stock = "SELECT id FROM t WHERE c >= '9007199254740993' || '' ORDER BY id";
        let rounded = "SELECT id FROM t WHERE c >= 9007199254740992.0 ORDER BY id";
        assert!(
            select_diff(&f, &r, rounded, stock, 0, false).await.is_some(),
            "the comparison must report the row a rounded probe adds"
        );
        assert_eq!(select_diff(&f, &r, stock, stock, 0, false).await, None);
        f.close().await.unwrap();
    });
}
