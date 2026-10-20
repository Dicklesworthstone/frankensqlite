#![recursion_limit = "512"]

//! bd-gtjlv follow-up: an equality scan with a residual whose further key
//! terms are pinned by BIND PARAMETERS (`a = ?1 AND k = ?2 AND v > 0` on
//! `(a, k)`) probed only the leading column, and every such scan, literal or
//! not, ended only where the leading term's block ended, walking and filtering
//! every later entry of it. Seek-safe parameters now join the probe prefix
//! (coerced to the column affinity, as the equality filter coerces them), and
//! the scan stops at the end of the whole pinned prefix.
//!
//! Results are compared with rusqlite for binds of every storage class,
//! in memory and file-backed; the probe width and the block-end compare width
//! are read from EXPLAIN.

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
    "CREATE TABLE n(id INTEGER PRIMARY KEY, a INTEGER, k TEXT, v)",
    "CREATE INDEX n_ak ON n(a, k)",
    "CREATE TABLE m(id INTEGER PRIMARY KEY, a INTEGER, k TEXT COLLATE NOCASE, x, v)",
    "CREATE INDEX m_ak ON m(a, k)",
    "CREATE INDEX m_ax ON m(a, x)",
    "CREATE TABLE u(id INTEGER PRIMARY KEY, a INTEGER, x, v)",
    "CREATE INDEX u_ax ON u(a, x)",
    "CREATE TABLE w(t TEXT NOT NULL, f TEXT NOT NULL, art TEXT NOT NULL, obj TEXT NOT NULL, \
     at INTEGER NOT NULL, v, PRIMARY KEY (t, f, art, obj, at))",
    "WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 400) \
     INSERT INTO n SELECT i, i % 4, CASE WHEN i % 10 = 0 THEN NULL ELSE 'k' || (i % 50) END, i FROM s",
    "INSERT INTO n VALUES (1001, 1, '7', 'text7'), (1002, 1, 7, 'int7'), (1003, 1, 'K1', 'upper'), \
     (1004, 1, x'6b3231', 'blob'), (1005, 1, 7.0, 'real7')",
    "WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 200) \
     INSERT INTO m SELECT i, i % 3, CASE i % 4 WHEN 0 THEN 'k1' WHEN 1 THEN 'K1' ELSE 'k2' END, \
     CASE i % 5 WHEN 0 THEN 7 WHEN 1 THEN '7' ELSE 'x' END, i FROM s",
    "INSERT INTO u SELECT id, a, x, v FROM m",
    "WITH RECURSIVE s(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM s WHERE i < 599) \
     INSERT INTO w SELECT 'T' || (i % 2), 'fam', 'art_' || (i / 100), 'obj_' || i, 1000 + i, i FROM s",
];

fn int(n: i64) -> SqliteValue {
    SqliteValue::Integer(n)
}

fn text(s: &str) -> SqliteValue {
    SqliteValue::Text(s.to_owned().into())
}

/// `(query, binds, probe fields, block-end compare fields)`. Rows are compared
/// as sorted sets (no ORDER BY or rowid predicate, so the index scan is the plan).
fn cases() -> Vec<(&'static str, Vec<SqliteValue>, usize, usize)> {
    let ak = "SELECT id, v FROM n WHERE a = ?1 AND k = ?2 AND v IS NOT NULL";
    vec![
        // A text bind on a BINARY TEXT term joins the prefix; the scan ends with it.
        (ak, vec![int(1), text("k21")], 2, 2),
        (ak, vec![int(1), text("nope")], 2, 2),
        (ak, vec![int(9), text("k21")], 2, 2),
        (
            "SELECT id, v FROM n WHERE k = ?2 AND a = ?1 AND v IS NOT NULL",
            vec![int(1), text("k21")],
            2,
            2,
        ),
        // Other storage classes are coerced like the filter coerces them:
        // an integer becomes '7' (the '7' rows), a real becomes '7.0' (the
        // real row stored as text), a blob stays a blob (the blob row only),
        // NULL matches nothing.
        (ak, vec![int(1), int(7)], 2, 2),
        (ak, vec![int(1), SqliteValue::Float(7.0)], 2, 2),
        (ak, vec![int(1), SqliteValue::Blob(b"k21".to_vec().into())], 2, 2),
        (ak, vec![int(1), SqliteValue::Null], 2, 2),
        // A leading bind the column would convert takes the run-time
        // fallback (same program; the rows must still match).
        (ak, vec![text("1"), text("k21")], 2, 2),
        // NOCASE stays out of the prefix.
        (
            "SELECT id, v FROM m WHERE a = ?1 AND k = ?2 AND v > 0",
            vec![int(1), text("k1")],
            1,
            1,
        ),
        // On m the planner picks m_ak, whose second term this WHERE does not
        // pin, so the leading term probes alone.
        (
            "SELECT id, v FROM m WHERE a = ?1 AND x = ?2 AND v > 0",
            vec![int(1), int(7)],
            1,
            1,
        ),
        // A typeless column compares raw, so a bind there is seek-safe
        // without coercion: 7 finds the integer rows, '7' the text rows.
        (
            "SELECT id, v FROM u WHERE a = ?1 AND x = ?2 AND v > 0",
            vec![int(1), int(7)],
            2,
            2,
        ),
        (
            "SELECT id, v FROM u WHERE a = ?1 AND x = ?2 AND v > 0",
            vec![int(1), text("7")],
            2,
            2,
        ),
        // The FLDT shape: a composite primary key probed by a 3- and a
        // 4-column bound prefix with a residual.
        (
            "SELECT obj, v FROM w WHERE t = ?1 AND f = ?2 AND art = ?3 AND v >= 0",
            vec![text("T1"), text("fam"), text("art_3")],
            3,
            3,
        ),
        (
            "SELECT obj, v FROM w WHERE t = ?1 AND f = ?2 AND art = ?3 AND obj = ?4 AND v >= 0",
            vec![text("T0"), text("fam"), text("art_2"), text("obj_250")],
            4,
            4,
        ),
        (
            "SELECT obj, v FROM w WHERE t = ?1 AND f = ?2 AND art = ?3 AND obj = ?4 AND v >= 0",
            vec![text("T1"), text("fam"), text("art_2"), text("obj_250")],
            4,
            4,
        ),
        // A leading literal the column converts is not exact and has no run-
        // time check, so an empty narrowed block is not authoritative: the
        // scan keeps ending at the leading term's block (no new full-scan
        // fallback where it used to finish that block).
        (
            "SELECT id, v FROM n WHERE a = '1' AND k = 'k21' AND v > 0",
            vec![],
            2,
            1,
        ),
        // A literal prefix keeps its probe and now ends with it too.
        (
            "SELECT id, v FROM n WHERE a = 1 AND k = 'k21' AND v > 0",
            vec![],
            2,
            2,
        ),
    ]
}

fn to_rusqlite(value: &SqliteValue) -> rusqlite::types::Value {
    match value {
        SqliteValue::Null => rusqlite::types::Value::Null,
        SqliteValue::Integer(n) => rusqlite::types::Value::Integer(*n),
        SqliteValue::Float(f) => rusqlite::types::Value::Real(*f),
        SqliteValue::Text(s) => rusqlite::types::Value::Text(s.to_string()),
        SqliteValue::Blob(b) => rusqlite::types::Value::Blob(b.to_vec()),
    }
}

async fn rows_f(conn: &Connection, sql: &str, binds: &[SqliteValue]) -> Vec<Vec<String>> {
    conn.query_with_params(sql, binds)
        .await
        .unwrap_or_else(|e| panic!("franken `{sql}`: {e:?}"))
        .iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect()
}

fn rows_r(conn: &rusqlite::Connection, sql: &str, binds: &[SqliteValue]) -> Vec<Vec<String>> {
    let mut stmt = conn.prepare(sql).expect("rusqlite prepare");
    let ncol = stmt.column_count();
    let params: Vec<rusqlite::types::Value> = binds.iter().map(to_rusqlite).collect();
    stmt.query_map(rusqlite::params_from_iter(params), |row| {
        Ok((0..ncol)
            .map(|i| tag_r(&row.get::<_, rusqlite::types::Value>(i).unwrap()))
            .collect())
    })
    .expect("rusqlite query")
    .map(|r| r.unwrap())
    .collect()
}

fn ops(explain: &str) -> Vec<Vec<&str>> {
    explain
        .lines()
        .map(|line| line.split_whitespace().collect::<Vec<_>>())
        .filter(|fields| fields.len() >= 5 && fields[0].parse::<usize>().is_ok())
        .collect()
}

/// The field count of the record the index `SeekGE` probes with.
fn seek_probe_fields(explain: &str) -> Option<usize> {
    let ops = ops(explain);
    // `SeekGE cursor jump record`, `MakeRecord first count record`.
    let seek = ops.iter().position(|op| op[1] == "SeekGE")?;
    let record_reg = ops[seek][4];
    ops[..seek]
        .iter()
        .rev()
        .find(|op| op[1] == "MakeRecord" && op[4] == record_reg)
        .and_then(|op| op[3].parse().ok())
}

/// The key fields the scan's block-end `IdxGT` compares (its p5).
fn block_end_fields(explain: &str) -> Option<usize> {
    let ops = ops(explain);
    let seek = ops.iter().position(|op| op[1] == "SeekGE")?;
    let record_reg = ops[seek][4];
    ops[seek..]
        .iter()
        .find(|op| op[1] == "IdxGT" && op[4] == record_reg)
        // p5 is the line's last column (`addr opcode p1 p2 p3 p4 p5`; an
        // empty p4 leaves no field of its own).
        .and_then(|op| op.last().and_then(|p5| p5.parse().ok()))
}

#[test]
fn composite_equality_scans_seek_and_end_with_the_bound_prefix() {
    if std::env::var_os("BD_GTJLV_PRINT_EXPLAIN").is_some() {
        asupersync::test_utils::run_test(|| async move {
            let f = Connection::open(":memory:").await.unwrap();
            for sql in SETUP {
                f.execute(sql).await.unwrap();
            }
            let (sql, _, _, _) = &cases()[0];
            println!("{}", f.prepare(sql).await.unwrap().explain());
        });
        return;
    }
    for file_backed in [false, true] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let target = if file_backed {
                dir.path()
                    .join("bd_gtjlv_param.db")
                    .to_string_lossy()
                    .into_owned()
            } else {
                ":memory:".to_owned()
            };
            let f = Connection::open(&target).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            for sql in SETUP {
                f.execute(sql).await.unwrap();
                r.execute(sql, []).unwrap();
            }
            for (sql, binds, probe, block_end) in cases() {
                let mut franken = rows_f(&f, sql, &binds).await;
                let mut stock = rows_r(&r, sql, &binds);
                franken.sort();
                stock.sort();
                assert_eq!(franken, stock, "query `{sql}` binds {binds:?}");
                let explain = f.prepare(sql).await.unwrap().explain();
                assert_eq!(
                    seek_probe_fields(&explain),
                    Some(probe),
                    "probe width of `{sql}`:\n{explain}"
                );
                assert_eq!(
                    block_end_fields(&explain),
                    Some(block_end),
                    "block-end width of `{sql}`:\n{explain}"
                );
            }
        });
    }
}
