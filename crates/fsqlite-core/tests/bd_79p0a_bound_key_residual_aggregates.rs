#![allow(
    clippy::await_holding_lock,
    reason = "the process-global VDBE opcode counter needs each test serialized"
)]

//! bd-79p0a: an aggregate whose WHERE pins an index prefix with a bound key
//! and has a residual term, `SELECT id, count(*) FROM t WHERE a = ?1 AND
//! n = ?2`, scanned the table in rowid order. The equality-prefix seek its
//! literal spelling takes admitted only literal keys. So the bound form
//! visited every row, and a bare column took the first matching row by rowid
//! where stock, which seeks `t_ab`, takes the first in index order. The seek
//! now takes a numbered parameter key, exact at run time when the bound value
//! holds the storage class the column's affinity leaves unconverted (an
//! integer for INTEGER, text for BINARY TEXT), as the whole-WHERE equality
//! seek does (bd-aavtp). Any other value scans, as before.
//!
//! - `bound_keys_with_a_residual_match_stock` compares, with rusqlite's
//!   bundled SQLite, bare values included, each result list over each WHERE
//!   form spelled with bound keys (prepared and ad hoc) and with literals (ad
//!   hoc), in memory and in a file.
//! - `typed_bound_keys_match_stock` binds a text key on a TEXT index (exact,
//!   bare values compared) and keys of another storage class on the INTEGER
//!   index: '5', 5.0, NULL, 'x'. Those take the scan, and only aggregates
//!   are compared: stock converts '5' to 5 and seeks, so its bare columns
//!   come from the index order, while the scan visits rows by rowid. That
//!   gap is shared with the whole-WHERE seek and is not fixed here. A seek
//!   that trusted its miss for '5' or 5.0 would answer count 0; stock
//!   answers 7.
//! - `bound_keys_with_a_residual_seek` bounds the opcodes the bound forms run
//!   over 1,000 rows, which a scan exceeds many times over, with a scan
//!   control.

use std::sync::{Mutex, MutexGuard};

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;
use fsqlite_vdbe::engine::{reset_vdbe_metrics, set_vdbe_metrics_enabled, vdbe_metrics_snapshot};

static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// One bound value, bound the same way on both engines.
#[derive(Clone, Copy, Debug)]
enum Bind {
    Int(i64),
    Real(f64),
    Text(&'static str),
    Null,
}

impl Bind {
    fn fsqlite(self) -> SqliteValue {
        match self {
            Self::Int(v) => SqliteValue::Integer(v),
            Self::Real(v) => SqliteValue::Float(v),
            Self::Text(v) => SqliteValue::Text(v.into()),
            Self::Null => SqliteValue::Null,
        }
    }

    fn stock(self) -> rusqlite::types::Value {
        match self {
            Self::Int(v) => rusqlite::types::Value::Integer(v),
            Self::Real(v) => rusqlite::types::Value::Real(v),
            Self::Text(v) => rusqlite::types::Value::Text(v.to_owned()),
            Self::Null => rusqlite::types::Value::Null,
        }
    }
}

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

fn rq(conn: &rusqlite::Connection, sql: &str, binds: &[Bind]) -> Vec<Vec<String>> {
    let mut st = conn.prepare(sql).unwrap();
    let n = st.column_count();
    st.query_map(
        rusqlite::params_from_iter(binds.iter().map(|b| b.stock())),
        |row| {
            Ok((0..n)
                .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect())
        },
    )
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

async fn fq(conn: &Connection, sql: &str, binds: &[Bind], prepared: bool) -> Vec<Vec<String>> {
    let params: Vec<SqliteValue> = binds.iter().map(|b| b.fsqlite()).collect();
    let rows = match (prepared, params.is_empty()) {
        (true, true) => conn.prepare(sql).await.unwrap().query().await,
        (true, false) => {
            conn.prepare(sql)
                .await
                .unwrap()
                .query_with_params(&params)
                .await
        }
        (false, true) => conn.query(sql).await,
        (false, false) => conn.query_with_params(sql, &params).await,
    };
    rows.unwrap_or_else(|e| panic!("`{sql}` {binds:?}: {e}"))
        .iter()
        .map(|r| r.values().iter().map(tag_f).collect())
        .collect()
}

/// The schema of `bd_d4rbi_bare_column_aggregate_seeks`: `a`, `b` and `c` are
/// not monotone in the rowid, so the first row in index order is not the
/// first in rowid order; `b` has NULLs and ties.
const SCHEMA: &str = "\
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c TEXT, n INTEGER);\
INSERT INTO t WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 400) \
  SELECT i, i % 11, CASE WHEN i % 29 = 0 THEN NULL ELSE (i * 7) % 17 END, \
  'v' || ((i * 5) % 23), i % 5 FROM s;\
CREATE INDEX t_ab ON t(a, b);\
CREATE INDEX t_b ON t(b);\
CREATE INDEX t_c ON t(c);";

/// Result lists: bare columns next to aggregates.
const LISTS: &[&str] = &[
    "id, b, c, count(*)",
    "b, count(*)",
    "id, count(*)",
    "count(*), c",
    "n, sum(b)",
    "id, b, c, n, count(*), sum(n)",
    "id, min(b)",
    "id, c, min(n)",
    "c, min(b), max(b)",
];

/// (WHERE with bound keys, the same WHERE with literals, the bound values).
/// The first two and the last are the bd-79p0a shapes; the others had no
/// divergence and stay as controls.
const WHERES: &[(&str, &str, &[Bind])] = &[
    (
        "a = ?1 AND n = ?2",
        "a = 5 AND n = 2",
        &[Bind::Int(5), Bind::Int(2)],
    ),
    (
        "a = ?1 AND c LIKE 'v1%'",
        "a = 5 AND c LIKE 'v1%'",
        &[Bind::Int(5)],
    ),
    (
        "a = ?1 AND b = ?2",
        "a = 5 AND b = 3",
        &[Bind::Int(5), Bind::Int(3)],
    ),
    ("a = ?1", "a = 5", &[Bind::Int(5)]),
    (
        "a = ?1 AND b > ?2",
        "a = 5 AND b > 6",
        &[Bind::Int(5), Bind::Int(6)],
    ),
    (
        "id = ?1 AND n >= ?2",
        "id = 7 AND n >= 0",
        &[Bind::Int(7), Bind::Int(0)],
    ),
    (
        "a = ?1 AND b = ?2 AND n < ?3",
        "a = 5 AND b = 3 AND n < 4",
        &[Bind::Int(5), Bind::Int(3), Bind::Int(4)],
    ),
];

async fn open_pair(dir: &tempfile::TempDir, file_backed: bool) -> (Connection, rusqlite::Connection) {
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
    (f, r)
}

#[test]
fn bound_keys_with_a_residual_match_stock() {
    let _serial = serial();
    asupersync::test_utils::run_test(|| async {
        let mut diffs = Vec::new();
        let mut runs = 0_usize;
        for file_backed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let (f, r) = open_pair(&dir, file_backed).await;
            let db = if file_backed { "file" } else { "memory" };
            for list in LISTS {
                for (bound, literal, binds) in WHERES {
                    let bound_sql = format!("SELECT {list} FROM t WHERE {bound}");
                    let literal_sql = format!("SELECT {list} FROM t WHERE {literal}");
                    let stock = rq(&r, &bound_sql, binds);
                    let stock_literal = rq(&r, &literal_sql, &[]);
                    for (label, sql, want, got) in [
                        (
                            "prepared bound",
                            &bound_sql,
                            &stock,
                            fq(&f, &bound_sql, binds, true).await,
                        ),
                        (
                            "ad hoc bound",
                            &bound_sql,
                            &stock,
                            fq(&f, &bound_sql, binds, false).await,
                        ),
                        (
                            "ad hoc literal",
                            &literal_sql,
                            &stock_literal,
                            fq(&f, &literal_sql, &[], false).await,
                        ),
                    ] {
                        runs += 1;
                        if &got != want {
                            diffs.push(format!(
                                "  [{db}, {label}] `{sql}` {binds:?}\n     frank= {got:?}\n     stock= {want:?}"
                            ));
                        }
                    }
                }
            }
            f.close().await.unwrap();
        }
        assert!(
            diffs.is_empty(),
            "{} divergence(s) over {runs} statement run(s) from rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}

/// Aggregates only: the lists `typed_bound_keys_match_stock` compares for a
/// key the seek is not exact for.
const AGGREGATE_LISTS: &[&str] = &[
    "count(*)",
    "sum(n), count(*)",
    "min(n), max(b)",
    "total(b), avg(n)",
];

#[test]
fn typed_bound_keys_match_stock() {
    // A text key on the BINARY TEXT index `t_c` is exact: bare values compared.
    const EXACT: &[(&str, &[Bind])] = &[
        ("c = ?1 AND n = ?2", &[Bind::Text("v14"), Bind::Int(2)]),
        ("c = ?1 AND a > ?2", &[Bind::Text("v3"), Bind::Int(4)]),
    ];
    // Keys of another storage class on the INTEGER index `t_ab`, and a miss.
    const OTHER_CLASS: &[(&str, &[Bind])] = &[
        ("a = ?1 AND n = ?2", &[Bind::Text("5"), Bind::Int(2)]),
        ("a = ?1 AND n = ?2", &[Bind::Real(5.0), Bind::Int(2)]),
        ("a = ?1 AND n = ?2", &[Bind::Null, Bind::Int(2)]),
        ("a = ?1 AND n = ?2", &[Bind::Text("x"), Bind::Int(2)]),
        ("a = ?1 AND n = ?2", &[Bind::Int(99), Bind::Int(2)]),
        ("a = ?1 AND c LIKE 'v1%'", &[Bind::Text("5")]),
        ("a = ?1 AND c LIKE 'v1%'", &[Bind::Real(5.5)]),
        ("a = ?1 AND b = ?2 AND n < ?3", &[Bind::Int(5), Bind::Text("3"), Bind::Int(4)]),
    ];
    let _serial = serial();
    asupersync::test_utils::run_test(|| async {
        let mut diffs = Vec::new();
        let mut runs = 0_usize;
        for file_backed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let (f, r) = open_pair(&dir, file_backed).await;
            let db = if file_backed { "file" } else { "memory" };
            let cases = EXACT
                .iter()
                .flat_map(|(wh, binds)| LISTS.iter().map(move |list| (*list, *wh, *binds)))
                .chain(OTHER_CLASS.iter().flat_map(|(wh, binds)| {
                    AGGREGATE_LISTS.iter().map(move |list| (*list, *wh, *binds))
                }));
            for (list, wh, binds) in cases {
                let sql = format!("SELECT {list} FROM t WHERE {wh}");
                let want = rq(&r, &sql, binds);
                for prepared in [false, true] {
                    runs += 1;
                    let got = fq(&f, &sql, binds, prepared).await;
                    if got != want {
                        diffs.push(format!(
                            "  [{db}, prepared={prepared}] `{sql}` {binds:?}\n     frank= {got:?}\n     stock= {want:?}"
                        ));
                    }
                }
            }
            f.close().await.unwrap();
        }
        assert!(
            diffs.is_empty(),
            "{} divergence(s) over {runs} statement run(s) from rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}

/// Rows in the cost table.
const COST_ROWS: u64 = 1000;

/// The opcodes one statement may run. A seek below visits at most a dozen
/// rows; a scan of the table runs over 10,000.
const COST_LIMIT: u64 = 1000;

#[test]
fn bound_keys_with_a_residual_seek() {
    // Each shape seeks; the control must scan.
    const SHAPES: &[(&str, &[Bind])] = &[
        ("SELECT id, count(*) FROM t WHERE a = ?1 AND n > 0", &[Bind::Int(7)]),
        ("SELECT count(*) FROM t WHERE a = ?1 AND n > 0", &[Bind::Int(7)]),
        ("SELECT sum(n) FROM t WHERE a = ?1 AND s LIKE 'v%'", &[Bind::Int(7)]),
        (
            "SELECT b, sum(n) FROM t WHERE a = ?1 AND b = ?2 AND n > ?3",
            &[Bind::Int(7), Bind::Int(0), Bind::Int(0)],
        ),
        ("SELECT id, count(*) FROM t WHERE s = ?1 AND n > ?2", &[Bind::Text("v7"), Bind::Int(0)]),
    ];
    const SCAN_CONTROLS: &[(&str, &[Bind])] =
        &[("SELECT id, count(*) FROM t WHERE n + 0 = ?1", &[Bind::Int(5)])];
    let _serial = serial();
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let f = Connection::open(dir.path().join("cost.db").to_str().unwrap())
            .await
            .unwrap();
        f.execute_batch(&format!(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, s TEXT, n INTEGER);\
             INSERT INTO t WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s \
               WHERE i < {COST_ROWS}) SELECT i, i % 250, i % 7, 'v' || (i % 100), i \
               FROM s;\
             CREATE INDEX t_ab ON t(a, b);\
             CREATE INDEX t_s ON t(s);"
        ))
        .await
        .unwrap();
        let mut report = Vec::new();
        let mut over = Vec::new();
        for (sql, binds) in SHAPES.iter().chain(SCAN_CONTROLS) {
            let params: Vec<SqliteValue> = binds.iter().map(|b| b.fsqlite()).collect();
            set_vdbe_metrics_enabled(true);
            reset_vdbe_metrics();
            let result = f.query_with_params(sql, &params).await.map(|_| ());
            let opcodes = vdbe_metrics_snapshot().opcodes_executed_total;
            set_vdbe_metrics_enabled(false);
            result.unwrap();
            report.push(format!("  {opcodes:>10} `{sql}` {binds:?}"));
            let is_control = SCAN_CONTROLS.iter().any(|(control, _)| control == sql);
            if (opcodes > COST_LIMIT) != is_control {
                over.push(format!(
                    "  {opcodes} opcodes, control={is_control}: `{sql}` {binds:?}"
                ));
            }
        }
        f.close().await.unwrap();
        eprintln!(
            "opcodes per statement (limit {COST_LIMIT}):\n{}",
            report.join("\n")
        );
        assert!(
            over.is_empty(),
            "the seeks must stay under {COST_LIMIT} opcodes and the scan control over it:\n{}",
            over.join("\n")
        );
    });
}
