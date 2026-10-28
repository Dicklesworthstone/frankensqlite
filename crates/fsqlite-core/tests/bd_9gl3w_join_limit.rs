#![recursion_limit = "512"]

//! bd-9gl3w: a two-table join with a LIMIT and no ORDER BY ran the whole join
//! in the materializing executor and only then truncated it (80k rows: 42 ms
//! per `... JOIN k ON t.g = k.g LIMIT 5`, stock under 1 ms). The VDBE join
//! codegen now compiles LIMIT/OFFSET where it emits each row, and a lookup
//! join (a rowid or index seek per outer row) with a LIMIT runs there, so it
//! stops after its last row.
//!
//! The keepers compare rows, in order, with rusqlite's bundled SQLite for
//! lookup joins (INNER and LEFT, rowid, index and UNIQUE text seeks, WHERE
//! terms on both sides), a nested-loop join that keeps the materializing
//! executor, `*`, aliases, and every LIMIT form: zero, negative, past the end,
//! `LIMIT a, b`, expressions, a subquery, and parameters (anonymous ones
//! numbered in text order), ad hoc and prepared. The shapes are also checked
//! as correlated EXISTS probes, which now carry LIMIT 1 over a lookup join.
//! The opcode keeper pins the early exit: the hot-path profile counters are
//! process-global, so the tests here run one at a time.
//!
//! Left out, each diverging the same way before this change: a correlated
//! EXISTS with a WHERE term on the joined table (bd-jf4s4) or with an OFFSET
//! (bd-zje20), and `LIMIT ?, ?` (bd-s9i69). `k JOIN t ON t.g = k.g` is left
//! out because stock scans `t` first and fsqlite scans `k`.

use fsqlite_core::connection::{
    Connection, hot_path_profile_snapshot, reset_hot_path_profile, set_hot_path_profile_enabled,
};
use fsqlite_types::value::SqliteValue;

static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
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

type Rows = Result<Vec<Vec<String>>, String>;

fn tag_rows(rows: &[fsqlite_core::connection::Row]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|r| r.values().iter().map(tag_f).collect())
        .collect()
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

fn rq(conn: &rusqlite::Connection, sql: &str, params: &[i64]) -> Rows {
    let mut st = conn.prepare(sql).map_err(stock_err)?;
    let n = st.column_count();
    st.query_map(rusqlite::params_from_iter(params.iter()), |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .map_err(stock_err)?
    .collect::<Result<Vec<_>, _>>()
    .map_err(stock_err)
}

const SCHEMA: &str = "\
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, g INTEGER, n INTEGER);\
CREATE TABLE k(g INTEGER PRIMARY KEY, label TEXT);\
CREATE TABLE u(id INTEGER PRIMARY KEY, tid INTEGER, w TEXT);\
CREATE INDEX u_tid ON u(tid);\
CREATE TABLE p(id INTEGER PRIMARY KEY, code TEXT UNIQUE, note TEXT);\
CREATE TABLE q(x INTEGER, y TEXT);\
INSERT INTO t WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 60) \
  SELECT i, 'v' || (i % 7), i % 9, i % 5 FROM s;\
INSERT INTO k WITH RECURSIVE s(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM s WHERE i < 6) \
  SELECT i, 'k' || i FROM s;\
INSERT INTO u WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 40) \
  SELECT i, i % 25 + 3, 'w' || i FROM s;\
INSERT INTO p VALUES (1, 'v3', 'p3'), (2, 'v0', 'p0'), (3, 'v4', 'p4'), (4, 'v1', 'p1');\
INSERT INTO q VALUES (2, 'q2a'), (4, 'q4'), (2, 'q2b'), (0, 'q0');";

/// Join shapes, and whether a correlated EXISTS probes each. Stock scans `t`
/// as the outer table of every one (EXPLAIN QUERY PLAN), so their unordered
/// rows have one order to compare.
const SHAPES: &[(&str, bool)] = &[
    ("SELECT t.id, k.label FROM t JOIN k ON t.g = k.g", true),
    ("SELECT t.id, k.label FROM t LEFT JOIN k ON t.g = k.g", true),
    ("SELECT t.id, u.w FROM t LEFT JOIN u ON u.tid = t.id", true),
    ("SELECT t.id, p.note FROM t JOIN p ON p.code = t.v", true),
    // bd-jf4s4: its EXISTS form tests the joined table.
    (
        "SELECT t.id, k.label FROM t JOIN k ON t.g = k.g WHERE t.n > 1 AND k.label <> 'k3'",
        false,
    ),
    (
        "SELECT a.id, b.label FROM t AS a LEFT JOIN k AS b ON b.g = a.g WHERE a.n <> 2",
        true,
    ),
    ("SELECT * FROM t JOIN k ON t.g = k.g", true),
    // No index on q.x: a nested loop, which keeps the materializing executor.
    ("SELECT t.id, q.y FROM t JOIN q ON q.x = t.n", true),
];

/// The LIMIT forms an EXISTS probe also takes; none has an OFFSET past the
/// first row (bd-zje20). The empty one gets EXISTS's own LIMIT 1.
const EXISTS_LIMITS: &[&str] = &[
    "",
    "LIMIT 1",
    "LIMIT 3",
    "LIMIT 0",
    "LIMIT -1",
    "LIMIT 4 OFFSET -3",
    "LIMIT '3'",
    "LIMIT (SELECT 2)",
];

const LIMITS: &[&str] = &[
    "LIMIT 3",
    "LIMIT 3 OFFSET 4",
    "LIMIT 1",
    "LIMIT 0",
    "LIMIT -1",
    "LIMIT -1 OFFSET 50",
    "LIMIT 5 OFFSET 1000",
    "LIMIT 4 OFFSET -3",
    "LIMIT 2, 3",
    "LIMIT 1 + 2 OFFSET 2 * 2",
    "LIMIT '3'",
    "LIMIT (SELECT 2) OFFSET 1",
];

async fn open_pair(
    file_backed: bool,
    dir: &tempfile::TempDir,
) -> (Connection, rusqlite::Connection) {
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

async fn compare_limits(file_backed: bool) -> (Vec<String>, usize) {
    let dir = tempfile::tempdir().unwrap();
    let (f, r) = open_pair(file_backed, &dir).await;
    let label = if file_backed { "file" } else { "memory" };
    let mut diffs = Vec::new();
    let mut statements = 0usize;
    let mut statements_sql = Vec::new();
    for &(shape, probe_with_exists) in SHAPES {
        statements_sql.extend(LIMITS.iter().map(|limit| format!("{shape} {limit}")));
        if !probe_with_exists {
            continue;
        }
        // The EXISTS probe keeps the shape's rows up to the outer row's id.
        let inner_id = if shape.contains(" AS a ") {
            "a.id"
        } else {
            "t.id"
        };
        let keyword = if shape.contains(" WHERE ") {
            "AND"
        } else {
            "WHERE"
        };
        let correlated = format!("{shape} {keyword} {inner_id} <= o.id");
        statements_sql.extend(EXISTS_LIMITS.iter().map(|limit| {
            format!("SELECT id, EXISTS ({correlated} {limit}) FROM t AS o WHERE o.id % 7 = 0")
        }));
    }
    for sql in &statements_sql {
        statements += 1;
        let fr = f.query(sql).await.map(|rows| tag_rows(&rows));
        let fr = fr.map_err(|e| e.to_string());
        let rr = rq(&r, sql, &[]);
        if fr != rr {
            diffs.push(format!(
                "  [{label}] `{sql}`\n     frank= {fr:?}\n     stock= {rr:?}"
            ));
        }
    }
    f.close().await.unwrap();
    (diffs, statements)
}

#[test]
fn join_limit_matches_stock_in_order() {
    let _serial = serial();
    asupersync::test_utils::run_test(|| async {
        let (mut diffs, mut statements) = compare_limits(false).await;
        let (file_diffs, file_statements) = compare_limits(true).await;
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

/// Bound LIMIT/OFFSET parameters, ad hoc and through a prepared statement.
/// Anonymous `?` are numbered in text order: the WHERE's before the LIMIT's,
/// although the LIMIT is evaluated before the join's loops.
#[test]
fn join_limit_parameters_match_stock() {
    let cases: [(&str, &[i64]); 6] = [
        (
            "SELECT t.id, k.label FROM t JOIN k ON t.g = k.g LIMIT ? OFFSET ?",
            &[3, 2],
        ),
        (
            "SELECT t.id, k.label FROM t JOIN k ON t.g = k.g WHERE t.n > ? LIMIT ?",
            &[1, 4],
        ),
        (
            "SELECT t.id, u.w FROM t LEFT JOIN u ON u.tid = t.id WHERE t.n <> ? LIMIT ? OFFSET ?",
            &[3, 5, 2],
        ),
        (
            "SELECT t.id, p.note FROM t JOIN p ON p.code = t.v LIMIT ?2 OFFSET ?1",
            &[1, 3],
        ),
        // Numbered: `LIMIT ?, ?` binds in the wrong order (bd-s9i69).
        (
            "SELECT t.id, k.label FROM t JOIN k ON t.g = k.g LIMIT ?1, ?2",
            &[2, 3],
        ),
        (
            "SELECT t.id, k.label FROM t LEFT JOIN k ON t.g = k.g LIMIT ?",
            &[0],
        ),
    ];
    let _serial = serial();
    asupersync::test_utils::run_test(|| async {
        let mut diffs = Vec::new();
        for file_backed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let (f, r) = open_pair(file_backed, &dir).await;
            for (sql, params) in &cases {
                let values: Vec<SqliteValue> =
                    params.iter().copied().map(SqliteValue::Integer).collect();
                let rr = rq(&r, sql, params);
                let adhoc = f
                    .query_with_params(sql, &values)
                    .await
                    .map(|rows| tag_rows(&rows))
                    .map_err(|e| e.to_string());
                let prepared = match f.prepare(sql).await {
                    Ok(stmt) => stmt
                        .query_with_params(&values)
                        .await
                        .map(|rows| tag_rows(&rows))
                        .map_err(|e| e.to_string()),
                    Err(e) => Err(e.to_string()),
                };
                for (path, fr) in [("ad hoc", adhoc), ("prepared", prepared)] {
                    if fr != rr {
                        diffs.push(format!(
                            "  [file={file_backed}, {path}] `{sql}` {params:?}\n     \
                             frank= {fr:?}\n     stock= {rr:?}"
                        ));
                    }
                }
            }
            f.close().await.unwrap();
        }
        assert!(
            diffs.is_empty(),
            "{} divergence(s) from rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}

const ROWS: i64 = 20_000;

/// Opcodes allowed for a LIMIT 10 lookup join: it stops after its tenth
/// row. Running the join to the end costs at least one opcode per outer row.
const MAX_LIMITED_OPCODES: u64 = 5_000;

#[test]
fn join_limit_stops_after_its_last_row() {
    let _serial = serial();
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("f.db");
            let conn = if file_backed {
                Connection::open(path.to_str().unwrap()).await.unwrap()
            } else {
                Connection::open(":memory:").await.unwrap()
            };
            conn.execute_batch(&format!(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, g INTEGER);\
                 INSERT INTO t SELECT value, 'v' || value, value % 50 \
                   FROM generate_series(1, {ROWS});\
                 CREATE TABLE k(g INTEGER PRIMARY KEY, label TEXT);\
                 INSERT INTO k SELECT value, 'k' || value FROM generate_series(0, 49);"
            ))
            .await
            .expect("schema");
            let mut opcodes = Vec::new();
            for limit in ["LIMIT 10", "LIMIT -1"] {
                let sql = format!("SELECT t.v, k.label FROM t JOIN k ON t.g = k.g {limit}");
                set_hot_path_profile_enabled(true);
                reset_hot_path_profile();
                let rows = conn.query(&sql).await.expect("join");
                opcodes.push(hot_path_profile_snapshot().vdbe.opcodes_executed_total);
                set_hot_path_profile_enabled(false);
                let expected = if limit == "LIMIT 10" {
                    10
                } else {
                    ROWS as usize
                };
                assert_eq!(rows.len(), expected, "`{sql}` (file={file_backed})");
                let first = tag_rows(&rows[..1]);
                assert_eq!(
                    first,
                    vec![vec!["text:v1".to_owned(), "text:k1".to_owned()]]
                );
            }
            eprintln!(
                "bd-9gl3w (file={file_backed}): LIMIT 10 ran {} opcodes, LIMIT -1 ran {}",
                opcodes[0], opcodes[1]
            );
            assert!(
                opcodes[0] > 0 && opcodes[0] <= MAX_LIMITED_OPCODES,
                "bd-9gl3w (file={file_backed}): a LIMIT 10 lookup join ran {} VDBE opcodes; it \
                 must run in the VDBE (more than 0) and stop after its tenth row (at most \
                 {MAX_LIMITED_OPCODES})",
                opcodes[0]
            );
            assert!(
                opcodes[1] >= ROWS as u64,
                "bd-9gl3w (file={file_backed}): LIMIT -1 ran {} opcodes for {ROWS} outer rows; \
                 the counter does not see this statement",
                opcodes[1]
            );
            conn.close().await.unwrap();
        }
    });
}
