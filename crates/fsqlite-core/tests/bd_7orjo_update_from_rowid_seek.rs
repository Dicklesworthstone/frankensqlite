#![recursion_limit = "512"]

//! bd-7orjo: `UPDATE t SET v = d.nv FROM d WHERE d.k = t.id` scanned the
//! whole target once per FROM row in its first pass (10k rows: 40 s, stock
//! 0.05 s). When an ON / WHERE conjunct pins the target's rowid (or its
//! INTEGER PRIMARY KEY) to a FROM column, a literal or a numbered parameter,
//! pass 1 now seeks that one row.
//!
//! The keepers pin the complexity (opcodes grow linearly with the row count)
//! and the semantics against stock SQLite: key storage classes (integer,
//! numeric text, real, non-integral real, NULL, non-numeric text), a target
//! row matched by several FROM rows, aliases, a JOIN inside FROM, literal and
//! parameter keys, the hidden rowid, RETURNING and changes(). The hot-path
//! profile counters are process-global, so the tests here run one at a time.

use fsqlite_core::connection::{
    Connection, Row, hot_path_profile_snapshot, reset_hot_path_profile,
    set_hot_path_profile_enabled,
};
use fsqlite_types::value::SqliteValue;

static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

const ROWS: i64 = 2_000;

/// Opcodes allowed per updated row for both passes. Scanning the target per
/// FROM row costs ROWS times more.
const MAX_OPCODES_PER_ROW: u64 = 400;

#[test]
fn update_from_rowid_key_seeks_the_target() {
    let _serial = serial();
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.expect("open");
        conn.execute_batch(&format!(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, g INTEGER, v INTEGER);\
             INSERT INTO t SELECT value, value % 100, value FROM generate_series(1, {ROWS});\
             CREATE TABLE d(k INTEGER PRIMARY KEY, nv INTEGER);\
             INSERT INTO d SELECT value, value * 2 FROM generate_series(1, {ROWS});"
        ))
        .await
        .expect("schema");
        set_hot_path_profile_enabled(true);
        reset_hot_path_profile();
        let changed = conn
            .execute("UPDATE t SET v = d.nv FROM d WHERE d.k = t.id")
            .await
            .expect("update from");
        let opcodes = hot_path_profile_snapshot().vdbe.opcodes_executed_total;
        set_hot_path_profile_enabled(false);
        eprintln!("bd-7orjo: {changed} rows updated with {opcodes} opcodes");
        assert_eq!(changed, ROWS as usize);
        assert!(
            opcodes <= MAX_OPCODES_PER_ROW * ROWS as u64,
            "bd-7orjo: {opcodes} opcodes to update {ROWS} rows; pass 1 scans the target per FROM \
             row instead of seeking it (limit {MAX_OPCODES_PER_ROW} per row)"
        );
        let rows = conn
            .query("SELECT count(*), sum(v) FROM t")
            .await
            .expect("sum");
        assert_eq!(
            rows[0].values(),
            &[
                SqliteValue::Integer(ROWS),
                SqliteValue::Integer(ROWS * (ROWS + 1))
            ]
        );
    });
}

const SCHEMA: &str = "\
CREATE TABLE t(id INTEGER PRIMARY KEY, g INTEGER, v);\
INSERT INTO t VALUES (1, 1, 'a'), (2, 1, 'b'), (3, 2, 'c'), (4, 2, 'd'), (5, 3, 'e'), (9, 9, 'z');\
CREATE TABLE h(g INTEGER, v);\
INSERT INTO h VALUES (1, 'h1'), (2, 'h2'), (3, 'h3');\
CREATE TABLE d(k, nv, tag);\
INSERT INTO d VALUES (1, 'one', 'x'), ('2', 'two-text', 'x'), (3.0, 'three-real', 'y'), \
    (3.5, 'three-half', 'y'), (NULL, 'null', 'z'), ('four', 'text', 'z'), (4, 'four-int', 'w'), \
    (4, 'four-again', 'w'), (1, 'one-again', 'x');\
CREATE TABLE e(k INTEGER PRIMARY KEY, dk);\
INSERT INTO e VALUES (10, 1), (11, 2), (12, 5);";

/// (statement, params); RETURNING statements report their rows.
const STATEMENTS: &[(&str, &[i64])] = &[
    ("UPDATE t SET v = d.nv FROM d WHERE d.k = t.id", &[]),
    ("UPDATE t SET v = d.nv || '!' FROM d WHERE t.id = d.k AND d.tag <> 'w'", &[]),
    ("UPDATE t AS x SET v = 'alias ' || d.tag FROM d WHERE x.rowid = d.k AND d.tag = 'y'", &[]),
    (
        "UPDATE t SET v = 'join ' || e.k FROM e JOIN d ON d.k = e.dk WHERE t.id = e.dk",
        &[],
    ),
    ("UPDATE t SET v = 'lit ' || d.nv FROM d WHERE t.id = 3 AND d.tag = 'x'", &[]),
    ("UPDATE t SET v = 'param ' || d.nv FROM d WHERE t.id = ?1 AND d.tag = 'w'", &[4]),
    (
        "UPDATE t SET v = h.v FROM h WHERE h.g = t.g RETURNING id, v",
        &[],
    ),
    ("UPDATE t SET g = e.k FROM e WHERE e.dk = t._rowid_ RETURNING id, g", &[]),
];

const DUMPS: &[&str] = &["SELECT id, g, quote(v) FROM t ORDER BY id"];

fn render_fsqlite(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Integer(v) => format!("i:{v}"),
        SqliteValue::Float(v) => format!("f:{v}"),
        SqliteValue::Text(v) => format!("t:{v}"),
        SqliteValue::Blob(v) => format!("b:{:?}", v.to_vec()),
        SqliteValue::Null => "null".to_owned(),
    }
}

fn render_rusqlite(value: rusqlite::types::ValueRef<'_>) -> String {
    match value {
        rusqlite::types::ValueRef::Integer(v) => format!("i:{v}"),
        rusqlite::types::ValueRef::Real(v) => format!("f:{v}"),
        rusqlite::types::ValueRef::Text(v) => format!("t:{}", String::from_utf8_lossy(v)),
        rusqlite::types::ValueRef::Blob(v) => format!("b:{v:?}"),
        rusqlite::types::ValueRef::Null => "null".to_owned(),
    }
}

fn sorted(mut rows: Vec<String>) -> Vec<String> {
    rows.sort();
    rows
}

fn stock_transcript() -> Vec<String> {
    let conn = rusqlite::Connection::open_in_memory().expect("stock open");
    conn.execute_batch(SCHEMA).expect("stock schema");
    let mut transcript = Vec::new();
    for (sql, params) in STATEMENTS {
        let mut stmt = conn.prepare(sql).expect("stock prepare");
        let width = stmt.column_count();
        let rows = stmt
            .query_map(rusqlite::params_from_iter(params.iter()), |row| {
                Ok((0..width)
                    .map(|i| render_rusqlite(row.get_ref(i).expect("stock cell")))
                    .collect::<Vec<_>>()
                    .join("|"))
            })
            .expect("stock query")
            .collect::<Result<Vec<_>, _>>()
            .expect("stock rows");
        drop(stmt);
        let changes: i64 = conn
            .query_row("SELECT changes()", [], |row| row.get(0))
            .expect("stock changes");
        transcript.push(format!("{sql} => {:?} changes={changes}", sorted(rows)));
    }
    for sql in DUMPS {
        let mut stmt = conn.prepare(sql).expect("stock dump");
        let width = stmt.column_count();
        let rows = stmt
            .query_map([], |row| {
                Ok((0..width)
                    .map(|i| render_rusqlite(row.get_ref(i).expect("stock cell")))
                    .collect::<Vec<_>>()
                    .join("|"))
            })
            .expect("stock dump query")
            .collect::<Result<Vec<_>, _>>()
            .expect("stock dump rows");
        transcript.push(format!("{sql} => {rows:?}"));
    }
    transcript
}

fn render_rows(rows: &[Row]) -> Vec<String> {
    rows.iter()
        .map(|row| {
            row.values()
                .iter()
                .map(render_fsqlite)
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

async fn fsqlite_transcript(conn: &Connection) -> Vec<String> {
    conn.execute_batch(SCHEMA).await.expect("schema");
    let mut transcript = Vec::new();
    for (sql, params) in STATEMENTS {
        let bound: Vec<SqliteValue> = params.iter().copied().map(SqliteValue::Integer).collect();
        let rows = conn
            .query_with_params(sql, &bound)
            .await
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
        let changes = conn.query("SELECT changes()").await.expect("changes");
        let changes = match changes[0].values()[0] {
            SqliteValue::Integer(n) => n,
            ref other => panic!("changes() returned {other:?}"),
        };
        transcript.push(format!(
            "{sql} => {:?} changes={changes}",
            sorted(render_rows(&rows))
        ));
    }
    for sql in DUMPS {
        let rows = conn.query(sql).await.expect("dump");
        transcript.push(format!("{sql} => {:?}", render_rows(&rows)));
    }
    transcript
}

fn assert_matches_stock(label: &str, got: &[String], want: &[String]) {
    for (line, (got, want)) in got.iter().zip(want).enumerate() {
        assert_eq!(got, want, "bd-7orjo {label} parity line {line}");
    }
    assert_eq!(got.len(), want.len(), "bd-7orjo {label} transcript length");
}

#[test]
fn update_from_rowid_seek_matches_stock_in_memory() {
    let _serial = serial();
    let expected = stock_transcript();
    asupersync::test_utils::run_test(|| async move {
        let conn = Connection::open(":memory:").await.expect("open");
        let got = fsqlite_transcript(&conn).await;
        assert_matches_stock("in-memory", &got, &expected);
    });
}

#[test]
fn update_from_rowid_seek_matches_stock_file_backed() {
    let _serial = serial();
    let expected = stock_transcript();
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("7orjo.db");
    let path_str = path.to_str().expect("utf-8 path").to_owned();
    asupersync::test_utils::run_test(|| async move {
        let conn = Connection::open(&path_str).await.expect("open");
        let got = fsqlite_transcript(&conn).await;
        assert_matches_stock("file-backed", &got, &expected);
    });
    let checked = rusqlite::Connection::open(&path).expect("stock open of fsqlite file");
    let verdict: String = checked
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("integrity_check");
    assert_eq!(verdict, "ok", "stock integrity_check of the fsqlite file");
}
