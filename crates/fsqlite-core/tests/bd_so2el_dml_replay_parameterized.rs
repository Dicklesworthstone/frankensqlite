#![recursion_limit = "512"]

//! bd-so2el: a multi-row UPDATE or DELETE replayed one row at a time (BEFORE
//! or AFTER triggers, FK actions, observable DELETE trigger order, `UPDATE ...
//! FROM` with triggers) compiled a new statement for every row.
//!
//! Each row's statement carried its locator (and, for `UPDATE ... FROM`, its
//! SET values) as literals, so every row had different SQL text and missed
//! the compiled-statement cache, and the single-row statement then ran a
//! locator SELECT of its own only to find it matched one row. The values now
//! bind as parameters numbered after every slot the statement already uses,
//! and a WHERE that pins the whole unique row key skips the locator SELECT.
//!
//! The keepers pin the complexity (compilations do not grow with the row
//! count) and the semantics: outcomes, RETURNING rows, `changes()`, trigger
//! logs and final contents match stock SQLite, in memory and file-backed,
//! for statements that carry their own anonymous, numbered and named
//! parameters, WITHOUT ROWID keys of mixed storage classes, NOCASE keys, a
//! table whose columns shadow `rowid` and `oid`, conflict modes and
//! single-row statements. The hot-path profile counters are process-global,
//! so the tests in this binary run one at a time.

use fsqlite_core::connection::{
    Connection, Row, hot_path_profile_snapshot, reset_hot_path_profile,
    set_hot_path_profile_enabled,
};
use fsqlite_types::value::SqliteValue;

const REPLAYED_ROWS: i64 = 300;

/// Serializes this binary's tests: a parity test running beside the
/// compilation keeper would add its own compilations to the global counter.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Compilations allowed per replayed statement: well under one per row.
/// Before the fix every row compiled its UPDATE/DELETE and its locator
/// SELECT (1,094 for the 300-row UPDATE); with parameters each statement
/// compiles a constant handful (4 or 5 measured from 60 to 1,200 rows).
const MAX_COMPILATIONS_PER_STATEMENT: u64 = (REPLAYED_ROWS / 4) as u64;

#[test]
fn row_by_row_replay_compiles_once_not_per_row() {
    let _serial = serial();
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.expect("open");
        conn.execute_batch(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER, w TEXT);\
             CREATE TABLE src(k INTEGER PRIMARY KEY, d INTEGER);\
             CREATE TABLE counter(n INTEGER);\
             INSERT INTO counter VALUES (0);\
             CREATE TRIGGER t_au AFTER UPDATE ON t BEGIN UPDATE counter SET n = n + 1; END;\
             CREATE TRIGGER t_bd BEFORE DELETE ON t BEGIN UPDATE counter SET n = n + 10; END;\
             CREATE TRIGGER t_ad AFTER DELETE ON t BEGIN UPDATE counter SET n = n + 100; END;",
        )
        .await
        .expect("schema");
        conn.execute(&format!(
            "INSERT INTO t SELECT value, value, 'w' || value FROM generate_series(1, {});",
            2 * REPLAYED_ROWS
        ))
        .await
        .expect("seed t");
        conn.execute(&format!(
            "INSERT INTO src SELECT value, value * 2 FROM generate_series(1, {REPLAYED_ROWS});"
        ))
        .await
        .expect("seed src");

        let statements = [
            format!("UPDATE t SET v = v + 1 WHERE id <= {REPLAYED_ROWS};"),
            "UPDATE t SET v = t.v + src.d FROM src WHERE src.k = t.id;".to_owned(),
            format!("DELETE FROM t WHERE id > {REPLAYED_ROWS};"),
        ];
        for sql in &statements {
            set_hot_path_profile_enabled(true);
            reset_hot_path_profile();
            let changed = conn.execute(sql).await.expect("replayed statement");
            let compilations = hot_path_profile_snapshot().parser.compiled_cache_misses;
            set_hot_path_profile_enabled(false);
            assert_eq!(changed, REPLAYED_ROWS as usize, "{sql}: rows changed");
            assert!(
                compilations <= MAX_COMPILATIONS_PER_STATEMENT,
                "bd-so2el: {sql} compiled {compilations} programs for {REPLAYED_ROWS} replayed \
                 rows; per-row compilation is back (limit {MAX_COMPILATIONS_PER_STATEMENT})"
            );
        }

        let rows = conn.query("SELECT n FROM counter;").await.expect("counter");
        assert_eq!(
            rows[0].values(),
            &[SqliteValue::Integer(2 * REPLAYED_ROWS + 110 * REPLAYED_ROWS)]
        );
    });
}

#[derive(Debug, Clone, Copy)]
enum P {
    Int(i64),
    Real(f64),
    Text(&'static str),
    Blob(&'static [u8]),
    Null,
}

impl P {
    fn fsqlite(self) -> SqliteValue {
        match self {
            Self::Int(v) => SqliteValue::Integer(v),
            Self::Real(v) => SqliteValue::Float(v),
            Self::Text(v) => SqliteValue::Text(v.into()),
            Self::Blob(v) => SqliteValue::Blob(v.to_vec().into()),
            Self::Null => SqliteValue::Null,
        }
    }

    fn rusqlite(self) -> rusqlite::types::Value {
        match self {
            Self::Int(v) => rusqlite::types::Value::Integer(v),
            Self::Real(v) => rusqlite::types::Value::Real(v),
            Self::Text(v) => rusqlite::types::Value::Text(v.to_owned()),
            Self::Blob(v) => rusqlite::types::Value::Blob(v.to_vec()),
            Self::Null => rusqlite::types::Value::Null,
        }
    }
}

const SCHEMA: &str = "\
CREATE TABLE log(seq INTEGER PRIMARY KEY, msg);\
CREATE TABLE t(id INTEGER PRIMARY KEY, v, w TEXT);\
INSERT INTO t VALUES (1, 10, 'a'), (2, '20', 'b'), (3, 3.5, 'c'), (4, NULL, 'd'), \
    (5, x'05', 'e'), (6, 'six', 'f'), (7, 70, 'g');\
CREATE TRIGGER t_bu BEFORE UPDATE ON t BEGIN \
    INSERT INTO log(msg) VALUES ('t-bu ' || OLD.id || ' ' || quote(OLD.v) || '>' || quote(NEW.v) \
        || ' max=' || (SELECT max(id) FROM t WHERE v IS NOT NULL)); END;\
CREATE TRIGGER t_au AFTER UPDATE ON t BEGIN \
    INSERT INTO log(msg) VALUES ('t-au ' || NEW.id || ' ' || quote(NEW.v) || ' ' || quote(NEW.w)); END;\
CREATE TRIGGER t_bd BEFORE DELETE ON t BEGIN \
    INSERT INTO log(msg) VALUES ('t-bd ' || OLD.id || ' n=' || (SELECT count(*) FROM t)); END;\
CREATE TRIGGER t_ad AFTER DELETE ON t BEGIN \
    INSERT INTO log(msg) VALUES ('t-ad ' || OLD.id || ' n=' || (SELECT count(*) FROM t)); END;\
CREATE TABLE src(k INTEGER PRIMARY KEY, d);\
INSERT INTO src VALUES (1, 'x1'), (2, 22), (3, NULL), (7, 7.25);\
CREATE TABLE wr(a TEXT COLLATE NOCASE, b INTEGER, v, PRIMARY KEY (a, b)) WITHOUT ROWID;\
INSERT INTO wr VALUES ('a', 1, 'a1'), ('B', 1, 'B1'), ('a', 2, 'a2'), ('c', 2, 'c2');\
CREATE TRIGGER wr_bu BEFORE UPDATE ON wr BEGIN \
    INSERT INTO log(msg) VALUES ('wr-bu ' || OLD.a || OLD.b || ' ' || quote(NEW.v) \
        || ' n=' || (SELECT count(*) FROM wr WHERE v LIKE 'u%')); END;\
CREATE TRIGGER wr_ad AFTER DELETE ON wr BEGIN \
    INSERT INTO log(msg) VALUES ('wr-ad ' || OLD.a || OLD.b); END;\
CREATE TRIGGER wr_bd BEFORE DELETE ON wr BEGIN \
    INSERT INTO log(msg) VALUES ('wr-bd ' || OLD.a || OLD.b || ' n=' || (SELECT count(*) FROM wr)); END;\
CREATE TABLE wt(k PRIMARY KEY, v) WITHOUT ROWID;\
INSERT INTO wt VALUES (1, 'int'), ('1', 'text'), (1.5, 'real'), (x'01', 'blob'), ('01', 'text01');\
CREATE TRIGGER wt_au AFTER UPDATE ON wt BEGIN \
    INSERT INTO log(msg) VALUES ('wt-au ' || quote(NEW.k) || ' ' || NEW.v); END;\
CREATE TABLE s(rowid TEXT, oid INTEGER, v);\
INSERT INTO s VALUES ('r1', 1, 'one'), ('r2', 2, 'two'), ('r3', 3, 'three');\
CREATE TRIGGER s_au AFTER UPDATE ON s BEGIN \
    INSERT INTO log(msg) VALUES ('s-au ' || NEW.rowid || ' ' || NEW.v); END;\
CREATE TABLE u(id INTEGER PRIMARY KEY, k UNIQUE, v);\
INSERT INTO u VALUES (1, 1, 'u1'), (2, 2, 'u2'), (3, 3, 'u3'), (4, 4, 'u4');\
CREATE TRIGGER u_au AFTER UPDATE ON u BEGIN \
    INSERT INTO log(msg) VALUES ('u-au ' || NEW.id || ' ' || NEW.k); END;";

/// (statement, params). Statements with RETURNING report their rows.
const STATEMENTS: &[(&str, &[P])] = &[
    // The statement's own anonymous parameters, in SET and RETURNING.
    (
        "UPDATE t SET v = ? || quote(v) WHERE id > ? AND id < 7 RETURNING id, v, ?",
        &[P::Text("p:"), P::Int(2), P::Text("ret")],
    ),
    // A numbered parameter far past the bound count, and named parameters.
    (
        "UPDATE t SET w = ?5 || w WHERE id <= ?1",
        &[P::Int(3), P::Null, P::Null, P::Null, P::Text("n5:")],
    ),
    (
        "UPDATE t SET w = :tag || w WHERE id >= :lo RETURNING id, w, :tag",
        &[P::Text("tag:"), P::Int(6)],
    ),
    // Single-row statements: the WHERE pins the whole rowid.
    ("UPDATE t SET v = 'one' WHERE id = ?", &[P::Int(1)]),
    ("UPDATE t SET v = 'two' WHERE rowid = '2'", &[]),
    ("UPDATE t SET v = 'none' WHERE id = 3 AND w = 'no such w'", &[]),
    ("UPDATE t SET v = 'neg' WHERE id = -1", &[]),
    // UPDATE ... FROM with triggers, with a parameter in SET.
    (
        "UPDATE t SET v = quote(src.d) || ? FROM src WHERE src.k = t.id RETURNING t.id, t.v",
        &[P::Text("!")],
    ),
    // WITHOUT ROWID with a NOCASE key: one key pinned leaves several rows.
    ("UPDATE wr SET v = 'u' || v WHERE a = 'A'", &[]),
    ("UPDATE wr SET v = ?2 || b WHERE b > ?1", &[P::Int(1), P::Text("ub")]),
    ("UPDATE wr SET v = 'pinned' WHERE a = 'b' AND b = 1", &[]),
    // Mixed storage classes in a typeless WITHOUT ROWID key.
    ("UPDATE wt SET v = v || '*'", &[]),
    ("UPDATE wt SET v = v || '!' WHERE k = ?", &[P::Int(1)]),
    ("UPDATE wt SET v = v || '?' WHERE k = ?", &[P::Text("1")]),
    ("UPDATE wt SET v = v || '#' WHERE k = ?", &[P::Blob(&[1])]),
    ("UPDATE wt SET v = v || '%' WHERE k = ?", &[P::Real(1.5)]),
    // Columns named rowid and oid: the replay locates rows by _rowid_.
    ("UPDATE s SET v = v || '+' WHERE oid >= 2", &[]),
    ("UPDATE s SET v = 'r' WHERE rowid = 'r1'", &[]),
    // Conflict modes over a replay.
    ("UPDATE OR IGNORE u SET k = 3 WHERE id < 4", &[]),
    ("UPDATE OR REPLACE u SET k = k + 1 WHERE id >= 3", &[]),
    ("UPDATE OR FAIL u SET k = 4 WHERE id < 3", &[]),
    // Observable-order DELETE with RETURNING and a parameter.
    ("DELETE FROM t WHERE id > ? RETURNING id, ?", &[P::Int(4), P::Text("gone")]),
    ("DELETE FROM t WHERE id = ?", &[P::Int(1)]),
    ("DELETE FROM wr WHERE a = ?", &[P::Text("A")]),
];

const DUMPS: &[&str] = &[
    "SELECT seq, msg FROM log ORDER BY seq",
    "SELECT id, quote(v), w FROM t ORDER BY id",
    "SELECT a, b, v FROM wr ORDER BY a, b",
    "SELECT quote(k), v FROM wt ORDER BY quote(k)",
    "SELECT _rowid_, rowid, oid, v FROM s ORDER BY _rowid_",
    "SELECT id, k, v FROM u ORDER BY id",
];

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

/// Constraint failures are compared by kind; their wording is not the
/// subject of this keeper.
fn outcome_error(message: &str) -> String {
    if message.contains("UNIQUE") {
        "error:UNIQUE".to_owned()
    } else {
        format!("error:{message}")
    }
}

fn stock_transcript() -> Vec<String> {
    let conn = rusqlite::Connection::open_in_memory().expect("stock open");
    conn.execute_batch(SCHEMA).expect("stock schema");
    let query = |sql: &str, params: &[P]| -> Result<Vec<String>, String> {
        let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
        let width = stmt.column_count();
        let mut rows = stmt
            .query(rusqlite::params_from_iter(params.iter().map(|p| p.rusqlite())))
            .map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            out.push(
                (0..width)
                    .map(|i| render_rusqlite(row.get_ref(i).expect("stock cell")))
                    .collect::<Vec<_>>()
                    .join("|"),
            );
        }
        Ok(out)
    };
    let mut transcript = Vec::new();
    for (sql, params) in STATEMENTS {
        let result = query(sql, params);
        let changes = query("SELECT changes()", &[]).expect("stock changes");
        transcript.push(match result {
            Ok(rows) => format!("{sql} => ok {rows:?} changes={changes:?}"),
            Err(message) => format!("{sql} => {} changes={changes:?}", outcome_error(&message)),
        });
    }
    for sql in DUMPS {
        transcript.push(format!("{sql} => {:?}", query(sql, &[]).expect("stock dump")));
    }
    transcript
}

async fn fsqlite_transcript(conn: &Connection) -> Vec<String> {
    conn.execute_batch(SCHEMA).await.expect("schema");
    let mut transcript = Vec::new();
    for (sql, params) in STATEMENTS {
        let bound: Vec<SqliteValue> = params.iter().map(|p| p.fsqlite()).collect();
        let result = conn.query_with_params(sql, &bound).await;
        let changes = render_rows(&conn.query("SELECT changes()").await.expect("changes"));
        transcript.push(match result {
            Ok(rows) => format!("{sql} => ok {:?} changes={changes:?}", render_rows(&rows)),
            Err(error) => format!(
                "{sql} => {} changes={changes:?}",
                outcome_error(&error.to_string())
            ),
        });
    }
    for sql in DUMPS {
        let rows = conn.query(sql).await.expect("dump");
        transcript.push(format!("{sql} => {:?}", render_rows(&rows)));
    }
    transcript
}

fn assert_matches_stock(label: &str, got: &[String], want: &[String]) {
    for (line, (got, want)) in got.iter().zip(want).enumerate() {
        assert_eq!(got, want, "bd-so2el {label} parity line {line}");
    }
    assert_eq!(got.len(), want.len(), "bd-so2el {label} transcript length");
}

#[test]
fn parameterized_row_replay_matches_stock_in_memory() {
    let _serial = serial();
    let expected = stock_transcript();
    asupersync::test_utils::run_test(|| async move {
        let conn = Connection::open(":memory:").await.expect("open");
        let got = fsqlite_transcript(&conn).await;
        assert_matches_stock("in-memory", &got, &expected);
    });
}

#[test]
fn parameterized_row_replay_matches_stock_file_backed() {
    let _serial = serial();
    let expected = stock_transcript();
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("so2el.db");
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
