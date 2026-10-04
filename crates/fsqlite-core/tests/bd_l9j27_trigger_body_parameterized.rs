#![recursion_limit = "512"]

//! bd-l9j27: every trigger body statement compiled again for every firing
//! row, because OLD/NEW references were substituted into it as literals and
//! each row's statement therefore had different SQL text. A BEFORE UPDATE
//! replay also froze each row's NEW values into the UPDATE's SET clause as
//! literals, so the replayed UPDATE compiled per row as well.
//!
//! OLD/NEW now bind as parameters, so a body statement and a frozen UPDATE
//! keep one SQL text across rows and reuse one compiled program. A statement
//! with a subquery keeps literals, because some subquery paths run without
//! the statement's parameters or bake the subquery's result into the
//! compiled program (the `a2` trigger below reads NULL parameters if it is
//! parameterized). The keepers pin the complexity (compilations do not grow
//! with the row count) and the semantics: trigger logs, outcomes and final
//! contents match stock SQLite, in memory and file-backed, across the
//! positions an OLD/NEW reference can take in a trigger body. The hot-path
//! profile counters are process-global, so the tests in this binary run one
//! at a time.

use fsqlite_core::connection::{
    Connection, Row, hot_path_profile_snapshot, reset_hot_path_profile,
    set_hot_path_profile_enabled,
};
use fsqlite_types::value::SqliteValue;

const FIRING_ROWS: i64 = 300;

/// Compilations allowed per statement: well under one per firing row. Each
/// trigger body statement, the replayed row statement and the OLD/NEW
/// collection compile a constant handful.
const MAX_COMPILATIONS_PER_STATEMENT: u64 = (FIRING_ROWS / 4) as u64;

/// Serializes this binary's tests: a parity test running beside the
/// compilation keeper would add its own compilations to the global counter.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[test]
fn trigger_bodies_compile_once_not_per_firing_row() {
    let _serial = serial();
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.expect("open");
        conn.execute_batch(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, g INTEGER, v INTEGER, s TEXT);\
             CREATE TABLE agg(g INTEGER PRIMARY KEY, total INTEGER);\
             CREATE TABLE log(a, b, c);\
             CREATE TABLE cnt(n INTEGER);\
             INSERT INTO cnt VALUES (0);\
             INSERT INTO agg SELECT value, 0 FROM generate_series(0, 9);\
             CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN \
                 INSERT INTO log VALUES (NEW.id, NEW.v, NEW.s); END;\
             CREATE TRIGGER t_bu BEFORE UPDATE ON t BEGIN \
                 UPDATE cnt SET n = n + NEW.v - OLD.v; END;\
             CREATE TRIGGER t_au AFTER UPDATE ON t BEGIN \
                 INSERT INTO log VALUES (OLD.v, NEW.v, NEW.s); \
                 UPDATE agg SET total = total + NEW.v - OLD.v WHERE g = NEW.g; END;\
             CREATE TRIGGER t_bd BEFORE DELETE ON t BEGIN \
                 DELETE FROM log WHERE a = OLD.id AND b = OLD.v - 2; END;\
             CREATE TRIGGER t_ad AFTER DELETE ON t BEGIN \
                 INSERT INTO log SELECT OLD.id, OLD.v, 'gone ' || OLD.s; END;",
        )
        .await
        .expect("schema");

        let statements = [
            format!(
                "INSERT INTO t SELECT value, value % 10, value * 3, 'row' || value \
                 FROM generate_series(1, {FIRING_ROWS});"
            ),
            "UPDATE t SET v = v + 2, s = s || 'x';".to_owned(),
            "DELETE FROM t;".to_owned(),
        ];
        for sql in &statements {
            set_hot_path_profile_enabled(true);
            reset_hot_path_profile();
            let changed = conn.execute(sql).await.expect("trigger-firing statement");
            let compilations = hot_path_profile_snapshot().parser.compiled_cache_misses;
            set_hot_path_profile_enabled(false);
            eprintln!("bd-l9j27: {sql} compiled {compilations} programs");
            assert_eq!(changed, FIRING_ROWS as usize, "{sql}: rows changed");
            assert!(
                compilations <= MAX_COMPILATIONS_PER_STATEMENT,
                "bd-l9j27: {sql} compiled {compilations} programs for {FIRING_ROWS} firing \
                 rows; per-row trigger body compilation is back \
                 (limit {MAX_COMPILATIONS_PER_STATEMENT})"
            );
        }

        let rows = conn
            .query("SELECT (SELECT n FROM cnt), (SELECT sum(total) FROM agg), count(*) FROM log;")
            .await
            .expect("totals");
        assert_eq!(
            rows[0].values(),
            &[
                SqliteValue::Integer(2 * FIRING_ROWS),
                SqliteValue::Integer(2 * FIRING_ROWS),
                SqliteValue::Integer(2 * FIRING_ROWS),
            ]
        );
    });
}

#[derive(Debug, Clone, Copy)]
enum P {
    Int(i64),
    Real(f64),
    Text(&'static str),
}

impl P {
    fn fsqlite(self) -> SqliteValue {
        match self {
            Self::Int(v) => SqliteValue::Integer(v),
            Self::Real(v) => SqliteValue::Float(v),
            Self::Text(v) => SqliteValue::Text(v.into()),
        }
    }

    fn rusqlite(self) -> rusqlite::types::Value {
        match self {
            Self::Int(v) => rusqlite::types::Value::Integer(v),
            Self::Real(v) => rusqlite::types::Value::Real(v),
            Self::Text(v) => rusqlite::types::Value::Text(v.to_owned()),
        }
    }
}

/// OLD/NEW in every position a body statement can hold one: VALUES, upsert
/// DO UPDATE SET / WHERE, INSERT ... SELECT with aggregates, a subquery's
/// ORDER BY and LIMIT, NOCASE comparisons and LIKE, function arguments, IN
/// lists and subqueries, BETWEEN, scalar and EXISTS subqueries, a CTE in a
/// FROM subquery, UPDATE ... FROM, window frames, a self-referencing scalar
/// subquery in DELETE, RAISE in its WHERE and CASE forms, nested triggers on
/// the body's own target, an INSTEAD OF trigger on a view, and VALUES
/// subqueries into a target that has triggers of its own (`a2`).
const SCHEMA: &str = "\
CREATE TABLE t(id INTEGER PRIMARY KEY, g INTEGER, v, s TEXT COLLATE NOCASE);\
CREATE TABLE log(seq INTEGER PRIMARY KEY, msg);\
CREATE TABLE agg(g INTEGER PRIMARY KEY, total, n);\
CREATE TABLE nums(n INTEGER);\
INSERT INTO nums VALUES (1), (3), (5), (8), (13), (21), (34);\
CREATE TABLE kv(k TEXT PRIMARY KEY, v) WITHOUT ROWID;\
INSERT INTO kv VALUES ('a', 1), ('b', 5), ('c', 9), ('d', 2), ('e', 7);\
CREATE VIEW tv AS SELECT id, v FROM t;\
CREATE TRIGGER t_ai1 AFTER INSERT ON t BEGIN \
  INSERT INTO log(msg) VALUES ('ins ' || NEW.id || ' ' || quote(NEW.v) || ' ' || NEW.s || ' ' || typeof(NEW.v)); \
  INSERT INTO agg(g, total, n) VALUES (NEW.g, NEW.v, 1) \
    ON CONFLICT(g) DO UPDATE SET total = total + excluded.total, n = n + 1 WHERE NEW.v IS NOT NULL; \
  INSERT INTO log(msg) SELECT 'sel ' || NEW.id || ' ' || count(*) FROM t WHERE g = NEW.g; \
  INSERT INTO log(msg) SELECT 'ord ' || group_concat(n) FROM \
    (SELECT n FROM nums ORDER BY abs(n - NEW.id), n LIMIT NEW.g % 3 + 1); \
  INSERT INTO log(msg) SELECT 'nc ' || count(*) FROM t WHERE s = NEW.s AND s LIKE substr(NEW.s, 1, 2) || '%'; \
  INSERT INTO log(msg) VALUES ('json ' || json_object('id', NEW.id, 's', NEW.s, 'in', NEW.g IN (NEW.id, 2, 3))); \
END;\
CREATE TRIGGER t_ai2 AFTER INSERT ON t WHEN NEW.id % 4 = 1 BEGIN \
  INSERT INTO log(msg) SELECT 'cte ' || x FROM (WITH c(x) AS (SELECT NEW.id * 2) SELECT x FROM c); \
  UPDATE agg SET n = agg.n + src.c FROM (SELECT NEW.g AS gg, 10 AS c) AS src WHERE agg.g = src.gg; \
  INSERT INTO log(msg) SELECT 'win ' || sum(x) OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) \
    FROM (SELECT NEW.id AS x UNION ALL SELECT NEW.g) ORDER BY 1 LIMIT 1; \
  INSERT INTO log(msg) VALUES ('lir ' || last_insert_rowid() || ' chg ' || changes()); \
END;\
CREATE TRIGGER t_bi BEFORE INSERT ON t BEGIN \
  SELECT RAISE(ABORT, 'negative v') WHERE typeof(NEW.v) = 'integer' AND NEW.v < 0; \
  SELECT RAISE(ABORT, 'boom s') WHERE NEW.s = 'boom'; \
  SELECT CASE WHEN NEW.s = 'boom2' THEN RAISE(ABORT, 'boom2 s') END; \
END;\
CREATE TRIGGER t_bu BEFORE UPDATE ON t BEGIN \
  INSERT INTO log(msg) SELECT 'bu ' || NEW.id || ' ' || quote(OLD.v) || '>' || quote(NEW.v) || ' ' || NEW.s \
    WHERE NEW.v IN (SELECT v FROM t WHERE id <> NEW.id) OR NEW.v BETWEEN OLD.v AND 100 OR NEW.s LIKE 'p:%'; \
END;\
CREATE TRIGGER t_au AFTER UPDATE ON t BEGIN \
  UPDATE agg SET total = total - OLD.v + NEW.v WHERE g = NEW.g AND OLD.v IS NOT NEW.v \
    AND typeof(OLD.v) = 'integer' AND typeof(NEW.v) = 'integer'; \
  INSERT INTO log(msg) VALUES ('upd ' || OLD.id || ' ' || quote(OLD.v) || '>' || quote(NEW.v) \
    || ' rank=' || (SELECT count(*) FROM t WHERE v < NEW.v)); \
  INSERT INTO log(msg) SELECT 'ex ' || NEW.id WHERE EXISTS (SELECT 1 FROM kv WHERE v = NEW.g); \
END;\
CREATE TRIGGER t_bd BEFORE DELETE ON t BEGIN \
  UPDATE agg SET n = n - 1 WHERE g = OLD.g; \
  DELETE FROM kv WHERE v > (SELECT avg(v) FROM kv WHERE k >= OLD.s); \
END;\
CREATE TRIGGER t_ad AFTER DELETE ON t BEGIN \
  DELETE FROM kv WHERE k = OLD.s; \
  INSERT INTO log(msg) VALUES ('del ' || OLD.id || ' ' || (SELECT count(*) FROM t) || ' ' || quote(OLD.v)); \
END;\
CREATE TRIGGER agg_au AFTER UPDATE ON agg BEGIN \
  INSERT INTO log(msg) VALUES ('agg ' || NEW.g || ' ' || quote(OLD.total) || '>' || quote(NEW.total) || ' n=' || NEW.n); \
END;\
CREATE TRIGGER tv_ii INSTEAD OF INSERT ON tv BEGIN \
  INSERT INTO t(id, g, v, s) VALUES (NEW.id, NEW.id % 3, NEW.v, 'via view ' || NEW.id); \
END;\
CREATE TABLE a2(k INTEGER PRIMARY KEY, c1, c2, c3, c4, hits INTEGER DEFAULT 0);\
CREATE TABLE a3(msg);\
CREATE TRIGGER a2_ai AFTER INSERT ON a2 BEGIN \
  INSERT INTO a3 VALUES ('a2 ins ' || NEW.k || ' ' || quote(NEW.c1) || quote(NEW.c2) || quote(NEW.c3) || quote(NEW.c4)); \
END;\
CREATE TRIGGER a2_au AFTER UPDATE ON a2 BEGIN \
  INSERT INTO a3 VALUES ('a2 upd ' || NEW.k || ' ' || NEW.hits || ' ' || quote(NEW.c1)); \
END;\
CREATE TRIGGER t_ai3 AFTER INSERT ON t BEGIN \
  INSERT INTO a2(k, c1, c2, c3, c4) VALUES (NEW.g, (SELECT count(*) FROM t WHERE v < NEW.v) > 1, \
      NEW.id IN (SELECT id FROM t WHERE g = NEW.g), EXISTS (SELECT 1 FROM kv WHERE v = NEW.g), \
      (SELECT max(id) FROM t WHERE g = NEW.g) = NEW.id) \
    ON CONFLICT(k) DO UPDATE SET hits = hits + 1, c1 = excluded.c1 \
      WHERE (SELECT count(*) FROM t WHERE g = NEW.g) > 1; \
END;";

/// (statement, params). The UPDATEs with parameters of their own reach the
/// BEFORE UPDATE freeze with caller parameters: anonymous, numbered and in a
/// row-value assignment, single-row and replayed.
const STATEMENTS: &[(&str, &[P])] = &[
    (
        "INSERT INTO t VALUES (1, 1, 10, 'a'), (2, 2, '20', 'B'), (3, 1, 3.5, 'c'), \
         (4, 0, NULL, 'D'), (5, 2, x'05', 'e'), (6, 1, 'six', 'A'), (7, 0, 70, 'b')",
        &[],
    ),
    (
        "INSERT INTO t SELECT id + 10, g, v * 2, s || 'x' FROM t WHERE typeof(v) IN ('integer', 'real')",
        &[],
    ),
    ("INSERT INTO t VALUES (30, 1, -5, 'neg')", &[]),
    ("INSERT INTO t VALUES (31, 1, 5, 'boom')", &[]),
    ("INSERT INTO t VALUES (32, 2, 6, 'boom2')", &[]),
    ("INSERT INTO tv VALUES (40, 44), (41, 'forty-one')", &[]),
    ("UPDATE t SET v = v + 1 WHERE g = 1", &[]),
    ("UPDATE t SET v = v || 'x' WHERE id IN (2, 7)", &[]),
    ("UPDATE t SET v = NULL WHERE id = 3", &[]),
    ("UPDATE t SET g = g + 1, v = 3 WHERE id > 10", &[]),
    (
        "UPDATE t SET v = ?, s = ? || s WHERE id = ?",
        &[P::Int(99), P::Text("p:"), P::Int(5)],
    ),
    ("UPDATE t SET v = ?2 WHERE g = ?1", &[P::Int(0), P::Real(2.5)]),
    (
        "UPDATE t SET (v, s) = (?, 'rv' || id) WHERE id >= 40",
        &[P::Text("rowval")],
    ),
    ("DELETE FROM t WHERE id % 2 = 0", &[]),
    ("DELETE FROM t WHERE id = ?", &[P::Int(1)]),
    // The self-referencing scalar subquery a body DELETE now carries with a
    // parameter, from the public API.
    (
        "DELETE FROM kv WHERE v > (SELECT avg(v) FROM kv WHERE k >= ?) - 2",
        &[P::Text("a")],
    ),
];

const DUMPS: &[&str] = &[
    "SELECT seq, msg FROM log ORDER BY seq",
    "SELECT id, g, quote(v), s FROM t ORDER BY id",
    "SELECT g, quote(total), n FROM agg ORDER BY g",
    "SELECT k, v FROM kv ORDER BY k",
    "SELECT k, c1, c2, c3, c4, hits FROM a2 ORDER BY k",
    "SELECT rowid, msg FROM a3 ORDER BY rowid",
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
            Err(message) => format!("{sql} => error:{message} changes={changes:?}"),
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
            Err(error) => format!("{sql} => error:{error} changes={changes:?}"),
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
        assert_eq!(got, want, "bd-l9j27 {label} parity line {line}");
    }
    assert_eq!(got.len(), want.len(), "bd-l9j27 {label} transcript length");
}

#[test]
fn parameterized_trigger_bodies_match_stock_in_memory() {
    let _serial = serial();
    let expected = stock_transcript();
    asupersync::test_utils::run_test(|| async move {
        let conn = Connection::open(":memory:").await.expect("open");
        let got = fsqlite_transcript(&conn).await;
        assert_matches_stock("in-memory", &got, &expected);
    });
}

#[test]
fn parameterized_trigger_bodies_match_stock_file_backed() {
    let _serial = serial();
    let expected = stock_transcript();
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("l9j27.db");
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
