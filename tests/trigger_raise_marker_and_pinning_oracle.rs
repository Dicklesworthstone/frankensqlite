#![recursion_limit = "512"]

//! Review of 1711fdd43 and 2bbc489da: trigger RAISE shapes and non-TEMP
//! trigger name binding, compared with stock SQLite.
//!
//! 1711fdd43 made `SELECT RAISE(...) FROM ...` fire, but only for a lone
//! RAISE column or a single-branch searched CASE. Every other RAISE-bearing
//! trigger SELECT was still skipped as if nothing fired: a CASE with several
//! RAISE or value branches, a simple `CASE x WHEN`, an `iif`, a RAISE beside
//! other result columns, with or without FROM. Stock evaluates the result
//! columns of each row, left to right, and stops at the first RAISE reached.
//!
//! 2bbc489da pinned a non-TEMP trigger's unqualified names to MAIN only while
//! a TEMP table shadowed a main one, so a name only TEMP has still resolved to
//! TEMP; stock binds the persistent trigger to its own schema and fails with
//! `no such table: main.x`.

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

const SCHEMA: &str = "\
CREATE TABLE u(k INTEGER, lim INTEGER, tag TEXT);\
INSERT INTO u VALUES (1, 10, 'a'), (2, 20, 'b'), (2, 5, 'bb'), (5, 50, 'e');\
CREATE TABLE t(k INTEGER, v INTEGER, note TEXT);\
CREATE TABLE log(msg);\
CREATE TRIGGER t_multi BEFORE INSERT ON t WHEN NEW.note = 'multi' BEGIN \
  SELECT CASE WHEN NEW.v > u.lim THEN RAISE(ABORT, 'multi over') \
              WHEN NEW.v < 0 THEN RAISE(IGNORE) ELSE 0 END FROM u WHERE u.k = NEW.k; \
END;\
CREATE TRIGGER t_simple BEFORE INSERT ON t WHEN NEW.note = 'simple' BEGIN \
  SELECT CASE NEW.v WHEN 1 THEN RAISE(ABORT, 'simple one') WHEN 2 THEN RAISE(FAIL, 'simple two') END \
    FROM u WHERE u.k = NEW.k; \
END;\
CREATE TRIGGER t_iif BEFORE INSERT ON t WHEN NEW.note = 'iif' BEGIN \
  SELECT iif(NEW.v > u.lim, RAISE(ABORT, 'iif over'), 'fine') FROM u WHERE u.k = NEW.k; \
END;\
CREATE TRIGGER t_second BEFORE INSERT ON t WHEN NEW.note = 'second' BEGIN \
  SELECT u.tag, RAISE(ABORT, 'second column') FROM u WHERE u.k = NEW.k; \
END;\
CREATE TRIGGER t_left BEFORE INSERT ON t WHEN NEW.note = 'left' BEGIN \
  SELECT CASE WHEN u.tag = 'bb' THEN RAISE(ABORT, 'left bb') END, \
         CASE WHEN u.lim > 0 THEN RAISE(ABORT, 'right lim') END \
    FROM u WHERE u.k = NEW.k ORDER BY u.tag DESC; \
END;\
CREATE TRIGGER t_bare BEFORE INSERT ON t WHEN NEW.note = 'bare' BEGIN \
  SELECT CASE WHEN NEW.v > 100 THEN RAISE(ABORT, 'bare big') WHEN NEW.v < 0 THEN RAISE(IGNORE) END; \
END;\
CREATE TRIGGER t_agg BEFORE INSERT ON t WHEN NEW.note = 'agg' BEGIN \
  SELECT CASE WHEN count(*) > 1 THEN RAISE(ABORT, 'agg many') END FROM u WHERE u.k = NEW.k; \
END;\
CREATE TRIGGER t_agg_empty BEFORE INSERT ON t WHEN NEW.note = 'aggempty' BEGIN \
  SELECT count(*), RAISE(ABORT, 'agg always') FROM u WHERE u.k = NEW.k; \
END;\
CREATE TRIGGER t_alias BEFORE INSERT ON t WHEN NEW.note = 'alias' BEGIN \
  SELECT u.lim AS l, CASE WHEN u.lim < NEW.v THEN RAISE(ABORT, 'alias order') END \
    FROM u WHERE u.k = NEW.k ORDER BY l; \
END;\
CREATE TRIGGER t_distinct BEFORE INSERT ON t WHEN NEW.note = 'distinct' BEGIN \
  SELECT DISTINCT CASE WHEN u.k = NEW.k THEN RAISE(ABORT, 'distinct hit') END FROM u; \
END;\
CREATE TRIGGER t_limited BEFORE INSERT ON t WHEN NEW.note = 'limited' BEGIN \
  SELECT CASE WHEN u.tag = 'bb' THEN RAISE(ABORT, 'limited bb') END FROM u ORDER BY u.k LIMIT 2; \
END;\
CREATE TRIGGER t_offset BEFORE INSERT ON t WHEN NEW.note = 'offset' BEGIN \
  SELECT CASE WHEN u.tag = 'bb' THEN RAISE(ABORT, 'offset bb') END FROM u ORDER BY u.k LIMIT 1 OFFSET NEW.v; \
END;\
CREATE TRIGGER t_log AFTER INSERT ON t BEGIN \
  INSERT INTO log VALUES ('in ' || NEW.k || ' ' || NEW.v || ' ' || NEW.note); \
END;";

const STATEMENTS: &[&str] = &[
    "INSERT INTO t VALUES (1, 5, 'multi')",
    "INSERT INTO t VALUES (1, 50, 'multi')",
    "INSERT INTO t VALUES (1, -1, 'multi')",
    "INSERT INTO t VALUES (2, 7, 'multi')",
    "INSERT INTO t VALUES (9, 999, 'multi')",
    "INSERT INTO t VALUES (1, 3, 'simple')",
    "INSERT INTO t VALUES (1, 1, 'simple')",
    "INSERT INTO t VALUES (1, 2, 'simple')",
    "INSERT INTO t VALUES (2, 4, 'iif')",
    "INSERT INTO t VALUES (2, 15, 'iif')",
    "INSERT INTO t VALUES (9, 1, 'second')",
    "INSERT INTO t VALUES (5, 1, 'second')",
    "INSERT INTO t VALUES (1, 1, 'left')",
    "INSERT INTO t VALUES (2, 1, 'left')",
    "INSERT INTO t VALUES (3, 1, 'bare')",
    "INSERT INTO t VALUES (3, 101, 'bare')",
    "INSERT INTO t VALUES (3, -5, 'bare')",
    "INSERT INTO t VALUES (1, 0, 'agg')",
    "INSERT INTO t VALUES (2, 0, 'agg')",
    "INSERT INTO t VALUES (9, 0, 'aggempty')",
    "INSERT INTO t VALUES (2, 6, 'alias')",
    "INSERT INTO t VALUES (2, 4, 'alias')",
    "INSERT INTO t VALUES (7, 0, 'distinct')",
    "INSERT INTO t VALUES (5, 0, 'distinct')",
    "INSERT INTO t VALUES (0, 0, 'limited')",
    "INSERT INTO t VALUES (0, 0, 'offset')",
    "INSERT INTO t VALUES (0, 1, 'offset')",
    "INSERT INTO t VALUES (0, 2, 'offset')",
    "INSERT INTO t VALUES (10, 1, 'x'), (1, 99, 'multi'), (11, 1, 'y')",
    "INSERT INTO t VALUES (12, 1, 'x'), (1, 2, 'simple'), (13, 1, 'y')",
    "INSERT INTO t VALUES (14, -9, 'multi'), (1, -9, 'multi'), (15, 0, 'z')",
];

const DUMPS: &[&str] = &[
    "SELECT k, v, note FROM t ORDER BY rowid",
    "SELECT msg FROM log ORDER BY rowid",
];

/// Persistent triggers whose bodies name relations only TEMP has, or that a
/// TEMP table shadows.
const PIN_SCHEMA: &str = "\
CREATE TABLE cfg(k, v);\
INSERT INTO cfg VALUES (1, 'main');\
CREATE TABLE plog(s);\
CREATE TABLE p(k);\
CREATE TRIGGER p_ai AFTER INSERT ON p BEGIN INSERT INTO plog SELECT v FROM cfg WHERE k = NEW.k; END;\
CREATE TABLE q(k);\
CREATE TRIGGER q_ai AFTER INSERT ON q BEGIN INSERT INTO only_temp VALUES (NEW.k); END;\
CREATE TABLE r(k);\
CREATE TRIGGER r_ai AFTER INSERT ON r WHEN EXISTS (SELECT 1 FROM only_temp) BEGIN \
  INSERT INTO plog VALUES ('r ' || NEW.k); END;\
CREATE TABLE s(k);\
CREATE TRIGGER s_ai AFTER INSERT ON s BEGIN INSERT INTO plog SELECT 'view ' || x FROM temp_view; END;\
CREATE TABLE w(k);\
CREATE TRIGGER w_ai AFTER INSERT ON w BEGIN \
  INSERT INTO plog SELECT 'tvf ' || value FROM json_each('[1,2]') WHERE value <= NEW.k; \
  INSERT INTO plog WITH c(n) AS (SELECT NEW.k) SELECT 'cte ' || n FROM c; END;\
CREATE TEMP TABLE only_temp(x);\
INSERT INTO only_temp VALUES (0);\
CREATE TEMP VIEW temp_view AS SELECT 7 AS x;";

const PIN_STATEMENTS: &[&str] = &[
    "INSERT INTO p VALUES (1)",
    "INSERT INTO q VALUES (5)",
    "INSERT INTO r VALUES (6)",
    "INSERT INTO s VALUES (1)",
    "INSERT INTO w VALUES (2)",
    "INSERT INTO main.only_temp VALUES (9)",
    "UPDATE main.only_temp SET x = 1",
    "DELETE FROM main.only_temp",
    "UPDATE only_temp SET x = x + 1",
    "CREATE TEMP TABLE cfg(k, v)",
    "INSERT INTO temp.cfg VALUES (1, 'temp')",
    "INSERT INTO p VALUES (1)",
    "DROP TABLE temp.only_temp",
    "DROP TABLE temp.cfg",
    "CREATE TABLE only_temp(x)",
    "INSERT INTO q VALUES (7)",
    "DROP VIEW temp.temp_view",
    "CREATE VIEW temp_view AS SELECT 8 AS x",
    "INSERT INTO s VALUES (2)",
];

const PIN_DUMPS: &[&str] = &[
    "SELECT s FROM main.plog ORDER BY rowid",
    "SELECT count(*) FROM q",
    "SELECT count(*) FROM r",
    "SELECT count(*) FROM s",
    "SELECT x FROM main.only_temp ORDER BY rowid",
];

const KNOWN_MESSAGES: &[&str] = &[
    "multi over",
    "simple one",
    "simple two",
    "iif over",
    "second column",
    "left bb",
    "right lim",
    "bare big",
    "agg many",
    "agg always",
    "alias order",
    "distinct hit",
    "limited bb",
    "offset bb",
    "no such table: main.only_temp",
    "no such table: main.temp_view",
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

fn is_dml(sql: &str) -> bool {
    ["INSERT", "UPDATE", "DELETE"]
        .iter()
        .any(|verb| sql.starts_with(verb))
}

/// The RAISE / resolution message is what both engines must agree on; the
/// error-code decoration around it is not the subject here.
fn outcome_error(message: &str) -> String {
    KNOWN_MESSAGES
        .iter()
        .find(|known| message.contains(*known))
        .map_or_else(|| format!("error:{message}"), |known| format!("error:{known}"))
}

fn stock_transcript(schema: &str, statements: &[&str], dumps: &[&str]) -> Vec<String> {
    let conn = rusqlite::Connection::open_in_memory().expect("stock open");
    conn.execute_batch(schema).expect("stock schema");
    let mut transcript = Vec::new();
    for sql in statements {
        let outcome = match conn.execute_batch(sql) {
            Ok(()) if is_dml(sql) => {
                let changes: i64 = conn
                    .query_row("SELECT changes()", [], |row| row.get(0))
                    .expect("stock changes");
                format!("ok changes={changes}")
            }
            Ok(()) => "ok".to_owned(),
            Err(error) => outcome_error(&error.to_string()),
        };
        transcript.push(format!("{sql} => {outcome}"));
    }
    for sql in dumps {
        let mut stmt = conn.prepare(sql).expect("stock dump prepare");
        let width = stmt.column_count();
        let rows = stmt
            .query_map([], |row| {
                Ok((0..width)
                    .map(|i| render_rusqlite(row.get_ref(i).expect("stock cell")))
                    .collect::<Vec<_>>()
                    .join("|"))
            })
            .expect("stock dump")
            .collect::<Result<Vec<_>, _>>()
            .expect("stock rows");
        transcript.push(format!("{sql} => {rows:?}"));
    }
    transcript
}

async fn fsqlite_transcript(
    conn: &Connection,
    schema: &str,
    statements: &[&str],
    dumps: &[&str],
) -> Vec<String> {
    conn.execute_batch(schema).await.expect("schema");
    let mut transcript = Vec::new();
    for sql in statements {
        let outcome = match conn.execute(sql).await {
            Ok(_) if is_dml(sql) => {
                let changes = render_rows(&conn.query("SELECT changes()").await.expect("changes"));
                format!("ok changes={}", changes.join(",").trim_start_matches("i:"))
            }
            Ok(_) => "ok".to_owned(),
            Err(error) => outcome_error(&error.to_string()),
        };
        transcript.push(format!("{sql} => {outcome}"));
    }
    for sql in dumps {
        let rows = conn.query(sql).await.expect("dump");
        transcript.push(format!("{sql} => {:?}", render_rows(&rows)));
    }
    transcript
}

fn assert_matches_stock(label: &str, got: &[String], want: &[String]) {
    for (line, (got, want)) in got.iter().zip(want).enumerate() {
        assert_eq!(got, want, "{label} parity line {line}");
    }
    assert_eq!(got.len(), want.len(), "{label} transcript length");
}

fn run_case(label: &str, schema: &'static str, statements: &'static [&str], dumps: &'static [&str]) {
    let expected = stock_transcript(schema, statements, dumps);
    let in_memory = expected.clone();
    asupersync::test_utils::run_test(|| async move {
        let conn = Connection::open(":memory:").await.expect("open");
        let got = fsqlite_transcript(&conn, schema, statements, dumps).await;
        assert_matches_stock(&format!("{label} in-memory"), &got, &in_memory);
    });
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("raise_marker.db");
    let path_str = path.to_str().expect("utf-8 path").to_owned();
    asupersync::test_utils::run_test(|| async move {
        let conn = Connection::open(&path_str).await.expect("open");
        let got = fsqlite_transcript(&conn, schema, statements, dumps).await;
        assert_matches_stock(&format!("{label} file-backed"), &got, &expected);
    });
    let checked = rusqlite::Connection::open(&path).expect("stock open of fsqlite file");
    let verdict: String = checked
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("integrity_check");
    assert_eq!(verdict, "ok", "{label}: stock integrity_check of the fsqlite file");
}

#[test]
fn raise_select_branches_and_columns_fire_like_stock() {
    run_case("raise", SCHEMA, STATEMENTS, DUMPS);
}

#[test]
fn persistent_trigger_names_never_bind_to_temp_like_stock() {
    run_case("pinning", PIN_SCHEMA, PIN_STATEMENTS, PIN_DUMPS);
}
