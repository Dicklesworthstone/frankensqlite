#![recursion_limit = "512"]
//! Trigger timing / OLD-NEW binding differential oracle (vs rusqlite / bundled
//! SQLite). Verifies that table-trigger firing timing, OLD/NEW binding, WHEN /
//! `UPDATE OF` scoping, fire counts, creation order, RAISE semantics, FK-cascade
//! interaction, and the `recursive_triggers=OFF` default all match stock. Each
//! case runs one DML and compares post-DML STATE (main + audit tables), which
//! captures every trigger side-effect. Grown from a broad oracle-probe sweep
//! (24 cases across common, adversarial, and esoteric edges — all matched stock).

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tf(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".into(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(_) => "B".into(),
    }
}
fn tr(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".into(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(_) => "B".into(),
    }
}
async fn fq(f: &Connection, sql: &str) -> Vec<Vec<String>> {
    match f.query_with_params(sql, &[]).await {
        Ok(rows) => rows
            .iter()
            .map(|r| r.values().iter().map(tf).collect())
            .collect(),
        Err(e) => vec![vec![format!("<ERR {e:?}>")]],
    }
}
fn rqe(r: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut st = match r.prepare(sql) {
        Ok(s) => s,
        Err(e) => return vec![vec![format!("<ERR {e}>")]],
    };
    let n = st.column_count();
    match st
        .query_map([], |row| {
            Ok((0..n)
                .map(|i| tr(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect::<Vec<_>>())
        })
        .and_then(|it| it.collect::<Result<Vec<_>, _>>())
    {
        Ok(r) => r,
        Err(e) => vec![vec![format!("<ERR {e}>")]],
    }
}
/// Run setup + one DML on both engines, then assert a no-param verify SELECT of the
/// post-DML state (main + audit tables) agrees.
async fn state(setup: &[&str], dml: &str, verify: &str, msg: &str) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in setup {
        let _ = f.execute(s).await;
        let _ = r.execute_batch(s);
    }
    let _ = f.execute(dml).await;
    let _ = r.execute_batch(dml);
    let ff = fq(&f, verify).await;
    let rr = rqe(&r, verify);
    assert_eq!(
        ff, rr,
        "{msg}\n  dml={dml}\n  frank ={ff:?}\n  sqlite={rr:?}"
    );
}
const AUDIT: &str =
    "CREATE TABLE audit(seq INTEGER PRIMARY KEY, op TEXT, oldv INTEGER, newv INTEGER)";
const AUD2: &str = "CREATE TABLE audit(seq INTEGER PRIMARY KEY, op TEXT, v INTEGER)";

#[test]
fn trigger_timing_common() {
    asupersync::test_utils::run_test(|| async {
        state(&["CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)", AUDIT, "INSERT INTO t VALUES (1, 10)",
                "CREATE TRIGGER au AFTER UPDATE ON t BEGIN INSERT INTO audit(op,oldv,newv) VALUES ('u', OLD.a, NEW.a); END"],
            "UPDATE t SET a = 99 WHERE id = 1", "SELECT op, oldv, newv FROM audit", "AFTER UPDATE OLD/NEW").await;
        state(&["CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)", AUDIT, "INSERT INTO t VALUES (1, 10)",
                "CREATE TRIGGER au AFTER UPDATE ON t WHEN OLD.a <> NEW.a BEGIN INSERT INTO audit(op,oldv,newv) VALUES ('u', OLD.a, NEW.a); END"],
            "UPDATE t SET a = a WHERE id = 1", "SELECT count(*) FROM audit", "WHEN no-op no fire").await;
        state(&["CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)", AUDIT, "INSERT INTO t VALUES (1, 10, 20)",
                "CREATE TRIGGER au AFTER UPDATE OF a ON t BEGIN INSERT INTO audit(op,oldv,newv) VALUES ('a', OLD.a, NEW.a); END"],
            "UPDATE t SET b = 77 WHERE id = 1", "SELECT count(*) FROM audit", "UPDATE OF col scope").await;
        state(&["CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)", AUDIT,
                "CREATE TRIGGER a1 AFTER INSERT ON t BEGIN INSERT INTO audit(op,newv) VALUES ('t1', NEW.a); END",
                "CREATE TRIGGER a2 AFTER INSERT ON t BEGIN INSERT INTO audit(op,newv) VALUES ('t2', NEW.a); END"],
            "INSERT INTO t(id,a) VALUES (1, 5)", "SELECT op, newv FROM audit ORDER BY seq", "creation order").await;
        state(&["CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)", AUDIT, "INSERT INTO t VALUES (1,10),(2,20)",
                "CREATE TRIGGER ad AFTER DELETE ON t BEGIN INSERT INTO audit(op,oldv) VALUES ('d', OLD.a); END"],
            "DELETE FROM t WHERE id = 2", "SELECT op, oldv FROM audit", "AFTER DELETE OLD").await;
        state(&["CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)",
                "CREATE TRIGGER bi BEFORE INSERT ON t WHEN NEW.a < 0 BEGIN SELECT RAISE(IGNORE); END"],
            "INSERT INTO t(id,a) VALUES (1, -5)", "SELECT count(*) FROM t", "RAISE(IGNORE) skip").await;
        state(&["CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)", "CREATE TABLE t2(id INTEGER PRIMARY KEY, a INTEGER)", AUD2,
                "CREATE TRIGGER t_ins AFTER INSERT ON t BEGIN INSERT INTO t2(id,a) VALUES (NEW.id, NEW.a + 100); END",
                "CREATE TRIGGER t2_ins AFTER INSERT ON t2 BEGIN INSERT INTO audit(op,v) VALUES ('t2ins', NEW.a); END"],
            "INSERT INTO t(id,a) VALUES (1, 5)", "SELECT (SELECT a FROM t2 WHERE id=1), (SELECT v FROM audit)", "recursion chain").await;
        state(&["CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)", AUDIT, "INSERT INTO t VALUES (1, 10)",
                "CREATE TRIGGER bu BEFORE UPDATE ON t BEGIN INSERT INTO audit(op,oldv,newv) VALUES ('b', OLD.a, NEW.a); END"],
            "UPDATE t SET a = 42 WHERE id = 1", "SELECT a FROM t", "BEFORE UPDATE visibility").await;
    });
}

#[test]
fn trigger_body_table_aliases_match_sqlite() {
    asupersync::test_utils::run_test(|| async {
        state(
            &[
                "CREATE TABLE source(value INTEGER)",
                "CREATE TABLE audit(id INTEGER PRIMARY KEY, value INTEGER)",
                "INSERT INTO audit VALUES (1, 10), (2, 20)",
                "CREATE TRIGGER aliases AFTER INSERT ON source BEGIN \
                 UPDATE audit AS update_target \
                    SET value = update_target.value + NEW.value \
                  WHERE update_target.id = 1; \
                 INSERT INTO audit AS insert_target(id, value) VALUES (3, NEW.value); \
                 DELETE FROM audit AS delete_target WHERE delete_target.id = 2; \
                 END",
            ],
            "INSERT INTO source VALUES (7)",
            "SELECT id, value FROM audit ORDER BY id",
            "trigger-body INSERT, UPDATE, and DELETE aliases",
        )
        .await;
    });
}

#[test]
fn trigger_timing_adversarial() {
    asupersync::test_utils::run_test(|| async {
        state(&["CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)", AUD2, "INSERT INTO t VALUES (1, 10)",
                "CREATE TRIGGER ad AFTER DELETE ON t BEGIN INSERT INTO audit(op,v) VALUES ('del', OLD.a); END",
                "CREATE TRIGGER ai AFTER INSERT ON t BEGIN INSERT INTO audit(op,v) VALUES ('ins', NEW.a); END"],
            "INSERT OR REPLACE INTO t(id,a) VALUES (1, 99)", "SELECT op, v FROM audit ORDER BY seq", "REPLACE del+ins").await;
        state(&["CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)", AUD2, "INSERT INTO t VALUES (1,10),(2,20),(3,30)",
                "CREATE TRIGGER ad AFTER DELETE ON t BEGIN INSERT INTO audit(op,v) VALUES ('d', OLD.a); END"],
            "DELETE FROM t", "SELECT op, v FROM audit ORDER BY seq", "multi-row delete fire count").await;
        state(&["CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)", AUD2, "INSERT INTO t VALUES (1, 10)",
                "CREATE TRIGGER au AFTER UPDATE OF a ON t BEGIN INSERT INTO audit(op,v) VALUES ('a', NEW.a); END"],
            "UPDATE t SET a = a WHERE id = 1", "SELECT count(*) FROM audit", "UPDATE OF unchanged fires").await;
        state(&["CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)",
                "CREATE TRIGGER bi BEFORE INSERT ON t WHEN NEW.a < 0 BEGIN SELECT RAISE(ABORT, 'neg'); END"],
            "INSERT INTO t(id,a) VALUES (1, 10), (2, -5), (3, 30)", "SELECT id, a FROM t ORDER BY id", "RAISE(ABORT) rollback").await;
        state(&["CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)",
                "CREATE TRIGGER self_ins AFTER INSERT ON t WHEN NEW.a < 5 BEGIN INSERT INTO t(id,a) VALUES (NEW.id + 100, NEW.a + 1); END"],
            "INSERT INTO t(id,a) VALUES (1, 1)", "SELECT count(*), max(a) FROM t", "recursive_triggers OFF default").await;
        state(&["CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)", AUD2, "INSERT INTO t VALUES (1,10),(2,20)",
                "CREATE TRIGGER ad AFTER DELETE ON t BEGIN INSERT INTO audit(op,v) VALUES ('cnt', (SELECT count(*) FROM t)); END"],
            "DELETE FROM t WHERE id = 1", "SELECT op, v FROM audit", "subquery on OLD").await;
        state(&["CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER)", AUD2, "INSERT INTO t VALUES (1, 10, 20, 30)",
                "CREATE TRIGGER au AFTER UPDATE OF a, b ON t BEGIN INSERT INTO audit(op,v) VALUES ('ab', NEW.b); END"],
            "UPDATE t SET c = 99 WHERE id = 1", "SELECT count(*) FROM audit", "multi-column UPDATE OF scope").await;
        state(&["CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)", AUD2, "INSERT INTO t VALUES (1, 10)",
                "CREATE TRIGGER au AFTER UPDATE ON t BEGIN INSERT INTO audit(op,v) VALUES ('after', NEW.a); END",
                "CREATE TRIGGER bu BEFORE UPDATE ON t BEGIN INSERT INTO audit(op,v) VALUES ('before', OLD.a); END"],
            "UPDATE t SET a = 42 WHERE id = 1", "SELECT op, v FROM audit ORDER BY seq", "BEFORE then AFTER order").await;
    });
}

#[test]
fn trigger_timing_esoteric() {
    asupersync::test_utils::run_test(|| async {
        state(&["PRAGMA foreign_keys=ON", "CREATE TABLE p(id INTEGER PRIMARY KEY)",
                "CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) ON DELETE CASCADE, v INTEGER)",
                AUD2, "INSERT INTO p VALUES (1)", "INSERT INTO c VALUES (10, 1, 100), (11, 1, 101)",
                "CREATE TRIGGER cad AFTER DELETE ON c BEGIN INSERT INTO audit(op,v) VALUES ('cdel', OLD.v); END"],
            "DELETE FROM p WHERE id = 1", "SELECT op, v FROM audit ORDER BY seq", "FK cascade fires child trigger").await;
        state(&["CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)", AUD2, "INSERT INTO t VALUES (1, 10)",
                "CREATE TRIGGER au AFTER UPDATE ON t BEGIN INSERT INTO audit(op,v) VALUES ('oldid', OLD.id); INSERT INTO audit(op,v) VALUES ('newid', NEW.id); END"],
            "UPDATE t SET id = 5 WHERE id = 1", "SELECT op, v FROM audit ORDER BY seq", "UPDATE INTEGER PK OLD/NEW id").await;
        state(&["CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, a INTEGER)", AUD2, "INSERT INTO t(a) VALUES (10)",
                "CREATE TRIGGER ai AFTER INSERT ON t BEGIN INSERT INTO audit(op,v) VALUES ('newid', NEW.id); END"],
            "INSERT INTO t(a) VALUES (20)", "SELECT op, v FROM audit", "AUTOINCREMENT NEW.id").await;
        state(&["CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER DEFAULT 7, b INTEGER DEFAULT 8)", AUD2,
                "CREATE TRIGGER ai AFTER INSERT ON t BEGIN INSERT INTO audit(op,v) VALUES ('a', NEW.a); INSERT INTO audit(op,v) VALUES ('b', NEW.b); END"],
            "INSERT INTO t DEFAULT VALUES", "SELECT op, v FROM audit ORDER BY seq", "DEFAULT VALUES NEW").await;
        state(&["CREATE TABLE src(x INTEGER)", "INSERT INTO src VALUES (1),(2),(3)",
                "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)", AUD2,
                "CREATE TRIGGER ai AFTER INSERT ON t BEGIN INSERT INTO audit(op,v) VALUES ('ins', NEW.a); END"],
            "INSERT INTO t(id,a) SELECT x, x*10 FROM src", "SELECT op, v FROM audit ORDER BY seq", "INSERT...SELECT per-row fire").await;
        state(&["CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)",
                "CREATE TRIGGER bi BEFORE INSERT ON t WHEN NEW.a < 0 BEGIN SELECT RAISE(ROLLBACK, 'neg'); END"],
            "INSERT INTO t(id,a) VALUES (1, 10), (2, -5)", "SELECT count(*) FROM t", "RAISE(ROLLBACK) undoes statement").await;
        state(&["CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)", AUD2, "INSERT INTO t VALUES (1, 10)",
                "CREATE TRIGGER au AFTER UPDATE ON t BEGIN INSERT INTO audit(op,v) VALUES ('u', NEW.a); END"],
            "UPDATE t SET a = 99 WHERE id = 999", "SELECT count(*) FROM audit", "zero-row update no fire").await;
        state(&["CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)", "CREATE TABLE mirror(id INTEGER PRIMARY KEY, a INTEGER)",
                "INSERT INTO mirror VALUES (1, 0)", "INSERT INTO t VALUES (1, 10)",
                "CREATE TRIGGER au AFTER UPDATE ON t BEGIN UPDATE mirror SET a = NEW.a - OLD.a WHERE id = OLD.id; END"],
            "UPDATE t SET a = 30 WHERE id = 1", "SELECT id, a FROM mirror", "compound body NEW-OLD mix").await;
    });
}
