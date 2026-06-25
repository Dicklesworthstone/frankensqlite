//! bd-zsohl — Oracle-parity e2e: trigger semantics vs rusqlite (real SQLite).
//!
//! Covers AFTER INSERT/UPDATE/DELETE row triggers writing OLD/NEW into an audit
//! table, BEFORE triggers maintaining a counter, the WHEN guard, RAISE(IGNORE)
//! (silently skip the row) and RAISE(ABORT) (fail + roll back the statement),
//! INSTEAD OF triggers on a view, and multiple triggers on one event. The
//! trigger-firing DML runs on both engines; the resulting table state is then
//! compared. All data is fixed and deterministic.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn render_frank(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

fn frank_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rows = conn.query(sql).map_err(|e| e.to_string())?;
    Ok(rows
        .iter()
        .map(|row| row.values().iter().map(render_frank).collect())
        .collect())
}

fn sqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let v: rusqlite::types::Value = row.get_unwrap(i);
            out.push(match v {
                rusqlite::types::Value::Null => "NULL".to_owned(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f}"),
                rusqlite::types::Value::Text(s) => format!("'{s}'"),
                rusqlite::types::Value::Blob(b) => format!(
                    "X'{}'",
                    b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                ),
            });
        }
        Ok(out)
    })
    .map_err(|e| e.to_string())?
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| e.to_string())
}

/// Apply DDL/DML that must succeed on both engines (trigger-firing statements).
fn setup(stmts: &[&str]) -> (Connection, rusqlite::Connection) {
    let fconn = Connection::open(":memory:").expect("open frank");
    let rconn = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in stmts {
        fconn
            .execute(s)
            .unwrap_or_else(|e| panic!("frank `{s}`: {e}"));
        rconn
            .execute_batch(s)
            .unwrap_or_else(|e| panic!("rusqlite `{s}`: {e}"));
    }
    (fconn, rconn)
}

fn check(fconn: &Connection, rconn: &rusqlite::Connection, queries: &[&str], label: &str) {
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(fconn, q), sqlite_rows(rconn, q)) {
            (Ok(f), Ok(s)) if f == s => {}
            (Ok(f), Ok(s)) => {
                mismatches.push(format!("MISMATCH: {q}\n  frank: {f:?}\n  csql:  {s:?}"));
            }
            (Err(fe), Ok(s)) => {
                mismatches.push(format!(
                    "FRANK_ERR: {q}\n  frank: ERROR({fe})\n  csql:  {s:?}"
                ));
            }
            (Ok(f), Err(se)) => {
                mismatches.push(format!(
                    "CSQL_ERR: {q}\n  frank: {f:?}\n  csql: ERROR({se})"
                ));
            }
            (Err(_), Err(_)) => {}
        }
    }
    assert!(
        mismatches.is_empty(),
        "{label}: {} mismatch(es)\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

#[test]
fn trigger_after_insert_update_delete_audit() {
    let (f, r) = setup(&[
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
        "CREATE TABLE audit (seq INTEGER PRIMARY KEY AUTOINCREMENT, op TEXT, oldv INTEGER, newv INTEGER)",
        "CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN \
           INSERT INTO audit(op, oldv, newv) VALUES ('ins', NULL, NEW.v); END",
        "CREATE TRIGGER t_au AFTER UPDATE ON t BEGIN \
           INSERT INTO audit(op, oldv, newv) VALUES ('upd', OLD.v, NEW.v); END",
        "CREATE TRIGGER t_ad AFTER DELETE ON t BEGIN \
           INSERT INTO audit(op, oldv, newv) VALUES ('del', OLD.v, NULL); END",
        "INSERT INTO t VALUES (1,10),(2,20),(3,30)",
        "UPDATE t SET v = v + 5 WHERE id = 2",
        "DELETE FROM t WHERE id = 3",
    ]);
    check(
        &f,
        &r,
        &[
            "SELECT op, oldv, newv FROM audit ORDER BY seq",
            "SELECT id, v FROM t ORDER BY id",
        ],
        "trigger_after_insert_update_delete_audit",
    );
}

#[test]
fn trigger_before_insert_counter() {
    let (f, r) = setup(&[
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
        "CREATE TABLE counter (k TEXT PRIMARY KEY, n INTEGER)",
        "INSERT INTO counter VALUES ('inserts', 0)",
        "CREATE TRIGGER t_bi BEFORE INSERT ON t BEGIN \
           UPDATE counter SET n = n + 1 WHERE k = 'inserts'; END",
        "INSERT INTO t VALUES (1,10),(2,20)",
        "INSERT INTO t VALUES (3,30)",
    ]);
    check(
        &f,
        &r,
        &[
            "SELECT n FROM counter WHERE k = 'inserts'",
            "SELECT count(*) FROM t",
        ],
        "trigger_before_insert_counter",
    );
}

#[test]
fn trigger_when_guard() {
    let (f, r) = setup(&[
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
        "CREATE TABLE big (v INTEGER)",
        // Only log values >= 100.
        "CREATE TRIGGER t_ai AFTER INSERT ON t WHEN NEW.v >= 100 BEGIN \
           INSERT INTO big(v) VALUES (NEW.v); END",
        "INSERT INTO t VALUES (1,50),(2,150),(3,99),(4,200)",
    ]);
    check(
        &f,
        &r,
        &["SELECT v FROM big ORDER BY v"],
        "trigger_when_guard",
    );
}

#[test]
fn trigger_raise_ignore_skips_row() {
    let (f, r) = setup(&[
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
        // BEFORE INSERT raises IGNORE for negative values -> row silently skipped.
        "CREATE TRIGGER t_bi BEFORE INSERT ON t WHEN NEW.v < 0 BEGIN \
           SELECT RAISE(IGNORE); END",
        "INSERT INTO t VALUES (1,10)",
        "INSERT INTO t VALUES (2,-5)",
        "INSERT INTO t VALUES (3,30)",
    ]);
    check(
        &f,
        &r,
        &["SELECT id, v FROM t ORDER BY id", "SELECT count(*) FROM t"],
        "trigger_raise_ignore_skips_row",
    );
}

#[test]
fn trigger_raise_abort_rolls_back() {
    // RAISE(ABORT) must fail the statement on both engines and leave prior rows.
    let (f, r) = setup(&[
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
        "CREATE TRIGGER t_bi BEFORE INSERT ON t WHEN NEW.v < 0 BEGIN \
           SELECT RAISE(ABORT, 'no negatives'); END",
        "INSERT INTO t VALUES (1,10),(2,20)",
    ]);
    let fe = f.execute("INSERT INTO t VALUES (3,-1)");
    let re = r.execute_batch("INSERT INTO t VALUES (3,-1)");
    assert!(
        fe.is_err() && re.is_err(),
        "RAISE(ABORT): frank ok={:?}, rusqlite ok={:?} (both must error)",
        fe.is_ok(),
        re.is_ok()
    );
    // Prior rows intact; the aborted insert left nothing behind.
    check(
        &f,
        &r,
        &["SELECT id, v FROM t ORDER BY id", "SELECT count(*) FROM t"],
        "trigger_raise_abort_rolls_back",
    );
}

#[test]
fn trigger_instead_of_view_insert() {
    let (f, r) = setup(&[
        "CREATE TABLE base (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)",
        "CREATE VIEW v AS SELECT id, a + b AS total FROM base",
        // INSTEAD OF INSERT on the view writes split values into base.
        "CREATE TRIGGER v_ii INSTEAD OF INSERT ON v BEGIN \
           INSERT INTO base(id, a, b) VALUES (NEW.id, NEW.total, 0); END",
        "INSERT INTO v(id, total) VALUES (1, 100), (2, 250)",
    ]);
    check(
        &f,
        &r,
        &[
            "SELECT id, a, b FROM base ORDER BY id",
            "SELECT id, total FROM v ORDER BY id",
        ],
        "trigger_instead_of_view_insert",
    );
}

#[test]
fn trigger_multiple_on_same_event() {
    let (f, r) = setup(&[
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
        "CREATE TABLE log (tag TEXT, v INTEGER)",
        "CREATE TRIGGER t_ai1 AFTER INSERT ON t BEGIN \
           INSERT INTO log(tag, v) VALUES ('one', NEW.v); END",
        "CREATE TRIGGER t_ai2 AFTER INSERT ON t BEGIN \
           INSERT INTO log(tag, v) VALUES ('two', NEW.v * 2); END",
        "INSERT INTO t VALUES (1,10),(2,20)",
    ]);
    check(
        &f,
        &r,
        &[
            // Both triggers fire per row; ORDER BY makes the result deterministic.
            "SELECT tag, v FROM log ORDER BY v, tag",
            "SELECT count(*) FROM log",
        ],
        "trigger_multiple_on_same_event",
    );
}
