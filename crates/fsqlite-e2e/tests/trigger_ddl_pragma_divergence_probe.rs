//! Trigger / DDL / PRAGMA / transaction oracle DIVERGENCE PROBE (not a keeper):
//! exercise triggers (BEFORE/AFTER/INSTEAD OF, recursive, WHEN-gated, multi-
//! trigger ordering), single-connection transactional control (SAVEPOINT /
//! RELEASE / ROLLBACK TO, nested), schema DDL (ALTER TABLE ADD/RENAME/DROP
//! COLUMN, constraints), and PRAGMA / schema introspection (table_info,
//! index_list/index_info, foreign_key_list, sqlite_master) against C SQLite,
//! printing every scenario whose setup-error parity or final result set diverges.
//!
//! Single-connection only, so frank's MVCC concurrent-writer semantics never come
//! into play — savepoint rollback of a single connection reduces to the same
//! visible state as stock SQLite. Each scenario runs on a fresh frank+rusqlite
//! pair; the verify result set is compared as a SORTED multiset.
//!
//! `#[ignore]` by default; run with:
//!   cargo test -p fsqlite-e2e --test trigger_ddl_pragma_divergence_probe -- --ignored --nocapture
#![recursion_limit = "512"]

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn render_frank(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => format!("i:{n}"),
        SqliteValue::Float(f) => format!("f:{f}"),
        SqliteValue::Text(s) => format!("t:{s}"),
        SqliteValue::Blob(b) => format!("b:{}", b.len()),
    }
}

async fn frank_rows(conn: &Connection, sql: &str) -> Result<Vec<String>, String> {
    match conn.query(sql).await {
        Ok(rs) => {
            let mut rows: Vec<String> = rs
                .iter()
                .map(|row| {
                    row.values()
                        .iter()
                        .map(render_frank)
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .collect();
            rows.sort();
            Ok(rows)
        }
        Err(e) => Err(e.to_string()),
    }
}

fn sqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<String>, String> {
    let mut stmt = match conn.prepare(sql) {
        Ok(s) => s,
        Err(e) => return Err(format!("prep: {e}")),
    };
    let n = stmt.column_count();
    let out = stmt.query_map([], |row| {
        let mut cells = Vec::with_capacity(n);
        for i in 0..n {
            let cell = match row.get_ref(i)? {
                rusqlite::types::ValueRef::Null => "NULL".to_owned(),
                rusqlite::types::ValueRef::Integer(x) => format!("i:{x}"),
                rusqlite::types::ValueRef::Real(f) => format!("f:{f}"),
                rusqlite::types::ValueRef::Text(t) => format!("t:{}", String::from_utf8_lossy(t)),
                rusqlite::types::ValueRef::Blob(b) => format!("b:{}", b.len()),
            };
            cells.push(cell);
        }
        Ok(cells.join(","))
    });
    match out {
        Ok(iter) => {
            let collected: Result<Vec<String>, _> = iter.collect();
            match collected {
                Ok(mut rows) => {
                    rows.sort();
                    Ok(rows)
                }
                Err(e) => Err(format!("run: {e}")),
            }
        }
        Err(e) => Err(format!("map: {e}")),
    }
}

type Scenario = (&'static str, &'static [&'static str], &'static str);

fn scenarios() -> Vec<Scenario> {
    vec![
        // ---- triggers: AFTER INSERT/UPDATE/DELETE side effects ----
        (
            "after_insert_counter",
            &[
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
                "CREATE TABLE log(n INTEGER)",
                "CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO log VALUES (NEW.v); END",
                "INSERT INTO t(v) VALUES (10),(20),(30)",
            ],
            "SELECT n FROM log",
        ),
        (
            "after_update_oldnew",
            &[
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
                "CREATE TABLE log(o INTEGER, nw INTEGER)",
                "INSERT INTO t VALUES (1,10),(2,20)",
                "CREATE TRIGGER tr AFTER UPDATE ON t BEGIN INSERT INTO log VALUES (OLD.v, NEW.v); END",
                "UPDATE t SET v=v+1",
            ],
            "SELECT o, nw FROM log",
        ),
        (
            "after_delete",
            &[
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
                "CREATE TABLE log(v INTEGER)",
                "INSERT INTO t VALUES (1,10),(2,20),(3,30)",
                "CREATE TRIGGER tr AFTER DELETE ON t BEGIN INSERT INTO log VALUES (OLD.v); END",
                "DELETE FROM t WHERE v>=20",
            ],
            "SELECT v FROM log",
        ),
        (
            "before_insert_raise",
            &[
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
                "CREATE TRIGGER guard BEFORE INSERT ON t WHEN NEW.v<0 BEGIN SELECT RAISE(IGNORE); END",
                "INSERT INTO t(v) VALUES (10),(-5),(20),(-1),(30)",
            ],
            "SELECT v FROM t",
        ),
        (
            "trigger_when_gate",
            &[
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
                "CREATE TABLE log(v INTEGER)",
                "CREATE TRIGGER tr AFTER INSERT ON t WHEN NEW.v%2=0 BEGIN INSERT INTO log VALUES (NEW.v); END",
                "INSERT INTO t(v) VALUES (1),(2),(3),(4),(5),(6)",
            ],
            "SELECT v FROM log",
        ),
        (
            "multi_trigger_same_event",
            &[
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
                "CREATE TABLE log(src TEXT, v INTEGER)",
                "CREATE TRIGGER tr_a AFTER INSERT ON t BEGIN INSERT INTO log VALUES ('a', NEW.v); END",
                "CREATE TRIGGER tr_b AFTER INSERT ON t BEGIN INSERT INTO log VALUES ('b', NEW.v*10); END",
                "INSERT INTO t(v) VALUES (5)",
            ],
            "SELECT src, v FROM log",
        ),
        (
            "recursive_trigger_bounded",
            &[
                "PRAGMA recursive_triggers=ON",
                "CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER)",
                "CREATE TRIGGER tr AFTER INSERT ON t WHEN NEW.n<5 BEGIN INSERT INTO t(n) VALUES (NEW.n+1); END",
                "INSERT INTO t(n) VALUES (1)",
            ],
            "SELECT n FROM t ORDER BY n",
        ),
        (
            "instead_of_view",
            &[
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
                "CREATE VIEW vt AS SELECT id, v FROM t",
                "CREATE TRIGGER tr INSTEAD OF INSERT ON vt BEGIN INSERT INTO t(id, v) VALUES (NEW.id, NEW.v*2); END",
                "INSERT INTO vt(id, v) VALUES (1, 50)",
            ],
            "SELECT id, v FROM t",
        ),
        (
            "trigger_when_and_or",
            &[
                "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)",
                "CREATE TABLE log(id INTEGER)",
                "CREATE TRIGGER tr AFTER INSERT ON t WHEN NEW.a>0 AND (NEW.b>10 OR NEW.b<0) BEGIN INSERT INTO log VALUES (NEW.id); END",
                "INSERT INTO t VALUES (1,5,20),(2,5,5),(3,-1,20),(4,5,-3),(5,5,11)",
            ],
            "SELECT id FROM log",
        ),
        (
            "fk_cascade_delete",
            &[
                "PRAGMA foreign_keys=ON",
                "CREATE TABLE parent(id INTEGER PRIMARY KEY)",
                "CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE CASCADE)",
                "INSERT INTO parent VALUES (1),(2)",
                "INSERT INTO child VALUES (10,1),(11,1),(12,2)",
                "DELETE FROM parent WHERE id=1",
            ],
            "SELECT id, pid FROM child",
        ),
        // ---- transactions: single-connection SAVEPOINT / ROLLBACK ----
        (
            "savepoint_rollback_to",
            &[
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
                "INSERT INTO t VALUES (1,10)",
                "SAVEPOINT sp1",
                "INSERT INTO t VALUES (2,20)",
                "SAVEPOINT sp2",
                "INSERT INTO t VALUES (3,30)",
                "ROLLBACK TO sp2",
                "INSERT INTO t VALUES (4,40)",
                "RELEASE sp1",
            ],
            "SELECT id FROM t",
        ),
        (
            "nested_savepoint_release",
            &[
                "CREATE TABLE t(id INTEGER PRIMARY KEY)",
                "SAVEPOINT a",
                "INSERT INTO t VALUES (1)",
                "SAVEPOINT b",
                "INSERT INTO t VALUES (2)",
                "RELEASE b",
                "INSERT INTO t VALUES (3)",
                "ROLLBACK TO a",
                "INSERT INTO t VALUES (4)",
                "RELEASE a",
            ],
            "SELECT id FROM t",
        ),
        (
            "begin_rollback",
            &[
                "CREATE TABLE t(id INTEGER PRIMARY KEY)",
                "INSERT INTO t VALUES (1)",
                "BEGIN",
                "INSERT INTO t VALUES (2),(3)",
                "ROLLBACK",
                "INSERT INTO t VALUES (4)",
            ],
            "SELECT id FROM t",
        ),
        // ---- DDL: ALTER TABLE ----
        (
            "alter_add_column_default",
            &[
                "CREATE TABLE t(a INTEGER)",
                "INSERT INTO t VALUES (1),(2)",
                "ALTER TABLE t ADD COLUMN b TEXT DEFAULT 'z'",
                "INSERT INTO t(a) VALUES (3)",
            ],
            "SELECT a, b FROM t",
        ),
        (
            "alter_rename_column",
            &[
                "CREATE TABLE t(a INTEGER, b INTEGER)",
                "INSERT INTO t VALUES (1,2)",
                "ALTER TABLE t RENAME COLUMN b TO c",
            ],
            "SELECT a, c FROM t",
        ),
        (
            "alter_rename_table",
            &[
                "CREATE TABLE t(a INTEGER)",
                "INSERT INTO t VALUES (7)",
                "ALTER TABLE t RENAME TO t2",
            ],
            "SELECT a FROM t2",
        ),
        (
            "alter_drop_column",
            &[
                "CREATE TABLE t(a INTEGER, b INTEGER, c INTEGER)",
                "INSERT INTO t VALUES (1,2,3)",
                "ALTER TABLE t DROP COLUMN b",
            ],
            "SELECT * FROM t",
        ),
        (
            "check_constraint_reject",
            &[
                "CREATE TABLE t(a INTEGER CHECK(a>0))",
                "INSERT INTO t VALUES (5)",
                "INSERT OR IGNORE INTO t VALUES (-1)",
                "INSERT INTO t VALUES (7)",
            ],
            "SELECT a FROM t",
        ),
        // ---- PRAGMA / schema introspection ----
        (
            "pragma_table_info",
            &[
                "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT NOT NULL DEFAULT 'x', age INTEGER)",
            ],
            "SELECT cid, name, type, \"notnull\", dflt_value, pk FROM pragma_table_info('t')",
        ),
        (
            "pragma_index_list_info",
            &[
                "CREATE TABLE t(a INTEGER, b INTEGER)",
                "CREATE UNIQUE INDEX ix ON t(a, b DESC)",
            ],
            "SELECT name, \"unique\", origin FROM pragma_index_list('t')",
        ),
        (
            "pragma_index_info",
            &[
                "CREATE TABLE t(a INTEGER, b INTEGER)",
                "CREATE INDEX ix ON t(b, a)",
            ],
            "SELECT seqno, cid, name FROM pragma_index_info('ix')",
        ),
        (
            "pragma_foreign_key_list",
            &[
                "CREATE TABLE parent(id INTEGER PRIMARY KEY)",
                "CREATE TABLE child(id INTEGER, pid INTEGER REFERENCES parent(id) ON DELETE CASCADE)",
            ],
            "SELECT \"table\", \"from\", \"to\", on_delete FROM pragma_foreign_key_list('child')",
        ),
        (
            "sqlite_master_query",
            &[
                "CREATE TABLE zt(a INTEGER)",
                "CREATE INDEX zx ON zt(a)",
                "CREATE VIEW zv AS SELECT a FROM zt",
            ],
            "SELECT type, name, tbl_name FROM sqlite_master WHERE name LIKE 'z%'",
        ),
        (
            "pragma_table_xinfo_hidden",
            &["CREATE TABLE t(a INTEGER, b INTEGER GENERATED ALWAYS AS (a+1) VIRTUAL)"],
            "SELECT name, hidden FROM pragma_table_xinfo('t')",
        ),
    ]
}

#[test]
#[ignore = "trigger/DDL/PRAGMA/txn divergence probe (not a keeper): frank-vs-sqlite3 mismatches"]
fn trigger_ddl_pragma_divergence_probe() {
    asupersync::test_utils::run_test(|| async {
        let mut diverged = 0usize;
        let mut both_err = 0usize;
        for (name, setup, verify) in scenarios() {
            let f = Connection::open(":memory:").await.expect("open frank");
            let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
            let mut setup_diverged = false;
            for s in setup {
                let fe = f.execute(s).await;
                let re = r.execute_batch(s);
                if fe.is_ok() != re.is_ok() {
                    diverged += 1;
                    setup_diverged = true;
                    println!(
                        "SETUP-DIVERGE [{name}] `{s}`\n    frank: {:?}\n    csql:  {:?}",
                        fe.as_ref()
                            .map(|_| "ok")
                            .map_err(std::string::ToString::to_string),
                        re.as_ref().map(|_| "ok").map_err(|e| e.to_string())
                    );
                    break;
                }
            }
            if setup_diverged {
                continue;
            }
            let fr = frank_rows(&f, verify).await;
            let sr = sqlite_rows(&r, verify);
            match (&fr, &sr) {
                (Ok(a), Ok(b)) => {
                    if a != b {
                        diverged += 1;
                        println!("DIVERGE [{name}] {verify}\n    frank: {a:?}\n    csql:  {b:?}");
                    }
                }
                (Err(_), Err(_)) => both_err += 1,
                (Ok(a), Err(b)) => {
                    diverged += 1;
                    println!(
                        "F-OK/C-ERR [{name}] {verify}\n    frank: {a:?}\n    csql:  <err: {b}>"
                    );
                }
                (Err(a), Ok(b)) => {
                    diverged += 1;
                    println!(
                        "F-ERR/C-OK [{name}] {verify}\n    frank: <err: {a}>\n    csql:  {b:?}"
                    );
                }
            }
        }
        println!(
            "\nPROBE SUMMARY: {} scenarios, {} diverged, {} both-error",
            scenarios().len(),
            diverged,
            both_err
        );
    });
}
