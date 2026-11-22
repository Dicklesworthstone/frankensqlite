#![recursion_limit = "512"]

//! bd-d8s37 — Stock SQLite 3.54.1 differential oracle for parent-side foreign
//! key ON UPDATE actions (CASCADE / RESTRICT / NO ACTION / SET NULL /
//! SET DEFAULT) through both ordinary UPDATE and UPSERT (`ON CONFLICT DO
//! UPDATE`), and their exact ordering relative to parent and child
//! `AFTER UPDATE` triggers.
//!
//! Every scenario runs on BOTH `:memory:` and file-backed databases, direct and
//! prepared, with stock `rusqlite` live in the same test invocation:
//! - Point 3: `ON UPDATE CASCADE` finishes updating children BEFORE the
//!   parent's `AFTER UPDATE` trigger fires (both plain `UPDATE` and `UPSERT`).
//! - Point 4: `ON UPDATE RESTRICT` (and `NO ACTION` / `CASCADE` / `SET NULL` /
//!   `SET DEFAULT`) is enforced through `UPSERT` (`ON CONFLICT(pk)`,
//!   `ON CONFLICT(unique_col)`, targetless `ON CONFLICT DO UPDATE`, expression
//!   index targets, multi-row statements, and `WITHOUT ROWID` parent tables).
//! - Negative controls: updating a parent key with no referencing children,
//!   updating only non-FK columns, a false `DO UPDATE ... WHERE`, a
//!   non-conflicting insert, and an affinity-coerced no-op key update all
//!   succeed identically to stock.

use fsqlite_core::connection::Connection;
use fsqlite_error::FrankenError;
use fsqlite_types::value::SqliteValue;

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Ok(usize),
    FkViolation,
    PkOrUnique,
    OtherErr(String),
}

fn classify_franken(r: Result<usize, FrankenError>) -> Outcome {
    match r {
        Ok(n) => Outcome::Ok(n),
        Err(FrankenError::ForeignKeyViolation) => Outcome::FkViolation,
        Err(error) if error.is_unique_conflict() => Outcome::PkOrUnique,
        Err(error) => Outcome::OtherErr(error.to_string()),
    }
}

fn classify_rusqlite(r: rusqlite::Result<usize>) -> Outcome {
    match r {
        Ok(n) => Outcome::Ok(n),
        Err(rusqlite::Error::SqliteFailure(e, msg)) => match e.extended_code {
            // SQLITE_CONSTRAINT_FOREIGNKEY (787), and SQLITE_CONSTRAINT_TRIGGER (1811)
            // which stock SQLite emits for `ON UPDATE/DELETE RESTRICT` (compiled as an
            // internal FK action trigger raising `FOREIGN KEY constraint failed`).
            787 => Outcome::FkViolation,
            1811 if msg.as_deref() == Some("FOREIGN KEY constraint failed") => Outcome::FkViolation,
            1555 | 2067 => Outcome::PkOrUnique,
            _ => Outcome::OtherErr(format!("{e:?}: {msg:?}")),
        },
        Err(error) => Outcome::OtherErr(error.to_string()),
    }
}

async fn dump_franken(conn: &Connection, sql: &str) -> Vec<Vec<SqliteValue>> {
    conn.query(sql)
        .await
        .unwrap()
        .iter()
        .map(|row| row.values().to_vec())
        .collect()
}

fn dump_rusqlite(conn: &rusqlite::Connection, sql: &str, ncols: usize) -> Vec<Vec<SqliteValue>> {
    let mut stmt = conn.prepare(sql).unwrap();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(ncols);
        for i in 0..ncols {
            let val = match row.get_ref(i).unwrap() {
                rusqlite::types::ValueRef::Null => SqliteValue::Null,
                rusqlite::types::ValueRef::Integer(n) => SqliteValue::Integer(n),
                rusqlite::types::ValueRef::Real(x) => SqliteValue::Float(x),
                rusqlite::types::ValueRef::Text(t) => {
                    SqliteValue::Text(String::from_utf8_lossy(t).into_owned().into())
                }
                rusqlite::types::ValueRef::Blob(b) => SqliteValue::Blob(b.into()),
            };
            out.push(val);
        }
        Ok(out)
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

async fn open_pair(
    file_backed: bool,
    name: &str,
) -> (Option<tempfile::TempDir>, Connection, rusqlite::Connection) {
    if file_backed {
        let dir = tempfile::tempdir().unwrap();
        let f_path = dir.path().join(format!("{name}_f.db"));
        let r_path = dir.path().join(format!("{name}_r.db"));
        let f = Connection::open(f_path.to_str().unwrap()).await.unwrap();
        let r = rusqlite::Connection::open(&r_path).unwrap();
        (Some(dir), f, r)
    } else {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        (None, f, r)
    }
}

async fn exec_both_setup(f: &Connection, r: &rusqlite::Connection, stmts: &[&str]) {
    for sql in stmts {
        f.execute(sql).await.unwrap();
        r.execute(sql, []).unwrap();
    }
}

#[test]
fn bd_d8s37_point3_on_update_cascade_runs_before_parent_after_update_trigger() {
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            for prepared in [false, true] {
                let (_dir, f, r) = open_pair(file_backed, "cascade_order").await;
                exec_both_setup(
                    &f,
                    &r,
                    &[
                        "PRAGMA foreign_keys = ON;",
                        "CREATE TABLE parent (id INTEGER PRIMARY KEY, val TEXT);",
                        "CREATE TABLE child (
                            id INTEGER PRIMARY KEY,
                            pid INTEGER REFERENCES parent(id) ON UPDATE CASCADE
                        );",
                        "CREATE TABLE log (seq INTEGER PRIMARY KEY AUTOINCREMENT, msg TEXT);",
                        "CREATE TRIGGER child_au AFTER UPDATE ON child BEGIN
                            INSERT INTO log(msg)
                            SELECT 'child_au:' || OLD.pid || '->' || NEW.pid || ':p=' ||
                                COALESCE((SELECT group_concat(id || '=' || val, ',')
                                          FROM (SELECT id, val FROM parent ORDER BY id)), 'none');
                        END;",
                        "CREATE TRIGGER parent_au AFTER UPDATE ON parent BEGIN
                            INSERT INTO log(msg)
                            SELECT 'parent_au:' || OLD.id || '->' || NEW.id || ':c=' ||
                                COALESCE((SELECT group_concat(id || '=' || pid, ',')
                                          FROM (SELECT id, pid FROM child ORDER BY id)), 'none');
                        END;",
                        "INSERT INTO parent VALUES (1, 'p1'), (2, 'p2'), (3, 'orphan_free');",
                        "INSERT INTO child VALUES (10, 1), (11, 1), (20, 2);",
                    ],
                )
                .await;

                // 1. Single-row plain UPDATE on parent with ON UPDATE CASCADE.
                // 2. Multi-row plain UPDATE on parent with ON UPDATE CASCADE.
                // 3. UPSERT (ON CONFLICT(id) DO UPDATE) changing parent PK with ON UPDATE CASCADE.
                // 4. Negative control: parent update with no child rows still fires parent_au cleanly.
                let ops = [
                    "UPDATE parent SET id = 100, val = 'p1_upd' WHERE id = 1;",
                    "UPDATE parent SET id = id + 1000 WHERE id IN (2, 100);",
                    "INSERT INTO parent VALUES (1100, 'upsert_hit')
                        ON CONFLICT(id) DO UPDATE SET id = 2000, val = excluded.val;",
                    "INSERT INTO parent VALUES (3, 'no_children_upsert')
                        ON CONFLICT(id) DO UPDATE SET id = 30, val = excluded.val;",
                ];

                for (step, sql) in ops.iter().enumerate() {
                    let f_res = if prepared {
                        let stmt = f.prepare(sql).await.unwrap();
                        stmt.execute().await
                    } else {
                        f.execute(sql).await
                    };
                    let r_res = if prepared {
                        let mut stmt = r.prepare(sql).unwrap();
                        stmt.execute([])
                    } else {
                        r.execute(sql, [])
                    };
                    assert_eq!(
                        classify_franken(f_res),
                        classify_rusqlite(r_res),
                        "file={file_backed} prepared={prepared} step={step} sql={sql}"
                    );
                    assert_eq!(
                        dump_franken(&f, "SELECT seq, msg FROM log ORDER BY seq;").await,
                        dump_rusqlite(&r, "SELECT seq, msg FROM log ORDER BY seq;", 2),
                        "trigger observation log diverged at file={file_backed} prepared={prepared} step={step}"
                    );
                    assert_eq!(
                        dump_franken(&f, "SELECT id, val FROM parent ORDER BY id;").await,
                        dump_rusqlite(&r, "SELECT id, val FROM parent ORDER BY id;", 2),
                        "parent table diverged at file={file_backed} prepared={prepared} step={step}"
                    );
                    assert_eq!(
                        dump_franken(&f, "SELECT id, pid FROM child ORDER BY id;").await,
                        dump_rusqlite(&r, "SELECT id, pid FROM child ORDER BY id;", 2),
                        "child table diverged at file={file_backed} prepared={prepared} step={step}"
                    );
                }

                // Sanity: parent_au MUST have observed cascaded child rows (never stale old pids).
                let log_rows = dump_franken(&f, "SELECT msg FROM log ORDER BY seq;").await;
                let parent_logs: Vec<String> = log_rows
                    .iter()
                    .filter_map(|row| match &row[0] {
                        SqliteValue::Text(s) if s.starts_with("parent_au:") => Some(s.to_string()),
                        _ => None,
                    })
                    .collect();
                assert!(
                    parent_logs
                        .iter()
                        .any(|s| s.contains("parent_au:1->100:c=10=100,11=100,20=2")),
                    "plain UPDATE parent_au must see cascaded children: {parent_logs:?}"
                );
                assert!(
                    parent_logs
                        .iter()
                        .any(|s| s.contains("parent_au:1100->2000:c=10=2000,11=2000,20=1002")),
                    "UPSERT DO UPDATE parent_au must see cascaded children: {parent_logs:?}"
                );

                f.close().await.unwrap();
            }
        }
    });
}

#[test]
fn bd_d8s37_point4_on_update_restrict_and_actions_enforced_through_upsert() {
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            for prepared in [false, true] {
                let (_dir, f, r) = open_pair(file_backed, "restrict_upsert").await;
                exec_both_setup(
                    &f,
                    &r,
                    &[
                        "PRAGMA foreign_keys = ON;",
                        "CREATE TABLE parent (
                            id INTEGER PRIMARY KEY,
                            code TEXT UNIQUE,
                            tag TEXT,
                            val TEXT
                        );",
                        "CREATE UNIQUE INDEX idx_parent_tag_lower ON parent(lower(tag));",
                        "CREATE TABLE child_id (
                            id INTEGER PRIMARY KEY,
                            pid INTEGER REFERENCES parent(id) ON UPDATE RESTRICT
                        );",
                        "CREATE TABLE child_code (
                            id INTEGER PRIMARY KEY,
                            pcode TEXT REFERENCES parent(code) ON UPDATE RESTRICT
                        );",
                        "INSERT INTO parent VALUES
                            (1, 'c1', 'Alpha', 'v1'),
                            (2, 'c2', 'Beta', 'v2'),
                            (3, 'c3', 'Gamma', 'v3');",
                        "INSERT INTO child_id VALUES (10, 1);",
                        "INSERT INTO child_code VALUES (20, 'c2');",
                    ],
                )
                .await;

                let cases = [
                    // 0. RESTRICT via ON CONFLICT(id) changing referenced PK -> FkViolation
                    "INSERT INTO parent VALUES (1, 'x', 'Delta', 'new')
                        ON CONFLICT(id) DO UPDATE SET id = 100;",
                    // 1. RESTRICT via ON CONFLICT(code) changing referenced UNIQUE key -> FkViolation
                    "INSERT INTO parent VALUES (99, 'c2', 'Delta', 'new')
                        ON CONFLICT(code) DO UPDATE SET code = 'c2_new';",
                    // 2. RESTRICT via targetless ON CONFLICT DO UPDATE hitting UNIQUE(code) -> FkViolation
                    "INSERT INTO parent VALUES (99, 'c2', 'Delta', 'new')
                        ON CONFLICT DO UPDATE SET code = 'c2_targetless';",
                    // 3. RESTRICT via expression index target ON CONFLICT(lower(tag)) -> FkViolation
                    "INSERT INTO parent VALUES (99, 'x', 'BETA', 'new')
                        ON CONFLICT(lower(tag)) DO UPDATE SET code = 'c2_expr';",
                    // 4. Multi-row UPSERT where row 1 inserts and row 2 violates RESTRICT -> rolls back whole stmt!
                    "INSERT INTO parent VALUES (50, 'c50', 'Fifty', 'v50'), (1, 'x', 'Other', 'v_bad')
                        ON CONFLICT(id) DO UPDATE SET id = 101;",
                    // 5. Negative control: false WHERE guard on DO UPDATE (no-op) -> Ok(0)
                    "INSERT INTO parent VALUES (1, 'x', 'Other', 'new')
                        ON CONFLICT(id) DO UPDATE SET id = 102 WHERE parent.val = 'nope';",
                    // 6. Negative control: DO UPDATE modifies only non-FK column -> Ok(1)
                    "INSERT INTO parent VALUES (1, 'x', 'Other', 'v1_updated')
                        ON CONFLICT(id) DO UPDATE SET val = excluded.val;",
                    // 7. Negative control: affinity-coerced key equals existing key ('1' -> 1) -> Ok(1)
                    "INSERT INTO parent VALUES (1, 'x', 'Other', 'v1_same_key')
                        ON CONFLICT(id) DO UPDATE SET id = '1', val = excluded.val;",
                    // 8. Negative control: parent row 3 has NO child rows -> changing id & code succeeds!
                    "INSERT INTO parent VALUES (3, 'x', 'Other', 'v3_moved')
                        ON CONFLICT(id) DO UPDATE SET id = 30, code = 'c30', val = excluded.val;",
                    // 9. Negative control: non-conflicting insert succeeds!
                    "INSERT INTO parent VALUES (4, 'c4', 'Four', 'v4')
                        ON CONFLICT(id) DO UPDATE SET id = 40;",
                ];

                for (idx, sql) in cases.iter().enumerate() {
                    let f_res = if prepared {
                        let stmt = f.prepare(sql).await.unwrap();
                        stmt.execute().await
                    } else {
                        f.execute(sql).await
                    };
                    let r_res = if prepared {
                        let mut stmt = r.prepare(sql).unwrap();
                        stmt.execute([])
                    } else {
                        r.execute(sql, [])
                    };
                    let fo = classify_franken(f_res);
                    let ro = classify_rusqlite(r_res);
                    assert_eq!(
                        fo, ro,
                        "outcome mismatch file={file_backed} prepared={prepared} case={idx} sql={sql}"
                    );
                    if idx <= 4 {
                        assert_eq!(
                            fo,
                            Outcome::FkViolation,
                            "RESTRICT case {idx} must fail with ForeignKeyViolation"
                        );
                    }
                    assert_eq!(
                        dump_franken(&f, "SELECT id, code, tag, val FROM parent ORDER BY id;")
                            .await,
                        dump_rusqlite(&r, "SELECT id, code, tag, val FROM parent ORDER BY id;", 4),
                        "parent table diverged after case {idx} (file={file_backed} prepared={prepared})"
                    );
                    assert_eq!(
                        dump_franken(&f, "SELECT id, pid FROM child_id ORDER BY id;").await,
                        dump_rusqlite(&r, "SELECT id, pid FROM child_id ORDER BY id;", 2),
                        "child_id diverged after case {idx}"
                    );
                    assert_eq!(
                        dump_franken(&f, "SELECT id, pcode FROM child_code ORDER BY id;").await,
                        dump_rusqlite(&r, "SELECT id, pcode FROM child_code ORDER BY id;", 2),
                        "child_code diverged after case {idx}"
                    );
                }

                f.close().await.unwrap();
            }
        }
    });
}

#[test]
fn bd_d8s37_upsert_set_null_set_default_no_action_and_without_rowid_oracle() {
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            let (_dir, f, r) = open_pair(file_backed, "actions_without_rowid").await;
            exec_both_setup(
                &f,
                &r,
                &[
                    "PRAGMA foreign_keys = ON;",
                    "CREATE TABLE parent_wr (
                        k TEXT PRIMARY KEY,
                        v INTEGER
                    ) WITHOUT ROWID;",
                    "CREATE TABLE child_restrict (
                        id INTEGER PRIMARY KEY,
                        pk TEXT REFERENCES parent_wr(k) ON UPDATE RESTRICT
                    );",
                    "CREATE TABLE child_set_null (
                        id INTEGER PRIMARY KEY,
                        pk TEXT REFERENCES parent_wr(k) ON UPDATE SET NULL
                    );",
                    "CREATE TABLE child_set_default (
                        id INTEGER PRIMARY KEY,
                        pk TEXT DEFAULT 'fallback' REFERENCES parent_wr(k) ON UPDATE SET DEFAULT
                    );",
                    "INSERT INTO parent_wr VALUES ('fallback', 0), ('k1', 1), ('k2', 2), ('k3', 3);",
                    "INSERT INTO child_restrict VALUES (1, 'k1');",
                    "INSERT INTO child_set_null VALUES (2, 'k2');",
                    "INSERT INTO child_set_default VALUES (3, 'k2');",
                ],
            )
            .await;

            // 1. WITHOUT ROWID parent with ON UPDATE RESTRICT through UPSERT -> FkViolation
            let sql_restrict = "INSERT INTO parent_wr VALUES ('k1', 99)
                ON CONFLICT(k) DO UPDATE SET k = 'k1_moved';";
            assert_eq!(
                classify_franken(f.execute(sql_restrict).await),
                classify_rusqlite(r.execute(sql_restrict, []))
            );
            assert_eq!(
                classify_franken(f.execute(sql_restrict).await),
                Outcome::FkViolation
            );

            // 2. WITHOUT ROWID parent with ON UPDATE SET NULL + SET DEFAULT through UPSERT -> Ok(1)
            let sql_actions = "INSERT INTO parent_wr VALUES ('k2', 22)
                ON CONFLICT(k) DO UPDATE SET k = 'k2_moved', v = excluded.v;";
            assert_eq!(
                classify_franken(f.execute(sql_actions).await),
                classify_rusqlite(r.execute(sql_actions, []))
            );

            // 3. Negative control: WITHOUT ROWID parent key 'k3' has no children -> Ok(1)
            let sql_free = "INSERT INTO parent_wr VALUES ('k3', 33)
                ON CONFLICT(k) DO UPDATE SET k = 'k3_moved', v = excluded.v;";
            assert_eq!(
                classify_franken(f.execute(sql_free).await),
                classify_rusqlite(r.execute(sql_free, []))
            );

            assert_eq!(
                dump_franken(&f, "SELECT k, v FROM parent_wr ORDER BY k;").await,
                dump_rusqlite(&r, "SELECT k, v FROM parent_wr ORDER BY k;", 2),
                "parent_wr diverged (file={file_backed})"
            );
            assert_eq!(
                dump_franken(&f, "SELECT id, pk FROM child_restrict ORDER BY id;").await,
                dump_rusqlite(&r, "SELECT id, pk FROM child_restrict ORDER BY id;", 2),
                "child_restrict diverged (file={file_backed})"
            );
            assert_eq!(
                dump_franken(&f, "SELECT id, pk FROM child_set_null ORDER BY id;").await,
                dump_rusqlite(&r, "SELECT id, pk FROM child_set_null ORDER BY id;", 2),
                "child_set_null diverged (file={file_backed})"
            );
            assert_eq!(
                dump_franken(&f, "SELECT id, pk FROM child_set_default ORDER BY id;").await,
                dump_rusqlite(&r, "SELECT id, pk FROM child_set_default ORDER BY id;", 2),
                "child_set_default diverged (file={file_backed})"
            );

            f.close().await.unwrap();
        }
    });
}
