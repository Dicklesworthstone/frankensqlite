#![recursion_limit = "512"]

//! Issue #111 — differential FK/conflict oracle for the direct-insert lane.
//!
//! The #111 fix lets explicit-rowid FK-checked INSERTs stay on the
//! direct-simple-insert lane (instead of bailing to the VDBE fallback) and
//! moves FK enforcement from a PRE-insert pre-check to a POST-insert check,
//! gated on a row actually landing. This test pins the resulting semantics to
//! stock SQLite (rusqlite) across the full conflict-clause × rowid-kind ×
//! FK-validity × self-reference matrix, so the performance fix cannot silently
//! change FK behavior.
//!
//! Each case is run on BOTH engines and the *normalized outcome* is compared:
//!   - `Ok(rows_changed)`
//!   - `FkViolation`
//!   - `PkOrUnique` (PRIMARY KEY / UNIQUE conflict)
//!   - `OtherErr`
//! plus the resulting table contents (so a silently-dropped or silently-kept
//! row is caught even when both engines return Ok).

use fsqlite_core::connection::Connection;
use fsqlite_error::FrankenError;
use fsqlite_types::value::SqliteValue;

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Ok(usize),
    FkViolation,
    PkOrUnique,
    OtherErr,
}

fn classify_franken(r: Result<usize, FrankenError>) -> Outcome {
    match r {
        Ok(n) => Outcome::Ok(n),
        Err(FrankenError::ForeignKeyViolation) => Outcome::FkViolation,
        // A UNIQUE, PRIMARY KEY or hidden-rowid conflict (bd-w4pr1.32).
        Err(error) if error.is_unique_conflict() => Outcome::PkOrUnique,
        Err(_) => Outcome::OtherErr,
    }
}

fn classify_rusqlite(r: rusqlite::Result<usize>) -> Outcome {
    match r {
        Ok(n) => Outcome::Ok(n),
        Err(rusqlite::Error::SqliteFailure(e, _)) => match e.extended_code {
            // SQLITE_CONSTRAINT_FOREIGNKEY
            787 => Outcome::FkViolation,
            // SQLITE_CONSTRAINT_PRIMARYKEY / SQLITE_CONSTRAINT_UNIQUE
            1555 | 2067 => Outcome::PkOrUnique,
            _ => Outcome::OtherErr,
        },
        Err(_) => Outcome::OtherErr,
    }
}

/// (frankensqlite, rusqlite) pair with `parent`/`child` schema and FK on.
async fn setup_pair(child_ddl: &str) -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    f.execute("PRAGMA foreign_keys=ON;").await.unwrap();
    r.execute("PRAGMA foreign_keys=ON;", []).unwrap();
    let parent_ddl = "CREATE TABLE parent (id INTEGER PRIMARY KEY);";
    f.execute(parent_ddl).await.unwrap();
    r.execute(parent_ddl, []).unwrap();
    f.execute(child_ddl).await.unwrap();
    r.execute(child_ddl, []).unwrap();
    (f, r)
}

async fn franken_table_dump(conn: &Connection, sql: &str) -> Vec<Vec<SqliteValue>> {
    conn.query(sql)
        .await
        .unwrap()
        .iter()
        .map(|row| row.values().to_vec())
        .collect()
}

fn rusqlite_table_dump(
    conn: &rusqlite::Connection,
    sql: &str,
    ncols: usize,
) -> Vec<Vec<SqliteValue>> {
    let mut stmt = conn.prepare(sql).unwrap();
    stmt.query_map([], |row| {
        let mut v = Vec::with_capacity(ncols);
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
            v.push(val);
        }
        Ok(v)
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

/// Run a sequence of (sql, expected-row-changed-or-error) inserts on both
/// engines and assert the normalized outcomes match step-by-step, then assert
/// the final table contents match. `select_sql` and `ncols` describe the dump.
async fn assert_parity(
    child_ddl: &str,
    seed: &[&str],
    cases: &[&str],
    select_sql: &str,
    ncols: usize,
    label: &str,
) {
    let (f, r) = setup_pair(child_ddl).await;
    // Seed (must succeed on both).
    f.execute("BEGIN;").await.unwrap();
    r.execute("BEGIN;", []).unwrap();
    for s in seed {
        f.execute(s).await.unwrap();
        r.execute(s, []).unwrap();
    }
    for (i, sql) in cases.iter().enumerate() {
        let fo = classify_franken(f.execute(sql).await);
        let ro = classify_rusqlite(r.execute(sql, []));
        assert_eq!(
            fo, ro,
            "[{label}] case #{i} `{sql}`: frankensqlite={fo:?} vs rusqlite={ro:?}"
        );
    }
    // After potentially-erroring statements, both engines remain in the txn
    // (immediate FK / PK errors do not auto-rollback the whole txn in SQLite;
    // they abort only the offending statement). Compare table contents.
    let fd = franken_table_dump(&f, select_sql).await;
    let rd = rusqlite_table_dump(&r, select_sql, ncols);
    assert_eq!(
        fd, rd,
        "[{label}] final table contents diverged: frankensqlite={fd:?} vs rusqlite={rd:?}"
    );
    f.execute("COMMIT;").await.unwrap();
    r.execute("COMMIT;", []).unwrap();
}

const CHILD_FK: &str =
    "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id));";
const SELECT_CHILD: &str = "SELECT id, parent_id FROM child ORDER BY id;";

#[test]
fn fk_explicit_rowid_plain_valid_and_bad() {
    asupersync::test_utils::run_test(|| async {
        assert_parity(
            CHILD_FK,
            &[
                "INSERT INTO parent VALUES (1);",
                "INSERT INTO parent VALUES (2);",
            ],
            &[
                "INSERT INTO child VALUES (10, 1);",  // valid -> Ok(1)
                "INSERT INTO child VALUES (11, 99);", // bad FK -> FkViolation, row not persisted
                "INSERT INTO child VALUES (12, 2);",  // valid -> Ok(1)
            ],
            SELECT_CHILD,
            2,
            "explicit_plain",
        )
        .await;
    });
}

#[test]
fn fk_explicit_rowid_dup_pk_plain_pk_wins_over_fk() {
    // Plain INSERT with a dup PK AND a bad FK must raise PK/UNIQUE (PK first),
    // never FK.
    asupersync::test_utils::run_test(|| async {
        assert_parity(
            CHILD_FK,
            &[
                "INSERT INTO parent VALUES (1);",
                "INSERT INTO child VALUES (5, 1);",
            ],
            &["INSERT INTO child VALUES (5, 99);"], // dup PK + bad FK -> PkOrUnique
            SELECT_CHILD,
            2,
            "explicit_dup_pk_plain",
        )
        .await;
    });
}

#[test]
fn fk_or_ignore_dup_pk_drops_no_fk() {
    // OR IGNORE on a dup PK drops the row (0 changed), and must NOT raise FK
    // even though the dropped row had a bad FK value.
    asupersync::test_utils::run_test(|| async {
        assert_parity(
            CHILD_FK,
            &[
                "INSERT INTO parent VALUES (1);",
                "INSERT INTO child VALUES (5, 1);",
            ],
            &["INSERT OR IGNORE INTO child VALUES (5, 99);"], // dup PK -> Ok(0), ignored
            SELECT_CHILD,
            2,
            "or_ignore_dup_pk",
        )
        .await;
    });
}

#[test]
fn fk_or_ignore_new_pk_bad_fk_still_raises() {
    // OR IGNORE does NOT suppress an immediate FK violation when there is no
    // PK conflict.
    asupersync::test_utils::run_test(|| async {
        assert_parity(
            CHILD_FK,
            &["INSERT INTO parent VALUES (1);"],
            &["INSERT OR IGNORE INTO child VALUES (6, 99);"], // new PK + bad FK -> FkViolation
            SELECT_CHILD,
            2,
            "or_ignore_new_pk_bad_fk",
        )
        .await;
    });
}

#[test]
fn fk_or_replace_dup_pk_bad_fk_raises_fk() {
    // OR REPLACE replaces the conflicting row, but the replacement row's FK is
    // still enforced -> FK violation.
    asupersync::test_utils::run_test(|| async {
        assert_parity(
            CHILD_FK,
            &[
                "INSERT INTO parent VALUES (1);",
                "INSERT INTO child VALUES (5, 1);",
            ],
            &["INSERT OR REPLACE INTO child VALUES (5, 99);"], // dup PK + bad FK -> FkViolation
            SELECT_CHILD,
            2,
            "or_replace_dup_pk_bad_fk",
        )
        .await;
    });
}

#[test]
fn fk_or_replace_dup_pk_valid_fk_replaces() {
    asupersync::test_utils::run_test(|| async {
        assert_parity(
            CHILD_FK,
            &[
                "INSERT INTO parent VALUES (1);",
                "INSERT INTO parent VALUES (2);",
                "INSERT INTO child VALUES (5, 1);",
            ],
            &["INSERT OR REPLACE INTO child VALUES (5, 2);"], // dup PK + valid FK -> Ok(1), replaced
            SELECT_CHILD,
            2,
            "or_replace_dup_pk_valid",
        )
        .await;
    });
}

#[test]
fn fk_or_abort_fail_dup_pk_pk_wins() {
    asupersync::test_utils::run_test(|| async {
        for clause in ["OR ABORT", "OR FAIL"] {
            let case = format!("INSERT {clause} INTO child VALUES (5, 99);");
            assert_parity(
                CHILD_FK,
                &[
                    "INSERT INTO parent VALUES (1);",
                    "INSERT INTO child VALUES (5, 1);",
                ],
                &[&case],
                SELECT_CHILD,
                2,
                "or_abort_fail_dup_pk",
            )
            .await;
        }
    });
}

#[test]
fn fk_implicit_rowid_regression() {
    asupersync::test_utils::run_test(|| async {
        // Implicit-rowid child (id is NOT the IPK alias used for the FK). FK is on
        // parent_id. Auto-assigned rowid never conflicts. Guards the pre-existing
        // implicit-rowid behavior.
        let child = "CREATE TABLE child (parent_id INTEGER REFERENCES parent(id), note TEXT);";
        let select = "SELECT parent_id, note FROM child ORDER BY rowid;";
        assert_parity(
            child,
            &["INSERT INTO parent VALUES (1);"],
            &[
                "INSERT INTO child (parent_id, note) VALUES (1, 'a');", // Ok(1)
                "INSERT INTO child (parent_id, note) VALUES (99, 'b');", // bad FK -> FkViolation
                "INSERT OR IGNORE INTO child (parent_id, note) VALUES (99, 'c');", // OR IGNORE new row bad FK -> FkViolation
            ],
            select,
            2,
            "implicit_rowid",
        )
        .await;
    });
}

#[test]
fn fk_self_reference_explicit_rowid() {
    asupersync::test_utils::run_test(|| async {
        // Self-referential FK: child.parent_id REFERENCES child.id. Post-insert FK
        // must see the just-inserted (and earlier in-flight) rows.
        let child =
            "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES child(id));";
        let (f, r) = setup_pair(child).await; // parent table unused but harmless
        f.execute("BEGIN;").await.unwrap();
        r.execute("BEGIN;", []).unwrap();
        let cases: &[&str] = &[
            "INSERT INTO child VALUES (1, NULL);", // root, Ok(1)
            "INSERT INTO child VALUES (2, 1);",    // references in-flight id=1 -> Ok(1)
            "INSERT INTO child VALUES (3, 3);",    // references itself -> Ok(1)
            "INSERT INTO child VALUES (4, 99);",   // references missing id -> FkViolation
        ];
        for (i, sql) in cases.iter().enumerate() {
            let fo = classify_franken(f.execute(sql).await);
            let ro = classify_rusqlite(r.execute(sql, []));
            assert_eq!(fo, ro, "[self_ref] case #{i} `{sql}`: {fo:?} vs {ro:?}");
        }
        let fd = franken_table_dump(&f, "SELECT id, parent_id FROM child ORDER BY id;").await;
        let rd = rusqlite_table_dump(&r, "SELECT id, parent_id FROM child ORDER BY id;", 2);
        assert_eq!(
            fd, rd,
            "[self_ref] final contents diverged: {fd:?} vs {rd:?}"
        );
    });
}

#[test]
fn fk_null_fk_column_satisfied() {
    asupersync::test_utils::run_test(|| async {
        assert_parity(
            CHILD_FK,
            &["INSERT INTO parent VALUES (1);"],
            &["INSERT INTO child VALUES (7, NULL);"], // NULL FK col -> satisfied -> Ok(1)
            SELECT_CHILD,
            2,
            "null_fk",
        )
        .await;
    });
}

#[test]
fn fk_multi_fk_one_bad() {
    asupersync::test_utils::run_test(|| async {
        let child = "CREATE TABLE child (id INTEGER PRIMARY KEY, p1 INTEGER REFERENCES parent(id), p2 INTEGER REFERENCES parent(id));";
        let select = "SELECT id, p1, p2 FROM child ORDER BY id;";
        assert_parity(
            child,
            &[
                "INSERT INTO parent VALUES (1);",
                "INSERT INTO parent VALUES (2);",
            ],
            &[
                "INSERT INTO child VALUES (10, 1, 2);",  // both valid -> Ok(1)
                "INSERT INTO child VALUES (11, 1, 99);", // one bad -> FkViolation
            ],
            select,
            3,
            "multi_fk",
        )
        .await;
    });
}

#[test]
fn fk_autocommit_single_inserts() {
    asupersync::test_utils::run_test(|| async {
        // Same matrix but in autocommit (no explicit BEGIN). Each statement is its
        // own transaction; a failed FK insert rolls back its own statement.
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        f.execute("PRAGMA foreign_keys=ON;").await.unwrap();
        r.execute("PRAGMA foreign_keys=ON;", []).unwrap();
        for ddl in [
            "CREATE TABLE parent (id INTEGER PRIMARY KEY);",
            CHILD_FK,
            "INSERT INTO parent VALUES (1);",
        ] {
            f.execute(ddl).await.unwrap();
            r.execute(ddl, []).unwrap();
        }
        for sql in [
            "INSERT INTO child VALUES (1, 1);",            // Ok
            "INSERT INTO child VALUES (2, 99);",           // bad FK
            "INSERT OR IGNORE INTO child VALUES (1, 99);", // dup PK -> ignored
            "INSERT INTO child VALUES (3, 1);",            // Ok
        ] {
            let fo = classify_franken(f.execute(sql).await);
            let ro = classify_rusqlite(r.execute(sql, []));
            assert_eq!(fo, ro, "[autocommit] `{sql}`: {fo:?} vs {ro:?}");
        }
        let fd = franken_table_dump(&f, SELECT_CHILD).await;
        let rd = rusqlite_table_dump(&r, SELECT_CHILD, 2);
        assert_eq!(fd, rd, "[autocommit] final contents: {fd:?} vs {rd:?}");
    });
}

/// bd-d8s37: FK checks belong to the row the selected UPSERT arm wrote.
/// Every failing statement must roll back its earlier rows and trigger effects.
#[test]
fn fk_upsert_written_row_oracle_matrix() {
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            for prepared in [false, true] {
                for explicit in [false, true] {
                    let dir = tempfile::tempdir().unwrap();
                    let path = dir.path().join("upsert_fk.db");
                    let f = Connection::open(if file_backed {
                        path.to_str().unwrap()
                    } else {
                        ":memory:"
                    })
                    .await
                    .unwrap();
                    let r = rusqlite::Connection::open_in_memory().unwrap();
                    let schema = "
                        PRAGMA foreign_keys=ON;
                        CREATE TABLE parent(id INTEGER PRIMARY KEY);
                        CREATE TABLE child(
                            id INTEGER PRIMARY KEY,
                            tag TEXT UNIQUE,
                            parent_id INTEGER REFERENCES parent(id),
                            other_id INTEGER REFERENCES parent(id),
                            payload INTEGER
                        );
                        CREATE TABLE audit(id INTEGER);
                        CREATE TRIGGER child_before BEFORE INSERT ON child BEGIN
                            INSERT INTO audit VALUES(NEW.id);
                        END;
                        INSERT INTO parent VALUES(1),(2);
                        INSERT INTO child VALUES(10,'a',1,2,100);";
                    f.execute(schema).await.unwrap();
                    r.execute_batch(schema).unwrap();
                    if explicit {
                        f.execute("BEGIN; INSERT INTO audit VALUES(999);")
                            .await
                            .unwrap();
                        r.execute_batch("BEGIN; INSERT INTO audit VALUES(999);")
                            .unwrap();
                    }
                    for sql in [
                        "INSERT INTO child VALUES(10,'a',1,2,101)
                         ON CONFLICT(id) DO UPDATE SET parent_id=99",
                        "INSERT INTO child VALUES(10,'a',99,99,102)
                         ON CONFLICT(id) DO UPDATE SET parent_id=2,payload=excluded.payload",
                        "INSERT INTO child VALUES(10,'a',99,99,103)
                         ON CONFLICT(id) DO UPDATE SET parent_id=99 WHERE 0",
                        "INSERT INTO child VALUES(20,'a',99,99,104)
                         ON CONFLICT(tag) DO NOTHING
                         ON CONFLICT(id) DO UPDATE SET parent_id=99",
                        "INSERT INTO child VALUES(11,'b',99,2,105)
                         ON CONFLICT(id) DO UPDATE SET parent_id=1",
                        "INSERT OR FAIL INTO child VALUES(10,'a',1,2,300),(11,'b',99,2,301)
                         ON CONFLICT(id) DO UPDATE SET payload=excluded.payload",
                        "INSERT OR IGNORE INTO child VALUES(11,'b',1,2,400),(10,'a',1,2,401)
                         ON CONFLICT(id) DO UPDATE SET parent_id=99",
                    ] {
                        let actual = if prepared {
                            f.prepare(sql).await.unwrap().execute().await
                        } else {
                            f.execute(sql).await
                        };
                        let label = format!(
                            "file={file_backed}, prepared={prepared}, explicit={explicit}: {sql}"
                        );
                        assert_eq!(
                            classify_franken(actual),
                            classify_rusqlite(r.execute(sql, [])),
                            "{label}"
                        );
                        for (select, columns) in [
                            (
                                "SELECT id,tag,parent_id,other_id,payload FROM child ORDER BY id",
                                5,
                            ),
                            ("SELECT id FROM audit ORDER BY rowid", 1),
                        ] {
                            assert_eq!(
                                franken_table_dump(&f, select).await,
                                rusqlite_table_dump(&r, select, columns),
                                "{label}: {select}"
                            );
                        }
                        assert_eq!(f.in_transaction(), explicit, "{label}");
                    }
                    if explicit {
                        f.execute("COMMIT").await.unwrap();
                        r.execute_batch("COMMIT").unwrap();
                    }
                    assert!(
                        f.query("PRAGMA foreign_key_check")
                            .await
                            .unwrap()
                            .is_empty()
                    );
                    f.close().await.unwrap();
                }
            }
        }
    });
}

#[test]
fn fk_upsert_expression_partial_and_targetless_arms_use_written_values() {
    asupersync::test_utils::run_test(|| async {
        for (index, target) in [
            ("CREATE UNIQUE INDEX key_index ON child(tag)", "(tag)"),
            (
                "CREATE UNIQUE INDEX key_index ON child(lower(tag))",
                "(lower(tag))",
            ),
            (
                "CREATE UNIQUE INDEX key_index ON child(tag) WHERE active=1",
                "(tag) WHERE active=1",
            ),
            ("CREATE UNIQUE INDEX key_index ON child(tag)", ""),
        ] {
            let invalid_update = format!(
                "INSERT INTO child VALUES(20,'a',1,1)
                 ON CONFLICT {target} DO UPDATE SET parent_id=99"
            );
            let valid_update = format!(
                "INSERT INTO child VALUES(20,'a',1,99)
                 ON CONFLICT {target} DO UPDATE SET parent_id=(SELECT max(id) FROM parent)"
            );
            assert_parity(
                "CREATE TABLE child(id INTEGER PRIMARY KEY, tag TEXT, active INTEGER,
                    parent_id INTEGER REFERENCES parent(id))",
                &[
                    index,
                    "INSERT INTO parent VALUES(1),(2)",
                    "INSERT INTO child VALUES(10,'a',1,1)",
                ],
                &[&invalid_update, &valid_update],
                "SELECT id,tag,active,parent_id FROM child ORDER BY id",
                4,
                target,
            )
            .await;
        }
    });
}

#[test]
fn fk_upsert_untouched_keys_and_deferred_obligations_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let child = "CREATE TABLE child(id INTEGER PRIMARY KEY,
            parent_id INTEGER REFERENCES parent(id),
            untouched INTEGER REFERENCES parent(id))";
        let (f, r) = setup_pair(child).await;
        let seed = "INSERT INTO parent VALUES(1),(2);
            PRAGMA foreign_keys=OFF;
            INSERT INTO child VALUES(10,1,99);
            PRAGMA foreign_keys=ON;";
        f.execute(seed).await.unwrap();
        r.execute_batch(seed).unwrap();
        let sql = "INSERT INTO child VALUES(10,99,1)
            ON CONFLICT(id) DO UPDATE SET parent_id=2";
        assert_eq!(
            classify_franken(f.execute(sql).await),
            classify_rusqlite(r.execute(sql, []))
        );
        assert_eq!(
            franken_table_dump(&f, "SELECT * FROM child").await,
            rusqlite_table_dump(&r, "SELECT * FROM child", 3)
        );
        f.close().await.unwrap();

        let (f, r) = setup_pair(
            "CREATE TABLE child(id INTEGER PRIMARY KEY,
                parent_id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)",
        )
        .await;
        let seed = "INSERT INTO parent VALUES(1); INSERT INTO child VALUES(10,1); BEGIN;";
        f.execute(seed).await.unwrap();
        r.execute_batch(seed).unwrap();
        let sql = "INSERT INTO child VALUES(10,1)
            ON CONFLICT(id) DO UPDATE SET parent_id=99";
        assert_eq!(
            classify_franken(f.execute(sql).await),
            classify_rusqlite(r.execute(sql, []))
        );
        assert!(matches!(
            f.execute("COMMIT").await,
            Err(FrankenError::ForeignKeyViolation)
        ));
        assert!(matches!(
            r.execute_batch("COMMIT"),
            Err(rusqlite::Error::SqliteFailure(code, _)) if code.extended_code == 787
        ));
        assert!(f.in_transaction());
        assert!(!r.is_autocommit());
        f.execute("INSERT INTO parent VALUES(99); COMMIT;")
            .await
            .unwrap();
        r.execute_batch("INSERT INTO parent VALUES(99); COMMIT;")
            .unwrap();
        assert_eq!(
            franken_table_dump(&f, SELECT_CHILD).await,
            rusqlite_table_dump(&r, SELECT_CHILD, 2)
        );
        assert!(
            f.query("PRAGMA foreign_key_check")
                .await
                .unwrap()
                .is_empty()
        );
        f.close().await.unwrap();

        // A rowid alias assignment also assigns its INTEGER PRIMARY KEY child key.
        assert_parity(
            "CREATE TABLE child(id INTEGER PRIMARY KEY REFERENCES parent(id), note INTEGER)",
            &[
                "INSERT INTO parent VALUES(1),(2)",
                "INSERT INTO child VALUES(1,10)",
            ],
            &[
                "INSERT INTO child VALUES(1,20)
                 ON CONFLICT(id) DO UPDATE SET rowid=99",
                "INSERT INTO child VALUES(1,20)
                 ON CONFLICT(id) DO UPDATE SET _rowid_=2",
            ],
            "SELECT id,note FROM child ORDER BY id",
            2,
            "rowid-alias-child-fk",
        )
        .await;
    });
}

struct FkUpsertNextParent(std::sync::Arc<std::sync::atomic::AtomicUsize>);

impl fsqlite_func::ScalarFunction for FkUpsertNextParent {
    fn invoke(&self, _args: &[SqliteValue]) -> fsqlite_error::Result<SqliteValue> {
        let call = self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(SqliteValue::Integer(if call.is_multiple_of(2) {
            2
        } else {
            99
        }))
    }

    fn is_deterministic(&self) -> bool {
        false
    }

    fn num_args(&self) -> i32 {
        0
    }

    fn name(&self) -> &str {
        "upsert_next_parent"
    }
}

#[test]
fn fk_upsert_prepared_returning_keeps_parameters_and_evaluates_set_once() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup_pair(CHILD_FK).await;
        let seed = "INSERT INTO parent VALUES(1),(2); INSERT INTO child VALUES(10,1);";
        f.execute(seed).await.unwrap();
        r.execute_batch(seed).unwrap();
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        f.register_nondeterministic_scalar_function(FkUpsertNextParent(std::sync::Arc::clone(&calls)));
        let sql = "INSERT INTO child VALUES(10,?1)
            ON CONFLICT(id) DO UPDATE SET parent_id=upsert_next_parent()
            RETURNING ?4,id,parent_id,?";
        let retained = f.prepare(sql).await.unwrap();
        let params = [
            SqliteValue::Integer(99),
            SqliteValue::Null,
            SqliteValue::Null,
            SqliteValue::Integer(700),
            SqliteValue::Integer(800),
        ];
        for execution in 0..3 {
            let actual = retained.query_with_params(&params).await;
            let parent = if execution == 1 { 99 } else { 2 };
            let stock_sql = format!(
                "INSERT INTO child VALUES(10,99)
                 ON CONFLICT(id) DO UPDATE SET parent_id={parent}"
            );
            let expected = r.execute(&stock_sql, []);
            if execution == 1 {
                assert!(matches!(actual, Err(FrankenError::ForeignKeyViolation)));
                assert_eq!(classify_rusqlite(expected), Outcome::FkViolation);
            } else {
                expected.unwrap();
                let rows = actual.unwrap();
                assert_eq!(rows.len(), 1);
                assert_eq!(
                    rows[0].values(),
                    &[
                        SqliteValue::Integer(700),
                        SqliteValue::Integer(10),
                        SqliteValue::Integer(2),
                        SqliteValue::Integer(800),
                    ]
                );
            }
            assert_eq!(
                calls.load(std::sync::atomic::Ordering::Relaxed),
                execution + 1,
                "FK validation must never re-evaluate the volatile SET expression"
            );
            assert_eq!(
                franken_table_dump(&f, SELECT_CHILD).await,
                rusqlite_table_dump(&r, SELECT_CHILD, 2)
            );
        }
        drop(retained);
        // Internal snapshot ResultRows must not escape a query without RETURNING.
        assert!(
            f.query(
                "INSERT INTO child VALUES(10,99)
                 ON CONFLICT(id) DO UPDATE SET parent_id=1"
            )
            .await
            .unwrap()
            .is_empty()
        );
        // A projected RETURNING already owns the complete row image, including
        // its hidden-rowid column. FK checking must preserve that layout.
        let projected = "INSERT INTO child VALUES(10,99)
            ON CONFLICT(id) DO UPDATE SET parent_id=1
            RETURNING id,(WITH c(v) AS (VALUES(99)) SELECT v FROM c),parent_id";
        assert_eq!(
            franken_table_dump(&f, projected).await,
            rusqlite_table_dump(&r, projected, 3)
        );
        f.close().await.unwrap();
    });
}
