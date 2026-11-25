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
    rusqlite_table_dump_with_params(conn, sql, ncols, [])
}

fn rusqlite_table_dump_with_params(
    conn: &rusqlite::Connection,
    sql: &str,
    ncols: usize,
    params: impl rusqlite::Params,
) -> Vec<Vec<SqliteValue>> {
    let mut stmt = conn.prepare(sql).unwrap();
    stmt.query_map(params, |row| {
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

#[test]
fn fk_upsert_snapshot_preserves_mixed_parameter_identity() {
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            for prepared in [false, true] {
                let directory = tempfile::tempdir().unwrap();
                let path = directory.path().join("upsert_parameters.db");
                let f = Connection::open(if file_backed {
                    path.to_str().unwrap()
                } else {
                    ":memory:"
                })
                .await
                .unwrap();
                let r = rusqlite::Connection::open_in_memory().unwrap();
                let setup = "PRAGMA foreign_keys=ON;
                    CREATE TABLE parent(id INTEGER PRIMARY KEY);
                    INSERT INTO parent VALUES(1),(2),(3);
                    CREATE TABLE child(id INTEGER PRIMARY KEY,
                        parent_id INTEGER REFERENCES parent(id));";
                f.execute(setup).await.unwrap();
                r.execute_batch(setup).unwrap();
                // Numbered slots leave a gap, anonymous slots follow the
                // largest preceding number, and a repeated name reuses its
                // first slot. FK snapshots must preserve that namespace on
                // both INSERT and DO UPDATE, including retained preparation.
                let sql = "INSERT INTO child VALUES(?2,?1)
                    ON CONFLICT(id) DO UPDATE SET parent_id=?3
                    RETURNING ?5,id,parent_id,?,:label,:label,?";
                let retained = if prepared {
                    Some(f.prepare(sql).await.unwrap())
                } else {
                    None
                };
                for (round, (id, candidate, updated)) in
                    [(10, 1, 2), (10, 99, 3), (20, 2, 1), (20, 99, 2)]
                        .into_iter()
                        .enumerate()
                {
                    let marker = i64::try_from(round).unwrap() * 100;
                    let label = format!("round-{round}");
                    let native_params = [
                        SqliteValue::Integer(candidate),
                        SqliteValue::Integer(id),
                        SqliteValue::Integer(updated),
                        SqliteValue::Null,
                        SqliteValue::Integer(marker + 5),
                        SqliteValue::Integer(marker + 6),
                        SqliteValue::Text(label.clone().into()),
                        SqliteValue::Integer(marker + 8),
                    ];
                    let stock_params = [
                        rusqlite::types::Value::Integer(candidate),
                        rusqlite::types::Value::Integer(id),
                        rusqlite::types::Value::Integer(updated),
                        rusqlite::types::Value::Null,
                        rusqlite::types::Value::Integer(marker + 5),
                        rusqlite::types::Value::Integer(marker + 6),
                        rusqlite::types::Value::Text(label),
                        rusqlite::types::Value::Integer(marker + 8),
                    ];
                    let actual = if let Some(retained) = &retained {
                        retained.query_with_params(&native_params).await
                    } else {
                        f.query_with_params(sql, &native_params).await
                    }
                    .unwrap();
                    let expected = rusqlite_table_dump_with_params(
                        &r,
                        sql,
                        7,
                        rusqlite::params_from_iter(stock_params),
                    );
                    assert_eq!(
                        actual.iter().map(|row| row.values().to_vec()).collect::<Vec<_>>(),
                        expected,
                        "file={file_backed}, prepared={prepared}, round={round}"
                    );
                    assert_eq!(
                        franken_table_dump(&f, SELECT_CHILD).await,
                        rusqlite_table_dump(&r, SELECT_CHILD, 2),
                        "file={file_backed}, prepared={prepared}, round={round}"
                    );
                }
                drop(retained);
                f.close().await.unwrap();
            }
        }
    });
}

/// Live A/B benchmark for bd-5ofes (GH#496): multi-row `INSERT ... VALUES`
/// under `PRAGMA foreign_keys = ON` vs `OFF` and vs live bundled `rusqlite`.
#[test]
#[ignore = "live release-perf A/B benchmark for bd-5ofes (GH#496)"]
#[allow(clippy::too_many_lines)]
fn bd_5ofes_multirow_insert_fk_ab_live() {
    use std::time::Instant;

    fn build_literal_events_sql(table: &str, run_id_mod: i64, start_seq: i64, rows: usize) -> String {
        let mut sql = format!("INSERT INTO {table} VALUES ");
        for i in 0..rows {
            if i > 0 {
                sql.push_str(", ");
            }
            let seq = start_seq + i64::try_from(i).unwrap();
            let run_id = (seq % run_id_mod) + 1;
            let _ = std::fmt::Write::write_fmt(
                &mut sql,
                format_args!("({run_id}, {seq}, 'payload_{seq}')"),
            );
        }
        sql
    }

    fn build_literal_text_fk_sql(start_seq: i64, rows: usize) -> String {
        let mut sql = String::from("INSERT INTO child_u VALUES ");
        for i in 0..rows {
            if i > 0 {
                sql.push_str(", ");
            }
            let seq = start_seq + i64::try_from(i).unwrap();
            let _ = std::fmt::Write::write_fmt(
                &mut sql,
                format_args!("({seq}, 'run-1', 'payload_{seq}')"),
            );
        }
        sql
    }

    fn build_prepared_events_sql(table: &str, rows: usize) -> String {
        let mut sql = format!("INSERT INTO {table} VALUES ");
        for i in 0..rows {
            if i > 0 {
                sql.push_str(", ");
            }
            let base = i * 3 + 1;
            let _ = std::fmt::Write::write_fmt(
                &mut sql,
                format_args!("(?{base}, ?{}, ?{})", base + 1, base + 2),
            );
        }
        sql
    }

    async fn setup_f(path: &str, fk_on: bool) -> Connection {
        let conn = Connection::open(path).await.unwrap();
        if path != ":memory:" {
            let _ = conn.query("PRAGMA journal_mode=WAL;").await.unwrap();
            conn.execute("PRAGMA synchronous=NORMAL;").await.unwrap();
        }
        conn.execute(if fk_on {
            "PRAGMA foreign_keys = ON;"
        } else {
            "PRAGMA foreign_keys = OFF;"
        })
        .await
        .unwrap();
        for ddl in [
            "CREATE TABLE runs (run_id INTEGER PRIMARY KEY);",
            "INSERT INTO runs VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10);",
            "CREATE TABLE events (run_id INTEGER NOT NULL, seq INTEGER NOT NULL, v TEXT, \
             PRIMARY KEY (run_id, seq), FOREIGN KEY (run_id) REFERENCES runs(run_id));",
            "CREATE INDEX events_v ON events (v);",
            "CREATE TABLE plain (run_id INTEGER NOT NULL, seq INTEGER NOT NULL, v TEXT, \
             PRIMARY KEY (run_id, seq));",
            "CREATE INDEX plain_v ON plain (v);",
            "CREATE TABLE parent_u (k TEXT UNIQUE NOT NULL);",
            "INSERT INTO parent_u VALUES ('run-1');",
            "CREATE TABLE child_u (id INTEGER PRIMARY KEY, pk TEXT REFERENCES parent_u(k), v TEXT);",
        ] {
            conn.execute(ddl).await.unwrap();
        }
        conn
    }

    fn setup_r(path: &str, fk_on: bool) -> rusqlite::Connection {
        let conn = if path == ":memory:" {
            rusqlite::Connection::open_in_memory().unwrap()
        } else {
            rusqlite::Connection::open(path).unwrap()
        };
        if path != ":memory:" {
            let _: String = conn
                .query_row("PRAGMA journal_mode=WAL;", [], |r| r.get(0))
                .unwrap();
            conn.execute_batch("PRAGMA synchronous=NORMAL;").unwrap();
        }
        conn.execute_batch(if fk_on {
            "PRAGMA foreign_keys = ON;"
        } else {
            "PRAGMA foreign_keys = OFF;"
        })
        .unwrap();
        conn.execute_batch(
            "CREATE TABLE runs (run_id INTEGER PRIMARY KEY);
             INSERT INTO runs VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10);
             CREATE TABLE events (run_id INTEGER NOT NULL, seq INTEGER NOT NULL, v TEXT,
               PRIMARY KEY (run_id, seq), FOREIGN KEY (run_id) REFERENCES runs(run_id));
             CREATE INDEX events_v ON events (v);
             CREATE TABLE plain (run_id INTEGER NOT NULL, seq INTEGER NOT NULL, v TEXT,
               PRIMARY KEY (run_id, seq));
             CREATE INDEX plain_v ON plain (v);
             CREATE TABLE parent_u (k TEXT UNIQUE NOT NULL);
             INSERT INTO parent_u VALUES ('run-1');
             CREATE TABLE child_u (id INTEGER PRIMARY KEY, pk TEXT REFERENCES parent_u(k), v TEXT);",
        )
        .unwrap();
        conn
    }

    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        println!("SQLite version: {}", rusqlite::version());
        println!(
            "{:<38} {:>10} {:>10} {:>10} {:>9} {:>9}",
            "case", "f_off_us/r", "f_on_us/r", "sql_on_us", "on/off", "f_on/sql"
        );

        #[derive(Clone, Copy)]
        struct AbCase {
            label: &'static str,
            table: &'static str,
            rows_per_stmt: usize,
            batches: usize,
            run_id_mod: i64,
            explicit_txn: bool,
            file_backed: bool,
            prepared: bool,
        }

        let cases: &[AbCase] = &[
            AbCase { label: "mem_plain_20r_txn_lit", table: "plain", rows_per_stmt: 20, batches: 40, run_id_mod: 1, explicit_txn: true, file_backed: false, prepared: false },
            AbCase { label: "mem_plain_100r_txn_lit", table: "plain", rows_per_stmt: 100, batches: 30, run_id_mod: 1, explicit_txn: true, file_backed: false, prepared: false },
            AbCase { label: "mem_plain_200r_txn_lit", table: "plain", rows_per_stmt: 200, batches: 20, run_id_mod: 1, explicit_txn: true, file_backed: false, prepared: false },
            AbCase { label: "mem_fk_20r_1p_txn_lit", table: "events", rows_per_stmt: 20, batches: 40, run_id_mod: 1, explicit_txn: true, file_backed: false, prepared: false },
            AbCase { label: "mem_fk_100r_1p_txn_lit", table: "events", rows_per_stmt: 100, batches: 30, run_id_mod: 1, explicit_txn: true, file_backed: false, prepared: false },
            AbCase { label: "mem_fk_200r_1p_txn_lit", table: "events", rows_per_stmt: 200, batches: 20, run_id_mod: 1, explicit_txn: true, file_backed: false, prepared: false },
            AbCase { label: "mem_fk_100r_10p_txn_lit", table: "events", rows_per_stmt: 100, batches: 30, run_id_mod: 10, explicit_txn: true, file_backed: false, prepared: false },
            AbCase { label: "mem_fk_100r_1p_auto_lit", table: "events", rows_per_stmt: 100, batches: 20, run_id_mod: 1, explicit_txn: false, file_backed: false, prepared: false },
            AbCase { label: "file_fk_100r_1p_txn_lit", table: "events", rows_per_stmt: 100, batches: 20, run_id_mod: 1, explicit_txn: true, file_backed: true, prepared: false },
            AbCase { label: "file_fk_100r_1p_auto_lit", table: "events", rows_per_stmt: 100, batches: 15, run_id_mod: 1, explicit_txn: false, file_backed: true, prepared: false },
            AbCase { label: "mem_fk_100r_1p_txn_prep", table: "events", rows_per_stmt: 100, batches: 30, run_id_mod: 1, explicit_txn: true, file_backed: false, prepared: true },
            AbCase { label: "mem_fk_100r_1p_auto_prep", table: "events", rows_per_stmt: 100, batches: 20, run_id_mod: 1, explicit_txn: false, file_backed: false, prepared: true },
            AbCase { label: "mem_textfk_100r_1p_txn_lit", table: "child_u", rows_per_stmt: 100, batches: 20, run_id_mod: 1, explicit_txn: true, file_backed: false, prepared: false },
            AbCase { label: "mem_textfk_100r_1p_auto_lit", table: "child_u", rows_per_stmt: 100, batches: 15, run_id_mod: 1, explicit_txn: false, file_backed: false, prepared: false },
        ];

        for &AbCase {
            label,
            table,
            rows_per_stmt,
            batches,
            run_id_mod,
            explicit_txn,
            file_backed,
            prepared,
        } in cases
        {
            let total_rows = rows_per_stmt * batches;
            let sqls: Vec<String> = if prepared {
                Vec::new()
            } else {
                (0..batches)
                    .map(|b| {
                        let start = i64::try_from(b * rows_per_stmt).unwrap();
                        if table == "child_u" {
                            build_literal_text_fk_sql(start, rows_per_stmt)
                        } else {
                            build_literal_events_sql(table, run_id_mod, start, rows_per_stmt)
                        }
                    })
                    .collect()
            };
            let prep_sql = prepared.then(|| build_prepared_events_sql(table, rows_per_stmt));

            let mut times_us_per_row = [0.0f64; 3]; // 0=f_off, 1=f_on, 2=sql_on
            for (slot, fk_on) in [(0usize, false), (1usize, true)] {
                let path = if file_backed {
                    dir.path()
                        .join(format!("{label}_f_{slot}.db"))
                        .to_string_lossy()
                        .into_owned()
                } else {
                    ":memory:".to_owned()
                };
                let conn = setup_f(&path, fk_on).await;
                if explicit_txn {
                    conn.execute("BEGIN;").await.unwrap();
                }
                let start = Instant::now();
                if let Some(ref ps) = prep_sql {
                    let stmt = conn.prepare(ps).await.unwrap();
                    for b in 0..batches {
                        let mut params = Vec::with_capacity(rows_per_stmt * 3);
                        for i in 0..rows_per_stmt {
                            let seq = i64::try_from(b * rows_per_stmt + i).unwrap();
                            let run_id = (seq % run_id_mod) + 1;
                            params.push(SqliteValue::Integer(run_id));
                            params.push(SqliteValue::Integer(seq));
                            params.push(SqliteValue::Text(format!("payload_{seq}").into()));
                        }
                        let n = stmt.execute_with_params(&params).await.unwrap();
                        assert_eq!(n, rows_per_stmt);
                    }
                } else {
                    for sql in &sqls {
                        let n = conn.execute(sql).await.unwrap();
                        assert_eq!(n, rows_per_stmt);
                    }
                }
                if explicit_txn {
                    conn.execute("COMMIT;").await.unwrap();
                }
                let elapsed = start.elapsed();
                let count = conn
                    .query(&format!("SELECT count(*) FROM {table};"))
                    .await
                    .unwrap();
                assert_eq!(
                    count[0].values()[0],
                    SqliteValue::Integer(i64::try_from(total_rows).unwrap())
                );
                conn.close().await.unwrap();
                times_us_per_row[slot] = elapsed.as_secs_f64() * 1e6 / (total_rows as f64);
            }

            // Live bundled rusqlite with foreign_keys = ON
            {
                let path = if file_backed {
                    dir.path()
                        .join(format!("{label}_r.db"))
                        .to_string_lossy()
                        .into_owned()
                } else {
                    ":memory:".to_owned()
                };
                let r = setup_r(&path, true);
                if explicit_txn {
                    r.execute_batch("BEGIN;").unwrap();
                }
                let start = Instant::now();
                if let Some(ref ps) = prep_sql {
                    let mut stmt = r.prepare(ps).unwrap();
                    for b in 0..batches {
                        let mut params = Vec::with_capacity(rows_per_stmt * 3);
                        for i in 0..rows_per_stmt {
                            let seq = i64::try_from(b * rows_per_stmt + i).unwrap();
                            let run_id = (seq % run_id_mod) + 1;
                            params.push(rusqlite::types::Value::Integer(run_id));
                            params.push(rusqlite::types::Value::Integer(seq));
                            params.push(rusqlite::types::Value::Text(format!("payload_{seq}")));
                        }
                        let n = stmt.execute(rusqlite::params_from_iter(params)).unwrap();
                        assert_eq!(n, rows_per_stmt);
                    }
                } else {
                    for sql in &sqls {
                        let n = r.execute(sql, []).unwrap();
                        assert_eq!(n, rows_per_stmt);
                    }
                }
                if explicit_txn {
                    r.execute_batch("COMMIT;").unwrap();
                }
                let elapsed = start.elapsed();
                let count: i64 = r
                    .query_row(&format!("SELECT count(*) FROM {table};"), [], |row| {
                        row.get(0)
                    })
                    .unwrap();
                assert_eq!(count, i64::try_from(total_rows).unwrap());
                times_us_per_row[2] = elapsed.as_secs_f64() * 1e6 / (total_rows as f64);
            }

            println!(
                "{:<38} {:>10.2} {:>10.2} {:>10.2} {:>8.2}x {:>8.2}x",
                label,
                times_us_per_row[0],
                times_us_per_row[1],
                times_us_per_row[2],
                times_us_per_row[1] / times_us_per_row[0],
                times_us_per_row[1] / times_us_per_row[2],
            );
        }
    });
}

/// bd-5ofes (GH#496): comprehensive differential oracle against live bundled
/// `rusqlite` covering multi-row `INSERT VALUES` (below and above
/// `MORSEL_INSERT_THRESHOLD = 64`), direct & prepared lanes, `:memory:` &
/// file-backed, explicit txn & autocommit, composite/NULL/DEFAULT/affinity/
/// WITHOUT ROWID/self-referential/deferred FKs, PK-beats-FK across morsels,
/// and all negative controls (OR IGNORE/REPLACE/FAIL/ROLLBACK, schema
/// ON CONFLICT IGNORE, IPK-FK, generated FK, volatile VALUES, triggers,
/// and foreign_keys toggled ON after prepare()).
#[test]
#[allow(clippy::too_many_lines)]
fn bd_5ofes_multirow_fk_batched_and_morsel_oracle_matrix() {
    fn build_child_values(rows: impl IntoIterator<Item = (i64, i64)>) -> String {
        let vals = rows
            .into_iter()
            .map(|(id, pid)| format!("({id}, {pid})"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("INSERT INTO child VALUES {vals};")
    }

    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            for explicit_txn in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let f_path = if file_backed {
                    dir.path().join("bd_5ofes_f.db").to_string_lossy().into_owned()
                } else {
                    ":memory:".to_owned()
                };
                let r_path = if file_backed {
                    dir.path().join("bd_5ofes_r.db").to_string_lossy().into_owned()
                } else {
                    ":memory:".to_owned()
                };
                let f = Connection::open(&f_path).await.unwrap();
                let r = if file_backed {
                    rusqlite::Connection::open(&r_path).unwrap()
                } else {
                    rusqlite::Connection::open_in_memory().unwrap()
                };

                let setup = "
                    PRAGMA foreign_keys = ON;
                    CREATE TABLE parent (id INTEGER PRIMARY KEY);
                    INSERT INTO parent VALUES (1), (2), (3), (10);
                    CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id));
                    CREATE TABLE plain (id INTEGER PRIMARY KEY, v TEXT);
                    CREATE TABLE comp_p (a INTEGER, b TEXT, PRIMARY KEY (a, b));
                    INSERT INTO comp_p VALUES (1, 'x'), (2, 'y');
                    CREATE TABLE comp_c (id INTEGER PRIMARY KEY, a INTEGER, b TEXT,
                        FOREIGN KEY (a, b) REFERENCES comp_p(a, b));
                    CREATE TABLE def_c (id INTEGER PRIMARY KEY,
                        pid INTEGER DEFAULT 1 REFERENCES parent(id),
                        bad_pid INTEGER DEFAULT 99 REFERENCES parent(id));
                    CREATE TABLE self_ref (id INTEGER PRIMARY KEY,
                        parent_id INTEGER REFERENCES self_ref(id));
                    CREATE TABLE blob_p (k BLOB PRIMARY KEY);
                    INSERT INTO blob_p VALUES ('42');
                    CREATE TABLE text_aff_c (id INTEGER PRIMARY KEY,
                        pk TEXT REFERENCES blob_p(k));
                    CREATE TABLE wr_p (k TEXT PRIMARY KEY) WITHOUT ROWID;
                    INSERT INTO wr_p VALUES ('alpha'), ('beta');
                    CREATE TABLE wr_c (id INTEGER PRIMARY KEY,
                        pk TEXT REFERENCES wr_p(k)) WITHOUT ROWID;
                    CREATE TABLE ipk_fk (id INTEGER PRIMARY KEY REFERENCES parent(id), note TEXT);
                    CREATE TABLE gen_fk (id INTEGER PRIMARY KEY, base INTEGER,
                        pid INTEGER GENERATED ALWAYS AS (base + 1) STORED REFERENCES parent(id));
                    CREATE TABLE ign_col_fk (id INTEGER PRIMARY KEY,
                        tag TEXT UNIQUE ON CONFLICT IGNORE,
                        pid INTEGER REFERENCES parent(id));
                ";
                f.execute(setup).await.unwrap();
                r.execute_batch(setup).unwrap();

                if explicit_txn {
                    f.execute("BEGIN;").await.unwrap();
                    r.execute_batch("BEGIN;").unwrap();
                    f.execute("INSERT INTO child VALUES (500, 1);").await.unwrap();
                    r.execute("INSERT INTO child VALUES (500, 1);", []).unwrap();
                }

                // 1. Plain non-FK table: 20 rows and 100 rows under foreign_keys=ON.
                let plain_20 = format!(
                    "INSERT INTO plain VALUES {};",
                    (0..20)
                        .map(|i| format!("({i}, 'p{i}')"))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                let plain_100 = format!(
                    "INSERT INTO plain VALUES {};",
                    (100..200)
                        .map(|i| format!("({i}, 'p{i}')"))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                // 2. Outbound FK: 20 rows (below morsel), 64 rows (threshold), 100 rows (morsel),
                //    constant parent and mixed parents.
                let child_20 = build_child_values((1..=20).map(|i| (i, 1)));
                let child_64 = build_child_values(
                    (101..=164).map(|i| (i, if i % 2 == 0 { 1 } else { 2 })),
                );
                // 3. Missing parent on row 99 of 100 -> FkViolation, all 100 rolled back.
                let child_100_bad_last =
                    build_child_values((1000..1100).map(|i| (i, if i == 1099 { 999 } else { 1 })));
                // 4. PK-beats-FK across morsel boundaries: row 0 has bad FK (999),
                //    row 80 collides with existing id=120 -> PkOrUnique, NOT FkViolation!
                let child_100_pk_beats_fk = build_child_values((2000..2100).map(|i| {
                    if i == 2000 {
                        (i, 999)
                    } else if i == 2080 {
                        (120, 1)
                    } else {
                        (i, 1)
                    }
                }));
                // 5. Composite FK (valid + NULL partial + bad pair) across 70 rows.
                let comp_ok = format!(
                    "INSERT INTO comp_c VALUES {};",
                    (0..70)
                        .map(|i| match i % 3 {
                            0 => format!("({i}, 1, 'x')"),
                            1 => format!("({i}, 2, 'y')"),
                            _ => format!("({i}, 999, NULL)"),
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                let comp_bad = format!(
                    "INSERT INTO comp_c VALUES {};",
                    (100..170)
                        .map(|i| if i == 165 {
                            format!("({i}, 1, 'y')")
                        } else {
                            format!("({i}, 1, 'x')")
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                // 6. Omitted FK column with constant DEFAULT (valid vs bad).
                let def_ok = format!(
                    "INSERT INTO def_c (id, bad_pid) VALUES {};",
                    (0..70)
                        .map(|i| format!("({i}, 1)"))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                let def_bad = format!(
                    "INSERT INTO def_c (id) VALUES {};",
                    (100..170)
                        .map(|i| format!("({i})"))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                // 7. Self-referential FK across morsel boundaries (forward refs!).
                let self_ok = format!(
                    "INSERT INTO self_ref VALUES {};",
                    (1..=100)
                        .rev()
                        .map(|i| if i == 1 {
                            "(1, NULL)".to_owned()
                        } else {
                            format!("({i}, {})", i - 1)
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                let self_bad = format!(
                    "INSERT INTO self_ref VALUES {};",
                    (200..270)
                        .map(|i| if i == 200 {
                            "(200, 9999)".to_owned()
                        } else {
                            format!("({i}, {})", i - 1)
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                // 8. Child affinity coercion: INTEGER 42 -> TEXT '42' matching BLOB parent '42',
                //    plus child.pid INTEGER receiving TEXT '10' matching parent.id 10.
                let aff_ok = format!(
                    "INSERT INTO text_aff_c VALUES {};",
                    (0..70)
                        .map(|i| format!("({i}, 42)"))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                let aff_int_from_text = format!(
                    "INSERT INTO child VALUES {};",
                    (3000..3070)
                        .map(|i| format!("({i}, '10')"))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                // 9. WITHOUT ROWID child & parent across 70 rows (valid + bad).
                let wr_ok = format!(
                    "INSERT INTO wr_c VALUES {};",
                    (0..70)
                        .map(|i| format!("({i}, '{}')", if i % 2 == 0 { "alpha" } else { "beta" }))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                let wr_bad = format!(
                    "INSERT INTO wr_c VALUES {};",
                    (100..170)
                        .map(|i| format!("({i}, '{}')", if i == 150 { "gamma" } else { "alpha" }))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                // 10. Negative controls: OR IGNORE, OR REPLACE, OR FAIL, IPK-FK, generated FK,
                //     column-level UNIQUE ON CONFLICT IGNORE.
                let or_ignore_skip_bad =
                    "INSERT OR IGNORE INTO child VALUES (1, 999), (4000, 1), (2, 888), (4001, 2);";
                let or_ignore_new_bad =
                    "INSERT OR IGNORE INTO child VALUES (1, 999), (4010, 999);";
                let or_replace_valid =
                    "INSERT OR REPLACE INTO child VALUES (1, 2), (2, 1);";
                let or_replace_bad =
                    "INSERT OR REPLACE INTO child VALUES (1, 1), (4020, 999);";
                let or_fail_pk_after_good =
                    "INSERT OR FAIL INTO child VALUES (4030, 1), (1, 1), (4031, 1);";
                let or_fail_fk_aborts_all =
                    "INSERT OR FAIL INTO child VALUES (4040, 1), (4041, 999);";
                let ipk_fk_auto = "INSERT INTO ipk_fk (note) VALUES ('a'), ('b'), ('c');";
                let ipk_fk_auto_fail = "INSERT INTO ipk_fk (note) VALUES ('d');"; // rowid 4 missing!
                let gen_fk_ok = "INSERT INTO gen_fk (id, base) VALUES (1, 0), (2, 1), (3, 2);";
                let ign_col_seed = "INSERT INTO ign_col_fk VALUES (1, 'dup', 1);";
                let ign_col_skip =
                    "INSERT INTO ign_col_fk VALUES (2, 'dup', 999), (3, 'fresh', 2);";

                let steps: &[(&str, &str)] = &[
                    ("plain_20", &plain_20),
                    ("plain_100", &plain_100),
                    ("child_20", &child_20),
                    ("child_64", &child_64),
                    ("child_100_bad_last", &child_100_bad_last),
                    ("child_100_pk_beats_fk", &child_100_pk_beats_fk),
                    ("comp_ok", &comp_ok),
                    ("comp_bad", &comp_bad),
                    ("def_ok", &def_ok),
                    ("def_bad", &def_bad),
                    ("self_ok", &self_ok),
                    ("self_bad", &self_bad),
                    ("aff_ok", &aff_ok),
                    ("aff_int_from_text", &aff_int_from_text),
                    ("wr_ok", &wr_ok),
                    ("wr_bad", &wr_bad),
                    ("or_ignore_skip_bad", or_ignore_skip_bad),
                    ("or_ignore_new_bad", or_ignore_new_bad),
                    ("or_replace_valid", or_replace_valid),
                    ("or_replace_bad", or_replace_bad),
                    ("or_fail_pk_after_good", or_fail_pk_after_good),
                    ("or_fail_fk_aborts_all", or_fail_fk_aborts_all),
                    ("ipk_fk_auto", ipk_fk_auto),
                    ("ipk_fk_auto_fail", ipk_fk_auto_fail),
                    ("gen_fk_ok", gen_fk_ok),
                    ("ign_col_seed", ign_col_seed),
                    ("ign_col_skip", ign_col_skip),
                ];

                for &(step_name, sql) in steps {
                    let fo = classify_franken(f.execute(sql).await);
                    let ro = classify_rusqlite(r.execute(sql, []));
                    assert_eq!(
                        fo, ro,
                        "[file={file_backed} txn={explicit_txn} step={step_name}] outcome diverged: f={fo:?} vs r={ro:?}"
                    );
                    assert_eq!(
                        f.in_transaction(),
                        !r.is_autocommit(),
                        "[file={file_backed} txn={explicit_txn} step={step_name}] txn state diverged"
                    );
                }

                // 11. Prepared multi-row OR IGNORE negative control (must skip dup PK without
                //     raising false FK on ignored row!) + prepared multi-row plain INSERT (70 rows).
                {
                    let prep_ign = f
                        .prepare("INSERT OR IGNORE INTO child VALUES (?1, ?2), (?3, ?4);")
                        .await
                        .unwrap();
                    let fo = classify_franken(
                        prep_ign
                            .execute_with_params(&[
                                SqliteValue::Integer(1),
                                SqliteValue::Integer(999),
                                SqliteValue::Integer(4100),
                                SqliteValue::Integer(2),
                            ])
                            .await,
                    );
                    let ro = classify_rusqlite(r.execute(
                        "INSERT OR IGNORE INTO child VALUES (?1, ?2), (?3, ?4);",
                        rusqlite::params![1i64, 999i64, 4100i64, 2i64],
                    ));
                    assert_eq!(
                        fo, ro,
                        "[file={file_backed} txn={explicit_txn}] prepared multi-row OR IGNORE diverged"
                    );

                    let mut prep_70_sql = String::from("INSERT INTO child VALUES ");
                    let mut f_params = Vec::with_capacity(140);
                    let mut r_params = Vec::with_capacity(140);
                    for i in 0..70i64 {
                        if i > 0 {
                            prep_70_sql.push_str(", ");
                        }
                        let base = i * 2 + 1;
                        let _ = std::fmt::Write::write_fmt(
                            &mut prep_70_sql,
                            format_args!("(?{base}, ?{})", base + 1),
                        );
                        f_params.push(SqliteValue::Integer(5000 + i));
                        f_params.push(SqliteValue::Integer(1));
                        r_params.push(rusqlite::types::Value::Integer(5000 + i));
                        r_params.push(rusqlite::types::Value::Integer(1));
                    }
                    let prep_70 = f.prepare(&prep_70_sql).await.unwrap();
                    assert_eq!(
                        classify_franken(prep_70.execute_with_params(&f_params).await),
                        classify_rusqlite(
                            r.execute(&prep_70_sql, rusqlite::params_from_iter(r_params))
                        ),
                        "[file={file_backed} txn={explicit_txn}] prepared 70-row valid diverged"
                    );
                    // Now execute the SAME prepared statement with a bad FK on row 69.
                    let mut f_params_bad = Vec::with_capacity(140);
                    let mut r_params_bad = Vec::with_capacity(140);
                    for i in 0..70i64 {
                        let pid = if i == 69 { 999 } else { 1 };
                        f_params_bad.push(SqliteValue::Integer(6000 + i));
                        f_params_bad.push(SqliteValue::Integer(pid));
                        r_params_bad.push(rusqlite::types::Value::Integer(6000 + i));
                        r_params_bad.push(rusqlite::types::Value::Integer(pid));
                    }
                    assert_eq!(
                        classify_franken(prep_70.execute_with_params(&f_params_bad).await),
                        classify_rusqlite(
                            r.execute(&prep_70_sql, rusqlite::params_from_iter(r_params_bad))
                        ),
                        "[file={file_backed} txn={explicit_txn}] prepared 70-row bad diverged"
                    );
                }

                if explicit_txn {
                    f.execute("COMMIT;").await.unwrap();
                    r.execute_batch("COMMIT;").unwrap();
                }

                for (select, ncols) in [
                    ("SELECT id, v FROM plain ORDER BY id;", 2),
                    ("SELECT id, pid FROM child ORDER BY id;", 2),
                    ("SELECT id, a, b FROM comp_c ORDER BY id;", 3),
                    ("SELECT id, pid, bad_pid FROM def_c ORDER BY id;", 3),
                    ("SELECT id, parent_id FROM self_ref ORDER BY id;", 2),
                    ("SELECT id, pk FROM text_aff_c ORDER BY id;", 2),
                    ("SELECT id, pk FROM wr_c ORDER BY id;", 2),
                    ("SELECT id, note FROM ipk_fk ORDER BY id;", 2),
                    ("SELECT id, base, pid FROM gen_fk ORDER BY id;", 3),
                    ("SELECT id, tag, pid FROM ign_col_fk ORDER BY id;", 3),
                ] {
                    assert_eq!(
                        franken_table_dump(&f, select).await,
                        rusqlite_table_dump(&r, select, ncols),
                        "[file={file_backed} txn={explicit_txn}] table dump `{select}` diverged"
                    );
                }
                assert!(
                    f.query("PRAGMA foreign_key_check;")
                        .await
                        .unwrap()
                        .is_empty(),
                    "[file={file_backed} txn={explicit_txn}] foreign_key_check not empty"
                );
                f.close().await.unwrap();
            }
        }
    });
}

/// bd-5ofes: deferred multi-row FK multiplicity, volatile VALUES single-eval,
/// `PRAGMA foreign_keys = ON` toggled AFTER `prepare()`, and multi-statement
/// `morsel_insert_dispatch` FK parity.
#[test]
#[allow(clippy::too_many_lines)]
fn bd_5ofes_deferred_multiplicity_volatile_and_toggle_after_prepare() {
    asupersync::test_utils::run_test(|| async {
        // 1. Deferred FK multiplicity across 80 rows in an explicit transaction:
        //    inserting 80 rows referencing missing parent 99, then deleting only 1
        //    of them, must STILL fail COMMIT until parent 99 exists.
        let (f, r) = setup_pair(
            "CREATE TABLE child (id INTEGER PRIMARY KEY, \
             pid INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED);",
        )
        .await;
        f.execute("INSERT INTO parent VALUES (1); BEGIN;").await.unwrap();
        r.execute_batch("INSERT INTO parent VALUES (1); BEGIN;").unwrap();
        let deferred_80 = format!(
            "INSERT INTO child VALUES {};",
            (0..80)
                .map(|i| format!("({i}, 99)"))
                .collect::<Vec<_>>()
                .join(", ")
        );
        assert_eq!(
            classify_franken(f.execute(&deferred_80).await),
            classify_rusqlite(r.execute(&deferred_80, []))
        );
        f.execute("DELETE FROM child WHERE id = 0;").await.unwrap();
        r.execute("DELETE FROM child WHERE id = 0;", []).unwrap();
        assert_eq!(
            classify_franken(f.execute("COMMIT;").await),
            classify_rusqlite(r.execute("COMMIT;", []))
        );
        assert!(f.in_transaction());
        assert!(!r.is_autocommit());
        f.execute("INSERT INTO parent VALUES (99); COMMIT;").await.unwrap();
        r.execute_batch("INSERT INTO parent VALUES (99); COMMIT;").unwrap();
        assert_eq!(
            franken_table_dump(&f, "SELECT count(*) FROM child;").await,
            rusqlite_table_dump(&r, "SELECT count(*) FROM child;", 1)
        );
        // Autocommit deferred 80-row INSERT with missing parent fails at statement end.
        let deferred_80_auto = format!(
            "INSERT INTO child VALUES {};",
            (100..180)
                .map(|i| format!("({i}, 888)"))
                .collect::<Vec<_>>()
                .join(", ")
        );
        assert_eq!(
            classify_franken(f.execute(&deferred_80_auto).await),
            classify_rusqlite(r.execute(&deferred_80_auto, []))
        );
        assert_eq!(
            franken_table_dump(&f, "SELECT count(*) FROM child;").await,
            rusqlite_table_dump(&r, "SELECT count(*) FROM child;", 1)
        );
        f.close().await.unwrap();

        // 2. Volatile scalar function in 20-row (< 64) multi-row INSERT VALUES
        //    under foreign_keys = ON must be invoked EXACTLY once per row.
        let f = Connection::open(":memory:").await.unwrap();
        f.execute("PRAGMA foreign_keys = ON;").await.unwrap();
        f.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY); INSERT INTO parent VALUES (1),(2);")
            .await
            .unwrap();
        f.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id));")
            .await
            .unwrap();
        struct CountingParent(std::sync::Arc<std::sync::atomic::AtomicUsize>);
        impl fsqlite_func::ScalarFunction for CountingParent {
            fn invoke(&self, _args: &[SqliteValue]) -> fsqlite_error::Result<SqliteValue> {
                let n = self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(SqliteValue::Integer(if n.is_multiple_of(2) { 1 } else { 2 }))
            }
            fn is_deterministic(&self) -> bool {
                false
            }
            fn num_args(&self) -> i32 {
                0
            }
            fn name(&self) -> &str {
                "counting_parent"
            }
        }
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        f.register_nondeterministic_scalar_function(CountingParent(std::sync::Arc::clone(&calls)));
        let vol_20 = format!(
            "INSERT INTO child VALUES {};",
            (0..20)
                .map(|i| format!("({i}, counting_parent())"))
                .collect::<Vec<_>>()
                .join(", ")
        );
        f.execute(&vol_20).await.unwrap();
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::Relaxed),
            20,
            "volatile VALUES function must execute exactly once per row"
        );
        f.close().await.unwrap();

        // 3. Statement prepared while foreign_keys = OFF, then foreign_keys toggled ON:
        //    precompiled FK plan must still reject missing parent.
        let f = Connection::open(":memory:").await.unwrap();
        f.execute("PRAGMA foreign_keys = OFF;").await.unwrap();
        f.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY); INSERT INTO parent VALUES (1);")
            .await
            .unwrap();
        f.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id));")
            .await
            .unwrap();
        let stmt = f
            .prepare("INSERT INTO child VALUES (?1, ?2), (?3, ?4);")
            .await
            .unwrap();
        f.execute("PRAGMA foreign_keys = ON;").await.unwrap();
        let err = stmt
            .execute_with_params(&[
                SqliteValue::Integer(1),
                SqliteValue::Integer(1),
                SqliteValue::Integer(2),
                SqliteValue::Integer(99),
            ])
            .await;
        assert!(matches!(err, Err(FrankenError::ForeignKeyViolation)));
        assert_eq!(
            franken_table_dump(&f, "SELECT count(*) FROM child;").await,
            vec![vec![SqliteValue::Integer(0)]]
        );
        drop(stmt);
        f.close().await.unwrap();

        // 4. Multi-statement batches bypass ad-hoc prepared reuse and exercise
        //    execute_insert_dispatch -> morsel_insert_dispatch ->
        //    BatchedInsertFkCollector directly (valid 100-row, bad-last-row
        //    100-row rollback, PK-beats-FK across morsels, self-ref 100-row,
        //    and deferred 80-row multiplicity) against live rusqlite.
        let (f, r) = setup_pair(
            "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id));",
        )
        .await;
        let extra_ddl = "
            INSERT INTO parent VALUES (1), (2);
            CREATE TABLE self_ref (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES self_ref(id));
            CREATE TABLE def_c (id INTEGER PRIMARY KEY,
                pid INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED);
            BEGIN;
            INSERT INTO child VALUES (50, 1);";
        f.execute(extra_ddl).await.unwrap();
        r.execute_batch(extra_ddl).unwrap();

        let morsel_ok = format!(
            "SELECT 1; INSERT INTO child VALUES {};",
            (100..200)
                .map(|i| format!("({i}, {})", if i % 2 == 0 { 1 } else { 2 }))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let morsel_bad_last = format!(
            "SELECT 1; INSERT INTO child VALUES {};",
            (300..400)
                .map(|i| format!("({i}, {})", if i == 399 { 999 } else { 1 }))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let morsel_pk_beats_fk = format!(
            "SELECT 1; INSERT INTO child VALUES {};",
            (500..600)
                .map(|i| if i == 500 {
                    "(500, 999)".to_owned()
                } else if i == 580 {
                    "(50, 1)".to_owned()
                } else {
                    format!("({i}, 1)")
                })
                .collect::<Vec<_>>()
                .join(", ")
        );
        let morsel_self_ok = format!(
            "SELECT 1; INSERT INTO self_ref VALUES {};",
            (1..=100)
                .rev()
                .map(|i| if i == 1 {
                    "(1, NULL)".to_owned()
                } else {
                    format!("({i}, {})", i - 1)
                })
                .collect::<Vec<_>>()
                .join(", ")
        );
        let morsel_def_80 = format!(
            "SELECT 1; INSERT INTO def_c VALUES {};",
            (0..80)
                .map(|i| format!("({i}, 99)"))
                .collect::<Vec<_>>()
                .join(", ")
        );

        for (name, sql) in [
            ("morsel_ok", &morsel_ok),
            ("morsel_bad_last", &morsel_bad_last),
            ("morsel_pk_beats_fk", &morsel_pk_beats_fk),
            ("morsel_self_ok", &morsel_self_ok),
            ("morsel_def_80", &morsel_def_80),
        ] {
            let fo = classify_franken(f.execute(sql).await);
            let ro = match r.execute_batch(sql) {
                Ok(()) => Outcome::Ok(0),
                Err(e) => classify_rusqlite(Err(e)),
            };
            match (&fo, &ro) {
                (Outcome::Ok(_), Outcome::Ok(_)) => {}
                _ => assert_eq!(fo, ro, "[{name}] morsel outcome diverged: f={fo:?} vs r={ro:?}"),
            }
            assert!(f.in_transaction(), "[{name}] explicit txn must stay open");
            assert!(!r.is_autocommit(), "[{name}] rusqlite txn must stay open");
        }
        // Delete 1 deferred row -> COMMIT must still fail until parent 99 exists.
        f.execute("DELETE FROM def_c WHERE id = 0;").await.unwrap();
        r.execute("DELETE FROM def_c WHERE id = 0;", []).unwrap();
        assert_eq!(
            classify_franken(f.execute("COMMIT;").await),
            classify_rusqlite(r.execute("COMMIT;", []))
        );
        f.execute("INSERT INTO parent VALUES (99); COMMIT;").await.unwrap();
        r.execute_batch("INSERT INTO parent VALUES (99); COMMIT;").unwrap();
        for (select, ncols) in [
            ("SELECT id, pid FROM child ORDER BY id;", 2),
            ("SELECT id, parent_id FROM self_ref ORDER BY id;", 2),
            ("SELECT count(*) FROM def_c;", 1),
        ] {
            assert_eq!(
                franken_table_dump(&f, select).await,
                rusqlite_table_dump(&r, select, ncols),
                "[morsel_batch] `{select}` diverged"
            );
        }
        f.close().await.unwrap();
    });
}
