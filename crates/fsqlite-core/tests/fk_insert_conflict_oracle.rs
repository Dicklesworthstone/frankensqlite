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
        Err(FrankenError::PrimaryKeyViolation | FrankenError::UniqueViolation { .. }) => {
            Outcome::PkOrUnique
        }
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
fn setup_pair(child_ddl: &str) -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    f.execute("PRAGMA foreign_keys=ON;").unwrap();
    r.execute("PRAGMA foreign_keys=ON;", []).unwrap();
    let parent_ddl = "CREATE TABLE parent (id INTEGER PRIMARY KEY);";
    f.execute(parent_ddl).unwrap();
    r.execute(parent_ddl, []).unwrap();
    f.execute(child_ddl).unwrap();
    r.execute(child_ddl, []).unwrap();
    (f, r)
}

fn franken_table_dump(conn: &Connection, sql: &str) -> Vec<Vec<SqliteValue>> {
    conn.query(sql)
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
fn assert_parity(
    child_ddl: &str,
    seed: &[&str],
    cases: &[&str],
    select_sql: &str,
    ncols: usize,
    label: &str,
) {
    let (f, r) = setup_pair(child_ddl);
    // Seed (must succeed on both).
    f.execute("BEGIN;").unwrap();
    r.execute("BEGIN;", []).unwrap();
    for s in seed {
        f.execute(s).unwrap();
        r.execute(s, []).unwrap();
    }
    for (i, sql) in cases.iter().enumerate() {
        let fo = classify_franken(f.execute(sql));
        let ro = classify_rusqlite(r.execute(sql, []));
        assert_eq!(
            fo, ro,
            "[{label}] case #{i} `{sql}`: frankensqlite={fo:?} vs rusqlite={ro:?}"
        );
    }
    // After potentially-erroring statements, both engines remain in the txn
    // (immediate FK / PK errors do not auto-rollback the whole txn in SQLite;
    // they abort only the offending statement). Compare table contents.
    let fd = franken_table_dump(&f, select_sql);
    let rd = rusqlite_table_dump(&r, select_sql, ncols);
    assert_eq!(
        fd, rd,
        "[{label}] final table contents diverged: frankensqlite={fd:?} vs rusqlite={rd:?}"
    );
    f.execute("COMMIT;").unwrap();
    r.execute("COMMIT;", []).unwrap();
}

const CHILD_FK: &str =
    "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id));";
const SELECT_CHILD: &str = "SELECT id, parent_id FROM child ORDER BY id;";

#[test]
fn fk_explicit_rowid_plain_valid_and_bad() {
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
    );
}

#[test]
fn fk_explicit_rowid_dup_pk_plain_pk_wins_over_fk() {
    // Plain INSERT with a dup PK AND a bad FK must raise PK/UNIQUE (PK first),
    // never FK.
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
    );
}

#[test]
fn fk_or_ignore_dup_pk_drops_no_fk() {
    // OR IGNORE on a dup PK drops the row (0 changed), and must NOT raise FK
    // even though the dropped row had a bad FK value.
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
    );
}

#[test]
fn fk_or_ignore_new_pk_bad_fk_still_raises() {
    // OR IGNORE does NOT suppress an immediate FK violation when there is no
    // PK conflict.
    assert_parity(
        CHILD_FK,
        &["INSERT INTO parent VALUES (1);"],
        &["INSERT OR IGNORE INTO child VALUES (6, 99);"], // new PK + bad FK -> FkViolation
        SELECT_CHILD,
        2,
        "or_ignore_new_pk_bad_fk",
    );
}

#[test]
fn fk_or_replace_dup_pk_bad_fk_raises_fk() {
    // OR REPLACE replaces the conflicting row, but the replacement row's FK is
    // still enforced -> FK violation.
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
    );
}

#[test]
fn fk_or_replace_dup_pk_valid_fk_replaces() {
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
    );
}

#[test]
fn fk_or_abort_fail_dup_pk_pk_wins() {
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
        );
    }
}

#[test]
fn fk_implicit_rowid_regression() {
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
    );
}

#[test]
fn fk_self_reference_explicit_rowid() {
    // Self-referential FK: child.parent_id REFERENCES child.id. Post-insert FK
    // must see the just-inserted (and earlier in-flight) rows.
    let child =
        "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES child(id));";
    let (f, r) = setup_pair(child); // parent table unused but harmless
    f.execute("BEGIN;").unwrap();
    r.execute("BEGIN;", []).unwrap();
    let cases: &[&str] = &[
        "INSERT INTO child VALUES (1, NULL);", // root, Ok(1)
        "INSERT INTO child VALUES (2, 1);",    // references in-flight id=1 -> Ok(1)
        "INSERT INTO child VALUES (3, 3);",    // references itself -> Ok(1)
        "INSERT INTO child VALUES (4, 99);",   // references missing id -> FkViolation
    ];
    for (i, sql) in cases.iter().enumerate() {
        let fo = classify_franken(f.execute(sql));
        let ro = classify_rusqlite(r.execute(sql, []));
        assert_eq!(fo, ro, "[self_ref] case #{i} `{sql}`: {fo:?} vs {ro:?}");
    }
    let fd = franken_table_dump(&f, "SELECT id, parent_id FROM child ORDER BY id;");
    let rd = rusqlite_table_dump(&r, "SELECT id, parent_id FROM child ORDER BY id;", 2);
    assert_eq!(
        fd, rd,
        "[self_ref] final contents diverged: {fd:?} vs {rd:?}"
    );
}

#[test]
fn fk_null_fk_column_satisfied() {
    assert_parity(
        CHILD_FK,
        &["INSERT INTO parent VALUES (1);"],
        &["INSERT INTO child VALUES (7, NULL);"], // NULL FK col -> satisfied -> Ok(1)
        SELECT_CHILD,
        2,
        "null_fk",
    );
}

#[test]
fn fk_multi_fk_one_bad() {
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
    );
}

#[test]
fn fk_autocommit_single_inserts() {
    // Same matrix but in autocommit (no explicit BEGIN). Each statement is its
    // own transaction; a failed FK insert rolls back its own statement.
    let f = Connection::open(":memory:").unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    f.execute("PRAGMA foreign_keys=ON;").unwrap();
    r.execute("PRAGMA foreign_keys=ON;", []).unwrap();
    for ddl in [
        "CREATE TABLE parent (id INTEGER PRIMARY KEY);",
        CHILD_FK,
        "INSERT INTO parent VALUES (1);",
    ] {
        f.execute(ddl).unwrap();
        r.execute(ddl, []).unwrap();
    }
    for sql in [
        "INSERT INTO child VALUES (1, 1);",            // Ok
        "INSERT INTO child VALUES (2, 99);",           // bad FK
        "INSERT OR IGNORE INTO child VALUES (1, 99);", // dup PK -> ignored
        "INSERT INTO child VALUES (3, 1);",            // Ok
    ] {
        let fo = classify_franken(f.execute(sql));
        let ro = classify_rusqlite(r.execute(sql, []));
        assert_eq!(fo, ro, "[autocommit] `{sql}`: {fo:?} vs {ro:?}");
    }
    let fd = franken_table_dump(&f, SELECT_CHILD);
    let rd = rusqlite_table_dump(&r, SELECT_CHILD, 2);
    assert_eq!(fd, rd, "[autocommit] final contents: {fd:?} vs {rd:?}");
}
