//! Regression: a FK-enforced INSERT into a table that takes the *cold* FK path
//! (e.g. one with a composite UNIQUE constraint, so it is off the direct-simple
//! insert lane) must accept anonymous `?` and named placeholders.
//!
//! The cold FK-enforcement path re-parses the original prepared SQL (which is
//! NOT canonicalized) and evaluates each VALUES row to FK-check it. The direct
//! row evaluator requires canonical `Numbered` placeholders, so an anonymous
//! `?` / named `:x` placeholder tripped the internal invariant
//! "expected canonicalized numbered placeholder" (a generic DATABASE_ERROR to
//! callers). This was surfaced by the GH#116 direct-eval change and reported via
//! Agent Mail `request_contact` failures on a reused pooled connection.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn setup(conn: &Connection) {
    conn.execute("PRAGMA foreign_keys=ON").unwrap();
    conn.execute("CREATE TABLE parent (id TEXT PRIMARY KEY NOT NULL)")
        .unwrap();
    // Composite UNIQUE keeps this table off the direct-simple insert lane, so
    // FK enforcement takes the cold re-parse path.
    conn.execute(
        "CREATE TABLE item (
            id INTEGER PRIMARY KEY,
            parent_id TEXT NOT NULL REFERENCES parent(id),
            seq INTEGER NOT NULL,
            UNIQUE(parent_id, seq)
        )",
    )
    .unwrap();
    conn.execute("INSERT INTO parent (id) VALUES ('p1')")
        .unwrap();
}

fn count_items(conn: &Connection) -> i64 {
    match &conn.query("SELECT count(*) FROM item").unwrap()[0].values()[0] {
        SqliteValue::Integer(n) => *n,
        other => panic!("expected integer count, got {other:?}"),
    }
}

#[test]
fn fk_cold_path_insert_accepts_anonymous_placeholders() {
    let conn = Connection::open(":memory:").unwrap();
    setup(&conn);
    conn.execute_with_params(
        "INSERT INTO item (id, parent_id, seq) VALUES (?, ?, ?)",
        &[
            SqliteValue::Integer(1),
            SqliteValue::Text("p1".into()),
            SqliteValue::Integer(1),
        ],
    )
    .expect("anonymous-placeholder FK insert must not trip the canonicalization invariant");
    assert_eq!(count_items(&conn), 1);
}

#[test]
fn fk_cold_path_reused_prepared_insert_anonymous_placeholders() {
    // Mirrors the pooled-connection / many-execution scenario (request_contact
    // failed around the 10th reuse): reuse one prepared statement many times.
    let conn = Connection::open(":memory:").unwrap();
    setup(&conn);
    let stmt = conn
        .prepare("INSERT INTO item (id, parent_id, seq) VALUES (?, ?, ?)")
        .unwrap();
    for i in 1..=20 {
        conn.execute_prepared_with_params(
            &stmt,
            &[
                SqliteValue::Integer(i),
                SqliteValue::Text("p1".into()),
                SqliteValue::Integer(i),
            ],
        )
        .unwrap_or_else(|e| panic!("reused anonymous-placeholder FK insert {i} failed: {e}"));
    }
    assert_eq!(count_items(&conn), 20);
}

#[test]
fn fk_cold_path_insert_accepts_named_placeholders() {
    let conn = Connection::open(":memory:").unwrap();
    setup(&conn);
    conn.execute_with_params(
        "INSERT INTO item (id, parent_id, seq) VALUES (:id, :pid, :seq)",
        &[
            SqliteValue::Integer(1),
            SqliteValue::Text("p1".into()),
            SqliteValue::Integer(1),
        ],
    )
    .expect("named-placeholder FK insert must not trip the canonicalization invariant");
    assert_eq!(count_items(&conn), 1);
}

#[test]
fn fk_cold_path_insert_still_accepts_numbered_placeholders() {
    // The canonicalization must be idempotent for already-Numbered placeholders.
    let conn = Connection::open(":memory:").unwrap();
    setup(&conn);
    conn.execute_with_params(
        "INSERT INTO item (id, parent_id, seq) VALUES (?1, ?2, ?3)",
        &[
            SqliteValue::Integer(1),
            SqliteValue::Text("p1".into()),
            SqliteValue::Integer(1),
        ],
    )
    .expect("numbered-placeholder FK insert must still work");
    assert_eq!(count_items(&conn), 1);
}
