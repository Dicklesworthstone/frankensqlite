//! Regression for issue #76: with parameterized queries, combining
//! `id IN (SELECT issue_id FROM other WHERE col = ?)` with another
//! `col IN (?)` predicate via AND returns empty under fsqlite, even
//! though both predicates individually match rows.
//!
//! - Inlined-string variants of the same SQL work (no params).
//! - The bug only triggers when both `IN (?)` literals are bound
//!   parameters via `query_with_params`.
//!
//! Repro originally surfaced in beads_rust commit a0b45bd.

use fsqlite::{Connection, SqliteValue};

fn setup() -> Connection {
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE issues (id TEXT PRIMARY KEY, issue_type TEXT NOT NULL, status TEXT NOT NULL);").unwrap();
    conn.execute(
        "CREATE TABLE labels (
            issue_id TEXT NOT NULL,
            label TEXT NOT NULL,
            PRIMARY KEY (issue_id, label)
         );",
    )
    .unwrap();
    conn.execute("INSERT INTO issues VALUES ('a', 'task', 'open');").unwrap();
    conn.execute("INSERT INTO issues VALUES ('b', 'feature', 'open');").unwrap();
    conn.execute("INSERT INTO labels VALUES ('a', 'core');").unwrap();
    conn.execute("INSERT INTO labels VALUES ('b', 'core');").unwrap();
    conn
}

fn ids(rows: Vec<fsqlite::Row>) -> Vec<String> {
    rows.into_iter()
        .map(|r| r.values()[0].as_text().unwrap().to_string())
        .collect()
}

#[test]
fn inlined_in_subquery_alone_returns_matching_rows() {
    let conn = setup();
    let rows = conn
        .query("SELECT id FROM issues WHERE id IN (SELECT issue_id FROM labels WHERE label = 'core') ORDER BY id;")
        .unwrap();
    assert_eq!(ids(rows), vec!["a", "b"]);
}

#[test]
fn inlined_in_value_alone_returns_matching_row() {
    let conn = setup();
    let rows = conn
        .query("SELECT id FROM issues WHERE issue_type IN ('task');")
        .unwrap();
    assert_eq!(ids(rows), vec!["a"]);
}

#[test]
fn inlined_in_subquery_combined_with_in_value_returns_intersection() {
    let conn = setup();
    let rows = conn
        .query("SELECT id FROM issues WHERE id IN (SELECT issue_id FROM labels WHERE label = 'core') AND issue_type IN ('task');")
        .unwrap();
    assert_eq!(ids(rows), vec!["a"]);
}

/// THIS IS THE BUG (issue #76): the parameterized form of the combined
/// predicate returns empty. Same SQL shape, same data — just bound
/// parameters instead of inlined string literals.
#[test]
fn parameterized_in_subquery_combined_with_in_value_returns_intersection() {
    let conn = setup();
    let rows = conn
        .query_with_params(
            "SELECT id FROM issues WHERE id IN (SELECT issue_id FROM labels WHERE label = ?) AND issue_type IN (?);",
            &[SqliteValue::from("core"), SqliteValue::from("task")],
        )
        .unwrap();
    assert_eq!(
        ids(rows),
        vec!["a"],
        "BUG: parameterized IN-subquery + IN(?) returns empty; expected the intersection"
    );
}

#[test]
fn parameterized_in_subquery_combined_with_in_value_swapped_order() {
    let conn = setup();
    let rows = conn
        .query_with_params(
            "SELECT id FROM issues WHERE issue_type IN (?) AND id IN (SELECT issue_id FROM labels WHERE label = ?);",
            &[SqliteValue::from("task"), SqliteValue::from("core")],
        )
        .unwrap();
    assert_eq!(ids(rows), vec!["a"]);
}

#[test]
fn parameterized_exists_subquery_combined_with_in_value_works() {
    // EXISTS form is the workaround that already worked.
    let conn = setup();
    let rows = conn
        .query_with_params(
            "SELECT id FROM issues WHERE EXISTS (SELECT 1 FROM labels WHERE labels.issue_id = issues.id AND labels.label = ?) AND issue_type IN (?);",
            &[SqliteValue::from("core"), SqliteValue::from("task")],
        )
        .unwrap();
    assert_eq!(ids(rows), vec!["a"]);
}
