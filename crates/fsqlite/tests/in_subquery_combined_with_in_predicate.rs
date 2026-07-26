//! Regression test for issue #76:
//! Combining `id IN (SELECT ...)` with another `IN (?)` predicate via AND
//! returned an empty result set when both bound via parameters, even though
//! each predicate matched rows individually.
//!
//! The fix landed before this test was written; this test guards the property.

use fsqlite::{Connection, Row};
use fsqlite_types::SqliteValue;

async fn setup() -> Connection {
    let conn = Connection::open(":memory:").await.unwrap();
    conn.execute(
        "CREATE TABLE issues (
            id TEXT PRIMARY KEY,
            issue_type TEXT NOT NULL,
            status TEXT NOT NULL
        );",
    )
    .await
    .unwrap();
    conn.execute(
        "CREATE TABLE labels (
            issue_id TEXT NOT NULL,
            label TEXT NOT NULL,
            PRIMARY KEY (issue_id, label),
            FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
        );",
    )
    .await
    .unwrap();
    conn.execute("INSERT INTO issues VALUES ('a', 'task',    'open');")
        .await
        .unwrap();
    conn.execute("INSERT INTO issues VALUES ('b', 'feature', 'open');")
        .await
        .unwrap();
    conn.execute("INSERT INTO labels VALUES ('a', 'core');")
        .await
        .unwrap();
    conn.execute("INSERT INTO labels VALUES ('b', 'core');")
        .await
        .unwrap();
    conn
}

fn ids(rows: &[Row]) -> Vec<String> {
    rows.iter()
        .map(|r| match &r.values()[0] {
            SqliteValue::Text(s) => s.to_string(),
            other => panic!("expected text id, got {other:?}"),
        })
        .collect()
}

#[test]
fn in_subquery_alone_works() {
    asupersync::test_utils::run_test(|| async {
        let conn = setup().await;
        let rows = conn
            .query_with_params(
                "SELECT id FROM issues \
             WHERE id IN (SELECT issue_id FROM labels WHERE label = ?) \
             ORDER BY id",
                &[SqliteValue::Text("core".into())],
            )
            .await
            .unwrap();
        assert_eq!(ids(&rows), vec!["a".to_string(), "b".to_string()]);
    });
}

#[test]
fn in_param_alone_works() {
    asupersync::test_utils::run_test(|| async {
        let conn = setup().await;
        let rows = conn
            .query_with_params(
                "SELECT id FROM issues WHERE issue_type IN (?) ORDER BY id",
                &[SqliteValue::Text("task".into())],
            )
            .await
            .unwrap();
        assert_eq!(ids(&rows), vec!["a".to_string()]);
    });
}

#[test]
fn in_subquery_combined_with_in_param_returns_intersection() {
    asupersync::test_utils::run_test(|| async {
        let conn = setup().await;
        let rows = conn
            .query_with_params(
                "SELECT id FROM issues \
             WHERE id IN (SELECT issue_id FROM labels WHERE label = ?) \
               AND issue_type IN (?) \
             ORDER BY id",
                &[
                    SqliteValue::Text("core".into()),
                    SqliteValue::Text("task".into()),
                ],
            )
            .await
            .unwrap();
        assert_eq!(ids(&rows), vec!["a".to_string()]);
    });
}

#[test]
fn in_subquery_combined_with_in_param_order_swapped() {
    asupersync::test_utils::run_test(|| async {
        let conn = setup().await;
        let rows = conn
            .query_with_params(
                "SELECT id FROM issues \
             WHERE issue_type IN (?) \
               AND id IN (SELECT issue_id FROM labels WHERE label = ?) \
             ORDER BY id",
                &[
                    SqliteValue::Text("task".into()),
                    SqliteValue::Text("core".into()),
                ],
            )
            .await
            .unwrap();
        assert_eq!(ids(&rows), vec!["a".to_string()]);
    });
}

#[test]
fn exists_form_continues_to_work() {
    asupersync::test_utils::run_test(|| async {
        let conn = setup().await;
        let rows = conn
            .query_with_params(
                "SELECT id FROM issues \
             WHERE EXISTS ( \
                 SELECT 1 FROM labels \
                 WHERE labels.issue_id = issues.id AND labels.label = ? \
             ) \
               AND issue_type IN (?) \
             ORDER BY id",
                &[
                    SqliteValue::Text("core".into()),
                    SqliteValue::Text("task".into()),
                ],
            )
            .await
            .unwrap();
        assert_eq!(ids(&rows), vec!["a".to_string()]);
    });
}
