//! bd-wwqen.4: Proof tests for SimpleIndexedEqualityLookup query_row fast path.
//!
//! Tests use `query_row_with_params` (prepared statement) where the fast path
//! fires, and `query_with_params` (Connection) for multi-row correctness.
//! Counter assertions prove the direct MemDB path is taken.
//!
//! Run:
//!   cargo test -p fsqlite-core --test b4_query_row_indexed_equality \
//!     -- --test-threads=1 --nocapture

use fsqlite_core::connection::{
    Connection, hot_path_profile_enabled, hot_path_profile_snapshot, reset_hot_path_profile,
    set_hot_path_profile_enabled,
};
use fsqlite_types::SqliteValue;
use std::sync::{Mutex, MutexGuard};

static B4_PROFILE_LOCK: Mutex<()> = Mutex::new(());

struct B4ProfileGuard {
    _lock: MutexGuard<'static, ()>,
    previous_enabled: bool,
}

impl B4ProfileGuard {
    fn new() -> Self {
        let lock = B4_PROFILE_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let previous_enabled = hot_path_profile_enabled();
        set_hot_path_profile_enabled(true);
        reset_hot_path_profile();
        Self {
            _lock: lock,
            previous_enabled,
        }
    }
}

impl Drop for B4ProfileGuard {
    fn drop(&mut self) {
        reset_hot_path_profile();
        set_hot_path_profile_enabled(self.previous_enabled);
    }
}

async fn setup_indexed_table(conn: &Connection) {
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, category TEXT)")
        .await
        .unwrap();
    conn.execute("CREATE INDEX idx_items_category ON items(category)")
        .await
        .unwrap();
    conn.execute("INSERT INTO items VALUES (1, 'apple', 'fruit')")
        .await
        .unwrap();
    conn.execute("INSERT INTO items VALUES (2, 'banana', 'fruit')")
        .await
        .unwrap();
    conn.execute("INSERT INTO items VALUES (3, 'carrot', 'vegetable')")
        .await
        .unwrap();
    conn.execute("INSERT INTO items VALUES (4, 'date', 'fruit')")
        .await
        .unwrap();
    conn.execute("INSERT INTO items VALUES (5, 'eggplant', 'vegetable')")
        .await
        .unwrap();
}

/// B4.1: Multi-row indexed equality returns correct rows (correctness only,
/// query_with_params does not use query_row fast path).
#[test]
fn test_query_row_indexed_equality_basic() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        setup_indexed_table(&conn).await;

        let rows = conn
            .query_with_params(
                "SELECT id, name FROM items WHERE category = ?1",
                &[SqliteValue::Text("fruit".into())],
            )
            .await
            .unwrap();

        assert_eq!(rows.len(), 3, "should find 3 fruit rows");
        let ids: Vec<i64> = rows
            .iter()
            .filter_map(|r| r.values().first().and_then(|v| v.as_integer()))
            .collect();
        assert!(ids.contains(&1), "apple should be found");
        assert!(ids.contains(&2), "banana should be found");
        assert!(ids.contains(&4), "date should be found");
    });
}

/// B4.2: No-match via prepared query_row fires fast path, returns error.
#[test]
fn test_query_row_indexed_equality_no_match() {
    asupersync::test_utils::run_test(|| async {
        let _guard = B4ProfileGuard::new();
        let conn = Connection::open(":memory:").await.unwrap();
        setup_indexed_table(&conn).await;

        let stmt = conn
            .prepare("SELECT * FROM items WHERE category = ?1")
            .await
            .unwrap();
        // Warm.
        drop(
            stmt.query_row_with_params(&[SqliteValue::Text("grain".into())])
                .await,
        );
        reset_hot_path_profile();

        let before = hot_path_profile_snapshot();
        let result = stmt
            .query_row_with_params(&[SqliteValue::Text("grain".into())])
            .await;
        let after = hot_path_profile_snapshot();

        assert!(result.is_err(), "no-match query_row should return error");

        let hits_delta = after
            .direct_indexed_equality_query_hits
            .saturating_sub(before.direct_indexed_equality_query_hits);
        eprintln!("[B4.2] no-match: hits_delta={hits_delta}");
        assert!(
            hits_delta >= 1,
            "fast path should fire for no-match: {hits_delta}"
        );
    });
}

/// B4.3: NULL parameter via prepared query_row fires fast path, returns error.
#[test]
fn test_query_row_indexed_equality_null_param() {
    asupersync::test_utils::run_test(|| async {
        let _guard = B4ProfileGuard::new();
        let conn = Connection::open(":memory:").await.unwrap();
        setup_indexed_table(&conn).await;

        let stmt = conn
            .prepare("SELECT * FROM items WHERE category = ?1")
            .await
            .unwrap();
        drop(stmt.query_row_with_params(&[SqliteValue::Null]).await);
        reset_hot_path_profile();

        let before = hot_path_profile_snapshot();
        let result = stmt.query_row_with_params(&[SqliteValue::Null]).await;
        let after = hot_path_profile_snapshot();

        assert!(result.is_err(), "NULL param query_row should return error");

        let hits_delta = after
            .direct_indexed_equality_query_hits
            .saturating_sub(before.direct_indexed_equality_query_hits);
        eprintln!("[B4.3] null-param: hits_delta={hits_delta}");
        assert!(
            hits_delta >= 1,
            "fast path should fire for NULL: {hits_delta}"
        );
    });
}

/// B4.4: Read-after-write correctness.
#[test]
fn test_query_row_indexed_equality_read_after_write() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        setup_indexed_table(&conn).await;
        conn.execute("INSERT INTO items VALUES (6, 'fig', 'fruit')")
            .await
            .unwrap();

        let rows = conn
            .query_with_params(
                "SELECT id, name FROM items WHERE category = ?1",
                &[SqliteValue::Text("fruit".into())],
            )
            .await
            .unwrap();

        assert_eq!(rows.len(), 4, "should find 4 fruit rows after insert");
        let ids: Vec<i64> = rows
            .iter()
            .filter_map(|r| r.values().first().and_then(|v| v.as_integer()))
            .collect();
        assert!(ids.contains(&6), "fig should be found");
    });
}

/// B4.5: Prepared query_row reuse with different params fires fast path each time.
#[test]
fn test_query_row_indexed_equality_prepared_reuse() {
    asupersync::test_utils::run_test(|| async {
        let _guard = B4ProfileGuard::new();
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, name TEXT)")
            .await
            .unwrap();
        conn.execute("CREATE UNIQUE INDEX idx_email ON users(email)")
            .await
            .unwrap();
        conn.execute("INSERT INTO users VALUES (1, 'a@b.com', 'Alice')")
            .await
            .unwrap();
        conn.execute("INSERT INTO users VALUES (2, 'c@d.com', 'Bob')")
            .await
            .unwrap();
        conn.execute("INSERT INTO users VALUES (3, 'e@f.com', 'Carol')")
            .await
            .unwrap();

        let stmt = conn
            .prepare("SELECT * FROM users WHERE email = ?1")
            .await
            .unwrap();
        // Warm.
        drop(
            stmt.query_row_with_params(&[SqliteValue::Text("a@b.com".into())])
                .await,
        );
        reset_hot_path_profile();

        let before = hot_path_profile_snapshot();
        let r1 = stmt
            .query_row_with_params(&[SqliteValue::Text("a@b.com".into())])
            .await
            .unwrap();
        assert_eq!(r1.get(2), Some(&SqliteValue::Text("Alice".into())));

        let r2 = stmt
            .query_row_with_params(&[SqliteValue::Text("c@d.com".into())])
            .await
            .unwrap();
        assert_eq!(r2.get(2), Some(&SqliteValue::Text("Bob".into())));

        let r3 = stmt
            .query_row_with_params(&[SqliteValue::Text("e@f.com".into())])
            .await
            .unwrap();
        assert_eq!(r3.get(2), Some(&SqliteValue::Text("Carol".into())));
        let after = hot_path_profile_snapshot();

        let hits_delta = after
            .direct_indexed_equality_query_hits
            .saturating_sub(before.direct_indexed_equality_query_hits);
        eprintln!("[B4.5] prepared reuse: hits_delta={hits_delta}");
        assert!(
            hits_delta >= 3,
            "fast path should fire for all 3 lookups: {hits_delta}"
        );
    });
}

/// B4.6: Unique index query_row returns exactly one row and fires fast path.
#[test]
fn test_query_row_indexed_equality_unique_index() {
    asupersync::test_utils::run_test(|| async {
        let _guard = B4ProfileGuard::new();
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT)")
            .await
            .unwrap();
        conn.execute("CREATE UNIQUE INDEX idx_users_email ON users(email)")
            .await
            .unwrap();
        conn.execute("INSERT INTO users VALUES (1, 'a@b.com', 'Alice')")
            .await
            .unwrap();
        conn.execute("INSERT INTO users VALUES (2, 'c@d.com', 'Bob')")
            .await
            .unwrap();

        let stmt = conn
            .prepare("SELECT * FROM users WHERE email = ?1")
            .await
            .unwrap();
        drop(
            stmt.query_row_with_params(&[SqliteValue::Text("a@b.com".into())])
                .await,
        );
        reset_hot_path_profile();

        let before = hot_path_profile_snapshot();
        let row = stmt
            .query_row_with_params(&[SqliteValue::Text("a@b.com".into())])
            .await
            .unwrap();
        let after = hot_path_profile_snapshot();

        assert_eq!(row.get(2), Some(&SqliteValue::Text("Alice".into())));

        let hits_delta = after
            .direct_indexed_equality_query_hits
            .saturating_sub(before.direct_indexed_equality_query_hits);
        eprintln!("[B4.6] unique: hits_delta={hits_delta}");
        assert!(hits_delta >= 1, "fast path should fire: {hits_delta}");
    });
}

/// B4.7: Integer column multi-row correctness (no counter assertion).
#[test]
fn test_query_row_indexed_equality_integer_column() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute(
            "CREATE TABLE scores (id INTEGER PRIMARY KEY, player_id INTEGER, score INTEGER)",
        )
        .await
        .unwrap();
        conn.execute("CREATE INDEX idx_scores_player ON scores(player_id)")
            .await
            .unwrap();
        conn.execute("INSERT INTO scores VALUES (1, 10, 100)")
            .await
            .unwrap();
        conn.execute("INSERT INTO scores VALUES (2, 20, 200)")
            .await
            .unwrap();
        conn.execute("INSERT INTO scores VALUES (3, 10, 150)")
            .await
            .unwrap();

        let rows = conn
            .query_with_params(
                "SELECT id, score FROM scores WHERE player_id = ?1",
                &[SqliteValue::Integer(10)],
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 2, "player 10 should have 2 scores");
    });
}
