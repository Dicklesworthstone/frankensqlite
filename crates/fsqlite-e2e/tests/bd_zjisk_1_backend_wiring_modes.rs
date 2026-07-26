//! Deterministic backend-wiring mode checks for bd-zjisk.1.
//!
//! This suite verifies startup/query behavior across runtime and certifying
//! modes without relying on nondeterministic workload generation.
#![recursion_limit = "512"]

use fsqlite::Connection;
use fsqlite_types::SqliteValue;
use tempfile::tempdir;

const BEAD_ID: &str = "bd-zjisk.1";
const SCENARIO_ID: &str = "BACKEND-WIRING-MODES";
const SEED: u64 = 3520;

async fn setup_join_fixture(conn: &Connection) {
    conn.execute("CREATE TABLE items (id INTEGER, name TEXT);")
        .await
        .expect("create items");
    conn.execute("CREATE TABLE tags (item_id INTEGER, tag TEXT);")
        .await
        .expect("create tags");
    conn.execute("INSERT INTO items VALUES (1, 'alpha');")
        .await
        .expect("insert item");
    conn.execute("INSERT INTO tags VALUES (1, 'fruit');")
        .await
        .expect("insert tag");
}

#[test]
fn certifying_mode_strict_allows_file_backed_join_path() {
    asupersync::test_utils::run_test(|| async {
        let temp = tempdir().expect("tempdir");
        let db_path = temp.path().join("zjisk1-certifying-strict.db");
        let db_path = db_path.to_string_lossy().to_string();
        let conn = Connection::open(&db_path).await.expect("open connection");

        setup_join_fixture(&conn).await;
        conn.execute("PRAGMA fsqlite.parity_cert=ON;")
            .await
            .expect("enable parity cert");
        conn.execute("PRAGMA fsqlite.parity_cert_strict=ON;")
            .await
            .expect("enable strict cert mode");

        let sql = "SELECT items.name, tags.tag \
                   FROM items JOIN tags ON items.id = tags.item_id;";
        let rows = conn
            .query(sql)
            .await
            .expect("strict certifying mode should allow pager-backed file join path");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values()[0], SqliteValue::Text("alpha".into()));
        assert_eq!(rows[0].values()[1], SqliteValue::Text("fruit".into()));
    });
}

#[test]
fn certifying_mode_strict_rejects_in_memory_fallback_query() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.expect("open connection");

        conn.execute("CREATE TABLE items (id INTEGER, name TEXT);")
            .await
            .expect("create items");
        conn.execute("CREATE TABLE tags (id INTEGER, tag TEXT);")
            .await
            .expect("create tags");
        conn.execute("INSERT INTO items VALUES (1, 'alpha');")
            .await
            .expect("insert item");
        conn.execute("INSERT INTO tags VALUES (1, 'fruit');")
            .await
            .expect("insert tag");
        conn.execute("PRAGMA fsqlite.parity_cert=ON;")
            .await
            .expect("enable parity cert");
        conn.execute("PRAGMA fsqlite.parity_cert_strict=ON;")
            .await
            .expect("enable strict cert mode");

        let err = conn
            .query("SELECT items.name, tags.tag FROM items JOIN tags USING (id);")
            .await
            .expect_err("strict certifying mode must reject interpreted memory fallback");
        assert!(
            err.to_string()
                .contains("in-memory fallback disabled in strict parity-cert mode"),
            "unexpected error in certifying strict mode: {err}"
        );
    });
}

#[test]
fn runtime_mode_allows_same_query_path() {
    asupersync::test_utils::run_test(|| async {
        let temp = tempdir().expect("tempdir");
        let db_path = temp.path().join("zjisk1-runtime-mode.db");
        let db_path = db_path.to_string_lossy().to_string();
        let conn = Connection::open(&db_path).await.expect("open connection");

        setup_join_fixture(&conn).await;
        conn.execute("PRAGMA fsqlite.parity_cert=OFF;")
            .await
            .expect("disable certifying mode");
        conn.execute("PRAGMA fsqlite.parity_cert_strict=ON;")
            .await
            .expect("strict flag may stay on in runtime mode");

        let rows = conn
            .query(
                "SELECT items.name, tags.tag \
                 FROM items JOIN tags ON items.id = tags.item_id;",
            )
            .await
            .expect("runtime mode should allow fallback query");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values()[0], SqliteValue::Text("alpha".into()));
        assert_eq!(rows[0].values()[1], SqliteValue::Text("fruit".into()));
    });
}

#[test]
fn mode_pragmas_report_expected_state() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:")
            .await
            .expect("open in-memory connection");
        let parity_rows = conn
            .query("PRAGMA fsqlite.parity_cert;")
            .await
            .expect("query parity_cert");
        assert_eq!(
            parity_rows[0].values()[0],
            SqliteValue::Integer(1),
            "parity_cert must default to ON"
        );

        let strict_rows = conn
            .query("PRAGMA fsqlite.parity_cert_strict;")
            .await
            .expect("query parity_cert_strict");
        assert_eq!(
            strict_rows[0].values()[0],
            SqliteValue::Integer(0),
            "parity_cert_strict defaults OFF for non-certifying runtime by default"
        );

        let backend_kind_rows = conn
            .query("PRAGMA fsqlite.backend_kind;")
            .await
            .expect("query backend_kind");
        assert_eq!(
            backend_kind_rows[0].values()[0],
            SqliteValue::Text("memory".into()),
            "in-memory connection must report memory backend_kind"
        );

        let backend_mode_rows = conn
            .query("PRAGMA fsqlite.backend_mode;")
            .await
            .expect("query backend_mode");
        assert_eq!(
            backend_mode_rows[0].values()[0],
            SqliteValue::Text("parity_cert".into()),
            "backend_mode must reflect default parity_cert mode"
        );
    });
}

#[test]
fn bead_metadata_constants_are_stable_for_replay() {
    assert_eq!(BEAD_ID, "bd-zjisk.1");
    assert_eq!(SCENARIO_ID, "BACKEND-WIRING-MODES");
    assert_eq!(SEED, 3520);
}
