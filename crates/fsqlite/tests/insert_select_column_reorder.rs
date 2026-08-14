//! Relocated from the workspace-root `tests/insert_test.rs` (bd-root-tests-
//! orphans-wo59o): the root directory is under a virtual manifest, so this
//! test had NEVER compiled anywhere. Ported from the pre-0.2 sync API to the
//! async storage API in the move; the guarded behavior is INSERT ... SELECT
//! with a reordered explicit column list mapping source columns onto
//! differently-ordered destination columns.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

#[test]
fn insert_select_reordered_columns_map_by_name() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE src (a INTEGER, b TEXT);")
            .await
            .unwrap();
        conn.execute("CREATE TABLE dst (x TEXT, y INTEGER);")
            .await
            .unwrap();
        conn.execute("INSERT INTO src VALUES (10, 'ten');")
            .await
            .unwrap();

        let rows = conn.query("SELECT a, b FROM src;").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(10));
        assert_eq!(rows[0].values()[1], SqliteValue::Text("ten".into()));

        conn.execute("INSERT INTO dst (y, x) SELECT a, b FROM src;")
            .await
            .unwrap();

        let rows2 = conn.query("SELECT x, y FROM dst;").await.unwrap();
        assert_eq!(rows2.len(), 1);
        assert_eq!(
            rows2[0].values()[0],
            SqliteValue::Text("ten".into()),
            "column-list reorder must map SELECT position 2 (b) onto dst.x"
        );
        assert_eq!(
            rows2[0].values()[1],
            SqliteValue::Integer(10),
            "column-list reorder must map SELECT position 1 (a) onto dst.y"
        );

        conn.close().await.unwrap();
    });
}
