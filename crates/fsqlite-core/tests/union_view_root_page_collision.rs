//! Regression test for the UNION+VIEW root-page collision bug.
//!
//! Symptom on main pre-fix: a UNION query whose arms reference a VIEW caused
//! `execute_with_materialized_views` to allocate the view's MemDatabase temp
//! table at a `next_root_page` that collided with a real index root in the
//! pager. A subsequent index_seek on that real index would then read the
//! pager page that the temp materialization touched, producing the
//!   "index_seek called on table page (type LeafTable, page N, root N)"
//! corruption error reported in beads_rust import_jsonl + REINDEX runs.
//!
//! Root cause: `execute_with_materialized_views` called `create_table()`
//! without first calling `reserve_clean_memdb_root_pages`, unlike the
//! companion `execute_with_materialized_sqlite_schema` path which has called
//! it since inception.
//!
//! Fix: `self.reserve_clean_memdb_root_pages(referenced.len())?` at the top
//! of `execute_with_materialized_views`'s exec_result closure. After the
//! fix, view temp tables get root pages past any pager-occupied page so the
//! collision cannot occur.

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;

#[test]
fn union_with_view_does_not_corrupt_existing_index() {
    asupersync::test_utils::run_test(|| async {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("union_view_collision.db");
    let conn = Connection::open(db_path.to_str().expect("db path utf8"))
        .await
        .expect("open file db");

    // Real persistent tables + indexes — these allocate pager root pages.
    conn.execute(
        "CREATE TABLE blocks (id INTEGER PRIMARY KEY, kind TEXT NOT NULL, payload TEXT NOT NULL)",
    )
    .await
    .expect("create table blocks");
    conn.execute("CREATE INDEX blocks_kind_idx ON blocks(kind)")
        .await
        .expect("create blocks_kind_idx");

    let insert_block = conn
        .prepare("INSERT INTO blocks (id, kind, payload) VALUES (?1, ?2, ?3)")
        .await
        .expect("prepare insert blocks");
    for i in 0..32_i64 {
        insert_block
            .execute_with_params(&[
                SqliteValue::Integer(i),
                SqliteValue::Text(format!("k{}", i % 4).into()),
                SqliteValue::Text(format!("payload-{i}").into()),
            ])
            .await
            .expect("insert blocks row");
    }

    conn.execute(
        "CREATE TABLE deps (id INTEGER PRIMARY KEY, from_block INTEGER NOT NULL, to_block INTEGER NOT NULL)",
    )
    .await
    .expect("create table deps");
    conn.execute("CREATE INDEX deps_from_idx ON deps(from_block)")
        .await
        .expect("create deps_from_idx");
    conn.execute("CREATE INDEX deps_to_idx ON deps(to_block)")
        .await
        .expect("create deps_to_idx");

    let insert_dep = conn
        .prepare("INSERT INTO deps (id, from_block, to_block) VALUES (?1, ?2, ?3)")
        .await
        .expect("prepare insert deps");
    for i in 0..16_i64 {
        insert_dep
            .execute_with_params(&[
                SqliteValue::Integer(i),
                SqliteValue::Integer(i),
                SqliteValue::Integer((i + 1) % 16),
            ])
            .await
            .expect("insert deps row");
    }

    // The VIEW that triggers materialization.
    conn.execute("CREATE VIEW dep_edges AS SELECT from_block AS src, to_block AS dst FROM deps")
        .await
        .expect("create view");

    // The UNION query — same shape that broke beads_rust `get_blocks_dep_edges`.
    // The arm referencing `dep_edges` forces view materialization, and pre-fix
    // that materialization reused a root page belonging to one of the real
    // indexes (typically blocks_kind_idx or deps_from_idx).
    let edges = conn
        .query(
            "SELECT src, dst FROM dep_edges \
             UNION \
             SELECT to_block AS src, from_block AS dst FROM deps \
             ORDER BY src, dst",
        )
        .await
        .expect("execute UNION over view+table");
    assert!(
        !edges.is_empty(),
        "UNION over view+table should yield rows; got empty"
    );

    // After the UNION, exercise the indexes — these would explode pre-fix
    // with `index_seek called on table page (type LeafTable, page N, root N)`
    // because the temp view table overwrote the index's pager state.
    for kind in ["k0", "k1", "k2", "k3"] {
        let rows = conn
            .query_with_params(
                "SELECT id FROM blocks WHERE kind = ?1 ORDER BY id",
                &[SqliteValue::Text((*kind).into())],
            )
            .await
            .expect("execute blocks index lookup");
        assert_eq!(
            rows.len(),
            8,
            "index scan on blocks(kind={kind}) returned wrong count: {} rows",
            rows.len()
        );
    }

    // REINDEX — the original failing operation. Pre-fix this could panic with
    // a B-tree page-type mismatch; post-fix it walks all index roots cleanly
    // because none have been clobbered.
    conn.execute("REINDEX").await.expect("REINDEX");

    // Re-run the index lookups after REINDEX to confirm the indexes survived.
    for kind in ["k0", "k1", "k2", "k3"] {
        let rows = conn
            .query_with_params(
                "SELECT id FROM blocks WHERE kind = ?1 ORDER BY id",
                &[SqliteValue::Text((*kind).into())],
            )
            .await
            .expect("execute post-REINDEX index lookup");
        assert_eq!(
            rows.len(),
            8,
            "post-REINDEX index scan on blocks(kind={kind}) returned wrong count: {} rows",
            rows.len()
        );
    }
    });
}
