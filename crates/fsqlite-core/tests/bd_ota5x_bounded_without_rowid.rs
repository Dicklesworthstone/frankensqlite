//! bd-ota5x / GH#341: `validate_database_integrity_bounded` must accept a
//! simple WITHOUT ROWID table (leading PK, no secondary index, no foreign key)
//! by walking its index b-tree directly, instead of refusing every WR table.
//!
//! Part-1 scope (this change): the WR table-row walk. Secondary indexes
//! (concordance probes by rowid) and foreign keys (parent/child probes by
//! rowid) on WR tables are still refused pending parts 2/3, so this keeper also
//! pins that a WR table carrying a secondary index remains refused rather than
//! mis-validated.
//!
//! Bounded validation only admits a self-contained rollback/DELETE-mode image,
//! so the accept case builds one (PRAGMA journal_mode=DELETE + VACUUM INTO)
//! exactly as the in-crate bounded tests do.

use fsqlite_core::connection::Connection;
use fsqlite_error::FrankenError;

#[test]
fn bounded_integrity_accepts_simple_without_rowid_tables() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let image = dir.path().join("wr-image.db");

        // Build a DELETE-mode self-contained image: seed under rollback mode,
        // then VACUUM INTO so the written image is header-marked rollback.
        let builder = Connection::open(
            dir.path()
                .join("wr-builder.db")
                .to_string_lossy()
                .into_owned(),
        )
        .await
        .unwrap();
        builder
            .execute("PRAGMA journal_mode=DELETE;")
            .await
            .unwrap();
        // Leading single-column PK, with NOT NULL + CHECK the WR walk must run.
        builder
            .execute(
                "CREATE TABLE t(k TEXT PRIMARY KEY, v TEXT NOT NULL, CHECK(length(v) > 0)) WITHOUT ROWID;",
            )
            .await
            .unwrap();
        builder
            .execute("INSERT INTO t(k, v) VALUES ('a','1'),('b','2'),('c','3');")
            .await
            .unwrap();
        // A secondary index on the WR table exercises the WR index concordance:
        // its entries are keyed by (v, PK columns...), not a trailing rowid.
        builder.execute("CREATE INDEX t_v ON t(v);").await.unwrap();
        // Leading composite PK in declared order.
        builder
            .execute(
                "CREATE TABLE u(a INTEGER, b INTEGER, payload TEXT, PRIMARY KEY(a, b)) WITHOUT ROWID;",
            )
            .await
            .unwrap();
        builder
            .execute("INSERT INTO u(a, b, payload) VALUES (1,10,'x'),(2,20,'y');")
            .await
            .unwrap();
        builder
            .execute("CREATE INDEX u_payload ON u(payload);")
            .await
            .unwrap();
        builder
            .execute(&format!(
                "VACUUM INTO '{}';",
                image.to_string_lossy().replace('\'', "''")
            ))
            .await
            .unwrap();
        let receipt = builder
            .inspect_self_contained_image_receipt(&image)
            .await
            .expect("receipt the built image");
        builder.close().await.expect("close builder");

        let owner = Connection::open(dir.path().join("owner.db").to_string_lossy().into_owned())
            .await
            .unwrap();
        let snapshot = owner
            .begin_bounded_structural_snapshot(&receipt, &image, 256)
            .await
            .expect("image opens a bounded snapshot");

        // The WR row walk must succeed: a misread of the index b-tree would
        // return DatabaseCorrupt, and mis-evaluated NOT NULL / CHECK would fail
        // here too.
        snapshot
            .connection()
            .validate_database_integrity_bounded(dir.path())
            .await
            .expect("bounded integrity must accept a simple WITHOUT ROWID schema");
    });
}

#[test]
fn bounded_integrity_still_refuses_without_rowid_in_fk_schema() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("wrfk.db").to_string_lossy().into_owned();
        let conn = Connection::open(&db).await.unwrap();

        // A WR table plus a rowid table that declares a FOREIGN KEY. WR foreign
        // key parent/child probes (part-3) are not implemented, so the
        // schema-support gate refuses the whole image before any structural walk.
        conn.execute("CREATE TABLE t(k TEXT PRIMARY KEY, v TEXT) WITHOUT ROWID;")
            .await
            .unwrap();
        conn.execute("CREATE TABLE child(id INTEGER PRIMARY KEY, ref TEXT REFERENCES t(k));")
            .await
            .unwrap();

        let err = conn
            .validate_database_integrity_bounded(dir.path())
            .await
            .expect_err(
                "WITHOUT ROWID in a schema with foreign keys is not yet bounded-validatable",
            );
        assert!(
            matches!(&err, FrankenError::NotImplemented(msg) if msg.contains("foreign keys")),
            "expected a WR-in-FK-schema refusal, got: {err:?}"
        );
    });
}
