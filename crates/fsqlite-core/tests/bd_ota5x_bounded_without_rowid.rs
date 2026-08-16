//! bd-ota5x / GH#341: `validate_database_integrity_bounded` must accept a
//! simple WITHOUT ROWID table (leading PK, no secondary index, no foreign key)
//! by walking its index b-tree directly, instead of refusing every WR table.
//!
//! Parts 1+2 (landed): the WR table-row walk AND secondary-index concordance
//! (entries keyed by the PK locator, not a rowid). Foreign keys on WR tables
//! (parent/child probes by rowid) are still refused pending part-3, so this
//! keeper also pins that a WR table in a foreign-key-declaring schema remains
//! refused rather than mis-validated.
//!
//! Bounded validation only admits a self-contained rollback/DELETE-mode image,
//! so the accept case builds one (PRAGMA journal_mode=DELETE + VACUUM INTO)
//! exactly as the in-crate bounded tests do.

use fsqlite_core::connection::Connection;
use fsqlite_error::FrankenError;
use fsqlite_types::value::SqliteValue;

/// Read the database page size from the file header (bytes 16..18, big-endian;
/// the value `1` encodes 65536 per the SQLite format).
fn header_page_size(bytes: &[u8]) -> usize {
    let raw = u16::from_be_bytes([bytes[16], bytes[17]]);
    if raw == 1 { 65_536 } else { usize::from(raw) }
}

/// Zero the b-tree cell count (u16 at header offset 3..5) of a 1-based page.
/// Page 1 carries the 100-byte database header before its b-tree header; every
/// other page starts with it.
fn set_cell_count_zero(bytes: &mut [u8], page_size: usize, page_1based: usize) {
    let base = (page_1based - 1) * page_size;
    let hdr = if page_1based == 1 { base + 100 } else { base };
    bytes[hdr + 3] = 0;
    bytes[hdr + 4] = 0;
}

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

/// Negative safety net for the part-2 concordance probe: a WITHOUT ROWID table
/// whose secondary index no longer covers its rows MUST be rejected. This proves
/// the WR index-concordance branch actually probes the index with the computed
/// `(index terms..., PRIMARY KEY columns...)` key — a byte-exact match against a
/// wrong/missing entry, not a silent pass. If `bounded_index_key`'s PK-suffix or
/// the probe were wrong, this corruption would slip through as "ok".
#[test]
fn bounded_integrity_rejects_without_rowid_with_broken_secondary_index() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let image = dir.path().join("wr-broken-index.db");

        // Build the same DELETE-mode self-contained image shape as the accept
        // case: a WR table (leading single-column PK) carrying a secondary index.
        let builder = Connection::open(
            dir.path()
                .join("wr-broken-builder.db")
                .to_string_lossy()
                .into_owned(),
        )
        .await
        .unwrap();
        builder
            .execute("PRAGMA journal_mode=DELETE;")
            .await
            .unwrap();
        builder
            .execute("CREATE TABLE t(k TEXT PRIMARY KEY, v TEXT NOT NULL) WITHOUT ROWID;")
            .await
            .unwrap();
        builder
            .execute("INSERT INTO t(k, v) VALUES ('a','1'),('b','2'),('c','3');")
            .await
            .unwrap();
        builder.execute("CREATE INDEX t_v ON t(v);").await.unwrap();
        builder
            .execute(&format!(
                "VACUUM INTO '{}';",
                image.to_string_lossy().replace('\'', "''")
            ))
            .await
            .unwrap();
        builder.close().await.expect("close builder");

        // Locate the secondary index's root page in the freshly written image.
        let reader = Connection::open(image.to_string_lossy().into_owned())
            .await
            .unwrap();
        let rows = reader
            .query("SELECT rootpage FROM sqlite_schema WHERE type='index' AND name='t_v';")
            .await
            .expect("query the secondary index root page");
        let index_root = match rows.first().map(|row| &row.values()[0]) {
            Some(SqliteValue::Integer(n)) if *n > 0 => *n as usize,
            other => panic!("expected a positive integer index rootpage, got {other:?}"),
        };
        reader.close().await.expect("close reader");

        // Zero the index root leaf's cell count: the secondary index now reports
        // no entries while every table row remains. An empty *root* leaf is a
        // structurally legal empty index, so structural validation still passes —
        // it is the concordance probe (key = index terms ++ PK columns) that must
        // catch the divergence, because each expected key now misses the index.
        let mut bytes = std::fs::read(&image).expect("read image");
        let page_size = header_page_size(&bytes);
        assert!(
            index_root >= 2 && index_root * page_size <= bytes.len(),
            "index rootpage {index_root} out of range for a {}-page image",
            bytes.len() / page_size
        );
        set_cell_count_zero(&mut bytes, page_size, index_root);
        std::fs::write(&image, &bytes).expect("write corrupted image");

        // Receipt on the (corrupt) image so the snapshot admits it, then run the
        // bounded integrity path exactly as the accept case does.
        let owner = Connection::open(
            dir.path()
                .join("owner-broken.db")
                .to_string_lossy()
                .into_owned(),
        )
        .await
        .unwrap();
        let receipt = owner
            .inspect_self_contained_image_receipt(&image)
            .await
            .expect("receipt the corrupt image");
        let snapshot = owner
            .begin_bounded_structural_snapshot(&receipt, &image, 256)
            .await
            .expect("an empty (but structurally legal) secondary index still opens a snapshot");
        let err = snapshot
            .connection()
            .validate_database_integrity_bounded(dir.path())
            .await
            .expect_err("a WR table whose secondary index is missing every entry must be rejected");
        assert!(
            matches!(&err, FrankenError::DatabaseCorrupt { .. }),
            "expected DatabaseCorrupt from the WR index-concordance probe, got: {err:?}"
        );
        let detail = format!("{err:?}");
        assert!(
            detail.contains("t_v")
                && (detail.contains("missing")
                    || detail.contains("requires exactly")
                    || detail.contains("byte-exact")),
            "expected a WR index-concordance miss naming `t_v`, got: {detail}"
        );
    });
}
