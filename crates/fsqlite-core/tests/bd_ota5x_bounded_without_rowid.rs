//! bd-ota5x / GH#341: `validate_database_integrity_bounded` must accept a
//! WITHOUT ROWID table across its full structural family — the row walk, the
//! secondary-index concordance, and (part-3) foreign keys — instead of refusing
//! every WR table.
//!
//! Parts 1+2 (landed): the WR table-row walk AND secondary-index concordance
//! (entries keyed by the PK locator, not a rowid). Part-3 (this file's foreign
//! key cases): a WR table may be a foreign-key PARENT (probed by seeking its own
//! PRIMARY KEY b-tree on the PK-tuple prefix) or a foreign-key CHILD (walked as
//! an index b-tree). The three failing-direction cases pin that the WR parent
//! probe and the WR child walk actually reject a dangling reference rather than
//! pass corruption as ok.
//!
//! Bounded validation only admits a self-contained rollback/DELETE-mode image,
//! so every case builds one (PRAGMA journal_mode=DELETE + VACUUM INTO) exactly
//! as the in-crate bounded tests do.

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

/// Part-3 positive: WITHOUT ROWID tables that participate in FOREIGN KEYs as
/// both parents and children, with every reference satisfied, must pass bounded
/// integrity. Exercises: a rowid child -> WR parent (single-column PK probe), a
/// rowid child -> WR parent (composite PK probe), a WR child -> rowid parent
/// (WR child walk + IPK probe), and a WR child -> WR parent (WR child walk + WR
/// parent probe). A wrong PK-tuple key/seek would miss a present parent and
/// wrongly report DatabaseCorrupt here.
#[test]
fn bounded_integrity_accepts_without_rowid_foreign_keys() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let image = dir.path().join("wrfk-ok.db");

        let builder = Connection::open(
            dir.path()
                .join("wrfk-ok-builder.db")
                .to_string_lossy()
                .into_owned(),
        )
        .await
        .unwrap();
        builder
            .execute("PRAGMA journal_mode=DELETE;")
            .await
            .unwrap();
        // Parents.
        builder
            .execute("CREATE TABLE rp(id INTEGER PRIMARY KEY, name TEXT);")
            .await
            .unwrap();
        builder
            .execute("INSERT INTO rp(id, name) VALUES (1,'one'),(2,'two');")
            .await
            .unwrap();
        builder
            .execute("CREATE TABLE wp(k TEXT PRIMARY KEY, d TEXT) WITHOUT ROWID;")
            .await
            .unwrap();
        builder
            .execute("INSERT INTO wp(k, d) VALUES ('a','A'),('b','B');")
            .await
            .unwrap();
        builder
            .execute(
                "CREATE TABLE wpc(x INTEGER, y INTEGER, note TEXT, PRIMARY KEY(x, y)) WITHOUT ROWID;",
            )
            .await
            .unwrap();
        builder
            .execute("INSERT INTO wpc(x, y, note) VALUES (1,10,'p'),(2,20,'q');")
            .await
            .unwrap();
        // Children.
        builder
            .execute("CREATE TABLE rc(id INTEGER PRIMARY KEY, ref TEXT REFERENCES wp(k));")
            .await
            .unwrap();
        // A NULL child key is skipped by the FK check, so it must not trip it.
        builder
            .execute("INSERT INTO rc(id, ref) VALUES (1,'a'),(2,'b'),(3,NULL);")
            .await
            .unwrap();
        builder
            .execute(
                "CREATE TABLE crc(id INTEGER PRIMARY KEY, fx INTEGER, fy INTEGER, FOREIGN KEY(fx, fy) REFERENCES wpc(x, y));",
            )
            .await
            .unwrap();
        builder
            .execute("INSERT INTO crc(id, fx, fy) VALUES (1,1,10),(2,2,20);")
            .await
            .unwrap();
        builder
            .execute(
                "CREATE TABLE wc(k TEXT PRIMARY KEY, pid INTEGER REFERENCES rp(id)) WITHOUT ROWID;",
            )
            .await
            .unwrap();
        builder
            .execute("INSERT INTO wc(k, pid) VALUES ('m',1),('n',2);")
            .await
            .unwrap();
        builder
            .execute(
                "CREATE TABLE wc2(k TEXT PRIMARY KEY, wref TEXT REFERENCES wp(k)) WITHOUT ROWID;",
            )
            .await
            .unwrap();
        builder
            .execute("INSERT INTO wc2(k, wref) VALUES ('p','a'),('q','b');")
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

        let owner = Connection::open(
            dir.path()
                .join("wrfk-ok-owner.db")
                .to_string_lossy()
                .into_owned(),
        )
        .await
        .unwrap();
        let snapshot = owner
            .begin_bounded_structural_snapshot(&receipt, &image, 256)
            .await
            .expect("image opens a bounded snapshot");
        snapshot
            .connection()
            .validate_database_integrity_bounded(dir.path())
            .await
            .expect("bounded integrity must accept satisfied WITHOUT ROWID foreign keys");
    });
}

/// Part-3 negative (WR parent probe): a rowid child holding a dangling reference
/// to a WITHOUT ROWID parent's PRIMARY KEY must be rejected. Seeded with
/// foreign_keys=OFF so the orphan reaches disk, then VACUUM INTO. If the WR
/// parent probe silently skipped validation (or the PK-tuple seek were wrong),
/// this corruption would pass as ok.
#[test]
fn bounded_integrity_rejects_orphan_rowid_child_of_without_rowid_parent() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let image = dir.path().join("wrfk-orphan-parent.db");

        let builder = Connection::open(
            dir.path()
                .join("wrfk-orphan-parent-builder.db")
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
            .execute("PRAGMA foreign_keys=OFF;")
            .await
            .unwrap();
        builder
            .execute("CREATE TABLE wp(k TEXT PRIMARY KEY, d TEXT) WITHOUT ROWID;")
            .await
            .unwrap();
        builder
            .execute("INSERT INTO wp(k, d) VALUES ('a','A'),('b','B');")
            .await
            .unwrap();
        builder
            .execute("CREATE TABLE rc(id INTEGER PRIMARY KEY, ref TEXT REFERENCES wp(k));")
            .await
            .unwrap();
        // 'zzz' has no parent row in wp — a dangling reference.
        builder
            .execute("INSERT INTO rc(id, ref) VALUES (1,'a'),(2,'zzz');")
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

        let owner = Connection::open(
            dir.path()
                .join("wrfk-orphan-parent-owner.db")
                .to_string_lossy()
                .into_owned(),
        )
        .await
        .unwrap();
        let snapshot = owner
            .begin_bounded_structural_snapshot(&receipt, &image, 256)
            .await
            .expect("image opens a bounded snapshot");
        let err = snapshot
            .connection()
            .validate_database_integrity_bounded(dir.path())
            .await
            .expect_err("a dangling reference to a WITHOUT ROWID parent must be rejected");
        assert!(
            matches!(&err, FrankenError::DatabaseCorrupt { .. }),
            "expected DatabaseCorrupt from the WR parent FK probe, got: {err:?}"
        );
        let detail = format!("{err:?}");
        assert!(
            detail.contains("FOREIGN KEY"),
            "expected a FOREIGN KEY violation detail, got: {detail}"
        );
    });
}

/// Part-3 negative (WR child walk): a WITHOUT ROWID child holding a dangling
/// reference must be rejected. Proves the WR child index-cursor walk actually
/// reaches each WR child row and feeds it to the parent probe (a rowid parent
/// here, so the failure isolates the WR child walk from the WR parent probe).
#[test]
fn bounded_integrity_rejects_orphan_without_rowid_child() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let image = dir.path().join("wrfk-orphan-child.db");

        let builder = Connection::open(
            dir.path()
                .join("wrfk-orphan-child-builder.db")
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
            .execute("PRAGMA foreign_keys=OFF;")
            .await
            .unwrap();
        builder
            .execute("CREATE TABLE rp(id INTEGER PRIMARY KEY, name TEXT);")
            .await
            .unwrap();
        builder
            .execute("INSERT INTO rp(id, name) VALUES (1,'one'),(2,'two');")
            .await
            .unwrap();
        builder
            .execute(
                "CREATE TABLE wc(k TEXT PRIMARY KEY, pid INTEGER REFERENCES rp(id)) WITHOUT ROWID;",
            )
            .await
            .unwrap();
        // pid=99 has no parent row in rp — a dangling reference in a WR child.
        builder
            .execute("INSERT INTO wc(k, pid) VALUES ('m',1),('n',99);")
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

        let owner = Connection::open(
            dir.path()
                .join("wrfk-orphan-child-owner.db")
                .to_string_lossy()
                .into_owned(),
        )
        .await
        .unwrap();
        let snapshot = owner
            .begin_bounded_structural_snapshot(&receipt, &image, 256)
            .await
            .expect("image opens a bounded snapshot");
        let err = snapshot
            .connection()
            .validate_database_integrity_bounded(dir.path())
            .await
            .expect_err("a dangling reference from a WITHOUT ROWID child must be rejected");
        assert!(
            matches!(&err, FrankenError::DatabaseCorrupt { .. }),
            "expected DatabaseCorrupt from the WR child FK walk, got: {err:?}"
        );
        let detail = format!("{err:?}");
        assert!(
            detail.contains("FOREIGN KEY"),
            "expected a FOREIGN KEY violation detail, got: {detail}"
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
