//! bd-m0e3b / GH#340: WITHOUT ROWID VACUUM INTO column-order coverage + the
//! supported-PK-shape boundary.
//!
//! fsqlite supports a WITHOUT ROWID table only when its PRIMARY KEY is exactly
//! the leading declared columns, in declared order (codegen.rs
//! `without_rowid_pk_indices`: `pos == col_idx`). For that shape declared-order
//! storage IS PK-leading order, so m0e3b's VACUUM INTO serialization
//! (compat_persist.rs, "declared column order") is correct — the leading
//! `pk_count` columns are exactly the b-tree key. The original m0e3b keeper used
//! `t(k PRIMARY KEY, v)` (single leading-PK column); these cases add trailing
//! data columns and a composite leading PK, then diff the vacuumed image
//! against stock sqlite3.
//!
//! A non-leading-PK WITHOUT ROWID table can be CREATEd and INSERTed, but its
//! persist/VACUUM path refuses it with `NotImplemented(... non-leading PRIMARY
//! KEY ...)` — so m0e3b never serializes a shape it cannot represent. Stock
//! SQLite *does* support those tables, so full non-leading-PK WR support is a
//! separate feature gap; this keeper pins the current refusal boundary.

use fsqlite_core::connection::Connection;
use fsqlite_error::FrankenError;
use fsqlite_types::value::SqliteValue;

#[test]
fn without_rowid_leading_pk_vacuum_roundtrips_against_stock() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("src.db").to_string_lossy().into_owned();
        let target_path = dir.path().join("tgt.db");
        let target = target_path.to_string_lossy().into_owned();

        let conn = Connection::open(&source).await.unwrap();
        // Leading single-column PK with TWO trailing data columns.
        conn.execute("CREATE TABLE t(k TEXT PRIMARY KEY, v TEXT, w TEXT) WITHOUT ROWID;")
            .await
            .unwrap();
        conn.execute("INSERT INTO t(k, v, w) VALUES ('b','v1','w1'),('a','v2','w2'),('c','v3','w3');")
            .await
            .unwrap();
        // Leading COMPOSITE PK (a, b) in declared order, with a trailing column.
        conn.execute("CREATE TABLE u(a INTEGER, b INTEGER, payload TEXT, PRIMARY KEY(a, b)) WITHOUT ROWID;")
            .await
            .unwrap();
        conn.execute("INSERT INTO u(a, b, payload) VALUES (1,10,'x'),(1,20,'y'),(2,10,'z');")
            .await
            .unwrap();
        conn.execute_with_params("VACUUM INTO ?1;", &[SqliteValue::Text(target.clone().into())])
            .await
            .unwrap();
        drop(conn);

        let stock = rusqlite::Connection::open(&target_path).unwrap();
        let integrity: String = stock
            .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(integrity, "ok", "GH#340: stock rejected the vacuumed WR image: {integrity}");

        let t_rows: Vec<(String, String, String)> = {
            let mut s = stock.prepare("SELECT k, v, w FROM t ORDER BY k;").unwrap();
            s.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                .unwrap()
                .collect::<std::result::Result<_, _>>()
                .unwrap()
        };
        assert_eq!(
            t_rows,
            vec![
                ("a".into(), "v2".into(), "w2".into()),
                ("b".into(), "v1".into(), "w1".into()),
                ("c".into(), "v3".into(), "w3".into()),
            ]
        );
        let u_rows: Vec<(i64, i64, String)> = {
            let mut s = stock.prepare("SELECT a, b, payload FROM u ORDER BY a, b;").unwrap();
            s.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                .unwrap()
                .collect::<std::result::Result<_, _>>()
                .unwrap()
        };
        assert_eq!(
            u_rows,
            vec![(1, 10, "x".into()), (1, 20, "y".into()), (2, 10, "z".into())]
        );
    });
}

#[test]
fn without_rowid_non_leading_pk_vacuum_is_refused() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        // CREATE of a non-leading-PK WITHOUT ROWID table is allowed today...
        conn.execute("CREATE TABLE t(v TEXT, k TEXT PRIMARY KEY) WITHOUT ROWID;")
            .await
            .unwrap();
        // ...but the first INSERT refuses the shape it cannot serialize, so no
        // row image is ever written for a PK layout m0e3b's VACUUM path (and the
        // storage format generally) cannot represent.
        let err = conn
            .execute("INSERT INTO t(k, v) VALUES ('a','1');")
            .await
            .expect_err("INSERT into a non-leading-PK WITHOUT ROWID table must be refused, not corrupt");
        assert!(
            matches!(&err, FrankenError::NotImplemented(msg) if msg.contains("non-leading PRIMARY KEY")),
            "expected a non-leading-PK WR refusal, got: {err:?}"
        );
    });
}
