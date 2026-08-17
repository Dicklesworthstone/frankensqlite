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
//! A non-leading / reordered-PK WITHOUT ROWID table is now stored physically
//! PK-leading (codegen `emit_wr_record` reorders on write; the table cursor and
//! the row inflater remap on read — bd-v6pjf/bd-0ntuc), so the in-memory
//! read/write path round-trips it against stock. The second keeper below pins
//! that support. On-disk VACUUM/file-format parity for non-leading PK is a
//! separate follow-up slice (compat_persist record reorder).

use fsqlite_core::connection::Connection;
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

fn text_of(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Text(s) => s.to_string(),
        other => panic!("expected a TEXT value, got {other:?}"),
    }
}

#[test]
fn without_rowid_non_leading_pk_roundtrips_in_memory() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        // bd-v6pjf: a non-leading-PK WITHOUT ROWID table is now stored
        // physically PK-leading and its reads remap back to declared order, so
        // the shape round-trips instead of being refused.
        conn.execute("CREATE TABLE t(v TEXT, k TEXT PRIMARY KEY) WITHOUT ROWID;")
            .await
            .unwrap();
        conn.execute("INSERT INTO t(k, v) VALUES ('a','1'),('c','3'),('b','2');")
            .await
            .expect("non-leading-PK WITHOUT ROWID INSERT must now succeed");

        // Declared projection (v, k) returned in natural PK(k) scan order.
        let rows = conn.query("SELECT v, k FROM t").await.expect("select");
        let got: Vec<(String, String)> = rows
            .iter()
            .map(|row| {
                let vals = row.values();
                (text_of(&vals[0]), text_of(&vals[1]))
            })
            .collect();
        assert_eq!(
            got,
            vec![
                ("1".to_owned(), "a".to_owned()),
                ("2".to_owned(), "b".to_owned()),
                ("3".to_owned(), "c".to_owned()),
            ],
            "non-leading-PK WR must read back declared order in PK scan order"
        );

        // A PK point lookup remaps the projected column too.
        let one = conn
            .query("SELECT v FROM t WHERE k = 'b'")
            .await
            .expect("point lookup");
        assert_eq!(text_of(&one[0].values()[0]), "2");
    });
}
