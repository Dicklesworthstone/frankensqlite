//! bd-yqjjx item (5) — WITHOUT ROWID, non-leading PRIMARY KEY, ALTER ADD COLUMN,
//! reopened from disk.
//!
//! A WITHOUT ROWID table stores its record physically PK-leading (PK columns
//! first, then the rest in declared order). For a non-leading PK the read path
//! must reorder physical -> declared before consuming the payload positionally.
//! That reorder was gated on a FULL-WIDTH payload, so an old row stored SHORT
//! (an `ALTER TABLE ADD COLUMN` back-fill, missing the new trailing column) was
//! left in physical order and consumed as declared order -> the original columns
//! came out transposed. This guards the fix: the short payload is reordered too,
//! and the ALTER-added column back-fills from its DEFAULT.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

#[test]
fn wr_nonleading_pk_alter_add_column_reopen_bd_yqjjx() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("wr_alter_reopen.db");
        let db_str = db_path.to_string_lossy().into_owned();

        {
            let conn = Connection::open(&db_str).await.unwrap();
            // Non-leading PK: 'k' is the PRIMARY KEY but the SECOND declared
            // column, so the physical record stores [k, v] while the declared
            // order is [v, k].
            conn.execute("CREATE TABLE t (v INTEGER, k TEXT PRIMARY KEY) WITHOUT ROWID")
                .await
                .unwrap();
            conn.execute("INSERT INTO t (v, k) VALUES (10, 'a'), (20, 'b')")
                .await
                .unwrap();
            // Old rows are now stored short (two physical fields, no 'c').
            conn.execute("ALTER TABLE t ADD COLUMN c TEXT DEFAULT 'z'")
                .await
                .unwrap();
            conn.close().await.unwrap();
        }

        // Reopen from disk and read the short rows back.
        let conn = Connection::open(&db_str).await.unwrap();
        let rows = conn
            .query("SELECT v, k, c FROM t ORDER BY k")
            .await
            .unwrap();
        let got: Vec<Vec<SqliteValue>> = rows.iter().map(|r| r.values().to_vec()).collect();
        assert_eq!(
            got,
            vec![
                vec![
                    SqliteValue::Integer(10),
                    SqliteValue::Text("a".into()),
                    SqliteValue::Text("z".into()),
                ],
                vec![
                    SqliteValue::Integer(20),
                    SqliteValue::Text("b".into()),
                    SqliteValue::Text("z".into()),
                ],
            ]
        );
    });
}

/// The same short-row reopen, but read through the streaming JOIN source
/// (`try_scan_join_source_from_pager` -> the interpreted inflater the bead
/// names). This is the path most likely to consume a physical short payload
/// without reordering.
#[test]
fn wr_nonleading_pk_alter_add_column_reopen_join_bd_yqjjx() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("wr_alter_reopen_join.db");
        let db_str = db_path.to_string_lossy().into_owned();

        {
            let conn = Connection::open(&db_str).await.unwrap();
            conn.execute("CREATE TABLE t (v INTEGER, k TEXT PRIMARY KEY) WITHOUT ROWID")
                .await
                .unwrap();
            conn.execute("INSERT INTO t (v, k) VALUES (10, 'a'), (20, 'b')")
                .await
                .unwrap();
            conn.execute("ALTER TABLE t ADD COLUMN c TEXT DEFAULT 'z'")
                .await
                .unwrap();
            conn.execute("CREATE TABLE other (k TEXT, x INTEGER)")
                .await
                .unwrap();
            conn.execute("INSERT INTO other VALUES ('a', 100), ('b', 200)")
                .await
                .unwrap();
            conn.close().await.unwrap();
        }

        let conn = Connection::open(&db_str).await.unwrap();
        let rows = conn
            .query(
                "SELECT t.v, t.k, t.c, other.x FROM t JOIN other ON t.k = other.k ORDER BY t.k",
            )
            .await
            .unwrap();
        let got: Vec<Vec<SqliteValue>> = rows.iter().map(|r| r.values().to_vec()).collect();
        assert_eq!(
            got,
            vec![
                vec![
                    SqliteValue::Integer(10),
                    SqliteValue::Text("a".into()),
                    SqliteValue::Text("z".into()),
                    SqliteValue::Integer(100),
                ],
                vec![
                    SqliteValue::Integer(20),
                    SqliteValue::Text("b".into()),
                    SqliteValue::Text("z".into()),
                    SqliteValue::Integer(200),
                ],
            ]
        );
    });
}
