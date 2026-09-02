//! bd-kon3m residual: a legacy contentless FTS5 table (`content=''` WITHOUT
//! `contentless_delete=1`) whose structure record was poisoned into origin
//! tracking by a pre-fix automerge must NOT keep writing 3-column origin
//! `_docsize` rows into its 2-column shadow, and must reopen.
//!
//! The 6dad0e026 fix stopped NEW poisoning at merge time. The residual was an
//! already-poisoned structure on disk: `uses_origin_tracking()` stayed true, so
//! every later append wrote a 3-column `_docsize` row and the table refused to
//! reopen (`stores 3 payload columns but schema allows at most 2`).
//!
//! The fix keys origin tracking off the DECLARED config, not the structure
//! byte: on a legacy table the connection strips the spurious origin fields
//! before appending, so the append writes legacy rows and rewrites a clean
//! (legacy) structure record — self-healing the poison on the next write.
//!
//! This keeper plants the poison directly (stock SQLite rewrites the structure
//! `_data` row), then proves an fsqlite append heals it: pre-fix, the reopen
//! after the append would fail with `DatabaseCorrupt`.
#![cfg(feature = "ext-fts5")]

use fsqlite_core::connection::Connection;
use fsqlite_ext_fts5::Fts5StructureRecord;
use fsqlite_types::value::SqliteValue;

const STRUCTURE_ROWID: i64 = 10;

async fn match_rowids(conn: &Connection, term: &str) -> Vec<i64> {
    let mut ids: Vec<i64> = conn
        .query(&format!(
            "SELECT rowid FROM t WHERE t MATCH '{term}' ORDER BY rowid;"
        ))
        .await
        .unwrap_or_else(|e| panic!("MATCH '{term}' failed: {e}"))
        .iter()
        .map(|row| match row.values()[0] {
            SqliteValue::Integer(n) => n,
            ref other => panic!("expected rowid integer, got {other:?}"),
        })
        .collect();
    ids.sort_unstable();
    ids
}

fn read_structure(stock: &rusqlite::Connection) -> Fts5StructureRecord {
    let block: Vec<u8> = stock
        .query_row(
            "SELECT block FROM t_data WHERE id = ?1",
            [STRUCTURE_ROWID],
            |r| r.get(0),
        )
        .expect("read structure _data row");
    Fts5StructureRecord::decode(&block).expect("decode structure record")
}

#[test]
fn bd_kon3m_legacy_contentless_append_self_heals_a_poisoned_structure() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let db_path = dir.path().join("bd_kon3m.db");
        let db_str = db_path.to_string_lossy().into_owned();

        // A LEGACY contentless table: content='' and NO contentless_delete.
        {
            let conn = Connection::open(&db_str).await.expect("open franken");
            conn.execute(
                "CREATE VIRTUAL TABLE t USING fts5(body, content='', tokenize='porter');",
            )
            .await
            .expect("create legacy contentless fts5");
            for id in 1..=6 {
                conn.execute(&format!(
                    "INSERT INTO t(rowid, body) VALUES ({id}, 'common word{id}');"
                ))
                .await
                .expect("insert");
            }
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                .await
                .expect("checkpoint");
            conn.close().await.expect("close franken");
        }

        // Plant the poison with stock SQLite: stamp origin tracking onto the
        // structure record, exactly as the pre-fix automerge did.
        {
            let stock = rusqlite::Connection::open(&db_path).expect("stock open");
            let mut structure = read_structure(&stock);
            assert!(
                !structure.uses_origin_tracking(),
                "a legacy table starts non-origin-tracking"
            );
            structure.origin_counter = 1;
            if let Some(segment) = structure
                .levels
                .iter_mut()
                .flat_map(|level| level.segments.iter_mut())
                .next()
            {
                segment.entry_count = 5;
            }
            let poisoned = structure.encode();
            stock
                .execute(
                    "UPDATE t_data SET block = ?1 WHERE id = ?2",
                    rusqlite::params![poisoned, STRUCTURE_ROWID],
                )
                .expect("write poisoned structure");
            assert!(
                read_structure(&stock).uses_origin_tracking(),
                "the planted structure is now origin-poisoned"
            );
            // Stock still verifies the file structurally (the poison is an FTS5
            // metadata lie, not a b-tree defect).
            let integrity: String = stock
                .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
                .expect("stock integrity_check after poison");
            assert_eq!(integrity, "ok");
        }

        // fsqlite reopens lazily and appends. Pre-fix this writes a 3-column
        // `_docsize` row against the 2-column shadow; post-fix it strips the
        // spurious origin and writes legacy rows.
        {
            let conn = Connection::open(&db_str).await.expect("reopen franken");
            assert_eq!(
                match_rowids(&conn, "common").await,
                vec![1, 2, 3, 4, 5, 6],
                "lazy reopen reads the legacy corpus"
            );
            conn.execute("INSERT INTO t(rowid, body) VALUES (100, 'common fresh');")
                .await
                .expect("append after poison");
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                .await
                .expect("checkpoint");
            conn.close().await.expect("close franken");
        }

        // The discriminator: a second fsqlite reopen. Pre-fix the 3-column
        // `_docsize` row makes this fail with `DatabaseCorrupt`.
        {
            let conn = Connection::open(&db_str).await.expect("reopen after append");
            assert_eq!(
                match_rowids(&conn, "common").await,
                vec![1, 2, 3, 4, 5, 6, 100],
                "old corpus plus the appended row survive the poisoned-structure append"
            );
            assert_eq!(match_rowids(&conn, "fresh").await, vec![100]);
            conn.close().await.expect("close");
        }

        // The append self-healed the structure record, and stock still verifies.
        let stock = rusqlite::Connection::open(&db_path).expect("stock open");
        assert!(
            !read_structure(&stock).uses_origin_tracking(),
            "the append rewrote a clean (legacy) structure record"
        );
        let integrity: String = stock
            .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
            .expect("final stock integrity_check");
        assert_eq!(integrity, "ok");
        let docsize_columns: i64 = stock
            .query_row("SELECT count(*) FROM pragma_table_info('t_docsize')", [], |r| {
                r.get(0)
            })
            .expect("docsize column count");
        assert_eq!(docsize_columns, 2, "the _docsize shadow stays 2-column");
    });
}
