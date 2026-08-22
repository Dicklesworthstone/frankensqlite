//! Fresh-eyes probe: lazy segment MERGE over STOCK-written segments must not
//! orphan the inputs' %_idx rows / dlidx %_data rows (the strict
//! integrity_report then rejects the shadow on the next promote).

use fsqlite_core::connection::Connection;

#[test]
fn fresh_eyes_merge_over_stock_segments_then_promote() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("stock_merge.db");

        // Stock writes a contentless_delete corpus at tiny pgsz so segments are
        // multi-leaf (with %_idx rows and, for the big doclist, dlidx rows).
        {
            let stock = rusqlite::Connection::open(&db_path).unwrap();
            stock
                .execute_batch(
                    "CREATE VIRTUAL TABLE t USING fts5(body, content='', contentless_delete=1);
                     INSERT INTO t(t, rank) VALUES('pgsz', 64);",
                )
                .unwrap();
            // One big segment via optimize: 'common' doclist spans many leaves
            // at pgsz=64, so stock writes a doclist index (dlidx) for it.
            let tx = stock.unchecked_transaction().unwrap();
            for row in 0..200 {
                let mut body = String::from("common ");
                for w in 0..6 {
                    body.push_str(&format!("w{row}x{w} "));
                }
                tx.execute(
                    "INSERT INTO t(rowid, body) VALUES (?1, ?2)",
                    rusqlite::params![row + 1, body],
                )
                .unwrap();
            }
            tx.commit().unwrap();
            // no optimize: keep the dlidx-bearing segment at level 0
            let idx_rows: i64 = stock
                .query_row("SELECT count(*) FROM t_idx", [], |r| r.get(0))
                .unwrap();
            let dlidx_rows: i64 = stock
                .query_row(
                    "SELECT count(*) FROM t_data WHERE id >= 1<<37 AND (id >> 36) % 2 = 1",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            eprintln!("stock wrote idx_rows={idx_rows} dlidx_rows={dlidx_rows}");
            let segs: i64 = stock
                .query_row("SELECT count(*) FROM t_data WHERE id = 10", [], |r| {
                    r.get(0)
                })
                .unwrap();
            let _ = segs;
        }

        // Frank opens (lazy default), appends rows until a merge fires, then
        // forces a promote via 'optimize'.
        let db_str = db_path.to_string_lossy().into_owned();
        let conn = Connection::open_existing_schema_only(&db_str)
            .await
            .unwrap();
        for id in 1000..1006 {
            conn.execute(&format!(
                "INSERT INTO t(rowid, body) VALUES ({id}, 'common fresh{id}');"
            ))
            .await
            .unwrap();
        }
        // MATCH still works through the lazy reader.
        let rows = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'common' ORDER BY rowid;")
            .await
            .unwrap();
        eprintln!("match common -> {} rows", rows.len());
        assert_eq!(
            rows.len(),
            206,
            "all stock + frank rows match before promote"
        );

        // Promote (optimize promotes before its no-op). If the merge orphaned
        // stock rows, this errors with 'references unknown segment'.
        conn.execute("INSERT INTO t(t) VALUES('optimize');")
            .await
            .unwrap();
        let rows2 = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'common' ORDER BY rowid;")
            .await
            .unwrap();
        assert_eq!(rows2.len(), 206, "corpus intact after promote");
        conn.close().await.unwrap();
    });
}
