//! bd-r82et: a single-connection multi-statement DDL batch must commit.
//!
//! Downstream (mcp_agent_mail_rust migrate) opens a FRESH file and executes
//! one multi-hundred-KB DDL batch (~50 CREATE TABLE + ~150 CREATE INDEX) in a
//! single transaction. With no peers, a snapshot conflict is impossible by
//! definition — the v0.3.1-era regression refused the batch with `database is
//! busy (snapshot conflict on pages: N)` out of the append-gate freelist
//! guards (self-superseded publications / in-memory-origin pops treated as
//! peer conflicts). Fixed by the durable-pop classification (4b5a1b04c) and
//! the self-superseded-publication recognition; this keeper pins the shape,
//! plus a drop/recreate churn round, with the stock oracle green.

use fsqlite::Connection;

const TABLES: usize = 50;

fn ddl_batch(round: usize) -> String {
    let mut sql = String::from("BEGIN;\n");
    for t in 0..TABLES {
        sql.push_str(&format!(
            "CREATE TABLE t_{round}_{t} (id TEXT PRIMARY KEY, body TEXT NOT NULL DEFAULT '', \
             created_ts INTEGER NOT NULL DEFAULT 0, flags INTEGER NOT NULL DEFAULT 0);\n"
        ));
        sql.push_str(&format!(
            "CREATE INDEX idx_{round}_{t}_created ON t_{round}_{t}(created_ts);\n"
        ));
        sql.push_str(&format!(
            "CREATE INDEX idx_{round}_{t}_flags ON t_{round}_{t}(flags);\n"
        ));
        sql.push_str(&format!(
            "CREATE INDEX idx_{round}_{t}_body ON t_{round}_{t}(body);\n"
        ));
    }
    sql.push_str("COMMIT;\n");
    sql
}

#[test]
fn single_writer_ddl_batch_commits_without_snapshot_conflict() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("r82et_ddl_batch.db");
    let db = db_path.to_string_lossy().into_owned();

    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(&db).await.expect("open fresh file");
        conn.execute_batch("PRAGMA journal_mode=WAL;")
            .await
            .expect("wal mode");

        // Round 0: the exact downstream shape — one large DDL batch on a
        // fresh file, single connection, zero peers.
        conn.execute_batch(&ddl_batch(0))
            .await
            .expect("single-connection DDL batch must commit (no peers => no snapshot conflict)");

        // Churn round: drop everything and recreate — exercises freelist
        // consume + republish within one writer, the regression's surface.
        let mut drop_sql = String::from("BEGIN;\n");
        for t in 0..TABLES {
            drop_sql.push_str(&format!("DROP TABLE t_0_{t};\n"));
        }
        drop_sql.push_str("COMMIT;\n");
        conn.execute_batch(&drop_sql)
            .await
            .expect("bulk DROP batch must commit");
        conn.execute_batch(&ddl_batch(1))
            .await
            .expect("recreate DDL batch must commit over the freed pages");

        let rows = conn
            .query("SELECT COUNT(*) FROM sqlite_master WHERE type IN ('table','index');")
            .await
            .expect("catalog query");
        assert_eq!(rows.len(), 1);
        conn.close().await.expect("close");
    });

    let oracle = rusqlite::Connection::open(&db_path).expect("oracle open");
    let integrity: String = oracle
        .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
        .expect("oracle integrity_check");
    assert_eq!(
        integrity, "ok",
        "stock integrity_check must pass after DDL churn"
    );
    let tables: i64 = oracle
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name LIKE 't_1_%';",
            [],
            |row| row.get(0),
        )
        .expect("oracle table count");
    assert_eq!(tables, TABLES as i64);
}
