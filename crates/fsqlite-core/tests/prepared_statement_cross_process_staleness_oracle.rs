#![recursion_limit = "512"]

//! Review of 43ec7360d (bd-xml0z) and a5bc1e3c4 (bd-9zuif): prepared reads on a
//! file-backed connection no longer reload the MemDatabase on every execution,
//! and a statement prepared before another connection's DDL re-prepares.
//!
//! This holds prepared statements on one connection while OTHER PROCESSES
//! (this test binary re-executed, running fsqlite or stock SQLite) change the
//! database between executions: row DML, index and table rebuilds that move
//! root pages, a TRUNCATE checkpoint followed by writes into the restarted WAL,
//! VACUUM, and ADD COLUMN. After every step each held statement must return
//! what stock SQLite reads from the same file at that moment. The oracle runs
//! in a child process too: a stock connection closed inside this process would
//! drop the POSIX locks fsqlite holds on the same file.
//!
//! An explicit transaction must keep reading its own snapshot while another
//! process commits, and see the commit once it ends.

#[cfg(all(unix, feature = "native"))]
mod unix_only {
    use std::path::{Path, PathBuf};
    use std::process::Command;

    use fsqlite_core::connection::Connection;
    use fsqlite_types::SqliteValue;

    const ROLE: &str = "XPROC_PREP_ROLE";
    const DB: &str = "XPROC_PREP_DB";
    const SQL: &str = "XPROC_PREP_SQL";
    const OUT: &str = "XPROC_PREP_OUT";
    const CHILD_TEST: &str = "unix_only::xproc_prepared_child";

    /// The held statements and the parameter each runs with.
    const PROBES: &[(&str, &str)] = &[
        ("SELECT v FROM r WHERE tenant = 't' AND id = ?1", "k0042"),
        ("SELECT v FROM w WHERE id = ?1", "w0042"),
        (
            "SELECT id, v FROM r WHERE tenant = 't' AND party = ?1 ORDER BY id",
            "p7",
        ),
        (
            "SELECT count(*), sum(length(v)) FROM r WHERE tenant = ?1",
            "t",
        ),
        (
            "SELECT * FROM w WHERE id >= ?1 ORDER BY id LIMIT 3",
            "w0040",
        ),
    ];

    fn render(value: &SqliteValue) -> String {
        match value {
            SqliteValue::Null => "N".to_owned(),
            SqliteValue::Integer(v) => format!("I{v}"),
            SqliteValue::Float(v) => format!("R{v}"),
            SqliteValue::Text(v) => format!("T{v}"),
            SqliteValue::Blob(v) => format!("B{v:?}"),
        }
    }

    fn render_stock(value: rusqlite::types::ValueRef<'_>) -> String {
        use rusqlite::types::ValueRef;
        match value {
            ValueRef::Null => "N".to_owned(),
            ValueRef::Integer(v) => format!("I{v}"),
            ValueRef::Real(v) => format!("R{v}"),
            ValueRef::Text(v) => format!("T{}", String::from_utf8_lossy(v)),
            ValueRef::Blob(v) => format!("B{v:?}"),
        }
    }

    /// Run `role` in a child process and wait for it. The script travels in a
    /// file: the seed is too large for the environment.
    fn child(role: &str, db: &Path, sql: &str, out: Option<&Path>) {
        let script = db.with_extension("script.sql");
        std::fs::write(&script, sql).unwrap();
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([CHILD_TEST, "--exact", "--nocapture", "--test-threads=1"])
            .env(ROLE, role)
            .env(DB, db)
            .env(SQL, &script);
        if let Some(out) = out {
            command.env(OUT, out);
        }
        let status = command.status().unwrap();
        assert!(
            status.success(),
            "{role} child failed on {sql:?}: {status:?}"
        );
    }

    /// What stock reads for every probe right now, one line per probe.
    fn stock_answers(db: &Path) -> Vec<String> {
        let out = db.with_extension("oracle");
        child("oracle", db, "", Some(&out));
        let text = std::fs::read_to_string(&out).unwrap();
        text.lines().map(str::to_owned).collect()
    }

    async fn fsqlite_answer(
        stmt: &fsqlite_core::connection::PreparedStatement<'_>,
        param: &str,
    ) -> String {
        match stmt.query_with_params(&[SqliteValue::from(param)]).await {
            Ok(rows) => rows
                .iter()
                .map(|row| {
                    row.values()
                        .iter()
                        .map(render)
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .collect::<Vec<_>>()
                .join("|"),
            Err(error) => format!("ERR {error}"),
        }
    }

    fn seed_sql() -> String {
        let mut sql = String::from(
            "PRAGMA journal_mode=WAL;\n\
             CREATE TABLE r(tenant TEXT, id TEXT, party TEXT, v TEXT, PRIMARY KEY(tenant, id));\n\
             CREATE INDEX r_party ON r(tenant, party);\n\
             CREATE TABLE w(id TEXT PRIMARY KEY, v TEXT) WITHOUT ROWID;\n\
             BEGIN;\n",
        );
        for i in 0..3000 {
            sql.push_str(&format!(
                "INSERT INTO r VALUES('t','k{i:04}','p{}','{}');\n",
                i % 11,
                "x".repeat(40 + i % 50)
            ));
            sql.push_str(&format!(
                "INSERT INTO w VALUES('w{i:04}','{}');\n",
                "y".repeat(30 + i % 7)
            ));
        }
        sql.push_str("COMMIT;\n");
        sql
    }

    /// Child entry point; a no-op unless re-executed by the test below.
    #[test]
    fn xproc_prepared_child() {
        let Ok(role) = std::env::var(ROLE) else {
            return;
        };
        let db = PathBuf::from(std::env::var(DB).unwrap());
        let sql = std::fs::read_to_string(std::env::var(SQL).unwrap()).unwrap();
        match role.as_str() {
            "fsqlite" => asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(db.to_str().unwrap().to_owned())
                    .await
                    .unwrap();
                conn.execute_batch(&sql).await.unwrap();
                conn.close().await.unwrap();
            }),
            "stock" => {
                let conn = rusqlite::Connection::open(&db).unwrap();
                conn.execute_batch(&sql).unwrap();
            }
            "oracle" => {
                let conn = rusqlite::Connection::open(&db).unwrap();
                let check: String = conn
                    .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
                    .unwrap();
                assert_eq!(check, "ok");
                let mut lines = Vec::new();
                for (probe, param) in PROBES {
                    let line = match conn.prepare(probe) {
                        Ok(mut stmt) => {
                            let width = stmt.column_count();
                            let mut rows = stmt.query([param]).unwrap();
                            let mut out = Vec::new();
                            while let Some(row) = rows.next().unwrap() {
                                out.push(
                                    (0..width)
                                        .map(|i| render_stock(row.get_ref(i).unwrap()))
                                        .collect::<Vec<_>>()
                                        .join(","),
                                );
                            }
                            out.join("|")
                        }
                        Err(error) => format!("ERR {error}"),
                    };
                    lines.push(line);
                }
                std::fs::write(std::env::var(OUT).unwrap(), lines.join("\n")).unwrap();
            }
            other => panic!("unknown role {other}"),
        }
    }

    #[test]
    fn held_prepared_statements_track_other_processes() {
        if std::env::var(ROLE).is_ok() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("xproc.db");
        child("fsqlite", &db, &seed_sql(), None);

        // (engine, script) applied by another process between executions.
        let steps: &[(&str, &str)] = &[
            (
                "fsqlite",
                "UPDATE r SET v = 'upd-' || id WHERE id IN ('k0042','k0100'); UPDATE w SET v = 'wupd' WHERE id = 'w0042';",
            ),
            (
                "stock",
                "UPDATE r SET v = 'stock-' || id WHERE party = 'p7'; DELETE FROM w WHERE id = 'w0041';",
            ),
            (
                "fsqlite",
                "DROP INDEX r_party; CREATE INDEX r_party ON r(tenant, party, v); INSERT INTO r VALUES('t','k9999','p7','late');",
            ),
            (
                "stock",
                "DROP TABLE w; CREATE TABLE w(id TEXT PRIMARY KEY, v TEXT) WITHOUT ROWID; INSERT INTO w VALUES('w0040','a'),('w0042','b'),('w0050','c');",
            ),
            (
                "fsqlite",
                "PRAGMA wal_checkpoint(TRUNCATE); UPDATE r SET v = 'after-reset' WHERE id = 'k0042'; INSERT INTO w VALUES('w0041','reset');",
            ),
            (
                "stock",
                "PRAGMA wal_checkpoint(TRUNCATE); UPDATE r SET v = 'stock-reset' WHERE id = 'k0042';",
            ),
            ("stock", "DELETE FROM r WHERE id > 'k1500'; VACUUM;"),
            (
                "stock",
                "UPDATE r SET v = 'post-vacuum' WHERE id = 'k0042';",
            ),
            (
                "fsqlite",
                "ALTER TABLE w ADD COLUMN extra INTEGER DEFAULT 7; UPDATE w SET extra = 9 WHERE id = 'w0042';",
            ),
            (
                "stock",
                "DROP INDEX r_party; UPDATE r SET party = 'p7' WHERE id = 'k0042';",
            ),
        ];

        let mut mismatches = Vec::new();
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(db.to_str().unwrap().to_owned())
                .await
                .unwrap();
            let mut stmts = Vec::new();
            for (probe, _) in PROBES {
                stmts.push(conn.prepare(probe).await.unwrap());
            }
            let check = |label: &str, got: Vec<String>, mismatches: &mut Vec<String>| {
                let want = stock_answers(&db);
                for (i, (got, want)) in got.iter().zip(&want).enumerate() {
                    if got != want {
                        mismatches.push(format!(
                            "{label} probe {i} ({}):\n  fsqlite {got}\n  stock   {want}",
                            PROBES[i].0
                        ));
                    }
                }
            };
            let mut answers = Vec::new();
            for (stmt, (_, param)) in stmts.iter().zip(PROBES) {
                answers.push(fsqlite_answer(stmt, param).await);
            }
            check("initial", answers, &mut mismatches);

            for (step, (engine, script)) in steps.iter().enumerate() {
                child(engine, &db, script, None);
                // Twice: the second run catches a stale cache the first refilled.
                for round in 0..2 {
                    let mut answers = Vec::new();
                    for (stmt, (_, param)) in stmts.iter().zip(PROBES) {
                        answers.push(fsqlite_answer(stmt, param).await);
                    }
                    check(
                        &format!("step {step} {engine} round {round}"),
                        answers,
                        &mut mismatches,
                    );
                }
            }

            // An explicit transaction reads one snapshot while another process
            // commits, then sees the commit once it ends.
            let before = fsqlite_answer(&stmts[0], PROBES[0].1).await;
            conn.execute("BEGIN;").await.unwrap();
            let inside_first = fsqlite_answer(&stmts[0], PROBES[0].1).await;
            child(
                "fsqlite",
                &db,
                "UPDATE r SET v = 'snapshot-peer' WHERE id = 'k0042';",
                None,
            );
            let inside_after_peer = fsqlite_answer(&stmts[0], PROBES[0].1).await;
            conn.execute("COMMIT;").await.unwrap();
            let after = fsqlite_answer(&stmts[0], PROBES[0].1).await;
            if inside_first != before || inside_after_peer != before {
                mismatches.push(format!(
                    "snapshot: before {before}, in txn {inside_first} then {inside_after_peer}"
                ));
            }
            if after != "Tsnapshot-peer" {
                mismatches.push(format!("after snapshot txn: {after}"));
            }
            drop(stmts);
            conn.close().await.unwrap();
        });
        assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    }
}
