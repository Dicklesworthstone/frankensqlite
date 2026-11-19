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
//!
//! Held integer secondary-key lookups (bd-05xlf, the ORM point lookup) run the
//! same way between commits of a stock child, an in-process fsqlite peer and
//! an fsqlite child, across RESTART and TRUNCATE checkpoints. Between two
//! executions no read mark may be left behind, so a stock TRUNCATE checkpoint
//! completes in full; under a stock writer committing and restarting the WAL
//! while the lookup runs, every answer must be the state of one commit.

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
            "stock_rows" => {
                stock_rows_child(&db, &sql, Path::new(&std::env::var(OUT).unwrap()));
            }
            "stock_insert_loop" => stock_insert_loop_child(&db, &sql),
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

    // bd-05xlf lever 1 (read/seek round 7): the per-execution read
    // transaction of a held integer secondary-key lookup, the ORM point
    // lookup prepared-dml-probe measures. Between executions the connection
    // holds no read mark and no snapshot, so every execution must admit a
    // reader that sees the latest commit of any engine in any process, across
    // WAL restarts and truncations, and must release its mark again so that a
    // stock TRUNCATE checkpoint run between executions completes in full.

    /// The held lookups and the key each runs with: three in a pager read
    /// transaction and a rowid lookup, which the prepared memdb fast path
    /// answers from the connection's row mirror. bd-2mpxx: that mirror kept
    /// the pre-UPDATE row after a stock commit that followed a WAL reset, until
    /// an fsqlite process committed, because the WAL-derived commit sequence
    /// did not move.
    const KEY_PROBES: &[(&str, i64)] = &[
        ("SELECT id FROM k WHERE a = ?1", 7),
        ("SELECT b FROM k WHERE a = ?1", 7),
        ("SELECT count(*), max(id) FROM k WHERE a = ?1", 7),
        ("SELECT b FROM k WHERE id = ?1", 7),
    ];

    /// Rows the seed inserts; `a = id % 50`, so key 7 starts with 80 rows and
    /// the race key -1 with none.
    const KEY_SEED_ROWS: i64 = 4000;

    fn key_seed_sql() -> String {
        format!(
            "PRAGMA journal_mode=WAL;\n\
             CREATE TABLE k(id INTEGER PRIMARY KEY, a INTEGER, b TEXT);\n\
             CREATE INDEX k_a ON k(a);\n\
             INSERT INTO k(id, a, b) WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 \
             FROM s WHERE i < {KEY_SEED_ROWS}) SELECT i, i % 50, 'seed-' || i FROM s;\n"
        )
    }

    /// What stock answers for each query right now, one line per query. A
    /// query line is `<integer parameter or empty>\t<sql>`.
    fn stock_rows(db: &Path, queries: &[(&str, Option<i64>)]) -> Vec<String> {
        let script = queries
            .iter()
            .map(|(sql, param)| format!("{}\t{sql}", param.map_or_else(String::new, |p| p.to_string())))
            .collect::<Vec<_>>()
            .join("\n");
        let out = db.with_extension("rows");
        child("stock_rows", db, &script, Some(&out));
        let text = std::fs::read_to_string(&out).unwrap();
        text.split('\n').map(str::to_owned).collect()
    }

    fn stock_key_answers(db: &Path) -> Vec<String> {
        let queries: Vec<(&str, Option<i64>)> = KEY_PROBES
            .iter()
            .map(|(sql, key)| (*sql, Some(*key)))
            .collect();
        stock_rows(db, &queries)
    }

    /// The `stock_rows` child: run each query line and render its rows.
    fn stock_rows_child(db: &Path, script: &str, out: &Path) {
        let conn = rusqlite::Connection::open(db).unwrap();
        let mut lines = Vec::new();
        for line in script.split('\n') {
            let (param, sql) = line.split_once('\t').unwrap();
            let mut stmt = conn.prepare(sql).unwrap();
            let width = stmt.column_count();
            let params: Vec<i64> = if param.is_empty() {
                Vec::new()
            } else {
                vec![param.parse().unwrap()]
            };
            let mut rows = stmt.query(rusqlite::params_from_iter(params)).unwrap();
            let mut answer = Vec::new();
            while let Some(row) = rows.next().unwrap() {
                answer.push(
                    (0..width)
                        .map(|i| render_stock(row.get_ref(i).unwrap()))
                        .collect::<Vec<_>>()
                        .join(","),
                );
            }
            lines.push(answer.join("|"));
        }
        std::fs::write(out, lines.join("\n")).unwrap();
    }

    /// The `stock_insert_loop` child: commit one row of key -1 per
    /// transaction, with a RESTART checkpoint after every 20th, while the
    /// parent reads.
    fn stock_insert_loop_child(db: &Path, script: &str) {
        let rows: i64 = script.trim().parse().unwrap();
        let conn = rusqlite::Connection::open(db).unwrap();
        conn.busy_timeout(std::time::Duration::from_secs(10)).unwrap();
        conn.execute_batch("PRAGMA synchronous=NORMAL;").unwrap();
        for i in 1..=rows {
            conn.execute("INSERT INTO k(a, b) VALUES(-1, ?1)", [format!("race-{i}")])
                .unwrap();
            if i % 20 == 0 {
                conn.query_row("PRAGMA wal_checkpoint(RESTART);", [], |_| Ok(()))
                    .unwrap();
            }
        }
    }

    async fn fsqlite_key_answer(
        stmt: &fsqlite_core::connection::PreparedStatement<'_>,
        key: i64,
    ) -> String {
        match stmt.query_with_params(&[SqliteValue::Integer(key)]).await {
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

    async fn fsqlite_key_answers(
        stmts: &[fsqlite_core::connection::PreparedStatement<'_>],
    ) -> Vec<String> {
        let mut answers = Vec::new();
        for (stmt, (_, key)) in stmts.iter().zip(KEY_PROBES) {
            answers.push(fsqlite_key_answer(stmt, *key).await);
        }
        answers
    }

    /// The rows of one rendered answer in a fixed order. The probes have no
    /// ORDER BY, so their row order is the plan's: after the stock CREATE
    /// INDEX step stock walks `k_ab` and fsqlite may walk `k_a`, and both
    /// orders are SQLite's answer. The rows themselves must match.
    fn row_set(answer: &str) -> Vec<&str> {
        let mut rows: Vec<&str> = answer.split('|').collect();
        rows.sort_unstable();
        rows
    }

    fn compare_key_answers(db: &Path, label: &str, got: &[String], mismatches: &mut Vec<String>) {
        let want = stock_key_answers(db);
        for (i, (got, want)) in got.iter().zip(&want).enumerate() {
            if row_set(got) != row_set(want) {
                mismatches.push(format!(
                    "{label} probe {i} ({}):\n  fsqlite {got}\n  stock   {want}",
                    KEY_PROBES[i].0
                ));
            }
        }
    }

    /// Stock's TRUNCATE checkpoint must find no reader between two
    /// executions: it answers busy 0, no frames left, and an empty `-wal`.
    fn assert_stock_truncate_completes(db: &Path, label: &str, mismatches: &mut Vec<String>) {
        let answer = stock_rows(db, &[("PRAGMA wal_checkpoint(TRUNCATE);", None)]);
        let wal_len = std::fs::metadata(db.with_extension("db-wal")).map_or(0, |meta| meta.len());
        if answer != ["I0,I0,I0"] || wal_len != 0 {
            mismatches.push(format!(
                "{label}: stock TRUNCATE between executions answered {answer:?}, -wal {wal_len} bytes"
            ));
        }
    }

    #[test]
    fn held_integer_key_lookups_see_every_commit_and_release_their_read_mark() {
        if std::env::var(ROLE).is_ok() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("keys.db");
        child("stock", &db, &key_seed_sql(), None);

        // (engine, script) between executions. `peer` is a second fsqlite
        // connection in this process.
        let steps: &[(&str, &str)] = &[
            ("stock", "INSERT INTO k(a, b) VALUES(7, 'stock-1');"),
            ("stock", "UPDATE k SET b = 'stock-upd' WHERE id = 7;"),
            ("peer", "INSERT INTO k(a, b) VALUES(7, 'peer-1');"),
            (
                "stock",
                "PRAGMA wal_checkpoint(RESTART); INSERT INTO k(a, b) VALUES(7, 'after-restart');",
            ),
            (
                "stock",
                "PRAGMA wal_checkpoint(TRUNCATE); DELETE FROM k WHERE b = 'stock-1';",
            ),
            (
                "fsqlite",
                "INSERT INTO k(a, b) VALUES(7, 'fsq-child'); UPDATE k SET b = 'fsq-upd' WHERE id = 57;",
            ),
            (
                "stock",
                "INSERT INTO k(a, b) WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 \
                 FROM s WHERE i < 500) SELECT 7, 'bulk-' || i FROM s;",
            ),
            ("peer", "UPDATE k SET b = 'peer-upd' WHERE a = 7 AND b LIKE 'bulk-1%';"),
            ("stock", "CREATE INDEX k_ab ON k(a, b);"),
            ("stock", "DROP INDEX k_ab; DELETE FROM k WHERE a = 7 AND id > 4000;"),
            // bd-2mpxx: a commit and then a TRUNCATE leave no WAL frame at
            // all; the change lives only in the main file.
            (
                "stock",
                "UPDATE k SET b = 'truncated-away' WHERE id = 7; PRAGMA wal_checkpoint(TRUNCATE);",
            ),
            (
                "stock",
                "DROP INDEX k_a; PRAGMA wal_checkpoint(TRUNCATE); \
                 UPDATE k SET b = 'no-index' WHERE id = 7; PRAGMA wal_checkpoint(TRUNCATE);",
            ),
            (
                "stock",
                "CREATE INDEX k_a ON k(a); PRAGMA wal_checkpoint(TRUNCATE);",
            ),
        ];

        let mut mismatches = Vec::new();
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(db.to_str().unwrap().to_owned())
                .await
                .unwrap();
            let peer = Connection::open(db.to_str().unwrap().to_owned())
                .await
                .unwrap();
            let mut stmts = Vec::new();
            for (probe, _) in KEY_PROBES {
                stmts.push(conn.prepare(probe).await.unwrap());
            }
            let answers = fsqlite_key_answers(&stmts).await;
            compare_key_answers(&db, "initial", &answers, &mut mismatches);
            assert_stock_truncate_completes(&db, "initial", &mut mismatches);

            for (step, (engine, script)) in steps.iter().enumerate() {
                if *engine == "peer" {
                    peer.execute_batch(script).await.unwrap();
                } else {
                    child(engine, &db, script, None);
                }
                // Twice: the second execution reads through whatever the
                // first one cached.
                for round in 0..2 {
                    let answers = fsqlite_key_answers(&stmts).await;
                    compare_key_answers(
                        &db,
                        &format!("step {step} {engine} round {round}"),
                        &answers,
                        &mut mismatches,
                    );
                }
                assert_stock_truncate_completes(
                    &db,
                    &format!("after step {step} {engine}"),
                    &mut mismatches,
                );
            }

            // An explicit transaction keeps its snapshot while the in-process
            // peer and a stock process commit, and sees both once it ends.
            let before = fsqlite_key_answer(&stmts[2], 7).await;
            conn.execute("BEGIN;").await.unwrap();
            let first = fsqlite_key_answer(&stmts[2], 7).await;
            peer.execute("INSERT INTO k(a, b) VALUES(7, 'in-txn-peer');")
                .await
                .unwrap();
            let after_peer = fsqlite_key_answer(&stmts[2], 7).await;
            child(
                "stock",
                &db,
                "INSERT INTO k(a, b) VALUES(7, 'in-txn-stock');",
                None,
            );
            let after_stock = fsqlite_key_answer(&stmts[2], 7).await;
            conn.execute("COMMIT;").await.unwrap();
            if first != before || after_peer != before || after_stock != before {
                mismatches.push(format!(
                    "snapshot: before {before}, in txn {first}, after peer {after_peer}, \
                     after stock {after_stock}"
                ));
            }
            let answers = fsqlite_key_answers(&stmts).await;
            compare_key_answers(&db, "after the snapshot transaction", &answers, &mut mismatches);
            assert_stock_truncate_completes(&db, "after the snapshot transaction", &mut mismatches);

            drop(stmts);
            peer.close().await.unwrap();
            conn.close().await.unwrap();
        });
        assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    }

    /// Rows the racing stock writer commits.
    const RACE_ROWS: i64 = 400;

    #[test]
    fn held_integer_key_lookup_reads_whole_commits_while_stock_commits_and_restarts() {
        if std::env::var(ROLE).is_ok() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("race.db");
        child("stock", &db, &key_seed_sql(), None);
        let script = db.with_extension("script.sql");
        std::fs::write(&script, RACE_ROWS.to_string()).unwrap();

        let mut failures = Vec::new();
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(db.to_str().unwrap().to_owned())
                .await
                .unwrap();
            let stmt = conn
                .prepare("SELECT count(*), max(id) FROM k WHERE a = ?1")
                .await
                .unwrap();
            let mut writer = Command::new(std::env::current_exe().unwrap())
                .args([CHILD_TEST, "--exact", "--nocapture", "--test-threads=1"])
                .env(ROLE, "stock_insert_loop")
                .env(DB, &db)
                .env(SQL, &script)
                .spawn()
                .unwrap();
            // Each answer is one commit's state: the writer's rows are ids
            // KEY_SEED_ROWS + 1 ..= KEY_SEED_ROWS + n, so count n pairs with
            // max(id) KEY_SEED_ROWS + n, and n never moves backward.
            let mut last = 0_i64;
            let mut executions = 0_u64;
            loop {
                let done = writer.try_wait().unwrap();
                let answer = fsqlite_key_answer(&stmt, -1).await;
                executions += 1;
                let count = if answer == "I0,N" {
                    Some(0)
                } else {
                    answer.split_once(',').and_then(|(count, max)| {
                        let count: i64 = count.strip_prefix('I')?.parse().ok()?;
                        let max: i64 = max.strip_prefix('I')?.parse().ok()?;
                        (max == KEY_SEED_ROWS + count).then_some(count)
                    })
                };
                match count {
                    Some(count) if count >= last => last = count,
                    _ => failures.push(format!("after count {last}: {answer}")),
                }
                if let Some(status) = done {
                    assert!(status.success(), "racing stock writer failed: {status:?}");
                    break;
                }
            }
            let final_answer = fsqlite_key_answer(&stmt, -1).await;
            if final_answer != format!("I{RACE_ROWS},I{}", KEY_SEED_ROWS + RACE_ROWS) {
                failures.push(format!("after the writer exited: {final_answer}"));
            }
            eprintln!("{executions} executions raced the writer; last count seen {last}");
            drop(stmt);
            conn.close().await.unwrap();
        });
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }
}
