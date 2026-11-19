#![recursion_limit = "512"]

//! bd-kcf1h: a held prepared rowid lookup after an own commit that jumped past
//! a peer's commit.
//!
//! The prepared rowid fast path answers from the connection's row mirror. The
//! mirror decides staleness by commit sequence and, since bd-2mpxx, by the page
//! cache's image epoch. When this connection (R) commits a transaction that
//! began before another process (P) committed, R's commit certificate's first
//! sequence is above R's `commit_seq + 1`: the commit jumps R's sequence past
//! P's (bd-iay3d). If the mirror were then stamped with R's own commit, the
//! peer's row would stay invisible to R's held lookup.
//!
//! Shape, with stock as the oracle and P an fsqlite child process in one arm and
//! a stock child process in the other:
//! 1. R holds `SELECT b FROM k WHERE id = ?1` and runs it, from the mirror.
//! 2. R begins and rewrites row 3900 (another page than row 7).
//! 3. P commits an UPDATE of row 7.
//! 4. R commits.
//! 5. R's held lookups of rows 7 and 3900 must answer what stock reads.
//!
//! Non-vacuity, asserted: step 1 is served by the direct rowid lookup over the
//! mirror (the hot-path profile's `direct_rowid_lookup_query_hits` moves), and
//! in the fsqlite arm step 4 is a jump (the pager's
//! `fsqlite_pager::own_commit_jump` event fires once, with a first sequence
//! above `commit_seq + 1`). A stock peer's commit is caught up before R records
//! its own, so R's COMMIT does not jump there; that arm checks the answers
//! across the same interleaving. An arm whose P cannot commit while R's
//! transaction is open fails on its P step rather than passing vacuously.

#[cfg(all(unix, feature = "native"))]
mod unix_only {
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

    use fsqlite_core::connection::{
        Connection, hot_path_profile_snapshot, reset_hot_path_profile, set_hot_path_profile_enabled,
    };
    use fsqlite_types::SqliteValue;

    const ROLE: &str = "BD_KCF1H_ROLE";
    const DB: &str = "BD_KCF1H_DB";
    const SQL: &str = "BD_KCF1H_SQL";
    const OUT: &str = "BD_KCF1H_OUT";
    const CHILD_TEST: &str = "unix_only::bd_kcf1h_child";
    const LOOKUP: &str = "SELECT b FROM k WHERE id = ?1";
    const SEED_ROWS: i64 = 4000;

    /// The jumps the pager reports (`fsqlite_pager::own_commit_jump`), and the
    /// first-sequence / commit_seq pair of the last one.
    struct JumpCounter {
        count: AtomicU64,
        last: Mutex<Option<(u64, u64)>>,
    }

    struct FieldGrab {
        first: Option<u64>,
        commit: Option<u64>,
    }

    impl tracing::field::Visit for FieldGrab {
        fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
            match field.name() {
                "first_commit_seq" => self.first = Some(value),
                "commit_seq" => self.commit = Some(value),
                _ => {}
            }
        }

        fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
    }

    struct JumpLayer(Arc<JumpCounter>);

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for JumpLayer {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if event.metadata().target() != "fsqlite_pager::own_commit_jump" {
                return;
            }
            let mut grab = FieldGrab {
                first: None,
                commit: None,
            };
            event.record(&mut grab);
            if let (Some(first), Some(commit)) = (grab.first, grab.commit) {
                *self.0.last.lock().unwrap() = Some((first, commit));
            }
            self.0.count.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// The process-wide jump counter. The events can be emitted on runtime
    /// worker threads, so the subscriber is the global default.
    fn jumps() -> &'static Arc<JumpCounter> {
        static JUMPS: OnceLock<Arc<JumpCounter>> = OnceLock::new();
        JUMPS.get_or_init(|| {
            use tracing_subscriber::Layer;
            use tracing_subscriber::layer::SubscriberExt;
            let counter = Arc::new(JumpCounter {
                count: AtomicU64::new(0),
                last: Mutex::new(None),
            });
            // Only the one target, so no other debug callsite in the process turns on.
            let layer = JumpLayer(Arc::clone(&counter)).with_filter(
                tracing_subscriber::filter::Targets::new().with_target(
                    "fsqlite_pager::own_commit_jump",
                    tracing_subscriber::filter::LevelFilter::DEBUG,
                ),
            );
            tracing::subscriber::set_global_default(tracing_subscriber::registry().with(layer))
                .expect("this test binary installs the only global subscriber");
            counter
        })
    }

    /// The hot-path profile and the jump counter are process-wide.
    fn serial() -> MutexGuard<'static, ()> {
        static SERIAL: Mutex<()> = Mutex::new(());
        SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

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

    /// Run `role` in a child process (this test binary re-executed) and report
    /// whether it succeeded. The script travels in a file.
    fn child(role: &str, db: &Path, sql: &str, out: Option<&Path>) -> bool {
        let script = db.with_extension(format!("{role}.sql"));
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
        command.status().unwrap().success()
    }

    /// What stock reads for `LOOKUP` with each key, one answer per key. The
    /// oracle is a child: a stock connection closed in this process would drop
    /// the POSIX locks fsqlite holds on the same file.
    fn stock_lookups(db: &Path, keys: &[i64]) -> Vec<String> {
        let out = db.with_extension("oracle");
        let script = keys
            .iter()
            .map(i64::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(child("oracle", db, &script, Some(&out)), "stock oracle failed");
        std::fs::read_to_string(&out)
            .unwrap()
            .split('\n')
            .map(str::to_owned)
            .collect()
    }

    #[test]
    fn bd_kcf1h_child() {
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
                conn.execute("PRAGMA busy_timeout=5000").await.unwrap();
                conn.execute_batch(&sql).await.unwrap();
                conn.close().await.unwrap();
            }),
            "stock" => {
                let conn = rusqlite::Connection::open(&db).unwrap();
                conn.busy_timeout(std::time::Duration::from_secs(5)).unwrap();
                conn.execute_batch(&sql).unwrap();
            }
            "oracle" => {
                let conn = rusqlite::Connection::open(&db).unwrap();
                let check: String = conn
                    .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
                    .unwrap();
                assert_eq!(check, "ok");
                let mut stmt = conn.prepare(LOOKUP).unwrap();
                let answers: Vec<String> = sql
                    .split('\n')
                    .map(|key| {
                        let key: i64 = key.parse().unwrap();
                        let mut rows = stmt.query([key]).unwrap();
                        let mut out = Vec::new();
                        while let Some(row) = rows.next().unwrap() {
                            out.push(render_stock(row.get_ref(0).unwrap()));
                        }
                        out.join("|")
                    })
                    .collect();
                std::fs::write(std::env::var(OUT).unwrap(), answers.join("\n")).unwrap();
            }
            other => panic!("unknown role {other}"),
        }
    }

    async fn lookup(stmt: &fsqlite_core::connection::PreparedStatement<'_>, key: i64) -> String {
        match stmt.query_with_params(&[SqliteValue::Integer(key)]).await {
            Ok(rows) => rows
                .iter()
                .map(|row| row.values().iter().map(render).collect::<Vec<_>>().join(","))
                .collect::<Vec<_>>()
                .join("|"),
            Err(error) => format!("ERR {error}"),
        }
    }

    fn seed_sql() -> String {
        format!(
            "PRAGMA journal_mode=WAL;\n\
             CREATE TABLE k(id INTEGER PRIMARY KEY, a INTEGER, b TEXT);\n\
             INSERT INTO k(id, a, b) WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 \
             FROM s WHERE i < {SEED_ROWS}) SELECT i, i % 50, 'seed-' || i FROM s;\n"
        )
    }

    /// One arm: P is a `peer_role` child. `jump_required`: R's COMMIT must be a
    /// jump past P's commit (the non-vacuity of the fsqlite arm).
    fn own_commit_jump_arm(peer_role: &str, jump_required: bool) {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join(format!("kcf1h-{peer_role}.db"));
        assert!(child("stock", &db, &seed_sql(), None), "seed failed");
        let jumps = jumps();
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(db.to_str().unwrap().to_owned())
                .await
                .unwrap();
            let stmt = conn.prepare(LOOKUP).await.unwrap();

            // 1. The held lookup, from the mirror.
            set_hot_path_profile_enabled(true);
            reset_hot_path_profile();
            let before_peer = lookup(&stmt, 7).await;
            let direct_hits = hot_path_profile_snapshot().direct_rowid_lookup_query_hits;
            assert_eq!(before_peer, "Tseed-7", "[{peer_role}] the seeded row");
            assert!(
                direct_hits >= 1,
                "[{peer_role}] non-vacuity: the held lookup must be served by the direct rowid \
                 lookup over the row mirror (direct_rowid_lookup_query_hits = {direct_hits})"
            );

            // 2. R's transaction, open across P's commit.
            conn.execute("BEGIN").await.unwrap();
            conn.execute("UPDATE k SET b = 'r-own' WHERE id = 3900")
                .await
                .unwrap();

            // 3. P commits row 7 while R's transaction is open.
            let peer_value = format!("peer-{peer_role}");
            assert!(
                child(
                    peer_role,
                    &db,
                    &format!("UPDATE k SET b = '{peer_value}' WHERE id = 7;"),
                    None
                ),
                "[{peer_role}] P could not commit while R's transaction was open, so this arm \
                 cannot form the jump"
            );

            // 4. R commits past P's commit.
            let jumps_before = jumps.count.load(Ordering::SeqCst);
            conn.execute("COMMIT").await.unwrap();
            let jumps_in_commit = jumps.count.load(Ordering::SeqCst) - jumps_before;
            let jump = (jumps_in_commit > 0)
                .then(|| *jumps.last.lock().unwrap())
                .flatten();
            if jump_required {
                assert_eq!(
                    jumps_in_commit, 1,
                    "[{peer_role}] non-vacuity: R's COMMIT must jump past P's commit \
                     (own_commit_jump events in the COMMIT: {jumps_in_commit})"
                );
                let (first, commit_seq) = jump.unwrap();
                assert!(
                    first > commit_seq + 1,
                    "[{peer_role}] the jump's first sequence {first} must be above \
                     commit_seq + 1 ({commit_seq} + 1)"
                );
            }

            // 5. The held lookups answer what stock reads.
            reset_hot_path_profile();
            let got = vec![lookup(&stmt, 7).await, lookup(&stmt, 3900).await];
            let profile = hot_path_profile_snapshot();
            set_hot_path_profile_enabled(false);
            let want = stock_lookups(&db, &[7, 3900]);
            assert_eq!(
                want,
                vec![format!("T{peer_value}"), "Tr-own".to_owned()],
                "[{peer_role}] stock reads both commits"
            );
            eprintln!(
                "event=bd_kcf1h arm={peer_role} jumps_in_commit={jumps_in_commit} jump={jump:?} \
                 got={got:?} direct_rowid_lookup_query_hits={} memdb_pager_reloads={} \
                 memdb_refresh_count={}",
                profile.direct_rowid_lookup_query_hits,
                profile.memdb_pager_reloads,
                profile.memdb_refresh_count
            );
            assert_eq!(
                got, want,
                "[{peer_role}] after R's commit the held lookups must read P's commit and R's own"
            );
            conn.close().await.unwrap();
        });
    }

    #[test]
    fn held_rowid_lookup_reads_a_peer_commit_its_own_commit_jumped_past_fsqlite_peer() {
        let _serial = serial();
        own_commit_jump_arm("fsqlite", true);
    }

    /// A stock peer's commit is caught up before R records its own: R's
    /// COMMIT reports no jump (observed 2026-11-19 on 5a62e9606), so this arm
    /// checks the answers across the same interleaving without requiring one.
    #[test]
    fn held_rowid_lookup_reads_a_stock_peer_commit_made_inside_its_transaction() {
        let _serial = serial();
        own_commit_jump_arm("stock", false);
    }
}
