//! Interop integrity tests: FrankenSQLite-authored databases must pass stock SQLite
//! `PRAGMA integrity_check`. Tests cover the patterns reported in issue #54 and
//! additional edge cases that exercise B-tree splitting, overflow pages, freeblock
//! accounting, and WAL checkpoint behavior.
//!
//! Every test creates a database with FrankenSQLite, closes the connection, then
//! reopens with rusqlite (stock SQLite) and asserts `PRAGMA integrity_check = "ok"`.
#![recursion_limit = "512"]

use tempfile::tempdir;

fn assert_stock_sqlite_integrity(db_path: &std::path::Path, label: &str) {
    let conn = rusqlite::Connection::open(db_path)
        .unwrap_or_else(|e| panic!("[{label}] stock SQLite failed to open: {e}"));
    let integrity: String = conn
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .unwrap_or_else(|e| panic!("[{label}] integrity_check query failed: {e}"));
    assert_eq!(integrity, "ok", "[{label}] integrity_check = {integrity}");
}

/// Issue #54 exact reproduction: execute_batch with CREATE + 2 INSERTs, drop without close.
#[test]
fn issue54_execute_batch_drop_without_close() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempdir().unwrap();
        let path = dir.path().join("issue54.db");
        {
            let conn = fsqlite::Connection::open(path.to_str().unwrap())
                .await
                .unwrap();
            conn.execute_batch(
                "CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT);
             INSERT INTO items (id, label) VALUES (1, 'alpha');
             INSERT INTO items (id, label) VALUES (2, 'bravo');",
            )
            .await
            .unwrap();
            // Drop without close — matches reporter's exact pattern
        }
        assert_stock_sqlite_integrity(&path, "issue54_drop");

        let conn = rusqlite::Connection::open(&path).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM items", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 2);
    });
}

/// Same as issue #54 but with explicit close.
#[test]
fn issue54_execute_batch_explicit_close() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempdir().unwrap();
        let path = dir.path().join("issue54_close.db");
        {
            let conn = fsqlite::Connection::open(path.to_str().unwrap())
                .await
                .unwrap();
            conn.execute_batch(
                "CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT);
             INSERT INTO items (id, label) VALUES (1, 'alpha');
             INSERT INTO items (id, label) VALUES (2, 'bravo');",
            )
            .await
            .unwrap();
            conn.close().await.unwrap();
        }
        assert_stock_sqlite_integrity(&path, "issue54_close");
    });
}

/// DELETE + reinsert exercises freeblock accounting and page reuse.
#[test]
fn delete_reinsert_freeblock_accounting() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempdir().unwrap();
        let path = dir.path().join("delete_reinsert.db");
        {
            let conn = fsqlite::Connection::open(path.to_str().unwrap())
                .await
                .unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx_val ON t(val)")
                .await
                .unwrap();
            for i in 0..100 {
                conn.execute(&format!("INSERT INTO t VALUES ({i}, 'row_{i}')"))
                    .await
                    .unwrap();
            }
            conn.execute("DELETE FROM t WHERE id BETWEEN 20 AND 60")
                .await
                .unwrap();
            for i in 200..250 {
                conn.execute(&format!("INSERT INTO t VALUES ({i}, 'new_{i}')"))
                    .await
                    .unwrap();
            }
            // Drop without close
        }
        assert_stock_sqlite_integrity(&path, "delete_reinsert");
    });
}

/// Delete all rows then reinsert — exercises freelist page recycling.
#[test]
fn delete_all_reinsert_freelist() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempdir().unwrap();
        let path = dir.path().join("delete_all.db");
        {
            let conn = fsqlite::Connection::open(path.to_str().unwrap())
                .await
                .unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx_val ON t(val)")
                .await
                .unwrap();
            for i in 0..100 {
                conn.execute(&format!("INSERT INTO t VALUES ({i}, 'row_{i}')"))
                    .await
                    .unwrap();
            }
            conn.execute("DELETE FROM t").await.unwrap();
            for i in 200..250 {
                conn.execute(&format!("INSERT INTO t VALUES ({i}, 'new_{i}')"))
                    .await
                    .unwrap();
            }
        }
        assert_stock_sqlite_integrity(&path, "delete_all_reinsert");
    });
}

/// Multiple indexes exercise B-tree page balancing across index trees.
#[test]
fn multiple_indexes_balance() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempdir().unwrap();
        let path = dir.path().join("multi_idx.db");
        {
            let conn = fsqlite::Connection::open(path.to_str().unwrap())
                .await
                .unwrap();
            conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT, c REAL, d INTEGER)")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx_b ON t(b)").await.unwrap();
            conn.execute("CREATE INDEX idx_c ON t(c)").await.unwrap();
            conn.execute("CREATE INDEX idx_d ON t(d)").await.unwrap();
            conn.execute("CREATE INDEX idx_bc ON t(b, c)")
                .await
                .unwrap();
            for i in 0..200 {
                conn.execute(&format!(
                    "INSERT INTO t VALUES ({i}, 'val_{i}', {i}.5, {})",
                    i % 10
                ))
                .await
                .unwrap();
            }
        }
        assert_stock_sqlite_integrity(&path, "multiple_indexes");
    });
}

/// Large blobs trigger overflow page chains.
#[test]
fn overflow_pages() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempdir().unwrap();
        let path = dir.path().join("overflow.db");
        {
            let conn = fsqlite::Connection::open(path.to_str().unwrap())
                .await
                .unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB)")
                .await
                .unwrap();
            for i in 0..10 {
                let blob_hex: String = (0..8192)
                    .map(|j| format!("{:02x}", ((i * 7 + j) % 256) as u8))
                    .collect();
                conn.execute(&format!("INSERT INTO t VALUES ({i}, X'{blob_hex}')"))
                    .await
                    .unwrap();
            }
        }
        assert_stock_sqlite_integrity(&path, "overflow_pages");
    });
}

/// UPDATE with indexed column exercises index entry removal + reinsertion.
#[test]
fn updates_with_index() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempdir().unwrap();
        let path = dir.path().join("updates.db");
        {
            let conn = fsqlite::Connection::open(path.to_str().unwrap())
                .await
                .unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx_val ON t(val)")
                .await
                .unwrap();
            for i in 0..100 {
                conn.execute(&format!("INSERT INTO t VALUES ({i}, 'original_{i}')"))
                    .await
                    .unwrap();
            }
            for i in (0..100).step_by(2) {
                conn.execute(&format!(
                    "UPDATE t SET val = 'updated_value_longer_string_{i}' WHERE id = {i}"
                ))
                .await
                .unwrap();
            }
        }
        assert_stock_sqlite_integrity(&path, "updates_with_index");
    });
}

/// 2000 rows with indexes triggers multiple levels of B-tree splitting.
#[test]
fn large_table_btree_splitting() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempdir().unwrap();
        let path = dir.path().join("large.db");
        {
            let conn = fsqlite::Connection::open(path.to_str().unwrap())
                .await
                .unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT, c INTEGER)")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx_a ON t(a)").await.unwrap();
            conn.execute("CREATE INDEX idx_c ON t(c)").await.unwrap();
            for i in 0..2000 {
                conn.execute(&format!(
                    "INSERT INTO t VALUES ({i}, 'name_{i}_padding_to_make_longer', \
                 'description_{i}_with_some_more_text_here', {})",
                    i % 100
                ))
                .await
                .unwrap();
            }
        }
        assert_stock_sqlite_integrity(&path, "large_table_2000_rows");
    });
}

/// Composite primary key (WITHOUT ROWID-like layout for the PK index).
#[test]
fn composite_primary_key() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempdir().unwrap();
        let path = dir.path().join("composite_pk.db");
        {
            let conn = fsqlite::Connection::open(path.to_str().unwrap())
                .await
                .unwrap();
            conn.execute(
                "CREATE TABLE edges(src INTEGER, dst INTEGER, weight REAL, PRIMARY KEY(src, dst))",
            )
            .await
            .unwrap();
            for i in 0..30 {
                for j in 0..5 {
                    conn.execute(&format!("INSERT INTO edges VALUES ({i}, {j}, {i}.{j})"))
                        .await
                        .unwrap();
                }
            }
        }
        assert_stock_sqlite_integrity(&path, "composite_pk");
    });
}

/// UNIQUE constraint exercises unique index maintenance.
#[test]
fn unique_constraint_delete_reinsert() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempdir().unwrap();
        let path = dir.path().join("unique.db");
        {
            let conn = fsqlite::Connection::open(path.to_str().unwrap())
                .await
                .unwrap();
            conn.execute(
                "CREATE TABLE users(id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT)",
            )
            .await
            .unwrap();
            for i in 0..50 {
                conn.execute(&format!(
                    "INSERT INTO users VALUES ({i}, 'user{i}@example.com', 'User {i}')"
                ))
                .await
                .unwrap();
            }
            conn.execute("DELETE FROM users WHERE id BETWEEN 20 AND 30")
                .await
                .unwrap();
            for i in 100..111 {
                conn.execute(&format!(
                    "INSERT INTO users VALUES ({i}, 'new{i}@example.com', 'New User {i}')"
                ))
                .await
                .unwrap();
            }
        }
        assert_stock_sqlite_integrity(&path, "unique_constraint");
    });
}

/// Transaction rollback then commit exercises journal/wal replay paths.
#[test]
fn transaction_rollback_then_commit() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempdir().unwrap();
        let path = dir.path().join("txn.db");
        {
            let conn = fsqlite::Connection::open(path.to_str().unwrap())
                .await
                .unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .unwrap();
            for i in 0..20 {
                conn.execute(&format!("INSERT INTO t VALUES ({i}, 'row_{i}')"))
                    .await
                    .unwrap();
            }
            conn.execute("BEGIN").await.unwrap();
            conn.execute("DELETE FROM t WHERE id > 10").await.unwrap();
            conn.execute("ROLLBACK").await.unwrap();
            conn.execute("BEGIN").await.unwrap();
            for i in 20..40 {
                conn.execute(&format!("INSERT INTO t VALUES ({i}, 'row_{i}')"))
                    .await
                    .unwrap();
            }
            conn.execute("COMMIT").await.unwrap();
        }
        assert_stock_sqlite_integrity(&path, "txn_rollback_commit");
    });
}

/// WAL journal mode exercises WAL checkpoint on close.
#[test]
fn wal_mode_checkpoint() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.db");
        {
            let conn = fsqlite::Connection::open(path.to_str().unwrap())
                .await
                .unwrap();
            let _ = conn.execute("PRAGMA journal_mode=WAL").await;
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx_val ON t(val)")
                .await
                .unwrap();
            for i in 0..100 {
                conn.execute(&format!("INSERT INTO t VALUES ({i}, 'row_{i}')"))
                    .await
                    .unwrap();
            }
        }
        assert_stock_sqlite_integrity(&path, "wal_mode");
    });
}

/// Issue #55: Multi-table schema with AUTOINCREMENT, composite UNIQUE,
/// foreign keys, and explicit indexes. Single-table tests pass, but this
/// combination produces "database disk image is malformed" when reopened
/// by stock SQLite.
#[test]
fn issue55_multi_table_autoincrement_composite_unique_fk() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempdir().unwrap();
        let path = dir.path().join("issue55.db");
        {
            let conn = fsqlite::Connection::open(path.to_str().unwrap())
                .await
                .unwrap();

            // Create multi-table schema from the issue report.
            conn.execute(
                "CREATE TABLE groups(
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT NOT NULL,
                source_ref TEXT,
                notes TEXT,
                created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                updated_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            )",
            )
            .await
            .unwrap();
            conn.execute("CREATE INDEX idx_groups_source_ref ON groups (source_ref)")
                .await
                .unwrap();

            conn.execute(
                "CREATE TABLE records(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id TEXT NOT NULL,
                seq INTEGER NOT NULL,
                value_a REAL,
                value_b REAL,
                value_c REAL,
                value_d REAL,
                value_e REAL,
                value_f REAL,
                payload TEXT NOT NULL DEFAULT 'null',
                UNIQUE (group_id, seq),
                FOREIGN KEY(group_id) REFERENCES groups(id) ON DELETE CASCADE
            )",
            )
            .await
            .unwrap();
            conn.execute("CREATE INDEX idx_records_group_seq ON records (group_id, seq)")
                .await
                .unwrap();

            conn.execute(
                "CREATE TABLE config_snapshots (
                config_hash TEXT PRIMARY KEY NOT NULL,
                schema_version INTEGER NOT NULL,
                config_json TEXT NOT NULL
            )",
            )
            .await
            .unwrap();

            conn.execute(
                "CREATE TABLE group_config_links (
                group_id TEXT PRIMARY KEY NOT NULL REFERENCES groups (id) ON DELETE CASCADE,
                config_hash TEXT NOT NULL REFERENCES config_snapshots (config_hash),
                linked_at INTEGER NOT NULL
            )",
            )
            .await
            .unwrap();
            conn.execute("CREATE INDEX idx_config_links_hash ON group_config_links (config_hash)")
                .await
                .unwrap();

            conn.execute(
                "CREATE TABLE events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id TEXT NOT NULL REFERENCES groups (id) ON DELETE CASCADE,
                seq INTEGER NOT NULL,
                severity TEXT NOT NULL,
                message TEXT NOT NULL,
                raw_payload TEXT NOT NULL DEFAULT 'null'
            )",
            )
            .await
            .unwrap();
            conn.execute("CREATE INDEX idx_events_group_seq ON events (group_id, seq)")
                .await
                .unwrap();

            // Insert data across tables.
            conn.execute(
                "INSERT INTO groups (id, name, category) VALUES ('g1', 'Group One', 'cat_a')",
            )
            .await
            .unwrap();
            conn.execute(
                "INSERT INTO groups (id, name, category) VALUES ('g2', 'Group Two', 'cat_b')",
            )
            .await
            .unwrap();

            conn.execute(
                "INSERT INTO records (group_id, seq, value_a, payload) VALUES ('g1', 1, 3.14, '{\"x\":1}')",
            )
            .await
            .unwrap();
            conn.execute(
                "INSERT INTO records (group_id, seq, value_a, payload) VALUES ('g1', 2, 2.72, '{\"x\":2}')",
            )
            .await
            .unwrap();
            conn.execute(
                "INSERT INTO records (group_id, seq, value_a, payload) VALUES ('g2', 1, 1.41, '{\"x\":3}')",
            )
            .await
            .unwrap();

            conn.execute(
                "INSERT INTO config_snapshots VALUES ('hash_abc', 1, '{\"setting\":\"value\"}')",
            )
            .await
            .unwrap();
            conn.execute("INSERT INTO group_config_links VALUES ('g1', 'hash_abc', 1711900000)")
                .await
                .unwrap();

            conn.execute(
                "INSERT INTO events (group_id, seq, severity, message) VALUES ('g1', 1, 'INFO', 'started')",
            )
            .await
            .unwrap();
            conn.execute(
                "INSERT INTO events (group_id, seq, severity, message) VALUES ('g1', 2, 'WARN', 'threshold')",
            )
            .await
            .unwrap();
        }
        assert_stock_sqlite_integrity(&path, "issue55_multi_table");

        // Also verify data is readable via stock SQLite.
        let sqlite = rusqlite::Connection::open(&path).unwrap();
        let group_count: i64 = sqlite
            .query_row("SELECT COUNT(*) FROM groups", [], |r| r.get(0))
            .unwrap();
        assert_eq!(group_count, 2, "expected 2 groups");
        let record_count: i64 = sqlite
            .query_row("SELECT COUNT(*) FROM records", [], |r| r.get(0))
            .unwrap();
        assert_eq!(record_count, 3, "expected 3 records");
        let event_count: i64 = sqlite
            .query_row("SELECT COUNT(*) FROM events", [], |r| r.get(0))
            .unwrap();
        assert_eq!(event_count, 2, "expected 2 events");
    });
}

/// Minimal multi-table reproduction: two tables, one with AUTOINCREMENT and
/// a UNIQUE constraint. This isolates whether the issue is autoindex + sqlite_sequence.
#[test]
fn multi_table_autoincrement_with_unique_constraint() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempdir().unwrap();
        let path = dir.path().join("multi_autoinc.db");
        {
            let conn = fsqlite::Connection::open(path.to_str().unwrap())
                .await
                .unwrap();
            conn.execute("CREATE TABLE parent(id TEXT PRIMARY KEY, label TEXT NOT NULL)")
                .await
                .unwrap();
            conn.execute(
                "CREATE TABLE child(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                parent_id TEXT NOT NULL,
                seq INTEGER NOT NULL,
                data TEXT,
                UNIQUE(parent_id, seq)
            )",
            )
            .await
            .unwrap();
            conn.execute("INSERT INTO parent VALUES ('p1', 'Parent One')")
                .await
                .unwrap();
            conn.execute("INSERT INTO child (parent_id, seq, data) VALUES ('p1', 1, 'first')")
                .await
                .unwrap();
            conn.execute("INSERT INTO child (parent_id, seq, data) VALUES ('p1', 2, 'second')")
                .await
                .unwrap();
        }
        assert_stock_sqlite_integrity(&path, "multi_table_autoinc_unique");
    });
}

/// Multi-table with explicit indexes on each table but no AUTOINCREMENT.
#[test]
fn multi_table_explicit_indexes_no_autoincrement() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempdir().unwrap();
        let path = dir.path().join("multi_idx_no_autoinc.db");
        {
            let conn = fsqlite::Connection::open(path.to_str().unwrap())
                .await
                .unwrap();
            conn.execute("CREATE TABLE t1(id TEXT PRIMARY KEY, a TEXT, b INTEGER)")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx_t1_a ON t1(a)")
                .await
                .unwrap();
            conn.execute("CREATE TABLE t2(id TEXT PRIMARY KEY, t1_id TEXT, c REAL)")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx_t2_t1 ON t2(t1_id)")
                .await
                .unwrap();
            for i in 0..20 {
                conn.execute(&format!("INSERT INTO t1 VALUES ('k{i}', 'val_{i}', {i})"))
                    .await
                    .unwrap();
                conn.execute(&format!("INSERT INTO t2 VALUES ('j{i}', 'k{i}', {i}.5)"))
                    .await
                    .unwrap();
            }
        }
        assert_stock_sqlite_integrity(&path, "multi_table_explicit_indexes");
    });
}

/// Issue #55 exact reproduction schema from the bug report: five tables with
/// AUTOINCREMENT + composite UNIQUE + foreign keys + explicit CREATE INDEX.
/// Stock SQLite reports "unlinked pages" and "entry count mismatches" for
/// sqlite_autoindex_records_1.
#[test]
fn issue55_exact_reproduction_schema() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempdir().unwrap();
        let path = dir.path().join("issue55_exact.db");
        {
            let conn = fsqlite::Connection::open(path.to_str().unwrap())
                .await
                .unwrap();
            conn.execute(
                "CREATE TABLE groups (\
                id INTEGER PRIMARY KEY AUTOINCREMENT, \
                name TEXT NOT NULL UNIQUE, \
                created_at TEXT NOT NULL DEFAULT (datetime('now')))",
            )
            .await
            .unwrap();
            conn.execute(
                "CREATE TABLE records (\
                id INTEGER PRIMARY KEY AUTOINCREMENT, \
                group_id INTEGER NOT NULL REFERENCES groups(id), \
                key TEXT NOT NULL, \
                value BLOB, \
                updated_at TEXT NOT NULL DEFAULT (datetime('now')), \
                UNIQUE(group_id, key))",
            )
            .await
            .unwrap();
            conn.execute(
                "CREATE TABLE config_snapshots (\
                id INTEGER PRIMARY KEY AUTOINCREMENT, \
                group_id INTEGER NOT NULL REFERENCES groups(id), \
                snapshot BLOB NOT NULL, \
                taken_at TEXT NOT NULL DEFAULT (datetime('now')))",
            )
            .await
            .unwrap();
            conn.execute(
                "CREATE TABLE group_config_links (\
                group_id INTEGER NOT NULL REFERENCES groups(id), \
                config_id INTEGER NOT NULL REFERENCES config_snapshots(id), \
                PRIMARY KEY (group_id, config_id))",
            )
            .await
            .unwrap();
            conn.execute(
                "CREATE TABLE events (\
                id INTEGER PRIMARY KEY AUTOINCREMENT, \
                record_id INTEGER REFERENCES records(id), \
                event_type TEXT NOT NULL, \
                payload TEXT, \
                created_at TEXT NOT NULL DEFAULT (datetime('now')))",
            )
            .await
            .unwrap();
            conn.execute("CREATE INDEX idx_records_group ON records(group_id)")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx_records_key ON records(key)")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx_events_record ON events(record_id)")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx_events_type ON events(event_type)")
                .await
                .unwrap();

            // Insert data across all tables.
            conn.execute("INSERT INTO groups (name, created_at) VALUES ('alpha', '2024-01-01')")
                .await
                .unwrap();
            conn.execute("INSERT INTO groups (name, created_at) VALUES ('beta', '2024-01-02')")
                .await
                .unwrap();

            conn.execute(
                "INSERT INTO records (group_id, key, value, updated_at) VALUES (1, 'k1', X'DEADBEEF', '2024-01-01')",
            )
            .await
            .unwrap();
            conn.execute(
                "INSERT INTO records (group_id, key, value, updated_at) VALUES (1, 'k2', X'CAFEBABE', '2024-01-01')",
            )
            .await
            .unwrap();
            conn.execute(
                "INSERT INTO records (group_id, key, value, updated_at) VALUES (2, 'k1', X'01020304', '2024-01-02')",
            )
            .await
            .unwrap();

            conn.execute(
                "INSERT INTO config_snapshots (group_id, snapshot, taken_at) VALUES (1, X'AABB', '2024-01-01')",
            )
            .await
            .unwrap();
            conn.execute("INSERT INTO group_config_links VALUES (1, 1)")
                .await
                .unwrap();

            conn.execute(
                "INSERT INTO events (record_id, event_type, payload, created_at) VALUES (1, 'create', '{\"a\":1}', '2024-01-01')",
            )
            .await
            .unwrap();
            conn.execute(
                "INSERT INTO events (record_id, event_type, payload, created_at) VALUES (2, 'update', '{\"b\":2}', '2024-01-01')",
            )
            .await
            .unwrap();
            conn.execute(
                "INSERT INTO events (record_id, event_type, payload, created_at) VALUES (3, 'create', '{\"c\":3}', '2024-01-02')",
            )
            .await
            .unwrap();
        }
        assert_stock_sqlite_integrity(&path, "issue55_exact_reproduction");

        // Verify data roundtrip via stock SQLite.
        let sqlite = rusqlite::Connection::open(&path).unwrap();
        let group_count: i64 = sqlite
            .query_row("SELECT COUNT(*) FROM groups", [], |r| r.get(0))
            .unwrap();
        assert_eq!(group_count, 2, "expected 2 groups");
        let record_count: i64 = sqlite
            .query_row("SELECT COUNT(*) FROM records", [], |r| r.get(0))
            .unwrap();
        assert_eq!(record_count, 3, "expected 3 records");
        let event_count: i64 = sqlite
            .query_row("SELECT COUNT(*) FROM events", [], |r| r.get(0))
            .unwrap();
        assert_eq!(event_count, 3, "expected 3 events");
        let config_count: i64 = sqlite
            .query_row("SELECT COUNT(*) FROM config_snapshots", [], |r| r.get(0))
            .unwrap();
        assert_eq!(config_count, 1, "expected 1 config snapshot");
    });
}
