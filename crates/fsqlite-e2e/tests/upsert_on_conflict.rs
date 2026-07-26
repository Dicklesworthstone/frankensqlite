//! UPSERT (INSERT ... ON CONFLICT) E2E tests (bd-nyami).
//!
//! Verifies that `INSERT ... ON CONFLICT DO NOTHING` and
//! `INSERT ... ON CONFLICT DO UPDATE SET ...` work correctly,
//! including the `excluded` pseudo-table for referencing
//! the attempted insert values.
//!
//! SQLite 3.24+ reference: <https://www.sqlite.org/lang_upsert.html>

use std::ops::Deref;

use fsqlite::Connection;
use tempfile::{TempDir, tempdir};

struct TestConnection {
    conn: Connection,
    _temp: TempDir,
}

impl Deref for TestConnection {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

async fn open_db(name: &str) -> TestConnection {
    let temp = tempdir().expect("tempdir");
    let db_path = temp.path().join(name);
    let conn = Connection::open(db_path.to_string_lossy().to_string())
        .await
        .expect("open connection");
    TestConnection { conn, _temp: temp }
}

/// Helper: extract column value as text (without surrounding quotes).
fn col(rows: &[fsqlite::Row], row: usize, col: usize) -> String {
    rows[row]
        .get(col)
        .map(fsqlite_types::SqliteValue::to_text)
        .unwrap_or_default()
}

// ─── ON CONFLICT DO NOTHING ────────────────────────────────────────────

#[test]
fn upsert_do_nothing_skips_duplicate_pk() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_db("upsert-do-nothing-pk.db").await;
        conn.execute("CREATE TABLE kv (k INTEGER PRIMARY KEY, v TEXT);")
            .await
            .expect("create");
        conn.execute("INSERT INTO kv VALUES (1, 'first');")
            .await
            .expect("first insert");
        // This should silently skip — not error.
        conn.execute("INSERT INTO kv VALUES (1, 'second') ON CONFLICT DO NOTHING;")
            .await
            .expect("upsert DO NOTHING should not error");
        let rows = conn
            .query("SELECT v FROM kv WHERE k = 1;")
            .await
            .expect("query");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            col(&rows, 0, 0),
            "first",
            "original value should be preserved"
        );
    });
}

#[test]
fn upsert_do_nothing_skips_duplicate_unique() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_db("upsert-do-nothing-unique.db").await;
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT UNIQUE, val INTEGER);")
            .await
            .expect("create");
        conn.execute("INSERT INTO t VALUES (1, 'alice', 100);")
            .await
            .expect("first insert");
        // Conflict on UNIQUE(name), should skip.
        conn.execute("INSERT INTO t VALUES (2, 'alice', 200) ON CONFLICT DO NOTHING;")
            .await
            .expect("upsert DO NOTHING on unique");
        let rows = conn
            .query("SELECT val FROM t WHERE name = 'alice';")
            .await
            .expect("query");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            col(&rows, 0, 0),
            "100",
            "original value should be preserved"
        );
    });
}

#[test]
fn upsert_do_nothing_inserts_when_no_conflict() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_db("upsert-do-nothing-insert.db").await;
        conn.execute("CREATE TABLE kv (k INTEGER PRIMARY KEY, v TEXT);")
            .await
            .expect("create");
        conn.execute("INSERT INTO kv VALUES (1, 'first');")
            .await
            .expect("first insert");
        // No conflict on k=2, should insert normally.
        conn.execute("INSERT INTO kv VALUES (2, 'second') ON CONFLICT DO NOTHING;")
            .await
            .expect("insert with no conflict");
        let rows = conn
            .query("SELECT v FROM kv ORDER BY k;")
            .await
            .expect("query");
        assert_eq!(rows.len(), 2);
        assert_eq!(col(&rows, 0, 0), "first");
        assert_eq!(col(&rows, 1, 0), "second");
    });
}

#[test]
fn upsert_do_nothing_with_target_column() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_db("upsert-do-nothing-target.db").await;
        conn.execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT);")
            .await
            .expect("create");
        conn.execute("INSERT INTO t VALUES (1, 'one');")
            .await
            .expect("first insert");
        // ON CONFLICT(a) DO NOTHING — same semantics, explicit target.
        conn.execute("INSERT INTO t VALUES (1, 'two') ON CONFLICT(a) DO NOTHING;")
            .await
            .expect("upsert DO NOTHING with target");
        let rows = conn
            .query("SELECT b FROM t WHERE a = 1;")
            .await
            .expect("query");
        assert_eq!(col(&rows, 0, 0), "one");
    });
}

// ─── ON CONFLICT DO UPDATE (basic) ─────────────────────────────────────

#[test]
fn upsert_do_update_with_excluded() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_db("upsert-do-update.db").await;
        conn.execute("CREATE TABLE kv (k INTEGER PRIMARY KEY, v TEXT);")
            .await
            .expect("create");
        conn.execute("INSERT INTO kv VALUES (1, 'first');")
            .await
            .expect("first insert");
        // ON CONFLICT DO UPDATE: should update v to the "excluded" value.
        conn.execute(
            "INSERT INTO kv VALUES (1, 'second') ON CONFLICT(k) DO UPDATE SET v = excluded.v;",
        )
        .await
        .expect("upsert DO UPDATE");
        let rows = conn
            .query("SELECT v FROM kv WHERE k = 1;")
            .await
            .expect("query");
        assert_eq!(rows.len(), 1);
        assert_eq!(col(&rows, 0, 0), "second", "value should be updated");
    });
}

#[test]
fn upsert_do_update_inserts_when_no_conflict() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_db("upsert-do-update-insert.db").await;
        conn.execute("CREATE TABLE kv (k INTEGER PRIMARY KEY, v TEXT);")
            .await
            .expect("create");
        conn.execute("INSERT INTO kv VALUES (1, 'first');")
            .await
            .expect("first insert");
        // No conflict on k=2, should insert normally despite ON CONFLICT clause.
        conn.execute(
            "INSERT INTO kv VALUES (2, 'second') ON CONFLICT(k) DO UPDATE SET v = excluded.v;",
        )
        .await
        .expect("insert with no conflict");
        let rows = conn
            .query("SELECT v FROM kv ORDER BY k;")
            .await
            .expect("query");
        assert_eq!(rows.len(), 2);
        assert_eq!(col(&rows, 0, 0), "first");
        assert_eq!(col(&rows, 1, 0), "second");
    });
}

#[test]
fn upsert_do_update_expression_with_existing_value() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_db("upsert-do-update-expr.db").await;
        conn.execute("CREATE TABLE counters (name TEXT PRIMARY KEY, count INTEGER);")
            .await
            .expect("create");
        conn.execute("INSERT INTO counters VALUES ('hits', 10);")
            .await
            .expect("first insert");
        // Increment: count = count + 1
        conn.execute(
            "INSERT INTO counters VALUES ('hits', 1) ON CONFLICT(name) DO UPDATE SET count = count + 1;",
        )
        .await
        .expect("upsert increment");
        let rows = conn
            .query("SELECT count FROM counters WHERE name = 'hits';")
            .await
            .expect("query");
        assert_eq!(col(&rows, 0, 0), "11", "count should be incremented");
    });
}

#[test]
fn upsert_do_update_mixed_excluded_and_existing() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_db("upsert-do-update-mixed.db").await;
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b INTEGER);")
            .await
            .expect("create");
        conn.execute("INSERT INTO t VALUES (1, 'old', 10);")
            .await
            .expect("first insert");
        // Update a from excluded, but add excluded.b to existing b.
        conn.execute(
            "INSERT INTO t VALUES (1, 'new', 5) ON CONFLICT(id) DO UPDATE SET a = excluded.a, b = b + excluded.b;",
        )
        .await
        .expect("upsert mixed");
        let rows = conn
            .query("SELECT a, b FROM t WHERE id = 1;")
            .await
            .expect("query");
        assert_eq!(col(&rows, 0, 0), "new", "a should come from excluded");
        assert_eq!(col(&rows, 0, 1), "15", "b should be old + excluded (10+5)");
    });
}

// ─── ON CONFLICT DO UPDATE with WHERE ──────────────────────────────────

#[test]
fn upsert_do_update_where_true() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_db("upsert-where-true.db").await;
        conn.execute("CREATE TABLE kv (k INTEGER PRIMARY KEY, v INTEGER);")
            .await
            .expect("create");
        conn.execute("INSERT INTO kv VALUES (1, 100);")
            .await
            .expect("first");
        // WHERE v < 200 is true, so update should proceed.
        conn.execute(
            "INSERT INTO kv VALUES (1, 999) ON CONFLICT(k) DO UPDATE SET v = excluded.v WHERE v < 200;",
        )
        .await
        .expect("upsert where true");
        let rows = conn
            .query("SELECT v FROM kv WHERE k = 1;")
            .await
            .expect("query");
        assert_eq!(col(&rows, 0, 0), "999");
    });
}

#[test]
fn upsert_do_update_where_false_skips() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_db("upsert-where-false.db").await;
        conn.execute("CREATE TABLE kv (k INTEGER PRIMARY KEY, v INTEGER);")
            .await
            .expect("create");
        conn.execute("INSERT INTO kv VALUES (1, 100);")
            .await
            .expect("first");
        // WHERE v > 200 is false, so update should be skipped (row preserved).
        conn.execute(
            "INSERT INTO kv VALUES (1, 999) ON CONFLICT(k) DO UPDATE SET v = excluded.v WHERE v > 200;",
        )
        .await
        .expect("upsert where false");
        let rows = conn
            .query("SELECT v FROM kv WHERE k = 1;")
            .await
            .expect("query");
        assert_eq!(col(&rows, 0, 0), "100", "update should be skipped");
    });
}

// ─── Multiple Rows ─────────────────────────────────────────────────────

#[test]
fn upsert_do_nothing_multiple_rows() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_db("upsert-multi-nothing.db").await;
        conn.execute("CREATE TABLE kv (k INTEGER PRIMARY KEY, v TEXT);")
            .await
            .expect("create");
        conn.execute("INSERT INTO kv VALUES (1, 'one'), (2, 'two');")
            .await
            .expect("initial");
        // Insert 3 rows, 2 conflict — should insert only k=3.
        conn.execute(
            "INSERT INTO kv VALUES (1, 'x'), (2, 'y'), (3, 'three') ON CONFLICT DO NOTHING;",
        )
        .await
        .expect("multi upsert");
        let rows = conn
            .query("SELECT k, v FROM kv ORDER BY k;")
            .await
            .expect("query");
        assert_eq!(rows.len(), 3);
        assert_eq!(col(&rows, 0, 1), "one", "k=1 preserved");
        assert_eq!(col(&rows, 1, 1), "two", "k=2 preserved");
        assert_eq!(col(&rows, 2, 1), "three", "k=3 inserted");
    });
}

#[test]
fn upsert_do_update_multiple_rows() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_db("upsert-multi-update.db").await;
        conn.execute("CREATE TABLE kv (k INTEGER PRIMARY KEY, v TEXT);")
            .await
            .expect("create");
        conn.execute("INSERT INTO kv VALUES (1, 'one');")
            .await
            .expect("initial");
        // Insert 2 rows: k=1 conflicts → update, k=2 inserts new.
        conn.execute(
            "INSERT INTO kv VALUES (1, 'ONE'), (2, 'two') ON CONFLICT(k) DO UPDATE SET v = excluded.v;",
        )
        .await
        .expect("multi upsert update");
        let rows = conn
            .query("SELECT k, v FROM kv ORDER BY k;")
            .await
            .expect("query");
        assert_eq!(rows.len(), 2);
        assert_eq!(col(&rows, 0, 1), "ONE", "k=1 updated");
        assert_eq!(col(&rows, 1, 1), "two", "k=2 inserted");
    });
}

// ─── UNIQUE Index (non-PK) ─────────────────────────────────────────────

#[test]
fn upsert_do_update_on_unique_index() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_db("upsert-unique-idx.db").await;
        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT);")
            .await
            .expect("create");
        conn.execute("INSERT INTO users VALUES (1, 'alice@test.com', 'Alice');")
            .await
            .expect("first insert");
        // Conflict on UNIQUE(email), should update name.
        conn.execute(
            "INSERT INTO users VALUES (2, 'alice@test.com', 'Alice Updated') ON CONFLICT(email) DO UPDATE SET name = excluded.name;",
        )
        .await
        .expect("upsert on unique index");
        let rows = conn
            .query("SELECT id, name FROM users WHERE email = 'alice@test.com';")
            .await
            .expect("query");
        assert_eq!(rows.len(), 1);
        assert_eq!(col(&rows, 0, 0), "1", "id should be original");
        assert_eq!(col(&rows, 0, 1), "Alice Updated", "name should be updated");
    });
}

// ─── INSERT OR REPLACE vs ON CONFLICT comparison ───────────────────────

#[test]
fn insert_or_replace_replaces_entire_row() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_db("upsert-vs-replace.db").await;
        conn.execute("CREATE TABLE kv (k INTEGER PRIMARY KEY, v TEXT, extra INTEGER DEFAULT 42);")
            .await
            .expect("create");
        conn.execute("INSERT INTO kv (k, v) VALUES (1, 'first');")
            .await
            .expect("first insert");
        // INSERT OR REPLACE replaces the ENTIRE row — extra resets to default.
        conn.execute("INSERT OR REPLACE INTO kv (k, v) VALUES (1, 'replaced');")
            .await
            .expect("replace");
        let rows = conn
            .query("SELECT v, extra FROM kv WHERE k = 1;")
            .await
            .expect("query");
        assert_eq!(col(&rows, 0, 0), "replaced");
        // With REPLACE, extra should be the default 42 since the whole row is replaced.
        assert_eq!(col(&rows, 0, 1), "42");
    });
}

// ─── Non-target UNIQUE conflict during DO UPDATE must abort ─────────────

#[test]
fn upsert_do_update_aborts_on_nontarget_unique_conflict() {
    asupersync::test_utils::run_test(|| async {
        // Regression for the targeted-upsert "non-target conflict swallow" bug
        // (hfdt-fsqlite-upsert-conflict-target-a756a). `ON CONFLICT(<target>) DO UPDATE`
        // resolves a conflict only on the named target index. When the inserted row
        // instead violates a DIFFERENT unique index, stock SQLite ABORTs with a
        // constraint error — it must never silently swallow the write as a no-op.
        let conn = open_db("upsert-nontarget-unique.db").await;
        conn.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, label TEXT NOT NULL UNIQUE, v TEXT);",
        )
        .await
        .expect("create");
        conn.execute("INSERT INTO t VALUES (1, 'alpha', 'one');")
            .await
            .expect("seed row 1");
        conn.execute("INSERT INTO t VALUES (2, 'beta', 'two');")
            .await
            .expect("seed row 2");

        // id = 99 is a NEW pk (no conflict on the ON CONFLICT(id) target), but
        // label = 'beta' already exists on the secondary UNIQUE index. This MUST
        // abort, not be swallowed.
        let result = conn
            .execute(
                "INSERT INTO t VALUES (99, 'beta', 'ninety-nine') ON CONFLICT(id) DO UPDATE SET v = excluded.v;",
            )
            .await;
        assert!(
            result.is_err(),
            "non-target UNIQUE(label) conflict during DO UPDATE must abort, got {result:?}",
        );

        // Nothing should have been inserted or mutated.
        let count = conn.query("SELECT COUNT(*) FROM t;").await.expect("count");
        assert_eq!(
            col(&count, 0, 0),
            "2",
            "no new row may be inserted by an aborted upsert"
        );
        let beta = conn
            .query("SELECT id, v FROM t WHERE label = 'beta';")
            .await
            .expect("query beta");
        assert_eq!(col(&beta, 0, 0), "2", "existing beta row id unchanged");
        assert_eq!(col(&beta, 0, 1), "two", "existing beta row value unchanged");

        // Positive control: a genuine target conflict still performs the DO UPDATE.
        conn.execute(
            "INSERT INTO t VALUES (1, 'alpha', 'one-updated') ON CONFLICT(id) DO UPDATE SET v = excluded.v;",
        )
        .await
        .expect("target-conflict upsert should still update");
        let alpha = conn
            .query("SELECT v FROM t WHERE id = 1;")
            .await
            .expect("query alpha");
        assert_eq!(
            col(&alpha, 0, 0),
            "one-updated",
            "target conflict still updates"
        );
    });
}
