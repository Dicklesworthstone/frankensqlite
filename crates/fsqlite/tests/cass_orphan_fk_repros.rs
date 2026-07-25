//! Regression tests pinning the fix for a correlated `NOT EXISTS` planner
//! bug that was discovered downstream in `cass`
//! (`coding_agent_session_search`).
//!
//! Background
//! ----------
//! `cass` runs an "orphan FK row" repair pass at indexer startup
//! (`storage::sqlite::cleanup_orphan_fk_rows`). The probe for orphan
//! `message_metrics` rows was originally written as:
//!
//! ```sql
//! SELECT message_id FROM message_metrics
//! WHERE NOT EXISTS (
//!     SELECT 1 FROM messages WHERE messages.id = message_metrics.message_id
//! )
//! ```
//!
//! On fsqlite `c8ce64fd` (the revision cass currently pins) this returned
//! **every** row in `message_metrics` rather than only those whose
//! `message_id` has no matching `messages.id`. The correlation predicate
//! (`messages.id = message_metrics.message_id`) was not being evaluated as
//! a correlation — every outer row's `NOT EXISTS` evaluated true.
//!
//! The downstream test failure was
//! `cleanup_orphan_fk_rows_handles_more_than_one_delete_chunk`:
//! `assertion left == right failed: left: 518, right: 259` — the per-table
//! orphan count for `message_metrics` was being inflated from 0 up to N.
//!
//! cass landed a workaround in commit `1f20bd57` (`fix(storage): avoid
//! fragile frankensqlite query shapes`) that rewrites the probe as the
//! non-correlated form:
//!
//! ```sql
//! SELECT message_id FROM message_metrics
//! WHERE message_id NOT IN (SELECT id FROM messages)
//! ```
//!
//! Bisect summary
//! --------------
//! - fsqlite `c8ce64fd` — bug present, both probe shapes diverge.
//! - fsqlite `HEAD` (when this test landed) — both probe shapes agree.
//!
//! Once `cass` bumps its fsqlite pin to a revision that includes the fix,
//! the workaround in `cass::src/storage/sqlite.rs` (`ORPHAN_DIRECT_CHILD_TABLES`)
//! can be reverted to the natural `NOT EXISTS` shape.
//!
//! This file exists to prevent the bug from regressing into fsqlite again.
//! If any `correlated_not_exists_*` test starts failing, the planner has
//! regressed and downstream cass tests will break.

use fsqlite::{Connection, Row};
use fsqlite_types::SqliteValue;
use tempfile::TempDir;

// ---- Fixture: cass orphan-cleanup schema replica ---------------------------

/// Build a schema slice that matches cass's `conversations` / `messages` /
/// `message_metrics` triple, including FK constraints and `UNIQUE`
/// constraints. Opens a persistent file with WAL journal mode and
/// FK enforcement on — the same configuration `FrankenStorage::open`
/// applies in cass.
///
/// Plants `real_rows` valid messages (conversation_id = 1, which exists)
/// and `orphan_rows` orphan messages (conversation_id = 99999, which does
/// not exist), then inserts one `message_metrics` row per message. None of
/// the metric rows are themselves orphans — they all reference an existing
/// `messages.id`.
async fn cass_orphan_schema(real_rows: i64, orphan_rows: i64) -> (Connection, TempDir) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("orphan_fk_repro.db");
    let conn = Connection::open(path.to_str().unwrap()).await.unwrap();
    conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
    conn.execute("PRAGMA foreign_keys = OFF;").await.unwrap();

    conn.execute(
        "CREATE TABLE conversations (
            id INTEGER PRIMARY KEY,
            agent_id INTEGER NOT NULL,
            source_path TEXT NOT NULL
        );",
    )
    .await
    .unwrap();
    conn.execute(
        "CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            conversation_id INTEGER NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
            idx INTEGER NOT NULL,
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            UNIQUE(conversation_id, idx)
        );",
    )
    .await
    .unwrap();
    conn.execute(
        "CREATE TABLE message_metrics (
            message_id INTEGER PRIMARY KEY REFERENCES messages(id) ON DELETE CASCADE,
            created_at_ms INTEGER NOT NULL,
            hour_id INTEGER NOT NULL,
            day_id INTEGER NOT NULL,
            agent_slug TEXT NOT NULL,
            role TEXT NOT NULL,
            content_chars INTEGER NOT NULL,
            content_tokens_est INTEGER NOT NULL
        );",
    )
    .await
    .unwrap();

    conn.execute("INSERT INTO conversations(id, agent_id, source_path) VALUES (1, 1, '/real');")
        .await
        .unwrap();

    for i in 1..=real_rows {
        conn.query_with_params(
            "INSERT INTO messages(id, conversation_id, idx, role, content) \
             VALUES (?1, 1, ?2, 'user', ?3);",
            &[
                SqliteValue::Integer(i),
                SqliteValue::Integer(i),
                SqliteValue::Text(format!("real-msg-{i}").into()),
            ],
        )
        .await
        .unwrap();
    }
    let orphan_start = real_rows + 1;
    let orphan_end = real_rows + orphan_rows;
    for (off, i) in (orphan_start..=orphan_end).enumerate() {
        conn.query_with_params(
            "INSERT INTO messages(id, conversation_id, idx, role, content) \
             VALUES (?1, 99999, ?2, 'user', ?3);",
            &[
                SqliteValue::Integer(i),
                SqliteValue::Integer(off as i64),
                SqliteValue::Text(format!("orphan-msg-{i}").into()),
            ],
        )
        .await
        .unwrap();
    }
    for i in 1..=orphan_end {
        conn.query_with_params(
            "INSERT INTO message_metrics(
                message_id, created_at_ms, hour_id, day_id,
                agent_slug, role, content_chars, content_tokens_est
            ) VALUES (?1, 0, 0, 0, 'test-agent', 'user', 13, 2);",
            &[SqliteValue::Integer(i)],
        )
        .await
        .unwrap();
    }
    conn.execute("PRAGMA foreign_keys = ON;").await.unwrap();
    (conn, dir)
}

fn int_col(rows: &[Row], col: usize) -> Vec<i64> {
    rows.iter()
        .map(|r| match &r.values()[col] {
            SqliteValue::Integer(n) => *n,
            other => panic!("expected Integer, got {other:?}"),
        })
        .collect()
}

const NOT_EXISTS_PROBE_SQL: &str = "SELECT message_id FROM message_metrics \
     WHERE NOT EXISTS (SELECT 1 FROM messages WHERE messages.id = message_metrics.message_id) \
     ORDER BY message_id";

const NOT_IN_PROBE_SQL: &str = "SELECT message_id FROM message_metrics \
     WHERE message_id NOT IN (SELECT id FROM messages) \
     ORDER BY message_id";

// ---- Baselines: non-correlated `NOT IN` shape must always agree ------------

#[test]
fn baseline_not_in_finds_zero_orphans_when_every_metric_has_matching_message() {
    asupersync::test_utils::run_test(|| async {
        let (conn, _dir) = cass_orphan_schema(1, 3).await;
        let rows = conn.query(NOT_IN_PROBE_SQL).await.unwrap();
        assert_eq!(int_col(&rows, 0), Vec::<i64>::new());
    });
}

#[test]
fn baseline_not_in_finds_only_real_orphan_when_one_metric_is_dangling() {
    asupersync::test_utils::run_test(|| async {
        let (conn, _dir) = cass_orphan_schema(5, 0).await;
        conn.execute("PRAGMA foreign_keys = OFF;").await.unwrap();
        conn.query_with_params(
            "INSERT INTO message_metrics(
            message_id, created_at_ms, hour_id, day_id,
            agent_slug, role, content_chars, content_tokens_est
        ) VALUES (?1, 0, 0, 0, 'test-agent', 'user', 13, 2);",
            &[SqliteValue::Integer(9999)],
        )
        .await
        .unwrap();
        conn.execute("PRAGMA foreign_keys = ON;").await.unwrap();
        let rows = conn.query(NOT_IN_PROBE_SQL).await.unwrap();
        assert_eq!(int_col(&rows, 0), vec![9999_i64]);
    });
}

// ---- Regression tests: correlated `NOT EXISTS` must match `NOT IN` ---------

/// **Regression pin** — correlated `NOT EXISTS` must return zero rows when
/// every `message_metrics.message_id` has a matching `messages.id`.
///
/// Failed on fsqlite `c8ce64fd` with `left: [1, 2, 3, 4], right: []`.
/// Once any cass-side grep for `ORPHAN_DIRECT_CHILD_TABLES` shows the
/// `NOT EXISTS` shape, the workaround has been (or can be) reverted.
#[test]
fn correlated_not_exists_must_match_not_in_at_small_scale() {
    asupersync::test_utils::run_test(|| async {
        let (conn, _dir) = cass_orphan_schema(1, 3).await;
        let rows = conn.query(NOT_EXISTS_PROBE_SQL).await.unwrap();
        assert_eq!(
            int_col(&rows, 0),
            Vec::<i64>::new(),
            "correlated NOT EXISTS regressed — it is leaking every metric row \
         instead of evaluating the correlation predicate."
        );
    });
}

/// **Regression pin at the cass test scale** — the downstream cass test
/// `cleanup_orphan_fk_rows_handles_more_than_one_delete_chunk` uses
/// `ORPHAN_FK_ID_CHUNK_SIZE + 3 = 259` rows and failed with `left: 518,
/// right: 259` because the per-table orphan count for `message_metrics` was
/// being inflated from 0 to N.
#[test]
fn correlated_not_exists_must_match_not_in_at_cass_test_scale() {
    asupersync::test_utils::run_test(|| async {
        let n = 259_i64;
        let (conn, _dir) = cass_orphan_schema(1, n - 1).await;

        let rows_not_in = conn.query(NOT_IN_PROBE_SQL).await.unwrap();
        let rows_not_exists = conn.query(NOT_EXISTS_PROBE_SQL).await.unwrap();

        assert_eq!(
            int_col(&rows_not_in, 0),
            Vec::<i64>::new(),
            "baseline NOT IN must find no orphans (every metric has a matching message)"
        );
        assert_eq!(
            int_col(&rows_not_exists, 0),
            Vec::<i64>::new(),
            "correlated NOT EXISTS regressed at scale — downstream cass test \
         `cleanup_orphan_fk_rows_handles_more_than_one_delete_chunk` will fail \
         with `left: <2N>, right: <N>`."
        );
    });
}

/// **Regression pin** — correlated `NOT EXISTS` must isolate exactly the
/// genuinely-orphan row when one is planted.
#[test]
fn correlated_not_exists_must_return_only_real_orphans() {
    asupersync::test_utils::run_test(|| async {
        let (conn, _dir) = cass_orphan_schema(5, 0).await;
        conn.execute("PRAGMA foreign_keys = OFF;").await.unwrap();
        conn.query_with_params(
            "INSERT INTO message_metrics(
            message_id, created_at_ms, hour_id, day_id,
            agent_slug, role, content_chars, content_tokens_est
        ) VALUES (?1, 0, 0, 0, 'test-agent', 'user', 13, 2);",
            &[SqliteValue::Integer(9999)],
        )
        .await
        .unwrap();
        conn.execute("PRAGMA foreign_keys = ON;").await.unwrap();
        let rows = conn.query(NOT_EXISTS_PROBE_SQL).await.unwrap();
        assert_eq!(
            int_col(&rows, 0),
            vec![9999_i64],
            "correlated NOT EXISTS must isolate exactly the dangling row, not return everything."
        );
    });
}

// ---- Chunked-IN DELETE — never broken, kept as a shape regression pin ------

fn positional_placeholders(n: usize) -> String {
    assert!(n > 0);
    let mut s = String::with_capacity(n * 2 - 1);
    for i in 0..n {
        if i > 0 {
            s.push(',');
        }
        s.push('?');
    }
    s
}

/// **Shape regression pin** — `DELETE FROM t WHERE id IN (?, ?, ?, ?, ?)`
/// with positional placeholders must delete exactly the bound IDs. cass's
/// pass-20 commit additionally rewrote this shape into a per-id loop as a
/// defensive measure; this test confirms the chunked-IN form has always been
/// correct in fsqlite (both on the cass-pinned rev and on HEAD).
#[test]
fn dynamic_in_delete_with_positional_placeholders_deletes_exactly_bound_ids() {
    asupersync::test_utils::run_test(|| async {
        let (conn, _dir) = cass_orphan_schema(10, 0).await;
        let to_delete: Vec<i64> = vec![1, 3, 5, 7, 9];
        let sql = format!(
            "DELETE FROM message_metrics WHERE message_id IN ({})",
            positional_placeholders(to_delete.len())
        );
        let params: Vec<SqliteValue> = to_delete
            .iter()
            .copied()
            .map(SqliteValue::Integer)
            .collect();
        let _ = conn.query_with_params(&sql, &params).await.unwrap();

        let remaining = conn
            .query("SELECT message_id FROM message_metrics ORDER BY message_id")
            .await
            .unwrap();
        assert_eq!(int_col(&remaining, 0), vec![2_i64, 4, 6, 8, 10]);
    });
}

/// Same shape at the chunk size cass used (256 placeholders).
#[test]
fn dynamic_in_delete_at_chunk_size_256() {
    asupersync::test_utils::run_test(|| async {
        let n_rows: i64 = 300;
        let (conn, _dir) = cass_orphan_schema(n_rows, 0).await;

        let to_delete: Vec<i64> = (1..=256).collect();
        let sql = format!(
            "DELETE FROM message_metrics WHERE message_id IN ({})",
            positional_placeholders(to_delete.len())
        );
        let params: Vec<SqliteValue> = to_delete
            .iter()
            .copied()
            .map(SqliteValue::Integer)
            .collect();
        let _ = conn.query_with_params(&sql, &params).await.unwrap();

        let remaining = conn
            .query("SELECT COUNT(*) FROM message_metrics")
            .await
            .unwrap();
        let count = match &remaining[0].values()[0] {
            SqliteValue::Integer(n) => *n,
            other => panic!("expected count, got {other:?}"),
        };
        assert_eq!(count, n_rows - 256);
    });
}
