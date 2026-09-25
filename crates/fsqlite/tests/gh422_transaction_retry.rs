//! Public-API regressions for opt-in whole-transaction retry (GH #422).
//!
//! SQL conflict tests exercise two actual engine connections. Rollback fault
//! injection lives beside the production cleanup loop in transaction/retry.rs.
#![cfg(all(feature = "native", not(target_arch = "wasm32")))]
#![recursion_limit = "512"]

use std::cell::Cell;
use std::future::{Future, pending, poll_fn};
use std::task::Poll;
use std::time::Duration;

use fsqlite::compat::{RetryPolicy, RetryStopReason, RowExt, TransactionExt, TransactionRetryExt};
use fsqlite::{Connection, FrankenError, SqliteValue};

#[test]
fn two_writers_append_once_and_the_loser_rereads_a_fresh_snapshot() {
    asupersync::test_utils::run_test(|| async {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("retry.db");
        let path = path.to_str().unwrap();
        let winner = Connection::open(path).await.unwrap();
        winner.execute("PRAGMA journal_mode = WAL").await.unwrap();
        winner
            .execute("CREATE TABLE counter(id INTEGER PRIMARY KEY, value INTEGER)")
            .await
            .unwrap();
        winner
            .execute("INSERT INTO counter VALUES (1, 0)")
            .await
            .unwrap();
        winner
            .execute("CREATE TABLE events(id INTEGER PRIMARY KEY, label TEXT)")
            .await
            .unwrap();
        let loser = Connection::open(path).await.unwrap();
        winner.execute("PRAGMA busy_timeout = 0").await.unwrap();
        loser.execute("PRAGMA busy_timeout = 0").await.unwrap();

        let mut observations = Vec::new();
        let committed_value = loser
            .transaction_with_retry(RetryPolicy::default(), async |tx| {
                let value: i64 = tx
                    .query_row("SELECT value FROM counter WHERE id = 1")
                    .await?
                    .get_typed(0)?;
                observations.push(value);
                if observations.len() == 1 {
                    // Both transactions have observed the old counter. The winner
                    // commits while the loser's original snapshot is still live.
                    // A same-row update forces a real conflict even if disjoint
                    // appends can later be merged by a finer-grained write path.
                    let mut first = winner.transaction().await?;
                    first
                        .execute("UPDATE counter SET value = value + 1 WHERE id = 1")
                        .await?;
                    first
                        .execute("INSERT INTO events VALUES (1, 'winner')")
                        .await?;
                    first.commit().await?;
                }
                tx.execute_with_params(
                    "UPDATE counter SET value = ?1 WHERE id = 1",
                    &[SqliteValue::Integer(value + 1)],
                )
                .await?;
                tx.execute("INSERT INTO events VALUES (2, 'loser')").await?;
                Ok(value + 1)
            })
            .await
            .unwrap();

        assert_eq!(
            observations,
            [0, 1],
            "the loser must rerun its reads, not just COMMIT"
        );
        assert_eq!(committed_value, 2);
        assert!(!winner.in_transaction());
        assert!(!loser.in_transaction());
        let rows = loser
            .query("SELECT label FROM events ORDER BY id")
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get_typed::<String>(0).unwrap(), "winner");
        assert_eq!(rows[1].get_typed::<String>(0).unwrap(), "loser");
        assert_eq!(
            loser
                .query_row("SELECT value FROM counter WHERE id = 1")
                .await
                .unwrap()
                .get_typed::<i64>(0)
                .unwrap(),
            2
        );
        assert_eq!(
            loser
                .query_row("PRAGMA integrity_check")
                .await
                .unwrap()
                .get_typed::<String>(0)
                .unwrap(),
            "ok"
        );
        loser.close().await.unwrap();
        winner.close().await.unwrap();

        // Check the physical image independently, after both engine handles
        // have closed. Both acknowledged appends must survive reopen.
        let oracle = rusqlite::Connection::open(path).unwrap();
        let count: i64 = oracle
            .query_row("SELECT count(*) FROM events", [], |row| row.get(0))
            .unwrap();
        let counter: i64 = oracle
            .query_row("SELECT value FROM counter WHERE id = 1", [], |row| {
                row.get(0)
            })
            .unwrap();
        let integrity: String = oracle
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 2);
        assert_eq!(counter, 2);
        assert_eq!(integrity, "ok");
    });
}

#[test]
fn corruption_is_not_retried_and_partial_writes_are_rolled_back() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(value INTEGER)").await.unwrap();
        let calls = Cell::new(0);
        let failure = conn
            .transaction_with_retry(RetryPolicy::default(), async |tx| {
                calls.set(calls.get() + 1);
                tx.execute("INSERT INTO t VALUES (1)").await?;
                Err::<(), _>(FrankenError::DatabaseCorrupt {
                    detail: "injected nontransient error".into(),
                })
            })
            .await
            .unwrap_err();
        assert_eq!(failure.reason, RetryStopReason::NonTransient);
        assert_eq!(failure.attempts, 1);
        assert_eq!(calls.get(), 1);
        assert!(matches!(
            failure.last_error.as_deref(),
            Some(FrankenError::DatabaseCorrupt { .. })
        ));
        assert!(!failure.transaction_open);
        assert!(conn.query("SELECT * FROM t").await.unwrap().is_empty());
        assert!(std::error::Error::source(&failure).is_some());
    });
}

#[test]
fn a_real_constraint_failure_does_not_replay_the_callback() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
            .await
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1)").await.unwrap();
        let calls = Cell::new(0);
        let failure = conn
            .transaction_with_retry(RetryPolicy::default(), async |tx| {
                calls.set(calls.get() + 1);
                tx.execute("INSERT INTO t VALUES (2)").await?;
                tx.execute("INSERT INTO t VALUES (1)").await?;
                Ok(())
            })
            .await
            .unwrap_err();
        assert_eq!(failure.reason, RetryStopReason::NonTransient);
        assert_eq!(calls.get(), 1);
        assert!(!failure.last_error.as_deref().unwrap().is_transient());
        let rows = conn.query("SELECT id FROM t ORDER BY id").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get_typed::<i64>(0).unwrap(), 1);
    });
}

#[test]
fn invalid_limits_and_an_expired_budget_do_not_begin_or_call_user_code() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        let normal = RetryPolicy::default();
        let cases = [
            (
                RetryPolicy {
                    max_attempts: 0,
                    ..normal
                },
                RetryStopReason::InvalidPolicy,
            ),
            (
                RetryPolicy {
                    max_rollback_attempts: 0,
                    ..normal
                },
                RetryStopReason::InvalidPolicy,
            ),
            (
                RetryPolicy {
                    max_backoff: Duration::ZERO,
                    ..normal
                },
                RetryStopReason::InvalidPolicy,
            ),
            (
                RetryPolicy {
                    timeout: Duration::ZERO,
                    ..normal
                },
                RetryStopReason::DeadlineExceeded,
            ),
        ];
        for (policy, expected) in cases {
            let calls = Cell::new(0);
            let failure = conn
                .transaction_with_retry(policy, async |_| {
                    calls.set(calls.get() + 1);
                    Ok(())
                })
                .await
                .unwrap_err();
            assert_eq!(failure.reason, expected);
            assert_eq!(failure.attempts, 0);
            assert_eq!(calls.get(), 0);
            assert!(!conn.in_transaction());
        }
    });
}

#[test]
fn rejecting_a_nested_call_does_not_rollback_the_callers_transaction() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(value INTEGER)").await.unwrap();
        let mut owner = conn.transaction().await.unwrap();
        owner.execute("INSERT INTO t VALUES (1)").await.unwrap();
        let failure = conn
            .transaction_with_retry(RetryPolicy::default(), async |_| Ok(()))
            .await
            .unwrap_err();
        assert_eq!(failure.reason, RetryStopReason::AlreadyInTransaction);
        assert_eq!(failure.attempts, 0);
        assert!(failure.transaction_open);
        // A mistakenly armed cleanup guard would discard this write before
        // COMMIT or make the owner's wrapper report NoActiveTransaction.
        owner.commit().await.unwrap();
        assert_eq!(conn.query("SELECT * FROM t").await.unwrap().len(), 1);
    });
}

#[test]
fn callback_transaction_control_is_refused_before_any_commit() {
    asupersync::test_utils::run_test(|| async {
        for control in ["COMMIT", "END TRANSACTION", "ROLLBACK", "BEGIN"] {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(value INTEGER)").await.unwrap();
            let calls = Cell::new(0);
            let failure = conn
                .transaction_with_retry(RetryPolicy::default(), async |tx| {
                    calls.set(calls.get() + 1);
                    tx.execute("INSERT INTO t VALUES (1)").await?;
                    tx.execute(control).await?;
                    Err::<(), _>(FrankenError::Busy)
                })
                .await
                .unwrap_err();
            assert_eq!(failure.reason, RetryStopReason::NonTransient);
            assert_eq!(failure.attempts, 1);
            assert_eq!(calls.get(), 1);
            assert!(matches!(
                failure.last_error.as_deref(),
                Some(FrankenError::FunctionError(_))
            ));
            assert!(!conn.in_transaction());
            assert!(conn.query("SELECT * FROM t").await.unwrap().is_empty());
        }
    });
}

#[test]
fn callback_batches_cannot_commit_prefixes_or_hide_a_replacement_transaction() {
    asupersync::test_utils::run_test(|| async {
        for batch in [
            "INSERT INTO t VALUES (2); COMMIT; BEGIN; INSERT INTO t VALUES (3)",
            "INSERT INTO t VALUES (2); END TRANSACTION; BEGIN; SELECT 1",
            "INSERT INTO t VALUES (2); ROLLBACK; BEGIN; INSERT INTO t VALUES (3)",
            "INSERT INTO t VALUES (2); COMMIT; SELECT * FROM nonexistent",
        ] {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(value INTEGER)").await.unwrap();
            let calls = Cell::new(0);
            let failure = conn
                .transaction_with_retry(RetryPolicy::default(), async |tx| {
                    calls.set(calls.get() + 1);
                    tx.execute("INSERT INTO t VALUES (1)").await?;
                    tx.execute_batch(batch).await?;
                    Err::<(), _>(FrankenError::Busy)
                })
                .await
                .unwrap_err();
            assert_eq!(failure.reason, RetryStopReason::NonTransient, "{batch}");
            assert_eq!(calls.get(), 1);
            assert_eq!(failure.attempts, 1);
            assert!(!failure.transaction_open);
            assert!(conn.query("SELECT * FROM t").await.unwrap().is_empty());
        }
    });
}

#[test]
fn catching_refused_control_cannot_leak_a_batch_prefix_across_retries() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(value INTEGER)").await.unwrap();
        let mut calls = 0;
        conn.transaction_with_retry(RetryPolicy::default(), async |tx| {
            calls += 1;
            tx.execute("INSERT INTO t VALUES (1)").await?;
            let refused = tx
                .execute_batch("INSERT INTO t VALUES (2); COMMIT; BEGIN")
                .await;
            assert!(matches!(refused, Err(FrankenError::FunctionError(_))));
            if calls == 1 {
                return Err(FrankenError::Busy);
            }
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(calls, 2);
        let rows = conn.query("SELECT value FROM t").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0), Some(&SqliteValue::Integer(1)));
    });
}

#[test]
fn dropping_a_pending_body_rolls_back_before_the_next_sql_statement() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(value INTEGER)").await.unwrap();
        let written = Cell::new(false);
        {
            let mut operation = Box::pin(conn.transaction_with_retry(
                RetryPolicy::default(),
                async |tx| {
                    tx.execute("INSERT INTO t VALUES (1)").await?;
                    written.set(true);
                    pending::<Result<(), FrankenError>>().await
                },
            ));
            poll_fn(|cx| {
                assert!(operation.as_mut().poll(cx).is_pending());
                if written.get() {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
            assert!(conn.in_transaction());
            // Dropping the actual future (not just its Pin reference) must
            // install the ordinary Transaction deferred-cleanup obligation.
        }
        assert!(conn.query("SELECT * FROM t").await.unwrap().is_empty());
        assert!(!conn.in_transaction());
    });
}

#[test]
fn dropping_an_unpolled_helper_does_not_start_a_transaction() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        let called = Cell::new(false);
        let operation = conn.transaction_with_retry(RetryPolicy::default(), async |_| {
            called.set(true);
            Ok(())
        });
        drop(operation);
        assert!(!called.get());
        assert!(!conn.in_transaction());
        conn.execute("CREATE TABLE still_usable(id INTEGER)")
            .await
            .unwrap();
    });
}

#[test]
fn a_body_finishing_after_the_deadline_is_rolled_back_not_committed() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(value INTEGER)").await.unwrap();
        let policy = RetryPolicy {
            timeout: Duration::from_millis(100),
            ..RetryPolicy::default()
        };
        let failure = conn
            .transaction_with_retry(policy, async |tx| {
                tx.execute("INSERT INTO t VALUES (1)").await?;
                let cx = asupersync::Cx::current().unwrap();
                asupersync::time::sleep(cx.now(), Duration::from_millis(200)).await;
                Ok(())
            })
            .await
            .unwrap_err();
        assert_eq!(failure.reason, RetryStopReason::DeadlineExceeded);
        assert_eq!(failure.attempts, 1);
        assert!(failure.elapsed >= policy.timeout);
        assert!(!failure.transaction_open);
        assert!(conn.query("SELECT * FROM t").await.unwrap().is_empty());
    });
}

#[test]
fn the_final_failure_does_not_wait_for_another_backoff() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        let policy = RetryPolicy {
            max_attempts: 1,
            initial_backoff: Duration::from_secs(3600),
            max_backoff: Duration::from_secs(3600),
            ..RetryPolicy::default()
        };
        let failure = conn
            .transaction_with_retry(policy, async |_| Err::<(), _>(FrankenError::Busy))
            .await
            .unwrap_err();
        assert_eq!(failure.reason, RetryStopReason::AttemptsExhausted);
        assert_eq!(failure.attempts, 1);
        assert!(!failure.transaction_open);
    });
}
