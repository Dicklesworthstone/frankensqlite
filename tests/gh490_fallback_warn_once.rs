#![recursion_limit = "512"]

//! GH#490: in the default (non-strict parity-cert) mode, every multi-row
//! `INSERT ... VALUES` into a table with foreign keys or triggers logged a WARN
//! ("using in-memory fallback path while parity-cert mode is enabled", reason
//! `insert_values_row_by_row_trigger_or_fk_fallback`), one per statement:
//! about 1,700 lines in 45 s downstream. That row-by-row replay is the intended
//! path for stock's row-at-a-time trigger / FK order, so it now logs at DEBUG,
//! as do the UPDATE / DELETE replays and the morsel batching of large VALUES
//! lists. Any other in-memory fallback still warns, once per connection and
//! `(statement kind, reason)`; repeats log at DEBUG.
//!
//! Captures the connection's `fsqlite.storage_wiring` fallback events with a
//! tracing layer, over the reporter's shapes (a composite-key FK child table
//! written with `INSERT OR REPLACE ... RETURNING 1` and plain `INSERT`, 100
//! rows per statement) and a table with an AFTER INSERT trigger. Its own test
//! binary, so the subscriber cannot leak into other tests.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;
use std::sync::{Arc, Mutex};
use tracing_subscriber::prelude::*;

const FALLBACK_MESSAGE_PREFIX: &str = "execute_statement_dispatch: using in-memory fallback path";
const ROW_BY_ROW: &str = "insert_values_row_by_row_trigger_or_fk_fallback";

/// One fallback event: whether it was a WARN, its statement kind and reason.
type Captured = (bool, String, String);

struct FallbackCollector {
    events: Arc<Mutex<Vec<Captured>>>,
}

impl<S> tracing_subscriber::Layer<S> for FallbackCollector
where
    S: tracing::Subscriber,
{
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if event.metadata().target() != "fsqlite.storage_wiring" {
            return;
        }
        #[derive(Default)]
        struct Fields {
            message: String,
            statement_kind: String,
            decision_reason: String,
        }
        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                let value = format!("{value:?}");
                self.record(field.name(), value.trim_matches('"').to_owned());
            }
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                self.record(field.name(), value.to_owned());
            }
        }
        impl Fields {
            fn record(&mut self, name: &str, value: String) {
                match name {
                    "message" => self.message = value,
                    "statement_kind" => self.statement_kind = value,
                    "decision_reason" => self.decision_reason = value,
                    _ => {}
                }
            }
        }
        let mut fields = Fields::default();
        event.record(&mut fields);
        if fields.message.starts_with(FALLBACK_MESSAGE_PREFIX) {
            self.events
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push((
                    *event.metadata().level() == tracing::Level::WARN,
                    fields.statement_kind,
                    fields.decision_reason,
                ));
        }
    }
}

fn values_list(start: i64, count: i64) -> String {
    (start..start + count)
        .map(|seq| format!("(1, {seq}, 'v{seq}')"))
        .collect::<Vec<_>>()
        .join(", ")
}

#[test]
fn row_by_row_inserts_do_not_warn_and_other_fallbacks_warn_once() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(FallbackCollector {
        events: Arc::clone(&events),
    });
    let dispatch = tracing::Dispatch::new(subscriber);
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir
        .path()
        .join("gh490.db")
        .to_str()
        .expect("utf-8 path")
        .to_owned();

    tracing::dispatcher::with_default(&dispatch, || {
        asupersync::test_utils::run_test(|| async move {
            let conn = Connection::open(&path).await.expect("open");
            for sql in [
                "PRAGMA foreign_keys = ON",
                "CREATE TABLE runs (run_id INTEGER PRIMARY KEY)",
                "CREATE TABLE events (run_id INTEGER NOT NULL, seq INTEGER NOT NULL, v TEXT, \
                 PRIMARY KEY (run_id, seq), FOREIGN KEY (run_id) REFERENCES runs(run_id))",
                "CREATE INDEX events_v ON events (v)",
                "CREATE TABLE audit (n INTEGER)",
                "CREATE TABLE logged (run_id INTEGER, seq INTEGER, v TEXT)",
                "CREATE TRIGGER logged_ai AFTER INSERT ON logged BEGIN \
                 INSERT INTO audit VALUES (NEW.seq); END",
                "CREATE TABLE w (k PRIMARY KEY, v) WITHOUT ROWID",
                "INSERT INTO runs VALUES (1)",
                "INSERT INTO w VALUES (1, 'a'), (2, 'a'), (3, 'b')",
            ] {
                conn.execute(sql).await.expect("setup");
            }
            // GH#490's workload: one 100-row statement per batch.
            for batch in 0..10 {
                let start = batch * 100;
                let returning = conn
                    .query(&format!(
                        "INSERT OR REPLACE INTO events VALUES {} RETURNING 1",
                        values_list(start, 100)
                    ))
                    .await
                    .expect("fk insert or replace returning");
                assert_eq!(returning.len(), 100);
                conn.execute(&format!(
                    "INSERT INTO events VALUES {}",
                    values_list(10_000 + start, 100)
                ))
                .await
                .expect("fk insert");
                conn.execute(&format!(
                    "INSERT INTO logged VALUES {}",
                    values_list(start, 100)
                ))
                .await
                .expect("trigger insert");
            }
            // An unexpected fallback, repeated: reported once.
            for _ in 0..5 {
                conn.query(
                    "SELECT k FROM w WHERE EXISTS \
                     (SELECT 1 FROM w AS w2 WHERE w2.v = w.v AND w2.k < w.k)",
                )
                .await
                .expect("correlated exists");
            }
            for (sql, expected) in [
                ("SELECT count(*) FROM events", 2000),
                ("SELECT count(*) FROM logged", 1000),
                ("SELECT count(*) FROM audit", 1000),
            ] {
                let rows = conn.query(sql).await.expect("count");
                assert_eq!(
                    rows[0].values()[0],
                    SqliteValue::Integer(expected),
                    "`{sql}`: every row must still land"
                );
            }
            conn.close().await.expect("close");
        });
    });

    let events = events
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    assert!(
        events.iter().any(|(_, _, reason)| reason == ROW_BY_ROW),
        "the multi-row FK / trigger inserts must take the row-by-row replay this test is about: \
         {events:?}"
    );
    let warnings: Vec<&Captured> = events.iter().filter(|(warn, _, _)| *warn).collect();
    assert!(
        warnings.iter().all(|(_, _, reason)| reason != ROW_BY_ROW),
        "the row-by-row replay must not warn: {warnings:?}"
    );
    let mut seen = std::collections::HashSet::new();
    for warning in &warnings {
        assert!(
            seen.insert((&warning.1, &warning.2)),
            "fallback {warning:?} warned more than once on one connection: {warnings:?}"
        );
    }
}
