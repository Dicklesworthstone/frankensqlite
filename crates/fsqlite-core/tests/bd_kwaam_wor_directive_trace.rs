// Diagnostic integration test for frankensqlite#377 (WITHOUT ROWID half).
//
// Captures the connection's own planner-directive outcome telemetry
// (`vdbe.planner_select_directive`, target "fsqlite.planner_runtime") for an
// equality probe against a WITHOUT ROWID table's persisted UNIQUE autoindex
// and, for contrast, the identical shape on a rowid table.
//
// Runs as its own integration-test binary so the global tracing subscriber
// installed here can never leak into lib-test hot-path lane assertions (see
// the init_publication_test_tracing commentary in connection.rs).

use fsqlite_core::connection::Connection;
use std::sync::{Arc, Mutex};
use tracing_subscriber::prelude::*;

#[derive(Debug, Clone, Default)]
struct CapturedEvent {
    honor_mode: String,
    bypass_reason: String,
    lowered_ops: String,
    access_kind: String,
    table: String,
    index: String,
}

/// Poison-tolerant snapshot: a panicking visitor must not lose the trace.
fn drain(events: &Mutex<Vec<CapturedEvent>>) -> Vec<CapturedEvent> {
    let mut guard = events
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    std::mem::take(&mut *guard)
}

struct FieldCollector {
    events: Arc<Mutex<Vec<CapturedEvent>>>,
}

impl<S> tracing_subscriber::Layer<S> for FieldCollector
where
    S: tracing::Subscriber,
{
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        struct Rec(Vec<(String, String)>);
        impl tracing::field::Visit for Rec {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.0.push((field.name().to_owned(), format!("{value:?}")));
            }
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                self.0.push((field.name().to_owned(), value.to_owned()));
            }
            fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
                self.0.push((field.name().to_owned(), value.to_string()));
            }
            fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
                self.0.push((field.name().to_owned(), value.to_string()));
            }
            fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
                self.0.push((field.name().to_owned(), value.to_string()));
            }
        }
        let mut rec = Rec(Vec::new());
        event.record(&mut rec);
        let mut current = CapturedEvent::default();
        let mut is_directive_event = false;
        for (name, value) in rec.0 {
            match name.as_str() {
                "honor_mode" => {
                    is_directive_event = true;
                    current.honor_mode = value;
                }
                "bypass_reason" => current.bypass_reason = value,
                "lowered_ops" => current.lowered_ops = value,
                "access_kind" => current.access_kind = value,
                "table" => current.table = value,
                "index" => current.index = value,
                _ => {}
            }
        }
        if is_directive_event {
            let mut guard = self.events.lock().unwrap_or_else(|p| p.into_inner());
            guard.push(current);
        }
    }
}

fn run_shape(label: &str, suffix: &str) {
    let events = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(FieldCollector {
        events: Arc::clone(&events),
    });
    let dispatch = tracing::Dispatch::new(subscriber);

    tracing::dispatcher::with_default(&dispatch, || {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute(&format!(
                "CREATE TABLE facts (id TEXT PRIMARY KEY NOT NULL, cap TEXT NOT NULL, \
                 ord INTEGER NOT NULL, UNIQUE(cap, ord)){suffix};"
            ))
            .await
            .unwrap();
            conn.execute("INSERT INTO facts VALUES ('a','c',1);")
                .await
                .unwrap();
            let rows = conn
                .query("SELECT 1 FROM facts WHERE cap = 'c' AND ord = 1")
                .await
                .unwrap();
            println!("[{label}] rows={}", rows.len());
        });
    });

    let captured = drain(&events);
    println!("[{label}] captured {} directive events", captured.len());
    for event in &captured {
        println!(
            "[{label}] access_kind={} table={} index={} honor_mode={} bypass_reason={} lowered_ops={}",
            event.access_kind,
            event.table,
            event.index,
            event.honor_mode,
            event.bypass_reason,
            event.lowered_ops
        );
    }
}

#[test]
fn wor_directive_outcome_trace() {
    run_shape("wor", " WITHOUT ROWID");
    run_shape("rid", "");
}
