#![cfg(feature = "async-api")]

//! bd-wymdl diagnostic (defect 4a): measure how deep trigger recursion can go
//! on the worker-API path, where the engine runs on the dedicated
//! `WORKER_STACK_BYTES` (16 MiB) thread owned by `AsyncConnection`.
//!
//! A stack overflow aborts the process, so the sweep drives this test
//! out-of-process, one depth per run:
//!
//! ```text
//! FSQLITE_PROBE_DEPTH=200 cargo test -p fsqlite --features async-api \
//!     --test trigger_depth_worker_probe -- --ignored --nocapture
//! ```

use asupersync::runtime::RuntimeBuilder;
use fsqlite::AsyncConnection;
use fsqlite_types::cx::Cx;

#[test]
#[ignore = "diagnostic measurement, not a regression assertion"]
fn diag_worker_trigger_depth_survival() {
    let depth: usize = std::env::var("FSQLITE_PROBE_DEPTH")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(8);

    let runtime = RuntimeBuilder::current_thread()
        .blocking_threads(1, 1)
        .build()
        .expect("test runtime should build");

    runtime.block_on(async {
        let cx = Cx::new();
        let connection = AsyncConnection::open(&cx, ":memory:".to_owned())
            .await
            .expect("in-memory async connection should open");

        for statement in [
            "PRAGMA recursive_triggers = ON;".to_owned(),
            "CREATE TABLE a (n INTEGER);".to_owned(),
            "CREATE TABLE b (n INTEGER);".to_owned(),
            "INSERT INTO a VALUES (0);".to_owned(),
            "INSERT INTO b VALUES (0);".to_owned(),
            format!(
                "CREATE TRIGGER trg_a AFTER UPDATE ON a WHEN NEW.n < {depth} \
                 BEGIN UPDATE b SET n = NEW.n + 1; END;"
            ),
            format!(
                "CREATE TRIGGER trg_b AFTER UPDATE ON b WHEN NEW.n < {depth} \
                 BEGIN UPDATE a SET n = NEW.n + 1; END;"
            ),
        ] {
            connection
                .execute(&cx, &statement)
                .await
                .unwrap_or_else(|error| panic!("setup statement failed: {statement}: {error}"));
        }

        let result = connection.execute(&cx, "UPDATE a SET n = 1;").await;
        match result {
            Ok(_) => println!("PROBE_SURVIVED worker depth={depth}"),
            Err(error) => println!("PROBE_ERROR worker depth={depth} error={error}"),
        }
    });
}
