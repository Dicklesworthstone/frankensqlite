//! GH #424: opening a connection through an explicit `RuntimeContext` that
//! captured the calling runtime's handle must not stall.
//!
//! The CLI builds a `current_thread` runtime, constructs its `RuntimeContext`
//! inside `block_on`, and opens on that thread. The io_uring spawner mint
//! (bd-fo6xw) used to block the executor thread on a 5-second synchronous
//! `recv_timeout` while waiting for a task that only that thread could run,
//! so every CLI start paid the full timeout and then silently fell back.
#![cfg(all(feature = "native", not(target_arch = "wasm32")))]

use std::sync::Arc;
use std::time::{Duration, Instant};

use fsqlite::compat::RowExt;
use fsqlite::{Connection, ConnectionEnv, RuntimeConfig, RuntimeContext};

/// Mirrors `fsqlite-cli`'s `shell_runtime_builder`.
fn cli_shaped_runtime() -> asupersync::runtime::Runtime {
    asupersync::runtime::RuntimeBuilder::current_thread()
        .blocking_threads(1, 2)
        .build()
        .expect("current_thread runtime")
}

#[test]
fn open_with_captured_runtime_context_does_not_block_the_executor_thread() {
    let runtime = cli_shaped_runtime();
    runtime.block_on(async {
        // Constructed inside `block_on`, so the context captures this
        // runtime's handle — the exact shape the CLI uses.
        let context = Arc::new(RuntimeContext::new(RuntimeConfig::default()));
        let env = ConnectionEnv::new(Arc::clone(&context));

        let started = Instant::now();
        let connection = Connection::open_with_env(":memory:", env)
            .await
            .expect("open with captured runtime context");
        let elapsed = started.elapsed();

        // The regression cost exactly 5 s per open on every platform. A
        // healthy open is milliseconds; the bound leaves room for a loaded
        // host without admitting the timeout.
        assert!(
            elapsed < Duration::from_secs(2),
            "open stalled for {elapsed:?}: the io_native_cx handoff is blocking the executor"
        );

        // And the mint actually succeeded rather than being skipped: the
        // captured handle produced a shared io_uring spawner.
        let rendered = format!("{context:?}");
        assert!(
            rendered.contains("has_native_runtime_handle: true"),
            "context built inside block_on must capture the runtime handle: {rendered}"
        );
        assert!(
            rendered.contains("has_io_native_cx: true"),
            "io native cx must be minted on the awaited path: {rendered}"
        );

        let row = connection.query_row("SELECT 1").await.expect("query");
        assert_eq!(row.get_typed::<i64>(0).expect("integer"), 1);

        // A second open on the same context reuses the cached mint.
        let again = Instant::now();
        let second = Connection::open_with_env(":memory:", ConnectionEnv::new(context))
            .await
            .expect("second open");
        assert!(again.elapsed() < Duration::from_secs(2));
        drop(second);
        drop(connection);
    });
}
