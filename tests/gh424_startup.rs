//! GH#424: a cold explicit environment must not block its own executor.
//!
//! Also protect the lifetime contract: fixing startup must not replace the
//! connection's runtime-rooted spawner with a short-lived block_on context.

#![cfg(all(not(target_arch = "wasm32"), feature = "native"))]
#![recursion_limit = "512"]

use std::sync::Arc;
use std::time::{Duration, Instant};

use asupersync::runtime::RuntimeBuilder;
use fsqlite_core::connection::{
    Connection, ConnectionEnv, IoPollStrategy, RuntimeConfig, RuntimeContext,
};
use fsqlite_types::SqliteValue;

#[test]
fn gh424_cold_env_is_prompt_and_native_spawner_survives_block_on() {
    let runtime = RuntimeBuilder::current_thread()
        .blocking_threads(1, 2)
        .build()
        .expect("build the same runtime flavor and blocking pool as the CLI");

    let (mut connection, native_cx) = runtime.block_on(async {
        // Construct INSIDE block_on, just like the CLI. Constructing outside
        // would leave native_runtime_handle empty and bypass the faulty path.
        // Use a fresh environment so a warmed OnceLock cannot hide the wait.
        let env = ConnectionEnv::new(Arc::new(RuntimeContext::new(RuntimeConfig {
            worker_threads: 1,
            io_poll_strategy: IoPollStrategy::Auto,
        })));
        let started = Instant::now();
        let connection = Connection::open_with_env(":memory:", env)
            .await
            .expect("open a cold explicit runtime environment");
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(2),
            "GH#424: cold explicit-environment open stalled for {elapsed:?}"
        );
        assert!(connection.is_concurrent_mode_default());
        let native_cx = connection
            .root_cx()
            .attached_native_cx()
            .expect("retain native runtime authority, not a detached fallback");
        (connection, native_cx)
    });

    // The opening root future has ended. The connection and its native Cx
    // must remain usable in a later entry into the same owning runtime.
    runtime.block_on(async {
        let mut child = native_cx
            .spawn(|child_cx| async move {
                child_cx.checkpoint().expect("child context remains live");
                42_u8
            })
            .expect("native Cx can still spawn after the opening block_on");
        assert_eq!(child.join(&native_cx).await.expect("join child"), 42);
        let rows = connection.query("SELECT 1;").await.expect("query connection");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values(), &[SqliteValue::Integer(1)]);
        connection
            .close_in_place()
            .await
            .expect("await connection shutdown");
    });
}
