use fsqlite_core::connection::{Connection, ConnectionEnv, IoPollStrategy, RuntimeConfig};
use fsqlite_types::SqliteValue;
use fsqlite_types::cx::Cx;

fn bridge_runtime_config() -> RuntimeConfig {
    RuntimeConfig {
        worker_threads: 1,
        io_poll_strategy: IoPollStrategy::Auto,
    }
}

#[test]
fn explicit_root_cx_env_drives_file_backed_production_path() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir should be created");
        let db_path = dir.path().join("bd-2lt76-1-bridge.db");
        let db_path = db_path.to_string_lossy().into_owned();
        let parent_cx = Cx::new().with_trace_context(21_760_001, 0, 0);
        let env = ConnectionEnv::new_with_root_cx(bridge_runtime_config(), &parent_cx);
        let runtime = env.runtime().clone();
        let conn = Connection::open_with_env(&db_path, env.clone())
            .await
            .expect("file-backed bridge connection should open");

        assert!(
            conn.root_cx().checkpoint().is_ok(),
            "explicit-root bridge should start with a live connection Cx"
        );
        assert_ne!(
            conn.root_cx().trace_id(),
            parent_cx.trace_id(),
            "connection root should derive a fresh trace under the supplied parent"
        );
        assert_eq!(
            runtime.runtime_id(),
            env.runtime().runtime_id(),
            "environment should preserve the explicit runtime context"
        );
        assert!(
            conn.is_concurrent_mode_default(),
            "bridge must preserve concurrent-writer default"
        );

        conn.set_strict_mem_fallback_rejection(true);
        conn.execute("CREATE TABLE bridge_probe(id INTEGER PRIMARY KEY, v INTEGER);")
            .await
            .expect("DDL should use the production execution path");
        conn.execute("BEGIN;")
            .await
            .expect("plain BEGIN should promote under concurrent default");
        conn.execute("INSERT INTO bridge_probe VALUES (1, 42);")
            .await
            .expect("INSERT should traverse the production pager path");
        conn.execute("COMMIT;")
            .await
            .expect("commit should publish through MVCC/WAL");
        assert!(conn.is_concurrent_mode_default());

        conn.reset_fallback_decision_evidence();
        let rows = conn
            .query("SELECT v FROM bridge_probe WHERE id = 1;")
            .await
            .expect("SELECT should traverse the production pager path");
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(42));
        let fallback = conn.fallback_decision_snapshot();
        assert!(
            fallback.decisions.is_empty(),
            "bridge proof must fail closed on compatibility fallback: {fallback:?}"
        );
        assert!(!fallback.truncated);

        drop(conn);

        let reopened = Connection::open_existing_with_env(&db_path, env)
            .await
            .expect("reopen should use the same explicit runtime");
        assert!(
            reopened.is_concurrent_mode_default(),
            "concurrent default must survive reopen"
        );
        reopened.set_strict_mem_fallback_rejection(true);
        let reopened_rows = reopened
            .query("SELECT v FROM bridge_probe WHERE id = 1;")
            .await
            .expect("reopened SELECT should stay on the production pager path");
        assert_eq!(reopened_rows[0].values()[0], SqliteValue::Integer(42));
    });
}

#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
#[test]
fn explicit_root_cx_env_propagates_native_cancellation() {
    use asupersync::Cx as NativeCx;
    use asupersync::types::CancelReason as NativeCancelReason;

    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir should be created");
        let db_path = dir.path().join("bd-2lt76-1-cancel.db");
        let db_path = db_path.to_string_lossy().into_owned();
        let parent_cx = Cx::new().with_trace_context(21_760_002, 0, 0);
        let native = NativeCx::for_testing();
        parent_cx.set_native_cx(native.clone());
        let env = ConnectionEnv::new_with_root_cx(bridge_runtime_config(), &parent_cx);
        let runtime = env.runtime().clone();
        let conn = Connection::open_with_env(&db_path, env)
            .await
            .expect("file-backed bridge connection should open");

        assert!(runtime.root_cx().attached_native_cx().is_some());
        assert!(conn.root_cx().attached_native_cx().is_some());

        native.set_cancel_reason(NativeCancelReason::timeout());
        assert!(
            conn.root_cx().checkpoint().is_err(),
            "explicit-root cancellation should reach connection root"
        );
    });
}
