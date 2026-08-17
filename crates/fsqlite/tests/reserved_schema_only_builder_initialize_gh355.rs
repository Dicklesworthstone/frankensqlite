//! GH#355 (bd-h5oaj) regression reproducer: on Windows,
//! `Connection::initialize_reserved_schema_only_builder` returns `CannotOpen`
//! on a reservation that `reserve_schema_only_builder_target_with_env` has just
//! returned successfully. The identical sequence succeeds on Linux/macOS.
//!
//! This is the reporter's minimal reproducer (no database, no migration, just
//! the two engine calls), adapted to the in-tree async test harness. It PASSES
//! today on Unix — it is a cross-platform regression guard that will catch the
//! Windows failure once wired into the `windows-vfs-interop` CI job (kept out of
//! the required Windows matrix until the fix lands, so it does not red the build
//! in the meantime; it is directly runnable on a Windows host with
//! `cargo test -p fsqlite --features native reserved_schema_only_builder`).

use fsqlite::{Connection, ConnectionEnv};

#[test]
fn initialize_reserved_schema_only_builder_succeeds_on_a_fresh_reservation_gh355() {
    asupersync::test_utils::run_test(|| async {
        let dir = std::env::temp_dir().join(format!("fsq-gh355-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let target = dir.join("db.sqlite");
        // Clean any leftovers from a previous run so `reserve` starts fresh.
        let _ = std::fs::remove_file(&target);

        // Same shape as a page-size-bound schema-only writer env.
        let page_limit = (128_usize * 1024 * 1024).div_ceil(4096);
        let mut env = ConnectionEnv::default();
        env.set_page_buffer_max(page_limit);
        env.set_schema_only_write_set_page_limit(page_limit);
        env.set_strict_multi_process(true); // the issue reproduces with `false` too

        let reservation = Connection::reserve_schema_only_builder_target_with_env(&target, &env)
            .expect("reserve_schema_only_builder_target_with_env should succeed");

        // The defect: this returns Err(CannotOpen { .. }) on Windows for a
        // reservation that `reserve` just handed back. Must be Ok on every
        // platform — the reserved 0-byte file is the just-returned target.
        let connection =
            Connection::initialize_reserved_schema_only_builder(&reservation, env.clone())
                .await
                .unwrap_or_else(|err| {
                    panic!(
                        "initialize_reserved_schema_only_builder on a fresh reservation must \
                         succeed, got {err:?} (GH#355 / bd-h5oaj)"
                    )
                });
        connection
            .close()
            .await
            .expect("close bootstrapped builder");

        let _ = std::fs::remove_dir_all(&dir);
    });
}
