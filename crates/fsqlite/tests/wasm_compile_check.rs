//! Tests that verify wasm32 compatibility at the type/API level.
//! These run on native but enforce invariants needed for wasm builds.

use fsqlite_types::SqliteValue;
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::VfsOpenFlags;
use fsqlite_vfs::{MemoryVfs, Vfs, VfsFile};

#[test]
fn sqlite_value_is_send_sync_on_native() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<SqliteValue>();
}

#[test]
fn memory_vfs_roundtrip_smoke() {
    let cx = Cx::new();
    let vfs = MemoryVfs::new();

    let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::MAIN_DB;
    let (mut file, _actual_flags) = vfs
        .open(&cx, Some("wasm-smoke.db".as_ref()), flags)
        .expect("memory vfs should open file");

    let page = [0u8; 128];
    file.write(&cx, &page, 0)
        .expect("memory vfs write should succeed");

    let mut buf = [0u8; 128];
    file.read(&cx, &mut buf, 0)
        .expect("memory vfs read should succeed");
    assert_eq!(buf, page);

    file.close(&cx).expect("memory vfs close should succeed");
}

#[test]
fn feature_flags_documented() {
    let cargo_toml = include_str!("../../fsqlite-types/Cargo.toml");
    assert!(
        cargo_toml.contains("[features]"),
        "Missing [features] section"
    );
    assert!(
        cargo_toml.contains("native ="),
        "Missing native feature in fsqlite-types"
    );
    assert!(
        cargo_toml.contains("wasm ="),
        "Missing wasm feature in fsqlite-types"
    );
}
