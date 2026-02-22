# fsqlite-vfs

Virtual filesystem abstraction layer for the fsqlite storage engine. This crate defines the traits and implementations that isolate all file I/O behind a pluggable interface, mirroring SQLite's `sqlite3_vfs` architecture.

## Overview

`fsqlite-vfs` is the lowest layer in the fsqlite storage stack. Every byte that reaches disk (or memory, or io_uring) passes through the `Vfs` and `VfsFile` traits defined here. The pager, WAL, and all higher layers depend on this crate but never call `std::fs` directly -- an ambient-authority audit gate enforces this boundary.

**Position in the dependency graph:**

```
fsqlite-types, fsqlite-error
        |
   fsqlite-vfs          <-- you are here
        |
   fsqlite-pager
      /    \
fsqlite-wal  fsqlite-btree
      \    /
   fsqlite-mvcc
```

## Key Types

- `Vfs` (trait) -- A virtual filesystem implementation. Provides `open`, `delete`, `access`, `full_pathname`, `randomness`, and `current_time`. Generic over its associated `File` type.
- `VfsFile` (trait) -- A file handle opened by a VFS. Supports `read`, `write`, `truncate`, `sync`, `file_size`, five-level locking (`lock`/`unlock`/`check_reserved_lock`), and shared-memory operations (`shm_map`, `shm_lock`, `shm_barrier`, `shm_unmap`) required for WAL mode.
- `UnixVfs` / `UnixFile` -- Production VFS for Unix systems using POSIX file I/O and `fcntl` locking.
- `IoUringVfs` / `IoUringFile` -- Linux-only VFS backed by `io_uring` for asynchronous I/O.
- `WindowsVfs` / `WindowsFile` -- Windows VFS using native file APIs and advisory locks.
- `MemoryVfs` / `MemoryFile` -- Fully in-memory VFS for testing. No disk I/O.
- `ShmRegion` -- Safe handle for shared-memory regions with bounds-checked accessors.
- `TracingFile` / `VfsMetrics` / `GLOBAL_VFS_METRICS` -- Instrumentation wrapper that records read/write/sync counts and latencies.
- `host_fs` (module) -- Audited helpers (`read`, `write`, `create_dir_all`, etc.) that are the only code permitted to call `std::fs` outside of VFS implementations.

## Usage

```rust
use fsqlite_vfs::{Vfs, VfsFile, MemoryVfs};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::VfsOpenFlags;

let cx = Cx::new();
let vfs = MemoryVfs::new();

// Open a database file in memory.
let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::MAIN_DB;
let (mut file, _actual_flags) = vfs.open(&cx, Some("test.db".as_ref()), flags).unwrap();

// Write a page.
let page = [0u8; 4096];
file.write(&cx, &page, 0).unwrap();

// Read it back.
let mut buf = [0u8; 4096];
file.read(&cx, &mut buf, 0).unwrap();
assert_eq!(buf, page);

file.close(&cx).unwrap();
```

## Dependencies

- `fsqlite-types` -- Shared type definitions (`LockLevel`, `VfsOpenFlags`, `SyncFlags`, `Cx`).
- `fsqlite-error` -- Unified error/result types.
- `nix`, `libc` -- POSIX syscall bindings (Unix builds).
- `uring-fs`, `pollster` -- io_uring support (Linux builds).
- `advisory-lock` -- File locking (Windows builds).

## License

MIT
