use std::cmp::Ordering;
use std::path::{Path, PathBuf};

use fsqlite_error::{FrankenError, Result};
use fsqlite_func::{
    AggregateFunction, AuthAction, AuthResult, Authorizer, CollationFunction, ColumnContext,
    IndexInfo, ScalarFunction, VirtualTable, VirtualTableCursor, WindowFunction,
};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{AccessFlags, SyncFlags, VfsOpenFlags};
use fsqlite_types::{LockLevel, SqliteValue};
use fsqlite_vfs::{ShmRegion, Vfs, VfsFile};

struct DemoFile;
impl VfsFile for DemoFile {
    fn close(&mut self, _cx: &Cx) -> Result<()> {
        Ok(())
    }
    fn read<'a>(
        &'a self,
        _cx: &'a Cx,
        _buf: &'a mut [u8],
        _offset: u64,
    ) -> impl std::future::Future<Output = Result<usize>> + Send + 'a {
        async { Ok(0) }
    }
    fn write<'a>(
        &'a mut self,
        _cx: &'a Cx,
        _buf: &'a [u8],
        _offset: u64,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a {
        async { Ok(()) }
    }
    fn truncate(&mut self, _cx: &Cx, _size: u64) -> Result<()> {
        Ok(())
    }
    fn sync(&mut self, _cx: &Cx, _flags: SyncFlags) -> Result<()> {
        Ok(())
    }
    fn file_size(&self, _cx: &Cx) -> Result<u64> {
        Ok(0)
    }
    fn lock(&mut self, _cx: &Cx, _level: LockLevel) -> Result<()> {
        Ok(())
    }
    fn unlock(&mut self, _cx: &Cx, _level: LockLevel) -> Result<()> {
        Ok(())
    }
    fn check_reserved_lock(&self, _cx: &Cx) -> Result<bool> {
        Ok(false)
    }
    fn shm_map(&mut self, _cx: &Cx, _region: u32, _size: u32, _extend: bool) -> Result<ShmRegion> {
        Err(FrankenError::Unsupported)
    }
    fn shm_lock(&mut self, _cx: &Cx, _offset: u32, _n: u32, _flags: u32) -> Result<()> {
        Err(FrankenError::Unsupported)
    }
    fn shm_barrier(&self) {}
    fn shm_unmap(&mut self, _cx: &Cx, _delete: bool) -> Result<()> {
        Ok(())
    }
}

struct DemoVfs;
impl Vfs for DemoVfs {
    type File = DemoFile;

    fn name(&self) -> &'static str {
        "demo"
    }
    fn open(
        &self,
        _cx: &Cx,
        _path: Option<&Path>,
        flags: VfsOpenFlags,
    ) -> Result<(Self::File, VfsOpenFlags)> {
        Ok((DemoFile, flags))
    }
    fn delete(&self, _cx: &Cx, _path: &Path, _sync_dir: bool) -> Result<()> {
        Ok(())
    }
    fn access(&self, _cx: &Cx, _path: &Path, _flags: AccessFlags) -> Result<bool> {
        Ok(true)
    }
    fn full_pathname(&self, _cx: &Cx, path: &Path) -> Result<PathBuf> {
        Ok(path.to_path_buf())
    }
}

struct DemoScalar;
impl ScalarFunction for DemoScalar {
    fn invoke(&self, _args: &[SqliteValue]) -> Result<SqliteValue> {
        Ok(SqliteValue::Integer(1))
    }
    fn num_args(&self) -> i32 {
        0
    }
    fn name(&self) -> &str {
        "demo_scalar"
    }
}

struct DemoAggregate;
impl AggregateFunction for DemoAggregate {
    type State = i64;
    fn initial_state(&self) -> Self::State {
        0
    }
    fn step(&self, state: &mut Self::State, _args: &[SqliteValue]) -> Result<()> {
        *state += 1;
        Ok(())
    }
    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        Ok(SqliteValue::Integer(state))
    }
    fn num_args(&self) -> i32 {
        -1
    }
    fn name(&self) -> &str {
        "demo_aggregate"
    }
}

struct DemoWindow;
impl WindowFunction for DemoWindow {
    type State = i64;
    fn initial_state(&self) -> Self::State {
        0
    }
    fn step(&self, state: &mut Self::State, _args: &[SqliteValue]) -> Result<()> {
        *state += 1;
        Ok(())
    }
    fn inverse(&self, state: &mut Self::State, _args: &[SqliteValue]) -> Result<()> {
        *state -= 1;
        Ok(())
    }
    fn value(&self, state: &Self::State) -> Result<SqliteValue> {
        Ok(SqliteValue::Integer(*state))
    }
    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        Ok(SqliteValue::Integer(state))
    }
    fn num_args(&self) -> i32 {
        -1
    }
    fn name(&self) -> &str {
        "demo_window"
    }
}

struct DemoVtab;
struct DemoCursor;
impl VirtualTable for DemoVtab {
    type Cursor = DemoCursor;
    fn connect(_cx: &Cx, _args: &[&str]) -> Result<Self> {
        Ok(Self)
    }
    fn best_index(&self, _info: &mut IndexInfo) -> Result<()> {
        Ok(())
    }
    fn open(&self) -> Result<Self::Cursor> {
        Ok(DemoCursor)
    }
}
impl VirtualTableCursor for DemoCursor {
    fn filter(
        &mut self,
        _cx: &Cx,
        _idx_num: i32,
        _idx_str: Option<&str>,
        _args: &[SqliteValue],
    ) -> Result<()> {
        Ok(())
    }
    fn next(&mut self, _cx: &Cx) -> Result<()> {
        Ok(())
    }
    fn eof(&self) -> bool {
        true
    }
    fn column(&self, ctx: &mut ColumnContext, _col: i32) -> Result<()> {
        ctx.set_value(SqliteValue::Integer(1));
        Ok(())
    }
    fn rowid(&self) -> Result<i64> {
        Ok(1)
    }
}

struct DemoCollation;
impl CollationFunction for DemoCollation {
    fn name(&self) -> &str {
        "demo_collation"
    }
    fn compare(&self, left: &[u8], right: &[u8]) -> Ordering {
        left.cmp(right)
    }
}

struct DemoAuthorizer;
impl Authorizer for DemoAuthorizer {
    fn authorize(
        &self,
        _action: AuthAction,
        _arg1: Option<&str>,
        _arg2: Option<&str>,
        _db_name: Option<&str>,
        _trigger: Option<&str>,
    ) -> AuthResult {
        AuthResult::Ok
    }
}

fn main() {
    let _ = DemoVfs;
    let _ = DemoScalar;
    let _ = DemoAggregate;
    let _ = DemoWindow;
    let _ = DemoVtab;
    let _ = DemoCollation;
    let _ = DemoAuthorizer;
}
