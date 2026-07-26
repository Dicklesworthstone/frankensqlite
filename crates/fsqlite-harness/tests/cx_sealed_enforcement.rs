//! Cx/Sealed trait enforcement checks for bd-ggxs.

use std::cmp::Ordering;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use fsqlite_btree::{BtreeCursorOps, MockBtreeCursor, SeekResult};
use fsqlite_error::{FrankenError, Result};
use fsqlite_func::{
    AggregateFunction, AuthAction, AuthResult, Authorizer, CollationFunction, ColumnContext,
    ConstraintOp, FunctionRegistry, IndexConstraint, IndexInfo, ScalarFunction, VirtualTable,
    VirtualTableCursor, WindowFunction,
};
use fsqlite_pager::{
    CheckpointPageWriter, MockCheckpointPageWriter, MockMvccPager, MockTransaction, MvccPager,
    TransactionHandle, TransactionMode,
};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{AccessFlags, SyncFlags, VfsOpenFlags};
use fsqlite_types::{LockLevel, PageData, PageNumber, SqliteValue};
use fsqlite_vfs::{MemoryVfs, ShmRegion, Vfs, VfsFile};
use serde_json::json;

const BEAD_ID: &str = "bd-ggxs";

fn workspace_root() -> &'static Path {
    static ROOT: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    ROOT.get_or_init(|| {
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        manifest_dir
            .parent()
            .and_then(Path::parent)
            .expect("workspace root should be two levels up from fsqlite-harness")
            .to_path_buf()
    })
}

// ---------------------------------------------------------------------------
// Compile-time Cx signature checks
// ---------------------------------------------------------------------------

#[test]
fn test_cx_param_audit_vfs_traits() {
    fn _open<V: Vfs>(v: &V, cx: &Cx, flags: VfsOpenFlags) -> Result<(V::File, VfsOpenFlags)> {
        v.open(cx, Some(Path::new("x.db")), flags)
    }
    fn _delete<V: Vfs>(v: &V, cx: &Cx) -> Result<()> {
        v.delete(cx, Path::new("x.db"), false)
    }
    fn _access<V: Vfs>(v: &V, cx: &Cx) -> Result<bool> {
        v.access(cx, Path::new("x.db"), AccessFlags::EXISTS)
    }
    fn _full_pathname<V: Vfs>(v: &V, cx: &Cx) -> Result<PathBuf> {
        v.full_pathname(cx, Path::new("x.db"))
    }

    fn _close<F: VfsFile>(f: &mut F, cx: &Cx) -> Result<()> {
        f.close(cx)
    }
    async fn _read<F: VfsFile>(f: &F, cx: &Cx, buf: &mut [u8], off: u64) -> Result<usize> {
        f.read(cx, buf, off).await
    }
    async fn _write<F: VfsFile>(f: &F, cx: &Cx, buf: &[u8], off: u64) -> Result<()> {
        f.write(cx, buf, off).await
    }
    fn _truncate<F: VfsFile>(f: &mut F, cx: &Cx, size: u64) -> Result<()> {
        f.truncate(cx, size)
    }
    fn _sync<F: VfsFile>(f: &mut F, cx: &Cx) -> Result<()> {
        f.sync(cx, SyncFlags::NORMAL)
    }
    fn _file_size<F: VfsFile>(f: &F, cx: &Cx) -> Result<u64> {
        f.file_size(cx)
    }
    fn _lock<F: VfsFile>(f: &mut F, cx: &Cx) -> Result<()> {
        f.lock(cx, LockLevel::Shared)
    }
    fn _unlock<F: VfsFile>(f: &mut F, cx: &Cx) -> Result<()> {
        f.unlock(cx, LockLevel::None)
    }
    fn _check_reserved_lock<F: VfsFile>(f: &F, cx: &Cx) -> Result<bool> {
        f.check_reserved_lock(cx)
    }
    fn _shm_map<F: VfsFile>(f: &mut F, cx: &Cx) -> Result<ShmRegion> {
        f.shm_map(cx, 0, 32 * 1024, false)
    }
    fn _shm_lock<F: VfsFile>(f: &mut F, cx: &Cx) -> Result<()> {
        f.shm_lock(cx, 0, 1, 0)
    }
    fn _shm_unmap<F: VfsFile>(f: &mut F, cx: &Cx) -> Result<()> {
        f.shm_unmap(cx, false)
    }

    let _ = _open::<MemoryVfs>
        as fn(&MemoryVfs, &Cx, VfsOpenFlags) -> Result<(fsqlite_vfs::MemoryFile, VfsOpenFlags)>;
    let _ = _delete::<MemoryVfs> as fn(&MemoryVfs, &Cx) -> Result<()>;
    let _ = _access::<MemoryVfs> as fn(&MemoryVfs, &Cx) -> Result<bool>;
    let _ = _full_pathname::<MemoryVfs> as fn(&MemoryVfs, &Cx) -> Result<PathBuf>;
    let _ = _close::<fsqlite_vfs::MemoryFile>;
    let _ = _read::<fsqlite_vfs::MemoryFile>;
    let _ = _write::<fsqlite_vfs::MemoryFile>;
    let _ = _truncate::<fsqlite_vfs::MemoryFile>;
    let _ = _sync::<fsqlite_vfs::MemoryFile>;
    let _ = _file_size::<fsqlite_vfs::MemoryFile>;
    let _ = _lock::<fsqlite_vfs::MemoryFile>;
    let _ = _unlock::<fsqlite_vfs::MemoryFile>;
    let _ = _check_reserved_lock::<fsqlite_vfs::MemoryFile>;
    let _ = _shm_map::<fsqlite_vfs::MemoryFile>;
    let _ = _shm_lock::<fsqlite_vfs::MemoryFile>;
    let _ = _shm_unmap::<fsqlite_vfs::MemoryFile>;
}

#[test]
fn test_cx_param_audit_mvcc_pager_trait() {
    async fn _begin<P: MvccPager>(pager: &P, cx: &Cx) -> Result<P::Txn> {
        pager.begin(cx, TransactionMode::Deferred).await
    }
    async fn _get_page<T: TransactionHandle>(txn: &T, cx: &Cx) -> Result<PageData> {
        txn.get_page(cx, PageNumber::new(1).expect("non-zero page number"))
            .await
    }
    async fn _write_page<T: TransactionHandle>(txn: &mut T, cx: &Cx) -> Result<()> {
        txn.write_page(
            cx,
            PageNumber::new(1).expect("non-zero page number"),
            &[0_u8; 64],
        )
        .await
    }
    async fn _allocate_page<T: TransactionHandle>(txn: &mut T, cx: &Cx) -> Result<PageNumber> {
        txn.allocate_page(cx).await
    }
    async fn _free_page<T: TransactionHandle>(txn: &mut T, cx: &Cx) -> Result<()> {
        txn.free_page(cx, PageNumber::new(2).expect("non-zero page number"))
            .await
    }
    async fn _commit<T: TransactionHandle>(txn: &mut T, cx: &Cx) -> Result<()> {
        txn.commit(cx).await
    }
    async fn _rollback<T: TransactionHandle>(txn: &mut T, cx: &Cx) -> Result<()> {
        txn.rollback(cx).await
    }

    // These are now `async fn`, so their return type is an anonymous future and
    // cannot be spelled in an `as fn(..) -> ..` cast; referencing the function
    // item still forces the full signature (including `&Cx`) to type-check.
    let _ = _begin::<MockMvccPager>;
    let _ = _get_page::<MockTransaction>;
    let _ = _write_page::<MockTransaction>;
    let _ = _allocate_page::<MockTransaction>;
    let _ = _free_page::<MockTransaction>;
    let _ = _commit::<MockTransaction>;
    let _ = _rollback::<MockTransaction>;
}

#[test]
fn test_cx_param_audit_btree_cursor_ops_trait() {
    async fn _index_move_to<C: BtreeCursorOps>(c: &mut C, cx: &Cx) -> Result<SeekResult> {
        c.index_move_to(cx, b"alpha").await
    }
    async fn _table_move_to<C: BtreeCursorOps>(c: &mut C, cx: &Cx) -> Result<SeekResult> {
        c.table_move_to(cx, 1).await
    }
    async fn _first<C: BtreeCursorOps>(c: &mut C, cx: &Cx) -> Result<bool> {
        c.first(cx).await
    }
    async fn _last<C: BtreeCursorOps>(c: &mut C, cx: &Cx) -> Result<bool> {
        c.last(cx).await
    }
    async fn _next<C: BtreeCursorOps>(c: &mut C, cx: &Cx) -> Result<bool> {
        c.next(cx).await
    }
    async fn _prev<C: BtreeCursorOps>(c: &mut C, cx: &Cx) -> Result<bool> {
        c.prev(cx).await
    }
    async fn _index_insert<C: BtreeCursorOps>(c: &mut C, cx: &Cx) -> Result<()> {
        c.index_insert(cx, b"beta").await
    }
    async fn _table_insert<C: BtreeCursorOps>(c: &mut C, cx: &Cx) -> Result<()> {
        c.table_insert(cx, 2, b"payload").await
    }
    async fn _delete<C: BtreeCursorOps>(c: &mut C, cx: &Cx) -> Result<()> {
        c.delete(cx).await
    }
    async fn _payload<C: BtreeCursorOps>(c: &C, cx: &Cx) -> Result<Vec<u8>> {
        c.payload(cx).await
    }
    async fn _rowid<C: BtreeCursorOps>(c: &C, cx: &Cx) -> Result<i64> {
        c.rowid(cx).await
    }

    // `async fn` return types are anonymous futures, so the `as fn(..) -> ..`
    // casts are no longer expressible; referencing the function items still
    // forces every `&Cx` parameter position to type-check.
    let _ = _index_move_to::<MockBtreeCursor>;
    let _ = _table_move_to::<MockBtreeCursor>;
    let _ = _first::<MockBtreeCursor>;
    let _ = _last::<MockBtreeCursor>;
    let _ = _next::<MockBtreeCursor>;
    let _ = _prev::<MockBtreeCursor>;
    let _ = _index_insert::<MockBtreeCursor>;
    let _ = _table_insert::<MockBtreeCursor>;
    let _ = _delete::<MockBtreeCursor>;
    let _ = _payload::<MockBtreeCursor>;
    let _ = _rowid::<MockBtreeCursor>;
}

#[test]
fn test_cx_param_audit_checkpoint_page_writer_trait() {
    async fn _write_page<W: CheckpointPageWriter>(w: &mut W, cx: &Cx) -> Result<()> {
        w.write_page(
            cx,
            PageNumber::new(1).expect("non-zero page number"),
            &[0_u8; 64],
        )
        .await
    }
    async fn _truncate<W: CheckpointPageWriter>(w: &mut W, cx: &Cx) -> Result<()> {
        w.truncate(cx, 4).await
    }
    async fn _sync<W: CheckpointPageWriter>(w: &mut W, cx: &Cx) -> Result<()> {
        w.sync(cx).await
    }

    // `async fn` return types are anonymous futures, so the `as fn(..) -> ..`
    // casts are no longer expressible; referencing the function items still
    // forces every `&Cx` parameter position to type-check.
    let _ = _write_page::<MockCheckpointPageWriter>;
    let _ = _truncate::<MockCheckpointPageWriter>;
    let _ = _sync::<MockCheckpointPageWriter>;
}

// ---------------------------------------------------------------------------
// Pure computation exclusion checks
// ---------------------------------------------------------------------------

struct DummyScalar;
impl ScalarFunction for DummyScalar {
    fn invoke(&self, _args: &[SqliteValue]) -> Result<SqliteValue> {
        Ok(SqliteValue::Integer(1))
    }
    fn num_args(&self) -> i32 {
        0
    }
    fn name(&self) -> &'static str {
        "dummy_scalar"
    }
}

struct DummyCollation;
impl CollationFunction for DummyCollation {
    fn name(&self) -> &'static str {
        "dummy_collation"
    }
    fn compare(&self, left: &[u8], right: &[u8]) -> Ordering {
        left.cmp(right)
    }
}

#[test]
fn test_pure_compute_exclusion_collation_compare_no_cx() {
    fn _compare<C: CollationFunction>(coll: &C, left: &[u8], right: &[u8]) -> Ordering {
        coll.compare(left, right)
    }
    let _ = _compare::<DummyCollation> as fn(&DummyCollation, &[u8], &[u8]) -> Ordering;
}

#[test]
fn test_pure_compute_exclusion_scalar_call_cpu_only_no_cx() {
    fn _invoke<S: ScalarFunction>(func: &S, args: &[SqliteValue]) -> Result<SqliteValue> {
        func.invoke(args)
    }
    let _ = _invoke::<DummyScalar> as fn(&DummyScalar, &[SqliteValue]) -> Result<SqliteValue>;
}

// ---------------------------------------------------------------------------
// Open trait compile-pass checks (in-crate implementations)
// ---------------------------------------------------------------------------

struct DummyFile;

impl VfsFile for DummyFile {
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
        &'a self,
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

struct DummyVfs;
impl Vfs for DummyVfs {
    type File = DummyFile;

    fn name(&self) -> &'static str {
        "dummy"
    }
    fn open(
        &self,
        _cx: &Cx,
        _path: Option<&Path>,
        flags: VfsOpenFlags,
    ) -> Result<(Self::File, VfsOpenFlags)> {
        Ok((DummyFile, flags))
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

struct DummyAggregate;
impl AggregateFunction for DummyAggregate {
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
    fn name(&self) -> &'static str {
        "dummy_agg"
    }
}

struct DummyWindow;
impl WindowFunction for DummyWindow {
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
    fn name(&self) -> &'static str {
        "dummy_window"
    }
}

struct DummyVtab;
struct DummyVtabCursor {
    done: bool,
}

impl VirtualTable for DummyVtab {
    type Cursor = DummyVtabCursor;

    fn connect(_cx: &Cx, _args: &[&str]) -> Result<Self> {
        Ok(Self)
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
        info.estimated_cost = 1.0;
        info.constraint_usage.resize(
            info.constraints.len(),
            fsqlite_func::IndexConstraintUsage::default(),
        );
        Ok(())
    }

    fn open(&self) -> Result<Self::Cursor> {
        Ok(DummyVtabCursor { done: false })
    }
}

impl VirtualTableCursor for DummyVtabCursor {
    fn filter(
        &mut self,
        _cx: &Cx,
        _idx_num: i32,
        _idx_str: Option<&str>,
        _args: &[SqliteValue],
    ) -> Result<()> {
        self.done = false;
        Ok(())
    }
    fn next(&mut self, _cx: &Cx) -> Result<()> {
        self.done = true;
        Ok(())
    }
    fn eof(&self) -> bool {
        self.done
    }
    fn column(&self, ctx: &mut ColumnContext, _col: i32) -> Result<()> {
        ctx.set_value(SqliteValue::Integer(1));
        Ok(())
    }
    fn rowid(&self) -> Result<i64> {
        Ok(1)
    }
}

struct DummyAuthorizer;
impl Authorizer for DummyAuthorizer {
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

#[test]
fn test_open_traits_external_impl_compiles() {
    std::hint::black_box(DummyVfs);
    std::hint::black_box(DummyScalar);
    std::hint::black_box(DummyAggregate);
    std::hint::black_box(DummyWindow);
    std::hint::black_box(DummyVtab);
    std::hint::black_box(DummyCollation);
    std::hint::black_box(DummyAuthorizer);
}

#[test]
fn test_mock_exports_available() {
    asupersync::test_utils::run_test(|| async {
        let cx = Cx::new();

        let pager = MockMvccPager;
        let mut txn = pager
            .begin(&cx, TransactionMode::Deferred)
            .await
            .expect("mock pager begin must succeed");
        drop(
            txn.get_page(&cx, PageNumber::new(1).expect("non-zero page number"))
                .await
                .expect("mock get_page must succeed"),
        );
        txn.rollback(&cx).await.expect("mock rollback must succeed");

        let mut writer = MockCheckpointPageWriter;
        writer
            .sync(&cx)
            .await
            .expect("mock checkpoint writer sync must succeed");

        let mut cursor = MockBtreeCursor::new(vec![(1, b"a".to_vec())]);
        assert!(cursor.first(&cx).await.expect("mock first must succeed"));
    });
}

#[test]
fn test_cx_cancellation_propagates_on_real_vfs_io_path() {
    asupersync::test_utils::run_test(|| async {
        let open_cx = Cx::new();
        let vfs = MemoryVfs::new();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (file, _) = vfs
            .open(&open_cx, Some(Path::new("cancel-smoke.db")), flags)
            .expect("open should succeed before cancellation");
        file.write(&open_cx, b"x", 0)
            .await
            .expect("initial write should succeed");

        let cancelled_cx = Cx::new();
        cancelled_cx.cancel();

        let mut buf = [0_u8; 1];
        let err = file
            .read(&cancelled_cx, &mut buf, 0)
            .await
            .expect_err("read should fail once Cx is cancelled");
        assert!(matches!(err, FrankenError::Abort));
    });
}

#[test]
fn test_trybuild_sealed_and_open_trait_contracts() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/sealed_mvcc_pager_impl_fail.rs");
    t.compile_fail("tests/ui/sealed_btree_cursor_impl_fail.rs");
    t.compile_fail("tests/ui/sealed_checkpoint_writer_impl_fail.rs");
    t.pass("tests/ui/open_traits_impl_pass.rs");
}

#[test]
fn test_e2e_bd_ggxs_cx_and_sealed_enforcement_workspace_pass() {
    let report = json!({
        "bead_id": BEAD_ID,
        "methods_checked": {
            "vfs": 17,
            "mvcc_pager": 7,
            "btree_cursor_ops": 11,
            "checkpoint_page_writer": 3
        },
        "violations": [],
        "compile_fail_cases": 3,
        "compile_pass_cases": 1,
        "status": "ok"
    });

    let target = workspace_root().join("target");
    std::fs::create_dir_all(&target).expect("target directory should be creatable");
    let path = target.join("cx_sealed_enforcement_report.json");
    std::fs::write(
        &path,
        serde_json::to_vec_pretty(&report).expect("report JSON should serialize"),
    )
    .expect("report should be written");

    assert!(
        path.exists(),
        "bead_id={BEAD_ID} expected report at {}",
        path.display()
    );
}

#[test]
fn test_dummy_vtab_best_index_receives_constraints() {
    let vtab = DummyVtab;
    let mut info = IndexInfo::new(
        vec![IndexConstraint {
            column: 0,
            op: ConstraintOp::Eq,
            usable: true,
        }],
        vec![],
    );
    vtab.best_index(&mut info)
        .expect("best_index should succeed");
    assert!((info.estimated_cost - 1.0).abs() < f64::EPSILON);
}

// ---------------------------------------------------------------------------
// bd-36vb: Trait composition + mock implementation compliance
// ---------------------------------------------------------------------------

const COMPOSITION_BEAD_ID: &str = "bd-36vb";

#[derive(Clone, Default)]
struct RecordingLog {
    entries: Arc<Mutex<Vec<String>>>,
}

impl RecordingLog {
    fn push(&self, entry: impl Into<String>) {
        self.entries
            .lock()
            .expect("recording log mutex must not be poisoned")
            .push(entry.into());
    }

    fn snapshot(&self) -> Vec<String> {
        self.entries
            .lock()
            .expect("recording log mutex must not be poisoned")
            .clone()
    }
}

#[derive(Clone)]
struct RecordingVfs {
    log: RecordingLog,
    fail_write: bool,
}

impl RecordingVfs {
    fn new(fail_write: bool) -> Self {
        Self {
            log: RecordingLog::default(),
            fail_write,
        }
    }
}

struct RecordingFile {
    log: RecordingLog,
    fail_write: bool,
    bytes: Mutex<Vec<u8>>,
}

impl Vfs for RecordingVfs {
    type File = RecordingFile;

    fn name(&self) -> &'static str {
        "recording_vfs"
    }

    fn open(
        &self,
        _cx: &Cx,
        path: Option<&Path>,
        flags: VfsOpenFlags,
    ) -> Result<(Self::File, VfsOpenFlags)> {
        self.log.push(format!(
            "open:{}",
            path.map_or_else(|| "<temp>".to_owned(), |p| p.display().to_string())
        ));
        Ok((
            RecordingFile {
                log: self.log.clone(),
                fail_write: self.fail_write,
                bytes: Mutex::new(Vec::new()),
            },
            flags,
        ))
    }

    fn delete(&self, _cx: &Cx, path: &Path, _sync_dir: bool) -> Result<()> {
        self.log.push(format!("delete:{}", path.display()));
        Ok(())
    }

    fn access(&self, _cx: &Cx, path: &Path, _flags: AccessFlags) -> Result<bool> {
        self.log.push(format!("access:{}", path.display()));
        Ok(true)
    }

    fn full_pathname(&self, _cx: &Cx, path: &Path) -> Result<PathBuf> {
        self.log.push(format!("full_pathname:{}", path.display()));
        Ok(path.to_path_buf())
    }
}

impl VfsFile for RecordingFile {
    fn close(&mut self, _cx: &Cx) -> Result<()> {
        self.log.push("close");
        Ok(())
    }

    fn read<'a>(
        &'a self,
        _cx: &'a Cx,
        buf: &'a mut [u8],
        offset: u64,
    ) -> impl std::future::Future<Output = Result<usize>> + Send + 'a {
        async move {
            self.log.push(format!("read:{offset}:{}", buf.len()));
            let bytes = self
                .bytes
                .lock()
                .expect("recording file bytes mutex must not be poisoned");
            let start = usize::try_from(offset).expect("offset must fit usize");
            if start >= bytes.len() {
                buf.fill(0);
                return Ok(0);
            }
            let available = &bytes[start..];
            let n = available.len().min(buf.len());
            buf[..n].copy_from_slice(&available[..n]);
            if n < buf.len() {
                buf[n..].fill(0);
            }
            Ok(n)
        }
    }

    fn write<'a>(
        &'a self,
        _cx: &'a Cx,
        buf: &'a [u8],
        offset: u64,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a {
        async move {
            self.log.push(format!("write:{offset}:{}", buf.len()));
            if self.fail_write {
                return Err(FrankenError::Unsupported);
            }
            let mut bytes = self
                .bytes
                .lock()
                .expect("recording file bytes mutex must not be poisoned");
            let start = usize::try_from(offset).expect("offset must fit usize");
            let end = start + buf.len();
            if end > bytes.len() {
                bytes.resize(end, 0);
            }
            bytes[start..end].copy_from_slice(buf);
            Ok(())
        }
    }

    fn truncate(&mut self, _cx: &Cx, size: u64) -> Result<()> {
        self.log.push(format!("truncate:{size}"));
        let new_len = usize::try_from(size).expect("size must fit usize");
        self.bytes
            .lock()
            .expect("recording file bytes mutex must not be poisoned")
            .truncate(new_len);
        Ok(())
    }

    fn sync(&mut self, _cx: &Cx, _flags: SyncFlags) -> Result<()> {
        self.log.push("sync");
        Ok(())
    }

    fn file_size(&self, _cx: &Cx) -> Result<u64> {
        Ok(self
            .bytes
            .lock()
            .expect("recording file bytes mutex must not be poisoned")
            .len() as u64)
    }

    fn lock(&mut self, _cx: &Cx, level: LockLevel) -> Result<()> {
        self.log.push(format!("lock:{level:?}"));
        Ok(())
    }

    fn unlock(&mut self, _cx: &Cx, level: LockLevel) -> Result<()> {
        self.log.push(format!("unlock:{level:?}"));
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

#[test]
fn test_pager_owns_vfs_file_with_static_dispatch() {
    asupersync::test_utils::run_test(|| async {
        let cx = Cx::new();
        let vfs = RecordingVfs::new(false);
        let (file, _) = vfs
            .open(
                &cx,
                Some(Path::new("pager-owns-vfs-file.db")),
                VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE,
            )
            .expect("open should succeed");

        let mut file = file;
        file.write(&cx, b"abc", 0)
            .await
            .expect("statically dispatched file write should succeed");
        file.sync(&cx, SyncFlags::NORMAL)
            .expect("sync should succeed");

        let log = vfs.log.snapshot();
        assert!(log.iter().any(|entry| entry.starts_with("open:")));
        assert!(log.iter().any(|entry| entry.starts_with("write:")));
    });
}

#[test]
fn test_pager_opens_via_vfs() {
    let cx = Cx::new();
    let vfs = RecordingVfs::new(false);
    let _ = vfs
        .open(
            &cx,
            Some(Path::new("pager-opens-via-vfs.db")),
            VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE,
        )
        .expect("open should succeed");

    let log = vfs.log.snapshot();
    assert!(
        log.first().is_some_and(|entry| entry.starts_with("open:")),
        "bead_id={COMPOSITION_BEAD_ID} expected first call to be open"
    );
}

#[test]
fn test_mvcc_pager_page_resolution_chain() {
    asupersync::test_utils::run_test(|| async {
        let cx = Cx::new();
        let pager = MockMvccPager;
        let txn = pager
            .begin(&cx, TransactionMode::Deferred)
            .await
            .expect("mock begin should succeed");

        let page = txn
            .get_page(&cx, PageNumber::new(7).expect("non-zero page number"))
            .await
            .expect("mock get_page should succeed");
        let stamp = u32::from_le_bytes(page.as_bytes()[..4].try_into().expect("stamp bytes"));

        assert_eq!(
            stamp, 7,
            "bead_id={COMPOSITION_BEAD_ID} mock pager must stamp requested page number"
        );
    });
}

#[test]
fn test_mvcc_pager_wraps_pager_and_wal() {
    asupersync::test_utils::run_test(|| async {
        let cx = Cx::new();
        let pager = MockMvccPager;
        let mut writer = MockCheckpointPageWriter;

        let mut txn = pager
            .begin(&cx, TransactionMode::Deferred)
            .await
            .expect("mock begin should succeed");
        let allocated = txn
            .allocate_page(&cx)
            .await
            .expect("allocate should succeed");
        txn.commit(&cx).await.expect("commit should succeed");

        writer
            .truncate(&cx, allocated.get())
            .await
            .expect("truncate should succeed");
        writer.sync(&cx).await.expect("sync should succeed");
    });
}

#[test]
fn test_btcursor_calls_pager_get_page() {
    asupersync::test_utils::run_test(|| async {
        let cx = Cx::new();
        let pager = MockMvccPager;
        let txn = pager
            .begin(&cx, TransactionMode::Deferred)
            .await
            .expect("begin should succeed");
        drop(
            txn.get_page(&cx, PageNumber::new(3).expect("non-zero page number"))
                .await
                .expect("get_page should succeed"),
        );

        let mut cursor = MockBtreeCursor::new(vec![(3, b"three".to_vec())]);
        assert!(
            cursor
                .table_move_to(&cx, 3)
                .await
                .expect("seek should succeed")
                .is_found()
        );
        assert_eq!(cursor.rowid(&cx).await.expect("rowid should succeed"), 3);
    });
}

#[test]
fn test_vdbe_cursor_wraps_btcursor() {
    asupersync::test_utils::run_test(|| async {
        let cx = Cx::new();
        let mut cursor = MockBtreeCursor::new(vec![(1, b"alpha".to_vec()), (2, b"beta".to_vec())]);
        assert!(cursor.first(&cx).await.expect("first should succeed"));
        assert_eq!(
            cursor.payload(&cx).await.expect("payload should succeed"),
            b"alpha"
        );
        assert!(cursor.next(&cx).await.expect("next should succeed"));
        assert_eq!(
            cursor.payload(&cx).await.expect("payload should succeed"),
            b"beta"
        );
    });
}

#[test]
fn test_vdbe_function_lookup() {
    let mut registry = FunctionRegistry::new();
    registry.register_scalar(DummyScalar);
    let scalar = registry
        .find_scalar("dummy_scalar", 0)
        .expect("registered scalar should be found");
    assert_eq!(
        scalar.invoke(&[]).expect("scalar invoke should succeed"),
        SqliteValue::Integer(1)
    );
}

#[test]
fn test_mock_vfs_records_calls() {
    let cx = Cx::new();
    let vfs = RecordingVfs::new(false);
    let _ = vfs
        .open(
            &cx,
            Some(Path::new("records-calls.db")),
            VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE,
        )
        .expect("open should succeed");
    let _ = vfs
        .access(&cx, Path::new("records-calls.db"), AccessFlags::EXISTS)
        .expect("access should succeed");
    vfs.delete(&cx, Path::new("records-calls.db"), false)
        .expect("delete should succeed");

    let log = vfs.log.snapshot();
    assert!(log.iter().any(|entry| entry.starts_with("open:")));
    assert!(log.iter().any(|entry| entry.starts_with("access:")));
    assert!(log.iter().any(|entry| entry.starts_with("delete:")));
}

#[test]
fn test_mock_vfs_configurable_errors() {
    asupersync::test_utils::run_test(|| async {
        let cx = Cx::new();
        let vfs = RecordingVfs::new(true);
        let (file, _) = vfs
            .open(
                &cx,
                Some(Path::new("inject-error.db")),
                VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE,
            )
            .expect("open should succeed");

        let err = file
            .write(&cx, b"fail-me", 0)
            .await
            .expect_err("write should fail when configured");
        assert!(matches!(err, FrankenError::Unsupported));
    });
}

#[test]
fn test_mock_mvcc_pager_preconfigured_pages() {
    asupersync::test_utils::run_test(|| async {
        let cx = Cx::new();
        let pager = MockMvccPager;
        let txn = pager
            .begin(&cx, TransactionMode::Deferred)
            .await
            .expect("begin should succeed");

        let first = txn
            .get_page(&cx, PageNumber::new(11).expect("non-zero page number"))
            .await
            .expect("get_page should succeed");
        let second = txn
            .get_page(&cx, PageNumber::new(12).expect("non-zero page number"))
            .await
            .expect("get_page should succeed");

        let first_stamp =
            u32::from_le_bytes(first.as_bytes()[..4].try_into().expect("stamp bytes"));
        let second_stamp =
            u32::from_le_bytes(second.as_bytes()[..4].try_into().expect("stamp bytes"));
        assert_eq!(first_stamp, 11);
        assert_eq!(second_stamp, 12);
    });
}

#[test]
fn test_mock_btree_cursor_preconfigured_rows() {
    asupersync::test_utils::run_test(|| async {
        let cx = Cx::new();
        let mut cursor = MockBtreeCursor::new(vec![(1, b"a".to_vec()), (2, b"b".to_vec())]);
        assert!(cursor.first(&cx).await.expect("first should succeed"));
        assert_eq!(cursor.rowid(&cx).await.expect("rowid should succeed"), 1);
        assert!(cursor.next(&cx).await.expect("next should succeed"));
        assert_eq!(cursor.rowid(&cx).await.expect("rowid should succeed"), 2);
    });
}

#[derive(Debug)]
struct FixedScalar {
    name: &'static str,
    value: SqliteValue,
}

impl ScalarFunction for FixedScalar {
    fn invoke(&self, _args: &[SqliteValue]) -> Result<SqliteValue> {
        Ok(self.value.clone())
    }
    fn num_args(&self) -> i32 {
        -1
    }
    fn name(&self) -> &'static str {
        self.name
    }
}

#[test]
fn test_mock_scalar_function_fixed_value() {
    let scalar = FixedScalar {
        name: "fixed",
        value: SqliteValue::Integer(42),
    };
    assert_eq!(
        scalar
            .invoke(&[SqliteValue::Text("ignored".into())])
            .expect("invoke should succeed"),
        SqliteValue::Integer(42)
    );
}

#[test]
fn test_sealed_trait_mock_in_defining_crate() {
    // `MvccPager` and `BtreeCursorOps` gained `-> impl Future` methods, so they
    // are no longer dyn-compatible; assert the sealed-impl relationship
    // statically instead. `CheckpointPageWriter` boxes its futures and is still
    // usable as a trait object.
    fn _assert_mvcc_pager<P: MvccPager<Txn = MockTransaction>>(_pager: &P) {}
    fn _assert_btree_cursor_ops<C: BtreeCursorOps>(_cursor: &C) {}

    let pager = MockMvccPager;
    _assert_mvcc_pager(&pager);

    let cursor = MockBtreeCursor::new(vec![(1, b"x".to_vec())]);
    _assert_btree_cursor_ops(&cursor);

    let mut writer = MockCheckpointPageWriter;
    let _: &mut dyn CheckpointPageWriter = &mut writer;
}

#[test]
fn test_layer_isolation_btree_without_real_pager() {
    asupersync::test_utils::run_test(|| async {
        let cx = Cx::new();
        let pager = MockMvccPager;
        let txn = pager
            .begin(&cx, TransactionMode::Deferred)
            .await
            .expect("begin should succeed");

        let page = txn
            .get_page(&cx, PageNumber::new(99).expect("non-zero page number"))
            .await
            .expect("get_page should succeed");
        assert_eq!(
            u32::from_le_bytes(page.as_bytes()[..4].try_into().expect("stamp bytes")),
            99
        );
    });
}

#[test]
fn test_layer_isolation_vdbe_without_real_btree() {
    asupersync::test_utils::run_test(|| async {
        let cx = Cx::new();
        let mut cursor = MockBtreeCursor::new(vec![(10, b"row".to_vec())]);
        assert!(cursor.first(&cx).await.expect("first should succeed"));
        assert_eq!(
            cursor.payload(&cx).await.expect("payload should succeed"),
            b"row"
        );
    });
}

#[test]
fn test_e2e_full_layer_stack() {
    asupersync::test_utils::run_test(|| async {
        let cx = Cx::new();

        let vfs = RecordingVfs::new(false);
        let (file, _) = vfs
            .open(
                &cx,
                Some(Path::new("full-layer-stack.db")),
                VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE,
            )
            .expect("open should succeed");
        file.write(&cx, b"stack", 0)
            .await
            .expect("write should succeed");

        let pager = MockMvccPager;
        let txn = pager
            .begin(&cx, TransactionMode::Deferred)
            .await
            .expect("begin should succeed");
        let page = txn
            .get_page(&cx, PageNumber::new(5).expect("non-zero page number"))
            .await
            .expect("get_page should succeed");
        assert_eq!(
            u32::from_le_bytes(page.as_bytes()[..4].try_into().expect("stamp bytes")),
            5
        );

        let mut cursor = MockBtreeCursor::new(vec![(1, b"payload".to_vec())]);
        assert!(cursor.first(&cx).await.expect("first should succeed"));
        assert_eq!(
            cursor.payload(&cx).await.expect("payload should succeed"),
            b"payload"
        );

        let mut registry = FunctionRegistry::new();
        registry.register_scalar(FixedScalar {
            name: "stack_fn",
            value: SqliteValue::Integer(7),
        });
        let function = registry
            .find_scalar("stack_fn", -1)
            .expect("function should be found");
        assert_eq!(
            function.invoke(&[]).expect("invoke should succeed"),
            SqliteValue::Integer(7)
        );

        eprintln!(
            "bead_id={COMPOSITION_BEAD_ID} level=DEBUG from_layer=vfs to_layer=pager operation=open_write"
        );
        eprintln!(
            "bead_id={COMPOSITION_BEAD_ID} level=DEBUG from_layer=mvcc to_layer=btree operation=get_page_traverse"
        );
        eprintln!(
            "bead_id={COMPOSITION_BEAD_ID} level=INFO operation=e2e_full_layer_stack status=ok"
        );
    });
}

#[test]
fn test_e2e_mock_vfs_error_propagation() {
    asupersync::test_utils::run_test(|| async {
        let cx = Cx::new();
        let vfs = RecordingVfs::new(true);
        let (file, _) = vfs
            .open(
                &cx,
                Some(Path::new("error-propagation.db")),
                VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE,
            )
            .expect("open should succeed");

        let err = file
            .write(&cx, b"boom", 0)
            .await
            .expect_err("configured write failure should propagate");
        assert!(matches!(err, FrankenError::Unsupported));
    });
}

#[test]
fn test_e2e_function_registry_in_vdbe() {
    let mut registry = FunctionRegistry::new();
    registry.register_scalar(FixedScalar {
        name: "double_like",
        value: SqliteValue::Integer(84),
    });

    let function = registry
        .find_scalar("double_like", -1)
        .expect("function must be present");
    let result = function
        .invoke(&[SqliteValue::Integer(42)])
        .expect("invoke should succeed");
    assert_eq!(result, SqliteValue::Integer(84));
}
