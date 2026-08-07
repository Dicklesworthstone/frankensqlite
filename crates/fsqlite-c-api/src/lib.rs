// bd-19u.8: SQLite C API compatibility shim (adoption wedge)
//
// Drop-in replacement for sqlite3_open/close/exec/prepare/step/finalize/column_*
// via C FFI.  Read-only compat is the first milestone; writes via execute() are
// included but the step-based iteration model is the primary focus.
//
// Tracing: span 'compat_api' with fields api_func, duration_us.
// Log level: INFO API calls via compat layer, WARN for unsupported features.
// Metric: fsqlite_compat_api_calls_total counter by api_func.

#![allow(
    unsafe_code,
    unsafe_op_in_unsafe_fn,
    clippy::borrow_as_ptr,
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap
)]

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_double, c_int, c_void};
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI32, AtomicU64, AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use fsqlite::Connection;
use fsqlite_ast::Statement;
use fsqlite_error::{ErrorCode, FrankenError};

/// Drive an engine future to completion on this shim's own runtime.
///
/// The C ABI is a hard synchronous boundary — `extern "C"` functions cannot be
/// `async`, and a C caller has no `Cx` to hand down. So unlike the rest of the
/// workspace (where AGENTS.md forbids building a runtime), this crate MUST own
/// one: it is the consumer on behalf of the C caller.
fn block_on<F: std::future::Future>(future: F) -> F::Output {
    thread_local! {
        static RUNTIME: asupersync::runtime::Runtime =
            asupersync::runtime::RuntimeBuilder::current_thread()
                .build()
                .expect("build fsqlite-c-api runtime");
    }
    RUNTIME.with(|runtime| runtime.block_on(future))
}
use fsqlite_parser::{Parser, parse_first_statement_with_tail};
use fsqlite_types::value::SqliteValue;

// ── SQLite result codes ─────────────────────────────────────────────

pub const SQLITE_OK: c_int = ErrorCode::Ok as c_int;
pub const SQLITE_ERROR: c_int = ErrorCode::Error as c_int;
pub const SQLITE_INTERNAL: c_int = ErrorCode::Internal as c_int;
pub const SQLITE_BUSY: c_int = ErrorCode::Busy as c_int;
pub const SQLITE_NOMEM: c_int = ErrorCode::NoMem as c_int;
pub const SQLITE_READONLY: c_int = ErrorCode::ReadOnly as c_int;
pub const SQLITE_IOERR: c_int = ErrorCode::IoErr as c_int;
pub const SQLITE_CORRUPT: c_int = ErrorCode::Corrupt as c_int;
pub const SQLITE_FULL: c_int = ErrorCode::Full as c_int;
pub const SQLITE_CANTOPEN: c_int = ErrorCode::CantOpen as c_int;
pub const SQLITE_SCHEMA: c_int = ErrorCode::Schema as c_int;
pub const SQLITE_TOOBIG: c_int = ErrorCode::TooBig as c_int;
pub const SQLITE_CONSTRAINT: c_int = ErrorCode::Constraint as c_int;
pub const SQLITE_MISMATCH: c_int = ErrorCode::Mismatch as c_int;
pub const SQLITE_MISUSE: c_int = ErrorCode::Misuse as c_int;
pub const SQLITE_AUTH: c_int = ErrorCode::Auth as c_int;
pub const SQLITE_RANGE: c_int = ErrorCode::Range as c_int;
pub const SQLITE_NOTADB: c_int = ErrorCode::NotADb as c_int;
pub const SQLITE_ROW: c_int = ErrorCode::Row as c_int;
pub const SQLITE_DONE: c_int = ErrorCode::Done as c_int;
pub const SQLITE_ABORT: c_int = ErrorCode::Abort as c_int;

// ── Column type constants ───────────────────────────────────────────

pub const SQLITE_INTEGER: c_int = 1;
pub const SQLITE_FLOAT: c_int = 2;
pub const SQLITE_TEXT: c_int = 3;
pub const SQLITE_BLOB: c_int = 4;
pub const SQLITE_NULL: c_int = 5;

// ── Metrics ─────────────────────────────────────────────────────────

static COMPAT_OPEN: AtomicU64 = AtomicU64::new(0);
static COMPAT_CLOSE: AtomicU64 = AtomicU64::new(0);
static COMPAT_EXEC: AtomicU64 = AtomicU64::new(0);
static COMPAT_PREPARE: AtomicU64 = AtomicU64::new(0);
static COMPAT_STEP: AtomicU64 = AtomicU64::new(0);
static COMPAT_FINALIZE: AtomicU64 = AtomicU64::new(0);
static COMPAT_COLUMN: AtomicU64 = AtomicU64::new(0);
static COMPAT_ERRMSG: AtomicU64 = AtomicU64::new(0);
static TEMP_DB_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone)]
pub struct CompatMetricsSnapshot {
    pub open: u64,
    pub close: u64,
    pub exec: u64,
    pub prepare: u64,
    pub step: u64,
    pub finalize: u64,
    pub column: u64,
    pub errmsg: u64,
}

impl CompatMetricsSnapshot {
    pub fn total(&self) -> u64 {
        self.open
            + self.close
            + self.exec
            + self.prepare
            + self.step
            + self.finalize
            + self.column
            + self.errmsg
    }
}

pub fn compat_metrics_snapshot() -> CompatMetricsSnapshot {
    CompatMetricsSnapshot {
        open: COMPAT_OPEN.load(Ordering::Relaxed),
        close: COMPAT_CLOSE.load(Ordering::Relaxed),
        exec: COMPAT_EXEC.load(Ordering::Relaxed),
        prepare: COMPAT_PREPARE.load(Ordering::Relaxed),
        step: COMPAT_STEP.load(Ordering::Relaxed),
        finalize: COMPAT_FINALIZE.load(Ordering::Relaxed),
        column: COMPAT_COLUMN.load(Ordering::Relaxed),
        errmsg: COMPAT_ERRMSG.load(Ordering::Relaxed),
    }
}

pub fn reset_compat_metrics() {
    COMPAT_OPEN.store(0, Ordering::Relaxed);
    COMPAT_CLOSE.store(0, Ordering::Relaxed);
    COMPAT_EXEC.store(0, Ordering::Relaxed);
    COMPAT_PREPARE.store(0, Ordering::Relaxed);
    COMPAT_STEP.store(0, Ordering::Relaxed);
    COMPAT_FINALIZE.store(0, Ordering::Relaxed);
    COMPAT_COLUMN.store(0, Ordering::Relaxed);
    COMPAT_ERRMSG.store(0, Ordering::Relaxed);
}

// ── Opaque handle types ─────────────────────────────────────────────

const DEFAULT_ERROR_MESSAGE: &str = "not an error";

/// Opaque database connection handle exposed via C FFI.
///
/// Wraps a `Connection` plus the last error message for `sqlite3_errmsg`.
pub struct Sqlite3 {
    conn: Connection,
    temporary_path: Option<PathBuf>,
    last_error: Mutex<CString>,
    last_error_code: AtomicI32,
    last_changes: AtomicI32,
    active_statements: AtomicUsize,
}

impl Sqlite3 {
    fn new(conn: Connection, temporary_path: Option<PathBuf>) -> Self {
        Self {
            conn,
            temporary_path,
            last_error: Mutex::new(CString::new(DEFAULT_ERROR_MESSAGE).expect("static")),
            last_error_code: AtomicI32::new(SQLITE_OK),
            last_changes: AtomicI32::new(0),
            active_statements: AtomicUsize::new(0),
        }
    }

    fn set_error(&self, err: &FrankenError) {
        self.set_error_message_and_code(&err.to_string(), error_to_code(err));
    }

    fn set_error_message_and_code(&self, message: &str, code: c_int) {
        if let Ok(mut guard) = self.last_error.lock() {
            *guard = c_string_truncate_on_nul(message);
        }
        self.last_error_code.store(code, Ordering::Relaxed);
    }

    fn clear_error(&self) {
        if let Ok(mut guard) = self.last_error.lock() {
            *guard = c_string_truncate_on_nul(DEFAULT_ERROR_MESSAGE);
        }
        self.last_error_code.store(SQLITE_OK, Ordering::Relaxed);
    }

    fn refresh_last_changes(&self) {
        let changes = block_on(self.conn.query_row("SELECT changes();"))
            .ok()
            .and_then(|row| match row.get(0) {
                Some(SqliteValue::Integer(n)) => Some(i64_to_c_int_saturating(*n)),
                _ => None,
            })
            .unwrap_or(0);
        self.last_changes.store(changes, Ordering::Relaxed);
    }

    fn register_statement(&self) {
        self.active_statements.fetch_add(1, Ordering::Relaxed);
    }

    fn release_statement(&self) {
        // `try_update` is only stable since Rust 1.95, while this workspace's
        // MSRV is 1.85. Keep the equivalent MSRV-safe operation until the
        // workspace toolchain floor advances.
        #[allow(deprecated)]
        let _ =
            self.active_statements
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                    Some(count.saturating_sub(1))
                });
    }

    fn active_statement_count(&self) -> usize {
        self.active_statements.load(Ordering::Relaxed)
    }
}

fn cleanup_temporary_database_artifacts(path: &Path) {
    let journal_path = {
        let mut jp = path.as_os_str().to_owned();
        jp.push("-journal");
        PathBuf::from(jp)
    };
    let wal_path = {
        let mut wp = path.as_os_str().to_owned();
        wp.push("-wal");
        PathBuf::from(wp)
    };
    let shm_path = {
        let mut sp = path.as_os_str().to_owned();
        sp.push("-shm");
        PathBuf::from(sp)
    };

    for candidate in [path, &journal_path, &wal_path, &shm_path] {
        match std::fs::remove_file(candidate) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => tracing::warn!(
                target: "fsqlite.compat",
                path = %candidate.display(),
                error = %error,
                "failed to remove temporary sqlite3_open database artifact"
            ),
        }
    }
}

enum OpenTarget {
    Path(String),
    Temporary(PathBuf),
}

fn reserve_temporary_database_path() -> std::io::Result<PathBuf> {
    let temp_dir = std::env::temp_dir();
    for _ in 0..32 {
        let counter = TEMP_DB_COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = temp_dir.join(format!(
            "frankensqlite-c-api-{}-{nanos}-{counter}.db",
            std::process::id()
        ));
        match std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
        {
            Ok(_) => return Ok(path),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "failed to reserve unique temporary database path",
    ))
}

/// Opaque prepared statement handle exposed via C FFI.
///
/// Wraps the SQL string, the parent connection, and a row cursor so
/// that `sqlite3_step` can return one row at a time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PreparedStepMode {
    Query,
    Execute,
}

pub struct Sqlite3Stmt {
    db: *mut Sqlite3,
    sql: String,
    step_mode: PreparedStepMode,
    /// Cached rows from last execution.  `None` means not yet stepped.
    rows: Option<Vec<fsqlite::Row>>,
    /// Current row index (0-based, incremented by each `sqlite3_step`).
    cursor: usize,
    /// Whether the most recent `sqlite3_step` returned `SQLITE_ROW`.
    active_row: bool,
    /// Last sqlite3_step-style result code for sqlite3_reset semantics.
    last_step_code: c_int,
    /// Column count from the most recent result set.
    column_count: c_int,
    /// Cached NUL-terminated buffers for coerced text/blob accessors (kept
    /// alive until next step or finalize to satisfy C lifetime expectations,
    /// including values that contain embedded NUL bytes).
    text_cache: Vec<Option<Vec<u8>>>,
}

type ExecCallback = unsafe extern "C" fn(
    parg: *mut c_void,
    ncols: c_int,
    values: *mut *mut c_char,
    names: *mut *mut c_char,
) -> c_int;

// ── Helper: convert FrankenError → c_int ────────────────────────────

fn error_to_code(err: &FrankenError) -> c_int {
    parse_embedded_vdbe_result_code(err).unwrap_or_else(|| err.error_code() as c_int)
}

fn parse_embedded_vdbe_result_code(err: &FrankenError) -> Option<c_int> {
    let FrankenError::Internal(message) = err else {
        return None;
    };
    let suffix = message.strip_prefix("VDBE halted with code ")?;
    let (code_text, _) = suffix.split_once(':')?;
    code_text.parse::<c_int>().ok()
}

fn i64_to_c_int_saturating(value: i64) -> c_int {
    if value < i64::from(c_int::MIN) {
        c_int::MIN
    } else if value > i64::from(c_int::MAX) {
        c_int::MAX
    } else {
        value as c_int
    }
}

fn c_string_truncate_on_nul(value: &str) -> CString {
    let bytes = value.as_bytes();
    let nul_index = bytes
        .iter()
        .position(|&byte| byte == 0)
        .unwrap_or(bytes.len());

    // SAFETY: the prefix before the first NUL byte contains no interior NULs.
    unsafe { CString::from_vec_unchecked(bytes[..nul_index].to_vec()) }
}

fn can_prepare_statement(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::Select(_) | Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_)
    )
}

fn prepared_step_mode(statement: &Statement) -> PreparedStepMode {
    match statement {
        Statement::Insert(stmt) if stmt.returning.is_empty() => PreparedStepMode::Execute,
        Statement::Update(stmt) if stmt.returning.is_empty() => PreparedStepMode::Execute,
        Statement::Delete(stmt) if stmt.returning.is_empty() => PreparedStepMode::Execute,
        _ => PreparedStepMode::Query,
    }
}

struct PreparedSqlInfo {
    consumed_sql: String,
    tail_offset: usize,
    step_mode: PreparedStepMode,
    column_count: c_int,
}

fn validate_and_classify_prepared_sql(
    conn: &Connection,
    sql: &str,
) -> Result<Option<PreparedSqlInfo>, FrankenError> {
    let Some((statement, tail_offset)) =
        parse_first_statement_with_tail(sql).map_err(|err| FrankenError::ParseError {
            offset: err.span.start as usize,
            detail: err.message,
        })?
    else {
        return Ok(None);
    };
    let consumed_sql = &sql[..tail_offset];
    let step_mode = prepared_step_mode(&statement);
    let column_count = if can_prepare_statement(&statement) {
        let prepared = block_on(conn.prepare(consumed_sql))?;
        c_int::try_from(prepared.column_count()).unwrap_or(c_int::MAX)
    } else {
        0
    };

    Ok(Some(PreparedSqlInfo {
        consumed_sql: consumed_sql.to_owned(),
        tail_offset,
        step_mode,
        column_count,
    }))
}

fn first_statement_tail_offset(sql: &str) -> Result<Option<usize>, FrankenError> {
    parse_first_statement_with_tail(sql)
        .map_err(|err| FrankenError::ParseError {
            offset: err.span.start as usize,
            detail: err.message,
        })
        .map(|parsed| parsed.map(|(_, tail_offset)| tail_offset))
}

fn best_effort_exec_callback_column_names(conn: &Connection, sql: &str) -> Option<Vec<String>> {
    let mut parser = Parser::from_sql(sql);
    let (statements, errors) = parser.parse_all();
    if let Some(error) = errors.into_iter().next() {
        tracing::warn!(
            target: "fsqlite.compat",
            error = %error,
            "failed to recover sqlite3_exec callback column names"
        );
        return None;
    }

    let statement = statements.last()?;
    if !can_prepare_statement(statement) {
        return None;
    }

    match block_on(conn.prepare(&statement.to_string())) {
        Ok(prepared) => Some(prepared.column_names().to_vec()),
        Err(error) => {
            tracing::warn!(
                target: "fsqlite.compat",
                error = %error,
                "failed to prepare sqlite3_exec callback column metadata"
            );
            None
        }
    }
}

unsafe fn emit_exec_callback_rows(
    handle: &Sqlite3,
    sql: &str,
    rows: &[fsqlite::Row],
    callback: ExecCallback,
    parg: *mut c_void,
    errmsg: *mut *mut c_char,
) -> c_int {
    let callback_column_names = (!rows.is_empty())
        .then(|| best_effort_exec_callback_column_names(&handle.conn, sql))
        .flatten();
    for row in rows {
        let vals = row.values();
        let ncols = vals.len() as c_int;

        let mut c_values: Vec<*mut c_char> = Vec::with_capacity(vals.len());
        let mut c_names: Vec<*mut c_char> = Vec::with_capacity(vals.len());
        let mut owned_vals: Vec<Option<Vec<u8>>> = Vec::with_capacity(vals.len());
        let mut owned_names: Vec<CString> = Vec::with_capacity(vals.len());

        for (i, v) in vals.iter().enumerate() {
            let col_name = callback_column_names
                .as_ref()
                .and_then(|names| names.get(i))
                .cloned()
                .unwrap_or_else(|| format!("column{i}"));
            let cname = c_string_truncate_on_nul(&col_name);
            c_names.push(cname.as_ptr().cast_mut());
            owned_names.push(cname);

            if matches!(v, SqliteValue::Null) {
                c_values.push(std::ptr::null_mut());
                owned_vals.push(None);
                continue;
            }
            let mut text = sqlite_value_to_callback_bytes(v);
            c_values.push(text.as_mut_ptr().cast());
            owned_vals.push(Some(text));
        }

        debug_assert_eq!(owned_vals.len(), c_values.len());
        debug_assert_eq!(owned_names.len(), c_names.len());
        let rc = callback(parg, ncols, c_values.as_mut_ptr(), c_names.as_mut_ptr());
        if rc != SQLITE_OK {
            let err = FrankenError::Abort;
            handle.set_error(&err);
            write_error_message(errmsg, &err.to_string());
            return SQLITE_ABORT;
        }
    }
    SQLITE_OK
}

fn sqlite_value_to_callback_bytes(value: &SqliteValue) -> Vec<u8> {
    let mut bytes = sqlite_value_to_text_bytes(value);
    bytes.push(0);
    bytes
}

unsafe fn cache_stmt_text_bytes(stmt: *mut Sqlite3Stmt, i_col: c_int, text: Vec<u8>) -> *const u8 {
    let s = &mut *stmt;
    let mut cached = text;
    cached.push(0);

    if (i_col as usize) < s.text_cache.len() {
        s.text_cache[i_col as usize] = Some(cached);
        return s.text_cache[i_col as usize]
            .as_ref()
            .map_or(std::ptr::null(), Vec::as_ptr);
    }

    while s.text_cache.len() <= i_col as usize {
        s.text_cache.push(None);
    }
    s.text_cache[i_col as usize] = Some(cached);
    s.text_cache[i_col as usize]
        .as_ref()
        .map_or(std::ptr::null(), Vec::as_ptr)
}

fn sqlite_value_to_text_bytes(value: &SqliteValue) -> Vec<u8> {
    match value {
        SqliteValue::Null => Vec::new(),
        SqliteValue::Integer(number) => number.to_string().into_bytes(),
        value @ SqliteValue::Float(_) => value.to_text().into_bytes(),
        SqliteValue::Text(text) => text.as_bytes_direct().to_vec(),
        SqliteValue::Blob(bytes) => bytes.to_vec(),
    }
}

unsafe fn execute_exec_batch(
    handle: &Sqlite3,
    sql: &str,
    callback: Option<ExecCallback>,
    parg: *mut c_void,
    errmsg: *mut *mut c_char,
) -> c_int {
    let mut remaining = sql;

    loop {
        let trimmed = remaining.trim_start();
        if trimmed.is_empty() {
            handle.clear_error();
            handle.refresh_last_changes();
            return SQLITE_OK;
        }
        let statement_offset = sql.len().saturating_sub(trimmed.len());

        let tail_offset = match first_statement_tail_offset(trimmed) {
            Ok(Some(tail_offset)) => tail_offset,
            Ok(None) => {
                handle.clear_error();
                handle.refresh_last_changes();
                return SQLITE_OK;
            }
            Err(FrankenError::ParseError { offset, detail }) => {
                let error = FrankenError::ParseError {
                    offset: statement_offset.saturating_add(offset),
                    detail,
                };
                tracing::warn!(
                    target: "fsqlite.compat",
                    error = %error,
                    "sqlite3_exec failed while parsing statement batch"
                );
                handle.set_error(&error);
                write_error_message(errmsg, &error.to_string());
                return error_to_code(&error);
            }
            Err(error) => {
                tracing::warn!(
                    target: "fsqlite.compat",
                    error = %error,
                    "sqlite3_exec failed while parsing statement batch"
                );
                handle.set_error(&error);
                write_error_message(errmsg, &error.to_string());
                return error_to_code(&error);
            }
        };

        let statement_sql = &trimmed[..tail_offset];
        let rows = match block_on(handle.conn.query(statement_sql)) {
            Ok(rows) => rows,
            Err(FrankenError::QueryReturnedNoRows) => Vec::new(),
            Err(error) => {
                tracing::warn!(
                    target: "fsqlite.compat",
                    error = %error,
                    statement_sql = %statement_sql,
                    "sqlite3_exec failed while executing statement batch"
                );
                handle.set_error(&error);
                write_error_message(errmsg, &error.to_string());
                return error_to_code(&error);
            }
        };

        if let Some(cb) = callback {
            let rc = emit_exec_callback_rows(handle, statement_sql, &rows, cb, parg, errmsg);
            if rc != SQLITE_OK {
                return rc;
            }
        }

        remaining = &trimmed[tail_offset..];
    }
}

unsafe fn write_error_message(errmsg: *mut *mut c_char, message: &str) {
    if errmsg.is_null() {
        return;
    }
    let cmsg = c_string_truncate_on_nul(message);
    let len = cmsg.as_bytes_with_nul().len();
    let buf = libc_malloc(len);
    if !buf.is_null() {
        std::ptr::copy_nonoverlapping(cmsg.as_ptr(), buf.cast(), len);
        *errmsg = buf.cast();
    }
}

// ── sqlite3_open ────────────────────────────────────────────────────

/// Open a new database connection.
///
/// # Safety
/// `filename` must be a valid null-terminated C string (or null for `:memory:`).
/// `pp_db` must point to a valid `*mut Sqlite3` location.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_open(filename: *const c_char, pp_db: *mut *mut Sqlite3) -> c_int {
    COMPAT_OPEN.fetch_add(1, Ordering::Relaxed);
    let _span = tracing::info_span!("compat_api", api_func = "open").entered();

    if pp_db.is_null() {
        return SQLITE_MISUSE;
    }

    let open_target = if filename.is_null() {
        OpenTarget::Path(":memory:".to_owned())
    } else if let Ok(s) = CStr::from_ptr(filename).to_str() {
        if s.is_empty() {
            match reserve_temporary_database_path() {
                Ok(path) => OpenTarget::Temporary(path),
                Err(error) => {
                    tracing::warn!(
                        target: "fsqlite.compat",
                        error = %error,
                        "sqlite3_open failed to reserve temporary database path"
                    );
                    *pp_db = std::ptr::null_mut();
                    return SQLITE_CANTOPEN;
                }
            }
        } else {
            OpenTarget::Path(s.to_owned())
        }
    } else {
        *pp_db = std::ptr::null_mut();
        return SQLITE_CANTOPEN;
    };

    let (path, temporary_path) = match open_target {
        OpenTarget::Path(path) => (path, None),
        OpenTarget::Temporary(path) => {
            let path_str = if let Some(path_str) = path.to_str() {
                path_str.to_owned()
            } else {
                let _ = std::fs::remove_file(&path);
                *pp_db = std::ptr::null_mut();
                return SQLITE_CANTOPEN;
            };
            (path_str, Some(path))
        }
    };

    tracing::info!(target: "fsqlite.compat", path = %path, "sqlite3_open");

    let open_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        block_on(Connection::open(&path))
    }));

    match open_result {
        Ok(Ok(conn)) => {
            let handle = Box::new(Sqlite3::new(conn, temporary_path));
            *pp_db = Box::into_raw(handle);
            SQLITE_OK
        }
        Ok(Err(e)) => {
            tracing::warn!(target: "fsqlite.compat", error = %e, "sqlite3_open failed");
            if let Some(path) = temporary_path {
                let _ = std::fs::remove_file(path);
            }
            *pp_db = std::ptr::null_mut();
            error_to_code(&e)
        }
        Err(_) => {
            tracing::error!(target: "fsqlite.compat", path = %path, "sqlite3_open panicked");
            if let Some(path) = temporary_path {
                let _ = std::fs::remove_file(path);
            }
            *pp_db = std::ptr::null_mut();
            SQLITE_ERROR
        }
    }
}

// ── sqlite3_close ───────────────────────────────────────────────────

/// Close a database connection.
///
/// # Safety
/// `db` must have been obtained from `sqlite3_open` and must not be used
/// after this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_close(db: *mut Sqlite3) -> c_int {
    COMPAT_CLOSE.fetch_add(1, Ordering::Relaxed);
    let _span = tracing::info_span!("compat_api", api_func = "close").entered();

    if db.is_null() {
        return SQLITE_OK;
    }

    tracing::info!(target: "fsqlite.compat", "sqlite3_close");

    let mut handle = Box::from_raw(db);
    if handle.active_statement_count() != 0 {
        handle.set_error_message_and_code(
            "unable to close due to unfinalized statements",
            SQLITE_BUSY,
        );
        let _ = Box::into_raw(handle);
        return SQLITE_BUSY;
    }

    let close_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        block_on(handle.conn.close_in_place())
    }));

    match close_result {
        Ok(Ok(())) => {
            let temporary_path = handle.temporary_path.clone();
            drop(handle);
            if let Some(path) = temporary_path.as_deref() {
                cleanup_temporary_database_artifacts(path);
            }
            SQLITE_OK
        }
        Ok(Err(e)) => {
            tracing::warn!(target: "fsqlite.compat", error = %e, "sqlite3_close failed");
            handle.set_error(&e);
            let code = error_to_code(&e);
            let _ = Box::into_raw(handle);
            code
        }
        Err(_) => {
            tracing::error!(target: "fsqlite.compat", "sqlite3_close panicked");
            let _ = Box::into_raw(handle); // leak it to prevent further panics
            SQLITE_ERROR
        }
    }
}

// ── sqlite3_exec ────────────────────────────────────────────────────

/// Execute one or more SQL statements.
///
/// # Safety
/// - `db` must be a valid handle from `sqlite3_open`.
/// - `sql` must be a valid null-terminated C string.
/// - `callback` may be null.  If non-null, it is invoked for each result row.
/// - `errmsg` may be null.  If non-null and an error occurs, it is set to a
///   malloc'd string that the caller must free with `sqlite3_free`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_exec(
    db: *mut Sqlite3,
    sql: *const c_char,
    callback: Option<ExecCallback>,
    parg: *mut c_void,
    errmsg: *mut *mut c_char,
) -> c_int {
    COMPAT_EXEC.fetch_add(1, Ordering::Relaxed);
    let _span = tracing::info_span!("compat_api", api_func = "exec").entered();

    if db.is_null() || sql.is_null() {
        return SQLITE_MISUSE;
    }

    if !errmsg.is_null() {
        *errmsg = std::ptr::null_mut();
    }

    let handle = &*db;
    let Ok(sql_str) = CStr::from_ptr(sql).to_str() else {
        let err = FrankenError::ParseError {
            offset: 0,
            detail: "SQL text is not valid UTF-8".to_owned(),
        };
        handle.set_error(&err);
        write_error_message(errmsg, &err.to_string());
        return error_to_code(&err);
    };

    tracing::info!(target: "fsqlite.compat", sql = %sql_str, "sqlite3_exec");

    let exec_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        execute_exec_batch(handle, sql_str, callback, parg, errmsg)
    }));

    if let Ok(rc) = exec_result {
        rc
    } else {
        let e = FrankenError::Internal("Rust panic during sqlite3_exec".to_owned());
        tracing::error!(target: "fsqlite.compat", error = %e, "sqlite3_exec panicked");
        handle.set_error(&e);
        write_error_message(errmsg, &e.to_string());
        error_to_code(&e)
    }
}

/// Free memory allocated by this library (for errmsg from `sqlite3_exec`).
///
/// # Safety
/// `ptr` must have been allocated by this library or be null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_free(ptr: *mut c_void) {
    if ptr.is_null() {
        return;
    }
    // We use a simple Vec<u8> allocation strategy: find the length and dealloc.
    // Since we allocated via libc malloc in sqlite3_exec, use libc free.
    libc_free(ptr);
}

// Thin wrappers around libc malloc/free so we don't depend on the libc crate
// directly (it's in the workspace via nix but we keep the surface minimal).
const ALLOC_HEADER_SIZE: usize = std::mem::size_of::<usize>();
const ALLOC_HEADER_ALIGN: usize = std::mem::align_of::<usize>();

unsafe fn libc_malloc(size: usize) -> *mut u8 {
    // std::alloc requires the exact Layout for deallocation. Since sqlite3_free
    // doesn't receive the size, we must prefix the allocation with its size.
    let Ok(layout) =
        std::alloc::Layout::from_size_align(size + ALLOC_HEADER_SIZE, ALLOC_HEADER_ALIGN)
    else {
        return std::ptr::null_mut();
    };
    let ptr = std::alloc::alloc(layout);
    if ptr.is_null() {
        return ptr;
    }
    let size_bytes = size.to_ne_bytes();
    std::ptr::copy_nonoverlapping(size_bytes.as_ptr(), ptr, ALLOC_HEADER_SIZE);
    ptr.add(ALLOC_HEADER_SIZE)
}

unsafe fn libc_free(ptr: *mut c_void) {
    if ptr.is_null() {
        return;
    }
    let real_ptr = ptr.cast::<u8>().sub(ALLOC_HEADER_SIZE);
    let mut size_bytes = [0_u8; ALLOC_HEADER_SIZE];
    std::ptr::copy_nonoverlapping(real_ptr, size_bytes.as_mut_ptr(), ALLOC_HEADER_SIZE);
    let size = usize::from_ne_bytes(size_bytes);
    if let Ok(layout) =
        std::alloc::Layout::from_size_align(size + ALLOC_HEADER_SIZE, ALLOC_HEADER_ALIGN)
    {
        std::alloc::dealloc(real_ptr, layout);
    }
}

// ── sqlite3_prepare_v2 ─────────────────────────────────────────────

/// Compile an SQL statement.
///
/// # Safety
/// - `db` must be a valid handle.
/// - `sql` must be a valid UTF-8 C string with at least `n_byte` bytes
///   (or null-terminated if `n_byte` < 0).
/// - `pp_stmt` must point to a valid `*mut Sqlite3Stmt` location.
/// - `pz_tail` may be null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_prepare_v2(
    db: *mut Sqlite3,
    sql: *const c_char,
    n_byte: c_int,
    pp_stmt: *mut *mut Sqlite3Stmt,
    pz_tail: *mut *const c_char,
) -> c_int {
    COMPAT_PREPARE.fetch_add(1, Ordering::Relaxed);
    let _span = tracing::info_span!("compat_api", api_func = "prepare_v2").entered();

    if db.is_null() || sql.is_null() || pp_stmt.is_null() {
        return SQLITE_MISUSE;
    }

    *pp_stmt = std::ptr::null_mut();
    if !pz_tail.is_null() {
        *pz_tail = std::ptr::null();
    }

    let handle = &*db;
    let source = if n_byte < 0 {
        if let Ok(s) = CStr::from_ptr(sql).to_str() {
            s
        } else {
            let err = FrankenError::ParseError {
                offset: 0,
                detail: "SQL text is not valid UTF-8".to_owned(),
            };
            handle.set_error(&err);
            return error_to_code(&err);
        }
    } else {
        let slice = std::slice::from_raw_parts(sql.cast::<u8>(), n_byte as usize);
        let end = slice.iter().position(|&b| b == 0).unwrap_or(slice.len());
        if let Ok(s) = std::str::from_utf8(&slice[..end]) {
            s
        } else {
            let err = FrankenError::ParseError {
                offset: 0,
                detail: "SQL text is not valid UTF-8".to_owned(),
            };
            handle.set_error(&err);
            return error_to_code(&err);
        }
    };

    let source_len = source.len();

    let compile_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        validate_and_classify_prepared_sql(&handle.conn, source)
    }));

    match compile_result {
        Ok(Ok(Some(info))) => {
            tracing::info!(
                target: "fsqlite.compat",
                sql = %info.consumed_sql,
                "sqlite3_prepare_v2"
            );
            handle.clear_error();
            let stmt = Box::new(Sqlite3Stmt {
                db,
                sql: info.consumed_sql,
                step_mode: info.step_mode,
                rows: None,
                cursor: 0,
                active_row: false,
                last_step_code: SQLITE_OK,
                column_count: info.column_count,
                text_cache: Vec::new(),
            });
            handle.register_statement();
            *pp_stmt = Box::into_raw(stmt);
            if !pz_tail.is_null() {
                *pz_tail = sql.add(info.tail_offset);
            }
            SQLITE_OK
        }
        Ok(Ok(None)) => {
            handle.clear_error();
            if !pz_tail.is_null() {
                *pz_tail = sql.add(source_len);
            }
            SQLITE_OK
        }
        Ok(Err(e)) => {
            tracing::warn!(target: "fsqlite.compat", error = %e, "sqlite3_prepare_v2 failed");
            handle.set_error(&e);
            error_to_code(&e)
        }
        Err(_) => {
            let e = FrankenError::Internal("Rust panic during sqlite3_prepare_v2".to_owned());
            tracing::error!(target: "fsqlite.compat", error = %e, "sqlite3_prepare_v2 panicked");
            handle.set_error(&e);
            error_to_code(&e)
        }
    }
}

// ── sqlite3_step ────────────────────────────────────────────────────

/// Step through a prepared statement.
///
/// Returns `SQLITE_ROW` when a result row is available, `SQLITE_DONE`
/// when execution is complete.
///
/// # Safety
/// `stmt` must be a valid handle from `sqlite3_prepare_v2`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_step(stmt: *mut Sqlite3Stmt) -> c_int {
    COMPAT_STEP.fetch_add(1, Ordering::Relaxed);
    let _span = tracing::info_span!("compat_api", api_func = "step").entered();

    if stmt.is_null() {
        return SQLITE_MISUSE;
    }

    let s = &mut *stmt;
    let db = &*s.db;

    // First call: execute the query and cache all rows.
    if s.rows.is_none() {
        tracing::info!(target: "fsqlite.compat", sql = %s.sql, "sqlite3_step (first call)");

        let execute_result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match s.step_mode {
                PreparedStepMode::Query => block_on(db.conn.query(&s.sql)).map(Some),
                PreparedStepMode::Execute => block_on(db.conn.execute(&s.sql)).map(|_| None),
            }));

        match execute_result {
            Ok(Ok(Some(rows))) => {
                db.clear_error();
                db.refresh_last_changes();
                if s.column_count == 0
                    && let Some(first) = rows.first()
                {
                    s.column_count = first.values().len() as c_int;
                }
                s.rows = Some(rows);
                s.cursor = 0;
                s.active_row = false;
                s.last_step_code = SQLITE_OK;
                s.text_cache.clear();
            }
            Ok(Ok(None)) => {
                db.clear_error();
                db.refresh_last_changes();
                s.rows = Some(Vec::new());
                s.cursor = 0;
                s.active_row = false;
                s.last_step_code = SQLITE_OK;
                s.text_cache.clear();
            }
            Ok(Err(ref e)) if matches!(e, FrankenError::QueryReturnedNoRows) => {
                db.clear_error();
                db.refresh_last_changes();
                s.rows = Some(Vec::new());
                s.cursor = 0;
                s.active_row = false;
                s.last_step_code = SQLITE_OK;
                s.text_cache.clear();
            }
            Ok(Err(e)) => {
                tracing::warn!(target: "fsqlite.compat", error = %e, "sqlite3_step failed");
                s.active_row = false;
                s.text_cache.clear();
                let code = error_to_code(&e);
                s.last_step_code = code;
                db.set_error(&e);
                return code;
            }
            Err(_) => {
                let e = FrankenError::Internal("Rust panic during statement execution".to_owned());
                tracing::error!(target: "fsqlite.compat", error = %e, "sqlite3_step panicked");
                s.active_row = false;
                s.text_cache.clear();
                let code = error_to_code(&e);
                s.last_step_code = code;
                db.set_error(&e);
                return code;
            }
        }
    }

    // Advance cursor.
    if let Some(ref rows) = s.rows {
        if s.cursor < rows.len() {
            // Clear text cache for this row.
            let ncols = if rows.is_empty() {
                0
            } else {
                rows[s.cursor].values().len()
            };
            s.text_cache = vec![None; ncols];

            s.cursor += 1;
            s.active_row = true;
            s.last_step_code = SQLITE_ROW;
            SQLITE_ROW
        } else {
            s.active_row = false;
            s.last_step_code = SQLITE_DONE;
            s.text_cache.clear();
            SQLITE_DONE
        }
    } else {
        s.active_row = false;
        s.last_step_code = SQLITE_DONE;
        s.text_cache.clear();
        SQLITE_DONE
    }
}

// ── sqlite3_finalize ────────────────────────────────────────────────

/// Destroy a prepared statement.
///
/// # Safety
/// `stmt` must have been obtained from `sqlite3_prepare_v2` and must not
/// be used after this call.  Passing null is safe (no-op).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_finalize(stmt: *mut Sqlite3Stmt) -> c_int {
    COMPAT_FINALIZE.fetch_add(1, Ordering::Relaxed);
    let _span = tracing::info_span!("compat_api", api_func = "finalize").entered();

    if stmt.is_null() {
        return SQLITE_OK;
    }

    tracing::info!(target: "fsqlite.compat", "sqlite3_finalize");

    let stmt = Box::from_raw(stmt);
    let rc = match stmt.last_step_code {
        SQLITE_ROW | SQLITE_DONE => SQLITE_OK,
        code => code,
    };
    if !stmt.db.is_null() {
        (&*stmt.db).release_statement();
    }
    drop(stmt);
    rc
}

// ── sqlite3_reset ───────────────────────────────────────────────────

/// Reset a prepared statement so it can be stepped again.
///
/// # Safety
/// `stmt` must be a valid handle from `sqlite3_prepare_v2`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_reset(stmt: *mut Sqlite3Stmt) -> c_int {
    if stmt.is_null() {
        return SQLITE_MISUSE;
    }

    let s = &mut *stmt;
    let rc = match s.last_step_code {
        SQLITE_ROW | SQLITE_DONE => SQLITE_OK,
        code => code,
    };
    s.rows = None;
    s.cursor = 0;
    s.active_row = false;
    s.last_step_code = SQLITE_OK;
    s.text_cache.clear();
    rc
}

// ── Column accessors ────────────────────────────────────────────────

/// Return a reference to the current row's value at column `i_col`, or None if out of bounds.
unsafe fn current_value_ref<'a>(stmt: *const Sqlite3Stmt, i_col: c_int) -> Option<&'a SqliteValue> {
    if i_col < 0 {
        return None;
    }
    let s = &*stmt;
    if !s.active_row {
        return None;
    }
    let rows = s.rows.as_ref()?;
    let row_idx = s.cursor.checked_sub(1)?;
    let row = rows.get(row_idx)?;
    row.get(i_col as usize)
}

/// Number of columns in the result set.
///
/// # Safety
/// `stmt` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_column_count(stmt: *mut Sqlite3Stmt) -> c_int {
    COMPAT_COLUMN.fetch_add(1, Ordering::Relaxed);

    if stmt.is_null() {
        return 0;
    }

    (*stmt).column_count
}

/// Type of value in column `i_col` of the current row.
///
/// # Safety
/// `stmt` must be a valid handle, and a row must be available via `sqlite3_step`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_column_type(stmt: *mut Sqlite3Stmt, i_col: c_int) -> c_int {
    COMPAT_COLUMN.fetch_add(1, Ordering::Relaxed);

    match current_value_ref(stmt, i_col) {
        Some(SqliteValue::Integer(_)) => SQLITE_INTEGER,
        Some(SqliteValue::Float(_)) => SQLITE_FLOAT,
        Some(SqliteValue::Text(_)) => SQLITE_TEXT,
        Some(SqliteValue::Blob(_)) => SQLITE_BLOB,
        Some(SqliteValue::Null) | None => SQLITE_NULL,
    }
}

/// Get an integer value from column `i_col`.
///
/// # Safety
/// `stmt` must be a valid handle with an active row.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_column_int64(stmt: *mut Sqlite3Stmt, i_col: c_int) -> i64 {
    COMPAT_COLUMN.fetch_add(1, Ordering::Relaxed);

    match current_value_ref(stmt, i_col) {
        Some(v) => v.to_integer(),
        None => 0,
    }
}

/// Get a 32-bit integer value from column `i_col`.
///
/// # Safety
/// `stmt` must be a valid handle with an active row.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_column_int(stmt: *mut Sqlite3Stmt, i_col: c_int) -> c_int {
    COMPAT_COLUMN.fetch_add(1, Ordering::Relaxed);

    sqlite3_column_int64(stmt, i_col) as c_int
}

/// Get a double value from column `i_col`.
///
/// # Safety
/// `stmt` must be a valid handle with an active row.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_column_double(stmt: *mut Sqlite3Stmt, i_col: c_int) -> c_double {
    COMPAT_COLUMN.fetch_add(1, Ordering::Relaxed);

    match current_value_ref(stmt, i_col) {
        Some(v) => v.to_float(),
        None => 0.0,
    }
}

/// Get a text pointer from column `i_col`.
///
/// The returned pointer is valid until the next `sqlite3_step`,
/// `sqlite3_reset`, or `sqlite3_finalize` on this statement.
///
/// # Safety
/// `stmt` must be a valid handle with an active row.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_column_text(
    stmt: *mut Sqlite3Stmt,
    i_col: c_int,
) -> *const c_char {
    COMPAT_COLUMN.fetch_add(1, Ordering::Relaxed);

    if stmt.is_null() {
        return std::ptr::null();
    }

    let text = match current_value_ref(stmt, i_col) {
        Some(SqliteValue::Null) | None => return std::ptr::null(),
        Some(value) => sqlite_value_to_text_bytes(value),
    };

    cache_stmt_text_bytes(stmt, i_col, text).cast()
}

/// Get a blob pointer from column `i_col`.
///
/// The returned pointer is valid until the next `sqlite3_step`,
/// `sqlite3_reset`, or `sqlite3_finalize` on this statement.
///
/// # Safety
/// `stmt` must be a valid handle with an active row.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_column_blob(
    stmt: *mut Sqlite3Stmt,
    i_col: c_int,
) -> *const c_void {
    COMPAT_COLUMN.fetch_add(1, Ordering::Relaxed);

    if stmt.is_null() {
        return std::ptr::null();
    }

    match current_value_ref(stmt, i_col) {
        Some(SqliteValue::Blob(b)) => {
            if b.is_empty() {
                std::ptr::null()
            } else {
                b.as_ptr().cast()
            }
        }
        Some(SqliteValue::Text(s)) => {
            if s.is_empty() {
                std::ptr::null()
            } else {
                s.as_ptr().cast()
            }
        }
        Some(value @ (SqliteValue::Integer(_) | SqliteValue::Float(_))) => {
            cache_stmt_text_bytes(stmt, i_col, sqlite_value_to_text_bytes(value)).cast()
        }
        _ => std::ptr::null(),
    }
}

/// Get the byte size of a blob or text value in column `i_col`.
///
/// # Safety
/// `stmt` must be a valid handle with an active row.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_column_bytes(stmt: *mut Sqlite3Stmt, i_col: c_int) -> c_int {
    COMPAT_COLUMN.fetch_add(1, Ordering::Relaxed);

    if stmt.is_null() || i_col < 0 {
        return 0;
    }

    let s = &*stmt;
    if !s.active_row {
        return 0;
    }
    if let Some(Some(text)) = s.text_cache.get(i_col as usize) {
        return c_int::try_from(text.len().saturating_sub(1)).unwrap_or(c_int::MAX);
    }

    match current_value_ref(stmt, i_col) {
        Some(SqliteValue::Blob(b)) => b.len() as c_int,
        Some(SqliteValue::Text(s)) => s.len() as c_int,
        // C SQLite coerces to text and returns the byte length.
        Some(v @ (SqliteValue::Integer(_) | SqliteValue::Float(_))) => v.to_text().len() as c_int,
        _ => 0,
    }
}

// ── sqlite3_errmsg / sqlite3_errcode ────────────────────────────────

/// Get the most recent error message.
///
/// # Safety
/// `db` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_errmsg(db: *mut Sqlite3) -> *const c_char {
    static DEFAULT_MSG: LazyLock<CString> =
        LazyLock::new(|| CString::new(DEFAULT_ERROR_MESSAGE).expect("static"));

    COMPAT_ERRMSG.fetch_add(1, Ordering::Relaxed);

    if db.is_null() {
        return DEFAULT_MSG.as_ptr();
    }

    let handle = &*db;
    match handle.last_error.lock() {
        Ok(guard) => guard.as_ptr(),
        Err(_) => DEFAULT_MSG.as_ptr(),
    }
}

/// Get the most recent error code.
///
/// # Safety
/// `db` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_errcode(db: *mut Sqlite3) -> c_int {
    if db.is_null() {
        return SQLITE_OK;
    }
    (&*db).last_error_code.load(Ordering::Relaxed)
}

// ── sqlite3_changes ─────────────────────────────────────────────────

/// Return the number of rows modified by the most recent INSERT/UPDATE/DELETE.
///
/// # Safety
/// `db` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_changes(db: *mut Sqlite3) -> c_int {
    if db.is_null() {
        return 0;
    }
    (&*db).last_changes.load(Ordering::Relaxed)
}

// ── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;
    use std::ptr;

    /// Helper: open an in-memory database via C API.
    unsafe fn open_memory() -> *mut Sqlite3 {
        let mut db: *mut Sqlite3 = ptr::null_mut();
        let path = CString::new(":memory:").unwrap();
        let rc = sqlite3_open(path.as_ptr(), &mut db);
        assert_eq!(rc, SQLITE_OK);
        assert!(!db.is_null());
        db
    }

    #[test]
    fn test_open_close_memory() {
        unsafe {
            let db = open_memory();
            let rc = sqlite3_close(db);
            assert_eq!(rc, SQLITE_OK);
        }
    }

    #[test]
    fn test_c_string_truncate_on_nul_discards_suffix() {
        let value = c_string_truncate_on_nul("alpha\0beta");
        assert_eq!(value.as_bytes(), b"alpha");
    }

    #[test]
    fn test_open_null_filename() {
        unsafe {
            let mut db: *mut Sqlite3 = ptr::null_mut();
            let rc = sqlite3_open(ptr::null(), &mut db);
            assert_eq!(rc, SQLITE_OK);
            assert!(!db.is_null());
            sqlite3_close(db);
        }
    }

    #[test]
    fn test_open_empty_filename() {
        unsafe {
            let mut db: *mut Sqlite3 = ptr::null_mut();
            let path = CString::new("").unwrap();
            let rc = sqlite3_open(path.as_ptr(), &mut db);
            assert_eq!(rc, SQLITE_OK);
            assert!(!db.is_null());
            let temp_path = (&*db)
                .temporary_path
                .clone()
                .expect("empty filename should create a temporary database");
            assert!(
                temp_path.exists(),
                "temporary database file should exist while handle is open"
            );
            assert_eq!(sqlite3_close(db), SQLITE_OK);
            assert!(
                !temp_path.exists(),
                "temporary database file should be removed on close"
            );
        }
    }

    #[test]
    fn test_cleanup_temporary_database_artifacts_removes_sidecars() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("temp.db");
        let journal_path = PathBuf::from(format!("{}-journal", path.display()));
        let wal_path = PathBuf::from(format!("{}-wal", path.display()));
        let shm_path = PathBuf::from(format!("{}-shm", path.display()));

        std::fs::write(&path, b"db").unwrap();
        std::fs::write(&journal_path, b"journal").unwrap();
        std::fs::write(&wal_path, b"wal").unwrap();
        std::fs::write(&shm_path, b"shm").unwrap();

        cleanup_temporary_database_artifacts(&path);

        assert!(!path.exists());
        assert!(!journal_path.exists());
        assert!(!wal_path.exists());
        assert!(!shm_path.exists());
    }

    #[test]
    fn test_close_null() {
        unsafe {
            let rc = sqlite3_close(ptr::null_mut());
            assert_eq!(rc, SQLITE_OK);
        }
    }

    #[test]
    fn test_exec_create_insert_select() {
        unsafe {
            unsafe extern "C" fn count_cb(
                parg: *mut c_void,
                _ncols: c_int,
                _values: *mut *mut c_char,
                _names: *mut *mut c_char,
            ) -> c_int {
                let counter = &*(parg.cast::<AtomicU64>());
                counter.fetch_add(1, Ordering::Relaxed);
                0
            }

            let db = open_memory();

            let sql = CString::new("CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT);").unwrap();
            let rc = sqlite3_exec(db, sql.as_ptr(), None, ptr::null_mut(), ptr::null_mut());
            assert_eq!(rc, SQLITE_OK);

            let sql = CString::new("INSERT INTO t1 VALUES(1, 'alice');").unwrap();
            let rc = sqlite3_exec(db, sql.as_ptr(), None, ptr::null_mut(), ptr::null_mut());
            assert_eq!(rc, SQLITE_OK);

            let sql = CString::new("INSERT INTO t1 VALUES(2, 'bob');").unwrap();
            let rc = sqlite3_exec(db, sql.as_ptr(), None, ptr::null_mut(), ptr::null_mut());
            assert_eq!(rc, SQLITE_OK);

            // SELECT with callback: pass counter through parg.
            let row_count = AtomicU64::new(0);
            let sql = CString::new("SELECT * FROM t1;").unwrap();
            let rc = sqlite3_exec(
                db,
                sql.as_ptr(),
                Some(count_cb),
                std::ptr::from_ref::<AtomicU64>(&row_count)
                    .cast_mut()
                    .cast(),
                ptr::null_mut(),
            );
            assert_eq!(rc, SQLITE_OK);
            assert_eq!(row_count.load(Ordering::Relaxed), 2);

            sqlite3_close(db);
        }
    }

    #[test]
    fn test_exec_error_sets_errmsg() {
        unsafe {
            let db = open_memory();

            let mut errmsg: *mut c_char = ptr::null_mut();
            let sql = CString::new("SELEC invalid;").unwrap();
            let rc = sqlite3_exec(db, sql.as_ptr(), None, ptr::null_mut(), &mut errmsg);
            assert_ne!(rc, SQLITE_OK);

            if !errmsg.is_null() {
                let msg = CStr::from_ptr(errmsg).to_string_lossy();
                assert!(!msg.is_empty());
                sqlite3_free(errmsg.cast());
            }

            sqlite3_close(db);
        }
    }

    #[test]
    fn test_prepare_step_finalize() {
        unsafe {
            let db = open_memory();

            // Create table and insert data.
            let sql = CString::new(
                "CREATE TABLE t1(a INTEGER, b TEXT); INSERT INTO t1 VALUES(10, 'hello'); INSERT INTO t1 VALUES(20, 'world');",
            ).unwrap();
            sqlite3_exec(db, sql.as_ptr(), None, ptr::null_mut(), ptr::null_mut());

            // Prepare a SELECT.
            let sql = CString::new("SELECT a, b FROM t1;").unwrap();
            let mut stmt: *mut Sqlite3Stmt = ptr::null_mut();
            let rc = sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, ptr::null_mut());
            assert_eq!(rc, SQLITE_OK);
            assert!(!stmt.is_null());

            // Step through rows.
            let rc = sqlite3_step(stmt);
            assert_eq!(rc, SQLITE_ROW);
            assert_eq!(sqlite3_column_count(stmt), 2);
            assert_eq!(sqlite3_column_int64(stmt, 0), 10);
            assert_eq!(sqlite3_column_type(stmt, 0), SQLITE_INTEGER);

            let text = sqlite3_column_text(stmt, 1);
            assert!(!text.is_null());
            assert_eq!(CStr::from_ptr(text).to_str().unwrap(), "hello");
            assert_eq!(sqlite3_column_type(stmt, 1), SQLITE_TEXT);

            let rc = sqlite3_step(stmt);
            assert_eq!(rc, SQLITE_ROW);
            assert_eq!(sqlite3_column_int64(stmt, 0), 20);

            let text = sqlite3_column_text(stmt, 1);
            assert!(!text.is_null());
            assert_eq!(CStr::from_ptr(text).to_str().unwrap(), "world");

            let rc = sqlite3_step(stmt);
            assert_eq!(rc, SQLITE_DONE);

            let rc = sqlite3_finalize(stmt);
            assert_eq!(rc, SQLITE_OK);

            sqlite3_close(db);
        }
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_column_type_variants() {
        unsafe {
            let db = open_memory();

            let sql = CString::new("SELECT 42, 3.14, 'text', X'CAFE', NULL;").unwrap();
            let mut stmt: *mut Sqlite3Stmt = ptr::null_mut();
            sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, ptr::null_mut());

            let rc = sqlite3_step(stmt);
            assert_eq!(rc, SQLITE_ROW);

            assert_eq!(sqlite3_column_type(stmt, 0), SQLITE_INTEGER);
            assert_eq!(sqlite3_column_type(stmt, 1), SQLITE_FLOAT);
            assert_eq!(sqlite3_column_type(stmt, 2), SQLITE_TEXT);
            assert_eq!(sqlite3_column_type(stmt, 3), SQLITE_BLOB);
            assert_eq!(sqlite3_column_type(stmt, 4), SQLITE_NULL);

            assert_eq!(sqlite3_column_int64(stmt, 0), 42);
            let f = sqlite3_column_double(stmt, 1);
            assert!((f - 3.14).abs() < 0.001);

            let text = sqlite3_column_text(stmt, 2);
            assert_eq!(CStr::from_ptr(text).to_str().unwrap(), "text");

            let blob_bytes = sqlite3_column_bytes(stmt, 3);
            assert_eq!(blob_bytes, 2); // X'CAFE' = 2 bytes

            let blob_ptr = sqlite3_column_blob(stmt, 3);
            assert!(!blob_ptr.is_null());

            sqlite3_finalize(stmt);
            sqlite3_close(db);
        }
    }

    #[test]
    fn test_column_int_32bit() {
        unsafe {
            let db = open_memory();

            let sql = CString::new("SELECT 42;").unwrap();
            let mut stmt: *mut Sqlite3Stmt = ptr::null_mut();
            sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, ptr::null_mut());

            let rc = sqlite3_step(stmt);
            assert_eq!(rc, SQLITE_ROW);
            assert_eq!(sqlite3_column_int(stmt, 0), 42);

            sqlite3_finalize(stmt);
            sqlite3_close(db);
        }
    }

    #[test]
    fn test_column_coercion() {
        unsafe {
            let db = open_memory();

            // Integer as double, text as integer, float as integer.
            let sql = CString::new("SELECT 42, '123', 3.7;").unwrap();
            let mut stmt: *mut Sqlite3Stmt = ptr::null_mut();
            sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, ptr::null_mut());

            sqlite3_step(stmt);

            // Int → double.
            let f = sqlite3_column_double(stmt, 0);
            assert!((f - 42.0).abs() < 0.001);

            // Text → int64.
            assert_eq!(sqlite3_column_int64(stmt, 1), 123);

            // Float → int64 (truncation).
            assert_eq!(sqlite3_column_int64(stmt, 2), 3);

            // Int → text.
            let text = sqlite3_column_text(stmt, 0);
            assert_eq!(CStr::from_ptr(text).to_str().unwrap(), "42");

            sqlite3_finalize(stmt);
            sqlite3_close(db);
        }
    }

    #[test]
    fn test_column_blob_coerces_numeric_values_to_text_bytes() {
        unsafe {
            let db = open_memory();

            let sql = CString::new("SELECT 42, 3.5;").unwrap();
            let mut stmt: *mut Sqlite3Stmt = ptr::null_mut();
            sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, ptr::null_mut());

            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);

            let int_blob = sqlite3_column_blob(stmt, 0);
            assert!(!int_blob.is_null());
            let int_len = sqlite3_column_bytes(stmt, 0) as usize;
            assert_eq!(
                std::slice::from_raw_parts(int_blob.cast::<u8>(), int_len),
                b"42"
            );

            let float_blob = sqlite3_column_blob(stmt, 1);
            assert!(!float_blob.is_null());
            let float_len = sqlite3_column_bytes(stmt, 1) as usize;
            assert_eq!(
                std::slice::from_raw_parts(float_blob.cast::<u8>(), float_len),
                b"3.5"
            );

            sqlite3_finalize(stmt);
            sqlite3_close(db);
        }
    }

    #[test]
    fn test_errmsg_default() {
        unsafe {
            let db = open_memory();

            let msg = sqlite3_errmsg(db);
            assert!(!msg.is_null());
            let s = CStr::from_ptr(msg).to_str().unwrap();
            assert_eq!(s, "not an error");

            sqlite3_close(db);
        }
    }

    #[test]
    fn test_errmsg_after_error() {
        unsafe {
            let db = open_memory();

            let sql = CString::new("SELECT * FROM nonexistent;").unwrap();
            let mut stmt: *mut Sqlite3Stmt = ptr::null_mut();
            let rc = sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, ptr::null_mut());
            assert_ne!(rc, SQLITE_OK);

            let msg = sqlite3_errmsg(db);
            assert!(!msg.is_null());
            let s = CStr::from_ptr(msg).to_string_lossy();
            assert!(
                s.contains("no such table") || s.contains("nonexistent"),
                "expected error about missing table, got: {s}"
            );

            sqlite3_close(db);
        }
    }

    #[test]
    fn test_errcode_tracks_last_result() {
        unsafe {
            let db = open_memory();

            assert_eq!(sqlite3_errcode(db), SQLITE_OK);

            let sql = CString::new("SELEC invalid;").unwrap();
            let rc = sqlite3_exec(db, sql.as_ptr(), None, ptr::null_mut(), ptr::null_mut());
            assert_ne!(rc, SQLITE_OK);
            assert_eq!(sqlite3_errcode(db), rc);

            let sql = CString::new("SELECT 1;").unwrap();
            assert_eq!(
                sqlite3_exec(db, sql.as_ptr(), None, ptr::null_mut(), ptr::null_mut()),
                SQLITE_OK
            );
            assert_eq!(sqlite3_errcode(db), SQLITE_OK);

            sqlite3_close(db);
        }
    }

    #[test]
    fn test_errmsg_null_db() {
        unsafe {
            let msg = sqlite3_errmsg(ptr::null_mut());
            assert!(!msg.is_null());
            let s = CStr::from_ptr(msg).to_str().unwrap();
            assert_eq!(s, "not an error");
        }
    }

    #[test]
    fn test_finalize_null() {
        unsafe {
            let rc = sqlite3_finalize(ptr::null_mut());
            assert_eq!(rc, SQLITE_OK);
        }
    }

    #[test]
    fn test_reset_and_restep() {
        unsafe {
            let db = open_memory();

            let sql = CString::new("SELECT 1, 2, 3;").unwrap();
            let mut stmt: *mut Sqlite3Stmt = ptr::null_mut();
            sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, ptr::null_mut());

            // First pass.
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            assert_eq!(sqlite3_column_int64(stmt, 0), 1);
            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);

            // Reset and step again.
            assert_eq!(sqlite3_reset(stmt), SQLITE_OK);
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            assert_eq!(sqlite3_column_int64(stmt, 0), 1);
            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);

            sqlite3_finalize(stmt);
            sqlite3_close(db);
        }
    }

    #[test]
    fn test_prepare_empty_sql() {
        unsafe {
            let db = open_memory();

            let sql = CString::new("   ").unwrap();
            let mut stmt: *mut Sqlite3Stmt = ptr::null_mut();
            let rc = sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, ptr::null_mut());
            assert_eq!(rc, SQLITE_OK);
            assert!(stmt.is_null()); // Empty SQL → no statement.

            sqlite3_close(db);
        }
    }

    #[test]
    fn test_reset_returns_last_error_code_once() {
        unsafe {
            let db = open_memory();

            let setup =
                CString::new("CREATE TABLE t(x INTEGER PRIMARY KEY); INSERT INTO t VALUES(1);")
                    .unwrap();
            assert_eq!(
                sqlite3_exec(db, setup.as_ptr(), None, ptr::null_mut(), ptr::null_mut()),
                SQLITE_OK
            );

            let sql = CString::new("INSERT INTO t VALUES(1);").unwrap();
            let mut stmt: *mut Sqlite3Stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, ptr::null_mut()),
                SQLITE_OK
            );

            let step_rc = sqlite3_step(stmt);
            assert_ne!(step_rc, SQLITE_OK);
            assert_eq!(sqlite3_reset(stmt), step_rc);
            assert_eq!(sqlite3_reset(stmt), SQLITE_OK);

            sqlite3_finalize(stmt);
            sqlite3_close(db);
        }
    }

    #[test]
    fn test_finalize_returns_last_error_code() {
        unsafe {
            let db = open_memory();

            let setup =
                CString::new("CREATE TABLE t(x INTEGER PRIMARY KEY); INSERT INTO t VALUES(1);")
                    .unwrap();
            assert_eq!(
                sqlite3_exec(db, setup.as_ptr(), None, ptr::null_mut(), ptr::null_mut()),
                SQLITE_OK
            );

            let sql = CString::new("INSERT INTO t VALUES(1);").unwrap();
            let mut stmt: *mut Sqlite3Stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, ptr::null_mut()),
                SQLITE_OK
            );

            let step_rc = sqlite3_step(stmt);
            assert_eq!(step_rc, SQLITE_CONSTRAINT);
            assert_eq!(sqlite3_finalize(stmt), step_rc);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_prepare_uses_first_statement_and_sets_tail() {
        unsafe {
            let db = open_memory();

            let sql = CString::new("SELECT 99; SELECT 100;").unwrap();
            let mut stmt: *mut Sqlite3Stmt = ptr::null_mut();
            let mut tail: *const c_char = ptr::null();
            let rc = sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, &mut tail);
            assert_eq!(rc, SQLITE_OK);
            assert!(!stmt.is_null());
            assert!(!tail.is_null());

            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            assert_eq!(sqlite3_column_int64(stmt, 0), 99);
            assert_eq!(CStr::from_ptr(tail).to_str().unwrap(), " SELECT 100;");
            sqlite3_finalize(stmt);

            let mut tail_stmt: *mut Sqlite3Stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(db, tail, -1, &mut tail_stmt, ptr::null_mut()),
                SQLITE_OK
            );
            assert!(!tail_stmt.is_null());
            assert_eq!(sqlite3_step(tail_stmt), SQLITE_ROW);
            assert_eq!(sqlite3_column_int64(tail_stmt, 0), 100);
            assert_eq!(sqlite3_step(tail_stmt), SQLITE_DONE);

            sqlite3_finalize(tail_stmt);
            sqlite3_close(db);
        }
    }

    #[test]
    fn test_prepare_trigger_consumes_full_trigger_statement() {
        unsafe {
            let db = open_memory();

            let setup =
                CString::new("CREATE TABLE t(id INTEGER); CREATE TABLE audit(msg TEXT);").unwrap();
            assert_eq!(
                sqlite3_exec(db, setup.as_ptr(), None, ptr::null_mut(), ptr::null_mut()),
                SQLITE_OK
            );

            let sql = CString::new(
                "CREATE TRIGGER trg AFTER INSERT ON t BEGIN INSERT INTO audit VALUES('first'); INSERT INTO audit VALUES('second'); END; SELECT 1;",
            )
            .unwrap();
            let mut stmt: *mut Sqlite3Stmt = ptr::null_mut();
            let mut tail: *const c_char = ptr::null();
            assert_eq!(
                sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, &mut tail),
                SQLITE_OK
            );
            assert!(!stmt.is_null());
            assert!(!tail.is_null());

            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
            assert_eq!(CStr::from_ptr(tail).to_str().unwrap(), " SELECT 1;");
            sqlite3_finalize(stmt);

            let fire = CString::new("INSERT INTO t VALUES(1);").unwrap();
            assert_eq!(
                sqlite3_exec(db, fire.as_ptr(), None, ptr::null_mut(), ptr::null_mut()),
                SQLITE_OK
            );

            let verify = CString::new("SELECT COUNT(*) FROM audit;").unwrap();
            let mut verify_stmt: *mut Sqlite3Stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(db, verify.as_ptr(), -1, &mut verify_stmt, ptr::null_mut()),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(verify_stmt), SQLITE_ROW);
            assert_eq!(sqlite3_column_int64(verify_stmt, 0), 2);

            sqlite3_finalize(verify_stmt);
            sqlite3_close(db);
        }
    }

    #[test]
    fn test_prepare_rejects_adjacent_statements_without_separator() {
        unsafe {
            let db = open_memory();

            let sql = CString::new("SELECT 1 SELECT 2").unwrap();
            let mut stmt: *mut Sqlite3Stmt = ptr::null_mut();
            let rc = sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, ptr::null_mut());
            assert_ne!(rc, SQLITE_OK);
            assert!(stmt.is_null());

            let msg = CStr::from_ptr(sqlite3_errmsg(db)).to_string_lossy();
            assert!(
                msg.contains("separator") || msg.contains("unexpected token"),
                "unexpected error: {msg}"
            );

            sqlite3_close(db);
        }
    }

    #[test]
    fn test_prepare_with_n_byte() {
        unsafe {
            let db = open_memory();

            // Pass a longer buffer but limit via n_byte.
            let full_sql = "SELECT 99; SELECT 100;";
            let sql = CString::new(full_sql).unwrap();
            let mut stmt: *mut Sqlite3Stmt = ptr::null_mut();
            let mut tail: *const c_char = ptr::null();

            // Only prepare "SELECT 99;" (10 chars).
            let rc = sqlite3_prepare_v2(db, sql.as_ptr(), 10, &mut stmt, &mut tail);
            assert_eq!(rc, SQLITE_OK);
            assert!(!stmt.is_null());

            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            assert_eq!(sqlite3_column_int64(stmt, 0), 99);

            sqlite3_finalize(stmt);
            sqlite3_close(db);
        }
    }

    #[test]
    fn test_close_with_unfinalized_statement_returns_busy() {
        unsafe {
            let db = open_memory();

            let sql = CString::new("SELECT 1;").unwrap();
            let mut stmt: *mut Sqlite3Stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, ptr::null_mut()),
                SQLITE_OK
            );
            assert!(!stmt.is_null());

            let rc = sqlite3_close(db);
            assert_eq!(rc, SQLITE_BUSY);
            assert_eq!(sqlite3_errcode(db), SQLITE_BUSY);
            let msg = CStr::from_ptr(sqlite3_errmsg(db)).to_str().unwrap();
            assert!(msg.contains("unfinalized statements"));

            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            assert_eq!(sqlite3_column_int64(stmt, 0), 1);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_column_count_preserved_for_empty_result_and_reset() {
        unsafe {
            let db = open_memory();

            let sql = CString::new("SELECT 1 WHERE 0;").unwrap();
            let mut stmt: *mut Sqlite3Stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, ptr::null_mut()),
                SQLITE_OK
            );
            assert!(!stmt.is_null());

            assert_eq!(sqlite3_column_count(stmt), 1);
            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
            assert_eq!(sqlite3_column_count(stmt), 1);

            assert_eq!(sqlite3_reset(stmt), SQLITE_OK);
            assert_eq!(sqlite3_column_count(stmt), 1);

            sqlite3_finalize(stmt);
            sqlite3_close(db);
        }
    }

    #[test]
    fn test_prepared_insert_steps_through_execute_path() {
        unsafe {
            let db = open_memory();

            let setup = CString::new("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);").unwrap();
            assert_eq!(
                sqlite3_exec(db, setup.as_ptr(), None, ptr::null_mut(), ptr::null_mut()),
                SQLITE_OK
            );

            let sql = CString::new("INSERT INTO t VALUES(1, 'a');").unwrap();
            let mut stmt: *mut Sqlite3Stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, ptr::null_mut()),
                SQLITE_OK
            );
            assert!(!stmt.is_null());

            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
            assert_eq!(sqlite3_changes(db), 1);
            sqlite3_finalize(stmt);

            let verify = CString::new("SELECT COUNT(*) FROM t;").unwrap();
            let mut verify_stmt: *mut Sqlite3Stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(db, verify.as_ptr(), -1, &mut verify_stmt, ptr::null_mut()),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(verify_stmt), SQLITE_ROW);
            assert_eq!(sqlite3_column_int64(verify_stmt, 0), 1);

            sqlite3_finalize(verify_stmt);
            sqlite3_close(db);
        }
    }

    #[test]
    fn test_column_out_of_range() {
        unsafe {
            let db = open_memory();

            let sql = CString::new("SELECT 1;").unwrap();
            let mut stmt: *mut Sqlite3Stmt = ptr::null_mut();
            sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, ptr::null_mut());
            sqlite3_step(stmt);

            // Column 99 is out of range → defaults.
            assert_eq!(sqlite3_column_type(stmt, 99), SQLITE_NULL);
            assert_eq!(sqlite3_column_int64(stmt, 99), 0);
            assert!((sqlite3_column_double(stmt, 99)).abs() < 0.001);
            assert!(sqlite3_column_text(stmt, 99).is_null());

            sqlite3_finalize(stmt);
            sqlite3_close(db);
        }
    }

    #[test]
    fn test_column_accessors_clear_after_done() {
        unsafe {
            let db = open_memory();

            let sql = CString::new("SELECT 42, 'hello';").unwrap();
            let mut stmt: *mut Sqlite3Stmt = ptr::null_mut();
            sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, ptr::null_mut());

            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            let text = sqlite3_column_text(stmt, 1);
            assert!(!text.is_null());
            assert_eq!(CStr::from_ptr(text).to_str().unwrap(), "hello");
            assert_eq!(sqlite3_column_bytes(stmt, 1), 5);

            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
            assert_eq!(sqlite3_column_type(stmt, 0), SQLITE_NULL);
            assert_eq!(sqlite3_column_int64(stmt, 0), 0);
            assert!((sqlite3_column_double(stmt, 0)).abs() < 0.001);
            assert!(sqlite3_column_text(stmt, 1).is_null());
            assert!(sqlite3_column_blob(stmt, 1).is_null());
            assert_eq!(sqlite3_column_bytes(stmt, 1), 0);

            sqlite3_finalize(stmt);
            sqlite3_close(db);
        }
    }

    #[test]
    fn test_exec_callback_abort() {
        unsafe {
            unsafe extern "C" fn abort_cb(
                _parg: *mut c_void,
                _ncols: c_int,
                _values: *mut *mut c_char,
                _names: *mut *mut c_char,
            ) -> c_int {
                1 // non-zero → abort
            }

            let db = open_memory();

            let sql = CString::new(
                "CREATE TABLE t1(x); INSERT INTO t1 VALUES(1); INSERT INTO t1 VALUES(2);",
            )
            .unwrap();
            sqlite3_exec(db, sql.as_ptr(), None, ptr::null_mut(), ptr::null_mut());

            let sql = CString::new("SELECT * FROM t1;").unwrap();
            let rc = sqlite3_exec(
                db,
                sql.as_ptr(),
                Some(abort_cb),
                ptr::null_mut(),
                ptr::null_mut(),
            );
            assert_eq!(rc, SQLITE_ABORT);

            sqlite3_close(db);
        }
    }

    #[test]
    fn test_exec_callback_uses_actual_column_names() {
        unsafe {
            unsafe extern "C" fn capture_names_cb(
                parg: *mut c_void,
                ncols: c_int,
                _values: *mut *mut c_char,
                names: *mut *mut c_char,
            ) -> c_int {
                let out = &mut *parg.cast::<Vec<String>>();
                let names =
                    std::slice::from_raw_parts(names.cast::<*const c_char>(), ncols as usize);
                out.extend(
                    names
                        .iter()
                        .map(|ptr| CStr::from_ptr(*ptr).to_str().unwrap().to_owned()),
                );
                SQLITE_OK
            }

            let db = open_memory();
            let mut captured_names: Vec<String> = Vec::new();

            let sql = CString::new("SELECT 1 AS alpha, 2 AS beta;").unwrap();
            assert_eq!(
                sqlite3_exec(
                    db,
                    sql.as_ptr(),
                    Some(capture_names_cb),
                    std::ptr::from_mut(&mut captured_names).cast(),
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert_eq!(captured_names, vec!["alpha", "beta"]);

            sqlite3_close(db);
        }
    }

    #[test]
    fn test_exec_callback_runs_for_each_query_statement_in_batch() {
        unsafe {
            #[derive(Debug, PartialEq, Eq)]
            struct CallbackRow {
                names: Vec<String>,
                values: Vec<Option<String>>,
            }

            unsafe extern "C" fn capture_rows_cb(
                parg: *mut c_void,
                ncols: c_int,
                values: *mut *mut c_char,
                names: *mut *mut c_char,
            ) -> c_int {
                let out = &mut *parg.cast::<Vec<CallbackRow>>();
                let names =
                    std::slice::from_raw_parts(names.cast::<*const c_char>(), ncols as usize);
                let values =
                    std::slice::from_raw_parts(values.cast::<*const c_char>(), ncols as usize);
                out.push(CallbackRow {
                    names: names
                        .iter()
                        .map(|ptr| CStr::from_ptr(*ptr).to_str().unwrap().to_owned())
                        .collect(),
                    values: values
                        .iter()
                        .map(|ptr| {
                            (!ptr.is_null())
                                .then(|| CStr::from_ptr(*ptr).to_str().unwrap().to_owned())
                        })
                        .collect(),
                });
                SQLITE_OK
            }

            let db = open_memory();
            let mut callback_rows: Vec<CallbackRow> = Vec::new();

            let sql = CString::new("SELECT 1 AS alpha; SELECT 2 AS beta;").unwrap();
            assert_eq!(
                sqlite3_exec(
                    db,
                    sql.as_ptr(),
                    Some(capture_rows_cb),
                    std::ptr::from_mut(&mut callback_rows).cast(),
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert_eq!(
                callback_rows,
                vec![
                    CallbackRow {
                        names: vec!["alpha".to_owned()],
                        values: vec![Some("1".to_owned())],
                    },
                    CallbackRow {
                        names: vec!["beta".to_owned()],
                        values: vec![Some("2".to_owned())],
                    },
                ]
            );

            sqlite3_close(db);
        }
    }

    #[test]
    fn test_column_text_preserves_embedded_nul_bytes() {
        unsafe {
            let db = open_memory();
            block_on((*db).conn.execute("CREATE TABLE t(v TEXT)")).unwrap();
            block_on((*db).conn.execute_with_params(
                "INSERT INTO t(v) VALUES (?)",
                &[SqliteValue::Text(fsqlite_types::SmallText::from("a\0b"))],
            ))
            .unwrap();

            let sql = CString::new("SELECT v FROM t;").unwrap();
            let mut stmt: *mut Sqlite3Stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, ptr::null_mut()),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);

            let text = sqlite3_column_text(stmt, 0);
            assert!(
                !text.is_null(),
                "embedded NUL text should still expose a buffer"
            );
            let bytes = std::slice::from_raw_parts(text.cast::<u8>(), 4);
            assert_eq!(bytes, b"a\0b\0");
            assert_eq!(sqlite3_column_bytes(stmt, 0), 3);

            sqlite3_finalize(stmt);
            sqlite3_close(db);
        }
    }

    #[test]
    fn test_exec_callback_preserves_embedded_nul_bytes() {
        unsafe {
            unsafe extern "C" fn capture_bytes_cb(
                parg: *mut c_void,
                _ncols: c_int,
                values: *mut *mut c_char,
                _names: *mut *mut c_char,
            ) -> c_int {
                let out = &mut *parg.cast::<Vec<u8>>();
                let value_ptr = *values;
                let bytes = std::slice::from_raw_parts(value_ptr.cast::<u8>(), 4);
                out.extend_from_slice(bytes);
                SQLITE_OK
            }

            let conn = block_on(Connection::open(":memory:")).unwrap();
            block_on(conn.execute("CREATE TABLE t(v TEXT)")).unwrap();
            block_on(conn.execute_with_params(
                "INSERT INTO t(v) VALUES (?)",
                &[SqliteValue::Text(fsqlite_types::SmallText::from("a\0b"))],
            ))
            .unwrap();
            let rows = block_on(conn.query("SELECT v FROM t")).unwrap();
            let handle = Sqlite3::new(conn, None);
            let mut captured: Vec<u8> = Vec::new();

            assert_eq!(
                emit_exec_callback_rows(
                    &handle,
                    "SELECT v FROM t",
                    &rows,
                    capture_bytes_cb,
                    std::ptr::from_mut(&mut captured).cast(),
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert_eq!(captured, b"a\0b\0");
        }
    }

    #[test]
    fn test_exec_error_in_later_statement_reports_global_offset() {
        unsafe {
            let db = open_memory();
            let mut errmsg: *mut c_char = ptr::null_mut();
            let sql = CString::new("SELECT 1; SELECT * FROM").unwrap();

            assert_eq!(
                sqlite3_exec(db, sql.as_ptr(), None, ptr::null_mut(), &mut errmsg),
                SQLITE_ERROR
            );
            assert!(!errmsg.is_null());
            let message = CStr::from_ptr(errmsg).to_string_lossy().into_owned();
            let offset = message
                .split("offset ")
                .nth(1)
                .and_then(|suffix| suffix.split(':').next())
                .and_then(|value| value.parse::<usize>().ok())
                .expect("errmsg should contain parse offset");
            assert!(
                offset >= "SELECT 1; ".len(),
                "later statement parse error should report an offset in the original SQL string: {message}"
            );

            sqlite3_free(errmsg.cast());
            sqlite3_close(db);
        }
    }

    #[test]
    fn test_metrics_increment() {
        reset_compat_metrics();

        unsafe {
            let db = open_memory();

            let sql = CString::new("SELECT 1;").unwrap();
            let mut stmt: *mut Sqlite3Stmt = ptr::null_mut();
            sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, ptr::null_mut());
            sqlite3_step(stmt);
            sqlite3_column_int64(stmt, 0);
            sqlite3_finalize(stmt);
            sqlite3_close(db);
        }

        let snap = compat_metrics_snapshot();
        assert!(snap.open >= 1);
        assert!(snap.prepare >= 1);
        assert!(snap.step >= 1);
        assert!(snap.column >= 1);
        assert!(snap.finalize >= 1);
        assert!(snap.close >= 1);
        assert!(snap.total() >= 6);
    }

    #[test]
    fn test_column_bytes_text_and_blob() {
        unsafe {
            let db = open_memory();

            let sql = CString::new("SELECT 'hello', X'DEADBEEF';").unwrap();
            let mut stmt: *mut Sqlite3Stmt = ptr::null_mut();
            sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, ptr::null_mut());
            sqlite3_step(stmt);

            assert_eq!(sqlite3_column_bytes(stmt, 0), 5); // "hello" = 5 bytes
            assert_eq!(sqlite3_column_bytes(stmt, 1), 4); // X'DEADBEEF' = 4 bytes

            sqlite3_finalize(stmt);
            sqlite3_close(db);
        }
    }

    #[test]
    fn test_column_text_exposes_raw_blob_bytes() {
        unsafe {
            let db = open_memory();

            let sql = CString::new("SELECT X'CAFE';").unwrap();
            let mut stmt: *mut Sqlite3Stmt = ptr::null_mut();
            sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, ptr::null_mut());
            sqlite3_step(stmt);

            let text = sqlite3_column_text(stmt, 0);
            assert!(!text.is_null());
            let bytes = std::slice::from_raw_parts(
                text.cast::<u8>(),
                sqlite3_column_bytes(stmt, 0) as usize + 1,
            );
            assert_eq!(bytes, &[0xCA, 0xFE, 0x00]);
            assert_eq!(sqlite3_column_bytes(stmt, 0), 2);

            sqlite3_finalize(stmt);
            sqlite3_close(db);
        }
    }

    #[test]
    fn test_exec_callback_exposes_raw_blob_bytes() {
        unsafe {
            unsafe extern "C" fn capture_blob_cb(
                parg: *mut c_void,
                _ncols: c_int,
                values: *mut *mut c_char,
                _names: *mut *mut c_char,
            ) -> c_int {
                let out = &mut *parg.cast::<Vec<u8>>();
                let value_ptr = *values;
                let bytes = std::slice::from_raw_parts(value_ptr.cast::<u8>(), 3);
                out.extend_from_slice(bytes);
                SQLITE_OK
            }

            let conn = block_on(Connection::open(":memory:")).unwrap();
            let rows = block_on(conn.query("SELECT X'CAFE'")).unwrap();
            let handle = Sqlite3::new(conn, None);
            let mut captured: Vec<u8> = Vec::new();

            assert_eq!(
                emit_exec_callback_rows(
                    &handle,
                    "SELECT X'CAFE'",
                    &rows,
                    capture_blob_cb,
                    std::ptr::from_mut(&mut captured).cast(),
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert_eq!(captured, [0xCA, 0xFE, 0x00]);
        }
    }

    #[test]
    fn test_misuse_null_args() {
        unsafe {
            // Null pp_db → MISUSE.
            assert_eq!(sqlite3_open(ptr::null(), ptr::null_mut()), SQLITE_MISUSE);

            // Null filename with valid pp_db → :memory: (OK).
            let mut db: *mut Sqlite3 = ptr::null_mut();
            assert_eq!(sqlite3_open(ptr::null(), &mut db), SQLITE_OK);
            sqlite3_close(db);

            assert_eq!(sqlite3_step(ptr::null_mut()), SQLITE_MISUSE);
            assert_eq!(sqlite3_reset(ptr::null_mut()), SQLITE_MISUSE);
            assert_eq!(sqlite3_column_count(ptr::null_mut()), 0);
        }
    }

    #[test]
    fn test_exec_dml() {
        unsafe {
            let db = open_memory();

            // Full DDL + DML cycle via exec.
            let sql = CString::new("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);").unwrap();
            assert_eq!(
                sqlite3_exec(db, sql.as_ptr(), None, ptr::null_mut(), ptr::null_mut()),
                SQLITE_OK
            );

            let sql = CString::new("INSERT INTO t VALUES(1, 'a');").unwrap();
            assert_eq!(
                sqlite3_exec(db, sql.as_ptr(), None, ptr::null_mut(), ptr::null_mut()),
                SQLITE_OK
            );

            let sql = CString::new("UPDATE t SET v = 'b' WHERE id = 1;").unwrap();
            assert_eq!(
                sqlite3_exec(db, sql.as_ptr(), None, ptr::null_mut(), ptr::null_mut()),
                SQLITE_OK
            );

            let sql = CString::new("DELETE FROM t WHERE id = 1;").unwrap();
            assert_eq!(
                sqlite3_exec(db, sql.as_ptr(), None, ptr::null_mut(), ptr::null_mut()),
                SQLITE_OK
            );

            sqlite3_close(db);
        }
    }

    #[test]
    fn test_changes_tracks_last_dml_and_survives_select() {
        unsafe {
            let db = open_memory();

            let sql = CString::new("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);").unwrap();
            assert_eq!(
                sqlite3_exec(db, sql.as_ptr(), None, ptr::null_mut(), ptr::null_mut()),
                SQLITE_OK
            );
            assert_eq!(sqlite3_changes(db), 0);

            let sql = CString::new("INSERT INTO t VALUES(1, 'a');").unwrap();
            assert_eq!(
                sqlite3_exec(db, sql.as_ptr(), None, ptr::null_mut(), ptr::null_mut()),
                SQLITE_OK
            );
            assert_eq!(sqlite3_changes(db), 1);

            let sql = CString::new("SELECT v FROM t;").unwrap();
            assert_eq!(
                sqlite3_exec(db, sql.as_ptr(), None, ptr::null_mut(), ptr::null_mut()),
                SQLITE_OK
            );
            assert_eq!(sqlite3_changes(db), 1);

            let sql = CString::new("UPDATE t SET v = 'b' WHERE id = 99;").unwrap();
            assert_eq!(
                sqlite3_exec(db, sql.as_ptr(), None, ptr::null_mut(), ptr::null_mut()),
                SQLITE_OK
            );
            assert_eq!(sqlite3_changes(db), 0);

            sqlite3_close(db);
        }
    }
}
