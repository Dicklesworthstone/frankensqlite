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
// The C ABI is a hard synchronous boundary: `extern "C"` functions cannot be
// `async`, and a C caller has no `Cx` to hand down, so the shim drives each
// engine future itself. It uses the driver of `fsqlite::blocking`, which polls
// the future on the calling thread and parks only while it is pending, instead
// of entering a thread-local asupersync runtime for every call (bd-zqr9h).
use fsqlite::blocking::block_on;
use fsqlite_ast::Statement;
use fsqlite_error::{ErrorCode, FrankenError};
use fsqlite_parser::{Parser, parse_first_statement_with_tail};
use fsqlite_types::SmallText;
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
    active_statements: AtomicUsize,
}

impl Sqlite3 {
    fn new(conn: Connection, temporary_path: Option<PathBuf>) -> Self {
        Self {
            conn,
            temporary_path,
            last_error: Mutex::new(CString::new(DEFAULT_ERROR_MESSAGE).expect("static")),
            last_error_code: AtomicI32::new(SQLITE_OK),
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
    /// Held engine prepared statement borrowing `(&*db).conn` (bd-w9s20).
    ///
    /// # Safety invariant
    /// `db` points to a heap-boxed `Sqlite3` created in `sqlite3_open`.
    /// `sqlite3_prepare_v2` increments `Sqlite3::active_statements` before
    /// returning the handle, `sqlite3_close` refuses to drop `Sqlite3`
    /// (`SQLITE_BUSY`) while `active_statements != 0`, and `sqlite3_finalize`
    /// drops `prepared` before decrementing `active_statements`.
    prepared: Option<fsqlite::PreparedStatement<'static>>,
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
    /// Statement-owned labels, independent of per-row conversion buffers.
    /// Their pointers stay valid through ordinary step/reset cycles.
    column_names: Vec<CString>,
    /// Reprepare generation whose labels are held in `column_names`.
    column_names_generation: u64,
    /// Cached NUL-terminated buffers for coerced text/blob accessors (kept
    /// alive until next step or finalize to satisfy C lifetime expectations,
    /// including values that contain embedded NUL bytes).
    text_cache: Vec<Option<Vec<u8>>>,
    /// Bound parameter values (0-based; SQL parameter index `i` maps to
    /// `bindings[i - 1]`). Sized to the statement's parameter count at prepare
    /// time; slots left unbound stay NULL, matching SQLite. bd-w2i2w.
    bindings: Vec<SqliteValue>,
    /// Full parameter name including its sigil (e.g. `":id"`, `"@x"`, `"$y"`)
    /// for each 1-based index, or `None` for nameless `?` / `?NNN`. Parallel to
    /// `bindings`; drives `sqlite3_bind_parameter_name` / `_index`. bd-w2i2w.
    param_names: Vec<Option<CString>>,
}

impl Sqlite3Stmt {
    fn refresh_column_metadata(&mut self) {
        let Some(prepared) = self.prepared.as_ref() else {
            return;
        };
        self.column_count = usize_to_c_int_saturating(prepared.column_count());
        let generation = prepared.reprepare_count();
        if generation != self.column_names_generation {
            self.column_names = prepared
                .column_names()
                .iter()
                .map(|name| c_string_truncate_on_nul(name))
                .collect();
            self.column_names_generation = generation;
        }
    }
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

fn usize_to_c_int_saturating(value: usize) -> c_int {
    c_int::try_from(value).unwrap_or(c_int::MAX)
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
    // The core prepares schema, control, and PRAGMA statements without
    // executing them. EXPLAIN retains its connection dispatch path.
    !matches!(statement, Statement::Explain { .. })
}

fn explain_column_names(query_plan: bool) -> &'static [&'static str] {
    if query_plan {
        &["id", "parent", "notused", "detail"]
    } else {
        &["addr", "opcode", "p1", "p2", "p3", "p4", "p5", "comment"]
    }
}

fn prepared_step_mode(statement: &Statement) -> PreparedStepMode {
    match statement {
        Statement::Insert(stmt) if stmt.returning.is_empty() => PreparedStepMode::Execute,
        Statement::Update(stmt) if stmt.returning.is_empty() => PreparedStepMode::Execute,
        Statement::Delete(stmt) if stmt.returning.is_empty() => PreparedStepMode::Execute,
        _ => PreparedStepMode::Query,
    }
}

struct PreparedSqlInfo<'conn> {
    consumed_sql: String,
    tail_offset: usize,
    step_mode: PreparedStepMode,
    column_count: c_int,
    column_names: Vec<CString>,
    prepared: Option<fsqlite::PreparedStatement<'conn>>,
}

fn validate_and_classify_prepared_sql<'conn>(
    conn: &'conn Connection,
    sql: &str,
) -> Result<Option<PreparedSqlInfo<'conn>>, FrankenError> {
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
    let (column_count, column_names, prepared) = if can_prepare_statement(&statement) {
        let prepared = block_on(conn.prepare(consumed_sql))?;
        let column_count = usize_to_c_int_saturating(prepared.column_count());
        let column_names = prepared
            .column_names()
            .iter()
            .map(|name| c_string_truncate_on_nul(name))
            .collect();
        (column_count, column_names, Some(prepared))
    } else {
        let labels = explain_column_names(matches!(
            statement,
            Statement::Explain {
                query_plan: true,
                ..
            }
        ));
        let column_names = labels
            .iter()
            .map(|name| c_string_truncate_on_nul(name))
            .collect();
        (usize_to_c_int_saturating(labels.len()), column_names, None)
    };

    Ok(Some(PreparedSqlInfo {
        consumed_sql: consumed_sql.to_owned(),
        tail_offset,
        step_mode,
        column_count,
        column_names,
        prepared,
    }))
}

/// Parameter binding layout computed once at prepare time. bd-w2i2w.
struct PreparedParamInfo {
    /// One NULL slot per bindable parameter (index `i` → `bindings[i - 1]`).
    bindings: Vec<SqliteValue>,
    /// Full name (with sigil) per 1-based index, `None` for `?` / `?NNN`.
    param_names: Vec<Option<CString>>,
}

/// Enumerate a statement's bind parameters and their SQLite-canonical indices
/// ([`fsqlite_parser::bind_parameter_slots`]), as `sqlite3_bind_parameter_count`
/// / `_index` / `_name` report them. `bindings` is sized to the largest index
/// (all NULL); `param_names[i - 1]` is the sigil-prefixed name of index `i`, or
/// `None` for the nameless `?` / `?NNN` forms. bd-w2i2w.
fn compute_prepared_param_info(sql: &str) -> PreparedParamInfo {
    let slots = fsqlite_parser::bind_parameter_slots(sql);
    let count = slots.count;
    let mut param_names: Vec<Option<CString>> = vec![None; count];
    for (index, full) in slots.names {
        if index >= 1 && index <= count {
            param_names[index - 1] = CString::new(full).ok();
        }
    }

    PreparedParamInfo {
        bindings: vec![SqliteValue::Null; count],
        param_names,
    }
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
    if let Statement::Explain { query_plan, .. } = statement {
        return Some(
            explain_column_names(*query_plan)
                .iter()
                .map(|name| (*name).to_owned())
                .collect(),
        );
    }

    match block_on(conn.prepare(&statement.to_string())) {
        Ok(prepared) => Some(prepared.column_names()),
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
            return SQLITE_OK;
        }
        let statement_offset = sql.len().saturating_sub(trimmed.len());

        let tail_offset = match first_statement_tail_offset(trimmed) {
            Ok(Some(tail_offset)) => tail_offset,
            Ok(None) => {
                handle.clear_error();
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

    // SAFETY: `db` is heap-allocated by `Box::into_raw` in `sqlite3_open`, so
    // `handle.conn` remains at a stable address until `sqlite3_close`. Any
    // `PreparedStatement<'static>` created from `&handle.conn` is kept alive
    // only while `handle.active_statements > 0` (`sqlite3_close` returns
    // `SQLITE_BUSY` otherwise) and is dropped in `sqlite3_finalize` before
    // `release_statement()` decrements the count.
    let handle: &'static Sqlite3 = &*db;
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
            let param_info = compute_prepared_param_info(&info.consumed_sql);
            let column_names_generation = info
                .prepared
                .as_ref()
                .map_or(0, fsqlite::PreparedStatement::reprepare_count);
            let stmt = Box::new(Sqlite3Stmt {
                db,
                sql: info.consumed_sql,
                step_mode: info.step_mode,
                prepared: info.prepared,
                rows: None,
                cursor: 0,
                active_row: false,
                last_step_code: SQLITE_OK,
                column_count: info.column_count,
                column_names: info.column_names,
                column_names_generation,
                text_cache: Vec::new(),
                bindings: param_info.bindings,
                param_names: param_info.param_names,
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
#[allow(clippy::too_many_lines)]
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

        // Route through the held PreparedStatement when present (reusing the
        // compiled program while unchanged and transparently re-preparing on
        // schema or pragma invalidation, bd-w9s20). Statements that
        // Connection::prepare does not yet compile fall back to Connection
        // dispatch. Unbound slots default to NULL (SQLite semantics).
        let has_params = !s.bindings.is_empty();
        let execute_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let result = if let Some(ref prepared) = s.prepared {
                match s.step_mode {
                    PreparedStepMode::Query => {
                        if has_params {
                            block_on(prepared.query_with_params(&s.bindings)).map(Some)
                        } else {
                            block_on(prepared.query()).map(Some)
                        }
                    }
                    PreparedStepMode::Execute => {
                        if has_params {
                            block_on(prepared.execute_with_params(&s.bindings)).map(|_| None)
                        } else {
                            block_on(prepared.execute()).map(|_| None)
                        }
                    }
                }
            } else {
                match s.step_mode {
                    PreparedStepMode::Query => {
                        if has_params {
                            block_on(db.conn.query_with_params(&s.sql, &s.bindings)).map(Some)
                        } else {
                            block_on(db.conn.query(&s.sql)).map(Some)
                        }
                    }
                    PreparedStepMode::Execute => {
                        if has_params {
                            block_on(db.conn.execute_with_params(&s.sql, &s.bindings)).map(|_| None)
                        } else {
                            block_on(db.conn.execute(&s.sql)).map(|_| None)
                        }
                    }
                }
            };
            s.refresh_column_metadata();
            result
        }));

        match execute_result {
            Ok(Ok(Some(rows))) => {
                db.clear_error();
                if s.prepared.is_none()
                    && let Some(first) = rows.first()
                {
                    s.column_count = usize_to_c_int_saturating(first.values().len());
                }
                s.rows = Some(rows);
                s.cursor = 0;
                s.active_row = false;
                s.last_step_code = SQLITE_OK;
                s.text_cache.clear();
            }
            Ok(Ok(None)) => {
                db.clear_error();
                s.rows = Some(Vec::new());
                s.cursor = 0;
                s.active_row = false;
                s.last_step_code = SQLITE_OK;
                s.text_cache.clear();
            }
            Ok(Err(ref e)) if matches!(e, FrankenError::QueryReturnedNoRows) => {
                db.clear_error();
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
            // The next step after DONE starts a fresh execution with the same
            // bindings. Release the exhausted result, including for DML.
            s.rows = None;
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

    let mut stmt = Box::from_raw(stmt);
    let rc = match stmt.last_step_code {
        SQLITE_ROW | SQLITE_DONE => SQLITE_OK,
        code => code,
    };
    let db = stmt.db;
    // Drop the borrowed PreparedStatement BEFORE decrementing active_statements
    // so sqlite3_close cannot free `Sqlite3` while `stmt.prepared` holds `&conn`.
    stmt.prepared = None;
    drop(stmt);
    if !db.is_null() {
        (&*db).release_statement();
    }
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

// ── Parameter binding (bd-w2i2w) ────────────────────────────────────

/// C `void(*)(void*)` destructor sentinel: do not free the caller's buffer.
pub const SQLITE_STATIC: isize = 0;
/// C `void(*)(void*)` destructor sentinel: SQLite must copy the caller's buffer.
pub const SQLITE_TRANSIENT: isize = -1;

/// The 5th argument of `sqlite3_bind_text`/`_blob`: an optional destructor for
/// the caller's buffer, or the `SQLITE_STATIC` / `SQLITE_TRANSIENT` sentinels.
type SqliteDestructor = Option<unsafe extern "C" fn(*mut c_void)>;

/// Invoke a caller-supplied destructor, but never the `SQLITE_STATIC` (null) or
/// `SQLITE_TRANSIENT` (-1) sentinels. Every bind copies the bytes immediately,
/// so a real destructor is safe to call right after the copy. bd-w2i2w.
unsafe fn run_bind_destructor(xdel: SqliteDestructor, ptr: *const c_void) {
    let Some(func) = xdel else {
        return; // SQLITE_STATIC (null): nothing to free.
    };
    let addr = func as usize as isize;
    if addr == SQLITE_TRANSIENT {
        return; // SQLITE_TRANSIENT sentinel is not a callable pointer.
    }
    func(ptr.cast_mut());
}

/// Resolve a 1-based parameter index to its 0-based binding slot, or `None`
/// (which callers map to `SQLITE_RANGE`) when out of range. bd-w2i2w.
unsafe fn binding_slot_index(stmt: *const Sqlite3Stmt, index: c_int) -> Option<usize> {
    if index < 1 {
        return None;
    }
    let s = &*stmt;
    let slot = (index as usize).checked_sub(1)?;
    (slot < s.bindings.len()).then_some(slot)
}

/// Store a resolved value into parameter `index`, mapping out-of-range to
/// `SQLITE_RANGE`. bd-w2i2w.
unsafe fn store_binding(stmt: *mut Sqlite3Stmt, index: c_int, value: SqliteValue) -> c_int {
    match binding_slot_index(stmt, index) {
        Some(slot) => {
            let s = &mut *stmt;
            s.bindings[slot] = value;
            SQLITE_OK
        }
        None => SQLITE_RANGE,
    }
}

/// Bind a 64-bit integer to parameter `index` (1-based).
///
/// # Safety
/// `stmt` must be a valid handle from `sqlite3_prepare_v2`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_bind_int64(
    stmt: *mut Sqlite3Stmt,
    index: c_int,
    value: i64,
) -> c_int {
    if stmt.is_null() {
        return SQLITE_MISUSE;
    }
    store_binding(stmt, index, SqliteValue::Integer(value))
}

/// Bind a 32-bit integer to parameter `index` (1-based).
///
/// # Safety
/// `stmt` must be a valid handle from `sqlite3_prepare_v2`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_bind_int(
    stmt: *mut Sqlite3Stmt,
    index: c_int,
    value: c_int,
) -> c_int {
    sqlite3_bind_int64(stmt, index, i64::from(value))
}

/// Bind a floating-point value to parameter `index` (1-based).
///
/// # Safety
/// `stmt` must be a valid handle from `sqlite3_prepare_v2`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_bind_double(
    stmt: *mut Sqlite3Stmt,
    index: c_int,
    value: c_double,
) -> c_int {
    if stmt.is_null() {
        return SQLITE_MISUSE;
    }
    store_binding(stmt, index, SqliteValue::Float(value))
}

/// Bind SQL NULL to parameter `index` (1-based).
///
/// # Safety
/// `stmt` must be a valid handle from `sqlite3_prepare_v2`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_bind_null(stmt: *mut Sqlite3Stmt, index: c_int) -> c_int {
    if stmt.is_null() {
        return SQLITE_MISUSE;
    }
    store_binding(stmt, index, SqliteValue::Null)
}

/// Bind UTF-8 text to parameter `index` (1-based).
///
/// `n < 0` treats `text` as NUL-terminated; otherwise exactly `n` bytes are
/// copied (embedded NULs preserved). A NULL `text` pointer binds SQL NULL. The
/// bytes are always copied, so `SQLITE_STATIC` and `SQLITE_TRANSIENT` behave
/// identically; a real destructor is invoked once the copy completes.
///
/// # Safety
/// `stmt` must be a valid handle; when non-null, `text` must point to at least
/// `n` readable bytes (or be NUL-terminated when `n < 0`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_bind_text(
    stmt: *mut Sqlite3Stmt,
    index: c_int,
    text: *const c_char,
    n: c_int,
    xdel: SqliteDestructor,
) -> c_int {
    // Stock SQLite invokes a real destructor even when the bind FAILS: its
    // `bindText` runs `xDel(zData)` whenever `vdbeUnbind` returns non-OK (null /
    // not-ready stmt → SQLITE_MISUSE, out-of-range index → SQLITE_RANGE). Match
    // that so a caller's heap-backed buffer is not leaked on a failed bind
    // (bd-3745a). STATIC/TRANSIENT sentinels are no-ops inside the helper.
    if stmt.is_null() {
        run_bind_destructor(xdel, text.cast());
        return SQLITE_MISUSE;
    }
    // Validate the index before touching the caller's buffer, matching SQLite.
    if binding_slot_index(stmt, index).is_none() {
        run_bind_destructor(xdel, text.cast());
        return SQLITE_RANGE;
    }
    let value = if text.is_null() {
        SqliteValue::Null
    } else {
        let bytes: &[u8] = if n < 0 {
            CStr::from_ptr(text).to_bytes()
        } else {
            std::slice::from_raw_parts(text.cast::<u8>(), n as usize)
        };
        let value = SqliteValue::Text(SmallText::from_bytes(bytes));
        run_bind_destructor(xdel, text.cast());
        value
    };
    store_binding(stmt, index, value)
}

/// Bind a BLOB to parameter `index` (1-based).
///
/// `n` must be non-negative; a NULL `data` pointer binds SQL NULL. The bytes are
/// always copied, so `SQLITE_STATIC` and `SQLITE_TRANSIENT` behave identically;
/// a real destructor is invoked once the copy completes.
///
/// # Safety
/// `stmt` must be a valid handle; when non-null, `data` must point to at least
/// `n` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_bind_blob(
    stmt: *mut Sqlite3Stmt,
    index: c_int,
    data: *const c_void,
    n: c_int,
    xdel: SqliteDestructor,
) -> c_int {
    // See `sqlite3_bind_text`: dispose of a real destructor on the failing bind
    // paths so heap-backed blobs are not leaked (bd-3745a).
    if stmt.is_null() {
        run_bind_destructor(xdel, data);
        return SQLITE_MISUSE;
    }
    if binding_slot_index(stmt, index).is_none() {
        run_bind_destructor(xdel, data);
        return SQLITE_RANGE;
    }
    let value = if data.is_null() {
        SqliteValue::Null
    } else {
        if n < 0 {
            // Negative length is an API-armor misuse; stock rejects it up front
            // (before the bind machinery) and does not invoke the destructor.
            return SQLITE_MISUSE;
        }
        let bytes = std::slice::from_raw_parts(data.cast::<u8>(), n as usize);
        let value = SqliteValue::Blob(std::sync::Arc::from(bytes));
        run_bind_destructor(xdel, data);
        value
    };
    store_binding(stmt, index, value)
}

/// Bind a BLOB of `n` zero bytes to parameter `index` (1-based).
///
/// # Safety
/// `stmt` must be a valid handle from `sqlite3_prepare_v2`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_bind_zeroblob(
    stmt: *mut Sqlite3Stmt,
    index: c_int,
    n: c_int,
) -> c_int {
    if stmt.is_null() {
        return SQLITE_MISUSE;
    }
    let len = n.max(0) as usize;
    let value = SqliteValue::Blob(std::sync::Arc::from(vec![0u8; len].as_slice()));
    store_binding(stmt, index, value)
}

/// Number of bindable parameters in the prepared statement (the largest index).
///
/// # Safety
/// `stmt` must be a valid handle from `sqlite3_prepare_v2`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_bind_parameter_count(stmt: *mut Sqlite3Stmt) -> c_int {
    if stmt.is_null() {
        return 0;
    }
    let s = &*stmt;
    c_int::try_from(s.bindings.len()).unwrap_or(c_int::MAX)
}

/// 1-based index of the parameter named `name` (with its sigil, e.g. `":id"`),
/// or 0 if no such parameter exists.
///
/// # Safety
/// `stmt` must be a valid handle; `name`, when non-null, must be NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_bind_parameter_index(
    stmt: *mut Sqlite3Stmt,
    name: *const c_char,
) -> c_int {
    if stmt.is_null() || name.is_null() {
        return 0;
    }
    let needle = CStr::from_ptr(name).to_bytes();
    let s = &*stmt;
    for (offset, entry) in s.param_names.iter().enumerate() {
        if let Some(candidate) = entry
            && candidate.as_bytes() == needle
        {
            return (offset + 1) as c_int;
        }
    }
    0
}

/// Name (with sigil) of the parameter at 1-based `index`.
///
/// NULL for the nameless `?` / `?NNN` forms or an out-of-range index. The
/// returned pointer is owned by the statement and valid until it is finalized.
///
/// # Safety
/// `stmt` must be a valid handle from `sqlite3_prepare_v2`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_bind_parameter_name(
    stmt: *mut Sqlite3Stmt,
    index: c_int,
) -> *const c_char {
    if stmt.is_null() || index < 1 {
        return std::ptr::null();
    }
    let s = &*stmt;
    match s.param_names.get((index - 1) as usize) {
        Some(Some(name)) => name.as_ptr(),
        _ => std::ptr::null(),
    }
}

/// Reset every bound parameter back to NULL. bd-w2i2w.
///
/// # Safety
/// `stmt` must be a valid handle from `sqlite3_prepare_v2`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_clear_bindings(stmt: *mut Sqlite3Stmt) -> c_int {
    if stmt.is_null() {
        return SQLITE_MISUSE;
    }
    let s = &mut *stmt;
    for value in &mut s.bindings {
        *value = SqliteValue::Null;
    }
    SQLITE_OK
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

/// UTF-8 name of result column `i_col`, available before the first step.
///
/// The returned string is owned by the statement. Ordinary steps, column
/// value conversions, and resets preserve it. Automatic reprepare may replace
/// the labels, including when a schema change alters a SELECT * result.
///
/// # Safety
/// `stmt` must be a live handle from `sqlite3_prepare_v2`, or null. The
/// returned pointer is read-only and must not be used after the statement is
/// finalized or automatically reprepared.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_column_name(stmt: *mut Sqlite3Stmt, i_col: c_int) -> *const c_char {
    COMPAT_COLUMN.fetch_add(1, Ordering::Relaxed);

    if stmt.is_null() || i_col < 0 {
        return std::ptr::null();
    }
    let s = &*stmt;
    if i_col >= s.column_count {
        return std::ptr::null();
    }
    s.column_names
        .get(i_col as usize)
        .map_or(std::ptr::null(), |name| name.as_ptr())
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

/// Return the number of rows modified by the most recent completed
/// INSERT/UPDATE/DELETE.
///
/// This is the connection's own counter, read without running a statement
/// (bd-zqr9h). As in stock, a statement that fails to prepare leaves it
/// unchanged and one that fails while running leaves 0.
///
/// # Safety
/// `db` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_changes(db: *mut Sqlite3) -> c_int {
    if db.is_null() {
        return 0;
    }
    usize_to_c_int_saturating((&*db).conn.last_change_count())
}

/// Return the number of rows modified by INSERT/UPDATE/DELETE since the
/// connection was opened.
///
/// # Safety
/// `db` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_total_changes(db: *mut Sqlite3) -> c_int {
    if db.is_null() {
        return 0;
    }
    usize_to_c_int_saturating((&*db).conn.total_change_count())
}

/// Return the rowid of the most recent successful INSERT on this connection.
///
/// A rolled-back INSERT still sets this value, as in SQLite. Reading it does
/// not execute SQL or change the connection's error state.
///
/// # Safety
/// `db` must be a live handle from `sqlite3_open`, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_last_insert_rowid(db: *mut Sqlite3) -> i64 {
    if db.is_null() {
        return 0;
    }
    (&*db).conn.last_insert_rowid()
}

/// Return nonzero when the connection is in autocommit mode.
///
/// Explicit transactions and outermost SAVEPOINTs disable autocommit until
/// they end. Reading this flag does not execute SQL or change error state.
///
/// # Safety
/// `db` must be a live handle from `sqlite3_open`, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_get_autocommit(db: *mut Sqlite3) -> c_int {
    if db.is_null() {
        return 0;
    }
    c_int::from(!(&*db).conn.in_transaction())
}

/// Set the connection's busy timeout in milliseconds.
///
/// A nonpositive timeout disables waiting. Successful configuration preserves
/// the most recent C API error, matching SQLite's configuration API.
///
/// # Safety
/// `db` must be a live handle from `sqlite3_open`, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_busy_timeout(db: *mut Sqlite3, ms: c_int) -> c_int {
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let handle = &*db;
    let sql = format!("PRAGMA busy_timeout = {}", ms.max(0));
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        block_on(handle.conn.execute(&sql))
    }));
    match result {
        Ok(Ok(_)) => SQLITE_OK,
        Ok(Err(error)) => {
            handle.set_error(&error);
            error_to_code(&error)
        }
        Err(_) => {
            let error = FrankenError::Internal("Rust panic during sqlite3_busy_timeout".to_owned());
            handle.set_error(&error);
            error_to_code(&error)
        }
    }
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
    fn test_connection_state_getters_preserve_errors_and_track_transactions() {
        unsafe {
            let db = open_memory();
            let other = open_memory();
            assert_eq!(sqlite3_last_insert_rowid(db), 0);
            assert_eq!(sqlite3_get_autocommit(db), 1);
            schema_exec_ok(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, value TEXT UNIQUE)");
            schema_exec_ok(db, "INSERT INTO t VALUES (7, 'first')");
            assert_eq!(sqlite3_last_insert_rowid(db), 7);
            let duplicate = CString::new("INSERT INTO t VALUES (8, 'first')").unwrap();
            assert_eq!(
                sqlite3_exec(db, duplicate.as_ptr(), None, ptr::null_mut(), ptr::null_mut()),
                SQLITE_CONSTRAINT
            );
            let message = CStr::from_ptr(sqlite3_errmsg(db)).to_bytes().to_vec();
            assert_eq!(sqlite3_last_insert_rowid(db), 7);
            assert_eq!(sqlite3_get_autocommit(db), 1);
            assert_eq!(sqlite3_busy_timeout(db, 250), SQLITE_OK);
            assert_eq!(sqlite3_errcode(db), SQLITE_CONSTRAINT);
            assert_eq!(CStr::from_ptr(sqlite3_errmsg(db)).to_bytes(), message);
            assert_eq!(sqlite3_total_changes(db), 1);

            schema_exec_ok(db, "BEGIN; INSERT INTO t VALUES (42, 'rolled back')");
            assert_eq!(sqlite3_get_autocommit(db), 0);
            assert_eq!(sqlite3_last_insert_rowid(db), 42);
            let changes = sqlite3_changes(db);
            let total = sqlite3_total_changes(db);
            assert_eq!(sqlite3_busy_timeout(db, 0), SQLITE_OK);
            assert_eq!(sqlite3_get_autocommit(db), 0);
            assert_eq!(sqlite3_last_insert_rowid(db), 42);
            assert_eq!(sqlite3_changes(db), changes);
            assert_eq!(sqlite3_total_changes(db), total);
            schema_exec_ok(db, "ROLLBACK");
            assert_eq!(sqlite3_get_autocommit(db), 1);
            assert_eq!(sqlite3_last_insert_rowid(db), 42);
            assert_eq!(schema_query_int(db, "SELECT count(*) FROM t"), 1);

            schema_exec_ok(db, "SAVEPOINT outer_savepoint; SAVEPOINT inner_savepoint");
            assert_eq!(sqlite3_get_autocommit(db), 0);
            schema_exec_ok(db, "ROLLBACK TO inner_savepoint; RELEASE inner_savepoint");
            assert_eq!(sqlite3_get_autocommit(db), 0);
            schema_exec_ok(db, "RELEASE outer_savepoint");
            assert_eq!(sqlite3_get_autocommit(db), 1);

            schema_exec_ok(db, "BEGIN");
            let rollback =
                CString::new("INSERT OR ROLLBACK INTO t VALUES (7, 'duplicate')").unwrap();
            assert_eq!(
                sqlite3_exec(db, rollback.as_ptr(), None, ptr::null_mut(), ptr::null_mut()),
                SQLITE_CONSTRAINT
            );
            assert_eq!(sqlite3_get_autocommit(db), 1);
            assert_eq!(sqlite3_last_insert_rowid(db), 42);
            assert_eq!(sqlite3_errcode(db), SQLITE_CONSTRAINT);
            assert_eq!(sqlite3_get_autocommit(other), 1);
            assert_eq!(sqlite3_last_insert_rowid(other), 0);
            assert_eq!(sqlite3_close(other), SQLITE_OK);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_busy_timeout_configuration_boundaries_and_null_handles() {
        unsafe {
            let db = open_memory();
            for timeout in [250, 0, -1, c_int::MIN, c_int::MAX] {
                assert_eq!(sqlite3_busy_timeout(db, timeout), SQLITE_OK);
                assert_eq!(
                    schema_query_int(db, "PRAGMA busy_timeout"),
                    i64::from(timeout.max(0))
                );
            }
            assert_eq!(sqlite3_last_insert_rowid(ptr::null_mut()), 0);
            assert_eq!(sqlite3_get_autocommit(ptr::null_mut()), 0);
            assert_eq!(sqlite3_busy_timeout(ptr::null_mut(), 100), SQLITE_MISUSE);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_open_close_memory() {
        unsafe {
            let db = open_memory();
            let rc = sqlite3_close(db);
            assert_eq!(rc, SQLITE_OK);
        }
    }

    /// bd-zqr9h: a write against a RESERVED lock held by another handle waits
    /// for busy_timeout and then returns SQLITE_BUSY, returns SQLITE_BUSY at
    /// once at busy_timeout 0, and succeeds once the holder commits. The
    /// engine's busy wait sleeps on a timer, so this also checks that the
    /// shim's driver is woken for timers.
    #[test]
    fn test_busy_timeout_waits_then_returns_busy() {
        unsafe fn exec(db: *mut Sqlite3, sql: &str) -> c_int {
            let sql = CString::new(sql).unwrap();
            sqlite3_exec(db, sql.as_ptr(), None, ptr::null_mut(), ptr::null_mut())
        }
        unsafe {
            let dir = tempfile::TempDir::new().unwrap();
            let path = CString::new(dir.path().join("busy.db").to_str().unwrap()).unwrap();
            let mut holder: *mut Sqlite3 = ptr::null_mut();
            let mut writer: *mut Sqlite3 = ptr::null_mut();
            assert_eq!(sqlite3_open(path.as_ptr(), &mut holder), SQLITE_OK);
            assert_eq!(
                exec(holder, "PRAGMA journal_mode=WAL; CREATE TABLE t(x);"),
                SQLITE_OK
            );
            assert_eq!(sqlite3_open(path.as_ptr(), &mut writer), SQLITE_OK);
            assert_eq!(exec(holder, "BEGIN IMMEDIATE;"), SQLITE_OK);
            assert_eq!(exec(holder, "INSERT INTO t VALUES (1);"), SQLITE_OK);
            for (timeout_ms, at_least, below) in [
                (
                    300_u64,
                    std::time::Duration::from_millis(250),
                    std::time::Duration::from_secs(5),
                ),
                (
                    0,
                    std::time::Duration::ZERO,
                    std::time::Duration::from_millis(250),
                ),
            ] {
                assert_eq!(
                    sqlite3_busy_timeout(writer, c_int::try_from(timeout_ms).unwrap()),
                    SQLITE_OK
                );
                let started = std::time::Instant::now();
                let rc = exec(writer, "INSERT INTO t VALUES (2);");
                let waited = started.elapsed();
                assert_eq!(rc, SQLITE_BUSY, "busy_timeout={timeout_ms}");
                assert!(
                    waited >= at_least && waited < below,
                    "busy_timeout={timeout_ms} waited {waited:?}"
                );
            }
            assert_eq!(exec(holder, "COMMIT;"), SQLITE_OK);
            assert_eq!(exec(writer, "INSERT INTO t VALUES (2);"), SQLITE_OK);
            assert_eq!(sqlite3_close(writer), SQLITE_OK);
            assert_eq!(sqlite3_close(holder), SQLITE_OK);
        }
    }

    /// bd-71vt5 (owner decision 2026-11-17): WAL-FEC generation is opt-in, and
    /// the C API has no runtime to run the repair worker on. A default database
    /// written at `synchronous = FULL` gets no `-wal-fec` or lock file, and
    /// opting in is refused with an error naming the PRAGMA and persists
    /// nothing. (The refusal to open a database opted in elsewhere is the same
    /// fsqlite-core path; fsqlite::blocking's
    /// wal_fec_is_off_by_default_and_an_opt_in_is_refused covers it.)
    #[test]
    fn test_wal_fec_is_off_by_default_and_an_opt_in_is_refused() {
        unsafe fn exec(db: *mut Sqlite3, sql: &str) -> c_int {
            let sql = CString::new(sql).unwrap();
            sqlite3_exec(db, sql.as_ptr(), None, ptr::null_mut(), ptr::null_mut())
        }
        unsafe fn errmsg(db: *mut Sqlite3) -> String {
            std::ffi::CStr::from_ptr(sqlite3_errmsg(db))
                .to_string_lossy()
                .into_owned()
        }
        unsafe {
            let dir = tempfile::TempDir::new().unwrap();
            let db_path = dir.path().join("fec.db");
            let sidecar = dir.path().join("fec.db-wal-fec");
            let lock = dir.path().join("fec.db-wal-fec.lock");
            let path = CString::new(db_path.to_str().unwrap()).unwrap();
            let mut db: *mut Sqlite3 = ptr::null_mut();
            assert_eq!(sqlite3_open(path.as_ptr(), &mut db), SQLITE_OK);
            assert_eq!(
                exec(
                    db,
                    "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; CREATE TABLE t(x); \
                     INSERT INTO t VALUES (1); INSERT INTO t VALUES (2);"
                ),
                SQLITE_OK
            );
            assert_ne!(exec(db, "PRAGMA raptorq_repair_symbols = 2;"), SQLITE_OK);
            assert!(
                errmsg(db).contains("raptorq_repair_symbols"),
                "{}",
                errmsg(db)
            );
            assert_eq!(exec(db, "INSERT INTO t VALUES (3);"), SQLITE_OK);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
            assert!(!sidecar.exists() && !lock.exists());
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
    fn test_open_named_file_database_roundtrip() {
        // bd-slgya regression guard: solo `-p fsqlite-c-api` builds get only
        // the facade features listed in this crate's manifest; if the native
        // pager is dropped there, file-backed opens fail at runtime with
        // NotImplemented while workspace-unified builds stay green.
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("roundtrip.db");
        let c_path = CString::new(path.to_str().unwrap()).unwrap();
        unsafe {
            let mut db: *mut Sqlite3 = ptr::null_mut();
            let rc = sqlite3_open(c_path.as_ptr(), &mut db);
            assert_eq!(rc, SQLITE_OK, "file-backed open failed");
            assert!(!db.is_null());

            let sql = CString::new("CREATE TABLE t(x INTEGER); INSERT INTO t VALUES(42);").unwrap();
            let rc = sqlite3_exec(db, sql.as_ptr(), None, ptr::null_mut(), ptr::null_mut());
            assert_eq!(rc, SQLITE_OK, "exec on file-backed database failed");
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
        assert!(path.exists(), "database file was not created on disk");
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

    unsafe fn schema_exec_ok(db: *mut Sqlite3, sql: &str) {
        let source = CString::new(sql).unwrap();
        assert_eq!(
            sqlite3_exec(db, source.as_ptr(), None, ptr::null_mut(), ptr::null_mut()),
            SQLITE_OK,
            "{sql}: {}",
            CStr::from_ptr(sqlite3_errmsg(db)).to_string_lossy()
        );
    }

    unsafe fn schema_query_int(db: *mut Sqlite3, sql: &str) -> i64 {
        let statement = prepare(db, sql);
        assert_eq!(sqlite3_step(statement), SQLITE_ROW, "{sql}");
        let value = sqlite3_column_int64(statement, 0);
        assert_eq!(sqlite3_step(statement), SQLITE_DONE, "{sql}");
        assert_eq!(sqlite3_finalize(statement), SQLITE_OK);
        value
    }

    /// bd-b8cfq: these errors must be raised by the C prepare boundary,
    /// before a caller can obtain a statement handle. Expected messages and
    /// return codes were checked against the bundled SQLite 3.54.1 C API.
    /// Each rejected prepare must leave the previous DML counters intact.
    #[test]
    fn test_prepare_schema_errors_match_stock_before_step() {
        let cases = [
            ("CREATE TABLE t(z)", "table t already exists"),
            ("CREATE VIEW v AS SELECT 1", "view v already exists"),
            ("CREATE TABLE ib(z)", "there is already an index named ib"),
            ("CREATE INDEX ib ON t(b)", "index ib already exists"),
            (
                "CREATE INDEX ix ON missing(b)",
                "no such table: main.missing",
            ),
            ("CREATE INDEX ix ON t(nosuch)", "no such column: nosuch"),
            ("CREATE INDEX ix ON v(a)", "views may not be indexed"),
            (
                "CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1; END",
                "trigger tr already exists",
            ),
            (
                "CREATE TRIGGER tr2 AFTER INSERT ON missing BEGIN SELECT 1; END",
                "no such table: main.missing",
            ),
            (
                "CREATE TRIGGER tr2 INSTEAD OF INSERT ON t BEGIN SELECT 1; END",
                "cannot create INSTEAD OF trigger on table: t",
            ),
            (
                "CREATE TABLE sqlite_x(a)",
                "object name reserved for internal use: sqlite_x",
            ),
            (
                "CREATE TEMP TABLE main.q(a)",
                "temporary table name must be unqualified",
            ),
            ("CREATE TABLE aux.q(a)", "unknown database aux"),
            ("DROP TABLE missing", "no such table: missing"),
            ("DROP INDEX missing", "no such index: missing"),
            ("DROP VIEW missing", "no such view: missing"),
            ("DROP TRIGGER missing", "no such trigger: missing"),
            ("DROP VIEW t", "use DROP TABLE to delete table t"),
            ("DROP TABLE v", "use DROP VIEW to delete view v"),
            ("ALTER TABLE missing ADD COLUMN c", "no such table: missing"),
            ("ALTER TABLE t ADD COLUMN b", "duplicate column name: b"),
            (
                "ALTER TABLE t RENAME TO v",
                "there is already another table or index with this name: v",
            ),
            ("ANALYZE missing", "no such table: missing"),
            (
                "REINDEX missing",
                "unable to identify the object to be reindexed",
            ),
        ];
        for file_backed in [false, true] {
            let dir = tempfile::TempDir::new().unwrap();
            let path = if file_backed {
                CString::new(dir.path().join("prepare.db").to_str().unwrap()).unwrap()
            } else {
                CString::new(":memory:").unwrap()
            };
            unsafe {
                let mut db = ptr::null_mut();
                assert_eq!(sqlite3_open(path.as_ptr(), &mut db), SQLITE_OK);
                schema_exec_ok(
                    db,
                    "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT); \
                     INSERT INTO t VALUES(1, 'x'), (2, 'y'); \
                     CREATE INDEX ib ON t(b); \
                     CREATE VIEW v AS SELECT a, b FROM t; \
                     CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1; END;",
                );
                for (sql, expected) in cases {
                    let source = CString::new(sql).unwrap();
                    let mut statement = ptr::null_mut();
                    let rc = sqlite3_prepare_v2(
                        db,
                        source.as_ptr(),
                        -1,
                        &mut statement,
                        ptr::null_mut(),
                    );
                    let message = CStr::from_ptr(sqlite3_errmsg(db))
                        .to_string_lossy()
                        .into_owned();
                    let no_statement = statement.is_null();
                    if !no_statement {
                        sqlite3_finalize(statement);
                    }
                    assert_eq!(rc, SQLITE_ERROR, "{sql}, file={file_backed}: {message}");
                    assert_eq!(message, expected, "{sql}, file={file_backed}");
                    assert!(no_statement, "a failed prepare returned a handle: {sql}");
                    assert_eq!(sqlite3_changes(db), 2, "{sql}");
                    assert_eq!(sqlite3_total_changes(db), 2, "{sql}");
                }
                assert_eq!(schema_query_int(db, "SELECT count(*) FROM t"), 2);
                assert_eq!(sqlite3_close(db), SQLITE_OK);
            }
        }
    }

    /// Preparing DDL and control statements validates them without applying
    /// them. Dropping the prepared handle must not mutate schema, rows,
    /// transaction state, or the set of attached databases.
    #[test]
    fn test_prepare_schema_statements_have_no_side_effects() {
        unsafe {
            let db = open_memory();
            schema_exec_ok(
                db,
                "CREATE TABLE t(a, b); INSERT INTO t VALUES(1, 'x'), (2, 'y'); \
                 CREATE INDEX ib ON t(b);",
            );
            for sql in [
                "CREATE TABLE unseen(x)",
                "CREATE TABLE copied AS SELECT * FROM t",
                "CREATE INDEX ia ON t(a)",
                "CREATE VIEW v AS SELECT * FROM t",
                "CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1; END",
                "ALTER TABLE t ADD COLUMN c",
                "ALTER TABLE t RENAME TO renamed",
                "DROP TABLE t",
                "DROP INDEX ib",
                "BEGIN",
                "SAVEPOINT sp",
                "ATTACH ':memory:' AS aux",
                "ANALYZE",
                "REINDEX",
            ] {
                let statement = prepare(db, sql);
                assert_eq!(sqlite3_column_count(statement), 0, "{sql}");
                assert_eq!(sqlite3_finalize(statement), SQLITE_OK, "{sql}");
            }
            assert_eq!(
                schema_query_int(db, "SELECT count(*) FROM sqlite_schema"),
                2
            );
            assert_eq!(schema_query_int(db, "SELECT count(*) FROM t"), 2);
            assert_eq!(
                schema_query_int(db, "SELECT count(*) FROM pragma_table_info('t')"),
                2
            );
            assert_eq!(
                schema_query_int(db, "SELECT count(*) FROM pragma_database_list"),
                1
            );
            schema_exec_ok(db, "BEGIN; ROLLBACK;");
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    /// Held schema statements recheck the live schema when stepped. Bound
    /// ATTACH arguments and transaction errors still belong to execution.
    #[test]
    fn test_prepare_schema_statements_revalidate_and_bind_at_step() {
        unsafe {
            let db = open_memory();
            let statement = prepare(db, "CREATE TABLE held(a)");
            schema_exec_ok(db, "CREATE TABLE held(b)");
            assert_eq!(sqlite3_step(statement), SQLITE_ERROR);
            assert_eq!(
                CStr::from_ptr(sqlite3_errmsg(db)).to_str().unwrap(),
                "table held already exists"
            );
            assert_eq!(sqlite3_finalize(statement), SQLITE_ERROR);

            let statement = prepare(db, "CREATE TABLE repeated(a)");
            assert_eq!(sqlite3_step(statement), SQLITE_DONE);
            assert_eq!(sqlite3_reset(statement), SQLITE_OK);
            assert_eq!(sqlite3_step(statement), SQLITE_ERROR);
            assert_eq!(sqlite3_finalize(statement), SQLITE_ERROR);

            let statement = prepare(db, "ATTACH ?1 AS aux");
            assert_eq!(sqlite3_bind_parameter_count(statement), 1);
            let memory = CString::new(":memory:").unwrap();
            assert_eq!(
                sqlite3_bind_text(statement, 1, memory.as_ptr(), -1, None),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(statement), SQLITE_DONE);
            assert_eq!(sqlite3_finalize(statement), SQLITE_OK);
            assert_eq!(
                schema_query_int(db, "SELECT count(*) FROM pragma_database_list"),
                2
            );
            schema_exec_ok(db, "DETACH aux");

            let statement = prepare(db, "COMMIT");
            assert_eq!(sqlite3_step(statement), SQLITE_ERROR);
            assert_eq!(
                CStr::from_ptr(sqlite3_errmsg(db)).to_str().unwrap(),
                "cannot commit - no transaction is active"
            );
            assert_eq!(sqlite3_finalize(statement), SQLITE_ERROR);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    /// bd-7c6m7: preparing parenless eponymous pragma TVFs (`pragma_database_list`,
    /// `pragma_collation_list`, `pragma_function_list`, `pragma_compile_options`,
    /// `pragma_table_list`) via `sqlite3_prepare_v2` must succeed with stock-exact
    /// columns and respect CTE / table shadowing.
    #[test]
    fn test_prepare_bare_pragma_table_functions_and_shadowing() {
        unsafe {
            let db = open_memory();
            schema_exec_ok(db, "CREATE TABLE t(a INTEGER, b TEXT);");

            assert_eq!(
                schema_query_int(db, "SELECT count(*) FROM pragma_database_list"),
                1
            );
            assert!(schema_query_int(db, "SELECT count(*) FROM pragma_collation_list") >= 3);
            assert!(schema_query_int(db, "SELECT count(*) FROM pragma_function_list") > 10);
            assert!(schema_query_int(db, "SELECT count(*) FROM pragma_compile_options") >= 1);

            let stmt = prepare(db, "SELECT * FROM pragma_database_list");
            assert_eq!(sqlite3_column_count(stmt), 3);
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            assert_eq!(sqlite3_column_int64(stmt, 0), 0);
            assert_eq!(
                CStr::from_ptr(sqlite3_column_text(stmt, 1))
                    .to_str()
                    .unwrap(),
                "main"
            );
            assert_eq!(
                CStr::from_ptr(sqlite3_column_text(stmt, 2))
                    .to_str()
                    .unwrap(),
                ""
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

            // Hidden columns of pragma_table_list must be omitted by `SELECT *`.
            let tl = prepare(db, "SELECT * FROM pragma_table_list");
            assert_eq!(sqlite3_column_count(tl), 6);
            assert_eq!(sqlite3_finalize(tl), SQLITE_OK);

            // A CTE or real table with the same name shadows the bare pragma TVF.
            assert_eq!(
                schema_query_int(
                    db,
                    "WITH pragma_database_list(x) AS (SELECT 42) SELECT x FROM pragma_database_list"
                ),
                42
            );
            schema_exec_ok(
                db,
                "CREATE TABLE pragma_database_list(x); INSERT INTO pragma_database_list VALUES (99);",
            );
            assert_eq!(
                schema_query_int(db, "SELECT x FROM pragma_database_list"),
                99
            );
            assert_eq!(sqlite3_close(db), SQLITE_OK);
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

    unsafe fn result_column_names(stmt: *mut Sqlite3Stmt) -> Vec<String> {
        (0..sqlite3_column_count(stmt))
            .map(|index| {
                let name = sqlite3_column_name(stmt, index);
                assert!(!name.is_null());
                CStr::from_ptr(name).to_str().unwrap().to_owned()
            })
            .collect()
    }

    #[test]
    fn test_column_name_pointers_survive_steps_conversions_and_reset() {
        unsafe {
            let db = open_memory();
            let stmt = prepare(
                db,
                r#"SELECT 7 AS "κόσμος", 'first' AS "spaced name" UNION ALL SELECT 8, 'next'"#,
            );
            assert_eq!(result_column_names(stmt), ["κόσμος", "spaced name"]);
            let first_name = sqlite3_column_name(stmt, 0);
            let second_name = sqlite3_column_name(stmt, 1);
            for expected in [7, 8] {
                assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
                assert_eq!(sqlite3_column_int64(stmt, 0), expected);
                assert!(!sqlite3_column_text(stmt, 0).is_null());
                assert!(!sqlite3_column_blob(stmt, 1).is_null());
                assert_eq!(sqlite3_column_name(stmt, 0), first_name);
                assert_eq!(sqlite3_column_name(stmt, 1), second_name);
                assert_eq!(result_column_names(stmt), ["κόσμος", "spaced name"]);
            }
            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
            assert_eq!(sqlite3_column_name(stmt, 0), first_name);
            assert_eq!(sqlite3_reset(stmt), SQLITE_OK);
            assert_eq!(sqlite3_column_name(stmt, 1), second_name);
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            assert_eq!(sqlite3_column_name(stmt, 0), first_name);
            assert_eq!(sqlite3_column_name(stmt, 1), second_name);
            for index in [-1, 2, c_int::MAX] {
                assert!(sqlite3_column_name(stmt, index).is_null());
            }
            assert!(sqlite3_column_name(ptr::null_mut(), 0).is_null());
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_column_names_for_pragma_explain_and_returning_before_step() {
        unsafe {
            let db = open_memory();
            schema_exec_ok(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, value TEXT)");
            let cases: &[(&str, &[&str])] = &[
                (
                    "PRAGMA table_info(t)",
                    &["cid", "name", "type", "notnull", "dflt_value", "pk"],
                ),
                (
                    "EXPLAIN SELECT * FROM t",
                    &["addr", "opcode", "p1", "p2", "p3", "p4", "p5", "comment"],
                ),
                (
                    "EXPLAIN QUERY PLAN SELECT * FROM t",
                    &["id", "parent", "notused", "detail"],
                ),
            ];
            for &(sql, names) in cases {
                let stmt = prepare(db, sql);
                assert_eq!(result_column_names(stmt), names, "{sql}");
                assert_eq!(sqlite3_step(stmt), SQLITE_ROW, "{sql}");
                assert_eq!(result_column_names(stmt), names, "{sql}");
                assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
            }

            let stmt = prepare(
                db,
                "INSERT INTO t VALUES (41, 'new') RETURNING id AS inserted_id, value",
            );
            assert_eq!(result_column_names(stmt), ["inserted_id", "value"]);
            assert_eq!(sqlite3_last_insert_rowid(db), 0);
            assert_eq!(schema_query_int(db, "SELECT count(*) FROM t"), 0);
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            assert_eq!(sqlite3_column_int64(stmt, 0), 41);
            assert_eq!(sqlite3_last_insert_rowid(db), 41);
            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
            assert_eq!(result_column_names(stmt), ["inserted_id", "value"]);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);

            let ddl = prepare(db, "CREATE TABLE empty_metadata(x)");
            assert_eq!(sqlite3_column_count(ddl), 0);
            assert!(sqlite3_column_name(ddl, 0).is_null());
            assert_eq!(sqlite3_step(ddl), SQLITE_DONE);
            assert_eq!(sqlite3_finalize(ddl), SQLITE_OK);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    // Preserve FrankenSQLite's existing deferred PRAGMA dispatch. SQLite applies
    // some setters during prepare; this regression does not establish timing parity.
    #[test]
    fn test_prepared_pragma_metadata_does_not_execute_setter() {
        unsafe {
            let db = open_memory();
            assert_eq!(sqlite3_busy_timeout(db, 123), SQLITE_OK);
            let stmt = prepare(db, "PRAGMA busy_timeout = 456");
            assert_eq!(result_column_names(stmt), ["timeout"]);
            assert_eq!(schema_query_int(db, "PRAGMA busy_timeout"), 123);
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            assert_eq!(sqlite3_column_int(stmt, 0), 456);
            assert_eq!(result_column_names(stmt), ["timeout"]);
            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
            assert_eq!(schema_query_int(db, "PRAGMA busy_timeout"), 456);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_column_count_preserved_for_empty_result_and_reset() {
        unsafe {
            let db = open_memory();

            let sql = CString::new("SELECT 1 AS empty_label WHERE 0;").unwrap();
            let mut stmt: *mut Sqlite3Stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, ptr::null_mut()),
                SQLITE_OK
            );
            assert!(!stmt.is_null());

            assert_eq!(sqlite3_column_count(stmt), 1);
            assert_eq!(result_column_names(stmt), ["empty_label"]);
            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
            assert_eq!(sqlite3_column_count(stmt), 1);
            assert_eq!(result_column_names(stmt), ["empty_label"]);

            assert_eq!(sqlite3_reset(stmt), SQLITE_OK);
            assert_eq!(sqlite3_column_count(stmt), 1);
            assert_eq!(result_column_names(stmt), ["empty_label"]);

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

            captured_names.clear();
            let sql = CString::new("PRAGMA busy_timeout").unwrap();
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
            assert_eq!(captured_names, ["timeout"]);

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

    /// (prepare rc, last step rc or -1, sqlite3_changes, sqlite3_total_changes,
    /// first column of the first row) after one statement.
    type ChangeLine = (c_int, c_int, c_int, c_int, Option<i64>);

    /// Run each statement through prepare_v2 / step / finalize, as a C caller
    /// does, and record its [`ChangeLine`].
    unsafe fn change_counts_after(db: *mut Sqlite3, statements: &[&str]) -> Vec<ChangeLine> {
        let mut seen = Vec::with_capacity(statements.len());
        for sql in statements {
            let c = CString::new(*sql).unwrap();
            let mut stmt: *mut Sqlite3Stmt = ptr::null_mut();
            let prepare_rc = sqlite3_prepare_v2(db, c.as_ptr(), -1, &mut stmt, ptr::null_mut());
            let mut step_rc = -1;
            let mut value = None;
            if prepare_rc == SQLITE_OK && !stmt.is_null() {
                loop {
                    step_rc = sqlite3_step(stmt);
                    if step_rc != SQLITE_ROW {
                        break;
                    }
                    value.get_or_insert_with(|| sqlite3_column_int64(stmt, 0));
                }
                sqlite3_finalize(stmt);
            }
            seen.push((
                prepare_rc,
                step_rc,
                sqlite3_changes(db),
                sqlite3_total_changes(db),
                value,
            ));
        }
        seen
    }

    /// bd-zqr9h L2: sqlite3_changes and sqlite3_total_changes read the
    /// connection's own counters. These are the 19 statements of the bead's
    /// dlopen differential, run as a C caller runs them; `stock` is stock
    /// libsqlite3 3.54.1's line for each, through the same calls. A statement
    /// that fails to prepare keeps the count (#4, #8); one that fails while
    /// running (a UNIQUE violation part-way through a multi-row UPDATE, #11)
    /// leaves 0 and adds nothing to the total; a successful UPDATE of no rows
    /// sets 0 (#13); DDL and SELECT keep the count. The cached count this
    /// replaces kept 2 after #11, and the shim had no sqlite3_total_changes.
    ///
    /// #6 is the one line whose return codes are not stock's. Stock rejects
    /// `UPDATE w SET (x, nope) = (SELECT 1, 2, 3)` in sqlite3_prepare_v2
    /// ("no such column: nope"); fsqlite's prepare accepts it and its step
    /// fails. That prepare acceptance is bd-4l4y3, whose probe owns stock's
    /// line (prepare 1, no step); this test pins main's return codes there.
    /// Its counts are stock's: since bd-rwo47 (2a359ad03) a statement that
    /// fails with a prepare-time error keeps the count wherever fsqlite
    /// raises the error, so #6 to #9 read 3 as in stock.
    #[test]
    fn test_changes_and_total_changes_match_stock() {
        unsafe {
            let dir = tempfile::TempDir::new().unwrap();
            let path = CString::new(dir.path().join("changes.db").to_str().unwrap()).unwrap();
            let mut db: *mut Sqlite3 = ptr::null_mut();
            assert_eq!(sqlite3_open(path.as_ptr(), &mut db), SQLITE_OK);
            let statements = [
                "CREATE TABLE w(x INTEGER PRIMARY KEY, y UNIQUE)",
                "INSERT INTO w VALUES (1, 10), (2, 20), (3, 30)",
                "SELECT count(*) FROM w",
                "UPDATE nosuch SET x = 1",
                "SELECT changes()",
                "UPDATE w SET (x, nope) = (SELECT 1, 2, 3)",
                "SELECT changes()",
                "SELEC 1",
                "SELECT changes()",
                "UPDATE w SET y = y + 1 WHERE x <= 2",
                "UPDATE w SET y = 21 WHERE x >= 1",
                "SELECT changes()",
                "UPDATE w SET y = y WHERE x = 99",
                "INSERT INTO w VALUES (4, 40)",
                "CREATE TABLE z(a)",
                "SELECT changes()",
                "DELETE FROM w WHERE x >= 3",
                "SELECT changes()",
                "SELECT total_changes()",
            ];
            let (ok, err, done) = (SQLITE_OK, SQLITE_ERROR, SQLITE_DONE);
            let stock = [
                (ok, done, 0, 0, None),
                (ok, done, 3, 3, None),
                (ok, done, 3, 3, Some(3)),
                (err, -1, 3, 3, None),
                (ok, done, 3, 3, Some(3)),
                (err, -1, 3, 3, None),
                (ok, done, 3, 3, Some(3)),
                (err, -1, 3, 3, None),
                (ok, done, 3, 3, Some(3)),
                (ok, done, 2, 5, None),
                (ok, SQLITE_CONSTRAINT, 0, 5, None),
                (ok, done, 0, 5, Some(0)),
                (ok, done, 0, 5, None),
                (ok, done, 1, 6, None),
                (ok, done, 1, 6, None),
                (ok, done, 1, 6, Some(1)),
                (ok, done, 2, 8, None),
                (ok, done, 2, 8, Some(2)),
                (ok, done, 2, 8, Some(8)),
            ];
            // #6: main's return codes (bd-4l4y3), stock's counts.
            let mut want_lines = stock;
            want_lines[5] = (ok, err, 3, 3, None);
            let seen = change_counts_after(db, &statements);
            assert_eq!(seen.len(), statements.len());
            for ((sql, want), got) in statements.iter().zip(want_lines).zip(&seen) {
                assert_eq!(*got, want, "{sql}");
            }
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    // ── Parameter binding (bd-w2i2w) ────────────────────────────────

    unsafe fn prepare(db: *mut Sqlite3, sql: &str) -> *mut Sqlite3Stmt {
        let c = CString::new(sql).unwrap();
        let mut stmt: *mut Sqlite3Stmt = ptr::null_mut();
        assert_eq!(
            sqlite3_prepare_v2(db, c.as_ptr(), -1, &mut stmt, ptr::null_mut()),
            SQLITE_OK,
            "prepare failed for: {sql}"
        );
        assert!(!stmt.is_null());
        stmt
    }

    #[test]
    fn step_after_done_reexecutes_with_retained_bindings() {
        unsafe {
            let db = open_memory();
            schema_exec_ok(db, "CREATE TABLE t(v INTEGER)");
            let insert = prepare(db, "INSERT INTO t VALUES (?1)");
            let select = prepare(db, "SELECT v FROM t ORDER BY rowid");

            // The empty SELECT reaches DONE immediately. Its next execution
            // must see subsequent writes.
            assert_eq!(sqlite3_step(select), SQLITE_DONE);
            assert_eq!(sqlite3_bind_int(insert, 1, 42), SQLITE_OK);
            assert_eq!(sqlite3_step(insert), SQLITE_DONE);
            assert_eq!(sqlite3_step(insert), SQLITE_DONE);
            assert_eq!(sqlite3_total_changes(db), 2);

            for _ in 0..2 {
                assert_eq!(sqlite3_step(select), SQLITE_ROW);
                assert_eq!(sqlite3_column_int(select, 0), 42);
            }
            assert_eq!(sqlite3_step(select), SQLITE_DONE);
            schema_exec_ok(db, "DELETE FROM t");
            assert_eq!(sqlite3_step(select), SQLITE_DONE);
            assert_eq!(sqlite3_finalize(select), SQLITE_OK);
            assert_eq!(sqlite3_finalize(insert), SQLITE_OK);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_bind_positional_roundtrip() {
        unsafe {
            let db = open_memory();
            let setup = CString::new("CREATE TABLE t(id INTEGER, name TEXT, score REAL);").unwrap();
            assert_eq!(
                sqlite3_exec(db, setup.as_ptr(), None, ptr::null_mut(), ptr::null_mut()),
                SQLITE_OK
            );

            let ins = prepare(db, "INSERT INTO t VALUES(?, ?, ?);");
            assert_eq!(sqlite3_bind_parameter_count(ins), 3);
            assert_eq!(sqlite3_bind_int(ins, 1, 42), SQLITE_OK);
            let name = CString::new("hello").unwrap();
            assert_eq!(
                sqlite3_bind_text(ins, 2, name.as_ptr(), -1, None),
                SQLITE_OK
            );
            assert_eq!(sqlite3_bind_double(ins, 3, 3.5), SQLITE_OK);
            assert_eq!(sqlite3_step(ins), SQLITE_DONE);
            sqlite3_finalize(ins);

            // Bound parameter in a SELECT WHERE clause round-trips.
            let sel = prepare(db, "SELECT id, name, score FROM t WHERE id = ?;");
            assert_eq!(sqlite3_bind_int(sel, 1, 42), SQLITE_OK);
            assert_eq!(sqlite3_step(sel), SQLITE_ROW);
            assert_eq!(sqlite3_column_int64(sel, 0), 42);
            let text = sqlite3_column_text(sel, 1);
            assert_eq!(CStr::from_ptr(text.cast::<c_char>()).to_bytes(), b"hello");
            assert!((sqlite3_column_double(sel, 2) - 3.5).abs() < 1e-9);
            assert_eq!(sqlite3_step(sel), SQLITE_DONE);
            sqlite3_finalize(sel);

            // A non-matching bind yields no rows (proves the value reaches the engine).
            let sel2 = prepare(db, "SELECT id FROM t WHERE id = ?;");
            assert_eq!(sqlite3_bind_int(sel2, 1, 999), SQLITE_OK);
            assert_eq!(sqlite3_step(sel2), SQLITE_DONE);
            sqlite3_finalize(sel2);

            sqlite3_close(db);
        }
    }

    #[test]
    fn test_bind_parameter_count_variants() {
        unsafe {
            let db = open_memory();
            let cases: &[(&str, c_int)] = &[
                ("SELECT 1;", 0),
                ("SELECT ?, ?, ?;", 3),
                ("SELECT ?1, ?2;", 2),
                // ?1 reused collapses to one slot; ?3 pushes the count to 3.
                ("SELECT ?1, ?1, ?3;", 3),
                ("SELECT :a, @b, $c;", 3),
                // A '?' inside a string literal is not a parameter.
                ("SELECT ?, 'literal ? mark';", 1),
            ];
            for (sql, expected) in cases {
                let stmt = prepare(db, sql);
                assert_eq!(
                    sqlite3_bind_parameter_count(stmt),
                    *expected,
                    "count mismatch for {sql}"
                );
                sqlite3_finalize(stmt);
            }
            sqlite3_close(db);
        }
    }

    #[test]
    fn test_bind_named_parameters() {
        unsafe {
            let db = open_memory();
            let setup = CString::new("CREATE TABLE t(a INTEGER, b INTEGER);").unwrap();
            assert_eq!(
                sqlite3_exec(db, setup.as_ptr(), None, ptr::null_mut(), ptr::null_mut()),
                SQLITE_OK
            );

            let ins = prepare(db, "INSERT INTO t VALUES(:first, @second);");
            assert_eq!(sqlite3_bind_parameter_count(ins), 2);

            let n1 = CString::new(":first").unwrap();
            let n2 = CString::new("@second").unwrap();
            assert_eq!(sqlite3_bind_parameter_index(ins, n1.as_ptr()), 1);
            assert_eq!(sqlite3_bind_parameter_index(ins, n2.as_ptr()), 2);
            let missing = CString::new(":nope").unwrap();
            assert_eq!(sqlite3_bind_parameter_index(ins, missing.as_ptr()), 0);

            assert_eq!(
                CStr::from_ptr(sqlite3_bind_parameter_name(ins, 1)).to_bytes(),
                b":first"
            );
            assert_eq!(
                CStr::from_ptr(sqlite3_bind_parameter_name(ins, 2)).to_bytes(),
                b"@second"
            );

            // Bind by resolved index and confirm the row lands.
            assert_eq!(sqlite3_bind_int(ins, 1, 10), SQLITE_OK);
            assert_eq!(sqlite3_bind_int(ins, 2, 20), SQLITE_OK);
            assert_eq!(sqlite3_step(ins), SQLITE_DONE);
            sqlite3_finalize(ins);

            let sel = prepare(db, "SELECT a + b FROM t;");
            assert_eq!(sqlite3_step(sel), SQLITE_ROW);
            assert_eq!(sqlite3_column_int64(sel, 0), 30);
            sqlite3_finalize(sel);
            sqlite3_close(db);
        }
    }

    #[test]
    fn test_bind_range_and_nameless() {
        unsafe {
            let db = open_memory();
            let stmt = prepare(db, "SELECT ?, ?2;");
            assert_eq!(sqlite3_bind_parameter_count(stmt), 2);
            // Out-of-range indices report SQLITE_RANGE without corrupting state.
            assert_eq!(sqlite3_bind_int(stmt, 0, 1), SQLITE_RANGE);
            assert_eq!(sqlite3_bind_int(stmt, 3, 1), SQLITE_RANGE);
            // '?' and '?2' are nameless → no parameter name.
            assert!(sqlite3_bind_parameter_name(stmt, 1).is_null());
            assert!(sqlite3_bind_parameter_name(stmt, 2).is_null());
            sqlite3_finalize(stmt);
            sqlite3_close(db);
        }
    }

    #[test]
    fn test_bind_null_and_clear_bindings() {
        unsafe {
            let db = open_memory();
            let setup = CString::new("CREATE TABLE t(v);").unwrap();
            assert_eq!(
                sqlite3_exec(db, setup.as_ptr(), None, ptr::null_mut(), ptr::null_mut()),
                SQLITE_OK
            );

            let ins = prepare(db, "INSERT INTO t VALUES(?);");
            assert_eq!(sqlite3_bind_int(ins, 1, 7), SQLITE_OK);
            // clear_bindings resets the slot to NULL before stepping.
            assert_eq!(sqlite3_clear_bindings(ins), SQLITE_OK);
            assert_eq!(sqlite3_step(ins), SQLITE_DONE);
            sqlite3_finalize(ins);

            let sel = prepare(db, "SELECT v, v IS NULL FROM t;");
            assert_eq!(sqlite3_step(sel), SQLITE_ROW);
            assert_eq!(sqlite3_column_type(sel, 0), SQLITE_NULL);
            assert_eq!(sqlite3_column_int64(sel, 1), 1);
            sqlite3_finalize(sel);
            sqlite3_close(db);
        }
    }

    #[test]
    fn test_bind_blob_and_zeroblob() {
        unsafe {
            let db = open_memory();
            let setup = CString::new("CREATE TABLE t(id INTEGER, data BLOB);").unwrap();
            assert_eq!(
                sqlite3_exec(db, setup.as_ptr(), None, ptr::null_mut(), ptr::null_mut()),
                SQLITE_OK
            );

            let ins = prepare(db, "INSERT INTO t VALUES(?, ?);");
            let blob: [u8; 4] = [0xDE, 0xAD, 0xBE, 0xEF];
            assert_eq!(sqlite3_bind_int(ins, 1, 1), SQLITE_OK);
            assert_eq!(
                sqlite3_bind_blob(ins, 2, blob.as_ptr().cast(), 4, None),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(ins), SQLITE_DONE);
            sqlite3_finalize(ins);

            let ins2 = prepare(db, "INSERT INTO t VALUES(?, ?);");
            assert_eq!(sqlite3_bind_int(ins2, 1, 2), SQLITE_OK);
            assert_eq!(sqlite3_bind_zeroblob(ins2, 2, 3), SQLITE_OK);
            assert_eq!(sqlite3_step(ins2), SQLITE_DONE);
            sqlite3_finalize(ins2);

            let sel = prepare(db, "SELECT length(data) FROM t ORDER BY id;");
            assert_eq!(sqlite3_step(sel), SQLITE_ROW);
            assert_eq!(sqlite3_column_int64(sel, 0), 4);
            assert_eq!(sqlite3_step(sel), SQLITE_ROW);
            assert_eq!(sqlite3_column_int64(sel, 0), 3);
            sqlite3_finalize(sel);
            sqlite3_close(db);
        }
    }

    #[test]
    fn test_bind_text_preserves_embedded_nul() {
        unsafe {
            let db = open_memory();
            let setup = CString::new("CREATE TABLE t(v TEXT);").unwrap();
            assert_eq!(
                sqlite3_exec(db, setup.as_ptr(), None, ptr::null_mut(), ptr::null_mut()),
                SQLITE_OK
            );

            let ins = prepare(db, "INSERT INTO t VALUES(?);");
            // Explicit length keeps the embedded NUL, unlike the -1 strlen form.
            let raw = b"a\0b";
            assert_eq!(
                sqlite3_bind_text(ins, 1, raw.as_ptr().cast(), 3, None),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(ins), SQLITE_DONE);
            sqlite3_finalize(ins);

            // The stored value keeps all three bytes (verified by the byte count
            // accessor; SQL `length()` would stop at the NUL by C-string rule).
            let sel = prepare(db, "SELECT v FROM t;");
            assert_eq!(sqlite3_step(sel), SQLITE_ROW);
            assert_eq!(sqlite3_column_bytes(sel, 0), 3);
            let text = sqlite3_column_text(sel, 0);
            assert!(!text.is_null());
            let bytes = std::slice::from_raw_parts(text.cast::<u8>(), 3);
            assert_eq!(bytes, b"a\0b");
            sqlite3_finalize(sel);
            sqlite3_close(db);
        }
    }

    #[test]
    fn test_bind_destructor_is_invoked_for_custom() {
        unsafe {
            static CALLS: AtomicUsize = AtomicUsize::new(0);
            unsafe extern "C" fn dtor(_ptr: *mut c_void) {
                CALLS.fetch_add(1, Ordering::Relaxed);
            }

            let db = open_memory();
            let stmt = prepare(db, "SELECT ?;");
            let text = CString::new("x").unwrap();
            // A real destructor is called once the bytes are copied; STATIC/None
            // and the TRANSIENT sentinel are not.
            assert_eq!(
                sqlite3_bind_text(stmt, 1, text.as_ptr(), -1, Some(dtor)),
                SQLITE_OK
            );
            assert_eq!(CALLS.load(Ordering::Relaxed), 1);

            let transient: SqliteDestructor =
                std::mem::transmute::<isize, SqliteDestructor>(SQLITE_TRANSIENT);
            assert_eq!(
                sqlite3_bind_text(stmt, 1, text.as_ptr(), -1, transient),
                SQLITE_OK
            );
            assert_eq!(CALLS.load(Ordering::Relaxed), 1, "TRANSIENT must not call");

            sqlite3_finalize(stmt);
            sqlite3_close(db);
        }
    }

    #[test]
    fn test_bind_destructor_invoked_on_error_paths() {
        // bd-3745a: stock SQLite disposes of a real destructor even when the
        // bind fails (out-of-range index → SQLITE_RANGE, null/misused stmt →
        // SQLITE_MISUSE). A heap-backed buffer must not leak on a failed bind.
        unsafe {
            static CALLS: AtomicUsize = AtomicUsize::new(0);
            unsafe extern "C" fn dtor(_ptr: *mut c_void) {
                CALLS.fetch_add(1, Ordering::Relaxed);
            }

            let db = open_memory();
            let stmt = prepare(db, "SELECT ?;"); // one parameter (index 1)
            let text = CString::new("leak-me").unwrap();
            let blob = [1_u8, 2, 3, 4];

            // Out-of-range index: RANGE, destructor still runs.
            assert_eq!(
                sqlite3_bind_text(stmt, 2, text.as_ptr(), -1, Some(dtor)),
                SQLITE_RANGE
            );
            assert_eq!(CALLS.load(Ordering::Relaxed), 1);
            assert_eq!(
                sqlite3_bind_blob(stmt, 2, blob.as_ptr().cast(), 4, Some(dtor)),
                SQLITE_RANGE
            );
            assert_eq!(CALLS.load(Ordering::Relaxed), 2);

            // Null statement: MISUSE, destructor still runs.
            assert_eq!(
                sqlite3_bind_text(ptr::null_mut(), 1, text.as_ptr(), -1, Some(dtor)),
                SQLITE_MISUSE
            );
            assert_eq!(CALLS.load(Ordering::Relaxed), 3);
            assert_eq!(
                sqlite3_bind_blob(ptr::null_mut(), 1, blob.as_ptr().cast(), 4, Some(dtor)),
                SQLITE_MISUSE
            );
            assert_eq!(CALLS.load(Ordering::Relaxed), 4);

            // The STATIC (None) sentinel is still a no-op on failure.
            assert_eq!(
                sqlite3_bind_text(stmt, 2, text.as_ptr(), -1, None),
                SQLITE_RANGE
            );
            assert_eq!(CALLS.load(Ordering::Relaxed), 4, "STATIC must not call");

            sqlite3_finalize(stmt);
            sqlite3_close(db);
        }
    }

    #[test]
    fn test_bind_on_unparameterized_is_range() {
        unsafe {
            let db = open_memory();
            let stmt = prepare(db, "SELECT 1;");
            assert_eq!(sqlite3_bind_parameter_count(stmt), 0);
            assert_eq!(sqlite3_bind_int(stmt, 1, 5), SQLITE_RANGE);
            // Unparameterized statements still execute normally.
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            assert_eq!(sqlite3_column_int64(stmt, 0), 1);
            sqlite3_finalize(stmt);
            sqlite3_close(db);
        }
    }

    #[test]
    fn test_bd_w9s20_c_api_held_select_star_reprepares_after_cross_and_same_conn_alter() {
        unsafe fn exec(db: *mut Sqlite3, sql: &str) -> c_int {
            let sql = CString::new(sql).unwrap();
            sqlite3_exec(db, sql.as_ptr(), None, ptr::null_mut(), ptr::null_mut())
        }
        unsafe {
            let dir = tempfile::TempDir::new().unwrap();
            let path = CString::new(dir.path().join("w9s20_capi.db").to_str().unwrap()).unwrap();
            let mut db_a: *mut Sqlite3 = ptr::null_mut();
            let mut db_b: *mut Sqlite3 = ptr::null_mut();
            assert_eq!(sqlite3_open(path.as_ptr(), &mut db_a), SQLITE_OK);
            assert_eq!(
                exec(
                    db_a,
                    "PRAGMA journal_mode=WAL;
                     CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT);
                     INSERT INTO t VALUES (1, 'x'), (2, 'y');
                     CREATE TABLE empty_t(a INTEGER PRIMARY KEY, b TEXT);"
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_open(path.as_ptr(), &mut db_b), SQLITE_OK);

            // Prepare SELECT * on connection A before connection B alters the table.
            let stmt = prepare(db_a, "SELECT * FROM t ORDER BY a;");
            let empty_stmt = prepare(db_a, "SELECT * FROM empty_t ORDER BY a;");
            assert_eq!(sqlite3_column_count(stmt), 2);
            assert_eq!(sqlite3_column_count(empty_stmt), 2);
            assert_eq!(result_column_names(stmt), ["a", "b"]);
            assert_eq!(result_column_names(empty_stmt), ["a", "b"]);

            // Connection B adds column c DEFAULT 7 to both tables.
            assert_eq!(
                exec(
                    db_b,
                    "ALTER TABLE t ADD COLUMN c INTEGER DEFAULT 7;
                     ALTER TABLE empty_t ADD COLUMN c INTEGER DEFAULT 7;"
                ),
                SQLITE_OK
            );

            // Stepping A's held statement must re-prepare and expand `*` to 3 columns.
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            assert_eq!(
                sqlite3_column_count(stmt),
                3,
                "cross-connection ALTER TABLE ADD COLUMN must widen sqlite3_column_count"
            );
            assert_eq!(result_column_names(stmt), ["a", "b", "c"]);
            assert_eq!(sqlite3_column_int64(stmt, 0), 1);
            assert_eq!(
                CStr::from_ptr(sqlite3_column_text(stmt, 1))
                    .to_str()
                    .unwrap(),
                "x"
            );
            assert_eq!(sqlite3_column_int64(stmt, 2), 7);

            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            assert_eq!(sqlite3_column_count(stmt), 3);
            assert_eq!(sqlite3_column_int64(stmt, 0), 2);
            assert_eq!(
                CStr::from_ptr(sqlite3_column_text(stmt, 1))
                    .to_str()
                    .unwrap(),
                "y"
            );
            assert_eq!(sqlite3_column_int64(stmt, 2), 7);
            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);

            // Empty table also widens sqlite3_column_count to 3 after stepping.
            assert_eq!(sqlite3_step(empty_stmt), SQLITE_DONE);
            assert_eq!(
                sqlite3_column_count(empty_stmt),
                3,
                "empty table cross-connection ALTER TABLE ADD COLUMN must widen sqlite3_column_count"
            );
            assert_eq!(result_column_names(empty_stmt), ["a", "b", "c"]);

            // Same-connection ALTER TABLE ADD COLUMN on A itself.
            assert_eq!(
                exec(db_a, "ALTER TABLE t ADD COLUMN d INTEGER DEFAULT 9;"),
                SQLITE_OK
            );
            assert_eq!(sqlite3_reset(stmt), SQLITE_OK);
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            assert_eq!(
                sqlite3_column_count(stmt),
                4,
                "same-connection ALTER TABLE ADD COLUMN must widen sqlite3_column_count"
            );
            assert_eq!(result_column_names(stmt), ["a", "b", "c", "d"]);
            assert_eq!(sqlite3_column_int64(stmt, 3), 9);

            assert_eq!(sqlite3_finalize(empty_stmt), SQLITE_OK);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
            assert_eq!(sqlite3_close(db_b), SQLITE_OK);
            assert_eq!(sqlite3_close(db_a), SQLITE_OK);
        }
    }

    #[test]
    fn test_bd_w9s20_c_api_held_select_reprepares_on_reverse_unordered_selects() {
        unsafe fn exec(db: *mut Sqlite3, sql: &str) -> c_int {
            let sql = CString::new(sql).unwrap();
            sqlite3_exec(db, sql.as_ptr(), None, ptr::null_mut(), ptr::null_mut())
        }
        unsafe fn collect_first_col(stmt: *mut Sqlite3Stmt) -> Vec<i64> {
            let mut vals = Vec::new();
            loop {
                match sqlite3_step(stmt) {
                    SQLITE_ROW => vals.push(sqlite3_column_int64(stmt, 0)),
                    SQLITE_DONE => break,
                    other => panic!("unexpected step code {other}"),
                }
            }
            vals
        }
        unsafe {
            let db = open_memory();
            assert_eq!(
                exec(
                    db,
                    "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT);
                     INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c');"
                ),
                SQLITE_OK
            );
            let stmt_col = prepare(db, "SELECT a FROM t;");
            let stmt_star = prepare(db, "SELECT * FROM t;");

            assert_eq!(collect_first_col(stmt_col), vec![1, 2, 3]);
            assert_eq!(collect_first_col(stmt_star), vec![1, 2, 3]);

            assert_eq!(
                exec(db, "PRAGMA reverse_unordered_selects = ON;"),
                SQLITE_OK
            );
            assert_eq!(sqlite3_reset(stmt_col), SQLITE_OK);
            assert_eq!(sqlite3_reset(stmt_star), SQLITE_OK);
            assert_eq!(
                collect_first_col(stmt_col),
                vec![3, 2, 1],
                "held C API SELECT a must reverse when PRAGMA reverse_unordered_selects = ON"
            );
            assert_eq!(
                collect_first_col(stmt_star),
                vec![3, 2, 1],
                "held C API SELECT * must reverse when PRAGMA reverse_unordered_selects = ON"
            );

            assert_eq!(
                exec(db, "PRAGMA reverse_unordered_selects = OFF;"),
                SQLITE_OK
            );
            assert_eq!(sqlite3_reset(stmt_col), SQLITE_OK);
            assert_eq!(sqlite3_reset(stmt_star), SQLITE_OK);
            assert_eq!(collect_first_col(stmt_col), vec![1, 2, 3]);
            assert_eq!(collect_first_col(stmt_star), vec![1, 2, 3]);

            assert_eq!(sqlite3_finalize(stmt_star), SQLITE_OK);
            assert_eq!(sqlite3_finalize(stmt_col), SQLITE_OK);
            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    #[test]
    fn test_bd_w9s20_c_api_held_statement_reuses_program_without_schema_change() {
        unsafe fn exec(db: *mut Sqlite3, sql: &str) -> c_int {
            let sql = CString::new(sql).unwrap();
            sqlite3_exec(db, sql.as_ptr(), None, ptr::null_mut(), ptr::null_mut())
        }
        unsafe {
            let dir = tempfile::TempDir::new().unwrap();
            let path = CString::new(dir.path().join("w9s20_reuse.db").to_str().unwrap()).unwrap();
            let mut db_a: *mut Sqlite3 = ptr::null_mut();
            let mut db_b: *mut Sqlite3 = ptr::null_mut();
            assert_eq!(sqlite3_open(path.as_ptr(), &mut db_a), SQLITE_OK);
            assert_eq!(
                exec(
                    db_a,
                    "PRAGMA journal_mode=WAL;
                     CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT);
                     INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c');"
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_open(path.as_ptr(), &mut db_b), SQLITE_OK);

            let stmt = prepare(db_a, "SELECT * FROM t;");
            let initial_id = (*stmt)
                .prepared
                .as_ref()
                .expect("held PreparedStatement")
                .program_identity();
            assert_eq!((*stmt).prepared.as_ref().unwrap().reprepare_count(), 0);

            // Negative case: 5 reset+step cycles with NO schema/pragma change
            // must reuse the exact same compiled program with 0 re-prepares.
            for _ in 0..5 {
                assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
                assert_eq!(sqlite3_column_count(stmt), 2);
                assert_eq!(sqlite3_column_int64(stmt, 0), 1);
                assert_eq!(sqlite3_reset(stmt), SQLITE_OK);
                let prep = (*stmt).prepared.as_ref().unwrap();
                assert_eq!(prep.reprepare_count(), 0);
                assert_eq!(prep.program_identity(), initial_id);
            }

            // Cross-connection ALTER TABLE -> re-prepares ONCE, then reuses 4 times.
            assert_eq!(
                exec(db_b, "ALTER TABLE t ADD COLUMN c INTEGER DEFAULT 7;"),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            assert_eq!(sqlite3_column_count(stmt), 3);
            assert_eq!(sqlite3_column_int64(stmt, 2), 7);
            assert_eq!(sqlite3_reset(stmt), SQLITE_OK);
            let after_alter_id = (*stmt).prepared.as_ref().unwrap().program_identity();
            assert_eq!((*stmt).prepared.as_ref().unwrap().reprepare_count(), 1);
            assert_ne!(after_alter_id, initial_id);

            for _ in 0..4 {
                assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
                assert_eq!(sqlite3_column_count(stmt), 3);
                assert_eq!(sqlite3_reset(stmt), SQLITE_OK);
                let prep = (*stmt).prepared.as_ref().unwrap();
                assert_eq!(prep.reprepare_count(), 1);
                assert_eq!(prep.program_identity(), after_alter_id);
            }

            // PRAGMA reverse_unordered_selects = ON -> re-prepares ONCE, then reuses.
            assert_eq!(
                exec(db_a, "PRAGMA reverse_unordered_selects = ON;"),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
            assert_eq!(sqlite3_column_count(stmt), 3);
            assert_eq!(sqlite3_column_int64(stmt, 0), 3);
            assert_eq!(sqlite3_reset(stmt), SQLITE_OK);
            let after_pragma_id = (*stmt).prepared.as_ref().unwrap().program_identity();
            assert_eq!((*stmt).prepared.as_ref().unwrap().reprepare_count(), 2);
            assert_ne!(after_pragma_id, after_alter_id);

            for _ in 0..4 {
                assert_eq!(sqlite3_step(stmt), SQLITE_ROW);
                assert_eq!(sqlite3_column_int64(stmt, 0), 3);
                assert_eq!(sqlite3_reset(stmt), SQLITE_OK);
                let prep = (*stmt).prepared.as_ref().unwrap();
                assert_eq!(prep.reprepare_count(), 2);
                assert_eq!(prep.program_identity(), after_pragma_id);
            }

            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
            assert_eq!(sqlite3_close(db_b), SQLITE_OK);
            assert_eq!(sqlite3_close(db_a), SQLITE_OK);
        }
    }
}
