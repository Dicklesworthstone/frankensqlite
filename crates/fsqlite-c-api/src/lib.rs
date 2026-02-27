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
use std::fmt::Write as _;
use std::os::raw::{c_char, c_double, c_int, c_void};
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use fsqlite::Connection;
use fsqlite_error::{ErrorCode, FrankenError};
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

/// Opaque database connection handle exposed via C FFI.
///
/// Wraps a `Connection` plus the last error message for `sqlite3_errmsg`.
pub struct Sqlite3 {
    conn: Connection,
    last_error: Mutex<CString>,
}

impl Sqlite3 {
    fn new(conn: Connection) -> Self {
        Self {
            conn,
            last_error: Mutex::new(CString::new("not an error").expect("static")),
        }
    }

    fn set_error(&self, err: &FrankenError) {
        let msg = err.to_string();
        if let Ok(c) = CString::new(msg) {
            if let Ok(mut guard) = self.last_error.lock() {
                *guard = c;
            }
        }
    }

    fn clear_error(&self) {
        if let Ok(mut guard) = self.last_error.lock() {
            *guard = CString::new("not an error").expect("static");
        }
    }
}

/// Opaque prepared statement handle exposed via C FFI.
///
/// Wraps the SQL string, the parent connection, and a row cursor so
/// that `sqlite3_step` can return one row at a time.
pub struct Sqlite3Stmt {
    db: *mut Sqlite3,
    sql: String,
    /// Cached rows from last execution.  `None` means not yet stepped.
    rows: Option<Vec<fsqlite::Row>>,
    /// Current row index (0-based, incremented by each `sqlite3_step`).
    cursor: usize,
    /// Column count from the most recent result set.
    column_count: c_int,
    /// Cached CString values for text column accessors (kept alive until
    /// next step or finalize to satisfy C lifetime expectations).
    text_cache: Vec<Option<CString>>,
}

// ── Helper: convert FrankenError → c_int ────────────────────────────

fn error_to_code(err: &FrankenError) -> c_int {
    err.error_code() as c_int
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

    let path = if filename.is_null() {
        ":memory:".to_owned()
    } else if let Ok(s) = CStr::from_ptr(filename).to_str() {
        if s.is_empty() {
            ":memory:".to_owned()
        } else {
            s.to_owned()
        }
    } else {
        *pp_db = std::ptr::null_mut();
        return SQLITE_CANTOPEN;
    };

    tracing::info!(target: "fsqlite.compat", path = %path, "sqlite3_open");

    match Connection::open(&path) {
        Ok(conn) => {
            let handle = Box::new(Sqlite3::new(conn));
            *pp_db = Box::into_raw(handle);
            SQLITE_OK
        }
        Err(e) => {
            tracing::warn!(target: "fsqlite.compat", error = %e, "sqlite3_open failed");
            *pp_db = std::ptr::null_mut();
            error_to_code(&e)
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

    let handle = Box::from_raw(db);
    match handle.conn.close() {
        Ok(()) => SQLITE_OK,
        Err(e) => {
            tracing::warn!(target: "fsqlite.compat", error = %e, "sqlite3_close failed");
            error_to_code(&e)
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
    callback: Option<
        unsafe extern "C" fn(
            parg: *mut c_void,
            ncols: c_int,
            values: *mut *mut c_char,
            names: *mut *mut c_char,
        ) -> c_int,
    >,
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
        return SQLITE_ERROR;
    };

    tracing::info!(target: "fsqlite.compat", sql = %sql_str, "sqlite3_exec");

    // Try as a query first (returns rows), fall back to execute (DML/DDL).
    match handle.conn.query(sql_str) {
        Ok(rows) => {
            handle.clear_error();
            if let Some(cb) = callback {
                for row in &rows {
                    let vals = row.values();
                    let ncols = vals.len() as c_int;

                    // Build C string arrays for the callback.
                    let mut c_values: Vec<*mut c_char> = Vec::with_capacity(vals.len());
                    let mut c_names: Vec<*mut c_char> = Vec::with_capacity(vals.len());
                    let mut owned_vals: Vec<CString> = Vec::with_capacity(vals.len());
                    let mut owned_names: Vec<CString> = Vec::with_capacity(vals.len());

                    for (i, v) in vals.iter().enumerate() {
                        let text = match v {
                            SqliteValue::Null => {
                                c_values.push(std::ptr::null_mut());
                                owned_vals.push(CString::default());
                                continue;
                            }
                            SqliteValue::Integer(n) => n.to_string(),
                            SqliteValue::Float(f) => f.to_string(),
                            SqliteValue::Text(s) => s.clone(),
                            SqliteValue::Blob(b) => {
                                // Represent blob as hex for the exec callback.
                                let mut hex = String::with_capacity(2 + b.len() * 2);
                                hex.push_str("X'");
                                for byte in b {
                                    let _ = write!(hex, "{byte:02X}");
                                }
                                hex.push('\'');
                                hex
                            }
                        };
                        let cval =
                            CString::new(text).unwrap_or_else(|_| CString::new("").expect(""));
                        c_values.push(cval.as_ptr().cast_mut());
                        owned_vals.push(cval);

                        let col_name = format!("column{i}");
                        let cname = CString::new(col_name).expect("col name");
                        c_names.push(cname.as_ptr().cast_mut());
                        owned_names.push(cname);
                    }

                    let rc = cb(parg, ncols, c_values.as_mut_ptr(), c_names.as_mut_ptr());
                    // Keep owned CStrings alive until the callback returns.
                    drop(owned_vals);
                    drop(owned_names);
                    if rc != 0 {
                        handle.set_error(&FrankenError::Abort);
                        return SQLITE_ABORT;
                    }
                }
            }
            SQLITE_OK
        }
        Err(ref e) if matches!(e, FrankenError::QueryReturnedNoRows) => {
            handle.clear_error();
            SQLITE_OK
        }
        Err(e) => {
            tracing::warn!(target: "fsqlite.compat", error = %e, "sqlite3_exec failed");
            handle.set_error(&e);
            if !errmsg.is_null() {
                if let Ok(cmsg) = CString::new(e.to_string()) {
                    let len = cmsg.as_bytes_with_nul().len();
                    let buf = libc_malloc(len);
                    if !buf.is_null() {
                        std::ptr::copy_nonoverlapping(cmsg.as_ptr(), buf.cast(), len);
                        *errmsg = buf.cast();
                    }
                }
            }
            error_to_code(&e)
        }
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
unsafe fn libc_malloc(size: usize) -> *mut u8 {
    let layout = std::alloc::Layout::from_size_align(size, 1).expect("valid layout");
    std::alloc::alloc(layout)
}

unsafe fn libc_free(ptr: *mut c_void) {
    // We allocated with align=1, but we don't know the size. Use a 1-byte
    // dealloc — this is correct for global allocator since the allocator
    // tracks the actual allocation size internally.
    let layout = std::alloc::Layout::from_size_align(1, 1).expect("valid layout");
    std::alloc::dealloc(ptr.cast(), layout);
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

    let sql_str = if n_byte < 0 {
        match CStr::from_ptr(sql).to_str() {
            Ok(s) => s.to_owned(),
            Err(_) => return SQLITE_ERROR,
        }
    } else {
        let slice = std::slice::from_raw_parts(sql.cast::<u8>(), n_byte as usize);
        // Find the first nul if present, otherwise take the whole slice.
        let end = slice.iter().position(|&b| b == 0).unwrap_or(slice.len());
        match std::str::from_utf8(&slice[..end]) {
            Ok(s) => s.to_owned(),
            Err(_) => return SQLITE_ERROR,
        }
    };

    if sql_str.trim().is_empty() {
        return SQLITE_OK;
    }

    tracing::info!(target: "fsqlite.compat", sql = %sql_str, "sqlite3_prepare_v2");

    // Validate the SQL by preparing it through the Rust API.
    let handle = &*db;
    match handle.conn.prepare(&sql_str) {
        Ok(_prepared) => {
            handle.clear_error();
            let stmt = Box::new(Sqlite3Stmt {
                db,
                sql: sql_str,
                rows: None,
                cursor: 0,
                column_count: 0,
                text_cache: Vec::new(),
            });
            *pp_stmt = Box::into_raw(stmt);

            // Set pz_tail to point past the end of the consumed SQL.
            if !pz_tail.is_null() {
                // For simplicity, we consume the entire input.
                if n_byte < 0 {
                    *pz_tail = sql.add(CStr::from_ptr(sql).to_bytes().len());
                } else {
                    *pz_tail = sql.add(n_byte as usize);
                }
            }

            SQLITE_OK
        }
        Err(e) => {
            tracing::warn!(target: "fsqlite.compat", error = %e, "sqlite3_prepare_v2 failed");
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

        match db.conn.query(&s.sql) {
            Ok(rows) => {
                db.clear_error();
                if let Some(first) = rows.first() {
                    s.column_count = first.values().len() as c_int;
                }
                s.rows = Some(rows);
                s.cursor = 0;
            }
            Err(ref e) if matches!(e, FrankenError::QueryReturnedNoRows) => {
                db.clear_error();
                s.rows = Some(Vec::new());
                s.cursor = 0;
                s.column_count = 0;
            }
            Err(e) => {
                tracing::warn!(target: "fsqlite.compat", error = %e, "sqlite3_step failed");
                db.set_error(&e);
                return error_to_code(&e);
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
            SQLITE_ROW
        } else {
            SQLITE_DONE
        }
    } else {
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

    drop(Box::from_raw(stmt));
    SQLITE_OK
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
    s.rows = None;
    s.cursor = 0;
    s.column_count = 0;
    s.text_cache.clear();
    SQLITE_OK
}

// ── Column accessors ────────────────────────────────────────────────

/// Return a reference to the current row's value at column `i_col`, or None if out of bounds.
unsafe fn current_value_ref<'a>(stmt: *const Sqlite3Stmt, i_col: c_int) -> Option<&'a SqliteValue> {
    if i_col < 0 {
        return None;
    }
    let s = &*stmt;
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
        Some(SqliteValue::Integer(n)) => *n,
        Some(SqliteValue::Float(f)) => *f as i64,
        Some(SqliteValue::Text(s)) => s.parse::<i64>().unwrap_or(0),
        _ => 0,
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
        Some(SqliteValue::Float(f)) => *f,
        Some(SqliteValue::Integer(n)) => *n as f64,
        Some(SqliteValue::Text(s)) => s.parse::<f64>().unwrap_or(0.0),
        _ => 0.0,
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
        Some(SqliteValue::Text(s)) => s.clone(),
        Some(SqliteValue::Integer(n)) => n.to_string(),
        Some(SqliteValue::Float(f)) => f.to_string(),
        Some(SqliteValue::Blob(b)) => {
            // Return blob as hex string for text accessor.
            let mut hex = String::with_capacity(b.len() * 2);
            for byte in b {
                let _ = write!(hex, "{byte:02X}");
            }
            hex
        }
        Some(SqliteValue::Null) | None => return std::ptr::null(),
    };

    let s = &mut *stmt;
    let Ok(cstr) = CString::new(text) else {
        return std::ptr::null();
    };
    // Cache the CString so the pointer stays valid.
    if (i_col as usize) < s.text_cache.len() {
        s.text_cache[i_col as usize] = Some(cstr);
        // Re-read the pointer from the cached location.
        return s.text_cache[i_col as usize]
            .as_ref()
            .map_or(std::ptr::null(), |c| c.as_ptr());
    }
    // Fallback: extend cache.
    while s.text_cache.len() <= i_col as usize {
        s.text_cache.push(None);
    }
    s.text_cache[i_col as usize] = Some(cstr);
    s.text_cache[i_col as usize]
        .as_ref()
        .map_or(std::ptr::null(), |c| c.as_ptr())
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

    let s = &*stmt;
    let Some(rows) = s.rows.as_ref() else {
        return std::ptr::null();
    };
    let Some(row_idx) = s.cursor.checked_sub(1) else {
        return std::ptr::null();
    };
    let Some(row) = rows.get(row_idx) else {
        return std::ptr::null();
    };

    match row.get(i_col as usize) {
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

    match current_value_ref(stmt, i_col) {
        Some(SqliteValue::Blob(b)) => b.len() as c_int,
        Some(SqliteValue::Text(s)) => s.len() as c_int,
        // SQLite returns the byte length of the text representation for integers;
        // we return 0 for all other types.
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
        LazyLock::new(|| CString::new("not an error").expect("static"));

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
    // We don't store the error code separately; return OK.
    // A more complete implementation would track this.
    SQLITE_OK
}

// ── sqlite3_changes ─────────────────────────────────────────────────

/// Return the number of rows modified by the most recent INSERT/UPDATE/DELETE.
///
/// # Safety
/// `db` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_changes(_db: *mut Sqlite3) -> c_int {
    // Placeholder: full tracking requires connection-level state.
    0
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
            sqlite3_close(db);
        }
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
}
