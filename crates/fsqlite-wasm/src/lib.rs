#![allow(clippy::future_not_send)]
// wasm-bindgen drives these futures on the browser's single-threaded local
// executor. Requiring `Send` would reject the core connection's intentional
// `Rc`/`RefCell` state even though no future crosses a thread boundary.

//! WebAssembly bindings for FrankenSQLite.
//!
//! This crate exposes a small browser-facing surface backed by
//! `fsqlite-core`'s wasm-compatible in-memory engine, while continuing to
//! re-export the parser/planner crates for lower-level integration.
//!
//! The browser build retains FrankenSQLite's portable in-memory VFS, pager,
//! WAL, and MVCC stack. Only native OS backends and facilities such as
//! `io_uring` are excluded from `wasm32-unknown-unknown`.
//!
//! JavaScript conversion semantics currently follow the WASM 2.6 bead:
//! - `null` / `undefined` <-> `SqliteValue::Null`
//! - `INTEGER` <-> `number` when within `Number.MAX_SAFE_INTEGER`, otherwise `BigInt`
//! - `REAL` <-> `number`
//! - `TEXT` <-> `string`
//! - `BLOB` <-> `Uint8Array`
//! - `NaN` coerces to `NULL` with a browser warning
//! - `Infinity` and `-Infinity` are rejected
//! - `Date` inputs are stored as ISO 8601 `TEXT` when the `date-params`
//!   feature is enabled

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Once;

#[cfg(feature = "memory-options")]
use fsqlite_core::connection::ConnectionEnv;
#[cfg(feature = "diagnostics")]
use fsqlite_core::connection::ConnectionMemoryStats;
use fsqlite_core::connection::{Connection as CoreConnection, Row as CoreRow};
use fsqlite_error::FrankenError;
#[cfg(feature = "date-params")]
use fsqlite_types::SmallText;
use fsqlite_types::SqliteValue;
#[cfg(feature = "date-params")]
use js_sys::Date;
#[cfg(all(feature = "diagnostics", feature = "memory-options"))]
use js_sys::Function;
use js_sys::{Array, BigInt, Number, Object, Promise, Reflect, Uint8Array};
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

pub use fsqlite_ast as ast;
pub use fsqlite_error as error;
pub use fsqlite_func as func;
pub use fsqlite_parser as parser;
pub use fsqlite_planner as planner;
pub use fsqlite_types as types;

static WASM_RUNTIME_INIT: Once = Once::new();
#[cfg(any(feature = "diagnostics", feature = "memory-options"))]
const WASM_LINEAR_MEMORY_PAGE_BYTES: usize = 64 * 1024;
#[cfg(not(all(feature = "diagnostics", feature = "memory-options")))]
const WASM_OUT_OF_MEMORY_MESSAGE: &str = "FrankenSQLite WASM ran out of memory";
#[cfg(all(feature = "diagnostics", feature = "memory-options"))]
const WASM_OUT_OF_MEMORY_MESSAGE: &str = "FrankenSQLite WASM ran out of memory; adjust memory.maxPages or memory.maxBytes, \
     memory.growthChunkPages or memory.growthChunkBytes, or pageBufferMax, and remember \
     the browser WebAssembly linear-memory ceiling is 4 GiB";

/// Parse a SQL string into a list of AST statements.
///
/// Returns the parsed statements and any parse errors encountered.
pub fn parse_sql(input: &str) -> (Vec<ast::Statement>, Vec<parser::ParseError>) {
    let tokens = parser::Lexer::tokenize(input);
    let mut p = parser::Parser::new(tokens);
    p.parse_all()
}

fn install_wasm_runtime() {
    WASM_RUNTIME_INIT.call_once(|| {
        #[cfg(feature = "panic-hook")]
        console_error_panic_hook::set_once();
        #[cfg(all(target_arch = "wasm32", feature = "tracing"))]
        tracing_wasm::set_as_global_default();
    });
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen(start))]
pub fn init() {
    install_wasm_runtime();
}

#[cfg(feature = "diagnostics")]
#[wasm_bindgen(js_name = parseSql)]
pub fn parse_sql_js(input: &str) -> Result<JsValue, JsValue> {
    install_wasm_runtime();
    let (statements, errors) = parse_sql(input);

    let summary = Object::new();
    set_property(
        &summary,
        "statementCount",
        &JsValue::from_f64(statements.len() as f64),
    )
    .map_err(franken_error_to_js)?;
    set_property(
        &summary,
        "errorCount",
        &JsValue::from_f64(errors.len() as f64),
    )
    .map_err(franken_error_to_js)?;

    let error_messages = Array::new();
    for error in errors {
        error_messages.push(&JsValue::from_str(&error.to_string()));
    }
    set_property(&summary, "errors", &error_messages.into()).map_err(franken_error_to_js)?;

    Ok(summary.into())
}

/// Minimal JavaScript-facing database wrapper.
///
/// Query results expose `rows` as JavaScript objects keyed by column label and
/// include best-effort `columnTypes` metadata. Labels use core inference when
/// available and fall back to `_cN` for unnamed expressions. Enable the
/// `row-arrays` feature when consumers also need positional `rowArrays`.
#[wasm_bindgen(js_name = FrankenDB)]
pub struct FrankenDb {
    state: Rc<FrankenDbState>,
}

struct FrankenDbState {
    #[cfg(feature = "diagnostics")]
    path: String,
    inner: RefCell<Option<Rc<CoreConnection>>>,
    operation_active: Cell<bool>,
    #[cfg(all(feature = "diagnostics", feature = "memory-options"))]
    memory_warning_threshold_bytes: Cell<Option<usize>>,
    #[cfg(all(feature = "diagnostics", feature = "memory-options"))]
    memory_warning_threshold_percent: Cell<Option<usize>>,
    #[cfg(all(feature = "diagnostics", feature = "memory-options"))]
    memory_warning_above_threshold: Cell<bool>,
    #[cfg(all(feature = "diagnostics", feature = "memory-options"))]
    memory_warning_callback: RefCell<Option<Function>>,
}

struct ConnectionOperationGuard {
    state: Rc<FrankenDbState>,
    conn: Rc<CoreConnection>,
}

impl ConnectionOperationGuard {
    async fn finish<T>(
        self,
        operation: impl std::ops::AsyncFnOnce(&CoreConnection) -> Result<T, FrankenError>,
    ) -> Result<T, JsValue> {
        match operation(self.conn.as_ref()).await {
            Ok(value) => {
                #[cfg(all(feature = "diagnostics", feature = "memory-options"))]
                // Keep the admission guard live through the callback so
                // callback-triggered re-entry follows the same `Busy` contract.
                self.state.observe_memory_warning();
                Ok(value)
            }
            Err(error) => Err(self.state.connection_error_to_js(self.conn.as_ref(), error)),
        }
    }
}

impl Drop for ConnectionOperationGuard {
    fn drop(&mut self) {
        self.state.operation_active.set(false);
    }
}

#[cfg(all(feature = "diagnostics", feature = "prepared-statements"))]
struct PreparedMetadata {
    column_count: usize,
    column_names: Vec<String>,
}

#[derive(Default, Clone)]
#[cfg(feature = "memory-options")]
struct WasmDatabaseOptions {
    page_buffer_max: Option<usize>,
    initial_reserve_bytes: Option<usize>,
    growth_chunk_bytes: Option<usize>,
    max_bytes: Option<usize>,
    #[cfg(feature = "diagnostics")]
    warning_threshold_bytes: Option<usize>,
    #[cfg(feature = "diagnostics")]
    warning_threshold_percent: Option<usize>,
    #[cfg(feature = "diagnostics")]
    warning_callback: Option<Function>,
}

#[cfg(feature = "memory-options")]
impl WasmDatabaseOptions {
    #[cfg(feature = "diagnostics")]
    fn effective_warning_threshold_bytes(&self) -> Result<Option<usize>, FrankenError> {
        match (self.warning_threshold_bytes, self.warning_threshold_percent) {
            (Some(_), Some(_)) => Err(FrankenError::TypeMismatch {
                expected: "use either memory.warningThresholdBytes or memory.warnAtPercent"
                    .to_owned(),
                actual: "both threshold properties were provided".to_owned(),
            }),
            (Some(bytes), None) => Ok(Some(bytes)),
            (None, Some(percent)) => {
                let max_bytes = self.max_bytes.ok_or_else(|| FrankenError::TypeMismatch {
                    expected: "memory.maxBytes or memory.maxPages when using memory.warnAtPercent"
                        .to_owned(),
                    actual: "warnAtPercent without a tracked memory cap".to_owned(),
                })?;
                threshold_bytes_from_percent(max_bytes, percent).map(Some)
            }
            (None, None) => Ok(None),
        }
    }

    #[allow(clippy::useless_let_if_seq)] // three independent optional fields
    fn memory_vfs_config(&self) -> Result<Option<fsqlite_vfs::MemoryVfsConfig>, FrankenError> {
        let mut config = fsqlite_vfs::MemoryVfsConfig::default();
        let mut configured = false;

        if let Some(initial_reserve_bytes) = self.initial_reserve_bytes {
            config.initial_reserve_bytes = initial_reserve_bytes;
            configured = true;
        }
        if let Some(growth_chunk_bytes) = self.growth_chunk_bytes {
            if growth_chunk_bytes == 0 {
                return Err(FrankenError::OutOfRange {
                    what: "memory.growthChunkBytes".to_owned(),
                    value: growth_chunk_bytes.to_string(),
                });
            }
            config.growth_chunk_bytes = growth_chunk_bytes;
            configured = true;
        }
        if let Some(max_bytes) = self.max_bytes {
            if max_bytes < config.initial_reserve_bytes {
                return Err(FrankenError::OutOfRange {
                    what: "memory.maxBytes".to_owned(),
                    value: max_bytes.to_string(),
                });
            }
            config.max_bytes = Some(max_bytes);
            configured = true;
        }

        Ok(configured.then_some(config))
    }
}

#[cfg(all(feature = "diagnostics", feature = "memory-options"))]
fn memory_warning_transition(
    estimated_used_bytes: usize,
    threshold: usize,
    was_above_threshold: bool,
) -> (bool, bool) {
    let above_threshold = estimated_used_bytes >= threshold;
    let crossed_threshold = above_threshold && !was_above_threshold;
    (above_threshold, crossed_threshold)
}

#[cfg(feature = "prepared-statements")]
#[wasm_bindgen(js_name = FrankenPreparedStatement)]
pub struct FrankenPreparedStatement {
    state: Rc<FrankenDbState>,
    sql: String,
    #[cfg(feature = "diagnostics")]
    column_count: usize,
    #[cfg(feature = "diagnostics")]
    column_names: Vec<String>,
}

// `wasm_bindgen`'s Promise adapter already owns each exported async future on
// the browser's heap. Boxing nested core futures here would add allocations and
// pointer indirection without reducing the JavaScript boundary's stack use.
#[allow(clippy::large_futures)]
#[wasm_bindgen(js_class = FrankenDB)]
impl FrankenDb {
    /// Open a database.
    ///
    /// NOTE: opening is `async` now that the storage stack is async, and
    /// `wasm_bindgen` cannot export an `async` constructor. This is therefore a
    /// static factory returning a `Promise` rather than a JS `new` constructor:
    /// `await FrankenDB.create(name)`.
    #[wasm_bindgen(js_name = create)]
    pub async fn new(name: Option<String>) -> Result<Self, JsValue> {
        install_wasm_runtime();
        let path = name.unwrap_or_else(|| ":memory:".to_owned());
        let conn = open_core_connection(&path)
            .await
            .map_err(franken_error_to_js)?;
        #[cfg(feature = "memory-options")]
        {
            Self::from_parts(path, conn, WasmDatabaseOptions::default())
        }
        #[cfg(not(feature = "memory-options"))]
        {
            Self::from_parts(path, conn)
        }
    }

    #[cfg(feature = "api-extras")]
    #[wasm_bindgen(js_name = open)]
    pub async fn open(name: Option<String>) -> Result<Self, JsValue> {
        Self::new(name).await
    }

    #[cfg(feature = "memory-options")]
    #[wasm_bindgen(js_name = openWithOptions)]
    pub async fn open_with_options(
        name: Option<String>,
        options: Option<JsValue>,
    ) -> Result<Self, JsValue> {
        install_wasm_runtime();
        let path = name.unwrap_or_else(|| ":memory:".to_owned());
        let options = parse_database_options(options)?;
        let conn = open_core_connection_with_options(&path, &options)
            .await
            .map_err(franken_error_to_js)?;
        Self::from_parts(path, conn, options)
    }

    #[cfg(feature = "backup")]
    #[wasm_bindgen(js_name = import)]
    pub async fn import(data: Uint8Array) -> Result<Self, JsValue> {
        install_wasm_runtime();
        let conn = import_core_connection(&data.to_vec())
            .await
            .map_err(franken_error_to_js)?;
        #[cfg(feature = "memory-options")]
        {
            Self::from_parts(":memory:".to_owned(), conn, WasmDatabaseOptions::default())
        }
        #[cfg(not(feature = "memory-options"))]
        {
            Self::from_parts(":memory:".to_owned(), conn)
        }
    }

    #[cfg(all(feature = "backup", feature = "memory-options"))]
    #[wasm_bindgen(js_name = importWithOptions)]
    pub async fn import_with_options(
        data: Uint8Array,
        options: Option<JsValue>,
    ) -> Result<Self, JsValue> {
        install_wasm_runtime();
        let options = parse_database_options(options)?;
        let conn = import_core_connection_with_options(&data.to_vec(), &options)
            .await
            .map_err(franken_error_to_js)?;
        Self::from_parts(":memory:".to_owned(), conn, options)
    }

    #[cfg(feature = "diagnostics")]
    #[wasm_bindgen(getter)]
    pub fn path(&self) -> String {
        self.state.path.clone()
    }

    /// Close this JavaScript handle.
    ///
    /// An operation whose `Promise` was already admitted retains the core
    /// connection and is allowed to finish. Operations admitted after `close`
    /// fail with a closed-handle error. Calls that overlap another operation
    /// on the same handle fail fast instead of concurrently driving the
    /// single-connection state machine.
    pub fn close(&self) {
        let _ = self.state.inner.borrow_mut().take();
    }

    #[wasm_bindgen(js_name = execute)]
    pub fn execute_js(&self, sql: &str) -> Promise {
        let guard = match self.admit_operation() {
            Ok(guard) => guard,
            Err(error) => return Promise::reject(&error),
        };
        let sql = sql.to_owned();
        future_to_promise(async move {
            FrankenDb::execute_admitted(guard, sql)
                .await
                .map(|changes| JsValue::from_f64(changes as f64))
        })
    }

    #[cfg(feature = "batch-execution")]
    #[wasm_bindgen(js_name = executeBatch)]
    pub fn execute_batch_js(&self, sql: &str) -> Promise {
        let guard = match self.admit_operation() {
            Ok(guard) => guard,
            Err(error) => return Promise::reject(&error),
        };
        let sql = sql.to_owned();
        future_to_promise(async move {
            FrankenDb::execute_batch_admitted(guard, sql)
                .await
                .map(|()| JsValue::UNDEFINED)
        })
    }

    #[wasm_bindgen(js_name = executeWithParams)]
    pub fn execute_with_params_js(&self, sql: &str, params: JsValue) -> Promise {
        let guard = match self.admit_operation() {
            Ok(guard) => guard,
            Err(error) => return Promise::reject(&error),
        };
        let sql = sql.to_owned();
        future_to_promise(async move {
            // Admission deliberately precedes JavaScript parameter conversion:
            // overlapping calls deterministically observe `SQLITE_BUSY`
            // without invoking getters or coercions. Conversion also runs
            // after this exported method returns, so a getter may safely free
            // its wasm-bindgen wrapper.
            let params = parse_js_params(params)?;
            FrankenDb::execute_with_params_admitted(guard, sql, params)
                .await
                .map(|changes| JsValue::from_f64(changes as f64))
        })
    }

    #[wasm_bindgen(js_name = query)]
    pub fn query_js(&self, sql: &str) -> Promise {
        let guard = match self.admit_operation() {
            Ok(guard) => guard,
            Err(error) => return Promise::reject(&error),
        };
        let sql = sql.to_owned();
        future_to_promise(FrankenDb::query_admitted(guard, sql))
    }

    #[wasm_bindgen(js_name = queryWithParams)]
    pub fn query_with_params_js(&self, sql: &str, params: JsValue) -> Promise {
        let guard = match self.admit_operation() {
            Ok(guard) => guard,
            Err(error) => return Promise::reject(&error),
        };
        let sql = sql.to_owned();
        future_to_promise(async move {
            let params = parse_js_params(params)?;
            FrankenDb::query_with_params_admitted(guard, sql, params).await
        })
    }

    #[cfg(feature = "api-extras")]
    #[wasm_bindgen(js_name = pragma)]
    pub fn pragma_js(&self, pragma: &str) -> Promise {
        let guard = match self.admit_operation() {
            Ok(guard) => guard,
            Err(error) => return Promise::reject(&error),
        };
        let sql = format!("PRAGMA {pragma}");
        future_to_promise(FrankenDb::query_admitted(guard, sql))
    }

    #[cfg(feature = "prepared-statements")]
    #[wasm_bindgen(js_name = prepare)]
    pub fn prepare_js(&self, sql: &str) -> Promise {
        let guard = match self.admit_operation() {
            Ok(guard) => guard,
            Err(error) => return Promise::reject(&error),
        };
        let sql = sql.to_owned();
        future_to_promise(async move {
            FrankenDb::prepare_admitted(guard, sql)
                .await
                .map(JsValue::from)
        })
    }

    #[cfg(feature = "diagnostics")]
    #[wasm_bindgen(js_name = explain)]
    pub fn explain_js(&self, sql: &str) -> Promise {
        let guard = match self.admit_operation() {
            Ok(guard) => guard,
            Err(error) => return Promise::reject(&error),
        };
        let sql = sql.to_owned();
        future_to_promise(async move {
            FrankenDb::explain_admitted(guard, sql)
                .await
                .map(|explanation| JsValue::from_str(&explanation))
        })
    }

    #[cfg(feature = "backup")]
    #[wasm_bindgen(js_name = export)]
    pub fn export_js(&self) -> Promise {
        let guard = match self.admit_operation() {
            Ok(guard) => guard,
            Err(error) => return Promise::reject(&error),
        };
        future_to_promise(async move {
            FrankenDb::export_admitted(guard)
                .await
                .map(|bytes| Uint8Array::from(bytes.as_slice()).into())
        })
    }

    #[cfg(feature = "diagnostics")]
    #[wasm_bindgen(js_name = memoryStats)]
    pub fn memory_stats(&self) -> Result<JsValue, JsValue> {
        self.state.memory_stats_js()
    }
}

impl FrankenDbState {
    fn connection_snapshot(&self) -> Result<Rc<CoreConnection>, FrankenError> {
        self.inner
            .borrow()
            .clone()
            .ok_or_else(|| FrankenError::internal("database handle is closed"))
    }

    fn admit_connection_operation(
        self: &Rc<Self>,
    ) -> Result<ConnectionOperationGuard, FrankenError> {
        let conn = self.connection_snapshot()?;
        if self.operation_active.replace(true) {
            return Err(FrankenError::Busy);
        }
        Ok(ConnectionOperationGuard {
            state: Rc::clone(self),
            conn,
        })
    }

    #[cfg(test)]
    async fn with_connection<T>(
        self: &Rc<Self>,
        f: impl std::ops::AsyncFnOnce(&CoreConnection) -> Result<T, FrankenError>,
    ) -> Result<T, JsValue> {
        install_wasm_runtime();
        let operation_guard = self
            .admit_connection_operation()
            .map_err(franken_error_to_js)?;
        operation_guard.finish(f).await
    }

    #[cfg(feature = "diagnostics")]
    fn memory_stats_js(&self) -> Result<JsValue, JsValue> {
        install_wasm_runtime();
        if self.operation_active.get() {
            return Err(franken_error_to_js(FrankenError::Busy));
        }
        let conn = self.connection_snapshot().map_err(franken_error_to_js)?;
        let stats = conn
            .memory_stats()
            .map_err(|error| self.connection_error_to_js(conn.as_ref(), error))?;
        connection_memory_stats_to_js(
            conn.as_ref(),
            stats,
            #[cfg(all(feature = "diagnostics", feature = "memory-options"))]
            self.memory_warning_threshold_bytes.get(),
            #[cfg(not(all(feature = "diagnostics", feature = "memory-options")))]
            None,
            #[cfg(all(feature = "diagnostics", feature = "memory-options"))]
            self.memory_warning_threshold_percent.get(),
            #[cfg(not(all(feature = "diagnostics", feature = "memory-options")))]
            None,
        )
    }

    #[cfg(all(feature = "diagnostics", feature = "memory-options"))]
    fn observe_memory_warning(&self) {
        let Some(threshold) = self.memory_warning_threshold_bytes.get() else {
            return;
        };
        let Some(callback) = self.memory_warning_callback.borrow().clone() else {
            return;
        };
        let Some(conn) = self.inner.borrow().as_ref().cloned() else {
            return;
        };
        let Ok(stats) = conn.memory_stats() else {
            return;
        };
        let (above_threshold, crossed_threshold) = memory_warning_transition(
            stats.estimated_used_bytes(),
            threshold,
            self.memory_warning_above_threshold.get(),
        );
        self.memory_warning_above_threshold.set(above_threshold);
        if crossed_threshold
            && let Ok(payload) = connection_memory_stats_to_js(
                conn.as_ref(),
                stats,
                self.memory_warning_threshold_bytes.get(),
                self.memory_warning_threshold_percent.get(),
            )
        {
            let _ = callback.call1(&JsValue::NULL, &payload);
        }
    }

    #[cfg_attr(not(feature = "diagnostics"), allow(clippy::unused_self))]
    fn connection_error_to_js(&self, conn: &CoreConnection, error: FrankenError) -> JsValue {
        #[cfg(not(feature = "diagnostics"))]
        let _ = conn;
        let is_oom = matches!(error, FrankenError::OutOfMemory);
        let object = Object::from(franken_error_to_js(error));
        if is_oom {
            let _ = set_property(
                &object,
                "message",
                &JsValue::from_str(WASM_OUT_OF_MEMORY_MESSAGE),
            );
            let _ = set_property(&object, "oom", &JsValue::from_bool(true));
            #[cfg(feature = "diagnostics")]
            if let Ok(stats) = conn.memory_stats()
                && let Ok(stats_js) = connection_memory_stats_to_js(
                    conn,
                    stats,
                    #[cfg(all(feature = "diagnostics", feature = "memory-options"))]
                    self.memory_warning_threshold_bytes.get(),
                    #[cfg(not(all(feature = "diagnostics", feature = "memory-options")))]
                    None,
                    #[cfg(all(feature = "diagnostics", feature = "memory-options"))]
                    self.memory_warning_threshold_percent.get(),
                    #[cfg(not(all(feature = "diagnostics", feature = "memory-options")))]
                    None,
                )
            {
                let _ = set_property(&object, "memoryStats", &stats_js);
            }
        }
        object.into()
    }
}

#[cfg(feature = "memory-options")]
async fn open_core_connection_with_options(
    path: &str,
    options: &WasmDatabaseOptions,
) -> Result<CoreConnection, FrankenError> {
    let env = connection_env_from_options(options)?;
    CoreConnection::open_with_env(path, env).await
}

#[cfg(feature = "backup")]
async fn import_core_connection(bytes: &[u8]) -> Result<CoreConnection, FrankenError> {
    CoreConnection::import_bytes(bytes).await
}

#[cfg(all(feature = "backup", feature = "memory-options"))]
async fn import_core_connection_with_options(
    bytes: &[u8],
    options: &WasmDatabaseOptions,
) -> Result<CoreConnection, FrankenError> {
    let env = connection_env_from_options(options)?;
    CoreConnection::import_bytes_with_env(bytes, env).await
}

impl FrankenDb {
    #[cfg_attr(not(feature = "diagnostics"), allow(clippy::unnecessary_wraps))]
    fn from_parts(
        path: String,
        conn: CoreConnection,
        #[cfg(feature = "memory-options")] options: WasmDatabaseOptions,
    ) -> Result<Self, JsValue> {
        #[cfg(not(feature = "diagnostics"))]
        let _ = &path;
        #[cfg(all(feature = "memory-options", not(feature = "diagnostics")))]
        let _ = &options;
        let db = Self {
            state: Rc::new(FrankenDbState {
                #[cfg(feature = "diagnostics")]
                path,
                inner: RefCell::new(Some(Rc::new(conn))),
                operation_active: Cell::new(false),
                #[cfg(all(feature = "diagnostics", feature = "memory-options"))]
                memory_warning_threshold_bytes: Cell::new(
                    options
                        .effective_warning_threshold_bytes()
                        .map_err(franken_error_to_js)?,
                ),
                #[cfg(all(feature = "diagnostics", feature = "memory-options"))]
                memory_warning_threshold_percent: Cell::new(options.warning_threshold_percent),
                #[cfg(all(feature = "diagnostics", feature = "memory-options"))]
                memory_warning_above_threshold: Cell::new(false),
                #[cfg(all(feature = "diagnostics", feature = "memory-options"))]
                memory_warning_callback: RefCell::new(options.warning_callback),
            }),
        };
        #[cfg(all(feature = "diagnostics", feature = "memory-options"))]
        db.state.observe_memory_warning();
        Ok(db)
    }

    fn admit_operation(&self) -> Result<ConnectionOperationGuard, JsValue> {
        install_wasm_runtime();
        self.state
            .admit_connection_operation()
            .map_err(franken_error_to_js)
    }

    async fn execute_admitted(
        guard: ConnectionOperationGuard,
        sql: String,
    ) -> Result<usize, JsValue> {
        guard.finish(async |conn| conn.execute(&sql).await).await
    }

    #[cfg(feature = "batch-execution")]
    async fn execute_batch_admitted(
        guard: ConnectionOperationGuard,
        sql: String,
    ) -> Result<(), JsValue> {
        guard
            .finish(async |conn| conn.execute_batch(&sql).await)
            .await
    }

    async fn execute_with_params_admitted(
        guard: ConnectionOperationGuard,
        sql: String,
        params: Vec<SqliteValue>,
    ) -> Result<usize, JsValue> {
        guard
            .finish(async |conn| conn.execute_with_params(&sql, &params).await)
            .await
    }

    async fn query_admitted(
        guard: ConnectionOperationGuard,
        sql: String,
    ) -> Result<JsValue, JsValue> {
        guard
            .finish(async |conn| {
                let stmt = conn.prepare(&sql).await?;
                let rows = stmt.query().await?;
                query_result_to_js(rows, stmt.column_names(), stmt.column_count())
            })
            .await
    }

    async fn query_with_params_admitted(
        guard: ConnectionOperationGuard,
        sql: String,
        params: Vec<SqliteValue>,
    ) -> Result<JsValue, JsValue> {
        guard
            .finish(async |conn| {
                let stmt = conn.prepare(&sql).await?;
                let rows = stmt.query_with_params(&params).await?;
                query_result_to_js(rows, stmt.column_names(), stmt.column_count())
            })
            .await
    }

    #[cfg(feature = "prepared-statements")]
    async fn prepare_admitted(
        guard: ConnectionOperationGuard,
        sql: String,
    ) -> Result<FrankenPreparedStatement, JsValue> {
        let state = Rc::clone(&guard.state);
        #[cfg(feature = "diagnostics")]
        let metadata = guard
            .finish(async |conn| {
                let stmt = conn.prepare(&sql).await?;
                Ok(PreparedMetadata {
                    column_count: stmt.column_count(),
                    column_names: stmt.column_names().to_vec(),
                })
            })
            .await?;
        #[cfg(not(feature = "diagnostics"))]
        guard
            .finish(async |conn| {
                let _stmt = conn.prepare(&sql).await?;
                Ok(())
            })
            .await?;
        Ok(FrankenPreparedStatement {
            state,
            sql,
            #[cfg(feature = "diagnostics")]
            column_count: metadata.column_count,
            #[cfg(feature = "diagnostics")]
            column_names: metadata.column_names,
        })
    }

    #[cfg(feature = "diagnostics")]
    async fn explain_admitted(
        guard: ConnectionOperationGuard,
        sql: String,
    ) -> Result<String, JsValue> {
        guard
            .finish(async |conn| {
                let stmt = conn.prepare(&sql).await?;
                Ok(stmt.explain())
            })
            .await
    }

    #[cfg(feature = "backup")]
    async fn export_admitted(guard: ConnectionOperationGuard) -> Result<Vec<u8>, JsValue> {
        guard.finish(async |conn| conn.export_bytes().await).await
    }

    #[cfg(test)]
    async fn with_connection<T>(
        &self,
        f: impl std::ops::AsyncFnOnce(&CoreConnection) -> Result<T, FrankenError>,
    ) -> Result<T, JsValue> {
        self.state.with_connection(f).await
    }
}

#[cfg(test)]
#[allow(dead_code)]
impl FrankenDb {
    async fn execute(&self, sql: &str) -> Result<usize, JsValue> {
        Self::execute_admitted(self.admit_operation()?, sql.to_owned()).await
    }

    #[cfg(feature = "batch-execution")]
    async fn execute_batch(&self, sql: &str) -> Result<(), JsValue> {
        Self::execute_batch_admitted(self.admit_operation()?, sql.to_owned()).await
    }

    async fn execute_with_params(&self, sql: &str, params: JsValue) -> Result<usize, JsValue> {
        let guard = self.admit_operation()?;
        let params = parse_js_params(params)?;
        Self::execute_with_params_admitted(guard, sql.to_owned(), params).await
    }

    async fn query(&self, sql: &str) -> Result<JsValue, JsValue> {
        Self::query_admitted(self.admit_operation()?, sql.to_owned()).await
    }

    async fn query_with_params(&self, sql: &str, params: JsValue) -> Result<JsValue, JsValue> {
        let guard = self.admit_operation()?;
        let params = parse_js_params(params)?;
        Self::query_with_params_admitted(guard, sql.to_owned(), params).await
    }

    #[cfg(feature = "api-extras")]
    async fn pragma(&self, pragma: &str) -> Result<JsValue, JsValue> {
        Self::query_admitted(self.admit_operation()?, format!("PRAGMA {pragma}")).await
    }

    #[cfg(feature = "prepared-statements")]
    async fn prepare(&self, sql: &str) -> Result<FrankenPreparedStatement, JsValue> {
        Self::prepare_admitted(self.admit_operation()?, sql.to_owned()).await
    }

    #[cfg(feature = "diagnostics")]
    async fn explain(&self, sql: &str) -> Result<String, JsValue> {
        Self::explain_admitted(self.admit_operation()?, sql.to_owned()).await
    }

    #[cfg(feature = "backup")]
    async fn export(&self) -> Result<Uint8Array, JsValue> {
        let bytes = Self::export_admitted(self.admit_operation()?).await?;
        Ok(Uint8Array::from(bytes.as_slice()))
    }
}

async fn open_core_connection(path: &str) -> Result<CoreConnection, FrankenError> {
    CoreConnection::open(path).await
}

#[cfg(feature = "memory-options")]
fn connection_env_from_options(
    options: &WasmDatabaseOptions,
) -> Result<ConnectionEnv, FrankenError> {
    let mut env = ConnectionEnv::default();
    if options.page_buffer_max.is_some() {
        env.set_page_buffer_max(options.page_buffer_max);
    }
    if let Some(memory_vfs_config) = options.memory_vfs_config()? {
        env.set_memory_vfs_config(Some(memory_vfs_config));
    }
    Ok(env)
}

#[cfg(feature = "prepared-statements")]
// As above, the exported Promise owns this future; nested boxing would only add
// per-operation allocation overhead.
#[allow(clippy::large_futures)]
#[wasm_bindgen(js_class = FrankenPreparedStatement)]
impl FrankenPreparedStatement {
    #[cfg(feature = "diagnostics")]
    #[wasm_bindgen(getter)]
    pub fn sql(&self) -> String {
        self.sql.clone()
    }

    #[cfg(feature = "diagnostics")]
    #[wasm_bindgen(getter, js_name = columnCount)]
    pub fn column_count(&self) -> usize {
        self.column_count
    }

    #[cfg(feature = "diagnostics")]
    #[wasm_bindgen(js_name = columnNames)]
    pub fn column_names_js(&self) -> JsValue {
        let names = Array::new();
        for name in &self.column_names {
            names.push(&JsValue::from_str(name));
        }
        names.into()
    }

    #[wasm_bindgen(js_name = execute)]
    pub fn execute_js(&self) -> Promise {
        let guard = match self.admit_operation() {
            Ok(guard) => guard,
            Err(error) => return Promise::reject(&error),
        };
        let sql = self.sql.clone();
        future_to_promise(async move {
            FrankenPreparedStatement::execute_admitted(guard, sql)
                .await
                .map(|changes| JsValue::from_f64(changes as f64))
        })
    }

    #[wasm_bindgen(js_name = executeWithParams)]
    pub fn execute_with_params_js(&self, params: JsValue) -> Promise {
        let guard = match self.admit_operation() {
            Ok(guard) => guard,
            Err(error) => return Promise::reject(&error),
        };
        // Detach every future input from the wasm-bindgen wrapper before
        // parameter getters can re-enter JavaScript and free that wrapper.
        let sql = self.sql.clone();
        future_to_promise(async move {
            let params = parse_js_params(params)?;
            FrankenPreparedStatement::execute_with_params_admitted(guard, sql, params)
                .await
                .map(|changes| JsValue::from_f64(changes as f64))
        })
    }

    #[wasm_bindgen(js_name = query)]
    pub fn query_js(&self) -> Promise {
        let guard = match self.admit_operation() {
            Ok(guard) => guard,
            Err(error) => return Promise::reject(&error),
        };
        let sql = self.sql.clone();
        future_to_promise(FrankenPreparedStatement::query_admitted(guard, sql))
    }

    #[wasm_bindgen(js_name = queryWithParams)]
    pub fn query_with_params_js(&self, params: JsValue) -> Promise {
        let guard = match self.admit_operation() {
            Ok(guard) => guard,
            Err(error) => return Promise::reject(&error),
        };
        // See `execute_with_params_js`: JS conversion may run arbitrary
        // getters, including `stmt.free()`.
        let sql = self.sql.clone();
        future_to_promise(async move {
            let params = parse_js_params(params)?;
            FrankenPreparedStatement::query_with_params_admitted(guard, sql, params).await
        })
    }

    #[cfg(feature = "diagnostics")]
    #[wasm_bindgen(js_name = explain)]
    pub fn explain_js(&self) -> Promise {
        let guard = match self.admit_operation() {
            Ok(guard) => guard,
            Err(error) => return Promise::reject(&error),
        };
        let sql = self.sql.clone();
        future_to_promise(async move {
            FrankenPreparedStatement::explain_admitted(guard, sql)
                .await
                .map(|explanation| JsValue::from_str(&explanation))
        })
    }
}

#[cfg(feature = "prepared-statements")]
impl FrankenPreparedStatement {
    fn admit_operation(&self) -> Result<ConnectionOperationGuard, JsValue> {
        install_wasm_runtime();
        self.state
            .admit_connection_operation()
            .map_err(franken_error_to_js)
    }

    async fn execute_admitted(
        guard: ConnectionOperationGuard,
        sql: String,
    ) -> Result<usize, JsValue> {
        guard
            .finish(async |conn| {
                let stmt = conn.prepare(&sql).await?;
                stmt.execute().await
            })
            .await
    }

    async fn execute_with_params_admitted(
        guard: ConnectionOperationGuard,
        sql: String,
        params: Vec<SqliteValue>,
    ) -> Result<usize, JsValue> {
        guard
            .finish(async |conn| {
                let stmt = conn.prepare(&sql).await?;
                stmt.execute_with_params(&params).await
            })
            .await
    }

    async fn query_admitted(
        guard: ConnectionOperationGuard,
        sql: String,
    ) -> Result<JsValue, JsValue> {
        guard
            .finish(async |conn| {
                let stmt = conn.prepare(&sql).await?;
                let rows = stmt.query().await?;
                query_result_to_js(rows, stmt.column_names(), stmt.column_count())
            })
            .await
    }

    async fn query_with_params_admitted(
        guard: ConnectionOperationGuard,
        sql: String,
        params: Vec<SqliteValue>,
    ) -> Result<JsValue, JsValue> {
        guard
            .finish(async |conn| {
                let stmt = conn.prepare(&sql).await?;
                let rows = stmt.query_with_params(&params).await?;
                query_result_to_js(rows, stmt.column_names(), stmt.column_count())
            })
            .await
    }

    #[cfg(feature = "diagnostics")]
    async fn explain_admitted(
        guard: ConnectionOperationGuard,
        sql: String,
    ) -> Result<String, JsValue> {
        guard
            .finish(async |conn| {
                let stmt = conn.prepare(&sql).await?;
                Ok(stmt.explain())
            })
            .await
    }
}

#[cfg(all(test, feature = "prepared-statements"))]
#[allow(dead_code)]
impl FrankenPreparedStatement {
    async fn execute(&self) -> Result<usize, JsValue> {
        Self::execute_admitted(self.admit_operation()?, self.sql.clone()).await
    }

    async fn execute_with_params(&self, params: JsValue) -> Result<usize, JsValue> {
        let guard = self.admit_operation()?;
        let params = parse_js_params(params)?;
        Self::execute_with_params_admitted(guard, self.sql.clone(), params).await
    }

    async fn query(&self) -> Result<JsValue, JsValue> {
        Self::query_admitted(self.admit_operation()?, self.sql.clone()).await
    }

    async fn query_with_params(&self, params: JsValue) -> Result<JsValue, JsValue> {
        let guard = self.admit_operation()?;
        let params = parse_js_params(params)?;
        Self::query_with_params_admitted(guard, self.sql.clone(), params).await
    }

    #[cfg(feature = "diagnostics")]
    async fn explain(&self) -> Result<String, JsValue> {
        Self::explain_admitted(self.admit_operation()?, self.sql.clone()).await
    }
}

fn query_result_to_js(
    rows: Vec<CoreRow>,
    column_names: &[String],
    column_count: usize,
) -> Result<JsValue, FrankenError> {
    let resolved_columns = resolved_column_names(&rows, column_names, column_count);
    let columns = Array::new();
    for name in &resolved_columns {
        columns.push(&JsValue::from_str(name));
    }

    let column_types = Array::new();
    for ty in infer_column_types(&rows, resolved_columns.len()) {
        column_types.push(&JsValue::from_str(ty));
    }

    let js_rows = Array::new();
    #[cfg(feature = "row-arrays")]
    let row_arrays = Array::new();
    for row in &rows {
        #[cfg(feature = "row-arrays")]
        {
            let row_array = row_to_js_array(row)?;
            let row_value = JsValue::from(row_array);
            row_arrays.push(&row_value);
        }
        js_rows.push(&row_to_js_object(row, &resolved_columns)?);
    }

    let result = Object::new();
    set_property(&result, "columns", &columns.into())?;
    set_property(
        &result,
        "columnCount",
        &JsValue::from_f64(resolved_columns.len() as f64),
    )?;
    set_property(&result, "columnTypes", &column_types.into())?;
    set_property(&result, "rows", &js_rows.into())?;
    #[cfg(feature = "row-arrays")]
    set_property(&result, "rowArrays", &row_arrays.into())?;
    #[cfg(feature = "diagnostics")]
    set_property(&result, "changes", &JsValue::from_f64(0.0))?;
    Ok(result.into())
}

fn resolved_column_names(
    rows: &[CoreRow],
    column_names: &[String],
    column_count: usize,
) -> Vec<String> {
    let width = rows.first().map_or_else(
        || column_count.max(column_names.len()),
        |row| row.values().len().max(column_count.max(column_names.len())),
    );
    (0..width)
        .map(|index| {
            column_names
                .get(index)
                .cloned()
                .unwrap_or_else(|| format!("_c{index}"))
        })
        .collect()
}

fn infer_column_types(rows: &[CoreRow], width: usize) -> Vec<&'static str> {
    (0..width)
        .map(|index| {
            rows.iter()
                .filter_map(|row| row.values().get(index))
                .find(|value| !matches!(value, SqliteValue::Null))
                .map_or("unknown", sqlite_value_type_name)
        })
        .collect()
}

fn sqlite_value_type_name(value: &SqliteValue) -> &'static str {
    match value {
        SqliteValue::Null => "null",
        SqliteValue::Integer(_) => "integer",
        SqliteValue::Float(_) => "real",
        SqliteValue::Text(_) => "text",
        SqliteValue::Blob(_) => "blob",
    }
}

#[cfg(feature = "row-arrays")]
fn row_to_js_array(row: &CoreRow) -> Result<Array, FrankenError> {
    let values = Array::new();
    for value in row.values() {
        values.push(&sqlite_value_to_js(value)?);
    }
    Ok(values)
}

fn row_to_js_object(row: &CoreRow, columns: &[String]) -> Result<JsValue, FrankenError> {
    let object = Object::new();
    for (index, name) in columns.iter().enumerate() {
        let value = row
            .values()
            .get(index)
            .map(sqlite_value_to_js)
            .transpose()?
            .unwrap_or(JsValue::NULL);
        set_property(&object, name, &value)?;
    }
    Ok(object.into())
}

fn parse_js_params(params: JsValue) -> Result<Vec<SqliteValue>, JsValue> {
    if params.is_null() || params.is_undefined() {
        return Ok(Vec::new());
    }

    if !Array::is_array(&params) {
        return Err(franken_error_to_js(FrankenError::TypeMismatch {
            expected: "JavaScript array of query parameters".to_owned(),
            actual: "non-array value".to_owned(),
        }));
    }

    let js_params = Array::from(&params);
    let mut out = Vec::with_capacity(js_params.length() as usize);
    for value in js_params.iter() {
        out.push(js_value_to_sqlite_value(&value)?);
    }
    Ok(out)
}

const MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_991;
const MIN_SAFE_INTEGER: i64 = -9_007_199_254_740_991;

fn js_value_to_sqlite_value(value: &JsValue) -> Result<SqliteValue, JsValue> {
    if value.is_null() || value.is_undefined() {
        return Ok(SqliteValue::Null);
    }
    if let Some(text) = value.as_string() {
        return Ok(SqliteValue::Text(text.into()));
    }
    if let Some(boolean) = value.as_bool() {
        return Ok(SqliteValue::Integer(i64::from(boolean)));
    }
    if value.is_bigint() {
        let bigint_text = bigint_to_decimal_string(value).map_err(franken_error_to_js)?;
        return parse_bigint_sqlite_value(&bigint_text).map_err(franken_error_to_js);
    }
    if let Some(bytes) = value.dyn_ref::<Uint8Array>() {
        return Ok(SqliteValue::Blob(bytes.to_vec().into()));
    }
    #[cfg(feature = "date-params")]
    if let Some(date) = value.dyn_ref::<Date>() {
        return date_to_sqlite_value(date).map_err(franken_error_to_js);
    }
    if let Some(number) = value.as_f64() {
        return parse_js_number_value(number, Number::is_safe_integer(value))
            .map_err(franken_error_to_js);
    }

    Err(franken_error_to_js(FrankenError::TypeMismatch {
        expected: "SQLite-compatible scalar parameter".to_owned(),
        actual: describe_js_value(value),
    }))
}

fn sqlite_value_to_js(value: &SqliteValue) -> Result<JsValue, FrankenError> {
    match value {
        SqliteValue::Null => Ok(JsValue::NULL),
        SqliteValue::Integer(number) => {
            if is_js_safe_integer(*number) {
                Ok(JsValue::from_f64(*number as f64))
            } else {
                Ok(JsValue::bigint_from_str(&number.to_string()))
            }
        }
        SqliteValue::Float(number) => sqlite_float_to_js(*number),
        SqliteValue::Text(text) => Ok(JsValue::from_str(text)),
        SqliteValue::Blob(bytes) => Ok(Uint8Array::from(&**bytes).into()),
    }
}

fn franken_error_to_js(error: FrankenError) -> JsValue {
    let object = Object::new();
    let _ = set_property(
        &object,
        "code",
        &JsValue::from_str(&sqlite_error_name(&error)),
    );
    let _ = set_property(
        &object,
        "sqliteCode",
        &JsValue::from_f64(f64::from(error.exit_code())),
    );
    let _ = set_property(
        &object,
        "extendedCode",
        &JsValue::from_f64(f64::from(error.extended_error_code())),
    );
    let _ = set_property(&object, "message", &JsValue::from_str(&error.to_string()));
    #[cfg(feature = "diagnostics")]
    set_diagnostic_error_properties(&object, &error);
    object.into()
}

#[cfg(feature = "diagnostics")]
fn set_diagnostic_error_properties(object: &Object, error: &FrankenError) {
    let _ = set_property(
        object,
        "transient",
        &JsValue::from_bool(error.is_transient()),
    );
    let _ = set_property(
        object,
        "userRecoverable",
        &JsValue::from_bool(error.is_user_recoverable()),
    );
    if let Some(suggestion) = error.suggestion() {
        let _ = set_property(object, "suggestion", &JsValue::from_str(suggestion));
    }
}

fn sqlite_error_name(error: &FrankenError) -> String {
    match error {
        FrankenError::BusyRecovery => "SQLITE_BUSY_RECOVERY".to_owned(),
        FrankenError::BusySnapshot { .. } => "SQLITE_BUSY_SNAPSHOT".to_owned(),
        FrankenError::DatatypeViolation { .. } => "SQLITE_CONSTRAINT_DATATYPE".to_owned(),
        _ => format!("SQLITE_{:?}", error.error_code()).to_ascii_uppercase(),
    }
}

fn set_property(object: &Object, key: &str, value: &JsValue) -> Result<(), FrankenError> {
    Reflect::set(object.as_ref(), &JsValue::from_str(key), value)
        .map(|_| ())
        .map_err(|error| {
            FrankenError::internal(format!(
                "failed to set JavaScript property `{key}`: {}",
                js_error_message(&error)
            ))
        })
}

fn js_error_message(error: &JsValue) -> String {
    error
        .as_string()
        .unwrap_or_else(|| "non-string JavaScript exception".to_owned())
}

fn is_js_safe_integer(number: i64) -> bool {
    (MIN_SAFE_INTEGER..=MAX_SAFE_INTEGER).contains(&number)
}

fn parse_js_number_value(number: f64, is_safe_integer: bool) -> Result<SqliteValue, FrankenError> {
    if number.is_nan() {
        warn_nan_to_null();
        return Ok(SqliteValue::Null);
    }
    if !number.is_finite() {
        return Err(FrankenError::TypeMismatch {
            expected: "finite JavaScript number".to_owned(),
            actual: number.to_string(),
        });
    }
    if number.fract() == 0.0 && is_safe_integer {
        #[allow(clippy::cast_possible_truncation)]
        return Ok(SqliteValue::Integer(number as i64));
    }
    if number.fract() == 0.0 {
        return Err(FrankenError::TypeMismatch {
            expected: "JavaScript BigInt for INTEGER values outside Number.MAX_SAFE_INTEGER"
                .to_owned(),
            actual: number.to_string(),
        });
    }
    Ok(SqliteValue::Float(number))
}

fn sqlite_float_to_js(number: f64) -> Result<JsValue, FrankenError> {
    if number.is_nan() {
        warn_nan_to_null();
        return Ok(JsValue::NULL);
    }
    if !number.is_finite() {
        return Err(FrankenError::TypeMismatch {
            expected: "finite SQLite REAL".to_owned(),
            actual: number.to_string(),
        });
    }
    Ok(JsValue::from_f64(number))
}

#[cfg(all(feature = "diagnostics", target_arch = "wasm32"))]
fn warn_nan_to_null() {
    let global = js_sys::global();
    let Ok(console) = Reflect::get(&global, &JsValue::from_str("console")) else {
        return;
    };
    let Ok(warn) = Reflect::get(&console, &JsValue::from_str("warn")) else {
        return;
    };
    let Some(warn) = warn.dyn_ref::<js_sys::Function>() else {
        return;
    };
    let _ = warn.call1(
        &console,
        &JsValue::from_str("FrankenSQLite WASM coerced a JavaScript NaN parameter to SQLite NULL"),
    );
}

#[cfg(not(all(feature = "diagnostics", target_arch = "wasm32")))]
fn warn_nan_to_null() {}

fn bigint_to_decimal_string(value: &JsValue) -> Result<String, FrankenError> {
    let bigint = BigInt::new(value).map_err(|error| FrankenError::TypeMismatch {
        expected: "JavaScript BigInt".to_owned(),
        actual: format!("invalid bigint: {}", js_error_message(&error)),
    })?;
    bigint
        .to_string(10)
        .map(String::from)
        .map_err(|error| FrankenError::TypeMismatch {
            expected: "BigInt convertible to decimal string".to_owned(),
            actual: format!("BigInt formatting failed: {error:?}"),
        })
}

fn parse_bigint_sqlite_value(value: &str) -> Result<SqliteValue, FrankenError> {
    value
        .parse::<i64>()
        .map(SqliteValue::Integer)
        .map_err(|_| FrankenError::TypeMismatch {
            expected: "SQLite INTEGER in signed 64-bit range".to_owned(),
            actual: "BigInt outside SQLite INTEGER range".to_owned(),
        })
}

#[cfg(feature = "date-params")]
fn date_to_sqlite_value(date: &Date) -> Result<SqliteValue, FrankenError> {
    let timestamp = date.get_time();
    if !timestamp.is_finite() {
        return Err(FrankenError::TypeMismatch {
            expected: "valid JavaScript Date".to_owned(),
            actual: "invalid Date".to_owned(),
        });
    }
    Ok(SqliteValue::Text(SmallText::from_string(String::from(
        date.to_iso_string(),
    ))))
}

#[cfg(feature = "diagnostics")]
fn describe_js_value(value: &JsValue) -> String {
    if value.is_null() {
        return "null".to_owned();
    }
    if value.is_undefined() {
        return "undefined".to_owned();
    }
    if value.is_bigint() {
        return "bigint".to_owned();
    }
    #[cfg(feature = "date-params")]
    if value.dyn_ref::<Date>().is_some() {
        return "Date".to_owned();
    }
    if value.dyn_ref::<Uint8Array>().is_some() {
        return "Uint8Array".to_owned();
    }
    if Array::is_array(value) {
        return "Array".to_owned();
    }
    if value.is_object()
        && let Ok(constructor) = Reflect::get(value, &JsValue::from_str("constructor"))
        && let Ok(name) = Reflect::get(&constructor, &JsValue::from_str("name"))
        && let Some(name) = name.as_string()
    {
        return name;
    }
    value
        .js_typeof()
        .as_string()
        .unwrap_or_else(|| "unknown JavaScript value".to_owned())
}

#[cfg(not(feature = "diagnostics"))]
fn describe_js_value(value: &JsValue) -> String {
    if value.is_null() {
        return "null".to_owned();
    }
    if value.is_undefined() {
        return "undefined".to_owned();
    }
    value
        .js_typeof()
        .as_string()
        .unwrap_or_else(|| "unknown JavaScript value".to_owned())
}

#[cfg(feature = "memory-options")]
fn parse_database_options(options: Option<JsValue>) -> Result<WasmDatabaseOptions, JsValue> {
    let Some(options) = options.filter(|value| !value.is_null() && !value.is_undefined()) else {
        return Ok(WasmDatabaseOptions::default());
    };
    if !options.is_object() || Array::is_array(&options) {
        return Err(franken_error_to_js(FrankenError::TypeMismatch {
            expected: "FrankenDB open options object".to_owned(),
            actual: describe_js_value(&options),
        }));
    }

    let mut parsed = WasmDatabaseOptions {
        page_buffer_max: parse_optional_usize_property(&options, "pageBufferMax")
            .map_err(franken_error_to_js)?,
        ..WasmDatabaseOptions::default()
    };

    if let Some(memory_options) =
        get_optional_property(&options, "memory").map_err(franken_error_to_js)?
    {
        if !memory_options.is_object() || Array::is_array(&memory_options) {
            return Err(franken_error_to_js(FrankenError::TypeMismatch {
                expected: "FrankenDB memory options object".to_owned(),
                actual: describe_js_value(&memory_options),
            }));
        }
        parsed.initial_reserve_bytes = resolve_byte_or_page_memory_setting(
            parse_optional_usize_property(&memory_options, "initialReserveBytes")
                .map_err(franken_error_to_js)?,
            parse_optional_usize_property(&memory_options, "initialPages")
                .map_err(franken_error_to_js)?,
            "memory.initialReserveBytes",
            "memory.initialPages",
        )
        .map_err(franken_error_to_js)?;
        parsed.growth_chunk_bytes = resolve_byte_or_page_memory_setting(
            parse_optional_usize_property(&memory_options, "growthChunkBytes")
                .map_err(franken_error_to_js)?,
            parse_optional_usize_property(&memory_options, "growthChunkPages")
                .map_err(franken_error_to_js)?,
            "memory.growthChunkBytes",
            "memory.growthChunkPages",
        )
        .map_err(franken_error_to_js)?;
        parsed.max_bytes = resolve_byte_or_page_memory_setting(
            parse_optional_usize_property(&memory_options, "maxBytes")
                .map_err(franken_error_to_js)?,
            parse_optional_usize_property(&memory_options, "maxPages")
                .map_err(franken_error_to_js)?,
            "memory.maxBytes",
            "memory.maxPages",
        )
        .map_err(franken_error_to_js)?;
        #[cfg(feature = "diagnostics")]
        {
            parsed.warning_threshold_bytes =
                parse_optional_usize_property(&memory_options, "warningThresholdBytes")
                    .map_err(franken_error_to_js)?;
            parsed.warning_threshold_percent =
                parse_optional_percent_property(&memory_options, "warnAtPercent")
                    .map_err(franken_error_to_js)?;
            parsed.warning_callback =
                parse_optional_function_property(&memory_options, "onWarning")
                    .map_err(franken_error_to_js)?;
        }
        #[cfg(not(feature = "diagnostics"))]
        reject_diagnostics_memory_options(&memory_options).map_err(franken_error_to_js)?;
    }

    #[cfg(feature = "diagnostics")]
    parsed
        .effective_warning_threshold_bytes()
        .map_err(franken_error_to_js)?;
    parsed.memory_vfs_config().map_err(franken_error_to_js)?;
    Ok(parsed)
}

#[cfg(feature = "memory-options")]
fn get_optional_property(object: &JsValue, key: &str) -> Result<Option<JsValue>, FrankenError> {
    let value = Reflect::get(object, &JsValue::from_str(key)).map_err(|error| {
        FrankenError::internal(format!(
            "failed to read JavaScript property `{key}`: {}",
            js_error_message(&error)
        ))
    })?;
    if value.is_null() || value.is_undefined() {
        Ok(None)
    } else {
        Ok(Some(value))
    }
}

#[cfg(feature = "memory-options")]
fn parse_optional_usize_property(
    object: &JsValue,
    key: &str,
) -> Result<Option<usize>, FrankenError> {
    let Some(value) = get_optional_property(object, key)? else {
        return Ok(None);
    };
    parse_js_usize(&value, key).map(Some)
}

#[cfg(all(feature = "diagnostics", feature = "memory-options"))]
fn parse_optional_percent_property(
    object: &JsValue,
    key: &str,
) -> Result<Option<usize>, FrankenError> {
    let Some(value) = get_optional_property(object, key)? else {
        return Ok(None);
    };
    let percent = parse_js_usize(&value, key)?;
    if percent > 100 {
        return Err(FrankenError::OutOfRange {
            what: key.to_owned(),
            value: percent.to_string(),
        });
    }
    Ok(Some(percent))
}

#[cfg(all(feature = "memory-options", not(feature = "diagnostics")))]
fn reject_diagnostics_memory_options(object: &JsValue) -> Result<(), FrankenError> {
    for key in ["warningThresholdBytes", "warnAtPercent", "onWarning"] {
        if get_optional_property(object, key)?.is_some() {
            return Err(FrankenError::TypeMismatch {
                expected: format!("enable fsqlite-wasm diagnostics to use memory.{key}"),
                actual: "diagnostics-only memory warning option".to_owned(),
            });
        }
    }
    Ok(())
}

#[cfg(all(feature = "diagnostics", feature = "memory-options"))]
fn parse_optional_function_property(
    object: &JsValue,
    key: &str,
) -> Result<Option<Function>, FrankenError> {
    let Some(value) = get_optional_property(object, key)? else {
        return Ok(None);
    };
    value
        .dyn_ref::<Function>()
        .cloned()
        .ok_or_else(|| FrankenError::TypeMismatch {
            expected: format!("JavaScript function for `{key}`"),
            actual: describe_js_value(&value),
        })
        .map(Some)
}

#[cfg(feature = "memory-options")]
fn parse_js_usize(value: &JsValue, key: &str) -> Result<usize, FrankenError> {
    let Some(number) = value.as_f64() else {
        return Err(FrankenError::TypeMismatch {
            expected: format!("non-negative safe integer for `{key}`"),
            actual: describe_js_value(value),
        });
    };
    if !number.is_finite()
        || number < 0.0
        || number.fract() != 0.0
        || !Number::is_safe_integer(value)
    {
        return Err(FrankenError::TypeMismatch {
            expected: format!("non-negative safe integer for `{key}`"),
            actual: number.to_string(),
        });
    }
    usize::try_from(number as u64).map_err(|_| FrankenError::OutOfRange {
        what: key.to_owned(),
        value: number.to_string(),
    })
}

#[cfg(feature = "memory-options")]
fn resolve_byte_or_page_memory_setting(
    byte_value: Option<usize>,
    page_value: Option<usize>,
    byte_key: &str,
    page_key: &str,
) -> Result<Option<usize>, FrankenError> {
    let page_bytes = page_value
        .map(|pages| wasm_pages_to_bytes(pages, page_key))
        .transpose()?;
    match (byte_value, page_value, page_bytes) {
        (Some(bytes), Some(pages), Some(page_bytes)) if bytes != page_bytes => {
            Err(FrankenError::TypeMismatch {
                expected: format!("{byte_key} and {page_key} to resolve to the same byte count"),
                actual: format!("{byte_key}={bytes}, {page_key}={pages} ({page_bytes} bytes)"),
            })
        }
        (Some(bytes), _, _) => Ok(Some(bytes)),
        (None, _, Some(page_bytes)) => Ok(Some(page_bytes)),
        (None, None, None) => Ok(None),
        (None, Some(_), None) => unreachable!("page settings either convert to bytes or error"),
    }
}

#[cfg(feature = "memory-options")]
fn wasm_pages_to_bytes(pages: usize, key: &str) -> Result<usize, FrankenError> {
    pages
        .checked_mul(WASM_LINEAR_MEMORY_PAGE_BYTES)
        .ok_or_else(|| FrankenError::OutOfRange {
            what: key.to_owned(),
            value: pages.to_string(),
        })
}

#[cfg(all(feature = "diagnostics", feature = "memory-options"))]
fn threshold_bytes_from_percent(max_bytes: usize, percent: usize) -> Result<usize, FrankenError> {
    max_bytes
        .checked_mul(percent)
        .map(|scaled| scaled / 100)
        .ok_or_else(|| FrankenError::OutOfRange {
            what: "memory.warnAtPercent".to_owned(),
            value: percent.to_string(),
        })
}

#[cfg(feature = "diagnostics")]
fn exact_wasm_page_count(bytes: usize) -> Option<usize> {
    (bytes % WASM_LINEAR_MEMORY_PAGE_BYTES == 0).then_some(bytes / WASM_LINEAR_MEMORY_PAGE_BYTES)
}

#[cfg(feature = "diagnostics")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PageCachePressureAdvisory {
    level: &'static str,
    tracked_headroom_bytes: Option<usize>,
    budget_bytes: Option<usize>,
    recommended_page_buffer_max_pages: Option<usize>,
    recommended_page_buffer_max_bytes: Option<usize>,
}

#[cfg(feature = "diagnostics")]
fn page_cache_pressure_advisory(
    stats: ConnectionMemoryStats,
    warning_threshold_bytes: Option<usize>,
) -> PageCachePressureAdvisory {
    let tracked_max_bytes = stats.memory_vfs.and_then(|memory_vfs| memory_vfs.max_bytes);
    let budget_bytes = warning_threshold_bytes.or(tracked_max_bytes);
    let estimated_used_bytes = stats.estimated_used_bytes();
    let page_cache_used_bytes = stats.page_cache_used_bytes();
    let non_page_cache_bytes = estimated_used_bytes.saturating_sub(page_cache_used_bytes);
    let page_size_bytes = stats.page_size_bytes.max(1);
    let recommended_page_buffer_max_pages = budget_bytes.map(|budget| {
        let page_cache_budget_bytes = budget.saturating_sub(non_page_cache_bytes);
        let recommended_pages = page_cache_budget_bytes / page_size_bytes;
        recommended_pages.min(stats.page_cache.pool_capacity)
    });
    let recommended_page_buffer_max_bytes = recommended_page_buffer_max_pages
        .map(|recommended_pages| recommended_pages.saturating_mul(stats.page_size_bytes));
    let tracked_headroom_bytes =
        tracked_max_bytes.map(|max_bytes| max_bytes.saturating_sub(estimated_used_bytes));
    let level = if let Some(max_bytes) = tracked_max_bytes {
        if estimated_used_bytes >= max_bytes {
            "critical"
        } else if warning_threshold_bytes.is_some_and(|threshold| estimated_used_bytes >= threshold)
            || recommended_page_buffer_max_pages
                .is_some_and(|recommended_pages| recommended_pages < stats.page_cache.pool_capacity)
        {
            "warning"
        } else {
            "normal"
        }
    } else if warning_threshold_bytes.is_some() {
        if warning_threshold_bytes.is_some_and(|threshold| estimated_used_bytes >= threshold)
            || recommended_page_buffer_max_pages
                .is_some_and(|recommended_pages| recommended_pages < stats.page_cache.pool_capacity)
        {
            "warning"
        } else {
            "normal"
        }
    } else {
        "unbounded"
    };

    PageCachePressureAdvisory {
        level,
        tracked_headroom_bytes,
        budget_bytes,
        recommended_page_buffer_max_pages,
        recommended_page_buffer_max_bytes,
    }
}

#[cfg(feature = "diagnostics")]
fn connection_memory_stats_to_js(
    conn: &CoreConnection,
    stats: ConnectionMemoryStats,
    warning_threshold_bytes: Option<usize>,
    warning_threshold_percent: Option<usize>,
) -> Result<JsValue, JsValue> {
    let object = Object::new();
    let page_cache_used_bytes = stats.page_cache_used_bytes();
    let page_cache_capacity_bytes = stats.page_cache_capacity_bytes();
    let tracked_live_bytes = stats
        .memory_vfs
        .map_or(0, fsqlite_vfs::MemoryVfsUsageSnapshot::live_bytes);
    let tracked_reserved_bytes = stats
        .memory_vfs
        .map_or(0, fsqlite_vfs::MemoryVfsUsageSnapshot::reserved_bytes);
    let tracked_fragmentation_bytes = stats
        .memory_vfs
        .map_or(0, fsqlite_vfs::MemoryVfsUsageSnapshot::fragmentation_bytes);
    let estimated_used_bytes = stats.estimated_used_bytes();
    let pressure_advisory = page_cache_pressure_advisory(stats, warning_threshold_bytes);

    set_property(
        &object,
        "backendKind",
        &JsValue::from_str(conn.pager_backend_kind()),
    )
    .map_err(franken_error_to_js)?;
    set_property(
        &object,
        "pageSizeBytes",
        &JsValue::from_f64(stats.page_size_bytes as f64),
    )
    .map_err(franken_error_to_js)?;
    set_property(
        &object,
        "pageCachePages",
        &JsValue::from_f64(stats.page_cache.cached_pages as f64),
    )
    .map_err(franken_error_to_js)?;
    set_property(
        &object,
        "pageCacheCapacityPages",
        &JsValue::from_f64(stats.page_cache.pool_capacity as f64),
    )
    .map_err(franken_error_to_js)?;
    set_property(
        &object,
        "pageCacheBytes",
        &JsValue::from_f64(page_cache_used_bytes as f64),
    )
    .map_err(franken_error_to_js)?;
    set_property(
        &object,
        "pageCacheCapacityBytes",
        &JsValue::from_f64(page_cache_capacity_bytes as f64),
    )
    .map_err(franken_error_to_js)?;
    set_property(
        &object,
        "pageCacheDirtyRatioPct",
        &JsValue::from_f64(stats.page_cache.dirty_ratio_pct as f64),
    )
    .map_err(franken_error_to_js)?;
    set_property(
        &object,
        "trackedLiveBytes",
        &JsValue::from_f64(tracked_live_bytes as f64),
    )
    .map_err(franken_error_to_js)?;
    set_property(
        &object,
        "trackedReservedBytes",
        &JsValue::from_f64(tracked_reserved_bytes as f64),
    )
    .map_err(franken_error_to_js)?;
    set_property(
        &object,
        "trackedFragmentationBytes",
        &JsValue::from_f64(tracked_fragmentation_bytes as f64),
    )
    .map_err(franken_error_to_js)?;
    set_property(
        &object,
        "estimatedUsedBytes",
        &JsValue::from_f64(estimated_used_bytes as f64),
    )
    .map_err(franken_error_to_js)?;
    set_property(
        &object,
        "pageCachePressureLevel",
        &JsValue::from_str(pressure_advisory.level),
    )
    .map_err(franken_error_to_js)?;
    match pressure_advisory.tracked_headroom_bytes {
        Some(headroom_bytes) => set_property(
            &object,
            "trackedHeadroomBytes",
            &JsValue::from_f64(headroom_bytes as f64),
        )
        .map_err(franken_error_to_js)?,
        None => set_property(&object, "trackedHeadroomBytes", &JsValue::NULL)
            .map_err(franken_error_to_js)?,
    }
    match pressure_advisory.budget_bytes {
        Some(budget_bytes) => set_property(
            &object,
            "pageCachePressureBudgetBytes",
            &JsValue::from_f64(budget_bytes as f64),
        )
        .map_err(franken_error_to_js)?,
        None => set_property(&object, "pageCachePressureBudgetBytes", &JsValue::NULL)
            .map_err(franken_error_to_js)?,
    }
    match pressure_advisory.recommended_page_buffer_max_pages {
        Some(recommended_pages) => set_property(
            &object,
            "recommendedPageBufferMaxPages",
            &JsValue::from_f64(recommended_pages as f64),
        )
        .map_err(franken_error_to_js)?,
        None => set_property(&object, "recommendedPageBufferMaxPages", &JsValue::NULL)
            .map_err(franken_error_to_js)?,
    }
    match pressure_advisory.recommended_page_buffer_max_bytes {
        Some(recommended_bytes) => set_property(
            &object,
            "recommendedPageBufferMaxBytes",
            &JsValue::from_f64(recommended_bytes as f64),
        )
        .map_err(franken_error_to_js)?,
        None => set_property(&object, "recommendedPageBufferMaxBytes", &JsValue::NULL)
            .map_err(franken_error_to_js)?,
    }

    if let Some(memory_vfs) = stats.memory_vfs {
        set_property(
            &object,
            "fileBytes",
            &JsValue::from_f64(memory_vfs.file_bytes as f64),
        )
        .map_err(franken_error_to_js)?;
        set_property(
            &object,
            "fileReservedBytes",
            &JsValue::from_f64(memory_vfs.file_reserved_bytes as f64),
        )
        .map_err(franken_error_to_js)?;
        set_property(
            &object,
            "shmBytes",
            &JsValue::from_f64(memory_vfs.shm_bytes as f64),
        )
        .map_err(franken_error_to_js)?;
        set_property(
            &object,
            "shmReservedBytes",
            &JsValue::from_f64(memory_vfs.shm_reserved_bytes as f64),
        )
        .map_err(franken_error_to_js)?;
        set_property(
            &object,
            "trackedPeakReservedBytes",
            &JsValue::from_f64(memory_vfs.peak_reserved_bytes as f64),
        )
        .map_err(franken_error_to_js)?;
        set_property(
            &object,
            "growthEvents",
            &JsValue::from_f64(memory_vfs.growth_events as f64),
        )
        .map_err(franken_error_to_js)?;
        set_property(
            &object,
            "initialReserveBytes",
            &JsValue::from_f64(memory_vfs.initial_reserve_bytes as f64),
        )
        .map_err(franken_error_to_js)?;
        match exact_wasm_page_count(memory_vfs.initial_reserve_bytes) {
            Some(page_count) => set_property(
                &object,
                "initialReservePages",
                &JsValue::from_f64(page_count as f64),
            )
            .map_err(franken_error_to_js)?,
            None => set_property(&object, "initialReservePages", &JsValue::NULL)
                .map_err(franken_error_to_js)?,
        }
        set_property(
            &object,
            "growthChunkBytes",
            &JsValue::from_f64(memory_vfs.growth_chunk_bytes as f64),
        )
        .map_err(franken_error_to_js)?;
        match exact_wasm_page_count(memory_vfs.growth_chunk_bytes) {
            Some(page_count) => set_property(
                &object,
                "growthChunkPages",
                &JsValue::from_f64(page_count as f64),
            )
            .map_err(franken_error_to_js)?,
            None => set_property(&object, "growthChunkPages", &JsValue::NULL)
                .map_err(franken_error_to_js)?,
        }
        match memory_vfs.max_bytes {
            Some(max_bytes) => {
                set_property(
                    &object,
                    "trackedMaxBytes",
                    &JsValue::from_f64(max_bytes as f64),
                )
                .map_err(franken_error_to_js)?;
                match exact_wasm_page_count(max_bytes) {
                    Some(page_count) => set_property(
                        &object,
                        "trackedMaxPages",
                        &JsValue::from_f64(page_count as f64),
                    )
                    .map_err(franken_error_to_js)?,
                    None => set_property(&object, "trackedMaxPages", &JsValue::NULL)
                        .map_err(franken_error_to_js)?,
                }
            }
            None => {
                set_property(&object, "trackedMaxBytes", &JsValue::NULL)
                    .map_err(franken_error_to_js)?;
                set_property(&object, "trackedMaxPages", &JsValue::NULL)
                    .map_err(franken_error_to_js)?;
            }
        }
    } else {
        set_property(&object, "trackedMaxBytes", &JsValue::NULL).map_err(franken_error_to_js)?;
        set_property(&object, "trackedMaxPages", &JsValue::NULL).map_err(franken_error_to_js)?;
        set_property(&object, "initialReservePages", &JsValue::NULL)
            .map_err(franken_error_to_js)?;
        set_property(&object, "growthChunkPages", &JsValue::NULL).map_err(franken_error_to_js)?;
    }

    match warning_threshold_bytes {
        Some(threshold) => {
            set_property(
                &object,
                "warningThresholdBytes",
                &JsValue::from_f64(threshold as f64),
            )
            .map_err(franken_error_to_js)?;
            set_property(
                &object,
                "warningThresholdExceeded",
                &JsValue::from_bool(estimated_used_bytes >= threshold),
            )
            .map_err(franken_error_to_js)?;
        }
        None => {
            set_property(&object, "warningThresholdBytes", &JsValue::NULL)
                .map_err(franken_error_to_js)?;
            set_property(
                &object,
                "warningThresholdExceeded",
                &JsValue::from_bool(false),
            )
            .map_err(franken_error_to_js)?;
        }
    }
    match warning_threshold_percent {
        Some(percent) => set_property(
            &object,
            "warningThresholdPercent",
            &JsValue::from_f64(percent as f64),
        )
        .map_err(franken_error_to_js)?,
        None => set_property(&object, "warningThresholdPercent", &JsValue::NULL)
            .map_err(franken_error_to_js)?,
    }

    match wasm_linear_memory_bytes() {
        Some(linear_memory_bytes) => {
            set_property(
                &object,
                "linearMemoryBytes",
                &JsValue::from_f64(linear_memory_bytes as f64),
            )
            .map_err(franken_error_to_js)?;
            match exact_wasm_page_count(linear_memory_bytes) {
                Some(page_count) => set_property(
                    &object,
                    "linearMemoryPages",
                    &JsValue::from_f64(page_count as f64),
                )
                .map_err(franken_error_to_js)?,
                None => set_property(&object, "linearMemoryPages", &JsValue::NULL)
                    .map_err(franken_error_to_js)?,
            }
        }
        None => {
            set_property(&object, "linearMemoryBytes", &JsValue::NULL)
                .map_err(franken_error_to_js)?;
            set_property(&object, "linearMemoryPages", &JsValue::NULL)
                .map_err(franken_error_to_js)?;
        }
    }

    Ok(object.into())
}

#[cfg(all(feature = "diagnostics", target_arch = "wasm32"))]
fn wasm_linear_memory_bytes() -> Option<usize> {
    let memory = wasm_bindgen::memory()
        .dyn_into::<js_sys::WebAssembly::Memory>()
        .ok()?;
    let buffer = memory.buffer().dyn_into::<js_sys::ArrayBuffer>().ok()?;
    usize::try_from(buffer.byte_length()).ok()
}

#[cfg(all(feature = "diagnostics", not(target_arch = "wasm32")))]
fn wasm_linear_memory_bytes() -> Option<usize> {
    None
}

#[cfg(all(test, not(target_arch = "wasm32")))]
// The standard mutex deliberately serializes whole host-side LabRuntime tests;
// it is never acquired by production async code. Direct core futures are also
// large here, but these tests keep them inline to exercise their exact API.
#[allow(clippy::await_holding_lock, clippy::large_futures)]
mod tests {
    use super::*;
    #[cfg(feature = "diagnostics")]
    use fsqlite_pager::PageCacheMetricsSnapshot;
    use std::future::Future as _;
    use std::sync::{Mutex, OnceLock};

    fn host_connection_test_guard() -> std::sync::MutexGuard<'static, ()> {
        static HOST_CONNECTION_TEST_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
        HOST_CONNECTION_TEST_MUTEX
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap()
    }

    #[test]
    fn parse_select() {
        let (stmts, errors) = parse_sql("SELECT 1 + 2");
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn parse_create_table() {
        let (stmts, errors) = parse_sql("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn parse_error_reported() {
        let (_stmts, errors) = parse_sql("NOT VALID SQL {{{{");
        assert!(!errors.is_empty());
    }

    #[test]
    fn core_connection_roundtrip_for_wasm_wrapper() {
        asupersync::test_utils::run_test(|| async {
            let _guard = host_connection_test_guard();
            let conn = open_core_connection(":memory:")
                .await
                .expect("in-memory connection should open");
            conn.execute("CREATE TABLE wasm_rt (id INTEGER PRIMARY KEY, name TEXT)")
                .await
                .expect("schema create should succeed");
            conn.execute("INSERT INTO wasm_rt (id, name) VALUES (1, 'alpha'), (2, 'beta')")
                .await
                .expect("seed rows should insert");

            let stmt = conn
                .prepare("SELECT id, name FROM wasm_rt ORDER BY id")
                .await
                .expect("statement should prepare");
            assert_eq!(stmt.column_count(), 2);

            let rows = stmt.query().await.expect("query should succeed");
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values()[0], SqliteValue::Integer(1));
            assert_eq!(rows[0].values()[1], SqliteValue::Text("alpha".into()));
            assert_eq!(rows[1].values()[0], SqliteValue::Integer(2));
            assert_eq!(rows[1].values()[1], SqliteValue::Text("beta".into()));
        });
    }

    #[test]
    fn admitted_operation_survives_close_without_holding_state_borrow() {
        asupersync::test_utils::run_test(|| async {
            let _guard = host_connection_test_guard();
            let db = FrankenDb::new(None).await.expect("db should open");
            let yielded_once = Cell::new(false);
            let mut operation = Box::pin(db.with_connection(async |conn| {
                std::future::poll_fn(|cx| {
                    if yielded_once.replace(true) {
                        std::task::Poll::Ready(())
                    } else {
                        cx.waker().wake_by_ref();
                        std::task::Poll::Pending
                    }
                })
                .await;
                conn.execute("SELECT 1").await
            }));

            std::future::poll_fn(|cx| {
                assert!(
                    operation.as_mut().poll(cx).is_pending(),
                    "the operation must suspend after admission"
                );
                std::task::Poll::Ready(())
            })
            .await;

            assert!(db.state.operation_active.get());
            assert!(
                matches!(
                    db.state.admit_connection_operation(),
                    Err(FrankenError::Busy)
                ),
                "a second operation on the same connection must use SQLITE_BUSY"
            );

            db.close();
            assert!(
                db.state.connection_snapshot().is_err(),
                "close must reject operations admitted after it returns"
            );

            assert_eq!(
                operation
                    .await
                    .expect("the already-admitted operation should retain the connection"),
                1
            );
            assert!(!db.state.operation_active.get());
        });
    }

    #[test]
    fn core_prepared_statement_exposes_inferred_column_names() {
        asupersync::test_utils::run_test(|| async {
            let _guard = host_connection_test_guard();
            let conn = open_core_connection(":memory:")
                .await
                .expect("in-memory connection should open");
            conn.execute("CREATE TABLE wasm_cols (id INTEGER PRIMARY KEY, name TEXT)")
                .await
                .expect("schema create should succeed");

            let stmt = conn
                .prepare("SELECT id AS user_id, name, 1 + 2 FROM wasm_cols")
                .await
                .expect("statement should prepare");

            assert_eq!(stmt.column_count(), 3);
            assert_eq!(stmt.column_names(), &["user_id", "name", "_c2"]);
        });
    }

    #[cfg(feature = "memory-options")]
    #[test]
    fn core_connection_memory_stats_follow_wasm_memory_options() {
        asupersync::test_utils::run_test(|| async {
            let _guard = host_connection_test_guard();
            let options = WasmDatabaseOptions {
                page_buffer_max: Some(8),
                initial_reserve_bytes: Some(64 * 1024),
                growth_chunk_bytes: Some(16 * 1024),
                max_bytes: Some(128 * 1024),
                #[cfg(feature = "diagnostics")]
                warning_threshold_bytes: None,
                #[cfg(feature = "diagnostics")]
                warning_threshold_percent: None,
                #[cfg(feature = "diagnostics")]
                warning_callback: None,
            };
            let conn = open_core_connection_with_options(":memory:", &options)
                .await
                .expect("in-memory connection with explicit memory policy should open");
            let stats = conn
                .memory_stats()
                .expect("memory stats should be available");
            let memory_vfs = stats
                .memory_vfs
                .expect("memory backend should expose MemoryVfs usage");

            assert_eq!(stats.page_cache.pool_capacity, 8);
            assert_eq!(memory_vfs.initial_reserve_bytes, 64 * 1024);
            assert_eq!(memory_vfs.growth_chunk_bytes, 16 * 1024);
            assert_eq!(memory_vfs.max_bytes, Some(128 * 1024));
            assert_eq!(memory_vfs.file_reserved_bytes, 64 * 1024);
        });
    }

    #[cfg(all(feature = "diagnostics", feature = "memory-options"))]
    #[test]
    fn memory_warning_transition_fires_on_upward_crossing_and_rearms_after_recovery() {
        let (above, crossed) = memory_warning_transition(63, 64, false);
        assert!(!above);
        assert!(!crossed);

        let (above, crossed) = memory_warning_transition(64, 64, false);
        assert!(above);
        assert!(crossed);

        let (above, crossed) = memory_warning_transition(128, 64, true);
        assert!(above);
        assert!(!crossed);

        let (above, crossed) = memory_warning_transition(32, 64, true);
        assert!(!above);
        assert!(!crossed);

        let (above, crossed) = memory_warning_transition(65, 64, false);
        assert!(above);
        assert!(crossed);
    }

    #[cfg(feature = "diagnostics")]
    #[test]
    fn page_cache_pressure_advisory_is_unbounded_without_budget() {
        let stats = ConnectionMemoryStats {
            page_size_bytes: 4096,
            page_cache: PageCacheMetricsSnapshot {
                hits: 0,
                misses: 0,
                admits: 0,
                evictions: 0,
                cached_pages: 8,
                pool_capacity: 32,
                dirty_ratio_pct: 0,
                t1_size: 0,
                t2_size: 0,
                b1_size: 0,
                b2_size: 0,
                p_target: 0,
                mvcc_multi_version_pages: 0,
            },
            memory_vfs: None,
        };

        let advisory = page_cache_pressure_advisory(stats, None);
        assert_eq!(advisory.level, "unbounded");
        assert_eq!(advisory.tracked_headroom_bytes, None);
        assert_eq!(advisory.budget_bytes, None);
        assert_eq!(advisory.recommended_page_buffer_max_pages, None);
        assert_eq!(advisory.recommended_page_buffer_max_bytes, None);
    }

    #[cfg(feature = "diagnostics")]
    #[test]
    fn page_cache_pressure_advisory_recommends_lower_page_buffer_cap_under_budget() {
        let stats = ConnectionMemoryStats {
            page_size_bytes: 4096,
            page_cache: PageCacheMetricsSnapshot {
                hits: 0,
                misses: 0,
                admits: 0,
                evictions: 0,
                cached_pages: 64,
                pool_capacity: 128,
                dirty_ratio_pct: 0,
                t1_size: 0,
                t2_size: 0,
                b1_size: 0,
                b2_size: 0,
                p_target: 0,
                mvcc_multi_version_pages: 0,
            },
            memory_vfs: Some(fsqlite_vfs::MemoryVfsUsageSnapshot {
                file_bytes: 65_536,
                file_reserved_bytes: 131_072,
                shm_bytes: 0,
                shm_reserved_bytes: 0,
                peak_reserved_bytes: 131_072,
                growth_events: 1,
                file_count: 1,
                shm_region_count: 0,
                initial_reserve_bytes: 65_536,
                growth_chunk_bytes: 65_536,
                max_bytes: Some(524_288),
            }),
        };

        let advisory = page_cache_pressure_advisory(stats, Some(327_680));
        assert_eq!(advisory.level, "warning");
        assert_eq!(advisory.tracked_headroom_bytes, Some(131_072));
        assert_eq!(advisory.budget_bytes, Some(327_680));
        assert_eq!(advisory.recommended_page_buffer_max_pages, Some(48));
        assert_eq!(advisory.recommended_page_buffer_max_bytes, Some(196_608));
    }

    #[cfg(feature = "diagnostics")]
    #[test]
    fn page_cache_pressure_advisory_becomes_critical_at_tracked_cap() {
        let stats = ConnectionMemoryStats {
            page_size_bytes: 4096,
            page_cache: PageCacheMetricsSnapshot {
                hits: 0,
                misses: 0,
                admits: 0,
                evictions: 0,
                cached_pages: 96,
                pool_capacity: 128,
                dirty_ratio_pct: 0,
                t1_size: 0,
                t2_size: 0,
                b1_size: 0,
                b2_size: 0,
                p_target: 0,
                mvcc_multi_version_pages: 0,
            },
            memory_vfs: Some(fsqlite_vfs::MemoryVfsUsageSnapshot {
                file_bytes: 65_536,
                file_reserved_bytes: 131_072,
                shm_bytes: 0,
                shm_reserved_bytes: 0,
                peak_reserved_bytes: 131_072,
                growth_events: 1,
                file_count: 1,
                shm_region_count: 0,
                initial_reserve_bytes: 65_536,
                growth_chunk_bytes: 65_536,
                max_bytes: Some(524_288),
            }),
        };

        let advisory = page_cache_pressure_advisory(stats, Some(393_216));
        assert_eq!(advisory.level, "critical");
        assert_eq!(advisory.tracked_headroom_bytes, Some(0));
        assert_eq!(advisory.budget_bytes, Some(393_216));
        assert_eq!(advisory.recommended_page_buffer_max_pages, Some(64));
        assert_eq!(advisory.recommended_page_buffer_max_bytes, Some(262_144));
    }

    #[cfg(all(feature = "backup", feature = "memory-options"))]
    #[test]
    fn import_with_wasm_memory_cap_returns_out_of_memory() {
        asupersync::test_utils::run_test(|| async {
            let _guard = host_connection_test_guard();
            let seed = open_core_connection(":memory:")
                .await
                .expect("seed connection should open");
            seed.execute("CREATE TABLE wasm_seed (id INTEGER PRIMARY KEY, name TEXT)")
                .await
                .expect("seed schema create should succeed");
            seed.execute("INSERT INTO wasm_seed (id, name) VALUES (1, 'alpha')")
                .await
                .expect("seed insert should succeed");
            let image = seed
                .export_bytes()
                .await
                .expect("seed export should succeed");

            let options = WasmDatabaseOptions {
                max_bytes: Some(1024),
                ..WasmDatabaseOptions::default()
            };
            let error = import_core_connection_with_options(&image, &options)
                .await
                .expect_err("tight memory cap should reject import");
            assert!(matches!(error, FrankenError::OutOfMemory));
        });
    }

    #[cfg(all(feature = "batch-execution", feature = "prepared-statements"))]
    #[test]
    fn franken_db_prepare_and_execute_batch_work_on_host() {
        asupersync::test_utils::run_test(|| async {
            let _guard = host_connection_test_guard();
            let db = FrankenDb::new(None).await.expect("db should open");
            db.execute_batch(
                "CREATE TABLE wasm_batch (id INTEGER PRIMARY KEY, name TEXT);\
                 INSERT INTO wasm_batch (id, name) VALUES (1, 'alpha');\
                 INSERT INTO wasm_batch (id, name) VALUES (2, 'beta');",
            )
            .await
            .expect("batch execution should succeed");

            let stmt = db
                .prepare("SELECT id AS user_id, name FROM wasm_batch ORDER BY id")
                .await
                .expect("select should prepare");
            #[cfg(feature = "diagnostics")]
            assert_eq!(stmt.column_count(), 2);
            assert_eq!(
                stmt.execute()
                    .await
                    .expect("select execute should count rows"),
                2
            );
        });
    }

    #[cfg(feature = "batch-execution")]
    #[test]
    fn franken_db_execute_batch_allows_empty_and_comment_only_input_on_host() {
        asupersync::test_utils::run_test(|| async {
            let _guard = host_connection_test_guard();
            let db = FrankenDb::new(None).await.expect("db should open");
            db.execute_batch("")
                .await
                .expect("empty batch should be a no-op");
            db.execute_batch("  -- nothing here\n/* still empty */ ; ")
                .await
                .expect("comment-only batch should be a no-op");
            assert_eq!(
                db.execute("SELECT 1")
                    .await
                    .expect("database should remain usable after no-op batches"),
                1
            );
        });
    }

    #[test]
    fn js_safe_integer_boundaries_match_bigint_cutover() {
        assert!(is_js_safe_integer(MAX_SAFE_INTEGER));
        assert!(is_js_safe_integer(MIN_SAFE_INTEGER));
        assert!(!is_js_safe_integer(MAX_SAFE_INTEGER + 1));
        assert!(!is_js_safe_integer(MIN_SAFE_INTEGER - 1));
    }

    #[test]
    fn nan_number_maps_to_sqlite_null() {
        assert!(matches!(
            parse_js_number_value(f64::NAN, false).expect("NaN should coerce to NULL"),
            SqliteValue::Null
        ));
    }

    #[test]
    fn unsafe_integer_number_requires_bigint() {
        let error = parse_js_number_value((MAX_SAFE_INTEGER + 1) as f64, false)
            .expect_err("unsafe integers should be rejected");
        assert!(matches!(error, FrankenError::TypeMismatch { .. }));
        assert!(error.to_string().contains("BigInt"));
    }

    #[test]
    fn fractional_number_with_representable_precision_remains_real() {
        let number = ((1_i64 << 51) as f64) + 0.5;
        assert_eq!(number.fract(), 0.5);

        let value =
            parse_js_number_value(number, false).expect("fractional numbers should remain REAL");
        assert_eq!(value, SqliteValue::Float(number));
    }

    #[test]
    fn rounded_large_number_requires_bigint_after_js_precision_loss() {
        // JavaScript numbers above 2^53 lose sub-integer precision before the
        // binding sees them, so a source value like `MAX_SAFE_INTEGER + 0.5`
        // arrives as an integral f64 and must follow the BigInt path.
        let rounded = (MAX_SAFE_INTEGER as f64) + 0.5;
        assert_eq!(rounded, (MAX_SAFE_INTEGER + 1) as f64);
        assert_eq!(rounded.fract(), 0.0);

        let error = parse_js_number_value(rounded, false)
            .expect_err("precision-lost large numbers should require BigInt");
        assert!(matches!(error, FrankenError::TypeMismatch { .. }));
        assert!(error.to_string().contains("BigInt"));
    }

    #[test]
    fn infinite_number_is_rejected() {
        let error =
            parse_js_number_value(f64::INFINITY, false).expect_err("Infinity should be rejected");
        assert!(matches!(error, FrankenError::TypeMismatch { .. }));
    }

    #[test]
    fn infinite_sqlite_float_is_rejected() {
        let error = sqlite_float_to_js(f64::NEG_INFINITY)
            .expect_err("infinite SQLite REAL should be rejected");
        assert!(matches!(error, FrankenError::TypeMismatch { .. }));
    }

    #[test]
    fn bigint_text_must_fit_sqlite_integer_range() {
        let value =
            parse_bigint_sqlite_value("9223372036854775807").expect("i64::MAX should parse");
        assert_eq!(value, SqliteValue::Integer(i64::MAX));

        let error = parse_bigint_sqlite_value("9223372036854775808")
            .expect_err("overflowing BigInt should fail");
        assert!(matches!(error, FrankenError::TypeMismatch { .. }));
    }
}

#[cfg(all(test, target_arch = "wasm32"))]
#[wasm_bindgen(inline_js = r#"
export async function boundaryExecuteThenClose(db) {
    const admitted = db.execute(
        "CREATE TABLE boundary_close (id INTEGER PRIMARY KEY)"
    );
    db.close();
    const changes = await admitted;
    let postCloseError;
    try {
        await db.query("SELECT 1");
    } catch (error) {
        postCloseError = error;
    }
    return {
        changes,
        postCloseCode: postCloseError?.code,
        postCloseMessage: postCloseError?.message,
    };
}

export async function boundaryExecuteThenCloseAndFree(db) {
    const admitted = db.execute(
        "CREATE TABLE boundary_close_free (id INTEGER PRIMARY KEY)"
    );
    db.close();
    db.free();
    return { changes: await admitted };
}

export async function boundaryOverlapIsBusy(db) {
    const admitted = db.execute(
        "CREATE TABLE boundary_busy (id INTEGER PRIMARY KEY)"
    );
    let overlapError;
    try {
        await db.query("SELECT 1");
    } catch (error) {
        overlapError = error;
    }
    await admitted;
    const after = await db.query("SELECT 1 AS value");
    return {
        overlapCode: overlapError?.code,
        overlapMessage: overlapError?.message,
        afterRows: after.rows.length,
    };
}

export async function boundaryParamAdmissionOrdering(db) {
    let conversionTouches = 0;
    const untouchedWhileBusy = [];
    Object.defineProperty(untouchedWhileBusy, 0, {
        configurable: true,
        enumerable: true,
        get() {
            conversionTouches += 1;
            return { invalid: true };
        },
    });

    const admitted = db.execute(
        "CREATE TABLE boundary_params (id INTEGER PRIMARY KEY)"
    );
    let busyError;
    try {
        await db.executeWithParams("SELECT ?", untouchedWhileBusy);
    } catch (error) {
        busyError = error;
    }
    const touchesWhileBusy = conversionTouches;
    await admitted;

    let reentrantPromise;
    const reentrantParams = [];
    Object.defineProperty(reentrantParams, 0, {
        configurable: true,
        enumerable: true,
        get() {
            conversionTouches += 1;
            reentrantPromise = db.query("SELECT 1");
            return { invalid: true };
        },
    });

    let conversionError;
    try {
        await db.executeWithParams("SELECT ?", reentrantParams);
    } catch (error) {
        conversionError = error;
    }
    let reentrantError;
    try {
        await reentrantPromise;
    } catch (error) {
        reentrantError = error;
    }

    const after = await db.query("SELECT 1 AS value");
    return {
        busyCode: busyError?.code,
        touchesWhileBusy,
        conversionCode: conversionError?.code,
        reentrantCode: reentrantError?.code,
        finalTouches: conversionTouches,
        afterRows: after.rows.length,
    };
}

export async function boundaryPreparedQueryThenFree(db) {
    await db.execute(
        "CREATE TABLE boundary_prepared (id INTEGER PRIMARY KEY, value TEXT)"
    );
    await db.execute(
        "INSERT INTO boundary_prepared VALUES (1, 'kept alive')"
    );
    const stmt = await db.prepare(
        "SELECT id, value FROM boundary_prepared ORDER BY id"
    );
    const admitted = stmt.query();
    stmt.free();
    const result = await admitted;
    const after = await db.query("SELECT count(*) AS total FROM boundary_prepared");
    return {
        admittedRows: result.rows.length,
        afterRows: after.rows.length,
    };
}

export async function boundaryPreparedGetterFreesWrapper(db) {
    const stmt = await db.prepare("SELECT ? AS value");
    const params = [];
    Object.defineProperty(params, 0, {
        configurable: true,
        enumerable: true,
        get() {
            stmt.free();
            return 7;
        },
    });
    const prepared = await stmt.queryWithParams(params);
    const after = await db.query("SELECT 1 AS value");
    return {
        preparedRows: prepared.rows.length,
        preparedValue: prepared.rows[0].value,
        afterRows: after.rows.length,
    };
}
"#)]
extern "C" {
    #[wasm_bindgen(js_name = boundaryExecuteThenClose)]
    fn boundary_execute_then_close(db: &JsValue) -> Promise;

    #[wasm_bindgen(js_name = boundaryExecuteThenCloseAndFree)]
    fn boundary_execute_then_close_and_free(db: &JsValue) -> Promise;

    #[wasm_bindgen(js_name = boundaryOverlapIsBusy)]
    fn boundary_overlap_is_busy(db: &JsValue) -> Promise;

    #[wasm_bindgen(js_name = boundaryParamAdmissionOrdering)]
    fn boundary_param_admission_ordering(db: &JsValue) -> Promise;

    #[cfg(feature = "prepared-statements")]
    #[wasm_bindgen(js_name = boundaryPreparedQueryThenFree)]
    fn boundary_prepared_query_then_free(db: &JsValue) -> Promise;

    #[cfg(feature = "prepared-statements")]
    #[wasm_bindgen(js_name = boundaryPreparedGetterFreesWrapper)]
    fn boundary_prepared_getter_frees_wrapper(db: &JsValue) -> Promise;
}

#[cfg(all(test, target_arch = "wasm32"))]
mod wasm_tests {
    use super::*;
    use wasm_bindgen_futures::JsFuture;
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);

    #[wasm_bindgen_test]
    async fn wasm_memory_vfs_paths_and_open_are_host_independent() {
        use std::path::Path;

        use fsqlite_pager::SimplePager;
        use fsqlite_types::PageSize;
        use fsqlite_types::cx::Cx;
        use fsqlite_vfs::{MemoryVfs, Vfs};

        let path = Path::new("/:memory:");
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let resolved = vfs
            .full_pathname(&cx, path)
            .expect("MemoryVfs virtual path resolution should not access the host");
        assert_eq!(resolved, path);
        let relative = Path::new("relative.db");
        assert_eq!(
            vfs.full_pathname(&cx, relative)
                .expect("relative virtual keys should not access the host"),
            relative
        );

        SimplePager::open_with_cx(&cx, vfs, &resolved, PageSize::DEFAULT)
            .await
            .expect("MemoryVfs SimplePager bootstrap should open in the browser");

        CoreConnection::open(":memory:")
            .await
            .expect("CoreConnection memory bootstrap should open in the browser");
    }

    fn rows(result: &JsValue) -> Array {
        Reflect::get(result, &JsValue::from_str("rows"))
            .expect("rows field should exist")
            .unchecked_into::<Array>()
    }

    fn row_property(row: &Object, key: &str) -> JsValue {
        Reflect::get(row, &JsValue::from_str(key)).expect("row field should exist")
    }

    fn string_property(value: &JsValue, key: &str) -> Option<String> {
        Reflect::get(value, &JsValue::from_str(key))
            .expect("string property lookup should succeed")
            .as_string()
    }

    fn number_property(value: &JsValue, key: &str) -> Option<f64> {
        Reflect::get(value, &JsValue::from_str(key))
            .expect("number property lookup should succeed")
            .as_f64()
    }

    struct HotPathProfileGuard {
        hot_path: bool,
        btree_copy: bool,
        btree_metrics: bool,
        record: bool,
        vdbe: bool,
        pager_commit: bool,
    }

    impl HotPathProfileGuard {
        fn enabled() -> Self {
            let previous = Self {
                hot_path: fsqlite_core::connection::hot_path_profile_enabled(),
                btree_copy: fsqlite_btree::btree_copy_profile_enabled(),
                btree_metrics: fsqlite_btree::btree_metrics_enabled(),
                record: fsqlite_types::record::record_profile_enabled(),
                vdbe: fsqlite_vdbe::engine::vdbe_metrics_enabled(),
                pager_commit: fsqlite_pager::pager_commit_profile_enabled(),
            };
            fsqlite_core::connection::set_hot_path_profile_enabled(true);
            previous
        }
    }

    impl Drop for HotPathProfileGuard {
        fn drop(&mut self) {
            // The aggregate setter mutates every subordinate gate in
            // non-test dependency builds, so restore it first and then put
            // each independently configurable gate back exactly as found.
            fsqlite_core::connection::set_hot_path_profile_enabled(self.hot_path);
            fsqlite_btree::set_btree_copy_profile_enabled(self.btree_copy);
            fsqlite_btree::set_btree_metrics_enabled(self.btree_metrics);
            fsqlite_types::record::set_record_profile_enabled(self.record);
            fsqlite_vdbe::engine::set_vdbe_metrics_enabled(self.vdbe);
            fsqlite_pager::set_pager_commit_profile_enabled(self.pager_commit);
        }
    }

    #[cfg(feature = "row-arrays")]
    fn row_arrays(result: &JsValue) -> Array {
        Reflect::get(result, &JsValue::from_str("rowArrays"))
            .expect("rowArrays field should exist")
            .unchecked_into::<Array>()
    }

    async fn execute_seed_statements(db: &FrankenDb, statements: &[&str]) {
        for sql in statements {
            db.execute(sql)
                .await
                .expect("seed statement should succeed");
        }
    }

    fn error_message(error: &JsValue) -> String {
        Reflect::get(error, &JsValue::from_str("message"))
            .expect("message field should exist")
            .as_string()
            .expect("message should be a string")
    }

    #[wasm_bindgen_test]
    async fn generated_js_execute_then_close_keeps_admitted_operation_alive() {
        let db = FrankenDb::new(None).await.expect("db should open");
        let db_js = JsValue::from(db);
        let result = JsFuture::from(boundary_execute_then_close(&db_js))
            .await
            .expect("generated JS lifecycle probe should resolve");

        assert_eq!(number_property(&result, "changes"), Some(0.0));
        assert_eq!(
            string_property(&result, "postCloseCode").as_deref(),
            Some("SQLITE_INTERNAL")
        );
        assert!(
            string_property(&result, "postCloseMessage")
                .expect("closed error should include a message")
                .contains("closed")
        );
    }

    #[wasm_bindgen_test]
    async fn generated_js_execute_survives_immediate_close_and_free() {
        let db = FrankenDb::new(None).await.expect("db should open");
        let db_js = JsValue::from(db);
        let result = JsFuture::from(boundary_execute_then_close_and_free(&db_js))
            .await
            .expect("generated JS close-and-free probe should resolve");

        assert_eq!(number_property(&result, "changes"), Some(0.0));
    }

    #[wasm_bindgen_test]
    async fn generated_js_overlapping_operation_is_deterministically_busy() {
        let db = FrankenDb::new(None).await.expect("db should open");
        let db_js = JsValue::from(db);
        let result = JsFuture::from(boundary_overlap_is_busy(&db_js))
            .await
            .expect("generated JS overlap probe should resolve");

        assert_eq!(
            string_property(&result, "overlapCode").as_deref(),
            Some("SQLITE_BUSY")
        );
        assert!(
            string_property(&result, "overlapMessage")
                .expect("busy error should include a message")
                .contains("busy")
        );
        assert_eq!(number_property(&result, "afterRows"), Some(1.0));
    }

    #[wasm_bindgen_test]
    async fn generated_js_parameter_admission_precedes_conversion_and_reentry() {
        let db = FrankenDb::new(None).await.expect("db should open");
        let db_js = JsValue::from(db);
        let result = JsFuture::from(boundary_param_admission_ordering(&db_js))
            .await
            .expect("generated JS parameter-ordering probe should resolve");

        assert_eq!(
            string_property(&result, "busyCode").as_deref(),
            Some("SQLITE_BUSY")
        );
        assert_eq!(number_property(&result, "touchesWhileBusy"), Some(0.0));
        assert_eq!(
            string_property(&result, "conversionCode").as_deref(),
            Some("SQLITE_MISMATCH")
        );
        assert_eq!(
            string_property(&result, "reentrantCode").as_deref(),
            Some("SQLITE_BUSY")
        );
        assert_eq!(number_property(&result, "finalTouches"), Some(1.0));
        assert_eq!(
            number_property(&result, "afterRows"),
            Some(1.0),
            "synchronous conversion rejection must release the operation guard"
        );
    }

    #[cfg(feature = "prepared-statements")]
    #[wasm_bindgen_test]
    async fn generated_js_prepared_query_survives_immediate_free() {
        let db = FrankenDb::new(None).await.expect("db should open");
        let db_js = JsValue::from(db);
        let result = JsFuture::from(boundary_prepared_query_then_free(&db_js))
            .await
            .expect("generated JS prepared lifecycle probe should resolve");

        assert_eq!(number_property(&result, "admittedRows"), Some(1.0));
        assert_eq!(number_property(&result, "afterRows"), Some(1.0));
    }

    #[cfg(feature = "prepared-statements")]
    #[wasm_bindgen_test]
    async fn generated_js_prepared_param_getter_can_free_wrapper_safely() {
        let db = FrankenDb::new(None).await.expect("db should open");
        let db_js = JsValue::from(db);
        let result = JsFuture::from(boundary_prepared_getter_frees_wrapper(&db_js))
            .await
            .expect("generated JS getter/free probe should resolve");

        assert_eq!(number_property(&result, "preparedRows"), Some(1.0));
        assert_eq!(number_property(&result, "preparedValue"), Some(7.0));
        assert_eq!(number_property(&result, "afterRows"), Some(1.0));
    }

    #[cfg(feature = "memory-options")]
    fn set_js_number_property(object: &Object, key: &str, value: usize) {
        set_property(object, key, &JsValue::from_f64(value as f64))
            .expect("numeric option property should be set");
    }

    #[cfg(feature = "diagnostics")]
    fn js_object_property_usize(object: &Object, key: &str) -> Option<usize> {
        Reflect::get(object, &JsValue::from_str(key))
            .expect("numeric property lookup should succeed")
            .as_f64()
            .map(|value| value as usize)
    }

    #[cfg(feature = "diagnostics")]
    fn js_object_property_string(object: &Object, key: &str) -> Option<String> {
        Reflect::get(object, &JsValue::from_str(key))
            .expect("string property lookup should succeed")
            .as_string()
    }

    #[cfg(all(feature = "diagnostics", feature = "memory-options"))]
    #[wasm_bindgen_test]
    fn wasm_parse_database_options_accepts_page_aliases_and_warn_at_percent() {
        let options = Object::new();
        set_js_number_property(&options, "pageBufferMax", 16);
        let memory = Object::new();
        set_js_number_property(&memory, "initialPages", 2);
        set_js_number_property(&memory, "growthChunkPages", 1);
        set_js_number_property(&memory, "maxPages", 8);
        set_js_number_property(&memory, "warnAtPercent", 75);
        set_property(&options, "memory", &memory.into()).expect("memory options should be set");

        let parsed = parse_database_options(Some(options.into())).expect("options should parse");
        assert_eq!(parsed.page_buffer_max, Some(16));
        assert_eq!(
            parsed.initial_reserve_bytes,
            Some(2 * WASM_LINEAR_MEMORY_PAGE_BYTES)
        );
        assert_eq!(
            parsed.growth_chunk_bytes,
            Some(WASM_LINEAR_MEMORY_PAGE_BYTES)
        );
        assert_eq!(parsed.max_bytes, Some(8 * WASM_LINEAR_MEMORY_PAGE_BYTES));
        assert_eq!(parsed.warning_threshold_percent, Some(75));
        assert_eq!(
            parsed.effective_warning_threshold_bytes().unwrap(),
            Some((8 * WASM_LINEAR_MEMORY_PAGE_BYTES * 75) / 100)
        );
    }

    #[cfg(all(feature = "memory-options", not(feature = "diagnostics")))]
    #[wasm_bindgen_test]
    fn wasm_parse_database_options_rejects_warning_options_without_diagnostics() {
        let options = Object::new();
        let memory = Object::new();
        set_js_number_property(&memory, "maxPages", 8);
        set_js_number_property(&memory, "warnAtPercent", 75);
        set_property(&options, "memory", &memory.into()).expect("memory options should be set");

        let error = parse_database_options(Some(options.into()))
            .err()
            .expect("default build should reject diagnostics-only memory warnings");
        let message = error_message(&error);
        assert!(message.contains("enable fsqlite-wasm diagnostics"));
        assert!(message.contains("memory.warnAtPercent"));
    }

    #[cfg(feature = "memory-options")]
    #[wasm_bindgen_test]
    fn wasm_parse_database_options_rejects_conflicting_page_and_byte_aliases() {
        let options = Object::new();
        let memory = Object::new();
        set_js_number_property(
            &memory,
            "initialReserveBytes",
            WASM_LINEAR_MEMORY_PAGE_BYTES,
        );
        set_js_number_property(&memory, "initialPages", 2);
        set_property(&options, "memory", &memory.into()).expect("memory options should be set");

        let error = parse_database_options(Some(options.into()))
            .err()
            .expect("conflicting aliases should fail");
        let message = error_message(&error);
        assert!(message.contains("memory.initialReserveBytes"));
        assert!(message.contains("memory.initialPages"));
    }

    #[cfg(all(feature = "diagnostics", feature = "memory-options"))]
    #[wasm_bindgen_test]
    fn wasm_parse_database_options_requires_tracked_cap_for_warn_at_percent() {
        let options = Object::new();
        let memory = Object::new();
        set_js_number_property(&memory, "warnAtPercent", 80);
        set_property(&options, "memory", &memory.into()).expect("memory options should be set");

        let error = parse_database_options(Some(options.into()))
            .err()
            .expect("warnAtPercent without cap should fail");
        assert!(error_message(&error).contains("memory.maxBytes or memory.maxPages"));
    }

    #[cfg(all(feature = "diagnostics", feature = "memory-options"))]
    #[wasm_bindgen_test]
    async fn wasm_open_with_options_surfaces_page_aliases_and_warning_percent() {
        let options = Object::new();
        set_js_number_property(&options, "pageBufferMax", 8);
        let memory = Object::new();
        set_js_number_property(&memory, "initialPages", 2);
        set_js_number_property(&memory, "growthChunkPages", 1);
        set_js_number_property(&memory, "maxPages", 8);
        set_js_number_property(&memory, "warnAtPercent", 75);
        set_property(&options, "memory", &memory.into()).expect("memory options should be set");

        let db = FrankenDb::open_with_options(None, Some(options.into()))
            .await
            .expect("memory-configured connection should open");
        let stats = db.memory_stats().expect("memory stats should be available");
        let object = stats.unchecked_into::<Object>();

        assert_eq!(
            js_object_property_usize(&object, "initialReservePages"),
            Some(2)
        );
        assert_eq!(
            js_object_property_usize(&object, "growthChunkPages"),
            Some(1)
        );
        assert_eq!(
            js_object_property_usize(&object, "trackedMaxPages"),
            Some(8)
        );
        assert_eq!(
            js_object_property_usize(&object, "warningThresholdPercent"),
            Some(75)
        );
        assert_eq!(
            js_object_property_usize(&object, "warningThresholdBytes"),
            Some((8 * WASM_LINEAR_MEMORY_PAGE_BYTES * 75) / 100)
        );
        assert_eq!(
            js_object_property_string(&object, "pageCachePressureLevel").as_deref(),
            Some("normal")
        );
        assert_eq!(
            js_object_property_usize(&object, "pageCachePressureBudgetBytes"),
            Some((8 * WASM_LINEAR_MEMORY_PAGE_BYTES * 75) / 100)
        );
    }

    #[cfg(feature = "diagnostics")]
    #[wasm_bindgen_test]
    fn describe_js_value_reports_rich_types_with_diagnostics() {
        assert_eq!(describe_js_value(&Array::new().into()), "Array");
        assert_eq!(
            describe_js_value(&Uint8Array::new_with_length(0).into()),
            "Uint8Array"
        );
    }

    #[cfg(not(feature = "diagnostics"))]
    #[wasm_bindgen_test]
    fn describe_js_value_uses_compact_default_types() {
        assert_eq!(describe_js_value(&JsValue::NULL), "null");
        assert_eq!(describe_js_value(&JsValue::UNDEFINED), "undefined");
        assert_eq!(describe_js_value(&Array::new().into()), "object");
    }

    #[wasm_bindgen_test]
    async fn wasm_db_roundtrip() {
        let db = FrankenDb::new(None).await.expect("db should open");
        db.execute("CREATE TABLE wasm_t (id INTEGER PRIMARY KEY, name TEXT)")
            .await
            .expect("table create should succeed");
        db.execute("INSERT INTO wasm_t (id, name) VALUES (1, 'alpha'), (2, 'beta')")
            .await
            .expect("seed insert should succeed");

        let result = db
            .query("SELECT id, name FROM wasm_t ORDER BY id")
            .await
            .expect("query should succeed");
        let rows = Reflect::get(&result, &JsValue::from_str("rows"))
            .expect("rows field should exist")
            .unchecked_into::<Array>();

        assert_eq!(rows.length(), 2);
    }

    #[wasm_bindgen_test]
    async fn wasm_wall_clock_sql_and_snapshot_capture_use_browser_time() {
        let db = FrankenDb::new(None).await.expect("db should open");
        let result = db
            .query("SELECT CURRENT_TIMESTAMP AS observed_at")
            .await
            .expect("browser wall-clock query should succeed");
        let rows = Reflect::get(&result, &JsValue::from_str("rows"))
            .expect("rows field should exist")
            .unchecked_into::<Array>();
        let first_row = rows.get(0).unchecked_into::<Object>();
        let observed_at = row_property(&first_row, "observed_at")
            .as_string()
            .expect("CURRENT_TIMESTAMP should be text");
        assert_eq!(observed_at.len(), 19);

        db.execute("CREATE TABLE wasm_clock (id INTEGER PRIMARY KEY)")
            .await
            .expect("table create should capture a time-travel snapshot");
        db.execute("INSERT INTO wasm_clock VALUES (1)")
            .await
            .expect("insert should capture a time-travel snapshot");
    }

    #[wasm_bindgen_test]
    async fn wasm_hot_path_profiles_use_browser_clock() {
        let _profile_guard = HotPathProfileGuard::enabled();
        let db = FrankenDb::new(None).await.expect("db should open");
        db.execute("CREATE TABLE wasm_profile (id INTEGER PRIMARY KEY, value TEXT)")
            .await
            .expect("profiled table create should succeed");
        db.execute("INSERT INTO wasm_profile (id, value) VALUES (1, 'alpha'), (2, 'beta')")
            .await
            .expect("profiled insert should succeed");

        let result = db
            .query("SELECT id, value FROM wasm_profile ORDER BY id")
            .await
            .expect("profiled query should succeed");
        assert_eq!(rows(&result).length(), 2);

        db.execute("DELETE FROM wasm_profile WHERE id = 1")
            .await
            .expect("profiled delete should succeed");
        let result = db
            .query("SELECT id FROM wasm_profile ORDER BY id")
            .await
            .expect("post-delete profile query should succeed");
        assert_eq!(rows(&result).length(), 1);
    }

    #[wasm_bindgen_test]
    async fn wasm_open_and_close_is_idempotent() {
        let db = FrankenDb::new(None)
            .await
            .expect("db should open via constructor");
        #[cfg(feature = "diagnostics")]
        assert_eq!(db.path(), ":memory:");

        db.close();
        db.close();

        let error = db
            .query("SELECT 1")
            .await
            .expect_err("queries after close should produce a JS error");
        assert!(error_message(&error).contains("closed"));
    }

    #[cfg(feature = "api-extras")]
    #[wasm_bindgen_test]
    async fn wasm_static_open_constructor_creates_database() {
        let db = FrankenDb::open(None)
            .await
            .expect("db should open via static constructor");
        assert_eq!(
            db.execute("CREATE TABLE wasm_static_open (id INTEGER PRIMARY KEY)")
                .await
                .expect("table create should succeed"),
            0
        );
    }

    #[cfg(all(feature = "diagnostics", feature = "memory-options"))]
    #[wasm_bindgen_test]
    async fn wasm_memory_warning_callback_receives_stats_payload_once_while_above_threshold() {
        use wasm_bindgen::closure::Closure;

        let warning_count = Rc::new(Cell::new(0));
        let callback_warning_count = Rc::clone(&warning_count);
        let saw_exceeded_payload = Rc::new(Cell::new(false));
        let callback_saw_exceeded_payload = Rc::clone(&saw_exceeded_payload);
        let callback = Closure::<dyn FnMut(JsValue)>::wrap(Box::new(move |stats: JsValue| {
            callback_warning_count.set(callback_warning_count.get() + 1);
            let exceeded = Reflect::get(&stats, &JsValue::from_str("warningThresholdExceeded"))
                .expect("warningThresholdExceeded field should exist")
                .as_bool()
                .expect("warningThresholdExceeded should be a bool");
            let page_cache_bytes = Reflect::get(&stats, &JsValue::from_str("pageCacheBytes"))
                .expect("pageCacheBytes field should exist")
                .as_f64()
                .expect("pageCacheBytes should be numeric");
            assert!(page_cache_bytes.is_finite());
            callback_saw_exceeded_payload.set(exceeded);
        }));

        let options = Object::new();
        let memory = Object::new();
        set_property(&memory, "initialPages", &JsValue::from_f64(1.0)).unwrap();
        set_property(&memory, "maxPages", &JsValue::from_f64(8.0)).unwrap();
        set_property(&memory, "warningThresholdBytes", &JsValue::from_f64(1.0)).unwrap();
        set_property(&memory, "onWarning", callback.as_ref()).unwrap();
        set_property(&options, "memory", &memory.into()).unwrap();

        let db = FrankenDb::open_with_options(None, Some(options.into()))
            .await
            .expect("diagnostic memory options should open");
        assert_eq!(warning_count.get(), 1);
        assert!(saw_exceeded_payload.get());

        db.execute("CREATE TABLE wasm_memory_warning (id INTEGER PRIMARY KEY)")
            .await
            .expect("table create should succeed");
        assert_eq!(warning_count.get(), 1);
    }

    #[cfg(feature = "batch-execution")]
    #[wasm_bindgen_test]
    async fn wasm_execute_reports_changes_and_batch_runs_multiple_statements() {
        let db = FrankenDb::new(None).await.expect("db should open");
        assert_eq!(
            db.execute("CREATE TABLE wasm_counts (id INTEGER PRIMARY KEY, name TEXT)")
                .await
                .expect("table create should succeed"),
            0
        );
        assert_eq!(
            db.execute("INSERT INTO wasm_counts (id, name) VALUES (1, 'alpha')")
                .await
                .expect("single insert should report one change"),
            1
        );
        db.execute_batch(
            "INSERT INTO wasm_counts (id, name) VALUES (2, 'beta');\
             INSERT INTO wasm_counts (id, name) VALUES (3, 'gamma');\
             UPDATE wasm_counts SET name = 'delta' WHERE id = 2;",
        )
        .await
        .expect("batch execution should succeed");

        let rows = rows(
            &db.query("SELECT id, name FROM wasm_counts ORDER BY id")
                .await
                .expect("query should succeed"),
        );
        assert_eq!(rows.length(), 3);
        let second_row = rows.get(1).unchecked_into::<Object>();
        assert_eq!(
            row_property(&second_row, "name").as_string().as_deref(),
            Some("delta")
        );
    }

    #[cfg(feature = "batch-execution")]
    #[wasm_bindgen_test]
    async fn wasm_execute_batch_allows_empty_and_comment_only_input() {
        let db = FrankenDb::new(None).await.expect("db should open");
        db.execute_batch("")
            .await
            .expect("empty batch should be a no-op");
        db.execute_batch("  -- nothing here\n/* still empty */ ; ")
            .await
            .expect("comment-only batch should be a no-op");
        assert_eq!(
            db.execute("SELECT 1")
                .await
                .expect("database should remain usable after no-op batches"),
            1
        );
    }

    #[cfg(feature = "backup")]
    #[wasm_bindgen_test]
    async fn wasm_export_import_roundtrips_sqlite_image() {
        let db = FrankenDb::new(None).await.expect("db should open");
        execute_seed_statements(
            &db,
            &[
                "CREATE TABLE wasm_export (id INTEGER PRIMARY KEY, name TEXT, payload BLOB)",
                "INSERT INTO wasm_export VALUES (1, 'alpha', X'DEADBEEF')",
                "INSERT INTO wasm_export VALUES (2, 'beta', X'010203')",
            ],
        )
        .await;

        let exported = db.export().await.expect("export should succeed");
        let exported_bytes = exported.to_vec();
        assert!(
            exported_bytes.starts_with(b"SQLite format 3\0"),
            "export should produce a standard SQLite image header"
        );

        let imported = FrankenDb::import(exported)
            .await
            .expect("import should succeed");
        #[cfg(feature = "diagnostics")]
        assert_eq!(imported.path(), ":memory:");

        let result_rows = rows(
            &imported
                .query("SELECT id, name, payload FROM wasm_export ORDER BY id")
                .await
                .expect("query should succeed after import"),
        );
        assert_eq!(result_rows.length(), 2);

        let first_row = result_rows.get(0).unchecked_into::<Object>();
        assert_eq!(row_property(&first_row, "id").as_f64(), Some(1.0));
        assert_eq!(
            row_property(&first_row, "name").as_string().as_deref(),
            Some("alpha")
        );
        assert_eq!(
            Uint8Array::new(&row_property(&first_row, "payload")).to_vec(),
            vec![0xDE, 0xAD, 0xBE, 0xEF]
        );

        let second_row = result_rows.get(1).unchecked_into::<Object>();
        assert_eq!(row_property(&second_row, "id").as_f64(), Some(2.0));
        assert_eq!(
            row_property(&second_row, "name").as_string().as_deref(),
            Some("beta")
        );
        assert_eq!(
            Uint8Array::new(&row_property(&second_row, "payload")).to_vec(),
            vec![1, 2, 3]
        );

        #[cfg(feature = "memory-options")]
        {
            let options = Object::new();
            set_js_number_property(&options, "pageBufferMax", 8);
            let imported_with_options = FrankenDb::import_with_options(
                Uint8Array::from(exported_bytes.as_slice()),
                Some(options.into()),
            )
            .await
            .expect("importWithOptions should import the exported image");
            let result = imported_with_options
                .query("SELECT count(*) AS row_count FROM wasm_export")
                .await
                .expect("query should succeed after importWithOptions");
            let imported_rows = rows(&result);
            assert_eq!(imported_rows.length(), 1);
            let row = imported_rows.get(0).unchecked_into::<Object>();
            assert_eq!(row_property(&row, "row_count").as_f64(), Some(2.0));
        }
    }

    #[cfg(feature = "backup")]
    #[wasm_bindgen_test]
    async fn wasm_import_rejects_empty_database_image() {
        let error = FrankenDb::import(Uint8Array::new_with_length(0))
            .await
            .err()
            .expect("empty image should be rejected");
        assert!(error_message(&error).contains("empty"));
    }

    #[cfg(feature = "diagnostics")]
    #[wasm_bindgen_test]
    fn parse_sql_export_reports_errors() {
        let result = parse_sql_js("NOT VALID SQL {{{{").expect("parse export should return");
        let error_count = Reflect::get(&result, &JsValue::from_str("errorCount"))
            .expect("errorCount should exist")
            .as_f64()
            .expect("errorCount should be numeric");
        assert!(error_count >= 1.0);
    }

    #[wasm_bindgen_test]
    fn wasm_nan_sqlite_float_maps_to_js_null() {
        let value = sqlite_float_to_js(f64::NAN).expect("NaN should coerce to JS null");
        assert!(value.is_null());
    }

    #[wasm_bindgen_test]
    async fn wasm_value_conversion_round_trips_with_type_fidelity() {
        let db = FrankenDb::new(None).await.expect("db should open");
        db.execute(
            "CREATE TABLE wasm_types (
                safe_i INTEGER,
                big_i INTEGER,
                real_v REAL,
                text_v TEXT,
                blob_v BLOB,
                null_v
            )",
        )
        .await
        .expect("table create should succeed");

        let params = Array::new();
        params.push(&JsValue::from_f64(42.0));
        params.push(&JsValue::bigint_from_str("9007199254740992"));
        params.push(&JsValue::from_f64(3.5));
        params.push(&JsValue::from_str("hello"));
        let input_blob = Uint8Array::from([0xDE_u8, 0xAD, 0xBE, 0xEF].as_slice());
        params.push(&input_blob.clone().into());
        params.push(&JsValue::NULL);

        db.execute_with_params(
            "INSERT INTO wasm_types VALUES (?, ?, ?, ?, ?, ?)",
            params.into(),
        )
        .await
        .expect("parameterized insert should succeed");

        let result = db
            .query("SELECT safe_i, big_i, real_v, text_v, blob_v, null_v FROM wasm_types")
            .await
            .expect("query should succeed");
        let rows = rows(&result);
        assert_eq!(rows.length(), 1);

        let row = rows.get(0).unchecked_into::<Object>();
        assert_eq!(row_property(&row, "safe_i").as_f64(), Some(42.0));
        assert!(
            row_property(&row, "big_i").is_bigint(),
            "large INTEGER should surface as BigInt"
        );
        let roundtrip_bigint = BigInt::new(&row_property(&row, "big_i"))
            .expect("returned large integer should be a BigInt");
        assert_eq!(
            String::from(
                roundtrip_bigint
                    .to_string(10)
                    .expect("returned BigInt should format")
            ),
            "9007199254740992"
        );
        assert_eq!(row_property(&row, "real_v").as_f64(), Some(3.5));
        assert_eq!(
            row_property(&row, "text_v").as_string().as_deref(),
            Some("hello")
        );

        let blob = Uint8Array::new(&row_property(&row, "blob_v"));
        assert_eq!(blob.to_vec(), vec![0xDE, 0xAD, 0xBE, 0xEF]);

        assert!(
            row_property(&row, "null_v").is_null(),
            "NULL should remain null in JS"
        );
    }

    #[cfg(feature = "date-params")]
    #[wasm_bindgen_test]
    async fn wasm_date_parameter_converts_to_iso_text() {
        let db = FrankenDb::new(None).await.expect("db should open");
        db.execute("CREATE TABLE wasm_dates (date_v TEXT)")
            .await
            .expect("table create should succeed");

        let input_date = Date::new(&JsValue::from_str("2026-03-11T12:34:56.000Z"));
        let expected_iso = String::from(input_date.to_iso_string());
        let params = Array::new();
        params.push(&input_date.into());

        db.execute_with_params("INSERT INTO wasm_dates VALUES (?)", params.into())
            .await
            .expect("Date parameter insert should succeed");

        let result = db
            .query("SELECT date_v FROM wasm_dates")
            .await
            .expect("query should succeed");
        let rows = rows(&result);
        let row = rows.get(0).unchecked_into::<Object>();
        assert_eq!(
            row_property(&row, "date_v").as_string().as_deref(),
            Some(expected_iso.as_str())
        );
    }

    #[wasm_bindgen_test]
    async fn wasm_value_conversion_reports_overflow_and_unsupported_types() {
        let db = FrankenDb::new(None).await.expect("db should open");

        let overflow_params = Array::new();
        overflow_params.push(&JsValue::bigint_from_str("9223372036854775808"));
        let overflow_error = db
            .query_with_params("SELECT ?", overflow_params.into())
            .await
            .expect_err("overflowing BigInt should be rejected");
        let overflow_message = Reflect::get(&overflow_error, &JsValue::from_str("message"))
            .expect("message field should exist")
            .as_string()
            .expect("message should be a string");
        assert!(overflow_message.contains("BigInt outside SQLite INTEGER range"));

        let unsupported_params = Array::new();
        unsupported_params.push(&Object::new().into());
        let unsupported_error = db
            .query_with_params("SELECT ?", unsupported_params.into())
            .await
            .expect_err("plain objects should be rejected");
        let unsupported_message = Reflect::get(&unsupported_error, &JsValue::from_str("message"))
            .expect("message field should exist")
            .as_string()
            .expect("message should be a string");
        assert!(unsupported_message.contains("SQLite-compatible scalar parameter"));
        #[cfg(feature = "diagnostics")]
        assert!(unsupported_message.contains("Object"));
        #[cfg(not(feature = "diagnostics"))]
        assert!(unsupported_message.contains("object"));
    }

    #[cfg(all(feature = "diagnostics", feature = "prepared-statements"))]
    #[wasm_bindgen_test]
    async fn wasm_prepare_roundtrip_uses_core_column_names() {
        let db = FrankenDb::new(None).await.expect("db should open");
        execute_seed_statements(
            &db,
            &[
                "CREATE TABLE wasm_prepared (id INTEGER PRIMARY KEY, name TEXT)",
                "INSERT INTO wasm_prepared (id, name) VALUES (1, 'alpha')",
                "INSERT INTO wasm_prepared (id, name) VALUES (2, 'beta')",
            ],
        )
        .await;

        let stmt = db
            .prepare("SELECT id AS user_id, name FROM wasm_prepared WHERE id = ?")
            .await
            .expect("statement should prepare");
        assert_eq!(
            stmt.sql(),
            "SELECT id AS user_id, name FROM wasm_prepared WHERE id = ?"
        );
        assert_eq!(stmt.column_count(), 2);

        let prepared_columns = stmt.column_names_js().unchecked_into::<Array>();
        assert_eq!(
            prepared_columns.get(0).as_string().as_deref(),
            Some("user_id")
        );
        assert_eq!(prepared_columns.get(1).as_string().as_deref(), Some("name"));

        let params = Array::new();
        params.push(&JsValue::from_f64(2.0));
        let result = stmt
            .query_with_params(params.into())
            .await
            .expect("prepared query should succeed");

        let columns = Reflect::get(&result, &JsValue::from_str("columns"))
            .expect("columns field should exist")
            .unchecked_into::<Array>();
        assert_eq!(columns.get(0).as_string().as_deref(), Some("user_id"));
        assert_eq!(columns.get(1).as_string().as_deref(), Some("name"));

        let rows = Reflect::get(&result, &JsValue::from_str("rows"))
            .expect("rows field should exist")
            .unchecked_into::<Array>();
        assert_eq!(rows.length(), 1);
        let row = rows.get(0).unchecked_into::<Object>();
        assert_eq!(
            Reflect::get(&row, &JsValue::from_str("user_id"))
                .expect("user_id field should exist")
                .as_f64(),
            Some(2.0)
        );
        assert_eq!(
            Reflect::get(&row, &JsValue::from_str("name"))
                .expect("name field should exist")
                .as_string()
                .as_deref(),
            Some("beta")
        );

        #[cfg(feature = "row-arrays")]
        {
            let row_arrays = row_arrays(&result);
            let raw_row = row_arrays.get(0).unchecked_into::<Array>();
            assert_eq!(raw_row.get(0).as_f64(), Some(2.0));
            assert_eq!(raw_row.get(1).as_string().as_deref(), Some("beta"));
        }
    }

    #[cfg(feature = "prepared-statements")]
    #[wasm_bindgen_test]
    async fn wasm_prepare_supports_sql_query_execute_without_params() {
        let db = FrankenDb::new(None).await.expect("db should open");
        execute_seed_statements(
            &db,
            &[
                "CREATE TABLE wasm_stmt_surface (id INTEGER PRIMARY KEY, name TEXT)",
                "INSERT INTO wasm_stmt_surface (id, name) VALUES (1, 'alpha')",
                "INSERT INTO wasm_stmt_surface (id, name) VALUES (2, 'beta')",
            ],
        )
        .await;

        let stmt = db
            .prepare("SELECT id, name FROM wasm_stmt_surface ORDER BY id")
            .await
            .expect("statement should prepare");
        assert_eq!(
            stmt.execute()
                .await
                .expect("execute should report visible row count"),
            2
        );

        let rows = rows(&stmt.query().await.expect("prepared query should succeed"));
        assert_eq!(rows.length(), 2);
        let first_row = rows.get(0).unchecked_into::<Object>();
        assert_eq!(row_property(&first_row, "id").as_f64(), Some(1.0));
        assert_eq!(
            row_property(&first_row, "name").as_string().as_deref(),
            Some("alpha")
        );
    }

    #[cfg(all(feature = "diagnostics", feature = "prepared-statements"))]
    #[wasm_bindgen_test]
    async fn wasm_diagnostics_explain_methods_return_program_text() {
        let db = FrankenDb::new(None).await.expect("db should open");
        execute_seed_statements(
            &db,
            &[
                "CREATE TABLE wasm_stmt_surface (id INTEGER PRIMARY KEY, name TEXT)",
                "INSERT INTO wasm_stmt_surface (id, name) VALUES (1, 'alpha')",
            ],
        )
        .await;

        let stmt = db
            .prepare("SELECT id, name FROM wasm_stmt_surface ORDER BY id")
            .await
            .expect("statement should prepare");
        let stmt_explain = stmt
            .explain()
            .await
            .expect("statement explain should succeed");
        assert!(
            !stmt_explain.trim().is_empty(),
            "statement explain output should not be empty"
        );

        let db_explain = db
            .explain("SELECT id, name FROM wasm_stmt_surface ORDER BY id")
            .await
            .expect("db explain should succeed");
        assert!(
            !db_explain.trim().is_empty(),
            "db explain output should not be empty"
        );
    }

    #[cfg(feature = "prepared-statements")]
    #[wasm_bindgen_test]
    async fn wasm_prepared_execute_with_params_inserts_rows() {
        let db = FrankenDb::new(None).await.expect("db should open");
        db.execute("CREATE TABLE wasm_stmt_insert (id INTEGER PRIMARY KEY, name TEXT)")
            .await
            .expect("table create should succeed");

        let stmt = db
            .prepare("INSERT INTO wasm_stmt_insert (id, name) VALUES (?, ?)")
            .await
            .expect("insert statement should prepare");
        let params = Array::new();
        params.push(&JsValue::from_f64(1.0));
        params.push(&JsValue::from_str("alpha"));
        assert_eq!(
            stmt.execute_with_params(params.into())
                .await
                .expect("prepared insert should report one change"),
            1
        );

        let rows = rows(
            &db.query("SELECT id, name FROM wasm_stmt_insert")
                .await
                .expect("query should succeed"),
        );
        assert_eq!(rows.length(), 1);
        let row = rows.get(0).unchecked_into::<Object>();
        assert_eq!(row_property(&row, "id").as_f64(), Some(1.0));
        assert_eq!(
            row_property(&row, "name").as_string().as_deref(),
            Some("alpha")
        );
    }

    #[wasm_bindgen_test]
    async fn wasm_value_conversion_keeps_representable_fractional_numbers_real() {
        let db = FrankenDb::new(None).await.expect("db should open");
        let number = ((1_i64 << 51) as f64) + 0.5;

        let params = Array::new();
        params.push(&JsValue::from_f64(number));
        let result = db
            .query_with_params("SELECT ? AS value", params.into())
            .await
            .expect("representable fractional JS numbers should stay REAL");
        let rows = rows(&result);
        let row = rows.get(0).unchecked_into::<Object>();
        assert_eq!(row_property(&row, "value").as_f64(), Some(number));
    }

    #[wasm_bindgen_test]
    async fn wasm_value_conversion_rejects_large_fraction_after_js_rounding() {
        let db = FrankenDb::new(None).await.expect("db should open");
        let rounded = (MAX_SAFE_INTEGER as f64) + 0.5;

        let params = Array::new();
        params.push(&JsValue::from_f64(rounded));
        let error = db
            .query_with_params("SELECT ?", params.into())
            .await
            .expect_err("rounded large JS numbers should require BigInt");
        let message = Reflect::get(&error, &JsValue::from_str("message"))
            .expect("message field should exist")
            .as_string()
            .expect("message should be a string");
        assert!(message.contains("BigInt"));
    }

    #[wasm_bindgen_test]
    async fn wasm_query_exposes_column_metadata() {
        let db = FrankenDb::new(None).await.expect("db should open");
        execute_seed_statements(
            &db,
            &[
                "CREATE TABLE wasm_meta (id INTEGER PRIMARY KEY, name TEXT)",
                "INSERT INTO wasm_meta (id, name) VALUES (1, 'alpha')",
                "INSERT INTO wasm_meta (id, name) VALUES (2, 'beta')",
            ],
        )
        .await;

        let result = db
            .query("SELECT id AS user_id, name FROM wasm_meta ORDER BY id")
            .await
            .expect("query should succeed");

        let columns = Reflect::get(&result, &JsValue::from_str("columns"))
            .expect("columns field should exist")
            .unchecked_into::<Array>();
        assert_eq!(columns.length(), 2);
        assert_eq!(columns.get(0).as_string().as_deref(), Some("user_id"));
        assert_eq!(columns.get(1).as_string().as_deref(), Some("name"));

        let column_count = Reflect::get(&result, &JsValue::from_str("columnCount"))
            .expect("columnCount field should exist")
            .as_f64()
            .expect("columnCount should be numeric");
        assert_eq!(column_count, 2.0);

        let column_types = Reflect::get(&result, &JsValue::from_str("columnTypes"))
            .expect("columnTypes field should exist")
            .unchecked_into::<Array>();
        assert_eq!(column_types.get(0).as_string().as_deref(), Some("integer"));
        assert_eq!(column_types.get(1).as_string().as_deref(), Some("text"));

        let changes = Reflect::get(&result, &JsValue::from_str("changes"))
            .expect("changes property lookup should not throw");
        #[cfg(feature = "diagnostics")]
        assert_eq!(changes.as_f64(), Some(0.0));
        #[cfg(not(feature = "diagnostics"))]
        assert!(changes.is_undefined());

        let rows = Reflect::get(&result, &JsValue::from_str("rows"))
            .expect("rows field should exist")
            .unchecked_into::<Array>();
        let first_row = rows.get(0).unchecked_into::<Object>();
        assert_eq!(
            Reflect::get(&first_row, &JsValue::from_str("user_id"))
                .expect("user_id field should exist")
                .as_f64(),
            Some(1.0)
        );
        assert_eq!(
            Reflect::get(&first_row, &JsValue::from_str("name"))
                .expect("name field should exist")
                .as_string()
                .as_deref(),
            Some("alpha")
        );
    }

    #[cfg(feature = "row-arrays")]
    #[wasm_bindgen_test]
    async fn wasm_row_arrays_feature_exposes_positional_rows() {
        let db = FrankenDb::new(None).await.expect("db should open");
        execute_seed_statements(
            &db,
            &[
                "CREATE TABLE wasm_row_arrays (id INTEGER PRIMARY KEY, name TEXT)",
                "INSERT INTO wasm_row_arrays (id, name) VALUES (1, 'alpha')",
            ],
        )
        .await;

        let result = db
            .query("SELECT id, name FROM wasm_row_arrays")
            .await
            .expect("query should succeed");
        let row_arrays = row_arrays(&result);
        assert_eq!(row_arrays.length(), 1);
        let row = row_arrays.get(0).unchecked_into::<Array>();
        assert_eq!(row.get(0).as_f64(), Some(1.0));
        assert_eq!(row.get(1).as_string().as_deref(), Some("alpha"));
    }

    #[cfg(feature = "prepared-statements")]
    #[wasm_bindgen_test]
    async fn wasm_prepared_statement_reuses_sql_with_different_params() {
        let db = FrankenDb::new(None).await.expect("db should open");
        execute_seed_statements(
            &db,
            &[
                "CREATE TABLE wasm_reuse (id INTEGER PRIMARY KEY, name TEXT)",
                "INSERT INTO wasm_reuse (id, name) VALUES (1, 'alpha')",
                "INSERT INTO wasm_reuse (id, name) VALUES (2, 'beta')",
            ],
        )
        .await;

        let stmt = db
            .prepare("SELECT name FROM wasm_reuse WHERE id = ?")
            .await
            .expect("statement should prepare");

        let first_params = Array::new();
        first_params.push(&JsValue::from_f64(1.0));
        let first_result = stmt
            .query_with_params(first_params.into())
            .await
            .expect("first prepared query should succeed");
        let first_rows = Reflect::get(&first_result, &JsValue::from_str("rows"))
            .expect("rows field should exist")
            .unchecked_into::<Array>();
        assert_eq!(first_rows.length(), 1);
        let first_row = first_rows.get(0).unchecked_into::<Object>();
        assert_eq!(
            Reflect::get(&first_row, &JsValue::from_str("name"))
                .expect("name field should exist")
                .as_string()
                .as_deref(),
            Some("alpha")
        );

        let second_params = Array::new();
        second_params.push(&JsValue::from_f64(2.0));
        let second_result = stmt
            .query_with_params(second_params.into())
            .await
            .expect("second prepared query should succeed");
        let second_rows = Reflect::get(&second_result, &JsValue::from_str("rows"))
            .expect("rows field should exist")
            .unchecked_into::<Array>();
        assert_eq!(second_rows.length(), 1);
        let second_row = second_rows.get(0).unchecked_into::<Object>();
        assert_eq!(
            Reflect::get(&second_row, &JsValue::from_str("name"))
                .expect("name field should exist")
                .as_string()
                .as_deref(),
            Some("beta")
        );
    }

    #[cfg(feature = "api-extras")]
    #[wasm_bindgen_test]
    async fn wasm_pragma_surface_returns_query_result_shape() {
        let db = FrankenDb::new(None).await.expect("db should open");
        let result = db
            .pragma("user_version")
            .await
            .expect("pragma should succeed");

        let columns = Reflect::get(&result, &JsValue::from_str("columns"))
            .expect("columns field should exist")
            .unchecked_into::<Array>();
        assert_eq!(columns.length(), 1);
        assert_eq!(columns.get(0).as_string().as_deref(), Some("user_version"));

        let rows = Reflect::get(&result, &JsValue::from_str("rows"))
            .expect("rows field should exist")
            .unchecked_into::<Array>();
        assert_eq!(rows.length(), 1);
        let row = rows.get(0).unchecked_into::<Object>();
        assert_eq!(
            Reflect::get(&row, &JsValue::from_str("user_version"))
                .expect("user_version field should exist")
                .as_f64(),
            Some(0.0)
        );
    }

    #[wasm_bindgen_test]
    async fn wasm_errors_include_core_sqlite_metadata() {
        let db = FrankenDb::new(None).await.expect("db should open");
        let error = db
            .execute("NOT VALID SQL {{{{")
            .await
            .expect_err("invalid SQL should produce a JS error");

        let code = Reflect::get(&error, &JsValue::from_str("code"))
            .expect("code field should exist")
            .as_string()
            .expect("code should be a string");
        assert_eq!(code, "SQLITE_ERROR");

        let sqlite_code = Reflect::get(&error, &JsValue::from_str("sqliteCode"))
            .expect("sqliteCode field should exist")
            .as_f64()
            .expect("sqliteCode should be numeric");
        assert_eq!(sqlite_code, 1.0);

        let extended_code = Reflect::get(&error, &JsValue::from_str("extendedCode"))
            .expect("extendedCode field should exist")
            .as_f64()
            .expect("extendedCode should be numeric");
        assert_eq!(extended_code, 1.0);

        let message = Reflect::get(&error, &JsValue::from_str("message"))
            .expect("message field should exist")
            .as_string()
            .expect("message should be a string");
        assert!(message.contains("SQL error at offset"));
        assert!(message.contains("unexpected token"));
    }

    #[wasm_bindgen_test]
    async fn wasm_out_of_memory_errors_are_structured_for_js() {
        let db = FrankenDb::new(None).await.expect("db should open");
        let error = db
            .with_connection(async |_conn| Err::<(), FrankenError>(FrankenError::OutOfMemory))
            .await
            .expect_err("out-of-memory should produce a JS error");

        let message = Reflect::get(&error, &JsValue::from_str("message"))
            .expect("message field should exist")
            .as_string()
            .expect("message should be a string");
        assert!(message.contains("FrankenSQLite WASM ran out of memory"));
        #[cfg(all(feature = "diagnostics", feature = "memory-options"))]
        assert!(message.contains("4 GiB"));
        assert_eq!(
            Reflect::get(&error, &JsValue::from_str("oom"))
                .expect("oom field should exist")
                .as_bool(),
            Some(true)
        );
    }

    #[cfg(feature = "diagnostics")]
    #[wasm_bindgen_test]
    async fn wasm_diagnostic_errors_include_recovery_metadata() {
        let db = FrankenDb::new(None).await.expect("db should open");
        let error = db
            .execute("NOT VALID SQL {{{{")
            .await
            .expect_err("invalid SQL should produce a JS error");

        let transient = Reflect::get(&error, &JsValue::from_str("transient"))
            .expect("transient field should exist")
            .as_bool()
            .expect("transient should be a bool");
        assert!(!transient);

        let user_recoverable = Reflect::get(&error, &JsValue::from_str("userRecoverable"))
            .expect("userRecoverable field should exist")
            .as_bool()
            .expect("userRecoverable should be a bool");
        assert!(user_recoverable);
    }
}
