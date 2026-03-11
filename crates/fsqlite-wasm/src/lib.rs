//! WebAssembly bindings for FrankenSQLite.
//!
//! This crate exposes a small browser-facing surface backed by
//! `fsqlite-core`'s wasm-compatible in-memory engine, while continuing to
//! re-export the parser/planner crates for lower-level integration.
//!
//! All OS-specific functionality (VFS, pager, WAL, MVCC, io_uring) is
//! excluded — those require the `native` feature on `fsqlite-types` and
//! OS-level primitives not available in `wasm32-unknown-unknown`.
//!
//! JavaScript conversion semantics currently follow the WASM 2.6 bead:
//! - `null` / `undefined` <-> `SqliteValue::Null`
//! - `INTEGER` <-> `number` when within `Number.MAX_SAFE_INTEGER`, otherwise `BigInt`
//! - `REAL` <-> `number`
//! - `TEXT` <-> `string`
//! - `BLOB` <-> `Uint8Array`
//! - `NaN` coerces to `NULL` with a browser warning
//! - `Infinity` and `-Infinity` are rejected
//! - `Date` inputs are stored as ISO 8601 `TEXT`

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Once;

use fsqlite_core::connection::{
    Connection as CoreConnection, PreparedStatement as CorePreparedStatement, Row as CoreRow,
};
use fsqlite_error::FrankenError;
use fsqlite_types::SqliteValue;
use js_sys::{Array, BigInt, Date, Number, Object, Reflect, Uint8Array};
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;

pub use fsqlite_ast as ast;
pub use fsqlite_error as error;
pub use fsqlite_func as func;
pub use fsqlite_parser as parser;
pub use fsqlite_planner as planner;
pub use fsqlite_types as types;

static WASM_RUNTIME_INIT: Once = Once::new();

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
        console_error_panic_hook::set_once();
        #[cfg(all(target_arch = "wasm32", feature = "tracing"))]
        tracing_wasm::set_as_global_default();
    });
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen(start))]
pub fn init() {
    install_wasm_runtime();
}

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
/// Query results expose `rows` as JavaScript objects keyed by column label,
/// preserve positional access in `rowArrays`, and include best-effort
/// `columnTypes` metadata. Labels use core inference when available and fall
/// back to `_cN` for unnamed expressions.
#[wasm_bindgen(js_name = FrankenDB)]
pub struct FrankenDb {
    state: Rc<FrankenDbState>,
}

struct FrankenDbState {
    path: String,
    inner: RefCell<Option<CoreConnection>>,
}

struct PreparedMetadata {
    column_count: usize,
    column_names: Vec<String>,
}

#[wasm_bindgen(js_name = FrankenPreparedStatement)]
pub struct FrankenPreparedStatement {
    state: Rc<FrankenDbState>,
    sql: String,
    column_count: usize,
    column_names: Vec<String>,
}

#[wasm_bindgen(js_class = FrankenDB)]
impl FrankenDb {
    #[wasm_bindgen(constructor)]
    pub fn new(name: Option<String>) -> Result<Self, JsValue> {
        install_wasm_runtime();
        let path = name.unwrap_or_else(|| ":memory:".to_owned());
        let conn = open_core_connection(&path).map_err(franken_error_to_js)?;
        Ok(Self {
            state: Rc::new(FrankenDbState {
                path,
                inner: RefCell::new(Some(conn)),
            }),
        })
    }

    #[wasm_bindgen(js_name = open)]
    pub fn open(name: Option<String>) -> Result<Self, JsValue> {
        Self::new(name)
    }

    #[wasm_bindgen(getter)]
    pub fn path(&self) -> String {
        self.state.path.clone()
    }

    pub fn close(&self) {
        let _ = self.state.inner.borrow_mut().take();
    }

    pub fn execute(&self, sql: &str) -> Result<usize, JsValue> {
        self.with_connection(|conn| conn.execute(sql))
    }

    #[wasm_bindgen(js_name = executeBatch)]
    pub fn execute_batch(&self, sql: &str) -> Result<(), JsValue> {
        self.with_connection(|conn| conn.execute(sql).map(|_| ()))
    }

    #[wasm_bindgen(js_name = executeWithParams)]
    pub fn execute_with_params(&self, sql: &str, params: JsValue) -> Result<usize, JsValue> {
        let params = parse_js_params(params)?;
        self.with_connection(|conn| conn.execute_with_params(sql, &params))
    }

    pub fn query(&self, sql: &str) -> Result<JsValue, JsValue> {
        self.with_connection(|conn| {
            let stmt = conn.prepare(sql)?;
            let rows = stmt.query()?;
            query_result_to_js(rows, stmt.column_names(), stmt.column_count())
        })
    }

    #[wasm_bindgen(js_name = queryWithParams)]
    pub fn query_with_params(&self, sql: &str, params: JsValue) -> Result<JsValue, JsValue> {
        let params = parse_js_params(params)?;
        self.with_connection(|conn| {
            let stmt = conn.prepare(sql)?;
            let rows = stmt.query_with_params(&params)?;
            query_result_to_js(rows, stmt.column_names(), stmt.column_count())
        })
    }

    pub fn pragma(&self, pragma: &str) -> Result<JsValue, JsValue> {
        let sql = format!("PRAGMA {pragma}");
        self.with_connection(|conn| {
            let rows = conn.query(&sql)?;
            query_result_to_js(rows, &[], 0)
        })
    }

    pub fn prepare(&self, sql: &str) -> Result<FrankenPreparedStatement, JsValue> {
        let metadata = self.with_connection(|conn| {
            let stmt = conn.prepare(sql)?;
            Ok(PreparedMetadata {
                column_count: stmt.column_count(),
                column_names: stmt.column_names().to_vec(),
            })
        })?;
        Ok(FrankenPreparedStatement {
            state: Rc::clone(&self.state),
            sql: sql.to_owned(),
            column_count: metadata.column_count,
            column_names: metadata.column_names,
        })
    }

    pub fn explain(&self, sql: &str) -> Result<String, JsValue> {
        self.with_connection(|conn| {
            let stmt = conn.prepare(sql)?;
            Ok(stmt.explain())
        })
    }
}

impl FrankenDb {
    fn with_connection<T>(
        &self,
        f: impl FnOnce(&CoreConnection) -> Result<T, FrankenError>,
    ) -> Result<T, JsValue> {
        self.state.with_connection(f)
    }
}

impl FrankenDbState {
    fn with_connection<T>(
        &self,
        f: impl FnOnce(&CoreConnection) -> Result<T, FrankenError>,
    ) -> Result<T, JsValue> {
        install_wasm_runtime();
        let borrow = self.inner.borrow();
        let conn = borrow.as_ref().ok_or_else(|| {
            franken_error_to_js(FrankenError::internal("database handle is closed"))
        })?;
        f(conn).map_err(franken_error_to_js)
    }
}

fn open_core_connection(path: &str) -> Result<CoreConnection, FrankenError> {
    CoreConnection::open(path)
}

#[wasm_bindgen(js_class = FrankenPreparedStatement)]
impl FrankenPreparedStatement {
    #[wasm_bindgen(getter)]
    pub fn sql(&self) -> String {
        self.sql.clone()
    }

    #[wasm_bindgen(getter, js_name = columnCount)]
    pub fn column_count(&self) -> usize {
        self.column_count
    }

    #[wasm_bindgen(js_name = columnNames)]
    pub fn column_names_js(&self) -> JsValue {
        let names = Array::new();
        for name in &self.column_names {
            names.push(&JsValue::from_str(name));
        }
        names.into()
    }

    pub fn execute(&self) -> Result<usize, JsValue> {
        self.with_prepared_statement(|stmt| stmt.execute())
    }

    #[wasm_bindgen(js_name = executeWithParams)]
    pub fn execute_with_params(&self, params: JsValue) -> Result<usize, JsValue> {
        let params = parse_js_params(params)?;
        self.with_prepared_statement(|stmt| stmt.execute_with_params(&params))
    }

    pub fn query(&self) -> Result<JsValue, JsValue> {
        self.with_prepared_statement(|stmt| {
            let rows = stmt.query()?;
            query_result_to_js(rows, stmt.column_names(), stmt.column_count())
        })
    }

    #[wasm_bindgen(js_name = queryWithParams)]
    pub fn query_with_params(&self, params: JsValue) -> Result<JsValue, JsValue> {
        let params = parse_js_params(params)?;
        self.with_prepared_statement(|stmt| {
            let rows = stmt.query_with_params(&params)?;
            query_result_to_js(rows, stmt.column_names(), stmt.column_count())
        })
    }

    pub fn explain(&self) -> Result<String, JsValue> {
        self.with_prepared_statement(|stmt| Ok(stmt.explain()))
    }
}

impl FrankenPreparedStatement {
    fn with_prepared_statement<T>(
        &self,
        f: impl FnOnce(&CorePreparedStatement<'_>) -> Result<T, FrankenError>,
    ) -> Result<T, JsValue> {
        self.state.with_connection(|conn| {
            let stmt = conn.prepare(&self.sql)?;
            f(&stmt)
        })
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
    let row_arrays = Array::new();
    for row in &rows {
        let row_array = row_to_js_array(row)?;
        row_arrays.push(&row_array.clone().into());
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
    set_property(&result, "rowArrays", &row_arrays.into())?;
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
        return Ok(SqliteValue::Text(text));
    }
    if let Some(boolean) = value.as_bool() {
        return Ok(SqliteValue::Integer(i64::from(boolean)));
    }
    if value.is_bigint() {
        let bigint_text = bigint_to_decimal_string(value).map_err(franken_error_to_js)?;
        return parse_bigint_sqlite_value(&bigint_text).map_err(franken_error_to_js);
    }
    if let Some(bytes) = value.dyn_ref::<Uint8Array>() {
        return Ok(SqliteValue::Blob(bytes.to_vec()));
    }
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
        SqliteValue::Blob(bytes) => Ok(Uint8Array::from(bytes.as_slice()).into()),
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
    let _ = set_property(
        &object,
        "transient",
        &JsValue::from_bool(error.is_transient()),
    );
    let _ = set_property(
        &object,
        "userRecoverable",
        &JsValue::from_bool(error.is_user_recoverable()),
    );
    if let Some(suggestion) = error.suggestion() {
        let _ = set_property(&object, "suggestion", &JsValue::from_str(suggestion));
    }
    object.into()
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
    if number.fract() == 0.0 {
        if !is_safe_integer {
            return Err(FrankenError::TypeMismatch {
                expected: "safe JavaScript integer or BigInt".to_owned(),
                actual: "unsafe JavaScript number; use BigInt for INTEGER values outside Number.MAX_SAFE_INTEGER"
                    .to_owned(),
            });
        }
        #[allow(clippy::cast_possible_truncation)]
        return Ok(SqliteValue::Integer(number as i64));
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

#[cfg(target_arch = "wasm32")]
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

#[cfg(not(target_arch = "wasm32"))]
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

fn date_to_sqlite_value(date: &Date) -> Result<SqliteValue, FrankenError> {
    let timestamp = date.get_time();
    if !timestamp.is_finite() {
        return Err(FrankenError::TypeMismatch {
            expected: "valid JavaScript Date".to_owned(),
            actual: "invalid Date".to_owned(),
        });
    }
    Ok(SqliteValue::Text(String::from(date.to_iso_string())))
}

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

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

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
        let conn = open_core_connection(":memory:").expect("in-memory connection should open");
        conn.execute("CREATE TABLE wasm_rt (id INTEGER PRIMARY KEY, name TEXT)")
            .expect("schema create should succeed");
        conn.execute("INSERT INTO wasm_rt (id, name) VALUES (1, 'alpha'), (2, 'beta')")
            .expect("seed rows should insert");

        let stmt = conn
            .prepare("SELECT id, name FROM wasm_rt ORDER BY id")
            .expect("statement should prepare");
        assert_eq!(stmt.column_count(), 2);

        let rows = stmt.query().expect("query should succeed");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(1));
        assert_eq!(rows[0].values()[1], SqliteValue::Text("alpha".to_owned()));
        assert_eq!(rows[1].values()[0], SqliteValue::Integer(2));
        assert_eq!(rows[1].values()[1], SqliteValue::Text("beta".to_owned()));
    }

    #[test]
    fn core_prepared_statement_exposes_inferred_column_names() {
        let conn = open_core_connection(":memory:").expect("in-memory connection should open");
        conn.execute("CREATE TABLE wasm_cols (id INTEGER PRIMARY KEY, name TEXT)")
            .expect("schema create should succeed");

        let stmt = conn
            .prepare("SELECT id AS user_id, name, 1 + 2 FROM wasm_cols")
            .expect("statement should prepare");

        assert_eq!(stmt.column_count(), 3);
        assert_eq!(stmt.column_names(), &["user_id", "name", "_c2"]);
    }

    #[test]
    fn franken_db_prepare_and_execute_batch_work_on_host() {
        let db = FrankenDb::new(None).expect("db should open");
        db.execute_batch(
            "CREATE TABLE wasm_batch (id INTEGER PRIMARY KEY, name TEXT);\
             INSERT INTO wasm_batch (id, name) VALUES (1, 'alpha');\
             INSERT INTO wasm_batch (id, name) VALUES (2, 'beta');",
        )
        .expect("batch execution should succeed");

        let stmt = db
            .prepare("SELECT id AS user_id, name FROM wasm_batch ORDER BY id")
            .expect("select should prepare");
        assert_eq!(stmt.column_count(), 2);
        assert_eq!(stmt.execute().expect("select execute should count rows"), 2);
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
mod wasm_tests {
    use super::*;
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);

    #[wasm_bindgen_test]
    fn wasm_db_roundtrip() {
        let db = FrankenDb::new(None).expect("db should open");
        db.execute("CREATE TABLE wasm_t (id INTEGER PRIMARY KEY, name TEXT)")
            .expect("table create should succeed");
        db.execute("INSERT INTO wasm_t (id, name) VALUES (1, 'alpha'), (2, 'beta')")
            .expect("seed insert should succeed");

        let result = db
            .query("SELECT id, name FROM wasm_t ORDER BY id")
            .expect("query should succeed");
        let rows = Reflect::get(&result, &JsValue::from_str("rows"))
            .expect("rows field should exist")
            .unchecked_into::<Array>();

        assert_eq!(rows.length(), 2);
    }

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
    fn wasm_value_conversion_round_trips_with_type_fidelity() {
        let db = FrankenDb::new(None).expect("db should open");
        db.execute(
            "CREATE TABLE wasm_types (
                safe_i INTEGER,
                big_i INTEGER,
                real_v REAL,
                text_v TEXT,
                blob_v BLOB,
                null_v,
                date_v TEXT
            )",
        )
        .expect("table create should succeed");

        let params = Array::new();
        params.push(&JsValue::from_f64(42.0));
        params.push(&JsValue::bigint_from_str("9007199254740992"));
        params.push(&JsValue::from_f64(3.5));
        params.push(&JsValue::from_str("hello"));
        let input_blob = Uint8Array::from([0xDE_u8, 0xAD, 0xBE, 0xEF].as_slice());
        params.push(&input_blob.clone().into());
        params.push(&JsValue::NULL);
        let input_date = Date::new(&JsValue::from_str("2026-03-11T12:34:56.000Z"));
        let expected_iso = String::from(input_date.to_iso_string());
        params.push(&input_date.into());

        db.execute_with_params(
            "INSERT INTO wasm_types VALUES (?, ?, ?, ?, ?, ?, ?)",
            params.into(),
        )
        .expect("parameterized insert should succeed");

        let result = db
            .query("SELECT safe_i, big_i, real_v, text_v, blob_v, null_v, date_v FROM wasm_types")
            .expect("query should succeed");
        let rows = Reflect::get(&result, &JsValue::from_str("rowArrays"))
            .expect("rowArrays field should exist")
            .unchecked_into::<Array>();
        assert_eq!(rows.length(), 1);

        let row = rows.get(0).unchecked_into::<Array>();
        assert_eq!(row.get(0).as_f64(), Some(42.0));
        assert!(
            row.get(1).is_bigint(),
            "large INTEGER should surface as BigInt"
        );
        let roundtrip_bigint =
            BigInt::new(&row.get(1)).expect("returned large integer should be a BigInt");
        assert_eq!(
            String::from(
                roundtrip_bigint
                    .to_string(10)
                    .expect("returned BigInt should format")
            ),
            "9007199254740992"
        );
        assert_eq!(row.get(2).as_f64(), Some(3.5));
        assert_eq!(row.get(3).as_string().as_deref(), Some("hello"));

        let blob = Uint8Array::new(&row.get(4));
        assert_eq!(blob.to_vec(), vec![0xDE, 0xAD, 0xBE, 0xEF]);

        assert!(row.get(5).is_null(), "NULL should remain null in JS");
        assert_eq!(
            row.get(6).as_string().as_deref(),
            Some(expected_iso.as_str())
        );
    }

    #[wasm_bindgen_test]
    fn wasm_value_conversion_reports_overflow_and_unsupported_types() {
        let db = FrankenDb::new(None).expect("db should open");

        let overflow_params = Array::new();
        overflow_params.push(&JsValue::bigint_from_str("9223372036854775808"));
        let overflow_error = db
            .query_with_params("SELECT ?", overflow_params.into())
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
            .expect_err("plain objects should be rejected");
        let unsupported_message = Reflect::get(&unsupported_error, &JsValue::from_str("message"))
            .expect("message field should exist")
            .as_string()
            .expect("message should be a string");
        assert!(unsupported_message.contains("SQLite-compatible scalar parameter"));
        assert!(unsupported_message.contains("Object"));
    }

    #[wasm_bindgen_test]
    fn wasm_prepare_roundtrip_uses_core_column_names() {
        let db = FrankenDb::new(None).expect("db should open");
        db.execute_batch(
            "CREATE TABLE wasm_prepared (id INTEGER PRIMARY KEY, name TEXT);\
             INSERT INTO wasm_prepared (id, name) VALUES (1, 'alpha');\
             INSERT INTO wasm_prepared (id, name) VALUES (2, 'beta');",
        )
        .expect("batch execution should succeed");

        let stmt = db
            .prepare("SELECT id AS user_id, name FROM wasm_prepared WHERE id = ?")
            .expect("statement should prepare");
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

        let row_arrays = Reflect::get(&result, &JsValue::from_str("rowArrays"))
            .expect("rowArrays field should exist")
            .unchecked_into::<Array>();
        let raw_row = row_arrays.get(0).unchecked_into::<Array>();
        assert_eq!(raw_row.get(0).as_f64(), Some(2.0));
        assert_eq!(raw_row.get(1).as_string().as_deref(), Some("beta"));
    }

    #[wasm_bindgen_test]
    fn wasm_query_exposes_column_metadata() {
        let db = FrankenDb::new(None).expect("db should open");
        db.execute_batch(
            "CREATE TABLE wasm_meta (id INTEGER PRIMARY KEY, name TEXT);\
             INSERT INTO wasm_meta (id, name) VALUES (1, 'alpha');\
             INSERT INTO wasm_meta (id, name) VALUES (2, 'beta');",
        )
        .expect("batch execution should succeed");

        let result = db
            .query("SELECT id AS user_id, name FROM wasm_meta ORDER BY id")
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

    #[wasm_bindgen_test]
    fn wasm_prepared_statement_reuses_sql_with_different_params() {
        let db = FrankenDb::new(None).expect("db should open");
        db.execute_batch(
            "CREATE TABLE wasm_reuse (id INTEGER PRIMARY KEY, name TEXT);\
             INSERT INTO wasm_reuse (id, name) VALUES (1, 'alpha');\
             INSERT INTO wasm_reuse (id, name) VALUES (2, 'beta');",
        )
        .expect("batch execution should succeed");

        let stmt = db
            .prepare("SELECT name FROM wasm_reuse WHERE id = ?")
            .expect("statement should prepare");

        let first_params = Array::new();
        first_params.push(&JsValue::from_f64(1.0));
        let first_result = stmt
            .query_with_params(first_params.into())
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

    #[wasm_bindgen_test]
    fn wasm_pragma_surface_returns_query_result_shape() {
        let db = FrankenDb::new(None).expect("db should open");
        let result = db.pragma("user_version").expect("pragma should succeed");

        let columns = Reflect::get(&result, &JsValue::from_str("columns"))
            .expect("columns field should exist")
            .unchecked_into::<Array>();
        assert_eq!(columns.length(), 1);
        assert_eq!(columns.get(0).as_string().as_deref(), Some("_c0"));

        let rows = Reflect::get(&result, &JsValue::from_str("rows"))
            .expect("rows field should exist")
            .unchecked_into::<Array>();
        assert_eq!(rows.length(), 1);
        let row = rows.get(0).unchecked_into::<Object>();
        assert_eq!(
            Reflect::get(&row, &JsValue::from_str("_c0"))
                .expect("_c0 field should exist")
                .as_f64(),
            Some(0.0)
        );
    }

    #[wasm_bindgen_test]
    fn wasm_errors_include_sqlite_metadata() {
        let db = FrankenDb::new(None).expect("db should open");
        let error = db
            .execute("NOT VALID SQL {{{{")
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

        let message = Reflect::get(&error, &JsValue::from_str("message"))
            .expect("message field should exist")
            .as_string()
            .expect("message should be a string");
        assert!(message.contains("syntax error"));
    }
}
