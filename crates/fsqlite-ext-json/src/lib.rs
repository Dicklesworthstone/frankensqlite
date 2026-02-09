//! JSON1 foundations for `fsqlite-ext-json` (`bd-3cvl`).
//!
//! This module currently provides:
//! - JSON validation/minification (`json`, `json_valid`)
//! - JSONB encode/decode helpers (`jsonb`, `jsonb_*`, `json_valid` JSONB flags)
//! - JSON type inspection (`json_type`)
//! - JSON path extraction with SQLite-like single vs multi-path semantics (`json_extract`)
//! - JSON value constructors and aggregates (`json_quote`, `json_array`, `json_object`,
//!   `json_group_array`, `json_group_object`)
//! - mutators (`json_set`, `json_insert`, `json_replace`, `json_remove`, `json_patch`)
//! - formatting and diagnostics (`json_pretty`, `json_error_position`, `json_array_length`)
//!
//! Path support in this slice:
//! - `$` root
//! - `$.key` object member
//! - `$[N]` array index
//! - `$[#-N]` reverse array index

use fsqlite_error::{FrankenError, Result};
use fsqlite_func::{ColumnContext, IndexInfo, VirtualTable, VirtualTableCursor};
use fsqlite_types::{SqliteValue, cx::Cx};
use serde_json::{Map, Number, Value};

const JSON_VALID_DEFAULT_FLAGS: u8 = 0x01;
const JSON_VALID_RFC_8259_FLAG: u8 = 0x01;
const JSON_VALID_JSON5_FLAG: u8 = 0x02;
const JSON_VALID_JSONB_SUPERFICIAL_FLAG: u8 = 0x04;
const JSON_VALID_JSONB_STRICT_FLAG: u8 = 0x08;
const JSON_PRETTY_DEFAULT_INDENT_WIDTH: usize = 4;

const JSONB_NULL_TYPE: u8 = 0x0;
const JSONB_TRUE_TYPE: u8 = 0x1;
const JSONB_FALSE_TYPE: u8 = 0x2;
const JSONB_INT_TYPE: u8 = 0x3;
const JSONB_FLOAT_TYPE: u8 = 0x5;
const JSONB_TEXT_TYPE: u8 = 0x7;
const JSONB_TEXT_JSON_TYPE: u8 = 0x8;
const JSONB_ARRAY_TYPE: u8 = 0xB;
const JSONB_OBJECT_TYPE: u8 = 0xC;

#[derive(Debug, Clone, PartialEq, Eq)]
enum PathSegment {
    Key(String),
    Index(usize),
    FromEnd(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EditMode {
    Set,
    Insert,
    Replace,
}

/// Parse and minify JSON text.
///
/// Returns a canonical minified JSON string or a `FunctionError` if invalid.
pub fn json(input: &str) -> Result<String> {
    let value = parse_json_text(input)?;
    serde_json::to_string(&value)
        .map_err(|error| FrankenError::function_error(format!("json serialize failed: {error}")))
}

/// Validate JSON text under flags compatible with SQLite `json_valid`.
///
/// Supported flags:
/// - `0x01`: strict RFC-8259 JSON text
/// - `0x02`: JSON5 text
/// - `0x04`: superficial JSONB check
/// - `0x08`: strict JSONB parse
#[must_use]
pub fn json_valid(input: &str, flags: Option<u8>) -> i64 {
    json_valid_blob(input.as_bytes(), flags)
}

/// Validate binary JSONB payloads and/or JSON text (when UTF-8).
#[must_use]
pub fn json_valid_blob(input: &[u8], flags: Option<u8>) -> i64 {
    let effective_flags = flags.unwrap_or(JSON_VALID_DEFAULT_FLAGS);
    if effective_flags == 0 {
        return 0;
    }

    let allow_json = effective_flags & JSON_VALID_RFC_8259_FLAG != 0;
    let allow_json5 = effective_flags & JSON_VALID_JSON5_FLAG != 0;
    let allow_jsonb_superficial = effective_flags & JSON_VALID_JSONB_SUPERFICIAL_FLAG != 0;
    let allow_jsonb_strict = effective_flags & JSON_VALID_JSONB_STRICT_FLAG != 0;

    if allow_json || allow_json5 {
        if let Ok(text) = std::str::from_utf8(input) {
            if allow_json && parse_json_text(text).is_ok() {
                return 1;
            }
            if allow_json5 && parse_json5_text(text).is_ok() {
                return 1;
            }
        }
    }

    if allow_jsonb_strict && decode_jsonb_root(input).is_ok() {
        return 1;
    }
    if allow_jsonb_superficial && is_superficially_valid_jsonb(input) {
        return 1;
    }

    0
}

/// Convert JSON text into JSONB bytes.
pub fn jsonb(input: &str) -> Result<Vec<u8>> {
    let value = parse_json_text(input)?;
    encode_jsonb_root(&value)
}

/// Convert JSONB bytes back into minified JSON text.
pub fn json_from_jsonb(input: &[u8]) -> Result<String> {
    let value = decode_jsonb_root(input)?;
    serde_json::to_string(&value).map_err(|error| {
        FrankenError::function_error(format!("json_from_jsonb encode failed: {error}"))
    })
}

/// Return JSON type name at the root or an optional path.
///
/// Returns `None` when the path does not resolve.
pub fn json_type(input: &str, path: Option<&str>) -> Result<Option<&'static str>> {
    let root = parse_json_text(input)?;
    let target = match path {
        Some(path_expr) => resolve_path(&root, path_expr)?,
        None => Some(&root),
    };
    Ok(target.map(json_type_name))
}

/// Extract JSON value(s) by path, following SQLite single vs multi-path behavior.
///
/// - One path: return SQL-native value (text unwrapped, number typed, JSON null -> SQL NULL)
/// - Multiple paths: return JSON array text of extracted values (missing paths become `null`)
pub fn json_extract(input: &str, paths: &[&str]) -> Result<SqliteValue> {
    if paths.is_empty() {
        return Err(FrankenError::function_error(
            "json_extract requires at least one path",
        ));
    }

    let root = parse_json_text(input)?;

    if paths.len() == 1 {
        let selected = resolve_path(&root, paths[0])?;
        return Ok(selected.map_or(SqliteValue::Null, json_to_sqlite_scalar));
    }

    let mut out = Vec::with_capacity(paths.len());
    for path_expr in paths {
        let selected = resolve_path(&root, path_expr)?;
        out.push(selected.cloned().unwrap_or(Value::Null));
    }

    let encoded = serde_json::to_string(&Value::Array(out)).map_err(|error| {
        FrankenError::function_error(format!("json_extract array encode failed: {error}"))
    })?;
    Ok(SqliteValue::Text(encoded))
}

/// JSONB variant of `json_extract`.
///
/// The extracted JSON subtree is always returned as JSONB bytes.
pub fn jsonb_extract(input: &str, paths: &[&str]) -> Result<Vec<u8>> {
    if paths.is_empty() {
        return Err(FrankenError::function_error(
            "jsonb_extract requires at least one path",
        ));
    }

    let root = parse_json_text(input)?;
    let output = if paths.len() == 1 {
        resolve_path(&root, paths[0])?
            .cloned()
            .unwrap_or(Value::Null)
    } else {
        let mut values = Vec::with_capacity(paths.len());
        for path_expr in paths {
            values.push(
                resolve_path(&root, path_expr)?
                    .cloned()
                    .unwrap_or(Value::Null),
            );
        }
        Value::Array(values)
    };

    encode_jsonb_root(&output)
}

/// Extract with `->` semantics: always returns JSON text for the selected node.
///
/// Missing paths yield SQL NULL.
pub fn json_arrow(input: &str, path: &str) -> Result<SqliteValue> {
    let root = parse_json_text(input)?;
    let selected = resolve_path(&root, path)?;
    let Some(value) = selected else {
        return Ok(SqliteValue::Null);
    };
    let encoded = serde_json::to_string(value).map_err(|error| {
        FrankenError::function_error(format!("json_arrow encode failed: {error}"))
    })?;
    Ok(SqliteValue::Text(encoded))
}

/// Extract with `->>` semantics: returns SQL-native value.
pub fn json_double_arrow(input: &str, path: &str) -> Result<SqliteValue> {
    json_extract(input, &[path])
}

/// Return the array length at root or path, or `None` when target is not an array.
pub fn json_array_length(input: &str, path: Option<&str>) -> Result<Option<usize>> {
    let root = parse_json_text(input)?;
    let target = match path {
        Some(path_expr) => resolve_path(&root, path_expr)?,
        None => Some(&root),
    };
    Ok(target.and_then(Value::as_array).map(Vec::len))
}

/// Return 0 for valid JSON, otherwise a 1-based position for first parse error.
#[must_use]
pub fn json_error_position(input: &str) -> usize {
    match serde_json::from_str::<Value>(input) {
        Ok(_) => 0,
        Err(error) => {
            let line = error.line();
            let column = error.column();
            if line == 0 || column == 0 {
                return 1;
            }

            let mut current_line = 1usize;
            let mut current_col = 1usize;
            for (idx, ch) in input.char_indices() {
                if current_line == line && current_col == column {
                    return idx + 1;
                }
                if ch == '\n' {
                    current_line += 1;
                    current_col = 1;
                } else {
                    current_col += 1;
                }
            }
            input.len().saturating_add(1)
        }
    }
}

/// Pretty-print JSON with default 4-space indentation or custom indent token.
pub fn json_pretty(input: &str, indent: Option<&str>) -> Result<String> {
    let root = parse_json_text(input)?;
    let indent_unit = match indent {
        Some(indent) => indent.to_owned(),
        None => " ".repeat(JSON_PRETTY_DEFAULT_INDENT_WIDTH),
    };
    let mut out = String::new();
    write_pretty_value(&root, &indent_unit, 0, &mut out)?;
    Ok(out)
}

/// Quote a SQL value as JSON.
#[must_use]
pub fn json_quote(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "null".to_owned(),
        SqliteValue::Integer(i) => i.to_string(),
        SqliteValue::Float(f) => {
            if f.is_finite() {
                format!("{f}")
            } else {
                "null".to_owned()
            }
        }
        SqliteValue::Text(text) => {
            serde_json::to_string(text).unwrap_or_else(|_| "\"\"".to_owned())
        }
        SqliteValue::Blob(bytes) => {
            let mut hex = String::with_capacity(bytes.len() * 2);
            for byte in bytes {
                use std::fmt::Write;
                let _ = write!(hex, "{byte:02x}");
            }
            serde_json::to_string(&hex).unwrap_or_else(|_| "\"\"".to_owned())
        }
    }
}

/// Build a JSON array from SQL values.
pub fn json_array(values: &[SqliteValue]) -> Result<String> {
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        out.push(sqlite_to_json(value)?);
    }
    serde_json::to_string(&Value::Array(out))
        .map_err(|error| FrankenError::function_error(format!("json_array encode failed: {error}")))
}

/// Build a JSON object from alternating key/value SQL arguments.
///
/// Duplicate keys are overwritten by later entries.
pub fn json_object(args: &[SqliteValue]) -> Result<String> {
    if args.len() % 2 != 0 {
        return Err(FrankenError::function_error(
            "json_object requires an even number of arguments",
        ));
    }

    let mut map = Map::with_capacity(args.len() / 2);
    let mut idx = 0;
    while idx < args.len() {
        let key = match &args[idx] {
            SqliteValue::Text(text) => text.clone(),
            _ => {
                return Err(FrankenError::function_error(
                    "json_object keys must be text",
                ));
            }
        };
        let value = sqlite_to_json(&args[idx + 1])?;
        map.insert(key, value);
        idx += 2;
    }

    serde_json::to_string(&Value::Object(map)).map_err(|error| {
        FrankenError::function_error(format!("json_object encode failed: {error}"))
    })
}

/// Build JSONB from SQL values.
pub fn jsonb_array(values: &[SqliteValue]) -> Result<Vec<u8>> {
    let json_text = json_array(values)?;
    jsonb(&json_text)
}

/// Build JSONB object from alternating key/value SQL arguments.
pub fn jsonb_object(args: &[SqliteValue]) -> Result<Vec<u8>> {
    let json_text = json_object(args)?;
    jsonb(&json_text)
}

/// Aggregate rows into a JSON array, preserving SQL NULL as JSON null.
pub fn json_group_array(values: &[SqliteValue]) -> Result<String> {
    json_array(values)
}

/// JSONB variant of `json_group_array`.
pub fn jsonb_group_array(values: &[SqliteValue]) -> Result<Vec<u8>> {
    let json_text = json_group_array(values)?;
    jsonb(&json_text)
}

/// Aggregate key/value pairs into a JSON object.
///
/// Duplicate keys keep the last value.
pub fn json_group_object(entries: &[(SqliteValue, SqliteValue)]) -> Result<String> {
    let mut map = Map::with_capacity(entries.len());
    for (key_value, value) in entries {
        let key = match key_value {
            SqliteValue::Text(text) => text.clone(),
            _ => {
                return Err(FrankenError::function_error(
                    "json_group_object keys must be text",
                ));
            }
        };
        map.insert(key, sqlite_to_json(value)?);
    }
    serde_json::to_string(&Value::Object(map)).map_err(|error| {
        FrankenError::function_error(format!("json_group_object encode failed: {error}"))
    })
}

/// JSONB variant of `json_group_object`.
pub fn jsonb_group_object(entries: &[(SqliteValue, SqliteValue)]) -> Result<Vec<u8>> {
    let json_text = json_group_object(entries)?;
    jsonb(&json_text)
}

/// Set JSON values at path(s), creating object keys when missing.
pub fn json_set(input: &str, pairs: &[(&str, SqliteValue)]) -> Result<String> {
    edit_json_paths(input, pairs, EditMode::Set)
}

/// JSONB variant of `json_set`.
pub fn jsonb_set(input: &str, pairs: &[(&str, SqliteValue)]) -> Result<Vec<u8>> {
    let json_text = json_set(input, pairs)?;
    jsonb(&json_text)
}

/// Insert JSON values at path(s) only when path does not already exist.
pub fn json_insert(input: &str, pairs: &[(&str, SqliteValue)]) -> Result<String> {
    edit_json_paths(input, pairs, EditMode::Insert)
}

/// JSONB variant of `json_insert`.
pub fn jsonb_insert(input: &str, pairs: &[(&str, SqliteValue)]) -> Result<Vec<u8>> {
    let json_text = json_insert(input, pairs)?;
    jsonb(&json_text)
}

/// Replace JSON values at path(s) only when path already exists.
pub fn json_replace(input: &str, pairs: &[(&str, SqliteValue)]) -> Result<String> {
    edit_json_paths(input, pairs, EditMode::Replace)
}

/// JSONB variant of `json_replace`.
pub fn jsonb_replace(input: &str, pairs: &[(&str, SqliteValue)]) -> Result<Vec<u8>> {
    let json_text = json_replace(input, pairs)?;
    jsonb(&json_text)
}

/// Remove JSON values at path(s). Array removals compact the array.
pub fn json_remove(input: &str, paths: &[&str]) -> Result<String> {
    let mut root = parse_json_text(input)?;
    for path in paths {
        let segments = parse_path(path)?;
        remove_at_path(&mut root, &segments);
    }
    serde_json::to_string(&root).map_err(|error| {
        FrankenError::function_error(format!("json_remove encode failed: {error}"))
    })
}

/// JSONB variant of `json_remove`.
pub fn jsonb_remove(input: &str, paths: &[&str]) -> Result<Vec<u8>> {
    let json_text = json_remove(input, paths)?;
    jsonb(&json_text)
}

/// Apply RFC 7396 JSON Merge Patch.
pub fn json_patch(input: &str, patch: &str) -> Result<String> {
    let root = parse_json_text(input)?;
    let patch_value = parse_json_text(patch)?;
    let merged = merge_patch(root, patch_value);
    serde_json::to_string(&merged)
        .map_err(|error| FrankenError::function_error(format!("json_patch encode failed: {error}")))
}

/// JSONB variant of `json_patch`.
pub fn jsonb_patch(input: &str, patch: &str) -> Result<Vec<u8>> {
    let json_text = json_patch(input, patch)?;
    jsonb(&json_text)
}

/// Row shape produced by `json_each` and `json_tree`.
#[derive(Debug, Clone, PartialEq)]
pub struct JsonTableRow {
    /// Object key, array index, or NULL (root/scalar).
    pub key: SqliteValue,
    /// Value column: scalars are SQL-native, objects/arrays are JSON text.
    pub value: SqliteValue,
    /// One of: null, true, false, integer, real, text, array, object.
    pub type_name: &'static str,
    /// Scalar atom or NULL for arrays/objects.
    pub atom: SqliteValue,
    /// Stable row identifier within the result set.
    pub id: i64,
    /// Parent row id (NULL at root/top-level).
    pub parent: SqliteValue,
    /// Absolute JSON path for this row.
    pub fullkey: String,
    /// Parent container path (or same as fullkey for root/scalar rows).
    pub path: String,
}

/// Table-valued `json_each`: iterate immediate children at root or `path`.
pub fn json_each(input: &str, path: Option<&str>) -> Result<Vec<JsonTableRow>> {
    let root = parse_json_text(input)?;
    let base_path = path.unwrap_or("$");
    let target = match path {
        Some(path_expr) => resolve_path(&root, path_expr)?,
        None => Some(&root),
    };
    let Some(target) = target else {
        return Ok(Vec::new());
    };

    let mut rows = Vec::new();
    let mut next_id = 1_i64;

    match target {
        Value::Array(array) => {
            for (index, item) in array.iter().enumerate() {
                let index_i64 = i64::try_from(index).map_err(|error| {
                    FrankenError::function_error(format!("json_each index overflow: {error}"))
                })?;
                let fullkey = append_array_path(base_path, index);
                rows.push(JsonTableRow {
                    key: SqliteValue::Integer(index_i64),
                    value: json_value_column(item)?,
                    type_name: json_type_name(item),
                    atom: json_atom_column(item),
                    id: next_id,
                    parent: SqliteValue::Null,
                    fullkey,
                    path: base_path.to_owned(),
                });
                next_id += 1;
            }
        }
        Value::Object(object) => {
            for (key, item) in object {
                let fullkey = append_object_path(base_path, key);
                rows.push(JsonTableRow {
                    key: SqliteValue::Text(key.clone()),
                    value: json_value_column(item)?,
                    type_name: json_type_name(item),
                    atom: json_atom_column(item),
                    id: next_id,
                    parent: SqliteValue::Null,
                    fullkey,
                    path: base_path.to_owned(),
                });
                next_id += 1;
            }
        }
        scalar => {
            rows.push(JsonTableRow {
                key: SqliteValue::Null,
                value: json_value_column(scalar)?,
                type_name: json_type_name(scalar),
                atom: json_atom_column(scalar),
                id: next_id,
                parent: SqliteValue::Null,
                fullkey: base_path.to_owned(),
                path: base_path.to_owned(),
            });
        }
    }

    Ok(rows)
}

/// Table-valued `json_tree`: recursively iterate subtree at root or `path`.
pub fn json_tree(input: &str, path: Option<&str>) -> Result<Vec<JsonTableRow>> {
    let root = parse_json_text(input)?;
    let base_path = path.unwrap_or("$");
    let target = match path {
        Some(path_expr) => resolve_path(&root, path_expr)?,
        None => Some(&root),
    };
    let Some(target) = target else {
        return Ok(Vec::new());
    };

    let mut rows = Vec::new();
    let mut next_id = 1_i64;
    append_tree_rows(
        &mut rows,
        target,
        SqliteValue::Null,
        None,
        base_path,
        base_path,
        &mut next_id,
    )?;
    Ok(rows)
}

/// Virtual table module for `json_each`.
pub struct JsonEachVtab;

/// Cursor for `json_each` virtual table scans.
#[derive(Default)]
pub struct JsonEachCursor {
    rows: Vec<JsonTableRow>,
    pos: usize,
}

impl VirtualTable for JsonEachVtab {
    type Cursor = JsonEachCursor;

    fn connect(_cx: &Cx, _args: &[&str]) -> Result<Self> {
        Ok(Self)
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
        info.estimated_cost = 100.0;
        info.estimated_rows = 100;
        Ok(())
    }

    fn open(&self) -> Result<Self::Cursor> {
        Ok(JsonEachCursor::default())
    }
}

impl VirtualTableCursor for JsonEachCursor {
    fn filter(
        &mut self,
        _cx: &Cx,
        _idx_num: i32,
        _idx_str: Option<&str>,
        args: &[SqliteValue],
    ) -> Result<()> {
        let (input, path) = parse_json_table_filter_args(args)?;
        self.rows = json_each(input, path)?;
        self.pos = 0;
        Ok(())
    }

    fn next(&mut self, _cx: &Cx) -> Result<()> {
        if self.pos < self.rows.len() {
            self.pos += 1;
        }
        Ok(())
    }

    fn eof(&self) -> bool {
        self.pos >= self.rows.len()
    }

    fn column(&self, ctx: &mut ColumnContext, col: i32) -> Result<()> {
        let row = self.rows.get(self.pos).ok_or_else(|| {
            FrankenError::function_error("json_each cursor is out of bounds for column read")
        })?;
        write_json_table_column(row, ctx, col)
    }

    fn rowid(&self) -> Result<i64> {
        self.rows.get(self.pos).map(|row| row.id).ok_or_else(|| {
            FrankenError::function_error("json_each cursor is out of bounds for rowid")
        })
    }
}

/// Virtual table module for `json_tree`.
pub struct JsonTreeVtab;

/// Cursor for `json_tree` virtual table scans.
#[derive(Default)]
pub struct JsonTreeCursor {
    rows: Vec<JsonTableRow>,
    pos: usize,
}

impl VirtualTable for JsonTreeVtab {
    type Cursor = JsonTreeCursor;

    fn connect(_cx: &Cx, _args: &[&str]) -> Result<Self> {
        Ok(Self)
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
        info.estimated_cost = 200.0;
        info.estimated_rows = 1_000;
        Ok(())
    }

    fn open(&self) -> Result<Self::Cursor> {
        Ok(JsonTreeCursor::default())
    }
}

impl VirtualTableCursor for JsonTreeCursor {
    fn filter(
        &mut self,
        _cx: &Cx,
        _idx_num: i32,
        _idx_str: Option<&str>,
        args: &[SqliteValue],
    ) -> Result<()> {
        let (input, path) = parse_json_table_filter_args(args)?;
        self.rows = json_tree(input, path)?;
        self.pos = 0;
        Ok(())
    }

    fn next(&mut self, _cx: &Cx) -> Result<()> {
        if self.pos < self.rows.len() {
            self.pos += 1;
        }
        Ok(())
    }

    fn eof(&self) -> bool {
        self.pos >= self.rows.len()
    }

    fn column(&self, ctx: &mut ColumnContext, col: i32) -> Result<()> {
        let row = self.rows.get(self.pos).ok_or_else(|| {
            FrankenError::function_error("json_tree cursor is out of bounds for column read")
        })?;
        write_json_table_column(row, ctx, col)
    }

    fn rowid(&self) -> Result<i64> {
        self.rows.get(self.pos).map(|row| row.id).ok_or_else(|| {
            FrankenError::function_error("json_tree cursor is out of bounds for rowid")
        })
    }
}

fn parse_json_text(input: &str) -> Result<Value> {
    serde_json::from_str::<Value>(input)
        .map_err(|error| FrankenError::function_error(format!("invalid JSON input: {error}")))
}

fn parse_json5_text(input: &str) -> Result<Value> {
    json5::from_str::<Value>(input)
        .map_err(|error| FrankenError::function_error(format!("invalid JSON5 input: {error}")))
}

fn encode_jsonb_root(value: &Value) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    encode_jsonb_value(value, &mut out)?;
    Ok(out)
}

fn encode_jsonb_value(value: &Value, out: &mut Vec<u8>) -> Result<()> {
    match value {
        Value::Null => append_jsonb_node(JSONB_NULL_TYPE, &[], out),
        Value::Bool(true) => append_jsonb_node(JSONB_TRUE_TYPE, &[], out),
        Value::Bool(false) => append_jsonb_node(JSONB_FALSE_TYPE, &[], out),
        Value::Number(number) => {
            if let Some(i) = number.as_i64() {
                append_jsonb_node(JSONB_INT_TYPE, &i.to_be_bytes(), out)
            } else if let Some(u) = number.as_u64() {
                if let Ok(i) = i64::try_from(u) {
                    append_jsonb_node(JSONB_INT_TYPE, &i.to_be_bytes(), out)
                } else {
                    let float = u as f64;
                    append_jsonb_node(JSONB_FLOAT_TYPE, &float.to_bits().to_be_bytes(), out)
                }
            } else {
                let float = number.as_f64().ok_or_else(|| {
                    FrankenError::function_error("failed to encode non-finite JSON number")
                })?;
                append_jsonb_node(JSONB_FLOAT_TYPE, &float.to_bits().to_be_bytes(), out)
            }
        }
        Value::String(text) => append_jsonb_node(JSONB_TEXT_TYPE, text.as_bytes(), out),
        Value::Array(array) => {
            let mut payload = Vec::new();
            for item in array {
                encode_jsonb_value(item, &mut payload)?;
            }
            append_jsonb_node(JSONB_ARRAY_TYPE, &payload, out)
        }
        Value::Object(object) => {
            let mut payload = Vec::new();
            for (key, item) in object {
                append_jsonb_node(JSONB_TEXT_JSON_TYPE, key.as_bytes(), &mut payload)?;
                encode_jsonb_value(item, &mut payload)?;
            }
            append_jsonb_node(JSONB_OBJECT_TYPE, &payload, out)
        }
    }
}

fn append_jsonb_node(node_type: u8, payload: &[u8], out: &mut Vec<u8>) -> Result<()> {
    let (len_size, len_bytes) = encode_jsonb_payload_len(payload.len())?;
    let len_size_u8 = u8::try_from(len_size).map_err(|error| {
        FrankenError::function_error(format!("jsonb length-size conversion failed: {error}"))
    })?;
    let header = (node_type << 4) | len_size_u8;
    out.push(header);
    out.extend_from_slice(&len_bytes[..len_size]);
    out.extend_from_slice(payload);
    Ok(())
}

fn encode_jsonb_payload_len(payload_len: usize) -> Result<(usize, [u8; 8])> {
    if payload_len == 0 {
        return Ok((0, [0; 8]));
    }

    let payload_u64 = u64::try_from(payload_len).map_err(|error| {
        FrankenError::function_error(format!("jsonb payload too large: {error}"))
    })?;
    let len_size = if u8::try_from(payload_u64).is_ok() {
        1
    } else if u16::try_from(payload_u64).is_ok() {
        2
    } else if u32::try_from(payload_u64).is_ok() {
        4
    } else {
        8
    };

    let raw = payload_u64.to_be_bytes();
    let mut out = [0u8; 8];
    out[..len_size].copy_from_slice(&raw[8 - len_size..]);
    Ok((len_size, out))
}

fn decode_jsonb_root(input: &[u8]) -> Result<Value> {
    let (value, consumed) = decode_jsonb_value(input)?;
    if consumed != input.len() {
        return Err(FrankenError::function_error(
            "invalid JSONB: trailing bytes",
        ));
    }
    Ok(value)
}

fn decode_jsonb_value(input: &[u8]) -> Result<(Value, usize)> {
    let (node_type, payload, consumed) = decode_jsonb_node(input)?;
    let value = match node_type {
        JSONB_NULL_TYPE => {
            if !payload.is_empty() {
                return Err(FrankenError::function_error("invalid JSONB null payload"));
            }
            Value::Null
        }
        JSONB_TRUE_TYPE => {
            if !payload.is_empty() {
                return Err(FrankenError::function_error("invalid JSONB true payload"));
            }
            Value::Bool(true)
        }
        JSONB_FALSE_TYPE => {
            if !payload.is_empty() {
                return Err(FrankenError::function_error("invalid JSONB false payload"));
            }
            Value::Bool(false)
        }
        JSONB_INT_TYPE => {
            if payload.len() != 8 {
                return Err(FrankenError::function_error(
                    "invalid JSONB integer payload size",
                ));
            }
            let mut raw = [0u8; 8];
            raw.copy_from_slice(payload);
            Value::Number(Number::from(i64::from_be_bytes(raw)))
        }
        JSONB_FLOAT_TYPE => {
            if payload.len() != 8 {
                return Err(FrankenError::function_error(
                    "invalid JSONB float payload size",
                ));
            }
            let mut raw = [0u8; 8];
            raw.copy_from_slice(payload);
            let float = f64::from_bits(u64::from_be_bytes(raw));
            let number = Number::from_f64(float).ok_or_else(|| {
                FrankenError::function_error("invalid non-finite JSONB float payload")
            })?;
            Value::Number(number)
        }
        JSONB_TEXT_TYPE | JSONB_TEXT_JSON_TYPE => {
            let text = String::from_utf8(payload.to_vec()).map_err(|error| {
                FrankenError::function_error(format!("invalid JSONB text payload: {error}"))
            })?;
            Value::String(text)
        }
        JSONB_ARRAY_TYPE => {
            let mut cursor = 0usize;
            let mut values = Vec::new();
            while cursor < payload.len() {
                let (item, used) = decode_jsonb_value(&payload[cursor..])?;
                values.push(item);
                cursor += used;
            }
            Value::Array(values)
        }
        JSONB_OBJECT_TYPE => {
            let mut cursor = 0usize;
            let mut map = Map::new();
            while cursor < payload.len() {
                let (key_node, key_used) = decode_jsonb_value(&payload[cursor..])?;
                cursor += key_used;
                let Value::String(key) = key_node else {
                    return Err(FrankenError::function_error(
                        "invalid JSONB object key payload",
                    ));
                };
                if cursor >= payload.len() {
                    return Err(FrankenError::function_error(
                        "invalid JSONB object missing value",
                    ));
                }
                let (item, used) = decode_jsonb_value(&payload[cursor..])?;
                cursor += used;
                map.insert(key, item);
            }
            Value::Object(map)
        }
        _ => {
            return Err(FrankenError::function_error("invalid JSONB node type"));
        }
    };

    Ok((value, consumed))
}

fn decode_jsonb_node(input: &[u8]) -> Result<(u8, &[u8], usize)> {
    if input.is_empty() {
        return Err(FrankenError::function_error("invalid JSONB: empty payload"));
    }

    let header = input[0];
    let node_type = header >> 4;
    let len_size = usize::from(header & 0x0f);
    if !matches!(len_size, 0 | 1 | 2 | 4 | 8) {
        return Err(FrankenError::function_error(
            "invalid JSONB length-size nibble",
        ));
    }
    if !matches!(
        node_type,
        JSONB_NULL_TYPE
            | JSONB_TRUE_TYPE
            | JSONB_FALSE_TYPE
            | JSONB_INT_TYPE
            | JSONB_FLOAT_TYPE
            | JSONB_TEXT_TYPE
            | JSONB_TEXT_JSON_TYPE
            | JSONB_ARRAY_TYPE
            | JSONB_OBJECT_TYPE
    ) {
        return Err(FrankenError::function_error("invalid JSONB node type"));
    }

    if input.len() < 1 + len_size {
        return Err(FrankenError::function_error(
            "invalid JSONB: truncated payload length",
        ));
    }

    let len_end = 1 + len_size;
    let payload_len = decode_jsonb_payload_len(&input[1..len_end])?;
    let total = 1 + len_size + payload_len;
    if input.len() < total {
        return Err(FrankenError::function_error(
            "invalid JSONB: truncated payload",
        ));
    }

    Ok((node_type, &input[1 + len_size..total], total))
}

fn decode_jsonb_payload_len(bytes: &[u8]) -> Result<usize> {
    if bytes.is_empty() {
        return Ok(0);
    }
    if !matches!(bytes.len(), 1 | 2 | 4 | 8) {
        return Err(FrankenError::function_error(
            "invalid JSONB length encoding size",
        ));
    }

    let mut raw = [0u8; 8];
    raw[8 - bytes.len()..].copy_from_slice(bytes);
    let payload_len = u64::from_be_bytes(raw);
    usize::try_from(payload_len).map_err(|error| {
        FrankenError::function_error(format!("JSONB payload length overflow: {error}"))
    })
}

fn is_superficially_valid_jsonb(input: &[u8]) -> bool {
    if input.is_empty() {
        return false;
    }
    let header = input[0];
    let node_type = header >> 4;
    let len_size = usize::from(header & 0x0f);
    if !matches!(len_size, 0 | 1 | 2 | 4 | 8) {
        return false;
    }
    if !matches!(
        node_type,
        JSONB_NULL_TYPE
            | JSONB_TRUE_TYPE
            | JSONB_FALSE_TYPE
            | JSONB_INT_TYPE
            | JSONB_FLOAT_TYPE
            | JSONB_TEXT_TYPE
            | JSONB_TEXT_JSON_TYPE
            | JSONB_ARRAY_TYPE
            | JSONB_OBJECT_TYPE
    ) {
        return false;
    }
    if input.len() < 1 + len_size {
        return false;
    }
    let len_end = 1 + len_size;
    let Ok(payload_len) = decode_jsonb_payload_len(&input[1..len_end]) else {
        return false;
    };
    1 + len_size + payload_len <= input.len()
}

fn parse_path(path: &str) -> Result<Vec<PathSegment>> {
    let bytes = path.as_bytes();
    if bytes.first().copied() != Some(b'$') {
        return Err(FrankenError::function_error(format!(
            "invalid json path `{path}`: must start with `$`"
        )));
    }

    let mut idx = 1;
    let mut segments = Vec::new();
    while idx < bytes.len() {
        match bytes[idx] {
            b'.' => {
                idx += 1;
                let start = idx;
                while idx < bytes.len() && bytes[idx] != b'.' && bytes[idx] != b'[' {
                    idx += 1;
                }
                if start == idx {
                    return Err(FrankenError::function_error(format!(
                        "invalid json path `{path}`: empty key segment"
                    )));
                }
                segments.push(PathSegment::Key(path[start..idx].to_owned()));
            }
            b'[' => {
                idx += 1;
                let start = idx;
                while idx < bytes.len() && bytes[idx] != b']' {
                    idx += 1;
                }
                if idx >= bytes.len() {
                    return Err(FrankenError::function_error(format!(
                        "invalid json path `{path}`: missing closing `]`"
                    )));
                }
                let segment_text = &path[start..idx];
                idx += 1;

                if let Some(rest) = segment_text.strip_prefix("#-") {
                    let from_end = rest.parse::<usize>().map_err(|error| {
                        FrankenError::function_error(format!(
                            "invalid json path `{path}` from-end index `{segment_text}`: {error}"
                        ))
                    })?;
                    if from_end == 0 {
                        return Err(FrankenError::function_error(format!(
                            "invalid json path `{path}`: from-end index must be >= 1"
                        )));
                    }
                    segments.push(PathSegment::FromEnd(from_end));
                } else {
                    let index = segment_text.parse::<usize>().map_err(|error| {
                        FrankenError::function_error(format!(
                            "invalid json path `{path}` array index `{segment_text}`: {error}"
                        ))
                    })?;
                    segments.push(PathSegment::Index(index));
                }
            }
            _ => {
                return Err(FrankenError::function_error(format!(
                    "invalid json path `{path}` at byte offset {idx}"
                )));
            }
        }
    }

    Ok(segments)
}

fn resolve_path<'a>(root: &'a Value, path: &str) -> Result<Option<&'a Value>> {
    let segments = parse_path(path)?;
    let mut cursor = root;

    for segment in segments {
        match segment {
            PathSegment::Key(key) => {
                let Some(next) = cursor.get(&key) else {
                    return Ok(None);
                };
                cursor = next;
            }
            PathSegment::Index(index) => {
                let Some(array) = cursor.as_array() else {
                    return Ok(None);
                };
                let Some(next) = array.get(index) else {
                    return Ok(None);
                };
                cursor = next;
            }
            PathSegment::FromEnd(from_end) => {
                let Some(array) = cursor.as_array() else {
                    return Ok(None);
                };
                if from_end > array.len() {
                    return Ok(None);
                }
                let index = array.len() - from_end;
                cursor = &array[index];
            }
        }
    }

    Ok(Some(cursor))
}

fn append_object_path(base: &str, key: &str) -> String {
    format!("{base}.{key}")
}

fn append_array_path(base: &str, index: usize) -> String {
    format!("{base}[{index}]")
}

fn json_value_column(value: &Value) -> Result<SqliteValue> {
    match value {
        Value::Array(_) | Value::Object(_) => serde_json::to_string(value)
            .map(SqliteValue::Text)
            .map_err(|error| {
                FrankenError::function_error(format!("json table value encode failed: {error}"))
            }),
        _ => Ok(json_to_sqlite_scalar(value)),
    }
}

fn json_atom_column(value: &Value) -> SqliteValue {
    match value {
        Value::Array(_) | Value::Object(_) => SqliteValue::Null,
        _ => json_to_sqlite_scalar(value),
    }
}

fn append_tree_rows(
    rows: &mut Vec<JsonTableRow>,
    value: &Value,
    key: SqliteValue,
    parent_id: Option<i64>,
    fullkey: &str,
    path: &str,
    next_id: &mut i64,
) -> Result<()> {
    let current_id = *next_id;
    *next_id += 1;

    rows.push(JsonTableRow {
        key,
        value: json_value_column(value)?,
        type_name: json_type_name(value),
        atom: json_atom_column(value),
        id: current_id,
        parent: parent_id.map_or(SqliteValue::Null, SqliteValue::Integer),
        fullkey: fullkey.to_owned(),
        path: path.to_owned(),
    });

    match value {
        Value::Array(array) => {
            for (index, item) in array.iter().enumerate() {
                let index_i64 = i64::try_from(index).map_err(|error| {
                    FrankenError::function_error(format!("json_tree index overflow: {error}"))
                })?;
                let child_fullkey = append_array_path(fullkey, index);
                append_tree_rows(
                    rows,
                    item,
                    SqliteValue::Integer(index_i64),
                    Some(current_id),
                    &child_fullkey,
                    fullkey,
                    next_id,
                )?;
            }
        }
        Value::Object(object) => {
            for (child_key, item) in object {
                let child_fullkey = append_object_path(fullkey, child_key);
                append_tree_rows(
                    rows,
                    item,
                    SqliteValue::Text(child_key.clone()),
                    Some(current_id),
                    &child_fullkey,
                    fullkey,
                    next_id,
                )?;
            }
        }
        _ => {}
    }

    Ok(())
}

fn parse_json_table_filter_args(args: &[SqliteValue]) -> Result<(&str, Option<&str>)> {
    let Some(input_arg) = args.first() else {
        return Err(FrankenError::function_error(
            "json table-valued functions require JSON input argument",
        ));
    };
    let SqliteValue::Text(input_text) = input_arg else {
        return Err(FrankenError::function_error(
            "json table-valued input must be TEXT JSON",
        ));
    };

    let path = match args.get(1) {
        None | Some(SqliteValue::Null) => None,
        Some(SqliteValue::Text(path)) => Some(path.as_str()),
        Some(_) => {
            return Err(FrankenError::function_error(
                "json table-valued PATH argument must be TEXT or NULL",
            ));
        }
    };

    Ok((input_text.as_str(), path))
}

fn write_json_table_column(row: &JsonTableRow, ctx: &mut ColumnContext, col: i32) -> Result<()> {
    let value = match col {
        0 => row.key.clone(),
        1 => row.value.clone(),
        2 => SqliteValue::Text(row.type_name.to_owned()),
        3 => row.atom.clone(),
        4 => SqliteValue::Integer(row.id),
        5 => row.parent.clone(),
        6 => SqliteValue::Text(row.fullkey.clone()),
        7 => SqliteValue::Text(row.path.clone()),
        _ => {
            return Err(FrankenError::function_error(format!(
                "json table-valued invalid column index {col}"
            )));
        }
    };
    ctx.set_value(value);
    Ok(())
}

fn json_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(true) => "true",
        Value::Bool(false) => "false",
        Value::Number(number) => {
            if number.is_i64() || number.is_u64() {
                "integer"
            } else {
                "real"
            }
        }
        Value::String(_) => "text",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn json_to_sqlite_scalar(value: &Value) -> SqliteValue {
    match value {
        Value::Null => SqliteValue::Null,
        Value::Bool(true) => SqliteValue::Integer(1),
        Value::Bool(false) => SqliteValue::Integer(0),
        Value::Number(number) => {
            if let Some(i) = number.as_i64() {
                SqliteValue::Integer(i)
            } else if let Some(u) = number.as_u64() {
                if let Ok(i) = i64::try_from(u) {
                    SqliteValue::Integer(i)
                } else {
                    SqliteValue::Float(u as f64)
                }
            } else {
                SqliteValue::Float(number.as_f64().unwrap_or(0.0))
            }
        }
        Value::String(text) => SqliteValue::Text(text.clone()),
        Value::Array(_) | Value::Object(_) => {
            let encoded = serde_json::to_string(value).unwrap_or_else(|_| "null".to_owned());
            SqliteValue::Text(encoded)
        }
    }
}

fn sqlite_to_json(value: &SqliteValue) -> Result<Value> {
    match value {
        SqliteValue::Null => Ok(Value::Null),
        SqliteValue::Integer(i) => Ok(Value::Number(Number::from(*i))),
        SqliteValue::Float(f) => {
            if !f.is_finite() {
                return Err(FrankenError::function_error(
                    "non-finite float is not representable in JSON",
                ));
            }
            let number = Number::from_f64(*f).ok_or_else(|| {
                FrankenError::function_error("failed to convert floating-point value to JSON")
            })?;
            Ok(Value::Number(number))
        }
        SqliteValue::Text(text) => Ok(Value::String(text.clone())),
        SqliteValue::Blob(bytes) => {
            let mut hex = String::with_capacity(bytes.len() * 2);
            for byte in bytes {
                use std::fmt::Write;
                let _ = write!(hex, "{byte:02x}");
            }
            Ok(Value::String(hex))
        }
    }
}

fn write_pretty_value(value: &Value, indent: &str, depth: usize, out: &mut String) -> Result<()> {
    match value {
        Value::Array(array) => {
            if array.is_empty() {
                out.push_str("[]");
                return Ok(());
            }

            out.push('[');
            out.push('\n');
            for (idx, item) in array.iter().enumerate() {
                out.push_str(&indent.repeat(depth + 1));
                write_pretty_value(item, indent, depth + 1, out)?;
                if idx + 1 < array.len() {
                    out.push(',');
                }
                out.push('\n');
            }
            out.push_str(&indent.repeat(depth));
            out.push(']');
            Ok(())
        }
        Value::Object(object) => {
            if object.is_empty() {
                out.push_str("{}");
                return Ok(());
            }

            out.push('{');
            out.push('\n');
            for (idx, (key, item)) in object.iter().enumerate() {
                out.push_str(&indent.repeat(depth + 1));
                let key_quoted = serde_json::to_string(key).map_err(|error| {
                    FrankenError::function_error(format!(
                        "json_pretty key-encode failed for `{key}`: {error}"
                    ))
                })?;
                out.push_str(&key_quoted);
                out.push_str(": ");
                write_pretty_value(item, indent, depth + 1, out)?;
                if idx + 1 < object.len() {
                    out.push(',');
                }
                out.push('\n');
            }
            out.push_str(&indent.repeat(depth));
            out.push('}');
            Ok(())
        }
        _ => {
            let encoded = serde_json::to_string(value).map_err(|error| {
                FrankenError::function_error(format!("json_pretty value-encode failed: {error}"))
            })?;
            out.push_str(&encoded);
            Ok(())
        }
    }
}

fn edit_json_paths(input: &str, pairs: &[(&str, SqliteValue)], mode: EditMode) -> Result<String> {
    let mut root = parse_json_text(input)?;
    for (path, value) in pairs {
        let segments = parse_path(path)?;
        let replacement = sqlite_to_json(value)?;
        apply_edit(&mut root, &segments, replacement, mode);
    }

    serde_json::to_string(&root)
        .map_err(|error| FrankenError::function_error(format!("json edit encode failed: {error}")))
}

fn apply_edit(root: &mut Value, segments: &[PathSegment], new_value: Value, mode: EditMode) {
    if segments.is_empty() {
        match mode {
            EditMode::Set | EditMode::Replace => *root = new_value,
            EditMode::Insert => {}
        }
        return;
    }

    let (parent_segments, last) = segments.split_at(segments.len() - 1);
    let Some(last_segment) = last.first() else {
        return;
    };
    let Some(parent) = resolve_path_mut(root, parent_segments) else {
        return;
    };

    match (parent, last_segment) {
        (Value::Object(object), PathSegment::Key(key)) => {
            let exists = object.contains_key(key);
            match mode {
                EditMode::Set => {
                    object.insert(key.clone(), new_value);
                }
                EditMode::Insert => {
                    if !exists {
                        object.insert(key.clone(), new_value);
                    }
                }
                EditMode::Replace => {
                    if exists {
                        object.insert(key.clone(), new_value);
                    }
                }
            }
        }
        (Value::Array(array), PathSegment::Index(index)) => {
            apply_array_edit(array, *index, new_value, mode);
        }
        (Value::Array(array), PathSegment::FromEnd(from_end)) => {
            if *from_end == 0 || *from_end > array.len() {
                return;
            }
            let index = array.len() - *from_end;
            apply_array_edit(array, index, new_value, mode);
        }
        _ => {}
    }
}

fn apply_array_edit(array: &mut [Value], index: usize, new_value: Value, mode: EditMode) {
    if index >= array.len() {
        return;
    }

    match mode {
        EditMode::Set | EditMode::Replace => array[index] = new_value,
        EditMode::Insert => {}
    }
}

fn remove_at_path(root: &mut Value, segments: &[PathSegment]) {
    if segments.is_empty() {
        *root = Value::Null;
        return;
    }

    let (parent_segments, last) = segments.split_at(segments.len() - 1);
    let Some(last_segment) = last.first() else {
        return;
    };
    let Some(parent) = resolve_path_mut(root, parent_segments) else {
        return;
    };

    match (parent, last_segment) {
        (Value::Object(object), PathSegment::Key(key)) => {
            object.remove(key);
        }
        (Value::Array(array), PathSegment::Index(index)) => {
            if *index < array.len() {
                array.remove(*index);
            }
        }
        (Value::Array(array), PathSegment::FromEnd(from_end)) => {
            if *from_end == 0 || *from_end > array.len() {
                return;
            }
            let index = array.len() - *from_end;
            array.remove(index);
        }
        _ => {}
    }
}

fn resolve_path_mut<'a>(root: &'a mut Value, segments: &[PathSegment]) -> Option<&'a mut Value> {
    let mut cursor = root;

    for segment in segments {
        match segment {
            PathSegment::Key(key) => {
                let next = cursor.as_object_mut()?.get_mut(key)?;
                cursor = next;
            }
            PathSegment::Index(index) => {
                let next = cursor.as_array_mut()?.get_mut(*index)?;
                cursor = next;
            }
            PathSegment::FromEnd(from_end) => {
                let array = cursor.as_array_mut()?;
                if *from_end == 0 || *from_end > array.len() {
                    return None;
                }
                let index = array.len() - *from_end;
                let next = array.get_mut(index)?;
                cursor = next;
            }
        }
    }

    Some(cursor)
}

fn merge_patch(target: Value, patch: Value) -> Value {
    match patch {
        Value::Object(patch_map) => {
            let mut target_map = match target {
                Value::Object(map) => map,
                _ => Map::new(),
            };

            for (key, patch_value) in patch_map {
                if patch_value.is_null() {
                    target_map.remove(&key);
                    continue;
                }
                let prior = target_map.remove(&key).unwrap_or(Value::Null);
                target_map.insert(key, merge_patch(prior, patch_value));
            }

            Value::Object(target_map)
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_valid_text() {
        assert_eq!(json(r#"{"a":1}"#).unwrap(), r#"{"a":1}"#);
    }

    #[test]
    fn test_json_invalid_error() {
        let err = json("not json").unwrap_err();
        assert!(matches!(err, FrankenError::FunctionError(_)));
    }

    #[test]
    fn test_json_valid_flags_default() {
        assert_eq!(json_valid(r#"{"a":1}"#, None), 1);
        assert_eq!(json_valid("not json", None), 0);
    }

    #[test]
    fn test_json_valid_flags_json5() {
        let json5_text = concat!("{", "a:1", "}");
        assert_eq!(json_valid(json5_text, Some(JSON_VALID_JSON5_FLAG)), 1);
        assert_eq!(json_valid(json5_text, Some(JSON_VALID_RFC_8259_FLAG)), 0);
    }

    #[test]
    fn test_json_valid_flags_jsonb() {
        let payload = jsonb(r#"{"a":[1,2,3]}"#).unwrap();
        assert_eq!(
            json_valid_blob(&payload, Some(JSON_VALID_JSONB_SUPERFICIAL_FLAG)),
            1
        );
        assert_eq!(
            json_valid_blob(&payload, Some(JSON_VALID_JSONB_STRICT_FLAG)),
            1
        );
        let mut broken = payload;
        broken.push(0xFF);
        assert_eq!(
            json_valid_blob(&broken, Some(JSON_VALID_JSONB_SUPERFICIAL_FLAG)),
            1
        );
        assert_eq!(
            json_valid_blob(&broken, Some(JSON_VALID_JSONB_STRICT_FLAG)),
            0
        );
    }

    #[test]
    fn test_json_type_object() {
        assert_eq!(json_type(r#"{"a":1}"#, None).unwrap(), Some("object"));
    }

    #[test]
    fn test_json_type_path() {
        assert_eq!(
            json_type(r#"{"a":1}"#, Some("$.a")).unwrap(),
            Some("integer")
        );
    }

    #[test]
    fn test_json_type_missing_path() {
        assert_eq!(json_type(r#"{"a":1}"#, Some("$.b")).unwrap(), None);
    }

    #[test]
    fn test_json_extract_single() {
        let result = json_extract(r#"{"a":1}"#, &["$.a"]).unwrap();
        assert_eq!(result, SqliteValue::Integer(1));
    }

    #[test]
    fn test_json_extract_multiple() {
        let result = json_extract(r#"{"a":1,"b":2}"#, &["$.a", "$.b"]).unwrap();
        assert_eq!(result, SqliteValue::Text("[1,2]".to_owned()));
    }

    #[test]
    fn test_json_extract_string_unwrap() {
        let result = json_extract(r#"{"a":"hello"}"#, &["$.a"]).unwrap();
        assert_eq!(result, SqliteValue::Text("hello".to_owned()));
    }

    #[test]
    fn test_arrow_preserves_json() {
        let result = json_arrow(r#"{"a":"hello"}"#, "$.a").unwrap();
        assert_eq!(result, SqliteValue::Text(r#""hello""#.to_owned()));
    }

    #[test]
    fn test_double_arrow_unwraps() {
        let result = json_double_arrow(r#"{"a":"hello"}"#, "$.a").unwrap();
        assert_eq!(result, SqliteValue::Text("hello".to_owned()));
    }

    #[test]
    fn test_json_extract_array_index() {
        let result = json_extract("[10,20,30]", &["$[1]"]).unwrap();
        assert_eq!(result, SqliteValue::Integer(20));
    }

    #[test]
    fn test_json_extract_from_end() {
        let result = json_extract("[10,20,30]", &["$[#-1]"]).unwrap();
        assert_eq!(result, SqliteValue::Integer(30));
    }

    #[test]
    fn test_jsonb_extract_returns_blob() {
        let blob = jsonb_extract(r#"{"a":"hello"}"#, &["$.a"]).unwrap();
        let text = json_from_jsonb(&blob).unwrap();
        assert_eq!(text, r#""hello""#);
    }

    #[test]
    fn test_json_quote_text() {
        assert_eq!(
            json_quote(&SqliteValue::Text("hello".to_owned())),
            r#""hello""#
        );
    }

    #[test]
    fn test_json_quote_null() {
        assert_eq!(json_quote(&SqliteValue::Null), "null");
    }

    #[test]
    fn test_json_array_basic() {
        let out = json_array(&[
            SqliteValue::Integer(1),
            SqliteValue::Text("two".to_owned()),
            SqliteValue::Null,
        ])
        .unwrap();
        assert_eq!(out, r#"[1,"two",null]"#);
    }

    #[test]
    fn test_json_object_basic() {
        let out = json_object(&[
            SqliteValue::Text("a".to_owned()),
            SqliteValue::Integer(1),
            SqliteValue::Text("b".to_owned()),
            SqliteValue::Text("two".to_owned()),
        ])
        .unwrap();
        assert_eq!(out, r#"{"a":1,"b":"two"}"#);
    }

    #[test]
    fn test_jsonb_roundtrip() {
        let blob = jsonb(r#"{"a":1,"b":[2,3]}"#).unwrap();
        let text = json_from_jsonb(&blob).unwrap();
        assert_eq!(text, r#"{"a":1,"b":[2,3]}"#);
    }

    #[test]
    fn test_json_array_length() {
        assert_eq!(json_array_length("[1,2,3]", None).unwrap(), Some(3));
        assert_eq!(json_array_length("[]", None).unwrap(), Some(0));
        assert_eq!(json_array_length(r#"{"a":1}"#, None).unwrap(), None);
    }

    #[test]
    fn test_json_error_position_valid() {
        assert_eq!(json_error_position(r#"{"a":1}"#), 0);
    }

    #[test]
    fn test_json_error_position_invalid() {
        assert!(json_error_position(r#"{"a":}"#) > 0);
    }

    #[test]
    fn test_json_pretty_default() {
        let output = json_pretty(r#"{"a":1}"#, None).unwrap();
        assert!(output.contains('\n'));
        assert!(output.contains("    \"a\""));
    }

    #[test]
    fn test_json_pretty_custom_indent() {
        let output = json_pretty(r#"{"a":1}"#, Some("\t")).unwrap();
        assert!(output.contains("\n\t\"a\""));
    }

    #[test]
    fn test_json_set_create() {
        let out = json_set(r#"{"a":1}"#, &[("$.b", SqliteValue::Integer(2))]).unwrap();
        assert_eq!(out, r#"{"a":1,"b":2}"#);
    }

    #[test]
    fn test_json_set_overwrite() {
        let out = json_set(r#"{"a":1}"#, &[("$.a", SqliteValue::Integer(2))]).unwrap();
        assert_eq!(out, r#"{"a":2}"#);
    }

    #[test]
    fn test_json_insert_no_overwrite() {
        let out = json_insert(r#"{"a":1}"#, &[("$.a", SqliteValue::Integer(2))]).unwrap();
        assert_eq!(out, r#"{"a":1}"#);
    }

    #[test]
    fn test_json_insert_create() {
        let out = json_insert(r#"{"a":1}"#, &[("$.b", SqliteValue::Integer(2))]).unwrap();
        assert_eq!(out, r#"{"a":1,"b":2}"#);
    }

    #[test]
    fn test_json_replace_overwrite() {
        let out = json_replace(r#"{"a":1}"#, &[("$.a", SqliteValue::Integer(2))]).unwrap();
        assert_eq!(out, r#"{"a":2}"#);
    }

    #[test]
    fn test_json_replace_no_create() {
        let out = json_replace(r#"{"a":1}"#, &[("$.b", SqliteValue::Integer(2))]).unwrap();
        assert_eq!(out, r#"{"a":1}"#);
    }

    #[test]
    fn test_json_remove_key() {
        let out = json_remove(r#"{"a":1,"b":2}"#, &["$.a"]).unwrap();
        assert_eq!(out, r#"{"b":2}"#);
    }

    #[test]
    fn test_json_remove_array_compact() {
        let out = json_remove("[1,2,3]", &["$[1]"]).unwrap();
        assert_eq!(out, "[1,3]");
    }

    #[test]
    fn test_json_patch_merge() {
        let out = json_patch(r#"{"a":1,"b":2}"#, r#"{"b":3,"c":4}"#).unwrap();
        assert_eq!(out, r#"{"a":1,"b":3,"c":4}"#);
    }

    #[test]
    fn test_json_patch_delete() {
        let out = json_patch(r#"{"a":1,"b":2}"#, r#"{"b":null}"#).unwrap();
        assert_eq!(out, r#"{"a":1}"#);
    }

    #[test]
    fn test_jsonb_set_variant() {
        let blob = jsonb_set(r#"{"a":1}"#, &[("$.a", SqliteValue::Integer(9))]).unwrap();
        let text = json_from_jsonb(&blob).unwrap();
        assert_eq!(text, r#"{"a":9}"#);
    }

    #[test]
    fn test_json_group_array_includes_nulls() {
        let out = json_group_array(&[
            SqliteValue::Integer(1),
            SqliteValue::Null,
            SqliteValue::Integer(3),
        ])
        .unwrap();
        assert_eq!(out, "[1,null,3]");
    }

    #[test]
    fn test_json_group_object_duplicate_keys_last_wins() {
        let out = json_group_object(&[
            (SqliteValue::Text("k".to_owned()), SqliteValue::Integer(1)),
            (SqliteValue::Text("k".to_owned()), SqliteValue::Integer(2)),
        ])
        .unwrap();
        assert_eq!(out, r#"{"k":2}"#);
    }

    #[test]
    fn test_jsonb_group_array_and_object_variants() {
        let array_blob = jsonb_group_array(&[SqliteValue::Integer(1), SqliteValue::Null]).unwrap();
        assert_eq!(json_from_jsonb(&array_blob).unwrap(), "[1,null]");

        let object_blob =
            jsonb_group_object(&[(SqliteValue::Text("a".to_owned()), SqliteValue::Integer(7))])
                .unwrap();
        assert_eq!(json_from_jsonb(&object_blob).unwrap(), r#"{"a":7}"#);
    }

    #[test]
    fn test_json_each_array() {
        let rows = json_each("[10,20]", None).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].key, SqliteValue::Integer(0));
        assert_eq!(rows[1].key, SqliteValue::Integer(1));
        assert_eq!(rows[0].value, SqliteValue::Integer(10));
        assert_eq!(rows[1].value, SqliteValue::Integer(20));
    }

    #[test]
    fn test_json_each_object() {
        let rows = json_each(r#"{"a":1,"b":2}"#, None).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].key, SqliteValue::Text("a".to_owned()));
        assert_eq!(rows[1].key, SqliteValue::Text("b".to_owned()));
        assert_eq!(rows[0].value, SqliteValue::Integer(1));
        assert_eq!(rows[1].value, SqliteValue::Integer(2));
    }

    #[test]
    fn test_json_tree_recursive() {
        let rows = json_tree(r#"{"a":{"b":1}}"#, None).unwrap();
        assert!(rows.iter().any(|row| row.fullkey == "$.a"));
        assert!(rows.iter().any(|row| row.fullkey == "$.a.b"));
    }

    #[test]
    fn test_json_each_columns() {
        let rows = json_each(r#"{"a":1}"#, None).unwrap();
        let row = rows.first().unwrap();
        assert_eq!(row.key, SqliteValue::Text("a".to_owned()));
        assert_eq!(row.value, SqliteValue::Integer(1));
        assert_eq!(row.type_name, "integer");
        assert_eq!(row.atom, SqliteValue::Integer(1));
        assert_eq!(row.parent, SqliteValue::Null);
        assert_eq!(row.fullkey, "$.a");
        assert_eq!(row.path, "$");
    }

    #[test]
    fn test_json_each_vtab_cursor_scan() {
        let cx = Cx::new();
        let vtab = JsonEachVtab::connect(&cx, &[]).unwrap();
        let mut cursor = vtab.open().unwrap();
        cursor
            .filter(&cx, 0, None, &[SqliteValue::Text("[4,5]".to_owned())])
            .unwrap();

        let mut values = Vec::new();
        while !cursor.eof() {
            let mut key_ctx = ColumnContext::new();
            let mut value_ctx = ColumnContext::new();
            cursor.column(&mut key_ctx, 0).unwrap();
            cursor.column(&mut value_ctx, 1).unwrap();
            values.push((
                key_ctx.take_value().unwrap(),
                value_ctx.take_value().unwrap(),
            ));
            cursor.next(&cx).unwrap();
        }

        assert_eq!(
            values,
            vec![
                (SqliteValue::Integer(0), SqliteValue::Integer(4)),
                (SqliteValue::Integer(1), SqliteValue::Integer(5)),
            ]
        );
    }

    #[test]
    fn test_json_tree_vtab_cursor_scan() {
        let cx = Cx::new();
        let vtab = JsonTreeVtab::connect(&cx, &[]).unwrap();
        let mut cursor = vtab.open().unwrap();
        cursor
            .filter(
                &cx,
                0,
                None,
                &[
                    SqliteValue::Text(r#"{"a":{"b":1}}"#.to_owned()),
                    SqliteValue::Text("$.a".to_owned()),
                ],
            )
            .unwrap();

        let mut fullkeys = Vec::new();
        while !cursor.eof() {
            let mut ctx = ColumnContext::new();
            cursor.column(&mut ctx, 6).unwrap();
            let fullkey = ctx.take_value().unwrap();
            if let SqliteValue::Text(text) = fullkey {
                fullkeys.push(text);
            }
            cursor.next(&cx).unwrap();
        }

        assert_eq!(fullkeys, vec!["$.a".to_owned(), "$.a.b".to_owned()]);
    }
}
