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
//! - `$."key.with.dots"` quoted object member
//! - `$[N]` array index
//! - `$[#]` append pseudo-index
//! - `$[#-N]` reverse array index

use std::borrow::Cow;
use std::sync::Arc;

use fsqlite_error::{FrankenError, Result};
use fsqlite_func::{
    ColumnContext, FunctionRegistry, IndexInfo, JSON_SUBTYPE, ScalarFunction, VirtualTable,
    VirtualTableCursor,
};
use fsqlite_types::{SmallText, SqliteValue, cx::Cx};
use serde_json::{Map, Number, Value};

const JSON_VALID_DEFAULT_FLAGS: u8 = 0x01;
const JSON_VALID_RFC_8259_FLAG: u8 = 0x01;
const JSON_VALID_JSON5_FLAG: u8 = 0x02;
const JSON_VALID_JSONB_SUPERFICIAL_FLAG: u8 = 0x04;
const JSON_VALID_JSONB_STRICT_FLAG: u8 = 0x08;
const JSON_PRETTY_DEFAULT_INDENT_WIDTH: usize = 4;
/// Output columns for the `json_each` / `json_tree` table-valued functions.
pub const JSON_TABLE_COLUMN_NAMES: [&str; 8] = [
    "key", "value", "type", "atom", "id", "parent", "fullkey", "path",
];

const JSONB_NULL_TYPE: u8 = 0x0;
const JSONB_TRUE_TYPE: u8 = 0x1;
const JSONB_FALSE_TYPE: u8 = 0x2;
const JSONB_INT_TYPE: u8 = 0x3;
const JSONB_INT5_TYPE: u8 = 0x4;
const JSONB_FLOAT_TYPE: u8 = 0x5;
const JSONB_FLOAT5_TYPE: u8 = 0x6;
const JSONB_TEXT_TYPE: u8 = 0x7;
const JSONB_TEXT_JSON_TYPE: u8 = 0x8;
const JSONB_TEXT5_TYPE: u8 = 0x9;
const JSONB_TEXTRAW_TYPE: u8 = 0xA;
const JSONB_ARRAY_TYPE: u8 = 0xB;
const JSONB_OBJECT_TYPE: u8 = 0xC;

#[derive(Debug, Clone, PartialEq, Eq)]
enum PathSegment {
    Key(SmallText),
    Index(usize),
    Append,
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
    // Stock json() on text minifies whitespace but preserves every token of the
    // source verbatim (number literals, string escapes, duplicate keys). Parse
    // only to validate, then lexically minify — re-serializing a parsed
    // `serde_json::Value` would normalize exponents, unescape `\/`, and drop
    // duplicate keys (bd-6b0pe / bd-p2xrc).
    parse_json_text(input)?;
    Ok(minify_json_text(input))
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

    if (allow_json || allow_json5)
        && let Ok(text) = std::str::from_utf8(input)
    {
        if allow_json && parse_json_text(text).is_ok() {
            return 1;
        }
        if allow_json5 && parse_json5_text(text).is_ok() {
            return 1;
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
    encode_json_text("json_from_jsonb encode failed", &value)
}

fn encode_json_text(context: &str, value: &Value) -> Result<String> {
    let mut out = String::new();
    write_canonical_json_text(value, &mut out)
        .map_err(|error| FrankenError::function_error(format!("{context}: {error}")))?;
    Ok(out)
}

/// Minify already-validated JSON *text* the way stock `json()` does: strip only
/// insignificant (inter-token) whitespace and preserve every token verbatim.
///
/// Stock SQLite's `json()` never rewrites the parsed source — number literals
/// keep their exact form (`1e2`, `1E2`, `1e+2`, `1e02` are all distinct;
/// `1.50` keeps its trailing zero), string escapes are preserved (`"a\/b"`
/// stays `\/`, not `/`), and duplicate object keys are kept. Round-tripping
/// through `serde_json::Value` loses all of that (exponents are normalised to
/// `e+NN`, `\/` is unescaped, duplicate keys are dropped), so the text-input
/// path lexically minifies the source instead of re-serialising a parsed tree.
/// The caller MUST have validated `input` as well-formed JSON first.
fn minify_json_text(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut in_string = false;
    let mut escaped = false;
    for ch in input.chars() {
        if in_string {
            out.push(ch);
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
        } else if matches!(ch, ' ' | '\t' | '\n' | '\r') {
            // Insignificant whitespace between tokens — drop it.
        } else {
            out.push(ch);
            if ch == '"' {
                in_string = true;
            }
        }
    }
    out
}

/// Minified JSON writer with stock-SQLite-canonical float text: `{:?}` is
/// Rust's shortest round-trip form ("1e300", "-0.0", "0.1"), whereas
/// `serde_json::to_string` renders exponents with a non-canonical '+'
/// ("1e+300") that diverges from stock `json()` output (bd-t75hg).
fn write_canonical_json_text(value: &Value, out: &mut String) -> Result<()> {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(true) => out.push_str("true"),
        Value::Bool(false) => out.push_str("false"),
        Value::Number(number) => {
            if number.is_i64() || number.is_u64() {
                out.push_str(&number.to_string());
            } else if let Some(float) = number.as_f64().filter(|value| value.is_finite()) {
                // Finite float: Rust's shortest round-trip text (`{float:?}` ->
                // "1e300", "-0.0", "0.1") matches stock json()'s rendering of a
                // JSONB numeric payload and of a constructed real. This branch is
                // reached by json(BLOB) (decode -> Value -> render), json_object/
                // json_array/json_set, and json_extract-to-JSON — NOT by the
                // text-input json() path, which preserves the source literal via
                // `minify_json_text` (bd-6b0pe). `Number::to_string()` would emit
                // the arbitrary_precision-normalized form, inserting a '+' into
                // the exponent ("1e300" -> "1e+300"), which diverges from stock
                // on the json(BLOB) round-trip and breaks byte-compatible JSONB
                // interop (bd-t75hg; bd-p2xrc's number.to_string here was
                // superseded by minify and reverted).
                out.push_str(&format!("{float:?}"));
            } else {
                // Non-finite (or beyond-f64 magnitude) JSON number, only
                // reachable with the `arbitrary_precision` Number backing.
                // SQLite renders +Inf/-Inf as a numeric literal and preserves
                // the source text of a parsed value: a constructed value
                // carries the canonical `9.0e+999`, a parsed value carries its
                // source (`9e999`) — stock's construct-vs-preserve asymmetry
                // (GH#212, bd-t75hg).
                out.push_str(&render_non_finite_number(number));
            }
        }
        Value::String(text) => {
            let escaped = serde_json::to_string(text).map_err(|error| {
                FrankenError::function_error(format!("JSON string escape failed: {error}"))
            })?;
            out.push_str(&escaped);
        }
        Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_canonical_json_text(item, out)?;
            }
            out.push(']');
        }
        Value::Object(map) => {
            out.push('{');
            for (index, (key, item)) in map.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                let escaped = serde_json::to_string(key).map_err(|error| {
                    FrankenError::function_error(format!("JSON key escape failed: {error}"))
                })?;
                out.push_str(&escaped);
                out.push(':');
                write_canonical_json_text(item, out)?;
            }
            out.push('}');
        }
    }
    Ok(())
}

fn parse_json_input_blob(input: &[u8]) -> Result<Value> {
    match decode_jsonb_root(input) {
        Ok(value) => Ok(value),
        Err(jsonb_error) => match std::str::from_utf8(input) {
            Ok(text) => parse_json_text(text),
            Err(_) => Err(jsonb_error),
        },
    }
}

fn json_type_value(root: &Value, path: Option<&str>) -> Result<Option<&'static str>> {
    let target = match path {
        Some(path_expr) => resolve_path(root, path_expr)?,
        None => Some(root),
    };
    Ok(target.map(json_type_name))
}

fn json_extract_value(root: &Value, paths: &[&str]) -> Result<SqliteValue> {
    if paths.is_empty() {
        return Err(FrankenError::function_error(
            "json_extract requires at least one path",
        ));
    }

    if paths.len() == 1 {
        return json_extract_single_path(root, paths[0]);
    }

    let mut out = Vec::with_capacity(paths.len());
    for path_expr in paths {
        let selected = resolve_path(root, path_expr)?;
        out.push(selected.cloned().unwrap_or(Value::Null));
    }

    let encoded = encode_json_text("json_extract array encode failed", &Value::Array(out))?;
    Ok(SqliteValue::Text(encoded.into()))
}

fn jsonb_extract_value(root: &Value, paths: &[&str]) -> Result<Vec<u8>> {
    if paths.is_empty() {
        return Err(FrankenError::function_error(
            "jsonb_extract requires at least one path",
        ));
    }

    let output = if paths.len() == 1 {
        jsonb_extract_single_path_value(root, paths[0])?
    } else {
        let mut values = Vec::with_capacity(paths.len());
        for path_expr in paths {
            values.push(
                resolve_path(root, path_expr)?
                    .cloned()
                    .unwrap_or(Value::Null),
            );
        }
        Value::Array(values)
    };

    encode_jsonb_root(&output)
}

fn json_arrow_value(root: &Value, path: &str) -> Result<SqliteValue> {
    let selected = resolve_path(root, path)?;
    let Some(value) = selected else {
        return Ok(SqliteValue::Null);
    };
    let encoded = encode_json_text("json_arrow encode failed", value)?;
    Ok(SqliteValue::Text(encoded.into()))
}

fn json_extract_single_path(root: &Value, path: &str) -> Result<SqliteValue> {
    let selected = resolve_path(root, path)?;
    Ok(selected.map_or(SqliteValue::Null, json_to_sqlite_scalar))
}

fn jsonb_extract_single_path_value(root: &Value, path: &str) -> Result<Value> {
    Ok(resolve_path(root, path)?.cloned().unwrap_or(Value::Null))
}

fn json_array_length_value(root: &Value, path: Option<&str>) -> Result<Option<usize>> {
    let target = match path {
        Some(path_expr) => resolve_path(root, path_expr)?,
        None => Some(root),
    };
    Ok(match target {
        Some(Value::Array(array)) => Some(array.len()),
        Some(_) => Some(0),
        None => None,
    })
}

fn json_error_position_blob(input: &[u8]) -> usize {
    if decode_jsonb_root(input).is_ok() {
        return 0;
    }
    match std::str::from_utf8(input) {
        Ok(text) => json_error_position(text),
        Err(_) => 1,
    }
}

fn json_pretty_value(root: &Value, indent: Option<&str>) -> Result<String> {
    let indent_unit = match indent {
        Some(indent) => indent.to_owned(),
        None => " ".repeat(JSON_PRETTY_DEFAULT_INDENT_WIDTH),
    };
    let mut out = String::new();
    write_pretty_value(root, &indent_unit, 0, &mut out)?;
    Ok(out)
}

fn edit_json_paths_value(
    root: &Value,
    pairs: &[(&str, SqliteValue)],
    mode: EditMode,
) -> Result<Value> {
    let mut edited = root.clone();
    for (path, value) in pairs {
        let segments = parse_path(path)?;
        let replacement = sqlite_to_json(value)?;
        apply_edit(&mut edited, &segments, replacement, mode);
    }
    Ok(edited)
}

/// Like [`edit_json_paths_value`], but honors each value argument's subtype: a
/// value carrying `JSON_SUBTYPE` (e.g. produced by `json(...)`) is embedded as a
/// JSON subtree instead of being stringified. `arg_subtypes` is indexed parallel
/// to `args`; the value for the (path, value) pair beginning at `idx` is
/// `args[idx + 1]` with subtype `arg_subtypes[idx + 1]`. (GH #233)
fn edit_json_paths_value_with_subtypes(
    name: &str,
    input: &Value,
    args: &[SqliteValue],
    arg_subtypes: &[u32],
    start: usize,
    mode: EditMode,
) -> Result<Value> {
    let mut edited = input.clone();
    let mut idx = start;
    while idx + 1 < args.len() {
        let path = text_arg(name, args, idx)?.to_owned();
        let segments = parse_path(&path)?;
        let subtype = arg_subtypes.get(idx + 1).copied().unwrap_or(0);
        let replacement = sqlite_to_json_with_subtype(&args[idx + 1], subtype)?;
        apply_edit(&mut edited, &segments, replacement, mode);
        idx += 2;
    }
    Ok(edited)
}

fn json_remove_value(root: &Value, paths: &[&str]) -> Result<Option<Value>> {
    let mut edited = root.clone();
    for path in paths {
        let segments = parse_path(path)?;
        if segments.is_empty() {
            return Ok(None);
        }
        remove_at_path(&mut edited, &segments);
    }
    Ok(Some(edited))
}

fn json_patch_value(root: &Value, patch: &Value) -> Value {
    merge_patch(root.clone(), patch.clone())
}

/// Return JSON type name at the root or an optional path.
///
/// Returns `None` when the path does not resolve.
pub fn json_type(input: &str, path: Option<&str>) -> Result<Option<&'static str>> {
    let root = parse_json_text(input)?;
    json_type_value(&root, path)
}

/// Extract JSON value(s) by path, following SQLite single vs multi-path behavior.
///
/// - One path: return SQL-native value (text unwrapped, number typed, JSON null -> SQL NULL)
/// - Multiple paths: return JSON array text of extracted values (missing paths become `null`)
pub fn json_extract(input: &str, paths: &[&str]) -> Result<SqliteValue> {
    let root = parse_json_text(input)?;
    json_extract_value(&root, paths)
}

/// JSONB variant of `json_extract`.
///
/// The extracted JSON subtree is always returned as JSONB bytes.
pub fn jsonb_extract(input: &str, paths: &[&str]) -> Result<Vec<u8>> {
    let root = parse_json_text(input)?;
    jsonb_extract_value(&root, paths)
}

/// Extract with `->` semantics: always returns JSON text for the selected node.
///
/// Missing paths yield SQL NULL.
pub fn json_arrow(input: &str, path: &str) -> Result<SqliteValue> {
    let root = parse_json_text(input)?;
    json_arrow_value(&root, path)
}

/// Extract with `->>` semantics: returns SQL-native value.
pub fn json_double_arrow(input: &str, path: &str) -> Result<SqliteValue> {
    json_extract(input, &[path])
}

/// Return the array length at root or path.
///
/// Matches SQLite JSON1 semantics: a missing path returns SQL NULL, while an
/// existing non-array target returns 0.
pub fn json_array_length(input: &str, path: Option<&str>) -> Result<Option<usize>> {
    let root = parse_json_text(input)?;
    json_array_length_value(&root, path)
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
            let mut char_pos = 1usize;
            for (_idx, ch) in input.char_indices() {
                if current_line == line && current_col == column {
                    return char_pos;
                }
                if ch == '\n' {
                    current_line += 1;
                    current_col = 1;
                } else {
                    current_col += 1;
                }
                char_pos += 1;
            }
            char_pos
        }
    }
}

/// Pretty-print JSON with default 4-space indentation or custom indent token.
pub fn json_pretty(input: &str, indent: Option<&str>) -> Result<String> {
    let root = parse_json_text(input)?;
    json_pretty_value(&root, indent)
}

/// Quote a SQL value as JSON.
pub fn json_quote(value: &SqliteValue) -> Result<String> {
    match value {
        SqliteValue::Null => Ok("null".to_owned()),
        SqliteValue::Integer(i) => Ok(i.to_string()),
        v @ SqliteValue::Float(f) => {
            if f.is_finite() {
                Ok(v.to_text())
            } else if f.is_nan() {
                // Only NaN maps to JSON null (SQLite stores NaN as NULL).
                Ok("null".to_owned())
            } else if *f > 0.0 {
                // +Inf/-Inf render as the numeric literal 9.0e+999 / -9.0e+999
                // (stock `json_quote(1e999)` -> `9.0e+999`, GH#212).
                Ok("9.0e+999".to_owned())
            } else {
                Ok("-9.0e+999".to_owned())
            }
        }
        SqliteValue::Text(text) => {
            Ok(serde_json::to_string(text).unwrap_or_else(|_| "\"\"".to_owned()))
        }
        SqliteValue::Blob(_) => Err(FrankenError::function_error("JSON cannot hold BLOB values")),
    }
}

/// Convert a SQL value to a JSON value, honouring the JSON subtype.
///
/// When `subtype == JSON_SUBTYPE`, a TEXT (or UTF-8 BLOB/JSONB) argument is
/// the textual rendering of an existing JSON value (e.g. the result of
/// `json('[2,3]')`), so it is parsed and embedded verbatim instead of being
/// quoted as a string. This mirrors C SQLite, which keys this off
/// `sqlite3_value_subtype()`.
fn sqlite_to_json_with_subtype(value: &SqliteValue, subtype: u32) -> Result<Value> {
    if subtype == JSON_SUBTYPE {
        match value {
            SqliteValue::Text(text) => return parse_json_text(text),
            SqliteValue::Blob(bytes) => {
                if let Ok(text) = std::str::from_utf8(bytes) {
                    return parse_json_text(text);
                }
            }
            _ => {}
        }
    }
    sqlite_to_json(value)
}

/// Build a JSON array from SQL values.
pub fn json_array(values: &[SqliteValue]) -> Result<String> {
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        out.push(sqlite_to_json(value)?);
    }
    // Route through the canonical writer (not `serde_json::to_string`) so floats
    // render in stock-canonical form and non-finite numbers are handled — the
    // `arbitrary_precision` Number backing serializes an exponent as `e+NNN`
    // and would diverge here (GH#212, bd-t75hg).
    encode_json_text("json_array encode failed", &Value::Array(out))
}

/// Build a JSON array from SQL values, embedding JSON-subtyped arguments as
/// parsed JSON values rather than quoting them as strings.
pub fn json_array_with_subtypes(values: &[SqliteValue], subtypes: &[u32]) -> Result<String> {
    let mut out = Vec::with_capacity(values.len());
    for (i, value) in values.iter().enumerate() {
        let subtype = subtypes.get(i).copied().unwrap_or(0);
        out.push(sqlite_to_json_with_subtype(value, subtype)?);
    }
    encode_json_text("json_array encode failed", &Value::Array(out))
}

/// Serialize an ordered key/value list as JSON object text.
///
/// Preserves argument order AND duplicate labels verbatim — stock SQLite
/// semantics (`json_object('a',1,'a',2)` -> `{"a":1,"a":2}`). serde_json's `Map`
/// collapses duplicate labels to the last value, so the object text is assembled
/// directly instead of routing through `Value::Object`.
fn encode_json_object_members(members: &[(String, Value)]) -> Result<String> {
    let mut out = String::from("{");
    for (index, (key, value)) in members.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        let key_json = serde_json::to_string(key).map_err(|error| {
            FrankenError::function_error(format!("json_object key encode failed: {error}"))
        })?;
        out.push_str(&key_json);
        out.push(':');
        // Canonical writer (not `serde_json::to_string`): stock-canonical floats
        // and correct non-finite rendering under `arbitrary_precision` (GH#212).
        write_canonical_json_text(value, &mut out)?;
    }
    out.push('}');
    Ok(out)
}

/// Build a JSON object from alternating key/value arguments, embedding
/// JSON-subtyped values as parsed JSON rather than quoting them as strings.
///
/// Duplicate labels are preserved verbatim in argument order, matching stock
/// SQLite.
pub fn json_object_with_subtypes(args: &[SqliteValue], subtypes: &[u32]) -> Result<String> {
    if !args.len().is_multiple_of(2) {
        return Err(FrankenError::function_error(
            "json_object requires an even number of arguments",
        ));
    }

    let mut members = Vec::with_capacity(args.len() / 2);
    let mut idx = 0;
    while idx < args.len() {
        let key = match &args[idx] {
            SqliteValue::Text(text) => text.to_string(),
            _ => {
                return Err(FrankenError::function_error(
                    "json_object keys must be text",
                ));
            }
        };
        let subtype = subtypes.get(idx + 1).copied().unwrap_or(0);
        members.push((key, sqlite_to_json_with_subtype(&args[idx + 1], subtype)?));
        idx += 2;
    }

    encode_json_object_members(&members)
}

/// Build a JSON object from alternating key/value SQL arguments.
///
/// Duplicate labels are preserved verbatim in argument order, matching stock
/// SQLite (`json_object('a',1,'a',2)` -> `{"a":1,"a":2}`).
pub fn json_object(args: &[SqliteValue]) -> Result<String> {
    if !args.len().is_multiple_of(2) {
        return Err(FrankenError::function_error(
            "json_object requires an even number of arguments",
        ));
    }

    let mut members = Vec::with_capacity(args.len() / 2);
    let mut idx = 0;
    while idx < args.len() {
        let key = match &args[idx] {
            SqliteValue::Text(text) => text.to_string(),
            _ => {
                return Err(FrankenError::function_error(
                    "json_object keys must be text",
                ));
            }
        };
        members.push((key, sqlite_to_json(&args[idx + 1])?));
        idx += 2;
    }

    encode_json_object_members(&members)
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
/// Duplicate keys are preserved verbatim in row order, matching stock SQLite
/// (rows `('a',1),('a',2)` -> `{"a":1,"a":2}`).
pub fn json_group_object(entries: &[(SqliteValue, SqliteValue)]) -> Result<String> {
    let mut members = Vec::with_capacity(entries.len());
    for (key_value, value) in entries {
        let key = match key_value {
            SqliteValue::Text(text) => text.to_string(),
            _ => {
                return Err(FrankenError::function_error(
                    "json_group_object keys must be text",
                ));
            }
        };
        members.push((key, sqlite_to_json(value)?));
    }
    encode_json_object_members(&members)
}

/// JSONB variant of `json_group_object`.
pub fn jsonb_group_object(entries: &[(SqliteValue, SqliteValue)]) -> Result<Vec<u8>> {
    let json_text = json_group_object(entries)?;
    jsonb(&json_text)
}

/// Set JSON values at path(s), creating object keys when missing.
pub fn json_set(input: &str, pairs: &[(&str, SqliteValue)]) -> Result<String> {
    let root = parse_json_text(input)?;
    let edited = edit_json_paths_value(&root, pairs, EditMode::Set)?;
    encode_json_text("json edit encode failed", &edited)
}

/// JSONB variant of `json_set`.
pub fn jsonb_set(input: &str, pairs: &[(&str, SqliteValue)]) -> Result<Vec<u8>> {
    let root = parse_json_text(input)?;
    let edited = edit_json_paths_value(&root, pairs, EditMode::Set)?;
    encode_jsonb_root(&edited)
}

/// Insert JSON values at path(s) only when path does not already exist.
pub fn json_insert(input: &str, pairs: &[(&str, SqliteValue)]) -> Result<String> {
    let root = parse_json_text(input)?;
    let edited = edit_json_paths_value(&root, pairs, EditMode::Insert)?;
    encode_json_text("json edit encode failed", &edited)
}

/// JSONB variant of `json_insert`.
pub fn jsonb_insert(input: &str, pairs: &[(&str, SqliteValue)]) -> Result<Vec<u8>> {
    let root = parse_json_text(input)?;
    let edited = edit_json_paths_value(&root, pairs, EditMode::Insert)?;
    encode_jsonb_root(&edited)
}

/// Replace JSON values at path(s) only when path already exists.
pub fn json_replace(input: &str, pairs: &[(&str, SqliteValue)]) -> Result<String> {
    let root = parse_json_text(input)?;
    let edited = edit_json_paths_value(&root, pairs, EditMode::Replace)?;
    encode_json_text("json edit encode failed", &edited)
}

/// JSONB variant of `json_replace`.
pub fn jsonb_replace(input: &str, pairs: &[(&str, SqliteValue)]) -> Result<Vec<u8>> {
    let root = parse_json_text(input)?;
    let edited = edit_json_paths_value(&root, pairs, EditMode::Replace)?;
    encode_jsonb_root(&edited)
}

/// Remove JSON values at path(s). Array removals compact the array.
pub fn json_remove(input: &str, paths: &[&str]) -> Result<String> {
    let root = parse_json_text(input)?;
    let Some(edited) = json_remove_value(&root, paths)? else {
        return Ok("null".to_owned());
    };
    encode_json_text("json_remove encode failed", &edited)
}

/// JSONB variant of `json_remove`.
pub fn jsonb_remove(input: &str, paths: &[&str]) -> Result<Vec<u8>> {
    let root = parse_json_text(input)?;
    let Some(edited) = json_remove_value(&root, paths)? else {
        return encode_jsonb_root(&Value::Null);
    };
    encode_jsonb_root(&edited)
}

/// Apply RFC 7396 JSON Merge Patch.
pub fn json_patch(input: &str, patch: &str) -> Result<String> {
    let root = parse_json_text(input)?;
    let patch_value = parse_json_text(patch)?;
    let merged = json_patch_value(&root, &patch_value);
    encode_json_text("json_patch encode failed", &merged)
}

/// JSONB variant of `json_patch`.
pub fn jsonb_patch(input: &str, patch: &str) -> Result<Vec<u8>> {
    let root = parse_json_text(input)?;
    let patch_value = parse_json_text(patch)?;
    let merged = json_patch_value(&root, &patch_value);
    encode_jsonb_root(&merged)
}

/// Row shape produced by `json_each` and `json_tree`.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    json_each_value(&root, path)
}

/// Table-valued `json_each` over TEXT JSON or JSONB bytes.
pub fn json_each_blob(input: &[u8], path: Option<&str>) -> Result<Vec<JsonTableRow>> {
    let root = parse_json_input_blob(input)?;
    json_each_value(&root, path)
}

fn json_each_value(root: &Value, path: Option<&str>) -> Result<Vec<JsonTableRow>> {
    let base_path = path.unwrap_or("$");
    let target = match path {
        Some(path_expr) => resolve_path(root, path_expr)?,
        None => Some(root),
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
                    key: SqliteValue::Text(key.as_str().into()),
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
    json_tree_value(&root, path)
}

/// Table-valued `json_tree` over TEXT JSON or JSONB bytes.
pub fn json_tree_blob(input: &[u8], path: Option<&str>) -> Result<Vec<JsonTableRow>> {
    let root = parse_json_input_blob(input)?;
    json_tree_value(&root, path)
}

fn json_tree_value(root: &Value, path: Option<&str>) -> Result<Vec<JsonTableRow>> {
    let base_path = path.unwrap_or("$");
    let target = match path {
        Some(path_expr) => resolve_path(root, path_expr)?,
        None => Some(root),
    };
    let Some(target) = target else {
        return Ok(Vec::new());
    };

    let mut rows = Vec::new();
    let mut next_id = 0_i64;
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
        self.rows = json_each_value(&input, path)?;
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
        let Some(row) = self.rows.get(self.pos) else {
            ctx.set_value(SqliteValue::Null);
            return Ok(());
        };
        write_json_table_column(row, ctx, col);
        Ok(())
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.rows.get(self.pos).map_or(0, |row| row.id))
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
        self.rows = json_tree_value(&input, path)?;
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
        let Some(row) = self.rows.get(self.pos) else {
            ctx.set_value(SqliteValue::Null);
            return Ok(());
        };
        write_json_table_column(row, ctx, col);
        Ok(())
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.rows.get(self.pos).map_or(0, |row| row.id))
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
            // bd-t75hg: SQLite JSONB stores numeric payloads as the ASCII
            // text of the number (RFC-8259 text for INT/FLOAT), never as
            // fixed-width binary — a stock reader decodes binary payloads
            // as garbage digits. Integers (including u64 beyond i64::MAX,
            // which stock also keeps INT-typed) use decimal text; floats
            // use serde's canonical shortest text, which matches stock's
            // stored bytes whenever the source text was canonical.
            if number.is_i64() || number.is_u64() {
                append_jsonb_node(JSONB_INT_TYPE, number.to_string().as_bytes(), out)
            } else if let Some(float) = number.as_f64().filter(|value| value.is_finite()) {
                // `{:?}` is Rust's shortest round-trip float text ("1e300",
                // "-0.0", "0.1"), matching stock's stored bytes for
                // canonical-form inputs; `Number::to_string` inserts a
                // non-canonical '+' in exponents ("1e+300").
                append_jsonb_node(JSONB_FLOAT_TYPE, format!("{float:?}").as_bytes(), out)
            } else {
                // Non-finite REAL (+Inf/-Inf as 9.0e+999 / 9e999): store the
                // stock-rendered numeric text so the payload round-trips
                // (GH#212).
                append_jsonb_node(
                    JSONB_FLOAT_TYPE,
                    render_non_finite_number(number).as_bytes(),
                    out,
                )
            }
        }
        Value::String(text) => append_jsonb_string(text, out),
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
                append_jsonb_string(key, &mut payload)?;
                encode_jsonb_value(item, &mut payload)?;
            }
            append_jsonb_node(JSONB_OBJECT_TYPE, &payload, out)
        }
    }
}

/// Encode a string (value or object key) the way stock SQLite does: plain
/// `TEXT` (0x7) when the payload needs no JSON escaping, otherwise `TEXTJ`
/// (0x8) with the payload carrying RFC-8259 escape sequences. Emitting raw
/// bytes under `TEXT` for a string containing quotes/control characters
/// would make stock's `json()` render invalid JSON (bd-t75hg).
fn append_jsonb_string(text: &str, out: &mut Vec<u8>) -> Result<()> {
    if text
        .bytes()
        .all(|byte| byte != b'"' && byte != b'\\' && byte >= 0x20)
    {
        return append_jsonb_node(JSONB_TEXT_TYPE, text.as_bytes(), out);
    }
    let mut escaped = String::with_capacity(text.len() + 8);
    for ch in text.chars() {
        match ch {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\u{0008}' => escaped.push_str("\\b"),
            '\u{000C}' => escaped.push_str("\\f"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            control if (control as u32) < 0x20 => {
                escaped.push_str(&format!("\\u{:04x}", control as u32));
            }
            other => escaped.push(other),
        }
    }
    append_jsonb_node(JSONB_TEXT_JSON_TYPE, escaped.as_bytes(), out)
}

fn append_jsonb_node(node_type: u8, payload: &[u8], out: &mut Vec<u8>) -> Result<()> {
    // SQLite JSONB header: lower 4 bits = element type; upper 4 bits encode
    // the payload size DIRECTLY for sizes 0..=11, and the values 12/13/14/15
    // mean the size follows in 1/2/4/8 big-endian bytes (bd-t75hg — the
    // previous scheme wrote a length byte for every non-empty payload, which
    // stock misparses as payload content).
    let len = payload.len();
    if len <= 11 {
        let header = (u8::try_from(len).expect("len <= 11 fits in u8") << 4) | node_type;
        out.push(header);
    } else if let Ok(len8) = u8::try_from(len) {
        out.push((12 << 4) | node_type);
        out.push(len8);
    } else if let Ok(len16) = u16::try_from(len) {
        out.push((13 << 4) | node_type);
        out.extend_from_slice(&len16.to_be_bytes());
    } else if let Ok(len32) = u32::try_from(len) {
        out.push((14 << 4) | node_type);
        out.extend_from_slice(&len32.to_be_bytes());
    } else {
        let len64 = u64::try_from(len).map_err(|error| {
            FrankenError::function_error(format!("jsonb payload too large: {error}"))
        })?;
        out.push((15 << 4) | node_type);
        out.extend_from_slice(&len64.to_be_bytes());
    }
    out.extend_from_slice(payload);
    Ok(())
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
        JSONB_INT_TYPE | JSONB_INT5_TYPE => {
            // bd-t75hg: numeric payloads are the ASCII text of the number.
            // INT5 additionally admits JSON5 forms (hex, leading '+').
            let text = std::str::from_utf8(payload).map_err(|error| {
                FrankenError::function_error(format!("invalid JSONB integer payload: {error}"))
            })?;
            let normalized = if node_type == JSONB_INT5_TYPE {
                text.strip_prefix('+').unwrap_or(text)
            } else {
                text
            };
            let number = if node_type == JSONB_INT5_TYPE
                && let Some(hex) = normalized
                    .strip_prefix("0x")
                    .or_else(|| normalized.strip_prefix("0X"))
            {
                i64::from_str_radix(hex, 16).ok().map(Number::from)
            } else if let Ok(int) = normalized.parse::<i64>() {
                Some(Number::from(int))
            } else if let Ok(uint) = normalized.parse::<u64>() {
                Some(Number::from(uint))
            } else {
                // Stock keeps arbitrarily long integer text INT-typed; a
                // value beyond u64 degrades to its nearest double, which
                // preserves the numeric reading even though serde_json
                // cannot carry the exact digits.
                normalized.parse::<f64>().ok().and_then(Number::from_f64)
            };
            Value::Number(number.ok_or_else(|| {
                FrankenError::function_error("invalid JSONB integer payload text")
            })?)
        }
        JSONB_FLOAT_TYPE | JSONB_FLOAT5_TYPE => {
            let text = std::str::from_utf8(payload).map_err(|error| {
                FrankenError::function_error(format!("invalid JSONB float payload: {error}"))
            })?;
            let normalized = if node_type == JSONB_FLOAT5_TYPE {
                text.strip_prefix('+').unwrap_or(text)
            } else {
                text
            };
            let float = normalized.parse::<f64>().map_err(|error| {
                FrankenError::function_error(format!("invalid JSONB float payload text: {error}"))
            })?;
            if float.is_finite() {
                Value::Number(Number::from_f64(float).ok_or_else(|| {
                    FrankenError::function_error("invalid JSONB float payload")
                })?)
            } else {
                // Non-finite payload (e.g. `9e999` +Inf): preserve the raw
                // source text so the JSONB value round-trips (GH#212).
                json_number_from_raw(normalized)?
            }
        }
        JSONB_TEXT_TYPE | JSONB_TEXTRAW_TYPE => {
            let text = String::from_utf8(payload.to_vec()).map_err(|error| {
                FrankenError::function_error(format!("invalid JSONB text payload: {error}"))
            })?;
            Value::String(text)
        }
        JSONB_TEXT_JSON_TYPE | JSONB_TEXT5_TYPE => {
            // Escaped text: the payload carries RFC-8259 (TEXTJ) or JSON5
            // (TEXT5) escape sequences that must be resolved to the raw
            // string (bd-t75hg: previously returned verbatim, so stock-
            // written escaped strings decoded with literal backslashes).
            let text = std::str::from_utf8(payload).map_err(|error| {
                FrankenError::function_error(format!("invalid JSONB text payload: {error}"))
            })?;
            Value::String(decode_jsonb_escaped_text(
                text,
                node_type == JSONB_TEXT5_TYPE,
            )?)
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

/// Resolve TEXTJ/TEXT5 escape sequences to the raw string. TEXT5 adds the
/// JSON5 forms (`\'`, `\xNN`, escaped line continuations) on top of the
/// RFC-8259 set.
fn decode_jsonb_escaped_text(text: &str, json5: bool) -> Result<String> {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        let Some(escape) = chars.next() else {
            return Err(FrankenError::function_error(
                "invalid JSONB text escape: trailing backslash",
            ));
        };
        match escape {
            '"' => out.push('"'),
            '\\' => out.push('\\'),
            '/' => out.push('/'),
            'b' => out.push('\u{0008}'),
            'f' => out.push('\u{000C}'),
            'n' => out.push('\n'),
            'r' => out.push('\r'),
            't' => out.push('\t'),
            'u' => {
                let code: String = chars.by_ref().take(4).collect();
                if code.len() != 4 {
                    return Err(FrankenError::function_error(
                        "invalid JSONB \\u escape: truncated",
                    ));
                }
                let unit = u32::from_str_radix(&code, 16).map_err(|error| {
                    FrankenError::function_error(format!("invalid JSONB \\u escape: {error}"))
                })?;
                // Lone surrogates are replaced, matching lossy JSON readers.
                out.push(char::from_u32(unit).unwrap_or('\u{FFFD}'));
            }
            '\'' if json5 => out.push('\''),
            '0' if json5 => out.push('\u{0000}'),
            'x' if json5 => {
                let code: String = chars.by_ref().take(2).collect();
                let unit = u8::from_str_radix(&code, 16).map_err(|error| {
                    FrankenError::function_error(format!("invalid JSONB \\x escape: {error}"))
                })?;
                out.push(char::from(unit));
            }
            '\n' if json5 => {}
            other => {
                return Err(FrankenError::function_error(format!(
                    "invalid JSONB text escape: \\{other}"
                )));
            }
        }
    }
    Ok(out)
}

fn decode_jsonb_node(input: &[u8]) -> Result<(u8, &[u8], usize)> {
    if input.is_empty() {
        return Err(FrankenError::function_error("invalid JSONB: empty payload"));
    }

    let header = input[0];
    // SQLite JSONB header: lower 4 bits = element type; upper 4 bits are the
    // payload size itself for 0..=11, or 12/13/14/15 meaning the size follows
    // in 1/2/4/8 big-endian bytes (bd-t75hg).
    let node_type = header & 0x0f;
    let size_nibble = usize::from(header >> 4);
    let (len_size, direct_len) = match size_nibble {
        0..=11 => (0usize, Some(size_nibble)),
        12 => (1, None),
        13 => (2, None),
        14 => (4, None),
        _ => (8, None),
    };
    if !matches!(
        node_type,
        JSONB_NULL_TYPE
            | JSONB_TRUE_TYPE
            | JSONB_FALSE_TYPE
            | JSONB_INT_TYPE
            | JSONB_INT5_TYPE
            | JSONB_FLOAT_TYPE
            | JSONB_FLOAT5_TYPE
            | JSONB_TEXT_TYPE
            | JSONB_TEXT_JSON_TYPE
            | JSONB_TEXT5_TYPE
            | JSONB_TEXTRAW_TYPE
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
    let payload_len = if let Some(direct) = direct_len {
        direct
    } else {
        decode_jsonb_payload_len(&input[1..len_end])?
    };
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
    // SQLite JSONB header (bd-t75hg): lower 4 bits = element type; upper 4
    // bits are the payload size for 0..=11, or 12/13/14/15 meaning the size
    // follows in 1/2/4/8 big-endian bytes.
    let node_type = header & 0x0f;
    let size_nibble = usize::from(header >> 4);
    let (len_size, direct_len) = match size_nibble {
        0..=11 => (0usize, Some(size_nibble)),
        12 => (1, None),
        13 => (2, None),
        14 => (4, None),
        _ => (8, None),
    };
    if node_type > JSONB_OBJECT_TYPE {
        return false;
    }
    if input.len() < 1 + len_size {
        return false;
    }
    let len_end = 1 + len_size;
    let payload_len = if let Some(direct) = direct_len {
        direct
    } else {
        let Ok(decoded) = decode_jsonb_payload_len(&input[1..len_end]) else {
            return false;
        };
        decoded
    };
    1 + len_size + payload_len == input.len()
}

#[allow(clippy::too_many_lines)]
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
                if idx >= bytes.len() {
                    return Err(FrankenError::function_error(format!(
                        "invalid json path `{path}`: empty key segment"
                    )));
                }
                // ... rest of the dot case remains same ...

                if bytes[idx] == b'"' {
                    let quoted_start = idx;
                    idx += 1;
                    let mut escaped = false;
                    while idx < bytes.len() {
                        let byte = bytes[idx];
                        if escaped {
                            escaped = false;
                            idx += 1;
                            continue;
                        }
                        if byte == b'\\' {
                            escaped = true;
                            idx += 1;
                            continue;
                        }
                        if byte == b'"' {
                            break;
                        }
                        idx += 1;
                    }
                    if idx >= bytes.len() {
                        return Err(FrankenError::function_error(format!(
                            "invalid json path `{path}`: missing closing quote in key segment"
                        )));
                    }
                    let quoted_key = &path[quoted_start..=idx];
                    let key = serde_json::from_str::<String>(quoted_key).map_err(|error| {
                        FrankenError::function_error(format!(
                            "invalid json path `{path}` quoted key `{quoted_key}`: {error}"
                        ))
                    })?;
                    idx += 1; // closing quote
                    segments.push(PathSegment::Key(SmallText::from_string(key)));
                } else {
                    let start = idx;
                    while idx < bytes.len() && bytes[idx] != b'.' && bytes[idx] != b'[' {
                        idx += 1;
                    }
                    if start == idx {
                        return Err(FrankenError::function_error(format!(
                            "invalid json path `{path}`: empty key segment"
                        )));
                    }
                    segments.push(PathSegment::Key(SmallText::new(&path[start..idx])));
                }
            }
            b'[' => {
                idx += 1;
                let start = idx;
                let mut escaped = false;
                while idx < bytes.len() {
                    let byte = bytes[idx];
                    if escaped {
                        escaped = false;
                        idx += 1;
                        continue;
                    }
                    if byte == b'\\' {
                        escaped = true;
                        idx += 1;
                        continue;
                    }
                    if byte == b']' {
                        break;
                    }
                    idx += 1;
                }
                if idx >= bytes.len() {
                    return Err(FrankenError::function_error(format!(
                        "invalid json path `{path}`: missing closing `]`"
                    )));
                }
                let segment_text = &path[start..idx];
                idx += 1;

                if segment_text == "#" {
                    segments.push(PathSegment::Append);
                } else if let Some(rest) = segment_text.strip_prefix("#-") {
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

fn parse_quoted_path_key<'a>(path: &'a str, bytes: &[u8], idx: &mut usize) -> Result<Cow<'a, str>> {
    let quoted_start = *idx;
    *idx += 1;
    let mut escaped = false;
    let mut saw_escape = false;

    while *idx < bytes.len() {
        let byte = bytes[*idx];
        if escaped {
            escaped = false;
            *idx += 1;
            continue;
        }
        if byte == b'\\' {
            escaped = true;
            saw_escape = true;
            *idx += 1;
            continue;
        }
        if byte == b'"' {
            let raw = &path[(quoted_start + 1)..*idx];
            let key = if saw_escape {
                let quoted_key = &path[quoted_start..=*idx];
                Cow::Owned(serde_json::from_str::<String>(quoted_key).map_err(|error| {
                    FrankenError::function_error(format!(
                        "invalid json path `{path}` quoted key `{quoted_key}`: {error}"
                    ))
                })?)
            } else {
                Cow::Borrowed(raw)
            };
            *idx += 1;
            return Ok(key);
        }
        *idx += 1;
    }

    Err(FrankenError::function_error(format!(
        "invalid json path `{path}`: missing closing quote in key segment"
    )))
}

fn resolve_path<'a>(root: &'a Value, path: &str) -> Result<Option<&'a Value>> {
    let bytes = path.as_bytes();
    if bytes.first().copied() != Some(b'$') {
        return Err(FrankenError::function_error(format!(
            "invalid json path `{path}`: must start with `$`"
        )));
    }

    let mut idx = 1;
    let mut cursor = root;

    while idx < bytes.len() {
        match bytes[idx] {
            b'.' => {
                idx += 1;
                if idx >= bytes.len() {
                    return Err(FrankenError::function_error(format!(
                        "invalid json path `{path}`: empty key segment"
                    )));
                }

                let next = if bytes[idx] == b'"' {
                    let key = parse_quoted_path_key(path, bytes, &mut idx)?;
                    cursor.get(key.as_ref())
                } else {
                    let start = idx;
                    while idx < bytes.len() && bytes[idx] != b'.' && bytes[idx] != b'[' {
                        idx += 1;
                    }
                    if start == idx {
                        return Err(FrankenError::function_error(format!(
                            "invalid json path `{path}`: empty key segment"
                        )));
                    }
                    cursor.get(&path[start..idx])
                };

                let Some(resolved) = next else {
                    return Ok(None);
                };
                cursor = resolved;
            }
            b'[' => {
                idx += 1;
                let start = idx;
                let mut escaped = false;
                while idx < bytes.len() {
                    let byte = bytes[idx];
                    if escaped {
                        escaped = false;
                        idx += 1;
                        continue;
                    }
                    if byte == b'\\' {
                        escaped = true;
                        idx += 1;
                        continue;
                    }
                    if byte == b']' {
                        break;
                    }
                    idx += 1;
                }
                if idx >= bytes.len() {
                    return Err(FrankenError::function_error(format!(
                        "invalid json path `{path}`: missing closing `]`"
                    )));
                }
                let segment_text = &path[start..idx];
                idx += 1;

                if segment_text == "#" {
                    return Ok(None);
                }
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
                    let Some(array) = cursor.as_array() else {
                        return Ok(None);
                    };
                    if from_end > array.len() {
                        return Ok(None);
                    }
                    cursor = &array[array.len() - from_end];
                    continue;
                }

                let index = segment_text.parse::<usize>().map_err(|error| {
                    FrankenError::function_error(format!(
                        "invalid json path `{path}` array index `{segment_text}`: {error}"
                    ))
                })?;
                let Some(array) = cursor.as_array() else {
                    return Ok(None);
                };
                let Some(next) = array.get(index) else {
                    return Ok(None);
                };
                cursor = next;
            }
            _ => {
                return Err(FrankenError::function_error(format!(
                    "invalid json path `{path}` at byte offset {idx}"
                )));
            }
        }
    }

    Ok(Some(cursor))
}

fn append_object_path(base: &str, key: &str) -> String {
    // Quote keys containing special characters that would make the path
    // ambiguous (dots, brackets, quotes, whitespace).
    if key.contains(|c: char| matches!(c, '.' | '[' | ']' | '"' | '\'') || c.is_whitespace()) {
        format!("{base}.\"{}\"", key.replace('"', "\\\""))
    } else {
        format!("{base}.{key}")
    }
}

fn append_array_path(base: &str, index: usize) -> String {
    format!("{base}[{index}]")
}

fn json_value_column(value: &Value) -> Result<SqliteValue> {
    match value {
        // Canonical writer keeps floats stock-canonical and renders non-finite
        // numbers correctly under `arbitrary_precision` (GH#212).
        Value::Array(_) | Value::Object(_) => {
            encode_json_text("json table value encode failed", value)
                .map(|encoded| SqliteValue::Text(encoded.into()))
        }
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
                    SqliteValue::Text(child_key.as_str().into()),
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

fn parse_json_table_filter_args(args: &[SqliteValue]) -> Result<(Value, Option<&str>)> {
    let Some(input_arg) = args.first() else {
        return Err(FrankenError::function_error(
            "json table-valued functions require JSON input argument",
        ));
    };
    let input_value = match input_arg {
        SqliteValue::Text(input_text) => parse_json_text(input_text)?,
        SqliteValue::Blob(input_blob) => parse_json_input_blob(input_blob)?,
        // A bare SQL numeric is a JSON number (C SQLite convention), mirroring
        // the scalar `json_arg_value` path: json_each(1.5) yields a single row
        // with type='real', atom=1.5; json_tree(7) yields type='integer'. A
        // non-finite REAL renders as the numeric literal 9.0e+999 / -9.0e+999
        // (NaN -> null), so json_each(9e999) yields a single row (GH#212).
        SqliteValue::Integer(i) => Value::Number((*i).into()),
        SqliteValue::Float(f) => float_to_json(*f)?,
        _ => {
            return Err(FrankenError::function_error(
                "json table-valued input must be TEXT or BLOB JSON",
            ));
        }
    };

    let path = match args.get(1) {
        None | Some(SqliteValue::Null) => None,
        Some(SqliteValue::Text(path)) => Some(&**path),
        Some(_) => {
            return Err(FrankenError::function_error(
                "json table-valued PATH argument must be TEXT or NULL",
            ));
        }
    };

    Ok((input_value, path))
}
fn write_json_table_column(row: &JsonTableRow, ctx: &mut ColumnContext, col: i32) {
    let value = match col {
        0 => row.key.clone(),
        1 => row.value.clone(),
        2 => SqliteValue::Text(row.type_name.into()),
        3 => row.atom.clone(),
        4 => SqliteValue::Integer(row.id),
        5 => row.parent.clone(),
        6 => SqliteValue::Text(row.fullkey.as_str().into()),
        7 => SqliteValue::Text(row.path.as_str().into()),
        _ => SqliteValue::Null,
    };
    ctx.set_value(value);
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
                // `as_f64` parses the raw stored text, yielding +Inf/-Inf for a
                // non-finite literal such as `9e999` (GH#212: json_extract of a
                // non-finite JSON number reads back as a REAL Inf); fall back to
                // an explicit parse of the raw text if the accessor declines.
                let float = number
                    .as_f64()
                    .or_else(|| number.to_string().parse::<f64>().ok())
                    .unwrap_or(0.0);
                SqliteValue::Float(float)
            }
        }
        Value::String(text) => SqliteValue::Text(text.as_str().into()),
        Value::Array(_) | Value::Object(_) => {
            // Canonical writer (not `serde_json::to_string`): stock-canonical
            // floats and correct non-finite rendering under `arbitrary_precision`
            // (GH#212). Falls back to `null` on the (unreachable) encode error.
            let encoded =
                encode_json_text("json scalar encode", value).unwrap_or_else(|_| "null".to_owned());
            SqliteValue::Text(encoded.into())
        }
    }
}

/// Build a JSON `Value::Number` from raw numeric source text (e.g. `9.0e+999`).
///
/// Non-finite numbers cannot be held by a plain `serde_json` `Number`; the
/// crate's `arbitrary_precision` backing stores the raw text verbatim, so this
/// keeps the exact bytes that must round-trip (GH#212).
fn json_number_from_raw(raw: &str) -> Result<Value> {
    serde_json::from_str::<Number>(raw)
        .map(Value::Number)
        .map_err(|error| {
            FrankenError::function_error(format!("failed to build JSON number `{raw}`: {error}"))
        })
}

/// Render a non-finite JSON number (a `Value::Number` whose `as_f64` is not a
/// finite double) the way stock SQLite prints it.
///
/// SQLite copies a number's source text verbatim, so `json('[9e999]')` yields
/// `[9e999]` while a constructed `+Inf` yields `[9.0e+999]`. The crate's
/// `arbitrary_precision` backing cannot preserve the exact source bytes — it
/// canonicalizes a parsed exponent to `e+NNN` (`9e999` is stored as `9e+999`).
/// To reproduce stock's construct-vs-preserve forms for the literals JSON
/// callers actually produce, drop that inserted `+` when the mantissa has no
/// fractional part (`9e+999` -> `9e999`) while keeping it otherwise (the
/// constructed `9.0e+999`, or a source that already wrote `9.0e+999`). Exotic
/// forms that serde also normalizes (a fractional non-finite mantissa, or an
/// uppercase `E`) cannot be recovered and keep serde's canonical form (GH#212).
fn render_non_finite_number(number: &Number) -> String {
    let raw = number.to_string();
    let Some(exp) = raw.find(['e', 'E']) else {
        return raw;
    };
    if raw[..exp].contains('.') {
        return raw;
    }
    let mut rendered = String::with_capacity(raw.len());
    rendered.push_str(&raw[..=exp]);
    rendered.push_str(raw[exp + 1..].trim_start_matches('+'));
    rendered
}

/// Convert a SQL REAL to a JSON value exactly as stock SQLite does:
/// finite floats become JSON numbers; +Inf/-Inf render as the numeric literal
/// `9.0e+999` / `-9.0e+999`; NaN becomes JSON null (SQLite stores NaN as NULL).
fn float_to_json(f: f64) -> Result<Value> {
    if f.is_nan() {
        return Ok(Value::Null);
    }
    if f.is_infinite() {
        return json_number_from_raw(if f > 0.0 { "9.0e+999" } else { "-9.0e+999" });
    }
    Number::from_f64(f).map(Value::Number).ok_or_else(|| {
        FrankenError::function_error("failed to convert floating-point value to JSON")
    })
}

fn sqlite_to_json(value: &SqliteValue) -> Result<Value> {
    match value {
        SqliteValue::Null => Ok(Value::Null),
        SqliteValue::Integer(i) => Ok(Value::Number(Number::from(*i))),
        SqliteValue::Float(f) => float_to_json(*f),
        SqliteValue::Text(text) => Ok(Value::String(text.to_string())),
        SqliteValue::Blob(_) => Err(FrankenError::function_error("JSON cannot hold BLOB values")),
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
            // Scalars pretty-print identically to their minified form; the
            // canonical writer keeps floats stock-canonical and renders
            // non-finite numbers correctly under `arbitrary_precision` (GH#212).
            write_canonical_json_text(value, out)
        }
    }
}

fn apply_edit(root: &mut Value, segments: &[PathSegment], new_value: Value, mode: EditMode) {
    if segments.is_empty() {
        match mode {
            EditMode::Set | EditMode::Replace => *root = new_value,
            EditMode::Insert => {}
        }
        return;
    }
    if !matches!(root, Value::Object(_) | Value::Array(_)) {
        // Match SQLite JSON1 semantics: non-root path edits are no-ops when
        // the document root is a scalar value.
        return;
    }

    let original = root.clone();
    let (parent_segments, last) = segments.split_at(segments.len() - 1);
    let Some(last_segment) = last.first() else {
        return;
    };
    let Some(parent) = resolve_parent_for_edit(root, parent_segments, Some(last_segment), mode)
    else {
        *root = original;
        return;
    };

    let applied = match (parent, last_segment) {
        (Value::Object(object), PathSegment::Key(key)) => {
            let exists = object.contains_key(key.as_ref());
            match mode {
                EditMode::Set => {
                    object.insert(key.to_string(), new_value);
                    true
                }
                EditMode::Insert => {
                    if exists {
                        false
                    } else {
                        object.insert(key.to_string(), new_value);
                        true
                    }
                }
                EditMode::Replace => {
                    if exists {
                        object.insert(key.to_string(), new_value);
                        true
                    } else {
                        false
                    }
                }
            }
        }
        (Value::Array(array), PathSegment::Index(index)) => {
            apply_array_edit(array, *index, new_value, mode)
        }
        (Value::Array(array), PathSegment::Append) => {
            if matches!(mode, EditMode::Set | EditMode::Insert) {
                array.push(new_value);
                true
            } else {
                false
            }
        }
        (Value::Array(array), PathSegment::FromEnd(from_end)) => {
            if *from_end == 0 || *from_end > array.len() {
                false
            } else {
                let index = array.len() - *from_end;
                apply_array_edit(array, index, new_value, mode)
            }
        }
        _ => false,
    };

    if !applied {
        *root = original;
    }
}

fn apply_array_edit(
    array: &mut Vec<Value>,
    index: usize,
    new_value: Value,
    mode: EditMode,
) -> bool {
    if index > array.len() {
        return false;
    }

    if index == array.len() {
        if matches!(mode, EditMode::Set | EditMode::Insert) {
            array.push(new_value);
            return true;
        }
        return false;
    }

    match mode {
        EditMode::Set | EditMode::Replace => {
            array[index] = new_value;
            true
        }
        EditMode::Insert => false,
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
            object.remove(key.as_ref());
        }
        (Value::Array(array), PathSegment::Index(index)) if *index < array.len() => {
            array.remove(*index);
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
                let next = cursor.as_object_mut()?.get_mut(key.as_ref())?;
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
            PathSegment::Append => return None,
        }
    }

    Some(cursor)
}

fn resolve_parent_for_edit<'a>(
    root: &'a mut Value,
    segments: &[PathSegment],
    tail_hint: Option<&PathSegment>,
    mode: EditMode,
) -> Option<&'a mut Value> {
    fn scaffold_for_next_segment(next: Option<&PathSegment>) -> Value {
        match next {
            Some(PathSegment::Index(_) | PathSegment::Append | PathSegment::FromEnd(_)) => {
                Value::Array(Vec::new())
            }
            _ => Value::Object(Map::new()),
        }
    }

    let mut cursor = root;

    for (idx, segment) in segments.iter().enumerate() {
        let next_segment = segments.get(idx + 1).or_else(|| {
            if idx + 1 == segments.len() {
                tail_hint
            } else {
                None
            }
        });
        match segment {
            PathSegment::Key(key) => {
                if cursor.is_null() {
                    if matches!(mode, EditMode::Set | EditMode::Insert) {
                        *cursor = Value::Object(Map::new());
                    } else {
                        return None;
                    }
                }

                let object = match cursor {
                    Value::Object(o) => o,
                    _ => return None,
                };

                if !object.contains_key(key.as_ref()) {
                    if !matches!(mode, EditMode::Set | EditMode::Insert) {
                        return None;
                    }
                    object.insert(key.to_string(), scaffold_for_next_segment(next_segment));
                }
                cursor = object
                    .get_mut(key.as_ref())
                    .expect("just inserted or checked");
            }
            PathSegment::Index(index) => {
                if cursor.is_null() {
                    if matches!(mode, EditMode::Set | EditMode::Insert) {
                        *cursor = Value::Array(Vec::new());
                    } else {
                        return None;
                    }
                }
                let array = match cursor {
                    Value::Array(a) => a,
                    _ => return None,
                };
                if *index > array.len() {
                    return None;
                }
                if *index == array.len() {
                    if !matches!(mode, EditMode::Set | EditMode::Insert) {
                        return None;
                    }
                    array.push(scaffold_for_next_segment(next_segment));
                }
                cursor = array.get_mut(*index).expect("just pushed or checked");
            }
            PathSegment::Append => {
                if cursor.is_null() {
                    if matches!(mode, EditMode::Set | EditMode::Insert) {
                        *cursor = Value::Array(Vec::new());
                    } else {
                        return None;
                    }
                }
                let array = match cursor {
                    Value::Array(a) => a,
                    _ => return None,
                };
                if !matches!(mode, EditMode::Set | EditMode::Insert) {
                    return None;
                }
                array.push(scaffold_for_next_segment(next_segment));
                cursor = array.last_mut().expect("just pushed");
            }
            PathSegment::FromEnd(from_end) => {
                let array = match cursor {
                    Value::Array(a) => a,
                    _ => return None,
                };
                if *from_end == 0 || *from_end > array.len() {
                    return None;
                }
                let index = array.len() - *from_end;
                cursor = array.get_mut(index).expect("checked length");
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
                if let Some(prior) = target_map.get_mut(&key) {
                    let old_val = std::mem::replace(prior, Value::Null);
                    *prior = merge_patch(old_val, patch_value);
                } else {
                    target_map.insert(key, merge_patch(Value::Null, patch_value));
                }
            }

            Value::Object(target_map)
        }
        other => other,
    }
}

// ---------------------------------------------------------------------------
// Scalar function registration
// ---------------------------------------------------------------------------

fn invalid_arity(name: &str, expected: &str, got: usize) -> FrankenError {
    FrankenError::function_error(format!("{name} expects {expected}; got {got} argument(s)"))
}

fn text_arg<'a>(name: &str, args: &'a [SqliteValue], index: usize) -> Result<&'a str> {
    match args.get(index) {
        Some(SqliteValue::Text(text)) => Ok(&**text),
        Some(other) => Err(FrankenError::function_error(format!(
            "{name} argument {} must be TEXT, got {}",
            index + 1,
            other.typeof_str()
        ))),
        None => Err(FrankenError::function_error(format!(
            "{name} missing argument {}",
            index + 1
        ))),
    }
}

fn json_arg_value(name: &str, args: &[SqliteValue], index: usize) -> Result<Value> {
    match args.get(index) {
        Some(SqliteValue::Text(text)) => parse_json_text(text),
        Some(SqliteValue::Blob(bytes)) => parse_json_input_blob(bytes),
        // A bare SQL numeric value is interpreted as a JSON number (C SQLite's
        // JSON-argument convention): e.g. json_type(123) -> 'integer',
        // json_type(1.5) -> 'real', json_type(9e999) -> 'real'. A non-finite
        // REAL renders as the numeric literal 9.0e+999 / -9.0e+999 (NaN -> null),
        // matching stock (GH#212).
        Some(SqliteValue::Integer(i)) => Ok(Value::Number((*i).into())),
        Some(SqliteValue::Float(f)) => float_to_json(*f),
        Some(other) => Err(FrankenError::function_error(format!(
            "{name} argument {} must be TEXT or BLOB, got {}",
            index + 1,
            other.typeof_str()
        ))),
        None => Err(FrankenError::function_error(format!(
            "{name} missing argument {}",
            index + 1
        ))),
    }
}

fn optional_flags_arg(name: &str, args: &[SqliteValue], index: usize) -> Result<Option<u8>> {
    let Some(value) = args.get(index) else {
        return Ok(None);
    };
    let raw = value.to_integer();
    let flags = u8::try_from(raw).map_err(|_| {
        FrankenError::function_error(format!("{name} flags out of range for u8: {raw}"))
    })?;
    if !(1..=15).contains(&flags) {
        return Err(FrankenError::function_error(format!(
            "{name} FLAGS parameter must be between 1 and 15"
        )));
    }
    Ok(Some(flags))
}

fn usize_to_i64(name: &str, value: usize) -> Result<i64> {
    i64::try_from(value).map_err(|_| {
        FrankenError::function_error(format!("{name} result does not fit in i64: {value}"))
    })
}

fn collect_path_args<'a>(
    name: &str,
    args: &'a [SqliteValue],
    start: usize,
) -> Result<Vec<&'a str>> {
    let mut out = Vec::with_capacity(args.len().saturating_sub(start));
    for idx in start..args.len() {
        out.push(text_arg(name, args, idx)?);
    }
    Ok(out)
}

fn normalize_arrow_path_arg<'a>(
    name: &str,
    value: &'a SqliteValue,
    index: usize,
) -> Result<Cow<'a, str>> {
    match value {
        SqliteValue::Text(path) => {
            if path.starts_with('$') || path.is_empty() {
                Ok(Cow::Borrowed(&**path))
            } else {
                let quoted = serde_json::to_string(path).map_err(|error| {
                    FrankenError::function_error(format!(
                        "{name} argument {} key encoding failed: {error}",
                        index + 1
                    ))
                })?;
                Ok(Cow::Owned(format!("$.{quoted}")))
            }
        }
        SqliteValue::Integer(index_value) => {
            if *index_value >= 0 {
                Ok(Cow::Owned(format!("$[{index_value}]")))
            } else {
                Ok(Cow::Owned(format!("$[#-{}]", index_value.unsigned_abs())))
            }
        }
        other => Err(FrankenError::function_error(format!(
            "{name} argument {} must be TEXT or INTEGER, got {}",
            index + 1,
            other.typeof_str()
        ))),
    }
}

fn invoke_json_arrow(name: &str, args: &[SqliteValue], double_arrow: bool) -> Result<SqliteValue> {
    if args.len() != 2 {
        return Err(invalid_arity(name, "exactly 2 arguments", args.len()));
    }
    if args.iter().any(SqliteValue::is_null) {
        return Ok(SqliteValue::Null);
    }

    let input = json_arg_value(name, args, 0)?;
    let path = normalize_arrow_path_arg(name, &args[1], 1)?;
    if double_arrow {
        json_extract_value(&input, &[path.as_ref()])
    } else {
        json_arrow_value(&input, path.as_ref())
    }
}

fn collect_path_value_pairs(
    name: &str,
    args: &[SqliteValue],
    start: usize,
) -> Result<Vec<(String, SqliteValue)>> {
    let mut pairs = Vec::with_capacity((args.len().saturating_sub(start)) / 2);
    let mut idx = start;
    while idx < args.len() {
        if idx + 1 >= args.len() {
            break;
        }
        let path = text_arg(name, args, idx)?.to_owned();
        let value = args[idx + 1].clone();
        pairs.push((path, value));
        idx += 2;
    }
    Ok(pairs)
}

pub struct JsonFunc;

impl ScalarFunction for JsonFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.len() != 1 {
            return Err(invalid_arity(self.name(), "exactly 1 argument", args.len()));
        }
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        // TEXT input: stock json() minifies whitespace but preserves every
        // token of the source verbatim (number literals incl. exponent form,
        // string escapes like `\/`, duplicate keys). Parse only to validate,
        // then lexically minify the source rather than re-serialising a parsed
        // `serde_json::Value` (which normalises exponents, unescapes `\/`, and
        // drops duplicate keys) — bd-6b0pe / bd-p2xrc.
        if let SqliteValue::Text(text) = &args[0] {
            return Ok(SqliteValue::Text(json(text)?.into()));
        }
        let input = json_arg_value(self.name(), args, 0)?;
        let encoded = encode_json_text("json serialize failed", &input)?;
        Ok(SqliteValue::Text(encoded.into()))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &'static str {
        "json"
    }

    fn result_subtype(&self) -> Option<u32> {
        Some(JSON_SUBTYPE)
    }
}

pub struct JsonbFunc;

impl ScalarFunction for JsonbFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.len() != 1 {
            return Err(invalid_arity(self.name(), "exactly 1 argument", args.len()));
        }
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        let input = json_arg_value(self.name(), args, 0)?;
        let blob = encode_jsonb_root(&input)?;
        Ok(SqliteValue::Blob(Arc::from(blob.as_slice())))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &'static str {
        "jsonb"
    }
}

pub struct JsonValidFunc;

impl ScalarFunction for JsonValidFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if !(1..=2).contains(&args.len()) {
            return Err(invalid_arity(self.name(), "1 or 2 arguments", args.len()));
        }
        let flags = optional_flags_arg(self.name(), args, 1)?;
        let value = match &args[0] {
            SqliteValue::Null => return Ok(SqliteValue::Null),
            SqliteValue::Text(text) => json_valid(text, flags),
            SqliteValue::Blob(bytes) => json_valid_blob(bytes, flags),
            // C SQLite validates a bare SQL numeric via sqlite3_value_text():
            // it renders to its JSON numeric text and is checked against the
            // requested flags. So it is valid JSON/JSON5 (flags 1/2) but is NOT
            // a JSONB blob (flags 4/8 -> 0), and a non-finite REAL renders as
            // non-JSON text (e.g. json_valid(9e999) -> 0).
            SqliteValue::Integer(n) => json_valid(&n.to_string(), flags),
            SqliteValue::Float(f) if f.is_finite() => json_valid(&f.to_string(), flags),
            SqliteValue::Float(_) => 0,
        };
        Ok(SqliteValue::Integer(value))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "json_valid"
    }
}

pub struct JsonTypeFunc;

impl ScalarFunction for JsonTypeFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if !(1..=2).contains(&args.len()) {
            return Err(invalid_arity(self.name(), "1 or 2 arguments", args.len()));
        }
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        let input = json_arg_value(self.name(), args, 0)?;
        let path = if args.len() == 2 {
            if matches!(args[1], SqliteValue::Null) {
                return Ok(SqliteValue::Null);
            }
            Some(text_arg(self.name(), args, 1)?)
        } else {
            None
        };
        Ok(match json_type_value(&input, path)? {
            Some(kind) => SqliteValue::Text(SmallText::new(kind)),
            None => SqliteValue::Null,
        })
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "json_type"
    }
}

pub struct JsonExtractFunc;

impl ScalarFunction for JsonExtractFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.is_empty() {
            return Err(invalid_arity(
                self.name(),
                "at least 1 argument (json, path...)",
                args.len(),
            ));
        }
        if args.len() == 1 {
            return Ok(SqliteValue::Null);
        }
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        // SQLite returns NULL when any path argument is NULL.
        if args[1..].iter().any(|a| matches!(a, SqliteValue::Null)) {
            return Ok(SqliteValue::Null);
        }
        let input = json_arg_value(self.name(), args, 0)?;
        if args.len() == 2 {
            let path = text_arg(self.name(), args, 1)?;
            return json_extract_single_path(&input, path);
        }
        let paths = collect_path_args(self.name(), args, 1)?;
        json_extract_value(&input, &paths)
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "json_extract"
    }
}

pub struct JsonbExtractFunc;

impl ScalarFunction for JsonbExtractFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.is_empty() {
            return Err(invalid_arity(
                self.name(),
                "at least 1 argument (json, path...)",
                args.len(),
            ));
        }
        if args.len() == 1 {
            return Ok(SqliteValue::Null);
        }
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        if args[1..].iter().any(|a| matches!(a, SqliteValue::Null)) {
            return Ok(SqliteValue::Null);
        }
        let input = json_arg_value(self.name(), args, 0)?;
        let blob = if args.len() == 2 {
            let path = text_arg(self.name(), args, 1)?;
            encode_jsonb_root(&jsonb_extract_single_path_value(&input, path)?)
        } else {
            let paths = collect_path_args(self.name(), args, 1)?;
            jsonb_extract_value(&input, &paths)
        }?;
        Ok(SqliteValue::Blob(Arc::from(blob.as_slice())))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "jsonb_extract"
    }
}

pub struct JsonArrowFunc;

impl ScalarFunction for JsonArrowFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        invoke_json_arrow(self.name(), args, false)
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &'static str {
        "json_arrow"
    }

    fn result_subtype(&self) -> Option<u32> {
        Some(JSON_SUBTYPE)
    }
}

pub struct JsonDoubleArrowFunc;

impl ScalarFunction for JsonDoubleArrowFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        invoke_json_arrow(self.name(), args, true)
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &'static str {
        "json_double_arrow"
    }
}

pub struct JsonArrowOperatorFunc;

impl ScalarFunction for JsonArrowOperatorFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        invoke_json_arrow(self.name(), args, false)
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &'static str {
        "->"
    }

    fn result_subtype(&self) -> Option<u32> {
        Some(JSON_SUBTYPE)
    }
}

pub struct JsonDoubleArrowOperatorFunc;

impl ScalarFunction for JsonDoubleArrowOperatorFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        invoke_json_arrow(self.name(), args, true)
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &'static str {
        "->>"
    }
}

pub struct JsonArrayFunc;

impl ScalarFunction for JsonArrayFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        let s = json_array(args)?;
        Ok(SqliteValue::Text(SmallText::from_string(s)))
    }

    fn invoke_with_arg_subtypes(
        &self,
        args: &[SqliteValue],
        arg_subtypes: &[u32],
    ) -> Result<SqliteValue> {
        let s = json_array_with_subtypes(args, arg_subtypes)?;
        Ok(SqliteValue::Text(SmallText::from_string(s)))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "json_array"
    }

    fn result_subtype(&self) -> Option<u32> {
        Some(JSON_SUBTYPE)
    }
}

pub struct JsonbArrayFunc;

impl ScalarFunction for JsonbArrayFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        let blob = jsonb_array(args)?;
        Ok(SqliteValue::Blob(Arc::from(blob.as_slice())))
    }

    fn invoke_with_arg_subtypes(
        &self,
        args: &[SqliteValue],
        arg_subtypes: &[u32],
    ) -> Result<SqliteValue> {
        let s = json_array_with_subtypes(args, arg_subtypes)?;
        let blob = jsonb(&s)?;
        Ok(SqliteValue::Blob(Arc::from(blob.as_slice())))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "jsonb_array"
    }
}

pub struct JsonObjectFunc;

impl ScalarFunction for JsonObjectFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        let s = json_object(args)?;
        Ok(SqliteValue::Text(SmallText::from_string(s)))
    }

    fn invoke_with_arg_subtypes(
        &self,
        args: &[SqliteValue],
        arg_subtypes: &[u32],
    ) -> Result<SqliteValue> {
        let s = json_object_with_subtypes(args, arg_subtypes)?;
        Ok(SqliteValue::Text(SmallText::from_string(s)))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "json_object"
    }

    fn result_subtype(&self) -> Option<u32> {
        Some(JSON_SUBTYPE)
    }
}

pub struct JsonbObjectFunc;

impl ScalarFunction for JsonbObjectFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        let blob = jsonb_object(args)?;
        Ok(SqliteValue::Blob(Arc::from(blob.as_slice())))
    }

    fn invoke_with_arg_subtypes(
        &self,
        args: &[SqliteValue],
        arg_subtypes: &[u32],
    ) -> Result<SqliteValue> {
        let s = json_object_with_subtypes(args, arg_subtypes)?;
        let blob = jsonb(&s)?;
        Ok(SqliteValue::Blob(Arc::from(blob.as_slice())))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "jsonb_object"
    }
}

pub struct JsonQuoteFunc;

impl ScalarFunction for JsonQuoteFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.len() != 1 {
            return Err(invalid_arity(self.name(), "exactly 1 argument", args.len()));
        }
        let s = json_quote(&args[0])?;
        Ok(SqliteValue::Text(SmallText::from_string(s)))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &'static str {
        "json_quote"
    }
}

pub struct JsonSetFunc;

impl ScalarFunction for JsonSetFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.len() < 3 || args.len().is_multiple_of(2) {
            return Err(invalid_arity(
                self.name(),
                "an odd argument count >= 3 (json, path, value, ...)",
                args.len(),
            ));
        }
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        // SQLite returns NULL when any path argument is NULL.
        if args[1..]
            .iter()
            .step_by(2)
            .any(|a| matches!(a, SqliteValue::Null))
        {
            return Ok(SqliteValue::Null);
        }
        let input = json_arg_value(self.name(), args, 0)?;
        let pairs_owned = collect_path_value_pairs(self.name(), args, 1)?;
        let pairs = pairs_owned
            .iter()
            .map(|(path, value)| (path.as_str(), value.clone()))
            .collect::<Vec<_>>();
        let edited = edit_json_paths_value(&input, &pairs, EditMode::Set)?;
        let encoded = encode_json_text("json edit encode failed", &edited)?;
        Ok(SqliteValue::Text(encoded.into()))
    }

    fn invoke_with_arg_subtypes(
        &self,
        args: &[SqliteValue],
        arg_subtypes: &[u32],
    ) -> Result<SqliteValue> {
        if args.len() < 3 || args.len().is_multiple_of(2) {
            return Err(invalid_arity(
                self.name(),
                "an odd argument count >= 3 (json, path, value, ...)",
                args.len(),
            ));
        }
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        if args[1..]
            .iter()
            .step_by(2)
            .any(|a| matches!(a, SqliteValue::Null))
        {
            return Ok(SqliteValue::Null);
        }
        let input = json_arg_value(self.name(), args, 0)?;
        let edited = edit_json_paths_value_with_subtypes(
            self.name(),
            &input,
            args,
            arg_subtypes,
            1,
            EditMode::Set,
        )?;
        let encoded = encode_json_text("json edit encode failed", &edited)?;
        Ok(SqliteValue::Text(encoded.into()))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "json_set"
    }

    fn result_subtype(&self) -> Option<u32> {
        Some(JSON_SUBTYPE)
    }
}

pub struct JsonbSetFunc;

impl ScalarFunction for JsonbSetFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.len() < 3 || args.len().is_multiple_of(2) {
            return Err(invalid_arity(
                self.name(),
                "an odd argument count >= 3 (json, path, value, ...)",
                args.len(),
            ));
        }
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        if args[1..]
            .iter()
            .step_by(2)
            .any(|a| matches!(a, SqliteValue::Null))
        {
            return Ok(SqliteValue::Null);
        }
        let input = json_arg_value(self.name(), args, 0)?;
        let pairs_owned = collect_path_value_pairs(self.name(), args, 1)?;
        let pairs = pairs_owned
            .iter()
            .map(|(path, value)| (path.as_str(), value.clone()))
            .collect::<Vec<_>>();
        let edited = edit_json_paths_value(&input, &pairs, EditMode::Set)?;
        let blob = encode_jsonb_root(&edited)?;
        Ok(SqliteValue::Blob(Arc::from(blob.as_slice())))
    }

    fn invoke_with_arg_subtypes(
        &self,
        args: &[SqliteValue],
        arg_subtypes: &[u32],
    ) -> Result<SqliteValue> {
        if args.len() < 3 || args.len().is_multiple_of(2) {
            return Err(invalid_arity(
                self.name(),
                "an odd argument count >= 3 (json, path, value, ...)",
                args.len(),
            ));
        }
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        if args[1..]
            .iter()
            .step_by(2)
            .any(|a| matches!(a, SqliteValue::Null))
        {
            return Ok(SqliteValue::Null);
        }
        let input = json_arg_value(self.name(), args, 0)?;
        let edited = edit_json_paths_value_with_subtypes(
            self.name(),
            &input,
            args,
            arg_subtypes,
            1,
            EditMode::Set,
        )?;
        let blob = encode_jsonb_root(&edited)?;
        Ok(SqliteValue::Blob(Arc::from(blob.as_slice())))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "jsonb_set"
    }
}

pub struct JsonInsertFunc;

impl ScalarFunction for JsonInsertFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.len() < 3 || args.len().is_multiple_of(2) {
            return Err(invalid_arity(
                self.name(),
                "an odd argument count >= 3 (json, path, value, ...)",
                args.len(),
            ));
        }
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        // SQLite returns NULL when any path argument is NULL.
        if args[1..]
            .iter()
            .step_by(2)
            .any(|a| matches!(a, SqliteValue::Null))
        {
            return Ok(SqliteValue::Null);
        }
        let input = json_arg_value(self.name(), args, 0)?;
        let pairs_owned = collect_path_value_pairs(self.name(), args, 1)?;
        let pairs = pairs_owned
            .iter()
            .map(|(path, value)| (path.as_str(), value.clone()))
            .collect::<Vec<_>>();
        let edited = edit_json_paths_value(&input, &pairs, EditMode::Insert)?;
        let encoded = encode_json_text("json edit encode failed", &edited)?;
        Ok(SqliteValue::Text(encoded.into()))
    }

    fn invoke_with_arg_subtypes(
        &self,
        args: &[SqliteValue],
        arg_subtypes: &[u32],
    ) -> Result<SqliteValue> {
        if args.len() < 3 || args.len().is_multiple_of(2) {
            return Err(invalid_arity(
                self.name(),
                "an odd argument count >= 3 (json, path, value, ...)",
                args.len(),
            ));
        }
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        if args[1..]
            .iter()
            .step_by(2)
            .any(|a| matches!(a, SqliteValue::Null))
        {
            return Ok(SqliteValue::Null);
        }
        let input = json_arg_value(self.name(), args, 0)?;
        let edited = edit_json_paths_value_with_subtypes(
            self.name(),
            &input,
            args,
            arg_subtypes,
            1,
            EditMode::Insert,
        )?;
        let encoded = encode_json_text("json edit encode failed", &edited)?;
        Ok(SqliteValue::Text(encoded.into()))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "json_insert"
    }

    fn result_subtype(&self) -> Option<u32> {
        Some(JSON_SUBTYPE)
    }
}

pub struct JsonbInsertFunc;

impl ScalarFunction for JsonbInsertFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.len() < 3 || args.len().is_multiple_of(2) {
            return Err(invalid_arity(
                self.name(),
                "an odd argument count >= 3 (json, path, value, ...)",
                args.len(),
            ));
        }
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        if args[1..]
            .iter()
            .step_by(2)
            .any(|a| matches!(a, SqliteValue::Null))
        {
            return Ok(SqliteValue::Null);
        }
        let input = json_arg_value(self.name(), args, 0)?;
        let pairs_owned = collect_path_value_pairs(self.name(), args, 1)?;
        let pairs = pairs_owned
            .iter()
            .map(|(path, value)| (path.as_str(), value.clone()))
            .collect::<Vec<_>>();
        let edited = edit_json_paths_value(&input, &pairs, EditMode::Insert)?;
        let blob = encode_jsonb_root(&edited)?;
        Ok(SqliteValue::Blob(Arc::from(blob.as_slice())))
    }

    fn invoke_with_arg_subtypes(
        &self,
        args: &[SqliteValue],
        arg_subtypes: &[u32],
    ) -> Result<SqliteValue> {
        if args.len() < 3 || args.len().is_multiple_of(2) {
            return Err(invalid_arity(
                self.name(),
                "an odd argument count >= 3 (json, path, value, ...)",
                args.len(),
            ));
        }
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        if args[1..]
            .iter()
            .step_by(2)
            .any(|a| matches!(a, SqliteValue::Null))
        {
            return Ok(SqliteValue::Null);
        }
        let input = json_arg_value(self.name(), args, 0)?;
        let edited = edit_json_paths_value_with_subtypes(
            self.name(),
            &input,
            args,
            arg_subtypes,
            1,
            EditMode::Insert,
        )?;
        let blob = encode_jsonb_root(&edited)?;
        Ok(SqliteValue::Blob(Arc::from(blob.as_slice())))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "jsonb_insert"
    }
}

pub struct JsonReplaceFunc;

impl ScalarFunction for JsonReplaceFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.len() < 3 || args.len().is_multiple_of(2) {
            return Err(invalid_arity(
                self.name(),
                "an odd argument count >= 3 (json, path, value, ...)",
                args.len(),
            ));
        }
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        // SQLite returns NULL when any path argument is NULL.
        if args[1..]
            .iter()
            .step_by(2)
            .any(|a| matches!(a, SqliteValue::Null))
        {
            return Ok(SqliteValue::Null);
        }
        let input = json_arg_value(self.name(), args, 0)?;
        let pairs_owned = collect_path_value_pairs(self.name(), args, 1)?;
        let pairs = pairs_owned
            .iter()
            .map(|(path, value)| (path.as_str(), value.clone()))
            .collect::<Vec<_>>();
        let edited = edit_json_paths_value(&input, &pairs, EditMode::Replace)?;
        let encoded = encode_json_text("json edit encode failed", &edited)?;
        Ok(SqliteValue::Text(encoded.into()))
    }

    fn invoke_with_arg_subtypes(
        &self,
        args: &[SqliteValue],
        arg_subtypes: &[u32],
    ) -> Result<SqliteValue> {
        if args.len() < 3 || args.len().is_multiple_of(2) {
            return Err(invalid_arity(
                self.name(),
                "an odd argument count >= 3 (json, path, value, ...)",
                args.len(),
            ));
        }
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        if args[1..]
            .iter()
            .step_by(2)
            .any(|a| matches!(a, SqliteValue::Null))
        {
            return Ok(SqliteValue::Null);
        }
        let input = json_arg_value(self.name(), args, 0)?;
        let edited = edit_json_paths_value_with_subtypes(
            self.name(),
            &input,
            args,
            arg_subtypes,
            1,
            EditMode::Replace,
        )?;
        let encoded = encode_json_text("json edit encode failed", &edited)?;
        Ok(SqliteValue::Text(encoded.into()))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "json_replace"
    }

    fn result_subtype(&self) -> Option<u32> {
        Some(JSON_SUBTYPE)
    }
}

pub struct JsonbReplaceFunc;

impl ScalarFunction for JsonbReplaceFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.len() < 3 || args.len().is_multiple_of(2) {
            return Err(invalid_arity(
                self.name(),
                "an odd argument count >= 3 (json, path, value, ...)",
                args.len(),
            ));
        }
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        if args[1..]
            .iter()
            .step_by(2)
            .any(|a| matches!(a, SqliteValue::Null))
        {
            return Ok(SqliteValue::Null);
        }
        let input = json_arg_value(self.name(), args, 0)?;
        let pairs_owned = collect_path_value_pairs(self.name(), args, 1)?;
        let pairs = pairs_owned
            .iter()
            .map(|(path, value)| (path.as_str(), value.clone()))
            .collect::<Vec<_>>();
        let edited = edit_json_paths_value(&input, &pairs, EditMode::Replace)?;
        let blob = encode_jsonb_root(&edited)?;
        Ok(SqliteValue::Blob(Arc::from(blob.as_slice())))
    }

    fn invoke_with_arg_subtypes(
        &self,
        args: &[SqliteValue],
        arg_subtypes: &[u32],
    ) -> Result<SqliteValue> {
        if args.len() < 3 || args.len().is_multiple_of(2) {
            return Err(invalid_arity(
                self.name(),
                "an odd argument count >= 3 (json, path, value, ...)",
                args.len(),
            ));
        }
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        if args[1..]
            .iter()
            .step_by(2)
            .any(|a| matches!(a, SqliteValue::Null))
        {
            return Ok(SqliteValue::Null);
        }
        let input = json_arg_value(self.name(), args, 0)?;
        let edited = edit_json_paths_value_with_subtypes(
            self.name(),
            &input,
            args,
            arg_subtypes,
            1,
            EditMode::Replace,
        )?;
        let blob = encode_jsonb_root(&edited)?;
        Ok(SqliteValue::Blob(Arc::from(blob.as_slice())))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "jsonb_replace"
    }
}

pub struct JsonRemoveFunc;

impl ScalarFunction for JsonRemoveFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.is_empty() {
            return Err(invalid_arity(
                self.name(),
                "at least 1 argument (json [, path...])",
                args.len(),
            ));
        }
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        let input = json_arg_value(self.name(), args, 0)?;
        if args.len() == 1 {
            // With just the JSON argument, validate and return minified.
            let encoded = encode_json_text("json serialize failed", &input)?;
            return Ok(SqliteValue::Text(SmallText::from_string(encoded)));
        }
        // SQLite returns NULL when any path argument is NULL.
        if args[1..].iter().any(|a| matches!(a, SqliteValue::Null)) {
            return Ok(SqliteValue::Null);
        }
        let paths = collect_path_args(self.name(), args, 1)?;
        let Some(edited) = json_remove_value(&input, &paths)? else {
            return Ok(SqliteValue::Null);
        };
        let encoded = encode_json_text("json_remove encode failed", &edited)?;
        Ok(SqliteValue::Text(encoded.into()))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "json_remove"
    }
}

pub struct JsonbRemoveFunc;

impl ScalarFunction for JsonbRemoveFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.is_empty() {
            return Err(invalid_arity(
                self.name(),
                "at least 1 argument (json [, path...])",
                args.len(),
            ));
        }
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        let input = json_arg_value(self.name(), args, 0)?;
        if args.len() == 1 {
            let blob = encode_jsonb_root(&input)?;
            return Ok(SqliteValue::Blob(Arc::from(blob.as_slice())));
        }
        if args[1..].iter().any(|a| matches!(a, SqliteValue::Null)) {
            return Ok(SqliteValue::Null);
        }
        let paths = collect_path_args(self.name(), args, 1)?;
        let Some(edited) = json_remove_value(&input, &paths)? else {
            return Ok(SqliteValue::Null);
        };
        let blob = encode_jsonb_root(&edited)?;
        Ok(SqliteValue::Blob(Arc::from(blob.as_slice())))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "jsonb_remove"
    }
}

pub struct JsonPatchFunc;

impl ScalarFunction for JsonPatchFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.len() != 2 {
            return Err(invalid_arity(
                self.name(),
                "exactly 2 arguments",
                args.len(),
            ));
        }
        if matches!(args[0], SqliteValue::Null) || matches!(args[1], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        let input = json_arg_value(self.name(), args, 0)?;
        let patch = json_arg_value(self.name(), args, 1)?;
        let merged = json_patch_value(&input, &patch);
        let encoded = encode_json_text("json_patch encode failed", &merged)?;
        Ok(SqliteValue::Text(encoded.into()))
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &'static str {
        "json_patch"
    }
}

pub struct JsonbPatchFunc;

impl ScalarFunction for JsonbPatchFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.len() != 2 {
            return Err(invalid_arity(
                self.name(),
                "exactly 2 arguments",
                args.len(),
            ));
        }
        if matches!(args[0], SqliteValue::Null) || matches!(args[1], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        let input = json_arg_value(self.name(), args, 0)?;
        let patch = json_arg_value(self.name(), args, 1)?;
        let merged = json_patch_value(&input, &patch);
        let blob = encode_jsonb_root(&merged)?;
        Ok(SqliteValue::Blob(Arc::from(blob.as_slice())))
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &'static str {
        "jsonb_patch"
    }
}

pub struct JsonArrayLengthFunc;

impl ScalarFunction for JsonArrayLengthFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if !(1..=2).contains(&args.len()) {
            return Err(invalid_arity(self.name(), "1 or 2 arguments", args.len()));
        }
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        let input = json_arg_value(self.name(), args, 0)?;
        let path = if args.len() == 2 {
            if matches!(args[1], SqliteValue::Null) {
                return Ok(SqliteValue::Null);
            }
            Some(text_arg(self.name(), args, 1)?)
        } else {
            None
        };
        Ok(match json_array_length_value(&input, path)? {
            Some(len) => SqliteValue::Integer(usize_to_i64(self.name(), len)?),
            None => SqliteValue::Null,
        })
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "json_array_length"
    }
}

pub struct JsonErrorPositionFunc;

impl ScalarFunction for JsonErrorPositionFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.len() != 1 {
            return Err(invalid_arity(self.name(), "exactly 1 argument", args.len()));
        }
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        let position = match &args[0] {
            SqliteValue::Text(text) => json_error_position(text),
            SqliteValue::Blob(bytes) => json_error_position_blob(bytes),
            // A bare SQL numeric is valid JSON (a JSON number), so there is no
            // parse error: json_error_position(1) = json_error_position(1.5)
            // = json_error_position(9e999) = 0 (C SQLite convention; a
            // non-finite REAL renders as the numeric literal 9.0e+999, GH#212).
            SqliteValue::Integer(_) | SqliteValue::Float(_) => 0,
            other => {
                return Err(FrankenError::function_error(format!(
                    "{} argument 1 must be TEXT or BLOB, got {}",
                    self.name(),
                    other.typeof_str()
                )));
            }
        };
        Ok(SqliteValue::Integer(usize_to_i64(self.name(), position)?))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &'static str {
        "json_error_position"
    }
}

pub struct JsonPrettyFunc;

impl ScalarFunction for JsonPrettyFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if !(1..=2).contains(&args.len()) {
            return Err(invalid_arity(self.name(), "1 or 2 arguments", args.len()));
        }
        if matches!(args[0], SqliteValue::Null) {
            return Ok(SqliteValue::Null);
        }
        let input = json_arg_value(self.name(), args, 0)?;
        let indent = if args.len() == 2 {
            if matches!(args[1], SqliteValue::Null) {
                None
            } else {
                Some(text_arg(self.name(), args, 1)?)
            }
        } else {
            None
        };
        let s = json_pretty_value(&input, indent)?;
        Ok(SqliteValue::Text(SmallText::from_string(s)))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &'static str {
        "json_pretty"
    }
}

/// Register JSON1 scalar functions into a `FunctionRegistry`.
pub fn register_json_scalars(registry: &mut FunctionRegistry) {
    registry.register_scalar(JsonFunc);
    registry.register_scalar(JsonbFunc);
    registry.register_scalar(JsonValidFunc);
    registry.register_scalar(JsonTypeFunc);
    registry.register_scalar(JsonExtractFunc);
    registry.register_scalar(JsonbExtractFunc);
    registry.register_scalar(JsonArrowFunc);
    registry.register_scalar(JsonDoubleArrowFunc);
    registry.register_scalar(JsonArrowOperatorFunc);
    registry.register_scalar(JsonDoubleArrowOperatorFunc);
    registry.register_scalar(JsonArrayFunc);
    registry.register_scalar(JsonbArrayFunc);
    registry.register_scalar(JsonObjectFunc);
    registry.register_scalar(JsonbObjectFunc);
    registry.register_scalar(JsonQuoteFunc);
    registry.register_scalar(JsonSetFunc);
    registry.register_scalar(JsonbSetFunc);
    registry.register_scalar(JsonInsertFunc);
    registry.register_scalar(JsonbInsertFunc);
    registry.register_scalar(JsonReplaceFunc);
    registry.register_scalar(JsonbReplaceFunc);
    registry.register_scalar(JsonRemoveFunc);
    registry.register_scalar(JsonbRemoveFunc);
    registry.register_scalar(JsonPatchFunc);
    registry.register_scalar(JsonbPatchFunc);
    registry.register_scalar(JsonArrayLengthFunc);
    registry.register_scalar(JsonErrorPositionFunc);
    registry.register_scalar(JsonPrettyFunc);
}

#[cfg(test)]
mod tests {
    use super::*;
    use fsqlite_func::FunctionRegistry;

    #[allow(dead_code)]
    #[derive(Debug)]
    struct JsonTableRowStructure {
        key: String,
        value: String,
        type_name: &'static str,
        atom: String,
        id: i64,
        parent: String,
        fullkey: String,
        path: String,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct JsonTableJsonbStructure {
        each: Vec<JsonTableRowStructure>,
        tree: Vec<JsonTableRowStructure>,
    }

    fn json_value_structure(value: &SqliteValue) -> String {
        format!("{value:?}")
    }

    fn json_table_row_structure(row: &JsonTableRow) -> JsonTableRowStructure {
        JsonTableRowStructure {
            key: json_value_structure(&row.key),
            value: json_value_structure(&row.value),
            type_name: row.type_name,
            atom: json_value_structure(&row.atom),
            id: row.id,
            parent: json_value_structure(&row.parent),
            fullkey: row.fullkey.clone(),
            path: row.path.clone(),
        }
    }

    #[test]
    fn test_register_json_scalars_registers_core_functions() {
        let mut registry = FunctionRegistry::new();
        register_json_scalars(&mut registry);

        for name in [
            "json",
            "jsonb",
            "json_valid",
            "json_type",
            "json_extract",
            "jsonb_extract",
            "json_arrow",
            "json_double_arrow",
            "->",
            "->>",
            "json_set",
            "jsonb_set",
            "json_remove",
            "jsonb_remove",
            "json_array",
            "jsonb_array",
            "json_object",
            "jsonb_object",
            "json_quote",
            "json_patch",
            "jsonb_patch",
        ] {
            assert!(
                registry.contains_scalar(name),
                "missing registration for {name}"
            );
        }
    }

    #[test]
    fn test_registered_json_extract_scalar_executes() {
        let mut registry = FunctionRegistry::new();
        register_json_scalars(&mut registry);
        let func = registry
            .find_scalar("json_extract", 2)
            .expect("json_extract should be registered");
        let out = func
            .invoke(&[
                SqliteValue::Text(SmallText::from_string(r#"{"a":1,"b":[2,3]}"#)),
                SqliteValue::Text(SmallText::from_string("$.b[1]")),
            ])
            .unwrap();
        assert_eq!(out, SqliteValue::Integer(3));
    }

    // Conformance guard: pins a batch of JSON scalar behaviors to the C SQLite
    // oracle (verified against sqlite3 3.46.1) and reports every divergence in
    // one run. Anchors the bd-p2xrc fix — stock json() minifies whitespace but
    // never rewrites a number literal, so parsed reals keep their exact source
    // text (json('1.50') -> '1.50', json('0.3333333333333333') round-trips,
    // json('1.5e+3') keeps its exponent) while a real constructed from a SQL
    // value renders canonically (json_array(1.0) -> '[1.0]'). Residual
    // unsigned/uppercase-exponent exactness (json('1e2') -> '1e+2') is bd-6b0pe.
    #[test]
    fn test_json_scalar_conformance_oracle_bd_p2xrc() {
        let mut registry = FunctionRegistry::new();
        register_json_scalars(&mut registry);
        let t = |s: &str| SqliteValue::Text(SmallText::from_string(s.to_string()));
        let call = |name: &str, args: Vec<SqliteValue>| -> SqliteValue {
            registry
                .find_scalar(name, args.len() as i32)
                .unwrap_or_else(|| panic!("missing scalar {name}/{}", args.len()))
                .invoke(&args)
                .unwrap_or_else(|e| panic!("{name} invoke failed: {e:?}"))
        };
        let as_text = |v: &SqliteValue| -> String {
            match v {
                SqliteValue::Text(s) => s.as_ref().to_string(),
                SqliteValue::Integer(i) => i.to_string(),
                SqliteValue::Float(f) => format!("{f}"),
                SqliteValue::Null => "NULL".to_string(),
                other => format!("{other:?}"),
            }
        };
        let mut fails: Vec<String> = Vec::new();
        macro_rules! ck {
            ($label:expr, $got:expr, $expect:expr) => {{
                let g = as_text(&$got);
                if g != $expect {
                    fails.push(format!("{}: got {:?} expected {:?}", $label, g, $expect));
                }
            }};
        }
        // JSON-string / text / integer results (exact, version-independent)
        ck!("json_min", call("json", vec![t(" { \"a\" : 1 , \"b\" : [ 1,2 ] } ")]), "{\"a\":1,\"b\":[1,2]}");
        ck!("json_type", call("json_type", vec![t("{\"a\":1}")]), "object");
        ck!("json_type_path", call("json_type", vec![t("{\"a\":[1,2]}"), t("$.a")]), "array");
        ck!("json_type_int", call("json_type", vec![t("123")]), "integer");
        ck!("json_type_real", call("json_type", vec![t("1.5")]), "real");
        ck!("json_type_true", call("json_type", vec![t("true")]), "true");
        ck!("json_type_null", call("json_type", vec![t("null")]), "null");
        ck!("jx_obj", call("json_extract", vec![t("{\"a\":1,\"b\":2}"), t("$.a")]), "1");
        ck!("jx_multi", call("json_extract", vec![t("{\"a\":1,\"b\":2}"), t("$.a"), t("$.b")]), "[1,2]");
        ck!("jx_whole", call("json_extract", vec![t("{\"a\":1}"), t("$")]), "{\"a\":1}");
        ck!("jx_text", call("json_extract", vec![t("{\"a\":\"x\"}"), t("$.a")]), "x");
        ck!("jx_arr", call("json_extract", vec![t("[10,20,30]"), t("$[1]")]), "20");
        ck!("jx_neg", call("json_extract", vec![t("[10,20,30]"), t("$[#-1]")]), "30");
        ck!("jx_nested_obj", call("json_extract", vec![t("{\"a\":{\"b\":9}}"), t("$.a")]), "{\"b\":9}");
        ck!("arrow", call("->", vec![t("{\"a\":{\"b\":5}}"), t("$.a")]), "{\"b\":5}");
        ck!("arrow2", call("->>", vec![t("{\"a\":{\"b\":5}}"), t("$.a.b")]), "5");
        ck!("arrow_str", call("->", vec![t("{\"a\":\"hi\"}"), t("$.a")]), "\"hi\"");
        ck!("arrow2_str", call("->>", vec![t("{\"a\":\"hi\"}"), t("$.a")]), "hi");
        ck!("json_array", call("json_array", vec![SqliteValue::Integer(1), t("x"), SqliteValue::Null, SqliteValue::Float(2.5)]), "[1,\"x\",null,2.5]");
        ck!("json_object", call("json_object", vec![t("a"), SqliteValue::Integer(1), t("b"), t("y")]), "{\"a\":1,\"b\":\"y\"}");
        ck!("json_object_dup", call("json_object", vec![t("a"), SqliteValue::Integer(1), t("a"), SqliteValue::Integer(2)]), "{\"a\":1,\"a\":2}");
        ck!("json_quote_num", call("json_quote", vec![SqliteValue::Float(3.0)]), "3.0");
        ck!("json_quote_str", call("json_quote", vec![t("a\"b")]), "\"a\\\"b\"");
        ck!("json_valid_ok", call("json_valid", vec![t("{\"a\":1}")]), "1");
        ck!("json_valid_bad", call("json_valid", vec![t("{\"a\":}")]), "0");
        ck!("json_valid_trail", call("json_valid", vec![t("{} ")]), "1");
        ck!("json_arrlen", call("json_array_length", vec![t("[1,2,3]")]), "3");
        ck!("json_arrlen_path", call("json_array_length", vec![t("{\"a\":[1,2,3,4]}"), t("$.a")]), "4");
        ck!("json_set", call("json_set", vec![t("{\"a\":1}"), t("$.a"), SqliteValue::Integer(99), t("$.b"), SqliteValue::Integer(2)]), "{\"a\":99,\"b\":2}");
        ck!("json_insert_exist", call("json_insert", vec![t("{\"a\":1}"), t("$.a"), SqliteValue::Integer(99)]), "{\"a\":1}");
        ck!("json_replace_miss", call("json_replace", vec![t("{\"a\":1}"), t("$.b"), SqliteValue::Integer(2)]), "{\"a\":1}");
        ck!("json_remove", call("json_remove", vec![t("{\"a\":1,\"b\":2}"), t("$.a")]), "{\"b\":2}");
        ck!("json_patch", call("json_patch", vec![t("{\"a\":1,\"b\":2}"), t("{\"b\":null,\"c\":3}")]), "{\"a\":1,\"c\":3}");
        ck!("json_bignum", call("json", vec![t("{\"a\":12345678901234567890}")]), "{\"a\":12345678901234567890}");
        ck!("json_neg_zero", call("json_extract", vec![t("[-0]"), t("$[0]")]), "0");
        // ── deeper batch ──
        ck!("jx_true", call("json_extract", vec![t("{\"a\":true}"), t("$.a")]), "1");
        ck!("jx_false", call("json_extract", vec![t("{\"a\":false}"), t("$.a")]), "0");
        ck!("jx_json_null", call("json_extract", vec![t("{\"a\":null}"), t("$.a")]), "NULL");
        ck!("jx_deep", call("json_extract", vec![t("{\"a\":{\"b\":{\"c\":7}}}"), t("$.a.b.c")]), "7");
        ck!("jx_quoted_key", call("json_extract", vec![t("{\"a b\":1}"), t("$.\"a b\"")]), "1");
        ck!("jx_i64max", call("json_extract", vec![t("[9223372036854775807]"), t("$[0]")]), "9223372036854775807");
        ck!("arr_of_jsontext", call("json_array", vec![t("[1,2]")]), "[\"[1,2]\"]");
        ck!("arr_of_real", call("json_array", vec![SqliteValue::Float(1.0)]), "[1.0]");
        ck!("arr_of_real2", call("json_array", vec![SqliteValue::Float(2.50)]), "[2.5]");
        ck!("obj_val_real", call("json_object", vec![t("a"), SqliteValue::Float(1.0)]), "{\"a\":1.0}");
        ck!("json_scalar_int", call("json", vec![t("5")]), "5");
        ck!("json_scalar_str", call("json", vec![t("\"hi\"")]), "\"hi\"");
        ck!("json_scalar_real", call("json", vec![t("1.50")]), "1.50");
        ck!("json_num_1500", call("json", vec![t("1.500")]), "1.500");
        ck!("json_num_eplus", call("json", vec![t("1.5e+3")]), "1.5e+3");
        ck!("json_num_eneg", call("json", vec![t("1e-7")]), "1e-7");
        ck!("json_num_leadzero", call("json", vec![t("0.10")]), "0.10");
        ck!("json_num_neg", call("json", vec![t("-1.50")]), "-1.50");
        ck!("json_num_neg0real", call("json", vec![t("-0.0")]), "-0.0");
        ck!("json_num_16dig", call("json", vec![t("0.3333333333333333")]), "0.3333333333333333");
        ck!("json_num_obj", call("json", vec![t("{\"a\":1.50,\"b\":2.0}")]), "{\"a\":1.50,\"b\":2.0}");
        ck!("json_num_arr_ws", call("json", vec![t("[1.50, 2.30]")]), "[1.50,2.30]");
        ck!("json_pretty", call("json_pretty", vec![t("{\"a\":1,\"b\":[2,3]}")]), "{\n    \"a\": 1,\n    \"b\": [\n        2,\n        3\n    ]\n}");
        ck!("json_err_pos_ok", call("json_error_position", vec![t("{\"a\":1}")]), "0");
        ck!("json_err_pos_bad", call("json_error_position", vec![t("{\"a\":}")]), "6");
        ck!("json_valid_2arg1", call("json_valid", vec![t("{\"a\":1}"), SqliteValue::Integer(1)]), "1");
        ck!("json_valid_2arg6", call("json_valid", vec![t("{\"a\":1}"), SqliteValue::Integer(6)]), "1");
        ck!("json_valid_5x", call("json_valid", vec![t("{\"x\":"), SqliteValue::Integer(6)]), "0");
        ck!("arrow_int_key", call("->", vec![t("[1,2,3]"), SqliteValue::Integer(0)]), "1");
        ck!("arrow2_int_key", call("->>", vec![t("[1,2,3]"), SqliteValue::Integer(2)]), "3");
        ck!("json_set_create", call("json_set", vec![t("{\"a\":1}"), t("$.b.c"), SqliteValue::Integer(5)]), "{\"a\":1,\"b\":{\"c\":5}}");
        ck!("json_set_append", call("json_set", vec![t("{\"a\":[1,2]}"), t("$.a[#]"), SqliteValue::Integer(3)]), "{\"a\":[1,2,3]}");
        ck!("json_arrlen_obj", call("json_array_length", vec![t("{\"a\":1}")]), "0");
        ck!("json_remove_arr_idx", call("json_remove", vec![t("[1,2,3]"), t("$[1]")]), "[1,3]");
        ck!("json_insert_arr", call("json_insert", vec![t("[1,2]"), t("$[#]"), SqliteValue::Integer(3)]), "[1,2,3]");
        ck!("json_quote_null", call("json_quote", vec![SqliteValue::Null]), "null");
        ck!("json_obj_num_key", call("json_object", vec![t("1"), SqliteValue::Integer(2)]), "{\"1\":2}");

        // Float results: compare numerically (float→text is oracle-version-sensitive)
        let mut ck_num = |label: &str, got: SqliteValue, expect: f64| {
            match got {
                SqliteValue::Float(f) if (f - expect).abs() < 1e-9 => {}
                SqliteValue::Integer(i) if (i as f64 - expect).abs() < 1e-9 => {}
                other => fails.push(format!("{label}: got {other:?} expected ~{expect}")),
            }
        };
        ck_num("json_deep_num", call("json_extract", vec![t("{\"a\":1.50}"), t("$.a")]), 1.5);
        ck_num("json_e_notation", call("json_extract", vec![t("[1e3]"), t("$[0]")]), 1000.0);

        assert!(fails.is_empty(), "json oracle divergences:\n{}", fails.join("\n"));
    }

    #[test]
    fn test_json_text_minify_preserves_source_bd_6b0pe() {
        // Stock json() on TEXT input strips only insignificant whitespace and
        // preserves every token of the source verbatim (verified vs sqlite3
        // 3.46.1). Round-tripping through serde_json::Value used to normalise
        // exponents (1e2 -> 1e+2), unescape `\/`, and drop duplicate keys.
        for (input, expect) in [
            // exponent notation preserved exactly (bd-6b0pe)
            ("1e2", "1e2"),
            ("1E2", "1E2"),
            ("1e+2", "1e+2"),
            ("1e02", "1e02"),
            ("[1e+2, 1E2, 1e2]", "[1e+2,1E2,1e2]"),
            (r#"{"a":1.5E+3}"#, r#"{"a":1.5E+3}"#),
            // mantissa/precision still preserved (bd-p2xrc)
            ("1.50", "1.50"),
            ("2.500000", "2.500000"),
            // string escapes preserved verbatim, not canonicalised
            (r#"{"a":"a\/b"}"#, r#"{"a":"a\/b"}"#),
            (r#"["a\tb"]"#, r#"["a\tb"]"#),
            // in-string whitespace kept; inter-token whitespace stripped
            (r#"[ "a  b" , 1 ]"#, r#"["a  b",1]"#),
            (r#"{ "a" : [ 1 , 2 ] , "b" : "c" }"#, r#"{"a":[1,2],"b":"c"}"#),
            // duplicate object keys preserved (stock does not dedup on parse)
            (r#"{"a":1,"a":2}"#, r#"{"a":1,"a":2}"#),
            (r#"{"x":1e2,"x":3e4}"#, r#"{"x":1e2,"x":3e4}"#),
        ] {
            assert_eq!(
                json(input).unwrap(),
                expect,
                "json({input:?}) must minify whitespace but preserve the source verbatim"
            );
        }
        // Invalid JSON is still rejected (validation runs before minification).
        assert!(json("{\"a\":}").is_err());
        assert!(json("[1,]").is_err());
    }

    #[test]
    fn test_json_blob_render_uses_stock_float_format_bd_p2xrc_revert() {
        // json(BLOB) decodes JSONB (numeric payloads are ASCII text) and
        // re-renders. Floats must use stock's shortest form (`{float:?}`),
        // NOT the arbitrary_precision `to_string()` which inserts a '+' into
        // the exponent ("1e300" -> "1e+300") and breaks byte-compatible JSONB
        // round-trip through stock sqlite3 (the bd-p2xrc-revert regression
        // that RED'd the release gate). Text-input preservation stays in
        // minify_json_text (bd-6b0pe) and is unaffected.
        let mut registry = FunctionRegistry::new();
        register_json_scalars(&mut registry);
        let jsonb_of = |s: &str| -> Vec<u8> {
            match registry
                .find_scalar("jsonb", 1)
                .unwrap()
                .invoke(&[SqliteValue::Text(SmallText::from_string(s.to_string()))])
                .unwrap()
            {
                SqliteValue::Blob(b) => b.to_vec(),
                other => panic!("jsonb not blob: {other:?}"),
            }
        };
        let json_of_blob = |bytes: Vec<u8>| -> String {
            match registry
                .find_scalar("json", 1)
                .unwrap()
                .invoke(&[SqliteValue::Blob(Arc::from(bytes.as_slice()))])
                .unwrap()
            {
                SqliteValue::Text(t) => t.as_ref().to_string(),
                other => panic!("json not text: {other:?}"),
            }
        };
        assert_eq!(json_of_blob(jsonb_of("1e300")), "1e300");
        assert_eq!(json_of_blob(jsonb_of("-0.0")), "-0.0");
        assert_eq!(
            json_of_blob(jsonb_of("[1.5,-0.0,1e300,9223372036854775807]")),
            "[1.5,-0.0,1e300,9223372036854775807]"
        );
    }

    #[test]
    fn test_registered_json_arrow_scalar_normalizes_label_shorthand() {
        let mut registry = FunctionRegistry::new();
        register_json_scalars(&mut registry);
        let func = registry
            .find_scalar("json_arrow", 2)
            .expect("json_arrow should be registered");
        let out = func
            .invoke(&[
                SqliteValue::Text(SmallText::from_string(r#"{"a.b":1,"a":{"b":2}}"#)),
                SqliteValue::Text(SmallText::from_string("a.b")),
            ])
            .expect("json_arrow should normalize bare labels");
        assert_eq!(out, SqliteValue::Text(SmallText::from_string("1")));
        assert_eq!(func.result_subtype(), Some(JSON_SUBTYPE));

        let operator = registry
            .find_scalar("->", 2)
            .expect("JSON arrow operator should be registered");
        assert_eq!(operator.result_subtype(), Some(JSON_SUBTYPE));
    }

    #[test]
    fn test_registered_json_double_arrow_scalar_normalizes_integer_shorthand() {
        let mut registry = FunctionRegistry::new();
        register_json_scalars(&mut registry);
        let func = registry
            .find_scalar("json_double_arrow", 2)
            .expect("json_double_arrow should be registered");
        let out = func
            .invoke(&[
                SqliteValue::Text(SmallText::from_string("[10,20,30]")),
                SqliteValue::Integer(1),
            ])
            .expect("json_double_arrow should normalize integer indexes");
        assert_eq!(out, SqliteValue::Integer(20));
    }

    #[test]
    fn test_registered_json_set_scalar_executes() {
        let mut registry = FunctionRegistry::new();
        register_json_scalars(&mut registry);
        let func = registry
            .find_scalar("json_set", 3)
            .expect("json_set should be registered");
        let out = func
            .invoke(&[
                SqliteValue::Text(SmallText::from_string(r#"{"a":1}"#)),
                SqliteValue::Text(SmallText::from_string("$.b")),
                SqliteValue::Integer(2),
            ])
            .unwrap();
        assert_eq!(
            out,
            SqliteValue::Text(SmallText::from_string(r#"{"a":1,"b":2}"#))
        );
    }

    #[test]
    fn test_registered_jsonb_scalar_executes() -> Result<()> {
        let mut registry = FunctionRegistry::new();
        register_json_scalars(&mut registry);
        let func = registry
            .find_scalar("jsonb", 1)
            .expect("jsonb should be registered");
        let out = func
            .invoke(&[SqliteValue::Text(SmallText::from_string(r#"{"a":[1,2]}"#))])
            .expect("jsonb should encode to JSONB");
        let SqliteValue::Blob(blob) = out else {
            return Err(FrankenError::function_error("jsonb should return BLOB"));
        };
        assert_eq!(json_from_jsonb(&blob).unwrap(), r#"{"a":[1,2]}"#);
        Ok(())
    }

    #[test]
    fn test_registered_json_extract_accepts_jsonb_blob_input() {
        let mut registry = FunctionRegistry::new();
        register_json_scalars(&mut registry);
        let func = registry
            .find_scalar("json_extract", 2)
            .expect("json_extract should be registered");
        let input = jsonb(r#"{"a":{"b":7}}"#).unwrap();
        let out = func
            .invoke(&[
                SqliteValue::Blob(Arc::from(input)),
                SqliteValue::Text(SmallText::from_string("$.a.b")),
            ])
            .expect("json_extract should accept JSONB blob input");
        assert_eq!(out, SqliteValue::Integer(7));
    }

    #[test]
    fn test_registered_jsonb_set_accepts_jsonb_blob_input() -> Result<()> {
        let mut registry = FunctionRegistry::new();
        register_json_scalars(&mut registry);
        let func = registry
            .find_scalar("jsonb_set", 3)
            .expect("jsonb_set should be registered");
        let input = jsonb(r#"{"a":1}"#).unwrap();
        let out = func
            .invoke(&[
                SqliteValue::Blob(Arc::from(input)),
                SqliteValue::Text(SmallText::from_string("$.b")),
                SqliteValue::Integer(9),
            ])
            .expect("jsonb_set should accept JSONB blob input");
        let SqliteValue::Blob(blob) = out else {
            return Err(FrankenError::function_error("jsonb_set should return BLOB"));
        };
        assert_eq!(json_from_jsonb(&blob).unwrap(), r#"{"a":1,"b":9}"#);
        Ok(())
    }

    // bd-7h73c: jsonb_set/jsonb_insert/jsonb_replace must honor the JSON subtype
    // on value args (results of json(), json_array(), ->, ...) and embed the value
    // as a JSON subtree rather than stringify it — matching the text json_* twins
    // and stock SQLite (verified sqlite3 3.46.1:
    // json(jsonb_set('{}','$.a',json_array(1,2))) = {"a":[1,2]}).
    #[test]
    fn test_jsonb_set_honors_json_subtype_on_value() {
        let out = JsonbSetFunc
            .invoke_with_arg_subtypes(
                &[
                    SqliteValue::Text(SmallText::from_string("{}")),
                    SqliteValue::Text(SmallText::from_string("$.a")),
                    SqliteValue::Text(SmallText::from_string("[1,2]")),
                ],
                &[0, 0, JSON_SUBTYPE],
            )
            .unwrap();
        let SqliteValue::Blob(blob) = out else {
            panic!("jsonb_set should return BLOB");
        };
        assert_eq!(json_from_jsonb(&blob).unwrap(), r#"{"a":[1,2]}"#);
    }

    #[test]
    fn test_jsonb_set_without_subtype_stringifies_value() {
        // Negative control: a plain TEXT value (subtype 0) stays a JSON string.
        let out = JsonbSetFunc
            .invoke_with_arg_subtypes(
                &[
                    SqliteValue::Text(SmallText::from_string("{}")),
                    SqliteValue::Text(SmallText::from_string("$.a")),
                    SqliteValue::Text(SmallText::from_string("[1,2]")),
                ],
                &[0, 0, 0],
            )
            .unwrap();
        let SqliteValue::Blob(blob) = out else {
            panic!("jsonb_set should return BLOB");
        };
        assert_eq!(json_from_jsonb(&blob).unwrap(), r#"{"a":"[1,2]"}"#);
    }

    #[test]
    fn test_jsonb_insert_honors_json_subtype_on_value() {
        let out = JsonbInsertFunc
            .invoke_with_arg_subtypes(
                &[
                    SqliteValue::Text(SmallText::from_string(r#"{"a":1}"#)),
                    SqliteValue::Text(SmallText::from_string("$.b")),
                    SqliteValue::Text(SmallText::from_string("[3,4]")),
                ],
                &[0, 0, JSON_SUBTYPE],
            )
            .unwrap();
        let SqliteValue::Blob(blob) = out else {
            panic!("jsonb_insert should return BLOB");
        };
        assert_eq!(json_from_jsonb(&blob).unwrap(), r#"{"a":1,"b":[3,4]}"#);
    }

    #[test]
    fn test_jsonb_replace_honors_json_subtype_on_value() {
        let out = JsonbReplaceFunc
            .invoke_with_arg_subtypes(
                &[
                    SqliteValue::Text(SmallText::from_string(r#"{"a":1}"#)),
                    SqliteValue::Text(SmallText::from_string("$.a")),
                    SqliteValue::Text(SmallText::from_string(r#"{"n":5}"#)),
                ],
                &[0, 0, JSON_SUBTYPE],
            )
            .unwrap();
        let SqliteValue::Blob(blob) = out else {
            panic!("jsonb_replace should return BLOB");
        };
        assert_eq!(json_from_jsonb(&blob).unwrap(), r#"{"a":{"n":5}}"#);
    }

    // bd-obb21: jsonb_array/jsonb_object must honor the JSON subtype on value
    // args (same class as bd-7h73c) — embed as a JSON subtree, not stringify
    // (verified sqlite3 3.46.1: json(jsonb_array(json_array(1,2),3)) = [[1,2],3];
    // json(jsonb_object('a',json_array(1,2))) = {"a":[1,2]}).
    #[test]
    fn test_jsonb_array_honors_json_subtype_on_value() {
        let out = JsonbArrayFunc
            .invoke_with_arg_subtypes(
                &[
                    SqliteValue::Text(SmallText::from_string("[1,2]")),
                    SqliteValue::Integer(3),
                ],
                &[JSON_SUBTYPE, 0],
            )
            .unwrap();
        let SqliteValue::Blob(blob) = out else {
            panic!("jsonb_array should return BLOB");
        };
        assert_eq!(json_from_jsonb(&blob).unwrap(), "[[1,2],3]");
    }

    #[test]
    fn test_jsonb_object_honors_json_subtype_on_value() {
        let out = JsonbObjectFunc
            .invoke_with_arg_subtypes(
                &[
                    SqliteValue::Text(SmallText::from_string("a")),
                    SqliteValue::Text(SmallText::from_string("[1,2]")),
                ],
                &[0, JSON_SUBTYPE],
            )
            .unwrap();
        let SqliteValue::Blob(blob) = out else {
            panic!("jsonb_object should return BLOB");
        };
        assert_eq!(json_from_jsonb(&blob).unwrap(), r#"{"a":[1,2]}"#);
    }

    #[test]
    fn test_registered_json_pretty_accepts_jsonb_blob_input() -> Result<()> {
        let mut registry = FunctionRegistry::new();
        register_json_scalars(&mut registry);
        let func = registry
            .find_scalar("json_pretty", 1)
            .expect("json_pretty should be registered");
        let input = jsonb(r#"{"a":[1,2]}"#).unwrap();
        let out = func
            .invoke(&[SqliteValue::Blob(Arc::from(input))])
            .expect("json_pretty should accept JSONB blob input");
        let SqliteValue::Text(pretty) = out else {
            return Err(FrankenError::function_error(
                "json_pretty should return TEXT",
            ));
        };
        assert!(pretty.contains('\n'));
        assert!(pretty.contains("\"a\""));
        Ok(())
    }

    #[test]
    fn test_registered_json_error_position_accepts_jsonb_blob_input() {
        let mut registry = FunctionRegistry::new();
        register_json_scalars(&mut registry);
        let func = registry
            .find_scalar("json_error_position", 1)
            .expect("json_error_position should be registered");
        let input = jsonb(r#"{"a":1}"#).unwrap();
        let out = func
            .invoke(&[SqliteValue::Blob(Arc::from(input))])
            .expect("json_error_position should accept JSONB blob input");
        assert_eq!(out, SqliteValue::Integer(0));
    }

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
    fn test_json_valid_flags_strict() {
        assert_eq!(json_valid("invalid", Some(JSON_VALID_RFC_8259_FLAG)), 0);
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
        // Trailing byte changes total blob size — both superficial and strict
        // reject it since header + payload_len != blob_len.
        let mut trailing = payload.clone();
        trailing.push(0xFF);
        assert_eq!(
            json_valid_blob(&trailing, Some(JSON_VALID_JSONB_SUPERFICIAL_FLAG)),
            0
        );
        assert_eq!(
            json_valid_blob(&trailing, Some(JSON_VALID_JSONB_STRICT_FLAG)),
            0
        );

        // Corrupt an interior header byte — top-level header still valid
        // (superficial passes) but deep decode fails (strict rejects).
        // Byte 2 is the first sub-element header inside the object payload;
        // 0xFF has node_type=0x0F (invalid) so strict parsing fails.
        let mut corrupted = payload;
        assert!(corrupted.len() > 3);
        corrupted[2] = 0xFF;
        assert_eq!(
            json_valid_blob(&corrupted, Some(JSON_VALID_JSONB_SUPERFICIAL_FLAG)),
            1
        );
        assert_eq!(
            json_valid_blob(&corrupted, Some(JSON_VALID_JSONB_STRICT_FLAG)),
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
        assert_eq!(result, SqliteValue::Text(SmallText::from_string("[1,2]")));
    }

    #[test]
    fn test_json_extract_string_unwrap() {
        let result = json_extract(r#"{"a":"hello"}"#, &["$.a"]).unwrap();
        assert_eq!(result, SqliteValue::Text(SmallText::from_string("hello")));
    }

    #[test]
    fn test_arrow_preserves_json() {
        let result = json_arrow(r#"{"a":"hello"}"#, "$.a").unwrap();
        assert_eq!(
            result,
            SqliteValue::Text(SmallText::from_string(r#""hello""#))
        );
    }

    #[test]
    fn test_double_arrow_unwraps() {
        let result = json_double_arrow(r#"{"a":"hello"}"#, "$.a").unwrap();
        assert_eq!(result, SqliteValue::Text(SmallText::from_string("hello")));
    }

    #[test]
    fn test_json_extract_array_index() {
        let result = json_extract("[10,20,30]", &["$[1]"]).unwrap();
        assert_eq!(result, SqliteValue::Integer(20));
    }

    #[test]
    fn test_json_extract_quoted_key_segment() {
        let result = json_extract(r#"{"a.b":1}"#, &["$.\"a.b\""]).unwrap();
        assert_eq!(result, SqliteValue::Integer(1));
    }

    #[test]
    fn test_json_extract_from_end() {
        let result = json_extract("[10,20,30]", &["$[#-1]"]).unwrap();
        assert_eq!(result, SqliteValue::Integer(30));
    }

    #[test]
    #[ignore = "perf-only benchmark"]
    fn perf_json_extract_deep_single_path() {
        use std::hint::black_box;
        use std::time::Instant;

        const ROWS: usize = 200_000;
        const REPEATS: usize = 5;
        const JSON: &str = r#"{"a":{"b":{"c":{"d":[{"e":123},{"e":456}]}}}}"#;
        const PATH: &str = "$.a.b.c.d[1].e";

        let func = JsonExtractFunc;
        let args = [
            SqliteValue::Text(SmallText::from_string(JSON)),
            SqliteValue::Text(SmallText::from_string(PATH)),
        ];
        let mut best_ns = u128::MAX;
        let mut last_result = SqliteValue::Null;

        for _ in 0..REPEATS {
            let started = Instant::now();
            for _ in 0..ROWS {
                last_result = black_box(
                    func.invoke(black_box(&args))
                        .expect("json_extract benchmark invocation must succeed"),
                );
            }
            let elapsed_ns = started.elapsed().as_nanos();
            if elapsed_ns < best_ns {
                best_ns = elapsed_ns;
            }
        }

        println!(
            "json_extract_deep_single_path rows={ROWS} repeats={REPEATS} best_ns={best_ns} last_result={last_result:?}"
        );
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
            json_quote(&SqliteValue::Text(SmallText::from_string("hello"))).unwrap(),
            r#""hello""#
        );
    }

    #[test]
    fn test_json_quote_null() {
        assert_eq!(json_quote(&SqliteValue::Null).unwrap(), "null");
    }

    #[test]
    fn test_json_array_basic() {
        let out = json_array(&[
            SqliteValue::Integer(1),
            SqliteValue::Text(SmallText::from_string("two")),
            SqliteValue::Null,
        ])
        .unwrap();
        assert_eq!(out, r#"[1,"two",null]"#);
    }

    #[test]
    fn test_json_object_basic() {
        let out = json_object(&[
            SqliteValue::Text(SmallText::from_string("a")),
            SqliteValue::Integer(1),
            SqliteValue::Text(SmallText::from_string("b")),
            SqliteValue::Text(SmallText::from_string("two")),
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
    fn test_jsonb_array_variant() {
        let blob = jsonb_array(&[
            SqliteValue::Integer(1),
            SqliteValue::Text(SmallText::from_string("two")),
            SqliteValue::Null,
        ])
        .unwrap();
        assert_eq!(json_from_jsonb(&blob).unwrap(), r#"[1,"two",null]"#);
    }

    #[test]
    fn test_jsonb_object_variant() {
        let blob = jsonb_object(&[
            SqliteValue::Text(SmallText::from_string("a")),
            SqliteValue::Integer(1),
            SqliteValue::Text(SmallText::from_string("b")),
            SqliteValue::Text(SmallText::from_string("two")),
        ])
        .unwrap();
        assert_eq!(json_from_jsonb(&blob).unwrap(), r#"{"a":1,"b":"two"}"#);
    }

    #[test]
    fn test_json_array_length() {
        assert_eq!(json_array_length("[1,2,3]", None).unwrap(), Some(3));
        assert_eq!(json_array_length("[]", None).unwrap(), Some(0));
        assert_eq!(json_array_length(r#"{"a":1}"#, None).unwrap(), Some(0));
    }

    #[test]
    fn test_json_array_length_path() {
        assert_eq!(
            json_array_length(r#"{"a":[1,2,3]}"#, Some("$.a")).unwrap(),
            Some(3)
        );
    }

    #[test]
    fn test_json_array_length_not_array() {
        assert_eq!(
            json_array_length(r#"{"a":1}"#, Some("$.a")).unwrap(),
            Some(0)
        );
        assert_eq!(json_array_length(r#""text""#, None).unwrap(), Some(0));
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
    fn test_json_set_nested_path_create() {
        let out = json_set("{}", &[("$.a.b", SqliteValue::Integer(1))]).unwrap();
        assert_eq!(out, r#"{"a":{"b":1}}"#);
    }

    #[test]
    fn test_json_set_nested_array_path_create() {
        let out = json_set("{}", &[("$.a[0]", SqliteValue::Integer(1))]).unwrap();
        assert_eq!(out, r#"{"a":[1]}"#);
    }

    #[test]
    fn test_json_set_nested_append_path_create() {
        let out = json_set("{}", &[("$.a[#]", SqliteValue::Integer(1))]).unwrap();
        assert_eq!(out, r#"{"a":[1]}"#);
    }

    #[test]
    fn test_json_set_nested_array_object_create() {
        let out = json_set("{}", &[("$.a[0].b", SqliteValue::Integer(1))]).unwrap();
        assert_eq!(out, r#"{"a":[{"b":1}]}"#);
    }

    #[test]
    fn test_json_set_nested_array_index_out_of_range_does_not_scaffold() {
        let out = json_set("{}", &[("$.a[1]", SqliteValue::Integer(1))]).unwrap();
        assert_eq!(out, "{}");
    }

    #[test]
    fn test_json_set_nested_from_end_does_not_scaffold() {
        let out = json_set("{}", &[("$.a[#-1]", SqliteValue::Integer(1))]).unwrap();
        assert_eq!(out, "{}");
    }

    #[test]
    fn test_json_set_scalar_root_with_array_path_is_noop() {
        let out = json_set("null", &[("$.a[0]", SqliteValue::Integer(1))]).unwrap();
        assert_eq!(out, "null");
    }

    #[test]
    fn test_json_set_existing_null_value_with_array_path_is_noop() {
        let out = json_set(r#"{"a":null}"#, &[("$.a[1]", SqliteValue::Integer(1))]).unwrap();
        assert_eq!(out, r#"{"a":null}"#);
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
    fn test_json_insert_nested_path_create() {
        let out = json_insert("{}", &[("$.a.b", SqliteValue::Integer(1))]).unwrap();
        assert_eq!(out, r#"{"a":{"b":1}}"#);
    }

    #[test]
    fn test_json_insert_nested_array_path_create() {
        let out = json_insert("{}", &[("$.a[0]", SqliteValue::Integer(1))]).unwrap();
        assert_eq!(out, r#"{"a":[1]}"#);
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
    fn test_jsonb_insert_variant() {
        let blob = jsonb_insert(r#"{"a":1}"#, &[("$.b", SqliteValue::Integer(2))]).unwrap();
        let text = json_from_jsonb(&blob).unwrap();
        assert_eq!(text, r#"{"a":1,"b":2}"#);
    }

    #[test]
    fn test_jsonb_replace_variant() {
        let blob = jsonb_replace(r#"{"a":1}"#, &[("$.a", SqliteValue::Integer(5))]).unwrap();
        let text = json_from_jsonb(&blob).unwrap();
        assert_eq!(text, r#"{"a":5}"#);
    }

    #[test]
    fn test_jsonb_remove_variant() {
        let blob = jsonb_remove(r#"{"a":1,"b":2}"#, &["$.a"]).unwrap();
        let text = json_from_jsonb(&blob).unwrap();
        assert_eq!(text, r#"{"b":2}"#);
    }

    #[test]
    fn test_jsonb_patch_variant() {
        let blob = jsonb_patch(r#"{"a":1,"b":2}"#, r#"{"b":7}"#).unwrap();
        let text = json_from_jsonb(&blob).unwrap();
        assert_eq!(text, r#"{"a":1,"b":7}"#);
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
    fn test_json_group_array_basic() {
        let out = json_group_array(&[
            SqliteValue::Integer(1),
            SqliteValue::Integer(2),
            SqliteValue::Integer(3),
        ])
        .unwrap();
        assert_eq!(out, "[1,2,3]");
    }

    #[test]
    fn test_json_group_object_basic() {
        let out = json_group_object(&[
            (
                SqliteValue::Text(SmallText::from_string("a")),
                SqliteValue::Integer(1),
            ),
            (
                SqliteValue::Text(SmallText::from_string("b")),
                SqliteValue::Integer(2),
            ),
        ])
        .unwrap();
        assert_eq!(out, r#"{"a":1,"b":2}"#);
    }

    // bd-55eq3: stock json_group_object also keeps duplicate labels verbatim in
    // row order (`{"a":1,"a":2,"b":3}`, verified against sqlite3 3.46.1), not
    // last-wins.
    #[test]
    fn test_json_group_object_duplicate_keys_kept_verbatim() {
        let out = json_group_object(&[
            (
                SqliteValue::Text(SmallText::from_string("k")),
                SqliteValue::Integer(1),
            ),
            (
                SqliteValue::Text(SmallText::from_string("k")),
                SqliteValue::Integer(2),
            ),
        ])
        .unwrap();
        assert_eq!(out, r#"{"k":1,"k":2}"#);
    }

    #[test]
    fn test_jsonb_group_array_and_object_variants() {
        let array_blob = jsonb_group_array(&[SqliteValue::Integer(1), SqliteValue::Null]).unwrap();
        assert_eq!(json_from_jsonb(&array_blob).unwrap(), "[1,null]");

        let object_blob = jsonb_group_object(&[(
            SqliteValue::Text(SmallText::from_string("a")),
            SqliteValue::Integer(7),
        )])
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
        assert_eq!(rows[0].key, SqliteValue::Text(SmallText::from_string("a")));
        assert_eq!(rows[1].key, SqliteValue::Text(SmallText::from_string("b")));
        assert_eq!(rows[0].value, SqliteValue::Integer(1));
        assert_eq!(rows[1].value, SqliteValue::Integer(2));
    }

    #[test]
    fn test_json_each_path() {
        let rows = json_each(r#"{"a":{"b":1,"c":2}}"#, Some("$.a")).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].path, "$.a");
        assert_eq!(rows[1].path, "$.a");
    }

    #[test]
    fn test_json_each_nested_value_is_json_text() {
        let rows = json_each(r#"{"a":[1,2]}"#, None).unwrap();
        assert_eq!(
            rows[0].value,
            SqliteValue::Text(SmallText::from_string("[1,2]"))
        );
        assert_eq!(rows[0].atom, SqliteValue::Null); // arrays have null atom
    }

    #[test]
    fn test_json_each_blob_accepts_text_json_and_jsonb() {
        let text_rows = json_each_blob(br#"{"a":[10,20],"b":30}"#, Some("$.a")).unwrap();
        let jsonb_input = jsonb(r#"{"a":[10,20],"b":30}"#).unwrap();
        let jsonb_rows = json_each_blob(&jsonb_input, Some("$.a")).unwrap();

        assert_eq!(jsonb_rows, text_rows);
        assert_eq!(
            jsonb_rows
                .iter()
                .map(|row| row.value.clone())
                .collect::<Vec<_>>(),
            vec![SqliteValue::Integer(10), SqliteValue::Integer(20)]
        );
    }

    #[test]
    fn test_json_tree_recursive() {
        let rows = json_tree(r#"{"a":{"b":1}}"#, None).unwrap();
        assert!(rows.iter().any(|row| row.fullkey == "$.a"));
        assert!(rows.iter().any(|row| row.fullkey == "$.a.b"));
    }

    #[test]
    fn test_json_tree_columns() {
        let rows = json_tree(r#"{"a":{"b":1}}"#, None).unwrap();
        let row = rows
            .iter()
            .find(|candidate| candidate.fullkey == "$.a.b")
            .expect("nested row should exist");
        assert_eq!(row.key, SqliteValue::Text(SmallText::from_string("b")));
        assert_eq!(row.value, SqliteValue::Integer(1));
        assert_eq!(row.type_name, "integer");
        assert_eq!(row.atom, SqliteValue::Integer(1));
        assert_eq!(row.path, "$.a");
    }

    #[test]
    fn test_json_tree_blob_accepts_jsonb_path() {
        let jsonb_input = jsonb(r#"{"a":{"b":[1,2]},"c":3}"#).unwrap();
        let rows = json_tree_blob(&jsonb_input, Some("$.a")).unwrap();

        assert_eq!(rows.first().map(|row| row.fullkey.as_str()), Some("$.a"));
        assert!(
            rows.iter()
                .any(|row| row.fullkey == "$.a.b[1]" && row.value == SqliteValue::Integer(2))
        );
        assert_eq!(
            rows.iter()
                .find(|row| row.fullkey == "$.a.b[1]")
                .map(|row| row.path.as_str()),
            Some("$.a.b")
        );
    }

    #[test]
    fn test_json_structural_snapshot_table_functions_jsonb() -> Result<()> {
        let input = jsonb(r#"{"a":[10,{"b":20}],"c":null}"#)?;
        let each = json_each_blob(&input, Some("$.a"))?
            .iter()
            .map(json_table_row_structure)
            .collect();
        let tree = json_tree_blob(&input, Some("$.a"))?
            .iter()
            .map(json_table_row_structure)
            .collect();

        let actual = format!("{:#?}", JsonTableJsonbStructure { each, tree });
        let expected = r#"JsonTableJsonbStructure {
    each: [
        JsonTableRowStructure {
            key: "Integer(0)",
            value: "Integer(10)",
            type_name: "integer",
            atom: "Integer(10)",
            id: 1,
            parent: "Null",
            fullkey: "$.a[0]",
            path: "$.a",
        },
        JsonTableRowStructure {
            key: "Integer(1)",
            value: "Text(\"{\\\"b\\\":20}\")",
            type_name: "object",
            atom: "Null",
            id: 2,
            parent: "Null",
            fullkey: "$.a[1]",
            path: "$.a",
        },
    ],
    tree: [
        JsonTableRowStructure {
            key: "Null",
            value: "Text(\"[10,{\\\"b\\\":20}]\")",
            type_name: "array",
            atom: "Null",
            id: 0,
            parent: "Null",
            fullkey: "$.a",
            path: "$.a",
        },
        JsonTableRowStructure {
            key: "Integer(0)",
            value: "Integer(10)",
            type_name: "integer",
            atom: "Integer(10)",
            id: 1,
            parent: "Integer(0)",
            fullkey: "$.a[0]",
            path: "$.a",
        },
        JsonTableRowStructure {
            key: "Integer(1)",
            value: "Text(\"{\\\"b\\\":20}\")",
            type_name: "object",
            atom: "Null",
            id: 2,
            parent: "Integer(0)",
            fullkey: "$.a[1]",
            path: "$.a",
        },
        JsonTableRowStructure {
            key: "Text(\"b\")",
            value: "Integer(20)",
            type_name: "integer",
            atom: "Integer(20)",
            id: 3,
            parent: "Integer(2)",
            fullkey: "$.a[1].b",
            path: "$.a[1]",
        },
    ],
}"#;
        assert_eq!(actual, expected);
        Ok(())
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
                    SqliteValue::Text(SmallText::from_string(r#"{"a":{"b":1}}"#)),
                    SqliteValue::Text(SmallText::from_string("$.a")),
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

        assert_eq!(
            fullkeys,
            vec![SmallText::from("$.a"), SmallText::from("$.a.b")]
        );
    }

    #[test]
    fn test_json_each_vtab_cursor_accepts_jsonb_blob_input() {
        let cx = Cx::new();
        let vtab = JsonEachVtab::connect(&cx, &[]).unwrap();
        let mut cursor = vtab.open().unwrap();
        let input = jsonb(r#"{"a":[10,20]}"#).unwrap();
        cursor
            .filter(
                &cx,
                0,
                None,
                &[
                    SqliteValue::Blob(Arc::from(input)),
                    SqliteValue::Text(SmallText::from_string("$.a")),
                ],
            )
            .expect("json_each cursor should accept JSONB blob input");

        let mut values = Vec::new();
        while !cursor.eof() {
            let mut ctx = ColumnContext::new();
            cursor.column(&mut ctx, 1).unwrap();
            values.push(ctx.take_value().unwrap());
            cursor.next(&cx).unwrap();
        }

        assert_eq!(
            values,
            vec![SqliteValue::Integer(10), SqliteValue::Integer(20)]
        );
    }

    #[test]
    fn test_json_each_cursor_past_end_returns_null_and_zero_rowid() {
        let cx = Cx::new();
        let vtab = JsonEachVtab::connect(&cx, &[]).unwrap();
        let mut cursor = vtab.open().unwrap();
        cursor
            .filter(
                &cx,
                0,
                None,
                &[SqliteValue::Text(SmallText::from_string("[10]"))],
            )
            .unwrap();

        assert!(!cursor.eof());
        cursor.next(&cx).unwrap();
        assert!(cursor.eof());

        let mut ctx = ColumnContext::new();
        cursor.column(&mut ctx, 1).unwrap();
        assert_eq!(ctx.take_value(), Some(SqliteValue::Null));
        assert_eq!(cursor.rowid().unwrap(), 0);
    }

    #[test]
    fn test_json_each_cursor_invalid_column_returns_null() {
        let cx = Cx::new();
        let vtab = JsonEachVtab::connect(&cx, &[]).unwrap();
        let mut cursor = vtab.open().unwrap();
        cursor
            .filter(
                &cx,
                0,
                None,
                &[SqliteValue::Text(SmallText::from_string("[10]"))],
            )
            .unwrap();

        let mut ctx = ColumnContext::new();
        cursor.column(&mut ctx, 99).unwrap();
        assert_eq!(ctx.take_value(), Some(SqliteValue::Null));
    }

    #[test]
    fn test_json_tree_vtab_cursor_accepts_jsonb_blob_input() {
        let cx = Cx::new();
        let vtab = JsonTreeVtab::connect(&cx, &[]).unwrap();
        let mut cursor = vtab.open().unwrap();
        let input = jsonb(r#"{"a":{"b":1}}"#).unwrap();
        cursor
            .filter(
                &cx,
                0,
                None,
                &[
                    SqliteValue::Blob(Arc::from(input)),
                    SqliteValue::Text(SmallText::from_string("$.a")),
                ],
            )
            .expect("json_tree cursor should accept JSONB blob input");

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

        assert_eq!(
            fullkeys,
            vec![SmallText::from("$.a"), SmallText::from("$.a.b")]
        );
    }

    #[test]
    fn test_json_tree_cursor_past_end_returns_null_and_zero_rowid() {
        let cx = Cx::new();
        let vtab = JsonTreeVtab::connect(&cx, &[]).unwrap();
        let mut cursor = vtab.open().unwrap();
        cursor
            .filter(
                &cx,
                0,
                None,
                &[SqliteValue::Text(SmallText::from_string(r#"{"a":1}"#))],
            )
            .unwrap();

        while !cursor.eof() {
            cursor.next(&cx).unwrap();
        }

        let mut ctx = ColumnContext::new();
        cursor.column(&mut ctx, 6).unwrap();
        assert_eq!(ctx.take_value(), Some(SqliteValue::Null));
        assert_eq!(cursor.rowid().unwrap(), 0);
    }

    #[test]
    fn test_json_tree_cursor_invalid_column_returns_null() {
        let cx = Cx::new();
        let vtab = JsonTreeVtab::connect(&cx, &[]).unwrap();
        let mut cursor = vtab.open().unwrap();
        cursor
            .filter(
                &cx,
                0,
                None,
                &[SqliteValue::Text(SmallText::from_string(r#"{"a":1}"#))],
            )
            .unwrap();

        let mut ctx = ColumnContext::new();
        cursor.column(&mut ctx, 99).unwrap();
        assert_eq!(ctx.take_value(), Some(SqliteValue::Null));
    }

    #[test]
    fn test_jsonb_chain_validity() {
        let first = jsonb_set(r#"{"a":1}"#, &[("$.a", SqliteValue::Integer(9))]).unwrap();
        let first_text = json_from_jsonb(&first).unwrap();
        let second = jsonb_patch(&first_text, r#"{"b":2}"#).unwrap();
        assert_eq!(
            json_valid_blob(&second, Some(JSON_VALID_JSONB_STRICT_FLAG)),
            1
        );
    }

    // -----------------------------------------------------------------------
    // json() edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_json_minify_whitespace() {
        assert_eq!(json("  { \"a\" : 1 }  ").unwrap(), r#"{"a":1}"#);
    }

    #[test]
    fn test_json_scalar_string() {
        assert_eq!(json(r#""hello""#).unwrap(), r#""hello""#);
    }

    #[test]
    fn test_json_scalar_number() {
        assert_eq!(json("42").unwrap(), "42");
    }

    #[test]
    fn test_json_scalar_null() {
        assert_eq!(json("null").unwrap(), "null");
    }

    #[test]
    fn test_json_scalar_bool() {
        assert_eq!(json("true").unwrap(), "true");
        assert_eq!(json("false").unwrap(), "false");
    }

    #[test]
    fn test_json_nested_structure() {
        let input = r#"{"a":{"b":[1,2,{"c":3}]}}"#;
        assert_eq!(json(input).unwrap(), input);
    }

    #[test]
    fn test_json_unicode() {
        let input = r#"{"key":"\u00fc\u00e9"}"#;
        let result = json(input).unwrap();
        // After parse/re-serialize, unicode escapes become literal chars
        assert!(result.contains("key"));
    }

    // -----------------------------------------------------------------------
    // json_valid edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_json_valid_zero_flags() {
        assert_eq!(json_valid(r#"{"a":1}"#, Some(0)), 0);
    }

    #[test]
    fn test_registered_json_valid_rejects_out_of_range_flags() {
        let func = JsonValidFunc;
        for flags in [0, 16, -1] {
            let err = func
                .invoke(&[
                    SqliteValue::Text(SmallText::from_string(r#"{"a":1}"#)),
                    SqliteValue::Integer(flags),
                ])
                .expect_err("SQL json_valid must reject flags outside 1..=15");
            assert!(
                err.to_string().contains("between 1 and 15")
                    || err.to_string().contains("out of range"),
                "unexpected error for flags {flags}: {err}"
            );
        }
    }

    #[test]
    fn test_json_valid_empty_string() {
        assert_eq!(json_valid("", None), 0);
    }

    // -----------------------------------------------------------------------
    // json_type all variants
    // -----------------------------------------------------------------------

    #[test]
    fn test_json_type_null() {
        assert_eq!(json_type("null", None).unwrap(), Some("null"));
    }

    #[test]
    fn test_json_type_true() {
        assert_eq!(json_type("true", None).unwrap(), Some("true"));
    }

    #[test]
    fn test_json_type_false() {
        assert_eq!(json_type("false", None).unwrap(), Some("false"));
    }

    #[test]
    fn test_json_type_real() {
        assert_eq!(json_type("3.14", None).unwrap(), Some("real"));
    }

    #[test]
    fn test_json_type_text() {
        assert_eq!(json_type(r#""hello""#, None).unwrap(), Some("text"));
    }

    #[test]
    fn test_json_type_array() {
        assert_eq!(json_type("[1,2]", None).unwrap(), Some("array"));
    }

    #[test]
    fn test_json_valid_bare_numeric_values() {
        // Regression (#259): bare SQL numerics are valid JSON; a non-finite REAL
        // (Inf/NaN) is not (e.g. json_valid(9e999) -> 0).
        let f = JsonValidFunc;
        assert_eq!(
            f.invoke(&[SqliteValue::Integer(123)]).unwrap(),
            SqliteValue::Integer(1)
        );
        assert_eq!(
            f.invoke(&[SqliteValue::Float(1.5)]).unwrap(),
            SqliteValue::Integer(1)
        );
        assert_eq!(
            f.invoke(&[SqliteValue::Float(f64::INFINITY)]).unwrap(),
            SqliteValue::Integer(0)
        );
        assert_eq!(
            f.invoke(&[SqliteValue::Float(f64::NAN)]).unwrap(),
            SqliteValue::Integer(0)
        );
        // A bare numeric renders to its JSON numeric text and is checked against
        // the flags, so it is valid JSON/JSON5 (1/2) but is NOT a JSONB blob
        // (4/8 -> 0), matching C SQLite json_valid(123,4)=0 / (123,8)=0.
        assert_eq!(
            f.invoke(&[SqliteValue::Integer(123), SqliteValue::Integer(1)]).unwrap(),
            SqliteValue::Integer(1)
        );
        assert_eq!(
            f.invoke(&[SqliteValue::Integer(123), SqliteValue::Integer(2)]).unwrap(),
            SqliteValue::Integer(1)
        );
        assert_eq!(
            f.invoke(&[SqliteValue::Integer(123), SqliteValue::Integer(4)]).unwrap(),
            SqliteValue::Integer(0)
        );
        assert_eq!(
            f.invoke(&[SqliteValue::Integer(123), SqliteValue::Integer(8)]).unwrap(),
            SqliteValue::Integer(0)
        );
        // json_valid(NULL) -> NULL.
        assert_eq!(f.invoke(&[SqliteValue::Null]).unwrap(), SqliteValue::Null);
    }

    #[test]
    fn test_json_type_bare_numeric_values() {
        // Regression (#260): json_type(123) -> 'integer', json_type(1.5) -> 'real'.
        let f = JsonTypeFunc;
        assert_eq!(
            f.invoke(&[SqliteValue::Integer(123)]).unwrap(),
            SqliteValue::Text(SmallText::from_string("integer"))
        );
        assert_eq!(
            f.invoke(&[SqliteValue::Float(1.5)]).unwrap(),
            SqliteValue::Text(SmallText::from_string("real"))
        );
    }

    #[test]
    fn test_json_table_valued_bare_numeric_input() {
        // Regression (#260): json_each/json_tree accept a bare SQL numeric as a
        // JSON number (yielding a single scalar row) instead of erroring
        // "must be TEXT or BLOB JSON". C SQLite: json_each(1.5) -> type='real'
        // atom=1.5; json_tree(7) -> type='integer'.
        use serde_json::Value;
        let (v_float, path) =
            super::parse_json_table_filter_args(&[SqliteValue::Float(1.5)]).unwrap();
        assert_eq!(v_float, Value::from(1.5));
        assert!(path.is_none());
        let (v_int, _) =
            super::parse_json_table_filter_args(&[SqliteValue::Integer(7)]).unwrap();
        assert_eq!(v_int, Value::from(7_i64));
        // GH#212 (verified against sqlite3 3.46.1): json_each(9e999) yields a
        // single row (type='real', atom=Inf), so a bare +Inf renders as the
        // numeric literal 9.0e+999 rather than erroring.
        let (v_inf, _) =
            super::parse_json_table_filter_args(&[SqliteValue::Float(f64::INFINITY)]).unwrap();
        assert_eq!(v_inf, super::json_number_from_raw("9.0e+999").unwrap());
    }

    #[test]
    fn test_json_error_position_bare_numeric() {
        // Regression (#260): a bare finite numeric is valid JSON, so
        // json_error_position(1) = json_error_position(1.5) = 0 (no parse error).
        let f = JsonErrorPositionFunc;
        assert_eq!(
            f.invoke(&[SqliteValue::Integer(1)]).unwrap(),
            SqliteValue::Integer(0)
        );
        assert_eq!(
            f.invoke(&[SqliteValue::Float(1.5)]).unwrap(),
            SqliteValue::Integer(0)
        );
        // NULL still short-circuits to NULL. GH#212 (verified against sqlite3
        // 3.46.1): json_error_position(9e999) = 0 — a bare +Inf renders as a
        // valid JSON numeric literal (9.0e+999), so there is no parse error.
        assert_eq!(f.invoke(&[SqliteValue::Null]).unwrap(), SqliteValue::Null);
        assert_eq!(
            f.invoke(&[SqliteValue::Float(f64::INFINITY)]).unwrap(),
            SqliteValue::Integer(0)
        );
    }

    #[test]
    fn test_json_mutation_embeds_nested_json_subtype() {
        // Regression (#233): a value carrying the JSON subtype (produced by
        // json(...)) is embedded as a JSON subtree, not stringified.
        let nested = SqliteValue::Text(SmallText::from_string(r#"{"a":2}"#));
        let subtypes = [0u32, 0, JSON_SUBTYPE];
        let embedded = SqliteValue::Text(SmallText::from_string(r#"{"x":{"a":2}}"#));

        let set_args = [
            SqliteValue::Text(SmallText::from_string("{}")),
            SqliteValue::Text(SmallText::from_string("$.x")),
            nested.clone(),
        ];
        assert_eq!(
            JsonSetFunc
                .invoke_with_arg_subtypes(&set_args, &subtypes)
                .unwrap(),
            embedded
        );
        assert_eq!(
            JsonInsertFunc
                .invoke_with_arg_subtypes(&set_args, &subtypes)
                .unwrap(),
            embedded
        );

        // json_replace only rewrites an existing key.
        let replace_args = [
            SqliteValue::Text(SmallText::from_string(r#"{"x":9}"#)),
            SqliteValue::Text(SmallText::from_string("$.x")),
            nested,
        ];
        assert_eq!(
            JsonReplaceFunc
                .invoke_with_arg_subtypes(&replace_args, &subtypes)
                .unwrap(),
            embedded
        );
    }

    // -----------------------------------------------------------------------
    // json_extract edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_json_extract_missing_path_null() {
        let result = json_extract(r#"{"a":1}"#, &["$.b"]).unwrap();
        assert_eq!(result, SqliteValue::Null);
    }

    #[test]
    fn test_json_extract_no_paths_error() {
        let empty: &[&str] = &[];
        assert!(json_extract(r#"{"a":1}"#, empty).is_err());

        let func = JsonExtractFunc;
        let args = vec![SqliteValue::Text(SmallText::from_string(r#"{"a":1}"#))];
        assert_eq!(func.invoke(&args).unwrap(), SqliteValue::Null);
    }

    #[test]
    fn test_json_extract_null_value() {
        let result = json_extract(r#"{"a":null}"#, &["$.a"]).unwrap();
        assert_eq!(result, SqliteValue::Null);
    }

    #[test]
    fn test_json_extract_boolean() {
        let result = json_extract(r#"{"a":true}"#, &["$.a"]).unwrap();
        assert_eq!(result, SqliteValue::Integer(1));
        let result = json_extract(r#"{"a":false}"#, &["$.a"]).unwrap();
        assert_eq!(result, SqliteValue::Integer(0));
    }

    #[test]
    fn test_json_extract_nested_array() {
        let result = json_extract(r#"{"a":[[1,2],[3,4]]}"#, &["$.a[1][0]"]).unwrap();
        assert_eq!(result, SqliteValue::Integer(3));
    }

    #[test]
    fn test_json_extract_multiple_with_missing() {
        let result = json_extract(r#"{"a":1}"#, &["$.a", "$.b"]).unwrap();
        assert_eq!(
            result,
            SqliteValue::Text(SmallText::from_string("[1,null]"))
        );
    }

    // -----------------------------------------------------------------------
    // json_arrow edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_json_arrow_missing_path_null() {
        let result = json_arrow(r#"{"a":1}"#, "$.b").unwrap();
        assert_eq!(result, SqliteValue::Null);
    }

    #[test]
    fn test_json_arrow_number() {
        let result = json_arrow(r#"{"a":42}"#, "$.a").unwrap();
        assert_eq!(result, SqliteValue::Text(SmallText::from_string("42")));
    }

    #[test]
    fn test_json_arrow_null() {
        let result = json_arrow(r#"{"a":null}"#, "$.a").unwrap();
        assert_eq!(result, SqliteValue::Text(SmallText::from_string("null")));
    }

    // -----------------------------------------------------------------------
    // json_array_length edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_json_array_length_nested_not_array() {
        assert_eq!(
            json_array_length(r#"{"a":"text"}"#, Some("$.a")).unwrap(),
            Some(0)
        );
    }

    #[test]
    fn test_json_array_length_missing_path() {
        assert_eq!(json_array_length(r#"{"a":1}"#, Some("$.b")).unwrap(), None);
    }

    // -----------------------------------------------------------------------
    // json_error_position edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_json_error_position_empty() {
        assert!(json_error_position("") > 0);
    }

    #[test]
    fn test_json_error_position_just_brace() {
        assert!(json_error_position("{") > 0);
    }

    // -----------------------------------------------------------------------
    // json_pretty edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_json_pretty_empty_array() {
        assert_eq!(json_pretty("[]", None).unwrap(), "[]");
    }

    #[test]
    fn test_json_pretty_empty_object() {
        assert_eq!(json_pretty("{}", None).unwrap(), "{}");
    }

    #[test]
    fn test_json_pretty_scalar() {
        assert_eq!(json_pretty("42", None).unwrap(), "42");
    }

    #[test]
    fn test_json_pretty_nested() {
        let result = json_pretty(r#"{"a":[1,2]}"#, None).unwrap();
        assert!(result.contains('\n'));
        assert!(result.contains("\"a\""));
    }

    // -----------------------------------------------------------------------
    // bd-6i2s required tests: json_pretty + jsonb availability
    // -----------------------------------------------------------------------

    #[test]
    fn test_json_pretty_object() {
        let output = json_pretty(r#"{"a":1,"b":[2,3]}"#, None).unwrap();
        assert!(output.contains('\n'));
        assert!(output.contains("    \"a\""));
        assert!(output.contains("    \"b\""));
    }

    #[test]
    fn test_json_pretty_array() {
        let output = json_pretty("[1,2,3]", None).unwrap();
        assert!(output.contains('\n'));
        assert!(output.contains("    1"));
        assert!(output.contains("    2"));
        assert!(output.contains("    3"));
    }

    #[test]
    fn test_json_pretty_idempotent() {
        let input = r#"{"a":1,"b":[2,3]}"#;
        let first = json_pretty(input, None).unwrap();
        let second = json_pretty(&first, None).unwrap();
        assert_eq!(first, second, "json_pretty should be idempotent");
    }

    #[test]
    fn test_jsonb_functions_available() {
        let blob = jsonb_array(&[SqliteValue::Integer(1), SqliteValue::Integer(2)]).unwrap();
        assert!(
            !blob.is_empty(),
            "jsonb_array should produce non-empty output"
        );

        let blob2 = jsonb_set(r#"{"a":1}"#, &[("$.a", SqliteValue::Integer(9))]).unwrap();
        assert!(
            !blob2.is_empty(),
            "jsonb_set should produce non-empty output"
        );

        let blob3 = jsonb_object(&[
            SqliteValue::Text(SmallText::from_string("key")),
            SqliteValue::Integer(42),
        ])
        .unwrap();
        assert!(
            !blob3.is_empty(),
            "jsonb_object should produce non-empty output"
        );
    }

    // -----------------------------------------------------------------------
    // json_quote edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_json_quote_integer() {
        assert_eq!(json_quote(&SqliteValue::Integer(42)).unwrap(), "42");
        assert_eq!(json_quote(&SqliteValue::Integer(-1)).unwrap(), "-1");
    }

    #[test]
    fn test_json_quote_float() {
        #[allow(clippy::approx_constant)]
        let result = json_quote(&SqliteValue::Float(3.14)).unwrap();
        assert!(result.starts_with("3.14"));
    }

    #[test]
    fn test_json_quote_float_infinity() {
        // GH#212 (verified against sqlite3 3.46.1): stock renders +Inf/-Inf as
        // the numeric literal 9.0e+999 / -9.0e+999; only NaN maps to `null`.
        assert_eq!(
            json_quote(&SqliteValue::Float(f64::INFINITY)).unwrap(),
            "9.0e+999"
        );
        assert_eq!(
            json_quote(&SqliteValue::Float(f64::NEG_INFINITY)).unwrap(),
            "-9.0e+999"
        );
        assert_eq!(json_quote(&SqliteValue::Float(f64::NAN)).unwrap(), "null");
    }

    // -----------------------------------------------------------------------
    // GH#212: non-finite REALs (±Inf -> 9.0e+999 / -9.0e+999, NaN -> null).
    // Every expected string below was captured from stock sqlite3 3.46.1.
    // -----------------------------------------------------------------------

    fn text(value: &str) -> SqliteValue {
        SqliteValue::Text(SmallText::from_string(value))
    }

    #[test]
    fn test_json_array_non_finite_reals() {
        // SELECT json_array(1e999)         -> [9.0e+999]
        assert_eq!(
            json_array(&[SqliteValue::Float(f64::INFINITY)]).unwrap(),
            "[9.0e+999]"
        );
        // SELECT json_array(-1e999)        -> [-9.0e+999]
        assert_eq!(
            json_array(&[SqliteValue::Float(f64::NEG_INFINITY)]).unwrap(),
            "[-9.0e+999]"
        );
        // SELECT json_array(1.0, 2e308*10) -> [1.0,9.0e+999]
        assert_eq!(
            json_array(&[SqliteValue::Float(1.0), SqliteValue::Float(f64::INFINITY)]).unwrap(),
            "[1.0,9.0e+999]"
        );
        // SELECT json_array( (SELECT 1e999 - 1e999) ) -> [null]  (NaN)
        assert_eq!(
            json_array(&[SqliteValue::Float(f64::NAN)]).unwrap(),
            "[null]"
        );
    }

    #[test]
    fn test_json_object_non_finite_real() {
        // SELECT json_object('k', 1e999)   -> {"k":9.0e+999}
        assert_eq!(
            json_object(&[text("k"), SqliteValue::Float(f64::INFINITY)]).unwrap(),
            r#"{"k":9.0e+999}"#
        );
        // NaN value -> null.
        assert_eq!(
            json_object(&[text("k"), SqliteValue::Float(f64::NAN)]).unwrap(),
            r#"{"k":null}"#
        );
    }

    #[test]
    fn test_json_quote_non_finite_reals() {
        // SELECT json_quote(1e999)  -> 9.0e+999   (NOT null: only NaN is null)
        assert_eq!(
            json_quote(&SqliteValue::Float(f64::INFINITY)).unwrap(),
            "9.0e+999"
        );
        assert_eq!(
            json_quote(&SqliteValue::Float(f64::NEG_INFINITY)).unwrap(),
            "-9.0e+999"
        );
        // SELECT json_quote(1e999 - 1e999) -> null  (NaN)
        assert_eq!(json_quote(&SqliteValue::Float(f64::NAN)).unwrap(), "null");
    }

    #[test]
    fn test_json_parse_preserves_non_finite_source_text() {
        // SELECT json('[9e999]')      -> [9e999]      (source text PRESERVED)
        assert_eq!(json("[9e999]").unwrap(), "[9e999]");
        // SELECT json('[9.0e+999]')   -> [9.0e+999]
        assert_eq!(json("[9.0e+999]").unwrap(), "[9.0e+999]");
        // SELECT json('[-9e999]')     -> [-9e999]
        assert_eq!(json("[-9e999]").unwrap(), "[-9e999]");
    }

    #[test]
    fn test_json_valid_accepts_non_finite_literal_text() {
        // SELECT json_valid('[9e999]') -> 1
        assert_eq!(json_valid("[9e999]", None), 1);
        assert_eq!(json_valid("[9.0e+999]", None), 1);
        assert_eq!(json_valid("[-9e999]", None), 1);
    }

    #[test]
    fn test_json_extract_non_finite_reads_back_as_infinity() {
        // SELECT json_extract('[9.0e+999]','$[0]') -> Inf (REAL +Inf)
        let value = json_extract("[9.0e+999]", &["$[0]"]).unwrap();
        match value {
            SqliteValue::Float(f) => {
                assert!(f.is_infinite() && f.is_sign_positive(), "expected +Inf, got {f}");
            }
            other => panic!("expected REAL +Inf, got {other:?}"),
        }
        // A parsed `9e999` (minimal source) also reads back as +Inf.
        let value = json_extract("[9e999]", &["$[0]"]).unwrap();
        assert!(matches!(value, SqliteValue::Float(f) if f.is_infinite() && f.is_sign_positive()));
        // Negative literal -> -Inf.
        let value = json_extract("[-9e999]", &["$[0]"]).unwrap();
        assert!(matches!(value, SqliteValue::Float(f) if f.is_infinite() && f.is_sign_negative()));
    }

    #[test]
    fn test_json_type_non_finite_literal_is_real() {
        // SELECT json_type('[9.0e+999]','$[0]') -> real
        assert_eq!(json_type("[9.0e+999]", Some("$[0]")).unwrap(), Some("real"));
        assert_eq!(json_type("[9e999]", Some("$[0]")).unwrap(), Some("real"));
    }

    #[test]
    fn test_jsonb_round_trip_preserves_non_finite() {
        // SELECT json(jsonb('[9e999]')) -> [9e999]  (JSONB payload round-trips)
        let blob = jsonb("[9e999]").unwrap();
        assert_eq!(json_from_jsonb(&blob).unwrap(), "[9e999]");

        // Constructed via jsonb_array: +Inf stored canonically -> [9.0e+999].
        let blob = jsonb_array(&[SqliteValue::Float(f64::INFINITY)]).unwrap();
        assert_eq!(json_from_jsonb(&blob).unwrap(), "[9.0e+999]");

        // -Inf and NaN through the JSONB construct path.
        let blob = jsonb_array(&[SqliteValue::Float(f64::NEG_INFINITY)]).unwrap();
        assert_eq!(json_from_jsonb(&blob).unwrap(), "[-9.0e+999]");
        let blob = jsonb_array(&[SqliteValue::Float(f64::NAN)]).unwrap();
        assert_eq!(json_from_jsonb(&blob).unwrap(), "[null]");
    }

    #[test]
    fn test_json_array_embeds_parsed_non_finite_preserving_source() {
        // SELECT json_array(json('9e999'))    -> [9e999]      (source preserved)
        assert_eq!(
            json_array_with_subtypes(&[text("9e999")], &[JSON_SUBTYPE]).unwrap(),
            "[9e999]"
        );
        // SELECT json_array(json('9.0e+999')) -> [9.0e+999]
        assert_eq!(
            json_array_with_subtypes(&[text("9.0e+999")], &[JSON_SUBTYPE]).unwrap(),
            "[9.0e+999]"
        );
        // SELECT json_object('k', json('9e999')) -> {"k":9e999}
        assert_eq!(
            json_object_with_subtypes(&[text("k"), text("9e999")], &[0, JSON_SUBTYPE]).unwrap(),
            r#"{"k":9e999}"#
        );
    }

    #[test]
    fn test_json_extract_nested_container_preserves_non_finite() {
        // SELECT json_extract('{"a":[1e999]}','$.a') -> [1e999]
        assert_eq!(
            json_extract(r#"{"a":[1e999]}"#, &["$.a"]).unwrap(),
            text("[1e999]")
        );
    }

    #[test]
    fn test_json_pretty_preserves_non_finite() {
        // SELECT json_pretty('[9e999]') -> "[\n    9e999\n]"
        assert_eq!(json_pretty("[9e999]", None).unwrap(), "[\n    9e999\n]");
    }

    #[test]
    fn test_json_group_array_non_finite() {
        // Aggregate construct path shares the scalar rendering.
        assert_eq!(
            json_group_array(&[
                SqliteValue::Float(f64::INFINITY),
                SqliteValue::Float(f64::NAN),
                SqliteValue::Float(f64::NEG_INFINITY),
            ])
            .unwrap(),
            "[9.0e+999,null,-9.0e+999]"
        );
    }

    #[test]
    fn test_json_quote_blob() {
        let result = json_quote(&SqliteValue::Blob(Arc::from(vec![0xDE, 0xAD])));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("JSON cannot hold BLOB values")
        );
    }

    #[test]
    fn test_json_quote_text_special_chars() {
        let result = json_quote(&SqliteValue::Text(SmallText::from_string("a\"b\\c"))).unwrap();
        assert!(result.contains("\\\""));
        assert!(result.contains("\\\\"));
    }

    // -----------------------------------------------------------------------
    // json_object edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_json_object_odd_args_error() {
        let err = json_object(&[
            SqliteValue::Text(SmallText::from_string("a")),
            SqliteValue::Integer(1),
            SqliteValue::Text(SmallText::from_string("b")),
        ]);
        assert!(err.is_err());
    }

    #[test]
    fn test_json_object_non_text_key_error() {
        let err = json_object(&[SqliteValue::Integer(1), SqliteValue::Integer(2)]);
        assert!(err.is_err());
    }

    #[test]
    fn test_json_object_empty() {
        assert_eq!(json_object(&[]).unwrap(), "{}");
    }

    // bd-55eq3: stock SQLite keeps duplicate labels verbatim in argument order
    // (`json_object('k',1,'k',2)` -> `{"k":1,"k":2}`, verified against sqlite3
    // 3.46.1). serde_json's `Map` collapsed them to the last value; the object
    // text is now assembled directly so both entries survive.
    #[test]
    fn test_json_object_duplicate_keys_kept_verbatim() {
        let out = json_object(&[
            SqliteValue::Text(SmallText::from_string("k")),
            SqliteValue::Integer(1),
            SqliteValue::Text(SmallText::from_string("k")),
            SqliteValue::Integer(2),
        ])
        .unwrap();
        assert_eq!(out, r#"{"k":1,"k":2}"#);
    }

    #[test]
    fn test_json_object_duplicate_keys_preserve_interleaved_order() {
        // Matches stock: json_object('a',1,'b',2,'a',3) -> {"a":1,"b":2,"a":3}.
        let out = json_object(&[
            SqliteValue::Text(SmallText::from_string("a")),
            SqliteValue::Integer(1),
            SqliteValue::Text(SmallText::from_string("b")),
            SqliteValue::Integer(2),
            SqliteValue::Text(SmallText::from_string("a")),
            SqliteValue::Integer(3),
        ])
        .unwrap();
        assert_eq!(out, r#"{"a":1,"b":2,"a":3}"#);
    }

    // -----------------------------------------------------------------------
    // json_set/insert/replace array index
    // -----------------------------------------------------------------------

    #[test]
    fn test_json_set_array_element() {
        let out = json_set("[1,2,3]", &[("$[1]", SqliteValue::Integer(99))]).unwrap();
        assert_eq!(out, "[1,99,3]");
    }

    #[test]
    fn test_json_set_array_append_at_len() {
        let out = json_set("[1,2]", &[("$[2]", SqliteValue::Integer(3))]).unwrap();
        assert_eq!(out, "[1,2,3]");
    }

    #[test]
    fn test_json_insert_array_append_at_len() {
        let out = json_insert("[1,2]", &[("$[2]", SqliteValue::Integer(3))]).unwrap();
        assert_eq!(out, "[1,2,3]");
    }

    #[test]
    fn test_json_set_append_pseudo_index() {
        let out = json_set("[1,2]", &[("$[#]", SqliteValue::Integer(3))]).unwrap();
        assert_eq!(out, "[1,2,3]");
    }

    #[test]
    fn test_json_replace_append_pseudo_index_noop() {
        let out = json_replace("[1,2]", &[("$[#]", SqliteValue::Integer(3))]).unwrap();
        assert_eq!(out, "[1,2]");
    }

    #[test]
    fn test_json_replace_array_element() {
        let out = json_replace("[1,2,3]", &[("$[0]", SqliteValue::Integer(0))]).unwrap();
        assert_eq!(out, "[0,2,3]");
    }

    #[test]
    fn test_json_set_multiple_paths() {
        let out = json_set(
            r#"{"a":1,"b":2}"#,
            &[
                ("$.a", SqliteValue::Integer(10)),
                ("$.c", SqliteValue::Integer(30)),
            ],
        )
        .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(parsed["a"], 10);
        assert_eq!(parsed["c"], 30);
    }

    // -----------------------------------------------------------------------
    // json_remove edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_json_remove_missing_key_no_change() {
        let out = json_remove(r#"{"a":1}"#, &["$.b"]).unwrap();
        assert_eq!(out, r#"{"a":1}"#);
    }

    #[test]
    fn test_json_remove_multiple_paths() {
        let out = json_remove(r#"{"a":1,"b":2,"c":3}"#, &["$.a", "$.c"]).unwrap();
        assert_eq!(out, r#"{"b":2}"#);
    }

    #[test]
    fn test_json_remove_from_end_index() {
        let out = json_remove("[1,2,3]", &["$[#-1]"]).unwrap();
        assert_eq!(out, "[1,2]");
    }

    #[test]
    fn test_registered_json_remove_root_returns_sql_null() {
        let func = JsonRemoveFunc;
        let out = func
            .invoke(&[
                SqliteValue::Text(SmallText::from_string(r#"{"a":1}"#)),
                SqliteValue::Text(SmallText::from_string("$")),
            ])
            .expect("json_remove root path should execute");
        assert_eq!(out, SqliteValue::Null);
    }

    #[test]
    fn test_registered_jsonb_remove_root_returns_sql_null() {
        let func = JsonbRemoveFunc;
        let out = func
            .invoke(&[
                SqliteValue::Text(SmallText::from_string(r#"{"a":1}"#)),
                SqliteValue::Text(SmallText::from_string("$")),
            ])
            .expect("jsonb_remove root path should execute");
        assert_eq!(out, SqliteValue::Null);
    }

    // -----------------------------------------------------------------------
    // json_patch edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_json_patch_non_object_replaces() {
        let out = json_patch(r#"{"a":1}"#, "42").unwrap();
        assert_eq!(out, "42");
    }

    #[test]
    fn test_json_patch_nested_merge() {
        let out = json_patch(r#"{"a":{"b":1,"c":2}}"#, r#"{"a":{"b":10,"d":4}}"#).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(parsed["a"]["b"], 10);
        assert_eq!(parsed["a"]["c"], 2);
        assert_eq!(parsed["a"]["d"], 4);
    }

    // -----------------------------------------------------------------------
    // json_each edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_json_each_scalar() {
        let rows = json_each("42", None).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].key, SqliteValue::Null);
        assert_eq!(rows[0].value, SqliteValue::Integer(42));
        assert_eq!(rows[0].type_name, "integer");
    }

    #[test]
    fn test_json_each_empty_array() {
        let rows = json_each("[]", None).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn test_json_each_empty_object() {
        let rows = json_each("{}", None).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn test_json_each_missing_path() {
        let rows = json_each(r#"{"a":1}"#, Some("$.b")).unwrap();
        assert!(rows.is_empty());
    }

    // -----------------------------------------------------------------------
    // json_tree edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_json_tree_scalar() {
        let rows = json_tree("42", None).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].type_name, "integer");
    }

    #[test]
    fn test_json_tree_empty_array() {
        let rows = json_tree("[]", None).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].type_name, "array");
    }

    #[test]
    fn test_json_tree_parent_ids() {
        let rows = json_tree(r#"{"a":1}"#, None).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].parent, SqliteValue::Null); // root
        assert_eq!(rows[1].parent, SqliteValue::Integer(rows[0].id)); // child
    }

    #[test]
    fn test_json_tree_missing_path() {
        let rows = json_tree(r#"{"a":1}"#, Some("$.b")).unwrap();
        assert!(rows.is_empty());
    }

    // -----------------------------------------------------------------------
    // JSONB edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_jsonb_null() {
        let blob = jsonb("null").unwrap();
        assert_eq!(json_from_jsonb(&blob).unwrap(), "null");
    }

    #[test]
    fn test_jsonb_booleans() {
        assert_eq!(json_from_jsonb(&jsonb("true").unwrap()).unwrap(), "true");
        assert_eq!(json_from_jsonb(&jsonb("false").unwrap()).unwrap(), "false");
    }

    #[test]
    fn test_jsonb_integer() {
        let blob = jsonb("42").unwrap();
        assert_eq!(json_from_jsonb(&blob).unwrap(), "42");
    }

    #[test]
    fn test_jsonb_float() {
        let blob = jsonb("3.14").unwrap();
        let text = json_from_jsonb(&blob).unwrap();
        assert!(text.starts_with("3.14"));
    }

    #[test]
    fn test_jsonb_nested_array() {
        let blob = jsonb("[[1],[2,3]]").unwrap();
        assert_eq!(json_from_jsonb(&blob).unwrap(), "[[1],[2,3]]");
    }

    #[test]
    fn test_jsonb_empty_string() {
        let blob = jsonb(r#""""#).unwrap();
        assert_eq!(json_from_jsonb(&blob).unwrap(), r#""""#);
    }

    #[test]
    fn test_jsonb_extract_multiple_paths() {
        let blob = jsonb_extract(r#"{"a":1,"b":2}"#, &["$.a", "$.b"]).unwrap();
        assert_eq!(json_from_jsonb(&blob).unwrap(), "[1,2]");
    }

    #[test]
    fn test_jsonb_extract_no_paths_error() {
        let empty: &[&str] = &[];
        assert!(jsonb_extract(r#"{"a":1}"#, empty).is_err());
    }

    #[test]
    fn test_jsonb_decode_trailing_bytes() {
        let mut blob = jsonb("42").unwrap();
        blob.push(0xFF); // trailing garbage
        assert!(json_from_jsonb(&blob).is_err());
        assert_eq!(
            json_valid_blob(&blob, Some(JSON_VALID_JSONB_SUPERFICIAL_FLAG)),
            0
        );
    }

    #[test]
    fn test_jsonb_decode_empty() {
        assert!(json_from_jsonb(&[]).is_err());
    }

    // -----------------------------------------------------------------------
    // Path parsing edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_path_invalid_no_dollar() {
        assert!(json_extract(r#"{"a":1}"#, &["a"]).is_err());
    }

    #[test]
    fn test_path_empty_key_error() {
        assert!(json_extract(r#"{"a":1}"#, &["$."]).is_err());
    }

    #[test]
    fn test_path_unclosed_bracket() {
        assert!(json_extract(r"[1,2]", &["$[0"]).is_err());
    }

    #[test]
    fn test_path_from_end_zero_error() {
        assert!(json_extract("[1,2,3]", &["$[#-0]"]).is_err());
    }

    #[test]
    fn test_path_from_end_beyond_length() {
        let result = json_extract("[1,2,3]", &["$[#-10]"]).unwrap();
        assert_eq!(result, SqliteValue::Null);
    }

    // -----------------------------------------------------------------------
    // json_group_object edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_json_group_object_non_text_key_error() {
        let err = json_group_object(&[(SqliteValue::Integer(1), SqliteValue::Integer(2))]);
        assert!(err.is_err());
    }

    #[test]
    fn test_json_group_object_empty() {
        assert_eq!(json_group_object(&[]).unwrap(), "{}");
    }

    // -----------------------------------------------------------------------
    // json_array edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_json_array_empty() {
        assert_eq!(json_array(&[]).unwrap(), "[]");
    }

    #[test]
    fn test_json_array_with_blob() {
        let err = json_array(&[SqliteValue::Blob(Arc::from(vec![0xCA, 0xFE]))]).unwrap_err();
        assert!(err.to_string().contains("JSON cannot hold BLOB values"));
    }

    /// bd-t75hg byte-level oracle keeper: every vector below is the exact
    /// `hex(jsonb(...))` output of stock sqlite3 3.46.1, captured live on
    /// 2026-08-14. Our encoder must be byte-identical for these canonical
    /// shapes, and our decoder must read the oracle bytes back to the same
    /// value.
    #[test]
    fn test_jsonb_bytes_match_sqlite3_oracle() {
        let vectors: &[(&str, &str)] = &[
            ("1", "1331"),
            // bd-pnfrr: the bead's cited oracle — `hex(jsonb(5))` is `1335`
            // (header 0x13 = INT type 3, direct size 1; payload 0x35 = '5'),
            // never a raw fixed-width binary payload.
            ("5", "1335"),
            ("-7", "232D37"),
            ("1.5", "35312E35"),
            ("0.1", "35302E31"),
            ("-0.0", "452D302E30"),
            ("1e300", "553165333030"),
            (
                "9223372036854775807",
                "C31339323233333732303336383534373735383037",
            ),
            (
                "-9223372036854775808",
                "C3142D39323233333732303336383534373735383038",
            ),
            (
                "12345678901234567890",
                "C3143132333435363738393031323334353637383930",
            ),
            ("[1,2.5,\"x\"]", "8B133135322E351778"),
            ("{\"a\":1}", "4C17611331"),
            ("[]", "0B"),
            ("{}", "0C"),
            ("\"\"", "07"),
        ];
        for (input, expected_hex) in vectors {
            let value = parse_json_text(input).expect("oracle vector parses");
            let encoded = encode_jsonb_root(&value).expect("oracle vector encodes");
            let got_hex: String = encoded.iter().map(|b| format!("{b:02X}")).collect();
            assert_eq!(
                &got_hex, expected_hex,
                "jsonb({input}) bytes must match stock sqlite3"
            );
            let oracle_bytes: Vec<u8> = (0..expected_hex.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&expected_hex[i..i + 2], 16).unwrap())
                .collect();
            let decoded = decode_jsonb_root(&oracle_bytes).expect("oracle bytes decode");
            assert_eq!(
                decoded, value,
                "decoding stock sqlite3 bytes for {input} must recover the value"
            );
        }
    }

    /// bd-t75hg: escaped strings encode as TEXTJ with RFC-8259 escapes (the
    /// shape stock emits and expects), and stock-written escaped payloads
    /// decode to raw strings.
    #[test]
    fn test_jsonb_escaped_strings_round_trip_and_decode_stock_escapes() {
        let value = parse_json_text("\"a\\\"b\\\\c\\nd\"").expect("escaped string parses");
        let encoded = encode_jsonb_root(&value).expect("encodes");
        assert_eq!(
            encoded[0] & 0x0f,
            JSONB_TEXT_JSON_TYPE,
            "strings needing escapes must use TEXTJ"
        );
        let decoded = decode_jsonb_root(&encoded).expect("decodes");
        assert_eq!(decoded, value, "escaped string round-trips");

        // TEXT5 payload with JSON5 escapes decodes too (read tolerance).
        let mut text5 = Vec::new();
        let payload = b"it\\'s";
        text5.push((u8::try_from(payload.len()).unwrap() << 4) | JSONB_TEXT5_TYPE);
        text5.extend_from_slice(payload);
        let decoded = decode_jsonb_root(&text5).expect("TEXT5 decodes");
        assert_eq!(decoded, Value::String("it's".to_owned()));
    }

    /// bd-t75hg: INT5/FLOAT5 numeric text (JSON5 forms) decodes numerically.
    #[test]
    fn test_jsonb_json5_numeric_payloads_decode() {
        let mut int5 = Vec::new();
        int5.push((4u8 << 4) | JSONB_INT5_TYPE);
        int5.extend_from_slice(b"0x1A");
        assert_eq!(
            decode_jsonb_root(&int5).unwrap(),
            Value::Number(Number::from(26)),
        );
        let mut float5 = Vec::new();
        float5.push((3u8 << 4) | JSONB_FLOAT5_TYPE);
        float5.extend_from_slice(b"+.5");
        assert_eq!(
            decode_jsonb_root(&float5).unwrap(),
            Value::Number(Number::from_f64(0.5).unwrap()),
        );
    }
}
