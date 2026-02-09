//! JSON1 scalar-function foundations for `fsqlite-ext-json` (`bd-3cvl`).
//!
//! This module provides a focused, test-backed subset of JSON1 behavior:
//! - strict JSON validation/minification (`json`, `json_valid`)
//! - JSON type inspection (`json_type`)
//! - JSON path extraction with SQLite-like single vs multi-path semantics (`json_extract`)
//! - JSON value constructors (`json_quote`, `json_array`, `json_object`)
//! - array length inspection (`json_array_length`)
//!
//! Path support in this slice:
//! - `$` root
//! - `$.key` object member
//! - `$[N]` array index
//! - `$[#-N]` reverse array index

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::SqliteValue;
use serde_json::{Map, Number, Value};

const JSON_VALID_DEFAULT_FLAGS: u8 = 0x01;
const JSON_VALID_RFC_8259_FLAG: u8 = 0x01;

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
/// Currently supports strict RFC-8259 mode (`0x01`).
#[must_use]
pub fn json_valid(input: &str, flags: Option<u8>) -> i64 {
    let effective_flags = flags.unwrap_or(JSON_VALID_DEFAULT_FLAGS);
    if effective_flags & JSON_VALID_RFC_8259_FLAG == 0 {
        return 0;
    }
    i64::from(parse_json_text(input).is_ok())
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

/// Return the array length at root or path, or `None` when target is not an array.
pub fn json_array_length(input: &str, path: Option<&str>) -> Result<Option<usize>> {
    let root = parse_json_text(input)?;
    let target = match path {
        Some(path_expr) => resolve_path(&root, path_expr)?,
        None => Some(&root),
    };
    Ok(target.and_then(Value::as_array).map(Vec::len))
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

/// Set JSON values at path(s), creating object keys when missing.
pub fn json_set(input: &str, pairs: &[(&str, SqliteValue)]) -> Result<String> {
    edit_json_paths(input, pairs, EditMode::Set)
}

/// Insert JSON values at path(s) only when path does not already exist.
pub fn json_insert(input: &str, pairs: &[(&str, SqliteValue)]) -> Result<String> {
    edit_json_paths(input, pairs, EditMode::Insert)
}

/// Replace JSON values at path(s) only when path already exists.
pub fn json_replace(input: &str, pairs: &[(&str, SqliteValue)]) -> Result<String> {
    edit_json_paths(input, pairs, EditMode::Replace)
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

/// Apply RFC 7396 JSON Merge Patch.
pub fn json_patch(input: &str, patch: &str) -> Result<String> {
    let root = parse_json_text(input)?;
    let patch_value = parse_json_text(patch)?;
    let merged = merge_patch(root, patch_value);
    serde_json::to_string(&merged)
        .map_err(|error| FrankenError::function_error(format!("json_patch encode failed: {error}")))
}

fn parse_json_text(input: &str) -> Result<Value> {
    serde_json::from_str::<Value>(input)
        .map_err(|error| FrankenError::function_error(format!("invalid JSON input: {error}")))
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
    fn test_json_array_length() {
        assert_eq!(json_array_length("[1,2,3]", None).unwrap(), Some(3));
        assert_eq!(json_array_length("[]", None).unwrap(), Some(0));
        assert_eq!(json_array_length(r#"{"a":1}"#, None).unwrap(), None);
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
}
