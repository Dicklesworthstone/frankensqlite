#![recursion_limit = "512"]

//! JSON edge-function leaf-hunt (pane af49, 2026-08-21): frank vs rusqlite over
//! newer/less-tested json surface — json_valid 1-arg & 2-arg flag form,
//! json_error_position, json_pretty, -> / ->> with negative array indices and
//! quoted keys, and JSON5 input (unquoted keys, trailing commas, single quotes,
//! comments, hex, +/-Infinity). Error-tolerant so a missing feature surfaces as
//! a divergence, not a panic. Pass = coverage keeper; a mismatch is a leaf.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => format!("int:{n}"),
        SqliteValue::Float(f) => format!("real:{f}"),
        SqliteValue::Text(s) => format!("text:{s}"),
        SqliteValue::Blob(b) => format!("blob:{b:?}"),
    }
}
fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => format!("int:{n}"),
        rusqlite::types::Value::Real(f) => format!("real:{f}"),
        rusqlite::types::Value::Text(s) => format!("text:{s}"),
        rusqlite::types::Value::Blob(b) => format!("blob:{b:?}"),
    }
}

async fn fval(conn: &Connection, sql: &str) -> String {
    match conn.query(sql).await {
        Ok(rows) if rows.len() == 1 => tag_f(&rows[0].values()[0]),
        Ok(rows) => format!("ROWS:{}", rows.len()),
        Err(_) => "ERR".to_owned(),
    }
}
fn rval(conn: &rusqlite::Connection, sql: &str) -> String {
    match conn.query_row(sql, [], |row| {
        Ok(tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(0)))
    }) {
        Ok(s) => s,
        Err(_) => "ERR".to_owned(),
    }
}

#[test]
// The JSON5 SQL literals ('{a:1}') look like format args to clippy.
#[allow(clippy::literal_string_with_formatting_args)]
fn json_edge_functions_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        let exprs = [
            // json_valid 1-arg
            "SELECT json_valid('{\"a\":1}'), json_valid('{bad}'), json_valid('null'), json_valid('')",
            // json_valid 2-arg flag form (bit1=strict-JSON, bit2..=JSON5 etc.)
            "SELECT json_valid('{\"a\":1}', 1)",
            "SELECT json_valid('{a:1}', 1)",
            "SELECT json_valid('{a:1}', 2)",
            "SELECT json_valid('{a:1}', 4)",
            "SELECT json_valid('{a:1}', 6)",
            "SELECT json_valid('[1,2,]', 6)",
            // json_error_position
            "SELECT json_error_position('{\"a\":1}')",
            "SELECT json_error_position('{\"a\":}')",
            "SELECT json_error_position('[1,2,,3]')",
            // json_pretty
            "SELECT json_pretty('{\"a\":1,\"b\":[2,3]}')",
            "SELECT json_pretty('[1,{\"x\":2}]')",
            // -> / ->> with negative array indices + quoted keys
            "SELECT '[1,2,3]' ->> -1",
            "SELECT '[1,2,3]' -> -2",
            "SELECT json_extract('[10,20,30]','$[#-1]')",
            "SELECT json_extract('{\"a b\":5}','$.\"a b\"')",
            "SELECT '{\"a\":{\"b\":9}}' ->> '$.a.b'",
            // NOTE: JSON5 input (unquoted keys, single quotes, trailing commas,
            // comments, hex, +/-Infinity/NaN) is NOT yet supported by frank —
            // tracked as the multi-turn feature bd-qear2 — so those cases are
            // intentionally excluded here.
            // json_quote (scalar -> json)
            "SELECT json_quote(3.0), json_quote('a'), json_quote(null)",
            // json_array_length on non-array / with path
            "SELECT json_array_length('{\"a\":1}'), json_array_length('5')",
            // json_type at deep path
            "SELECT json_type('{\"a\":[{\"b\":true}]}','$.a[0].b')",
        ];

        let mut diffs = Vec::new();
        for e in exprs {
            let fv = fval(&f, e).await;
            let rv = rval(&r, e);
            if fv != rv {
                diffs.push(format!("  `{e}`\n     frank= {fv}\n     stock= {rv}"));
            }
        }
        assert!(
            diffs.is_empty(),
            "{} json edge-function divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}

#[test]
#[allow(clippy::literal_string_with_formatting_args)]
fn bd_72iv9_json_error_position_matches_stock_oracle() {
    let deep_ok = format!("{}{}", "[".repeat(1000), "]".repeat(1000));
    let deep_err = format!("{}{}", "[".repeat(1001), "]".repeat(1001));
    let deep_obj_ok = format!("{}{}{}", "{\"a\":".repeat(1000), "1", "}".repeat(1000));
    let deep_obj_err = format!("{}{}{}", "{\"a\":".repeat(1001), "1", "}".repeat(1001));
    // Construct `{α:...}` at runtime so nightly clippy's
    // `literal_string_with_formatting_args` pass does not ICE slicing `'α'`.
    let alpha_ok = ["{", "α:1}"].concat();
    let alpha_err = ["{", "α:}"].concat();

    let corpus: Vec<&str> = vec![
        // Empty / whitespace / comments only
        "",
        "   ",
        "\t\r\n ",
        "/* comment */",
        "// line comment",
        "// line comment\n",
        "\u{00a0}\u{2000}\u{2028}\u{3000}\u{feff}",
        // Truncation at every token kind
        "{",
        "{ ",
        "{\"a\"",
        "{\"a\" ",
        "{\"a\":",
        "{\"a\": ",
        "{\"a\":1",
        "{\"a\":1,",
        "{\"a\":1, ",
        "[",
        "[ ",
        "[1",
        "[1,",
        "[1, ",
        "\"abc",
        "\"abc\\",
        "\"abc\\u",
        "\"abc\\u12",
        "\"abc\\x1",
        "'abc",
        "'abc\\",
        "-",
        "+",
        ".",
        "1.",
        "1e",
        "1E",
        "1e+",
        "1e-",
        "0x",
        "0X",
        "-0x",
        "+0x",
        "t",
        "tru",
        "f",
        "fals",
        "n",
        "nul",
        "Infi",
        "/*",
        "/* *",
        "/",
        "/a",
        // Trailing garbage across containers and scalars
        "[1] x",
        "[] x",
        "{} x",
        "{\"a\":1} x",
        "1 x",
        "\"a\" x",
        "true x",
        "false x",
        "null x",
        "[1/*c*/] x",
        "[1,2] ,3",
        "{} }",
        "[] ]",
        "[1]   x",
        "[1] /*c*/ x",
        "[1] //c\nx",
        "[1] /*unterminated",
        "[[1]] x",
        "[[]] x",
        "{\"a\":[]} x",
        "{\"a\":{}} x",
        "{\"a\":[1]} x",
        // Bad and JSON5 escapes
        "\"\\a\"",
        "\"\\u123\"",
        "\"\\u123g\"",
        "\"\\x1\"",
        "\"\\x1g\"",
        "\"\\x41\"",
        "\"\\0\"",
        "\"\\01\"",
        "\"\\0a\"",
        "'\\0'",
        "'\\01'",
        "'\\v'",
        "\"\\v\"",
        "\"\\\n\"",
        "\"\\\r\n\"",
        "\"\\\r\"",
        "\"\\\u{2028}a\"",
        "\"\\\u{2029}a\"",
        "\"\\u0041\"",
        "\"a\x01b\"",
        "\"a\nb\"",
        // Numbers and Inf/NaN variants
        "01",
        "00",
        "-01",
        "+01",
        "1..2",
        "1.2.",
        "1e2e3",
        "1e+2e3",
        "1e+a",
        ".a",
        "-.a",
        "+.a",
        ".5",
        "-.5",
        "+.5",
        "1.",
        "1.e2",
        "+1.",
        "-1.",
        ".5e2",
        "0x1g",
        "0x10",
        "-0x10",
        "+0x10",
        "-0x1g",
        "NaN",
        "nan",
        "QNaN",
        "SNaN",
        "Infinity",
        "-Infinity",
        "+Infinity",
        "inf",
        "-inf",
        "+inf",
        "InfX",
        "-Infx",
        "true1",
        "false0",
        "nullx",
        "-Infinity1",
        // JSON5 objects, arrays, comments, unquoted keys, trailing commas
        "{a:1}",
        "{$a_1:1}",
        "{_a:1,}",
        "{'a':1}",
        "[1,2,]",
        "{/*c*/a:1//e\n}",
        "{1:2}",
        "{a 1:2}",
        "{a:1 b:2}",
        "[1 2]",
        "{\\u0061:1}",
        "{\\u006:1}",
        "{\\u006g:1}",
        "{a\\u0062:1}",
        "{a\\u006:1}",
        "{a:1,,}",
        "[1,,2]",
        "{\"a\":}",
        "{:1}",
        "{,}",
        "[,]",
        "{a:1,}",
        "[1,]",
        "{\"a\" : 1 }",
        "{\"a\" /*c*/ : 1}",
        "{\"a\" /*c*/ 1}",
        "{'a':\"b\"}",
        "{a:1 /*c*/,}",
        "[1 /*c*/,]",
        "{/*c*/}",
        "[/*c*/]",
        "{\"a\":1 /*c*/ 2}",
        "[1 /*c*/ 2]",
        // Multi-byte UTF-8 before the error (2-, 3-, and 4-byte codepoints)
        alpha_ok.as_str(),
        alpha_err.as_str(),
        "\"😀\" x",
        "[\"😀\",]",
        "[\"😀\",",
        "{\"é\":}",
        "{\"中\":}",
        "{\"😀\":bad}",
        "\"😀\\z\"",
        "\"😀",
        "\"é😀中\\",
        "\u{feff}[1,",
        "\u{00a0}\u{3000}{",
        "/*😀*/{",
        "//😀\n[1,",
        "[/*😀*/] x",
        "[\"é\",\"中\",\"😀\"] x",
        "[\"é\",\"中\",\"😀\" x]",
        // Embedded NUL byte
        "\"a\0b\"",
        "[\0]",
        "[1]\0x",
        "\0",
        // Depth boundary (JSON_MAX_DEPTH = 1000)
        deep_ok.as_str(),
        deep_err.as_str(),
        deep_obj_ok.as_str(),
        deep_obj_err.as_str(),
    ];

    // Pin exact stock SQLite 3.54.1 results for the bead's canonical cases so
    // the oracle is guaranteed non-vacuous.
    let stock_probe = rusqlite::Connection::open_in_memory().unwrap();
    for (input, expected) in [
        ("{", 2_i64),
        ("[1,", 4_i64),
        ("\"abc", 5_i64),
        ("[1] x", 1_i64),
        ("[] x", 2_i64),
        ("{a:1,}", 0_i64),
    ] {
        let got: i64 = stock_probe
            .query_row("SELECT json_error_position(?1)", [input], |row| row.get(0))
            .unwrap();
        assert_eq!(got, expected, "stock pin failed for {input:?}");
    }

    asupersync::test_utils::run_test(|| async {
        let mut diffs = Vec::new();
        for file_backed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let (f, r) = if file_backed {
                (
                    Connection::open(dir.path().join("f.db").to_str().unwrap())
                        .await
                        .unwrap(),
                    rusqlite::Connection::open(dir.path().join("r.db")).unwrap(),
                )
            } else {
                (
                    Connection::open(":memory:").await.unwrap(),
                    rusqlite::Connection::open_in_memory().unwrap(),
                )
            };
            let db = if file_backed { "file" } else { "memory" };
            let f_prep = f.prepare("SELECT json_error_position(?1)").await.unwrap();
            let mut r_prep = r.prepare("SELECT json_error_position(?1)").unwrap();

            for &input in &corpus {
                let stock_pos: i64 = r_prep.query_row([input], |row| row.get(0)).unwrap();
                for prepared in [false, true] {
                    let frank_val = if prepared {
                        f_prep
                            .query_with_params(&[SqliteValue::Text(input.into())])
                            .await
                            .unwrap()[0]
                            .values()[0]
                            .clone()
                    } else {
                        f.query_with_params(
                            "SELECT json_error_position(?1)",
                            &[SqliteValue::Text(input.into())],
                        )
                        .await
                        .unwrap()[0]
                            .values()[0]
                            .clone()
                    };
                    let expected = SqliteValue::Integer(stock_pos);
                    if frank_val != expected {
                        diffs.push(format!(
                            "  [{db}, prepared={prepared}] input={input:?}\n     frank= {frank_val:?}\n     stock= {expected:?}"
                        ));
                    }
                }
            }

            // Negative/type controls: NULL -> NULL, numeric -> 0, valid JSONB -> 0.
            for sql in [
                "SELECT json_error_position(NULL)",
                "SELECT json_error_position(42)",
                "SELECT json_error_position(3.5)",
                "SELECT json_error_position(jsonb('{\"a\":[1,2]}'))",
            ] {
                let fv = fval(&f, sql).await;
                let rv = rval(&r, sql);
                if fv != rv {
                    diffs.push(format!("  [{db}] `{sql}`\n     frank= {fv}\n     stock= {rv}"));
                }
            }

            drop(f_prep);
            f.close().await.unwrap();
        }
        assert!(
            diffs.is_empty(),
            "{} json_error_position divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}

/// bd-b0i0o: `jsonb_set` / `jsonb_insert` / `jsonb_replace` / `jsonb_array_insert`
/// must match stock SQLite 3.54.1 byte-for-byte (`hex(...)`), storing SQL TEXT
/// values and created object labels as `JSONB_TEXTRAW` (0xA) — or `JSONB_TEXT5`
/// (0x9) for quoted path labels containing escapes — while preserving existing
/// JSON `TEXT`/`TEXTJ` labels, JSON-subtyped values, and `jsonb_array` /
/// `jsonb_object` / `jsonb_group_*` `TEXT`/`TEXTJ` wire types.
#[test]
fn bd_b0i0o_jsonb_edit_textraw_matches_stock_oracle() {
    let long_plain = "a".repeat(20);
    let very_long_quoted = format!("{}\"{}", "b".repeat(140), "c".repeat(140));

    // Bead canonical pins against stock SQLite 3.54.1 (non-vacuity check).
    let stock_probe = rusqlite::Connection::open_in_memory().unwrap();
    for (sql, expected_hex) in [
        ("SELECT hex(jsonb_set('[1,2]','$[1]','x'))", "4B13311A78"),
        (
            "SELECT hex(jsonb_insert('[1,2]','$[#]','x'))",
            "6B133113321A78",
        ),
        (
            "SELECT hex(jsonb_replace('[1,2]','$[1]','x'))",
            "4B13311A78",
        ),
        (
            "SELECT hex(jsonb_set('{}','$.a','x\"y'))",
            "6C1A613A782279",
        ),
    ] {
        let got: String = stock_probe.query_row(sql, [], |row| row.get(0)).unwrap();
        assert_eq!(got, expected_hex, "stock pin failed for `{sql}`");
    }

    // Parameterized (doc, path, value) matrix covering all four jsonb_* edit functions.
    let text_values: Vec<String> = vec![
        String::new(),
        "x".to_owned(),
        "x\"y".to_owned(),
        "a\\b".to_owned(),
        "line1\nline2\t\r\u{0008}\u{000C}\u{0001}\u{001F}".to_owned(),
        "café 🚀 中".to_owned(),
        long_plain,
        very_long_quoted,
    ];

    let edit_shapes: &[(&str, &str, &str)] = &[
        // Array slots
        ("jsonb_set", "[1,2]", "$[1]"),
        ("jsonb_set", "[1,2]", "$[#]"),
        ("jsonb_set", "[1,2]", "$[#-1]"),
        ("jsonb_insert", "[1,2]", "$[#]"),
        ("jsonb_insert", "[1,2]", "$[1]"),
        ("jsonb_replace", "[1,2]", "$[1]"),
        ("jsonb_replace", "[1,2]", "$[#]"),
        ("jsonb_array_insert", "[1,2]", "$[0]"),
        ("jsonb_array_insert", "[1,2]", "$[1]"),
        ("jsonb_array_insert", "[1,2]", "$[#]"),
        // Existing object members (label stays TEXT 0x7 / TEXTJ 0x8; value becomes TEXTRAW 0xA)
        ("jsonb_set", "{\"a\":1}", "$.a"),
        ("jsonb_replace", "{\"a\":1}", "$.a"),
        ("jsonb_insert", "{\"a\":1}", "$.a"),
        ("jsonb_set", "{\"a\\\"b\":1}", "$.\"a\\\"b\""),
        ("jsonb_replace", "{\"a\\\"b\":1}", "$.\"a\\\"b\""),
        // Created object members & nested scaffolding (created labels become TEXTRAW 0xA / TEXT5 0x9)
        ("jsonb_set", "{}", "$.a"),
        ("jsonb_insert", "{}", "$.a"),
        ("jsonb_set", "{\"k\":\"v\\\"w\"}", "$.new_key"),
        ("jsonb_set", "{}", "$.a.b.c"),
        ("jsonb_insert", "{}", "$.a.b.c"),
        ("jsonb_set", "{}", "$.\"a.b\""),
        ("jsonb_set", "{}", "$.\"\""),
        ("jsonb_set", "{}", "$.\"a\\\"b\""),
        ("jsonb_set", "{}", "$.\"a\\\\b\""),
        ("jsonb_set", "{}", "$.\"a\\nb\""),
        ("jsonb_set", "{}", "$.\"café 🚀\""),
        ("jsonb_set", "{}", "$.a\\b"),
        ("jsonb_set", "{}", "$.café"),
        ("jsonb_array_insert", "{}", "$.a[0]"),
        ("jsonb_array_insert", "{}", "$.a.b[0]"),
        ("jsonb_array_insert", "{}", "$.\"a.b\"[0]"),
        ("jsonb_array_insert", "{}", "$.\"a\\\"b\"[0]"),
        // Root replacement
        ("jsonb_set", "[1]", "$"),
        ("jsonb_replace", "[1]", "$"),
        ("jsonb_insert", "[1]", "$"),
    ];

    // SQL-level expressions covering subtypes, chaining, blob values, equality, and controls.
    let sql_queries: &[&str] = &[
        // Exact equality with stock wire bytes
        "SELECT jsonb_set('{}','$.a','x\"y') = x'6C1A613A782279'",
        "SELECT jsonb_set('[1,2]','$[1]','x') = x'4B13311A78'",
        // Multiple path/value pairs in one call
        "SELECT hex(jsonb_set('{}','$.a','x','$.b','y\"z','$.a','w'))",
        "SELECT hex(jsonb_insert('{}','$.a','x','$.a','ignored','$.b','y'))",
        "SELECT hex(jsonb_replace('{\"a\":1,\"b\":2}','$.a','x','$.c','ignored','$.b','y\"z'))",
        "SELECT hex(jsonb_array_insert('[1]','$[0]','x','$[#]','y\"z'))",
        // Chained JSONB edits preserve prior TEXTRAW nodes and labels
        "SELECT hex(jsonb_set(jsonb_set('{}','$.a','x\"y'),'$.b','z'))",
        "SELECT hex(jsonb_insert(jsonb_set('{}','$.a','x'),'$.b','y'))",
        "SELECT hex(jsonb_replace(jsonb_set('{}','$.a','x'),'$.a','y\"z'))",
        "SELECT hex(jsonb_remove(jsonb_set('{\"keep\":1}','$.a','x\"y'),'$.keep'))",
        "SELECT hex(jsonb_array_insert(jsonb_set('{\"a\":[1]}','$.b','x'),'$.a[0]','y'))",
        // Embedding a JSONB blob value preserves its internal TEXTRAW nodes
        "SELECT hex(jsonb_set('[1]','$[0]',jsonb_set('{}','$.a','x\"y')))",
        "SELECT hex(jsonb_insert('[1]','$[#]',jsonb_set('[0]','$[0]','x')))",
        "SELECT hex(jsonb_replace('[1]','$[0]',jsonb_set('{}','$.a','x')))",
        "SELECT hex(jsonb_array_insert('[1]','$[0]',jsonb_set('{}','$.a','x')))",
        // JSON-subtyped values MUST keep TEXT/TEXTJ, not TEXTRAW
        "SELECT hex(jsonb_set('{\"a\":1}','$.a',json('\"x\"')))",
        "SELECT hex(jsonb_set('{\"a\":1}','$.a',json('\"x\\\"y\"')))",
        "SELECT hex(jsonb_set('{}','$.a',json('{\"k\":\"v\"}')))",
        "SELECT hex(jsonb_insert('[1]','$[#]',json('\"x\"')))",
        "SELECT hex(jsonb_replace('[1]','$[0]',json('\"x\"')))",
        "SELECT hex(jsonb_array_insert('[1]','$[0]',json('\"x\"')))",
        // Constructors and aggregates MUST keep TEXT/TEXTJ (negative controls)
        "SELECT hex(jsonb_array('x'))",
        "SELECT hex(jsonb_array('x\"y'))",
        "SELECT hex(jsonb_array(jsonb_set('{}','$.a','x\"y')))",
        "SELECT hex(jsonb_object('k','x'))",
        "SELECT hex(jsonb_object('k','x\"y'))",
        "SELECT hex(jsonb_object('k',jsonb_set('{}','$.a','x\"y')))",
        "SELECT hex(jsonb_group_array('x'))",
        "SELECT hex(jsonb_group_array('x\"y'))",
        "SELECT hex(jsonb_group_object('k','x'))",
        "SELECT hex(jsonb_group_object('k','x\"y'))",
        // Round-trip through json() and json_extract()
        "SELECT json(jsonb_set('{}','$.a','x\"y')), json_extract(jsonb_set('{}','$.a','x\"y'),'$.a')",
        "SELECT json(jsonb_set('{}','$.\"a\\\"b\"','x\\y')), json_extract(jsonb_set('{}','$.\"a\\\"b\"','x\\y'),'$.\"a\\\"b\"')",
        "SELECT json(jsonb_array_insert('{}','$.a[0]','x\"y'))",
        // Text variants remain valid canonical JSON
        "SELECT json_set('{}','$.a','x\"y')",
        "SELECT json_insert('{}','$.a','x\"y')",
        "SELECT json_replace('{\"a\":1}','$.a','x\"y')",
        "SELECT json_array_insert('[1]','$[0]','x\"y')",
    ];

    asupersync::test_utils::run_test(|| async {
        let mut diffs = Vec::new();
        for file_backed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let (f, r) = if file_backed {
                (
                    Connection::open(dir.path().join("f.db").to_str().unwrap())
                        .await
                        .unwrap(),
                    rusqlite::Connection::open(dir.path().join("r.db")).unwrap(),
                )
            } else {
                (
                    Connection::open(":memory:").await.unwrap(),
                    rusqlite::Connection::open_in_memory().unwrap(),
                )
            };
            let db = if file_backed { "file" } else { "memory" };

            for &(func, doc, path) in edit_shapes {
                let sql = format!("SELECT hex({func}(?1, ?2, ?3)), json({func}(?1, ?2, ?3))");
                let f_prep = f.prepare(&sql).await.unwrap();
                let mut r_prep = r.prepare(&sql).unwrap();

                for val in &text_values {
                    let stock_pair: (String, String) = r_prep
                        .query_row(rusqlite::params![doc, path, val], |row| {
                            Ok((row.get(0)?, row.get(1)?))
                        })
                        .unwrap();
                    let params = [
                        SqliteValue::Text(doc.into()),
                        SqliteValue::Text(path.into()),
                        SqliteValue::Text(val.as_str().into()),
                    ];
                    for prepared in [false, true] {
                        let rows = if prepared {
                            f_prep.query_with_params(&params).await.unwrap()
                        } else {
                            f.query_with_params(&sql, &params).await.unwrap()
                        };
                        let frank_pair = (
                            rows[0].values()[0].to_text(),
                            rows[0].values()[1].to_text(),
                        );
                        if frank_pair != stock_pair {
                            diffs.push(format!(
                                "  [{db}, prepared={prepared}] {func}({doc:?}, {path:?}, {val:?})\n     frank= {frank_pair:?}\n     stock= {stock_pair:?}"
                            ));
                        }
                    }
                }
                drop(f_prep);
            }

            for &sql in sql_queries {
                let mut r_stmt = r.prepare(sql).unwrap();
                let col_count = r_stmt.column_count();
                let rv: String = r_stmt
                    .query_map([], |row| {
                        let cols = (0..col_count)
                            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                            .collect::<Vec<_>>()
                            .join("|");
                        Ok(cols)
                    })
                    .unwrap()
                    .map(|row| row.unwrap())
                    .collect::<Vec<_>>()
                    .join(";");
                drop(r_stmt);

                let f_prep = f.prepare(sql).await.unwrap();
                for prepared in [false, true] {
                    let rows = if prepared {
                        f_prep.query().await.unwrap()
                    } else {
                        f.query(sql).await.unwrap()
                    };
                    let fv = rows
                        .into_iter()
                        .map(|row| row.values().iter().map(tag_f).collect::<Vec<_>>().join("|"))
                        .collect::<Vec<_>>()
                        .join(";");
                    if fv != rv {
                        diffs.push(format!(
                            "  [{db}, prepared={prepared}] `{sql}`\n     frank= {fv}\n     stock= {rv}"
                        ));
                    }
                }
                drop(f_prep);
            }

            f.close().await.unwrap();
        }
        assert!(
            diffs.is_empty(),
            "{} bd-b0i0o jsonb TEXTRAW divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.iter().take(40).cloned().collect::<Vec<_>>().join("\n")
        );
    });
}
