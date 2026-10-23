#![recursion_limit = "512"]

//! Differential oracle sweep (pane af49, 2026-08-20): frank vs rusqlite over
//! JSON scalar + manipulation functions — json/json_extract, -> and ->>
//! operators, json_type/json_valid/json_array_length/json_quote, json_object/
//! json_array construction, and json_set/insert/replace/remove/patch. Distinct
//! from bd-76x57 (aggregate JSON subtype). Pass = parity coverage keeper; a
//! mismatch is a leaf divergence. Typed structural compare.

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
    let rows = conn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("frank `{sql}`: {e:?}"));
    assert_eq!(rows.len(), 1, "frank `{sql}` returned {} rows", rows.len());
    tag_f(&rows[0].values()[0])
}
fn rval(conn: &rusqlite::Connection, sql: &str) -> String {
    conn.query_row(sql, [], |row| {
        Ok(tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(0)))
    })
    .unwrap_or_else(|e| panic!("rusqlite `{sql}`: {e:?}"))
}

#[test]
fn json_scalar_functions_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        let exprs = [
            "SELECT json(' { \"a\" : 1 , \"b\" : [ 2 , 3 ] } ')",
            "SELECT json_extract('{\"a\":{\"b\":2}}','$.a.b')",
            "SELECT json_extract('[10,20,30]','$[1]')",
            "SELECT json_extract('{\"a\":[{\"b\":7}]}','$.a[0].b')",
            "SELECT json_extract('{\"a\":5}','$.a','$.c')",
            "SELECT json_extract('{\"a\":5}','$.missing')",
            "SELECT json_extract('{\"x\":\"hi\"}','$.x')",
            "SELECT json_extract('{\"n\":null}','$.n')",
            "SELECT '{\"a\":5}' -> '$.a'",
            "SELECT '{\"a\":5}' ->> '$.a'",
            "SELECT '{\"a\":\"hi\"}' -> '$.a'",
            "SELECT '{\"a\":\"hi\"}' ->> '$.a'",
            "SELECT '[1,2,3]' -> 1",
            "SELECT '[1,2,3]' ->> 1",
            "SELECT json_type('{\"a\":1}'),json_type('[1]'),json_type('1'),json_type('1.5'),json_type('true'),json_type('null'),json_type('\"s\"')",
            "SELECT json_type('{\"a\":[1]}','$.a')",
            "SELECT json_valid('{\"a\":1}'),json_valid('{bad}'),json_valid('[1,2'),json_valid('null')",
            "SELECT json_array_length('[1,2,3]')",
            "SELECT json_array_length('{\"a\":[1,2]}','$.a')",
            "SELECT json_array_length('{}')",
            "SELECT json_quote(3.14),json_quote('a\"b'),json_quote(42)",
            "SELECT json_object('a',1,'b','x','c',null)",
            "SELECT json_array(1,'x',null,2.5)",
            "SELECT json_set('{\"a\":1}','$.a',99)",
            "SELECT json_set('{\"a\":1}','$.b',2)",
            "SELECT json_insert('{\"a\":1}','$.a',99)",
            "SELECT json_insert('{\"a\":1}','$.b',2)",
            "SELECT json_replace('{\"a\":1}','$.a',99)",
            "SELECT json_replace('{\"a\":1}','$.b',2)",
            "SELECT json_remove('{\"a\":1,\"b\":2}','$.a')",
            "SELECT json_remove('[1,2,3]','$[1]')",
            "SELECT json_patch('{\"a\":1,\"b\":2}','{\"b\":null,\"c\":3}')",
            "SELECT json_object('k', json_array(1,2))",
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
            "{} json scalar divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}

/// bd-reality-check-1022-llk6g.14: the JSON subtype must flow from one JSON
/// function into the next, as in stock. `json_quote` of a JSON value is a
/// no-op and its own result is JSON; json_extract of an array or object, and
/// json_patch / json_remove results, are JSON; a JSONB blob argument is the
/// JSON it encodes. The join and GROUP BY evaluators must pass the subtype
/// too (they used to quote `json('[1]')` inside `json_array`).
#[test]
fn json_subtype_flows_between_json_functions() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t(x INTEGER, doc TEXT)",
            r#"INSERT INTO t VALUES (1, '{"a":[1,2],"s":"[1]"}')"#,
        ] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        let shapes = [
            r#"json_quote(json('{"a":1}'))"#,
            r#"json_quote(json(' {"a" : 1} '))"#,
            r#"json_quote(json('"s"'))"#,
            r#"json_quote(json_set('{"a":1}','$.b',2))"#,
            "json_quote(json_object('a', 1))",
            "json_quote(json_array(1, 'b'))",
            r#"json_quote('{"a":1}')"#,
            "json_quote(json_pretty('[1]'))",
            "json_quote(json_quote('x'))",
            "json_array(json_quote('x'))",
            "json_object('k', json_quote('x'))",
            "json_quote(jsonb('[1, 2]'))",
            "json_quote(x'01')",
            "typeof(json_quote(jsonb('{}')))",
            "json_quote(jsonb_array(1))",
            r#"json_quote('{"a":1}' -> '$')"#,
            r#"json_quote('{"a":"x"}' ->> '$.a')"#,
            r#"json_array(json_extract('{"a":[1]}','$.a'))"#,
            r#"json_array(json_extract('{"a":"x"}','$.a'))"#,
            r#"json_array(json_extract('{"a":"[1]"}','$.a'))"#,
            r#"json_array(json_extract('{"a":[1],"b":2}','$.a','$.b'))"#,
            r#"json_object('k', json_extract('{"a":{"b":1}}','$.a'))"#,
            r#"json_array(json_patch('{}','{"a":1}'))"#,
            "json_array(json_remove('[1,2]','$[0]'))",
            "json_array(jsonb('[1]'))",
            "json_set('{}', '$.a', jsonb('[1]'))",
            r#"json_object('k', jsonb('{"x":1}'))"#,
            r#"json_array('{"a":1}' ->> '$')"#,
            "json_array(json('[1]'))",
            "json_object('k', json_object('a', json_array(1)))",
        ];
        let mut diffs = Vec::new();
        for shape in shapes {
            for sql in [
                format!("SELECT {shape}"),
                format!("SELECT {shape} FROM t"),
                format!("SELECT {shape} FROM t a JOIN t b ON a.x = b.x"),
                format!("SELECT {shape} FROM t a LEFT JOIN t b ON a.x = b.x"),
                format!("SELECT {shape} FROM t GROUP BY x"),
            ] {
                let fv = fval(&f, &sql).await;
                let rv = rval(&r, &sql);
                if fv != rv {
                    diffs.push(format!("  `{sql}`\n     frank= {fv}\n     stock= {rv}"));
                }
            }
        }
        // Column-sourced JSON inside a join (the common application shape).
        for sql in [
            "SELECT json_object('id', a.x, 'doc', json(b.doc)) FROM t a JOIN t b ON a.x = b.x",
            "SELECT json_array(json_extract(b.doc, '$.a')) FROM t a JOIN t b ON a.x = b.x",
            "SELECT json_object('n', count(*), 'docs', json_group_array(json(doc))) FROM t",
            "SELECT json_array(json_group_array(x)) FROM t GROUP BY x",
        ] {
            let fv = fval(&f, sql).await;
            let rv = rval(&r, sql);
            if fv != rv {
                diffs.push(format!("  `{sql}`\n     frank= {fv}\n     stock= {rv}"));
            }
        }
        // Blobs that are not JSONB still fail on both.
        for sql in ["SELECT json_quote(x'dead')", "SELECT json_array(x'cafe')"] {
            let fe = f.query(sql).await.is_err();
            let re = r.query_row(sql, [], |_| Ok(())).is_err();
            if fe != re {
                diffs.push(format!("  `{sql}`: frank err={fe}, stock err={re}"));
            }
        }
        assert!(
            diffs.is_empty(),
            "{} JSON subtype divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
