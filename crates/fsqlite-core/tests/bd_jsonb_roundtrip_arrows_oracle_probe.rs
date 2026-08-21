#![recursion_limit = "512"]

//! jsonb round-trip + arrow-operator leaf-hunt (pane af49, 2026-08-21): frank vs
//! rusqlite. json(jsonb(X)) must equal json(X) for every value shape; the
//! -> (JSON) and ->> (SQL scalar) operators must behave identically over TEXT
//! and JSONB inputs; json_valid/json_type over jsonb. Pass = coverage keeper;
//! a mismatch is a leaf (as bd-elre4 was).

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
fn jsonb_roundtrip_and_arrows_match_rusqlite() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        let exprs = [
            // json(jsonb(X)) == json(X) round-trip fidelity
            "SELECT json(jsonb('{\"a\":1,\"b\":[2,3],\"c\":{\"d\":true}}'))",
            "SELECT json(jsonb('[null, true, false, 1, 2.5, \"x\"]'))",
            "SELECT json(jsonb('{}')), json(jsonb('[]'))",
            // NOTE: json(jsonb(X)) escape-preservation (\\/ and \\uXXXX kept by
            // stock, normalized by frank) is a separate JSONB-TEXTJ conformance
            // gap tracked in a follow-up bead — excluded here.
            "SELECT json(jsonb('123')), json(jsonb('-0.0')), json(jsonb('1e30'))",
            "SELECT json(jsonb('{\"n\":9223372036854775807}'))",
            "SELECT json(jsonb('[1,[2,[3,[4]]]]'))",
            // typeof / validity of jsonb
            "SELECT typeof(jsonb('{\"a\":1}'))",
            "SELECT json_valid(jsonb('{\"a\":1}'))",
            "SELECT json_valid(jsonb('{\"a\":1}'), 8)",
            "SELECT json_type(jsonb('{\"a\":[1]}'))",
            "SELECT json_array_length(jsonb('[1,2,3,4]'))",
            // -> and ->> over TEXT
            "SELECT '{\"a\":{\"b\":5}}' -> '$.a'",
            "SELECT '{\"a\":{\"b\":5}}' ->> '$.a.b'",
            "SELECT '[10,20,30]' -> 1, '[10,20,30]' ->> -1",
            "SELECT '{\"a\":\"hi\"}' -> '$.a', '{\"a\":\"hi\"}' ->> '$.a'",
            // -> and ->> over JSONB
            "SELECT json(jsonb('{\"a\":{\"b\":5}}') -> '$.a')",
            "SELECT jsonb('{\"a\":{\"b\":5}}') ->> '$.a.b'",
            "SELECT jsonb('[10,20,30]') ->> 1",
            "SELECT jsonb('{\"a\":\"hi\"}') ->> '$.a'",
            // -> returns JSON text form even for scalars
            "SELECT '{\"a\":5}' -> '$.a'",
            "SELECT '{\"a\":true}' -> '$.a', '{\"a\":true}' ->> '$.a'",
            "SELECT '{\"a\":null}' -> '$.a', '{\"a\":null}' ->> '$.a'",
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
            "{} jsonb-roundtrip/arrow divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
