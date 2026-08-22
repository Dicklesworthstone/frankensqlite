#![recursion_limit = "512"]

//! SQL literal & identifier-quoting leaf-hunt (pane af49, 2026-08-21): frank vs
//! rusqlite over lexical corners — single-quoted string escaping (`''` -> `'`),
//! blob literals `x'..'` / `X'..'`, the three identifier-quoting styles SQLite
//! accepts (double-quote, [brackets], `backticks`) including a keyword used as a
//! quoted identifier, and SQLite's double-quoted-string fallback quirk (a
//! double-quoted token that matches no identifier is treated as a string
//! literal). Also unicode text literals and embedded quotes/newlines. Scalar and
//! small result sets compared. Pass = coverage keeper; a mismatch is a leaf.

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

async fn fq(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    match conn.query(sql).await {
        Ok(rows) => rows.iter().map(|r| r.values().iter().map(tag_f).collect()).collect(),
        Err(_) => vec![vec!["ERR".to_owned()]],
    }
}
fn rq(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let Ok(mut st) = conn.prepare(sql) else { return vec![vec!["ERR".to_owned()]] };
    let n = st.column_count();
    match st.query_map([], |row| {
        Ok((0..n).map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i))).collect::<Vec<_>>())
    }) {
        Ok(rows) => rows.collect::<Result<Vec<_>, _>>().unwrap_or_else(|_| vec![vec!["ERR".to_owned()]]),
        Err(_) => vec![vec!["ERR".to_owned()]],
    }
}
async fn ex(f: &Connection, r: &rusqlite::Connection, sql: &str) {
    let _ = f.execute(sql).await;
    let _ = r.execute(sql, []);
}

#[test]
fn literal_identifier_quoting_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        // a table + column whose names are keywords / need quoting
        for s in [
            "CREATE TABLE \"select\"(\"from\" INTEGER, [group] TEXT, `order` REAL)",
            "INSERT INTO \"select\" VALUES (1,'a',1.5),(2,'b',2.5)",
        ] {
            ex(&f, &r, s).await;
        }

        let exprs = [
            // single-quote escaping: '' -> '
            "SELECT 'it''s', 'a''b''c', ''''",
            // blob literals
            "SELECT x'00ff10', X'414243', x''",
            // hex/blob equality
            "SELECT x'4142' = X'4142', typeof(x'01')",
            // unicode + embedded newline in a string literal
            "SELECT 'café', 'line1
line2', length('café')",
            // double-quoted string fallback: \"no_such_col\" as a value -> string 'no_such_col'
            "SELECT \"hello\" ",
            // typeof of literals
            "SELECT typeof('s'), typeof(1), typeof(1.0), typeof(x'00'), typeof(NULL)",
        ];

        let mut diffs = Vec::new();
        let check = |label: &str, fr: Vec<Vec<String>>, rr: Vec<Vec<String>>, d: &mut Vec<String>| {
            if fr != rr { d.push(format!("  [{label}]\n     frank= {fr:?}\n     stock= {rr:?}")); }
        };
        for (i, q) in exprs.iter().enumerate() {
            check(&format!("expr{i}"), fq(&f, q).await, rq(&r, q), &mut diffs);
        }

        // identifier quoting: all three styles reach the same keyword-named columns
        check("dquote identifiers", fq(&f, "SELECT \"from\", [group], `order` FROM \"select\" ORDER BY \"from\"").await,
              rq(&r, "SELECT \"from\", [group], `order` FROM \"select\" ORDER BY \"from\""), &mut diffs);
        check("bracket table", fq(&f, "SELECT [group] FROM [select] ORDER BY [group]").await,
              rq(&r, "SELECT [group] FROM [select] ORDER BY [group]"), &mut diffs);
        check("backtick table", fq(&f, "SELECT `group` FROM `select` ORDER BY `group`").await,
              rq(&r, "SELECT `group` FROM `select` ORDER BY `group`"), &mut diffs);
        // qualified quoted column
        check("qualified quoted", fq(&f, "SELECT \"select\".\"from\" FROM \"select\" ORDER BY 1").await,
              rq(&r, "SELECT \"select\".\"from\" FROM \"select\" ORDER BY 1"), &mut diffs);
        // aliased with a quoted keyword alias
        check("quoted alias", fq(&f, "SELECT \"from\" AS \"where\" FROM \"select\" ORDER BY \"where\"").await,
              rq(&r, "SELECT \"from\" AS \"where\" FROM \"select\" ORDER BY \"where\""), &mut diffs);
        // WHERE with a string literal containing a quote
        ex(&f, &r, "CREATE TABLE s(v TEXT)").await;
        ex(&f, &r, "INSERT INTO s VALUES ('it''s'),('plain'),('a\"b')").await;
        check("where quoted string", fq(&f, "SELECT v FROM s WHERE v = 'it''s'").await,
              rq(&r, "SELECT v FROM s WHERE v = 'it''s'"), &mut diffs);
        check("string with dquote char", fq(&f, "SELECT v FROM s WHERE v LIKE '%\"%'").await,
              rq(&r, "SELECT v FROM s WHERE v LIKE '%\"%'"), &mut diffs);
        // the view/table appears in sqlite_master with its quoted name unquoted
        check("schema names", fq(&f, "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").await,
              rq(&r, "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"), &mut diffs);

        assert!(diffs.is_empty(), "{} literal/identifier-quoting divergence(s) vs rusqlite:\n{}", diffs.len(), diffs.join("\n"));
    });
}
