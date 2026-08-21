#![recursion_limit = "512"]

//! Behavioral-PRAGMA leaf-hunt (pane af49, 2026-08-21): frank vs rusqlite over
//! PRAGMAs that change QUERY RESULTS (not just introspection) — case_sensitive_like
//! (toggles LIKE ASCII case folding) and its interaction with the default, plus
//! a default-behavior baseline. Behavioral pragmas are a common
//! incompletely-implemented spot. Pass = coverage keeper; a mismatch is a leaf.

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

/// Apply a statement (pragma) to both engines, ignoring errors symmetrically.
async fn apply_both(f: &Connection, r: &rusqlite::Connection, sql: &str) {
    let _ = f.execute(sql).await;
    let _ = r.execute(sql, []);
}

#[test]
fn behavioral_pragmas_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        let like_probe = "SELECT ('ABC' LIKE 'abc') || '/' || ('abc' LIKE 'ABC') || '/' || ('AbC' LIKE 'a_c')";

        let mut diffs = Vec::new();
        let mut check = |label: &str, fv: String, rv: String, diffs: &mut Vec<String>| {
            if fv != rv {
                diffs.push(format!("  [{label}]\n     frank= {fv}\n     stock= {rv}"));
            }
        };

        // Default: LIKE is ASCII case-insensitive on both.
        check(
            "default LIKE",
            fval(&f, like_probe).await,
            rval(&r, like_probe),
            &mut diffs,
        );

        // case_sensitive_like = ON -> LIKE becomes case-sensitive.
        apply_both(&f, &r, "PRAGMA case_sensitive_like = ON").await;
        check(
            "case_sensitive_like=ON",
            fval(&f, like_probe).await,
            rval(&r, like_probe),
            &mut diffs,
        );

        // Turn it back OFF -> case-insensitive again.
        apply_both(&f, &r, "PRAGMA case_sensitive_like = OFF").await;
        check(
            "case_sensitive_like=OFF",
            fval(&f, like_probe).await,
            rval(&r, like_probe),
            &mut diffs,
        );

        // GLOB is always case-sensitive regardless of the pragma.
        apply_both(&f, &r, "PRAGMA case_sensitive_like = ON").await;
        let glob = "SELECT ('ABC' GLOB 'abc') || '/' || ('ABC' GLOB 'ABC')";
        check("GLOB under CSL=ON", fval(&f, glob).await, rval(&r, glob), &mut diffs);
        apply_both(&f, &r, "PRAGMA case_sensitive_like = OFF").await;

        assert!(
            diffs.is_empty(),
            "{} behavioral-pragma divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
