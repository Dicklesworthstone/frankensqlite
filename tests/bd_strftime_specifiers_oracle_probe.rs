#![recursion_limit = "512"]

//! Leaf-hunt (pane af49, 2026-08-21): exhaustive strftime specifier coverage,
//! frank vs rusqlite — one row per specifier over a fixed timestamp, to catch
//! any specifier that (like %U, bd-zv4ra) is unimplemented and passed through
//! literally. Also checks that specifiers SQLite does NOT define behave
//! identically on both engines. Deterministic UTC input.

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
fn strftime_specifiers_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        // Every documented SQLite specifier + a batch stock leaves literal /
        // undefined, each isolated so one bad specifier can't mask another.
        let specs = [
            "%d", "%e", "%f", "%F", "%G", "%g", "%H", "%I", "%j", "%J", "%k", "%l", "%m", "%M",
            "%n", "%p", "%P", "%R", "%s", "%S", "%t", "%T", "%u", "%U", "%V", "%w", "%W", "%Y",
            "%%", // not defined by SQLite strftime — must behave identically on both
            "%a", "%A", "%b", "%B", "%c", "%C", "%D", "%h", "%r", "%x", "%X", "%y", "%Z", "%z",
            "%q", "%1",
        ];

        let ts = "2024-03-05 09:07:05.5";
        let mut diffs = Vec::new();
        for spec in specs {
            let sql = format!("SELECT strftime('[{spec}]', '{ts}')");
            let fv = fval(&f, &sql).await;
            let rv = rval(&r, &sql);
            if fv != rv {
                diffs.push(format!("  `{spec}`\n     frank= {fv}\n     stock= {rv}"));
            }
        }
        // A few midnight / PM / boundary timestamps for hour-sensitive specs.
        for (spec, t) in [
            ("%p %P %I %l", "2024-03-05 00:30:00"),
            ("%p %P %I %l", "2024-03-05 13:45:00"),
            ("%p %P %I %l", "2024-03-05 12:00:00"),
            ("%H %k", "2024-03-05 07:00:00"),
            ("%e %F %T %R", "2024-03-05 07:08:09"),
            // bd-565ji: %J (Julian day) rendered with canonical float precision
            ("%J", "2024-03-05 09:07:05.5"),
            ("%J", "2024-06-15 12:00:00"),
            ("%J", "2024-01-01 00:00:00"),
            ("%J", "1970-01-01 00:00:00.123"),
        ] {
            let sql = format!("SELECT strftime('{spec}', '{t}')");
            let fv = fval(&f, &sql).await;
            let rv = rval(&r, &sql);
            if fv != rv {
                diffs.push(format!(
                    "  `{spec}` @ {t}\n     frank= {fv}\n     stock= {rv}"
                ));
            }
        }

        assert!(
            diffs.is_empty(),
            "{} strftime specifier divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
