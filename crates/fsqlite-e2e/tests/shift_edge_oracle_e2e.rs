//! bd-e3yxd — Oracle-parity e2e: bitwise shift edge cases vs rusqlite.
//!
//! numeric_edge_oracle covers small in-range shifts. SQLite's `<<` / `>>` have
//! three subtle corners: a shift count whose magnitude is >= 64 collapses the
//! result (to 0, or to -1 for an arithmetic right shift of a negative); a
//! NEGATIVE shift count reverses the direction (`x << -n` == `x >> n` and vice
//! versa); and `>>` is an arithmetic (sign-propagating) shift, so a negative left
//! operand keeps its sign. These pin all three against rusqlite using fixed
//! integer operands.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn render_frank(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

async fn frank_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rows = conn.query(sql).await.map_err(|e| e.to_string())?;
    Ok(rows
        .iter()
        .map(|row| row.values().iter().map(render_frank).collect())
        .collect())
}

fn sqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let v: rusqlite::types::Value = row.get_unwrap(i);
            out.push(match v {
                rusqlite::types::Value::Null => "NULL".to_owned(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f}"),
                rusqlite::types::Value::Text(s) => format!("'{s}'"),
                rusqlite::types::Value::Blob(b) => format!(
                    "X'{}'",
                    b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                ),
            });
        }
        Ok(out)
    })
    .map_err(|e| e.to_string())?
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| e.to_string())
}

async fn assert_scalar(queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    let mut mismatches = Vec::new();
    for q in queries {
        let frank = frank_rows(&f, q).await;
        match (frank, sqlite_rows(&r, q)) {
            (Ok(a), Ok(b)) if a == b => {}
            (Ok(a), Ok(b)) => {
                mismatches.push(format!("MISMATCH: {q}\n  frank: {a:?}\n  csql:  {b:?}"));
            }
            (Err(e), Ok(b)) => mismatches.push(format!(
                "FRANK_ERR: {q}\n  frank: ERROR({e})\n  csql:  {b:?}"
            )),
            (Ok(a), Err(e)) => {
                mismatches.push(format!("CSQL_ERR: {q}\n  frank: {a:?}\n  csql: ERROR({e})"));
            }
            (Err(_), Err(_)) => {}
        }
    }
    assert!(
        mismatches.is_empty(),
        "{label}: {} mismatch(es)\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

#[test]
fn shift_count_at_or_beyond_64() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT 1 << 63",                   // -9223372036854775808 (sign bit)
                "SELECT 1 << 64",                   // 0
                "SELECT 1 << 100",                  // 0
                "SELECT 255 >> 64",                 // 0
                "SELECT 9223372036854775807 >> 63", // 0
                "SELECT -1 >> 64",                  // -1 (arithmetic, sign fills)
            ],
            "shift_count_at_or_beyond_64",
        )
        .await;
    });
}

#[test]
fn shift_negative_count_reverses_direction() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT 1 << -1",    // 1 >> 1 -> 0
                "SELECT 1024 << -2", // 1024 >> 2 -> 256
                "SELECT 256 >> -1",  // 256 << 1 -> 512
                "SELECT 1 >> -3",    // 1 << 3 -> 8
            ],
            "shift_negative_count_reverses_direction",
        )
        .await;
    });
}

#[test]
fn arithmetic_right_shift_on_negatives() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT -8 >> 1",   // -4
                "SELECT -1 >> 1",   // -1
                "SELECT -1 >> 63",  // -1
                "SELECT -256 >> 4", // -16
                "SELECT -2 << 1",   // -4
            ],
            "arithmetic_right_shift_on_negatives",
        )
        .await;
    });
}
