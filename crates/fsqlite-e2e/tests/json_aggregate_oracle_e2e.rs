//! bd-233zb — Oracle-parity e2e: JSON aggregate functions vs rusqlite.
//!
//! json_function_oracle covered the JSON1 SCALAR functions; this covers the
//! aggregates `json_group_array(value)` (build a JSON array from a group) and
//! `json_group_object(key, value)` (build a JSON object), plain and with
//! GROUP BY, over mixed value types and NULLs. Compared against rusqlite
//! (bundled SQLite ~3.46).
#![recursion_limit = "512"]

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

async fn check(f: &Connection, r: &rusqlite::Connection, queries: &[&str], label: &str) {
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(f, q).await, sqlite_rows(r, q)) {
            (Ok(a), Ok(b)) if a == b => {}
            (Ok(a), Ok(b)) => {
                mismatches.push(format!("MISMATCH: {q}\n  frank: {a:?}\n  csql:  {b:?}"))
            }
            (Err(e), Ok(b)) => mismatches.push(format!(
                "FRANK_ERR: {q}\n  frank: ERROR({e})\n  csql:  {b:?}"
            )),
            (Ok(a), Err(e)) => {
                mismatches.push(format!("CSQL_ERR: {q}\n  frank: {a:?}\n  csql: ERROR({e})"))
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

async fn data() -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, g TEXT, k TEXT, v INTEGER)",
        "INSERT INTO t VALUES (1,'a','x',10),(2,'a','y',20),(3,'b','z',30)",
    ] {
        f.execute(s).await.unwrap();
        r.execute_batch(s).unwrap();
    }
    (f, r)
}

#[test]
fn json_group_array_basic_and_grouped() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        check(
            &f,
            &r,
            &[
                "SELECT json_group_array(v) FROM t", // [10,20,30]
                "SELECT g, json_group_array(v) FROM t GROUP BY g ORDER BY g", // a:[10,20], b:[30]
                "SELECT json_group_array(k) FROM t", // ["x","y","z"]
            ],
            "json_group_array_basic_and_grouped",
        )
        .await;
    });
}

#[test]
fn json_group_object_basic_and_grouped() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        check(
            &f,
            &r,
            &[
                "SELECT json_group_object(k, v) FROM t", // {"x":10,"y":20,"z":30}
                "SELECT g, json_group_object(k, v) FROM t GROUP BY g ORDER BY g", // a:{x:10,y:20}, b:{z:30}
            ],
            "json_group_object_basic_and_grouped",
        )
        .await;
    });
}

#[test]
fn json_group_array_mixed_and_null() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE m (id INTEGER PRIMARY KEY, v)",
            "INSERT INTO m VALUES (1,1),(2,2.5),(3,'text'),(4,NULL)",
        ] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        check(
            &f,
            &r,
            &[
                // Mixed storage classes + NULL -> JSON array [1,2.5,"text",null].
                "SELECT json_group_array(v) FROM m",
                // Validity + element count.
                "SELECT json_valid(json_group_array(v)), json_array_length(json_group_array(v)) FROM m",
            ],
            "json_group_array_mixed_and_null",
        )
        .await;
    });
}

#[test]
fn json_group_array_empty_group() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        check(
            &f,
            &r,
            &[
                // Aggregate over zero matching rows.
                "SELECT json_group_array(v) FROM t WHERE v > 1000",
            ],
            "json_group_array_empty_group",
        )
        .await;
    });
}

#[test]
#[ignore = "bd-76x57: nested-JSON subtype embedding still fails — the fix in \
            49a8e1d64 threads subtypes through the VDBE AggStep opcode, but \
            SELECT agg(...) FROM t executes via the INTERPRETED aggregate path \
            in connection.rs (func.step, no subtype channel), so the VDBE fix is \
            never reached. Real fix = subtype-aware interpreted arg eval + \
            step_with_arg_subtypes at the connection.rs agg sites (and carry the \
            subtype through the GROUP BY sorter). Un-ignore when that lands."]
fn json_group_aggregates_nested_json_subtype_embedded() {
    // bd-76x57 keeper: the canonical use — folding `json_object(...)` /
    // `json_array(...)` rows into a nested JSON array or object. The JSON subtype
    // of the aggregate's argument must survive the step, so each element is
    // EMBEDDED (`[{"k":10},…]`) rather than quoted (`["{\"k\":10}",…]`). Before
    // the aggregate subtype channel existed, frank quoted these; this guards the
    // regression against rusqlite (which embeds).
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        check(
            &f,
            &r,
            &[
                // Array of nested objects: [{"k":10},{"k":20},{"k":30}]
                "SELECT json_group_array(json_object('k', v)) FROM t",
                // Array of nested arrays: [[10,10],[20,20],[30,30]]
                "SELECT json_group_array(json_array(v, v)) FROM t",
                // Grouped, nested-object elements.
                "SELECT g, json_group_array(json_object('v', v)) FROM t GROUP BY g ORDER BY g",
                // Object whose VALUES are nested JSON objects: {"x":{"v":10},…}
                "SELECT json_group_object(k, json_object('v', v)) FROM t",
                // Object whose values are nested JSON arrays.
                "SELECT json_group_object(k, json_array(v)) FROM t",
                // json() over a text literal also carries the subtype through.
                "SELECT json_group_array(json('[1,2]')) FROM t",
            ],
            "json_group_aggregates_nested_json_subtype_embedded",
        )
        .await;
    });
}
