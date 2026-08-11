//! bd-5310l: `ORDER BY ... LIMIT l OFFSET o` now bounds the top-N sorter to `l + o` retained rows
//! (was a full sort). HARD GATE: results must be byte-identical to C SQLite (rusqlite) across small
//! and deep offsets, deterministic and tie-heavy orderings, ASC/DESC, ints, text, and NULLs. Plus an
//! opcode gate: `SorterOpen` reads the runtime `LIMIT + OFFSET` bound computed by `OffsetLimit`.

use std::collections::HashMap;

use fsqlite::Connection;
use fsqlite_types::SqliteValue;
use fsqlite_types::opcode::SORTER_OPEN_TOP_N_REGISTER;

fn render(v: &SqliteValue) -> String {
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

async fn frank_rows(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("frank `{sql}`: {e}"))
        .iter()
        .map(|row| row.values().iter().map(render).collect())
        .collect()
}

fn sqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = conn.prepare(sql).unwrap();
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            out.push(match row.get_unwrap::<_, rusqlite::types::Value>(i) {
                rusqlite::types::Value::Null => "NULL".to_owned(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f}"),
                rusqlite::types::Value::Text(s) => format!("'{s}'"),
                rusqlite::types::Value::Blob(b) => {
                    format!(
                        "X'{}'",
                        b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                    )
                }
            });
        }
        Ok(out)
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

fn explain_integer(values: &[SqliteValue], index: usize) -> Option<i64> {
    match values.get(index) {
        Some(SqliteValue::Integer(value)) => Some(*value),
        _ => None,
    }
}

/// The effective literal top-N bound of the first `SorterOpen`, or `None` if there is no sorter.
///
/// Modern programs set `SORTER_OPEN_TOP_N_REGISTER`, making P3 a register rather than an immediate.
/// Interpret the integer setup opcodes before `SorterOpen` so this oracle verifies the effective
/// runtime bound instead of mistaking a register number for the bound itself.
async fn sorter_top_n(conn: &Connection, sql: &str) -> Option<i64> {
    let rows = conn.query(&format!("EXPLAIN {sql}")).await.unwrap();
    let mut registers = HashMap::new();
    for row in &rows {
        let vals = row.values();
        let Some(SqliteValue::Text(op)) = vals.get(1) else {
            continue;
        };
        match op.as_ref() {
            "Integer" => {
                if let (Some(value), Some(target)) =
                    (explain_integer(vals, 2), explain_integer(vals, 3))
                {
                    registers.insert(target, value);
                }
            }
            "MemMax" => {
                if let (Some(source), Some(target)) =
                    (explain_integer(vals, 2), explain_integer(vals, 3))
                    && let (Some(source_value), Some(target_value)) = (
                        registers.get(&source).copied(),
                        registers.get(&target).copied(),
                    )
                {
                    registers.insert(target, source_value.max(target_value));
                }
            }
            "OffsetLimit" => {
                if let (Some(limit), Some(offset), Some(target)) = (
                    explain_integer(vals, 2),
                    explain_integer(vals, 3),
                    explain_integer(vals, 4),
                ) && let (Some(limit), Some(offset)) = (
                    registers.get(&limit).copied(),
                    registers.get(&offset).copied(),
                ) {
                    registers.insert(
                        target,
                        if limit < 0 {
                            -1
                        } else {
                            limit.saturating_add(offset)
                        },
                    );
                }
            }
            "SorterOpen" => {
                let p3 = explain_integer(vals, 4).unwrap_or(0);
                let p5 = explain_integer(vals, 6).unwrap_or(0);
                if (p5 & i64::from(SORTER_OPEN_TOP_N_REGISTER)) == 0 {
                    return Some(p3);
                }
                return Some(*registers.get(&p3).unwrap_or_else(|| {
                    panic!("SorterOpen reads uninitialized top-N register {p3}: `{sql}`")
                }));
            }
            _ => {}
        }
    }
    None
}

#[test]
fn ordered_limit_offset_bounded_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.expect("frank");
        let r = rusqlite::Connection::open_in_memory().expect("sqlite");
        let create = "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT, k INTEGER);";
        f.execute(create).await.unwrap();
        r.execute_batch(create).unwrap();
        // Tie-heavy v (only 50 distinct values across 2000 rows) so the bounded top-N tie handling is
        // exercised; ORDER BY v, id gives a deterministic total order (id is unique).
        for i in 1..=2000_i64 {
            let v = format!("v{:03}", (i.wrapping_mul(7)) % 50);
            let k = i % 13;
            let stmt = format!("INSERT INTO t VALUES ({i}, '{v}', {k});");
            f.execute(&stmt).await.unwrap();
            r.execute_batch(&stmt).unwrap();
        }
        for s in [
            "INSERT INTO t VALUES (5001, NULL, NULL);",
            "INSERT INTO t VALUES (5002, NULL, 7);",
        ] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }

        for sql in [
            // Small bounded heaps.
            "SELECT id, v FROM t ORDER BY v, id LIMIT 10 OFFSET 100",
            "SELECT id, v FROM t ORDER BY v, id LIMIT 10 OFFSET 0",
            "SELECT id, v FROM t ORDER BY v, id LIMIT 1 OFFSET 500",
            "SELECT id, v FROM t ORDER BY v DESC, id DESC LIMIT 20 OFFSET 50",
            "SELECT id, k FROM t ORDER BY k, id LIMIT 15 OFFSET 200",
            "SELECT id, v, k FROM t ORDER BY v, k, id LIMIT 7 OFFSET 300",
            "SELECT id FROM t ORDER BY v, id LIMIT 5 OFFSET 1019",
            // Deep offsets and edges must still match.
            "SELECT id, v FROM t ORDER BY v, id LIMIT 5 OFFSET 1020",
            "SELECT id, v FROM t ORDER BY v, id LIMIT 10 OFFSET 5000",
            "SELECT id FROM t ORDER BY v, id LIMIT 0 OFFSET 5",
            "SELECT id, v FROM t ORDER BY v, id", // no limit
            // NULLs ordering + offset.
            "SELECT id, v FROM t ORDER BY v, id LIMIT 3 OFFSET 1",
            "SELECT id, v FROM t ORDER BY v DESC, id LIMIT 3 OFFSET 1",
        ] {
            assert_eq!(
                frank_rows(&f, sql).await,
                sqlite_rows(&r, sql),
                "LIMIT/OFFSET sort diverged: `{sql}`"
            );
        }

        // Opcode gate: the runtime sorter bound is LIMIT + max(OFFSET, 0).
        assert_eq!(
            sorter_top_n(&f, "SELECT id, v FROM t ORDER BY v, id LIMIT 10 OFFSET 100").await,
            Some(110),
            "LIMIT 10 OFFSET 100 must bound the sorter to 110"
        );
        assert_eq!(
            sorter_top_n(
                &f,
                "SELECT id, v FROM t ORDER BY v, id LIMIT 10 OFFSET 5000"
            )
            .await,
            Some(5010),
            "deep pagination must retain LIMIT + OFFSET rows"
        );
        assert_eq!(
            sorter_top_n(&f, "SELECT id, v FROM t ORDER BY v, id LIMIT 10").await,
            Some(10),
            "bare LIMIT 10 stays bounded to 10 (unchanged)"
        );
    });
}
