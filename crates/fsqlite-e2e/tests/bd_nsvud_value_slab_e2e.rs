//! Value slab allocator E2E tests (bd-nsvud).
//!
//! Exercises the thread-local value slab through real SQL execution paths
//! to verify that pooled value reuse doesn't corrupt data at the application
//! level.
//!
//! ## Scenarios
//!
//! | ID | Name               | Description                                      |
//! |----|--------------------|--------------------------------------------------|
//! | E1 | insert_10k         | 10K row INSERT + full readback + oracle compare   |
//! | E2 | mixed_types        | All column types: INTEGER, TEXT, BLOB, REAL, NULL  |
//! | E3 | insert_readback_cycle | Repeated INSERT-then-SELECT cycles to stress pool |
//!
//! ## Run
//!
//! ```sh
//! cargo test -p fsqlite-e2e --test bd_nsvud_value_slab_e2e -- --nocapture --test-threads=1
//! ```

#![allow(clippy::cast_precision_loss)]

use serde_json::json;

const BEAD_ID: &str = "bd-nsvud";
const REPLAY_CMD: &str =
    "cargo test -p fsqlite-e2e --test bd_nsvud_value_slab_e2e -- --nocapture --test-threads=1";

type MixedOracleRow = (
    i64,
    Option<i64>,
    Option<String>,
    Option<f64>,
    String,
    Option<Vec<u8>>,
    Option<String>,
);

fn emit_log(test_name: &str, phase: &str, data: serde_json::Value) {
    eprintln!(
        "VALUE_SLAB_E2E:{}",
        json!({
            "bead_id": BEAD_ID,
            "test": test_name,
            "phase": phase,
            "replay_command": REPLAY_CMD,
            "data": data,
        })
    );
}

fn integer_value(
    value: &fsqlite_types::value::SqliteValue,
    context: String,
) -> Result<i64, String> {
    match value {
        fsqlite_types::value::SqliteValue::Integer(n) => Ok(*n),
        other => Err(format!("{context}: expected integer, got {other:?}")),
    }
}

fn text_value(value: &fsqlite_types::value::SqliteValue, context: String) -> Result<&str, String> {
    match value {
        fsqlite_types::value::SqliteValue::Text(s) => Ok(s.as_str()),
        other => Err(format!("{context}: expected text, got {other:?}")),
    }
}

// ─── E1: 10K row INSERT + oracle comparison ──────────────────────────

#[test]
fn e1_insert_10k_oracle_comparison() -> Result<(), String> {
    let test_name = "e1_insert_10k";
    let row_count = 10_000i64;

    emit_log(test_name, "start", json!({"rows": row_count}));

    let fconn = fsqlite::Connection::open(":memory:").expect("fsqlite open");
    fconn
        .execute(
            "CREATE TABLE slab_test (id INTEGER PRIMARY KEY, thread_id INTEGER NOT NULL, val INTEGER NOT NULL, label TEXT NOT NULL)",
        )
        .expect("fsqlite create");

    let cconn = rusqlite::Connection::open_in_memory().expect("csqlite open");
    cconn
        .execute_batch(
            "CREATE TABLE slab_test (id INTEGER PRIMARY KEY, thread_id INTEGER NOT NULL, val INTEGER NOT NULL, label TEXT NOT NULL);",
        )
        .expect("csqlite create");

    // Insert into both engines
    fconn.execute("BEGIN").expect("fsqlite begin");
    cconn.execute_batch("BEGIN;").expect("csqlite begin");
    for i in 0..row_count {
        let val = i * 7 + 13;
        let label = format!("row_{i:05}");
        fconn
            .execute(&format!(
                "INSERT INTO slab_test (id, thread_id, val, label) VALUES ({i}, 0, {val}, '{label}')"
            ))
            .expect("fsqlite insert");
        cconn
            .execute(
                "INSERT INTO slab_test (id, thread_id, val, label) VALUES (?1, 0, ?2, ?3)",
                rusqlite::params![i, val, label],
            )
            .expect("csqlite insert");
    }
    fconn.execute("COMMIT").expect("fsqlite commit");
    cconn.execute_batch("COMMIT;").expect("csqlite commit");

    // Verify row counts match
    let f_rows = fconn
        .query("SELECT COUNT(*) FROM slab_test")
        .expect("fsqlite count");
    let f_count = integer_value(&f_rows[0].values()[0], "count result".to_owned())?;
    let c_count: i64 = cconn
        .query_row("SELECT COUNT(*) FROM slab_test", [], |row| row.get(0))
        .expect("csqlite count");

    assert_eq!(f_count, c_count, "row count mismatch");
    assert_eq!(f_count, row_count);

    // Full readback and compare: SELECT all rows ordered by id
    let f_data = fconn
        .query("SELECT id, thread_id, val, label FROM slab_test ORDER BY id")
        .expect("fsqlite select");

    let mut c_stmt = cconn
        .prepare("SELECT id, thread_id, val, label FROM slab_test ORDER BY id")
        .expect("csqlite prepare");
    let c_data: Vec<(i64, i64, i64, String)> = c_stmt
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .expect("csqlite query")
        .map(|r| r.expect("row"))
        .collect();

    assert_eq!(f_data.len(), c_data.len(), "row count mismatch in readback");

    let mut mismatches = 0u64;
    for (i, (f_row, c_row)) in f_data.iter().zip(c_data.iter()).enumerate() {
        let f_vals = f_row.values();
        let f_id = integer_value(&f_vals[0], format!("row {i} id"))?;
        let f_tid = integer_value(&f_vals[1], format!("row {i} thread_id"))?;
        let f_val = integer_value(&f_vals[2], format!("row {i} val"))?;
        let f_label = text_value(&f_vals[3], format!("row {i} label"))?.to_owned();

        if f_id != c_row.0 || f_tid != c_row.1 || f_val != c_row.2 || f_label != c_row.3 {
            mismatches += 1;
            if mismatches <= 5 {
                emit_log(
                    test_name,
                    "mismatch",
                    json!({
                        "row": i,
                        "fsqlite": {"id": f_id, "tid": f_tid, "val": f_val, "label": f_label},
                        "csqlite": {"id": c_row.0, "tid": c_row.1, "val": c_row.2, "label": c_row.3},
                    }),
                );
            }
        }
    }

    emit_log(
        test_name,
        "result",
        json!({
            "rows_compared": row_count,
            "mismatches": mismatches,
            "oracle_match": mismatches == 0,
        }),
    );

    assert_eq!(
        mismatches, 0,
        "[E1] {mismatches} value mismatches between fsqlite and csqlite"
    );
    Ok(())
}

// ─── E2: Mixed types (INTEGER, TEXT, BLOB, REAL, NULL) ───────────────

#[test]
fn e2_mixed_types() -> Result<(), String> {
    let test_name = "e2_mixed_types";

    emit_log(test_name, "start", json!({}));

    let fconn = fsqlite::Connection::open(":memory:").expect("fsqlite open");
    fconn
        .execute(
            "CREATE TABLE mixed (id INTEGER PRIMARY KEY, int_col INTEGER, text_col TEXT, real_col REAL, blob_col BLOB, null_col TEXT)",
        )
        .expect("create");

    let cconn = rusqlite::Connection::open_in_memory().expect("csqlite open");
    cconn
        .execute_batch(
            "CREATE TABLE mixed (id INTEGER PRIMARY KEY, int_col INTEGER, text_col TEXT, real_col REAL, blob_col BLOB, null_col TEXT);",
        )
        .expect("create");

    let test_cases: Vec<(i64, &str)> = vec![
        (
            1,
            "INSERT INTO mixed VALUES (1, 42, 'hello', 3.14, X'DEADBEEF', NULL)",
        ),
        (
            2,
            "INSERT INTO mixed VALUES (2, -999999, '', 0.0, X'', NULL)",
        ),
        (
            3,
            "INSERT INTO mixed VALUES (3, 0, 'a string with ''quotes''', -1.5e10, X'0102030405060708', NULL)",
        ),
        (
            4,
            "INSERT INTO mixed VALUES (4, 9223372036854775807, 'max_int_neighbor', 1.7976931348623157e308, X'FF', NULL)",
        ),
        (
            5,
            "INSERT INTO mixed VALUES (5, -9223372036854775808, 'min_int', -1.7976931348623157e308, X'00', NULL)",
        ),
        (
            6,
            "INSERT INTO mixed VALUES (6, NULL, NULL, NULL, NULL, NULL)",
        ),
        (
            7,
            "INSERT INTO mixed VALUES (7, 1, 'unicode: こんにちは 🌍', 2.718281828, X'CAFEBABE', 'not null')",
        ),
        (
            8,
            "INSERT INTO mixed VALUES (8, 100, 'trailing spaces   ', 0.1, X'0000000000', NULL)",
        ),
    ];

    for (_id, sql) in &test_cases {
        fconn.execute(sql).expect("fsqlite insert");
        cconn
            .execute_batch(&format!("{sql};"))
            .expect("csqlite insert");
    }

    // Compare all rows
    let f_rows = fconn
        .query(
            "SELECT id, int_col, text_col, real_col, typeof(real_col), blob_col, null_col FROM mixed ORDER BY id",
        )
        .expect("fsqlite select");
    let c_rows: Vec<MixedOracleRow> = {
        let mut stmt = cconn
            .prepare(
                "SELECT id, int_col, text_col, real_col, typeof(real_col), blob_col, null_col FROM mixed ORDER BY id",
            )
            .expect("prepare");
        stmt.query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
            ))
        })
        .expect("query")
        .map(|r| r.expect("row"))
        .collect()
    };

    assert_eq!(f_rows.len(), c_rows.len(), "row count mismatch");
    assert_eq!(f_rows.len(), test_cases.len());

    let mut type_checks_passed = 0u64;
    for (i, (f_row, c_row)) in f_rows.iter().zip(c_rows.iter()).enumerate() {
        let f_vals = f_row.values();

        // Verify id
        let f_id = integer_value(&f_vals[0], format!("row {i} id"))?;
        assert_eq!(f_id, c_row.0, "row {i}: id mismatch");

        // Verify int_col (may be NULL)
        match (&f_vals[1], &c_row.1) {
            (fsqlite_types::value::SqliteValue::Null, None) => {}
            (fsqlite_types::value::SqliteValue::Integer(f), Some(c)) => {
                assert_eq!(*f, *c, "row {i}: int_col mismatch");
            }
            (f, c) => {
                return Err(format!(
                    "row {i}: int_col type mismatch: fsqlite={f:?}, csqlite={c:?}"
                ));
            }
        }

        // Verify text_col (may be NULL)
        match (&f_vals[2], &c_row.2) {
            (fsqlite_types::value::SqliteValue::Null, None) => {}
            (fsqlite_types::value::SqliteValue::Text(f), Some(c)) => {
                assert_eq!(f.as_str(), c.as_str(), "row {i}: text_col mismatch");
            }
            (f, c) => {
                return Err(format!(
                    "row {i}: text_col type mismatch: fsqlite={f:?}, csqlite={c:?}"
                ));
            }
        }

        // Verify real_col (may be NULL)
        match (&f_vals[3], &c_row.3) {
            (fsqlite_types::value::SqliteValue::Null, None) => {}
            (fsqlite_types::value::SqliteValue::Float(f), Some(c)) => {
                assert_eq!(*f, *c, "row {i}: real_col mismatch");
            }
            (f, c) => {
                return Err(format!(
                    "row {i}: real_col mismatch: fsqlite={f:?}, csqlite={c:?}"
                ));
            }
        }

        // Verify typeof(real_col) — compare type name string
        match (&f_vals[4], &c_row.4) {
            (fsqlite_types::value::SqliteValue::Text(f), c) => {
                assert_eq!(f.as_str(), c.as_str(), "row {i}: real_col typeof mismatch");
            }
            (f, c) => {
                return Err(format!(
                    "row {i}: typeof(real_col) mismatch: fsqlite={f:?}, csqlite={c:?}"
                ));
            }
        }

        // Verify blob_col (may be NULL)
        match (&f_vals[5], &c_row.5) {
            (fsqlite_types::value::SqliteValue::Null, None) => {}
            (fsqlite_types::value::SqliteValue::Blob(f), Some(c)) => {
                assert_eq!(f.as_ref(), c.as_slice(), "row {i}: blob_col mismatch");
            }
            (f, c) => {
                return Err(format!(
                    "row {i}: blob_col mismatch: fsqlite={f:?}, csqlite={c:?}"
                ));
            }
        }

        // Verify null_col
        match (&f_vals[6], &c_row.6) {
            (fsqlite_types::value::SqliteValue::Null, None) => {}
            (fsqlite_types::value::SqliteValue::Text(f), Some(c)) => {
                assert_eq!(f.as_str(), c.as_str(), "row {i}: null_col mismatch");
            }
            (f, c) => {
                return Err(format!(
                    "row {i}: null_col mismatch: fsqlite={f:?}, csqlite={c:?}"
                ));
            }
        }

        type_checks_passed += 1;
    }

    emit_log(
        test_name,
        "result",
        json!({
            "rows_checked": test_cases.len(),
            "type_checks_passed": type_checks_passed,
            "all_types_verified": true,
        }),
    );

    assert_eq!(type_checks_passed, test_cases.len() as u64);
    Ok(())
}

// ─── E3: INSERT-then-SELECT cycles to stress pool turnover ───────────

#[test]
fn e3_insert_readback_cycle() -> Result<(), String> {
    let test_name = "e3_insert_readback_cycle";
    let cycles = 50u64;
    let rows_per_cycle = 200u64;

    emit_log(
        test_name,
        "start",
        json!({"cycles": cycles, "rows_per_cycle": rows_per_cycle}),
    );

    let fconn = fsqlite::Connection::open(":memory:").expect("open");
    fconn
        .execute("CREATE TABLE cycle_test (id INTEGER PRIMARY KEY, cycle INTEGER NOT NULL, val TEXT NOT NULL)")
        .expect("create");

    let cconn = rusqlite::Connection::open_in_memory().expect("csqlite open");
    cconn
        .execute_batch("CREATE TABLE cycle_test (id INTEGER PRIMARY KEY, cycle INTEGER NOT NULL, val TEXT NOT NULL);")
        .expect("csqlite create");

    let mut total_mismatches = 0u64;
    let mut total_rows = 0u64;

    for cycle in 0..cycles {
        let base = cycle * rows_per_cycle;

        // Insert batch
        fconn.execute("BEGIN").expect("begin");
        cconn.execute_batch("BEGIN;").expect("c begin");
        for i in 0..rows_per_cycle {
            let id = base + i;
            let val = format!("c{cycle}_r{i}_{}", id * 3 + 7);
            fconn
                .execute(&format!(
                    "INSERT INTO cycle_test VALUES ({id}, {cycle}, '{val}')"
                ))
                .expect("f insert");
            cconn
                .execute(
                    "INSERT INTO cycle_test VALUES (?1, ?2, ?3)",
                    rusqlite::params![id as i64, cycle as i64, val],
                )
                .expect("c insert");
        }
        fconn.execute("COMMIT").expect("commit");
        cconn.execute_batch("COMMIT;").expect("c commit");

        // Readback and compare this cycle's rows
        let f_rows = fconn
            .query(&format!(
                "SELECT id, cycle, val FROM cycle_test WHERE cycle = {cycle} ORDER BY id"
            ))
            .expect("f select");

        let c_rows: Vec<(i64, i64, String)> = {
            let mut stmt = cconn
                .prepare("SELECT id, cycle, val FROM cycle_test WHERE cycle = ?1 ORDER BY id")
                .expect("prepare");
            stmt.query_map(rusqlite::params![cycle as i64], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .expect("query")
            .map(|r| r.expect("row"))
            .collect()
        };

        assert_eq!(
            f_rows.len(),
            c_rows.len(),
            "cycle {cycle}: row count mismatch"
        );

        for (f_row, c_row) in f_rows.iter().zip(c_rows.iter()) {
            let f_vals = f_row.values();
            let f_id = integer_value(&f_vals[0], "cycle row id".to_owned())?;
            let f_val = text_value(&f_vals[2], "cycle row val".to_owned())?;

            if f_id != c_row.0 || f_val != c_row.2 {
                total_mismatches += 1;
            }
        }

        total_rows += rows_per_cycle;
    }

    emit_log(
        test_name,
        "result",
        json!({
            "cycles": cycles,
            "total_rows": total_rows,
            "total_mismatches": total_mismatches,
            "pool_stress_passed": total_mismatches == 0,
        }),
    );

    assert_eq!(
        total_mismatches, 0,
        "[E3] {total_mismatches} mismatches across {cycles} INSERT-readback cycles"
    );
    Ok(())
}
