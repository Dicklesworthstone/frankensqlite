//! Stock-oracle keeper for `INSERT ... ON CONFLICT(<rowid alias>) DO UPDATE`
//! on a TEMP table.
//!
//! The main-schema form of this statement has long been covered; the TEMP-schema
//! form was not. It surfaced while bringing up the bd-fjieg.5 generated-column
//! oracle, whose TEMP arm is simply the first matrix cell that ever reached an
//! upsert on a TEMP table.
//!
//! Deliberately uses a table with **no generated columns at all**, so the
//! bd-fjieg.5/.6 codegen paths (`emit_virtual_generated_column`, the VIRTUAL
//! branch of `emit_not_null_constraints`) are inert here — they are entered only
//! when a column carries a generated expression. A failure in this file is
//! therefore independent of that work.
//!
//! State of the three symptoms originally filed as bd-29phg:
//!   1. upsert inserting a duplicate — FIXED, guarded by
//!      `temp_table_upsert_on_rowid_alias_matches_stock_sqlite` (runs by default);
//!   2. a failed statement leaving partial rows — open as **bd-5bq6u**,
//!      `temp_table_failed_multi_row_insert_is_atomic_like_stock` (`#[ignore]`d);
//!   3. a UNIQUE violation naming the wrong column — open as **bd-towj6**,
//!      visible in the Q5 line of `temp_table_constraint_lane_diagnostic`.
//!
//! Three `#[ignore]`d investigation aids print characterisations rather than
//! asserting — `temp_table_constraint_lane_diagnostic` (which layer of the TEMP
//! lane is wrong), `temp_vs_main_upsert_program_dump` (the program comparison
//! that ruled codegen out for symptom 1), and
//! `temp_statement_atomicity_blast_radius` (which statement shapes bd-5bq6u
//! affects). Run them with
//! `cargo test -p fsqlite-core --test temp_table_upsert_rowid_conflict_oracle -- --ignored --nocapture`.

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;

/// Read `SELECT id, v FROM <table> ORDER BY id` from both engines and compare.
async fn assert_same_rows(conn: &Connection, stock: &rusqlite::Connection, table: &str, ctx: &str) {
    let sql = format!("SELECT id, v FROM {table} ORDER BY id");
    let expected = stock
        .prepare(&sql)
        .unwrap()
        .query_map([], |row| {
            Ok(vec![
                SqliteValue::Integer(row.get(0)?),
                SqliteValue::Integer(row.get(1)?),
            ])
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    let actual = conn
        .query(&sql)
        .await
        .unwrap_or_else(|error| panic!("{ctx}: read: {error}"))
        .iter()
        .map(|row| row.values().to_vec())
        .collect::<Vec<_>>();
    assert_eq!(actual, expected, "{ctx}");
}

/// Regression guard for **bd-29phg symptom 1** — now GREEN.
///
/// Before the fix this failed on the `temporary=true` arm: `INSERT INTO g(id,v)
/// VALUES(1,13) ON CONFLICT(id) DO UPDATE SET v=excluded.v` produced
/// `[[1,5],[2,7],[3,13]]` where stock gives `[[1,13],[2,7]]` — the update never
/// happened and a duplicate row appeared at a fresh rowid.
///
/// Cause: the MemDatabase branch of the seek opcodes never positioned its
/// cursor, so `Delete` in the DO UPDATE body was a silent no-op (it requires
/// `MemCursor.position`) and the re-insert then drew a new rowid. See the
/// `NotFound`/`NotExists` and `Found` arms in `fsqlite-vdbe/src/engine.rs`.
#[test]
fn temp_table_upsert_on_rowid_alias_matches_stock_sqlite() {
    asupersync::test_utils::run_test(|| async {
        for temporary in [false, true] {
            let keyword = if temporary { "TEMP " } else { "" };
            let ctx = format!("temporary={temporary}");

            let conn = Connection::open(":memory:").await.unwrap();
            let stock = rusqlite::Connection::open_in_memory().unwrap();

            let ddl = format!("CREATE {keyword}TABLE g(id INTEGER PRIMARY KEY, v INTEGER)");
            stock.execute_batch(&ddl).unwrap();
            conn.execute(&ddl).await.unwrap();

            for sql in [
                "INSERT INTO g(v) VALUES(5)",
                "INSERT INTO g(v) VALUES(7)",
                // The statement under test: an explicit rowid that collides
                // with an existing row must take the DO UPDATE branch, not
                // insert a fresh row under a newly allocated rowid.
                "INSERT INTO g(id,v) VALUES(1,13) ON CONFLICT(id) DO UPDATE SET v=excluded.v",
                // The DO NOTHING branch of the same shape.
                "INSERT INTO g(id,v) VALUES(2,99) ON CONFLICT(id) DO NOTHING",
                // A non-colliding explicit rowid still inserts.
                "INSERT INTO g(id,v) VALUES(9,21) ON CONFLICT(id) DO UPDATE SET v=excluded.v",
            ] {
                let expected = stock.execute(sql, []);
                let actual = conn.execute(sql).await;
                let ctx = format!("{ctx}, sql={sql}");
                assert_eq!(
                    actual.unwrap_or_else(|error| panic!("{ctx}: {error}")),
                    expected.unwrap_or_else(|error| panic!("stock: {ctx}: {error}")),
                    "{ctx}: affected row count"
                );
                assert_same_rows(&conn, &stock, "g", &ctx).await;
            }

            conn.close().await.unwrap();
        }
    });
}

/// KNOWN-RED on the `temporary=true` arm — executable repro for **bd-29phg**'s
/// second symptom: a multi-row INSERT that violates NOT NULL on its *second*
/// row must leave the table untouched, but on a TEMP table the first row
/// survives the failed statement.
///
/// Confirmed INDEPENDENT of symptom 1: still red after the cursor-positioning
/// fix that turned the upsert guard above green, so this is a
/// statement-rollback problem rather than a cursor-position one.
///
/// Again deliberately **no generated columns**, so this is independent of
/// bd-fjieg.5. It is the plain-NOT-NULL analogue of the divergence the .5
/// oracle hits on its TEMP arm with `INSERT INTO g(v) VALUES(19),(0)`.
#[test]
#[ignore = "bd-5bq6u: a failed statement leaves partial rows on a TEMP table"]
fn temp_table_failed_multi_row_insert_is_atomic_like_stock() {
    asupersync::test_utils::run_test(|| async {
        for temporary in [false, true] {
            let keyword = if temporary { "TEMP " } else { "" };
            let ctx = format!("temporary={temporary}");

            let conn = Connection::open(":memory:").await.unwrap();
            let stock = rusqlite::Connection::open_in_memory().unwrap();

            let ddl = format!("CREATE {keyword}TABLE g(id INTEGER PRIMARY KEY, v INTEGER NOT NULL)");
            stock.execute_batch(&ddl).unwrap();
            conn.execute(&ddl).await.unwrap();

            for sql in ["INSERT INTO g(v) VALUES(5)", "INSERT INTO g(v) VALUES(7)"] {
                stock.execute(sql, []).unwrap();
                conn.execute(sql).await.unwrap();
            }

            // Second row violates NOT NULL: the whole statement must roll back.
            let sql = "INSERT INTO g(v) VALUES(19),(NULL)";
            let ctx = format!("{ctx}, sql={sql}");
            let stock_error = stock.execute(sql, []).unwrap_err().to_string();
            assert!(stock_error.contains("NOT NULL"), "stock: {ctx}: {stock_error}");
            let error = conn.execute(sql).await.expect_err(&ctx).to_string();
            assert!(error.contains("NOT NULL"), "{ctx}: {error}");

            assert_same_rows(&conn, &stock, "g", &ctx).await;

            conn.close().await.unwrap();
        }
    });
}

/// Diagnostic ladder for **bd-29phg**: isolates *which* layer of the TEMP lane
/// is wrong, by asking the narrowest questions in order. Each step prints its
/// result rather than stopping at the first divergence, so one run characterises
/// the whole defect.
///
/// Q1/Q2 — can the TEMP lane even *find* a row by its rowid alias? The upsert
/// conflict probe locates the conflicting row with exactly such a SELECT.
/// Q3 — is the implicit rowid/PRIMARY KEY uniqueness enforced at all?
/// Q4 — is an explicit rowid honoured on insert?
/// Q5 — is a secondary UNIQUE index enforced?
#[test]
#[ignore = "bd-29phg diagnostic ladder; prints a characterisation, run explicitly"]
fn temp_table_constraint_lane_diagnostic() {
    asupersync::test_utils::run_test(|| async {
        for temporary in [false, true] {
            let keyword = if temporary { "TEMP " } else { "" };
            println!("\n######## temporary={temporary} ########");

            let conn = Connection::open(":memory:").await.unwrap();
            let stock = rusqlite::Connection::open_in_memory().unwrap();
            let ddl = format!("CREATE {keyword}TABLE g(id INTEGER PRIMARY KEY, v INTEGER)");
            stock.execute_batch(&ddl).unwrap();
            conn.execute(&ddl).await.unwrap();
            for sql in ["INSERT INTO g(v) VALUES(5)", "INSERT INTO g(v) VALUES(7)"] {
                stock.execute(sql, []).unwrap();
                conn.execute(sql).await.unwrap();
            }

            for (label, sql) in [
                ("Q1 find by ipk      ", "SELECT id,v FROM g WHERE id=1"),
                ("Q2 project rowid    ", "SELECT rowid,id,v FROM g WHERE id=1"),
            ] {
                let got = conn.query(sql).await;
                println!(
                    "{label} {sql}\n      fsqlite = {:?}",
                    got.map(|rows| rows
                        .iter()
                        .map(|r| r.values().to_vec())
                        .collect::<Vec<_>>())
                );
            }

            for (label, sql) in [
                ("Q3 dup explicit pk  ", "INSERT INTO g(id,v) VALUES(1,99)"),
                ("Q4 fresh explicit pk", "INSERT INTO g(id,v) VALUES(50,50)"),
            ] {
                let stock_res = stock.execute(sql, []).map_err(|e| e.to_string());
                let fs_res = conn.execute(sql).await.map_err(|e| e.to_string());
                println!("{label} {sql}\n      stock   = {stock_res:?}\n      fsqlite = {fs_res:?}");
            }

            let idx = "CREATE UNIQUE INDEX gu ON g(v)";
            stock.execute_batch(idx).unwrap();
            conn.execute(idx).await.unwrap();
            let sql = "INSERT INTO g(v) VALUES(7)";
            let stock_res = stock.execute(sql, []).map_err(|e| e.to_string());
            let fs_res = conn.execute(sql).await.map_err(|e| e.to_string());
            println!("Q5 unique index      {sql}\n      stock   = {stock_res:?}\n      fsqlite = {fs_res:?}");

            let read = "SELECT id,v FROM g ORDER BY id";
            let expected = stock
                .prepare(read)
                .unwrap()
                .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            let actual = conn.query(read).await.unwrap();
            println!(
                "FINAL stock   = {expected:?}\nFINAL fsqlite = {:?}",
                actual.iter().map(|r| r.values().to_vec()).collect::<Vec<_>>()
            );

            conn.close().await.unwrap();
        }
    });
}

/// bd-29phg step 2: dump the compiled program for the *same* upsert statement
/// against a main-schema table and a TEMP table, and print them side by side.
///
/// The live hypothesis is that on the TEMP lane the conflict probe tests a
/// freshly allocated rowid rather than the explicit one, which would explain
/// both the missed conflict and the row landing at max(rowid)+1. That is a
/// claim about which instruction populates the register feeding
/// `NotExists`/`NotFound`, so the two programs are the evidence.
///
/// Caveat to keep in mind when reading the output: `EXPLAIN` renders
/// `try_compile_statement`, and TEMP inserts are deliberately routed away from
/// the direct lane (connection.rs ~44955, GH#290). If the two programs come
/// back identical, that does NOT clear codegen — it means EXPLAIN is not
/// showing the program TEMP execution actually runs, and the next step is to
/// instrument the executing lane instead.
#[test]
#[ignore = "bd-29phg program dump; prints evidence, run explicitly"]
fn temp_vs_main_upsert_program_dump() {
    asupersync::test_utils::run_test(|| async {
        const UPSERT: &str =
            "INSERT INTO g(id,v) VALUES(1,13) ON CONFLICT(id) DO UPDATE SET v=excluded.v";

        for temporary in [false, true] {
            let keyword = if temporary { "TEMP " } else { "" };
            println!("\n######## temporary={temporary} ########");

            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute(&format!(
                "CREATE {keyword}TABLE g(id INTEGER PRIMARY KEY, v INTEGER)"
            ))
            .await
            .unwrap();
            conn.execute("INSERT INTO g(v) VALUES(5)").await.unwrap();
            conn.execute("INSERT INTO g(v) VALUES(7)").await.unwrap();

            match conn.query(&format!("EXPLAIN {UPSERT}")).await {
                Ok(rows) => {
                    for row in &rows {
                        let v = row.values();
                        let cell = |i: usize| {
                            v.get(i).map_or_else(
                                || "-".to_owned(),
                                |value| match value {
                                    SqliteValue::Integer(n) => n.to_string(),
                                    SqliteValue::Text(t) => t.as_str().to_owned(),
                                    other => format!("{other:?}"),
                                },
                            )
                        };
                        println!(
                            "{:>4}  {:<18} p1={:<6} p2={:<6} p3={:<6} p4={}",
                            cell(0),
                            cell(1),
                            cell(2),
                            cell(3),
                            cell(4),
                            cell(5)
                        );
                    }
                }
                Err(error) => println!("EXPLAIN failed: {error}"),
            }

            conn.close().await.unwrap();
        }
    });
}

/// bd-5bq6u blast-radius probe: is the missing statement-level undo on the TEMP
/// lane confined to INSERT, or does it also affect UPDATE and INSERT ... SELECT?
///
/// That question decides the shape of the fix — an INSERT-only undo log needs
/// just the inserted rowids, whereas UPDATE needs pre-images — so measure it
/// rather than assume. Prints every case instead of stopping at the first
/// divergence.
#[test]
#[ignore = "bd-5bq6u blast-radius probe; prints a characterisation, run explicitly"]
fn temp_statement_atomicity_blast_radius() {
    asupersync::test_utils::run_test(|| async {
        let cases: [(&str, &[&str], &str); 4] = [
            (
                "multi-row INSERT, row 2 fails",
                &["INSERT INTO g(v) VALUES(5)", "INSERT INTO g(v) VALUES(7)"],
                "INSERT INTO g(v) VALUES(19),(NULL)",
            ),
            (
                "multi-row INSERT, row 3 fails",
                &["INSERT INTO g(v) VALUES(5)"],
                "INSERT INTO g(v) VALUES(19),(21),(NULL)",
            ),
            (
                "multi-row UPDATE, later row fails",
                &[
                    "INSERT INTO g(v) VALUES(5)",
                    "INSERT INTO g(v) VALUES(7)",
                    "INSERT INTO g(v) VALUES(9)",
                ],
                "UPDATE g SET v = CASE WHEN id < 3 THEN v + 100 ELSE NULL END",
            ),
            (
                "INSERT ... SELECT, later row fails",
                &["INSERT INTO g(v) VALUES(5)"],
                "INSERT INTO g(v) SELECT CASE WHEN x = 2 THEN NULL ELSE x END \
                 FROM (SELECT 1 AS x UNION ALL SELECT 2)",
            ),
        ];

        for temporary in [false, true] {
            let keyword = if temporary { "TEMP " } else { "" };
            println!("\n######## temporary={temporary} ########");

            for (label, setup, failing) in &cases {
                let conn = Connection::open(":memory:").await.unwrap();
                let stock = rusqlite::Connection::open_in_memory().unwrap();
                let ddl =
                    format!("CREATE {keyword}TABLE g(id INTEGER PRIMARY KEY, v INTEGER NOT NULL)");
                stock.execute_batch(&ddl).unwrap();
                conn.execute(&ddl).await.unwrap();
                for sql in *setup {
                    stock.execute(sql, []).unwrap();
                    conn.execute(sql).await.unwrap();
                }

                let stock_failed = stock.execute(failing, []).is_err();
                let fsqlite_failed = conn.execute(failing).await.is_err();

                let read = "SELECT id,v FROM g ORDER BY id";
                let expected = stock
                    .prepare(read)
                    .unwrap()
                    .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
                    .unwrap()
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .unwrap();
                let actual = conn
                    .query(read)
                    .await
                    .map(|rows| {
                        rows.iter()
                            .map(|row| match (&row.values()[0], &row.values()[1]) {
                                (SqliteValue::Integer(a), SqliteValue::Integer(b)) => (*a, *b),
                                _ => (-1, -1),
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();

                println!(
                    "{label}\n  stmt         = {failing}\n  errored      stock={stock_failed} fsqlite={fsqlite_failed}\n  stock rows   = {expected:?}\n  fsqlite rows = {actual:?}\n  ATOMIC       = {}",
                    if expected == actual { "yes" } else { "NO" }
                );

                conn.close().await.unwrap();
            }
        }
    });
}
