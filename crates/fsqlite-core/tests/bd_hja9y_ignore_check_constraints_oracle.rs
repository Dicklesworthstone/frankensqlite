#![recursion_limit = "512"]

//! bd-hja9y: live stock SQLite 3.54.1 differential keeper for
//! `PRAGMA ignore_check_constraints`.
//!
//! Pins:
//! - readback value, column metadata, boolean parsing, and per-connection isolation;
//! - main, TEMP, and ATTACHed schemas (attached before and after toggle, plus
//!   schema-qualified `PRAGMA aux.ignore_check_constraints`);
//! - INSERT, UPDATE, UPSERT DO UPDATE, REPLACE, and WITHOUT ROWID tables, both
//!   direct and prepared, in `:memory:` and file-backed databases;
//! - held prepared statements across OFF -> ON -> OFF and ON -> OFF toggles
//!   (stock's `OP_Expire` re-prepare semantics);
//! - `PRAGMA integrity_check` and `PRAGMA quick_check` both while the flag stays
//!   ON (CHECK violations suppressed) and after turning it OFF (CHECK violations
//!   reported);
//! - negatives: `NOT NULL` and `UNIQUE` remain enforced while ON, and OFF error
//!   text matches stock verbatim.

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

#[derive(Debug, Clone, PartialEq)]
enum Value {
    Null,
    Int(i64),
    Float(u64),
    Text(String),
    Blob(Vec<u8>),
}

impl From<&SqliteValue> for Value {
    fn from(v: &SqliteValue) -> Self {
        match v {
            SqliteValue::Null => Self::Null,
            SqliteValue::Integer(n) => Self::Int(*n),
            SqliteValue::Float(f) => Self::Float(f.to_bits()),
            SqliteValue::Text(s) => Self::Text(s.to_string()),
            SqliteValue::Blob(b) => Self::Blob(b.to_vec()),
        }
    }
}

impl From<rusqlite::types::Value> for Value {
    fn from(v: rusqlite::types::Value) -> Self {
        match v {
            rusqlite::types::Value::Null => Self::Null,
            rusqlite::types::Value::Integer(n) => Self::Int(n),
            rusqlite::types::Value::Real(f) => Self::Float(f.to_bits()),
            rusqlite::types::Value::Text(s) => Self::Text(s),
            rusqlite::types::Value::Blob(b) => Self::Blob(b),
        }
    }
}

fn stock_rows(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<Value>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let ncols = stmt.column_count();
    let mut out = Vec::new();
    let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        let mut r = Vec::with_capacity(ncols);
        for i in 0..ncols {
            let v: rusqlite::types::Value = row.get(i).map_err(|e| e.to_string())?;
            r.push(Value::from(v));
        }
        out.push(r);
    }
    Ok(out)
}

fn stock_columns(conn: &rusqlite::Connection, sql: &str) -> Vec<String> {
    let stmt = conn.prepare(sql).expect("stock prepare");
    stmt.column_names().map(str::to_owned).collect()
}

fn stock_exec(conn: &rusqlite::Connection, sql: &str) -> Result<(), String> {
    conn.execute_batch(sql).map_err(|e| e.to_string())
}

fn fsqlite_to_values(rows: Vec<Row>) -> Vec<Vec<Value>> {
    rows.into_iter()
        .map(|row| row.values().iter().map(Value::from).collect())
        .collect()
}

async fn native_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<Value>>, String> {
    conn.query(sql)
        .await
        .map(fsqlite_to_values)
        .map_err(|e| e.to_string())
}

async fn native_exec(conn: &Connection, sql: &str) -> Result<(), String> {
    conn.execute(sql).await.map(|_| ()).map_err(|e| e.to_string())
}

fn assert_outcome_matches<T: PartialEq + std::fmt::Debug>(
    label: &str,
    native: &Result<T, String>,
    stock: &Result<T, String>,
) {
    match (native, stock) {
        (Ok(n), Ok(s)) => assert_eq!(n, s, "{label}: ok payload mismatch"),
        (Err(n), Err(s)) => assert_eq!(n, s, "{label}: error text mismatch"),
        (n, s) => panic!("{label}: outcome mismatch\n  native: {n:?}\n  stock:  {s:?}"),
    }
}

fn assert_same_error_kind(label: &str, native: &Result<(), String>, stock: &Result<(), String>) {
    match (native, stock) {
        (Ok(()), Ok(())) => {}
        (Err(n), Err(s)) => assert_eq!(n, s, "{label}: error text mismatch"),
        (n, s) => panic!("{label}: outcome mismatch\n  native: {n:?}\n  stock:  {s:?}"),
    }
}

#[test]
fn pragma_readback_boolean_spellings_and_connection_isolation() {
    let dir = tempfile::tempdir().expect("tempdir");
    let stock_path = dir.path().join("stock.db");
    let native_path = dir.path().join("native.db");

    let stock_a = rusqlite::Connection::open(&stock_path).expect("stock open a");
    let stock_b = rusqlite::Connection::open(&stock_path).expect("stock open b");
    stock_a
        .execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, x INTEGER CHECK(x > 0));")
        .expect("stock schema");

    asupersync::test_utils::run_test(|| async {
        let native_a = Connection::open(native_path.to_str().expect("utf8"))
            .await
            .expect("native open a");
        let native_b = Connection::open(native_path.to_str().expect("utf8"))
            .await
            .expect("native open b");
        native_a
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x INTEGER CHECK(x > 0));")
            .await
            .expect("native schema");

        // Default readback is 0, and bare pragma column metadata matches stock.
        let bare = "PRAGMA ignore_check_constraints;";
        let assign = "PRAGMA ignore_check_constraints = ON;";
        assert_eq!(
            native_a
                .prepare(bare)
                .await
                .expect("prep bare")
                .column_names(),
            stock_columns(&stock_a, bare),
            "bare pragma column names"
        );
        assert_outcome_matches(
            "default readback",
            &native_rows(&native_a, bare).await,
            &stock_rows(&stock_a, bare),
        );
        // Check assignment column metadata on a scratch connection because
        // SQLite evaluates PragTyp_FLAG setters during sqlite3_prepare_v2.
        let stock_scratch = rusqlite::Connection::open_in_memory().expect("stock scratch");
        assert_eq!(
            native_a
                .prepare(assign)
                .await
                .expect("prep assign")
                .column_names(),
            stock_columns(&stock_scratch, assign),
            "assignment pragma column names"
        );

        // Exercise all stock boolean spellings (assign and call forms).
        for setter in [
            "PRAGMA ignore_check_constraints = ON;",
            "PRAGMA ignore_check_constraints = OFF;",
            "PRAGMA ignore_check_constraints = 1;",
            "PRAGMA ignore_check_constraints = 0;",
            "PRAGMA ignore_check_constraints = TRUE;",
            "PRAGMA ignore_check_constraints = FALSE;",
            "PRAGMA ignore_check_constraints = YES;",
            "PRAGMA ignore_check_constraints = NO;",
            "PRAGMA ignore_check_constraints = 2;",
            "PRAGMA ignore_check_constraints = 'no';",
            "PRAGMA ignore_check_constraints = 'yes';",
            "PRAGMA ignore_check_constraints = 'bogus';",
            "PRAGMA ignore_check_constraints(1);",
            "PRAGMA ignore_check_constraints(0);",
            "PRAGMA ignore_check_constraints = ON;",
        ] {
            assert_outcome_matches(
                &format!("setter rows {setter}"),
                &native_rows(&native_a, setter).await,
                &stock_rows(&stock_a, setter),
            );
            assert_outcome_matches(
                &format!("readback after {setter}"),
                &native_rows(&native_a, bare).await,
                &stock_rows(&stock_a, bare),
            );
        }

        // Per-connection isolation: connection A is ON, connection B stays OFF.
        assert_outcome_matches(
            "conn B readback stays OFF",
            &native_rows(&native_b, bare).await,
            &stock_rows(&stock_b, bare),
        );
        assert_same_error_kind(
            "conn A accepts violation",
            &native_exec(&native_a, "INSERT INTO t VALUES(1, -10);").await,
            &stock_exec(&stock_a, "INSERT INTO t VALUES(1, -10);"),
        );
        assert_same_error_kind(
            "conn B still rejects violation",
            &native_exec(&native_b, "INSERT INTO t VALUES(2, -20);").await,
            &stock_exec(&stock_b, "INSERT INTO t VALUES(2, -20);"),
        );
        assert_outcome_matches(
            "rows visible after isolation test",
            &native_rows(&native_b, "SELECT id, x FROM t ORDER BY id;").await,
            &stock_rows(&stock_b, "SELECT id, x FROM t ORDER BY id;"),
        );

        native_b.close().await.expect("close b");
        native_a.close().await.expect("close a");
    });
}

async fn run_dml_and_integrity_matrix(file_backed: bool) {
    let dir = tempfile::tempdir().expect("tempdir");
    let stock_main_path = dir.path().join("stock_main.db");
    let stock_aux1_path = dir.path().join("stock_aux1.db");
    let stock_aux2_path = dir.path().join("stock_aux2.db");
    let native_main_path = dir.path().join("native_main.db");
    let native_aux1_path = dir.path().join("native_aux1.db");
    let native_aux2_path = dir.path().join("native_aux2.db");

    let stock = if file_backed {
        rusqlite::Connection::open(&stock_main_path).expect("stock open file")
    } else {
        rusqlite::Connection::open_in_memory().expect("stock open mem")
    };
    let native = if file_backed {
        Connection::open(native_main_path.to_str().expect("utf8"))
            .await
            .expect("native open file")
    } else {
        Connection::open(":memory:").await.expect("native open mem")
    };

    let attach_aux1_stock = if file_backed {
        format!("ATTACH DATABASE '{}' AS aux1;", stock_aux1_path.display())
    } else {
        "ATTACH DATABASE ':memory:' AS aux1;".to_owned()
    };
    let attach_aux1_native = if file_backed {
        format!("ATTACH DATABASE '{}' AS aux1;", native_aux1_path.display())
    } else {
        "ATTACH DATABASE ':memory:' AS aux1;".to_owned()
    };
    let attach_aux2_stock = if file_backed {
        format!("ATTACH DATABASE '{}' AS aux2;", stock_aux2_path.display())
    } else {
        "ATTACH DATABASE ':memory:' AS aux2;".to_owned()
    };
    let attach_aux2_native = if file_backed {
        format!("ATTACH DATABASE '{}' AS aux2;", native_aux2_path.display())
    } else {
        "ATTACH DATABASE ':memory:' AS aux2;".to_owned()
    };

    stock.execute_batch(&attach_aux1_stock).expect("stock attach aux1");
    native.execute(&attach_aux1_native).await.expect("native attach aux1");

    // Create tables across main, TEMP, and aux1 — including named CHECK, unnamed
    // CHECK, NOT NULL, UNIQUE, and WITHOUT ROWID.
    let ddls = [
        "CREATE TABLE main.t(\
            id INTEGER PRIMARY KEY, \
            x INTEGER NOT NULL CHECK(x > 0), \
            u TEXT UNIQUE, \
            CONSTRAINT ck_pair CHECK(x < 1000)\
        );",
        "CREATE TABLE main.wr(\
            k INTEGER PRIMARY KEY, \
            x INTEGER NOT NULL CHECK(x > 0)\
        ) WITHOUT ROWID;",
        "CREATE TEMP TABLE tt(\
            id INTEGER PRIMARY KEY, \
            x INTEGER NOT NULL CHECK(x > 0), \
            u TEXT UNIQUE\
        );",
        "CREATE TABLE aux1.at(\
            id INTEGER PRIMARY KEY, \
            x INTEGER NOT NULL CHECK(x > 0), \
            u TEXT UNIQUE\
        );",
    ];
    for ddl in ddls {
        stock.execute_batch(ddl).expect("stock ddl");
        native.execute(ddl).await.expect("native ddl");
    }

    // Seed one valid row in each table.
    let seeds = [
        "INSERT INTO main.t VALUES(1, 10, 'a');",
        "INSERT INTO main.wr VALUES(1, 10);",
        "INSERT INTO temp.tt VALUES(1, 10, 'a');",
        "INSERT INTO aux1.at VALUES(1, 10, 'a');",
    ];
    for sql in seeds {
        assert_same_error_kind(sql, &native_exec(&native, sql).await, &stock_exec(&stock, sql));
    }

    // While OFF: violating INSERT, UPDATE, UPSERT DO UPDATE, REPLACE fail with stock's exact text.
    let off_violations = [
        "INSERT INTO main.t VALUES(2, -1, 'b');",
        "INSERT INTO main.t VALUES(2, 2000, 'b');",
        "INSERT INTO main.wr VALUES(2, -1);",
        "UPDATE main.t SET x = -2 WHERE id = 1;",
        "INSERT INTO main.t VALUES(1, 5, 'x') ON CONFLICT(id) DO UPDATE SET x = -3;",
        "REPLACE INTO main.t VALUES(1, -4, 'a');",
        "INSERT INTO temp.tt VALUES(2, -1, 'b');",
        "UPDATE temp.tt SET x = -2 WHERE id = 1;",
        "INSERT INTO aux1.at VALUES(2, -1, 'b');",
        "UPDATE aux1.at SET x = -2 WHERE id = 1;",
    ];
    for sql in off_violations {
        let s = stock_exec(&stock, sql);
        let n = native_exec(&native, sql).await;
        assert!(s.is_err(), "stock must reject {sql} while OFF");
        assert_same_error_kind(&format!("OFF {sql}"), &n, &s);
    }

    // Turn ON (via schema-qualified pragma to verify global connection scope too!).
    let enable = "PRAGMA aux1.ignore_check_constraints = ON;";
    assert_outcome_matches(
        "enable via aux1",
        &native_rows(&native, enable).await,
        &stock_rows(&stock, enable),
    );
    assert_outcome_matches(
        "readback after aux1 enable",
        &native_rows(&native, "PRAGMA ignore_check_constraints;").await,
        &stock_rows(&stock, "PRAGMA ignore_check_constraints;"),
    );

    // Attach aux2 AFTER setting ON: newly attached DB must inherit ON!
    stock.execute_batch(&attach_aux2_stock).expect("stock attach aux2");
    native.execute(&attach_aux2_native).await.expect("native attach aux2");
    let aux2_ddl = "CREATE TABLE aux2.at2(id INTEGER PRIMARY KEY, x INTEGER NOT NULL CHECK(x > 0), u TEXT UNIQUE);";
    stock.execute_batch(aux2_ddl).expect("stock aux2 ddl");
    native.execute(aux2_ddl).await.expect("native aux2 ddl");

    // While ON: INSERT, UPDATE, UPSERT DO UPDATE, REPLACE, WITHOUT ROWID, TEMP, aux1, aux2 all succeed.
    let on_mutations = [
        // main.t: INSERT (unnamed + named check violation), UPDATE, UPSERT DO UPDATE, REPLACE
        "INSERT INTO main.t VALUES(2, -1, 'b');",
        "INSERT INTO main.t VALUES(3, 2000, 'c');",
        "UPDATE main.t SET x = -20 WHERE id = 1;",
        "INSERT INTO main.t VALUES(2, 5, 'z') ON CONFLICT(id) DO UPDATE SET x = -30;",
        "REPLACE INTO main.t VALUES(3, -40, 'c');",
        // main.wr: WITHOUT ROWID INSERT + UPDATE + UPSERT + REPLACE
        "INSERT INTO main.wr VALUES(2, -1);",
        "UPDATE main.wr SET x = -2 WHERE k = 1;",
        "INSERT INTO main.wr VALUES(2, 5) ON CONFLICT(k) DO UPDATE SET x = -3;",
        "REPLACE INTO main.wr VALUES(3, -4);",
        // temp.tt: INSERT + UPDATE + UPSERT + REPLACE
        "INSERT INTO temp.tt VALUES(2, -1, 'b');",
        "UPDATE temp.tt SET x = -2 WHERE id = 1;",
        "INSERT INTO temp.tt VALUES(2, 5, 'z') ON CONFLICT(id) DO UPDATE SET x = -3;",
        "REPLACE INTO temp.tt VALUES(3, -4, 'c');",
        // aux1.at: INSERT + UPDATE + UPSERT + REPLACE
        "INSERT INTO aux1.at VALUES(2, -1, 'b');",
        "UPDATE aux1.at SET x = -2 WHERE id = 1;",
        "INSERT INTO aux1.at VALUES(2, 5, 'z') ON CONFLICT(id) DO UPDATE SET x = -3;",
        "REPLACE INTO aux1.at VALUES(3, -4, 'c');",
        // aux2.at2 (attached after ON): INSERT + UPDATE
        "INSERT INTO aux2.at2 VALUES(1, -5, 'a');",
        "UPDATE aux2.at2 SET x = -6 WHERE id = 1;",
    ];
    for sql in on_mutations {
        let s = stock_exec(&stock, sql);
        let n = native_exec(&native, sql).await;
        assert!(s.is_ok(), "stock must accept {sql} while ON: {s:?}");
        assert_same_error_kind(&format!("ON {sql}"), &n, &s);
    }

    // Negatives while ON: NOT NULL and UNIQUE MUST still be enforced everywhere!
    let still_enforced = [
        "INSERT INTO main.t VALUES(10, NULL, 'nn1');",
        "UPDATE main.t SET x = NULL WHERE id = 1;",
        "INSERT INTO main.t VALUES(10, 5, 'a');",
        "UPDATE main.t SET u = 'b' WHERE id = 1;",
        "INSERT INTO main.wr VALUES(10, NULL);",
        "INSERT INTO temp.tt VALUES(10, NULL, 'nn1');",
        "INSERT INTO temp.tt VALUES(10, 5, 'a');",
        "INSERT INTO aux1.at VALUES(10, NULL, 'nn1');",
        "INSERT INTO aux1.at VALUES(10, 5, 'a');",
        "INSERT INTO aux2.at2 VALUES(10, NULL, 'nn1');",
        "INSERT INTO aux2.at2 VALUES(10, 5, 'a');",
    ];
    for sql in still_enforced {
        let s = stock_exec(&stock, sql);
        let n = native_exec(&native, sql).await;
        assert!(s.is_err(), "stock must still enforce NOT NULL/UNIQUE for {sql}");
        assert_same_error_kind(&format!("ON negative {sql}"), &n, &s);
    }

    // Read every table back and verify exact parity.
    for query in [
        "SELECT id, x, u FROM main.t ORDER BY id;",
        "SELECT k, x FROM main.wr ORDER BY k;",
        "SELECT id, x, u FROM temp.tt ORDER BY id;",
        "SELECT id, x, u FROM aux1.at ORDER BY id;",
        "SELECT id, x, u FROM aux2.at2 ORDER BY id;",
    ] {
        assert_outcome_matches(
            &format!("readback {query}"),
            &native_rows(&native, query).await,
            &stock_rows(&stock, query),
        );
    }

    // WildSnow case (a): while ignore_check_constraints is STILL ON,
    // PRAGMA integrity_check and quick_check suppress CHECK violations across
    // main, temp, aux1, aux2!
    for check_pragma in [
        "PRAGMA integrity_check;",
        "PRAGMA quick_check;",
        "PRAGMA main.integrity_check;",
        "PRAGMA temp.integrity_check;",
        "PRAGMA aux1.integrity_check;",
        "PRAGMA aux2.integrity_check;",
    ] {
        let s = stock_rows(&stock, check_pragma);
        let n = native_rows(&native, check_pragma).await;
        assert_eq!(s, Ok(vec![vec![Value::Text("ok".to_owned())]]), "stock {check_pragma} while ON");
        assert_outcome_matches(&format!("case(a) ON {check_pragma}"), &n, &s);
    }

    // WildSnow case (b): turn OFF, then run integrity_check and quick_check:
    // stock reports the CHECK violation rows, and fsqlite matches exactly!
    stock
        .execute_batch("PRAGMA ignore_check_constraints = OFF;")
        .expect("stock off");
    native
        .execute("PRAGMA ignore_check_constraints = OFF;")
        .await
        .expect("native off");

    for check_pragma in [
        "PRAGMA integrity_check;",
        "PRAGMA quick_check;",
        "PRAGMA main.integrity_check;",
        "PRAGMA temp.integrity_check;",
        "PRAGMA aux1.integrity_check;",
        "PRAGMA aux2.integrity_check;",
    ] {
        let mut s = stock_rows(&stock, check_pragma).expect("stock integrity_check");
        let mut n = native_rows(&native, check_pragma)
            .await
            .expect("native integrity_check");
        assert!(
            s.iter().any(|r| matches!(
                r.first(),
                Some(Value::Text(t)) if t.contains("CHECK constraint failed")
            )),
            "stock must report CHECK violations for {check_pragma} while OFF: {s:?}"
        );
        // Stock walks tables within a schema in `tblHash` order whereas fsqlite
        // walks creation order; compare the exact sorted multiset of reports.
        let sort_key = |row: &Vec<Value>| format!("{row:?}");
        s.sort_by_key(sort_key);
        n.sort_by_key(sort_key);
        assert_eq!(n, s, "case(b) OFF {check_pragma} report multiset mismatch");
    }

    // After turning OFF, new violating writes are refused again across all schemas.
    for sql in [
        "INSERT INTO main.t VALUES(99, -9, 'off1');",
        "INSERT INTO temp.tt VALUES(99, -9, 'off1');",
        "INSERT INTO aux1.at VALUES(99, -9, 'off1');",
        "INSERT INTO aux2.at2 VALUES(99, -9, 'off1');",
    ] {
        let s = stock_exec(&stock, sql);
        let n = native_exec(&native, sql).await;
        assert!(s.is_err(), "stock must reject {sql} after toggle OFF");
        assert_same_error_kind(&format!("after OFF {sql}"), &n, &s);
    }

    native.close().await.expect("native close");

    // If file-backed, open native's written files with stock SQLite and verify
    // stock sees the exact same rows and CHECK violations on disk.
    if file_backed {
        drop(stock);
        let stock_reopen_main = rusqlite::Connection::open(&native_main_path).expect("reopen main");
        assert_eq!(
            stock_rows(&stock_reopen_main, "SELECT id, x, u FROM t ORDER BY id;"),
            stock_rows(
                &rusqlite::Connection::open(&stock_main_path).expect("reopen stock main"),
                "SELECT id, x, u FROM t ORDER BY id;",
            ),
            "on-disk main.t rows match stock"
        );
        let main_ic = stock_rows(&stock_reopen_main, "PRAGMA integrity_check;").expect("ic");
        assert!(
            main_ic.iter().any(|r| matches!(r.first(), Some(Value::Text(t)) if t == "CHECK constraint failed in t")),
            "stock integrity_check on fsqlite file should detect CHECK violation: {main_ic:?}"
        );
    }
}

#[test]
fn dml_and_integrity_matrix_in_memory() {
    asupersync::test_utils::run_test(|| async {
        run_dml_and_integrity_matrix(false).await;
    });
}

#[test]
fn dml_and_integrity_matrix_file_backed() {
    asupersync::test_utils::run_test(|| async {
        run_dml_and_integrity_matrix(true).await;
    });
}

async fn run_prepared_statement_toggle_matrix(file_backed: bool) {
    let dir = tempfile::tempdir().expect("tempdir");
    let stock_path = dir.path().join("stock_prep.db");
    let stock_aux_path = dir.path().join("stock_prep_aux.db");
    let native_path = dir.path().join("native_prep.db");
    let native_aux_path = dir.path().join("native_prep_aux.db");

    let stock = if file_backed {
        rusqlite::Connection::open(&stock_path).expect("stock open")
    } else {
        rusqlite::Connection::open_in_memory().expect("stock open mem")
    };
    let native = if file_backed {
        Connection::open(native_path.to_str().expect("utf8"))
            .await
            .expect("native open")
    } else {
        Connection::open(":memory:").await.expect("native open mem")
    };

    let attach_stock = if file_backed {
        format!("ATTACH DATABASE '{}' AS aux;", stock_aux_path.display())
    } else {
        "ATTACH DATABASE ':memory:' AS aux;".to_owned()
    };
    let attach_native = if file_backed {
        format!("ATTACH DATABASE '{}' AS aux;", native_aux_path.display())
    } else {
        "ATTACH DATABASE ':memory:' AS aux;".to_owned()
    };

    let setup_ddls = [
        "CREATE TABLE main.t(id INTEGER PRIMARY KEY, x INTEGER NOT NULL CHECK(x > 0), u TEXT UNIQUE);",
        "CREATE TEMP TABLE tt(id INTEGER PRIMARY KEY, x INTEGER NOT NULL CHECK(x > 0), u TEXT UNIQUE);",
        "CREATE TABLE aux.at(id INTEGER PRIMARY KEY, x INTEGER NOT NULL CHECK(x > 0), u TEXT UNIQUE);",
        "INSERT INTO main.t VALUES(1, 10, 'k1');",
        "INSERT INTO temp.tt VALUES(1, 10, 'k1');",
        "INSERT INTO aux.at VALUES(1, 10, 'k1');",
    ];
    stock.execute_batch(&attach_stock).expect("stock attach");
    native.execute(&attach_native).await.expect("native attach");
    for ddl in setup_ddls {
        stock.execute_batch(ddl).expect("stock ddl");
        native.execute(ddl).await.expect("native ddl");
    }

    // 1) Prepare INSERT / UPDATE / UPSERT / REPLACE while OFF, then toggle ON,
    //    then toggle OFF — executing the SAME held statement handles across
    //    every phase (testing stock's OP_Expire re-prepare behavior).
    let insert_main_sql = "INSERT INTO main.t VALUES(?1, ?2, ?3);";
    let update_main_sql = "UPDATE main.t SET x = ?1 WHERE id = ?2;";
    let upsert_main_sql = "INSERT INTO main.t VALUES(?1, ?2, ?3) ON CONFLICT(id) DO UPDATE SET x = excluded.x;";
    let replace_main_sql = "REPLACE INTO main.t VALUES(?1, ?2, ?3);";
    let insert_temp_sql = "INSERT INTO temp.tt VALUES(?1, ?2, ?3);";
    let insert_aux_sql = "INSERT INTO aux.at VALUES(?1, ?2, ?3);";

    let mut s_ins_main = stock.prepare(insert_main_sql).expect("s prep ins main");
    let mut s_upd_main = stock.prepare(update_main_sql).expect("s prep upd main");
    let mut s_ups_main = stock.prepare(upsert_main_sql).expect("s prep ups main");
    let mut s_rep_main = stock.prepare(replace_main_sql).expect("s prep rep main");
    let mut s_ins_temp = stock.prepare(insert_temp_sql).expect("s prep ins temp");
    let mut s_ins_aux = stock.prepare(insert_aux_sql).expect("s prep ins aux");

    let n_ins_main = native.prepare(insert_main_sql).await.expect("n prep ins main");
    let n_upd_main = native.prepare(update_main_sql).await.expect("n prep upd main");
    let n_ups_main = native.prepare(upsert_main_sql).await.expect("n prep ups main");
    let n_rep_main = native.prepare(replace_main_sql).await.expect("n prep rep main");
    let n_ins_temp = native.prepare(insert_temp_sql).await.expect("n prep ins temp");
    let n_ins_aux = native.prepare(insert_aux_sql).await.expect("n prep ins aux");

    // Phase 1 (OFF): all violating executions must fail identically to stock.
    assert_same_error_kind(
        "held OFF ins main",
        &n_ins_main
            .execute_with_params(&[SqliteValue::Integer(2), SqliteValue::Integer(-1), SqliteValue::Text("k2".into())])
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
        &s_ins_main.execute(rusqlite::params![2, -1, "k2"]).map(|_| ()).map_err(|e| e.to_string()),
    );
    assert_same_error_kind(
        "held OFF upd main",
        &n_upd_main
            .execute_with_params(&[SqliteValue::Integer(-2), SqliteValue::Integer(1)])
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
        &s_upd_main.execute(rusqlite::params![-2, 1]).map(|_| ()).map_err(|e| e.to_string()),
    );
    assert_same_error_kind(
        "held OFF ins temp",
        &n_ins_temp
            .execute_with_params(&[SqliteValue::Integer(2), SqliteValue::Integer(-1), SqliteValue::Text("k2".into())])
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
        &s_ins_temp.execute(rusqlite::params![2, -1, "k2"]).map(|_| ()).map_err(|e| e.to_string()),
    );
    assert_same_error_kind(
        "held OFF ins aux",
        &n_ins_aux
            .execute_with_params(&[SqliteValue::Integer(2), SqliteValue::Integer(-1), SqliteValue::Text("k2".into())])
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
        &s_ins_aux.execute(rusqlite::params![2, -1, "k2"]).map(|_| ()).map_err(|e| e.to_string()),
    );

    // Toggle ON while holding all 6 prepared statements!
    stock
        .execute_batch("PRAGMA ignore_check_constraints = ON;")
        .expect("stock ON");
    native
        .execute("PRAGMA ignore_check_constraints = ON;")
        .await
        .expect("native ON");

    // Phase 2 (ON with SAME held statements): all violating executions must succeed!
    assert_same_error_kind(
        "held ON ins main",
        &n_ins_main
            .execute_with_params(&[SqliteValue::Integer(2), SqliteValue::Integer(-1), SqliteValue::Text("k2".into())])
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
        &s_ins_main.execute(rusqlite::params![2, -1, "k2"]).map(|_| ()).map_err(|e| e.to_string()),
    );
    assert_same_error_kind(
        "held ON upd main",
        &n_upd_main
            .execute_with_params(&[SqliteValue::Integer(-2), SqliteValue::Integer(1)])
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
        &s_upd_main.execute(rusqlite::params![-2, 1]).map(|_| ()).map_err(|e| e.to_string()),
    );
    assert_same_error_kind(
        "held ON ups main",
        &n_ups_main
            .execute_with_params(&[SqliteValue::Integer(2), SqliteValue::Integer(-3), SqliteValue::Text("k2".into())])
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
        &s_ups_main.execute(rusqlite::params![2, -3, "k2"]).map(|_| ()).map_err(|e| e.to_string()),
    );
    assert_same_error_kind(
        "held ON rep main",
        &n_rep_main
            .execute_with_params(&[SqliteValue::Integer(3), SqliteValue::Integer(-4), SqliteValue::Text("k3".into())])
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
        &s_rep_main.execute(rusqlite::params![3, -4, "k3"]).map(|_| ()).map_err(|e| e.to_string()),
    );
    assert_same_error_kind(
        "held ON ins temp",
        &n_ins_temp
            .execute_with_params(&[SqliteValue::Integer(2), SqliteValue::Integer(-1), SqliteValue::Text("k2".into())])
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
        &s_ins_temp.execute(rusqlite::params![2, -1, "k2"]).map(|_| ()).map_err(|e| e.to_string()),
    );
    assert_same_error_kind(
        "held ON ins aux",
        &n_ins_aux
            .execute_with_params(&[SqliteValue::Integer(2), SqliteValue::Integer(-1), SqliteValue::Text("k2".into())])
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
        &s_ins_aux.execute(rusqlite::params![2, -1, "k2"]).map(|_| ()).map_err(|e| e.to_string()),
    );

    // Negative: NOT NULL and UNIQUE still fail on held prepared statement while ON.
    assert_same_error_kind(
        "held ON NOT NULL main",
        &n_ins_main
            .execute_with_params(&[SqliteValue::Integer(50), SqliteValue::Null, SqliteValue::Text("nn".into())])
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
        &s_ins_main.execute(rusqlite::params![50, rusqlite::types::Null, "nn"]).map(|_| ()).map_err(|e| e.to_string()),
    );
    assert_same_error_kind(
        "held ON UNIQUE main",
        &n_ins_main
            .execute_with_params(&[SqliteValue::Integer(51), SqliteValue::Integer(-5), SqliteValue::Text("k1".into())])
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
        &s_ins_main.execute(rusqlite::params![51, -5, "k1"]).map(|_| ()).map_err(|e| e.to_string()),
    );

    // Also prepare a fresh statement WHILE ON, then toggle OFF and ensure the
    // held ON-prepared statement ALSO re-prepares and rejects!
    let mut s_ins_prepared_on = stock.prepare(insert_main_sql).expect("s prep on");
    let n_ins_prepared_on = native.prepare(insert_main_sql).await.expect("n prep on");

    stock
        .execute_batch("PRAGMA ignore_check_constraints = OFF;")
        .expect("stock OFF");
    native
        .execute("PRAGMA ignore_check_constraints = OFF;")
        .await
        .expect("native OFF");

    // Phase 3 (OFF with SAME held statements): violations must be refused again!
    assert_same_error_kind(
        "held back-to-OFF ins main",
        &n_ins_main
            .execute_with_params(&[SqliteValue::Integer(4), SqliteValue::Integer(-9), SqliteValue::Text("k4".into())])
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
        &s_ins_main.execute(rusqlite::params![4, -9, "k4"]).map(|_| ()).map_err(|e| e.to_string()),
    );
    assert_same_error_kind(
        "held ON->OFF ins main",
        &n_ins_prepared_on
            .execute_with_params(&[SqliteValue::Integer(5), SqliteValue::Integer(-9), SqliteValue::Text("k5".into())])
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
        &s_ins_prepared_on.execute(rusqlite::params![5, -9, "k5"]).map(|_| ()).map_err(|e| e.to_string()),
    );
    assert_same_error_kind(
        "held back-to-OFF ins temp",
        &n_ins_temp
            .execute_with_params(&[SqliteValue::Integer(4), SqliteValue::Integer(-9), SqliteValue::Text("k4".into())])
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
        &s_ins_temp.execute(rusqlite::params![4, -9, "k4"]).map(|_| ()).map_err(|e| e.to_string()),
    );
    assert_same_error_kind(
        "held back-to-OFF ins aux",
        &n_ins_aux
            .execute_with_params(&[SqliteValue::Integer(4), SqliteValue::Integer(-9), SqliteValue::Text("k4".into())])
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
        &s_ins_aux.execute(rusqlite::params![4, -9, "k4"]).map(|_| ()).map_err(|e| e.to_string()),
    );

    drop((s_ins_main, s_upd_main, s_ups_main, s_rep_main, s_ins_temp, s_ins_aux, s_ins_prepared_on));
    drop((n_ins_main, n_upd_main, n_ups_main, n_rep_main, n_ins_temp, n_ins_aux, n_ins_prepared_on));

    for query in [
        "SELECT id, x, u FROM main.t ORDER BY id;",
        "SELECT id, x, u FROM temp.tt ORDER BY id;",
        "SELECT id, x, u FROM aux.at ORDER BY id;",
    ] {
        assert_outcome_matches(
            &format!("prepared final rows {query}"),
            &native_rows(&native, query).await,
            &stock_rows(&stock, query),
        );
    }

    native.close().await.expect("native close");
}

#[test]
fn prepared_statement_toggle_matrix_in_memory() {
    asupersync::test_utils::run_test(|| async {
        run_prepared_statement_toggle_matrix(false).await;
    });
}

#[test]
fn prepared_statement_toggle_matrix_file_backed() {
    asupersync::test_utils::run_test(|| async {
        run_prepared_statement_toggle_matrix(true).await;
    });
}

#[test]
fn alter_table_add_column_check_and_explicit_txn_toggle_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let stock = rusqlite::Connection::open_in_memory().expect("stock");
        let native = Connection::open(":memory:").await.expect("native");

        let setup = [
            "CREATE TABLE a(id INTEGER PRIMARY KEY, x INTEGER CHECK(x > 0));",
            "INSERT INTO a VALUES(1, 5);",
        ];
        for sql in setup {
            stock.execute_batch(sql).expect("stock setup");
            native.execute(sql).await.expect("native setup");
        }

        // Toggle inside an explicit transaction and verify held prepared DML honors it.
        let mut s_stmt = stock.prepare("INSERT INTO a VALUES(?1, ?2);").expect("s prep");
        let n_stmt = native.prepare("INSERT INTO a VALUES(?1, ?2);").await.expect("n prep");

        stock.execute_batch("BEGIN;").expect("s begin");
        native.execute("BEGIN;").await.expect("n begin");
        stock
            .execute_batch("PRAGMA ignore_check_constraints = ON;")
            .expect("s on");
        native
            .execute("PRAGMA ignore_check_constraints = ON;")
            .await
            .expect("n on");
        assert_same_error_kind(
            "explicit txn ON prepared insert",
            &n_stmt
                .execute_with_params(&[SqliteValue::Integer(2), SqliteValue::Integer(-11)])
                .await
                .map(|_| ())
                .map_err(|e| e.to_string()),
            &s_stmt
                .execute(rusqlite::params![2, -11])
                .map(|_| ())
                .map_err(|e| e.to_string()),
        );
        stock
            .execute_batch("PRAGMA ignore_check_constraints = OFF;")
            .expect("s off");
        native
            .execute("PRAGMA ignore_check_constraints = OFF;")
            .await
            .expect("n off");
        assert_same_error_kind(
            "explicit txn back-to-OFF prepared insert",
            &n_stmt
                .execute_with_params(&[SqliteValue::Integer(3), SqliteValue::Integer(-12)])
                .await
                .map(|_| ())
                .map_err(|e| e.to_string()),
            &s_stmt
                .execute(rusqlite::params![3, -12])
                .map(|_| ())
                .map_err(|e| e.to_string()),
        );
        drop(s_stmt);
        drop(n_stmt);
        stock.execute_batch("COMMIT;").expect("s commit");
        native.execute("COMMIT;").await.expect("n commit");

        // ALTER TABLE ADD COLUMN ... CHECK(...) fails while OFF, succeeds while ON.
        let alter_sql = "ALTER TABLE a ADD COLUMN y INTEGER DEFAULT -1 CHECK(y > 0);";
        assert_same_error_kind(
            "ALTER ADD CHECK while OFF",
            &native_exec(&native, alter_sql).await,
            &stock_exec(&stock, alter_sql),
        );
        stock
            .execute_batch("PRAGMA ignore_check_constraints = ON;")
            .expect("s on");
        native
            .execute("PRAGMA ignore_check_constraints = ON;")
            .await
            .expect("n on");
        assert_same_error_kind(
            "ALTER ADD CHECK while ON",
            &native_exec(&native, alter_sql).await,
            &stock_exec(&stock, alter_sql),
        );
        assert_outcome_matches(
            "rows after ALTER while ON",
            &native_rows(&native, "SELECT id, x, y FROM a ORDER BY id;").await,
            &stock_rows(&stock, "SELECT id, x, y FROM a ORDER BY id;"),
        );

        native.close().await.expect("close");
    });
}
