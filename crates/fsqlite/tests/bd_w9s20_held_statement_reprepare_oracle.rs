#![recursion_limit = "512"]

//! bd-w9s20: held prepared statements (`fsqlite::blocking::Statement` and
//! `fsqlite::PreparedStatement`) must re-prepare transparently on schema
//! changes (cross-connection and same-connection `ALTER TABLE ... ADD COLUMN`
//! / `RENAME COLUMN` / `DROP TABLE`) and on `PRAGMA reverse_unordered_selects`,
//! updating column metadata (`column_count`, `column_names`, `column_name`,
//! `column_index`, `Row::get` by name) and scan order to match bundled
//! SQLite 3.54.1 (`rusqlite`), while reusing the compiled program with zero
//! recompiles when neither schema nor pragma changed.

use fsqlite::Connection as AsyncConnection;
use fsqlite::blocking::Connection as BlockingConnection;
use fsqlite_core::connection::{
    hot_path_profile_snapshot, reset_hot_path_profile, set_hot_path_profile_enabled,
};
use fsqlite_types::SqliteValue;
use rusqlite::types::Value as StockValue;

fn stock_to_sqlite(v: StockValue) -> SqliteValue {
    match v {
        StockValue::Null => SqliteValue::Null,
        StockValue::Integer(n) => SqliteValue::Integer(n),
        StockValue::Real(f) => SqliteValue::Float(f),
        StockValue::Text(s) => SqliteValue::from(s.as_str()),
        StockValue::Blob(b) => SqliteValue::Blob(b.into()),
    }
}

fn blocking_val_to_sqlite(v: fsqlite::blocking::types::Value) -> SqliteValue {
    match v {
        fsqlite::blocking::types::Value::Null => SqliteValue::Null,
        fsqlite::blocking::types::Value::Integer(n) => SqliteValue::Integer(n),
        fsqlite::blocking::types::Value::Real(f) => SqliteValue::Float(f),
        fsqlite::blocking::types::Value::Text(s) => SqliteValue::from(s.as_str()),
        fsqlite::blocking::types::Value::Blob(b) => SqliteValue::Blob(b.into()),
    }
}

type StepOutcome = (usize, Vec<String>, Vec<Vec<SqliteValue>>);

fn step_stock(stmt: &mut rusqlite::Statement<'_>) -> Result<StepOutcome, String> {
    let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        let width = row.as_ref().column_count();
        let vals = (0..width)
            .map(|i| row.get::<_, StockValue>(i).map(stock_to_sqlite))
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|e| e.to_string())?;
        out.push(vals);
    }
    drop(rows);
    let col_count = stmt.column_count();
    let col_names = stmt.column_names().map(str::to_owned).collect::<Vec<_>>();
    Ok((col_count, col_names, out))
}

fn step_blocking(stmt: &mut fsqlite::blocking::Statement<'_>) -> Result<StepOutcome, String> {
    let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        let width = row.as_ref().column_count();
        let vals = (0..width)
            .map(|i| {
                row.get::<_, fsqlite::blocking::types::Value>(i)
                    .map(blocking_val_to_sqlite)
            })
            .collect::<fsqlite::blocking::Result<Vec<_>>>()
            .map_err(|e| e.to_string())?;
        out.push(vals);
    }
    drop(rows);
    let col_count = stmt.column_count();
    let col_names = stmt.column_names().map(str::to_owned).collect::<Vec<_>>();
    Ok((col_count, col_names, out))
}

async fn step_async(stmt: &fsqlite::PreparedStatement<'_>) -> Result<StepOutcome, String> {
    let rows = stmt.query().await.map_err(|e| e.to_string())?;
    let col_count = stmt.column_count();
    let col_names = stmt.column_names();
    let out = rows.iter().map(|r| r.values().to_vec()).collect();
    Ok((col_count, col_names, out))
}

#[test]
fn blocking_held_select_star_reprepares_after_cross_and_same_conn_alter() {
    let dir = tempfile::tempdir().expect("tempdir");
    let frank_path = dir.path().join("frank_blocking.db");
    let stock_path = dir.path().join("stock_blocking.db");

    let frank_a = BlockingConnection::open(&frank_path).expect("frank a");
    let stock_a = rusqlite::Connection::open(&stock_path).expect("stock a");
    stock_a
        .execute_batch("PRAGMA journal_mode=WAL;")
        .expect("stock wal");

    let setup = "\
        CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT);
        INSERT INTO t VALUES (1, 'x'), (2, 'y');
        CREATE TABLE empty_t(a INTEGER PRIMARY KEY, b TEXT);";
    frank_a.execute_batch(setup).expect("frank setup");
    stock_a.execute_batch(setup).expect("stock setup");

    let frank_b = BlockingConnection::open(&frank_path).expect("frank b");
    let stock_b = rusqlite::Connection::open(&stock_path).expect("stock b");

    let mut f_stmt = frank_a
        .prepare("SELECT * FROM t ORDER BY a")
        .expect("frank prep t");
    let mut s_stmt = stock_a
        .prepare("SELECT * FROM t ORDER BY a")
        .expect("stock prep t");
    let mut f_empty = frank_a
        .prepare("SELECT * FROM empty_t ORDER BY a")
        .expect("frank prep empty_t");
    let mut s_empty = stock_a
        .prepare("SELECT * FROM empty_t ORDER BY a")
        .expect("stock prep empty_t");

    // Prepare-time metadata matches stock before any ALTER.
    assert_eq!(f_stmt.column_count(), s_stmt.column_count());
    assert_eq!(
        f_stmt.column_names().collect::<Vec<_>>(),
        s_stmt.column_names().collect::<Vec<_>>()
    );
    assert_eq!(step_blocking(&mut f_stmt), step_stock(&mut s_stmt));
    assert_eq!(step_blocking(&mut f_empty), step_stock(&mut s_empty));

    // Cross-connection ALTER TABLE ADD COLUMN on B while A holds both statements.
    let cross_ddl = "\
        ALTER TABLE t ADD COLUMN c INTEGER DEFAULT 7;
        ALTER TABLE empty_t ADD COLUMN c INTEGER DEFAULT 7;";
    frank_b.execute_batch(cross_ddl).expect("frank cross ddl");
    stock_b.execute_batch(cross_ddl).expect("stock cross ddl");

    // Step A's held statements: column count, names, and row values must match stock.
    let f_after = step_blocking(&mut f_stmt).expect("frank step after cross alter");
    let s_after = step_stock(&mut s_stmt).expect("stock step after cross alter");
    assert_eq!(f_after, s_after, "populated table cross-conn ADD COLUMN");
    assert_eq!(f_after.0, 3);
    assert_eq!(f_after.1, ["a", "b", "c"]);
    assert_eq!(f_stmt.column_name(2).expect("col 2 name"), "c");
    assert_eq!(f_stmt.column_index("c").expect("col c index"), 2);

    // Lookup by newly added column name via Row::get must also work.
    {
        let mut rows = f_stmt.query([]).expect("requery");
        let first = rows.next().expect("next").expect("first row");
        assert_eq!(first.as_ref().column_count(), 3);
        assert_eq!(first.get::<_, i64>("c").expect("get c by name"), 7);
    }

    // Empty table widens column_count and column_names too, matching stock.
    let f_empty_after = step_blocking(&mut f_empty).expect("frank empty after cross alter");
    let s_empty_after = step_stock(&mut s_empty).expect("stock empty after cross alter");
    assert_eq!(
        f_empty_after, s_empty_after,
        "empty table cross-conn ADD COLUMN"
    );
    assert_eq!(f_empty_after.0, 3);
    assert_eq!(f_empty_after.1, ["a", "b", "c"]);

    // Same-connection ALTER TABLE ADD COLUMN and RENAME COLUMN on A itself.
    let same_ddl = "\
        ALTER TABLE t ADD COLUMN d TEXT DEFAULT 'nine';
        ALTER TABLE t RENAME COLUMN b TO bb;";
    frank_a.execute_batch(same_ddl).expect("frank same ddl");
    stock_a.execute_batch(same_ddl).expect("stock same ddl");

    let f_same = step_blocking(&mut f_stmt).expect("frank step after same-conn alter");
    let s_same = step_stock(&mut s_stmt).expect("stock step after same-conn alter");
    assert_eq!(f_same, s_same, "same-conn ADD + RENAME COLUMN");
    assert_eq!(f_same.0, 4);
    assert_eq!(f_same.1, ["a", "bb", "c", "d"]);

    // In-memory same-connection variant.
    let fm = BlockingConnection::open_in_memory().expect("fm");
    let sm = rusqlite::Connection::open_in_memory().expect("sm");
    fm.execute_batch("CREATE TABLE m(a, b); INSERT INTO m VALUES (1, 2);")
        .expect("fm setup");
    sm.execute_batch("CREATE TABLE m(a, b); INSERT INTO m VALUES (1, 2);")
        .expect("sm setup");
    let mut fm_stmt = fm.prepare("SELECT * FROM m").expect("fm prep");
    let mut sm_stmt = sm.prepare("SELECT * FROM m").expect("sm prep");
    assert_eq!(step_blocking(&mut fm_stmt), step_stock(&mut sm_stmt));
    fm.execute_batch("ALTER TABLE m ADD COLUMN c DEFAULT 7;")
        .expect("fm alter");
    sm.execute_batch("ALTER TABLE m ADD COLUMN c DEFAULT 7;")
        .expect("sm alter");
    assert_eq!(
        step_blocking(&mut fm_stmt),
        step_stock(&mut sm_stmt),
        ":memory: same-conn ADD COLUMN"
    );
    assert_eq!(fm_stmt.column_count(), 3);
    assert_eq!(fm_stmt.column_names().collect::<Vec<_>>(), ["a", "b", "c"]);

    // Dropping the table makes re-prepare surface stock's error.
    frank_b.execute_batch("DROP TABLE t;").expect("frank drop");
    stock_b.execute_batch("DROP TABLE t;").expect("stock drop");
    let f_err = step_blocking(&mut f_stmt).expect_err("frank drop error");
    let s_err = step_stock(&mut s_stmt).expect_err("stock drop error");
    assert!(
        f_err.contains("no such table") && s_err.contains("no such table"),
        "expected no such table: frank={f_err:?}, stock={s_err:?}"
    );
}

#[test]
fn async_and_blocking_held_statements_reprepare_on_reverse_unordered_selects() {
    let dir = tempfile::tempdir().expect("tempdir");
    let frank_path = dir.path().join("frank_rev.db");
    let stock_path = dir.path().join("stock_rev.db");

    // 1. Async PreparedStatement: cross-conn ALTER + PRAGMA reverse_unordered_selects.
    asupersync::test_utils::run_test(|| async {
        let f_path = frank_path.to_str().expect("utf8").to_owned();
        let fa = AsyncConnection::open(&f_path).await.expect("fa");
        let sa = rusqlite::Connection::open(&stock_path).expect("sa");
        sa.execute_batch("PRAGMA journal_mode=WAL;")
            .expect("sa wal");

        let setup = "\
            CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT);
            INSERT INTO t VALUES (1, 'one'), (2, 'two'), (3, 'three');";
        fa.execute_batch(setup).await.expect("fa setup");
        sa.execute_batch(setup).expect("sa setup");

        let fb = AsyncConnection::open(&f_path).await.expect("fb");
        let sb = rusqlite::Connection::open(&stock_path).expect("sb");

        // Hold both `SELECT a FROM t` and `SELECT * FROM t` (which can hit
        // SimpleFullTableScan) plus an explicitly ordered control.
        let f_col = fa.prepare("SELECT a FROM t").await.expect("f_col");
        let mut s_col = sa.prepare("SELECT a FROM t").expect("s_col");
        let f_star = fa.prepare("SELECT * FROM t").await.expect("f_star");
        let mut s_star = sa.prepare("SELECT * FROM t").expect("s_star");
        let f_ord = fa
            .prepare("SELECT a FROM t ORDER BY a")
            .await
            .expect("f_ord");
        let mut s_ord = sa.prepare("SELECT a FROM t ORDER BY a").expect("s_ord");

        // Before pragma: forward order matches stock.
        assert_eq!(step_async(&f_col).await, step_stock(&mut s_col));
        assert_eq!(step_async(&f_star).await, step_stock(&mut s_star));
        assert_eq!(step_async(&f_ord).await, step_stock(&mut s_ord));

        // Toggle PRAGMA reverse_unordered_selects = ON on A while statements are held.
        fa.execute("PRAGMA reverse_unordered_selects = ON")
            .await
            .expect("fa pragma on");
        sa.execute_batch("PRAGMA reverse_unordered_selects = ON;")
            .expect("sa pragma on");

        let f_col_on = step_async(&f_col).await.expect("f_col on");
        let s_col_on = step_stock(&mut s_col).expect("s_col on");
        assert_eq!(
            f_col_on, s_col_on,
            "held SELECT a FROM t must reverse under PRAGMA reverse_unordered_selects = ON"
        );
        assert_eq!(
            f_col_on.2,
            vec![
                vec![SqliteValue::Integer(3)],
                vec![SqliteValue::Integer(2)],
                vec![SqliteValue::Integer(1)],
            ]
        );

        let f_star_on = step_async(&f_star).await.expect("f_star on");
        let s_star_on = step_stock(&mut s_star).expect("s_star on");
        assert_eq!(
            f_star_on, s_star_on,
            "held SELECT * FROM t must reverse under PRAGMA reverse_unordered_selects = ON"
        );

        // Explicit ORDER BY remains ascending.
        assert_eq!(step_async(&f_ord).await, step_stock(&mut s_ord));

        // Cross-connection ALTER TABLE while reverse_unordered_selects is ON.
        fb.execute("ALTER TABLE t ADD COLUMN c INTEGER DEFAULT 7")
            .await
            .expect("fb alter");
        sb.execute_batch("ALTER TABLE t ADD COLUMN c INTEGER DEFAULT 7;")
            .expect("sb alter");
        let f_star_wide_rev = step_async(&f_star).await.expect("f_star wide rev");
        let s_star_wide_rev = step_stock(&mut s_star).expect("s_star wide rev");
        assert_eq!(
            f_star_wide_rev, s_star_wide_rev,
            "held SELECT * widens and stays reversed"
        );
        assert_eq!(f_star_wide_rev.0, 3);
        assert_eq!(f_star_wide_rev.1, ["a", "b", "c"]);

        // Toggle PRAGMA reverse_unordered_selects = OFF: held statements restore forward order.
        fa.execute("PRAGMA reverse_unordered_selects = OFF")
            .await
            .expect("fa pragma off");
        sa.execute_batch("PRAGMA reverse_unordered_selects = OFF;")
            .expect("sa pragma off");
        assert_eq!(
            step_async(&f_col).await,
            step_stock(&mut s_col),
            "held SELECT a FROM t returns to forward order after OFF"
        );
        assert_eq!(
            step_async(&f_star).await,
            step_stock(&mut s_star),
            "held SELECT * FROM t returns to forward order after OFF"
        );
    });

    // 2. Blocking Statement over :memory: across PRAGMA reverse_unordered_selects.
    let fm = BlockingConnection::open_in_memory().expect("fm");
    let sm = rusqlite::Connection::open_in_memory().expect("sm");
    let setup = "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT); INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c');";
    fm.execute_batch(setup).expect("fm setup");
    sm.execute_batch(setup).expect("sm setup");

    let mut fm_star = fm.prepare("SELECT * FROM t").expect("fm_star");
    let mut sm_star = sm.prepare("SELECT * FROM t").expect("sm_star");
    let mut fm_col = fm.prepare("SELECT a FROM t").expect("fm_col");
    let mut sm_col = sm.prepare("SELECT a FROM t").expect("sm_col");

    assert_eq!(step_blocking(&mut fm_star), step_stock(&mut sm_star));
    assert_eq!(step_blocking(&mut fm_col), step_stock(&mut sm_col));

    fm.execute_batch("PRAGMA reverse_unordered_selects = ON;")
        .expect("fm on");
    sm.execute_batch("PRAGMA reverse_unordered_selects = ON;")
        .expect("sm on");
    assert_eq!(
        step_blocking(&mut fm_star),
        step_stock(&mut sm_star),
        "blocking held SELECT * reverses on PRAGMA ON"
    );
    assert_eq!(
        step_blocking(&mut fm_col),
        step_stock(&mut sm_col),
        "blocking held SELECT a reverses on PRAGMA ON"
    );

    fm.execute_batch("PRAGMA reverse_unordered_selects = OFF;")
        .expect("fm off");
    sm.execute_batch("PRAGMA reverse_unordered_selects = OFF;")
        .expect("sm off");
    assert_eq!(
        step_blocking(&mut fm_star),
        step_stock(&mut sm_star),
        "blocking held SELECT * restores forward on PRAGMA OFF"
    );
    assert_eq!(
        step_blocking(&mut fm_col),
        step_stock(&mut sm_col),
        "blocking held SELECT a restores forward on PRAGMA OFF"
    );
}

#[test]
fn held_statement_reuses_program_when_schema_and_pragma_unchanged() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("reuse.db");
        let path_str = path.to_str().expect("utf8").to_owned();

        for (label, conn_path) in [("memory", ":memory:"), ("file", path_str.as_str())] {
            let conn = AsyncConnection::open(conn_path).await.expect("open");
            conn.execute_batch("CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT); INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c');")
                .await
                .expect("setup");

            let stmt = conn.prepare("SELECT * FROM t").await.expect("prepare");
            let initial_program_id = stmt.program_identity();
            assert_eq!(stmt.reprepare_count(), 0, "{label}");
            let initial_rows = stmt.query().await.expect("initial query");
            assert_eq!(initial_rows.len(), 3, "{label}");
            assert_eq!(stmt.reprepare_count(), 0, "{label}");
            assert_eq!(stmt.program_identity(), initial_program_id, "{label}");

            // Negative case: no schema or pragma change -> 0 re-prepares across 5 steps.
            set_hot_path_profile_enabled(true);
            reset_hot_path_profile();
            for _ in 0..5 {
                let rows = stmt.query().await.expect("steady query");
                assert_eq!(rows.len(), 3, "{label}");
                assert_eq!(rows[0].values().len(), 2, "{label}");
                assert_eq!(stmt.reprepare_count(), 0, "{label}");
                assert_eq!(stmt.program_identity(), initial_program_id, "{label}");
            }
            let steady = hot_path_profile_snapshot().parser;
            set_hot_path_profile_enabled(false);
            assert_eq!(
                steady.prepared_cache_hits + steady.prepared_cache_misses,
                0,
                "{label}: held statement with no schema/pragma change must not recompile: {steady:?}"
            );

            // One PRAGMA toggle -> exactly 1 re-prepare across 5 steps, then reuse.
            conn.execute("PRAGMA reverse_unordered_selects = ON")
                .await
                .expect("pragma on");
            set_hot_path_profile_enabled(true);
            reset_hot_path_profile();
            let mut reversed_program_id = None;
            for _ in 0..5 {
                let rows = stmt.query().await.expect("reversed query");
                assert_eq!(rows[0].values()[0], SqliteValue::Integer(3), "{label}");
                assert_eq!(stmt.reprepare_count(), 1, "{label}");
                let pid = stmt.program_identity();
                assert_ne!(pid, initial_program_id, "{label}");
                if let Some(prev) = reversed_program_id {
                    assert_eq!(pid, prev, "{label}: reversed program must be reused");
                } else {
                    reversed_program_id = Some(pid);
                }
            }
            let after_pragma = hot_path_profile_snapshot().parser;
            set_hot_path_profile_enabled(false);
            assert_eq!(
                after_pragma.prepared_cache_hits + after_pragma.prepared_cache_misses,
                1,
                "{label}: 5 steps after PRAGMA reverse_unordered_selects must recompile once: {after_pragma:?}"
            );
        }
    });
}
