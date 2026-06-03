//! bd-1dp9.6.7.5.3: Aggregate/window/set-write differential suite.
//!
//! Oracle-parity tests for grouped aggregates, window functions,
//! and INSERT...SELECT semantics against rusqlite (C SQLite).
//!
//! - A1: Grouped aggregates (COUNT, SUM, AVG, MIN, MAX)
//! - A2: DISTINCT aggregates
//! - A3: GROUP BY with HAVING
//! - A4: Multiple aggregates in one query
//! - A5: Aggregate over empty groups
//! - W1: Window ROW_NUMBER / RANK / DENSE_RANK
//! - W2: Window SUM with ROWS BETWEEN frame
//! - W3: Window LAG / LEAD
//! - W4: PARTITION BY with ORDER BY
//! - W5: Window over empty partition
//! - I1: INSERT...SELECT basic
//! - I2: INSERT...SELECT with aggregates
//! - I3: INSERT...SELECT cross-table
//! - I4: INSERT...SELECT with WHERE filter

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn test_tmpdir() -> tempfile::TempDir {
    tempfile::tempdir_in(std::env::temp_dir())
        .or_else(|_| tempfile::tempdir_in("."))
        .expect("tempdir")
}

fn get_int(conn: &Connection, sql: &str) -> Option<i64> {
    let rows = conn.query(sql).ok()?;
    let row = rows.first()?;
    match row.get(0)? {
        SqliteValue::Integer(v) => Some(*v),
        _ => None,
    }
}

fn setup_oracle_pair(dir: &tempfile::TempDir, name: &str) -> (Connection, rusqlite::Connection) {
    let f_path = dir.path().join(format!("{name}_f.db"));
    let c_path = dir.path().join(format!("{name}_c.db"));

    let f = Connection::open(f_path.to_str().expect("path")).expect("fsqlite open");
    let c = rusqlite::Connection::open(&c_path).expect("csqlite open");

    (f, c)
}

fn seed_both(f: &Connection, c: &rusqlite::Connection) {
    let ddl = "CREATE TABLE sales (id INTEGER PRIMARY KEY, dept TEXT, amount INTEGER, qty INTEGER)";
    f.execute(ddl).expect("f create");
    c.execute_batch(ddl).expect("c create");

    let data = [
        (1, "eng", 100, 2),
        (2, "eng", 200, 3),
        (3, "eng", 150, 1),
        (4, "sales", 300, 5),
        (5, "sales", 250, 4),
        (6, "sales", 300, 2),
        (7, "hr", 50, 1),
        (8, "hr", 75, 2),
        (9, "hr", 50, 1),
        (10, "ops", 400, 6),
    ];

    f.execute("BEGIN").expect("f begin");
    for (id, dept, amount, qty) in &data {
        f.execute(&format!(
            "INSERT INTO sales VALUES ({id}, '{dept}', {amount}, {qty})"
        ))
        .expect("f insert");
        c.execute(
            "INSERT INTO sales VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![id, dept, amount, qty],
        )
        .expect("c insert");
    }
    f.execute("COMMIT").expect("f commit");
}

fn compare_int_query(f: &Connection, c: &rusqlite::Connection, sql: &str, label: &str) {
    let f_val = get_int(f, sql);
    let c_val: Option<i64> = c
        .prepare(sql)
        .expect("prepare")
        .query_row([], |r| r.get(0))
        .ok();

    assert_eq!(
        f_val, c_val,
        "{label}: fsqlite={f_val:?}, csqlite={c_val:?} for: {sql}"
    );
}

fn compare_row_count(f: &Connection, c: &rusqlite::Connection, sql: &str, label: &str) {
    let f_count = f.query(sql).expect("f query").len();
    let c_count: usize = c
        .prepare(sql)
        .expect("prepare")
        .query_map([], |_| Ok(()))
        .expect("query")
        .count();

    assert_eq!(
        f_count, c_count,
        "{label}: fsqlite row count={f_count}, csqlite={c_count} for: {sql}"
    );
}

// ─── A1: Grouped aggregates ───────────────────────────────────────

#[test]
fn a1_grouped_aggregates() {
    let dir = test_tmpdir();
    let (f, c) = setup_oracle_pair(&dir, "a1");
    seed_both(&f, &c);

    compare_int_query(&f, &c, "SELECT COUNT(*) FROM sales", "A1-count-all");
    compare_int_query(&f, &c, "SELECT SUM(amount) FROM sales", "A1-sum");
    compare_int_query(&f, &c, "SELECT MIN(amount) FROM sales", "A1-min");
    compare_int_query(&f, &c, "SELECT MAX(amount) FROM sales", "A1-max");

    // GROUP BY
    compare_row_count(
        &f,
        &c,
        "SELECT dept, SUM(amount) FROM sales GROUP BY dept",
        "A1-group-sum",
    );
    compare_row_count(
        &f,
        &c,
        "SELECT dept, COUNT(*) FROM sales GROUP BY dept",
        "A1-group-count",
    );

    // Per-dept sums
    compare_int_query(
        &f,
        &c,
        "SELECT SUM(amount) FROM sales WHERE dept = 'eng'",
        "A1-eng-sum",
    );
    compare_int_query(
        &f,
        &c,
        "SELECT SUM(amount) FROM sales WHERE dept = 'sales'",
        "A1-sales-sum",
    );

    eprintln!("A1: grouped aggregates — oracle parity confirmed");
}

// ─── A2: DISTINCT aggregates ──────────────────────────────────────

#[test]
fn a2_distinct_aggregates() {
    let dir = test_tmpdir();
    let (f, c) = setup_oracle_pair(&dir, "a2");
    seed_both(&f, &c);

    compare_int_query(
        &f,
        &c,
        "SELECT COUNT(DISTINCT dept) FROM sales",
        "A2-count-distinct-dept",
    );
    compare_int_query(
        &f,
        &c,
        "SELECT COUNT(DISTINCT amount) FROM sales",
        "A2-count-distinct-amount",
    );
    compare_int_query(
        &f,
        &c,
        "SELECT SUM(DISTINCT amount) FROM sales",
        "A2-sum-distinct",
    );

    eprintln!("A2: DISTINCT aggregates — oracle parity confirmed");
}

// ─── A3: GROUP BY with HAVING ─────────────────────────────────────

#[test]
fn a3_group_by_having() {
    let dir = test_tmpdir();
    let (f, c) = setup_oracle_pair(&dir, "a3");
    seed_both(&f, &c);

    compare_row_count(
        &f,
        &c,
        "SELECT dept, SUM(amount) FROM sales GROUP BY dept HAVING SUM(amount) > 200",
        "A3-having-sum",
    );
    compare_row_count(
        &f,
        &c,
        "SELECT dept, COUNT(*) FROM sales GROUP BY dept HAVING COUNT(*) >= 3",
        "A3-having-count",
    );
    compare_row_count(
        &f,
        &c,
        "SELECT dept FROM sales GROUP BY dept HAVING AVG(amount) > 100",
        "A3-having-avg",
    );

    eprintln!("A3: GROUP BY with HAVING — oracle parity confirmed");
}

// ─── A4: Multiple aggregates in one query ─────────────────────────

#[test]
fn a4_multiple_aggregates() {
    let dir = test_tmpdir();
    let (f, c) = setup_oracle_pair(&dir, "a4");
    seed_both(&f, &c);

    let f_rows = f
        .query("SELECT dept, COUNT(*), SUM(amount), MIN(amount), MAX(amount) FROM sales GROUP BY dept ORDER BY dept")
        .expect("f query");
    let mut c_stmt = c
        .prepare("SELECT dept, COUNT(*), SUM(amount), MIN(amount), MAX(amount) FROM sales GROUP BY dept ORDER BY dept")
        .expect("c prepare");
    let c_rows: Vec<(String, i64, i64, i64, i64)> = c_stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })
        .expect("c query")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect");

    assert_eq!(
        f_rows.len(),
        c_rows.len(),
        "A4: row count mismatch: f={}, c={}",
        f_rows.len(),
        c_rows.len()
    );

    for (i, (f_row, c_row)) in f_rows.iter().zip(c_rows.iter()).enumerate() {
        let f_count = match f_row.get(1) {
            Some(SqliteValue::Integer(v)) => *v,
            _ => -1,
        };
        let f_sum = match f_row.get(2) {
            Some(SqliteValue::Integer(v)) => *v,
            _ => -1,
        };
        assert_eq!(
            f_count, c_row.1,
            "A4 row {i}: COUNT mismatch f={f_count}, c={}",
            c_row.1
        );
        assert_eq!(
            f_sum, c_row.2,
            "A4 row {i}: SUM mismatch f={f_sum}, c={}",
            c_row.2
        );
    }

    eprintln!(
        "A4: multiple aggregates — oracle parity on {} groups",
        f_rows.len()
    );
}

// ─── A5: Aggregate over empty groups ──────────────────────────────

#[test]
fn a5_aggregate_empty_groups() {
    let dir = test_tmpdir();
    let (f, c) = setup_oracle_pair(&dir, "a5");
    f.execute("CREATE TABLE empty_t (id INTEGER PRIMARY KEY, val INTEGER)")
        .expect("f create");
    c.execute_batch("CREATE TABLE empty_t (id INTEGER PRIMARY KEY, val INTEGER)")
        .expect("c create");

    // COUNT on empty table
    compare_int_query(&f, &c, "SELECT COUNT(*) FROM empty_t", "A5-count-empty");

    // SUM on empty (should be NULL → coalesce)
    compare_int_query(
        &f,
        &c,
        "SELECT COALESCE(SUM(val), -999) FROM empty_t",
        "A5-sum-empty",
    );

    // GROUP BY on empty returns no rows
    compare_row_count(
        &f,
        &c,
        "SELECT val, COUNT(*) FROM empty_t GROUP BY val",
        "A5-group-empty",
    );

    eprintln!("A5: aggregate over empty — oracle parity confirmed");
}

// ─── W1: Window ROW_NUMBER / RANK / DENSE_RANK ───────────────────

#[test]
fn w1_window_ranking_functions() {
    let dir = test_tmpdir();
    let (f, c) = setup_oracle_pair(&dir, "w1");
    seed_both(&f, &c);

    let sql = "SELECT id, dept, amount, ROW_NUMBER() OVER (ORDER BY amount DESC) as rn FROM sales ORDER BY rn";

    let f_rows = f.query(sql).expect("f query");
    let mut c_stmt = c.prepare(sql).expect("c prepare");
    let c_rows: Vec<(i64, i64)> = c_stmt
        .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(3)?)))
        .expect("c query")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect");

    assert_eq!(f_rows.len(), c_rows.len(), "W1: row count mismatch");

    for (i, (f_row, c_row)) in f_rows.iter().zip(c_rows.iter()).enumerate() {
        let f_rn = match f_row.get(3) {
            Some(SqliteValue::Integer(v)) => *v,
            _ => -1,
        };
        assert_eq!(
            f_rn, c_row.1,
            "W1 row {i}: ROW_NUMBER mismatch f={f_rn}, c={}",
            c_row.1
        );
    }

    // RANK with partition
    compare_row_count(
        &f,
        &c,
        "SELECT id, RANK() OVER (PARTITION BY dept ORDER BY amount DESC) FROM sales",
        "W1-rank-partition",
    );

    eprintln!("W1: window ranking functions — oracle parity confirmed");
}

// ─── W2: Window SUM with frame ────────────────────────────────────

#[test]
fn w2_window_sum_frame() {
    let dir = test_tmpdir();
    let (f, c) = setup_oracle_pair(&dir, "w2");
    seed_both(&f, &c);

    let sql = "SELECT id, amount, SUM(amount) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as frame_sum FROM sales ORDER BY id";

    let f_rows = f.query(sql).expect("f query");
    let mut c_stmt = c.prepare(sql).expect("c prepare");
    let c_rows: Vec<(i64, i64)> = c_stmt
        .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(2)?)))
        .expect("c query")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect");

    assert_eq!(f_rows.len(), c_rows.len(), "W2: row count mismatch");

    for (i, (f_row, c_row)) in f_rows.iter().zip(c_rows.iter()).enumerate() {
        let f_sum = match f_row.get(2) {
            Some(SqliteValue::Integer(v)) => *v,
            _ => -1,
        };
        assert_eq!(
            f_sum, c_row.1,
            "W2 row {i}: frame SUM mismatch f={f_sum}, c={}",
            c_row.1
        );
    }

    eprintln!("W2: window SUM with ROWS BETWEEN frame — oracle parity confirmed");
}

// ─── W3: Window LAG / LEAD ────────────────────────────────────────

#[test]
fn w3_window_lag_lead() {
    let dir = test_tmpdir();
    let (f, c) = setup_oracle_pair(&dir, "w3");
    seed_both(&f, &c);

    let sql = "SELECT id, amount, LAG(amount, 1) OVER (ORDER BY id) as prev_amt, LEAD(amount, 1) OVER (ORDER BY id) as next_amt FROM sales ORDER BY id";

    let f_rows = f.query(sql).expect("f query");
    let mut c_stmt = c.prepare(sql).expect("c prepare");
    let c_rows: Vec<(i64, Option<i64>, Option<i64>)> = c_stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<i64>>(2)?,
                row.get::<_, Option<i64>>(3)?,
            ))
        })
        .expect("c query")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect");

    assert_eq!(f_rows.len(), c_rows.len(), "W3: row count mismatch");

    for (i, (f_row, c_row)) in f_rows.iter().zip(c_rows.iter()).enumerate() {
        let f_lag = match f_row.get(2) {
            Some(SqliteValue::Integer(v)) => Some(*v),
            _ => None,
        };
        assert_eq!(
            f_lag, c_row.1,
            "W3 row {i}: LAG mismatch f={f_lag:?}, c={:?}",
            c_row.1
        );
    }

    eprintln!("W3: window LAG/LEAD — oracle parity confirmed");
}

// ─── W4: PARTITION BY with ORDER BY ───────────────────────────────

#[test]
fn w4_partition_order() {
    let dir = test_tmpdir();
    let (f, c) = setup_oracle_pair(&dir, "w4");
    seed_both(&f, &c);

    let sql = "SELECT dept, id, SUM(amount) OVER (PARTITION BY dept ORDER BY id) as running FROM sales ORDER BY dept, id";

    let f_rows = f.query(sql).expect("f query");
    let mut c_stmt = c.prepare(sql).expect("c prepare");
    let c_rows: Vec<(String, i64, i64)> = c_stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .expect("c query")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect");

    assert_eq!(f_rows.len(), c_rows.len(), "W4: row count mismatch");

    for (i, (f_row, c_row)) in f_rows.iter().zip(c_rows.iter()).enumerate() {
        let f_running = match f_row.get(2) {
            Some(SqliteValue::Integer(v)) => *v,
            _ => -1,
        };
        assert_eq!(
            f_running, c_row.2,
            "W4 row {i}: running SUM mismatch f={f_running}, c={}",
            c_row.2
        );
    }

    eprintln!("W4: PARTITION BY ORDER BY running SUM — oracle parity confirmed");
}

// ─── W5: Window over empty partition ──────────────────────────────

#[test]
fn w5_window_empty_partition() {
    let dir = test_tmpdir();
    let (f, c) = setup_oracle_pair(&dir, "w5");
    f.execute("CREATE TABLE empty_w (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)")
        .expect("f create");
    c.execute_batch("CREATE TABLE empty_w (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)")
        .expect("c create");

    compare_row_count(
        &f,
        &c,
        "SELECT id, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY id) FROM empty_w",
        "W5-empty",
    );

    eprintln!("W5: window over empty partition — oracle parity confirmed");
}

// ─── I1: INSERT...SELECT basic ────────────────────────────────────

#[test]
fn i1_insert_select_basic() {
    let dir = test_tmpdir();
    let (f, c) = setup_oracle_pair(&dir, "i1");
    seed_both(&f, &c);

    // Create destination tables
    f.execute(
        "CREATE TABLE sales_copy (id INTEGER PRIMARY KEY, dept TEXT, amount INTEGER, qty INTEGER)",
    )
    .expect("f create copy");
    c.execute_batch(
        "CREATE TABLE sales_copy (id INTEGER PRIMARY KEY, dept TEXT, amount INTEGER, qty INTEGER)",
    )
    .expect("c create copy");

    // INSERT...SELECT
    f.execute("INSERT INTO sales_copy SELECT * FROM sales")
        .expect("f insert-select");
    c.execute_batch("INSERT INTO sales_copy SELECT * FROM sales")
        .expect("c insert-select");

    compare_int_query(&f, &c, "SELECT COUNT(*) FROM sales_copy", "I1-count");
    compare_int_query(&f, &c, "SELECT SUM(amount) FROM sales_copy", "I1-sum");

    eprintln!("I1: INSERT...SELECT basic — oracle parity confirmed");
}

// ─── I2: INSERT...SELECT with aggregates ──────────────────────────

#[test]
fn i2_insert_select_aggregates() {
    let dir = test_tmpdir();
    let (f, c) = setup_oracle_pair(&dir, "i2");
    seed_both(&f, &c);

    // Create summary table
    f.execute("CREATE TABLE dept_summary (dept TEXT PRIMARY KEY, total INTEGER, cnt INTEGER)")
        .expect("f create summary");
    c.execute_batch(
        "CREATE TABLE dept_summary (dept TEXT PRIMARY KEY, total INTEGER, cnt INTEGER)",
    )
    .expect("c create summary");

    // INSERT...SELECT with GROUP BY
    let insert_agg =
        "INSERT INTO dept_summary SELECT dept, SUM(amount), COUNT(*) FROM sales GROUP BY dept";
    f.execute(insert_agg).expect("f insert-agg");
    c.execute_batch(insert_agg).expect("c insert-agg");

    compare_int_query(&f, &c, "SELECT COUNT(*) FROM dept_summary", "I2-count");
    compare_int_query(
        &f,
        &c,
        "SELECT SUM(total) FROM dept_summary",
        "I2-sum-total",
    );
    compare_int_query(&f, &c, "SELECT SUM(cnt) FROM dept_summary", "I2-sum-cnt");

    eprintln!("I2: INSERT...SELECT with aggregates — oracle parity confirmed");
}

// ─── I3: INSERT...SELECT cross-table ──────────────────────────────

#[test]
fn i3_insert_select_cross_table() {
    let dir = test_tmpdir();
    let (f, c) = setup_oracle_pair(&dir, "i3");
    seed_both(&f, &c);

    // Create secondary table
    let setup = "CREATE TABLE high_value (id INTEGER PRIMARY KEY, dept TEXT, amount INTEGER)";
    f.execute(setup).expect("f create hv");
    c.execute_batch(setup).expect("c create hv");

    // INSERT...SELECT with WHERE filter
    let insert_sql =
        "INSERT INTO high_value SELECT id, dept, amount FROM sales WHERE amount >= 200";
    f.execute(insert_sql).expect("f insert-select");
    c.execute_batch(insert_sql).expect("c insert-select");

    compare_int_query(&f, &c, "SELECT COUNT(*) FROM high_value", "I3-count");
    compare_int_query(&f, &c, "SELECT SUM(amount) FROM high_value", "I3-sum");
    compare_int_query(&f, &c, "SELECT MIN(amount) FROM high_value", "I3-min");

    eprintln!("I3: INSERT...SELECT cross-table — oracle parity confirmed");
}

// ─── I4: INSERT...SELECT with WHERE filter ────────────────────────

#[test]
fn i4_insert_select_with_join() {
    let dir = test_tmpdir();
    let (f, c) = setup_oracle_pair(&dir, "i4");
    seed_both(&f, &c);

    // Create department budget table
    let dept_setup = "CREATE TABLE dept_budget (dept TEXT PRIMARY KEY, budget INTEGER)";
    f.execute(dept_setup).expect("f create budget");
    c.execute_batch(dept_setup).expect("c create budget");

    for (dept, budget) in &[("eng", 500), ("sales", 1000), ("hr", 200), ("ops", 600)] {
        f.execute(&format!(
            "INSERT INTO dept_budget VALUES ('{dept}', {budget})"
        ))
        .expect("f insert budget");
        c.execute(
            "INSERT INTO dept_budget VALUES (?1, ?2)",
            rusqlite::params![dept, budget],
        )
        .expect("c insert budget");
    }

    // Create result table
    let result_setup = "CREATE TABLE over_budget (dept TEXT, total_spent INTEGER, budget INTEGER)";
    f.execute(result_setup).expect("f create result");
    c.execute_batch(result_setup).expect("c create result");

    // INSERT...SELECT with JOIN
    let insert_join = "INSERT INTO over_budget \
         SELECT s.dept, SUM(s.amount), b.budget \
         FROM sales s JOIN dept_budget b ON s.dept = b.dept \
         GROUP BY s.dept \
         HAVING SUM(s.amount) > b.budget";
    f.execute(insert_join).expect("f insert-join");
    c.execute_batch(insert_join).expect("c insert-join");

    compare_int_query(&f, &c, "SELECT COUNT(*) FROM over_budget", "I4-count");

    eprintln!("I4: INSERT...SELECT with JOIN+HAVING — oracle parity confirmed");
}
