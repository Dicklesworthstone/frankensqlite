//! bd-5haia: an acyclic chain of views must never abort the process.
//!
//! `CREATE VIEW v0 AS SELECT 1 AS x; CREATE VIEW v1 AS SELECT x FROM v0; ...;
//! SELECT * FROM vN` used to re-enter statement execution once per view level,
//! so the native stack grew with the chain and the process aborted with a
//! stack overflow at depth 8 on a 2 MiB thread. Stock SQLite expands such
//! chains without a fixed limit.
//!
//! Every workload here runs on a dedicated 2 MiB thread inside a helper
//! process (this test binary, re-invoked on one test). A stack overflow aborts
//! the helper, not the test runner, and the parent test fails with the
//! helper's exit status and stderr. Results are compared with stock SQLite
//! (the bundled rusqlite oracle) running the same statements.

use std::io::Read;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// Names the test whose body the helper process should run.
const HELPER_ENV: &str = "FSQLITE_BD_5HAIA_HELPER";
/// Printed by a helper after its workload passed, so the parent can tell a
/// real run from a filter that matched no test.
const HELPER_DONE_MARKER: &str = "bd-5haia helper workload completed";
/// Rust's default spawned-thread stack, and the size the bead reports
/// overflowing at depth 8.
const SMALL_STACK_BYTES: usize = 2 * 1024 * 1024;
const HELPER_TIMEOUT: Duration = Duration::from_secs(600);

/// Depth used for the capability checks: comfortably past the bead's
/// "depth >= 100" bar and 16x the depth that used to abort.
const CHAIN_DEPTH: usize = 128;

fn is_helper_for(test_name: &str) -> bool {
    std::env::var(HELPER_ENV).is_ok_and(|value| value == test_name)
}

/// Re-run `test_name` alone in a child process and fail unless it exits
/// cleanly after completing its workload.
fn run_in_helper_process(test_name: &str) {
    let mut child = Command::new(std::env::current_exe().expect("test binary path"))
        .arg(test_name)
        .arg("--exact")
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(HELPER_ENV, test_name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("helper process should launch");
    let mut child_stdout = child.stdout.take().expect("piped stdout");
    let mut child_stderr = child.stderr.take().expect("piped stderr");
    let stdout_reader = std::thread::spawn(move || {
        let mut text = String::new();
        let _ = child_stdout.read_to_string(&mut text);
        text
    });
    let stderr_reader = std::thread::spawn(move || {
        let mut text = String::new();
        let _ = child_stderr.read_to_string(&mut text);
        text
    });
    let deadline = Instant::now() + HELPER_TIMEOUT;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(20));
            }
            Ok(None) => {
                let kill = child.kill();
                let wait = child.wait();
                panic!(
                    "helper `{test_name}` did not finish within {HELPER_TIMEOUT:?}; \
                     kill={kill:?}, wait={wait:?}"
                );
            }
            Err(error) => panic!("could not wait for helper `{test_name}`: {error}"),
        }
    };
    let stdout = stdout_reader.join().expect("stdout reader");
    let stderr = stderr_reader.join().expect("stderr reader");
    assert!(
        status.success() && stdout.contains(HELPER_DONE_MARKER),
        "helper `{test_name}` on a {SMALL_STACK_BYTES}-byte thread stack failed: {status}\n\
         --- helper stdout ---\n{stdout}\n--- helper stderr ---\n{stderr}"
    );
}

/// Run `workload` against a fresh in-memory connection on a dedicated thread
/// whose native stack is exactly [`SMALL_STACK_BYTES`].
fn on_small_stack<F>(workload: F)
where
    F: FnOnce(&asupersync::runtime::Runtime, &Connection) + Send + 'static,
{
    std::thread::Builder::new()
        .name("bd-5haia-2mib".to_owned())
        .stack_size(SMALL_STACK_BYTES)
        .spawn(move || {
            let dispatch = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
            tracing::dispatcher::with_default(&dispatch, || {
                let runtime = asupersync::runtime::RuntimeBuilder::current_thread()
                    .build()
                    .expect("current-thread runtime should build");
                // Drive one engine future at a time so this test's own async
                // state does not embed every engine future at once.
                let conn = runtime
                    .block_on(Connection::open(":memory:"))
                    .expect("open in-memory connection");
                workload(&runtime, &conn);
            });
        })
        .expect("spawn 2 MiB workload thread")
        .join()
        .expect("2 MiB workload thread panicked");
    println!("{HELPER_DONE_MARKER}");
}

fn frank_value(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => format!("int:{n}"),
        SqliteValue::Float(f) => format!("real:{f}"),
        SqliteValue::Text(s) => format!("text:{s}"),
        SqliteValue::Blob(b) => format!("blob:{b:02X?}"),
    }
}

fn stock_value(value: &rusqlite::types::Value) -> String {
    match value {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => format!("int:{n}"),
        rusqlite::types::Value::Real(f) => format!("real:{f}"),
        rusqlite::types::Value::Text(s) => format!("text:{s}"),
        rusqlite::types::Value::Blob(b) => format!("blob:{b:02X?}"),
    }
}

fn frank_rows(
    runtime: &asupersync::runtime::Runtime,
    conn: &Connection,
    sql: &str,
) -> Vec<Vec<String>> {
    let rows = runtime
        .block_on(conn.query(sql))
        .unwrap_or_else(|error| panic!("fsqlite failed `{sql}`: {error:?}"));
    rows.iter()
        .map(|row| row.values().iter().map(frank_value).collect())
        .collect()
}

fn stock_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut statement = conn
        .prepare(sql)
        .unwrap_or_else(|error| panic!("stock SQLite failed to prepare `{sql}`: {error}"));
    let width = statement.column_count();
    statement
        .query_map([], |row| {
            Ok((0..width)
                .map(|index| stock_value(&row.get_unwrap::<_, rusqlite::types::Value>(index)))
                .collect())
        })
        .unwrap_or_else(|error| panic!("stock SQLite failed `{sql}`: {error}"))
        .collect::<Result<Vec<_>, _>>()
        .unwrap_or_else(|error| panic!("stock SQLite row error for `{sql}`: {error}"))
}

/// Apply `setup` to both engines, then require every query in `queries` to
/// return exactly the rows stock SQLite returns.
fn assert_matches_stock(
    runtime: &asupersync::runtime::Runtime,
    conn: &Connection,
    setup: &[String],
    queries: &[String],
) {
    let stock = rusqlite::Connection::open_in_memory().expect("open stock SQLite");
    for statement in setup {
        runtime
            .block_on(conn.execute(statement))
            .unwrap_or_else(|error| panic!("fsqlite failed `{statement}`: {error:?}"));
        stock
            .execute_batch(statement)
            .unwrap_or_else(|error| panic!("stock SQLite failed `{statement}`: {error}"));
    }
    for query in queries {
        let expected = stock_rows(&stock, query);
        let actual = frank_rows(runtime, conn, query);
        assert_eq!(
            actual, expected,
            "fsqlite and stock SQLite differ on `{query}`"
        );
    }
}

/// The bead's shape: `v0 AS SELECT 1 AS x`, `vi AS SELECT x FROM v{i-1}`.
fn bead_chain(prefix: &str, depth: usize) -> Vec<String> {
    let mut setup = vec![format!("CREATE VIEW {prefix}0 AS SELECT 1 AS x;")];
    setup.extend((1..=depth).map(|level| {
        format!(
            "CREATE VIEW {prefix}{level} AS SELECT x FROM {prefix}{};",
            level - 1
        )
    }));
    setup
}

#[test]
fn bd_5haia_view_chain_128_deep_matches_stock_on_2mib_stack() {
    const NAME: &str = "bd_5haia_view_chain_128_deep_matches_stock_on_2mib_stack";
    if !is_helper_for(NAME) {
        run_in_helper_process(NAME);
        return;
    }
    on_small_stack(|runtime, conn| {
        let setup = bead_chain("v", CHAIN_DEPTH);
        let queries = [
            format!("SELECT * FROM v{CHAIN_DEPTH};"),
            format!("SELECT x FROM v{CHAIN_DEPTH};"),
            format!("SELECT x + 1, typeof(x) FROM v{CHAIN_DEPTH};"),
        ];
        assert_matches_stock(runtime, conn, &setup, &queries);
        // The connection stays fully usable after the deep expansion.
        assert_eq!(
            frank_rows(runtime, conn, "SELECT 41 + 1;"),
            vec![vec!["int:42".to_owned()]]
        );
    });
}

/// Every level transforms the row, so the result proves each of the 128
/// view bodies was applied exactly once, in order.
#[test]
fn bd_5haia_accumulating_view_chain_applies_every_level_on_2mib_stack() {
    const NAME: &str = "bd_5haia_accumulating_view_chain_applies_every_level_on_2mib_stack";
    if !is_helper_for(NAME) {
        run_in_helper_process(NAME);
        return;
    }
    on_small_stack(|runtime, conn| {
        let mut setup = vec!["CREATE VIEW w0 AS SELECT 0 AS n, 'leaf' AS tag;".to_owned()];
        setup.extend((1..=CHAIN_DEPTH).map(|level| {
            format!(
                "CREATE VIEW w{level} AS SELECT n + 1 AS n, tag || '' AS tag FROM w{};",
                level - 1
            )
        }));
        let queries = [
            format!("SELECT n, tag FROM w{CHAIN_DEPTH};"),
            format!("SELECT * FROM w{CHAIN_DEPTH};"),
            format!("SELECT n * 2 FROM w{CHAIN_DEPTH} WHERE tag = 'leaf';"),
        ];
        assert_matches_stock(runtime, conn, &setup, &queries);
    });
}

/// A `SELECT *` chain over a real table where every level filters one more
/// key, aggregated at the top.
#[test]
fn bd_5haia_filtered_star_view_chain_over_table_matches_stock_on_2mib_stack() {
    const NAME: &str = "bd_5haia_filtered_star_view_chain_over_table_matches_stock_on_2mib_stack";
    if !is_helper_for(NAME) {
        run_in_helper_process(NAME);
        return;
    }
    on_small_stack(|runtime, conn| {
        let rows = (1..=300)
            .map(|k| format!("({k}, 'row{k}')"))
            .collect::<Vec<_>>()
            .join(", ");
        let mut setup = vec![
            "CREATE TABLE t(k INTEGER PRIMARY KEY, label TEXT);".to_owned(),
            format!("INSERT INTO t(k, label) VALUES {rows};"),
            "CREATE VIEW s0 AS SELECT k, label FROM t;".to_owned(),
        ];
        setup.extend((1..=CHAIN_DEPTH).map(|level| {
            format!(
                "CREATE VIEW s{level} AS SELECT * FROM s{} WHERE k <> {};",
                level - 1,
                level * 2
            )
        }));
        let queries = [
            format!("SELECT count(*), sum(k), min(k), max(k) FROM s{CHAIN_DEPTH};"),
            format!("SELECT k, label FROM s{CHAIN_DEPTH} WHERE k < 12 ORDER BY k;"),
        ];
        assert_matches_stock(runtime, conn, &setup, &queries);
    });
}

/// The same 128-deep chain read through a join, a compound, scalar / IN /
/// EXISTS subqueries, a derived table and `INSERT ... SELECT`.
///
/// The subqueries are uncorrelated: a subquery over a view that refers to
/// the outer row (`EXISTS (SELECT 1 FROM v0 WHERE x = k)`) fails with
/// "no such column: k" even for a single view, on origin/main too. That is
/// a separate defect from the chain depth.
#[test]
fn bd_5haia_view_chain_read_through_joins_subqueries_and_dml_matches_stock_on_2mib_stack() {
    const NAME: &str =
        "bd_5haia_view_chain_read_through_joins_subqueries_and_dml_matches_stock_on_2mib_stack";
    if !is_helper_for(NAME) {
        run_in_helper_process(NAME);
        return;
    }
    on_small_stack(|runtime, conn| {
        let mut setup = vec![
            "CREATE TABLE t(k INTEGER PRIMARY KEY);".to_owned(),
            "INSERT INTO t(k) VALUES (1), (2), (3);".to_owned(),
            "CREATE TABLE sink(x);".to_owned(),
        ];
        setup.extend(bead_chain("v", CHAIN_DEPTH));
        setup.push(format!(
            "INSERT INTO sink SELECT x + 10 FROM v{CHAIN_DEPTH};"
        ));
        let queries = [
            format!(
                "SELECT t.k, v{CHAIN_DEPTH}.x FROM t JOIN v{CHAIN_DEPTH} ON t.k = v{CHAIN_DEPTH}.x;"
            ),
            format!("SELECT x FROM v{CHAIN_DEPTH} UNION ALL SELECT x + 1 FROM v{CHAIN_DEPTH};"),
            format!("SELECT (SELECT x FROM v{CHAIN_DEPTH}) * 7;"),
            format!("SELECT * FROM (SELECT x AS y FROM v{CHAIN_DEPTH});"),
            format!("SELECT k FROM t WHERE k IN (SELECT x FROM v{CHAIN_DEPTH}) ORDER BY k;"),
            format!(
                "SELECT k FROM t WHERE EXISTS (SELECT 1 FROM v{CHAIN_DEPTH} WHERE x = 1) ORDER BY k;"
            ),
            "SELECT x FROM sink;".to_owned(),
        ];
        assert_matches_stock(runtime, conn, &setup, &queries);
    });
}

/// 128-deep chains whose views reach the next one through a WITH clause, a
/// derived table, a scalar subquery or an IN subquery rather than a FROM
/// source, so each level re-enters statement execution while the view above
/// it is being materialized.
#[test]
fn bd_5haia_view_chains_linked_through_with_and_subqueries_match_stock_on_2mib_stack() {
    const NAME: &str =
        "bd_5haia_view_chains_linked_through_with_and_subqueries_match_stock_on_2mib_stack";
    if !is_helper_for(NAME) {
        run_in_helper_process(NAME);
        return;
    }
    on_small_stack(|runtime, conn| {
        let mut setup = Vec::new();
        let mut queries = Vec::new();
        for (prefix, body) in [
            ("w", "WITH c AS (SELECT x FROM {prev}) SELECT x FROM c"),
            ("d", "SELECT x FROM (SELECT x FROM {prev})"),
            ("s", "SELECT (SELECT x FROM {prev}) AS x"),
            ("i", "SELECT 1 AS x WHERE 1 IN (SELECT x FROM {prev})"),
        ] {
            setup.push(format!("CREATE VIEW {prefix}0 AS SELECT 1 AS x;"));
            setup.extend((1..=CHAIN_DEPTH).map(|level| {
                let previous = format!("{prefix}{}", level - 1);
                format!(
                    "CREATE VIEW {prefix}{level} AS {};",
                    body.replace("{prev}", &previous)
                )
            }));
            queries.push(format!("SELECT * FROM {prefix}{CHAIN_DEPTH};"));
        }
        assert_matches_stock(runtime, conn, &setup, &queries);
    });
}
