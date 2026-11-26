//! bd-5haia: an acyclic chain of views must never abort the process.
//!
//! `CREATE VIEW v0 AS SELECT 1 AS x; CREATE VIEW v1 AS SELECT x FROM v0; ...;
//! SELECT * FROM vN` used to re-enter statement execution once per view level,
//! so the native stack grew with the chain and the process aborted with a
//! stack overflow at depth 8 on a 2 MiB thread.
//!
//! The engine now accepts chains nesting up to `MAX_VIEW_NESTING_DEPTH`
//! (999) views on that stack and refuses deeper ones with the typed
//! `FrankenError::ViewNestingTooDeep`. That is stock SQLite 3.54.1's
//! boundary (bd-lpsil): it answers 999 nested views and refuses the 1000th
//! with "VIEWs and/or subqueries nested too deep", the text the typed error
//! carries. The boundary tests check both sides against live stock.
//!
//! bd-6tmuj: stock counts each CTE expansion as a level too. The `bd_6tmuj_*`
//! tests pin a flat CTE chain at 999 / 1000, a chain of views read through
//! WITH at 499 / 500 (two levels each), and an alternating chain at 665 /
//! 666, against live stock; flat chains of 2,028 and 5,000 CTEs, which
//! aborted the process, must be refused with stock's text.
//!
//! Every workload here runs on a dedicated 2 MiB thread inside a helper
//! process (this test binary, re-invoked on one test). A stack overflow aborts
//! the helper, not the test runner, and the parent test fails with the
//! helper's exit status and stderr. Results are compared with stock SQLite
//! (the bundled rusqlite oracle) running the same statements.

use std::io::Read;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use fsqlite_core::connection::{Connection, MAX_VIEW_NESTING_DEPTH};
use fsqlite_error::FrankenError;
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

/// View bodies that name the view they read with a `schema.table.column`
/// reference. Materialization renames each body's view sources to TEMP
/// tables, so these references must be bound (bd-at0bx) before that rename
/// or `main.v0.x` no longer finds `main.v0`.
#[test]
fn bd_5haia_view_chain_with_schema_qualified_columns_matches_stock_on_2mib_stack() {
    const NAME: &str =
        "bd_5haia_view_chain_with_schema_qualified_columns_matches_stock_on_2mib_stack";
    if !is_helper_for(NAME) {
        run_in_helper_process(NAME);
        return;
    }
    on_small_stack(|runtime, conn| {
        let mut setup = vec![
            "CREATE TABLE t(k INTEGER PRIMARY KEY);".to_owned(),
            "INSERT INTO t(k) VALUES (1), (2), (3);".to_owned(),
            "CREATE VIEW q0 AS SELECT main.t.k AS x FROM t;".to_owned(),
        ];
        setup.extend((1..=CHAIN_DEPTH).map(|level| {
            let previous = level - 1;
            format!("CREATE VIEW q{level} AS SELECT main.q{previous}.x + 1 AS x FROM q{previous};")
        }));
        setup.extend([
            format!(
                "CREATE VIEW joined AS SELECT main.q{CHAIN_DEPTH}.x AS top, main.q0.x AS leaf \
                 FROM q{CHAIN_DEPTH} JOIN main.q0 ON main.q{CHAIN_DEPTH}.x = main.q0.x + {CHAIN_DEPTH};"
            ),
            "CREATE TEMP VIEW tq AS SELECT main.joined.top FROM main.joined;".to_owned(),
        ]);
        let queries = [
            format!("SELECT * FROM q{CHAIN_DEPTH} ORDER BY x;"),
            "SELECT * FROM joined ORDER BY leaf;".to_owned(),
            "SELECT * FROM tq ORDER BY top;".to_owned(),
            format!("SELECT main.q{CHAIN_DEPTH}.x FROM q{CHAIN_DEPTH} ORDER BY 1;"),
        ];
        assert_matches_stock(runtime, conn, &setup, &queries);
    });
}

/// The deepest chain the engine accepts: `SELECT * FROM v998` over
/// `v0 <- ... <- v998` nests exactly `MAX_VIEW_NESTING_DEPTH` (999) views,
/// the deepest chain stock SQLite answers. It must work on the same 2 MiB
/// stack and agree with stock, so the limit is a real supported depth rather
/// than a refusal standing in for one; a limit one view deeper than stock's
/// fails here, where stock refuses.
#[test]
fn bd_5haia_view_chain_at_nesting_limit_matches_stock_on_2mib_stack() {
    const NAME: &str = "bd_5haia_view_chain_at_nesting_limit_matches_stock_on_2mib_stack";
    if !is_helper_for(NAME) {
        run_in_helper_process(NAME);
        return;
    }
    on_small_stack(|runtime, conn| {
        let top = MAX_VIEW_NESTING_DEPTH - 1;
        let mut setup = bead_chain("v", top);
        setup.push("CREATE VIEW w0 AS SELECT 0 AS n;".to_owned());
        setup.extend((1..=top).map(|level| {
            format!(
                "CREATE VIEW w{level} AS SELECT n + 1 AS n FROM w{};",
                level - 1
            )
        }));
        let queries = [
            format!("SELECT * FROM v{top};"),
            format!("SELECT n FROM w{top};"),
            format!("SELECT (SELECT n FROM w{top}) + (SELECT x FROM v{top});"),
            // bd-6tmuj: a derived table over a view chain is no level.
            format!("SELECT x FROM (SELECT x FROM v{top});"),
        ];
        assert_matches_stock(runtime, conn, &setup, &queries);
    });
}

/// The message stock SQLite refuses `sql` with. Panics if stock answers.
fn stock_refusal(conn: &rusqlite::Connection, sql: &str) -> String {
    let error = match conn.prepare(sql) {
        Err(error) => error,
        Ok(mut statement) => match statement
            .query([])
            .and_then(|mut rows| rows.next().map(|_| ()))
        {
            Err(error) => error,
            Ok(()) => panic!("stock SQLite answered `{sql}`, which it must refuse"),
        },
    };
    match error {
        rusqlite::Error::SqliteFailure(_, Some(message)) => message,
        other => panic!("stock SQLite refused `{sql}` without a message: {other:?}"),
    }
}

/// One view more than the limit fails with the typed error, wherever the
/// chain is read from, and leaves the connection usable. Stock SQLite 3.54.1
/// refuses the same four statements, and the typed error's text is stock's
/// (bd-lpsil); a limit one view shallower than stock's fails here, where
/// stock answers. Stock caches a view's column names once it has resolved
/// them, after which it would answer this chain too, so every refusal is
/// checked before anything on the chain is answered.
#[test]
fn bd_5haia_view_chain_beyond_nesting_limit_is_a_typed_error_on_2mib_stack() {
    const NAME: &str = "bd_5haia_view_chain_beyond_nesting_limit_is_a_typed_error_on_2mib_stack";
    if !is_helper_for(NAME) {
        run_in_helper_process(NAME);
        return;
    }
    on_small_stack(|runtime, conn| {
        let top = MAX_VIEW_NESTING_DEPTH;
        let stock = rusqlite::Connection::open_in_memory().expect("open stock SQLite");
        for statement in bead_chain("v", top) {
            runtime
                .block_on(conn.execute(&statement))
                .unwrap_or_else(|error| panic!("fsqlite failed `{statement}`: {error:?}"));
            stock
                .execute_batch(&statement)
                .unwrap_or_else(|error| panic!("stock SQLite failed `{statement}`: {error}"));
        }
        for query in [
            format!("SELECT * FROM v{top};"),
            format!("SELECT (SELECT x FROM v{top});"),
            format!("SELECT * FROM (SELECT x FROM v{top});"),
            format!("SELECT 1 WHERE EXISTS (SELECT 1 FROM v{top});"),
        ] {
            let stock_message = stock_refusal(&stock, &query);
            let error = runtime
                .block_on(conn.query(&query))
                .expect_err("a chain one view past the limit must be refused");
            assert!(
                matches!(
                    error,
                    FrankenError::ViewNestingTooDeep { max } if max == MAX_VIEW_NESTING_DEPTH
                ),
                "`{query}` must fail with ViewNestingTooDeep, got {error:?}"
            );
            assert_eq!(
                error.to_string(),
                stock_message,
                "`{query}`: the refusal must carry stock's text"
            );
        }
        // The refusal leaves no materialized state behind: the chain one
        // level shallower still reads, as in stock, and so does an unrelated
        // statement.
        let shallower = format!("SELECT * FROM v{};", top - 1);
        assert_eq!(
            frank_rows(runtime, conn, &shallower),
            stock_rows(&stock, &shallower)
        );
        assert_eq!(
            frank_rows(runtime, conn, &shallower),
            vec![vec!["int:1".to_owned()]]
        );
        assert_eq!(
            frank_rows(runtime, conn, "SELECT 41 + 1;"),
            vec![vec!["int:42".to_owned()]]
        );
    });
}

// ---------------------------------------------------------------------------
// bd-6tmuj: stock SQLite counts each CTE expansion as a nesting level beside
// each view. One WITH holding a flat chain of CTEs answers 999 of them and
// refuses 1000; a view read through a WITH costs two levels, so a chain of
// such views is refused at 500. The flat chain used to recurse once per CTE
// in the resolvers and abort the process with a stack overflow at about
// 2,000 on the CLI's main-thread stack, sooner on a 2 MiB thread.
// ---------------------------------------------------------------------------

/// `WITH c0 AS (SELECT 1 AS x), ci AS (SELECT x FROM c{i-1}), ...` up to
/// `c{depth}`, with no consuming SELECT.
fn flat_cte_chain(depth: usize) -> String {
    let mut ctes = vec!["c0 AS (SELECT 1 AS x)".to_owned()];
    ctes.extend((1..=depth).map(|level| format!("c{level} AS (SELECT x FROM c{})", level - 1)));
    format!("WITH {}", ctes.join(", "))
}

/// Views `{prefix}0 AS SELECT 1 AS x` and, above it, each view reading the
/// one below through `WITH c AS (SELECT x FROM ...) SELECT x FROM c` when
/// `through_with(level)` holds, else directly.
fn view_chain(prefix: &str, depth: usize, through_with: impl Fn(usize) -> bool) -> Vec<String> {
    let mut setup = vec![format!("CREATE VIEW {prefix}0 AS SELECT 1 AS x;")];
    setup.extend((1..=depth).map(|level| {
        let below = format!("{prefix}{}", level - 1);
        if through_with(level) {
            format!(
                "CREATE VIEW {prefix}{level} AS WITH c AS (SELECT x FROM {below}) SELECT x FROM c;"
            )
        } else {
            format!("CREATE VIEW {prefix}{level} AS SELECT x FROM {below};")
        }
    }));
    setup
}

/// `sql` is refused by stock and by fsqlite, directly and prepared, with the
/// typed ViewNestingTooDeep carrying stock's text.
fn assert_refused_like_stock(
    runtime: &asupersync::runtime::Runtime,
    conn: &Connection,
    stock: &rusqlite::Connection,
    sql: &str,
) {
    let stock_message = stock_refusal(stock, sql);
    let direct = runtime
        .block_on(conn.query(sql))
        .expect_err("fsqlite must refuse what stock refuses");
    let prepared = match runtime.block_on(conn.prepare(sql)) {
        Err(error) => error,
        Ok(statement) => runtime
            .block_on(statement.query())
            .expect_err("fsqlite must refuse the prepared statement too"),
    };
    for (mode, error) in [("direct", direct), ("prepared", prepared)] {
        assert!(
            matches!(
                error,
                FrankenError::ViewNestingTooDeep { max } if max == MAX_VIEW_NESTING_DEPTH
            ),
            "{mode}: must fail with ViewNestingTooDeep, got {error:?}"
        );
        assert_eq!(
            error.to_string(),
            stock_message,
            "{mode}: the refusal must carry stock's text"
        );
    }
}

fn apply_to_both(
    runtime: &asupersync::runtime::Runtime,
    conn: &Connection,
    stock: &rusqlite::Connection,
    setup: &[String],
) {
    for statement in setup {
        runtime
            .block_on(conn.execute(statement))
            .unwrap_or_else(|error| panic!("fsqlite failed `{statement}`: {error:?}"));
        stock
            .execute_batch(statement)
            .unwrap_or_else(|error| panic!("stock SQLite failed `{statement}`: {error}"));
    }
}

/// 999 chained CTEs answer, as in stock, also read through a scalar
/// subquery, which is no level; 1000 are refused with stock's text, read
/// either way.
#[test]
fn bd_6tmuj_flat_cte_chain_at_the_nesting_limit_matches_stock_on_2mib_stack() {
    const NAME: &str = "bd_6tmuj_flat_cte_chain_at_the_nesting_limit_matches_stock_on_2mib_stack";
    if !is_helper_for(NAME) {
        run_in_helper_process(NAME);
        return;
    }
    on_small_stack(|runtime, conn| {
        let stock = rusqlite::Connection::open_in_memory().expect("open stock SQLite");
        let top = MAX_VIEW_NESTING_DEPTH - 1;
        let answered = [
            format!("{} SELECT x FROM c{top};", flat_cte_chain(top)),
            format!("{} SELECT (SELECT x FROM c{top});", flat_cte_chain(top)),
        ];
        for query in &answered {
            let expected = stock_rows(&stock, query);
            assert_eq!(
                expected,
                vec![vec!["int:1".to_owned()]],
                "stock on `{query}`"
            );
            assert_eq!(frank_rows(runtime, conn, query), expected);
        }
        let deeper = top + 1;
        for refused in [
            format!("{} SELECT x FROM c{deeper};", flat_cte_chain(deeper)),
            format!(
                "{} SELECT (SELECT x FROM c{deeper});",
                flat_cte_chain(deeper)
            ),
        ] {
            assert_refused_like_stock(runtime, conn, &stock, &refused);
        }
        assert_eq!(
            frank_rows(runtime, conn, "SELECT 41 + 1;"),
            vec![vec!["int:42".to_owned()]]
        );
    });
}

/// The chains that aborted the process are refused, not a crash.
#[test]
fn bd_6tmuj_flat_cte_chains_far_past_the_limit_are_refused_on_2mib_stack() {
    const NAME: &str = "bd_6tmuj_flat_cte_chains_far_past_the_limit_are_refused_on_2mib_stack";
    if !is_helper_for(NAME) {
        run_in_helper_process(NAME);
        return;
    }
    on_small_stack(|runtime, conn| {
        let stock = rusqlite::Connection::open_in_memory().expect("open stock SQLite");
        for depth in [2_028, 5_000] {
            let query = format!("{} SELECT x FROM c{depth};", flat_cte_chain(depth));
            assert_refused_like_stock(runtime, conn, &stock, &query);
        }
        assert_eq!(
            frank_rows(runtime, conn, "SELECT 41 + 1;"),
            vec![vec!["int:42".to_owned()]]
        );
    });
}

/// Each view read through a WITH is two levels: 499 such views over v0 are
/// 999 levels and answer, 500 are refused, as in stock. Stock caches a
/// view's column names once resolved, so the refusal is checked first.
#[test]
fn bd_6tmuj_view_chain_through_with_matches_stock_on_2mib_stack() {
    const NAME: &str = "bd_6tmuj_view_chain_through_with_matches_stock_on_2mib_stack";
    if !is_helper_for(NAME) {
        run_in_helper_process(NAME);
        return;
    }
    on_small_stack(|runtime, conn| {
        let stock = rusqlite::Connection::open_in_memory().expect("open stock SQLite");
        let top = MAX_VIEW_NESTING_DEPTH.div_ceil(2);
        apply_to_both(runtime, conn, &stock, &view_chain("v", top, |_| true));
        assert_refused_like_stock(runtime, conn, &stock, &format!("SELECT x FROM v{top};"));
        let answered = format!("SELECT x FROM v{};", top - 1);
        let expected = stock_rows(&stock, &answered);
        assert_eq!(expected, vec![vec!["int:1".to_owned()]]);
        assert_eq!(frank_rows(runtime, conn, &answered), expected);
    });
}

/// Views alternating through a WITH (odd levels) and directly (even ones):
/// 665 over v0 are 999 levels and answer, 666 are refused, as in stock.
#[test]
fn bd_6tmuj_alternating_view_chain_matches_stock_on_2mib_stack() {
    const NAME: &str = "bd_6tmuj_alternating_view_chain_matches_stock_on_2mib_stack";
    if !is_helper_for(NAME) {
        run_in_helper_process(NAME);
        return;
    }
    on_small_stack(|runtime, conn| {
        let stock = rusqlite::Connection::open_in_memory().expect("open stock SQLite");
        let top = 666;
        apply_to_both(
            runtime,
            conn,
            &stock,
            &view_chain("v", top, |level| level % 2 == 1),
        );
        assert_refused_like_stock(runtime, conn, &stock, &format!("SELECT x FROM v{top};"));
        let answered = format!("SELECT x FROM v{};", top - 1);
        let expected = stock_rows(&stock, &answered);
        assert_eq!(expected, vec![vec!["int:1".to_owned()]]);
        assert_eq!(frank_rows(runtime, conn, &answered), expected);
    });
}

// ---------------------------------------------------------------------------
// bd-5iud6: nested scalar/EXISTS/IN/CASE subqueries and view chains nesting
// multiple scalar subqueries per view level must neither overflow the native
// stack nor hit exponential validation blowup.
// ---------------------------------------------------------------------------

fn nested_scalar_subquery(depth: usize, leaf: &str) -> String {
    let mut sql = leaf.to_owned();
    for _ in 0..depth {
        sql = format!("(SELECT {sql})");
    }
    format!("SELECT {sql};")
}

fn nested_exists_subquery(depth: usize) -> String {
    let mut sql = "SELECT 1".to_owned();
    for _ in 0..depth {
        sql = format!("SELECT 1 WHERE EXISTS ({sql})");
    }
    format!("SELECT EXISTS ({sql});")
}

fn nested_in_subquery(depth: usize) -> String {
    let mut sql = "SELECT 1".to_owned();
    for _ in 0..depth {
        sql = format!("SELECT 1 WHERE 1 IN ({sql})");
    }
    format!("SELECT 1 IN ({sql});")
}

fn nested_case_subquery(depth: usize) -> String {
    let mut sql = "1".to_owned();
    for _ in 0..depth {
        sql = format!("(SELECT CASE {sql} WHEN 1 THEN 1 ELSE 0 END)");
    }
    format!("SELECT {sql};")
}

fn nested_derived_table(depth: usize) -> String {
    let mut sql = "SELECT 1 AS x".to_owned();
    for _ in 0..depth {
        sql = format!("SELECT x FROM ({sql})");
    }
    format!("{sql};")
}

fn frank_prepared_rows(
    runtime: &asupersync::runtime::Runtime,
    conn: &Connection,
    sql: &str,
) -> Vec<Vec<String>> {
    let stmt = runtime
        .block_on(conn.prepare(sql))
        .unwrap_or_else(|error| panic!("fsqlite failed to prepare `{sql}`: {error:?}"));
    let rows = runtime
        .block_on(stmt.query())
        .unwrap_or_else(|error| panic!("fsqlite prepared query failed `{sql}`: {error:?}"));
    rows.iter()
        .map(|row| row.values().iter().map(frank_value).collect())
        .collect()
}

fn assert_direct_and_prepared_match_stock(
    runtime: &asupersync::runtime::Runtime,
    conn: &Connection,
    stock: &rusqlite::Connection,
    sql: &str,
) {
    let expected = stock_rows(stock, sql);
    let direct = frank_rows(runtime, conn, sql);
    let prepared = frank_prepared_rows(runtime, conn, sql);
    assert_eq!(direct, expected, "direct query mismatch on `{sql}`");
    assert_eq!(prepared, expected, "prepared query mismatch on `{sql}`");
}

fn on_pinned_stack<F>(stack_bytes: usize, label: &str, workload: F)
where
    F: FnOnce(&asupersync::runtime::Runtime) + Send + 'static,
{
    std::thread::Builder::new()
        .name(label.to_owned())
        .stack_size(stack_bytes)
        .spawn(move || {
            let dispatch = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
            tracing::dispatcher::with_default(&dispatch, || {
                let runtime = asupersync::runtime::RuntimeBuilder::current_thread()
                    .build()
                    .expect("current-thread runtime should build");
                workload(&runtime);
            });
        })
        .unwrap_or_else(|error| panic!("spawn {stack_bytes}-byte `{label}` thread: {error}"))
        .join()
        .unwrap_or_else(|panic| panic!("{stack_bytes}-byte `{label}` thread panicked: {panic:?}"));
}

/// Without views, `SELECT (SELECT (... (SELECT 1) ...))` used to abort a 2 MiB
/// thread at depth 12, take 29 s at depth 24, and fail to finish within 10 min
/// at depth 32 because collation validation re-walked subqueries
/// `O((1 + sqrt(2))^d)` times and nested subquery execution had no stack
/// checkpoint. Both `:memory:` and file-backed, direct and prepared, must
/// agree with live stock SQLite cleanly and fast on a 2 MiB thread.
#[test]
fn bd_5iud6_nested_scalar_subqueries_on_2mib_stack() {
    const NAME: &str = "bd_5iud6_nested_scalar_subqueries_on_2mib_stack";
    if !is_helper_for(NAME) {
        run_in_helper_process(NAME);
        return;
    }
    on_pinned_stack(SMALL_STACK_BYTES, "bd-5iud6-2mib", |runtime| {
        let tmp = tempfile::tempdir().expect("tempdir for file-backed bd-5iud6 check");
        let file_db_path = tmp.path().join("bd_5iud6.db");
        let file_db_str = file_db_path.to_str().expect("utf-8 tempdir path");

        for (mode, path) in [("memory", ":memory:"), ("file", file_db_str)] {
            let conn = runtime
                .block_on(Connection::open(path))
                .unwrap_or_else(|error| panic!("open {mode} fsqlite connection: {error:?}"));
            let stock = rusqlite::Connection::open_in_memory().expect("open stock SQLite");

            // Plain nested scalar subqueries across the bead's failure depths
            // and up to stock SQLite's own expression-depth threshold (stock
            // refuses depth 45+ with "Expression tree is too large"), including
            // a timing guard against O(2.414^d) regression.
            for depth in [12_usize, 20, 24, 32, 40] {
                let sql = nested_scalar_subquery(depth, "1");
                let started = Instant::now();
                assert_direct_and_prepared_match_stock(runtime, &conn, &stock, &sql);
                let elapsed = started.elapsed();
                assert!(
                    elapsed < Duration::from_secs(15),
                    "{mode}: depth-{depth} nested scalar subquery took {elapsed:?} \
                     (expected sub-second; exponential validation regression?)"
                );
            }
            // Also verify depth 64 directly on fsqlite without overflowing the
            // 2 MiB thread or regressing into exponential validation time.
            {
                let sql = nested_scalar_subquery(64, "1");
                let started = Instant::now();
                assert_eq!(
                    frank_rows(runtime, &conn, &sql),
                    vec![vec!["int:1".to_owned()]]
                );
                assert_eq!(
                    frank_prepared_rows(runtime, &conn, &sql),
                    vec![vec!["int:1".to_owned()]]
                );
                let elapsed = started.elapsed();
                assert!(
                    elapsed < Duration::from_secs(15),
                    "{mode}: depth-64 nested scalar subquery took {elapsed:?}"
                );
            }

            // Nested EXISTS, IN, CASE, and derived tables at depth 24.
            for sql in [
                nested_exists_subquery(24),
                nested_in_subquery(24),
                nested_case_subquery(24),
                nested_derived_table(24),
            ] {
                assert_direct_and_prepared_match_stock(runtime, &conn, &stock, &sql);
            }

            // Table-backed correlated nested scalar subqueries (20 deep).
            let setup = [
                "CREATE TABLE t_nested(k INTEGER PRIMARY KEY, v INTEGER);".to_owned(),
                "INSERT INTO t_nested(k, v) VALUES (1, 10), (2, 20), (3, 30);".to_owned(),
            ];
            apply_to_both(runtime, &conn, &stock, &setup);
            let mut correlated = "v + k".to_owned();
            for _ in 0..20 {
                correlated = format!("(SELECT {correlated})");
            }
            let correlated_sql = format!("SELECT k, {correlated} FROM t_nested ORDER BY k;");
            assert_direct_and_prepared_match_stock(runtime, &conn, &stock, &correlated_sql);

            // Memoizing collation consumer visits must preserve error
            // reporting for an unresolved collation buried deep inside nested
            // scalar subqueries.
            let bad_collation_sql =
                nested_scalar_subquery(24, "'a' = 'b' COLLATE bd_5iud6_missing_collation");
            let expected_msg = stock_refusal(&stock, &bad_collation_sql);
            let actual_err = runtime
                .block_on(conn.query(&bad_collation_sql))
                .expect_err("deep invalid collation must still be rejected");
            assert_eq!(
                actual_err.to_string(),
                expected_msg,
                "{mode}: deep invalid collation diagnostic mismatch"
            );

            // Connection stays healthy after both deep queries and deep error.
            assert_eq!(
                frank_rows(runtime, &conn, "SELECT 41 + 1;"),
                vec![vec!["int:42".to_owned()]]
            );
        }
    });
    println!("{HELPER_DONE_MARKER}");
}

/// Exact `bd-5iud6` view-chain repro: 16 view levels where each level nests
/// three scalar subqueries (`CREATE VIEW v{i} AS SELECT (SELECT (SELECT
/// (SELECT x FROM v{i-1}) AS x) AS x) AS x`). Swept across all 62 thread stack
/// sizes from 768 KiB to 16 MiB in 256 KiB steps (12/62 including 8 MiB used
/// to abort with only a view-boundary stack checkpoint), plus a file-backed
/// check on a 2 MiB stack.
#[test]
fn bd_5iud6_view_chain_nesting_three_subqueries_across_stack_sweep() {
    const NAME: &str = "bd_5iud6_view_chain_nesting_three_subqueries_across_stack_sweep";
    if !is_helper_for(NAME) {
        run_in_helper_process(NAME);
        return;
    }

    let mut setup = vec!["CREATE VIEW v0 AS SELECT 1 AS x;".to_owned()];
    setup.extend((1..=16_usize).map(|level| {
        let prev = level - 1;
        format!(
            "CREATE VIEW v{level} AS SELECT (SELECT (SELECT (SELECT x FROM v{prev}) AS x) AS x) AS x;"
        )
    }));
    let query = "SELECT * FROM v16;";

    let stock = rusqlite::Connection::open_in_memory().expect("open stock SQLite");
    for statement in &setup {
        stock
            .execute_batch(statement)
            .unwrap_or_else(|error| panic!("stock SQLite failed `{statement}`: {error}"));
    }
    let expected = stock_rows(&stock, query);
    assert_eq!(expected, vec![vec!["int:1".to_owned()]]);

    // Sweep 768 KiB ..= 16 MiB in 256 KiB steps (62 sizes).
    for step in 3..=64_usize {
        let stack_bytes = step * 256 * 1024;
        let setup_clone = setup.clone();
        let expected_clone = expected.clone();
        let check_prepared = matches!(step, 3 | 8 | 32);
        on_pinned_stack(stack_bytes, &format!("bd-5iud6-sweep-{step}"), move |runtime| {
            let conn = runtime
                .block_on(Connection::open(":memory:"))
                .expect("open in-memory connection");
            for statement in &setup_clone {
                runtime
                    .block_on(conn.execute(statement))
                    .unwrap_or_else(|error| {
                        panic!("stack={stack_bytes}: failed `{statement}`: {error:?}")
                    });
            }
            let actual = frank_rows(runtime, &conn, query);
            assert_eq!(
                actual, expected_clone,
                "stack={stack_bytes}: direct `{query}` differed from stock"
            );
            if check_prepared {
                let prepared = frank_prepared_rows(runtime, &conn, query);
                assert_eq!(
                    prepared, expected_clone,
                    "stack={stack_bytes}: prepared `{query}` differed from stock"
                );
            }
        });
    }

    // Also verify the exact same 16-level 3-subquery view chain on a
    // file-backed database on a 2 MiB stack (direct + prepared).
    let setup_file = setup;
    let expected_file = expected;
    on_pinned_stack(SMALL_STACK_BYTES, "bd-5iud6-file-2mib", move |runtime| {
        let tmp = tempfile::tempdir().expect("tempdir for file-backed view sweep");
        let db_path = tmp.path().join("bd_5iud6_views.db");
        let conn = runtime
            .block_on(Connection::open(db_path.to_str().expect("utf-8 path")))
            .expect("open file-backed connection");
        for statement in &setup_file {
            runtime
                .block_on(conn.execute(statement))
                .unwrap_or_else(|error| panic!("file-backed failed `{statement}`: {error:?}"));
        }
        assert_eq!(frank_rows(runtime, &conn, query), expected_file);
        assert_eq!(frank_prepared_rows(runtime, &conn, query), expected_file);
    });

    println!("{HELPER_DONE_MARKER}");
}

/// GH#414 preservation check for `bd-5iud6`: adding a native-stack checkpoint
/// to `Statement::Select` execution must NOT shift trigger/FK DML recursion
/// off the caller's thread stack, so `ensure_native_stack_headroom` and the
/// configured `MAX_TRIGGER_DEPTH` cap still fire cleanly with
/// `FrankenError::TriggerRecursionDepthExceeded` on pinned 4 MiB and 1 MiB
/// stacks — both for plain ping-pong triggers and for triggers whose bodies
/// evaluate nested scalar subqueries.
#[test]
fn bd_5iud6_preserves_gh414_recursive_trigger_depth_limit() {
    const NAME: &str = "bd_5iud6_preserves_gh414_recursive_trigger_depth_limit";
    if !is_helper_for(NAME) {
        run_in_helper_process(NAME);
        return;
    }

    for (label, stack_bytes, delta_expr) in [
        ("gh414-plain-4mib", 4 * 1024 * 1024, "1"),
        ("gh414-tight-1mib", 1024 * 1024, "1"),
        (
            "gh414-subquery-4mib",
            4 * 1024 * 1024,
            "(SELECT (SELECT (SELECT (SELECT 1))))",
        ),
    ] {
        on_pinned_stack(stack_bytes, label, move |runtime| {
            let conn = runtime
                .block_on(Connection::open(":memory:"))
                .expect("open in-memory connection");
            for stmt in [
                "PRAGMA recursive_triggers = ON;",
                "CREATE TABLE a (n INTEGER);",
                "CREATE TABLE b (n INTEGER);",
                "INSERT INTO a VALUES (0);",
                "INSERT INTO b VALUES (0);",
                &format!(
                    "CREATE TRIGGER trg_a AFTER UPDATE ON a \
                     BEGIN UPDATE b SET n = NEW.n + {delta_expr}; END;"
                ),
                &format!(
                    "CREATE TRIGGER trg_b AFTER UPDATE ON b \
                     BEGIN UPDATE a SET n = NEW.n + {delta_expr}; END;"
                ),
            ] {
                runtime
                    .block_on(conn.execute(stmt))
                    .unwrap_or_else(|error| panic!("{label}: setup `{stmt}` failed: {error:?}"));
            }
            let error = runtime
                .block_on(conn.execute("UPDATE a SET n = 1;"))
                .expect_err("recursive trigger cap must fire");
            assert!(
                matches!(&error, FrankenError::TriggerRecursionDepthExceeded),
                "{label}: expected TriggerRecursionDepthExceeded, got {error:?}"
            );
            assert_eq!(
                error.to_string(),
                "too many levels of trigger recursion",
                "{label}: unexpected trigger-depth error message"
            );
            assert!(
                !conn.in_transaction(),
                "{label}: failed autocommit trigger statement must leave no transaction open"
            );
            assert_eq!(
                frank_rows(runtime, &conn, "SELECT (SELECT n FROM a), (SELECT n FROM b);"),
                vec![vec!["int:0".to_owned(), "int:0".to_owned()]],
                "{label}: failed recursive trigger must roll back all nested writes"
            );
        });
    }

    println!("{HELPER_DONE_MARKER}");
}
