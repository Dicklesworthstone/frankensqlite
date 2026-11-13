//! bd-reality-check-1022-llk6g.2: every connection on a `RuntimeContext`
//! built inside a runtime (how the CLI opens) carries that context's io Cx,
//! and closing any one of them cancelled it, which cancelled every other
//! connection on the context. DETACH closes the attached child, so on such a
//! connection every statement after DETACH failed with "callback requested
//! query abort", and close failed the same way. A plain `Connection::open`
//! carries no runtime handle and never showed it, which is how the e2e oracle
//! missed it.

#![cfg(all(not(target_arch = "wasm32"), feature = "native"))]
#![recursion_limit = "512"]

use std::path::Path;
use std::sync::Arc;

use asupersync::runtime::RuntimeBuilder;
use fsqlite_core::connection::{Connection, ConnectionEnv, RuntimeConfig, RuntimeContext};
use fsqlite_types::cx::Cx;
use fsqlite_types::value::SqliteValue;
use rusqlite::types::Value;

/// A statement's rows rendered as text, or the fact that it failed.
type Outcome = Result<Vec<Vec<String>>, String>;

fn frank_text(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(integer) => integer.to_string(),
        SqliteValue::Float(float) => float.to_string(),
        _ => value.as_text().unwrap_or_default().to_owned(),
    }
}

fn stock_text(value: Value) -> String {
    match value {
        Value::Null => "NULL".to_owned(),
        Value::Integer(integer) => integer.to_string(),
        Value::Real(float) => float.to_string(),
        Value::Text(text) => text,
        Value::Blob(blob) => format!("{blob:?}"),
    }
}

async fn frank_outcome(conn: &Connection, sql: &str) -> Outcome {
    conn.query(sql)
        .await
        .map(|rows| {
            rows.iter()
                .map(|row| row.values().iter().map(frank_text).collect())
                .collect()
        })
        .map_err(|error| error.to_string())
}

fn stock_outcome(conn: &rusqlite::Connection, sql: &str) -> Outcome {
    let mut statement = conn.prepare(sql).map_err(|error| error.to_string())?;
    let width = statement.column_count();
    statement
        .query_map([], |row| {
            (0..width)
                .map(|index| row.get::<_, Value>(index).map(stock_text))
                .collect::<rusqlite::Result<Vec<_>>>()
        })
        .map_err(|error| error.to_string())?
        .collect::<rusqlite::Result<_>>()
        .map_err(|error| error.to_string())
}

/// Attach, write, DETACH, then keep using the connection: main-schema DDL
/// and DML, a re-ATTACH under the same alias, a DETACH that succeeds and
/// one that fails.
fn script(aux: &str) -> Vec<String> {
    [
        format!("ATTACH DATABASE '{aux}' AS aux"),
        "CREATE TABLE aux.t (x INTEGER)".to_owned(),
        "INSERT INTO aux.t VALUES (1), (2)".to_owned(),
        "DETACH DATABASE aux".to_owned(),
        "SELECT 'after-detach'".to_owned(),
        "CREATE TABLE main.m (y INTEGER)".to_owned(),
        "INSERT INTO main.m VALUES (3)".to_owned(),
        "SELECT y FROM main.m".to_owned(),
        format!("ATTACH DATABASE '{aux}' AS aux"),
        // A file-backed aux kept its rows; a fresh :memory: aux has no table.
        "SELECT count(*) FROM aux.t".to_owned(),
        "DETACH DATABASE aux".to_owned(),
        "DETACH DATABASE aux".to_owned(),
        "SELECT 'still-usable', (SELECT count(*) FROM main.m)".to_owned(),
    ]
    .into()
}

fn location(directory: &Path, file: Option<&str>) -> String {
    file.map_or_else(
        || ":memory:".to_owned(),
        |name| {
            directory
                .join(name)
                .to_str()
                .expect("utf-8 path")
                .to_owned()
        },
    )
}

#[test]
fn detach_leaves_a_runtime_env_connection_usable_like_stock() {
    let runtime = RuntimeBuilder::current_thread()
        .blocking_threads(1, 2)
        .build()
        .expect("build the CLI's runtime flavor and blocking pool");
    for (main_file, aux_file) in [
        (None, None),
        (None, Some("aux.db")),
        (Some("main.db"), None),
        (Some("main.db"), Some("aux.db")),
    ] {
        let label = format!(
            "main={} aux={}",
            main_file.unwrap_or(":memory:"),
            aux_file.unwrap_or(":memory:")
        );
        let frank_dir = tempfile::tempdir().expect("tempdir");
        let stock_dir = tempfile::tempdir().expect("tempdir");
        let stock =
            rusqlite::Connection::open(location(stock_dir.path(), main_file)).expect("stock open");
        let stock_outcomes: Vec<Outcome> = script(&location(stock_dir.path(), aux_file))
            .iter()
            .map(|sql| stock_outcome(&stock, sql))
            .collect();

        runtime.block_on(async {
            // Built inside block_on, so the context captures this runtime's
            // handle, as the CLI's does.
            let env = ConnectionEnv::new(Arc::new(RuntimeContext::new(RuntimeConfig::default())));
            let conn = Connection::open_with_env(location(frank_dir.path(), main_file), env)
                .await
                .expect("fsqlite open");
            let statements = script(&location(frank_dir.path(), aux_file));
            for (sql, stock) in statements.iter().zip(&stock_outcomes) {
                let frank = frank_outcome(&conn, sql).await;
                match (&frank, stock) {
                    (Ok(frank_rows), Ok(stock_rows)) => {
                        assert_eq!(frank_rows, stock_rows, "{label}: `{sql}`");
                    }
                    (Err(_), Err(_)) => {}
                    _ => panic!("{label}: `{sql}`\n  fsqlite: {frank:?}\n  stock:   {stock:?}"),
                }
                if let Err(error) = &frank {
                    assert!(
                        !error.contains("query abort"),
                        "{label}: `{sql}` failed with a cancelled context: {error}"
                    );
                }
            }

            // The prepared path runs on the same context.
            let statement = conn
                .prepare("SELECT y FROM main.m")
                .await
                .unwrap_or_else(|error| panic!("{label}: prepare after DETACH: {error}"));
            let rows = statement
                .query()
                .await
                .unwrap_or_else(|error| panic!("{label}: prepared query after DETACH: {error}"));
            assert_eq!(rows.len(), 1, "{label}: prepared rows");
            drop(statement);

            conn.close()
                .await
                .unwrap_or_else(|error| panic!("{label}: close after DETACH: {error}"));
        });
        drop(stock);
    }
}

/// One row of one column.
fn single(text: &str) -> Vec<Vec<String>> {
    vec![vec![text.to_owned()]]
}

/// Closing one connection, awaited or dropped, must leave the other
/// connections on its `RuntimeContext`, and later opens on it, usable.
#[test]
fn closing_one_connection_keeps_its_runtime_siblings_usable() {
    let runtime = RuntimeBuilder::current_thread()
        .blocking_threads(1, 2)
        .build()
        .expect("build the CLI's runtime flavor and blocking pool");
    runtime.block_on(async {
        let dir = tempfile::tempdir().expect("tempdir");
        let env = ConnectionEnv::new(Arc::new(RuntimeContext::new(RuntimeConfig::default())));
        let sibling = Connection::open_with_env(location(dir.path(), Some("sibling.db")), env.clone())
            .await
            .expect("open the sibling");
        sibling
            .execute("CREATE TABLE t (x INTEGER)")
            .await
            .expect("create");

        let closed = Connection::open_with_env(":memory:", env.clone())
            .await
            .expect("open the connection to close");
        closed.close().await.expect("close a peer");
        assert_eq!(frank_outcome(&sibling, "SELECT 'after-close'").await, Ok(single("after-close")));

        let dropped = Connection::open_with_env(location(dir.path(), Some("dropped.db")), env.clone())
            .await
            .expect("open the connection to drop");
        drop(dropped);
        sibling
            .execute("INSERT INTO t VALUES (1)")
            .await
            .unwrap_or_else(|error| panic!("sibling write after a peer closed: {error}"));

        let later = Connection::open_with_env(":memory:", env)
            .await
            .expect("open after a peer closed");
        assert_eq!(frank_outcome(&later, "SELECT 'later'").await, Ok(single("later")));
        later.close().await.expect("close the later connection");

        assert_eq!(frank_outcome(&sibling, "SELECT count(*) FROM t").await, Ok(single("1")));
        sibling.close().await.expect("close the sibling");
    });
}

/// A context seeded with the caller's own `Cx` hands the caller's native
/// context to every connection. Closing a connection must not cancel it, and
/// with it the caller.
#[test]
fn closing_a_connection_leaves_the_callers_native_context_alone() {
    let runtime = RuntimeBuilder::current_thread()
        .blocking_threads(1, 2)
        .build()
        .expect("build the CLI's runtime flavor and blocking pool");
    runtime.block_on(async {
        let native = asupersync::Cx::for_testing();
        let caller: Cx = Cx::new();
        caller.set_native_cx(native.clone());
        let env = ConnectionEnv::new(Arc::new(RuntimeContext::new_with_root_cx(
            RuntimeConfig::default(),
            &caller,
        )));

        let first = Connection::open_with_env(":memory:", env.clone())
            .await
            .expect("open");
        first
            .execute("CREATE TABLE t (x INTEGER)")
            .await
            .expect("create");
        first.close().await.expect("close");
        assert!(
            !native.is_cancel_requested(),
            "closing a connection cancelled the caller's native context"
        );
        assert!(caller.checkpoint().is_ok(), "the caller's Cx was cancelled");

        let second = Connection::open_with_env(":memory:", env)
            .await
            .expect("open after a close");
        assert_eq!(frank_outcome(&second, "SELECT 'second'").await, Ok(single("second")));
        second.close().await.expect("close the second connection");
    });
}
