//! GH #350 (bd-xfmv9) — CREATE TRIGGER / INDEX / VIEW text must persist into
//! sqlite_master verbatim, matching stock SQLite. The engine previously
//! re-rendered the AST, collapsing redundant parentheses (e.g. a `WHEN` clause's
//! `(((a) AND (b)))` became `a AND b`), so the stored text was semantically
//! equivalent but byte-different and shorter than what was issued. Any consumer
//! that treats `sqlite_master.sql` as a stable identity — a schema digest, an
//! admission contract pinned from a real store, or stock sqlite3 tooling — could
//! never round-trip such an object.
//!
//! bd-lgolw already fixed this class for CREATE TABLE (verbatim capture via
//! `pending_ddl_source`); bd-xfmv9 extends the same capture to the remaining
//! CREATE object kinds. Each object is checked twice: byte-verbatim against the
//! issued statement (the issue's own methodology) and against the C-SQLite
//! oracle on the identical DDL.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

async fn frank_master_sql(conn: &Connection, kind: &str, name: &str) -> String {
    let rows = conn
        .query(&format!(
            "SELECT sql FROM sqlite_master WHERE type = '{kind}' AND name = '{name}'"
        ))
        .await
        .expect("read sqlite_master");
    assert!(!rows.is_empty(), "no sqlite_master row for {kind} {name}");
    match rows[0].values()[0].clone() {
        SqliteValue::Text(s) => s.to_string(),
        other => panic!("sqlite_master.sql for {kind} {name} not text: {other:?}"),
    }
}

fn oracle_master_sql(conn: &rusqlite::Connection, kind: &str, name: &str) -> String {
    conn.query_row(
        "SELECT sql FROM sqlite_master WHERE type = ?1 AND name = ?2",
        rusqlite::params![kind, name],
        |row| row.get::<_, String>(0),
    )
    .expect("oracle read sqlite_master")
}

/// Run `ddl` (a single CREATE statement for `kind`/`name`, preceded by any
/// `setup` statements) on both engines and assert the stored `sqlite_master.sql`
/// is byte-identical to `ddl` on the engine, and identical to the oracle's.
async fn assert_verbatim(setup: &[&str], ddl: &str, kind: &str, name: &str) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open oracle");
    for s in setup {
        f.execute(s)
            .await
            .unwrap_or_else(|e| panic!("frank `{s}`: {e}"));
        r.execute_batch(s)
            .unwrap_or_else(|e| panic!("oracle `{s}`: {e}"));
    }
    f.execute(ddl)
        .await
        .unwrap_or_else(|e| panic!("frank `{ddl}`: {e}"));
    r.execute_batch(ddl)
        .unwrap_or_else(|e| panic!("oracle `{ddl}`: {e}"));

    let frank_sql = frank_master_sql(&f, kind, name).await;
    let oracle_sql = oracle_master_sql(&r, kind, name);

    assert_eq!(
        frank_sql, ddl,
        "stored {kind} SQL must be byte-verbatim with the issued statement"
    );
    assert_eq!(
        frank_sql, oracle_sql,
        "stored {kind} SQL must match the C-SQLite oracle"
    );
}

#[test]
fn create_trigger_when_clause_redundant_parens_persist_verbatim_gh350() {
    asupersync::test_utils::run_test(|| async {
        // Redundant parentheses around associative ANDs — semantically neutral,
        // which is exactly why the AST re-render silently dropped them (GH#350).
        assert_verbatim(
            &["CREATE TABLE facts (a TEXT, b TEXT, c TEXT)"],
            "CREATE TRIGGER trg_guard BEFORE INSERT ON facts \
             WHEN NOT (((NEW.a IS NULL) AND (NEW.b IS NULL)) AND (NEW.c IS NULL)) \
             BEGIN SELECT RAISE(ABORT, 'nope'); END",
            "trigger",
            "trg_guard",
        )
        .await;
    });
}

#[test]
fn create_index_partial_where_redundant_parens_persist_verbatim_gh350() {
    asupersync::test_utils::run_test(|| async {
        assert_verbatim(
            &["CREATE TABLE facts (a TEXT, b TEXT, c TEXT)"],
            "CREATE INDEX idx_guard ON facts (a) \
             WHERE (((a IS NOT NULL)) AND (b IS NOT NULL))",
            "index",
            "idx_guard",
        )
        .await;
    });
}

#[test]
fn create_view_redundant_parens_persist_verbatim_gh350() {
    asupersync::test_utils::run_test(|| async {
        assert_verbatim(
            &["CREATE TABLE facts (a TEXT, b TEXT, c TEXT)"],
            "CREATE VIEW v_guard AS \
             SELECT a FROM facts WHERE (((a IS NOT NULL)) AND (b IS NOT NULL))",
            "view",
            "v_guard",
        )
        .await;
    });
}
