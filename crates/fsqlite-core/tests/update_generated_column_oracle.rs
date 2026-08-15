//! bd-gh-generated-column-update-target-4r7kw (GH #165): UPDATE that assigns to
//! a generated (STORED or VIRTUAL) column must be rejected ("cannot UPDATE
//! generated column"), like C SQLite — on both the interpreted (:memory:) and
//! compiled (file-backed) UPDATE lanes. Assigning only ordinary columns still
//! works, and the generated column recomputes.
//!
//! KNOWN-RED investigation anchor (bd-gh-generated-column-update-target). The
//! fix must run at UPDATE statement PREPARE/COMPILE time, before the prepared
//! fast-path program is built — NOT per-execute-path. `:memory:` routes through
//! `execute_precompiled_prepared_update_or_delete`, which operates on the
//! compiled program (no AST), so a check placed in `execute_statement_dispatch_impl`
//! (the general lane) misses it. The ready helper shape is
//! `validate_update_target_columns(table_schema, assignments)` mirroring the
//! INSERT-side `validate_insert_target_columns` (connection.rs) — reject when a
//! target column has `generated_expr`/`generated_stored`. Un-ignore once the
//! guard is wired at the universal prepare/compile gate covering all UPDATE
//! lanes (direct-simple, precompiled, row-by-row, CTE, table-program).

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;

const SEED: &[&str] = &[
    "CREATE TABLE t(a INTEGER, s INTEGER GENERATED ALWAYS AS (a*2) STORED, \
     v INTEGER GENERATED ALWAYS AS (a+1) VIRTUAL)",
    "INSERT INTO t(a) VALUES (1)",
];

// (sql, must_be_rejected)
const CASES: &[(&str, bool)] = &[
    ("UPDATE t SET s = 99", true),         // STORED generated
    ("UPDATE t SET v = 99", true),         // VIRTUAL generated
    ("UPDATE t SET a = 5, s = 100", true), // mixed ordinary + generated
    ("UPDATE t SET a = 5", false),         // ordinary column only — allowed
];

fn oracle_rejects(sql: &str) -> bool {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    for s in SEED {
        conn.execute(s, []).unwrap();
    }
    conn.execute(sql, []).is_err()
}

async fn open_seeded(path: Option<&std::path::Path>) -> Connection {
    let conn = match path {
        Some(p) => Connection::open(p.to_str().unwrap()).await.unwrap(),
        None => Connection::open(":memory:").await.unwrap(),
    };
    for s in SEED {
        conn.execute(s)
            .await
            .unwrap_or_else(|e| panic!("seed `{s}`: {e:?}"));
    }
    conn
}

#[test]
fn update_generated_column_matches_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        for (case_i, (sql, must_reject)) in CASES.iter().enumerate() {
            assert_eq!(
                oracle_rejects(sql),
                *must_reject,
                "oracle premise for `{sql}`"
            );

            // Both the interpreted (:memory:) and compiled (file-backed) lanes.
            let file_path = dir.path().join(format!("case{case_i}.db"));
            for path in [None, Some(file_path.as_path())] {
                let conn = open_seeded(path).await;
                // bd-gh-generated-column-update-target-4r7kw: the PREPARED
                // lane is the historically unguarded surface (the :memory:
                // precompiled program bypasses the execute-path guard) — a
                // generated-column target must now fail at prepare(), like
                // stock's prepare-time "cannot UPDATE generated column".
                let prepared = conn.prepare(sql).await;
                if *must_reject {
                    assert!(
                        prepared.is_err(),
                        "`{sql}` must be rejected at prepare, path={path:?}"
                    );
                } else {
                    prepared
                        .unwrap_or_else(|e| panic!("`{sql}` must prepare, path={path:?}: {e:?}"));
                }
                let result = conn.execute(sql).await;
                if *must_reject {
                    assert!(
                        result.is_err(),
                        "`{sql}` must be rejected (generated column target), path={path:?}"
                    );
                    // The rejected UPDATE must leave the row unchanged.
                    let rows = conn.query("SELECT a FROM t").await.expect("select a");
                    assert!(
                        matches!(rows[0].values()[0], SqliteValue::Integer(1)),
                        "rejected UPDATE must not mutate the row, path={path:?}"
                    );
                } else {
                    result.unwrap_or_else(|e| panic!("`{sql}` must succeed, path={path:?}: {e:?}"));
                    // a := 5, so the STORED generated column recomputes to 10.
                    let rows = conn.query("SELECT a, s FROM t").await.expect("select a,s");
                    assert!(matches!(rows[0].values()[0], SqliteValue::Integer(5)));
                    assert!(matches!(rows[0].values()[1], SqliteValue::Integer(10)));
                }
            }
        }
    });
}
