//! bd-gh-generated-column-pk-ddl-frvx8 (GH #181): a generated column may not be
//! part of a PRIMARY KEY (column-level, table-level, or composite). C SQLite
//! rejects "generated columns cannot be part of the PRIMARY KEY"; UNIQUE on a
//! generated column and a plain generated column are fine. This keeper pins
//! fsqlite's accept/reject decision against the rusqlite oracle.

use fsqlite_core::connection::Connection;

// (ddl, must_be_rejected)
const CASES: &[(&str, bool)] = &[
    // Column-level PRIMARY KEY on a generated column.
    ("CREATE TABLE t1(a INT, b INT GENERATED ALWAYS AS (a) STORED PRIMARY KEY)", true),
    // Table-level PRIMARY KEY naming a generated column.
    ("CREATE TABLE t2(a INT, b INT GENERATED ALWAYS AS (a) STORED, PRIMARY KEY(b))", true),
    // Composite PRIMARY KEY including a generated column.
    ("CREATE TABLE t3(a INT, b INT GENERATED ALWAYS AS (a) STORED, PRIMARY KEY(a,b))", true),
    // VIRTUAL generated column PK is also rejected.
    ("CREATE TABLE t4(a INT, b INT GENERATED ALWAYS AS (a) VIRTUAL PRIMARY KEY)", true),
    // Controls that must be ACCEPTED:
    ("CREATE TABLE u1(a INT, b INT GENERATED ALWAYS AS (a) STORED UNIQUE)", false),
    ("CREATE TABLE u2(a INT PRIMARY KEY, b INT GENERATED ALWAYS AS (a) STORED)", false),
];

fn oracle_rejects(ddl: &str) -> bool {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.execute(ddl, []).is_err()
}

#[test]
fn generated_column_primary_key_matches_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        for (ddl, must_reject) in CASES {
            // Premise: fsqlite and C SQLite must agree on the accept/reject.
            assert_eq!(
                oracle_rejects(ddl),
                *must_reject,
                "oracle premise wrong for `{ddl}`"
            );

            let conn = Connection::open(":memory:").await.unwrap();
            let result = conn.execute(ddl).await;
            if *must_reject {
                assert!(
                    result.is_err(),
                    "`{ddl}` must be rejected (generated column in PRIMARY KEY), but succeeded"
                );
            } else {
                result.unwrap_or_else(|e| panic!("`{ddl}` must be accepted, got {e:?}"));
            }
        }
    });
}
