//! bd-gh-ctas-decltype-affinity-nmexg (GH #179): CREATE TABLE AS SELECT must
//! give each target column the affinity of the SELECT result-column decltype
//! (from the AST), not the storage class of the first materialized value. A
//! no-type source column yields NONE affinity (values keep their storage
//! class); a typed source column applies its affinity. Pinned vs the rusqlite
//! oracle.

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;

// Each script ends by selecting typeof(a) from the CTAS target, ordered.
const SCRIPTS: &[&[&str]] = &[
    // No-type source column: mixed storage must be preserved (NONE affinity).
    &[
        "CREATE TABLE s(a)",
        "INSERT INTO s VALUES ('1')",
        "INSERT INTO s VALUES (2)",
        "CREATE TABLE t AS SELECT a FROM s",
    ],
    // Reverse order.
    &[
        "CREATE TABLE s(a)",
        "INSERT INTO s VALUES (2)",
        "INSERT INTO s VALUES ('5')",
        "CREATE TABLE t AS SELECT a FROM s",
    ],
    // Declared INTEGER source column: INTEGER affinity coerces text digits.
    &[
        "CREATE TABLE s(a INTEGER)",
        "INSERT INTO s VALUES ('1')",
        "INSERT INTO s VALUES (2)",
        "CREATE TABLE t AS SELECT a FROM s",
    ],
    // Declared TEXT source column: TEXT affinity coerces integers to text.
    &[
        "CREATE TABLE s(a TEXT)",
        "INSERT INTO s VALUES ('1')",
        "INSERT INTO s VALUES (2)",
        "CREATE TABLE t AS SELECT a FROM s",
    ],
];

const PROBE: &str = "SELECT typeof(a) FROM t ORDER BY rowid";

fn oracle(script: &[&str]) -> Vec<String> {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    for s in script {
        conn.execute(s, []).unwrap();
    }
    conn.prepare(PROBE)
        .unwrap()
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

#[test]
fn ctas_column_affinity_matches_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        for script in SCRIPTS {
            let expected = oracle(script);
            let conn = Connection::open(":memory:").await.unwrap();
            for s in *script {
                conn.execute(s)
                    .await
                    .unwrap_or_else(|e| panic!("`{s}`: {e:?}"));
            }
            let rows = conn.query(PROBE).await.expect("probe");
            let got: Vec<String> = rows
                .iter()
                .map(|r| match r.values()[0] {
                    SqliteValue::Text(ref s) => s.as_ref().to_owned(),
                    ref other => panic!("typeof not text: {other:?}"),
                })
                .collect();
            assert_eq!(
                got, expected,
                "CTAS affinity for script {script:?} diverged from oracle"
            );
        }
    });
}
