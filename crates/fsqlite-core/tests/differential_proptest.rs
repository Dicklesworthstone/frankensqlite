//! frankensqlite#86 — superseded property-based differential scaffold.
//!
//! This integration target is intentionally inert. The independent, typed,
//! schema-aware generator now belongs to
//! `fsqlite_harness::typed_sql_generator` under
//! `bd-turso-test-adaptation-zu081.3`. That replacement records deterministic
//! seed lineage and never treats arbitrary paired engine errors as agreement.
//! The file remains in place because repository policy forbids deleting files
//! without explicit user permission.
#![cfg(any())]
//!
//! Builds on the existing `oracle_compare` pattern (see
//! `conformance_oracle_*.rs`) by generating random valid SELECT
//! statements via `proptest` and asserting that frankensqlite and
//! stock SQLite (via rusqlite) agree on the result rows.
//!
//! This is a SCAFFOLD: it covers a narrow grammar (single-table
//! SELECT with WHERE/ORDER BY/LIMIT) and a small numeric/text dataset
//! to keep tests fast (the proptest config below caps this at 64 cases per
//! property). It's deliberately not trying to subsume the existing
//! conformance oracles — those exercise specific code paths (joins,
//! aggregates, window functions, DML) that this grammar doesn't
//! reach. The point here is to demonstrate the property-based
//! generator pattern so future tests can extend the grammar without
//! rebuilding the harness.
//!
//! The generators bias toward boundary values (NULL, 0, empty
//! string, signed-int extremes) because most disagreements between
//! engines manifest at boundaries.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;
use proptest::prelude::*;

/// Bounded test dataset. Small enough to keep proptest cases cheap;
/// boundary-heavy because that's where engines disagree.
const FIXTURE_ROWS: &[(i64, &str, Option<i64>)] = &[
    (1, "alpha", Some(0)),
    (2, "beta", Some(1)),
    (3, "", Some(i64::MIN)),
    (4, "gamma", None),
    (5, "delta", Some(i64::MAX)),
    (6, "", Some(-1)),
    (7, "epsilon", Some(0)),
    (8, "zeta", None),
];

async fn setup_pair() -> (Connection, rusqlite::Connection) {
    let fconn = Connection::open(":memory:")
        .await
        .expect("open frankensqlite");
    let rconn = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    let ddl = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL, val INTEGER)";
    fconn.execute(ddl).await.expect("frankensqlite ddl");
    rconn.execute(ddl, []).expect("rusqlite ddl");
    for (id, name, val) in FIXTURE_ROWS {
        let insert = match val {
            Some(v) => format!("INSERT INTO t VALUES ({id}, '{name}', {v})"),
            None => format!("INSERT INTO t VALUES ({id}, '{name}', NULL)"),
        };
        fconn.execute(&insert).await.expect("frankensqlite insert");
        rconn.execute(&insert, []).expect("rusqlite insert");
    }
    (fconn, rconn)
}

/// Compare results across the two engines as opaque `String` rows.
/// We canonicalize each value via the engine's debug-Display so we
/// don't accidentally hide a real disagreement behind divergent
/// formatting.
///
/// Both-engines-error is treated as agreement, not failure: the
/// grammar is intentionally narrow so spurious parse errors are
/// unlikely, but if a query is rejected by both engines that is the
/// same observable result. Only an asymmetric error (one engine
/// accepts, the other rejects) or differing row contents constitute
/// a real disagreement.
async fn rows_match(fconn: &Connection, rconn: &rusqlite::Connection, query: &str) -> bool {
    match (fconn.query(query).await, rconn.prepare(query)) {
        // Both engines agree on rejection — same observable result.
        (Err(_), Err(_)) => true,
        // Asymmetric rejection: real disagreement.
        (Err(_), Ok(_)) | (Ok(_), Err(_)) => false,
        (Ok(frank_result), Ok(mut stmt)) => {
            let frank_rows = frank_result
                .iter()
                .map(|row| {
                    row.values()
                        .iter()
                        .map(format_franken_value)
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();

            let col_count = stmt.column_count();
            let rusq_rows: Vec<Vec<String>> = match stmt.query_map([], |row| {
                let mut vals = Vec::with_capacity(col_count);
                for i in 0..col_count {
                    let v: rusqlite::types::Value = row.get_unwrap(i);
                    vals.push(format_rusq_value(&v));
                }
                Ok(vals)
            }) {
                Ok(it) => match it.collect::<Result<Vec<_>, _>>() {
                    Ok(rs) => rs,
                    Err(_) => return false,
                },
                Err(_) => return false,
            };
            frank_rows == rusq_rows
        }
    }
}

fn format_franken_value(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

fn format_rusq_value(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

// =============================================================================
// Generators
// =============================================================================

/// Biased integer generator: 80% of cases pick a boundary value
/// (zero, min, max, ±1, ±2) and 20% pick a uniformly-random value.
/// Boundary bias is the dominant signal for engine disagreements.
fn biased_int() -> impl Strategy<Value = i64> {
    let boundary = prop_oneof![
        Just(0_i64),
        Just(1),
        Just(-1),
        Just(2),
        Just(-2),
        Just(i64::MAX),
        Just(i64::MIN),
        Just(i64::MAX - 1),
        Just(i64::MIN + 1),
    ];
    prop_oneof![8 => boundary, 2 => any::<i64>()]
}

/// Bounded text generator. Mostly empty-string, single-char, and
/// short ASCII at the boundaries.
fn biased_text() -> impl Strategy<Value = String> {
    let boundary = prop_oneof![
        Just(String::new()),
        Just("a".to_owned()),
        Just("Z".to_owned()),
        Just("alpha".to_owned()),
        Just("beta".to_owned()),
    ];
    prop_oneof![8 => boundary, 2 => "[a-zA-Z]{0,20}".prop_map(String::from)]
}

#[derive(Debug, Clone)]
enum WhereCmp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl WhereCmp {
    fn sql(&self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::Ne => "<>",
            Self::Lt => "<",
            Self::Le => "<=",
            Self::Gt => ">",
            Self::Ge => ">=",
        }
    }
}

fn cmp_strategy() -> impl Strategy<Value = WhereCmp> {
    prop_oneof![
        Just(WhereCmp::Eq),
        Just(WhereCmp::Ne),
        Just(WhereCmp::Lt),
        Just(WhereCmp::Le),
        Just(WhereCmp::Gt),
        Just(WhereCmp::Ge),
    ]
}

// =============================================================================
// Properties
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 64,                   // keep the test fast; loop already covers a lot of boundaries.
        max_shrink_iters: 256,
        failure_persistence: None,    // integration-test path layout confuses SourceParallel persistence.
        .. ProptestConfig::default()
    })]

    /// `SELECT * FROM t WHERE id <cmp> :val [ORDER BY id [DESC]] LIMIT N`
    /// must agree byte-for-byte (after our shared canonicalization)
    /// between frankensqlite and stock SQLite.
    #[test]
    fn select_id_cmp_int(
        cmp in cmp_strategy(),
        val in biased_int(),
        desc in any::<bool>(),
        limit in 0u32..=8u32,
    ) {
        let order = if desc { "DESC" } else { "ASC" };
        let query = format!(
            "SELECT id, name, val FROM t WHERE id {} {} ORDER BY id {} LIMIT {}",
            cmp.sql(), val, order, limit
        );
        let mut matched = false;
        asupersync::test_utils::run_test(|| async {
            let (fconn, rconn) = setup_pair().await;
            matched = rows_match(&fconn, &rconn, &query).await;
        });
        prop_assert!(
            matched,
            "query disagreement: {query}"
        );
    }

    /// `SELECT * FROM t WHERE name <cmp> :str [ORDER BY id] LIMIT N`
    /// agreement check. Text comparison is the cmp variant.
    #[test]
    fn select_name_cmp_text(
        cmp in cmp_strategy(),
        s in biased_text(),
        limit in 0u32..=8u32,
    ) {
        // Skip strings with embedded quotes — that would need
        // parameterized binds, which the harness doesn't yet thread.
        prop_assume!(!s.contains('\''));
        let query = format!(
            "SELECT id, name, val FROM t WHERE name {} '{}' ORDER BY id ASC LIMIT {}",
            cmp.sql(), s, limit
        );
        let mut matched = false;
        asupersync::test_utils::run_test(|| async {
            let (fconn, rconn) = setup_pair().await;
            matched = rows_match(&fconn, &rconn, &query).await;
        });
        prop_assert!(
            matched,
            "query disagreement: {query}"
        );
    }

    /// `SELECT val FROM t WHERE val IS NULL` and `IS NOT NULL`
    /// must agree. Null handling is the most common source of
    /// engine disagreements at the SQL surface.
    #[test]
    fn select_null_handling(want_null in any::<bool>()) {
        let predicate = if want_null { "IS NULL" } else { "IS NOT NULL" };
        let query = format!("SELECT id, val FROM t WHERE val {predicate} ORDER BY id ASC");
        let mut matched = false;
        asupersync::test_utils::run_test(|| async {
            let (fconn, rconn) = setup_pair().await;
            matched = rows_match(&fconn, &rconn, &query).await;
        });
        prop_assert!(
            matched,
            "null-handling disagreement: {query}"
        );
    }
}
