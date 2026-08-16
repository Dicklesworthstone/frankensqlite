//! bd-rwaxp: a builtin scalar's NEEDCOLL (collation-consuming) requirement must
//! survive when a custom scalar registration shadows only SOME arities.
//!
//! `min`/`max` (2+ args) and `nullif/2` are builtin collation-consuming scalars.
//! Registering a custom `min/2` must NOT drop the builtin NEEDCOLL for `min/3`
//! (an arity the custom registration does not cover). fsqlite-vdbe codegen
//! previously did
//! `registry.scalar_consumes_argument_collation(name, n).unwrap_or(false)`, so
//! an uncovered arity silently lost the builtin requirement: an explicit
//! `COLLATE` on such a call was left opaque instead of resolved (and erroring on
//! a missing collation). The fix falls back to the builtin NEEDCOLL table when
//! the registry does not cover the (name, arity)
//! (`scalar_consumes_argument_collation_for_codegen`).
//!
//! This keeper isolates the flag behaviour without the separate LIMIT-0
//! constant-folding path: it uses a real table row (non-constant args, no
//! LIMIT), so the missing collation is resolved at prepare time and the query
//! fails closed — the custom scalar is never invoked (min/3 uses the builtin)
//! and no projection is constant-folded.

use fsqlite_core::connection::Connection;
use fsqlite_error::{FrankenError, Result};
use fsqlite_func::ScalarFunction;
use fsqlite_types::value::SqliteValue;

/// A collation-opaque custom `min/2` (the trait default
/// `consumes_argument_collation()` is `false`), registered to shadow only the
/// two-argument arity.
struct OpaqueMin2;

impl ScalarFunction for OpaqueMin2 {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        Ok(args.first().cloned().unwrap_or(SqliteValue::Null))
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &str {
        "min"
    }
}

#[test]
fn builtin_min_needcoll_survives_partial_custom_registration_uncovered_arity() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(a TEXT, b TEXT, c TEXT);")
            .await
            .unwrap();
        conn.execute("INSERT INTO t VALUES ('x', 'y', 'z');")
            .await
            .unwrap();

        // Custom scalar shadows only min/2. min/3 must still use the builtin,
        // which is NEEDCOLL: the explicit COLLATE selects the (missing)
        // collation, so the query must fail closed at prepare time.
        conn.register_deterministic_scalar_function(OpaqueMin2);

        let sql = "SELECT min(a COLLATE missing, b, c) FROM t;";
        let direct = conn
            .query(sql)
            .await
            .expect_err("builtin min/3 NEEDCOLL must resolve the (missing) argument collation");
        let prepared = match conn.prepare(sql).await {
            Ok(statement) => statement
                .query()
                .await
                .expect_err("prepared min/3 must retain builtin NEEDCOLL"),
            Err(error) => error,
        };
        for error in [direct, prepared] {
            assert!(
                matches!(
                    &error,
                    FrankenError::FunctionError(message)
                        if message == "no such collation sequence: missing"
                ),
                "expected a missing-collation error for builtin min/3, got {error:?}"
            );
        }

        // The COVERED arity (custom min/2) stays collation-opaque: the COLLATE
        // is ignored, no collation is resolved, and the query succeeds.
        let ok = conn
            .query("SELECT min(a COLLATE missing, b) FROM t;")
            .await
            .expect("custom min/2 is collation-opaque; COLLATE must be ignored");
        assert_eq!(ok.len(), 1);
    });
}
