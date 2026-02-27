//! Contract tests for B7 differential metadata schema + validator fixtures.
//!
//! Bead: bd-2yqp6.2.7

use fsqlite_harness::differential_v2::{
    self, DIFFERENTIAL_METADATA_SCHEMA_VERSION, DifferentialFirstFailure, DifferentialMetadata,
    DifferentialTiming, EngineIdentity, ExecutionEnvelope, NormalizedValue, SqlExecutor,
};
use proptest::prelude::{any, prop_assert, prop_assert_eq, proptest};

const BEAD_ID: &str = "bd-2yqp6.2.7";
const VALID_FIXTURE_JSON: &str = r#"{"schema_version":"1.0.0","trace_id":"d8ca3f7fa704b5f7f4f4234157c85ba1a4f2f43ecf704b36f8e19e1dcf0af3ee","run_id":"run-bd-2yqp6.2.7-0001","scenario_id":"DIFF-METADATA-FIXTURE-VALID","seed":4242,"oracle_identity":"csqlite-oracle","oracle_version":"3.52.0-test","fixture_manifest_hash":"a56fd3124bb2f3ce590f07f14e4f988614d607f29f1878ce5fa89f52f737c9cd","timing":{"total_ms":17},"normalized_outcome":"divergence","first_failure":{"statement_index":3,"sql":"SELECT * FROM t ORDER BY id"}}"#;

const INVALID_FIXTURES: [(&str, &str, &str); 5] = [
    (
        "empty-trace-id",
        r#"{"schema_version":"1.0.0","trace_id":"","run_id":"run-ok","scenario_id":"SCN-OK","seed":1,"oracle_identity":"csqlite-oracle","oracle_version":"3.52.0-test","fixture_manifest_hash":"a56fd3124bb2f3ce590f07f14e4f988614d607f29f1878ce5fa89f52f737c9cd","timing":{"total_ms":1},"normalized_outcome":"pass"}"#,
        "trace_id must be non-empty",
    ),
    (
        "schema-mismatch",
        r#"{"schema_version":"2.0.0","trace_id":"trace-ok","run_id":"run-ok","scenario_id":"SCN-OK","seed":1,"oracle_identity":"csqlite-oracle","oracle_version":"3.52.0-test","fixture_manifest_hash":"a56fd3124bb2f3ce590f07f14e4f988614d607f29f1878ce5fa89f52f737c9cd","timing":{"total_ms":1},"normalized_outcome":"pass"}"#,
        "schema mismatch",
    ),
    (
        "invalid-hash-format",
        r#"{"schema_version":"1.0.0","trace_id":"trace-ok","run_id":"run-ok","scenario_id":"SCN-OK","seed":1,"oracle_identity":"csqlite-oracle","oracle_version":"3.52.0-test","fixture_manifest_hash":"ABC123","timing":{"total_ms":1},"normalized_outcome":"pass"}"#,
        "fixture_manifest_hash must be 64 lowercase hex chars",
    ),
    (
        "invalid-outcome",
        r#"{"schema_version":"1.0.0","trace_id":"trace-ok","run_id":"run-ok","scenario_id":"SCN-OK","seed":1,"oracle_identity":"csqlite-oracle","oracle_version":"3.52.0-test","fixture_manifest_hash":"a56fd3124bb2f3ce590f07f14e4f988614d607f29f1878ce5fa89f52f737c9cd","timing":{"total_ms":1},"normalized_outcome":"unknown"}"#,
        "normalized_outcome must be pass|divergence|error",
    ),
    (
        "invalid-first-failure-sql",
        r#"{"schema_version":"1.0.0","trace_id":"trace-ok","run_id":"run-ok","scenario_id":"SCN-OK","seed":1,"oracle_identity":"csqlite-oracle","oracle_version":"3.52.0-test","fixture_manifest_hash":"a56fd3124bb2f3ce590f07f14e4f988614d607f29f1878ce5fa89f52f737c9cd","timing":{"total_ms":1},"normalized_outcome":"error","first_failure":{"statement_index":0,"sql":"   "}}"#,
        "first_failure.sql must be non-empty",
    ),
];

struct StaticExecutor {
    identity: EngineIdentity,
}

impl SqlExecutor for StaticExecutor {
    fn execute(&self, _sql: &str) -> Result<usize, String> {
        Ok(0)
    }

    fn query(&self, _sql: &str) -> Result<Vec<Vec<NormalizedValue>>, String> {
        Ok(vec![vec![NormalizedValue::Integer(1)]])
    }

    fn engine_identity(&self) -> EngineIdentity {
        self.identity
    }
}

#[test]
fn canonical_valid_fixture_decodes_strictly() {
    let metadata = DifferentialMetadata::from_json_strict(VALID_FIXTURE_JSON)
        .expect("canonical valid fixture must decode");
    assert!(metadata.validate().is_empty());
    assert_eq!(
        metadata.schema_version, DIFFERENTIAL_METADATA_SCHEMA_VERSION,
        "bead_id={BEAD_ID} schema drift in canonical fixture"
    );
    assert_eq!(
        metadata.to_canonical_json(),
        VALID_FIXTURE_JSON,
        "canonical fixture serialization ordering must stay deterministic"
    );
}

#[test]
fn canonical_invalid_fixtures_fail_deterministically() {
    for (fixture_id, json, expected_fragment) in INVALID_FIXTURES {
        let error = DifferentialMetadata::from_json_strict(json)
            .expect_err("invalid fixture should fail strict decode");
        assert!(
            error.contains(expected_fragment),
            "bead_id={BEAD_ID} fixture={fixture_id} expected error fragment `{expected_fragment}`, got `{error}`"
        );
    }
}

#[test]
fn schema_evolution_policy_requires_major_bump_notice() {
    let issues = differential_v2::differential_metadata_schema_evolution_issues("1.4.2", "2.0.0");
    assert_eq!(issues.len(), 1);
    assert!(issues[0].contains("backward-incompatible"));

    let no_issues =
        differential_v2::differential_metadata_schema_evolution_issues("1.4.2", "1.5.0");
    assert!(no_issues.is_empty());

    let parse_issues =
        differential_v2::differential_metadata_schema_evolution_issues("version-one", "2.0.0");
    assert_eq!(parse_issues.len(), 1);
    assert!(parse_issues[0].contains("unable to parse"));
}

#[test]
fn run_differential_emits_schema_compliant_metadata() {
    let envelope = ExecutionEnvelope::builder(7)
        .scenario_id("DIFF-METADATA-LIVE-CHECK")
        .engines("0.1.0-test", "3.52.0-test")
        .workload(["SELECT 1".to_owned()])
        .build();
    let subject = StaticExecutor {
        identity: EngineIdentity::FrankenSqlite,
    };
    let oracle = StaticExecutor {
        identity: EngineIdentity::CSqliteOracle,
    };

    let result = differential_v2::run_differential(&envelope, &subject, &oracle);
    assert!(
        result.metadata.validate().is_empty(),
        "bead_id={BEAD_ID} run metadata must satisfy validator"
    );
    assert_eq!(result.metadata.trace_id, envelope.artifact_id());
    assert_eq!(result.metadata.scenario_id, "DIFF-METADATA-LIVE-CHECK");
    assert_eq!(result.metadata.seed, envelope.seed);
    assert_eq!(
        result.metadata.normalized_outcome,
        result.outcome.to_string()
    );

    let canonical_json = result.metadata.to_canonical_json();
    let decoded = DifferentialMetadata::from_json_strict(&canonical_json)
        .expect("run metadata canonical json should decode strictly");
    assert_eq!(decoded, result.metadata);
}

proptest! {
    #[test]
    fn metadata_roundtrip_is_deterministic(
        seed in any::<u64>(),
        timing_ms in 0_u64..10_000_u64,
        statement_index in 0_usize..256_usize,
        include_failure in any::<bool>(),
    ) {
        let metadata = DifferentialMetadata {
            schema_version: DIFFERENTIAL_METADATA_SCHEMA_VERSION.to_owned(),
            trace_id: format!("trace-{seed:016x}"),
            run_id: format!("run-{seed:016x}"),
            scenario_id: format!("SCN-{seed:016x}"),
            seed,
            oracle_identity: "csqlite-oracle".to_owned(),
            oracle_version: "3.52.0-test".to_owned(),
            fixture_manifest_hash: format!("{seed:064x}"),
            timing: DifferentialTiming { total_ms: timing_ms },
            normalized_outcome: "pass".to_owned(),
            first_failure: if include_failure {
                Some(DifferentialFirstFailure {
                    statement_index,
                    sql: "SELECT 1".to_owned(),
                })
            } else {
                None
            },
        };

        let validation_errors = metadata.validate();
        prop_assert!(validation_errors.is_empty(), "metadata validation failed: {:?}", validation_errors);

        let canonical = metadata.to_canonical_json();
        let decoded = DifferentialMetadata::from_json_strict(&canonical)
            .expect("strict decode should pass for generated metadata");
        prop_assert_eq!(&decoded, &metadata);
        prop_assert_eq!(decoded.to_canonical_json(), canonical);
    }
}
