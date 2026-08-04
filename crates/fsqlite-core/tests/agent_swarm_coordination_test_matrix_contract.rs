use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const HEAVY_GATE_PREFIX: &str = "timeout 900 rch exec -- env CARGO_TARGET_DIR=/data/tmp/frankensqlite-target-agent-swarm-matrix";

const LAYER_UNIT: u8 = 1 << 0;
const LAYER_PROPERTY: u8 = 1 << 1;
const LAYER_GOLDEN: u8 = 1 << 2;
const LAYER_REGRESSION: u8 = 1 << 3;

#[derive(Clone, Copy, Default)]
struct CoverageFlags(u8);

impl CoverageFlags {
    const HAPPY: Self = Self(1 << 0);
    const EMPTY: Self = Self(1 << 1);
    const BOUNDARY: Self = Self(1 << 2);
    const ROLLBACK: Self = Self(1 << 3);
    const ERROR: Self = Self(1 << 4);
    const ALL: Self =
        Self(Self::HAPPY.0 | Self::EMPTY.0 | Self::BOUNDARY.0 | Self::ROLLBACK.0 | Self::ERROR.0);
    const WITHOUT_ROLLBACK: Self =
        Self(Self::HAPPY.0 | Self::EMPTY.0 | Self::BOUNDARY.0 | Self::ERROR.0);

    const fn contains(self, flag: Self) -> bool {
        self.0 & flag.0 != 0
    }

    const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

#[derive(Clone, Copy)]
struct TestMatrixRow<'a> {
    invariant_id: &'a str,
    surface: &'a str,
    source_test: &'a str,
    test_layer: &'a str,
    deterministic_seed: &'a str,
    coverage: CoverageFlags,
    property_obligation: &'a str,
    golden_artifact: &'a str,
    regression_name: &'a str,
    heavy_rch_command: &'a str,
    first_failure_diag: &'a str,
}

struct GoldenRow<'a> {
    golden_name: &'a str,
    surface: &'a str,
    source_test: &'a str,
    update_policy: &'a str,
    scrubbers: &'a str,
    canonical_row_json: &'a str,
}

#[derive(Default)]
struct SurfaceCoverage {
    layers: u8,
    coverage: CoverageFlags,
}

#[derive(Clone, Copy)]
struct QueueAttempt {
    attempt_seq: u64,
    worker_id: &'static str,
}

#[derive(Clone, Copy)]
struct LeaseAttempt {
    worker_id: &'static str,
    observed_now_ms: i64,
    previous_expires_ms: i64,
}

#[derive(Clone, Copy)]
struct RangeAttempt {
    range_id: &'static str,
    start_key: i64,
    end_key: i64,
}

fn sql_text(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn sql_bool(value: bool) -> &'static str {
    if value { "1" } else { "0" }
}

fn row_values(row: &Row) -> Vec<SqliteValue> {
    row.values().to_vec()
}

fn rows_to_values(rows: &[Row]) -> Vec<Vec<SqliteValue>> {
    rows.iter().map(row_values).collect()
}

fn sqlite_text(value: &SqliteValue) -> TestResult<&str> {
    match value {
        SqliteValue::Text(text) => Ok(text.as_str()),
        other => Err(format!("expected text value, got {other:?}").into()),
    }
}

fn sqlite_bool(value: &SqliteValue) -> TestResult<bool> {
    match value {
        SqliteValue::Integer(0) => Ok(false),
        SqliteValue::Integer(1) => Ok(true),
        other => Err(format!("expected boolean integer value, got {other:?}").into()),
    }
}

fn has_layer(layers: u8, layer: u8) -> bool {
    layers & layer != 0
}

fn coverage_flags_from_values(values: &[SqliteValue]) -> TestResult<CoverageFlags> {
    let mut flags = CoverageFlags::default();
    for (index, flag) in [
        (2, CoverageFlags::HAPPY),
        (3, CoverageFlags::EMPTY),
        (4, CoverageFlags::BOUNDARY),
        (5, CoverageFlags::ROLLBACK),
        (6, CoverageFlags::ERROR),
    ] {
        if sqlite_bool(&values[index])? {
            flags = flags.union(flag);
        }
    }
    Ok(flags)
}

async fn install_test_matrix_schema(conn: &Connection) -> TestResult {
    conn.execute(
        "CREATE TABLE fsqlite_coordination_test_matrix_contract(
            invariant_id TEXT NOT NULL PRIMARY KEY,
            surface TEXT NOT NULL,
            source_test TEXT NOT NULL,
            test_layer TEXT NOT NULL,
            deterministic_seed TEXT NOT NULL,
            covers_happy INTEGER NOT NULL,
            covers_empty INTEGER NOT NULL,
            covers_boundary INTEGER NOT NULL,
            covers_rollback INTEGER NOT NULL,
            covers_error INTEGER NOT NULL,
            property_obligation TEXT NOT NULL,
            golden_artifact TEXT NOT NULL,
            regression_name TEXT NOT NULL,
            heavy_rch_command TEXT NOT NULL,
            first_failure_diag TEXT NOT NULL
        );",
    )
    .await?;
    conn.execute(
        "CREATE TABLE fsqlite_coordination_golden_rows_contract(
            golden_name TEXT NOT NULL PRIMARY KEY,
            surface TEXT NOT NULL,
            source_test TEXT NOT NULL,
            update_policy TEXT NOT NULL,
            scrubbers TEXT NOT NULL,
            canonical_row_json TEXT NOT NULL
        );",
    )
    .await?;
    seed_matrix_rows(conn).await?;
    seed_golden_rows(conn).await?;
    Ok(())
}

async fn seed_matrix_rows(conn: &Connection) -> TestResult {
    let rows = [
        TestMatrixRow {
            invariant_id: "queue.unit.claim-release",
            surface: "queue",
            source_test: "agent_swarm_queue_claim_contract",
            test_layer: "unit",
            deterministic_seed: "queue-seed-0001",
            coverage: CoverageFlags::ALL,
            property_obligation: "none",
            golden_artifact: "none",
            regression_name: "queue.no_double_claim",
            heavy_rch_command: concat!(
                "timeout 900 rch exec -- env CARGO_TARGET_DIR=/data/tmp/frankensqlite-target-agent-swarm-matrix ",
                "cargo test -p fsqlite-core --test agent_swarm_queue_claim_contract -- --nocapture"
            ),
            first_failure_diag: "queue claim transition lost owner or generation",
        },
        TestMatrixRow {
            invariant_id: "queue.property.generated-interleavings",
            surface: "queue",
            source_test: "agent_swarm_coordination_test_matrix_contract",
            test_layer: "property",
            deterministic_seed: "queue-generated-0000..0031",
            coverage: CoverageFlags::WITHOUT_ROLLBACK,
            property_obligation: "no_double_claim",
            golden_artifact: "none",
            regression_name: "queue.generated_no_double_claim",
            heavy_rch_command: concat!(
                "timeout 900 rch exec -- env CARGO_TARGET_DIR=/data/tmp/frankensqlite-target-agent-swarm-matrix ",
                "cargo test -p fsqlite-core --test agent_swarm_coordination_test_matrix_contract -- generated_interleavings -- --nocapture"
            ),
            first_failure_diag: "generated queue schedule accepted more than one owner",
        },
        TestMatrixRow {
            invariant_id: "lease.unit.lifecycle",
            surface: "lease",
            source_test: "agent_swarm_lease_contract",
            test_layer: "unit",
            deterministic_seed: "lease-seed-0001",
            coverage: CoverageFlags::ALL,
            property_obligation: "none",
            golden_artifact: "none",
            regression_name: "lease.no_double_active_owner",
            heavy_rch_command: concat!(
                "timeout 900 rch exec -- env CARGO_TARGET_DIR=/data/tmp/frankensqlite-target-agent-swarm-matrix ",
                "cargo test -p fsqlite-core --test agent_swarm_lease_contract -- --nocapture"
            ),
            first_failure_diag: "lease owner-token-generation contract drifted",
        },
        TestMatrixRow {
            invariant_id: "lease.property.generated-expiration",
            surface: "lease",
            source_test: "agent_swarm_coordination_test_matrix_contract",
            test_layer: "property",
            deterministic_seed: "lease-generated-0000..0031",
            coverage: CoverageFlags::WITHOUT_ROLLBACK,
            property_obligation: "no_double_lease",
            golden_artifact: "none",
            regression_name: "lease.generated_no_double_lease",
            heavy_rch_command: concat!(
                "timeout 900 rch exec -- env CARGO_TARGET_DIR=/data/tmp/frankensqlite-target-agent-swarm-matrix ",
                "cargo test -p fsqlite-core --test agent_swarm_coordination_test_matrix_contract -- generated_interleavings -- --nocapture"
            ),
            first_failure_diag: "generated lease schedule accepted overlapping active owners",
        },
        TestMatrixRow {
            invariant_id: "range.unit.split-merge",
            surface: "range",
            source_test: "agent_swarm_worker_range_contract",
            test_layer: "unit",
            deterministic_seed: "range-seed-0001",
            coverage: CoverageFlags::ALL,
            property_obligation: "none",
            golden_artifact: "none",
            regression_name: "range.no_overlap_after_split_merge",
            heavy_rch_command: concat!(
                "timeout 900 rch exec -- env CARGO_TARGET_DIR=/data/tmp/frankensqlite-target-agent-swarm-matrix ",
                "cargo test -p fsqlite-core --test agent_swarm_worker_range_contract -- --nocapture"
            ),
            first_failure_diag: "worker range allocation overlapped or lost metadata",
        },
        TestMatrixRow {
            invariant_id: "range.property.generated-overlap",
            surface: "range",
            source_test: "agent_swarm_coordination_test_matrix_contract",
            test_layer: "property",
            deterministic_seed: "range-generated-0000..0031",
            coverage: CoverageFlags::WITHOUT_ROLLBACK,
            property_obligation: "no_overlapping_enforced_ranges",
            golden_artifact: "none",
            regression_name: "range.generated_no_overlap",
            heavy_rch_command: concat!(
                "timeout 900 rch exec -- env CARGO_TARGET_DIR=/data/tmp/frankensqlite-target-agent-swarm-matrix ",
                "cargo test -p fsqlite-core --test agent_swarm_coordination_test_matrix_contract -- generated_interleavings -- --nocapture"
            ),
            first_failure_diag: "generated range schedule accepted overlapping active ranges",
        },
        TestMatrixRow {
            invariant_id: "explain.unit.reason-codes",
            surface: "explain",
            source_test: "agent_swarm_explain_concurrency_contract",
            test_layer: "unit",
            deterministic_seed: "explain-seed-0001",
            coverage: CoverageFlags::ALL,
            property_obligation: "none",
            golden_artifact: "none",
            regression_name: "explain.reason_codes_stable",
            heavy_rch_command: concat!(
                "timeout 900 rch exec -- env CARGO_TARGET_DIR=/data/tmp/frankensqlite-target-agent-swarm-matrix ",
                "cargo test -p fsqlite-core --test agent_swarm_explain_concurrency_contract -- --nocapture"
            ),
            first_failure_diag: "concurrency diagnostic reason-code contract drifted",
        },
        TestMatrixRow {
            invariant_id: "explain.golden.canonical-row",
            surface: "explain",
            source_test: "agent_swarm_coordination_test_matrix_contract",
            test_layer: "golden",
            deterministic_seed: "golden-explain-0001",
            coverage: CoverageFlags::ALL,
            property_obligation: "none",
            golden_artifact: "explain_concurrency_hot_page_row",
            regression_name: "explain.hot_page_row_shape",
            heavy_rch_command: concat!(
                "timeout 900 rch exec -- env CARGO_TARGET_DIR=/data/tmp/frankensqlite-target-agent-swarm-matrix ",
                "cargo test -p fsqlite-core --test agent_swarm_coordination_test_matrix_contract -- canonical_golden_rows -- --nocapture"
            ),
            first_failure_diag: "canonical EXPLAIN CONCURRENCY row changed without review",
        },
        TestMatrixRow {
            invariant_id: "fallback.unit.reason-codes",
            surface: "fallback",
            source_test: "agent_swarm_fallback_transparency_contract",
            test_layer: "unit",
            deterministic_seed: "fallback-seed-0001",
            coverage: CoverageFlags::ALL,
            property_obligation: "none",
            golden_artifact: "none",
            regression_name: "fallback.reason_codes_stable",
            heavy_rch_command: concat!(
                "timeout 900 rch exec -- env CARGO_TARGET_DIR=/data/tmp/frankensqlite-target-agent-swarm-matrix ",
                "cargo test -p fsqlite-core --test agent_swarm_fallback_transparency_contract -- --nocapture"
            ),
            first_failure_diag: "fallback transparency reason-code contract drifted",
        },
        TestMatrixRow {
            invariant_id: "fallback.golden.canonical-row",
            surface: "fallback",
            source_test: "agent_swarm_coordination_test_matrix_contract",
            test_layer: "golden",
            deterministic_seed: "golden-fallback-0001",
            coverage: CoverageFlags::ALL,
            property_obligation: "none",
            golden_artifact: "fallback_compatibility_row",
            regression_name: "fallback.compatibility_row_shape",
            heavy_rch_command: concat!(
                "timeout 900 rch exec -- env CARGO_TARGET_DIR=/data/tmp/frankensqlite-target-agent-swarm-matrix ",
                "cargo test -p fsqlite-core --test agent_swarm_coordination_test_matrix_contract -- canonical_golden_rows -- --nocapture"
            ),
            first_failure_diag: "canonical fallback row changed without review",
        },
    ];

    for row in rows {
        insert_matrix_row(conn, row).await?;
    }
    Ok(())
}

async fn seed_golden_rows(conn: &Connection) -> TestResult {
    let rows = [
        GoldenRow {
            golden_name: "explain_concurrency_hot_page_row",
            surface: "explain",
            source_test: "agent_swarm_explain_concurrency_contract",
            update_policy: "intentional-change-only-review",
            scrubbers: "none-required-static-contract-row",
            canonical_row_json: concat!(
                "{\"abort_count\":1,",
                "\"busy_family\":\"busy_snapshot\",",
                "\"conflict_reason\":\"hot_page_predicted\",",
                "\"coordination_strategy\":\"page_mvcc\",",
                "\"diagnostics_available\":true,",
                "\"external_wait\":null,",
                "\"fallback_reason\":null,",
                "\"hotspot_kind\":\"page\",",
                "\"page_number\":47,",
                "\"plan_id\":\"plan-update-leaf-47\",",
                "\"retry_count\":1,",
                "\"statement_fingerprint\":\"fp-hot-page-update\",",
                "\"suggested_next_inspection\":\"inspect_page_heat\",",
                "\"table_name\":\"jobs\"}"
            ),
        },
        GoldenRow {
            golden_name: "fallback_compatibility_row",
            surface: "fallback",
            source_test: "agent_swarm_fallback_transparency_contract",
            update_policy: "intentional-change-only-review",
            scrubbers: "none-required-static-contract-row",
            canonical_row_json: concat!(
                "{\"concurrency_impact\":\"may_reduce_parallelism\",",
                "\"diagnostics_available\":true,",
                "\"fallback_reason\":\"compatibility_fallback\",",
                "\"fallback_surface\":\"vdbe_compatibility\",",
                "\"first_failure_diag\":\"window frame still uses compatibility path\",",
                "\"impact_class\":\"latency\",",
                "\"plan_id\":\"plan-window-compat\",",
                "\"statement_fingerprint\":\"fp-window-fallback\",",
                "\"supported_fast_path\":false,",
                "\"table_name\":\"events\"}"
            ),
        },
    ];

    for row in rows {
        insert_golden_row(conn, row).await?;
    }
    Ok(())
}

async fn insert_matrix_row(conn: &Connection, row: TestMatrixRow<'_>) -> TestResult {
    conn.execute(&format!(
        "INSERT INTO fsqlite_coordination_test_matrix_contract(
            invariant_id,
            surface,
            source_test,
            test_layer,
            deterministic_seed,
            covers_happy,
            covers_empty,
            covers_boundary,
            covers_rollback,
            covers_error,
            property_obligation,
            golden_artifact,
            regression_name,
            heavy_rch_command,
            first_failure_diag
        ) VALUES (
            {invariant_id},
            {surface},
            {source_test},
            {test_layer},
            {deterministic_seed},
            {covers_happy},
            {covers_empty},
            {covers_boundary},
            {covers_rollback},
            {covers_error},
            {property_obligation},
            {golden_artifact},
            {regression_name},
            {heavy_rch_command},
            {first_failure_diag}
        );",
        invariant_id = sql_text(row.invariant_id),
        surface = sql_text(row.surface),
        source_test = sql_text(row.source_test),
        test_layer = sql_text(row.test_layer),
        deterministic_seed = sql_text(row.deterministic_seed),
        covers_happy = sql_bool(row.coverage.contains(CoverageFlags::HAPPY)),
        covers_empty = sql_bool(row.coverage.contains(CoverageFlags::EMPTY)),
        covers_boundary = sql_bool(row.coverage.contains(CoverageFlags::BOUNDARY)),
        covers_rollback = sql_bool(row.coverage.contains(CoverageFlags::ROLLBACK)),
        covers_error = sql_bool(row.coverage.contains(CoverageFlags::ERROR)),
        property_obligation = sql_text(row.property_obligation),
        golden_artifact = sql_text(row.golden_artifact),
        regression_name = sql_text(row.regression_name),
        heavy_rch_command = sql_text(row.heavy_rch_command),
        first_failure_diag = sql_text(row.first_failure_diag),
    ))
    .await?;
    Ok(())
}

async fn insert_golden_row(conn: &Connection, row: GoldenRow<'_>) -> TestResult {
    conn.execute(&format!(
        "INSERT INTO fsqlite_coordination_golden_rows_contract(
            golden_name,
            surface,
            source_test,
            update_policy,
            scrubbers,
            canonical_row_json
        ) VALUES (
            {golden_name},
            {surface},
            {source_test},
            {update_policy},
            {scrubbers},
            {canonical_row_json}
        );",
        golden_name = sql_text(row.golden_name),
        surface = sql_text(row.surface),
        source_test = sql_text(row.source_test),
        update_policy = sql_text(row.update_policy),
        scrubbers = sql_text(row.scrubbers),
        canonical_row_json = sql_text(row.canonical_row_json),
    ))
    .await?;
    Ok(())
}

async fn coverage_by_surface(conn: &Connection) -> TestResult<Vec<(String, SurfaceCoverage)>> {
    let rows = conn
        .query(
            "SELECT surface,
                test_layer,
                covers_happy,
                covers_empty,
                covers_boundary,
                covers_rollback,
                covers_error
           FROM fsqlite_coordination_test_matrix_contract
          ORDER BY surface, invariant_id;",
        )
        .await?;

    let mut coverage = Vec::<(String, SurfaceCoverage)>::new();
    for row in rows {
        let values = row_values(&row);
        let surface = sqlite_text(&values[0])?;
        let layer = sqlite_text(&values[1])?;

        let (_, current) = match coverage.iter_mut().find(|(name, _)| name == surface) {
            Some(existing) => existing,
            None => {
                coverage.push((surface.to_owned(), SurfaceCoverage::default()));
                coverage
                    .last_mut()
                    .ok_or("coverage vector unexpectedly empty after push")?
            }
        };

        current.layers |= match layer {
            "unit" => LAYER_UNIT | LAYER_REGRESSION,
            "property" => LAYER_PROPERTY | LAYER_REGRESSION,
            "golden" => LAYER_GOLDEN | LAYER_REGRESSION,
            _ => 0,
        };
        current.coverage = current.coverage.union(coverage_flags_from_values(&values)?);
    }
    Ok(coverage)
}

fn generated_queue_attempts(seed: u64) -> Vec<QueueAttempt> {
    let workers = ["worker-a", "worker-b", "worker-c", "worker-d"];
    workers
        .iter()
        .enumerate()
        .map(|(offset, worker_id)| QueueAttempt {
            attempt_seq: ((seed + offset as u64 * 7) % 19) + offset as u64,
            worker_id,
        })
        .collect()
}

fn accepted_queue_owner(attempts: &[QueueAttempt]) -> Option<&'static str> {
    attempts
        .iter()
        .min_by_key(|attempt| attempt.attempt_seq)
        .map(|attempt| attempt.worker_id)
}

fn generated_lease_attempts(seed: u64) -> Vec<LeaseAttempt> {
    let base = i64::try_from(seed).unwrap_or(0) * 11;
    vec![
        LeaseAttempt {
            worker_id: "worker-a",
            observed_now_ms: base,
            previous_expires_ms: base - 1,
        },
        LeaseAttempt {
            worker_id: "worker-b",
            observed_now_ms: base + 2,
            previous_expires_ms: base + 50,
        },
        LeaseAttempt {
            worker_id: "worker-c",
            observed_now_ms: base + 60,
            previous_expires_ms: base + 50,
        },
    ]
}

fn accepted_lease_owners(attempts: &[LeaseAttempt]) -> Vec<&'static str> {
    let mut active_owner = None;
    let mut active_expires_ms = i64::MIN;
    let mut accepted = Vec::new();

    for attempt in attempts {
        if active_owner.is_none() || attempt.observed_now_ms >= active_expires_ms {
            active_owner = Some(attempt.worker_id);
            active_expires_ms = attempt
                .previous_expires_ms
                .max(attempt.observed_now_ms + 25);
            accepted.push(attempt.worker_id);
        }
    }

    accepted
}

fn generated_range_attempts(seed: u64) -> Vec<RangeAttempt> {
    let base = i64::try_from(seed % 9).unwrap_or(0) * 10;
    vec![
        RangeAttempt {
            range_id: "range-a",
            start_key: base,
            end_key: base + 9,
        },
        RangeAttempt {
            range_id: "range-b",
            start_key: base + 5,
            end_key: base + 12,
        },
        RangeAttempt {
            range_id: "range-c",
            start_key: base + 10,
            end_key: base + 19,
        },
    ]
}

fn ranges_overlap(left: RangeAttempt, right: RangeAttempt) -> bool {
    left.start_key <= right.end_key && right.start_key <= left.end_key
}

fn accepted_ranges(attempts: &[RangeAttempt]) -> Vec<RangeAttempt> {
    let mut accepted = Vec::new();
    for attempt in attempts {
        if accepted
            .iter()
            .all(|existing| !ranges_overlap(*existing, *attempt))
        {
            accepted.push(*attempt);
        }
    }
    accepted
}

#[test]
fn matrix_has_required_fast_layers_and_exact_rch_commands() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let conn = Connection::open(":memory:").await?;
            install_test_matrix_schema(&conn).await?;

            let coverage = coverage_by_surface(&conn).await?;
            assert_eq!(coverage.len(), 5);

            for (surface, current) in coverage {
                assert!(
                    has_layer(current.layers, LAYER_UNIT),
                    "{surface} must have unit contract coverage"
                );
                assert!(
                    current.coverage.contains(CoverageFlags::HAPPY),
                    "{surface} must cover happy paths"
                );
                assert!(
                    current.coverage.contains(CoverageFlags::EMPTY),
                    "{surface} must cover empty input"
                );
                assert!(
                    current.coverage.contains(CoverageFlags::BOUNDARY),
                    "{surface} must cover boundary cases"
                );
                assert!(
                    current.coverage.contains(CoverageFlags::ROLLBACK),
                    "{surface} must cover rollback semantics"
                );
                assert!(
                    current.coverage.contains(CoverageFlags::ERROR),
                    "{surface} must cover error conditions"
                );
                assert!(
                    has_layer(current.layers, LAYER_REGRESSION),
                    "{surface} must have a named regression guard"
                );
                if matches!(surface.as_str(), "queue" | "lease" | "range") {
                    assert!(
                        has_layer(current.layers, LAYER_PROPERTY),
                        "{surface} must have deterministic interleaving coverage"
                    );
                }
                if matches!(surface.as_str(), "explain" | "fallback") {
                    assert!(
                        has_layer(current.layers, LAYER_GOLDEN),
                        "{surface} must have canonical golden row coverage"
                    );
                }
            }

            let command_rows = conn
                .query(
                    "SELECT invariant_id, heavy_rch_command
           FROM fsqlite_coordination_test_matrix_contract
          ORDER BY invariant_id;",
                )
                .await?;
            for row in command_rows {
                let values = row_values(&row);
                let invariant_id = sqlite_text(&values[0])?;
                let command = sqlite_text(&values[1])?;
                assert!(
                    command.starts_with(HEAVY_GATE_PREFIX),
                    "{invariant_id} must use the repo's foreground rch command shape"
                );
                assert!(
                    !command.starts_with("cargo "),
                    "{invariant_id} must not document a local heavy cargo gate"
                );
            }

            Ok::<(), Box<dyn std::error::Error>>(())
        }
        .await;
    });
    outcome
}

#[test]
fn generated_interleavings_preserve_single_owner_properties() {
    for seed in 0..32 {
        let queue_attempts = generated_queue_attempts(seed);
        let owner = accepted_queue_owner(&queue_attempts);
        assert!(owner.is_some(), "seed {seed} must accept one queue owner");
        let second_owner = queue_attempts
            .iter()
            .filter(|attempt| Some(attempt.worker_id) != owner)
            .find(|attempt| {
                attempt.attempt_seq
                    > queue_attempts
                        .iter()
                        .map(|candidate| candidate.attempt_seq)
                        .min()
                        .unwrap_or(0)
            });
        assert!(
            second_owner.is_some(),
            "seed {seed} must include rejected queue contenders"
        );

        let lease_attempts = generated_lease_attempts(seed);
        let accepted_leases = accepted_lease_owners(&lease_attempts);
        assert!(
            accepted_leases.len() <= 2,
            "seed {seed} accepted overlapping active leases"
        );

        let range_attempts = generated_range_attempts(seed);
        let accepted = accepted_ranges(&range_attempts);
        for left in &accepted {
            for right in &accepted {
                if left.range_id != right.range_id {
                    assert!(
                        !ranges_overlap(*left, *right),
                        "seed {seed} accepted overlapping ranges {} and {}",
                        left.range_id,
                        right.range_id
                    );
                }
            }
        }
    }
}

#[test]
fn canonical_golden_rows_are_exact_and_review_gated() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let conn = Connection::open(":memory:").await?;
            install_test_matrix_schema(&conn).await?;

            let rows = conn
                .query(
                    "SELECT golden_name,
                surface,
                update_policy,
                scrubbers,
                canonical_row_json
           FROM fsqlite_coordination_golden_rows_contract
          ORDER BY golden_name;",
                )
                .await?;

            assert_eq!(
                rows_to_values(&rows),
                vec![
                    vec![
                        SqliteValue::Text("explain_concurrency_hot_page_row".into()),
                        SqliteValue::Text("explain".into()),
                        SqliteValue::Text("intentional-change-only-review".into()),
                        SqliteValue::Text("none-required-static-contract-row".into()),
                        SqliteValue::Text(
                            concat!(
                                "{\"abort_count\":1,",
                                "\"busy_family\":\"busy_snapshot\",",
                                "\"conflict_reason\":\"hot_page_predicted\",",
                                "\"coordination_strategy\":\"page_mvcc\",",
                                "\"diagnostics_available\":true,",
                                "\"external_wait\":null,",
                                "\"fallback_reason\":null,",
                                "\"hotspot_kind\":\"page\",",
                                "\"page_number\":47,",
                                "\"plan_id\":\"plan-update-leaf-47\",",
                                "\"retry_count\":1,",
                                "\"statement_fingerprint\":\"fp-hot-page-update\",",
                                "\"suggested_next_inspection\":\"inspect_page_heat\",",
                                "\"table_name\":\"jobs\"}"
                            )
                            .into()
                        ),
                    ],
                    vec![
                SqliteValue::Text("fallback_compatibility_row".into()),
                SqliteValue::Text("fallback".into()),
                SqliteValue::Text("intentional-change-only-review".into()),
                SqliteValue::Text("none-required-static-contract-row".into()),
                SqliteValue::Text(
                    concat!(
                        "{\"concurrency_impact\":\"may_reduce_parallelism\",",
                        "\"diagnostics_available\":true,",
                        "\"fallback_reason\":\"compatibility_fallback\",",
                        "\"fallback_surface\":\"vdbe_compatibility\",",
                        "\"first_failure_diag\":\"window frame still uses compatibility path\",",
                        "\"impact_class\":\"latency\",",
                        "\"plan_id\":\"plan-window-compat\",",
                        "\"statement_fingerprint\":\"fp-window-fallback\",",
                        "\"supported_fast_path\":false,",
                        "\"table_name\":\"events\"}"
                    )
                    .into()
                ),
            ],
                ]
            );

            for row in rows {
                let values = row_values(&row);
                let golden_name = sqlite_text(&values[0])?;
                let json = sqlite_text(&values[4])?;
                assert!(
                    !json.contains("trace-") && !json.contains("run-"),
                    "{golden_name} must scrub dynamic trace/run identifiers"
                );
                assert!(
                    !json.contains("global_writer_lock"),
                    "{golden_name} must not bless serialized writer-lock diagnostics"
                );
            }

            Ok::<(), Box<dyn std::error::Error>>(())
        }
        .await;
    });
    outcome
}

#[test]
fn regression_names_cover_every_contract_surface() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let conn = Connection::open(":memory:").await?;
            install_test_matrix_schema(&conn).await?;

            let rows = conn
                .query(
                    "SELECT surface, regression_name, property_obligation, first_failure_diag
           FROM fsqlite_coordination_test_matrix_contract
          ORDER BY surface, invariant_id;",
                )
                .await?;

            let mut seen_surfaces = Vec::<String>::new();
            let mut property_obligations = Vec::<String>::new();
            for row in rows {
                let values = row_values(&row);
                let surface = sqlite_text(&values[0])?;
                let regression_name = sqlite_text(&values[1])?;
                let property_obligation = sqlite_text(&values[2])?;
                let first_failure_diag = sqlite_text(&values[3])?;

                assert!(
                    regression_name.starts_with(surface),
                    "{regression_name} must be scoped to {surface}"
                );
                assert!(
                    !first_failure_diag.is_empty() && first_failure_diag != "none",
                    "{regression_name} must have operator-facing failure context"
                );
                assert!(
                    !first_failure_diag.contains("global writer")
                        && !first_failure_diag.contains("global_writer_lock"),
                    "{regression_name} must not recommend serialized writer locking"
                );

                if !seen_surfaces.iter().any(|seen| seen == surface) {
                    seen_surfaces.push(surface.to_owned());
                }
                if property_obligation != "none" {
                    property_obligations.push(property_obligation.to_owned());
                }
            }

            seen_surfaces.sort();
            property_obligations.sort();

            assert_eq!(
                seen_surfaces,
                vec![
                    "explain".to_owned(),
                    "fallback".to_owned(),
                    "lease".to_owned(),
                    "queue".to_owned(),
                    "range".to_owned(),
                ]
            );
            assert_eq!(
                property_obligations,
                vec![
                    "no_double_claim".to_owned(),
                    "no_double_lease".to_owned(),
                    "no_overlapping_enforced_ranges".to_owned(),
                ]
            );

            Ok::<(), Box<dyn std::error::Error>>(())
        }
        .await;
    });
    outcome
}
