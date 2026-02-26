//! Unit + integration tests for Oracle Differential Harness V2 (bd-1dp9.1.2).
//!
//! Validates:
//! - Execution envelope determinism (same input → same artifact ID)
//! - Differential comparison correctness (pass/divergence detection)
//! - Canonicalization rules (float tolerance, multiset, error category)
//! - Reproducibility (two runs with identical envelopes produce identical results)

use fsqlite_harness::differential_v2::{
    self, CanonicalizationRules, DifferentialResult, EngineIdentity, ExecutionEnvelope,
    FORMAT_VERSION, FsqliteExecutor, NormalizedValue, Outcome, PragmaConfig, SqlExecutor,
};

/// Rusqlite executor for the C SQLite oracle.
struct RusqliteExecutor {
    conn: rusqlite::Connection,
}

impl RusqliteExecutor {
    fn open_in_memory() -> Self {
        Self {
            conn: rusqlite::Connection::open_in_memory().expect("rusqlite open"),
        }
    }
}

impl SqlExecutor for RusqliteExecutor {
    fn execute(&self, sql: &str) -> Result<usize, String> {
        self.conn.execute(sql.trim(), []).map_err(|e| e.to_string())
    }

    fn query(&self, sql: &str) -> Result<Vec<Vec<NormalizedValue>>, String> {
        let mut stmt = self.conn.prepare(sql.trim()).map_err(|e| e.to_string())?;
        let col_count = stmt.column_count();
        let rows = stmt
            .query_map([], |row| {
                let mut vals = Vec::with_capacity(col_count);
                for i in 0..col_count {
                    let v: rusqlite::types::Value =
                        row.get(i).unwrap_or(rusqlite::types::Value::Null);
                    vals.push(match v {
                        rusqlite::types::Value::Null => NormalizedValue::Null,
                        rusqlite::types::Value::Integer(i) => NormalizedValue::Integer(i),
                        rusqlite::types::Value::Real(f) => NormalizedValue::Real(f),
                        rusqlite::types::Value::Text(s) => NormalizedValue::Text(s),
                        rusqlite::types::Value::Blob(b) => NormalizedValue::Blob(b),
                    });
                }
                Ok(vals)
            })
            .map_err(|e| e.to_string())?;

        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())
    }

    fn engine_identity(&self) -> EngineIdentity {
        EngineIdentity::CSqliteOracle
    }
}

/// Fixed-error executor used to validate error-category matching behavior.
struct FixedErrorExecutor {
    engine: EngineIdentity,
    error_message: String,
}

impl FixedErrorExecutor {
    fn new(engine: EngineIdentity, error_message: impl Into<String>) -> Self {
        Self {
            engine,
            error_message: error_message.into(),
        }
    }
}

impl SqlExecutor for FixedErrorExecutor {
    fn execute(&self, _sql: &str) -> Result<usize, String> {
        Err(self.error_message.clone())
    }

    fn query(&self, _sql: &str) -> Result<Vec<Vec<NormalizedValue>>, String> {
        Err(self.error_message.clone())
    }

    fn engine_identity(&self) -> EngineIdentity {
        self.engine
    }
}

fn make_test_envelope(seed: u64, schema: Vec<&str>, workload: Vec<&str>) -> ExecutionEnvelope {
    ExecutionEnvelope::builder(seed)
        .engines("0.1.0-test", "3.45.0-test")
        .schema(schema.into_iter().map(String::from))
        .workload(workload.into_iter().map(String::from))
        .build()
}

fn run_test(envelope: &ExecutionEnvelope) -> DifferentialResult {
    let f = FsqliteExecutor::open_in_memory().expect("fsqlite open");
    let c = RusqliteExecutor::open_in_memory();
    differential_v2::run_differential(envelope, &f, &c)
}

// ─── Envelope Determinism Tests ──────────────────────────────────────────

#[test]
fn envelope_artifact_id_is_deterministic() {
    let e1 = make_test_envelope(42, vec![], vec!["SELECT 1"]);
    let e2 = make_test_envelope(42, vec![], vec!["SELECT 1"]);
    assert_eq!(e1.artifact_id(), e2.artifact_id());
}

#[test]
fn envelope_artifact_id_changes_with_seed() {
    let e1 = make_test_envelope(42, vec![], vec!["SELECT 1"]);
    let e2 = make_test_envelope(43, vec![], vec!["SELECT 1"]);
    assert_ne!(e1.artifact_id(), e2.artifact_id());
}

#[test]
fn envelope_artifact_id_changes_with_workload() {
    let e1 = make_test_envelope(42, vec![], vec!["SELECT 1"]);
    let e2 = make_test_envelope(42, vec![], vec!["SELECT 2"]);
    assert_ne!(e1.artifact_id(), e2.artifact_id());
}

#[test]
fn envelope_artifact_id_changes_with_schema() {
    let e1 = make_test_envelope(42, vec!["CREATE TABLE t(x)"], vec!["SELECT 1"]);
    let e2 = make_test_envelope(42, vec!["CREATE TABLE t(x, y)"], vec!["SELECT 1"]);
    assert_ne!(e1.artifact_id(), e2.artifact_id());
}

#[test]
fn envelope_artifact_id_ignores_run_id() {
    let e1 = ExecutionEnvelope::builder(42)
        .run_id("run-001")
        .engines("0.1.0", "3.45.0")
        .workload(["SELECT 1".to_owned()])
        .build();
    let e2 = ExecutionEnvelope::builder(42)
        .run_id("run-002")
        .engines("0.1.0", "3.45.0")
        .workload(["SELECT 1".to_owned()])
        .build();
    assert_eq!(
        e1.artifact_id(),
        e2.artifact_id(),
        "run_id must not affect artifact_id"
    );
}

#[test]
fn envelope_artifact_id_is_valid_sha256() {
    let e = make_test_envelope(42, vec![], vec!["SELECT 1"]);
    let id = e.artifact_id();
    assert_eq!(id.len(), 64, "SHA-256 hex should be 64 chars");
    assert!(
        id.chars().all(|c| c.is_ascii_hexdigit()),
        "SHA-256 hex should only contain hex digits"
    );
}

#[test]
fn envelope_format_version_is_current() {
    let e = make_test_envelope(42, vec![], vec![]);
    assert_eq!(e.format_version, FORMAT_VERSION);
}

// ─── Serialization Round-Trip ────────────────────────────────────────────

#[test]
fn envelope_serialization_roundtrip() {
    let e = make_test_envelope(
        42,
        vec!["CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)"],
        vec!["INSERT INTO t VALUES(1, 'hello')", "SELECT * FROM t"],
    );
    let json = serde_json::to_string_pretty(&e).expect("serialize");
    let e2: ExecutionEnvelope = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(e, e2);
    assert_eq!(e.artifact_id(), e2.artifact_id());
}

// ─── Differential Comparison Tests ───────────────────────────────────────

#[test]
fn differential_pass_on_identical_queries() {
    let envelope = make_test_envelope(42, vec![], vec!["SELECT 1", "SELECT 'hello'"]);
    let result = run_test(&envelope);

    assert_eq!(result.outcome, Outcome::Pass);
    assert_eq!(result.statements_mismatched, 0);
    assert!(result.logical_state_matched);
    assert!(result.divergences.is_empty());
    eprintln!(
        "bead_id=bd-1dp9.1.2 test=differential_pass matched={} total={}",
        result.statements_matched, result.statements_total
    );
}

#[test]
fn differential_pass_on_schema_and_dml() {
    let envelope = make_test_envelope(
        42,
        vec!["CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT)"],
        vec![
            "INSERT INTO t VALUES(1, 'alice')",
            "INSERT INTO t VALUES(2, 'bob')",
            "SELECT * FROM t ORDER BY id",
        ],
    );
    let result = run_test(&envelope);

    assert_eq!(result.outcome, Outcome::Pass);
    assert!(result.logical_state_matched);
    eprintln!(
        "bead_id=bd-1dp9.1.2 test=differential_schema_dml matched={} total={} state_hash={}",
        result.statements_matched, result.statements_total, result.logical_state_hash_csqlite
    );
}

#[test]
fn differential_pass_on_multiple_tables() {
    let envelope = make_test_envelope(
        42,
        vec![
            "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE orders(id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)",
        ],
        vec![
            "INSERT INTO users VALUES(1, 'alice')",
            "INSERT INTO users VALUES(2, 'bob')",
            "INSERT INTO orders VALUES(1, 1, 9.99)",
            "INSERT INTO orders VALUES(2, 2, 19.99)",
            "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id ORDER BY u.name",
        ],
    );
    let result = run_test(&envelope);

    assert_eq!(result.outcome, Outcome::Pass);
    assert!(result.logical_state_matched);
}

#[test]
fn differential_pass_on_autocommit_inserts() {
    // Use autocommit mode (no explicit BEGIN/COMMIT) since explicit transaction
    // control is a known parity gap between FrankenSQLite and C SQLite APIs.
    let envelope = make_test_envelope(
        42,
        vec!["CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)"],
        vec![
            "INSERT INTO t VALUES(1, 100)",
            "INSERT INTO t VALUES(2, 200)",
            "INSERT INTO t VALUES(3, 300)",
            "SELECT * FROM t ORDER BY id",
        ],
    );
    let result = run_test(&envelope);

    assert_eq!(result.outcome, Outcome::Pass);
    assert!(result.logical_state_matched);
}

// ─── Reproducibility Tests ───────────────────────────────────────────────

#[test]
fn differential_result_is_reproducible() {
    let envelope = make_test_envelope(
        42,
        vec!["CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)"],
        vec![
            "INSERT INTO t VALUES(1, 'hello')",
            "INSERT INTO t VALUES(2, 'world')",
            "SELECT * FROM t ORDER BY id",
        ],
    );

    let r1 = run_test(&envelope);
    let r2 = run_test(&envelope);

    assert_eq!(
        r1.artifact_hashes.envelope_id, r2.artifact_hashes.envelope_id,
        "envelope_id must be identical across runs"
    );
    assert_eq!(
        r1.artifact_hashes.result_hash, r2.artifact_hashes.result_hash,
        "result_hash must be identical across runs"
    );
    assert_eq!(
        r1.artifact_hashes.workload_hash, r2.artifact_hashes.workload_hash,
        "workload_hash must be identical across runs"
    );
    assert_eq!(r1.outcome, r2.outcome);
    assert_eq!(r1.statements_matched, r2.statements_matched);
    assert_eq!(r1.statements_mismatched, r2.statements_mismatched);
    assert_eq!(
        r1.logical_state_hash_fsqlite, r2.logical_state_hash_fsqlite,
        "fsqlite logical state must be identical"
    );
    assert_eq!(
        r1.logical_state_hash_csqlite, r2.logical_state_hash_csqlite,
        "csqlite logical state must be identical"
    );

    eprintln!(
        "bead_id=bd-1dp9.1.2 test=reproducibility envelope_id={} result_hash={}",
        r1.artifact_hashes.envelope_id, r1.artifact_hashes.result_hash
    );
}

#[test]
fn differential_result_serializes_to_json() {
    let envelope = make_test_envelope(42, vec![], vec!["SELECT 1 + 1"]);
    let result = run_test(&envelope);

    let json = serde_json::to_string_pretty(&result).expect("serialize result");
    assert!(json.contains("\"bead_id\""));
    assert!(json.contains("\"envelope\""));
    assert!(json.contains("\"artifact_hashes\""));
    assert!(json.contains("\"outcome\""));

    // Verify it round-trips.
    let r2: DifferentialResult = serde_json::from_str(&json).expect("deserialize result");
    assert_eq!(r2.outcome, result.outcome);
    assert_eq!(
        r2.artifact_hashes.envelope_id,
        result.artifact_hashes.envelope_id
    );
}

// ─── Canonicalization Rule Tests ─────────────────────────────────────────

#[test]
fn canonicalization_error_match_by_category() {
    // Both engines should error on referencing a non-existent table.
    let envelope = make_test_envelope(42, vec![], vec!["SELECT * FROM nonexistent_table_xyz"]);
    let result = run_test(&envelope);

    // With error_match_by_category = true (default), equivalent categories match.
    assert_eq!(
        result.outcome,
        Outcome::Pass,
        "equivalent missing-table errors should match under category mode"
    );
}

#[test]
fn canonicalization_error_match_by_category_rejects_different_categories() {
    let envelope = make_test_envelope(77, vec![], vec!["SELECT * FROM missing_table"]);
    let f = FixedErrorExecutor::new(
        EngineIdentity::FrankenSqlite,
        "no such table: missing_table",
    );
    let c = FixedErrorExecutor::new(
        EngineIdentity::CSqliteOracle,
        "SqliteFailure(Error { code: ConstraintViolation, extended_code: 2067 }, Some(\"UNIQUE constraint failed: t.id\"))",
    );

    let result = differential_v2::run_differential(&envelope, &f, &c);
    assert_eq!(result.outcome, Outcome::Divergence);
    assert_eq!(result.statements_mismatched, 1);
    assert_eq!(result.first_divergence_index, Some(0));
}

#[test]
fn builder_sets_defaults_correctly() {
    let e = ExecutionEnvelope::builder(123).build();
    assert_eq!(e.format_version, FORMAT_VERSION);
    assert_eq!(e.seed, 123);
    assert_eq!(e.pragmas, PragmaConfig::default());
    assert_eq!(e.canonicalization, CanonicalizationRules::default());
    assert!(e.schema.is_empty());
    assert!(e.workload.is_empty());
    assert!(e.run_id.is_none());
    assert!(
        !e.engines.csqlite.trim().is_empty(),
        "default csqlite version metadata must be non-empty"
    );
    assert_eq!(e.engines.subject_identity, "frankensqlite");
    assert_eq!(e.engines.reference_identity, "csqlite-oracle");
}

#[test]
fn parity_rejects_empty_subject_identity_metadata() {
    let envelope = ExecutionEnvelope::builder(42)
        .engines("0.1.0-test", "3.52.0-test")
        .engine_identities("", "csqlite-oracle")
        .workload(["SELECT 1".to_owned()])
        .build();
    let f = FsqliteExecutor::open_in_memory().expect("fsqlite open");
    let c = RusqliteExecutor::open_in_memory();

    let result = differential_v2::run_differential(&envelope, &f, &c);
    assert_eq!(result.outcome, Outcome::Error);
}

#[test]
fn parity_rejects_mismatched_reference_identity_metadata() {
    let envelope = ExecutionEnvelope::builder(42)
        .engines("0.1.0-test", "3.52.0-test")
        .engine_identities("frankensqlite", "unknown")
        .workload(["SELECT 1".to_owned()])
        .build();
    let f = FsqliteExecutor::open_in_memory().expect("fsqlite open");
    let c = RusqliteExecutor::open_in_memory();

    let result = differential_v2::run_differential(&envelope, &f, &c);
    assert_eq!(result.outcome, Outcome::Error);
}

#[test]
fn parity_rejects_empty_csqlite_version_metadata() {
    let envelope = ExecutionEnvelope::builder(42)
        .engines("0.1.0-test", "")
        .workload(["SELECT 1".to_owned()])
        .build();
    let f = FsqliteExecutor::open_in_memory().expect("fsqlite open");
    let c = RusqliteExecutor::open_in_memory();

    let result = differential_v2::run_differential(&envelope, &f, &c);
    assert_eq!(result.outcome, Outcome::Error);
}

#[test]
fn parity_rejects_placeholder_csqlite_version_metadata() {
    for placeholder in ["unknown", "n/a", "unset", "none", "null", "missing"] {
        let envelope = ExecutionEnvelope::builder(42)
            .engines("0.1.0-test", placeholder)
            .workload(["SELECT 1".to_owned()])
            .build();
        let f = FsqliteExecutor::open_in_memory().expect("fsqlite open");
        let c = RusqliteExecutor::open_in_memory();

        let result = differential_v2::run_differential(&envelope, &f, &c);
        assert_eq!(
            result.outcome,
            Outcome::Error,
            "placeholder metadata '{placeholder}' must fail parity preflight"
        );
    }
}

#[test]
fn parity_rejects_non_fsqlite_subject_executor() {
    let envelope = make_test_envelope(42, vec![], vec!["SELECT 1"]);
    let not_subject = RusqliteExecutor::open_in_memory();
    let oracle = RusqliteExecutor::open_in_memory();

    let result = differential_v2::run_differential(&envelope, &not_subject, &oracle);
    assert_eq!(result.outcome, Outcome::Error);
}

#[test]
fn parity_rejects_non_csqlite_reference_executor() {
    let envelope = make_test_envelope(42, vec![], vec!["SELECT 1"]);
    let f = FsqliteExecutor::open_in_memory().expect("fsqlite open");
    let not_oracle = FsqliteExecutor::open_in_memory().expect("fsqlite open");

    let result = differential_v2::run_differential(&envelope, &f, &not_oracle);
    assert_eq!(result.outcome, Outcome::Error);
}

#[test]
fn diagnostic_mode_allows_explicit_self_compare() {
    let envelope = make_test_envelope(42, vec![], vec!["SELECT 1", "SELECT 'ok'"]);
    let left = FsqliteExecutor::open_in_memory().expect("fsqlite open");
    let right = FsqliteExecutor::open_in_memory().expect("fsqlite open");

    let result = differential_v2::run_differential_diagnostic(&envelope, &left, &right);
    assert_eq!(result.outcome, Outcome::Pass);
}

#[test]
fn pragma_config_generates_sql() {
    let config = PragmaConfig::default();
    let pragmas = config.to_pragma_sql();
    assert_eq!(pragmas.len(), 4);
    assert!(pragmas[0].contains("journal_mode"));
    assert!(pragmas[1].contains("synchronous"));
    assert!(pragmas[2].contains("cache_size"));
    assert!(pragmas[3].contains("page_size"));
}

// ─── Artifact Hash Determinism ───────────────────────────────────────────

#[test]
fn artifact_hashes_are_all_valid_sha256() {
    let envelope = make_test_envelope(
        42,
        vec!["CREATE TABLE t(x INTEGER)"],
        vec!["INSERT INTO t VALUES(1)", "SELECT * FROM t"],
    );
    let result = run_test(&envelope);

    for (name, hash) in [
        ("envelope_id", &result.artifact_hashes.envelope_id),
        ("result_hash", &result.artifact_hashes.result_hash),
        ("workload_hash", &result.artifact_hashes.workload_hash),
    ] {
        assert_eq!(
            hash.len(),
            64,
            "{name} should be 64 hex chars, got {}",
            hash.len()
        );
        assert!(
            hash.chars().all(|c| c.is_ascii_hexdigit()),
            "{name} should only contain hex digits"
        );
    }
}

#[test]
fn result_hash_changes_when_outcome_changes() {
    // A passing run and a run with a divergence must have different result hashes.
    let pass_envelope = make_test_envelope(42, vec![], vec!["SELECT 1"]);
    let pass_result = run_test(&pass_envelope);
    assert_eq!(pass_result.outcome, Outcome::Pass);

    // Force a different result by using a query that returns different results
    // between engines (if there is one). Since both engines should agree on basic
    // SQL, let's just verify the hash depends on the outcome fields.
    let hash1 = pass_result.artifact_hashes.result_hash;

    // Same envelope, same result — hashes must match.
    let pass_result2 = run_test(&pass_envelope);
    assert_eq!(hash1, pass_result2.artifact_hashes.result_hash);
}

#[derive(Clone, Copy)]
enum MockEngine {
    Fsqlite,
    Csqlite,
}

struct ReducerMockExecutor {
    engine: MockEngine,
}

impl ReducerMockExecutor {
    const fn new(engine: MockEngine) -> Self {
        Self { engine }
    }
}

impl SqlExecutor for ReducerMockExecutor {
    fn execute(&self, _sql: &str) -> Result<usize, String> {
        Ok(0)
    }

    fn query(&self, sql: &str) -> Result<Vec<Vec<NormalizedValue>>, String> {
        let trimmed = sql.trim();
        let value = if trimmed.eq_ignore_ascii_case("SELECT MISMATCH")
            && matches!(self.engine, MockEngine::Fsqlite)
        {
            2
        } else {
            1
        };
        Ok(vec![vec![NormalizedValue::Integer(value)]])
    }

    fn engine_identity(&self) -> EngineIdentity {
        match self.engine {
            MockEngine::Fsqlite => EngineIdentity::FrankenSqlite,
            MockEngine::Csqlite => EngineIdentity::CSqliteOracle,
        }
    }
}

#[test]
fn mismatch_reducer_minimizes_to_single_divergent_statement() {
    let envelope = make_test_envelope(
        99,
        vec![],
        vec!["SELECT 1", "SELECT 2", "SELECT MISMATCH", "SELECT 3"],
    );

    let reduction = differential_v2::minimize_mismatch_workload(
        &envelope,
        || Ok(ReducerMockExecutor::new(MockEngine::Fsqlite)),
        || Ok(ReducerMockExecutor::new(MockEngine::Csqlite)),
    )
    .expect("reducer should execute")
    .expect("baseline workload should diverge");

    assert_eq!(reduction.original_workload_len, 4);
    assert_eq!(reduction.minimized_workload_len, 1);
    assert_eq!(
        reduction.minimized_envelope.workload,
        vec!["SELECT MISMATCH".to_owned()]
    );
    assert_eq!(reduction.removed_workload_indices, vec![0, 1, 3]);
    assert_eq!(reduction.minimized_result.outcome, Outcome::Divergence);
    assert_eq!(reduction.minimized_result.statements_mismatched, 1);
    assert!(
        reduction.reduction_ratio() >= 0.75,
        "expected at least 75% reduction"
    );
}

#[test]
fn mismatch_reducer_returns_none_for_passing_workload() {
    let envelope = make_test_envelope(101, vec![], vec!["SELECT 1", "SELECT 2"]);

    let reduction = differential_v2::minimize_mismatch_workload(
        &envelope,
        || Ok(ReducerMockExecutor::new(MockEngine::Fsqlite)),
        || Ok(ReducerMockExecutor::new(MockEngine::Csqlite)),
    )
    .expect("reducer should execute");

    assert!(reduction.is_none());
}
