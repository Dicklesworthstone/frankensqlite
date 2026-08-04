#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-verify}"

run_lib_test() {
  local package="$1"
  local test_name="$2"
  if [[ "${FSQLITE_USE_RCH:-0}" == "1" ]]; then
    rch exec -- cargo test -p "${package}" --lib "${test_name}" -- --exact --nocapture --test-threads=1
  else
    cargo test -p "${package}" --lib "${test_name}" -- --exact --nocapture --test-threads=1
  fi
}

case "${MODE}" in
  verify)
    run_lib_test fsqlite-harness 'failure_bundle::tests::execution_lane_match_matrix_is_fail_closed'
    run_lib_test fsqlite-harness 'failure_bundle::tests::storage_requirements_reject_any_compatibility_fallback'
    run_lib_test fsqlite-harness 'failure_bundle::tests::execution_lane_validation_rejects_duplicates_conflicts_and_registry_drift'
    run_lib_test fsqlite-harness 'failure_bundle::tests::incomplete_fallback_capture_is_a_valid_fail_closed_mismatch'
    run_lib_test fsqlite-harness 'failure_bundle::tests::execution_lane_fallback_reasons_bind_to_canonical_inventory'
    run_lib_test fsqlite-harness 'failure_bundle::tests::execution_lane_serialization_is_stable_and_unknown_values_fail'
    run_lib_test fsqlite-harness 'failure_bundle::tests::execution_lane_property_recomputes_requirement'
    run_lib_test fsqlite-harness 'failure_bundle::tests::builder_rejects_missing_execution_lane_evidence'
    run_lib_test fsqlite-harness 'failure_bundle::tests::builder_rejects_corrupted_execution_lane_evidence'
    run_lib_test fsqlite-e2e 'failure_bundle::tests::test_builder_creates_bundle'
    run_lib_test fsqlite-e2e 'failure_bundle::tests::invalid_lane_evidence_fails_before_artifact_creation'
    run_lib_test fsqlite-e2e 'failure_bundle::tests::mismatched_lane_scenario_fails_before_artifact_creation'
    run_lib_test fsqlite-e2e 'failure_bundle::tests::interrupted_manifest_write_is_never_accepted_as_a_bundle'
    run_lib_test fsqlite-e2e 'failure_bundle::tests::manifest_is_not_a_completion_marker_until_repro_exists'
    run_lib_test fsqlite-e2e 'failure_bundle::tests::unsafe_artifact_path_fails_before_bundle_creation'
    run_lib_test fsqlite-e2e 'fsqlite_executor::tests::run_oplog_fsqlite_basic_serial'
    run_lib_test fsqlite-e2e 'fsqlite_executor::tests::public_executor_proves_pager_planner_vdbe_and_mvcc_lanes'
    run_lib_test fsqlite-e2e 'fsqlite_executor::tests::public_native_recovery_produces_recovery_lane_evidence'
    run_lib_test fsqlite-e2e 'fsqlite_executor::tests::pager_required_rejects_forced_compatibility_fallback_with_reason'
    run_lib_test fsqlite-e2e 'fsqlite_executor::tests::pager_required_rejects_allowed_compatibility_fallback_from_core_evidence'
    run_lib_test fsqlite-e2e 'fsqlite_executor::tests::lane_diagnostics_are_bounded_counted_and_correlated'
    ;;
  replay-mismatch)
    run_lib_test fsqlite-e2e 'fsqlite_executor::tests::pager_required_rejects_forced_compatibility_fallback_with_reason'
    ;;
  *)
    printf 'usage: %s [verify|replay-mismatch]\n' "$0" >&2
    exit 64
    ;;
esac
