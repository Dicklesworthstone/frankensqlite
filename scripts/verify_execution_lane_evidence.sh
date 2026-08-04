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
    run_lib_test fsqlite-harness 'failure_bundle::tests::execution_lane_evidence_guard_unit_contract'
    run_lib_test fsqlite-e2e 'failure_bundle::tests::execution_lane_evidence_guard_bundle_contract'
    run_lib_test fsqlite-e2e 'fsqlite_executor::tests::execution_lane_evidence_guard_public_paths'
    ;;
  replay-mismatch)
    run_lib_test fsqlite-e2e 'fsqlite_executor::tests::pager_required_rejects_forced_compatibility_fallback_with_reason'
    ;;
  *)
    printf 'usage: %s [verify|replay-mismatch]\n' "$0" >&2
    exit 64
    ;;
esac
