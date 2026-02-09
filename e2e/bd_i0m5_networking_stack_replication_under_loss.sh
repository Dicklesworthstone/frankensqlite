#!/usr/bin/env bash
set -euo pipefail

BEAD_ID="bd-i0m5"
WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ISSUES_PATH="${WORKSPACE_ROOT}/.beads/issues.jsonl"
TEST_TARGET="bd_i0m5_networking_stack_compliance"

printf 'bead_id=%s level=DEBUG case=start workspace=%s target=%s\n' \
    "${BEAD_ID}" "${WORKSPACE_ROOT}" "${TEST_TARGET}"

if [[ ! -f "${ISSUES_PATH}" ]]; then
    printf 'bead_id=%s level=ERROR case=missing_issues_jsonl path=%s\n' "${BEAD_ID}" "${ISSUES_PATH}"
    exit 1
fi

description="$(
    jq -r '
        select(.id == "bd-i0m5")
        | .description,
          (.comments[]?.text // empty)
    ' "${ISSUES_PATH}" | tr '\n' ' '
)"

if [[ -z "${description// }" ]]; then
    printf 'bead_id=%s level=ERROR case=missing_bead_description path=%s\n' "${BEAD_ID}" "${ISSUES_PATH}"
    exit 1
fi

required_tokens=(
    "test_tls_by_default"
    "test_plaintext_requires_explicit_opt_in"
    "test_http2_max_concurrent_streams"
    "test_http2_max_header_list_size"
    "test_http2_continuation_timeout"
    "test_message_size_cap_enforced"
    "test_handshake_timeout_bounded"
    "test_virtual_tcp_deterministic"
    "test_virtual_tcp_fault_injection"
    "test_e2e_networking_stack_replication_under_loss"
    "DEBUG"
    "INFO"
    "WARN"
    "ERROR"
    "rustls"
    "max_concurrent_streams=256"
    "max_header_list_size=64KiB"
    "continuation_timeout=5s"
    "header_fragment_cap"
    "4MiB"
    "VirtualTcp"
    "drop/reorder/corrupt"
)

declare -a missing_tokens=()
for token in "${required_tokens[@]}"; do
    if ! rg -Fq "${token}" <<<"${description}"; then
        missing_tokens+=("${token}")
    fi
done

printf \
    'bead_id=%s level=INFO case=description_scan required=%s missing=%s\n' \
    "${BEAD_ID}" "${#required_tokens[@]}" "${#missing_tokens[@]}"

if [[ "${#missing_tokens[@]}" -gt 0 ]]; then
    printf 'bead_id=%s level=WARN case=degraded_mode_count=%s\n' \
        "${BEAD_ID}" "${#missing_tokens[@]}"
    printf 'bead_id=%s level=ERROR case=missing_tokens items=%s\n' \
        "${BEAD_ID}" "${missing_tokens[*]}"
    exit 1
fi

if (cd "${WORKSPACE_ROOT}" && cargo test -p fsqlite-harness --test "${TEST_TARGET}" -- --nocapture); then
    printf 'bead_id=%s level=WARN case=degraded_mode_count=0\n' "${BEAD_ID}"
    printf 'bead_id=%s level=ERROR case=terminal_failure_count=0\n' "${BEAD_ID}"
    printf 'bead_id=%s level=INFO case=pass\n' "${BEAD_ID}"
    exit 0
fi

printf 'bead_id=%s level=WARN case=degraded_mode_count=1\n' "${BEAD_ID}"
printf 'bead_id=%s level=ERROR case=harness_test_failed target=%s\n' "${BEAD_ID}" "${TEST_TARGET}"
exit 1
