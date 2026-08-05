#!/usr/bin/env bash
# Verification gate for bd-2yqp6.7.1:
# strict 100% empirical parity plus finite-sample statistical-floor policy.
#
# Deterministic replay:
#   bash scripts/verify_bd_2yqp6_7_1_release_threshold_policy.sh

set -euo pipefail

BEAD_ID="bd-2yqp6.7.1"
SCENARIO_ID="PARITY-RELEASE-THRESHOLD-G1"
SEED=7100
TIMESTAMP_UTC="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ID="${BEAD_ID}-${TIMESTAMP_UTC}-${SEED}"
TRACE_ID="trace-${RUN_ID}"
ARTIFACT_DIR="artifacts/${BEAD_ID}/${RUN_ID}"
EVENTS_JSONL="${ARTIFACT_DIR}/events.jsonl"
REPORT_JSON="${ARTIFACT_DIR}/report.json"
POLICY_FILE="docs/contracts/parity_release_threshold_policy.toml"
ROOT_POLICY_FILE="parity_release_threshold_policy.toml"
export POLICY_FILE ROOT_POLICY_FILE

mkdir -p "${ARTIFACT_DIR}"

emit_event() {
  local phase="$1"
  local event_type="$2"
  local outcome="$3"
  local message="$4"
  printf '{"trace_id":"%s","run_id":"%s","scenario_id":"%s","seed":%d,"phase":"%s","event_type":"%s","outcome":"%s","timestamp":"%s","message":"%s"}\n' \
    "${TRACE_ID}" "${RUN_ID}" "${SCENARIO_ID}" "${SEED}" "${phase}" "${event_type}" "${outcome}" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "${message}" \
    >> "${EVENTS_JSONL}"
}

echo "=== ${BEAD_ID}: strict empirical and statistical release-threshold policy verification ==="
echo "run_id=${RUN_ID}"
echo "trace_id=${TRACE_ID}"
echo "scenario_id=${SCENARIO_ID}"

emit_event "bootstrap" "start" "running" "verification started"

if [[ ! -f "${POLICY_FILE}" ]]; then
  emit_event "policy_presence" "fail" "fail" "missing ${POLICY_FILE}"
  echo "missing ${POLICY_FILE}" >&2
  exit 1
fi
if [[ ! -f "${ROOT_POLICY_FILE}" ]]; then
  emit_event "policy_presence" "fail" "fail" "missing ${ROOT_POLICY_FILE}"
  echo "missing ${ROOT_POLICY_FILE}" >&2
  exit 1
fi
emit_event "policy_presence" "pass" "pass" "${POLICY_FILE} and ${ROOT_POLICY_FILE} exist"

emit_event "policy_schema" "start" "running" "validating policy schema and signature"
python3 - <<'PY'
import hashlib
import os
import tomllib
from pathlib import Path

policy_path = Path(os.environ["POLICY_FILE"])
root_policy_path = Path(os.environ["ROOT_POLICY_FILE"])
if policy_path.read_bytes() != root_policy_path.read_bytes():
    raise SystemExit("root and documented policy copies differ")
doc = tomllib.loads(policy_path.read_text(encoding="utf-8"))

meta = doc.get("meta", {})
thresholds = doc.get("thresholds", {})
evidence = doc.get("evidence", {})
signature = doc.get("signature", {})
references = doc.get("references", {})

def require_exact_keys(section_name, section, expected):
    actual = set(section)
    expected = set(expected)
    if actual != expected:
        raise SystemExit(
            f"{section_name} keys mismatch: missing={sorted(expected - actual)} "
            f"extra={sorted(actual - expected)}"
        )

require_exact_keys("document", doc, ("meta", "thresholds", "evidence", "signature", "references"))
require_exact_keys(
    "meta",
    meta,
    (
        "schema_version",
        "policy_version",
        "certification_policy_id",
        "bead_id",
        "track_id",
        "generated_at",
        "policy_owner",
    ),
)
require_exact_keys(
    "thresholds",
    thresholds,
    (
        "declared_surface_parity_min",
        "required_suite_pass_rate_min",
        "score_engine_release_threshold",
        "confidence_gate_release_threshold",
        "ratchet_minimum_release_threshold",
        "allow_threshold_downgrade",
        "allow_waived_obligations",
    ),
)
require_exact_keys(
    "evidence",
    evidence,
    ("max_evidence_age_hours", "require_fresh_evidence_for_release"),
)
require_exact_keys("signature", signature, ("algorithm", "canonical_payload", "sha256"))
require_exact_keys(
    "references",
    references,
    (
        "parity_score_contract",
        "supported_surface_matrix",
        "score_engine_module",
        "confidence_gates_module",
        "ratchet_policy_module",
    ),
)

for key, value in meta.items():
    if not str(value).strip():
        raise SystemExit(f"empty meta.{key}")

if str(meta["schema_version"]) != "2.0.0":
    raise SystemExit("schema_version must equal 2.0.0")
if str(meta["policy_version"]) != "strict-100.v2":
    raise SystemExit("policy_version must equal strict-100.v2")
if str(meta["certification_policy_id"]) != "strict-conformant-release.v2":
    raise SystemExit("certification_policy_id must equal strict-conformant-release.v2")

for key, value in references.items():
    if not str(value).strip():
        raise SystemExit(f"empty references.{key}")
for key, value in signature.items():
    if not str(value).strip():
        raise SystemExit(f"empty signature.{key}")

if float(thresholds["declared_surface_parity_min"]) != 1.0:
    raise SystemExit("declared_surface_parity_min must equal 1.0")
if float(thresholds["required_suite_pass_rate_min"]) != 1.0:
    raise SystemExit("required_suite_pass_rate_min must equal 1.0")
statistical_floor = 0.70
for key in (
    "score_engine_release_threshold",
    "confidence_gate_release_threshold",
    "ratchet_minimum_release_threshold",
):
    value = float(thresholds[key])
    if value != statistical_floor:
        raise SystemExit(f"{key} must equal the {statistical_floor:.2f} finite-sample floor")
    if not 0.0 < value < 1.0:
        raise SystemExit(f"{key} must remain a feasible statistical floor in (0, 1)")
if bool(thresholds["allow_threshold_downgrade"]):
    raise SystemExit("allow_threshold_downgrade must be false")
if bool(thresholds["allow_waived_obligations"]):
    raise SystemExit("allow_waived_obligations must be false")
if not bool(evidence["require_fresh_evidence_for_release"]):
    raise SystemExit("require_fresh_evidence_for_release must be true")
if int(evidence["max_evidence_age_hours"]) <= 0:
    raise SystemExit("max_evidence_age_hours must be positive")

canonical = (
    f"schema_version={meta['schema_version']}"
    f"|policy_version={meta['policy_version']}"
    f"|certification_policy_id={meta['certification_policy_id']}"
    f"|bead_id={meta['bead_id']}"
    f"|track_id={meta['track_id']}"
    f"|generated_at={meta['generated_at']}"
    f"|policy_owner={meta['policy_owner']}"
    f"|declared_surface_parity_min={float(thresholds['declared_surface_parity_min']):.6f}"
    f"|required_suite_pass_rate_min={float(thresholds['required_suite_pass_rate_min']):.6f}"
    f"|score_engine_release_threshold={float(thresholds['score_engine_release_threshold']):.6f}"
    f"|confidence_gate_release_threshold={float(thresholds['confidence_gate_release_threshold']):.6f}"
    f"|ratchet_minimum_release_threshold={float(thresholds['ratchet_minimum_release_threshold']):.6f}"
    f"|allow_threshold_downgrade={str(bool(thresholds['allow_threshold_downgrade'])).lower()}"
    f"|allow_waived_obligations={str(bool(thresholds['allow_waived_obligations'])).lower()}"
    f"|max_evidence_age_hours={int(evidence['max_evidence_age_hours'])}"
    f"|require_fresh_evidence_for_release={str(bool(evidence['require_fresh_evidence_for_release'])).lower()}"
    f"|parity_score_contract={references['parity_score_contract']}"
    f"|supported_surface_matrix={references['supported_surface_matrix']}"
    f"|score_engine_module={references['score_engine_module']}"
    f"|confidence_gates_module={references['confidence_gates_module']}"
    f"|ratchet_policy_module={references['ratchet_policy_module']}"
)
if canonical != str(signature["canonical_payload"]):
    raise SystemExit("signature canonical_payload mismatch")
if str(signature["algorithm"]) != "sha256":
    raise SystemExit("signature algorithm must be sha256")

actual = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
if actual != str(signature["sha256"]):
    raise SystemExit("signature sha256 mismatch")
PY
emit_event "policy_schema" "pass" "pass" "policy schema and signature validated"

emit_event "harness_test" "start" "running" "running rch cargo test gate"
if rch exec -- cargo test -p fsqlite-harness --test bd_2yqp6_7_1_release_threshold_policy -- --nocapture; then
  emit_event "harness_test" "pass" "pass" "cargo test gate passed"
  RESULT="pass"
else
  emit_event "harness_test" "fail" "fail" "cargo test gate failed"
  RESULT="fail"
fi

cat > "${REPORT_JSON}" <<EOF_JSON
{
  "trace_id": "${TRACE_ID}",
  "run_id": "${RUN_ID}",
  "scenario_id": "${SCENARIO_ID}",
  "seed": ${SEED},
  "bead_id": "${BEAD_ID}",
  "policy_file": "${POLICY_FILE}",
  "events_jsonl": "${EVENTS_JSONL}",
  "result": "${RESULT}"
}
EOF_JSON

emit_event "finalize" "info" "${RESULT}" "report written to ${REPORT_JSON}"

if [[ "${RESULT}" != "pass" ]]; then
  echo "[GATE FAIL] ${BEAD_ID} release threshold policy verification failed"
  exit 1
fi

echo "[GATE PASS] ${BEAD_ID} release threshold policy is valid"
