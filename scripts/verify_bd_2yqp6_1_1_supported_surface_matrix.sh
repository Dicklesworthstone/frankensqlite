#!/usr/bin/env bash
# Verification gate for bd-2yqp6.1.1:
# freeze supported-surface matrix + explicit exclusions.
#
# Deterministic replay:
#   bash scripts/verify_bd_2yqp6_1_1_supported_surface_matrix.sh
#
# Structured logging contract:
#   emits JSONL events with trace_id/run_id/scenario_id/seed/timing/outcome
#   to artifacts/bd-2yqp6.1.1/<run_id>/events.jsonl

set -euo pipefail

BEAD_ID="bd-2yqp6.1.1"
SCENARIO_ID="PARITY-SURFACE-A1"
SEED=3520
TIMESTAMP_UTC="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ID="${BEAD_ID}-${TIMESTAMP_UTC}-${SEED}"
TRACE_ID="trace-${RUN_ID}"
ARTIFACT_DIR="artifacts/${BEAD_ID}/${RUN_ID}"
EVENTS_JSONL="${ARTIFACT_DIR}/events.jsonl"
REPORT_JSON="${ARTIFACT_DIR}/report.json"
MANIFEST="supported_surface_matrix.toml"

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

echo "=== ${BEAD_ID}: supported surface matrix verification ==="
echo "run_id=${RUN_ID}"
echo "trace_id=${TRACE_ID}"
echo "scenario_id=${SCENARIO_ID}"

emit_event "bootstrap" "start" "running" "verification started"

if [[ ! -f "${MANIFEST}" ]]; then
  emit_event "manifest_presence" "fail" "fail" "missing ${MANIFEST}"
  echo "missing ${MANIFEST}" >&2
  exit 1
fi
emit_event "manifest_presence" "pass" "pass" "${MANIFEST} exists"

emit_event "manifest_schema" "start" "running" "validating manifest schema with python tomllib"
python3 - <<'PY'
import sys
import tomllib
from pathlib import Path

manifest_path = Path("supported_surface_matrix.toml")
doc = tomllib.loads(manifest_path.read_text(encoding="utf-8"))

meta = doc.get("meta", {})
required_meta = ["schema_version", "bead_id", "track_id", "sqlite_target", "generated_at", "contract_owner"]
for key in required_meta:
    if key not in meta or not str(meta[key]).strip():
        raise SystemExit(f"missing meta.{key}")

surface = doc.get("surface", [])
if not surface:
    raise SystemExit("surface entries missing")

allowed_states = {"supported", "partial", "excluded"}
seen = set()
for row in surface:
    fid = row.get("feature_id", "").strip()
    if not fid:
        raise SystemExit("empty feature_id")
    if fid in seen:
        raise SystemExit(f"duplicate feature_id: {fid}")
    seen.add(fid)
    state = row.get("support_state", "").strip()
    if state not in allowed_states:
        raise SystemExit(f"invalid support_state for {fid}: {state}")
    for key in ("rationale", "owner", "verification_status"):
        if key not in row or not str(row[key]).strip():
            raise SystemExit(f"missing {key} for {fid}")
    ev = row.get("target_evidence", [])
    if not isinstance(ev, list) or not ev or any(not str(x).strip() for x in ev):
        raise SystemExit(f"invalid target_evidence for {fid}")

if not any(row.get("support_state") == "excluded" for row in surface):
    raise SystemExit("expected at least one explicit exclusion")
PY
emit_event "manifest_schema" "pass" "pass" "manifest schema validated"

emit_event "harness_test" "start" "running" "running rch cargo test gate"
if rch exec -- cargo test -p fsqlite-harness --test bd_2yqp6_1_1_supported_surface_matrix -- --nocapture; then
  emit_event "harness_test" "pass" "pass" "cargo test gate passed"
  RESULT="pass"
else
  emit_event "harness_test" "fail" "fail" "cargo test gate failed"
  RESULT="fail"
fi

cat > "${REPORT_JSON}" <<EOF
{
  "trace_id": "${TRACE_ID}",
  "run_id": "${RUN_ID}",
  "scenario_id": "${SCENARIO_ID}",
  "seed": ${SEED},
  "bead_id": "${BEAD_ID}",
  "manifest_path": "${MANIFEST}",
  "events_jsonl": "${EVENTS_JSONL}",
  "result": "${RESULT}"
}
EOF

emit_event "finalize" "info" "${RESULT}" "report written to ${REPORT_JSON}"

if [[ "${RESULT}" != "pass" ]]; then
  echo "[GATE FAIL] ${BEAD_ID} supported-surface contract gate failed"
  exit 1
fi

echo "[GATE PASS] ${BEAD_ID} supported-surface contract is valid"
