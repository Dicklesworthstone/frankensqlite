#!/usr/bin/env bash
# Verification gate for bd-2yqp6.1.2:
# canonical feature-universe ledger with lint enforcement for test/evidence links.
#
# Deterministic replay:
#   bash scripts/verify_bd_2yqp6_1_2_feature_universe_ledger.sh

set -euo pipefail

BEAD_ID="bd-2yqp6.1.2"
SCENARIO_ID="PARITY-LEDGER-A2"
SEED=3520
TIMESTAMP_UTC="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ID="${BEAD_ID}-${TIMESTAMP_UTC}-${SEED}"
TRACE_ID="trace-${RUN_ID}"
ARTIFACT_DIR="artifacts/${BEAD_ID}/${RUN_ID}"
EVENTS_JSONL="${ARTIFACT_DIR}/events.jsonl"
REPORT_JSON="${ARTIFACT_DIR}/report.json"
LEDGER_FILE="feature_universe_ledger.toml"

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

echo "=== ${BEAD_ID}: feature universe ledger verification ==="
echo "run_id=${RUN_ID}"
echo "trace_id=${TRACE_ID}"
echo "scenario_id=${SCENARIO_ID}"

emit_event "bootstrap" "start" "running" "verification started"

if [[ ! -f "${LEDGER_FILE}" ]]; then
  emit_event "ledger_presence" "fail" "fail" "missing ${LEDGER_FILE}"
  echo "missing ${LEDGER_FILE}" >&2
  exit 1
fi
emit_event "ledger_presence" "pass" "pass" "${LEDGER_FILE} exists"

emit_event "ledger_schema" "start" "running" "validating ledger schema with python tomllib"
python3 - <<'PY'
import tomllib
from pathlib import Path

ledger = tomllib.loads(Path("feature_universe_ledger.toml").read_text(encoding="utf-8"))
surface = tomllib.loads(Path("supported_surface_matrix.toml").read_text(encoding="utf-8"))

meta = ledger.get("meta", {})
for key in ("schema_version", "bead_id", "track_id", "sqlite_target", "generated_at", "contract_owner"):
    if key not in meta or not str(meta[key]).strip():
        raise SystemExit(f"missing meta.{key}")

features = ledger.get("features", [])
if not features:
    raise SystemExit("features array is empty")

surface_ids = {entry["feature_id"] for entry in surface.get("surface", [])}
allowed_components = {"parser", "planner", "vdbe", "core", "extension"}
allowed_lifecycle = {"declared", "implemented", "tested", "differentially_verified"}
seen = set()

for entry in features:
    fid = entry.get("feature_id", "").strip()
    if not fid:
        raise SystemExit("feature with empty feature_id")
    if fid in seen:
        raise SystemExit(f"duplicate feature_id: {fid}")
    seen.add(fid)

    if entry.get("component") not in allowed_components:
        raise SystemExit(f"invalid component for {fid}: {entry.get('component')}")
    if entry.get("lifecycle_state") not in allowed_lifecycle:
        raise SystemExit(f"invalid lifecycle_state for {fid}: {entry.get('lifecycle_state')}")
    if entry.get("surface_id") not in surface_ids:
        raise SystemExit(f"unknown surface_id for {fid}: {entry.get('surface_id')}")

    for key in ("feature_name", "owner"):
        if key not in entry or not str(entry[key]).strip():
            raise SystemExit(f"missing {key} for {fid}")
    for key in ("test_links", "evidence_links"):
        links = entry.get(key, [])
        if not isinstance(links, list) or not links or any(not str(x).strip() for x in links):
            raise SystemExit(f"invalid {key} for {fid}")
PY
emit_event "ledger_schema" "pass" "pass" "ledger schema validated"

emit_event "harness_test" "start" "running" "running rch cargo test gate"
if rch exec -- cargo test -p fsqlite-harness --test bd_2yqp6_1_2_feature_universe_ledger -- --nocapture; then
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
  "ledger_file": "${LEDGER_FILE}",
  "events_jsonl": "${EVENTS_JSONL}",
  "result": "${RESULT}"
}
EOF

emit_event "finalize" "info" "${RESULT}" "report written to ${REPORT_JSON}"

if [[ "${RESULT}" != "pass" ]]; then
  echo "[GATE FAIL] ${BEAD_ID} feature-universe ledger gate failed"
  exit 1
fi

echo "[GATE PASS] ${BEAD_ID} feature-universe ledger is valid"
