#!/usr/bin/env bash
# Verification gate for bd-2yqp6.4.1:
# inventory + classification of runtime NotImplemented/stub paths.
#
# Deterministic replay:
#   bash scripts/verify_bd_2yqp6_4_1_runtime_stub_inventory.sh

set -euo pipefail

BEAD_ID="bd-2yqp6.4.1"
SCENARIO_ID="PARITY-STUB-INVENTORY-D1"
SEED=3520
TIMESTAMP_UTC="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ID="${BEAD_ID}-${TIMESTAMP_UTC}-${SEED}"
TRACE_ID="trace-${RUN_ID}"
ARTIFACT_DIR="artifacts/${BEAD_ID}/${RUN_ID}"
EVENTS_JSONL="${ARTIFACT_DIR}/events.jsonl"
REPORT_JSON="${ARTIFACT_DIR}/report.json"
INVENTORY_FILE="runtime_stub_inventory.toml"

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

echo "=== ${BEAD_ID}: runtime stub inventory verification ==="
echo "run_id=${RUN_ID}"
echo "trace_id=${TRACE_ID}"
echo "scenario_id=${SCENARIO_ID}"

emit_event "bootstrap" "start" "running" "verification started"

if [[ ! -f "${INVENTORY_FILE}" ]]; then
  emit_event "inventory_presence" "fail" "fail" "missing ${INVENTORY_FILE}"
  echo "missing ${INVENTORY_FILE}" >&2
  exit 1
fi
emit_event "inventory_presence" "pass" "pass" "${INVENTORY_FILE} exists"

emit_event "inventory_schema" "start" "running" "validating runtime stub inventory schema"
python3 - <<'PY'
import tomllib
from pathlib import Path

doc = tomllib.loads(Path("runtime_stub_inventory.toml").read_text(encoding="utf-8"))
meta = doc.get("meta", {})
required_meta = [
    "schema_version",
    "bead_id",
    "track_id",
    "sqlite_target",
    "generated_at",
    "contract_owner",
    "inventory_scope",
    "source_patterns",
    "parity_critical_severities",
]
for key in required_meta:
    if key not in meta:
        raise SystemExit(f"missing meta.{key}")

if meta.get("bead_id") != "bd-2yqp6.4.1":
    raise SystemExit(f"unexpected bead_id: {meta.get('bead_id')}")

entries = doc.get("runtime_stubs", [])
if not isinstance(entries, list) or not entries:
    raise SystemExit("runtime_stubs must be a non-empty array")

seen_ids = set()
seen_keys = set()
for entry in entries:
    sid = str(entry.get("stub_id", "")).strip()
    file = str(entry.get("file", "")).strip()
    line = entry.get("line")
    kind = str(entry.get("kind", "")).strip()
    severity = str(entry.get("severity", "")).strip()
    feature_id = str(entry.get("feature_id", "")).strip()
    owner = str(entry.get("owner", "")).strip()
    closure = str(entry.get("closure_strategy", "")).strip()

    if not sid:
        raise SystemExit("entry missing stub_id")
    if sid in seen_ids:
        raise SystemExit(f"duplicate stub_id: {sid}")
    seen_ids.add(sid)

    if not file:
        raise SystemExit(f"{sid}: missing file")
    if not isinstance(line, int) or line <= 0:
        raise SystemExit(f"{sid}: invalid line")
    key = (file, line, kind)
    if key in seen_keys:
        raise SystemExit(f"duplicate file:line:kind tuple: {key}")
    seen_keys.add(key)

    if kind not in {"not_implemented", "unsupported_codegen", "todo_placeholder"}:
        raise SystemExit(f"{sid}: invalid kind {kind}")
    if severity not in {"critical", "high", "medium", "low"}:
        raise SystemExit(f"{sid}: invalid severity {severity}")
    if closure not in {"implement", "explicit_exclusion"}:
        raise SystemExit(f"{sid}: invalid closure strategy {closure}")
    if not feature_id:
        raise SystemExit(f"{sid}: missing feature_id")
    if not owner:
        raise SystemExit(f"{sid}: missing owner")
PY
emit_event "inventory_schema" "pass" "pass" "runtime stub inventory schema validated"

emit_event "harness_test" "start" "running" "running rch cargo test gate"
if rch exec -- cargo test -p fsqlite-harness --test bd_2yqp6_4_1_runtime_stub_inventory -- --nocapture; then
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
  "inventory_file": "${INVENTORY_FILE}",
  "events_jsonl": "${EVENTS_JSONL}",
  "result": "${RESULT}"
}
EOF_JSON

emit_event "finalize" "info" "${RESULT}" "report written to ${REPORT_JSON}"

if [[ "${RESULT}" != "pass" ]]; then
  echo "[GATE FAIL] ${BEAD_ID} runtime stub inventory verification failed"
  exit 1
fi

echo "[GATE PASS] ${BEAD_ID} runtime stub inventory is valid"
