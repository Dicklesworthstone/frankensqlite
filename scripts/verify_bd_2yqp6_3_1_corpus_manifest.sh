#!/usr/bin/env bash
# Verification gate for bd-2yqp6.3.1:
# category-complete corpus manifest floors + shard replay/hash determinism.
#
# Deterministic replay:
#   bash scripts/verify_bd_2yqp6_3_1_corpus_manifest.sh
#
# Structured logging contract:
#   emits JSONL events with trace_id/run_id/scenario_id/seed/timing/outcome
#   to artifacts/bd-2yqp6.3.1/<run_id>/events.jsonl

set -euo pipefail

BEAD_ID="bd-2yqp6.3.1"
SCENARIO_ID="PARITY-CORPUS-C1"
SHARD_ID="${SHARD_ID:-all}"
SEED="${SEED:-3520}"
TIMESTAMP_UTC="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ID="${BEAD_ID}-${SHARD_ID}-${TIMESTAMP_UTC}-${SEED}"
TRACE_ID="trace-${RUN_ID}"
ARTIFACT_DIR="artifacts/${BEAD_ID}/${RUN_ID}"
EVENTS_JSONL="${ARTIFACT_DIR}/events.jsonl"
REPORT_JSON="${ARTIFACT_DIR}/report.json"
TEST_LOG="${ARTIFACT_DIR}/cargo-test.log"
MANIFEST="corpus_manifest.toml"

mkdir -p "${ARTIFACT_DIR}"

start_ns="$(date +%s%N)"

emit_event() {
  local phase="$1"
  local event_type="$2"
  local outcome="$3"
  local message="$4"
  local now_ns elapsed_ms
  now_ns="$(date +%s%N)"
  elapsed_ms="$(( (now_ns - start_ns) / 1000000 ))"
  printf '{"trace_id":"%s","run_id":"%s","scenario_id":"%s","seed":%d,"phase":"%s","event_type":"%s","outcome":"%s","elapsed_ms":%d,"timestamp":"%s","message":"%s"}\n' \
    "${TRACE_ID}" "${RUN_ID}" "${SCENARIO_ID}" "${SEED}" "${phase}" "${event_type}" "${outcome}" "${elapsed_ms}" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "${message}" \
    >> "${EVENTS_JSONL}"
}

echo "=== ${BEAD_ID}: corpus manifest verification ==="
echo "run_id=${RUN_ID}"
echo "trace_id=${TRACE_ID}"
echo "scenario_id=${SCENARIO_ID}"
echo "shard_id=${SHARD_ID}"

emit_event "bootstrap" "start" "running" "verification started"

if [[ ! -f "${MANIFEST}" ]]; then
  emit_event "manifest_presence" "fail" "fail" "missing ${MANIFEST}"
  echo "missing ${MANIFEST}" >&2
  exit 1
fi
emit_event "manifest_presence" "pass" "pass" "${MANIFEST} exists"

emit_event "manifest_schema" "start" "running" "validating manifest schema with python tomllib"
python3 - <<'PY'
import tomllib
from pathlib import Path

manifest = tomllib.loads(Path("corpus_manifest.toml").read_text(encoding="utf-8"))

meta = manifest.get("meta", {})
for key in ("schema_version", "bead_id", "track_id", "sqlite_target", "generated_at", "contract_owner", "root_seed"):
    if key not in meta:
        raise SystemExit(f"missing meta.{key}")

floors = manifest.get("category_floors", [])
if not floors:
    raise SystemExit("missing category_floors")
for floor in floors:
    if not str(floor.get("category", "")).strip():
        raise SystemExit("empty category in category_floors")
    if int(floor.get("min_entries", 0)) <= 0:
        raise SystemExit(f"invalid floor min_entries for category {floor.get('category')}")

entries = manifest.get("entries", [])
if not entries:
    raise SystemExit("missing entries")
for entry in entries:
    if not str(entry.get("entry_id", "")).strip():
        raise SystemExit("entry with empty entry_id")
    if not str(entry.get("category", "")).strip():
        raise SystemExit(f"entry {entry.get('entry_id')} missing category")
    if bool(entry.get("in_scope", False)) and not entry.get("feature_ids"):
        raise SystemExit(f"in-scope entry missing feature_ids: {entry.get('entry_id')}")

shards = manifest.get("shards", [])
if not shards:
    raise SystemExit("missing shards")
for shard in shards:
    if not str(shard.get("shard_id", "")).strip():
        raise SystemExit("shard missing shard_id")
    if int(shard.get("seed", 0)) <= 0:
        raise SystemExit(f"invalid shard seed for {shard.get('shard_id')}")
    if not str(shard.get("bundle_hash", "")).strip():
        raise SystemExit(f"missing bundle_hash for {shard.get('shard_id')}")
PY
emit_event "manifest_schema" "pass" "pass" "manifest schema validated"

emit_event "harness_test" "start" "running" "running rch cargo test gate"
if rch exec -- cargo test -p fsqlite-harness --test bd_2yqp6_3_1_corpus_manifest -- --nocapture >"${TEST_LOG}" 2>&1; then
  emit_event "harness_test" "pass" "pass" "cargo test gate passed"
  RESULT="pass"
else
  emit_event "harness_test" "fail" "fail" "cargo test gate failed"
  RESULT="fail"
fi

manifest_sha="$(sha256sum "${MANIFEST}" | awk '{print $1}')"
test_log_sha="$(sha256sum "${TEST_LOG}" | awk '{print $1}')"

cat > "${REPORT_JSON}" <<EOF_JSON
{
  "trace_id": "${TRACE_ID}",
  "run_id": "${RUN_ID}",
  "scenario_id": "${SCENARIO_ID}",
  "seed": ${SEED},
  "shard_id": "${SHARD_ID}",
  "bead_id": "${BEAD_ID}",
  "manifest_path": "${MANIFEST}",
  "manifest_sha256": "${manifest_sha}",
  "events_jsonl": "${EVENTS_JSONL}",
  "test_log": "${TEST_LOG}",
  "test_log_sha256": "${test_log_sha}",
  "result": "${RESULT}"
}
EOF_JSON

emit_event "finalize" "info" "${RESULT}" "report written to ${REPORT_JSON}"

if [[ "${RESULT}" != "pass" ]]; then
  echo "[GATE FAIL] ${BEAD_ID} corpus-manifest contract gate failed"
  echo "See ${TEST_LOG}"
  exit 1
fi

echo "[GATE PASS] ${BEAD_ID} corpus-manifest contract is valid"
echo "Artifacts: ${ARTIFACT_DIR}"
