#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-human}"
if [[ "${MODE}" != "human" && "${MODE}" != "--json" ]]; then
  printf 'usage: %s [--json]\n' "$0" >&2
  exit 64
fi

ROOT=$(git rev-parse --show-toplevel)
cd "${ROOT}"

run_cargo() {
  if [[ "${FSQLITE_USE_RCH:-0}" == "1" ]]; then
    rch exec -- cargo "$@"
  else
    cargo "$@"
  fi
}

if ! TEST_OUTPUT=$(run_cargo test -p fsqlite-harness --lib \
  'canonical_parity_contract::tests::workspace_authority_report_is_valid_stable_and_machine_readable' \
  -- --exact --nocapture --test-threads=1 2>&1); then
  printf '%s\n' "${TEST_OUTPUT}" >&2
  exit 1
fi
printf '%s\n' "${TEST_OUTPUT}" >&2
REPORT_JSON=$(printf '%s\n' "${TEST_OUTPUT}" |
  sed -n 's/^.*FSQLITE_CONTRACT_AUTHORITY_REPORT=//p' | tail -n 1)
if [[ -z "${REPORT_JSON}" ]]; then
  printf 'ERROR: canonical authority test emitted no machine-readable report\n' >&2
  exit 1
fi

run_cargo test -p fsqlite-harness --test parity_taxonomy_test -- --test-threads=1 >&2

REPORT_JSON="${REPORT_JSON}" python3 - "${ROOT}" "${MODE}" <<'PY'
import json
import os
import re
import subprocess
import sys
import tomllib
from pathlib import Path

root = Path(sys.argv[1])
mode = sys.argv[2]
authority_report = json.loads(os.environ["REPORT_JSON"])
inventory_path = root / "docs/contracts/turso_test_adaptation_inventory.toml"
inventory = tomllib.loads(inventory_path.read_text(encoding="utf-8"))

approved = {}
inventory_authorities = {
    authority["logical_name"]: authority
    for authority in inventory.get("contract_authority", [])
}
for authority in inventory_authorities.values():
    for reference in authority.get("root_reference", []):
        approved.setdefault(reference["path"], []).append(reference)

tracked = subprocess.run(
    ["git", "ls-files", "-z"],
    cwd=root,
    check=True,
    capture_output=True,
).stdout.decode("utf-8").split("\0")
guard_path = "scripts/verify_canonical_contract_authority.sh"
if guard_path not in tracked:
    tracked.append(guard_path)

authorities = {
    Path(item["canonical_path"]).name: item for item in authority_report["authorities"]
}
consumer_references = []
failures = list(authority_report.get("diagnostics", []))
allowed_suffixes = {
    ".rs",
    ".sh",
    ".bash",
    ".py",
    ".js",
    ".mjs",
    ".cjs",
    ".ts",
    ".tsx",
    ".go",
    ".yml",
    ".yaml",
}

reported_logical_names = {
    item["logical_name"] for item in authority_report["authorities"]
}
for unexpected in sorted(set(inventory_authorities) - reported_logical_names):
    failures.append(
        {
            "code": "unexpected_inventory_authority",
            "message": f"inventory authority {unexpected} is absent from the runtime registry",
        }
    )

for logical_name, authority in sorted(
    (item["logical_name"], item) for item in authority_report["authorities"]
):
    inventoried = inventory_authorities.get(logical_name)
    if inventoried is None:
        failures.append(
            {
                "code": "authority_missing_from_intake_inventory",
                "message": f"{logical_name} has no contract_authority inventory row",
            }
        )
        continue
    for field in ("canonical_path", "canonical_sha256"):
        if inventoried.get(field) != authority[field]:
            failures.append(
                {
                    "code": "authority_inventory_drift",
                    "message": (
                        f"{logical_name} {field} inventory value "
                        f"{inventoried.get(field)!r} does not match {authority[field]!r}"
                    ),
                }
            )
    if inventoried.get("root_duplicate_path") != authority["inert_root_path"]:
        failures.append(
            {
                "code": "root_pointer_inventory_drift",
                "message": (
                    f"{logical_name} root_duplicate_path "
                    f"{inventoried.get('root_duplicate_path')!r} does not match "
                    f"{authority['inert_root_path']!r}"
                ),
            }
        )
    if inventoried.get("live_root_consumers") != []:
        failures.append(
            {
                "code": "live_root_consumer_inventory_not_empty",
                "message": f"{logical_name} declares live root consumers",
            }
        )

for relative in sorted(filter(None, tracked)):
    path = root / relative
    if not path.is_file() or path.suffix not in allowed_suffixes:
        continue
    text = path.read_text(encoding="utf-8", errors="replace")
    for line_number, line in enumerate(text.splitlines(), start=1):
        for basename, authority in authorities.items():
            if basename not in line:
                continue
            classification = None
            if authority["canonical_path"] in line:
                classification = "canonical_path"
            else:
                for expected in approved.get(relative, []):
                    if (
                        expected.get("resolves_repository_root") is False
                        and expected["anchor"] in line
                    ):
                        classification = expected["classification"]
                        break
            if classification is None:
                exact_literal = re.search(
                    rf"(?P<quote>['\"]){re.escape(basename)}(?P=quote)", line
                )
                root_relative_literal = re.search(
                    rf"(?P<quote>['\"])\./{re.escape(basename)}(?P=quote)", line
                )
                interpolated_root_path = re.search(
                    rf"(?:\$\{{?(?:ROOT|WORKSPACE_ROOT)\}}?|workspace_root)/{re.escape(basename)}",
                    line,
                )
                if relative.startswith(".github/workflows/") and re.search(
                    rf"^\s*-\s*['\"]{re.escape(basename)}['\"]\s*$", line
                ):
                    classification = "workflow_path_trigger"
                elif (
                    relative
                    == "crates/fsqlite-harness/src/canonical_parity_contract.rs"
                    and re.search(
                        rf"inert_root_path:\s*['\"]{re.escape(basename)}['\"]", line
                    )
                ):
                    classification = "inert_root_registry_field"
                elif (
                    relative
                    == "crates/fsqlite-harness/src/canonical_parity_contract.rs"
                    and re.search(
                        rf"Some\(['\"]{re.escape(basename)}['\"]\)", line
                    )
                ):
                    classification = "canonical_resolution_unit_test"
                elif re.search(
                    rf"contracts_dir\s*/\s*['\"]{re.escape(basename)}['\"]", line
                ):
                    classification = "canonical_contracts_directory_join"
                elif (
                    relative
                    == "crates/fsqlite-harness/tests/bd_2yqp6_1_4_parity_score_contract.rs"
                    and exact_literal
                ):
                    classification = "canonical_relative_contract_reference_test"
                elif (
                    exact_literal is None
                    and root_relative_literal is None
                    and interpolated_root_path is None
                ):
                    classification = "non_reader_reference"
            record = {
                "path": relative,
                "line": line_number,
                "logical_name": authority["logical_name"],
                "classification": classification or "unregistered_bare_reference",
            }
            consumer_references.append(record)
            if classification is None:
                failures.append(
                    {
                        "code": "unregistered_bare_contract_reference",
                        "message": (
                            f"{relative}:{line_number} contains unregistered bare "
                            f"contract reference {basename}"
                        ),
                    }
                )

result = {
    "schema_version": "fsqlite.canonical_contract_authority_e2e.v1",
    "status": "pass" if not failures else "fail",
    "inventory_path": str(inventory_path.relative_to(root)),
    "inventory_authorities": inventory_authorities,
    "authority_report": authority_report,
    "consumer_references": consumer_references,
    "failures": failures,
}

if mode == "--json":
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
else:
    print("=== Canonical Contract Authority ===")
    print(f"Status: {result['status'].upper()}")
    print(f"Authorities: {len(authority_report['authorities'])}")
    print(f"Specialized constants: {len(authority_report['specialized_constants'])}")
    print(f"Classified consumer references: {len(consumer_references)}")
    print(f"Failures: {len(failures)}")
    for failure in failures:
        print(f"ERROR [{failure['code']}]: {failure['message']}", file=sys.stderr)

if failures:
    raise SystemExit(1)
PY
