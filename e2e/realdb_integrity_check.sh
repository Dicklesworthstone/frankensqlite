#!/usr/bin/env bash
set -euo pipefail

WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GOLDEN_DIR_DEFAULT="${WORKSPACE_ROOT}/sample_sqlite_db_files/golden"

usage() {
  cat <<'EOF'
Usage:
  e2e/realdb_integrity_check.sh [--golden-dir PATH]

Validates that all golden SQLite DB files pass:
  - PRAGMA integrity_check == "ok"
  - PRAGMA page_count > 0
  - SELECT count(*) FROM sqlite_master > 0

Notes:
  - This script is read-only with respect to the DB files.
  - It is intended to gate the E2E RealDB suite (bead: bd-3bsz).
EOF
}

golden_dir="${GOLDEN_DIR_DEFAULT}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --golden-dir)
      golden_dir="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown arg: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ ! -d "${golden_dir}" ]]; then
  echo "Golden dir does not exist: ${golden_dir}" >&2
  exit 1
fi

shopt -s nullglob
db_files=("${golden_dir}"/*.db "${golden_dir}"/*.sqlite "${golden_dir}"/*.sqlite3)
shopt -u nullglob

if [[ "${#db_files[@]}" -eq 0 ]]; then
  echo "No DB files found under: ${golden_dir}" >&2
  exit 1
fi

failures=0
checked=0

for db in "${db_files[@]}"; do
  checked=$((checked + 1))

  integrity="$(sqlite3 "${db}" "PRAGMA integrity_check;" | tr -d '\r' | tail -n 1)"
  if [[ "${integrity}" != "ok" ]]; then
    echo "FAIL integrity_check: ${db}" >&2
    sqlite3 "${db}" "PRAGMA integrity_check;" >&2 || true
    failures=$((failures + 1))
    continue
  fi

  page_count="$(sqlite3 "${db}" "PRAGMA page_count;" | tr -d '\r' | tail -n 1)"
  if [[ -z "${page_count}" ]] || [[ "${page_count}" -le 0 ]]; then
    echo "FAIL page_count: ${db} page_count=${page_count:-<empty>}" >&2
    failures=$((failures + 1))
    continue
  fi

  master_count="$(sqlite3 "${db}" "SELECT count(*) FROM sqlite_master;" | tr -d '\r' | tail -n 1)"
  if [[ -z "${master_count}" ]] || [[ "${master_count}" -le 0 ]]; then
    echo "FAIL sqlite_master count: ${db} sqlite_master_count=${master_count:-<empty>}" >&2
    failures=$((failures + 1))
    continue
  fi
done

if [[ "${failures}" -ne 0 ]]; then
  echo "Integrity gate FAILED: ${failures}/${checked} database(s) failed." >&2
  exit 1
fi

echo "Integrity gate OK: ${checked} database(s) passed."

