#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

test_name='page_cache::tests::bd_2jpu6_2_ten_concurrent_misses_coalesce_to_one_vfs_read'
cargo_command=(cargo)
if [[ "${PAGER_COALESCING_USE_RCH:-0}" == "1" ]]; then
  rch_binary="${RCH_BIN:-rch}"
  cargo_command=(
    "${rch_binary}"
    --no-self-healing
    exec
    --
    cargo
  )
fi

set +e
output="$(
  "${cargo_command[@]}" test -p fsqlite-pager --lib -j 4 "${test_name}" \
    -- --exact --nocapture 2>&1
)"
test_status=$?
set -e
printf '%s\n' "${output}"
if ((test_status != 0)); then
  exit "${test_status}"
fi

expected='PAGER_COALESCING tasks=10 vfs_reads=1 cache_admits=1'
if ! grep -Fq "${expected}" <<<"${output}"; then
  printf 'pager coalescing diagnostic missing expected measurement: %s\n' "${expected}" >&2
  exit 1
fi

printf 'pager coalescing diagnostic passed: %s\n' "${expected}"
