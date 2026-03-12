#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Build the fsqlite-wasm crate into a publishable npm artifact.

Usage:
  scripts/build_fsqlite_wasm_package.sh [OUT_DIR]

Environment:
  FSQLITE_WASM_TARGET     wasm-pack target: bundler | web | nodejs | deno | no-modules
                          default: bundler
  FSQLITE_WASM_MODE       wasm-pack install mode: normal | no-install | force
                          default: normal
  FSQLITE_WASM_SCOPE      temporary wasm-pack npm scope before normalization
                          default: frankensqlite
  FSQLITE_WASM_PKG_NAME   final npm package name
                          default: @frankensqlite/core
  FSQLITE_WASM_OUT_NAME   generated file stem
                          default: frankensqlite_wasm
  FSQLITE_WASM_PROFILE    wasm-pack profile: release | dev | profiling
                          default: release
  FSQLITE_WASM_MAX_PACKED_BYTES
                          max packed npm tarball size in bytes
                          default: 2097152 (2 MiB); set to 0 to disable

The default output directory is target/fsqlite-wasm-pkg.
EOF
}

require_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "Missing required command: $1" >&2
        exit 1
    fi
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    usage
    exit 0
fi

require_cmd wasm-pack
require_cmd jq
require_cmd npm
require_cmd realpath

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
crate_dir="${repo_root}/crates/fsqlite-wasm"
out_dir_input="${1:-${repo_root}/target/fsqlite-wasm-pkg}"
out_dir="$(realpath -m "${out_dir_input}")"
target="${FSQLITE_WASM_TARGET:-bundler}"
mode="${FSQLITE_WASM_MODE:-normal}"
scope="${FSQLITE_WASM_SCOPE:-frankensqlite}"
package_name="${FSQLITE_WASM_PKG_NAME:-@frankensqlite/core}"
out_name="${FSQLITE_WASM_OUT_NAME:-frankensqlite_wasm}"
profile="${FSQLITE_WASM_PROFILE:-release}"
max_packed_bytes="${FSQLITE_WASM_MAX_PACKED_BYTES:-2097152}"

required_files=(
    "${out_name}_bg.wasm"
    "${out_name}.js"
    "${out_name}.d.ts"
)

if [[ ! "${max_packed_bytes}" =~ ^[0-9]+$ ]]; then
    echo "FSQLITE_WASM_MAX_PACKED_BYTES must be an integer number of bytes" >&2
    exit 1
fi

case "${profile}" in
    release) profile_flag="--release" ;;
    dev) profile_flag="--dev" ;;
    profiling) profile_flag="--profiling" ;;
    *)
        echo "Unsupported FSQLITE_WASM_PROFILE: ${profile}" >&2
        exit 1
        ;;
esac

case "${mode}" in
    normal|no-install|force) ;;
    *)
        echo "Unsupported FSQLITE_WASM_MODE: ${mode}" >&2
        exit 1
        ;;
esac

mkdir -p "${out_dir}"
out_dir_rel="$(realpath -m --relative-to "${crate_dir}" "${out_dir}")"

pushd "${crate_dir}" >/dev/null
wasm-pack build . \
    --target "${target}" \
    --mode "${mode}" \
    --scope "${scope}" \
    --out-dir "${out_dir_rel}" \
    --out-name "${out_name}" \
    "${profile_flag}"
popd >/dev/null

if [[ -f "${crate_dir}/README.md" && ! -f "${out_dir}/README.md" ]]; then
    cp "${crate_dir}/README.md" "${out_dir}/README.md"
fi

if [[ -f "${repo_root}/LICENSE" && ! -f "${out_dir}/LICENSE" ]]; then
    cp "${repo_root}/LICENSE" "${out_dir}/LICENSE"
fi

tmp_json="$(mktemp)"
jq \
    --arg name "${package_name}" \
    --arg description "FrankenSQLite — concurrent-writer SQLite in the browser via WebAssembly" \
    --arg main "${out_name}.js" \
    --arg types "${out_name}.d.ts" \
    --arg wasm "${out_name}_bg.wasm" \
    '
    .name = $name |
    .description = $description |
    .type = "module" |
    .main = $main |
    .module = $main |
    .types = $types |
    .exports = {
      ".": {
        "import": ("./" + $main),
        "default": ("./" + $main),
        "types": ("./" + $types)
      }
    } |
    .files = [
      $wasm,
      $main,
      $types,
      "snippets/",
      "README.md",
      "LICENSE"
    ] |
    .sideEffects = ["./snippets/*"] |
    .keywords = ["sqlite", "wasm", "webassembly", "database", "sql", "mvcc"] |
    .license = "SEE LICENSE IN LICENSE" |
    .repository = {
      "type": "git",
      "url": "https://github.com/Dicklesworthstone/frankensqlite"
    } |
    .publishConfig = { "access": "public" }
    ' "${out_dir}/package.json" > "${tmp_json}"
mv "${tmp_json}" "${out_dir}/package.json"

for required in "${required_files[@]}"; do
    if [[ ! -f "${out_dir}/${required}" ]]; then
        echo "Missing expected wasm package artifact: ${required}" >&2
        exit 1
    fi
done

if command -v find >/dev/null 2>&1; then
    echo "Generated package files:"
    find "${out_dir}" -maxdepth 2 -type f | sort | sed 's#^#  - #'
fi

packed_file="$(npm pack "${out_dir}" --pack-destination "${out_dir}")"
packed_path="${out_dir}/${packed_file}"

if [[ ! -f "${packed_path}" ]]; then
    echo "npm pack did not produce an archive in ${out_dir}" >&2
    exit 1
fi

packed_bytes="$(wc -c < "${packed_path}" | tr -d '[:space:]')"
if [[ ! "${packed_bytes}" =~ ^[0-9]+$ ]]; then
    echo "Unable to determine packed archive size for ${packed_path}" >&2
    exit 1
fi

if [[ "${max_packed_bytes}" != "0" ]] && (( packed_bytes > max_packed_bytes )); then
    echo "Packed wasm npm artifact exceeds size budget: ${packed_bytes} > ${max_packed_bytes} bytes" >&2
    exit 1
fi

echo "Packed npm artifact: ${packed_file} (${packed_bytes} bytes)"
echo "Packed npm artifact into ${out_dir}"
