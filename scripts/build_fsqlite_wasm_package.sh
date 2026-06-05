#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Build the fsqlite-wasm crate into a publishable npm artifact.

Usage:
  scripts/build_fsqlite_wasm_package.sh [OUT_DIR]

Environment:
  FSQLITE_WASM_TARGET     wasm-pack target: bundler | web | nodejs | deno | no-modules
                          default: web
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
  FSQLITE_WASM_PACKAGE_ONLY
                          skip wasm-pack and postprocess an existing OUT_DIR
                          default: 0
  FSQLITE_WASM_FORBID_LOCAL_BUILD
                          fail before wasm-pack unless package-only mode is used
                          default: 0
  FSQLITE_WASM_NO_DEFAULT_FEATURES
                          pass --no-default-features to cargo via wasm-pack
                          default: 0
  FSQLITE_WASM_FEATURES   comma-separated cargo features to enable
                          default: empty
  FSQLITE_WASM_TWIGGY     twiggy size report mode: auto | required | disabled
                          default: auto
  FSQLITE_WASM_WASM_OPT   wasm-opt mode: auto | required | disabled
                          default: required for release/profiling, auto for dev
  FSQLITE_WASM_WASM_OPT_FLAGS
                          whitespace-separated wasm-opt flags
                          default: -Oz plus Rust wasm feature enables
  FSQLITE_WASM_STRIP_LOCATION_DETAIL
                          pass -Zlocation-detail=none for release/profiling builds
                          default: 1 for release/profiling, 0 for dev
  FSQLITE_WASM_MAX_GZIP_BYTES
                          max gzipped .wasm size in bytes
                          default: 800000 (800 KB); set to 0 to disable
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

gzip_size_bytes() {
    local path="$1"
    gzip -cn "${path}" | wc -c | tr -d '[:space:]'
}

json_number_or_null() {
    local value="$1"
    if [[ -n "${value}" ]]; then
        printf '%s' "${value}"
    else
        printf 'null'
    fi
}

wasm_opt_kept=""
wasm_opt_original_gzip_bytes=""
wasm_opt_optimized_gzip_bytes=""

select_wasm_opt_output() {
    local original_path="$1"
    local optimized_path="$2"

    wasm_opt_original_gzip_bytes="$(gzip_size_bytes "${original_path}")"
    wasm_opt_optimized_gzip_bytes="$(gzip_size_bytes "${optimized_path}")"
    if [[ ! "${wasm_opt_original_gzip_bytes}" =~ ^[0-9]+$ ]]; then
        echo "Unable to determine gzipped wasm size for ${original_path}" >&2
        exit 1
    fi
    if [[ ! "${wasm_opt_optimized_gzip_bytes}" =~ ^[0-9]+$ ]]; then
        echo "Unable to determine gzipped wasm size for ${optimized_path}" >&2
        exit 1
    fi

    if (( wasm_opt_optimized_gzip_bytes <= wasm_opt_original_gzip_bytes )); then
        mv "${optimized_path}" "${original_path}"
        wasm_opt_kept="optimized"
    else
        rm -f -- "${optimized_path}"
        wasm_opt_kept="original"
    fi
}

normalize_package_json() {
    jq \
        --arg name "${package_name}" \
        --arg description "FrankenSQLite — concurrent-writer SQLite in the browser via WebAssembly" \
        --arg version "0.1.0" \
        --arg main "${out_name}.js" \
        --arg types "${out_name}.d.ts" \
        --arg wasm "${out_name}_bg.wasm" \
        '
        .name = $name |
        .version = (.version // $version) |
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
        ' "$@"
}

write_size_report() {
    local packed_file_arg="${1:-}"
    local packed_bytes_arg="${2:-}"
    local package_only_json="false"
    local strip_location_detail_json="false"
    local build_target="${target}"
    local twiggy_report=""

    if [[ "${package_only}" == "1" ]]; then
        package_only_json="true"
        build_target="prebuilt"
        strip_location_detail_json="null"
    elif [[ "${strip_location_detail}" == "1" ]]; then
        strip_location_detail_json="true"
    fi
    if [[ -f "${out_dir}/twiggy-top.txt" ]]; then
        twiggy_report="twiggy-top.txt"
    fi

    jq -n \
        --arg packageName "${package_name}" \
        --arg outName "${out_name}" \
        --arg buildTarget "${build_target}" \
        --arg profile "${profile}" \
        --arg wasmOptMode "${wasm_opt_mode}" \
        --arg wasmOptKept "${wasm_opt_kept}" \
        --arg packedArchive "${packed_file_arg}" \
        --arg twiggyReport "${twiggy_report}" \
        --argjson packageOnly "${package_only_json}" \
        --argjson stripLocationDetail "${strip_location_detail_json}" \
        --argjson wasmBytes "$(json_number_or_null "${wasm_bytes}")" \
        --argjson wasmGzipBytes "$(json_number_or_null "${gzip_bytes}")" \
        --argjson maxGzipBytes "${max_gzip_bytes}" \
        --argjson packedBytes "$(json_number_or_null "${packed_bytes_arg}")" \
        --argjson maxPackedBytes "${max_packed_bytes}" \
        --argjson wasmOptOriginalGzipBytes "$(json_number_or_null "${wasm_opt_original_gzip_bytes}")" \
        --argjson wasmOptOptimizedGzipBytes "$(json_number_or_null "${wasm_opt_optimized_gzip_bytes}")" \
        '{
          packageName: $packageName,
          outName: $outName,
          packageOnly: $packageOnly,
          buildTarget: $buildTarget,
          profile: $profile,
          stripLocationDetail: $stripLocationDetail,
          wasmOpt: {
            mode: $wasmOptMode,
            kept: (if $wasmOptKept == "" then null else $wasmOptKept end),
            originalGzipBytes: $wasmOptOriginalGzipBytes,
            optimizedGzipBytes: $wasmOptOptimizedGzipBytes
          },
          wasmBytes: $wasmBytes,
          wasmGzipBytes: $wasmGzipBytes,
          maxGzipBytes: $maxGzipBytes,
          gzipBudgetPass: (if $maxGzipBytes == 0 then true else $wasmGzipBytes <= $maxGzipBytes end),
          packedArchive: (if $packedArchive == "" then null else $packedArchive end),
          packedBytes: $packedBytes,
          maxPackedBytes: $maxPackedBytes,
          packedBudgetPass: (
            if $packedBytes == null then null
            elif $maxPackedBytes == 0 then true
            else $packedBytes <= $maxPackedBytes
            end
          ),
          twiggyReport: (if $twiggyReport == "" then null else $twiggyReport end)
        }' > "${size_report_path}"
}

write_twiggy_report() {
    case "${twiggy_mode}" in
        auto)
            if command -v twiggy >/dev/null 2>&1; then
                twiggy top "${out_dir}/${out_name}_bg.wasm" > "${out_dir}/twiggy-top.txt"
            fi
            ;;
        required)
            require_cmd twiggy
            twiggy top "${out_dir}/${out_name}_bg.wasm" > "${out_dir}/twiggy-top.txt"
            ;;
        disabled) ;;
    esac
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    usage
    exit 0
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
crate_dir="${repo_root}/crates/fsqlite-wasm"
out_dir_input="${1:-${repo_root}/target/fsqlite-wasm-pkg}"
require_cmd realpath
out_dir="$(realpath -m "${out_dir_input}")"
target="${FSQLITE_WASM_TARGET:-web}"
mode="${FSQLITE_WASM_MODE:-normal}"
scope="${FSQLITE_WASM_SCOPE:-frankensqlite}"
package_name="${FSQLITE_WASM_PKG_NAME:-@frankensqlite/core}"
out_name="${FSQLITE_WASM_OUT_NAME:-frankensqlite_wasm}"
profile="${FSQLITE_WASM_PROFILE:-release}"
package_only="${FSQLITE_WASM_PACKAGE_ONLY:-0}"
forbid_local_build="${FSQLITE_WASM_FORBID_LOCAL_BUILD:-0}"
no_default_features="${FSQLITE_WASM_NO_DEFAULT_FEATURES:-0}"
features="${FSQLITE_WASM_FEATURES:-}"
twiggy_mode="${FSQLITE_WASM_TWIGGY:-auto}"
if [[ -n "${FSQLITE_WASM_WASM_OPT:-}" ]]; then
    wasm_opt_mode="${FSQLITE_WASM_WASM_OPT}"
elif [[ "${profile}" == "dev" ]]; then
    wasm_opt_mode="auto"
else
    wasm_opt_mode="required"
fi
wasm_opt_flags_string="${FSQLITE_WASM_WASM_OPT_FLAGS:--Oz --enable-bulk-memory --enable-bulk-memory-opt --enable-nontrapping-float-to-int --strip-dwarf --strip-producers}"
if [[ -n "${FSQLITE_WASM_STRIP_LOCATION_DETAIL:-}" ]]; then
    strip_location_detail="${FSQLITE_WASM_STRIP_LOCATION_DETAIL}"
elif [[ "${profile}" == "dev" ]]; then
    strip_location_detail="0"
else
    strip_location_detail="1"
fi
max_gzip_bytes="${FSQLITE_WASM_MAX_GZIP_BYTES:-800000}"
max_packed_bytes="${FSQLITE_WASM_MAX_PACKED_BYTES:-2097152}"
size_report_path="${out_dir}/size-report.json"

required_files=(
    "${out_name}_bg.wasm"
    "${out_name}.js"
    "${out_name}.d.ts"
)

if [[ ! "${max_packed_bytes}" =~ ^[0-9]+$ ]]; then
    echo "FSQLITE_WASM_MAX_PACKED_BYTES must be an integer number of bytes" >&2
    exit 1
fi

if [[ ! "${max_gzip_bytes}" =~ ^[0-9]+$ ]]; then
    echo "FSQLITE_WASM_MAX_GZIP_BYTES must be an integer number of bytes" >&2
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

case "${package_only}" in
    0|1) ;;
    *)
        echo "FSQLITE_WASM_PACKAGE_ONLY must be 0 or 1" >&2
        exit 1
        ;;
esac

case "${forbid_local_build}" in
    0|1) ;;
    *)
        echo "FSQLITE_WASM_FORBID_LOCAL_BUILD must be 0 or 1" >&2
        exit 1
        ;;
esac

case "${no_default_features}" in
    0|1) ;;
    *)
        echo "FSQLITE_WASM_NO_DEFAULT_FEATURES must be 0 or 1" >&2
        exit 1
        ;;
esac

case "${twiggy_mode}" in
    auto|required|disabled) ;;
    *)
        echo "Unsupported FSQLITE_WASM_TWIGGY: ${twiggy_mode}" >&2
        exit 1
        ;;
esac

case "${wasm_opt_mode}" in
    auto|required|disabled) ;;
    *)
        echo "Unsupported FSQLITE_WASM_WASM_OPT: ${wasm_opt_mode}" >&2
        exit 1
        ;;
esac

case "${strip_location_detail}" in
    0|1) ;;
    *)
        echo "FSQLITE_WASM_STRIP_LOCATION_DETAIL must be 0 or 1" >&2
        exit 1
        ;;
esac

if [[ "${package_only}" == "1" ]]; then
    if [[ -n "${FSQLITE_WASM_TARGET+x}" ]]; then
        echo "FSQLITE_WASM_PACKAGE_ONLY=1 cannot apply FSQLITE_WASM_TARGET=${FSQLITE_WASM_TARGET}." >&2
        echo "Build that target through cargo/wasm-pack first, then postprocess the output directory." >&2
        exit 1
    fi
    if [[ -n "${FSQLITE_WASM_MODE+x}" ]]; then
        echo "FSQLITE_WASM_PACKAGE_ONLY=1 cannot apply FSQLITE_WASM_MODE=${FSQLITE_WASM_MODE}." >&2
        echo "Build that mode through cargo/wasm-pack first, then postprocess the output directory." >&2
        exit 1
    fi
    if [[ -n "${FSQLITE_WASM_SCOPE+x}" ]]; then
        echo "FSQLITE_WASM_PACKAGE_ONLY=1 cannot apply FSQLITE_WASM_SCOPE=${FSQLITE_WASM_SCOPE}." >&2
        echo "Build that temporary wasm-pack scope first, then postprocess the output directory." >&2
        exit 1
    fi
    if [[ -n "${FSQLITE_WASM_PROFILE+x}" ]]; then
        echo "FSQLITE_WASM_PACKAGE_ONLY=1 cannot apply FSQLITE_WASM_PROFILE=${FSQLITE_WASM_PROFILE}." >&2
        echo "Build that profile through cargo/wasm-pack first, then postprocess the output directory." >&2
        exit 1
    fi
    if [[ -n "${FSQLITE_WASM_STRIP_LOCATION_DETAIL+x}" ]]; then
        echo "FSQLITE_WASM_PACKAGE_ONLY=1 cannot apply FSQLITE_WASM_STRIP_LOCATION_DETAIL=${FSQLITE_WASM_STRIP_LOCATION_DETAIL}." >&2
        echo "Build with that Rust flag first, then postprocess the output directory." >&2
        exit 1
    fi
    if [[ "${no_default_features}" == "1" ]]; then
        echo "FSQLITE_WASM_PACKAGE_ONLY=1 cannot apply FSQLITE_WASM_NO_DEFAULT_FEATURES." >&2
        echo "Build that feature set through cargo/wasm-pack first, then postprocess the output directory." >&2
        exit 1
    fi
    if [[ -n "${features}" ]]; then
        echo "FSQLITE_WASM_PACKAGE_ONLY=1 cannot apply FSQLITE_WASM_FEATURES=${features}." >&2
        echo "Build that feature set through cargo/wasm-pack first, then postprocess the output directory." >&2
        exit 1
    fi
fi

if [[ "${package_only}" == "0" ]]; then
    if [[ "${forbid_local_build}" == "1" ]]; then
        echo "FSQLITE_WASM_FORBID_LOCAL_BUILD=1 refuses to run wasm-pack locally." >&2
        echo "Use an RCH/CI-produced wasm-bindgen output directory with FSQLITE_WASM_PACKAGE_ONLY=1." >&2
        exit 1
    fi
    require_cmd wasm-pack
fi
require_cmd jq
require_cmd npm
require_cmd gzip

cargo_feature_flags=()
if [[ "${no_default_features}" == "1" ]]; then
    cargo_feature_flags+=(--no-default-features)
fi
if [[ -n "${features}" ]]; then
    cargo_feature_flags+=(--features "${features}")
fi

if [[ "${package_only}" == "1" ]]; then
    if [[ ! -d "${out_dir}" ]]; then
        echo "FSQLITE_WASM_PACKAGE_ONLY=1 requires an existing output directory: ${out_dir}" >&2
        exit 1
    fi
    echo "FSQLITE_WASM_PACKAGE_ONLY=1: skipping wasm-pack build and postprocessing ${out_dir}"
else
    mkdir -p "${out_dir}"
    out_dir_rel="$(realpath -m --relative-to "${crate_dir}" "${out_dir}")"

    if [[ "${strip_location_detail}" == "1" ]]; then
        export RUSTFLAGS="${RUSTFLAGS:+${RUSTFLAGS} }-Zlocation-detail=none"
    fi

    pushd "${crate_dir}" >/dev/null
    wasm-pack build . \
        --target "${target}" \
        --mode "${mode}" \
        --scope "${scope}" \
        --out-dir "${out_dir_rel}" \
        --out-name "${out_name}" \
        --no-opt \
        "${profile_flag}" \
        "${cargo_feature_flags[@]}"
    popd >/dev/null
fi

if [[ -f "${crate_dir}/README.md" && ! -f "${out_dir}/README.md" ]]; then
    cp "${crate_dir}/README.md" "${out_dir}/README.md"
fi

if [[ -f "${repo_root}/LICENSE" && ! -f "${out_dir}/LICENSE" ]]; then
    cp "${repo_root}/LICENSE" "${out_dir}/LICENSE"
fi

tmp_json="$(mktemp)"
if [[ -f "${out_dir}/package.json" ]]; then
    package_json_source="${out_dir}/package.json"
else
    package_json_source="/dev/stdin"
fi

if [[ "${package_json_source}" == "/dev/stdin" ]]; then
    printf '{}\n' | normalize_package_json > "${tmp_json}"
else
    normalize_package_json "${package_json_source}" > "${tmp_json}"
fi
mv "${tmp_json}" "${out_dir}/package.json"

for required in "${required_files[@]}"; do
    if [[ ! -f "${out_dir}/${required}" ]]; then
        echo "Missing expected wasm package artifact: ${required}" >&2
        exit 1
    fi
done

wasm_path="${out_dir}/${out_name}_bg.wasm"
case "${wasm_opt_mode}" in
    auto)
        if command -v wasm-opt >/dev/null 2>&1; then
            read -r -a wasm_opt_flags <<< "${wasm_opt_flags_string}"
            wasm_opt_path="${wasm_path}.opt"
            wasm-opt "${wasm_opt_flags[@]}" "${wasm_path}" -o "${wasm_opt_path}"
            select_wasm_opt_output "${wasm_path}" "${wasm_opt_path}"
        else
            echo "wasm-opt not found; skipping optional optimization" >&2
        fi
        ;;
    required)
        require_cmd wasm-opt
        read -r -a wasm_opt_flags <<< "${wasm_opt_flags_string}"
        wasm_opt_path="${wasm_path}.opt"
        wasm-opt "${wasm_opt_flags[@]}" "${wasm_path}" -o "${wasm_opt_path}"
        select_wasm_opt_output "${wasm_path}" "${wasm_opt_path}"
        ;;
    disabled) ;;
esac

gzip_path="${wasm_path}.gz"
wasm_bytes="$(wc -c < "${wasm_path}" | tr -d '[:space:]')"
if [[ ! "${wasm_bytes}" =~ ^[0-9]+$ ]]; then
    echo "Unable to determine wasm size for ${wasm_path}" >&2
    exit 1
fi
gzip -cn "${wasm_path}" > "${gzip_path}"
gzip_bytes="$(wc -c < "${gzip_path}" | tr -d '[:space:]')"
if [[ ! "${gzip_bytes}" =~ ^[0-9]+$ ]]; then
    echo "Unable to determine gzipped wasm size for ${gzip_path}" >&2
    exit 1
fi
if [[ -n "${wasm_opt_kept}" ]]; then
    echo "wasm-opt gzip comparison: original=${wasm_opt_original_gzip_bytes} optimized=${wasm_opt_optimized_gzip_bytes} kept=${wasm_opt_kept}"
fi
write_twiggy_report
if [[ "${max_gzip_bytes}" != "0" ]] && (( gzip_bytes > max_gzip_bytes )); then
    write_size_report
    echo "Gzipped wasm artifact exceeds size budget: ${gzip_bytes} > ${max_gzip_bytes} bytes" >&2
    exit 1
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

write_size_report "${packed_file}" "${packed_bytes}"

if [[ "${max_packed_bytes}" != "0" ]] && (( packed_bytes > max_packed_bytes )); then
    echo "Packed wasm npm artifact exceeds size budget: ${packed_bytes} > ${max_packed_bytes} bytes" >&2
    exit 1
fi

if command -v find >/dev/null 2>&1; then
    echo "Generated package files:"
    find "${out_dir}" -maxdepth 2 -type f | sort | sed 's#^#  - #'
fi

echo "Packed npm artifact: ${packed_file} (${packed_bytes} bytes)"
echo "Gzipped wasm artifact: $(basename "${gzip_path}") (${gzip_bytes} bytes)"
echo "Size report: $(basename "${size_report_path}")"
echo "Packed npm artifact into ${out_dir}"
