#!/usr/bin/env bash
#
# Run the Gate 0 async-bridge matrix with frozen binaries and retained receipts.
#
# The runner derives its topology receipt from live kernel state. It never
# accepts caller-authored verification booleans. The selected CPUs must live in
# an isolated cgroup-v2 cpuset partition whose exclusive CPU set covers every
# online SMT sibling of the selected physical cores. Full-dynticks coverage,
# IRQ disjointness, exact per-thread affinity, stable frequency policy, load,
# pressure, boot identity, single-node memory placement, unlimited ancestor CPU
# quota, and cgroup identity are rechecked around every run. The entire
# isolated partition tree must contain only descendants of this runner.
# Every inner bridge report remains explicitly diagnostic-only. The final
# analysis is a fail-closed outer-protocol candidate receipt, not permission to
# publish an uncited numeric claim.

set -Eeuo pipefail
umask 077

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
readonly SCRIPT_DIR
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd -P)"
readonly REPO_ROOT

die() {
    printf 'gate0-async-bridge: ERROR: %s\n' "$*" >&2
    exit 1
}

usage() {
    cat <<'USAGE'
Usage:
  scripts/run_gate0_async_bridge.sh \
    --artifact-dir /absolute/new/artifact-directory \
    --build-root /absolute/new/build-directory \
    --taskset-cpus 2,3 \
    --monitor-cpu 4 \
    [--seed-base 2026072601] [--seed-count 20] \
    [--samples 48] [--operations 1000] \
    [--run-timeout-seconds 1800] [--build-timeout-seconds 7200] \
    [--max-load-average-1m 1.0] \
    [--aa-max-abs-log-ratio 0.05] \
    [--cas-publisher /absolute/publisher] [--require-cas]

The CAS publisher, when configured, is invoked as:
  publisher <absolute-file> <sha256> <artifact-kind>

It must emit exactly one JSON object with matching `sha256` and a nonempty
content-addressed `uri`. The runner never evaluates publisher output as shell.

The artifact and build directories must not already exist. The source checkout
must be clean, on main, and exactly match a fresh `git ls-remote` result for
origin/main.
USAGE
}

artifact_dir=
build_root=
taskset_cpu_list=
monitor_cpu=
seed_base=2026072601
seed_count=20
samples_per_arm=48
operation_count=1000
run_timeout_seconds=1800
build_timeout_seconds=7200
max_load_average_1m=1.0
aa_max_abs_log_ratio=0.05
cas_publisher=
require_cas=false

while (($# > 0)); do
    case "$1" in
        --artifact-dir)
            (($# >= 2)) || die "--artifact-dir requires a value"
            artifact_dir=$2
            shift 2
            ;;
        --build-root)
            (($# >= 2)) || die "--build-root requires a value"
            build_root=$2
            shift 2
            ;;
        --taskset-cpus)
            (($# >= 2)) || die "--taskset-cpus requires a value"
            taskset_cpu_list=$2
            shift 2
            ;;
        --monitor-cpu)
            (($# >= 2)) || die "--monitor-cpu requires a value"
            monitor_cpu=$2
            shift 2
            ;;
        --seed-base)
            (($# >= 2)) || die "--seed-base requires a value"
            seed_base=$2
            shift 2
            ;;
        --seed-count)
            (($# >= 2)) || die "--seed-count requires a value"
            seed_count=$2
            shift 2
            ;;
        --samples)
            (($# >= 2)) || die "--samples requires a value"
            samples_per_arm=$2
            shift 2
            ;;
        --operations)
            (($# >= 2)) || die "--operations requires a value"
            operation_count=$2
            shift 2
            ;;
        --run-timeout-seconds)
            (($# >= 2)) || die "--run-timeout-seconds requires a value"
            run_timeout_seconds=$2
            shift 2
            ;;
        --build-timeout-seconds)
            (($# >= 2)) || die "--build-timeout-seconds requires a value"
            build_timeout_seconds=$2
            shift 2
            ;;
        --max-load-average-1m)
            (($# >= 2)) || die "--max-load-average-1m requires a value"
            max_load_average_1m=$2
            shift 2
            ;;
        --aa-max-abs-log-ratio)
            (($# >= 2)) || die "--aa-max-abs-log-ratio requires a value"
            aa_max_abs_log_ratio=$2
            shift 2
            ;;
        --cas-publisher)
            (($# >= 2)) || die "--cas-publisher requires a value"
            cas_publisher=$2
            shift 2
            ;;
        --require-cas)
            require_cas=true
            shift
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            die "unknown argument: $1"
            ;;
    esac
done

[[ -n "${artifact_dir}" ]] || die "--artifact-dir is required"
[[ -n "${build_root}" ]] || die "--build-root is required"
[[ -n "${taskset_cpu_list}" ]] || die "--taskset-cpus is required"
[[ -n "${monitor_cpu}" ]] || die "--monitor-cpu is required"
[[ "${artifact_dir}" == /* ]] || die "--artifact-dir must be absolute"
[[ "${build_root}" == /* ]] || die "--build-root must be absolute"
[[ "${taskset_cpu_list}" =~ ^[0-9]+,[0-9]+$ ]] ||
    die "--taskset-cpus must select exactly two comma-separated CPUs"
[[ "${monitor_cpu}" =~ ^[0-9]+$ ]] ||
    die "--monitor-cpu must be one base-10 CPU number"

for integer_name in seed_base seed_count samples_per_arm operation_count \
    run_timeout_seconds build_timeout_seconds; do
    integer_value=${!integer_name}
    [[ "${integer_value}" =~ ^[0-9]+$ ]] ||
        die "${integer_name} must be an unsigned base-10 integer"
done
((seed_count == 20)) ||
    die "--seed-count is preregistered at exactly 20 independent seeds"
((samples_per_arm >= 48 && samples_per_arm % 48 == 0)) ||
    die "--samples must be a multiple of 48 and at least 48"
((operation_count > 0)) || die "--operations must be greater than zero"
((run_timeout_seconds >= 60)) ||
    die "--run-timeout-seconds must be at least 60"
((build_timeout_seconds >= 300)) ||
    die "--build-timeout-seconds must be at least 300"
[[ "${aa_max_abs_log_ratio}" =~ ^(0|[0-9]+)(\.[0-9]+)?$ ]] ||
    die "--aa-max-abs-log-ratio must be a non-negative decimal"
[[ "${max_load_average_1m}" =~ ^(0|[0-9]+)(\.[0-9]+)?$ ]] ||
    die "--max-load-average-1m must be a non-negative decimal"
awk -v value="${max_load_average_1m}" \
    'BEGIN { exit !(value >= 0.0 && value <= 1.0) }' ||
    die "--max-load-average-1m must be between 0.0 and 1.0"

readonly REQUIRED_TOOLS=(
    awk
    cargo
    cat
    date
    dirname
    find
    flock
    git
    id
    install
    jq
    mkdir
    od
    python3
    realpath
    rustc
    sed
    sha256sum
    sh
    sleep
    sort
    tail
    taskset
    timeout
    tr
    xargs
)
for tool_name in "${REQUIRED_TOOLS[@]}"; do
    command -v "${tool_name}" >/dev/null 2>&1 ||
        die "required tool is unavailable: ${tool_name}"
done

if [[ -n "${cas_publisher}" ]]; then
    [[ "${cas_publisher}" == /* ]] ||
        die "--cas-publisher must be an absolute path"
    [[ -x "${cas_publisher}" ]] ||
        die "CAS publisher is not executable: ${cas_publisher}"
elif [[ "${require_cas}" == true ]]; then
    die "--require-cas requires --cas-publisher"
fi

artifact_dir="$(realpath -m -- "${artifact_dir}")"
build_root="$(realpath -m -- "${build_root}")"
[[ "${artifact_dir}" != "${REPO_ROOT}"/* && "${artifact_dir}" != "${REPO_ROOT}" ]] ||
    die "artifact directory must be outside the source checkout"
[[ "${build_root}" != "${REPO_ROOT}"/* && "${build_root}" != "${REPO_ROOT}" ]] ||
    die "build root must be outside the source checkout"
[[ "${artifact_dir}" != "${build_root}" \
    && "${artifact_dir}" != "${build_root}"/* \
    && "${build_root}" != "${artifact_dir}"/* ]] ||
    die "artifact and build directories must be distinct and non-nested"
[[ ! -e "${artifact_dir}" ]] ||
    die "artifact directory already exists: ${artifact_dir}"
[[ ! -e "${build_root}" ]] ||
    die "build root already exists: ${build_root}"

cd -- "${REPO_ROOT}"
[[ "$(git branch --show-current)" == main ]] ||
    die "Gate 0 must run from branch main"
git diff --quiet -- || die "tracked source files have unstaged changes"
git diff --cached --quiet -- || die "tracked source files have staged changes"
[[ -z "$(git ls-files --others --exclude-standard)" ]] ||
    die "source checkout contains untracked files"

# Cargo merges configuration from `.cargo/config{,.toml}` in every ancestor of
# the invocation directory. A fresh CARGO_HOME is therefore insufficient to
# prove that the tracked repository config is the only effective filesystem
# configuration: `/data/projects/.cargo/config.toml`, for example, would still
# participate. Fail closed if any ancestor config exists rather than recording
# a provenance claim the build did not establish.
cargo_config_search_dir="$(dirname -- "${REPO_ROOT}")"
while :; do
    for cargo_config_name in config.toml config; do
        cargo_config_candidate="${cargo_config_search_dir}/.cargo/${cargo_config_name}"
        [[ ! -e "${cargo_config_candidate}" ]] ||
            die "ancestor Cargo config would affect the frozen build: ${cargo_config_candidate}"
    done
    [[ "${cargo_config_search_dir}" == / ]] && break
    cargo_config_search_dir="$(dirname -- "${cargo_config_search_dir}")"
done

head_sha="$(git rev-parse --verify HEAD)"
set +e
origin_main_query="$(
    GIT_TERMINAL_PROMPT=0 timeout --foreground --signal=TERM --kill-after=10s \
        120s git ls-remote --exit-code origin refs/heads/main
)"
origin_main_status=$?
set -e
[[ ${origin_main_status} -eq 0 ]] ||
    die "could not verify the current remote origin/main ref"
[[ "$(printf '%s\n' "${origin_main_query}" | awk 'NF {count++} END {print count+0}')" == 1 ]] ||
    die "origin/main query did not return exactly one ref"
origin_main_sha="$(printf '%s\n' "${origin_main_query}" | awk 'NF {print $1}')"
[[ "${origin_main_sha}" =~ ^[0-9a-f]{40}$ ]] ||
    die "origin/main query returned a malformed Git SHA"
[[ "${head_sha}" == "${origin_main_sha}" ]] ||
    die "HEAD ${head_sha} does not match origin/main ${origin_main_sha}"
[[ -f crates/fsqlite-e2e/tests/bd_105ga_replace_conflict_guard.rs ]] ||
    die "required executed integration guard is absent"

proc_status_cpu_list="$(
    taskset -c "${taskset_cpu_list}" \
        sh -c 'sed -n "s/^Cpus_allowed_list:[[:space:]]*//p" /proc/self/status'
)"
[[ -n "${proc_status_cpu_list}" ]] ||
    die "taskset launch probe did not expose Cpus_allowed_list"

mkdir -p -- "$(dirname -- "${artifact_dir}")" "$(dirname -- "${build_root}")"
mkdir -- "${artifact_dir}" "${build_root}"
exec 9>"${build_root}/runner.lock"
flock -n 9 || die "another runner holds ${build_root}/runner.lock"
mkdir -- \
    "${artifact_dir}/binaries" \
    "${artifact_dir}/build-receipts" \
    "${artifact_dir}/cas-receipts" \
    "${artifact_dir}/guards" \
    "${artifact_dir}/reports" \
    "${artifact_dir}/schemas" \
    "${artifact_dir}/topology-snapshots" \
    "${artifact_dir}/verification-receipts" \
    "${artifact_dir}/watchdog-receipts"

capture_topology_snapshot() {
    local output_path=$1
    local phase=$2
    local require_quiet=$3

    # Topology collection is control-plane work. Keep it off the measured CPUs
    # so repeated pre/post snapshots cannot perturb their caches or thermal
    # state between benchmark blocks.
    taskset -c "${monitor_cpu}" \
        python3 - \
        "${output_path}" \
        "${phase}" \
        "${head_sha}" \
        "${taskset_cpu_list}" \
        "${proc_status_cpu_list}" \
        "${monitor_cpu}" \
        "${max_load_average_1m}" \
        "${require_quiet}" \
        "$$" <<'PY'
import datetime
import hashlib
import json
import os
import re
import sys
from pathlib import Path

output_path = Path(sys.argv[1])
phase = sys.argv[2]
source_sha = sys.argv[3]
taskset_cpu_list = sys.argv[4]
benchmark_proc_cpu_list = sys.argv[5]
monitor_cpu = int(sys.argv[6])
max_load_1m = float(sys.argv[7])
require_quiet = sys.argv[8] == "true"
runner_pid = int(sys.argv[9])


def fail(message):
    raise SystemExit(f"gate0 topology verification: {message}")


def read_text(path, *, allow_empty=False):
    path = Path(path)
    try:
        value = path.read_text(encoding="utf-8").strip()
    except OSError as error:
        fail(f"cannot read {path}: {error}")
    if not value and not allow_empty:
        fail(f"{path} is empty")
    return value


def parse_cpu_list(value):
    value = value.strip()
    if not value or value == "(null)":
        return set()
    cpus = set()
    for segment in value.split(","):
        if not segment:
            fail(f"CPU list has an empty segment: {value!r}")
        if "-" in segment:
            start_raw, end_raw = segment.split("-", 1)
        else:
            start_raw = end_raw = segment
        if not start_raw.isdigit() or not end_raw.isdigit():
            fail(f"CPU list has a non-numeric segment: {value!r}")
        start = int(start_raw)
        end = int(end_raw)
        if start > end:
            fail(f"CPU list range is reversed: {segment!r}")
        cpus.update(range(start, end + 1))
    return cpus


def format_cpu_list(cpus):
    ordered = sorted(cpus)
    if not ordered:
        return ""
    ranges = []
    start = previous = ordered[0]
    for cpu in ordered[1:]:
        if cpu == previous + 1:
            previous = cpu
            continue
        ranges.append(str(start) if start == previous else f"{start}-{previous}")
        start = previous = cpu
    ranges.append(str(start) if start == previous else f"{start}-{previous}")
    return ",".join(ranges)


def parse_hex_cpu_mask(value):
    compact = value.strip().replace(",", "")
    if not compact or not re.fullmatch(r"[0-9a-fA-F]+", compact):
        fail(f"invalid hexadecimal CPU mask: {value!r}")
    mask = int(compact, 16)
    cpus = set()
    bit = 0
    while mask:
        if mask & 1:
            cpus.add(bit)
        bit += 1
        mask >>= 1
    return cpus


def proc_status_value(pid, name):
    status = read_text(f"/proc/{pid}/status")
    prefix = f"{name}:"
    for line in status.splitlines():
        if line.startswith(prefix):
            return line[len(prefix):].strip()
    fail(f"/proc/{pid}/status omits {name}")


def parent_pid(pid):
    try:
        return int(proc_status_value(pid, "PPid"))
    except (ValueError, SystemExit):
        return None


def descends_from(pid, ancestor):
    seen = set()
    while pid > 0 and pid not in seen:
        if pid == ancestor:
            return True
        seen.add(pid)
        parent = parent_pid(pid)
        if parent is None:
            return False
        pid = parent
    return False


def cgroup_tree_processes(root):
    try:
        process_files = [root / "cgroup.procs", *root.rglob("cgroup.procs")]
    except OSError as error:
        fail(f"cannot enumerate cgroup subtree {root}: {error}")
    membership = {}
    all_pids = set()
    for process_file in sorted(set(process_files), key=str):
        raw = read_text(process_file, allow_empty=True)
        pids = sorted(int(value) for value in raw.splitlines() if value.isdigit())
        membership[str(process_file.parent)] = pids
        all_pids.update(pids)
    return all_pids, membership


selected = parse_cpu_list(taskset_cpu_list)
if len(selected) != 2:
    fail(f"selected CPU cardinality is {len(selected)}, expected exactly two")
online = parse_cpu_list(read_text("/sys/devices/system/cpu/online"))
if not selected <= online:
    fail(f"offline CPUs were selected: {sorted(selected - online)}")
if monitor_cpu not in online:
    fail(f"monitor CPU {monitor_cpu} is offline")

actual_affinity = set(os.sched_getaffinity(0))
expected_snapshot_affinity = {monitor_cpu}
if actual_affinity != expected_snapshot_affinity:
    fail(
        "snapshot-process affinity mismatch: "
        f"expected {monitor_cpu}, observed {format_cpu_list(actual_affinity)}"
    )
actual_proc_cpu_list = proc_status_value(os.getpid(), "Cpus_allowed_list")
if actual_proc_cpu_list != str(monitor_cpu):
    fail(
        "snapshot-process /proc affinity changed: "
        f"expected {str(monitor_cpu)!r}, observed {actual_proc_cpu_list!r}"
    )
if benchmark_proc_cpu_list != format_cpu_list(selected):
    fail(
        "benchmark launch-probe affinity changed: "
        f"expected {format_cpu_list(selected)!r}, "
        f"observed {benchmark_proc_cpu_list!r}"
    )

topology = {}
physical_cores = set()
numa_nodes = set()
online_sibling_closure = set()
for cpu in sorted(selected):
    cpu_root = Path(f"/sys/devices/system/cpu/cpu{cpu}")
    topology_root = cpu_root / "topology"
    package = read_text(topology_root / "physical_package_id")
    core = read_text(topology_root / "core_id")
    siblings_raw = read_text(topology_root / "thread_siblings_list")
    siblings = parse_cpu_list(siblings_raw) & online
    if cpu not in siblings:
        fail(f"CPU {cpu} is absent from its online SMT sibling set {siblings_raw!r}")
    node_names = sorted(
        path.name
        for path in cpu_root.glob("node[0-9]*")
        if path.name[4:].isdigit()
    )
    if len(node_names) != 1:
        fail(f"CPU {cpu} exposes NUMA nodes {node_names!r}, expected exactly one")
    node = node_names[0]
    if (package, core) in physical_cores:
        fail(f"selected CPUs include two threads from physical core {package}:{core}")
    physical_cores.add((package, core))
    numa_nodes.add(node)
    online_sibling_closure.update(siblings)
    topology[f"cpu{cpu}"] = {
        "physical_package_id": package,
        "core_id": core,
        "thread_siblings_list": siblings_raw,
        "online_thread_siblings_list": format_cpu_list(siblings),
        "numa_node": node,
    }
if len(numa_nodes) != 1:
    fail(f"selected CPUs span NUMA nodes {sorted(numa_nodes)!r}")
monitor_topology_root = Path(
    f"/sys/devices/system/cpu/cpu{monitor_cpu}/topology"
)
monitor_physical_core = (
    read_text(monitor_topology_root / "physical_package_id"),
    read_text(monitor_topology_root / "core_id"),
)
if monitor_physical_core in physical_cores:
    fail(
        f"monitor CPU {monitor_cpu} shares a physical core with a measured CPU"
    )

cgroup_lines = [
    line
    for line in read_text("/proc/self/cgroup").splitlines()
    if line.startswith("0::")
]
if len(cgroup_lines) != 1:
    fail("the benchmark process is not in exactly one unified cgroup-v2 hierarchy")
cgroup_relative = cgroup_lines[0][3:]
cgroup_mount = Path("/sys/fs/cgroup").resolve()
cgroup_path = (cgroup_mount / cgroup_relative.lstrip("/")).resolve()
if cgroup_mount != cgroup_path and cgroup_mount not in cgroup_path.parents:
    fail(f"resolved cgroup path escapes the cgroup-v2 mount: {cgroup_path}")
if not (cgroup_path / "cgroup.controllers").exists():
    fail(f"cgroup-v2 controls are unavailable at {cgroup_path}")

partition_root = None
candidate = cgroup_path
while True:
    partition_file = candidate / "cpuset.cpus.partition"
    if partition_file.exists():
        partition_state = read_text(partition_file)
        if partition_state == "isolated":
            partition_root = candidate
            break
        if "invalid" in partition_state:
            fail(f"cpuset partition {candidate} is invalid: {partition_state!r}")
    if candidate == cgroup_mount:
        break
    candidate = candidate.parent
if partition_root is None:
    fail("no isolated cgroup-v2 cpuset partition contains the benchmark process")

current_effective = parse_cpu_list(read_text(cgroup_path / "cpuset.cpus.effective"))
partition_effective = parse_cpu_list(
    read_text(partition_root / "cpuset.cpus.effective")
)
exclusive_path = partition_root / "cpuset.cpus.exclusive.effective"
if not exclusive_path.exists():
    fail("the kernel exposes no cpuset.cpus.exclusive.effective proof")
partition_exclusive = parse_cpu_list(read_text(exclusive_path))
globally_isolated = parse_cpu_list(
    read_text(cgroup_mount / "cpuset.cpus.isolated", allow_empty=True)
)
for label, cpus in (
    ("current cgroup effective set", current_effective),
    ("isolated partition effective set", partition_effective),
    ("isolated partition exclusive set", partition_exclusive),
    ("global isolated cpuset", globally_isolated),
):
    if not online_sibling_closure <= cpus:
        fail(
            f"{label} omits selected online SMT siblings "
            f"{format_cpu_list(online_sibling_closure - cpus)}"
        )
for label, cpus in (
    ("current cgroup effective set", current_effective),
    ("isolated partition effective set", partition_effective),
    ("isolated partition exclusive set", partition_exclusive),
    ("global isolated cpuset", globally_isolated),
):
    if monitor_cpu not in cpus:
        fail(f"{label} omits monitor CPU {monitor_cpu}")

selected_numa_nodes = {int(node.removeprefix("node")) for node in numa_nodes}
current_mems = parse_cpu_list(read_text(cgroup_path / "cpuset.mems.effective"))
partition_mems = parse_cpu_list(
    read_text(partition_root / "cpuset.mems.effective")
)
for label, mems in (
    ("current cgroup effective memory-node set", current_mems),
    ("isolated partition effective memory-node set", partition_mems),
):
    if mems != selected_numa_nodes:
        fail(
            f"{label} is {format_cpu_list(mems)}, expected the selected "
            f"single NUMA node {format_cpu_list(selected_numa_nodes)}"
        )

cgroup_pids, cgroup_tree_membership = cgroup_tree_processes(partition_root)
foreign_cgroup_pids = sorted(
    pid for pid in cgroup_pids if not descends_from(pid, runner_pid)
)
if foreign_cgroup_pids:
    fail(
        "the isolated benchmark partition contains processes outside the runner tree: "
        f"{foreign_cgroup_pids}"
    )

ancestor_cpu_controls = {}
candidate = cgroup_path
while True:
    cpu_max_path = candidate / "cpu.max"
    if cpu_max_path.exists():
        cpu_max = read_text(cpu_max_path)
        cpu_max_fields = cpu_max.split()
        if len(cpu_max_fields) != 2 or cpu_max_fields[0] != "max":
            fail(f"cgroup ancestor {candidate} imposes CPU quota {cpu_max!r}")
        ancestor_cpu_controls[str(candidate)] = {
            "cpu.max": cpu_max,
            "cpu.weight": read_text(candidate / "cpu.weight")
            if (candidate / "cpu.weight").exists()
            else None,
        }
    if candidate == cgroup_mount:
        break
    candidate = candidate.parent

nohz_full_raw = read_text(
    "/sys/devices/system/cpu/nohz_full", allow_empty=True
)
nohz_full = parse_cpu_list(nohz_full_raw)
if not online_sibling_closure <= nohz_full:
    fail(
        "full-dynticks coverage omits selected online SMT siblings "
        f"{format_cpu_list(online_sibling_closure - nohz_full)}"
    )

default_irq_raw = read_text("/proc/irq/default_smp_affinity")
default_irq_cpus = parse_hex_cpu_mask(default_irq_raw)
if default_irq_cpus & online_sibling_closure:
    fail(
        "default IRQ affinity intersects the isolated SMT closure: "
        f"{format_cpu_list(default_irq_cpus & online_sibling_closure)}"
    )


def read_irq_affinity_text(path, *, allow_empty=False):
    """Read an IRQ affinity control without treating IRQ retirement as drift."""
    try:
        value = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return None
    except OSError as error:
        fail(f"cannot read {path}: {error}")
    if not value and not allow_empty:
        fail(f"{path} is empty")
    return value


irq_affinities = {}
irq_offenders = {}
for irq_path in sorted(
    (path for path in Path("/proc/irq").iterdir() if path.name.isdigit()),
    key=lambda path: int(path.name),
):
    list_path = irq_path / "effective_affinity_list"
    mask_path = irq_path / "effective_affinity"
    raw = read_irq_affinity_text(list_path, allow_empty=True)
    if raw is not None:
        cpus = parse_cpu_list(raw)
        source = "effective_affinity_list"
    else:
        raw = read_irq_affinity_text(mask_path)
        if raw is None:
            if not irq_path.exists():
                continue
            fail(f"IRQ {irq_path.name} exposes no readable effective affinity")
        cpus = parse_hex_cpu_mask(raw)
        source = "effective_affinity"
    irq_affinities[irq_path.name] = {
        "source": source,
        "raw": raw,
        "cpus": format_cpu_list(cpus),
    }
    overlap = cpus & online_sibling_closure
    if overlap:
        irq_offenders[irq_path.name] = format_cpu_list(overlap)
if irq_offenders:
    fail(f"effective IRQ affinities intersect the isolated SMT closure: {irq_offenders}")

frequency_policies = {}
for cpu in sorted(online_sibling_closure):
    cpufreq_path = Path(f"/sys/devices/system/cpu/cpu{cpu}/cpufreq")
    if not cpufreq_path.exists():
        fail(f"CPU {cpu} exposes no cpufreq policy controls")
    policy_path = cpufreq_path.resolve()
    policy_key = str(policy_path)
    if policy_key in frequency_policies:
        continue
    values = {}
    for name in (
        "affected_cpus",
        "related_cpus",
        "scaling_available_governors",
        "scaling_driver",
        "scaling_governor",
        "scaling_min_freq",
        "scaling_max_freq",
        "cpuinfo_min_freq",
        "cpuinfo_max_freq",
        "energy_performance_available_preferences",
        "energy_performance_preference",
    ):
        path = policy_path / name
        if path.exists():
            values[name] = read_text(path)
    if values.get("scaling_governor") != "performance":
        fail(
            f"frequency policy {policy_path} uses governor "
            f"{values.get('scaling_governor')!r}, expected 'performance'"
        )
    preference = values.get("energy_performance_preference")
    if preference is not None and preference != "performance":
        fail(
            f"frequency policy {policy_path} uses energy preference "
            f"{preference!r}, expected 'performance'"
        )
    frequency_policies[policy_key] = values

boost_controls = {}
for name, path, expected in (
    ("cpufreq.boost", "/sys/devices/system/cpu/cpufreq/boost", "1"),
    ("intel_pstate.no_turbo", "/sys/devices/system/cpu/intel_pstate/no_turbo", "0"),
    ("amd_pstate.status", "/sys/devices/system/cpu/amd_pstate/status", "active"),
):
    if Path(path).exists():
        value = read_text(path)
        if value != expected:
            fail(f"boost control {name} is {value!r}, expected {expected!r}")
        boost_controls[name] = value

load_parts = read_text("/proc/loadavg").split()
if not load_parts:
    fail("/proc/loadavg has no fields")
try:
    load_1m = float(load_parts[0])
except ValueError:
    fail(f"invalid one-minute load average: {load_parts[0]!r}")
if require_quiet and load_1m > max_load_1m:
    fail(f"one-minute load average {load_1m:.3f} exceeds {max_load_1m:.3f}")


def pressure_average(path, window):
    for line in read_text(path).splitlines():
        if not line.startswith("some "):
            continue
        for field in line.split()[1:]:
            if field.startswith(f"{window}="):
                try:
                    return float(field.split("=", 1)[1])
                except ValueError:
                    fail(f"invalid pressure value in {path}: {field!r}")
    fail(f"{path} omits some {window}")


cpu_pressure_some_avg10 = pressure_average("/proc/pressure/cpu", "avg10")
io_pressure_some_avg60 = pressure_average("/proc/pressure/io", "avg60")
if require_quiet and cpu_pressure_some_avg10 > 1.0:
    fail(
        "CPU pressure some avg10 "
        f"{cpu_pressure_some_avg10:.3f} exceeds 1.000"
    )
if require_quiet and io_pressure_some_avg60 > 0.10:
    fail(
        "I/O pressure some avg60 "
        f"{io_pressure_some_avg60:.3f} exceeds 0.100"
    )

stability_state = {
    "boot_id": read_text("/proc/sys/kernel/random/boot_id"),
    "kernel_release": read_text("/proc/sys/kernel/osrelease"),
    "kernel_cmdline": read_text("/proc/cmdline"),
    "online_cpus": format_cpu_list(online),
    "nohz_full": format_cpu_list(nohz_full),
    "global_isolated_cpuset": format_cpu_list(globally_isolated),
    "cgroup_path": str(cgroup_path),
    "partition_root": str(partition_root),
    "current_cpuset_effective": format_cpu_list(current_effective),
    "partition_cpuset_effective": format_cpu_list(partition_effective),
    "partition_cpuset_exclusive_effective": format_cpu_list(partition_exclusive),
    "current_cpuset_mems_effective": format_cpu_list(current_mems),
    "partition_cpuset_mems_effective": format_cpu_list(partition_mems),
    "ancestor_cpu_controls": ancestor_cpu_controls,
    "selected_topology": topology,
    "monitor_cpu": monitor_cpu,
    "monitor_physical_core": {
        "physical_package_id": monitor_physical_core[0],
        "core_id": monitor_physical_core[1],
    },
    "default_irq_affinity": default_irq_raw,
    "effective_irq_affinities": irq_affinities,
    "frequency_policies": frequency_policies,
    "boost_controls": boost_controls,
}
stability_fingerprint = hashlib.sha256(
    json.dumps(stability_state, sort_keys=True, separators=(",", ":")).encode()
).hexdigest()
receipt = {
    "schema_version": "fsqlite-e2e.gate0-topology-receipt.v2",
    "generated_at_utc": datetime.datetime.now(datetime.timezone.utc)
    .replace(microsecond=0)
    .isoformat()
    .replace("+00:00", "Z"),
    "phase": phase,
    "verified": True,
    "verification_source": "live Linux procfs, sysfs, and cgroup-v2 state",
    "source_git_sha": source_sha,
    "runner_pid": runner_pid,
    "taskset_cpu_list": taskset_cpu_list,
    "monitor_cpu": monitor_cpu,
    "proc_status_cpu_list": benchmark_proc_cpu_list,
    "snapshot_process_cpu_list": actual_proc_cpu_list,
    "selected_online_smt_closure": format_cpu_list(online_sibling_closure),
    "isolated_cgroup_v2_cpuset_verified": True,
    "online_smt_siblings_covered": True,
    "full_dynticks_verified": True,
    "effective_irq_affinity_disjoint": True,
    "launch_probe_affinity_verified": True,
    "snapshot_process_affinity_verified": True,
    "frequency_policy_verified": True,
    "max_load_average_1m": max_load_1m,
    "observed_load_average_1m": load_1m,
    "cpu_pressure_some_avg10": cpu_pressure_some_avg10,
    "io_pressure_some_avg60": io_pressure_some_avg60,
    "quiet_host_required": require_quiet,
    "quiet_host_verified": (
        load_1m <= max_load_1m
        and cpu_pressure_some_avg10 <= 1.0
        and io_pressure_some_avg60 <= 0.10
    ),
    "cgroup_partition_tree_processes": sorted(cgroup_pids),
    "cgroup_partition_tree_membership": cgroup_tree_membership,
    "foreign_cgroup_processes": foreign_cgroup_pids,
    "stability_fingerprint": stability_fingerprint,
    "stability_state": stability_state,
}
try:
    with output_path.open("x", encoding="utf-8") as handle:
        json.dump(receipt, handle, indent=2, sort_keys=True)
        handle.write("\n")
except OSError as error:
    fail(f"cannot create {output_path}: {error}")
PY
}

topology_receipt="${artifact_dir}/topology-receipt.json"
capture_topology_snapshot "${topology_receipt}" "initial" true
jq -e \
    --arg source_sha "${head_sha}" \
    '
      .schema_version == "fsqlite-e2e.gate0-topology-receipt.v2"
      and .verified == true
      and .source_git_sha == $source_sha
      and .isolated_cgroup_v2_cpuset_verified == true
      and .online_smt_siblings_covered == true
      and .full_dynticks_verified == true
      and .effective_irq_affinity_disjoint == true
      and .launch_probe_affinity_verified == true
      and .snapshot_process_affinity_verified == true
      and .quiet_host_required == true
      and .quiet_host_verified == true
      and .frequency_policy_verified == true
      and (.stability_fingerprint | type == "string" and test("^[0-9a-f]{64}$"))
    ' "${topology_receipt}" >/dev/null ||
    die "live topology receipt failed its strict Gate 0 contract"
initial_topology_fingerprint="$(
    jq -er '.stability_fingerprint' "${topology_receipt}"
)"
readonly initial_topology_fingerprint
benchmark_cgroup_path="$(
    jq -er '.stability_state.cgroup_path' "${topology_receipt}"
)"
readonly benchmark_cgroup_path
benchmark_partition_root="$(
    jq -er '.stability_state.partition_root' "${topology_receipt}"
)"
readonly benchmark_partition_root

cargo_bin="$(command -v cargo)"
readonly cargo_bin
rustc_bin="$(command -v rustc)"
readonly rustc_bin
python_bin="$(command -v python3)"
readonly python_bin
readonly clean_home="${HOME:?HOME must be set}"
readonly clean_user="${USER:-$(id -un)}"
readonly rustup_home="${RUSTUP_HOME:-${clean_home}/.rustup}"
readonly isolated_cargo_home="${build_root}/cargo-home"
[[ -d "${rustup_home}" ]] ||
    die "Rustup home is unavailable: ${rustup_home}"
mkdir -- "${isolated_cargo_home}"
[[ -z "$(find "${isolated_cargo_home}" -mindepth 1 -print -quit)" ]] ||
    die "isolated Cargo home was not created empty"
clean_path="$(dirname -- "${cargo_bin}"):/usr/local/bin:/usr/bin:/bin"
readonly clean_path
rustc_verbose="$("${rustc_bin}" -vV)"
readonly rustc_verbose
cargo_verbose="$("${cargo_bin}" -Vv)"
readonly cargo_verbose
target_host="$(printf '%s\n' "${rustc_verbose}" | sed -n 's/^host: //p')"
readonly target_host
[[ -n "${target_host}" ]] || die "rustc did not report a host target"
git ls-files --error-unmatch .cargo/config.toml >/dev/null ||
    die "repository Cargo config is not tracked"
repository_cargo_config_sha256="$(
    sha256sum .cargo/config.toml | awk '{print $1}'
)"
readonly repository_cargo_config_sha256
rust_toolchain_sha256="$(
    sha256sum rust-toolchain.toml | awk '{print $1}'
)"
readonly rust_toolchain_sha256

cas_index="${artifact_dir}/cas-receipts/index.jsonl"
if [[ -z "${cas_publisher}" ]]; then
    jq -n \
        '{schema_version:"fsqlite-e2e.gate0-cas-policy.v1",
          configured:false,
          required:false,
          note:"Local retained copies and SHA-256 receipts are verified; no external CAS publisher was configured."}' \
        >"${artifact_dir}/cas-receipts/policy.json"
else
    publisher_sha="$(sha256sum -- "${cas_publisher}" | awk '{print $1}')"
    jq -n \
        --arg path "${cas_publisher}" \
        --arg sha256 "${publisher_sha}" \
        --argjson required "${require_cas}" \
        '{schema_version:"fsqlite-e2e.gate0-cas-policy.v1",
          configured:true,
          required:$required,
          publisher_path:$path,
          publisher_sha256:$sha256}' \
        >"${artifact_dir}/cas-receipts/policy.json"
fi

publish_to_cas() {
    local artifact_kind=$1
    local file_path=$2
    local file_sha
    local publisher_output
    local canonical_file

    [[ -f "${file_path}" ]] || die "CAS input is not a file: ${file_path}"
    canonical_file="$(realpath -- "${file_path}")"
    file_sha="$(sha256sum -- "${canonical_file}" | awk '{print $1}')"
    if [[ -z "${cas_publisher}" ]]; then
        return
    fi
    publisher_output="$(
        timeout --foreground --signal=TERM --kill-after=30s \
            "${run_timeout_seconds}s" \
            "${cas_publisher}" \
            "${canonical_file}" "${file_sha}" "${artifact_kind}"
    )" ||
        die "CAS publisher failed for ${canonical_file}"
    jq -s -e --arg sha256 "${file_sha}" \
        '
          length == 1
          and (
            .[0]
            | type == "object"
              and .sha256 == $sha256
              and (.uri | type == "string" and length > 0)
          )
        ' <<<"${publisher_output}" >/dev/null ||
        die "CAS publisher returned an invalid receipt for ${canonical_file}"
    jq -s -c \
        --arg artifact_kind "${artifact_kind}" \
        --arg local_path "${canonical_file}" \
        '.[0] + {artifact_kind:$artifact_kind, local_path:$local_path}' \
        <<<"${publisher_output}" >>"${cas_index}"
}

topology_sha="$(sha256sum -- "${artifact_dir}/topology-receipt.json" | awk '{print $1}')"
publish_to_cas topology-receipt "${artifact_dir}/topology-receipt.json"

jq -n \
    --arg source_git_sha "${head_sha}" \
    --arg origin_main_sha "${origin_main_sha}" \
    --arg cargo_lock_sha256 "$(sha256sum Cargo.lock | awk '{print $1}')" \
    --arg topology_receipt_sha256 "${topology_sha}" \
    --arg taskset_cpu_list "${taskset_cpu_list}" \
    --arg proc_status_cpu_list "${proc_status_cpu_list}" \
    --argjson monitor_cpu "${monitor_cpu}" \
    --arg target_host "${target_host}" \
    --arg isolated_cargo_home "${isolated_cargo_home}" \
    --arg origin_main_query "git ls-remote --exit-code origin refs/heads/main" \
    --arg rustc_verbose "${rustc_verbose}" \
    --arg cargo_verbose "${cargo_verbose}" \
    --arg repository_cargo_config_sha256 "${repository_cargo_config_sha256}" \
    --arg rust_toolchain_sha256 "${rust_toolchain_sha256}" \
    --arg generated_at_utc "$(date --utc +%Y-%m-%dT%H:%M:%SZ)" \
    --argjson seed_base "${seed_base}" \
    --argjson seed_count "${seed_count}" \
    --argjson samples_per_arm "${samples_per_arm}" \
    --argjson operation_count "${operation_count}" \
    --argjson run_timeout_seconds "${run_timeout_seconds}" \
    --argjson build_timeout_seconds "${build_timeout_seconds}" \
    --argjson max_load_average_1m "${max_load_average_1m}" \
    --argjson aa_max_abs_log_ratio "${aa_max_abs_log_ratio}" \
    '{
      schema_version:"fsqlite-e2e.gate0-async-bridge-protocol.v1",
      generated_at_utc:$generated_at_utc,
      source_git_sha:$source_git_sha,
      origin_main_sha:$origin_main_sha,
      cargo_lock_sha256:$cargo_lock_sha256,
      topology_receipt_sha256:$topology_receipt_sha256,
      taskset_cpu_list:$taskset_cpu_list,
      proc_status_cpu_list:$proc_status_cpu_list,
      monitor_cpu:$monitor_cpu,
      target_host:$target_host,
      isolated_cargo_home:$isolated_cargo_home,
      cargo_config_policy:"fresh CARGO_HOME plus tracked repository .cargo configuration only",
      repository_cargo_config_sha256:$repository_cargo_config_sha256,
      rust_toolchain_sha256:$rust_toolchain_sha256,
      rustc_verbose:$rustc_verbose,
      cargo_verbose:$cargo_verbose,
      origin_main_query:$origin_main_query,
      profiles:["release","release-perf"],
      ancestor_cargo_configs_absent:true,
      ordering:"alternating four-run ABBA/BAAB profile blocks; each independent seed runs once per profile",
      seed_base:$seed_base,
      independent_seed_count:$seed_count,
      samples_per_arm:$samples_per_arm,
      operation_count:$operation_count,
      run_timeout_seconds:$run_timeout_seconds,
      build_timeout_seconds:$build_timeout_seconds,
      max_load_average_1m:$max_load_average_1m,
      aa_max_abs_log_ratio:$aa_max_abs_log_ratio,
      primary_contrast_family_size:6,
      per_contrast_simultaneous_confidence_level:0.991667,
      inner_reports_are_diagnostic_only:true
    }' >"${artifact_dir}/protocol.json"

profile_environment() {
    local profile=$1
    local prefix
    local opt_level

    case "${profile}" in
        release)
            prefix=CARGO_PROFILE_RELEASE
            opt_level=z
            ;;
        release-perf)
            prefix=CARGO_PROFILE_RELEASE_PERF
            opt_level=3
            ;;
        *)
            die "unsupported profile: ${profile}"
            ;;
    esac

    PROFILE_ENVIRONMENT=(
        "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER="
        "CARGO_BUILD_RUSTC_WRAPPER="
        "CARGO_BUILD_RUSTFLAGS="
        "RUSTC_WORKSPACE_WRAPPER="
        "RUSTC_WRAPPER="
        "${prefix}_CODEGEN_UNITS=1"
        "${prefix}_DEBUG=false"
        "${prefix}_DEBUG_ASSERTIONS=false"
        "${prefix}_INCREMENTAL=false"
        "${prefix}_LTO=true"
        "${prefix}_OPT_LEVEL=${opt_level}"
        "${prefix}_OVERFLOW_CHECKS=false"
        "${prefix}_PANIC=abort"
        "${prefix}_RPATH=false"
        "${prefix}_SPLIT_DEBUGINFO=off"
        "${prefix}_STRIP=true"
    )
}

clean_environment_prefix() {
    CLEAN_ENVIRONMENT_PREFIX=(
        env -i
        "HOME=${clean_home}"
        "USER=${clean_user}"
        "LOGNAME=${clean_user}"
        "PATH=${clean_path}"
        "LC_ALL=C"
        "CARGO_HOME=${isolated_cargo_home}"
        "RUSTUP_HOME=${rustup_home}"
        "CARGO_ENCODED_RUSTFLAGS="
        "LIBSQLITE3_FLAGS=-DSQLITE_ENABLE_MATH_FUNCTIONS"
    )
}

build_profile() {
    local profile=$1
    local profile_dir="${artifact_dir}/build-receipts/${profile}"
    local target_dir="${build_root}/${profile}/target"
    local events_path="${profile_dir}/build-events.jsonl"
    local log_path="${profile_dir}/build-vv.log"
    local guard_log="${artifact_dir}/guards/${profile}.log"
    local guard_events="${artifact_dir}/guards/${profile}-events.jsonl"
    local frozen_dir="${artifact_dir}/binaries/${profile}"
    local frozen_binary="${frozen_dir}/comprehensive-bench"
    local nonce
    local built_binary
    local binary_sha
    local build_status
    local guard_status
    local build_started
    local build_finished
    local profile_environment_json

    mkdir -p -- "${profile_dir}" "${target_dir}" "${frozen_dir}"
    nonce="$(od -An -N32 -tx1 /dev/urandom | tr -d ' \n')"
    [[ "${nonce}" =~ ^[0-9a-f]{64}$ ]] ||
        die "could not generate a 64-character build nonce"
    profile_environment "${profile}"
    profile_environment_json="$(
        printf '%s\n' "${PROFILE_ENVIRONMENT[@]}" |
            jq -Rn '
              [inputs | capture("^(?<key>[^=]+)=(?<value>.*)$")]
              | map({key:.key, value:.value})
              | from_entries
            '
    )"
    clean_environment_prefix
    build_started="$(date --utc +%Y-%m-%dT%H:%M:%SZ)"
    set +e
    timeout --foreground --signal=TERM --kill-after=30s \
        "${build_timeout_seconds}s" \
        "${CLEAN_ENVIRONMENT_PREFIX[@]}" \
        "${PROFILE_ENVIRONMENT[@]}" \
        "FSQLITE_BENCH_PROFILE_NAME=${profile}" \
        "FSQLITE_BENCH_BUILD_NONCE=${nonce}" \
        "${cargo_bin}" build \
        --locked \
        -vv \
        --color never \
        --message-format=json-render-diagnostics \
        --target-dir "${target_dir}" \
        --profile "${profile}" \
        --target "${target_host}" \
        -p fsqlite-e2e \
        --features bridge-experiment \
        --bin comprehensive-bench \
        >"${events_path}" 2>"${log_path}"
    build_status=$?
    set -e
    build_finished="$(date --utc +%Y-%m-%dT%H:%M:%SZ)"
    [[ ${build_status} -eq 0 ]] ||
        die "${profile} locked benchmark build failed or timed out with status ${build_status}"

    built_binary="$(jq -Rr '
        fromjson?
        | select(.reason == "compiler-artifact")
        | select(.target.name == "comprehensive-bench")
        | select(.target.kind | index("bin"))
        | .executable // empty
      ' "${events_path}" | tail -n 1)"
    [[ -n "${built_binary}" && -x "${built_binary}" ]] ||
        die "${profile} build did not emit an executable receipt"
    install -m 0555 -- "${built_binary}" "${frozen_binary}"
    binary_sha="$(sha256sum -- "${frozen_binary}" | awk '{print $1}')"

    set +e
    timeout --foreground --signal=TERM --kill-after=30s \
        "${build_timeout_seconds}s" \
        "${CLEAN_ENVIRONMENT_PREFIX[@]}" \
        "${PROFILE_ENVIRONMENT[@]}" \
        "FSQLITE_BENCH_PROFILE_NAME=${profile}" \
        "FSQLITE_BENCH_BUILD_NONCE=${nonce}" \
        "${cargo_bin}" test \
        --locked \
        -vv \
        --color never \
        --message-format=json-render-diagnostics \
        --target-dir "${target_dir}" \
        --profile "${profile}" \
        --target "${target_host}" \
        -p fsqlite-e2e \
        --test bd_105ga_replace_conflict_guard \
        -- \
        --exact replace_conflict_churn_leaves_a_stock_valid_database \
        --nocapture \
        >"${guard_events}" 2>"${guard_log}"
    guard_status=$?
    set -e
    guard_summary_verified=false
    if [[ ${guard_status} -eq 0 ]] && "${python_bin}" - "${guard_events}" <<'PY'
import re
import sys
from pathlib import Path

output = Path(sys.argv[1]).read_text(encoding="utf-8", errors="replace")
test_name = "replace_conflict_churn_leaves_a_stock_valid_database"
name_pattern = re.compile(rf"(?m)^test {re.escape(test_name)} \.\.\. ok\s*$")
summary_pattern = re.compile(
    r"(?m)^test result: ok\. 1 passed; 0 failed; 0 ignored; "
    r"0 measured; 0 filtered out; finished in .+$"
)
if name_pattern.search(output) is None or summary_pattern.search(output) is None:
    raise SystemExit(1)
PY
    then
        guard_summary_verified=true
    fi
    jq -n \
        --arg profile "${profile}" \
        --arg source_git_sha "${head_sha}" \
        --arg command "cargo test --locked --profile ${profile} -p fsqlite-e2e --test bd_105ga_replace_conflict_guard -- --exact replace_conflict_churn_leaves_a_stock_valid_database --nocapture" \
        --arg test_name "replace_conflict_churn_leaves_a_stock_valid_database" \
        --arg log_sha256 "$(sha256sum -- "${guard_log}" | awk '{print $1}')" \
        --arg events_sha256 "$(sha256sum -- "${guard_events}" | awk '{print $1}')" \
        --argjson exit_status "${guard_status}" \
        --argjson summary_verified "${guard_summary_verified}" \
        '{
          schema_version:"fsqlite-e2e.gate0-executed-guard-receipt.v1",
          profile:$profile,
          source_git_sha:$source_git_sha,
          command:$command,
          test_name:$test_name,
          execution_kind:"executed_integration_test_binary",
          compile_only:false,
          executed_test_count:(if $summary_verified then 1 else 0 end),
          test_result_summary_verified:$summary_verified,
          exit_status:$exit_status,
          log_sha256:$log_sha256,
          cargo_events_sha256:$events_sha256,
          verified:($exit_status == 0 and $summary_verified)
        }' >"${artifact_dir}/guards/${profile}-receipt.json"
    [[ ${guard_status} -eq 0 && "${guard_summary_verified}" == true ]] ||
        die "${profile} persisted corruption guard did not prove one exact passing test (status ${guard_status})"

    "${frozen_binary}" --bridge-experiment --print-json-schema \
        >"${artifact_dir}/schemas/${profile}-bridge-schema.json"
    jq -e \
        '."$schema" == "https://json-schema.org/draft/2020-12/schema"
         and ."properties"."schema_version"."const" == "fsqlite-e2e.bridge-experiment.v3"' \
        "${artifact_dir}/schemas/${profile}-bridge-schema.json" >/dev/null ||
        die "${profile} frozen binary emitted an invalid bridge schema"

    jq -n \
        --arg profile "${profile}" \
        --arg source_git_sha "${head_sha}" \
        --arg build_nonce "${nonce}" \
        --arg target_host "${target_host}" \
        --arg build_started_at_utc "${build_started}" \
        --arg build_finished_at_utc "${build_finished}" \
        --arg command "cargo build --locked -vv --profile ${profile} --target ${target_host} -p fsqlite-e2e --features bridge-experiment --bin comprehensive-bench" \
        --arg build_log_path "${log_path}" \
        --arg build_log_sha256 "$(sha256sum -- "${log_path}" | awk '{print $1}')" \
        --arg build_events_sha256 "$(sha256sum -- "${events_path}" | awk '{print $1}')" \
        --arg frozen_binary_path "${frozen_binary}" \
        --arg frozen_binary_sha256 "${binary_sha}" \
        --arg schema_sha256 "$(sha256sum -- "${artifact_dir}/schemas/${profile}-bridge-schema.json" | awk '{print $1}')" \
        --arg rustc_verbose "${rustc_verbose}" \
        --arg cargo_verbose "${cargo_verbose}" \
        --arg cargo_home "${isolated_cargo_home}" \
        --arg repository_cargo_config_sha256 "${repository_cargo_config_sha256}" \
        --arg rust_toolchain_sha256 "${rust_toolchain_sha256}" \
        --argjson canonical_profile_environment "${profile_environment_json}" \
        '{
          schema_version:"fsqlite-e2e.gate0-frozen-build-receipt.v1",
          profile:$profile,
          source_git_sha:$source_git_sha,
          cargo_locked:true,
          build_nonce:$build_nonce,
          target_host:$target_host,
          build_started_at_utc:$build_started_at_utc,
          build_finished_at_utc:$build_finished_at_utc,
          command:$command,
          build_log_path:$build_log_path,
          build_log_sha256:$build_log_sha256,
          build_events_sha256:$build_events_sha256,
          frozen_binary_path:$frozen_binary_path,
          frozen_binary_sha256:$frozen_binary_sha256,
          bridge_schema_sha256:$schema_sha256,
          rustc_verbose:$rustc_verbose,
          cargo_verbose:$cargo_verbose,
          effective_cargo_configuration:{
            cargo_home:$cargo_home,
            cargo_home_was_created_empty:true,
            repository_cargo_config_sha256:$repository_cargo_config_sha256,
            ancestor_cargo_configs_absent:true,
            rust_toolchain_sha256:$rust_toolchain_sha256,
            encoded_rustflags:"",
            native_environment:{LIBSQLITE3_FLAGS:"-DSQLITE_ENABLE_MATH_FUNCTIONS"},
            canonical_profile_environment:$canonical_profile_environment
          },
          verified:true
        }' >"${profile_dir}/receipt.json"
    publish_to_cas "frozen-${profile}-binary" "${frozen_binary}"
    publish_to_cas "${profile}-build-receipt" "${profile_dir}/receipt.json"
    publish_to_cas "${profile}-guard-receipt" \
        "${artifact_dir}/guards/${profile}-receipt.json"
}

printf 'Building and freezing release profile...\n' >&2
build_profile release
printf 'Building and freezing release-perf profile...\n' >&2
build_profile release-perf

order_path="${artifact_dir}/run-order.tsv"
printf 'sequence\tgroup_index\tgroup_pattern\tslot\tseed\tprofile\trun_id\n' \
    >"${order_path}"
sequence=0
for ((pair_index = 0; pair_index < seed_count / 2; pair_index++)); do
    first_seed=$((seed_base + pair_index * 2))
    second_seed=$((first_seed + 1))
    if ((pair_index % 2 == 0)); then
        group_pattern=ABBA
        profiles=(release release-perf release-perf release)
    else
        group_pattern=BAAB
        profiles=(release-perf release release release-perf)
    fi
    seeds=("${first_seed}" "${first_seed}" "${second_seed}" "${second_seed}")
    for slot_index in 0 1 2 3; do
        sequence=$((sequence + 1))
        profile=${profiles[slot_index]}
        seed=${seeds[slot_index]}
        run_id="$(printf '%03d-seed-%s-%s' "${sequence}" "${seed}" "${profile}")"
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "${sequence}" "${pair_index}" "${group_pattern}" \
            "$((slot_index + 1))" "${seed}" "${profile}" "${run_id}" \
            >>"${order_path}"
    done
done

capture_stable_topology_snapshot() {
    local output_path=$1
    local phase=$2
    local require_quiet=$3
    local observed_fingerprint

    capture_topology_snapshot "${output_path}" "${phase}" "${require_quiet}"
    observed_fingerprint="$(jq -er '.stability_fingerprint' "${output_path}")"
    [[ "${observed_fingerprint}" == "${initial_topology_fingerprint}" ]] ||
        die "${phase} topology fingerprint drifted from the initial receipt"
}

monitor_benchmark_affinity() {
    local watchdog_pid=$1
    local binary=$2
    local output_path=$3
    local run_id=$4

    taskset -c "${monitor_cpu}" \
        python3 - \
        "${watchdog_pid}" \
        "${binary}" \
        "${proc_status_cpu_list}" \
        "${benchmark_cgroup_path}" \
        "${benchmark_partition_root}" \
        "$$" \
        "${topology_receipt}" \
        "${output_path}" \
        "${run_id}" <<'PY'
import datetime
import json
import os
import sys
import time
from pathlib import Path

watchdog_pid = int(sys.argv[1])
expected_binary = str(Path(sys.argv[2]).resolve())
expected_affinity = sys.argv[3]
expected_cgroup_path = Path(sys.argv[4]).resolve()
expected_partition_root = Path(sys.argv[5]).resolve()
runner_pid = int(sys.argv[6])
topology_receipt_path = Path(sys.argv[7])
output_path = Path(sys.argv[8])
run_id = sys.argv[9]
poll_interval_seconds = 0.01
cgroup_poll_interval_seconds = 0.1
# Procfs/sysfs control scans enumerate every live IRQ and frequency policy.
# Keep them continuous without turning the observer into a material package
# load beside the benchmark. Process/thread placement remains sampled at the
# tighter intervals above.
control_poll_interval_seconds = 1.0
failures = []

try:
    expected_topology_receipt = json.loads(
        topology_receipt_path.read_text(encoding="utf-8")
    )
except (OSError, json.JSONDecodeError) as error:
    raise SystemExit(
        f"cannot load initial topology receipt {topology_receipt_path}: {error}"
    )
if (
    expected_topology_receipt.get("schema_version")
    != "fsqlite-e2e.gate0-topology-receipt.v2"
    or expected_topology_receipt.get("verified") is not True
):
    raise SystemExit("initial topology receipt is not verified v2 evidence")
expected_stability = expected_topology_receipt["stability_state"]
selected_smt_closure = expected_topology_receipt["selected_online_smt_closure"]


def read_text(path, *, allow_empty=False):
    path = Path(path)
    try:
        value = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        failures.append(f"control path disappeared: {path}")
        return None
    except OSError as error:
        failures.append(f"cannot read {path}: {error}")
        return None
    if not value and not allow_empty:
        failures.append(f"control path is empty: {path}")
        return None
    return value


def require_exact_control(path, expected, *, allow_empty=False):
    observed = read_text(path, allow_empty=allow_empty)
    if observed is not None and observed != expected:
        failures.append(
            f"control drift at {path}: observed {observed!r}, expected {expected!r}"
        )


def parse_cpu_list(value):
    value = value.strip()
    if not value or value == "(null)":
        return set()
    cpus = set()
    for segment in value.split(","):
        if "-" in segment:
            start_raw, end_raw = segment.split("-", 1)
        else:
            start_raw = end_raw = segment
        if not start_raw.isdigit() or not end_raw.isdigit():
            raise ValueError(f"invalid CPU list {value!r}")
        start = int(start_raw)
        end = int(end_raw)
        if start > end:
            raise ValueError(f"reversed CPU range {segment!r}")
        cpus.update(range(start, end + 1))
    return cpus


def parse_hex_cpu_mask(value):
    compact = value.strip().replace(",", "")
    if not compact:
        raise ValueError("empty hexadecimal CPU mask")
    return {
        bit
        for bit in range(int(compact, 16).bit_length())
        if int(compact, 16) & (1 << bit)
    }


selected_smt_cpus = parse_cpu_list(selected_smt_closure)


def process_children(pid):
    path = Path(f"/proc/{pid}/task/{pid}/children")
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return []
    except OSError as error:
        failures.append(f"cannot read {path}: {error}")
        return []
    return [int(value) for value in raw.split() if value.isdigit()]


def process_start_time_ticks(pid):
    path = Path(f"/proc/{pid}/stat")
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except OSError as error:
        failures.append(f"cannot read {path}: {error}")
        return None
    command_end = raw.rfind(")")
    if command_end < 0:
        failures.append(f"{path} has malformed process-stat syntax")
        return None
    fields_after_command = raw[command_end + 1 :].split()
    # The first token after `(comm)` is field 3 (state); starttime is field 22.
    if len(fields_after_command) <= 19:
        failures.append(f"{path} omits process starttime")
        return None
    try:
        return int(fields_after_command[19])
    except ValueError:
        failures.append(f"{path} has invalid process starttime")
        return None


def descendants(root):
    found = []
    pending = [root]
    seen = set()
    while pending:
        pid = pending.pop()
        if pid in seen:
            continue
        seen.add(pid)
        children = process_children(pid)
        found.extend(children)
        pending.extend(children)
    return found


def executable(pid):
    try:
        return str(Path(f"/proc/{pid}/exe").resolve(strict=True))
    except (FileNotFoundError, OSError):
        return None


def status_value(path, name):
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except OSError as error:
        failures.append(f"cannot read {path}: {error}")
        return None
    prefix = f"{name}:"
    for line in text.splitlines():
        if line.startswith(prefix):
            return line[len(prefix):].strip()
    failures.append(f"{path} omits {name}")
    return None


def parent_pid(pid):
    value = status_value(Path(f"/proc/{pid}/status"), "PPid")
    try:
        return int(value) if value is not None else None
    except ValueError:
        failures.append(f"PID {pid} has invalid PPid {value!r}")
        return None


def descends_from(pid, ancestor):
    seen = set()
    while pid > 0 and pid not in seen:
        if pid == ancestor:
            return True
        seen.add(pid)
        parent = parent_pid(pid)
        if parent is None:
            return False
        pid = parent
    return False


def process_cgroup_path(pid):
    path = Path(f"/proc/{pid}/cgroup")
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return None
    except OSError as error:
        failures.append(f"cannot read {path}: {error}")
        return None
    unified = [line[3:] for line in lines if line.startswith("0::")]
    if len(unified) != 1:
        failures.append(f"PID {pid} is not in exactly one unified cgroup")
        return None
    return (Path("/sys/fs/cgroup") / unified[0].lstrip("/")).resolve()


def partition_tree_pids():
    try:
        process_files = [
            expected_partition_root / "cgroup.procs",
            *expected_partition_root.rglob("cgroup.procs"),
        ]
    except OSError as error:
        failures.append(
            f"cannot enumerate isolated partition {expected_partition_root}: {error}"
        )
        return set()
    pids = set()
    for process_file in sorted(set(process_files), key=str):
        try:
            raw = process_file.read_text(encoding="utf-8")
        except FileNotFoundError:
            continue
        except OSError as error:
            failures.append(f"cannot read {process_file}: {error}")
            continue
        pids.update(int(value) for value in raw.splitlines() if value.isdigit())
    return pids


def read_irq_affinity_text(path, *, allow_empty=False):
    """Read a live IRQ control, allowing the IRQ to retire during enumeration."""
    try:
        value = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return None
    except OSError as error:
        failures.append(f"cannot read {path}: {error}")
        return None
    if not value and not allow_empty:
        failures.append(f"control path is empty: {path}")
        return None
    return value


def scan_stability_controls():
    require_exact_control(
        "/sys/devices/system/cpu/nohz_full",
        expected_stability["nohz_full"],
        allow_empty=True,
    )
    require_exact_control(
        "/sys/fs/cgroup/cpuset.cpus.isolated",
        expected_stability["global_isolated_cpuset"],
        allow_empty=True,
    )
    require_exact_control(
        expected_cgroup_path / "cpuset.cpus.effective",
        expected_stability["current_cpuset_effective"],
    )
    require_exact_control(
        expected_partition_root / "cpuset.cpus.effective",
        expected_stability["partition_cpuset_effective"],
    )
    require_exact_control(
        expected_partition_root / "cpuset.cpus.exclusive.effective",
        expected_stability["partition_cpuset_exclusive_effective"],
    )
    require_exact_control(
        expected_cgroup_path / "cpuset.mems.effective",
        expected_stability["current_cpuset_mems_effective"],
    )
    require_exact_control(
        expected_partition_root / "cpuset.mems.effective",
        expected_stability["partition_cpuset_mems_effective"],
    )
    for cgroup_raw, controls in expected_stability[
        "ancestor_cpu_controls"
    ].items():
        cgroup = Path(cgroup_raw)
        require_exact_control(cgroup / "cpu.max", controls["cpu.max"])
        if controls["cpu.weight"] is not None:
            require_exact_control(cgroup / "cpu.weight", controls["cpu.weight"])

    default_irq_raw = read_text("/proc/irq/default_smp_affinity")
    if default_irq_raw is not None:
        try:
            default_irq_overlap = (
                parse_hex_cpu_mask(default_irq_raw) & selected_smt_cpus
            )
        except ValueError as error:
            failures.append(f"invalid default IRQ affinity: {error}")
        else:
            if default_irq_overlap:
                failures.append(
                    "default IRQ affinity moved onto the measured SMT closure: "
                    f"{sorted(default_irq_overlap)}"
                )

    try:
        observed_irqs = [
            path
            for path in Path("/proc/irq").iterdir()
            if path.name.isdigit()
        ]
    except OSError as error:
        failures.append(f"cannot enumerate /proc/irq: {error}")
        observed_irqs = []
    for irq_path in observed_irqs:
        list_path = irq_path / "effective_affinity_list"
        mask_path = irq_path / "effective_affinity"
        raw = read_irq_affinity_text(list_path, allow_empty=True)
        if raw is not None:
            parse = parse_cpu_list
        else:
            raw = read_irq_affinity_text(mask_path)
            if raw is None:
                if not irq_path.exists():
                    continue
                failures.append(
                    f"IRQ {irq_path.name} exposes no readable effective affinity"
                )
                continue
            parse = parse_hex_cpu_mask
        try:
            overlap = parse(raw) & selected_smt_cpus
        except ValueError as error:
            failures.append(
                f"IRQ {irq_path.name} has invalid effective affinity: {error}"
            )
            continue
        if overlap:
            failures.append(
                f"IRQ {irq_path.name} moved onto the measured SMT closure: "
                f"{sorted(overlap)}"
            )

    for policy_raw, controls in expected_stability["frequency_policies"].items():
        policy = Path(policy_raw)
        for name, expected in controls.items():
            require_exact_control(policy / name, expected, allow_empty=True)

    boost_paths = {
        "cpufreq.boost": "/sys/devices/system/cpu/cpufreq/boost",
        "intel_pstate.no_turbo": "/sys/devices/system/cpu/intel_pstate/no_turbo",
        "amd_pstate.status": "/sys/devices/system/cpu/amd_pstate/status",
    }
    for name, expected in expected_stability["boost_controls"].items():
        require_exact_control(boost_paths[name], expected)


observed_binary_pids = set()
observed_thread_ids = set()
observed_thread_names = set()
sample_count = 0
maximum_thread_count = 0
started = time.monotonic()
next_cgroup_scan = started
cgroup_scan_count = 0
next_control_scan = started
control_scan_count = 0
watchdog_start_time_ticks = process_start_time_ticks(watchdog_pid)
if watchdog_start_time_ticks is None:
    raise SystemExit("benchmark watchdog exited before affinity monitoring started")

while process_start_time_ticks(watchdog_pid) == watchdog_start_time_ticks:
    now = time.monotonic()
    if now >= next_cgroup_scan:
        cgroup_pids = partition_tree_pids()
        foreign = sorted(
            pid
            for pid in cgroup_pids
            if Path(f"/proc/{pid}").exists()
            and not descends_from(pid, runner_pid)
        )
        if foreign:
            failures.append(
                "isolated benchmark partition gained processes outside the "
                f"runner tree: {foreign}"
            )
        cgroup_scan_count += 1
        next_cgroup_scan = now + cgroup_poll_interval_seconds
    if now >= next_control_scan:
        scan_stability_controls()
        control_scan_count += 1
        next_control_scan = now + control_poll_interval_seconds
    binary_pids = [
        pid
        for pid in descendants(watchdog_pid)
        if executable(pid) == expected_binary
    ]
    for pid in binary_pids:
        if process_cgroup_path(pid) != expected_cgroup_path:
            failures.append(
                f"benchmark PID {pid} moved outside {expected_cgroup_path}"
            )
        observed_binary_pids.add(pid)
        task_paths = sorted(Path(f"/proc/{pid}/task").glob("[0-9]*"))
        maximum_thread_count = max(maximum_thread_count, len(task_paths))
        for task_path in task_paths:
            status_path = task_path / "status"
            affinity = status_value(status_path, "Cpus_allowed_list")
            if affinity is None:
                continue
            tid = int(task_path.name)
            observed_thread_ids.add(tid)
            name = status_value(status_path, "Name")
            if name is not None:
                observed_thread_names.add(name)
            if affinity != expected_affinity:
                failures.append(
                    f"PID {pid} TID {tid} affinity {affinity!r}, "
                    f"expected {expected_affinity!r}"
                )
        sample_count += 1
    if failures:
        break
    time.sleep(poll_interval_seconds)

verified = (
    not failures
    and bool(observed_binary_pids)
    and bool(observed_thread_ids)
    and sample_count > 0
    and cgroup_scan_count > 0
    and control_scan_count > 0
)
receipt = {
    "schema_version": "fsqlite-e2e.gate0-thread-affinity-receipt.v1",
    "generated_at_utc": datetime.datetime.now(datetime.timezone.utc)
    .replace(microsecond=0)
    .isoformat()
    .replace("+00:00", "Z"),
    "run_id": run_id,
    "watchdog_pid": watchdog_pid,
    "watchdog_start_time_ticks": watchdog_start_time_ticks,
    "expected_binary": expected_binary,
    "expected_cpus_allowed_list": expected_affinity,
    "expected_cgroup_path": str(expected_cgroup_path),
    "expected_partition_root": str(expected_partition_root),
    "expected_topology_receipt": str(topology_receipt_path.resolve()),
    "expected_selected_online_smt_closure": selected_smt_closure,
    "runner_pid": runner_pid,
    "poll_interval_seconds": poll_interval_seconds,
    "cgroup_poll_interval_seconds": cgroup_poll_interval_seconds,
    "cgroup_scan_count": cgroup_scan_count,
    "control_poll_interval_seconds": control_poll_interval_seconds,
    "control_scan_count": control_scan_count,
    "monitor_elapsed_seconds": time.monotonic() - started,
    "sample_count": sample_count,
    "observed_binary_pids": sorted(observed_binary_pids),
    "observed_thread_count": len(observed_thread_ids),
    "maximum_simultaneous_thread_count": maximum_thread_count,
    "observed_thread_names": sorted(observed_thread_names),
    "failures": failures,
    "verified": verified,
}
try:
    with output_path.open("x", encoding="utf-8") as handle:
        json.dump(receipt, handle, indent=2, sort_keys=True)
        handle.write("\n")
except OSError as error:
    raise SystemExit(f"cannot create affinity receipt {output_path}: {error}")
if not verified:
    raise SystemExit("benchmark per-thread affinity verification failed")
PY
}

wait_for_quiet_host() {
    local deadline=$((SECONDS + run_timeout_seconds))
    local next_report=${SECONDS}
    local probe_output
    local probe_status

    while true; do
        set +e
        probe_output="$(
            taskset -c "${monitor_cpu}" \
                "${python_bin}" - "${max_load_average_1m}" <<'PY'
import sys
from pathlib import Path

max_load_1m = float(sys.argv[1])


def pressure_average(path, window):
    try:
        lines = Path(path).read_text(encoding="utf-8").splitlines()
    except OSError as error:
        print(f"cannot read {path}: {error}")
        raise SystemExit(2)
    for line in lines:
        fields = line.split()
        if fields and fields[0] == "some":
            for field in fields[1:]:
                if field.startswith(f"{window}="):
                    try:
                        return float(field.split("=", 1)[1])
                    except ValueError:
                        print(f"invalid {window} value in {path}: {field!r}")
                        raise SystemExit(2)
    print(f"{path} omits some {window}")
    raise SystemExit(2)


try:
    load_1m = float(Path("/proc/loadavg").read_text().split()[0])
except (OSError, IndexError, ValueError) as error:
    print(f"cannot read one-minute load average: {error}")
    raise SystemExit(2)
cpu_some_avg10 = pressure_average("/proc/pressure/cpu", "avg10")
io_some_avg60 = pressure_average("/proc/pressure/io", "avg60")
print(
    f"load1={load_1m:.3f}, cpu.some.avg10={cpu_some_avg10:.3f}, "
    f"io.some.avg60={io_some_avg60:.3f}"
)
raise SystemExit(
    0
    if (
        load_1m <= max_load_1m
        and cpu_some_avg10 <= 1.0
        and io_some_avg60 <= 0.10
    )
    else 1
)
PY
        )"
        probe_status=$?
        set -e
        case "${probe_status}" in
            0)
                printf 'Host settled for measurement: %s\n' "${probe_output}" >&2
                return
                ;;
            1)
                ;;
            *)
                die "host-quiet probe failed: ${probe_output}"
                ;;
        esac
        ((SECONDS < deadline)) ||
            die "host did not settle before the measurement deadline; last probe: ${probe_output}"
        if ((SECONDS >= next_report)); then
            printf 'Waiting for post-build load/pressure to settle: %s\n' \
                "${probe_output}" >&2
            next_report=$((SECONDS + 30))
        fi
        sleep 5
    done
}

wait_for_quiet_host
capture_stable_topology_snapshot \
    "${artifact_dir}/topology-snapshots/post-build.json" \
    "post-build" \
    true
taskset -pc "${monitor_cpu}" "$$" \
    >"${artifact_dir}/topology-snapshots/runner-monitor-affinity.log"
runner_cpu_list="$(
    sed -n 's/^Cpus_allowed_list:[[:space:]]*//p' "/proc/$$/status"
)"
[[ "${runner_cpu_list}" == "${monitor_cpu}" ]] ||
    die "could not bind the outer runner to monitor CPU ${monitor_cpu}"

run_one() {
    local sequence_number=$1
    local group_index=$2
    local group_pattern=$3
    local group_slot=$4
    local seed=$5
    local profile=$6
    local run_id=$7
    local binary="${artifact_dir}/binaries/${profile}/comprehensive-bench"
    local build_log="${artifact_dir}/build-receipts/${profile}/build-vv.log"
    local report_path="${artifact_dir}/reports/${run_id}.json"
    local stdout_path="${artifact_dir}/reports/${run_id}.stdout.log"
    local stderr_path="${artifact_dir}/reports/${run_id}.stderr.log"
    local verifier_path="${artifact_dir}/verification-receipts/${run_id}.json"
    local watchdog_path="${artifact_dir}/watchdog-receipts/${run_id}.json"
    local affinity_path="${artifact_dir}/topology-snapshots/${run_id}-threads.json"
    local topology_before_path="${artifact_dir}/topology-snapshots/${run_id}-before.json"
    local topology_after_path="${artifact_dir}/topology-snapshots/${run_id}-after.json"
    local started_at
    local finished_at
    local run_status
    local affinity_status
    local watchdog_pid
    local affinity_monitor_pid
    local report_sha
    local binary_sha

    wait_for_quiet_host
    capture_stable_topology_snapshot \
        "${topology_before_path}" \
        "before:${run_id}" \
        true
    started_at="$(date --utc +%Y-%m-%dT%H:%M:%SZ)"
    set +e
    taskset -c "${monitor_cpu}" \
        timeout --foreground --signal=TERM --kill-after=30s \
            "${run_timeout_seconds}s" \
            taskset -c "${taskset_cpu_list}" \
            env -i \
            "HOME=${clean_home}" \
            "USER=${clean_user}" \
            "LOGNAME=${clean_user}" \
            "PATH=${clean_path}" \
            "LC_ALL=C" \
            "CARGO_HOME=${isolated_cargo_home}" \
            "RUSTUP_HOME=${rustup_home}" \
            "FSQLITE_BENCH_BUILD_LOG_PATH=${build_log}" \
            "FSQLITE_BENCH_EXPECTED_CPU_AFFINITY=${proc_status_cpu_list}" \
            "FSQLITE_BENCH_MAX_LOAD_1M=${max_load_average_1m}" \
            "FSQLITE_BENCH_SOURCE_ROOT=${REPO_ROOT}" \
            "${binary}" \
            --bridge-experiment \
            --bridge-samples "${samples_per_arm}" \
            --bridge-operations "${operation_count}" \
            --bridge-seed "${seed}" \
            --allow-unverified-provenance \
            --json-out "${report_path}" \
            >"${stdout_path}" 2>"${stderr_path}" &
    watchdog_pid=$!
    monitor_benchmark_affinity \
        "${watchdog_pid}" "${binary}" "${affinity_path}" "${run_id}" &
    affinity_monitor_pid=$!
    wait "${watchdog_pid}"
    run_status=$?
    wait "${affinity_monitor_pid}"
    affinity_status=$?
    set -e
    finished_at="$(date --utc +%Y-%m-%dT%H:%M:%SZ)"
    capture_stable_topology_snapshot \
        "${topology_after_path}" \
        "after:${run_id}" \
        false
    jq -n \
        --arg run_id "${run_id}" \
        --arg profile "${profile}" \
        --arg started_at_utc "${started_at}" \
        --arg finished_at_utc "${finished_at}" \
        --argjson seed "${seed}" \
        --argjson watchdog_pid "${watchdog_pid}" \
        --argjson timeout_seconds "${run_timeout_seconds}" \
        --argjson exit_status "${run_status}" \
        '{
          schema_version:"fsqlite-e2e.gate0-watchdog-receipt.v1",
          run_id:$run_id,
          profile:$profile,
          seed:$seed,
          watchdog_pid:$watchdog_pid,
          mechanism:"GNU timeout TERM then KILL after 30 seconds",
          timeout_seconds:$timeout_seconds,
          started_at_utc:$started_at_utc,
          finished_at_utc:$finished_at_utc,
          exit_status:$exit_status,
          timed_out:($exit_status == 124 or $exit_status == 137),
          verified:($exit_status == 0)
        }' >"${watchdog_path}"
    [[ ${run_status} -eq 0 ]] ||
        die "${run_id} failed or timed out with status ${run_status}; no manifest will be emitted"
    [[ ${affinity_status} -eq 0 ]] ||
        die "${run_id} failed per-thread affinity verification"
    [[ -s "${report_path}" ]] ||
        die "${run_id} produced no bridge report"

    # Report verification is also control-plane work and must not warm or
    # throttle the CPUs used by the following measurement block.
    taskset -c "${monitor_cpu}" \
        env -i \
        "HOME=${clean_home}" \
        "USER=${clean_user}" \
        "LOGNAME=${clean_user}" \
        "PATH=${clean_path}" \
        "LC_ALL=C" \
        "CARGO_HOME=${isolated_cargo_home}" \
        "RUSTUP_HOME=${rustup_home}" \
        "${binary}" --verify-bridge-report "${report_path}" \
        >"${verifier_path}"
    jq -e \
        '
          .schema_version == "fsqlite-e2e.bridge-verification-receipt.v1"
          and .report_contract_verified == true
          and .diagnostic_only == true
          and .inner_provenance_citable == false
          and .exact_schedule_verified == true
          and .arm_statistics_recomputed == true
          and .ready_regression_recomputed == true
          and .paired_comparisons_verified == 9
          and .exact_route_receipts_verified == .raw_sample_count
          and .aa_null_comparison_verified == true
          and .replay_contract_verified == true
        ' "${verifier_path}" >/dev/null ||
        die "${run_id} failed frozen-binary report verification"

    report_sha="$(sha256sum -- "${report_path}" | awk '{print $1}')"
    binary_sha="$(sha256sum -- "${binary}" | awk '{print $1}')"
    [[ "$(jq -er '.report_sha256' "${verifier_path}")" == "${report_sha}" ]] ||
        die "${run_id} verifier report hash mismatch"
    [[ "$(jq -er '.frozen_verifier_binary_sha256' "${verifier_path}")" == "${binary_sha}" ]] ||
        die "${run_id} verifier binary hash mismatch"
    jq -n \
        --arg run_id "${run_id}" \
        --arg group_pattern "${group_pattern}" \
        --arg profile "${profile}" \
        --arg report_path "${report_path}" \
        --arg report_sha256 "${report_sha}" \
        --arg verifier_path "${verifier_path}" \
        --arg verifier_sha256 "$(sha256sum -- "${verifier_path}" | awk '{print $1}')" \
        --arg frozen_binary_sha256 "${binary_sha}" \
        --arg watchdog_receipt_path "${watchdog_path}" \
        --arg watchdog_receipt_sha256 "$(sha256sum -- "${watchdog_path}" | awk '{print $1}')" \
        --arg affinity_receipt_path "${affinity_path}" \
        --arg affinity_receipt_sha256 "$(sha256sum -- "${affinity_path}" | awk '{print $1}')" \
        --arg topology_before_path "${topology_before_path}" \
        --arg topology_before_sha256 "$(sha256sum -- "${topology_before_path}" | awk '{print $1}')" \
        --arg topology_after_path "${topology_after_path}" \
        --arg topology_after_sha256 "$(sha256sum -- "${topology_after_path}" | awk '{print $1}')" \
        --arg topology_fingerprint "${initial_topology_fingerprint}" \
        --argjson sequence "${sequence_number}" \
        --argjson group_index "${group_index}" \
        --argjson group_slot "${group_slot}" \
        --argjson seed "${seed}" \
        '{
          schema_version:"fsqlite-e2e.gate0-run-receipt.v1",
          run_id:$run_id,
          sequence:$sequence,
          group_index:$group_index,
          group_pattern:$group_pattern,
          group_slot:$group_slot,
          seed:$seed,
          profile:$profile,
          report_path:$report_path,
          report_sha256:$report_sha256,
          verifier_path:$verifier_path,
          verifier_sha256:$verifier_sha256,
          frozen_binary_sha256:$frozen_binary_sha256,
          watchdog_receipt_path:$watchdog_receipt_path,
          watchdog_receipt_sha256:$watchdog_receipt_sha256,
          affinity_receipt_path:$affinity_receipt_path,
          affinity_receipt_sha256:$affinity_receipt_sha256,
          topology_before_path:$topology_before_path,
          topology_before_sha256:$topology_before_sha256,
          topology_after_path:$topology_after_path,
          topology_after_sha256:$topology_after_sha256,
          topology_fingerprint:$topology_fingerprint,
          watchdog_verified:true,
          per_thread_affinity_verified:true,
          topology_stability_verified:true,
          report_contract_verified:true
        }' >"${artifact_dir}/reports/${run_id}-receipt.json"
    publish_to_cas bridge-report "${report_path}"
    publish_to_cas bridge-verification-receipt "${verifier_path}"
    publish_to_cas gate0-thread-affinity-receipt "${affinity_path}"
    publish_to_cas gate0-topology-snapshot "${topology_before_path}"
    publish_to_cas gate0-topology-snapshot "${topology_after_path}"
}

while IFS=$'\t' read -r sequence group_index group_pattern group_slot seed profile run_id; do
    [[ "${sequence}" == sequence ]] && continue
    printf 'Running %s (%s, seed %s, %s slot %s)...\n' \
        "${run_id}" "${profile}" "${seed}" "${group_pattern}" "${group_slot}" >&2
    run_one \
        "${sequence}" "${group_index}" "${group_pattern}" "${group_slot}" \
        "${seed}" "${profile}" "${run_id}"
done <"${order_path}"

final_topology_path="${artifact_dir}/topology-snapshots/final.json"
wait_for_quiet_host
capture_stable_topology_snapshot "${final_topology_path}" "final" true

timeout --foreground --signal=TERM --kill-after=30s \
    "${run_timeout_seconds}s" \
    "${python_bin}" - \
    "${artifact_dir}" \
    "${seed_count}" \
    "${aa_max_abs_log_ratio}" <<'PY'
import csv
import hashlib
import itertools
import json
import math
import random
import sys
from pathlib import Path

artifact = Path(sys.argv[1])
seed_count = int(sys.argv[2])
aa_limit = float(sys.argv[3])
confidence = 0.991667
analysis_path = artifact / "analysis.json"


def fail(message):
    raise SystemExit(f"gate0 analyzer: {message}")


def load_json(path):
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError) as error:
        fail(f"cannot read {path}: {error}")


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


artifact = artifact.resolve()


def exact_artifact_path(raw_path, expected_relative, context):
    expected = (artifact / expected_relative).resolve()
    try:
        observed = Path(raw_path).resolve(strict=True)
    except (OSError, TypeError) as error:
        fail(f"{context} is not a resolvable file path: {error}")
    if observed != expected:
        fail(f"{context} points to {observed}, expected {expected}")
    if artifact != observed and artifact not in observed.parents:
        fail(f"{context} escapes the artifact bundle: {observed}")
    return observed


def require_exact_keys(value, expected, context):
    if not isinstance(value, dict):
        fail(f"{context} is not an object")
    observed = set(value)
    expected = set(expected)
    if observed != expected:
        fail(
            f"{context} key set drifted; "
            f"missing={sorted(expected - observed)}, "
            f"unexpected={sorted(observed - expected)}"
        )


protocol_path = artifact / "protocol.json"
protocol = load_json(protocol_path)
if (
    protocol.get("schema_version")
    != "fsqlite-e2e.gate0-async-bridge-protocol.v1"
    or protocol.get("profiles") != ["release", "release-perf"]
    or protocol.get("independent_seed_count") != seed_count
    or float(protocol.get("aa_max_abs_log_ratio", -1.0)) != aa_limit
    or protocol.get("inner_reports_are_diagnostic_only") is not True
    or protocol.get("ancestor_cargo_configs_absent") is not True
):
    fail("protocol receipt does not match the preregistered analyzer inputs")
source_git_sha = protocol.get("source_git_sha")
if (
    not isinstance(source_git_sha, str)
    or len(source_git_sha) != 40
    or any(character not in "0123456789abcdef" for character in source_git_sha)
    or protocol.get("origin_main_sha") != source_git_sha
):
    fail("protocol source/origin SHA receipt is malformed or mismatched")
seed_base = protocol.get("seed_base")
if not isinstance(seed_base, int) or seed_base < 0:
    fail("protocol seed_base is not a non-negative integer")

build_receipts = {}
for profile in ("release", "release-perf"):
    build_receipt_path = (
        artifact / "build-receipts" / profile / "receipt.json"
    )
    build_receipt = load_json(build_receipt_path)
    frozen_binary = exact_artifact_path(
        build_receipt.get("frozen_binary_path"),
        Path("binaries") / profile / "comprehensive-bench",
        f"{profile} frozen binary path",
    )
    if (
        build_receipt.get("schema_version")
        != "fsqlite-e2e.gate0-frozen-build-receipt.v1"
        or build_receipt.get("profile") != profile
        or build_receipt.get("source_git_sha") != source_git_sha
        or build_receipt.get("cargo_locked") is not True
        or build_receipt.get("verified") is not True
        or build_receipt.get("effective_cargo_configuration", {}).get(
            "ancestor_cargo_configs_absent"
        )
        is not True
        or not isinstance(build_receipt.get("build_nonce"), str)
        or len(build_receipt.get("build_nonce")) != 64
        or any(
            character not in "0123456789abcdef"
            for character in build_receipt.get("build_nonce")
        )
        or sha256(frozen_binary)
        != build_receipt.get("frozen_binary_sha256")
    ):
        fail(f"{profile} frozen-build receipt is invalid")
    build_log = exact_artifact_path(
        build_receipt.get("build_log_path"),
        Path("build-receipts") / profile / "build-vv.log",
        f"{profile} build log path",
    )
    if sha256(build_log) != build_receipt.get("build_log_sha256"):
        fail(f"{profile} build log hash drifted")
    build_events_path = (
        artifact / "build-receipts" / profile / "build-events.jsonl"
    )
    if sha256(build_events_path) != build_receipt.get("build_events_sha256"):
        fail(f"{profile} build event-stream hash drifted")
    schema_path = artifact / "schemas" / f"{profile}-bridge-schema.json"
    if sha256(schema_path) != build_receipt.get("bridge_schema_sha256"):
        fail(f"{profile} bridge schema hash drifted")
    build_receipts[profile] = build_receipt


initial_topology_path = artifact / "topology-receipt.json"
final_topology_path = artifact / "topology-snapshots" / "final.json"
initial_topology = load_json(initial_topology_path)
final_topology = load_json(final_topology_path)
if (
    initial_topology.get("schema_version")
    != "fsqlite-e2e.gate0-topology-receipt.v2"
    or initial_topology.get("verified") is not True
    or initial_topology.get("quiet_host_required") is not True
    or initial_topology.get("quiet_host_verified") is not True
    or initial_topology.get("source_git_sha") != source_git_sha
    or sha256(initial_topology_path)
    != protocol.get("topology_receipt_sha256")
):
    fail("initial live topology receipt is not verified v2 evidence")
initial_topology_fingerprint = initial_topology.get("stability_fingerprint")
if (
    not isinstance(initial_topology_fingerprint, str)
    or len(initial_topology_fingerprint) != 64
):
    fail("initial topology receipt has no SHA-256 stability fingerprint")
if (
    final_topology.get("verified") is not True
    or final_topology.get("quiet_host_required") is not True
    or final_topology.get("quiet_host_verified") is not True
    or final_topology.get("source_git_sha") != source_git_sha
    or final_topology.get("stability_fingerprint")
    != initial_topology_fingerprint
):
    fail("final topology receipt drifted from the initial live state")


def sign_flip_p_value(values):
    observed = abs(sum(values) / len(values))
    exceedances = 0
    assignments = 0
    for signs in itertools.product((-1.0, 1.0), repeat=len(values)):
        statistic = abs(sum(sign * value for sign, value in zip(signs, values)) / len(values))
        assignments += 1
        if statistic + 1e-15 >= observed:
            exceedances += 1
    return exceedances / assignments


def simultaneous_bootstrap_interval(values, seed):
    rng = random.Random(seed)
    means = []
    for _ in range(200_000):
        means.append(sum(rng.choice(values) for _ in values) / len(values))
    means.sort()
    tail = (1.0 - confidence) / 2.0
    low_index = max(0, min(len(means) - 1, math.floor(tail * len(means))))
    high_index = max(
        0,
        min(len(means) - 1, math.ceil((1.0 - tail) * len(means)) - 1),
    )
    return means[low_index], means[high_index]


with (artifact / "run-order.tsv").open("r", encoding="utf-8", newline="") as handle:
    reader = csv.DictReader(handle, delimiter="\t")
    if reader.fieldnames != [
        "sequence",
        "group_index",
        "group_pattern",
        "slot",
        "seed",
        "profile",
        "run_id",
    ]:
        fail(f"run-order header drifted: {reader.fieldnames!r}")
    order = list(reader)
if len(order) != seed_count * 2:
    fail(f"run order has {len(order)} rows, expected {seed_count * 2}")

reports = {}
profile_seed_pairs = set()
for expected_sequence, row in enumerate(order, start=1):
    if int(row["sequence"]) != expected_sequence:
        fail("run order sequence is not contiguous")
    group_index = int(row["group_index"])
    slot = int(row["slot"])
    expected_group_index = (expected_sequence - 1) // 4
    expected_slot = (expected_sequence - 1) % 4 + 1
    if group_index != expected_group_index or slot != expected_slot:
        fail(
            f"run order row {expected_sequence} has group/slot "
            f"{group_index}/{slot}, expected {expected_group_index}/{expected_slot}"
        )
    expected_pattern = "ABBA" if group_index % 2 == 0 else "BAAB"
    expected_profiles = (
        ["release", "release-perf", "release-perf", "release"]
        if expected_pattern == "ABBA"
        else ["release-perf", "release", "release", "release-perf"]
    )
    if row["group_pattern"] != expected_pattern:
        fail(f"group {group_index} does not alternate ABBA/BAAB")
    if row["profile"] != expected_profiles[slot - 1]:
        fail(f"group {group_index} slot {slot} violates {expected_pattern}")
    expected_seed = seed_base + group_index * 2 + (0 if slot <= 2 else 1)
    if int(row["seed"]) != expected_seed:
        fail(
            f"group {group_index} slot {slot} uses seed {row['seed']}, "
            f"expected {expected_seed}"
        )
    expected_run_id = (
        f"{expected_sequence:03d}-seed-{expected_seed}-{row['profile']}"
    )
    if row["run_id"] != expected_run_id:
        fail(
            f"run order row {expected_sequence} has run_id {row['run_id']!r}, "
            f"expected {expected_run_id!r}"
        )

    run_id = row["run_id"]
    receipt_path = artifact / "reports" / f"{run_id}-receipt.json"
    receipt = load_json(receipt_path)
    require_exact_keys(
        receipt,
        {
            "schema_version",
            "run_id",
            "sequence",
            "group_index",
            "group_pattern",
            "group_slot",
            "seed",
            "profile",
            "report_path",
            "report_sha256",
            "verifier_path",
            "verifier_sha256",
            "frozen_binary_sha256",
            "watchdog_receipt_path",
            "watchdog_receipt_sha256",
            "affinity_receipt_path",
            "affinity_receipt_sha256",
            "topology_before_path",
            "topology_before_sha256",
            "topology_after_path",
            "topology_after_sha256",
            "topology_fingerprint",
            "watchdog_verified",
            "per_thread_affinity_verified",
            "topology_stability_verified",
            "report_contract_verified",
        },
        f"{run_id} run receipt",
    )
    if (
        receipt.get("schema_version")
        != "fsqlite-e2e.gate0-run-receipt.v1"
        or receipt.get("run_id") != run_id
        or receipt.get("sequence") != expected_sequence
        or receipt.get("group_index") != group_index
        or receipt.get("group_pattern") != expected_pattern
        or receipt.get("group_slot") != slot
        or receipt.get("seed") != expected_seed
        or receipt.get("profile") != row["profile"]
        or not receipt.get("watchdog_verified")
        or not receipt.get("report_contract_verified")
        or not receipt.get("per_thread_affinity_verified")
        or not receipt.get("topology_stability_verified")
    ):
        fail(f"{run_id} run receipt contradicts the exact order or proof contract")
    report_path = exact_artifact_path(
        receipt["report_path"],
        Path("reports") / f"{run_id}.json",
        f"{run_id} report path",
    )
    verifier_path = exact_artifact_path(
        receipt["verifier_path"],
        Path("verification-receipts") / f"{run_id}.json",
        f"{run_id} verifier path",
    )
    affinity_path = exact_artifact_path(
        receipt["affinity_receipt_path"],
        Path("topology-snapshots") / f"{run_id}-threads.json",
        f"{run_id} affinity path",
    )
    watchdog_path = exact_artifact_path(
        receipt["watchdog_receipt_path"],
        Path("watchdog-receipts") / f"{run_id}.json",
        f"{run_id} watchdog path",
    )
    topology_before_path = exact_artifact_path(
        receipt["topology_before_path"],
        Path("topology-snapshots") / f"{run_id}-before.json",
        f"{run_id} pre-run topology path",
    )
    topology_after_path = exact_artifact_path(
        receipt["topology_after_path"],
        Path("topology-snapshots") / f"{run_id}-after.json",
        f"{run_id} post-run topology path",
    )
    if sha256(report_path) != receipt["report_sha256"]:
        fail(f"{run_id} report hash drifted")
    if sha256(verifier_path) != receipt["verifier_sha256"]:
        fail(f"{run_id} verifier receipt hash drifted")
    if sha256(affinity_path) != receipt["affinity_receipt_sha256"]:
        fail(f"{run_id} affinity receipt hash drifted")
    if sha256(watchdog_path) != receipt["watchdog_receipt_sha256"]:
        fail(f"{run_id} watchdog receipt hash drifted")
    if sha256(topology_before_path) != receipt["topology_before_sha256"]:
        fail(f"{run_id} pre-run topology receipt hash drifted")
    if sha256(topology_after_path) != receipt["topology_after_sha256"]:
        fail(f"{run_id} post-run topology receipt hash drifted")
    frozen_binary_path = (
        artifact
        / "binaries"
        / row["profile"]
        / "comprehensive-bench"
    )
    expected_binary_sha = build_receipts[row["profile"]][
        "frozen_binary_sha256"
    ]
    if (
        receipt.get("frozen_binary_sha256") != expected_binary_sha
        or sha256(frozen_binary_path) != expected_binary_sha
    ):
        fail(f"{run_id} is not bound to the frozen {row['profile']} binary")
    affinity = load_json(affinity_path)
    watchdog = load_json(watchdog_path)
    require_exact_keys(
        watchdog,
        {
            "schema_version",
            "run_id",
            "profile",
            "seed",
            "watchdog_pid",
            "mechanism",
            "timeout_seconds",
            "started_at_utc",
            "finished_at_utc",
            "exit_status",
            "timed_out",
            "verified",
        },
        f"{run_id} watchdog receipt",
    )
    require_exact_keys(
        affinity,
        {
            "schema_version",
            "generated_at_utc",
            "run_id",
            "watchdog_pid",
            "watchdog_start_time_ticks",
            "expected_binary",
            "expected_cpus_allowed_list",
            "expected_cgroup_path",
            "expected_partition_root",
            "expected_topology_receipt",
            "expected_selected_online_smt_closure",
            "runner_pid",
            "poll_interval_seconds",
            "cgroup_poll_interval_seconds",
            "cgroup_scan_count",
            "control_poll_interval_seconds",
            "control_scan_count",
            "monitor_elapsed_seconds",
            "sample_count",
            "observed_binary_pids",
            "observed_thread_count",
            "maximum_simultaneous_thread_count",
            "observed_thread_names",
            "failures",
            "verified",
        },
        f"{run_id} affinity receipt",
    )
    if (
        watchdog.get("schema_version")
        != "fsqlite-e2e.gate0-watchdog-receipt.v1"
        or watchdog.get("run_id") != run_id
        or watchdog.get("profile") != row["profile"]
        or watchdog.get("seed") != expected_seed
        or watchdog.get("watchdog_pid") != affinity.get("watchdog_pid")
        or watchdog.get("exit_status") != 0
        or watchdog.get("timed_out") is not False
        or watchdog.get("verified") is not True
    ):
        fail(f"{run_id} lacks an exact successful watchdog receipt")
    if (
        affinity.get("schema_version")
        != "fsqlite-e2e.gate0-thread-affinity-receipt.v1"
        or affinity.get("run_id") != run_id
        or affinity.get("verified") is not True
        or not isinstance(affinity.get("watchdog_start_time_ticks"), int)
        or affinity.get("watchdog_start_time_ticks") <= 0
        or affinity.get("sample_count", 0) <= 0
        or affinity.get("cgroup_scan_count", 0) <= 0
        or affinity.get("control_scan_count", 0) <= 0
        or affinity.get("observed_thread_count", 0) <= 0
        or affinity.get("failures") != []
        or affinity.get("expected_binary") != str(frozen_binary_path.resolve())
        or affinity.get("expected_cpus_allowed_list")
        != initial_topology["proc_status_cpu_list"]
        or affinity.get("expected_cgroup_path")
        != initial_topology["stability_state"]["cgroup_path"]
        or affinity.get("expected_partition_root")
        != initial_topology["stability_state"]["partition_root"]
        or affinity.get("expected_topology_receipt")
        != str(initial_topology_path.resolve())
        or affinity.get("expected_selected_online_smt_closure")
        != initial_topology["selected_online_smt_closure"]
        or affinity.get("runner_pid") != initial_topology["runner_pid"]
    ):
        fail(f"{run_id} lacks exact per-thread affinity evidence")
    topology_before = load_json(topology_before_path)
    topology_after = load_json(topology_after_path)
    for phase, topology in (
        ("pre-run", topology_before),
        ("post-run", topology_after),
    ):
        expected_phase = (
            f"before:{run_id}" if phase == "pre-run" else f"after:{run_id}"
        )
        if (
            topology.get("schema_version")
            != "fsqlite-e2e.gate0-topology-receipt.v2"
            or topology.get("verified") is not True
            or topology.get("quiet_host_required")
            != (phase == "pre-run")
            or (
                phase == "pre-run"
                and topology.get("quiet_host_verified") is not True
            )
            or topology.get("source_git_sha") != source_git_sha
            or topology.get("phase") != expected_phase
            or topology.get("stability_fingerprint")
            != initial_topology_fingerprint
        ):
            fail(f"{run_id} {phase} topology is not stable verified evidence")
    if receipt.get("topology_fingerprint") != initial_topology_fingerprint:
        fail(f"{run_id} receipt is not bound to the initial topology fingerprint")
    verifier = load_json(verifier_path)
    if (
        verifier.get("schema_version")
        != "fsqlite-e2e.bridge-verification-receipt.v1"
        or verifier.get("report_path") != str(report_path)
        or verifier.get("report_sha256") != receipt["report_sha256"]
        or verifier.get("frozen_verifier_binary_sha256")
        != expected_binary_sha
        or verifier.get("report_contract_verified") is not True
        or verifier.get("exact_schedule_verified") is not True
        or verifier.get("replay_contract_verified") is not True
    ):
        fail(f"{run_id} verifier does not bind the report hash")
    if not verifier.get("diagnostic_only") or verifier.get("inner_provenance_citable"):
        fail(f"{run_id} lost its diagnostic-only provenance label")

    report = load_json(report_path)
    if report.get("schema_version") != "fsqlite-e2e.bridge-experiment.v3":
        fail(f"{run_id} report schema drifted")
    if report["config"]["order_seed"] != int(row["seed"]):
        fail(f"{run_id} seed receipt drifted")
    report_build = report["provenance"]["build"]
    report_runtime_source = report["provenance"]["runtime_source"]
    expected_build = build_receipts[row["profile"]]
    if (
        report_build["selected_profile"] != row["profile"]
        or report_build["git_commit_sha"] != source_git_sha
        or report_build["build_nonce"] != expected_build["build_nonce"]
        or report_build["verbose_build_log_sha256"]
        != expected_build["build_log_sha256"]
        or report["provenance"]["binary_sha256"] != expected_binary_sha
        or report_runtime_source["git_commit_sha"] != source_git_sha
        or report_runtime_source["git_dirty"] is not False
    ):
        fail(f"{run_id} profile receipt drifted")
    if sorted(report["provenance"]["validation_errors"]) != sorted(
        [
            "the three-arm bridge has no fail-bounded watchdog around a wedged engine future or worker-facade call",
            "the three-arm bridge is diagnostic-only until citable runs prove an isolated cgroup-v2 cpuset partition covering selected CPUs and online SMT siblings, full-dynticks coverage, disjoint effective IRQ affinities, per-thread affinity, and stable selected-policy frequency controls",
        ]
    ):
        fail(f"{run_id} has provenance failures beyond the outer-protocol blockers")
    pair = (int(row["seed"]), row["profile"])
    if pair in profile_seed_pairs:
        fail(f"duplicate seed/profile run: {pair}")
    profile_seed_pairs.add(pair)
    reports[pair] = report

expected_pairs = {
    (seed, profile)
    for seed in range(seed_base, seed_base + seed_count)
    for profile in ("release", "release-perf")
}
if profile_seed_pairs != expected_pairs:
    fail("seed/profile matrix is incomplete or non-contiguous")

primary_specs = [
    (
        "per_operation_over_inside_existing",
        "per_operation_block_on",
        "inside_existing_runtime",
    ),
    (
        "worker_over_inside_existing",
        "worker_sync_facade",
        "inside_existing_runtime",
    ),
    (
        "worker_over_per_operation",
        "worker_sync_facade",
        "per_operation_block_on",
    ),
]
primary = []
aa_controls = []
for profile_index, profile in enumerate(("release", "release-perf")):
    profile_reports = [
        reports[(seed, profile)] for seed in sorted(seed for seed, p in reports if p == profile)
    ]
    aa_logs = []
    effect_logs = {name: [] for name, _, _ in primary_specs}
    for report in profile_reports:
        comparisons = report["paired_comparisons"]

        def find_comparison(numerator, denominator):
            matches = [
                comparison
                for comparison in comparisons
                if comparison["workload"] == "raw_execute_with_params"
                and comparison["numerator"] == numerator
                and comparison["denominator"] == denominator
            ]
            if len(matches) != 1:
                fail(
                    f"{profile} report has {len(matches)} {numerator}/{denominator} comparisons"
                )
            ratio = float(matches[0]["geomean_ratio"])
            if not math.isfinite(ratio) or ratio <= 0.0:
                fail(f"{profile} comparison has invalid ratio {ratio}")
            return math.log(ratio)

        aa_log = find_comparison(
            "aa_inside_runtime_replicate", "aa_inside_runtime_baseline"
        )
        aa_logs.append(aa_log)
        for name, numerator, denominator in primary_specs:
            uncorrected_log = find_comparison(numerator, denominator)
            effect_logs[name].append(
                {
                    "uncorrected": uncorrected_log,
                    "aa_bias_corrected": uncorrected_log - aa_log,
                }
            )

    aa_mean = sum(aa_logs) / len(aa_logs)
    aa_low, aa_high = simultaneous_bootstrap_interval(
        aa_logs, 0xAA0000 + profile_index
    )
    aa_p = sign_flip_p_value(aa_logs)
    if abs(aa_mean) > aa_limit:
        fail(
            f"{profile} A/A aggregate abs(log ratio) {abs(aa_mean):.6f} exceeds {aa_limit:.6f}"
        )
    if aa_low < -aa_limit or aa_high > aa_limit:
        fail(
            f"{profile} A/A simultaneous confidence interval "
            f"[{aa_low:.6f}, {aa_high:.6f}] is not wholly contained in "
            f"the equivalence envelope [-{aa_limit:.6f}, {aa_limit:.6f}]"
        )
    if aa_p < 0.025:
        fail(
            f"{profile} A/A sign-flip p-value {aa_p:.6f} violates the two-profile 0.025 null-control gate"
        )
    aa_controls.append(
        {
            "profile": profile,
            "independent_seeds": len(aa_logs),
            "independent_unit": (
                "one outer order seed; the 48 inner samples per arm are repeated "
                "measurements within that seed, not independent replicates"
            ),
            "geomean_ratio": math.exp(aa_mean),
            "mean_log_ratio": aa_mean,
            "simultaneous_ci_log_low": aa_low,
            "simultaneous_ci_log_high": aa_high,
            "exact_sign_flip_p_value": aa_p,
            "max_abs_log_ratio": aa_limit,
            "verified": True,
        }
    )

    for contrast_index, (name, numerator, denominator) in enumerate(primary_specs):
        uncorrected_logs = [
            effect["uncorrected"] for effect in effect_logs[name]
        ]
        corrected_logs = [
            effect["aa_bias_corrected"] for effect in effect_logs[name]
        ]
        low, high = simultaneous_bootstrap_interval(
            corrected_logs, 0xB00000 + profile_index * 100 + contrast_index
        )
        primary.append(
            {
                "profile": profile,
                "contrast": name,
                "numerator": numerator,
                "denominator": denominator,
                "independent_seeds": len(corrected_logs),
                "independent_unit": (
                    "one outer order seed; inner arm samples are block-level repeated "
                    "measurements and are not counted as independent"
                ),
                "bias_correction": (
                    "paired per seed: mechanism log-ratio minus that seed's "
                    "A/A replicate-over-baseline log-ratio"
                ),
                "uncorrected_geomean_ratio": math.exp(
                    sum(uncorrected_logs) / len(uncorrected_logs)
                ),
                "aa_bias_corrected_geomean_ratio": math.exp(
                    sum(corrected_logs) / len(corrected_logs)
                ),
                "simultaneous_confidence_level": confidence,
                "aa_bias_corrected_simultaneous_ci_ratio_low": math.exp(low),
                "aa_bias_corrected_simultaneous_ci_ratio_high": math.exp(high),
                "aa_bias_corrected_exact_sign_flip_p_value": sign_flip_p_value(
                    corrected_logs
                ),
            }
        )

if len(primary) != 6:
    fail(f"primary contrast family has {len(primary)} members, expected 6")
for profile in ("release", "release-perf"):
    guard_path = artifact / "guards" / f"{profile}-receipt.json"
    guard = load_json(guard_path)
    guard_log_path = artifact / "guards" / f"{profile}.log"
    guard_events_path = artifact / "guards" / f"{profile}-events.jsonl"
    if (
        guard.get("schema_version")
        != "fsqlite-e2e.gate0-executed-guard-receipt.v1"
        or guard.get("profile") != profile
        or guard.get("source_git_sha") != source_git_sha
        or guard.get("execution_kind") != "executed_integration_test_binary"
        or guard.get("compile_only") is not False
        or guard.get("executed_test_count") != 1
        or guard.get("test_result_summary_verified") is not True
        or guard.get("exit_status") != 0
        or guard.get("log_sha256") != sha256(guard_log_path)
        or guard.get("cargo_events_sha256") != sha256(guard_events_path)
        or guard.get("verified") is not True
    ):
        fail(f"{profile} persisted corruption guard lacks an execution receipt")

analysis = {
    "schema_version": "fsqlite-e2e.gate0-async-bridge-analysis.v1",
    "outer_protocol_verified": True,
    "inner_reports_citable": False,
    "publication_status": "validated_outer_candidate_requires_human_claim_review",
    "diagnostic_only": True,
    "reason": (
        "Inner v3 reports deliberately retain the topology/watchdog design blockers. "
        "This bundle validates the external receipts and analysis contract but does not "
        "itself authorize an uncited numeric README claim."
    ),
    "independent_seed_count": seed_count,
    "independent_unit": (
        "outer order seed; no inner sample is treated as an independent replicate"
    ),
    "profile_run_order": "alternating_ABBA_BAAB",
    "live_topology_verification": {
        "schema_version": initial_topology["schema_version"],
        "initial_receipt_sha256": sha256(initial_topology_path),
        "final_receipt_sha256": sha256(final_topology_path),
        "stability_fingerprint": initial_topology_fingerprint,
        "isolated_cgroup_v2_cpuset_verified": True,
        "isolated_partition_tree_exclusive_to_runner": True,
        "online_smt_siblings_covered": True,
        "single_numa_memory_node_verified": True,
        "unlimited_ancestor_cpu_quota_verified": True,
        "full_dynticks_verified": True,
        "effective_irq_affinity_disjoint": True,
        "per_run_per_thread_affinity_verified": True,
        "frequency_policy_stable": True,
    },
    "primary_contrast_family_size": len(primary),
    "simultaneous_confidence_level": confidence,
    "primary_contrasts": primary,
    "aa_null_controls": aa_controls,
    "executed_corruption_guard_profiles": ["release", "release-perf"],
    "manifest_policy": "MANIFEST.sha256 is installed only after this analysis succeeds",
}
with analysis_path.open("x", encoding="utf-8") as handle:
    json.dump(analysis, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY

publish_to_cas gate0-analysis "${artifact_dir}/analysis.json"
publish_to_cas gate0-protocol "${artifact_dir}/protocol.json"
publish_to_cas gate0-run-order "${order_path}"
publish_to_cas gate0-topology-snapshot \
    "${artifact_dir}/topology-snapshots/post-build.json"
publish_to_cas gate0-topology-snapshot "${final_topology_path}"

manifest_staging="${build_root}/MANIFEST.sha256.pending"
(
    cd -- "${artifact_dir}"
    find . -type f ! -name MANIFEST.sha256 -print0 |
        sort -z |
        xargs -0 sha256sum
) >"${manifest_staging}"
[[ -s "${manifest_staging}" ]] || die "manifest staging file is empty"

# This is deliberately the final write beneath artifact_dir. Any earlier
# failure leaves no MANIFEST.sha256, which makes partial bundles unmistakable.
install -m 0444 -- "${manifest_staging}" "${artifact_dir}/MANIFEST.sha256"
(
    cd -- "${artifact_dir}"
    sha256sum --check --strict MANIFEST.sha256 >/dev/null
) || die "final manifest validation failed"

printf 'Gate 0 outer-protocol candidate complete.\n' >&2
printf 'Artifacts: %s\n' "${artifact_dir}"
printf 'Retained build root: %s\n' "${build_root}"
printf 'Manifest: %s\n' "${artifact_dir}/MANIFEST.sha256"
