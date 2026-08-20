#!/usr/bin/env bash
# verify_e6_1_lane_placement.sh (bd-db300.5.6.1)
#
# Operator entrypoint for the E6.1 lane-placement map
# (docs/design/e6-1-lane-placement-map.md). Resolves the live CPU topology
# (sockets / NUMA nodes / LLC domains / SMT siblings), applies the placement
# synthesis for the four lane classes (PUB / WRK / HLP / REC), prints the
# resolved placement table, and emits a structured artifact manifest whose
# fields match db300_topology_interference_contract.toml so a later interference
# case can bind probe lanes exactly as the map prescribes.
#
# STATUS: topology resolution + manifest emission are functional; the concrete
# per-lane CPU-set synthesis is intentionally a documented skeleton (the map doc
# is the spec; the golden-fixture synthesis is the follow-on implementation).
set -u

ART_DIR="${E6_1_ARTIFACT_DIR:-/tmp/e6_1_lane_placement}"
mkdir -p "$ART_DIR"
MANIFEST="$ART_DIR/placement_manifest.json"
HW_CLASS="linux_x86_64_many_core_numa"   # db300_topology_interference_contract [global_defaults]

log() { printf '%s\n' "$*" >&2; }

# ---- 1. resolve live topology (portable: lscpu + /sys, no hwloc dependency) ----
SOCKETS=$(lscpu 2>/dev/null | awk -F: '/^Socket\(s\)/{gsub(/ /,"",$2);print $2}')
NUMA=$(lscpu 2>/dev/null | awk -F: '/^NUMA node\(s\)/{gsub(/ /,"",$2);print $2}')
ONLINE=$(nproc 2>/dev/null || echo 1)
TPC=$(lscpu 2>/dev/null | awk -F: '/^Thread\(s\) per core/{gsub(/ /,"",$2);print $2}')
: "${SOCKETS:=1}" "${NUMA:=1}" "${TPC:=1}"

# LLC (L3) domains: count distinct L3 shared_cpu_list maps under /sys.
llc_count() {
  local d seen="" list c=0
  for d in /sys/devices/system/cpu/cpu[0-9]*/cache/index3/shared_cpu_list; do
    [ -r "$d" ] || continue
    list=$(cat "$d" 2>/dev/null)
    case " $seen " in *" $list "*) ;; *) seen="$seen $list"; c=$((c+1));; esac
  done
  echo "${c:-0}"
}
LLC=$(llc_count); [ "$LLC" -gt 0 ] 2>/dev/null || LLC=$NUMA

FALLBACK=""
[ "$NUMA" -le 1 ] 2>/dev/null && FALLBACK="flat_topology"
[ "$LLC" -le 0 ] 2>/dev/null && FALLBACK="${FALLBACK:+$FALLBACK,}llc_unknown"

log "== E6.1 live topology =="
log "hardware_class_id = $HW_CLASS"
log "sockets=$SOCKETS numa_nodes=$NUMA llc_domains=$LLC online_cpus=$ONLINE smt_threads_per_core=$TPC"
[ -n "$FALLBACK" ] && log "FALLBACK ENGAGED: $FALLBACK (see map doc 'Fallback' rows)"

# ---- 2. lane-class placement synthesis (skeleton; budgets from the map doc) ----
# home node = NUMA node 0 by convention (owns global next_commit_seq + VersionStore).
HOME_NODE=0
print_row() { printf '  %-4s | numa=%-6s | smt=%-14s | wake<=%-6s | remote_hitm=%s\n' "$@"; }
log ""
log "== resolved placement (see docs/design/e6-1-lane-placement-map.md) =="
print_row PUB "$HOME_NODE(home)" "no-sibling"   "10us"  "~0/commit"
print_row WRK "all(spread)"      "both-or-hlp"  "n/a"   "<=1/first-touch"
print_row HLP "per-socket"       "wrk-sibling"  "100us" "unbudgeted-bg"
print_row REC "$HOME_NODE(home)" "spillover"    "1s"    "unbudgeted"

# ---- 3. emit structured manifest (contract-shaped) ----
ts="${SOURCE_DATE_EPOCH:-manual}"   # avoid nondeterministic clock in CI provenance
{
  printf '{\n'
  printf '  "schema": "e6_1_lane_placement.v1",\n'
  printf '  "hardware_class_id": "%s",\n' "$HW_CLASS"
  printf '  "topology": {"sockets": %s, "numa_nodes": %s, "llc_domains": %s, "online_cpus": %s, "smt_threads_per_core": %s},\n' \
    "$SOCKETS" "$NUMA" "$LLC" "$ONLINE" "$TPC"
  printf '  "fallback": "%s",\n' "$FALLBACK"
  printf '  "home_node": %s,\n' "$HOME_NODE"
  printf '  "lanes": [\n'
  printf '    {"class":"PUB","numa":%s,"smt":"no-sibling","wake_to_run_budget_us":10,"remote_hitm_budget":"~0 per commit (next_commit_seq fetch-add)"},\n' "$HOME_NODE"
  printf '    {"class":"WRK","numa":"spread","smt":"both-or-reserve-sibling","wake_to_run_budget_us":10,"remote_hitm_budget":"<=1 per first-touch"},\n'
  printf '    {"class":"HLP","numa":"per-socket","smt":"wrk-sibling","wake_to_run_budget_us":100,"remote_hitm_budget":"background"},\n'
  printf '    {"class":"REC","numa":%s,"smt":"spillover","wake_to_run_budget_us":1000000,"remote_hitm_budget":"background"}\n' "$HOME_NODE"
  printf '  ]\n}\n'
} > "$MANIFEST"

log ""
log "manifest: $MANIFEST"
cat "$MANIFEST"
# Exit 0 = resolved; non-zero would indicate a topology-resolution hard failure.
