# E6.1 — Lane-Placement Map (`bd-db300.5.6.1`)

> **Status:** derived placement map (v1). First concrete step of E6
> (`bd-db300.5.6`) per its operator sequencing. Consumed by `bd-db300.5.6.2`
> (arena placement / handoff boundaries) and `bd-db300.5.6.3` (SMT-sibling +
> remote-HITM budgets), and by the interference verification beads.

## Purpose

Turn the logical pipeline stages and state-placement tiers already designed in
`many-core-transaction-pipeline-state-placement.md` (`bd-db300.5.1.1`) into a
concrete, operator-auditable placement map: for each lane class, name the
intended CPU set, NUMA node, LLC/CCD domain, SMT-sibling policy, home-node data
and allocator-arena ownership, allowed cross-node handoff boundaries, remote-HITM
and wake-to-run budgets, and the fallback when topology information is incomplete.

This document is a **plan**, not an implementation. It must be consumable
mechanically by `scripts/verify_e6_1_lane_placement.sh` (below) and by the
`db300_topology_interference_contract.toml` verification cases.

## Grounding (do not re-derive; cite)

1. **State tiers** — `many-core-transaction-pipeline-state-placement.md` §"State
   Placement Contract": lane-local / NUMA-local / socket-local-or-narrow-global /
   global. Boundaries A–D (§"Ownership Boundaries") define cross-tier transitions;
   Boundary C (prepared→publish, Stage 4→5) is Track E's focus.
2. **Lane taxonomy + wake-to-run budgets** —
   `queue-depth-wake-to-run-and-helper-lane-budgets.md`: Lane 0 Writer (per-core),
   Lane 1 Wakeup-Dispatch (≤10μs), Lane 2 Evidence (≤1ms), Lane 3 GC (≤100ms),
   Lane 4 Checkpoint (≤1s), Lane 5 Invalidation (≤100μs). p50 write budget ≤55μs
   (50μs WAL + 5μs IF); `:memory:` IC ≤1μs.
3. **Topology model + measurement contract** —
   `db300_topology_interference_contract.toml`:
   `hardware_class_id = "linux_x86_64_many_core_numa"`; structured-log +
   artifact-layout requirements; interference cases (e.g. co-locate two probe
   lanes on one logical CPU to force single-core interference even without SMT).

## Reference topology classes

Placement is expressed against two representative classes so the map is portable;
concrete masks are resolved at boot from `hwloc`/`/sys` (see fallback below).

- **T-class (AMD Zen3/Zen4 Threadripper / EPYC):** socket → CCD (chiplet) →
  CCX/core-complex sharing one **L3 (LLC)** → cores → 2 SMT siblings. NPS1/NPS2/NPS4
  changes NUMA-node count per socket. The LLC domain is the CCX; **first-touch
  ownership partitioning keys on the CCX/L3 domain, not just the NUMA node.**
- **X-class (Intel Xeon SP mesh):** socket → (optional Sub-NUMA Cluster, SNC) →
  shared-LLC mesh slice → cores → 2 SMT siblings. The LLC is socket-wide (sliced);
  SNC splits it into 2–4 NUMA-like domains. First-touch partitioning keys on the
  **SNC domain** when SNC is on, else the socket.

`llc_domain` below means CCX on T-class and (SNC-slice | socket) on X-class.

## The placement map

Lane classes (E6.1 taxonomy) ← budget-doc lanes:

| Lane class | Budget-doc lane(s) | Latency class |
| --- | --- | --- |
| **PUB** publication / commit-order authority | part of Stage 5 publish + Lane 1 Wakeup-Dispatch | latency-critical |
| **WRK** throughput writer / append / execute | Lane 0 Writer | throughput |
| **HLP** helper / spillover (evidence, invalidation, GC) | Lane 2, Lane 5, Lane 3 | background |
| **REC** recovery / checkpoint (WAL, checkpoint) | Lane 4 Checkpoint | best-effort durable |

| Dimension | **PUB** | **WRK** | **HLP** | **REC** |
| --- | --- | --- | --- | --- |
| CPU set / affinity | 1 dedicated **full core** on the **home node** (the node owning the global `next_commit_seq` + `VersionStore` publish authority); pinned, exclusive mask | 1 lane per physical core, spread across **all** NUMA nodes / CCDs; affinity = that core | small pool (1–2) per **socket**, placed on **SMT siblings** of WRK cores or on spillover cores | 1 (–2) lane near the **durable/WAL surface**, spillover-eligible |
| NUMA node | **home node** (fixed) | own node (spread) | own socket | home node (WAL/checkpoint state) |
| LLC / CCD (`llc_domain`) | its own LLC domain; publish structures L3-resident on home node | own `llc_domain`; lane-local caches stay L3-hot | shares the WRK lane's `llc_domain` (SMT sibling) so invalidation/evidence reads stay warm | own; checkpoint I/O is not LLC-sensitive |
| SMT-sibling policy | **no SMT sharing** — dedicate the full core; publish latency must not contend with a sibling | may use **both SMT siblings** for two WRK lanes (throughput) OR reserve the sibling for an HLP lane | **runs on WRK's SMT sibling** (Evidence/Invalidation are low-IPC, tolerate sibling contention); GC may take a spillover core | spillover; SMT-neutral |
| Home-node ownership (data + allocator arena) | global publish authority + durable-order counter **home** here; arena = home node | **lane-local** working set (parse/compiled/metadata caches, txn handle, read/write sets, staged pages) + first-touch **NUMA-local** ownership-directory partition for this node; arena = own node | evidence/invalidation buffers = own socket; GC operates on the **global** `VersionStore` (home node) but stages locally | WAL buffers + checkpoint scratch = home node |
| Cross-node handoff boundary | receives prepared surfaces from every WRK lane at **Boundary C** (prepared→publish): only the durable-order allocation + committed-surface publish crosses to PUB; everything else stays lane-local | **Boundary B** (local mutation→first-touch): already-owned page mutation stays lane-local; first-touch consults the **NUMA-local** ownership partition; cross-node only on a first-touch miss to another node's partition | consumes committed tail work (Boundary D) asynchronously; may cross nodes freely (off critical path) | reads committed VersionStore + WAL (home node); crosses only for checkpoint of remote-node pages |
| Remote-HITM budget | **≈0 on the hot path** — publish writes the tiny global order point once per commit; target ≤1 remote-HITM per commit (the `next_commit_seq` fetch-add) | ≤1 remote-HITM per **first-touch** acquisition (own-partition hits are local); own-page re-mutation = 0 remote-HITM | unbudgeted (background); must not steal LLC/bandwidth from PUB/WRK beyond the interference-contract thresholds | unbudgeted; checkpoint bandwidth capped by safe-mode trigger |
| Wake-to-run budget | ≤ **10μs** (Lane 1: wakeup must reach the waiting writer within 10μs of lock release) | n/a (running lane); its **wait**→wake is served by PUB's ≤10μs | Evidence ≤1ms, Invalidation ≤ **100μs**, GC ≤100ms | ≤ **1s** |
| Fallback (incomplete topology) | if home node undiscoverable: place PUB on **CPU 0's** node; degrade to a single global publish lane (correctness unchanged, locality lost) — log `fallback=home_node_unknown` | if NUMA/CCD unknown: **one flat pool**, affinity by round-robin over online CPUs; first-touch directory becomes single-partition (globally hot but correct) — log `fallback=flat_topology` | if SMT siblings unknown: place HLP on any spare online CPU | if node unknown: co-locate with PUB |

## Ownership-boundary alignment (from Boundaries A–D)

- **A (Stage 1→2, snapshot bind):** entirely within a WRK lane; no cross-node
  traffic. Snapshot binds to pager publication before `begin_concurrent` — unchanged.
- **B (Stage 3→4, first-touch):** the one hot cross-domain boundary. First-touch
  ownership directory is **partitioned per NUMA node / `llc_domain`**; a WRK lane
  consults its own partition (local) and only crosses on a page homed elsewhere.
- **C (Stage 4→5, prepared→publish):** the Track-E boundary. WRK lanes fully
  prepare page images + intent logs + conflict evidence **lane-locally**; only the
  durable-order allocation and committed-surface publish cross to **PUB**. Keep
  this crossing to the single global order point.
- **D (Stage 5→6, reclamation):** committed→GC/invalidation is **HLP** background
  work under guard/epoch discipline; never on the WRK critical path.

## Invariant guard (project-critical)

Concurrent-by-default is untouched by this map: it places lanes, it does **not**
serialize writers. `BEGIN`→concurrent promotion and page-level MVCC remain the
model (`INV-DB300-E1.1-1`). No lane class introduces a file/connection-level write
lock. See AGENTS.md "Concurrent-Writer Mode is the ENTIRE POINT."

## Verification plan

1. **Table-driven placement synthesis validation.** A synthesis function takes a
   topology snapshot (sockets, NUMA nodes, `llc_domain`s, SMT-sibling pairs) and
   emits the concrete affinity mask + home-node + fallback flags per lane class.
   Golden tests assert the map above for representative snapshots: T-class NPS1,
   T-class NPS4, X-class SNC-off, X-class SNC-on, and a degenerate single-node
   snapshot (exercises every fallback row). Snapshots live as fixtures so the
   synthesis is reproducible without the target hardware.
2. **Operator entrypoint** `scripts/verify_e6_1_lane_placement.sh` (skeleton
   staged with this doc): resolves the live topology (hwloc/`/sys`), runs the
   synthesis, prints the resolved placement table, and emits a structured artifact
   manifest matching `db300_topology_interference_contract.toml`
   (`hardware_class_id`, per-lane CPU set / NUMA / llc_domain / SMT policy /
   fallback flags), so a later interference case can bind probe lanes exactly as
   the map prescribes and measure remote-HITM / wake-to-run against the budgets here.
3. **Budget cross-check.** The manifest carries the wake-to-run and remote-HITM
   budgets per lane class so the interference beads fail closed if a measured lane
   exceeds its budget row.

## Remaining work (not in this v1)

- Implement the synthesis function + golden fixtures (this doc is the spec).
- Flesh `verify_e6_1_lane_placement.sh` from skeleton to full topology resolution
  + manifest emission.
- `.2` consumes the home-node/arena columns; `.3` consumes the SMT + remote-HITM
  columns — both should treat this table as the contract.
