# Per-Core WAL Buffer Architecture (`bd-ncivz.1`)

## Purpose

Define the design contract for per-core WAL buffering used by the parallel WAL path.  
This artifact provides:

- data model and state machine
- explicit invariants and failure modes
- deterministic fallback behavior
- MVCC integration points
- prototype evidence links (unit + e2e)

## Scope

This bead is a design-and-prototype slice. It does **not** replace the existing commit path end-to-end yet.  
It establishes a concrete contract that `bd-ncivz.2+` can implement against.

## Core Model

Each writer maps to a **core-local WAL buffer**.  
A core-local buffer is double-buffered:

- `active` lane: writable by the mapped writer
- `flush` lane: immutable while a flush is in progress
- `overflow` queue: bounded spillover when `active` fills before epoch advance

Default sizing:

- `active` capacity: `4 MiB` per core
- fallback trigger: `8 MiB` queued overflow per core

## WAL Record Schema

Each buffered mutation record carries:

- `txn_token` (`txn_id`, `txn_epoch`)
- `epoch` (group-commit epoch tag)
- `page_id`
- `begin_seq`
- `end_seq` (optional until commit sealing)
- `before_image`
- `after_image`

The prototype uses this shape directly in `crates/fsqlite-wal/src/per_core_buffer.rs`.

## Thread-to-Buffer Mapping

Deterministic mapping:

- writer thread is assigned one logical core id for the transaction lifetime
- all record appends for that writer go to that core’s `active` lane
- no cross-core append sharing on the fast path

Prototype evidence checks this with one writer thread per core and lock-contention counter assertions.

## NUMA Strategy

Allocation policy:

- allocate each core buffer on startup
- place memory on the local NUMA node of the mapped core whenever topology info is available
- if NUMA placement fails/unavailable, fall back to standard allocation and keep semantics unchanged

`bd-ncivz.1` captures this as a contract; low-level NUMA placement hooks are planned for implementation beads.

## State Machine

Lane states:

- `Writable`
- `Sealed { epoch }`
- `Flushing { epoch }`

Transitions:

1. `Writable -> Sealed` on epoch seal (`seal_active`)
2. `Sealed -> Flushing` by lane rotation (`begin_flush`)
3. `Flushing -> Writable` after successful drain (`complete_flush`)
4. `Writable -> Writable` normal append
5. `Writable -> overflow queue` when lane full and overflow policy permits

Invalid transitions are rejected deterministically (error return, no partial mutation).

## Overflow and Deterministic Fallback

Policies:

- `BlockWriter`: return blocked outcome on active-lane overflow
- `AllocateOverflow`: queue record in overflow queue

Deterministic fallback trigger:

- if overflow bytes exceed `overflow_fallback_bytes`, latch `ForceSerializedDrain`
- fallback action drains `active + flush + overflow` through serialized path and resets lanes

This avoids unbounded queue growth and ensures a predictable failure containment path.

## MVCC and Commit Integration Points

Write path integration points:

1. MVCC page write emits `WalRecord` into mapped core buffer
2. transaction commit marks records sealed under current epoch
3. group flush drains sealed lanes and hands batches to WAL append/fsync path
4. on fallback trigger, coordinator drains through serialized compatibility-safe path

This design preserves the project requirement that concurrent-writer mode remains enabled by default.

## Invariants

`INV-NCIVZ-1` Single-writer per core lane:
- one mapped writer owns one `active` lane at a time

`INV-NCIVZ-2` Lane exclusivity:
- a lane cannot be `Writable` and `Flushing` simultaneously

`INV-NCIVZ-3` Epoch monotonicity:
- `epoch` tags on sealed/flush batches are non-decreasing per core

`INV-NCIVZ-4` Flush immutability:
- records in `Flushing` lane are immutable until flush completion

`INV-NCIVZ-5` Overflow boundedness:
- overflow growth beyond threshold always latches deterministic fallback

`INV-NCIVZ-6` No silent drop:
- appends are either accepted in active lane, queued overflow, or explicitly blocked

`INV-NCIVZ-7` Deterministic fallback reset:
- serialized drain leaves both lanes writable/empty and clears fallback latch

`INV-NCIVZ-8` No-contention target:
- one writer per core has zero observed lock-contention events in prototype run

## Failure Modes and Handling

1. Active lane overflow before epoch seal:
- handled by configured policy (`BlockWriter` or `AllocateOverflow`)

2. Overflow runaway:
- deterministic `ForceSerializedDrain` trigger and drain

3. Invalid state transitions:
- rejected with explicit error, no state corruption

4. Flush failure:
- keep lane in flushing state and surface error; no writer success is reported from failed flush

5. Mapping mismatch (invalid core id):
- hard error at dispatch boundary; no implicit remap

## Observability Contract (Required Fields)

Per flush/fallback event must include:

- `trace_id`
- `run_id`
- `scenario_id`
- `bead_id`
- `core_id`
- `epoch`
- `lane_state_from`
- `lane_state_to`
- `outcome`
- `duration_us`
- `error_code` (nullable)

The e2e verifier emits these in JSONL summary artifacts.

## Prototype Evidence

Unit tests:

- `bd_ncivz_1_state_machine_double_buffering`
- `bd_ncivz_1_overflow_block_writer_policy`
- `bd_ncivz_1_overflow_allocate_triggers_deterministic_fallback`
- `bd_ncivz_1_per_core_pool_concurrent_writers_no_contention`

All are implemented in:

- `crates/fsqlite-wal/src/per_core_buffer.rs`

Pilot e2e script:

- `e2e/bd_ncivz_1_parallel_wal_buffer_pilot.sh`

Artifacts:

- `artifacts/ncivz_1_parallel_wal_buffer/`

Replay command:

- `RUST_TEST_THREADS=1 rch exec -- cargo test -p fsqlite-wal bd_ncivz_1_ -- --nocapture`
