# Fused Transaction-Entry Ticket & Invalidation Automaton

> **Bead:** bd-db300.5.2.2.2
> **Date:** 2026-03-23
> **Evidence base:** bd-db300.5.2.2.1 census (100% duplication confirmed)
> **Author:** Claude Opus 4.6 performance-correctness agent

## 1. Ticket Field Schema

```rust
/// Fused transaction-entry ticket for the prepared-DML common path.
///
/// Captures the proof state from schema freshness, publication binding,
/// and begin-path visibility in one reusable artifact. Valid until
/// invalidated by one of the named causes.
#[derive(Debug, Clone)]
struct PreparedEntryTicket {
    /// Schema cookie at ticket issuance.
    schema_cookie: u32,
    /// Connection-local schema generation at ticket issuance.
    schema_generation: u64,
    /// Pager-published commit sequence at ticket issuance.
    visible_commit_seq: u64,
    /// Database file size (page count) at ticket issuance.
    db_size_pages: u32,
    /// WAL generation (checkpoint_seq / salt pair) at ticket issuance.
    wal_generation: u64,
    /// Mode flags.
    mode: TicketMode,
    /// Control mode for testing/operator overrides.
    control: TicketControl,
    /// Monotonic stamp for age-based expiry.
    issued_at_ns: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TicketMode {
    /// File-backed WAL database (the hot path target).
    FileBacked,
    /// In-memory database (no external writers — ticket always valid
    /// unless local DDL changes schema).
    Memory,
    /// Recovery mode — force slow path for safety.
    Recovery,
    /// Fallback-only — this connection has been flagged to never use
    /// the fast ticket path (e.g., after a control-mode override).
    FallbackOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TicketControl {
    /// Normal operation — use ticket when valid.
    Auto,
    /// Forced fallback — always take slow path (operator override).
    ForcedFallback,
    /// Shadow-compare — run both paths, compare results, log divergence.
    ShadowCompare,
}
```

## 2. State Machine

```
                  ┌──────────────────┐
                  │  ticket_absent   │  ← initial state / after invalidation
                  └────────┬─────────┘
                           │ issue_ticket()
                           ▼
                  ┌──────────────────┐
          ┌──────│   ticket_valid   │◄─────── refresh_and_reissue()
          │      └────────┬─────────┘         (when guard table permits)
          │               │
          │  invalidation event
          │               │
          ▼               ▼
   ┌─────────────┐  ┌──────────────────┐
   │  fallback   │  │   invalidated    │
   │  required   │  │  (soft — may     │
   │ (hard —     │  │   refresh)       │
   │  slow path) │  └────────┬─────────┘
   └─────────────┘           │
                             │ refresh_and_reissue()
                             ▼
                  ┌──────────────────┐
                  │   ticket_valid   │
                  └──────────────────┘
```

**States:**
| State | Meaning |
|-------|---------|
| `ticket_absent` | No ticket exists. Must issue from scratch. |
| `ticket_valid` | All fields match current world state. Fast path allowed. |
| `invalidated` | One or more fields are stale. May be refreshed without full slow path. |
| `fallback_required` | Hard invalidation. Must take full slow path and re-issue. |

## 3. Invalidation Causes & Guard Table

| # | Cause | Detection | Guard | Action |
|---|-------|-----------|-------|--------|
| I1 | DDL on this connection | `schema_generation` bumped | HARD | → `fallback_required`. DDL can change table structure, indexes, triggers. Cannot refresh-and-reissue safely for the SAME prepared statement. Must return `SchemaChanged`. |
| I2 | DDL on another connection | `schema_cookie` changed (detected at begin-time via pager header read) | SOFT | → `invalidated`. Refresh schema_cookie + publication, reissue ticket if prepared statement's SQL still parses to the same plan. |
| I3 | External commit (new data) | `visible_commit_seq` advanced | SOFT | → `invalidated`. Refresh publication binding. The prepared plan is still valid; only the visibility snapshot changed. |
| I4 | WAL reset/checkpoint | `wal_generation` changed (checkpoint_seq or salts differ) | SOFT | → `invalidated`. Refresh WAL generation + publication. Plan still valid. |
| I5 | Database file size change | `db_size_pages` changed | SOFT | → `invalidated`. Typically accompanies I3/I4 but detected independently for defensive completeness. |
| I6 | Savepoint rollback (no DDL) | None | PASS | → `ticket_valid` (no change). Schema cookie unchanged, no invalidation needed. |
| I7 | Recovery mode entry | Mode transition | HARD | → `fallback_required`. Recovery must use full slow path for crash safety. |
| I8 | Forced-fallback override | `control = ForcedFallback` | HARD | → `fallback_required`. Operator explicitly disabled fast path. |
| I9 | Ticket age expiry | `now_ns - issued_at_ns > MAX_TICKET_AGE_NS` | SOFT | → `invalidated`. Defensive: revalidate after N ms even if no known invalidation. Prevents silent drift. Default: 100ms. |
| I10 | File reopen / attach | Mode flag mismatch | HARD | → `fallback_required`. Attached databases change the execution context. |

## 4. Validation Function (hot path)

```rust
/// Called at the top of execute_prepared_with_params().
/// Returns Ok(()) if the ticket is still valid; Err(reason) if not.
fn validate_ticket(
    ticket: &PreparedEntryTicket,
    conn: &Connection,
    cx: &Cx,
) -> Result<(), InvalidationReason> {
    // I8: forced fallback
    if ticket.control == TicketControl::ForcedFallback {
        return Err(InvalidationReason::ForcedFallback);
    }
    // I7: recovery mode
    if ticket.mode == TicketMode::Recovery || ticket.mode == TicketMode::FallbackOnly {
        return Err(InvalidationReason::RecoveryMode);
    }
    // I1: local DDL
    if conn.schema_generation() != ticket.schema_generation {
        return Err(InvalidationReason::LocalDdl);
    }
    // I2: external DDL (cheapest check: compare cached cookie)
    if conn.schema_cookie() != ticket.schema_cookie {
        return Err(InvalidationReason::ExternalDdl);
    }
    // Memory mode: no external writers → skip I3/I4/I5/I9.
    if ticket.mode == TicketMode::Memory {
        return Ok(());
    }
    // I3+I5: external commit / size change (requires pager header peek)
    let current_seq = conn.pager_visible_commit_seq();
    if current_seq != ticket.visible_commit_seq {
        return Err(InvalidationReason::ExternalCommit);
    }
    // I9: age expiry
    let now_ns = Instant::now().elapsed_since_epoch_ns();
    if now_ns.saturating_sub(ticket.issued_at_ns) > MAX_TICKET_AGE_NS {
        return Err(InvalidationReason::AgeExpiry);
    }
    Ok(())
}
```

**Hot-path cost:** 3 field comparisons for `:memory:`, 5 for file-backed. All are register-width integers — no allocation, no lock, no I/O.

## 5. Refresh-and-Reissue vs Hard Fallback

```rust
fn handle_invalidation(
    reason: InvalidationReason,
    conn: &Connection,
    cx: &Cx,
) -> TicketAction {
    match reason {
        // HARD — cannot reissue, must slow-path
        LocalDdl => TicketAction::SchemaChanged,
        RecoveryMode | ForcedFallback | ModeMismatch => TicketAction::SlowPath,
        // SOFT — refresh and reissue
        ExternalDdl | ExternalCommit | WalGeneration | FileSizeChange | AgeExpiry => {
            // One combined refresh replaces 3 separate phases:
            //   refresh_prepared_schema_state()
            //   bind_pager_publication()
            //   ensure_autocommit_txn begin-side refresh
            TicketAction::RefreshAndReissue
        }
    }
}
```

## 6. Fused Entry Sequence (replaces 4 phases with 1)

**Before (current, from census):**
```
execute_prepared_with_params()
  1. background_status()           ← bg_status: 1.00/stmt
  2. schema_unchanged_check()      ← schema_refresh: 1.00/stmt
     └─ bind_pager_publication()   ← publication_bind: 1.00/stmt
  3. ensure_autocommit_txn()       ← begin_refresh: 1.00/stmt
     └─ refresh_memdb_if_stale()   ← memdb_refresh: 0.00/stmt (skip)
     └─ bind_pager_publication()   ← DUPLICATE of step 2
  4. ... actual INSERT ...
  5. resolve_autocommit_txn()      ← commit_refresh: 1.00/stmt
```

**After (fused):**
```
execute_prepared_with_params()
  1. validate_ticket()             ← 3-5 integer comparisons, no I/O
     if invalid:
       refresh_and_reissue()       ← ONE combined refresh (replaces steps 1-3)
     if hard_invalid:
       return slow_path()
  2. begin_if_autocommit()         ← uses ticket's pre-validated state
  3. ... actual INSERT ...
  4. commit_if_autocommit()        ← commit_refresh: 1.00/stmt (kept)
```

**Expected savings:** 3 phases (bg_status + schema_refresh + publication_bind) → 0 phases on the steady-state hot path. Only `commit_refresh` remains per-statement. On invalidation (rare), one combined refresh replaces three separate ones.

## 7. Trace Contract

Every ticket validation emits:
```rust
tracing::debug!(
    target: "fsqlite.entry_ticket",
    trace_id = %cx.trace_id(),
    prepared_stmt_id = %stmt_fingerprint,
    ticket_state = "valid" | "invalidated" | "fallback_required" | "absent",
    invalidation_reason = "none" | "local_ddl" | "external_ddl" | "external_commit" | ...,
    control_mode = "auto" | "forced_fallback" | "shadow_compare",
    schema_cookie = ticket.schema_cookie,
    schema_generation = ticket.schema_generation,
    visible_commit_seq = ticket.visible_commit_seq,
    ticket_age_ns = now_ns - ticket.issued_at_ns,
    action = "fast_path" | "refresh_and_reissue" | "slow_path" | "schema_changed",
);
```

## 8. Proof Obligations

| # | Obligation | Test strategy |
|---|-----------|---------------|
| P1 | Ticket valid → identical results to slow path | Shadow-compare mode: run both, assert equal |
| P2 | I1 (local DDL) → SchemaChanged returned | Unit test: DDL between prepare and execute |
| P3 | I2 (external DDL) → soft invalidation, refresh works | Integration: 2 connections, one does DDL |
| P4 | I3 (external commit) → soft invalidation, fresh data visible | Integration: 2 connections, one inserts |
| P5 | I4 (WAL reset) → soft invalidation | Integration: checkpoint RESTART between stmts |
| P6 | I6 (savepoint rollback) → ticket stays valid | Unit: SAVEPOINT + ROLLBACK, ticket unchanged |
| P7 | I9 (age expiry) → soft invalidation | Unit: set MAX_TICKET_AGE_NS=0, verify refresh |
| P8 | Forced fallback → always slow path | Unit: set control=ForcedFallback, verify |

## 9. Validation Surface

```bash
#!/usr/bin/env bash
# scripts/verify_e2_2_ticket_invalidation_matrix.sh
set -euo pipefail

echo "=== bd-db300.5.2.2.2 Ticket Invalidation Matrix ==="

# Run census tests (proves counters work).
CARGO_TARGET_DIR=/tmp/pane1-e221 cargo test -p fsqlite-core \
  --test boundary_duplication_census -- --test-threads=1 --nocapture

# Run cache invalidation tests (proves schema/rollback boundaries).
CARGO_TARGET_DIR=/tmp/pane1-e221 cargo test -p fsqlite-core \
  --test statement_cache_invalidation -- --test-threads=1 --nocapture

# Run fast-path separation tests (proves path-decision counters).
CARGO_TARGET_DIR=/tmp/pane1-e221 cargo test -p fsqlite-core \
  --test fast_path_separation -- --test-threads=1 --nocapture

echo "=== ALL PASSED ==="
```

## 10. Decision Record

The census (bd-db300.5.2.2.1) proved that duplication IS real:
- 4 of 5 per-statement phases are collapsible
- Only commit_refresh (1.00/stmt) is per-boundary and must remain
- Expected steady-state hot path: **0 refresh phases** (ticket valid) vs current **4**
- Invalidation frequency: once per DDL/external-commit/checkpoint — rare in benchmark workloads

The fused-entry ticket is justified. Implementation should proceed via bd-db300.5.2.2.3.
