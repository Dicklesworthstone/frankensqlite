# fsqlite-observability

Conflict analytics, tracing, and observability infrastructure for the FrankenSQLite MVCC layer.

## Overview

`fsqlite-observability` provides shared types and utilities for conflict tracing, metrics aggregation, and diagnostic logging across FrankenSQLite's MVCC concurrency control system. It is designed around three principles:

1. **Zero-cost when unused** -- All observation is opt-in via the `ConflictObserver` trait. When no observer is registered, conflict emission compiles to a no-op (the default `NoOpObserver` is inlined away).
2. **Non-blocking** -- Observers must not acquire page locks or block writers. All conflict tracing is purely diagnostic.
3. **Shared foundation** -- Types defined here are reused by downstream observability components throughout the workspace.

This crate depends on `fsqlite-types`, `parking_lot`, `serde`, and `tracing`.

```
fsqlite-types --> fsqlite-observability
                      ^
                      |-- fsqlite-core (MVCC engine)
                      |-- fsqlite-shm
```

## Key Types and Globals

### Structured Trace Metrics
- `TraceMetricsSnapshot` - Point-in-time snapshot of trace counters (spans created, export errors, compat callbacks).
- `trace_metrics_snapshot()` - Read current trace counter values.
- `record_trace_span_created()`, `record_trace_export()`, `record_trace_export_error()` - Increment trace counters.
- `next_trace_id()`, `next_decision_id()` - Allocate monotonic identifiers for traces and decisions.

### Cx Propagation Telemetry
- `CxPropagationMetrics` - Atomic counters tracking how the capability context (`Cx`) is threaded through connection and transaction paths. Tracks successes, failures, cancellation cleanups, trace linkages, and Cx creation.
- `GLOBAL_CX_PROPAGATION_METRICS` - Global singleton instance.
- `CxPropagationMetricsSnapshot` - Serializable snapshot with a `failure_ratio()` helper.

### TxnSlot Crash/Occupancy Telemetry
- `TxnSlotMetrics` - Tracks active transaction slot occupancy and crash/orphan detections. Methods: `record_slot_allocated()`, `record_slot_released()`, `record_crash_detected()`.
- `GLOBAL_TXN_SLOT_METRICS` - Global singleton instance.

### Conflict Observer
- `ConflictObserver` trait - Opt-in observer for MVCC conflict events.
- `NoOpObserver` - Default no-op implementation that compiles away.
- `ConflictEvent` - Structured conflict event data (page, transactions, conflict type).

## Usage

```rust
use fsqlite_observability::{
    trace_metrics_snapshot, record_trace_span_created,
    GLOBAL_CX_PROPAGATION_METRICS, GLOBAL_TXN_SLOT_METRICS,
};

// Record trace activity
record_trace_span_created();
let snapshot = trace_metrics_snapshot();
println!("Total spans: {}", snapshot.fsqlite_trace_spans_total);

// Record Cx propagation success
GLOBAL_CX_PROPAGATION_METRICS.record_propagation_success();
let cx_snap = GLOBAL_CX_PROPAGATION_METRICS.snapshot();
println!("Failure ratio: {:.4}", cx_snap.failure_ratio());

// Record transaction slot lifecycle
GLOBAL_TXN_SLOT_METRICS.record_slot_allocated(0, std::process::id());
GLOBAL_TXN_SLOT_METRICS.record_slot_released(Some(0), std::process::id());
```

## License

MIT
