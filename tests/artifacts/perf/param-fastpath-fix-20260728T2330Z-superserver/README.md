# bd-5zeai fix receipt: parameterized count-probe fast path (superserver, 2026-07-28)

## Change
connection.rs: PreparedProbeRowidBound {None, LiteralExclusive, Parameter}
replaces the literal-only Option<i64> in SimpleCountIndexedRowidProbe.
Numbered-placeholder rowid bounds resolve at execute time (strict Integer;
Float/Text/Null bindings fall back to the general path, preserving affinity
semantics by never entering the shortcut). Gate lifts: query_row_internal
runs the probe fast path in BOTH params branches; the last-result cache keys
on the RESOLVED bound (correct per-value caching across rebinds); the
query-collection dispatch site resolves identically (rustc enumerated all
sites via the deliberate field type change — exactly one beyond the spec'd
list).

## Receipt (param-tax-controls, release-perf, taskset, this host)
- BEFORE (param-tax-controls-20260728T0630Z-superserver):
  control3 literal 0.64us vs placeholder 12.90us  -> 20.1x
- AFTER  (param-controls-after.log, this dir):
  control3 literal 0.62us vs placeholder 1.01us   -> 1.63x
  control2 (params plumbing on a fast-pathed stmt) = 0.40us — the placeholder
  residual (0.39us) IS the plumbing cost; the fast-path-absence tax is gone.
  A/A null control: 0.00us. 12.8x improvement on the parameterized shape.

## Gates
check/clippy fsqlite-core --lib clean; fmt; new inline test
test_prepared_count_indexed_rowid_probe_parameterized_bound (fast-path hit
counter on params, 4-way rebind incl. repeat, Float/Null fallback parity,
post-write invalidation) passes; full lib suite 3432 passed / 24 failed with
ALL 24 dispositioned: 23 reproduce on clean HEAD (pre-existing, RusticBasin's
active correctness lane), 1 (test_prepared_count_star_rowid_range_...) is a
pre-existing exact-equality-counter parallel-suite flake — passes alone and
serialized with the new test on the fixed tree.
