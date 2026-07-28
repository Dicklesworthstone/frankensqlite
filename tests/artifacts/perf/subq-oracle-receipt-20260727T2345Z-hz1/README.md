# Subquery-section oracle receipt (bd-czzlp close) + correlated-EXISTS pathology receipt (bd-8sfs3 filing)

- Command: `cargo run -p fsqlite-e2e --bin comprehensive-bench -- --quick --filter subquery`
- Host: rch worker hz1, debug (dev) profile, 2026-07-27T23:45Z. DIAGNOSTIC-ONLY
  provenance (no pinned CPUs, unpaired adaptive C-first blocks, no citable
  profile) — do not cite magnitudes as release numbers.
- Purpose 1 (functional): the run completed exit 0 with the bd-czzlp
  cross-engine result oracles active (assert_result_set_oracle on the
  scalar-subquery and CTE+JOIN rows; EXISTS/IN oracle asserts pre-existing) —
  no cross-engine result mismatch on any subquery/CTE row at 100/1k/10k rows.
- Purpose 2 (finding): first honest measurement of the parameter-varying
  correlated-EXISTS shape shows F ~740x/9,500x/26,000x slower at
  100/1k/10k rows (~427 ms per execution at 10k). Filed as bd-8sfs3 (P0).
  The magnitude (5 orders) transcends the provenance caveats; the shape's
  scaling curve, not the absolute numbers, is the evidence.
