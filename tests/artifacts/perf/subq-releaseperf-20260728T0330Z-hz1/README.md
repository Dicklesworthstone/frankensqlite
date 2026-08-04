# Release-perf subquery section: post-fix truth at full optimization

Fourth run in the paired hz1 series; FIRST at release-perf (opt-level=3) —
prior runs raced a debug FrankenSQLite against rusqlite's always--O2 C SQLite.
Still diagnostic-only host provenance; within-run row-vs-row comparisons are
the evidence.

Param-varying rows (F flat across scale — the fa85adfc/5d35b79e fixes hold):
  EXISTS: F=60.7/65.8/94.9us at 100/1k/10k (C=4.7/24.0/12.7us)
  IN:     F=58.3/71.9/72.4us               (C=4.2/6.6/8.4us)
Parameterless rows in the SAME section (F wins big):
  scalar subquery: F=11.0/5.8/8.0us  (C=76.0/35.5/70.4us)
  CTE+JOIN:        F=10.1/15.7/124us (C=90.7/215/2110us)
Recursive CTE general COUNT: F=2.46ms vs C=335.4us (~7.3x) — bd-gpi5i
confirmed at release profile.

KEY FINDING (bd for it filed): the residual on EXISTS/IN is NOT
subquery-specific. Within the F column, parameterless prepared execution
(stmt.query()) costs 6-16us while parameterized prepared execution
(query_row_with_params) costs 58-95us — a ~50-60us FLAT per-execution tax on
the parameterized path, invariant to table size and query shape.
