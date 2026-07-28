# bd-8sfs3 fix receipt: parameterized correlated-EXISTS routed to the compiled semijoin

Paired with tests/artifacts/perf/subq-oracle-receipt-20260727T2345Z-hz1/ (pre-fix,
same host hz1, same debug dev profile, same comprehensive-bench --quick --filter
subquery). DIAGNOSTIC-ONLY provenance both sides; the paired delta and the
scaling-shape change are the evidence, not the absolute numbers.

Parameter-varying EXISTS row (F side):
  rows=100:    14.76ms -> 366.9us   (C=17.3us)
  rows=1000:   155.68ms -> 461.1us  (C=21.1us)
  rows=10000:  ~427ms/exec -> 190.1us (C=20.8us)  — ~2,250x improvement

Pre-fix F time scaled ~linearly in products x categories (nested full
execute_statement per outer row); post-fix it is flat/small — the compiled
CountIndexEqRun semijoin with an Opcode::Variable residual bound.

Remaining gaps visible in the same run (NOT this bead):
  - IN subquery (parameter-varying) rows=10000: C=7.4us F=4.40ms — bd-2dgf5.
  - Recursive CTE general COUNT: C=387.4us F=12.97ms — general frontier path.
