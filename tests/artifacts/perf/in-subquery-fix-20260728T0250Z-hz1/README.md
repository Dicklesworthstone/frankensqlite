# bd-2dgf5 fix receipt: parameterized IN-subquery routed to the compiled probe

Third run in the paired series (pre-fix baseline: subq-oracle-receipt-20260727T2345Z-hz1;
EXISTS fix: exists-semijoin-fix-20260728T0230Z-hz1). Same host hz1, same debug
profile, comprehensive-bench --quick --filter subquery. DIAGNOSTIC-ONLY
provenance; the scaling-shape change is the evidence.

Parameter-varying IN-subquery row (F side):
  rows=100:    252.1us -> 171.4us  (C=10.0us)
  rows=1000:   423.4us -> 351.8us  (C=16.5us)
  rows=10000:  4.40ms  -> 137.3us  (C=31.5us)  — linear-in-N blowup eliminated

Also verified in the same session (CLI EXPLAIN, agg-seek-check.sql): the bead's
ORIGINAL findings are fixed on current main — COUNT(*)/SUM WHERE k=2 emit
SeekGE+AggStep (scan-fallback safety valve retained) and COUNT(*) WHERE id=2
emits SeekRowid. The aggregate index-seek machinery at codegen.rs:1851-1884
landed after the bead's notes and resolved the literal-aggregate scope.
