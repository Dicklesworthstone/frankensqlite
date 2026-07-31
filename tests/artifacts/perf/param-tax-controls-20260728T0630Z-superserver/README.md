# bd-5zeai control battery results (controls 1-3 + A/A)

param-tax-controls (self-SHA-reporting, release-perf, :memory:, 24 clusters
x 8 ABBA samples/arm; host superserver, DIAGNOSTIC-ONLY). Per the agreed
design (agent-mail 4469): isolate the Option gate, the params plumbing, and
fast-path presence on the same query shape.

  control1 (SELECT 40+2, query_row vs with_params(&[])):   delta 0.49us
  control2 (literal probe, same two APIs):                 delta 0.58us
  aa_null  (query_row vs query_row):                       delta 0.00us
  control3 (literal 'c.id <= K' vs placeholder '?1'=K):
      literal_bound_fast_path        median 0.64us
      placeholder_bound_no_fast_path median 12.90us  (20.1x)

VERDICT: fast-path PRESENCE dominates by ~20x while the Option gate and
parameter plumbing are sub-microsecond. The placeholder statement gets
prepared_query_fast_path=None (select_rowid_upper_bound_exclusive accepts
only integer literals, connection.rs:7797) and pays the generic
pager-txn+VDBE+storage-cursor lane. Magnitude here (12.9us) vs the
comprehensive bench (58-95us) differs with environment; the mechanism is
the same and singular. Implementation unblock: teach the probe fast path a
parameterized bound (PreparedProbeRowidBound {LiteralExclusive, Parameter})
resolved at execution — pending RusticBasin ack per the bd-5zeai agreement.
