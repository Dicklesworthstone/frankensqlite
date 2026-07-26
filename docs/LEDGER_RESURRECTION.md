# Ledger Resurrection Audit — `docs/progress/perf-negative-results.md`

**Campaign:** FrankenSuite Performance Domination, 2026-07-25 — Meta-Lever #1.
**Lane:** frankensqlite `cc` / STRUCTURAL (owns this audit per campaign §5).
**Ledger audited:** `docs/progress/perf-negative-results.md` @ `ffda8070`, 20 499 lines, 583 `##` entries.
**Audit tooling:** three mechanical extraction passes, then hand-verification of every
entry that survived to a verdict. Scripts are scratch-only; every VOID verdict below was
read by a human-equivalent pass, not emitted by regex.
**Corrected remeasurement:** 2026-07-26, Lane M (`cod`), exact-ELF
same-invocation A/A plus A/B contracts.

---

## Headline: the hypothesis does not hold for this repo, and the reason is on the record

The campaign's §1 hypothesis is that a large fraction of ledger REJECTs are **VOID** — the
measurement could not have detected the lever — with an expected yield around 5 % or better
(frankenlibc measured 39/93 = 42 %).

**frankensqlite's true void rate is 10 / 583 = 1.7 %** (1.9 % before excluding one
non-lever entry). That is roughly a 25× lower void rate than frankenlibc's.

This is not because the audit was run leniently. It is because **this repo already ran this
exact audit, four months before the campaign asked for it, and then strengthened it.** Two
entries dated 2026-07-10 are ledger-integrity self-audits explicitly triggered by the same
frankenmermaid crossing-minimization finding the campaign cites:

- `## 2026-07-10 - AUDIT: the five single-opcode hot-arm pruning rejects are NOT invalidated`
  (line 15930) — *"Requested by the crossing-min ledger-integrity finding: verify each REJECT
  was measured on an input that actually reaches the code under test."*
- `## 2026-07-10 - AUDIT v2 (exact dispatch counts): hot-arm stream matrix DOES reach the
  pruned arms` (line 15955) — supersedes v1 with a **stronger** reachability proof: exact
  per-opcode VDBE dispatch counters instead of sampled self-time, *"strictly stronger for the
  reachability question: they count dispatches rather than estimating them from samples."*

The v1 audit also records the distinction the campaign is trying to teach, in the repo's own
words: *"The crossing-min pathology is a NULL result on an input that misses the code. None of
these five/six are null results."*

**Recommendation to the orchestrator:** frankensqlite is a *source* of the §1 method, not a
target for it. The transferable artifact from this repo is not a resurrection yield — it is
**AUDIT v2's dispatch-counter reachability proof**, which is cheaper and stronger than
sampled self-time and which the other ten repos should copy. See "Method worth porting" below.

---

## Six-class hand-adjudication contract

The fleet correction supersedes the original regex labels. Mechanical matching
only builds a reading queue; a human-equivalent pass assigns one of these
classes after reading the row and its cited evidence:

- `VALID-PROFILE`: a pre-edit rejection names a non-zero-self-time frame and
  computes its Amdahl ceiling.
- `VALID-MECHANISM`: even without A/A, a counted mechanism proves that no work
  was removed (instructions, cycles, syscalls, allocations, faults, or an
  equivalent exact dispatch count).
- `VALID-AB`: the A/B effect lies inside a recorded same-invocation A/A null.
- `VOID-CV`: the row was killed only by a CV threshold.
- `VOID-ZEROSELF`: the benchmark's target frame had approximately zero
  self-time; an exact zero dispatch count is a stronger instance of this class.
- `VOID-NONULL`: a near-parity wall ratio has neither an A/A null nor a counted
  mechanism.

Applied honestly to the hand-read queue, all ten original genuine VOID entries
were `VOID-NONULL`: none had an A/A null or counted mechanism. No genuine queue
row was void solely because of CV, and none was promoted to
`VALID-MECHANISM` without a count. Screens, summaries, correctness failures,
and architecture decisions remain excluded from the performance-verdict
denominator rather than being forced into a class. The resulting entry-level
audit remains **10 / 583 = 1.7% VOID**.

---

## What the passes found, and why the first two were wrong

| Pass | Criterion | Flagged | Verdict on the pass |
|---|---|---:|---|
| 1 | Literal §1 criteria (any of: no null control, no binary sha, ratio in null band, ~0 % self-time, cv gate) | 463 / 473 REJECT-ish | **Useless.** "No null control recorded" alone voids 98 % of the ledger and says nothing. |
| 2 | Pass 1 + require the reject to be *measurement*-decided (not correctness/architecture) + ratio-magnitude decidability | 100 | **Still wrong.** Hand-verification of the top-ranked hits found the strongest flags were false positives (below). |
| 3 | Decisive verdict is a **neutral** claim ("within noise" / "no improvement"), **no** null control, and the entry is a **primary lever measurement** (not a screen, audit, summary, or pre-edit kill) | **11** | **Holds up.** Every one hand-read; 10 are genuine, 1 is a non-lever entry. |

### The false positives are the interesting part

Pass 2's four top-ranked "void" entries were all sound, and each failed in an instructive way:

- **L15898 — `opcode_trace_enabled()` caching (`ZERO_SELFTIME`).** The 0.00 % self-time *was
  the entry's own finding*: the prepared DELETE takes the direct BtCursor bypass and never
  enters the VDBE, so the candidate was killed **before any source edit**. The mechanical flag
  fired on a correctly-executed reachability check. This entry is a model of the §1 method, not
  a victim of its absence. It also carries a load-bearing corollary: *"the `vdbe_pipeline_execute`
  stream matrix is the WRONG gate for a write_single DELETE lever"* — gating a DELETE lever there
  would measure a path the workload does not take.
- **L16244 — ephemeral packed same-leaf DELETE atlas (`RATIO_IN_NULL_BAND`).** The
  best-evidenced entry in the ledger. It records **two** SHA-256s (timing binary
  `4e0cc65d…`, symbolized profiling binary `736c1c88…`), the worker (`ovh-a`), six-way arm-order
  rotation, separate warmed fixtures, CV 0.47–4.45 %, and a ledger-integrity proof showing
  non-zero self-time for *both* arms. Its 1.095×/1.103×/1.106× ratios land inside my null band,
  but they are **consistent across three problem sizes with tight CV and rotated arm order** —
  that is a real ~10 % regression, not noise. It exceeds the campaign's §2 contract everywhere
  except the literal `paired(base, base)` call.
- **L4041 — INSERT/DELETE CPU screen.** Not a lever reject at all: *"Touched during this pass:
  no source files."*
- **L11127 — CASS/artifact follow-up.** A summary of older rejects, not a primary measurement.

**Lesson for the other ten repos running this audit: a mechanical void-screen has a high false-positive
rate and must not be reported as a yield.** Flags identify entries to *read*; they do not decide.
Reporting pass-1 or pass-2 numbers as a resurrection yield would have overstated this repo's void
rate by 46× and sent the fleet chasing entries that are already correctly closed.

## Third failure class discovered during the rerun: baseline drift

The literal VOID audit above asks whether an entry's own harness could detect its
candidate. The campaign uncovered a separate failure mode while those reruns
were building: a sound measurement can still become stale when its baseline
later regresses.

`bd-zavyn` reproduced the README's cited `write_bulk` result on its cited
`140e77df` source snapshot (`n=22`, geomean `0.8754x`, within 1.1% of the
published `0.866x`), then ran the same release-perf binary target and
`--quick --filter insert` shape at `ef1e39c9`. The latter measured `n=22`,
geomean `2.5898x`; that is a **2.96x degradation** across the committed
`140e77df..ef1e39c9` range. Tight small-row CVs and a clean historical
worktree ruled out host noise, the harness, and the current feature-gated
uncommitted rerun code.

This does not change the hand-verified 10/583 literal null-control VOID count.
It adds a third audit class: **baseline-drift provisional**. Any performance
decision whose evidence predates or falls inside a confirmed regression window
must be re-decided after the regression is fixed, even if its original
reachability and statistics were sound. The concrete retry predicate is:
`bd-zavyn` closed with the culprit fixed, a post-fix profile still attributes
the target surface, and the exact candidate clears the same-invocation A/A
median-CI gate on the repaired lineage. Until then, the five queue reruns below
are useful for harness calibration and relative direction only; their
write-side verdicts cannot close the post-fix frontier.

---

## The VOID queue (10 genuine entries)

Ranked by resurrection value = (decisive claim's distance from a real decision) × (specificity of
the re-run recipe). Self-time ranking per §1 was not usable as the primary key: 7 of 10 record no
self-time for the target frame, which is itself part of why they are void.

| # | Line | Entry | Decisive claim | Why VOID | Re-runnable? |
|---|---:|---|---|---|---|
| **1** | 18529 | 2026-07-12 Table seek-cache MRU refresh short-circuit | median **25.457 → 24.646 µs, −3.19 %**; Criterion `−9.68 %..+0.34 %`, `p=0.07` | Rejected "as within noise" with **no A/A null control**. The point estimate is a 3.2 % *improvement* and the CI is almost entirely negative — it barely crosses zero at the top. "Within noise" was an assumption about the harness, never a measurement of it. | **Yes — best in queue.** Records exact bench, worker (`vmi1264463`), and full command. |
| 2 | 13625 | 2026-05-09 Same-leaf DELETE run search-hint narrowing | nearest ratio 1.65× | Neutral verdict, no null control; DELETE-tail surface, which is the repo's named remaining gap. | Partly — bench named, worker not pinned. |
| 3 | 11422 | 2026-05-05 SmallText direct-byte Eq/Ord/Hash traits | nearest ratio 3.08× | Neutral verdict, no null control. | Partly. |
| 4 | 7626 | 2026-05-07 Direct UPDATE fixed-width REAL leaf-payload patch | within noise | No null control; `write_single` surface. | Partly. |
| 5 | 10126 | 2026-05-06 Direct UPDATE lazy decoded-row scratch borrow | "improved only within noise", ratio ~1.00× | `hyperfine` A/B, 15 runs, no null control; sub-1.01× is undecidable per §2.3. | Yes (hyperfine recipe recorded). |
| 6 | 5730 | 2026-05-09 Same-leaf DELETE monotone duplicate-check skip | within noise | No null control. | Partly. |
| 7 | 8020 | 2026-05-07 Lazy conflict ring-buffer allocation | within noise on the focused probe | **Weakest of the ten** — the full-quick matrix independently moved the wrong way, which is a real signal the neutral focused probe does not undo. | Yes. |
| 8 | 1374 | 2026-05-24 VDBE `Opcode::Int64` hot-dispatch removal | within noise | No null control. Note the 2026-07-10 AUDIT v2 already validated the *sibling* hot-arm rejects by dispatch count; this one was not covered. | Yes — AUDIT v2's dispatch-counter harness applies directly. |
| 9 | 1769 | 2026-05-20 Planner reuse of `rowid_equality_term` for RowidLookup probe | within noise | No null control. | Partly. |
| 10 | 14960 | 2026-05-22 WASM private `init` start-function export removal | within noise | No null control; WASM lane, off the perf-campaign critical path. | Low priority. |

Excluded from the queue after inspection: 20 entries that are screens / audits / summaries /
pre-edit kills rather than primary lever measurements, and 1 entry that **did** record a null
control. One further neutral-verdict entry (L19895) is the `bd-x5gzk` benchmark-integrity
finding, not a lever — resolved separately in commit `ffda8070`.

---

## Re-run status

The top five were re-run on 2026-07-26. The release-perf executable reported
SHA-256
`bbbbe7c56ed067aa07ca47d9a403875ed91d111232c600cf08ba96f05fe9cc64`
(25,949,584 bytes) from worker `vmi1153651`. Its source identity was:
`pipeline_stage_bench.rs` `7c1bf644…`, `cursor.rs` `44708dee…`,
`value.rs` `88e7bbff…`, and `connection.rs` `5aa035df…`. Every timed case
used four independent fixtures (A/A plus A/B), 41 paired rounds, min-of-three,
10,000 median-bootstrap resamples, exact output checksums, and a counted
candidate dispatch. CV was printed but never gated.

| Queue | Corrected class | Counted mechanism | Claim median, 95% CI | A/A 95% CI | Result |
|---:|---|---:|---|---|---|
| 1 seek-cache MRU | `VALID-AB` | 488,064 hits | `1.007533 [0.970819, 1.057989]` | `[0.969946, 1.055463]` | `INCONCLUSIVE` |
| 2 DELETE search hint | `VOID-ZEROSELF` | **0 hits** | not interpreted | not published | `INVALID` |
| 3 SmallText traits | `VALID-AB` | 4,975,104 hits | `1.026980 [0.986791, 1.084763]` | `[0.993031, 1.036486]` | `INCONCLUSIVE` |
| 4 fixed-width REAL | `VALID-AB` | 503,808 hits | `0.978947 [0.956454, 1.021725]` | `[0.956153, 1.028183]` | `INCONCLUSIVE` |
| 5 lazy decoded scratch | `VALID-AB` | 15,744 hits | `0.991003 [0.938007, 1.078962]` | `[0.987739, 1.011725]` | `INCONCLUSIVE` |

The corrected yield is therefore **583 audited / 10 originally VOID / 5
re-run / 0 re-won**. Four rows are no longer evidentially void, but none clears
the keep/reject band; the DELETE row is confirmed unreachable by a stronger
zero-dispatch proof. Exact commands, full hashes, decision thresholds, and
per-row retry predicates are in
`docs/progress/perf-negative-results.md` under the dated top-five
resurrection entry.

---

## Method worth porting to the other ten repos

From `## 2026-07-10 - AUDIT v2`: when you need to prove a benchmark actually executes the code
under test, **exact dispatch counters beat sampled self-time**. Sampling can fail to obtain a
profile at all (AUDIT v2 was written precisely because sampled self-time was unobtainable), and
a 0-sample frame is ambiguous between "never ran" and "ran below the sampling floor". A counter
incremented in the dispatch path is unambiguous, survives a stripped binary, and costs one
`add` per dispatch. `crates/fsqlite-vdbe/tests/hot_arm_stream_reachability.rs` is the reference
implementation.

Second, from the same entry: **a non-null effect is itself an execution proof.** If removing a
code path makes the benchmark *slower*, that path demonstrably ran. Self-time need not be
re-derived; the effect direction settles it. This cheaply rescues rejects that would otherwise
be flagged void for missing profile data.
