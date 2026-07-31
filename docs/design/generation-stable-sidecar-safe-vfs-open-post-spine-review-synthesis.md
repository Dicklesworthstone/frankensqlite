---
summary: "Controlling post-spine review synthesis for Decision 87 at exact RFC commit df09e95b."
read_when:
  - "You need the legal review closure for Decision 87 after governance receipt 9375 corrected its RFC reference."
type: "review"
---

# Post-Spine Review Synthesis: Generation-Stable Existing-Runtime Open

**Decision:** Agent Kernel decision 87
**Owner issue:** [frankensqlite#308](https://github.com/Dicklesworthstone/frankensqlite/issues/308)
**Exact RFC commit:** `df09e95bacebb860f84c7431db5b83f23b0f63c3`
**RFC reference:** `https://github.com/tryingET/frankensqlite/blob/df09e95bacebb860f84c7431db5b83f23b0f63c3/docs/design/generation-stable-sidecar-safe-vfs-open-rfc.md`
**Spine-repair receipt:** Agent Kernel governance receipt `9375`
**Fresh review-set plan:** `ak/decision/87/review-set-plan/df09e95b-post-spine-repair-20260731`
**Fresh review memo:** `asc://dispatch-1785515237888`
**Outcome:** `ready_for_adr`

## Why a fresh closure was required

Decision 87 originally pointed to packet commit `deff2de9` while later adversarial review evaluated `df09e95b`. Governance receipt `9375` corrected the decision spine by exact compare-and-swap, preserved every historical review attempt against the old RFC, cleared summary readiness, attached a fresh review-set plan, and left no legal closure.

The old review attempts remain evidence of the revision history. They do not close the corrected RFC.

## Fresh exact-revision review

The fresh runtime/backend lane re-read all three packet documents at `df09e95b` and returned `ready_for_adr`.

It found no remaining architecture blocker because the revised packet now:

1. assigns authority ownership to `fsqlite-pager` while keeping `fsqlite-vfs` primitive-only and `fsqlite-core` as the sole supported conforming connection API;
2. permits a technically public but safe hidden pager factory without accepting caller-assembled authority or exposing authority extraction;
3. treats every cooperative main mutation outside the retained authority's coherence protocol as generation-changing publication;
4. preserves ordinary authority-governed writes, admitted recovery, checkpoint, and concurrent-writer behavior;
5. admits pre-existing recovery artifacts only through an enumerated exact-binding rule and otherwise refuses before recovery;
6. routes every artifact family through authority-owned resolution or typed unsupported refusal;
7. defines pre-effect refusal, definite completion, and indeterminate effect as distinct outcomes; and
8. requires Agent Kernel task 4195 execution, readback, and receipts to descend exclusively from the conforming constructor with no weaker fallback.

## Controlling judgment

**Outcome: `ready_for_adr`.**

The exact RFC is coherent enough to record as an ADR. Remaining work is implementation planning and proof, not another architecture selection.

The ADR must preserve these constraints:

- no pathname-selected authoritative main I/O after admission;
- no canonical-name-only claim of sidecar provenance;
- no live generation-changing publication while a strong authority exists;
- no global writer serialization presented as safety proof;
- no public or caller-minted generation authority;
- no silent backend, feature, or API downgrade;
- no recovery of ambiguous pre-existing artifacts;
- no generic open error after potentially durable effects; and
- no downstream pin or task-4195 return without immutable owner and consumer proof.

## Non-authorizations

This synthesis authorizes only the next operator decision to open ADR authoring.

It does not authorize:

- implementation;
- release or merge;
- FrankenSQLite pin rotation;
- Agent Kernel integration;
- task 4195 resumption;
- task 4196 execution;
- Decision 86 progress; or
- canary activation.

## Next legal move

The authorized decision owner may advance Decision 87 to `adr_required` with outcome `accepted`, author the ADR, and then create the implementation and validation/rollout/rollback packet. Implementation remains separately gated after ADR recording and plan review.
