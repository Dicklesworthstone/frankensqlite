# Read completed prerequisite results as a bounded snapshot

`queue.dependencyResults(lease, controls?)` reads the results of every immutable
prerequisite for the leased job, including parents in other queues on the SAME
database. It is the data-flow counterpart to the existing all-parent claim gate:
callers need not fetch complete parent jobs (including their payloads) one by one.

```ts
const inputs = await queue.dependencyResults(lease, {
  maxBytes: 4 * 1024 * 1024,
  signal,
  timeoutMs: 10_000,
});
const values = inputs.map(input => ({
  queue: input.queue,
  id: input.id,
  result: input.result,
}));
```

The frozen result array contains frozen `{queue, id, result, byteLength}` records
in the canonical dependency order. A root job returns an empty array. SQL NULL
and empty text remain distinct. BLOB projection and explicit database-encoding
decoding preserve embedded NUL characters, leading BOMs, and Unicode in UTF-8,
UTF-16LE, and UTF-16BE databases. Malformed encoded text is rejected, not silently
replacement-decoded. Parent payloads and error fields are never selected.

## Admission and lease checks

The complete dependency list, parent completion states, result sizes, and result
bodies are read in ONE transaction snapshot. The exact child lease identity is
checked before metadata admission and again after reading; expired, cancelled,
reclaimed or mismatched leases reject. Every parent must still be completed in
that snapshot. Missing/incomplete parents and damaged dependency storage reject
instead of returning an incomplete list or treating the child as a root.

`maxBytes` defaults to 4 MiB, accepts 0..64 MiB, and counts result bytes in the
DATABASE'S stored text encoding. This is deliberately not a UTF-8 quota: ASCII
text occupies twice as many stored bytes in UTF-16 as in UTF-8. Each entry's
`byteLength` uses the same units. Every metadata size is checked BEFORE the first
result body is loaded; an oversized join returns no prefix. Existing per-result
1-MiB UTF-8 limits still apply after strict decoding. A zero-byte quota admits
only NULL/empty results. At most 128 parents can be read.

The body query rechecks admitted type, size, and completed state. Database-encoded
parent identifiers are bounded before crossing the SQL adapter. These bounds
limit transferred data and retained results, not SQLite scan cost, process RSS,
wall time, or all JavaScript allocation copies.

Optional `signal` and `timeoutMs` are captured before admission; the deadline is
monotonic and includes queueing. Cancellation waits for an already-admitted SQL
operation to settle before propagating. No unjoined timeout race, SQL interrupt,
new transaction queue, writer mutex, retry loop, or schema repair is introduced.

## Snapshot and durability boundaries

This is a READ, not a claim, renewal, completion, checkpoint, or authorization
boundary. Another connection can cancel the lease or change trusted SQL state
after this read's snapshot. The caller must still observe its worker scope and
use the existing fenced completion APIs; returned inputs never authorize later
external effects by themselves. The final lease check sees the same SQL snapshot
and a fresh wall clock, not a magic view of subsequently committed changes.

The transaction owner must provide coherent snapshot semantics and complete
commit/rollback before settling. Failures and lost read responses are not success.
Parent completion and persistence still require the host's genuine storage
confirmation. Independently imported browser snapshots are not a shared queue.
Direct modification of trusted job/dependency SQL remains outside the API's
integrity guarantees. No external authentication or anti-rollback claim is made.

## Executed verification

```sh
node --experimental-transform-types \
  --experimental-loader=./packages/sdk/tests/helpers/production-source-loader.mjs \
  --test packages/sdk/tests/durable-job-results.test.mjs
```

41 tests pass on Node 22.16.0 / reference SQLite 3.49.1 with no skips. They execute
the actual current queue module and the existing SQL ownership adapter, not a
replacement job backend. Cases include all three encodings, NUL/BOM/NULL/empty
results, exact byte boundaries, 128 parents, corruption, stale/reclaimed leases,
mid-read expiry and cancellation, caller ownership, TEMP/attached isolation,
missing schema, same-owner mutation, independent WAL snapshots, and file reopen
following a lost parent-completion acknowledgement. The tests perform no new
process-kill or power-loss experiments. Strict TypeScript 5.8.3 checking passes
on the actual queue source. These are SDK-component/reference-SQL results, not
full SDK/worker packaging, FrankenSQLite Rust/WASM/MVCC, or browser certification.
