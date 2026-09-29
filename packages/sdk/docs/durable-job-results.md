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

The first increment passed 41 tests on Node 22.16.0 / reference SQLite 3.49.1 with no skips. They execute
the actual current queue module and the existing SQL ownership adapter, not a
replacement job backend. Cases include all three encodings, NUL/BOM/NULL/empty
results, exact byte boundaries, 128 parents, corruption, stale/reclaimed leases,
mid-read expiry and cancellation, caller ownership, TEMP/attached isolation,
missing schema, same-owner mutation, independent WAL snapshots, and file reopen
following a lost parent-completion acknowledgement. The tests perform no new
process-kill or power-loss experiments. Strict TypeScript 5.8.3 checking passes
on the actual queue source. These are SDK-component/reference-SQL results, not
full SDK/worker packaging, FrankenSQLite Rust/WASM/MVCC, or browser certification.

## Worker-managed data flow

Enable `loadDependencyResults: true` on the existing `DurableJobWorker` to load
inputs before a handler is admitted. Results are exposed through the frozen
`context.dependencyResults` array; roots receive `[]`. With the default false,
that property is undefined and no input reads occur. Existing adapters remain
compatible; opt-in requires a `dependencyResults` implementation at startup.

```ts
const worker = DurableJobWorker.start(jobs, async (lease, context) => {
  const inputs = context.dependencyResults!;
  const combined = inputs.map(input => ({ id: input.id, result: input.result }));
  return {
    result: JSON.stringify(combined),
    apply: async tx => {
      await tx.execute(
        'INSERT INTO workflow_outputs(job_id, output) VALUES(?, ?)',
        [lease.id, JSON.stringify(combined)],
      );
    },
  };
}, {
  owner: 'dataflow-worker',
  loadDependencyResults: true,
  maxDependencyResultBytes: 4 * 1024 * 1024,
  stopWhenIdle: true,
});
await worker.done;
```

`maxDependencyResultBytes` is a captured per-job stored-encoding quota, 0..64 MiB,
defaulting to 4 MiB. Inputs consume the ORIGINAL claim's monotonic lease budget.
The same job does not start a heartbeat transaction while its result read is
active. After reading, the worker checks lease/cancellation before starting the
handler and its heartbeat; a slow read cannot create a fresh full lease duration.
Choose a lease duration that accommodates admission. Across concurrent handlers,
per-job budgets add up; this is not a global process-memory governor.

Input/storage errors, malformed adapter replies and over-limit joins stop the
supervisor with phase `dependency-results`, before invoking the handler. It does
not call handler-failure scheduling or completion for an uncertain input read.
The already-committed claim may remain leased until explicit reconciliation or
normal expiry recovery. Sibling handlers are signalled and joined. Known lease
loss uses the existing lease-loss path. A cancellation acknowledged by the read
joins SQL cleanup and follows normal lease-aware cancelled-job handling.

Graceful stop drains an admitted input read and job. Abort waits for the read to
settle, then prevents handler admission; it cannot interrupt arbitrary SQL owners
or force their promises to settle. No detached read, new scheduler, writer lock,
or implicit callback replay is introduced. Inputs are copied and validated before
user code receives them, including identity uniqueness, text limits, own data
fields, and consistent byte counts. The existing completion fence remains the
authority for publishing output and any next workflow graph.

The SQL metadata read joins the ORIGINAL stored dependency edge to its parent.
This prevents a corrupt encoded edge identity that the SQL adapter would decode
lossily from selecting a different, valid replacement-character job. It does not
provide authentication against a trusted SQL writer forging the entire graph.

### Combined verification

Run the reader command above together with
`packages/sdk/tests/durable-job-worker-results.test.mjs`,
`packages/sdk/tests/durable-job-callback-lifetime.test.mjs`,
`packages/sdk/tests/durable-job-continuations.test.mjs`, and
`packages/sdk/tests/durable-job-worker-continuations.test.mjs`.

150 tests pass: 44 result-reader tests (including three encoded-edge regressions),
33 new worker-input tests, and 73 unchanged callback/continuation/worker tests.
The production queue and worker execute over reference SQLite transaction owners;
adapter-reply fault injection explicitly tests the worker's input boundary.
Cases include actual output-dependent fan-out/fan-in continuations in all three
encodings, root/default/legacy behavior, immutable inputs, malformed results,
shutdown joins, read/heartbeat separation, lease expiry, sibling cancellation and
fresh file owners consuming retained output without rerunning completed parents.

The unchanged continuation suite reruns its eight IPC-confirmed SIGKILL/reopen
cuts under WAL/DELETE; this feature adds no new process-kill scenarios. Strict
TypeScript 5.8.3 checks both actual source modules. These remain SDK-component
results using Node 22.16.0 / SQLite 3.49.1, not native FrankenSQLite Rust/WASM/MVCC,
full FrankenDB/worker packaging, browser persistence or physical power-loss proof.
