# Query-result delivery limits

Applications can request an acknowledged policy through the SDK:

```ts
import { FrankenDB } from "@frankensqlite/sdk";

const db = await FrankenDB.open({
  resultLimits: { maxRows: 10_000, maxCells: 100_000, maxBytes: 8 * 1024 * 1024 },
  resultEncoding: "auto",
});
console.log(db.resultLimits); // Frozen effective limits, possibly tighter.
```

An explicit `resultLimits: {}` enables defaults. Options are captured and
validated before worker creation and again at request admission, so mutation
while initialization waits cannot change the policy. An older worker that omits
the requested acknowledgement, or one reporting looser/invalid limits, is rejected
before `open()` exposes a database handle. Omitting the request preserves legacy
compatibility; `db.resultLimits === null` means no policy was acknowledged.

A custom worker can independently impose a ceiling:

```ts
import { WorkerConnectionHost } from "@frankensqlite/worker";

const host = new WorkerConnectionHost(undefined, {}, {}, {
  maxRows: 10_000,
  maxCells: 100_000,
  maxBytes: 8 * 1024 * 1024,
});
```

The fourth constructor argument is optional. Omitting it preserves the legacy
unbudgeted delivery behavior when initialization also omits `resultLimits`; `{}` enables defaults of 100,000 rows, 1,000,000
positional cells and a 64 MiB logical clone charge. Each positive integer limit
may be lowered, not raised above those defaults. The constructor captures and
validates the policy before loading or executing SQL. Initialization can request
tighter limits but cannot raise or disable that ceiling. The acknowledged policy
is published with the new session only after successful initialization. Failed
replacement leaves the old session and its policy intact; successful replacement
with no request returns to the host ceiling (or legacy unbudgeted behavior when
there is no ceiling). These limits concern each completed result, not aggregate
queued responses.

Both raw and prepared query results are checked before binary encoding or
structured cloning. Binary/auto fallback cannot bypass an explicit budget.
The host never truncates rows or silently changes the requested SQL. Ordinary
execute, export and checkpoint operations are outside this query-result policy.

## Byte accounting

`maxBytes` is a deterministic admission metric, **not** an exact serialized size,
JavaScript heap size, peak memory bound or SQL execution limit. It charges 16
bytes per value/container/property slot and per array index (including holes),
plus UTF-16 string/property-name bytes and complete ArrayBuffer storage. Both
named and positional rows are included. Aliased objects and backing buffers are
visited once; additional enumerable plain metadata and cycles are accounted for.
A tiny BLOB view can retain/clone a large backing buffer, so its complete backing
allocation is charged, even when binary encoding might copy only the view.

Fixed ordinary ArrayBuffers, Uint8Array/Buffer views, plain records, arrays and
SQL scalar values are supported, including cross-realm records/views. Accessor
properties, unstable/shared/detached buffers, exotic extension objects and graphs
deeper than 64 traversal frames are refused rather than measured by invoking
user accessors or pretending an incomplete accounting is complete. This is not
a sandbox against a malicious core adapter or JavaScript Proxy traps.

## Failures after SQL

`ERR_FSQLITE_RESULT_LIMIT` and `ERR_FSQLITE_RESULT_SHAPE` are non-transient
**delivery** failures. SQL has already executed. In particular, an oversized
`INSERT ... RETURNING` can have committed its inserts before the result is
rejected. Do not retry a write merely because its result was refused. Existing
managed-transaction failure/rollback rules still apply; this policy neither
starts a transaction nor asserts that rollback occurred. The connection remains
usable when no independent ownership/transport failure has fenced it.

The engine currently constructs the query result before this check. This avoids
additional codec/transport work on an oversized result, but does not prevent
eager core allocation. Use bounded SQL or the SDK's transaction-owned table
scan for large reads; engine-level streaming/memory limits remain separate work.

## Executable coverage

`tests/result-budget.test.mjs` executes the real host, result budget and codec
against Node's native reference SQLite adapter, with explicit admission,
statement-budget, storage and managed-transaction doubles. It covers both query
routes/all encodings, exact boundaries, BLOB backing storage, metadata, sparse
arrays, bad shapes, and committed INSERT RETURNING rows after a delivery refusal
(in memory and after file reopen). This is not a WASM/browser or FrankenSQLite
Rust-engine qualification.


`packages/sdk/tests/result-policy.test.mjs` additionally runs the actual SDK
`FrankenDB` and client, actual worker request admission and protocol, and the
shared policy helper over Node MessageChannel structured cloning/transfers and
native reference SQLite. The fixture names its remaining dependency doubles;
these are not full-package or browser tests. It checks request capture, atomic
replacement, host ceilings, acknowledgement refusal, prepared and direct
queries, and the same post-SQL failure semantics through the public SDK.
