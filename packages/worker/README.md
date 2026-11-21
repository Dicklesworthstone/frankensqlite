# `@frankensqlite/worker`

`@frankensqlite/worker` owns the browser worker-side transport for
FrankenSQLite's WebAssembly bindings. It loads `@frankensqlite/core`,
maintains a single worker-owned `FrankenDB` instance, and exposes a typed
message protocol that higher-level SDKs can consume.

Current behavior:

- `memory` is the default. The worker can also save and reopen whole-database
  checkpoints with `opfs-snapshot` or `indexeddb-snapshot`.
- The page-level `opfs` and `indexeddb` modes return
  `ERR_FSQLITE_UNSUPPORTED_PERSISTENCE`.

## Required Core Build

The worker is compatible only with an `@frankensqlite/core` artifact built
with the default `wasm-runtime-minimal` profile plus these exact five API
features:

```text
backup,batch-execution,diagnostics,prepared-statements,row-arrays
```

Those features provide the import/export, batch, prepared-statement,
diagnostic metadata, and positional-row surfaces used by the worker protocol.
Build the compatible core package with:

```bash
FSQLITE_WASM_FEATURES=backup,batch-execution,diagnostics,prepared-statements,row-arrays \
  ./scripts/build_fsqlite_wasm_package.sh
```

Build and publication are manual. Build a fresh core archive from the exact
release commit with the feature set above, verify its generated declarations
and runtime through the worker and SDK, and confirm the core size budgets
before publishing any browser package.

## Package Surface

- `createFrankenSqliteWorker()` creates a module worker from the packaged
  `worker.js` entrypoint.
- `WorkerConnectionHost` handles request/response dispatch inside the worker.
- `protocol` exports the request/response/message types shared with the SDK.

## Example

```ts
import { createFrankenSqliteWorker } from "@frankensqlite/worker";

const worker = createFrankenSqliteWorker();
worker.postMessage({
  kind: "init",
  requestId: 1,
  config: { persistence: "memory" },
});
```

## Transport failure and operation ownership

Replies are not proof that an operation never executed when decoding or delivery
fails. The SDK rejects malformed correlated replies with
`ERR_FSQLITE_WORKER_RESPONSE` and marks them non-transient. Its bounded remote
error decoder preserves valid SQLite codes and cleanup causes, while rejecting
cycles, excessive nesting, malformed fields and unsupported response kinds.
Valid late or duplicate response IDs without an outstanding request are inert.

When the worker cannot clone or transfer a result, it sends a primitive-only
`ERR_FSQLITE_RESPONSE_TRANSFER` error for that same request. SQL is not run again.
An autocommit `INSERT ... RETURNING` may already be committed; inside a managed
transaction the caller can still execute its normal rollback. If serializing an
operation error itself fails, `ERR_FSQLITE_WORKER_DISPATCH` is the final
correlated fallback. These errors do not justify automatic write retries.

`messageerror` is handled on both ends of the channel. A receiver-side failure
has no reliable request ID, so the worker sends a `WorkerFatalMessage`:
`{ kind: "worker-fatal", error }`, with no invented correlation ID. `WorkerMessage`
is the union of this notice and ordinary `WorkerResponse`. The worker immediately
fences later SQL and joins active operations before closing its database. The
SDK stops admission and notifies queues, subscriptions, scans and snapshot-pool
owners. Failures of accepted requests join that real close fence before they
can dispose the worker, including during initialization or a managed transaction.
A later close receives its own correlated acknowledgement of the same cleanup.

If even the fatal notice cannot be sent, the worker raises an actual worker
error rather than leaving an unobserved rejected dispatch promise. An owner-side
`messageerror` or worker crash rejects pending work and marks the connection
unusable with `ERR_FSQLITE_WORKER_TRANSPORT`. Partial listener registration is
cleaned up; disposal removes message, error and messageerror listeners. Custom
`WorkerLike` implementations must support all three event types, not map a
deserialization failure to an ordinary successful response.

**None of these transport errors establishes a write outcome.** Active SQL or
snapshot publication may have finished. Inspect authoritative state after reopen
and reconcile application operation IDs; do not automatically replay writes or
report an uncertain commit as a successful rollback. A failed channel cannot
provide an exactly-once outcome guarantee. Cleanup is cooperative and cannot
interrupt one core operation that never settles. Terminal faults do not silently
replace pool replicas or restart live queries.

The focused tests run from the repository root with its TypeScript tooling:

```sh
node --loader ./packages/sdk/tests/helpers/source-loader.mjs --test \
  packages/sdk/tests/transport.test.mjs \
  packages/sdk/tests/worker-transport.test.mjs
```

They include actual Node worker threads importing production `worker.ts`, real
structured-clone failures, native SQLite writes/rollback/reopen checks, failed
response and fallback sends, worker error propagation, and held-operation close
fences. Injected `messageerror` events are explicitly event models, not evidence
of a browser deserialization failure. The SQLite reference engine is not the
FrankenSQLite WASM artifact; real browser/WASM and release gates remain separate.

## Manual npm Release

Build the compatible core described above, then test the worker and SDK tarballs
together in an installed browser application. The release procedure prints the
exact archives to publish after the core is available from the registry.
[Manual npm release](https://github.com/Dicklesworthstone/frankensqlite/tree/main/packages/sdk#manual-npm-release)

## License

This package uses the repository's custom MIT License with OpenAI/Anthropic
Rider. It is not the SPDX `MIT` license. The complete, controlling terms are in
the package-local [`LICENSE`](LICENSE) file and are included in the npm tarball.
