# `@frankensqlite/worker`

`@frankensqlite/worker` owns the browser worker-side transport for
FrankenSQLite's WebAssembly bindings. It loads `@frankensqlite/core`,
maintains a single worker-owned `FrankenDB` instance, and exposes a typed
message protocol that higher-level SDKs can consume.

Current behavior:

- `memory` persistence works end to end.
- `opfs` and `indexeddb` are rejected with explicit "not implemented yet"
  worker errors instead of silently falling back.

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

The core-package CI/publish path is configured to build this exact
worker-compatible set, verify the generated declarations and runtime through
the worker and SDK, and publish the tested archive. A release remains blocked
until that workflow produces a green artifact within its size budgets; the
minimal default crate build alone is not compatible with this worker.

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
