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

For the v0.2.0 release, build and publication are manual. No GitHub Actions run
is required or used, and a green workflow is not release authorization. The
release operator must build a fresh core archive from the exact release commit
with the feature set above, verify its generated declarations and runtime
through the worker and SDK, and confirm the core size budgets before publishing
any browser package. The minimal default crate build alone is not compatible
with this worker.

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

## Manual npm Release

Publish the browser packages in dependency order:

1. `@frankensqlite/core`
2. `@frankensqlite/worker`
3. `@frankensqlite/sdk`

Start from the exact, clean release commit. Build and install the fresh core
archive described above, then run the worker typecheck and tests against that
installed archive. Inspect the worker package without publishing it, then
create the release tarball in a dedicated artifact directory:

```bash
npm pack --workspace @frankensqlite/worker --dry-run
mkdir -p artifacts/npm
npm pack --workspace @frankensqlite/worker --pack-destination artifacts/npm
```

The package's `prepack` script reruns typechecking, tests, and the TypeScript
build so the inspected or published package is built from the current source.
The pack listing must contain the expected `dist/` entries, `README.md`,
`LICENSE`, and `package.json`, and no source, test, or unrelated repository
files. Install and smoke-test the resulting
`artifacts/npm/frankensqlite-worker-0.2.0.tgz`; that exact archive is the only
worker payload eligible for publication. Publish the worker only after the
exact core version is available from the registry; publish the SDK only after
this exact worker version is available. Do not substitute an older `dist/`
tree or a package built from a different commit.

After those checks and the project-wide release gates pass, publish the exact
tested archive under the staging dist-tag:

```bash
npm publish artifacts/npm/frankensqlite-worker-0.2.0.tgz --access public --tag next
```

The manifest pins publication to the public npm registry with public access.
Promote `@frankensqlite/worker@0.2.0` to `latest` only after the core, worker,
and SDK archives have all been published under `next` and verified from clean
installs.

## License

This package uses the repository's custom MIT License with OpenAI/Anthropic
Rider. It is not the SPDX `MIT` license. The complete, controlling terms are in
the package-local [`LICENSE`](LICENSE) file and are included in the npm tarball.
