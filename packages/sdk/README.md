# `@frankensqlite/sdk`

`@frankensqlite/sdk` provides the async, worker-backed TypeScript client for
FrankenSQLite in browser environments.

Current behavior:

- `FrankenDB.open()` starts a dedicated module worker and initializes the WASM
  runtime through `@frankensqlite/worker`.
- `execute`, `executeBatch`, `query`, `prepare`, `export`, and `transaction`
  are exposed as Promise-based APIs.
- Persistence is intentionally memory-first until OPFS and IndexedDB backends
  land. Passing `opfs` or `indexeddb` surfaces an explicit worker error.

## Example

```ts
import { FrankenDB } from "@frankensqlite/sdk";

const db = await FrankenDB.open({ persistence: "memory" });
await db.execute("CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)");
await db.execute("INSERT INTO users(name) VALUES (?)", ["Ada"]);

const result = await db.query<{ id: number; name: string }>(
  "SELECT id, name FROM users ORDER BY id",
);

console.log(result.rows);
await db.close();
```

## Manual npm Release

For v0.2.0, release preparation and publication are manual. No GitHub Actions
run is required or used, and a green workflow is not release authorization.
Publish the browser packages in dependency order:

1. `@frankensqlite/core`
2. `@frankensqlite/worker`
3. `@frankensqlite/sdk`

Start from the exact, clean release commit. Build the worker-compatible core
archive and validate the worker and SDK against it, then publish the exact core
and worker versions before packaging the SDK. Inspect the SDK package without
publishing it, then create the release tarball in a dedicated artifact
directory:

```bash
npm pack --workspace @frankensqlite/sdk --dry-run
mkdir -p artifacts/npm
npm pack --workspace @frankensqlite/sdk --pack-destination artifacts/npm
```

The package's `prepack` script reruns typechecking, tests, and the TypeScript
build so the inspected or published package is built from the current source.
The pack listing must contain the expected `dist/` entries, `README.md`,
`LICENSE`, and `package.json`, and no source, test, or unrelated repository
files. Install and smoke-test the resulting
`artifacts/npm/frankensqlite-sdk-0.2.0.tgz`; that exact archive is the only SDK
payload eligible for publication. Publish only after
`@frankensqlite/core@0.2.0` and `@frankensqlite/worker@0.2.0` are available from
the registry. Do not substitute an older `dist/` tree or a package built from
a different commit.

After those checks and the project-wide release gates pass, publish the exact
tested archive under the staging dist-tag:

```bash
npm publish artifacts/npm/frankensqlite-sdk-0.2.0.tgz --access public --tag next
```

The manifest pins publication to the public npm registry with public access.
After all three packages have been verified from clean registry installs,
promote `latest` in dependency order: core, worker, then SDK.

## License

This package uses the repository's custom MIT License with OpenAI/Anthropic
Rider. It is not the SPDX `MIT` license. The complete, controlling terms are in
the package-local [`LICENSE`](LICENSE) file and are included in the npm tarball.
