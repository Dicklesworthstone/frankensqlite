# `fsqlite-wasm`

`fsqlite-wasm` is the Rust crate that produces FrankenSQLite's browser-facing
WebAssembly package.

The intended npm artifact is published as `@frankensqlite/core` and exposes the
generated `wasm-bindgen` glue plus the `FrankenDB` / `FrankenPreparedStatement`
APIs implemented in [`src/lib.rs`](./src/lib.rs).

## Package Build

Build a publishable package into `target/fsqlite-wasm-pkg/`:

```bash
./scripts/build_fsqlite_wasm_package.sh
```

Choose a different output directory or `wasm-pack` target:

```bash
FSQLITE_WASM_TARGET=web ./scripts/build_fsqlite_wasm_package.sh target/fsqlite-wasm-web
FSQLITE_WASM_TARGET=nodejs ./scripts/build_fsqlite_wasm_package.sh target/fsqlite-wasm-node
```

The helper script:

- runs `wasm-pack build`
- normalizes the generated `package.json` to the `@frankensqlite/core` package name
- copies README/license files into the output package
- validates the generated `.wasm`, `.js`, and `.d.ts` artifacts exist
- runs `npm pack` so the result is ready for registry or local install testing
- enforces a packed tarball size budget of 2 MiB by default (`FSQLITE_WASM_MAX_PACKED_BYTES=0` disables the guard)

## Expected Package Contents

- `frankensqlite_wasm_bg.wasm`
- `frankensqlite_wasm.js`
- `frankensqlite_wasm.d.ts`
- `snippets/`
- `README.md`
- `LICENSE`

## Import Example

```ts
import init, { FrankenDB } from "@frankensqlite/core";

await init();

const db = new FrankenDB(":memory:");
db.execute("CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)");
db.execute("INSERT INTO users(name) VALUES('Ada')");

const result = db.query("SELECT id, name FROM users ORDER BY id");
console.log(result.rows);
```
