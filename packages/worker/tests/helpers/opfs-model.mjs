// Copy-on-write OPFS and cooperative Web Locks MODEL. Not a browser, filesystem
// crash simulator or power-loss proof. Publication is delayed until close().
import assert from "node:assert/strict";

export function installOpfsModel() {
  const files = new Map(), directories = new Map(), tails = new Map();
  const sessions = new Set();
  const hooks = {}, counts = { writes: 0, publications: 0, aborts: 0, removals: 0 };
  const notFound = () => new DOMException("Missing entry", "NotFoundError");
  function directory(path) {
    if (directories.has(path)) return directories.get(path);
    const handle = {
      async getDirectoryHandle(name, options = {}) {
        assert.ok(name.length > 0 && !/[\\/]/.test(name));
        const child = `${path}/${name}`;
        if (!directories.has(child) && !options.create) throw notFound();
        return directory(child);
      },
      async getFileHandle(name, options = {}) {
        const key = `${path}/${name}`;
        if (!files.has(key)) {
          if (!options.create) throw notFound();
          files.set(key, new Uint8Array());
        }
        return {
          async getFile() {
            if (!files.has(key)) throw notFound();
            return new File([files.get(key)], name);
          },
          async createWritable(options) {
            assert.deepEqual(options, { keepExistingData: false });
            await hooks.create?.(key);
            const chunks = [];
            let closed = false;
            return {
              async write(bytes) {
                assert.equal(closed, false);
                counts.writes++;
                await hooks.write?.(key, bytes);
                chunks.push(new Uint8Array(bytes));
              },
              async close() {
                assert.equal(closed, false);
                await hooks.beforeClose?.(key);
                const all = new Uint8Array(chunks.reduce((n, chunk) => n + chunk.length, 0));
                let offset = 0;
                for (const chunk of chunks) { all.set(chunk, offset); offset += chunk.length; }
                files.set(key, all);
                counts.publications++;
                closed = true;
                await hooks.afterClose?.(key);
              },
              async abort() { counts.aborts++; closed = true; await hooks.abort?.(key); },
            };
          },
        };
      },
      async removeEntry(name) {
        if (!files.delete(`${path}/${name}`)) throw notFound();
        counts.removals++;
      },
    };
    directories.set(path, handle);
    return handle;
  }
  const locks = {
    request(name, options, callback) {
      if (options.ifAvailable === true) {
        assert.ok(name.startsWith("frankensqlite:snapshot-session:v1:"));
        assert.ok(options.mode === "shared" || options.mode === "exclusive");
        assert.deepEqual(options, { mode: options.mode, ifAvailable: true });
        return Promise.resolve().then(async () => {
          if ([...sessions].some(lock => lock.name === name &&
              (options.mode === "exclusive" || lock.mode === "exclusive"))) return callback(null);
          const lock = { name, mode: options.mode };
          sessions.add(lock);
          try { return await callback(lock); }
          finally { sessions.delete(lock); }
        });
      }
      assert.deepEqual(options, { mode: "exclusive" });
      const result = (tails.get(name) ?? Promise.resolve()).then(() => callback({ name, mode: "exclusive" }));
      const tail = result.then(() => {}, () => {});
      tails.set(name, tail);
      return result.finally(() => { if (tails.get(name) === tail) tails.delete(name); });
    },
  };
  const original = Object.getOwnPropertyDescriptor(globalThis, "navigator");
  Object.defineProperty(globalThis, "navigator", { configurable: true, value: {
    storage: { getDirectory: async () => directory("") }, locks,
  } });
  return { files, hooks, counts, locks, sessions, restore() {
    if (original) Object.defineProperty(globalThis, "navigator", original);
    else Reflect.deleteProperty(globalThis, "navigator");
  } };
}

export function databaseImage(marker = 0, pageSize = 512) {
  const bytes = new Uint8Array(pageSize);
  bytes.set(new TextEncoder().encode("SQLite format 3\0"));
  const encoded = pageSize === 65536 ? 1 : pageSize;
  bytes[16] = encoded >> 8; bytes[17] = encoded & 255;
  bytes[100] = marker;
  return bytes;
}

export function deferred() {
  let resolve;
  const promise = new Promise(r => { resolve = r; });
  return { promise, resolve };
}
