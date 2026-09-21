// A deliberately small transactional reference model for unit tests, NOT a
// browser IndexedDB implementation. Browser acceptance has its own runner.
// Models the subset the snapshot store uses: queued transactions, structured
// cloning, atomic abort/commit, open/upgrade events and close admission.
class Events extends EventTarget {
  emit(type, properties = {}) {
    const event = new Event(type);
    Object.assign(event, properties);
    this[`on${type}`]?.(event);
    this.dispatchEvent(event);
  }
}
class Request extends Events {
  result;
  error = null;
}
const task = (work) => setImmediate(work);

export class ModelObjectStore {
  constructor(transaction) {
    this.transaction = transaction;
  }
  get(key) {
    return this.transaction.request(() => structuredClone(this.transaction.values.get(key)));
  }
  put(value, key) {
    if (this.transaction.mode !== "readwrite") throw new DOMException("readonly", "ReadOnlyError");
    const copy = structuredClone(value);
    return this.transaction.request(() => {
      this.transaction.values.set(key, copy);
      return key;
    });
  }
}
class Transaction extends Events {
  active = true;
  started = false;
  requests = [];
  error = null;
  values = new Map();
  constructor(db, mode, durability) {
    super();
    this.db = db;
    this.mode = mode;
    this.durability = durability;
    db.state.queue.push(this);
    task(() => this.advance());
  }
  objectStore(name) {
    if (!this.db.state.stores.has(name)) throw new DOMException("missing", "NotFoundError");
    return new ModelObjectStore(this);
  }
  request(work) {
    if (!this.active) throw new DOMException("inactive", "TransactionInactiveError");
    const request = new Request();
    this.requests.push({ request, work });
    return request;
  }
  abort() {
    if (!this.active) throw new DOMException("inactive", "InvalidStateError");
    this.active = false;
    task(() => {
      this.emit("abort");
      this.release();
    });
  }
  release() {
    const index = this.db.state.queue.indexOf(this);
    if (index >= 0) this.db.state.queue.splice(index, 1);
    task(() => this.db.state.queue[0]?.advance());
  }
  advance() {
    if (!this.active || this.db.state.queue[0] !== this) return;
    if (!this.started) {
      this.started = true;
      this.values = structuredClone(this.db.state.values);
    }
    const next = this.requests.shift();
    if (next) {
      try {
        next.request.result = next.work();
        next.request.emit("success");
      } catch (error) {
        this.error = error;
        next.request.error = error;
        next.request.emit("error");
        if (this.active) this.abort();
      }
      task(() => this.advance());
    } else {
      if (this.mode === "readwrite") this.db.state.values = this.values;
      this.active = false;
      this.emit("complete");
      this.release();
    }
  }
}
class Database extends Events {
  closed = false;
  constructor(state) {
    super();
    this.state = state;
    state.connections.add(this);
  }
  get objectStoreNames() {
    return { contains: (name) => this.state.stores.has(name) };
  }
  createObjectStore(name) {
    this.state.stores.add(name);
  }
  transaction(store, mode, options) {
    if (this.closed) throw new DOMException("closed", "InvalidStateError");
    if (!this.state.stores.has(store)) throw new DOMException("missing", "NotFoundError");
    return new Transaction(this, mode, options?.durability ?? "default");
  }
  close() {
    this.closed = true;
    this.state.connections.delete(this);
  }
}

export function installIndexedDbModel() {
  const databases = new Map();
  const factory = {
    open(name, version) {
      const request = new Request();
      task(() => {
        let state = databases.get(name);
        if (state && version !== undefined && version < state.version) {
          request.error = new DOMException("older version", "VersionError");
          request.emit("error");
          return;
        }
        const oldVersion = state?.version ?? 0;
        const nextVersion = version ?? (oldVersion || 1);
        if (!state) {
          state = {
            version: nextVersion,
            values: new Map(),
            stores: new Set(),
            connections: new Set(),
            queue: [],
          };
          databases.set(name, state);
        } else if (nextVersion > oldVersion) {
          for (const connection of state.connections) connection.emit("versionchange");
          if (state.connections.size) {
            request.emit("blocked");
            return;
          }
          state.version = nextVersion;
        }
        request.result = new Database(state);
        if (oldVersion !== nextVersion) request.emit("upgradeneeded", { oldVersion });
        task(() => request.emit("success"));
      });
      return request;
    },
  };
  globalThis.indexedDB = factory;
  globalThis.IDBObjectStore = ModelObjectStore;
  return { databases, factory };
}

export function snapshotImage(marker = 7, pageSize = 512) {
  const bytes = new Uint8Array(pageSize);
  bytes.set(new TextEncoder().encode("SQLite format 3\0"));
  bytes[16] = pageSize === 65536 ? 0 : pageSize >> 8;
  bytes[17] = pageSize === 65536 ? 1 : pageSize & 255;
  bytes[100] = marker;
  return bytes;
}
