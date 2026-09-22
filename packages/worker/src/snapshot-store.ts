/** Explicit whole-image checkpoints, not an IndexedDB page VFS. */
export const MAX_SNAPSHOT_BYTES = 64 * 1024 * 1024;
const FORMAT = 1;
const STORE = "snapshots";
const HEAD = "head";

export interface SnapshotMetadata {
  readonly revision: string;
  readonly parentRevision: string | null;
  readonly byteLength: number;
  readonly sha256: string;
}

export interface StoredSnapshot extends SnapshotMetadata {
  readonly bytes: Uint8Array;
}

interface SnapshotRecord extends SnapshotMetadata {
  readonly format: number;
  readonly name: string;
  readonly bytes: ArrayBuffer;
}

export class SnapshotStoreError extends Error {
  constructor(
    readonly code: string,
    message: string,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = "SnapshotStoreError";
  }
}

/**
 * A named snapshot register with atomic compare-and-swap publication.
 * Each name has a separate IndexedDB database: unrelated images never share a
 * read/write transaction scope. SQL executes independently in memory. Only
 * checkpoint publication is serialized, by IndexedDB, across tabs/workers.
 */
export class IndexedDbSnapshotStore {
  readonly #db: IDBDatabase;
  readonly #name: string;
  #closed = false;

  private constructor(db: IDBDatabase, name: string) {
    this.#db = db;
    this.#name = name;
    // Do not block a later schema upgrade or origin-data removal indefinitely.
    db.onversionchange = () => {
      this.close();
    };
    db.onclose = () => {
      this.#closed = true;
    };
  }

  static open(name: string): Promise<IndexedDbSnapshotStore> {
    try {
      validateSnapshotName(name);
      if (typeof indexedDB === "undefined" || !globalThis.crypto?.subtle || !crypto.randomUUID) {
        throw new SnapshotStoreError(
          "ERR_FSQLITE_SNAPSHOT_UNAVAILABLE",
          "IndexedDB and secure-context Web Crypto are required for snapshot persistence",
        );
      }
      return new Promise((resolve, reject) => {
        const request = indexedDB.open(`frankensqlite:snapshot:v1:${name}`, FORMAT);
        let failed = false;
        const fail = (error: unknown): void => {
          failed = true;
          reject(error);
        };
        request.onblocked = () =>
          fail(
            new SnapshotStoreError(
              "ERR_FSQLITE_SNAPSHOT_BLOCKED",
              "Snapshot storage is blocked by another open browser connection",
            ),
          );
        request.onerror = () => fail(request.error ?? new Error("Cannot open snapshot storage"));
        request.onupgradeneeded = (event) => {
          if (failed || event.oldVersion !== 0) {
            request.transaction?.abort();
            return;
          }
          request.result.createObjectStore(STORE);
        };
        request.onsuccess = () => {
          const db = request.result;
          // IDBOpenDBRequest cannot be cancelled. A blocked open may finish
          // after we rejected it; close that late handle rather than leak it.
          if (failed) {
            db.close();
          } else if (!db.objectStoreNames.contains(STORE)) {
            db.close();
            fail(corrupt("Snapshot object store is missing"));
          } else {
            resolve(new IndexedDbSnapshotStore(db, name));
          }
        };
      });
    } catch (error: unknown) {
      return Promise.reject(error);
    }
  }

  async load(): Promise<StoredSnapshot | null> {
    return this.#readVerified();
  }

  async #readVerified(): Promise<StoredSnapshot | null> {
    this.#assertOpen();
    // Read and complete the transaction BEFORE awaiting the asynchronous hash.
    // Otherwise IndexedDB may auto-commit while the request is suspended.
    const record = await new Promise<unknown>((resolve, reject) => {
      const transaction = this.#db.transaction(STORE, "readonly");
      const request = transaction.objectStore(STORE).get(HEAD);
      transaction.onabort = () => reject(transaction.error ?? new Error("Snapshot read aborted"));
      transaction.oncomplete = () => resolve(request.result);
    });
    if (record === undefined) return null;
    const value = validateRecord(record, this.#name);
    const bytes = new Uint8Array(value.bytes);
    if ((await checksum(bytes)) !== value.sha256)
      throw corrupt("Snapshot SHA-256 does not match its bytes");
    return { ...metadata(value), bytes };
  }

  /**
   * Publish only if the saved revision is still expectedRevision. null means
   * create-only, NOT unconditional overwrite. Tokens are random, not counters,
   * so origin eviction/recreation cannot cause a stale-writer ABA match.
   * Completion means IDBTransaction.complete, never merely put().success.
   */
  async save(
    bytes: Uint8Array,
    expectedRevision: string | null,
    publicationId?: string,
  ): Promise<SnapshotMetadata> {
    this.#assertOpen();
    if (expectedRevision !== null && !validRevision(expectedRevision)) {
      throw new SnapshotStoreError(
        "ERR_FSQLITE_SNAPSHOT_INPUT",
        "Invalid expected snapshot revision",
      );
    }
    if (
      publicationId !== undefined &&
      (!validRevision(publicationId) || publicationId === expectedRevision)
    ) {
      throw new SnapshotStoreError(
        "ERR_FSQLITE_SNAPSHOT_INPUT",
        "Invalid checkpoint publication identity",
      );
    }
    validateSnapshotBytes(bytes);
    // Own an exact-size copy before yielding. Do not detach the caller's data,
    // persist a view's unrelated backing bytes, or race caller buffer mutation.
    const owned = new Uint8Array(bytes);
    const record: SnapshotRecord = {
      format: FORMAT,
      name: this.#name,
      revision: publicationId ?? crypto.randomUUID(),
      parentRevision: expectedRevision,
      byteLength: owned.byteLength,
      sha256: await checksum(owned),
      bytes: owned.buffer,
    };
    // Hash the authoritative head before replacing it. Crypto must run outside
    // a transaction: awaiting it inside onsuccess can let IndexedDB commit.
    const previous = await this.#readVerified();
    assertRevision(previous?.revision ?? null, expectedRevision);
    this.#assertOpen();
    await new Promise<void>((resolve, reject) => {
      const transaction = this.#db.transaction(STORE, "readwrite", { durability: "strict" });
      let failure: unknown;
      transaction.onabort = () =>
        reject(failure ?? transaction.error ?? new Error("Snapshot write aborted"));
      transaction.oncomplete = () => resolve();
      // Refuse implementations that silently ignore the requested durability
      // policy. "strict" remains a browser hint, not a power-loss guarantee.
      if (transaction.durability !== "strict") {
        failure = new SnapshotStoreError(
          "ERR_FSQLITE_SNAPSHOT_UNAVAILABLE",
          "This browser does not support strict IndexedDB durability",
        );
        transaction.abort();
        return;
      }
      const store = transaction.objectStore(STORE);
      const request = store.get(HEAD);
      request.onsuccess = () => {
        try {
          const current =
            request.result === undefined ? null : validateRecord(request.result, this.#name);
          assertRevision(current?.revision ?? null, expectedRevision);
          // Recheck the exact verified envelope and bytes under the atomic CAS
          // transaction. A same-revision mutation must not bypass the digest
          // check, and a legitimate competing revision must remain a conflict.
          if (current !== null) assertVerifiedHead(current, previous);
          store.put(record, HEAD);
        } catch (error: unknown) {
          failure = error;
          transaction.abort();
        }
      };
    });
    return metadata(record);
  }

  /**
   * Reconstruct a lost receipt by reading and hashing authoritative storage.
   * Never exports SQL, republishes bytes, or changes the stored head. Failure
   * does NOT prove this publication never happened: it may have been replaced.
   */
  async confirmPublication(
    revision: string,
    parentRevision: string | null,
  ): Promise<SnapshotMetadata> {
    if (
      !validRevision(revision) ||
      (parentRevision !== null && !validRevision(parentRevision)) ||
      revision === parentRevision
    ) {
      throw new SnapshotStoreError(
        "ERR_FSQLITE_SNAPSHOT_INPUT",
        "Invalid checkpoint recovery identity",
      );
    }
    const saved = await this.load();
    if (saved === null || saved.revision !== revision || saved.parentRevision !== parentRevision) {
      throw new SnapshotStoreError(
        "ERR_FSQLITE_SNAPSHOT_NOT_CONFIRMED",
        "The stored checkpoint does not confirm this publication. Export and reconcile; never replay committed SQL.",
      );
    }
    return Object.freeze({
      revision: saved.revision,
      parentRevision: saved.parentRevision,
      byteLength: saved.byteLength,
      sha256: saved.sha256,
    });
  }

  /** Closes admission; already-started IDB transactions finish normally. */
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#db.close();
  }

  #assertOpen(): void {
    if (this.#closed)
      throw new SnapshotStoreError("ERR_FSQLITE_SNAPSHOT_CLOSED", "Snapshot storage is closed");
  }
}

export function validateSnapshotName(name: string): void {
  if (
    typeof name !== "string" ||
    name.trim().length === 0 ||
    name === ":memory:" ||
    name.length > 256 ||
    name.includes("\0")
  ) {
    throw new SnapshotStoreError(
      "ERR_FSQLITE_SNAPSHOT_INPUT",
      "Snapshot persistence requires a nonempty dbName of at most 256 characters (not :memory:)",
    );
  }
}

export function validateSnapshotBytes(bytes: Uint8Array): void {
  if (!(bytes instanceof Uint8Array)) {
    throw new SnapshotStoreError("ERR_FSQLITE_SNAPSHOT_INPUT", "Snapshot must be a Uint8Array");
  }
  if (bytes.byteLength > MAX_SNAPSHOT_BYTES) {
    throw new SnapshotStoreError(
      "ERR_FSQLITE_SNAPSHOT_TOO_LARGE",
      `Snapshot exceeds ${MAX_SNAPSHOT_BYTES} bytes`,
    );
  }
  const magic = "SQLite format 3\0";
  if (
    bytes.byteLength < 512 ||
    [...magic].some((value, index) => bytes[index] !== value.charCodeAt(0))
  ) {
    throw corrupt("Snapshot is not a SQLite database image");
  }
  const encodedPageSize = (bytes[16]! << 8) | bytes[17]!;
  const pageSize = encodedPageSize === 1 ? 65536 : encodedPageSize;
  if (
    pageSize < 512 ||
    pageSize > 65536 ||
    (pageSize & (pageSize - 1)) !== 0 ||
    bytes.byteLength % pageSize !== 0
  ) {
    throw corrupt("Snapshot has an invalid page size or a truncated page");
  }
}

function validateRecord(value: unknown, name: string): SnapshotRecord {
  if (typeof value !== "object" || value === null) throw corrupt("Invalid snapshot envelope");
  const record = value as Partial<SnapshotRecord>;
  if (
    record.format !== FORMAT ||
    record.name !== name ||
    !validRevision(record.revision) ||
    (record.parentRevision !== null && !validRevision(record.parentRevision)) ||
    record.parentRevision === record.revision ||
    typeof record.sha256 !== "string" ||
    !/^[0-9a-f]{64}$/.test(record.sha256) ||
    !(record.bytes instanceof ArrayBuffer) ||
    record.byteLength !== record.bytes.byteLength
  ) {
    throw corrupt("Unsupported or malformed snapshot envelope");
  }
  validateSnapshotBytes(new Uint8Array(record.bytes));
  return record as SnapshotRecord;
}

function assertRevision(actual: string | null, expected: string | null): void {
  if (actual !== expected) {
    throw new SnapshotStoreError(
      "ERR_FSQLITE_SNAPSHOT_CONFLICT",
      `Snapshot changed: expected ${expected ?? "no snapshot"}, found ${actual ?? "no snapshot"}. Reopen and merge; do not blindly retry.`,
    );
  }
}

function assertVerifiedHead(current: SnapshotRecord, verified: StoredSnapshot | null): void {
  if (
    verified === null ||
    current.revision !== verified.revision ||
    current.parentRevision !== verified.parentRevision ||
    current.byteLength !== verified.byteLength ||
    current.sha256 !== verified.sha256
  ) {
    throw corrupt("Snapshot changed without advancing its revision");
  }
  const bytes = new Uint8Array(current.bytes);
  for (let index = 0; index < bytes.length; index++) {
    if (bytes[index] !== verified.bytes[index])
      throw corrupt("Snapshot changed without advancing its revision");
  }
}

function metadata(record: SnapshotRecord): SnapshotMetadata {
  return {
    revision: record.revision,
    parentRevision: record.parentRevision,
    byteLength: record.byteLength,
    sha256: record.sha256,
  };
}

function validRevision(value: unknown): value is string {
  return (
    typeof value === "string" &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(value)
  );
}

async function checksum(bytes: Uint8Array): Promise<string> {
  const hash = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
  return [...hash].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

function corrupt(message: string): SnapshotStoreError {
  return new SnapshotStoreError("ERR_FSQLITE_SNAPSHOT_CORRUPT", message);
}
