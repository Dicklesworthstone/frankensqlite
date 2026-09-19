import {
  MAX_SNAPSHOT_BYTES, SnapshotStoreError, validateSnapshotBytes, validateSnapshotName,
} from "./snapshot-store";
import type { SnapshotMetadata, StoredSnapshot } from "./snapshot-store";

const MAGIC = new TextEncoder().encode("FSQLOP01");
const PREFIX_BYTES = MAGIC.length + 4;
const MAX_HEADER_BYTES = 4096;
const ROOT = "frankensqlite-snapshots-v1";
const HEAD = "head";

interface Envelope extends SnapshotMetadata {
  readonly format: 1;
  readonly name: string;
}

/**
 * Explicit whole-image OPFS checkpoints, NOT a page VFS or native multi-tab
 * MVCC. Web Locks serialize the head comparison and publication for one name;
 * independent databases do not share a lock. Same-origin writers must use this
 * protocol: Web Locks are cooperative, not an authorization boundary.
 *
 * A writable stream stages one envelope containing both metadata and SQLite
 * bytes; close() publishes it. Never truncate the current file in place.
 * https://fs.spec.whatwg.org/#api-filesystemfilehandle-createwritable
 * Completion is a browser storage acknowledgement, not a power-loss or
 * eviction guarantee. An ambiguous close failure is recoverable by reading
 * and hashing the head with confirmPublication(), never by replaying SQL.
 */
export class OpfsSnapshotStore {
  readonly #directory: FileSystemDirectoryHandle;
  readonly #locks: LockManager;
  readonly #lockName: string;
  readonly #name: string;
  #closed = false;

  private constructor(directory: FileSystemDirectoryHandle, locks: LockManager,
    lockName: string, name: string) {
    this.#directory = directory;
    this.#locks = locks;
    this.#lockName = lockName;
    this.#name = name;
  }

  static async open(name: string): Promise<OpfsSnapshotStore> {
    validateSnapshotName(name);
    if (typeof navigator === "undefined" || typeof navigator.storage?.getDirectory !== "function" ||
        typeof navigator.locks?.request !== "function" || !globalThis.crypto?.subtle ||
        typeof crypto.randomUUID !== "function") {
      throw new SnapshotStoreError("ERR_FSQLITE_SNAPSHOT_UNAVAILABLE",
        "OPFS, Web Locks and secure-context Web Crypto are required for OPFS checkpoints");
    }
    // JSON preserves lone UTF-16 surrogates too. Encoding name directly would
    // map distinct invalid surrogate sequences to the same replacement byte.
    const key = await checksum(new TextEncoder().encode(JSON.stringify(name)));
    const root = await navigator.storage.getDirectory();
    const snapshots = await root.getDirectoryHandle(ROOT, { create: true });
    const directory = await snapshots.getDirectoryHandle(key, { create: true });
    return new OpfsSnapshotStore(directory, navigator.locks,
      `frankensqlite:opfs-snapshot:v1:${key}`, name);
  }

  async load(): Promise<StoredSnapshot | null> {
    return this.#locked(() => this.#read());
  }

  /** null is create-only; a rejected stale writer retains its local SQL data. */
  async save(bytes: Uint8Array, expectedRevision: string | null,
    publicationId?: string): Promise<SnapshotMetadata> {
    this.#assertOpen();
    validateIdentity(expectedRevision, publicationId);
    validateSnapshotBytes(bytes);
    // Capture before the first await, including exact view bounds. Caller
    // mutation during hashing or lock acquisition cannot change publication.
    const owned = new Uint8Array(bytes);
    validateSnapshotBytes(owned);
    const envelope: Envelope = {
      format: 1, name: this.#name, revision: publicationId ?? crypto.randomUUID(),
      parentRevision: expectedRevision, byteLength: owned.byteLength,
      sha256: await checksum(owned),
    };
    const header = new TextEncoder().encode(JSON.stringify(envelope));
    if (header.byteLength > MAX_HEADER_BYTES) throw corrupt("OPFS snapshot header is too large");
    const prefix = new Uint8Array(PREFIX_BYTES);
    prefix.set(MAGIC);
    new DataView(prefix.buffer).setUint32(MAGIC.length, header.byteLength);

    return this.#locked(async () => {
      // Hash the authoritative head, not just its revision, before allowing
      // replacement. A corrupt checkpoint must not silently become new data.
      const previous = await this.#read();
      const actual = previous?.revision ?? null;
      if (actual !== expectedRevision) {
        throw new SnapshotStoreError("ERR_FSQLITE_SNAPSHOT_CONFLICT",
          `Snapshot changed: expected ${expectedRevision ?? "no snapshot"}, found ${actual ?? "no snapshot"}. Reopen and merge; do not blindly retry.`);
      }
      const file = await this.#directory.getFileHandle(HEAD, { create: true });
      let stream: FileSystemWritableFileStream | undefined;
      try {
        if (typeof file.createWritable !== "function") {
          throw new SnapshotStoreError("ERR_FSQLITE_SNAPSHOT_UNAVAILABLE",
            "This browser does not support writable OPFS snapshot streams");
        }
        stream = await file.createWritable({ keepExistingData: false });
        await stream.write(prefix);
        await stream.write(header);
        await stream.write(owned);
        await stream.close();
      } catch (cause: unknown) {
        // Never restore the previous head: close may have published before
        // its acknowledgement was lost. Confirmation is read-only instead.
        const cleanupErrors: unknown[] = [];
        if (stream !== undefined) {
          try { await stream.abort(); }
          catch (error: unknown) { cleanupErrors.push(error); }
        }
        // getFileHandle(create) may have created an empty file before failure.
        // Remove ONLY that new, still-empty entry under the publication lock.
        // A pre-existing empty/truncated head is corruption, never "absent".
        if (previous === null) {
          try {
            if ((await file.getFile()).size === 0) await this.#directory.removeEntry(HEAD);
          } catch (error: unknown) { cleanupErrors.push(error); }
        }
        if (cleanupErrors.length !== 0) {
          throw new SnapshotStoreError("ERR_FSQLITE_SNAPSHOT_IO",
            "OPFS checkpoint failed and staging cleanup was incomplete; confirm the publication before retrying",
            { cause: new AggregateError([cause, ...cleanupErrors], "OPFS checkpoint publication failed") });
        }
        throw cause;
      }
      return metadata(envelope);
    });
  }

  async confirmPublication(revision: string, parentRevision: string | null): Promise<SnapshotMetadata> {
    if (!validRevision(revision)) {
      throw new SnapshotStoreError("ERR_FSQLITE_SNAPSHOT_INPUT", "Invalid checkpoint recovery identity");
    }
    validateIdentity(parentRevision, revision);
    const saved = await this.load();
    if (saved === null || saved.revision !== revision || saved.parentRevision !== parentRevision) {
      throw new SnapshotStoreError("ERR_FSQLITE_SNAPSHOT_NOT_CONFIRMED",
        "The stored checkpoint does not confirm this publication. Export and reconcile; never replay committed SQL.");
    }
    return Object.freeze(metadata(saved));
  }

  /** Already-running operations finish; queued operations cannot start later. */
  close(): void { this.#closed = true; }

  async #locked<T>(operation: () => Promise<T>): Promise<T> {
    this.#assertOpen();
    return this.#locks.request(this.#lockName, { mode: "exclusive" }, async () => {
      this.#assertOpen();
      return operation();
    });
  }

  async #read(): Promise<StoredSnapshot | null> {
    let handle: FileSystemFileHandle;
    try { handle = await this.#directory.getFileHandle(HEAD); }
    catch (error: unknown) {
      if (error instanceof DOMException && error.name === "NotFoundError") return null;
      throw error;
    }
    const file = await handle.getFile();
    if (file.size < PREFIX_BYTES || file.size > PREFIX_BYTES + MAX_HEADER_BYTES + MAX_SNAPSHOT_BYTES) {
      throw corrupt("OPFS snapshot is truncated or exceeds the size limit");
    }
    const prefix = new Uint8Array(await file.slice(0, PREFIX_BYTES).arrayBuffer());
    if (prefix.length !== PREFIX_BYTES || MAGIC.some((byte, index) => prefix[index] !== byte)) {
      throw corrupt("Unsupported OPFS snapshot format");
    }
    const headerLength = new DataView(prefix.buffer).getUint32(MAGIC.length);
    if (headerLength === 0 || headerLength > MAX_HEADER_BYTES || PREFIX_BYTES + headerLength > file.size) {
      throw corrupt("Invalid OPFS snapshot header length");
    }
    let value: unknown;
    try {
      const header = await file.slice(PREFIX_BYTES, PREFIX_BYTES + headerLength).arrayBuffer();
      value = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(header));
    } catch (cause: unknown) { throw corrupt("Invalid OPFS snapshot metadata", cause); }
    const envelope = validateEnvelope(value, this.#name, file.size - PREFIX_BYTES - headerLength);
    const bytes = new Uint8Array(await file.slice(PREFIX_BYTES + headerLength).arrayBuffer());
    if (bytes.byteLength !== envelope.byteLength) throw corrupt("Truncated OPFS snapshot bytes");
    validateSnapshotBytes(bytes);
    if (await checksum(bytes) !== envelope.sha256) throw corrupt("OPFS snapshot SHA-256 does not match its bytes");
    return { ...metadata(envelope), bytes };
  }

  #assertOpen(): void {
    if (this.#closed) throw new SnapshotStoreError("ERR_FSQLITE_SNAPSHOT_CLOSED", "Snapshot storage is closed");
  }
}

function validRevision(value: unknown): value is string {
  return typeof value === "string" && /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(value);
}

function validateIdentity(parent: string | null, revision?: string): void {
  if ((parent !== null && !validRevision(parent)) ||
      (revision !== undefined && (!validRevision(revision) || revision === parent))) {
    throw new SnapshotStoreError("ERR_FSQLITE_SNAPSHOT_INPUT", "Invalid checkpoint publication identity");
  }
}

function validateEnvelope(value: unknown, name: string, byteLength: number): Envelope {
  if (typeof value !== "object" || value === null) throw corrupt("Invalid OPFS snapshot envelope");
  const e = value as Partial<Envelope>;
  if (e.format !== 1 || e.name !== name || !validRevision(e.revision) ||
      (e.parentRevision !== null && !validRevision(e.parentRevision)) || e.parentRevision === e.revision ||
      typeof e.sha256 !== "string" || !/^[0-9a-f]{64}$/.test(e.sha256) ||
      e.byteLength !== byteLength || byteLength > MAX_SNAPSHOT_BYTES) {
    throw corrupt("Unsupported or malformed OPFS snapshot envelope");
  }
  return e as Envelope;
}

function metadata(value: SnapshotMetadata): SnapshotMetadata {
  return { revision: value.revision, parentRevision: value.parentRevision,
    byteLength: value.byteLength, sha256: value.sha256 };
}

async function checksum(bytes: Uint8Array): Promise<string> {
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
  return [...digest].map(byte => byte.toString(16).padStart(2, "0")).join("");
}

function corrupt(message: string, cause?: unknown): SnapshotStoreError {
  return new SnapshotStoreError("ERR_FSQLITE_SNAPSHOT_CORRUPT", message, { cause });
}
