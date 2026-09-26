import type { ChangesetExecutor, ChangesetTarget } from "./changeset-apply";
import { ChangesetCaptureError, prepareSnapshotChangesetStream } from "./changeset-capture";
import type { ChangesetSnapshot, SnapshotChangesetOptions } from "./changeset-capture";
import { decodeChangeset, encodeChangeset } from "./changeset-codec";
import type { ChangesetChange, ChangesetField, ChangesetLimits, ChangesetTable, ChangesetValue } from "./changeset-codec";

/** Each before/after scan has its own image budget; output has separate codec limits. */
export type SnapshotCaptureOptions = SnapshotChangesetOptions;
export interface SnapshotCapturedChangeset<T> extends ChangesetSnapshot {
  readonly value: T;
  readonly beforeRows: number;
  readonly afterRows: number;
}
const fold = (name: string): string => name.replace(/[A-Z]/g, c => c.toLowerCase());
function fail(kind: "INPUT" | "RESULT" | "SCHEMA", message: string): never {
  throw new ChangesetCaptureError(`ERR_FSQLITE_CAPTURE_${kind}`, message);
}
function limit(value: number | undefined, fallback: number, maximum: number): number {
  const n = value === undefined ? fallback : value;
  if (!Number.isSafeInteger(n) || n < 1 || n > maximum) fail("INPUT", `Snapshot capture limit must be in 1..${maximum}`);
  return n;
}
function equal(a: ChangesetValue, b: ChangesetValue): boolean {
  if (a instanceof Uint8Array && b instanceof Uint8Array)
    return a.length === b.length && a.every((v, i) => v === b[i]);
  return typeof a === typeof b && a === b;
}
/** Tagged JSON is collision-free for the admitted SQLite key storage classes. */
function rowKey(pk: readonly number[], row: readonly ChangesetValue[]): string {
  return JSON.stringify(pk.flatMap((position, i) => {
    if (position === 0) return [];
    const value = row[i]!;
    if (value === null) fail("RESULT", "Snapshot capture cannot represent a NULL key");
    const tag = typeof value === "bigint" ? "i" : typeof value === "number" ? "r" : typeof value === "string" ? "t" : "b";
    let text = "";
    if (value instanceof Uint8Array) {
      // Avoid a per-byte JS array spanning a potentially large BLOB key.
      for (let start = 0; start < value.length; start += 4096)
        text += Array.from(value.subarray(start, start + 4096), b => b.toString(16).padStart(2, "0")).join("");
    } else text = String(value);
    return [[tag, text]];
  }));
}
interface TableImages {
  name: string;
  pk: readonly number[];
  before: Map<string, readonly ChangesetValue[]>;
  changes: ChangesetChange[];
  seen: Set<string>;
}
async function schemaVersion(tx: ChangesetExecutor, namespace: "main" | "temp"): Promise<string> {
  const { rowArrays: rows } = await tx.query(`PRAGMA ${namespace}.schema_version`);
  const value = rows?.[0]?.[0];
  if (!Array.isArray(rows) || rows.length !== 1 || rows[0]!.length !== 1 ||
      !((typeof value === "bigint" && value >= 0n && value <= 0x7fffffffn) ||
        (typeof value === "number" && Number.isSafeInteger(value) && value >= 0 && value <= 0x7fffffff)))
    fail("RESULT", "Invalid schema version during snapshot capture");
  return String(value);
}
/** A callback cannot leave admitted SQL running into collection or use its executor afterward. */
async function runWork<T>(tx: ChangesetExecutor, work: (tx: ChangesetExecutor) => T | Promise<T>, check: () => void): Promise<T> {
  let accepting = true;
  const pending = new Set<Promise<unknown>>(), failures: unknown[] = [];
  const submit = <U>(operation: () => Promise<U>): Promise<U> => {
    const task = (async () => {
      if (!accepting) fail("INPUT", "Snapshot capture callback SQL scope has ended");
      check(); const value = await operation(); check(); return value;
    })();
    pending.add(task);
    void task.then(() => pending.delete(task), error => { pending.delete(task); failures.push(error); });
    return task;
  };
  const scoped = Object.freeze({
    execute: (sql, params) => submit(() => tx.execute(sql, params)),
    query: (sql, params) => submit(() => tx.query(sql, params)),
  } satisfies ChangesetExecutor);
  let value: T;
  try { value = await work(scoped); }
  finally { accepting = false; await Promise.allSettled(pending); }
  if (failures.length) throw failures[0];
  check(); return value;
}

/**
 * Opt-in full-scope before/after capture for tables with application triggers,
 * view-driven writes and foreign-key effects. No observation triggers are added.
 * Callback DML and output validation share one owned transaction/savepoint.
 * This is net-state capture, NOT a touched-row journal or per-operation audit.
 */
export async function captureSnapshotChangeset<T>(
  target: ChangesetTarget, work: (tx: ChangesetExecutor) => T | Promise<T>, options: SnapshotCaptureOptions,
): Promise<SnapshotCapturedChangeset<T>> {
  const capture = prepareSnapshotCapture(work, options);
  return target.transaction(capture.run, capture.transactionOptions);
}

/** @internal Outbox publication runs collection inside the SAME source transaction. */
export function prepareSnapshotCapture<T>(
  work: (tx: ChangesetExecutor) => T | Promise<T>, options: SnapshotCaptureOptions,
) {
  if (typeof work !== "function" || typeof options !== "object" || options === null)
    fail("INPUT", "Snapshot capture requires a callback and options");
  const maxRows = limit(options?.maxRows, 10_000, 100_000);
  const maxBytes = limit(options?.maxBytes, 8 * 1024 * 1024, 64 * 1024 * 1024);
  const maxCells = limit(options?.maxCells, 100_000, 1_000_000);
  const limits: ChangesetLimits = {};
  for (const k of ["maxBytes", "maxTables", "maxColumns", "maxChanges", "maxCells"] as const) {
    const value = options.limits?.[k]; if (value !== undefined) limits[k] = value;
  }
  encodeChangeset([], limits);
  // The existing reader captures/validates table names, flags and controls before
  // any await. Reusing ONE prepared reader keeps the same monotonic deadline.
  const stream = prepareSnapshotChangesetStream({ ...options, maxRows, maxBytes, maxCells,
    chunkRows: 32, chunkBytes: maxBytes, maxChunks: 100_000 });
  return { transactionOptions: stream.transactionOptions, checkpoint: stream.checkpoint,
    tables: stream.tables, indirect: stream.indirect,
    run: async (tx: ChangesetExecutor): Promise<SnapshotCapturedChangeset<T>> => {
      stream.checkpoint();
      const mainVersion = await schemaVersion(tx, "main"), tempVersion = await schemaVersion(tx, "temp");
      const images = new Map<string, TableImages>();
      function table(t: ChangesetTable): TableImages {
        const key = fold(t.name); let item = images.get(key);
        if (item === undefined) {
          item = { name: t.name, pk: t.primaryKey, before: new Map(), changes: [], seen: new Set() }; images.set(key, item);
        } else if (JSON.stringify(item.pk) !== JSON.stringify(t.primaryKey)) fail("SCHEMA", "Captured table shape changed");
        return item;
      }
      const before = await stream.run(tx, chunk => {
        for (const t of decodeChangeset(chunk.changeset)) {
          const item = table(t);
          for (const change of t.changes) {
            if (change.operation !== "insert") fail("RESULT", "Snapshot reader returned a non-INSERT");
            const key = rowKey(item.pk, change.new);
            if (item.before.has(key)) fail("RESULT", "Snapshot reader repeated a key");
            item.before.set(key, change.new);
          }
        }
      });
      const value = await runWork(tx, work, stream.checkpoint);
      const checkSchema = async () => {
        stream.checkpoint();
        if (await schemaVersion(tx, "main") !== mainVersion || await schemaVersion(tx, "temp") !== tempVersion)
          fail("SCHEMA", "Snapshot capture callbacks must not change schemas");
        stream.checkpoint();
      };
      await checkSchema();
      const after = await stream.run(tx, chunk => {
        for (const t of decodeChangeset(chunk.changeset)) {
          const item = table(t);
          for (const change of t.changes) {
            if (change.operation !== "insert") fail("RESULT", "Snapshot reader returned a non-INSERT");
            const key = rowKey(item.pk, change.new), old = item.before.get(key);
            if (item.seen.has(key)) fail("RESULT", "Snapshot reader repeated an after-image key");
            item.seen.add(key); item.before.delete(key);
            if (old === undefined) { item.changes.push({ ...change, indirect: stream.indirect }); continue; }
            const previous: ChangesetField[] = [], next: ChangesetField[] = [];
            let modified = false;
            for (let i = 0; i < item.pk.length; i++) {
              const different = item.pk[i] === 0 && !equal(old[i]!, change.new[i]!);
              previous.push(item.pk[i] !== 0 || different ? old[i] : undefined);
              next.push(different ? change.new[i] : undefined); modified ||= different;
            }
            if (modified) item.changes.push({ operation: "update", indirect: stream.indirect, old: previous, new: next });
          }
        }
      });
      await checkSchema();
      const tables: ChangesetTable[] = [];
      for (const requested of stream.tables) {
        const item = images.get(fold(requested)); if (item === undefined) continue;
        const deletions: ChangesetChange[] = Array.from(item.before.values(), old =>
          ({ operation: "delete", indirect: stream.indirect, old }));
        // Vacate deleted/rekeyed rows before inserting replacement identities.
        const changes = [...deletions, ...item.changes];
        if (changes.length) tables.push({ name: item.name, primaryKey: item.pk, changes });
      }
      stream.checkpoint(); const changeset = encodeChangeset(tables, limits); stream.checkpoint();
      return Object.freeze({ value, changeset, beforeRows: before.changes, afterRows: after.changes,
        changes: tables.reduce((n, t) => n + t.changes.length, 0) });
    },
  };
}
