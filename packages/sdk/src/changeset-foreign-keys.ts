import type { ChangesetExecutor, ChangesetTarget } from "./changeset-apply";

export class ChangesetForeignKeyError extends Error {
  constructor(
    readonly code:
      | "ERR_FSQLITE_FOREIGN_KEY_INPUT"
      | "ERR_FSQLITE_FOREIGN_KEY_BUSY"
      | "ERR_FSQLITE_FOREIGN_KEY_STATE"
      | "ERR_FSQLITE_FOREIGN_KEY_VIOLATION"
      | "ERR_FSQLITE_FOREIGN_KEY_CANCELLED"
      | "ERR_FSQLITE_FOREIGN_KEY_TIMEOUT",
    message: string,
    options?: ErrorOptions,
    /** Restoration failures mean the connection must be reconciled before reuse. */
    readonly cleanupErrors: readonly unknown[] = [],
  ) {
    super(message, options);
    this.name = "ChangesetForeignKeyError";
  }
}

type Controls = { signal?: AbortSignal; timeoutMs?: number };
const MAX_SCHEMAS = 127;
const active = new WeakSet<object>();
const wrappers = new WeakSet<object>();
const fold = (name: string): string => name.replace(/[A-Z]/g, c => c.toLowerCase());
function fail(kind: "INPUT" | "STATE" | "VIOLATION", message: string): never {
  throw new ChangesetForeignKeyError(`ERR_FSQLITE_FOREIGN_KEY_${kind}`, message);
}
function budget(options: Controls) {
  const signal = options.signal, timeoutMs = options.timeoutMs;
  if (signal !== undefined) {
    try { Object.getOwnPropertyDescriptor(AbortSignal.prototype, "aborted")!.get!.call(signal); }
    catch { fail("INPUT", "signal must be an AbortSignal"); }
  }
  if (timeoutMs !== undefined &&
      (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 2_147_483_647))
    fail("INPUT", "timeoutMs must be an integer in 1..2147483647");
  const deadline = timeoutMs === undefined ? undefined : performance.now() + timeoutMs;
  const checkpoint = (): void => {
    if (signal?.aborted) throw new ChangesetForeignKeyError(
      "ERR_FSQLITE_FOREIGN_KEY_CANCELLED", "Deferred foreign-key scope cancelled", { cause: signal.reason });
    if (deadline !== undefined && performance.now() >= deadline) throw new ChangesetForeignKeyError(
      "ERR_FSQLITE_FOREIGN_KEY_TIMEOUT", "Deferred foreign-key scope deadline expired");
  };
  const controls: Controls = {};
  if (signal !== undefined) controls.signal = signal;
  if (timeoutMs !== undefined) controls.timeoutMs = timeoutMs;
  checkpoint();
  return { checkpoint, controls };
}
async function flag(tx: ChangesetExecutor, name: "foreign_keys" | "defer_foreign_keys"): Promise<0 | 1> {
  const result = await tx.query(`PRAGMA ${name}`);
  const rows = result.rowArrays;
  if (!Array.isArray(rows) || rows.length !== 1 || !Array.isArray(rows[0]) || rows[0].length !== 1)
    fail("STATE", `The SQL target did not acknowledge PRAGMA ${name}`);
  const value = rows[0][0];
  if (value === 0 || value === 0n) return 0;
  if (value === 1 || value === 1n) return 1;
  return fail("STATE", `Invalid PRAGMA ${name} result`);
}
async function schemas(tx: ChangesetExecutor): Promise<readonly string[]> {
  // Bound returned metadata, and never retrieve attached database filenames.
  const result = await tx.query(`SELECT name FROM pragma_database_list() LIMIT ${MAX_SCHEMAS + 1}`);
  const rows = result.rowArrays;
  if (!Array.isArray(rows) || rows.length === 0 || rows.length > MAX_SCHEMAS)
    fail("STATE", "Invalid or oversized database schema list");
  const names = new Map<string, string>();
  for (const row of rows) {
    if (!Array.isArray(row) || row.length !== 1)
      fail("STATE", "Invalid database schema row");
    const name = row[0];
    if (typeof name !== "string" ||
        !name.length || name.length > 1024 || name.includes("\0") || names.has(fold(name)))
      fail("STATE", "Invalid or repeated database schema name");
    names.set(fold(name), name);
  }
  if (names.get("main") !== "main" || (names.has("temp") && names.get("temp") !== "temp"))
    fail("STATE", "Missing canonical main/TEMP database binding");
  // Reading TEMP can initialize its empty schema. Always include it, even when
  // database_list did not list it yet, so this does not look like an ATTACH.
  names.set("temp", "temp");
  return Object.freeze([...names.values()].sort());
}
async function clean(tx: ChangesetExecutor, names: readonly string[], checkpoint: () => void): Promise<void> {
  for (const name of names) {
    checkpoint();
    // Deferral is connection-wide: checking only changed/main tables could
    // erase evidence of a caller's unresolved TEMP or attached-schema writes.
    // LIMIT bounds returned violations, not the engine's scan work or RSS.
    const { rowArrays: rows } = await tx.query(
      "SELECT 1 FROM pragma_foreign_key_check(NULL, ?) LIMIT 1", [name]);
    checkpoint();
    if (!Array.isArray(rows) || rows.length > 1 ||
        rows.some(row => !Array.isArray(row) || row.length !== 1 || (row[0] !== 1 && row[0] !== 1n)))
      fail("STATE", "Invalid foreign-key validation result");
    if (rows.length) fail("VIOLATION", "Deferred application requires a foreign-key-clean entry and exit state");
  }
}

/** Drain admitted SQL and reject retained executors after the callback ends. */
async function runWork<T>(
  tx: ChangesetExecutor, work: (tx: ChangesetExecutor) => Promise<T>, checkpoint: () => void,
): Promise<T> {
  let accepting = true;
  const pending = new Set<Promise<unknown>>(), failures: unknown[] = [];
  const submit = <U>(operation: () => Promise<U>): Promise<U> => {
    if (!accepting) return Promise.reject(new ChangesetForeignKeyError(
      "ERR_FSQLITE_FOREIGN_KEY_STATE", "Deferred foreign-key SQL scope has ended"));
    const promise = (async () => {
      checkpoint();
      const result = await operation();
      checkpoint();
      return result;
    })();
    pending.add(promise);
    void promise.then(() => pending.delete(promise), error => {
      pending.delete(promise); failures.push(error);
    });
    return promise;
  };
  const scoped = Object.freeze({
    execute: (sql, params) => submit(() => tx.execute(sql, params)),
    query: (sql, params) => submit(() => tx.query(sql, params)),
  } satisfies ChangesetExecutor);
  let value: T;
  try { value = await work(scoped); }
  finally { accepting = false; await Promise.allSettled(pending); }
  if (failures.length) throw failures[0];
  checkpoint();
  return value;
}

/**
 * Opt-in FK deferral for one real owned transaction/savepoint. Wrap the target
 * passed to applyChangeset, applyPatchset, a rebase journal, or ChangesetOrder.
 * Application rows, inbox/journal/order metadata and the final FK check then
 * share the SAME transaction; no extra BEGIN, COMMIT or writer lock is added.
 *
 * Requires foreign_keys=ON and a clean entry state in EVERY attached schema.
 * Pre-existing deferred violations are rejected without changing their pragma
 * or counters. Remaining violations abort, never become an omission decision.
 * Callbacks must not change connection pragmas, attach/detach schemas or run
 * transaction-control SQL. This is ownership composition, not a SQL sandbox.
 *
 * The target must roll back on rejection, and must not allow unrelated SQL
 * during its callback. Cleanup is attempted even after cancellation. A STATE
 * error with cleanupErrors means restoration could not be established: drain
 * rollback and reconcile/discard the connection before reuse. No retry occurs.
 */
export function withDeferredForeignKeys(target: ChangesetTarget): ChangesetTarget {
  if ((typeof target !== "object" && typeof target !== "function") || target === null ||
      typeof target.transaction !== "function") fail("INPUT", "An owned SQL transaction target is required");
  if (wrappers.has(target)) return target;
  const transaction = target.transaction;
  const wrapped: ChangesetTarget = Object.freeze({
    transaction: async <T>(work: (tx: ChangesetExecutor) => Promise<T>, options: Controls = {}): Promise<T> => {
      if (typeof work !== "function") fail("INPUT", "A transaction callback is required");
      const time = budget(options);
      if (active.has(target)) throw new ChangesetForeignKeyError(
        "ERR_FSQLITE_FOREIGN_KEY_BUSY", "This deferred target is already active; nothing was queued");
      active.add(target);
      try {
        return await transaction.call(target, async (tx: ChangesetExecutor) => {
          time.checkpoint();
          if (await flag(tx, "foreign_keys") !== 1)
            fail("STATE", "Enable PRAGMA foreign_keys=ON before deferred application");
          const previous = await flag(tx, "defer_foreign_keys");
          const before = await schemas(tx);
          await clean(tx, before, time.checkpoint);
          let failed = false, failure: unknown;
          try {
            time.checkpoint();
            if (previous === 0) await tx.execute("PRAGMA defer_foreign_keys=ON");
            if (await flag(tx, "defer_foreign_keys") !== 1)
              fail("STATE", "The SQL target did not enable foreign-key deferral");
            const value = await runWork(tx, work, time.checkpoint);
            if (await flag(tx, "foreign_keys") !== 1 || await flag(tx, "defer_foreign_keys") !== 1)
              fail("STATE", "Application changed foreign-key enforcement pragmas");
            const after = await schemas(tx);
            if (JSON.stringify(after) !== JSON.stringify(before))
              fail("STATE", "Application changed attached database bindings");
            await clean(tx, after, time.checkpoint);
            return value;
          } catch (error: unknown) { failed = true; failure = error; throw error; }
          finally {
            // Do not checkpoint here: cancellation must not bypass restoration.
            // Turning deferral OFF clears SQLite's deferred-immediate counter.
            // On success all schemas are clean; on failure a real target MUST
            // roll back this scope to the validated clean entry state.
            try {
              await tx.execute(`PRAGMA defer_foreign_keys=${previous === 1 ? "ON" : "OFF"}`);
              if (await flag(tx, "defer_foreign_keys") !== previous)
                fail("STATE", "The SQL target did not restore foreign-key deferral");
            } catch (cleanup: unknown) {
              throw new ChangesetForeignKeyError("ERR_FSQLITE_FOREIGN_KEY_STATE",
                "Foreign-key state restoration failed; reconcile or discard this connection before reuse",
                { cause: failed ? failure : cleanup }, Object.freeze([cleanup]));
            }
          }
        }, time.controls) as T;
      } finally { active.delete(target); }
    },
  });
  wrappers.add(wrapped);
  return wrapped;
}
