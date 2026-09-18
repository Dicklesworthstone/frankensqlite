import { FrankenPreparedStatement } from "./statement";
import { captureTransactionOptions, combineTransactionSignals, FrankenTransaction, TransactionBudget } from "./transaction";
import type { ExecuteManyOptions, ExecuteManyResult, FrankenDbOpenOptions, PersistenceMode, QueryResult, SqlScalar, SqlBindings, SnapshotMetadata } from "./types";
import { normalizeOpenOptions, resolveWorker } from "./utils";
import { FrankenWorkerClient } from "./worker-client";
import { FrankenSQLiteError } from "./errors";
import { checkStreamCancellation, executeRowStream, streamOptions } from "./stream";
import type { ExecuteStreamOptions, ExecuteStreamResult, SqlRowSource } from "./types";
import { resolveRequestLimits, resolveResultEncoding, resolvePreparedStatementLimits } from "@frankensqlite/worker";
import type { PreparedStatementLimits } from "@frankensqlite/worker";
import type { RequestQueueStats } from "./types";
import type { TransactionOptions } from "./types";
import { isTransactionConflict, resolveTransactionRetryOptions, runTransactionRetry } from "./transaction-retry";
import type { RetryRecovery, TransactionRetryAttempt, TransactionRetryOptions } from "./transaction-retry";

const databaseClients = new WeakMap<FrankenDB, FrankenWorkerClient>();

function snapshotReceiptFailure(cause: unknown): FrankenSQLiteError {
  const error = new FrankenSQLiteError({ code: "ERR_FSQLITE_SNAPSHOT_RECEIPT",
    message: "Snapshot acknowledgement is invalid or its revision lineage is unknown",
    transient: false, userRecoverable: false,
    suggestion: "Publication may have completed. Export the live image, reopen the authoritative snapshot and reconcile; do not replay committed SQL or blindly publish again." });
  error.cause = cause;
  return error;
}

/** Wire metadata must be own data fields, not callbacks or inherited claims. */
function snapshotDataField(record: object, key: string, required = true): unknown {
  const descriptor = Object.getOwnPropertyDescriptor(record, key);
  if (descriptor === undefined && !required) return undefined;
  if (descriptor === undefined || !Object.hasOwn(descriptor, "value")) {
    throw new TypeError(`Invalid snapshot metadata field: ${key}`);
  }
  return descriptor.value;
}

/** Validate a receipt, not the image: the worker/store still owns byte hashing. */
function captureSnapshotReceipt(value: unknown): SnapshotMetadata {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new TypeError("Invalid snapshot acknowledgement");
  }
  const revision = snapshotDataField(value, "revision");
  const parentRevision = snapshotDataField(value, "parentRevision");
  const byteLength = snapshotDataField(value, "byteLength");
  const sha256 = snapshotDataField(value, "sha256");
  const validRevision = (token: unknown): token is string => typeof token === "string" &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(token);
  if (!validRevision(revision) || (parentRevision !== null && !validRevision(parentRevision)) ||
      revision === parentRevision || typeof sha256 !== "string" || !/^[0-9a-f]{64}$/.test(sha256) ||
      typeof byteLength !== "number" || !Number.isSafeInteger(byteLength) ||
      byteLength < 512 || byteLength > 64 * 1024 * 1024 || byteLength % 512 !== 0) {
    throw new TypeError("Malformed snapshot acknowledgement metadata");
  }
  return Object.freeze({ revision, parentRevision, byteLength, sha256 });
}

/** A requested policy must be acknowledged, not silently ignored by an old worker. */
function captureStatementPolicy(value: unknown, requested: Readonly<PreparedStatementLimits> | undefined):
  Readonly<PreparedStatementLimits> | null {
  if (value === undefined && requested === undefined) return null;
  const reject = (): never => {
    throw new FrankenSQLiteError({ code: "ERR_FSQLITE_STATEMENT_POLICY", transient: false,
      message: "The worker did not acknowledge a valid prepared-statement policy within the requested limits",
      suggestion: "Use a worker that supports preparedStatementLimits; no SQL has been submitted by this database handle." });
  };
  if (typeof value !== "object" || value === null || Array.isArray(value)) return reject();
  const count = Object.getOwnPropertyDescriptor(value, "maxStatements");
  const bytes = Object.getOwnPropertyDescriptor(value, "maxBytes");
  if (count === undefined || bytes === undefined || !Object.hasOwn(count, "value") || !Object.hasOwn(bytes, "value")) return reject();
  let effective: Readonly<PreparedStatementLimits>;
  try { effective = resolvePreparedStatementLimits({ maxStatements: count.value, maxBytes: bytes.value }); }
  catch { return reject(); }
  // Missing values must not become locally supplied defaults in an acknowledgement.
  if (count.value !== effective.maxStatements || bytes.value !== effective.maxBytes ||
      (requested !== undefined && (effective.maxStatements > requested.maxStatements || effective.maxBytes > requested.maxBytes))) return reject();
  return effective;
}

/** Internal lifecycle subscription for owners of a private connection. */
export function observeDatabaseFailure(db: FrankenDB, listener: (error: Error) => void): () => void {
  const client = databaseClients.get(db);
  if (client === undefined) throw new TypeError("A FrankenDB connection is required");
  return client.observeFailure(listener);
}

interface TransactionScope {
  readonly id: string;
  readonly signal: AbortSignal;
  readonly budget: TransactionBudget;
  cancellationError: FrankenSQLiteError | null;
  accepting: boolean;
  pending: Set<Promise<unknown>>;
  statements: Set<string>;
  errors: unknown[];
  children: Set<Promise<unknown>>;
  cleanupFailed: boolean;
}

export class FrankenDB {
  readonly #client: FrankenWorkerClient;
  readonly #path: string;
  readonly #persistence: PersistenceMode;
  readonly #preparedStatementLimits: Readonly<PreparedStatementLimits> | null;
  #snapshotRevision: string | null;
  #snapshotReceiptFailure: FrankenSQLiteError | null = null;
  #transactionScope: TransactionScope | null = null;
  #transactionFailure: Error | null = null;
  #nextTransactionId = 1n;
  #retryOwner: object | null = null;

  private constructor(client: FrankenWorkerClient, path: string, persistence: PersistenceMode, snapshotRevision: string | null,
    preparedStatementLimits: Readonly<PreparedStatementLimits> | null) {
    this.#client = client;
    databaseClients.set(this, client);
    this.#path = path;
    this.#persistence = persistence;
    this.#snapshotRevision = snapshotRevision;
    this.#preparedStatementLimits = preparedStatementLimits;
  }

  static async open(options?: FrankenDbOpenOptions | string): Promise<FrankenDB> {
    const normalized = normalizeOpenOptions(options);
    // Validate before allocating a worker or transferring a snapshot buffer.
    const limits = resolveRequestLimits(normalized.requestLimits);
    const resultEncoding = resolveResultEncoding(normalized.resultEncoding);
    const requestedStatementLimits = normalized.preparedStatementLimits;
    const statementLimits = requestedStatementLimits === undefined
      ? undefined : resolvePreparedStatementLimits(requestedStatementLimits);
    const client = new FrankenWorkerClient(resolveWorker(normalized.worker), limits);
    const config: FrankenDbOpenOptions = {};
    if (statementLimits !== undefined) config.preparedStatementLimits = statementLimits;
    if (normalized.resultEncoding !== undefined) config.resultEncoding = resultEncoding;
    if (normalized.dbName !== undefined) {
      config.dbName = normalized.dbName;
    }
    if (normalized.persistence !== undefined) {
      config.persistence = normalized.persistence;
    }
    if (normalized.wasmUrl !== undefined) {
      config.wasmUrl = normalized.wasmUrl;
    }
    if (normalized.snapshot !== undefined) {
      config.snapshot = normalized.snapshot;
    }
    try {
      const ready = await client.init(config);
      let snapshot: SnapshotMetadata | null = null;
      try {
        const saved = snapshotDataField(ready, "snapshot", false);
        if (saved !== undefined && saved !== null) {
          if (ready.persistence !== "indexeddb-snapshot") {
            throw new TypeError("Only snapshot persistence can return saved snapshot metadata");
          }
          snapshot = captureSnapshotReceipt(saved);
        }
      } catch (cause: unknown) { throw snapshotReceiptFailure(cause); }
      const policy = Object.getOwnPropertyDescriptor(ready, "preparedStatementLimits");
      if (policy !== undefined && !Object.hasOwn(policy, "value")) {
        throw new FrankenSQLiteError({ code: "ERR_FSQLITE_STATEMENT_POLICY", message: "Invalid worker prepared-statement policy" });
      }
      const effectiveStatements = captureStatementPolicy(policy?.value, statementLimits);
      return new FrankenDB(client, ready.path, ready.persistence, snapshot?.revision ?? null, effectiveStatements);
    } catch (error: unknown) {
      try {
        client.dispose();
      } catch (cleanupError: unknown) {
        throw new AggregateError([error, cleanupError],
          "FrankenSQLite initialization and worker cleanup both failed", { cause: error });
      }
      throw error;
    }
  }

  static import(
    snapshot: Uint8Array,
    options?: Omit<FrankenDbOpenOptions, "snapshot">,
  ): Promise<FrankenDB> {
    return FrankenDB.open({
      ...options,
      snapshot,
    });
  }

  get path(): string {
    return this.#path;
  }

  get persistence(): PersistenceMode {
    return this.#persistence;
  }

  /** Acknowledged worker limits; null means an older worker supplied no policy. */
  get preparedStatementLimits(): Readonly<PreparedStatementLimits> | null { return this.#preparedStatementLimits; }

  /** Effective worker policy; individual noncanonical results may still fall back. */
  get resultEncoding() {
    return this.#client.resultEncoding;
  }

  /** Last loaded/published checkpoint, not the state of unsaved memory writes. */
  get snapshotRevision(): string | null {
    return this.#snapshotRevision;
  }

  /** Ordinary requests only; close/cancellation have a separate control lane. */
  get requestQueue(): RequestQueueStats {
    return this.#client.requestQueue;
  }

  execute(sql: string, params: SqlBindings = []): Promise<number> {
    return this.#run(null, () => this.#client.execute(sql, params));
  }

  executeBatch(sql: string): Promise<void> {
    return this.#run(null, () => this.#client.executeBatch(sql));
  }

  /** Execute one prepared DML statement for every parameter set, atomically. */
  executeMany(
    sql: string,
    parameterSets: readonly (readonly SqlScalar[])[],
    options?: ExecuteManyOptions,
  ): Promise<ExecuteManyResult> {
    return this.#run(null, () => this.#client.executeMany(sql, parameterSets, options));
  }

  /** Consume a bounded row stream in one transaction, not one commit per batch. */
  executeStream(
    sql: string,
    rows: SqlRowSource,
    options?: ExecuteStreamOptions,
  ): Promise<ExecuteStreamResult> {
    return this.#streamTransaction(null, sql, rows, options);
  }

  #streamTransaction(
    parent: TransactionScope | null,
    sql: string,
    rows: SqlRowSource,
    options?: ExecuteStreamOptions,
  ): Promise<ExecuteStreamResult> {
    try {
      this.#assertOwner(parent);
      const config = streamOptions(sql, options);
      // No user transaction handle escapes this callback. The stream owns and
      // awaits its prepare, chunks, source cleanup and finalize itself, so SQL
      // errors retain their global stream index instead of being double-wrapped
      // by per-operation callback bookkeeping.
      const consume = (_tx: FrankenTransaction, scope: TransactionScope) => {
        const signal = combineTransactionSignals(scope.signal, config.signal)!;
        return executeRowStream({
          prepare: (statementSql) => this.#client.prepare(statementSql, scope.id),
          executePreparedMany: (id, values, settings) => this.#client.executePreparedMany(id, values, settings, scope.id),
          finalizePrepared: (id) => this.#client.finalizePrepared(id, scope.id),
        }, sql, rows, { ...config, signal });
      };
      const beforeCommit = () => checkStreamCancellation(config);
      return parent === null
        ? this.#transaction(null, consume, beforeCommit)
        : this.#nestedTransaction(parent, consume, beforeCommit);
    } catch (error: unknown) {
      return Promise.reject(error);
    }
  }

  query<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params: SqlBindings = [],
  ): Promise<QueryResult<Row>> {
    return this.#run(null, () => this.#client.query<Row>(sql, params));
  }

  prepare<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
  ): Promise<FrankenPreparedStatement<Row>> {
    return this.#run(null, () => this.#prepare<Row>(sql, null));
  }

  async #prepare<Row extends Record<string, unknown>>(
    sql: string,
    scope: TransactionScope | null,
  ): Promise<FrankenPreparedStatement<Row>> {
    const metadata = await this.#client.prepare(sql, scope?.id);
    scope?.statements.add(metadata.statementId);
    return new FrankenPreparedStatement<Row>(
      this.#client,
      metadata.statementId,
      metadata.sql,
      metadata.columnCount,
      metadata.columnNames,
      (operation) => this.#run(scope, operation),
      (failed) => {
        scope?.statements.delete(metadata.statementId);
        if (scope !== null && failed) scope.cleanupFailed = true;
      },
      scope?.id,
      scope?.signal,
    );
  }

  export(): Promise<Uint8Array> {
    return this.#run(null, () => this.#client.export());
  }

  /** Publish an explicit whole-image checkpoint after all SQL transactions end. */
  checkpoint(): Promise<SnapshotMetadata> {
    return this.#run(null, async () => {
      if (this.#snapshotReceiptFailure !== null) throw this.#snapshotReceiptFailure;
      let response: unknown;
      try { response = await this.#client.checkpoint(); }
      catch (cause: unknown) {
        // The transport can reject a correlated malformed response before it
        // reaches metadata capture. That is not a recoverable quota/CAS error.
        if (cause instanceof FrankenSQLiteError && cause.code === "ERR_FSQLITE_WORKER_RESPONSE") {
          this.#snapshotReceiptFailure ??= snapshotReceiptFailure(cause);
          throw this.#snapshotReceiptFailure;
        }
        throw cause;
      }
      // A different in-flight acknowledgement may already have lost lineage.
      // A late response cannot erase that uncertainty or regress the revision.
      if (this.#snapshotReceiptFailure !== null) throw this.#snapshotReceiptFailure;
      try {
        const saved = captureSnapshotReceipt(response);
        // Compare at acceptance, not dispatch: normal FIFO checkpoints may be
        // queued together and form a chain as each response is acknowledged.
        if (saved.parentRevision !== this.#snapshotRevision) {
          throw new TypeError("Snapshot acknowledgement does not extend the last acknowledged revision");
        }
        this.#snapshotRevision = saved.revision;
        return saved;
      } catch (cause: unknown) {
        this.#snapshotReceiptFailure ??= snapshotReceiptFailure(cause);
        throw this.#snapshotReceiptFailure;
      }
    });
  }

  transaction<T>(
    work: (tx: FrankenTransaction) => T | Promise<T>,
    options?: TransactionOptions,
  ): Promise<T> {
    // Never expose the internal scope/capability as a second callback argument.
    return this.#transaction(null, tx => work(tx), undefined, options);
  }

  /**
   * Opt-in whole-transaction replay for confirmed SQLite BUSY conflicts.
   * Each callback runs in a fresh transaction after the preceding rollback.
   * Recreate input streams in the callback and keep external effects idempotent.
   * This connection stays exclusively owned during backoff as well as SQL.
   */
  async transactionWithRetry<T>(
    work: (tx: FrankenTransaction, attempt: TransactionRetryAttempt) => T | Promise<T>,
    options?: TransactionRetryOptions,
  ): Promise<T> {
    this.#assertOwner(null);
    if (typeof work !== "function") throw new TypeError("Transaction work must be a function");
    const config = resolveTransactionRetryOptions(options);
    // Options/getters may re-enter the database; never capture stale authority.
    this.#assertOwner(null);
    const owner = {};
    this.#retryOwner = owner;
    try {
      return await runTransactionRetry((signal, attempt, recovery) =>
        this.#transaction(null, tx => work(tx, attempt), undefined, { signal }, { owner, recovery }), config);
    } finally {
      this.#retryOwner = null;
    }
  }

  async #transaction<T>(
    parent: TransactionScope | null,
    work: (tx: FrankenTransaction, scope: TransactionScope) => T | Promise<T>,
    beforeCommit?: () => void,
    options?: TransactionOptions,
    retry?: { owner: object; recovery: RetryRecovery },
  ): Promise<T> {
    this.#assertOwner(parent, retry?.owner);
    const policy = captureTransactionOptions(options);
    // Reading caller options can re-enter this connection. Check again before
    // capturing authority; a child always inherits its parent's abort signal.
    this.#assertOwner(parent, retry?.owner);
    const budget = new TransactionBudget(parent?.budget, policy, retry?.recovery.checkpoint);
    const signal = budget.signal;
    const scope: TransactionScope = {
      id: String(this.#nextTransactionId++),
      signal, budget, cancellationError: null,
      accepting: true, pending: new Set(), statements: new Set(), errors: [],
      children: new Set(),
      cleanupFailed: false,
    };
    // Claim before the first await so foreign operations cannot enter between
    // BEGIN and the callback, or while the callback awaits application work.
    this.#transactionScope = scope;
    let began = false;
    let beginDispatched = false;
    let settling = false;
    let cancelSent = false;
    const cancel = (): void => {
      if (began && !settling && !cancelSent) {
        cancelSent = true;
        this.#client.cancelTransaction(scope.id);
      }
    };
    try {
      this.#checkCancellation(scope);
      signal.addEventListener("abort", cancel, { once: true });
      beginDispatched = true;
      await this.#client.transaction("begin", scope.id, parent?.id);
      began = true;
      // An abort during BEGIN must wait for its actual result before rollback.
      // Do not race the callback against abort: escaped callback work must drain.
      this.#checkCancellation(scope);
      const result = await this.#finishScope(scope, work);
      // Last synchronous cancellation boundary, after all source/handle cleanup.
      // Once COMMIT/RELEASE dispatch starts, its actual outcome is authoritative.
      beforeCommit?.();
      this.#checkCancellation(scope);
      settling = true;
      await this.#client.transaction("commit", scope.id);
      return result;
    } catch (error: unknown) {
      settling = true;
      // A failed BEGIN does not authorize rolling back an existing transaction.
      if (began && this.#transactionFailure === null) {
        try {
          await this.#client.transaction("rollback", scope.id);
        } catch (rollbackError: unknown) {
          const failure = new AggregateError([error, rollbackError],
            "FrankenSQLite transaction and rollback both failed", { cause: error });
          this.#transactionFailure = failure;
          // The connection's transactional state is now unknown. Never allow
          // the next caller to accidentally commit the failed callback's work.
          try {
            this.#client.dispose(failure);
          } catch (cleanupError: unknown) {
            const cleanupFailure = new AggregateError([error, rollbackError, cleanupError],
              "FrankenSQLite transaction, rollback and cleanup failed", { cause: error });
            this.#transactionFailure = cleanupFailure;
            throw cleanupFailure;
          }
          throw failure;
        }
      }
      // This is recovery evidence, not a guess from an error's `transient`
      // flag. A failed BEGIN is eligible only for a known core conflict; every
      // started transaction must first acknowledge its full rollback. Unknown
      // cleanup/transport outcomes never reach the retry scheduler as safe.
      if (retry !== undefined && this.#transactionFailure === null && !scope.cleanupFailed &&
          (began || !beginDispatched || isTransactionConflict(error))) {
        retry.recovery.recovered = true;
        retry.recovery.retryAllowed = isTransactionConflict(error);
      }
      throw error;
    } finally {
      scope.accepting = false;
      // A child's cleanup failure remains unsafe to replay even when the
      // parent's callback catches it or a later parent operation conflicts.
      if (scope.cleanupFailed && parent !== null) parent.cleanupFailed = true;
      this.#transactionScope = parent;
      signal.removeEventListener("abort", cancel);
      budget.finish();
    }
  }

  #nestedTransaction<T>(
    parent: TransactionScope,
    work: (tx: FrankenTransaction, scope: TransactionScope) => T | Promise<T>,
    beforeCommit?: () => void,
    options?: TransactionOptions,
  ): Promise<T> {
    try {
      this.#assertOwner(parent);
    } catch (error: unknown) {
      return Promise.reject(error);
    }
    const promise = this.#transaction(parent, work, beforeCommit, options);
    parent.children.add(promise);
    // A rolled-back child is recoverable by its parent. Unlike a direct SQL
    // failure, a caught child failure must not automatically poison the parent.
    void promise.then(
      () => { parent.children.delete(promise); },
      () => { parent.children.delete(promise); },
    );
    return promise;
  }

  close(): Promise<void> {
    return this.#run(null, () => this.#client.close());
  }

  #assertOwner(scope: TransactionScope | null, retryOwner?: object): void {
    if (scope !== null && !scope.accepting) {
      throw new FrankenSQLiteError({ code: "ERR_FSQLITE_TRANSACTION_CLOSED",
        message: "This FrankenSQLite transaction callback has finished" });
    }
    if (this.#transactionScope !== scope ||
        (scope === null && this.#retryOwner !== null && this.#retryOwner !== retryOwner)) {
      throw new FrankenSQLiteError({ code: "ERR_FSQLITE_TRANSACTION_OWNERSHIP",
        message: "A transaction owns this connection; use its transaction handle or wait until it finishes" });
    }
    if (scope !== null) this.#checkCancellation(scope);
    // The client may already retain the host's rollback error. Preserve the
    // richer local failure (callback/cancellation plus cleanup) on every later
    // handle operation instead of silently dropping its original cause.
    if (this.#transactionFailure !== null) throw this.#transactionFailure;
  }

  #checkCancellation(scope: TransactionScope): void {
    scope.budget.checkpoint();
    if (!scope.signal.aborted) return;
    if (scope.cancellationError === null) {
      scope.cancellationError = new FrankenSQLiteError({
        code: scope.budget.timedOut ? "ERR_FSQLITE_TRANSACTION_TIMEOUT" : "ERR_FSQLITE_TRANSACTION_CANCELLED",
        message: scope.budget.timedOut ? "This managed transaction exceeded its deadline" : "This managed transaction was cancelled",
        transient: false });
      // Keep the caller's exact reason locally; it need not be structured-cloneable.
      scope.cancellationError.cause = scope.signal.reason;
    }
    throw scope.cancellationError;
  }

  #run<T>(scope: TransactionScope | null, operation: () => Promise<T>): Promise<T> {
    try {
      this.#assertOwner(scope);
    } catch (error: unknown) {
      return Promise.reject(error);
    }
    let promise: Promise<T>;
    try {
      promise = operation();
    } catch (error: unknown) {
      promise = Promise.reject(error);
    }
    if (scope !== null) {
      scope.pending.add(promise);
      void promise.then(
        () => { scope.pending.delete(promise); },
        (error: unknown) => {
          scope.pending.delete(promise);
          if (!scope.errors.includes(error)) scope.errors.push(error);
        },
      );
    }
    return promise;
  }

  async #finishScope<T>(
    scope: TransactionScope,
    work: (tx: FrankenTransaction, scope: TransactionScope) => T | Promise<T>,
  ): Promise<T> {
    const tx = new FrankenTransaction({
      execute: (sql, params) => this.#run(scope, () => this.#client.execute(sql, params, scope.id)),
      executeBatch: (sql) => this.#run(scope, () => this.#client.executeBatch(sql, scope.id)),
      executeMany: (sql, parameterSets, options) => this.#run(scope, () => this.#client.executeMany(sql, parameterSets,
        { signal: combineTransactionSignals(scope.signal, options?.signal)! }, scope.id)),
      executeStream: (sql, rows, options) => this.#streamTransaction(scope, sql, rows, options),
      query: <Row extends Record<string, unknown>>(sql: string, params: SqlBindings = []) =>
        this.#run(scope, () => this.#client.query<Row>(sql, params, scope.id)),
      prepare: <Row extends Record<string, unknown>>(sql: string) =>
        this.#run(scope, () => this.#prepare<Row>(sql, scope)),
    }, (nestedWork, options) => this.#nestedTransaction(scope, tx => nestedWork(tx), undefined, options), scope.signal);
    let result!: T;
    let callbackErrors: unknown[] = [];
    try {
      result = await work(tx, scope);
    } catch (error: unknown) {
      callbackErrors = [error];
    } finally {
      // Reject escaped handles before draining work admitted during the callback.
      scope.accepting = false;
    }
    const children = [...scope.children];
    if (children.length > 0) {
      scope.errors.push(new FrankenSQLiteError({ code: "ERR_FSQLITE_TRANSACTION_UNAWAITED",
        message: "Await nested transactions before returning from the parent callback" }));
    }
    await Promise.allSettled([...scope.pending]);
    for (const child of await Promise.allSettled(children)) {
      if (child.status === "rejected") scope.errors.push(child.reason);
    }
    for (const statementId of scope.statements) {
      try {
        await this.#client.finalizePrepared(statementId, scope.id);
      } catch (error: unknown) {
        scope.cleanupFailed = true;
        scope.errors.push(error);
      }
    }
    scope.statements.clear();
    try { this.#checkCancellation(scope); }
    catch (error: unknown) { scope.errors.push(error); }
    if (this.#transactionFailure !== null) scope.errors.push(this.#transactionFailure);
    const errors = [...new Set([...callbackErrors, ...scope.errors])];
    if (errors.length === 1) throw errors[0];
    if (errors.length > 1) {
      throw new AggregateError(errors, "FrankenSQLite transaction operations failed", { cause: errors[0] });
    }
    return result;
  }
}
