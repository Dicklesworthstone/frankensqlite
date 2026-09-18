import type {
  BinaryQueryResponse,
  CheckpointResponse,
  ExecuteBatchResponse,
  ExecuteManyResponse,
  ExecuteResponse,
  ExportResponse,
  InitConfig,
  PrepareResponse,
  QueryResponse,
  ReadyResponse,
  SerializedFrankenError,
  StatementFinalizeResponse,
  SqlScalar,
  SqlBindings,
  WorkerRequest,
  WorkerResponse,
} from "./protocol";
import {
  assertSupportedPersistenceMode,
  createReadyResult,
  resolveDatabasePath,
  resolvePersistenceMode,
  UnsupportedPersistenceModeError,
} from "./vfs-init";
import { BulkCancellation, BulkExecutionError, executeMany } from "./bulk";
import { IndexedDbSnapshotStore, SnapshotStoreError, validateSnapshotBytes, validateSnapshotName } from "./snapshot-store";
import type { SnapshotMetadata } from "./snapshot-store";
import { RequestAdmissionError, RequestBudget, validateRequestId } from "./admission";
import type { RequestLimits, RequestQueueStats } from "./admission";
import { ManagedTransactionError, ManagedTransactions, executeManagedBatch, validateManagedSql } from "./transactions";
import { encodeQueryResponse, resolveResultEncoding } from "./result-codec";
import type { ResultEncoding } from "./result-codec";
import { parameterLayout, resolveBindings } from "./bindings";
import type { ParameterLayout } from "./bindings";

class CheckpointRollbackError extends SnapshotStoreError {
  readonly cleanupErrors: unknown[] = [];
  constructor(cause: unknown) {
    super("ERR_FSQLITE_SNAPSHOT_CONNECTION_UNUSABLE",
      "The checkpoint transaction probe could not roll back; reopen the connection", { cause });
  }
}

export interface CorePreparedStatementHandle {
  readonly sql: string;
  readonly columnCount: number;
  free(): void;
  columnNames(): string[];
  execute(): Promise<number>;
  executeWithParams(params: unknown[]): Promise<number>;
  query(): Promise<QueryResponse["data"]>;
  queryWithParams(params: unknown[]): Promise<QueryResponse["data"]>;
}

export interface CoreDatabaseHandle {
  readonly path: string;
  free(): void;
  close(): void;
  execute(sql: string): Promise<number>;
  executeBatch(sql: string): Promise<void>;
  executeWithParams(sql: string, params: unknown[]): Promise<number>;
  query(sql: string): Promise<QueryResponse["data"]>;
  queryWithParams(
    sql: string,
    params: unknown[],
  ): Promise<QueryResponse["data"]>;
  prepare(sql: string): Promise<CorePreparedStatementHandle>;
  export(): Promise<Uint8Array>;
}

export interface CoreDatabaseConstructor {
  create(path?: string): Promise<CoreDatabaseHandle>;
  import(data: Uint8Array): Promise<CoreDatabaseHandle>;
}

export interface CoreModule {
  FrankenDB: CoreDatabaseConstructor;
}

export interface CoreModuleLoader {
  load(wasmUrl?: string): Promise<CoreModule>;
}

export const defaultCoreModuleLoader: CoreModuleLoader = {
  async load(wasmUrl?: string): Promise<CoreModule> {
    const core = await import("@frankensqlite/core");
    await core.default(wasmUrl);
    return {
      FrankenDB: core.FrankenDB,
    };
  },
};

export class WorkerConnectionHost {
  readonly #loader: CoreModuleLoader;
  #db: CoreDatabaseHandle | null = null;
  #nextStatementId = 1;
  readonly #statements = new Map<string, CorePreparedStatementHandle>();
  readonly #parameterLayouts = new Map<string, ParameterLayout>();
  readonly #statementOwners = new Map<string, string>();
  readonly #transactions = new ManagedTransactions();
  #requestTail: Promise<void> = Promise.resolve();
  #nextBulkSavepoint = 1n;
  #terminalError: Error | null = null;
  readonly #bulkCancellations = new Map<number, BulkCancellation>();
  #snapshotStore: IndexedDbSnapshotStore | null = null;
  #snapshotRevision: string | null = null;
  readonly #budget: RequestBudget;
  #closePromise: Promise<WorkerResponse> | null = null;
  #resultEncoding: ResultEncoding = "structured-clone";

  constructor(loader: CoreModuleLoader = defaultCoreModuleLoader, limits: Partial<RequestLimits> = {}) {
    this.#loader = loader;
    this.#budget = new RequestBudget(limits);
  }

  get requestQueue(): RequestQueueStats {
    return this.#budget.stats;
  }

  /** Fence queued SQL immediately; close only after active work has settled. */
  failTransport(error: Error): Promise<WorkerResponse> {
    this.#terminalError ??= error;
    for (const cancellation of this.#bulkCancellations.values()) cancellation.request();
    // This internal response is not a client acknowledgement. A later client
    // close joins the same fence and receives its own correlated response.
    return this.handle({ kind: "close", requestId: 0 });
  }

  handle(request: WorkerRequest): Promise<WorkerResponse> {
    try {
      validateRequestId(request.requestId);
      return this.#admit(request);
    } catch (error: unknown) {
      return Promise.resolve({ kind: "error", requestId: request.requestId,
        error: serializeFrankenError(error) });
    }
  }

  #admit(request: WorkerRequest): Promise<WorkerResponse> {
    if (request.kind === "cancel-transaction") {
      // No SQL, queue reservation or tombstone is created by this control.
      return Promise.resolve({ kind: "cancel-transaction-result", requestId: request.requestId,
        accepted: this.#transactions.cancel(request.targetTransactionId) });
    }
    if (request.kind === "cancel-bulk") {
      validateRequestId(request.targetRequestId);
      // Do not put cancellation behind the work it needs to cancel. This only
      // updates a token; SQL and handle destruction still run in FIFO order.
      return Promise.resolve({ kind: "cancel-bulk-result", requestId: request.requestId,
        accepted: this.#bulkCancellations.get(request.targetRequestId)?.request() ?? false });
    }
    if (request.kind === "close") {
      const requestId = request.requestId;
      if (this.#closePromise !== null) {
        return this.#closePromise.then(response => ({ ...response, requestId }));
      }
      // Reserve one close fence independently of ordinary queue capacity. Later
      // SQL cannot enter; earlier SQL must settle before any handle is freed.
      const response = this.#requestTail.then(() => this.#handle({ kind: "close", requestId }));
      this.#closePromise = response;
      this.#requestTail = response.then(() => undefined, () => undefined);
      return response;
    }
    if (this.#closePromise !== null) {
      return Promise.resolve({ kind: "error", requestId: request.requestId,
        error: { code: "ERR_FSQLITE_CONNECTION_CLOSED", message: "FrankenSQLite worker connection is closing or closed" } });
    }
    if (this.#bulkCancellations.has(request.requestId)) {
      return Promise.resolve({ kind: "error", requestId: request.requestId,
        error: { code: "ERR_FSQLITE_BULK_INPUT", message: "Duplicate active bulk request id" } });
    }
    const admitted = this.#budget.admit(request);
    request = admitted.request;
    const release = admitted.release;
    // Direct host callers may supply getters. As on the SDK side, capture can
    // re-enter close; never append captured SQL after an already-admitted fence.
    if (this.#closePromise !== null) {
      release();
      return Promise.resolve({ kind: "error", requestId: request.requestId,
        error: { code: "ERR_FSQLITE_CONNECTION_CLOSED", message: "FrankenSQLite worker connection is closing or closed" } });
    }
    let cancellation: BulkCancellation | undefined;
    if (request.kind === "execute-many" || request.kind === "statement-execute-many") {
      const transactionId = request.transactionId;
      if (request.cancellable || transactionId !== undefined) {
        cancellation = new BulkCancellation(transactionId === undefined
          ? undefined : () => this.#transactions.assertOwner(transactionId));
        if (request.cancellable) this.#bulkCancellations.set(request.requestId, cancellation);
      }
    }
    // Worker message callbacks are not awaited by the browser. Keep ownership
    // of this connection (and its WASM handles) until each request settles,
    // including init, finalize, export and close. Other hosts remain independent.
    const ordinary = request as Exclude<WorkerRequest, { kind: "cancel-bulk" | "cancel-transaction" }>;
    const response = this.#requestTail.then(() => this.#handle(ordinary, cancellation)).finally(() => {
      release();
      if (cancellation !== undefined) {
        cancellation.finish();
        this.#bulkCancellations.delete(request.requestId);
      }
    });
    // A failed request must not poison the queue, even if serializing its error
    // throws. Return the original promise so the caller still sees that failure.
    this.#requestTail = response.then(() => undefined, () => undefined);
    return response;
  }

  async #handle(
    request: Exclude<WorkerRequest, { kind: "cancel-bulk" | "cancel-transaction" }>,
    cancellation?: BulkCancellation,
  ): Promise<WorkerResponse> {
    try {
      if (this.#terminalError !== null && request.kind !== "close") {
        throw this.#terminalError;
      }
      if (request.kind !== "transaction" && request.kind !== "close") {
        this.#transactions.assertOwner(request.transactionId, request.kind === "statement-finalize");
        if ("statementId" in request && this.#statementOwners.get(request.statementId) !== request.transactionId) {
          throw new ManagedTransactionError("ERR_FSQLITE_TRANSACTION_OWNERSHIP", "Prepared statement belongs to another scope");
        }
        if (request.transactionId !== undefined) {
          if (request.kind === "init" || request.kind === "export" || request.kind === "checkpoint") {
            throw new ManagedTransactionError("ERR_FSQLITE_TRANSACTION_OWNERSHIP", "Finish the transaction before replacing or exporting its database");
          }
          // Managed batches perform the same complete preflight in their
          // cancellable runner, before any script statement is dispatched.
          if ("sql" in request && request.kind !== "execute-batch") validateManagedSql(request.sql);
          else if ("statementId" in request && request.kind !== "statement-finalize") {
            validateManagedSql(this.#requireStatement(request.statementId).sql);
          }
        }
      }
      switch (request.kind) {
        case "transaction":
          await this.#transactions.boundary(this.#requireDatabase(), request,
            id => this.#finalizeTransactionStatements(id));
          return { kind: "transaction-result", requestId: request.requestId };
        case "init":
          return await this.#initialize(request.requestId, request.config);
        case "execute":
          return await this.#execute(
            request.requestId,
            request.sql,
            request.params ?? [],
          );
        case "execute-batch":
          return await this.#executeBatch(request.requestId, request.sql, request.transactionId);
        case "execute-many":
          return await this.#executeMany(request.requestId, request.sql, request.parameterSets, undefined, cancellation);
        case "statement-execute-many": {
          const statement = this.#requireStatement(request.statementId);
          return await this.#executeMany(request.requestId, statement.sql, request.parameterSets, statement, cancellation);
        }
        case "query":
          return await this.#query(
            request.requestId,
            request.sql,
            request.params ?? [],
          );
        case "prepare":
          return await this.#prepare(request.requestId, request.sql, request.transactionId);
        case "statement-execute":
          return await this.#statementExecute(
            request.requestId,
            request.statementId,
            request.params ?? [],
          );
        case "statement-query":
          return await this.#statementQuery(
            request.requestId,
            request.statementId,
            request.params ?? [],
          );
        case "statement-finalize":
          return this.#statementFinalize(request.requestId, request.statementId);
        case "export":
          return await this.#exportSnapshot(request.requestId);
        case "checkpoint":
          return await this.#checkpoint(request.requestId);
        case "close":
          return this.#close(request.requestId);
      }
    } catch (error: unknown) {
      // Set the failure fence before the FIFO advances, not after a client has
      // observed the rejection. Queued writes must not escape after OR ROLLBACK.
      if (request.kind !== "transaction") this.#transactions.fail(request.transactionId, error);
      if ((error instanceof BulkExecutionError || error instanceof ManagedTransactionError) &&
          error.connectionUnusable && this.#terminalError === null) {
        this.#terminalError = error;
        try {
          this.#disposeDatabase();
        } catch (cleanupError: unknown) {
          error.cleanupErrors.push(cleanupError);
        }
      }
      return {
        kind: "error",
        requestId: request.requestId,
        error: serializeFrankenError(error),
      };
    }
  }

  async #initialize(
    requestId: number,
    config: InitConfig,
  ): Promise<ReadyResponse> {
    const ready = createReadyResult(config);
    assertSupportedPersistenceMode(ready.persistence);
    const resultEncoding = resolveResultEncoding(config.resultEncoding);

    let stagedStore: IndexedDbSnapshotStore | null = null;
    try {
      let image = config.snapshot;
      let saved: SnapshotMetadata | null = null;
      if (ready.persistence === "indexeddb-snapshot") {
        validateSnapshotName(config.dbName ?? "");
        if (image !== undefined) validateSnapshotBytes(image);
        stagedStore = await IndexedDbSnapshotStore.open(config.dbName!);
        const loaded = await stagedStore.load();
        if (loaded !== null) {
          if (image !== undefined) {
            throw new SnapshotStoreError("ERR_FSQLITE_SNAPSHOT_EXISTS",
              "An existing checkpoint cannot be replaced by an initialization snapshot");
          }
          image = loaded.bytes;
          saved = { revision: loaded.revision, parentRevision: loaded.parentRevision,
            byteLength: loaded.byteLength, sha256: loaded.sha256 };
        }
      }
      // Validate storage before disposing the old session. Failed or corrupt
      // loads must neither erase the old image nor initialize an empty DB.
      const core = await this.#loader.load(config.wasmUrl);
      this.#disposeDatabase();
      this.#db = image !== undefined
        ? await core.FrankenDB.import(image)
        : await core.FrankenDB.create(resolveDatabasePath(config));
      this.#snapshotStore = stagedStore;
      stagedStore = null; // Ownership transfers only after core initialization.
      this.#snapshotRevision = saved?.revision ?? null;
      this.#resultEncoding = resultEncoding;
      return {
        kind: "ready", requestId,
        data: {
          path: ready.persistence === "indexeddb-snapshot" ? ready.path : this.#db.path || ready.path,
          persistence: resolvePersistenceMode(config.persistence),
          resultEncoding,
          ...(ready.persistence === "indexeddb-snapshot" ? { snapshot: saved } : {}),
        },
      };
    } finally {
      stagedStore?.close();
    }
  }

  async #execute(
    requestId: number,
    sql: string,
    bindings: SqlBindings,
  ): Promise<ExecuteResponse> {
    const db = this.#requireDatabase();
    const params = this.#parameters(sql, bindings);
    const changes =
      params.length === 0
        ? await db.execute(sql)
        : await db.executeWithParams(sql, [...params]);
    return {
      kind: "execute-result",
      requestId,
      changes,
    };
  }

  async #executeBatch(
    requestId: number,
    sql: string,
    transactionId?: string,
  ): Promise<ExecuteBatchResponse> {
    const db = this.#requireDatabase();
    if (transactionId === undefined) {
      // Manual scripts may own their own BEGIN/COMMIT and keep core semantics.
      await db.executeBatch(sql);
    } else {
      await executeManagedBatch(db, sql, () => {
        if (this.#terminalError !== null) throw this.#terminalError;
        this.#transactions.assertOwner(transactionId);
      });
    }
    return {
      kind: "execute-batch-result",
      requestId,
    };
  }

  async #query(
    requestId: number,
    sql: string,
    bindings: SqlBindings,
  ): Promise<QueryResponse | BinaryQueryResponse> {
    const db = this.#requireDatabase();
    const params = this.#parameters(sql, bindings);
    const data =
      params.length === 0
        ? await db.query(sql)
        : await db.queryWithParams(sql, [...params]);
    return encodeQueryResponse(requestId, data, this.#resultEncoding);
  }

  async #executeMany(
    requestId: number,
    sql: string,
    parameterSets: readonly (readonly SqlScalar[])[],
    prepared?: CorePreparedStatementHandle,
    cancellation?: BulkCancellation,
  ): Promise<ExecuteManyResponse> {
    return {
      kind: "execute-many-result",
      requestId,
      data: await executeMany(this.#requireDatabase(), sql, parameterSets,
        `fsqlite_bulk_${this.#nextBulkSavepoint++}`, prepared, cancellation),
    };
  }

  async #prepare(requestId: number, sql: string, transactionId?: string): Promise<PrepareResponse> {
    const layout = parameterLayout(sql);
    const stmt = await this.#requireDatabase().prepare(sql);
    const statementId = String(this.#nextStatementId++);
    this.#statements.set(statementId, stmt);
    this.#parameterLayouts.set(statementId, layout);
    if (transactionId !== undefined) this.#statementOwners.set(statementId, transactionId);
    return {
      kind: "prepare-result",
      requestId,
      data: {
        statementId,
        sql: stmt.sql,
        columnCount: stmt.columnCount,
        columnNames: stmt.columnNames(),
        parameterCount: layout.count,
        parameterNames: layout.names,
      },
    };
  }

  async #statementExecute(
    requestId: number,
    statementId: string,
    bindings: SqlBindings,
  ): Promise<ExecuteResponse> {
    const stmt = this.#requireStatement(statementId);
    const params = this.#parameters(stmt.sql, bindings, this.#parameterLayouts.get(statementId));
    const changes =
      params.length === 0
        ? await stmt.execute()
        : await stmt.executeWithParams([...params]);
    return {
      kind: "execute-result",
      requestId,
      changes,
    };
  }

  async #statementQuery(
    requestId: number,
    statementId: string,
    bindings: SqlBindings,
  ): Promise<QueryResponse | BinaryQueryResponse> {
    const stmt = this.#requireStatement(statementId);
    const params = this.#parameters(stmt.sql, bindings, this.#parameterLayouts.get(statementId));
    const data =
      params.length === 0
        ? await stmt.query()
        : await stmt.queryWithParams([...params]);
    return encodeQueryResponse(requestId, data, this.#resultEncoding);
  }

  #statementFinalize(
    requestId: number,
    statementId: string,
  ): StatementFinalizeResponse {
    const stmt = this.#requireStatement(statementId);
    this.#statements.delete(statementId);
    this.#parameterLayouts.delete(statementId);
    this.#statementOwners.delete(statementId);
    stmt.free();
    return {
      kind: "statement-finalize-result",
      requestId,
    };
  }

  async #exportSnapshot(requestId: number): Promise<ExportResponse> {
    return {
      kind: "export-result",
      requestId,
      data: await this.#requireDatabase().export(),
    };
  }

  async #checkpoint(requestId: number): Promise<CheckpointResponse> {
    const db = this.#requireDatabase();
    const store = this.#snapshotStore;
    if (store === null) {
      throw new SnapshotStoreError("ERR_FSQLITE_SNAPSHOT_MODE",
        "Explicit checkpoints require persistence: indexeddb-snapshot");
    }
    // The current WASM contract has no transaction-state accessor. Probe an
    // empty BEGIN/ROLLBACK boundary instead of guessing from SQL text (which
    // misses scripts, prepared control statements and implicit rollbacks).
    // A failed BEGIN does NOT authorize rolling back the caller's transaction.
    try {
      await db.executeBatch("BEGIN");
    } catch (cause: unknown) {
      throw new SnapshotStoreError("ERR_FSQLITE_SNAPSHOT_TRANSACTION",
        "Could not establish an idle checkpoint boundary; finish any active transaction first", { cause });
    }
    try {
      await db.executeBatch("ROLLBACK");
    } catch (cause: unknown) {
      const failure = new CheckpointRollbackError(cause);
      this.#terminalError = failure;
      try { this.#disposeDatabase(); }
      catch (cleanupError: unknown) { failure.cleanupErrors.push(cleanupError); }
      throw failure;
    }
    // Remain in the same host FIFO slot through export, hash, CAS and commit.
    // Quota/conflict/export failures leave both the prior durable checkpoint
    // and this session's expected revision unchanged; memory remains usable.
    const saved = await store.save(await db.export(), this.#snapshotRevision);
    this.#snapshotRevision = saved.revision;
    return { kind: "checkpoint-result", requestId, data: saved };
  }

  #close(requestId: number): WorkerResponse {
    this.#disposeDatabase();
    return {
      kind: "close-result",
      requestId,
    };
  }

  #disposeDatabase(): void {
    const statements = [...this.#statements.values()];
    this.#statements.clear();
    this.#parameterLayouts.clear();
    this.#statementOwners.clear();
    this.#transactions.clear();
    const db = this.#db;
    this.#db = null;
    const snapshotStore = this.#snapshotStore;
    this.#snapshotStore = null;
    this.#snapshotRevision = null;

    let firstError: unknown;
    try { snapshotStore?.close(); }
    catch (error: unknown) { firstError = error; }
    for (const stmt of statements) {
      try {
        stmt.free();
      } catch (error: unknown) {
        firstError ??= error;
      }
    }
    if (db !== null) {
      try {
        db.close();
      } catch (error: unknown) {
        firstError ??= error;
      }
      try {
        db.free();
      } catch (error: unknown) {
        firstError ??= error;
      }
    }
    if (firstError !== undefined) {
      throw firstError;
    }
  }

  #requireDatabase(): CoreDatabaseHandle {
    if (this.#db === null) {
      throw new Error("FrankenSQLite worker is not initialized");
    }
    return this.#db;
  }

  #finalizeTransactionStatements(transactionId: string): void {
    const errors: unknown[] = [];
    for (const [statementId, owner] of this.#statementOwners) {
      if (owner !== transactionId) continue;
      try { this.#statementFinalize(0, statementId); }
      catch (error: unknown) { errors.push(error); }
    }
    if (errors.length !== 0) throw new AggregateError(errors, "Managed statement cleanup failed");
  }

  #requireStatement(statementId: string): CorePreparedStatementHandle {
    const stmt = this.#statements.get(statementId);
    if (stmt === undefined) {
      throw new Error(`Unknown prepared statement id \`${statementId}\``);
    }
    return stmt;
  }

  #parameters(sql: string, bindings: SqlBindings, layout?: ParameterLayout): readonly SqlScalar[] {
    // Existing positional calls keep the core's unbound-value semantics.
    // Named objects are complete bindings; refuse scripts before any SQL runs.
    if (Array.isArray(bindings)) return bindings;
    validateManagedSql(sql);
    return resolveBindings(layout ?? parameterLayout(sql), bindings);
  }
}

export function serializeFrankenError(
  error: unknown,
  depth = 0,
): SerializedFrankenError {
  if (error instanceof BulkExecutionError && depth < 4) {
    const cause = serializeFrankenError(error.cause, depth + 1);
    const serialized: SerializedFrankenError = {
      ...cause,
      message: `${error.message}: ${cause.message}`,
      cause,
      cleanupErrors: error.cleanupErrors.map((item) => serializeFrankenError(item, depth + 1)),
    };
    if (error.batchIndex !== undefined) serialized.batchIndex = error.batchIndex;
    if (error.connectionUnusable) {
      serialized.code = "ERR_FSQLITE_BULK_CONNECTION_UNUSABLE";
      serialized.transient = false;
      serialized.userRecoverable = false;
      serialized.suggestion = "Reopen the database; do not retry on this connection. Inspect the original failure and rollback errors.";
    }
    return serialized;
  }
  const code =
    error instanceof UnsupportedPersistenceModeError
      ? error.code
      : extractStringProperty(error, "code") ?? "ERR_FSQLITE_WORKER";

  const serialized: SerializedFrankenError = {
    code,
    message:
      error instanceof Error
        ? error.message
        : typeof error === "string"
          ? error
          : "Unknown FrankenSQLite worker error",
  };
  if (error instanceof RequestAdmissionError && error.batchIndex !== undefined) {
    serialized.batchIndex = error.batchIndex;
  }

  const sqliteCode = extractNumberProperty(error, "sqliteCode");
  if (sqliteCode !== undefined) {
    serialized.sqliteCode = sqliteCode;
  }
  const extendedCode = extractNumberProperty(error, "extendedCode");
  if (extendedCode !== undefined) {
    serialized.extendedCode = extendedCode;
  }
  const transient = extractBooleanProperty(error, "transient");
  if (transient !== undefined) {
    serialized.transient = transient;
  }
  const userRecoverable = extractBooleanProperty(error, "userRecoverable");
  if (userRecoverable !== undefined) {
    serialized.userRecoverable = userRecoverable;
  }
  const suggestion = extractStringProperty(error, "suggestion");
  if (suggestion !== undefined) {
    serialized.suggestion = suggestion;
  }
  if (error instanceof Error && error.stack !== undefined) {
    serialized.stack = error.stack;
  }

  if (error instanceof SnapshotStoreError && depth < 4 && error.cause !== undefined) {
    serialized.cause = serializeFrankenError(error.cause, depth + 1);
  }
  if (error instanceof CheckpointRollbackError) {
    serialized.transient = false;
    serialized.userRecoverable = false;
    serialized.cleanupErrors = depth < 4
      ? error.cleanupErrors.map((item) => serializeFrankenError(item, depth + 1)) : [];
  }
  if (error instanceof ManagedTransactionError) {
    if (depth < 4 && error.cause !== undefined) serialized.cause = serializeFrankenError(error.cause, depth + 1);
    if (error.connectionUnusable) serialized.userRecoverable = false;
    if (error.cleanupErrors.length > 0 && depth < 4) {
      serialized.cleanupErrors = error.cleanupErrors.map(item => serializeFrankenError(item, depth + 1));
    }
  }

  return serialized;
}

function extractStringProperty(
  value: unknown,
  key: string,
): string | undefined {
  if (typeof value !== "object" || value === null) {
    return undefined;
  }
  const property = Reflect.get(value, key);
  return typeof property === "string" ? property : undefined;
}

function extractNumberProperty(
  value: unknown,
  key: string,
): number | undefined {
  if (typeof value !== "object" || value === null) {
    return undefined;
  }
  const property = Reflect.get(value, key);
  return typeof property === "number" ? property : undefined;
}

function extractBooleanProperty(
  value: unknown,
  key: string,
): boolean | undefined {
  if (typeof value !== "object" || value === null) {
    return undefined;
  }
  const property = Reflect.get(value, key);
  return typeof property === "boolean" ? property : undefined;
}
