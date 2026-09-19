import type {
  ExecuteBatchResponse,
  ExecuteManyResult,
  ExecuteResponse,
  ExportResponse,
  InitConfig,
  PrepareResponse,
  QueryResult,
  QueryResponse,
  SqlScalar,
  SqlBindings,
  SnapshotMetadata,
  WorkerRequest,
  WorkerResponse,
  WorkerMessage,
  RequestLimits,
  RequestQueueStats,
} from "@frankensqlite/worker";
import { BindingError, RequestAdmissionError, RequestBudget, resolveBindings, isSnapshotPersistenceMode } from "@frankensqlite/worker";
import type { ParameterLayout } from "@frankensqlite/worker";

import { decodeFrankenError, FrankenSQLiteError } from "./errors";
import { decodeQueryResult, resolveResultEncoding, ResultCodecError } from "@frankensqlite/worker";
import type { ResultEncoding } from "@frankensqlite/worker";
import type { ExecuteManyOptions } from "./types";
import { resolveSnapshotOwnership, SnapshotOwnershipError } from "@frankensqlite/worker";
import type { SnapshotOwnership } from "@frankensqlite/worker";

export interface WorkerMessageEvent {
  readonly data: WorkerMessage;
}

export interface WorkerErrorEventLike {
  readonly message: string;
}

export interface WorkerLike {
  addEventListener(
    type: "message",
    listener: (event: WorkerMessageEvent) => void,
  ): void;
  addEventListener(
    type: "error",
    listener: (event: WorkerErrorEventLike) => void,
  ): void;
  addEventListener(type: "messageerror", listener: () => void): void;
  removeEventListener(
    type: "message",
    listener: (event: WorkerMessageEvent) => void,
  ): void;
  removeEventListener(
    type: "error",
    listener: (event: WorkerErrorEventLike) => void,
  ): void;
  removeEventListener(type: "messageerror", listener: () => void): void;
  postMessage(message: WorkerRequest, transfer?: Transferable[]): void;
  terminate?(): void;
}

interface PendingRequest {
  resolve: (value: WorkerResponse) => void;
  reject: (reason?: unknown) => void;
  release: () => void;
  isClose: boolean;
  requestKind: WorkerRequest["kind"];
}

export class FrankenWorkerClient {
  readonly #worker: WorkerLike;
  readonly #budget: RequestBudget;
  readonly #pending = new Map<number, PendingRequest>();
  #nextRequestId = 1;
  #terminalError: Error | null = null;
  #hostTerminal = false;
  #remoteTransportFailure = false;
  #failure: Error | null = null;
  readonly #failureListeners = new Set<(error: Error) => void>();
  #closing = false;
  #disposed = false;
  #closePromise: Promise<void> | null = null;
  #resultEncoding: ResultEncoding = "structured-clone";
  #snapshotOwnership: SnapshotOwnership | null = null;

  readonly #onMessage = (event: WorkerMessageEvent): void => {
    if (this.#disposed) return;
    let pending: PendingRequest | undefined;
    let requestId: number | undefined;
    try {
      const source = responseObject(event.data);
      if (source.kind === "worker-fatal") {
        const cause = decodeFrankenError(source.error);
        this.#remoteTransportFailure = true;
        this.#failTransport(transportFailure("Worker could not receive or deliver an operation message", cause), true);
        return;
      }
      const id = source.requestId;
      if (!safeCount(id)) throw new TypeError("Worker response has no valid request id");
      requestId = id;
      pending = this.#pending.get(id);
      // Late responses and acknowledgements without retained promises are inert.
      if (pending === undefined) return;
      const kind = source.kind;
      const error = kind === "error" ? decodeFrankenError(source.error) : null;
      const response = error === null ? captureResponse(source, kind, id, pending.requestKind) : null;
      // A custom transport getter can synchronously dispose or fail this client.
      if (this.#pending.get(id) !== pending) return;
      this.#pending.delete(id);
      pending.release();
      if (error === null) { pending.resolve(response!); return; }
      pending.reject(error);
      // These are the host's explicit terminal contracts, not ordinary SQL,
      // quota, schema or admission failures. A live message channel does not
      // imply that its database remains usable after failed rollback.
      if (error.code === "ERR_FSQLITE_SNAPSHOT_CONNECTION_UNUSABLE" ||
          error.code === "ERR_FSQLITE_TRANSACTION_CONNECTION_UNUSABLE" ||
          error.code === "ERR_FSQLITE_BULK_CONNECTION_UNUSABLE") {
        this.#terminalError ??= error;
        this.#hostTerminal = true;
        // The host has closed its database but still accepts its close fence.
        // Keep an admitted close awaiting that real acknowledgement.
        this.#rejectPending(this.#terminalError, true);
        this.#notifyFailure(this.#terminalError);
      }
    } catch (cause: unknown) {
      const error = responseFailure(cause);
      if (pending !== undefined && requestId !== undefined) {
        if (this.#pending.get(requestId) !== pending) return;
        this.#pending.delete(requestId);
        pending.release();
        pending.reject(error);
      } else {
        // Without correlation, any outstanding operation might own the lost
        // result. Fail all of them; never guess an id or replay a write.
        this.#failTransport(error);
      }
    }
  };

  readonly #onError = (event: WorkerErrorEventLike): void => {
    if (this.#disposed) return;
    const error = transportFailure(
      `FrankenSQLite worker crashed: ${event.message || "unknown error"}`,
    );
    this.#failTransport(error);
  };

  readonly #onMessageError = (): void => {
    if (!this.#disposed) this.#failTransport(transportFailure("Worker response could not be deserialized"));
  };

  #failTransport(error: Error, canCloseHost = false): void {
    // A late notice cannot resurrect a channel already known to have crashed.
    this.#hostTerminal = canCloseHost && (this.#terminalError === null || this.#hostTerminal);
    this.#terminalError ??= error;
    this.#rejectPending(this.#terminalError, this.#hostTerminal);
    this.#notifyFailure(this.#terminalError);
  }

  constructor(worker: WorkerLike, limits: Partial<RequestLimits> = {}) {
    this.#budget = new RequestBudget(limits);
    this.#worker = worker;
    try {
      this.#worker.addEventListener("message", this.#onMessage);
      this.#worker.addEventListener("error", this.#onError);
      this.#worker.addEventListener("messageerror", this.#onMessageError);
    } catch (cause: unknown) {
      try { this.dispose(); }
      catch (cleanup: unknown) { throw new AggregateError([cause, cleanup], "Worker setup and cleanup failed", { cause }); }
      throw cause;
    }
  }

  get requestQueue(): RequestQueueStats {
    return this.#budget.stats;
  }

  get resultEncoding(): ResultEncoding {
    return this.#resultEncoding;
  }

  get snapshotOwnership(): SnapshotOwnership | null { return this.#snapshotOwnership; }

  /** Internal owner lifecycle, including faults observed before registration. */
  observeFailure(listener: (error: Error) => void): () => void {
    if (this.#failure !== null) {
      listener(this.#failure);
      return () => {};
    }
    if (this.#disposed) return () => {};
    this.#failureListeners.add(listener);
    return () => { this.#failureListeners.delete(listener); };
  }

  #notifyFailure(error: Error): void {
    this.#failure ??= error;
    const listeners = [...this.#failureListeners];
    this.#failureListeners.clear();
    // Bookkeeping must not interfere with the original request settlement.
    for (const listener of listeners) {
      try { listener(this.#failure); } catch { /* Internal owner already has the error. */ }
    }
  }

  /** Local handle operations must also reject after close/crash/disposal. */
  assertOpen(): void {
    if (this.#terminalError !== null) throw this.#terminalError;
    if (this.#closing) throw new Error("FrankenSQLite worker client is closing");
  }

  /** Validate/capture one complete binding without posting or reserving queue capacity. */
  captureBindings(statementId: string, values: SqlBindings, layout: ParameterLayout, copyBlobs = false): readonly SqlScalar[] {
    this.assertOpen();
    try {
      if (values === null || values === undefined) {
        throw new BindingError("ERR_FSQLITE_BINDING_INPUT", "Bindings must be an array or a named object");
      }
      // Retained bindings are bounded per handle by the request byte limit;
      // they are not outstanding requests and do not inflate queue metrics.
      const budget = new RequestBudget({ maxPendingBytes: this.#budget.stats.maxPendingBytes });
      const admitted = budget.admit({ kind: "statement-query", requestId: 0, statementId, params: values });
      try {
        const request = admitted.request as Extract<WorkerRequest, { kind: "statement-query" }>;
        const resolved = resolveBindings(layout, request.params!);
        // Named ?NNN bindings can expand into a dense positional array. Check
        // that representation too, independently of the incoming map budget.
        const output = new RequestBudget({ maxPendingBytes: this.#budget.stats.maxPendingBytes })
          .admit({ kind: "statement-query", requestId: 0, statementId, params: resolved });
        output.release();
        if (!copyBlobs) return resolved;
        const copies = new Map<ArrayBufferLike, ArrayBuffer>();
        return Object.freeze(resolved.map(value => {
          if (!(value instanceof Uint8Array)) return value;
          let copy = copies.get(value.buffer);
          if (copy === undefined) {
            copy = new Uint8Array(value.buffer).slice().buffer;
            copies.set(value.buffer, copy);
          }
          return new Uint8Array(copy, value.byteOffset, value.byteLength);
        }));
      } finally { admitted.release(); }
    } catch (error: unknown) {
      if (error instanceof BindingError || error instanceof RequestAdmissionError) {
        throw new FrankenSQLiteError({ code: error.code, message: error.message,
          transient: error.transient, userRecoverable: error.userRecoverable });
      }
      throw error;
    }
  }

  async init(config: InitConfig) {
    // Capture caller-owned getters before transport dispatch. A later mutation
    // cannot change which database/persistence acknowledgement is acceptable.
    const captured = { ...config };
    const requested = resolveResultEncoding(captured.resultEncoding);
    const persistence = captured.persistence ?? "memory";
    const ownership = resolveSnapshotOwnership(captured.snapshotOwnership);
    if (ownership !== undefined && !isSnapshotPersistenceMode(persistence)) {
      throw new SnapshotOwnershipError("ERR_FSQLITE_SNAPSHOT_OWNERSHIP_INPUT",
        "snapshotOwnership requires snapshot persistence");
    }
    const path = captured.dbName ?? ":memory:";
    const response = await this.#send({
      kind: "init",
      requestId: this.#nextId(),
      config: captured,
    });
    const ready = ensureKind(response, "ready").data;
    // Snapshot names select durable storage. Memory imports instead report
    // their core-assigned path, which need not equal a caller's display name.
    if (ready.persistence !== persistence || (isSnapshotPersistenceMode(persistence) && ready.path !== path)) {
      const error = new FrankenSQLiteError({ code: "ERR_FSQLITE_PERSISTENCE_POLICY", transient: false,
        userRecoverable: false,
        message: "The worker did not acknowledge the requested database and persistence mode",
        suggestion: "Use a matching SDK and worker. No database handle was opened; do not fall back to memory or another snapshot backend." });
      this.#failTransport(error, true);
      throw error;
    }
    const held = ready.snapshotOwnership;
    if ((held !== undefined && (held !== "shared" && held !== "exclusive" || !isSnapshotPersistenceMode(persistence))) ||
        (ownership !== undefined && held !== ownership) || (ownership === undefined && held === "exclusive")) {
      const error = new FrankenSQLiteError({ code: "ERR_FSQLITE_SNAPSHOT_OWNERSHIP_UNAVAILABLE",
        transient: false, userRecoverable: false,
        message: "The worker did not acknowledge the requested snapshot session ownership",
        suggestion: "Use a matching worker with Web Locks support; no SQL handle was exposed." });
      this.#failTransport(error, true);
      throw error;
    }
    const accepted = resolveResultEncoding(ready.resultEncoding);
    if (accepted !== "structured-clone" && accepted !== requested) {
      throw new FrankenSQLiteError({ code: "ERR_FSQLITE_RESULT_ENCODING", transient: false,
        message: "Worker enabled a result encoding that was not requested" });
    }
    // An older worker may omit the acknowledgement and keep structured clone.
    this.#resultEncoding = accepted;
    this.#snapshotOwnership = held ?? null;
    return ready;
  }

  /** Boundaries are worker operations, never SQL supplied through a public handle. */
  async transaction(action: "begin" | "commit" | "rollback", transactionId: string, parentId?: string): Promise<void> {
    const response = await this.#send({
      kind: "transaction", requestId: this.#nextId(), transactionId, action,
      ...(parentId === undefined ? {} : { parentId }),
    });
    ensureKind(response, "transaction-result");
  }

  cancelTransaction(transactionId: string): void {
    if (this.#terminalError !== null) return;
    try {
      // No acknowledgement promise is retained. Only the scope's actual
      // rollback response proves cleanup; cancellation must bypass saturation.
      this.#worker.postMessage({ kind: "cancel-transaction", requestId: this.#nextId(),
        targetTransactionId: transactionId });
    } catch {
      // Failed delivery is not proof of rollback. Local admission stops and
      // the transaction still awaits its real terminal database response.
    }
  }

  async execute(sql: string, params: SqlBindings = [], transactionId?: string): Promise<number> {
    const response = await this.#send({
      kind: "execute",
      requestId: this.#nextId(),
      sql,
      params,
      ...(transactionId === undefined ? {} : { transactionId }),
    });
    return ensureKind(response, "execute-result").changes;
  }

  async executeBatch(sql: string, transactionId?: string): Promise<void> {
    const response = await this.#send({
      kind: "execute-batch",
      requestId: this.#nextId(),
      sql,
      ...(transactionId === undefined ? {} : { transactionId }),
    });
    ensureKind(response, "execute-batch-result");
  }

  async executeMany(
    sql: string,
    parameterSets: readonly (readonly SqlScalar[])[],
    options: ExecuteManyOptions = {},
    transactionId?: string,
  ): Promise<ExecuteManyResult> {
    const response = await this.#sendBulk({
      kind: "execute-many",
      requestId: this.#nextId(),
      sql,
      parameterSets,
      ...(transactionId === undefined ? {} : { transactionId }),
    }, options.signal);
    return ensureKind(response, "execute-many-result").data;
  }

  async executePreparedMany(
    statementId: string,
    parameterSets: readonly (readonly SqlScalar[])[],
    options: ExecuteManyOptions = {},
    transactionId?: string,
  ): Promise<ExecuteManyResult> {
    const response = await this.#sendBulk({
      kind: "statement-execute-many",
      requestId: this.#nextId(),
      statementId,
      parameterSets,
      ...(transactionId === undefined ? {} : { transactionId }),
    }, options.signal);
    return ensureKind(response, "execute-many-result").data;
  }

  async query<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params: SqlBindings = [],
    transactionId?: string,
  ): Promise<QueryResult<Row>> {
    const response = await this.#send({
      kind: "query",
      requestId: this.#nextId(),
      sql,
      params,
      ...(transactionId === undefined ? {} : { transactionId }),
    });
    return this.#queryResult<Row>(response);
  }

  async prepare(sql: string, transactionId?: string) {
    const response = await this.#send({
      kind: "prepare",
      requestId: this.#nextId(),
      sql,
      ...(transactionId === undefined ? {} : { transactionId }),
    });
    return ensureKind(response, "prepare-result").data;
  }

  async executePrepared(
    statementId: string,
    params: SqlBindings = [],
    transactionId?: string,
  ): Promise<number> {
    const response = await this.#send({
      kind: "statement-execute",
      requestId: this.#nextId(),
      statementId,
      params,
      ...(transactionId === undefined ? {} : { transactionId }),
    });
    return ensureKind(response, "execute-result").changes;
  }

  async queryPrepared<Row extends Record<string, unknown> = Record<string, unknown>>(
    statementId: string,
    params: SqlBindings = [],
    transactionId?: string,
  ): Promise<QueryResult<Row>> {
    const response = await this.#send({
      kind: "statement-query",
      requestId: this.#nextId(),
      statementId,
      params,
      ...(transactionId === undefined ? {} : { transactionId }),
    });
    return this.#queryResult<Row>(response);
  }

  #queryResult<Row extends Record<string, unknown>>(response: WorkerResponse): QueryResult<Row> {
    if (response.kind !== "query-binary-result") {
      return ensureKind(response, "query-result").data as QueryResult<Row>;
    }
    try {
      if (this.#resultEncoding === "structured-clone" || response.encoding !== "fqr1") {
        throw new ResultCodecError("Unexpected or unnegotiated binary result encoding");
      }
      return decodeQueryResult(response.data) as QueryResult<Row>;
    } catch (error: unknown) {
      // SQL has already executed. Reject AFTER #send released its reservation;
      // never retry a write/RETURNING query because its result could not decode.
      throw new FrankenSQLiteError({ code: "ERR_FSQLITE_RESULT_DECODE", transient: false,
        userRecoverable: false,
        message: error instanceof Error ? error.message : "Could not decode query result",
        suggestion: "The SQL may have executed. Do not automatically retry writes; inspect database state." });
    }
  }

  async finalizePrepared(statementId: string, transactionId?: string): Promise<void> {
    const response = await this.#send({
      kind: "statement-finalize",
      requestId: this.#nextId(),
      statementId,
      ...(transactionId === undefined ? {} : { transactionId }),
    });
    ensureKind(response, "statement-finalize-result");
  }

  async export(): Promise<Uint8Array> {
    const response = await this.#send({
      kind: "export",
      requestId: this.#nextId(),
    });
    return ensureKind(response, "export-result").data;
  }

  async checkpoint(publicationId?: string): Promise<SnapshotMetadata> {
    const response = await this.#send({ kind: "checkpoint", requestId: this.#nextId(),
      ...(publicationId === undefined ? {} : { publicationId }) });
    return ensureKind(response, "checkpoint-result").data;
  }

  async recoverCheckpoint(publicationId: string, parentRevision: string | null): Promise<SnapshotMetadata> {
    const response = await this.#send({ kind: "checkpoint-recover", requestId: this.#nextId(), publicationId, parentRevision });
    return ensureKind(response, "checkpoint-result").data;
  }

  close(): Promise<void> {
    if (this.#closePromise !== null) {
      return this.#closePromise;
    }
    // Stop admission synchronously. Requests already posted precede close in
    // the worker's FIFO queue and keep their original completion promises.
    this.#closing = true;
    this.#closePromise = this.#send({
      kind: "close",
      requestId: this.#nextId(),
    }, true).then((response) => {
      ensureKind(response, "close-result");
    }).then(
      () => { this.dispose(); },
      (error: unknown) => {
        try {
          this.dispose();
        } catch (cleanupError: unknown) {
          throw new AggregateError([error, cleanupError],
            "FrankenSQLite close and worker cleanup both failed", { cause: error });
        }
        throw error;
      },
    );
    return this.#closePromise;
  }

  dispose(reason: Error = new Error("FrankenSQLite worker client is disposed")): void {
    if (this.#disposed) {
      return;
    }
    this.#disposed = true;
    this.#terminalError ??= reason;
    // Settle promises BEFORE detaching listeners, including when cleanup throws.
    this.#rejectPending(this.#terminalError);
    if (!this.#closing) this.#notifyFailure(this.#terminalError);
    this.#failureListeners.clear();
    const errors: unknown[] = [];
    for (const cleanup of [
      () => this.#worker.removeEventListener("message", this.#onMessage),
      () => this.#worker.removeEventListener("error", this.#onError),
      () => this.#worker.removeEventListener("messageerror", this.#onMessageError),
      () => this.#worker.terminate?.(),
    ]) {
      try {
        cleanup();
      } catch (error: unknown) {
        errors.push(error);
      }
    }
    if (errors.length === 1) throw errors[0];
    if (errors.length > 1) {
      throw new AggregateError(errors, "FrankenSQLite worker cleanup failed", { cause: errors[0] });
    }
  }

  #rejectPending(error: Error, keepClose = false): void {
    for (const [id, pending] of this.#pending) {
      if (keepClose && pending.isClose) continue;
      pending.release();
      pending.reject(error);
      this.#pending.delete(id);
    }
  }

  #nextId(): number {
    return this.#nextRequestId++;
  }

  async #sendBulk(
    request: Extract<WorkerRequest, { kind: "execute-many" | "statement-execute-many" }>,
    signal?: AbortSignal,
  ): Promise<WorkerResponse> {
    if (signal === undefined) return this.#send(request);
    const checkAborted = (): void => {
      if (signal.aborted) {
        throw new FrankenSQLiteError({ code: "ERR_FSQLITE_BULK_CANCELLED",
          message: "FrankenSQLite bulk execution was cancelled before admission", transient: false });
      }
    };
    checkAborted();
    request.cancellable = true;
    let posted = false;
    let cancelSent = false;
    let settled = false;
    const cancel = (): void => {
      if (!posted || settled || cancelSent) return;
      cancelSent = true;
      // Close may already be queued behind this batch. Allow its cancellation
      // control message through, but never admit additional SQL during close.
      if (this.#terminalError !== null || !this.#pending.has(request.requestId)) return;
      try {
        // No pending promise for this acknowledgement: only the original bulk
        // result proves rollback. A peer withholding cancel acks must not cause
        // an unbounded secondary queue. At most one cancel is sent per batch.
        this.#worker.postMessage({ kind: "cancel-bulk", requestId: this.#nextId(),
          targetRequestId: request.requestId });
      } catch {
        // A failed cancellation delivery cannot establish rollback. The batch's
        // own response (or worker-crash error) remains authoritative.
      }
    };
    signal.addEventListener("abort", cancel, { once: true });
    try {
      checkAborted();
      const response = this.#send(request, false, () => { posted = true; });
      // Covers abort during custom transport dispatch or listener registration.
      if (signal.aborted) cancel();
      return await response;
    } finally {
      settled = true;
      signal.removeEventListener("abort", cancel);
    }
  }

  #send(request: WorkerRequest, allowClosing = false, onPosted?: () => void): Promise<WorkerResponse> {
    const canCloseHost = (): boolean => allowClosing && request.kind === "close" && this.#hostTerminal && !this.#disposed;
    if (this.#terminalError !== null && !canCloseHost()) {
      return Promise.reject(this.#terminalError);
    }
    if (this.#closing && !allowClosing) {
      return Promise.reject(new Error("FrankenSQLite worker client is closing"));
    }
    let release = () => {};
    if (request.kind !== "close") {
      try {
        const admitted = this.#budget.admit(request);
        request = admitted.request;
        release = admitted.release;
      } catch (error: unknown) {
        if (error instanceof RequestAdmissionError) {
          return Promise.reject(new FrankenSQLiteError({ code: error.code, message: error.message,
            transient: error.transient, userRecoverable: error.userRecoverable, suggestion: error.suggestion,
            ...(error.batchIndex === undefined ? {} : { batchIndex: error.batchIndex }) }));
        }
        if (error instanceof BindingError) return Promise.reject(new FrankenSQLiteError({
          code: error.code, message: error.message, transient: false, userRecoverable: true }));
        return Promise.reject(error);
      }
    }
    // Capture can invoke application getters which may have closed/disposed
    // this client. Recheck before posting, and return the unused reservation.
    if ((this.#terminalError !== null && !canCloseHost()) || (this.#closing && !allowClosing)) {
      release();
      return Promise.reject(this.#terminalError ?? new Error("FrankenSQLite worker client is closing"));
    }
    const response = new Promise<WorkerResponse>((resolve, reject) => {
      this.#pending.set(request.requestId, { resolve, reject, release, isClose: request.kind === "close", requestKind: request.kind });
      try {
        if (request.kind === "init" && request.config.snapshot) {
          this.#worker.postMessage(request, [request.config.snapshot.buffer]);
        } else {
          this.#worker.postMessage(request);
        }
        onPosted?.();
      } catch (error: unknown) {
        // A synchronous clone/transport failure has no response to consume.
        this.#pending.delete(request.requestId);
        release();
        reject(error);
      }
    });
    return response.catch(async (error: unknown) => {
      // A receiver-side fatal notice fences SQL but the worker may still own an
      // active operation. Join its close fence before returning a failure that
      // would make a managed transaction dispose that still-active worker.
      // This is cleanup acknowledgement, never proof of the operation outcome.
      if (this.#remoteTransportFailure && request.kind !== "close") {
        try { await this.close(); }
        catch (cleanup: unknown) {
          if (cleanup !== error) throw new AggregateError([error, cleanup],
            "Worker operation response and transport cleanup both failed", { cause: error });
        }
      }
      throw error;
    });
  }
}

function ensureKind<K extends WorkerResponse["kind"]>(
  response: WorkerResponse,
  kind: K,
): Extract<WorkerResponse, { kind: K }> {
  if (response.kind !== kind) {
    throw new Error(
      `Expected worker response kind \`${kind}\`, got \`${response.kind}\``,
    );
  }
  return response as Extract<WorkerResponse, { kind: K }>;
}

function responseObject(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) throw new TypeError("Invalid worker response object");
  return value as Record<string, unknown>;
}
function safeCount(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}
function responseFailure(cause: unknown): FrankenSQLiteError {
  const error = new FrankenSQLiteError({ code: "ERR_FSQLITE_WORKER_RESPONSE", transient: false,
    message: "FrankenSQLite returned an invalid operation response",
    suggestion: "SQL or snapshot publication may have executed. Inspect authoritative state; do not blindly retry the operation." });
  error.cause = cause;
  return error;
}
function transportFailure(message: string, cause?: unknown): FrankenSQLiteError {
  const error = new FrankenSQLiteError({ code: "ERR_FSQLITE_WORKER_TRANSPORT", transient: false,
    userRecoverable: false, message,
    suggestion: "Connection outcomes are unknown: SQL or snapshot publication may have executed. Reopen and reconcile authoritative data; do not blindly retry writes." });
  if (cause !== undefined) error.cause = cause;
  return error;
}
function captureResponse(source: Record<string, unknown>, kind: unknown, requestId: number,
  request: WorkerRequest["kind"]): WorkerResponse {
  const expected: Record<WorkerRequest["kind"], string> = {
    init: "ready", execute: "execute-result", "execute-batch": "execute-batch-result",
    "execute-many": "execute-many-result", query: "query-result", prepare: "prepare-result",
    "statement-execute": "execute-result", "statement-execute-many": "execute-many-result",
    "statement-query": "query-result", "statement-finalize": "statement-finalize-result",
    transaction: "transaction-result", "cancel-transaction": "cancel-transaction-result",
    "cancel-bulk": "cancel-bulk-result", export: "export-result", checkpoint: "checkpoint-result",
    "checkpoint-recover": "checkpoint-result", close: "close-result",
  };
  if (kind !== expected[request] && !(kind === "query-binary-result" && expected[request] === "query-result")) {
    throw new TypeError(`Unexpected response kind for ${request}`);
  }
  switch (kind) {
    case "ready": {
      const data = responseObject(source.data);
      // Capture own data properties rather than retaining a mutable transport
      // envelope or invoking getters while accepting persistence authority.
      const field = (key: string, required = false): unknown => {
        const descriptor = Object.getOwnPropertyDescriptor(data, key);
        if (descriptor === undefined && !required) return undefined;
        if (descriptor === undefined || !Object.hasOwn(descriptor, "value")) {
          throw new TypeError(`Invalid initialization field: ${key}`);
        }
        return descriptor.value;
      };
      const path = field("path", true), persistence = field("persistence", true);
      if (typeof path !== "string" || !["memory", "opfs", "indexeddb", "indexeddb-snapshot", "opfs-snapshot"].includes(persistence as string)) {
        throw new TypeError("Invalid initialization result");
      }
      const ready: Record<string, unknown> = { path, persistence };
      for (const key of ["snapshot", "checkpointRecovery", "preparedStatementLimits", "resultEncoding", "snapshotOwnership"]) {
        const value = field(key);
        if (value !== undefined) ready[key] = value;
      }
      return { kind, requestId, data: Object.freeze(ready) as unknown as import("@frankensqlite/worker").InitResult };
    }
    case "execute-result": {
      const changes = source.changes;
      if (!safeCount(changes)) throw new TypeError("Invalid affected-row count");
      return { kind, requestId, changes };
    }
    case "execute-many-result": {
      const data = responseObject(source.data), executions = data.executions, changes = data.changes;
      const counts = data.changesPerExecution;
      if (!safeCount(executions) || executions > 10000 || !safeCount(changes) || !Array.isArray(counts) ||
          counts.length !== executions) throw new TypeError("Invalid bulk result");
      let total = 0;
      for (let i = 0; i < counts.length; i++) {
        if (!safeCount(counts[i])) throw new TypeError("Invalid bulk affected-row count");
        total += counts[i];
      }
      if (!Number.isSafeInteger(total) || total !== changes) throw new TypeError("Inconsistent bulk counts");
      return { kind, requestId, data: { executions, changes, changesPerExecution: counts as number[] } };
    }
    case "query-result": {
      const data = responseObject(source.data);
      if (!safeCount(data.columnCount) || !Array.isArray(data.columns) || data.columns.length !== data.columnCount ||
          !Array.isArray(data.columnTypes) || !Array.isArray(data.rows) || !Array.isArray(data.rowArrays) ||
          data.rows.length !== data.rowArrays.length) throw new TypeError("Invalid query result envelope");
      // Preserve extension metadata; the core and binary codec own SQL value
      // validation. Do not duplicate or traverse an entire large result here.
      return { kind, requestId, data: data as unknown as QueryResult };
    }
    case "query-binary-result":
      // Keep the existing codec error/negotiation contract and zero-copy input.
      return { kind, requestId, encoding: source.encoding as "fqr1", data: source.data as ArrayBuffer };
    case "prepare-result": {
      const data = responseObject(source.data);
      if (typeof data.statementId !== "string" || data.statementId.length === 0 || typeof data.sql !== "string" ||
          !safeCount(data.columnCount) || !Array.isArray(data.columnNames) || data.columnNames.length !== data.columnCount) {
        throw new TypeError("Invalid prepared statement metadata");
      }
      return { kind, requestId, data: data as unknown as PrepareResponse["data"] };
    }
    case "export-result": {
      const data = source.data;
      if (!(data instanceof Uint8Array)) throw new TypeError("Invalid export bytes");
      return { kind, requestId, data };
    }
    case "checkpoint-result": {
      const data = responseObject(source.data);
      if (typeof data.revision !== "string" || typeof data.sha256 !== "string" || !safeCount(data.byteLength) ||
          (data.parentRevision !== null && typeof data.parentRevision !== "string")) throw new TypeError("Invalid checkpoint metadata");
      return { kind, requestId, data: data as unknown as SnapshotMetadata };
    }
    case "cancel-bulk-result": case "cancel-transaction-result": {
      if (typeof source.accepted !== "boolean") throw new TypeError("Invalid cancellation acknowledgement");
      return { kind, requestId, accepted: source.accepted };
    }
    case "execute-batch-result": case "transaction-result": case "statement-finalize-result": case "close-result":
      return { kind, requestId };
    default: throw new TypeError("Unknown worker response");
  }
}
