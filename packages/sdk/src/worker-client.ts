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
  SnapshotMetadata,
  WorkerRequest,
  WorkerResponse,
  RequestLimits,
  RequestQueueStats,
} from "@frankensqlite/worker";
import { RequestAdmissionError, RequestBudget } from "@frankensqlite/worker";

import { FrankenSQLiteError } from "./errors";
import type { ExecuteManyOptions } from "./types";

export interface WorkerMessageEvent {
  readonly data: WorkerResponse;
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
  removeEventListener(
    type: "message",
    listener: (event: WorkerMessageEvent) => void,
  ): void;
  removeEventListener(
    type: "error",
    listener: (event: WorkerErrorEventLike) => void,
  ): void;
  postMessage(message: WorkerRequest, transfer?: Transferable[]): void;
  terminate?(): void;
}

interface PendingRequest {
  resolve: (value: WorkerResponse) => void;
  reject: (reason?: unknown) => void;
  release: () => void;
}

export class FrankenWorkerClient {
  readonly #worker: WorkerLike;
  readonly #budget: RequestBudget;
  readonly #pending = new Map<number, PendingRequest>();
  #nextRequestId = 1;
  #terminalError: Error | null = null;
  #closing = false;
  #disposed = false;
  #closePromise: Promise<void> | null = null;

  readonly #onMessage = (event: WorkerMessageEvent): void => {
    const pending = this.#pending.get(event.data.requestId);
    if (pending === undefined) {
      return;
    }
    this.#pending.delete(event.data.requestId);
    pending.release();
    if (event.data.kind === "error") {
      pending.reject(new FrankenSQLiteError(event.data.error));
      return;
    }
    pending.resolve(event.data);
  };

  readonly #onError = (event: WorkerErrorEventLike): void => {
    const error = new Error(
      `FrankenSQLite worker crashed: ${event.message || "unknown error"}`,
    );
    this.#terminalError ??= error;
    this.#rejectPending(this.#terminalError);
  };

  constructor(worker: WorkerLike, limits: Partial<RequestLimits> = {}) {
    this.#budget = new RequestBudget(limits);
    this.#worker = worker;
    this.#worker.addEventListener("message", this.#onMessage);
    this.#worker.addEventListener("error", this.#onError);
  }

  get requestQueue(): RequestQueueStats {
    return this.#budget.stats;
  }

  async init(config: InitConfig) {
    const response = await this.#send({
      kind: "init",
      requestId: this.#nextId(),
      config,
    });
    return ensureKind(response, "ready").data;
  }

  /** Boundaries are worker operations, never SQL supplied through a public handle. */
  async transaction(action: "begin" | "commit" | "rollback", transactionId: string, parentId?: string): Promise<void> {
    const response = await this.#send({
      kind: "transaction", requestId: this.#nextId(), transactionId, action,
      ...(parentId === undefined ? {} : { parentId }),
    });
    ensureKind(response, "transaction-result");
  }

  async execute(sql: string, params: readonly SqlScalar[] = [], transactionId?: string): Promise<number> {
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
    params: readonly SqlScalar[] = [],
    transactionId?: string,
  ): Promise<QueryResult<Row>> {
    const response = await this.#send({
      kind: "query",
      requestId: this.#nextId(),
      sql,
      params,
      ...(transactionId === undefined ? {} : { transactionId }),
    });
    return ensureKind(response, "query-result").data as QueryResult<Row>;
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
    params: readonly SqlScalar[] = [],
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
    params: readonly SqlScalar[] = [],
    transactionId?: string,
  ): Promise<QueryResult<Row>> {
    const response = await this.#send({
      kind: "statement-query",
      requestId: this.#nextId(),
      statementId,
      params,
      ...(transactionId === undefined ? {} : { transactionId }),
    });
    return ensureKind(response, "query-result").data as QueryResult<Row>;
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

  async checkpoint(): Promise<SnapshotMetadata> {
    const response = await this.#send({ kind: "checkpoint", requestId: this.#nextId() });
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
    const errors: unknown[] = [];
    for (const cleanup of [
      () => this.#worker.removeEventListener("message", this.#onMessage),
      () => this.#worker.removeEventListener("error", this.#onError),
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

  #rejectPending(error: Error): void {
    for (const pending of this.#pending.values()) {
      pending.release();
      pending.reject(error);
    }
    this.#pending.clear();
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
    if (this.#terminalError !== null) {
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
        return Promise.reject(error);
      }
    }
    // Capture can invoke application getters which may have closed/disposed
    // this client. Recheck before posting, and return the unused reservation.
    if (this.#terminalError !== null || (this.#closing && !allowClosing)) {
      release();
      return Promise.reject(this.#terminalError ?? new Error("FrankenSQLite worker client is closing"));
    }
    return new Promise<WorkerResponse>((resolve, reject) => {
      this.#pending.set(request.requestId, { resolve, reject, release });
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
