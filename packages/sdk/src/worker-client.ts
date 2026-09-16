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
} from "@frankensqlite/worker";
import { MAX_EXECUTE_MANY_ROWS } from "@frankensqlite/worker";

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
}

export class FrankenWorkerClient {
  readonly #worker: WorkerLike;
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

  constructor(worker: WorkerLike) {
    this.#worker = worker;
    this.#worker.addEventListener("message", this.#onMessage);
    this.#worker.addEventListener("error", this.#onError);
  }

  async init(config: InitConfig) {
    const response = await this.#send({
      kind: "init",
      requestId: this.#nextId(),
      config,
    });
    return ensureKind(response, "ready").data;
  }

  async execute(sql: string, params: readonly SqlScalar[] = []): Promise<number> {
    const response = await this.#send({
      kind: "execute",
      requestId: this.#nextId(),
      sql,
      params: [...params],
    });
    return ensureKind(response, "execute-result").changes;
  }

  async executeBatch(sql: string): Promise<void> {
    const response = await this.#send({
      kind: "execute-batch",
      requestId: this.#nextId(),
      sql,
    });
    ensureKind(response, "execute-batch-result");
  }

  async executeMany(
    sql: string,
    parameterSets: readonly (readonly SqlScalar[])[],
    options: ExecuteManyOptions = {},
  ): Promise<ExecuteManyResult> {
    const response = await this.#sendBulk({
      kind: "execute-many",
      requestId: this.#nextId(),
      sql,
      parameterSets: copyParameterSets(parameterSets),
    }, options.signal);
    return ensureKind(response, "execute-many-result").data;
  }

  async executePreparedMany(
    statementId: string,
    parameterSets: readonly (readonly SqlScalar[])[],
    options: ExecuteManyOptions = {},
  ): Promise<ExecuteManyResult> {
    const response = await this.#sendBulk({
      kind: "statement-execute-many",
      requestId: this.#nextId(),
      statementId,
      parameterSets: copyParameterSets(parameterSets),
    }, options.signal);
    return ensureKind(response, "execute-many-result").data;
  }

  async query<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params: readonly SqlScalar[] = [],
  ): Promise<QueryResult<Row>> {
    const response = await this.#send({
      kind: "query",
      requestId: this.#nextId(),
      sql,
      params: [...params],
    });
    return ensureKind(response, "query-result").data as QueryResult<Row>;
  }

  async prepare(sql: string) {
    const response = await this.#send({
      kind: "prepare",
      requestId: this.#nextId(),
      sql,
    });
    return ensureKind(response, "prepare-result").data;
  }

  async executePrepared(
    statementId: string,
    params: readonly SqlScalar[] = [],
  ): Promise<number> {
    const response = await this.#send({
      kind: "statement-execute",
      requestId: this.#nextId(),
      statementId,
      params: [...params],
    });
    return ensureKind(response, "execute-result").changes;
  }

  async queryPrepared<Row extends Record<string, unknown> = Record<string, unknown>>(
    statementId: string,
    params: readonly SqlScalar[] = [],
  ): Promise<QueryResult<Row>> {
    const response = await this.#send({
      kind: "statement-query",
      requestId: this.#nextId(),
      statementId,
      params: [...params],
    });
    return ensureKind(response, "query-result").data as QueryResult<Row>;
  }

  async finalizePrepared(statementId: string): Promise<void> {
    const response = await this.#send({
      kind: "statement-finalize",
      requestId: this.#nextId(),
      statementId,
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
      void this.#send({ kind: "cancel-bulk", requestId: this.#nextId(),
        targetRequestId: request.requestId }, true).catch(() => {
        // A failed cancellation delivery cannot establish rollback. The batch's
        // own response (or worker-crash error) remains authoritative.
      });
    };
    signal.addEventListener("abort", cancel, { once: true });
    try {
      checkAborted();
      const response = this.#send(request);
      posted = true;
      // Covers abort during custom transport dispatch or listener registration.
      if (signal.aborted) cancel();
      return await response;
    } finally {
      settled = true;
      signal.removeEventListener("abort", cancel);
    }
  }

  #send(request: WorkerRequest, allowClosing = false): Promise<WorkerResponse> {
    if (this.#terminalError !== null) {
      return Promise.reject(this.#terminalError);
    }
    if (this.#closing && !allowClosing) {
      return Promise.reject(new Error("FrankenSQLite worker client is closing"));
    }
    return new Promise<WorkerResponse>((resolve, reject) => {
      this.#pending.set(request.requestId, { resolve, reject });
      try {
        if (request.kind === "init" && request.config.snapshot) {
          this.#worker.postMessage(request, [request.config.snapshot.buffer]);
        } else {
          this.#worker.postMessage(request);
        }
      } catch (error: unknown) {
        // A synchronous clone/transport failure has no response to consume.
        this.#pending.delete(request.requestId);
        reject(error);
      }
    });
  }
}

function copyParameterSets(parameterSets: readonly (readonly SqlScalar[])[]): SqlScalar[][] {
  // Bound input before cloning/posting it. Never split a batch into separately
  // committed chunks behind the caller's back.
  if (!Array.isArray(parameterSets) || parameterSets.length > MAX_EXECUTE_MANY_ROWS) {
    throw new FrankenSQLiteError({ code: "ERR_FSQLITE_BULK_INPUT",
      message: `Bulk execution accepts at most ${MAX_EXECUTE_MANY_ROWS} parameter sets` });
  }
  return Array.from(parameterSets, (params, batchIndex) => {
    if (!Array.isArray(params)) {
      throw new FrankenSQLiteError({ code: "ERR_FSQLITE_BULK_INPUT", batchIndex,
        message: "Each parameter set must be an array" });
    }
    return [...params];
  });
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
