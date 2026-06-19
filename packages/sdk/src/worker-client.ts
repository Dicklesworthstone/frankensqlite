import type {
  ExecuteBatchResponse,
  ExecuteResponse,
  ExportResponse,
  InitConfig,
  PrepareResponse,
  QueryResult,
  QueryResponse,
  SqlScalar,
  WorkerRequest,
  WorkerResponse,
} from "@frankensqlite/worker";

import { FrankenSQLiteError } from "./errors";

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
    for (const pending of this.#pending.values()) {
      pending.reject(error);
    }
    this.#pending.clear();
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

  async close(): Promise<void> {
    const response = await this.#send({
      kind: "close",
      requestId: this.#nextId(),
    });
    ensureKind(response, "close-result");
  }

  dispose(): void {
    this.#worker.removeEventListener("message", this.#onMessage);
    this.#worker.removeEventListener("error", this.#onError);
    this.#worker.terminate?.();
    this.#pending.clear();
  }

  #nextId(): number {
    return this.#nextRequestId++;
  }

  #send(request: WorkerRequest): Promise<WorkerResponse> {
    return new Promise<WorkerResponse>((resolve, reject) => {
      this.#pending.set(request.requestId, { resolve, reject });
      if (request.kind === "init" && request.config.snapshot) {
        this.#worker.postMessage(request, [request.config.snapshot.buffer]);
        return;
      }
      this.#worker.postMessage(request);
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
