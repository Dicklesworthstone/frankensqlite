import type { ExecuteManyOptions, ExecuteManyResult, QueryResult, SqlScalar } from "./types";
import { FrankenWorkerClient } from "./worker-client";
import { FrankenSQLiteError } from "./errors";

type StatementOperation = <T>(operation: () => Promise<T>) => Promise<T>;

export class FrankenPreparedStatement<
  Row extends Record<string, unknown> = Record<string, unknown>,
> {
  readonly #client: FrankenWorkerClient;
  readonly #statementId: string;
  readonly #run: StatementOperation;
  readonly #onFinalize: (() => void) | undefined;
  #finalizePromise: Promise<void> | null = null;
  readonly sql: string;
  readonly columnCount: number;
  readonly columnNames: readonly string[];

  constructor(
    client: FrankenWorkerClient,
    statementId: string,
    sql: string,
    columnCount: number,
    columnNames: readonly string[],
    run: StatementOperation = (operation) => operation(),
    onFinalize?: () => void,
  ) {
    this.#client = client;
    this.#statementId = statementId;
    this.#run = run;
    this.#onFinalize = onFinalize;
    this.sql = sql;
    this.columnCount = columnCount;
    this.columnNames = [...columnNames];
  }

  execute(params: readonly SqlScalar[] = []): Promise<number> {
    if (this.#finalizePromise !== null) {
      return Promise.reject(new Error("FrankenSQLite prepared statement is finalized"));
    }
    return this.#run(() => this.#client.executePrepared(this.#statementId, params));
  }

  executeMany(
    parameterSets: readonly (readonly SqlScalar[])[],
    options?: ExecuteManyOptions,
  ): Promise<ExecuteManyResult> {
    if (this.#finalizePromise !== null) {
      return Promise.reject(new Error("FrankenSQLite prepared statement is finalized"));
    }
    return this.#run(() => this.#client.executePreparedMany(this.#statementId, parameterSets, options));
  }

  query(params: readonly SqlScalar[] = []): Promise<QueryResult<Row>> {
    if (this.#finalizePromise !== null) {
      return Promise.reject(new Error("FrankenSQLite prepared statement is finalized"));
    }
    return this.#run(() => this.#client.queryPrepared<Row>(this.#statementId, params));
  }

  finalize(): Promise<void> {
    if (this.#finalizePromise !== null) {
      return this.#finalizePromise;
    }
    return this.#run(() => {
      // Keep scope ownership until the worker actually accepts finalization.
      // Admission refusals run no SQL and must leave the handle retryable (or
      // available to the owning transaction's drain/cleanup after overload).
      this.#finalizePromise = this.#client.finalizePrepared(this.#statementId).then(
        () => { this.#onFinalize?.(); },
        (error: unknown) => {
          if (error instanceof FrankenSQLiteError &&
            (error.code === "ERR_FSQLITE_QUEUE_FULL" || error.code === "ERR_FSQLITE_REQUEST_TOO_LARGE" ||
             error.code === "ERR_FSQLITE_REQUEST_INPUT")) {
            this.#finalizePromise = null;
          } else {
            // A free() error can occur after the worker removed the handle;
            // that outcome is not a license to finalize the same handle twice.
            this.#onFinalize?.();
          }
          throw error;
        },
      );
      return this.#finalizePromise;
    });
  }
}
