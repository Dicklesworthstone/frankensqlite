import type { ExecuteManyOptions, ExecuteManyResult, QueryResult, SqlScalar, SqlBindings } from "./types";
import { parameterLayout } from "@frankensqlite/worker";
import type { ParameterLayout } from "@frankensqlite/worker";
import { FrankenWorkerClient } from "./worker-client";
import { FrankenSQLiteError } from "./errors";
import { combineTransactionSignals } from "./transaction";

type StatementOperation = <T>(operation: () => Promise<T>) => Promise<T>;

export class FrankenPreparedStatement<
  Row extends Record<string, unknown> = Record<string, unknown>,
> {
  readonly #client: FrankenWorkerClient;
  readonly #transactionId: string | undefined;
  readonly #statementId: string;
  readonly #run: StatementOperation;
  readonly #onFinalize: ((failed: boolean) => void) | undefined;
  readonly #signal: AbortSignal | undefined;
  readonly #layout: ParameterLayout;
  #bindings: readonly SqlScalar[] | null = null;
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
    onFinalize?: (failed: boolean) => void,
    transactionId?: string,
    signal?: AbortSignal,
  ) {
    this.#client = client;
    this.#transactionId = transactionId;
    this.#statementId = statementId;
    this.#run = run;
    this.#onFinalize = onFinalize;
    this.#signal = signal;
    this.#layout = parameterLayout(sql);
    this.sql = sql;
    this.columnCount = columnCount;
    this.columnNames = [...columnNames];
  }

  get parameterCount(): number { return this.#layout.count; }

  /** Null denotes an anonymous slot or unused ?NNN hole; includes SQL prefixes. */
  get parameterNames(): readonly (string | null)[] { return this.#layout.names; }

  /** Replace the entire binding with a validated, privately copied snapshot. */
  bind(params: SqlBindings): Promise<this> {
    return this.#operate(() => {
      const captured = this.#client.captureBindings(this.#statementId, params, this.#layout, true);
      // Capturing getters can re-enter close, finalize or another scope. Check
      // again before replacing a valid binding or admitting any SQL.
      return this.#operate(() => { this.#bindings = captured; return Promise.resolve(this); });
    });
  }

  clearBindings(): Promise<void> {
    return this.#operate(() => { this.#bindings = null; return Promise.resolve(); });
  }

  /** Complete bindings required; returns the affected-row count, not a row id. */
  run(params?: SqlBindings): Promise<number> {
    return this.#strict(params, values => this.#client.executePrepared(this.#statementId, values, this.#transactionId));
  }

  /** Executes once to completion; returns the first row or undefined. Not a cursor. */
  get(params?: SqlBindings): Promise<Row | undefined> {
    return this.#strict(params, async values =>
      (await this.#client.queryPrepared<Row>(this.#statementId, values, this.#transactionId)).rows[0]);
  }

  /** Executes once to completion and returns the typed object rows. */
  all(params?: SqlBindings): Promise<Row[]> {
    return this.#strict(params, async values =>
      (await this.#client.queryPrepared<Row>(this.#statementId, values, this.#transactionId)).rows);
  }

  #strict<T>(params: SqlBindings | undefined, operation: (values: readonly SqlScalar[]) => Promise<T>): Promise<T> {
    return this.#operate(() => {
      const values = this.#client.captureBindings(this.#statementId,
        params === undefined ? this.#bindings ?? [] : params, this.#layout);
      return this.#operate(() => operation(values));
    });
  }

  #operate<T>(operation: () => Promise<T>): Promise<T> {
    if (this.#finalizePromise !== null) {
      return Promise.reject(new Error("FrankenSQLite prepared statement is finalized"));
    }
    return this.#run(() => { this.#client.assertOpen(); return operation(); });
  }

  execute(params?: SqlBindings): Promise<number> {
    if (this.#finalizePromise !== null) {
      return Promise.reject(new Error("FrankenSQLite prepared statement is finalized"));
    }
    return this.#run(() => this.#client.executePrepared(this.#statementId,
      params === undefined ? this.#bindings ?? [] : params, this.#transactionId));
  }

  executeMany(
    parameterSets: readonly (readonly SqlScalar[])[],
    options?: ExecuteManyOptions,
  ): Promise<ExecuteManyResult> {
    if (this.#finalizePromise !== null) {
      return Promise.reject(new Error("FrankenSQLite prepared statement is finalized"));
    }
    return this.#run(() => {
      const signal = combineTransactionSignals(this.#signal, options?.signal);
      return this.#client.executePreparedMany(this.#statementId, parameterSets,
        signal === undefined ? {} : { signal }, this.#transactionId);
    });
  }

  query(params?: SqlBindings): Promise<QueryResult<Row>> {
    if (this.#finalizePromise !== null) {
      return Promise.reject(new Error("FrankenSQLite prepared statement is finalized"));
    }
    return this.#run(() => this.#client.queryPrepared<Row>(this.#statementId,
      params === undefined ? this.#bindings ?? [] : params, this.#transactionId));
  }

  finalize(): Promise<void> {
    if (this.#finalizePromise !== null) {
      return this.#finalizePromise;
    }
    return this.#run(() => {
      // Keep scope ownership until the worker actually accepts finalization.
      // Admission refusals run no SQL and must leave the handle retryable (or
      // available to the owning transaction's drain/cleanup after overload).
      this.#finalizePromise = this.#client.finalizePrepared(this.#statementId, this.#transactionId).then(
        () => { this.#bindings = null; this.#onFinalize?.(false); },
        (error: unknown) => {
          if (error instanceof FrankenSQLiteError &&
            (error.code === "ERR_FSQLITE_QUEUE_FULL" || error.code === "ERR_FSQLITE_REQUEST_TOO_LARGE" ||
             error.code === "ERR_FSQLITE_REQUEST_INPUT")) {
            this.#finalizePromise = null;
          } else {
            // A free() error can occur after the worker removed the handle;
            // that outcome is not a license to finalize the same handle twice.
            this.#bindings = null;
            this.#onFinalize?.(true);
          }
          throw error;
        },
      );
      return this.#finalizePromise;
    });
  }
}
