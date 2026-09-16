import type { QueryResult, SqlScalar } from "./types";
import { FrankenWorkerClient } from "./worker-client";

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
      // Do not mark finalized if transaction ownership rejects admission.
      this.#onFinalize?.();
      this.#finalizePromise = this.#client.finalizePrepared(this.#statementId);
      return this.#finalizePromise;
    });
  }
}
