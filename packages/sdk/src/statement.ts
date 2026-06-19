import type { QueryResult, SqlScalar } from "./types";
import { FrankenWorkerClient } from "./worker-client";

export class FrankenPreparedStatement<
  Row extends Record<string, unknown> = Record<string, unknown>,
> {
  readonly #client: FrankenWorkerClient;
  readonly #statementId: string;
  readonly sql: string;
  readonly columnCount: number;
  readonly columnNames: readonly string[];

  constructor(
    client: FrankenWorkerClient,
    statementId: string,
    sql: string,
    columnCount: number,
    columnNames: readonly string[],
  ) {
    this.#client = client;
    this.#statementId = statementId;
    this.sql = sql;
    this.columnCount = columnCount;
    this.columnNames = [...columnNames];
  }

  execute(params: readonly SqlScalar[] = []): Promise<number> {
    return this.#client.executePrepared(this.#statementId, params);
  }

  query(params: readonly SqlScalar[] = []): Promise<QueryResult<Row>> {
    return this.#client.queryPrepared<Row>(this.#statementId, params);
  }

  finalize(): Promise<void> {
    return this.#client.finalizePrepared(this.#statementId);
  }
}
