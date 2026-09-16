import type { ExecuteManyOptions, ExecuteManyResult, QueryResult, SqlScalar } from "./types";
import type { FrankenDB } from "./database";
import { FrankenPreparedStatement } from "./statement";

type TransactionCapableDb = Pick<
  FrankenDB,
  "execute" | "executeMany" | "query" | "prepare"
>;

type NestedTransaction = <T>(work: (tx: FrankenTransaction) => T | Promise<T>) => Promise<T>;

export class FrankenTransaction {
  readonly #db: TransactionCapableDb;
  readonly #nested: NestedTransaction | undefined;

  constructor(db: TransactionCapableDb, nested?: NestedTransaction) {
    this.#db = db;
    this.#nested = nested;
  }

  execute(sql: string, params: readonly SqlScalar[] = []): Promise<number> {
    return this.#db.execute(sql, params);
  }

  executeMany(
    sql: string,
    parameterSets: readonly (readonly SqlScalar[])[],
    options?: ExecuteManyOptions,
  ): Promise<ExecuteManyResult> {
    return this.#db.executeMany(sql, parameterSets, options);
  }

  query<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params: readonly SqlScalar[] = [],
  ): Promise<QueryResult<Row>> {
    return this.#db.query<Row>(sql, params);
  }

  prepare<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
  ): Promise<FrankenPreparedStatement<Row>> {
    return this.#db.prepare<Row>(sql);
  }

  /** Run an isolated child scope using a SAVEPOINT on this transaction. */
  transaction<T>(work: (tx: FrankenTransaction) => T | Promise<T>): Promise<T> {
    if (this.#nested === undefined) {
      return Promise.reject(new Error("Nested transactions require a managed transaction callback"));
    }
    return this.#nested(work);
  }
}
