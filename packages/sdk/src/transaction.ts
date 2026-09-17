import type { ExecuteManyOptions, ExecuteManyResult, QueryResult, SqlScalar, SqlBindings, TransactionOptions } from "./types";
import type { ExecuteStreamOptions, ExecuteStreamResult, SqlRowSource } from "./types";
import type { FrankenDB } from "./database";
import type { FrankenPreparedStatement } from "./statement";

type TransactionCapableDb = Pick<
  FrankenDB,
  "execute" | "executeBatch" | "executeMany" | "executeStream" | "query" | "prepare"
>;

type NestedTransaction = <T>(work: (tx: FrankenTransaction) => T | Promise<T>, options?: TransactionOptions) => Promise<T>;

/** Native dependent signals avoid a secondary set of retained JS listeners. */
export function combineTransactionSignals(scope: AbortSignal | undefined, operation: AbortSignal | undefined): AbortSignal | undefined {
  if (scope === undefined) return operation;
  if (operation === undefined || operation === scope) return scope;
  return AbortSignal.any([scope, operation]);
}

export class FrankenTransaction {
  readonly #db: TransactionCapableDb;
  readonly #nested: NestedTransaction | undefined;
  readonly signal: AbortSignal;

  constructor(db: TransactionCapableDb, nested?: NestedTransaction, signal = new AbortController().signal) {
    this.#db = db;
    this.#nested = nested;
    this.signal = signal;
  }

  execute(sql: string, params: SqlBindings = []): Promise<number> {
    return this.#db.execute(sql, params);
  }

  /** Run a script in this scope; the worker rejects manual transaction boundaries. */
  executeBatch(sql: string): Promise<void> {
    return this.#db.executeBatch(sql);
  }

  executeMany(
    sql: string,
    parameterSets: readonly (readonly SqlScalar[])[],
    options?: ExecuteManyOptions,
  ): Promise<ExecuteManyResult> {
    return this.#db.executeMany(sql, parameterSets, options);
  }

  /** A recoverable child savepoint containing the entire stream. */
  executeStream(
    sql: string,
    rows: SqlRowSource,
    options?: ExecuteStreamOptions,
  ): Promise<ExecuteStreamResult> {
    return this.#db.executeStream(sql, rows, options);
  }

  query<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params: SqlBindings = [],
  ): Promise<QueryResult<Row>> {
    return this.#db.query<Row>(sql, params);
  }

  prepare<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
  ): Promise<FrankenPreparedStatement<Row>> {
    return this.#db.prepare<Row>(sql);
  }

  /** Run an isolated child scope using a SAVEPOINT on this transaction. */
  transaction<T>(work: (tx: FrankenTransaction) => T | Promise<T>, options?: TransactionOptions): Promise<T> {
    if (this.#nested === undefined) {
      return Promise.reject(new Error("Nested transactions require a managed transaction callback"));
    }
    return this.#nested(work, options);
  }
}
