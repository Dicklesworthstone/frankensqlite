import type { QueryResult, SqlScalar } from "./types";
import type { FrankenDB } from "./database";
import { FrankenPreparedStatement } from "./statement";

type TransactionCapableDb = Pick<
  FrankenDB,
  "execute" | "query" | "prepare"
>;

export class FrankenTransaction {
  readonly #db: TransactionCapableDb;

  constructor(db: TransactionCapableDb) {
    this.#db = db;
  }

  execute(sql: string, params: readonly SqlScalar[] = []): Promise<number> {
    return this.#db.execute(sql, params);
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
}
