import type { ExecuteManyOptions, ExecuteManyResult, QueryResult, SqlScalar, SqlBindings, TransactionOptions } from "./types";
import type { ExecuteStreamOptions, ExecuteStreamResult, SqlRowSource } from "./types";
import type { FrankenDB } from "./database";
import type { FrankenPreparedStatement } from "./statement";
import { FrankenSQLiteError } from "./errors";

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

/** Capture caller getters before reserving any connection or queue authority. */
export function captureTransactionOptions(options?: TransactionOptions): TransactionOptions {
  const signal = options?.signal;
  const timeoutMs = options?.timeoutMs;
  if (timeoutMs !== undefined && (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 2_147_483_647)) {
    throw new FrankenSQLiteError({ code: "ERR_FSQLITE_TRANSACTION_INPUT", transient: false,
      message: "timeoutMs must be an integer in 1..2147483647" });
  }
  if (signal !== undefined) {
    Object.getOwnPropertyDescriptor(AbortSignal.prototype, "aborted")!.get!.call(signal);
  }
  const captured: TransactionOptions = {};
  if (signal !== undefined) captured.signal = signal;
  if (timeoutMs !== undefined) captured.timeoutMs = timeoutMs;
  return captured;
}

/** Internal scope lifetime; timers wake waiting work, checkpoints gate new SQL. */
export class TransactionBudget {
  readonly signal: AbortSignal;
  readonly #parent: TransactionBudget | undefined;
  readonly #outerCheckpoint: (() => void) | undefined;
  readonly #deadline: number | undefined;
  readonly #timeout: AbortController | undefined;
  readonly #reason: Error | undefined;
  #timer: ReturnType<typeof setTimeout> | undefined;
  #finished = false;

  constructor(parent: TransactionBudget | undefined, options: TransactionOptions,
    outerCheckpoint?: () => void) {
    this.#parent = parent;
    this.#outerCheckpoint = outerCheckpoint;
    const signals: AbortSignal[] = [];
    if (parent !== undefined) signals.push(parent.signal);
    if (options.signal !== undefined) signals.push(options.signal);
    if (options.timeoutMs !== undefined) {
      this.#deadline = performance.now() + options.timeoutMs;
      this.#timeout = new AbortController();
      this.#reason = new Error("Managed transaction deadline expired");
      signals.push(this.#timeout.signal);
    }
    this.signal = AbortSignal.any(signals);
    this.#arm();
  }

  checkpoint(): void {
    if (this.#finished) return;
    // Poll ancestors, not just their signals: timers can be starved by CPU
    // work or resolved-Promise chains. A child's budget cannot reset a parent
    // or retry deadline, even if it supplies a longer timeout of its own.
    this.#parent?.checkpoint();
    this.#outerCheckpoint?.();
    if (this.#deadline !== undefined && performance.now() >= this.#deadline && !this.#timeout!.signal.aborted) {
      clearTimeout(this.#timer);
      this.#timer = undefined;
      this.#timeout!.abort(this.#reason);
    }
  }

  get timedOut(): boolean {
    if (!this.signal.aborted) return false;
    return (this.#reason !== undefined && this.signal.reason === this.#reason) ||
      (this.#parent?.timedOut === true && this.signal.reason === this.#parent.signal.reason);
  }

  finish(): void {
    this.#finished = true;
    clearTimeout(this.#timer);
    this.#timer = undefined;
  }

  #arm(): void {
    if (this.#finished || this.signal.aborted || this.#deadline === undefined) return;
    this.#timer = setTimeout(() => {
      this.#timer = undefined;
      this.checkpoint();
      // An early timer is not authority to expire the monotonic deadline.
      this.#arm();
    }, Math.max(1, Math.ceil(this.#deadline - performance.now())));
  }
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
