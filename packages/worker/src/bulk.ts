import type { CoreDatabaseHandle, CorePreparedStatementHandle } from "./connection";
import { MAX_EXECUTE_MANY_ROWS } from "./protocol";
import type { ExecuteManyResult, SqlScalar } from "./protocol";

export class BulkExecutionError extends Error {
  readonly batchIndex: number | undefined;
  readonly cleanupErrors: unknown[];
  readonly connectionUnusable: boolean;

  constructor(
    cause: unknown,
    batchIndex: number | undefined,
    cleanupErrors: unknown[] = [],
    connectionUnusable = false,
  ) {
    super(batchIndex === undefined
      ? "FrankenSQLite bulk execution failed"
      : `FrankenSQLite bulk execution failed at parameter set ${batchIndex}`, { cause });
    this.name = "BulkExecutionError";
    this.batchIndex = batchIndex;
    this.cleanupErrors = cleanupErrors;
    this.connectionUnusable = connectionUnusable;
  }
}

function invalid(message: string): Error {
  return Object.assign(new Error(message), { code: "ERR_FSQLITE_BULK_INPUT" });
}

/** One token per admitted batch, owned by its WorkerConnectionHost. */
export class BulkCancellation {
  #requested = false;
  #sealed = false;

  constructor(private readonly checkOwner?: () => void) {}

  request(): boolean {
    if (this.#sealed) return false;
    this.#requested = true;
    return true;
  }

  check(): void {
    this.checkOwner?.();
    if (this.#requested) {
      throw Object.assign(new Error("FrankenSQLite bulk execution was cancelled"), {
        code: "ERR_FSQLITE_BULK_CANCELLED", transient: false,
      });
    }
  }

  seal(): void {
    this.check();
    this.finish();
  }

  finish(): void {
    this.#sealed = true;
  }

  async yield(): Promise<void> {
    // A chain of already-resolved core promises would otherwise starve worker
    // message events. This is a task yield, not just another microtask.
    await new Promise<void>((resolve) => { setTimeout(resolve, 0); });
    this.check();
  }
}

/**
 * Admit one DML statement, never transaction control or a multi-statement script.
 * This is a boundary check, not a SQL parser: the core still parses the SQL and
 * checks bindings. Quoted strings/identifiers and comments may contain semicolons.
 * WITH is admitted, but its prepared column count must also be zero.
 */
export function validateBulkSql(sql: string): void {
  let firstWord: string | undefined;
  let ended = false;
  for (let i = 0; i < sql.length;) {
    const char = sql[i]!;
    if (/[\t\n\v\f\r \uFEFF]/.test(char)) {
      i += 1;
    } else if (sql.startsWith("--", i)) {
      const newline = sql.indexOf("\n", i + 2);
      i = newline === -1 ? sql.length : newline + 1;
    } else if (sql.startsWith("/*", i)) {
      const close = sql.indexOf("*/", i + 2);
      if (close === -1) throw invalid("Unterminated SQL comment in bulk statement");
      i = close + 2;
    } else {
      if (ended) throw invalid("Bulk execution requires exactly one DML statement");
      if (firstWord === undefined) {
        const word = /^[A-Za-z]+/.exec(sql.slice(i))?.[0];
        if (word === undefined || !["INSERT", "UPDATE", "DELETE", "REPLACE", "WITH"].includes(word.toUpperCase())) {
          throw invalid("Bulk execution accepts INSERT, UPDATE, DELETE, REPLACE or WITH DML only");
        }
        firstWord = word;
        i += word.length;
      } else if (char === ";") {
        ended = true;
        i += 1;
      } else if (char === "'" || char === '"' || char === "`" || char === "[") {
        const close = char === "[" ? "]" : char;
        i += 1;
        let closed = false;
        while (i < sql.length) {
          if (sql[i] !== close) {
            i += 1;
          } else if (char !== "[" && sql[i + 1] === close) {
            i += 2;
          } else {
            i += 1;
            closed = true;
            break;
          }
        }
        if (!closed) throw invalid("Unterminated SQL quote in bulk statement");
      } else {
        if (char === "\0") throw invalid("NUL bytes are not allowed in bulk SQL");
        i += 1;
      }
    }
  }
  if (firstWord === undefined) throw invalid("Bulk SQL must not be empty");
}

export function validateParameterSets(parameterSets: readonly (readonly SqlScalar[])[]): void {
  if (!Array.isArray(parameterSets) || parameterSets.length > MAX_EXECUTE_MANY_ROWS) {
    throw invalid(`Bulk execution accepts at most ${MAX_EXECUTE_MANY_ROWS} parameter sets`);
  }
  for (let index = 0; index < parameterSets.length; index += 1) {
    if (!Array.isArray(parameterSets[index])) {
      throw new BulkExecutionError(invalid("Each parameter set must be an array"), index);
    }
  }
}

/** Called only while WorkerConnectionHost owns its connection's FIFO slot. */
export async function executeMany(
  db: CoreDatabaseHandle,
  sql: string,
  parameterSets: readonly (readonly SqlScalar[])[],
  savepoint: string,
  prepared?: CorePreparedStatementHandle,
  cancellation?: BulkCancellation,
): Promise<ExecuteManyResult> {
  cancellation?.check();
  validateBulkSql(sql);
  validateParameterSets(parameterSets);
  if (prepared !== undefined && prepared.columnCount !== 0) {
    throw invalid("Bulk execution does not accept result rows; use query for RETURNING/SELECT");
  }
  const result: ExecuteManyResult = { executions: 0, changes: 0, changesPerExecution: [] };
  if (parameterSets.length === 0) {
    cancellation?.seal();
    return result;
  }

  let statement = prepared;
  let ownsStatement = false;
  let began = false;
  let batchIndex: number | undefined;
  let cleanupFailed = false;
  const freeOwned = (): void => {
    if (ownsStatement) {
      // Never call free twice, even when its first call throws.
      ownsStatement = false;
      try {
        statement!.free();
      } catch (error: unknown) {
        cleanupFailed = true;
        throw error;
      }
    }
  };
  try {
    if (cancellation !== undefined) await cancellation.yield();
    if (statement === undefined) {
      statement = await db.prepare(sql);
      ownsStatement = true;
    }
    cancellation?.check();
    if (statement.columnCount !== 0) {
      throw invalid("Bulk execution does not accept result rows; use query for RETURNING/SELECT");
    }
    await db.executeBatch(`SAVEPOINT ${savepoint}`);
    began = true;
    for (const [index, params] of parameterSets.entries()) {
      batchIndex = index;
      cancellation?.check();
      // Explicitly bind EVERY row, even []. Never reuse the previous row's binds.
      const changes = await statement.executeWithParams([...params]);
      cancellation?.check();
      if (!Number.isSafeInteger(changes) || changes < 0 || !Number.isSafeInteger(result.changes + changes)) {
        throw invalid("Bulk affected-row count is outside the safe integer range");
      }
      result.changesPerExecution.push(changes);
      result.changes += changes;
      result.executions += 1;
      if (cancellation !== undefined && result.executions % 128 === 0) {
        await cancellation.yield();
      }
    }
    batchIndex = undefined;
    // Finalization failure must roll back, not report failure after committing.
    freeOwned();
    // Once RELEASE is dispatched, cancellation cannot safely claim that the
    // batch did not commit. Keep the actual commit outcome authoritative.
    cancellation?.seal();
    await db.executeBatch(`RELEASE SAVEPOINT ${savepoint}`);
    began = false;
    return result;
  } catch (cause: unknown) {
    cancellation?.finish();
    const cleanupErrors: unknown[] = [];
    try {
      freeOwned();
    } catch (error: unknown) {
      cleanupErrors.push(error);
    }
    let rollbackFailed = false;
    if (began) {
      try {
        await db.executeBatch(`ROLLBACK TO SAVEPOINT ${savepoint}`);
        // Do not RELEASE after a failed rollback: that could commit partial work.
        await db.executeBatch(`RELEASE SAVEPOINT ${savepoint}`);
      } catch (error: unknown) {
        rollbackFailed = true;
        cleanupErrors.push(error);
      }
    }
    // OR ROLLBACK and rollback-raising triggers may destroy the outer transaction.
    // Missing savepoint/failed cleanup makes the host unusable, never retryable.
    throw new BulkExecutionError(cause, batchIndex, cleanupErrors, rollbackFailed || cleanupFailed);
  }
}
