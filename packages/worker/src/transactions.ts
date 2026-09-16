import type { CoreDatabaseHandle } from "./connection";
import type { TransactionRequest } from "./protocol";

export class ManagedTransactionError extends Error {
  readonly cleanupErrors: unknown[] = [];
  readonly transient = false;

  constructor(
    readonly code: string,
    message: string,
    options?: ErrorOptions,
    readonly connectionUnusable = false,
  ) {
    super(message, options);
    this.name = "ManagedTransactionError";
  }
}

export function validateTransactionId(id: string): void {
  if (typeof id !== "string" || !/^[1-9][0-9]{0,31}$/.test(id)) {
    throw new ManagedTransactionError("ERR_FSQLITE_TRANSACTION_INPUT",
      "Transaction ids must be positive decimal integers of at most 32 digits");
  }
}

/**
 * Transaction-boundary preflight, NOT a SQL parser or an authorization sandbox.
 * Walk the entire input before executing a script. Quotes/comments may contain
 * semicolons; CREATE [TEMP|TEMPORARY] TRIGGER ends at its `; END [;]`, not at an
 * internal body semicolon. Syntax and object semantics remain the core's job.
 */
export function validateManagedSql(sql: string, script = false): void {
  const reject = (message: string): never => {
    throw new ManagedTransactionError("ERR_FSQLITE_TRANSACTION_SQL", message);
  };
  if (typeof sql !== "string" || sql.includes("\0")) reject("Managed SQL must be a string without NUL bytes");
  let start = true;
  let count = 0;
  let createPrefix = 0;
  let trigger = false;
  let triggerTail = 0; // 1: body semicolon, 2: END following that semicolon.
  for (let i = 0; i < sql.length;) {
    const char = sql[i]!;
    if (/[\t\n\v\f\r \uFEFF]/.test(char)) { i++; continue; }
    if (sql.startsWith("--", i)) {
      const end = sql.indexOf("\n", i + 2);
      i = end < 0 ? sql.length : end + 1;
      continue;
    }
    if (sql.startsWith("/*", i)) {
      const end = sql.indexOf("*/", i + 2);
      i = end < 0 ? sql.length : end + 2;
      continue;
    }
    if (char === ";") {
      i++;
      if (trigger && triggerTail !== 2) { triggerTail = 1; continue; }
      trigger = false;
      triggerTail = 0;
      createPrefix = 0;
      start = true;
      continue;
    }
    let word = "";
    if (char === "'" || char === '"' || char === "`" || char === "[") {
      const end = char === "[" ? "]" : char;
      let closed = false;
      for (i++; i < sql.length; i++) {
        if (sql[i] !== end) continue;
        if (char !== "[" && sql[i + 1] === end) { i++; continue; }
        i++;
        closed = true;
        break;
      }
      if (!closed) reject("Unterminated quote in managed SQL");
    } else if (/[A-Za-z_\u0080-\uFFFF]/.test(char)) {
      const begin = i++;
      while (i < sql.length && /[A-Za-z0-9_$\u0080-\uFFFF]/.test(sql[i]!)) i++;
      word = sql.slice(begin, i).toUpperCase();
    } else {
      i++;
    }
    if (start) {
      if (["BEGIN", "COMMIT", "END", "ROLLBACK", "SAVEPOINT", "RELEASE"].includes(word)) {
        reject("Transaction boundaries belong to the SDK; use transaction() for nesting");
      }
      if (++count > 1 && !script) reject("This managed operation requires one SQL statement; use executeBatch for scripts");
      start = false;
      createPrefix = word === "CREATE" ? 1 : 0;
    } else if (createPrefix !== 0) {
      if (word === "TRIGGER") { trigger = true; createPrefix = 0; }
      else if (createPrefix === 1 && (word === "TEMP" || word === "TEMPORARY")) createPrefix = 2;
      else createPrefix = 0;
    }
    if (trigger) triggerTail = triggerTail === 1 && word === "END" ? 2 : 0;
  }
  if (count === 0) reject("Managed SQL must contain a statement");
}

interface Frame {
  id: string;
  savepoint: string | null;
  failure: { cause: unknown } | null;
}

/** Only called inside a single host's SQL FIFO, never a cross-connection lock. */
export class ManagedTransactions {
  readonly #stack: Frame[] = [];
  #lastId = 0n;

  clear(): void { this.#stack.length = 0; }

  assertOwner(id: string | undefined, cleanup = false): void {
    const frame = this.#stack.at(-1);
    if (frame === undefined) {
      if (id !== undefined) throw new ManagedTransactionError("ERR_FSQLITE_TRANSACTION_CLOSED", "This worker transaction is no longer active");
      return;
    }
    if (frame.id !== id) throw new ManagedTransactionError("ERR_FSQLITE_TRANSACTION_OWNERSHIP", "Another transaction scope owns this worker connection");
    if (!cleanup && frame.failure !== null) {
      throw new ManagedTransactionError("ERR_FSQLITE_TRANSACTION_ABORTED",
        "This scope failed; only cleanup and rollback may run", { cause: frame.failure.cause });
    }
  }

  fail(id: string | undefined, cause: unknown): void {
    const frame = this.#stack.at(-1);
    if (frame !== undefined && frame.id === id) frame.failure ??= { cause };
  }

  async boundary(
    db: CoreDatabaseHandle,
    request: TransactionRequest,
    finalize: (id: string) => void,
  ): Promise<void> {
    const id = request.transactionId;
    validateTransactionId(id);
    if (request.action === "begin") {
      this.assertOwner(request.parentId);
      // Never reuse a finished/failed id, including across reinitialization.
      if (BigInt(id) <= this.#lastId || this.#stack.length >= 64) {
        throw new ManagedTransactionError("ERR_FSQLITE_TRANSACTION_INPUT", "Transaction id must advance and nesting must not exceed 64 scopes");
      }
      this.#lastId = BigInt(id);
      const savepoint = request.parentId === undefined ? null : `fsqlite_owned_${id}`;
      try {
        await db.executeBatch(savepoint === null ? "BEGIN" : `SAVEPOINT ${savepoint}`);
      } catch (cause: unknown) {
        // Failed BEGIN does not authorize rolling back a caller's manual txn.
        this.fail(request.parentId, cause);
        throw cause;
      }
      this.#stack.push({ id, savepoint, failure: null });
      return;
    }
    this.assertOwner(id, request.action === "rollback");
    const frame = this.#stack.at(-1)!;
    try {
      finalize(id);
    } catch (cause: unknown) {
      const failure = new ManagedTransactionError("ERR_FSQLITE_TRANSACTION_CONNECTION_UNUSABLE",
        "Transaction handle cleanup failed; reopen the connection", { cause }, true);
      try { await this.#rollback(db, frame); }
      catch (cleanupError: unknown) { failure.cleanupErrors.push(cleanupError); }
      throw failure;
    }
    if (request.action === "rollback") {
      try { await this.#rollback(db, frame); }
      catch (cause: unknown) {
        throw new ManagedTransactionError("ERR_FSQLITE_TRANSACTION_CONNECTION_UNUSABLE",
          "Managed rollback failed; no further SQL may execute on this connection", { cause }, true);
      }
    } else {
      try { await db.executeBatch(frame.savepoint === null ? "COMMIT" : `RELEASE SAVEPOINT ${frame.savepoint}`); }
      catch (cause: unknown) { this.fail(id, cause); throw cause; }
    }
    this.#stack.pop();
  }

  async #rollback(db: CoreDatabaseHandle, frame: Frame): Promise<void> {
    if (frame.savepoint === null) { await db.executeBatch("ROLLBACK"); return; }
    await db.executeBatch(`ROLLBACK TO SAVEPOINT ${frame.savepoint}`);
    // Never release after failed rollback-to: RELEASE might commit partial work.
    await db.executeBatch(`RELEASE SAVEPOINT ${frame.savepoint}`);
  }
}
