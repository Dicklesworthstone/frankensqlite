import { namedParameterEnd } from "./bindings";
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
    throw new ManagedTransactionError(
      "ERR_FSQLITE_TRANSACTION_INPUT",
      "Transaction ids must be positive decimal integers of at most 32 digits",
    );
  }
}

/**
 * Transaction-boundary preflight, NOT a SQL parser or an authorization sandbox.
 * Walk the entire input before executing a script. Quotes/comments may contain
 * semicolons; CREATE [TEMP|TEMPORARY] TRIGGER ends at its `; END [;]`, not at an
 * internal body semicolon. Syntax and object semantics remain the core's job.
 */
export function validateManagedSql(sql: string, script = false): void {
  // Exhaust the scanner before executing ANY part of a managed script. A
  // transaction boundary or malformed token in its tail must not be hidden
  // behind an earlier statement with externally observable function effects.
  for (const _end of managedStatementEnds(sql, script)) {
    /* validation only */
  }
}

/**
 * Classify one SELECT, optionally preceded by ordinary/recursive CTEs. This is
 * an ownership/replay fence, not a SQL sandbox: the core still checks syntax,
 * and SELECT functions must be side-effect free. In particular, WITH alone is
 * not evidence of a read: its outer statement may be INSERT/UPDATE/DELETE.
 *
 * Share the managed scanner so quotes, comments and opaque Tcl bind suffixes
 * cannot forge parentheses or statement boundaries. The prefix state machine
 * retains no token array and uses no recursion, including for nested CTEs.
 */
export function isSelectStatement(sql: string): boolean {
  type State =
    | "start"
    | "with"
    | "name"
    | "after-name"
    | "column"
    | "after-column"
    | "as"
    | "hint"
    | "materialized"
    | "open"
    | "body-start"
    | "body"
    | "after-body"
    | "done";
  let state: State = "start";
  let depth = 0;
  let selected = false;
  const visit = (word: string, char: string): void => {
    const name = word !== "" || char === "'" || char === '"' || char === "`" || char === "[";
    switch (state) {
      case "done":
        return;
      case "start":
        if (char === ";") return;
        if (word === "WITH") {
          state = "with";
          return;
        }
        selected = word === "SELECT";
        state = "done";
        return;
      case "with":
        if (word === "RECURSIVE") {
          state = "name";
          return;
        }
        state = name ? "after-name" : "done";
        return;
      case "name":
        state = name ? "after-name" : "done";
        return;
      case "after-name":
        state = char === "(" ? "column" : word === "AS" ? "hint" : "done";
        return;
      case "column":
        state = name ? "after-column" : "done";
        return;
      case "after-column":
        state = char === "," ? "column" : char === ")" ? "as" : "done";
        return;
      case "as":
        state = word === "AS" ? "hint" : "done";
        return;
      case "hint":
        if (word === "NOT") {
          state = "materialized";
          return;
        }
        if (word === "MATERIALIZED") {
          state = "open";
          return;
        }
        // With no hint, this token must be the CTE body's opening parenthesis.
        state = char === "(" ? "body-start" : "done";
        depth = 1;
        return;
      case "materialized":
        state = word === "MATERIALIZED" ? "open" : "done";
        return;
      case "open":
        state = char === "(" ? "body-start" : "done";
        depth = 1;
        return;
      case "body-start":
        state = ["SELECT", "VALUES", "WITH"].includes(word) ? "body" : "done";
        return;
      case "body":
        if (char === "(") depth++;
        else if (char === ")" && --depth === 0) state = "after-body";
        else if (char === ";") state = "done";
        return;
      case "after-body":
        if (char === ",") state = "name";
        else {
          selected = word === "SELECT";
          state = "done";
        }
    }
  };
  // Do not return early on SELECT: malformed tokens or another statement in
  // the tail must still be refused before the caller submits any SQL.
  for (const _end of managedStatementEnds(sql, false, visit)) {
    /* validation and classification */
  }
  return selected;
}

/** Statement end offsets, sharing exactly the managed preflight's lexer. */
function* managedStatementEnds(
  sql: string,
  script: boolean,
  visit?: (word: string, char: string) => void,
): Generator<number> {
  const reject = (message: string): never => {
    throw new ManagedTransactionError("ERR_FSQLITE_TRANSACTION_SQL", message);
  };
  if (typeof sql !== "string" || sql.includes("\0"))
    reject("Managed SQL must be a string without NUL bytes");
  let start = true;
  let count = 0;
  let createPrefix = 0;
  let trigger = false;
  let triggerTail = 0; // 1: body semicolon, 2: END following that semicolon.
  for (let i = 0; i < sql.length; ) {
    const char = sql[i]!;
    if (/[\t\n\v\f\r \uFEFF]/.test(char)) {
      i++;
      continue;
    }
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
      visit?.("", char);
      i++;
      if (trigger && triggerTail !== 2) {
        triggerTail = 1;
        continue;
      }
      if (!start) yield i;
      trigger = false;
      triggerTail = 0;
      createPrefix = 0;
      start = true;
      continue;
    }
    let word = "";
    if (char === ":" || char === "@" || char === "$") {
      // A parameter suffix may contain semicolons and quote characters. It is
      // one opaque token, not a transaction boundary or quoted SQL fragment.
      i = namedParameterEnd(sql, i);
    } else if (char === "'" || char === '"' || char === "`" || char === "[") {
      const end = char === "[" ? "]" : char;
      let closed = false;
      for (i++; i < sql.length; i++) {
        if (sql[i] !== end) continue;
        if (char !== "[" && sql[i + 1] === end) {
          i++;
          continue;
        }
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
    visit?.(word, char);
    if (start) {
      if (["BEGIN", "COMMIT", "END", "ROLLBACK", "SAVEPOINT", "RELEASE"].includes(word)) {
        reject("Transaction boundaries belong to the SDK; use transaction() for nesting");
      }
      if (++count > 1 && !script)
        reject("This managed operation requires one SQL statement; use executeBatch for scripts");
      start = false;
      createPrefix = word === "CREATE" ? 1 : 0;
    } else if (createPrefix !== 0) {
      if (word === "TRIGGER") {
        trigger = true;
        createPrefix = 0;
      } else if (createPrefix === 1 && (word === "TEMP" || word === "TEMPORARY")) createPrefix = 2;
      else createPrefix = 0;
    }
    if (trigger) triggerTail = triggerTail === 1 && word === "END" ? 2 : 0;
  }
  if (count === 0) reject("Managed SQL must contain a statement");
  if (!start) yield sql.length;
}

/**
 * Execute only inside a caller-owned transaction and the host's FIFO slot.
 * The owner supplies a checkpoint that rejects cancellation/transport failure.
 * Rollback belongs to that owner: this routine never starts or commits a txn.
 * A trigger body stays one statement; no array proportional to script length
 * is built. One long SQL statement still requires core-level interruption.
 */
export async function executeManagedBatch(
  db: Pick<CoreDatabaseHandle, "executeBatch">,
  sql: string,
  checkpoint: () => void,
): Promise<void> {
  checkpoint();
  let remaining = 0;
  for (const _end of managedStatementEnds(sql, true)) remaining++;
  checkpoint();
  const yieldTask = (): Promise<void> =>
    new Promise((resolve) => {
      setTimeout(resolve, 0);
    });
  // Give already-arriving cancellation controls a task turn before a script
  // starts. Keep ordinary single-statement calls on their existing fast path.
  if (remaining > 1) {
    await yieldTask();
    checkpoint();
  }
  let start = 0;
  let sinceYield = 0;
  let lastYield = performance.now();
  for (const end of managedStatementEnds(sql, true)) {
    checkpoint();
    await db.executeBatch(sql.slice(start, end));
    // Do not report a script successful or admit its next statement after a
    // cancellation that arrived while the current core operation was awaited.
    checkpoint();
    start = end;
    remaining--;
    sinceYield++;
    if (remaining > 0 && (sinceYield >= 32 || performance.now() - lastYield >= 4)) {
      await yieldTask();
      checkpoint();
      sinceYield = 0;
      lastYield = performance.now();
    }
  }
}

interface Frame {
  id: string;
  savepoint: string | null;
  failure: { cause: unknown } | null;
  committing: boolean;
}

/** SQL runs in one host's FIFO; cancel() only changes its in-memory fence. */
export class ManagedTransactions {
  readonly #stack: Frame[] = [];
  #lastId = 0n;

  clear(): void {
    this.#stack.length = 0;
  }

  cancel(id: string): boolean {
    validateTransactionId(id);
    const index = this.#stack.findIndex((frame) => frame.id === id);
    if (index < 0 || this.#stack[index]!.committing) return false;
    const cause = new ManagedTransactionError(
      "ERR_FSQLITE_TRANSACTION_CANCELLED",
      "This managed transaction was cancelled; cleanup and rollback must finish",
    );
    // Cancelling a child must not poison its parent or successful siblings.
    // A parent can still be cancelled while a child's RELEASE is in flight:
    // that release is provisional, and the parent will roll back afterward.
    for (let i = index; i < this.#stack.length; i++) {
      this.#stack[i]!.failure ??= { cause };
    }
    return true;
  }

  assertOwner(id: string | undefined, cleanup = false): void {
    const frame = this.#stack.at(-1);
    if (frame === undefined) {
      if (id !== undefined)
        throw new ManagedTransactionError(
          "ERR_FSQLITE_TRANSACTION_CLOSED",
          "This worker transaction is no longer active",
        );
      return;
    }
    if (frame.id !== id)
      throw new ManagedTransactionError(
        "ERR_FSQLITE_TRANSACTION_OWNERSHIP",
        "Another transaction scope owns this worker connection",
      );
    if (!cleanup && frame.failure !== null) {
      if (
        frame.failure.cause instanceof ManagedTransactionError &&
        frame.failure.cause.code === "ERR_FSQLITE_TRANSACTION_CANCELLED"
      ) {
        throw frame.failure.cause;
      }
      throw new ManagedTransactionError(
        "ERR_FSQLITE_TRANSACTION_ABORTED",
        "This scope failed; only cleanup and rollback may run",
        { cause: frame.failure.cause },
      );
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
        throw new ManagedTransactionError(
          "ERR_FSQLITE_TRANSACTION_INPUT",
          "Transaction id must advance and nesting must not exceed 64 scopes",
        );
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
      // An out-of-band cancel may have fenced the parent while SAVEPOINT was
      // awaited. Still acknowledge the successful begin so its caller knows
      // it owns rollback, but never let the new child execute unfenced work.
      this.#stack.push({
        id,
        savepoint,
        failure: this.#stack.at(-1)?.failure ?? null,
        committing: false,
      });
      return;
    }
    this.assertOwner(id, request.action === "rollback");
    const frame = this.#stack.at(-1)!;
    try {
      finalize(id);
    } catch (cause: unknown) {
      const failure = new ManagedTransactionError(
        "ERR_FSQLITE_TRANSACTION_CONNECTION_UNUSABLE",
        "Transaction handle cleanup failed; reopen the connection",
        { cause },
        true,
      );
      try {
        await this.#rollback(db, frame);
      } catch (cleanupError: unknown) {
        failure.cleanupErrors.push(cleanupError);
      }
      throw failure;
    }
    if (request.action === "rollback") {
      try {
        await this.#rollback(db, frame);
      } catch (cause: unknown) {
        throw new ManagedTransactionError(
          "ERR_FSQLITE_TRANSACTION_CONNECTION_UNUSABLE",
          "Managed rollback failed; no further SQL may execute on this connection",
          { cause },
          true,
        );
      }
    } else {
      // free() may re-enter control delivery. Check again after cleanup and
      // seal immediately before dispatch, not while commit is merely queued.
      this.assertOwner(id);
      frame.committing = true;
      try {
        await db.executeBatch(
          frame.savepoint === null ? "COMMIT" : `RELEASE SAVEPOINT ${frame.savepoint}`,
        );
      } catch (cause: unknown) {
        frame.committing = false;
        this.fail(id, cause);
        throw cause;
      }
    }
    this.#stack.pop();
  }

  async #rollback(db: CoreDatabaseHandle, frame: Frame): Promise<void> {
    if (frame.savepoint === null) {
      await db.executeBatch("ROLLBACK");
      return;
    }
    await db.executeBatch(`ROLLBACK TO SAVEPOINT ${frame.savepoint}`);
    // Never release after failed rollback-to: RELEASE might commit partial work.
    await db.executeBatch(`RELEASE SAVEPOINT ${frame.savepoint}`);
  }
}
