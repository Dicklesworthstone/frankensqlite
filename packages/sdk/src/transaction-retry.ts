import { FrankenSQLiteError } from "./errors";

/** Explicit opt-in: the callback may run again, including its non-SQL effects. */
export interface TransactionRetryOptions {
  /** Total attempts, including the first. Integer 1..100; default 4. */
  maxAttempts?: number;
  /** One cooperative deadline for all attempts, cleanup and backoff; default 5000. */
  timeoutMs?: number;
  /** Exponential backoff starts here, with full jitter. Default 5 ms. */
  initialDelayMs?: number;
  /** Maximum backoff between attempts. Default 250 ms. */
  maxDelayMs?: number;
  /** Cancellation joins the active transaction's cleanup; it never abandons it. */
  signal?: AbortSignal;
}

export interface TransactionRetryAttempt {
  /** One-based attempt number. Each attempt has a fresh transaction and handles. */
  readonly attempt: number;
  readonly maxAttempts: number;
}

export class FrankenTransactionRetryError extends Error {
  constructor(
    readonly code: "ERR_FSQLITE_TRANSACTION_RETRY_INPUT" |
      "ERR_FSQLITE_TRANSACTION_RETRY_CANCELLED" | "ERR_FSQLITE_TRANSACTION_RETRY_TIMEOUT",
    message: string,
    readonly attempts: number,
    options?: ErrorOptions,
    readonly lastError?: unknown,
  ) {
    super(message, options);
    this.name = "FrankenTransactionRetryError";
  }
}

/** Internal: populated by the owning transaction AFTER rollback and cleanup. */
export interface RetryRecovery {
  recovered: boolean;
  retryAllowed: boolean;
}

export interface ResolvedTransactionRetryOptions {
  readonly maxAttempts: number;
  readonly timeoutMs: number;
  readonly initialDelayMs: number;
  readonly maxDelayMs: number;
  readonly signal: AbortSignal | undefined;
}

export function resolveTransactionRetryOptions(options?: TransactionRetryOptions): ResolvedTransactionRetryOptions {
  // Capture caller getters once, before the database claims its retry lease.
  const maxAttempts = options?.maxAttempts ?? 4;
  const timeoutMs = options?.timeoutMs ?? 5000;
  const initialDelayMs = options?.initialDelayMs ?? 5;
  const maxDelayMs = options?.maxDelayMs ?? 250;
  const signal = options?.signal;
  for (const [name, value, min, max] of [
    ["maxAttempts", maxAttempts, 1, 100],
    ["timeoutMs", timeoutMs, 1, 2_147_483_647],
    ["initialDelayMs", initialDelayMs, 0, 2_147_483_647],
    ["maxDelayMs", maxDelayMs, 0, 2_147_483_647],
  ] as const) {
    if (!Number.isSafeInteger(value) || value < min || value > max) {
      throw new FrankenTransactionRetryError("ERR_FSQLITE_TRANSACTION_RETRY_INPUT",
        `${name} must be an integer in ${min}..${max}`, 0);
    }
  }
  if (initialDelayMs > maxDelayMs) {
    throw new FrankenTransactionRetryError("ERR_FSQLITE_TRANSACTION_RETRY_INPUT",
      "initialDelayMs must not exceed maxDelayMs", 0);
  }
  if (signal !== undefined) {
    Object.getOwnPropertyDescriptor(AbortSignal.prototype, "aborted")!.get!.call(signal);
  }
  return { maxAttempts, timeoutMs, initialDelayMs, maxDelayMs, signal };
}

/**
 * Only positively identified SQLite BUSY-family errors qualify. A `transient`
 * flag, message substring, LOCKED, storage CAS conflict, or a cause somewhere
 * inside an otherwise fatal error does not authorize transaction replay.
 */
export function isTransactionConflict(error: unknown): boolean {
  const ancestors = new Set<object>();
  let remaining = 64;
  const visit = (value: unknown, depth: number): boolean => {
    if (--remaining < 0 || depth > 8 || !(value instanceof Error) || ancestors.has(value)) return false;
    ancestors.add(value);
    try {
      if (value instanceof AggregateError) {
        const children = value.errors;
        if (!Array.isArray(children) || children.length === 0 || children.length > remaining) return false;
        for (let i = 0; i < children.length; i++) {
          if (!Object.hasOwn(children, i) || !visit(children[i], depth + 1)) return false;
        }
        return true;
      }
      if (!(value instanceof FrankenSQLiteError) || value.cleanupErrors.length !== 0 || value.userRecoverable === false) return false;
      // Later operations in an already-failed scope can report this wrapper.
      // It cannot continue in-place, but the enclosing transaction can restart
      // after confirmed rollback if every underlying failure is a conflict.
      if (value.code === "ERR_FSQLITE_TRANSACTION_ABORTED") return visit(value.cause, depth + 1);
      if (value.transient === false) return false;
      const codes: Record<string, number> = {
        SQLITE_BUSY: 5, SQLITE_BUSY_RECOVERY: 261,
        SQLITE_BUSY_SNAPSHOT: 517, SQLITE_BUSY_TIMEOUT: 773,
      };
      const code = Object.hasOwn(codes, value.code) ? codes[value.code] : undefined;
      if (code === undefined) return false;
      if (value.sqliteCode !== undefined && value.sqliteCode !== 5) return false;
      if (value.extendedCode !== undefined &&
          (code === 5 ? ![5, 261, 517, 773].includes(value.extendedCode) : value.extendedCode !== code)) return false;
      return value.cause === undefined || visit(value.cause, depth + 1);
    } finally {
      ancestors.delete(value);
    }
  };
  try { return visit(error, 0); }
  catch { return false; } // Caller-created Error getters are not retry authority.
}

/** Internal scheduler. Never returns while an attempt/rollback is outstanding. */
export async function runTransactionRetry<T>(
  attempt: (signal: AbortSignal, info: TransactionRetryAttempt, recovery: RetryRecovery) => Promise<T>,
  options: ResolvedTransactionRetryOptions,
): Promise<T> {
  const timeout = new AbortController();
  const timeoutReason = new Error("FrankenSQLite transaction retry deadline expired");
  const signal = AbortSignal.any(options.signal === undefined ? [timeout.signal] : [options.signal, timeout.signal]);
  const deadline = performance.now() + options.timeoutMs;
  let attempts = 0;
  let lastError: unknown;
  const timer = setTimeout(() => timeout.abort(timeoutReason), options.timeoutMs);
  const check = (): void => {
    if (performance.now() >= deadline && !timeout.signal.aborted) timeout.abort(timeoutReason);
    if (!signal.aborted) return;
    if (signal.reason === timeoutReason) {
      throw new FrankenTransactionRetryError("ERR_FSQLITE_TRANSACTION_RETRY_TIMEOUT",
        "Transaction retry deadline expired; the active attempt has been joined", attempts,
        lastError === undefined ? undefined : { cause: lastError }, lastError);
    }
    throw new FrankenTransactionRetryError("ERR_FSQLITE_TRANSACTION_RETRY_CANCELLED",
      "Transaction retry was cancelled; the active attempt has been joined", attempts,
      { cause: signal.reason }, lastError);
  };
  try {
    while (true) {
      check();
      const recovery: RetryRecovery = { recovered: false, retryAllowed: false };
      attempts++;
      try {
        // Do not recheck the signal after success: an acknowledged COMMIT wins
        // over an abort/deadline that arrived after its dispatch.
        return await attempt(signal, Object.freeze({ attempt: attempts, maxAttempts: options.maxAttempts }), recovery);
      } catch (error: unknown) {
        // Failed cleanup/uncertain outcomes remain authoritative even if abort
        // fired. Never hide them under a convenient cancellation/timeout error.
        if (!recovery.recovered) throw error;
        if (signal.aborted && error instanceof FrankenSQLiteError &&
            error.code === "ERR_FSQLITE_TRANSACTION_CANCELLED") {
          lastError = error;
          check();
        }
        if (!recovery.retryAllowed || !isTransactionConflict(error)) throw error;
        lastError = error;
        check();
        if (attempts >= options.maxAttempts) throw error;
      }
      const cap = Math.min(options.maxDelayMs, options.initialDelayMs * 2 ** (attempts - 1));
      const delay = Math.min(Math.floor(Math.random() * cap), Math.max(0, deadline - performance.now()));
      // Even zero-delay retries yield a TASK, so cancellation and other worker
      // messages cannot be starved by resolved-Promise retry loops.
      await new Promise<void>((resolve) => {
        let wake: ReturnType<typeof setTimeout>;
        const finish = (): void => {
          clearTimeout(wake);
          signal.removeEventListener("abort", finish);
          resolve();
        };
        wake = setTimeout(finish, delay);
        signal.addEventListener("abort", finish, { once: true });
        if (signal.aborted) finish();
      });
    }
  } finally {
    clearTimeout(timer);
  }
}
