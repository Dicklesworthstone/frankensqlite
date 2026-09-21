/** Retained prepared handles, separate from the in-flight request budget. */
export interface PreparedStatementLimits {
  /** Retained plus currently preparing statements, 1..4096. */
  maxStatements: number;
  /** Accounted SQL/binding/column metadata bytes, 256..1 GiB. Not heap/RSS. */
  maxBytes: number;
}

export interface PreparedStatementStats extends Readonly<PreparedStatementLimits> {
  readonly statements: number;
  readonly bytes: number;
  readonly rejectedStatements: number;
}

export const DEFAULT_PREPARED_STATEMENT_LIMITS: Readonly<PreparedStatementLimits> = Object.freeze({
  maxStatements: 256,
  maxBytes: 16 * 1024 * 1024,
});

export class PreparedStatementError extends Error {
  readonly userRecoverable = true;
  readonly transient: boolean;
  readonly suggestion: string;

  constructor(
    readonly code:
      | "ERR_FSQLITE_STATEMENT_INPUT"
      | "ERR_FSQLITE_STATEMENT_LIMIT"
      | "ERR_FSQLITE_STATEMENT_TOO_LARGE"
      | "ERR_FSQLITE_STATEMENT_METADATA",
    message: string,
  ) {
    super(message);
    this.name = "PreparedStatementError";
    this.transient = code === "ERR_FSQLITE_STATEMENT_LIMIT";
    this.suggestion = this.transient
      ? "Finalize an unused prepared statement before preparing another; no statement was published."
      : "Check the statement or preparedStatementLimits; no statement was published.";
  }
}

export function resolvePreparedStatementLimits(
  options: Partial<PreparedStatementLimits> = {},
): Readonly<PreparedStatementLimits> {
  if (typeof options !== "object" || options === null || Array.isArray(options)) {
    throw new PreparedStatementError(
      "ERR_FSQLITE_STATEMENT_INPUT",
      "preparedStatementLimits must be an object",
    );
  }
  const maxStatements = options.maxStatements ?? DEFAULT_PREPARED_STATEMENT_LIMITS.maxStatements;
  const maxBytes = options.maxBytes ?? DEFAULT_PREPARED_STATEMENT_LIMITS.maxBytes;
  if (!Number.isSafeInteger(maxStatements) || maxStatements < 1 || maxStatements > 4096) {
    throw new PreparedStatementError(
      "ERR_FSQLITE_STATEMENT_INPUT",
      "maxStatements must be an integer in 1..4096",
    );
  }
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 256 || maxBytes > 1024 ** 3) {
    throw new PreparedStatementError(
      "ERR_FSQLITE_STATEMENT_INPUT",
      "maxBytes must be an integer in 256..1073741824",
    );
  }
  return Object.freeze({ maxStatements, maxBytes });
}

export interface StatementReservation {
  /** Charge additional retained metadata before constructing its owned copy. */
  grow(bytes: number): void;
  /** Idempotent; call only after preparation/finalization actually settles. */
  release(): void;
}

/**
 * Accounting contract: 256 bytes per handle, both requested/core SQL at two
 * bytes per code unit, 16 per parameter slot, and 16 + UTF-16 bytes per column
 * name. Native query plans, execution memory and database storage are NOT
 * measured here. No live statement is evicted to make room for another.
 */
export class PreparedStatementBudget {
  readonly limits: Readonly<PreparedStatementLimits>;
  #statements = 0;
  #bytes = 0;
  #rejected = 0;

  constructor(options: Partial<PreparedStatementLimits> = {}) {
    this.limits = resolvePreparedStatementLimits(options);
  }

  get stats(): PreparedStatementStats {
    return Object.freeze({
      ...this.limits,
      statements: this.#statements,
      bytes: this.#bytes,
      rejectedStatements: this.#rejected,
    });
  }

  reserve(sql: string): StatementReservation {
    let size = 256 + sql.length * 2;
    let released = false;
    let refused = false;
    const refuse = (
      code: "ERR_FSQLITE_STATEMENT_LIMIT" | "ERR_FSQLITE_STATEMENT_TOO_LARGE",
      message: string,
    ): never => {
      if (!refused) this.#rejected = Math.min(this.#rejected + 1, Number.MAX_SAFE_INTEGER);
      refused = true;
      throw new PreparedStatementError(code, message);
    };
    if (!Number.isSafeInteger(size) || size > this.limits.maxBytes) {
      refuse(
        "ERR_FSQLITE_STATEMENT_TOO_LARGE",
        "Prepared statement exceeds its metadata byte limit",
      );
    }
    if (
      this.#statements >= this.limits.maxStatements ||
      size > this.limits.maxBytes - this.#bytes
    ) {
      refuse("ERR_FSQLITE_STATEMENT_LIMIT", "Prepared statement capacity is occupied");
    }
    this.#statements++;
    this.#bytes += size;
    return {
      grow: (bytes) => {
        if (released || refused) throw new Error("Prepared statement reservation is not active");
        if (!Number.isSafeInteger(bytes) || bytes < 0 || bytes > this.limits.maxBytes - size) {
          refuse(
            "ERR_FSQLITE_STATEMENT_TOO_LARGE",
            "Prepared statement exceeds its metadata byte limit",
          );
        }
        if (bytes > this.limits.maxBytes - this.#bytes) {
          refuse("ERR_FSQLITE_STATEMENT_LIMIT", "Prepared statement metadata capacity is occupied");
        }
        size += bytes;
        this.#bytes += bytes;
      },
      release: () => {
        if (released) return;
        released = true;
        this.#statements--;
        this.#bytes -= size;
      },
    };
  }
}
