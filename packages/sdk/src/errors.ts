import type { SerializedFrankenError } from "@frankensqlite/worker";

/** Decode the wire error tree before constructing Error instances. */
export function decodeFrankenError(value: unknown): FrankenSQLiteError {
  let remaining = 64;
  const ancestors = new Set<object>();
  function capture(value: unknown, depth: number): SerializedFrankenError {
    if (
      depth > 8 ||
      --remaining < 0 ||
      typeof value !== "object" ||
      value === null ||
      Array.isArray(value) ||
      ancestors.has(value)
    ) {
      throw new TypeError("Invalid or excessive worker error tree");
    }
    const source = value as Record<string, unknown>;
    const code = source.code,
      message = source.message;
    if (typeof code !== "string" || code.length === 0 || typeof message !== "string") {
      throw new TypeError("Worker error requires a code and message");
    }
    const result: SerializedFrankenError = { code, message };
    for (const key of ["sqliteCode", "extendedCode", "batchIndex"] as const) {
      const field = source[key];
      if (field !== undefined) {
        if (
          typeof field !== "number" ||
          !Number.isSafeInteger(field) ||
          (key === "batchIndex" && field < 0)
        ) {
          throw new TypeError(`Invalid worker error ${key}`);
        }
        result[key] = field;
      }
    }
    for (const key of ["transient", "userRecoverable"] as const) {
      const field = source[key];
      if (field !== undefined) {
        if (typeof field !== "boolean") throw new TypeError(`Invalid worker error ${key}`);
        result[key] = field;
      }
    }
    for (const key of ["suggestion", "stack"] as const) {
      const field = source[key];
      if (field !== undefined) {
        if (typeof field !== "string") throw new TypeError(`Invalid worker error ${key}`);
        result[key] = field;
      }
    }
    ancestors.add(value);
    const cause = source.cause,
      cleanup = source.cleanupErrors;
    if (cause !== undefined) result.cause = capture(cause, depth + 1);
    if (cleanup !== undefined) {
      if (!Array.isArray(cleanup) || cleanup.length > remaining)
        throw new TypeError("Invalid worker cleanup errors");
      result.cleanupErrors = [];
      // Indexed capture rejects holes and avoids a caller-defined array iterator.
      for (let i = 0; i < cleanup.length; i++)
        result.cleanupErrors.push(capture(cleanup[i], depth + 1));
    }
    ancestors.delete(value);
    return result;
  }
  return new FrankenSQLiteError(capture(value, 0));
}

export class FrankenSQLiteError extends Error {
  readonly code: string;
  readonly sqliteCode?: number;
  readonly extendedCode?: number;
  readonly transient?: boolean;
  readonly userRecoverable?: boolean;
  readonly suggestion?: string;
  readonly batchIndex?: number;
  readonly cleanupErrors: readonly FrankenSQLiteError[];

  constructor(error: SerializedFrankenError) {
    super(
      error.message,
      error.cause === undefined ? undefined : { cause: new FrankenSQLiteError(error.cause) },
    );
    this.name = "FrankenSQLiteError";
    this.code = error.code;
    this.cleanupErrors = (error.cleanupErrors ?? []).map((item) => new FrankenSQLiteError(item));
    if (error.batchIndex !== undefined) {
      this.batchIndex = error.batchIndex;
    }
    if (error.sqliteCode !== undefined) {
      this.sqliteCode = error.sqliteCode;
    }
    if (error.extendedCode !== undefined) {
      this.extendedCode = error.extendedCode;
    }
    if (error.transient !== undefined) {
      this.transient = error.transient;
    }
    if (error.userRecoverable !== undefined) {
      this.userRecoverable = error.userRecoverable;
    }
    if (error.suggestion !== undefined) {
      this.suggestion = error.suggestion;
    }
    if (error.stack) {
      this.stack = error.stack;
    }
  }
}
