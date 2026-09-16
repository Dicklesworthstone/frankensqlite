import type { SerializedFrankenError } from "@frankensqlite/worker";

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
    super(error.message, error.cause === undefined
      ? undefined : { cause: new FrankenSQLiteError(error.cause) });
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
