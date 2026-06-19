import type { SerializedFrankenError } from "@frankensqlite/worker";

export class FrankenSQLiteError extends Error {
  readonly code: string;
  readonly sqliteCode?: number;
  readonly extendedCode?: number;
  readonly transient?: boolean;
  readonly userRecoverable?: boolean;
  readonly suggestion?: string;

  constructor(error: SerializedFrankenError) {
    super(error.message);
    this.name = "FrankenSQLiteError";
    this.code = error.code;
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
