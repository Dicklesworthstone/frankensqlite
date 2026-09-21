import type { InitConfig, InitResult, PersistenceMode } from "./protocol";
import { isSnapshotPersistenceMode } from "./protocol";

export class UnsupportedPersistenceModeError extends Error {
  readonly code = "ERR_FSQLITE_UNSUPPORTED_PERSISTENCE";
  readonly persistence: PersistenceMode;

  constructor(persistence: PersistenceMode) {
    super(
      `FrankenSQLite browser persistence mode \`${persistence}\` is not implemented yet; use \`memory\` for now.`,
    );
    this.name = "UnsupportedPersistenceModeError";
    this.persistence = persistence;
  }
}

export function resolvePersistenceMode(persistence: PersistenceMode | undefined): PersistenceMode {
  return persistence ?? "memory";
}

export function resolveDatabasePath(config: InitConfig): string {
  return config.dbName ?? ":memory:";
}

export function createReadyResult(config: InitConfig): InitResult {
  return {
    path: resolveDatabasePath(config),
    persistence: resolvePersistenceMode(config.persistence),
  };
}

export function assertSupportedPersistenceMode(persistence: PersistenceMode): void {
  if (persistence !== "memory" && !isSnapshotPersistenceMode(persistence)) {
    throw new UnsupportedPersistenceModeError(persistence);
  }
}
