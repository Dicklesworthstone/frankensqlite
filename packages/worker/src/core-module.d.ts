declare module "@frankensqlite/core" {
  export type CoreSqlScalar =
    | null
    | string
    | number
    | bigint
    | boolean
    | Uint8Array;

  export interface CoreQueryResult<Row extends Record<string, unknown> = Record<string, unknown>> {
    columns: string[];
    columnCount: number;
    columnTypes: string[];
    rows: Row[];
    rowArrays: CoreSqlScalar[][];
    changes: number;
  }

  export class FrankenPreparedStatement {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    readonly sql: string;
    readonly columnCount: number;
    columnNames(): string[];
    execute(): Promise<number>;
    executeWithParams(params: unknown[]): Promise<number>;
    query(): Promise<CoreQueryResult>;
    queryWithParams(params: unknown[]): Promise<CoreQueryResult>;
    explain(): Promise<string>;
  }

  export class FrankenDB {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    static create(name?: string | null): Promise<FrankenDB>;
    static open(name?: string | null): Promise<FrankenDB>;
    static openWithOptions(
      name?: string | null,
      options?: unknown | null,
    ): Promise<FrankenDB>;
    static import(data: Uint8Array): Promise<FrankenDB>;
    static importWithOptions(
      data: Uint8Array,
      options?: unknown | null,
    ): Promise<FrankenDB>;
    readonly path: string;
    close(): void;
    execute(sql: string): Promise<number>;
    executeBatch(sql: string): Promise<void>;
    executeWithParams(sql: string, params: unknown[]): Promise<number>;
    query(sql: string): Promise<CoreQueryResult>;
    queryWithParams(sql: string, params: unknown[]): Promise<CoreQueryResult>;
    pragma(pragma: string): Promise<CoreQueryResult>;
    prepare(sql: string): Promise<FrankenPreparedStatement>;
    export(): Promise<Uint8Array>;
    explain(sql: string): Promise<string>;
    memoryStats(): unknown;
  }

  export function init(): void;
  export function parseSql(input: string): unknown;

  export type InitInput =
    | RequestInfo
    | URL
    | Response
    | BufferSource
    | WebAssembly.Module;

  export default function init(
    moduleOrPath?:
      | { module_or_path: InitInput | Promise<InitInput> }
      | InitInput
      | Promise<InitInput>,
  ): Promise<unknown>;
}
