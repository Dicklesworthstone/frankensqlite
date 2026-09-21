import type { SqlBindings, SqlScalar } from "./protocol";

/** Mirrors the engine's SQLite parameter numbering; never rewrites SQL text. */
export const MAX_BIND_PARAMETERS = 32_766;

export class BindingError extends Error {
  readonly transient = false;
  readonly userRecoverable = true;
  constructor(
    readonly code:
      | "ERR_FSQLITE_BINDING_INPUT"
      | "ERR_FSQLITE_BINDING_ARITY"
      | "ERR_FSQLITE_BINDING_NAME",
    message: string,
  ) {
    super(message);
    this.name = "BindingError";
  }
}

export interface ParameterLayout {
  /** Largest 1-based slot, including unused holes introduced by ?NNN. */
  readonly count: number;
  readonly names: readonly (string | null)[];
  readonly slots: readonly number[];
  /** Exact spelling to slot, including numbered aliases such as ?01 and ?1. */
  readonly aliases: Readonly<Record<string, number>>;
  readonly bare: Readonly<Record<string, number | null>>;
}

function inputError(message: string): never {
  throw new BindingError("ERR_FSQLITE_BINDING_INPUT", message);
}

function identifier(code: number): boolean {
  return (
    code >= 128 ||
    (code >= 65 && code <= 90) ||
    (code >= 97 && code <= 122) ||
    (code >= 48 && code <= 57) ||
    code === 95 ||
    code === 36
  );
}

/** End of a :/@/$ token, including its opaque Tcl-style suffix. */
export function namedParameterEnd(sql: string, start: number): number {
  let i = start + 1,
    characters = 0;
  while (i < sql.length) {
    if (identifier(sql.charCodeAt(i))) {
      i++;
      characters++;
    } else if (sql.startsWith("::", i)) i += 2;
    else break;
  }
  if (characters === 0) inputError("Named SQL parameters require a name");
  if (sql[i] === "(") {
    i++;
    while (i < sql.length && sql[i] !== ")" && !/[\t\n\v\f\r ]/.test(sql[i]!)) i++;
    if (sql[i] !== ")") inputError("Unterminated or whitespace-containing parameter suffix");
    i++;
  }
  return i;
}

/**
 * Lexical bind layout, not a SQL parser. Strings, identifiers and comments are
 * skipped; the core still validates statement syntax and owns query semantics.
 * Layouts are for ONE statement. Script execution retains its existing API.
 */
export function parameterLayout(sql: string): ParameterLayout {
  if (typeof sql !== "string" || sql.includes("\0"))
    inputError("SQL must be a string without NUL bytes");
  const names: (string | null)[] = [];
  const aliases: Record<string, number> = Object.create(null);
  const slots = new Set<number>();
  let count = 0;
  for (let i = 0; i < sql.length; ) {
    const c = sql[i]!;
    if (c === "\uFEFF") {
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
    if (c === "'" || c === '"' || c === "`" || c === "[") {
      const end = c === "[" ? "]" : c;
      let closed = false;
      for (i++; i < sql.length; i++) {
        if (sql[i] !== end) continue;
        if (c !== "[" && sql[i + 1] === end) {
          i++;
          continue;
        }
        i++;
        closed = true;
        break;
      }
      if (!closed) inputError("Unterminated quoted value or identifier in SQL");
      continue;
    }
    if (c !== "?" && c !== ":" && c !== "@" && c !== "$") {
      // '$' is legal WITHIN an unquoted identifier, not a separate bind token.
      if (identifier(sql.charCodeAt(i))) {
        for (i++; i < sql.length && identifier(sql.charCodeAt(i)); i++);
      } else i++;
      continue;
    }
    const start = i++;
    let slot: number;
    let name: string | null = null;
    if (c === "?") {
      while (i < sql.length && sql.charCodeAt(i) >= 48 && sql.charCodeAt(i) <= 57) i++;
      if (i > start + 1) {
        name = sql.slice(start, i);
        slot = Number(name.slice(1));
      } else slot = count + 1;
    } else {
      // SQLite's named-token grammar also accepts Tcl :: segments and a
      // whitespace-free parenthesized suffix. All three sigils use it.
      i = namedParameterEnd(sql, start);
      name = sql.slice(start, i);
      slot = aliases[name] ?? count + 1;
    }
    if (!Number.isSafeInteger(slot) || slot < 1 || slot > MAX_BIND_PARAMETERS) {
      inputError(`Parameter index must be in 1..${MAX_BIND_PARAMETERS}`);
    }
    while (names.length < slot) names.push(null);
    if (name !== null) {
      aliases[name] = slot;
      names[slot - 1] ??= name;
    }
    count = Math.max(count, slot);
    slots.add(slot);
  }
  const bare: Record<string, number | null> = Object.create(null);
  for (const name of Object.keys(aliases)) {
    if (name[0] === "?") continue;
    const key = name.slice(1),
      slot = aliases[name]!;
    if (!Object.hasOwn(bare, key)) bare[key] = slot;
    else if (bare[key] !== slot) bare[key] = null;
  }
  return Object.freeze({
    count,
    names: Object.freeze(names),
    slots: Object.freeze([...slots]),
    aliases: Object.freeze(aliases),
    bare: Object.freeze(bare),
  });
}

export function validateBindingScalar(value: unknown): asserts value is SqlScalar {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "boolean" ||
    typeof value === "number" ||
    value instanceof Uint8Array
  )
    return;
  if (typeof value === "bigint" && value >= -(1n << 63n) && value < 1n << 63n) return;
  inputError("Bindings must be SQL scalars; integers must fit signed 64-bit values");
}

export function isNamedBindings(value: unknown): value is Readonly<Record<string, SqlScalar>> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === null || prototype === Object.prototype;
}

/** Complete binding, never a partial update of values from an earlier call. */
export function resolveBindings(layout: ParameterLayout, values: SqlBindings): SqlScalar[] {
  if (Array.isArray(values)) {
    if (values.length !== layout.count) {
      throw new BindingError(
        "ERR_FSQLITE_BINDING_ARITY",
        `Expected ${layout.count} positional bindings, received ${values.length}`,
      );
    }
    const result: SqlScalar[] = [];
    for (let i = 0; i < values.length; i++) {
      const value: unknown = values[i];
      validateBindingScalar(value);
      result.push(value);
    }
    return result;
  }
  if (!isNamedBindings(values))
    inputError("Bindings must be a positional array or a plain named-parameter object");
  const result: SqlScalar[] = new Array<SqlScalar>(layout.count).fill(null);
  const assigned = new Set<number>();
  for (const key of Object.keys(values)) {
    let slot = layout.aliases[key];
    if (slot === undefined && !/^[?:@$]/.test(key)) {
      const candidate = layout.bare[key];
      if (candidate === null) {
        throw new BindingError(
          "ERR_FSQLITE_BINDING_NAME",
          `Ambiguous binding ${JSON.stringify(key)}; include its SQL prefix`,
        );
      }
      slot = candidate;
    }
    if (slot === undefined)
      throw new BindingError("ERR_FSQLITE_BINDING_NAME", `Unknown binding ${JSON.stringify(key)}`);
    if (assigned.has(slot))
      throw new BindingError(
        "ERR_FSQLITE_BINDING_NAME",
        `Multiple bindings target parameter slot ${slot}`,
      );
    const value: unknown = values[key];
    validateBindingScalar(value);
    result[slot - 1] = value;
    assigned.add(slot);
  }
  for (const slot of layout.slots) {
    if (!assigned.has(slot))
      throw new BindingError(
        "ERR_FSQLITE_BINDING_ARITY",
        `Missing binding for ${layout.names[slot - 1] ?? `anonymous parameter ${slot}`}`,
      );
  }
  return result;
}
