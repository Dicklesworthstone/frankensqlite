import type {
  ChangesetDeliveryOptions, ChangesetDeliveryReceipt, ChangesetEnvelope,
  ChangesetReceiver, ChangesetTransport,
} from "./changeset-delivery";

// Type-checked against the delivery protocol without loading the SQL runtime in
// a transport-only client. Changing that protocol requires a wire-version review.
const PROTOCOL: ChangesetEnvelope["protocol"] = "fsqlite-changeset-v1";
export const CHANGESET_HTTP_CONTENT_TYPE = "application/vnd.fsqlite.changeset.v1";
export const CHANGESET_HTTP_RECEIPT_TYPE = "application/vnd.fsqlite.changeset-receipt.v1+json";
const MAX_PAYLOAD = 64 * 1024 * 1024;
const MAX_METADATA = 8192;
const MAX_RECEIPT = 8192;
const utf8 = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
const encoder = new TextEncoder();

type FailureCode = "INPUT" | "LIMIT" | "PROTOCOL" | "HTTP" | "NETWORK" | "CANCELLED" | "TIMEOUT";
export class ChangesetHttpError extends Error {
  readonly code: `ERR_FSQLITE_HTTP_${FailureCode}`;
  constructor(kind: FailureCode, message: string,
    /** Once fetch is dispatched, a missing/invalid response does NOT prove rollback. */
    readonly outcome: "not-sent" | "unknown" = "not-sent",
    readonly status?: number, options?: ErrorOptions) {
    super(message, options); this.name = "ChangesetHttpError";
    this.code = `ERR_FSQLITE_HTTP_${kind}`;
  }
}
function fail(kind: FailureCode, message: string): never { throw new ChangesetHttpError(kind, message); }
function bound(value: unknown, fallback: number, maximum: number): number {
  const n = value === undefined ? fallback : value;
  if (typeof n !== "number" || !Number.isSafeInteger(n) || n < 1 || n > maximum) fail("INPUT", `Expected an integer in 1..${maximum}`);
  return n;
}
function field(record: unknown, key: string): unknown {
  if (typeof record !== "object" || record === null || Array.isArray(record)) fail("PROTOCOL", "Expected an HTTP delivery record");
  const d = Object.getOwnPropertyDescriptor(record, key);
  if (d === undefined || !Object.hasOwn(d, "value")) fail("PROTOCOL", "HTTP delivery fields must be own data properties");
  return d.value;
}
function identity(value: unknown, maximum: number): string {
  if (typeof value !== "string" || !value.length || value.length > maximum || value.includes("\0")) fail("PROTOCOL", "Invalid delivery identity");
  const bytes = encoder.encode(value);
  if (bytes.length > maximum || utf8.decode(bytes) !== value) fail("PROTOCOL", "Invalid identity encoding");
  return value;
}
function digest(value: unknown): string {
  if (typeof value !== "string" || !/^[0-9a-f]{64}$/.test(value)) fail("PROTOCOL", "Invalid SHA-256 digest");
  return value;
}
function integer(value: unknown, maximum: number): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0 || value > maximum) fail("PROTOCOL", "Invalid delivery counter");
  return value;
}
function ownedBytes(value: unknown, maximum: number): Uint8Array {
  if (!(value instanceof Uint8Array)) fail("INPUT", "A changeset Uint8Array is required");
  const proto = Object.getPrototypeOf(Uint8Array.prototype) as object;
  const get = (key: string): unknown => Object.getOwnPropertyDescriptor(proto, key)!.get!.call(value);
  const buffer = get("buffer"), offset = get("byteOffset") as number, length = get("byteLength") as number;
  if (!(buffer instanceof ArrayBuffer) || Object.getOwnPropertyDescriptor(ArrayBuffer.prototype, "resizable")?.get?.call(buffer)) fail("INPUT", "Use fixed, non-shared changeset buffers");
  if (length > maximum) fail("LIMIT", "Changeset exceeds the HTTP message budget");
  return new Uint8Array(new Uint8Array(buffer, offset, length));
}
interface Metadata {
  readonly protocol: typeof PROTOCOL;
  readonly receiverId: string;
  readonly deliveryId: string;
  readonly sha256: string;
  readonly byteLength: number;
}
function metadata(value: unknown, maximum: number): Metadata {
  if (field(value, "protocol") !== PROTOCOL) fail("PROTOCOL", "Unsupported changeset protocol");
  return Object.freeze({ protocol: PROTOCOL, receiverId: identity(field(value, "receiverId"), 256),
    deliveryId: identity(field(value, "deliveryId"), 512), sha256: digest(field(value, "sha256")),
    byteLength: integer(field(value, "byteLength"), maximum) });
}
function encode(message: ChangesetEnvelope, maximum: number) {
  const bytes = ownedBytes(field(message, "changeset"), maximum);
  const meta = metadata({ protocol: field(message, "protocol"), receiverId: field(message, "receiverId"),
    deliveryId: field(message, "deliveryId"), sha256: field(message, "sha256"), byteLength: bytes.length }, maximum);
  const json = encoder.encode(JSON.stringify(meta));
  if (json.length > MAX_METADATA) fail("LIMIT", "HTTP delivery metadata is too large");
  const wire = new Uint8Array(8 + json.length + bytes.length);
  wire.set([70, 67, 68, 49]); // FCD1; uint32 BE JSON length; JSON; unmodified binary payload.
  new DataView(wire.buffer).setUint32(4, json.length);
  wire.set(json, 8); wire.set(bytes, 8 + json.length);
  return { meta, wire };
}
function parseJson(bytes: Uint8Array): unknown {
  try { return JSON.parse(utf8.decode(bytes)); }
  catch { return fail("PROTOCOL", "Invalid HTTP delivery JSON or UTF-8"); }
}
function decode(wire: Uint8Array, maximum: number): ChangesetEnvelope {
  if (wire.length < 8 || wire[0] !== 70 || wire[1] !== 67 || wire[2] !== 68 || wire[3] !== 49) fail("PROTOCOL", "Invalid HTTP delivery frame");
  const length = new DataView(wire.buffer, wire.byteOffset, wire.byteLength).getUint32(4);
  if (length === 0 || length > MAX_METADATA || length > wire.length - 8) fail("PROTOCOL", "Invalid HTTP metadata length");
  const meta = metadata(parseJson(wire.subarray(8, 8 + length)), maximum);
  if (wire.length - 8 - length !== meta.byteLength) fail("PROTOCOL", "HTTP payload length does not match metadata");
  return Object.freeze({ protocol: PROTOCOL, receiverId: meta.receiverId, deliveryId: meta.deliveryId,
    sha256: meta.sha256, changeset: wire.subarray(8 + length) });
}
function receipt(value: unknown, meta: Metadata): ChangesetDeliveryReceipt {
  if (field(value, "protocol") !== PROTOCOL || field(value, "receiverId") !== meta.receiverId ||
      field(value, "deliveryId") !== meta.deliveryId || field(value, "sha256") !== meta.sha256 ||
      field(value, "byteLength") !== meta.byteLength || field(value, "confirmed") !== true) fail("PROTOCOL", "The HTTP receipt does not confirm this exact delivery");
  const applied = integer(field(value, "applied"), 100_000), omitted = integer(field(value, "omitted"), 100_000);
  const replayed = field(value, "replayed");
  if (applied + omitted > 100_000 || typeof replayed !== "boolean") fail("PROTOCOL", "Invalid HTTP receipt decision");
  return Object.freeze({ ...meta, confirmed: true, applied, omitted, replayed });
}
class Budget {
  readonly signal: AbortSignal;
  readonly #controller = new AbortController();
  readonly #reason = new Error("HTTP delivery deadline expired");
  readonly #deadline: number | undefined;
  #timer: ReturnType<typeof setTimeout> | undefined;
  #stop: (() => void) | undefined;
  constructor(options: ChangesetDeliveryOptions = {}, defaultTimeout = 30_000) {
    const external = options.signal;
    if (external !== undefined) {
      try { Object.getOwnPropertyDescriptor(AbortSignal.prototype, "aborted")!.get!.call(external); }
      catch { fail("INPUT", "signal must be an AbortSignal"); }
    }
    const timeout = bound(options.timeoutMs, defaultTimeout, 2_147_483_647);
    this.#deadline = performance.now() + timeout;
    this.signal = this.#controller.signal;
    if (external !== undefined) {
      const abort = () => { this.#controller.abort(external.reason); };
      external.addEventListener("abort", abort, { once: true });
      this.#stop = () => { external.removeEventListener("abort", abort); };
      if (external.aborted) abort();
    }
    this.#arm();
  }
  #expire(): void {
    if (this.#deadline !== undefined && performance.now() >= this.#deadline && !this.signal.aborted) this.#controller.abort(this.#reason);
  }
  #arm(): void {
    if (this.signal.aborted || this.#deadline === undefined) return;
    this.#timer = setTimeout(() => { this.#expire(); this.#arm(); }, Math.max(1, Math.ceil(this.#deadline - performance.now())));
  }
  checkpoint(): void {
    this.#expire();
    if (this.signal.aborted) throw new ChangesetHttpError(this.signal.reason === this.#reason ? "TIMEOUT" : "CANCELLED",
      "HTTP delivery interrupted; a dispatched request may have committed", "not-sent", undefined, { cause: this.signal.reason });
  }
  remaining(): number { this.checkpoint(); return Math.max(1, Math.ceil(this.#deadline! - performance.now())); }
  finish(): void { clearTimeout(this.#timer); this.#stop?.(); }
}
function declaredLength(headers: Headers, maximum: number): number | null {
  const encoding = headers.get("content-encoding");
  if (encoding !== null && encoding.toLowerCase() !== "identity") fail("PROTOCOL", "Content encoding is not supported on the changeset endpoint");
  const value = headers.get("content-length");
  if (value === null) return null;
  if (!/^(0|[1-9][0-9]*)$/.test(value)) fail("PROTOCOL", "Invalid HTTP content length");
  const length = Number(value);
  if (!Number.isSafeInteger(length) || length > maximum) fail("LIMIT", "HTTP body exceeds its byte budget");
  return length;
}
/** Bound actual stream bytes even when Content-Length is absent or dishonest. */
async function readBody(body: ReadableStream<Uint8Array> | null, headers: Headers, maximum: number, budget: Budget): Promise<Uint8Array> {
  let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
  let cancelling: Promise<void> | undefined;
  let finished = false;
  const cancel = () => {
    if (reader !== undefined) cancelling ??= reader.cancel().catch(() => {});
  };
  try {
    budget.checkpoint();
    const expected = declaredLength(headers, maximum);
    if (body === null) { if (expected !== null && expected !== 0) fail("PROTOCOL", "Missing HTTP body"); return new Uint8Array(); }
    reader = body.getReader();
    budget.signal.addEventListener("abort", cancel, { once: true });
    if (budget.signal.aborted) cancel();
    let output = new Uint8Array(Math.min(4096, maximum)), used = 0;
    while (true) {
      budget.checkpoint();
      const item = await reader.read(); budget.checkpoint();
      if (item.done) { finished = true; break; }
      const chunk = item.value;
      if (!(chunk instanceof Uint8Array)) fail("PROTOCOL", "HTTP body is not a byte stream");
      if (chunk.byteLength > maximum - used || (expected !== null && chunk.byteLength > expected - used)) fail("LIMIT", "HTTP body exceeds its declared or configured byte budget");
      if (used + chunk.byteLength > output.length) {
        const grown = new Uint8Array(Math.min(maximum, Math.max(used + chunk.byteLength, output.length * 2)));
        grown.set(output.subarray(0, used)); output = grown;
      }
      output.set(chunk, used); used += chunk.byteLength;
    }
    if (expected !== null && used !== expected) fail("PROTOCOL", "Truncated HTTP body");
    return output.subarray(0, used);
  } finally {
    budget.signal.removeEventListener("abort", cancel);
    if (!finished) {
      if (reader !== undefined) cancel();
      else if (body !== null && !body.locked) cancelling = body.cancel().catch(() => {});
    }
    await cancelling;
    reader?.releaseLock();
  }
}
function contentType(headers: Headers, expected: string): boolean {
  return headers.get("content-type")?.trim().toLowerCase() === expected;
}
function endpoint(input: string | URL, insecure: boolean): string {
  let url: URL;
  try { url = new URL(String(input)); } catch { return fail("INPUT", "Use an absolute changeset endpoint URL"); }
  const local = ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname);
  if (url.href.length > 8192 || url.username || url.password || url.hash ||
      (url.protocol !== "https:" && !(insecure && local && url.protocol === "http:"))) fail("INPUT", "Use HTTPS without URL credentials or fragments; development HTTP is loopback-only");
  return url.href;
}

export interface ChangesetHttpTransportOptions {
  maxMessageBytes?: number;
  /** Default 30 seconds. Pump-supplied remaining budgets can only shorten it. */
  timeoutMs?: number;
  /** Explicit development-only exception for localhost/127.0.0.1/[::1]. */
  allowInsecureLoopback?: boolean;
  /** Trusted credentials provider. Does not receive payload bytes. Cookies are omitted. */
  headers?: (delivery: Readonly<Omit<ChangesetEnvelope, "changeset">>, signal: AbortSignal) => HeadersInit | Promise<HeadersInit>;
  /** Defaults to global fetch. Custom implementations must honor Request policies. */
  fetch?: typeof globalThis.fetch;
}
/** Binary POST, no redirects, no automatic retries, bounded receipt parsing. */
export function createChangesetHttpTransport(url: string | URL, options: ChangesetHttpTransportOptions = {}): ChangesetTransport {
  const maximum = bound(options.maxMessageBytes, 8 * 1024 * 1024, MAX_PAYLOAD);
  const timeout = bound(options.timeoutMs, 30_000, 2_147_483_647), insecure = options.allowInsecureLoopback ?? false;
  if (typeof insecure !== "boolean") fail("INPUT", "allowInsecureLoopback must be boolean");
  const address = endpoint(url, insecure), credentials = options.headers;
  const send = options.fetch ?? globalThis.fetch?.bind(globalThis);
  if (typeof send !== "function" || (credentials !== undefined && typeof credentials !== "function")) fail("INPUT", "A fetch implementation and valid credential provider are required");
  return async (message, controls = {}) => {
    let budget: Budget | undefined, sent = false;
    let response: Response | undefined;
    try {
      const inputTimeout = controls.timeoutMs;
      const timeoutMs = inputTimeout === undefined ? timeout : Math.min(timeout, bound(inputTimeout, timeout, 2_147_483_647));
      budget = new Budget({ ...controls, timeoutMs }); budget.checkpoint();
      // Capture before invoking application code or awaiting credentials/fetch.
      const { wire, meta } = encode(message, maximum); budget.checkpoint();
      const headers = new Headers(credentials === undefined ? undefined : await credentials(meta, budget.signal));
      budget.checkpoint();
      for (const name of headers.keys()) {
        if (["content-type", "content-length", "content-encoding", "accept", "cookie", "cookie2", "host", "origin", "referer", "connection", "transfer-encoding", "trailer", "upgrade"].includes(name) || name.startsWith("sec-") || name.startsWith("proxy-")) fail("INPUT", "Credential headers may not override transport policies");
      }
      headers.set("content-type", CHANGESET_HTTP_CONTENT_TYPE); headers.set("accept", CHANGESET_HTTP_RECEIPT_TYPE);
      const request = new Request(address, { method: "POST", headers, body: wire,
        signal: budget.signal, redirect: "error", credentials: "omit", cache: "no-store", referrerPolicy: "no-referrer", mode: "cors" });
      budget.checkpoint(); sent = true;
      response = await send(request); budget.checkpoint();
      if (!(response instanceof Response) || response.redirected || response.type === "opaque" || response.type === "opaqueredirect") fail("PROTOCOL", "An inspectable, unredirected HTTP response is required");
      if (response.status !== 200) throw new ChangesetHttpError("HTTP", "The changeset endpoint did not acknowledge delivery", "unknown", response.status);
      if (!contentType(response.headers, CHANGESET_HTTP_RECEIPT_TYPE)) fail("PROTOCOL", "Unexpected changeset receipt content type");
      const bytes = await readBody(response.body, response.headers, MAX_RECEIPT, budget);
      budget.checkpoint(); return receipt(parseJson(bytes), meta);
    } catch (cause: unknown) {
      // Never return remote messages, URLs, headers or SQL errors as diagnostics.
      if (cause instanceof ChangesetHttpError) {
        throw new ChangesetHttpError(cause.code.slice("ERR_FSQLITE_HTTP_".length) as FailureCode,
          cause.message, sent ? "unknown" : "not-sent", cause.status, { cause });
      }
      try { budget?.checkpoint(); } catch (cancelled: unknown) {
        if (cancelled instanceof ChangesetHttpError) throw new ChangesetHttpError(
          cancelled.code.endsWith("TIMEOUT") ? "TIMEOUT" : "CANCELLED", cancelled.message, sent ? "unknown" : "not-sent", undefined, { cause });
      }
      throw new ChangesetHttpError(sent ? "NETWORK" : "INPUT", "HTTP delivery failed; preserve the delivery identity before retrying",
        sent ? "unknown" : "not-sent", undefined, { cause });
    } finally {
      if (response instanceof Response && response.body !== null && !response.body.locked) {
        await response.body.cancel().catch(() => {});
      }
      budget?.finish();
    }
  };
}

export interface ChangesetHttpAuthorization {
  readonly method: string;
  readonly url: string;
  /** Detached header copy; no body access is handed to authorization code. */
  readonly headers: Headers;
  readonly receiverId: string;
  readonly signal: AbortSignal;
}
export interface ChangesetHttpHandlerOptions {
  /** Required, fail-closed authentication/authorization for this fixed receiver. */
  authorize: (request: ChangesetHttpAuthorization) => boolean | Promise<boolean>;
  /** Optional sender-namespace/business policy, after framing and before SQL. */
  authorizeDelivery?: (request: ChangesetHttpAuthorization, delivery: Readonly<Omit<ChangesetEnvelope, "changeset">>) => boolean | Promise<boolean>;
  maxMessageBytes?: number;
  /** Bounds admission, authorization, upload and awaited receiver execution; default 30s. */
  timeoutMs?: number;
  /** Active authorization + upload + receive calls. Default 1; no waiting queue. */
  maxInFlight?: number;
  /** Exact additional browser origins. Same-origin and originless requests are admitted. */
  allowedOrigins?: readonly string[];
  /** Additional preflight header names; content-type and authorization are always allowed. */
  allowedHeaders?: readonly string[];
}
export type ChangesetHttpHandler = (request: Request) => Promise<Response>;

function origins(input: readonly string[] | undefined): ReadonlySet<string> {
  if (input !== undefined && (!Array.isArray(input) || input.length > 64)) fail("INPUT", "Configure at most 64 exact origins");
  const result = new Set<string>();
  for (const item of input ?? []) {
    let url: URL;
    try { url = new URL(item); } catch { return fail("INPUT", "Invalid allowed origin"); }
    if (typeof item !== "string" || item.length > 2048 || url.origin !== item ||
        !["https:", "http:"].includes(url.protocol)) fail("INPUT", "Origins must be exact HTTP(S) origins, without paths, credentials or wildcards");
    result.add(item);
  }
  return result;
}
function corsHeaders(input: readonly string[] | undefined): ReadonlySet<string> {
  if (input !== undefined && (!Array.isArray(input) || input.length > 32)) fail("INPUT", "Configure at most 32 additional CORS header names");
  const result = new Set(["authorization", "content-type"]);
  for (const header of input ?? []) {
    if (typeof header !== "string" || header.length > 128 || !/^[!#$%&'+.^_`|~0-9a-z-]+$/i.test(header)) fail("INPUT", "Invalid CORS header name");
    const name = header.toLowerCase();
    if (["cookie", "cookie2", "host", "origin", "connection", "transfer-encoding"].includes(name) || name.startsWith("sec-") || name.startsWith("proxy-")) fail("INPUT", "CORS cannot authorize ambient credentials or connection headers");
    result.add(name);
  }
  return result;
}
function httpResponse(status: number, value: unknown, origin: string | null, extra?: HeadersInit): Response {
  const headers = new Headers(extra);
  headers.set("cache-control", "no-store"); headers.set("x-content-type-options", "nosniff");
  headers.set("vary", "Origin, Access-Control-Request-Method, Access-Control-Request-Headers");
  if (origin !== null) headers.set("access-control-allow-origin", origin);
  if (status === 204) return new Response(null, { status, headers });
  headers.set("content-type", status === 200 ? CHANGESET_HTTP_RECEIPT_TYPE : "application/json");
  return new Response(JSON.stringify(value), { status, headers });
}
function errorResponse(status: number, origin: string | null, extra?: HeadersInit): Response {
  // Never serialize authentication errors, SQL errors, routing IDs or stack traces.
  // Even errors that predate this request's SQL cannot describe another attempt.
  return httpResponse(status, { error: "ERR_FSQLITE_HTTP_REJECTED", outcome: "unknown" }, origin, extra);
}

/**
 * Fetch-standard endpoint for an existing verified receiver. Its slot is held
 * through cancellation/confirmation drain; transport disconnect is NOT rollback.
 * Mount on an exact application route behind HTTPS and trusted host/proxy policy.
 */
export function createChangesetHttpHandler(receiver: Pick<ChangesetReceiver, "receiverId" | "receive">,
  options: ChangesetHttpHandlerOptions): ChangesetHttpHandler {
  const id = identity(receiver?.receiverId, 256), method = receiver?.receive;
  const authorize = options?.authorize, authorizeDelivery = options?.authorizeDelivery;
  if (typeof method !== "function" || typeof authorize !== "function" ||
      (authorizeDelivery !== undefined && typeof authorizeDelivery !== "function")) fail("INPUT", "A receiver and explicit authorization callback are required");
  const receive = method.bind(receiver);
  const maximum = bound(options.maxMessageBytes, 8 * 1024 * 1024, MAX_PAYLOAD);
  const timeout = bound(options.timeoutMs, 30_000, 2_147_483_647);
  const capacity = bound(options.maxInFlight, 1, 64);
  const allowed = origins(options.allowedOrigins), allowedHeaders = corsHeaders(options.allowedHeaders);
  let active = 0;
  return async request => {
    let admitted = false, budget: Budget | undefined, origin: string | null = null, receiving = false;
    try {
      if (!(request instanceof Request) || request.url.length > 8192) return errorResponse(400, null);
      const requestedOrigin = request.headers.get("origin");
      if (requestedOrigin !== null) {
        let parsed: URL;
        try { parsed = new URL(requestedOrigin); } catch { return errorResponse(403, null); }
        if (parsed.origin !== requestedOrigin || !["https:", "http:"].includes(parsed.protocol) ||
            (requestedOrigin !== new URL(request.url).origin && !allowed.has(requestedOrigin))) return errorResponse(403, null);
        origin = requestedOrigin;
      }
      if (request.method === "OPTIONS") {
        const names = request.headers.get("access-control-request-headers") ?? "";
        if (origin === null || request.headers.get("access-control-request-method") !== "POST" || names.length > 4096) return errorResponse(403, origin);
        const requested = names === "" ? [] : names.split(",").map(name => name.trim().toLowerCase());
        if (requested.length > 34 || requested.some(name => !allowedHeaders.has(name))) return errorResponse(403, origin);
        return httpResponse(204, null, origin, { "access-control-allow-methods": "POST",
          "access-control-allow-headers": [...new Set(requested)].join(", ") });
      }
      if (request.method !== "POST") return errorResponse(405, origin, { allow: "POST, OPTIONS" });
      if (!contentType(request.headers, CHANGESET_HTTP_CONTENT_TYPE)) return errorResponse(415, origin);
      if (active >= capacity) return errorResponse(503, origin);
      active++; admitted = true;
      budget = new Budget({ signal: request.signal, timeoutMs: timeout }); budget.checkpoint();
      const context = Object.freeze({ method: request.method, url: request.url,
        headers: new Headers(request.headers), receiverId: id, signal: budget.signal });
      let permitted: boolean;
      try { permitted = await authorize(context) === true; }
      catch { permitted = false; }
      budget.checkpoint();
      if (!permitted) return errorResponse(403, origin);
      const wire = await readBody(request.body, request.headers, 8 + MAX_METADATA + maximum, budget);
      const envelope = decode(wire, maximum); budget.checkpoint();
      if (envelope.receiverId !== id) return errorResponse(400, origin);
      const meta = Object.freeze({ protocol: envelope.protocol, receiverId: id, deliveryId: envelope.deliveryId,
        sha256: envelope.sha256, byteLength: envelope.changeset.byteLength });
      if (authorizeDelivery !== undefined) {
        try { permitted = await authorizeDelivery(context, meta) === true; }
        catch { permitted = false; }
        budget.checkpoint();
        if (!permitted) return errorResponse(403, origin);
      }
      budget.checkpoint(); receiving = true;
      // Await the real receiver, including its same-target commit confirmation.
      // Do not race SQL/confirmation against a timer or release this admission
      // slot while abandoned work could still commit on the connection.
      const result = await receive(envelope, { signal: budget.signal, timeoutMs: budget.remaining() });
      budget.checkpoint();
      return httpResponse(200, receipt(result, meta), origin);
    } catch (cause: unknown) {
      if (cause instanceof ChangesetHttpError) {
        if (cause.code.endsWith("TIMEOUT") || cause.code.endsWith("CANCELLED")) return errorResponse(408, origin);
        if (cause.code.endsWith("LIMIT")) return errorResponse(413, origin);
        return errorResponse(receiving ? 500 : 400, origin);
      }
      const code = typeof cause === "object" && cause !== null
        ? Object.getOwnPropertyDescriptor(cause, "code")?.value : undefined;
      return errorResponse(code === "ERR_FSQLITE_DELIVERY_BUSY" ? 503 : 500, origin);
    } finally {
      try {
        // A denied request never acquires a reader or buffers its payload. Cancel
        // rather than draining arbitrary unauthorized uploads into application RAM.
        if (request instanceof Request && request.body !== null && !request.body.locked) await request.body.cancel().catch(() => {});
      } finally {
        budget?.finish(); if (admitted) active--;
      }
    }
  };
}
