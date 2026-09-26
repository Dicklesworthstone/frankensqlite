import type {
  BootstrapInstallReceipt, BootstrapManifest, BootstrapOperationOptions, BootstrapProgress,
} from "./changeset-bootstrap";
import type { BootstrapTransferTransport } from "./changeset-bootstrap-transfer";

/** Bootstrap staging is not the ordinary per-changeset acknowledgement protocol. */
export const BOOTSTRAP_HTTP_PROTOCOL = "fsqlite-bootstrap-http-v1";
export const BOOTSTRAP_HTTP_CONTENT_TYPE = "application/vnd.fsqlite.bootstrap.v1";
export const BOOTSTRAP_HTTP_RESPONSE_TYPE = "application/vnd.fsqlite.bootstrap-response.v1+json";
export const BOOTSTRAP_HTTP_ACTION_HEADER = "x-fsqlite-bootstrap-action";
export type BootstrapHttpAction = "status" | "stage" | "install";
export interface BootstrapHttpRequestInfo {
  readonly action: BootstrapHttpAction;
  readonly manifest: BootstrapManifest;
  readonly index?: number;
  readonly byteLength: number;
}
export interface BootstrapHttpTransportOptions {
  maxChunkBytes?: number;
  timeoutMs?: number;
  /** Trusted source incarnation; require this exact ordered installation receipt. */
  orderedSourceId?: string;
  allowInsecureLoopback?: boolean;
  /** Explicit credentials; never receives the chunk body. No ambient cookies. */
  headers?: (request: BootstrapHttpRequestInfo, signal: AbortSignal) => HeadersInit | Promise<HeadersInit>;
  fetch?: typeof globalThis.fetch;
}
type Failure = "INPUT" | "LIMIT" | "PROTOCOL" | "HTTP" | "NETWORK" | "CANCELLED" | "TIMEOUT";
export class BootstrapHttpError extends Error {
  readonly code: `ERR_FSQLITE_BOOTSTRAP_HTTP_${Failure}`;
  constructor(kind: Failure, message: string, readonly outcome: "not-sent" | "unknown" = "not-sent",
    readonly status?: number, options?: ErrorOptions) {
    super(message, options); this.name = "BootstrapHttpError";
    this.code = `ERR_FSQLITE_BOOTSTRAP_HTTP_${kind}`;
  }
}
const MAX_CHUNK = 64 * 1024 * 1024, MAX_METADATA = 128 * 1024, MAX_RESPONSE = 8192;
const utf8 = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
const encoder = new TextEncoder();
function fail(kind: Failure, message: string): never { throw new BootstrapHttpError(kind, message); }
function data(value: unknown, key: string): unknown {
  if (typeof value !== "object" || value === null || Array.isArray(value)) fail("PROTOCOL", "Expected a bootstrap HTTP record");
  const d = Object.getOwnPropertyDescriptor(value, key);
  if (d === undefined || !Object.hasOwn(d, "value")) fail("PROTOCOL", "Bootstrap HTTP fields must be own data properties");
  return d.value;
}
function integer(value: unknown, maximum: number, minimum = 0): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < minimum || value > maximum) fail("PROTOCOL", "Invalid bootstrap counter");
  return value;
}
function bound(value: unknown, fallback: number, maximum: number): number {
  try { return integer(value === undefined ? fallback : value, maximum, 1); }
  catch { return fail("INPUT", "Invalid bootstrap HTTP limit"); }
}
function text(value: unknown, maximum: number): string {
  if (typeof value !== "string" || !value.length || value.length > maximum || value.includes("\0")) fail("PROTOCOL", "Invalid bootstrap identity");
  const bytes = encoder.encode(value);
  if (bytes.length > maximum || utf8.decode(bytes) !== value) fail("PROTOCOL", "Invalid bootstrap identity encoding");
  return value;
}
function digest(value: unknown): string {
  if (typeof value !== "string" || !/^[0-9a-f]{64}$/.test(value)) fail("PROTOCOL", "Invalid bootstrap digest");
  return value;
}
function manifest(value: unknown): BootstrapManifest {
  if (data(value, "protocol") !== "fsqlite-bootstrap-v1") fail("PROTOCOL", "Wrong bootstrap protocol");
  const source = data(value, "tables");
  if (!Array.isArray(source) || !source.length || source.length > 64) fail("PROTOCOL", "Invalid bootstrap tables");
  const tables: string[] = [], seen = new Set<string>();
  for (let i = 0, n = source.length; i < n; i++) {
    const d = Object.getOwnPropertyDescriptor(source, String(i));
    if (d === undefined || !Object.hasOwn(d, "value")) fail("PROTOCOL", "Bootstrap tables require own data entries");
    const name = text(d.value, 1024).replace(/[A-Z]/g, c => c.toLowerCase());
    if (seen.has(name) || name.startsWith("sqlite_") || name.startsWith("__fsqlite_")) fail("PROTOCOL", "Invalid bootstrap table scope");
    seen.add(name); tables.push(name);
  }
  return Object.freeze({ protocol: "fsqlite-bootstrap-v1", receiverId: text(data(value, "receiverId"), 256),
    deliveryId: text(data(value, "deliveryId"), 480), tables: Object.freeze(tables),
    chunks: integer(data(value, "chunks"), 100_000, 1), changes: integer(data(value, "changes"), 10_000_000),
    byteLength: integer(data(value, "byteLength"), 1024 * 1024 * 1024), sha256: digest(data(value, "sha256")) });
}
/** Intrinsic slots prevent typed-array subclasses from bypassing byte limits. */
function view(value: unknown, maximum: number): Uint8Array {
  if (!(value instanceof Uint8Array)) fail("INPUT", "A fixed Uint8Array is required");
  try {
    const proto = Object.getPrototypeOf(Uint8Array.prototype) as object;
    const get = (key: string): unknown => Object.getOwnPropertyDescriptor(proto, key)!.get!.call(value);
    const buffer = get("buffer"), offset = get("byteOffset") as number, length = get("byteLength") as number;
    if (length > maximum) fail("LIMIT", "Bootstrap bytes exceed their budget");
    if (!(buffer instanceof ArrayBuffer) || Object.getOwnPropertyDescriptor(ArrayBuffer.prototype, "resizable")?.get?.call(buffer)) fail("INPUT", "Use fixed non-shared buffers");
    return new Uint8Array(buffer, offset, length);
  } catch (cause: unknown) {
    if (cause instanceof BootstrapHttpError) throw cause;
    return fail("INPUT", "An attached Uint8Array is required");
  }
}
function progress(value: unknown, m: BootstrapManifest): BootstrapProgress | null {
  if (value === null) return null;
  const receivedChunks = integer(data(value, "receivedChunks"), m.chunks);
  const receivedBytes = integer(data(value, "receivedBytes"), m.byteLength);
  const receivedChanges = integer(data(value, "receivedChanges"), m.changes);
  const installed = data(value, "installed");
  if (typeof installed !== "boolean" || (installed && receivedChunks !== m.chunks) ||
      (receivedChunks === 0 && (receivedBytes !== 0 || receivedChanges !== 0)) ||
      (receivedChunks === m.chunks && (receivedBytes !== m.byteLength || receivedChanges !== m.changes))) fail("PROTOCOL", "Inconsistent bootstrap progress");
  return Object.freeze({ receivedChunks, receivedBytes, receivedChanges, installed });
}
function receipt(value: unknown, m: BootstrapManifest, orderedSourceId: string | undefined): BootstrapInstallReceipt {
  for (const key of ["protocol", "receiverId", "deliveryId", "sha256", "chunks", "changes", "byteLength"] as const) {
    if (data(value, key) !== m[key]) fail("PROTOCOL", "Installation receipt does not match the manifest");
  }
  const replayed = data(value, "replayed");
  if (data(value, "installed") !== true || data(value, "confirmed") !== true || typeof replayed !== "boolean") fail("PROTOCOL", "A confirmed installation receipt is required");
  let order: BootstrapInstallReceipt["order"];
  if (orderedSourceId === undefined) {
    if ("order" in (value as object)) fail("PROTOCOL", "Ordered installation requires an explicit source policy");
  } else {
    const input = data(value, "order");
    if (data(input, "protocol") !== "fsqlite-ordered-changeset-v1" || data(input, "streamId") !== orderedSourceId || data(input, "sequence") !== String(m.chunks)) fail("PROTOCOL", "Installation receipt has a different ordered source prefix");
    order = Object.freeze({ protocol: "fsqlite-ordered-changeset-v1", streamId: orderedSourceId, sequence: String(m.chunks) });
  }
  return Object.freeze({ protocol: m.protocol, receiverId: m.receiverId, deliveryId: m.deliveryId, sha256: m.sha256,
    chunks: m.chunks, changes: m.changes, byteLength: m.byteLength, installed: true, confirmed: true, replayed,
    ...(order === undefined ? {} : { order }) });
}
function json(bytes: Uint8Array): unknown {
  try { return JSON.parse(utf8.decode(bytes)); } catch { return fail("PROTOCOL", "Invalid bootstrap HTTP JSON or UTF-8"); }
}
function encode(info: BootstrapHttpRequestInfo, bytes: Uint8Array): Uint8Array {
  const meta = encoder.encode(JSON.stringify({ protocol: BOOTSTRAP_HTTP_PROTOCOL, ...info }));
  if (meta.length > MAX_METADATA) fail("LIMIT", "Bootstrap HTTP metadata exceeds 128 KiB");
  const wire = new Uint8Array(8 + meta.length + bytes.length);
  wire.set([70, 67, 66, 49]); new DataView(wire.buffer).setUint32(4, meta.length);
  wire.set(meta, 8); wire.set(bytes, 8 + meta.length); return wire;
}
class Budget {
  readonly signal: AbortSignal;
  readonly #controller = new AbortController();
  readonly #reason = new Error("Bootstrap HTTP deadline expired");
  readonly #deadline: number;
  #timer: ReturnType<typeof setTimeout> | undefined;
  #stop: (() => void) | undefined;
  constructor(options: BootstrapOperationOptions, timeout: number) {
    const external = options.signal;
    if (external !== undefined) {
      try { Object.getOwnPropertyDescriptor(AbortSignal.prototype, "aborted")!.get!.call(external); }
      catch { fail("INPUT", "signal must be an AbortSignal"); }
    }
    this.#deadline = performance.now() + Math.min(timeout, bound(options.timeoutMs, timeout, 2_147_483_647));
    this.signal = this.#controller.signal;
    if (external !== undefined) {
      const abort = () => this.#controller.abort(external.reason);
      external.addEventListener("abort", abort, { once: true });
      this.#stop = () => external.removeEventListener("abort", abort);
      if (external.aborted) abort();
    }
    this.#arm();
  }
  #expire(): void { if (performance.now() >= this.#deadline && !this.signal.aborted) this.#controller.abort(this.#reason); }
  #arm(): void {
    if (!this.signal.aborted) this.#timer = setTimeout(() => { this.#expire(); this.#arm(); }, Math.max(1, Math.ceil(this.#deadline - performance.now())));
  }
  check(): void {
    this.#expire();
    if (this.signal.aborted) throw new BootstrapHttpError(this.signal.reason === this.#reason ? "TIMEOUT" : "CANCELLED",
      "Bootstrap HTTP interrupted; reconcile the same manifest", "not-sent", undefined, { cause: this.signal.reason });
  }
  options(): BootstrapOperationOptions { this.check(); return { signal: this.signal, timeoutMs: Math.max(1, Math.ceil(this.#deadline - performance.now())) }; }
  finish(): void { clearTimeout(this.#timer); this.#stop?.(); }
}
async function readBody(body: ReadableStream<Uint8Array> | null, headers: Headers, maximum: number, b: Budget): Promise<Uint8Array> {
  let reader: ReadableStreamDefaultReader<Uint8Array> | undefined, cleanup: Promise<void> | undefined, done = false;
  const cancel = () => { if (reader !== undefined) cleanup ??= reader.cancel().catch(() => {}); };
  try {
    b.check();
    const encoding = headers.get("content-encoding"), declared = headers.get("content-length");
    if (encoding !== null && encoding.toLowerCase() !== "identity") fail("PROTOCOL", "Compressed bootstrap HTTP bodies are not supported");
    if (declared !== null && !/^(0|[1-9][0-9]*)$/.test(declared)) fail("PROTOCOL", "Invalid HTTP content length");
    const expected = declared === null ? null : Number(declared);
    if (expected !== null && (!Number.isSafeInteger(expected) || expected > maximum)) fail("LIMIT", "HTTP body exceeds its budget");
    if (body === null) {
      if (expected !== null && expected !== 0) fail("PROTOCOL", "Missing HTTP body");
      return new Uint8Array();
    }
    reader = body.getReader(); b.signal.addEventListener("abort", cancel, { once: true });
    if (b.signal.aborted) cancel();
    let output = new Uint8Array(Math.min(maximum, 4096)), used = 0;
    while (true) {
      b.check(); const part = await reader.read(); b.check();
      if (part.done) { done = true; break; }
      const chunk = view(part.value, Math.min(maximum - used, expected === null ? maximum - used : expected - used));
      if (used + chunk.length > output.length) {
        const grown = new Uint8Array(Math.min(maximum, Math.max(used + chunk.length, output.length * 2)));
        grown.set(output.subarray(0, used)); output = grown;
      }
      output.set(chunk, used); used += chunk.length;
    }
    if (expected !== null && used !== expected) fail("PROTOCOL", "Truncated HTTP body");
    return output.subarray(0, used);
  } finally {
    b.signal.removeEventListener("abort", cancel);
    if (!done) {
      if (reader !== undefined) cancel();
      else if (body !== null && !body.locked) cleanup = body.cancel().catch(() => {});
    }
    await cleanup; reader?.releaseLock();
  }
}
function endpoint(input: string | URL, insecure: boolean): string {
  let url: URL;
  try { url = new URL(String(input)); } catch { return fail("INPUT", "Use an absolute bootstrap endpoint URL"); }
  if (url.href.length > 8192 || url.username || url.password || url.hash ||
      (url.protocol !== "https:" && !(insecure && url.protocol === "http:" && ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname)))) fail("INPUT", "Use HTTPS without URL credentials or fragments; development HTTP is loopback-only");
  return url.href;
}
function credentialHeaders(input: HeadersInit | undefined): Headers {
  const h = new Headers(input);
  for (const name of h.keys()) {
    if (["content-type", "content-length", "content-encoding", "accept", "cookie", "cookie2", "host", "origin", "referer", "connection", "transfer-encoding", "trailer", "upgrade", BOOTSTRAP_HTTP_ACTION_HEADER].includes(name) || name.startsWith("sec-") || name.startsWith("proxy-")) fail("INPUT", "Credential headers cannot override bootstrap transport policies");
  }
  return h;
}
function responseResult(value: unknown, info: BootstrapHttpRequestInfo, orderedSourceId: string | undefined): BootstrapProgress | BootstrapInstallReceipt | null {
  if (data(value, "protocol") !== BOOTSTRAP_HTTP_PROTOCOL || data(value, "action") !== info.action || data(value, "sha256") !== info.manifest.sha256) fail("PROTOCOL", "Bootstrap response belongs to another operation or manifest");
  if (info.action === "stage") {
    if (data(value, "index") !== info.index) fail("PROTOCOL", "Bootstrap response belongs to another chunk");
  } else if ("index" in (value as object)) fail("PROTOCOL", "Control response has a chunk index");
  const result = data(value, "result");
  if (info.action === "install") return receipt(result, info.manifest, orderedSourceId);
  const p = progress(result, info.manifest);
  if (info.action === "stage" && (p === null || p.receivedChunks <= info.index!)) fail("PROTOCOL", "The receiver did not retain the staged prefix");
  return p;
}

/** No automatic retry; every post-dispatch failure has an unknown remote outcome. */
export function createBootstrapHttpTransport(url: string | URL, options: BootstrapHttpTransportOptions = {}): BootstrapTransferTransport {
  const maximum = bound(options.maxChunkBytes, 8 * 1024 * 1024, MAX_CHUNK), timeout = bound(options.timeoutMs, 30_000, 2_147_483_647);
  const insecure = options.allowInsecureLoopback ?? false;
  if (typeof insecure !== "boolean") fail("INPUT", "allowInsecureLoopback must be boolean");
  const address = endpoint(url, insecure), credentials = options.headers, send = options.fetch ?? globalThis.fetch?.bind(globalThis);
  const orderedSourceId = options.orderedSourceId === undefined ? undefined : text(options.orderedSourceId, 256);
  if (typeof send !== "function" || (credentials !== undefined && typeof credentials !== "function")) fail("INPUT", "A Fetch implementation and valid credentials provider are required");
  async function request(action: BootstrapHttpAction, input: BootstrapManifest, index?: number, bytes?: Uint8Array, controls: BootstrapOperationOptions = {}) {
    let b: Budget | undefined, sent = false, response: Response | undefined;
    try {
      b = new Budget(controls, timeout); b.check();
      const m = manifest(input);
      if (action === "stage") integer(index, m.chunks - 1);
      // Framing copies intrinsic bytes synchronously before credentials or Fetch can yield.
      const payload = action === "stage" ? view(bytes, Math.min(maximum, m.byteLength)) : new Uint8Array();
      const info: BootstrapHttpRequestInfo = Object.freeze({ action, manifest: m, byteLength: payload.length, ...(action === "stage" ? { index: index! } : {}) });
      const wire = encode(info, payload); b.check();
      const h = credentialHeaders(credentials === undefined ? undefined : await credentials(info, b.signal)); b.check();
      h.set("content-type", BOOTSTRAP_HTTP_CONTENT_TYPE); h.set("accept", BOOTSTRAP_HTTP_RESPONSE_TYPE); h.set(BOOTSTRAP_HTTP_ACTION_HEADER, action);
      const r = new Request(address, { method: "POST", headers: h, body: wire, signal: b.signal, redirect: "error", credentials: "omit", cache: "no-store", referrerPolicy: "no-referrer", mode: "cors" });
      b.check(); sent = true; response = await send(r); b.check();
      if (!(response instanceof Response) || response.redirected || response.type === "opaque" || response.type === "opaqueredirect") fail("PROTOCOL", "An inspectable unredirected response is required");
      if (response.status !== 200) throw new BootstrapHttpError("HTTP", "Bootstrap endpoint did not confirm this operation", "unknown", response.status);
      if (response.headers.get("content-type")?.trim().toLowerCase() !== BOOTSTRAP_HTTP_RESPONSE_TYPE) fail("PROTOCOL", "Unexpected bootstrap response media type");
      const value = json(await readBody(response.body, response.headers, MAX_RESPONSE, b)); b.check();
      return responseResult(value, info, orderedSourceId);
    } catch (cause: unknown) {
      let kind: Failure = sent ? "NETWORK" : "INPUT";
      if (cause instanceof BootstrapHttpError) kind = cause.code.slice("ERR_FSQLITE_BOOTSTRAP_HTTP_".length) as Failure;
      else { try { b?.check(); } catch (error: unknown) { if (error instanceof BootstrapHttpError) kind = error.code.endsWith("TIMEOUT") ? "TIMEOUT" : "CANCELLED"; } }
      throw new BootstrapHttpError(kind, "Bootstrap HTTP failed; retain the original manifest and source bytes before retrying", sent ? "unknown" : "not-sent", cause instanceof BootstrapHttpError ? cause.status : undefined, { cause });
    } finally {
      try { if (response instanceof Response && response.body !== null && !response.body.locked) await response.body.cancel().catch(() => {}); }
      finally { b?.finish(); }
    }
  }
  return Object.freeze({
    status: async (m, controls) => await request("status", m, undefined, undefined, controls) as BootstrapProgress | null,
    stage: async (m, index, bytes, controls) => await request("stage", m, index, bytes, controls) as BootstrapProgress,
    install: async (m, controls) => await request("install", m, undefined, undefined, controls) as BootstrapInstallReceipt,
  } satisfies BootstrapTransferTransport);
}

export interface BootstrapHttpAuthorization {
  readonly method: string;
  readonly url: string;
  readonly headers: Headers;
  readonly receiverId: string;
  readonly action: BootstrapHttpAction;
  readonly signal: AbortSignal;
}
export interface BootstrapHttpHandlerOptions {
  /** Mandatory, before body reads. Return literal true; CORS is not authentication. */
  authorize: (request: BootstrapHttpAuthorization) => boolean | Promise<boolean>;
  /** Sender namespace and action policy, after bounded parsing but before receiver SQL. */
  authorizeManifest?: (request: BootstrapHttpAuthorization, info: BootstrapHttpRequestInfo) => boolean | Promise<boolean>;
  /** Must match the receiver's trusted policy; never derived from a remote receipt. */
  orderedSourceId?: string;
  maxChunkBytes?: number;
  timeoutMs?: number;
  /** Covers authorization, upload, receiver work and cleanup. Default 1; max 64. */
  maxInFlight?: number;
  allowedOrigins?: readonly string[];
  allowedHeaders?: readonly string[];
}
export type BootstrapHttpHandler = (request: Request) => Promise<Response>;
export type BootstrapHttpReceiver = BootstrapTransferTransport & { readonly receiverId: string };
function action(value: unknown): BootstrapHttpAction {
  if (value !== "status" && value !== "stage" && value !== "install") fail("PROTOCOL", "Unsupported bootstrap HTTP action");
  return value;
}
function decode(wire: Uint8Array, expectedAction: BootstrapHttpAction, maximum: number) {
  if (wire.length < 8 || wire[0] !== 70 || wire[1] !== 67 || wire[2] !== 66 || wire[3] !== 49) fail("PROTOCOL", "Invalid bootstrap HTTP frame");
  const size = new DataView(wire.buffer, wire.byteOffset, wire.byteLength).getUint32(4);
  if (size < 1 || size > MAX_METADATA || size > wire.length - 8) fail("PROTOCOL", "Invalid bootstrap metadata length");
  const input = json(wire.subarray(8, 8 + size));
  if (data(input, "protocol") !== BOOTSTRAP_HTTP_PROTOCOL || action(data(input, "action")) !== expectedAction) fail("PROTOCOL", "Bootstrap header and frame action disagree");
  const m = manifest(data(input, "manifest"));
  const byteLength = integer(data(input, "byteLength"), expectedAction === "stage" ? Math.min(maximum, m.byteLength) : 0);
  if (byteLength !== wire.length - size - 8) fail("PROTOCOL", "Bootstrap body does not match its declared length");
  let index: number | undefined;
  if (expectedAction === "stage") index = integer(data(input, "index"), m.chunks - 1);
  else if ("index" in (input as object)) fail("PROTOCOL", "Control requests cannot have a chunk index");
  const info: BootstrapHttpRequestInfo = Object.freeze({ action: expectedAction, manifest: m, byteLength, ...(index === undefined ? {} : { index }) });
  return { info, bytes: wire.subarray(8 + size) };
}
function allowedOrigins(input: readonly string[] | undefined): ReadonlySet<string> {
  if (input !== undefined && (!Array.isArray(input) || input.length > 64)) fail("INPUT", "Use at most 64 exact browser origins");
  const result = new Set<string>();
  for (const item of input ?? []) {
    if (typeof item !== "string" || item.length > 2048) fail("INPUT", "Invalid browser origin");
    let url: URL;
    try { url = new URL(item); } catch { return fail("INPUT", "Invalid browser origin"); }
    if (url.origin !== item || !["http:", "https:"].includes(url.protocol)) fail("INPUT", "Browser origins must be exact HTTP(S) origins without paths or credentials");
    result.add(item);
  }
  return result;
}
function allowedHeaders(input: readonly string[] | undefined): ReadonlySet<string> {
  if (input !== undefined && (!Array.isArray(input) || input.length > 32)) fail("INPUT", "Use at most 32 additional preflight headers");
  const result = new Set(["content-type", "authorization", BOOTSTRAP_HTTP_ACTION_HEADER]);
  for (const item of input ?? []) {
    if (typeof item !== "string" || item.length > 128 || !/^[!#$%&'+.^_`|~0-9a-z-]+$/i.test(item)) fail("INPUT", "Invalid preflight header");
    const name = item.toLowerCase();
    if (["cookie", "cookie2", "host", "origin", "connection", "transfer-encoding"].includes(name) || name.startsWith("sec-") || name.startsWith("proxy-")) fail("INPUT", "Preflight cannot permit ambient credentials or connection headers");
    result.add(name);
  }
  return result;
}
function reply(status: number, value: unknown, origin: string | null, extra?: HeadersInit): Response {
  const headers = new Headers(extra);
  headers.set("cache-control", "no-store"); headers.set("x-content-type-options", "nosniff");
  headers.set("vary", "Origin, Access-Control-Request-Method, Access-Control-Request-Headers");
  if (origin !== null) headers.set("access-control-allow-origin", origin);
  if (status === 204) return new Response(null, { status, headers });
  headers.set("content-type", status === 200 ? BOOTSTRAP_HTTP_RESPONSE_TYPE : "application/json");
  const bytes = encoder.encode(JSON.stringify(value));
  if (bytes.length > MAX_RESPONSE) fail("PROTOCOL", "Bootstrap response exceeded its wire budget");
  return new Response(bytes, { status, headers });
}
function rejected(status: number, origin: string | null, extra?: HeadersInit): Response {
  return reply(status, { error: "ERR_FSQLITE_BOOTSTRAP_HTTP_REJECTED", outcome: "unknown" }, origin, extra);
}

/** Authenticated Fetch endpoint. No reset/discard operation is exposed remotely. */
export function createBootstrapHttpHandler(receiver: BootstrapHttpReceiver, options: BootstrapHttpHandlerOptions): BootstrapHttpHandler {
  const id = text(receiver?.receiverId, 256), authorize = options?.authorize, authorizeManifest = options?.authorizeManifest;
  const status = receiver?.status, stage = receiver?.stage, install = receiver?.install;
  if ([status, stage, install, authorize].some(fn => typeof fn !== "function") || (authorizeManifest !== undefined && typeof authorizeManifest !== "function")) fail("INPUT", "A bootstrap receiver and explicit authorization policy are required");
  const methods = { status: status.bind(receiver), stage: stage.bind(receiver), install: install.bind(receiver) };
  const maximum = bound(options.maxChunkBytes, 8 * 1024 * 1024, MAX_CHUNK), timeout = bound(options.timeoutMs, 30_000, 2_147_483_647);
  const capacity = bound(options.maxInFlight, 1, 64), origins = allowedOrigins(options.allowedOrigins), headers = allowedHeaders(options.allowedHeaders);
  const orderedSourceId = options.orderedSourceId === undefined ? undefined : text(options.orderedSourceId, 256);
  let active = 0;
  return async request => {
    let b: Budget | undefined, admitted = false, receiving = false, origin: string | null = null;
    try {
      if (!(request instanceof Request) || request.url.length > 8192) return rejected(400, null);
      const requested = request.headers.get("origin");
      if (requested !== null) {
        let url: URL;
        try { url = new URL(requested); } catch { return rejected(403, null); }
        if (url.origin !== requested || !["http:", "https:"].includes(url.protocol) || (requested !== new URL(request.url).origin && !origins.has(requested))) return rejected(403, null);
        origin = requested;
      }
      if (request.method === "OPTIONS") {
        const names = request.headers.get("access-control-request-headers") ?? "";
        if (origin === null || request.headers.get("access-control-request-method") !== "POST" || names.length > 4096) return rejected(403, origin);
        const requestedHeaders = names === "" ? [] : names.split(",").map(n => n.trim().toLowerCase());
        if (requestedHeaders.length > 35 || requestedHeaders.some(n => !headers.has(n))) return rejected(403, origin);
        return reply(204, null, origin, { "access-control-allow-methods": "POST", "access-control-allow-headers": [...new Set(requestedHeaders)].join(", ") });
      }
      if (request.method !== "POST") return rejected(405, origin, { allow: "POST, OPTIONS" });
      if (request.headers.get("content-type")?.trim().toLowerCase() !== BOOTSTRAP_HTTP_CONTENT_TYPE) return rejected(415, origin);
      const op = action(request.headers.get(BOOTSTRAP_HTTP_ACTION_HEADER));
      if (active >= capacity) return rejected(503, origin);
      active++; admitted = true;
      b = new Budget({ signal: request.signal }, timeout); b.check();
      const context: BootstrapHttpAuthorization = Object.freeze({ method: request.method, url: request.url,
        headers: new Headers(request.headers), receiverId: id, action: op, signal: b.signal });
      let permitted: boolean;
      try { permitted = (await authorize(context)) === true; } catch { permitted = false; }
      b.check(); if (!permitted) return rejected(403, origin);
      const wire = await readBody(request.body, request.headers, 8 + MAX_METADATA + (op === "stage" ? maximum : 0), b);
      const { info, bytes } = decode(wire, op, maximum); b.check();
      if (info.manifest.receiverId !== id) return rejected(400, origin);
      if (authorizeManifest !== undefined) {
        try { permitted = (await authorizeManifest(context, info)) === true; } catch { permitted = false; }
        b.check(); if (!permitted) return rejected(403, origin);
      }
      b.check(); receiving = true;
      // The admission slot outlives client cancellation until SQL, confirmation
      // and cleanup settle. Never race or detach an installation transaction.
      const result = op === "stage" ? await methods.stage(info.manifest, info.index!, bytes, b.options()) :
        op === "install" ? await methods.install(info.manifest, b.options()) : await methods.status(info.manifest, b.options());
      b.check();
      const packet = { protocol: BOOTSTRAP_HTTP_PROTOCOL, action: op, sha256: info.manifest.sha256,
        ...(op === "stage" ? { index: info.index } : {}), result };
      const captured = responseResult(packet, info, orderedSourceId);
      return reply(200, { ...packet, result: captured }, origin);
    } catch (cause: unknown) {
      if (cause instanceof BootstrapHttpError) {
        if (cause.code.endsWith("CANCELLED") || cause.code.endsWith("TIMEOUT")) return rejected(408, origin);
        if (cause.code.endsWith("LIMIT")) return rejected(413, origin);
        return rejected(receiving ? 500 : 400, origin);
      }
      const code = typeof cause === "object" && cause !== null ? Object.getOwnPropertyDescriptor(cause, "code")?.value : undefined;
      const status = code === "ERR_FSQLITE_BOOTSTRAP_BUSY" ? 503 : code === "ERR_FSQLITE_BOOTSTRAP_STATE" ? 409 :
        code === "ERR_FSQLITE_BOOTSTRAP_INPUT" ? 400 : code === "ERR_FSQLITE_BOOTSTRAP_LIMIT" ? 413 :
        code === "ERR_FSQLITE_BOOTSTRAP_CANCELLED" || code === "ERR_FSQLITE_BOOTSTRAP_TIMEOUT" ? 408 : 500;
      return rejected(status, origin);
    } finally {
      try { if (request instanceof Request && request.body !== null && !request.body.locked) await request.body.cancel().catch(() => {}); }
      finally { b?.finish(); if (admitted) active--; }
    }
  };
}
