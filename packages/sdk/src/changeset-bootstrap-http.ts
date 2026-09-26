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
