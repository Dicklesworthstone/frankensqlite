import type { ErrorResponse, SerializedFrankenError, WorkerFatalMessage, WorkerRequest, WorkerResponse } from "./protocol";
import { serializeFrankenError, WorkerConnectionHost } from "./connection";
import { responseTransferList } from "./result-codec";

const host = new WorkerConnectionHost();
const workerScope = globalThis as unknown as DedicatedWorkerGlobalScope;
let terminal = false;
let reported = false;

workerScope.addEventListener("message", (event: MessageEvent<unknown>) => {
  // dispatch consumes its own errors. An unhandled async rejection in a worker
  // is not a reliable correlated response (nor a reliable parent error event).
  void dispatch(event.data).catch(() => {
    failTransport("Worker dispatch failed outside its correlated response path", true);
  });
});
workerScope.addEventListener("messageerror", () => {
  failTransport("Worker request could not be deserialized");
});

function transportError(code: string, message: string): SerializedFrankenError {
  return { code, message, transient: false, userRecoverable: false,
    suggestion: "SQL or snapshot publication may have executed. Inspect authoritative state; do not blindly retry writes." };
}

function reportTransportError(): void {
  if (reported) return;
  reported = true;
  setTimeout(() => { throw new Error("FrankenSQLite worker transport failed; operation outcomes are unknown"); }, 0);
}

function failTransport(message: string, deliveryFailed = false): void {
  if (terminal) { if (deliveryFailed) reportTransportError(); return; }
  terminal = true;
  // Never run queued writes after a channel-wide failure; never destroy handles
  // underneath active SQL. A close request may still join this cleanup fence.
  void host.failTransport(new Error(message)).catch(() => {});
  const fatal: WorkerFatalMessage = { kind: "worker-fatal", error: transportError("ERR_FSQLITE_WORKER_TRANSPORT", message) };
  try { workerScope.postMessage(fatal); }
  catch {
    // Even the primitive-only fatal notice failed. Surface a real worker error
    // to the owner rather than merely rejecting an unobserved dispatch Promise.
    reportTransportError();
  }
}

async function dispatch(input: unknown): Promise<void> {
  let requestId: number;
  let request: WorkerRequest;
  try {
    if (typeof input !== "object" || input === null || Array.isArray(input)) throw new TypeError("Invalid request");
    request = input as WorkerRequest;
    requestId = request.requestId;
    if (!Number.isSafeInteger(requestId) || requestId < 0) throw new TypeError("Invalid request id");
  } catch {
    failTransport("Worker received a request without a usable correlation id");
    return;
  }
  if (terminal && request.kind !== "close") return;
  let response: WorkerResponse;
  try { response = await host.handle(request); }
  catch (cause: unknown) {
    // Core error conversion can itself fail (for example a throwing getter).
    // The final fallback contains only owned primitives and never retries SQL.
    let error: SerializedFrankenError;
    try { error = serializeFrankenError(cause); }
    catch { error = transportError("ERR_FSQLITE_WORKER_DISPATCH", "Worker could not serialize an operation failure"); }
    response = { kind: "error", requestId, error };
  }
  if (terminal && request.kind !== "close") return;
  try { postResponse(response); }
  catch {
    const fallback: ErrorResponse = { kind: "error", requestId,
      error: transportError("ERR_FSQLITE_RESPONSE_TRANSFER", "Worker could not clone or transfer the operation response") };
    try { workerScope.postMessage(fallback); }
    catch { failTransport("Worker could not deliver the response or its failure acknowledgement", true); }
  }
}

function postResponse(response: WorkerResponse): void {
  workerScope.postMessage(response, responseTransferList(response));
}

export {};
