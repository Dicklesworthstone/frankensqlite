import type { ErrorResponse, WorkerRequest, WorkerResponse } from "./protocol";
import { serializeFrankenError, WorkerConnectionHost } from "./connection";
import { responseTransferList } from "./result-codec";

const host = new WorkerConnectionHost();
const workerScope = globalThis as unknown as DedicatedWorkerGlobalScope;

workerScope.addEventListener("message", (event: MessageEvent<WorkerRequest>) => {
  void dispatch(event.data);
});

async function dispatch(request: WorkerRequest): Promise<void> {
  try {
    const response = await host.handle(request);
    postResponse(response);
  } catch (error: unknown) {
    postResponse({
      kind: "error",
      requestId: request.requestId,
      error: serializeFrankenError(error),
    });
  }
}

function postResponse(response: WorkerResponse | ErrorResponse): void {
  workerScope.postMessage(response, responseTransferList(response));
}

export {};
