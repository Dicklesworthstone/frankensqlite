import type { WorkerRequest, WorkerResponse } from "@frankensqlite/worker";
import type {
  WorkerErrorEventLike,
  WorkerLike,
  WorkerMessageEvent,
} from "../../src/worker-client";

// Deliberately controllable transport: tests decide when responses, crashes and
// synchronous postMessage failures occur. This is not a browser/WASM fixture.
export class ControlledWorker implements WorkerLike {
  readonly requests: WorkerRequest[] = [];
  readonly messages = new Set<(event: WorkerMessageEvent) => void>();
  readonly errors = new Set<(event: WorkerErrorEventLike) => void>();
  onPost: ((request: WorkerRequest) => void) | undefined;
  onTerminate: (() => void) | undefined;
  terminateCount = 0;

  addEventListener(type: "message", listener: (event: WorkerMessageEvent) => void): void;
  addEventListener(type: "error", listener: (event: WorkerErrorEventLike) => void): void;
  addEventListener(
    type: "message" | "error",
    listener: ((event: WorkerMessageEvent) => void) | ((event: WorkerErrorEventLike) => void),
  ): void {
    if (type === "message") {
      this.messages.add(listener as (event: WorkerMessageEvent) => void);
    } else {
      this.errors.add(listener as (event: WorkerErrorEventLike) => void);
    }
  }

  removeEventListener(type: "message", listener: (event: WorkerMessageEvent) => void): void;
  removeEventListener(type: "error", listener: (event: WorkerErrorEventLike) => void): void;
  removeEventListener(
    type: "message" | "error",
    listener: ((event: WorkerMessageEvent) => void) | ((event: WorkerErrorEventLike) => void),
  ): void {
    if (type === "message") {
      this.messages.delete(listener as (event: WorkerMessageEvent) => void);
    } else {
      this.errors.delete(listener as (event: WorkerErrorEventLike) => void);
    }
  }

  postMessage(request: WorkerRequest): void {
    this.requests.push(request);
    this.onPost?.(request);
  }

  terminate(): void {
    this.terminateCount += 1;
    this.onTerminate?.();
  }

  reply(response: WorkerResponse): void {
    for (const listener of this.messages) listener({ data: response });
  }

  crash(message: string): void {
    for (const listener of this.errors) listener({ message });
  }
}

export function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}

export type Outcome<T> =
  | { status: "pending" }
  | { status: "fulfilled"; value: T }
  | { status: "rejected"; reason: unknown };

export function observe<T>(promise: Promise<T>) {
  let outcome: Outcome<T> = { status: "pending" };
  const settled = promise.then(
    (value) => { outcome = { status: "fulfilled", value }; },
    (reason: unknown) => { outcome = { status: "rejected", reason }; },
  );
  return { get outcome() { return outcome; }, settled };
}

// Bounded microtask turns detect abandoned promises without a hanging test or
// sleeps. No worker response is synthesized by draining the microtask queue.
export async function drain(): Promise<void> {
  for (let i = 0; i < 30; i += 1) await Promise.resolve();
}

export function rejected<T>(observation: { outcome: Outcome<T> }): unknown {
  const outcome = observation.outcome;
  if (outcome.status !== "rejected") {
    throw new Error(`Expected prompt rejection, got ${outcome.status}`);
  }
  return outcome.reason;
}
