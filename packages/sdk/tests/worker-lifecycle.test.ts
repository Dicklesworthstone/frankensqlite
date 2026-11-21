import { describe, expect, it } from "vitest";

import { FrankenDB } from "../src/database";
import { FrankenSQLiteError } from "../src/errors";
import { FrankenWorkerClient } from "../src/worker-client";
import { ControlledWorker, drain, observe, rejected } from "./helpers/controlled-worker";

function fixture() {
  const worker = new ControlledWorker();
  return { worker, client: new FrankenWorkerClient(worker) };
}

describe("worker client terminal lifecycle", () => {
  it("rejects every outstanding request on dispose instead of abandoning promises", async () => {
    const { worker, client } = fixture();
    const first = observe(client.execute("INSERT INTO t VALUES (1)"));
    const second = observe(client.export());
    const lateListeners = [...worker.messages];
    client.dispose();
    await drain();
    const error = rejected(first);
    expect(error instanceof Error).toBe(true);
    expect(rejected(second)).toBe(error);
    for (const listener of lateListeners) {
      listener({ data: { kind: "execute-result", requestId: 1, changes: 1 } });
    }
    await drain();
    expect(rejected(first)).toBe(error);
    expect(worker.messages.size).toBe(0);
    expect(worker.errors.size).toBe(0);
  });

  it("rejects all operation kinds immediately after disposal without posting", async () => {
    const { worker, client } = fixture();
    client.dispose();
    const operations = [
      client.init({}),
      client.execute("SELECT 1"),
      client.executeBatch("SELECT 1"),
      client.query("SELECT 1"),
      client.prepare("SELECT 1"),
      client.export(),
      client.executePrepared("1"),
      client.queryPrepared("1"),
      client.finalizePrepared("1"),
      client.close(),
    ].map((operation) => observe<unknown>(operation));
    await drain();
    for (const operation of operations) expect(rejected(operation) instanceof Error).toBe(true);
    expect(worker.requests).toEqual([]);
  });

  it("makes disposal idempotent", () => {
    const { worker, client } = fixture();
    client.dispose();
    client.dispose();
    expect(worker.terminateCount).toBe(1);
  });

  it("preserves a crash as the cause of pending and future request failures", async () => {
    const { worker, client } = fixture();
    const pending = observe(client.execute("SELECT 1"));
    worker.crash("WASM trap");
    await drain();
    const cause = rejected(pending);
    expect(worker.terminateCount).toBe(1);
    const after = observe(client.query("SELECT 2"));
    const close = observe(client.close());
    await drain();
    expect(rejected(after)).toBe(cause);
    expect(rejected(close)).toBe(cause);
    expect((cause as Error).message).toContain("WASM trap");
    expect(worker.requests.map((r) => r.kind)).toEqual(["execute"]);
    expect(worker.terminateCount).toBe(1);
  });

  it("cleans up an idle unreadable channel and preserves cleanup errors for close", async () => {
    const { worker, client } = fixture();
    const cleanup = new Error("could not terminate worker");
    worker.onTerminate = () => {
      throw cleanup;
    };
    worker.messageError();
    await drain();
    expect(worker.terminateCount).toBe(1);
    const failed = observe(client.query("SELECT 2"));
    const close = client.close();
    const closing = observe(close);
    await drain();
    const transport = rejected(failed);
    const combined = rejected(closing) as AggregateError;
    expect(combined).toBeInstanceOf(AggregateError);
    expect(combined.cause).toBe(transport);
    expect(combined.errors).toEqual([transport, cleanup]);
    expect(client.close()).toBe(close);
    expect(worker.requests).toEqual([]);
  });

  it("shares a public close with a worker adapter that closes again during postMessage", async () => {
    const worker = new ControlledWorker();
    const opening = FrankenDB.open({ worker });
    worker.reply({ kind: "ready", requestId: 1, data: { path: ":memory:", persistence: "memory" } });
    const db = await opening;
    const pending = db.execute("INSERT INTO t VALUES (1)");
    let nested: Promise<void> | undefined;
    worker.onPost = (request) => {
      if (request.kind === "close") {
        worker.onPost = undefined;
        nested = db.close();
      }
    };
    const close = db.close();
    expect(worker.requests.map((r) => r.kind)).toEqual(["init", "execute", "close"]);
    expect(nested).toBe(close);
    worker.reply({ kind: "execute-result", requestId: 2, changes: 1 });
    worker.reply({ kind: "close-result", requestId: 3 });
    expect(await pending).toBe(1);
    await close;
    expect(db.close()).toBe(close);
    expect(worker.terminateCount).toBe(1);
  });

  it("preserves cleanup failure when the transport crashes during the close post", async () => {
    const { worker, client } = fixture();
    const cleanup = new Error("terminate failed");
    worker.onTerminate = () => {
      throw cleanup;
    };
    let nested: Promise<void> | undefined;
    worker.onPost = (request) => {
      if (request.kind === "close") {
        worker.onPost = undefined;
        nested = client.close();
        void observe(nested);
        worker.crash("close channel lost");
      }
    };
    const close = client.close();
    const error = await close.catch((failure: unknown) => failure);
    expect(error).toBeInstanceOf(AggregateError);
    const combined = error as AggregateError;
    expect(nested).toBe(close);
    expect(combined.cause).toBe(combined.errors[0]);
    expect(combined.errors[1]).toBe(cleanup);
    expect(client.close()).toBe(close);
    expect(worker.terminateCount).toBe(1);
    expect(worker.requests.map((r) => r.kind)).toEqual(["close"]);
  });

  it("coalesces close, drains accepted requests, and refuses new requests", async () => {
    const { worker, client } = fixture();
    const pending = observe(client.execute("INSERT INTO t VALUES (1)"));
    const close = client.close();
    const again = client.close();
    const closing = observe(close);
    const late = observe(client.execute("INSERT INTO t VALUES (2)"));
    await drain();
    expect(rejected(late) instanceof Error).toBe(true);
    expect(close).toBe(again);
    expect(pending.outcome.status).toBe("pending");
    expect(closing.outcome.status).toBe("pending");
    expect(worker.requests.map((r) => r.kind)).toEqual(["execute", "close"]);
    worker.reply({ kind: "execute-result", requestId: 1, changes: 1 });
    worker.reply({ kind: "close-result", requestId: 2 });
    await close;
    await drain();
    expect(pending.outcome).toEqual({ status: "fulfilled", value: 1 });
    expect(client.close()).toBe(close);
    client.dispose();
    expect(worker.terminateCount).toBe(1);
  });

  it("settles an in-flight close if explicitly disposed", async () => {
    const { client } = fixture();
    const closing = observe(client.close());
    client.dispose();
    await drain();
    expect(rejected(closing) instanceof Error).toBe(true);
  });

  it("does not poison the connection after a synchronous postMessage error", async () => {
    const { worker, client } = fixture();
    const cloneError = new Error("DataCloneError");
    worker.onPost = () => {
      throw cloneError;
    };
    const failed = observe(client.execute("SELECT 1"));
    await drain();
    expect(rejected(failed)).toBe(cloneError);
    worker.onPost = undefined;
    const next = client.execute("SELECT 2");
    worker.reply({ kind: "execute-result", requestId: 1, changes: 99 });
    worker.reply({ kind: "execute-result", requestId: 2, changes: 2 });
    expect(await next).toBe(2);
    client.dispose();
  });

  it("releases worker resources when database initialization fails", async () => {
    const worker = new ControlledWorker();
    const opening = observe(FrankenDB.open({ worker }));
    worker.reply({
      kind: "error",
      requestId: 1,
      error: {
        code: "SQLITE_NOTADB",
        sqliteCode: 26,
        message: "invalid snapshot",
      },
    });
    await drain();
    const error = rejected(opening);
    expect(error instanceof FrankenSQLiteError).toBe(true);
    expect((error as FrankenSQLiteError).sqliteCode).toBe(26);
    expect(worker.terminateCount).toBe(1);
    expect(worker.messages.size).toBe(0);
    expect(worker.errors.size).toBe(0);
  });

  it("disposes after a close error and preserves the worker's error", async () => {
    const { worker, client } = fixture();
    const closing = observe(client.close());
    worker.reply({
      kind: "error",
      requestId: 1,
      error: {
        code: "SQLITE_IOERR",
        message: "close failed",
      },
    });
    await drain();
    expect((rejected(closing) as FrankenSQLiteError).code).toBe("SQLITE_IOERR");
    expect(worker.terminateCount).toBe(1);
    expect(worker.messages.size).toBe(0);
  });

  it("preserves both initialization and cleanup failures", async () => {
    const worker = new ControlledWorker();
    const cleanupError = new Error("terminate failed");
    worker.onTerminate = () => {
      throw cleanupError;
    };
    const opening = observe(FrankenDB.open({ worker }));
    worker.reply({
      kind: "error",
      requestId: 1,
      error: {
        code: "SQLITE_NOTADB",
        message: "invalid snapshot",
      },
    });
    await drain();
    const error = rejected(opening);
    expect(error instanceof AggregateError).toBe(true);
    const aggregate = error as AggregateError;
    expect(aggregate.errors[0]).toBe(aggregate.cause);
    expect((aggregate.errors[0] as FrankenSQLiteError).code).toBe("SQLITE_NOTADB");
    expect(aggregate.errors[1]).toBe(cleanupError);
  });

  it("reports termination failure when the worker crashes during initialization", async () => {
    const worker = new ControlledWorker();
    const cleanup = new Error("terminate failed");
    worker.onTerminate = () => {
      throw cleanup;
    };
    const opening = FrankenDB.open({ worker });
    worker.crash("failed before ready");
    const failure = await opening.catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(AggregateError);
    const combined = failure as AggregateError;
    expect(combined.errors).toEqual([combined.cause, cleanup]);
    expect((combined.cause as Error).message).toContain("failed before ready");
    expect(worker.terminateCount).toBe(1);
  });

  it("preserves both close and cleanup failures", async () => {
    const { worker, client } = fixture();
    const cleanupError = new Error("terminate failed");
    worker.onTerminate = () => {
      throw cleanupError;
    };
    const closing = observe(client.close());
    worker.reply({
      kind: "error",
      requestId: 1,
      error: {
        code: "SQLITE_IOERR",
        message: "close failed",
      },
    });
    await drain();
    const error = rejected(closing);
    expect(error instanceof AggregateError).toBe(true);
    const aggregate = error as AggregateError;
    expect(aggregate.errors[0]).toBe(aggregate.cause);
    expect(aggregate.errors[1]).toBe(cleanupError);
    client.dispose();
    expect(worker.terminateCount).toBe(1);
  });
});
