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
    const after = observe(client.query("SELECT 2"));
    const close = observe(client.close());
    await drain();
    expect(rejected(after)).toBe(cause);
    expect(rejected(close)).toBe(cause);
    expect((cause as Error).message).toContain("WASM trap");
    expect(worker.requests.map((r) => r.kind)).toEqual(["execute"]);
    expect(worker.terminateCount).toBe(1);
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
