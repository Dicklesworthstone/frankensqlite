// Test-only Node transport and SQLite adapter. SDK client and worker host are
// production modules; this is not the browser entrypoint or FrankenSQLite WASM.
import { parentPort, workerData } from "node:worker_threads";
import { responseTransferList } from "../../src/result-codec.ts";
import { sqliteSnapshotWorker } from "./snapshot-sqlite-core.mjs";

const fixture = sqliteSnapshotWorker({
  statementFree(sql) {
    if (workerData?.freeFailure && sql.includes("fail_free"))
      throw new Error("reference finalization failure");
  },
});
parentPort.on("message", (request) => {
  if (request.testControl === "stats") {
    parentPort.postMessage({
      testControl: "stats",
      id: request.id,
      prepared: fixture.host.preparedStatements,
      requests: fixture.host.requestQueue,
    });
    return;
  }
  void fixture.host
    .handle(request)
    .then((response) => {
      if (response.kind === "ready") {
        if (workerData?.omitPolicy) delete response.data.preparedStatementLimits;
        if (workerData?.responsePolicy !== undefined)
          response.data.preparedStatementLimits = workerData.responsePolicy;
      }
      parentPort.postMessage(response, responseTransferList(response));
    })
    .catch((error) => {
      throw error;
    });
});
