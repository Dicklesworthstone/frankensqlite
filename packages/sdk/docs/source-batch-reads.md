# Consistent source-batch reads

`ChangesetOutbox.readBatch(ids, { maxBytes })` reads 1–256 distinct retained
identities in one owned SQL transaction. `ChangesetFanout.forReplica(id)` exposes
the same method, using that recipient's verified cursor in the same snapshot as
the source metadata and payloads. No read transaction spans network delivery.

```ts
const records = await outbox.readBatch(deliveryIds, { maxBytes: 8 * 1024 * 1024 });
const eastRecords = await fanout.forReplica("east").readBatch(deliveryIds);
```

Results follow the requested order. An acknowledged identity has `changeset:
null`; another fanout member may still need its retained bytes. Pending payloads
are private copies, while the array, records and metadata are frozen. Mutating a
returned payload cannot alter SQL storage or another read. This operation neither
acknowledges deliveries nor confirms source durability.

The input is captured before SQL admission or any await. Empty, duplicate,
sparse, accessor-backed, invalid and oversized identity lists are rejected. The
aggregate pending-payload budget defaults to 64 MiB and may only be lowered. The
complete metadata window must fit before any payload is fetched. Missing
identities, invalid retained metadata, digest/count/scope errors and corrupt
payloads reject the entire read; no partial array is returned. Existing bootstrap
manifest checks remain in force. Acknowledged payloads do not consume the budget.

`createOrderedChangesetBatchTransport` captures and uses the optional `readBatch`
method when supplied by its source. It verifies all returned identities, counts
and contiguous sequence numbers before sending. Source failure, a malformed
result or cancellation never falls back to individual reads. The original
per-message read path remains available for legacy source ports without the
capability. Authentication, source confirmation, receipt verification, omission
consent and acknowledgements retain their existing owners.

`ChangesetDeliveryPump.runBatch` also uses the source's optional `readBatch`
capability during initial payload collection. It first selects a contiguous
metadata prefix that fits the per-message, window and remaining run limits,
then loads only that prefix in one source snapshot. It validates and owns the
complete response before asynchronous digest checks. An invalid response or
failed read never falls back to separate reads or sends a partial window.
The single-message `run()` path is unchanged, and ports without `readBatch`
retain their individual-read path.

Pending-page selection is still a separate transaction: a concurrent ACK may
win before the payload snapshot begins. Such acknowledged members end the
current delivery window without skipping an earlier pending prefix. The pump
then rereads the oldest pending state. Cancellation joins the admitted source
read before releasing pump admission; no source transaction spans confirmation
or transport. Empty payloads remain valid even when the run's byte allowance
has been exhausted. Receipt checks, omission consent and the explicit
`atomicAcknowledgements` policy are unaffected.

A source snapshot avoids observing a mixture of recipient progress and retained
payload generations when another WAL connection acknowledges concurrently. It is
not a distributed snapshot, writer lock, reservation preventing later reclamation,
or guarantee that an external caller's transaction has committed. All returned
payloads remain in memory, bounded by the aggregate budget; metadata, the SQL
adapter and the transport's separately owned input have additional memory costs.
The integration reduces authoritative sender validation from N source read
transactions to one with the production ports, but no throughput claim is made.
Pump payload collection likewise uses one read transaction per admitted window.
It and the sender's subsequent authoritative validation remain separate snapshots,
not one reservation or distributed transaction. The pump owns another bounded
copy of the returned payloads; the wire budget is not a total heap/RSS bound.

Validation uses actual outbox/fanout/capture/transport/receiver/application modules
on native reference SQLite. Tests include a second WAL connection committing ACKs
while the first snapshot is held, exact budgets before payload queries, native
Session application, corrupt final payloads, receiver-specific retention,
cancellation and legacy fallback. It is not FrankenSQLite Rust-engine,
browser/WASM, TLS or physical crash-safety qualification.
