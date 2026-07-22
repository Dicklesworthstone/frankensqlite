# `.fsqlite-history` format v1

Status: normative format contract for `bd-hsi34`.

## Purpose and authority boundary

`<database>.fsqlite-history` is an append-only lookup log from durable commit
coordinates to historical catalog roots. It is not a WAL, a backup, or a copy
of historical pages. A record is usable only while the page-version set for
the same `(database_history_id, commit_seq)` remains durably retained.

The recovered main database and WAL are authoritative. Before this sidecar is
opened, the caller supplies the recovered database identity, history-format
generation, database/WAL generation, and durable commit horizon. Identity or
generation mismatch is a hard stale-sidecar error. A valid record suffix past
the recovered commit horizon is discarded as an unrecovered tail.

All integers are unsigned little-endian. All byte offsets below are inclusive
at the start and exclusive at the end.

## Pathnames and slot geometry

- History: `<database>.fsqlite-history`
- Optional cache: `<database>.fsqlite-history-idx`
- Slot size: 4096 bytes
- Slot 0: immutable history header
- Slot `n + 1`: history record `n`
- Record byte offset: `(n + 1) * 4096`

An append writes the complete 4096-byte slot. V1 populates bytes `0..64` and
zeros bytes `64..4096`. A slot whose file extent is shorter than 4096 bytes is
never committed, even if its first 64 bytes happen to be present. This rule
makes later slot extensions unambiguous.

## History header

Only bytes `0..64` of the 4096-byte header slot are defined by v1. The rest
must be zero when v1 creates the file and is ignored by the v1 reader.

| Offset | Size | Field | V1 value / meaning |
| ---: | ---: | --- | --- |
| 0 | 8 | `magic` | ASCII `FSQLHST1` |
| 8 | 2 | `format_version` | `1` |
| 10 | 2 | `header_len` | `64` |
| 12 | 4 | `slot_size` | `4096` |
| 16 | 2 | `record_v1_len` | `64` |
| 18 | 2 | `hash_algorithm` | `1` = BLAKE3-64 |
| 20 | 8 | `format_generation` | Nonzero history-coordinate generation |
| 28 | 16 | `database_history_id` | Nonzero persistent logical lineage ID |
| 44 | 8 | `database_generation` | Nonzero recovered database/WAL lineage generation |
| 52 | 8 | `header_blake3_64` | Header checksum described below |
| 60 | 4 | `reserved` | Zero in v1 |

The identity is deliberately not `fsqlite_vfs::FileIdentity`: that value is a
live filesystem coordination identity and is not stable across boots or
machines.

The header checksum is the first eight bytes of BLAKE3 over header bytes
`0..64`, with bytes `52..60` replaced by eight zero bytes. Those digest bytes
are interpreted as a little-endian `u64` for storage.

## Record prefix

The v1 record is exactly 64 bytes at the start of its slot.

| Offset | Size | Field | Meaning |
| ---: | ---: | --- | --- |
| 0 | 8 | `commit_seq` | Nonzero canonical durable commit coordinate |
| 8 | 8 | `catalog_root_page` | Historical catalog-root page coordinate |
| 16 | 8 | `wall_ts_unix_nanos` | Informational Unix timestamp in nanoseconds |
| 24 | 8 | `prev_record_blake3_64` | Predecessor's `this_record_blake3_64`, or zero for the first retained anchor |
| 32 | 8 | `this_record_blake3_64` | This record's checksum |
| 40 | 8 | `schema_epoch` | Schema epoch visible at this commit |
| 48 | 4 | `flags` | Bit 0 checkpoint anchor; all other v1 bits zero |
| 52 | 2 | `record_version` | `1` |
| 54 | 2 | `padding` | Zero |
| 56 | 8 | `reserved` | Zero in v1 |

The record checksum is the first eight bytes of BLAKE3 over all 64 bytes with
bytes `32..40` replaced by eight zero bytes. The digest bytes are interpreted
as a little-endian `u64`. Zeroing the checksum field removes the self-reference
in the original design sketch while keeping the promised 64-byte layout.

The first retained record has `prev_record_blake3_64 = 0` and must set the
checkpoint-anchor flag. Later records link exactly to their predecessor and
have strictly increasing `commit_seq` values. Gaps are permitted so retained
history can begin at an anchor after pruning.

### Why one catalog root is sufficient as a lookup coordinate

`catalog_root_page` is not a claim that one page contains the database. It is
the root of the historical schema catalog. At the record's `commit_seq`, that
catalog enumerates the table and index roots; the same retained snapshot
coordinate supplies database-header state, freelist/database-size state, and
all reachable page versions. If any part of that retained snapshot is absent,
the historical open must report `history not retained`. It must not use the
current catalog, current freelist, or current database size as a substitute.

This catalog-root interpretation replaces the provisional generic
`root_page` wording without expanding the stable record prefix.

### Sample v1 record

The sample uses:

- `commit_seq = 0x0102030405060708`
- `catalog_root_page = 0x1112131415161718`
- `wall_ts_unix_nanos = 0x2122232425262728`
- `prev_record_blake3_64 = 0`
- `schema_epoch = 0x3132333435363738`
- checkpoint-anchor flag and record version 1

The canonical 64 bytes are shown in 16-byte rows:

```text
0000: 08 07 06 05 04 03 02 01  18 17 16 15 14 13 12 11
0010: 28 27 26 25 24 23 22 21  00 00 00 00 00 00 00 00
0020: bb 10 69 7b c1 7f 30 ee  38 37 36 35 34 33 32 31
0030: 01 00 00 00 01 00 00 00  00 00 00 00 00 00 00 00
```

## Append and durability protocol

1. Open the history file read-write and take an exclusive advisory lock on
   the sidecar only. This serializes immutable tail publication, not database
   transaction execution or page ownership.
2. Validate the immutable header, all full slots, the hash chain, and the
   recovered durable commit horizon.
3. Construct the new record with the validated tail hash and write its entire
   4096-byte slot at the next slot boundary.
4. Issue `VfsFile::durable_sync(..., SyncKind::FullDurable)` before reporting
   the history append durable.
5. Release the sidecar lock.

On first creation, the complete header slot is full-durably synced and the
parent directory is synced before the file is advertised. A batch append may
write several immutable slots under one lock and one final full-durable barrier;
none is promised durable to the caller until that barrier succeeds.

Commit integration must order the live commit, retained page-version set, and
history append according to `docs/design/time-travel-file-backed.md`. This
format module refuses records beyond the caller's recovered durable horizon;
it does not guess durability from sidecar contents.

## Restart validation and repair

Startup validates forward from the anchor:

1. Header magic, dimensions, checksum, identity, and both generations.
2. Complete 4096-byte slot extent.
3. Record version, reserved fields, flags, and record checksum.
4. `prev_record_blake3_64 == predecessor.this_record_blake3_64`.
5. Strictly increasing commit sequence.
6. `commit_seq <= recovered_commit_horizon`.

Only a suffix can be repaired automatically:

- bytes after the last complete slot are a torn append and are truncated;
- a final complete slot with v1 structural or checksum corruption is a
  torn-tail candidate and is truncated;
- otherwise-valid records beyond the recovered horizon are truncated as an
  unrecovered suffix.

An invalid header, identity/generation mismatch, invalid anchor, or invalid
non-tail slot is a hard error. An unsupported record version, invalid record
semantics, short read, or VFS/storage error is also a hard error even in the
final slot; recovery propagates it without changing the file. Interior
corruption is never hidden by truncation. Tail truncation is followed by
`FullDurable` sync.

## Forward compatibility

A v1 reader decodes only bytes `0..64` of a complete record slot. Nonzero bytes
later in the slot are ignored, so a later format can append fields without
moving the v1 prefix. A different `record_version` is rejected until its prefix
semantics are defined; merely adding extension bytes does not require changing
the v1 record version.

## Optional sparse index

The index is a rebuildable cache, never an authority. It has a 4096-byte header
followed by packed 24-byte `{commit_seq, byte_offset, entry_checksum}` entries
sampled at record 0 and every 1024 records.

Index header v1:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | ASCII `FSQLHIX1` |
| 8 | 2 | format version `1` |
| 10 | 2 | header length `80` |
| 12 | 4 | entry length `24` |
| 16 | 4 | sample stride `1024` |
| 20 | 4 | reserved zero |
| 24 | 16 | `database_history_id` |
| 40 | 8 | `format_generation` |
| 48 | 8 | `database_generation` |
| 56 | 8 | exact history record count |
| 64 | 8 | exact final record hash, or zero for an empty log |
| 72 | 8 | BLAKE3-64 checksum of header bytes 0..80 with this field zeroed |

The remaining header-slot bytes are zero. Entries must be monotone and their
offsets must equal `(sample_record + 1) * 4096`. Each entry checksum is
BLAKE3-64 over a domain separator, database identity/generations, exact history
record count and final hash, entry ordinal, commit sequence, and byte offset.
This lets a cold lookup validate only the entries it probes without trusting a
torn entry or reading the entire index.

Rebuild publication is fail-closed:

1. truncate the optional cache and write a complete image with checksum zero;
2. full-durably sync that unpublished image;
3. write the header slot containing the final header checksum;
4. full-durably sync again and sync the parent directory on first creation.

A missing, zero-checksum, malformed, corrupt, wrong-identity, wrong-generation,
wrong-tail, or wrong-length index is ignored and rebuilt. Rebuild validation is
a streaming `O(N)` pass with constant record memory; it never materializes the
history. A cold lookup reads the tail-bound header, binary-searches entries on
disk, validates only those `O(log N)` probes against the authoritative history,
and then reads at most 1024 contiguous history records. It never scans every
sparse sample. An append changes the exact tail binding and forces a rebuild.

The 10-million-record Criterion benchmark is at
`tests/perf/history_bisect_bench.rs`; its runnable Cargo wrapper is
`crates/fsqlite-mvcc/benches/history_bisect_bench.rs`. It uses a synthetic
monotone corpus to isolate the linear-versus-sparse lookup algorithm. The
`ten_million_record_vfs_lookup_does_not_scan_the_full_history` unit test is the
production-path scale proof: it creates a 40.96 GB sparse logical history,
materializes only index samples and the selected refinement window, and runs
two cold `HistoryLog::lookup_floor` calls. It asserts the complete VFS read
budget from lookup entry: at most 15 index reads and at most 1044 history reads.
Any regression to `read_all`, whole-index loading, or all-sample history
validation fails the budget (and normally encounters an absent history slot).

## Cross-endian stability

Every integer is encoded with `to_le_bytes` and decoded with `from_le_bytes`;
native-layout serialization is forbidden. The record golden test runs
unchanged on x86_64 and aarch64. The release proof compares the complete golden
bytes from both targets, not merely decoded field values.
