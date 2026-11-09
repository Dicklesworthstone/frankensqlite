# WAL reopen batching for Beads #598

Related work: `bd-1dp9.6.7.8`, the
[generation/index contract](../design/wal-generation-index-authoritative-lookup-contract.md),
and [Beads #598](https://github.com/Dicklesworthstone/beads_rust/issues/598).
The implementation baseline is `074c2033b4dd5212bf2f6c6aa80480ff711dfd9f`.

## What the reproduced double scan means

An independent run of the official Beads 0.7.6 Linux x86_64 binary reproduced
the reporter's exact WAL sizes and read counts. A WAL with 11,841 frames caused
23,690 full-frame reads and 23,684 frame-header reads for both default and
explicit no-auto-sync `br show`. A separate 28-frame trace showed two WAL opens,
56 `startup_open` decisions, and no rebuild/refresh replay.

Those scans belong to two Beads connections: an advisory pending-merge
inspector, followed by startup/command storage. Reusing the already admitted
read-only snapshot in Beads removes that duplication. The startup correction
is committed to Beads main as
[`8f627089`](https://github.com/Dicklesworthstone/beads_rust/commit/8f627089545aa5b245833bb76bca27fbda67674d),
with the fresh reproduction records. It does not eliminate the engine's cold
recovery obligation.

## Engine change

`WalFrameReader` batches the required sequential frame reads in `WalFile::open`,
`rebuild_state_from_file`, `refresh`, and `refresh_preserving_append_prefix`.
It retains one bounded buffer and yields complete frames to the existing
validation loops. The scanner does not own or publish recovery state.

For a valid WAL with reads that fill the requested buffer, the same bytes are
read once per scan. At a 4,096-byte page
size, each 4,120-byte frame fits in a 15-frame batch of 61,800 bytes. Thus
11,841 frames require 790 data-read requests instead of 11,841. These are
request-count consequences of the implementation, not measured latency or CPU
speedups. The limit is 64 KiB, except that a maximum-size 65,536-byte page
requires one indivisible 65,560-byte frame, as before.

The publication adapter remains unchanged. An evaluated header-batching patch
would reduce its syscall count but increase its read volume from 284,184 bytes
to about 45.5 MB at the reported WAL size. It is excluded pending measurements.
The native probe measures both WAL open alone and open plus the unchanged
publication adapter so that this remaining cost stays visible.

## Recovery proof and limits

- Every admitted frame still passes the original salt and full-payload rolling
  checksum checks. Commit markers, the last committed prefix, and append seeds
  are selected by the same validation loops.
- The upper frame bound comes from the original file-size sample. Appends
  during a batch cannot silently extend that recovery horizon; a later refresh
  validates the new suffix.
- A short batch yields all complete preceding frames. Since the public VFS
  contract does not make every short read an EOF indication, scanning resumes
  at the first unconsumed frame boundary. A partial fragment is reread, and a
  response shorter than one whole frame stops recovery as in the original
  frame-at-a-time loop. Bytes beyond the returned length are never used.
- I/O errors propagate. No frame is accepted merely because a larger read
  failed. Read-ahead may encounter an I/O failure beyond a corrupt frame where
  the old one-frame reader would already have stopped; that case conservatively
  refuses instead of recovering the earlier prefix.
- Rebuild still invalidates previous sync authority and revalidates the whole
  sampled WAL, including equal-length generations. Reset/truncate and
  same-salt checkpoint-sequence changes keep their existing admission rules.
- Recovery performs no reader-side WAL write, truncation, or sync. Native SHM
  recovery, physical header probes, and publication atomicity are unchanged.

Matching WAL header, size, generation, and terminal checksum do not prove the
middle payloads remain intact. The regression suite explicitly changes a middle
page while leaving the header and terminal checksum unchanged. Ordinary
incremental refresh and explicit crash recovery have different obligations;
full recovery must still discover that damage. A SHM header checksum also does
not authenticate its index arrays or all persisted WAL payloads. No shared
index trust shortcut, cross-process validation cache, or O(1) cold-open claim
is introduced here. The broader authoritative-index bead remains open.

## Regression and performance entry points

`crates/fsqlite-wal/tests/wal_reopen.rs` adds ten public-API regressions for
512/4096/65536-byte pages, batch-edge payload/salt/checksum corruption, short
headers and pages, capped non-EOF reads, read errors and retry, uncommitted suffixes, same-salt
equal-length reset, unchanged-tail middle damage, retained append prefixes,
and a deterministically scheduled peer-descriptor append after the sampled horizon. The audit
wrapper also checks reader mutation neutrality and complete byte coverage.

Existing WAL unit tests and the `bd_xfn30_3_fault_injection_matrix` and
`bd_db300_7_2_3_recovery_invariants` targets remain recovery controls. The latter
requires the `fault-injection` feature. Existing native recovery/header fault
injectors still fire on their original physical operations.
The new `wal_reopen` integration target is a local keeper: the repository's
currently disabled workflow allowlists do not execute it. It requires an
explicit `cargo test -p fsqlite-wal --test wal_reopen` run; adding its source
does not establish an automated gate or a passing result.

`crates/fsqlite-core/examples/wal_reopen_probe.rs` and
`scripts/bench_wal_reopen.py` compare two actual builds against identical,
immutable native WAL files. The controller alternates the binaries in one
invocation, records source/build/binary identities, separates instrumented
reads from timings, checks checksum/index results, and verifies the WAL hash
afterward. It is a warm-filesystem component probe; Beads' separate CLI harness
is required for end-to-end `br show` claims.

## Validation status on 2026-11-09

The changed WAL source and new tests pass formatting checks, the native example
passes its separate formatting check (the repository formatter intentionally
skips the core tree), the Python controller parses, and patch whitespace is
clean. Independent source review checked all four callers and existing
read-fault injectors. The excluded adapter file is byte-identical to baseline.

Review found and corrected an initial assumption that every short batched read
means EOF. The actual VFS contract only promises an actual byte count and zero
filling. The tenth regression checks capped reads with and without partial
fragments; two independent source reviews confirmed the aligned-resume fix.

**The changed WAL crate and its new Rust regressions have not compiled or
executed successfully in this environment. No native component timing has
executed.** A focused native `fsqlite-wal --test wal_reopen` build used the
pinned nightly, the locked/offline dependency graph, one Cargo job, and isolated
compiler-memory settings. The first admission was held for memory; a second
attempt was interrupted without a final result. The resumed third attempt
completed type checking of unchanged Asupersync 0.6.2, then rustc was killed by
SIGKILL before compiling the changed WAL crate. Cargo exited 101 after 955.5
seconds, peak child RSS was 4,073,404 KiB, and the host OOM counter increased
from 8 to 9. The source hashes were unchanged throughout that attempt. These
are build-resource failures, not observed Rust-test failures or passes.

The [validation manifest](../../crates/fsqlite-wal/tests/artifacts/wal-reopen-598/manifest.json)
and [raw build records](../../crates/fsqlite-wal/tests/artifacts/wal-reopen-598/validation.tar.gz)
retain exact commands, source snapshots/hashes, compiler settings, logs, and
receipts. Three earlier builds of the unchanged Beads baseline were also killed
in its Asupersync 0.5.0 dependency; their records are in the linked Beads commit.
No dependency features, lockfile, or recovery expectations were weakened to
obtain a pass, and no further heavy compilation was started after this result.

The new WAL regressions, existing WAL library and fault-injection controls,
relevant native recovery keepers, all-target check/Clippy, and paired component
measurements remain required qualification. The component example is also
uncompiled. Syscall-count arithmetic above is not a measured CPU/latency result;
this source-reviewed implementation is not release qualification.

## Beads dependency boundary

The Beads startup correction uses its existing published dependencies. Beads'
WAL pin is 0.4.12 and its Asupersync pin is 0.5.0; this development workspace is
0.4.13 with Asupersync 0.6.2. No Beads dependency override or unpublished release
version is introduced. Engine adoption requires a compatible published family
and the existing Beads engine-upgrade qualification, including library/model/
linearizability tests and both retained-family stress runs.
