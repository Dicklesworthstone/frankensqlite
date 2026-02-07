# COMPREHENSIVE SPEC FOR FRANKENSQLITE V1 (CODEX)

**Document status:** Draft v1 (agent companion, kept in sync with canon)  
**Repo:** `/data/projects/frankensqlite`  
**Target oracle:** SQLite 3.52.0 (C reference in `legacy_sqlite_code/`)  
**RaptorQ bible:** `docs/rfc6330.txt` (RFC 6330)

This document is intentionally **self-contained**: it includes the goals, constraints, architecture, formal semantics, verification strategy, and execution plan. It is written to be handed to other agents without requiring them to read any other docs first.

**Canon / precedence:**

- `AGENTS.md` is the operational ruleset for agents.
- `Cargo.toml` and `rust-toolchain.toml` are ground truth for toolchain, workspace membership, and build profiles.
- `COMPREHENSIVE_SPEC_FOR_FRANKENSQLITE_V1.md` is the full canonical spec.
- This file is the *Codex companion* (more operational, sometimes more concrete).

If this file conflicts with `AGENTS.md`, `Cargo.toml`, `rust-toolchain.toml`, or `COMPREHENSIVE_SPEC_FOR_FRANKENSQLITE_V1.md`, treat those as authoritative and update this file to match.

---

## Table of Contents

0. What We Are Building (One Paragraph)
0.1 Glossary + Normative Language (MUST/SHOULD/MAY)
1. Non-Negotiables (Hard Constraints)
2. Success Definition (What “Parity” Means)
3. The Big Idea: “Erasure-Coded Streams” Everywhere
3.1 RaptorQ Primer (RFC 6330 Terms We Use)
3.2 RaptorQ Permeation Map (Every Pore, Every Layer)
4. Operating Modes (Compatibility vs Native)
5. Core Data Model (Formal-ish, Testable)
6. ECS Storage Substrate (Objects, Symbols, Physical Layout)
7. Concurrency: MVCC + SSI (Serializable by Default)
8. Safe Write Merging (Intent + Structured Patches)
9. Durability: Coded Commit Stream (Protocol + Recovery + Compaction)
10. The Radical Index: RaptorQ-Coded Index Segments (Lookup, Repair, Rebuild)
11. Caching & Acceleration (ARC, Bloom/Quotient Filters, Hot Paths)
12. Replication: Fountain-Coded, Loss-Tolerant, Late-Join Friendly
13. Asupersync Integration Contract (Cx, LabRuntime, DPOR, TLA+ Export)
14. Conformance Harness (The Oracle Is The Judge)
15. Performance Discipline (Extreme Optimization)
16. Implementation Plan (V1 Phase Gates)
17. Risk Register + Open Questions
18. Local References (Canon)

---

## 0. What We Are Building (One Paragraph)

**FrankenSQLite** is a clean-room, safe-Rust reimplementation of SQLite that keeps **SQLite’s SQL semantics and public API** (as proven by a conformance harness against C SQLite 3.52.0), while replacing SQLite’s single-writer bottleneck with **concurrent writers** and upgrading durability + replication by making **RaptorQ erasure coding** the universal substrate for persistence, recovery, and synchronization. The guiding idea is that the database is not “a file with a WAL”, it is an **information-theoretically optimal, self-healing, erasure-coded stream of commits** that can be stored locally, repaired after crashes, and replicated over lossy networks without fragile reliable-transport assumptions.

---

## 0.1 Glossary + Normative Language

### Normative Language

We use RFC-style terms:

- **MUST**: required for correctness, conformance, or project constraints.
- **SHOULD**: strong default; deviations require justification and tests.
- **MAY**: optional enhancement or later-phase refinement.

### Canonical Terms

- **Oracle**: the authoritative reference implementation (C SQLite 3.52.0 built from `legacy_sqlite_code/`).
- **Conformance harness**: the machinery that runs the same input against Oracle + FrankenSQLite and compares outputs.
- **ECS (Erasure-Coded Stream)**: our universal persistence/replication substrate: a stream of **objects**, each encoded into **RaptorQ symbols**.
- **Object**: a byte payload + canonical header, encoded as RaptorQ symbols. Examples: `CommitCapsule`, `CommitMarker`, `IndexSegment`, `SnapshotManifest`, `PageHistory`.
- **OTI**: Object Transmission Information (RFC 6330), the parameters required to decode.
- **Symbol**: an encoding symbol produced by the RaptorQ encoder (source or repair). The unit of storage and replication.
- **Symbol store**: a component that can persist and later serve symbols (local disk, remote replicas, simulated lab transport).
- **Commit capsule**: the primary commit payload (what changed + evidence).
- **Commit marker**: the atomic “this commit exists” record; if a marker is durable, the commit is considered durable.
- **Index segment**: a (coded) object that accelerates lookups (page → latest patch pointer, object locator maps, filters).
- **Compatibility view**: a materialized SQLite-compatible `.db`/`.wal` view produced from ECS state for tooling/oracle parity.
- **Native mode**: ECS is the source-of-truth; compatibility views are derived artifacts.

### What “RaptorQ Everywhere” Means (No Weasel Words)

RaptorQ is not an “optional replication feature”. It is the default substrate for:

- durability objects (commit capsules, markers, checkpoints)
- indexing objects (index segments, locator segments)
- replication traffic (symbols, not files)
- repair (recover from partial loss/corruption by decoding, not by panicking)
- compression of history (patch chains stored as coded objects, not infinite full-page copies)

If a subsystem persists or synchronizes bytes, it MUST specify how those bytes are represented as ECS objects and how they are repaired/replicated.

---

## 1. Non-Negotiables (Hard Constraints)

### 1.1 Engineering / Process Constraints

These are hard project constraints for all work:

- **User is in charge.** If the user overrides anything, follow the user.
- **No file deletion** without explicit written permission.
- **No destructive commands** unless the user explicitly provides the exact command and confirms they want the irreversible consequences. This includes but is not limited to `git reset --hard`, `git clean -fd`, and `rm -rf`.
- **Branch:** `main` only (the legacy default-branch name must not appear in docs or code).
- **Rust toolchain:** nightly, edition 2024.
- **No `unsafe`** anywhere: workspace lints forbid unsafe code.
- **Clippy:** `pedantic` + `nursery` denied at workspace level; warnings are treated as errors.
- **No script-based code transformations.** Manual edits only.
- **No file proliferation** unless genuinely necessary (this spec file exists only because the user explicitly requested a single canonical spec).
- **After substantive code changes:** run `cargo check --all-targets`, `cargo clippy --all-targets -- -D warnings`, `cargo fmt --check`, and tests.
- **Use `br`** for tasks/issues and dependencies (Beads). Use `bv --robot-*` for triage. (This spec includes a plan so we don’t have to read beads to understand direction, but beads remain the execution substrate.)

### 1.2 Library / Dependency Constraints

- **All async/network/I/O patterns must use** `/dp/asupersync`.  
  - No Tokio. No “I’ll just use async-std”. No bespoke runtimes.
- **All console/terminal rendering must use** `/dp/frankentui`.
- **“RaptorQ-ness” must permeate the whole design.** RaptorQ is not a side feature; it is the organizing principle.

### 1.3 Workspace Structure (What Exists In This Repo)

FrankenSQLite is a Cargo workspace with crates under `crates/`. The intent is strict layering (SQL layer cannot reach into VFS; parser cannot grab pager internals, etc.).

Core crates (high-level roles):

- `crates/fsqlite-types`: foundational types and constants (page ids, record encoding, opcodes, flags).
- `crates/fsqlite-error`: unified error types and SQLite-ish error code mapping.
- `crates/fsqlite-vfs`: VFS traits + implementations (MemoryVfs, UnixVfs). All OS I/O must go through here.
- `crates/fsqlite-pager`: pager + cache policy plumbing + page buffer lifecycle.
- `crates/fsqlite-wal`: durability substrate (compat-WAL and/or native ECS-backed persistence hooks).
- `crates/fsqlite-mvcc`: MVCC + SSI + commit capsule formation + conflict reduction machinery.
- `crates/fsqlite-btree`: B-tree engine (spec-driven).
- `crates/fsqlite-ast`: typed AST nodes.
- `crates/fsqlite-parser`: tokenizer + parser.
- `crates/fsqlite-planner`: planner / optimizer.
- `crates/fsqlite-vdbe`: bytecode VM + opcode semantics.
- `crates/fsqlite-func`: built-in functions (scalar/aggregate/window).
- `crates/fsqlite-ext-*`: extensions (FTS, JSON1, RTree, Session, ICU, misc) as compile-time features.
- `crates/fsqlite-core`: wires everything into an engine.
- `crates/fsqlite`: the public API facade.
- `crates/fsqlite-cli`: CLI shell binary (must use frankentui).
- `crates/fsqlite-harness`: conformance runner and oracle comparison.

This spec defines the “true end-state” architecture for these crates, even if many are currently stubs.

---

## 2. Success Definition (What “Parity” Means)

We are not aiming for “close enough”. We want:

### 2.1 Behavioral Parity

- **SQL semantics parity** with C SQLite 3.52.0 for the chosen surface: query results, NULL semantics, type conversions, constraint behavior, PRAGMAs, error codes (normalized where needed).
- **Public API parity**: connection lifecycle, prepared statements, transactions/savepoints, extension surface (as scoped), pragmas, and error mapping.

### 2.2 Concurrency Semantics (Important Caveat)

SQLite’s single-writer design makes many anomaly classes impossible “by construction”. If we add concurrency, we must not silently weaken correctness.

Therefore:

- **Default isolation target:** **Serializable** behavior for concurrent writers (not just Snapshot Isolation).  
  - We achieve this by implementing **Serializable Snapshot Isolation (SSI)** with page-granularity tracking and aborts on dangerous structures.
- We may expose explicit modes for compatibility or experimentation, but **defaults must not silently introduce write skew**.

### 2.3 Durability + Crash Recovery

We must define a crash model and then meet it. Additionally, with RaptorQ we explicitly target **stronger-than-SQLite** resilience against corruption/erasures:

- SQLite-style fsync ordering still exists as a baseline guarantee.
- But we add **erasure-coded repair** so that bounded corruption/torn writes can be corrected rather than merely detected.

### 2.4 Performance

We will not “optimize later”. Performance is tracked from day one:

- Baselines first (criterion microbenches + macrobenches).
- Profile-driven changes only.
- Correctness proven by oracle conformance for every optimization change.

---

## 3. The Big Idea: “Erasure-Coded Streams” Everywhere

SQLite’s classic architecture:

```
DB file + WAL file + WAL index + checkpoints
```

FrankenSQLite’s architecture is reorganized around a single universal abstraction:

> **Erasure-Coded Stream (ECS):** an append-only stream of *objects*, where each object is encoded as RaptorQ symbols such that any `K` of the transmitted/stored symbols suffice to reconstruct the object. Objects can be stored locally, distributed across replicas, repaired after partial loss, and replayed to reconstruct state.

This is the database engine reframed as information theory:

- **Durability** becomes: “did we persist enough symbols for the commit object under the declared loss model?”
- **Replication** becomes: “can replicas collect enough symbols for each commit object?”
- **Recovery** becomes: “if some symbols are corrupt/missing, can we decode the object anyway?”
- **Indexing** becomes: “can we represent index state as encoded objects too, so indices are self-healing and replication-native?”

This is “RaptorQ in every pore”: not only for snapshot shipping, but for WAL, WAL index, page versions, and MVCC history compression via delta patches.

---

## 3.1 RaptorQ Primer (RFC 6330 Terms We Use)

RaptorQ (RFC 6330) is a systematic fountain code for object delivery.

We use the following conceptual mapping:

- **Object** (RFC): the byte string we want to deliver/durably store/replicate.
  - In FrankenSQLite: a commit capsule, an index segment, a snapshot manifest chunk, etc.
- **Source symbols**: the `K` “original” blocks (systematic encoding) derived from the object at a chosen symbol size.
- **Repair symbols**: additional encoded blocks that provide redundancy.
- **Decoding**: reconstruct the original object once enough symbols are available.
- **OTI (Object Transmission Information)**: the parameters required to interpret symbol ids and decode correctly.
  - In FrankenSQLite: this must be present in the ECS object header, and is treated as *part of the object’s canonical identity*.

We treat RFC 6330 as normative. In practice:

- The canonical reference text is vendored at `docs/rfc6330.txt`.
- `/dp/asupersync/src/raptorq/` provides an RFC 6330-grade implementation and is the only RaptorQ implementation we use.

Implementation note (practical API surface we will bind to):

- `asupersync::config::RaptorQConfig` for validated parameterization
- `asupersync::raptorq::RaptorQSenderBuilder` / `RaptorQReceiverBuilder` for pipeline construction

### 3.1.1 Overhead, Failure Probability, and Defaults (Operational Guidance)

RaptorQ is “any K symbols suffice” in the *engineering* sense, but the decode success probability at exactly `K` is not literally 1. The whole point of repair symbols is to drive decode failure probability into the floor.

Terminology we use consistently:

- `K_block = ObjectParams.symbols_per_block` (source symbols per source block)
- `B = ObjectParams.source_blocks` (number of source blocks)
- `K_total = K_block * B` (total source symbols across the whole object)

Rules of thumb (backed by RFC 6330 guidance and typical RaptorQ behavior):

- decoding with **exactly K** received symbols can very rarely fail (still extremely good compared to older fountain codes)
- decoding with **K+1** or **K+2** received symbols makes failure probability effectively negligible for our purposes

Therefore, we define a default redundancy policy:

- **V1 default:** aim to persist/replicate enough symbols that a decoder can almost always collect **K+2** symbols for each source block without coordination.
- expose knobs via PRAGMAs (see §9.3, §9.9, §12.6):
  - `PRAGMA raptorq_overhead = <percent>`
  - `PRAGMA raptorq_repair_symbols = ...` (compatibility view policy)

We also respect RFC 6330 source-block limits by chunking large objects into multiple source blocks via `ObjectParams`.

**Why a primer belongs here:** Every agent implementing any durable or replicated path must understand the meaning of `K`, symbol sizing, overhead, and OTI. RaptorQ is not “a compression trick”; it is the algebra of our durability and replication contracts.

---

## 3.2 RaptorQ Permeation Map (Every Pore, Every Layer)

This is the “no excuses” mapping from subsystem → ECS/RaptorQ role.

**Durability plane (disk):**

- commits: `CommitCapsule` + `CommitMarker` objects (always ECS)
- checkpoints: chunked snapshots as ECS objects (always ECS)
- indices: IndexSegments as ECS objects (always ECS)
- repair: decode from surviving symbols; produce `DecodeProof` in lab/debug builds

**Concurrency plane (memory):**

- MVCC history: PageHistory objects (patch chains encoded as ECS objects)
- conflict reduction: intent logs are small ECS objects, replayed deterministically for rebase merge
- explainability: abort witnesses are attachable artifacts (lab builds)

**Replication plane (network):**

- transport primitives are symbol-native (`SymbolSink`/`SymbolStream`)
- anti-entropy uses IBLT set reconciliation (O(Δ) in symmetric difference), with a segment-hash fallback
- bootstrap is “stream checkpoint chunks until decode”

**Observability plane (alien-artifact explainability):**

- decode proofs when repair happens
- deterministic trace capture + replay under `LabRuntime`
- e-process monitors for invariants (anytime-valid alarms)
- TLA+ export of traces for bounded model checking of commit/replication/recovery

**Wild but aligned experiments (encouraged, feature-gated):**

- **Inter-object coding for replication:** treat a batch of objects as a “super-object” so a sender can transmit mixed symbols that maximize throughput over lossy links.
- **Symbol-level RAID on a single machine:** distribute symbols across multiple local devices/paths; any `K` reconstructs (RAID-like redundancy without strict striping).
- **Integrity sweeps as information theory:** periodically sample symbols and attempt partial decodes; use e-process monitors to detect elevated corruption rates early.

If a new feature persists bytes or ships bytes, it MUST declare its ECS object type, symbol policy (K/R), and repair story.

---

## 4. Two Operating Modes (So We Can Prove Parity While Inventing The Future)

We need to be bold without losing the ability to verify parity. We therefore define two modes:

### 4.1 Compatibility Mode (Oracle-Friendly)

Purpose: quickly prove SQL/API correctness against C SQLite 3.52.0.

- DB file is standard SQLite format.
- WAL frames are standard SQLite WAL frames (or a minimally compatible derivative, explicitly documented).
- We may write *extra* sidecars (e.g., `.wal-fec`, `.idx-fec`) but the core `.db` stays SQLite-compatible when checkpointed.

### 4.2 Native Mode (RaptorQ-First)

Purpose: maximum concurrency + durability + replication; “the future”.

- Primary durable state is an ECS commit stream (commit capsules).
- Checkpointing can materialize a canonical `.db` for compatibility export, but the source-of-truth is the commit stream and its encoded objects.

Both modes must be supported by the **same SQL/API layer**. Conformance harness validates behavior, not internal format.

---

## 5. Core Data Model (Formal-ish, Testable)

We keep this section intentionally executable: every predicate here must map to unit tests, property tests, and lab runtime schedule exploration.

### 5.1 Identifiers and Types

- `TxnId`: monotone increasing transaction id.
- `CommitSeq`: monotone increasing commit sequence number (the durable “commit clock”).
- `SnapshotId`: opaque identifier for a captured snapshot basis (can be derived from `CommitSeq` + in-flight set digest).
- `Pgno`: page number (1-based).
- `PageSize`: configured page size.
- `ObjectId`: content-addressed id of an ECS object (e.g., hash of canonical object header + source bytes).
- `SymbolId`: (object id, encoding symbol id) per RFC 6330.
- `Digest`: integrity hash (algorithm is versioned in object headers).

### 5.2 The ECS Object

An **ECS object** is the unit of RaptorQ encoding.

Examples:

- a commit capsule (page diffs + metadata)
- a WAL segment
- an index segment
- a snapshot manifest chunk

Each object has:

- `object_header`: versioned, canonical encoding (byte-exact), includes:
  - `object_type`
  - `object_len`
  - `encoding_params` (symbol size, K, etc; RFC 6330 derived)
  - `integrity` (hash / checksum and algorithm id)
  - `links` (parents / dependencies; see §9.4)
- `source_bytes`: the payload to be encoded

RaptorQ encoding produces:

- `K` **source symbols** (systematic)
- optionally `R` **repair symbols** (overhead), such that receiving any `K` symbols from the union suffices (with high probability, but we treat RFC 6330 compliance as the operational contract)

#### 5.2.1 Canonical Encoding (Deterministic Bytes, Not “Serde Vibes”)

If `ObjectId` is content-derived, then object headers MUST be canonically encoded:

- explicit versioned wire format
- explicit endianness (little-endian for fixed-width ints)
- explicit map ordering (sorted keys)
- no floating-point in canonical headers

We do not use JSON for canonical bytes. JSON is for fixtures and human interchange, not identity.

### 5.3 Commit Capsule (The Heartbeat)

We define a commit capsule object type:

```
CommitCapsule {
  snapshot_basis: SnapshotId,

  // Preferred: semantic intent log (merge/rebase friendly)
  intent_log: Vec<IntentOp>,

  // Optional: materialized patches for fast reads / recovery acceleration
  page_deltas: Vec<PageDelta>,

  // Explainability / conformance witnesses (feature-gated in release)
  read_set_digest: Digest,
  write_set_digest: Digest,
  ssi_witnesses: Vec<SsiWitness>,

  // DDL and other metadata
  schema_delta: Option<SchemaDelta>,
  checks: CommitChecks,
}
```

`commit_seq` is carried by `CommitMarker` (the commit clock), not by the capsule payload. This lets us:

- build/content-address the capsule deterministically before publish
- allocate `commit_seq` strictly at marker append time (preserving total order without head-of-line blocking)

Where `PageDelta` is not necessarily “a full page image”. It can be:

- full page image (baseline)
- sparse byte-range patch (preferred)
- algebraic delta (GF(256) vector / XOR patch) for encoding/history compression (merge eligibility is semantic/structured; see §8)

The capsule is encoded into RaptorQ symbols and persisted/distributed via ECS.

### 5.4 Commit Marker (The Commit Clock)

A commit marker is the atomic publish record:

```
CommitMarker {
  commit_seq: u64,
  capsule_object_id: ObjectId,
  prev_marker: Option<ObjectId>,
  integrity: Digest,
}
```

If the marker is committed, the commit is committed. Recovery replays markers in order and ignores unmarked capsules.

---

## 6. ECS Storage Substrate (Objects, Symbols, Physical Layout)

This section is the “steel beam” of the whole system. If ECS is underspecified, everything else becomes vibes.

### 6.1 ECS Invariants (What We Rely On)

ECS is defined by these invariants:

1. **Append-only first:** the primary durable structures are append-only streams of records. Mutations occur via new objects and new pointers, not in-place rewrites.
2. **Self-description:** a symbol record MUST carry enough metadata to be routed, authenticated, and decoded without out-of-band state.
3. **Repair-first recovery:** corruption/loss is handled by *decoding from any sufficient subset*, not by assuming perfect disks/networks.
4. **Determinism in tests:** given identical inputs (seed, object bytes), encoding and scheduling MUST be reproducible under `asupersync::LabRuntime`.
5. **One tiny mutable root:** we allow a minimal mutable “root pointer” file for bootstrapping (like git refs). Everything else is append-only and/or content addressed.

### 6.2 ECS Object Params = RaptorQ Decode Params

We standardize on asupersync’s `ObjectParams` as our concrete OTI carrier:

- `object_size` (bytes)
- `symbol_size` (bytes)
- `source_blocks` (SBN count)
- `symbols_per_block` (K)

These are exactly the parameters needed to know “how many symbols are enough” and how to interpret `(SBN, ESI)` within the object.

### 6.3 Object Identity (ObjectId) and Content Addressing

We use `asupersync::types::symbol::ObjectId` as the on-the-wire object id type.

**V1 policy (native mode):**

- `ObjectId` MUST be *deterministically derived* from canonical bytes, not randomly generated.
- The derivation MUST be stable across machines and runs.
- The derivation MUST be independent of physical storage location.

Recommended construction:

```
object_id = Trunc128( BLAKE3( "fsqlite:ecs:v1" || canonical_object_header || payload_hash ) )
```

Notes:

- `payload_hash` SHOULD be BLAKE3 as well (crypto + speed), but the exact hash choice is a decision we can change behind a versioned header as long as conformance fixtures pin it.
- Even if we later add authenticated symbol transport, the object id remains a *content identity*, not a security credential.

### 6.4 ECS Symbol Record (On-Disk + On-Wire Envelope)

We store and replicate **symbol records**. Each record wraps one RaptorQ symbol payload plus the metadata required to validate it.

Symbol record fields (conceptual):

```
EcsSymbolRecordV1 {
  magic: [u8; 4] = *b"FSQ1",
  version: u16 = 1,

  // Decode params (OTI)
  object_id: ObjectId (128-bit),
  object_size: u64,
  symbol_size: u16,
  source_blocks: u8,
  symbols_per_block: u16, // K

  // Symbol identity
  sbn: u8,
  esi: u32,
  kind: u8, // 0=source, 1=repair

  // Payload
  payload_len: u16, // must equal symbol_size except possibly last source symbol padding rules
  payload_bytes: [u8; payload_len],

  // Integrity/auth
  frame_xxh3_64: u64,              // fast corruption detection for local storage
  auth_tag: Option<AuthenticationTag>, // present when security context enabled (asupersync)
}
```

**Why OTI is repeated in every record:** it makes every record independently decodable/routable and removes “index chicken-and-egg” during recovery and replication. This is deliberate redundancy in service of self-healing.

### 6.5 Local Physical Layout (Native Mode)

For a database path `foo.db`, native ECS state lives under `foo.db.fsqlite/ecs/`:

- `root` (small mutable file, updated via atomic rename):
  - points to the latest `RootManifest` object id
  - includes a tiny redundant payload (e.g., the `RootManifest`’s canonical header + hash) so startup can sanity check quickly
- `symbols/segment-000000.log` (append-only symbol records)
- `symbols/segment-000001.log` (rotated by size)
- `markers/segment-000000.log` (append-only commit marker records; also ECS objects, but we keep a fast sequential stream for scanning)

We also allow derived caches (rebuildable):

- `cache/object_locator.cache` (accelerator mapping object_id → offsets of symbol records)
- `cache/page_index.cache` (accelerator mapping pgno → latest page-update pointer)

**Rule:** Caches MUST be safely deletable and reconstructable by scanning the append-only logs plus the `root` pointer.

### 6.6 RootManifest (The Bootstrapping Anchor)

`RootManifest` is an ECS object whose payload declares:

- current configuration (page size, symbol sizing policy, hash algorithms)
- tips (latest pointers) for:
  - commit marker stream
  - index segment stream(s)
  - snapshot manifest stream
  - compaction/checkpoint generations
- compatibility view status (if enabled)

Startup:

1. Read `ecs/root`
2. Fetch/decode the referenced `RootManifest` (repairing via symbols if needed)
3. Use its tips to locate the latest index segments and commit markers
4. Rebuild any missing caches if configured

### 6.7 Object Retrieval (Decoder-Centric)

To retrieve object bytes:

1. Obtain `ObjectParams` for the object.
   - from any symbol record header (preferred)
   - or from `RootManifest` / index segments
2. Collect symbols until decode succeeds (typically ≥`K_total`, often with a small redundancy margin).
   - from local symbol logs
   - from remote peers (replication)
3. Decode via `asupersync::raptorq::RaptorQReceiver`.
4. Validate payload hash (and optionally decode proof, see below).

### 6.8 Decode Proofs (Auditable Repair)

Asupersync includes a `DecodeProof` facility (`asupersync::raptorq::proof`). We exploit this in two ways:

- In **lab runtime**: every decode that repairs corruption MUST produce a proof artifact attached to the test trace.
- In **replication**: a replica MAY demand proof artifacts for suspicious objects (e.g., repeated decode failures), enabling explainable “why did we reject this commit?” answers.

### 6.9 Deterministic Encoding (Required For Content-Addressed ECS)

If `ObjectId` is content-derived, symbol generation must be deterministic:

- The set of source symbols is deterministic by definition (payload chunking).
- Repair symbol generation MUST be deterministic for a given object id and config.

Practical rule:

- Derive any internal “repair schedule seed” from `ObjectId` (e.g., `seed = xxh3_64(object_id_bytes)`), and wire it through `RaptorQConfig` or sender construction as needed.

This makes “the object” a platonic mathematical entity: any replica can regenerate missing repair symbols (within policy) without coordination.

### 6.10 Symbol Size Policy (Object-Type-Aware, Measured)

Symbol size is a major performance lever:

- too small: too many symbols, higher metadata overhead, more routing work
- too large: worse cache behavior, higher per-symbol loss impact, more wasted decode work

We therefore choose symbol size per object type, with sane defaults and benchmark-driven tuning:

- `CommitCapsule` (page deltas / intent chunks):
  - default `symbol_size = min(page_size, 4096)` (symbol size is `u16`-bounded; if `page_size = 65536`, we chunk)
  - rationale: aligns encoding units with page boundaries without exceeding RFC/asupersync sizing
- `IndexSegment`:
  - default `symbol_size` in the 1–4 KiB range (often 1280 or 4096 depending on size)
  - rationale: segment payloads are metadata-heavy; smaller symbols can reduce tail loss impact
- `CheckpointChunk`:
  - default larger `symbol_size` (e.g., 16–64 KiB, capped at `u16::MAX`) when shipping over reliable local disk, but MAY fall back to page-sized for compatibility export

All of this is versioned in `RootManifest` so replicas decode correctly.

---

## 7. Concurrency: MVCC + SSI (Serializable by Default)

### 7.1 Surface Semantics (What Users See)

SQLite’s concurrency is “serial by single-writer lock”. FrankenSQLite’s concurrency is “serializable by validation”. We MUST not silently downgrade correctness.

User-visible contract:

- **Default isolation:** serializable.
- **Readers never block writers** and **writers never block readers**.
- **Writers do not wait while holding locks.** If a required lock is unavailable, the operation fails fast with a retryable error (`BUSY`-class).

API / SQL surface (proposed, aligned to the canonical spec’s *Serialized vs Concurrent* modes):

- `BEGIN` / `BEGIN DEFERRED` / `BEGIN IMMEDIATE` / `BEGIN EXCLUSIVE`:
  - start a **Serialized** transaction (single-writer semantics for parity and safety)
  - this is the default in V1 conformance runs (SQLite is the oracle)
- `BEGIN CONCURRENT`:
  - start a **Concurrent** transaction (page MVCC + first-committer-wins + SSI by default)
- `PRAGMA fsqlite.serializable = ON|OFF` (applies to **Concurrent** mode only):
  - `ON` (default): SSI validation enabled (serializable)
  - `OFF`: Snapshot Isolation (SI) allowed as an explicit opt-in (benchmarking / permissive apps only)

Error mapping:

- **Page lock conflict** (could not acquire a page write lock): `SQLITE_BUSY`.
- **Commit-time conflict** (first-committer-wins) or **SSI abort**: `SQLITE_BUSY_SNAPSHOT` (retryable).

Note: “fail fast” is an engine rule. SQLite-style `busy_timeout` behavior can be implemented in the API layer by retrying with backoff; we do not block while holding multiple resources.

### 7.2 Formal MVCC Model (Executable Definitions)

We work at **page granularity** (with optional refinement via range/cell tags).

Let:

- `T` be a transaction.
- `Pgno` be a page identifier.
- `commit_seq(T)` be a monotonically increasing commit sequence number assigned at commit marker append time (not wall clock).

Each transaction has:

- `begin_seq(T)` (logical begin time; derived from current commit stream tip)
- `snapshot(T)` = `(high, in_flight)` as of begin
- `read_set(T)` and `write_set(T)` at page granularity:
  - `read_set(T) ⊆ Pgno`
  - `write_set(T) ⊆ Pgno`

Visibility predicate (page versions):

```
visible(version_created_by, snapshot) :=
  version_created_by == 0
  OR (version_created_by <= snapshot.high
      AND version_created_by ∉ snapshot.in_flight
      AND version_created_by ∈ committed_set)
```

Read rule (self-visibility wins):

```
read(P, T) =
  if P ∈ write_set(T) then T.private_version(P)
  else newest visible committed version of P under snapshot(T)
```

Write rule:

- First write to `P` creates a private delta `Δ(P,T)` in `T`’s write set.
- `Δ` is a `PageDelta` (full page, sparse patch, or algebraic patch).

### 7.3 Why SI Is Not Acceptable (Write Skew)

SI permits write skew: `T1` and `T2` read overlapping logical constraints and write disjoint pages; both commit; constraint violated.

Therefore:

- SI MAY exist only as an explicit opt-in for experiments.
- Default MUST be serializable.

### 7.4 Serializable Strategy: “Page-SSI” (Conservative SSI at Page Granularity)

We implement **Serializable Snapshot Isolation** ideas, but we are explicit about the granularity:

- We track predicate reads using **SIREAD** state on pages (and later, page ranges/cells).
- We track **rw-antidependencies**:

`T1 ->rw T2` exists if:

1. `T1` reads page/key `X` (represented as `Pgno` or `(Pgno, tag)`),
2. `T2` later writes `X`,
3. and `T1` and `T2` overlap in time.

Dangerous structure (SSI):

```
T1 ->rw T2  and  T2 ->rw T3
```

and (in classic SSI) `T3` commits before `T1`, implying a serialization cycle.

**V1 guarantee (conservative, simple, correct):**

> No transaction that commits is allowed to have *both* an incoming and outgoing rw-antidependency.

Rationale:

- Any cycle in a directed dependency graph implies every node on the cycle has at least one incoming and one outgoing edge.
- Under SI + first-committer-wins, anomalies require rw edges; preventing “rw pivots” prevents dangerous structures and therefore prevents cycles.
- This is conservative (more aborts than necessary) but serializable.

Proof sketch (why this implies serializable behavior):

- Consider the serialization graph whose edges are the rw-antidependencies we track at our chosen granularity.
- Any directed cycle would require every transaction on the cycle to have at least one incoming and at least one outgoing edge.
- Therefore, if we prevent any committed transaction from having both `has_in_rw && has_out_rw`, the committed graph cannot contain a directed cycle.
- Acyclic serialization graph ⇒ a valid topological order exists ⇒ the execution is equivalent to some serial schedule.

This is the “alien artifact” stance: we start with a simple rule with a clear correctness argument, then we refine to reduce aborts once conformance is stable.

This is the “go for broke” choice: correctness first, then reduce aborts with refinements once conformance is stable.

### 7.5 Concrete SSI State (Low-Overhead, Deterministic)

We maintain:

- `TxnState`: `Active | Committed { commit_seq } | Aborted { reason }`
- `SireadTable`: predicate reads
  - baseline key: `Pgno`
  - later refinement key: `(Pgno, RangeTag)` or `(Pgno, CellTag)`
  - value: a compact set of active txn ids (SmallVec/bitset style)
- Per-txn flags:
  - `has_in_rw(T)`: ∃`R` such that `R ->rw T` (some other txn read a page/key that `T` later overwrote)
  - `has_out_rw(T)`: ∃`W` such that `T ->rw W` (`T` read a page/key that some other txn later overwrote)

We MUST also maintain “explainability witnesses” in debug/lab builds:

- the specific `(Pgno, reader_txn, writer_txn)` events that established `has_in_rw/has_out_rw`

### 7.6 Dependency Updates (When Reads and Writes Happen)

Operations:

```
on_read(T, P):
  read_set(T).insert(P)
  SireadTable.add(P, T)

on_write(U, P):
  write_set(U).insert(P)
  for each active reader T in SireadTable.readers(P):
    if overlaps(T, U):
      // T read something U wrote later: T has an outgoing rw; U has an incoming rw.
      has_out_rw(T) = true
      has_in_rw(U) = true
      record_witness(T, U, P)
```

Overlap predicate:

- Two transactions overlap if both were active at some instant.
- In implementation, we approximate overlap by:
  - `T` is `Active` when `U` writes, and
  - `T.begin_seq <= current_tip` etc (we define exact rules in code; tests enforce determinism).

### 7.7 Commit-Time Rule (Deterministic Abort)

On commit of `U`:

- If `has_in_rw(U) && has_out_rw(U)` then **abort U** (retryable serialization failure).
- Else, proceed to durability protocol (capsule + marker).

This is deterministic: the committing txn is the victim. (We later MAY add a more sophisticated victim selection, but V1 correctness is simplest with “abort self”.)

### 7.8 Deadlock Freedom (Structural Theorem)

Rule:

- No wait while holding page locks. Locks are try-acquire only.

Theorem:

- No deadlock is possible because there is no blocking wait; therefore there is no wait-for cycle.

This must be validated under `asupersync::LabRuntime` schedule exploration.

### 7.9 Relationship To Mergeable Writes

Write-write conflicts are not binary. Section §8 defines safe write merging via intent replay and structured page patches.

Policy:

- If `U` attempts to commit but detects that a page in `write_set(U)` has been updated since `snapshot(U)`, we MAY attempt a **rebase merge**:
  - rebase `U`’s patch onto the new base version
  - if mergeable and invariant-preserving → commit proceeds
  - else → abort/retry (`BUSY`-class)

This reduces conflict rates on hot B-tree pages without row-level MVCC metadata.

### 7.10 What Must Be Proven (Tests, Not Prose)

For concurrency correctness, we require:

- **Schedule exploration** tests for invariants under DPOR (no deadlocks, no panics, bounded memory, deterministic decisions).
- **Serializability regression suite**:
  - construct known write-skew patterns and ensure at least one txn aborts under default serializable mode.
- **Oracle parity**:
  - for all sequential workloads (the vast majority of conformance), results match C SQLite.

### 7.11 Conflict Probability Model (So We Know What To Optimize)

We explicitly model expected conflict rates so we can tell if improvements are real.

Uniform write model (rough, but useful):

- Table has `P` pages.
- Each txn writes `W` (distinct) pages, uniformly at random.

Probability that two txns conflict on at least one page:

```
Pr[conflict] = 1 - (1 - W/P)^W  ≈  W^2 / P   (when W << P)
```

Reality model:

- B-tree workloads are not uniform; they are hot-page heavy (root/internal pages) and often Zipf-like.
- This is exactly why §8 exists: deterministic rebase and mergeable intents reduce conflicts on hot pages.

We will validate the model with benchmarks that control:

- `P` (table size)
- `W` (writes/txn)
- hot-page skew (Zipf parameter)

### 7.12 Mechanical Sympathy: Data Structures That Make This Fast

The math is only valuable if it is embodied in the right low-level structures.

#### 7.12.1 Active Transaction Set Representation (Adaptive)

We need fast membership tests for:

- snapshot visibility (`created_by ∈ in_flight?`)
- SSI overlap checks / state pruning

We therefore define `ActiveTxnSet` as an adaptive structure:

- small case (common): `SmallVec<TxnId>` sorted
- large case: `roaring::RoaringTreemap` (u64) for fast set operations
- optional accelerator: Bloom filter for fast negative checks, with exact verification on “might contain”

Correctness rule:

- Bloom filters MAY be used only as a fast negative; positives MUST be verified against an exact set, or else we risk hiding visible commits (incorrect).

#### 7.12.2 Sharded Tables (No Global Contention)

Global maps on hot paths must be sharded:

- page write lock table
- SIREAD table (page → readers)
- per-page version presence filter metadata

Implementation strategy:

- `N` shards (power of two, e.g., 64 or 256)
- shard chosen by `pgno.hash() & (N-1)`
- each shard guarded by `parking_lot` lock

This preserves our “no waiting while holding locks” rule: the only blocking is narrow (a shard mutex), and we never hold multiple shard locks at once.

#### 7.12.3 Epoch-Based Reclamation (Safe, No Unsafe)

We want the *effect* of EBR (quick reclamation without global pauses) without unsafe pointer tricks.

We therefore define reclamation epochs in terms of the commit clock:

- `CommitSeq` acts as the global epoch.
- `min_active_begin_seq` (or `min_snapshot_high`) defines the GC horizon.
- any page history version whose `created_by_commit_seq < horizon` is reclaimable.

Implementation:

- store versions as `Arc`-owned objects
- remove reclaimable versions from maps/lists when horizon advances
- memory is freed by `Arc` drops (safe Rust)

This gives us “epoch reclamation semantics” while preserving `#[forbid(unsafe_code)]`.

### 7.13 Upgrade Path: Full SSI (Cahill/Fekete) To Reduce Abort Rate

The conservative “no committed pivot” rule (§7.4) is intentionally simple and correct, but it may abort more than necessary.

Once conformance is stable, we can upgrade to a closer-to-classic SSI implementation:

- Track rw-antidependencies with enough metadata to apply the classic commit-order condition (the “T3 commits before T1” aspect).
- Persist SIREAD state beyond txn commit when required (a committed txn’s SIREAD locks may still participate in a dangerous structure until all overlapping txns resolve).
- Abort policy becomes more selective (abort only when a genuine dangerous structure is formed).

This upgrade is driven by data:

- collect abort witnesses (which pages/tags, which txns) in lab builds
- quantify false-positive aborts vs throughput
- refine granularity (page → range/cell tags) only where needed

---

## 8. Safe Write Merging (Intent + Structured Patches)

Page-level MVCC can still conflict on hot pages. We want to reduce false conflicts **without** upgrading to row-level MVCC metadata (which would break file format and cost space).

We exploit two “merge planes”:

1. **Logical plane (preferred):** merge *intent-level* B-tree operations that commute (e.g., inserts into distinct keys).
2. **Physical plane (fallback):** merge *structured page patches* keyed by stable identifiers (e.g., `cell_key_digest`). Raw byte-disjoint XOR merge is forbidden for SQLite structured pages.

RaptorQ alignment:

- Physical patches are vectors over GF(256); XOR composition is natural.
- Logical intent logs are *small* and therefore encode/replicate extremely efficiently as ECS objects.

### 8.1 Patch Types (What A Transaction Actually Records)

Each writing txn records both:

1. **Intent log** (semantic operations; merge-friendly):
   - `Insert { table, key, record }`
   - `Delete { table, key }`
   - `Update { table, key, new_record }`
   - index maintenance ops as needed
2. **Materialized page deltas** (for fast intra-txn reads):
   - `FullPageImage`
   - `SparseRangeXor` (byte ranges + XOR payload)
   - `StructuredPagePatch` (header/cell/free operations)

Commit capsules MAY carry either:

- a fully materialized set of page deltas, OR
- the intent log plus enough metadata to deterministically replay it during recovery.

V1 default:

- Store intent log + the minimal stable materializations needed for fast reads and deterministic replay.

### 8.2 Logical Merge: Deterministic Rebase (The Big Win)

The dominant “same-page conflict” in SQLite workloads is: two writers insert/update rows that land on the same hot leaf page (or the same hot internal pages during splits).

Instead of treating that as fatal, we do:

1. Detect base drift:
   - `base_version(pgno)` for a txn’s write set changed since its snapshot.
2. Attempt **deterministic rebase**:
   - take the txn’s intent log
   - replay it against the *current* committed snapshot
   - produce new page deltas
3. If replay succeeds without violating constraints/invariants → commit proceeds.
4. If replay fails (true conflict, constraint violation, or non-determinism) → abort/retry.

This is “merge by re-execution”, not “merge by bytes”. It’s how we get *row-level concurrency effects* without storing row-level MVCC metadata.

Determinism requirement:

- The replay engine MUST be deterministic for a given `(intent_log, base_snapshot)` under lab runtime (no dependence on wall-clock, iteration order, hash randomization, etc.).

### 8.3 Physical Merge: Structured Page Patches

Physical merge is the fallback when base drift is detected and deterministic rebase (§8.2)
does not apply or does not succeed.

Encoding note: XOR/`GF(256)` deltas are a useful *representation* for page history compression,
but for SQLite structured pages, merge eligibility is never decided by raw byte-range
disjointness. Physical merge must be expressed as a `StructuredPagePatch` keyed by stable
identifiers (e.g., `cell_key_digest`) with explicit invariant checks.

### 8.4 StructuredPagePatch: Make Safety Explicit

Byte disjointness is not enough if we touch structural metadata (cell pointer array, free list, header).

We therefore define a structured patch:

```
StructuredPagePatch {
  // Serialized or “single-writer only” unless we implement a merge law
  header_ops: Vec<HeaderOp>,

  // Mergeable when disjoint by (cell_key) not merely by byte range
  cell_ops: Vec<CellOp>,

  // Default: conflict. Future: structured merge with proofs.
  free_ops: Vec<FreeSpaceOp>,

  // Forbidden for SQLite structured pages; debug-only for explicitly-opaque pages
  raw_xor_ranges: Vec<RangeXorPatch>,
}
```

Key point:

- `cell_ops` SHOULD be keyed by a stable identifier (`cell_key_digest` derived from rowid/index key), not by raw offsets. This enables safe merges even when the page layout shifts.

### 8.5 Commit-Time Merge Policy (Pragmatic, Aggressive, Safe)

When a txn `U` reaches commit:

1. Run serializability rule (§7) first. If `U` is a pivot → abort.
2. For each page in `write_set(U)`:
   - if base unchanged → OK
   - else attempt SAFE merge ladder (default):
     1. try deterministic rebase replay (preferred)
     2. else try structured patch merge (if supported for those ops)
     3. else abort/retry

Policy knob: `PRAGMA fsqlite.write_merge = OFF | SAFE | LAB_UNSAFE`. `SAFE` never allows raw
byte-range XOR merging for SQLite structured pages; `LAB_UNSAFE` is a debug facility for
explicitly-opaque pages only.

This yields a strict safety ladder: we only take merges we can justify.

### 8.6 What Must Be Proven (And How We Prove It)

We require runnable proofs:

- **B-tree invariants** hold after replay/merge:
  - ordering
  - cell count bounds
  - free space accounting
  - overflow chain validity
- **Patch algebra invariants**:
  - `apply(p, merge(a,b)) == apply(apply(p,a), b)` when mergeable
  - commutativity for declared commutative ops
- **Determinism**:
  - identical `(intent_log, base_snapshot)` yields identical replay outcome under `LabRuntime` across seeds

These become:

- proptest suites for patch algebra and B-tree invariants
- DPOR schedule exploration tests for merge/commit interleavings

### 8.7 MVCC History Compression: “PageHistory” Objects

Storing full page images per version is not acceptable long-term. Our history representation is:

- newest committed page version: full image (for fast reads)
- older versions: patches (intent logs and/or structured patches)
- for hot pages: encode patch chains as ECS **PageHistory objects** so:
  - history itself is repairable (bounded corruption tolerated)
  - remote replicas can fetch “just enough symbols” to reconstruct a needed historical version

This is not optional fluff: it is how MVCC avoids eating memory under real write concurrency.

---

## 9. Durability: The WAL Is Dead, Long Live The Coded Commit Stream

Durability is the place where “RaptorQ everywhere” becomes real: commits are not “written to a file”, they are **encoded as symbols** and then persisted/replicated under explicit loss/corruption budgets.

### 9.1 Crash Model (Explicit Contract)

We assume:

1. Process can crash at any point.
2. `fsync()` is a durability barrier for data and metadata as documented by the OS.
3. Writes can be reordered unless constrained by fsync barriers.
4. Torn writes exist at sector granularity (tests simulate multiple sector sizes).
5. Corruption/bitrot may exist.
6. File metadata durability may require directory `fsync()` (platform-dependent); our VFS MUST model this and tests MUST include it.

### 9.2 Erasure-Coded Durability Contract (“Self-Healing WAL”)

We add a stronger contract:

> If the commit protocol reports “durable”, then the system MUST be able to reconstruct the committed capsule bytes exactly during recovery, even if some fraction of locally stored symbols are missing or corrupted within the configured tolerance budget.

This is the operational meaning of “self-healing”: we do not merely *detect* corruption; we *repair* it by decoding.

### 9.3 Durability Policy (Local vs Quorum)

Durability is policy-driven:

- **Local durability**: enough symbols persisted to local symbol store(s) such that decode will succeed under the local corruption budget.
- **Quorum durability**: enough symbols persisted across `M` of `N` replicas to survive node loss budgets (see §12).

Policy is exposed via:

- `PRAGMA durability = local`
- `PRAGMA durability = quorum(M)`

and possibly:

- `PRAGMA raptorq_overhead = <percent>` (controls repair symbol budget).

### 9.4 Commit Objects: Capsule + Marker (Atomicity by Construction)

We define:

1. `CommitCapsule`: the coded payload (what changed + evidence references).
2. `CommitProof`: a small coded evidence object persisted by the sequencer.
3. `CommitMarkerRecord`: a fixed-size record appended to the marker stream (the commit clock).

**Atomicity rule:**

- A commit is committed iff its `CommitMarkerRecord` is durably appended to the marker stream.
- A marker record MUST reference exactly one capsule (by object id) and its proof.
- Recovery MUST ignore any capsule/proof without a committed marker record.

#### CommitCapsule (V1 payload)

Capsule contains:

- `snapshot_basis` (what the txn read)
- `intent_log` (preferred) and/or `page_deltas` (materialized patches)
- `read_set_digest` and `write_set_digest` (for debugging + conformance witnesses)
- SSI witnesses (debug/lab builds; can be feature-gated in release)
- schema delta (DDL) if applicable

During recovery/apply, the `commit_seq` from the marker is associated with the capsule and used as the “created_by” identifier for any committed versions it publishes.

#### CommitMarkerRecord (V1 marker stream record)

Marker contains:

- `commit_seq`
- `commit_time_unix_ns` (monotonic)
- `capsule_object_id`
- `proof_object_id`
- `prev_marker_id` pointer (hash-chain for linear history; like a linked list)
- `marker_id` (BLAKE3-128 over the record prefix; domain separated)
- `record_xxh3` (fast corruption check)

The marker stream is append-only and totally ordered; it is the “commit clock”.
Optional (recommended for distributed replication): maintain an MMR accumulator over the marker stream for O(log N) inclusion/prefix proofs.

### 9.5 Commit Protocol (Native Mode, High-Concurrency)

Goal:

- Writers prepare in parallel.
- Only the minimal “publish commit” step is serialized.

Protocol for txn `T`:

1. Build `CommitCapsuleBytes(T)` deterministically.
2. Encode capsule bytes into symbols using `asupersync::raptorq::RaptorQSender`.
3. Persist symbols to local symbol logs (and optionally stream to replicas) until the durability policy is satisfied:
   - local: persist ≥`K_total + margin` symbols (where `K_total = ObjectParams.total_source_symbols()`)
   - quorum: persist/ack ≥`K_total + margin` symbols across M replicas (asupersync quorum combinator)
4. Submit `(capsule_object_id, write_set_summary, txn_identity)` to the WriteCoordinator.
5. WriteCoordinator (serialized, tiny I/O):
   - validate (FCW + SSI re-check)
   - allocate `commit_seq` + monotonic `commit_time_unix_ns`
   - persist `CommitProof`
   - fsync barrier (referents durable)
   - append fixed-size `CommitMarkerRecord` to marker stream
   - fsync marker stream
   - respond success to the client
6. Index segments and caches update asynchronously.

**Critical ordering:** marker publication MUST happen after capsule durability is satisfied. If marker is durable but capsule is not decodable, we violated our core contract.

Proof sketch (marker ⇒ decodable capsule under budget):

- The commit path does not publish a marker until the durability policy reports enough persisted symbols for the capsule.
- For an object with `ObjectParams`, decoding requires (at minimum) ≥`K_block` symbols per source block, i.e. ≥`K_total` symbols total (and in practice slightly more to crush failure probability).
- Therefore, under the assumed loss/corruption budget and redundancy margin, recovery can collect ≥`K_total` valid symbols and decode the capsule.

### 9.6 Recovery Algorithm (Native Mode)

Startup:

1. Load `RootManifest` via `ecs/root` (§6.6).
2. Locate the latest checkpoint (if any) and its manifest.
3. Scan marker stream from the checkpoint tip forward (or from genesis).
4. For each marker:
   - fetch/decode referenced capsule (repairing via symbols)
   - apply capsule to state (materialize page deltas or replay intent log)
5. Rebuild/refresh index segments and caches as needed.

Correctness requirement:

- If recovery sees a committed marker, it MUST eventually be able to decode the capsule (within configured budgets), or else the system must surface a “durability contract violated” diagnostic with decode proofs attached (lab builds).

### 9.7 Checkpointing, Compaction, and Garbage Collection

Without compaction, an append-only commit stream grows forever.

We define checkpoints as ECS objects:

- `CheckpointManifest`: declares a consistent materialized state (either a full `.db` image or a set of page images) at a specific `commit_seq`.
- `CheckpointChunk` objects: the actual bytes (full DB or page groups), encoded as symbols.

Checkpoint procedure (simplified):

1. Choose a `commit_seq` boundary `C`.
2. Materialize the DB image at `C` (or a page set).
3. Encode and persist checkpoint chunks (symbols).
4. Publish a new `RootManifest` pointing to the checkpoint.
5. Now older commits < C become reclaimable by policy (subject to replication lag and reader snapshots).

This is how we bound:

- MVCC history length
- symbol log size
- index segment count

### 9.8 Compatibility Mode Mapping (SQLite Views)

Compatibility mode exists for:

- oracle conformance
- external tooling that expects `.db/.wal`

Two mapping strategies:

1. **View-only**: materialize `.db` from ECS state at open/close/checkpoint boundaries.
2. **Shadow WAL** (optional): maintain a conventional WAL stream plus an FEC sidecar:
   - `.wal` contains normal frames
   - `.wal-fec` contains repair symbols for those bytes

In both cases, ECS remains the source-of-truth in native mode; compatibility artifacts are derived views.

### 9.9 WAL-FEC Sidecar (Corrected, SQLite-Compatible, Still RaptorQ)

Earlier drafts proposed: treat each WAL commit group as `K` source symbols (page images) plus `R` repair symbols. That idea is correct.

The *frame-header embedding* idea, however, must be corrected:

- SQLite WAL frame headers do **not** have spare padding bytes; the 24-byte header contains page number, dbsize-for-commit, salts, and checksums (see `legacy_sqlite_code/sqlite/src/wal.c`).
- Therefore, we MUST NOT repurpose header bytes for RaptorQ metadata in a way that would break salt/checksum validation or cause C SQLite to truncate the WAL at the first “weird” frame.

So the compatibility design is:

> Keep `.wal` strictly SQLite-correct. Put all redundancy in `.wal-fec` (sidecar).

#### 9.9.1 PRAGMA: `raptorq_repair_symbols`

We expose:

```sql
PRAGMA raptorq_repair_symbols;          -- query (default: 2)
PRAGMA raptorq_repair_symbols = N;      -- set (0 disables)
```

Semantics:

- `N = 0`: exact SQLite-style WAL behavior (no FEC sidecar writes)
- `N = 1`: tolerate 1 torn/corrupt frame per commit group
- `N = 2`: tolerate 2 torn/corrupt frames per commit group (default)

Persistence:

- This PRAGMA SHOULD be stored in the SQLite DB header reserved bytes (offset 72–91) so it travels with the database file without breaking compatibility (C SQLite ignores those bytes).

#### 9.9.2 FEC Object Model (Compat Mode)

For each WAL commit group `G` that writes `K` pages:

- define a compat ECS object `CompatWalCommitGroup` whose **source symbols** are the commit group’s page images (chunked to `symbol_size <= u16::MAX`)
- generate `R = PRAGMA raptorq_repair_symbols` **repair symbols**
- write the `K` source frames to `.wal` normally
- write only the `R` repair symbols + minimal metadata to `.wal-fec`

Metadata we MUST store per group:

- a stable `group_id` (e.g., `(wal_salt1, wal_salt2, end_frame_no)` or a content-derived id)
- `ObjectParams` (object id, object_size, symbol_size, source_blocks, `K_block`)
- the ordered list of `Pgno`s for frames in the group, plus `(page_size, symbol_size)` so source symbol indices map deterministically to `(pgno, chunk_index)`
- the WAL frame range `(start_frame, end_frame)` so recovery can partition without relying on possibly-corrupted commit headers

`wal-fec` record format can reuse our ECS symbol record envelope (§6.4), with one additional “group metadata” record that carries the `Pgno` list.

#### 9.9.3 Recovery With WAL-FEC (Compat Mode)

On open/recovery in compatibility mode:

1. Scan `.wal` frames in fixed increments (frame size is known from page size).
2. Partition frames into commit groups using WAL frame range hints from `.wal-fec` when present.
3. For each group:
   - validate each source frame’s salt+checksum (SQLite rules)
   - if all valid: apply normally
   - if some invalid:
     - collect valid source page images
     - collect repair symbols from `.wal-fec`
     - if `valid_sources + repairs >= K`: decode to reconstruct missing source pages, then apply the reconstructed pages as if they were present in the WAL
     - else: group unrecoverable (equivalent to catastrophic loss)

This gives us “self-healing WAL” behavior without breaking SQLite’s WAL format.

---

## 10. The Radical Index: RaptorQ-Coded Index Segments

Classic SQLite uses a separate WAL-index structure to avoid scanning the WAL.

FrankenSQLite’s premise is stronger:

- our durability is object-based (capsules, markers, page patches)
- our transport/storage is symbol-based (ECS)
- therefore our index MUST also be object-based and self-healing

This section specifies an **index stack** that lives inside ECS and can be repaired/rebuilt.

### 10.1 What The Index Must Answer (Minimum Query API)

Given `(pgno, snapshot)` we need:

1. The newest committed version ≤ snapshot.
2. A pointer to the bytes (or the intent replay recipe) to materialize that page.

Given `object_id` we need:

- where to find symbols quickly (optional accelerator; not correctness-critical).

### 10.2 VersionPointer (The Atom of Lookup)

We define:

```
VersionPointer {
  commit_seq: u64,
  patch_object: ObjectId,     // ECS object containing the patch (or intent chunk)
  patch_kind: PatchKind,      // intent vs physical, etc.
  base_hint: Option<ObjectId> // optional “base image” hint for faster materialization
}
```

The pointer is stable and replicable:

- it references only object ids
- it does not embed physical offsets

### 10.3 IndexSegment Types (Yes, Multiple)

We use multiple segment kinds, all ECS objects:

1. **PageVersionIndexSegment**
   - maps `Pgno -> VersionPointer` for a commit range
   - includes filters for “segment contains pgno”
2. **ObjectLocatorSegment** (accelerator only)
   - maps `ObjectId -> Vec<SymbolLogOffset>` (or “segment id + offset”)
   - rebuildable by scanning symbol logs
3. **ManifestSegment**
   - maps `commit_seq ranges -> index segment tips`
   - speeds bootstrapping so we don’t need to scan everything

All segments are RaptorQ-encoded objects and therefore self-healing.

### 10.4 Lookup Algorithm (Read Path)

To read page `P` under snapshot `S`:

1. Check pager cache (ARC) for a visible committed version.
2. If cache miss:
   - consult Version Presence Filter (§11.2). If “no versions” → read base page view.
3. Else:
   - find candidate `VersionPointer`s by scanning newest index segments backward until we find `commit_seq <= S.high` and visibility holds
   - fetch/decode referenced patch objects (repairing if needed)
   - materialize page by:
     - applying physical patch to base, or
     - replaying intent log chunk deterministically

This algorithm is designed to degrade gracefully:

- if index segments are missing/corrupt, we can fall back to scanning marker stream and rebuilding segments.

### 10.5 Segment Construction (Background, Deterministic)

Segment builder:

- consumes the commit marker stream in order
- for each committed capsule:
  - extracts page patch object references
  - updates a builder map `Pgno -> VersionPointer`
- periodically flushes a new segment object covering `[start_commit, end_commit]`

We MUST make construction deterministic:

- stable ordering of map iteration
- stable encoding of segments
- stable object id derivation

### 10.6 Repair and Rebuild

IndexSegments are self-healing:

- if some symbols are missing/corrupt, decode from remaining symbols
- if decode impossible (beyond budget), rebuild by re-consuming marker stream + capsules

We treat “index unrebuildable but commit markers exist” as a serious integrity failure; lab tests should force this case to ensure diagnostics are good.

### 10.7 Boldness Constraint

We do this in V1:

- coded index segments are not a “Phase 9 nice-to-have”
- the index is part of the fundamental ECS thesis

Fallbacks exist only as emergency escape hatches after conformance/perf data proves a need.

---

## 11. Caching & Acceleration (ARC, Bloom/Quotient Filters, Hot Paths)

If ECS is the “durability brain”, caching is the “p95 nervous system”. The ECS design only wins if the common read/write paths are:

- O(1) or O(log n) with tiny constants
- bounded-memory under concurrency
- predictable under scans (no catastrophic cache thrash)

### 11.1 Pager Cache Policy: ARC, Not LRU

SQLite-era LRU is a known footgun for databases: one large table scan can evict the entire working set.

**V1 cache policy:** ARC (Adaptive Replacement Cache).

Why ARC:

- It adapts between recency and frequency automatically.
- It is scan-resistant compared to naive LRU.
- It is a strong “online algorithm” choice: the policy is driven by observed workload, not hard-coded heuristics.

Implementation constraints:

- MUST be safe Rust (`#[forbid(unsafe_code)]`).
- MUST be O(1) per access/update.
- MUST support “pinned pages” (pages referenced by active cursors/transactions are non-evictable).

### 11.2 MVCC Fast Path: “Most Pages Have No Versions”

MVCC overhead must be near-zero when there is no concurrent modification of a page.

We therefore maintain a **Version Presence Filter**:

- A Bloom filter (or quotient filter) keyed by `Pgno` that answers:
  - “this page definitely has no MVCC versions” (fast skip)
  - “this page might have versions” (fall back to version-chain lookup)

This makes the hot read path:

1. Check cache for current committed page
2. If miss: check filter; if “no versions” → read base page (db or compatibility view)
3. Else → consult version store / index segments

### 11.3 Index Acceleration: Filters + Two-Level Lookup

IndexSegments SHOULD embed:

- a fast filter for “does this segment mention pgno?”
- optionally, a “page hotness” summary to prioritize caching of hot ranges

Readers maintain a two-level lookup:

1. `Pgno -> candidate VersionPointer` using the newest few IndexSegments
2. `VersionPointer -> object_id` lookup via object locator cache (offsets of symbol records)

Both layers are rebuildable; correctness never depends on caches.

### 11.4 Bounded History = Bounded Overhead

MVCC overhead is proportional to version chain length.

We treat this as a bound we can reason about:

- Let `K` be average versions per hot page visible to readers.
- Let `N` be page reads per query.
- Naive MVCC overhead is `O(N*K)` for visibility scan.

Therefore:

- GC/checkpoint MUST keep `K` small.
- For read-mostly workloads, the version filter SHOULD ensure `K ~ 0` for most pages.

This “performance model” is not optional; it drives GC triggers and benchmark budgets.

### 11.5 Memory Accounting (No Surprise OOM)

Every subsystem that stores history MUST have:

- a strict byte budget
- a policy for reclamation under pressure
- metrics exported for harness + benchmarks

We do not accept unbounded growth of:

- page version chains
- SIREAD state
- symbol caches
- index segment caches

### 11.6 ARC Mechanics (Enough Detail To Implement Correctly)

ARC maintains four LRU lists:

- `T1`: recent pages in cache (recency)
- `T2`: frequent pages in cache (frequency)
- `B1`: “ghost” entries recently evicted from `T1`
- `B2`: “ghost” entries recently evicted from `T2`

The key control variable is `p` (target size of `T1`). ARC adapts `p` based on whether misses hit `B1` or `B2`:

- miss hits `B1` ⇒ increase `p` (more recency)
- miss hits `B2` ⇒ decrease `p` (more frequency)

Implementation requirements:

- O(1) amortized per request
- ghost lists store keys only (no page data)
- eviction never evicts pinned pages (or else cursors/txns break)

This is enough structure to avoid the common “LRU scan death spiral”.

### 11.7 MVCC-Aware ARC Keys (Versions Are Not Free)

Standard ARC keys on `Pgno`. MVCC complicates this because there may be multiple versions of a page visible to different snapshots.

We therefore define an ARC key:

```
CacheKey = (pgno, version_tag)
```

Where `version_tag` is:

- `CommittedTip` for the newest committed version
- or a `CommitSeq`/`TxnId`-derived tag for a historical version needed by an active snapshot

Policy:

- the cache MUST prefer keeping the newest committed version of hot pages
- historical versions are kept only while required by active snapshots; once the GC horizon advances (§7.12.3), they become eligible for eviction/reclamation
- version presence filter (§11.2) keeps the common case fast: most pages never take the “multi-version” path

### 11.8 Optional: Cache Integrity Verification (Debuggable Corruption)

We MAY support a debug/diagnostic mode:

- store `xxh3_128(page_bytes)` alongside cached pages
- on cache read, optionally re-hash and compare

If mismatch:

- evict the page from cache
- surface a diagnostic (and in lab builds, attach evidence to the trace)

This is useful for catching “impossible” corruption early during development and stress testing, without making it a mandatory hot-path tax.

---

## 12. Replication: Fountain-Coded, Loss-Tolerant, Late-Join Friendly

Replication is not “ship WAL frames over TCP”. Replication is:

> Make ECS symbols flow through the network so that every replica can decode the same objects, without requiring reliable ordered delivery.

We explicitly embrace:

- loss
- reordering
- duplication
- multipath delivery

because fountain codes make that cheap.

### 12.1 Replication Roles and Modes

We define two modes:

1. **Leader commit clock (V1 default):**
   - one node publishes the authoritative marker stream
   - other nodes replicate objects + markers and serve reads
   - writers can still be concurrent within the leader (MVCC)
2. **Multi-writer (experimental):**
   - multiple nodes publish capsules
   - marker stream ordering becomes a distributed problem (not V1 default)

V1 focuses on (1) to keep semantics sharp and testable while still delivering the core “cure concurrent writers” win.

### 12.2 What We Replicate (Object Classes)

We replicate ECS objects, not files:

- commit capsules (and patch objects they reference)
- commit markers (the commit clock)
- index segments
- checkpoints and snapshot manifests
- (optionally) decode proofs / audit traces for debugging

### 12.3 Transport Substrate (asupersync)

We build replication on:

- `asupersync::transport::{SymbolSink, SymbolStream, SymbolRouter, MultipathAggregator, SymbolDeduplicator, SymbolReorderer}`
- simulated networks for tests: `asupersync::transport::mock::SimNetwork`

The transport layer deals in `AuthenticatedSymbol` so we can turn security on without redesigning the pipeline.

### 12.4 Symbol Routing: Consistent Hashing + Policies

We do not assign “objects to nodes”. We assign **symbols** to nodes.

Default:

- encode object into `K_total` source symbols + `R` repair symbols
- assign each symbol to one or more nodes via `asupersync::distributed::consistent_hash`
- replication factor and `R` determine:
  - node-loss tolerance
  - loss tolerance
  - catch-up rate

### 12.5 Anti-Entropy (Late Join Is Just Another Case)

Replication must converge even if nodes are offline.

Anti-entropy loop:

1. Exchange tips:
   - latest `RootManifest` object id
   - latest marker stream position / marker id
   - optional index segment tips
2. Compute missing object ids (set difference via manifests/index summaries).
3. Request symbols for missing objects.
4. Stream symbols until decode succeeds; stop early once the receiver reports completion (typically around `K_total + ε` symbols).
5. Persist decoded objects locally; update caches.

Because objects are fountain-coded:

- a requester can ask for “any symbols for object X” without tracking which ESIs it already has
- the responder can send whatever is convenient (source first, then repairs)

### 12.6 Quorum Durability (Commit-Time Policy)

Commit can be declared durable only after a quorum of symbol stores have accepted enough symbols.

We reuse asupersync quorum semantics (`asupersync::combinator::quorum`):

- local-only: `quorum(1, [local_store])`
- 2-of-3: `quorum(2, [storeA, storeB, storeC])`

This is integrated into §9.5 step (3).

### 12.7 Snapshot Shipping (Bootstrap by Fountain Code)

To bring up a new replica:

1. Send the latest checkpoint manifest id.
2. Stream checkpoint chunks as symbols until the replica decodes the checkpoint.
3. Stream marker stream deltas since the checkpoint.
4. Replica is now caught up.

Because chunks are fountain-coded:

- loss does not require retransmission bookkeeping
- multiple existing replicas can multicast symbols; the joiner just needs any K

### 12.8 Consistency Checking (Sheaf + TLA+ Export)

We treat distributed correctness as first-class:

- **Sheaf check:** use `asupersync::trace::distributed::sheaf` to detect anomalies that pairwise comparisons miss (phantom “global” commits that no single node witnessed end-to-end).
- **TLA+ export:** use `asupersync::trace::tla_export` to export traces into TLA+ behaviors for model checking of bounded scenarios (commit, replication, recovery).

This is how “alien artifact” systems stay explainable under distributed complexity.

### 12.9 Security (Authenticated Symbols)

Replication MAY be secured by enabling an `asupersync::security::SecurityContext`:

- symbols become `AuthenticatedSymbol`
- receivers verify tags before accepting symbols
- unauthenticated/corrupted symbols are ignored (repair handles loss)

Security is an orthogonal dimension; it does not change ECS semantics.

---

## 13. The Asupersync Integration Contract (Cx Everywhere)

Asupersync is not “a convenience dependency”. It is the runtime semantics of the project:

- no Tokio
- no ad-hoc async
- deterministic concurrency testing as a first-class capability

### 13.1 `Cx` Capability Context (Ambient Semantics, Explicitly Passed)

All non-trivial operations MUST take a `&asupersync::Cx`:

- VFS I/O
- ECS symbol persistence
- replication networking
- timeouts / deadlines
- cancellation
- determinism hooks (lab runtime)
- observability / tracing attachments

`Cx` is cheaply cloneable and carries structured observability + drivers (I/O, timers, entropy, logical clock).

Design rule:

- Core engine APIs SHOULD accept `&Cx` even if they are synchronous today. This keeps the architecture honest and prevents “oops we need async later” rewrites.

### 13.2 RaptorQ Pipelines (The Only RaptorQ We Use)

We use asupersync’s RaptorQ integration:

- `asupersync::raptorq::{RaptorQSenderBuilder, RaptorQReceiverBuilder, RaptorQSender, RaptorQReceiver}`
- `asupersync::transport::{SymbolSink, SymbolStream}`
- `asupersync::security::{SecurityContext, AuthenticatedSymbol}` (optional)
- `asupersync::raptorq::proof::{DecodeProof, DecodeProofBuilder}` (audit)

Canonical encode path (conceptual):

```rust
use asupersync::{Cx, config::RaptorQConfig, raptorq::RaptorQSenderBuilder};

fn encode_object(cx: &Cx, object_id: asupersync::types::symbol::ObjectId, bytes: &[u8]) {
    let mut sender = RaptorQSenderBuilder::new()
        .config(RaptorQConfig::default())
        .transport(/* SymbolSink */)
        .build()
        .unwrap();
    let _outcome = sender.send_object(cx, object_id, bytes).unwrap();
}
```

### 13.3 Transport & Network Simulation (Testing Is The Spec)

Replication and ECS symbol plumbing MUST be testable without the real network:

- `asupersync::transport::mock::SimNetwork` for loss/reorder/duplication
- routers/dispatchers for symbol routing
- multipath aggregator for combining symbol streams

We want to be able to write tests like:

- “drop 30% of symbols, reorder arbitrarily, still decode”
- “corrupt every 1000th symbol, decode proof shows repair”

### 13.4 LabRuntime: Deterministic Scheduling + Virtual Time + Chaos

All concurrency-critical tests MUST run under `asupersync::LabRuntime`:

- deterministic scheduling by seed
- virtual time for timeouts and timers
- chaos injection (deterministic) to stress I/O ordering
- trace capture and replay for debugging

We consider it a bug if we cannot reproduce a concurrency failure with a single seed.

### 13.5 DPOR Exploration (Mazurkiewicz Trace Semantics)

We do not accept “run it 10,000 times and hope”.

For MVCC/commit/replication protocols:

- use DPOR-style schedule exploration (`asupersync::lab::explorer`)
- track coverage by Mazurkiewicz trace equivalence (trace monoid fingerprints)

This makes concurrency bugs discoverable early, not after production incidents.

### 13.6 Oracles: Anytime-Valid Monitoring (e-processes) + Conformal Budgets

We use:

- `asupersync::lab::oracle::eprocess` for anytime-valid invariant monitoring:
  - MVCC invariants
  - memory growth bounds
  - replication divergence signals
  - recommended: mixture e-processes (a small grid of betting strategies) to avoid brittle hand-tuned λ
- `asupersync::lab::conformal` for distribution-free calibration of performance thresholds in lab harnesses (avoid “benchmark noise” excuses).

### 13.7 Formalization Hooks: TLA+ Export + Distributed Sheaf Checks

Asupersync includes:

- `asupersync::trace::tla_export` for exporting traces into TLA+ behaviors/spec skeletons
- `asupersync::trace::distributed::sheaf` for higher-order consistency detection

We will use these specifically for:

- commit marker publish protocol
- recovery replay
- replication anti-entropy

### 13.8 `Cx` In Every Trait (No Ambient Authority)

Concrete rule:

- Any trait method that can touch time, I/O, networking, cancellation, concurrency, or randomness MUST accept `&Cx` (typically immediately after `&self`).

Examples (conceptual signatures):

```rust
use asupersync::Cx;

pub trait Vfs {
    fn open(&self, cx: &Cx, path: &str, flags: OpenFlags) -> Result<Box<dyn VfsFile>>;
}

pub trait VfsFile {
    fn read_at(&self, cx: &Cx, offset: u64, buf: &mut [u8]) -> Result<usize>;
    fn write_at(&self, cx: &Cx, offset: u64, buf: &[u8]) -> Result<()>;
    fn sync(&self, cx: &Cx, flags: SyncFlags) -> Result<()>;
}

pub trait Ecs {
    fn put_symbol_records(&self, cx: &Cx, records: &[EcsSymbolRecordV1]) -> Result<()>;
    fn get_any_k(&self, cx: &Cx, params: ObjectParams, k: usize) -> Result<Vec<AuthenticatedSymbol>>;
}
```

---

## 14. Conformance Harness (The Oracle Is The Judge)

Conformance is not Phase 9. It starts immediately, and it is how we keep the project honest while being radically innovative internally.

Principle:

> We are allowed to change *how* it works. We are not allowed to change *what it does* (unless explicitly approved).

### 14.1 The Oracle

Oracle = C SQLite 3.52.0 built from `legacy_sqlite_code/`.

The harness MUST be able to:

- run the Oracle in-process or via a small runner binary
- execute SQL statements and prepared statements
- capture results and error codes deterministically

### 14.2 What We Compare (Not Just Rows)

For each test case, we compare:

- result rows (including NULL behavior)
- type affinity where observable
- error code + extended error code (normalized)
- affected-row counts (`changes()`, `total_changes()`)
- `last_insert_rowid()` where relevant
- transaction boundary effects (commit/rollback, savepoints)

### 14.3 Fixture Format (Self-Describing)

We standardize on a JSON fixture format so it can be generated by the Oracle runner and consumed by Rust tests:

```json
{
  "name": "insert-and-select",
  "steps": [
    { "op": "open", "flags": "readwrite_create", "pragmas": ["journal_mode=WAL"] },
    { "op": "exec", "sql": "CREATE TABLE t(x INTEGER);" },
    { "op": "exec", "sql": "INSERT INTO t VALUES (1),(2),(3);" },
    { "op": "query", "sql": "SELECT x FROM t ORDER BY x;", "expect": { "rows": [["1"],["2"],["3"]] } }
  ]
}
```

Notes:

- Harness MUST support multi-step cases (transactions, temp objects, pragmas).
- Results are string-normalized by default; type-aware comparison is opt-in per case when needed.

### 14.4 Corpora (Breadth + Depth)

We run:

- SQLLogicTest (SLT) ingestion (broad SQL coverage).
- targeted micro-suites for tricky semantics:
  - floating-point corner cases
  - collations
  - NULL and type affinity oddities
  - triggers/CTEs/window functions
- regression tests for concurrency anomalies:
  - write skew patterns (must abort under default serializable mode)

And also non-oracle suites:

- crash/recovery fault-injection tests (native mode durability contract)
- replication convergence tests (SimNetwork, loss/reorder/dup)

### 14.5 Normalization Rules (Avoid False Failures)

The harness MUST encode the Oracle’s semantics faithfully while avoiding meaningless diffs:

- unordered SELECT results: compare as multisets only when SQL has no ORDER BY and Oracle makes no ordering guarantee (SLT already encodes sorting rules; we follow that).
- floating-point: compare either exact strings (default) or tolerance mode where explicitly requested.
- error messages: compare error codes; messages can be normalized (Oracle’s exact phrasing is not always stable).

### 14.6 Golden Output Discipline (Behavior Isomorphism)

Every optimization or refactor must preserve golden outputs unless:

- we explicitly document an intentional divergence, and
- add a harness annotation explaining why it is acceptable.

This is the “extreme optimization” guardrail: change one lever, prove behavior unchanged.

---

## 15. Performance Discipline (Extreme Optimization)

We operate under the strict loop:

1. Baseline
2. Profile
3. Prove behavior unchanged (oracle)
4. Implement one lever
5. Re-measure

Non-negotiable rule:

- We do not optimize “from vibes”. We optimize from profiles and budgets.

Practical commands (examples):

```bash
# Micro baseline (example)
hyperfine --warmup 3 --runs 10 'cargo run -p fsqlite-harness -- bench-case page_read_hot'

# CPU profile (example)
cargo flamegraph -p fsqlite-core -- benchmark_name
```

### 15.1 Benchmarks We Must Have Early

Micro:

- page read path: resolve visible version (with varying chain lengths)
- delta apply / merge cost
- SSI tracking overhead (SIREAD locks + dangerous structure detection)
- RaptorQ encode/decode throughput (object sizes typical for capsules/index segments)
- coded index lookup

Macro:

- multi-writer throughput scaling vs conflict rate
- scan-heavy vs random workloads (cache policy sensitivity)
- replication convergence time under loss

### 15.2 Checksums / Hashes (Performance Reality)

We separate three concerns:

1. **Hot-path corruption detection** (fast, non-crypto).
2. **Content identity** (stable, collision-resistant enough for addressing).
3. **Authenticity / security boundaries** (cryptographic, keyed where needed).

Policy:

- Hot-path page/symbol integrity: **XXH3-128** (via `xxhash-rust`) is the default.
  - Rationale: extremely high throughput; excellent for detecting torn writes/bitrot quickly.
  - Note: the crate may use unsafe internally for SIMD; our workspace code remains `#[forbid(unsafe_code)]`.
- Content addressing (`ObjectId` derivation): **BLAKE3** truncated to 128 bits (stable, fast, strong).
- Auth boundaries (optional): use `asupersync::security::SecurityContext` and authenticated symbols (don’t reinvent crypto).

We do NOT use SHA-256 on hot paths unless we have a specific security requirement; it is too slow for page-per-access integrity.

### 15.3 Mechanical Sympathy (Speed Without Cheating)

Principle: “math is instructions.” The project should feel like it was built by someone who can see the CPU.

Non-negotiables:

- **Avoid allocation in the read path**: cache lookups, version checks, and index resolution must be allocation-free in the common case.
- **Keep working sets in L1/L2**: small, contiguous structures for hot metadata (active txn sets, per-page flags, shard locks).
- **Exploit auto-vectorization**: GF(256) symbol ops and XOR patches should operate on `u64`/`u128` chunks where possible (safe Rust), letting LLVM vectorize.
- **Use optimized deps instead of writing unsafe**: SIMD happens inside vetted crates (xxhash/blake3/asupersync), not in our code.

### 15.4 Zero-Copy (Where It Helps, Without Breaking Canonical Bytes)

We distinguish:

- **Canonical bytes** (affecting `ObjectId`): MUST be explicit, stable, versioned, and not dependent on compiler/layout. We keep manual encoding here (§5.2.1).
- **Rebuildable caches** (accelerators): MAY use zero-copy formats for speed.

V1 stance:

- Use explicit byte layouts for ECS symbol records and log segments.
- Consider `rkyv`/zero-copy for caches only (object locator cache, index cache), because caches can be blown away and rebuilt without affecting correctness.

### 15.5 Isomorphism Proof Template (Required For Optimizations)

For every performance change:

```
Change: <description>
- Ordering preserved:     yes/no (+why)
- Tie-breaking unchanged: yes/no (+why)
- Float behavior:         identical / N/A
- RNG seeds:              unchanged / N/A
- Oracle fixtures:        PASS (reference case ids)
```

This is how we stay fast without drifting from parity.

### 15.6 Build Profiles (Perf First, Size As A Separate Track)

This repo currently sets `[profile.release]` to size-optimized settings (`opt-level="z"`, `lto=true`, `codegen-units=1`, `panic="abort"`, `strip=true`) in `Cargo.toml`.

Policy:

- Treat the existing `profile.release` as **V1 default** (it’s what CI and most agents will use unless explicitly changed).
- If/when we need a dedicated performance profile, add a separate Cargo profile (e.g. `profile.perf`) and benchmark under that profile explicitly.

Rationale:

- The spec and the repo configuration must agree (agents should not chase phantom profiles).
- If we introduce a perf profile later, it must come with benchmark evidence and a clear “which profile is canonical for perf claims” rule.

---

## 16. Implementation Plan (V1 Phases)

This plan is ordered to prevent the “build then refactor” trap, and each phase has an explicit exit criterion.

Guiding rule (porting-to-rust):

- We implement from *spec and oracle behavior*, not by translating C function-by-function.

### Phase 0 Gate: Oracle Harness Is Alive

Deliverables:

- Oracle runner builds C SQLite 3.52.0 from `legacy_sqlite_code/`.
- A JSON fixture format exists (§14.3).
- At least one smoke suite runs Oracle vs Rust and produces a clear diff on failure.

Exit criteria:

- `cargo test -p fsqlite-harness` passes locally.

### Phase 1 Gate: ECS Skeleton + `Cx` Plumbing

Deliverables:

- ECS symbol record format (§6.4) implemented in `crates/fsqlite-wal` initially (we can split into a dedicated ECS crate later only if layering pain is proven).
- Local symbol log append + scan works.
- `RootManifest` exists; `ecs/root` bootstraps.
- Core traits accept `&Cx` (VFS, pager, ECS).

Exit criteria:

- A “toy commit” object can be encoded, persisted, decoded, and verified end-to-end.

### Phase 2 Gate: Serializable MVCC Core

Deliverables:

- MVCC snapshot capture + visibility rules (§7.2).
- Page-SSI conservative rule implemented (§7.4–§7.7).
- Commit capsule + marker protocol implemented (§9.4–§9.5).
- Deterministic schedule tests under `LabRuntime` for:
  - no deadlocks
  - deterministic abort decisions

Exit criteria:

- Concurrency regression suite catches write skew (at least one txn aborts).
- Sequential conformance smoke cases still match Oracle.

### Phase 3 Gate: Coded Index Segments

Deliverables:

- `PageVersionIndexSegment` object type implemented (§10.3).
- Lookup path can resolve `(pgno, snapshot)` using segments.
- Rebuild-from-marker-stream works (delete caches, still boot).

Exit criteria:

- A benchmark demonstrates “no index scan” page lookup is fast enough for p95 goals.

### Phase 4 Gate: SQL Surface Marches Toward Parity (Driven by Oracle)

Deliverables:

- Parser/AST/planner/VDBE coverage expands guided by failing Oracle fixtures.
- B-tree correctness enforced by proptest invariants.
- Continuous fuzzing against the oracle harness:
  - fuzz SQL text → execute Oracle vs Rust → compare outputs
  - fuzz VDBE opcode sequences in isolation where possible (crash = failure, divergence = failure)
- Extensions are feature-gated but spec’d for parity targets.

Exit criteria:

- SLT subset passes (increasing over time).
- No “known failing” bucket unless explicitly documented.

### Phase 5 Gate: Replication + Snapshot Shipping

Deliverables:

- Symbol streaming over asupersync transport (SimNetwork tests).
- Anti-entropy loop converges under loss/reorder/dup.
- Snapshot bootstrap works from checkpoint chunks.

Exit criteria:

- Deterministic replication tests under LabRuntime show convergence.

### Phase 6 Gate: Mergeable Writes (Conflict Rate Collapse)

Deliverables:

- Deterministic rebase replay engine (§8.2) for a meaningful subset of ops.
- Physical patch merge ladder (§8.5) for safe cases.

Exit criteria:

- Multi-writer benchmark shows reduced abort rate on hot workloads compared to “abort on any same-page write”.

---

## 17. Risk Register + Open Questions

This section is explicit so future us doesn’t rediscover the same cliffs.

### 17.1 Risks (With Mitigations)

R1. **Serializable abort rate too high (Page-SSI is conservative).**  
Mitigations:

- refine SIREAD keys from page → (page, range/cell tag)
- add safe snapshot optimizations for read-only txns
- add intent-level rebase (§8.2) to turn conflicts into merges

R2. **RaptorQ overhead dominates CPU.**  
Mitigations:

- choose symbol sizing policy based on object type (capsules vs checkpoints)
- cache decoded objects aggressively
- profile and tune encoder/decoder hot paths (one lever per change)

R3. **Append-only storage grows without bound.**  
Mitigations:

- checkpoint and compaction are first-class (§9.7)
- enforce budgets for history, SIREAD, symbol caches (§11.5)

R4. **Bootstrapping chicken-and-egg (need index to find symbols, need symbols to decode index).**  
Mitigations:

- symbol records are self-describing (§6.4)
- one tiny mutable `ecs/root` pointer (§6.5–§6.6)
- rebuild-from-scan is always possible

R5. **Multi-process concurrency semantics unclear.**  
Mitigations:

- V1 focuses on in-process correctness + leader replication mode
- design APIs so shared-memory/lock-table evolution is possible
- add explicit tests for multi-process behaviors before promising support

R6. **File format compatibility vs “do it right”.**  
Mitigations:

- treat SQLite `.db/.wal` as compatibility views (§9.8)
- conformance harness validates observable behavior, not byte-identical layout

R7. **Mergeable writes become a correctness minefield.**  
Mitigations:

- strict merge ladder (§8.5)
- proptest invariants + DPOR tests (§8.6)
- start with deterministic rebase replay for a small op subset

R8. **Distributed mode correctness is hard.**  
Mitigations:

- keep V1 replication “leader commit clock” default (§12.1)
- use sheaf checks + TLA+ export for bounded model checking (§12.8, §13.7)

### 17.2 Open Questions (Tracked, With How We Answer Them)

Q1. Multi-process writers: do we support true multi-process concurrent writes in V1?  
Answer plan: prototype file-lock + marker-stream publish across processes; measure contention; decide based on benchmarks.

Q2. How far do we go with range/cell refinement for SIREAD?  
Answer plan: start page-only; collect abort witnesses; refine only when abort rate is proven unacceptable.

Q3. Symbol sizing policy per object type (capsule vs checkpoint vs index).  
Answer plan: benchmark encode/decode throughput vs object sizes; pick defaults; expose PRAGMA overrides for experiments.

Q4. Where to checkpoint for compatibility `.db` without bottlenecking writes?  
Answer plan: background checkpoint with ECS chunks; measure; keep export optional.

Q5. Which B-tree operations can be replayed deterministically for rebase merge?  
Answer plan: implement inserts/updates on leaf pages first; grow coverage guided by conflict benchmarks.

Q6. Do we need B-link style concurrency techniques for hot-page split/merge contention?  
Answer plan: benchmark workloads that hammer the same index/table; if internal-page conflicts dominate, add an internal “structure modification” protocol (ephemeral metadata, not file format changes) inspired by B-link trees: optimistic descent + right-sibling guidance + deterministic retry.

---

## 18. Local References (In This Repo)

- **RaptorQ canon:** `docs/rfc6330.txt`
- **Legacy oracle (C SQLite 3.52.0):** `legacy_sqlite_code/`
- Canon (source of truth):
  - `AGENTS.md`
  - `COMPREHENSIVE_SPEC_FOR_FRANKENSQLITE_V1.md`
- Other extracted structure notes (useful context; superseded where they conflict with canon):
  - `EXISTING_SQLITE_STRUCTURE.md`
  - `PLAN_TO_PORT_SQLITE_TO_RUST.md`
  - `PROPOSED_ARCHITECTURE.md`
- Older / partial specs (generally superseded by canon):
  - `MVCC_SPECIFICATION.md`
- Issue tracker state:
  - `.beads/`
