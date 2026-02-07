# COMPREHENSIVE SPECIFICATION FOR FRANKENSQLITE

> A clean-room Rust reimplementation of SQLite 3.52.0 with MVCC concurrent
> writers and RaptorQ-pervasive information-theoretic architecture.

---

## 0. How to Read This Document

This is the single authoritative specification for FrankenSQLite. It is
self-contained: every design decision, formal model, and implementation detail
needed to build the system is here. It supersedes and consolidates:

- `PROPOSED_ARCHITECTURE.md` (Rust design)
- `MVCC_SPECIFICATION.md` (concurrency model)
- `PLAN_TO_PORT_SQLITE_TO_RUST.md` (implementation phases)
- `EXISTING_SQLITE_STRUCTURE.md` (C SQLite behavior extraction)

Those documents remain in the repository for reference but this document is
the source of truth. Where they conflict, this document wins.

**Audience:** AI coding agents, human reviewers, and any collaborator who needs
the full picture of what FrankenSQLite is, why it exists, and exactly how to
build it.

### 0.1 Non-Negotiable Scope Doctrine

This specification describes the **complete target system**. There is no
"V1 scope" and no "we'll do it later" escape hatch. Every feature, protocol,
and subsystem described in this document is in scope for implementation. If
something is genuinely excluded, it appears in Section 15 (Exclusions) with a
technical rationale. Everything else MUST be built.

Implementation is phased (Section 16) for practical sequencing, not for
scope reduction. A feature being in Phase 9 does not make it optional -- it
means it depends on Phase 8 being complete.

### 0.2 Normative Language

This specification uses RFC 2119 / RFC 8174 keywords:

- **MUST** / **MUST NOT**: Absolute requirement or prohibition. Violation is a
  spec-conformance bug.
- **SHOULD** / **SHOULD NOT**: Strong recommendation. Deviation requires
  documented justification in code comments.
- **MAY**: Truly optional. Implementation can omit without justification.

Pseudocode and type definitions are normative unless explicitly labeled
"illustrative" or "example."

### 0.3 Glossary

| Term | Definition |
|------|-----------|
| **MVCC** | Multi-Version Concurrency Control. Transactions see a consistent snapshot while writers create new versions. |
| **SSI** | Serializable Snapshot Isolation. Extends SI to detect write skew via rw-antidependency tracking. |
| **ECS** | Erasure-Coded Stream. The universal persistence substrate: objects encoded as RaptorQ symbols. |
| **ObjectId** | Content-addressed identifier: `BLAKE3(canonical_encoding(object))`, 32 bytes. |
| **CommitCapsule** | Atomic unit of commit state in Native mode: intent log, page deltas, SSI witnesses. |
| **CommitMarker** | The durable "this commit exists" record: capsule ObjectId + prev-marker chain. |
| **RaptorQ** | RFC 6330 fountain code: K source symbols → unlimited encoding symbols, recoverable from any K' ≈ K. |
| **OTI** | Object Transmission Information. RaptorQ metadata needed for decoding: (F, Al, T, Z, N). |
| **Cx** | Capability context (asupersync). Threads cancellation, deadlines, and capability narrowing through every operation. |
| **PageNumber** | 1-based `NonZeroU32` identifying a database page. Page 1 is always the database header. |
| **TxnId** | Monotonically increasing `u64` transaction identifier. `TxnId::ZERO` represents the on-disk baseline. |
| **SIREAD lock** | Marker recording "transaction T read page P under snapshot." Used for SSI rw-antidependency detection. |
| **Intent log** | Semantic operation log: `Vec<IntentOp>`. Records what a transaction intended to do (insert, delete, update). |
| **Deterministic rebase** | Replaying intent logs against the current committed snapshot to merge without byte-level patches. |
| **PageHistory** | Compressed version chain: newest = full image, older = patches (intent logs and/or structured patches). |
| **ARC** | Adaptive Replacement Cache. Balances recency and frequency for buffer pool eviction. |
| **RootManifest** | Bootstrap object in ECS: maps logical database name → current committed state ObjectId. |
| **TxnSlot** | Fixed-size shared-memory record for cross-process MVCC coordination. |

---

## 1. Project Identity

### 1.1 What It Is

FrankenSQLite is a **clean-room Rust reimplementation** of SQLite version 3.52.0
(~218K lines of C). It targets:

- Full SQL dialect compatibility with C SQLite
- File format round-trip interoperability (read/write standard `.sqlite` files)
- Safe Rust (`unsafe_code = "forbid"` at workspace level)
- 95%+ conformance against a golden-file test suite

### 1.2 The Two Innovations

**Innovation 1: MVCC Concurrent Writers.** SQLite's single biggest limitation
is the `WAL_WRITE_LOCK` at `wal.c:3698` -- a single exclusive lock byte that
serializes ALL writers. FrankenSQLite replaces this with page-level MVCC
versioning, allowing transactions that touch different pages to commit in full
parallel. This is the PostgreSQL concurrency model applied at page granularity.

**Innovation 2: RaptorQ-Pervasive Architecture.** Every layer of FrankenSQLite
is infused with RaptorQ fountain codes (RFC 6330), leveraging asupersync's
production-grade implementation. This isn't bolted-on replication -- it's woven
into the storage format, WAL durability, snapshot transfer, version chain
compression, and conflict resolution. The result is a database that treats data
loss as a quantitatively bounded, repairable event under an explicit failure
model rather than a silent corruption or a "panic and pray" failure mode.

### 1.3 Key External Dependencies

| Dependency | Location | Role |
|-----------|----------|------|
| `asupersync` | `/dp/asupersync` | Async runtime, RaptorQ codec, Cx contexts, channels, lab reactor, e-processes |
| `frankentui` | `/dp/frankentui` | TUI framework (CLI shell only) |

**No tokio.** All async I/O uses asupersync exclusively.

### 1.4 Constraints

- **Edition 2024**, nightly toolchain required
- **`unsafe_code = "forbid"`** -- no escape hatches
- **Clippy pedantic + nursery at deny level** -- with specific documented allows
- **23 crates** in workspace under `crates/`
- **Release profile** (as configured in the workspace `Cargo.toml`): `opt-level = 3`,
  `lto = true`, `codegen-units = 1`, `panic = "abort"`, `strip = true`
  (This is a database engine, not a size-constrained CLI tool. Peak throughput
  matters more than binary size.)

### 1.5 Mechanical Sympathy

Database engines live and die by cache behavior, memory layout, and I/O
patterns. The following constraints are non-negotiable for hot-path code:

- **Page alignment.** All page buffers MUST be allocated at `page_size`
  alignment (4096 by default). This enables direct I/O (`O_DIRECT`) and
  avoids partial-page kernel copies. Use `std::alloc::Layout` with alignment.

- **Zero-copy I/O.** The VFS read/write paths MUST NOT allocate intermediate
  buffers. `read_exact_at` / `write_all_at` operate directly on page-aligned
  buffers. The pager hands out `&[u8]` references to cached pages, not copies.

- **SIMD-friendly layouts.** Hot comparison paths (B-tree key comparison,
  checksum computation, RaptorQ GF(256) arithmetic) SHOULD use types whose
  in-memory representation is SIMD-friendly: contiguous byte arrays, no
  pointer chasing, no padding between elements. `xxhash3` already exploits
  this; B-tree cell comparison and RaptorQ matrix ops SHOULD follow suit.

- **Canonical byte representation.** All on-disk structures (page headers,
  cell formats, WAL frames, ECS symbol records) MUST have a single canonical
  byte encoding. Big-endian for SQLite-compatible structures (matching C
  SQLite), little-endian for FrankenSQLite-native ECS structures (matching
  x86/ARM native order for zero-cost decode).

- **Cache-line awareness.** The MVCC `PageLockTable` shards (Section 5.8)
  and `SireadTable` shards MUST be padded to 64-byte cache-line boundaries
  to prevent false sharing between concurrent writers.

- **Prefetch hints.** B-tree descent SHOULD issue prefetch hints for child
  pages when the next page number is known (via `std::arch::x86_64::_mm_prefetch`
  behind a feature gate, with no-op fallback on other architectures).

---

## 2. Why Page-Level MVCC

### 2.1 The Problem

In WAL mode, C SQLite allows multiple concurrent readers but caps the number
of simultaneously active reader locks via `WAL_NREADER` in the wal-index shared
memory (default: 5). It still allows only ONE writer at a time. The
`WAL_WRITE_LOCK` (byte 120 of the WAL index shared memory) is an exclusive
advisory lock. Any connection attempting to write while another holds this lock
receives `SQLITE_BUSY` (or `SQLITE_BUSY_SNAPSHOT` in the fork-protection case).

For applications with mixed read/write workloads across different tables or
different regions of the same table, this is a needless bottleneck. Two users
inserting into unrelated tables should never wait for each other.

### 2.2 Why Page Granularity

| Granularity | Pros | Cons |
|-------------|------|------|
| **Row-level** (PostgreSQL) | Minimal false conflicts | Requires visibility map, per-row xmin/xmax, breaks file format |
| **Page-level** (our choice) | Maps to B-tree I/O unit, preserves file format, simple version chains | False conflicts when rows share a page |
| **Table-level** | Trivial implementation | Nearly useless (most apps have few tables) |

Page-level is the sweet spot: it maps directly to SQLite's B-tree page
architecture (pages are already the unit of I/O, caching, and WAL frames),
preserves the on-disk file format, and provides meaningful concurrency for
real-world workloads where writers typically touch different leaf pages.

### 2.3 The Isolation Level Problem (CRITICAL)

**C SQLite provides SERIALIZABLE isolation** -- trivially, because writers are
serialized by the WAL_WRITE_LOCK. Every execution is equivalent to some serial
ordering of transactions.

**Page-level MVCC provides Snapshot Isolation (SI)**, which is weaker. SI allows
the **write skew anomaly**: two transactions T1 and T2 each read overlapping
data, each writes to a different item based on what they read, and both commit
successfully -- but the combined result is inconsistent.

**Example:** Table has two rows summing to 100. Constraint: sum must stay >= 0.
T1 reads both (50, 50), writes row A = -40. T2 reads both (50, 50), writes
row B = -40. Both commit. Sum is now -30. Constraint violated. Under
SERIALIZABLE, one would have seen the other's write and aborted.

**This is a data corruption risk.** SQLite users depend on SERIALIZABLE. We
cannot silently downgrade.

### 2.4 The Solution: Layered Isolation

**Layer 1 (Ship first): Exact SQLite compatibility mode.**
- `BEGIN` / `BEGIN DEFERRED` / `BEGIN IMMEDIATE` / `BEGIN EXCLUSIVE`: These
  acquire the traditional WAL write lock equivalent. Writers are serialized.
  This provides exact C SQLite SERIALIZABLE semantics. Zero risk of write skew.
- This is the default mode. Existing SQLite applications work unchanged.

**Layer 2: MVCC concurrent mode with SSI (Serializable by Default).**
- `BEGIN CONCURRENT`: New non-standard syntax (matching SQLite's own
  experimental `BEGIN CONCURRENT` branch). Uses page-level MVCC with
  **Serializable Snapshot Isolation (SSI)** -- not merely Snapshot Isolation.
- Multiple concurrent writers, first-committer-wins on page conflicts, plus
  SSI validation to prevent write skew anomalies.
- SSI implements the conservative Cahill/Fekete rule at page granularity
  ("Page-SSI"): no committed transaction may have both an incoming AND
  outgoing rw-antidependency edge. This prevents serialization cycles.
- Applications that opt in get **SERIALIZABLE** concurrent writes. The ~7%
  overhead measured by PostgreSQL 9.1+ is acceptable for correctness.
- `PRAGMA fsqlite.serializable = OFF` provides an explicit opt-out to plain
  Snapshot Isolation for benchmarking or applications that tolerate write skew.
  This is NOT the default.
- This is where the concurrency innovation lives.

**Why SSI ships by default (not deferred):**
- SI silently downgrades correctness. SQLite users depend on SERIALIZABLE.
  Shipping SI-only concurrent mode creates a correctness trap where applications
  that switch from `BEGIN` to `BEGIN CONCURRENT` get weaker guarantees without
  warning.
- The conservative Page-SSI rule (`has_in_rw && has_out_rw => abort`) is
  simple to implement: two boolean flags per transaction, one SIREAD table.
  The overhead is bounded by the number of active transactions times pages read.
- PostgreSQL has proven SSI viable in production since 2011 with <7% overhead
  and ~0.5% false positive abort rate. At page granularity, our false positive
  rate will be somewhat higher, but algebraic write merging (Section 5.10)
  compensates by turning many apparent conflicts into successful merges.
- Starting with SSI from day one means we never ship a correctness regression.
  We can always *reduce* abort rates later (finer-grained SIREAD tracking,
  better victim selection), but we cannot retroactively fix applications that
  relied on SI and experienced silent write skew.

**Layer 3 (Future refinement): Reduced-abort SSI.**
- Refine SIREAD granularity from page to (page, cell_tag) or (page, range_tag)
  to reduce false positive aborts on hot pages.
- Smarter victim selection (instead of always aborting the committing pivot).
- These are optimizations of SSI, not correctness changes.

---

## 3. RaptorQ: The Information-Theoretic Foundation

### 3.1 What RaptorQ Is

RaptorQ (RFC 6330) is a fountain code -- a class of erasure codes where the
encoder can produce a practically unlimited stream of encoding symbols from K
source symbols, and the decoder can recover the original K source symbols from
ANY set of K' encoding symbols where K' is only slightly larger than K (in most
cases, K' = K suffices).

**Key properties:**
- **Information-theoretically optimal**: Achieves capacity of the erasure channel
- **Systematic**: The first K encoding symbols ARE the source symbols (zero
  encoding overhead for the common no-loss case)
- **Rateless**: Generate as many repair symbols as needed on-the-fly
- **Universal**: Works for any symbol size (we use page-sized symbols)

RaptorQ improves upon the original Raptor code (RFC 5053) in several ways:
it uses GF(256) arithmetic for the HDPC constraints instead of GF(2), which
dramatically improves the failure probability at the minimum overhead of zero
extra symbols. Where Raptor codes over GF(2) have a ~1-2% failure rate when
decoding with exactly K symbols, RaptorQ achieves ~0.01% failure rate under
the same conditions. With just one additional symbol (K+1 received), the
failure rate drops to approximately 10^-5. With two additional symbols (K+2),
it drops to approximately 10^-7. This near-perfect recovery rate is what
makes RaptorQ suitable as a foundational building block for database durability
rather than merely a network transport optimization.

The RFC 6330 specification defines behavior for source blocks containing up
to 56,403 source symbols (K_max = 56403). Each symbol is a contiguous block
of T octets. For FrankenSQLite, T = page_size (typically 4096 bytes), so a
single source block can cover up to 56,403 pages, or approximately 220MB of
database content. Larger databases are partitioned into multiple source blocks
   (see Section 3.4.3).

### 3.2 How RaptorQ Works (Essential Understanding)

This section provides the depth necessary for an implementor to understand
every step of the RaptorQ encoding and decoding pipeline. While FrankenSQLite
uses asupersync's production-grade implementation rather than re-implementing
RFC 6330, understanding the internals is essential for correct integration,
debugging, and performance tuning.

#### 3.2.1 GF(256) Arithmetic -- The Algebraic Foundation

All RaptorQ operations beyond simple XOR are performed over the Galois Field
GF(2^8), commonly written GF(256). This is the field with exactly 256
elements, which maps perfectly to byte values 0x00 through 0xFF. Understanding
this arithmetic is critical because it appears in HDPC constraint generation,
the LT encoding function, and all symbol operations.

**The Field GF(2^8) with Irreducible Polynomial**

GF(2^8) is constructed as the quotient ring GF(2)[x] / p(x), where p(x) is
an irreducible polynomial of degree 8 over GF(2). RFC 6330 specifies:

```
p(x) = x^8 + x^4 + x^3 + x^2 + 1
```

In hexadecimal, this is 0x11D (binary: 1_0001_1101). The field elements are
the 256 polynomials of degree < 8 with coefficients in GF(2) = {0, 1}. Each
such polynomial maps to a byte:

```
Element     Polynomial            Byte
-------     ----------            ----
0           0                     0x00
1           1                     0x01
2           x                     0x02
3           x + 1                 0x03
...
0xA3        x^7 + x^5 + x + 1    0xA3
0x47        x^6 + x^2 + x + 1    0x47
...
255         x^7 + ... + x + 1     0xFF
```

**Addition: XOR**

Addition in GF(2^8) is polynomial addition with coefficients reduced modulo 2.
Since coefficients are in {0, 1}, addition modulo 2 is just XOR:

```
a + b = a XOR b
```

The additive identity is 0x00. Every element is its own additive inverse
(a + a = a XOR a = 0), which means subtraction is also XOR:

```
a - b = a XOR b = a + b
```

This is enormously convenient for implementation: addition is a single XOR
instruction, and it works on any register width. On a 64-bit machine, we can
add 8 GF(256) elements simultaneously with a single u64 XOR.

**Multiplication via Log/Exp Tables**

Direct polynomial multiplication modulo p(x) requires a sequence of shifts
and conditional XORs. While possible, this is slow. RaptorQ instead uses
logarithm and exponential tables based on a primitive element (generator) of
the multiplicative group GF(256)*.

The multiplicative group GF(256)* consists of the 255 non-zero elements and
is cyclic. RFC 6330 §5.7 specifies the generator g = 2 (the polynomial
x). Every non-zero element a can be written as a = g^k for some unique
k in {0, 1, ..., 254}. We define:

```
OCT_LOG[a] = k    such that g^k = a    (for a != 0)
OCT_EXP[k] = g^k  (for k = 0, 1, ..., 254)
```

The OCT_LOG table has 256 entries (OCT_LOG[0] is undefined / sentinel).
The OCT_EXP table has 256 entries but is typically extended to 510 entries
(OCT_EXP[k] for k = 0..509, where OCT_EXP[k+255] = OCT_EXP[k]) to avoid
a modular reduction after addition of logarithms.

Together, these tables consume 256 + 510 = 766 bytes. In practice, the
OCT_EXP table is stored with 512 entries for alignment, so total storage
is 256 + 512 = 768 bytes for the base lookup tables.

**Multiplication algorithm:**

```
multiply(a, b):
    if a == 0 or b == 0: return 0
    return OCT_EXP[(OCT_LOG[a] + OCT_LOG[b]) % 255]
```

With the extended OCT_EXP table (510 entries), the modular reduction
is unnecessary since OCT_LOG[a] + OCT_LOG[b] <= 254 + 254 = 508 < 510:

```
multiply(a, b):
    if a == 0 or b == 0: return 0
    return OCT_EXP[OCT_LOG[a] + OCT_LOG[b]]    // no modular reduction needed
```

This is O(1): two table lookups, one addition, one more table lookup.

**Division:**

```
divide(a, b):
    assert(b != 0)
    if a == 0: return 0
    return OCT_EXP[(OCT_LOG[a] - OCT_LOG[b] + 255) % 255]
```

Or equivalently, using the multiplicative inverse:

```
inverse(b):
    assert(b != 0)
    return OCT_EXP[255 - OCT_LOG[b]]

divide(a, b):
    return multiply(a, inverse(b))
```

**Worked Example: 0xA3 * 0x47**

Let us multiply 0xA3 (163 decimal) by 0x47 (71 decimal) step by step.

```
Step 1: Look up logarithms
    0xA3 = x^7 + x^5 + x + 1
    Using the OCT_LOG table (computed from g = 2):
    OCT_LOG[0xA3] = 146
    OCT_LOG[0x47] = 63

Step 2: Add logarithms
    146 + 63 = 209

Step 3: Look up exponential
    OCT_EXP[209] = 0x8E   (this is g^209 mod p(x))

Step 4: Result
    0xA3 * 0x47 = 0x8E   (142 decimal)
```

Verification: 0x8E = x^7 + x^3 + x^2 + x. We can confirm by directly
multiplying the polynomials (x^7 + x^5 + x + 1)(x^6 + x^2 + x + 1) modulo
p(x) = x^8 + x^4 + x^3 + x^2 + 1, and reducing modulo 2 in each coefficient.

**Bulk Multiplication Tables (MUL_TABLES)**

For high-throughput encoding and decoding, asupersync precomputes a 64KB
table MUL_TABLES[256][256] where MUL_TABLES[a][b] = a * b in GF(256). This
trades memory for speed: a single array index replaces the log-add-exp
sequence, reducing multiplication to a single memory load.

```
MUL_TABLES: [[u8; 256]; 256]    // 65,536 bytes total

// Precomputation (done once at startup):
for a in 0..256 {
    for b in 0..256 {
        MUL_TABLES[a][b] = if a == 0 || b == 0 {
            0
        } else {
            OCT_EXP[(OCT_LOG[a] as u16 + OCT_LOG[b] as u16) as usize]
        };
    }
}

// Usage (O(1) single lookup):
fn mul(a: u8, b: u8) -> u8 {
    MUL_TABLES[a as usize][b as usize]
}
```

**Why GF(256) and Not GF(2)?**

The original Raptor codes (RFC 5053) use GF(2) (binary) for all operations,
meaning addition is XOR and the only multiplication is by 0 or 1. This is
extremely fast but limits the algebraic structure. RaptorQ uses GF(256) for
the HDPC (Half-Distance Parity-Check) constraints specifically because:

1. **Byte alignment**: GF(256) elements are exactly one byte. All operations
   are naturally aligned to the machine's byte-addressable memory model.
2. **SIMD friendliness**: XOR (addition) works on entire 64-bit words,
   processing 8 GF(256) additions in a single instruction. For multiplication,
   modern CPUs with PCLMULQDQ or VPGATHERDD can process multiple GF(256)
   multiplications in parallel.
3. **Algebraic strength**: The HDPC constraints over GF(256) provide much
   stronger error-correction capability than GF(2), which is the primary
   reason RaptorQ achieves better failure probability than Raptor codes.
4. **Information density**: Each GF(256) coefficient carries 8 bits of
   information (vs 1 bit for GF(2)), meaning the dense HDPC matrix rows
   carry 8x more constraint information per element.

The cost is that GF(256) multiplication is more expensive than GF(2)
multiplication (a table lookup vs a single AND), but this is paid only in
the HDPC rows (H rows out of L total), not in the LDPC or LT rows which
remain sparse and binary.

#### 3.2.2 Symbol Operations

A **symbol** in RaptorQ is a vector of T octets, where T is the symbol size.
For FrankenSQLite, T = page_size = 4096 bytes (the default SQLite page size).
All encoding and decoding operations are performed symbol-by-symbol, where
each "scalar" operation on a GF(256) element is lifted to a vector operation
on T octets.

**Symbol Addition (XOR)**

```
symbol_add(A: &[u8; T], B: &[u8; T]) -> [u8; T]:
    result = [0u8; T]
    for i in 0..T:
        result[i] = A[i] ^ B[i]
    return result
```

In practice, this is SIMD-accelerated by operating on u64 (8 bytes at a time)
or u128 / SIMD registers (16-32 bytes at a time):

```
symbol_add_fast(A: &[u8; T], B: &[u8; T], out: &mut [u8; T]):
    let a_words = A.as_ptr() as *const u64
    let b_words = B.as_ptr() as *const u64
    let o_words = out.as_mut_ptr() as *mut u64
    for i in 0..(T / 8):
        *o_words.add(i) = *a_words.add(i) ^ *b_words.add(i)
```

For T = 4096, this is 512 u64 XOR operations = 512 instructions, which
modern CPUs can execute in ~64 cycles (8-wide superscalar pipeline). This
is the dominant operation in both encoding and decoding.

**Symbol Scalar Multiplication**

Multiplying a symbol by a GF(256) scalar c means multiplying each byte
independently:

```
symbol_mul(c: u8, A: &[u8; T]) -> [u8; T]:
    if c == 0: return [0u8; T]
    if c == 1: return A.clone()
    result = [0u8; T]
    for i in 0..T:
        result[i] = MUL_TABLES[c as usize][A[i] as usize]
    return result
```

This requires T table lookups. For T = 4096, that is 4096 lookups into the
same 256-byte row of MUL_TABLES (MUL_TABLES[c]), which fits in L1 cache
and achieves excellent throughput.

**Symbol Multiply-and-Add (Fused Operation)**

The most common operation in Gaussian elimination is "add c * row_j to row_i":

```
symbol_addmul(dst: &mut [u8; T], c: u8, src: &[u8; T]):
    if c == 0: return    // no-op
    if c == 1:
        symbol_xor(dst, src)    // just XOR
        return
    let mul_row = &MUL_TABLES[c as usize]
    for i in 0..T:
        dst[i] ^= mul_row[src[i] as usize]
```

This fused operation avoids allocating a temporary symbol and is the
innermost loop of the decoder. Performance here directly determines
overall decode throughput.

**Symbol Operations Are the Building Blocks**

Every RaptorQ operation -- LDPC constraint evaluation, HDPC constraint
evaluation, LT encoding, Gaussian elimination during decoding -- reduces
to sequences of symbol_add (XOR) and symbol_addmul. The entire algebraic
machinery of GF(256) ultimately manifests as these two operations applied
to 4096-byte vectors.

#### 3.2.3 Encoding Step by Step

The RaptorQ encoding process transforms K source symbols into a potentially
unlimited stream of encoding symbols. Here is the complete procedure:

**Step 1: Determine Coding Parameters**

Given K source symbols C'[0], C'[1], ..., C'[K-1]:

1. Look up K' in the systematic index table (RFC 6330 Table 2). K' is the
   smallest value in the table that is >= K. The table contains 477 entries
   covering K from 1 to 56,403. For example:
   - K = 5 -> K' = 6
   - K = 10 -> K' = 10
   - K = 100 -> K' = 101

2. Pad the source block with (K' - K) zero symbols to get exactly K' source
   symbols: C'[0], ..., C'[K-1], 0, 0, ..., 0.

3. For K', the systematic index table also defines:
   - J(K'): the systematic index (used in the Tuple generator)
   - S(K'): the number of LDPC symbols (S is the smallest prime >= ceil(0.01 * K') + X,
     where X depends on K')
   - H(K'): the number of HDPC symbols (H = ceil(sqrt(K')))
   - W(K'): used in the LT generator (W is the smallest prime >= floor(K'/S) * S + 1)

4. L = K' + S + H: the total number of intermediate symbols.

**Step 2: Construct the Constraint Matrix A**

The constraint matrix A is an L x L matrix that encodes the relationship
between intermediate symbols C[0], ..., C[L-1] and the source/constraint
data. A is divided into three regions:

```
A (L x L matrix):
    Rows 0 to S-1:          LDPC constraints (sparse, over GF(2))
    Rows S to S+H-1:        HDPC constraints (dense, over GF(256))
    Rows S+H to L-1:        LT constraints for source symbols (sparse, over GF(2))

         |<--- K' cols --->|<- S cols ->|<- H cols ->|
    LDPC |   LDPC_LEFT     | I_S(SxS)  |   0        |  S rows
    HDPC |   MT * GAMMA    |   0        | I_H(HxH)  |  H rows
    LT   |   LT_MATRIX     |   0        |   0        |  K' rows
```

**LDPC rows (0..S-1):** Each LDPC row has exactly ceil(K'/S) + 2 non-zero
entries in the leftmost K' columns, plus a 1 on the diagonal of the S x S
identity block. These constraints are sparse (typically ~7 non-zero entries
per row for typical K' values) and binary (over GF(2)).

The LDPC constraint for row i (0 <= i < S) sets the following positions to 1:
- For j = 0, 1, ..., W-2: if (i == (j % S)): set column j to 1
- Two additional columns determined by Rand(j, i, S) for specific j values
- Column K' + i is set to 1 (the identity block)

**HDPC rows (S..S+H-1):** These rows use GF(256) coefficients and are dense
over the first K' + S columns. The HDPC constraints are generated using:
1. The MT matrix (H x (K'+S)), computed from a random walk using the
   Rand function
2. The GAMMA matrix ((K'+S) x (K'+S)), a specific structured matrix over
   GF(256) defined by alpha (a primitive element of GF(256))

The HDPC rows provide the "algebraic strength" that makes RaptorQ achieve
near-optimal failure probability. They are the reason GF(256) is used.

**LT rows (S+H..L-1):** Row S+H+i corresponds to source symbol C'[i]. Each
LT row is generated by the Tuple function and the LT encoding relation. For
source symbol i:

```
(d, a, b, d1, a1, b1) = Tuple(K', i)
// d = LT degree, a/b = LT parameters
// d1, a1, b1 = permanent inactivation parameters

Row S+H+i has 1s at positions:
    b                          (always)
    (b + a) mod W              (if d >= 2)
    (b + 2*a) mod W            (if d >= 3)
    ...
    (b + (d-1)*a) mod W        (if degree is d)
Plus "permanent inactivation" entries from d1, a1, b1 in columns W..K'-1
```

**Step 3: Build the Source Vector D**

The source vector D has L entries:

```
D[0..S-1]      = zero symbols (LDPC constraints have zero right-hand side)
D[S..S+H-1]    = zero symbols (HDPC constraints have zero right-hand side)
D[S+H..L-1]    = C'[0], C'[1], ..., C'[K'-1]  (the padded source symbols)
```

**Step 4: Solve A * C = D for Intermediate Symbols**

This is the key step. We need to find intermediate symbols C[0], ..., C[L-1]
such that A * C = D. Since A is L x L and invertible (by construction for
valid K'), this is a standard linear system solve over GF(256).

The solve uses Gaussian elimination with partial pivoting. The matrix A has
been carefully designed so that its structure (sparse LDPC + dense HDPC +
sparse LT) is amenable to efficient elimination. In particular, the
inactivation decoding algorithm (Section 3.2.4) exploits this structure.

After solving, we have intermediate symbols C[0], C[1], ..., C[L-1].

**Step 5: Generate Encoding Symbols**

Given the intermediate symbols, any encoding symbol with Internal Symbol ID
(ISI) X can be generated:

```
generate_symbol(X, K', C[0..L-1]):
    if X < K':
        return C'[X]    // systematic: return the source symbol itself
    else:
        return LTEnc(K', C[0..L-1], X)
```

The LTEnc function for ISI X >= K':

```
LTEnc(K', C[0..L-1], X):
    (d, a, b, d1, a1, b1) = Tuple(K', X)
    result = C[b]
    for j in 1..d:
        b = (b + a) mod W
        result = result XOR C[b]
    // Permanent inactivation component
    while b1 >= L:
        b1 = (b1 + a1) mod P1
    result = result XOR C[b1]
    for j in 1..d1:
        b1 = (b1 + a1) mod P1
        while b1 >= L:
            b1 = (b1 + a1) mod P1
        result = result XOR C[b1]
    return result
```

**Systematic Property:** For ISI X < K', the encoding symbol is exactly the
source symbol C'[X]. This means that in the no-loss case, the receiver
already has all K source symbols and no decoding is needed. The repair
symbols (ISI >= K') are generated only as redundancy.

#### 3.2.4 Decoding Step by Step

Decoding is the inverse problem: given N received encoding symbols (where
N >= K' and ideally N is close to K'), recover the K' source symbols.

**Step 1: Collect Received Symbols**

The receiver collects N encoding symbols with their ISIs. Some may be source
symbols (ISI < K'), others may be repair symbols (ISI >= K'). The receiver
does not need to know which symbols were lost -- it only needs N symbols,
any N symbols.

**Step 2: Build the Decoding Matrix A'**

Construct an N x L matrix A' where row i corresponds to received symbol with
ISI X_i:

```
For each received symbol with ISI X_i:
    If X_i < K' (source symbol):
        Row i = row S+H+X_i of the original constraint matrix A
    Else (repair symbol):
        Row i = LT encoding vector for ISI X_i
        (computed from Tuple(K', X_i), same as during encoding)
```

Prepend the S LDPC constraint rows and H HDPC constraint rows to get the
full system. The extended matrix has (S + H + N) rows and L columns:

```
A_extended (S+H+N rows x L columns):
    Rows 0..S-1:       LDPC constraints
    Rows S..S+H-1:     HDPC constraints
    Rows S+H..S+H+N-1: received symbol constraints

D_extended:
    D[0..S-1]       = zero symbols
    D[S..S+H-1]     = zero symbols
    D[S+H..S+H+N-1] = received symbol data
```

The system is overdetermined (S+H+N >= L when N >= K'), so we need to find
C[0..L-1] satisfying at least L of the S+H+N equations.

**Step 3: Inactivation Decoding (Two Phases)**

This is the heart of RaptorQ decoding and what makes it efficient. Direct
Gaussian elimination on an L x L matrix over GF(256) would cost O(L^3)
operations. Inactivation decoding exploits the sparse structure to achieve
near-linear average-case performance.

**Phase 1: Peeling (O(K) average case)**

The peeling phase iteratively processes rows that have exactly one unknown
symbol (i.e., rows with exactly one non-zero entry in the remaining
unresolved columns):

```
peeling():
    resolved = {}   // set of resolved symbol indices
    while exists row r with exactly 1 unresolved column c:
        // Row r: a_{r,c} * C[c] = D[r] - sum(a_{r,j} * C[j] for j in resolved)
        // Since a_{r,c} is the only unresolved coefficient:
        C[c] = (D[r] XOR sum(a_{r,j} * C[j] for resolved j)) * inverse(a_{r,c})
        resolved.add(c)
        // Remove column c from all other rows (update their right-hand sides)
```

Because the LDPC and LT rows are sparse, the peeling phase resolves the
majority of intermediate symbols. For a well-received block (N slightly
above K'), peeling typically resolves 90-95% of symbols in O(K) total
operations (each row touches only ~d columns where d is the LT degree,
and the average degree is O(log K)).

The peeling phase also identifies **inactive** symbols: those that cannot
be resolved by peeling because they appear in multiple unresolved rows.
The number of inactive symbols is typically small (on the order of
sqrt(K') to log(K')), thanks to the careful code design.

**Phase 2: Gaussian Elimination on the Inactive Subsystem**

After peeling, a small dense subsystem of I inactive symbols remains.
This subsystem has I unknowns and is solved by standard Gaussian
elimination over GF(256):

```
gaussian_solve(inactive_matrix, inactive_rhs):
    // inactive_matrix is approximately I x I where I ~ O(sqrt(K'))
    // Standard GF(256) Gaussian elimination with partial pivoting:
    for col in 0..I:
        // Find pivot row
        pivot_row = find_row_with_nonzero_entry_in_column(col)
        if pivot_row is None:
            return DECODING_FAILURE
        swap_rows(col, pivot_row)
        // Eliminate column from all other rows
        pivot_val = inactive_matrix[col][col]
        for row in 0..I:
            if row != col and inactive_matrix[row][col] != 0:
                factor = mul(inactive_matrix[row][col], inverse(pivot_val))
                // Row operation: row[row] -= factor * row[col]
                for j in col..I:
                    inactive_matrix[row][j] ^= mul(factor, inactive_matrix[col][j])
                inactive_rhs[row] = symbol_addmul(inactive_rhs[row], factor, inactive_rhs[col])
    // Back-substitute to get inactive symbol values
    for col in (0..I).rev():
        C[inactive[col]] = symbol_mul(inverse(inactive_matrix[col][col]), inactive_rhs[col])
```

The cost of Phase 2 is O(I^2 * T) for the symbol operations plus O(I^3)
for the matrix operations. Since I is small (typically < 50 for K' < 10000),
this is negligible compared to Phase 1.

**Step 4: Recover All Intermediate Symbols**

After Phase 2, all inactive symbols are known. We then "reverse peel" through
the Phase 1 resolutions in reverse order to recover all intermediate symbols.

**Step 5: Reconstruct Source Symbols**

With all intermediate symbols C[0..L-1] known, any source symbol can be
reconstructed:

```
for i in 0..K':
    C'[i] = LTEnc(K', C[0..L-1], i)
    // But since the code is systematic, this just picks the right
    // linear combination of intermediate symbols
```

For source symbols that were received directly, the reconstructed value
should match exactly (this serves as a verification check).

**Step 6: Strip Padding**

Discard the (K' - K) padding symbols to recover the original K source
symbols C'[0], ..., C'[K-1].

**Decoding Failure Behavior (Normative):**

RFC 6330 states that the decoder can recover the source block from *almost any*
set of encoding symbols of sufficient cardinality: *in most cases* `K` symbols
suffice; *in rare cases* slightly more than `K` are required. We therefore
treat decoding failure as a normal, recoverable event:

- Correctness MUST NOT depend on decoding succeeding with exactly `K` symbols.
- Durability/replication code MUST be able to obtain more symbols (local repair
  store and/or peers) and retry decode.
- For durability-critical objects, the writer MUST persist an explicit overhead
  policy (e.g., "store `K + r` repair symbols") in the object metadata so
  readers know what to request.

**Verification (Alien-Artifact Discipline):** we do not hard-code or assume
numerical failure probabilities. Instead, we continuously validate the *observed*
failure rate envelope as a function of `(K, r, symbol_size)` using lab tests and
anytime-valid monitoring (e-process/e-values) so regressions are caught even
under optional stopping.

#### 3.2.5 The Tuple Generator and Systematic Index Table

The Tuple function maps an ISI (Internal Symbol ID) to a 6-tuple
(d, a, b, d1, a1, b1) that determines which intermediate symbols participate
in generating that encoding symbol. This function is deterministic and
depends only on K' and the ISI.

The systematic index table (RFC 6330 Table 2) is a precomputed table of
477 entries. For each supported K', it stores a value J(K') such that the
first K' encoding symbols (ISIs 0 through K'-1) correspond exactly to the
K' source symbols. This is the "systematic" property -- it's engineered so
that the encoding matrix has an embedded identity for the source symbols.

The Tuple function uses the Rand function (a hash combining K', ISI, and
an iteration counter) to pseudorandomly but deterministically select the
LT degree and the positions of the non-zero entries. The degree distribution
is the "RaptorQ degree distribution" (RFC 6330 §5.3.5.4), which is a
carefully tuned soliton-like distribution optimized for inactivation decoding.

### 3.3 Asupersync's RaptorQ Implementation

Asupersync contains a complete, production-grade RFC 6330 implementation:

- **GF(256) engine**: 64KB MUL_TABLES for O(1) multiply, u64-wide bulk XOR
  operations for SIMD-like throughput on symbol data
- **Systematic encoder**: Full LDPC+HDPC+LT constraint construction, Gaussian
  elimination for intermediate symbol generation
- **Inactivation decoder**: Two-phase (peeling then Gaussian on inactive subset),
  efficient for the typical case where most symbols are "easy"
- **Decode proof system**: When decoding fails, produces explainable artifacts
  with replay verification
- **Cancel-safe pipelines**: Uses Cx checkpoint at symbol boundaries for
  cooperative cancellation
- **Distributed module**: Consistent hashing, quorum-based symbol distribution,
  recovery protocols

The implementation is structured as a layered set of modules (asupersync paths
shown for navigation):

```
src/raptorq/gf256.rs        -- GF(256) arithmetic
src/raptorq/linalg.rs       -- sparse/dense linear algebra over GF(256)
src/raptorq/systematic.rs   -- systematic index table + tuple generator machinery
src/raptorq/decoder.rs      -- inactivation decoder (peeling + Gaussian)
src/raptorq/proof.rs        -- explainable decode proofs / failure reasons
src/raptorq/pipeline.rs     -- end-to-end sender/receiver pipelines
src/distributed/            -- quorum routing + recovery (for replication use-cases)
```

FrankenSQLite integrates primarily via the pipeline builders (`RaptorQSender*`
and `RaptorQReceiver*`) plus the lower-level decode proof artifacts:

```rust
use asupersync::config::RaptorQConfig;
use asupersync::raptorq::{RaptorQReceiverBuilder, RaptorQSenderBuilder};

// Encoding + send (transport is a SymbolSink; omitted here)
let config = RaptorQConfig::default();
let mut sender = RaptorQSenderBuilder::new()
    .config(config.clone())
    .transport(sink)
    .build()?;
sender.send_object(cx, object_id, &bytes)?;

// Receive + decode (source is a SymbolStream; omitted here)
let mut receiver = RaptorQReceiverBuilder::new()
    .config(config)
    .source(stream)
    .build()?;
let out = receiver.receive_object(cx, &params)?;
let bytes = out.data;
```

### 3.4 RaptorQ Integration Points in FrankenSQLite

RaptorQ permeates every layer of FrankenSQLite:

#### 3.4.1 Self-Healing WAL (Erasure-Coded Durability)

**Problem:** Traditional WAL relies on "hope nothing goes wrong during the
write." A torn write (power loss mid-write) corrupts the frame. SQLite detects
this via checksums and discards the frame, losing the transaction.

**Solution:** Each WAL commit group is RaptorQ-encoded.

```
WAL Commit (N pages):
  Source symbols:   [Page1_data | Page2_data | ... | PageN_data]
  Repair symbols:   R additional symbols (configurable redundancy)
  Written to WAL:  N source frames + R repair frames

Recovery:
  If any frames are torn/corrupted (detected by checksum):
    Collect surviving frames (source + repair)
    If |surviving| >= N: RaptorQ-decode to recover ALL source pages
    Else: transaction is truly lost (requires catastrophic multi-frame loss)
```

**Concrete WAL Commit Frame Layout**

Each frame in the WAL (both source and repair) has an identical physical
structure:

```
Frame Layout (4120 bytes for 4096-byte pages):
    Offset  Size    Field
    ------  ----    -----
    0       4       Page number (u32 big-endian)
                    - For source frames: the actual database page number (1-based)
                    - For repair frames: 0xFFFFFFFF (sentinel indicating repair)
    4       4       Commit frame count (u32 big-endian)
                    - Non-zero only on the LAST frame of a commit group
                    - Equals N + R (total frames in commit group)
                    - Zero for all other frames in the group
    8       4       Checksum-1 (cumulative, SQLite-native algorithm)
    12      4       Checksum-2 (cumulative, SQLite-native algorithm)
    16      4       RaptorQ metadata (u32 big-endian):
                    - Bits 31-28: repair symbol flag (0x0 = source, 0xF = repair)
                    - Bits 27-24: reserved
                    - Bits 23-0: encoding symbol ID (ISI)
    20      4       Source block size K (u32 big-endian)
                    - Number of source symbols (= N, the number of pages in commit)
    24      4096    Page data (source frame) or repair symbol data (repair frame)

Total frame size: 24 + page_size = 4120 bytes
```

The RaptorQ metadata field at offset 16 is stored in space that C SQLite
reserves as padding in the frame header. This means the frame header remains
exactly 24 bytes, maintaining compatibility with tools that scan WAL files.
A C SQLite reader will see repair frames as writes to page 0xFFFFFFFF, which
it will ignore (no valid page has that number). This provides forward
compatibility: a C SQLite database can read a FrankenSQLite WAL and simply
skip the repair frames.

**Worked Example: Commit of 5 Pages with 2 Repair Symbols**

Suppose a transaction modifies pages 7, 12, 45, 100, and 203. With
`PRAGMA raptorq_repair_symbols = 2`, the WAL commit group contains 7 frames:

```
Frame 0 (Source, ISI=0):
    [00000007 | 00000000 | cksum1 | cksum2 | 00000000 | 00000005 | <page 7 data, 4096B>]
    Page number: 7, commit count: 0 (not last), ISI: 0, K: 5

Frame 1 (Source, ISI=1):
    [0000000C | 00000000 | cksum1 | cksum2 | 00000001 | 00000005 | <page 12 data, 4096B>]

Frame 2 (Source, ISI=2):
    [0000002D | 00000000 | cksum1 | cksum2 | 00000002 | 00000005 | <page 45 data, 4096B>]

Frame 3 (Source, ISI=3):
    [00000064 | 00000000 | cksum1 | cksum2 | 00000003 | 00000005 | <page 100 data, 4096B>]

Frame 4 (Source, ISI=4):
    [000000CB | 00000000 | cksum1 | cksum2 | 00000004 | 00000005 | <page 203 data, 4096B>]

Frame 5 (Repair, ISI=5):
    [FFFFFFFF | 00000000 | cksum1 | cksum2 | F0000005 | 00000005 | <repair symbol 5, 4096B>]
    Page number: sentinel, ISI: 5, repair flag: 0xF

Frame 6 (Repair, ISI=6):
    [FFFFFFFF | 00000007 | cksum1 | cksum2 | F0000006 | 00000005 | <repair symbol 6, 4096B>]
    Page number: sentinel, commit count: 7 (last frame!), ISI: 6, repair flag: 0xF
```

Total WAL bytes for this commit: 7 * 4120 = 28,840 bytes.
Without repair symbols: 5 * 4120 = 20,600 bytes.
Overhead: 40% for R=2. This is configurable; R=1 gives 20% overhead.

**Recovery Algorithm**

When opening a database with a WAL file, FrankenSQLite scans the WAL and
recovers commit groups:

```
recover_wal(wal_file):
    frames = []
    offset = 32    // skip WAL header

    while offset < wal_file.size():
        header = read_frame_header(wal_file, offset)

        // Verify frame checksum
        if not verify_checksum(header, wal_file, offset):
            // Frame is corrupted (torn write)
            frames.append(CorruptedFrame { offset, header })
        else:
            frames.append(ValidFrame { offset, header, data })

        offset += 24 + page_size

    // Group frames into commit groups
    // (a commit group ends at a frame with non-zero commit count)
    commit_groups = partition_into_commit_groups(frames)

    for group in commit_groups:
        valid_sources = [f for f in group if f.is_valid() and f.is_source()]
        valid_repairs = [f for f in group if f.is_valid() and f.is_repair()]
        corrupted     = [f for f in group if f.is_corrupted()]
        K = group.source_block_size()    // from the K field in frame headers

        if len(corrupted) == 0:
            // No corruption: apply all source frames directly
            apply_source_frames(valid_sources)

        else if len(valid_sources) + len(valid_repairs) >= K:
            // RaptorQ recovery possible!
            decoder = RaptorQDecoder::new(K, page_size)
            for f in valid_sources:
                decoder.add_symbol(f.isi(), f.data())
            for f in valid_repairs:
                decoder.add_symbol(f.isi(), f.data())
            recovered = decoder.decode()?
            // Apply recovered source symbols as page writes
            apply_recovered_pages(recovered, group.page_numbers())

        else:
            // Catastrophic loss: too many frames corrupted
            // Discard this commit group (transaction is lost)
            log_warning("Commit group at offset {} unrecoverable", group.offset)
            break   // No further commit groups can be trusted
```

**PRAGMA raptorq_repair_symbols Semantics**

```
PRAGMA raptorq_repair_symbols;          -- Query current value (default: 2)
PRAGMA raptorq_repair_symbols = N;      -- Set to N (0 disables, max 255)
```

- N = 0: Exact C SQLite behavior. No repair frames written. No recovery
  from torn writes beyond what the checksum chain provides.
- N = 1: Tolerates 1 corrupted frame per commit group. Recommended minimum
  for production use. Overhead: 1/K additional WAL space per commit.
- N = 2: Tolerates 2 corrupted frames. Default. Overhead: 2/K additional
  WAL space.
- N > K: Valid but wasteful (more repair symbols than source symbols). The
  encoder will generate them, but the marginal benefit beyond N = 3 or 4
  is negligible for typical corruption patterns.

The PRAGMA is persistent (stored in the database header's reserved bytes
at offset 72-91) and takes effect on the next transaction commit.

**Impact:** A commit of 10 pages with 2 repair symbols survives ANY 2 torn
frames. The probability of losing a committed transaction drops from
`P(any_torn_write)` to `P(more_than_R_torn_writes)`, which for R >= 2 is
astronomically small. This eliminates the need for double-write journaling.

**Configuration:** `PRAGMA raptorq_repair_symbols = N` (default: 2).
Set to 0 for exact C SQLite behavior (no repair symbols).

#### 3.4.2 Fountain-Coded Replication

**Problem:** Database replication traditionally uses TCP streams or
change-based approaches. These are fragile (connection drops require
restart), bandwidth-inefficient (retransmission of lost packets), and
order-dependent.

**Solution:** FrankenSQLite's replication protocol is fountain-coded:

```
Replication of changeset C (K pages):
  Sender: Continuously emit RaptorQ repair symbols for C
  Receiver: Collect symbols until K' >= K received
  Decode: Recover all K pages
  Apply: Write pages to local database

Properties:
  - UDP-based: no connection state, no retransmission
  - Multicast-capable: one sender, many receivers
  - Bandwidth-optimal: no wasted retransmission
  - Order-independent: symbols arrive in any order
  - Resumable: receiver can start collecting from any point
```

**Protocol State Machine -- Sender Side**

```
States: IDLE -> ENCODING -> STREAMING -> COMPLETE

IDLE:
    Entry: No active replication session.
    Trigger: New committed transaction (or explicit REPLICATE command).
    Action: Collect the transaction's write set (K dirty pages).
    Transition -> ENCODING

ENCODING:
    Entry: Have K source symbols (page data).
    Action:
        - Create RaptorQ encoder for K source symbols, symbol size = page_size
        - Compute intermediate symbols (one-time cost: O(K * page_size))
        - Prepare the ISI counter starting at 0
    Transition -> STREAMING

STREAMING:
    Entry: Encoder ready, ISI counter initialized.
    Action (loop):
        - Generate encoding symbol for current ISI
        - Package into UDP packet (format below)
        - Send packet to destination(s) (unicast or multicast)
        - Increment ISI
        - If ISI < K: sending source symbols (systematic)
        - If ISI >= K: sending repair symbols (fountain)
        - Continue until:
            a) Receiver ACKs completion (optional, for unicast), OR
            b) ISI reaches sender-configured maximum (e.g., 2*K), OR
            c) Explicit stop command
    Transition -> COMPLETE (on any stop condition)

COMPLETE:
    Entry: Streaming finished.
    Action: Release encoder resources. Log replication metrics.
    Transition -> IDLE
```

**UDP Packet Format**

```
Replication Packet (variable size, typically 4104 bytes):
    Offset  Size    Field
    ------  ----    -----
    0       1       Source block number (u8)
                    - Identifies which source block this symbol belongs to
                    - For changesets <= 56,403 pages, always 0
                    - For larger changesets, identifies the partition
    1       3       Encoding Symbol ID (u24 big-endian)
                    - The ISI of this symbol
                    - 0 to K-1 for source symbols, >= K for repair symbols
    4       4       Source block size K (u32 big-endian)
                    - Number of source symbols in this block
    8       T       Symbol data (T bytes, where T = page_size)
                    - The actual encoding symbol content

Total packet size: 8 + T bytes (e.g., 8 + 4096 = 4104 bytes)
```

For MTU-constrained networks (e.g., standard Ethernet MTU = 1500 bytes),
the symbol size T can be reduced by sub-dividing each page into sub-symbols.
A 4096-byte page becomes 3 sub-symbols of 1400 bytes each (with 4096 - 4200
= -104 bytes padding, so actually 3 sub-symbols of 1366 bytes to fit). This
is handled transparently by the sub-blocking mechanism of RFC 6330.

**Receiver State Machine**

```
States: LISTENING -> COLLECTING -> DECODING -> APPLYING -> COMPLETE

LISTENING:
    Entry: Receiver is ready to accept replication data.
    Action: Listen on configured UDP port (unicast or multicast group).
    Trigger: First packet received.
    Transition -> COLLECTING

COLLECTING:
    Entry: At least one packet received.
    State:
        - decoder: RaptorQDecoder (created from K in first packet)
        - received_count: number of symbols added to decoder
        - source_block_decoders: HashMap<u8, RaptorQDecoder> (for multi-block)
    Action (on each packet):
        - Parse packet header (source_block, ISI, K)
        - Get or create decoder for this source block
        - Add symbol to decoder: decoder.add_symbol(ISI, symbol_data)
        - Increment received_count
        - If received_count >= K: attempt decode
    Transition -> DECODING (when enough symbols collected)

DECODING:
    Entry: >= K symbols collected for at least one source block.
    Action:
        - Call decoder.decode(cx)
        - If success: recovered K source symbols
        - If failure (rare, ~1% at exactly K): stay in COLLECTING, wait for more
    Transition -> APPLYING (on successful decode)
    Transition -> COLLECTING (on decode failure, need more symbols)

APPLYING:
    Entry: All K source symbols recovered.
    Action:
        - Map ISI -> page number using the changeset metadata
        - For each recovered page:
            - Write page to local database at the correct page number
        - Flush WAL / checkpoint as needed
    Transition -> COMPLETE

COMPLETE:
    Entry: All pages applied.
    Action:
        - Optionally send ACK to sender (for unicast protocols)
        - Log replication metrics (symbols received, decode time, etc.)
    Transition -> LISTENING (ready for next changeset)
```

**Multicast Operation**

Fountain coding is uniquely suited to multicast replication. The sender emits
the same stream of encoding symbols to a multicast group address:

```
Sender:     [sym_0] [sym_1] [sym_2] ... [sym_K-1] [sym_K] [sym_K+1] ...
                |       |       |           |          |         |
Multicast:  ====|=======|=======|===========|==========|=========|======
                |       |       |           |          |         |
Receiver A: [sym_0] [  X  ] [sym_2] ... [sym_K-1] [sym_K] [  X    ] ...
Receiver B: [  X  ] [sym_1] [  X  ] ... [  X    ] [sym_K] [sym_K+1] ...
Receiver C: [sym_0] [sym_1] [sym_2] ... [sym_K-1] [  X  ] [  X    ] ...
```

Each receiver experiences different packet losses (marked X). But since
RaptorQ decoding works with ANY K' >= K symbols, each receiver independently
collects until it has enough and then decodes. No retransmission is needed.
No feedback channel from receiver to sender is needed.

For N receivers with independent packet loss rate p, the sender needs to
emit approximately K / (1 - p) symbols total. All N receivers decode
simultaneously from this single stream. Compare with TCP unicast, which
requires N separate streams, each requiring K / (1 - p) symbols plus
retransmission overhead from ACK/NACK handshakes.

**Bandwidth Analysis**

Let K = number of source symbols (pages), p = packet loss rate, N = number
of receivers.

```
Traditional TCP (per receiver):
    Expected transmissions: K / (1 - p) + retransmission_overhead
    For N receivers: N * K / (1 - p) * (1 + overhead)
    Total sender bandwidth: O(N * K / (1 - p))

Fountain-coded multicast:
    Sender emits: K * (1 + epsilon) / (1 - p) symbols, where epsilon ~ 0.02
    All N receivers decode from this single stream
    Total sender bandwidth: O(K / (1 - p))
    Bandwidth savings: factor of N

Example:
    K = 1000 pages, p = 5% loss, N = 10 receivers
    TCP: ~10 * 1000 / 0.95 * 1.1 ~ 11,579 transmissions from sender
    Fountain: ~1000 * 1.02 / 0.95 ~ 1,074 transmissions from sender
    Savings: 10.8x
```

**This is the killer feature for edge/IoT deployments** where network
reliability is poor. A sensor network can replicate its database to a
central server over lossy radio links with optimal bandwidth usage.

#### 3.4.3 Fountain-Coded Snapshot Shipping

**Problem:** Initializing a new replica requires transferring the entire
database. A 1GB database over a lossy link is painful with TCP.

**Solution:** The database snapshot is treated as a large source block and
fountain-coded:

```
Snapshot Transfer (P total pages):
  Partition into source blocks of up to 56,403 symbols each (RFC 6330 max)
  For each source block:
    Emit encoding symbols continuously
  Receiver:
    For each source block: collect until K' >= K, decode
  Result: Complete database reconstructed

Advantages:
  - No handshake or acknowledgment needed
  - Receiver can start receiving from any point in the stream
  - Multiple partial receives can be combined
  - Natural multicast: initialize many replicas simultaneously
```

**Source Block Partitioning Algorithm**

RFC 6330 limits each source block to K_max = 56,403 source symbols. For a
database with P pages (where P may exceed K_max), we must partition into
multiple source blocks:

```
partition_source_blocks(P: u32, page_size: u32) -> Vec<SourceBlock>:
// RFC 6330 §4.4.1 source block partitioning
    K_max = 56403
    T = page_size    // symbol size = page size

    if P <= K_max:
        // Single source block covers the entire database
        return [SourceBlock { index: 0, start_page: 1, num_pages: P }]

    // Multiple source blocks needed
    // Partition P pages into Z blocks as evenly as possible
    Z = ceil(P / K_max)
    // RFC 6330 partitioning: Z_L blocks of K_L symbols, Z_S blocks of K_S symbols
    K_L = ceil(P / Z)    // larger block size
    K_S = floor(P / Z)   // smaller block size
    Z_L = P - K_S * Z    // number of larger blocks
    Z_S = Z - Z_L        // number of smaller blocks

    blocks = []
    offset = 1    // page numbers are 1-based
    for i in 0..Z_L:
        blocks.append(SourceBlock { index: i, start_page: offset, num_pages: K_L })
        offset += K_L
    for i in Z_L..(Z_L + Z_S):
        blocks.append(SourceBlock { index: i, start_page: offset, num_pages: K_S })
        offset += K_S

    assert(offset == P + 1)
    return blocks
```

Example: A 1GB database with 4096-byte pages has P = 262,144 pages.

```
Z = ceil(262144 / 56403) = 5 source blocks
K_L = ceil(262144 / 5) = 52429
K_S = floor(262144 / 5) = 52428
Z_L = 262144 - 52428 * 5 = 4 blocks of 52429 pages
Z_S = 5 - 4 = 1 block of 52428 pages

Source blocks:
    Block 0: pages 1-52429      (52,429 pages, ~205 MB)
    Block 1: pages 52430-104858 (52,429 pages, ~205 MB)
    Block 2: pages 104859-157287 (52,429 pages, ~205 MB)
    Block 3: pages 157288-209716 (52,429 pages, ~205 MB)
    Block 4: pages 209717-262144 (52,428 pages, ~205 MB)
```

**Progressive Transfer: Receiver Can Start Using Partial Data**

Because source blocks are independent, the receiver can begin using data
from decoded blocks before the entire database is transferred:

```
progressive_receive():
    for each source block (in any order):
        collect symbols until K' >= K
        decode source block -> recovered pages
        write recovered pages to local database file
        // At this point, queries touching only these pages can execute
        // (read-only, since the database is still being populated)

    after all source blocks decoded:
        verify database integrity (PRAGMA integrity_check)
        mark replica as fully initialized
        enable read-write access
```

This is particularly valuable for large databases: a 1GB database partitioned
into 5 source blocks means the receiver has usable data after receiving just
20% of the total. For read-heavy workloads where the query working set may
be concentrated in a small region of the database, the receiver can answer
queries before the full transfer completes.

**Resume Protocol After Connection Loss**

Because fountain codes are rateless and stateless, resuming after a
connection loss requires no protocol negotiation:

```
resume_protocol():
    // Receiver state is just: for each source block, the set of received symbols
    // This state is persisted locally in a small metadata file:
    // resume_state.bin: [block_id(1B) | num_received(4B) | ISI_bitmap(variable)] per block

    on_connection_loss():
        persist resume_state to disk

    on_reconnect():
        load resume_state from disk
        for each incomplete source block:
            // Tell sender to continue from any ISI (sender doesn't care which)
            // Actually, sender doesn't need to know anything --
            // it just keeps emitting symbols, receiver ignores duplicates
            continue collecting symbols
            // Duplicates (same ISI received twice) are detected and discarded
            // by the decoder in O(1) via a hash set of received ISIs

    // The sender doesn't even need to know the receiver reconnected.
    // If the sender is continuously streaming (e.g., multicast), the receiver
    // simply starts collecting again from wherever the stream currently is.
```

This is fundamentally different from TCP-based transfer protocols, which
must negotiate sequence numbers, retransmit lost segments, and maintain
connection state. Fountain-coded transfer is inherently resumable with
zero overhead.

#### 3.4.4 MVCC Version Chain Compression

**Problem:** Version chains store full copies of each page version. For pages
where only a few bytes change per transaction, this wastes memory.

**Solution:** Store diffs as RaptorQ repair symbols:

```
Version chain for page P:
  V3 (newest): full page data (4096 bytes)
  V2: RaptorQ repair symbol set that, combined with V3's data,
      can reconstruct V2's full page
  V1: RaptorQ repair symbol set relative to V2

Reconstruction of V1:
  Start from V3 (full data)
  Apply V2's repair symbols to recover V2
  Apply V1's repair symbols to recover V1

Space savings:
  If delta between versions is D bytes out of 4096:
  Full copy: 4096 bytes per version
  RaptorQ delta: ~D bytes per version (information-theoretically optimal)
```

**Worked Example with Actual Byte Values**

Consider a B-tree leaf page (page 42) that undergoes three successive
modifications. The page starts as version V1, then V2 and V3 are created
by subsequent transactions:

```
V1 (TxnId=100): Original page (4096 bytes)
    Bytes 0-7:     [0D 00 00 00 03 0F E0 00]  (page header: leaf, 3 cells, cell offset)
    Bytes 8-99:    [cell pointer array + free block list]
    Bytes 100-999: [Cell 1: rowid=5, data="Alice"]
    Bytes 1000-1999: [Cell 2: rowid=10, data="Bob"]
    Bytes 2000-2999: [Cell 3: rowid=15, data="Charlie"]
    Bytes 3000-4095: [free space, all zeros]

V2 (TxnId=105): INSERT rowid=12
    Changes from V1:
    - Bytes 0-7 updated: cell count 3->4, cell content offset changes
    - Bytes 8-99: cell pointer array gains one entry
    - Bytes 2000-2099: Cell 3 shifted right by ~100 bytes
    - Bytes 1900-1999: New Cell 4 inserted (rowid=12, data="Dana")
    Delta size: ~300 bytes modified out of 4096

V3 (TxnId=110): UPDATE SET data="Robert" WHERE rowid=10
    Changes from V2:
    - Bytes 1000-1049: Cell 2 data changed from "Bob" to "Robert"
    - Bytes 0-7: cell content offset may change
    Delta size: ~60 bytes modified out of 4096
```

**Storage under full-copy version chain:**

```
V3: 4096 bytes (full page, always stored in full)
V2: 4096 bytes (full copy)
V1: 4096 bytes (full copy)
Total: 12,288 bytes for 3 versions
```

**Storage under RaptorQ delta compression:**

```
V3: 4096 bytes (full page)
V2 delta: RaptorQ encoding of (V2 XOR V3)
    V2 XOR V3 has ~60 non-zero bytes out of 4096
    This sparse vector can be represented as a repair symbol set
    Stored as: [delta_header(8B) | compressed_delta(~80B)]
    Total: ~88 bytes

V1 delta: RaptorQ encoding of (V1 XOR V2)
    V1 XOR V2 has ~300 non-zero bytes out of 4096
    Stored as: [delta_header(8B) | compressed_delta(~340B)]
    Total: ~348 bytes

Total: 4096 + 88 + 348 = 4,532 bytes for 3 versions
Savings: 63% reduction (4,532 vs 12,288)
```

**When to Use Delta vs Full Copy (Threshold Analysis)**

Delta compression is not always beneficial. When the delta is large (many
bytes changed), the overhead of the delta header plus the compressed delta
may approach or exceed the full page size. The decision threshold:

```
use_delta(old_page, new_page) -> bool:
    delta = old_page XOR new_page
    nonzero_bytes = count_nonzero(delta)

    // Fixed overhead: delta header (8 bytes) + RaptorQ metadata
    OVERHEAD = 16

    // RaptorQ delta size is approximately nonzero_bytes * 1.05
    // (the 5% accounts for RaptorQ encoding overhead for small symbol counts)
    estimated_delta_size = OVERHEAD + (nonzero_bytes as f64 * 1.05) as usize

    // Use delta if it saves at least 25% vs full copy
    // (the 25% threshold accounts for the CPU cost of delta reconstruction)
    return estimated_delta_size < page_size * 3 / 4
```

Typical thresholds for T = 4096:

```
| Workload                    | Avg bytes changed | Delta size | Use delta? |
|-----------------------------|-------------------|------------|------------|
| Single-row UPDATE (leaf)    | 20-100            | ~120       | Yes (97% savings) |
| INSERT into leaf page       | 100-500           | ~540       | Yes (87% savings) |
| B-tree split (interior)     | 2048 (half page)  | ~2160      | Yes (47% savings) |
| VACUUM (page rewrite)       | 4096 (full page)  | ~4320      | No (delta > page) |
| Bulk INSERT (new page)      | 4096 (full page)  | ~4320      | No |
```

**Compression Ratio Estimates for Different Workloads**

```
| Workload                          | Avg versions/page | Avg delta | Compression ratio |
|-----------------------------------|-------------------|-----------|-------------------|
| OLTP (single-row updates)         | 5-10              | 50 bytes  | 10-15x            |
| Mixed read-write web app          | 3-5               | 200 bytes | 4-6x              |
| Batch import (sequential inserts) | 2-3               | 1500 bytes | 1.5-2x           |
| Analytics (read-heavy, few writes)| 1-2               | N/A       | 1x (no versions)  |
```

This is particularly effective for B-tree interior pages where only child
pointers change during splits, and for leaf pages where insertions affect
only a portion of the page.

#### 3.4.5 Algebraic Write Merging Over GF(256)

**Problem:** When two transactions write different byte ranges of the same page,
standard MVCC must abort one (page-level conflict). But if the modifications
don't overlap, aborting is wasteful.

**Solution:** Exploit GF(256) linearity for non-overlapping modifications:

```
Page P (original): [AAAA|BBBB|CCCC|DDDD]
T1 modifies bytes 0-3:  [XXXX|BBBB|CCCC|DDDD]
T2 modifies bytes 8-11: [AAAA|BBBB|YYYY|DDDD]

Traditional MVCC: One must abort.

Algebraic merge:
  delta_T1 = T1_page XOR original = [XXXX^AAAA|0000|0000|0000]
  delta_T2 = T2_page XOR original = [0000|0000|YYYY^CCCC|0000]
  merged = original XOR delta_T1 XOR delta_T2
         = [XXXX|BBBB|YYYY|DDDD]

This is valid because XOR is GF(2) addition, and the deltas have
disjoint support (non-overlapping non-zero regions).
```

**Complete Mathematical Formalization in GF(2)**

Let page P be a vector in GF(2)^n where n = page_size * 8 (the page
represented as a bit vector). Let P_0 be the original page at the
snapshot point.

Transaction T1 produces page P_1 = P_0 + D_1, where D_1 = P_1 XOR P_0
is the delta (in GF(2) vector addition = XOR).

Transaction T2 produces page P_2 = P_0 + D_2, where D_2 = P_2 XOR P_0.

Define the **support** of a delta vector:
```
supp(D) = { i : D[i] != 0 } = { bit positions where D is non-zero }
```

**Theorem (Correctness of Algebraic Merge):**
If supp(D_1) and supp(D_2) are disjoint (supp(D_1) intersection supp(D_2) = empty set),
then the merged page:
```
P_merged = P_0 + D_1 + D_2 = P_0 XOR D_1 XOR D_2
```
is the unique page that incorporates both T1's changes and T2's changes,
and agrees with P_0 on all bytes that neither transaction modified.

**Proof:**

For any bit position i:
- Case 1: i not in supp(D_1) and i not in supp(D_2).
  Then D_1[i] = D_2[i] = 0, so P_merged[i] = P_0[i] + 0 + 0 = P_0[i].
  The merged page preserves the original value. Correct.

- Case 2: i in supp(D_1) and i not in supp(D_2).
  Then D_2[i] = 0, so P_merged[i] = P_0[i] + D_1[i] + 0 = P_1[i].
  The merged page has T1's value. Correct.

- Case 3: i not in supp(D_1) and i in supp(D_2).
  Then D_1[i] = 0, so P_merged[i] = P_0[i] + 0 + D_2[i] = P_2[i].
  The merged page has T2's value. Correct.

- Case 4: i in supp(D_1) and i in supp(D_2).
  This case cannot occur because the supports are disjoint.

Therefore P_merged = P_0 + D_1 + D_2 is correct for all bit positions. QED.

**Corollary (Byte-Level Sufficiency):**
Since page data is byte-addressed and all modifications are byte-aligned
(SQLite never modifies individual bits within a byte), it suffices to check
disjointness at byte granularity rather than bit granularity. Define:

```
byte_supp(D) = { j : D[j*8..(j+1)*8] != 0x00 } = { byte positions where D is non-zero }
```

If byte_supp(D_1) intersection byte_supp(D_2) = empty set, then
supp(D_1) intersection supp(D_2) = empty set, and the merge is valid.

**Proof of Correctness for Non-Overlapping Modifications**

The merge operation P_merged = P_0 XOR D_1 XOR D_2 is:
1. **Associative**: (P_0 XOR D_1) XOR D_2 = P_0 XOR (D_1 XOR D_2). Order
   of applying deltas does not matter.
2. **Commutative**: D_1 XOR D_2 = D_2 XOR D_1. T1 and T2's changes can be
   applied in either order.
3. **Idempotent (self-inverse)**: Applying the same delta twice cancels out
   (D XOR D = 0). This means the merge is reversible.
4. **Extends to N transactions**: For N transactions with pairwise disjoint
   byte supports, P_merged = P_0 XOR D_1 XOR D_2 XOR ... XOR D_N.

**Byte-Level Conflict Detection Algorithm**

```
can_algebraic_merge(
    T1: &Transaction,
    T2_committed: &CommitRecord,
    pgno: PageNumber,
    original: &PageData,
) -> bool:
    let page_T1 = T1.write_set[pgno].data
    let page_T2 = T2_committed.get_page_data(pgno)

    // Compute byte-level deltas
    let delta_T1 = xor_pages(page_T1, original)     // 4096 bytes
    let delta_T2 = xor_pages(page_T2, original)      // 4096 bytes

    // Check for overlapping non-zero bytes
    // This is SIMD-friendly: AND the deltas and check for any non-zero result
    for i in (0..page_size).step_by(8):
        let d1 = u64::from_le_bytes(delta_T1[i..i+8])
        let d2 = u64::from_le_bytes(delta_T2[i..i+8])
        // If both deltas have non-zero bits in the same u64 word,
        // check byte-level overlap
        if d1 != 0 && d2 != 0:
            // Check each byte within this 8-byte word
            for j in 0..8:
                if delta_T1[i+j] != 0 && delta_T2[i+j] != 0:
                    return false    // Byte-level conflict detected!
    return true    // No overlap, merge is safe

perform_algebraic_merge(
    T1: &mut Transaction,
    T2_committed: &CommitRecord,
    pgno: PageNumber,
    original: &PageData,
):
    let page_T1 = T1.write_set[pgno].data
    let page_T2 = T2_committed.get_page_data(pgno)

    // Compute deltas
    let delta_T1 = xor_pages(page_T1, original)
    let delta_T2 = xor_pages(page_T2, original)

    // Merge: original XOR delta_T1 XOR delta_T2
    // Equivalently: page_T1 XOR delta_T2  (since page_T1 = original XOR delta_T1)
    let merged = xor_pages(page_T1, delta_T2)

    // Update T1's write set with the merged page
    T1.write_set[pgno].data = merged
```

**Performance Analysis: When Merging Saves vs Just Aborting**

The algebraic merge check has a cost:

```
Cost of merge check:
    - Compute delta_T1: page_size / 8 = 512 XOR operations (u64)
    - Compute delta_T2: 512 XOR operations
    - Check overlap: 512 AND + compare operations (worst case)
    - Total: ~1536 u64 operations = ~192 cycles on modern CPU
    - Wall time: ~50 nanoseconds

Cost of successful merge:
    - Merge check: ~50 ns
    - Compute merged page: 512 XOR operations = ~64 cycles = ~20 ns
    - Total: ~70 nanoseconds

Cost of abort + retry:
    - Abort current transaction: release locks, discard write set (~1 microsecond)
    - Application-level retry: re-execute entire transaction (~100 microseconds to ~10 ms)
    - Total: 100x to 100,000x more expensive than merge
```

Therefore, algebraic merging is always worth attempting when a page-level
conflict is detected. The merge check is so cheap (50ns) that even if it
fails 99% of the time, the 1% of successful merges easily justify the cost.

In practice, for B-tree workloads:
- Interior page conflicts (two child pointers updated): ~70% mergeable
  (different child pointers in different regions of the page)
- Leaf page conflicts (two rows in same page): ~40% mergeable
  (depends on whether the cells are in different regions)
- Overall false conflict reduction: estimated 30-50% of page-level
  conflicts are resolved by algebraic merging

**Detection:** At commit validation, if a page conflict is detected, check
whether the two write sets have disjoint byte-level modifications. If yes,
merge algebraically instead of aborting. This turns page-level conflicts
into byte-level conflicts, dramatically reducing false aborts.

**Caveat:** This only works for truly independent modifications. If both
transactions read the overlapping region before writing, write skew applies
and the merge may be incorrect. The SSI layer (Section 2.4, Layer 2) catches
this: rw-antidependency tracking detects the read-before-write pattern and
aborts the transaction that would produce an anomaly. Algebraic merging is
gated behind a PRAGMA: `PRAGMA raptorq_write_merge = ON`.

#### 3.4.6 Erasure-Coded Page Storage

For maximum durability, database pages themselves can be stored with redundancy:

```
Page group (G pages):
  RaptorQ-encode G source pages into G + R symbols
  Store all G + R symbols across storage

On read:
  Read G symbols (prefer source symbols for zero-decode overhead)
  If any corrupted: decode from remaining symbols

Effect: Tolerates up to R corrupted pages per group
```

**Page Group Partitioning**

The database is divided into page groups. The partitioning strategy must
balance several concerns:
- Group size G determines the granularity of redundancy (larger G = more
  efficient encoding but larger blast radius for correlated corruption)
- Groups should align with B-tree structure for locality
- The first page (database header) requires special handling

```
partition_page_groups(db_size_pages: u32) -> Vec<PageGroup>:
    G = 64    // Default group size: 64 pages = 256KB per group
    R = 4     // Default repair symbols per group: 4 pages = 16KB overhead

    groups = []
    pgno = 1    // pages are 1-based

    // Special group for the database header page
    // (page 1 is critical; give it extra redundancy)
    groups.append(PageGroup {
        start: 1,
        size: 1,
        repair: 4,    // 4 repair symbols for just 1 page = 400% redundancy
    })
    pgno = 2

    // Group remaining pages in chunks of G
    while pgno <= db_size_pages:
        remaining = db_size_pages - pgno + 1
        group_size = min(G, remaining)
        groups.append(PageGroup {
            start: pgno,
            size: group_size,
            repair: R,
        })
        pgno += group_size

    return groups
```

The repair symbols for each group are stored in a dedicated region of the
database file, after the main page area. The file layout becomes:

```
Database File Layout (with erasure coding):
    Offset 0:                   Database header (page 1, 4096 bytes)
    Offset page_size:           Page 2
    ...
    Offset (P-1)*page_size:     Page P
    Offset P*page_size:         Repair region header (4096 bytes)
    Offset (P+1)*page_size:     Repair symbols for group 0
    ...
    Offset (P+1+R0)*page_size:  Repair symbols for group 1
    ...
```

**Read Path with On-the-Fly Repair**

The read path is modified to detect and repair corrupted pages transparently:

```
read_page_with_repair(pgno: PageNumber) -> Result<PageData>:
    // Step 1: Read the page directly (fast path, no overhead)
    page = read_raw_page(pgno)
    checksum = compute_xxhash3(page)

    if checksum == stored_checksum(pgno):
        return Ok(page)    // Page is intact, zero overhead

    // Step 2: Page is corrupted. Attempt on-the-fly repair.
    group = find_page_group(pgno)

    // Read all pages in the group + repair symbols
    available_symbols = []
    for pg in group.start..(group.start + group.size):
        if pg == pgno:
            continue    // Skip the corrupted page
        page_data = read_raw_page(pg)
        if verify_checksum(pg, page_data):
            available_symbols.append((pg - group.start, page_data))    // ISI = offset within group

    // Read repair symbols for this group
    for r in 0..group.repair:
        repair_data = read_repair_symbol(group, r)
        if verify_checksum_repair(group, r, repair_data):
            available_symbols.append((group.size + r, repair_data))    // ISI = G + r

    if available_symbols.len() >= group.size:
        // Enough symbols to decode
        decoder = RaptorQDecoder::new(group.size, page_size)
        for (isi, data) in available_symbols:
            decoder.add_symbol(isi, data)
        recovered = decoder.decode()?
        // Extract the corrupted page from recovered data
        repaired_page = recovered[pgno - group.start]

        // Write back the repaired page (self-healing)
        write_raw_page(pgno, repaired_page)
        update_checksum(pgno, compute_xxhash3(repaired_page))

        return Ok(repaired_page)
    else:
        return Err(SQLITE_CORRUPT)    // Unrecoverable: too many corrupted pages in group
```

**Interaction with B-tree Page Types**

Different B-tree page types have different corruption characteristics and
repair priorities:

```
| Page Type              | Corruption Impact | Repair Priority | Notes |
|------------------------|-------------------|-----------------|-------|
| Interior table (0x05)  | Lose subtree access | HIGH         | Can be rebuilt from leaves in theory |
| Leaf table (0x0D)      | Lose row data      | CRITICAL      | Contains actual user data |
| Interior index (0x02)  | Lose index subtree | MEDIUM        | Rebuildable via REINDEX |
| Leaf index (0x0A)      | Lose index entries | MEDIUM        | Rebuildable via REINDEX |
| Overflow page          | Lose large values  | HIGH          | Part of a chain; one loss breaks chain |
| Freelist trunk/leaf    | Lose free pages    | LOW           | VACUUM can rebuild |
| Pointer map (auto-vac) | Lose page mapping  | HIGH          | Needed for auto-vacuum |
```

Page grouping should ideally keep related pages together (e.g., a parent
interior page and its child leaves in the same group) so that correlated
corruption (e.g., a bad disk sector affecting contiguous pages) is more
likely to be repairable. However, this creates a tension: correlated
corruption within a group is the worst case for repair (all corrupted
pages might be in the same group). The default grouping by page number
(sequential groups of 64) is a reasonable compromise that works well
for the common case of random single-page corruption.

For maximum resilience, a future enhancement could interleave group
membership (page i belongs to group i mod Z), ensuring that contiguous
disk corruption distributes across multiple groups. This is analogous to
RAID striping and would be configurable via PRAGMA.

This transforms the database file from "one bit flip = SQLITE_CORRUPT" to
"R bit flips per group = automatically recovered." Combined with the
self-healing WAL, this creates defense in depth where data corruption
becomes a mathematical near-impossibility.

### 3.5 ECS: The Erasure-Coded Stream Substrate

ECS is the universal persistence layer for Native mode. Every durable object
in FrankenSQLite -- commit capsules, page snapshots, WAL segments, index
checkpoints, schema snapshots -- is stored as an ECS object. ECS provides
content addressing, self-describing encoding, deterministic repair symbol
generation, and rebuildable indexes.

#### 3.5.1 ObjectId: Content-Addressed Identity

Every ECS object is identified by its ObjectId:

```
ObjectId := BLAKE3(canonical_encoding(object))    -- 32 bytes
```

**Canonical encoding rules:**
- All integers are little-endian (native x86/ARM byte order for zero-cost
  decode on the dominant architectures).
- All variable-length fields are prefixed with a `u32` byte count.
- Struct fields are serialized in declaration order, no padding, no alignment
  holes.
- Enums use a `u8` discriminant followed by the variant's fields.
- Floating-point values use IEEE 754 binary64 representation.
- Strings are UTF-8 bytes (no null terminator).
- Optional fields use `0x00` (absent) or `0x01` + value (present).

This encoding is deterministic: the same logical object always produces the
same byte sequence and therefore the same ObjectId. There is no ambiguity,
no ordering sensitivity, no hash randomization.

**ObjectId properties:**
- Immutable: once created, an ObjectId never changes. Objects are
  write-once-read-many.
- Content-addressed: identical objects have identical ObjectIds. Deduplication
  is automatic.
- Collision-resistant: BLAKE3 provides 256-bit security. Probability of
  collision is `2^-128` even after `2^64` objects.

#### 3.5.2 Symbol Record Envelope

Every ECS object is stored as one or more **symbol records**. A symbol
record is the atomic unit of physical storage -- the smallest thing that
can be read, written, verified, and transmitted.

```
SymbolRecord := {
    magic       : [u8; 4],      -- 0x46 0x53 0x45 0x43 ("FSEC")
    version     : u8,           -- envelope version (1)
    object_id   : [u8; 32],     -- BLAKE3 hash of the logical object
    oti         : OTI,          -- RaptorQ Object Transmission Information
    esi         : u32,          -- Encoding Symbol Identifier (which symbol this is)
    symbol_size : u32,          -- T: symbol size in bytes
    symbol_data : [u8; T],      -- the actual RaptorQ encoding symbol
    checksum    : u64,          -- xxhash3 of all preceding fields
}

OTI := {
    F  : u64,       -- transfer length (original object size in bytes)
    Al : u16,       -- symbol alignment (always 4 for FrankenSQLite)
    T  : u16,       -- symbol size (derived from F, Al, and target symbol count)
    Z  : u32,       -- number of source blocks
    N  : u32,       -- number of sub-blocks per source block
}
```

**Self-describing property:** A symbol record contains everything needed to
decode it: the ObjectId identifies which object this symbol belongs to, the
OTI provides the RaptorQ parameters, and the ESI identifies which encoding
symbol this is. A decoder collecting K' symbols with the same ObjectId can
reconstruct the original object without any external metadata.

**Symbol record sizing:** For a given object, the symbol size `T` is chosen
based on the object type:
- **CommitCapsules:** Small symbols (T ≈ 256 bytes) for low-latency commit.
  Typical capsule is 1-4 KB, encoded as 4-16 source symbols + repair.
- **Page snapshots:** T = page_size (4096 bytes). One source symbol per page.
- **Index checkpoints:** Large symbols (T ≈ 4096-16384 bytes) for throughput.
- **WAL segments:** T = WAL frame size for natural alignment.

#### 3.5.3 Deterministic Repair Symbol Generation

Given an ECS object and a repair symbol count `R`, the set of repair symbols
is deterministic: the same object and same `R` always produce the same repair
symbols. This enables:

1. **Verification without the original object:** Given the ObjectId and repair
   symbols, any node can verify that the repair symbols are valid by
   re-encoding from the source symbols.
2. **Incremental repair:** If a storage node discovers corruption, it can
   request specific ESIs from peers and verify them independently.
3. **Idempotent writes:** Writing the same repair symbols twice has no effect.

The repair symbol budget is controlled per-object-type:
```
PRAGMA raptorq_overhead = <percent>    -- default: 20%
```

This means: for every 100 source symbols, generate 20 repair symbols. The
overhead-to-failure-tolerance relationship is approximately linear: 20%
overhead tolerates ~20% symbol loss.

#### 3.5.4 Local Physical Layout (Native Mode)

In Native mode, the database directory has the following layout:

```
foo.db.fsqlite/
├── ecs/
│   ├── objects/          -- symbol records, organized by ObjectId prefix
│   │   ├── 00/           -- first byte of ObjectId
│   │   │   ├── 00a1b2...rec  -- symbol records for this object
│   │   │   └── 00c3d4...rec
│   │   ├── 01/
│   │   │   └── ...
│   │   └── ff/
│   ├── commit_stream/    -- append-only sequence of CommitMarker records
│   │   └── stream.log    -- CommitMarkers in commit_seq order
│   └── manifest.root     -- RootManifest (tiny, mutable, the ONE mutable thing)
├── cache/                -- rebuildable derived state (NOT source of truth)
│   ├── btree.cache       -- materialized B-tree pages (hot set)
│   ├── index.cache       -- secondary index pages
│   └── schema.cache      -- parsed schema
└── compat/               -- optional compatibility export
    ├── foo.db            -- standard SQLite database file
    └── foo.db-wal        -- standard WAL (if compat checkpoint active)
```

**Key invariants:**
- `ecs/` is the source of truth. Everything in `cache/` is rebuildable from
  `ecs/`. Deleting `cache/` is always safe (costs a rebuild).
- `compat/` is an export target for compatibility mode. It is NOT the source
  of truth in Native mode.
- `manifest.root` is the ONLY mutable file. It is a single `RootManifest`
  record, updated atomically (write-to-temp + rename).

#### 3.5.5 RootManifest: Bootstrap

The RootManifest is the bootstrap entry point for the entire database:

```
RootManifest := {
    magic           : [u8; 8],     -- "FSQLROOT"
    version         : u32,         -- manifest version
    database_name   : String,      -- human-readable name
    current_commit  : ObjectId,    -- ObjectId of the latest CommitMarker
    commit_seq      : u64,         -- latest commit sequence number
    schema_snapshot : ObjectId,    -- ObjectId of current schema ECS object
    checkpoint_base : ObjectId,    -- ObjectId of last full checkpoint
    gc_horizon      : TxnId,      -- oldest TxnId that may still be needed
    created_at      : u64,         -- Unix timestamp
    updated_at      : u64,         -- Unix timestamp
    checksum        : u64,         -- xxhash3 of all preceding fields
}
```

**Bootstrap sequence:**
1. Read `manifest.root`. Verify magic and checksum.
2. Fetch `current_commit` → walk CommitMarker chain to find committed state.
3. Fetch `schema_snapshot` → reconstruct schema cache.
4. Fetch `checkpoint_base` → populate B-tree page cache for hot pages.
5. Database is open and ready for queries.

If `manifest.root` is corrupted, the database can be rebuilt by scanning
`ecs/commit_stream/stream.log` and replaying CommitMarkers from the
beginning. This is O(commit history) but always works.

#### 3.5.6 Inter-Object Coding (Replication Optimization)

For replication, ECS objects can be coded across objects using inter-object
RaptorQ encoding. This allows a replica to reconstruct missing objects from
a subset of symbols spanning multiple objects:

```
Inter-object coding group:
    Objects O1, O2, ..., Ok share a coding group
    RaptorQ-encode the concatenation of their canonical encodings
    Transmit encoding symbols with group metadata

Receiver:
    Collect symbols from any subset of the group
    Decode to recover all objects in the group
```

This is particularly effective for replication catch-up: a lagging replica
can request "all commits since sequence N" as a single coded group, and
recover even if some symbols are lost in transit (UDP multicast).

---

## 4. Asupersync Deep Integration

Asupersync is not just "a blocking pool and some channels." It is a
formally-specified async runtime with capabilities that map precisely to
FrankenSQLite's needs:

### 4.1 Cx (Capability Context) -- Everywhere

Every FrankenSQLite operation accepts `&Cx`. This enables:

- **Cooperative cancellation**: Long-running queries check `cx.is_cancelled()`
  at VDBE instruction boundaries. `SQLITE_INTERRUPT` on cancellation.
- **Deadline propagation**: `cx.with_deadline(duration)` automatically cancels
  operations that exceed their time budget.
- **Compile-time capability narrowing**: Functions that should not perform I/O
  accept `&Cx<NoIo>`. Functions that should not allocate accept `&Cx<NoBudget>`.
  The type system prevents capability escalation.

**Integration pattern:**
```rust
fn execute_query(cx: &Cx, stmt: &PreparedStatement) -> Result<Rows> {
    for opcode in &stmt.program {
        cx.check_cancelled()?;  // cooperatively yield to cancellation
        dispatch_opcode(cx, opcode)?;
    }
}
```

**Capability narrowing through the call stack:**

Asupersync's `Cx` type carries a phantom type parameter `Caps` that encodes
which capabilities are available. The capability set is a fixed-width vector
of booleans `[SPAWN, TIME, RANDOM, IO, REMOTE]` represented via const generics
as `CapSet<SPAWN, TIME, RANDOM, IO, REMOTE>`. The subset relation is the
pointwise `<=` ordering: `false <= false`, `false <= true`, `true <= true`.
Narrowing (dropping capabilities) always succeeds; widening (gaining
capabilities) is a compile-time error because the missing impl
`(Bit<true>, Bit<false>)` prevents it.

This means FrankenSQLite can express precise contracts at every layer boundary:

```rust
use asupersync::cx::{Cx, cap};

// Type aliases for FrankenSQLite-specific capability profiles
type FullCaps = cap::All;                                   // Connection level: everything
type StorageCaps = cap::CapSet<false, true, false, true, false>;  // VFS: time + I/O, no spawn/remote
type ComputeCaps = cap::CapSet<false, false, false, false, false>; // Parser/planner: pure computation

/// Connection::execute_query has full capabilities.
/// It is the outermost entry point from the public API.
pub fn execute_query(cx: &Cx<FullCaps>, sql: &str) -> Result<Rows> {
    let ast = parse_sql(cx.narrow(), sql)?;          // narrow to ComputeCaps
    let plan = plan_query(cx.narrow(), &ast)?;        // narrow to ComputeCaps
    let program = codegen(cx.narrow(), &plan)?;       // narrow to ComputeCaps
    execute_program(cx, &program)                     // full caps: needs I/O for page reads
}

/// The parser accepts only ComputeCaps. It cannot perform I/O.
/// This is a compile-time guarantee, not a runtime check.
fn parse_sql(cx: &Cx<ComputeCaps>, sql: &str) -> Result<Ast> {
    cx.check_cancelled()?;  // cancellation is always available
    // cx.blocking_io(...)  -- COMPILE ERROR: ComputeCaps lacks IO
    let lexer = Lexer::new(sql);
    Parser::parse(cx, lexer)
}

/// The VFS layer accepts StorageCaps: it can do I/O and timers
/// but cannot spawn tasks or make remote calls.
fn read_page(cx: &Cx<StorageCaps>, file: &impl VfsFile, pgno: PageNumber) -> Result<PageData> {
    cx.check_cancelled()?;
    let offset = u64::from(pgno.get() - 1) * u64::from(page_size);
    let mut buf = vec![0u8; page_size as usize];
    file.read_at(cx, offset, &mut buf)?;
    Ok(PageData::from(buf))
}
```

**Cx flows through the full call stack:**

```
Connection::execute(cx: &Cx<All>)
  -> VDBE::run(cx: &Cx<All>)
    -> BtreeCursor::move_to(cx: &Cx<StorageCaps>)
      -> MvccPager::get_page(cx: &Cx<StorageCaps>)
        -> ArcCache::fetch(cx: &Cx<StorageCaps>)
          -> VfsFile::read_at(cx: &Cx<StorageCaps>)
            -> BlockingPool::spawn_blocking(cx, || { pread64(...) })
```

At each level, capabilities can only be narrowed, never widened. The VDBE
has full capabilities (it orchestrates I/O for page reads). When it calls
down to the pager, it narrows to `StorageCaps`. When the parser is invoked
(a pure computation), it narrows to `ComputeCaps`. This means a bug in the
parser that accidentally tries to do I/O is caught at compile time, not at
runtime.

### 4.2 Lab Reactor -- Deterministic Testing

Asupersync's lab reactor provides a fully deterministic virtual I/O environment.
No real syscalls, no real time, no real threads -- everything is simulated with
controllable scheduling.

**For MVCC testing, this is transformative:**
- Run 100 concurrent transactions with deterministic interleaving
- Reproduce any race condition by replaying the same schedule
- Inject faults (disk failure, partial write) at precise points
- Test crash recovery without actually crashing

**Integration:** All VFS operations in test mode route through the lab reactor.
The MVCC stress tests use the lab reactor for reproducible concurrency testing
rather than hoping real-thread interleavings happen to cover edge cases.

**Complete test scenario: verifying snapshot isolation under deterministic scheduling:**

```rust
#[test]
fn snapshot_isolation_holds_under_specific_interleaving() {
    let config = LabConfig::new()
        .seed(0xDEAD_BEEF)           // deterministic scheduling seed
        .max_steps(100_000)
        .worker_count(4);

    let mut lab = LabRuntime::new(config);

    lab.run(|cx| async move {
        let db = Database::open_in_memory(cx).await.unwrap();
        db.execute(cx, "CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER)").await.unwrap();
        db.execute(cx, "INSERT INTO t VALUES(1, 100)").await.unwrap();
        db.execute(cx, "INSERT INTO t VALUES(2, 200)").await.unwrap();

        // T1: long-running reader -- takes a snapshot, then reads after T2 commits
        let db1 = db.clone();
        let t1 = cx.spawn("reader", async move |cx| {
            let txn = db1.begin_concurrent(cx).await.unwrap();
            // Read row 1 -- sees original value
            let val1 = txn.query_one(cx, "SELECT val FROM t WHERE id=1").await.unwrap();
            assert_eq!(val1, 100);

            // Yield -- this is where the deterministic scheduler allows T2 to run
            cx.yield_now().await;

            // Read row 1 again -- MUST still see 100 (snapshot isolation)
            // even though T2 has committed val=999 by now
            let val1_again = txn.query_one(cx, "SELECT val FROM t WHERE id=1").await.unwrap();
            assert_eq!(val1_again, 100, "snapshot isolation violated!");

            txn.commit(cx).await.unwrap();
        });

        // T2: writer -- modifies row 1 and commits while T1 is active
        let db2 = db.clone();
        let t2 = cx.spawn("writer", async move |cx| {
            let txn = db2.begin_concurrent(cx).await.unwrap();
            txn.execute(cx, "UPDATE t SET val=999 WHERE id=1").await.unwrap();
            txn.commit(cx).await.unwrap();
        });

        t1.await.unwrap();
        t2.await.unwrap();
    });

    let report = lab.report();
    assert!(report.invariant_violations.is_empty());
}
```

**Injecting disk failure at a specific write offset:**

```rust
#[test]
fn wal_survives_torn_write_at_frame_3() {
    let config = LabConfig::new().seed(42).max_steps(50_000);
    let mut lab = LabRuntime::new(config);

    lab.run(|cx| async move {
        let vfs = LabVfs::new(cx);

        // Inject a torn write: after writing exactly 2 complete WAL frames
        // (header + 2 * (frame_header + page_data)), corrupt the next write
        let wal_header_size = 32;
        let frame_size = 24 + 4096;  // frame header + page data
        let corruption_offset = wal_header_size + 2 * frame_size;

        vfs.inject_fault(FaultSpec {
            file_pattern: "*.wal",
            trigger: FaultTrigger::WriteAtOffset(corruption_offset),
            effect: FaultEffect::TornWrite { valid_bytes: 17 },  // partial frame header
        });

        let db = Database::open(cx, &vfs, "test.db").await.unwrap();
        // ... perform a 5-page transaction that writes 5 WAL frames ...
        // Frame 3 will be torn. With RaptorQ repair symbols (R=2),
        // the commit should be recoverable.

        // Simulate crash
        drop(db);

        // Recovery
        let db = Database::open(cx, &vfs, "test.db").await.unwrap();
        db.execute(cx, "PRAGMA integrity_check").await.unwrap();
        // All 5 pages should be recovered via RaptorQ decoding
    });
}
```

**Simulating power loss during WAL commit:**

```rust
#[test]
fn power_loss_during_wal_commit_preserves_atomicity() {
    let config = LabConfig::new().seed(7777).max_steps(50_000);
    let mut lab = LabRuntime::new(config);

    lab.run(|cx| async move {
        let vfs = LabVfs::new(cx);

        // Inject power loss after fsync of WAL frames but before
        // writing the commit record
        vfs.inject_fault(FaultSpec {
            file_pattern: "*.wal",
            trigger: FaultTrigger::AfterNthSync(1),  // after first fsync (frames)
            effect: FaultEffect::ProcessCrash,         // simulated power loss
        });

        let db = Database::open(cx, &vfs, "test.db").await.unwrap();
        db.execute(cx, "CREATE TABLE t(x INTEGER)").await.unwrap();
        db.execute(cx, "INSERT INTO t VALUES(1)").await.unwrap();

        // This transaction's commit will be interrupted
        let result = db.execute(cx, "INSERT INTO t VALUES(2)").await;
        // The process "crashed" -- result is irrelevant

        // Recovery: reopen database. The uncommitted transaction must not
        // be visible (atomicity guarantee).
        let db = Database::open(cx, &vfs, "test.db").await.unwrap();
        let count: i64 = db.query_one(cx, "SELECT count(*) FROM t").await.unwrap();
        assert_eq!(count, 1, "uncommitted transaction must not be visible after crash");
    });
}
```

### 4.3 E-Processes -- Anytime-Valid Invariant Monitoring

E-processes (based on Ville's inequality) provide statistically rigorous
runtime monitoring that can be checked at ANY point during execution, not just
at the end of a test.

**For MVCC, monitor these invariants as e-processes:**
- **INV-1 (Monotonicity)**: TxnId is strictly increasing
- **INV-2 (Lock Exclusivity)**: No two active transactions hold the same page lock
- **INV-3 (Version Chain Order)**: Versions are ordered by descending TxnId
- **INV-4 (Write Set Consistency)**: Write set only contains locked pages
- **INV-6 (Commit Atomicity)**: Committed transaction's pages all become visible

If an e-process detects a violation, it provides a **proof certificate** that
the invariant was violated, including the exact sequence of operations that
caused it. This is not a test that passes or fails -- it's a continuously
running formal monitor.

**Formal definition of an e-process:**

An **e-process** `(E_t)_{t >= 0}` is a sequence of random variables adapted
to a filtration `(F_t)` such that:

1. `E_0 = 1` (starts at one)
2. `E_t >= 0` for all `t` (non-negative)
3. `E[E_t | F_{t-1}] <= E_{t-1}` (supermartingale under the null hypothesis H_0)

The null hypothesis H_0 asserts that the invariant holds (violation probability
is at most `p_0`, typically 0.001). Each observation `X_t` is binary: 1 if a
violation is detected, 0 otherwise.

**Key property (Ville's inequality):** For any stopping time `tau` and
significance level `alpha`:

```
P_{H_0}(exists t : E_t >= 1/alpha) <= alpha
```

This means you can **peek at any time** and reject H_0 (conclude the invariant
is systematically violated) if `E_t >= 1/alpha`, without inflating the type-I
error rate. No correction for multiple testing over time is needed. This is
the fundamental advantage over classical hypothesis testing.

**The betting martingale update rule:**

```
E_t = E_{t-1} * (1 + lambda * (X_t - p_0))
```

where:
- `lambda` is the bet size, constrained to `(-1/(1-p_0), 1/p_0)` for non-negativity
- `X_t` is the observation (1 = violation, 0 = no violation)
- `p_0` is the null hypothesis violation rate (e.g., 0.001)

Under H_0, `E[X_t] = p_0`, so `E[E_t | E_{t-1}] = E_{t-1}` (martingale).
Under the alternative H_1 (actual violation rate `p_1 > p_0`), the e-process
grows exponentially at rate `KL(p_1 || p_0)` per observation, where KL is the
Kullback-Leibler divergence.

**Concrete e-process definitions for MVCC invariants:**

```rust
use asupersync::lab::oracle::eprocess::{EProcess, EProcessConfig};

/// Create e-processes for all MVCC invariants.
fn create_mvcc_monitors() -> Vec<EProcess> {
    let config = EProcessConfig {
        p0: 0.001,       // null: violation rate <= 0.1%
        lambda: 0.5,     // moderate bet
        alpha: 0.05,     // reject at 5% significance
        max_evalue: 1e15, // overflow guard
    };

    vec![
        EProcess::new("INV-1: TxnId Monotonicity", config.clone()),
        EProcess::new("INV-2: Lock Exclusivity", config.clone()),
        EProcess::new("INV-3: Version Chain Order", config.clone()),
        EProcess::new("INV-4: Write Set Consistency", config.clone()),
        EProcess::new("INV-5: Snapshot Stability", config.clone()),
        EProcess::new("INV-6: Commit Atomicity", config.clone()),
        EProcess::new("INV-7: Serialized Mode Exclusivity", config.clone()),
    ]
}
```

**Example: the Lock Exclusivity e-process (INV-2):**

The Lock Exclusivity invariant states: for any page P, at most one active
transaction holds a lock. We define the observation function:

```rust
/// Check INV-2 at the current instant.
/// Returns 1.0 (violation) if any page has two holders, 0.0 otherwise.
fn observe_lock_exclusivity(lock_table: &PageLockTable) -> f64 {
    // The lock table maps PageNumber -> TxnId.
    // By construction, HashMap allows only one value per key.
    // But we additionally verify against the per-transaction lock sets:
    let mut page_holders: HashMap<PageNumber, Vec<TxnId>> = HashMap::new();
    for txn in active_transactions.values() {
        if txn.state == TxnState::Active {
            for &pgno in &txn.page_locks {
                page_holders.entry(pgno).or_default().push(txn.txn_id);
            }
        }
    }
    for (pgno, holders) in &page_holders {
        if holders.len() > 1 {
            return 1.0;  // VIOLATION
        }
    }
    0.0  // no violation
}

// In the test loop, after each operation:
let observation = observe_lock_exclusivity(&lock_table);
inv2_eprocess.observe(observation);
if inv2_eprocess.current_evalue().rejects_at(0.05) {
    panic!(
        "INV-2 violated: e-value {} >= threshold {} after {} observations",
        inv2_eprocess.current_evalue().value,
        inv2_eprocess.config.threshold(),
        inv2_eprocess.observations,
    );
}
```

After 1000 operations with no violations, `E_1000 ~ 1.0` (fluctuates around 1
due to the martingale property). If a bug causes even a single violation, the
e-value jumps by a factor of `(1 + 0.5 * (1 - 0.001)) ~ 1.5` and continues
growing with each subsequent violation, rapidly crossing the rejection
threshold of `1/0.05 = 20`.

### 4.4 Mazurkiewicz Trace Monoid -- Systematic Interleaving

Standard concurrency testing relies on random interleaving, which may miss
rare but critical orderings. Asupersync's Mazurkiewicz trace implementation
systematically explores ALL distinct interleavings (up to commutativity of
independent operations).

**For MVCC:** Given a scenario with N transactions each performing M operations,
the trace monoid enumerates all non-equivalent orderings and verifies that:
- Snapshot isolation holds for every ordering
- First-committer-wins correctly identifies conflicts
- GC never reclaims a version needed by an active transaction

This provides exhaustive coverage that random testing cannot match.

**Formal definition:**

A **trace monoid** `M(Sigma, I)` is defined over:
- An **alphabet** `Sigma` of actions (e.g., `read_page(T1, P1)`, `write_page(T2, P3)`)
- A symmetric, irreflexive **independence relation** `I` on `Sigma x Sigma`

Two actions `a, b` are **independent** (written `(a, b) in I`) if swapping
their order does not change observable behavior. Two words (sequences of
actions) `w_1` and `w_2` are **trace-equivalent** (written `w_1 =_I w_2`)
if one can be transformed into the other by repeatedly swapping adjacent
independent actions.

The trace monoid is the quotient `M(Sigma, I) = Sigma* / =_I`.

**Independence relation for MVCC operations:**

| Action A | Action B | Independent? | Reason |
|----------|----------|-------------|--------|
| `read_page(T1, P1)` | `read_page(T2, P2)` | Yes, if P1 != P2 | Different pages, read-read |
| `read_page(T1, P1)` | `read_page(T2, P1)` | Yes | Read-read on same page (MVCC: each sees own snapshot) |
| `read_page(T1, P1)` | `write_page(T2, P1)` | **No** | T2's write might change what T1 sees (dependent) |
| `write_page(T1, P1)` | `write_page(T2, P2)` | Yes, if P1 != P2 | Different pages |
| `write_page(T1, P1)` | `write_page(T2, P1)` | **No** | Same page conflict |
| `commit(T1)` | `commit(T2)` | **No** | Serialized through write coordinator |
| `begin(T1)` | `begin(T2)` | **No** | Snapshot capture depends on ordering |
| `read_page(T1, P1)` | `commit(T2)` | **No** if P1 in T2.write_set | Commit publishes versions |

**How the trace monoid quotients out commutative reorderings:**

Given a concrete execution trace (a total order of events), the trace monoid
identifies which events could have been reordered without affecting the outcome.
Two traces that differ only in the order of independent events belong to the
same equivalence class. Asupersync computes the **Foata normal form** -- a
canonical representative where events are organized into layers of mutually
independent events, with a deterministic sort within each layer.

**Concrete example: 2 transactions, 3 operations each:**

Setup: T1 reads P1, writes P2, commits. T2 reads P3, writes P4, commits.
Pages are all distinct, so T1 and T2's reads/writes are independent of each
other (only commits are dependent due to coordinator serialization).

```
Alphabet Sigma = {
    a1 = read(T1, P1),   a2 = write(T1, P2),   a3 = commit(T1),
    b1 = read(T2, P3),   b2 = write(T2, P4),   b3 = commit(T2)
}

Independence relation I (symmetric pairs):
    (a1, b1), (a1, b2),          -- T1's read of P1 independent of T2's ops on P3,P4
    (a2, b1), (a2, b2),          -- T1's write of P2 independent of T2's ops on P3,P4
    (b1, a1), (b1, a2),          -- (symmetric)
    (b2, a1), (b2, a2),          -- (symmetric)

Dependent pairs (NOT in I):
    (a3, b3)                     -- commits are serialized
    (a1, a2), (a2, a3), (a1, a3) -- same-transaction ordering preserved
    (b1, b2), (b2, b3), (b1, b3) -- same-transaction ordering preserved
```

The distinct traces (equivalence classes) are determined by the relative
ordering of the two commit operations and the per-transaction operation order:

```
Trace class 1 (T1 commits first):
  Foata normal form: [a1, b1] [a2, b2] [a3] [b3]
  Layer 0: {a1, b1} -- both reads, mutually independent
  Layer 1: {a2, b2} -- both writes, mutually independent
  Layer 2: {a3}     -- T1 commits
  Layer 3: {b3}     -- T2 commits (depends on a3 via coordinator)

Trace class 2 (T2 commits first):
  Foata normal form: [a1, b1] [a2, b2] [b3] [a3]
  Layer 0-1: same as above
  Layer 2: {b3}     -- T2 commits first
  Layer 3: {a3}     -- T1 commits second
```

Only 2 distinct equivalence classes, despite 6! / constraints = many possible
linearizations. The explorer verifies MVCC invariants hold for both classes
rather than testing hundreds of redundant interleavings.

### 4.5 Two-Phase MPSC Channels -- Write Coordinator

The write coordinator uses asupersync's cancel-safe two-phase MPSC channel:

```
Phase 1 (Reserve): Writer reserves a slot in the commit pipeline
  - If cancelled before commit: slot automatically released (cancel-safe)
Phase 2 (Commit): Writer submits its write set for validation + WAL append
  - Coordinator validates, appends to WAL, responds via oneshot

Benefits over a simple Mutex:
  - Backpressure: pipeline capacity limits in-flight commits
  - Cancel-safety: if a transaction is interrupted mid-commit, no state leak
  - Ordering: commits are processed FIFO, providing fairness
```

**The two-phase API in detail:**

```rust
use asupersync::channel::mpsc;
use asupersync::cx::Cx;

// Create a bounded channel with capacity 16 (max in-flight commits)
let (tx, rx) = mpsc::channel::<CommitRequest>(16);

// Writer side (one per writing transaction):
async fn submit_commit(cx: &Cx, tx: &mpsc::Sender<CommitRequest>, req: CommitRequest) -> Result<()> {
    // Phase 1: Reserve a slot. This awaits if the channel is full (backpressure).
    // If the task is cancelled while waiting, the permit is never created -- no leak.
    let permit: mpsc::SendPermit<CommitRequest> = tx.reserve(cx).await?;

    // Between reserve() and send(), the slot is held but no data occupies it.
    // If we are cancelled here (e.g., client disconnects), dropping the permit
    // automatically releases the slot. This is the cancel-safety guarantee.

    // Phase 2: Commit the data into the reserved slot. This is synchronous
    // and cannot fail (the slot is already reserved).
    permit.send(req);
    // Alternatively: permit.abort() to explicitly release without sending.

    Ok(())
}
```

**Cancel-safety: why this matters for database commits:**

Consider the sequence of operations during a `COMMIT`:

1. B-tree modifications are complete (pages modified in write set)
2. CommitRequest is sent to the write coordinator
3. Coordinator validates the write set
4. Coordinator appends frames to WAL
5. Coordinator responds via oneshot channel
6. Transaction marks pages as committed in version store

If the task is cancelled between steps 1 and 2, the traditional approach
(simple `tx.send(req).await`) has a race: the message might be half-sent,
or the send future might be dropped while the message is being moved into
the channel buffer. With two-phase MPSC:

- Cancel between `reserve()` and `send()`: the `SendPermit` is dropped,
  which automatically releases the reserved slot. No orphaned state.
- Cancel during `reserve()` awaiting backpressure: the waiter is removed
  from the wait queue. No slot was ever reserved.

This means a cancelled transaction never leaves ghost entries in the commit
pipeline, never consumes a slot without producing a message, and never
causes the coordinator to hang waiting for a message that will never arrive.

**Backpressure: bounded channel capacity limits in-flight commits:**

The channel capacity (default: 16) limits the number of transactions that can
be simultaneously in the commit pipeline. This provides:

- **Memory boundedness**: At most 16 write sets are buffered, bounding the
  coordinator's memory usage regardless of the number of concurrent writers.
- **Latency signal**: When the channel is full, new committers block on
  `reserve()`, which signals that the commit pipeline is saturated. This
  naturally throttles the rate of new write transactions.
- **Fair queuing**: FIFO ordering of the reserve waiters ensures that
  long-waiting transactions are served first, preventing starvation.

### 4.6 Sheaf-Theoretic Consistency Checking (Optional, Speculative)

Sheaf-theoretic consistency is an optional formal lens for checking that local
observations are globally consistent (the sheaf condition). FrankenSQLite can
implement this check *in the harness* on top of the lab runtime:

- Each transaction's local view (its snapshot) is a "section" over its
  read set
- The sheaf condition requires that overlapping sections agree: if T1 and T2
  both read page P, and both see it through their respective snapshots, the
  versions they see must be consistent with the global version chain

This provides a formal framework for verifying that MVCC visibility rules
produce globally consistent views.

**Concrete example:**

```rust
// In a lab test, after running N concurrent transactions:
let sections: Vec<Section> = completed_txns.iter().map(|txn| {
    Section {
        domain: txn.read_set.clone(),
        assignment: txn.observed_versions.clone(),  // PageNumber -> (TxnId, PageData)
    }
}).collect();

// Check the sheaf condition: overlapping sections must agree (pseudocode)
let result = fsqlite_harness::sheaf::check_consistency(&sections, &global_version_chains);
assert!(result.is_consistent(), "Sheaf violation: {}", result.obstruction());
```

### 4.7 Conformal Calibration -- Distribution-Free Confidence

For performance benchmarks, conformal calibration provides **distribution-free
confidence intervals** rather than assuming normal distributions. This means:

- "Page lock contention is below 5% with 95% confidence" is a rigorous
  statement, not a guess based on assumed distributions
- Benchmark results include conformal p-values for regression detection
- No parametric assumptions needed (important because database workloads
  are highly non-normal)

**Concrete example: detecting MVCC throughput regression:**

```rust
use asupersync::lab::conformal::{ConformalCalibrator, ConformalConfig};

let mut calibrator = ConformalCalibrator::new(ConformalConfig {
    alpha: 0.05,                    // 95% coverage guarantee
    min_calibration_samples: 20,     // need at least 20 baseline runs
});

// Calibration phase: run baseline benchmark 50 times
for seed in 0..50 {
    let throughput = run_mvcc_benchmark(seed);
    calibrator.observe(throughput);
}

// Prediction phase: is this new measurement within the prediction set?
let new_throughput = run_mvcc_benchmark_after_code_change(51);
let in_prediction_set = calibrator.is_conforming(new_throughput);
if !in_prediction_set {
    panic!("MVCC throughput regression detected: {} ops/sec outside 95% prediction set",
           new_throughput);
}
```

### 4.8 TLA+ Export -- Model Checking

Asupersync can export protocol specifications to TLA+ for model checking.
The MVCC commit protocol, WAL checkpoint protocol, and GC protocol should
all be specified in a form that can be exported and verified by TLC (the
TLA+ model checker).

**Concrete example: exporting the commit protocol:**

```rust
// The MVCC commit protocol expressed as a state machine that can be
// exported to TLA+ via asupersync's trace infrastructure:
let protocol = ProtocolSpec::new("MvccCommit")
    .state_var("txn_states", "TxnId -> {Active, Committed, Aborted}")
    .state_var("page_locks", "PageNumber -> Option<TxnId>")
    .state_var("version_chains", "PageNumber -> Seq<PageVersion>")
    .action("begin", |s| { /* snapshot capture */ })
    .action("write_page", |s| { /* try_acquire + cow */ })
    .action("commit", |s| { /* validate + wal_append + publish */ })
    .action("abort", |s| { /* release locks + discard */ })
    .invariant("LockExclusive", "forall p: |{t: page_locks[p] = t}| <= 1")
    .invariant("SnapshotIsolation", "/* visibility predicate */");

// Export to TLA+
let tla_spec = protocol.to_tla_plus();
// Run TLC model checker with bounded state space
```

### 4.9 BlockingPool Integration

All file I/O in FrankenSQLite is dispatched to asupersync's blocking pool,
ensuring that the async runtime's worker threads are never blocked by
synchronous system calls.

**How file I/O is dispatched:**

```rust
/// VFS file operations are async at the trait level but dispatch to
/// the blocking pool for actual I/O.
impl VfsFile for UnixFile {
    async fn read_at(&self, cx: &Cx, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let fd = self.fd;
        let len = buf.len();
        let result = cx.spawn_blocking(move || {
            // This runs on a blocking pool thread, not an async worker
            let mut tmp = vec![0u8; len];
            let n = nix::sys::uio::pread(fd, &mut tmp, offset as i64)?;
            Ok((tmp, n))
        }).await?;
        let (data, n) = result;
        buf[..n].copy_from_slice(&data[..n]);
        Ok(n)
    }

    async fn write_at(&self, cx: &Cx, offset: u64, data: &[u8]) -> Result<()> {
        let fd = self.fd;
        let data = data.to_vec();
        cx.spawn_blocking(move || {
            nix::sys::uio::pwrite(fd, &data, offset as i64)?;
            Ok(())
        }).await?
    }

    async fn sync(&self, cx: &Cx, flags: SyncFlags) -> Result<()> {
        let fd = self.fd;
        cx.spawn_blocking(move || {
            if flags.contains(SyncFlags::DATAONLY) {
                nix::unistd::fdatasync(fd)?;
            } else {
                nix::unistd::fsync(fd)?;
            }
            Ok(())
        }).await?
    }
}
```

**Pool sizing:**

The blocking pool uses a min/max thread model:

- **Minimum threads: 1** -- always at least one blocking thread available for
  immediate dispatch, avoiding cold-start latency on the first I/O operation.
- **Maximum threads: 4** -- caps the number of concurrent blocking operations.
  This is intentionally small because:
  - SQLite workloads are typically I/O bound on a single file, so more threads
    do not increase disk throughput (they just increase contention)
  - WAL writes are serialized through the coordinator anyway
  - Excess threads waste memory (each OS thread costs ~8MB stack)
- **Idle timeout: 10 seconds** -- threads above the minimum are retired after
  10 seconds of inactivity, reclaiming resources during quiet periods.

**How this interacts with async callers:**

The async-to-blocking bridge works as follows:

1. Async task calls `cx.spawn_blocking(closure)`, which returns a `Future`.
2. The closure is placed on the blocking pool's work queue.
3. A blocking pool thread picks up the closure and executes it.
4. When the closure completes, the result is sent back via an internal oneshot
   channel, waking the async task.
5. The async task receives the result and continues.

This ensures that:
- The async runtime's worker threads (which drive the VDBE, parser, planner)
  are never blocked by disk I/O.
- File I/O operations are still cancellable: if the async task is cancelled,
  the blocking operation runs to completion (cannot interrupt `pread64`), but
  the result is discarded and the async task is cleaned up.
- Under the lab reactor, `spawn_blocking` is intercepted and executed
  synchronously within the virtual time framework, maintaining determinism.

---

## 5. MVCC Formal Model (Revised)

This section supersedes `MVCC_SPECIFICATION.md` with corrections for the
isolation level analysis, checksum performance, and multi-process semantics.

### 5.1 Core Types

```
TxnId       := u64                          -- monotonically increasing, AtomicU64
PageNumber  := NonZeroU32                   -- 1-based page number
PageData    := Vec<u8>                      -- page content, length = page_size

Snapshot := {
    high_water_mark : TxnId,               -- all txn_ids <= this are "potentially committed"
    in_flight       : RoaringBitmap,        -- active txns at snapshot creation
                                            -- Roaring Bitmap for O(1) membership test
                                            -- (replacing SortedVec+Bloom: fewer false positives,
                                            -- compressed storage, set operations for GC horizon)
}

PageVersion := {
    pgno       : PageNumber,
    created_by : TxnId,
    data       : PageData,                  -- or RaptorQ delta (Section 3.4.4)
    prev       : Option<Box<PageVersion>>,  -- link to older version
}

PageLockTable := ShardedHashMap<PageNumber, TxnId>  -- exclusive write locks
    -- Sharded by PageNumber hash into N shards (N = 64 default).
    -- Each shard is a parking_lot::Mutex<HashMap<PageNumber, TxnId>>.
    -- Sharding reduces contention under high writer counts (>16 concurrent
    -- writers would bottleneck on a single Mutex). Shard count is a power
    -- of two for fast modular arithmetic (pgno & (N-1)).

SireadTable := ShardedHashMap<PageNumber, SmallVec<TxnId>>
    -- Maps each page to the set of active transactions that have read it.
    -- Sharded like PageLockTable for concurrency. Entries cleaned up when
    -- transactions commit or abort. Used by SSI to track rw-antidependencies.
    -- Future refinement: key = (PageNumber, CellTag) for reduced false positives.

Transaction := {
    txn_id      : TxnId,
    snapshot    : Snapshot,
    write_set   : HashMap<PageNumber, PageVersion>,
    read_set    : HashSet<PageNumber>,
    intent_log  : Vec<IntentOp>,            -- semantic operation log for rebase merge
    page_locks  : HashSet<PageNumber>,
    state       : {Active, Committed{commit_seq}, Aborted{reason}},
    mode        : {Serialized, Concurrent},

    -- SSI state (active for Concurrent mode):
    has_in_rw   : bool,    -- some other txn read what this txn wrote (incoming rw edge)
    has_out_rw  : bool,    -- this txn read what some other txn later wrote (outgoing rw edge)
    rw_in_from  : SmallVec<TxnId>,   -- txns that have rw edges TO this txn
    rw_out_to   : SmallVec<TxnId>,   -- txns that this txn has rw edges TO
}

IntentOp := {
    -- Semantic B-tree operations for deterministic rebase merge (Section 5.10)
    Insert { table: TableId, key: RowId, record: Vec<u8> }
  | Delete { table: TableId, key: RowId }
  | Update { table: TableId, key: RowId, new_record: Vec<u8> }
  | IndexInsert { index: IndexId, key: Vec<u8>, rowid: RowId }
  | IndexDelete { index: IndexId, key: Vec<u8>, rowid: RowId }
}

CommitLog := BTreeMap<TxnId, CommitRecord>

CommitRecord := {
    txn_id    : TxnId,
    pages     : Vec<PageNumber>,
    timestamp : Instant,
}
```

### 5.2 Invariants

**INV-1 (Monotonicity):** TxnIds are strictly monotonically increasing.

```
Formal: forall T1, T2 : begin(T1) happens-before begin(T2) => T1.txn_id < T2.txn_id
```

*Enforcement:* `TxnManager::next_txn_id` is an `AtomicU64` incremented with
`fetch_add(1, SeqCst)`. Sequential consistency ordering guarantees that no two
calls return the same value, and the values are strictly increasing.

*Violation consequence:* If TxnIds are reused or non-monotone, snapshot
visibility becomes undefined. A transaction could see a "future" version as
old, or fail to see a committed version. This leads to phantom reads, lost
updates, and corrupted query results.

---

**INV-2 (Lock Exclusivity):** For any page P, at most one active transaction
holds a lock: `|{T : T.state = Active AND P IN T.page_locks}| <= 1`.

```
Formal: forall P : forall T1, T2 :
    T1.state = Active AND T2.state = Active AND T1 != T2
    => NOT (P in T1.page_locks AND P in T2.page_locks)
```

*Enforcement:* `PageLockTable::try_acquire(pgno, txn_id)` performs an atomic
check-and-set under a mutex. If `pgno` is already mapped to a different
`txn_id`, the call returns `Err(SQLITE_BUSY)` without modifying the table.
The lock is only inserted if the page is unlocked or already held by the
same transaction (idempotent re-acquire).

*Violation consequence:* Two transactions simultaneously modifying the same page
would produce two conflicting `PageVersion` entries. The version chain would
have a fork (two versions with different `created_by` but the same `prev`),
breaking INV-3. The resulting page data depends on which commit runs last,
leading to lost updates.

---

**INV-3 (Version Chain Order):** If `V.prev = Some(V')`, then
`V.created_by > V'.created_by`.

```
Formal: forall P, V, V' :
    V in version_chain(P) AND V.prev = Some(V')
    => V.created_by > V'.created_by
```

*Enforcement:* Versions are published to the version store during commit, in
the order of commit (newer transactions have higher TxnIds by INV-1). The
`publish()` operation prepends the new version to the head of the chain,
setting its `prev` to the current head. Since the committing transaction's
TxnId is greater than all previously committed TxnIds, the ordering holds.

*Violation consequence:* Version resolution walks the chain from newest to
oldest, returning the first visible version. If the chain is mis-ordered,
`resolve()` might return an older version when a newer one should be visible,
or skip a version entirely. This breaks snapshot isolation.

---

**INV-4 (Write Set Consistency):** `forall P in T.write_set.keys(): P in T.page_locks`.

```
Formal: forall T, P : P in T.write_set.keys() => P in T.page_locks
```

*Enforcement:* `write_page()` acquires the page lock before inserting into the
write set. The lock acquisition is the first operation; if it fails, the write
set is not modified.

*Violation consequence:* A page in the write set without a lock means another
transaction could also write the same page (since no lock prevents it). Both
transactions would attempt to publish conflicting versions during commit,
bypassing the first-committer-wins check.

---

**INV-5 (Snapshot Stability):** A transaction's snapshot is immutable.

```
Formal: forall T : T.snapshot at time t = T.snapshot at time t' for all t' > t
    where t is the time of begin(T)
```

*Enforcement:* The `Snapshot` struct is stored by value inside the `Transaction`
struct. No mutable references to `T.snapshot` are ever created after
`capture_snapshot()` returns. The `Snapshot` type does not implement
interior mutability.

*Violation consequence:* If a snapshot changes during a transaction, reads at
different times could see different versions of the same page, breaking
the repeatable-read guarantee that snapshot isolation provides.

---

**INV-6 (Commit Atomicity):** All-or-nothing visibility.

```
Formal: forall T, S :
    if T.state = Committed then
        (forall P in T.write_set.keys(): visible(T.write_set[P], S))
        OR (forall P in T.write_set.keys(): NOT visible(T.write_set[P], S))
```

*Enforcement:* Version publishing and commit log insertion happen while the
coordinator holds the commit pipeline. All versions are published, then the
commit record is inserted. The commit record is what makes the TxnId appear
in `committed_txns`. Until the record is inserted, no snapshot considers the
TxnId committed, so none of the versions are visible. Once inserted, all
versions become visible simultaneously.

*Violation consequence:* Partial visibility means a reader could see some of
a transaction's writes but not others, observing an inconsistent state. For
example, a transfer between two accounts might show the debit but not the
credit, temporarily "losing" money.

---

**INV-7 (Serialized Mode):** If `T.mode = Serialized`, then T holds the
global write mutex for the duration of its write operations. At most one
Serialized-mode writer is active at any time.

```
Formal: forall T1, T2 :
    T1.mode = Serialized AND T2.mode = Serialized
    AND T1.state = Active AND T2.state = Active
    AND T1 != T2
    => FALSE
```

*Enforcement:* `begin(mode=Serialized)` acquires `manager.global_write_mutex`
before returning. The mutex is held until `commit()` or `abort()` releases it.
Since Rust's `Mutex` allows at most one holder, at most one Serialized
transaction is active.

*Violation consequence:* If two Serialized transactions run simultaneously,
the system no longer provides SERIALIZABLE isolation in Serialized mode. This
breaks backward compatibility with C SQLite's guarantee that writers are
serialized.

### 5.3 Visibility Predicate

```
visible(V, S) :=
    V.created_by <= S.high_water_mark
    AND V.created_by NOT IN S.in_flight
    AND V.created_by IN committed_txns

resolve(P, S) :=
    first V in version_chain(P) where visible(V, S)
    // Falls back to on-disk data (TxnId::ZERO) if no version found
```

**Complete worked example with 5 transactions:**

Consider a database with page P1. Five transactions execute in the following
order:

```
Time  Action
----  ------
t0    T1 begins   (txn_id=1)
t1    T2 begins   (txn_id=2)
t2    T1 writes P1 (version V1, created_by=1)
t3    T3 begins   (txn_id=3)
t4    T1 commits  (V1 published)
t5    T2 writes P1 -- FAILS: page locked by T1 (wait... T1 committed at t4, lock released)
      T2 writes P1 (version V2, created_by=2) -- succeeds, lock now free
t6    T4 begins   (txn_id=4)
t7    T2 commits  (V2 published) -- but first-committer-wins check: T1 committed P1
      after T2's snapshot. T2 must ABORT.
t8    T5 begins   (txn_id=5)
t9    T3 writes P1 (version V3, created_by=3)
t10   T3 commits  (V3 published) -- first-committer-wins: T1 committed P1 after T3's
      snapshot (T3.snapshot.hwm=2, T1.txn_id=1 <= 2, but T1 was NOT in T3.in_flight
      since T1 committed before T3's snapshot was taken... wait, T3 began at t3,
      T1 committed at t4, so T1 was in T3's in_flight set). Check: T1.txn_id=1,
      T3.snapshot.hwm=2, T3.snapshot.in_flight={1,2}. T1 committed P1, T1.txn_id=1
      is in in_flight, so from T3's perspective T1 was not committed at snapshot time.
      Now at commit validation: any committed_txn with txn_id > T3.snapshot.hwm? None.
      Any committed_txn in T3.snapshot.in_flight that committed P1? T1 (txn_id=1) is
      in in_flight and committed P1. This is a conflict. T3 ABORTS.
t11   T5 writes P1 (version V5, created_by=5)
t12   T5 commits  (V5 published, no conflict: only T1 committed P1, and T5's
      snapshot.hwm >= T1.txn_id and T1 not in T5.in_flight)
```

**Snapshot contents at each transaction's begin:**

| Transaction | txn_id | hwm | in_flight | Committed at begin |
|-------------|--------|-----|-----------|-------------------|
| T1 | 1 | 0 | {} | {} |
| T2 | 2 | 1 | {1} | {} |
| T3 | 3 | 2 | {1, 2} | {} |
| T4 | 4 | 3 | {2, 3} | {1} |
| T5 | 5 | 4 | {3} | {1} (T2 aborted) |

**Visibility of P1 versions from each snapshot:**

| Version | created_by | T1's snapshot | T2's snapshot | T3's snapshot | T4's snapshot | T5's snapshot |
|---------|-----------|---------------|---------------|---------------|---------------|---------------|
| V1 | 1 | No (self-vis only) | No (1 in in_flight) | No (1 in in_flight) | Yes (1 <= 3, not in {2,3}, committed) | Yes (1 <= 4, not in {3}, committed) |
| V5 | 5 | No (5 > 0) | No (5 > 1) | No (5 > 2) | No (5 > 3) | No (self-vis only) |
| on-disk (TxnId::ZERO) | 0 | Yes | Yes | Yes | No (V1 supersedes) | No (V1/V5 supersede) |

What each transaction sees when reading P1 through its snapshot:

| Transaction | resolve(P1, snapshot) |
|-------------|----------------------|
| T1 (before own write) | on-disk (TxnId::ZERO) |
| T1 (after own write) | V1 (self-visibility) |
| T2 | on-disk (T1 is in in_flight, not visible) |
| T3 | on-disk (T1 is in in_flight, not visible) |
| T4 | V1 (T1 committed and visible) |
| T5 (before own write) | V1 (T1 committed and visible) |
| T5 (after own write) | V5 (self-visibility) |

### 5.4 Transaction Lifecycle

**Begin:**
```
begin(manager, mode) -> Transaction:
    txn_id = manager.next_txn_id.fetch_add(1, SeqCst)
    snapshot = capture_snapshot(manager)
    if mode == Serialized:
        manager.global_write_mutex.lock()  // exact SQLite compat
    return Transaction { txn_id, snapshot, mode, ... }
```

**Read (both modes):**
```
read_page(T, pgno) -> PageData:
    T.read_set.insert(pgno)
    if T.mode == Concurrent:
        siread_table.entry(pgno).or_default().push(T.txn_id)  // SSI tracking
    if pgno in T.write_set: return T.write_set[pgno].data
    return resolve(pgno, T.snapshot).data
```

**Write:**
```
write_page(T, pgno, new_data) -> Result<()>:
    if T.mode == Serialized:
        // Already hold global write mutex; no page lock needed
        // (but still track in write_set for WAL append)
    else: // Concurrent mode
        lock_result = page_lock_table.try_acquire(pgno, T.txn_id)
        if lock_result = AlreadyHeld(other): return Err(SQLITE_BUSY)
        T.page_locks.insert(pgno)

        // SSI: update rw-antidependencies for readers of this page
        for reader_id in siread_table.readers(pgno):
            if reader_id != T.txn_id AND transaction(reader_id).state == Active:
                // reader read this page, T is now writing it: reader ->rw T
                transaction(reader_id).has_out_rw = true
                T.has_in_rw = true
                transaction(reader_id).rw_out_to.push(T.txn_id)
                T.rw_in_from.push(reader_id)

    base = resolve_for_txn(pgno, T)
    T.write_set.insert(pgno, PageVersion { pgno, T.txn_id, new_data, base })
    Ok(())
```

**Commit:**
```
commit(T) -> Result<()>:
    if T.mode == Concurrent:
        // Step 1: SSI validation (serializable by default)
        if T.has_in_rw AND T.has_out_rw:
            // T is a "pivot" in a dangerous structure (T1 ->rw T ->rw T3).
            // Conservative rule: abort to prevent serialization cycle.
            abort(T)
            return Err(SQLITE_BUSY_SNAPSHOT)  // retryable

        // Step 2: First-committer-wins validation + algebraic merge
        for pgno in T.write_set.keys():
            for committed_txn in commit_log.range(T.snapshot.high_water_mark+1..):
                if pgno in committed_txn.pages:
                    // Attempt deterministic rebase merge (Section 5.10)
                    if can_rebase_merge(T, committed_txn, pgno):
                        perform_rebase_merge(T, committed_txn, pgno)
                    // Fallback: try algebraic byte merge (Section 3.4.5)
                    elif can_algebraic_merge(T, committed_txn, pgno):
                        perform_algebraic_merge(T, committed_txn, pgno)
                    else:
                        abort(T)
                        return Err(SERIALIZATION_FAILURE)

    // WAL append (serialized via write coordinator channel)
    coordinator.send(CommitRequest {
        write_set: T.write_set,
        repair_symbols: raptorq_encode(T.write_set),  // Section 3.4.1
        response: oneshot_rx,
    })
    result = oneshot_rx.recv()

    if result.is_ok():
        // Publish versions, record in commit log, release locks
        ...

    if T.mode == Serialized:
        manager.global_write_mutex.unlock()

    result
```

**Transaction state machine:**

```
                    +--------+
                    | Active |
                    +--------+
                   /          \
          commit()/            \abort() or
         succeeds              validation fails
                /                \
    +-----------+              +---------+
    | Committed |              | Aborted |
    +-----------+              +---------+

State transitions:
  Active -> Committed:  Only via successful commit validation + WAL append
  Active -> Aborted:    Via explicit ROLLBACK, commit validation failure,
                        SQLITE_BUSY on page lock, or SQLITE_INTERRUPT
  Committed -> *:       Terminal state (no further transitions)
  Aborted -> *:         Terminal state (no further transitions)

All transitions are irreversible. A committed transaction cannot be
rolled back; an aborted transaction cannot be retried (a new transaction
must be started).
```

**Concurrent mode vs Serialized mode side-by-side:**

```
                    Serialized Mode              Concurrent Mode
                    ===============              ===============

BEGIN:              Acquire global_write_mutex   No global lock
                    Capture snapshot             Capture snapshot

READ:               resolve(P, snapshot)         resolve(P, snapshot)
                    (identical)                  (identical)

WRITE:              No page lock needed          try_acquire page lock
                    (mutex provides exclusion)   Return SQLITE_BUSY if held
                    Add to write_set             Add to write_set

COMMIT:             No validation needed         SSI check: abort if pivot
                    (mutex ensures serial)       First-committer-wins check
                    WAL append                   Rebase merge / algebraic merge
                    Release global_write_mutex   WAL append
                                                 Release page locks

ABORT:              Release global_write_mutex   Release all page locks
                    Discard write_set            Discard write_set
                                                 Clean up SIREAD entries

CONCURRENCY:        One writer at a time         Multiple writers in parallel
                    (exact SQLite behavior)      (conflict on same page only)

ISOLATION:          SERIALIZABLE                 SERIALIZABLE (Page-SSI)
                    (trivially, by serializing)  (conservative rw-antidependency
                                                  tracking; write skew prevented)
                                                 PRAGMA fsqlite.serializable=OFF
                                                  downgrades to SI (opt-in only)

USE CASE:           DROP-in SQLite replacement   Applications that opt in
                    Legacy applications          to concurrent writes
```

**How savepoints interact with MVCC:**

Savepoints are a B-tree-level mechanism, NOT an MVCC-level mechanism. The
MVCC layer does not know about savepoints. Here is why:

- `SAVEPOINT name` records the current state of the B-tree modifications
  (specifically, the set of pages in the write set and their pre-modification
  data).
- `ROLLBACK TO name` undoes B-tree modifications back to the savepoint by
  restoring the recorded page states within the write set.
- `RELEASE name` discards the savepoint record.

All of this happens within a single MVCC transaction. The transaction's
`txn_id`, `snapshot`, and `page_locks` are unaffected by savepoint operations.
Page locks acquired after a savepoint are NOT released on `ROLLBACK TO` --
they are held until the enclosing transaction commits or aborts. This is
because releasing a page lock mid-transaction would allow another transaction
to acquire it, potentially violating first-committer-wins when the outer
transaction later tries to re-write the page.

### 5.5 Safety Proofs

**Theorem 1 (Deadlock Freedom):** The MVCC system is deadlock-free.

**Proof:** A deadlock requires a cycle in the wait-for graph. Our system has
no wait-for graph because `try_acquire()` never blocks -- it returns
`Err(SQLITE_BUSY)` immediately if the lock is held by another transaction.
Since no transaction ever waits for another transaction to release a lock,
no cycle can form. QED.

**Structural guarantee:** This is not a detection-based approach (like timeout
or cycle detection in a wait-for graph). Deadlocks are *structurally impossible*
because the `try_acquire` operation is non-blocking by construction.

---

**Theorem 2 (Snapshot Isolation in Concurrent Mode):** Every Concurrent-mode
transaction observes a consistent snapshot -- it never sees partial results
of concurrent transactions.

**Proof:** Let `T_r` be a reading transaction with snapshot `S_r`, and let
`T_w` be any other transaction. We must show that `T_r` either sees ALL of
`T_w`'s writes or NONE of them.

Define: `T_w`'s writes are the set of `PageVersion` entries
`{V_1, V_2, ..., V_k}` where `V_i.created_by = T_w.txn_id` for all `i`.

For any version `V_i` created by `T_w`, the visibility predicate is:

```
visible(V_i, S_r) =
    T_w.txn_id <= S_r.high_water_mark
    AND T_w.txn_id NOT IN S_r.in_flight
    AND T_w.txn_id IN committed_txns
```

Crucially, all three conditions depend ONLY on `T_w.txn_id`, not on the
specific page or version. Therefore, `visible(V_i, S_r)` has the same truth
value for all `i in {1, ..., k}`.

**Case 1:** `T_w.txn_id > S_r.high_water_mark`. Then
`T_w.txn_id <= S_r.high_water_mark` is false for all versions. `T_r` sees
NONE of `T_w`'s writes. This case covers transactions that began after
`T_r`'s snapshot was captured.

**Case 2:** `T_w.txn_id <= S_r.high_water_mark` AND `T_w.txn_id IN S_r.in_flight`.
Then the `NOT IN S_r.in_flight` condition is false for all versions. `T_r`
sees NONE of `T_w`'s writes. This case covers transactions that were active
(but not yet committed) when `T_r`'s snapshot was captured.

**Case 3:** `T_w.txn_id <= S_r.high_water_mark` AND `T_w.txn_id NOT IN S_r.in_flight`
AND `T_w.txn_id NOT IN committed_txns`. Then the `IN committed_txns` condition
is false for all versions. `T_r` sees NONE of `T_w`'s writes. This case covers
transactions that were assigned a TxnId but later aborted.

**Case 4:** `T_w.txn_id <= S_r.high_water_mark` AND `T_w.txn_id NOT IN S_r.in_flight`
AND `T_w.txn_id IN committed_txns`. All three conditions are true for all
versions. `T_r` sees ALL of `T_w`'s writes. This case covers transactions
that were committed before `T_r`'s snapshot was captured.

In no case does `T_r` see a strict subset of `T_w`'s writes.

Furthermore, the snapshot `S_r` is immutable (INV-5), so the truth value of
`visible(V_i, S_r)` does not change during `T_r`'s lifetime, even if `T_w`
commits midway through `T_r`'s execution.

QED.

---

**Theorem 3 (First-Committer-Wins):** If two Concurrent-mode transactions
`T_1`, `T_2` both write page P, at most one commits successfully.

**Proof:** We consider two exhaustive sub-cases based on the temporal ordering
of their page lock acquisitions.

**Case A (Concurrent lock contention):** `T_1` and `T_2` both attempt
`write_page(P)` while both are Active. Without loss of generality, suppose
`T_1` calls `try_acquire(P)` first and succeeds. When `T_2` subsequently
calls `try_acquire(P)`, it finds the lock held by `T_1` and receives
`Err(SQLITE_BUSY)`. `T_2` cannot write P at all. It either aborts entirely
or proceeds without writing P (in which case it does not conflict on P).

**Case B (Sequential commits):** `T_1` acquires the lock on P, writes P,
and commits first, releasing the lock. `T_2` then acquires the lock on P
(now free) and writes P. When `T_2` attempts to commit, the commit validation
phase scans the commit log for entries with `txn_id > T_2.snapshot.high_water_mark`
(or entries whose `txn_id` was in `T_2.snapshot.in_flight`). Since `T_1`
committed P, and `T_1`'s commit was either:

- After `T_2`'s snapshot (so `T_1.txn_id > T_2.snapshot.hwm`), which is
  caught by the range scan, OR
- During `T_2`'s snapshot (`T_1.txn_id` was in `T_2.snapshot.in_flight`),
  which is caught by checking in_flight members that subsequently committed

In either case, the validation finds that P was committed by `T_1` after
`T_2`'s snapshot was taken. Unless algebraic merge is possible (Section 3.4.5),
`T_2` must abort.

In both cases, at most one transaction's write to P survives. QED.

---

**Theorem 4 (GC Safety):** Garbage collection never removes a version that
any active or future transaction could need.

**Proof (by strong induction on the version chain):**

Define `gc_horizon = min(T.txn_id : T in active_transactions)`. If there are
no active transactions, `gc_horizon = latest_committed_txn_id`.

**Reclaimability predicate:** A version `V` of page `P` is reclaimable iff:

```
V.created_by < gc_horizon
AND exists V' in version_chain(P) such that:
    V'.created_by > V.created_by
    AND V'.created_by <= gc_horizon
    AND V'.created_by IN committed_txns
```

We must show: for any active transaction `T_a` and any future transaction
`T_f` (one that has not yet started), if `V` is reclaimable, then neither
`T_a` nor `T_f` will ever need `V`.

**For active transactions `T_a`:**
- `T_a.snapshot.high_water_mark >= T_a.txn_id >= gc_horizon > V.created_by`
  (by definition of gc_horizon).
- The superseding version `V'` satisfies `V'.created_by <= gc_horizon`.
- Since `V'.created_by <= gc_horizon <= T_a.txn_id`, and `V'` is committed,
  we need to verify that `V'` is visible to `T_a`'s snapshot:
  - `V'.created_by <= T_a.snapshot.high_water_mark` (since hwm >= txn_id >= gc_horizon >= V'.created_by)
  - `V'.created_by NOT IN T_a.snapshot.in_flight` -- this holds because
    `V'.created_by <= gc_horizon = min(active txn_ids)`, and the in_flight
    set contains only txn_ids of transactions that were active at snapshot
    time. Since `V'` is committed and `V'.created_by < T_a.txn_id`, `V'`
    must have committed before `T_a` began (otherwise it would still be in
    active_transactions, contradicting `V'.created_by <= gc_horizon`).
    Therefore `V'.created_by NOT IN T_a.snapshot.in_flight`.
  - `V'.created_by IN committed_txns` (given).
- So `visible(V', T_a.snapshot) = true`. Since `V'.created_by > V.created_by`,
  `V'` appears before `V` in the version chain (by INV-3). Therefore
  `resolve(P, T_a.snapshot)` returns `V'` or something newer, never `V`.
- `T_a` does not need `V`.

**For future transactions `T_f`:**
- `T_f` has not yet started, so `T_f.txn_id > current_max_txn_id >= gc_horizon`.
- `T_f.snapshot.high_water_mark >= gc_horizon`.
- By the same argument as above, `V'` is visible to `T_f`'s snapshot (it is
  committed and below `T_f`'s high water mark, and `V'`'s creating transaction
  completed before `T_f` started, so it cannot be in `T_f`'s in_flight set).
- `T_f` does not need `V`.

Therefore, `V` is safe to reclaim. QED.

---

**Theorem 5 (Memory Boundedness):** Under steady-state load with maximum
transaction duration `D` and commit rate `R` (commits per second), the
maximum number of retained versions per page is bounded by `R * D + 1`.

**Proof:** The GC horizon is `min(active txn_ids)`. Under steady state, the
oldest active transaction started at most `D` seconds ago. In those `D`
seconds, at most `R * D` transactions committed. Each committed transaction
can create at most one version per page. The version chain for any page
therefore contains at most `R * D` versions above the GC horizon, plus one
version at or below the horizon (the one visible to the oldest transaction).
All versions below this are reclaimable by GC Safety (Theorem 4) and will
be collected by the next GC sweep. Total retained versions per page:
`R * D + 1`. QED.

**Practical implication:** With `D = 5s` (max transaction duration) and
`R = 1000 commits/s`, at most 5001 versions per page. At 4KB per version,
this is ~20MB per hot page. In practice, most pages are not written by every
transaction, so actual memory usage is much lower.

---

**Theorem 6 (Liveness):** Every transaction either commits or aborts in
finite time, assuming:
(a) The application eventually calls COMMIT or ROLLBACK for every transaction.
(b) The write coordinator processes requests in finite time.
(c) WAL I/O completes in finite time.

**Proof:** We show that every transaction makes progress through its lifecycle
without unbounded blocking.

**Begin:** `fetch_add` on an `AtomicU64` completes in O(1). `capture_snapshot`
acquires a read lock on `active_transactions` (finite, by assumption that no
holder holds it forever). For Serialized mode, `global_write_mutex.lock()` may
wait, but only for the duration of another Serialized transaction, which by
inductive hypothesis completes in finite time.

**Read:** `resolve()` walks the version chain, which has bounded length
(Theorem 5). Each visibility check is O(1) amortized (Bloom filter). Total
time is bounded.

**Write:** `try_acquire` is non-blocking (returns immediately with `Ok` or
`Err`). Copy-on-write is O(page_size). Total time is bounded.

**Commit (Concurrent mode):** Commit validation scans the commit log
(bounded by `R * D` entries). WAL append completes in finite time (assumption c).
Version publishing is O(write_set_size). Lock release is O(page_locks_size).
Total time is bounded.

**Commit (Serialized mode):** No validation needed. WAL append and version
publishing same as above. Mutex release is O(1). Total time is bounded.

**Abort:** Discard write set O(write_set_size), release locks O(page_locks_size).
Total time is bounded.

Therefore, every transaction that begins will eventually reach either the
Committed or Aborted terminal state, assuming the application and I/O
subsystem cooperate. QED.

### 5.6 Multi-Process Semantics

FrankenSQLite provides MVCC concurrency both within a single process (via
in-memory lock tables and version chains) and across processes (via a
shared-memory coordination region). The in-process path is the fast path;
the cross-process path adds ~100ns per lock operation due to mmap-based
atomics.

**Architecture:** Each database file `foo.db` (or `foo.db.fsqlite/` in Native
mode) has an associated shared-memory file `foo.db.fsqlite-shm` that
provides the cross-process coordination plane. This is analogous to SQLite's
WAL-index shared memory but extended for MVCC.

#### 5.6.1 Shared-Memory Coordination Region

The shared-memory file is structured as a fixed-size header followed by
an array of TxnSlots:

```
SharedMemoryLayout := {
    magic            : [u8; 8],        -- "FSQLSHM\0"
    version          : u32,            -- layout version (1)
    page_size        : u32,            -- database page size
    max_txn_slots    : u32,            -- capacity of TxnSlot array (default: 256)
    next_txn_id      : AtomicU64,      -- global TxnId counter (fetch_add)
    commit_seq       : AtomicU64,      -- global commit sequence counter
    gc_horizon       : AtomicU64,      -- min(active txn_ids) across all processes
    lock_table_offset: u64,            -- byte offset to PageLockTable region
    siread_offset    : u64,            -- byte offset to SIREAD plane
    txn_slot_offset  : u64,            -- byte offset to TxnSlot array
    checksum         : u64,            -- xxhash3 of header fields
    _padding         : [u8; 64],       -- align to cache line
    // --- TxnSlot array follows at txn_slot_offset ---
    // --- PageLockTable region follows at lock_table_offset ---
    // --- SIREAD plane follows at siread_offset ---
}
```

The shared-memory file is created on first access and mapped by every
process that opens the database. All fields after the header use atomic
operations (SeqCst ordering for correctness, Relaxed for read-only counters).

#### 5.6.2 TxnSlot: Per-Transaction Cross-Process State

```
TxnSlot := {
    txn_id          : AtomicU64,     -- 0 = slot is free
    pid             : AtomicU32,     -- owning process ID
    lease_expiry    : AtomicU64,     -- Unix timestamp (seconds) of lease expiry
    snapshot_hwm    : AtomicU64,     -- snapshot high_water_mark
    state           : AtomicU8,      -- 0=Free, 1=Active, 2=Committing, 3=Committed, 4=Aborted
    mode            : AtomicU8,      -- 0=Serialized, 1=Concurrent
    has_in_rw       : AtomicBool,    -- SSI: has incoming rw-antidependency
    has_out_rw      : AtomicBool,    -- SSI: has outgoing rw-antidependency
    write_set_pages : AtomicU32,     -- count of pages in write set (for GC sizing)
    _padding        : [u8; 19],      -- pad to 64 bytes (cache-line aligned)
}
```

**Slot lifecycle:**
1. **Acquire:** Process scans TxnSlot array for a slot with `txn_id == 0`.
   CAS the `txn_id` from 0 to the new TxnId. Set `pid`, `lease_expiry`,
   `state = Active`.
2. **Renew lease:** While active, process periodically updates `lease_expiry`
   to `now + LEASE_DURATION` (default: 30 seconds). This is a simple
   atomic store.
3. **Commit/Abort:** Set `state` to Committed or Aborted. Release page locks.
   Set `txn_id` to 0 (slot is free).

**Lease-based crash cleanup:** If a process crashes, its TxnSlots become
orphaned (lease expires, `pid` is no longer alive). Any process can detect
this and clean up:

```
cleanup_orphaned_slots():
    now = unix_timestamp()
    for slot in txn_slots:
        if slot.txn_id != 0 AND slot.lease_expiry < now:
            // Lease expired -- check if process is alive
            if !process_alive(slot.pid):
                // Process crashed. Abort its transaction.
                release_page_locks_for(slot.txn_id)
                release_siread_locks_for(slot.txn_id)
                slot.state = Aborted
                slot.txn_id = 0    // Free the slot
```

`process_alive(pid)` uses `kill(pid, 0)` on Unix (signal 0 checks existence
without sending a signal). This is a standard POSIX technique.

#### 5.6.3 Cross-Process Page Lock Table

The shared-memory PageLockTable is a fixed-size hash table (not the
in-process sharded HashMap). It uses open addressing with linear probing:

```
SharedPageLockTable := {
    capacity : u32,                   -- power-of-2 (default: 65536)
    entries  : [PageLockEntry; capacity],
}

PageLockEntry := {
    page_number : AtomicU32,         -- 0 = empty slot
    owner_txn   : AtomicU64,         -- TxnId that holds the exclusive lock
}
```

**Acquire:** Hash `page_number`, probe linearly until finding a matching
entry or an empty slot. CAS `owner_txn` from 0 to the requesting TxnId.
If CAS fails (another transaction holds it), return `SQLITE_BUSY`.

**Release:** CAS `owner_txn` from owning TxnId to 0. If the slot is the
last in a probe chain, also clear `page_number` to allow reclamation.

This is simpler than the in-process sharded HashMap but provides the same
semantics: exclusive write locks per page, immediate failure on contention.

#### 5.6.4 Cross-Process SIREAD Plane

Full SIREAD tracking across processes is expensive (variable-size sets per
page). FrankenSQLite uses a **monotonic read filter** as a conservative
approximation:

```
SharedSireadFilter := {
    capacity  : u32,                  -- power-of-2 (default: 65536)
    entries   : [SireadEntry; capacity],
}

SireadEntry := {
    page_number    : AtomicU32,      -- 0 = empty slot
    min_reader_txn : AtomicU64,      -- oldest TxnId that has read this page
}
```

**On read:** Hash `page_number`, find or create entry. If `min_reader_txn`
is 0 or greater than the current TxnId, update it (atomic min).

**On write check:** When transaction `U` writes page `P`, check if
`siread_filter[P].min_reader_txn < U.txn_id`. If so, there exists at least
one transaction that read `P` before `U` wrote it -- a potential
rw-antidependency. Mark `U.has_in_rw = true` for the conservative SSI check.

**Properties:**
- No false negatives: if a read happened, the filter records it.
- Possible false positives: the filter may report a read that was already
  committed/aborted (stale entry). This is safe -- it only causes unnecessary
  aborts, never missed anomalies.
- Space-bounded: fixed-size hash table, O(1) operations.
- Cleanup: entries with `min_reader_txn < gc_horizon` can be cleared.

#### 5.6.5 GC Coordination

The `gc_horizon` in shared memory is updated by each process:

```
update_gc_horizon():
    local_min = min(txn_id for active local transactions)
    loop:
        current = shm.gc_horizon.load()
        // Only lower the horizon (never raise it without consensus)
        if local_min < current:
            shm.gc_horizon.compare_exchange(current, local_min)
            break
        else:
            break
```

Periodically, a process with no active transactions raises the horizon:
```
raise_gc_horizon():
    global_min = min(slot.txn_id for slot in txn_slots where slot.txn_id != 0)
    shm.gc_horizon.store(global_min)
```

#### 5.6.6 Compatibility: Falling Back to File Locks

When shared-memory coordination is not available (e.g., `foo.db.fsqlite-shm`
cannot be created due to filesystem restrictions), FrankenSQLite falls back
to C SQLite's file-level locking protocol:

- `WAL_WRITE_LOCK` for single-writer mutual exclusion
- Standard WAL reader marks for snapshot isolation
- No multi-writer MVCC, no SSI

This ensures FrankenSQLite works on any filesystem that supports advisory
file locks, degrading gracefully from multi-writer to single-writer.

### 5.7 SSI Algorithm Specification

Serializable Snapshot Isolation (SSI) extends Snapshot Isolation to detect and
prevent the write skew anomaly. SSI ships as the default isolation mode
for `BEGIN CONCURRENT` (Layer 2 of Section 2.4). This section provides the
full algorithm specification.

**Formal definition of SIREAD locks:**

An SIREAD lock records that a transaction has read a page under snapshot
isolation. Unlike exclusive write locks, SIREAD locks do not prevent other
transactions from reading or writing the page. They exist solely for
dependency tracking.

```
SireadLockTable := HashMap<PageNumber, SmallVec<TxnId>>
    -- Maps each page to the set of active transactions that have read it.
    -- Entries are cleaned up when transactions commit or abort.

SireadLock := (PageNumber, TxnId)
    -- "Transaction TxnId has read PageNumber under its snapshot."
```

When transaction `T` reads page `P`:
```
on_read(T, P):
    siread_table.entry(P).or_default().push(T.txn_id)
    T.read_set.insert(P)
```

When transaction `U` writes page `P`:
```
on_write(U, P):
    for each T_id in siread_table.get(P):
        if T_id != U.txn_id AND transaction(T_id).state == Active:
            // Create rw-antidependency edge: T ->rw U
            // "T read P before U wrote P"
            record_rw_edge(from=T_id, to=U.txn_id)
```

**The dangerous structure:**

SSI detects serialization anomalies by identifying "dangerous structures" --
patterns of rw-antidependency edges that imply a cycle in the serialization
graph.

The dangerous structure is two consecutive rw-antidependency edges:

```
T1 -rw-> T2 -rw-> T3
```

where:
- `T1` read something that `T2` later wrote (T1 -rw-> T2)
- `T2` read something that `T3` later wrote (T2 -rw-> T3)
- `T3` committed before `T1` in the serialization order

This implies a cycle: `T1` must precede `T2` (because `T1` did not see `T2`'s
write), `T2` must precede `T3` (same reason), but `T3` must precede `T1`
(because `T3` committed first and `T1` should have seen its write but did not,
due to snapshot isolation).

Formally, the dangerous structure exists when:
```
exists T1, T2, T3 :
    rw_edge(T1, T2) AND rw_edge(T2, T3)
    AND T2.has_incoming_rw AND T2.has_outgoing_rw
    AND (T1 committed OR T3 committed)
```

`T2` is called the **pivot** -- it sits in the middle of the two rw edges.

**Per-transaction state for SSI:**

```
Transaction (SSI extensions) := {
    ...existing fields...
    has_incoming_rw : bool,   -- some other transaction created a rw edge TO this txn
    has_outgoing_rw : bool,   -- some other transaction created a rw edge FROM this txn
    rw_in_from      : HashSet<TxnId>,  -- transactions that have rw edges to this txn
    rw_out_to       : HashSet<TxnId>,  -- transactions that this txn has rw edges to
}
```

**Detection algorithm pseudocode:**

```
on_commit(T):
    // T is about to commit. Check if T is a pivot in a dangerous structure.
    if T.has_incoming_rw AND T.has_outgoing_rw:
        // T is the pivot (T2 in T1 -rw-> T2 -rw-> T3).
        // Check if any T1 (source of incoming rw) and T3 (target of outgoing rw)
        // form a genuine dangerous structure.
        for T1_id in T.rw_in_from:
            T1 = transaction(T1_id)
            if T1.state == Committed OR T1.state == Active:
                for T3_id in T.rw_out_to:
                    T3 = transaction(T3_id)
                    if T3.state == Committed:
                        // Dangerous structure confirmed: T1 -rw-> T -rw-> T3
                        // T3 committed, T1 committed or still active.
                        // Must abort to prevent anomaly.
                        abort(T)
                        return Err(SQLITE_BUSY_SNAPSHOT)

    // Also check: is T the T3 in someone else's dangerous structure?
    // When T commits, it might complete a dangerous structure where
    // some active pivot T2 now has both incoming and outgoing rw edges
    // with committed endpoints.
    for T2_id in T.rw_in_from:
        T2 = transaction(T2_id)
        if T2.state == Active AND T2.has_incoming_rw AND T2.has_outgoing_rw:
            // T2 might be a pivot. But we only abort T2 at T2's commit time.
            // Mark T2 for eager abort (optimization: abort early rather than
            // waiting for T2 to attempt commit).
            T2.marked_for_abort = true

    // Passed SSI check. Proceed with normal commit.
    proceed_with_commit(T)
```

**When to abort T2 (the pivot) vs T3 (the unsafe):**

The abort victim selection policy balances false positive rate against
implementation complexity:

- **Conservative (PostgreSQL approach):** Abort the pivot (T2) when it
  attempts to commit, if it has both incoming and outgoing rw edges. This is
  simple but may abort transactions that would not actually cause an anomaly
  (false positives).

- **Optimistic:** Only abort if a genuine cycle can be demonstrated. This
  requires more bookkeeping (tracking commit order, verifying that the
  T1-T2-T3 chain actually implies a non-serializable execution) but has a
  lower false positive rate.

FrankenSQLite uses the conservative approach initially (matching PostgreSQL),
with the option to refine later.

**PostgreSQL's experience: false positive rate and overhead:**

Based on the PostgreSQL 9.1+ implementation (Ports, 2012):
- **False positive abort rate:** ~0.5% of transactions aborted unnecessarily
  under typical OLTP workloads. This is acceptable because the cost of a
  false positive (retry the transaction) is much lower than the cost of a
  missed anomaly (data corruption).
- **Overhead:** <7% throughput reduction compared to plain Snapshot Isolation,
  measured on TPC-C. The overhead comes from maintaining SIREAD locks and
  checking for dangerous structures.
- **Memory:** SIREAD lock table grows proportionally to the number of active
  transactions times pages read. Under PostgreSQL's row-level granularity,
  this can be significant; at page granularity, it is much smaller.

**How SSI maps to page granularity in FrankenSQLite:**

SSI at page granularity is coarser than PostgreSQL's row-level SSI. This means:
- **More false positives:** Two transactions that read and write different
  rows on the same page will appear to have an rw-antidependency even if
  they are logically independent. The false positive rate will be higher than
  PostgreSQL's 0.5%.
- **Less overhead:** Fewer SIREAD lock entries (one per page, not one per
  row). The SIREAD lock table is smaller and faster to scan.
- **Mitigation:** The algebraic write merging mechanism (Section 3.4.5) can
  refine page-level conflicts to byte-level, reducing false positives for
  the write side. For the read side, future work could add cell-level
  SIREAD tracking within B-tree pages.

**Interaction with BEGIN CONCURRENT:**

SSI is an enhancement to `BEGIN CONCURRENT` (Concurrent mode). When SSI is
enabled:
- `BEGIN CONCURRENT` provides SERIALIZABLE isolation (not just SI).
- Applications that previously tolerated write skew under SI will see
  occasional `SQLITE_BUSY_SNAPSHOT` aborts for transactions that would have
  produced non-serializable results.
- `BEGIN` / `BEGIN IMMEDIATE` / `BEGIN EXCLUSIVE` continue to use Serialized
  mode (global write mutex), which is trivially serializable and does not
  need SSI.

### 5.8 Conflict Detection and Resolution Detail

**Page lock table implementation:**

```rust
const LOCK_TABLE_SHARDS: usize = 64;  // power of two for fast modular arithmetic

pub struct PageLockTable {
    shards: [parking_lot::Mutex<HashMap<PageNumber, TxnId>>; LOCK_TABLE_SHARDS],
}

impl PageLockTable {
    fn shard(&self, pgno: PageNumber) -> &parking_lot::Mutex<HashMap<PageNumber, TxnId>> {
        &self.shards[pgno.get() as usize & (LOCK_TABLE_SHARDS - 1)]
    }

    /// Attempt to acquire exclusive lock on a page.
    /// Returns Ok(()) if acquired or already held by this txn.
    /// Returns Err(SQLITE_BUSY) if held by another txn.
    pub fn try_acquire(&self, pgno: PageNumber, txn_id: TxnId) -> Result<()> {
        let mut table = self.shard(pgno).lock();
        match table.entry(pgno) {
            Entry::Vacant(e) => {
                e.insert(txn_id);
                Ok(())
            }
            Entry::Occupied(e) => {
                if *e.get() == txn_id {
                    Ok(())  // idempotent re-acquire
                } else {
                    Err(FrankenError::Busy)  // held by another transaction
                }
            }
        }
    }

    /// Release a page lock. Panics if not held by this txn.
    pub fn release(&self, pgno: PageNumber, txn_id: TxnId) {
        let mut table = self.shard(pgno).lock();
        match table.entry(pgno) {
            Entry::Occupied(e) if *e.get() == txn_id => {
                e.remove();
            }
            _ => panic!("releasing lock not held by txn {}", txn_id),
        }
    }

    /// Release all locks held by a transaction.
    /// Iterates the per-transaction lock set, touching only relevant shards.
    pub fn release_all(&self, locks: &HashSet<PageNumber>, txn_id: TxnId) {
        // Group by shard to minimize lock acquisitions
        for pgno in locks {
            let mut table = self.shard(*pgno).lock();
            if let Entry::Occupied(e) = table.entry(*pgno) {
                if *e.get() == txn_id {
                    e.remove();
                }
            }
        }
    }
}
```

Note: `release_all` iterates the per-transaction lock set (typically tens
of entries), not the entire lock table (which could be thousands of entries
under high concurrency). This is O(W) where W is the transaction's write
set size, not O(L) where L is total locked pages.

**Commit validation algorithm:**

The first-committer-wins check determines whether any page in the committing
transaction's write set was also modified by a transaction that committed
after the snapshot was taken.

```
validate_commit(T, commit_log) -> Result<()>:
    // Determine which commits happened "after" our snapshot.
    // A commit is "after" our snapshot if:
    //   (a) its txn_id > our snapshot.high_water_mark, OR
    //   (b) its txn_id was in our snapshot.in_flight (it was active when
    //       we started but has since committed)
    //
    // We must check both categories.

    // Category (a): scan commit_log for entries above our high_water_mark
    for (committed_txn_id, record) in commit_log.range(T.snapshot.hwm + 1 ..):
        if pages_overlap(T.write_set.keys(), &record.pages):
            let conflicting_page = find_overlap(T.write_set.keys(), &record.pages);
            // Attempt algebraic merge
            if algebraic_merge_possible(T, committed_txn_id, conflicting_page):
                perform_merge(T, committed_txn_id, conflicting_page);
            else:
                return Err(SERIALIZATION_FAILURE)

    // Category (b): check in_flight transactions that have since committed
    for in_flight_txn_id in T.snapshot.in_flight:
        if let Some(record) = commit_log.get(in_flight_txn_id):
            // This transaction was active when we started but has since committed
            if pages_overlap(T.write_set.keys(), &record.pages):
                let conflicting_page = find_overlap(T.write_set.keys(), &record.pages);
                if algebraic_merge_possible(T, in_flight_txn_id, conflicting_page):
                    perform_merge(T, in_flight_txn_id, conflicting_page);
                else:
                    return Err(SERIALIZATION_FAILURE)

    Ok(())  // no conflicts, commit proceeds
```

**Interaction between Serialized and Concurrent mode transactions:**

When a Serialized-mode transaction is active (holding the global write mutex),
Concurrent-mode transactions can still:
- **Begin:** Capturing a snapshot does not require the write mutex.
- **Read:** Reads never require locks and proceed normally.
- **Write (to their own write set):** Page lock acquisition proceeds normally.
  The global write mutex does not interact with page locks.
- **Commit:** Here is the interaction point. The commit pathway requires
  sending a `CommitRequest` to the write coordinator and appending to the WAL.
  The write coordinator processes requests sequentially. If the Serialized
  transaction is currently in its commit phase (holding the coordinator's
  attention), the Concurrent transaction's commit request waits in the MPSC
  channel until the Serialized transaction's commit completes.

Conversely, when a Serialized-mode transaction attempts to begin while
Concurrent-mode transactions hold page locks:
- The Serialized transaction acquires the global write mutex (preventing other
  Serialized transactions).
- It does NOT wait for Concurrent transactions to release their page locks.
- Instead, it proceeds with its own writes. Since Serialized mode does not
  use page locks (it relies on the global mutex for exclusion against other
  Serialized transactions), there is no direct conflict with Concurrent
  transactions' page locks.
- At commit time, both the Serialized and Concurrent transactions go through
  the write coordinator. The coordinator serializes their WAL appends,
  maintaining a consistent WAL.

**What happens when a Serialized transaction and a Concurrent transaction
write the same page:**

The Serialized transaction does not acquire page locks, so no `SQLITE_BUSY`
occurs during the write phase. The conflict is detected at Concurrent
transaction's commit time via the first-committer-wins validation. If the
Serialized transaction committed first, the Concurrent transaction sees the
conflict and aborts. If the Concurrent transaction committed first, the
Serialized transaction (which does not perform validation since it holds the
global mutex) simply overwrites the page -- its version has a higher TxnId
and will be visible to future snapshots that see both commits.

### 5.9 Write Coordinator Detail

The write coordinator is a single background task that serializes the commit
pipeline: validation, WAL append, version publishing, and commit log insertion.

**Full state machine for the coordinator:**

```
                     +-------+
            +------->| Idle  |<----------+
            |        +-------+           |
            |            |               |
            |  recv(CommitRequest)       |
            |            |               |
            |            v               |
            |      +-----------+         |
            |      | Validate  |         |
            |      +-----------+         |
            |        |       |           |
            |   pass |       | fail      |
            |        v       v           |
            |  +---------+  +-------+    |
            |  |WALAppend|  | Abort |----+
            |  +---------+  +-------+
            |        |          ^
            |   sync |          | I/O error
            |        v          |
            |   +---------+    |
            |   | Publish |--->+
            |   +---------+
            |        |
            |  respond(Ok)
            |        |
            +--------+

States:
  Idle:       Waiting for next CommitRequest on MPSC channel.
  Validate:   Running first-committer-wins check on the request's write set.
  WALAppend:  Writing page frames + repair symbols to WAL file.
  Publish:    Inserting versions into version store and commit record into commit log.
  Abort:      Notifying the requester of failure; cleaning up partial state.
```

**CommitRequest and CommitResponse types:**

```rust
/// Sent by a committing transaction to the write coordinator.
pub struct CommitRequest {
    /// Transaction ID of the committing transaction.
    pub txn_id: TxnId,
    /// Transaction mode (Serialized or Concurrent).
    pub mode: TxnMode,
    /// Pages to be committed: page number -> new page data.
    pub write_set: HashMap<PageNumber, PageData>,
    /// Intent log for deterministic rebase merge (Section 5.10).
    pub intent_log: Vec<IntentOp>,
    /// Page locks held (for release after commit).
    pub page_locks: HashSet<PageNumber>,
    /// Snapshot of the committing transaction (for validation).
    pub snapshot: Snapshot,
    /// SSI state: has_in_rw and has_out_rw flags (pre-checked by caller,
    /// but coordinator may re-validate if needed).
    pub has_in_rw: bool,
    pub has_out_rw: bool,
    /// Pre-computed RaptorQ repair symbols for the write set.
    pub repair_symbols: Vec<RepairSymbol>,
    /// Oneshot channel for the coordinator's response.
    pub response_tx: oneshot::Sender<CommitResponse>,
}

/// Sent by the write coordinator back to the committing transaction.
pub enum CommitResponse {
    /// Commit succeeded. All versions published, WAL synced.
    Ok {
        /// WAL offset where the commit record was written.
        wal_offset: u64,
        /// Commit sequence number (monotonically increasing).
        commit_seq: u64,
    },
    /// Commit failed due to a page conflict.
    Conflict {
        /// The page(s) that conflicted.
        conflicting_pages: Vec<PageNumber>,
        /// The transaction that already committed the conflicting page(s).
        conflicting_txn: TxnId,
    },
    /// Commit failed due to an I/O error during WAL append.
    IoError {
        error: FrankenError,
    },
}
```

**Throughput model with derivation:**

The coordinator processes commits sequentially. Each commit involves:

1. **Validation**: Scan the commit log for conflicts.
   - Cost: O(W * C) where W = write set size, C = commits since snapshot.
   - Typical: W = 10 pages, C = 10 concurrent commits. Each check is a
     hash lookup: ~50ns. Total: 10 * 10 * 50ns = 5us.
   - Let `T_validate` denote this cost.

2. **WAL append**: Write page frames + repair symbols sequentially.
   - Cost: W frames * (24 bytes header + page_size data) + R repair frames.
   - Typical: 10 frames * 4120 bytes = ~40KB sequential write.
   - SSD sequential write throughput: ~2 GB/s. Time: 40KB / 2GB/s = 20us.
   - Plus fsync: ~50us on modern NVMe (group commit amortizes this).
   - Let `T_wal` denote this cost.

3. **Version publishing + commit log**: In-memory operations.
   - Cost: O(W) hash insertions. Typical: 10 * 100ns = 1us.
   - Let `T_publish` denote this cost.

Total per-commit latency:
```
T_commit = T_validate + T_wal + T_publish
         = 5us + 70us + 1us
         = ~76us
```

Throughput (single coordinator, no batching):
```
Throughput = 1 / T_commit = 1 / 76us ~ 13,000 commits/sec
```

With group commit batching (amortize fsync across N concurrent commits):
```
T_commit_batched = T_validate + T_wal_write + T_fsync/N + T_publish
                 = 5us + 20us + 50us/N + 1us

For N = 10:  T_commit = 31us  -> Throughput ~ 32,000 commits/sec
For N = 50:  T_commit = 27us  -> Throughput ~ 37,000 commits/sec
```

**Batching optimization: coalescing multiple commits into a single WAL sync:**

The coordinator implements group commit to amortize the fsync cost:

```
Coordinator main loop (with batching):

loop:
    // Drain all available requests (non-blocking after first)
    batch = Vec::new()
    first_request = commit_channel.recv().await   // blocking wait for first
    batch.push(first_request)

    // Drain additional pending requests (non-blocking)
    while let Ok(request) = commit_channel.try_recv():
        batch.push(request)
        if batch.len() >= MAX_BATCH_SIZE:
            break

    // Phase 1: Validate all requests in the batch
    valid = Vec::new()
    for request in batch:
        match validate(request):
            Ok(()) => valid.push(request),
            Err(conflict) => request.response_tx.send(CommitResponse::Conflict(conflict)),

    // Phase 2: Append all valid commits to WAL (one sequential write)
    wal_offsets = wal.append_batch(&valid)   // single write() call for all frames

    // Phase 3: Single fsync for the entire batch
    wal.sync()

    // Phase 4: Publish all versions and respond
    for (request, offset) in valid.iter().zip(wal_offsets):
        publish_versions(request)
        insert_commit_record(request)
        request.response_tx.send(CommitResponse::Ok { wal_offset: offset, ... })
```

The batching optimization transforms the throughput model from:

```
Without batching:  N commits * (T_write + T_fsync) = N * 70us
With batching:     N * T_write + 1 * T_fsync = N * 20us + 50us
```

For a batch of 10 commits: 250us total vs 700us, a 2.8x improvement. The
larger the batch (more concurrent committers), the greater the amortization
benefit. This is the standard group commit optimization used by PostgreSQL,
MySQL InnoDB, and other production databases.

**Interaction with the two-phase MPSC channel:**

The write coordinator receives `CommitRequest` messages from the MPSC channel's
receiver end (`rx`). The bounded capacity of the channel (default: 16) provides
natural batching: when the coordinator is busy processing a batch, new commit
requests accumulate in the channel buffer. When the coordinator finishes and
calls `try_recv()` to drain pending requests, it collects all buffered requests
into the next batch.

If the channel buffer fills up (16 in-flight commits), additional committers
block on `tx.reserve(cx).await`, which provides backpressure. This prevents
unbounded memory growth from write set buffering and naturally rate-limits
the commit pipeline when the WAL I/O is the bottleneck.

### 5.10 Algebraic Write Merging and Intent Logs

Page-level MVCC can conflict on hot pages (B-tree root, internal nodes during
splits, hot leaf pages). Algebraic Write Merging reduces false conflicts
**without** upgrading to row-level MVCC metadata (which would break file format
and cost space).

**The insight:** Many "same-page conflicts" in B-tree workloads involve
logically independent operations (e.g., two inserts into distinct keys that
happen to land on the same leaf page). Instead of treating these as fatal
conflicts, we attempt to **merge** them.

**Two merge planes:**

1. **Logical plane (preferred):** Merge *intent-level* B-tree operations that
   commute (e.g., inserts into distinct keys).
2. **Physical plane (fallback):** Merge *byte-level* patches when we can prove
   disjointness + invariant preservation.

#### 5.10.1 Intent Logs (Semantic Operations)

Each writing transaction records an `intent_log: Vec<IntentOp>` alongside its
materialized page deltas. Intent operations are:

```
IntentOp ::=
  | Insert { table: TableId, key: RowId, record: Vec<u8> }
  | Delete { table: TableId, key: RowId }
  | Update { table: TableId, key: RowId, new_record: Vec<u8> }
  | IndexInsert { index: IndexId, key: Vec<u8>, rowid: RowId }
  | IndexDelete { index: IndexId, key: Vec<u8>, rowid: RowId }
```

Intent logs are *small* (typically tens of entries) and encode/replicate
efficiently as ECS objects. They are the preferred merge substrate because
they carry semantic information that byte-level patches lack.

#### 5.10.2 Deterministic Rebase (The Big Win)

When a txn `U` reaches commit and discovers a page in `write_set(U)` has been
updated since its snapshot, we attempt **deterministic rebase**:

1. **Detect base drift:** `base_version(pgno)` for U's write set changed since
   its snapshot.
2. **Attempt rebase:** Take U's intent log and replay it against the *current*
   committed snapshot, producing new page deltas.
3. **If replay succeeds** without violating B-tree invariants or constraints:
   commit proceeds with the rebased page deltas.
4. **If replay fails** (true conflict, constraint violation): abort/retry.

This is "merge by re-execution", not "merge by bytes". It gives us *row-level
concurrency effects* without storing row-level MVCC metadata.

**Determinism requirement:** The replay engine MUST be deterministic for a
given `(intent_log, base_snapshot)`. Under `LabRuntime`, identical inputs yield
identical outputs across all seeds. No dependence on wall-clock, iteration
order, or hash randomization.

#### 5.10.3 Physical Merge: GF(256) Sparse XOR Patches

Physical merge is the fallback for tiny, local, obviously-disjoint changes.

A page is a vector `p ∈ GF(256)^n`. A sparse XOR patch `Δ` has support in a
set of byte ranges. Apply: `p' = p ⊕ Δ`.

Merge condition:
```
disjoint(ΔA, ΔB) := support(ΔA) ∩ support(ΔB) = ∅
merge(ΔA, ΔB) := ΔA ⊕ ΔB
```

When disjoint, merges commute and associate. This gives merge without ordering
assumptions and a clean algebra that matches the RaptorQ coding field.

**StructuredPagePatch** refines byte disjointness to account for structural
B-tree metadata:

```
StructuredPagePatch {
  header_ops: Vec<HeaderOp>,         -- serialized (not merged)
  cell_ops: Vec<CellOp>,            -- mergeable when disjoint by cell_key
  free_ops: Vec<FreeSpaceOp>,       -- default: conflict; future: structured merge
  raw_xor_ranges: Vec<RangeXorPatch>, -- escape hatch (debug only)
}
```

`cell_ops` are keyed by a stable identifier (`cell_key_digest` derived from
rowid/index key), not by raw offsets. This enables safe merges even when the
page layout shifts during a concurrent split.

#### 5.10.4 Commit-Time Merge Policy (Strict Safety Ladder)

When txn `U` reaches commit, for each page in `write_set(U)`:

1. If base unchanged since snapshot → OK (no merge needed).
2. Else, attempt merge in strict priority order:
   a. **Deterministic rebase replay** (preferred: semantic, highest success rate)
   b. **Structured page patch merge** (if ops are cell-disjoint)
   c. **Sparse XOR merge** (only if ranges are declared merge-safe)
   d. **Abort/retry** (no safe merge found)

This yields a strict safety ladder: we only take merges we can justify.

#### 5.10.5 What Must Be Proven

Runnable proofs (proptest + DPOR), not prose:

- **B-tree invariants** hold after replay/merge: ordering, cell count bounds,
  free space accounting, overflow chain validity.
- **Patch algebra invariants:** `apply(p, merge(a,b)) == apply(apply(p,a), b)`
  when mergeable. Commutativity for declared commutative ops.
- **Determinism:** Identical `(intent_log, base_snapshot)` yields identical
  replay outcome under `LabRuntime` across seeds.

#### 5.10.6 MVCC History Compression: PageHistory Objects

Storing full page images per version is not acceptable long-term:

- **Newest committed version:** full page image (for fast reads).
- **Older versions:** patches (intent logs and/or structured patches).
- **Hot pages:** Encode patch chains as ECS **PageHistory objects** so history
  itself is repairable and remote replicas can fetch "just enough symbols" to
  reconstruct a needed historical version.

This is how MVCC avoids eating memory under real write concurrency.

---

## 6. Buffer Pool: ARC Cache

### 6.1 Why ARC, Not LRU

LRU fails catastrophically for database workloads: a single table scan evicts
the entire working set. ARC (Adaptive Replacement Cache, Megiddo & Modha,
FAST '03) auto-tunes between recency and frequency, maintaining a provable
competitive ratio of 2 against OPT.

ARC's advantage over LRU is not marginal -- it is structural. Consider three
canonical database access patterns:

1. **Scan-then-point**: A reporting query scans an entire table (touching every
   page once), followed by OLTP point queries on a hot set of 100 pages. Under
   LRU, the scan evicts all 100 hot pages. Under ARC, the scan pages enter T1
   but never promote to T2; the hot pages remain in T2 untouched.

2. **Frequency skew**: 10% of pages receive 90% of accesses (Zipfian). LRU
   cannot distinguish between a page accessed once recently and one accessed
   1000 times. ARC promotes frequently-accessed pages to T2, protecting them
   from recency-only eviction.

3. **Loop patterns**: A query repeatedly scans a working set slightly larger
   than cache. LRU achieves 0% hit rate (every access is a miss). ARC detects
   the looping pattern via ghost hits in B1 and adjusts p to retain a portion
   of the loop, achieving partial hit rate.

### 6.2 MVCC-Aware ARC Data Structures

Standard ARC keys on page number. Our variant keys on `(PageNumber, TxnId)`
because multiple versions coexist.

```rust
/// Cache key: MVCC-aware page identity.
/// Multiple versions of the same page coexist when concurrent transactions
/// hold different snapshots. TxnId::ZERO represents the on-disk baseline.
#[derive(Clone, Copy, Hash, Eq, PartialEq)]
pub struct CacheKey {
    pub pgno: PageNumber,
    pub version_id: TxnId,
}

/// A cached page with metadata for eviction decisions.
pub struct CachedPage {
    pub key: CacheKey,
    pub data: PageData,
    pub ref_count: AtomicU32,     // pinned by active operations
    pub dirty: AtomicBool,        // modified but not yet flushed to WAL
    pub xxh3: Xxh3Hash,           // integrity hash of data at load time
    pub byte_size: usize,         // actual memory (for variable-size deltas)
    pub wal_frame: Option<u32>,   // WAL frame number if from WAL
}

/// The MVCC-aware ARC cache.
pub struct ArcCache {
    /// T1: pages accessed exactly once recently (recency-favored).
    t1: LinkedHashMap<CacheKey, Arc<CachedPage>>,
    /// T2: pages accessed two or more times recently (frequency-favored).
    t2: LinkedHashMap<CacheKey, Arc<CachedPage>>,
    /// B1: ghost entries evicted from T1 (metadata only, no page data).
    b1: LinkedHashSet<CacheKey>,
    /// B2: ghost entries evicted from T2 (metadata only, no page data).
    b2: LinkedHashSet<CacheKey>,
    /// Adaptive parameter: target size for T1. Range [0, capacity].
    p: usize,
    /// Maximum number of pages in T1 + T2 combined.
    capacity: usize,
    /// Total bytes consumed by cached page data (for memory accounting).
    total_bytes: usize,
    /// Maximum bytes allowed (derived from PRAGMA cache_size).
    max_bytes: usize,
}
```

**Eviction constraints:**
1. Never evict a pinned page (`ref_count > 0`)
2. Never evict a dirty page (must flush to WAL first)
3. Prefer superseded versions (newer committed version exists and is visible
   to all active snapshots)

### 6.3 Full ARC Algorithm: REPLACE Subroutine

The REPLACE subroutine selects a victim page for eviction. It chooses between
T1 and T2 based on the adaptive parameter p and a tie-breaking rule when the
target key was found in B2.

```
REPLACE(cache, target_key):
  // target_key is the page that triggered this replacement (for tie-breaking)
  loop:
    if |T1| > 0 AND (|T1| > p OR (|T1| == p AND target_key IN B2)):
      // Evict the LRU page of T1 (recency list)
      candidate = T1.front()
      if candidate.ref_count > 0:
        T1.rotate_front_to_back()  // skip pinned; try next
        continue
      if candidate.dirty:
        flush_to_wal(candidate)    // must persist before eviction
      (evicted_key, evicted_page) = T1.pop_front()
      B1.push_back(evicted_key)    // remember in ghost list
      total_bytes -= evicted_page.byte_size
      return
    else:
      // Evict the LRU page of T2 (frequency list)
      if |T2| == 0: panic("cache underflow: no evictable pages")
      candidate = T2.front()
      if candidate.ref_count > 0:
        T2.rotate_front_to_back()
        continue
      if candidate.dirty:
        flush_to_wal(candidate)
      (evicted_key, evicted_page) = T2.pop_front()
      B2.push_back(evicted_key)
      total_bytes -= evicted_page.byte_size
      return

    // Safety: if we have rotated through ALL entries in both T1 and T2
    // and every page is pinned, we are overcommitted. Allow temporary
    // growth beyond capacity rather than deadlock.
    if rotations >= |T1| + |T2|:
      capacity_overflow += 1
      return  // caller inserts without evicting
```

### 6.4 Full ARC Algorithm: REQUEST Subroutine

```
REQUEST(cache, key: CacheKey) -> Result<Arc<CachedPage>>:

  // Case I: Cache hit in T1 or T2
  if key IN T1:
    page = T1.remove(key)
    T2.push_back(key, page)       // promote to frequency list
    page.ref_count.fetch_add(1)
    return Ok(page)

  if key IN T2:
    page = T2.move_to_back(key)   // refresh MRU position
    page.ref_count.fetch_add(1)
    return Ok(page)

  // Case II: Ghost hit in B1 (recently evicted from T1)
  if key IN B1:
    // Evidence that T1 is too small. Increase p to favor recency.
    delta = max(1, |B2| / |B1|)
    p = min(p + delta, capacity)
    REPLACE(cache, key)
    B1.remove(key)
    page = fetch_from_storage(key.pgno, key.version_id)
    T2.push_back(key, page)       // enters T2 (second lifetime access)
    total_bytes += page.byte_size
    page.ref_count.fetch_add(1)
    return Ok(page)

  // Case III: Ghost hit in B2 (recently evicted from T2)
  if key IN B2:
    // Evidence that T2 is too small. Decrease p to favor frequency.
    delta = max(1, |B1| / |B2|)
    p = max(p.saturating_sub(delta), 0)
    REPLACE(cache, key)
    B2.remove(key)
    page = fetch_from_storage(key.pgno, key.version_id)
    T2.push_back(key, page)
    total_bytes += page.byte_size
    page.ref_count.fetch_add(1)
    return Ok(page)

  // Case IV: Complete miss (not in T1, T2, B1, or B2)
  let L1 = |T1| + |B1|
  let L2 = |T2| + |B2|

  if L1 == capacity:
    if |T1| < capacity:
      B1.pop_front()              // discard oldest ghost from B1
      REPLACE(cache, key)
    else:
      // T1 is full, B1 is empty. Evict LRU of T1 directly.
      evict_lru_t1(cache)
  else if L1 < capacity AND L1 + L2 >= capacity:
    if L1 + L2 >= 2 * capacity:
      B2.pop_front()              // discard oldest ghost from B2
    REPLACE(cache, key)
  // else: cache has room, no eviction needed

  page = fetch_from_storage(key.pgno, key.version_id)
  T1.push_back(key, page)         // new pages always enter T1
  total_bytes += page.byte_size
  page.ref_count.fetch_add(1)
  return Ok(page)
```

**Complexity:** Each cache operation is O(1) amortized. Ghost lists consume
only 12 bytes per entry (CacheKey), so maintaining them at `capacity` entries
each has negligible memory overhead compared to the page data.

### 6.5 MVCC Adaptation: (PageNumber, TxnId) Keying with Ghost Lists

**Ghost list semantics change.** When a ghost entry `(pgno, old_txn_id)` is
in B1 and a request arrives for `(pgno, new_txn_id)`, this is NOT a ghost
hit -- it is a different version. Ghost hits only occur on exact
`(pgno, txn_id)` match. This is correct because different versions have
genuinely different access patterns.

**Version coalescing in ghost lists.** Ghost lists may accumulate many entries
for the same page number with different TxnId values. To bound ghost list size,
when the GC horizon advances, prune ghost entries whose version_id is below the
new horizon:

```
prune_ghosts(cache, gc_horizon: TxnId):
  B1.retain(|k| k.version_id >= gc_horizon)
  B2.retain(|k| k.version_id >= gc_horizon)
```

**Capacity accounting.** Each `(pgno, txn_id)` pair counts as one entry. A
heavily-versioned page consumes multiple cache slots. Under high write
contention, the effective number of distinct pages cached decreases. This is
correct: the cache prioritizes versions actively needed over breadth.

### 6.6 Eviction: Pinned Pages and Dirty Flush Protocol

**All pages pinned scenario.** If REPLACE scans all of T1 and T2 without
finding an unpinned, clean page, the cache is overcommitted. Resolution:

1. Temporarily grow capacity by 1 (`capacity_overflow += 1`).
2. Log a warning: the application has too many concurrent pinned pages.
3. On the next `unpin()` call, decrement `capacity_overflow` and trigger
   eviction if needed.

This is a safety valve, not the normal path. In practice, pinned page count
is bounded by `(concurrent_cursors * max_btree_depth)`, which is typically
under 200 even for heavy workloads.

**Dirty page flush protocol:**

```rust
fn flush_dirty_page(page: &CachedPage, wal: &WalWriter) -> Result<()> {
    // Atomically claim the flush to prevent double-flush from concurrent evictors
    if page.dirty.compare_exchange(true, false, SeqCst, SeqCst).is_ok() {
        match wal.write_frame(page.key.pgno, &page.data) {
            Ok(()) => Ok(()),
            Err(e) => {
                // WAL write failed (e.g., disk full). Restore dirty flag.
                page.dirty.store(true, SeqCst);
                Err(e)
            }
        }
    } else {
        Ok(()) // already flushed by another thread
    }
}
```

### 6.7 MVCC Version Coalescing

When a newer committed version of a page is visible to ALL active snapshots,
older versions are reclaimable. The cache proactively drops them.

**Coalescing triggers:**
- During REPLACE (opportunistic: check if candidate is superseded)
- After GC horizon advances (batch scan)
- On `PRAGMA shrink_memory`

```
coalesce_versions(cache, pgno, gc_horizon):
  versions = all cached entries where key.pgno == pgno
  sort versions by version_id descending

  kept_committed = false
  for key in versions:
    if key.version_id <= gc_horizon AND is_committed(key.version_id):
      if !kept_committed:
        kept_committed = true   // keep newest committed below horizon
        continue
      // Superseded: remove if not pinned
      if let Some(page) = remove_from_t1_or_t2(key):
        if page.ref_count == 0:
          total_bytes -= page.byte_size
          // Do NOT add to ghost list (version is permanently dead)
        else:
          re_insert(key, page)  // pinned; try again later
```

### 6.8 Bloom Filter Integration for Snapshot Visibility

Each `Snapshot` includes a Bloom filter over its `in_flight` set for O(1)
amortized visibility checks during version chain traversal.

**Parameters:**

```
Given n = |in_flight|, target false positive rate epsilon = 0.01:
    m = ceil(-n * ln(0.01) / (ln(2))^2) bits
    k = round((m / n) * ln(2)) hash functions

Concrete:
    n=10:   m=96 bits (12 bytes),     k=7
    n=50:   m=479 bits (60 bytes),    k=7
    n=100:  m=959 bits (120 bytes),   k=7
    n=500:  m=4793 bits (600 bytes),  k=7
    n=1000: m=9586 bits (~1.2 KB),    k=7
```

**Hash function:** Kirsch-Mitzenmacher double hashing with XXH3:
```
h_i(x) = (xxh3_64(x, seed=0) + i * xxh3_64(x, seed=1)) mod m
```

**Skip threshold:** If `in_flight.len() < 8`, use direct binary search on the
sorted `in_flight` vector (faster than 7 hash computations for tiny sets).

**Visibility fast path:**

```rust
fn is_visible(&self, version_id: TxnId, snapshot: &Snapshot) -> bool {
    if version_id > snapshot.high_water_mark {
        return false;
    }
    let maybe_in_flight = if snapshot.in_flight.len() < 8 {
        snapshot.in_flight.binary_search(&version_id).is_ok()
    } else if snapshot.bloom.might_contain(version_id) {
        // Bloom positive (could be false positive); do exact check
        snapshot.in_flight.binary_search(&version_id).is_ok()
    } else {
        false  // Bloom negative: definitely not in-flight
    };
    !maybe_in_flight && self.commit_log.is_committed(version_id)
}
```

### 6.9 Memory Accounting

The cache tracks total byte consumption, not just page count, because MVCC
version chain compression (RaptorQ deltas, Section 3.4.4) produces
variable-size entries. A full page = 4096 bytes; a RaptorQ delta may be 200.

**Dual eviction trigger:** Eviction fires when EITHER page count exceeds
capacity OR `total_bytes` exceeds `max_bytes`. This prevents memory exhaustion
when many full-size pages are cached alongside compact deltas.

```rust
fn should_evict(&self) -> bool {
    (self.t1.len() + self.t2.len() > self.capacity)
        || (self.total_bytes > self.max_bytes)
}
```

### 6.10 Configuration: PRAGMA cache_size Mapping

```
PRAGMA cache_size = N:
    if N > 0:
        cache.capacity = N
        cache.max_bytes = N * page_size
    if N < 0:
        cache.max_bytes = |N| * 1024    // |N| KiB
        cache.capacity = max(10, cache.max_bytes / page_size)
    if N == 0:
        cache.capacity = 2000           // SQLite default
        cache.max_bytes = 2000 * page_size
```

**Default:** 2000 pages (8 MiB for 4096-byte pages). Ghost lists limited to
`capacity` entries each (~24 KB overhead for 2000 entries).

**Resize protocol (runtime change):**
1. Set new capacity and max_bytes.
2. If `|T1| + |T2| > new_capacity`: repeatedly call REPLACE until within
   limits (dirty pages are flushed during this process).
3. Trim ghost lists: `B1.truncate(new_capacity)`, `B2.truncate(new_capacity)`.
4. Clamp p to `[0, new_capacity]`.

### 6.11 Performance Analysis

| Workload | P (pages) | W (hot) | C (cache) | H (LRU) | H (ARC) |
|----------|-----------|---------|-----------|---------|---------|
| OLTP point queries | 100K | 500 | 2000 | 0.96 | 0.97 |
| Mixed OLTP + scan | 100K | 500 | 2000 | 0.60 | 0.85 |
| Full table scan | 100K | 100K | 2000 | 0.02 | 0.02 |
| Zipfian (s=1.0) | 100K | N/A | 2000 | 0.82 | 0.89 |
| MVCC 8 writers | 100K | 800 | 2000 | 0.55 | 0.78 |

ARC's advantage is most pronounced in mixed workloads. The T2 list protects
frequently-accessed pages from scan pollution. Under MVCC with multiple
writers, ARC naturally separates hot current versions (T2) from cold
superseded versions (evicted or coalesced).

### 6.12 Warm-Up Behavior

**Phase 1 -- Cold start (0 to ~50% full):** All misses. p=0. No adaptation.

**Phase 2 -- Learning (~50-100% full):** First evictions. Ghost lists populate.
p adapts toward workload. Hit rate climbs 20-60%.

**Phase 3 -- Steady state (full):** p converged. Hit rate at expected value.
Reached after approximately 3x capacity accesses.

**Pre-warming (optional, `PRAGMA cache_warm = ON`):** On database open, read
pages referenced in WAL index into T1 (limited to half capacity). Also read
root pages of all tables/indexes from sqlite_master.

---

## 7. Checksums and Integrity

### 7.1 SQLite Native Checksum Algorithm

The WAL uses a custom 64-bit checksum (two u32 accumulators) for frame
integrity. This must be implemented exactly for file format compatibility.

**Algorithm (from wal.c):**

```rust
/// Compute SQLite WAL checksum, chaining from (s1_init, s2_init).
/// `native_cksum` is true if WAL magic is 0x377f0682 on big-endian
/// or 0x377f0683 on little-endian (i.e., checksums use native byte order).
pub fn wal_checksum(
    data: &[u8],
    s1_init: u32,
    s2_init: u32,
    native_cksum: bool,
) -> (u32, u32) {
    assert!(data.len() % 8 == 0);
    let mut s1 = s1_init;
    let mut s2 = s2_init;

    for chunk in data.chunks_exact(8) {
        let (a, b) = if native_cksum {
            (
                u32::from_ne_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]),
                u32::from_ne_bytes([chunk[4], chunk[5], chunk[6], chunk[7]]),
            )
        } else {
            (
                u32::from_ne_bytes([chunk[3], chunk[2], chunk[1], chunk[0]]),
                u32::from_ne_bytes([chunk[7], chunk[6], chunk[5], chunk[4]]),
            )
        };
        s1 = s1.wrapping_add(a).wrapping_add(s2);
        s2 = s2.wrapping_add(b).wrapping_add(s1);
    }
    (s1, s2)
}
```

**Endianness determination from WAL magic:**
- `0x377f0682`: big-endian word order within each 8-byte chunk
- `0x377f0683`: little-endian word order within each 8-byte chunk

FrankenSQLite writes WAL files using native byte order for performance.

**Cumulative chaining:** Each frame's checksum chains from the previous:
```
WAL header checksum = wal_checksum(header[0..24], 0, 0, native)
Frame 0 checksum = wal_checksum(frame0_hdr[0..8] ++ page0_data, hdr_s1, hdr_s2, native)
Frame N checksum = wal_checksum(frameN_hdr[0..8] ++ pageN_data, s1_{N-1}, s2_{N-1}, native)
```

This creates a hash chain: modifying any frame invalidates all subsequent
checksums, detecting both random corruption and truncation.

### 7.2 XXH3 Integration

For internal integrity checks not requiring WAL format compatibility,
FrankenSQLite uses XXH3-128 from `xxhash-rust`. Throughput: ~50 GB/s on
x86-64 with AVX2 (~80ns per 4096-byte page).

**Storage:**

```rust
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct Xxh3Hash {
    pub low: u64,
    pub high: u64,
}

impl Xxh3Hash {
    pub fn compute(data: &[u8]) -> Self {
        let h = xxhash_rust::xxh3::xxh3_128(data);
        Self { low: h as u64, high: (h >> 64) as u64 }
    }
    pub fn verify(&self, data: &[u8]) -> bool { *self == Self::compute(data) }
}
```

**Where XXH3 is used:**

1. **Buffer pool:** On page read from disk, compute and store XXH3-128.
   Reverify on `get_page()` from cache when `PRAGMA integrity_check_cache = ON`.
2. **MVCC version chain:** Each PageVersion carries an XXH3-128 hash.
3. **Checkpoint:** Verify XXH3 before writing page from WAL to database file.
4. **PRAGMA integrity_check:** Full verification of all pages.

**Collision probability:** 2^-128 (~3e-39). For non-adversarial corruption
detection this is vastly sufficient.

### 7.3 CRC-32C for RaptorQ

RaptorQ repair symbols carry CRC-32C checksums (4-byte overhead per symbol).

**Hardware acceleration:** CRC-32C has native instruction support on:
- x86-64: SSE4.2 `crc32` instruction (~20 GB/s)
- ARM: ACLE CRC extension `__crc32cd` instruction (~15 GB/s)
- Software fallback: table-based Sarwate algorithm (~2 GB/s)

**Detection strategy:**

```rust
/// CRC-32C computation. The crc32fast crate auto-detects hardware support
/// at runtime via CPUID/feature probing and selects the fastest path.
/// The crate uses unsafe internally for SIMD intrinsics, but our workspace
/// only forbids unsafe in *our* crates, not dependencies.
pub fn crc32c(data: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(data);
    hasher.finalize()
}
```

**Verification point:** CRC-32C is checked on each repair symbol BEFORE
passing it to the RaptorQ decoder. A corrupted symbol with valid CRC-32C has
probability ~2^-32 of going undetected (adequate for repair symbols that are
themselves redundant).

### 7.4 Page-Level Integrity

**On-disk pages:** Standard SQLite format has NO per-page checksums. Corruption
is detected only by structural checks or `PRAGMA integrity_check`.

**Optional FrankenSQLite enhancement:** When `PRAGMA page_checksum = ON`, the
reserved space at the end of each page stores an XXH3-128 hash:

```
Page layout: [data: page_size - 16 bytes] [xxh3: 16 bytes]
Header byte offset 20 set to 16 (reserved space = 16).
```

This is compatible with C SQLite (reserved space is opaque to it). Default is
OFF for maximum compatibility.

**Verification points in the hot path:**
- Every disk read: compute XXH3, store in CachedPage
- Every cache read (optional): reverify XXH3
- Before WAL write: verify dirty page XXH3 matches
- Before checkpoint write: verify page XXH3

### 7.5 WAL Frame Integrity: Cumulative Checksum Chain

The WAL checksum chain provides these properties:

**Append-only integrity:** Inserting or modifying any frame invalidates all
subsequent checksums. Detects both corruption and tampering.

**Torn write detection:** A partial write produces an invalid checksum at
the torn frame. During recovery, frames are read sequentially; the first
invalid checksum marks the valid WAL end.

**Recovery procedure:**

```
recover_wal(wal_file):
  read and verify wal_header checksum
  if invalid: WAL is entirely corrupt; use database file only

  (s1, s2) = (wal_header.cksum1, wal_header.cksum2)
  valid_frames = 0

  loop:
    read frame_header (24 bytes) + page_data (page_size bytes)
    if EOF: break

    // Verify salts match WAL header
    if frame.salt1 != wal_header.salt1 OR frame.salt2 != wal_header.salt2:
      break  // stale frame from previous WAL generation

    // Verify cumulative checksum
    (expected_s1, expected_s2) = wal_checksum(
      frame_header[0..8] ++ page_data, s1, s2, native
    )
    if frame.cksum1 != expected_s1 OR frame.cksum2 != expected_s2:
      break  // torn write or corruption

    (s1, s2) = (frame.cksum1, frame.cksum2)
    valid_frames += 1

  // Only committed transactions (last frame has db_size > 0) are replayed
```

### 7.6 Double-Write Prevention

SQLite's WAL design prevents double-write corruption through:

1. **Cumulative checksums** (Section 7.5): torn writes produce invalid checksums.
2. **Salt values:** Each WAL generation has unique random salts. After
   checkpoint RESTART/TRUNCATE, old frames are rejected by salt mismatch.
3. **Commit frame marker:** A frame with non-zero `db_size` field marks a
   transaction boundary. Partial transactions (no valid commit frame) are
   discarded during recovery.
4. **Sector size alignment:** Frames align to filesystem sector size (detected
   via `VfsFile::sector_size()`, typically 4096 on SSDs). A torn write
   affects at most one frame.

**FrankenSQLite addition:** RaptorQ repair symbols (Section 3.4.1) turn
"detect and discard" into "detect and repair" -- corrupted frames within a
commit group are reconstructed if sufficient repair symbols survive.

### 7.7 PRAGMA integrity_check Implementation

**Level 1 -- Page-level:** Read every page. Verify page type flag is valid
(0x02, 0x05, 0x0A, 0x0D). Verify header fields are in range. If page
checksums enabled, verify XXH3.

**Level 2 -- B-tree structural:** Cell pointers within bounds and non-
overlapping. Cell content within cell content area. Interior child pointers
reference valid pages. Keys sorted within each page. Keys in child subtrees
bounded by parent keys. Freeblock list well-formed (no cycles). Fragmented
byte count matches actual fragmentation.

**Level 3 -- Record format:** Header varints valid. Serial types not 10 or 11.
Payload sizes match serial type declarations. Overflow chains well-formed.

**Level 4 -- Cross-reference:** Every page accounted for (B-tree, freelist, or
pointer-map). No page in multiple B-trees. Freelist structure consistent.
Pointer map entries match actual parents (auto-vacuum mode).

**Level 5 -- Schema:** sqlite_master readable. All entries parseable. Root page
numbers match existing B-trees. For each index, verify entries match table data.

**Output:** List of error strings, or the single string "ok" if no issues
found. Matches C SQLite behavior exactly.

### 7.8 Error Recovery by Checksum Type

**WAL frame checksum mismatch:** Frame is at or beyond the valid WAL end.
Truncate valid WAL at this point. Not an error (normal torn-write recovery).
If RaptorQ repair symbols available, attempt reconstruction first.

**XXH3 internal mismatch (buffer pool):** Return `SQLITE_CORRUPT` to caller.
Log page number, expected hash, actual hash. Evict page from cache. If page
exists in WAL, retry from WAL. Otherwise corruption is persistent.

**CRC-32C mismatch (RaptorQ symbol):** Exclude corrupted symbol from decoding
set. If `|surviving| >= K` source symbols, decoding proceeds. Otherwise the
commit group is unrecoverable.

**Database file corruption (found by integrity_check):** Reported as diagnostic
text. If WAL version exists, it supersedes the corrupt page. Otherwise
corruption is permanent without backups.

### 7.9 Crash Model (Explicit Contract)

FrankenSQLite assumes the following failure model. Every durability and
recovery mechanism is designed against these six points:

1. **Process crash at any point.** No code path is crash-immune. Any operation
   may be interrupted between any two instructions.
2. **`fsync()` is a durability barrier** for data and metadata as documented by
   the OS. We trust the OS's fsync contract but nothing weaker.
3. **Writes can be reordered** unless constrained by fsync barriers. The OS and
   storage hardware may reorder writes freely between fsync calls.
4. **Torn writes exist at sector granularity.** A sector write (typically 512B
   or 4KB) is atomic, but writes spanning multiple sectors can be partially
   completed. Tests simulate multiple sector sizes (512, 1024, 4096).
5. **Bitrot and corruption may exist.** Silent data corruption in storage media
   is a real threat. Checksums (Section 7) detect it; RaptorQ (Section 3)
   repairs it within the configured tolerance budget.
6. **File metadata durability may require directory `fsync()`.** Platform-
   dependent. Our VFS MUST model this. Tests MUST include directory fsync
   simulation.

**Self-healing durability contract:**

> If the commit protocol reports "durable", then the system MUST be able to
> reconstruct the committed data exactly during recovery, even if some
> fraction of locally stored symbols are missing or corrupted within the
> configured tolerance budget.

This is the operational meaning of "self-healing": we do not merely *detect*
corruption; we *repair* it by RaptorQ decoding.

**Durability policy (exposed via PRAGMA):**

- `PRAGMA durability = local` (default): Enough RaptorQ symbols persisted to
  local storage such that decode will succeed under the local corruption budget.
- `PRAGMA durability = quorum(M)`: Enough symbols persisted across M of N
  replicas to survive node loss budgets (see replication in Section 3.4.2).
- `PRAGMA raptorq_overhead = <percent>`: Controls repair symbol budget
  (default: 20% overhead, meaning 1.2x source symbols are stored).

### 7.10 Two Operating Modes

FrankenSQLite supports two operating modes to balance innovation with
verifiability:

**Compatibility Mode (Oracle-Friendly):**
- Purpose: Prove SQL/API correctness against C SQLite 3.52.0.
- DB file is standard SQLite format.
- WAL frames are standard SQLite WAL frames.
- We may write *extra* sidecars (`.wal-fec` for repair symbols, `.idx-fec`
  for index repair) but the core `.db` stays SQLite-compatible when
  checkpointed.
- This is the default mode for conformance testing.

**Native Mode (RaptorQ-First):**
- Purpose: Maximum concurrency + durability + replication.
- Primary durable state is an ECS commit stream (CommitCapsule objects encoded
  as RaptorQ symbols).
- CommitCapsule contains: `commit_seq`, `snapshot_basis`, `intent_log` and/or
  `page_deltas`, `read_set_digest`, `write_set_digest`, SSI witnesses.
- CommitMarker is the atomic "this commit exists" record: `commit_seq`,
  `capsule_object_id`, `prev_marker`, `integrity_hash`.
- **Atomicity rule:** A commit is committed iff its marker is durable. Recovery
  ignores any capsule without a committed marker.
- Checkpointing materializes a canonical `.db` for compatibility export, but
  the source-of-truth is the commit stream.
- Both modes are supported by the **same SQL/API layer**. Conformance harness
  validates behavior, not internal format.

**Mode selection:** `PRAGMA fsqlite.mode = compatibility | native` (default:
compatibility). Applications can switch modes between connections.

---

## 8. Architecture: Crate Map and Dependencies

### 8.1 Workspace Structure

23 crates under `crates/`, plus supporting directories:

```
frankensqlite/
  Cargo.toml                     # Workspace root
  rust-toolchain.toml            # channel = "nightly"
  AGENTS.md                      # Agent guidelines
  COMPREHENSIVE_SPEC_FOR_FRANKENSQLITE_V1.md

  crates/
    fsqlite-types/               # PageNumber, SqliteValue, TxnId, Opcode, limits
    fsqlite-error/               # FrankenError, ErrorCode
    fsqlite-vfs/                 # Vfs/VfsFile traits, MemoryVfs, UnixVfs
    fsqlite-pager/               # Page cache, journal, state machine
    fsqlite-wal/                 # WAL frames, index, checkpoint, recovery
    fsqlite-mvcc/                # Page versioning, snapshots, conflicts, GC
    fsqlite-btree/               # B-tree: cursor, cell, balance, overflow, freelist
    fsqlite-ast/                 # SQL AST nodes
    fsqlite-parser/              # Lexer + recursive descent parser
    fsqlite-planner/             # Name resolution, WHERE, join ordering, cost
    fsqlite-vdbe/                # Bytecode VM (190+ opcodes), Mem values, sort
    fsqlite-func/                # Built-in scalar/aggregate/window functions
    fsqlite-ext-fts3/            # FTS3/FTS4
    fsqlite-ext-fts5/            # FTS5
    fsqlite-ext-rtree/           # R-tree + geopoly
    fsqlite-ext-json/            # JSON1
    fsqlite-ext-session/         # Session/changeset
    fsqlite-ext-icu/             # ICU collation
    fsqlite-ext-misc/            # generate_series, dbstat, csv, etc.
    fsqlite-core/                # Connection, prepare, schema, codegen
    fsqlite/                     # Public API facade
    fsqlite-cli/                 # Interactive shell (frankentui)
    fsqlite-harness/             # Conformance test runner

  conformance/                   # Golden output fixtures
  tests/                         # Workspace integration tests
  benches/                       # Criterion benchmarks
  fuzz/                          # Fuzz targets
  legacy_sqlite_code/            # C source reference
```

### 8.2 Dependency Layers

```
Layer 0 (leaves):     fsqlite-types    fsqlite-error
Layer 1 (storage):    fsqlite-vfs      fsqlite-ast
Layer 2 (cache):      fsqlite-pager    fsqlite-parser     fsqlite-func
Layer 3 (log):        fsqlite-wal      fsqlite-planner
Layer 4 (btree):      fsqlite-btree
Layer 5 (vm):         fsqlite-vdbe
Layer 6 (mvcc):       fsqlite-mvcc
Layer 7 (ext):        fsqlite-ext-{fts3,fts5,rtree,json,session,icu,misc}
Layer 8 (core):       fsqlite-core
Layer 9 (api):        fsqlite
Layer 10 (apps):      fsqlite-cli      fsqlite-harness
```

### 8.3 Per-Crate Detailed Descriptions

**`fsqlite-types`** (~3,500 LOC estimated)

The foundational types crate with zero internal dependencies.

Key types and modules:
- `page.rs`: `PageNumber` (NonZeroU32), `PageData` (Vec<u8>), `PageSize` (validated power of 2)
- `value.rs`: `SqliteValue` enum (Null, Integer(i64), Real(f64), Text(String), Blob(Vec<u8>))
- `opcode.rs`: `Opcode` enum with all 190+ VDBE opcodes, plus `OpcodeInfo` metadata
- `serial.rs`: `SerialType` (u64), serial type encoding/decoding, content size formulas
- `record.rs`: `Record` struct, `RecordHeader`, serialization/deserialization
- `txn.rs`: `TxnId` (u64 newtype), `TxnMode` enum (Deferred, Immediate, Exclusive, Concurrent)
- `flags.rs`: `OpenFlags`, `SyncFlags`, `AccessFlags`, `LockLevel` (bitflags)
- `limits.rs`: SQLite limits (SQLITE_MAX_LENGTH, SQLITE_MAX_COLUMN, etc.)
- `affinity.rs`: `TypeAffinity` enum, affinity determination from type names
- `collation.rs`: `CollationId`, built-in collation identifiers (BINARY, NOCASE, RTRIM)

Public API surface: ~80 types, all `#[derive(Debug, Clone)]`, most `Copy` where possible.

**`fsqlite-error`** (~800 LOC estimated)

Error types using `thiserror` derive.

Key types:
- `error.rs`: `FrankenError` enum (~40 variants mapping to SQLite error codes)
- `code.rs`: `ErrorCode` enum (SQLITE_OK, SQLITE_ERROR, SQLITE_BUSY, ..., ~30 primary codes)
- `extended.rs`: Extended error codes (SQLITE_BUSY_RECOVERY, SQLITE_BUSY_SNAPSHOT, etc.)
- `result.rs`: `type Result<T> = std::result::Result<T, FrankenError>`

Every variant carries context: the operation that failed, the page or table involved,
and optionally a source error (for I/O errors wrapping std::io::Error).

**`fsqlite-vfs`** (~2,500 LOC estimated)

Virtual filesystem abstraction. Equivalent to sqlite3_vfs + sqlite3_io_methods.

Modules:
- `traits.rs`: `Vfs` and `VfsFile` trait definitions
- `memory.rs`: `MemoryVfs` -- fully in-memory VFS for testing. Stores file data
  in `HashMap<PathBuf, Arc<Mutex<Vec<u8>>>>`. Supports concurrent access.
- `unix.rs`: `UnixVfs` -- POSIX VFS using asupersync blocking I/O. File locking
  via `fcntl(F_SETLK)`. Implements all 5 SQLite lock levels (NONE, SHARED,
  RESERVED, PENDING, EXCLUSIVE).
- `flags.rs`: `VfsOpenFlags` (READONLY, READWRITE, CREATE, etc.)

Dependency rationale: depends on `fsqlite-types` for `PageNumber`, `OpenFlags`;
depends on `fsqlite-error` for `Result`. Uses `asupersync` for blocking I/O
pool in `UnixVfs`.

**`fsqlite-pager`** (~4,000 LOC estimated)

Page cache and transaction state machine. The core I/O layer.

Modules:
- `pager.rs`: `Pager` struct (the main type). State machine:
  `Open -> Reader -> Writer -> Error`. Manages database file handle, journal
  file, WAL file, and the ARC cache.
- `cache.rs`: `ArcCache` implementation (Section 6). Full ARC algorithm with
  MVCC-aware eviction.
- `page_ref.rs`: `PageRef` (RAII guard that pins a page in cache, decrements
  ref_count on drop).
- `journal.rs`: Rollback journal creation, page journaling, hot journal
  detection and rollback.
- `state.rs`: `PagerState` enum, transition validation.
- `header.rs`: Database header parsing and writing (100-byte header at offset 0).

Dependency rationale: needs `fsqlite-vfs` for file I/O; needs `fsqlite-types`
for `PageNumber`, `PageData`; needs `fsqlite-error` for error handling.

**`fsqlite-wal`** (~3,500 LOC estimated)

Write-ahead log implementation.

Modules:
- `wal.rs`: `Wal` struct. WAL file header parsing/writing. Frame append.
  Cumulative checksum computation (Section 7.1).
- `frame.rs`: `WalFrame` struct (24-byte header + page data). Frame
  serialization/deserialization.
- `index.rs`: `WalIndex` -- shared-memory hash table for page-to-frame lookup.
  Hash tables with linear probing, reader marks, lock bytes.
- `checkpoint.rs`: Checkpoint logic (PASSIVE, FULL, RESTART, TRUNCATE).
  Reads frames from WAL, writes pages to database file, resets WAL.
- `recovery.rs`: WAL recovery on database open. Validates checksum chain,
  replays committed transactions. RaptorQ self-healing integration.
- `raptorq.rs`: RaptorQ repair symbol generation for WAL commit groups.
  Encoding on commit, decoding during recovery.

Dependency rationale: needs `fsqlite-vfs` for WAL file and SHM file access;
needs `fsqlite-pager` for page cache interaction during checkpoint; needs
`asupersync` for RaptorQ codec.

**`fsqlite-mvcc`** (~3,000 LOC estimated)

MVCC version management, the heart of the concurrency innovation.

Modules:
- `manager.rs`: `MvccManager` -- coordinates transactions, version store,
  page lock table, commit log, and GC.
- `snapshot.rs`: `Snapshot` struct with Bloom filter. `capture_snapshot()`
  logic. Visibility predicate.
- `version.rs`: `PageVersion` struct, version chains (linked list per page).
  Version store keyed by `PageNumber`.
- `lock_table.rs`: `PageLockTable` (HashMap<PageNumber, TxnId>). `try_acquire`,
  `release`, `release_all`.
- `transaction.rs`: `Transaction` struct. Lifecycle: Active -> Committed/Aborted.
  Write set, read set, page locks.
- `commit.rs`: Commit validation (first-committer-wins). Algebraic write merge
  detection. WAL append via write coordinator.
- `gc.rs`: Garbage collection. Horizon computation, version chain pruning,
  reclaimability predicate.
- `coordinator.rs`: `WriteCoordinator` -- wraps asupersync two-phase MPSC
  channel. Serializes WAL appends while allowing concurrent validation.
- `bloom.rs`: Bloom filter implementation for snapshot in-flight set.

Dependency rationale: needs `fsqlite-wal` for WAL append; needs `fsqlite-pager`
for page cache; needs `parking_lot` for fast Mutex/RwLock on hot-path
structures; needs `asupersync` for channels and RaptorQ.

**`fsqlite-btree`** (~5,000 LOC estimated)

B-tree storage engine. The most complex crate after `fsqlite-vdbe`.

Modules:
- `cursor.rs`: `BtCursor` with page stack traversal (max depth 20 for 4KB
  pages, max depth 40 for 512-byte pages). Position save/restore for cursor
  stability across modifications.
- `cell.rs`: Cell format parsing. `IntKeyCell` (table leaf), `BlobKeyCell`
  (index leaf), `InteriorCell`. Varint decoding for payload size and rowid.
- `balance.rs`: Page splitting and merging. `balance_nonroot` (redistribution
  among siblings), `balance_deeper` (new root creation on root overflow),
  `balance_quick` (fast-path append to rightmost leaf).
- `overflow.rs`: Overflow page chain management. Read/write payload spanning
  multiple overflow pages. Chain creation, traversal, and freeing.
- `free_list.rs`: Free page management. Trunk/leaf structure. Allocate from
  freelist or grow file. Deallocate to freelist.
- `payload.rs`: `BtreePayload` -- unified read/write abstraction for cell
  payloads that may span local storage + overflow pages.
- `table.rs`: Table B-tree operations (intkey). Create table, drop table,
  row count.
- `index.rs`: Index B-tree operations (blobkey). Create index, drop index.

Dependency rationale: needs `fsqlite-pager` (via `MvccPager` trait) for page
access; needs `fsqlite-types` for `PageNumber`, `SerialType`, cell format types.

**`fsqlite-ast`** (~2,000 LOC estimated)

SQL abstract syntax tree node types.

Modules:
- `stmt.rs`: Top-level `Statement` enum (Select, Insert, Update, Delete,
  CreateTable, CreateIndex, CreateView, CreateTrigger, Drop, AlterTable,
  Attach, Detach, Begin, Commit, Rollback, Savepoint, Release, Pragma,
  Vacuum, Reindex, Analyze, Explain).
- `expr.rs`: `Expr` enum (~30 variants: Literal, Column, BinaryOp, UnaryOp,
  Between, In, Like, Case, Cast, Exists, Subquery, FunctionCall, Aggregate,
  Window, Collate, Raise, JsonAccess, etc.)
- `select.rs`: `SelectStatement`, `SelectCore`, `CompoundOp`, `JoinClause`,
  `JoinType`, `OrderingTerm`, `LimitClause`, `WithClause`, `Cte`.
- `table_ref.rs`: `TableRef` enum (Named, Subquery, JoinExpr, FunctionCall).
- `ddl.rs`: `ColumnDef`, `TableConstraint`, `IndexedColumn`, `ForeignKeyClause`.
- `literal.rs`: `Literal` enum (Integer, Float, String, Blob, Null, True, False, CurrentTime, CurrentDate, CurrentTimestamp).
- `operator.rs`: `BinaryOp`, `UnaryOp` enums with all SQL operators.
- `span.rs`: `Span` (byte offset range in source text) for error reporting.

All AST nodes carry `Span` for source location.

**`fsqlite-parser`** (~4,500 LOC estimated)

SQL lexer and recursive descent parser.

Modules:
- `lexer.rs`: Tokenizer. Token types enum (~150 variants). Memchr-accelerated
  scanning for string delimiters and comment markers. Line/column tracking.
- `parser.rs`: Recursive descent parser. One method per grammar production.
  Pratt precedence for expression parsing.
- `keyword.rs`: Perfect hash for 150+ SQL keywords (generated at build time
  or via phf crate).
- `error.rs`: Parse error types with source span, expected tokens, recovery hints.

**`fsqlite-planner`** (~3,000 LOC estimated)

Query planning and optimization.

Modules:
- `resolve.rs`: Name resolution. Table alias binding, column reference
  resolution, star expansion, subquery scoping.
- `where_clause.rs`: WHERE clause analysis. Extracting index-usable terms,
  range constraints, OR optimization.
- `join.rs`: Join ordering. Greedy algorithm for > 6 tables, exhaustive
  search for <= 6.
- `cost.rs`: Cost model. Estimated I/O per access path. Index selectivity
  estimation from sqlite_stat1/stat4.
- `index.rs`: Index usability determination. Which indexes can serve a
  given WHERE clause. Covering index detection.
- `plan.rs`: `QueryPlan` output type. Access path per table, join order,
  estimated cost.

**`fsqlite-vdbe`** (~6,000 LOC estimated)

The bytecode virtual machine. Largest crate by estimated LOC.

Modules:
- `vm.rs`: Fetch-execute loop. `VdbeExec` struct. Match-based opcode dispatch.
  Program counter management, jump resolution.
- `mem.rs`: `Mem` (sqlite3_value). Multi-representation storage (integer + text
  cached simultaneously). Type affinity application. Comparison with collation.
- `cursor.rs`: `VdbeCursor` wrapping `BtCursor`. Deferred seek, cached row
  decoding, pseudo-table support.
- `program.rs`: `VdbeProgram` (Vec<VdbeOp>). Register allocation metadata.
  Coroutine state.
- `op.rs`: `VdbeOp` struct (opcode, p1, p2, p3, p4, p5). `P4` enum variants.
- `sort.rs`: External merge sort for ORDER BY. Sorter cursor.
- `compare.rs`: Record comparison with collation sequences. Key comparison
  for index lookups.
- `func_dispatch.rs`: Function call dispatch. Scalar, aggregate, window.
- `subtype.rs`: Subtype management (for JSON functions).

**`fsqlite-func`** (~2,500 LOC estimated)

Built-in functions (~80 total).

Modules:
- `scalar.rs`: ~60 scalar functions (abs, char, hex, instr, length, lower, etc.)
- `aggregate.rs`: ~12 aggregate functions (avg, count, sum, group_concat, etc.)
- `window.rs`: ~11 window functions (row_number, rank, lag, lead, etc.)
- `math.rs`: Math functions (acos, sin, sqrt, log, etc.)
- `info.rs`: sqlite_version, changes, total_changes, last_insert_rowid
- `registry.rs`: `FunctionRegistry` -- maps (name, arg_count) to function impl

**`fsqlite-ext-json`** (~2,000 LOC)
JSON1 extension. json(), json_extract(), json_set(), json_remove(), json_type(),
json_valid(), json_each/json_tree virtual tables, JSONB binary format, -> and ->> operators.

**`fsqlite-ext-fts5`** (~4,000 LOC)
Full-text search v5. Porter stemmer, unicode61 tokenizer, inverted index, BM25
ranking, highlight/snippet auxiliary functions, custom tokenizer API.

**`fsqlite-ext-fts3`** (~2,000 LOC)
FTS3/4 compatibility layer. matchinfo(), offsets(), snippet(). Largely wraps FTS5.

**`fsqlite-ext-rtree`** (~2,000 LOC)
R-tree spatial index. R*-tree insertion, nearest-neighbor search. Geopoly extension.

**`fsqlite-ext-session`** (~1,500 LOC)
Session extension. Changeset/patchset generation, application, and inversion.

**`fsqlite-ext-icu`** (~800 LOC)
ICU collation integration. Unicode-aware comparison, case folding, FTS tokenizer.

**`fsqlite-ext-misc`** (~1,500 LOC)
Miscellaneous: generate_series, dbstat, dbpage, csv virtual table, decimal,
uuid, ieee754, carray.

**`fsqlite-core`** (~5,000 LOC estimated)

The orchestration layer that wires everything together.

Modules:
- `connection.rs`: `Connection` struct. Open/close, ATTACH/DETACH, schema cache,
  auto-commit state, busy handler, authorization callback.
- `prepare.rs`: SQL compilation pipeline: parse -> resolve -> plan -> codegen.
  Statement cache (LRU of prepared statements, keyed by SQL text hash).
- `schema.rs`: Schema loading from sqlite_master. Table, Index, View, Trigger
  objects. Schema cookie validation and reload.
- `codegen.rs`: AST-to-VDBE code generation. SELECT, INSERT, UPDATE, DELETE
  compilation. Expression codegen. Subquery/CTE coroutine generation.
- `pragma.rs`: PRAGMA command implementation (~80 pragmas).
- `auth.rs`: Authorization callback dispatch.
- `vtab.rs`: Virtual table module registration and lifecycle.

**`fsqlite`** (~1,000 LOC estimated)

Public API facade. Re-exports:

```rust
pub use fsqlite_core::{Connection, Statement, Row, Transaction};
pub use fsqlite_types::{SqliteValue, PageNumber};
pub use fsqlite_error::{FrankenError, ErrorCode, Result};
pub use fsqlite_vfs::{Vfs, VfsFile, MemoryVfs};
```

Adds convenience methods: `Connection::open()`, `Connection::open_in_memory()`,
`Connection::execute()`, `Connection::query_row()`.

**`fsqlite-cli`** (~2,000 LOC estimated)
Interactive shell using frankentui. Dot-commands (.tables, .schema, .mode, .import,
.dump, .headers, .separator). Output modes (column, csv, json, line, list, table).
Tab completion, syntax highlighting, history.

**`fsqlite-harness`** (~1,500 LOC estimated)
Conformance test runner. Runs identical SQL against FrankenSQLite and C sqlite3.
Compares output row-by-row. Error code matching. Golden file management.

### 8.4 Dependency Edges with Rationale

| From | To | Rationale |
|------|----|-----------|
| fsqlite-vfs | fsqlite-types | OpenFlags, PageNumber |
| fsqlite-vfs | fsqlite-error | Result type |
| fsqlite-pager | fsqlite-vfs | File I/O |
| fsqlite-pager | fsqlite-types | PageNumber, PageData |
| fsqlite-wal | fsqlite-vfs | WAL file + SHM file access |
| fsqlite-wal | fsqlite-pager | Page cache during checkpoint |
| fsqlite-wal | fsqlite-types | PageNumber, frame types |
| fsqlite-mvcc | fsqlite-wal | WAL append during commit |
| fsqlite-mvcc | fsqlite-pager | Page cache for version base data |
| fsqlite-mvcc | fsqlite-types | TxnId, PageNumber, Snapshot |
| fsqlite-mvcc | parking_lot | Fast Mutex for lock table (hot path) |
| fsqlite-mvcc | asupersync | Two-phase MPSC channel, RaptorQ codec |
| fsqlite-btree | fsqlite-pager | Page access (via MvccPager trait) |
| fsqlite-btree | fsqlite-types | Cell formats, SerialType |
| fsqlite-ast | fsqlite-types | SqliteValue (for AST literals) |
| fsqlite-parser | fsqlite-ast | Produces AST nodes |
| fsqlite-parser | fsqlite-types | Token types, keyword IDs |
| fsqlite-parser | memchr | SIMD byte scanning in lexer |
| fsqlite-planner | fsqlite-ast | Consumes AST, produces plan |
| fsqlite-planner | fsqlite-types | Column metadata, affinities |
| fsqlite-vdbe | fsqlite-btree | B-tree cursor operations |
| fsqlite-vdbe | fsqlite-pager | Direct page access for some opcodes |
| fsqlite-vdbe | fsqlite-types | Opcode enum, Mem values |
| fsqlite-func | fsqlite-types | SqliteValue args and return |
| fsqlite-core | (all above) | Orchestration layer |
| fsqlite | fsqlite-core | Public API wraps core |
| fsqlite-cli | fsqlite | Uses public API |
| fsqlite-cli | frankentui | TUI framework |
| fsqlite-harness | fsqlite | Uses public API for testing |

### 8.5 Feature Flags

```toml
# Status: not yet implemented in Cargo manifests.
#
# Feature flags MUST live on a real package manifest (e.g. `crates/fsqlite/Cargo.toml`),
# not the workspace root (which is a virtual manifest). The target shape is:
#
# crates/fsqlite/Cargo.toml (planned)
[features]
default = ["json", "fts5", "rtree"]

json = ["dep:fsqlite-ext-json"]
fts5 = ["dep:fsqlite-ext-fts5"]
fts3 = ["dep:fsqlite-ext-fts3"]
rtree = ["dep:fsqlite-ext-rtree"]
session = ["dep:fsqlite-ext-session"]
icu = ["dep:fsqlite-ext-icu"]
misc = ["dep:fsqlite-ext-misc"]

# Enables FrankenSQLite's RaptorQ-backed repair/replication hooks.
# Note: asupersync's RaptorQ module is not feature-gated upstream; this flag
# controls FrankenSQLite integration code only.
raptorq = []

# MVCC is core; use runtime configuration to choose default transaction behavior.
mvcc = []
```

### 8.6 Build Configuration

```toml
[workspace.package]
edition = "2024"
license = "MIT"
repository = "https://github.com/Dicklesworthstone/frankensqlite"
rust-version = "1.85"

[workspace.lints.rust]
unsafe_code = "forbid"            # No unsafe anywhere in workspace

[workspace.lints.clippy]
pedantic = { level = "deny", priority = -1 }
nursery = { level = "deny", priority = -1 }
cast_precision_loss = { level = "allow", priority = 1 }
doc_markdown = { level = "allow", priority = 1 }
missing_const_for_fn = { level = "allow", priority = 1 }
uninlined_format_args = { level = "allow", priority = 1 }
missing_errors_doc = { level = "allow", priority = 1 }
missing_panics_doc = { level = "allow", priority = 1 }
module_name_repetitions = { level = "allow", priority = 1 }
must_use_candidate = { level = "allow", priority = 1 }
option_if_let_else = { level = "allow", priority = 1 }

[profile.release]
opt-level = "z"        # Optimize for size (lean binary for distribution)
lto = true             # Whole-program optimization
codegen-units = 1      # Single codegen unit for maximum optimization
panic = "abort"        # No unwinding overhead
strip = true           # Strip debug info from release binary

[profile.dev]
opt-level = 1          # Mild optimization for acceptable test speed
```

---

## 9. Trait Hierarchy

**Cx Everywhere Rule:** Every trait method that touches I/O, acquires locks,
or could block MUST accept `&Cx` (asupersync's capability context) as its
first parameter. This enables:
- **Cancellation:** Any operation can be cancelled by the caller's context.
- **Deadline propagation:** Timeout budgets flow through the entire call chain.
- **Capability narrowing:** Callers can restrict what callees are allowed to do.

The `Cx` parameter appears in VFS, MvccPager, and any async-capable method.
Pure computation (e.g., `CollationFunction::compare`, `ScalarFunction::call`
for CPU-only work) does not take `Cx`. When in doubt, include `Cx`.

### 9.1 Storage Traits

```rust
/// Virtual filesystem abstraction.
/// Equivalent to sqlite3_vfs in C SQLite.
///
/// # Thread Safety
/// Implementations must be Send + Sync because a single VFS instance is shared
/// across all connections in a process. The VFS itself is stateless (or
/// internally synchronized); individual file handles carry mutable state.
///
/// # Error Handling
/// All methods return `Result<T, FrankenError>`. I/O errors are wrapped in
/// `FrankenError::IoError(std::io::Error)`. Permission errors map to
/// `FrankenError::CantOpen` or `FrankenError::Auth`.
pub trait Vfs: Send + Sync {
    /// The file handle type produced by this VFS.
    type File: VfsFile;

    /// Open a file at the given path with the specified flags.
    ///
    /// `path` is None for temporary files (the VFS chooses a path).
    /// Returns the opened file handle and the flags that were actually used
    /// (some flags may be modified, e.g., READWRITE downgraded to READONLY).
    ///
    /// # Errors
    /// - `FrankenError::CantOpen` if the file cannot be opened.
    /// - `FrankenError::IoError` for underlying I/O failures.
    fn open(&self, cx: &Cx, path: Option<&Path>, flags: VfsOpenFlags)
        -> Result<(Self::File, VfsOpenFlags)>;

    /// Delete a file. If `sync_dir` is true, also sync the directory
    /// containing the file to ensure the deletion is durable.
    ///
    /// # Errors
    /// - `FrankenError::IoError` if deletion fails.
    /// - Not an error if the file does not exist.
    fn delete(&self, cx: &Cx, path: &Path, sync_dir: bool) -> Result<()>;

    /// Check whether a file exists or has specific properties.
    ///
    /// `flags` determines what to check:
    /// - `AccessFlags::EXISTS`: file exists
    /// - `AccessFlags::READWRITE`: file exists and is read-write
    /// - `AccessFlags::READ`: file exists and is readable
    fn access(&self, path: &Path, flags: AccessFlags) -> Result<bool>;

    /// Convert a relative path to an absolute (canonical) path.
    fn full_pathname(&self, path: &Path) -> Result<PathBuf>;

    /// Fill `buf` with random bytes. Used for WAL salt generation.
    fn randomness(&self, buf: &mut [u8]);

    /// Return the current time as a Julian day number (fractional days
    /// since noon, November 24, 4714 BC, proleptic Gregorian calendar).
    fn current_time(&self) -> f64;
}

/// An open file handle within a VFS.
/// Equivalent to sqlite3_file + sqlite3_io_methods in C SQLite.
///
/// # Thread Safety
/// Send + Sync because file handles may be shared across threads (e.g.,
/// the WAL file is accessed by both readers and the write coordinator).
/// Implementations must use internal synchronization for mutable state.
///
/// # Lifetime
/// A VfsFile is owned by the component that opened it (Pager, Wal).
/// It is closed when dropped or when `close()` is called explicitly.
pub trait VfsFile: Send + Sync {
    /// Close the file handle and release all resources.
    /// After close(), no other methods may be called.
    fn close(&mut self) -> Result<()>;

    /// Read `buf.len()` bytes from the file at the given byte offset.
    /// Returns the number of bytes actually read (may be less than
    /// buf.len() if the file is shorter than offset + buf.len()).
    /// Short reads zero-fill the remainder of buf.
    fn read(&mut self, buf: &mut [u8], offset: u64) -> Result<usize>;

    /// Write `buf` to the file at the given byte offset.
    /// The file is extended if necessary.
    fn write(&mut self, buf: &[u8], offset: u64) -> Result<()>;

    /// Truncate the file to exactly `size` bytes.
    fn truncate(&mut self, size: u64) -> Result<()>;

    /// Sync file contents to durable storage.
    /// `flags`: SYNC_NORMAL or SYNC_FULL (FULL also syncs metadata).
    fn sync(&mut self, flags: SyncFlags) -> Result<()>;

    /// Return the current file size in bytes.
    fn file_size(&self) -> Result<u64>;

    /// Acquire or upgrade a file lock.
    /// Lock levels: NONE < SHARED < RESERVED < PENDING < EXCLUSIVE.
    /// Locks are advisory; they coordinate concurrent access between
    /// processes but do not prevent direct file I/O.
    ///
    /// # Errors
    /// - `FrankenError::Busy` if the lock cannot be acquired (another
    ///   process holds a conflicting lock).
    fn lock(&mut self, level: LockLevel) -> Result<()>;

    /// Release or downgrade a file lock.
    fn unlock(&mut self, level: LockLevel) -> Result<()>;

    /// Check whether another process holds a RESERVED lock.
    /// Used to determine if a write transaction is in progress elsewhere.
    fn check_reserved_lock(&self) -> Result<bool>;

    /// Return the sector size of the underlying storage device.
    /// Typically 512 (HDD) or 4096 (SSD). Used for WAL frame alignment.
    fn sector_size(&self) -> u32;

    /// Return device characteristics flags.
    /// Bit flags indicating device properties: IOCAP_ATOMIC, IOCAP_SAFE_APPEND,
    /// IOCAP_SEQUENTIAL, etc. Used to optimize sync behavior.
    fn device_characteristics(&self) -> u32;
}

/// MVCC-aware page access. The primary interface for B-tree and VDBE layers.
///
/// # Thread Safety
/// Send + Sync. Multiple transactions from different threads call into the
/// same MvccPager concurrently. The implementation uses internal locking
/// (version store RwLock, page lock table Mutex) for synchronization.
///
/// # Lifetime Relationships
/// The MvccPager outlives all Transactions it creates. Transaction holds
/// a reference (via Arc) to the MvccPager's internal state.
pub trait MvccPager: Send + Sync {
    /// Begin a new transaction with the specified mode.
    /// Serialized mode acquires the global write mutex immediately.
    /// Concurrent mode does not acquire any locks until write_page().
    fn begin(&self, cx: &Cx, mode: TxnMode) -> Result<Transaction>;

    /// Read a page within a transaction. Returns a pinned page reference.
    /// The page is resolved through: write_set -> version_chain -> disk.
    /// Tracks the page in the transaction's read set and SIREAD table (SSI).
    fn get_page(&self, cx: &Cx, txn: &Transaction, pgno: PageNumber) -> Result<PageRef>;

    /// Write a page within a transaction.
    /// In Concurrent mode, acquires a page lock (returns SQLITE_BUSY if held),
    /// and updates SSI rw-antidependency state.
    /// In Serialized mode, the global mutex is already held.
    fn write_page(&self, cx: &Cx, txn: &mut Transaction, pgno: PageNumber, data: PageData) -> Result<()>;

    /// Allocate a new page (from freelist or by growing the file).
    fn allocate_page(&self, cx: &Cx, txn: &mut Transaction) -> Result<PageNumber>;

    /// Mark a page as free (add to freelist).
    fn free_page(&self, cx: &Cx, txn: &mut Transaction, pgno: PageNumber) -> Result<()>;

    /// Commit the transaction. SSI validation (abort if pivot),
    /// first-committer-wins check, rebase/algebraic merge, WAL append,
    /// version publishing, SIREAD cleanup, lock release.
    /// Returns SQLITE_BUSY_SNAPSHOT on SSI abort or conflict.
    fn commit(&self, cx: &Cx, txn: Transaction) -> Result<()>;

    /// Abort the transaction. Discards write set, releases locks,
    /// cleans up SIREAD entries. Never fails.
    fn rollback(&self, txn: Transaction);
}

/// Cursor operations over a B-tree.
///
/// # Thread Safety
/// NOT Send or Sync. A cursor is bound to a single transaction and
/// should only be used from one thread at a time. The VDBE execution
/// loop is single-threaded per statement.
pub trait BtreeCursorOps {
    /// Position the cursor at or near the given key.
    /// Returns the cursor's final position relative to the key.
    fn move_to(&mut self, key: &[u8]) -> Result<CursorPosition>;

    /// Advance to the next entry. Returns false if no more entries.
    fn next(&mut self) -> Result<bool>;

    /// Move to the previous entry. Returns false if at the beginning.
    fn prev(&mut self) -> Result<bool>;

    /// Insert a key/data pair at the cursor's current position.
    /// May trigger page splits (balance operations).
    fn insert(&mut self, key: &[u8], data: &[u8]) -> Result<()>;

    /// Delete the entry at the cursor's current position.
    /// May trigger page merges.
    fn delete(&mut self) -> Result<()>;

    /// Read the key of the current entry.
    fn key(&self) -> Result<&[u8]>;

    /// Read the data (payload) of the current entry.
    fn data(&self) -> Result<&[u8]>;

    /// Read the rowid of the current entry (for intkey tables).
    fn rowid(&self) -> Result<i64>;

    /// Return true if the cursor is positioned past the last entry.
    fn eof(&self) -> bool;
}
```

### 9.2 Function Traits

```rust
/// A scalar function (deterministic or non-deterministic).
/// Equivalent to xFunc in sqlite3_create_function.
///
/// # Thread Safety
/// Send + Sync because function objects are shared across connections
/// and may be called concurrently by different VDBE executions.
pub trait ScalarFunction: Send + Sync {
    /// Invoke the function with the given arguments.
    /// Returns the result value, or an error.
    ///
    /// # Errors
    /// - `FrankenError::Error` with a message for domain errors (e.g., abs(NULL))
    /// - `FrankenError::TooBig` if result exceeds SQLITE_MAX_LENGTH
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue>;

    /// Whether this function is deterministic (same inputs always produce same output).
    /// Deterministic functions can be optimized (e.g., constant folding).
    fn is_deterministic(&self) -> bool { true }

    /// Number of arguments. -1 means variadic.
    fn num_args(&self) -> i32;

    /// Function name (for error messages and EXPLAIN output).
    fn name(&self) -> &str;
}

/// An aggregate function with step/finalize semantics.
/// Equivalent to xStep + xFinal in sqlite3_create_function.
pub trait AggregateFunction: Send + Sync {
    /// Aggregate accumulator state. Created via Default::default() at the
    /// start of each aggregation group.
    type State: Default + Send;

    /// Process one row. Called once per row in the group.
    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()>;

    /// Produce the final result for the group.
    /// Consumes the state (the accumulator is no longer needed).
    fn finalize(&self, state: Self::State) -> Result<SqliteValue>;

    fn num_args(&self) -> i32;
    fn name(&self) -> &str;
}

/// A window function with step/inverse/value/finalize semantics.
/// Equivalent to xStep + xInverse + xValue + xFinal.
pub trait WindowFunction: Send + Sync {
    type State: Default + Send;

    /// Add a row to the window frame.
    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()>;

    /// Remove a row from the window frame (for sliding windows).
    /// This is the key difference from aggregate: window functions must
    /// support efficient removal of rows that have left the frame.
    fn inverse(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()>;

    /// Return the current value of the window function without consuming state.
    /// Called after each step/inverse to produce the result for the current row.
    fn value(&self, state: &Self::State) -> Result<SqliteValue>;

    /// Produce the final value and consume the state.
    fn finalize(&self, state: Self::State) -> Result<SqliteValue>;

    fn num_args(&self) -> i32;
    fn name(&self) -> &str;
}
```

### 9.3 Extension Traits

```rust
/// A virtual table implementation.
/// Equivalent to sqlite3_module in C SQLite.
///
/// # Thread Safety
/// Send + Sync. A virtual table module is registered once and shared.
/// Individual table instances may have mutable state protected by
/// internal locks.
pub trait VirtualTable: Send + Sync {
    type Cursor: VirtualTableCursor;

    /// Create or connect to a virtual table.
    /// `args` contains the module arguments from the CREATE VIRTUAL TABLE statement.
    fn connect(db: &Database, args: &[&str]) -> Result<Self> where Self: Sized;

    /// Inform SQLite about the best index strategy for a given set of constraints.
    /// The planner calls this to determine which indexes are available and
    /// their estimated costs.
    fn best_index(&self, info: &mut IndexInfo) -> Result<()>;

    /// Open a new cursor for scanning the virtual table.
    fn open(&self) -> Result<Self::Cursor>;

    /// Disconnect from the virtual table (drop the instance).
    fn disconnect(&mut self) -> Result<()>;
}

/// A cursor for iterating over a virtual table.
pub trait VirtualTableCursor: Send {
    /// Begin a scan with the given filter parameters.
    /// `idx_num` and `idx_str` come from best_index().
    fn filter(&mut self, idx_num: i32, idx_str: Option<&str>,
              args: &[SqliteValue]) -> Result<()>;

    /// Advance to the next row. Call after filter() and between rows.
    fn next(&mut self) -> Result<()>;

    /// Return true if the cursor has moved past the last row.
    fn eof(&self) -> bool;

    /// Write the value of column `col` into the context.
    fn column(&self, ctx: &mut ColumnContext, col: i32) -> Result<()>;

    /// Return the rowid of the current row.
    fn rowid(&self) -> Result<i64>;
}
```

### 9.4 Collation and Authorization Traits

```rust
/// A collation function for string comparison.
/// Equivalent to sqlite3_create_collation.
///
/// The collation determines the sort order for text values.
/// Built-in collations: BINARY (memcmp), NOCASE (case-insensitive ASCII),
/// RTRIM (ignore trailing spaces).
pub trait CollationFunction: Send + Sync {
    /// Compare two strings according to this collation.
    /// Returns Ordering::Less, Equal, or Greater.
    ///
    /// The inputs are UTF-8 encoded byte slices.
    /// The comparison must be deterministic, antisymmetric, and transitive.
    fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering;

    /// Collation name (e.g., "BINARY", "NOCASE", "my_collation").
    fn name(&self) -> &str;
}

/// Authorization callback.
/// Equivalent to sqlite3_set_authorizer.
///
/// Called during SQL compilation (not execution) to approve or deny
/// each operation. Used for sandboxing untrusted SQL.
pub trait Authorizer: Send + Sync {
    /// Called for each operation during SQL compilation.
    /// Returns AuthResult::Ok to allow, Deny to reject with error,
    /// or Ignore to silently replace the result with NULL.
    ///
    /// `action` identifies the operation (READ, INSERT, DELETE, etc.).
    /// `arg1` and `arg2` provide context (table name, column name, etc.).
    /// `db_name` is the database name ("main", "temp", etc.).
    /// `trigger` is the name of the trigger if called from within one.
    fn authorize(
        &self,
        action: AuthAction,
        arg1: Option<&str>,
        arg2: Option<&str>,
        db_name: Option<&str>,
        trigger: Option<&str>,
    ) -> AuthResult;
}

/// Authorization action codes.
pub enum AuthAction {
    CreateIndex,
    CreateTable,
    CreateTempIndex,
    CreateTempTable,
    CreateTempTrigger,
    CreateTempView,
    CreateTrigger,
    CreateView,
    Delete,
    DropIndex,
    DropTable,
    DropTempIndex,
    DropTempTable,
    DropTempTrigger,
    DropTempView,
    DropTrigger,
    DropView,
    Insert,
    Pragma,
    Read,
    Select,
    Transaction,
    Update,
    Attach,
    Detach,
    AlterTable,
    Reindex,
    Analyze,
    CreateVtable,
    DropVtable,
    Function,
    Savepoint,
    Recursive,
}

pub enum AuthResult {
    Ok,
    Deny,
    Ignore,
}
```

### 9.5 Function Registry

```rust
/// Registry for scalar, aggregate, and window functions.
/// Supports both built-in functions and user-registered functions.
///
/// Functions are looked up by (name, arg_count). If an exact arg_count
/// match is not found, a variadic version (arg_count = -1) is tried.
pub struct FunctionRegistry {
    scalars: HashMap<FunctionKey, Arc<dyn ScalarFunction>>,
    aggregates: HashMap<FunctionKey, Arc<dyn AggregateFunction<State = Box<dyn Any + Send>>>>,
    windows: HashMap<FunctionKey, Arc<dyn WindowFunction<State = Box<dyn Any + Send>>>>,
}

#[derive(Hash, Eq, PartialEq)]
struct FunctionKey {
    name: String,      // case-insensitive (stored as uppercase)
    num_args: i32,     // -1 for variadic
}

impl FunctionRegistry {
    /// Register a scalar function. Overwrites any existing function
    /// with the same name and argument count.
    pub fn register_scalar(&mut self, func: Arc<dyn ScalarFunction>) { ... }

    /// Register an aggregate function.
    pub fn register_aggregate<F: AggregateFunction + 'static>(&mut self, func: F) { ... }

    /// Register a window function.
    pub fn register_window<F: WindowFunction + 'static>(&mut self, func: F) { ... }

    /// Look up a scalar function by name and argument count.
    /// Returns None if not found (caller should raise "no such function" error).
    pub fn find_scalar(&self, name: &str, num_args: i32)
        -> Option<Arc<dyn ScalarFunction>> { ... }

    /// Look up an aggregate function.
    pub fn find_aggregate(&self, name: &str, num_args: i32)
        -> Option<Arc<dyn AggregateFunction<State = Box<dyn Any + Send>>>> { ... }
}
```

### 9.6 Trait Composition: How Layers Connect

**Vfs + VfsFile -> Pager:** The Pager owns a `Box<dyn VfsFile>` for the database
file. It opens the file via `Vfs::open()` during connection setup.

**Pager + Wal -> MvccPager:** The MvccPager wraps both. `get_page()` checks the
version store first, then falls through to Pager (which checks WAL via WalIndex,
then reads from database file).

**MvccPager -> BtCursor:** Cursor calls `pager.get_page()` during traversal.
All page access goes through MVCC version resolution transparently.

**BtCursor -> VdbeCursor -> VDBE:** VDBE opcodes like `OpenRead` create
VdbeCursors wrapping BtCursors. `Column` extracts fields via cursor.

**VDBE + FunctionRegistry -> Execution:** `Function`/`PureFunc` opcodes look
up functions in the registry, call `invoke()`/`step()`/`finalize()`.

### 9.7 Mock Implementations for Testing

Each trait has a mock implementation for unit testing:

- `MockVfs` / `MockVfsFile`: Records all calls, returns configurable responses.
  Used in pager tests to simulate I/O errors.
- `MockMvccPager`: Returns pre-configured page data for given `(pgno, txn_id)`.
  Used in B-tree tests to isolate from MVCC.
- `MockBtreeCursor`: Returns pre-configured rows. Used in VDBE tests.
- `MockScalarFunction`: Returns a fixed value. Used in codegen tests.

---

## 10. Query Pipeline

```
SQL text
  |
  v
Lexer (memchr-accelerated, zero-copy token spans)
  |
  v
Parser (recursive descent, Pratt precedence for expressions)
  |
  v
AST (strongly typed enum hierarchy)
  |
  v
Name Resolution (table/column binding, * expansion)
  |
  v
Query Planning (index selection, cost estimation, join ordering)
  |
  v
VDBE Bytecode Generation (register-based VM, 190+ opcodes)
  |
  v
Execution (fetch-execute loop, match-based dispatch)
  |
  v
Results (iterator of Row, each row is a slice of SqliteValue)
```

### 10.1 Lexer Detail

The lexer converts SQL text into a stream of tokens. Each token carries a
`TokenType` discriminant and a `Span` (byte offset range in source).

**Token type enum (~150 variants):**

```rust
pub enum TokenType {
    // Literals
    Integer,          // 42, -7, 0xFF
    Float,            // 3.14, 1e10, .5
    String,           // 'hello', "hello" (SQL standard single-quote; double-quote for identifiers)
    Blob,             // X'CAFE', x'00ff'
    Variable,         // ?1, :name, @name, $name

    // Identifiers and keywords
    Id,               // unquoted identifier
    QuotedId,         // "quoted identifier" or [bracketed identifier] or `backtick`

    // Keywords (each is its own variant for fast matching)
    KwAbort, KwAction, KwAdd, KwAfter, KwAll, KwAlter, KwAlways,
    KwAnalyze, KwAnd, KwAs, KwAsc, KwAttach, KwAutoincrement,
    KwBefore, KwBegin, KwBetween, KwBy,
    KwCascade, KwCase, KwCast, KwCheck, KwCollate, KwColumn,
    KwCommit, KwConflict, KwConstraint, KwCreate, KwCross,
    KwCurrentDate, KwCurrentTime, KwCurrentTimestamp, KwConcurrent,
    KwDatabase, KwDefault, KwDeferrable, KwDeferred, KwDelete,
    KwDesc, KwDetach, KwDistinct, KwDo, KwDrop,
    KwEach, KwElse, KwEnd, KwEscape, KwExcept, KwExclusive,
    KwExists, KwExplain,
    KwFail, KwFilter, KwFirst, KwFollowing, KwFor, KwForeign, KwFrom, KwFull,
    KwGenerated, KwGlob, KwGroup, KwGroups,
    KwHaving,
    KwIf, KwIgnore, KwImmediate, KwIn, KwIndex, KwIndexed,
    KwInitially, KwInner, KwInsert, KwInstead, KwIntersect, KwInto, KwIs, KwIsnull,
    KwJoin,
    KwKey,
    KwLast, KwLeft, KwLike, KwLimit,
    KwMatch, KwMaterialized,
    KwNatural, KwNo, KwNot, KwNothing, KwNotnull, KwNull, KwNulls,
    KwOf, KwOffset, KwOn, KwOr, KwOrder, KwOuter, KwOver,
    KwPartition, KwPlan, KwPragma, KwPreceding, KwPrimary,
    KwQuery,
    KwRaise, KwRange, KwRecursive, KwReferences, KwRegexp, KwReindex,
    KwRelease, KwRename, KwReplace, KwRestrict, KwReturning, KwRight,
    KwRollback, KwRow, KwRows, KwRowid,
    KwSavepoint, KwSelect, KwSet, KwStrict,
    KwTable, KwTemp, KwTemporary, KwThen, KwTies, KwTo, KwTransaction, KwTrigger,
    KwUnbounded, KwUnion, KwUnique, KwUpdate, KwUsing,
    KwVacuum, KwValues, KwView, KwVirtual,
    KwWhen, KwWhere, KwWindow, KwWith, KwWithout,

    // Operators and punctuation
    Plus, Minus, Star, Slash, Percent,             // + - * / %
    Ampersand, Pipe, Tilde,                        // & | ~
    ShiftLeft, ShiftRight,                         // << >>
    Eq, Ne, Lt, Le, Gt, Ge,                        // = != < <= > >=
    EqEq, BangEq, LtGt,                           // == != <>
    Dot, Comma, Semicolon,                         // . , ;
    LeftParen, RightParen,                         // ( )
    Arrow, DoubleArrow,                            // -> ->>
    Concat,                                        // ||

    // Special
    Eof,              // end of input
    Error,            // lexer error (unterminated string, invalid character)

    // Whitespace and comments (not emitted to parser; consumed internally)
    // Whitespace, LineComment, BlockComment
}
```

**String/number/blob literal parsing:**

- **String literals:** Delimited by single quotes. Embedded quotes are escaped
  by doubling (`''`). The lexer uses `memchr` to find the closing quote
  efficiently. Scans forward from the opening quote; on finding `'`, checks
  if the next character is also `'` (escaped) or not (end of string).

- **Number literals:** Integer or float. The lexer recognizes:
  - Decimal integers: `[0-9]+`
  - Hex integers: `0x[0-9a-fA-F]+`
  - Floats: `[0-9]*\.[0-9]+([eE][+-]?[0-9]+)?` or `[0-9]+[eE][+-]?[0-9]+`
  - The token type is `Integer` or `Float` based on the presence of `.` or `e/E`.

- **Blob literals:** `X'[0-9a-fA-F]*'` or `x'...'`. Must have even number of
  hex digits. Odd count produces an `Error` token.

**Error tokens:** When the lexer encounters invalid input (unterminated string,
invalid hex in blob literal, unrecognized character), it emits an `Error` token
with a diagnostic message and the offending byte range. The parser can then
produce a user-friendly error with source location.

**Line/column tracking:** The lexer maintains `line: u32` and `col: u32`
counters, incremented on each newline. Every `Token` carries a `Span` with
byte offsets and the `(line, col)` at the token start. This enables error
messages like: `line 3, column 15: expected ')' but found ','`.

### 10.2 Parser Detail

Hand-written recursive descent, NOT a generated parser. Uses `parse.y`
(1,963 lines) as the authoritative grammar reference.

**Structure:** One method per grammar production. Each method consumes tokens
from the lexer and returns an AST node. Methods are named after the grammar
production they implement.

**Key parsing methods:**

```
parse_statement()              -> Statement
  parse_select_stmt()          -> SelectStatement
    parse_with_clause()        -> WithClause
    parse_select_core()        -> SelectCore
      parse_result_columns()   -> Vec<ResultColumn>
      parse_from_clause()      -> Option<TableRef>
        parse_join_clause()    -> JoinClause
      parse_where_clause()     -> Option<Expr>
      parse_group_by()         -> Option<GroupBy>
      parse_having()           -> Option<Expr>
      parse_window_clause()    -> Vec<WindowDef>
    parse_compound_op()        -> CompoundOp (UNION, INTERSECT, EXCEPT)
    parse_order_by()           -> Vec<OrderingTerm>
    parse_limit()              -> Option<LimitClause>
  parse_insert_stmt()          -> InsertStatement
    parse_upsert_clause()      -> Option<UpsertClause>
    parse_returning()          -> Option<Vec<ResultColumn>>
  parse_update_stmt()          -> UpdateStatement
  parse_delete_stmt()          -> DeleteStatement
  parse_create_table_stmt()    -> CreateTableStatement
    parse_column_def()         -> ColumnDef
    parse_table_constraint()   -> TableConstraint
  parse_create_index_stmt()    -> CreateIndexStatement
  parse_create_view_stmt()     -> CreateViewStatement
  parse_create_trigger_stmt()  -> CreateTriggerStatement
  parse_drop_stmt()            -> DropStatement
  parse_alter_table_stmt()     -> AlterTableStatement
  parse_begin_stmt()           -> BeginStatement
  parse_commit_stmt()          -> CommitStatement
  parse_rollback_stmt()        -> RollbackStatement
  parse_pragma_stmt()          -> PragmaStatement
  parse_explain_stmt()         -> ExplainStatement
  parse_expr()                 -> Expr (Pratt precedence)
    parse_prefix()             -> Expr (unary, literal, paren, subquery, case, cast, ...)
    parse_infix()              -> Expr (binary ops, BETWEEN, IN, LIKE, COLLATE, ...)
```

**Pratt precedence table for expressions:**

| Precedence | Operators | Associativity |
|------------|-----------|---------------|
| 1 (lowest) | OR | Left |
| 2 | AND | Left |
| 3 | NOT (prefix) | Right |
| 4 | =, ==, !=, <>, <, <=, >, >=, IS, IS NOT, IN, LIKE, GLOB, BETWEEN, MATCH, REGEXP | Left |
| 5 | ESCAPE | Left |
| 6 | &, \|, <<, >> | Left |
| 7 | +, - | Left |
| 8 | *, /, % | Left |
| 9 | \|\| (concat) | Left |
| 10 | COLLATE | Left |
| 11 | ~ (bitwise not), + (unary), - (unary) | Right |
| 12 (highest) | ->, ->> (JSON) | Left |

**Error recovery strategy:** On parse error, the parser:
1. Records the error (token, expected alternatives, source span).
2. Attempts to synchronize by skipping tokens until a "synchronization point"
   is found (semicolon, EOF, or a keyword that starts a new statement).
3. Continues parsing the next statement.
4. Returns all collected errors along with whatever AST was successfully parsed.

This allows the parser to report multiple errors in a single pass rather than
stopping at the first error.

### 10.3 AST Node Types

```rust
/// Top-level statement.
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    CreateTable(CreateTableStatement),
    CreateIndex(CreateIndexStatement),
    CreateView(CreateViewStatement),
    CreateTrigger(CreateTriggerStatement),
    CreateVirtualTable(CreateVirtualTableStatement),
    Drop(DropStatement),
    AlterTable(AlterTableStatement),
    Begin(BeginStatement),
    Commit,
    Rollback(RollbackStatement),
    Savepoint(String),
    Release(String),
    Attach(AttachStatement),
    Detach(String),
    Pragma(PragmaStatement),
    Vacuum(VacuumStatement),
    Reindex(Option<QualifiedName>),
    Analyze(Option<QualifiedName>),
    Explain { query_plan: bool, stmt: Box<Statement> },
}

pub struct SelectStatement {
    pub with: Option<WithClause>,
    pub body: SelectBody,
    pub order_by: Vec<OrderingTerm>,
    pub limit: Option<LimitClause>,
}

pub struct SelectBody {
    pub select: SelectCore,
    pub compounds: Vec<(CompoundOp, SelectCore)>,
}

pub struct SelectCore {
    pub distinct: Distinct,
    pub columns: Vec<ResultColumn>,
    pub from: Option<TableRef>,
    pub where_clause: Option<Expr>,
    pub group_by: Option<Vec<Expr>>,
    pub having: Option<Expr>,
    pub windows: Vec<WindowDef>,
}

pub enum Expr {
    Literal(Literal, Span),
    Column(ColumnRef, Span),
    BinaryOp { left: Box<Expr>, op: BinaryOp, right: Box<Expr>, span: Span },
    UnaryOp { op: UnaryOp, expr: Box<Expr>, span: Span },
    Between { expr: Box<Expr>, low: Box<Expr>, high: Box<Expr>, not: bool, span: Span },
    In { expr: Box<Expr>, set: InSet, not: bool, span: Span },
    Like { expr: Box<Expr>, pattern: Box<Expr>, escape: Option<Box<Expr>>, op: LikeOp, span: Span },
    Case { operand: Option<Box<Expr>>, whens: Vec<(Expr, Expr)>, else_: Option<Box<Expr>>, span: Span },
    Cast { expr: Box<Expr>, type_name: TypeName, span: Span },
    Exists { subquery: Box<SelectStatement>, not: bool, span: Span },
    Subquery(Box<SelectStatement>, Span),
    FunctionCall { name: String, args: Vec<Expr>, distinct: bool, filter: Option<Box<Expr>>, over: Option<WindowSpec>, span: Span },
    Collate { expr: Box<Expr>, collation: String, span: Span },
    IsNull { expr: Box<Expr>, not: bool, span: Span },
    Raise { action: RaiseAction, message: Option<String>, span: Span },
    JsonAccess { expr: Box<Expr>, path: Box<Expr>, arrow: JsonArrow, span: Span },
    Placeholder(PlaceholderType, Span),
}
```

### 10.4 Name Resolution

Name resolution transforms raw AST identifiers into fully-resolved references.

**Table alias binding:** When a FROM clause contains `table AS alias`, the
resolver creates a binding `alias -> table_schema`. Subsequent column references
can use either the table name or the alias.

**Column reference resolution:** For a reference like `t.col`:
1. Search the current scope's table aliases for `t`.
2. If found, verify `col` exists in that table's schema.
3. If `t` is omitted, search all tables in the FROM clause for a column
   named `col`. If found in exactly one table, resolve. If found in multiple
   tables, report "ambiguous column name" error.

**Star expansion:** `SELECT *` expands to all columns of all tables in the FROM
clause. `SELECT t.*` expands to all columns of table `t`.

**Subquery scoping:** Each subquery creates a new scope. Inner scopes can
reference outer scope columns (correlated subqueries). The resolver tracks
a stack of scopes. A column reference first checks the innermost scope, then
walks outward.

### 10.5 Query Planning

**Cost model:** The planner estimates the I/O cost (in page reads) for each
access path.

```
Full table scan:              cost = N_pages(table)
Index scan (range):           cost = log2(N_pages(index)) + selectivity * N_pages(table)
Index scan (equality):        cost = log2(N_pages(index)) + 1
Covering index scan:          cost = selectivity * N_pages(index)
Rowid lookup:                 cost = log2(N_pages(table))
```

**Index usability:** For each WHERE term, the planner determines if an index
can satisfy it:
- Equality (`col = expr`): usable if `col` is the leftmost column of an index.
- Range (`col > expr`, `col BETWEEN`): usable as the rightmost constraint.
- IN (`col IN (...)`): usable, expanded to multiple equality probes.
- LIKE (`col LIKE 'prefix%'`): usable if prefix is constant.

**Join ordering:** For N tables:
- N <= 6: exhaustive search of all N! orderings (at most 720).
- N > 6: greedy algorithm -- at each step, add the table that has the lowest
  estimated join cost with the already-joined set.

### 10.6 Code Generation

**SELECT -> VDBE opcodes:**
```
SELECT col FROM table WHERE rowid = ?
  Init       0, <end>
  Transaction 0, 0           # begin read transaction
  Integer    <bind>, 1       # load parameter into r1
  OpenRead   0, <root>, 0    # open cursor 0 on table
  SeekRowid  0, <notfound>, 1  # seek to rowid in r1
  Column     0, <col_idx>, 2   # extract column into r2
  ResultRow  2, 1              # emit r2 as result
  <notfound>:
  Close      0
  Halt       0, 0
  <end>:
```

**INSERT -> VDBE opcodes:**
```
INSERT INTO table VALUES (?, ?)
  Init       0, <end>
  Transaction 0, 1           # begin write transaction
  OpenWrite  0, <root>, 0    # open cursor 0 for writing
  NewRowid   0, 1            # generate new rowid into r1
  Variable   1, 2            # bind param 1 -> r2
  Variable   2, 3            # bind param 2 -> r3
  MakeRecord 2, 2, 4         # pack r2..r3 into record r4
  Insert     0, 4, 1         # insert record r4 with rowid r1
  Close      0
  Halt       0, 0
  <end>:
```

### 10.7 VDBE Instruction Format

```rust
pub struct VdbeOp {
    pub opcode: Opcode,    // u8, one of 190+ opcodes
    pub p1: i32,           // first operand (register, cursor, or literal)
    pub p2: i32,           // second operand (jump target, register, etc.)
    pub p3: i32,           // third operand
    pub p4: P4,            // extended operand
    pub p5: u16,           // flags
}

pub enum P4 {
    None,
    Int32(i32),
    Int64(i64),
    Real(f64),
    String(String),
    Blob(Vec<u8>),
    FuncDef(Arc<dyn ScalarFunction>),
    CollSeq(Arc<dyn CollationFunction>),
    KeyInfo(KeyInfo),        // column sort orders for index comparison
    Mem(Mem),                // pre-loaded register value
    Vtab(Arc<dyn VirtualTable>),
    Table(TableInfo),        // table metadata for Insert/Update
    Subprogram(VdbeProgram), // trigger sub-program
}
```

**Jump resolution:** During code generation, forward jumps target unknown
addresses. The codegen uses a label system: `emit_label()` returns a `Label`
handle, and `resolve_label(label, address)` patches all instructions that
reference that label. All labels must be resolved before execution begins.

**Register allocation:** Registers are numbered starting at 1. The codegen
allocates registers sequentially via `alloc_reg()` and `alloc_regs(n)`.
Temporary registers (used within a single opcode sequence) are allocated from
a pool and returned after use. Persistent registers (for result columns,
cursor positions) are allocated once and held for the statement's lifetime.

### 10.8 Coroutines

Subqueries and CTEs use the VDBE coroutine mechanism:

```
InitCoroutine  r_yield, <cte_end>, <cte_start>
  // ... CTE body: produces rows, each ending with Yield r_yield
  EndCoroutine r_yield
<cte_start>:
  // ... outer query: when it needs a row, executes Yield r_yield
  //     which transfers control to the CTE body
<cte_end>:
```

The `Yield` opcode swaps program counters between the outer query and the
coroutine. This allows the CTE to produce rows on-demand without materializing
the entire result set into a temporary table.

---

## 11. File Format Compatibility

FrankenSQLite reads and writes standard SQLite database files. This section
specifies every format detail needed for byte-level compatibility.

### 11.1 Database Header (100 bytes at offset 0)

Every field with exact byte offset, valid values, and what FrankenSQLite sets:

```
Offset  Size  Field                    Valid Values              FrankenSQLite Default
------  ----  -----                    ------------              ---------------------
  0      16   Magic string             "SQLite format 3\000"     Same (required)
 16       2   Page size                512,1024,2048,4096,       4096
                                       8192,16384,32768,
                                       1 (means 65536)
 18       1   Write version            1=journal, 2=WAL          2 (WAL mode default)
 19       1   Read version             1=journal, 2=WAL          2
 20       1   Reserved space/page      0..255                    0 (or 16 if page_checksum=ON)
 21       1   Max embed payload frac   64 (MUST be 64)           64
 22       1   Min embed payload frac   32 (MUST be 32)           32
 23       1   Leaf payload fraction    32 (MUST be 32)           32
 24       4   File change counter      any u32                   Incremented on commit
 28       4   Database size (pages)    0 or actual count         Actual count
 32       4   First freelist trunk     0 or page number          0 (empty freelist initially)
 36       4   Total freelist pages     count                     0
 40       4   Schema cookie            any u32                   Incremented on schema change
 44       4   Schema format number     1,2,3,4                   4 (current)
 48       4   Default cache size       PRAGMA default_cache_size  0 (use runtime default)
 52       4   Largest root b-tree page 0 or page# (auto-vacuum)  0
 56       4   Text encoding            1=UTF8, 2=UTF16le,        1 (UTF-8)
                                       3=UTF16be
 60       4   User version             any u32                   0
 64       4   Incremental vacuum       0 or non-zero             0
 68       4   Application ID           any u32                   0
 72      20   Reserved                 all zeros                 All zeros
 92       4   Version-valid-for        change counter value       Matches offset 24
 96       4   SQLite version number    X*1000000+Y*1000+Z        3052000 (3.52.0)
```

**Page size encoding:** The value 1 at offset 16-17 encodes a page size of
65536 (since 65536 does not fit in a u16). All other values are the literal
page size. Must be a power of 2 in the range [512, 65536].

**FrankenSQLite version number:** At offset 96, FrankenSQLite writes 3052000
(representing 3.52.0) to indicate compatibility with SQLite 3.52.0.

### 11.2 B-Tree Page Layout

**Page structure (top to bottom within a page):**

```
[Page header: 8 or 12 bytes]
[Cell pointer array: 2 * num_cells bytes]
[Unallocated space: variable]
[Cell content area: grows backward from end of page]
[Reserved space: reserved_per_page bytes at very end]
```

**Page 1 special case:** Page 1 has the 100-byte database header before the
B-tree page header. Cell pointer offsets on page 1 account for this prefix.
The usable start of page 1 is at byte 100.

**Cell pointer array:** Immediately after the page header. Each entry is a
2-byte big-endian u16 offset pointing to the start of a cell within the page.
The offsets are relative to the start of the page. Cells are stored from the
end of the page growing backward.

**Unallocated space:** Between the end of the cell pointer array and the start
of the cell content area. This is contiguous free space available for new cells.

**Freeblock list:** Within the cell content area, deleted cells form a linked
list of freeblocks. Each freeblock starts with a 2-byte pointer to the next
freeblock (0 if last) and a 2-byte size. Minimum freeblock size is 4 bytes.

**Fragmented bytes:** The page header byte at offset 7 counts bytes of space
lost to fragmentation (freeblocks smaller than 4 bytes, or gaps between cells
that are not tracked by the freeblock list). If this count reaches 60 or more,
the page is defragmented (cells are compacted toward the end of the page).

### 11.3 Cell Formats

**Table leaf cell (page type 0x0D):**
```
[payload_size: varint]    -- total bytes of payload
[rowid: varint]           -- integer primary key
[payload: bytes]          -- first min(payload_size, local_max) bytes
[overflow_pgno: u32BE]    -- only if payload overflows
```

**Table interior cell (page type 0x05):**
```
[left_child: u32BE]       -- 4-byte page number of left child
[rowid: varint]           -- divider key (integer)
```

**Index leaf cell (page type 0x0A):**
```
[payload_size: varint]    -- total bytes of payload
[payload: bytes]          -- first min(payload_size, local_max) bytes
[overflow_pgno: u32BE]    -- only if payload overflows
```

**Index interior cell (page type 0x02):**
```
[left_child: u32BE]       -- 4-byte page number of left child
[payload_size: varint]    -- total bytes of payload
[payload: bytes]          -- first min(payload_size, local_max) bytes
[overflow_pgno: u32BE]    -- only if payload overflows
```

### 11.4 Overflow Pages

**When overflow occurs:**

```
usable = page_size - reserved_per_page

Table leaf:
  max_local = usable - 35
  min_local = (usable - 12) * 32 / 255 - 23

Index (leaf and interior):
  max_local = (usable - 12) * 64 / 255 - 23
  min_local = (usable - 12) * 32 / 255 - 23

if payload_size <= max_local: all local, no overflow
else:
  local = min_local + (payload_size - min_local) % (usable - 4)
  if local > max_local: local = min_local
  overflow_bytes = payload_size - local
```

For 4096-byte page, 0 reserved: table leaf max_local = 4061, index max_local = 1003.

**Overflow page format:**
```
Offset  Size          Description
  0       4           Next overflow page number (0 if last)
  4       usable-4    Payload data
```

### 11.5 Freelist

**Trunk page format:**
```
Offset  Size    Description
  0       4     Next trunk page number (0 if last)
  4       4     Number of leaf page numbers (K)
  8       4*K   Array of leaf page numbers
```

Max leaves per trunk = (usable - 8) / 4 = 1022 for 4096-byte pages.

Header offset 32 = first trunk page; offset 36 = total freelist page count.

### 11.6 Pointer Map (Auto-Vacuum)

**Entry format (5 bytes per page):**
```
Byte 0:     Type code (1=root, 2=free, 3=overflow-first, 4=overflow-chain, 5=btree-child)
Bytes 1-4:  Parent page number (u32 BE)
```

**Location:** First pointer map page is always page 2.
entries_per_page = usable / 5. Group size = entries_per_page + 1.
Pointer map pages at: 2, 2+group_size, 2+2*group_size, ...

For 4096 pages: 819 entries/page, group size 820, pages at 2, 822, 1642, ...

### 11.7 Record Format Detail

**Structure:** `[header_size: varint] [serial_types: varint...] [data: bytes...]`

The header_size varint includes itself. Serial types encode both type and size.

**Serial types:**

| Value | Type | Content Bytes |
|-------|------|---------------|
| 0 | NULL | 0 |
| 1 | 8-bit signed int | 1 |
| 2 | 16-bit big-endian signed int | 2 |
| 3 | 24-bit big-endian signed int | 3 |
| 4 | 32-bit big-endian signed int | 4 |
| 5 | 48-bit big-endian signed int | 6 |
| 6 | 64-bit big-endian signed int | 8 |
| 7 | IEEE 754 64-bit float (BE) | 8 |
| 8 | Integer constant 0 | 0 |
| 9 | Integer constant 1 | 0 |
| 10,11 | Reserved (internal use) | - |
| N >= 12, even | BLOB of (N-12)/2 bytes | (N-12)/2 |
| N >= 13, odd | TEXT of (N-13)/2 bytes | (N-13)/2 |

**Worked example:** Row `(42, "hello", 3.14, NULL, X'CAFE')`:

Serial types: 1 (42 fits i8), 23 (5*2+13), 7 (float), 0 (NULL), 16 (2*2+12).
Header: [06, 01, 17, 07, 00, 10] (6 bytes total including size varint).
Data: [2A] [68 65 6C 6C 6F] [40 09 1E B8 51 EB 85 1F] [] [CA FE].
Total: 22 bytes.

### 11.8 WAL Header (32 bytes)

```
Offset  Size  Description
  0       4   Magic: 0x377F0682 (big-endian cksum) or 0x377F0683 (little-endian)
  4       4   Format version: 3007000
  8       4   Page size
 12       4   Checkpoint sequence number
 16       4   Salt-1
 20       4   Salt-2
 24       4   Checksum-1 (of bytes 0..23)
 28       4   Checksum-2 (of bytes 0..23)
```

### 11.9 WAL Frame Header (24 bytes)

```
Offset  Size  Description
  0       4   Page number
  4       4   For commit frames: db size in pages. Otherwise 0.
  8       4   Salt-1 (must match WAL header)
 12       4   Salt-2 (must match WAL header)
 16       4   Cumulative checksum-1
 20       4   Cumulative checksum-2
```

### 11.10 WAL Index (wal-index / SHM)

```
Header (136 bytes):
  [0..48]:   WAL index header (version, change counters, page counts)
  [48..96]:  Copy of header (lock-free reads: reader reads both, uses if they match)
  [96..136]: Lock bytes (8 read marks + write lock + checkpoint lock)

Hash table segments (32 KB each):
  Each covers up to 4062 frames.
  [0..16248]:     Page number array: 4062 entries x 4 bytes
  [16248..32760]: Hash table: 8192 slots x 2 bytes
                  Hash: page_number % 8192, linear probing
```

**Reader marks:** Byte offsets 100-131 contain 5 reader marks (u32 each).
Each reader mark records the WAL frame count at the time a reader began.
This prevents checkpoint from overwriting frames still needed by active readers.

### 11.11 sqlite_master Table

Every database contains a `sqlite_master` table (page 1 root) with this schema:

```sql
CREATE TABLE sqlite_master (
    type TEXT,      -- 'table', 'index', 'view', 'trigger'
    name TEXT,      -- object name
    tbl_name TEXT,  -- associated table name (for indexes/triggers: the table)
    rootpage INT,   -- root B-tree page number (0 for views/triggers)
    sql TEXT        -- CREATE statement text (NULL for auto-indexes)
);
```

For the temp database, the equivalent is `sqlite_temp_master`.

On database creation, FrankenSQLite creates page 1 as a table leaf page
containing zero rows in sqlite_master. The first `CREATE TABLE` inserts a
row into sqlite_master with the CREATE statement text.

### 11.12 Encoding

**Default:** UTF-8 (text encoding = 1 at header offset 56).

**UTF-16 alternatives:** UTF-16le (2) and UTF-16be (3) are supported. The
encoding is set at database creation and cannot be changed afterward. When
UTF-16 is used, all text stored in the database is UTF-16 encoded, and text
comparisons use UTF-16 collation.

**How encoding affects comparison:** The BINARY collation uses `memcmp` on
the raw bytes. For UTF-8, this produces correct Unicode code point ordering.
For UTF-16, byte-order matters (LE vs BE). NOCASE collation always operates
on Unicode code points regardless of encoding.

### 11.13 Page Size Constraints

- Minimum: 512 bytes
- Maximum: 65536 bytes
- Must be a power of 2
- The value 1 at header offset 16-17 encodes 65536 (since 65536 > u16::MAX)
- Page size is set at database creation and cannot be changed (except by VACUUM INTO)
- FrankenSQLite default: 4096 (matches modern filesystem block size and SSD page size)

## 12. SQL Coverage

FrankenSQLite implements the full SQLite 3.52.0 SQL dialect. This section
specifies every supported syntactic form with semantic details sufficient
to drive parser, planner, and VDBE codegen implementation.

### 12.1 SELECT

The SELECT statement is the most complex production in the SQLite grammar.
The full syntax tree is:

```sql
SELECT [DISTINCT | ALL] result-column [, result-column]*
  FROM table-or-subquery [join-clause]*
  [WHERE expr]
  [GROUP BY expr [, expr]* [HAVING expr]]
  [WINDOW window-defn [, window-defn]*]
  [ORDER BY ordering-term [, ordering-term]*]
  [LIMIT expr [OFFSET expr | , expr]]
```

**result-column** forms:
- `*` -- all columns from all tables in FROM
- `table-name.*` -- all columns from a specific table
- `expr [AS alias]` -- computed expression with optional alias

**FROM clause** table sources:
- Table name: `FROM t1`
- Table alias: `FROM t1 AS a`
- Indexed hint: `FROM t1 INDEXED BY idx_name` or `FROM t1 NOT INDEXED`
- Subquery: `FROM (SELECT ...) AS sub`
- Table-valued function: `FROM json_each(col)` or `FROM generate_series(1,100)`
- Multiple tables (implicit CROSS JOIN): `FROM t1, t2`

**JOIN types** (all produce VDBE nested-loop or hash join opcodes):
- `INNER JOIN ... ON expr` / `JOIN ... ON expr`
- `LEFT [OUTER] JOIN ... ON expr`
- `RIGHT [OUTER] JOIN ... ON expr` (SQLite 3.39+)
- `FULL [OUTER] JOIN ... ON expr` (SQLite 3.39+)
- `CROSS JOIN` (optimizer will not reorder)
- `NATURAL JOIN` (implicit ON using shared column names)
- `... USING (col1, col2)` (explicit shared columns)

**Compound SELECT operators** (vertically combine result sets):
- `UNION` -- deduplicate
- `UNION ALL` -- keep duplicates
- `INTERSECT` -- rows present in both
- `EXCEPT` -- rows in left but not right

Compound operators bind left-to-right. ORDER BY and LIMIT apply to the
entire compound result, not individual SELECT arms. Column names come from
the first (leftmost) SELECT.

**Common Table Expressions (CTEs):**
```sql
WITH [RECURSIVE]
  cte_name [(col1, col2, ...)] AS [NOT MATERIALIZED | MATERIALIZED] (
    select-stmt
  ) [, ...]
SELECT ... FROM cte_name ...
```

Recursive CTEs require `UNION ALL` between the base case and the recursive
step. The recursive step may reference `cte_name` exactly once. Cycle
detection is not automatic; use `LIMIT` to prevent infinite recursion.
`MATERIALIZED` forces the CTE to be evaluated once and stored as a temp
table. `NOT MATERIALIZED` allows the optimizer to inline the CTE as a
subquery (default behavior for non-recursive CTEs referenced once).

**Window functions:**
```sql
SELECT func(args) OVER (
  [PARTITION BY expr [, expr]*]
  [ORDER BY ordering-term [, ordering-term]*]
  [frame-spec]
)

frame-spec :=
  { RANGE | ROWS | GROUPS }
  { BETWEEN frame-bound AND frame-bound | frame-bound }

frame-bound :=
  UNBOUNDED PRECEDING
  | expr PRECEDING
  | CURRENT ROW
  | expr FOLLOWING
  | UNBOUNDED FOLLOWING

EXCLUDE := EXCLUDE { NO OTHERS | CURRENT ROW | GROUP | TIES }
```

Default frame: `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` when
ORDER BY is present; `RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED
FOLLOWING` when ORDER BY is absent.

**DISTINCT processing:** Implemented via a temporary B-tree index for
deduplication. The VDBE uses `OP_Found` / `OP_NotFound` on the temp
index to check for duplicates before emitting rows.

**LIMIT and OFFSET:** LIMIT takes a non-negative integer expression. OFFSET
takes a non-negative integer expression. The alternative form
`LIMIT count, offset` (offset as second argument) is supported for
backward compatibility. Negative LIMIT means unlimited. Negative OFFSET
is treated as zero.

### 12.2 INSERT

```sql
INSERT [OR conflict-clause] INTO table-name [(col-list)]
  { VALUES (expr, ...) [, (expr, ...)]* | select-stmt | DEFAULT VALUES }
  [upsert-clause]
  [RETURNING result-column [, result-column]*]
```

**Conflict resolution clauses** (OR keyword forms):
- `INSERT OR ABORT` -- default, abort current statement on conflict
- `INSERT OR ROLLBACK` -- rollback entire transaction on conflict
- `INSERT OR FAIL` -- abort statement but keep prior changes from same statement
- `INSERT OR IGNORE` -- silently skip conflicting row
- `INSERT OR REPLACE` -- delete existing conflicting row, then insert new

**UPSERT (ON CONFLICT):**
```sql
INSERT INTO t1 (a, b, c) VALUES (1, 2, 3)
  ON CONFLICT (a) DO UPDATE SET b = excluded.b, c = excluded.c
  WHERE excluded.c > t1.c;

INSERT INTO t1 (a, b) VALUES (1, 2)
  ON CONFLICT (a) DO NOTHING;

-- Multiple ON CONFLICT clauses (SQLite 3.35+):
INSERT INTO t1 (a, b, c) VALUES (1, 2, 3)
  ON CONFLICT (a) DO UPDATE SET b = excluded.b
  ON CONFLICT (b) DO NOTHING;
```

The `excluded` pseudo-table refers to the row that would have been inserted.
The conflict target `(column-list)` must match a UNIQUE index or PRIMARY KEY.
An optional WHERE clause on the conflict target restricts which index to match.
The DO UPDATE SET clause can reference both `excluded.*` and the original
table columns.

**RETURNING clause** (SQLite 3.35+): Returns the rows actually inserted,
including any default values, autoincrement values, and trigger-generated
modifications. Each returned row has columns matching the result-column
list. The RETURNING clause is processed after triggers.

**Multi-row VALUES:** `VALUES (1,'a'), (2,'b'), (3,'c')` inserts three
rows atomically within the same statement. The VDBE generates a loop over
the value lists.

**INSERT from SELECT:** `INSERT INTO t1 SELECT * FROM t2 WHERE ...`
streams rows from the SELECT result directly into the B-tree insert path.

**DEFAULT VALUES:** `INSERT INTO t1 DEFAULT VALUES` inserts a single row
using the DEFAULT expression for every column (NULL if no DEFAULT defined).

### 12.3 UPDATE

```sql
UPDATE [OR conflict-clause] table-name
  SET col = expr [, col = expr]*
  [FROM table-or-subquery [, table-or-subquery]*]
  [WHERE expr]
  [ORDER BY ordering-term [, ordering-term]*]
  [LIMIT expr [OFFSET expr]]
  [RETURNING result-column [, result-column]*]
```

**UPDATE FROM** (SQLite 3.33+): The FROM clause provides additional tables
for the SET expressions and WHERE clause, enabling UPDATE-with-JOIN:
```sql
UPDATE inventory SET quantity = inventory.quantity - orders.qty
  FROM orders
  WHERE inventory.product_id = orders.product_id
    AND orders.status = 'pending';
```
When the FROM clause is present and a row in the target table joins with
multiple rows from the FROM tables, the update is applied once with an
arbitrarily chosen matching row (implementation-defined which).

**ORDER BY + LIMIT on UPDATE:** Non-standard but SQLite-supported. Useful
for "update the top N rows" patterns:
```sql
UPDATE log SET processed = 1
  ORDER BY created_at ASC
  LIMIT 100;
```
Requires that ORDER BY columns identify a unique ordering; otherwise
the set of updated rows is non-deterministic.

### 12.4 DELETE

```sql
DELETE FROM table-name
  [WHERE expr]
  [ORDER BY ordering-term [, ordering-term]*]
  [LIMIT expr [OFFSET expr]]
  [RETURNING result-column [, result-column]*]
```

**ORDER BY + LIMIT on DELETE:** Same non-standard extension as UPDATE:
```sql
DELETE FROM log
  ORDER BY created_at ASC
  LIMIT 1000;
```

**Truncate optimization:** `DELETE FROM table_name` without WHERE is
optimized to drop and recreate the B-tree root page rather than deleting
rows one by one, unless triggers or foreign keys prevent it.

### 12.5 DDL: CREATE TABLE

```sql
CREATE [TEMP | TEMPORARY] TABLE [IF NOT EXISTS] [schema.]table-name (
  column-def [, column-def | table-constraint]*
) [WITHOUT ROWID] [STRICT];

CREATE [TEMP | TEMPORARY] TABLE [IF NOT EXISTS] [schema.]table-name
  AS select-stmt;
```

**Column definition:**
```sql
column-name [type-name] [column-constraint]*

column-constraint :=
  PRIMARY KEY [ASC | DESC] [conflict-clause] [AUTOINCREMENT]
  | NOT NULL [conflict-clause]
  | UNIQUE [conflict-clause]
  | CHECK (expr)
  | DEFAULT (expr) | DEFAULT literal | DEFAULT signed-number
  | COLLATE collation-name
  | REFERENCES foreign-table [(foreign-column)] [foreign-key-clause]
  | [GENERATED ALWAYS] AS (expr) [STORED | VIRTUAL]
```

**Table constraints:**
```sql
table-constraint :=
  PRIMARY KEY (indexed-column [, indexed-column]*) [conflict-clause]
  | UNIQUE (indexed-column [, indexed-column]*) [conflict-clause]
  | CHECK (expr)
  | FOREIGN KEY (column [, column]*) REFERENCES foreign-table
      [(column [, column]*)] [foreign-key-clause]
```

**Conflict clause** on constraints: `ON CONFLICT {ROLLBACK | ABORT | FAIL | IGNORE | REPLACE}`.

**Type affinity** is determined from the declared type name using these rules
(applied in order, first match wins):
1. Contains "INT" -> INTEGER affinity
2. Contains "CHAR", "CLOB", or "TEXT" -> TEXT affinity
3. Contains "BLOB" or no type name -> BLOB affinity (NONE)
4. Contains "REAL", "FLOA", or "DOUB" -> REAL affinity
5. Otherwise -> NUMERIC affinity

**WITHOUT ROWID tables:** The table uses an index B-tree (clustered on
PRIMARY KEY) instead of a table B-tree. Requires an explicit PRIMARY KEY.
Implications: no `rowid` pseudo-column, no `AUTOINCREMENT`, `INTEGER
PRIMARY KEY` is NOT an alias for `rowid`, sort order is determined by the
PRIMARY KEY declaration including COLLATE and ASC/DESC.

**STRICT tables** (SQLite 3.37+): Column type names are restricted to
exactly INT, INTEGER, REAL, TEXT, BLOB, or ANY. Type checking is enforced
on INSERT/UPDATE: a TEXT value cannot be stored in an INT column. ANY
columns accept any type without coercion.

**Generated columns** (SQLite 3.31+):
- `VIRTUAL`: Computed on read, not stored on disk. Cannot be indexed
  directly (but expression indexes can reference the underlying expression).
- `STORED`: Computed on INSERT/UPDATE, stored on disk. Can be indexed.
- Generated columns cannot reference other generated columns that come
  later in the column definition list.

**AUTOINCREMENT:** Only valid on `INTEGER PRIMARY KEY`. Guarantees that
rowids are never reused (uses the `sqlite_sequence` system table to track
the highest ever allocated). Without AUTOINCREMENT, rowids may be reused
after DELETE.

**Foreign key clause details:**
```sql
REFERENCES parent-table [(parent-column)]
  [ON DELETE {SET NULL | SET DEFAULT | CASCADE | RESTRICT | NO ACTION}]
  [ON UPDATE {SET NULL | SET DEFAULT | CASCADE | RESTRICT | NO ACTION}]
  [MATCH {SIMPLE | PARTIAL | FULL}]
  [[NOT] DEFERRABLE [INITIALLY DEFERRED | INITIALLY IMMEDIATE]]
```

Foreign key enforcement requires `PRAGMA foreign_keys = ON` (off by
default for backward compatibility).

### 12.6 DDL: CREATE INDEX

```sql
CREATE [UNIQUE] INDEX [IF NOT EXISTS] [schema.]index-name
  ON table-name (indexed-column [, indexed-column]*)
  [WHERE expr];

indexed-column := { column-name | expr } [COLLATE collation-name] [ASC | DESC]
```

**Partial indexes:** The WHERE clause restricts which rows appear in the
index. The query planner can only use a partial index if the query's WHERE
clause implies the index's WHERE clause. Example:
```sql
CREATE INDEX idx_active ON users(email) WHERE active = 1;
-- Usable by: SELECT * FROM users WHERE active = 1 AND email = ?
-- NOT usable by: SELECT * FROM users WHERE email = ?
```

**Expression indexes:** Index on computed expressions, not just column names:
```sql
CREATE INDEX idx_lower_email ON users(lower(email));
-- Usable by: SELECT * FROM users WHERE lower(email) = ?
```

The VDBE computes the expression for each row during index construction and
maintenance. The planner matches query expressions against index expressions
using structural equality of the AST after normalization.

### 12.7 DDL: CREATE VIEW

```sql
CREATE [TEMP | TEMPORARY] VIEW [IF NOT EXISTS] [schema.]view-name
  [(column-alias [, column-alias]*)]
  AS select-stmt;
```

Views are expanded inline during query compilation (they are not
materialized unless wrapped in a CTE with `MATERIALIZED`). Column aliases,
if provided, override the column names from the SELECT. Views can reference
CTEs, including recursive CTEs, to create recursive views.

Views are read-only unless an INSTEAD OF trigger is defined.

### 12.8 DDL: CREATE TRIGGER

```sql
CREATE [TEMP | TEMPORARY] TRIGGER [IF NOT EXISTS] [schema.]trigger-name
  {BEFORE | AFTER | INSTEAD OF}
  {DELETE | INSERT | UPDATE [OF column [, column]*]}
  ON table-name
  [FOR EACH ROW]
  [WHEN expr]
BEGIN
  dml-statement; [dml-statement; ...]
END;
```

**Trigger timing:**
- `BEFORE`: Fires before the DML operation. Can modify or prevent the
  operation by raising an error via `RAISE()`.
- `AFTER`: Fires after the DML operation has completed.
- `INSTEAD OF`: Only valid on views. Replaces the DML operation entirely.

**OLD and NEW pseudo-tables:**
- `INSERT` triggers: `NEW` refers to the inserted row. `OLD` is not available.
- `DELETE` triggers: `OLD` refers to the deleted row. `NEW` is not available.
- `UPDATE` triggers: Both `OLD` (pre-update) and `NEW` (post-update) are
  available.

**WHEN clause:** The trigger body only executes if the WHEN expression
evaluates to true. The WHEN clause can reference `OLD` and `NEW`.

**Trigger body:** May contain multiple DML statements (INSERT, UPDATE,
DELETE, SELECT). Each statement can reference `OLD`, `NEW`, and
`RAISE(IGNORE)`, `RAISE(ROLLBACK, msg)`, `RAISE(ABORT, msg)`,
`RAISE(FAIL, msg)`.

**Recursive triggers:** Enabled by `PRAGMA recursive_triggers = ON`.
When enabled, a trigger can cause itself to fire again. Maximum recursion
depth is controlled by `SQLITE_MAX_TRIGGER_DEPTH` (default 1000).

### 12.9 DDL: Other

**ALTER TABLE:**
```sql
ALTER TABLE table-name RENAME TO new-table-name;
ALTER TABLE table-name RENAME COLUMN old-name TO new-name;
ALTER TABLE table-name ADD COLUMN column-def;
ALTER TABLE table-name DROP COLUMN column-name;
```

DROP COLUMN (SQLite 3.35+) rewrites the table if the column is not the
last column and is referenced by indexes or constraints.

**DROP statements:**
```sql
DROP TABLE [IF EXISTS] [schema.]table-name;
DROP INDEX [IF EXISTS] [schema.]index-name;
DROP VIEW [IF EXISTS] [schema.]view-name;
DROP TRIGGER [IF EXISTS] [schema.]trigger-name;
```

### 12.10 Transaction Control

```sql
BEGIN [DEFERRED | IMMEDIATE | EXCLUSIVE | CONCURRENT] [TRANSACTION];
COMMIT [TRANSACTION];
END [TRANSACTION];           -- synonym for COMMIT
ROLLBACK [TRANSACTION];

SAVEPOINT savepoint-name;
RELEASE [SAVEPOINT] savepoint-name;
ROLLBACK [TRANSACTION] TO [SAVEPOINT] savepoint-name;
```

**Transaction modes:**
- `DEFERRED` (default): No locks acquired until first read/write.
- `IMMEDIATE`: Acquires a RESERVED lock immediately (blocks other writers).
- `EXCLUSIVE`: Acquires an EXCLUSIVE lock immediately (blocks readers too,
  in rollback journal mode; equivalent to IMMEDIATE in WAL mode).
- `CONCURRENT`: FrankenSQLite extension. Enters MVCC concurrent writer mode
  with Snapshot Isolation. Multiple CONCURRENT transactions can write
  simultaneously to different pages. Conflict on the same page results in
  `SQLITE_BUSY_SNAPSHOT` for the second committer.

**Savepoints** form a stack. `RELEASE X` commits all work since `SAVEPOINT X`
and removes X and all more recent savepoints from the stack. `ROLLBACK TO X`
undoes all work since `SAVEPOINT X` but leaves X on the stack (allowing
further work within the same savepoint scope).

### 12.11 ATTACH / DETACH

```sql
ATTACH [DATABASE] expr AS schema-name;
DETACH [DATABASE] schema-name;
```

`expr` evaluates to a filename string. The attached database gets the schema
name and its tables are accessible as `schema-name.table-name`. The main
database is always named `main`. The temp database is always named `temp`.
Maximum 10 attached databases by default (`SQLITE_MAX_ATTACHED`). Cross-database
transactions are atomic only in rollback journal mode (not WAL mode in
standard SQLite; FrankenSQLite preserves this limitation initially, with
cross-database atomic WAL transactions as future work).

### 12.12 EXPLAIN and EXPLAIN QUERY PLAN

```sql
EXPLAIN statement;
EXPLAIN QUERY PLAN statement;
```

**EXPLAIN** returns the VDBE bytecode program as a result set with columns:
`addr`, `opcode`, `p1`, `p2`, `p3`, `p4`, `p5`, `comment`. Each row is one
VDBE instruction. This is the primary debugging tool for understanding query
execution.

**EXPLAIN QUERY PLAN** returns a high-level description of the query plan
with columns: `id`, `parent`, `notused`, `detail`. The `detail` column
contains human-readable text describing scan order, index usage, and sort
operations. Tree structure is encoded via `id`/`parent` relationships.

### 12.13 VACUUM

```sql
VACUUM [schema-name];
VACUUM [schema-name] INTO filename;
```

`VACUUM` rebuilds the database file, reclaiming free pages and defragmenting.
It works by creating a new database, copying all content, then replacing the
original. `VACUUM INTO` writes the rebuilt database to a new file without
modifying the original, functioning as a compact backup.

### 12.14 Other Statements

```sql
REINDEX [collation-name | [schema.]table-or-index-name];
ANALYZE [schema-name | table-or-index-name];
PRAGMA [schema.]pragma-name [= value | (value)];
```

`ANALYZE` populates `sqlite_stat1` and optionally `sqlite_stat4` tables with
index statistics used by the query planner for cost estimation. `REINDEX`
rebuilds indexes after collation sequence changes.

### 12.15 Expression Syntax

Full expression grammar including all operators by precedence (highest first):

| Precedence | Operators |
|-----------|-----------|
| 1 (highest) | `~` (bitwise NOT), unary `+`, unary `-`, `NOT` |
| 2 | `||` (string concat) |
| 3 | `*`, `/`, `%` |
| 4 | `+`, `-` |
| 5 | `<<`, `>>`, `&`, `|` |
| 6 | `<`, `<=`, `>`, `>=` |
| 7 | `=`, `==`, `!=`, `<>`, `IS`, `IS NOT`, `IS DISTINCT FROM`, `IS NOT DISTINCT FROM`, `IN`, `LIKE`, `GLOB`, `MATCH`, `REGEXP`, `BETWEEN` |
| 8 | `AND` |
| 9 (lowest) | `OR` |

**Special expression forms:**
- `CAST(expr AS type-name)` -- explicit type conversion
- `CASE [expr] WHEN expr THEN expr [ELSE expr] END` -- conditional
- `EXISTS (select-stmt)` -- subquery existence test
- `expr [NOT] IN (select-stmt | expr-list)` -- membership test
- `expr [NOT] BETWEEN expr AND expr` -- range test
- `expr COLLATE collation-name` -- collation override
- `expr [NOT] LIKE pattern [ESCAPE char]` -- pattern match (% and _)
- `expr [NOT] GLOB pattern` -- case-sensitive glob (* and ?)
- `RAISE(IGNORE | ROLLBACK,msg | ABORT,msg | FAIL,msg)` -- trigger only
- `expr -> path` -- JSON extract (returns JSON)
- `expr ->> path` -- JSON extract (returns SQL value)

### 12.16 Type Affinity Rules

Five affinities: TEXT, NUMERIC, INTEGER, REAL, BLOB.

**Affinity determination from declared type** (first match wins):
1. Type name contains "INT" -> INTEGER
2. Type name contains "CHAR", "CLOB", or "TEXT" -> TEXT
3. Type name contains "BLOB" or is empty -> BLOB
4. Type name contains "REAL", "FLOA", or "DOUB" -> REAL
5. Otherwise -> NUMERIC

**Comparison affinity rules** (when comparing values of different types):
- If either operand has INTEGER, REAL, or NUMERIC affinity, apply numeric
  affinity to both operands
- If either operand has TEXT affinity and neither has numeric affinity, apply
  TEXT affinity
- Otherwise, no affinity applied (BLOB comparison)

---

## 13. Built-in Functions

FrankenSQLite implements all built-in functions from SQLite 3.52.0. All
functions follow SQLite's NULL propagation rule: if any argument is NULL,
the result is NULL, unless the function is specifically documented to handle
NULL differently.

### 13.1 Core Scalar Functions

**abs(X)** -> integer or real. Returns the absolute value of X. If X is
NULL, returns NULL. If X is the integer -9223372036854775808 (minimum i64),
an integer overflow error is raised because the result cannot be represented
as a positive i64. If X is a string that looks numeric, it is coerced.

**char(X1, X2, ..., XN)** -> text. Returns a string composed of characters
with Unicode code points X1 through XN. NULL arguments are silently skipped.

**coalesce(X, Y, ...)** -> any. Returns the first non-NULL argument. If all
arguments are NULL, returns NULL. Short-circuits: arguments after the first
non-NULL are not evaluated.

**concat(X, Y, ...)** -> text (SQLite 3.44+). Concatenates all arguments
as text. NULL arguments are treated as empty strings (unlike `||` which
propagates NULL).

**concat_ws(SEP, X, Y, ...)** -> text (SQLite 3.44+). Concatenates with
separator. NULL arguments are skipped entirely (no double separators).

**format(FORMAT, ...)** / **printf(FORMAT, ...)** -> text. SQL-specific
printf with format specifiers:
- `%d` -- integer (truncates floating point)
- `%f` -- floating point (default 6 decimal places)
- `%e` / `%E` -- scientific notation
- `%g` / `%G` -- shorter of %f and %e
- `%s` -- string (NULL renders as empty string)
- `%q` -- string with single-quotes doubled (for SQL literals)
- `%Q` -- like %q but wraps in single quotes, NULL renders as `NULL` (unquoted)
- `%w` -- like %q but wraps in double quotes (for identifiers)
- `%c` -- character from integer code point
- `%n` -- length of string so far (written to argument, not output)
- `%z` -- same as %s (compatibility)
- `%%` -- literal percent sign
Width, precision, and flag modifiers (`-`, `+`, ` `, `0`) are supported.

**glob(PATTERN, STRING)** -> integer (0 or 1). Case-sensitive glob match.
`*` matches any sequence, `?` matches any single character, `[...]` matches
character classes. This is the function form of the `GLOB` operator.

**hex(X)** -> text. Returns the hexadecimal rendering of X. If X is a blob,
each byte becomes two hex characters. If X is a number or text, it is first
converted to its blob representation.

**iif(COND, X, Y)** -> any. Equivalent to `CASE WHEN COND THEN X ELSE Y END`.
Short-circuits evaluation.

**ifnull(X, Y)** -> any. Returns X if X is not NULL, otherwise Y.
Equivalent to `coalesce(X, Y)`.

**instr(X, Y)** -> integer. Returns the 1-based position of the first
occurrence of Y in X, or 0 if not found. If either argument is NULL,
returns NULL. For blob arguments, operates on bytes; for text, operates
on characters.

**last_insert_rowid()** -> integer. Returns the rowid of the most recent
successful INSERT on the same database connection.

**length(X)** -> integer. For text: number of characters (not bytes). For
blob: number of bytes. For NULL: NULL. For numbers: length of text
representation.

**like(PATTERN, STRING [, ESCAPE])** -> integer. Case-insensitive pattern
match. `%` matches any sequence, `_` matches any single character. Optional
ESCAPE character. This is the function form of the `LIKE` operator.

**likelihood(X, P)** -> any. Returns X unchanged. Hints to the query
planner that X is true with probability P (0.0 to 1.0). P must be a
compile-time constant.

**likely(X)** -> any. Equivalent to `likelihood(X, 0.9375)`.

**unlikely(X)** -> any. Equivalent to `likelihood(X, 0.0625)`.

**lower(X)** -> text. Converts ASCII characters to lowercase. For full
Unicode case folding, the ICU extension is required.

**upper(X)** -> text. Converts ASCII characters to uppercase.

**ltrim(X [, Y])** -> text. Removes characters in Y from the left of X.
Default Y is spaces.

**rtrim(X [, Y])** -> text. Removes characters in Y from the right of X.

**trim(X [, Y])** -> text. Removes characters in Y from both sides of X.

**max(X, Y, ...)** -> any. Returns the argument with the maximum value.
Uses the standard SQLite comparison rules. NULL arguments are ignored
(returns NULL only if all arguments are NULL). When used as a scalar
function (not aggregate), handles 2+ arguments.

**min(X, Y, ...)** -> any. Returns the argument with the minimum value.

**nullif(X, Y)** -> any. Returns NULL if X = Y, otherwise returns X.

**octet_length(X)** -> integer (SQLite 3.43+). Returns the number of bytes
in the text or blob representation of X, without any type conversion.

**quote(X)** -> text. Returns X in a form suitable for inclusion in SQL.
Text is single-quoted with internal quotes doubled. Blobs become `X'hex'`.
NULL becomes the string `NULL`. Numbers are rendered as-is.

**random()** -> integer. Returns a pseudo-random 64-bit signed integer.
Uses a PRNG seeded from the system entropy source at connection open.

**randomblob(N)** -> blob. Returns an N-byte blob of pseudo-random data.

**replace(X, Y, Z)** -> text. Replaces every occurrence of Y in X with Z.
If Y is empty string, returns X unchanged.

**round(X [, N])** -> real. Rounds X to N decimal places (default 0).
Uses banker's rounding (round half to even) for exact halfway cases.

**sign(X)** -> integer. Returns -1, 0, or +1 for negative, zero, or
positive X. Returns NULL for NULL. Returns NULL for non-numeric strings.

**soundex(X)** -> text. Returns the Soundex encoding of X as a 4-character
string (letter + 3 digits). Returns `?000` for empty or NULL input.

**substr(X, START [, LENGTH])** / **substring(X, START [, LENGTH])** -> text
or blob. 1-based indexing. Negative START counts from the end. If LENGTH is
negative, returns characters to the left of START. For blob arguments,
operates on bytes.

**typeof(X)** -> text. Returns `"null"`, `"integer"`, `"real"`, `"text"`,
or `"blob"`.

**unhex(X [, Y])** -> blob (SQLite 3.41+). Decodes hex string X into blob.
Y specifies characters to ignore (e.g., spaces, dashes). Returns NULL if X
contains invalid hex characters (after removing Y characters).

**unicode(X)** -> integer. Returns the Unicode code point of the first
character of text X.

**unistr(X)** -> text (SQLite 3.45+). Interprets `\uXXXX` and `\UXXXXXXXX`
escape sequences in X.

**zeroblob(N)** -> blob. Returns a blob consisting of N zero bytes.
Efficiently represented internally without allocating N bytes.

**sqlite_version()** -> text. Returns the version string (e.g., "3.52.0").
FrankenSQLite returns its own version but the format matches SQLite.

**sqlite_source_id()** -> text. Returns source identification string.

**changes()** -> integer. Returns the number of rows modified by the most
recent INSERT, UPDATE, or DELETE on the same connection.

**total_changes()** -> integer. Returns the total number of rows modified
since the connection was opened.

**sqlite_offset(X)** -> integer. Returns the byte offset of the column X
in the database file. Only meaningful within a query; requires that X be a
direct column reference (not an expression).

### 13.2 Math Functions (SQLite 3.35+)

All math functions return NULL for NULL input. For domain errors (e.g.,
sqrt of negative), the behavior depends on the function.

**acos(X)** -> real. Arc cosine. Domain: [-1, 1]. Returns NULL for out-of-domain.
**acosh(X)** -> real. Inverse hyperbolic cosine. Domain: [1, +inf).
**asin(X)** -> real. Arc sine. Domain: [-1, 1].
**asinh(X)** -> real. Inverse hyperbolic sine. Domain: all reals.
**atan(X)** -> real. Arc tangent. Domain: all reals.
**atan2(Y, X)** -> real. Two-argument arc tangent. Returns angle in radians.
**atanh(X)** -> real. Inverse hyperbolic tangent. Domain: (-1, 1).
**ceil(X)** / **ceiling(X)** -> integer. Smallest integer >= X.
**cos(X)** -> real. Cosine (X in radians).
**cosh(X)** -> real. Hyperbolic cosine.
**degrees(X)** -> real. Converts radians to degrees.
**exp(X)** -> real. e raised to the power X. Overflow returns +Inf.
**floor(X)** -> integer. Largest integer <= X.
**ln(X)** -> real. Natural logarithm. Domain: (0, +inf). Returns NULL for X <= 0.
**log(X)** / **log10(X)** -> real. Base-10 logarithm.
**log(B, X)** -> real. Base-B logarithm. Computed as ln(X)/ln(B).
**log2(X)** -> real. Base-2 logarithm.
**mod(X, Y)** -> real or integer. Remainder of X/Y. Returns NULL if Y is 0.
**pi()** -> real. Returns 3.141592653589793.
**pow(X, Y)** / **power(X, Y)** -> real. X raised to the power Y.
**radians(X)** -> real. Converts degrees to radians.
**sign(X)** -> integer. -1, 0, or +1 (also listed in core scalars).
**sin(X)** -> real. Sine (X in radians).
**sinh(X)** -> real. Hyperbolic sine.
**sqrt(X)** -> real. Square root. Returns NULL for negative X.
**tan(X)** -> real. Tangent (X in radians).
**tanh(X)** -> real. Hyperbolic tangent.
**trunc(X)** -> integer. Truncates toward zero.

**NaN and Inf handling:** SQLite does not have first-class NaN or Inf values.
If a math function would return NaN (e.g., `0.0/0.0`), the result is NULL.
If a function would return +/-Inf (e.g., `exp(1000)`), the behavior is
platform-dependent in C SQLite; FrankenSQLite returns NULL for Inf to
maintain deterministic behavior.

### 13.3 Date/Time Functions

All date/time functions accept time strings in ISO-8601 format and optional
modifiers. The time string formats recognized are:
- `YYYY-MM-DD`
- `YYYY-MM-DD HH:MM`
- `YYYY-MM-DD HH:MM:SS`
- `YYYY-MM-DD HH:MM:SS.SSS`
- `YYYY-MM-DDTHH:MM:SS.SSS` (T separator)
- `HH:MM`, `HH:MM:SS`, `HH:MM:SS.SSS` (date defaults to 2000-01-01)
- `DDDDDDDDDD` (Julian day number as float)
- `now` (current date/time)

**Modifiers** (applied left to right):
- `NNN days`, `NNN hours`, `NNN minutes`, `NNN seconds`, `NNN months`, `NNN years`
- `start of month`, `start of year`, `start of day`
- `weekday N` (advance to next day-of-week, 0=Sunday)
- `unixepoch` (interpret input as Unix timestamp)
- `julianday` (interpret input as Julian day)
- `auto` (auto-detect unix epoch vs Julian day)
- `localtime` (convert to local time)
- `utc` (convert to UTC)
- `subsec` / `subsecond` (include fractional seconds in output)

**date(time-string, modifier, ...)** -> text. Returns `YYYY-MM-DD`.
**time(time-string, modifier, ...)** -> text. Returns `HH:MM:SS`.
**datetime(time-string, modifier, ...)** -> text. Returns `YYYY-MM-DD HH:MM:SS`.
**julianday(time-string, modifier, ...)** -> real. Returns Julian day number.
**unixepoch(time-string, modifier, ...)** -> integer. Returns Unix timestamp.
**strftime(format, time-string, modifier, ...)** -> text. Format specifiers:
`%d` day, `%f` fractional seconds, `%H` hour, `%j` day of year, `%J` Julian
day, `%m` month, `%M` minute, `%s` Unix timestamp, `%S` seconds, `%w` day
of week, `%W` week of year, `%Y` year, `%%` literal %.
**timediff(time1, time2)** -> text (SQLite 3.43+). Returns the difference
as `+YYYY-MM-DD HH:MM:SS.SSS`.

### 13.4 Aggregate Functions

**avg(X)** -> real. Average of non-NULL values. Returns NULL for empty set.
Internally accumulates sum and count separately to avoid precision loss.

**count(*)** -> integer. Counts all rows (including NULLs).
**count(X)** -> integer. Counts non-NULL values of X.

**group_concat(X [, SEP])** -> text. Concatenates non-NULL values with
separator (default `,`). Order is arbitrary unless the SELECT has ORDER BY.

**string_agg(X, SEP)** -> text (SQLite 3.44+). Same as group_concat but
with guaranteed argument order matching SQL standard.

**max(X)** -> any. Returns maximum non-NULL value. For aggregate use
(single argument).

**min(X)** -> any. Returns minimum non-NULL value.

**sum(X)** -> integer or real. Sum of non-NULL values. Returns integer 0
for empty set. Raises an integer overflow error if the sum exceeds i64 range.

**total(X)** -> real. Always returns a float (0.0 for empty set). Never
overflows (uses double precision). Use `total()` instead of `sum()` when
you need a guaranteed non-NULL result.

### 13.5 Window Functions

All aggregate functions can also be used as window functions. In addition,
the following are window-function-only:

**row_number()** -> integer. Sequential number of each row in its partition,
starting from 1. No frame clause needed.

**rank()** -> integer. Rank with gaps. Rows with equal ORDER BY values get
the same rank; the next distinct value gets rank = number of preceding rows + 1.

**dense_rank()** -> integer. Rank without gaps. Next distinct value gets
the previous rank + 1.

**percent_rank()** -> real. `(rank - 1) / (partition_rows - 1)`. Returns
0.0 for partitions with one row.

**cume_dist()** -> real. Cumulative distribution: fraction of rows with
values <= the current row's value.

**ntile(N)** -> integer. Distributes rows into N roughly equal groups,
numbered 1 through N.

**lag(X [, offset [, default]])** -> any. Returns the value of X from the
row `offset` rows before the current row in the partition. Default offset is
1. Default default is NULL.

**lead(X [, offset [, default]])** -> any. Returns the value of X from the
row `offset` rows after the current row.

**first_value(X)** -> any. Returns X from the first row in the window frame.

**last_value(X)** -> any. Returns X from the last row in the window frame.
Note: with the default frame (`RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT
ROW`), this always returns the current row's value. Use `ROWS BETWEEN
UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` for the true last value.

**nth_value(X, N)** -> any. Returns X from the Nth row (1-based) in the
window frame. Returns NULL if the frame has fewer than N rows.

**Frame interaction details:** The `inverse` method on the WindowFunction
trait is called when rows exit the frame (for ROWS and GROUPS modes). This
enables O(1) amortized per-row computation for functions like sum and count
over sliding windows, rather than recomputing from scratch.

### 13.6 COLLATE Interaction

Comparison functions (`min`, `max`, `instr`, `replace`, `LIKE`, `GLOB`)
respect the collation of their operands. When operands have different
collations, the affinity rules determine which collation wins:
1. Explicit `COLLATE` clause on either operand wins
2. Column collation from the schema
3. Default `BINARY` collation

Built-in collations: `BINARY` (memcmp), `NOCASE` (ASCII case-insensitive),
`RTRIM` (ignores trailing spaces).

---

## 14. Extensions

Each extension resides in its own crate under `crates/fsqlite-ext-*` and is
independently feature-gated. Extensions are compiled in (not dynamically
loaded), controlled by Cargo features on the `fsqlite` facade crate.

### 14.1 JSON1 (`fsqlite-ext-json`)

JSON1 provides comprehensive JSON manipulation within SQL. SQLite 3.45+
introduces JSONB, an internal binary format that avoids re-parsing JSON on
every function call.

#### 14.1.1 Scalar Functions

**json(X)** -> text. Validates and minifies JSON text X. Returns NULL if
X is not valid JSON. Converts JSONB to text JSON.

**json_valid(X [, FLAGS])** -> integer. Returns 1 if X is well-formed JSON
(or JSONB if FLAGS=2), 0 otherwise. FLAGS bitmask (SQLite 3.45+):
- 0x01: Accept JSON text
- 0x02: Accept JSONB
- 0x04: Accept JSON5 extensions
- 0x08: Require that X is a JSON object or array (not a primitive)

**json_type(X [, PATH])** -> text. Returns the type of the JSON value at
PATH as one of: `"null"`, `"true"`, `"false"`, `"integer"`, `"real"`,
`"text"`, `"array"`, `"object"`. Returns SQL NULL if PATH does not exist.

**json_extract(X, PATH, ...)** -> any. Extracts value(s) from JSON. Single
path: returns SQL value (text for strings, integer/real for numbers, NULL for
JSON null). Multiple paths: returns a JSON array of the extracted values.
PATH syntax: `$` for root, `.key` for object member, `[N]` for array element
(0-based), `[#-N]` for array element from end.

**X -> PATH** (alias for json_extract with single path, returning JSON text)
**X ->> PATH** (alias for json_extract with single path, returning SQL value)

The `->>` operator is the most commonly used. `json_extract` and `->>` both
unwrap JSON strings to SQL text, JSON numbers to SQL integers/reals, and
JSON null to SQL NULL. The `->` operator preserves JSON typing (returns JSON
text for string values, including the surrounding quotes).

**json_set(X, PATH, VALUE, ...)** -> text. Sets values at paths. Creates
new keys if they do not exist. Overwrites existing values. PATH/VALUE
arguments come in pairs.

**json_insert(X, PATH, VALUE, ...)** -> text. Like json_set but does NOT
overwrite existing values. Only creates new keys/elements.

**json_replace(X, PATH, VALUE, ...)** -> text. Like json_set but does NOT
create new keys. Only overwrites existing values.

**json_remove(X, PATH, ...)** -> text. Removes elements at the specified
paths. Array elements are removed and the array is compacted.

**json_patch(X, Y)** -> text. Implements RFC 7396 JSON Merge Patch.
Recursively merges Y into X. NULL values in Y delete keys in X.

**json_quote(X)** -> text. Converts SQL value X to its JSON representation.
Text becomes a JSON string (with escaping), integer/real become JSON numbers,
NULL becomes JSON `null`, blob becomes JSON text via hex encoding.

**json_array(X, ...)** -> text. Returns a JSON array containing all arguments.

**json_object(KEY, VALUE, ...)** -> text. Returns a JSON object. Arguments
are key/value pairs. Keys must be text.

#### 14.1.2 Aggregate Functions

**json_group_array(X)** -> text. Returns a JSON array containing X from all
rows in the group. NULL values are included as JSON `null`.

**json_group_object(KEY, VALUE)** -> text. Returns a JSON object with
key/value pairs from all rows. Duplicate keys result in the last value winning.

#### 14.1.3 Table-Valued Functions

**json_each(X [, PATH])** -> virtual table. Iterates over the top-level
elements of the JSON array or object at PATH. Columns:
- `key`: array index (integer) or object key (text)
- `value`: the element value (SQL type)
- `type`: JSON type name
- `atom`: the element value (always as SQL type, NULL for arrays/objects)
- `id`: unique integer ID for this element within the JSON
- `parent`: ID of the parent element
- `fullkey`: full path to this element (e.g., `$.store.book[0].title`)
- `path`: path to the parent (e.g., `$.store.book[0]`)

**json_tree(X [, PATH])** -> virtual table. Like json_each but recursively
descends into nested arrays and objects. Same column schema as json_each.

#### 14.1.4 JSONB Binary Format

JSONB is a binary encoding of JSON stored as a BLOB. Structure:
- Each node is a header byte (4-bit type + 4-bit size-of-payload-size),
  followed by the payload size (0, 1, 2, 4, or 8 bytes), followed by payload.
- Node types: null(0), true(1), false(2), int(3), int5(4), float(5),
  text(6), textj(7), text5(8), textraw(9), array(10), object(11).
- Arrays and objects store their children as concatenated child nodes.
- JSONB is approximately 10-20% larger than text JSON but avoids parsing
  overhead on every function call.

Functions that produce JSON output also accept and produce JSONB when the
input is JSONB, preserving the binary format through chains of function
calls. Use `json(X)` to convert JSONB to text, or `jsonb(X)` to convert
text to JSONB.

### 14.2 FTS5 (`fsqlite-ext-fts5`)

FTS5 (Full-Text Search version 5) provides efficient full-text search over
large text corpora using an inverted index architecture.

#### 14.2.1 Table Creation

```sql
CREATE VIRTUAL TABLE docs USING fts5(
  title,
  body,
  content=external_table,     -- external content table
  content_rowid=id,           -- rowid column in external content table
  tokenize='porter unicode61', -- tokenizer pipeline
  prefix='2,3',               -- prefix indexes for 2 and 3 character prefixes
  detail=full                 -- posting list detail level
);
```

**detail levels:**
- `full` (default): Stores column number and token position. Supports all queries.
- `column`: Stores only column number. Position-dependent queries (NEAR, phrase)
  not supported.
- `none`: Stores only docid. Neither column filters nor position queries supported.

#### 14.2.2 Tokenizer API

FTS5 tokenizers implement a trait that receives text and emits tokens:

```rust
pub trait Fts5Tokenizer: Send + Sync {
    fn tokenize(
        &self,
        text: &str,
        flags: TokenizeFlags,
        callback: &mut dyn FnMut(token: &str, start: usize, end: usize) -> Result<()>,
    ) -> Result<()>;
}
```

Built-in tokenizers:
- `unicode61`: Unicode-aware tokenization with diacritics removal. Configurable
  separators and token characters.
- `ascii`: ASCII-only tokenization. Faster but handles only ASCII text.
- `porter`: Porter stemming wrapper. Applied after another tokenizer:
  `tokenize='porter unicode61'`.
- `trigram`: Splits text into 3-character sequences. Enables substring search
  (`LIKE '%pattern%'`) via FTS.

Custom tokenizer registration:
```rust
db.create_fts5_tokenizer("my_tokenizer", MyTokenizer::new())?;
```

#### 14.2.3 Inverted Index Structure

FTS5 stores its index in a shadow table `{table}_data` as a segment-based
structure (similar to an LSM tree):

**Segments:** Each segment is a sorted run of term/doclist pairs. New
documents are initially written to a small in-memory segment, then flushed.
Background merge operations combine small segments into larger ones (tiered
compaction).

**Term format:** Terms are stored as prefix-compressed byte strings. Each
leaf page contains a sorted sequence of terms with their associated doclists.

**Doclist format:** For each term, the doclist is a sequence of:
- Varint-encoded docid deltas (difference from previous docid)
- For each docid, a position list: column number + offset pairs
- Position lists are varint-encoded with column number deltas and offset deltas

**Segment merge:** Merging reads from multiple input segments, deduplicates
docids, and writes a new output segment. The merge process is incremental
and can be performed during queries (auto-merge) or explicitly via
`INSERT INTO fts_table(fts_table) VALUES('merge=N')` where N is the number
of pages to merge.

#### 14.2.4 Query Syntax

FTS5 queries are passed as the right-hand side of the MATCH operator:

```sql
SELECT * FROM docs WHERE docs MATCH 'search terms';
SELECT * FROM docs('search terms');  -- shorthand
```

Query language:
- **Implicit AND:** `word1 word2` matches documents containing both words
- **OR:** `word1 OR word2`
- **NOT:** `NOT word1` or `word1 NOT word2`
- **Phrase:** `"exact phrase"` matches consecutive tokens
- **Prefix:** `pref*` matches any token starting with "pref"
- **NEAR:** `NEAR(word1 word2, 10)` matches when word1 and word2 appear
  within 10 tokens of each other
- **Column filter:** `title : search` restricts search to the title column
- **Caret initial token:** `^word` matches word only at the start of a column
- **Grouping:** Parentheses for complex boolean expressions

#### 14.2.5 Ranking and Auxiliary Functions

**Built-in ranking:** BM25 (Okapi BM25). Automatically available as a
ranking function:
```sql
SELECT *, rank FROM docs WHERE docs MATCH 'query' ORDER BY rank;
-- rank is automatically BM25 score (lower = better match)
```

**Custom ranking functions** are registered via:
```rust
db.create_fts5_function("my_rank", my_ranking_function)?;
```

**Built-in auxiliary functions:**
- `highlight(fts_table, col_idx, open_tag, close_tag)` -- returns text with
  matching tokens wrapped in open/close tags
- `snippet(fts_table, col_idx, open_tag, close_tag, ellipsis, max_tokens)` --
  returns a short snippet around matching tokens
- `bm25(fts_table, w1, w2, ...)` -- BM25 score with per-column weights

#### 14.2.6 Content Tables

**Internal content:** FTS5 stores its own copy of the content (default).

**External content:** `content=table_name` references an external table.
FTS5 does not store document text. The external table must be kept in sync
manually (using triggers or explicit management).

**Contentless:** `content=''` stores no content at all. Only the inverted
index is maintained. `highlight()` and `snippet()` are not available.
Useful for pure search-and-retrieve-rowid workloads.

### 14.3 FTS3/FTS4 (`fsqlite-ext-fts3`)

FTS3 and FTS4 are the predecessors to FTS5. They share an implementation
crate because FTS4 is a backward-compatible extension of FTS3.

**Key differences from FTS5:**
- FTS3/4 uses a different segment structure (B-tree based, not LSM-like)
- Query syntax differs: AND is explicit, not implicit
- FTS4 adds `matchinfo()`, `offsets()`, `content=` tables, `compress=`/`uncompress=`
- FTS3/4 uses `SELECT ... WHERE column MATCH 'query'` (column-level match)
  vs FTS5's table-level match

**matchinfo(X, FORMAT)** returns a blob of 32-bit unsigned integers encoding
match statistics. FORMAT string controls what is included:
- `p`: Number of matchable phrases
- `c`: Number of user-defined columns
- `n`: Number of rows in the FTS table
- `a`: Average number of tokens per column per row
- `l`: Length of the current row in tokens per column
- `s`: Longest common subsequence of phrase tokens
- `x`: 3 values per phrase/column pair: hits in this row, hits in all rows,
  number of rows containing hits

**offsets(X)** returns a text string listing the byte offsets of all matches:
`"col_num term_num byte_offset byte_length col_num term_num ..."`.

**compress/uncompress (FTS4 only):** Custom compression functions for stored
content: `CREATE VIRTUAL TABLE t USING fts4(content, compress=zlib_compress, uncompress=zlib_uncompress)`.

### 14.4 R-Tree (`fsqlite-ext-rtree`)

R-Tree provides efficient spatial indexing for multi-dimensional data.

```sql
CREATE VIRTUAL TABLE demo_index USING rtree(
  id,              -- integer primary key
  minX, maxX,      -- first dimension bounds
  minY, maxY       -- second dimension bounds
  -- up to 5 dimensions (10 coordinate columns)
);
```

**Dimension limits:** 1 to 5 dimensions (2 to 10 coordinate columns).
Coordinates are stored as 32-bit floats by default. Use `rtree_i32` for
32-bit integers instead.

**Query types:**
```sql
-- Range query: find all entries overlapping a bounding box
SELECT * FROM demo_index WHERE minX <= 100 AND maxX >= 50
                           AND minY <= 200 AND maxY >= 100;

-- Custom geometry callback
SELECT * FROM demo_index WHERE id MATCH my_geometry(50, 100, 30);
```

**Custom geometry callbacks** implement the `Fts5Geometry` trait:
```rust
pub trait RtreeGeometry: Send + Sync {
    fn query_func(&self, bbox: &[f64]) -> Result<RtreeQueryResult>;
    // Returns: Include, Exclude, or PartiallyContained
}
```

The R-tree query engine calls the geometry callback for each node in the
tree during descent, pruning branches where the callback returns `Exclude`.

**Geopoly extension:** Built on top of R-tree, provides polygon operations:
- `geopoly_overlap(P1, P2)` -- test if two polygons overlap
- `geopoly_within(P1, P2)` -- test if P1 is within P2
- `geopoly_area(P)` -- compute polygon area
- `geopoly_blob(P)` -- convert GeoJSON to internal binary format
- `geopoly_json(P)` -- convert internal format to GeoJSON
- `geopoly_svg(P)` -- render polygon as SVG path
- `geopoly_bbox(P)` -- bounding box of polygon
- `geopoly_contains_point(P, X, Y)` -- point-in-polygon test
- `geopoly_group_bbox(P)` -- aggregate bounding box
- `geopoly_regular(X, Y, R, N)` -- regular N-gon at center (X,Y) radius R
- `geopoly_ccw(P)` -- ensure counter-clockwise winding

Polygons are stored as binary blobs in the format: 4-byte header (type +
vertex count) followed by pairs of 32-bit float coordinates.

### 14.5 Session (`fsqlite-ext-session`)

The Session extension records changes to a database and represents them
as changesets or patchsets that can be applied to other databases.

#### 14.5.1 Changeset Format

A changeset is a binary blob with the following layout:
```
For each modified table:
  'T' byte (0x54)
  Number of columns (varint)
  For each column: 0x00 (not part of PK) or 0x01 (part of PK)
  Table name (nul-terminated string)

  For each changed row:
    Operation byte: SQLITE_INSERT (18), SQLITE_DELETE (9), SQLITE_UPDATE (23)

    For DELETE:
      Old values: one value per column (serial-type encoded)

    For INSERT:
      New values: one value per column (serial-type encoded)

    For UPDATE:
      Old values: one per column (undefined for non-PK columns that didn't change)
      New values: one per column (undefined for columns that didn't change)
```

Each value is encoded as: a single type byte (0x00=undefined, 0x01=integer,
0x02=real, 0x03=text, 0x04=blob, 0x05=null) followed by the value data
(varint-length-prefixed for text and blob, 8-byte big-endian for integer
and real).

#### 14.5.2 Conflict Resolution

When applying a changeset, conflicts are resolved via a callback:
```rust
pub enum ConflictAction {
    OmitChange,     // skip this change
    Replace,        // overwrite conflicting row
    Abort,          // abort the entire apply operation
}

pub enum ConflictType {
    Data,           // row exists but values differ from expected
    NotFound,       // row to update/delete does not exist
    Conflict,       // unique constraint violation
    Constraint,     // other constraint violation
    ForeignKey,     // foreign key constraint
}
```

#### 14.5.3 Patchset Differences

A patchset is a more compact format that omits the old values for UPDATE
operations (only stores new values and PK). Patchsets cannot detect
conflicts as precisely as changesets (cannot verify that the old row matched)
but are significantly smaller for tables with many columns.

### 14.6 ICU (`fsqlite-ext-icu`)

The ICU extension provides Unicode-aware string operations.

**Collation creation:**
```sql
SELECT icu_load_collation('de_DE', 'german');
-- Now: SELECT * FROM t ORDER BY name COLLATE german;
```

This creates a collation from an ICU locale identifier. The collation
uses ICU's `ucol_strcoll` for comparison, providing linguistically
correct sort order for the specified language.

**Case folding:** `icu_upper(X, LOCALE)` and `icu_lower(X, LOCALE)` provide
locale-aware case conversion (unlike the built-in `upper`/`lower` which
handle ASCII only).

**FTS tokenizer integration:** The ICU tokenizer `icu` can be used with
FTS3/4/5 for language-aware word breaking:
```sql
CREATE VIRTUAL TABLE docs USING fts5(body, tokenize='icu zh_CN');
```

This uses ICU's `UBreakIterator` with word-break rules appropriate for
the specified locale, which is critical for CJK languages where words
are not delimited by spaces.

### 14.7 Miscellaneous (`fsqlite-ext-misc`)

**generate_series(START, STOP [, STEP])** -> virtual table. Generates a
sequence of integers from START to STOP with optional STEP (default 1).
Columns: `value`, `start`, `stop`, `step`. Commonly used in joins:
```sql
SELECT value FROM generate_series(1, 100);
SELECT date(d.value) FROM generate_series(
  unixepoch('2024-01-01'), unixepoch('2024-12-31'), 86400
) AS d;
```

**dbstat** -> virtual table. Reports B-tree page usage statistics:
```sql
SELECT name, path, pageno, pagetype, ncell, payload, unused, mx_payload
  FROM dbstat WHERE aggregate=FALSE;
```
Columns provide per-page details: page number, type (leaf/internal), number
of cells, total payload bytes, unused bytes, maximum cell payload. The
`aggregate` hidden column controls whether to show per-page or per-table
aggregated statistics.

**dbpage** -> virtual table. Provides direct read/write access to database
pages:
```sql
SELECT data FROM dbpage WHERE pgno = 1;  -- read page 1
UPDATE dbpage SET data = X'...' WHERE pgno = 5;  -- write page 5 (dangerous!)
```

**csv** -> virtual table. Reads CSV files as virtual tables:
```sql
CREATE VIRTUAL TABLE temp.csv_data USING csv(
  filename='data.csv',
  header=YES,
  columns=4
);
```

**decimal** -> extension for arbitrary-precision decimal arithmetic:
- `decimal(X)` -- convert to decimal text representation
- `decimal_add(X, Y)`, `decimal_sub(X, Y)`, `decimal_mul(X, Y)` --
  arbitrary precision arithmetic
- `decimal_sum(X)` -- aggregate sum with arbitrary precision
- `decimal_cmp(X, Y)` -- comparison returning -1, 0, or +1

Decimal values are represented internally as strings to avoid floating-point
precision loss. This is useful for financial calculations.

**uuid** -> UUID generation functions:
- `uuid()` -- generate random UUID v4
- `uuid_str(X)` -- convert UUID blob to string representation
- `uuid_blob(X)` -- convert UUID string to 16-byte blob

---

## 15. Exclusions (What We Are NOT Building)

FrankenSQLite deliberately excludes the following components. Each exclusion
has a technical rationale; none are omitted from laziness.

**Amalgamation build system.** The C SQLite amalgamation (`sqlite3.c`) is a
single-file build artifact produced by concatenating ~150 source files. Its
purpose is simplifying C compilation. Rust's Cargo workspace with 23 crates
provides superior modularity, parallel compilation, and dependency tracking.
There is no analog of the amalgamation in a Rust project.

**TCL test harness.** C SQLite's test suite is driven by TCL scripts
(~90,000+ lines). These scripts are deeply intertwined with the C API
(`sqlite3_exec`, `sqlite3_step`, etc.) and cannot be meaningfully ported.
Instead, FrankenSQLite uses: (1) native Rust `#[test]` modules, (2) proptest
for property-based testing, (3) the conformance harness that compares SQL
output against C sqlite3 golden files, and (4) asupersync's lab reactor
for deterministic concurrency tests. This strategy provides equivalent or
superior coverage without the TCL dependency.

**LEMON parser generator.** C SQLite uses a custom LALR(1) parser generator
called LEMON to produce `parse.c` from `parse.y`. FrankenSQLite uses a
hand-written recursive descent parser with Pratt precedence for expressions.
Rationale: better error messages with precise source span reporting,
simpler maintenance, no build-time code generation step, and the `parse.y`
grammar serves as an authoritative reference even without LEMON.

**Loadable extension API (.so/.dll).** C SQLite supports dynamically loading
extensions via `sqlite3_load_extension()`. This requires a C-compatible ABI
and `dlopen`/`LoadLibrary` calls. FrankenSQLite instead compiles all
extensions directly into the binary, controlled by Cargo features. This
eliminates an entire class of security vulnerabilities (arbitrary code
loading) and simplifies deployment. Users who need custom extensions implement
Rust traits and recompile.

**Legacy file format quirks (schema format < 4).** Schema format number 4
has been the default since SQLite 3.3.0 (2006). Formats 1-3 have minor
differences in how DESC indexes and boolean handling work. Supporting these
would add complexity for a format that no actively maintained database uses.
FrankenSQLite requires schema format 4 and rejects databases with older formats
with a clear error message.

**Obsolete VFS implementations.** C SQLite ships VFS backends for OS/2,
VxWorks, Windows CE, and other legacy platforms. FrankenSQLite provides
`UnixVfs` (POSIX), `WindowsVfs` (Win32), and `MemoryVfs` (in-memory).
Other platforms can be supported via the `Vfs` trait.

**Shared-cache mode.** C SQLite's shared-cache mode allows multiple
connections within the same process to share a single page cache and use
table-level locking. It has been deprecated since SQLite 3.41.0 (2023) and
is widely considered a source of subtle bugs. FrankenSQLite's MVCC system
supersedes shared-cache entirely: multiple connections within a process
share the MVCC version chains and benefit from page-level concurrency, which
is strictly superior.

**Windows VFS.** Windows file locking uses `LockFileEx`/`UnlockFileEx`
instead of `fcntl`, and the shared memory coordination uses
`CreateFileMapping` instead of `mmap`. `WindowsVfs` is in-scope and
implements the same `Vfs` trait as `UnixVfs`. Platform-specific code is
isolated behind `#[cfg(target_os)]` gates.

**Multiplexor VFS.** C SQLite's multiplexor shards large databases across
multiple files to work around filesystem limitations (e.g., FAT32 4GB limit).
Modern filesystems do not have these limitations. Excluded.

**SEE (SQLite Encryption Extension).** C SQLite's commercial encryption
extension is not ported. Instead, FrankenSQLite provides page-level
encryption using the reserved-space-per-page field in the database header:
- `PRAGMA key = 'passphrase'` derives a 256-bit key via Argon2id.
- Each page is encrypted with AES-256-GCM. The 12-byte nonce is derived from
  `(page_number, write_counter)` to ensure uniqueness without storage overhead.
- The 16-byte GCM authentication tag is stored in the page's reserved space.
- `PRAGMA rekey = 'new_passphrase'` re-encrypts all pages.
- Key management uses the standard SQLite Encryption Extension API for
  compatibility with existing tooling.
- Encryption is orthogonal to ECS: encrypted pages are encoded as ECS symbols
  with encryption applied before RaptorQ encoding (encrypt-then-code).

---

## 16. Implementation Phases

### Phase 1: Bootstrap and Spec Extraction [COMPLETE]

**Deliverables:**
- `Cargo.toml` workspace root with 23 crate entries
- `crates/fsqlite-types/src/lib.rs`: `PageNumber` (NonZeroU32), `SqliteValue`
  enum (Null, Integer(i64), Real(f64), Text(String), Blob(Vec<u8>)),
  `Opcode` enum (190+ variants), limits module (`SQLITE_MAX_LENGTH`,
  `SQLITE_MAX_SQL_LENGTH`, etc.), serial type encoding/decoding, bitflags
- `crates/fsqlite-error/src/lib.rs`: `FrankenError` enum (~40 variants
  mapping to SQLite error codes), `ErrorCode` constants, `Display`/`Error`
  impls, conversion from `std::io::Error`
- Spec documents: `AGENTS.md`, `PROPOSED_ARCHITECTURE.md`,
  `PLAN_TO_PORT_SQLITE_TO_RUST.md`, `EXISTING_SQLITE_STRUCTURE.md`

**Acceptance criteria:**
- `cargo check --workspace` passes with zero errors
- `cargo clippy --workspace --all-targets -- -D warnings` passes
- 77 tests all green covering: SqliteValue type conversions, PageNumber
  construction (reject zero), all Opcode display names, limit constant
  values matching C SQLite, serial type round-trip for all type categories
- Every error variant has a distinct ErrorCode and meaningful Display output
- Conformance harness infrastructure: Oracle runner can execute SQL against
  C SQLite and capture results in JSON fixture format (Section 17.7)
- At least 10 basic conformance fixtures captured from Oracle

**Dependencies:** None (first phase).

**Risk areas:** Getting the Opcode enum right -- there are 190+ opcodes and
their numeric values must match C SQLite for EXPLAIN output compatibility.
Mitigation: extract opcode list mechanically from `opcodes.h`.

**Estimated complexity:** ~3,000 LOC across fsqlite-types, fsqlite-error,
and fsqlite-harness bootstrap.

### Phase 2: Core Types and Storage Foundation [IN PROGRESS]

**Deliverables:**
- `crates/fsqlite-vfs/src/lib.rs`: `Vfs` and `VfsFile` traits
- `crates/fsqlite-vfs/src/memory.rs`: `MemoryVfs` implementation (in-memory
  file system with `HashMap<String, Arc<Mutex<Vec<u8>>>>`)
- `crates/fsqlite-types/src/record.rs`: Record format serialization and
  deserialization (varint header, serial types, data payload)
- `crates/fsqlite-vfs/src/unix.rs`: `UnixVfs` with POSIX file operations and
  `fcntl`-based locking (5-level: NONE, SHARED, RESERVED, PENDING, EXCLUSIVE)

**Acceptance criteria:**
- MemoryVfs: create file, write, read-back, truncate, file_size all correct
- MemoryVfs: concurrent read/write from multiple threads (using Arc clone)
- Record format: encode/decode round-trip for NULL, integers (all 6 sizes),
  float, text, blob, constant 0, constant 1
- Record format: proptest with arbitrary SqliteValue vectors up to 100 columns
- Record format: edge case -- empty record (zero columns), single NULL column,
  maximum-size text (1GB), varint boundary values (127, 128, 16383, 16384)
- UnixVfs: create/open/read/write/delete on real filesystem via tempfile
- UnixVfs: lock escalation NONE -> SHARED -> RESERVED -> EXCLUSIVE
- UnixVfs: two processes cannot both hold EXCLUSIVE (test via fork or separate
  process spawn)
- Target: 200+ tests

**Dependencies:** Phase 1 complete.

**Risk areas:** Unix file locking semantics are notoriously tricky. POSIX
`fcntl` locks are per-process (not per-file-descriptor), meaning two fds
to the same file in the same process share locks. SQLite works around this
with a global lock table (`unixInodeInfo`). We need an equivalent.

**Estimated complexity:** ~4,000 LOC across fsqlite-vfs and fsqlite-types.

### Phase 3: B-Tree and SQL Parser

**Deliverables:**
- `crates/fsqlite-btree/src/cursor.rs`: `BtCursor` with page-stack
  traversal (max depth 20 for 4KB pages, 2^20 * ~100 entries/page = 100
  billion rows capacity)
- `crates/fsqlite-btree/src/cell.rs`: Cell parsing for all 4 page types
  (INTKEY table leaf/interior, BLOBKEY index leaf/interior), overflow
  detection, local payload calculation
- `crates/fsqlite-btree/src/balance.rs`: Page splitting algorithms --
  `balance_nonroot` (redistribute cells among siblings, typically 3-way
  split), `balance_deeper` (root overflow, increase tree depth by 1)
- `crates/fsqlite-btree/src/overflow.rs`: Overflow page chain read/write,
  following chain links, allocating overflow pages from freelist
- `crates/fsqlite-btree/src/freelist.rs`: Trunk + leaf freelist page
  management, allocation (prefer leaf pages from first trunk), deallocation
- `crates/fsqlite-btree/src/payload.rs`: `BtreePayload` abstraction for
  reading across page boundaries (local + overflow)
- `crates/fsqlite-ast/src/lib.rs`: Complete AST type hierarchy --
  `Statement`, `SelectStatement`, `InsertStatement`, `UpdateStatement`,
  `DeleteStatement`, `CreateTableStatement`, `Expr`, `JoinClause`,
  `OrderingTerm`, `WindowDefn`, etc.
- `crates/fsqlite-parser/src/lexer.rs`: Token enum, memchr-accelerated
  scanning for string literals and comments, keyword classification
- `crates/fsqlite-parser/src/parser.rs`: Recursive descent with Pratt
  precedence for expressions, all statement types from Section 12
- `crates/fsqlite-parser/src/keyword.rs`: Perfect hash (or PHF crate) for
  150+ SQL keywords with O(1) lookup

**Acceptance criteria:**
- B-tree: Insert 10,000 random i64 keys, verify all retrievable via cursor
- B-tree: Insert 10,000 sequential keys, delete 5,000 random subset, verify
  remaining 5,000 present and in order
- B-tree: Insert keys that force overflow pages (payload > page_size/4),
  verify read-back
- B-tree: Insert/delete pattern that causes tree depth to increase to 3 and
  then decrease back to 2
- B-tree: Freelist correctly tracks freed pages and reuses them on insert
- B-tree: Proptest -- random mix of insert/delete/lookup operations,
  invariant: cursor iteration always returns keys in sorted order
- Parser: Parse all statement types from Section 12 (at least one test per
  subsection)
- Parser: Expression precedence: `1 + 2 * 3` parses as `1 + (2 * 3)`
- Parser: All join types, CTE syntax, window function syntax
- Parser: Round-trip property test: parse -> pretty-print -> re-parse
  produces identical AST for 1000 generated SQL statements
- Parser: Error recovery: invalid SQL produces error with line:column span
- Parser: Keywords as identifiers in non-reserved positions (e.g., column
  named `order` in `SELECT "order" FROM t`)
- Target: 500+ tests

**Dependencies:** Phase 2 complete (B-tree depends on VFS for page I/O,
parser depends on types for AST nodes).

**Risk areas:**
- B-tree balance is the most algorithmically complex code in SQLite.
  `balance_nonroot` alone is ~1,200 lines of C. Incorrect balancing causes
  silent data corruption. Mitigation: extensive proptest with invariant
  checking after every operation (cell count, key ordering, child pointers,
  freespace accounting).
- Parser completeness: SQLite's grammar has many context-sensitive corners
  (e.g., `REPLACE` is both a keyword and a function name). Mitigation: use
  `parse.y` as the authoritative reference, test every production.

**Estimated complexity:** ~12,000 LOC (btree: 5,000, parser: 4,000, ast: 3,000).

### Phase 4: VDBE and Query Pipeline

**Deliverables:**
- `crates/fsqlite-vdbe/src/engine.rs`: Fetch-execute loop, match-based
  opcode dispatch, register file (Vec<Mem>)
- `crates/fsqlite-vdbe/src/mem.rs`: `Mem` type (SQLite's runtime value with
  type, affinity, encoding), comparison with collation, arithmetic
- `crates/fsqlite-vdbe/src/opcodes/`: Implementation modules for the 50+
  critical opcodes: Init, Goto, Halt, Integer, String8, Null, Blob,
  ResultRow, MakeRecord, Column, Rowid, OpenRead, OpenWrite, Rewind, Next,
  Prev, SeekGE, SeekGT, SeekLE, SeekLT, Found, NotFound, Insert, Delete,
  NewRowid, IdxInsert, IdxDelete, Transaction, AutoCommit, CreateBtree,
  Destroy, Clear, Noop, Explain, TableLock, ReadCookie, SetCookie, etc.
- `crates/fsqlite-vdbe/src/sorter.rs`: External merge sort for ORDER BY
- `crates/fsqlite-planner/src/resolve.rs`: Name resolution (table/column
  binding, `*` expansion, alias resolution)
- `crates/fsqlite-planner/src/codegen.rs`: AST-to-VDBE code generation for
  SELECT, INSERT, UPDATE, DELETE, CREATE TABLE
- `crates/fsqlite-core/src/connection.rs`: Connection state, schema cache,
  prepared statement management
- `crates/fsqlite/src/lib.rs`: Public API: `Connection::open()`,
  `connection.prepare()`, `stmt.execute()`, `stmt.query()`, `Row`, etc.

**Acceptance criteria:**
- End-to-end: `CREATE TABLE t(a INTEGER, b TEXT); INSERT INTO t VALUES(1,'hello'); SELECT * FROM t;` returns `[(1, "hello")]`
- End-to-end: `SELECT 1+2, 'abc'||'def', typeof(3.14)` returns `[(3, "abcdef", "real")]`
- End-to-end: INSERT with multiple rows, SELECT with WHERE, ORDER BY, LIMIT
- End-to-end: UPDATE with SET and WHERE, verify changed rows
- End-to-end: DELETE with WHERE, verify deleted rows gone
- End-to-end: EXPLAIN produces correct opcode listing
- VDBE: All comparison operators with type affinity coercion
- VDBE: NULL handling (NULL = NULL is NULL, NULL IS NULL is true)
- VDBE: CASE expression evaluation
- VDBE: Subquery (EXISTS, IN, scalar subquery)
- Sorter: ORDER BY correctly sorts 100,000 rows in-memory, correctly
  spills to disk for 1,000,000 rows
- Target: 1,000+ tests

**Dependencies:** Phase 3 complete (VDBE needs btree for storage, codegen
needs parser for AST).

**Risk areas:** Codegen is the glue layer where parser output meets VDBE
input. Getting register allocation right is subtle (SQLite uses a complex
register assignment algorithm to minimize register pressure). Mitigation:
start with naive one-register-per-expression, optimize later.

**Estimated complexity:** ~18,000 LOC (vdbe: 8,000, planner: 4,000,
core: 3,000, public api: 1,000, func: 2,000).

### Phase 5: Persistence, WAL, and Transactions

**Deliverables:**
- `crates/fsqlite-pager/src/pager.rs`: Pager state machine (OPEN, READER,
  WRITER, SYNCED, ERROR), journal/WAL mode switching
- `crates/fsqlite-pager/src/journal.rs`: Rollback journal (hot journal
  detection, playback on recovery)
- `crates/fsqlite-wal/src/wal.rs`: WAL file creation, frame append, frame
  read, checksum computation (SQLite's custom algorithm)
- `crates/fsqlite-wal/src/index.rs`: WAL index (shared memory hash table
  for page-to-frame lookup)
- `crates/fsqlite-wal/src/checkpoint.rs`: PASSIVE, FULL, RESTART, TRUNCATE
  checkpoint modes
- `crates/fsqlite-wal/src/recovery.rs`: WAL recovery on open (detect valid
  frames by checksum chain, discard torn tail)
- `crates/fsqlite-wal/src/raptorq.rs`: Self-healing WAL with RaptorQ repair
  symbols (Section 3.4.1)
- Transaction support: BEGIN/COMMIT/ROLLBACK, savepoint stack

**Acceptance criteria:**
- Persistence: Create table, insert data, close connection, reopen, data
  present
- Journal mode: Write data, simulate crash (truncate mid-write), reopen,
  hot journal detection and playback, data consistent
- WAL mode: Multiple readers concurrent with one writer, readers see
  consistent snapshots
- WAL checksum: Corrupt one byte of a frame, verify checksum detects it
- WAL recovery: Append 100 frames, truncate last frame (simulate torn write),
  recovery discards torn frame, prior 99 frames intact
- RaptorQ WAL: Append 10 frames + 2 repair symbols, corrupt 2 source frames,
  verify recovery reconstructs all 10 frames
- Checkpoint: Verify all 4 modes move frames back to database file correctly
- Savepoints: SAVEPOINT, RELEASE, ROLLBACK TO with nested savepoints
- Round-trip: Create database with FrankenSQLite, read with C sqlite3 (and
  vice versa), verify data integrity
- Target: 1,500+ tests

**Dependencies:** Phase 4 complete (persistence needs pager under VDBE).

**Risk areas:** WAL checksum compatibility is critical for file format
interop. The checksum algorithm is non-standard and byte-order-dependent.
Mitigation: generate test WAL files with C SQLite and verify FrankenSQLite
reads them correctly.

**Estimated complexity:** ~10,000 LOC (pager: 3,000, wal: 5,000,
raptorq integration: 2,000).

### Phase 6: MVCC Concurrent Writers with SSI

**Deliverables:**
- `crates/fsqlite-mvcc/src/txn.rs`: Transaction type with TxnId, Snapshot,
  write_set, read_set, intent_log, page_locks, mode (Serialized/Concurrent),
  SSI state (has_in_rw, has_out_rw, rw_in_from, rw_out_to)
- `crates/fsqlite-mvcc/src/snapshot.rs`: Snapshot capture, Roaring Bitmap
  for in_flight set (replacing Bloom filter), visibility predicate
- `crates/fsqlite-mvcc/src/version_chain.rs`: Page version chains, GF(256)
  delta encoding via RaptorQ (Section 3.4.4)
- `crates/fsqlite-mvcc/src/lock_table.rs`: Sharded PageLockTable (64 shards)
  with try_acquire (non-blocking) and release_all
- `crates/fsqlite-mvcc/src/siread.rs`: SireadTable (sharded) for SSI
  rw-antidependency tracking at page granularity
- `crates/fsqlite-mvcc/src/ssi.rs`: SSI validation (conservative
  has_in_rw && has_out_rw => abort rule), witness recording for debug/lab
- `crates/fsqlite-mvcc/src/conflict.rs`: First-committer-wins validation,
  deterministic rebase merge via intent logs (Section 5.10), algebraic
  write merging (Section 3.4.5), byte-level conflict detection
- `crates/fsqlite-mvcc/src/gc.rs`: Garbage collection -- horizon computation
  (min active TxnId), version chain trimming, SIREAD table cleanup,
  memory bound enforcement
- `crates/fsqlite-mvcc/src/coordinator.rs`: Write coordinator using
  asupersync two-phase MPSC channel, commit serialization for WAL append
- `crates/fsqlite-mvcc/src/arc_cache.rs`: ARC cache with (PageNumber, TxnId)
  keys, eviction constraints (pinned, dirty, superseded)
- `crates/fsqlite-pager/src/mvcc_pager.rs`: MvccPager trait implementation
  bridging B-tree layer to MVCC layer, Cx threading

**Acceptance criteria:**
- Serialized mode: Exact C SQLite behavior -- single writer, SERIALIZABLE
  isolation, `BEGIN IMMEDIATE` blocks other writers
- Concurrent mode: Two transactions writing to different pages both commit
  successfully
- Concurrent mode: Two transactions writing to the same page, second
  committer gets `SQLITE_BUSY_SNAPSHOT`
- Concurrent mode: 100 threads each insert 100 rows into separate rowid
  ranges, all 10,000 rows present after all commits
- Snapshot isolation: Long-running reader (started before writer) does not
  see writer's changes even after writer commits
- Snapshot isolation: Reader started after writer commits sees all changes
- Algebraic merge: Two transactions modify non-overlapping byte ranges of
  the same page, both commit successfully via GF(256) merge
- Algebraic merge: Two transactions modify overlapping byte ranges, merge
  rejected, second committer aborted
- GC: Sustained write load of 1,000 transactions, memory usage bounded by
  O(active_transactions * pages_per_transaction), not O(total_transactions)
- GC: Version chain length never exceeds active transaction count + 1
- Version chain compression: Pages with small diffs (< 10% changed) use
  RaptorQ delta encoding, space savings > 80%
- SSI: Write skew pattern (two txns read overlapping data, write disjoint
  pages based on reads) -- at least one txn aborted under default mode
- SSI: PRAGMA fsqlite.serializable=OFF allows both to commit (SI mode)
- SSI: has_in_rw/has_out_rw flags correctly set for known rw-antidependency
  patterns
- Rebase merge: Two transactions insert distinct keys into the same leaf
  page -- rebase succeeds, both commit
- Rebase merge: Two transactions update the same key -- rebase fails,
  second committer aborts
- Roaring Bitmap: Visibility checks with 100 in-flight transactions have
  zero false positives (exact, not probabilistic)
- ARC cache: Sequential scan does not evict frequently-accessed index pages
  (ARC adaptation test)
- Lab reactor: All above tests run under deterministic scheduling with
  same results across 100 different seeds
- Mazurkiewicz traces: 3-transaction scenario (T1 writes page A, T2 writes
  page B, T3 writes both A and B) -- all 6 possible commit orderings
  verified for correct conflict detection
- E-process monitors: INV-1 through INV-7 monitored continuously during
  100-thread stress test, zero violations
- Target: 2,000+ tests

**Dependencies:** Phase 5 complete (MVCC sits atop WAL and pager).

**Risk areas:** This is the hardest phase. Specific risks:
- Snapshot capture must be atomic with respect to concurrent commits.
  A non-atomic snapshot can miss a commit, violating SI. Mitigation: hold
  a read lock on active_transactions during snapshot capture.
- GC must not reclaim versions needed by any active transaction. Mitigation:
  formal proof in Section 5.5, e-process monitoring at runtime.
- Algebraic merge correctness depends on disjoint byte-range detection.
  Off-by-one errors cause silent corruption. Mitigation: proptest with
  random page mutations and merge verification.
- ARC cache interaction with MVCC versioning adds complexity to eviction
  decisions. Mitigation: start with simple LRU, upgrade to ARC once basic
  MVCC works.

**Estimated complexity:** ~15,000 LOC.

### Phase 7: Advanced Query Planner, Full VDBE, SQL Features

**Deliverables:**
- Full WHERE optimization: index scan selection, range narrowing, OR
  optimization via temp index, LIKE prefix optimization, skip-scan
  for composite indexes with leading column not constrained
- Join ordering: cost-based with cardinality estimation from sqlite_stat1,
  greedy algorithm for > 8 tables, exhaustive search for <= 8 tables
- All 190+ VDBE opcodes implemented
- Window function execution: frame management, ROWS/RANGE/GROUPS modes,
  EXCLUDE clause, partition-by sorting
- CTE execution: materialized and non-materialized, recursive with cycle
  detection via LIMIT
- Trigger compilation and execution: BEFORE/AFTER/INSTEAD OF, OLD/NEW
  access, recursive triggers
- Foreign key enforcement: deferred and immediate checking, CASCADE actions
- View expansion and INSTEAD OF trigger routing
- ALTER TABLE: RENAME, ADD COLUMN, DROP COLUMN (with table rewrite)
- VACUUM: full database rebuild, INTO variant
- REINDEX: rebuild specified or all indexes
- ANALYZE: populate sqlite_stat1 with sample-based statistics

**Acceptance criteria:**
- Index selection: query with equality on indexed column uses index scan
  (verified via EXPLAIN QUERY PLAN)
- Index selection: query with range (BETWEEN, <, >) uses index scan with
  proper bounds
- Partial index: query with matching WHERE clause uses partial index
- Expression index: query with matching expression uses expression index
- Join ordering: 4-table join selects optimal order (smallest intermediate
  result first)
- Window functions: row_number, rank, dense_rank, lag, lead, sum OVER
  with ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING all produce correct results
- CTE: recursive CTE generating Fibonacci sequence (first 20 terms)
- Trigger: BEFORE INSERT trigger that validates data, AFTER DELETE trigger
  that logs to audit table
- Foreign keys: INSERT into child table with non-existent parent FK fails,
  CASCADE DELETE removes child rows
- VACUUM INTO: creates identical but defragmented copy
- Target: 3,000+ tests

**Dependencies:** Phase 6 complete.

**Risk areas:** The WHERE optimizer is the most complex part of the query
planner. C SQLite's `where.c` is ~7,800 lines. Cost estimation without
statistics (before ANALYZE) relies on heuristics that must match C SQLite's
behavior for conformance.

**Estimated complexity:** ~20,000 LOC.

### Phase 8: Extensions

**Deliverables:** All extensions from Section 14, each in its own crate.

**Acceptance criteria per extension:**
- JSON1: All functions from Section 14.1 with JSONB round-trip, json_each
  and json_tree virtual table queries
- FTS5: Tokenize 100K documents, full-text search with BM25 ranking,
  highlight and snippet, prefix queries
- FTS3/4: matchinfo blob format matches C SQLite output
- R-Tree: 2D spatial index with 100K entries, range query, custom geometry
- Session: Generate changeset from modifications, apply to second database,
  verify identical content
- ICU: Create collation from locale, ORDER BY uses locale-correct sorting
- Misc: generate_series(1,1000000) performs in < 1 second

**Dependencies:** Phase 7 complete (extensions use virtual table API).

**Estimated complexity:** ~25,000 LOC.

### Phase 9: CLI, Conformance, Benchmarks, Replication

**Deliverables:**
- `crates/fsqlite-cli/`: Interactive shell using frankentui, dot-commands
  (`.tables`, `.schema`, `.mode`, `.headers`, `.import`, `.dump`), output
  modes (column, csv, json, table, markdown), tab completion, syntax
  highlighting, command history
- `crates/fsqlite-harness/`: Conformance test runner, golden file comparison
- `conformance/`: 1,000+ SQL test files with golden output from C sqlite3
- `benches/`: Criterion benchmark suite (see Section 17.8 for regression methodology)
- Fountain-coded replication: UDP-based symbol emission, receiver assembly,
  changeset application
- Snapshot shipping: full database transfer via RaptorQ encoding

**Acceptance criteria:**
- CLI: All sqlite3 dot-commands that have meaningful equivalents
- Conformance: 95%+ pass rate across all golden files
- Benchmarks: single-writer within 3x of C SQLite, multi-writer (non-
  contended) shows linear scaling up to 4 cores
- Replication: 10% packet loss, database replicates correctly within 1.2x
  of no-loss time (RaptorQ overhead)
- Target: 4,000+ tests

**Dependencies:** Phase 8 complete.

**Estimated complexity:** ~10,000 LOC.

---

## 17. Testing Strategy

### 17.1 Unit Tests (Per-Crate)

Every public function and every non-trivial private function has at least
one `#[test]`. Trait dependencies are mocked using hand-written mock
implementations (not a mocking framework) to keep tests understandable.

**Concrete test scenarios by crate:**

**fsqlite-types:**
- SqliteValue: comparison between Integer(3) and Real(3.0) returns Equal
- SqliteValue: Text("123") coerced to Integer context yields Integer(123)
- PageNumber: construction from 0 returns error
- Opcode: all 190+ variants have distinct u8 values
- Serial type: round-trip encode/decode for every serial type category

**fsqlite-vfs:**
- MemoryVfs: write 1MB, read back, verify byte-for-byte identity
- MemoryVfs: truncate from 1MB to 512KB, verify file_size and read
- UnixVfs: create in temp directory, write, close, reopen, read back
- UnixVfs: delete non-existent file returns appropriate error
- UnixVfs: two concurrent readers on same file see consistent data

**fsqlite-btree:**
- Test: insert 10K random i64 keys, delete 5K random subset, verify
  remaining 5K are all present and in sorted order via cursor iteration
- Test: insert keys forcing tree depth to 4, verify cursor traversal
  visits all keys
- Test: overflow page chain for 100KB payload, read back complete
- Test: freelist reclaims pages, verify via dbstat-equivalent accounting

### 17.2 Property-Based Tests (proptest)

**B-tree invariants:**
```rust
proptest! {
    #[test]
    fn btree_maintains_order(ops in vec(btree_op(), 0..10000)) {
        let mut tree = BTree::new(MemoryPager::new(4096));
        let mut reference = BTreeMap::new();
        for op in ops {
            match op {
                Op::Insert(k, v) => { tree.insert(k, v); reference.insert(k, v); }
                Op::Delete(k) => { tree.delete(k); reference.remove(&k); }
            }
        }
        // Invariant: cursor iteration matches reference
        let tree_entries: Vec<_> = tree.cursor().collect();
        let ref_entries: Vec<_> = reference.into_iter().collect();
        assert_eq!(tree_entries, ref_entries);
    }
}
```

**Parser round-trip:**
```rust
proptest! {
    #[test]
    fn parse_roundtrip(sql in arbitrary_select()) {
        let ast1 = parse(&sql).unwrap();
        let sql2 = ast1.to_sql_string();
        let ast2 = parse(&sql2).unwrap();
        assert_eq!(ast1, ast2);
    }
}
```

**Record format:**
```rust
proptest! {
    #[test]
    fn record_roundtrip(values in vec(arbitrary_sqlite_value(), 0..100)) {
        let encoded = encode_record(&values);
        let decoded = decode_record(&encoded);
        assert_eq!(values, decoded);
    }
}
```

**MVCC linearizability:**
```rust
proptest! {
    #[test]
    fn mvcc_snapshot_isolation(
        txns in vec(arbitrary_txn_ops(), 2..16),
        seed in any::<u64>()
    ) {
        let reactor = LabReactor::new(seed);
        let db = Database::open_in_memory(&reactor);
        // Execute all transactions concurrently under lab reactor
        // Verify: every committed transaction's reads are consistent
        // with its snapshot, every aborted transaction had a real conflict
    }
}
```

### 17.3 Deterministic Concurrency Tests (Lab Reactor)

All MVCC tests run under asupersync's lab reactor. Setup:

```rust
#[test]
fn mvcc_two_writers_different_pages() {
    let seed = 0xDEADBEEF_u64;
    let reactor = LabReactor::new(seed);

    reactor.run(async {
        let db = Database::open(":memory:", &reactor).await;
        db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").await;

        let (tx1_done, tx2_done) = (oneshot(), oneshot());

        // Transaction 1: insert into low rowids
        reactor.spawn(async {
            let txn = db.begin_concurrent().await;
            for i in 1..=100 { txn.execute("INSERT INTO t VALUES(?,?)", (i, "a")).await; }
            txn.commit().await.unwrap();
            tx1_done.send(());
        });

        // Transaction 2: insert into high rowids
        reactor.spawn(async {
            let txn = db.begin_concurrent().await;
            for i in 1001..=1100 { txn.execute("INSERT INTO t VALUES(?,?)", (i, "b")).await; }
            txn.commit().await.unwrap();
            tx2_done.send(());
        });

        join(tx1_done.recv(), tx2_done.recv()).await;
        let count: i64 = db.query_one("SELECT count(*) FROM t").await;
        assert_eq!(count, 200);
    });
}
```

**Seed management:** Each test uses a fixed seed for reproducibility.
CI runs each concurrency test with 100 different seeds. A failing seed is
recorded in the test failure message for exact replay.

**Fault injection:** The lab reactor supports injecting I/O failures:
```rust
reactor.inject_fault(FaultSpec {
    target: FaultTarget::Write { file: "test.db-wal", offset: 4096 },
    action: FaultAction::PartialWrite { bytes_written: 2048 },
    trigger: FaultTrigger::After { count: 50 },
});
```

### 17.4 Systematic Interleaving (Mazurkiewicz Traces)

**Concrete 3-transaction scenario:**

```
T1: BEGIN CONCURRENT; INSERT INTO t VALUES(1,'a'); COMMIT;
T2: BEGIN CONCURRENT; INSERT INTO t VALUES(2,'b'); COMMIT;
T3: BEGIN CONCURRENT; INSERT INTO t VALUES(3,'c'); COMMIT;

Operations (simplified):
  T1_w(page_A), T1_commit
  T2_w(page_B), T2_commit
  T3_w(page_A), T3_w(page_B), T3_commit

Independence relation:
  T1_w(A) independent of T2_w(B)  -- different pages
  T1_w(A) dependent on T3_w(A)    -- same page
  T2_w(B) dependent on T3_w(B)    -- same page

Distinct traces (non-equivalent orderings):
  1. T1_w(A), T1_commit, T2_w(B), T2_commit, T3_w(A), T3_w(B), T3_commit
     -> T3 sees T1's commit on page A: conflict if T3 also wrote A
  2. T1_w(A), T2_w(B), T1_commit, T2_commit, T3_w(A), T3_w(B), T3_commit
     -> Same outcome for T3
  3. T3_w(A), T3_w(B), T3_commit, T1_w(A), T1_commit, T2_w(B), T2_commit
     -> T1 sees T3's commit on page A: T1 conflict
  4. T1_w(A), T3_w(A), ...
     -> T3 gets SQLITE_BUSY immediately (page lock conflict)
  ... (enumerate all distinct orderings)

Verification for each trace:
  - If T_x committed: all its rows visible in final state
  - If T_x aborted: none of its rows visible
  - Total rows = sum of committed transactions' insert counts
  - No phantom rows
```

The Mazurkiewicz trace explorer generates all non-equivalent orderings
(typically tens to low hundreds for 3-5 transaction scenarios) and verifies
invariants for each. This is feasible for small scenarios and provides
exhaustive coverage that random testing cannot guarantee.

### 17.5 Runtime Invariant Monitoring (E-Processes)

E-process configuration for MVCC invariants:

| Invariant | Test statistic | Threshold | Alert condition |
|-----------|---------------|-----------|-----------------|
| INV-1 (Monotonicity) | Consecutive TxnId difference | >= 1 | Any difference < 1 |
| INV-2 (Lock Exclusivity) | Max concurrent holders per page | <= 1 | Any count > 1 |
| INV-3 (Version Chain Order) | Chain order violations per 1K ops | 0 | Any violation |
| INV-4 (Write Set Consistency) | Unlocked writes per 1K ops | 0 | Any unlocked write |
| INV-6 (Commit Atomicity) | Partial visibility observations | 0 | Any partial observation |

E-processes use Ville's inequality: the process `E_t = prod(1 + lambda_i *
(X_i - mu_0))` where lambda_i is the betting fraction and X_i is the
observed statistic. If `E_t >= 1/alpha`, reject the null hypothesis (system
is correct) at significance level alpha. For invariant monitoring,
alpha = 0.001 provides a < 0.1% false alarm rate.

The key advantage over traditional assertions: e-processes can be checked
at ANY point during execution, not just at the end. An invariant violation
is detected as soon as it occurs, not after the test completes.

### 17.6 Fuzz Test Specifications

**SQL parser fuzz target:**
```rust
// fuzz/fuzz_targets/sql_parser.rs
fuzz_target!(|data: &[u8]| {
    if let Ok(sql) = std::str::from_utf8(data) {
        let _ = fsqlite_parser::parse(sql);
        // Must not panic, must not loop forever
    }
});
```

**Grammar-based SQL fuzzing:** Use `arbitrary` crate to generate structured
SQL from the grammar, not just random bytes. This achieves deeper coverage:
```rust
#[derive(Arbitrary)]
enum FuzzStatement {
    Select(FuzzSelect),
    Insert(FuzzInsert),
    // ...
}

impl FuzzStatement {
    fn to_sql(&self) -> String { ... }
}

fuzz_target!(|stmt: FuzzStatement| {
    let sql = stmt.to_sql();
    let result = db.execute(&sql);
    // Must not panic, must not corrupt database
    // If Ok, verify with PRAGMA integrity_check
});
```

**Other fuzz targets:**
- `record_decoder`: arbitrary bytes -> `decode_record()` -> must not panic
- `btree_page_decoder`: arbitrary 4096-byte pages -> page parser -> no panic
- `wal_frame_decoder`: arbitrary frame bytes -> frame parser -> no panic
- `json_parser`: arbitrary bytes -> `json_valid()` returns 0 or 1, no panic
- `raptorq_decoder`: valid encoding with random bit flips -> decoder either
  succeeds with correct output or returns error, never silent corruption

### 17.7 Conformance Testing

**Principle:** Conformance is not Phase 9. It starts in Phase 1, and it is how
we keep the project honest while being radically innovative internally.

> We are allowed to change *how* it works. We are not allowed to change *what
> it does* (unless explicitly approved).

**The Oracle:** C SQLite 3.52.0 built from `legacy_sqlite_code/`. The harness
MUST be able to run the Oracle in-process or via a small runner binary, execute
SQL statements, and capture results deterministically.

**Categories:**
- DDL: CREATE/DROP/ALTER for tables, indexes, views, triggers (100+ tests)
- DML: INSERT/UPDATE/DELETE with all clause variants (200+ tests)
- Expressions: arithmetic, string ops, type coercion, NULL handling (150+ tests)
- Functions: every built-in function with edge cases (200+ tests)
- Transactions: BEGIN/COMMIT/ROLLBACK, savepoints, isolation (100+ tests)
- Edge cases: empty tables, MAX_LENGTH values, Unicode, zero-length blobs (100+ tests)
- Extensions: JSON1, FTS5, R-Tree basic operations (100+ tests)
- Concurrency regression: write skew patterns (must abort under default
  serializable mode in `BEGIN CONCURRENT`)

**What we compare (not just rows):**
- Result rows (including NULL behavior)
- Type affinity where observable
- Error code + extended error code (normalized)
- Affected-row counts (`changes()`, `total_changes()`)
- `last_insert_rowid()` where relevant
- Transaction boundary effects (commit/rollback, savepoints)

**JSON fixture format (self-describing):**

```json
{
  "name": "insert-and-select",
  "steps": [
    { "op": "open", "flags": "readwrite_create", "pragmas": ["journal_mode=WAL"] },
    { "op": "exec", "sql": "CREATE TABLE t(x INTEGER);" },
    { "op": "exec", "sql": "INSERT INTO t VALUES (1),(2),(3);" },
    { "op": "query", "sql": "SELECT x FROM t ORDER BY x;",
      "expect": { "rows": [["1"],["2"],["3"]] } }
  ]
}
```

JSON fixtures are generated by the Oracle runner and consumed by Rust tests.
Harness MUST support multi-step cases (transactions, temp objects, pragmas).
Results are string-normalized by default; type-aware comparison is opt-in.

**SQLLogicTest (SLT) ingestion:** The harness MUST also consume SQLLogicTest
files for broad SQL coverage. SLT provides thousands of pre-existing test
queries with expected results.

**Normalization rules (avoid false failures):**
- Unordered SELECT results: compare as multisets when SQL has no ORDER BY.
- Floating-point: compare exact strings (default) or tolerance mode where
  explicitly requested.
- Error messages: compare error codes; messages are normalized (Oracle's exact
  phrasing is not stable across versions).

**Golden output discipline:** Every optimization or refactor must preserve
golden outputs unless we explicitly document an intentional divergence and
add a harness annotation explaining why it is acceptable.

**Golden file format (simple text):**
```
-- test: insert_returning
-- description: INSERT with RETURNING clause
INSERT INTO t VALUES(1, 'a') RETURNING rowid, *;
-- expected:
-- 1|1|a
```

### 17.8 Performance Regression Detection

Statistical methodology using asupersync's conformal calibration:

1. **Baseline establishment:** Run benchmark suite 100 times on reference
   commit, record all measurements.
2. **Candidate measurement:** Run benchmark suite 30 times on candidate commit.
3. **Conformal p-value:** For each benchmark, compute the conformal p-value
   testing H0: "candidate is no slower than baseline."
4. **Threshold:** Flag as regression if p-value < 0.01 (1% significance).
5. **Multiple testing correction:** Bonferroni correction across all
   benchmarks to control family-wise error rate.
6. **No distributional assumptions:** Conformal p-values are valid regardless
   of the underlying distribution (important because database latencies are
   typically heavy-tailed, not normal).

---

## 18. Probabilistic Conflict Model

### 18.1 Problem Statement

Given N concurrent writing transactions, each touching W pages uniformly at
random from a database of P total pages, what is the probability that at
least two transactions conflict (write to the same page)?

### 18.2 Pairwise Conflict Probability

Consider two transactions T1 and T2, each writing W pages chosen uniformly
at random (without replacement) from P total pages.

The probability that T1 and T2 do NOT conflict is the probability that T2's
W pages are all disjoint from T1's W pages:

```
P(no conflict between T1, T2)
  = C(P-W, W) / C(P, W)
  = product_{i=0}^{W-1} (P - W - i) / (P - i)
```

For W << P, this approximates to:

```
P(no conflict) ~ ((P-W)/P)^W ~ e^(-W^2/P)
P(conflict between T1, T2) ~ 1 - e^(-W^2/P)
```

### 18.3 Birthday Paradox Connection

This is exactly the birthday paradox. If each transaction writes W pages
out of P, and we treat each written page as a "birthday" in a year with
P "days", the probability that any two of N transactions share a page is:

```
P(any conflict among N txns) ~ 1 - e^{-N(N-1)W^2 / (2P)}
```

This matches the birthday paradox formula `P ~ 1 - e^{-n^2/(2*365)}` with
the substitution `n = N*W` effective items and `365 = P` possible values,
divided by the within-transaction correlation factor.

**Intuition:** Conflicts become likely when `N * W ~ sqrt(P)`. For a
database with P = 1,000,000 pages, conflicts become probable when
`N * W ~ 1,000`. So 10 transactions each writing 100 pages, or 100
transactions each writing 10 pages, will start seeing conflicts.

### 18.4 Non-Uniform Page Access: Zipf Distribution

Real workloads are NOT uniform. B-tree access patterns follow approximately
a Zipf distribution because:

1. **Root page:** Every B-tree operation reads the root page. For writes
   that modify the tree structure (splits, merges), the root page is also
   written.

2. **Internal pages:** Higher-level internal pages are accessed more
   frequently than lower-level ones (they fan out to many leaf pages).

3. **Hot leaf pages:** In many workloads, recent inserts cluster on a few
   "hot" leaf pages (e.g., auto-increment keys always hit the rightmost leaf).

For Zipf-distributed access with parameter s, the probability of accessing
page ranked k is:

```
p(k) = (1/k^s) / H(P,s)

where H(P,s) = sum_{i=1}^{P} 1/i^s  (generalized harmonic number)
```

With s ~ 0.8-1.2 (typical for database workloads), the conflict probability
increases significantly compared to the uniform model:

```
P(conflict, Zipf) ~ 1 - product_{k} (1 - p(k))^{n_k}
```

where n_k is the expected number of transactions accessing page k.

### 18.5 B-Tree Hotspot Analysis

Specific B-tree operations that create conflict hotspots:

**Root page modifications:** When a B-tree root page splits, the root is
rewritten. Any concurrent transaction also writing to the same B-tree will
conflict on the root page, even if it targets a completely different key
range. Root splits are rare for large trees (depth 3+ trees split the root
only when growing from depth d to d+1) but catastrophic for concurrency
when they happen.

**Page splitting as conflict amplifier:** A single INSERT that causes a leaf
page split modifies: (1) the leaf page being split, (2) the new sibling
leaf page, (3) the parent internal page (to add the new child pointer), and
potentially (4) the parent's parent if the parent also splits. A single
INSERT can touch 2-4 pages, increasing the effective W per transaction.

**Index maintenance:** Each INSERT into a table with K indexes modifies
~1 + K pages (one table leaf + one leaf per index), multiplied by split
probability. A table with 5 indexes has an effective W per INSERT of ~6
in the no-split case, ~12-20 in the split case.

### 18.6 Empirical Validation Methodology

To validate the probabilistic model against actual conflict rates:

1. **Instrumentation:** Add counters to the MVCC commit path:
   - `conflicts_detected`: total page-level conflicts
   - `conflicts_merged`: conflicts resolved by algebraic merge
   - `conflicts_aborted`: conflicts that caused transaction abort
   - `total_commits`: total commit attempts
   - `pages_per_commit`: histogram of write set sizes

2. **Benchmark workloads:**
   - Uniform random: INSERT with random keys into large table
   - Sequential: INSERT with auto-increment keys
   - Zipf: INSERT with Zipf-distributed keys (s = 0.99)
   - Mixed: 80% read, 20% write across 4 tables

3. **Comparison:** Plot actual conflict rate vs model prediction. Expected
   result: uniform model matches uniform workload within 10%, Zipf model
   matches skewed workloads within 20%.

### 18.7 Impact of Algebraic Write Merging

Algebraic write merging (Section 3.4.5) reduces the effective conflict rate
by resolving page-level conflicts at byte level.

**Worked example:**
- Table page: 4096 bytes, containing ~40 rows of ~100 bytes each
- T1 inserts row at cell offset 200, modifying bytes 200-300
- T2 inserts row at cell offset 3500, modifying bytes 3500-3600
- Without merge: page conflict, one transaction aborted
- With merge: byte ranges [200,300] and [3500,3600] are disjoint,
  algebraic merge succeeds, both transactions commit

**Effective conflict rate reduction:**
For a page with C cells, two random insertions have a byte-level conflict
probability of approximately:
```
P(byte conflict) ~ P(page conflict) * (avg_cell_size / page_size)
                 ~ P(page conflict) * (1/C)
```

For a page with 40 cells, this is a ~97.5% reduction in effective conflicts.
However, this only applies when both transactions are pure insertions to
the same leaf page. Updates that modify cell content may have overlapping
byte ranges even for different logical rows (due to cell pointer array
modifications, free block list updates, etc.).

**Realistic estimate:** For INSERT-heavy workloads, algebraic merge reduces
effective conflict rate by 30-60%. For UPDATE-heavy workloads, the reduction
is smaller (10-20%) because updates are more likely to modify shared
page-level structures (cell pointer array reordering).

### 18.8 Throughput Model

The committed transactions per second (TPS) under contention:

```
TPS = N * (1 - P_abort) * (1 / T_txn)

where:
  N = number of concurrent writers
  P_abort = probability a transaction must abort and retry
  T_txn = average transaction duration (seconds)
```

P_abort depends on the conflict rate and the number of retries:

```
P_abort_first_attempt ~ 1 - product_{j != i} (1 - P(conflict with T_j))
P_abort_after_K_retries ~ P_abort_first^K  (geometric retry)
```

For the typical case (medium DB, moderate writers):
- P = 100,000 pages, W = 50 pages/txn, N = 8 writers
- P(pairwise conflict) ~ 1 - e^(-50^2/100000) ~ 0.025
- P(any conflict for one txn) ~ 1 - (1-0.025)^7 ~ 0.16
- With algebraic merge reducing 40%: effective P_abort ~ 0.10
- With one retry: P_abort ~ 0.01
- TPS ~ 8 * 0.99 / T_txn

This shows that for medium-to-large databases, MVCC concurrent writers
achieve near-linear scaling up to ~8 writers. Beyond that, conflict rates
grow quadratically (birthday paradox) and throughput plateaus.

---

## 19. C SQLite Behavioral Reference

For the complete behavior extraction from C SQLite source (data structures,
SQL grammar, all 190+ VDBE opcodes, B-tree page format, WAL format, all
PRAGMA commands, all built-in functions, extension APIs, error codes, locking
protocol, transaction semantics, virtual table interface, threading model,
and limits), see `EXISTING_SQLITE_STRUCTURE.md`.

That document is the authoritative behavioral spec. Implementation should
consult ONLY that document for C SQLite behavior, not the C source code
directly (per the porting methodology: extract spec from legacy, implement
from spec, never translate line-by-line).

**Key behavioral quirks that differ from naive expectations:**

- **Type affinity is advisory, not enforced** (except STRICT tables). You
  can store a TEXT value in an INTEGER column. The affinity only affects
  type coercion during comparison and storage, not rejection.

- **NULL handling in UNIQUE constraints:** SQLite allows multiple NULL
  values in a UNIQUE column (NULL != NULL). This differs from some other
  databases.

- **ORDER BY on compound SELECT:** ORDER BY at the end of a compound
  SELECT (UNION, EXCEPT, INTERSECT) uses column numbers or aliases from
  the FIRST select, not the last.

- **Integer overflow wraps silently** in some contexts. The `sum()`
  aggregate raises an error on overflow, but arithmetic expressions like
  `9223372036854775807 + 1` promote to REAL (floating point) rather than
  wrapping.

- **AUTOINCREMENT vs rowid reuse:** Without AUTOINCREMENT, deleted rowids
  CAN be reused. `max(rowid)+1` is used for new rows, but if the maximum
  rowid is `SQLITE_MAX_ROWID` (2^63-1), SQLite tries random rowids.

- **LIKE is case-insensitive for ASCII only.** The built-in LIKE does not
  handle Unicode case folding. `'a' LIKE 'A'` is true, but `'ä' LIKE 'Ä'`
  is false without ICU.

- **Empty string vs NULL:** `''` (empty string) is NOT NULL. `length('')`
  returns 0, not NULL. `'' IS NULL` is false.

- **Deterministic vs non-deterministic functions:** Functions like
  `random()`, `changes()`, and `last_insert_rowid()` are non-deterministic
  and are re-evaluated for each row. The query planner cannot factor them
  out of loops.

---

## 20. Key Reference Files

### C SQLite Source (for spec extraction only)

| File | Purpose | Lines | What to Extract |
|------|---------|-------|-----------------|
| `sqliteInt.h` | Master header | ~250KB | All struct definitions (Btree, BtCursor, Pager, Wal, Vdbe, Mem, Table, Index, Column, Expr, Select, etc.), all `#define` constants, all function prototypes. This is the Rosetta Stone. |
| `btree.c` | B-tree engine | 11,568 | Page format parsing, cell format, cursor movement algorithms (moveToChild, moveToRoot, moveToLeftmost, moveToRightmost), insert/delete with rebalancing, overflow page management, freelist operations. Focus on `balance_nonroot` (~1,200 lines) as the most complex function. |
| `pager.c` | Page cache | 7,834 | Pager state machine (OPEN, READER, WRITER_LOCKED, WRITER_CACHEMOD, WRITER_DBMOD, WRITER_FINISHED, ERROR), journal format, hot journal detection, page reference counting, cache eviction policy. |
| `wal.c` | WAL subsystem | 4,621 | WAL header/frame format, checksum algorithm implementation, WAL index (wal-index) hash table structure, checkpoint algorithm, the critical `WAL_WRITE_LOCK` at line 3698 that FrankenSQLite replaces with MVCC. |
| `vdbe.c` | VDBE interpreter | 9,316 | The giant switch statement dispatching all opcodes. Each case is the authoritative definition of what that opcode does. Extract: register manipulation, cursor operations, comparison semantics, NULL handling per opcode. |
| `select.c` | SELECT compilation | 8,972 | How SELECT is compiled to VDBE opcodes: result column processing, FROM clause flattening, subquery handling, compound SELECT, DISTINCT, ORDER BY, LIMIT. |
| `where.c` | WHERE optimization | 7,858 | Index selection algorithm, cost estimation, OR optimization, skip-scan, automatic index creation. The `WhereTerm`, `WhereLoop`, and `WherePath` structures define the optimizer's search space. |
| `parse.y` | LEMON grammar | 2,160 | The authoritative SQL grammar. Every production rule defines a valid SQL construct. Use as the reference for the recursive descent parser. |
| `tokenize.c` | SQL tokenizer | 899 | Token types, keyword recognition, string/number/blob literal parsing, comment handling. |
| `func.c` | Built-in functions | 3,461 | Implementation of all scalar and aggregate functions. Edge case behaviors (NULL handling, type coercion, overflow) are defined here. |
| `expr.c` | Expression handling | 7,702 | Expression compilation, affinity computation, collation resolution, constant folding. |
| `build.c` | DDL processing | 5,815 | CREATE TABLE/INDEX/VIEW/TRIGGER compilation, schema modification, type affinity determination from type name strings. |

### Asupersync Modules

| Module | What FrankenSQLite Uses | Why It Matters |
|--------|----------------------|----------------|
| `src/raptorq/` | RFC 6330 codec | WAL self-healing, replication, version chain compression. The core innovation enabler. |
| `src/sync/` | Mutex, RwLock, Condvar | MVCC lock table, version chain access, global write mutex for serialized mode. |
| `src/channel/mpsc.rs` | Two-phase MPSC | Write coordinator commit pipeline with cancel-safety and backpressure. |
| `src/channel/oneshot.rs` | Oneshot response | Commit response delivery from coordinator to committing transaction. |
| `src/cx/` | Capability context | Threading through every function for cancellation, deadlines, and capability narrowing. |
| `src/lab/runtime.rs` | Deterministic runtime | Reproducible concurrency testing, fault injection, virtual time. |
| `src/lab/explorer.rs` | DPOR + Mazurkiewicz traces | Systematic schedule exploration for small critical concurrency scenarios. |
| `src/obligation/eprocess.rs` | E-process core | Anytime-valid monitoring for invariant violations under optional stopping. |
| `src/lab/oracle/eprocess.rs` | E-process oracle | Test harness + certificates for e-process monitoring. |
| `src/lab/conformal.rs` | Distribution-free stats | Benchmark regression detection without parametric assumptions. |
| `src/database/sqlite.rs` | API reference | FrankenSQLite's public API mirrors asupersync's SQLite wrapper API for familiarity. |

### Project Documents

| Document | Purpose | When to Consult |
|----------|---------|-----------------|
| `COMPREHENSIVE_SPEC_FOR_FRANKENSQLITE_V1.md` | Source of truth | Always. This document supersedes all others. |
| `EXISTING_SQLITE_STRUCTURE.md` | C SQLite behavior | When implementing any feature: look up the C behavior first, then implement from the spec. |
| `docs/rfc6330.txt` | RaptorQ specification | When implementing RaptorQ integration (WAL, replication, version chains). |
| `AGENTS.md` | Coding guidelines | Before every coding session: review style, testing, and documentation requirements. |
| `MVCC_SPECIFICATION.md` | MVCC formal model (legacy) | Historical reference only. Section 5 of this document supersedes it with corrections. |
| `PROPOSED_ARCHITECTURE.md` | Architecture overview (legacy) | Historical reference. Section 8 of this document supersedes the crate map. |

---

## 21. Risk Register, Open Questions, and Future Work

### 21.0 Risk Register (With Mitigations)

**R1. SSI abort rate too high (Page-SSI is conservative).**
Mitigations:
- Refine SIREAD keys from page → (page, range/cell tag) to reduce false positives.
- Add safe snapshot optimizations for read-only transactions.
- Intent-level rebase (Section 5.10.2) turns page conflicts into merges,
  reducing effective conflict rate by 30-60%.
- PostgreSQL's measured false positive rate is ~0.5% at row granularity; our
  page granularity will be higher, but merge compensation helps.

**R2. RaptorQ overhead dominates CPU.**
Mitigations:
- Choose symbol sizing policy based on object type (capsules: small symbols
  for fast commit; checkpoints: large symbols for throughput).
- Cache decoded objects aggressively (ARC cache).
- Profile and tune encoder/decoder hot paths (one lever per change, per
  the Extreme Optimization methodology).

**R3. Append-only storage grows without bound.**
Mitigations:
- Checkpoint and compaction are first-class (Section 7.9).
- Enforce budgets for MVCC history, SIREAD table, symbol caches.
- GC horizon = min(active_txn_ids) bounds version chain length (Theorem 5).

**R4. Bootstrapping chicken-and-egg (need index to find symbols, need symbols
to decode index).**
Mitigations:
- Symbol records are self-describing (header + OTI).
- One tiny mutable root pointer per database.
- Rebuild-from-scan is always possible as a fallback.

**R5. Multi-process MVCC coordination is complex.**
Mitigations:
- Shared-memory coordination protocol specified (Section 5.6.1).
- Lease-based TxnSlot cleanup handles process crashes without blocking.
- In-process MVCC is validated first (Phase 6), cross-process follows (Phase 7).
- Explicit tests for multi-process behaviors required before shipping.

**R6. File format compatibility vs "do it right".**
Mitigations:
- Compatibility Mode (Section 7.10) treats SQLite `.db/.wal` as the standard
  format for conformance.
- Native Mode is the innovation layer.
- Conformance harness validates observable behavior, not byte-identical layout.

**R7. Mergeable writes become a correctness minefield.**
Mitigations:
- Strict merge ladder (Section 5.10.4): only take merges we can justify.
- Proptest invariants + DPOR tests (Section 5.10.5).
- Start with deterministic rebase replay for a small op subset (inserts/updates
  on leaf pages), grow coverage guided by conflict benchmarks.

**R8. Distributed mode correctness is hard.**
Mitigations:
- Symbol-native replication uses "leader commit clock" as the default mode.
- Use sheaf checks + TLA+ export for bounded model checking.
- Replication protocol is ECS-native: ObjectId set reconciliation + anti-entropy.
- Implementation phased: single-node first, then multi-node (Phase 9).

### 21.1 Open Questions (With How We Answer Them)

**Q1. Multi-process writers:** What is the performance envelope for cross-process
concurrent writes?
*Answer plan:* Implement shared-memory coordination (Section 5.6.1); benchmark
contention vs in-process baseline; tune TxnSlot count and lease intervals.

**Q2. How far do we go with range/cell refinement for SIREAD?**
*Answer plan:* Start page-only; collect abort witnesses; refine only when
abort rate is proven unacceptable by benchmark.

**Q3. Symbol sizing policy per object type (capsule vs checkpoint vs index).**
*Answer plan:* Benchmark encode/decode throughput vs object sizes; pick
defaults; expose PRAGMA overrides for experiments.

**Q4. Where to checkpoint for compatibility `.db` without bottlenecking writes?**
*Answer plan:* Background checkpoint with ECS chunks; measure; keep export
optional.

**Q5. Which B-tree operations can be replayed deterministically for rebase merge?**
*Answer plan:* Implement inserts/updates on leaf pages first; grow coverage
guided by conflict benchmarks.

### 21.2 Cross-Process MVCC (Implementation Notes)

Cross-process MVCC is specified in Section 5.6.1. Implementation notes:
- Phase 6 validates in-process MVCC correctness
- Phase 7 extends to cross-process using the shared-memory coordination region
- Key challenge: benchmarking the mmap-based TxnSlot array vs in-process atomics
- Lease-based cleanup must be stress-tested under process crash scenarios

### 21.3 Write-Ahead-Log Multiplexing

For very high write throughput (>100K TPS), a single WAL file becomes the
bottleneck (sequential append to one file). WAL multiplexing shards WAL
frames across multiple files:
- WAL file selected by `hash(page_number) % num_wal_files`
- Each WAL file has its own checkpoint state
- Commit requires atomic append to all WAL files touched by the transaction
  (2PC across WAL files)
- Potential 4-8x improvement in sustained write throughput on NVMe SSDs
  with high queue depth

### 21.4 Distributed Consensus Integration

For multi-node deployments, integrate Raft or Paxos for replicated state:
- WAL entries as the replicated log
- Leader handles all writes, followers handle reads (read replicas)
- Snapshot shipping (Section 3.4.3) for new follower initialization
- RaptorQ-coded replication (Section 3.4.2) for steady-state log shipping
- Challenge: linearizable reads require either reading from leader or
  implementing read leases

### 21.5 GPU-Accelerated RaptorQ Encoding

For bulk operations (full database backup, large changeset replication),
RaptorQ encoding is CPU-bound. GPU acceleration via compute shaders:
- GF(256) arithmetic maps well to SIMD/GPU (each symbol byte independent)
- Matrix multiplication for intermediate symbol generation is embarrassingly
  parallel
- Expected speedup: 10-50x for large source blocks (K > 10,000)
- Framework: wgpu for cross-platform GPU compute

### 21.6 Persistent Memory (PMEM) VFS

Intel Optane and CXL-attached persistent memory enables byte-addressable
persistent storage. A PMEM VFS would:
- Memory-map the database file directly to PMEM
- Eliminate the WAL entirely (direct in-place updates with 8-byte atomic
  writes for crash consistency)
- Use `clflush`/`clwb` instructions for cache line persistence
- MVCC version chains stored directly in PMEM with epoch-based reclamation
- Expected latency reduction: 10-100x for small transactions (eliminate
  WAL write + fsync)

### 21.7 Vectorized VDBE Execution

Current VDBE processes one row at a time (Volcano model). Vectorized
execution processes batches of rows through each operator:
- Column-at-a-time processing enables SIMD utilization
- Better CPU cache behavior (fewer instruction cache misses)
- Applicable to full table scans, aggregations, and hash joins
- Expected speedup: 2-5x for analytical queries, negligible for point lookups
- Challenge: must maintain row-at-a-time semantics for triggers and
  RETURNING clause

### 21.8 Column-Store Hybrid for Analytical Queries

For mixed OLTP/OLAP workloads, a column-store representation alongside
the row-store B-tree:
- Column groups stored in separate B-trees per column
- Automatic materialization of frequently-scanned columns
- RLE and dictionary compression for low-cardinality columns
- Query planner selects row-store or column-store based on query pattern
- Challenge: maintaining consistency between row-store and column-store
  under concurrent writes

### 21.9 Erasure-Coded Page Storage (Implementation Notes)

Section 3.4.6 fully specifies erasure-coded page storage. Implementation notes:
- Modified page allocation: allocate G pages as a group
- Repair page storage: in the ECS object store (Native mode) or in a
  dedicated repair region of the database file (Compatibility mode)
- Read path: attempt source page first, fall back to erasure recovery
- Group size selection: benchmark G=32, G=64, G=128 to find the optimal
  balance of space overhead vs recovery capability per workload

---

## 22. Verification Gates

Every phase must pass all applicable gates before proceeding to the next.

### Universal Gates (All Phases)

1. `cargo check --workspace` -- zero errors, zero warnings
2. `cargo clippy --workspace --all-targets -- -D warnings` -- zero warnings
   with pedantic + nursery lints
3. `cargo fmt --all -- --check` -- all code formatted
4. `cargo test --workspace` -- all tests pass, no ignored tests without
   documented reason
5. `cargo doc --workspace --no-deps` -- all public items documented, no
   broken doc links

### Phase-Specific Gates

**Phase 2 gates:**
- MemoryVfs passes all VFS trait contract tests
- Record format round-trip proptest with 10,000 iterations, zero failures
- Zero `unsafe` blocks in any crate

**Phase 3 gates:**
- B-tree proptest: 10,000-operation random sequence, invariants hold
- B-tree: cursor iteration after random ops matches BTreeMap reference
- Parser: 95% coverage of `parse.y` grammar productions
- Parser fuzz: 1 hour of fuzzing with zero panics

**Phase 4 gates:**
- End-to-end: 20 SQL conformance tests (basic DDL + DML) pass
- VDBE: EXPLAIN output for basic queries matches expected opcode sequence
- Sorter: correctly sorts 100,000 rows

**Phase 5 gates:**
- File format: database created by FrankenSQLite readable by C sqlite3
- File format: database created by C sqlite3 readable by FrankenSQLite
- WAL recovery: 100 crash-recovery scenarios with zero data loss
- RaptorQ WAL: recovery succeeds with up to R corrupted frames (R = repair
  symbol count)

**Phase 6 gates:**
- MVCC stress test: 100 concurrent writers, 100 operations each, all
  committed rows present, no phantom rows
- SSI: write skew patterns produce abort under default serializable mode;
  same patterns succeed under PRAGMA fsqlite.serializable=OFF
- SSI: no false negatives (no write skew anomaly escapes detection in
  3-transaction Mazurkiewicz trace exploration)
- Snapshot isolation: verified via Mazurkiewicz trace exploration for
  3-transaction scenarios (all non-equivalent orderings)
- E-process monitors: INV-1 through INV-7, zero violations over 1M operations
- GC memory bound: memory usage under sustained load stays within 2x of
  minimum theoretical (active transactions * pages per transaction * page size)
- Serialized mode: behavior identical to C SQLite for single-writer test suite
- Rebase merge: 1,000 merge attempts with distinct-key inserts on same page,
  zero false rejections
- Algebraic merge: 1,000 merge attempts with known-disjoint byte
  modifications, zero false rejections; 1,000 attempts with overlapping
  modifications, zero false acceptances
- Crash model: 100 crash-recovery scenarios validating self-healing durability
  contract (Section 7.9)

**Phase 7 gates:**
- Query planner: EXPLAIN QUERY PLAN shows index usage for indexed queries
- Window functions: 50 conformance tests matching C SQLite output
- CTE: recursive CTE terminates correctly with LIMIT

**Phase 8 gates:**
- JSON1: json_valid/json_extract/json_set pass 200 conformance tests
- FTS5: full-text search returns relevant results for 100 test queries
- R-Tree: spatial query returns correct results for 50 bounding box queries

**Phase 9 gates:**
- Conformance: 95%+ pass rate across 1,000+ golden files
- Benchmarks: single-writer within 3x of C SQLite
- Benchmarks: no regression (conformal p-value > 0.01) compared to Phase 8

---

## 23. Summary: What Makes FrankenSQLite Alien

FrankenSQLite is not an incremental improvement on SQLite. It is a
ground-up reimagination of what an embedded database engine can be when
built on information-theoretic foundations and modern language guarantees.

**1. MVCC with Serializable Concurrent Writers (In-Process and Cross-Process).**
The single biggest limitation of SQLite -- the WAL_WRITE_LOCK that serializes
all writers -- is replaced with page-level MVCC versioning and Serializable
Snapshot Isolation (SSI). Applications choose their isolation level: Serialized
mode for exact backward compatibility, Concurrent mode for true multi-writer
parallelism with full SERIALIZABLE guarantees (not merely Snapshot Isolation).
The conservative Page-SSI rule prevents write skew by default; algebraic write
merging and intent-based deterministic rebase reduce conflict rates on hot
pages without row-level MVCC metadata. Cross-process MVCC uses a shared-memory
coordination region with lease-based crash cleanup. The layered approach means
zero risk for existing applications and serializable concurrency for
applications that opt in.

**2. RaptorQ-Pervasive Architecture with ECS Substrate.** Fountain codes are
not bolted on as an afterthought. They are woven into every layer: the WAL
uses RaptorQ repair symbols for self-healing durability that survives torn
writes without double-write journaling. The replication protocol is
fountain-coded for bandwidth-optimal, UDP-based, multicast-capable data
transfer over lossy networks. Version chains use RaptorQ delta encoding for
near-optimal compression. Conflict resolution uses GF(256) algebraic merging
to resolve page-level false conflicts at byte granularity. The Erasure-Coded
Stream (ECS) substrate provides content-addressed, self-describing,
deterministic object storage with BLAKE3 ObjectIds and self-healing repair
symbols. The result: data loss becomes a mathematical near-impossibility
rather than a failure mode to mitigate.

**3. Asupersync Deep Integration.** Every operation threads a Cx capability
context for type-safe cancellation and deadline propagation. The lab reactor
enables fully deterministic concurrency testing with reproducible scheduling
and precise fault injection. E-processes provide anytime-valid statistical
invariant monitoring based on Ville's inequality. Mazurkiewicz traces
systematically enumerate all non-equivalent interleavings of concurrent
transactions for exhaustive verification. Conformal calibration provides
distribution-free confidence intervals for benchmark regression detection.
Sheaf-theoretic consistency checking formally verifies that MVCC snapshot
views are globally consistent.

**4. Safe Rust, No Compromises.** `unsafe_code = "forbid"` at workspace
level. Clippy pedantic and nursery lints at deny level. If it compiles,
it is free of undefined behavior, data races, and use-after-free. The
entire database engine -- including the B-tree, VDBE, MVCC system, and
all extensions -- is memory-safe by construction.

**5. Full Compatibility.** FrankenSQLite reads and writes standard SQLite
database files. It targets 95%+ conformance against golden-file tests
comparing output with C sqlite3. The SQL dialect, type affinity system,
VDBE instruction set, file format, and WAL format all match SQLite 3.52.0.
It is a drop-in replacement for the sqlite3 CLI and library.

**6. Formal Verification Depth.** The MVCC system is specified with formal
invariants (INV-1 through INV-7), safety proofs (deadlock freedom, snapshot
isolation, serializable mode, first-committer-wins, GC safety), SSI
correctness argument (conservative rw-antidependency rule prevents cycles),
and a probabilistic conflict model validated empirically. The testing strategy
combines property-based testing, deterministic concurrency testing, systematic
interleaving exploration, anytime-valid statistical monitoring, grammar-based
fuzzing, and conformance testing against the reference implementation starting
from Phase 1 (not deferred to Phase 9). An explicit crash model, risk
register, and operating mode duality (Compatibility vs Native) ensure the
system is both innovative and verifiable. This is not aspirational -- these
tools exist in asupersync and are integrated into the test infrastructure.

FrankenSQLite demonstrates that embedded databases need not sacrifice
concurrency for simplicity, durability for performance, or safety for speed.
By building on information-theoretic optimality (RaptorQ), formal
verification techniques (e-processes, Mazurkiewicz traces, sheaf theory),
and the memory safety guarantees of Rust, it sets a new standard for what
an embedded database engine can achieve.

---

*Document version: 1.3 (Scope doctrine, ECS substrate, multi-process MVCC, mechanical sympathy)*
*Last updated: 2026-02-07*
*Status: Draft for review*
