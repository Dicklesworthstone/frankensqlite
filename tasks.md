# FrankenSQLite Implementation Tasks

Derived from `COMPREHENSIVE_SPEC_FOR_FRANKENSQLITE_V1.md` Section 16.

## Phase 1: Bootstrap and Spec Extraction [COMPLETE]
- [x] Create workspace with 23 crates <!-- id: 0 -->
- [x] Implement `fsqlite-types` (PageNumber, SqliteValue, Opcode, limits) <!-- id: 1 -->
- [x] Implement `fsqlite-error` (FrankenError, ErrorCode) <!-- id: 2 -->
- [x] Setup conformance harness infrastructure (Oracle runner) <!-- id: 3 -->

## Phase 2: Core Types and Storage Foundation [IN PROGRESS]
- [ ] Implement `fsqlite-vfs` traits (`Vfs`, `VfsFile`) <!-- id: 4 -->
- [ ] Implement `MemoryVfs` <!-- id: 5 -->
- [ ] Implement `fsqlite-types` record format (varint, serial types) <!-- id: 6 -->
- [ ] Implement `UnixVfs` with POSIX locking <!-- id: 7 -->
- [ ] Verify `MemoryVfs` thread safety <!-- id: 8 -->
- [ ] Verify Record format round-trip <!-- id: 9 -->
- [ ] Verify `UnixVfs` locking escalation <!-- id: 10 -->

## Phase 3: B-Tree and SQL Parser
- [ ] Implement `fsqlite-btree` cursor (page stack) <!-- id: 11 -->
- [ ] Implement `fsqlite-btree` cell parsing <!-- id: 12 -->
- [ ] Implement `fsqlite-btree` balance logic (split/merge) <!-- id: 13 -->
- [ ] Implement `fsqlite-btree` overflow pages <!-- id: 14 -->
- [ ] Implement `fsqlite-btree` freelist <!-- id: 15 -->
- [ ] Implement `fsqlite-ast` type hierarchy <!-- id: 16 -->
- [ ] Implement `fsqlite-parser` lexer <!-- id: 17 -->
- [ ] Implement `fsqlite-parser` recursive descent parser <!-- id: 18 -->
- [ ] Verify B-tree random insert/delete invariants <!-- id: 19 -->
- [ ] Verify Parser coverage of Section 12 statements <!-- id: 20 -->

## Phase 4: VDBE and Query Pipeline
- [ ] Implement `fsqlite-vdbe` engine (fetch-execute loop) <!-- id: 21 -->
- [ ] Implement `fsqlite-vdbe` Mem type and comparison <!-- id: 22 -->
- [ ] Implement critical VDBE opcodes (~50) <!-- id: 23 -->
- [ ] Implement `fsqlite-vdbe` sorter <!-- id: 24 -->
- [ ] Implement `fsqlite-planner` name resolution <!-- id: 25 -->
- [ ] Implement `fsqlite-planner` codegen (AST -> VDBE) <!-- id: 26 -->
- [ ] Implement `fsqlite-core` connection and schema <!-- id: 27 -->
- [ ] Implement `fsqlite` public API <!-- id: 28 -->
- [ ] Verify End-to-end basic DDL/DML (CREATE, INSERT, SELECT) <!-- id: 29 -->

## Phase 5: Persistence, WAL, and Transactions
- [ ] Implement `fsqlite-pager` state machine <!-- id: 30 -->
- [ ] Implement `fsqlite-pager` rollback journal <!-- id: 31 -->
- [ ] Implement `fsqlite-wal` (format, checksum, frame append) <!-- id: 32 -->
- [ ] Implement `fsqlite-wal` index (shm hash table) <!-- id: 33 -->
- [ ] Implement `fsqlite-wal` checkpoint <!-- id: 34 -->
- [ ] Implement `fsqlite-wal` recovery <!-- id: 35 -->
- [ ] Implement `fsqlite-wal` RaptorQ integration <!-- id: 36 -->
- [ ] Verify WAL recovery and checksums <!-- id: 37 -->
- [ ] Verify RaptorQ self-healing <!-- id: 38 -->

## Phase 6: MVCC Concurrent Writers with SSI
- [ ] Implement `fsqlite-mvcc` transaction types <!-- id: 39 -->
- [ ] Implement `fsqlite-mvcc` version chain & delta encoding <!-- id: 40 -->
- [ ] Implement `fsqlite-mvcc` lock table (sharded) <!-- id: 41 -->
- [ ] Implement `fsqlite-mvcc` witness plane (SSI evidence) <!-- id: 42 -->
- [ ] Implement `fsqlite-mvcc` SSI validation & pivot abort <!-- id: 43 -->
- [ ] Implement `fsqlite-mvcc` conflict resolution (FCW + merge ladder) <!-- id: 44 -->
- [ ] Implement `fsqlite-mvcc` GC & horizon management <!-- id: 45 -->
- [ ] Implement `fsqlite-mvcc` coordinator (two-phase MPSC) <!-- id: 46 -->
- [ ] Implement `fsqlite-pager` ARC cache with MVCC keys <!-- id: 47 -->
- [ ] Verify Concurrent mode (multi-writer) correctness <!-- id: 48 -->
- [ ] Verify SSI write skew prevention <!-- id: 49 -->
- [ ] Verify GC memory bounds <!-- id: 50 -->

## Phase 7: Advanced Query Planner, Full VDBE, SQL Features
- [ ] Implement full WHERE optimization <!-- id: 51 -->
- [ ] Implement cost-based join ordering <!-- id: 52 -->
- [ ] Implement remaining VDBE opcodes <!-- id: 53 -->
- [ ] Implement Window functions <!-- id: 54 -->
- [ ] Implement CTEs (recursive) <!-- id: 55 -->
- [ ] Implement Triggers <!-- id: 56 -->
- [ ] Implement Foreign Key enforcement <!-- id: 57 -->
- [ ] Implement ALTER TABLE, VACUUM, REINDEX, ANALYZE <!-- id: 58 -->

## Phase 8: Extensions
- [ ] Implement JSON1 extension <!-- id: 59 -->
- [ ] Implement FTS5 extension <!-- id: 60 -->
- [ ] Implement FTS3/4 extension <!-- id: 61 -->
- [ ] Implement R*-Tree extension <!-- id: 62 -->
- [ ] Implement Session extension <!-- id: 63 -->
- [ ] Implement ICU extension <!-- id: 64 -->
- [ ] Implement Misc extensions <!-- id: 65 -->

## Phase 9: CLI, Conformance, Benchmarks, Replication
- [ ] Implement `fsqlite-cli` <!-- id: 66 -->
- [ ] Implement `fsqlite-harness` full runner <!-- id: 67 -->
- [ ] Populate `conformance/` with 1000+ tests <!-- id: 68 -->
- [ ] Implement fountain-coded replication <!-- id: 69 -->
- [ ] Implement snapshot shipping <!-- id: 70 -->
- [ ] Verify 100% parity target against golden conformance suite (with any intentional divergences documented + annotated) <!-- id: 71 -->
