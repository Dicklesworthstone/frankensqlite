//! Deterministic operation log (OpLog) format and preset library.
//!
//! An **OpLog** is a self-contained, JSONL-serializable description of a
//! database workload.  Given the same OpLog, any compliant executor must
//! produce bit-identical side effects, enabling reproducible differential
//! testing between FrankenSQLite and C SQLite.
//!
//! # Wire format
//!
//! Each line of the JSONL file is either:
//! - The **header** (first line): an [`OpLogHeader`] describing the fixture,
//!   seed, RNG, and concurrency model.
//! - A **record** (subsequent lines): an [`OpRecord`] describing one operation.
//!
//! # Example (JSONL)
//!
//! ```text
//! {"fixture_id":"beads-dp-proj","seed":42,"rng":{"algorithm":"ChaCha12","version":"rand 0.8"},...}
//! {"op_id":0,"worker":0,"kind":{"Sql":{"statement":"CREATE TABLE t0 ..."}},"expected":null}
//! {"op_id":1,"worker":0,"kind":{"Sql":{"statement":"INSERT INTO t0 ..."}},"expected":null}
//! ```

use serde::{Deserialize, Serialize};

// ── Header ──────────────────────────────────────────────────────────────

/// Metadata header for an OpLog — always the first JSONL line.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpLogHeader {
    /// Identifier linking this log to a golden fixture (copied into `working/` per run).
    pub fixture_id: String,
    /// Master seed used to derive all per-worker RNG streams.
    pub seed: u64,
    /// RNG algorithm and crate version for reproducibility.
    pub rng: RngSpec,
    /// Concurrency model governing how workers execute operations.
    pub concurrency: ConcurrencyModel,
    /// Human-readable preset name, if this log was generated from a preset.
    pub preset: Option<String>,
}

/// RNG algorithm and version tag for exact reproducibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RngSpec {
    /// Algorithm name (e.g. `"ChaCha12"`, `"StdRng/ChaCha12"`).
    pub algorithm: String,
    /// Crate + version string (e.g. `"rand 0.8"`).
    pub version: String,
}

impl Default for RngSpec {
    fn default() -> Self {
        Self {
            algorithm: "StdRng/ChaCha12".to_owned(),
            version: "rand 0.8".to_owned(),
        }
    }
}

/// Concurrency model that governs how workers interact with the database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConcurrencyModel {
    /// Number of concurrent workers (1 = serial).
    pub worker_count: u16,
    /// Number of operations per transaction before committing.
    pub transaction_size: u32,
    /// Policy for ordering commits across workers.
    ///
    /// - `"deterministic"` — workers commit in round-robin order by `op_id`.
    /// - `"free"` — workers commit as soon as their transaction is full.
    /// - `"barrier"` — all workers synchronize after each transaction batch.
    pub commit_order_policy: String,
}

impl Default for ConcurrencyModel {
    fn default() -> Self {
        Self {
            worker_count: 1,
            transaction_size: 50,
            commit_order_policy: "deterministic".to_owned(),
        }
    }
}

// ── Operation records ───────────────────────────────────────────────────

/// A single operation within the log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpRecord {
    /// Monotonically increasing, deterministic identifier.
    pub op_id: u64,
    /// Worker index that should execute this operation (0-based).
    pub worker: u16,
    /// The operation payload.
    pub kind: OpKind,
    /// Optional expected result shape for verification.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected: Option<ExpectedResult>,
}

/// The payload of an operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OpKind {
    /// A raw SQL statement to execute.
    Sql {
        /// The SQL text.
        statement: String,
    },
    /// A structured insert operation (avoids SQL injection concerns in
    /// generated workloads).
    Insert {
        /// Target table name.
        table: String,
        /// Row key (rowid or INTEGER PRIMARY KEY value).
        key: i64,
        /// Column name → value pairs (values are JSON-compatible strings).
        values: Vec<(String, String)>,
    },
    /// A structured update operation.
    Update {
        /// Target table name.
        table: String,
        /// Row key to update.
        key: i64,
        /// Column name → new value pairs.
        values: Vec<(String, String)>,
    },
    /// Begin a new transaction.
    Begin,
    /// Commit the current transaction.
    Commit,
    /// Rollback the current transaction.
    Rollback,
}

/// Optional expected result attached to an operation for verification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExpectedResult {
    /// The statement should succeed with the given number of affected rows.
    AffectedRows(usize),
    /// The statement should return exactly this many rows.
    RowCount(usize),
    /// The statement should fail (any error is acceptable).
    Error,
}

// ── Full OpLog (in-memory representation) ───────────────────────────────

/// A complete operation log: header + ordered records.
///
/// This is the in-memory representation.  For JSONL serialization, write the
/// header as the first line followed by one record per line.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpLog {
    /// Log metadata.
    pub header: OpLogHeader,
    /// Ordered sequence of operations.
    pub records: Vec<OpRecord>,
}

impl OpLog {
    /// Serialize the log to JSONL (one JSON object per line).
    ///
    /// # Errors
    ///
    /// Returns a `serde_json::Error` if serialization fails.
    pub fn to_jsonl(&self) -> Result<String, serde_json::Error> {
        let mut out = serde_json::to_string(&self.header)?;
        out.push('\n');
        for rec in &self.records {
            out.push_str(&serde_json::to_string(rec)?);
            out.push('\n');
        }
        Ok(out)
    }

    /// Deserialize an `OpLog` from JSONL text.
    ///
    /// # Errors
    ///
    /// Returns a `serde_json::Error` if any line is malformed.
    pub fn from_jsonl(text: &str) -> Result<Self, serde_json::Error> {
        let mut lines = text.lines().filter(|l| !l.trim().is_empty());
        let header_line = lines.next().unwrap_or("{}");
        let header: OpLogHeader = serde_json::from_str(header_line)?;
        let mut records = Vec::new();
        for line in lines {
            records.push(serde_json::from_str(line)?);
        }
        Ok(Self { header, records })
    }
}

// ── Presets ──────────────────────────────────────────────────────────────

/// Generate the **commutative inserts (disjoint keys)** preset.
///
/// Each of `worker_count` workers inserts into its own non-overlapping key
/// range, ensuring zero write conflicts.  Final row count and content are
/// independent of execution order.
#[must_use]
pub fn preset_commutative_inserts_disjoint_keys(
    fixture_id: &str,
    seed: u64,
    worker_count: u16,
    rows_per_worker: u32,
) -> OpLog {
    // Keep transactions short so heavily-contended executors (like stock SQLite)
    // don't spend a long time holding the single-writer lock.
    let transaction_size = rows_per_worker.clamp(1, 5);

    let header = OpLogHeader {
        fixture_id: fixture_id.to_owned(),
        seed,
        rng: RngSpec::default(),
        concurrency: ConcurrencyModel {
            worker_count,
            transaction_size,
            commit_order_policy: "free".to_owned(),
        },
        preset: Some("commutative_inserts_disjoint_keys".to_owned()),
    };

    let mut records = Vec::new();
    let mut op_id: u64 = 0;

    // Schema setup (worker 0).
    records.push(OpRecord {
        op_id,
        worker: 0,
        kind: OpKind::Sql {
            statement: "CREATE TABLE IF NOT EXISTS t0 (id INTEGER PRIMARY KEY, val TEXT, num REAL)"
                .to_owned(),
        },
        expected: None,
    });
    op_id += 1;

    // Each worker inserts into a disjoint key range.
    for w in 0..worker_count {
        let base_key = i64::from(w) * i64::from(rows_per_worker);
        for chunk_start in (0..rows_per_worker).step_by(transaction_size as usize) {
            let chunk_end = (chunk_start + transaction_size).min(rows_per_worker);

            records.push(OpRecord {
                op_id,
                worker: w,
                kind: OpKind::Begin,
                expected: None,
            });
            op_id += 1;

            for r in chunk_start..chunk_end {
                let key = base_key + i64::from(r);
                records.push(OpRecord {
                    op_id,
                    worker: w,
                    kind: OpKind::Insert {
                        table: "t0".to_owned(),
                        key,
                        values: vec![
                            ("val".to_owned(), format!("w{w}_r{r}")),
                            ("num".to_owned(), format!("{}", f64::from(r) * 1.1)),
                        ],
                    },
                    expected: Some(ExpectedResult::AffectedRows(1)),
                });
                op_id += 1;
            }

            records.push(OpRecord {
                op_id,
                worker: w,
                kind: OpKind::Commit,
                expected: None,
            });
            op_id += 1;
        }
    }

    // Final verification query.
    let expected_total = u64::from(worker_count) * u64::from(rows_per_worker);
    records.push(OpRecord {
        op_id,
        worker: 0,
        kind: OpKind::Sql {
            statement: "SELECT COUNT(*) FROM t0".to_owned(),
        },
        expected: Some(ExpectedResult::RowCount(1)),
    });
    let _ = expected_total; // used by executor, not stored here

    OpLog { header, records }
}

/// Generate the **hot-page contention** preset.
///
/// All workers repeatedly update the *same* small set of rows, forcing lock
/// contention and retry logic.  This stress-tests MVCC conflict detection and
/// the SSI retry path.
#[must_use]
pub fn preset_hot_page_contention(
    fixture_id: &str,
    seed: u64,
    worker_count: u16,
    rounds: u32,
) -> OpLog {
    let hot_rows: u32 = 10; // all workers compete for these 10 keys

    let header = OpLogHeader {
        fixture_id: fixture_id.to_owned(),
        seed,
        rng: RngSpec::default(),
        concurrency: ConcurrencyModel {
            worker_count,
            transaction_size: hot_rows,
            commit_order_policy: "deterministic".to_owned(),
        },
        preset: Some("hot_page_contention".to_owned()),
    };

    let mut records = Vec::new();
    let mut op_id: u64 = 0;

    // Schema + seed data (all workers). We use `INSERT OR IGNORE` so each worker
    // can safely seed without depending on a specific start order.
    for w in 0..worker_count {
        records.push(OpRecord {
            op_id,
            worker: w,
            kind: OpKind::Sql {
                statement: "CREATE TABLE IF NOT EXISTS hot (id INTEGER PRIMARY KEY, counter INTEGER DEFAULT 0)"
                    .to_owned(),
            },
            expected: None,
        });
        op_id += 1;

        for k in 0..hot_rows {
            records.push(OpRecord {
                op_id,
                worker: w,
                kind: OpKind::Sql {
                    statement: format!("INSERT OR IGNORE INTO hot (id, counter) VALUES ({k}, 0)"),
                },
                expected: None,
            });
            op_id += 1;
        }
    }

    // Contention rounds: each worker updates every hot row once per round.
    for round in 0..rounds {
        for w in 0..worker_count {
            records.push(OpRecord {
                op_id,
                worker: w,
                kind: OpKind::Begin,
                expected: None,
            });
            op_id += 1;

            for k in 0..hot_rows {
                records.push(OpRecord {
                    op_id,
                    worker: w,
                    kind: OpKind::Update {
                        table: "hot".to_owned(),
                        key: i64::from(k),
                        values: vec![(
                            "counter".to_owned(),
                            format!(
                                "{}",
                                u64::from(round) * u64::from(worker_count) + u64::from(w)
                            ),
                        )],
                    },
                    expected: Some(ExpectedResult::AffectedRows(1)),
                });
                op_id += 1;
            }

            records.push(OpRecord {
                op_id,
                worker: w,
                kind: OpKind::Commit,
                expected: None,
            });
            op_id += 1;
        }
    }

    OpLog { header, records }
}

/// Generate the **mixed read-write** preset.
///
/// Workers alternate between reads and writes on the same table, exercising
/// MVCC snapshot isolation under concurrent mixed workloads.
#[must_use]
pub fn preset_mixed_read_write(
    fixture_id: &str,
    seed: u64,
    worker_count: u16,
    ops_per_worker: u32,
) -> OpLog {
    let header = OpLogHeader {
        fixture_id: fixture_id.to_owned(),
        seed,
        rng: RngSpec::default(),
        concurrency: ConcurrencyModel {
            worker_count,
            transaction_size: ops_per_worker,
            commit_order_policy: "barrier".to_owned(),
        },
        preset: Some("mixed_read_write".to_owned()),
    };

    let mut records = Vec::new();
    let mut op_id: u64 = 0;

    // Schema + seed (all workers). Like other presets, we avoid depending on
    // executors to serialize worker 0's setup operations.
    for w in 0..worker_count {
        records.push(OpRecord {
            op_id,
            worker: w,
            kind: OpKind::Sql {
                statement: "CREATE TABLE IF NOT EXISTS mixed (id INTEGER PRIMARY KEY, val TEXT)"
                    .to_owned(),
            },
            expected: None,
        });
        op_id += 1;

        for k in 0..100 {
            records.push(OpRecord {
                op_id,
                worker: w,
                kind: OpKind::Sql {
                    statement: format!(
                        "INSERT OR IGNORE INTO mixed (id, val) VALUES ({k}, 'init_{k}')"
                    ),
                },
                expected: None,
            });
            op_id += 1;
        }
    }

    // Mixed operations: even op_ids read, odd op_ids write.
    for w in 0..worker_count {
        records.push(OpRecord {
            op_id,
            worker: w,
            kind: OpKind::Begin,
            expected: None,
        });
        op_id += 1;

        for i in 0..ops_per_worker {
            if i % 2 == 0 {
                // Read
                records.push(OpRecord {
                    op_id,
                    worker: w,
                    kind: OpKind::Sql {
                        statement: format!(
                            "SELECT val FROM mixed WHERE id = {}",
                            i64::from(i) % 100
                        ),
                    },
                    expected: Some(ExpectedResult::RowCount(1)),
                });
            } else {
                // Write
                let key = 100 + i64::from(w) * i64::from(ops_per_worker) + i64::from(i);
                records.push(OpRecord {
                    op_id,
                    worker: w,
                    kind: OpKind::Insert {
                        table: "mixed".to_owned(),
                        key,
                        values: vec![("val".to_owned(), format!("w{w}_i{i}"))],
                    },
                    expected: Some(ExpectedResult::AffectedRows(1)),
                });
            }
            op_id += 1;
        }

        records.push(OpRecord {
            op_id,
            worker: w,
            kind: OpKind::Commit,
            expected: None,
        });
        op_id += 1;
    }

    OpLog { header, records }
}

/// Generate the **deterministic transform** preset (sha256 proof).
///
/// Creates three tables in the `_fsqlite_e2e_` namespace, populates them with
/// deterministic data, then performs a fixed sequence of updates and deletes.
/// Indexes are created to exercise B-tree maintenance during mutations.
///
/// Designed for serial execution (`worker_count=1`) so that both engines
/// produce identical canonical SHA-256 outputs when the same seed is used.
///
/// # Tables
///
/// - `_fsqlite_e2e_kv (id, key, val, ver)` — key-value store
/// - `_fsqlite_e2e_events (id, ts, kind, payload)` — event log
/// - `_fsqlite_e2e_blob (id, data, checksum)` — blob-like text store
///
/// # Phases
///
/// 1. **Schema**: CREATE TABLE (indexes omitted in early phases)
/// 2. **Populate**: Insert `rows_per_table` rows per table
/// 3. **Transform**: Update ~33% of kv rows, delete ~10%, log events
/// 4. **Verify**: SELECT COUNT(*) from each table
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn preset_deterministic_transform(fixture_id: &str, seed: u64, rows_per_table: u32) -> OpLog {
    let header = OpLogHeader {
        fixture_id: fixture_id.to_owned(),
        seed,
        rng: RngSpec::default(),
        concurrency: ConcurrencyModel {
            worker_count: 1,
            transaction_size: rows_per_table.clamp(1, 50),
            commit_order_policy: "deterministic".to_owned(),
        },
        preset: Some("deterministic_transform".to_owned()),
    };

    let mut records = Vec::new();
    let mut op_id: u64 = 0;

    // ── Phase 1: Schema ───────────────────────────────────────────────

    let schema_stmts = [
        "CREATE TABLE IF NOT EXISTS _fsqlite_e2e_kv (\
            id INTEGER PRIMARY KEY, \
            key TEXT NOT NULL, \
            val TEXT, \
            ver INTEGER DEFAULT 0)",
        "CREATE TABLE IF NOT EXISTS _fsqlite_e2e_events (\
            id INTEGER PRIMARY KEY, \
            ts INTEGER NOT NULL, \
            kind TEXT NOT NULL, \
            payload TEXT)",
        "CREATE TABLE IF NOT EXISTS _fsqlite_e2e_blob (\
            id INTEGER PRIMARY KEY, \
            data TEXT NOT NULL, \
            checksum TEXT NOT NULL)",
    ];

    for stmt in &schema_stmts {
        records.push(OpRecord {
            op_id,
            worker: 0,
            kind: OpKind::Sql {
                statement: (*stmt).to_owned(),
            },
            expected: None,
        });
        op_id += 1;
    }

    // ── Phase 2: Populate ─────────────────────────────────────────────
    //
    // Data is deterministic: derived purely from seed + row index.

    records.push(OpRecord {
        op_id,
        worker: 0,
        kind: OpKind::Begin,
        expected: None,
    });
    op_id += 1;

    // Deterministic string generator: simple mixing of seed and index.
    let det_str = |prefix: &str, s: u64, i: u32| -> String {
        let mixed = s
            .wrapping_mul(0x517c_c1b7_2722_0a95)
            .wrapping_add(u64::from(i));
        format!("{prefix}_{mixed:016x}")
    };

    let event_kinds = ["insert", "update", "delete", "read"];

    for i in 0..rows_per_table {
        let key = i64::from(i);
        // KV table
        records.push(OpRecord {
            op_id,
            worker: 0,
            kind: OpKind::Insert {
                table: "_fsqlite_e2e_kv".to_owned(),
                key,
                values: vec![
                    ("key".to_owned(), format!("k_{i}")),
                    ("val".to_owned(), det_str("v", seed, i)),
                    ("ver".to_owned(), "0".to_owned()),
                ],
            },
            expected: Some(ExpectedResult::AffectedRows(1)),
        });
        op_id += 1;

        // Events table
        records.push(OpRecord {
            op_id,
            worker: 0,
            kind: OpKind::Insert {
                table: "_fsqlite_e2e_events".to_owned(),
                key,
                values: vec![
                    ("ts".to_owned(), format!("{}", i.saturating_mul(1000))),
                    (
                        "kind".to_owned(),
                        event_kinds[i as usize % event_kinds.len()].to_owned(),
                    ),
                    ("payload".to_owned(), det_str("evt", seed, i)),
                ],
            },
            expected: Some(ExpectedResult::AffectedRows(1)),
        });
        op_id += 1;

        // Blob table
        records.push(OpRecord {
            op_id,
            worker: 0,
            kind: OpKind::Insert {
                table: "_fsqlite_e2e_blob".to_owned(),
                key,
                values: vec![
                    ("data".to_owned(), det_str("blob", seed, i)),
                    ("checksum".to_owned(), det_str("ck", seed, i)),
                ],
            },
            expected: Some(ExpectedResult::AffectedRows(1)),
        });
        op_id += 1;
    }

    records.push(OpRecord {
        op_id,
        worker: 0,
        kind: OpKind::Commit,
        expected: None,
    });
    op_id += 1;

    // ── Phase 3: Transform ────────────────────────────────────────────
    //
    // - Update kv rows where id % 3 == 0  (increment ver, change val)
    // - Delete kv rows where id % 10 == 0 (remove every 10th)
    // - Insert an event for each mutation

    records.push(OpRecord {
        op_id,
        worker: 0,
        kind: OpKind::Begin,
        expected: None,
    });
    op_id += 1;

    let mut event_id = i64::from(rows_per_table); // continue event IDs after populate

    for i in 0..rows_per_table {
        let key = i64::from(i);

        if i % 10 == 0 {
            // Delete every 10th row from kv
            records.push(OpRecord {
                op_id,
                worker: 0,
                kind: OpKind::Sql {
                    statement: format!("DELETE FROM _fsqlite_e2e_kv WHERE id = {key}"),
                },
                expected: Some(ExpectedResult::AffectedRows(1)),
            });
            op_id += 1;

            // Log the delete event
            records.push(OpRecord {
                op_id,
                worker: 0,
                kind: OpKind::Insert {
                    table: "_fsqlite_e2e_events".to_owned(),
                    key: event_id,
                    values: vec![
                        ("ts".to_owned(), format!("{}", rows_per_table + i)),
                        ("kind".to_owned(), "delete".to_owned()),
                        ("payload".to_owned(), format!("deleted_k_{i}")),
                    ],
                },
                expected: Some(ExpectedResult::AffectedRows(1)),
            });
            op_id += 1;
            event_id += 1;
        } else if i % 3 == 0 {
            // Update every 3rd (non-deleted) row: increment ver, change val
            records.push(OpRecord {
                op_id,
                worker: 0,
                kind: OpKind::Update {
                    table: "_fsqlite_e2e_kv".to_owned(),
                    key,
                    values: vec![
                        ("val".to_owned(), det_str("upd", seed, i)),
                        ("ver".to_owned(), "1".to_owned()),
                    ],
                },
                expected: Some(ExpectedResult::AffectedRows(1)),
            });
            op_id += 1;

            // Log the update event
            records.push(OpRecord {
                op_id,
                worker: 0,
                kind: OpKind::Insert {
                    table: "_fsqlite_e2e_events".to_owned(),
                    key: event_id,
                    values: vec![
                        ("ts".to_owned(), format!("{}", rows_per_table + i)),
                        ("kind".to_owned(), "update".to_owned()),
                        ("payload".to_owned(), format!("updated_k_{i}")),
                    ],
                },
                expected: Some(ExpectedResult::AffectedRows(1)),
            });
            op_id += 1;
            event_id += 1;
        }
    }

    records.push(OpRecord {
        op_id,
        worker: 0,
        kind: OpKind::Commit,
        expected: None,
    });
    op_id += 1;

    // ── Phase 4: Verify ───────────────────────────────────────────────

    for table in &[
        "_fsqlite_e2e_kv",
        "_fsqlite_e2e_events",
        "_fsqlite_e2e_blob",
    ] {
        records.push(OpRecord {
            op_id,
            worker: 0,
            kind: OpKind::Sql {
                statement: format!("SELECT COUNT(*) FROM {table}"),
            },
            expected: Some(ExpectedResult::RowCount(1)),
        });
        op_id += 1;
    }

    OpLog { header, records }
}

// ── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oplog_jsonl_roundtrip() {
        let log = preset_commutative_inserts_disjoint_keys("test-fixture", 42, 2, 5);
        let jsonl = log.to_jsonl().unwrap();
        let parsed = OpLog::from_jsonl(&jsonl).unwrap();

        assert_eq!(parsed.header.fixture_id, "test-fixture");
        assert_eq!(parsed.header.seed, 42);
        assert_eq!(parsed.header.concurrency.worker_count, 2);
        assert_eq!(parsed.records.len(), log.records.len());

        // Verify op_ids are monotonically increasing.
        for (i, rec) in parsed.records.iter().enumerate() {
            if i > 0 {
                assert!(
                    rec.op_id > parsed.records[i - 1].op_id,
                    "op_id must be monotonically increasing"
                );
            }
        }
    }

    #[test]
    fn test_preset_disjoint_keys_structure() {
        let log = preset_commutative_inserts_disjoint_keys("fix-1", 99, 4, 10);

        assert_eq!(
            log.header.preset.as_deref(),
            Some("commutative_inserts_disjoint_keys")
        );
        assert_eq!(log.header.concurrency.worker_count, 4);
        assert_eq!(log.header.concurrency.commit_order_policy, "free");

        assert_eq!(log.header.concurrency.transaction_size, 5);

        // 1 CREATE + 4 workers × (2 × (1 BEGIN + 5 INSERTs + 1 COMMIT)) + 1 SELECT = 58
        assert_eq!(log.records.len(), 58);

        // Verify disjoint key ranges: worker 0 = [0..10), worker 1 = [10..20), etc.
        let insert_keys: Vec<(u16, i64)> = log
            .records
            .iter()
            .filter_map(|r| match &r.kind {
                OpKind::Insert { key, .. } => Some((r.worker, *key)),
                _ => None,
            })
            .collect();
        assert_eq!(insert_keys.len(), 40); // 4 workers × 10 rows

        // No duplicate keys.
        let mut all_keys: Vec<i64> = insert_keys.iter().map(|(_, k)| *k).collect();
        all_keys.sort_unstable();
        all_keys.dedup();
        assert_eq!(all_keys.len(), 40, "all keys must be unique");
    }

    #[test]
    fn test_preset_hot_page_contention_structure() {
        let log = preset_hot_page_contention("fix-2", 7, 3, 2);

        assert_eq!(log.header.preset.as_deref(), Some("hot_page_contention"));
        assert_eq!(log.header.concurrency.commit_order_policy, "deterministic");

        // 3 workers × (1 CREATE + 10 seed INSERT OR IGNORE) + 2 rounds × 3 workers × (1 BEGIN + 10 UPDATEs + 1 COMMIT)
        // = 3 × 11 + 2 × 3 × 12 = 105
        assert_eq!(log.records.len(), 105);

        // All updates target keys 0..10.
        let update_keys: Vec<i64> = log
            .records
            .iter()
            .filter_map(|r| match &r.kind {
                OpKind::Update { key, .. } => Some(*key),
                _ => None,
            })
            .collect();
        assert!(update_keys.iter().all(|&k| k < 10));
        // 2 rounds × 3 workers × 10 keys = 60 updates
        assert_eq!(update_keys.len(), 60);
    }

    #[test]
    fn test_preset_mixed_read_write_structure() {
        let log = preset_mixed_read_write("fix-3", 0, 2, 10);

        assert_eq!(log.header.preset.as_deref(), Some("mixed_read_write"));
        assert_eq!(log.header.concurrency.commit_order_policy, "barrier");

        // Check we have both reads (Sql) and writes (Insert) in the mixed section.
        assert!(
            log.records
                .iter()
                .any(|r| matches!(&r.kind, OpKind::Sql { statement } if statement.starts_with("SELECT val"))),
            "should have read operations"
        );

        assert!(
            log.records
                .iter()
                .any(|r| { matches!(&r.kind, OpKind::Insert { table, .. } if table == "mixed") }),
            "should have write operations"
        );

        let first_begin = log
            .records
            .iter()
            .position(|r| matches!(r.kind, OpKind::Begin))
            .expect("mixed preset must include a BEGIN");
        assert!(
            log.records[first_begin..]
                .iter()
                .any(|r| { matches!(&r.kind, OpKind::Insert { table, .. } if table == "mixed") }),
            "should have write operations after the mixed section begins"
        );
    }

    #[test]
    fn test_rng_spec_default() {
        let rng = RngSpec::default();
        assert_eq!(rng.algorithm, "StdRng/ChaCha12");
        assert_eq!(rng.version, "rand 0.8");
    }

    #[test]
    fn test_oplog_empty_records() {
        let log = OpLog {
            header: OpLogHeader {
                fixture_id: "empty".to_owned(),
                seed: 0,
                rng: RngSpec::default(),
                concurrency: ConcurrencyModel::default(),
                preset: None,
            },
            records: Vec::new(),
        };
        let jsonl = log.to_jsonl().unwrap();
        let parsed = OpLog::from_jsonl(&jsonl).unwrap();
        assert!(parsed.records.is_empty());
        assert_eq!(parsed.header.fixture_id, "empty");
    }

    #[test]
    fn test_op_kind_serde_variants() {
        // Test each OpKind variant roundtrips correctly.
        let ops = vec![
            OpKind::Sql {
                statement: "SELECT 1".to_owned(),
            },
            OpKind::Insert {
                table: "t".to_owned(),
                key: 42,
                values: vec![("col".to_owned(), "val".to_owned())],
            },
            OpKind::Update {
                table: "t".to_owned(),
                key: 1,
                values: vec![("col".to_owned(), "new".to_owned())],
            },
            OpKind::Begin,
            OpKind::Commit,
            OpKind::Rollback,
        ];

        for op in ops {
            let json = serde_json::to_string(&op).unwrap();
            let parsed: OpKind = serde_json::from_str(&json).unwrap();
            assert_eq!(
                serde_json::to_string(&parsed).unwrap(),
                json,
                "roundtrip failed for {json}"
            );
        }
    }

    #[test]
    fn test_preset_deterministic_transform_structure() {
        let log = preset_deterministic_transform("fix-dt", 42, 30);

        assert_eq!(
            log.header.preset.as_deref(),
            Some("deterministic_transform")
        );
        assert_eq!(log.header.concurrency.worker_count, 1);
        assert_eq!(log.header.concurrency.commit_order_policy, "deterministic");

        // Verify schema: 3 DDL statements (3 CREATE TABLE).
        let ddl_count = log
            .records
            .iter()
            .filter(|r| {
                matches!(&r.kind, OpKind::Sql { statement }
                    if statement.starts_with("CREATE"))
            })
            .count();
        assert_eq!(ddl_count, 3, "expected 3 tables");

        // Verify all three tables have inserts.
        for table in &[
            "_fsqlite_e2e_kv",
            "_fsqlite_e2e_events",
            "_fsqlite_e2e_blob",
        ] {
            let count = log
                .records
                .iter()
                .filter(|r| matches!(&r.kind, OpKind::Insert { table: t, .. } if t == *table))
                .count();
            assert!(count > 0, "expected inserts into {table}, got 0");
        }

        // Verify we have updates (from transform phase).
        let update_count = log
            .records
            .iter()
            .filter(|r| matches!(&r.kind, OpKind::Update { .. }))
            .count();
        assert!(update_count > 0, "expected updates in transform phase");

        // Verify we have deletes (from transform phase).
        let delete_count = log
            .records
            .iter()
            .filter(|r| {
                matches!(&r.kind, OpKind::Sql { statement }
                    if statement.starts_with("DELETE"))
            })
            .count();
        assert!(delete_count > 0, "expected deletes in transform phase");

        // Verify 3 verification queries at the end.
        let verify_count = log
            .records
            .iter()
            .filter(|r| {
                matches!(&r.kind, OpKind::Sql { statement }
                    if statement.starts_with("SELECT COUNT(*)"))
            })
            .count();
        assert_eq!(verify_count, 3, "expected 3 verification queries");
    }

    #[test]
    fn test_preset_deterministic_transform_seed_stability() {
        // Same seed → same JSONL output.
        let a = preset_deterministic_transform("fix", 99, 20);
        let b = preset_deterministic_transform("fix", 99, 20);

        let jsonl_a = a.to_jsonl().unwrap();
        let jsonl_b = b.to_jsonl().unwrap();
        assert_eq!(
            jsonl_a, jsonl_b,
            "identical seeds must produce identical JSONL"
        );
    }

    #[test]
    fn test_preset_deterministic_transform_different_seeds_differ() {
        let a = preset_deterministic_transform("fix", 1, 20);
        let b = preset_deterministic_transform("fix", 2, 20);

        let jsonl_a = a.to_jsonl().unwrap();
        let jsonl_b = b.to_jsonl().unwrap();
        assert_ne!(
            jsonl_a, jsonl_b,
            "different seeds must produce different JSONL"
        );
    }

    #[test]
    fn test_preset_deterministic_transform_jsonl_roundtrip() {
        let log = preset_deterministic_transform("rt-test", 42, 50);
        let jsonl = log.to_jsonl().unwrap();
        let parsed = OpLog::from_jsonl(&jsonl).unwrap();

        assert_eq!(parsed.records.len(), log.records.len());
        assert_eq!(parsed.header.fixture_id, "rt-test");
        assert_eq!(parsed.header.seed, 42);

        // Op IDs must be monotonically increasing.
        for (i, rec) in parsed.records.iter().enumerate() {
            if i > 0 {
                assert!(
                    rec.op_id > parsed.records[i - 1].op_id,
                    "op_id must increase: {} vs {}",
                    parsed.records[i - 1].op_id,
                    rec.op_id
                );
            }
        }
    }

    #[test]
    fn test_preset_deterministic_transform_op_counts() {
        let rows = 30_u32;
        let log = preset_deterministic_transform("counts", 7, rows);

        // Populate phase: 3 inserts per row (kv + events + blob).
        let populate_inserts = 3 * rows;

        // Transform: rows where i%10==0 get deleted (3 out of 30: i=0,10,20)
        let deletes = (0..rows).filter(|i| i % 10 == 0).count();
        // rows where i%3==0 AND i%10!=0 get updated
        let updates = (0..rows).filter(|i| i % 3 == 0 && i % 10 != 0).count();
        // Each delete/update also inserts an event
        let transform_events = deletes + updates;

        let total_inserts = log
            .records
            .iter()
            .filter(|r| matches!(&r.kind, OpKind::Insert { .. }))
            .count();
        assert_eq!(
            total_inserts,
            (populate_inserts as usize) + transform_events,
            "total inserts = populate + transform events"
        );

        let total_updates = log
            .records
            .iter()
            .filter(|r| matches!(&r.kind, OpKind::Update { .. }))
            .count();
        assert_eq!(total_updates, updates, "update count");

        let total_deletes = log
            .records
            .iter()
            .filter(|r| {
                matches!(&r.kind, OpKind::Sql { statement }
                    if statement.starts_with("DELETE"))
            })
            .count();
        assert_eq!(total_deletes, deletes, "delete count");
    }
}
