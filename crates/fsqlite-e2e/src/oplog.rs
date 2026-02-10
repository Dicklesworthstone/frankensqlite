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
    let header = OpLogHeader {
        fixture_id: fixture_id.to_owned(),
        seed,
        rng: RngSpec::default(),
        concurrency: ConcurrencyModel {
            worker_count,
            transaction_size: rows_per_worker,
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
        records.push(OpRecord {
            op_id,
            worker: w,
            kind: OpKind::Begin,
            expected: None,
        });
        op_id += 1;

        for r in 0..rows_per_worker {
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

    // Schema + seed data (worker 0).
    records.push(OpRecord {
        op_id,
        worker: 0,
        kind: OpKind::Sql {
            statement:
                "CREATE TABLE IF NOT EXISTS hot (id INTEGER PRIMARY KEY, counter INTEGER DEFAULT 0)"
                    .to_owned(),
        },
        expected: None,
    });
    op_id += 1;

    for k in 0..hot_rows {
        records.push(OpRecord {
            op_id,
            worker: 0,
            kind: OpKind::Insert {
                table: "hot".to_owned(),
                key: i64::from(k),
                values: vec![("counter".to_owned(), "0".to_owned())],
            },
            expected: Some(ExpectedResult::AffectedRows(1)),
        });
        op_id += 1;
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

    // Schema (worker 0).
    records.push(OpRecord {
        op_id,
        worker: 0,
        kind: OpKind::Sql {
            statement: "CREATE TABLE IF NOT EXISTS mixed (id INTEGER PRIMARY KEY, val TEXT)"
                .to_owned(),
        },
        expected: None,
    });
    op_id += 1;

    // Seed some initial data.
    for k in 0..100 {
        records.push(OpRecord {
            op_id,
            worker: 0,
            kind: OpKind::Insert {
                table: "mixed".to_owned(),
                key: k,
                values: vec![("val".to_owned(), format!("init_{k}"))],
            },
            expected: Some(ExpectedResult::AffectedRows(1)),
        });
        op_id += 1;
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

        // 1 CREATE + 4 workers × (1 BEGIN + 10 INSERTs + 1 COMMIT) + 1 SELECT = 50
        assert_eq!(log.records.len(), 50);

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

        // 1 CREATE + 10 seed INSERTs + 2 rounds × 3 workers × (1 BEGIN + 10 UPDATEs + 1 COMMIT)
        // = 1 + 10 + 2 × 3 × 12 = 83
        assert_eq!(log.records.len(), 83);

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
            log.records.iter().any(|r| {
                matches!(&r.kind, OpKind::Insert { table, .. } if table == "mixed") && r.op_id > 101 // past initial seed data
            }),
            "should have write operations"
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
}
