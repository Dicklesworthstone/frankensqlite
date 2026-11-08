//! `ColumnSubstrPrefix` on TEXT cells that fit the prefix: the generic path
//! (`cursor_column` + scalar `substr`, three copies of the cell per row)
//! against the whole-value path (`Column`'s shared decode, one copy, kept when
//! the cell is already its own prefix), in one binary, through identical
//! queries on one connection.
//!
//! Workload: cass's lexical-rebuild message read. A `messages` table with
//! cass's schema, seeded with its per-conversation message counts and content
//! sizes, read one conversation at a time with cass's exact projection
//! (`substr(content, 1, 8 MiB)`, `octet_length(content)`, ordered by `idx`
//! through the `(conversation_id, idx)` autoindex), plus a rowid-order scan of
//! the same projection.
//!
//! Arms (switched by `COLUMN_SUBSTR_PREFIX_WHOLE_VALUE_DISABLED_FOR_BENCH`):
//! - `generic`: whole-value path off (the incumbent).
//! - `whole`: whole-value path on (the candidate).
//! - `generic_null`: the incumbent again, for the A/A null ratio.
//!
//! Before anything is timed, every arm must return the same SHA-256 over every
//! row's typed values in order, for the timed cells and for an edge sweep of
//! `substr(content, 1, N)` over N in {0, 1, 7, 64, 300, 2000} that crosses
//! every `ColumnSubstrPrefix` branch (fits, needs truncation, embedded NUL,
//! multi-byte text, window path). Each round runs the three arms in rotated
//! order and reports per-arm p50 [min, max], the paired median ratio
//! `whole / generic` (`< 1.0` is faster) and the null `generic_null / generic`,
//! and each arm's peak RSS growth.
//!
//! ```bash
//! rch exec -- env SUBSTR_AB_CONVERSATIONS=200 SUBSTR_AB_ROUNDS=11 \
//!   cargo bench -p fsqlite-core --features bench-internals --profile release \
//!     --bench column_substr_prefix_whole_value_ab
//! ```

use std::future::Future;
use std::hint::black_box;
use std::sync::atomic::Ordering;
use std::time::Instant;

use asupersync::runtime::{Runtime, RuntimeBuilder};
use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::SqliteValue;
use fsqlite_vdbe::engine::COLUMN_SUBSTR_PREFIX_WHOLE_VALUE_DISABLED_FOR_BENCH as WHOLE_VALUE_OFF;
use sha2::{Digest, Sha256};

/// cass's default `CASS_LEXICAL_MAX_MESSAGE_CONTENT_BYTES` (8 MiB).
const CASS_CAP: usize = 8 * 1024 * 1024;
const EDGE_PREFIXES: [usize; 6] = [0, 1, 7, 64, 300, 2000];

/// cass archive shape: (weight %, min, max) messages per conversation.
const MESSAGES_PER_CONVERSATION: [(u64, u64, u64); 5] = [
    (14, 1, 10),
    (31, 11, 100),
    (41, 101, 1000),
    (13, 1001, 10000),
    (1, 10001, 30000),
];
/// cass archive shape: (weight %, min, max) content bytes per message.
const CONTENT_BYTES: [(u64, u64, u64); 4] = [
    (45, 16, 300),
    (35, 301, 2000),
    (17, 2001, 8000),
    (3, 8001, 40000),
];
const WORDS: [&str; 12] = [
    "the",
    "cargo",
    "build",
    "failed",
    "with",
    "error",
    "in",
    "src/lib.rs",
    "fn",
    "let",
    "assistant",
    "message",
];
const WIDE_WORDS: [&str; 6] = [
    "caf\u{e9}",
    "\u{20ac}100",
    "\u{1d518}",
    "na\u{ef}ve",
    "\u{4e2d}\u{6587}",
    "\u{2764}",
];

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Arm {
    Generic,
    Whole,
    GenericNull,
}

impl Arm {
    fn name(self) -> &'static str {
        match self {
            Self::Generic => "generic",
            Self::Whole => "whole",
            Self::GenericNull => "generic_null",
        }
    }

    fn select(self) {
        WHOLE_VALUE_OFF.store(self != Self::Whole, Ordering::Relaxed);
    }
}

const ARMS: [Arm; 3] = [Arm::Generic, Arm::Whole, Arm::GenericNull];

struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n.max(1)
    }

    fn bucketed(&mut self, buckets: &[(u64, u64, u64)]) -> u64 {
        let mut pick = self.below(buckets.iter().map(|(w, _, _)| w).sum());
        for &(weight, low, high) in buckets {
            if pick < weight {
                return low + self.below(high - low + 1);
            }
            pick -= weight;
        }
        buckets[0].1
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    thread_local! {
        static RUNTIME: Runtime = RuntimeBuilder::current_thread()
            .build()
            .expect("build benchmark runtime");
    }
    RUNTIME.with(|runtime| runtime.block_on(future))
}

/// One timed cell: reads every message once and returns the row count.
type CellRun<'a> = Box<dyn Fn(&Connection) -> usize + 'a>;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(default)
}

fn content(rng: &mut Rng, target: usize) -> String {
    let wide = rng.below(10) == 0;
    let mut text = String::with_capacity(target + 16);
    while text.len() < target {
        if wide && rng.below(4) == 0 {
            text.push_str(WIDE_WORDS[rng.below(WIDE_WORDS.len() as u64) as usize]);
        } else {
            text.push_str(WORDS[rng.below(WORDS.len() as u64) as usize]);
        }
        text.push(if rng.below(12) == 0 { '\n' } else { ' ' });
    }
    // A few cells carry an embedded NUL, where `substr` stops.
    if rng.below(200) == 0 {
        let at = text.len() / 2;
        let at = (0..=at)
            .rev()
            .find(|i| text.is_char_boundary(*i))
            .unwrap_or(0);
        text.insert(at, '\0');
    }
    text
}

/// Seed cass's `messages` shape; returns (conversation ids, message count, content bytes).
fn seed(conn: &Connection, conversations: usize, seed: u64) -> (Vec<i64>, usize, usize) {
    block_on(conn.execute(
        "CREATE TABLE messages (id INTEGER PRIMARY KEY, conversation_id INTEGER NOT NULL, \
         idx INTEGER NOT NULL, role TEXT NOT NULL, author TEXT, created_at INTEGER, \
         content TEXT NOT NULL, extra_json TEXT, extra_bin BLOB, \
         UNIQUE(conversation_id, idx));",
    ))
    .expect("create messages");
    let mut rng = Rng(seed);
    let counts: Vec<u64> = (0..conversations)
        .map(|_| rng.bucketed(&MESSAGES_PER_CONVERSATION))
        .collect();
    // cass appends most of a session at once and the tails later, interleaved.
    let mut order: Vec<(i64, u64)> = Vec::new();
    let mut tails: Vec<(i64, u64, u64)> = Vec::new();
    for (index, &count) in counts.iter().enumerate() {
        let conversation_id = i64::try_from(index + 1).unwrap();
        let head = count * 4 / 5;
        order.extend((0..head).map(|idx| (conversation_id, idx)));
        tails.push((conversation_id, head, count));
    }
    while tails.iter().any(|(_, next, end)| next < end) {
        for (conversation_id, next, end) in &mut tails {
            let stop = (*next + 16).min(*end);
            order.extend((*next..stop).map(|idx| (*conversation_id, idx)));
            *next = stop;
        }
    }
    let mut bytes = 0_usize;
    block_on(conn.execute("BEGIN;")).expect("begin");
    for (row, (conversation_id, idx)) in order.iter().enumerate() {
        let target = usize::try_from(rng.bucketed(&CONTENT_BYTES)).unwrap();
        let text = content(&mut rng, target);
        bytes += text.len();
        let role =
            ["user", "assistant", "tool", "system"][(row + usize::try_from(*idx).unwrap()) % 4];
        let author = if rng.below(3) == 0 {
            SqliteValue::Null
        } else {
            SqliteValue::Text("model-x".into())
        };
        let extra_bin = if rng.below(10) < 6 {
            let len = usize::try_from(rng.below(2048)).unwrap() + 8;
            SqliteValue::Blob(
                (0..len)
                    .map(|_| rng.next_u64() as u8)
                    .collect::<Vec<_>>()
                    .into(),
            )
        } else {
            SqliteValue::Null
        };
        block_on(conn.execute_with_params(
            "INSERT INTO messages (id, conversation_id, idx, role, author, created_at, content, \
             extra_json, extra_bin) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9);",
            &[
                SqliteValue::Integer(i64::try_from(row + 1).unwrap()),
                SqliteValue::Integer(*conversation_id),
                SqliteValue::Integer(i64::try_from(*idx).unwrap()),
                SqliteValue::Text(role.into()),
                author,
                SqliteValue::Integer(1_700_000_000_000 + i64::try_from(row).unwrap()),
                SqliteValue::Text(text.into()),
                SqliteValue::Text("{}".into()),
                extra_bin,
            ],
        ))
        .expect("insert message");
        if (row + 1) % 50_000 == 0 {
            block_on(conn.execute("COMMIT;")).expect("commit");
            block_on(conn.execute("BEGIN;")).expect("begin");
        }
    }
    block_on(conn.execute("COMMIT;")).expect("commit");
    let ids = (1..=conversations)
        .map(|i| i64::try_from(i).unwrap())
        .collect();
    (ids, order.len(), bytes)
}

fn hash_row(hasher: &mut Sha256, row: &Row) {
    hasher.update((row.values().len() as u64).to_le_bytes());
    for value in row.values() {
        match value {
            SqliteValue::Null => hasher.update([0]),
            SqliteValue::Integer(v) => {
                hasher.update([1]);
                hasher.update(v.to_le_bytes());
            }
            SqliteValue::Float(v) => {
                hasher.update([2]);
                hasher.update(v.to_bits().to_le_bytes());
            }
            SqliteValue::Text(v) => {
                hasher.update([3]);
                hasher.update((v.len() as u64).to_le_bytes());
                hasher.update(v.as_bytes_direct());
            }
            SqliteValue::Blob(v) => {
                hasher.update([4]);
                hasher.update((v.len() as u64).to_le_bytes());
                hasher.update(v.as_ref());
            }
        }
    }
}

fn conversation_sql(cap: usize) -> String {
    format!(
        "SELECT id, idx, role, author, created_at, substr(content, 1, {cap}), \
         COALESCE(octet_length(content), 0) FROM messages \
         INDEXED BY sqlite_autoindex_messages_1 WHERE conversation_id = ?1 ORDER BY idx"
    )
}

fn scan_sql(cap: usize) -> String {
    format!(
        "SELECT id, idx, role, author, created_at, substr(content, 1, {cap}), \
         COALESCE(octet_length(content), 0) FROM messages WHERE id >= ?1"
    )
}

/// Every conversation through cass's per-conversation query. With `hasher`,
/// digests every row; otherwise only counts rows (the timed form).
fn read_conversations(
    conn: &Connection,
    sql: &str,
    ids: &[i64],
    mut hasher: Option<&mut Sha256>,
) -> usize {
    let mut rows = 0_usize;
    for &id in ids {
        block_on(
            conn.query_with_params_for_each(sql, &[SqliteValue::Integer(id)], |row| {
                rows += 1;
                if let Some(hasher) = hasher.as_deref_mut() {
                    hash_row(hasher, row);
                } else {
                    black_box(row);
                }
                Ok(())
            }),
        )
        .expect("conversation query");
    }
    rows
}

fn read_scan(conn: &Connection, sql: &str, mut hasher: Option<&mut Sha256>) -> usize {
    let mut rows = 0_usize;
    block_on(
        conn.query_with_params_for_each(sql, &[SqliteValue::Integer(0)], |row| {
            rows += 1;
            if let Some(hasher) = hasher.as_deref_mut() {
                hash_row(hasher, row);
            } else {
                black_box(row);
            }
            Ok(())
        }),
    )
    .expect("scan query");
    rows
}

fn digest_arm(conn: &Connection, arm: Arm, ids: &[i64]) -> String {
    arm.select();
    let mut hasher = Sha256::new();
    let mut rows = read_conversations(conn, &conversation_sql(CASS_CAP), ids, Some(&mut hasher));
    rows += read_scan(conn, &scan_sql(CASS_CAP), Some(&mut hasher));
    for n in EDGE_PREFIXES {
        rows += read_scan(conn, &scan_sql(n), Some(&mut hasher));
    }
    let digest: String = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("{rows}:{digest}")
}

/// Linux peak-RSS (VmHWM) helpers; `None` elsewhere.
fn reset_peak_rss() {
    let _ = std::fs::write("/proc/self/clear_refs", "5");
}

fn peak_rss_kib() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with("VmHWM:"))?;
    line.split_whitespace().nth(1)?.parse().ok()
}

fn current_rss_kib() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
    line.split_whitespace().nth(1)?.parse().ok()
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    let mid = values.len() / 2;
    if values.len().is_multiple_of(2) {
        f64::midpoint(values[mid - 1], values[mid])
    } else {
        values[mid]
    }
}

fn main() {
    // `cargo bench` passes `--bench`; ignore harness arguments.
    let conversations = env_usize("SUBSTR_AB_CONVERSATIONS", 200);
    let rounds = env_usize("SUBSTR_AB_ROUNDS", 11);
    let rng_seed = u64::try_from(env_usize("SUBSTR_AB_SEED", 7)).unwrap();
    let base = std::env::var_os("SUBSTR_AB_DIR").map_or_else(std::env::temp_dir, Into::into);
    let dir = tempfile::Builder::new()
        .prefix("substr-ab-")
        .tempdir_in(base)
        .expect("bench dir");
    let db = dir.path().join("messages.db");

    let started = Instant::now();
    let conn = block_on(Connection::open(db.to_string_lossy().into_owned())).expect("open");
    let (ids, messages, bytes) = seed(&conn, conversations, rng_seed);
    drop(conn);
    let conn = block_on(Connection::open(db.to_string_lossy().into_owned())).expect("reopen");
    println!(
        "fixture conversations={conversations} messages={messages} content_bytes={bytes} \
         db_bytes={} seed_s={:.1}",
        std::fs::metadata(&db).map_or(0, |m| m.len()),
        started.elapsed().as_secs_f64()
    );

    // Witness: every arm returns the same rows, values and order.
    let digests: Vec<String> = ARMS
        .iter()
        .map(|arm| digest_arm(&conn, *arm, &ids))
        .collect();
    for (arm, digest) in ARMS.iter().zip(&digests) {
        println!("witness arm={} {digest}", arm.name());
    }
    assert!(
        digests.iter().all(|digest| digest == &digests[0]),
        "arms disagree: {digests:?}"
    );

    let conversation = conversation_sql(CASS_CAP);
    let scan = scan_sql(CASS_CAP);
    let cells: Vec<(&str, CellRun<'_>)> = vec![
        (
            "conversations",
            Box::new(|conn: &Connection| read_conversations(conn, &conversation, &ids, None)),
        ),
        (
            "scan",
            Box::new(|conn: &Connection| read_scan(conn, &scan, None)),
        ),
    ];
    for (cell, run) in &cells {
        // Warm the page cache once per cell.
        Arm::Generic.select();
        assert_eq!(run(&conn), messages);
        let mut ms: [Vec<f64>; 3] = [Vec::new(), Vec::new(), Vec::new()];
        let mut rss: [Vec<u64>; 3] = [Vec::new(), Vec::new(), Vec::new()];
        let mut ratio = Vec::new();
        let mut null_ratio = Vec::new();
        for round in 0..rounds {
            let mut round_ms = [0.0_f64; 3];
            for offset in 0..ARMS.len() {
                let slot = (round + offset) % ARMS.len();
                let arm = ARMS[slot];
                arm.select();
                reset_peak_rss();
                let before = current_rss_kib().unwrap_or(0);
                let start = Instant::now();
                let rows = run(&conn);
                let elapsed = start.elapsed().as_secs_f64() * 1000.0;
                assert_eq!(rows, messages, "{} row count", arm.name());
                round_ms[slot] = elapsed;
                ms[slot].push(elapsed);
                rss[slot].push(peak_rss_kib().unwrap_or(0).saturating_sub(before));
            }
            ratio.push(round_ms[1] / round_ms[0]);
            null_ratio.push(round_ms[2] / round_ms[0]);
        }
        for (slot, arm) in ARMS.iter().enumerate() {
            let mut values = ms[slot].clone();
            let low = values.iter().copied().fold(f64::INFINITY, f64::min);
            let high = values.iter().copied().fold(0.0, f64::max);
            let p50 = median(&mut values);
            let rss_max = rss[slot].iter().copied().max().unwrap_or(0);
            println!(
                "cell={cell} arm={} p50_ms={p50:.1} min_ms={low:.1} max_ms={high:.1} \
                 rows_per_s={:.0} mb_per_s={:.1} peak_rss_growth_kib_max={rss_max}",
                arm.name(),
                messages as f64 / (p50 / 1000.0),
                bytes as f64 / 1e6 / (p50 / 1000.0),
            );
        }
        let low = ratio.iter().copied().fold(f64::INFINITY, f64::min);
        let high = ratio.iter().copied().fold(0.0, f64::max);
        let null_low = null_ratio.iter().copied().fold(f64::INFINITY, f64::min);
        let null_high = null_ratio.iter().copied().fold(0.0, f64::max);
        println!(
            "cell={cell} ratio_whole_over_generic_p50={:.4} [{low:.4}, {high:.4}] \
             null_generic_null_over_generic_p50={:.4} [{null_low:.4}, {null_high:.4}] rounds={rounds}",
            median(&mut ratio),
            median(&mut null_ratio),
        );
    }
    Arm::Generic.select();
}
