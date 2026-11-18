#![recursion_limit = "512"]

//! bd-ivhow: a writer must not hand out, as a free page, a page number it
//! only reserved, after a peer process committed that number as a live page.
//!
//! An fsqlite pager's in-memory freelist holds two kinds of page number:
//! pages on the durable freelist, and volatile ones (the unused tail of a
//! page lease, abandoned EOF reservations) parked there while they lay above
//! the committed db_size and so never published as free. In the own-tables
//! shape (8 fsqlite processes, a table each), stock's integrity_check found
//! "2nd reference to page 132" in about 1 rep in 100 on hz3. In the kept
//! file, worker 3 had leased pages 128-135 and returned the unused 129-135
//! to its freelist; worker 6 then grew the file through 132 and committed it
//! as its right-most leaf; worker 3's next commit certified the larger
//! durable floor, so its db_size covered 132 while 132 was still on its
//! freelist; and a later split of worker 3 took 132 through the snapshot-safe
//! reuse arm (`alloc_freelist_gated`), overwriting worker 6's leaf. Neither
//! the EOF anti-alias gate (132 was within worker 3's snapshot) nor
//! first-committer-wins (worker 6 committed before it) could see it.
//!
//! The deterministic shape, with two fsqlite writer processes R and P and
//! stock as the oracle (stock seeds both tables so that every further
//! 3000-byte row is a quick balance onto exactly one new page):
//! 1. R commits a transaction that allocates two pages; the second takes a
//!    page lease, whose unused tail stays on R's freelist above db_size D,
//!    never published as free.
//! 2. R begins a transaction that rewrites a row in place (no growth).
//! 3. P grows the file by one page, D+1, and commits it as its leaf.
//! 4. R commits at P's larger durable size, so R's db_size now
//!    covers D+1 while D+1 is still on R's freelist.
//! 5. R splits again and again, one row per transaction, through the rest
//!    of its lease tail and past it.
//!
//! A lease tail that falls inside the writer's own extent is published as
//! durable free pages instead; then the freelist publication guard refuses
//! a stale reuse (safely), which is why the seed fixes the page layout.
//!
//! Required: stock finds the file intact, and each table's durable rows are
//! exactly its writer's committed rows. On main 3e6cc4c0c step 5 hands R one
//! of P's pages.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use fsqlite::Connection;

const ACTOR_ENV: &str = "FSQLITE_BD_IVHOW_ACTOR";
const FLOOR_CERTIFIED_MARKER: &str = "VNXJD FLOOR-CERTIFIED";

fn wait_for(path: &Path) {
    let started = Instant::now();
    while !path.exists() {
        assert!(
            started.elapsed() < Duration::from_secs(120),
            "timed out waiting for {}",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn write_atomically(path: &Path, text: &str) {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, text).expect("write tmp");
    std::fs::rename(&tmp, path).expect("rename");
}

/// An fsqlite actor process: one connection, then command files
/// `<name>.cmd.<n>` in order, one statement per line, one result line per
/// statement in `<name>.res.<n>` ("ok" or "err code=<extended> <text>");
/// `EXIT` closes it.
#[test]
fn bd_ivhow_actor() {
    let Ok(spec) = std::env::var(ACTOR_ENV) else {
        return;
    };
    let parts: Vec<&str> = spec.split('|').collect();
    let [db, dir, name] = parts[..] else {
        panic!("actor spec {spec}");
    };
    let dir = PathBuf::from(dir);
    fsqlite_e2e::block_on(async {
        let conn = Connection::open(db).await.expect("fsqlite open");
        conn.execute("PRAGMA busy_timeout=2000")
            .await
            .expect("busy_timeout");
        std::fs::write(dir.join(format!("{name}.ready")), b"").expect("ready");
        for n in 0.. {
            let path = dir.join(format!("{name}.cmd.{n}"));
            wait_for(&path);
            let text = std::fs::read_to_string(&path).expect("command");
            if text == "EXIT" {
                break;
            }
            let mut lines = Vec::new();
            for sql in text.lines().filter(|line| !line.is_empty()) {
                lines.push(match conn.execute(sql).await {
                    Ok(_) => "ok".to_owned(),
                    Err(error) => format!("err code={} {error}", error.extended_error_code()),
                });
            }
            write_atomically(&dir.join(format!("{name}.res.{n}")), &lines.join("\n"));
        }
        conn.close().await.expect("fsqlite close");
    });
}

struct Actor {
    name: String,
    dir: PathBuf,
    next: usize,
    child: Option<Child>,
}

impl Actor {
    /// The actor runs with the pager's DW8OE trace on; its stderr goes to
    /// `<name>.stderr`.
    fn spawn(db: &Path, dir: &Path, name: &str) -> Self {
        let stderr = std::fs::File::create(dir.join(format!("{name}.stderr"))).expect("stderr");
        let child = Command::new(std::env::current_exe().expect("test binary path"))
            .args([
                "--exact",
                "bd_ivhow_actor",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(
                ACTOR_ENV,
                format!("{}|{}|{name}", db.display(), dir.display()),
            )
            .env("DW8OE_TRACE", "1")
            .env(
                "FSQLITE_ALLOC_LEDGER_PATH",
                dir.join(format!("{name}.alloc")),
            )
            .stdout(Stdio::null())
            .stderr(stderr)
            .spawn()
            .expect("spawn actor");
        wait_for(&dir.join(format!("{name}.ready")));
        Self {
            name: name.to_owned(),
            dir: dir.to_path_buf(),
            next: 0,
            child: Some(child),
        }
    }

    fn run(&mut self, sql: &str) -> Vec<String> {
        let n = self.next;
        self.next += 1;
        write_atomically(&self.dir.join(format!("{}.cmd.{n}", self.name)), sql);
        let result = self.dir.join(format!("{}.res.{n}", self.name));
        wait_for(&result);
        std::fs::read_to_string(result)
            .expect("result")
            .lines()
            .map(str::to_owned)
            .collect()
    }

    fn exit(mut self) -> String {
        write_atomically(
            &self.dir.join(format!("{}.cmd.{}", self.name, self.next)),
            "EXIT",
        );
        let status = self
            .child
            .take()
            .expect("child")
            .wait()
            .expect("actor exit");
        let stderr = std::fs::read_to_string(self.dir.join(format!("{}.stderr", self.name)))
            .unwrap_or_default();
        assert!(status.success(), "actor {} failed: {stderr}", self.name);
        stderr
    }
}

impl Drop for Actor {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// On a failing run, every writer's page grants (the allocation ledger) and
/// its pager trace, so the failure names the pages involved.
struct Diagnostics(PathBuf);

impl Drop for Diagnostics {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            return;
        }
        for name in ["r", "p"] {
            for suffix in ["alloc", "stderr"] {
                let text = std::fs::read_to_string(self.0.join(format!("{name}.{suffix}")))
                    .unwrap_or_default();
                eprintln!("event=bd_ivhow_diag writer={name} file={suffix}:\n{text}");
            }
        }
    }
}

/// A row about 3000 bytes long: one per leaf, so every insert into a full
/// table allocates a page.
fn big_row(table: &str, id: i64) -> String {
    format!(
        "INSERT INTO {table}(id, pad) VALUES ({id}, '{table}{}')",
        "x".repeat(3000)
    )
}

/// Every statement of a transaction succeeded.
fn all_ok(results: &[String]) -> bool {
    results.iter().all(|line| line == "ok")
}

fn stock_ids(stock: &rusqlite::Connection, table: &str) -> BTreeSet<i64> {
    stock
        .prepare(&format!("SELECT id FROM {table}"))
        .expect("prepare")
        .query_map([], |row| row.get(0))
        .expect("query")
        .collect::<rusqlite::Result<_>>()
        .expect("ids")
}

#[test]
fn a_writer_never_reuses_a_reserved_page_number_a_peer_committed_live() {
    if std::env::var_os(ACTOR_ENV).is_some() {
        return;
    }
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("ivhow.db");
    // Stock builds both tables with an interior root and two leaves holding
    // one 3000-byte row each, so every further row is a quick balance onto
    // exactly one new page. Nothing is free in the durable image.
    let seed = rusqlite::Connection::open(&db).expect("seed open");
    seed.execute_batch(
        "PRAGMA journal_mode=WAL;
         CREATE TABLE r(id INTEGER PRIMARY KEY, pad TEXT NOT NULL);
         CREATE TABLE p(id INTEGER PRIMARY KEY, pad TEXT NOT NULL);",
    )
    .expect("schema");
    for table in ["r", "p"] {
        for id in 1..=2 {
            seed.execute_batch(&big_row(table, id)).expect("seed row");
        }
    }
    drop(seed);
    let _diagnostics = Diagnostics(dir.path().to_path_buf());
    let mut r = Actor::spawn(&db, dir.path(), "r");
    let mut p = Actor::spawn(&db, dir.path(), "p");
    let (mut r_rows, mut p_rows): (BTreeSet<i64>, BTreeSet<i64>) =
        ((1..=2).collect(), (1..=2).collect());

    // 1. R allocates two pages in one transaction. The second allocation
    //    takes a page lease; the lease's unused tail goes back to R's
    //    in-memory freelist above the committed db_size, never published.
    let results = r.run(&format!(
        "BEGIN\n{}\n{}\nCOMMIT",
        big_row("r", 3),
        big_row("r", 4)
    ));
    assert!(all_ok(&results), "R's first transaction: {results:?}");
    r_rows.extend([3, 4]);

    // 2. R begins a transaction that rewrites a row in place.
    let opened = r.run(&format!(
        "BEGIN\nUPDATE r SET pad = 'r{}' WHERE id = 1",
        "y".repeat(3000)
    ));
    assert!(all_ok(&opened), "R's open transaction: {opened:?}");

    // 3. P grows the file by one page, the first number of R's lease tail,
    //    and commits it as its right-most leaf.
    let results = p.run(&big_row("p", 3));
    assert!(all_ok(&results), "P's growth: {results:?}");
    p_rows.insert(3);

    // 4. R commits at P's larger durable size, so R's db_size
    //    now covers P's page while that number is still on R's freelist.
    let results = r.run("COMMIT");
    assert!(all_ok(&results), "R's in-place commit: {results:?}");

    // 5. R splits, one row per transaction, through its whole lease tail
    //    and past it.
    for id in 5..=16 {
        let results = r.run(&big_row("r", id));
        assert!(all_ok(&results), "R's row {id}: {results:?}");
        r_rows.insert(id);
    }
    // P keeps appending to its own table, through the leaf it committed.
    for id in 4..=6 {
        let results = p.run(&big_row("p", id));
        assert!(all_ok(&results), "P's row {id}: {results:?}");
        p_rows.insert(id);
    }
    let r_trace = r.exit();
    p.exit();

    // Non-vacuity, from the allocation ledgers: P's new page is a number of
    // R's lease tail (below the tail pages R itself went on to take from its
    // freelist), and R kept allocating after that tail ran out, so the
    // number P took was the next freelist candidate R had left.
    if let Some(certified) = r_trace
        .lines()
        .find(|line| line.contains(FLOOR_CERTIFIED_MARKER))
    {
        eprintln!("event=bd_ivhow_floor {certified}");
    }
    let r_ledger = std::fs::read_to_string(dir.path().join("r.alloc")).expect("R's ledger");
    let p_ledger = std::fs::read_to_string(dir.path().join("p.alloc")).expect("P's ledger");
    let pages = |ledger: &str, event: &str| -> Vec<u32> {
        ledger
            .lines()
            .filter_map(|line| {
                let fields: Vec<&str> = line.split_whitespace().collect();
                (fields.get(1) == Some(&event))
                    .then(|| fields.get(2).and_then(|page| page.parse().ok()))
                    .flatten()
            })
            .collect()
    };
    let p_growth = *pages(&p_ledger, "alloc_eof")
        .first()
        .unwrap_or_else(|| panic!("non-vacuity: P grew no page; ledger:\n{p_ledger}"));
    let r_tail = pages(&r_ledger, "alloc_freelist_hi");
    let tail_low = r_tail.iter().copied().min().unwrap_or_else(|| {
        panic!("non-vacuity: R took nothing from a lease tail on its freelist; ledger:\n{r_ledger}")
    });
    assert!(
        p_growth < tail_low,
        "non-vacuity: P's page {p_growth} is not a number of R's lease tail (R took {r_tail:?})"
    );
    let after_tail = r_ledger
        .lines()
        .skip_while(|line| !line.contains("alloc_freelist_hi"))
        .filter(|line| !line.contains("alloc_freelist_hi"))
        .count();
    assert!(
        after_tail > 0,
        "non-vacuity: R stopped allocating at the end of its lease tail; ledger:\n{r_ledger}"
    );
    eprintln!(
        "event=bd_ivhow_shape p_growth={p_growth} r_tail={r_tail:?} r_after_tail={after_tail}"
    );

    let stock = rusqlite::Connection::open(&db).expect("verify open");
    let integrity: Vec<String> = stock
        .prepare("PRAGMA integrity_check")
        .expect("prepare")
        .query_map([], |row| row.get(0))
        .expect("integrity")
        .collect::<rusqlite::Result<_>>()
        .expect("integrity rows");
    assert_eq!(
        integrity,
        ["ok"],
        "stock integrity_check: a page R only reserved was reused after P committed it live"
    );
    assert_eq!(stock_ids(&stock, "r"), r_rows, "R's durable rows");
    assert_eq!(stock_ids(&stock, "p"), p_rows, "P's durable rows");
    for table in ["r", "p"] {
        let foreign: i64 = stock
            .query_row(
                &format!("SELECT count(*) FROM {table} WHERE substr(pad, 1, 1) <> '{table}'"),
                [],
                |row| row.get(0),
            )
            .expect("foreign rows");
        assert_eq!(foreign, 0, "{table} lists rows of the other table");
    }
}
