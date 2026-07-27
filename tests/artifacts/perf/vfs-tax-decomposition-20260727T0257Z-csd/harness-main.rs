//! bd-trfah / bd-dqdoe mechanism decomposition harness (GreenBirch, 2026-07-27).
//!
//! Same-binary, paired, interleaved arms that decompose the per-page cost of
//! the UnixFile fallback I/O path (the production path per bd-fo6xw: 7520
//! fallbacks, 0 io_uring samples) and the sync-bridge runtime-entry overhead:
//!
//!   read/shipped   UnixFile::read as shipped: spawn_blocking_io hop +
//!                  vec![0; ps] alloc/zero + pread + copy_from_slice
//!   read/inline    counterfactual fix: identical pread loop, inline, straight
//!                  into the caller's buffer (no hop, no alloc, no copy)
//!   hop/empty      spawn_blocking_io round-trip with a trivial closure
//!   alloc+copy     vec![0; ps] + copy_from_slice only (no I/O)
//!   write/shipped  UnixFile::write as shipped: buf.to_vec() + hop + pwrite
//!   write/inline   counterfactual pwrite loop, inline, from caller's buffer
//!   rt/fresh       per-op RuntimeBuilder().build() + block_on(async{}) + drop
//!                  (the "every operation gets its own runtime" bridge shape)
//!   rt/reenter     per-op block_on(async{}) on a persistent runtime
//!
//! Expectation if the bd-trfah model is right:
//!   read/shipped - read/inline  ~=  hop/empty + alloc+copy
//!
//! Offsets are page-aligned via a fixed-seed LCG, identical across arms and
//! rounds. Arm order rotates each round so slow drift cancels. The counting
//! global allocator gives exact allocation counts per operation, which are
//! load-independent evidence even on a busy host.

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::io::Read as _;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use fsqlite_types::cx::Cx;
use fsqlite_types::flags::VfsOpenFlags;
use fsqlite_vfs::{UnixVfs, Vfs, VfsFile};

// ---------------------------------------------------------------- allocator

struct CountingAlloc;
static ALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

fn snap() -> (u64, u64) {
    (
        ALLOC_CALLS.load(Ordering::Relaxed),
        ALLOC_BYTES.load(Ordering::Relaxed),
    )
}

// ------------------------------------------------------------------ offsets

struct Lcg(u64);
impl Lcg {
    fn next_page(&mut self, pages: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.0 >> 33) % pages
    }
}

const SEED: u64 = 0x5eed_bd7f_a4_2026;

// ------------------------------------------------------------------- helpers

fn read_exact_at_inline(file: &std::fs::File, buf: &mut [u8], offset: u64) -> usize {
    let mut total = 0usize;
    while total < buf.len() {
        match file.read_at(&mut buf[total..], offset + total as u64) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => panic!("inline pread failed: {e}"),
        }
    }
    total
}

fn write_all_at_inline(file: &std::fs::File, buf: &[u8], offset: u64) {
    let mut total = 0usize;
    while total < buf.len() {
        match file.write_at(&buf[total..], offset + total as u64) {
            Ok(0) => panic!("inline pwrite wrote zero bytes"),
            Ok(n) => total += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => panic!("inline pwrite failed: {e}"),
        }
    }
}

struct Sample {
    ns_per_op: f64,
    allocs_per_op: f64,
    bytes_per_op: f64,
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = v.len();
    if n % 2 == 1 { v[n / 2] } else { (v[n / 2 - 1] + v[n / 2]) / 2.0 }
}

// ---------------------------------------------------------------------- main

fn main() {
    let ps: usize = 4096;
    let pages: u64 = 2048; // 8 MiB file: fits page cache; isolates CPU cost.
    let rounds: usize = 15;

    let dir = std::env::temp_dir().join(format!("vfs-tax-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    let db_path: PathBuf = dir.join("tax.db");

    // Populate the file with deterministic non-zero bytes via std, sync once.
    {
        let f = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&db_path)
            .expect("create data file");
        let mut page = vec![0u8; ps];
        for p in 0..pages {
            for (i, b) in page.iter_mut().enumerate() {
                *b = ((p as usize * 31 + i * 7) % 251) as u8;
            }
            f.write_at(&page, p * ps as u64).expect("populate");
        }
        f.sync_all().expect("sync populate");
    }

    // Shipped-path handle: the real UnixVfs/UnixFile, exactly as production
    // falls back to it (Cx WITHOUT an attached native cx — the bd-fo6xw gate
    // state).
    let cx = Cx::new();
    let vfs = UnixVfs::new();
    let (unix_file, _) = vfs
        .open(
            &cx,
            Some(db_path.as_path()),
            VfsOpenFlags::MAIN_DB | VfsOpenFlags::READWRITE,
        )
        .expect("UnixVfs open");

    // Counterfactual handle: plain std File on the same path.
    let raw_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&db_path)
        .expect("raw open");

    // Persistent runtime, mirroring the crate's own test-io runtime shape.
    let runtime = asupersync::runtime::RuntimeBuilder::current_thread()
        .blocking_threads(1, 2)
        .build()
        .expect("build runtime");

    let mut buf = vec![0u8; ps];
    let wpage = vec![0xA5u8; ps];

    let arm_names = [
        "read/shipped",
        "read/inline",
        "hop/empty",
        "alloc+copy",
        "write/shipped",
        "write/inline",
        "rt/fresh",
        "rt/reenter",
        "write/batch16",
    ];
    let iters: [u32; 9] = [2000, 2000, 2000, 20000, 2000, 2000, 200, 5000, 2048];
    let mut results: Vec<Vec<Sample>> = (0..arm_names.len()).map(|_| Vec::new()).collect();

    for round in 0..rounds {
        for k in 0..arm_names.len() {
            let arm = (round + k) % arm_names.len(); // rotate order per round
            let n = iters[arm];
            let (c0, b0) = snap();
            let t0 = Instant::now();
            match arm {
                0 => {
                    // Shipped UnixFile::read (hop + alloc/zero + pread + copy).
                    runtime.block_on(async {
                        let mut lcg = Lcg(SEED);
                        for _ in 0..n {
                            let off = lcg.next_page(pages) * ps as u64;
                            let got = VfsFile::read(&unix_file, &cx, &mut buf, off)
                                .await
                                .expect("shipped read");
                            assert_eq!(got, ps);
                            black_box(buf[0]);
                        }
                    });
                }
                1 => {
                    // Counterfactual inline pread into the caller's buffer.
                    runtime.block_on(async {
                        let mut lcg = Lcg(SEED);
                        for _ in 0..n {
                            let off = lcg.next_page(pages) * ps as u64;
                            let got = read_exact_at_inline(&raw_file, &mut buf, off);
                            assert_eq!(got, ps);
                            black_box(buf[0]);
                        }
                    });
                }
                2 => {
                    // Blocking-pool round-trip with a trivial closure.
                    runtime.block_on(async {
                        for _ in 0..n {
                            let r: std::io::Result<u64> =
                                asupersync::runtime::spawn_blocking_io(|| Ok(0u64)).await;
                            black_box(r.expect("empty hop"));
                        }
                    });
                }
                3 => {
                    // Allocation + zero-fill + full-page copy, no I/O.
                    let mut lcg = Lcg(SEED);
                    for _ in 0..n {
                        let _ = lcg.next_page(pages);
                        let data = vec![0u8; ps];
                        buf.copy_from_slice(black_box(&data));
                        black_box(buf[0]);
                    }
                }
                4 => {
                    // Shipped UnixFile::write (to_vec + hop + pwrite).
                    runtime.block_on(async {
                        let mut lcg = Lcg(SEED ^ 0xdead);
                        for _ in 0..n {
                            let off = lcg.next_page(pages) * ps as u64;
                            VfsFile::write(&unix_file, &cx, &wpage, off)
                                .await
                                .expect("shipped write");
                        }
                    });
                }
                5 => {
                    // Counterfactual inline pwrite from the caller's buffer.
                    runtime.block_on(async {
                        let mut lcg = Lcg(SEED ^ 0xdead);
                        for _ in 0..n {
                            let off = lcg.next_page(pages) * ps as u64;
                            write_all_at_inline(&raw_file, &wpage, off);
                        }
                    });
                }
                6 => {
                    // Fresh runtime per op: build + enter + teardown.
                    for _ in 0..n {
                        let rt = asupersync::runtime::RuntimeBuilder::current_thread()
                            .blocking_threads(1, 2)
                            .build()
                            .expect("fresh runtime");
                        rt.block_on(async {});
                        drop(rt);
                    }
                }
                7 => {
                    // Re-enter a persistent runtime per op.
                    for _ in 0..n {
                        runtime.block_on(async {});
                    }
                }
                8 => {
                    // The landed write_page_batch shape: stage 16 pages with
                    // to_vec, ONE spawn_blocking_io pwrites the group.
                    // ns/op is per PAGE (n pages total in groups of 16).
                    runtime.block_on(async {
                        let mut lcg = Lcg(SEED ^ 0xbeef);
                        let groups = n / 16;
                        for _ in 0..groups {
                            let mut staged: Vec<(u64, Vec<u8>)> = Vec::with_capacity(16);
                            for _ in 0..16 {
                                let off = lcg.next_page(pages) * ps as u64;
                                staged.push((off, wpage.to_vec()));
                            }
                            let file = raw_file.try_clone().expect("clone fd");
                            let r: std::io::Result<()> =
                                asupersync::runtime::spawn_blocking_io(move || {
                                    for (off, data) in staged {
                                        let mut total = 0usize;
                                        while total < data.len() {
                                            match file
                                                .write_at(&data[total..], off + total as u64)
                                            {
                                                Ok(0) => panic!("batch pwrite zero"),
                                                Ok(w) => total += w,
                                                Err(e)
                                                    if e.kind()
                                                        == std::io::ErrorKind::Interrupted => {}
                                                Err(e) => return Err(e),
                                            }
                                        }
                                    }
                                    Ok(())
                                })
                                .await;
                            r.expect("batched write");
                        }
                    });
                }
                _ => unreachable!(),
            }
            let dt = t0.elapsed().as_nanos() as f64;
            let (c1, b1) = snap();
            results[arm].push(Sample {
                ns_per_op: dt / f64::from(n),
                allocs_per_op: (c1 - c0) as f64 / f64::from(n),
                bytes_per_op: (b1 - b0) as f64 / f64::from(n),
            });
        }
    }

    // Load averages for the run record.
    let mut loadavg = String::new();
    let _ = std::fs::File::open("/proc/loadavg")
        .and_then(|mut f| f.read_to_string(&mut loadavg));

    println!("# vfs-tax decomposition — {} rounds, page_size={}, pages={}", rounds, ps, pages);
    println!("# loadavg at end: {}", loadavg.trim());
    println!(
        "{:<14} {:>10} {:>10} {:>10} {:>12} {:>12}",
        "arm", "med ns/op", "min ns/op", "max ns/op", "allocs/op", "bytes/op"
    );
    let mut json = String::from("{\"rounds\":");
    json.push_str(&format!("{rounds},\"page_size\":{ps},\"pages\":{pages},\"arms\":{{"));
    for (i, name) in arm_names.iter().enumerate() {
        let ns: Vec<f64> = results[i].iter().map(|s| s.ns_per_op).collect();
        let med = median(ns.clone());
        let min = ns.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = ns.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let allocs = median(results[i].iter().map(|s| s.allocs_per_op).collect());
        let bytes = median(results[i].iter().map(|s| s.bytes_per_op).collect());
        println!(
            "{:<14} {:>10.0} {:>10.0} {:>10.0} {:>12.2} {:>12.0}",
            name, med, min, max, allocs, bytes
        );
        json.push_str(&format!(
            "\"{name}\":{{\"median_ns\":{med:.1},\"min_ns\":{min:.1},\"max_ns\":{max:.1},\"allocs_per_op\":{allocs:.3},\"bytes_per_op\":{bytes:.1},\"rounds_ns\":{ns:?}}},"
        ));
    }
    json.pop();
    json.push_str("}}");
    let out = dir.join("vfs-tax.json");
    std::fs::write(&out, &json).expect("write json");
    println!("# json: {}", out.display());

    let shipped = median(results[0].iter().map(|s| s.ns_per_op).collect());
    let inline = median(results[1].iter().map(|s| s.ns_per_op).collect());
    let hop = median(results[2].iter().map(|s| s.ns_per_op).collect());
    let alloc_copy = median(results[3].iter().map(|s| s.ns_per_op).collect());
    println!(
        "# model check: (shipped - inline) = {:.0} ns/op vs (hop + alloc/copy) = {:.0} ns/op",
        shipped - inline,
        hop + alloc_copy
    );
    println!("# read tax factor: shipped/inline = {:.2}x", shipped / inline);
}
