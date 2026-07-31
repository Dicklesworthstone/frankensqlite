//! Standalone false-sharing A/B for Apple M-series cache-line verification.
//! Threads hammer adjacent per-thread atomics at 64B / 128B / 256B spacing.
//! If M-series coherence operates on 128B granules, 64B spacing shows
//! pairwise false sharing (higher ns/op) while 128B and 256B converge.
//! No dependencies; build: rustc -O cacheline_ab.rs

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

#[repr(C, align(64))]
struct Slot64 {
    v: AtomicU64,
    _pad: [u8; 56],
}

#[repr(C, align(128))]
struct Slot128 {
    v: AtomicU64,
    _pad: [u8; 120],
}

#[repr(C, align(256))]
struct Slot256 {
    v: AtomicU64,
    _pad: [u8; 248],
}

const ITERS: u64 = 20_000_000;

fn run_arm<S: Sync + Send + 'static>(
    name: &str,
    threads: usize,
    slots: Arc<Vec<S>>,
    slot_atomic: fn(&S) -> &AtomicU64,
) {
    let start_flag = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();
    for t in 0..threads {
        let slots = Arc::clone(&slots);
        let flag = Arc::clone(&start_flag);
        handles.push(std::thread::spawn(move || {
            while !flag.load(Ordering::Acquire) {
                std::hint::spin_loop();
            }
            let a = slot_atomic(&slots[t]);
            for _ in 0..ITERS {
                a.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    // Give threads time to park on the flag.
    std::thread::sleep(std::time::Duration::from_millis(50));
    let t0 = Instant::now();
    start_flag.store(true, Ordering::Release);
    for h in handles {
        h.join().unwrap();
    }
    let el = t0.elapsed();
    let ns_per_op = el.as_nanos() as f64 / (ITERS as f64 * threads as f64);
    println!(
        "arm={name} threads={threads} total={:?} ns_per_op={:.2}",
        el, ns_per_op
    );
}

fn main() {
    let max_threads: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    println!(
        "slot sizes: 64={} 128={} 256={}",
        std::mem::size_of::<Slot64>(),
        std::mem::size_of::<Slot128>(),
        std::mem::size_of::<Slot256>()
    );
    for &threads in &[2usize, 4, 8] {
        if threads > max_threads {
            break;
        }
        for rep in 0..3 {
            let s64: Arc<Vec<Slot64>> = Arc::new(
                (0..threads)
                    .map(|_| Slot64 {
                        v: AtomicU64::new(0),
                        _pad: [0; 56],
                    })
                    .collect(),
            );
            run_arm(&format!("spacing64_rep{rep}"), threads, s64, |s| &s.v);
            let s128: Arc<Vec<Slot128>> = Arc::new(
                (0..threads)
                    .map(|_| Slot128 {
                        v: AtomicU64::new(0),
                        _pad: [0; 120],
                    })
                    .collect(),
            );
            run_arm(&format!("spacing128_rep{rep}"), threads, s128, |s| &s.v);
            let s256: Arc<Vec<Slot256>> = Arc::new(
                (0..threads)
                    .map(|_| Slot256 {
                        v: AtomicU64::new(0),
                        _pad: [0; 248],
                    })
                    .collect(),
            );
            run_arm(&format!("spacing256_rep{rep}"), threads, s256, |s| &s.v);
        }
    }
}
