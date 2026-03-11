//! Platform-agnostic sync primitives for FrankenSQLite.
//!
//! On native targets, these re-export `parking_lot` types for performance.
//! On `wasm32`, they provide wrappers around `std::sync` primitives, which
//! work correctly in the single-threaded wasm environment without requiring
//! unsafe Send/Sync impls.
//!
//! Downstream crates should import from here instead of `parking_lot`
//! directly to enable WASM compilation without `#[cfg]` at every call site.

// ---------------------------------------------------------------------------
// Native (parking_lot) — faster than std::sync on multi-core
// ---------------------------------------------------------------------------

#[cfg(not(target_arch = "wasm32"))]
pub use parking_lot::{
    Condvar, Mutex, MutexGuard, Once, RwLock, RwLockReadGuard, RwLockWriteGuard,
};

// ---------------------------------------------------------------------------
// WASM — thin wrappers around std::sync to match parking_lot's API
// ---------------------------------------------------------------------------

#[cfg(target_arch = "wasm32")]
mod wasm_sync {
    use std::ops::{Deref, DerefMut};
    use std::sync::{
        Mutex as StdMutex, MutexGuard as StdMutexGuard, Once as StdOnce, PoisonError,
        RwLock as StdRwLock, RwLockReadGuard as StdRwReadGuard,
        RwLockWriteGuard as StdRwWriteGuard,
    };

    // -- Mutex ----------------------------------------------------------------

    pub struct Mutex<T: ?Sized>(StdMutex<T>);

    impl<T> Mutex<T> {
        pub const fn new(val: T) -> Self {
            Self(StdMutex::new(val))
        }

        pub fn into_inner(self) -> T {
            self.0.into_inner().unwrap_or_else(PoisonError::into_inner)
        }
    }

    impl<T: ?Sized> Mutex<T> {
        pub fn lock(&self) -> MutexGuard<'_, T> {
            MutexGuard(self.0.lock().unwrap_or_else(PoisonError::into_inner))
        }

        pub fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
            self.0.try_lock().ok().map(MutexGuard)
        }

        pub fn is_locked(&self) -> bool {
            self.0.try_lock().is_err()
        }

        pub fn get_mut(&mut self) -> &mut T {
            self.0.get_mut().unwrap_or_else(PoisonError::into_inner)
        }
    }

    impl<T: Default> Default for Mutex<T> {
        fn default() -> Self {
            Self::new(T::default())
        }
    }

    impl<T: std::fmt::Debug + ?Sized> std::fmt::Debug for Mutex<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self.try_lock() {
                Some(guard) => f.debug_struct("Mutex").field("data", &&*guard).finish(),
                None => f.debug_struct("Mutex").field("data", &"<locked>").finish(),
            }
        }
    }

    pub struct MutexGuard<'a, T: ?Sized>(StdMutexGuard<'a, T>);

    impl<T: ?Sized> Deref for MutexGuard<'_, T> {
        type Target = T;
        fn deref(&self) -> &T {
            &self.0
        }
    }

    impl<T: ?Sized> DerefMut for MutexGuard<'_, T> {
        fn deref_mut(&mut self) -> &mut T {
            &mut self.0
        }
    }

    // -- RwLock ---------------------------------------------------------------

    pub struct RwLock<T: ?Sized>(StdRwLock<T>);

    impl<T> RwLock<T> {
        pub const fn new(val: T) -> Self {
            Self(StdRwLock::new(val))
        }

        pub fn into_inner(self) -> T {
            self.0.into_inner().unwrap_or_else(PoisonError::into_inner)
        }
    }

    impl<T: ?Sized> RwLock<T> {
        pub fn read(&self) -> RwLockReadGuard<'_, T> {
            RwLockReadGuard(self.0.read().unwrap_or_else(PoisonError::into_inner))
        }

        pub fn try_read(&self) -> Option<RwLockReadGuard<'_, T>> {
            self.0.try_read().ok().map(RwLockReadGuard)
        }

        pub fn write(&self) -> RwLockWriteGuard<'_, T> {
            RwLockWriteGuard(self.0.write().unwrap_or_else(PoisonError::into_inner))
        }

        pub fn try_write(&self) -> Option<RwLockWriteGuard<'_, T>> {
            self.0.try_write().ok().map(RwLockWriteGuard)
        }

        pub fn get_mut(&mut self) -> &mut T {
            self.0.get_mut().unwrap_or_else(PoisonError::into_inner)
        }
    }

    impl<T: Default> Default for RwLock<T> {
        fn default() -> Self {
            Self::new(T::default())
        }
    }

    impl<T: std::fmt::Debug + ?Sized> std::fmt::Debug for RwLock<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self.try_read() {
                Some(guard) => f.debug_struct("RwLock").field("data", &&*guard).finish(),
                None => f.debug_struct("RwLock").field("data", &"<locked>").finish(),
            }
        }
    }

    pub struct RwLockReadGuard<'a, T: ?Sized>(StdRwReadGuard<'a, T>);

    impl<T: ?Sized> Deref for RwLockReadGuard<'_, T> {
        type Target = T;
        fn deref(&self) -> &T {
            &self.0
        }
    }

    pub struct RwLockWriteGuard<'a, T: ?Sized>(StdRwWriteGuard<'a, T>);

    impl<T: ?Sized> Deref for RwLockWriteGuard<'_, T> {
        type Target = T;
        fn deref(&self) -> &T {
            &self.0
        }
    }

    impl<T: ?Sized> DerefMut for RwLockWriteGuard<'_, T> {
        fn deref_mut(&mut self) -> &mut T {
            &mut self.0
        }
    }

    // -- Once -----------------------------------------------------------------

    pub struct Once(StdOnce);

    impl Once {
        pub const fn new() -> Self {
            Self(StdOnce::new())
        }

        pub fn call_once(&self, f: impl FnOnce()) {
            self.0.call_once(f);
        }

        pub fn is_completed(&self) -> bool {
            self.0.is_completed()
        }
    }

    impl Default for Once {
        fn default() -> Self {
            Self::new()
        }
    }

    // -- Condvar --------------------------------------------------------------

    pub struct Condvar(std::sync::Condvar);

    impl Condvar {
        pub const fn new() -> Self {
            Self(std::sync::Condvar::new())
        }

        pub fn notify_one(&self) {
            self.0.notify_one();
        }

        pub fn notify_all(&self) {
            self.0.notify_all();
        }

        /// Wait on the condvar. On wasm32, the underlying std::sync::Condvar
        /// works but will never actually block (single-threaded).
        /// Note: parking_lot's Condvar::wait takes `&mut MutexGuard`, so the
        /// wasm shim mirrors that signature even though it is effectively a
        /// no-op in the single-threaded runtime.
        pub fn wait<T>(&self, guard: &mut MutexGuard<'_, T>) {
            let _ = (&self.0, guard);
        }
    }

    impl Default for Condvar {
        fn default() -> Self {
            Self::new()
        }
    }

    impl std::fmt::Debug for Condvar {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("Condvar")
        }
    }
}

#[cfg(target_arch = "wasm32")]
pub use wasm_sync::{Condvar, Mutex, MutexGuard, Once, RwLock, RwLockReadGuard, RwLockWriteGuard};

// ---------------------------------------------------------------------------
// Time polyfill — Instant / Duration
// ---------------------------------------------------------------------------

#[cfg(not(target_arch = "wasm32"))]
pub use std::time::{Duration, Instant};

#[cfg(target_arch = "wasm32")]
pub use std::time::Duration;

#[cfg(target_arch = "wasm32")]
use std::sync::atomic::{AtomicU64, Ordering};

/// Monotonic instant polyfill for wasm32.
///
/// On `wasm32-unknown-unknown` there is no reliable host-independent
/// monotonic clock without a JavaScript runtime. We therefore expose a small
/// deterministic pseudo-clock that advances one millisecond on each `now()`
/// call. This preserves ordering/arithmetic semantics used by timeout and
/// observability code without introducing JS host dependencies.
#[cfg(target_arch = "wasm32")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Instant(Duration);

#[cfg(target_arch = "wasm32")]
static WASM_INSTANT_TICKS_MS: AtomicU64 = AtomicU64::new(0);

#[cfg(target_arch = "wasm32")]
impl Instant {
    pub fn now() -> Self {
        Self(Duration::from_millis(
            WASM_INSTANT_TICKS_MS.fetch_add(1, Ordering::Relaxed),
        ))
    }

    pub fn elapsed(&self) -> Duration {
        Self::now().saturating_duration_since(*self)
    }

    pub fn duration_since(self, earlier: Self) -> Duration {
        self.0.checked_sub(earlier.0).unwrap_or(Duration::ZERO)
    }

    pub fn saturating_duration_since(self, earlier: Self) -> Duration {
        self.duration_since(earlier)
    }

    pub fn checked_duration_since(self, earlier: Self) -> Option<Duration> {
        self.0.checked_sub(earlier.0)
    }

    pub fn checked_add(self, duration: Duration) -> Option<Self> {
        self.0.checked_add(duration).map(Self)
    }

    pub fn checked_sub(self, duration: Duration) -> Option<Self> {
        self.0.checked_sub(duration).map(Self)
    }
}

#[cfg(target_arch = "wasm32")]
impl std::ops::Add<Duration> for Instant {
    type Output = Self;

    fn add(self, rhs: Duration) -> Self::Output {
        Self(self.0 + rhs)
    }
}

#[cfg(target_arch = "wasm32")]
impl std::ops::AddAssign<Duration> for Instant {
    fn add_assign(&mut self, rhs: Duration) {
        self.0 += rhs;
    }
}

#[cfg(target_arch = "wasm32")]
impl std::ops::Sub<Duration> for Instant {
    type Output = Self;

    fn sub(self, rhs: Duration) -> Self::Output {
        Self(self.0.checked_sub(rhs).unwrap_or(Duration::ZERO))
    }
}

#[cfg(target_arch = "wasm32")]
impl std::ops::SubAssign<Duration> for Instant {
    fn sub_assign(&mut self, rhs: Duration) {
        self.0 = self.0.checked_sub(rhs).unwrap_or(Duration::ZERO);
    }
}

#[cfg(target_arch = "wasm32")]
impl std::ops::Sub<Instant> for Instant {
    type Output = Duration;

    fn sub(self, rhs: Instant) -> Self::Output {
        self.duration_since(rhs)
    }
}

// ---------------------------------------------------------------------------
// Thread ID polyfill
// ---------------------------------------------------------------------------

#[cfg(not(target_arch = "wasm32"))]
pub fn current_thread_id() -> u64 {
    // ThreadId doesn't expose a numeric value on stable Rust, so we
    // use the Debug format to extract a deterministic integer. This is
    // only used for diagnostics/tracing, never for correctness.
    let id = std::thread::current().id();
    let s = format!("{id:?}");
    s.trim_start_matches("ThreadId(")
        .trim_end_matches(')')
        .parse::<u64>()
        .unwrap_or(0)
}

#[cfg(target_arch = "wasm32")]
pub fn current_thread_id() -> u64 {
    0 // Single-threaded on wasm32, always "thread 0".
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mutex_lock_unlock() {
        let m = Mutex::new(42);
        {
            let mut g = m.lock();
            assert_eq!(*g, 42);
            *g = 99;
        }
        assert_eq!(*m.lock(), 99);
    }

    #[test]
    fn mutex_try_lock_when_unlocked() {
        let m = Mutex::new(1);
        assert!(m.try_lock().is_some());
    }

    #[test]
    fn rwlock_read_write() {
        let rw = RwLock::new(String::from("hello"));
        {
            let r = rw.read();
            assert_eq!(&*r, "hello");
        }
        {
            let mut w = rw.write();
            w.push_str(" world");
        }
        assert_eq!(&*rw.read(), "hello world");
    }

    #[test]
    fn once_calls_once() {
        let once = Once::new();
        let mut count = 0;
        once.call_once(|| count += 1);
        once.call_once(|| count += 1);
        assert_eq!(count, 1);
    }

    #[test]
    fn thread_id_returns_value() {
        let id = current_thread_id();
        // On native: non-zero thread ID; on wasm: 0
        let _ = id;
    }

    #[test]
    fn mutex_default() {
        let m: Mutex<i32> = Mutex::default();
        assert_eq!(*m.lock(), 0);
    }

    #[test]
    fn mutex_into_inner() {
        let m = Mutex::new(vec![1, 2, 3]);
        let v = m.into_inner();
        assert_eq!(v, vec![1, 2, 3]);
    }

    #[test]
    fn condvar_notify_noop() {
        let cv = Condvar::new();
        cv.notify_one();
        cv.notify_all();
    }

    #[test]
    fn rwlock_into_inner() {
        let rw = RwLock::new(42);
        assert_eq!(rw.into_inner(), 42);
    }

    #[cfg(target_arch = "wasm32")]
    #[test]
    fn once_is_completed() {
        let once = Once::new();
        assert!(!once.is_completed());
        once.call_once(|| {});
        assert!(once.is_completed());
    }
}
