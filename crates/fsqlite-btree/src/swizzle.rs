use std::sync::atomic::{AtomicU64, Ordering};

/// Bit tag for swizzled values.
pub const SWIZZLED_TAG: u64 = 0x1;
const PAGE_ID_SHIFT: u32 = 1;
/// Maximum page id encodable in the tagged representation.
pub const MAX_PAGE_ID: u64 = u64::MAX >> PAGE_ID_SHIFT;

/// Pointer state stored by [`SwizzlePtr`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwizzleState {
    /// On-disk reference.
    Unswizzled { page_id: u64 },
    /// In-memory direct pointer represented as an aligned frame address.
    Swizzled { frame_addr: u64 },
}

/// Page residency temperature for the HOT/COOLING/COLD protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageTemperature {
    Hot,
    Cooling,
    Cold,
}

/// Errors produced by swizzle operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwizzleError {
    /// The page id cannot be represented in 63 bits.
    PageIdOverflow { page_id: u64 },
    /// Swizzled addresses must keep bit 0 clear so it can hold the tag.
    FrameAddrUnaligned { frame_addr: u64 },
    /// Compare-and-swap failed because the slot no longer matched expected state.
    CompareExchangeFailed { expected: u64, observed: u64 },
    /// Invalid HOT/COOLING/COLD transition.
    InvalidTemperatureTransition {
        from: PageTemperature,
        to: PageTemperature,
    },
}

impl PageTemperature {
    /// Return true when `self -> next` is allowed by the protocol.
    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        if matches!(
            (self, next),
            (Self::Hot, Self::Hot) | (Self::Cooling, Self::Cooling) | (Self::Cold, Self::Cold)
        ) {
            return true;
        }

        matches!(
            (self, next),
            (Self::Hot, Self::Cooling)
                | (Self::Cooling | Self::Cold, Self::Hot)
                | (Self::Cooling, Self::Cold)
        )
    }

    /// Validate and apply a state transition.
    pub fn transition(self, next: Self) -> Result<Self, SwizzleError> {
        if self.can_transition_to(next) {
            Ok(next)
        } else {
            Err(SwizzleError::InvalidTemperatureTransition {
                from: self,
                to: next,
            })
        }
    }
}

/// Atomic tagged pointer for B-tree child references.
///
/// Encoding:
/// - `raw & 1 == 0`: unswizzled, page id stored as `raw >> 1`
/// - `raw & 1 == 1`: swizzled, frame address stored as `raw & !1`
#[derive(Debug)]
pub struct SwizzlePtr {
    raw: AtomicU64,
}

impl SwizzlePtr {
    /// Construct an unswizzled pointer.
    pub fn new_unswizzled(page_id: u64) -> Result<Self, SwizzleError> {
        Ok(Self {
            raw: AtomicU64::new(encode_unswizzled(page_id)?),
        })
    }

    /// Construct a swizzled pointer from a frame address.
    pub fn new_swizzled(frame_addr: u64) -> Result<Self, SwizzleError> {
        Ok(Self {
            raw: AtomicU64::new(encode_swizzled(frame_addr)?),
        })
    }

    /// Load the raw tagged word.
    #[must_use]
    pub fn load_raw(&self, ordering: Ordering) -> u64 {
        self.raw.load(ordering)
    }

    /// Decode the current state.
    #[must_use]
    pub fn state(&self, ordering: Ordering) -> SwizzleState {
        decode_state(self.load_raw(ordering))
    }

    /// Return true when this pointer is currently swizzled.
    #[must_use]
    pub fn is_swizzled(&self, ordering: Ordering) -> bool {
        self.load_raw(ordering) & SWIZZLED_TAG == SWIZZLED_TAG
    }

    /// Attempt to swizzle `expected_page_id -> frame_addr` atomically.
    pub fn try_swizzle(&self, expected_page_id: u64, frame_addr: u64) -> Result<(), SwizzleError> {
        let expected = encode_unswizzled(expected_page_id)?;
        let replacement = encode_swizzled(frame_addr)?;
        self.raw
            .compare_exchange(expected, replacement, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|observed| SwizzleError::CompareExchangeFailed { expected, observed })
    }

    /// Attempt to unswizzle `expected_frame_addr -> page_id` atomically.
    pub fn try_unswizzle(
        &self,
        expected_frame_addr: u64,
        page_id: u64,
    ) -> Result<(), SwizzleError> {
        let expected = encode_swizzled(expected_frame_addr)?;
        let replacement = encode_unswizzled(page_id)?;
        self.raw
            .compare_exchange(expected, replacement, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|observed| SwizzleError::CompareExchangeFailed { expected, observed })
    }
}

fn encode_unswizzled(page_id: u64) -> Result<u64, SwizzleError> {
    if page_id > MAX_PAGE_ID {
        return Err(SwizzleError::PageIdOverflow { page_id });
    }
    Ok(page_id << PAGE_ID_SHIFT)
}

fn encode_swizzled(frame_addr: u64) -> Result<u64, SwizzleError> {
    if frame_addr & SWIZZLED_TAG == SWIZZLED_TAG {
        return Err(SwizzleError::FrameAddrUnaligned { frame_addr });
    }
    Ok(frame_addr | SWIZZLED_TAG)
}

const fn decode_state(raw: u64) -> SwizzleState {
    if raw & SWIZZLED_TAG == SWIZZLED_TAG {
        return SwizzleState::Swizzled {
            frame_addr: raw & !SWIZZLED_TAG,
        };
    }
    SwizzleState::Unswizzled {
        page_id: raw >> PAGE_ID_SHIFT,
    }
}

// ── Swizzle Registry (bd-3ta.3) ─────────────────────────────────────────────

use std::collections::HashMap;
use std::sync::Mutex;

use crate::instrumentation::{
    record_swizzle_fault, record_swizzle_in, record_swizzle_out, set_swizzle_ratio,
};

/// Tracks the swizzle state of pages for buffer hot-path optimization.
///
/// The registry maintains a mapping from page IDs to their current swizzle
/// state, temperature, and frame address.  It coordinates with the
/// instrumentation layer to emit metrics and tracing spans.
///
/// Thread-safe: all operations are protected by a `Mutex`.
#[derive(Debug)]
pub struct SwizzleRegistry {
    /// Page ID → entry mapping.
    entries: Mutex<HashMap<u64, SwizzleEntry>>,
}

/// Per-page swizzle tracking entry.
#[derive(Debug, Clone, Copy)]
struct SwizzleEntry {
    /// Current temperature state.
    temperature: PageTemperature,
    /// Whether this page is currently swizzled (frame resident).
    swizzled: bool,
    /// Frame address if swizzled, 0 otherwise.
    frame_addr: u64,
}

impl SwizzleRegistry {
    /// Create an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// Register a page as tracked (initially unswizzled, cold).
    pub fn register_page(&self, page_id: u64) {
        let mut entries = self.entries.lock().expect("swizzle registry lock");
        entries.entry(page_id).or_insert(SwizzleEntry {
            temperature: PageTemperature::Cold,
            swizzled: false,
            frame_addr: 0,
        });
    }

    /// Attempt to swizzle a page (mark it as buffer-resident at `frame_addr`).
    ///
    /// Returns `true` if the swizzle succeeded, `false` if the page was
    /// already swizzled or not registered.
    pub fn try_swizzle(&self, page_id: u64, frame_addr: u64) -> bool {
        let mut entries = self.entries.lock().expect("swizzle registry lock");
        if let Some(entry) = entries.get_mut(&page_id) {
            if entry.swizzled {
                record_swizzle_fault();
                return false;
            }
            entry.swizzled = true;
            entry.frame_addr = frame_addr;
            entry.temperature = PageTemperature::Hot;
            drop(entries);
            record_swizzle_in(page_id);
            self.update_ratio();
            true
        } else {
            record_swizzle_fault();
            false
        }
    }

    /// Attempt to unswizzle a page (mark as evicted from buffer).
    ///
    /// Returns `true` if the unswizzle succeeded, `false` if the page was
    /// not swizzled or not registered.
    pub fn try_unswizzle(&self, page_id: u64) -> bool {
        let mut entries = self.entries.lock().expect("swizzle registry lock");
        if let Some(entry) = entries.get_mut(&page_id) {
            if !entry.swizzled {
                record_swizzle_fault();
                return false;
            }
            entry.swizzled = false;
            entry.frame_addr = 0;
            entry.temperature = PageTemperature::Cold;
            drop(entries);
            record_swizzle_out(page_id);
            self.update_ratio();
            true
        } else {
            record_swizzle_fault();
            false
        }
    }

    /// Check whether a page is currently swizzled.
    #[must_use]
    pub fn is_swizzled(&self, page_id: u64) -> bool {
        let entries = self.entries.lock().expect("swizzle registry lock");
        entries.get(&page_id).is_some_and(|entry| entry.swizzled)
    }

    /// Return the frame address for a swizzled page, or `None`.
    #[must_use]
    pub fn frame_addr(&self, page_id: u64) -> Option<u64> {
        let entries = self.entries.lock().expect("swizzle registry lock");
        entries.get(&page_id).and_then(|entry| {
            if entry.swizzled {
                Some(entry.frame_addr)
            } else {
                None
            }
        })
    }

    /// Number of tracked pages.
    #[must_use]
    pub fn tracked_count(&self) -> usize {
        self.entries.lock().expect("swizzle registry lock").len()
    }

    /// Number of currently swizzled pages.
    #[must_use]
    pub fn swizzled_count(&self) -> usize {
        self.entries
            .lock()
            .expect("swizzle registry lock")
            .values()
            .filter(|e| e.swizzled)
            .count()
    }

    /// Compute and update the global swizzle ratio gauge.
    fn update_ratio(&self) {
        let entries = self.entries.lock().expect("swizzle registry lock");
        let total = entries.len();
        if total == 0 {
            set_swizzle_ratio(0);
            return;
        }
        let swizzled = entries.values().filter(|e| e.swizzled).count();
        drop(entries);
        let ratio_milli = (swizzled as u64 * 1000) / total as u64;
        set_swizzle_ratio(ratio_milli);
    }
}

impl Default for SwizzleRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BEAD_ID: &str = "bd-2uza4.1";

    #[test]
    fn unswizzled_round_trips_page_id() {
        let ptr = SwizzlePtr::new_unswizzled(42).expect("page id should encode");
        assert_eq!(
            ptr.state(Ordering::Acquire),
            SwizzleState::Unswizzled { page_id: 42 },
            "bead_id={BEAD_ID} case=unswizzled_round_trip"
        );
        assert!(
            !ptr.is_swizzled(Ordering::Acquire),
            "bead_id={BEAD_ID} case=unswizzled_tag"
        );
    }

    #[test]
    fn swizzled_round_trips_frame_addr() {
        let ptr = SwizzlePtr::new_swizzled(0x1000).expect("aligned frame address should encode");
        assert_eq!(
            ptr.state(Ordering::Acquire),
            SwizzleState::Swizzled { frame_addr: 0x1000 },
            "bead_id={BEAD_ID} case=swizzled_round_trip"
        );
        assert!(
            ptr.is_swizzled(Ordering::Acquire),
            "bead_id={BEAD_ID} case=swizzled_tag"
        );
    }

    #[test]
    fn page_id_overflow_is_rejected() {
        let err = SwizzlePtr::new_unswizzled(MAX_PAGE_ID + 1).expect_err("must reject overflow");
        assert_eq!(
            err,
            SwizzleError::PageIdOverflow {
                page_id: MAX_PAGE_ID + 1,
            },
            "bead_id={BEAD_ID} case=page_id_overflow"
        );
    }

    #[test]
    fn unaligned_frame_address_is_rejected() {
        let err = SwizzlePtr::new_swizzled(0x1001).expect_err("must reject unaligned frame addr");
        assert_eq!(
            err,
            SwizzleError::FrameAddrUnaligned { frame_addr: 0x1001 },
            "bead_id={BEAD_ID} case=unaligned_frame_addr"
        );
    }

    #[test]
    fn try_swizzle_updates_atomically() {
        let ptr = SwizzlePtr::new_unswizzled(11).expect("page id should encode");
        ptr.try_swizzle(11, 0x2000)
            .expect("swizzle should succeed for expected state");
        assert_eq!(
            ptr.state(Ordering::Acquire),
            SwizzleState::Swizzled { frame_addr: 0x2000 },
            "bead_id={BEAD_ID} case=swizzle_success"
        );
    }

    #[test]
    fn try_swizzle_reports_observed_state_on_compare_exchange_failure() {
        let ptr = SwizzlePtr::new_unswizzled(11).expect("page id should encode");
        let err = ptr
            .try_swizzle(12, 0x2000)
            .expect_err("mismatched expected page id must fail");
        let expected = 12_u64 << PAGE_ID_SHIFT;
        let observed = 11_u64 << PAGE_ID_SHIFT;
        assert_eq!(
            err,
            SwizzleError::CompareExchangeFailed { expected, observed },
            "bead_id={BEAD_ID} case=swizzle_compare_exchange_failure"
        );
    }

    #[test]
    fn try_unswizzle_updates_atomically() {
        let ptr = SwizzlePtr::new_swizzled(0x4000).expect("aligned frame address should encode");
        ptr.try_unswizzle(0x4000, 77)
            .expect("unswizzle should succeed for expected state");
        assert_eq!(
            ptr.state(Ordering::Acquire),
            SwizzleState::Unswizzled { page_id: 77 },
            "bead_id={BEAD_ID} case=unswizzle_success"
        );
    }

    #[test]
    fn temperature_state_machine_transitions_match_design_contract() {
        assert!(
            PageTemperature::Hot
                .transition(PageTemperature::Cooling)
                .is_ok(),
            "bead_id={BEAD_ID} case=hot_to_cooling"
        );
        assert!(
            PageTemperature::Cooling
                .transition(PageTemperature::Cold)
                .is_ok(),
            "bead_id={BEAD_ID} case=cooling_to_cold"
        );
        assert!(
            PageTemperature::Cold
                .transition(PageTemperature::Hot)
                .is_ok(),
            "bead_id={BEAD_ID} case=cold_to_hot"
        );
        assert_eq!(
            PageTemperature::Hot
                .transition(PageTemperature::Cold)
                .expect_err("hot_to_cold must be invalid"),
            SwizzleError::InvalidTemperatureTransition {
                from: PageTemperature::Hot,
                to: PageTemperature::Cold,
            },
            "bead_id={BEAD_ID} case=reject_hot_to_cold"
        );
    }

    // ── SwizzleRegistry tests (bd-3ta.3) ────────────────────────────────

    const BEAD_REGISTRY: &str = "bd-3ta.3";

    #[test]
    fn registry_register_and_query() {
        let reg = SwizzleRegistry::new();
        reg.register_page(1);
        assert_eq!(
            reg.tracked_count(),
            1,
            "bead_id={BEAD_REGISTRY} case=tracked_count_after_register"
        );
        assert!(
            !reg.is_swizzled(1),
            "bead_id={BEAD_REGISTRY} case=newly_registered_not_swizzled"
        );
        assert_eq!(
            reg.frame_addr(1),
            None,
            "bead_id={BEAD_REGISTRY} case=no_frame_addr_when_unswizzled"
        );
    }

    #[test]
    fn registry_try_swizzle_success() {
        let reg = SwizzleRegistry::new();
        reg.register_page(10);
        assert!(
            reg.try_swizzle(10, 0x8000),
            "bead_id={BEAD_REGISTRY} case=swizzle_registered_page"
        );
        assert!(
            reg.is_swizzled(10),
            "bead_id={BEAD_REGISTRY} case=swizzled_after_try_swizzle"
        );
        assert_eq!(
            reg.frame_addr(10),
            Some(0x8000),
            "bead_id={BEAD_REGISTRY} case=frame_addr_after_swizzle"
        );
        assert_eq!(
            reg.swizzled_count(),
            1,
            "bead_id={BEAD_REGISTRY} case=swizzled_count_one"
        );
    }

    #[test]
    fn registry_double_swizzle_returns_false() {
        let reg = SwizzleRegistry::new();
        reg.register_page(20);
        assert!(reg.try_swizzle(20, 0x4000));
        assert!(
            !reg.try_swizzle(20, 0x6000),
            "bead_id={BEAD_REGISTRY} case=double_swizzle_rejected"
        );
        // Frame addr should remain at original value.
        assert_eq!(
            reg.frame_addr(20),
            Some(0x4000),
            "bead_id={BEAD_REGISTRY} case=frame_addr_unchanged_after_double_swizzle"
        );
    }

    #[test]
    fn registry_swizzle_unregistered_page_returns_false() {
        let reg = SwizzleRegistry::new();
        assert!(
            !reg.try_swizzle(999, 0x2000),
            "bead_id={BEAD_REGISTRY} case=swizzle_unregistered"
        );
    }

    #[test]
    fn registry_try_unswizzle_success() {
        let reg = SwizzleRegistry::new();
        reg.register_page(30);
        reg.try_swizzle(30, 0xA000);
        assert!(
            reg.try_unswizzle(30),
            "bead_id={BEAD_REGISTRY} case=unswizzle_success"
        );
        assert!(
            !reg.is_swizzled(30),
            "bead_id={BEAD_REGISTRY} case=not_swizzled_after_unswizzle"
        );
        assert_eq!(
            reg.frame_addr(30),
            None,
            "bead_id={BEAD_REGISTRY} case=no_frame_addr_after_unswizzle"
        );
        assert_eq!(
            reg.swizzled_count(),
            0,
            "bead_id={BEAD_REGISTRY} case=swizzled_count_zero_after_unswizzle"
        );
    }

    #[test]
    fn registry_unswizzle_already_cold_returns_false() {
        let reg = SwizzleRegistry::new();
        reg.register_page(40);
        assert!(
            !reg.try_unswizzle(40),
            "bead_id={BEAD_REGISTRY} case=unswizzle_cold_page"
        );
    }

    #[test]
    fn registry_unswizzle_unregistered_returns_false() {
        let reg = SwizzleRegistry::new();
        assert!(
            !reg.try_unswizzle(777),
            "bead_id={BEAD_REGISTRY} case=unswizzle_unregistered"
        );
    }

    #[test]
    fn registry_duplicate_register_is_idempotent() {
        let reg = SwizzleRegistry::new();
        reg.register_page(50);
        reg.try_swizzle(50, 0xC000);
        // Re-registering should not overwrite existing state.
        reg.register_page(50);
        assert!(
            reg.is_swizzled(50),
            "bead_id={BEAD_REGISTRY} case=duplicate_register_preserves_state"
        );
    }

    #[test]
    fn registry_swizzle_ratio_updates() {
        let reg = SwizzleRegistry::new();
        reg.register_page(1);
        reg.register_page(2);
        reg.register_page(3);
        reg.register_page(4);
        // 0/4 swizzled
        assert_eq!(reg.swizzled_count(), 0);
        reg.try_swizzle(1, 0x1000);
        reg.try_swizzle(2, 0x2000);
        // 2/4 = 500 milli
        assert_eq!(
            reg.swizzled_count(),
            2,
            "bead_id={BEAD_REGISTRY} case=swizzled_count_two_of_four"
        );
        reg.try_swizzle(3, 0x3000);
        reg.try_swizzle(4, 0x4000);
        // 4/4 = 1000 milli
        assert_eq!(
            reg.swizzled_count(),
            4,
            "bead_id={BEAD_REGISTRY} case=all_four_swizzled"
        );
    }

    #[test]
    fn registry_default_impl() {
        let reg = SwizzleRegistry::default();
        assert_eq!(
            reg.tracked_count(),
            0,
            "bead_id={BEAD_REGISTRY} case=default_empty"
        );
    }

    #[test]
    fn registry_swizzle_unswizzle_cycle() {
        let reg = SwizzleRegistry::new();
        reg.register_page(60);
        // Cold -> Hot (swizzle)
        assert!(reg.try_swizzle(60, 0xD000));
        assert!(reg.is_swizzled(60));
        // Hot -> Cold (unswizzle)
        assert!(reg.try_unswizzle(60));
        assert!(!reg.is_swizzled(60));
        // Cold -> Hot again (re-swizzle)
        assert!(
            reg.try_swizzle(60, 0xE000),
            "bead_id={BEAD_REGISTRY} case=re_swizzle_after_unswizzle"
        );
        assert_eq!(
            reg.frame_addr(60),
            Some(0xE000),
            "bead_id={BEAD_REGISTRY} case=new_frame_addr_after_re_swizzle"
        );
    }

    // ── Swizzle metrics tests (bd-3ta.3) ────────────────────────────────

    #[test]
    fn swizzle_metrics_appear_in_btree_snapshot() {
        use crate::instrumentation::{btree_metrics_snapshot, record_swizzle_in};
        let before = btree_metrics_snapshot();
        record_swizzle_in(42);
        let after = btree_metrics_snapshot();
        assert!(
            after.fsqlite_swizzle_in_total > before.fsqlite_swizzle_in_total,
            "bead_id={BEAD_REGISTRY} case=swizzle_in_metric_increments"
        );
    }

    #[test]
    fn swizzle_fault_metric_increments() {
        use crate::instrumentation::{btree_metrics_snapshot, record_swizzle_fault};
        let before = btree_metrics_snapshot();
        record_swizzle_fault();
        record_swizzle_fault();
        let after = btree_metrics_snapshot();
        assert!(
            after.fsqlite_swizzle_faults_total >= before.fsqlite_swizzle_faults_total + 2,
            "bead_id={BEAD_REGISTRY} case=swizzle_fault_metric"
        );
    }
}
