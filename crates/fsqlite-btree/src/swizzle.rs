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
            (Self::Hot, Self::Hot)
                | (Self::Cooling, Self::Cooling)
                | (Self::Cold, Self::Cold)
        ) {
            return true;
        }

        matches!(
            (self, next),
            (Self::Hot, Self::Cooling)
                | (Self::Cooling, Self::Hot)
                | (Self::Cooling, Self::Cold)
                | (Self::Cold, Self::Hot)
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
}
