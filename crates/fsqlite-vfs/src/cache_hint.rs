//! Stable, best-effort CPU cache hints for the storage engine.
//!
//! Keep the architecture-specific unsafe call at the VFS boundary so the
//! pager and B-tree can continue to forbid unsafe code.

/// Hint that the cache line containing `ptr` will be read soon.
///
/// On x86_64 this emits a temporal T0 prefetch (locality 3). Other targets
/// retain the no-op fallback. The hint never dereferences the pointer and
/// does not require it to be aligned, live, or non-null.
#[inline]
pub fn prefetch_l1_read<T>(ptr: *const T) {
    #[cfg(target_arch = "x86_64")]
    {
        use core::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};

        if ptr.is_null() {
            return;
        }

        // SAFETY: SSE is part of the x86_64 baseline. `_mm_prefetch` is only
        // a cache hint: it does not load from the pointer, require alignment,
        // or fault for an invalid address. T0 preserves the previous
        // prefetch_read_data::<T, 3> read/locality/data-cache behavior.
        unsafe { _mm_prefetch::<_MM_HINT_T0>(ptr.cast::<i8>()) };
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = ptr;
}

#[cfg(test)]
mod tests {
    use super::prefetch_l1_read;
    use std::hint::black_box;

    #[test]
    fn prefetch_accepts_null_pointers() {
        prefetch_l1_read(std::ptr::null::<u8>());
        prefetch_l1_read(std::ptr::null::<u64>());
    }

    #[test]
    fn prefetch_accepts_live_and_unaligned_addresses() {
        let bytes = [0xA5_u8; 65];
        let metadata = 0x0123_4567_89AB_CDEF_u64;
        prefetch_l1_read(black_box(bytes.as_ptr()));
        prefetch_l1_read(black_box(bytes.as_ptr().wrapping_add(1)));
        prefetch_l1_read(black_box(std::ptr::from_ref(&metadata)));
    }

    #[test]
    fn prefetch_accepts_retired_addresses() {
        let bytes = Box::new([0x5A_u8; 64]);
        let ptr = black_box(bytes.as_ptr());
        drop(bytes);
        // Cache entries can be evicted between computing and issuing a hint.
        prefetch_l1_read(ptr);
    }
}
