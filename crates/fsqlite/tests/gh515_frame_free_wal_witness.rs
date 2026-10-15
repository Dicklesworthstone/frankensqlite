#![cfg(feature = "native")]

use fsqlite::wal_recovery::{
    FrameFreeWalError, FrameFreeWalState, FrameFreeWalTransition, inspect_frame_free_wal,
    verify_frame_free_wal_transition,
};

const WAL_VERSION: u32 = 3_007_000;

fn header(page_size: u32, big_endian: bool, sequence: u32) -> [u8; 32] {
    let fields = [
        if big_endian { 0x377f_0683 } else { 0x377f_0682 },
        WAL_VERSION,
        page_size,
        sequence,
        0x1234_5678,
        0x9abc_def0,
    ];
    let mut bytes = [0_u8; 32];
    for (field, slot) in fields.iter().zip(bytes.chunks_exact_mut(4)) {
        slot.copy_from_slice(&field.to_be_bytes());
    }

    let mut a = 0_u32;
    let mut b = 0_u32;
    for pair in bytes[..24].chunks_exact(8) {
        let decode = |slice: &[u8]| {
            let word: [u8; 4] = slice.try_into().expect("four-byte WAL word");
            if big_endian {
                u32::from_be_bytes(word)
            } else {
                u32::from_le_bytes(word)
            }
        };
        a = a.wrapping_add(decode(&pair[..4])).wrapping_add(b);
        b = b.wrapping_add(decode(&pair[4..])).wrapping_add(a);
    }
    bytes[24..28].copy_from_slice(&a.to_be_bytes());
    bytes[28..32].copy_from_slice(&b.to_be_bytes());
    bytes
}

#[test]
fn public_facade_proves_only_zero_byte_to_header_binding() {
    let bytes = header(4096, false, 7);

    assert_eq!(
        inspect_frame_free_wal(None, 4096),
        Ok(FrameFreeWalState::Absent)
    );
    assert_eq!(
        inspect_frame_free_wal(Some(&[]), 4096),
        Ok(FrameFreeWalState::Empty)
    );

    let FrameFreeWalState::Header(proof) =
        inspect_frame_free_wal(Some(&bytes), 4096).expect("valid frame-free header")
    else {
        panic!("expected header witness");
    };
    assert_eq!(proof.as_bytes(), &bytes);
    assert_eq!(proof.page_size(), 4096);
    assert_eq!(proof.checkpoint_sequence(), 7);
    assert_eq!(proof.salts(), [0x1234_5678, 0x9abc_def0]);

    assert_eq!(
        verify_frame_free_wal_transition(Some(&[]), Some(&bytes), 4096),
        Ok(FrameFreeWalTransition::HeaderBound(proof))
    );
    assert_eq!(
        verify_frame_free_wal_transition(Some(&bytes), Some(&bytes), 4096),
        Ok(FrameFreeWalTransition::Unchanged)
    );
    assert_eq!(
        verify_frame_free_wal_transition(None, Some(&bytes), 4096),
        Err(FrameFreeWalError::NotGenerationBinding)
    );
}

#[test]
fn public_facade_never_treats_frame_bytes_as_generation_binding() {
    let header = header(4096, true, 3);
    let mut framed = header.to_vec();
    framed.extend_from_slice(&[0_u8; 24]);

    assert_eq!(
        inspect_frame_free_wal(Some(&framed), 4096),
        Err(FrameFreeWalError::FrameBytesPresent)
    );
    assert_eq!(
        verify_frame_free_wal_transition(Some(&[]), Some(&framed), 4096),
        Err(FrameFreeWalError::FrameBytesPresent)
    );
}

#[test]
fn public_facade_rejects_partial_or_wrong_page_size_headers() {
    let bytes = header(4096, false, 1);

    assert_eq!(
        inspect_frame_free_wal(Some(&bytes[..31]), 4096),
        Err(FrameFreeWalError::PartialHeader)
    );
    assert_eq!(
        inspect_frame_free_wal(Some(&bytes), 8192),
        Err(FrameFreeWalError::PageSizeMismatch)
    );
}
