//! Public sender regression for the bd-3mgq5.1 / MTDT build dependency seam.
//! This proves wire-symbol encoding and restart, not snapshot loss recovery.

use fsqlite_core::replication_sender::{
    PageEntry, SenderConfig, compute_changeset_id, derive_seed_from_changeset_id, encode_changeset,
};
use fsqlite_core::snapshot_shipping::SnapshotSender;
use fsqlite_error::FrankenError;
use fsqlite_types::cx::Cx;

fn pages() -> Vec<PageEntry> {
    (1..=3)
        .map(|page| PageEntry::new(page, vec![u8::try_from(page).unwrap(); 64]))
        .collect()
}

#[test]
fn zero_symbol_size_refuses_before_division() {
    let error = SnapshotSender::prepare(
        64,
        &mut pages(),
        SenderConfig {
            symbol_size: 0,
            max_isi_multiplier: 2,
        },
    )
    .expect_err("invalid transport parameters");
    assert!(matches!(error, FrankenError::OutOfRange { .. }));
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn first_repair_uses_wire_esi_and_restart_replays() {
    use asupersync::raptorq::systematic::SystematicEncoder;
    let mut pages = pages();
    let source = encode_changeset(64, &mut pages).expect("source changeset");
    let k = u32::try_from(source.len().div_ceil(64)).expect("source count fits");
    assert!(k > 1);
    let source_symbols: Vec<_> = source
        .chunks(64)
        .map(|chunk| {
            let mut symbol = vec![0; 64];
            symbol[..chunk.len()].copy_from_slice(chunk);
            symbol
        })
        .collect();
    let seed = derive_seed_from_changeset_id(&compute_changeset_id(&source));
    let oracle = SystematicEncoder::new(&source_symbols, 64, seed).expect("codec");
    // The retired isi-K implementation asks for source ESI zero as a repair.
    assert!(oracle.try_repair_symbol(0).is_err());
    let mut sender = SnapshotSender::prepare(
        64,
        &mut pages,
        SenderConfig {
            symbol_size: 64,
            max_isi_multiplier: 2,
        },
    )
    .expect("prepare");
    let cx = Cx::new();
    let mut first_pass = Vec::new();
    while let Some(packet) = sender.next_packet(&cx).expect("encode") {
        assert_eq!(packet.k_source, k);
        assert_eq!(
            packet.esi,
            u32::try_from(first_pass.len()).expect("ESI fits")
        );
        if packet.esi < k {
            assert_eq!(
                packet.symbol_data,
                source_symbols[usize::try_from(packet.esi).unwrap()]
            );
        } else {
            assert_eq!(
                packet.symbol_data,
                oracle.try_repair_symbol(packet.esi).expect("repair")
            );
        }
        first_pass.push(packet.to_bytes().expect("wire packet"));
    }
    assert_eq!(first_pass.len(), usize::try_from(2 * k).unwrap());
    sender.restart();
    let mut replay = Vec::new();
    while let Some(packet) = sender.next_packet(&cx).expect("re-encode") {
        replay.push(packet.to_bytes().expect("wire packet"));
    }
    assert_eq!(replay, first_pass);
}

#[test]
fn cancellation_refuses_without_consuming_a_packet() {
    let mut sender = SnapshotSender::prepare(64, &mut pages(), SenderConfig::default()).unwrap();
    let cancelled = Cx::new();
    cancelled.cancel();
    assert!(matches!(
        sender.next_packet(&cancelled),
        Err(FrankenError::Abort)
    ));
    let first = sender
        .next_packet(&Cx::new())
        .unwrap()
        .expect("first source packet");
    assert_eq!(first.esi, 0);
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn unsupported_repair_parameters_never_emit_a_placeholder() {
    // A real one-page snapshot with one-byte transport symbols exceeds the
    // codec's supported K. Systematic extraction is cheap; codec admission
    // must refuse before allocating its matrix or returning a fake repair.
    let mut pages = vec![PageEntry::new(1, vec![7; 65_536])];
    let mut sender = SnapshotSender::prepare(
        65_536,
        &mut pages,
        SenderConfig {
            symbol_size: 1,
            max_isi_multiplier: 2,
        },
    )
    .expect("systematic snapshot");
    let k = sender.total_source_symbols();
    assert!((56_404..70_000).contains(&k));
    let cx = Cx::new();
    for esi in 0..k {
        let packet = sender
            .next_packet(&cx)
            .expect("systematic encoding")
            .expect("source");
        assert_eq!(u64::from(packet.esi), esi);
        assert!(packet.is_source_symbol());
    }
    let first = sender
        .next_packet(&cx)
        .expect_err("unsupported repair must refuse");
    let second = sender
        .next_packet(&cx)
        .expect_err("failed repair must remain unconsumed");
    assert_eq!(first.to_string(), second.to_string());
    assert!(matches!(first, FrankenError::OutOfRange { .. }));
}
