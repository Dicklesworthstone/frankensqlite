//! Real sender/receiver packet paths, including decoded-result backpressure.
//! These tests do not claim database application or durable replication.

use fsqlite_core::replication_receiver::{
    PacketResult, ReceiverConfig, ReceiverState, ReplicationReceiver,
};
use fsqlite_core::replication_sender::{
    CHANGESET_HEADER_SIZE, ChangesetId, PageEntry, ReplicationPacket, ReplicationPacketV2Header,
    ReplicationSender, SenderConfig, compute_changeset_id, derive_seed_from_changeset_id,
    encode_changeset,
};
use fsqlite_error::FrankenError;
use fsqlite_types::cx::Cx;

fn packets(page: u32, copies: u32) -> Vec<Vec<u8>> {
    let mut pages = vec![
        PageEntry::new(page, vec![0x5a; 128]),
        PageEntry::new(page + 1, vec![0xa5; 128]),
    ];
    let mut sender = ReplicationSender::new();
    sender
        .prepare(
            128,
            &mut pages,
            SenderConfig {
                symbol_size: 128,
                max_isi_multiplier: copies,
            },
        )
        .expect("prepare");
    sender.start_streaming().expect("start");
    let mut packets = Vec::new();
    while let Some(packet) = sender.next_packet(&Cx::new()).expect("next packet") {
        packets.push(packet.to_bytes().expect("wire"));
    }
    packets
}

fn receive_source(receiver: &mut ReplicationReceiver, packets: &[Vec<u8>]) {
    let mut ready = 0;
    for wire in packets {
        if receiver
            .process_packet(&Cx::new(), wire)
            .expect("source packet")
            == PacketResult::DecodeReady
        {
            ready += 1;
        }
    }
    assert_eq!(ready, 1);
}

#[test]
fn pending_pages_remain_charged_and_rejected_symbol_can_retry_after_drain() {
    let a = packets(1, 1);
    let b = packets(10, 1);
    assert_eq!((a.len(), b.len()), (3, 3));
    let mut receiver = ReplicationReceiver::with_config(ReceiverConfig {
        max_buffered_symbol_bytes: 384,
        ..ReceiverConfig::default()
    });
    receive_source(&mut receiver, &a);
    assert_eq!(receiver.buffered_payload_bytes(), 256);
    assert_eq!(receiver.pending_changesets(), 1);
    assert_eq!(
        receiver
            .process_packet(&Cx::new(), &b[0])
            .expect("one peer symbol fits"),
        PacketResult::Accepted
    );
    assert_eq!(receiver.buffered_payload_bytes(), 384);
    assert!(matches!(
        receiver.process_packet(&Cx::new(), &b[1]),
        Err(FrankenError::TooBig)
    ));
    assert_eq!(receiver.buffered_payload_bytes(), 384);
    assert_eq!(receiver.state(), ReceiverState::Applying);
    let first = receiver.apply_pending().expect("A remains ready");
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].pages[0].page_number, 1);
    assert_eq!(receiver.buffered_payload_bytes(), 128);
    assert_eq!(receiver.state(), ReceiverState::Collecting);
    assert_eq!(
        receiver
            .process_packet(&Cx::new(), &b[1])
            .expect("retry rejected symbol"),
        PacketResult::Accepted
    );
    assert_eq!(
        receiver
            .process_packet(&Cx::new(), &b[2])
            .expect("finish B"),
        PacketResult::DecodeReady
    );
    assert_eq!(receiver.buffered_payload_bytes(), 256);
    let second = receiver.apply_pending().expect("B");
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].pages[0].page_number, 10);
    assert_eq!(receiver.buffered_payload_bytes(), 0);
    assert_eq!(receiver.state(), ReceiverState::Complete);
}

#[test]
fn decoded_changeset_keeps_its_slot_until_handoff() {
    let a = packets(1, 1);
    let b = packets(10, 1);
    let mut receiver = ReplicationReceiver::with_config(ReceiverConfig {
        max_inflight_decoders: 1,
        ..ReceiverConfig::default()
    });
    receive_source(&mut receiver, &a);
    assert_eq!(receiver.active_decoders(), 0);
    assert!(matches!(
        receiver.process_packet(&Cx::new(), &b[0]),
        Err(FrankenError::Busy)
    ));
    assert_eq!(receiver.pending_changesets(), 1);
    assert_eq!(receiver.state(), ReceiverState::Applying);
    let a = receiver.apply_pending().expect("handoff A");
    assert_eq!(a[0].pages[1].page_number, 2);
    receive_source(&mut receiver, &b);
    let b = receiver.apply_pending().expect("handoff B");
    assert_eq!(b[0].pages[1].page_number, 11);
    assert_eq!(receiver.buffered_payload_bytes(), 0);
}

#[test]
fn surplus_source_and_repair_packets_do_not_redecode_a_queued_object() {
    let wires = packets(1, 4);
    let mut receiver = ReplicationReceiver::with_config(ReceiverConfig {
        max_inflight_decoders: 1,
        ..ReceiverConfig::default()
    });
    let first = ReplicationPacket::from_bytes(&wires[0]).expect("packet");
    let k = usize::try_from(first.k_source).expect("K");
    assert!(wires.len() > k, "surplus repairs must actually be delivered");
    receive_source(&mut receiver, &wires[..k]);
    for wire in wires.iter().cycle().take(wires.len() * 3) {
        let packet = ReplicationPacket::from_bytes(wire).expect("packet");
        assert_eq!(
            receiver
                .process_packet(&Cx::new(), wire)
                .expect("late wire packet"),
            PacketResult::Duplicate
        );
        assert_eq!(
            receiver
                .process_parsed_packet(&Cx::new(), &packet)
                .expect("late parsed packet"),
            PacketResult::Duplicate
        );
        assert_eq!(receiver.pending_changesets(), 1);
        assert_eq!(receiver.active_decoders(), 0);
        assert_eq!(receiver.buffered_payload_bytes(), 256);
    }
    // Duplicate suppression must never skip authentication/integrity checks.
    let mut damaged = first.clone();
    damaged.symbol_data[0] ^= 1;
    assert_eq!(
        receiver
            .process_parsed_packet(&Cx::new(), &damaged)
            .expect("erasure"),
        PacketResult::Erasure
    );
    let mut invalid_seed = first;
    invalid_seed.seed ^= 1;
    assert!(matches!(
        receiver.process_parsed_packet(&Cx::new(), &invalid_seed),
        Err(FrankenError::DatabaseCorrupt { .. })
    ));
    assert_eq!(receiver.pending_changesets(), 1);
    assert_eq!(receiver.apply_pending().expect("one handoff").len(), 1);
    assert!(receiver.apply_pending().is_err());
    assert_eq!(receiver.buffered_payload_bytes(), 0);
}

#[test]
fn peer_validation_failure_cannot_hide_already_ready_work() {
    let a = packets(1, 1);
    let mut b: Vec<_> = packets(10, 1)
        .iter()
        .map(|wire| ReplicationPacket::from_bytes(wire).expect("packet"))
        .collect();
    // Keep every packet's payload integrity valid, but use a changeset ID that
    // cannot match the assembled bytes. This reaches changeset validation.
    let id = ChangesetId::from_bytes([0x33; 16]);
    for packet in &mut b {
        packet.changeset_id = id;
        packet.seed = derive_seed_from_changeset_id(&id);
    }
    let mut receiver = ReplicationReceiver::new();
    receive_source(&mut receiver, &a);
    for packet in &b[..b.len() - 1] {
        assert_eq!(
            receiver
                .process_parsed_packet(&Cx::new(), packet)
                .expect("partial peer"),
            PacketResult::Accepted
        );
        assert_eq!(receiver.state(), ReceiverState::Applying);
    }
    assert!(matches!(
        receiver.process_parsed_packet(&Cx::new(), b.last().expect("last")),
        Err(FrankenError::DatabaseCorrupt { .. })
    ));
    assert_eq!(receiver.pending_changesets(), 1);
    assert_eq!(receiver.active_decoders(), 0);
    assert_eq!(receiver.buffered_payload_bytes(), 256);
    assert_eq!(receiver.state(), ReceiverState::Applying);
    let result = receiver.apply_pending().expect("A survives");
    assert_eq!(result[0].pages[0].page_number, 1);
}

#[test]
fn force_reset_releases_collecting_and_decoded_payload_budgets() {
    let a = packets(1, 1);
    let b = packets(10, 1);
    let mut receiver = ReplicationReceiver::with_config(ReceiverConfig {
        max_buffered_symbol_bytes: 384,
        ..ReceiverConfig::default()
    });
    receive_source(&mut receiver, &a);
    receiver
        .process_packet(&Cx::new(), &b[0])
        .expect("partial B");
    assert_eq!(receiver.buffered_payload_bytes(), 384);
    receiver.force_reset();
    assert_eq!(receiver.buffered_payload_bytes(), 0);
    assert_eq!(receiver.pending_changesets(), 0);
    assert_eq!(receiver.active_decoders(), 0);
    assert_eq!(receiver.state(), ReceiverState::Listening);
    receive_source(&mut receiver, &b);
    let result = receiver.apply_pending().expect("new transfer");
    assert_eq!(result[0].pages[0].page_number, 10);
}

#[test]
fn self_consistent_changesets_with_invalid_page_ownership_are_refused() {
    for invalid_page in [0_u32, 1, u32::MAX] {
        let mut pages = vec![
            PageEntry::new(1, vec![0x5a; 128]),
            PageEntry::new(2, vec![0xa5; 128]),
        ];
        let mut encoded = encode_changeset(128, &mut pages).expect("encode");
        // Change page 2 to zero, the previous page (duplicate ownership), or
        // an out-of-domain page. Recompute the object ID, leaving page hashes
        // valid so refusal must come from the decoded ownership checks.
        let offset = CHANGESET_HEADER_SIZE + 12 + 128;
        encoded[offset..offset + 4].copy_from_slice(&invalid_page.to_le_bytes());
        let id = compute_changeset_id(&encoded);
        let k_source = u32::try_from(encoded.len().div_ceil(128)).expect("K");
        let mut receiver = ReplicationReceiver::new();
        for (index, chunk) in encoded.chunks(128).enumerate() {
            let mut symbol = vec![0; 128];
            symbol[..chunk.len()].copy_from_slice(chunk);
            let esi = u32::try_from(index).expect("ESI");
            let packet = ReplicationPacket::new_v2(
                ReplicationPacketV2Header {
                    changeset_id: id,
                    sbn: 0,
                    esi,
                    k_source,
                    r_repair: 0,
                    symbol_size_t: 128,
                    seed: derive_seed_from_changeset_id(&id),
                },
                symbol,
            );
            let result = receiver.process_packet(&Cx::new(), &packet.to_bytes().expect("wire"));
            if esi + 1 == k_source {
                assert!(matches!(result, Err(FrankenError::DatabaseCorrupt { .. })));
            } else {
                assert_eq!(result.expect("source admitted"), PacketResult::Accepted);
            }
        }
        assert_eq!(receiver.pending_changesets(), 0);
        assert_eq!(receiver.active_decoders(), 0);
        assert_eq!(receiver.buffered_payload_bytes(), 0);
        assert_eq!(receiver.state(), ReceiverState::Listening);
        assert!(receiver.decode_audit_entries().is_empty());
        receive_source(&mut receiver, &packets(1, 1));
        assert_eq!(receiver.apply_pending().expect("valid retry")[0].pages.len(), 2);
    }
}
