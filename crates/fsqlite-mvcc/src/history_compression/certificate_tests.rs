use super::*;
use fsqlite_types::{RowId, TableId};

fn intent() -> IntentOp {
    IntentOp {
        schema_epoch: 7,
        footprint: IntentFootprint::empty(),
        op: IntentOpKind::Insert {
            table: TableId::new(1),
            key: RowId::new(1),
            record: vec![1, 2, 3],
        },
    }
}

fn pages() -> Vec<(PageNumber, Vec<u8>)> {
    vec![
        (PageNumber::new(3).unwrap(), vec![0xA5; 512]),
        (PageNumber::new(9).unwrap(), vec![0x5A; 512]),
    ]
}

fn context() -> MergeCertificateContext {
    MergeCertificateContext {
        merge_kind: MergeKind::RebaseAndPatch,
        base_commit_seq: 41,
        schema_epoch: 7,
    }
}

fn certificate() -> MergeCertificate {
    generate_merge_certificate(
        context().merge_kind,
        context().base_commit_seq,
        context().schema_epoch,
        &[intent()],
        &pages(),
        [0x42; 16],
    )
    .unwrap()
}

fn verify(certificate: &MergeCertificate) -> Result<(), CertificateVerificationError> {
    verify_merge_certificate(&[intent()], &pages(), [0x42; 16], certificate)
}

#[test]
fn missing_all_or_one_page_hash_never_verifies() {
    let mut cert = certificate();
    cert.post_state.page_hashes.clear();
    assert_eq!(verify(&cert), Err(CertificateVerificationError::PageSetMismatch));
    let mut cert = certificate();
    cert.post_state.page_hashes.pop();
    assert_eq!(verify(&cert), Err(CertificateVerificationError::PageSetMismatch));
}

#[test]
fn omitting_a_page_from_both_manifests_does_not_hide_replay_output() {
    let mut cert = certificate();
    cert.pages.pop();
    cert.post_state.page_hashes.pop();
    assert_eq!(verify(&cert), Err(CertificateVerificationError::PageSetMismatch));
}

#[test]
fn declared_manifest_is_checked_not_just_hash_entries() {
    for replacement in [vec![], vec![PageNumber::ONE]] {
        let mut cert = certificate();
        cert.pages = replacement;
        assert_eq!(verify(&cert), Err(CertificateVerificationError::PageSetMismatch));
    }
}

#[test]
fn missing_actual_page_is_not_a_zero_hash() {
    let mut cert = certificate();
    cert.post_state.page_hashes[1].1 = [0; 16];
    let actual = pages();
    assert_eq!(
        verify_merge_certificate(&[intent()], &actual[..1], [0x42; 16], &cert),
        Err(CertificateVerificationError::PageSetMismatch)
    );
}

#[test]
fn extra_replay_page_is_rejected_even_when_certified_pages_match() {
    let mut actual = pages();
    actual.push((PageNumber::ONE, vec![0; 512]));
    assert_eq!(
        verify_merge_certificate(&[intent()], &actual, [0x42; 16], &certificate()),
        Err(CertificateVerificationError::PageSetMismatch)
    );
}

#[test]
fn duplicate_manifest_and_hash_entries_are_rejected() {
    let mut cert = certificate();
    let page = cert.pages[0];
    cert.pages.push(page);
    assert_eq!(verify(&cert), Err(CertificateVerificationError::DuplicatePage { page }));
    let mut cert = certificate();
    cert.post_state.page_hashes.push(cert.post_state.page_hashes[0]);
    assert_eq!(verify(&cert), Err(CertificateVerificationError::DuplicatePage { page }));
}

#[test]
fn duplicate_replay_page_is_never_collapsed_by_a_map() {
    for different in [false, true] {
        let mut actual = pages();
        let mut duplicate = actual[0].clone();
        if different {
            duplicate.1[0] ^= 1;
        }
        let page = duplicate.0;
        actual.insert(0, duplicate);
        assert_eq!(
            verify_merge_certificate(&[intent()], &actual, [0x42; 16], &certificate()),
            Err(CertificateVerificationError::DuplicatePage { page })
        );
    }
}

#[test]
fn generation_refuses_duplicate_pages_instead_of_signing_ambiguous_evidence() {
    let mut actual = pages();
    let page = actual[0].0;
    actual.push(actual[0].clone());
    assert_eq!(
        generate_merge_certificate(MergeKind::Rebase, 41, 7, &[intent()], &actual, [0; 16]),
        Err(CertificateVerificationError::DuplicatePage { page })
    );
}

#[test]
fn page_order_is_not_part_of_manifest_identity() {
    let mut cert = certificate();
    cert.pages.reverse();
    assert!(verify(&cert).is_ok());
    cert.post_state.page_hashes.reverse();
    assert!(verify(&cert).is_ok());
    let mut actual = pages();
    actual.reverse();
    assert!(verify_merge_certificate(&[intent()], &actual, [0x42; 16], &cert).is_ok());
}

#[test]
fn unsupported_algorithm_versions_fail_before_other_evidence() {
    for actual in [0, VERIFIER_VERSION + 1, u32::MAX] {
        let mut cert = certificate();
        cert.verifier_version = actual;
        assert_eq!(
            verify(&cert),
            Err(CertificateVerificationError::UnsupportedVerifierVersion {
                expected: VERIFIER_VERSION,
                actual,
            })
        );
    }
}

#[test]
fn generation_and_verification_bind_every_intent_schema_epoch() {
    let mut other = intent();
    other.schema_epoch = 8;
    let expected = CertificateVerificationError::SchemaEpochMismatch { expected: 7, actual: 8 };
    assert_eq!(
        generate_merge_certificate(MergeKind::Rebase, 41, 7, &[intent(), other.clone()], &pages(), [0; 16]),
        Err(expected.clone())
    );
    assert_eq!(
        verify_merge_certificate(&[intent(), other], &pages(), [0x42; 16], &certificate()),
        Err(expected)
    );
    let mut cert = certificate();
    cert.schema_epoch = 9;
    assert_eq!(verify(&cert), Err(CertificateVerificationError::SchemaEpochMismatch { expected: 9, actual: 7 }));
}

#[test]
fn context_binds_merge_kind_base_and_schema_including_empty_intents() {
    for ops in [vec![], vec![intent()]] {
        let expected = context();
        let cert = generate_merge_certificate(expected.merge_kind, expected.base_commit_seq,
            expected.schema_epoch, &ops, &pages(), [0x42; 16]).unwrap();
        assert!(cert.verify_in_context(expected, &ops, &pages(), [0x42; 16]).is_ok());
        for wrong in [
            MergeCertificateContext { merge_kind: MergeKind::Rebase, ..expected },
            MergeCertificateContext { base_commit_seq: 42, ..expected },
            MergeCertificateContext { schema_epoch: 8, ..expected },
        ] {
            assert_eq!(cert.verify_in_context(wrong, &ops, &pages(), [0x42; 16]),
                Err(CertificateVerificationError::ContextMismatch));
        }
    }
}

#[test]
fn corrupt_page_and_invariant_hashes_still_fail_after_manifest_validation() {
    let mut cert = certificate();
    cert.post_state.page_hashes[0].1[0] ^= 1;
    assert!(matches!(verify(&cert), Err(CertificateVerificationError::PageHashMismatch { .. })));
    let mut cert = certificate();
    cert.post_state.btree_invariant_hash[0] ^= 1;
    let error = verify(&cert).unwrap_err();
    assert!(matches!(error, CertificateVerificationError::BtreeInvariantHashMismatch { .. }));
    assert!(circuit_breaker_check(error, &cert).disable_safe_merge);
}

#[test]
fn empty_evidence_is_only_a_consistent_no_op() {
    let cert = generate_merge_certificate(MergeKind::Rebase, 41, 7, &[], &[], [0; 16]).unwrap();
    assert!(verify_merge_certificate(&[], &[], [0; 16], &cert).is_ok());
    assert_eq!(verify_merge_certificate(&[], &pages(), [0; 16], &cert),
        Err(CertificateVerificationError::PageSetMismatch));
}

#[test]
fn v1_certificates_require_regeneration_after_the_digest_upgrade() {
    let mut cert = certificate();
    assert_eq!(cert.verifier_version, 2);
    assert!(verify(&cert).is_ok());
    cert.verifier_version = 1;
    assert_eq!(
        verify(&cert),
        Err(CertificateVerificationError::UnsupportedVerifierVersion {
            expected: VERIFIER_VERSION,
            actual: 1,
        })
    );
}

#[test]
fn proof_digests_bind_semantic_key_routing_metadata() {
    use fsqlite_types::IndexId;

    let btree = BtreeRef::Table(TableId::new(1));
    let key = SemanticKeyRef {
        btree,
        kind: SemanticKeyKind::TableRow,
        key_digest: table_row_key_digest(btree, 1),
    };
    let mut baseline = intent();
    baseline.footprint.reads.push(key.clone());
    baseline.footprint.writes.push(key);
    let expected_op = compute_op_digest(&baseline);
    let expected_footprint = compute_footprint_digest(&[&baseline.footprint]);
    let cert = generate_merge_certificate(
        context().merge_kind,
        context().base_commit_seq,
        context().schema_epoch,
        &[baseline.clone()],
        &pages(),
        [0x42; 16],
    )
    .unwrap();
    assert!(cert.verify_in_context(context(), &[baseline.clone()], &pages(), [0x42; 16]).is_ok());
    for source in 0..2 {
        for variant in 0..3 {
            let mut changed = baseline.clone();
            let key = if source == 0 {
                &mut changed.footprint.reads[0]
            } else {
                &mut changed.footprint.writes[0]
            };
            let unchanged_digest = key.key_digest;
            match variant {
                0 => key.kind = SemanticKeyKind::IndexEntry,
                1 => key.btree = BtreeRef::Index(IndexId::new(1)),
                _ => key.btree = BtreeRef::Table(TableId::new(99)),
            }
            assert_eq!(key.key_digest, unchanged_digest);
            assert_ne!(compute_op_digest(&changed), expected_op);
            assert_ne!(compute_footprint_digest(&[&changed.footprint]), expected_footprint);
            assert!(matches!(
                cert.verify_in_context(context(), &[changed], &pages(), [0x42; 16]),
                Err(CertificateVerificationError::OpDigestMismatch { .. })
            ));
        }
    }
}

#[test]
fn complete_key_encoding_has_an_explicit_namespace_and_fixed_width() {
    let key = SemanticKeyRef {
        kind: SemanticKeyKind::IndexEntry,
        btree: BtreeRef::Index(fsqlite_types::IndexId::new(9)),
        key_digest: [0xAB; 16],
    };
    let encoded = canonical_semantic_key_bytes(&key);
    assert_eq!(&encoded[..6], &[1, 1, 9, 0, 0, 0]);
    assert_eq!(&encoded[6..], &[0xAB; 16]);
}

#[test]
fn circuit_breaker_identity_binds_all_certificate_fields() {
    let original = certificate();
    let digest = |cert: &MergeCertificate| {
        let event = circuit_breaker_check(CertificateVerificationError::InvalidNormalForm, cert);
        assert!(event.disable_safe_merge);
        event.certificate_digest
    };
    let expected = digest(&original);
    for field in 0..11 {
        let mut changed = original.clone();
        match field {
            0 => changed.verifier_version += 1,
            1 => changed.merge_kind = MergeKind::StructuredPatch,
            2 => changed.base_commit_seq += 1,
            3 => changed.schema_epoch += 1,
            4 => { let _ = changed.pages.pop(); }
            5 => changed.intent_op_digests[0][0] ^= 1,
            6 => changed.footprint_digest[0] ^= 1,
            7 => changed.normal_form[0][0] ^= 1,
            8 => changed.post_state.page_hashes[0].1[0] ^= 1,
            9 => changed.post_state.btree_invariant_hash[0] ^= 1,
            _ => changed.post_state.page_hashes[0].0 = PageNumber::ONE,
        }
        assert_ne!(digest(&changed), expected);
    }
}
