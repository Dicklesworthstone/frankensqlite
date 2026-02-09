use std::fs;

use fsqlite_types::{ObjectId, Oti, SymbolRecord, SymbolRecordFlags};
use fsqlite_wal::{
    WAL_FEC_GROUP_META_MAGIC, WAL_FEC_GROUP_META_VERSION, WalFecGroupId, WalFecGroupMeta,
    WalFecGroupMetaInit, WalFecGroupRecord, append_wal_fec_group, build_source_page_hashes,
    ensure_wal_with_fec_sidecar, find_wal_fec_group, scan_wal_fec, wal_fec_path_for_wal,
};
use tempfile::tempdir;

const PAGE_SIZE: u32 = 4096;

fn sample_payload(seed: u8) -> Vec<u8> {
    let mut payload = vec![0u8; usize::try_from(PAGE_SIZE).expect("PAGE_SIZE must fit usize")];
    for (idx, byte) in payload.iter_mut().enumerate() {
        let index_mod = u8::try_from(idx % 251).expect("modulo result should fit in u8");
        *byte = index_mod ^ seed;
    }
    payload
}

fn sample_page_payloads(k_source: u32, seed_base: u8) -> Vec<Vec<u8>> {
    (0..k_source)
        .map(|index| {
            let seed = seed_base.wrapping_add(u8::try_from(index).expect("index should fit in u8"));
            sample_payload(seed)
        })
        .collect()
}

#[derive(Clone, Copy)]
struct SampleMetaSpec<'a> {
    start_frame_no: u32,
    k_source: u32,
    r_repair: u32,
    wal_salt1: u32,
    wal_salt2: u32,
    object_tag: &'a [u8],
    seed_base: u8,
    db_size_pages: u32,
}

fn sample_meta(spec: SampleMetaSpec<'_>) -> WalFecGroupMeta {
    let end_frame_no = spec.start_frame_no + (spec.k_source - 1);
    let page_payloads = sample_page_payloads(spec.k_source, spec.seed_base);
    let source_hashes = build_source_page_hashes(&page_payloads);
    let page_numbers = (0..spec.k_source)
        .map(|index| index + 7)
        .collect::<Vec<_>>();
    let object_id = ObjectId::derive_from_canonical_bytes(spec.object_tag);
    let oti = Oti {
        f: u64::from(spec.k_source) * u64::from(PAGE_SIZE),
        al: 1,
        t: PAGE_SIZE,
        z: 1,
        n: 1,
    };

    WalFecGroupMeta::from_init(WalFecGroupMetaInit {
        wal_salt1: spec.wal_salt1,
        wal_salt2: spec.wal_salt2,
        start_frame_no: spec.start_frame_no,
        end_frame_no,
        db_size_pages: spec.db_size_pages,
        page_size: PAGE_SIZE,
        k_source: spec.k_source,
        r_repair: spec.r_repair,
        oti,
        object_id,
        page_numbers,
        source_page_xxh3_128: source_hashes,
    })
    .expect("sample wal-fec metadata should be valid")
}

fn sample_repair_symbols(meta: &WalFecGroupMeta) -> Vec<SymbolRecord> {
    (0..meta.r_repair)
        .map(|repair_index| {
            let esi = meta.k_source + repair_index;
            let fill = u8::try_from(esi % 251).expect("ESI modulo should fit in u8");
            let payload = vec![fill; usize::try_from(meta.oti.t).expect("OTI.t should fit usize")];
            SymbolRecord::new(
                meta.object_id,
                meta.oti,
                esi,
                payload,
                SymbolRecordFlags::empty(),
            )
        })
        .collect()
}

#[test]
fn test_bd_1hi_9_unit_compliance_gate() {
    let meta = sample_meta(SampleMetaSpec {
        start_frame_no: 1,
        k_source: 4,
        r_repair: 2,
        wal_salt1: 0xAA11_BB22,
        wal_salt2: 0xCC33_DD44,
        object_tag: b"bd-1hi.9-unit",
        seed_base: 10,
        db_size_pages: 128,
    });
    let repair_symbols = sample_repair_symbols(&meta);
    let group =
        WalFecGroupRecord::new(meta.clone(), repair_symbols).expect("group should validate");

    assert_eq!(group.meta.group_id().end_frame_no, meta.end_frame_no);
    assert_eq!(group.meta.k_source, 4);
}

#[test]
fn prop_bd_1hi_9_structure_compliance() {
    for k_source in 1..=8 {
        for r_repair in 1..=4 {
            let meta = sample_meta(SampleMetaSpec {
                start_frame_no: 5,
                k_source,
                r_repair,
                wal_salt1: 0x0102_0304,
                wal_salt2: 0x0506_0708,
                object_tag: b"bd-1hi.9-prop",
                seed_base: u8::try_from(k_source + r_repair)
                    .expect("small loop values should fit u8"),
                db_size_pages: 256,
            });
            let encoded = meta.to_record_bytes();
            let decoded = WalFecGroupMeta::from_record_bytes(&encoded)
                .expect("serialized metadata should round-trip");

            assert_eq!(decoded.k_source, k_source);
            assert_eq!(decoded.r_repair, r_repair);
            assert_eq!(
                decoded.page_numbers.len(),
                usize::try_from(k_source).expect("k_source fits usize")
            );
            assert_eq!(
                decoded.source_page_xxh3_128.len(),
                usize::try_from(k_source).expect("k_source fits usize")
            );

            let group = WalFecGroupRecord::new(decoded.clone(), sample_repair_symbols(&decoded))
                .expect("group layout should validate");
            assert_eq!(
                group.repair_symbols.len(),
                usize::try_from(r_repair).expect("small r fits usize")
            );
        }
    }
}

#[test]
fn test_wal_fec_header_format() {
    let meta = sample_meta(SampleMetaSpec {
        start_frame_no: 1,
        k_source: 5,
        r_repair: 2,
        wal_salt1: 0x1010_2020,
        wal_salt2: 0x3030_4040,
        object_tag: b"header-format",
        seed_base: 3,
        db_size_pages: 512,
    });
    let bytes = meta.to_record_bytes();
    let parsed = WalFecGroupMeta::from_record_bytes(&bytes).expect("metadata should parse");

    assert_eq!(parsed.magic, WAL_FEC_GROUP_META_MAGIC);
    assert_eq!(parsed.version, WAL_FEC_GROUP_META_VERSION);
    assert_eq!(parsed.k_source, 5);
    assert_eq!(parsed.r_repair, 2);
    assert_eq!(parsed.checksum, meta.checksum);
}

#[test]
fn test_wal_fec_group_layout() {
    let temp_dir = tempdir().expect("tempdir should be created");
    let sidecar_path = temp_dir.path().join("layout.wal-fec");

    let meta = sample_meta(SampleMetaSpec {
        start_frame_no: 10,
        k_source: 3,
        r_repair: 2,
        wal_salt1: 0xAAAA_BBBB,
        wal_salt2: 0xCCCC_DDDD,
        object_tag: b"group-layout",
        seed_base: 11,
        db_size_pages: 2048,
    });
    let group = WalFecGroupRecord::new(meta.clone(), sample_repair_symbols(&meta))
        .expect("group should validate");
    append_wal_fec_group(&sidecar_path, &group).expect("append should succeed");

    let scan = scan_wal_fec(&sidecar_path).expect("scan should succeed");
    assert!(!scan.truncated_tail);
    assert_eq!(scan.groups.len(), 1);

    let parsed = &scan.groups[0];
    assert_eq!(parsed.meta.group_id(), meta.group_id());
    assert_eq!(parsed.repair_symbols.len(), 2);
    assert_eq!(parsed.repair_symbols[0].esi, meta.k_source);
    assert_eq!(parsed.repair_symbols[1].esi, meta.k_source + 1);
}

#[test]
fn test_wal_fec_salt_binding() {
    let meta = sample_meta(SampleMetaSpec {
        start_frame_no: 2,
        k_source: 4,
        r_repair: 2,
        wal_salt1: 0x1122_3344,
        wal_salt2: 0x5566_7788,
        object_tag: b"salt-binding",
        seed_base: 5,
        db_size_pages: 512,
    });

    meta.verify_salt_binding(fsqlite_wal::WalSalts {
        salt1: 0x1122_3344,
        salt2: 0x5566_7788,
    })
    .expect("matching salts should validate");

    let mismatch = meta.verify_salt_binding(fsqlite_wal::WalSalts {
        salt1: 0x9999_3344,
        salt2: 0x5566_7788,
    });
    assert!(mismatch.is_err(), "salt mismatch must be rejected");
}

#[test]
fn test_wal_fec_created_alongside_wal() {
    let temp_dir = tempdir().expect("tempdir should be created");
    let wal_path = temp_dir.path().join("db-wal");
    let sidecar_path =
        ensure_wal_with_fec_sidecar(&wal_path).expect("sidecar creation should work");

    assert!(wal_path.exists());
    assert!(sidecar_path.exists());
    assert_eq!(sidecar_path, wal_fec_path_for_wal(&wal_path));
}

#[test]
fn test_wal_fec_group_id_computation() {
    let meta = sample_meta(SampleMetaSpec {
        start_frame_no: 5,
        k_source: 3,
        r_repair: 2,
        wal_salt1: 0xABCD_EF01,
        wal_salt2: 0x1234_5678,
        object_tag: b"group-id",
        seed_base: 7,
        db_size_pages: 111,
    });
    let group_id = meta.group_id();

    assert_eq!(
        group_id,
        WalFecGroupId {
            wal_salt1: 0xABCD_EF01,
            wal_salt2: 0x1234_5678,
            end_frame_no: 7,
        }
    );
}

#[test]
fn test_wal_fec_invalid_magic_rejected() {
    let meta = sample_meta(SampleMetaSpec {
        start_frame_no: 1,
        k_source: 2,
        r_repair: 1,
        wal_salt1: 0xAA,
        wal_salt2: 0xBB,
        object_tag: b"bad-magic",
        seed_base: 9,
        db_size_pages: 100,
    });
    let mut bytes = meta.to_record_bytes();
    bytes[0] ^= 0xFF;

    let parsed = WalFecGroupMeta::from_record_bytes(&bytes);
    assert!(parsed.is_err(), "invalid magic should be rejected");
}

#[test]
fn test_wal_fec_checksum_detects_corruption() {
    let meta = sample_meta(SampleMetaSpec {
        start_frame_no: 1,
        k_source: 2,
        r_repair: 1,
        wal_salt1: 0x10,
        wal_salt2: 0x20,
        object_tag: b"checksum",
        seed_base: 3,
        db_size_pages: 100,
    });
    let mut bytes = meta.to_record_bytes();
    let payload_offset = 8 + 4 + (8 * 4) + 22 + 16;
    bytes[payload_offset] ^= 0x40;

    let parsed = WalFecGroupMeta::from_record_bytes(&bytes);
    assert!(parsed.is_err(), "checksum corruption should be detected");
}

#[test]
fn test_wal_fec_duplicate_page_numbers_allowed() {
    let mut meta = sample_meta(SampleMetaSpec {
        start_frame_no: 1,
        k_source: 3,
        r_repair: 2,
        wal_salt1: 0x1,
        wal_salt2: 0x2,
        object_tag: b"dupe-page-nos",
        seed_base: 13,
        db_size_pages: 200,
    });
    meta.page_numbers = vec![7, 7, 7];
    meta.checksum = WalFecGroupMeta::from_init(WalFecGroupMetaInit {
        wal_salt1: meta.wal_salt1,
        wal_salt2: meta.wal_salt2,
        start_frame_no: meta.start_frame_no,
        end_frame_no: meta.end_frame_no,
        db_size_pages: meta.db_size_pages,
        page_size: meta.page_size,
        k_source: meta.k_source,
        r_repair: meta.r_repair,
        oti: meta.oti,
        object_id: meta.object_id,
        page_numbers: meta.page_numbers.clone(),
        source_page_xxh3_128: meta.source_page_xxh3_128.clone(),
    })
    .expect("recomputed metadata should stay valid")
    .checksum;

    let encoded = meta.to_record_bytes();
    let parsed = WalFecGroupMeta::from_record_bytes(&encoded).expect("metadata should parse");
    assert_eq!(parsed.page_numbers, vec![7, 7, 7]);
}

#[test]
fn test_e2e_bd_1hi_9_compliance() {
    let temp_dir = tempdir().expect("tempdir should be created");
    let sidecar_path = temp_dir.path().join("e2e.wal-fec");

    let meta_alpha = sample_meta(SampleMetaSpec {
        start_frame_no: 1,
        k_source: 3,
        r_repair: 2,
        wal_salt1: 0x101,
        wal_salt2: 0x202,
        object_tag: b"group-a",
        seed_base: 10,
        db_size_pages: 1000,
    });
    let meta_beta = sample_meta(SampleMetaSpec {
        start_frame_no: 4,
        k_source: 4,
        r_repair: 2,
        wal_salt1: 0x101,
        wal_salt2: 0x202,
        object_tag: b"group-b",
        seed_base: 30,
        db_size_pages: 1004,
    });
    let meta_gamma = sample_meta(SampleMetaSpec {
        start_frame_no: 8,
        k_source: 2,
        r_repair: 1,
        wal_salt1: 0x101,
        wal_salt2: 0x202,
        object_tag: b"group-c",
        seed_base: 90,
        db_size_pages: 1006,
    });

    let group_alpha =
        WalFecGroupRecord::new(meta_alpha.clone(), sample_repair_symbols(&meta_alpha))
            .expect("group alpha valid");
    let group_beta = WalFecGroupRecord::new(meta_beta.clone(), sample_repair_symbols(&meta_beta))
        .expect("group beta valid");
    let group_gamma =
        WalFecGroupRecord::new(meta_gamma.clone(), sample_repair_symbols(&meta_gamma))
            .expect("group gamma valid");

    append_wal_fec_group(&sidecar_path, &group_alpha).expect("append group alpha");
    append_wal_fec_group(&sidecar_path, &group_beta).expect("append group beta");
    append_wal_fec_group(&sidecar_path, &group_gamma).expect("append group gamma");

    let scan = scan_wal_fec(&sidecar_path).expect("full scan should succeed");
    assert!(!scan.truncated_tail);
    assert_eq!(scan.groups.len(), 3);

    let found_beta = find_wal_fec_group(&sidecar_path, meta_beta.group_id())
        .expect("lookup should succeed")
        .expect("group beta should be found");
    assert_eq!(found_beta.meta.group_id(), meta_beta.group_id());
    assert_eq!(
        found_beta.repair_symbols.len(),
        usize::try_from(meta_beta.r_repair).expect("small r fits usize")
    );

    let mut raw_sidecar = fs::read(&sidecar_path).expect("sidecar should be readable");
    let cut = raw_sidecar.len() - 17;
    raw_sidecar.truncate(cut);
    let truncated_path = temp_dir.path().join("e2e-truncated.wal-fec");
    fs::write(&truncated_path, raw_sidecar).expect("truncated sidecar should be writable");

    let truncated_scan = scan_wal_fec(&truncated_path).expect("truncated scan should still parse");
    assert!(
        truncated_scan.truncated_tail,
        "truncated tail must be reported"
    );
    assert!(
        truncated_scan.groups.len() < 3,
        "partial trailing group must not be treated as valid"
    );
}
