use std::fs;

use fsqlite_types::{ObjectId, Oti, SymbolRecord, SymbolRecordFlags};
use fsqlite_wal::{
    WAL_FEC_GROUP_META_MAGIC, WAL_FEC_GROUP_META_VERSION, WalFecGroupId, WalFecGroupMeta,
    WalFecGroupMetaInit, WalFecGroupRecord, append_wal_fec_group, build_source_page_hashes,
    ensure_wal_with_fec_sidecar, find_wal_fec_group, persist_wal_fec_raptorq_repair_symbols,
    read_wal_fec_raptorq_repair_symbols, scan_wal_fec, wal_fec_path_for_wal,
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

fn run_sidecar_mutation_child(sidecar: &std::path::Path, phase: &str) {
    let mut child = std::process::Command::new(std::env::current_exe().expect("test executable"))
        .args([
            "--exact",
            "test_sidecar_migration_excludes_cross_process_mutations",
            "--nocapture",
        ])
        .env("FSQLITE_FEC_MUTATION_CHILD_PATH", sidecar)
        .env("FSQLITE_FEC_MUTATION_CHILD_PHASE", phase)
        .spawn()
        .expect("spawn sidecar mutation child");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait().expect("poll sidecar mutation child") {
            assert!(status.success(), "sidecar child {phase}: {status}");
            return;
        }
        if std::time::Instant::now() >= deadline {
            child.kill().expect("stop child after timeout");
            child.wait().expect("reap timed-out child");
            panic!("sidecar child {phase} blocked instead of returning");
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}

#[test]
fn test_sidecar_migration_excludes_cross_process_mutations() {
    let spec = SampleMetaSpec {
        start_frame_no: 1,
        k_source: 3,
        r_repair: 2,
        wal_salt1: 11,
        wal_salt2: 22,
        object_tag: b"cross-process-prefix",
        seed_base: 5,
        db_size_pages: 128,
    };
    let tail_meta = sample_meta(SampleMetaSpec {
        start_frame_no: 4,
        object_tag: b"cross-process-tail",
        ..spec
    });
    let tail = WalFecGroupRecord::new(tail_meta.clone(), sample_repair_symbols(&tail_meta))
        .expect("valid tail group");
    if let Some(path) = std::env::var_os("FSQLITE_FEC_MUTATION_CHILD_PATH") {
        let path = std::path::Path::new(&path);
        match std::env::var("FSQLITE_FEC_MUTATION_CHILD_PHASE")
            .unwrap()
            .as_str()
        {
            "busy" => {
                let before = fs::read(path).expect("read original bytes");
                assert!(matches!(
                    append_wal_fec_group(path, &tail),
                    Err(fsqlite_error::FrankenError::Busy)
                ));
                assert!(matches!(
                    persist_wal_fec_raptorq_repair_symbols(path, 9),
                    Err(fsqlite_error::FrankenError::Busy)
                ));
                assert_eq!(fs::read(path).expect("read after refusal"), before);
            }
            "migrate_then_append" => {
                persist_wal_fec_raptorq_repair_symbols(path, 9).expect("migrate legacy header");
                append_wal_fec_group(path, &tail).expect("append after migration");
            }
            phase => panic!("unexpected sidecar child phase: {phase}"),
        }
        return;
    }

    let dir = tempdir().expect("tempdir");
    let sidecar = dir.path().join("db.wal-fec");
    let prefix_meta = sample_meta(spec);
    let prefix = WalFecGroupRecord::new(prefix_meta.clone(), sample_repair_symbols(&prefix_meta))
        .expect("valid prefix group");
    append_wal_fec_group(&sidecar, &prefix).expect("append legacy prefix");
    let lock_path = dir.path().join("db.wal-fec.lock");
    let guard = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .expect("open persistent sidecar coordination file");
    guard.try_lock().expect("hold sidecar mutation guard");
    run_sidecar_mutation_child(&sidecar, "busy");
    drop(guard);
    run_sidecar_mutation_child(&sidecar, "migrate_then_append");
    let scan = scan_wal_fec(&sidecar).expect("scan migrated sidecar");
    assert!(!scan.truncated_tail);
    assert_eq!(scan.groups, vec![prefix, tail]);
    assert_eq!(read_wal_fec_raptorq_repair_symbols(&sidecar).unwrap(), 9);
    let guard = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(lock_path)
        .unwrap();
    guard.try_lock().expect("child must release its guard");
}

#[test]
fn test_reclaim_sidecar_groups_preserves_uncheckpointed_generations() {
    use fsqlite_wal::WalSalts;
    use fsqlite_wal::wal_fec::reclaim_wal_fec_groups;

    let dir = tempdir().expect("tempdir");
    let sidecar = dir.path().join("gc.wal-fec");
    let salts = WalSalts {
        salt1: 11,
        salt2: 22,
    };
    assert_eq!(
        reclaim_wal_fec_groups(&sidecar, salts, u32::MAX).unwrap(),
        0
    );
    assert!(!sidecar.exists());
    let spec = SampleMetaSpec {
        start_frame_no: 1,
        k_source: 3,
        r_repair: 2,
        wal_salt1: salts.salt1,
        wal_salt2: salts.salt2,
        object_tag: b"gc-prefix",
        seed_base: 5,
        db_size_pages: 128,
    };
    let mut groups = Vec::new();
    for meta in [
        sample_meta(spec),
        sample_meta(SampleMetaSpec {
            start_frame_no: 4,
            object_tag: b"gc-tail",
            ..spec
        }),
        sample_meta(SampleMetaSpec {
            wal_salt1: 33,
            object_tag: b"gc-next-generation",
            ..spec
        }),
    ] {
        groups.push(WalFecGroupRecord::new(meta.clone(), sample_repair_symbols(&meta)).unwrap());
    }
    persist_wal_fec_raptorq_repair_symbols(&sidecar, 17).unwrap();
    for group in &groups {
        append_wal_fec_group(&sidecar, group).unwrap();
    }
    let before = fs::read(&sidecar).unwrap();
    assert_eq!(reclaim_wal_fec_groups(&sidecar, salts, 2).unwrap(), 0);
    assert_eq!(fs::read(&sidecar).unwrap(), before);
    assert_eq!(reclaim_wal_fec_groups(&sidecar, salts, 3).unwrap(), 1);
    assert_eq!(scan_wal_fec(&sidecar).unwrap().groups, groups[1..]);
    assert!(fs::metadata(&sidecar).unwrap().len() < u64::try_from(before.len()).unwrap());
    assert_eq!(
        reclaim_wal_fec_groups(&sidecar, salts, u32::MAX).unwrap(),
        1
    );
    assert_eq!(scan_wal_fec(&sidecar).unwrap().groups, groups[2..]);
    assert_eq!(read_wal_fec_raptorq_repair_symbols(&sidecar).unwrap(), 17);

    let remaining = fs::read(&sidecar).unwrap();
    let mut corrupt = remaining.clone();
    corrupt.extend_from_slice(&[0xff, 0xff]);
    fs::write(&sidecar, &corrupt).unwrap();
    let error = reclaim_wal_fec_groups(
        &sidecar,
        WalSalts {
            salt1: 33,
            salt2: 22,
        },
        u32::MAX,
    )
    .expect_err("corrupt-tail reclamation must be refused");
    assert!(
        error.to_string().contains("unvalidated tail"),
        "wrong refusal: {error}"
    );
    assert_eq!(
        fs::read(&sidecar).unwrap(),
        corrupt,
        "refused GC must preserve all bytes"
    );
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
fn test_scan_wal_fec_corrupt_meta_retains_preceding_groups_bd_xv5cm_m4() {
    // bd-xv5cm M4: a misframed / corrupt group-metadata record mid-sidecar must
    // NOT poison the whole scan. `scan_wal_fec` must return the valid groups
    // parsed before it (with `truncated_tail = true`), not `Err`. Pre-fix the
    // metadata parse `?`-propagated, discarding every preceding valid group —
    // exactly the "poisoning" the non-atomic multi-write append could produce.
    use std::io::Write;

    let temp_dir = tempdir().expect("tempdir should be created");
    let sidecar_path = temp_dir.path().join("corrupt_tail.wal-fec");

    // Two fully-valid groups, each appended with the single-buffer single-write
    // path (bd-xv5cm M4 append fix).
    let meta_a = sample_meta(SampleMetaSpec {
        start_frame_no: 10,
        k_source: 3,
        r_repair: 2,
        wal_salt1: 0x1111_2222,
        wal_salt2: 0x3333_4444,
        object_tag: b"corrupt-a",
        seed_base: 5,
        db_size_pages: 2048,
    });
    let group_a = WalFecGroupRecord::new(meta_a.clone(), sample_repair_symbols(&meta_a))
        .expect("group a should validate");
    append_wal_fec_group(&sidecar_path, &group_a).expect("append a should succeed");

    let meta_b = sample_meta(SampleMetaSpec {
        start_frame_no: 20,
        k_source: 3,
        r_repair: 2,
        wal_salt1: 0x5555_6666,
        wal_salt2: 0x7777_8888,
        object_tag: b"corrupt-b",
        seed_base: 6,
        db_size_pages: 2048,
    });
    let group_b = WalFecGroupRecord::new(meta_b.clone(), sample_repair_symbols(&meta_b))
        .expect("group b should validate");
    append_wal_fec_group(&sidecar_path, &group_b).expect("append b should succeed");

    // Append a length-prefixed but corrupt metadata record: a valid u32 length
    // prefix followed by garbage that `WalFecGroupMeta::from_record_bytes`
    // rejects (bad magic). This is the shape a torn/partial append could leave.
    let garbage = [0xFFu8; 64];
    let mut corrupt = Vec::new();
    corrupt.extend_from_slice(&u32::try_from(garbage.len()).unwrap().to_le_bytes());
    corrupt.extend_from_slice(&garbage);
    std::fs::OpenOptions::new()
        .append(true)
        .open(&sidecar_path)
        .expect("open sidecar for corrupt append")
        .write_all(&corrupt)
        .expect("append corrupt tail");

    // The scan must retain BOTH valid groups and flag the truncated/corrupt tail,
    // never erroring the whole scan.
    let scan =
        scan_wal_fec(&sidecar_path).expect("a corrupt metadata tail must not poison the scan");
    assert!(
        scan.truncated_tail,
        "the corrupt metadata tail must set truncated_tail"
    );
    assert_eq!(
        scan.groups.len(),
        2,
        "both valid preceding groups must be retained"
    );
    assert_eq!(scan.groups[0].meta.group_id(), meta_a.group_id());
    assert_eq!(scan.groups[1].meta.group_id(), meta_b.group_id());
}

#[test]
fn test_scan_wal_fec_layout_error_retains_only_preceding_groups() {
    use std::io::Write;

    let dir = tempdir().expect("tempdir");
    let spec = SampleMetaSpec {
        start_frame_no: 1,
        k_source: 3,
        r_repair: 2,
        wal_salt1: 11,
        wal_salt2: 22,
        object_tag: b"layout-prefix",
        seed_base: 5,
        db_size_pages: 128,
    };
    let prefix_meta = sample_meta(spec);
    let tail_meta = sample_meta(SampleMetaSpec {
        start_frame_no: 4,
        object_tag: b"layout-tail",
        ..spec
    });
    let prefix = WalFecGroupRecord::new(prefix_meta.clone(), sample_repair_symbols(&prefix_meta))
        .expect("valid prefix group");
    let tail = WalFecGroupRecord::new(tail_meta.clone(), sample_repair_symbols(&tail_meta))
        .expect("valid tail group");

    for mismatch in ["object_id", "oti", "esi"] {
        let mut symbols = sample_repair_symbols(&tail_meta);
        let original = &symbols[1];
        let object_id = if mismatch == "object_id" {
            prefix_meta.object_id
        } else {
            original.object_id
        };
        let mut oti = original.oti;
        if mismatch == "oti" {
            oti.al = 0;
        }
        let esi = if mismatch == "esi" {
            tail_meta.k_source
        } else {
            original.esi
        };
        symbols[1] = SymbolRecord::new(
            object_id,
            oti,
            esi,
            sample_payload(9),
            SymbolRecordFlags::empty(),
        );
        for symbol in &symbols {
            assert_eq!(
                SymbolRecord::from_bytes(&symbol.to_bytes()).expect("valid symbol checksum"),
                *symbol,
                "{mismatch}: each symbol must parse independently"
            );
        }
        let error = WalFecGroupRecord::new(tail_meta.clone(), symbols.clone())
            .expect_err("only the group layout must be invalid");
        assert!(
            error.to_string().to_ascii_lowercase().contains(mismatch),
            "{mismatch}: wrong rejection: {error}"
        );

        for with_prefix in [false, true] {
            let sidecar = dir.path().join(format!("{mismatch}-{with_prefix}.wal-fec"));
            if with_prefix {
                append_wal_fec_group(&sidecar, &prefix).expect("append valid prefix");
            }
            let mut corrupt = Vec::new();
            for record in std::iter::once(tail_meta.to_record_bytes())
                .chain(symbols.iter().map(SymbolRecord::to_bytes))
            {
                corrupt.extend_from_slice(&u32::try_from(record.len()).unwrap().to_le_bytes());
                corrupt.extend_from_slice(&record);
            }
            fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&sidecar)
                .expect("open sidecar")
                .write_all(&corrupt)
                .expect("append layout-invalid group");
            append_wal_fec_group(&sidecar, &tail).expect("append valid group after corruption");

            let scan = scan_wal_fec(&sidecar).expect("layout error must not poison the scan");
            assert!(
                scan.truncated_tail,
                "{mismatch}: corruption must be reported"
            );
            assert_eq!(scan.groups.len(), usize::from(with_prefix));
            assert_eq!(
                find_wal_fec_group(&sidecar, prefix_meta.group_id()).expect("find valid prefix"),
                with_prefix.then(|| prefix.clone())
            );
            assert!(
                find_wal_fec_group(&sidecar, tail_meta.group_id())
                    .expect("search must tolerate layout error")
                    .is_none(),
                "{mismatch}: groups after the corrupt record must not be returned"
            );
        }
    }
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
