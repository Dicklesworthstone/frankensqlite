use std::collections::BTreeSet;

use proptest::prelude::*;

use fsqlite_harness::tla::{
    DporExploration, MvccAction, MvccActionKind, are_independent, dpor_enumerate_trace_classes,
    enumerate_trace_classes, foata_normal_form, simulate_ssi_execution, trace_reduction_stats,
    verify_trace_invariants,
};

fn rr(txn_id: u64, ordinal: u32, page_id: u32) -> MvccAction {
    MvccAction::read(txn_id, ordinal, page_id)
}

fn ww(txn_id: u64, ordinal: u32, page_id: u32) -> MvccAction {
    MvccAction::write(txn_id, ordinal, page_id)
}

fn cc(txn_id: u64, ordinal: u32, pages: impl IntoIterator<Item = u32>) -> MvccAction {
    MvccAction::commit(txn_id, ordinal, pages)
}

fn bb(txn_id: u64, ordinal: u32) -> MvccAction {
    MvccAction::begin(txn_id, ordinal)
}

fn sample_two_txn_three_ops_each() -> Vec<Vec<MvccAction>> {
    vec![
        vec![rr(1, 0, 1), ww(1, 1, 2), cc(1, 2, [2])],
        vec![rr(2, 0, 3), ww(2, 1, 4), cc(2, 2, [4])],
    ]
}

fn sample_two_txn_two_ops_each() -> Vec<Vec<MvccAction>> {
    vec![
        vec![rr(1, 0, 1), cc(1, 1, [1])],
        vec![rr(2, 0, 2), cc(2, 1, [2])],
    ]
}

fn sample_three_txn_three_ops_each() -> Vec<Vec<MvccAction>> {
    vec![
        vec![rr(1, 0, 10), ww(1, 1, 11), cc(1, 2, [11])],
        vec![rr(2, 0, 20), ww(2, 1, 21), cc(2, 2, [21])],
        vec![rr(3, 0, 30), ww(3, 1, 31), cc(3, 2, [31])],
    ]
}

fn signatures(classes: &[fsqlite_harness::tla::TraceClass]) -> BTreeSet<String> {
    classes
        .iter()
        .map(|class| class.canonical.canonical_signature())
        .collect()
}

#[test]
fn test_read_read_different_pages_independent() {
    assert!(are_independent(&rr(1, 0, 1), &rr(2, 0, 2)));
}

#[test]
fn test_read_read_same_page_independent() {
    assert!(are_independent(&rr(1, 0, 7), &rr(2, 0, 7)));
}

#[test]
fn test_read_write_same_page_dependent() {
    assert!(!are_independent(&rr(1, 0, 7), &ww(2, 0, 7)));
}

#[test]
fn test_write_write_different_pages_independent() {
    assert!(are_independent(&ww(1, 0, 7), &ww(2, 0, 8)));
}

#[test]
fn test_write_write_same_page_dependent() {
    assert!(!are_independent(&ww(1, 0, 7), &ww(2, 0, 7)));
}

#[test]
fn test_commit_commit_dependent() {
    assert!(!are_independent(&cc(1, 0, [7]), &cc(2, 0, [8])));
}

#[test]
fn test_begin_begin_dependent() {
    assert!(!are_independent(&bb(1, 0), &bb(2, 0)));
}

#[test]
fn test_read_commit_dependent_if_overlapping() {
    let read = rr(1, 0, 42);
    let overlapping_commit = cc(2, 1, [42, 99]);
    let disjoint_commit = cc(2, 1, [7, 9]);

    assert!(!are_independent(&read, &overlapping_commit));
    assert!(are_independent(&read, &disjoint_commit));
}

#[test]
fn test_foata_2txn_3ops_each() {
    let classes = enumerate_trace_classes(&sample_two_txn_three_ops_each());
    assert_eq!(classes.len(), 2, "expected two commit-order classes");
}

#[test]
fn test_foata_layers_correct() {
    let schedule = vec![
        rr(1, 0, 1),
        rr(2, 0, 3),
        ww(1, 1, 2),
        ww(2, 1, 4),
        cc(1, 2, [2]),
        cc(2, 2, [4]),
    ];
    let foata = foata_normal_form(&schedule);

    assert_eq!(foata.layers.len(), 4);
    assert_eq!(foata.layers[0].len(), 2);
    assert!(
        foata.layers[0]
            .iter()
            .all(|action| matches!(&action.kind, MvccActionKind::Read { .. }))
    );

    assert_eq!(foata.layers[1].len(), 2);
    assert!(
        foata.layers[1]
            .iter()
            .all(|action| matches!(&action.kind, MvccActionKind::Write { .. }))
    );

    assert_eq!(foata.layers[2].len(), 1);
    assert!(matches!(
        &foata.layers[2][0].kind,
        MvccActionKind::Commit { .. }
    ));

    assert_eq!(foata.layers[3].len(), 1);
    assert!(matches!(
        &foata.layers[3][0].kind,
        MvccActionKind::Commit { .. }
    ));
}

#[test]
fn test_foata_canonical_deterministic() {
    let schedule_a = vec![
        rr(1, 0, 1),
        rr(2, 0, 3),
        ww(1, 1, 2),
        ww(2, 1, 4),
        cc(1, 2, [2]),
        cc(2, 2, [4]),
    ];
    let schedule_b = vec![
        rr(2, 0, 3),
        rr(1, 0, 1),
        ww(2, 1, 4),
        ww(1, 1, 2),
        cc(1, 2, [2]),
        cc(2, 2, [4]),
    ];

    let sig_a = foata_normal_form(&schedule_a).canonical_signature();
    let sig_b = foata_normal_form(&schedule_b).canonical_signature();
    assert_eq!(sig_a, sig_b);
}

#[test]
fn test_enumerate_all_classes() {
    let stats = trace_reduction_stats(&sample_two_txn_two_ops_each());
    assert_eq!(stats.naive_interleavings, 6);
    assert_eq!(stats.trace_classes, 2);
}

#[test]
fn test_trace_reduction_ratio() {
    let stats = trace_reduction_stats(&sample_three_txn_three_ops_each());
    assert!(stats.naive_interleavings > stats.trace_classes);
    assert!(
        stats.reduction_factor > 10.0,
        "expected strong reduction, got {}",
        stats.reduction_factor
    );
}

#[test]
fn test_mvcc_invariants_all_classes() {
    let chains = vec![
        vec![bb(1, 0), rr(1, 1, 1), ww(1, 2, 2), cc(1, 3, [2])],
        vec![bb(2, 0), rr(2, 1, 3), ww(2, 2, 4), cc(2, 3, [4])],
        vec![MvccAction::gc(0, 0, 0)],
    ];

    let classes = enumerate_trace_classes(&chains);
    assert!(!classes.is_empty());
    for class in classes {
        let report = verify_trace_invariants(&class.representative);
        assert!(
            report.all_hold(),
            "invariant failed for class {}",
            class.canonical.canonical_signature()
        );
    }
}

#[test]
fn test_dpor_explores_all_relevant_classes() {
    let scenario = vec![
        vec![ww(1, 0, 10), cc(1, 1, [10])],
        vec![ww(2, 0, 20), cc(2, 1, [20])],
    ];

    let exhaustive = enumerate_trace_classes(&scenario);
    let dpor: DporExploration = dpor_enumerate_trace_classes(&scenario);

    assert_eq!(signatures(&exhaustive), signatures(&dpor.classes));
    assert!(dpor.explored_paths <= dpor.naive_interleavings);
}

#[test]
fn test_dpor_finds_known_bug() {
    let scenario = vec![
        vec![bb(1, 0), ww(1, 1, 7), cc(1, 2, [7])],
        vec![bb(2, 0), ww(2, 1, 7), cc(2, 2, [7])],
    ];

    let dpor = dpor_enumerate_trace_classes(&scenario);
    let found_conflict_acceptance = dpor
        .classes
        .iter()
        .map(|class| verify_trace_invariants(&class.representative))
        .any(|report| !report.fcw_conflicts_detected);

    assert!(
        found_conflict_acceptance,
        "expected at least one class exposing FCW-unsafe acceptance"
    );
}

#[test]
fn test_lab_deterministic_seeds() {
    let scenario = sample_three_txn_three_ops_each();
    let baseline = signatures(&enumerate_trace_classes(&scenario));

    for seed in 0_u64..100 {
        let observed = signatures(&enumerate_trace_classes(&scenario));
        assert_eq!(
            observed, baseline,
            "deterministic trace class set mismatch for seed {seed}"
        );
    }
}

#[test]
fn test_mazurkiewicz_3txn_6_orderings() {
    let scenario = vec![
        vec![bb(1, 0), ww(1, 1, 10), cc(1, 2, [10])], // T1 writes page A
        vec![bb(2, 0), ww(2, 1, 20), cc(2, 2, [20])], // T2 writes page B
        vec![
            bb(3, 0),
            ww(3, 1, 10), // T3 writes A
            ww(3, 2, 20), // T3 writes B
            cc(3, 3, [10, 20]),
        ],
    ];

    let classes = enumerate_trace_classes(&scenario);
    let commit_orders: BTreeSet<Vec<u64>> = classes
        .iter()
        .map(|class| {
            class
                .representative
                .iter()
                .filter_map(|action| match action.kind {
                    MvccActionKind::Commit { .. } => Some(action.txn_id),
                    _ => None,
                })
                .collect::<Vec<_>>()
        })
        .filter(|order| order.len() == 3)
        .collect();

    assert_eq!(
        commit_orders.len(),
        6,
        "expected all 6 commit-order permutations for the 3-txn Mazurkiewicz scenario"
    );
}

fn arb_action() -> impl Strategy<Value = MvccAction> {
    (
        1_u64..=4,
        0_u32..=6,
        prop_oneof![Just(0_u8), Just(1_u8), Just(2_u8), Just(3_u8), Just(4_u8)],
        0_u32..=16,
        prop::collection::vec(0_u32..16, 0..4),
        0_u64..=16,
    )
        .prop_map(
            |(txn_id, ordinal, tag, page_id, commit_pages, horizon_seq)| match tag {
                0 => MvccAction::begin(txn_id, ordinal),
                1 => MvccAction::read(txn_id, ordinal, page_id),
                2 => MvccAction::write(txn_id, ordinal, page_id),
                3 => {
                    let pages: BTreeSet<u32> = commit_pages.into_iter().collect();
                    MvccAction::commit(txn_id, ordinal, pages)
                }
                _ => MvccAction::gc(txn_id, ordinal, horizon_seq),
            },
        )
}

proptest! {
    #[test]
    fn prop_independence_symmetric(a in arb_action(), b in arb_action()) {
        prop_assert_eq!(are_independent(&a, &b), are_independent(&b, &a));
    }

    #[test]
    fn prop_independence_irreflexive(a in arb_action()) {
        prop_assert!(!are_independent(&a, &a));
    }

    #[test]
    fn prop_foata_form_unique(page_a in 0_u32..64, page_b in 0_u32..64) {
        prop_assume!(page_a != page_b);

        let a = rr(1, 0, page_a);
        let b = rr(2, 0, page_b);
        let c = cc(1, 1, [page_a]);

        let word_1 = vec![a.clone(), b.clone(), c.clone()];
        let word_2 = vec![b, a, c];

        let sig_1 = foata_normal_form(&word_1).canonical_signature();
        let sig_2 = foata_normal_form(&word_2).canonical_signature();
        prop_assert_eq!(sig_1, sig_2);
    }
}

#[test]
fn test_e2e_exhaustive_2txn_write_skew() {
    let scenario = vec![
        vec![bb(1, 0), rr(1, 1, 10), ww(1, 2, 20), cc(1, 3, [20])],
        vec![bb(2, 0), rr(2, 1, 20), ww(2, 2, 10), cc(2, 3, [10])],
    ];

    let classes = enumerate_trace_classes(&scenario);
    assert!(!classes.is_empty());

    for class in classes {
        let outcome = simulate_ssi_execution(&class.representative);
        assert!(
            !(outcome.committed.contains(&1) && outcome.committed.contains(&2)),
            "SSI must reject write-skew cycle in class {}",
            class.canonical.canonical_signature()
        );
        assert!(
            outcome.aborted.contains(&1) || outcome.aborted.contains(&2),
            "at least one transaction must abort under SSI in class {}",
            class.canonical.canonical_signature()
        );
    }
}
