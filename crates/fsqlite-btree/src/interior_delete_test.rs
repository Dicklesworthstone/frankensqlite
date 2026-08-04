use std::collections::BTreeSet;

use crate::{BtCursor, BtreeCursorOps, MemPageStore};
use asupersync::runtime::RuntimeBuilder;
use fsqlite_types::PageNumber;
use fsqlite_types::cx::Cx;

fn key_for(i: u32) -> Vec<u8> {
    let mut key = vec![0u8; 100];
    key[0..4].copy_from_slice(&i.to_be_bytes());
    key
}

fn key_prefix_u32(key: &[u8]) -> u32 {
    let mut buf = [0u8; 4];
    buf.copy_from_slice(&key[0..4]);
    u32::from_be_bytes(buf)
}

#[test]
fn test_index_delete_interior() {
    RuntimeBuilder::current_thread()
        .build()
        .expect("build interior-delete test runtime")
        .block_on(async {
            let cx = Cx::new();
            let root = PageNumber::new(2).unwrap();
            // 512 byte pages so it splits quickly
            let store = MemPageStore::with_empty_index(root, 512);
            let mut cursor = BtCursor::new(store, root, 512, false);

            let mut expected = BTreeSet::new();

            // Insert 100 elements to force interior nodes
            for i in 0..100_u32 {
                let key = key_for(i);
                cursor.index_insert(&cx, &key).await.unwrap();
                expected.insert(i);
            }

            // Delete in ascending order and assert cursor positioning + remaining set.
            for i in 0..100_u32 {
                let key = key_for(i);
                let seek = cursor.index_move_to(&cx, &key).await.unwrap();
                assert!(seek.is_found());
                cursor.delete(&cx).await.unwrap();
                assert!(
                    expected.remove(&i),
                    "deleted key must exist in expected set"
                );

                if let Some(next_expected) = expected.iter().next().copied() {
                    assert!(
                        !cursor.eof(),
                        "cursor should be positioned at successor after delete"
                    );
                    let next_key = cursor.payload(&cx).await.unwrap();
                    assert_eq!(
                        key_prefix_u32(&next_key),
                        next_expected,
                        "cursor should point at immediate successor",
                    );
                } else {
                    assert!(cursor.eof(), "cursor should be EOF after final delete");
                }
            }

            // Final full scan: tree must be empty.
            assert!(
                !cursor.first(&cx).await.unwrap(),
                "first() on empty tree should be false"
            );
            assert!(cursor.eof(), "cursor must remain at EOF for empty tree");
        });
}

#[test]
fn test_index_delete_interior_needs_rebalance() {
    RuntimeBuilder::current_thread()
        .build()
        .expect("build interior-rebalance test runtime")
        .block_on(async {
            let cx = Cx::new();
            let root = PageNumber::new(2).unwrap();
            // 512 byte pages so it splits quickly
            let store = MemPageStore::with_empty_index(root, 512);
            let mut cursor = BtCursor::new(store, root, 512, false);

            // Let's explicitly construct the case.
            // Insert 10 (size 10), 20 (size 10), 30 (size 10)
            // 40 (size 10), 50 (size 10), 60 (size 10).
            // Let's just insert 0..100 with alternating sizes!
            // If i == 20, size = 10 (interior maybe?)
            // If i == 21, size = 200 (successor)
            for i in 0..100_u32 {
                let mut key = vec![0u8; if i % 2 == 0 { 10 } else { 200 }];
                key[0..4].copy_from_slice(&i.to_be_bytes());
                cursor.index_insert(&cx, &key).await.unwrap();
            }

            // Now delete everything!
            for i in 0..100_u32 {
                let mut key = vec![0u8; if i % 2 == 0 { 10 } else { 200 }];
                key[0..4].copy_from_slice(&i.to_be_bytes());
                if cursor.index_move_to(&cx, &key).await.unwrap().is_found() {
                    cursor.delete(&cx).await.unwrap();
                }
            }
        });
}
