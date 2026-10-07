//! A structurally shared page directory for write-set-sized snapshot updates.
//!
//! Sharing page buffers is not enough when publishing a snapshot still clones
//! or walks a flat directory containing every committed page (GH494). This map
//! shares the directory too: a snapshot clone shares one root, and replacing a
//! page copies at most eight radix nodes. Reads also have a fixed eight-level
//! bound; there is no chain of transaction deltas to replay on a later read.
//!
//! Keys are raw 32-bit page identifiers. Validation of SQLite page numbers and
//! page geometry belongs to the caller. Values need not implement `Clone`.
//! Shared payloads must be immutable; this map does not fence interior mutation.
//! Mutate a cloned map while preparing a transaction and publish its root only
//! after commit succeeds. Dropping that staged map leaves the old image intact.
//!
//! This is the page-directory primitive, not a transaction protocol: it does
//! not collect a pager write set, make a commit durable, or synchronize writers.

use std::iter::FusedIterator;
use std::sync::Arc;

const RADIX: usize = 16;
const DIGIT_BITS: u32 = 4;
const ROOT_SHIFT: u32 = u32::BITS - DIGIT_BITS;
const LEVELS: usize = (u32::BITS / DIGIT_BITS) as usize;

#[derive(Debug)]
enum Node<T> {
    Branch {
        len: usize,
        children: [Option<Arc<Self>>; RADIX],
    },
    Leaf {
        len: usize,
        values: [Option<Arc<T>>; RADIX],
    },
}

// A directory fork clones references, never page contents. Deriving Clone
// would unnecessarily require T: Clone, even though every value is behind Arc.
impl<T> Clone for Node<T> {
    fn clone(&self) -> Self {
        match self {
            Self::Branch { len, children } => Self::Branch {
                len: *len,
                children: children.clone(),
            },
            Self::Leaf { len, values } => Self::Leaf {
                len: *len,
                values: values.clone(),
            },
        }
    }
}

impl<T> Node<T> {
    fn empty(shift: u32) -> Self {
        if shift == 0 {
            Self::Leaf {
                len: 0,
                values: std::array::from_fn(|_| None),
            }
        } else {
            Self::Branch {
                len: 0,
                children: std::array::from_fn(|_| None),
            }
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Branch { len, .. } | Self::Leaf { len, .. } => *len,
        }
    }
}

fn digit(page: u32, shift: u32) -> usize {
    ((page >> shift) & 0xf) as usize
}

/// Persistent page directory with constant-depth lookup and copy-on-write.
///
/// Cloning is O(1). An insertion or deletion visits at most eight radix levels
/// and clones at most eight nodes, each containing sixteen shared references.
/// Applying several mutations to one fork reuses already-private paths.
/// Unchanged subtrees and page payloads retain their identities across forks.
///
/// Iteration deliberately visits all present pages; it is for explicit scans
/// or image export, not for normal transaction publication.
#[derive(Debug)]
pub struct PersistentPageMap<T> {
    root: Option<Arc<Node<T>>>,
}

impl<T> Default for PersistentPageMap<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Clone for PersistentPageMap<T> {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
        }
    }
}

impl<T> PersistentPageMap<T> {
    pub const fn new() -> Self {
        Self { root: None }
    }

    pub fn len(&self) -> usize {
        self.root.as_ref().map_or(0, |node| node.len())
    }

    pub const fn is_empty(&self) -> bool {
        self.root.is_none()
    }

    pub fn get(&self, page: u32) -> Option<&T> {
        self.get_shared(page).map(Arc::as_ref)
    }

    /// Borrow the payload handle without incrementing its reference count.
    pub fn get_shared(&self, page: u32) -> Option<&Arc<T>> {
        let mut node = self.root.as_deref()?;
        let mut shift = ROOT_SHIFT;
        loop {
            let index = digit(page, shift);
            match node {
                Node::Leaf { values, .. } => return values[index].as_ref(),
                Node::Branch { children, .. } => {
                    node = children[index].as_deref()?;
                    shift -= DIGIT_BITS;
                }
            }
        }
    }

    /// Insert a page, returning the previous shared payload when present.
    pub fn insert(&mut self, page: u32, value: T) -> Option<Arc<T>> {
        self.insert_shared(page, Arc::new(value))
    }

    /// Insert a payload already owned by a pager or another snapshot.
    pub fn insert_shared(&mut self, page: u32, value: Arc<T>) -> Option<Arc<T>> {
        insert_at(&mut self.root, ROOT_SHIFT, page, value)
    }

    /// Remove a page. A missing page leaves the directory root unchanged.
    pub fn remove(&mut self, page: u32) -> Option<Arc<T>> {
        self.get_shared(page)?;
        remove_at(&mut self.root, ROOT_SHIFT, page)
    }

    /// Remove identifiers greater than `last_page`, inclusively retaining it.
    ///
    /// The directory walk is bounded by eight levels and their sixteen slots,
    /// independent of the number of retained pages. Reclaiming removed payloads
    /// can still take time proportional to the removed state when this map owns
    /// their final references. Older snapshots keep their own images unchanged.
    pub fn truncate(&mut self, last_page: u32) {
        if self
            .root
            .as_deref()
            .is_some_and(|node| has_after(node, ROOT_SHIFT, last_page))
        {
            truncate_at(&mut self.root, ROOT_SHIFT, last_page);
        }
    }

    /// Prepare a new image from only the transaction's changed pages.
    ///
    /// `Some` replaces a page and `None` removes it. Repeated identifiers are
    /// applied in input order. This does not publish the result: the caller must
    /// finish its transaction protocol before replacing the committed image.
    pub fn with_changes<I>(&self, changes: I) -> Self
    where
        I: IntoIterator<Item = (u32, Option<Arc<T>>)>,
    {
        let mut next = self.clone();
        for (page, value) in changes {
            if let Some(value) = value {
                next.insert_shared(page, value);
            } else {
                next.remove(page);
            }
        }
        next
    }

    /// Iterate present pages in ascending identifier order, without flattening.
    pub fn iter(&self) -> Iter<'_, T> {
        let mut stack = Vec::with_capacity(LEVELS);
        if let Some(node) = self.root.as_deref() {
            stack.push(Frame {
                node,
                prefix: 0,
                shift: ROOT_SHIFT,
                next: 0,
            });
        }
        Iter {
            stack,
            remaining: self.len(),
        }
    }
}

fn insert_at<T>(
    slot: &mut Option<Arc<Node<T>>>,
    shift: u32,
    page: u32,
    value: Arc<T>,
) -> Option<Arc<T>> {
    let node = slot.get_or_insert_with(|| Arc::new(Node::empty(shift)));
    let index = digit(page, shift);
    match Arc::make_mut(node) {
        Node::Leaf { len, values } => {
            let previous = values[index].replace(value);
            *len += usize::from(previous.is_none());
            previous
        }
        Node::Branch { len, children } => {
            let previous = insert_at(&mut children[index], shift - DIGIT_BITS, page, value);
            *len += usize::from(previous.is_none());
            previous
        }
    }
}

fn remove_at<T>(
    slot: &mut Option<Arc<Node<T>>>,
    shift: u32,
    page: u32,
) -> Option<Arc<T>> {
    let node = Arc::make_mut(slot.as_mut()?);
    let index = digit(page, shift);
    let previous = match node {
        Node::Leaf { len, values } => {
            let previous = values[index].take();
            *len -= usize::from(previous.is_some());
            previous
        }
        Node::Branch { len, children } => {
            let previous = remove_at(&mut children[index], shift - DIGIT_BITS, page);
            *len -= usize::from(previous.is_some());
            previous
        }
    };
    if node.len() == 0 {
        *slot = None;
    }
    previous
}

fn has_after<T>(node: &Node<T>, shift: u32, last_page: u32) -> bool {
    let index = digit(last_page, shift);
    match node {
        Node::Leaf { values, .. } => values[index + 1..].iter().any(Option::is_some),
        Node::Branch { children, .. } => {
            children[index + 1..].iter().any(Option::is_some)
                || children[index]
                    .as_deref()
                    .is_some_and(|child| has_after(child, shift - DIGIT_BITS, last_page))
        }
    }
}

fn truncate_at<T>(slot: &mut Option<Arc<Node<T>>>, shift: u32, last_page: u32) -> usize {
    let Some(shared) = slot.as_mut() else {
        return 0;
    };
    let node = Arc::make_mut(shared);
    let index = digit(last_page, shift);
    let removed = match node {
        Node::Leaf { len, values } => {
            let mut removed = 0;
            for value in &mut values[index + 1..] {
                removed += usize::from(value.take().is_some());
            }
            *len -= removed;
            removed
        }
        Node::Branch { len, children } => {
            let mut removed = 0;
            for child in &mut children[index + 1..] {
                if let Some(child) = child.take() {
                    removed += child.len();
                }
            }
            removed += truncate_at(&mut children[index], shift - DIGIT_BITS, last_page);
            *len -= removed;
            removed
        }
    };
    if node.len() == 0 {
        *slot = None;
    }
    removed
}

struct Frame<'a, T> {
    node: &'a Node<T>,
    prefix: u32,
    shift: u32,
    next: usize,
}

/// Ordered borrowed iterator over a [`PersistentPageMap`].
pub struct Iter<'a, T> {
    stack: Vec<Frame<'a, T>>,
    remaining: usize,
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = (u32, &'a T);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let frame = self.stack.last_mut()?;
            if frame.next == RADIX {
                self.stack.pop();
                continue;
            }
            let index = frame.next;
            frame.next += 1;
            let page = frame.prefix | ((index as u32) << frame.shift);
            match frame.node {
                Node::Leaf { values, .. } => {
                    if let Some(value) = &values[index] {
                        self.remaining -= 1;
                        return Some((page, value.as_ref()));
                    }
                }
                Node::Branch { children, .. } => {
                    if let Some(child) = children[index].as_deref() {
                        let next = Frame {
                            node: child,
                            prefix: page,
                            shift: frame.shift - DIGIT_BITS,
                            next: 0,
                        };
                        self.stack.push(next);
                    }
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl<T> ExactSizeIterator for Iter<'_, T> {}
impl<T> FusedIterator for Iter<'_, T> {}

impl<'a, T> IntoIterator for &'a PersistentPageMap<T> {
    type Item = (u32, &'a T);
    type IntoIter = Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, VecDeque};

    // Count only new directory nodes. Shared subtrees are never traversed.
    fn changed_nodes<T>(new: &Arc<Node<T>>, old: Option<&Arc<Node<T>>>) -> usize {
        if old.is_some_and(|old| Arc::ptr_eq(new, old)) {
            return 0;
        }
        match new.as_ref() {
            Node::Leaf { .. } => 1,
            Node::Branch { children, .. } => {
                let old_children = old.and_then(|old| match old.as_ref() {
                    Node::Branch { children, .. } => Some(children),
                    Node::Leaf { .. } => None,
                });
                1 + children
                    .iter()
                    .enumerate()
                    .filter_map(|(index, child)| {
                        child.as_ref().map(|child| {
                            changed_nodes(child, old_children.and_then(|old| old[index].as_ref()))
                        })
                    })
                    .sum::<usize>()
            }
        }
    }

    #[test]
    fn non_clone_payloads_are_shared_and_rollback_is_isolated() {
        struct Payload(u64);
        let mut committed = PersistentPageMap::new();
        committed.insert(1, Payload(10));
        committed.insert(2, Payload(20));
        let mut staged = committed.clone();
        assert!(Arc::ptr_eq(
            committed.root.as_ref().unwrap(),
            staged.root.as_ref().unwrap()
        ));
        assert_eq!(staged.insert(1, Payload(11)).unwrap().0, 10);
        assert_eq!(staged.remove(2).unwrap().0, 20);
        staged.insert(3, Payload(30));
        assert_eq!(staged.get(1).unwrap().0, 11);
        assert!(staged.get(2).is_none());
        assert_eq!(committed.get(1).unwrap().0, 10);
        assert_eq!(committed.get(2).unwrap().0, 20);
        assert!(committed.get(3).is_none());
        drop(staged);
        assert_eq!(committed.len(), 2);
    }

    #[test]
    fn one_page_publication_has_bounded_work_with_32_mib_resident() {
        const HOT: u32 = 0x8000_0000;
        let mut committed = PersistentPageMap::new();
        committed.insert(HOT, vec![0_u8; 4096]);
        let mut resident = 0;
        for target in [0, 128, 512, 2048, 8192] {
            while resident < target {
                resident += 1;
                committed.insert(resident, vec![0xab; 4096]);
            }
            for revision in 1..=64_u8 {
                let previous = committed.clone();
                let expected = previous.get(HOT).unwrap()[0];
                committed.insert(HOT, vec![revision; 4096]);
                assert_eq!(
                    changed_nodes(committed.root.as_ref().unwrap(), previous.root.as_ref()),
                    LEVELS,
                    "publication grew with {resident} unrelated resident pages"
                );
                assert_eq!(committed.len(), (resident + 1) as usize);
                assert_eq!(previous.get(HOT).unwrap()[0], expected);
                assert_eq!(committed.get(HOT).unwrap()[0], revision);
                if resident > 0 {
                    for page in [1, resident] {
                        assert!(Arc::ptr_eq(
                            committed.get_shared(page).unwrap(),
                            previous.get_shared(page).unwrap()
                        ));
                        assert_eq!(committed.get(page).unwrap()[0], 0xab);
                    }
                }
            }
        }
    }

    #[test]
    fn truncate_boundaries_and_regrowth_do_not_resurrect_pages() {
        let keys = [
            0, 1, 15, 16, 17, 255, 256, 257, 65535, 65536, u32::MAX - 1, u32::MAX,
        ];
        let mut base = PersistentPageMap::new();
        for key in keys {
            base.insert(key, key);
        }
        for limit in keys {
            let mut image = base.clone();
            image.truncate(limit);
            let expected: Vec<_> = keys.into_iter().filter(|key| *key <= limit).collect();
            assert_eq!(image.iter().map(|(key, _)| key).collect::<Vec<_>>(), expected);
            assert_eq!(image.len(), expected.len());
            assert_eq!(base.len(), keys.len());
            image.insert(u32::MAX, 42);
            assert_eq!(image.get(u32::MAX), Some(&42));
            for key in keys {
                assert_eq!(base.get(key), Some(&key));
                if key > limit && key != u32::MAX {
                    assert!(image.get(key).is_none());
                }
            }
        }
    }

    #[test]
    fn missing_deletions_and_noop_truncation_keep_the_same_root() {
        let mut image = PersistentPageMap::new();
        image.insert(16, 1);
        let previous = image.clone();
        assert_eq!(image.remove(17), None);
        image.truncate(16);
        image.truncate(u32::MAX);
        assert!(Arc::ptr_eq(
            image.root.as_ref().unwrap(),
            previous.root.as_ref().unwrap()
        ));
        assert_eq!(image.remove(16).as_deref(), Some(&1));
        assert!(image.is_empty());
        assert_eq!(image.len(), 0);
        assert_eq!(previous.get(16), Some(&1));
    }

    #[test]
    fn history_eviction_does_not_turn_reads_into_delta_replay() {
        let mut current = PersistentPageMap::new();
        let mut history = VecDeque::new();
        for sequence in 0..1024_u32 {
            current.insert(7, sequence);
            current.insert(u32::MAX, sequence + 1);
            history.push_back((sequence, current.clone()));
            if history.len() > 32 {
                history.pop_front();
            }
            for (expected, image) in &history {
                assert_eq!(image.get(7), Some(expected));
                assert_eq!(image.get(u32::MAX), Some(&(expected + 1)));
            }
        }
    }

    #[test]
    fn mixed_forks_match_ordered_map_oracle() {
        let mut actual = PersistentPageMap::new();
        let mut expected = BTreeMap::new();
        let mut seed = 0x494_u64;
        let mut history = VecDeque::new();
        for step in 0..4000_u32 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let key = (seed as u32).rotate_left(step % 32);
            match step % 7 {
                0 => {
                    actual.truncate(key);
                    expected.retain(|page, _| *page <= key);
                }
                1 => {
                    let victim = expected.keys().next().copied().unwrap_or(key);
                    assert_eq!(
                        actual.remove(victim).as_deref(),
                        expected.remove(&victim).as_ref()
                    );
                }
                _ => {
                    assert_eq!(
                        actual.insert(key, step).as_deref(),
                        expected.insert(key, step).as_ref()
                    );
                }
            }
            if step % 31 == 0 {
                history.push_back((actual.clone(), expected.clone()));
                if history.len() > 16 {
                    history.pop_front();
                }
            }
            assert_matches(&actual, &expected);
            for (image, oracle) in &history {
                assert_matches(image, oracle);
            }
        }
    }

    fn assert_matches(image: &PersistentPageMap<u32>, oracle: &BTreeMap<u32, u32>) {
        assert_eq!(image.len(), oracle.len());
        assert_eq!(
            image.iter().map(|(key, value)| (key, *value)).collect::<Vec<_>>(),
            oracle.iter().map(|(key, value)| (*key, *value)).collect::<Vec<_>>()
        );
        for (key, value) in oracle {
            assert_eq!(image.get(*key), Some(value));
        }
    }

    #[test]
    fn write_set_forks_apply_deletes_and_repeated_keys_in_order() {
        let mut committed = PersistentPageMap::new();
        committed.insert(1, 10);
        committed.insert(2, 20);
        committed.insert(3, 30);
        let staged = committed.with_changes([
            (1, Some(Arc::new(11))),
            (2, None),
            (4, Some(Arc::new(40))),
            (1, None),
            (1, Some(Arc::new(12))),
        ]);
        assert_eq!(staged.len(), 3);
        assert_eq!(staged.get(1), Some(&12));
        assert_eq!(staged.get(2), None);
        assert_eq!(staged.get(4), Some(&40));
        assert_eq!(committed.get(1), Some(&10));
        assert_eq!(committed.get(2), Some(&20));
        assert_eq!(committed.get(4), None);
        assert!(Arc::ptr_eq(
            staged.get_shared(3).unwrap(),
            committed.get_shared(3).unwrap()
        ));
    }

    #[test]
    fn iterator_reports_exact_remaining_length_and_is_fused() {
        let mut image = PersistentPageMap::new();
        image.insert(42, 1);
        image.insert(0, 2);
        image.insert(u32::MAX, 3);
        let mut iter = image.iter();
        for expected in [(0, &2), (42, &1), (u32::MAX, &3)] {
            assert_eq!(iter.size_hint(), (iter.len(), Some(iter.len())));
            assert_eq!(iter.next(), Some(expected));
        }
        assert_eq!(iter.len(), 0);
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);
    }
}
