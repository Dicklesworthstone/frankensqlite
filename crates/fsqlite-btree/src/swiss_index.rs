//! Swiss Table implementation for SIMD-accelerated index lookups.
//!
//! Wraps `hashbrown::HashMap` to provide a drop-in replacement for `std::collections::HashMap`
//! with integrated observability (tracing spans and metrics) as required by §7.7.
//!
//! Uses SSE2/AVX2 control byte probing for cache-line parallel lookup (via hashbrown).

use crate::instrumentation::{record_swiss_probe, set_swiss_load_factor};
use hashbrown::HashMap;
use std::borrow::Borrow;
use std::hash::Hash;
use tracing::{Level, span};

/// A SIMD-accelerated hash map with integrated observability.
///
/// This structure is a thin wrapper around `hashbrown::HashMap` that automatically
/// emits `hash_probe` spans and updates `fsqlite_swiss_table_probes_total` and
/// `fsqlite_swiss_table_load_factor` metrics on operations.
#[derive(Debug, Clone, Default)]
pub struct SwissIndex<K, V> {
    inner: HashMap<K, V>,
}

impl<K, V> SwissIndex<K, V>
where
    K: Eq + Hash,
{
    /// Creates an empty `SwissIndex`.
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    /// Creates an empty `SwissIndex` with the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: HashMap::with_capacity(capacity),
        }
    }

    /// Returns the number of elements in the map.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true if the map contains no elements.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Inserts a key-value pair into the map.
    ///
    /// Triggers observability events.
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.record_probe();
        let result = self.inner.insert(key, value);
        self.update_load_factor();
        result
    }

    /// Returns a reference to the value corresponding to the key.
    ///
    /// Triggers observability events.
    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.record_probe();
        self.inner.get(key)
    }

    /// Returns a mutable reference to the value corresponding to the key.
    ///
    /// Triggers observability events.
    pub fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.record_probe();
        self.inner.get_mut(key)
    }

    /// Gets the given key's corresponding entry in the map for in-place manipulation.
    ///
    /// Triggers observability events.
    /// Gets the given key's corresponding entry for in-place manipulation,
    /// inserting a default value if the key is absent.
    ///
    /// Triggers observability events (probe count; load factor updated on
    /// next insert/remove since entry borrows prevent self-access).
    pub fn entry_or_insert_with(&mut self, key: K, default: impl FnOnce() -> V) -> &mut V {
        record_swiss_probe();
        self.inner.entry(key).or_insert_with(default)
    }

    /// Returns an iterator visiting all values in arbitrary order.
    pub fn values(&self) -> hashbrown::hash_map::Values<'_, K, V> {
        self.inner.values()
    }

    /// Returns true if the map contains a value for the specified key.
    ///
    /// Triggers observability events.
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.record_probe();
        self.inner.contains_key(key)
    }

    /// Removes a key from the map, returning the value at the key if the key
    /// was previously in the map.
    ///
    /// Triggers observability events.
    pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.record_probe();
        let result = self.inner.remove(key);
        self.update_load_factor();
        result
    }

    /// Clears the map, removing all key-value pairs.
    pub fn clear(&mut self) {
        self.inner.clear();
        self.update_load_factor();
    }

    /// An iterator visiting all key-value pairs in arbitrary order.
    pub fn iter(&self) -> hashbrown::hash_map::Iter<'_, K, V> {
        self.inner.iter()
    }

    /// An iterator visiting all key-value pairs in arbitrary order, with mutable references to the values.
    pub fn iter_mut(&mut self) -> hashbrown::hash_map::IterMut<'_, K, V> {
        self.inner.iter_mut()
    }

    // --- Observability Helpers ---

    #[inline]
    fn record_probe(&self) {
        record_swiss_probe();
        let span = span!(
            Level::TRACE,
            "hash_probe",
            probes = 1, // Simplified: hashbrown handles collision resolution internally
            items = self.len(),
            load_factor = self.load_factor_milli() as f64 / 1000.0
        );
        span.in_scope(|| {
            // Trace log is handled by the span entry/exit if configured
        });
    }

    #[inline]
    fn update_load_factor(&self) {
        set_swiss_load_factor(self.load_factor_milli());
    }

    #[inline]
    fn load_factor_milli(&self) -> u64 {
        let capacity = self.inner.capacity();
        if capacity == 0 {
            0
        } else {
            (self.inner.len() as u64 * 1000) / capacity as u64
        }
    }
}

impl<K, V, Q> std::ops::Index<&Q> for SwissIndex<K, V>
where
    K: Eq + Hash + Borrow<Q>,
    Q: Hash + Eq + ?Sized,
{
    type Output = V;

    fn index(&self, key: &Q) -> &V {
        self.inner.get(key).expect("no entry found for key")
    }
}

impl<K, V> IntoIterator for SwissIndex<K, V>
where
    K: Eq + Hash,
{
    type Item = (K, V);
    type IntoIter = hashbrown::hash_map::IntoIter<K, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

impl<'a, K, V> IntoIterator for &'a SwissIndex<K, V>
where
    K: Eq + Hash,
{
    type Item = (&'a K, &'a V);
    type IntoIter = hashbrown::hash_map::Iter<'a, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, K, V> IntoIterator for &'a mut SwissIndex<K, V>
where
    K: Eq + Hash,
{
    type Item = (&'a K, &'a mut V);
    type IntoIter = hashbrown::hash_map::IterMut<'a, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

impl<K, V> FromIterator<(K, V)> for SwissIndex<K, V>
where
    K: Eq + Hash,
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let inner = HashMap::from_iter(iter);
        let index = Self { inner };
        index.update_load_factor();
        index
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instrumentation::{btree_metrics_snapshot, reset_btree_metrics};

    #[test]
    fn test_swiss_index_basic_ops() {
        reset_btree_metrics();
        let mut map = SwissIndex::new();
        assert!(map.is_empty());

        map.insert("key1", 1);
        assert_eq!(map.len(), 1);
        assert_eq!(map.get("key1"), Some(&1));
        assert!(map.contains_key("key1"));

        map.insert("key2", 2);
        assert_eq!(map.len(), 2);

        assert_eq!(map.remove("key1"), Some(1));
        assert_eq!(map.len(), 1);
        assert!(!map.contains_key("key1"));

        // Check metrics
        let metrics = btree_metrics_snapshot();
        // probes: insert(1) + get(1) + contains(1) + insert(1) + remove(1) + contains(1 check) = 6
        assert_eq!(metrics.fsqlite_swiss_table_probes_total, 6);
        assert!(metrics.fsqlite_swiss_table_load_factor > 0);
    }

    #[test]
    fn test_swiss_index_capacity_and_load_factor() {
        reset_btree_metrics();
        let mut map = SwissIndex::with_capacity(100);
        assert_eq!(map.load_factor_milli(), 0);

        for i in 0..50 {
            map.insert(i, i * 10);
        }

        let metrics = btree_metrics_snapshot();
        assert_eq!(metrics.fsqlite_swiss_table_probes_total, 50);
        // Load factor should be roughly 50% (capacity might be > 100 due to power of 2 sizing)
        assert!(metrics.fsqlite_swiss_table_load_factor > 0);
        assert!(metrics.fsqlite_swiss_table_load_factor < 1000);
    }
}
