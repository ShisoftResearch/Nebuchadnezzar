//! Lightning lookup with compound mutation confined to one key.
//! The optional ordered index streams keys without copying the map.

use lightning::map::{Map, PtrHashMap, SwiftKeyProbe};
use parking_lot::{ArcMutexGuard, Mutex, RawMutex};
use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

/// Publish a complete value atomically. The pinned Lightning revision's
/// get_or_insert can remove another insertion's active temporary marker.
pub(crate) fn get_or_insert<K, V>(map: &PtrHashMap<K, V>, key: K, create: impl FnOnce() -> V) -> V
where
    K: Clone + Hash + SwiftKeyProbe,
    V: Clone,
{
    if let Some(value) = map.get(&key) {
        return value;
    }
    let value = create();
    map.try_insert(key, value.clone()).unwrap_or(value)
}

struct State<V> {
    registered: bool,
    listed: bool,
    value: Option<V>,
}

pub(crate) struct KeyedState<K: Clone + Hash + SwiftKeyProbe + Ord + Send + Sync + 'static, V> {
    map: PtrHashMap<K, Arc<Mutex<State<V>>>>,
    keys: Option<crossbeam_skiplist::SkipSet<K>>,
}

impl<K: Clone + Hash + SwiftKeyProbe + Ord + Send + Sync + 'static, V> Default
    for KeyedState<K, V>
{
    fn default() -> Self {
        Self {
            map: PtrHashMap::with_capacity(128),
            keys: None,
        }
    }
}

impl<K: Clone + Hash + SwiftKeyProbe + Ord + Send + Sync + 'static, V> KeyedState<K, V> {
    pub(crate) fn ordered() -> Self {
        Self {
            keys: Some(crossbeam_skiplist::SkipSet::new()),
            ..Self::default()
        }
    }

    pub(crate) fn entry(&self, key: &K) -> StateGuard<'_, K, V> {
        loop {
            let state = self.map.get(key).unwrap_or_else(|| {
                let new = Arc::new(Mutex::new(State {
                    registered: true,
                    listed: false,
                    value: None,
                }));
                self.map.try_insert(key.clone(), new.clone()).unwrap_or(new)
            });
            let guard = state.lock_arc();
            // Removal clears this flag under the same per-key mutex. A stale
            // lookup retries instead of publishing into an unlinked object.
            if guard.registered {
                return StateGuard {
                    registry: self,
                    key: key.clone(),
                    guard,
                };
            }
        }
    }

    pub(crate) fn keys(&self) -> impl Iterator<Item = K> + '_ {
        self.keys
            .as_ref()
            .expect("ordered registry")
            .iter()
            .map(|entry| entry.value().clone())
    }
}

pub(crate) struct StateGuard<'a, K: Clone + Hash + SwiftKeyProbe + Ord + Send + Sync + 'static, V> {
    registry: &'a KeyedState<K, V>,
    key: K,
    guard: ArcMutexGuard<RawMutex, State<V>>,
}

impl<K: Clone + Hash + SwiftKeyProbe + Ord + Send + Sync + 'static, V> Deref
    for StateGuard<'_, K, V>
{
    type Target = Option<V>;
    fn deref(&self) -> &Self::Target {
        &self.guard.value
    }
}
impl<K: Clone + Hash + SwiftKeyProbe + Ord + Send + Sync + 'static, V> DerefMut
    for StateGuard<'_, K, V>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard.value
    }
}
impl<K: Clone + Hash + SwiftKeyProbe + Ord + Send + Sync + 'static, V> Drop
    for StateGuard<'_, K, V>
{
    fn drop(&mut self) {
        if self.guard.value.is_none() {
            if self.guard.listed {
                self.registry.keys.as_ref().unwrap().remove(&self.key);
            }
            self.guard.registered = false;
            self.registry.map.remove(&self.key);
        } else if !self.guard.listed {
            if let Some(keys) = &self.registry.keys {
                keys.insert(self.key.clone());
                self.guard.listed = true;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn racing_constructors_publish_one_identity() {
        let map = PtrHashMap::with_capacity(16);
        let constructing = std::sync::Barrier::new(8);
        let values = std::thread::scope(|scope| {
            let threads: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        get_or_insert(&map, 7u16, || {
                            constructing.wait();
                            Arc::new(42)
                        })
                    })
                })
                .collect();
            threads
                .into_iter()
                .map(|thread| thread.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert!(values.iter().all(|value| Arc::ptr_eq(value, &values[0])));
        assert!(Arc::ptr_eq(&map.get(&7).unwrap(), &values[0]));
    }

    #[test]
    fn removal_and_recreation_preserve_every_keyed_update() {
        let registry = KeyedState::<u64, usize>::ordered();
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    for _ in 0..1000 {
                        let mut entry = registry.entry(&1);
                        let next = entry.take().unwrap_or(0) + 1;
                        *entry = Some(next);
                    }
                });
            }
        });
        assert_eq!(*registry.entry(&1), Some(8000));
        assert_eq!(registry.keys().collect::<Vec<_>>(), vec![1]);
        registry.entry(&1).take();
        assert_eq!(registry.keys().count(), 0);
        assert_eq!(*registry.entry(&1), None);
    }

    #[test]
    fn a_lookup_that_precedes_removal_cannot_republish_retired_state() {
        let registry = KeyedState::<u64, usize>::ordered();
        *registry.entry(&1) = Some(7);
        let old = registry.map.get(&1).unwrap();
        registry.entry(&1).take();
        *registry.entry(&1) = Some(9);
        assert!(!old.lock().registered);
        assert_eq!(old.lock().value, None);
        assert_eq!(*registry.entry(&1), Some(9));
        assert_eq!(registry.keys().collect::<Vec<_>>(), vec![1]);
    }

    #[test]
    fn unrelated_key_progresses_while_one_entry_is_held() {
        let registry = KeyedState::<u64, usize>::ordered();
        let mut held = registry.entry(&1);
        *held = Some(7);
        let (send, recv) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            scope.spawn(|| {
                *registry.entry(&2) = Some(9);
                send.send(()).unwrap();
            });
            let progress = recv.recv_timeout(std::time::Duration::from_secs(2));
            drop(held);
            assert!(progress.is_ok());
        });
        assert_eq!(registry.keys().collect::<Vec<_>>(), vec![1, 2]);
    }
}
