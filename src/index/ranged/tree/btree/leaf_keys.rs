// Prefix-compressed key storage for external (leaf) pages.
//
// A page stores one shared prefix and fixed-width suffixes, like the
// internal nodes' InternalKeys, but supports in-place mutation on the hot
// insert path.
//
// Concurrency contract: mutations happen only under the page's write latch.
// Optimistic readers (read_node closures, pinned in an epoch) may observe
// torn SUFFIX BYTES — the seqlock version check discards such reads — but
// must never observe invalid memory. Therefore the buffer is reached through
// a single atomic pointer to a self-describing allocation (prefix, suffix
// width and capacity live in the same allocation as the data): a reader
// dereferences one pointer and gets internally consistent bounds no matter
// how stale it is. Structural changes (prefix shrink, rebuilds) allocate a
// new buffer, swap the pointer, and retire the old buffer through
// crossbeam-epoch — the same swap-and-defer protocol verified in
// docs/tla/SeqlockReclaim.tla (readers hold no reference past their pinned
// section, so the refcount-resurrection cases do not apply).
use crate::index::entry::ID_SIZE;
use crate::index::{EntryKey, KEY_SIZE};
use crate::ram::types::Id;
use std::sync::atomic::{AtomicPtr, Ordering};

struct SuffixBuf {
    prefix: [u8; KEY_SIZE],
    prefix_len: usize,
    // capacity in ENTRIES (each entry is suffix_len bytes)
    capacity: usize,
    data: Vec<u8>, // capacity * suffix_len bytes, len == capacity * suffix_len
}

impl SuffixBuf {
    fn suffix_len(&self) -> usize {
        KEY_SIZE - self.prefix_len
    }

    fn alloc(prefix: &[u8], capacity: usize) -> Box<SuffixBuf> {
        let prefix_len = prefix.len();
        let mut p = [0u8; KEY_SIZE];
        p[..prefix_len].copy_from_slice(prefix);
        Box::new(SuffixBuf {
            prefix: p,
            prefix_len,
            capacity,
            data: vec![0u8; capacity * (KEY_SIZE - prefix_len)],
        })
    }

    fn entry(&self, index: usize) -> &[u8] {
        let w = self.suffix_len();
        &self.data[index * w..(index + 1) * w]
    }

    fn write_entry(&mut self, index: usize, key: &EntryKey) {
        let w = self.suffix_len();
        let pl = self.prefix_len;
        self.data[index * w..(index + 1) * w].copy_from_slice(&key.as_slice()[pl..]);
    }

    fn key_at(&self, index: usize) -> EntryKey {
        let mut key = EntryKey::new();
        key.as_mut_slice()[..self.prefix_len].copy_from_slice(&self.prefix[..self.prefix_len]);
        key.as_mut_slice()[self.prefix_len..].copy_from_slice(self.entry(index));
        key
    }

    fn cmp_at(&self, index: usize, key: &EntryKey) -> std::cmp::Ordering {
        let kb = key.as_slice();
        match self.prefix[..self.prefix_len].cmp(&kb[..self.prefix_len]) {
            std::cmp::Ordering::Equal => self.entry(index).cmp(&kb[self.prefix_len..]),
            other => other,
        }
    }

    // Longest prefix shared by the current prefix and `key`.
    fn common_prefix_with(&self, key: &EntryKey) -> usize {
        let kb = key.as_slice();
        let mut i = 0;
        while i < self.prefix_len && self.prefix[i] == kb[i] {
            i += 1;
        }
        i
    }
}

pub struct LeafKeys {
    buf: AtomicPtr<SuffixBuf>,
}

// A snapshot of a key range in compressed form: the shared prefix is
// pre-filled into a template key and only the suffix bytes are stored.
// Reconstruction happens per yielded key instead of per snapshotted key.
pub struct PackedKeys {
    template: EntryKey,
    prefix_len: usize,
    width: usize,
    suffixes: Vec<u8>,
    len: usize,
}

impl PackedKeys {
    pub fn empty() -> Self {
        PackedKeys {
            template: EntryKey::new(),
            prefix_len: KEY_SIZE,
            width: 0,
            suffixes: Vec::new(),
            len: 0,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn key(&self, index: usize) -> EntryKey {
        debug_assert!(index < self.len);
        let mut key = self.template.clone();
        if self.width > 0 {
            key.as_mut_slice()[self.prefix_len..].copy_from_slice(
                &self.suffixes[index * self.width..(index + 1) * self.width],
            );
        }
        key
    }

    // Extract only the Id (the key's trailing ID_SIZE bytes) without
    // materializing the full key: consumers that resolve ids (the common
    // case) skip one 32-byte reconstruction per yield.
    #[inline]
    pub fn id_at(&self, index: usize) -> Id {
        debug_assert!(index < self.len);
        const ID_START: usize = KEY_SIZE - ID_SIZE;
        let mut id_bytes = [0u8; ID_SIZE];
        let pl = self.prefix_len;
        if pl > ID_START {
            // Part (or all) of the id is shared in the prefix.
            let shared = pl - ID_START;
            id_bytes[..shared].copy_from_slice(&self.template.as_slice()[ID_START..pl]);
            let sfx = &self.suffixes[index * self.width..(index + 1) * self.width];
            id_bytes[shared..].copy_from_slice(sfx);
        } else {
            // The id lies entirely inside the suffix.
            let off = ID_START - pl;
            let sfx = &self.suffixes[index * self.width..(index + 1) * self.width];
            id_bytes[..].copy_from_slice(&sfx[off..off + ID_SIZE]);
        }
        Id::from_bits(u64::from_be_bytes(id_bytes))
    }
}

fn common_prefix_of(keys: &[EntryKey]) -> usize {
    if keys.is_empty() {
        return KEY_SIZE;
    }
    let first = keys[0].as_slice();
    let mut plen = KEY_SIZE;
    for key in &keys[1..] {
        let bytes = key.as_slice();
        let mut i = 0;
        while i < plen && bytes[i] == first[i] {
            i += 1;
        }
        plen = i;
        if plen == 0 {
            break;
        }
    }
    plen
}

impl LeafKeys {
    pub fn new(capacity: usize) -> Self {
        // Empty page: full-length prefix of zeros, zero-width suffixes; the
        // first divergence rebuilds with the right prefix.
        LeafKeys {
            buf: AtomicPtr::new(Box::into_raw(SuffixBuf::alloc(&[0u8; KEY_SIZE], capacity))),
        }
    }

    pub fn from_keys(keys: &[EntryKey], capacity: usize) -> Self {
        debug_assert!(keys.len() <= capacity);
        let plen = common_prefix_of(keys);
        let mut prefix = [0u8; KEY_SIZE];
        if let Some(first) = keys.first() {
            prefix.copy_from_slice(first.as_slice());
        }
        let mut b = SuffixBuf::alloc(&prefix[..plen], capacity);
        for (i, k) in keys.iter().enumerate() {
            b.write_entry(i, k);
        }
        LeafKeys {
            buf: AtomicPtr::new(Box::into_raw(b)),
        }
    }

    #[inline]
    fn load(&self) -> &SuffixBuf {
        // Readers: pinned optimistic reads; writers: under the page latch.
        unsafe { &*self.buf.load(Ordering::Acquire) }
    }

    #[inline]
    fn load_mut(&mut self) -> &mut SuffixBuf {
        unsafe { &mut *self.buf.load(Ordering::Relaxed) }
    }

    // Publish a rebuilt buffer and retire the old one after the current
    // epoch's readers are done.
    fn swap_buf(&self, new: Box<SuffixBuf>) {
        let old = self.buf.swap(Box::into_raw(new), Ordering::Release);
        let guard = crossbeam_epoch::pin();
        unsafe {
            guard.defer_unchecked(move || {
                drop(Box::from_raw(old));
            });
        }
    }

    pub fn capacity(&self) -> usize {
        self.load().capacity
    }

    pub fn key_at(&self, index: usize) -> EntryKey {
        self.load().key_at(index)
    }

    pub fn cmp_at(&self, index: usize, key: &EntryKey) -> std::cmp::Ordering {
        self.load().cmp_at(index, key)
    }

    // Copy a range without reconstructing the keys: one memcpy of the
    // suffix bytes. Used by cursor snapshots when no tombstone filtering is
    // needed.
    pub fn packed_snapshot(&self, range: std::ops::Range<usize>) -> PackedKeys {
        let b = self.load();
        let w = b.suffix_len();
        let pl = b.prefix_len;
        let mut template = EntryKey::new();
        template.as_mut_slice()[..pl].copy_from_slice(&b.prefix[..pl]);
        PackedKeys {
            template,
            prefix_len: pl,
            width: w,
            suffixes: b.data[range.start * w..range.end * w].to_vec(),
            len: range.len(),
        }
    }

    pub fn to_vec(&self, range: std::ops::Range<usize>) -> Vec<EntryKey> {
        let b = self.load();
        let w = b.suffix_len();
        let pl = b.prefix_len;
        // Template key with the shared prefix pre-filled; per key only the
        // suffix bytes are copied. chunks_exact keeps the loop branch-free.
        let mut template = EntryKey::new();
        template.as_mut_slice()[..pl].copy_from_slice(&b.prefix[..pl]);
        let mut out = Vec::with_capacity(range.len());
        if w == 0 {
            out.resize(range.len(), template);
            return out;
        }
        for suffix in b.data[range.start * w..range.end * w].chunks_exact(w) {
            let mut key = template.clone();
            key.as_mut_slice()[pl..].copy_from_slice(suffix);
            out.push(key);
        }
        out
    }

    // Lower-bound search over `len` keys: index of the first key >= `key`.
    pub fn search(&self, len: usize, key: &EntryKey) -> usize {
        let b = self.load();
        let kb = key.as_slice();
        match b.prefix[..b.prefix_len].cmp(&kb[..b.prefix_len]) {
            std::cmp::Ordering::Greater => return 0,   // all keys > key
            std::cmp::Ordering::Less => return len,    // all keys < key
            std::cmp::Ordering::Equal => {}
        }
        let suffix = &kb[b.prefix_len..];
        let mut left = 0;
        let mut right = len;
        while left < right {
            let mid = left + (right - left) / 2;
            match b.entry(mid).cmp(suffix) {
                std::cmp::Ordering::Less => left = mid + 1,
                std::cmp::Ordering::Greater => right = mid,
                std::cmp::Ordering::Equal => return mid,
            }
        }
        left
    }

    // Insert `key` at `pos` (caller guarantees order and capacity).
    pub fn insert_at(&mut self, key: EntryKey, pos: usize, len: &mut usize) {
        debug_assert!(pos <= *len);
        debug_assert!(*len < self.capacity());
        let needs_rebuild = {
            let b = self.load();
            b.common_prefix_with(&key) < b.prefix_len
        };
        if needs_rebuild {
            // The new key diverges inside the shared prefix: shrink it and
            // re-lay all suffixes in a fresh buffer.
            let b = self.load();
            let new_plen = b.common_prefix_with(&key);
            let capacity = b.capacity;
            let mut keys = self.to_vec(0..*len);
            keys.insert(pos, key);
            let mut nb = SuffixBuf::alloc(&keys[0].as_slice()[..new_plen], capacity);
            for (i, k) in keys.iter().enumerate() {
                nb.write_entry(i, k);
            }
            self.swap_buf(nb);
        } else {
            let b = self.load_mut();
            let w = b.suffix_len();
            b.data.copy_within(pos * w..*len * w, (pos + 1) * w);
            b.write_entry(pos, &key);
        }
        *len += 1;
    }

    pub fn remove_at(&mut self, pos: usize, len: &mut usize) {
        debug_assert!(pos < *len);
        let b = self.load_mut();
        let w = b.suffix_len();
        b.data.copy_within((pos + 1) * w..*len * w, pos * w);
        *len -= 1;
    }

    // Replace the whole content atomically (swap + epoch retire); safe
    // against concurrent optimistic readers of the old buffer.
    pub fn set(&self, keys: &[EntryKey]) {
        let capacity = self.capacity().max(keys.len()).max(1);
        let plen = common_prefix_of(keys);
        let mut prefix = [0u8; KEY_SIZE];
        if let Some(first) = keys.first() {
            prefix.copy_from_slice(first.as_slice());
        }
        let mut nb = SuffixBuf::alloc(&prefix[..plen], capacity);
        for (i, k) in keys.iter().enumerate() {
            nb.write_entry(i, k);
        }
        self.swap_buf(nb);
    }

    // Replace the whole content (bulk rebuild under the page latch).
    pub fn set_from(&mut self, keys: &[EntryKey], len: &mut usize) {
        let capacity = self.capacity();
        debug_assert!(keys.len() <= capacity);
        let plen = common_prefix_of(keys);
        let mut prefix = [0u8; KEY_SIZE];
        if let Some(first) = keys.first() {
            prefix.copy_from_slice(first.as_slice());
        }
        let mut nb = SuffixBuf::alloc(&prefix[..plen], capacity);
        for (i, k) in keys.iter().enumerate() {
            nb.write_entry(i, k);
        }
        self.swap_buf(nb);
        *len = keys.len();
    }

    // Split: keys [pivot..len) move to the returned LeafKeys; self keeps
    // [0..pivot).
    pub fn split_off(&mut self, pivot: usize, len: usize) -> LeafKeys {
        let right_keys = self.to_vec(pivot..len);
        LeafKeys::from_keys(&right_keys, self.capacity())
    }
}

impl Drop for LeafKeys {
    fn drop(&mut self) {
        // Defer the buffer free: a LeafKeys can be dropped by replacing a
        // node's key structure while pinned optimistic readers still hold a
        // stale view of the old buffer (docs/tla/SeqlockReclaim.tla).
        let old = self.buf.load(Ordering::Relaxed);
        let guard = crossbeam_epoch::pin();
        unsafe {
            guard.defer_unchecked(move || {
                drop(Box::from_raw(old));
            });
        }
    }
}

impl std::fmt::Debug for LeafKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let b = self.load();
        write!(
            f,
            "LeafKeys(prefix_len={}, cap={})",
            b.prefix_len, b.capacity
        )
    }
}

unsafe impl Send for LeafKeys {}
unsafe impl Sync for LeafKeys {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ram::types::Id;
    use rand::prelude::*;

    fn key_of(h: u64, l: u64) -> EntryKey {
        EntryKey::from_id(&Id::allocated(h as u16, 0, l))
    }

    #[test]
    fn basic_ops() {
        let mut lk = LeafKeys::new(8);
        let mut len = 0;
        lk.insert_at(key_of(1, 10), 0, &mut len);
        lk.insert_at(key_of(1, 30), 1, &mut len);
        lk.insert_at(key_of(1, 20), 1, &mut len);
        assert_eq!(len, 3);
        assert_eq!(lk.key_at(0), key_of(1, 10));
        assert_eq!(lk.key_at(1), key_of(1, 20));
        assert_eq!(lk.key_at(2), key_of(1, 30));
        assert_eq!(lk.search(len, &key_of(1, 20)), 1);
        assert_eq!(lk.search(len, &key_of(1, 25)), 2);
        assert_eq!(lk.search(len, &key_of(0, 0)), 0);
        assert_eq!(lk.search(len, &key_of(9, 0)), 3);
        // Diverging key forces a prefix rebuild.
        lk.insert_at(key_of(2, 5), 3, &mut len);
        assert_eq!(lk.key_at(3), key_of(2, 5));
        assert_eq!(lk.key_at(0), key_of(1, 10));
        lk.remove_at(0, &mut len);
        assert_eq!(lk.key_at(0), key_of(1, 20));
        assert_eq!(len, 3);
        let right = lk.split_off(1, len);
        assert_eq!(right.key_at(0), key_of(1, 30));
        assert_eq!(right.key_at(1), key_of(2, 5));
    }

    #[test]
    fn packed_id_extraction() {
        // prefix covering part of the id (shared higher half)
        let keys: Vec<EntryKey> = (0..10u64).map(|n| key_of(7, n * 3)).collect();
        let lk = LeafKeys::from_keys(&keys, 16);
        let packed = lk.packed_snapshot(0..10);
        for (i, k) in keys.iter().enumerate() {
            assert_eq!(packed.id_at(i), k.id(), "id mismatch at {}", i);
            assert_eq!(packed.key(i), *k);
        }
        // prefix shorter than the id boundary (diverging higher halves)
        let keys2: Vec<EntryKey> = (0..10u64).map(|n| key_of(n + 1, n)).collect();
        let lk2 = LeafKeys::from_keys(&keys2, 16);
        let packed2 = lk2.packed_snapshot(0..10);
        for (i, k) in keys2.iter().enumerate() {
            assert_eq!(packed2.id_at(i), k.id(), "id mismatch at {}", i);
        }
        // single key: prefix is the whole key, zero-width suffixes
        let keys3 = vec![key_of(3, 9)];
        let lk3 = LeafKeys::from_keys(&keys3, 4);
        let packed3 = lk3.packed_snapshot(0..1);
        assert_eq!(packed3.id_at(0), keys3[0].id());
    }

    // Differential test against a plain Vec<EntryKey> model.
    #[test]
    fn differential_against_vec_model() {
        let mut rng = rand::rng();
        for _round in 0..200 {
            const CAP: usize = 32;
            let mut lk = LeafKeys::new(CAP);
            let mut len = 0usize;
            let mut model: Vec<EntryKey> = Vec::new();
            for _op in 0..300 {
                let op = rng.random_range(0..100);
                if op < 60 && model.len() < CAP {
                    // random key, sometimes prefix-hostile
                    let k = if rng.random_bool(0.9) {
                        key_of(1, rng.random_range(0..1_000_000))
                    } else {
                        key_of(rng.random_range(0..u64::MAX), rng.random())
                    };
                    let pos = match model.binary_search(&k) {
                        Ok(_) => continue, // no dups
                        Err(p) => p,
                    };
                    model.insert(pos, k.clone());
                    lk.insert_at(k, pos, &mut len);
                } else if op < 80 && !model.is_empty() {
                    let pos = rng.random_range(0..model.len());
                    model.remove(pos);
                    lk.remove_at(pos, &mut len);
                } else if op < 90 && !model.is_empty() {
                    let probe = key_of(1, rng.random_range(0..1_000_000));
                    let expect = model.binary_search(&probe).unwrap_or_else(|p| p);
                    assert_eq!(lk.search(len, &probe), expect, "search mismatch");
                } else if !model.is_empty() {
                    let pivot = rng.random_range(0..=model.len());
                    let right_model = model.split_off(pivot);
                    let right = lk.split_off(pivot, len);
                    len = pivot;
                    for (i, k) in right_model.iter().enumerate() {
                        assert_eq!(&right.key_at(i), k, "split content mismatch");
                    }
                }
                assert_eq!(len, model.len());
                for (i, k) in model.iter().enumerate() {
                    assert_eq!(&lk.key_at(i), k, "content diverged at {}", i);
                }
            }
        }
    }
}
