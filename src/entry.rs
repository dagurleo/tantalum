//! A thin, refcounted key+value allocation.
//!
//! `KeyValue` is a single pointer to `[Header][key bytes][value bytes]`, the
//! value with room to grow if it was given some. Compared with `Arc<[u8]>`
//! (16-byte fat pointer, 16-byte header) this keeps table slots small and the
//! per-entry header at 16 bytes.

use std::alloc::{self, Layout};
use std::ptr::{self, NonNull};
use std::slice;
use std::sync::atomic::{AtomicU32, Ordering, fence};

#[repr(C)]
struct Header {
    refs: AtomicU32,
    key_len: u32,
    value_len: u32,
    /// The bytes allocated for the value.
    capacity: u32,
}

const HEADER_LEN: usize = size_of::<Header>();

/// One strong reference to a key+value allocation: immutable while shared, and
/// open to [`KeyValue::edit`] by the only holder.
pub struct KeyValue(NonNull<Header>);

// SAFETY: the bytes change only through `edit`, which takes `&mut self` and
// checks that this is the only reference, and the refcount is atomic, exactly
// like `Arc<[u8]>` and its `get_mut`.
unsafe impl Send for KeyValue {}
unsafe impl Sync for KeyValue {}

fn layout(key_len: usize, value_len: usize) -> Layout {
    let size = HEADER_LEN
        .checked_add(key_len)
        .and_then(|n| n.checked_add(value_len))
        .expect("entry size overflows usize");
    Layout::from_size_align(size, align_of::<Header>()).expect("entry size overflows isize")
}

impl KeyValue {
    /// Copies `key` and `value` into one new allocation.
    ///
    /// # Panics
    /// If either is 4 GiB or longer (the protocol caps them far lower).
    pub fn new(key: &[u8], value: &[u8]) -> Self {
        Self::with_capacity(key, value, value.len())
    }

    /// [`KeyValue::new`] with room for the value to grow to `capacity` bytes.
    pub fn with_capacity(key: &[u8], value: &[u8], capacity: usize) -> Self {
        let capacity = capacity.max(value.len());
        let key_len = u32::try_from(key.len()).expect("key too long");
        let value_len = u32::try_from(value.len()).expect("value too long");
        let capacity_len = u32::try_from(capacity).expect("value too long");
        let layout = layout(key.len(), capacity);
        // SAFETY: `layout` has non-zero size (the header). Every write below
        // stays within it: header at offset 0, then key, then value.
        unsafe {
            let Some(base) = NonNull::new(alloc::alloc(layout)) else {
                alloc::handle_alloc_error(layout);
            };
            let header = base.cast::<Header>();
            header.write(Header {
                refs: AtomicU32::new(1),
                key_len,
                value_len,
                capacity: capacity_len,
            });
            let data = base.as_ptr().add(HEADER_LEN);
            ptr::copy_nonoverlapping(key.as_ptr(), data, key.len());
            ptr::copy_nonoverlapping(value.as_ptr(), data.add(key.len()), value.len());
            KeyValue(header)
        }
    }

    /// Bytes allocated for an entry with these key and value lengths.
    pub fn size_for(key_len: usize, value_len: usize) -> usize {
        HEADER_LEN + key_len + value_len
    }

    /// Bytes allocated for this entry.
    pub fn allocation_size(&self) -> usize {
        let h = self.header();
        Self::size_for(h.key_len as usize, h.capacity as usize)
    }

    fn header(&self) -> &Header {
        // SAFETY: we hold a reference, so the allocation is live.
        unsafe { self.0.as_ref() }
    }

    fn data(&self) -> *const u8 {
        // SAFETY: offset is within the allocation (just past the header).
        unsafe { self.0.as_ptr().cast::<u8>().add(HEADER_LEN) }
    }

    pub fn key(&self) -> &[u8] {
        let len = self.header().key_len as usize;
        // SAFETY: the key occupies `len` initialized bytes right after the header.
        unsafe { slice::from_raw_parts(self.data(), len) }
    }

    pub fn value(&self) -> &[u8] {
        let h = self.header();
        // SAFETY: the value occupies `value_len` initialized bytes after the key.
        unsafe { slice::from_raw_parts(self.data().add(h.key_len as usize), h.value_len as usize) }
    }

    /// Bytes its value has room for.
    pub fn value_capacity(&self) -> usize {
        self.header().capacity as usize
    }

    /// Shortens the value [`edit`](Self::edit) lent, the only reference.
    pub fn shorten(&mut self, len: usize) {
        debug_assert!(len <= self.value().len());
        // SAFETY: `&mut self`, and only the length changes, to one that's in use.
        unsafe { self.0.as_mut().value_len = len as u32 };
    }

    /// The value to change where it lies, grown to `len` bytes (in zeroes), if
    /// this is the only reference and it has the room. Otherwise, for instance
    /// while a reader holds a reference to send the value, `None`.
    pub fn edit(&mut self, len: usize) -> Option<&mut [u8]> {
        let h = self.header();
        let (old, key_len) = (h.value_len as usize, h.key_len as usize);
        // Acquire, as in `Arc::get_mut`: readers that have let go are done.
        if h.refs.load(Ordering::Acquire) != 1 || len < old || len > h.capacity as usize {
            return None;
        }
        // SAFETY: this is the only reference, and `&mut self` keeps it so, so
        // nothing else reads the allocation. `len` is within what's allocated
        // for the value, and the bytes past `old` are zeroed before they're
        // part of the slice.
        unsafe {
            self.0.as_mut().value_len = len as u32;
            let value = self.0.as_ptr().cast::<u8>().add(HEADER_LEN + key_len);
            ptr::write_bytes(value.add(old), 0, len - old);
            Some(slice::from_raw_parts_mut(value, len))
        }
    }
}

impl Clone for KeyValue {
    fn clone(&self) -> Self {
        // Relaxed is enough to take a new reference, as in `Arc::clone`.
        let old = self.header().refs.fetch_add(1, Ordering::Relaxed);
        if old >= i32::MAX as u32 {
            // Refcount overflow would lead to use-after-free; mirror Arc.
            std::process::abort();
        }
        KeyValue(self.0)
    }
}

impl Drop for KeyValue {
    fn drop(&mut self) {
        let h = self.header();
        if h.refs.fetch_sub(1, Ordering::Release) != 1 {
            return;
        }
        // Synchronize with every other reference's Release decrement before
        // freeing, as in `Arc::drop`.
        fence(Ordering::Acquire);
        let layout = layout(h.key_len as usize, h.capacity as usize);
        // SAFETY: this was the last reference; the layout matches `with_capacity`.
        unsafe { alloc::dealloc(self.0.as_ptr().cast(), layout) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let kv = KeyValue::new(b"key", b"value");
        assert_eq!(kv.key(), b"key");
        assert_eq!(kv.value(), b"value");
        let empty = KeyValue::new(b"", b"");
        assert_eq!((empty.key(), empty.value()), (&b""[..], &b""[..]));
        assert_eq!(size_of::<KeyValue>(), 8);
        assert_eq!(size_of::<Option<KeyValue>>(), 8);
    }

    #[test]
    fn edits_only_alone_and_within_its_room() {
        let mut kv = KeyValue::with_capacity(b"key", b"abc", 8);
        assert_eq!(kv.allocation_size(), KeyValue::size_for(3, 8));
        kv.edit(3).unwrap()[0] = b'x';
        assert_eq!(kv.value(), b"xbc");
        assert_eq!(kv.edit(5).unwrap(), b"xbc\0\0");
        assert_eq!(kv.value(), b"xbc\0\0");
        assert_eq!(kv.key(), b"key");
        // Neither shrinks nor goes past its room.
        assert!(kv.edit(4).is_none());
        assert!(kv.edit(9).is_none());
        let reader = kv.clone();
        assert!(kv.edit(5).is_none());
        drop(reader);
        assert!(kv.edit(8).is_some());
        assert_eq!(kv.allocation_size(), KeyValue::size_for(3, 8));
    }

    #[test]
    fn clones_share_and_outlive_the_original() {
        let kv = KeyValue::new(b"k", &[9; 1000]);
        let clones: Vec<_> = (0..10).map(|_| kv.clone()).collect();
        drop(kv);
        for c in &clones {
            assert_eq!(c.value(), &[9; 1000]);
        }
    }

    #[test]
    fn cross_thread_drops() {
        let kv = KeyValue::new(b"shared", b"value");
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let kv = kv.clone();
                std::thread::spawn(move || {
                    for _ in 0..1000 {
                        let c = kv.clone();
                        assert_eq!(c.value(), b"value");
                    }
                })
            })
            .collect();
        drop(kv);
        for h in handles {
            h.join().unwrap();
        }
    }
}
