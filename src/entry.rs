//! A thin, refcounted key+value allocation.
//!
//! `KeyValue` is a single pointer to `[Header][key bytes][value bytes]`.
//! Compared with `Arc<[u8]>` (16-byte fat pointer, 16-byte header) this keeps
//! table slots small and the per-entry header at 12 bytes, which matters when
//! you have millions of small entries.

use std::alloc::{self, Layout};
use std::ptr::{self, NonNull};
use std::slice;
use std::sync::atomic::{AtomicU32, Ordering, fence};

#[repr(C)]
struct Header {
    refs: AtomicU32,
    key_len: u32,
    value_len: u32,
}

const HEADER_LEN: usize = size_of::<Header>();

/// One strong reference to an immutable key+value allocation.
pub struct KeyValue(NonNull<Header>);

// SAFETY: the bytes are immutable after construction and the refcount is
// atomic, exactly like `Arc<[u8]>`.
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
        let key_len = u32::try_from(key.len()).expect("key too long");
        let value_len = u32::try_from(value.len()).expect("value too long");
        let layout = layout(key.len(), value.len());
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
        Self::size_for(h.key_len as usize, h.value_len as usize)
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
        let layout = layout(h.key_len as usize, h.value_len as usize);
        // SAFETY: this was the last reference; the layout matches `new`.
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
