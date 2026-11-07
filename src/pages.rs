//! Memory straight from the OS, for the hash tables and segments.
//!
//! **Tables** (`PageAlloc`) grow by rebuilding: the bigger table is
//! allocated, slots move over and the old one is freed. Through the global
//! allocator, those freed tables ended up stuck inside partly used pages and
//! never went back to the OS: about 10 bytes per key at 1M keys. So tables are
//! mapped straight from the OS, and freeing one hands its memory back at once.
//! Only tables that waste at most an eighth of their size rounding up to whole
//! pages are mapped (with 4 KB pages, anything from about 4 KB; with 16 KB
//! pages, as on Apple Silicon, from about 128 KB). The rest use the global
//! allocator.
//!
//! **Segments** (`SegBuf`) are constantly allocated and freed across threads
//! under eviction churn. Through mimalloc that kept RSS ~9% above what was in
//! use, and mapping each one separately could exceed Linux's limit of ~65k
//! mappings in a large store. So they come from a pool: 8 MB mappings carved
//! into power-of-two blocks (4–256 KB). A block only becomes resident page by
//! page as it's written. Freed blocks are kept resident for quick reuse up to
//! `HOT_LIMIT` in total; beyond that, a freed block drops its pages
//! (`madvise(MADV_DONTNEED)`), so it costs address space but no RAM. Dropping
//! every freed block's pages made the next segment fault them back in (~10k
//! faults/s under steady churn), which cost ~8% of pipelined throughput. So
//! blocks of sizes still being taken push out those of sizes that aren't: a
//! shard's segments get bigger as it fills, and the small ones it frees later
//! would otherwise fill the limit for good.

use std::alloc::Layout;
use std::ops::{Deref, DerefMut};
use std::ptr::{self, NonNull};
use std::sync::Mutex;

use allocator_api2::alloc::{AllocError, Allocator, Global};

#[derive(Clone, Copy, Debug, Default)]
pub struct PageAlloc;

/// The OS page size (4 KB assumed where it can't be asked).
#[cfg(not(unix))]
pub fn page_size() -> usize {
    4096
}

#[cfg(unix)]
pub fn page_size() -> usize {
    use std::sync::OnceLock;
    static PAGE_SIZE: OnceLock<usize> = OnceLock::new();
    // SAFETY: sysconf has no preconditions.
    *PAGE_SIZE.get_or_init(|| match unsafe { libc::sysconf(libc::_SC_PAGESIZE) } {
        n if n > 0 => n as usize,
        _ => 4096,
    })
}

/// Whether an allocation of this layout is mapped from the OS. The answer
/// depends only on the layout, so `deallocate` always agrees with `allocate`.
fn mapped(layout: &Layout) -> bool {
    #[cfg(unix)]
    {
        let size = layout.size();
        let rounding = size.next_multiple_of(page_size()) - size;
        size >= page_size() && layout.align() <= page_size() && rounding * 8 <= size
    }
    #[cfg(not(unix))]
    {
        let _ = layout;
        false
    }
}

/// Bytes actually reserved for an allocation of `size` bytes (alignment 16 or
/// less): mapped ones round up to whole pages.
pub fn reserved(size: usize) -> usize {
    let layout = Layout::from_size_align(size, 16).expect("valid size");
    #[cfg(unix)]
    if mapped(&layout) {
        return size.next_multiple_of(page_size());
    }
    let _ = layout;
    size
}

// SAFETY: mapped blocks are page aligned (at least `layout.align()`), at least
// `layout.size()` long, and stay valid until `deallocate` unmaps them; the
// rest is delegated to the global allocator.
unsafe impl Allocator for PageAlloc {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        if !mapped(&layout) {
            return Global.allocate(layout);
        }
        #[cfg(unix)]
        {
            let len = reserved(layout.size());
            // SAFETY: a fresh anonymous private mapping; no existing memory
            // is touched.
            let ptr = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    len,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_PRIVATE | libc::MAP_ANON,
                    -1,
                    0,
                )
            };
            if ptr == libc::MAP_FAILED {
                return Err(AllocError);
            }
            let ptr = NonNull::new(ptr.cast::<u8>()).ok_or(AllocError)?;
            Ok(NonNull::slice_from_raw_parts(ptr, len))
        }
        #[cfg(not(unix))]
        unreachable!("nothing is mapped on this platform")
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        if !mapped(&layout) {
            // SAFETY: the caller guarantees `ptr` came from `allocate` with
            // this layout, which delegated to `Global`.
            return unsafe { Global.deallocate(ptr, layout) };
        }
        #[cfg(unix)]
        // SAFETY: `ptr` came from `allocate` with this layout, so it's the
        // start of a mapping of `reserved(layout.size())` bytes.
        unsafe {
            libc::munmap(ptr.as_ptr().cast(), reserved(layout.size()));
        }
    }
}

/// Smallest and largest pooled block sizes; pooled sizes are powers of two.
pub const POOL_MIN: usize = 4 * 1024;
pub const POOL_MAX: usize = 256 * 1024;
const CLASSES: usize = (POOL_MAX / POOL_MIN).trailing_zeros() as usize + 1;
/// Blocks are carved from mappings this big.
#[cfg(unix)]
const CHUNK: usize = 8 << 20;

/// Freed blocks kept resident for reuse, at most this many bytes in total.
/// RSS can exceed accounted memory by up to this much.
const HOT_LIMIT: usize = 4 << 20;

/// Free blocks per size class, as addresses.
struct Pool {
    /// Recently freed, pages still resident: handed out first.
    hot: [Vec<usize>; CLASSES],
    hot_bytes: usize,
    /// Which take last wanted each size class, and takes so far.
    last_take: [u64; CLASSES],
    takes: u64,
    /// No resident pages: dropped, or never touched.
    cold: [Vec<usize>; CLASSES],
}

static POOL: Mutex<Pool> = Mutex::new(Pool {
    hot: [const { Vec::new() }; CLASSES],
    hot_bytes: 0,
    last_take: [0; CLASSES],
    takes: 0,
    cold: [const { Vec::new() }; CLASSES],
});

impl Pool {
    /// Notes that a block of this class is wanted.
    fn wanted(&mut self, class: usize) {
        self.takes += 1;
        self.last_take[class] = self.takes;
    }

    /// Keeps a freed block hot if there's room. If not, returns the block to
    /// drop the pages of (class and address) and go cold: one of a class wanted
    /// less lately than this one's, and then this is asked again, or else this
    /// one.
    fn keep(&mut self, class: usize, addr: usize) -> Option<(usize, usize)> {
        let size = POOL_MIN << class;
        if self.hot_bytes + size <= HOT_LIMIT {
            self.hot[class].push(addr);
            self.hot_bytes += size;
            return None;
        }
        let stale = (0..CLASSES)
            .filter(|&c| self.last_take[c] < self.last_take[class] && !self.hot[c].is_empty())
            .min_by_key(|&c| self.last_take[c]);
        let Some(stale) = stale else {
            return Some((class, addr));
        };
        self.hot_bytes -= POOL_MIN << stale;
        self.hot[stale].pop().map(|block| (stale, block))
    }
}

fn pool() -> std::sync::MutexGuard<'static, Pool> {
    // The pool's state stays consistent even if a holder panicked.
    POOL.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// `TANTALUM_SEGMENT_POOL=off` (use the global allocator for segments) or
/// `=keep-pages` (pool, but don't drop freed blocks' pages): a diagnostic for
/// measuring what the pool costs.
#[derive(Clone, Copy, PartialEq, Eq)]
enum PoolMode {
    On,
    Off,
    KeepPages,
}

fn pool_mode() -> PoolMode {
    use std::sync::OnceLock;
    static MODE: OnceLock<PoolMode> = OnceLock::new();
    *MODE.get_or_init(|| match std::env::var("TANTALUM_SEGMENT_POOL").as_deref() {
        Ok("off") => PoolMode::Off,
        Ok("keep-pages") => PoolMode::KeepPages,
        _ => PoolMode::On,
    })
}

fn class_of(size: usize) -> Option<usize> {
    let pooled = cfg!(unix)
        && pool_mode() != PoolMode::Off
        && size.is_power_of_two()
        && (POOL_MIN..=POOL_MAX).contains(&size);
    pooled.then(|| (size / POOL_MIN).trailing_zeros() as usize)
}

#[cfg(unix)]
fn take_block(class: usize) -> Option<NonNull<u8>> {
    let size = POOL_MIN << class;
    {
        let mut pool = pool();
        pool.wanted(class);
        if let Some(addr) = pool.hot[class].pop() {
            pool.hot_bytes -= size;
            return NonNull::new(addr as *mut u8);
        }
        if let Some(addr) = pool.cold[class].pop() {
            return NonNull::new(addr as *mut u8);
        }
    }
    // SAFETY: a fresh anonymous private mapping; no existing memory is touched.
    let base = unsafe {
        libc::mmap(
            ptr::null_mut(),
            CHUNK,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANON,
            -1,
            0,
        )
    };
    if base == libc::MAP_FAILED {
        return None;
    }
    let base = base as usize;
    pool().cold[class].extend((1..CHUNK / size).map(|i| base + i * size));
    NonNull::new(base as *mut u8)
}

#[cfg(unix)]
fn give_block(class: usize, block: NonNull<u8>) {
    let size = POOL_MIN << class;
    let addr = block.as_ptr() as usize;
    if pool_mode() == PoolMode::KeepPages {
        let mut pool = pool();
        pool.hot[class].push(addr);
        pool.hot_bytes += size;
        return;
    }
    loop {
        let Some((going, going_addr)) = pool().keep(class, addr) else {
            return;
        };
        let going_size = POOL_MIN << going;
        // Blocks smaller than a page share it with neighbors, so only whole-page
        // blocks can have their memory dropped.
        if going_size >= page_size() {
            // SAFETY: the block is ours and nothing refers to it any more, so
            // its contents may be discarded.
            unsafe {
                libc::madvise(
                    going_addr as *mut libc::c_void,
                    going_size,
                    libc::MADV_DONTNEED,
                );
            }
        }
        pool().cold[going].push(going_addr);
        if going_addr == addr {
            return;
        }
    }
}

/// A fixed-capacity byte buffer for a segment: a pooled block when the
/// capacity is a pooled size, otherwise a global allocation.
pub struct SegBuf {
    ptr: NonNull<u8>,
    len: usize,
    cap: usize,
    pooled: bool,
}

// SAFETY: a SegBuf owns its block exclusively, like a Vec<u8>.
unsafe impl Send for SegBuf {}
unsafe impl Sync for SegBuf {}

impl SegBuf {
    pub fn with_capacity(cap: usize) -> Self {
        #[cfg(unix)]
        if let Some(class) = class_of(cap)
            && let Some(ptr) = take_block(class)
        {
            return SegBuf {
                ptr,
                len: 0,
                cap,
                pooled: true,
            };
        }
        let layout = Self::layout(cap);
        // SAFETY: the layout has a non-zero size.
        let ptr = unsafe { std::alloc::alloc(layout) };
        let ptr = NonNull::new(ptr).unwrap_or_else(|| std::alloc::handle_alloc_error(layout));
        SegBuf {
            ptr,
            len: 0,
            cap,
            pooled: false,
        }
    }

    fn layout(cap: usize) -> Layout {
        Layout::from_size_align(cap.max(1), 1).expect("segment size fits a layout")
    }

    pub fn capacity(&self) -> usize {
        self.cap
    }

    pub fn room(&self) -> usize {
        self.cap - self.len
    }

    pub fn extend_from_slice(&mut self, bytes: &[u8]) {
        assert!(bytes.len() <= self.room(), "segment overflow");
        // SAFETY: the destination is within the block per the assert, and
        // `bytes` can't alias it while we hold `&mut self`.
        unsafe {
            ptr::copy_nonoverlapping(bytes.as_ptr(), self.ptr.as_ptr().add(self.len), bytes.len());
        }
        self.len += bytes.len();
    }

    pub fn push(&mut self, byte: u8) {
        self.extend_from_slice(&[byte]);
    }
}

impl Deref for SegBuf {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        // SAFETY: the first `len` bytes were written by `extend_from_slice`.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl DerefMut for SegBuf {
    fn deref_mut(&mut self) -> &mut [u8] {
        // SAFETY: as in `deref`, and we hold `&mut self`.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl Drop for SegBuf {
    fn drop(&mut self) {
        if self.pooled {
            #[cfg(unix)]
            give_block(class_of(self.cap).expect("pooled size"), self.ptr);
        } else {
            // SAFETY: allocated in `with_capacity` with this layout.
            unsafe { std::alloc::dealloc(self.ptr.as_ptr(), Self::layout(self.cap)) }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A shard's segments get bigger as it fills. The small ones it frees later
    /// mustn't keep out the blocks being used now, whose pages would then be
    /// dropped and faulted back in each time.
    #[test]
    fn sizes_in_use_push_out_sizes_that_arent() {
        let (small, big) = (0, 4);
        let mut pool = Pool {
            hot: [const { Vec::new() }; CLASSES],
            hot_bytes: 0,
            last_take: [0; CLASSES],
            takes: 0,
            cold: [const { Vec::new() }; CLASSES],
        };
        pool.wanted(small);
        let mut addr = 0;
        for _ in 0..HOT_LIMIT / (POOL_MIN << small) {
            addr += 1;
            assert_eq!(pool.keep(small, addr), None);
        }
        assert_eq!(pool.hot_bytes, HOT_LIMIT);
        // Full. Nothing's wanted more lately than these, so another goes.
        addr += 1;
        assert_eq!(pool.keep(small, addr), Some((small, addr)));
        // Now a bigger size is in use: what's in its way goes first.
        pool.wanted(big);
        let freed = addr + 1;
        let room = (POOL_MIN << big) / (POOL_MIN << small);
        for _ in 0..room {
            let (class, _) = pool.keep(big, freed).expect("full");
            assert_eq!(class, small);
        }
        assert_eq!(pool.keep(big, freed), None);
        assert_eq!(pool.hot[big], [freed]);
        assert_eq!(pool.hot_bytes, HOT_LIMIT);
        // Filled with what's in use, and that stays.
        pool.hot_bytes = HOT_LIMIT;
        pool.hot[small].clear();
        assert_eq!(pool.keep(big, freed + 1), Some((big, freed + 1)));
    }

    #[test]
    fn segment_buffers_roundtrip_and_reuse_blocks() {
        for cap in [1000, POOL_MIN, 64 * 1024, POOL_MAX, POOL_MAX * 2] {
            for round in 0..3u8 {
                let mut buf = SegBuf::with_capacity(cap);
                assert_eq!((buf.len(), buf.capacity()), (0, cap));
                let data: Vec<u8> = (0..cap).map(|i| (i as u8) ^ round).collect();
                buf.extend_from_slice(&data[..cap / 2]);
                buf.push(7);
                buf.extend_from_slice(&data[cap / 2 + 1..]);
                assert_eq!(buf.room(), 0);
                buf[0] = 42;
                assert_eq!(buf[0], 42);
                assert_eq!(&buf[1..cap / 2], &data[1..cap / 2]);
                assert_eq!(buf[cap / 2], 7);
            }
        }
    }

    #[test]
    #[should_panic(expected = "segment overflow")]
    fn segment_buffers_never_overflow() {
        let mut buf = SegBuf::with_capacity(POOL_MIN);
        buf.extend_from_slice(&[0; POOL_MIN + 1]);
    }

    #[test]
    fn small_and_large_blocks_roundtrip() {
        for size in [8, 100, 4095, 4096, 4097, 100_000, 1 << 20] {
            let layout = Layout::from_size_align(size, 16).unwrap();
            let block = PageAlloc.allocate(layout).unwrap();
            assert!(block.len() >= size);
            assert_eq!(block.cast::<u8>().as_ptr() as usize % 16, 0);
            // SAFETY: the block is at least `size` bytes and we own it.
            unsafe {
                let bytes = std::slice::from_raw_parts_mut(block.cast::<u8>().as_ptr(), size);
                bytes.fill(0xAB);
                assert!(bytes.iter().all(|&b| b == 0xAB));
                PageAlloc.deallocate(block.cast(), layout);
            }
        }
    }

    #[test]
    fn only_maps_when_rounding_is_cheap() {
        assert_eq!(reserved(100), 100);
        #[cfg(unix)]
        {
            let page = page_size();
            assert_eq!(reserved(page), page, "exactly a page");
            assert_eq!(reserved(page + 1), page + 1, "would waste almost half");
            assert_eq!(reserved(8 * page + 1), 9 * page, "wastes under an eighth");
        }
    }
}
