//! Memory helpers: a huge-page-friendly global allocator (Linux) and
//! block-arena buckets that never reallocate.

use std::alloc::{GlobalAlloc, Layout, System};

pub struct Alloc;

#[cfg(target_os = "linux")]
const BIG: usize = 4 << 20;
#[cfg(target_os = "linux")]
const HUGE: usize = 2 << 20;

unsafe impl GlobalAlloc for Alloc {
    #[inline]
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        #[cfg(target_os = "linux")]
        {
            if l.size() >= BIG && l.align() <= HUGE {
                // over-map by 2 MB so the block can start on a huge-page boundary
                let len = l.size() + HUGE;
                let p = libc::mmap(
                    std::ptr::null_mut(),
                    len,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
                    -1,
                    0,
                );
                if p == libc::MAP_FAILED {
                    return std::ptr::null_mut();
                }
                let a = ((p as usize) + HUGE - 1) & !(HUGE - 1);
                libc::madvise(a as *mut libc::c_void, l.size(), libc::MADV_HUGEPAGE);
                return a as *mut u8;
            }
        }
        System.alloc(l)
    }
    #[inline]
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        #[cfg(target_os = "linux")]
        {
            if l.size() >= BIG && l.align() <= HUGE {
                // unmap the aligned part; the untouched slack stays mapped (no RSS)
                libc::munmap(p as *mut libc::c_void, l.size());
                return;
            }
        }
        System.dealloc(p, l)
    }
}

const CHUNK_BYTES: usize = 8 << 20;

/// Per-bucket append-only storage in fixed blocks (power-of-two records),
/// carved sequentially from large chunks. Records are never moved.
pub struct Blocks<T: Copy> {
    chunks: Vec<Vec<T>>,
    chunk_ptr: *mut T,
    chunk_left: usize, // blocks left in current chunk
    shift: u32,
    blocks: Vec<Vec<*mut T>>,
    cur: Vec<*mut T>,
    n: Vec<u32>,
}

unsafe impl<T: Copy + Send> Send for Blocks<T> {}
unsafe impl<T: Copy + Sync> Sync for Blocks<T> {}

impl<T: Copy> Blocks<T> {
    /// `blk` is rounded up to a power of two.
    pub fn new(nb: usize, blk: usize) -> Self {
        Blocks {
            chunks: Vec::new(),
            chunk_ptr: std::ptr::null_mut(),
            chunk_left: 0,
            shift: blk.max(16).next_power_of_two().trailing_zeros(),
            blocks: (0..nb).map(|_| Vec::new()).collect(),
            cur: vec![std::ptr::null_mut(); nb],
            n: vec![0; nb],
        }
    }
    #[inline(always)]
    fn blk(&self) -> usize {
        1 << self.shift
    }
    #[cold]
    fn new_block(&mut self, b: usize) {
        let blk = self.blk();
        if self.chunk_left == 0 {
            let per = (CHUNK_BYTES / (blk * std::mem::size_of::<T>())).max(1);
            let mut v: Vec<T> = Vec::with_capacity(blk * per);
            self.chunk_ptr = v.as_mut_ptr();
            self.chunks.push(v);
            self.chunk_left = per;
        }
        let p = self.chunk_ptr;
        self.chunk_ptr = unsafe { p.add(blk) };
        self.chunk_left -= 1;
        self.blocks[b].push(p);
        self.cur[b] = p;
    }
    #[inline(always)]
    pub fn push(&mut self, b: usize, v: T) {
        let n = self.n[b] as usize;
        let off = n & (self.blk() - 1);
        if off == 0 {
            self.new_block(b);
        }
        unsafe { self.cur[b].add(off).write(v) };
        self.n[b] = (n + 1) as u32;
    }
    #[inline(always)]
    pub fn len(&self, b: usize) -> usize {
        self.n[b] as usize
    }
    #[inline(always)]
    pub fn get(&self, b: usize, i: usize) -> &T {
        unsafe { &*self.blocks[b].get_unchecked(i >> self.shift).add(i & (self.blk() - 1)) }
    }
    /// The bucket's records as contiguous slices, in push order.
    #[inline]
    pub fn slices(&self, b: usize) -> impl Iterator<Item = &[T]> + '_ {
        let n = self.n[b] as usize;
        let blk = self.blk();
        self.blocks[b].iter().enumerate().map(move |(k, &p)| {
            let len = (n - k * blk).min(blk);
            unsafe { std::slice::from_raw_parts(p as *const T, len) }
        })
    }
    #[inline]
    pub fn iter(&self, b: usize) -> impl Iterator<Item = &T> + '_ {
        self.slices(b).flatten()
    }
}
