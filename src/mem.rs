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

pub const BLK: usize = 1024;
const CHUNK_BLOCKS: usize = 128;

/// Per-bucket append-only storage in fixed blocks of BLK records, carved
/// sequentially from large chunks. Records are never moved.
pub struct Blocks<T: Copy> {
    chunks: Vec<Vec<T>>,
    chunk_ptr: *mut T,
    chunk_left: usize, // blocks left in current chunk
    blocks: Vec<Vec<*mut T>>,
    cur: Vec<*mut T>,
    n: Vec<u32>,
}

unsafe impl<T: Copy + Send> Send for Blocks<T> {}
unsafe impl<T: Copy + Sync> Sync for Blocks<T> {}

impl<T: Copy> Blocks<T> {
    pub fn new(nb: usize) -> Self {
        Blocks {
            chunks: Vec::new(),
            chunk_ptr: std::ptr::null_mut(),
            chunk_left: 0,
            blocks: (0..nb).map(|_| Vec::new()).collect(),
            cur: vec![std::ptr::null_mut(); nb],
            n: vec![0; nb],
        }
    }
    #[cold]
    fn new_block(&mut self, b: usize) {
        if self.chunk_left == 0 {
            let mut v: Vec<T> = Vec::with_capacity(BLK * CHUNK_BLOCKS);
            self.chunk_ptr = v.as_mut_ptr();
            self.chunks.push(v);
            self.chunk_left = CHUNK_BLOCKS;
        }
        let p = self.chunk_ptr;
        self.chunk_ptr = unsafe { p.add(BLK) };
        self.chunk_left -= 1;
        self.blocks[b].push(p);
        self.cur[b] = p;
    }
    #[inline(always)]
    pub fn push(&mut self, b: usize, v: T) {
        let n = self.n[b] as usize;
        let off = n % BLK;
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
        unsafe { &*self.blocks[b][i / BLK].add(i % BLK) }
    }
    #[inline]
    pub fn iter(&self, b: usize) -> impl Iterator<Item = &T> + '_ {
        let n = self.n[b] as usize;
        self.blocks[b].iter().enumerate().flat_map(move |(k, &p)| {
            let len = (n - k * BLK).min(BLK);
            unsafe { std::slice::from_raw_parts(p as *const T, len) }.iter()
        })
    }
}
