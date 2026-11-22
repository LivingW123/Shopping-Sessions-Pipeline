//! Shopping Sessions pipeline. See rules.md.
//!
//! Plan:
//!  1. mmap every input CSV, cut files into newline-aligned chunks.
//!  2. Links: parse in parallel, union-find on 64-bit ID hashes, name each
//!     person by its byte-wise smallest ID. Build hash(ID) -> person table.
//!  3. Parse events and clicks in parallel. Events go to buckets by
//!     hash(event_id) (for dedup), clicks to buckets by person.
//!  4. Per event bucket: keep the copy with the smallest ingest_ts; if it is
//!     valid, send it to a person bucket.
//!  5. Per person bucket: sort, sessionize, credit and assist, aggregate.
//!  6. Write Parquet.

mod mem;
mod parquet;
mod util;

#[global_allocator]
static GLOBAL: mem::Alloc = mem::Alloc;
use mem::Blocks;

use memmap2::Mmap;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use util::*;

const LINKED: u64 = 1 << 63;
const EB_BITS: u32 = 8; // event-id buckets
const PB_BITS: u32 = 8; // person buckets
const NEB: usize = 1 << EB_BITS;
const NPB: usize = 1 << PB_BITS;
const SEED_U: u64 = 0x1234_5678_9abc_def1;
const SEED_P: u64 = 0x5eed_0f_9a_11_ce_d0;
const SEED_E1: u64 = 0x0f1e_2d3c_4b5a_6978;

// ---------------------------------------------------------------- files

struct Files {
    maps: Vec<Option<Mmap>>,
}

impl Files {
    #[inline(always)]
    fn data(&self, f: usize) -> &[u8] {
        match &self.maps[f] {
            Some(m) => &m[..],
            None => &[],
        }
    }
    #[inline(always)]
    fn get(&self, loc: u64) -> &[u8] {
        let f = (loc >> 52) as usize;
        let off = ((loc >> 12) & ((1 << 40) - 1)) as usize;
        let len = (loc & 0xfff) as usize;
        &self.data(f)[off..off + len]
    }
}

#[inline(always)]
fn pack_loc(file: usize, base: &[u8], s: &[u8]) -> u64 {
    let off = s.as_ptr() as usize - base.as_ptr() as usize;
    ((file as u64) << 52) | ((off as u64) << 12) | (s.len().min(0xfff) as u64)
}

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.is_file() && p.extension().map(|x| x == "csv").unwrap_or(false) {
                v.push(p);
            }
        }
    }
    v.sort();
    v
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Event,
    Click,
    Link,
}

struct Chunk {
    file: usize,
    kind: Kind,
    start: usize,
    end: usize,
}

fn make_chunks(files: &Files, file_kinds: &[Kind], target: usize) -> Vec<Chunk> {
    let mut out = Vec::new();
    for (fi, &kind) in file_kinds.iter().enumerate() {
        let d = files.data(fi);
        if d.is_empty() {
            continue;
        }
        // skip header line
        let first = match memchr::memchr(b'\n', d) {
            Some(p) => p + 1,
            None => continue,
        };
        let mut s = first;
        while s < d.len() {
            let mut e = (s + target).min(d.len());
            if e < d.len() {
                e = match memchr::memchr(b'\n', &d[e..]) {
                    Some(p) => e + p + 1,
                    None => d.len(),
                };
            }
            out.push(Chunk { file: fi, kind, start: s, end: e });
            s = e;
        }
    }
    // biggest first is not needed: chunks are uniform
    out
}

const SEG: usize = 1 << 16;

/// Scratch for one segment: separator offsets (`,` and `\n`) and a bitmask of `"`.
pub struct Scan {
    sep: Vec<u32>,
    qbits: Vec<u64>,
}

impl Scan {
    fn new() -> Self {
        Scan { sep: Vec::with_capacity(SEG / 4), qbits: Vec::with_capacity(SEG / 64 + 2) }
    }
}

fn scan_segment(s: &[u8], sc: &mut Scan) {
    sc.sep.clear();
    sc.qbits.clear();
    sc.sep.reserve(s.len() + 64);
    let mut i = 0usize;
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") {
            i = unsafe { scan_avx2(s, sc) };
        }
    }
    // scalar tail, in 64-byte words
    while i < s.len() {
        let e = (i + 64).min(s.len());
        let mut q = 0u64;
        for j in i..e {
            let c = s[j];
            if c == b',' || c == b'\n' {
                sc.sep.push(j as u32);
            } else if c == b'"' {
                q |= 1 << (j - i);
            }
        }
        sc.qbits.push(q);
        i = e;
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,bmi1,popcnt")]
unsafe fn scan_avx2(s: &[u8], sc: &mut Scan) -> usize {
    use std::arch::x86_64::*;
    let comma = _mm256_set1_epi8(b',' as i8);
    let nl = _mm256_set1_epi8(b'\n' as i8);
    let qq = _mm256_set1_epi8(b'"' as i8);
    let p = s.as_ptr();
    let n = s.len();
    let mut i = 0usize;
    let mut len = sc.sep.len();
    let dst = sc.sep.as_mut_ptr();
    while i + 64 <= n {
        let v0 = _mm256_loadu_si256(p.add(i) as *const __m256i);
        let v1 = _mm256_loadu_si256(p.add(i + 32) as *const __m256i);
        let s0 = _mm256_or_si256(_mm256_cmpeq_epi8(v0, comma), _mm256_cmpeq_epi8(v0, nl));
        let s1 = _mm256_or_si256(_mm256_cmpeq_epi8(v1, comma), _mm256_cmpeq_epi8(v1, nl));
        let q0 = _mm256_cmpeq_epi8(v0, qq);
        let q1 = _mm256_cmpeq_epi8(v1, qq);
        let mut bits = (_mm256_movemask_epi8(s0) as u32 as u64) | ((_mm256_movemask_epi8(s1) as u32 as u64) << 32);
        let qb = (_mm256_movemask_epi8(q0) as u32 as u64) | ((_mm256_movemask_epi8(q1) as u32 as u64) << 32);
        sc.qbits.push(qb);
        while bits != 0 {
            let t = bits.trailing_zeros();
            *dst.add(len) = (i as u32) + t;
            len += 1;
            bits &= bits - 1;
        }
        i += 64;
    }
    sc.sep.set_len(len);
    i
}

#[inline(always)]
fn popcnt_range(q: &[u64], a: usize, b: usize) -> u32 {
    if a >= b {
        return 0;
    }
    let wa = a >> 6;
    let wb = (b - 1) >> 6;
    let mut cnt = 0;
    let mut w = wa;
    while w <= wb {
        let mut m = q[w];
        if w == wa {
            m &= !0u64 << (a & 63);
        }
        if w == wb {
            let e = b - (w << 6);
            if e < 64 {
                m &= (1u64 << e) - 1;
            }
        }
        cnt += m.count_ones();
        w += 1;
    }
    cnt
}

/// Calls `f` with the N unquoted fields of every well-formed line that starts
/// in [start, end). Malformed lines (rules.md CSV dialect) are skipped.
/// A line is well-formed iff it has exactly N fields and every `"` in it is
/// the first or last byte of a field that both starts and ends with `"`.
#[inline(always)]
fn for_records<'a, const N: usize>(
    d: &'a [u8],
    start: usize,
    end: usize,
    sc: &mut Scan,
    mut f: impl FnMut(&[&'a [u8]; N]),
) {
    let mut s = start;
    while s < end {
        let mut e = (s + SEG).min(end);
        if e < end {
            e = match memchr::memchr(b'\n', &d[e..end]) {
                Some(p) => e + p + 1,
                None => end,
            };
        }
        let seg: &'a [u8] = &d[s..e];
        scan_segment(seg, sc);
        let slen = seg.len();
        if slen > 0 && seg[slen - 1] != b'\n' {
            sc.sep.push(slen as u32); // virtual line end
        }
        let sep = &sc.sep[..];
        let qbits = &sc.qbits[..];
        let mut fields: [&'a [u8]; N] = [&seg[..0]; N];
        let mut idx = 0usize;
        let mut ls = 0usize;
        while idx < sep.len() {
            let mut k = 0usize;
            let mut fs = ls;
            let mut qsum = 0u32;
            let mut p;
            loop {
                p = sep[idx] as usize;
                idx += 1;
                if k < N {
                    let (mut a, mut b) = (fs, p);
                    if b >= a + 2 && seg[a] == b'"' && seg[b - 1] == b'"' {
                        a += 1;
                        b -= 1;
                        qsum += 2;
                    }
                    fields[k] = &seg[a..b];
                }
                k += 1;
                fs = p + 1;
                if p >= slen || seg[p] == b'\n' || idx >= sep.len() {
                    break;
                }
            }
            if k == N && popcnt_range(qbits, ls, p.min(slen)) == qsum {
                f(&fields);
            }
            ls = p + 1;
        }
        s = e;
    }
}

// ---------------------------------------------------------------- parallel

fn run_par<S: Send, I: Fn() -> S + Sync, W: Fn(&mut S, usize) + Sync>(
    threads: usize,
    n: usize,
    init: I,
    work: W,
) -> Vec<S> {
    let ctr = AtomicUsize::new(0);
    std::thread::scope(|sc| {
        let hs: Vec<_> = (0..threads)
            .map(|_| {
                sc.spawn(|| {
                    let mut st = init();
                    loop {
                        let i = ctr.fetch_add(1, Ordering::Relaxed);
                        if i >= n {
                            break;
                        }
                        work(&mut st, i);
                    }
                    st
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

// ---------------------------------------------------------------- hash table u64 -> u32

#[derive(Clone, Copy)]
#[repr(C, align(16))]
struct Slot {
    k: u64,
    v: u32,
}

struct U64Map {
    slots: Vec<Slot>,
    mask: usize,
}

impl U64Map {
    fn with_capacity(n: usize) -> Self {
        let cap = (n * 2).next_power_of_two().max(16);
        U64Map { slots: vec![Slot { k: 0, v: u32::MAX }; cap], mask: cap - 1 }
    }
    #[inline(always)]
    fn prefetch(&self, k: u64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::*;
            _mm_prefetch(self.slots.as_ptr().add((k as usize) & self.mask) as *const i8, _MM_HINT_T0);
        }
    }
    #[inline(always)]
    fn get(&self, k: u64) -> Option<u32> {
        let mut i = (k as usize) & self.mask;
        loop {
            let s = self.slots[i];
            if s.v == u32::MAX {
                return None;
            }
            if s.k == k {
                return Some(s.v);
            }
            i = (i + 1) & self.mask;
        }
    }
    /// returns (value, inserted)
    #[inline]
    fn get_or_insert(&mut self, k: u64, v: u32) -> (u32, bool) {
        let mut i = (k as usize) & self.mask;
        loop {
            let s = self.slots[i];
            if s.v == u32::MAX {
                self.slots[i] = Slot { k, v };
                return (v, true);
            }
            if s.k == k {
                return (s.v, false);
            }
            i = (i + 1) & self.mask;
        }
    }
}

/// Small growable map u64 -> dense id (insertion order).
struct LMap {
    slots: Vec<Slot>,
    mask: usize,
    n: usize,
}

impl LMap {
    fn new() -> Self {
        LMap { slots: Vec::new(), mask: 0, n: 0 }
    }
    fn reset(&mut self, hint: usize) {
        let cap = (hint * 2).next_power_of_two().max(64);
        self.slots.clear();
        self.slots.resize(cap, Slot { k: 0, v: u32::MAX });
        self.mask = cap - 1;
        self.n = 0;
    }
    #[inline(always)]
    fn idx(&self, k: u64) -> usize {
        (k.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 20) as usize & self.mask
    }
    #[inline(always)]
    fn get(&self, k: u64) -> u32 {
        let mut i = self.idx(k);
        loop {
            let s = self.slots[i];
            if s.v == u32::MAX || s.k == k {
                return s.v;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(always)]
    fn get_or_insert(&mut self, k: u64) -> (u32, bool) {
        if self.n * 2 >= self.slots.len() {
            self.grow();
        }
        let mut i = self.idx(k);
        loop {
            let s = self.slots[i];
            if s.v == u32::MAX {
                self.slots[i] = Slot { k, v: self.n as u32 };
                self.n += 1;
                return (self.n as u32 - 1, true);
            }
            if s.k == k {
                return (s.v, false);
            }
            i = (i + 1) & self.mask;
        }
    }
    fn grow(&mut self) {
        let old = std::mem::take(&mut self.slots);
        let cap = (old.len() * 2).max(64);
        self.slots = vec![Slot { k: 0, v: u32::MAX }; cap];
        self.mask = cap - 1;
        for s in old {
            if s.v != u32::MAX {
                let mut i = self.idx(s.k);
                while self.slots[i].v != u32::MAX {
                    i = (i + 1) & self.mask;
                }
                self.slots[i] = s;
            }
        }
    }
}

// ---------------------------------------------------------------- records

#[derive(Clone, Copy)]
struct EvRec {
    h1: u64,
    eloc: u64,
    ukey: u64, // user hash until resolved to a person key
    uloc: u64,
    ts: u32,     // ms since window_start
    ingest: u32, // epoch seconds
    price: u32,
    cat: u16,
    etype: u8, // 0 = invalid, 1 view, 2 cart, 3 purchase
}

#[derive(Clone, Copy)]
struct SRec {
    ukey: u64,
    uloc: u64,
    pk: u64, // ts_rel << 33 | etype << 31 | price << 7 | cat
}

#[inline(always)]
fn pack_ev(ts_rel: u32, etype: u8, price: u32, cat: u16) -> u64 {
    ((ts_rel as u64) << 33) | ((etype as u64) << 31) | ((price as u64) << 7) | cat as u64
}

#[derive(Clone, Copy)]
struct CRec {
    ukey: u64,
    ts: i64,
    cloc: u64,
    camp: u32,
}

#[inline(always)]
fn pbucket(ukey: u64) -> usize {
    (ukey.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> (64 - PB_BITS)) as usize
}

// ---------------------------------------------------------------- small CSV tables

fn read_small_csv<const N: usize>(path: &Path) -> Vec<[Vec<u8>; N]> {
    let data = std::fs::read(path).unwrap_or_default();
    let mut rows = Vec::new();
    let mut first = true;
    for line in data.split(|&c| c == b'\n') {
        if first {
            first = false;
            continue;
        }
        let mut f = [&b""[..]; N];
        if split_fields::<N>(line, &mut f) {
            rows.push(std::array::from_fn(|i| f[i].to_vec()));
        }
    }
    rows
}

fn manifest_ts(text: &str, key: &str) -> i64 {
    let p = text.find(&format!("\"{}\"", key)).expect("manifest key");
    let rest = &text[p + key.len() + 2..];
    let c = rest.find(':').unwrap();
    let rest = &rest[c + 1..];
    let q1 = rest.find('"').unwrap();
    let rest = &rest[q1 + 1..];
    let q2 = rest.find('"').unwrap();
    parse_ts(rest[..q2].as_bytes()).expect("manifest ts")
}

// ---------------------------------------------------------------- main

struct UserRow {
    name: u64, // loc
    day: i32,
    sessions: u32,
    events: u32,
    revenue: u64,
    top: i32, // category index or -1
}

#[derive(Clone, Copy, Default)]
struct CStat {
    sessions: u64,
    purch: u64,
    revenue: u64,
    assisted: u64,
}

fn main() {
    let t0 = std::time::Instant::now();
    let args: Vec<String> = std::env::args().collect();
    let data_dir = PathBuf::from(
        std::env::var("DATA_DIR").ok().or_else(|| args.get(1).cloned()).unwrap_or("/data".into()),
    );
    let out_dir = PathBuf::from(
        std::env::var("OUTPUT_DIR").ok().or_else(|| args.get(2).cloned()).unwrap_or("/output".into()),
    );
    let threads: usize = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8))
        .max(1);
    let verbose = std::env::var("PIPE_VERBOSE").is_ok();
    let lap = |what: &str| {
        if verbose {
            eprintln!("{:>8.3}s {}", t0.elapsed().as_secs_f64(), what);
        }
    };

    // manifest
    let man = std::fs::read_to_string(data_dir.join("manifest.json")).expect("manifest");
    let ws = manifest_ts(&man, "window_start");
    let we = manifest_ts(&man, "window_end");
    let cws = ws - WEEK_MS;
    assert!(we - ws < (1i64 << 31), "window longer than 24 days");
    let ws_day = ws.div_euclid(DAY_MS);
    let ndays = ((we - 1).div_euclid(DAY_MS) - ws_day + 1).max(1) as usize;

    // products and categories
    let prod_rows = read_small_csv::<2>(&data_dir.join("products.csv"));
    let mut cat_names: Vec<Vec<u8>> = Vec::new();
    let mut cat_idx: HashMap<Vec<u8>, u16> = HashMap::new();
    let mut products: HashMap<Vec<u8>, u16> = HashMap::with_capacity(prod_rows.len() * 2);
    for [p, c] in &prod_rows {
        let ci = *cat_idx.entry(c.clone()).or_insert_with(|| {
            cat_names.push(c.clone());
            (cat_names.len() - 1) as u16
        });
        products.insert(p.clone(), ci);
    }
    let unknown_cat = {
        let u = b"unknown".to_vec();
        *cat_idx.entry(u.clone()).or_insert_with(|| {
            cat_names.push(u);
            (cat_names.len() - 1) as u16
        })
    };
    let ncat = cat_names.len();
    assert!(ncat <= 128, "more than 128 categories");
    // rank by bytes for ties
    let mut order: Vec<usize> = (0..ncat).collect();
    order.sort_by(|&a, &b| cat_names[a].cmp(&cat_names[b]));
    let mut cat_rank = vec![0u32; ncat];
    for (r, &c) in order.iter().enumerate() {
        cat_rank[c] = r as u32;
    }

    // campaigns
    let camp_rows = read_small_csv::<2>(&data_dir.join("campaigns.csv"));
    let mut camp_map: HashMap<Vec<u8>, u32> = HashMap::with_capacity(camp_rows.len() * 2);
    let mut camp_ids: Vec<Vec<u8>> = Vec::new();
    let mut camp_chan: Vec<Vec<u8>> = Vec::new();
    for [c, ch] in &camp_rows {
        if camp_map.contains_key(c) {
            continue;
        }
        camp_map.insert(c.clone(), camp_ids.len() as u32);
        camp_ids.push(c.clone());
        camp_chan.push(ch.clone());
    }
    let mut camp_tab = U64Map::with_capacity(camp_ids.len() + 16);
    for (i, c) in camp_ids.iter().enumerate() {
        camp_tab.get_or_insert(hash64(c, SEED_P), i as u32);
    }
    let mut prod_tab = U64Map::with_capacity(products.len() + 16);
    let mut prod_names: Vec<Vec<u8>> = Vec::with_capacity(products.len());
    let mut prod_cat: Vec<u16> = Vec::with_capacity(products.len());
    for (p, &c) in &products {
        let (_, ins) = prod_tab.get_or_insert(hash64(p, SEED_P), prod_names.len() as u32);
        if ins {
            prod_names.push(p.clone());
            prod_cat.push(c);
        }
    }
    let organic = camp_ids.len() as u32;
    camp_ids.push(b"organic".to_vec());
    camp_chan.push(b"organic".to_vec());
    let ncamp = camp_ids.len();

    // files
    let mut paths: Vec<(PathBuf, Kind)> = Vec::new();
    for p in list_csv(&data_dir.join("links")) {
        paths.push((p, Kind::Link));
    }
    for p in list_csv(&data_dir.join("events")) {
        paths.push((p, Kind::Event));
    }
    for p in list_csv(&data_dir.join("clicks")) {
        paths.push((p, Kind::Click));
    }
    let maps: Vec<Option<Mmap>> = paths
        .iter()
        .map(|(p, _)| {
            let f = std::fs::File::open(p).expect("open");
            let len = f.metadata().map(|m| m.len()).unwrap_or(0);
            if len == 0 {
                None
            } else {
                Some(unsafe { Mmap::map(&f).expect("mmap") })
            }
        })
        .collect();
    let files = Files { maps };
    let kinds: Vec<Kind> = paths.iter().map(|x| x.1).collect();
    let total_bytes: usize = (0..kinds.len()).map(|i| files.data(i).len()).sum();
    let target = (total_bytes / (threads * 16)).clamp(1 << 20, 32 << 20);
    let chunks = make_chunks(&files, &kinds, target);
    lap("mapped");

    // ---------------- links
    let link_chunks: Vec<&Chunk> = chunks.iter().filter(|c| c.kind == Kind::Link).collect();
    let link_parts: Vec<(Vec<(u64, u64, u64, u64)>, Scan)> = run_par(
        threads,
        link_chunks.len(),
        || (Vec::new(), Scan::new()),
        |(out, pos), i| {
            let c = link_chunks[i];
            let d = files.data(c.file);
            for_records::<2>(d, c.start, c.end, pos, |f| {
                if f[0].is_empty() || f[1].is_empty() {
                    return;
                }
                out.push((
                    hash64(f[0], SEED_U),
                    hash64(f[1], SEED_U),
                    pack_loc(c.file, d, f[0]),
                    pack_loc(c.file, d, f[1]),
                ));
            });
        },
    );
    let link_parts: Vec<Vec<(u64, u64, u64, u64)>> = link_parts.into_iter().map(|x| x.0).collect();
    let nlinks: usize = link_parts.iter().map(|v| v.len()).sum();
    let mut idmap = U64Map::with_capacity(nlinks * 2 + 16);
    let mut id_loc: Vec<u64> = Vec::with_capacity(nlinks * 2);
    let mut parent: Vec<u32> = Vec::with_capacity(nlinks * 2);
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        let mut r = x;
        while parent[r as usize] != r {
            r = parent[r as usize];
        }
        while parent[x as usize] != r {
            let n = parent[x as usize];
            parent[x as usize] = r;
            x = n;
        }
        r
    }
    for part in &link_parts {
        for &(ha, hb, la, lb) in part {
            let (a, ins) = idmap.get_or_insert(ha, id_loc.len() as u32);
            if ins {
                id_loc.push(la);
                parent.push(a);
            }
            let (b, ins) = idmap.get_or_insert(hb, id_loc.len() as u32);
            if ins {
                id_loc.push(lb);
                parent.push(b);
            }
            let ra = find(&mut parent, a);
            let rb = find(&mut parent, b);
            if ra != rb {
                if ra < rb {
                    parent[rb as usize] = ra;
                } else {
                    parent[ra as usize] = rb;
                }
            }
        }
    }
    drop(link_parts);
    let nids = id_loc.len();
    // person index per root, name = smallest id
    let mut person_of_root: Vec<u32> = vec![u32::MAX; nids];
    let mut person_name: Vec<u64> = Vec::new();
    let mut person_of: Vec<u32> = vec![0; nids];
    for i in 0..nids {
        let r = find(&mut parent, i as u32) as usize;
        if person_of_root[r] == u32::MAX {
            person_of_root[r] = person_name.len() as u32;
            person_name.push(id_loc[i]);
        } else {
            let p = person_of_root[r] as usize;
            if files.get(id_loc[i]) < files.get(person_name[p]) {
                person_name[p] = id_loc[i];
            }
        }
        person_of[i] = person_of_root[r];
    }
    for s in idmap.slots.iter_mut() {
        if s.v != u32::MAX {
            s.v = person_of[s.v as usize];
        }
    }
    drop(parent);
    drop(person_of_root);
    drop(person_of);
    let link_table = idmap;
    lap(&format!("links: {} lines, {} ids, {} persons", nlinks, nids, person_name.len()));

    // ---------------- parse events and clicks
    let resolve_h = |h: u64| -> u64 {
        match link_table.get(h) {
            Some(p) => LINKED | p as u64,
            None => h & !LINKED,
        }
    };
    let ec_chunks: Vec<&Chunk> = chunks.iter().filter(|c| c.kind != Kind::Link).collect();
    let ev_bytes: usize = (0..kinds.len()).filter(|&i| kinds[i] == Kind::Event).map(|i| files.data(i).len()).sum();
    let ck_bytes: usize = (0..kinds.len()).filter(|&i| kinds[i] == Kind::Click).map(|i| files.data(i).len()).sum();
    const BATCH: usize = 64;
    struct P1 {
        ev: Blocks<EvRec>,
        ck: Blocks<CRec>,
        pos: Scan,
        dc: DateCache,
        pev: Vec<EvRec>,
        pck: Vec<CRec>,
    }
    let flush_ev = |st_ev: &mut Blocks<EvRec>, pev: &mut Vec<EvRec>| {
        for r in pev.iter_mut() {
            if r.etype != 0 {
                r.ukey = resolve_h(r.ukey);
            }
            st_ev.push((r.h1 >> (64 - EB_BITS)) as usize, *r);
        }
        pev.clear();
    };
    let flush_ck = |st_ck: &mut Blocks<CRec>, pck: &mut Vec<CRec>| {
        for r in pck.iter_mut() {
            r.ukey = resolve_h(r.ukey);
            st_ck.push(pbucket(r.ukey), *r);
        }
        pck.clear();
    };
    let p1: Vec<P1> = run_par(
        threads,
        ec_chunks.len(),
        || P1 {
            ev: Blocks::new(NEB),
            ck: Blocks::new(NPB),
            pos: Scan::new(),
            dc: DateCache::new(),
            pev: Vec::with_capacity(BATCH),
            pck: Vec::with_capacity(BATCH),
        },
        |st, i| {
            let c = ec_chunks[i];
            let d = files.data(c.file);
            let P1 { ev, ck, pos, dc, pev, pck } = st;
            if c.kind == Kind::Event {
                for_records::<7>(d, c.start, c.end, pos, |f| {
                    if f[0].is_empty() {
                        return;
                    }
                    let h1 = hash64(f[0], SEED_E1);
                    let ingest_ms = parse_ts_c(f[3], dc).unwrap_or(i64::MAX);
                    let ingest = if ingest_ms == i64::MAX {
                        u32::MAX
                    } else {
                        (ingest_ms / 1000).clamp(0, u32::MAX as i64 - 1) as u32
                    };
                    let mut r = EvRec {
                        h1,
                        eloc: pack_loc(c.file, d, f[0]),
                        ukey: 0,
                        uloc: 0,
                        ts: 0,
                        ingest,
                        price: 0,
                        cat: 0,
                        etype: 0,
                    };
                    'v: {
                        if f[1].is_empty() || f[5].is_empty() {
                            break 'v;
                        }
                        let ts = match parse_ts_c(f[2], dc) {
                            Some(t) if t >= ws && t < we => t,
                            _ => break 'v,
                        };
                        let et = match f[4] {
                            b"view" => 1u8,
                            b"cart" => 2,
                            b"purchase" => 3,
                            _ => break 'v,
                        };
                        if ingest_ms == i64::MAX || ingest_ms - ts > DAY_MS {
                            break 'v;
                        }
                        if et == 3 {
                            match parse_price(f[6]) {
                                Some(p) => r.price = p,
                                None => break 'v,
                            }
                            r.cat = match prod_tab.get(hash64(f[5], SEED_P)) {
                                Some(pi) if prod_names[pi as usize] == f[5] => prod_cat[pi as usize],
                                _ => unknown_cat,
                            };
                        }
                        r.ts = (ts - ws) as u32;
                        let h = hash64(f[1], SEED_U);
                        link_table.prefetch(h);
                        r.ukey = h;
                        r.uloc = pack_loc(c.file, d, f[1]);
                        r.etype = et;
                    }
                    pev.push(r);
                    if pev.len() == BATCH {
                        flush_ev(ev, pev);
                    }
                });
                flush_ev(ev, pev);
            } else {
                for_records::<4>(d, c.start, c.end, pos, |f| {
                    if f[0].is_empty() || f[1].is_empty() {
                        return;
                    }
                    let ts = match parse_ts_c(f[3], dc) {
                        Some(t) if t >= cws && t < we => t,
                        _ => return,
                    };
                    let camp = match camp_tab.get(hash64(f[2], SEED_P)) {
                        Some(c) if camp_ids[c as usize] == f[2] => c,
                        _ => return,
                    };
                    let h = hash64(f[1], SEED_U);
                    link_table.prefetch(h);
                    pck.push(CRec { ukey: h, ts, cloc: pack_loc(c.file, d, f[0]), camp });
                    if pck.len() == BATCH {
                        flush_ck(ck, pck);
                    }
                });
                flush_ck(ck, pck);
            }
        },
    );
    lap("parsed");

    // ---------------- dedup events per event-id bucket
    let p1ev: Vec<&Blocks<EvRec>> = p1.iter().map(|x| &x.ev).collect();
    #[derive(Clone, Copy)]
    struct DSlot {
        h1: u64,
        ingest: u32,
        r: u32, // (thread << 24) | index, u32::MAX = empty
    }
    struct P2 {
        out: Blocks<SRec>,
        tab: Vec<DSlot>,
    }
    let p2: Vec<P2> = run_par(
        threads,
        NEB,
        || P2 { out: Blocks::new(NPB), tab: Vec::new() },
        |st, b| {
            let n: usize = p1ev.iter().map(|t| t.len(b)).sum();
            if n == 0 {
                return;
            }
            let cap = (n * 2).next_power_of_two();
            let mask = cap - 1;
            let tab = &mut st.tab;
            tab.clear();
            tab.resize(cap, DSlot { h1: 0, ingest: 0, r: u32::MAX });
            for (t, src) in p1ev.iter().enumerate() {
                for (i, r) in src.iter(b).enumerate() {
                    let me = ((t as u32) << 24) | i as u32;
                    let mut s = ((r.h1 >> 20) as usize) & mask;
                    loop {
                        let x = tab[s];
                        if x.r == u32::MAX {
                            tab[s] = DSlot { h1: r.h1, ingest: r.ingest, r: me };
                            break;
                        }
                        if x.h1 == r.h1 {
                            let o = p1ev[(x.r >> 24) as usize].get(b, (x.r & 0xff_ffff) as usize);
                            if files.get(o.eloc) == files.get(r.eloc) {
                                if r.ingest < x.ingest {
                                    tab[s] = DSlot { h1: r.h1, ingest: r.ingest, r: me };
                                }
                                break;
                            }
                        }
                        s = (s + 1) & mask;
                    }
                }
            }
            let out = &mut st.out;
            for x in tab.iter() {
                if x.r == u32::MAX {
                    continue;
                }
                let r = p1ev[(x.r >> 24) as usize].get(b, (x.r & 0xff_ffff) as usize);
                if r.etype == 0 {
                    continue;
                }
                out.push(pbucket(r.ukey), SRec { ukey: r.ukey, uloc: r.uloc, pk: pack_ev(r.ts, r.etype, r.price, r.cat) });
            }
        },
    );
    drop(p1ev);
    let p2: Vec<Blocks<SRec>> = p2.into_iter().map(|x| x.out).collect();
    let p1ck: Vec<Blocks<CRec>> = p1.into_iter().map(|x| x.ck).collect();
    lap("deduped");

    // ---------------- per person bucket
    struct P3 {
        rows: Vec<UserRow>,
        stats: Vec<CStat>,
        evs: Vec<u64>,
        names: Vec<u64>,
        cks: Vec<CRec>,
        tm: [u128; 4],
        lmap: LMap,
        lids: Vec<u32>,
        cnt: Vec<u32>,
        eoff: Vec<u32>,
        clid: Vec<u32>,
        ccnt: Vec<u32>,
        coff: Vec<u32>,
    }
    let p3: Vec<P3> = run_par(
        threads,
        NPB,
        || P3 { rows: Vec::new(), stats: vec![CStat::default(); ncamp * ndays], evs: Vec::new(),
            names: Vec::new(),
            cks: Vec::new(),
            tm: [0; 4],
            lmap: LMap::new(),
            lids: Vec::new(),
            cnt: Vec::new(),
            eoff: Vec::new(),
            clid: Vec::new(),
            ccnt: Vec::new(),
            coff: Vec::new(),
        },
        |st, b| {
            let n: usize = p2.iter().map(|t| t.len(b)).sum();
            if n == 0 {
                return;
            }
            let t0 = std::time::Instant::now();
            // dense local person ids, then counting sort by person
            let lm = &mut st.lmap;
            lm.reset(n / 8 + 16);
            let lids = &mut st.lids;
            lids.clear();
            let cnt = &mut st.cnt;
            cnt.clear();
            let names = &mut st.names;
            names.clear();
            for t in &p2 {
                for r in t.iter(b) {
                    let (l, ins) = lm.get_or_insert(r.ukey);
                    if ins {
                        cnt.push(0);
                        names.push(if r.ukey & LINKED != 0 { person_name[(r.ukey & !LINKED) as usize] } else { r.uloc });
                    }
                    cnt[l as usize] += 1;
                    lids.push(l);
                }
            }
            let m = cnt.len();
            let eoff = &mut st.eoff;
            eoff.clear();
            eoff.push(0);
            let mut acc = 0u32;
            for c in cnt.iter_mut() {
                acc += *c;
                eoff.push(acc);
                *c = acc - *c; // write cursor
            }
            let evs = &mut st.evs;
            evs.clear();
            evs.resize(n, 0);
            let mut k = 0;
            for t in &p2 {
                for r in t.iter(b) {
                    let l = lids[k] as usize;
                    k += 1;
                    evs[cnt[l] as usize] = r.pk;
                    cnt[l] += 1;
                }
            }
            // clicks of persons that have events
            let cl = &mut st.clid;
            cl.clear();
            let ccnt = &mut st.ccnt;
            ccnt.clear();
            ccnt.resize(m, 0);
            for t in &p1ck {
                for r in t.iter(b) {
                    let l = lm.get(r.ukey);
                    if l != u32::MAX {
                        ccnt[l as usize] += 1;
                    }
                    cl.push(l);
                }
            }
            let coff = &mut st.coff;
            coff.clear();
            coff.push(0);
            let mut acc = 0u32;
            for c in ccnt.iter_mut() {
                acc += *c;
                coff.push(acc);
                *c = acc - *c;
            }
            let nc = acc as usize;
            let cks = &mut st.cks;
            cks.clear();
            cks.resize(nc, CRec { ukey: 0, ts: 0, cloc: 0, camp: 0 });
            let mut k = 0;
            for t in &p1ck {
                for r in t.iter(b) {
                    let l = cl[k];
                    k += 1;
                    if l != u32::MAX {
                        cks[ccnt[l as usize] as usize] = *r;
                        ccnt[l as usize] += 1;
                    }
                }
            }
            let t1 = std::time::Instant::now();
            for l in 0..m {
                let g = &mut evs[eoff[l] as usize..eoff[l + 1] as usize];
                if g.len() > 1 {
                    g.sort_unstable();
                }
            }
            let t2 = std::time::Instant::now();
            for l in 0..m {
                let g = &mut cks[coff[l] as usize..coff[l + 1] as usize];
                if g.len() > 1 {
                    g.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| files.get(a.cloc).cmp(files.get(b.cloc))));
                }
            }
            let t3 = std::time::Instant::now();
            process_bucket(&st.evs, &st.eoff, &st.names, &st.cks, &st.coff, &mut st.rows, &mut st.stats, &Ctx {
                ws_day,
                ndays,
                ncat,
                cat_rank: &cat_rank,
                organic,
                ws,
            });
            let t4 = std::time::Instant::now();
            st.tm[0] += (t1 - t0).as_nanos();
            st.tm[1] += (t2 - t1).as_nanos();
            st.tm[2] += (t3 - t2).as_nanos();
            st.tm[3] += (t4 - t3).as_nanos();
        },
    );
    drop(p2);
    if verbose {
        let mut tm = [0u128; 4];
        for t in &p3 { for k in 0..4 { tm[k] += t.tm[k]; } }
        eprintln!("  phase3 thread-ms: gather {} sortE {} sortC {} process {}", tm[0] / 1_000_000, tm[1] / 1_000_000, tm[2] / 1_000_000, tm[3] / 1_000_000);
    }
    lap("sessionized");

    // ---------------- outputs
    let ud = out_dir.join("user_daily");
    let cd = out_dir.join("campaign_daily");
    std::fs::create_dir_all(&ud).unwrap();
    std::fs::create_dir_all(&cd).unwrap();
    let mut stats = vec![CStat::default(); ncamp * ndays];
    for t in &p3 {
        for (a, b) in stats.iter_mut().zip(&t.stats) {
            a.sessions += b.sessions;
            a.purch += b.purch;
            a.revenue += b.revenue;
            a.assisted += b.assisted;
        }
    }
    // campaign_daily
    {
        let mut cid = Vec::new();
        let mut ch = Vec::new();
        let mut day = Vec::new();
        let mut s = Vec::new();
        let mut ps = Vec::new();
        let mut rv = Vec::new();
        let mut asst = Vec::new();
        for c in 0..ncamp {
            for d in 0..ndays {
                let x = &stats[c * ndays + d];
                if x.sessions == 0 && x.assisted == 0 {
                    continue;
                }
                cid.push(&camp_ids[c][..]);
                ch.push(&camp_chan[c][..]);
                day.push((ws_day + d as i64) as i32);
                s.push(x.sessions as i64);
                ps.push(x.purch as i64);
                rv.push(x.revenue as i64);
                asst.push(x.assisted as i64);
            }
        }
        parquet::write(
            &cd.join("part-0.parquet"),
            &[
                ("campaign_id", parquet::Col::Str(cid)),
                ("channel", parquet::Col::Str(ch)),
                ("day", parquet::Col::Date(day)),
                ("sessions", parquet::Col::I64(s)),
                ("purchasing_sessions", parquet::Col::I64(ps)),
                ("revenue_cents", parquet::Col::I64(rv)),
                ("assisted_sessions", parquet::Col::I64(asst)),
            ],
        )
        .unwrap();
    }
    // user_daily: one file per worker
    let nonempty: Vec<&P3> = p3.iter().filter(|t| !t.rows.is_empty()).collect();
    let write_ud = |i: usize, rows: &[UserRow]| {
        let names: Vec<&[u8]> = rows.iter().map(|r| files.get(r.name)).collect();
        let tops: Vec<Option<&[u8]>> =
            rows.iter().map(|r| if r.top < 0 { None } else { Some(&cat_names[r.top as usize][..]) }).collect();
        parquet::write(
            &ud.join(format!("part-{}.parquet", i)),
            &[
                ("user_id", parquet::Col::Str(names)),
                ("day", parquet::Col::Date(rows.iter().map(|r| r.day).collect())),
                ("sessions", parquet::Col::I64(rows.iter().map(|r| r.sessions as i64).collect())),
                ("events", parquet::Col::I64(rows.iter().map(|r| r.events as i64).collect())),
                ("revenue_cents", parquet::Col::I64(rows.iter().map(|r| r.revenue as i64).collect())),
                ("top_category", parquet::Col::OptStr(tops)),
            ],
        )
        .unwrap();
    };
    if nonempty.is_empty() {
        write_ud(0, &[]);
    } else {
        std::thread::scope(|sc| {
            for (i, t) in nonempty.iter().enumerate() {
                let w = &write_ud;
                sc.spawn(move || w(i, &t.rows));
            }
        });
    }
    lap("written");
    // skip tearing down gigabytes of buffers
    std::process::exit(0);
}

struct Ctx<'a> {
    ws_day: i64,
    ndays: usize,
    ncat: usize,
    cat_rank: &'a [u32],
    organic: u32,
    ws: i64,
}

struct Sess {
    start: i64,
    day: u32,
    revenue: u64,
    purch: bool,
}

fn process_bucket(
    evs: &[u64],
    eoff: &[u32],
    names: &[u64],
    cks: &[CRec],
    coff: &[u32],
    rows: &mut Vec<UserRow>,
    stats: &mut [CStat],
    ctx: &Ctx,
) {
    let ndays = ctx.ndays;
    let mut sessions: Vec<Sess> = Vec::new();
    let mut starts: Vec<i64> = Vec::new();
    let mut next_day: Vec<u32> = Vec::new();
    let mut cat_rev: Vec<u64> = vec![0; ctx.ncat];
    let mut touched: Vec<u16> = Vec::new();
    let mut by_camp: Vec<(u32, i64)> = Vec::new();
    for l in 0..eoff.len() - 1 {
        let pe = &evs[eoff[l] as usize..eoff[l + 1] as usize];
        let pc = &cks[coff[l] as usize..coff[l + 1] as usize];
        let name = names[l];

        // sessionize + user_daily
        sessions.clear();
        let mut last_ts = i64::MIN;
        let mut cur: Option<UserRow> = None;
        let flush = |cur: &mut Option<UserRow>, cat_rev: &mut Vec<u64>, touched: &mut Vec<u16>, rows: &mut Vec<UserRow>| {
            if let Some(mut r) = cur.take() {
                let mut best: i32 = -1;
                let mut bv = 0u64;
                for &c in touched.iter() {
                    let v = cat_rev[c as usize];
                    if best < 0
                        || v > bv
                        || (v == bv && ctx.cat_rank[c as usize] < ctx.cat_rank[best as usize])
                    {
                        best = c as i32;
                        bv = v;
                    }
                    cat_rev[c as usize] = 0;
                }
                touched.clear();
                r.top = best;
                rows.push(r);
            }
        };
        for &pk in pe {
            let e_ts = ctx.ws + (pk >> 33) as i64;
            let e_etype = ((pk >> 31) & 3) as u8;
            let e_price = ((pk >> 7) & 0xff_ffff) as u32;
            let e_cat = (pk & 127) as u16;
            if sessions.is_empty() || e_ts - last_ts > GAP_MS {
                let day = (e_ts.div_euclid(DAY_MS) - ctx.ws_day) as u32;
                if cur.as_ref().map(|r| r.day != (ctx.ws_day + day as i64) as i32).unwrap_or(true) {
                    flush(&mut cur, &mut cat_rev, &mut touched, rows);
                    cur = Some(UserRow {
                        name,
                        day: (ctx.ws_day + day as i64) as i32,
                        sessions: 0,
                        events: 0,
                        revenue: 0,
                        top: -1,
                    });
                }
                cur.as_mut().unwrap().sessions += 1;
                sessions.push(Sess { start: e_ts, day, revenue: 0, purch: false });
            }
            last_ts = e_ts;
            let r = cur.as_mut().unwrap();
            r.events += 1;
            if e_etype == 3 {
                let s = sessions.last_mut().unwrap();
                s.revenue += e_price as u64;
                s.purch = true;
                r.revenue += e_price as u64;
                if cat_rev[e_cat as usize] == 0 {
                    touched.push(e_cat);
                }
                cat_rev[e_cat as usize] += e_price as u64;
            }
        }
        flush(&mut cur, &mut cat_rev, &mut touched, rows);

        // credit
        let mut k = 0usize;
        for s in &sessions {
            while k < pc.len() && pc[k].ts <= s.start {
                k += 1;
            }
            let camp = if k > 0 && s.start - pc[k - 1].ts <= WEEK_MS {
                let t = pc[k - 1].ts;
                let mut m = k - 1;
                while m > 0 && pc[m - 1].ts == t {
                    m -= 1;
                }
                pc[m].camp
            } else {
                ctx.organic
            };
            let st = &mut stats[camp as usize * ndays + s.day as usize];
            st.sessions += 1;
            st.revenue += s.revenue;
            st.purch += s.purch as u64;
        }

        // assists
        if !pc.is_empty() {
            starts.clear();
            starts.extend(sessions.iter().map(|s| s.start));
            next_day.clear();
            next_day.resize(sessions.len(), 0);
            let mut e = sessions.len();
            for x in (0..sessions.len()).rev() {
                if x + 1 < sessions.len() && sessions[x + 1].day != sessions[x].day {
                    e = x + 1;
                }
                next_day[x] = e as u32;
            }
            by_camp.clear();
            by_camp.extend(pc.iter().map(|c| (c.camp, c.ts)));
            by_camp.sort_unstable();
            let mut add = |camp: u32, a: i64, b: i64| {
                let lo = starts.partition_point(|&x| x < a);
                let hi = starts.partition_point(|&x| x <= b);
                let mut x = lo;
                while x < hi {
                    let e = (next_day[x] as usize).min(hi);
                    stats[camp as usize * ndays + sessions[x].day as usize].assisted += (e - x) as u64;
                    x = e;
                }
            };
            let mut q = 0;
            while q < by_camp.len() {
                let camp = by_camp[q].0;
                let mut a = by_camp[q].1;
                let mut b = a + WEEK_MS;
                q += 1;
                while q < by_camp.len() && by_camp[q].0 == camp {
                    let t = by_camp[q].1;
                    if t <= b {
                        b = t + WEEK_MS;
                    } else {
                        add(camp, a, b);
                        a = t;
                        b = t + WEEK_MS;
                    }
                    q += 1;
                }
                add(camp, a, b);
            }
        }
    }
}
