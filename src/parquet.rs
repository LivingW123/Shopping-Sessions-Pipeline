//! Minimal Parquet writer: one row group, one PLAIN uncompressed data page per
//! column. Supports required/optional UTF8 strings, INT64 and DATE (INT32).

use std::io::Write;

pub enum Col<'a> {
    Str(Vec<&'a [u8]>),
    OptStr(Vec<Option<&'a [u8]>>),
    I64(Vec<i64>),
    Date(Vec<i32>),
}

impl Col<'_> {
    fn len(&self) -> usize {
        match self {
            Col::Str(v) => v.len(),
            Col::OptStr(v) => v.len(),
            Col::I64(v) => v.len(),
            Col::Date(v) => v.len(),
        }
    }
    fn phys(&self) -> i32 {
        match self {
            Col::Str(_) | Col::OptStr(_) => 6,
            Col::I64(_) => 2,
            Col::Date(_) => 1,
        }
    }
}

struct Tw {
    b: Vec<u8>,
    last: Vec<i16>,
}

impl Tw {
    fn new() -> Self {
        Tw { b: Vec::new(), last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.b.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.b.push(v as u8);
    }
    fn field(&mut self, id: i16, ty: u8) {
        let last = *self.last.last().unwrap();
        let d = id - last;
        if d > 0 && d <= 15 {
            self.b.push(((d as u8) << 4) | ty);
        } else {
            self.b.push(ty);
            self.varint((((id as i32) << 1) ^ ((id as i32) >> 31)) as u32 as u64);
        }
        *self.last.last_mut().unwrap() = id;
    }
    fn i32f(&mut self, id: i16, v: i32) {
        self.field(id, 5);
        self.varint(((v << 1) ^ (v >> 31)) as u32 as u64);
    }
    fn i64f(&mut self, id: i16, v: i64) {
        self.field(id, 6);
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn binf(&mut self, id: i16, s: &[u8]) {
        self.field(id, 8);
        self.varint(s.len() as u64);
        self.b.extend_from_slice(s);
    }
    fn sbeg(&mut self, id: i16) {
        self.field(id, 12);
        self.last.push(0);
    }
    fn ebeg(&mut self) {
        self.last.push(0);
    }
    fn send(&mut self) {
        self.b.push(0);
        self.last.pop();
    }
    fn lbeg(&mut self, id: i16, ety: u8, n: usize) {
        self.field(id, 9);
        if n < 15 {
            self.b.push(((n as u8) << 4) | ety);
        } else {
            self.b.push(0xF0 | ety);
            self.varint(n as u64);
        }
    }
    fn i32elem(&mut self, v: i32) {
        self.varint(((v << 1) ^ (v >> 31)) as u32 as u64);
    }
    fn binelem(&mut self, s: &[u8]) {
        self.varint(s.len() as u64);
        self.b.extend_from_slice(s);
    }
}

pub fn write(path: &std::path::Path, cols: &[(&str, Col)]) -> std::io::Result<()> {
    let nrows = cols.first().map(|c| c.1.len()).unwrap_or(0);
    let mut out: Vec<u8> = Vec::with_capacity(nrows * 48 + 4096);
    out.extend_from_slice(b"PAR1");
    // (offset, total size) per column
    let mut chunks: Vec<(i64, i64)> = Vec::new();
    if nrows > 0 {
        for (_, col) in cols {
            let mut data: Vec<u8> = Vec::new();
            match col {
                Col::Str(v) => {
                    let tot: usize = v.iter().map(|s| s.len() + 4).sum();
                    data.reserve(tot);
                    for s in v {
                        data.extend_from_slice(&(s.len() as u32).to_le_bytes());
                        data.extend_from_slice(s);
                    }
                }
                Col::OptStr(v) => {
                    // definition levels: RLE/bit-packed hybrid, bit width 1, bit-packed run
                    let groups = v.len().div_ceil(8);
                    let mut lv: Vec<u8> = Vec::with_capacity(groups + 10);
                    let mut hdr = (groups as u64) << 1 | 1;
                    while hdr >= 0x80 {
                        lv.push((hdr as u8) | 0x80);
                        hdr >>= 7;
                    }
                    lv.push(hdr as u8);
                    let mut bits = vec![0u8; groups];
                    for (i, s) in v.iter().enumerate() {
                        if s.is_some() {
                            bits[i >> 3] |= 1 << (i & 7);
                        }
                    }
                    lv.extend_from_slice(&bits);
                    data.extend_from_slice(&(lv.len() as u32).to_le_bytes());
                    data.extend_from_slice(&lv);
                    for s in v.iter().flatten() {
                        data.extend_from_slice(&(s.len() as u32).to_le_bytes());
                        data.extend_from_slice(s);
                    }
                }
                Col::I64(v) => {
                    data.reserve(v.len() * 8);
                    for x in v {
                        data.extend_from_slice(&x.to_le_bytes());
                    }
                }
                Col::Date(v) => {
                    data.reserve(v.len() * 4);
                    for x in v {
                        data.extend_from_slice(&x.to_le_bytes());
                    }
                }
            }
            let mut ph = Tw::new();
            ph.i32f(1, 0); // DATA_PAGE
            ph.i32f(2, data.len() as i32);
            ph.i32f(3, data.len() as i32);
            ph.sbeg(5);
            ph.i32f(1, nrows as i32);
            ph.i32f(2, 0); // PLAIN
            ph.i32f(3, 3); // RLE
            ph.i32f(4, 3); // RLE
            ph.send();
            ph.b.push(0);
            let off = out.len() as i64;
            out.extend_from_slice(&ph.b);
            out.extend_from_slice(&data);
            chunks.push((off, (ph.b.len() + data.len()) as i64));
        }
    }
    // FileMetaData
    let mut m = Tw::new();
    m.i32f(1, 1);
    m.lbeg(2, 12, cols.len() + 1);
    // root
    m.ebeg();
    m.binf(4, b"schema");
    m.i32f(5, cols.len() as i32);
    m.send();
    for (name, col) in cols {
        m.ebeg();
        m.i32f(1, col.phys());
        m.i32f(3, if matches!(col, Col::OptStr(_)) { 1 } else { 0 });
        m.binf(4, name.as_bytes());
        match col {
            Col::Str(_) | Col::OptStr(_) => {
                m.i32f(6, 0); // UTF8
                m.sbeg(10);
                m.sbeg(1); // STRING
                m.send();
                m.send();
            }
            Col::Date(_) => {
                m.i32f(6, 6); // DATE
                m.sbeg(10);
                m.sbeg(6); // DATE
                m.send();
                m.send();
            }
            Col::I64(_) => {}
        }
        m.send();
    }
    m.i64f(3, nrows as i64);
    if nrows > 0 {
        m.lbeg(4, 12, 1);
        m.ebeg();
        m.lbeg(1, 12, cols.len());
        let mut total = 0i64;
        for (i, (name, col)) in cols.iter().enumerate() {
            let (off, sz) = chunks[i];
            total += sz;
            m.ebeg();
            m.i64f(2, off);
            m.sbeg(3);
            m.i32f(1, col.phys());
            m.lbeg(2, 5, 2);
            m.i32elem(0);
            m.i32elem(3);
            m.lbeg(3, 8, 1);
            m.binelem(name.as_bytes());
            m.i32f(4, 0);
            m.i64f(5, nrows as i64);
            m.i64f(6, sz);
            m.i64f(7, sz);
            m.i64f(9, off);
            m.send();
            m.send();
        }
        m.i64f(2, total);
        m.i64f(3, nrows as i64);
        m.send();
    } else {
        m.lbeg(4, 12, 0);
    }
    m.binf(6, b"shopping-sessions");
    m.b.push(0);
    out.extend_from_slice(&m.b);
    out.extend_from_slice(&(m.b.len() as u32).to_le_bytes());
    out.extend_from_slice(b"PAR1");
    let mut f = std::fs::File::create(path)?;
    f.write_all(&out)?;
    Ok(())
}
