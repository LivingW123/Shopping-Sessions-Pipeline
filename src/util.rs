//! Hashing, CSV field splitting and timestamp parsing, all exactly per rules.md.

pub const DAY_MS: i64 = 86_400_000;
pub const WEEK_MS: i64 = 7 * DAY_MS;
pub const GAP_MS: i64 = 1_800_000;

#[inline(always)]
fn mum(a: u64, b: u64) -> u64 {
    let r = (a as u128) * (b as u128);
    (r as u64) ^ ((r >> 64) as u64)
}

const P0: u64 = 0xa076_1d64_78bd_642f;
const P1: u64 = 0xe703_7ed1_a0b4_28db;
const P2: u64 = 0x8ebc_6af0_9c88_c6e3;
const P3: u64 = 0x5899_65cc_7537_4cc3;

/// 64-bit hash of a byte string (wyhash-style). Strings are opaque IDs.
/// Every byte is covered by (overlapping) unaligned loads; no memcpy.
#[inline]
pub fn hash64(b: &[u8], seed: u64) -> u64 {
    #[inline(always)]
    fn r8(b: &[u8], i: usize) -> u64 {
        u64::from_le_bytes(b[i..i + 8].try_into().unwrap())
    }
    #[inline(always)]
    fn r4(b: &[u8], i: usize) -> u64 {
        u32::from_le_bytes(b[i..i + 4].try_into().unwrap()) as u64
    }
    let n = b.len();
    let mut h = seed ^ P0;
    let (a, c);
    if n <= 16 {
        if n >= 8 {
            a = r8(b, 0);
            c = r8(b, n - 8);
        } else if n >= 4 {
            a = r4(b, 0);
            c = r4(b, n - 4);
        } else if n > 0 {
            a = ((b[0] as u64) << 16) | ((b[n >> 1] as u64) << 8) | b[n - 1] as u64;
            c = 0;
        } else {
            a = 0;
            c = 0;
        }
    } else {
        let mut i = 0;
        while i + 16 < n {
            h = mum(r8(b, i) ^ P1, r8(b, i + 8) ^ h);
            i += 16;
        }
        a = r8(b, n - 16);
        c = r8(b, n - 8);
    }
    let r = mum(mum(a ^ P1, c ^ h ^ P2) ^ P3, (n as u64) ^ P0 ^ seed);
    r | (r == 0) as u64 // never 0: 0 marks empty slots
}

/// Splits one line (without its `\n`) into exactly N fields, stripping the
/// syntactic quotes. Returns false if the line is malformed.
#[inline]
pub fn split_fields<'a, const N: usize>(line: &'a [u8], out: &mut [&'a [u8]; N]) -> bool {
    let mut k = 0;
    let mut start = 0;
    let n = line.len();
    let mut i = 0;
    loop {
        // find end of field
        let mut quotes = 0u32;
        while i < n {
            let c = line[i];
            if c == b',' {
                break;
            }
            if c == b'"' {
                quotes += 1;
            }
            i += 1;
        }
        if k == N {
            return false;
        }
        let f = &line[start..i];
        if quotes == 0 {
            out[k] = f;
        } else {
            if quotes != 2 || f.len() < 2 || f[0] != b'"' || f[f.len() - 1] != b'"' {
                return false;
            }
            out[k] = &f[1..f.len() - 1];
        }
        k += 1;
        if i >= n {
            break;
        }
        i += 1; // skip comma
        start = i;
    }
    k == N
}

#[inline(always)]
fn d2(b: &[u8], i: usize) -> Option<u32> {
    let a = b[i].wrapping_sub(b'0');
    let c = b[i + 1].wrapping_sub(b'0');
    if a > 9 || c > 9 {
        None
    } else {
        Some(a as u32 * 10 + c as u32)
    }
}

#[inline]
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy as i64;
    era * 146_097 + doe - 719_468
}

#[inline]
fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

/// Parses `YYYY-MM-DDTHH:MM:SS` (first 19 bytes) into epoch ms, local time.
#[inline]
fn parse_datetime19(b: &[u8]) -> Option<i64> {
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let y = (d2(b, 0)? * 100 + d2(b, 2)?) as i64;
    let mo = d2(b, 5)?;
    let d = d2(b, 8)?;
    let h = d2(b, 11)?;
    let mi = d2(b, 14)?;
    let s = d2(b, 17)?;
    if !(1..=12).contains(&mo) || d == 0 || h > 23 || mi > 59 || s > 59 {
        return None;
    }
    let dim = match mo {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if is_leap(y) {
                29
            } else {
                28
            }
        }
    };
    if d > dim {
        return None;
    }
    let days = days_from_civil(y, mo, d);
    Some(((days * 86_400) + (h as i64) * 3600 + (mi as i64) * 60 + s as i64) * 1000)
}

/// Direct-mapped cache of validated `YYYY-MM-DD` prefixes -> days since epoch.
pub struct DateCache {
    keys: [(u64, u16); 64],
    days: [i64; 64],
}

impl DateCache {
    pub fn new() -> Self {
        DateCache { keys: [(0, 0); 64], days: [i64::MIN; 64] }
    }
}

/// Parses `YYYY-MM-DDTHH:MM:SS` with a date cache.
#[inline(always)]
fn parse_datetime19_c(b: &[u8], dc: &mut DateCache) -> Option<i64> {
    let k1 = u64::from_le_bytes(b[0..8].try_into().unwrap());
    let k2 = u16::from_le_bytes(b[8..10].try_into().unwrap());
    let slot = ((b[9] as usize) ^ ((b[8] as usize) << 1) ^ ((b[6] as usize) << 3)) & 63;
    let days = if dc.keys[slot] == (k1, k2) && dc.days[slot] != i64::MIN {
        dc.days[slot]
    } else {
        if b[4] != b'-' || b[7] != b'-' {
            return None;
        }
        let y = (d2(b, 0)? * 100 + d2(b, 2)?) as i64;
        let mo = d2(b, 5)?;
        let d = d2(b, 8)?;
        if !(1..=12).contains(&mo) || d == 0 {
            return None;
        }
        let dim = match mo {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            _ => {
                if is_leap(y) {
                    29
                } else {
                    28
                }
            }
        };
        if d > dim {
            return None;
        }
        let days = days_from_civil(y, mo, d);
        dc.keys[slot] = (k1, k2);
        dc.days[slot] = days;
        days
    };
    if b[10] != b'T' {
        return None;
    }
    // "HH:MM:SS" in one 8-byte load: xor with the "00:00:00" template leaves
    // digit values in digit bytes and zero in colon bytes when well-formed.
    let w = u64::from_le_bytes(b[11..19].try_into().unwrap());
    let x = w ^ 0x3030_3a30_303a_3030;
    if (x & 0xF0F0_F0F0_F0F0_F0F0) != 0
        || (x.wrapping_add(0x0606_0606_0606_0606) & 0xF0F0_F0F0_F0F0_F0F0) != 0
        || (x & 0x0000_FF00_00FF_0000) != 0
    {
        return None;
    }
    let byte = |i: u32| ((x >> (8 * i)) & 0xff) as i64;
    let h = byte(0) * 10 + byte(1);
    let mi = byte(3) * 10 + byte(4);
    let s = byte(6) * 10 + byte(7);
    if h > 23 || mi > 59 || s > 59 {
        return None;
    }
    Some(((days * 86_400) + h * 3600 + mi * 60 + s) * 1000)
}

/// `parse_ts` with a date cache.
#[inline]
pub fn parse_ts_c(b: &[u8], dc: &mut DateCache) -> Option<i64> {
    let n = b.len();
    if n == 20 && b[19] == b'Z' {
        return parse_datetime19_c(b, dc);
    }
    if n == 25 && (b[19] == b'+' || b[19] == b'-') && b[22] == b':' {
        let base = parse_datetime19_c(b, dc)?;
        let oh = d2(b, 20)?;
        let om = d2(b, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        let off = ((oh as i64) * 60 + om as i64) * 60_000;
        return Some(if b[19] == b'+' { base - off } else { base + off });
    }
    if n <= 16 {
        // F3 fast path: two overlapping 8-byte loads, left-padded with '0'
        // digits, SWAR-validated and converted (no memcpy)
        if n < 8 {
            return parse_ts(b);
        }
        let k = n - 8; // leading digits beyond the last 8
        let lo = u64::from_le_bytes(b[k..n].try_into().unwrap());
        let hi = if k == 0 {
            0x3030_3030_3030_3030
        } else {
            let w = u64::from_le_bytes(b[0..8].try_into().unwrap());
            (w << (8 * (8 - k))) | 0x3030_3030_3030_3030u64.checked_shr(8 * k as u32).unwrap_or(0)
        };
        if !swar_digits(hi) || !swar_digits(lo) {
            return None;
        }
        return Some((swar8(hi) * 100_000_000 + swar8(lo)) as i64);
    }
    parse_ts(b)
}

#[inline(always)]
fn swar_digits(x: u64) -> bool {
    (x & 0xF0F0_F0F0_F0F0_F0F0) | (((x.wrapping_add(0x0606_0606_0606_0606)) & 0xF0F0_F0F0_F0F0_F0F0) >> 4)
        == 0x3333_3333_3333_3333
}

/// 8 ASCII digits (first digit at the lowest address) -> value.
#[inline(always)]
fn swar8(x: u64) -> u64 {
    let mut v = x.wrapping_sub(0x3030_3030_3030_3030);
    v = (v.wrapping_mul(10) + (v >> 8)) & 0x00FF_00FF_00FF_00FF;
    v = (v.wrapping_mul(100) + (v >> 16)) & 0x0000_FFFF_0000_FFFF;
    (v.wrapping_mul(10000) + (v >> 32)) & 0x0000_0000_FFFF_FFFF
}

/// Parses an event/click `ts` (F1, F2 or F3). Returns None for a bad
/// timestamp. F3 values that overflow i64 return i64::MAX (outside any window).
#[inline]
pub fn parse_ts(b: &[u8]) -> Option<i64> {
    let n = b.len();
    if n == 0 {
        return None;
    }
    if n == 20 && b[19] == b'Z' {
        return parse_datetime19(b);
    }
    if n == 25 && (b[19] == b'+' || b[19] == b'-') && b[22] == b':' {
        let base = parse_datetime19(b)?;
        let oh = d2(b, 20)?;
        let om = d2(b, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        let off = ((oh as i64) * 60 + om as i64) * 60_000;
        return Some(if b[19] == b'+' { base - off } else { base + off });
    }
    // F3: digits only
    let mut v: u64 = 0;
    let mut sig = 0usize;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        if sig == 0 && d == 0 {
            continue;
        }
        sig += 1;
        if sig > 19 {
            // keep validating digits, value overflows
            continue;
        }
        v = v * 10 + d as u64;
    }
    if sig > 19 || v > i64::MAX as u64 {
        return Some(i64::MAX);
    }
    Some(v as i64)
}

/// Purchase price: digits only, value 1..=10_000_000.
#[inline]
pub fn parse_price(b: &[u8]) -> Option<u32> {
    if b.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v * 10 + d as u64;
        if v > 10_000_000 {
            // still need all-digits check, but value is already too big
            v = 10_000_001;
        }
    }
    if v == 0 || v > 10_000_000 {
        None
    } else {
        Some(v as u32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ts_forms() {
        assert_eq!(parse_ts(b"2026-10-03T23:58:10Z"), Some(1791071890000));
        assert_eq!(parse_ts(b"2026-10-03T19:58:10-04:00"), Some(1791071890000));
        assert_eq!(parse_ts(b"1791071890000"), Some(1791071890000));
        assert_eq!(parse_ts(b"0001791071890000"), Some(1791071890000));
        assert_eq!(parse_ts(b"2026-10-03 23:58:10Z"), None);
        assert_eq!(parse_ts(b"2026-02-29T00:00:00Z"), None);
        assert!(parse_ts(b"2028-02-29T00:00:00Z").is_some());
        assert_eq!(parse_ts(b"2026-10-03T23:58:10+24:00"), None);
        assert_eq!(parse_ts(b"2026-10-03T23:58:10+0200"), None);
        assert_eq!(parse_ts(b"1.79e12"), None);
        assert_eq!(parse_ts(b"999999999999999999999"), Some(i64::MAX));
    }
    #[test]
    fn hash_distinct() {
        let mut seen = std::collections::HashSet::new();
        let mut n = 0;
        for len in 0..=48usize {
            for k in 0..8000u64 {
                let mut v: Vec<u8> = (0..len).map(|i| b'a' + ((k.wrapping_mul(2654435761) >> (i % 13)) % 26) as u8).collect();
                if len > 0 {
                    v[(k as usize) % len] = (k % 251) as u8;
                }
                if seen.insert(v.clone()) {
                    n += 1;
                    assert!(seen.len() == n);
                }
            }
        }
        let hs: std::collections::HashSet<u64> = seen.iter().map(|v| hash64(v, 7)).collect();
        assert_eq!(hs.len(), seen.len());
    }
    #[test]
    fn ts_cache() {
        let mut dc = DateCache::new();
        for t in [&b"2026-10-03T23:58:10Z"[..], b"2026-10-03T19:58:10-04:00", b"2026-02-29T00:00:00Z", b"2028-02-29T00:00:00Z",
                  b"2026-10-03T24:00:00Z", b"2026-10-03 23:58:10Z", b"2026-13-03T23:58:10Z", b"2026-10-03T23:58:10z", b"1791071890000",
                  b"2026-10-03T23:58:10+24:00", b"2026-10-03T23:58:10+05:60", b"2026-10-03T23:58:10-00:00",
                  b"2026-10-03T23:58:1aZ", b"2026-10-03T23:5:10Z0", b"2026-10-03T2::58:10Z", b"2026-10-03T23;58:10Z", b"2026-10-03T23:58:60Z",
                  b"2026-10-03T19:59:59+23:59", b"2026-10-03T00:00:00Z", b"2026-10-03T23:59:59Z", b"2026-10-03T23:58:10+05:30",
                  b"0", b"7", b"0001791071890000", b"1791071890000", b"9999999999999999", b"12345678", b"1234567x", b"1791071890000.0",
                  b"\xd9\xa2\xd9\xa5", b"86400000", b"/0000000", b":0000000", b"00000000000000000000001791071890000"] {
            for _ in 0..2 {
                assert_eq!(parse_ts_c(t, &mut dc), parse_ts(t), "{:?}", std::str::from_utf8(t));
            }
        }
    }
    #[test]
    fn fields() {
        let mut f = [&b""[..]; 3];
        assert!(split_fields(b"a,\"b\",", &mut f));
        assert_eq!(f, [&b"a"[..], b"b", b""]);
        assert!(!split_fields(b"a,b", &mut f));
        assert!(!split_fields(b"a,b\",c", &mut f));
        assert!(!split_fields(b"a,\"b\"\r,c", &mut f));
        assert!(split_fields(b"\"\",x,y", &mut f));
        assert!(!split_fields(b"\",x,y", &mut f));
    }
}
