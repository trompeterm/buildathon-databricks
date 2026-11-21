//! Shopping Sessions pipeline: a single native binary.
//!
//! Phase A (parallel): parse event CSV chunks, validate each row, hash-partition by event_id.
//!                     Concurrently, one thread parses links and builds the person map (union-find).
//! Phase B (parallel): parse click chunks into person partitions; dedup each event_id partition
//!                     (smallest ingest_ts wins), keep valid survivors, route them to person partitions.
//! Phase C (parallel): per person partition, sort by (person, ts), sessionize, attribute, aggregate,
//!                     and write user_daily Parquet; then merge campaign counters and write campaign_daily.

use memchr::memchr;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;
use xxhash_rust::xxh3::{xxh3_128, xxh3_64};

type FxMap<K, V> = std::collections::HashMap<K, V, rustc_hash::FxBuildHasher>;

const DAY: i64 = 86_400_000;
const GAP: u32 = 1_800_000;
const WEEK: i64 = 604_800_000;
const LATE: i64 = 86_400_000;

const P1_BITS: u32 = 8; // event_id partitions
const P1: usize = 1 << P1_BITS;
const Q_BITS: u32 = 8; // person partitions
const Q: usize = 1 << Q_BITS;
const CHUNK: usize = 4 << 20;

// ---------------------------------------------------------------------------------------------
// Global file table: every input file is mmapped once and lives for the whole run, so a field can
// be referenced by a packed u64 (file, offset, length) and resolved anywhere.

static FILES: OnceLock<Vec<&'static [u8]>> = OnceLock::new();

const OFF_BITS: u32 = 38;
const LEN_BITS: u32 = 12;

#[inline(always)]
fn mkref(file: usize, off: usize, len: usize) -> u64 {
    debug_assert!(len < (1 << LEN_BITS));
    ((file as u64) << (OFF_BITS + LEN_BITS)) | ((len as u64) << OFF_BITS) | off as u64
}

#[inline(always)]
fn deref(r: u64) -> &'static [u8] {
    let file = (r >> (OFF_BITS + LEN_BITS)) as usize;
    let len = ((r >> OFF_BITS) & ((1 << LEN_BITS) - 1)) as usize;
    let off = (r & ((1 << OFF_BITS) - 1)) as usize;
    &FILES.get().unwrap()[file][off..off + len]
}

#[inline(always)]
fn field_ref(file: usize, base: &[u8], f: &[u8]) -> u64 {
    let off = f.as_ptr() as usize - base.as_ptr() as usize;
    mkref(file, off, f.len().min((1 << LEN_BITS) - 1))
}

/// Hash of a user ID string. Never 0 (0 marks an empty slot in the open-addressing maps).
#[inline(always)]
fn uh(b: &[u8]) -> u64 {
    let h = xxh3_64(b);
    if h == 0 {
        1
    } else {
        h
    }
}

fn map_file(p: &Path) -> &'static [u8] {
    let f = File::open(p).unwrap_or_else(|e| panic!("open {}: {e}", p.display()));
    let len = f.metadata().map(|m| m.len()).unwrap_or(0);
    if len == 0 {
        return &[];
    }
    let m = unsafe { memmap2::Mmap::map(&f) }.unwrap_or_else(|e| panic!("mmap {}: {e}", p.display()));
    #[cfg(unix)]
    {
        let _ = m.advise(memmap2::Advice::WillNeed);
    }
    let m: &'static memmap2::Mmap = Box::leak(Box::new(m));
    &m[..]
}

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|x| x == "csv").unwrap_or(false) && p.is_file())
            .collect(),
        Err(_) => vec![],
    };
    v.sort();
    v
}

// ---------------------------------------------------------------------------------------------
// CSV dialect

/// Split one line (without its '\n') into exactly N fields, unquoting `"..."` fields.
/// Returns false for a malformed line (wrong field count, stray quote, empty line).
#[inline(always)]
fn split<'a, const N: usize>(line: &'a [u8], out: &mut [&'a [u8]; N]) -> bool {
    let mut n = 0usize;
    let mut fs = 0usize;
    let mut quotes = 0u32;
    let len = line.len();
    let mut i = 0usize;
    while i <= len {
        let c = if i < len { unsafe { *line.get_unchecked(i) } } else { b',' };
        if c == b',' {
            if n >= N {
                return false;
            }
            let f = &line[fs..i];
            if quotes == 0 {
                out[n] = f;
            } else if quotes == 2 && f.len() >= 2 && f[0] == b'"' && f[f.len() - 1] == b'"' {
                out[n] = &f[1..f.len() - 1];
            } else {
                return false;
            }
            n += 1;
            fs = i + 1;
            quotes = 0;
        } else if c == b'"' {
            quotes += 1;
        }
        i += 1;
    }
    n == N
}

/// Call `f(line)` for every line whose first byte lies in [start, end). The file's first line
/// (the header) is skipped.
#[inline(always)]
fn for_lines<'a>(data: &'a [u8], start: usize, end: usize, mut f: impl FnMut(&'a [u8])) {
    let mut p = if start == 0 {
        match memchr(b'\n', data) {
            Some(i) => i + 1,
            None => return,
        }
    } else {
        match memchr(b'\n', &data[start - 1..]) {
            Some(i) => start + i,
            None => return,
        }
    };
    while p < end {
        let e = match memchr(b'\n', &data[p..]) {
            Some(i) => p + i,
            None => data.len(),
        };
        f(&data[p..e]);
        p = e + 1;
    }
}

#[derive(Clone, Copy)]
struct Chunk {
    file: usize,
    start: usize,
    end: usize,
}

fn make_chunks(files: &[usize]) -> Vec<Chunk> {
    let all = FILES.get().unwrap();
    let mut v = Vec::new();
    for &fi in files {
        let len = all[fi].len();
        let mut s = 0;
        while s < len {
            let e = (s + CHUNK).min(len);
            v.push(Chunk { file: fi, start: s, end: e });
            s = e;
        }
    }
    // Biggest chunks first is irrelevant (all ~equal); keep file order for locality.
    v
}

// ---------------------------------------------------------------------------------------------
// Timestamps

#[inline(always)]
fn d2(s: &[u8], i: usize) -> Option<i64> {
    let a = s[i].wrapping_sub(b'0');
    let b = s[i + 1].wrapping_sub(b'0');
    if a < 10 && b < 10 {
        Some((a as i64) * 10 + b as i64)
    } else {
        None
    }
}

#[inline(always)]
fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

#[inline(always)]
fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if is_leap(y) {
                29
            } else {
                28
            }
        }
    }
}

/// Days since 1970-01-01 (Howard Hinnant's algorithm).
#[inline(always)]
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// Parse F1 / F2 / F3 into epoch ms. None for a bad timestamp. F3 values too long to matter are
/// returned as None too (they are outside every window anyway).
#[inline(always)]
fn parse_ts(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 0 {
        return None;
    }
    if n != 20 && n != 25 || s[4] != b'-' {
        // Only F3 can be valid now.
        let mut i = 0;
        while i < n && s[i] == b'0' {
            i += 1;
        }
        let mut v: i64 = 0;
        for &c in &s[i..] {
            let d = c.wrapping_sub(b'0');
            if d >= 10 {
                return None;
            }
            v = v * 10 + d as i64;
            if v > 1_000_000_000_000_000 {
                // Keep scanning for non-digits but avoid overflow: such a value is outside.
                for &c2 in &s[i..] {
                    if !c2.is_ascii_digit() {
                        return None;
                    }
                }
                return None;
            }
        }
        return Some(v);
    }
    if s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let y = d2(s, 0)? * 100 + d2(s, 2)?;
    let mo = d2(s, 5)?;
    let d = d2(s, 8)?;
    let h = d2(s, 11)?;
    let mi = d2(s, 14)?;
    let se = d2(s, 17)?;
    if !(1..=12).contains(&mo) || d < 1 || d > days_in_month(y, mo) || h > 23 || mi > 59 || se > 59 {
        return None;
    }
    let mut t = days_from_civil(y, mo, d) * 86400 + h * 3600 + mi * 60 + se;
    if n == 20 {
        if s[19] != b'Z' {
            return None;
        }
    } else {
        let sign = s[19];
        if (sign != b'+' && sign != b'-') || s[22] != b':' {
            return None;
        }
        let oh = d2(s, 20)?;
        let om = d2(s, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        let off = oh * 3600 + om * 60;
        if sign == b'+' {
            t -= off;
        } else {
            t += off;
        }
    }
    Some(t * 1000)
}

#[inline(always)]
fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in s {
        let d = c.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        v = v * 10 + d as u64;
        if v > 10_000_000 {
            // Still must be all digits to matter, but it is invalid either way.
            return None;
        }
    }
    if v >= 1 {
        Some(v as u32)
    } else {
        None
    }
}

// ---------------------------------------------------------------------------------------------
// Open-addressing map keyed by a non-zero u64 hash.

struct HMap<V: Copy + Default> {
    mask: usize,
    keys: Vec<u64>,
    vals: Vec<V>,
    len: usize,
}

impl<V: Copy + Default> HMap<V> {
    fn with_capacity(n: usize) -> Self {
        let cap = (n * 2).next_power_of_two().max(16);
        HMap { mask: cap - 1, keys: vec![0; cap], vals: vec![V::default(); cap], len: 0 }
    }
    #[inline(always)]
    fn get(&self, k: u64) -> Option<V> {
        let mut i = (k as usize) & self.mask;
        loop {
            let kk = unsafe { *self.keys.get_unchecked(i) };
            if kk == k {
                return Some(unsafe { *self.vals.get_unchecked(i) });
            }
            if kk == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
    fn grow(&mut self) {
        let mut n = HMap::with_capacity(self.keys.len());
        for i in 0..self.keys.len() {
            if self.keys[i] != 0 {
                n.insert(self.keys[i], self.vals[i]);
            }
        }
        *self = n;
    }
    /// Returns (slot, inserted).
    #[inline(always)]
    fn insert_or_get(&mut self, k: u64, v: V) -> V {
        if (self.len + 1) * 2 > self.keys.len() {
            self.grow();
        }
        let mut i = (k as usize) & self.mask;
        loop {
            let kk = self.keys[i];
            if kk == k {
                return self.vals[i];
            }
            if kk == 0 {
                self.keys[i] = k;
                self.vals[i] = v;
                self.len += 1;
                return v;
            }
            i = (i + 1) & self.mask;
        }
    }
    fn insert(&mut self, k: u64, v: V) {
        if (self.len + 1) * 2 > self.keys.len() {
            self.grow();
        }
        let mut i = (k as usize) & self.mask;
        loop {
            let kk = self.keys[i];
            if kk == k {
                self.vals[i] = v;
                return;
            }
            if kk == 0 {
                self.keys[i] = k;
                self.vals[i] = v;
                self.len += 1;
                return;
            }
            i = (i + 1) & self.mask;
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Records

#[derive(Clone, Copy)]
struct ERec {
    e0: u64,
    e1: u64,
    ingest: i64,
    user: u64,
    uref: u64,
    ts: u32,
    price: u32,
    cat: u16,
    kind: u8, // 0 invalid, 1 view/cart, 2 purchase
}

#[derive(Clone, Copy)]
struct SRec {
    person: u64,
    name: u64,
    ts: u32,
    price: u32,
    cat: u16,
    purch: u8,
}

#[derive(Clone, Copy)]
struct CRec {
    person: u64,
    ts: i64,
    cref: u64,
    camp: u32,
}

#[derive(Clone, Copy)]
struct URow {
    name: u64,
    day: i32,
    top: i32, // -1 null
    sessions: u32,
    events: u32,
    revenue: u64,
}

struct Ctx {
    ws: i64,
    we: i64,
    ws_day: i64,
    ndays: usize,
    products: FxMap<&'static [u8], u16>,
    unknown_cat: u16,
    cat_names: Vec<Vec<u8>>,
    cat_order: Vec<u16>, // category indexes sorted by name bytes
    campaigns: FxMap<&'static [u8], u32>,
    camp_names: Vec<Vec<u8>>,
    camp_channels: Vec<Vec<u8>>,
    ncamp: usize, // organic = ncamp
}

#[inline(always)]
fn ppart(person: u64) -> usize {
    (person >> (64 - Q_BITS)) as usize
}

// ---------------------------------------------------------------------------------------------

fn parse_events_chunk(ctx: &Ctx, ch: Chunk, out: &mut [Vec<ERec>]) {
    let base = FILES.get().unwrap()[ch.file];
    let ws = ctx.ws;
    let we = ctx.we;
    let mut f: [&[u8]; 7] = [&[]; 7];
    for_lines(base, ch.start, ch.end, |line| {
        if !split(line, &mut f) {
            return;
        }
        let eid = f[0];
        if eid.is_empty() {
            return;
        }
        let ingest = parse_ts(f[3]).unwrap_or(i64::MAX);
        let h = xxh3_128(eid);
        let e0 = h as u64;
        let e1 = (h >> 64) as u64;
        let mut r = ERec { e0, e1, ingest, user: 0, uref: 0, ts: 0, price: 0, cat: 0, kind: 0 };
        let uid = f[1];
        if !uid.is_empty() && !f[5].is_empty() {
            if let Some(ts) = parse_ts(f[2]) {
                if ts >= ws && ts < we && ingest.saturating_sub(ts) <= LATE {
                    let et = f[4];
                    let kind = if et == b"view" || et == b"cart" {
                        1
                    } else if et == b"purchase" {
                        match parse_price(f[6]) {
                            Some(p) => {
                                r.price = p;
                                r.cat = *ctx.products.get(f[5]).unwrap_or(&ctx.unknown_cat);
                                2
                            }
                            None => 0,
                        }
                    } else {
                        0
                    };
                    if kind != 0 {
                        r.kind = kind;
                        r.ts = (ts - ws) as u32;
                        r.user = uh(uid);
                        r.uref = field_ref(ch.file, base, uid);
                    }
                }
            }
        }
        out[(e1 >> (64 - P1_BITS)) as usize].push(r);
    });
}

fn parse_clicks_chunk(ctx: &Ctx, pm: &HMap<(u64, u64)>, ch: Chunk, out: &mut [Vec<CRec>]) {
    let base = FILES.get().unwrap()[ch.file];
    let lo = ctx.ws - WEEK;
    let hi = ctx.we;
    let mut f: [&[u8]; 4] = [&[]; 4];
    for_lines(base, ch.start, ch.end, |line| {
        if !split(line, &mut f) {
            return;
        }
        if f[0].is_empty() || f[1].is_empty() {
            return;
        }
        let camp = match ctx.campaigns.get(f[2]) {
            Some(&c) => c,
            None => return,
        };
        let ts = match parse_ts(f[3]) {
            Some(t) if t >= lo && t < hi => t,
            _ => return,
        };
        let u = uh(f[1]);
        let person = match pm.get(u) {
            Some((p, _)) => p,
            None => u,
        };
        out[ppart(person)].push(CRec { person, ts, cref: field_ref(ch.file, base, f[0]), camp });
    });
}

/// Parse links, union IDs, name each person by its byte-wise smallest ID.
/// Returns user hash -> (person hash, name ref) for every ID that appears in a kept link line.
fn build_links(link_files: &[usize]) -> HMap<(u64, u64)> {
    let all = FILES.get().unwrap();
    let mut pairs: Vec<(u64, u64, u64, u64)> = Vec::new();
    for &fi in link_files {
        let base = all[fi];
        let mut f: [&[u8]; 2] = [&[]; 2];
        for_lines(base, 0, base.len(), |line| {
            if !split(line, &mut f) {
                return;
            }
            if f[0].is_empty() || f[1].is_empty() {
                return;
            }
            pairs.push((uh(f[0]), uh(f[1]), field_ref(fi, base, f[0]), field_ref(fi, base, f[1])));
        });
    }
    let mut dense: HMap<u32> = HMap::with_capacity(pairs.len() * 2 + 16);
    let mut hashes: Vec<u64> = Vec::with_capacity(pairs.len() * 2);
    let mut refs: Vec<u64> = Vec::with_capacity(pairs.len() * 2);
    let mut parent: Vec<u32> = Vec::with_capacity(pairs.len() * 2);

    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let p = parent[x as usize];
            parent[x as usize] = parent[p as usize];
            x = p;
        }
        x
    }
    for &(ha, hb, ra, rb) in &pairs {
        let n = hashes.len() as u32;
        let ia = dense.insert_or_get(ha, n);
        if ia == n {
            hashes.push(ha);
            refs.push(ra);
            parent.push(n);
        }
        let n = hashes.len() as u32;
        let ib = dense.insert_or_get(hb, n);
        if ib == n {
            hashes.push(hb);
            refs.push(rb);
            parent.push(n);
        }
        let a = find(&mut parent, ia);
        let b = find(&mut parent, ib);
        if a != b {
            if a < b {
                parent[b as usize] = a;
            } else {
                parent[a as usize] = b;
            }
        }
    }
    let n = hashes.len();
    let mut minid: Vec<u32> = (0..n as u32).collect();
    for i in 0..n {
        let r = find(&mut parent, i as u32) as usize;
        let m = minid[r] as usize;
        if deref(refs[i]) < deref(refs[m]) {
            minid[r] = i as u32;
        }
    }
    let mut pm: HMap<(u64, u64)> = HMap::with_capacity(n + 16);
    for i in 0..n {
        let r = find(&mut parent, i as u32) as usize;
        let m = minid[r] as usize;
        pm.insert(hashes[i], (hashes[m], refs[m]));
    }
    pm
}

fn load_small(path: &Path) -> &'static [u8] {
    let v = std::fs::read(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    Box::leak(v.into_boxed_slice())
}

fn manifest_value(text: &str, key: &str) -> String {
    let k = format!("\"{key}\"");
    let i = text.find(&k).unwrap_or_else(|| panic!("manifest: no {key}")) + k.len();
    let rest = &text[i..];
    let c = rest.find(':').unwrap() + 1;
    let rest = &rest[c..];
    let q1 = rest.find('"').unwrap() + 1;
    let rest = &rest[q1..];
    let q2 = rest.find('"').unwrap();
    rest[..q2].to_string()
}

// ---------------------------------------------------------------------------------------------
// Phase C: one person at a time.

struct Agg {
    cd: Vec<[u64; 4]>, // (campaign, day) -> sessions, purchasing, revenue, assisted
    counts: Vec<u32>,
    pos: Vec<u32>,
    active: Vec<u32>,
    catsum: Vec<u64>,
    rows: Vec<URow>,
}

impl Agg {
    #[inline(always)]
    fn add(&mut self, c: u32) {
        let ci = c as usize;
        if self.counts[ci] == 0 {
            self.pos[ci] = self.active.len() as u32;
            self.active.push(c);
        }
        self.counts[ci] += 1;
    }
    #[inline(always)]
    fn remove(&mut self, c: u32) {
        let ci = c as usize;
        self.counts[ci] -= 1;
        if self.counts[ci] == 0 {
            let p = self.pos[ci] as usize;
            let last = self.active.pop().unwrap();
            if p < self.active.len() {
                self.active[p] = last;
                self.pos[last as usize] = p as u32;
            }
        }
    }
}

fn flush_day(ag: &mut Agg, ctx: &Ctx, name: u64, day: i64, s: u32, e: u32, rev: u64) {
    let mut top = -1i32;
    if rev > 0 {
        let mut best = 0u64;
        for &c in &ctx.cat_order {
            let v = ag.catsum[c as usize];
            if v > best {
                best = v;
                top = c as i32;
            }
        }
        for x in ag.catsum.iter_mut() {
            *x = 0;
        }
    }
    ag.rows.push(URow { name, day: (ctx.ws_day + day) as i32, top, sessions: s, events: e, revenue: rev });
}

fn process_person(ctx: &Ctx, ev: &[SRec], cl: &[CRec], ag: &mut Agg) {
    let ndays = ctx.ndays;
    let organic = ctx.ncamp as u32;
    let name = ev[0].name;
    let mut lo = 0usize;
    let mut hi = 0usize;
    let mut cur_day = i64::MIN;
    let (mut ds, mut de, mut dr) = (0u32, 0u32, 0u64);
    let n = ev.len();
    let mut i = 0usize;
    while i < n {
        let start = ev[i].ts;
        let mut last = start;
        let mut j = i + 1;
        while j < n && ev[j].ts - last <= GAP {
            last = ev[j].ts;
            j += 1;
        }
        let s_abs = ctx.ws + start as i64;
        let day = s_abs.div_euclid(DAY) - ctx.ws_day;
        if day != cur_day {
            if cur_day != i64::MIN {
                flush_day(ag, ctx, name, cur_day, ds, de, dr);
            }
            cur_day = day;
            ds = 0;
            de = 0;
            dr = 0;
        }
        let mut rev = 0u64;
        let mut purch = false;
        for r in &ev[i..j] {
            if r.purch != 0 {
                purch = true;
                rev += r.price as u64;
                ag.catsum[r.cat as usize] += r.price as u64;
            }
        }
        ds += 1;
        de += (j - i) as u32;
        dr += rev;

        let d = day as usize;
        let camp = if cl.is_empty() {
            organic
        } else {
            while hi < cl.len() && cl[hi].ts <= s_abs {
                ag.add(cl[hi].camp);
                hi += 1;
            }
            let lb = s_abs - WEEK;
            while lo < hi && cl[lo].ts < lb {
                ag.remove(cl[lo].camp);
                lo += 1;
            }
            if hi > lo {
                let t = cl[hi - 1].ts;
                let mut best = hi - 1;
                let mut m = hi - 1;
                while m > lo && cl[m - 1].ts == t {
                    m -= 1;
                    if deref(cl[m].cref) < deref(cl[best].cref) {
                        best = m;
                    }
                }
                for k in 0..ag.active.len() {
                    let c = ag.active[k] as usize;
                    ag.cd[c * ndays + d][3] += 1;
                }
                cl[best].camp
            } else {
                organic
            }
        };
        let e = &mut ag.cd[camp as usize * ndays + d];
        e[0] += 1;
        if purch {
            e[1] += 1;
        }
        e[2] += rev;
        i = j;
    }
    flush_day(ag, ctx, name, cur_day, ds, de, dr);
    for k in 0..ag.active.len() {
        let c = ag.active[k] as usize;
        ag.counts[c] = 0;
    }
    ag.active.clear();
}

// ---------------------------------------------------------------------------------------------
// Parquet output

use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use std::sync::Arc;

const ROW_GROUP: usize = 1 << 20;

fn write_user(path: &Path, rows: &[URow], ctx: &Ctx) -> parquet::errors::Result<()> {
    let schema = Arc::new(parse_message_type(
        "message user_daily {
            REQUIRED BYTE_ARRAY user_id (UTF8);
            REQUIRED INT32 day (DATE);
            REQUIRED INT64 sessions;
            REQUIRED INT64 events;
            REQUIRED INT64 revenue_cents;
            OPTIONAL BYTE_ARRAY top_category (UTF8);
        }",
    )?);
    let props = Arc::new(WriterProperties::builder().build());
    let file = std::io::BufWriter::with_capacity(1 << 20, File::create(path)?);
    let mut w = SerializedFileWriter::new(file, schema, props)?;
    let cats: Vec<ByteArray> = ctx.cat_names.iter().map(|c| ByteArray::from(c.clone())).collect();
    for chunk in rows.chunks(ROW_GROUP) {
        let mut rg = w.next_row_group()?;
        let mut idx = 0;
        while let Some(mut col) = rg.next_column()? {
            match idx {
                0 => {
                    let v: Vec<ByteArray> = chunk.iter().map(|r| ByteArray::from(deref(r.name).to_vec())).collect();
                    col.typed::<ByteArrayType>().write_batch(&v, None, None)?;
                }
                1 => {
                    let v: Vec<i32> = chunk.iter().map(|r| r.day).collect();
                    col.typed::<Int32Type>().write_batch(&v, None, None)?;
                }
                2 => {
                    let v: Vec<i64> = chunk.iter().map(|r| r.sessions as i64).collect();
                    col.typed::<Int64Type>().write_batch(&v, None, None)?;
                }
                3 => {
                    let v: Vec<i64> = chunk.iter().map(|r| r.events as i64).collect();
                    col.typed::<Int64Type>().write_batch(&v, None, None)?;
                }
                4 => {
                    let v: Vec<i64> = chunk.iter().map(|r| r.revenue as i64).collect();
                    col.typed::<Int64Type>().write_batch(&v, None, None)?;
                }
                _ => {
                    let mut v = Vec::new();
                    let mut def = Vec::with_capacity(chunk.len());
                    for r in chunk {
                        if r.top >= 0 {
                            v.push(cats[r.top as usize].clone());
                            def.push(1i16);
                        } else {
                            def.push(0i16);
                        }
                    }
                    col.typed::<ByteArrayType>().write_batch(&v, Some(&def), None)?;
                }
            }
            col.close()?;
            idx += 1;
        }
        rg.close()?;
    }
    w.close()?;
    Ok(())
}

fn write_campaign(path: &Path, cd: &[[u64; 4]], ctx: &Ctx) -> parquet::errors::Result<()> {
    let schema = Arc::new(parse_message_type(
        "message campaign_daily {
            REQUIRED BYTE_ARRAY campaign_id (UTF8);
            REQUIRED BYTE_ARRAY channel (UTF8);
            REQUIRED INT32 day (DATE);
            REQUIRED INT64 sessions;
            REQUIRED INT64 purchasing_sessions;
            REQUIRED INT64 revenue_cents;
            REQUIRED INT64 assisted_sessions;
        }",
    )?);
    let mut rows: Vec<(usize, usize)> = Vec::new();
    for c in 0..=ctx.ncamp {
        for d in 0..ctx.ndays {
            let e = &cd[c * ctx.ndays + d];
            if e[0] > 0 || e[3] > 0 {
                rows.push((c, d));
            }
        }
    }
    let props = Arc::new(WriterProperties::builder().build());
    let file = std::io::BufWriter::new(File::create(path)?);
    let mut w = SerializedFileWriter::new(file, schema, props)?;
    let mut rg = w.next_row_group()?;
    let mut idx = 0;
    while let Some(mut col) = rg.next_column()? {
        match idx {
            0 | 1 => {
                let src = if idx == 0 { &ctx.camp_names } else { &ctx.camp_channels };
                let v: Vec<ByteArray> = rows.iter().map(|&(c, _)| ByteArray::from(src[c].clone())).collect();
                col.typed::<ByteArrayType>().write_batch(&v, None, None)?;
            }
            2 => {
                let v: Vec<i32> = rows.iter().map(|&(_, d)| (ctx.ws_day + d as i64) as i32).collect();
                col.typed::<Int32Type>().write_batch(&v, None, None)?;
            }
            k => {
                let field = match k {
                    3 => 0,
                    4 => 1,
                    5 => 2,
                    _ => 3,
                };
                let v: Vec<i64> = rows.iter().map(|&(c, d)| cd[c * ctx.ndays + d][field] as i64).collect();
                col.typed::<Int64Type>().write_batch(&v, None, None)?;
            }
        }
        col.close()?;
        idx += 1;
    }
    rg.close()?;
    w.close()?;
    Ok(())
}

// ---------------------------------------------------------------------------------------------

fn main() {
    let t0 = Instant::now();
    let lap = |what: &str| eprintln!("[{:8.3}s] {what}", t0.elapsed().as_secs_f64());
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let outdir = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    let nthreads: usize = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8))
        .clamp(1, 256);

    // Manifest
    let man = std::fs::read_to_string(data.join("manifest.json")).expect("manifest.json");
    let ws = parse_ts(manifest_value(&man, "window_start").as_bytes()).expect("window_start");
    let we = parse_ts(manifest_value(&man, "window_end").as_bytes()).expect("window_end");
    let ws_day = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - ws_day + 1).max(1) as usize;

    // Inputs
    let ev_paths = list_csv(&data.join("events"));
    let cl_paths = list_csv(&data.join("clicks"));
    let ln_paths = list_csv(&data.join("links"));
    let mut all: Vec<&'static [u8]> = Vec::new();
    let idx = |paths: &[PathBuf], all: &mut Vec<&'static [u8]>| -> Vec<usize> {
        paths
            .iter()
            .map(|p| {
                all.push(map_file(p));
                all.len() - 1
            })
            .collect()
    };
    let ev_files = idx(&ev_paths, &mut all);
    let cl_files = idx(&cl_paths, &mut all);
    let ln_files = idx(&ln_paths, &mut all);
    let ev_bytes: usize = ev_files.iter().map(|&i| all[i].len()).sum();
    FILES.set(all).ok().unwrap();

    // Products and campaigns
    let mut cat_idx: FxMap<&'static [u8], u16> = FxMap::default();
    let mut cat_names: Vec<Vec<u8>> = Vec::new();
    let mut products: FxMap<&'static [u8], u16> = FxMap::default();
    {
        let buf = load_small(&data.join("products.csv"));
        let mut f: [&[u8]; 2] = [&[]; 2];
        for_lines(buf, 0, buf.len(), |line| {
            if !split(line, &mut f) {
                return;
            }
            let c = *cat_idx.entry(f[1]).or_insert_with(|| {
                cat_names.push(f[1].to_vec());
                (cat_names.len() - 1) as u16
            });
            products.insert(f[0], c);
        });
    }
    let unknown_cat = cat_names.len() as u16;
    cat_names.push(b"unknown".to_vec());
    let mut cat_order: Vec<u16> = (0..cat_names.len() as u16).collect();
    cat_order.sort_by(|&a, &b| cat_names[a as usize].cmp(&cat_names[b as usize]));

    let mut campaigns: FxMap<&'static [u8], u32> = FxMap::default();
    let mut camp_names: Vec<Vec<u8>> = Vec::new();
    let mut camp_channels: Vec<Vec<u8>> = Vec::new();
    {
        let buf = load_small(&data.join("campaigns.csv"));
        let mut f: [&[u8]; 2] = [&[]; 2];
        for_lines(buf, 0, buf.len(), |line| {
            if !split(line, &mut f) || f[0].is_empty() {
                return;
            }
            if campaigns.contains_key(f[0]) {
                return;
            }
            campaigns.insert(f[0], camp_names.len() as u32);
            camp_names.push(f[0].to_vec());
            camp_channels.push(f[1].to_vec());
        });
    }
    let ncamp = camp_names.len();
    camp_names.push(b"organic".to_vec());
    camp_channels.push(b"organic".to_vec());

    let ctx = Ctx {
        ws,
        we,
        ws_day,
        ndays,
        products,
        unknown_cat,
        cat_names,
        cat_order,
        campaigns,
        camp_names,
        camp_channels,
        ncamp,
    };
    lap("setup");

    // ---------------- Phase A: events -> eid partitions; links in parallel
    let ev_chunks = make_chunks(&ev_files);
    let est = (ev_bytes / 72 / P1 / nthreads) * 23 / 20 + 16;
    let next = AtomicUsize::new(0);
    let (outs_a, pm) = std::thread::scope(|s| {
        let lh = s.spawn(|| build_links(&ln_files));
        let hs: Vec<_> = (0..nthreads)
            .map(|_| {
                s.spawn(|| {
                    let mut out: Vec<Vec<ERec>> = (0..P1).map(|_| Vec::with_capacity(est)).collect();
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= ev_chunks.len() {
                            break;
                        }
                        parse_events_chunk(&ctx, ev_chunks[i], &mut out);
                    }
                    out
                })
            })
            .collect();
        let outs: Vec<Vec<Vec<ERec>>> = hs.into_iter().map(|h| h.join().unwrap()).collect();
        let pm = lh.join().unwrap();
        (outs, pm)
    });
    lap("phase A: events parsed, links built");
    let mut parts_a: Vec<Mutex<Vec<Vec<ERec>>>> = (0..P1).map(|_| Mutex::new(Vec::new())).collect();
    for o in outs_a.into_iter() {
        for (p, v) in o.into_iter().enumerate() {
            parts_a[p].get_mut().unwrap().push(v);
        }
    }

    // ---------------- Phase B: clicks -> person partitions; dedup events -> person partitions
    let cl_chunks = make_chunks(&cl_files);
    let n_items = cl_chunks.len() + P1;
    let next = AtomicUsize::new(0);
    let outs_b: Vec<(Vec<Vec<SRec>>, Vec<Vec<CRec>>)> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nthreads)
            .map(|_| {
                s.spawn(|| {
                    let mut oev: Vec<Vec<SRec>> = (0..Q).map(|_| Vec::new()).collect();
                    let mut ocl: Vec<Vec<CRec>> = (0..Q).map(|_| Vec::new()).collect();
                    let mut table: Vec<u64> = Vec::new();
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= n_items {
                            break;
                        }
                        if i < cl_chunks.len() {
                            parse_clicks_chunk(&ctx, &pm, cl_chunks[i], &mut ocl);
                            continue;
                        }
                        let p = i - cl_chunks.len();
                        let vecs: Vec<Vec<ERec>> = std::mem::take(&mut *parts_a[p].lock().unwrap());
                        let n: usize = vecs.iter().map(|v| v.len()).sum();
                        let cap = (n * 2).next_power_of_two().max(16);
                        let mask = cap - 1;
                        table.clear();
                        table.resize(cap, 0);
                        for (w, v) in vecs.iter().enumerate() {
                            for (k, r) in v.iter().enumerate() {
                                let code = (((w as u64) << 32) | k as u64) + 1;
                                let mut slot = (r.e0 as usize) & mask;
                                loop {
                                    let t = unsafe { *table.get_unchecked(slot) };
                                    if t == 0 {
                                        table[slot] = code;
                                        break;
                                    }
                                    let o = &vecs[((t - 1) >> 32) as usize][((t - 1) & 0xffff_ffff) as usize];
                                    if o.e0 == r.e0 && o.e1 == r.e1 {
                                        if r.ingest < o.ingest {
                                            table[slot] = code;
                                        }
                                        break;
                                    }
                                    slot = (slot + 1) & mask;
                                }
                            }
                        }
                        for &t in table.iter() {
                            if t == 0 {
                                continue;
                            }
                            let r = &vecs[((t - 1) >> 32) as usize][((t - 1) & 0xffff_ffff) as usize];
                            if r.kind == 0 {
                                continue;
                            }
                            let (person, name) = match pm.get(r.user) {
                                Some(x) => x,
                                None => (r.user, r.uref),
                            };
                            oev[ppart(person)].push(SRec {
                                person,
                                name,
                                ts: r.ts,
                                price: r.price,
                                cat: r.cat,
                                purch: (r.kind == 2) as u8,
                            });
                        }
                        drop(vecs);
                    }
                    (oev, ocl)
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    drop(parts_a);
    lap("phase B: clicks parsed, events deduped");
    let mut parts_b: Vec<Mutex<(Vec<Vec<SRec>>, Vec<Vec<CRec>>)>> =
        (0..Q).map(|_| Mutex::new((Vec::new(), Vec::new()))).collect();
    for (oev, ocl) in outs_b.into_iter() {
        for (q, v) in oev.into_iter().enumerate() {
            parts_b[q].get_mut().unwrap().0.push(v);
        }
        for (q, v) in ocl.into_iter().enumerate() {
            parts_b[q].get_mut().unwrap().1.push(v);
        }
    }

    // ---------------- Phase C: sessionize, attribute, aggregate, write user_daily
    let ud = outdir.join("user_daily");
    let cdir = outdir.join("campaign_daily");
    std::fs::create_dir_all(&ud).expect("mkdir user_daily");
    std::fs::create_dir_all(&cdir).expect("mkdir campaign_daily");
    let next = AtomicUsize::new(0);
    let ncd = (ncamp + 1) * ndays;
    let ncat = ctx.cat_names.len();
    let cds: Vec<Vec<[u64; 4]>> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nthreads)
            .map(|w| {
                let ctx = &ctx;
                let parts_b = &parts_b;
                let next = &next;
                let ud = &ud;
                s.spawn(move || {
                    let mut ag = Agg {
                        cd: vec![[0u64; 4]; ncd],
                        counts: vec![0; ncamp + 1],
                        pos: vec![0; ncamp + 1],
                        active: Vec::new(),
                        catsum: vec![0; ncat],
                        rows: Vec::new(),
                    };
                    let mut ev: Vec<SRec> = Vec::new();
                    let mut cl: Vec<CRec> = Vec::new();
                    loop {
                        let q = next.fetch_add(1, Ordering::Relaxed);
                        if q >= Q {
                            break;
                        }
                        let (evs, cls) = std::mem::take(&mut *parts_b[q].lock().unwrap());
                        ev.clear();
                        for v in evs {
                            ev.extend_from_slice(&v);
                        }
                        cl.clear();
                        for v in cls {
                            cl.extend_from_slice(&v);
                        }
                        ev.sort_unstable_by(|a, b| a.person.cmp(&b.person).then(a.ts.cmp(&b.ts)));
                        cl.sort_unstable_by(|a, b| a.person.cmp(&b.person).then(a.ts.cmp(&b.ts)));
                        let mut j = 0usize;
                        let mut a = 0usize;
                        while a < ev.len() {
                            let p = ev[a].person;
                            let mut b = a + 1;
                            while b < ev.len() && ev[b].person == p {
                                b += 1;
                            }
                            while j < cl.len() && cl[j].person < p {
                                j += 1;
                            }
                            let mut k = j;
                            while k < cl.len() && cl[k].person == p {
                                k += 1;
                            }
                            process_person(ctx, &ev[a..b], &cl[j..k], &mut ag);
                            j = k;
                            a = b;
                        }
                    }
                    if w == 0 || !ag.rows.is_empty() {
                        write_user(&ud.join(format!("part-{w:05}.parquet")), &ag.rows, ctx).expect("write user_daily");
                    }
                    ag.cd
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    lap("phase C: sessions + user_daily written");

    let mut cd = vec![[0u64; 4]; ncd];
    for c in &cds {
        for (a, b) in cd.iter_mut().zip(c.iter()) {
            for k in 0..4 {
                a[k] += b[k];
            }
        }
    }
    write_campaign(&cdir.join("part-00000.parquet"), &cd, &ctx).expect("write campaign_daily");
    lap("done");
    // Skip freeing the big heaps.
    std::process::exit(0);
}
