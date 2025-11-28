// src/main.rs
use std::fs::File;
use std::io;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use ahash::AHashMap;
use dashmap::DashMap;
use memchr::memchr;
use memmap2::Mmap;
use rayon::prelude::*;

#[derive(Clone, Copy, Default)]
struct Stat {
    min: i32,   // tenths
    max: i32,   // tenths
    sum: i64,   // sum of tenths
    count: i64, // number of values
}

// Interner: assigns a compact u32 ID to each unique city string.
// Stores names in a Vec<String> and exposes a concurrent map for lookup.
// We only allocate a String once per 
// unique city.
struct Interner {
    next_id: AtomicU32,
    name_by_id: parking_lot::RwLock<Vec<String>>,
    id_by_name: DashMap<String, u32>, // concurrent map
}

impl Interner {
    fn new(cap_names: usize) -> Self {
        Self {
            next_id: AtomicU32::new(0),
            name_by_id: parking_lot::RwLock::new(Vec::with_capacity(cap_names)),
            id_by_name: DashMap::with_capacity(cap_names),
        }
    }

    // Get or add returns a compact ID for city bytes, allocating a String only on first sight.
    fn get_or_add(&self, city_bytes: &[u8]) -> u32 {
        // Fast path: try to find without allocating a new String
        if let Ok(city_str) = std::str::from_utf8(city_bytes) {
            if let Some(id) = self.id_by_name.get(city_str) {
                return *id;
            }
        }

        // Slow path: allocate a String once and insert
        let s = unsafe { String::from_utf8_unchecked(city_bytes.to_vec()) };
        if let Some(id) = self.id_by_name.get(&s) {
            return *id;
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        {
            let mut names = self.name_by_id.write();
            if id as usize >= names.len() {
                names.push(s.clone());
            } else {
                names[id as usize] = s.clone();
            }
        }
        self.id_by_name.insert(s, id);
        id
    }

    fn names(&self) -> Vec<String> {
        self.name_by_id.read().clone()
    }
}

fn main() -> io::Result<()> {
    // --- mmap the file ---
    let path = "/Users/andreas/git/1brc/data/measurements.txt";
    let f = File::open(path)?;
    let mmap = unsafe { Mmap::map(&f)? };
    let data = &mmap[..];

    if data.is_empty() {
        return Ok(());
    }

    // --- set up interner ---
    // Heuristic capacity: number of unique cities is usually small vs. lines.
    let interner = Arc::new(Interner::new(1 << 15));

    // --- chunk the file across threads (rayon) ---
    let n_threads = rayon::current_num_threads();
    let chunk_size = (data.len() / n_threads).max(1);

    // Prepare chunk boundaries aligned to newlines
    let mut ranges = Vec::with_capacity(n_threads);
    let mut start = 0usize;
    for i in 0..n_threads {
        let mut end = if i == n_threads - 1 {
            data.len()
        } else {
            ((i + 1) * chunk_size).min(data.len())
        };
        // Move end forward to the next newline to keep lines intact
        if end < data.len() {
            if let Some(nl) = memchr(b'\n', &data[end..]) {
                end += nl + 1;
            } else {
                end = data.len();
            }
        }
        // Move start to the next newline after previous boundary (except first chunk)
        if i > 0 && start < end {
            if let Some(nl) = memchr(b'\n', &data[start..end]) {
                start += nl + 1;
            }
        }
        ranges.push((start, end));
        start = end;
    }

    // --- per-thread aggregation (ID -> Stat) ---
    let locals: Vec<AHashMap<u32, Stat>> = ranges
        .par_iter()
        .map(|&(s, e)| {
            let mut local: AHashMap<u32, Stat> = AHashMap::with_capacity(8192);
            parse_chunk_ids(&data[s..e], &interner, &mut local);
            local
        })
        .collect();

    // --- merge locals into a dense Vec<Stat> indexed by city ID ---
    let max_id = interner.next_id.load(Ordering::Relaxed) as usize;
    let mut global: Vec<Stat> = vec![Stat::default(); max_id + 1];
    for local in locals {
        for (id, st) in local {
            let g = &mut global[id as usize];
            if g.count == 0 {
                *g = st;
            } else {
                if st.min < g.min {
                    g.min = st.min;
                }
                if st.max > g.max {
                    g.max = st.max;
                }
                g.sum += st.sum;
                g.count += st.count;
            }
        }
    }

    // --- output ---
    let names = interner.names();
    for (id, s) in global.iter().enumerate() {
        if s.count == 0 {
            continue;
        }
        let name = &names[id];
        let avg = (s.sum as f64) / (s.count as f64) / 10.0;
        println!(
            "{} => min: {:.1}, max: {:.1}, avg: {:.2}",
            name,
            (s.min as f64) / 10.0,
            (s.max as f64) / 10.0,
            avg
        );
    }

    Ok(())
}

// Parse a chunk, assign city IDs via interner, and aggregate in local map.
fn parse_chunk_ids(buf: &[u8], interner: &Interner, out: &mut AHashMap<u32, Stat>) {
    let mut i = 0;
    let n = buf.len();

    while i < n {
        let line_start = i;

        // Find ';' delimiter quickly
        let semi_rel = match memchr(b';', &buf[i..]) {
            Some(p) => p,
            None => break,
        };
        let semi = i + semi_rel;
        i = semi + 1;

        // Parse temperature after ';'
        let mut sign: i32 = 1;
        if i < n {
            match buf[i] {
                b'-' => {
                    sign = -1;
                    i += 1;
                }
                b'+' => {
                    i += 1;
                }
                _ => {}
            }
        }

        // Integer part
        let mut int_part: i32 = 0;
        while i < n {
            let c = buf[i];
            if c.is_ascii_digit() {
                int_part = int_part * 10 + (c - b'0') as i32;
                i += 1;
            } else {
                break;
            }
        }

        // Decimal part: expect '.' and one digit (treat missing as 0)
        if i < n && buf[i] == b'.' {
            i += 1;
        }
        let mut dec_digit: i32 = 0;
        if i < n {
            let c = buf[i];
            if c.is_ascii_digit() {
                dec_digit = (c - b'0') as i32;
                i += 1;
            }
        }

        // Advance to end of line
        if let Some(nl_rel) = memchr(b'\n', &buf[i..]) {
            i += nl_rel + 1;
        } else {
            i = n;
        }

        // City slice and ID
        let city_bytes = &buf[line_start..semi];
        let id = interner.get_or_add(city_bytes);

        // Temperature in tenths
        let tenth = sign * (int_part * 10 + dec_digit);

        // Aggregate
        match out.get_mut(&id) {
            Some(st) => {
                if tenth < st.min {
                    st.min = tenth;
                }
                if tenth > st.max {
                    st.max = tenth;
                }
                st.sum += tenth as i64;
                st.count += 1;
            }
            None => {
                out.insert(
                    id,
                    Stat {
                        min: tenth,
                        max: tenth,
                        sum: tenth as i64,
                        count: 1,
                    },
                );
            }
        }
    }
}
