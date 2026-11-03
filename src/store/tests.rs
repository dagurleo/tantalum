use std::collections::{HashMap, HashSet};

use super::*;

impl ShardInner {
    /// Recomputes everything from scratch and checks it matches the books.
    fn check(&self, hasher: &Hasher) {
        let mut mem = Mem {
            expired: self.mem.expired,
            evicted: self.mem.evicted,
            writes: self.mem.writes,
            ..Mem::default()
        };
        let mut live_locs = HashSet::new();
        let mut externals_seen = vec![false; self.externals.len()];
        for (id, seg) in self.segments.iter().enumerate() {
            let free = self.free_ids.contains(&(id as u32));
            let Some(seg) = seg else {
                assert!(free, "segment id {id} is neither used nor free");
                continue;
            };
            assert!(!free, "segment id {id} is both used and free");
            mem.segment_capacity += seg.capacity();
            mem.segment_resident += resident(seg.buf.len());
            mem.segment_used += seg.buf.len();
            let (mut live, mut ttl_live) = (0, 0);
            for (offset, header) in seg.entries() {
                if header.is_dead() {
                    continue;
                }
                live += header.len;
                if header.has_ttl() {
                    ttl_live += 1;
                    assert!(
                        seg.min_deadline != NO_EXPIRY && seg.min_deadline <= header.deadline,
                        "segment {id} min_deadline {} is after a live deadline {}",
                        seg.min_deadline,
                        header.deadline
                    );
                }
                if let Body::External(index) = header.body {
                    let index = index as usize;
                    assert!(!externals_seen[index], "external {index} referenced twice");
                    externals_seen[index] = true;
                    mem.external += self.externals[index].as_ref().unwrap().allocation_size();
                }
                live_locs.insert(join(id, offset));
            }
            assert_eq!(seg.live, live, "segment {id} live bytes");
            assert_eq!(seg.ttl_live, ttl_live, "segment {id} TTL count");
            mem.live += live;
            mem.with_ttl += ttl_live;
            let sealed = self.sealed.contains(&(id as u32));
            if self.active == Some(id) {
                assert!(!sealed, "active segment {id} is also sealed");
            } else {
                assert!(sealed, "segment {id} is neither active nor sealed");
                assert!(
                    seg.live > 0,
                    "sealed segment {id} is empty and should be freed"
                );
            }
        }
        assert_eq!(mem, self.mem);
        for (index, kv) in self.externals.iter().enumerate() {
            assert_eq!(
                kv.is_some(),
                externals_seen[index],
                "external {index} leaked or dangling"
            );
            assert_eq!(kv.is_none(), self.free_externals.contains(&(index as u32)));
        }
        let mut slot_locs = HashSet::new();
        for slot in self.table.iter() {
            assert!(slot_locs.insert(slot.loc), "two slots point at one entry");
            assert!(
                live_locs.contains(&slot.loc),
                "slot points at a dead or missing entry"
            );
            let (id, offset) = split(slot.loc);
            let seg = self.segment(id);
            let header = segment::parse(&seg.buf, offset);
            let key = Self::entry_key(&seg.buf, &header, &self.externals);
            assert_eq!(slot_hash(hasher, key), slot.hash, "slot hash");
            assert_eq!(
                self.lookup(slot.hash, key).map(|(loc, _)| loc),
                Some(slot.loc)
            );
        }
        assert_eq!(
            slot_locs.len(),
            live_locs.len(),
            "live entry without a slot"
        );
    }
}

impl Store {
    /// Checks every shard, and that the global counter matches their reports.
    fn check(&self) {
        let mut flushed = 0i64;
        for shard in self.shards.iter() {
            let inner = shard.inner.lock();
            inner.check(&self.hasher);
            assert_eq!(
                inner.reported,
                inner.footprint(),
                "footprint changed without a sync"
            );
            assert_eq!(
                (
                    shard.table_base.load(Relaxed),
                    shard.table_buckets.load(Relaxed)
                ),
                inner.table.raw_parts(),
                "table moved without a sync"
            );
            flushed += inner.reported as i64 - inner.unflushed as i64;
        }
        assert_eq!(self.used.load(Relaxed), flushed);
    }
}

/// Deterministic xorshift for reproducible randomized tests.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

fn get(store: &Store, key: &[u8], now: Millis) -> Option<Vec<u8>> {
    store.get_with(key, now, |v| v.map(|v| v.as_bytes().to_vec()))
}

fn put(
    store: &Store,
    key: &[u8],
    value: &[u8],
    cond: Condition,
    expiry: Expiry,
    now: Millis,
) -> SetOutcome {
    store
        .set(key, value, cond, expiry, now, true)
        .expect("unlimited store")
}

fn set(store: &Store, key: &[u8], value: &[u8], now: Millis) -> bool {
    put(store, key, value, Condition::Always, Expiry::Never, now).applied
}

fn limited(maxmemory: usize, policy: EvictionPolicy) -> Store {
    Store::with_config(Config {
        shards: 16,
        maxmemory,
        policy,
    })
}

#[test]
fn set_get_overwrite() {
    let s = Store::new(4);
    assert_eq!(get(&s, b"k", 1), None);
    assert!(set(&s, b"k", b"v1", 1));
    assert_eq!(get(&s, b"k", 1).as_deref(), Some(&b"v1"[..]));
    let out = put(&s, b"k", b"v2", Condition::Always, Expiry::Never, 1);
    assert_eq!(out.previous.unwrap().as_bytes(), b"v1");
    assert_eq!(get(&s, b"k", 1).as_deref(), Some(&b"v2"[..]));
    assert!(set(&s, b"", b"", 1), "empty key and value are valid");
    assert_eq!(get(&s, b"", 1).as_deref(), Some(&b""[..]));
    assert_eq!(s.len(), 2);
    s.check();
}

#[test]
fn many_keys_across_shards() {
    let s = Store::new(16);
    for i in 0..50_000u32 {
        assert!(set(&s, &i.to_le_bytes(), &(i * 7).to_le_bytes(), 1));
    }
    for i in 0..50_000u32 {
        assert_eq!(
            get(&s, &i.to_le_bytes(), 1),
            Some((i * 7).to_le_bytes().to_vec())
        );
    }
    assert_eq!(s.len(), 50_000);
    s.check();
}

#[test]
fn nx_and_xx() {
    let s = Store::new(1);
    assert!(!put(&s, b"k", b"a", Condition::IfPresent, Expiry::Never, 1).applied);
    assert_eq!(get(&s, b"k", 1), None);
    assert!(put(&s, b"k", b"a", Condition::IfAbsent, Expiry::Never, 1).applied);
    let out = put(&s, b"k", b"b", Condition::IfAbsent, Expiry::Never, 1);
    assert!(!out.applied);
    assert_eq!(out.previous.unwrap().as_bytes(), b"a");
    assert!(put(&s, b"k", b"c", Condition::IfPresent, Expiry::Never, 1).applied);
    assert_eq!(get(&s, b"k", 1).as_deref(), Some(&b"c"[..]));
    s.check();
}

#[test]
fn previous_value_only_when_asked() {
    let s = Store::new(1);
    set(&s, b"k", b"a", 1);
    let out = s
        .set(b"k", b"b", Condition::Always, Expiry::Never, 1, false)
        .unwrap();
    assert!(out.applied && out.previous.is_none());
}

#[test]
fn expired_keys_count_as_absent() {
    let s = Store::new(1);
    put(&s, b"k", b"a", Condition::Always, Expiry::At(10), 1);
    assert_eq!(get(&s, b"k", 9).as_deref(), Some(&b"a"[..]));
    // NX succeeds and XX fails on an expired key, even before it's swept.
    assert!(!put(&s, b"k", b"b", Condition::IfPresent, Expiry::Never, 10).applied);
    let out = put(&s, b"k", b"b", Condition::IfAbsent, Expiry::Never, 10);
    assert!(out.applied && out.previous.is_none());
    assert_eq!(get(&s, b"k", 1_000).as_deref(), Some(&b"b"[..]));
    s.check();
}

#[test]
fn lazy_expiry_on_get() {
    let s = Store::new(1);
    put(&s, b"k", b"a", Condition::Always, Expiry::At(10), 1);
    assert_eq!(get(&s, b"k", 10), None);
    assert_eq!(s.len(), 0, "expired key is removed on access");
    assert_eq!(s.stats().expired_keys, 1);
    s.check();
}

#[test]
fn keepttl() {
    let s = Store::new(1);
    put(&s, b"k", b"a", Condition::Always, Expiry::At(10), 1);
    put(&s, b"k", b"b", Condition::Always, Expiry::Keep, 5);
    assert_eq!(get(&s, b"k", 9).as_deref(), Some(&b"b"[..]));
    assert_eq!(get(&s, b"k", 10), None);
    // KEEPTTL on an expired key starts fresh with no TTL.
    put(&s, b"j", b"a", Condition::Always, Expiry::At(10), 1);
    put(&s, b"j", b"b", Condition::Always, Expiry::Keep, 20);
    assert_eq!(get(&s, b"j", 1_000_000).as_deref(), Some(&b"b"[..]));
    s.check();
}

#[test]
fn plain_set_clears_ttl() {
    let s = Store::new(1);
    put(&s, b"k", b"a", Condition::Always, Expiry::At(10), 1);
    put(&s, b"k", b"b", Condition::Always, Expiry::Never, 2);
    assert_eq!(s.expire_cycle(100), 0);
    assert_eq!(get(&s, b"k", 100).as_deref(), Some(&b"b"[..]));
    s.check();
}

#[test]
fn far_deadlines_roundtrip() {
    let s = Store::new(1);
    put(
        &s,
        b"k",
        b"a",
        Condition::Always,
        Expiry::At(MAX_DEADLINE),
        1,
    );
    for _ in 0..10 {
        assert!(get(&s, b"k", 1).is_some()); // saturates the usage counter
    }
    put(&s, b"k", b"b", Condition::Always, Expiry::Keep, 2);
    let inner = s.shards[0].inner.lock();
    let slot = inner.table.iter().next().unwrap();
    let header = segment::parse(&inner.segment(split(slot.loc).0).buf, split(slot.loc).1);
    assert_eq!(header.deadline, MAX_DEADLINE);
    assert_eq!(header.usage(), MAX_USAGE);
}

#[test]
fn active_expiry() {
    let s = Store::new(4);
    for i in 0..1_000u32 {
        let deadline = if i % 2 == 0 {
            Expiry::At(100)
        } else {
            Expiry::Never
        };
        put(&s, &i.to_le_bytes(), b"v", Condition::Always, deadline, 1);
    }
    assert_eq!(s.expire_cycle(99), 0);
    assert_eq!(s.expire_cycle(100), 500);
    assert_eq!(s.len(), 500);
    assert_eq!(s.expire_cycle(1_000), 0);
    assert_eq!(s.stats().expired_keys, 500);
    s.check();
}

#[test]
fn expiry_sweep_skips_segments_with_nothing_due() {
    let s = Store::new(1);
    for i in 0..10_000u32 {
        put(
            &s,
            &i.to_le_bytes(),
            &[0; 50],
            Condition::Always,
            Expiry::At(1_000 + u64::from(i % 10)),
            1,
        );
    }
    // Nothing is due yet, so no segment needs scanning at all.
    assert_eq!(s.expire_cycle(999), 0);
    // Once everything is due, repeated cycles (each with a scan budget) get it all.
    let mut removed = 0;
    for _ in 0..1_000 {
        removed += s.expire_cycle(2_000);
        if removed == 10_000 {
            break;
        }
    }
    assert_eq!(removed, 10_000);
    let stats = s.stats();
    assert_eq!(stats.keys, 0);
    assert_eq!(stats.segment_bytes, 0, "every segment was freed");
    s.check();
}

#[test]
fn large_values_can_be_shared() {
    let s = Store::new(1);
    let big = vec![7u8; COPY_UNDER_LOCK_MAX * 4];
    set(&s, b"big", &big, 1);
    let shared = s.get_with(b"big", 1, |v| v.unwrap().share());
    set(&s, b"big", b"small", 1);
    assert_eq!(
        shared.as_bytes(),
        &big[..],
        "shared value outlives an overwrite"
    );
    assert_eq!(get(&s, b"big", 1).as_deref(), Some(&b"small"[..]));
    s.check();
}

#[test]
fn small_entries_are_dense() {
    let s = Store::new(16);
    let keys = 100_000;
    for i in 0..keys {
        set(&s, format!("key:{i:06}").as_bytes(), &[b'v'; 32], 1);
    }
    s.check();
    let stats = s.stats();
    let per_key = (stats.segment_bytes + stats.table_bytes) as f64 / keys as f64;
    // 10-byte key + 32-byte value = 42 bytes of data; the old design used ~90.
    assert!(per_key < 70.0, "{per_key:.1} bytes per key: {stats:?}");
    assert_eq!(stats.external_bytes, 0);
}

#[test]
fn overwrites_reclaim_dead_space() {
    let s = Store::new(4);
    for round in 0..200u32 {
        for i in 0..1_000u32 {
            set(&s, &i.to_le_bytes(), &[round as u8; 40], 1);
        }
    }
    s.check();
    let stats = s.stats();
    // ~46 bytes per entry live; dead space is kept under a quarter (plus
    // active segments), not 200 rounds' worth.
    assert!(
        stats.segment_bytes < stats.live_bytes * 2,
        "segments {} for {} live bytes",
        stats.segment_bytes,
        stats.live_bytes
    );
    for i in 0..1_000u32 {
        assert_eq!(get(&s, &i.to_le_bytes(), 1), Some(vec![199; 40]));
    }
}

#[test]
fn memory_returns_to_baseline() {
    let s = Store::new(8);
    for i in 0..10_000u32 {
        put(
            &s,
            &i.to_le_bytes(),
            &[1; 100],
            Condition::Always,
            Expiry::At(50),
            1,
        );
    }
    assert!(s.used_memory() > 10_000 * 100);
    while s.expire_cycle(50) > 0 {}
    s.check();
    assert_eq!(s.stats().keys, 0);
    // Every segment and table was freed; only bookkeeping vectors remain
    // (a few KB per shard).
    let stats = s.stats();
    assert_eq!((stats.segment_bytes, stats.table_bytes), (0, 0));
    assert!(stats.used_memory <= 8 * 4096, "{stats:?}");
}

/// Random operations against a plain map that knows the right answers.
#[test]
fn matches_a_model() {
    let s = Store::new(4);
    let mut model: HashMap<Vec<u8>, (Vec<u8>, Millis)> = HashMap::new();
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let mut now: Millis = 1;
    for step in 0..60_000u64 {
        let key = format!("k{}", rng.below(300)).into_bytes();
        let live = model
            .get(&key)
            .filter(|(_, d)| *d == 0 || *d > now)
            .cloned();
        match rng.below(100) {
            0..30 => {
                let len = match rng.below(10) {
                    0 => 0,
                    1..7 => rng.below(64),
                    7..9 => 100 + rng.below(400),
                    _ => 3_000 + rng.below(6_000), // some are big enough to be external
                };
                let value: Vec<u8> = (0..len).map(|i| (i ^ step) as u8).collect();
                let expiry = match rng.below(4) {
                    0 => Expiry::At(now + 1 + rng.below(50)),
                    1 => Expiry::Keep,
                    _ => Expiry::Never,
                };
                let cond = match rng.below(10) {
                    0 => Condition::IfAbsent,
                    1 => Condition::IfPresent,
                    _ => Condition::Always,
                };
                let out = s.set(&key, &value, cond, expiry, now, true).unwrap();
                let applies = match cond {
                    Condition::Always => true,
                    Condition::IfAbsent => live.is_none(),
                    Condition::IfPresent => live.is_some(),
                };
                assert_eq!(out.applied, applies, "step {step}");
                assert_eq!(
                    out.previous.map(|p| p.as_bytes().to_vec()),
                    live.as_ref().map(|(v, _)| v.clone()),
                    "step {step}"
                );
                if applies {
                    let deadline = match expiry {
                        Expiry::Never => 0,
                        Expiry::At(t) => t,
                        Expiry::Keep => live.map_or(0, |(_, d)| d),
                    };
                    model.insert(key, (value, deadline));
                }
            }
            30..60 => assert_eq!(get(&s, &key, now), live.map(|(v, _)| v), "step {step}"),
            60..85 => {
                // An update, after checking update_key shows the key as the model has it.
                let update = match rng.below(6) {
                    0 => Update::Delete,
                    1 => Update::Expire(Expiry::At(now + 1 + rng.below(50))),
                    2 => Update::Expire(Expiry::At(now.saturating_sub(rng.below(3)).max(1))),
                    3 => Update::Expire(Expiry::Never),
                    4 => Update::SetInt(rng.below(2_000) as i64 - 1_000),
                    _ => Update::Touch,
                };
                let shown = s
                    .update_key(s.key(&key), now, |current| {
                        let shown = current.map(|c| (c.value.as_bytes().to_vec(), c.expiry));
                        (update, shown)
                    })
                    .unwrap();
                let expected = live.as_ref().map(|(v, d)| {
                    let expiry = if *d == 0 {
                        Expiry::Never
                    } else {
                        Expiry::At(*d)
                    };
                    (v.clone(), expiry)
                });
                assert_eq!(shown, expected, "step {step}");
                match (update, live) {
                    (Update::SetInt(n), live) => {
                        let deadline = live.map_or(0, |(_, d)| d);
                        model.insert(key, (n.to_string().into_bytes(), deadline));
                    }
                    (_, None) | (Update::Touch, _) => {}
                    (Update::Delete, _) => {
                        model.remove(&key);
                    }
                    (Update::Expire(Expiry::At(d)), _) if d <= now => {
                        model.remove(&key);
                    }
                    (Update::Expire(expiry), Some((value, _))) => {
                        let deadline = match expiry {
                            Expiry::At(d) => d,
                            _ => 0,
                        };
                        model.insert(key, (value, deadline));
                    }
                    (Update::Peek, _) => unreachable!(),
                }
            }
            85..95 => now += rng.below(20),
            _ => {
                s.expire_cycle(now);
            }
        }
        if step % 5_000 == 0 {
            s.check();
        }
    }
    s.check();
}

/// Under eviction, keys may vanish but must never come back with stale values.
#[test]
fn eviction_never_returns_wrong_values() {
    let s = Store::with_config(Config {
        shards: 4,
        maxmemory: 300_000,
        policy: EvictionPolicy::AllKeysLru,
    });
    let mut model: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let mut rng = Rng(0xD1B5_4A32_D192_ED03);
    for step in 0..80_000u64 {
        let key = format!("k{}", rng.below(5_000)).into_bytes();
        if rng.below(2) == 0 {
            let len = if rng.below(50) == 0 {
                5_000 + rng.below(5_000)
            } else {
                rng.below(200)
            };
            let value: Vec<u8> = (0..len).map(|i| (i ^ step) as u8).collect();
            s.set(&key, &value, Condition::Always, Expiry::Never, 1, false)
                .expect("eviction makes room");
            model.insert(key, value);
        } else if let Some(got) = get(&s, &key, 1) {
            assert_eq!(Some(&got), model.get(&key), "step {step}: stale value");
        }
        if step % 10_000 == 0 {
            s.check();
        }
    }
    s.check();
    let stats = s.stats();
    assert!(stats.evicted_keys > 0, "{stats:?}");
    assert!(s.used_memory() <= 300_000 + 300_000 / 8, "{stats:?}");
}

#[test]
fn noeviction_rejects_writes_when_full() {
    let s = limited(100_000, EvictionPolicy::NoEviction);
    let mut accepted = 0;
    for i in 0..10_000u32 {
        match s.set(
            &i.to_le_bytes(),
            &[0; 100],
            Condition::Always,
            Expiry::Never,
            1,
            false,
        ) {
            Ok(_) => accepted += 1,
            Err(OutOfMemory) => break,
        }
    }
    assert!(accepted > 300 && accepted < 1_000, "accepted {accepted}");
    assert!(s.used_memory() <= 100_000 + 100_000 / 16);
    assert!(
        get(&s, &0u32.to_le_bytes(), 1).is_some(),
        "nothing was evicted"
    );
    s.check();
}

#[test]
fn allkeys_lru_stays_under_the_limit() {
    let limit = 1_000_000;
    let s = limited(limit, EvictionPolicy::AllKeysLru);
    let mut peak = 0;
    for i in 0..100_000u32 {
        s.set(
            &i.to_le_bytes(),
            &[0; 100],
            Condition::Always,
            Expiry::Never,
            1,
            false,
        )
        .expect("eviction makes room");
        peak = peak.max(s.used_memory());
    }
    // Within the accounting error, plus a segment or two in flight.
    assert!(peak <= limit + limit / 16, "peak {peak}");
    let stats = s.stats();
    assert!(stats.evicted_keys > 90_000, "{stats:?}");
    assert!(stats.keys > 5_000, "{stats:?}");
    s.check();
}

#[test]
fn lru_keeps_recently_read_keys() {
    let s = limited(2_000_000, EvictionPolicy::AllKeysLru);
    let hot: Vec<u32> = (0..1_000).collect();
    for i in 0..200_000u32 {
        let key = 1_000_000 + i;
        s.set(
            &key.to_le_bytes(),
            &[0; 100],
            Condition::Always,
            Expiry::Never,
            1,
            false,
        )
        .unwrap();
        if i < 1_000 {
            set(&s, &hot[i as usize].to_le_bytes(), &[1; 100], 1);
        }
        // Read the hot set regularly, as a cache would.
        if i % 2_000 == 0 {
            for k in &hot {
                get(&s, &k.to_le_bytes(), 1);
            }
        }
    }
    let survivors = hot
        .iter()
        .filter(|k| get(&s, &k.to_le_bytes(), 1).is_some())
        .count();
    // Without recency, ~10% would survive (2MB holds ~1/10 of the cold keys).
    assert!(survivors > 950, "only {survivors}/1000 hot keys survived");
    s.check();
}

#[test]
fn volatile_lru_only_evicts_keys_with_ttl() {
    let value = [0; 1000];
    let s = limited(400_000, EvictionPolicy::VolatileLru);
    let mut i = 0u32;
    while s.used_memory() < 200_000 {
        s.set(
            &i.to_le_bytes(),
            &value,
            Condition::Always,
            Expiry::Never,
            1,
            false,
        )
        .unwrap();
        i += 1;
    }
    let persistent = i;
    for j in 0..10_000u32 {
        let key = (1_000_000 + j).to_le_bytes();
        s.set(
            &key,
            &value,
            Condition::Always,
            Expiry::At(1_000_000),
            1,
            false,
        )
        .expect("TTL keys can be evicted to make room");
    }
    for k in 0..persistent {
        let key = k.to_le_bytes();
        assert!(get(&s, &key, 1).is_some(), "persistent key {k} was evicted");
    }
    // Once only persistent keys are left to evict, writes fail.
    let failed = (0..10_000u32).any(|j| {
        let key = (2_000_000 + j).to_le_bytes();
        s.set(&key, &value, Condition::Always, Expiry::Never, 1, false)
            .is_err()
    });
    assert!(failed);
    s.check();
}

#[test]
fn big_value_evicts_across_shards() {
    let s = limited(1_000_000, EvictionPolicy::AllKeysLru);
    for i in 0..8_000u32 {
        s.set(
            &i.to_le_bytes(),
            &[0; 100],
            Condition::Always,
            Expiry::Never,
            1,
            false,
        )
        .unwrap();
    }
    s.set(
        b"big",
        &vec![0; 600_000],
        Condition::Always,
        Expiry::Never,
        1,
        false,
    )
    .expect("evicts many small keys from many shards");
    assert!(get(&s, b"big", 1).is_some());
    assert!(s.used_memory() <= 1_000_000 + 1_000_000 / 16);
    // A value bigger than the whole limit can never fit.
    assert_eq!(
        s.set(
            b"huge",
            &vec![0; 2_000_000],
            Condition::Always,
            Expiry::Never,
            1,
            false
        )
        .err(),
        Some(OutOfMemory)
    );
    s.check();
}

#[test]
fn concurrent_writers_respect_the_limit() {
    let limit = 4_000_000;
    let s = Arc::new(Store::with_config(Config {
        shards: 64,
        maxmemory: limit,
        policy: EvictionPolicy::AllKeysLru,
    }));
    let threads: Vec<_> = (0..8u32)
        .map(|t| {
            let s = s.clone();
            std::thread::spawn(move || {
                for i in 0..50_000u32 {
                    let key = [t.to_le_bytes(), i.to_le_bytes()].concat();
                    s.set(&key, &[0; 64], Condition::Always, Expiry::Never, 1, false)
                        .unwrap();
                    get(&s, &key, 1);
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    assert!(
        s.used_memory() <= limit + limit / 16,
        "used {}",
        s.used_memory()
    );
    s.check();
}

fn segment_used(s: &Store) -> usize {
    s.shards
        .iter()
        .map(|sh| sh.inner.lock().mem.segment_used)
        .sum()
}

fn expire(s: &Store, key: &[u8], expiry: Expiry, now: Millis) {
    s.update_key(s.key(key), now, |_| (Update::Expire(expiry), ()))
        .unwrap();
}

fn expiry_of(s: &Store, key: &[u8], now: Millis) -> Option<Expiry> {
    s.update_key(s.key(key), now, |c| (Update::Peek, c.map(|c| c.expiry)))
        .unwrap()
}

#[test]
fn ttl_changes_in_place_when_they_fit() {
    let s = Store::new(1);
    put(&s, b"k", b"v", Condition::Always, Expiry::At(1_000_000), 1);
    let used = segment_used(&s);
    // Same width, shorter, and none: all rewritten in place.
    for expiry in [
        Expiry::At(2_000_000),
        Expiry::At(500),
        Expiry::Never,
        Expiry::At(700),
    ] {
        expire(&s, b"k", expiry, 1);
        assert_eq!(expiry_of(&s, b"k", 1), Some(expiry));
        assert_eq!(segment_used(&s), used, "{expiry:?} wasn't done in place");
        s.check();
    }
    // Wider than the field: the entry is copied.
    expire(&s, b"k", Expiry::At(MAX_DEADLINE), 1);
    assert_eq!(expiry_of(&s, b"k", 1), Some(Expiry::At(MAX_DEADLINE)));
    assert!(segment_used(&s) > used);
    s.check();

    // A key without a TTL gets one by being copied, then can lose it in place.
    set(&s, b"j", b"value", 1);
    expire(&s, b"j", Expiry::At(10), 1);
    assert_eq!(s.stats().keys_with_ttl, 2);
    expire(&s, b"j", Expiry::Never, 1);
    assert_eq!(s.stats().keys_with_ttl, 1);
    assert_eq!(s.expire_cycle(1_000), 0, "a removed TTL never expires");
    assert_eq!(get(&s, b"j", 1_000).as_deref(), Some(&b"value"[..]));
    // A deadline that has passed deletes the key.
    expire(&s, b"j", Expiry::At(1_000), 1_000);
    assert_eq!(get(&s, b"j", 1_000), None);
    s.check();
}

#[test]
fn ttl_changes_keep_big_values() {
    let s = Store::new(1);
    let big = vec![9u8; INLINE_MAX * 2];
    set(&s, b"big", &big, 1);
    expire(&s, b"big", Expiry::At(100), 1);
    expire(&s, b"big", Expiry::At(200), 1);
    expire(&s, b"big", Expiry::Never, 1);
    assert_eq!(get(&s, b"big", 1_000), Some(big));
    assert_eq!(
        s.stats().external_bytes,
        KeyValue::size_for(3, INLINE_MAX * 2)
    );
    s.check();
}

#[test]
fn integers_are_written_in_place_when_the_length_holds() {
    let s = Store::new(1);
    let incr = |n: i64| {
        s.update_key(s.key(b"n"), 1, |c| {
            let old: i64 = c.map_or(0, |c| {
                std::str::from_utf8(c.value.as_bytes())
                    .unwrap()
                    .parse()
                    .unwrap()
            });
            (Update::SetInt(old + n), ())
        })
        .unwrap();
    };
    incr(1);
    let used = segment_used(&s);
    for _ in 0..8 {
        incr(1);
    }
    assert_eq!(get(&s, b"n", 1).as_deref(), Some(&b"9"[..]));
    assert_eq!(segment_used(&s), used, "same-length writes are in place");
    incr(1);
    assert_eq!(get(&s, b"n", 1).as_deref(), Some(&b"10"[..]));
    assert!(segment_used(&s) > used);
    // A big old value is replaced by a small integer.
    set(&s, b"n", &vec![b'1'; INLINE_MAX * 2], 1);
    s.update_key(s.key(b"n"), 1, |_| (Update::SetInt(-7), ()))
        .unwrap();
    assert_eq!(get(&s, b"n", 1).as_deref(), Some(&b"-7"[..]));
    assert_eq!(s.stats().external_bytes, 0);
    s.check();
}

#[test]
fn integer_writes_respect_the_memory_limit() {
    let s = limited(64 * 1024, EvictionPolicy::NoEviction);
    set_int(&s, b"n", 1).unwrap();
    let mut i = 0u32;
    while s
        .set(
            &i.to_le_bytes(),
            &[0; 100],
            Condition::Always,
            Expiry::Never,
            1,
            false,
        )
        .is_ok()
    {
        i += 1;
    }
    set_int(&s, b"new", 5).expect_err("a new key needs memory");
    set_int(&s, b"n", 10).expect_err("a longer value needs memory");
    set_int(&s, b"n", 2).expect("same length: written in place");
    assert_eq!(get(&s, b"n", 1).as_deref(), Some(&b"2"[..]));
    s.check();
}

fn set_int(s: &Store, key: &[u8], n: i64) -> Result<(), OutOfMemory> {
    s.update_key(s.key(key), 1, |_| (Update::SetInt(n), ()))
}

#[test]
fn clear_frees_everything() {
    let s = Store::new(8);
    for i in 0..10_000u32 {
        set(&s, &i.to_le_bytes(), &[1; 100], 1);
    }
    set(&s, b"big", &vec![2; INLINE_MAX * 2], 1);
    put(&s, b"t", b"v", Condition::Always, Expiry::At(5), 1);
    assert_eq!(get(&s, b"t", 5), None); // counts one expiry
    s.clear();
    s.check();
    let stats = s.stats();
    assert_eq!(
        (stats.keys, stats.segment_bytes, stats.table_bytes),
        (0, 0, 0)
    );
    assert_eq!(stats.external_bytes, 0);
    assert!(s.used_memory() <= 8 * 4096, "{stats:?}");
    assert_eq!(stats.expired_keys, 1, "stats survive a flush");
    assert!(set(&s, b"k", b"v", 1));
    assert_eq!(get(&s, b"k", 1).as_deref(), Some(&b"v"[..]));
    s.check();
}

#[test]
fn removed_ttls_are_spared_by_volatile_lru() {
    let s = limited(256 * 1024, EvictionPolicy::VolatileLru);
    for i in 0..100u32 {
        put(
            &s,
            &i.to_le_bytes(),
            &[0; 100],
            Condition::Always,
            Expiry::At(1_000_000),
            1,
        );
        expire(&s, &i.to_le_bytes(), Expiry::Never, 1);
    }
    // Fill with keys that do have TTLs until eviction has to run a lot.
    for i in 100..10_000u32 {
        let _ = s.set(
            &i.to_le_bytes(),
            &[0; 100],
            Condition::Always,
            Expiry::At(1_000_000),
            1,
            false,
        );
    }
    for i in 0..100u32 {
        assert!(
            get(&s, &i.to_le_bytes(), 1).is_some(),
            "key {i} without a TTL was evicted"
        );
    }
    s.check();
}

/// Memory per key for the benchmark's dataset (`src/bin/bench.rs`), smaller.
/// A backstop for CI; `tools/speed-guard.sh` compares exactly between commits.
/// Raise the budget only with a written reason (CLAUDE.md).
#[test]
fn memory_budget() {
    let s = Store::new(256);
    let keys = 200_000;
    for i in 0..keys {
        set(&s, format!("key:{i}").as_bytes(), &[b'v'; 32], 1);
    }
    let per_key = s.stats().used_memory as f64 / keys as f64;
    println!("memory per key: {per_key:.2} bytes");
    assert!(
        per_key <= 60.0,
        "{per_key:.2} bytes per key, over the 60 budget"
    );
}
