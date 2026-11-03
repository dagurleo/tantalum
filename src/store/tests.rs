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
        let mut objects_seen = vec![false; self.objects.len()];
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
                    if header.kind().is_collection() {
                        assert!(!objects_seen[index], "object {index} referenced twice");
                        objects_seen[index] = true;
                        let object = self.objects[index].as_ref().unwrap();
                        assert_eq!(object.value.kind(), header.kind(), "object {index} kind");
                        mem.objects += object.value.allocation_size();
                    } else {
                        assert!(!externals_seen[index], "external {index} referenced twice");
                        externals_seen[index] = true;
                        mem.external += self.externals[index].as_ref().unwrap().allocation_size();
                    }
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
        for (index, object) in self.objects.iter().enumerate() {
            assert_eq!(
                object.is_some(),
                objects_seen[index],
                "object {index} leaked or dangling"
            );
            assert_eq!(
                object.is_none(),
                self.free_objects.contains(&(index as u32))
            );
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
            let key = Self::entry_key(&seg.buf, &header, &self.externals, &self.objects);
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
        ..Config::default()
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
    // Key to kind, value and deadline (0 for none).
    let mut model: HashMap<Vec<u8>, (Kind, Vec<u8>, Millis)> = HashMap::new();
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let mut now: Millis = 1;
    let random_value = |rng: &mut Rng, step: u64| -> Vec<u8> {
        let len = match rng.below(10) {
            0 => 0,
            1..7 => rng.below(64),
            7..9 => 100 + rng.below(400),
            _ => 3_000 + rng.below(6_000), // some are big enough to be external
        };
        (0..len).map(|i| (i ^ step) as u8).collect()
    };
    for step in 0..80_000u64 {
        let key = format!("k{}", rng.below(300)).into_bytes();
        let live = model
            .get(&key)
            .filter(|(_, _, d)| *d == 0 || *d > now)
            .cloned();
        match rng.below(100) {
            0..30 => {
                let value = random_value(&mut rng, step);
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
                let want_previous = rng.below(2) == 0;
                let out = s
                    .set(&key, &value, cond, expiry, now, want_previous)
                    .unwrap();
                // Asking for the previous value of another kind sets nothing.
                let wrong_type =
                    want_previous && live.as_ref().is_some_and(|(k, _, _)| *k != Kind::String);
                let applies = !wrong_type
                    && match cond {
                        Condition::Always => true,
                        Condition::IfAbsent => live.is_none(),
                        Condition::IfPresent => live.is_some(),
                    };
                assert_eq!(out.applied, applies, "step {step}");
                assert_eq!(out.wrong_type, wrong_type, "step {step}");
                let previous = live.as_ref().filter(|_| want_previous && !wrong_type);
                assert_eq!(
                    out.previous.map(|p| p.as_bytes().to_vec()),
                    previous.map(|(_, v, _)| v.clone()),
                    "step {step}"
                );
                if applies {
                    let deadline = match expiry {
                        Expiry::Never => 0,
                        Expiry::At(t) => t,
                        Expiry::Keep => live.map_or(0, |(_, _, d)| d),
                    };
                    model.insert(key, (Kind::String, value, deadline));
                }
            }
            30..55 => {
                let got = s.get_with(&key, now, |v| v.map(|v| (v.kind(), v.as_bytes().to_vec())));
                assert_eq!(got, live.map(|(k, v, _)| (k, v)), "step {step}");
            }
            55..85 => {
                // An update, after checking update_key shows the key as the model has it.
                let kind = if rng.below(2) == 0 {
                    Kind::String
                } else {
                    Kind::Hash
                };
                let update = match rng.below(9) {
                    0 => Update::Delete,
                    1 => Update::Expire(Expiry::At(now + 1 + rng.below(50))),
                    2 => Update::Expire(Expiry::At(now.saturating_sub(rng.below(3)).max(1))),
                    3 => Update::Expire(Expiry::Never),
                    4 => Update::SetInt(rng.below(2_000) as i64 - 1_000),
                    // As long as what's there, to be written in place when the kinds match.
                    5 | 6 if live.is_some() => {
                        let len = live.as_ref().unwrap().1.len();
                        Update::Replace(kind, (0..len).map(|i| (i as u64 ^ step) as u8).collect())
                    }
                    5..8 => Update::Replace(kind, random_value(&mut rng, step)),
                    _ => Update::Touch,
                };
                let shown = s
                    .update_key(s.key(&key), now, |current| {
                        let shown = current
                            .map(|c| (c.value.kind(), c.value.as_bytes().to_vec(), c.expiry));
                        (update.clone(), shown)
                    })
                    .unwrap();
                let expected = live.as_ref().map(|(k, v, d)| {
                    let expiry = if *d == 0 {
                        Expiry::Never
                    } else {
                        Expiry::At(*d)
                    };
                    (*k, v.clone(), expiry)
                });
                assert_eq!(shown, expected, "step {step}");
                let deadline = live.as_ref().map_or(0, |(_, _, d)| *d);
                match (update, live) {
                    (Update::SetInt(n), _) => {
                        model.insert(key, (Kind::String, n.to_string().into_bytes(), deadline));
                    }
                    (Update::Replace(kind, value), _) => {
                        model.insert(key, (kind, value, deadline));
                    }
                    (_, None) | (Update::Touch, _) => {}
                    (Update::Delete, _) => {
                        model.remove(&key);
                    }
                    (Update::Expire(Expiry::At(d)), _) if d <= now => {
                        model.remove(&key);
                    }
                    (Update::Expire(expiry), Some((kind, value, _))) => {
                        let deadline = match expiry {
                            Expiry::At(d) => d,
                            _ => 0,
                        };
                        model.insert(key, (kind, value, deadline));
                    }
                    (Update::Peek | Update::Object(_) | Update::Changed, _) => unreachable!(),
                }
            }
            85..88 => {
                // As snapshots load keys.
                let kind = if rng.below(2) == 0 {
                    Kind::String
                } else {
                    Kind::Hash
                };
                let value = random_value(&mut rng, step);
                let (expiry, deadline) = match rng.below(2) {
                    0 => (Expiry::Never, 0),
                    _ => {
                        let d = now + 1 + rng.below(50);
                        (Expiry::At(d), d)
                    }
                };
                s.restore(&key, kind, &value, expiry, now).unwrap();
                model.insert(key, (kind, value, deadline));
            }
            88..93 => {
                // RENAME and RENAMENX, sometimes onto itself.
                let to = format!("k{}", rng.below(300)).into_bytes();
                let only_if_absent = rng.below(3) == 0;
                let done = s
                    .rename(s.key(&key), s.key(&to), only_if_absent, now)
                    .unwrap();
                let target = model
                    .get(&to)
                    .filter(|(_, _, d)| *d == 0 || *d > now)
                    .cloned();
                let expected = match &live {
                    None => Renamed::NoSuchKey,
                    Some(_) if only_if_absent && (to == key || target.is_some()) => Renamed::Exists,
                    Some(_) => Renamed::Done,
                };
                assert_eq!(done, expected, "step {step}");
                if done == Renamed::Done && to != key {
                    let moved = model.remove(&key).unwrap();
                    model.insert(to, moved);
                }
            }
            93..97 => now += rng.below(20),
            _ => {
                s.expire_cycle(now);
            }
        }
        if step % 5_000 == 0 {
            s.check();
        }
    }
    s.check();
    // Every kind and value as the model has it, as a snapshot would see them.
    let mut seen = HashMap::new();
    for shard in 0..s.shard_count() {
        s.for_each_live(shard, now, |key, kind, value, _| {
            seen.insert(key.to_vec(), (kind, value.to_vec()));
        });
    }
    let expected: HashMap<_, _> = model
        .into_iter()
        .filter(|(_, (_, _, d))| *d == 0 || *d > now)
        .map(|(key, (kind, value, _))| (key, (kind, value)))
        .collect();
    assert_eq!(seen, expected);
}

/// Under eviction, keys may vanish but must never come back with stale values.
#[test]
fn eviction_never_returns_wrong_values() {
    let s = Store::with_config(Config {
        shards: 4,
        maxmemory: 300_000,
        policy: EvictionPolicy::AllKeysLru,
        ..Config::default()
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
        ..Config::default()
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

/// A whole scan returns every key that existed throughout exactly once, and
/// no key twice, whatever happens between its steps: overwrites and
/// compaction move entries, new keys move slots and grow the table, deletes
/// shrink it, TTL and type changes rewrite entries.
#[test]
fn scan_returns_lasting_keys_exactly_once() {
    for (round, count) in [1, 3, 50, 1000].into_iter().enumerate() {
        let s = Store::new(4);
        let mut rng = Rng(0xA076_1D64_78BD_642F ^ round as u64);
        let mut now: Millis = 1;
        let lasting: Vec<Vec<u8>> = (0..3_000)
            .map(|i| format!("keep{i}").into_bytes())
            .collect();
        for key in &lasting {
            put(&s, key, b"v", Condition::Always, Expiry::Never, now);
        }
        let mut ever: HashSet<Vec<u8>> = lasting.iter().cloned().collect();
        let mut temporary: Vec<Vec<u8>> = Vec::new();
        let mut seen: HashMap<Vec<u8>, usize> = HashMap::new();
        let mut cursor = 0;
        let mut steps = 0;
        loop {
            cursor = s.scan(cursor, count, now, |key, _| {
                *seen.entry(key.to_vec()).or_default() += 1;
            });
            if cursor == 0 {
                break;
            }
            steps += 1;
            assert!(steps < 100_000, "the scan never ended");
            for _ in 0..rng.below(8) {
                let keep = &lasting[rng.below(lasting.len() as u64) as usize];
                match rng.below(8) {
                    0 | 1 => {
                        let len = if rng.below(20) == 0 {
                            20_000
                        } else {
                            rng.below(200)
                        };
                        put(
                            &s,
                            keep,
                            &vec![b'x'; len as usize],
                            Condition::Always,
                            Expiry::Keep,
                            now,
                        );
                    }
                    2 => {
                        let deadline = if rng.below(2) == 0 {
                            Expiry::At(now + 1_000_000 + rng.below(1_000_000))
                        } else {
                            Expiry::Never
                        };
                        s.update_key(s.key(keep), now, |_| (Update::Expire(deadline), ()))
                            .unwrap();
                    }
                    3 => {
                        let kind = if rng.below(2) == 0 {
                            Kind::Hash
                        } else {
                            Kind::String
                        };
                        let value = vec![b'h'; rng.below(100) as usize];
                        s.update_key(s.key(keep), now, |_| {
                            (Update::Replace(kind, value.clone()), ())
                        })
                        .unwrap();
                    }
                    4 | 5 => {
                        // New keys, some expiring soon.
                        for _ in 0..rng.below(100) {
                            let key = format!("tmp{}", rng.below(20_000)).into_bytes();
                            let expiry = if rng.below(4) == 0 {
                                Expiry::At(now + 1 + rng.below(30))
                            } else {
                                Expiry::Never
                            };
                            put(&s, &key, b"t", Condition::Always, expiry, now);
                            ever.insert(key.clone());
                            temporary.push(key);
                        }
                    }
                    6 => {
                        for _ in 0..rng.below(150).min(temporary.len() as u64) {
                            let i = rng.below(temporary.len() as u64) as usize;
                            let key = temporary.swap_remove(i);
                            s.update_key(s.key(&key), now, |c| {
                                (
                                    if c.is_some() {
                                        Update::Delete
                                    } else {
                                        Update::Peek
                                    },
                                    (),
                                )
                            })
                            .unwrap();
                        }
                    }
                    _ => {
                        now += rng.below(10);
                        s.expire_cycle(now);
                    }
                }
            }
        }
        for key in &lasting {
            assert_eq!(seen.get(key), Some(&1), "count {count}: {key:?}");
        }
        for (key, times) in &seen {
            assert!(
                ever.contains(key),
                "count {count}: returned {key:?}, never written"
            );
            assert_eq!(*times, 1, "count {count}: returned {key:?} twice");
        }
        s.check();
    }
}

#[test]
fn scan_skips_expired_keys_and_takes_any_cursor() {
    let s = Store::new(8);
    for i in 0..2_000u32 {
        let expiry = if i % 2 == 0 {
            Expiry::At(50)
        } else {
            Expiry::Never
        };
        put(
            &s,
            format!("k{i}").as_bytes(),
            b"v",
            Condition::Always,
            expiry,
            1,
        );
    }
    let mut all = Vec::new();
    let mut cursor = 0;
    loop {
        cursor = s.scan(cursor, 100, 60, |key, kind| {
            assert_eq!(kind, Kind::String);
            all.push(key.to_vec());
        });
        if cursor == 0 {
            break;
        }
    }
    all.sort();
    let mut expected: Vec<Vec<u8>> = (1..2_000u32)
        .step_by(2)
        .map(|i| format!("k{i}").into_bytes())
        .collect();
    expected.sort();
    assert_eq!(all, expected);
    // Cursors from anywhere only ever give real keys. (`f` runs under the
    // shard's lock, so check them after.)
    let mut rng = Rng(0x5851_F42D_4C95_7F2D);
    for _ in 0..2_000 {
        let mut cursor = rng.below(u64::MAX) >> rng.below(64);
        for _ in 0..3 {
            let mut keys = Vec::new();
            cursor = s.scan(cursor, 1 + rng.below(50) as usize, 60, |key, _| {
                keys.push(key.to_vec());
            });
            for key in keys {
                assert!(get(&s, &key, 60).is_some(), "{key:?}");
            }
            if cursor == 0 {
                break;
            }
        }
    }
    // An empty store is done at once.
    assert_eq!(Store::new(16).scan(0, 10, 1, |_, _| panic!("a key")), 0);
}

#[test]
fn rename_keeps_kind_ttl_and_big_values() {
    let s = Store::new(8);
    let now = 1;
    let big = vec![7u8; 100_000];
    let long_key = vec![b'k'; 16_300];
    put(
        &s,
        b"small",
        b"v",
        Condition::Always,
        Expiry::At(10_000),
        now,
    );
    put(&s, b"big", &big, Condition::Always, Expiry::Never, now);
    s.restore(b"hash", Kind::Hash, b"\x01\x01f\x01v", Expiry::Never, now)
        .unwrap();
    let kind_and_value =
        |key: &[u8]| s.get_with(key, now, |v| v.map(|v| (v.kind(), v.as_bytes().to_vec())));
    let rename =
        |from: &[u8], to: &[u8], nx: bool| s.rename(s.key(from), s.key(to), nx, now).unwrap();
    // Across many destinations, so across shards and within one.
    let mut name = b"small".to_vec();
    for i in 0..50 {
        let next = format!("small{i}").into_bytes();
        assert_eq!(rename(&name, &next, false), Renamed::Done);
        assert_eq!(kind_and_value(&name), None);
        name = next;
    }
    assert_eq!(kind_and_value(&name), Some((Kind::String, b"v".to_vec())));
    let ttl = s
        .update_key(s.key(&name), now, |c| (Update::Peek, c.map(|c| c.expiry)))
        .unwrap();
    assert_eq!(ttl, Some(Expiry::At(10_000)));
    // Big values: external to external, to a key that makes it external, and back.
    assert_eq!(rename(b"big", b"big2", false), Renamed::Done);
    assert_eq!(kind_and_value(b"big2"), Some((Kind::String, big.clone())));
    put(&s, b"v", &[1; 100], Condition::Always, Expiry::Never, now);
    assert_eq!(rename(b"v", &long_key, false), Renamed::Done);
    assert_eq!(
        kind_and_value(&long_key),
        Some((Kind::String, vec![1; 100]))
    );
    assert_eq!(rename(&long_key, b"short", false), Renamed::Done);
    assert_eq!(kind_and_value(b"short"), Some((Kind::String, vec![1; 100])));
    // Over another kind, and NX.
    assert_eq!(rename(b"hash", b"short", true), Renamed::Exists);
    assert_eq!(rename(b"hash", b"short", false), Renamed::Done);
    assert_eq!(
        kind_and_value(b"short"),
        Some((Kind::Hash, b"\x01\x01f\x01v".to_vec()))
    );
    assert_eq!(rename(b"short", b"short", true), Renamed::Exists);
    assert_eq!(rename(b"short", b"short", false), Renamed::Done);
    assert_eq!(rename(b"missing", b"short", false), Renamed::NoSuchKey);
    // An expired source is gone, and an expired destination doesn't block NX.
    put(&s, b"old", b"o", Condition::Always, Expiry::At(5), now);
    assert_eq!(
        s.rename(s.key(b"old"), s.key(b"x"), false, 6).unwrap(),
        Renamed::NoSuchKey
    );
    put(&s, b"gone", b"g", Condition::Always, Expiry::At(5), now);
    assert_eq!(
        s.rename(s.key(b"big2"), s.key(b"gone"), true, 6).unwrap(),
        Renamed::Done
    );
    s.check();
}

/// Renames between keys in different shards from many threads at once,
/// each thread moving its own value back and forth: nothing deadlocks and
/// every value ends up under exactly one of its two names.
#[test]
fn concurrent_renames_neither_deadlock_nor_lose_values() {
    let s = Arc::new(Store::new(4));
    let threads = 8;
    let names = |t: usize| (format!("a{t}").into_bytes(), format!("b{t}").into_bytes());
    for t in 0..threads {
        put(
            &s,
            &names(t).0,
            format!("value{t}").as_bytes(),
            Condition::Always,
            Expiry::Never,
            1,
        );
    }
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let s = s.clone();
            std::thread::spawn(move || {
                let (a, b) = names(t);
                for i in 0..5_000 {
                    let (from, to) = if i % 2 == 0 { (&a, &b) } else { (&b, &a) };
                    assert_eq!(
                        s.rename(s.key(from), s.key(to), false, 1).unwrap(),
                        Renamed::Done
                    );
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    for t in 0..threads {
        let (a, b) = names(t);
        assert_eq!(get(&s, &a, 1), Some(format!("value{t}").into_bytes()));
        assert_eq!(get(&s, &b, 1), None);
    }
    s.check();
}

/// Keys that share a slot hash come in the same scan step, so a step that
/// ends on one doesn't lose the others.
#[test]
fn scan_never_splits_keys_with_the_same_hash() {
    let s = Store::with_config(Config {
        shards: 1,
        fixed_seed: true,
        ..Config::default()
    });
    // A 32-bit hash repeats within a few hundred thousand keys.
    let mut by_hash: HashMap<u32, Vec<Vec<u8>>> = HashMap::new();
    let mut colliding = Vec::new();
    for i in 0..1_000_000u32 {
        let key = format!("c{i}").into_bytes();
        let keys = by_hash.entry(s.locate(s.key(&key).hash).0).or_default();
        keys.push(key);
        if keys.len() == 2 {
            colliding.extend(keys.iter().cloned());
            if colliding.len() >= 10 {
                break;
            }
        }
    }
    assert!(colliding.len() >= 10, "no collisions found");
    let others: Vec<Vec<u8>> = (0..200).map(|i| format!("o{i}").into_bytes()).collect();
    for key in colliding.iter().chain(&others) {
        put(&s, key, b"v", Condition::Always, Expiry::Never, 1);
    }
    for count in [1, 2, 3] {
        let mut seen = Vec::new();
        let mut cursor = 0;
        loop {
            cursor = s.scan(cursor, count, 1, |key, _| seen.push(key.to_vec()));
            if cursor == 0 {
                break;
            }
        }
        seen.sort();
        let mut expected: Vec<_> = colliding.iter().chain(&others).cloned().collect();
        expected.sort();
        assert_eq!(seen, expected, "count {count}");
    }
}

/// Big collections as objects: made, changed in place, renamed, given and
/// past TTLs, overwritten, evicted and moved by compaction, with the books
/// checked throughout.
#[test]
fn big_collections_keep_the_books() {
    use crate::set::BigSet;
    for policy in [EvictionPolicy::NoEviction, EvictionPolicy::AllKeysLru] {
        let s = Store::with_config(Config {
            shards: 2,
            maxmemory: 2 << 20,
            policy,
            ..Config::default()
        });
        let mut rng = Rng(0x2545_F491_4F6C_DD1D);
        let mut now: Millis = 1;
        let big = |n: u64, tag: u64| {
            let mut set = BigSet::with_capacity(n as usize);
            for i in 0..n {
                set.insert(format!("{tag}-{i}").as_bytes());
            }
            Collection::Set(set)
        };
        for step in 0..4_000u64 {
            let key = format!("c{}", rng.below(40)).into_bytes();
            let k = s.key(&key);
            match rng.below(9) {
                0 | 1 => {
                    let mut made = Some(big(200 + rng.below(300), step));
                    let _ = s.update_collection(k, now, true, |_| {
                        (Update::Object(made.take().expect("once")), ())
                    });
                }
                2 | 3 => {
                    let extra = rng.below(50);
                    let _ = s.update_collection(k, now, true, |entry| match entry {
                        Some(Entry {
                            value: EntryValue::Object(Collection::Set(set)),
                            ..
                        }) => {
                            for i in 0..extra {
                                set.insert(format!("x{step}-{i}").as_bytes());
                            }
                            set.remove(b"0-0");
                            (Update::Changed, ())
                        }
                        _ => (Update::Peek, ()),
                    });
                }
                4 => {
                    let to = format!("c{}", rng.below(40)).into_bytes();
                    let _ = s.rename(k, s.key(&to), rng.below(2) == 0, now);
                }
                5 => {
                    let expiry = Expiry::At(now + 1 + rng.below(30));
                    s.update_key(k, now, |c| {
                        (
                            if c.is_some() {
                                Update::Expire(expiry)
                            } else {
                                Update::Peek
                            },
                            (),
                        )
                    })
                    .unwrap();
                }
                6 => {
                    // A string over it, or a delete.
                    if rng.below(2) == 0 {
                        let _ = s.set(&key, b"plain", Condition::Always, Expiry::Never, now, false);
                    } else {
                        s.update_key(k, now, |c| {
                            (
                                if c.is_some() {
                                    Update::Delete
                                } else {
                                    Update::Peek
                                },
                                (),
                            )
                        })
                        .unwrap();
                    }
                }
                7 => {
                    // Some strings, to fill and churn segments.
                    for i in 0..50 {
                        let _ = s.set(
                            format!("str{}", rng.below(2_000) + i).as_bytes(),
                            &[1; 300],
                            Condition::Always,
                            Expiry::Never,
                            now,
                            false,
                        );
                    }
                }
                _ => {
                    now += rng.below(20);
                    s.expire_cycle(now);
                }
            }
            if step % 500 == 0 {
                s.check();
            }
        }
        s.check();
        // A big set reads back as one, and snapshots see it packed.
        let mut sets = 0;
        for shard in 0..s.shard_count() {
            s.for_each_live(shard, now, |_, kind, value, _| {
                if kind == Kind::Set {
                    assert!(crate::set::is_valid(value));
                    sets += 1;
                }
            });
        }
        assert!(sets > 0, "{policy:?}: no sets survived");
    }
}
