# Tantalum

A small, fast key/value server in Rust that speaks the Redis protocol (RESP2
and RESP3). It does Redis's string commands for caching (`GET`/`SET`,
`MGET`/`MSET`, `DEL`, `EXPIRE`/`TTL`, `INCR` and friends) with TTLs, eviction
and a password, and that's the point: be a faster drop-in for the "Redis as a
cache" use case, using as little memory as possible.

Named after the metal of tantalum capacitors, known for packing the most into
the least space. (It was called ferrokv until v0.2; the raw logs in
`bench/railway/` from before the rename still use that name.)

```sh
cargo run --release -- --port 6379      # --threads N, --shards N, --bind ADDR
cargo run --release -- --maxmemory 2gb --maxmemory-policy allkeys-lru
TANTALUM_REQUIREPASS=s3cret cargo run --release -- --bind 0.0.0.0   # or --requirepass
cargo run --release -- --maxclients 2000 --timeout 300 --tcp-keepalive 60
redis-cli SET greeting hello EX 60
redis-cli GET greeting
```

Any Redis client, `redis-cli`, `redis-cli --pipe`, and `redis-benchmark` work as is.

To deploy, `deploy/Dockerfile` builds a small image with just the binary
(`docker build -f deploy/Dockerfile .`); on Railway, set
`RAILWAY_DOCKERFILE_PATH=deploy/Dockerfile` and put options in the start
command. It listens on `::` (IPv4 and IPv6) with no TLS, so keep it on a
private network and set a password.

## Commands

Replies match Redis 7 byte for byte, errors included, as far as the tests
check (`tests/server.rs`).

| Command | Notes |
|---|---|
| `GET key`, `MGET key [key ...]` | `MGET` prefetches its keys like a pipelined batch |
| `SET key value [NX\|XX] [GET] [EX s\|PX ms\|EXAT ts\|PXAT ts-ms\|KEEPTTL]` | All Redis 7 options |
| `MSET key value [key value ...]`, `SETNX`, `SETEX`, `PSETEX` | `MSET` writes keys one at a time: unlike Redis, other clients can see some written before the rest |
| `GETDEL key`, `GETEX key [EX s\|PX ms\|EXAT ts\|PXAT ts-ms\|PERSIST]` | |
| `DEL key [key ...]`, `UNLINK key [key ...]`, `EXISTS key [key ...]` | The same thing for `DEL` and `UNLINK`: big values are always freed after the lock is released |
| `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT` `key time [NX\|XX\|GT\|LT]` | A time already passed deletes the key |
| `TTL`, `PTTL`, `EXPIRETIME`, `PEXPIRETIME`, `PERSIST` | |
| `INCR`, `DECR`, `INCRBY`, `DECRBY` | 64-bit, keep the key's TTL |
| `FLUSHALL [ASYNC\|SYNC]`, `FLUSHDB [ASYNC\|SYNC]` | Not atomic across shards: writes running at the same time may survive |
| `AUTH [username] password` | With `--requirepass` (or `TANTALUM_REQUIREPASS`). One user, `default`, as in Redis without ACLs |
| `HELLO [2\|3 [AUTH user pass] [SETNAME name]]` | RESP3 too: redis-py 8 won't connect without it. For these commands it only changes nulls, maps (`HELLO`, `CONFIG GET`), sets (`COMMAND` flags) and `INFO` (verbatim text) |
| `CLIENT SETNAME\|GETNAME\|ID\|SETINFO` | What client libraries send on connect |
| `COMMAND [COUNT\|INFO\|LIST\|DOCS]`, `CONFIG GET pattern ...` | For clients and tools that discover commands or read `maxmemory` and the policy. No `CONFIG SET` |
| `INFO`, `DBSIZE` | `INFO` has the fields monitoring tools read: `redis_version` (7.2.4, the replies' compatibility level), `uptime_in_seconds`, `connected_clients`, `maxclients`, `used_memory` (with a breakdown: `segment_bytes`, `segment_live_bytes`, `external_bytes`, `table_bytes`, `mem_clients_normal`), `used_memory_rss` (Linux), `maxmemory`, `maxmemory_policy`, `total_connections_received`, `total_commands_processed`, `rejected_connections`, `keyspace_hits` and `keyspace_misses` (from `GET`, `MGET`, `GETEX` and `GETDEL`), `evicted_keys`, `expired_keys` and the keyspace line |
| `PING [msg]`, `ECHO msg`, `SELECT 0`, `QUIT` | |

Anything else gets `-ERR unknown command ...` and the connection stays open.
With a password set, every command except `AUTH`, `HELLO` and `QUIT` gets
`-NOAUTH` until the client authenticates.

## Design

- **Network** (`src/server.rs`): Tokio, one task per connection, `TCP_NODELAY`.
  Each read is parsed for *every* complete command (pipelining) and all
  replies go out in one write. Without pipelining, syscalls and the kernel's
  TCP stack are ~75% of the server's CPU time; with it, the memory the store
  reads is what's left.
- **Prefetching pipelined batches**: a read's commands are parsed before
  any runs. While command *i* runs, the store asks the CPU to start loading
  the two buckets command *i*+8's key can be in, and the entry command *i*+4's
  buckets point to (looked up with a `try_lock`, skipped if the shard is
  busy). The bucket arrays' addresses are published per shard so the first
  step needs no lock. At 1M keys a pipelined command otherwise waits on two
  or three cache misses in turn, which cost ~45% of throughput (measured by
  shrinking the data to 10k keys). Prefetching won back +46% per core at
  pipeline 16 and lowered p99; a batch of one command skips it.
- **An event loop per thread** (`--io per-thread`, the default where io_uring isn't available), like memcached:
  each thread runs its own single-threaded runtime, the main thread hands it
  connections round-robin, and a connection stays on that thread for life.
  Tokio's shared work-stealing runtime (`--io shared`) can run a ready
  connection on any thread, which costs cross-thread wake-ups and queue traffic
  on every request. Without pipelining, where each request is tiny work between two
  syscalls, that overhead decided the race: per-thread was 19% faster and
  overtook memcached. The trade-off: a few very busy connections can't be
  rebalanced across threads.
- **io_uring** (`--io uring`, the default where the kernel and container
  allow it): per-thread loops on [monoio](https://github.com/bytedance/monoio).
  On a 32-core EPYC it beat the epoll loop at every thread count (+17–25%
  without pipelining, +12–21% with, and lower p99; see below). The startup
  line says which I/O it got. The ring setup decides whether it
  pays off: by default the kernel interrupts the thread to post each
  completion, which made io_uring 8–13% *slower* than the tokio loop. With
  `DEFER_TASKRUN` (Linux 6.1+, falling back to `COOP_TASKRUN`, then plain),
  completions are posted in one batch when the loop runs out of work, and it
  became 2–3% faster with ~20% lower p99 (see below). Docker's and Railway's
  default seccomp profiles block io_uring. monoio's epoll fallback was 17–22%
  slower than the tokio loop, so where io_uring is unavailable Tantalum runs
  per-thread instead (and says so if `--io uring` was asked for).
- **Parser** (`src/resp.rs`): zero-copy and restartable. Arguments are ranges
  into the read buffer, so a command split across TCP reads costs nothing extra.
  Binary safe. Same limits as Redis (512 MB bulk, 64 KB inline, 1 GB query buffer).
- **Store** (`src/store.rs`): shards (256, or 1 per 4 MB of `maxmemory` between
  16 and 256), each a `parking_lot::Mutex` around a hash table and a set of
  segments. The key is hashed (foldhash, randomly seeded) outside the lock: the
  high 32 bits pick the shard, the low 32 are stored in the table.
- **Segments** (`src/segment.rs`): entries are packed back to back into
  append-only segments of 4–256 KB, sized to the shard (about 1/16 of its data).
  No heap allocation per key means no allocator header, no size-class rounding
  and no fragmentation between keys. An entry is a flags byte (dead bit,
  has-TTL, external, 2-bit usage counter), varint key and value lengths, an
  optional varint deadline, then key and value. A 15-byte key with a 32-byte
  value takes 50 bytes.
- **Table** (`src/table.rs`): bucketized cuckoo hashing over 8-byte slots,
  each holding 32 bits of hash plus a 32-bit location (segment id and offset).
  - Each key can live in one of two 8-slot buckets (a cache line each), so a
    lookup reads at most two, and the stored hash is compared before an entry is touched.
  - When both buckets are full, an insert moves existing slots to their other
    bucket (a short breadth-first search), so the table runs up to 15/16 full.
  - It grows 25% at a time and averages ~84% full: about 9.5 bytes per key,
    against 13.5–19 with hashbrown. hashbrown doubles and runs at most 87.5%
    full, and all shards doubled together, so at 1M keys every table was 48% full.
  - Resizes never read keys, and deletes leave no tombstones.
  - Keys that can't be placed (17+ sharing a 32-bit hash) go to an overflow
    list instead of growing the table forever. A test caught that runaway case.
- **TTL changes and counters update in place** when they can: a new deadline
  is written over the old one, padded to the same width (LEB128 allows extra
  continuation bytes, and 0 there means the TTL was removed), and an `INCR` that
  doesn't change the value's length overwrites its digits. `EXPIRE` on a key
  without a TTL, or a deadline too wide for the field, copies the entry like an
  overwrite.
- **Overwrites and deletes** append the new entry and mark the old one dead. A
  segment that becomes all dead is freed at once. When dead bytes pass a quarter
  of a shard, its deadest segment is compacted: live entries are copied to the
  active segment and the old one is freed. Within 10% of `maxmemory` the
  threshold drops to a tenth, since there memory is scarcer than the CPU spent copying.
- **Big entries** (over 16 KB) keep key and value in their own refcounted
  allocation (`src/entry.rs`), so compaction never copies them and readers
  share them without a copy.
- **Connections** (`src/context.rs`), with Redis' defaults:
  - At most `--maxclients` (10,000); beyond that a client gets
    `-ERR max number of clients reached`. The open-file limit is raised to fit.
  - `--timeout` closes idle connections (off by default), and
    `--tcp-keepalive` (300 s) probes idle ones so clients that vanished
    without closing don't hold a slot forever.
  - Idle connections hold almost no buffer memory. On epoll (per-thread and
    shared), a connection waits for data before taking a read buffer and hands
    both buffers back once it has nothing left to process; mimalloc's
    per-thread caches make that cheap. io_uring must post a read buffer before
    data arrives, so an idle connection keeps 2 KB, grown as reads fill it.
    10,000 idle connections on Linux: 14 MB on epoll and 43 MB on io_uring,
    against 92 and 101 MB before (RSS; macOS showed 166 MB against 11 MB for
    5,000). Paired runs on EC2 put the throughput cost at 0–1.7%
    (`bench/aws/results-c7a-8xlarge.txt`).
  - Buffers count toward `used_memory`, so eviction and the limit leave room
    for them, including a half-received big value up to the 1 GB query buffer
    limit. Each connection reports changes of 64 KB or more, so that buffers
    coming and going with every request don't make each one write a shared
    counter: `mem_clients_normal` shows big buffers, not small transient ones.
  - On SIGTERM or SIGINT, accepting stops and the read side of every socket is
    shut down: each connection finishes the commands it has read, replies,
    sees end of file and closes. Nothing checks a flag per request. The
    process exits when they're all closed, or after 10 s. A second signal
    exits at once.
  - A panic in a command (a bug) closes only its connection, with an error
    reply. monoio doesn't contain a panicking task the way tokio does, so
    without this a panic on `--io uring` would take down a whole I/O thread.
  - Server counters are per thread (one cache line each) and published once
    per read, so `INFO`'s totals cost the request path no shared writes.
- **Lock hygiene**: hashing and big-value allocations happen before locking.
  Values ≤ 16 KB are copied out under the lock; larger ones are shared by
  refcount and written with `writev` after unlocking. Freed big values are
  dropped after the lock is released.
- **TTL**: lazy (checked on every access) plus a background sweep every
  100 ms. Each segment tracks its earliest deadline, so the sweep only scans
  segments with something due, within a per-shard byte budget. No per-key
  expiry index: a TTL costs only the deadline's varint (~5 bytes).
- **Allocator**: mimalloc, except for segments and hash tables (`src/pages.rs`):
  - **Segments** come from a pool of 8 MB OS mappings carved into power-of-two
    blocks. Under eviction churn, segments allocated and freed across threads
    through mimalloc kept RSS ~9% above what was in use. Pool blocks become
    resident page by page as they're written. Freed blocks stay resident for
    reuse up to 4 MB in total; beyond that they drop their pages
    (`madvise(MADV_DONTNEED)`). An A/B on Railway showed that dropping every
    freed block's pages cost ~8% of pipelined throughput, because new segments
    had to fault them back in (~10k faults/s). Big mappings keep the mapping count far
    below Linux's ~65k limit.
  - **Hash tables** are mapped straight from the OS when rounding up to whole
    pages wastes at most 1/8 (on 4 KB-page Linux, anything over ~18 KB). A resize
    frees the old table, and through mimalloc those freed tables stayed stuck in
    partly used pages (~10 bytes/key at 1M keys). This is also why the default is
    256 shards rather than 1024: bigger tables per shard, all of them mappable.
- **Transparent huge pages are disabled for the process on Linux**, as Redis
  does. With them on (`always` on Railway's hosts), the kernel backed barely
  used 2 MB regions with whole huge pages: RSS was 114 MB instead of 97 MB at
  1M keys, and 61 MB instead of 37 MB under a 16 MB `maxmemory`.

## Memory limit and eviction

`--maxmemory <size>` (`100mb`, `2gb`, …; `1k` = 1000, `1kb` = 1024, as in Redis)
caps the accounted memory. `--maxmemory-policy` decides what happens at the cap:

| Policy | Behavior |
|---|---|
| `noeviction` (default, as in Redis) | Writes fail with `-OOM command not allowed when used memory > 'maxmemory'.` |
| `allkeys-lru` | Evict approximately least recently used keys |
| `volatile-lru` | Same, but only keys with a TTL; OOM once none are left |

- **What's counted**: segments' written pages (dead entries count; a
  segment's untouched tail doesn't, because it isn't resident), big-value
  allocations and hash tables. Connection buffers aren't, so RSS runs somewhat
  above `used_memory` (Redis has the same gap).
- **Accounting is sharded**: each shard tracks its bytes under its own lock and
  folds them into a global counter in batches, so writers don't contend on one
  atomic. The batch size scales with the limit, so the global figure is within
  ~1.6% of the truth. One write can also overshoot briefly by a shard's
  table resize; the next write evicts it back.
- **GCLOCK eviction, one segment at a time**: every read or overwrite bumps
  the entry's 2-bit usage counter; new keys start at zero, so keys written but
  never read go first. Eviction looks at a shard's oldest segment. If at least
  a quarter of it is reclaimable (dead, expired or unused since its last pass),
  it's compacted: used entries move to the young end with their counter
  decremented, the rest are dropped. Otherwise the segment is mostly hot, so it's
  aged in place instead: counters decremented without copying, segment moved to
  the back. Hot keys are never forced out, each compaction frees at least a
  quarter of a segment, and a segment can be aged at most 3 times before it
  qualifies. (A first version capped survivors at half a segment instead. It
  lost a third of a hot key set whenever hot keys were clustered.)
- **Choosing a shard**: each eviction visit compares the writer's shard with a
  random one and evicts from whichever holds more ("power of two choices").
  Always evicting from a fixed shard lets shards drift apart until some hold only
  hot keys and get drained anyway. A test caught exactly that: about 6% of a hot set
  was lost in one burst. If 64 visits in a row reclaim nothing, eviction sweeps
  every shard in turn. Each sweep ages every segment once, and OOM is reported
  only if four sweeps find nothing evictable.
- Eviction holds one shard lock at a time and happens before the write takes
  its own lock, so it can't deadlock. Evicted values are freed outside the lock.
- A value larger than the whole limit is rejected up front instead of emptying
  the cache trying to fit it.

## Benchmarks

### Linux (Railway, November 2026)

Railway containers with a 32-vCPU limit, all servers and the client in one
container over loopback, on AMD EPYC 9655P hosts. Versions: Redis 8.10.3,
Valkey 9.1.3, memcached 1.6.38, Dragonfly 2.0.2, all with persistence off.
Every multi-threaded server gets 4 threads. Client: `memtier_benchmark` with
random values, 15–16-byte keys, `--distinct-client-seed`. Script:
`bench/railway/suite.sh`; raw output: `bench/railway/results-v0.2-field.txt`.
Hosts vary by ±10–15% between runs, so compare servers within a table.
Total cloud cost of all benchmarking so far: ~$0.40.

**Memory with no limit**: each server alone, filled once, RSS after settling:

| Values (keys) | Tantalum | Redis | Valkey | memcached | Dragonfly |
|---|---|---|---|---|---|
| 16 B (2M) | **97 MB** | 159 | 154 | 209 | 270 |
| 100 B (1M) | **136 MB** | 183 | 186 | 201 | 295 |
| 1 KB (200k) | **223 MB** | 277 | 280 | 237 | 355 |
| 10 KB (20k) | **214 MB** | 258 | 262 | 222 | 410 |
| 32 B, all with a 1–2 h TTL (1M)* | **85 MB** | 137.5 | 143.2 | – | – |

Tantalum's numbers come from a follow-up run on the same host type after its
last fix; memcached measured the same in both runs. *TTL row from an earlier
version on another host. Dragonfly reserves ~134 MB up front, which dominates
at small sizes.

**Memory at a 1 GB limit**, the state most production caches live in:

| | Tantalum | Redis | Valkey | memcached | Dragonfly |
|---|---|---|---|---|---|
| 100 B values, gaussian keys: keys held | **7.59M** | 6.28M | 6.48M | 5.59M | 7.15M |
| … RSS | 1054 MB | 1068 | **1053** | 1065 | 1165 |
| … RSS per key held | **146 B** | 178 | 171 | 200 | 171 |
| Random keys, mixed 32 B–2 KB values, 5 min churn: RSS per key held | **1193 B** | 1244 | 1239 | 1271 | 1390 |
| RSS during the churn run | flat, 1054–1055 MB | flat | flat | flat | flat, 17% over its limit |

**Eviction quality**: GET hit rate on the 100 B workload above, with
independent requests, and relative to an ideal cache that holds the same
number of keys (the most popular ones):

| | Tantalum | Redis | Valkey | memcached | Dragonfly |
|---|---|---|---|---|---|
| GET hit rate | **31.1%** | 24.7% | 25.5% | 22.1% | 27.9% |
| Relative to an ideal cache of that size | **75%** | 71% | 71% | 71% | 72% |
| Throughput | 406k ops/s | 120k | 119k | **450k** | 289k |

The policies are within a few points of each other. Hit rates differ mostly by
how many keys fit in the same limit.

> **Correction:** earlier versions of this README showed Tantalum at 83–85% of
> ideal against ~71–74% for Redis and Valkey. Those runs had a flaw: memtier's seeds are fixed, so
> the measured phase replayed the warm-up's key sequence and asked for the
> oldest inserts first. That skewed results by policy; Dragonfly even scored
> above a perfect cache. The scripts now use `--randomize` for measured phases.

**Throughput, per-thread I/O** (the default from v0.3 until io_uring in v0.5;
`bench/railway/results-io-ab.txt`): 1M keys of 32 bytes, uniform random keys,
1:10 SET:GET, 4 threads each, three alternating rounds:

| | Tantalum, per-thread I/O | Tantalum, shared runtime | memcached |
|---|---|---|---|
| No pipelining, 200 clients | **458–493k ops/s** | 389–423k | 434–492k |
| … p99 | **0.70–0.84 ms** | 0.94–1.08 ms | 0.89–1.06 ms |
| No pipelining, 50 clients | 189–210k on **1.7 cores** | 183–203k on 2.4 cores | 179–215k on 1.9 cores |
| Pipeline 16, 200 clients | **3.08–3.43M** | 2.79–3.07M | 2.22–2.71M |

Without pipelining Tantalum now edges past memcached at 200 clients (+4% on
average), with lower tail latency. At 50 clients all three are bound by round
trips, and per-thread gets there on the least CPU. With pipelining it's 28% ahead.

**Throughput against everything**, from the full suite run with the older
shared runtime: 1M keys of 32 bytes, uniform random keys, 1:10 SET:GET, 200
clients, two alternating rounds (this host ran ~10–15% slower than earlier ones):

| | Tantalum (shared runtime) | memcached | Redis, 4 io-threads | Dragonfly | Valkey | Redis |
|---|---|---|---|---|---|---|
| No pipelining | 346–370k | **434–447k** | 267–292k | 258–285k | 116–138k | 104–113k |
| p50, no pipelining | 0.52–0.55 ms | **0.41 ms** | 0.62–0.68 ms | 0.67–0.74 ms | 1.3–1.6 ms | 1.6–1.8 ms |
| Pipeline 16 | **2.73–2.84M** | 2.20–2.45M | 1.61–1.77M | 1.32–1.54M | 1.10–1.20M | 0.87–0.88M |
| Server CPU, pipeline 16 | 3.8 cores | 4.6–4.7 | 2.6 | 3.7 | 0.97–0.98 | 0.91–0.92 |

With the shared runtime memcached was ~20% faster without pipelining; the
per-thread table above is the current picture. Per CPU core, single-threaded
Redis and Valkey still do the most work. The multi-threaded servers win by using
more cores.

**The segment pool's cost** (`bench/railway/results-pool-ab.txt`): A/B runs
with the pool on, off, and keeping freed pages resident. Dropping freed
blocks' pages makes new segments fault them back in (~8–10k faults/s pipelined).
In v0.1.1 that measured ~8% of pipelined throughput (2.85M vs 3.10M, 4 rounds). In
v0.2, with up to 4 MB of freed blocks kept resident, one run showed a large gap
and a reversed-order rerun showed ~2–3%. Both are within this host's noise. What
the pool buys: 96 MB RSS against 119 MB when keeping every freed page.

**How memory got here**, 1M keys of 32 bytes on Linux:

| Version | RSS |
|---|---|
| v0.0.2: one allocation per key, 16-byte slots | 111 MB |
| v0.1 segments, transparent huge pages on | 114 MB |
| … with transparent huge pages off | 97 MB |
| … plus page-mapped tables and 256 shards | **78 MB** |

v0.1.1's pooled segments, written-page accounting and eager compaction then
took the 1 GB-limit case from 172 to 155 bytes per key held. v0.2's cuckoo table
halved table memory (18.9 → 9.4 bytes per key at 1M keys): 2M × 16 B went from
116 to 97 MB, and the 1 GB-limit case went to 146 bytes per key held.

### Multi-core, AWS EC2 (November 2026)

A c7a.8xlarge: 32 AMD EPYC 9R14 cores (no SMT), Ubuntu 24.04, kernel 7.0.
Servers pinned to cores 0..N-1 and memtier to cores 8-31 (24 threads × 10
connections) at every N; Redis and Valkey get N I/O threads, Dragonfly N
proactor threads (on io_uring), memcached `-t N`. 1M keys × 32 B, 1:10
SET:GET, 2 rounds (`bench/aws/scale.sh`; raw output and caveats:
`bench/aws/results-c7a-8xlarge.txt`). Total ops/s:

| | 1 thread | 2 | 4 | 8 |
|---|---|---|---|---|
| **Tantalum, io_uring**, no pipelining | **100k** | **239k** | **488k** | **825k** |
| Tantalum, per-thread | 80k | 192k | 410k | 683k |
| Dragonfly | 81k | 190k | 399k | 730k |
| memcached | 77k | 190k | 387k | 650k |
| Valkey | 81k | 107k | 303k | 575k |
| Redis | 78k | 87k | 248k | 484k |
| **Tantalum, io_uring**, pipeline 16 | **1.13M** | **2.71M** | **5.33M** | **8.36M** |
| Tantalum, per-thread | 0.96M | 2.23M | 4.62M | 7.50M |
| memcached | 0.54M | 1.27M | 2.38M | 3.98M |
| Dragonfly | 0.43M | 1.03M | 1.94M | 3.20M |
| Valkey | 0.83M | 1.31M | 3.09M | 2.51M |
| Redis | 0.76M | 0.96M | 2.26M | 2.33M |

- Per core, a cloud VM does about half the M1 VM's work (syscalls cost more),
  so compare these with each other, not with the tables below.
- Tantalum on io_uring scales nearly linearly to 4 threads (~120k ops/core
  without pipelining at 1, 2 and 4) and keeps ~85% per core at 8. Pipelined,
  8 threads reached 8.4M with memtier near its limit, so that's a floor. A
  profile at 8 threads showed no lock contention, mostly the kernel's
  loopback TCP stack: the 20% per-core drop at 2 threads seen in the laptop
  VM doesn't happen here.
- The 1-thread column comes from a later pass that ran ~15% slower for every
  server (a first pass had Tantalum io_uring at 121k / 1.39M and Valkey at
  97k / 0.98M); the ratios between servers held.
- With N I/O threads, an idle Valkey kept a core busy for 15+ s after its
  fill, which skewed the first test of a round; Tantalum per-thread's numbers
  from 2 threads up are from the second round only.

### io_uring (Docker Desktop, November 2026)

Railway blocks io_uring, so this ran in Docker Desktop on an Apple M1 Pro
(Linux 6.12 VM, 8 vCPUs) with `--security-opt seccomp=unconfined`; each
`--io uring` server held one ring per thread. Servers and client share the VM,
so only the ratios mean anything. Same workload as above, 4 server threads;
medians of 5 rounds with the order rotated (`bench/docker/io-ab.sh`; raw output:
`bench/docker/results-io-ab.txt`):

| | No pipelining, 200 clients | 50 clients | Pipeline 16 | p99, no pipelining | p99, pipeline 16 |
|---|---|---|---|---|---|
| `--io per-thread` (epoll) | 539k | 301k | 3.64M | 2.69 ms | 3.52 ms |
| `--io uring`, `DEFER_TASKRUN` | 549k (+2%) | 311k (+3%) | **3.72M** (+2%) | **2.16 ms** | **2.72 ms** |
| … `COOP_TASKRUN` | **550k** (+2%) | **315k** (+4%) | 3.63M (0%) | 2.27 ms | 2.85 ms |
| … plain (monoio's default) | 468k (−13%) | 275k (−9%) | 3.37M (−8%) | 2.38 ms | 2.88 ms |

In an earlier run with all five servers, memcached did 0.49M / 0.29M / 2.08M.
Dragonfly did 0.39M / 0.29M / 1.87M on io_uring and 0.36M / 0.29M / 1.84M on
epoll, so io_uring is worth 1–9% to it here. The servers weren't saturated
(about 2.6 of 4 cores), so these numbers measure per-request cost more than
peak capacity. macOS background jobs disturbed parts of the earlier runs, which
the raw file marks.

### Per core, server saturated (Docker Desktop, November 2026)

The runs above leave the servers short of saturation, so they partly measure
the client. Here each server gets one thread and memtier six, so the server is
the bottleneck (~1.00 core used) and ops/s is ops per core. Same VM and
workload as the io_uring section, 1M keys × 32 B, all six servers in one run,
medians of 4 rotated rounds (`bench/docker/saturate.sh`; raw output:
`bench/docker/results-v0.5.txt`):

| Per core | No pipelining | Pipeline 16 | p99, pipeline 16 |
|---|---|---|---|
| **Tantalum 0.5, `--io uring`** | **247k** | **2.51M** | **2.34 ms** |
| Tantalum 0.5, per-thread | 245k | 1.95M | 3.90 ms |
| Valkey 9.1.3 | 225k | 1.77M | 3.96 ms |
| Redis 8.10.3 | 215k | 1.54M | 3.68 ms |
| Dragonfly 2.0.2 (1 thread, io_uring) | 219k | 0.64M | 9.41 ms |
| memcached 1.6.38 (`-t 1`, 1.24 cores) | 211k | 0.77M | 6.61 ms |

The first two rounds were disturbed on the host, mostly in Tantalum's slots:
0.5 per-thread did 1.78M and 1.80M pipelined there, then 2.12M and 2.09M. A
focused paired run right after (0.5 against 0.4.0 only, 6 rounds) was quiet:
0.5 did 2.64M (io_uring) and 2.24M (per-thread) pipelined, 297k and 266k
without, beating 0.4.0 in every pipelined round (+5%) and tying it without
pipelining. Against 0.4.1's numbers from an earlier run, the features and
safety checks of 0.5 cost about 1–3%. Without pipelining every server is
within ~15% of the others, since syscalls dominate. Dragonfly is built to
spread over many cores, so per core isn't where it shines.

How Tantalum got here, from earlier runs on the same VM
(`bench/docker/results-prefetch.txt`):

| Per core | No pipelining | Pipeline 16 | p99, pipeline 16 |
|---|---|---|---|
| **Tantalum 0.4, `--io uring`** | 280k | **2.59M** | **1.57 ms** |
| Tantalum 0.4, per-thread | 251k | 2.13M | 2.32 ms |
| Tantalum 0.3 (no prefetch), `--io uring` | **291k** | 1.77M | 2.37 ms |
| Tantalum 0.3, per-thread | 258k | 1.44M | 4.93 ms |
| Valkey | 225k | 1.80M | 3.10 ms |
| memcached (`-t 1`, 1.25 cores) | 217k | 0.84M | 6.18 ms |

Saturated, io_uring is worth 12–17% per core without pipelining, far more
than the 2–3% of the unsaturated runs. The non-pipelined differences between
0.3 and 0.4 are within this host's run-to-run noise (they went the other way in
an earlier run). With 2 server threads, prefetching took pipelined throughput
from 2.97M to 3.78M (uring) and from 2.61M to 3.56M (per-thread).

With the data shrunk to 10k keys, so everything stays in cache, 0.3 did 3.1M
per core at pipeline 16 against Valkey's 1.85M. That's the headroom
prefetching chips at: 0.4 gets ~80% of it at 1M keys.

0.4.1 then hashes each key once instead of three times, parses short
protocol headers (`$3\r\n`) in one pass instead of searching for the CR
first, and formats reply headers by hand. Against 0.4 in a paired 4-round run:
pipeline 16 went from 2.48M to 2.71M per core with io_uring (+9%) and from
2.09M to 2.27M per-thread (+9%); without pipelining, 288k to 301k (+5%) and
249k to 271k (+9%).

### Earlier results

v0.0.2 Linux runs (`bench/railway/results-network.txt`, `results-loopback.txt`)
and the macOS laptop comparison (`bench/compare.sh`) point the same way for
throughput. Two caveats about them: across Railway's private network the ~2.2 ms round
trip dominated, and on one laptop the client was the bottleneck. Their eviction
hit rates share the seeding flaw described above.

## Not there yet

- **io_uring extras untried**: multishot receive and registered or provided
  buffers, which could also let idle connections hold no read buffer.
- **Beyond 8 threads is unmeasured**: on one 32-core machine, memtier runs out
  of cores before Tantalum does. It needs a separate client machine.
- **Short pipelined batches start cold**: a batch is one connection's read, so
  with pipeline 16 the first few commands' buckets are requested just before
  they're needed. In 0.4's profile at pipeline 16, the entry prefetch step
  (where that wait now lands) was the top function at ~10%, then parsing
  (~10%), the lookup itself (~6%) and reply headers (~5%, since sped up).
  Prefetching across connections' batches could hide more.
- **Per-thread I/O can't rebalance**: connections are dealt out round-robin
  and stay put, so a few very busy clients can load one thread more than the
  others (`--io shared` is the fallback). Accepting is on one thread.
- **Segment pool page faults**: freed blocks beyond the 4 MB hot list drop their
  pages, and new segments fault them back in (~8k faults/s under pipelined
  churn). That costs a few percent, below the hosts' noise. Faults persist
  because, I suspect, freed blocks are mostly older, smaller segment sizes than
  the ones being allocated; a hot list that can split or borrow across sizes
  might remove them.
- **Table resizes still rebuild a whole shard's table** (25% at a time, without reading
  keys): sub-millisecond at today's sizes, but a few ms per shard at 100M keys.
- No `instantaneous_ops_per_sec`, latency histograms or a slow log.
- The server binds to `127.0.0.1` by default. If you expose it, set a password:
  traffic isn't encrypted (no TLS), so keep it on a private network.
- Persistence, replication, other data types, ACL users, Lua, pub/sub,
  `SCAN`/`KEYS`.
- LFU and random eviction policies, `CONFIG SET` at runtime.

## Development

```sh
cargo test                     # unit + end-to-end tests
cargo clippy --all-targets
# Run the tests under AddressSanitizer (needs a nightly toolchain):
RUSTFLAGS=-Zsanitizer=address cargo +nightly test --lib --tests --target aarch64-apple-darwin --target-dir target/asan
# Coverage-guided fuzzing of the parser and every command (cargo install cargo-fuzz):
cd fuzz && cargo +nightly fuzz run commands corpus/commands seeds/commands -- -max_total_time=600
```

`tests/fuzz.rs` runs 300,000 random commands (valid and not) on every
`cargo test`; `fuzz/` drives raw bytes through the parser and command
execution under AddressSanitizer. `bench/docker/soak.sh` runs Tantalum for
minutes under eviction, TTL, counter, big-value and connection churn, then
checks memory and a SIGTERM shutdown with clients still connected.

Linux benchmarks: the root `Dockerfile` bundles Tantalum, Redis 8, Valkey,
memcached, Dragonfly and `memtier_benchmark`, and `bench/railway/run.sh` picks a
role from `ROLE`:

- `ROLE=suite`: the full comparison in `suite.sh` (memory by value size,
  throughput, eviction quality and a churn run). Takes ~25 minutes.
- `ROLE=local`: servers and client in one container over loopback. Optional
  `ROUNDS=2` and `EVICTION_MAXMEMORY=16mb`.
- `ROLE=server` plus `ROLE=bench`: two services, with `SERVER_HOST` pointing the
  bench at the server.

On Railway, create a service, set its variables, and run `railway up --service <name>`.
The results appear in the service logs.
