<h1 align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/brand/readme-banner-dark.svg">
    <img alt="Tantalum" src="assets/brand/readme-banner-light.svg" width="640">
  </picture>
</h1>

A small, fast key/value server in Rust that speaks the Redis protocol (RESP2
and RESP3). It does Redis's string commands for caching (`GET`/`SET`,
`MGET`/`MSET`, `DEL`, `EXPIRE`/`TTL`, `INCR` and friends), hashes, sets,
lists, sorted sets, streams, blocking commands, pub/sub, transactions and
Lua scripts (enough to run [BullMQ](https://bullmq.io), whose jobs cost
the server about a third of the CPU they cost Redis), numbered databases, and
`SCAN`, `KEYS` and `RENAME`, with TTLs, eviction, a password, TLS, optional snapshots
and an append-only log on disk, and replication over Redis' own protocol, so Redis and
Tantalum can follow each other either way. The point: be a faster drop-in for the "Redis as
a cache" use case, using as little memory as possible.

Named after the metal of tantalum capacitors, known for packing the most into
the least space. (It was called ferrokv until v0.2; the raw logs in
`bench/railway/` from before the rename still use that name.)

```sh
cargo run --release -- --port 6379      # --threads N, --shards N, --bind ADDR
cargo run --release -- --maxmemory 2gb --maxmemory-policy allkeys-lru
TANTALUM_REQUIREPASS=s3cret cargo run --release -- --bind 0.0.0.0   # or --requirepass
cargo run --release -- --aclfile users.acl   # users from ACL SETUSER kept across restarts
cargo run --release -- --maxclients 2000 --timeout 300 --tcp-keepalive 60
cargo run --release -- --snapshot data/tantalum.snapshot   # keep data across restarts
cargo run --release -- --aof data/aof   # log every write too: a crash loses about a second
cargo run --release -- --tls-port 6380 --tls-cert-file cert.pem --tls-key-file key.pem
cargo run --release -- --notify-keyspace-events Egx   # keyspace notifications, as Redis' setting
cargo run --release -- --port 6380 --replicaof 127.0.0.1:6379   # a replica (of Redis or Tantalum)
cargo run --release -- --replicaof master.example:6380 --tls-replication yes --tls-ca-cert-file cert.pem
redis-cli SET greeting hello EX 60
redis-cli GET greeting
```

Any Redis client, `redis-cli`, `redis-cli --pipe`, and `redis-benchmark` work as is.

To deploy, `deploy/Dockerfile` builds a small image with just the binary
(`docker build -f deploy/Dockerfile .`); on Railway, set
`RAILWAY_DOCKERFILE_PATH=deploy/Dockerfile` and put options in the start
command. It listens on `::` (IPv4 and IPv6); set a password, and outside a
private network add `--tls-port` with a certificate.

A Railway setup that keeps data and serves both the private network and the
internet: a volume at `/data`, `TANTALUM_REQUIREPASS`, the certificate and
key as `TLS_CERT` and `TLS_KEY` variables, a TCP proxy to port 6380, a
drain time of 15 s (so the final snapshot has time), and the start command

```sh
/bin/sh -c "printenv TLS_CERT > /tmp/cert.pem && printenv TLS_KEY > /tmp/key.pem && chmod 600 /tmp/key.pem && exec tantalum --bind :: --port 6379 --tls-port 6380 --tls-cert-file /tmp/cert.pem --tls-key-file /tmp/key.pem --maxmemory 256mb --maxmemory-policy noeviction --snapshot /data/tantalum.snapshot --aof /data/aof"
```

Railway's redeploy reuses the old deployment's start command; to change it,
deploy the commit again.

Services on the private network use `redis://default:<password>@<service>.railway.internal:6379`
(with ioredis, add `?family=0` for IPv6); outside clients use
`rediss://` to the proxy, pinning the certificate (a self-signed one whose
names include the proxy's hostname works, and no CA has to renew it).

## Commands

Replies match Redis 7 byte for byte, errors included, as far as the tests
check (`tests/server.rs`, and suites such as `tests/hashes.rs` and
`tests/blocking.rs`, whose expected replies were recorded from Redis 7 with
`tools/redis-diff.py`).

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
| `INCRBYFLOAT key n` | In C's `long double`, as Redis, and logged as what it came to |
| `STRLEN`, `APPEND key value`, `GETRANGE key start end`, `SUBSTR`, `SETRANGE key offset value`, `GETSET key value` | `APPEND` and `SETRANGE` change a big string in place, and it grows with room to spare, as in Redis |
| `SETBIT key offset value`, `GETBIT key offset`, `BITCOUNT key [start end [BYTE\|BIT]]`, `BITPOS key bit [start [end [BYTE\|BIT]]]` | On strings. A `SETBIT` in a big bitmap changes it in place, without copying it |
| `BITOP AND\|OR\|XOR\|NOT destkey key [key ...]` | Atomic |
| `BITFIELD key [GET type offset\|SET type offset value\|INCRBY type offset n\|OVERFLOW WRAP\|SAT\|FAIL ...]`, `BITFIELD_RO` | |
| `MSETNX key value [key value ...]` | Atomic: all or none, with nothing else running meanwhile |
| `LCS key1 key2 [LEN] [IDX] [MINMATCHLEN n] [WITHMATCHLEN]` | |
| `COPY source destination [DB db] [REPLACE]`, `TOUCH key [key ...]`, `RANDOMKEY` | `COPY` is atomic, TTL included. `RANDOMKEY` is about uniform |
| `SELECT index`, `MOVE key db`, `SWAPDB index1 index2` | Numbered databases, 16 unless `--databases` says otherwise. `MOVE` copies a big collection rather than handing it over, so it takes time in proportion to its size, holding that key's shard |
| `OBJECT ENCODING\|FREQ\|IDLETIME\|REFCOUNT key`, `OBJECT HELP` | `ENCODING` is what Redis would use for what's held now, built from nothing (see "Not there yet"). `IDLETIME` is always 0: no last-access time is kept per key. `FREQ`, under an LFU policy, is the key's use counter, which eviction goes by: 0 to 3, where Redis' counts on a log scale to 255; under the others it's Redis' error |
| `DUMP key`, `RESTORE key ttl payload [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ n]` | Redis' RDB format, so values move between Redis and Tantalum either way. `RESTORE` reads what Redis 2.4 to 7.2 write (zipmaps, ziplists, quicklists, intsets, listpacks, the three stream formats; not modules' types); `DUMP` writes what Redis 7.2 would, byte for byte for strings, small collections, sorted sets, lists made by appending and streams with nothing deleted (see "Not there yet"). `IDLETIME` and `FREQ` are checked, and kept nowhere |
| `MIGRATE host port key\|"" db timeout [COPY] [REPLACE] [AUTH password\|AUTH2 user password] [KEYS key ...]` | `RESTORE`s the keys there and deletes them here, as Redis: over a connection kept for 10 seconds (`INFO`'s `migrate_cached_sockets`), sending `SELECT` only for another database, trying once more when a kept one was closed. It waits for the other server on its connection's thread, as Redis does on its only one, and is logged as the `DEL` of what it moved |
| `SORT key [BY pattern] [LIMIT offset count] [GET pattern ...] [ASC\|DESC] [ALPHA] [STORE destination]`, `SORT_RO` | Atomic. Lists, sets and sorted sets, `*->field` for hashes |
| `HSET key field value [field value ...]`, `HSETNX`, `HMSET` | Hashes keep their fields in the order they were added (as Redis does for small hashes) and keep the key's TTL |
| `HGET key field`, `HMGET key field [field ...]`, `HGETALL`, `HKEYS`, `HVALS` | `HGETALL` is a map in RESP3 |
| `HDEL key field [field ...]`, `HEXISTS`, `HLEN`, `HSTRLEN`, `HINCRBY key field n`, `HINCRBYFLOAT key field n` | `HDEL` of the last field deletes the key |
| `HRANDFIELD key [count [WITHVALUES]]` | A negative count may repeat fields |
| `HSCAN key cursor [MATCH pattern] [COUNT n] [NOVALUES]` | Every match at once, with cursor 0, as Redis does for a hash it keeps as a listpack; `COUNT` fields at a time for one it would keep as a hash table (over 512 fields, or a field or value over 64 bytes), as Redis pages those. Patterns as in Redis: `*`, `?`, `[a-z]`, `[^a]`, `\` |
| `SADD key member [member ...]`, `SREM`, `SISMEMBER`, `SMISMEMBER`, `SCARD`, `SMEMBERS` | Small sets reply in Redis' order: integers sorted, others in the order added. `SMEMBERS` is a set in RESP3 |
| `SPOP key [count]`, `SRANDMEMBER key [count]` | Random members; a negative `SRANDMEMBER` count may repeat them. A count covering the whole set gives it in Redis' order |
| `SMOVE source destination member` | Atomic: holds both keys' shards |
| `SINTER`, `SUNION`, `SDIFF key [key ...]`, their `STORE` forms, `SINTERCARD numkeys key ... [LIMIT n]` | Each key is read on its own, so unlike Redis, a write to another key meanwhile may or may not be seen |
| `LPUSH`, `RPUSH key element [element ...]`, `LPUSHX`, `RPUSHX` | |
| `LPOP`, `RPOP key [count]`, `LMPOP numkeys key [key ...] LEFT\|RIGHT [COUNT n]` | |
| `BLPOP`, `BRPOP key [key ...] timeout`, `BLMPOP timeout numkeys key ... LEFT\|RIGHT [COUNT n]`, `BLMOVE source destination LEFT\|RIGHT LEFT\|RIGHT timeout`, `BRPOPLPUSH` | Block while every key is empty, for `timeout` seconds (fractions allowed, 0 for ever). Served first come first served, right after the command that gave a key data, as in Redis 7.2 (see Design) |
| `LRANGE key start stop`, `LINDEX`, `LLEN`, `LPOS key element [RANK r] [COUNT n] [MAXLEN m]` | |
| `LSET`, `LINSERT key BEFORE\|AFTER pivot element`, `LREM key count element`, `LTRIM` | |
| `LMOVE source destination LEFT\|RIGHT LEFT\|RIGHT`, `RPOPLPUSH` | Atomic: holds both keys' shards |
| `ZADD key [NX\|XX] [GT\|LT] [CH] [INCR] score member ...`, `ZINCRBY`, `ZREM`, `ZSCORE`, `ZMSCORE`, `ZCARD` | Scores print as Redis 7.2 prints doubles (`1.1`, `1e+21`), and parse as it does (`inf`, hex, no NaN). RESP3 replies doubles |
| `ZRANGE key min max [BYSCORE\|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]`, `ZRANGESTORE`, and `ZRANGEBYSCORE`, `ZREVRANGE`, `ZREVRANGEBYSCORE`, `ZRANGEBYLEX`, `ZREVRANGEBYLEX` | RESP3 gives `WITHSCORES` as pairs, as Redis does |
| `ZRANK`, `ZREVRANK key member [WITHSCORE]`, `ZCOUNT`, `ZLEXCOUNT` | |
| `ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE`, `ZREMRANGEBYLEX` | |
| `ZPOPMIN`, `ZPOPMAX key [count]`, `ZMPOP numkeys key ... MIN\|MAX [COUNT n]`, `ZRANDMEMBER key [count [WITHSCORES]]` | |
| `BZPOPMIN`, `BZPOPMAX key [key ...] timeout`, `BZMPOP timeout numkeys key ... MIN\|MAX [COUNT n]` | As the blocking list commands |
| `ZUNION`, `ZINTER`, `ZDIFF numkeys key ... [WEIGHTS ...] [AGGREGATE SUM\|MIN\|MAX] [WITHSCORES]`, their `STORE` forms, `ZINTERCARD` | Sets count as sorted sets with scores of 1, as in Redis. Each key is read on its own |
| `ZSCAN key cursor [MATCH pattern] [COUNT n]` | As `SSCAN` |
| `PFADD key [element ...]`, `PFCOUNT key [key ...]`, `PFMERGE destkey [sourcekey ...]`, `PFDEBUG GETREG\|DECODE\|ENCODING\|TODENSE key`, `PFSELFTEST` | HyperLogLog, in strings with the same bytes as Redis', sparse then dense (`hll-sparse-max-bytes`) |
| `GEOADD key [NX\|XX] [CH] longitude latitude member ...`, `GEOPOS`, `GEOHASH`, `GEODIST key member member [M\|KM\|FT\|MI]` | Positions as sorted set scores, as in Redis |
| `GEOSEARCH key FROMMEMBER member\|FROMLONLAT longitude latitude BYRADIUS radius unit\|BYBOX width height unit [ASC\|DESC] [COUNT count [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]`, `GEOSEARCHSTORE`, `GEORADIUS`, `GEORADIUS_RO`, `GEORADIUSBYMEMBER`, `GEORADIUSBYMEMBER_RO` | Results in the order Redis gives them; `STORE` and `STOREDIST` make a sorted set |
| `SSCAN key cursor [MATCH pattern] [COUNT n]` | Small sets come whole with cursor 0, as in Redis; big ones `COUNT` at a time, each member there throughout returned at least once |
| `XADD key [NOMKSTREAM] [MAXLEN\|MINID [=\|~] threshold [LIMIT n]] *\|id field value ...` | IDs as Redis makes and reads them (`ms-*` included). Trimming with `~` removes whole nodes where Redis' end, so it removes what Redis does, with `stream-node-max-entries` and `-bytes` as set |
| `XLEN`, `XRANGE key start end [COUNT n]`, `XREVRANGE`, `XDEL key id ...`, `XTRIM key MAXLEN\|MINID [=\|~] threshold [LIMIT n]` | `(` for exclusive ends. An emptied stream stays, keeping its last ID, as in Redis |
| `XREAD [COUNT n] [BLOCK ms] STREAMS key ... id ...` | `$` for new entries only. Blocked readers are all served by an entry that's new to them (reading takes nothing), and `$` means the last ID when the command came |
| `XGROUP CREATE key group id\|$ [MKSTREAM] [ENTRIESREAD n]`, `XGROUP SETID key group id\|$ [ENTRIESREAD n]`, `XGROUP DESTROY key group`, `XGROUP CREATECONSUMER key group consumer`, `XGROUP DELCONSUMER key group consumer`, `XGROUP HELP` | As in Redis, what these do to a stream's groups isn't a change to clients watching the key, unless `MKSTREAM` makes it |
| `XREADGROUP GROUP group consumer [COUNT n] [BLOCK ms] [NOACK] STREAMS key ... id ...` | `>` for what the group hasn't read, an ID for the consumer's own pending entries (a deleted one comes as an ID and a nil). The consumer is made if there's none. Blocked consumers are served longest-waiting first, and get `-NOGROUP` if the group or key goes, `-WRONGTYPE` if the key becomes another kind |
| `XACK key group id ...`, `XPENDING key group [[IDLE ms] start end count [consumer]]` | |
| `XCLAIM key group consumer min-idle id ... [IDLE ms] [TIME unix-ms] [RETRYCOUNT n] [FORCE] [JUSTID] [LASTID id]`, `XAUTOCLAIM key group consumer min-idle start [COUNT n] [JUSTID]` | Entries gone from the stream leave the pending lists; `XAUTOCLAIM` lists them third, as Redis 7 does |
| `XINFO STREAM key [FULL [COUNT n]]`, `XINFO GROUPS key`, `XINFO CONSUMERS key group`, `XINFO HELP`, `XSETID key id [ENTRIESADDED n] [MAXDELETEDID id]` | Lag and `entries-read` by Redis' rules (null when deletions make them unknowable). `radix-tree-keys` and `-nodes` are what Redis' tree would hold, worked out from the chunks on each call |
| `TYPE key` | `string`, `hash`, `list`, `set`, `zset`, `stream` or `none`. Commands for one type reply `-WRONGTYPE` to the others, and `MGET` gives nil for them, as in Redis |
| `SCAN cursor [MATCH pattern] [COUNT n] [TYPE type]` | A whole scan returns every key that existed throughout exactly once (Redis may repeat keys), and any cursor is safe. `COUNT` is about how many keys a call looks at, as in Redis; for big shards a call looks at a 1024th of the shard at least. Cursors are Tantalum's own and stay under 2^53, so clients that read them as JavaScript numbers work |
| `KEYS pattern` | Every match at once, like Redis: it holds each shard's lock in turn while reading it, so prefer `SCAN` on big stores |
| `RENAME key newkey`, `RENAMENX key newkey` | Atomic across shards; keeps the type and TTL. As in Redis, it overwrites `newkey` of any type and ignores `maxmemory` |
| `FLUSHALL [ASYNC\|SYNC]`, `FLUSHDB [ASYNC\|SYNC]` | Not atomic across shards: writes running at the same time may survive |
| `AUTH [username] password` | The `default` user's password is `--requirepass` (or `TANTALUM_REQUIREPASS`); other users are made with `ACL SETUSER` |
| `ACL SETUSER\|GETUSER\|DELUSER\|LIST\|USERS\|WHOAMI\|CAT\|DRYRUN\|LOG\|GENPASS\|LOAD\|SAVE\|HELP` | Redis 7.2's users: passwords, `on`/`off`, commands, categories and subcommands (`+@all -@dangerous +config\|get`), key patterns with read and write (`~app:*`, `%R~cache:*`), channels, selectors in parentheses, and how `ACL LIST` and `GETUSER` write them back, each byte as Redis' (see Design). Rules may name every command Redis 7.2.4 has, those missing here too, so an ACL file from Redis loads. `ACL LOAD` and `SAVE` use `--aclfile`, which is also loaded at startup |
| `HELLO [2\|3 [AUTH user pass] [SETNAME name]]` | RESP3 too: redis-py 8 won't connect without it. For these commands it only changes nulls, maps (`HELLO`, `CONFIG GET`, `HGETALL`), sets (`COMMAND` flags) and `INFO` (verbatim text) |
| `CLIENT SETNAME\|GETNAME\|ID\|SETINFO` | What client libraries send on connect |
| `CLIENT LIST [TYPE type\|ID id ...]`, `CLIENT INFO` | Redis 7.2's line per connection: id, addresses, name, age, database, library, protocol. What isn't tracked per connection (idle time, buffers, other connections' last command and how many subscriptions) reads as 0, `N` or `NULL` |
| `CLIENT KILL addr:port`, `CLIENT KILL [ID id] [TYPE type] [USER name] [ADDR addr:port] [LADDR addr:port] [SKIPME yes\|no]` | Closes other connections at once, and the caller's own after its reply |
| `COMMAND [COUNT\|INFO\|LIST\|DOCS\|GETKEYS\|GETKEYSANDFLAGS]` | Each entry is Redis 7.2.4's own, ACL categories, key specifications, tips and documentation included (`tools/command-table.py`). `COUNT` and `LIST` say what's here, not what Redis has |
| `CONFIG GET pattern ...`, `CONFIG SET name value ...`, `CONFIG RESETSTAT` | Every Redis 7.2.4 setting reads as its default, and `CONFIG SET` refuses what Redis refuses, with its words (bounds, sizes and choices generated from a real Redis). These act: `maxmemory`, `maxmemory-policy`, `maxclients`, `timeout`, `tcp-keepalive`, `requirepass`, `acllog-max-len`, `acl-pubsub-default`, `slowlog-log-slower-than`, `slowlog-max-len`, `latency-monitor-threshold`, `latency-tracking`, `latency-tracking-info-percentiles`, `notify-keyspace-events`, `stream-node-max-entries` and `-bytes` (these two not with `--aof`), and replication's: `masterauth`, `masteruser`, `repl-backlog-size`, `repl-backlog-ttl`, `repl-timeout`, `repl-ping-replica-period`, `min-replicas-to-write`, `min-replicas-max-lag`, `replica-announce-ip` and `-port`, `replica-priority`, `replica-serve-stale-data`. The rest are remembered and do nothing, except that what can't be done is refused rather than remembered: `appendonly` other than as started, `save` with `--snapshot`, and `replica-read-only no`. `CONFIG REWRITE` says there's no config file |
| `MONITOR` | Every command run, as Redis' lines show them: what's secret `(redacted)`, a script's own commands as `lua`, admin commands left out. It costs commands nothing until someone runs it (see Design) |
| `SLOWLOG GET [count]\|LEN\|RESET\|HELP` | As Redis', with what's secret `(redacted)`, and the commands inside `EXEC` and scripts too. At its default of 10 ms and above, durations are in 5 ms ticks, and commands Redis flags `fast` aren't timed (see Design); below that, a real clock as in Redis |
| `LATENCY LATEST\|HISTORY\|RESET\|GRAPH\|DOCTOR\|HELP`, `LATENCY HISTOGRAM [command ...]` | With `latency-monitor-threshold`, spikes in `command` and `fast-command`, as Redis keeps them. `HISTOGRAM` is each command's latencies, in Redis' steps, counted from the first time something asks for command statistics (see "Command statistics") |
| `MEMORY USAGE key [SAMPLES n]\|STATS\|DOCTOR\|PURGE\|MALLOC-STATS\|HELP` | `STATS` has Redis' fields; the allocator's are what's in use and the process's RSS |
| `WAIT numreplicas timeout`, `WAITAOF numlocal numreplicas timeout` | `WAIT` replies once that many replicas have acknowledged what was written before it, or at the timeout with how many have (asking them at once with `REPLCONF GETACK`), as Redis does; on a replica it's Redis' error. `WAITAOF 1` syncs the log at once with `--aof`, and is refused without; replicas don't count toward it |
| `REPLICAOF host port`, `REPLICAOF NO ONE`, `SLAVEOF`, `ROLE` | Follow a master, Redis or Tantalum, over Redis' protocol (see "Replication" in the design notes), or stop following and keep the data, which then goes on under a new replication ID with the old one kept, so the other replicas can continue from here. `ROLE` as Redis 7.2 replies it, either side |
| `SYNC`, `PSYNC replid offset`, `REPLCONF listening-port\|ip-address\|capa\|ack\|getack ...` | What a replica sends its master (Redis' or Tantalum's): this server sends the data as a Redis RDB file and the stream after it, or only what came since, if its backlog still has that. On the plain port or the TLS one; a replica connects over TLS with `--tls-replication yes` and `--tls-ca-cert-file` (the master's CA, or its self-signed certificate, pinned). Replicas are in `CLIENT LIST` (flag `S`, `cmd=psync`) and `CLIENT KILL TYPE replica` closes them |
| `DEBUG POPULATE\|SLEEP\|ERROR\|OBJECT\|SET-ACTIVE-EXPIRE` | With `--enable-debug-command yes` or `local`, as in Redis |
| `LOLWUT`, `MODULE LIST` | No modules |
| `CLIENT PAUSE timeout [WRITE\|ALL]`, `CLIENT UNPAUSE` | Holds back what Redis holds back, for as long, and those it holds run after in order. Nothing expires by itself meanwhile |
| `CLIENT UNBLOCK id [TIMEOUT\|ERROR]`, `CLIENT REPLY ON\|OFF\|SKIP`, `CLIENT NO-EVICT`, `CLIENT NO-TOUCH` | `NO-EVICT` and `NO-TOUCH` are shown in the client list and do nothing else: clients aren't evicted, and keys hold no access times to protect |
| `CLIENT TRACKING ON\|OFF [REDIRECT id] [PREFIX prefix ...] [BCAST] [OPTIN] [OPTOUT] [NOLOOP]`, `CLIENT CACHING YES\|NO`, `CLIENT GETREDIR`, `CLIENT TRACKINGINFO` | Client-side caching, as Redis 7.2 has it: invalidations as RESP3 pushes, or as messages on `__redis__:invalidate` to the connection a RESP2 client redirects to; `BCAST` prefixes, `OPTIN`, `OPTOUT`, `NOLOOP`, a null after a flush, `tracking-table-max-keys`, and the `t`, `R` and `B` flags and `redir=` in the client list. The table is by database (see the design notes) |
| `SAVE`, `BGSAVE`, `LASTSAVE` | With `--snapshot`: save now (blocking the caller), save on another thread, and when the last save succeeded |
| `BGREWRITEAOF` | With `--aof`: rewrites the log on another thread (a new base of the data as it is, and a new, empty log after it). Without `--aof` it's an error, where Redis would write a log anyway |
| `INFO [section ...]`, `DBSIZE` | `INFO` gives the sections asked for, as Redis does, with the fields clients and monitoring tools read: `arch_bits`, `run_id`, `role`, `cluster_enabled`, `redis_version` (7.2.4, the replies' compatibility level), `uptime_in_seconds`, `connected_clients`, `maxclients`, `blocked_clients`, `used_memory` (with a breakdown: `segment_bytes`, `segment_live_bytes`, `external_bytes`, `table_bytes`, `mem_clients_normal`), `used_memory_rss` (Linux), `maxmemory`, `maxmemory_policy`, `total_connections_received`, `total_commands_processed`, `rejected_connections`, `sync_full`, `sync_partial_ok`, `sync_partial_err`, `keyspace_hits` and `keyspace_misses` (from `GET`, `MGET`, `GETEX`, `GETDEL` and the hash reads), `pubsub_channels`, `pubsub_patterns`, `pubsubshard_channels`, `tracking_clients`,
`tracking_total_keys`, `tracking_total_items`, `tracking_total_prefixes`, `total_error_replies`, `evicted_keys`, `evicted_clients` (0: clients aren't evicted), `expired_keys`, `native_scripts` and `native_script_fallbacks` (BullMQ script calls run natively, and handed back to Lua), a Replication section as Redis 7.2's (`role`, the master's link and offsets, `connected_slaves` and a `slaveN` line each, `master_replid` and `master_replid2`, the backlog's extent), a Persistence section (`rdb_changes_since_last_save`, `rdb_last_save_time`, `rdb_last_bgsave_status`, ..., and `aof_enabled`, with `--aof` also `aof_rewrite_in_progress`, `aof_last_bgrewrite_status`, `aof_last_write_status`, `aof_current_size`, `aof_base_size`) and the keyspace line; `commandstats`, `errorstats` and `latencystats` as Redis has them, counted from the first time something asks (see "Command statistics") |
| `SUBSCRIBE`, `PSUBSCRIBE`, `SSUBSCRIBE channel\|pattern [...]`, `UNSUBSCRIBE`, `PUNSUBSCRIBE`, `SUNSUBSCRIBE [...]` | Messages are pushes in RESP3. Subscribed in RESP2, a client may only run these, `PING`, `QUIT` and `RESET`, as in Redis. Shard channels are a namespace of their own (one server is one shard) |
| `PUBLISH channel message`, `SPUBLISH` | Replies how many got it; a client subscribed both by channel and by pattern gets it twice. One publishing to its own channel gets the reply, then the message, as from Redis 7.2 |
| `PUBSUB CHANNELS [pattern]\|NUMSUB [channel ...]\|NUMPAT\|SHARDCHANNELS\|SHARDNUMSUB\|HELP` | |
| `MULTI`, `EXEC`, `DISCARD` | `EXEC` runs the queued commands with the whole store to itself, so no other client's command runs in between, as in Redis. A command refused while queueing (unknown, wrong arity, `SAVE`) makes `EXEC` refuse them all; errors while running are among its replies. Blocking commands inside reply at once, as if timed out |
| `WATCH key [key ...]`, `UNWATCH` | `EXEC` replies nil if a watched key changed meanwhile: written, deleted, expired or evicted (by anyone, the watcher too), as in Redis 7.2 |
| `TIME` | |
| `EVAL script numkeys [key ...] [arg ...]`, `EVALSHA sha1 ...`, `EVAL_RO`, `EVALSHA_RO` | Lua scripts on Redis 7.2.4's own Lua and libraries (`cjson`, `cmsgpack`, `struct`, `bit`), with its sandbox and `redis` API (`call`, `pcall`, `error_reply`, `status_reply`, `sha1hex`, `log`, `setresp`, ...). A script runs with the whole store to itself, as in Redis. Replies, conversions and errors (with `script: <sha1>, on @user_script:<line>.`) as Redis 7.2.4's; `#!lua flags=no-writes` and the `_RO` forms refuse writes |
| `SCRIPT LOAD\|EXISTS\|FLUSH\|KILL\|HELP` | `KILL` stops a busy script that hasn't written, from a connection on another thread. `SCRIPT DEBUG NO` only: no Lua debugger |
| `FUNCTION LOAD [REPLACE] code`, `FUNCTION LIST [LIBRARYNAME pattern] [WITHCODE]\|DELETE\|FLUSH\|DUMP\|RESTORE\|STATS\|KILL\|HELP`, `FCALL function numkeys [key ...] [arg ...]`, `FCALL_RO` | Redis 7's function libraries (`#!lua name=...` and `redis.register_function`), in a Lua state of their own set up as Redis', with its flags (`no-writes`, `allow-oom`, ...), errors (`script: <function>, on @user_function:<line>.`) and limits (a library's top level gets only the library API, for at most 500 ms). `DUMP`'s payload is Redis' RDB format, LZF and CRC-64 included, so libraries move between Redis and Tantalum. Kept in snapshots and the log; `FLUSHALL` leaves them, as in Redis. `LIST` gives libraries by name, Redis in its hash tables' order |
| `PING [msg]`, `ECHO msg`, `QUIT`, `RESET` | `RESET` drops subscriptions, a transaction, watched keys, RESP3 and the name, goes back to database 0, and with a password, drops authentication |

Anything else gets `-ERR unknown command ...` and the connection stays open.
With a password set, every command except `AUTH`, `HELLO` and `QUIT` gets
`-NOAUTH` until the client authenticates.

## Real frameworks' own tests

These are each project's own test suites, unchanged, run against Tantalum and against
Redis 7.2.4 side by side on Agent37 VMs. Each server is fresh and at the address the suite
expects, and results are compared test by test (`bench/frameworks/`, whose `results.txt`
has the details):

| Suite | What ran | Redis 7.2.4 | Tantalum |
|---|---|---|---|
| Sidekiq 8.1.7 | All of it, the dashboard's too | 749 pass | 749 pass |
| Rails 8.1.4.1 | Cache store (681), Action Cable's Redis adapter (25), Active Job with real Sidekiq (12) and Resque (8) workers | 726 pass | 726 pass |
| Laravel 13.40.0 | Every test file that names Redis, with phpredis and with Predis | 1,363 pass, 19 skipped | the same |
| Celery 5.7.0 | Integration tests, Redis as broker and as result backend | 394 pass, 5 fail, 10 skipped | the same, the same 5 |
| Kombu 5.7.0 | Its Redis integration tests | 123 pass | 123 pass |
| BullMQ 6.5.4 | All of it | 1,111 pass, 3 skipped | the same |

Celery's 5 fail on Redis as well, so they say nothing of the server. Laravel's, Celery's and
Kombu's counts are of test names, so a test run several times under one name counts once;
the others are the runners' own totals. Rails pins the `json` gem below 3, as Resque's workers
can't read their jobs otherwise, with either server. These suites are what brought numbered
databases (Laravel's tests use databases 5 and 6, Action Cable's and Resque's 12, Sidekiq's
sharding 6), `CLIENT KILL` and the `INFO` fields Sidekiq's dashboard reads.

To run them, on a machine with Docker: `bench/frameworks/fetch.sh`, then
`bench/frameworks/run.sh setup SUITE` once, `run.sh SUITE redis`, `run.sh SUITE tantalum`
and `compare.py` on the two result folders.

Client libraries' own suites try far more commands, and show what's still missing. Tests that
pass on Redis 7.2.4 and fail on Tantalum, by library (`bench/frameworks/results-clients.txt`):

| Suite | Pass on Redis | Lost on Tantalum |
|---|---|---|
| redis-py 7.4.0 | 2,357 | 2 |
| Predis 3.6.1 | 4,525 | 5 |
| node-redis 6.3.1 | 1,486 | 2 |
| redis-rb 6.0.0 | 893 | 0 |
| ioredis 5.11.0 | 566 | 0 |
| phpredis 6.3.0 | 203 | 2 |

That's 11 of 10,530 (180 before users and ACLs came in 0.27.0, 99 before functions in 0.28.0,
60 before `DUMP`, `RESTORE` and `MIGRATE` in 0.29.0, 41 before client-side caching in 0.30.0,
19 before command statistics in 0.31.0), each listed under "Not there yet".
`tools/admin-diff.py` compares the admin commands with Redis 7.2.4 directly: 3,412 commands
and checks, every command's `COMMAND` entries and every
setting among them, no differences.

Redis' own test suite (its `tests/`, all 80 files but cluster mode's, run by its tcl
harness) is the strictest of all, as it also checks internals: encodings, replication, log
and snapshot files, logs. `bench/redis-tests/` runs it on Tantalum and on Redis 7.2.4 both
ways the harness can: against one server per file (as Redis' CI runs external servers),
where Tantalum 0.31.0 passed 1,651 of the 1,991 tests Redis passed (83%); and starting its
own servers from config files (through `redis-server-wrap.py`), 1,911 of 2,777 (69%). What
was lost, by what it needs (`results.txt` has the details), against one server:

| What it needs | Lost |
|---|---|
| `OBJECT ENCODING` as Redis reports it, and the `*-max-listpack-*` settings acting (50 of these are `SORT` tests that never ran: their file stops at an encoding check outside any test) | 101 |
| `unit/scripting`'s tests after its busy-script ones, which didn't run | 42 |
| Replication (most read what a command propagates, connected as a replica): none lost since 0.33.0, run again on its own machine, external mode | 35 |
| `CONFIG SET appendonly` at runtime | 29 |
| RDB files: `SAVE` without `--snapshot`, `DEBUG RELOAD` | 22 |
| Keyspace notifications (14 pass since 0.32.0, which also sends what subscribing says to a client with `CLIENT REPLY OFF`, as Redis does) | 15 |
| `MULTI` refusing a write when out of memory, and the transaction an earlier failure leaves open | 11 |
| Read-only scripts killed past `busy-reply-threshold`, which hang | 10 |
| `CLIENT PAUSE`d clients counted as blocked, as the tests wait for | 10 |
| `DEBUG DIGEST`, `DIGEST-VALUE`, `PROTOCOL`, `REPLYBUFFER` | 7 |
| Inline commands with unbalanced quotes, `WATCH` of expired keys, and small `INFO`, `CLIENT LIST` and `COMMAND LIST` fields | 58 |

## Design

- **Watching commands** (`Context::special`, `src/slowlog.rs`, `src/latency.rs`): `MONITOR`,
  `CLIENT PAUSE`, the latency monitor and a `SLOWLOG` threshold under 10 ms all need something
  done around every command. Connections read one flag once a batch, beside the numbered
  database check, and while it's on, their commands take the restricted path, where those
  checks are. So they cost nothing until someone asks for them. At the default of 10 ms the
  slow log doesn't need that: commands Redis flags `fast` aren't timed, and the others read a
  clock a thread ticks every 5 ms, which is two loads, so a command is logged once it has
  surely taken that long, with its time in ticks. That thread takes about 0.5% of a core while
  commands run, and stops when none has for a second, so an idle server isn't woken for it.
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
  rebalanced across threads. A connection that ran up to 16 commands doesn't
  send its replies: it copies them to the thread's outbox and goes back to waiting,
  and the thread's flusher task, once the other ready connections have run,
  sends what's there in one tight loop, as Redis sends replies just before it
  waits for more. Waking a client took the kernel more time than the request
  itself, and four things keep clients awake or the batch whole. The reactor is
  looked at only when the thread runs out of work (tokio's default was every
  61 task polls, about 30 connections of the hundreds that were ready). No task
  switch comes between sends, which had let client threads go back to sleep
  between replies. Replies go in socket order: a client's connections were
  mostly accepted together, so its replies come in one run, where in arrival
  order they alternated with those of its other threads. That took wakes per
  request from 0.22 to 0.017 (Redis: 0.08, io_uring: 0.03). And before a batch
  of 8 or more goes, the flusher looks at the reactor once more (twice if the
  first found anything), so what came in meanwhile joins it: the clients served last last time answer just too late
  for the look that started a cycle, and would wait all of it out (p99 at twice
  the median; with the look, just above it, and every connection is served
  once per cycle wherever it comes in the order). The buffers a connection
  used go straight to the next one, still in the CPU's cache, instead of being
  freed and allocated per request. Paired against 0.19.0, on EC2: +11–13% at 1
  to 4 threads with p50 and p99 both down about 10%; at 8 threads (30 clients
  each) +6–7% with p50 down 5%, but p99 up from 0.38–0.43 to 0.49–0.52 ms.
  Holding short batches' replies too gained 9–15% on one thread (Agent37: SET
  at pipeline 4 and 16, and the 1:10 mix at 16), and about nothing at two, with
  SET's p99 at pipeline 16 from 2.76 to 4.14 ms. Longer batches send at once:
  held, 32 commands gained 2–4% with a p99 worse than Redis'.
  On a busy thread (the last send had 8 replies or more), connections read
  and start loading their first keys' buckets, then run once all that are
  ready have read. The lookups saved about pay for the second poll, so
  throughput stays where it was, but p99 fell 6–23% (Agent37, one thread; SET at
  pipeline 16 about even). Loading the entries too moved the wait rather than
  saved it, and came out up to 3% slower.
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
  per-thread instead (and says so if `--io uring` was asked for). A read
  completes as soon as anything arrives, where an epoll thread reads what
  has gathered by the time it gets to the socket: BullMQ's streams of
  pipelined commands came in 2 at a time against 20, each batch's reply
  waking the client, 18% more server time per job. So before running a
  batch, a connection lets the thread's others run and takes what came
  meanwhile with one non-blocking `recv`; for clients that send in bursts
  or one command at a time nothing does, and after a miss it tries only
  every 2, 4, ... 256 batches until it gets something.
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
  has-TTL, external, 2-bit usage counter, and 3 bits for its type), varint key
  and value lengths, an optional varint deadline, then key and value. A 15-byte
  key with a 32-byte value takes 50 bytes.
- **Hashes** (`src/hash.rs`) are one entry like a string while small, with the type bits
  set and the fields in its value: a field count, then each field and value
  with a varint length (one byte below 128), in the order they were added.
  So a hash costs what its bytes do: 100k hashes of ten 6-byte fields with
  16-byte values take 27.5 bytes per field in all (Redis 7.0: 34.7, measured
  with `used_memory` on macOS). Reads scan the fields; writes build the new
  value and replace the entry, or overwrite it where it is when the length
  is unchanged (a field set to a value of the same length). Strings pay
  nothing for this: the type was already in the flags byte, so `GET` and
  `SET` read the same bytes, and the speed guard measured no difference.
  Past 512 fields or 8 KB a hash becomes an object the shard keeps and changes in
  place: its fields and values back to back, a list of their 5-byte addresses (the
  order added, and positions for `HRANDFIELD` and `HSCAN`), and a hash table of
  positions, all in small pieces so that it never stops to grow (see below). A write
  costs the same at any size. A value that fits is written where it is and a longer one
  goes at the end. Removing a field keeps the order up to 512 fields, where Redis'
  replies show it, and past that moves the last field into its place. In real memory,
  the growth of RSS once settled, a field of 16 bytes with a 32-byte value takes
  62.9-71.8 bytes from 500,000 to 8M fields (Redis 70.7-78.6, Valkey 70.1-80.5), ahead
  of both at every size. On one core, in one hash of 1M fields, `HGET` and `HSET` do
  9-10% more than Redis and 10-15% more than Valkey one at a time, and 38-40% and
  36-52% more at pipeline 16 (`bench/agent37/results-stops.txt`). A snapshot holds a
  big hash as bytes as above, still version 2, so a build from before big hashes reads it.
- **Numbered databases cost database 0 next to nothing.** Each database is a store of
  its own, made when first selected (about 160 KB of empty shards), with its own tables
  and segments, so a key takes no byte more for being able to be in one. They all use
  database 0's shard locks: what holds a shard holds that shard of every database, so
  `EXEC`, scripts, `MOVE` and `SWAPDB` hold just what they did, however the databases are
  used, and can't deadlock between them. A command on database 0 pays one comparison for
  its database and one for each lock, about 12 instructions a command (17 at pipeline 1, which
  also checks once a batch; line by line in `bench/frameworks/results.txt`), which doesn't show in
  its cycles or throughput; a key takes the same in any database (60 bytes of RSS with a 32-byte
  value, either way), and is as fast. They share one memory limit, and eviction takes from
  any of them, as in Redis. A snapshot marks where each database other than 0 starts,
  which only then makes it version 6 (one with just database 0 stays readable by older
  builds), and the append-only log has a `SELECT` wherever the database changes.
- **Big hashes, sets and sorted sets don't stop to grow** (`src/big.rs`). One buffer and
  one table would have to grow all at once, copying the buffer and reading every field
  again to hash it: 658 ms at 8M fields, under the shard's lock. So the bytes are in
  chunks of up to 4 MB and the list of addresses in pages of 64K, which stay where they
  are, and the table of positions is many small ones, each splitting in two when full:
  about 14,000 positions to read again, fetched ahead of time, in under a millisecond.
  Tidying up works the same way: once holes pass half of all, each write that leaves one
  takes four more places in the list and moves the record there if it's in a chunk with
  more holes than not, and a chunk of nothing but holes goes back. Filling one to 8M, the
  longest batch of 500 commands took 2.7 ms for a hash (658 before; Redis 2.8, Valkey
  2.4), 4.2 for a set (884; 2.8, 4.1) and 5.0 for a sorted set (865; 5.2, 4.4). Removing
  three in four took 3.4 (232; 3.7, 2.8), 3.7 (98; 3.0, 1.9) and 8.1 (102; 5.7, 7.9).
  It costs a few more loads per command, of small arrays: `HGET` on 4M fields takes the
  same instructions and 2-6% more cycles of the server's own, under the noise end to end
  at 1M items. Filling is faster for it (8% on a hash, 13% on a sorted set), with half
  the page faults. `used_memory` now says what's real (62.3 bytes a field, RSS 63.3), and
  a fill no longer needs old and new buffers at once (RSS right after 3M fields: 111.8
  bytes a field before, 63.3 now). Tables don't merge again when a collection shrinks:
  a few dozen bytes each, gone with the key (`bench/agent37/results-stops.txt`).
- **Sets** (`src/set.rs`) are packed in their entry while small, the way
  Redis packs them, and so reply in its order: up to 512 integers, sorted, as
  zigzag varints (Redis' intset), or up to 128 members of up to 64 bytes in
  the order added (its listpack). A write decodes, changes and writes the
  entry again. Past those limits a set becomes an object the shard keeps
  (`src/collection.rs`) and changes in place: its members back to back, a
  list of their addresses (the set's order, so `SPOP` and `SRANDMEMBER` pick
  in O(1) and `SSCAN` walks positions), and a hash table of positions, in
  small pieces as above. Measured with `used_memory` against Redis 7.2.4 (Docker):
  100k sets of ten 8-byte members take 12.5 bytes per member (Redis 17.8),
  and one set of 1M take 23.0 (Redis 29.3). In real memory, the growth of RSS
  once settled, a set of 32-byte members takes 45-54 bytes each from 500,000 to
  8M members (Redis 55-64, Valkey 54-65), ahead of both at every size measured
  (`bench/agent37/results-stops.txt`). Entries of object kinds use the
  external bit to point into the shard's object table, so strings and
  hashes read the same bytes as before.
- **Lists** (`src/list.rs`) are packed while small (up to 128 elements in
  4 KB): a count, then each element's length, bytes, and length again
  written backwards, so the list reads from either end, as Redis'
  listpacks do. A bigger list is chunks of up to 8 KB in that format (like
  Redis' quicklist): pushes and pops at either end touch one chunk, and an
  index walks chunk counts from the nearer end. Against Redis 7.2.4: 100k
  lists of ten 8-byte elements take 13.4 bytes per element (Redis 17.8),
  one of 1M take 9.0 (Redis 9.0). In-process on 200k elements, `LPUSH`
  ~500 ns, `RPOP` ~290 ns, `LINDEX` ~760 ns, `LRANGE -10 -1` ~530 ns.
- **Streams** (`src/stream.rs`) are kept in chunks that end exactly where
  Redis would start a new listpack node: after 100 entries (counting ones
  removed from it), or when its listpack would pass 4 KB. Each chunk counts
  the bytes Redis' listpack would take, by its encoding rules (integers,
  string lengths, the master entry, `SAMEFIELDS`), because trimming with `~`
  removes whole nodes: 118,000 random XADD/XDEL/XTRIM/XRANGE/XREAD commands,
  values from empty to 4 KB, gave the same replies from Redis 7.2.4. The
  entries themselves are stored more compactly than a listpack: varint IDs
  relative to the chunk's first, varint lengths, field names left out when
  they're the chunk's first entry's. A chunk that's done is copied to an
  exact fit (asked to shrink by less than half, mimalloc keeps the block, so
  `shrink_to_fit` had left every chunk in a 5,120-byte one and `used_memory` a
  third below what the process really took), and the next starts about that
  size. In real memory, the growth of RSS over 5M entries, an entry of one
  32-byte value takes 38.3 bytes (Redis 8.10.3 44.0, Valkey 9.1.3 44.2), of one 8-byte
  value 13.8 (21.6, 21.7). A stream is always an object (`collection.rs`);
  string entries pay nothing for it.
- **Consumer groups** (`src/stream.rs`, `src/cmd/group.rs`) keep groups and
  consumers by name, as Redis' radix trees list them, and a group's pending
  entries by ID, each consumer's too, in sorted leaves of 64 under a tree of
  where they start (`src/id_map.rs`). Entries delivered in order fill each leaf,
  where a B-tree's nodes stay half empty, and each knows its consumer by a
  number, so a pending entry takes 61.2 bytes of RSS, 10 consumers or one
  (Redis 242.3, Valkey 126.2). Entries that don't come in order split a leaf,
  and leaves left nearly empty join a neighbour. A stream without groups holds
  no memory for them and
  takes the same path as before; other kinds' objects stay the size they were
  (a test keeps it so). 225,000 commands against Redis 7.2.4 gave the same
  replies, times within a tolerance, over every argument error in both
  protocols and random runs across chunks, `XDEL`, `XTRIM`, `XSETID`, `COPY` and
  `RENAME` (`tools/groups-diff.py`, whose replies without times are replayed by
  `tests/groups.rs`); 44 scenarios with blocked clients, `WATCH`, `MULTI` and
  scripts too (`tools/groups-blocking-diff.py`). One Agent37 core, memtier
  3 x 80 clients, all servers up, in commands per second (raw rows and how:
  `bench/agent37/results-groups.txt`):

  | | Pipeline | Redis 8.10.3 | Valkey 9.1.3 | Tantalum | vs Redis | p99, Redis / Tantalum |
  |---|---|---|---|---|---|---|
  | `XADD` | 1 | 245,087 | 246,770 | 304,380 | +24% | 1.91 / 1.29 ms |
  | | 16 | 997,990 | 980,581 | 2,300,654 | +131% | 6.35 / 2.81 ms |
  | `XREADGROUP` `COUNT 1` | 1 | 142,518 | 147,048 | 251,679 | +77% | 3.27 / 1.45 ms |
  | | 16 | 289,085 | 305,254 | 761,258 | +163% | 17.28 / 5.89 ms |
  | `XREADGROUP` `COUNT 10` | 1 | 82,430 | 63,640 | 200,498 | +143% | 4.96 / 1.74 ms |
  | | 16 | 103,083 | 80,287 | 342,288 | +232% | 51.97 / 16.13 ms |
  | `XACK` | 1 | 281,264 | 284,264 | 348,313 | +24% | 1.68 / 1.05 ms |
  | | 16 | 1,655,401 | 1,471,894 | 2,525,810 | +53% | 3.24 / 2.58 ms |
  | `XPENDING` | 1 | 202,725 | 195,900 | 314,098 | +55% | 2.19 / 1.16 ms |
  | | 16 | 879,803 | 800,729 | 2,419,144 | +175% | 5.81 / 2.63 ms |
  | `XPENDING` of 10 rows | 1 | 163,798 | 153,748 | 250,953 | +53% | 2.70 / 1.47 ms |
  | | 16 | 379,690 | 313,722 | 733,710 | +93% | 16.06 / 8.56 ms |
  | `XINFO GROUPS` | 1 | 263,848 | 254,163 | 310,802 | +18% | 1.76 / 1.09 ms |
  | | 16 | 1,192,991 | 1,071,877 | 1,833,924 | +54% | 4.85 / 3.45 ms |

  With one client, so counting only what a server spends, 0.22.0 took 55-76% less
  CPU per command than Redis on each of these and on `XCLAIM`, `XAUTOCLAIM`, a
  consumer's history and a read-and-acknowledge cycle; since then, on x86-64,
  `XREADGROUP` of 10 takes 30% fewer instructions and `XACK` 11% fewer. A client blocked in
  `XREADGROUP`
  registers with the clients watching its key, which writes check for anyway,
  so it wakes when the key changes without `GET` or `SET` paying for it. Their
  fast paths don't look for ready keys, though, so a stream overwritten by `SET`,
  or expired on `GET`, tells a blocked consumer with the next other command, or
  from the expiry thread, within 100 ms; the reply is the same.
- **Sorted sets** (`src/zset.rs`) are packed while small (up to 128
  members of up to 64 bytes, Redis' listpack limits), in order, each entry
  a score and a member; a whole score below 2^52 is a varint of a byte or
  two, any other 8 bytes. A bigger one keeps its members as a big set does,
  a score per position, and its order as chunks of positions with a Fenwick
  tree of chunk sizes, so ranks are O(log n); a search reads a member only
  where scores tie. Against Redis 7.2.4: 100k sorted sets of ten 8-byte
  members take 13.4 bytes per member with whole scores (Redis 19.4) and
  22.3 with fractional ones (Redis 25.8); one of 1M takes 36.4 (Redis 93.4,
  its skiplist and dict). In-process on 500k members: `ZADD` of a new
  member ~790 ns, `ZINCRBY` ~1.3 µs, `ZSCORE` ~400 ns, `ZREVRANK` ~1 µs,
  `ZREVRANGE 0 9 WITHSCORES` ~1 µs, `ZREM` ~1.6 µs. Scores reply exactly
  as Redis 7.2 prints them, by the same `fpconv_dtoa` (`vendor/fpconv`).
- **HyperLogLog** (`src/hll.rs`, `src/cmd/hll.rs`) follows Redis 7.2.4's
  `hyperloglog.c`, to the same strings byte for byte, sparse and dense, so `GET`
  and `SET` move them between the two. Two things differ in how. A sparse one a
  key goes on adding to gets a place of its own with some room, as Redis' string
  has, rather than being written to the log anew each time, and scanning its
  opcodes looks each one's span up in a table: which kind comes next can't be
  predicted, and Redis' branches on it were most of `PFADD`'s time. `PFCOUNT`'s
  cache in the string isn't logged, as it's only ever rebuilt.
- **Geo** (`src/geo.rs`, `src/cmd/geo.rs`) keeps positions as sorted set scores
  and searches as Redis 7.2.4's `geo.c` does, in the same boxes in the same
  order, down to its partial sort, on which it depends which of several points at
  one distance make a `COUNT`. A search reads only scores until a point is in
  range, with the next ones prefetched, as a member's bytes are two more cache
  misses (`ZRANGEBYSCORE` and `ZCOUNT` find where to start the same way).
  `tools/geo-hll-diff.py` compares both families with Redis at random, replies
  and stored bytes, and they pass Redis' own `unit/geo` and `unit/hyperloglog`.
- **Transactions** (`src/transaction.rs`, `src/shard_lock.rs`): `EXEC`
  runs the queued commands as they always run, and each shard it touches
  stays its own until it ends. What it read can't change and what it wrote
  can't be seen until then, so it's as if nothing else ran meanwhile, as with
  Redis' single thread, while commands on other shards carry on. That works
  because a shard's lock lets the thread holding it for a transaction take it
  again: outside transactions, taking a lock costs one read more, of a counter
  only transactions write (the speed guard saw no difference). Transactions
  run one at a time, and they alone may wait for a shard for as long as it
  takes in any order: anything else holding a shard waits 1 ms at most for the
  next one, then lets go and starts over, so nothing can deadlock. Debug builds
  panic if a thread waits for a shard while holding another, which the whole
  test suite runs under. A
  `WATCH` registers the client on the key in its shard; every write,
  removal, expiry and eviction there marks the key's watchers, which costs
  writes one check of a field in cache while nothing is watched. `EXEC`
  takes the shards of what's watched before it checks the mark, so no change
  slips in between. 101
  write commands, no-ops included (`SET NX` of an existing key, `LREM` of
  nothing, `LTRIM` keeping all), change a watched key exactly when they do
  in Redis 7.2.4.
- **Scripts** (`src/script.rs`, `src/cmd/eval.rs`) run on Redis 7.2.4's own
  Lua, built from `vendor/` by `build.rs`: Lua 5.1 with Redis' patches
  (read-only tables among them) and the libraries scripts get, set up the
  way Redis sets them up, with the same globals allow list, `redis` API and
  error handler. So scripts see the same sandbox, fail with the same
  messages, and get the same `math.random` sequences. One interpreter
  serves every connection, made on the first script: a server that runs
  none only carries the code (the binary is ~200 KB bigger). A script runs
  as a transaction, as `EXEC` does, holding each shard it touches to the end,
  whether it declared the key or not (it can't be known beforehand: BullMQ
  builds key names from data), and a script inside `EXEC` is part of `EXEC`'s;
  its `redis.call`s run through the command table in a session of
  their own (RESP2 unless the script asks for RESP3; blocking commands
  answer at once), and replies become Lua values, and its result a reply,
  by Redis' rules. 311 commands' replies match Redis 7.2.4 in both
  protocols (`tests/scripting.rs`), and BullMQ 6.5.4's own test suite
  passes against it: all 1,111 tests (3 more are skipped in BullMQ's code).
  One difference on purpose: Redis 7.2.4 crashes on an
  error table without `err` (`error({})`); Tantalum replies Redis 8's
  `ERR unknown error`.
- **Functions** (`src/script/functions.rs`, `src/cmd/function.rs`) follow Redis 7.2.4's
  functions.c and function_lua.c: their own Lua state, whose globals are an empty
  read-only table leading to the real ones, and while a library loads, to a table with
  only `register_function`, `log` and the version, so its top level can't run
  commands. A library's code is kept on the store (`Store::function_codes`), where
  snapshots (from version 7) and the log's base find it; when a snapshot brings other
  code, the libraries are made again from it. The log has the `FUNCTION` commands that
  change them, and `FCALL`'s writes as a script's are. `tools/function-diff.py` runs
  148 scenario steps against Redis 7.2.4 with no differences, and Redis' own
  `unit/functions` passes but for its `DEBUG RELOAD` tests. A script or function that
  runs past `busy-reply-threshold` (5 s) makes other connections' commands `-BUSY`,
  but for `FUNCTION KILL`, `SCRIPT KILL` and the few Redis lets through: the flag
  rides in the word connections read once a batch, so it costs nothing until then.
- **`DUMP`, `RESTORE` and `MIGRATE`** (`src/rdb.rs`, `src/cmd/dump.rs`) speak Redis'
  RDB format: listpacks, ziplists, intsets, quicklists, zipmaps and stream nodes, read
  with every bound checked (a test feeds it 500,000 damaged payloads), as Redis does only
  with `sanitize-dump-payload yes`. `DUMP` writes a value in the encoding `OBJECT ENCODING`
  names, as Redis 7.2's `rdbSaveObject` does: a list's nodes split where Redis splits one
  made by appending, a stream's chunks as its listpack nodes (they're where Redis'
  nodes would be). `RESTORE` builds the value as a write would, so a stream's entries
  fall into chunks by this server's limits. `tools/dump-diff.py` runs 468 lines against
  Redis 7.2.4 with no differences: each type in each encoding, every error in Redis'
  order, payloads Redis 6.2 writes, and every value `MIGRATE`d from each server to the
  other and read back there. Redis' own `unit/dump` passes the 10 tests that need
  neither access times kept per key (`IDLETIME`, `FREQ`), replication, nor servers its
  harness starts itself.
- **Native BullMQ scripts** (`src/bullmq.rs`): BullMQ's three hottest
  scripts, `addStandardJob` (a job is added), `moveToActive` (a worker takes
  one) and `moveToFinished` (it completes or fails one, and takes the next),
  run as Rust when `EVALSHA` or `EVAL` gets their exact text (BullMQ
  6.5.4's, recognised by SHA-1; `--native-scripts no` turns this off). They
  do what the Lua does, with the same commands in the same order, so keys,
  replies, stream events and errors come out the same; they only skip the
  interpreter. First they read what decides the path, and anything they
  don't cover (rate limits, `keepJobs` by age or count, metrics, parents and
  dependencies, deduplication, due delayed jobs, legacy markers, other
  versions' scripts) goes to the Lua with nothing written. A job stores its
  options as the script's `cjson.encode` writes them, whose key order is the
  order Lua 5.1's table holds them in: `src/bullmq/json.rs` lays the keys out
  as `ltable.c` does (Redis' string hash, collisions, nil-valued keys, the
  table growing), so the JSON is the same byte for byte; tables with keys
  other than strings go to Lua. `tests/bullmq.rs` runs random workloads (with
  state nudged toward every branch: paused, concurrency, priorities, lost
  and stolen locks, custom and repeated ids) on two stores, natives on and
  off, comparing every reply and every key, and adds 4,000 jobs with random
  options (colliding and repeated keys, nils, nesting, escapes, floats),
  checking each runs natively exactly when it should; on a queue run as apps
  run it, every call stays native. Where a Lua script takes shards as it
  reaches them, one at a time with the others, a native one runs alongside
  others, holding those of the keys it uses, taken in ascending order
  as everything that holds two does: it asks for them before using them,
  those whose names depend on data too (the next job, from the waiting
  list's end or the first prioritized; a new job's id, from the counter),
  and all before it writes, so when one below those it holds is taken, it
  lets go and starts over holding them all (`src/shard_lock.rs`). Using a
  shard it didn't ask for panics, which every test would catch, rather
  than risk waiting for ever; and while it holds some, eviction only tries
  the others. Locking all 256 had been 40% of the server's work adding jobs
  and 19% processing them; native scripts on different queues now also run
  on different threads at once. Threads racing on three queues
  (`tests/bullmq.rs`, with Lua scripts, transactions, plain commands and
  eviction meanwhile) never deadlock and finish every job exactly once.
- **Pub/sub** (`src/pubsub.rs`): channels map to their subscribers in 64
  shards, so publishers on different threads rarely meet, and patterns are
  a list each `PUBLISH` matches, as in Redis, skipped while there are none.
  A subscriber has an outbox: a publisher appends the encoded message and
  wakes its connection (through the same per-thread mailbox as blocked
  clients on io_uring), which sends what has gathered. A subscribed
  connection's commands first take what's waiting, so replies and messages
  go out in the order they happened, and messages a command causes come
  after its reply, as in Redis 7.2. A client that doesn't read is cut off as
  Redis' default `client-output-buffer-limit pubsub 32mb 8mb 60` does (past
  32 MB, or over 8 MB for a minute), even while its socket is full and
  nothing can be written; until then its outbox counts as client memory.
  20 subscribers each got all of 4 publishers' 800 messages, every
  publisher's in order, in every I/O mode. Connections that never subscribe
  check one more flag per read; the speed guard saw no difference.
- **Client-side caching** (`src/tracking.rs`): a tracking client's read leaves an entry
  with the key, among the key's watchers in its shard, beside `WATCH`'s. A change, made
  under the shard's lock, finds whom to tell as it finds `WATCH`'s watchers, so with nobody
  tracking it costs a write nothing it didn't pay before: the one check for watchers
  became one flag. The entry goes with the message, as in Redis, and the message goes to
  the connection's outbox, pub/sub's (a connection has one outbox, for what it subscribes
  to, `MONITOR` and tracking), so it follows the reply of the command that caused it, even
  in a transaction or for a blocked command a writer serves. Redis remembers a command's
  keys after it runs; here they're registered before, so that a change on another thread
  between the read and the registration can't be missed, and a change the command makes
  itself (a key that expires as it's read) doesn't count, as Redis wouldn't have registered
  it yet. In `BCAST` mode every change is queued while some client broadcasts, and sent
  after the command, a message a prefix, as Redis sends them once an event loop;
  meanwhile every connection takes the checked path, whose commands look at that queue.
  Tracking connections take it too, so `GET` and `SET`'s fast path is the same as ever.
  `tools/tracking-diff.py` compares 563 replies with Redis 7.2.4 (Redis' own tracking
  tests among them, as steps), no differences, and Redis' `unit/tracking` passes 57 of its
  58 tests (the other has a write take memory over the limit and the next command evict:
  here that write evicts for its own room first, see "Never over the limit"); `tests/tracking.rs`
  replays the steps in every I/O mode and checks that a client caching reads while two
  others write is never left with a stale value. Connections that don't track pay nothing:
  their one outbox, which pub/sub and `MONITOR` use too, is now a single field to look at
  where it was two (callgrind: 8 instructions fewer a batch in the epoll loop; `execute`
  ran 3 more until 0.31.0 inlined its fast path, since when a `GET` at pipeline 16 takes
  1.8% fewer instructions than on 0.29.0).
- **Command statistics** (`src/stats.rs`): `INFO commandstats`, `errorstats` and
  `latencystats`, `LATENCY HISTOGRAM` and `total_error_replies`, counted as Redis 7.2
  counts them: calls and time, calls refused before they ran (arity, `NOAUTH`, ACLs,
  `-BUSY`, out of memory for a `denyoom` command, refused while queued or subscribed)
  and calls that replied with an error, each error once by its code (a script's error
  that's its command's isn't counted twice). Counting costs every command two clock
  reads and the checked path, so it starts only when something asks: `INFO` naming
  one of those sections (or `all`, `everything`), `LATENCY HISTOGRAM` or
  `CONFIG RESETSTAT`. Until then nothing's allocated and nothing changes for any
  command; from then on, until the server stops, every connection takes the checked
  path, timed. That's 869 instructions more a command (callgrind): on the Agent37 VM, a
  `GET` without pipelining takes 25% more user-space instructions and 3.4% more cycles
  with the kernel's, at pipeline 16 61% more instructions and 30% more cycles (`SET`
  41% and 21%). A plain `INFO`, which
  clients such as ioredis send when they connect, doesn't start it, so its Errorstats
  section stays empty until something does. Counts are kept in sixteen per-thread
  slots, so threads running `GET` don't share cache lines, and latencies in a
  histogram per command with sixteen steps a power of two (to within 1/16; Redis'
  keeps two significant figures), made when the command first runs. A blocked
  command counts when it blocks, and its error, from `CLIENT UNBLOCK` or what served
  it, when it ends. BullMQ's scripts that run natively count as the script, not the
  commands inside. Redis' `unit/info` passes 17 of its 20 tests (the other 3 read
  event-loop fields that aren't here) and `unit/latency-monitor` 14 of 15 (expiry isn't
  a latency event here).
- **Keyspace notifications** (`src/notify.rs`): `notify-keyspace-events` as Redis 7.2
  has it, every class and both channels (`__keyspace@DB__:key` and
  `__keyevent@DB__:event`), set by `CONFIG SET` or `--notify-keyspace-events`. Redis
  calls its notifier at each of the ninety-odd places a command changes a key; here no
  command changes for it. The store already tells `WATCH` and tracking of every key that
  changes, through a flag that's off while nobody listens. While notifications are on,
  that flag is set in every shard, each change (written, made, removed, expired,
  evicted) goes into the thread's log, and every connection takes the checked path,
  where a command's name, arguments and reply, with what the log says it did, give
  Redis' events in Redis' order. A few commands look before they run, only then:
  `XADD` with a limit (did it trim?), `XREADGROUP`, `XCLAIM` and `XAUTOCLAIM` (did they
  make a consumer?), and for `keymiss` the keys a command reads. A command's events go
  out when the command that ran it ends (`EXEC` and scripts included), then those of
  the blocked clients it served; keys that expire in the background are sent from the
  expiry thread. With notifications off, nothing is logged and commands run the same
  code: against 0.31.0 on the Agent37 VM, a `GET` takes 0.04% fewer user-space
  instructions and a `SET` at pipeline 16 0.02% fewer. With them on, every command takes
  the checked path, and each change is copied and each event formatted and published:
  a `GET` without pipelining takes 34% more user-space instructions (3% more cycles with
  the kernel's), at pipeline 16 84% more (32% more cycles); a `SET` at pipeline 16 2.7
  times the instructions with nobody subscribed (83% more cycles), and 3.7 times with a
  client subscribed to every event (2.4 times the cycles). `tools/notify-diff.py` runs the same commands on both with a client
  subscribed to everything and compares what it hears: for each of `KEA`, `KEAn` and
  `Em`, 217 scripted cases and 10,600 random commands (transactions, scripts, a blocked
  client and keys expiring as they're read among them) hear what they hear on Redis
  7.2.4, and reply the same (but for `keymiss` from `XREADGROUP` on a missing stream,
  fixed after that run). Redis' own `unit/pubsub` passes 33 of its 34 tests (18 on
  0.31.0). Approximate: `keymiss`
  goes by the keys of read-only commands and of the writes Redis reads (`GETDEL`,
  `BITOP`'s sources, `SUNIONSTORE`'s and the like), looked at before the command; and
  of a command's keys that expire as it runs, those it has events for hear `expired`
  just before them, the rest first. Redis' own test of `evicted` sets `maxmemory 1`
  with one key, which here refuses the next write without evicting (a write bigger than
  the whole limit is refused up front, and lowering the limit evicts nothing until a
  write needs room); with a fuller store, every evicted key is heard.
- **Blocking commands** (`src/blocking.rs`): a `BLPOP` whose keys are all
  empty registers its client on each key, in the key's shard under its lock,
  and checks the keys once more, so a push in between isn't missed. A write
  that creates a key with clients waiting on it queues the key, and right
  after that command (before its client's next one, as in Redis 7.2) the
  waiting commands run again, oldest first, while the key has data of their
  kind. Run again, a command can also fail: if one of `BLPOP a b`'s keys
  became a string, it gets `WRONGTYPE`, as in Redis. The writer sends that
  reply right there, on the waiting client's socket, without blocking, so the
  woken client hears before the writer does, as from Redis, though its
  connection may be on another thread. BullMQ counts on it: a queue's waiting
  worker takes a new job before the code that added it can start another.
  (What the socket won't take at once, and replies to TLS and subscribed
  clients, go to the waiting connection, which is woken to send them, so
  for those the order isn't kept across threads.) The writer's reply waits
  for that one `send`, ~3.5 µs over loopback (a write that wakes many
  clients at once sends to each in turn on its own thread, as Redis does),
  while the woken client hears as much sooner; a cost agreed for Redis'
  order. The connection meanwhile
  watches its timeout and the socket: a client that leaves stops waiting at
  once and takes nothing, and commands it pipelines after the blocking one
  wait their turn. monoio can't wake a task from another thread without
  its `sync` feature, which costs every connection, so each io_uring thread
  has a mailbox: other threads queue its wakers and ring a socket pair it
  reads. Unused, it costs next to nothing: writes look for waiters only
  when they create a key in a shard that has some, commands check one flag
  for queued keys, and the blocked reply is boxed so every other reply stays
  24 bytes. The speed guard saw no difference, and paired runs on EC2 were
  within -1.3% to +0.4%, inside one build's spread across rounds. Unlike Redis,
  commands run in parallel, so another client can take the data between
  the write and the serving; the waiter then goes on waiting.
- **`SCAN` goes in hash order** (`Store::scan`): a cursor is a shard and a
  32-bit slot hash, and each call takes the keys with the next smallest
  hashes in that shard, every key with the same hash at once, under its
  lock. Walking the table's buckets, as Redis does, would miss keys here:
  cuckoo inserts move keys between their two buckets, behind the cursor. A
  key's hash never changes, so overwrites, compaction, cuckoo moves and
  resizes can't hide one, and no key comes twice. The cost is that a call
  reads its shard's whole table (8 bytes per key) to find the next hashes,
  a bucket at a time in a loop the compiler vectorizes (~0.3 ns per slot).
  A whole scan of 1M keys at `COUNT 100` takes ~45 ns per key in-process,
  and a call holds its shard's lock for ~3.5 µs at 1M keys, ~36 µs at 10M
  and ~165 µs at 100M (M1 Pro, 256 shards). `RENAME` is the one command
  that holds two shard locks, taken in shard order.
- **Commands** (`src/cmd.rs`) are found by name length and first letter, then
  by a word holding the name's first 8 bytes lowercased, one comparison for each
  command in the bucket (callgrind: 129 instructions an `HGET` before, when each
  name was compared byte by byte); `GET` and `SET` skip even that, and their fast
  path is inlined into the connection loop with no frame of its own.
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
    both buffers back once it has nothing left to process, to a few its thread
    keeps for the next connection. io_uring must post a read buffer before
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
- **Users and ACLs** (`src/acl.rs`, `src/cmd/acl.rs`) follow Redis 7.2.4's `acl.c`: a
  selector holds a bit per command and subcommand of Redis 7.2.4 (from
  `src/acl_commands.txt`, which `tools/acl-table.py` makes from Redis' `commands.def`, so
  rules can name the commands missing here too), key patterns with what each allows, channel
  patterns, and the rules in the order given, which is how `ACL LIST` writes them back. A
  command runs if one of the user's selectors allows it, keys found by the key specs Redis'
  `COMMAND` gives (compiled once, so a check reads no table); a refusal picks the error Redis
  picks and goes to `ACL LOG`. Users are kept whole and replaced when they change, so a
  connection reads the one it holds without a lock; a change counts up in the word
  connections already read once a batch, and each takes up its user's new rules at its next
  batch (a batch read before then still runs under the old ones). A connection whose user may
  run everything, as `default` may until told otherwise, stays on the fast path and holds
  nothing shared, so ACLs cost it nothing (speed guard: see the commit). Deleting a user
  closes its connections, and taking channels away closes the subscribers that had them, as
  in Redis; scripts check each `redis.call`, and BullMQ's native scripts run only for users
  that may run everything. `tools/acl-diff.py` runs 296 scenario steps over several
  connections against Redis 7.2.4, with no differences, and Redis' own `unit/acl` and
  `unit/acl-v2` pass (but for a check of `INFO commandstats`).
- **TLS** (`--tls-port`, with `--tls-cert-file` and `--tls-key-file` as in
  Redis): a second listener next to the plain port (`--port 0` leaves only
  TLS), using rustls with its `ring` backend. Plain and TLS connections share
  the same connection loop, generic over the stream; a TLS connection reads
  without first waiting for data, since rustls may already hold decrypted
  bytes, and flushes after each write. With io_uring, TLS runs on its own
  epoll loops (monoio has no TLS here). There's no client-certificate
  authentication: use the password. A self-signed certificate that clients
  pin (`tls: { ca }` in ioredis) works without a public CA.
- **Snapshots** (`src/snapshot.rs`, `--snapshot FILE`): like Redis' RDB
  files. Every `--snapshot-interval` (60 s) with changes, on `SAVE`/`BGSAVE`
  and after a graceful shutdown, the live keys go to the file: type, key,
  value, and the expiry as a Unix time, so TTLs carry over and keys that
  expired while the server was down are dropped at startup. The format has a
  version in its header: 2 has hashes, 3 the other collections, 4 streams
  (with their chunks as they are, so `~` trims the same after a restart), 5
  streams' consumer groups, kept after their chunks, 6 numbered databases, 7
  function libraries' code, and 8 where the data is in a replication stream (its ID and
  offset), saved once the server is in one (a master's replicas, or a replica's master),
  so that a replica restarted from its snapshot, a restarted master's replicas, or a
  master that comes back as a replica of the one that took over, continue where they
  were instead of syncing in full. Such a save marks every shard as owing a copy of
  itself while no write runs, as a full sync does, and writes from the copies, so
  writes wait only while they're marked, and the file is the data at one point; the
  copies cost as much memory again as the shards written to meanwhile (as Redis' fork
  does). As it shuts down, it writes the data as it is. A master's place lapses if
  anything is written before its first replica asks. A
  save writes the lowest version that holds its data (without hashes still
  1, which builds from before hashes, 0.7, can read), and every build reads
  all versions up to its own (`tests/data/v1.snapshot` was written by 0.7.0). Each shard is copied out
  under its lock (a few MB: about a millisecond) and written after, so a save
  is consistent per shard, not across the store. It writes `FILE.tmp`, syncs
  it and renames it over the old one, and a CRC32 over the whole file is
  checked before anything loads: a damaged snapshot stops the server rather
  than being overwritten by an empty one. A crash loses what changed since
  the last save; a deploy (SIGTERM) loses nothing.
- **Append-only log** (`src/aof.rs`, `--aof DIR`): like Redis 7's
  multi-part AOF, for a crash to lose about a second instead of up to a
  minute. A manifest names a base (a snapshot, in the format above) and the
  logs after it, which hold the write commands in RESP as they were sent,
  each after a `TANTALUM.TIME` line with the Unix millisecond it ran at.
  With the log on, the store's clock reads Unix time, and at startup each
  command runs again at its own time, so TTLs, `XADD`'s IDs and what had
  expired by then come out as they first did. Only what chance decided is
  logged as what happened: `SPOP` as the `SREM` of what it popped, an
  eviction as a `DEL` (the replay itself ignores `maxmemory`). Transactions
  and scripts are logged as the writes they made, between `MULTI` and
  `EXEC`, and so is `MSET`, as a `SET` per pair, so each comes back whole or
  not at all; a blocked command is logged when it's served. To keep the log
  in the order things happened, write commands take turns: each takes its
  time when its turn comes, runs, and appends to a buffer, which a thread
  writes to the file every 10 ms and syncs every second (`--aof-fsync no`
  leaves syncing to the system). Reads don't take turns. When the logs
  since the base reach `--aof-rewrite-percentage` (100) of its size and
  `--aof-rewrite-min-size` (64 MB), or on `BGREWRITEAOF`, a rewrite starts a
  new log (in the manifest before anything goes in it), writes the data as
  a new base, and only then drops the old files, so whenever the server
  stops, the manifest names files that hold all the data. At startup, a
  record or group the server stopped in the middle of writing at the end of
  the last log is dropped (and cut off); damage anywhere else stops the
  server, as a damaged snapshot does. If the disk can't take a write, what
  wasn't written stays in memory, a record written in part is taken out
  again, and until a write works, commands that write (and `PING`) get
  Redis' `-MISCONF Errors writing to the AOF file: ...`. With `--snapshot`
  too, snapshots are still saved, but a log, once there, is what loads; the
  first start with `--aof` writes what the snapshot loaded as the base. To
  stop using the log, delete DIR too: if `--aof` comes back with an old log
  still there, that log loads and anything written since is gone.
- **Replication** (`src/repl/`): Redis 7.2's protocol, both sides, so a Redis replica
  follows Tantalum and Tantalum follows Redis (or each other, in chains). A replica sends
  `PING`, `AUTH` with `masterauth` and `masteruser`, `REPLCONF listening-port` and `capa eof
  psync2`, then `PSYNC replid offset`: if the master's backlog (`repl-backlog-size`, 1 MB)
  still holds what came after `offset` in the stream with that ID (or with the ID it had
  before a promotion, up to where that one ends), it sends just that (`+CONTINUE`);
  otherwise the whole data as a Redis RDB file (`src/rdb_file.rs`, which reads and writes
  every type as `DUMP` does, functions and numbered databases included) and the stream
  from the offset that file is at (`+FULLRESYNC`). Each replica has a thread of its own,
  which sends from the backlog where that replica is and reads its acks; a replica more
  than 256 MB behind is dropped, as by Redis' `client-output-buffer-limit replica`.
  - *What's propagated is what Redis propagates*, byte for byte where
    `tools/repl-diff.py` holds the two side by side (228 cases, from every write command
    to scripts, transactions, expiry and consumer groups; all the same but the one below): a command only if it changed
    something, rewritten where running it again could do otherwise (`SET ... EX` and
    `SETEX` as `SET ... PXAT`, `EXPIRE` as `PEXPIREAT`, or `DEL` if the time has passed,
    `SPOP` as `SREM` of what it popped, `INCRBYFLOAT` as `SET ... KEEPTTL`, a blocking pop
    as the pop it made, `XADD *` with its ID and `~` trimming as `=` what it left,
    `XREADGROUP` as the `XCLAIM ... FORCE JUSTID LASTID` of each entry it delivered,
    `GEOADD` as `ZADD`, a relative `RESTORE` with `ABSTTL`), an expired or evicted key
    as a `DEL`, and what one command, script or `EXEC` did as one unit, between `MULTI`
    and `EXEC` if it's more than one command, each after a `SELECT` when its database
    changes. Whether a command changed anything comes from the store's notices of
    changed keys, which commands that change nothing don't make. Where Tantalum
    propagates more than Redis: a consumer that a group read or claim made and nothing
    else implies goes as `XGROUP CREATECONSUMER`, so its replicas have it (Redis' don't).
  - *It costs nothing until a replica first asks* (or this server is told to follow a
    master). Then, as with the append-only log, every write takes a turn (the log's, if
    it's on: one turn for both), so the stream has writes in the order they ran, each
    unit appended to the backlog in its turn. The change is made where every write
    looks already (one pointer every write reads), and takes hold once every I/O thread
    has finished what it was running (a task sent to each, or a waker through a monoio
    thread's mailbox), so no write that started before is missed. From then on, reads
    leave expired keys they find (a read that removed one couldn't propagate it in
    order) and remove them after their batch in a turn, as `DEL`s; active expiry runs
    in a turn too, each key's `DEL` on its own, as from Redis. With replicas, writes
    scale as they do with the log on (see Benchmarks); without, nothing changes. After
    `repl-backlog-ttl` (an hour) without replicas, the stream is freed, as Redis frees its
    backlog: writes stop taking turns, and the replication ID changes, so a replica that
    comes back syncs in full.
  - *The full sync's file is the data at one point of the stream*, the offset the
    replica's reading starts from. Redis forks for that, and the kernel copies a page
    before a write changes it; here, each shard does. While no write runs, every shard is
    marked as owing a copy of itself (a shard's lock at a time: well under a
    millisecond), and then writes go on. A shard makes its copy (its segments' bytes,
    big collections cloned, big values shared) the next time anything locks it, before
    anything can change it, or when the file's writing gets to it, and the file is
    written from the copies, each dropped once written. Every way to a shard's data goes
    through one guard, which looks at one count of copies owed, 0 but during a sync. On
    a 4-vCPU VM, with 1M small keys and two collections of 100,000, a client writing
    throughout saw its longest write take 1 ms (107 when writes waited for the whole
    file; 11 when they waited for a copy of it all); with 4M, 7 ms. To a replica that
    says it can (`capa eof`, as Redis' and Tantalum's do), the file is streamed as it's
    written, between a mark it ends with, and the stream starts once the replica has
    acked; to one that can't, it's written to memory first, to send with its length.
    A replica loads the file as it comes (Redis' `repl-diskless-load`), a record at a
    time, so it never holds the file: emptied first, `LOADING` until it's done, and
    emptied again if the connection drops partway, for the next sync. A replica of
    Redis 7.2.4 with 1M keys and a 118 MB file peaked at 166-184 MB, where it
    peaked at 283-290 MB holding the file whole.
  - *A replica runs what its master sends as its master ran it*: on a thread of its own,
    in a write's turn (so its own replicas get the same bytes, at the same offsets, and
    a full sync of one is consistent), past `maxmemory`, and with no key expiring by its
    own clock: the master says when one goes. A key whose time had passed by the time
    it got there stays (logically expired: reads don't see it) until the master's `DEL`,
    as Redis' `checkAlreadyExpired` has it. A transaction counts as run, toward the
    offset it acks and what it passes on, once its `EXEC` has, so a link that drops in
    the middle gets it again whole; the database the stream is in carries over a
    reconnect. Other clients get `-READONLY` for writes (scripts' too), `-LOADING` while
    the master's file loads (the old data until then), and see keys expire by its clock;
    keyspace notifications, `MONITOR` and command statistics see the master's commands,
    which take the same checked path theirs do (passing its checks).
    `REPLICAOF NO ONE` keeps the data and goes on under a new replication ID, keeping the
    master's as the one before with the offset it ends at, so the old master and its
    other replicas can `PSYNC` here and continue (Redis' PSYNC2).
  - `WAIT` asks replicas for their acks (`REPLCONF GETACK *`, through the stream) and
    replies when enough have the writes made before it; `min-replicas-to-write` refuses
    writes with `-NOREPLICAS` while too few have acked within `min-replicas-max-lag`.
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
    (`madvise(MADV_DONTNEED)`), those of sizes no longer being taken first. An A/B
    on Railway showed that dropping every freed block's pages cost ~8% of pipelined
    throughput, because new segments had to fault them back in (~10k faults/s).
    Big mappings keep the mapping count far
    below Linux's ~65k limit.
  - **Hash tables** are mapped straight from the OS when rounding up to whole
    pages wastes at most 1/8 (on 4 KB-page Linux, anything over ~18 KB). A resize
    frees the old table, and through mimalloc those freed tables stayed stuck in
    partly used pages (~10 bytes/key at 1M keys). This is also why the default is
    256 shards rather than 1024: bigger tables per shard, all of them mappable.
  It's asked for an alignment only when one is needed (`Allocator` in `src/main.rs`).
  The `mimalloc` crate always asks, and when a size's bin has no block ready, unless
  its blocks are a power of two, that adds `align - 1` to the size: 320 bytes aligned
  to 8 took a 384-byte block, by a slower path, and left the bin empty, so the next did
  too. Which sizes this hit depended on what had come before, so it came and went from
  build to build: 134 instructions on an `HSET` on ten fields.
  Once a second a thread asks mimalloc to hand back what's been free past its delay
  (`spawn_memory_returner` in `src/main.rs`). It would otherwise do so only when
  something next asks it for whole pages, which a steady server may never do: a big
  set's old buffer stayed resident from the moment it doubled, up to 74.9 bytes a
  member for 45 in use. It costs commands nothing, memory reused within the delay
  stays resident, and `tests/memory.rs` checks it in the real binary on Linux.
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
| `allkeys-lfu` | Evict approximately least often used keys: by the same use counter |
| `allkeys-random` | Evict keys regardless of use: a segment picked at random gives up all of its keys |
| `volatile-lru`, `volatile-lfu`, `volatile-random` | As their `allkeys-` versions, but only keys with a TTL; OOM once none are left |
| `volatile-ttl` | Evict keys with a TTL nearest their deadline: of four segments (the oldest, the newest, two at random), the one whose TTL keys are due soonest gives up its half due soonest |

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
- **LRU and LFU are one counter**: it counts up to three uses and loses one
  each time eviction passes, so it keeps keys used often as well as lately,
  which is what either policy asks. Redis keeps one or the other in 24 bits a
  key; two bits are all an entry here has spare.
- **Random and `volatile-ttl` age nothing**: the segment they pick always gives
  up keys. `volatile-ttl` is approximate: filling a store at its limit with
  half its keys due far sooner than the rest, about a fifth of what's left is
  due soon (under a random policy, half), mostly keys written since eviction
  last looked at their segment.
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
- **Never over the limit**: a write makes room for itself first, and is refused
  with `-OOM` if eviction can't make enough. Redis checks only that memory is
  under the limit before a command, lets that command take it over, and evicts
  before the next one; so near the limit a big write that Redis would take can be
  refused here.

## Benchmarks

### Everyday apps (Agent37 VM, November 2026)

What people use Redis for, rather than one command on its own. Tantalum 0.26.0,
Redis 8.10.3 and Valkey 9.1.3, each on one core of a 4-vCPU Xeon E-2388G, with
memtier (3 × 80 clients) on the other three, persistence off, 1M items. Thousands
of ops/s, medians of 3 rotated rounds of 12 s, the best in each row in bold
(`bench/showcase/run.sh`, about 50 minutes, `summary.py` for the tables; raw
output: `bench/showcase/results-agent37.txt`). "vs Redis" is Tantalum on epoll. The
page cache (5 rounds) and login sessions are from re-runs: this VM runs at half speed
for some minutes after it starts, which hit their first go, and made the page cache's
lead look bigger than it is.

| Example | Pipeline | Tantalum | Tantalum, io_uring | Redis | Valkey | vs Redis |
|---|---|---|---|---|---|---|
| Login sessions: a 5-field hash each, mostly `HGETALL` | 1 | 307 | **317** | 256 | 218 | +19.8% |
|  | 16 | **1,974** | 1,956 | 1,035 | 770 | +90.7% |
| Event stream: `XADD` capped at 1M, `XREVRANGE` of the latest | 1 | 239 | **253** | 168 | 166 | +41.9% |
|  | 16 | 621 | **641** | 318 | 341 | +95.3% |
| Nearby search: `GEOSEARCH` among 1M places | 1 | **50** | 50 | 35 | 34 | +45.9% |
|  | 16 | **63** | 62 | 40 | 40 | +56.6% |
| Rate limiter: a Lua script doing `INCR` and `EXPIRE` | 1 | 252 | **266** | 219 | 205 | +15.3% |
|  | 16 | **819** | 812 | 622 | 554 | +31.6% |
| Page cache: 1 KB values with TTLs, 9 reads to a write | 1 | 295 | **310** | 259 | 260 | +14.1% |
|  | 16 | 1,336 | **1,486** | 1,087 | 1,066 | +22.9% |
| Activity feeds: lists of the latest 50, `LRANGE` of 20 | 1 | 272 | **280** | 247 | 247 | +10.2% |
|  | 16 | 1,003 | **1,088** | 848 | 828 | +18.3% |
| Unique visitors: a HyperLogLog per page, `PFADD`, `PFCOUNT` | 1 | 285 | **301** | 254 | 239 | +12.3% |
|  | 16 | 957 | **973** | 867 | 708 | +10.4% |
| Leaderboard: one sorted set of 1M players | 1 | 237 | **253** | 232 | 236 | +2.5% |
|  | 16 | 686 | 695 | 677 | **707** | +1.3% |

The job queue is the real BullMQ 6.5.4 (`bench/bullmq/run.sh`, 100,000 jobs, three
Node workers, medians of 3 rounds), in thousands of jobs/s. Tantalum's server used
0.83 of its core processing, as the workers limit the rate, and Redis' 0.98. A job,
added and processed, cost the server 17.7 µs of CPU, Redis 48.9 and Valkey 55.2.

| BullMQ | Tantalum | Tantalum, io_uring | Redis | Valkey | vs Redis |
|---|---|---|---|---|---|
| Adding jobs | 55.8 | **59.4** | 34.1 | 34.5 | +63.5% |
| Processing jobs | 66.6 | **70.4** | 29.9 | 25.9 | +122.6% |

Memory for the same data, RSS after the fill, in MB:

| Example | Tantalum | Redis | Valkey | vs Redis |
|---|---|---|---|---|
| Page cache | **1,025** | 1,317 | 1,330 | -22.2% |
| Login sessions | **154** | 195 | 196 | -21.0% |
| Leaderboard | **53** | 83 | 83 | -36.0% |
| Activity feeds | **185** | 190 | 189 | -2.6% |
| Event stream | **48** | 54 | 54 | -10.7% |
| Unique visitors | **15** | 21 | 20 | -27.6% |
| Nearby search | **52** | 83 | 83 | -38.0% |

Tantalum leads Redis everywhere. The closest is the leaderboard, by 2.5% and 1.3%, and
on this VM anything within 3% counts as level; Valkey is 3.0% ahead of Tantalum there at
pipeline 16, level too.

Two of these lost until 0.25.0. **The rate limiter** did 32% and 60% less than Redis,
because a Lua script took all 256 shard locks first, as it may touch keys it didn't
declare: about 2.5 µs a call. A script now takes each shard when it first touches it
and keeps it to the end, so nothing sees it half done. **Activity feeds at pipeline
16** did 16% less: a small list lives in the segment log, so each `LPUSH` and `LTRIM`
writes it again, and freed segments were losing their pages and faulting them back
in, 0.17 faults a command against Redis' 0.002. The pool keeps 4 MB of freed blocks
resident, and that had filled up with blocks of sizes shards had outgrown. Sizes in
use now push those out: faults fell to 0.0002 a command, for 2% more RSS after the
traffic (251 to 256 MB). The page cache at pipeline 16 rose from +1.9% to +17.9% in
the same release, probably for the same reason, though that wasn't measured alone.
Unique visitors and nearby search came in with 0.26.0, losing at first by up to 31%, for
reasons in their design notes above.

An error reply is the fastest reply there is, so `run.sh` asks every server the same
questions after filling it, and `summary.py` compares the bytes each server sent per
command: nothing differed here. These runs are at saturation and show what one core
can do; an app far below that sees the memory saved, and little else.

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
proactor threads (on io_uring), memcached `-t N`, KeyDB `--server-threads N`,
and Garnet sizes its thread pool to the N cores it may use. 1M keys × 32 B,
1:10 SET:GET, means of 2 rotated rounds, Tantalum 0.16.0, 2026-11-04
(`bench/aws/scale.sh`, Garnet and KeyDB from `bench/docker/others.Dockerfile`;
raw output and caveats: `bench/aws/results-c7a-8xlarge.txt`). Total ops/s:

| No pipelining | 1 thread | 2 | 4 | 8 |
|---|---|---|---|---|
| **Tantalum, io_uring** | **115k** | **233k** | **469k** | **962k** |
| Tantalum, epoll (per-thread) | 91k | 183k | 391k | 808k |
| Valkey 9.1.3 | 95k | 99k | 284k | 652k |
| Redis 8.10.3 | 93k | 84k | 227k | 438k |
| Dragonfly 2.0.2 | 94k | 188k | 378k | 712k |
| memcached 1.6.38 | 93k | 185k | 380k | 780k |
| KeyDB 6.3.4 | 42k | 173k | 343k | 608k |
| Garnet 2.2.5 | 45k | 134k | 290k | 608k |

| Pipeline 16 | 1 thread | 2 | 4 | 8 |
|---|---|---|---|---|
| **Tantalum, io_uring** | **1.35M** | **2.60M** | **5.12M** | **10.23M** |
| Tantalum, epoll (per-thread) | 1.10M | 2.17M | 4.45M | 9.08M |
| Valkey 9.1.3 | 0.99M | 1.25M | 2.85M | 2.70M |
| Redis 8.10.3 | 0.90M | 0.92M | 2.22M | 2.57M |
| Dragonfly 2.0.2 | 0.53M | 1.02M | 1.93M | 3.67M |
| memcached 1.6.38 | 0.64M | 1.22M | 2.31M | 4.82M |
| KeyDB 6.3.4 | 0.39M | 0.87M | 1.08M | 1.24M |
| Garnet 2.2.5 | 0.54M | 1.43M | 3.08M | 6.41M |

RSS after loading the 1M keys (1-thread pass): **Tantalum 68 MB**, Valkey 93,
Redis 104, Dragonfly 123, memcached 131, KeyDB 136, Garnet 297.

- Tantalum on io_uring is the fastest at every thread count: +21–24% over the
  next server without pipelining, +36% (1 thread) to +81% pipelined.
- Tantalum 0.16.0 on epoll, the mode on hosts that block io_uring (Railway),
  trailed without pipelining at 1 and 2 threads (table above): 91k against
  Valkey's 95k, 183k against Dragonfly's 188k. A profile at 1 thread: the same
  syscalls as Valkey (0.95 receives and sends per command) and less CPU in
  its own code (15% of samples against 20%), but waking the client on each
  reply cost it more in the kernel (15% against 10%); Valkey sends a loop's
  replies together. 0.16.1 does too (see the per-thread design note): in the
  same rounds as the others, 1 thread did **99k** against Dragonfly's 97k,
  Valkey's 97k and memcached's 94k, and 2 threads **195k** against
  memcached's 185k (Valkey 106k, Dragonfly 111k). From 4 threads on, and
  pipelined, it was already ahead.
- Per core, a cloud VM does about half the M1 VM's work (syscalls cost more),
  so compare these with each other, not with the tables below.
- Pipelined at 8 threads Tantalum used 7.8 of 8 cores with memtier near its
  limit, so those are floors. A profile at 8 threads (0.5.0) showed no lock
  contention, mostly the kernel's loopback TCP stack.
- Garnet and KeyDB do poorly with one core and 240 connections; they're built
  to spread over many.

### Replies sent together on epoll (AWS EC2, November 2026)

The same machine and setup as above, with 0.19.1 (the outbox in the per-thread
design note), SET only and 1:10 SET:GET. Total ops/s, then p99 in ms. Medians
of 5 s runs: Tantalum's 3 paired rounds, Redis' and Valkey's 2 rounds, each
server running alone, because their I/O threads spin on for some time after a
test and slow whatever runs next (raw output:
`bench/aws/results-outbox.txt`):

| No pipelining, SET | 1 thread | 2 | 4 | 8 |
|---|---|---|---|---|
| **Tantalum 0.19.1, io_uring** | **112k**, 2.37 | **225k**, 1.32 | **452k**, 0.68 | **917k**, 0.38 |
| **Tantalum 0.19.1, epoll** | **106k**, 2.53 | **210k**, 1.31 | **420k**, 0.68 | **829k**, 0.52 |
| Tantalum 0.19.0, epoll | 94k, 2.83 | 186k, 1.48 | 377k, 0.75 | 778k, 0.43 |
| Valkey 9.1.3 | 95k, 2.77 | 105k, 2.46 | 300k, 1.65 | 677k, 0.77 |
| Redis 8.10.3 | 95k, 2.69 | 97k, 4.74 | 289k, 1.42 | 684k, 0.52 |

| No pipelining, 1:10 | 1 thread | 2 | 4 | 8 |
|---|---|---|---|---|
| **Tantalum 0.19.1, io_uring** | **119k**, 2.21 | **237k**, 1.21 | **479k**, 0.63 | **978k**, 0.35 |
| **Tantalum 0.19.1, epoll** | **112k**, 2.35 | **220k**, 1.24 | **446k**, 0.62 | **889k**, 0.49 |
| Tantalum 0.19.0, epoll | 100k, 2.67 | 197k, 1.39 | 399k, 0.68 | 826k, 0.38 |
| Valkey 9.1.3 | 97k, 3.20 | 107k, 2.37 | 314k, 0.79 | 718k, 0.37 |
| Redis 8.10.3 | 96k, 2.67 | 100k, 4.69 | 296k, 1.42 | 703k, 0.51 |

| Pipeline 16, SET | 1 thread | 2 | 4 | 8 |
|---|---|---|---|---|
| **Tantalum 0.19.1, io_uring** | **983k**, 4.38 | **1.94M**, 2.32 | **3.51M**, 1.45 | **6.21M**, 0.89 |
| Tantalum 0.19.1, epoll | 818k, 5.21 | 1.69M, 2.61 | 3.18M, 1.44 | 5.82M, 0.85 |
| Valkey 9.1.3 | 915k, 8.26 | 1.30M, 3.08 | 2.38M, 1.72 | 2.17M, 1.91 |
| Redis 8.10.3 | 853k, 8.89 | 1.00M, 4.67 | 1.51M, 2.84 | 1.47M, 2.90 |

| Pipeline 16, 1:10 | 1 thread | 2 | 4 | 8 |
|---|---|---|---|---|
| **Tantalum 0.19.1, io_uring** | **1.37M**, 3.06 | **2.72M**, 1.71 | **5.34M**, 0.89 | **10.7M**, 0.54 |
| Tantalum 0.19.1, epoll | 1.12M, 3.69 | 2.25M, 1.89 | 4.64M, 0.94 | 9.47M, 0.50 |
| Valkey 9.1.3 | 1.02M, 7.46 | 1.32M, 3.02 | 3.12M, 1.51 | 2.79M, 1.50 |
| Redis 8.10.3 | 942k, 8.02 | 1.19M, 4.50 | 2.55M, 1.71 | 2.58M, 1.73 |

Against 0.19.0 pipelining gained 0.3–1.5%, and io_uring, unchanged, read −1.4%
to +0.5%. On one 4-vCPU Xeon E-2388G core (the Agent37 VM, three client threads,
before the flusher's look), one-command requests went from 3% behind Redis to
13–17% ahead. Two things Redis or Valkey still win: pipelined SET on one epoll
thread, and p99 at 8 epoll threads with the 1:10 mix (Valkey, 0.37 against 0.49
ms). io_uring wins both.

0.19.2 holds the replies of batches up to 16 commands as well. On one Agent37
core (4 paired rounds, memtier 3 x 80, `bench/agent37/results-outbox.txt`),
ops/s, then p99 in ms:

| One server core | SET P1 | 1:10 P1 | SET P4 | SET P16 | 1:10 P16 | SET P64 |
|---|---|---|---|---|---|---|
| **Tantalum 0.19.2, io_uring** | **323k**, 0.97 | **347k**, 0.90 | **1.01M**, 1.15 | **1.89M**, 2.86 | **2.98M**, 1.88 | **2.52M**, 8.08 |
| **Tantalum 0.19.2, epoll** | **313k**, 1.36 | **336k**, 1.34 | **970k**, 1.72 | **1.83M**, 4.14 | **2.88M**, 2.58 | **2.33M**, 9.34 |
| Tantalum 0.19.1, epoll | 305k, 1.52 | 334k, 1.39 | 843k, 1.40 | 1.72M, 2.76 | 2.63M, 2.56 | 2.35M, 8.93 |
| Valkey 9.1.3 | 274k, 1.69 | 284k, 1.63 | 759k, 2.24 | 1.44M, 3.93 | 1.70M, 3.62 | 1.87M, 10.88 |
| Redis 8.10.3 | 270k, 1.73 | 282k, 1.65 | 765k, 2.26 | 1.45M, 4.54 | 1.69M, 3.30 | 1.90M, 10.11 |

At 2 server threads (2 client cores, which likely limit it): −1.5% to +0.8%
against 0.19.1. More threads, where deferring pipelined replies once cost
0.7–2%, and the EC2 tables above, are yet to be measured with this.

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

### BullMQ jobs (Agent37 VM, November 2026)

BullMQ 6.5.4 adding 100,000 jobs with `addBulk`, then three Node worker
processes (concurrency 100, doing nothing) processing them, jobs removed on
completion. Each server alone on one core of a 4-vCPU Xeon E-2388G VM, the
clients on the other three; server CPU time from `/proc`. Medians of 3
rotated rounds, 2026-11-04 (`bench/bullmq/run.sh`; raw output, and Valkey and
Dragonfly in an earlier run: `bench/bullmq/results-agent37.txt`):

| One server core | Add, jobs/s | Server µs per job | Process, jobs/s | Server µs per job |
|---|---|---|---|---|
| **Tantalum 0.18.3, io_uring** | **60.5k** | **4.4** | **71.7k** | **11.7** |
| **Tantalum 0.18.3, epoll** | **61.2k** | **4.5** | **72.1k** | **11.9** |
| Tantalum 0.18.2 (scripts holding every shard) | 53.3k | 6.9 | 63.9k | 13.6 |
| Redis 8.10.3 | 37.6k | 14.6 | 30.7k | 32.0 |

A job, added and processed, costs the server 16.1 µs (io_uring) or 16.4
(epoll), Redis 46.6. Tantalum's server used 0.85 of its core processing
(the clients limit the rate) and less adding, Redis 0.98 in both. Every
one of a run's script calls stayed native (`INFO`:
`native_script_fallbacks:0`). With all three scripts on Lua
(`--native-scripts no`, an earlier run), processing cost 26.7 µs per job.

### Append-only log (Agent37 VM, November 2026)

What `--aof` (synced every second) costs, against Redis' AOF with
`appendfsync everysec`: memtier over 1M keys of 32 B, SET only and 1:10
SET:GET, each server alone on one core of the E-2388G VM above (the log's
writer thread on that core too), memtier 3 × 80 clients on the other three.
Medians of 3 rotated rounds, 2026-11-04 (`bench/docker/aof.sh`; raw output:
`bench/agent37/results-aof.txt`):

| One server core, ops/s | SET, P1 | SET, P16 | 1:10, P1 | 1:10, P16 |
|---|---|---|---|---|
| **Tantalum 0.19.0, io_uring, log on** | **287k** | **1.22M** | **339k** | **2.69M** |
| Tantalum 0.19.0, io_uring, log off | 308k | 1.75M | 350k | 2.94M |
| **Tantalum 0.19.0, epoll, log on** | **242k** | **1.10M** | **271k** | **2.35M** |
| Tantalum 0.19.0, epoll, log off | 250k | 1.55M | 274k | 2.56M |
| Redis 8.10.3, AOF on | 202k | 704k | 272k | 1.43M |
| Redis 8.10.3, AOF off | 272k | 1.51M | 282k | 1.72M |

The log costs Tantalum 3–7% of its SETs one at a time and 30% pipelined
(Redis' AOF: 26% and 53%), and 1–8% of a 1:10 mix (Redis 4–17%). With
both logs on, Tantalum does 1.25–1.9× Redis' work per core on io_uring
(SET 1.43× and 1.73×, the mix 1.25× and 1.89×, one at a time and
pipelined) and 1.0–1.6× on epoll, as on hosts that block io_uring like
Railway (SET 1.20× and 1.56×, the mix 1.00× and 1.64×): one request at a
time, mostly reads, is a tie there. SET p99 at P16 with the log on: 6.8 ms
on io_uring, 7.6 on epoll, Redis 18.9. A rewrite
(`BGREWRITEAOF`) of those 1M keys took 0.07–0.10 s, and the longest a SET
waited meanwhile was 32–34 ms (p99.9 0.12–0.17 ms); Redis' forked rewrite
took 0.2–1.1 s, with SETs waiting at most 5–9 ms but p99.9 3.1 ms. With the
log off, 0.19.0 matches 0.18.3 (`saturate.sh`, medians of 4 rounds: epoll P1
−0.1%, P16 +1.0%; io_uring P1 −1.0%, P16 0.0%).

Those numbers are from 0.19.0. 0.19.4 recycles the log's buffer, gives `SET`
its fast path back and encodes faster, for +2.0–3.4% with the log on (6 paired
rounds), on top of the epoll work above; the table is yet to be measured again.

### Earlier results

v0.0.2 Linux runs (`bench/railway/results-network.txt`, `results-loopback.txt`)
and the macOS laptop comparison (`bench/compare.sh`) point the same way for
throughput. Two caveats about them: across Railway's private network the ~2.2 ms round
trip dominated, and on one laptop the client was the bottleneck. Their eviction
hit rates share the seeding flaw described above.

## Not there yet

- **Missing, by how many client library tests need it** (see above; 11 of 10,530):
  turning the log on with `CONFIG SET appendonly` (5: without `--aof` the store's clock isn't
  the Unix time the log's replay needs, and a write already past its look at the log could
  miss the new base, so it wants its own change), `OBJECT IDLETIME` (2, one after `RESTORE`'s
  `IDLETIME`), and Unix sockets,
  without which redis-rb's tests won't start. Answers that differ: `SAVE`, `BGSAVE` and `BGREWRITEAOF`
  refuse without `--snapshot` or `--aof`, where Redis saves to its default (3), and `COMMAND LIST` has
  the 234 commands that are here (1, which expects over 300).
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
- **Pipelined SET on one epoll thread** trailed Valkey by 11% and Redis by 4%
  on EC2 with 0.19.1 (818k against 915k and 853k; io_uring: 983k), though it led
  by 18% on the Agent37 VM. 0.19.2 is meant for it, sparing clients wakes, and
  gained 9% on the VM; it hasn't been measured on EC2 yet.
- **p99 of pipelined SET on epoll**, held since 0.19.2: at pipeline 16 about
  Redis' and Valkey's (4.14 ms against 4.54 and 3.93), where io_uring has 2.86.
- **p99 at 8 epoll threads** rose with replies sent together (0.38–0.43 to
  0.49–0.52 ms, for 6–7% more throughput and a lower median). More looks at the
  reactor before sending didn't help; io_uring is at 0.35–0.38 ms.
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
- No `instantaneous_ops_per_sec`. Command statistics count only from the first time
  something asks for them (see "Command statistics"), where Redis counts from the start.
- **The slow log at its default**: a `fast` command that's slow (a `GET` of a huge value on a
  busy machine, say) isn't logged, and a command's time is in 5 ms ticks. Set the threshold
  under 10 ms for what Redis logs, at the cost of a clock read per command. A blocked command
  is logged when it blocks, where Redis logs it when it's served.
- The server binds to `127.0.0.1` by default. If you expose it, set a password,
  and use TLS outside a private network. No client certificates, no
  certificate reloading without a restart.
- **Removing from big collections**: on one sorted set of 8M, `ZREM` does about 14% less
  than Redis (1.69 against 1.46 ms per 500), as it searches the order twice, once for the
  member and once for the one moving into its place. Removing three in four, tidying
  included, a hash and a set end 2-4% behind Redis over the whole run (9.7 against 9.5 s,
  9.8 against 9.4, the same client's time in both), though the usual batch is level or
  ahead (0.37 against 0.38 ms). Its longest batch on a sorted set is 8.1 ms (Redis 5.7,
  Valkey 7.9).
- **A small hash is scanned and written whole**: `HGET` costs about 1.6× a `GET` at
  pipeline 16 (243 against 155 ns in the in-process benchmark, 10 fields), mostly in
  the cache lines the scan touches, and a write copies it, up to 512 fields or 8 KB.
  No field TTLs (`HEXPIRE`).
- **`SCAN` on very big stores**: each call reads a whole shard's table, so
  at 100M keys (390k per shard) a call holds a lock for ~165 µs, which
  commands on that shard wait out, and a whole scan reads each table up to
  1024 times.
- **Multi-key set commands read one key at a time** (`SINTER`, `SUNION`,
  `SDIFF`, `ZUNION`, `ZINTER`, `ZDIFF` and their stores), as `MGET` reads
  and `MSET` writes one at a time, so a transaction or script can run
  between two of their keys; `SMOVE`, `LMOVE` and `RENAME` are atomic.
- **Blocking gaps**: no `b` flag in a client list.
- **Writes to a small collection decode it whole**: a list, set or sorted
  set packed in its entry is decoded, changed and packed again on every
  write (`ZADD` on a 20-member sorted set ~1.5 µs). Redis edits its
  listpacks in place.
- **Consumer groups, pipelined, about 3-4% slower since 0.33.0's copy-on-write full
  sync** in the in-process benchmark (`group_p16_ns`: `XADD`, `XREADGROUP`, `XACK`), in
  three runs of the speed guard, where every other workload was level or faster. A
  build without the one check that change puts on shard locks was as slow, so it's
  where the stream code landed in the binary rather than work done; kept, as agreed,
  for full syncs that hold writes 1 ms where they held them 107 (1M keys).
- **Replication gaps**: no writable replicas (`replica-read-only no` is refused), no
  `FAILOVER` and no Sentinel, and no cluster. A replica over TLS checks its master's
  certificate against `--tls-ca-cert-file` only (no `tls-ca-cert-dir`, no client
  certificate of its own). A replica loads its master's file
  as it comes, whatever `repl-diskless-load` says, so it serves `LOADING`, not its old
  data, while a full sync's file comes (Redis' default, `disabled`, writes the file to
  disk first and serves the old data meanwhile; `swapdb` keeps it loaded beside the new).
  A full sync copies the master's data, shard by shard, as writes come (memory
  up to about the data's size again, at worst, as with Redis' fork).
  `repl-diskless-sync` and its delay are accepted and do nothing: a master streams the
  file at once. `repl-diskless-load` and `replica-lazy-flush` take anything and do
  nothing.
- **`DUMP` in Redis' order but for hash tables**: a big hash or set (past 128 members)
  comes out in Tantalum's order, a list made otherwise than by appending (`LPUSH`,
  `LINSERT`, ...) with its nodes split where an appended one's would be, and a stream
  without the entries deleted from it, which Redis keeps in their node, marked. Each
  `RESTORE`s to the same value in either server; only the bytes differ.
- **ACL gaps**: `ACL DRYRUN` of a command Redis has and Tantalum doesn't checks only the
  command rules, not knowing its keys. There's no config file, so no `user`
  lines in one (use `--aclfile`), and no module authentication. A rule change reaches another
  connection at its next batch: what it has already read runs under the old rules.
- **Settings with a syntax of their own aren't checked**: `client-output-buffer-limit`,
  `latency-tracking-info-percentiles`, `oom-score-adj-values`, `locale-collate` and
  `proc-title-template` take anything, as none of them does anything here.
- **`OBJECT ENCODING` doesn't know a value's past**: a short string `APPEND` made
  is `raw` in Redis and `embstr` here, and a collection that grew past a limit
  and shrank back stays big in Redis.
- **Append-only log gaps**: no `appendfsync always`. A rewrite writes its
  base while writes wait (reads go on), where Redis forks: about 30 ms per
  million small keys. The files are Tantalum's own: Redis can't load them,
  nor Tantalum Redis'. `CONFIG SET appendonly` can't turn it on or off, `WAITAOF`'s tests that
  need that can't run, and `stream-node-max-entries` and `-bytes` can't change with it on, as
  what `~` trims depends on them and the log's commands run again with the default. No `aof_rewrites`,
  `aof_delayed_fsync` and the rest of Redis' AOF counters in `INFO`. With
  the log on, writes take turns across threads, so they don't scale with
  `--threads` (reads do), and pipelined SETs lose about 30% (see Benchmarks),
  in many small costs per write: its turn, a clock read, encoding the command.
  (Each write reads the clock on its turn, not once per batch: active expiry
  runs on the real clock, so with an older time a command could find a key gone
  that its replay would find alive.)
- **Scripting gaps**: no Lua debugger (`SCRIPT DEBUG YES`). Before a script has
  run for `busy-reply-threshold`, other clients' commands wait for the keys it
  holds rather than all being held up; `SCRIPT KILL`, `FUNCTION KILL` and the
  `-BUSY` replies come only to connections on another thread than the busy
  script's (so not with `--threads 1`), and with `--io shared` they may wait behind
  it on its runtime thread. `allow-oom` acts for functions; for `EVAL`'s shebang it,
  `allow-stale`, `no-cluster` and `allow-cross-slot-keys` are accepted with nothing
  to relax, and there's no check before a script runs that memory is under
  `maxmemory` (a write over it fails inside the script instead). No
  `used_memory_lua`, `number_of_cached_scripts` or `number_of_functions` in `INFO`.
- **Stream gaps**: `XINFO STREAM` walks the stream's chunks for
  `radix-tree-nodes`, under the shard's lock, which a stream of billions of
  entries would feel.
- **Memory gaps**: in real memory, the growth of RSS per item of a 32-byte value
  (`bench/agent37/memtypes.py`), strings (56.8 bytes; Redis 96.6, Valkey 79.6) and
  a big sorted set (63.6; 108.4, 100.0), a big set (45.2; 64.0, 54.2) and a big hash
  (63.3; 80.0, 70.2) lead and a big list ties (34.5; 34.9, 35.0), but small collections of ten in one key trail:
  hashes 53.6 (51.1, 50.2), lists 51.2 (45.2, 43.9), sets 49.6 (45.2, 43.9), sorted
  sets 54.7 (51.0, 50.1). With 8-byte values small lists lead (see above).
  `used_memory` can also be off, as it counts capacity not yet touched: a big list's
  says 38.4, so `maxmemory` starts evicting a little early.
- **Transaction gaps**: Redis refuses a write command with `-OOM` while
  queueing when memory is over `maxmemory`; Tantalum queues it and the
  error is among `EXEC`'s replies. Queued commands aren't counted as client
  memory.
- **Tracking gaps**: the table is by database, where Redis' is by key name only, so a
  write to a key in one database doesn't tell clients that read a key of that name in
  another (Redis does; their cached value wasn't stale). A connection that's redirected
  to but has no outbox (it doesn't subscribe, track or monitor) hears once it next sends a
  command: Redis' connections are all one thread's, and nothing here wakes an idle one
  that's never had anything to hear. Which keys a full table (`tracking-table-max-keys`)
  drops is a matter of chance in both, but not the same chance. A tracking client's BullMQ
  scripts run in Lua, not natively, so that what they read is remembered.
- **Pub/sub gaps**: `CLIENT LIST` counts only
  the caller's subscriptions (it knows who is subscribed), and the output
  limit is fixed at Redis' defaults. The registry's
  own memory (channel names and their subscriber tables) isn't in
  `used_memory`; subscribers' unsent messages are. `PUBSUB CHANNELS` and multiple patterns' messages come in
  an order of Tantalum's own (Redis' depends on its random hash seed).

## Development

Building needs a C compiler as well as Rust: `build.rs` compiles the Lua in
`vendor/` (see `vendor/README.md`).

```sh
cargo test                     # unit + end-to-end tests
cargo clippy --all-targets
# Run the tests under AddressSanitizer (needs a nightly toolchain):
RUSTFLAGS=-Zsanitizer=address cargo +nightly test --lib --tests --target aarch64-apple-darwin --target-dir target/asan
# Coverage-guided fuzzing of the parser and every command (cargo install cargo-fuzz):
cd fuzz && cargo +nightly fuzz run commands corpus/commands seeds/commands -- -max_total_time=600
# Replication against a real Redis: what each sends its replicas, and a master's and
# replica's data compared key by key (either may be Redis):
python3 tools/repl-diff.py tools/repl-cases.txt --redis 6390 --tantalum 6391
python3 tools/repl-compare.py MASTER_PORT REPLICA_PORT
# Speed and memory against the last commit (see CLAUDE.md), and replies against a real Redis:
tools/speed-guard.sh
tools/redis-diff.py commands.txt   # with redis-server on 6390 and tantalum on 6391
```

`tests/fuzz.rs` runs 300,000 random commands (valid and not) on every
`cargo test`; `tests/hashes.rs` checks hash commands against a model; `fuzz/` drives raw bytes through the parser and command
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
