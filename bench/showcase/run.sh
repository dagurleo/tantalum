#!/usr/bin/env bash
# What people use Redis for, on Tantalum, Redis and Valkey: a page cache, login
# sessions, a rate limiter, a leaderboard, activity feeds, an event stream, counting
# unique visitors and finding what's nearby.
# Each server gets one core (SERVER_CPUS), memtier the rest of the machine, so
# the server is what limits the rate. Persistence is off everywhere. The job
# queue example is the real BullMQ: bench/bullmq/run.sh.
#
#   docker build -t tantalum-bench .
#   docker run -d --name showcase --privileged -v "$PWD:/repo" --entrypoint sleep tantalum-bench infinity
#   docker exec showcase bash /repo/bench/showcase/run.sh | tee results.txt
#   python3 bench/showcase/summary.py results.txt
#
# --privileged allows io_uring. TANTALUM picks a build, ONLY some of the
# examples, ITEMS how much data, ROUNDS and TIME how long (about 50 minutes as
# it stands). All the servers stay up while each is tested in turn, and each
# round starts with a different one, so a machine's slow moments are shared.
#
# Fast is worthless if the answers are wrong, and an error reply is the fastest
# reply there is. So each example asks every server the same questions after
# filling it (lines starting "check"), and summary.py compares the bytes each
# server sent per command.
set -uo pipefail

TANTALUM=${TANTALUM:-tantalum}
ROUNDS=${ROUNDS:-3}
TIME=${TIME:-12}
ITEMS=${ITEMS:-1000000}
SERVER_CPUS=${SERVER_CPUS:-0}
CLIENT_CPUS=${CLIENT_CPUS:-1-3}
read -ra ONLY <<<"${ONLY:-cache sessions limiter leaderboard feed events uniques nearby}"
USERS=$((ITEMS / 10))

NAMES=(tantalum tantalum-uring redis valkey)
PORTS=(7301 7302 7303 7304)
PIDS=()

start_servers() {
  local on=(taskset -c "$SERVER_CPUS")
  "${on[@]}" "$TANTALUM" --port 7301 --threads 1 --io per-thread >/tmp/showcase-1.log 2>&1 &
  PIDS[0]=$!
  "${on[@]}" "$TANTALUM" --port 7302 --threads 1 --io uring >/tmp/showcase-2.log 2>&1 &
  PIDS[1]=$!
  "${on[@]}" redis-server --port 7303 --save '' --appendonly no >/tmp/showcase-3.log 2>&1 &
  PIDS[2]=$!
  "${on[@]}" valkey-server --port 7304 --save '' --appendonly no >/tmp/showcase-4.log 2>&1 &
  PIDS[3]=$!
  for port in "${PORTS[@]}"; do
    for _ in $(seq 1 100); do
      redis-cli -p "$port" ping >/dev/null 2>&1 && break
      sleep 0.05
    done
  done
}

stop_servers() {
  kill -9 "${PIDS[@]}" 2>/dev/null
  wait "${PIDS[@]}" 2>/dev/null
  # An io_uring server's socket closes after the process is gone.
  for port in "${PORTS[@]}"; do
    while (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; do sleep 0.05; done
  done
}

rss_kb() { awk '/^VmRSS/ { print $2 }' "/proc/$1/status"; }

# Reads lines of words and writes them as Redis commands, for redis-cli --pipe.
to_resp() { awk '{ printf "*%d\r\n", NF; for (i = 1; i <= NF; i++) printf "$%d\r\n%s\r\n", length($i), $i }'; }

BEFORE=()
note_before() {
  local i
  for i in "${!NAMES[@]}"; do BEFORE[i]=$(rss_kb "${PIDS[$i]}"); done
}

# How much memory the data took, on each server.
note_memory() { # example, items
  sleep 3
  local i
  for i in "${!NAMES[@]}"; do
    local after
    after=$(rss_kb "${PIDS[$i]}")
    awk -v e="$1" -v n="${NAMES[$i]}" -v a="$after" -v b="${BEFORE[$i]}" -v items="$2" \
      'BEGIN { printf "memory %s %s rss_mb %.1f bytes_per_item %.1f\n", e, n, a / 1024, (a - b) * 1024 / items }'
  done
}

fill_from() { # file: the same data into every server
  local port
  for port in "${PORTS[@]}"; do
    local end
    end=$(redis-cli -p "$port" --pipe <"$1" 2>&1 | tail -1)
    case $end in
      *"errors: 0,"*) ;;
      *) echo "check FILL-FAILED port $port: $end" ;;
    esac
  done
}

agree() { # a command: every server has to answer it with the same bytes
  local first="" port answer
  for port in "${PORTS[@]}"; do
    answer=$(redis-cli -p "$port" "$@" 2>&1)
    if [ "$port" = "${PORTS[0]}" ]; then
      first=$answer
    elif [ "$answer" != "$first" ]; then
      echo "check DIFFERENT $* on port $port: $(echo "$answer" | head -3 | cut -c1-80) against $(echo "$first" | head -3 | cut -c1-80)"
      return
    fi
  done
  case $first in
    ERR* | WRONGTYPE* | NOSCRIPT*) echo "check ERROR $*: $(echo "$first" | cut -c1-120)" ;;
    *) echo "check same $*: $(echo "$first" | head -1 | cut -c1-40)" ;;
  esac
}

traffic() { # example, then memtier's options
  local example=$1
  shift
  local round i pipe
  for round in $(seq 1 "$ROUNDS"); do
    for i in "${!NAMES[@]}"; do
      local k=$(((i + round - 1) % ${#NAMES[@]}))
      for pipe in 1 16; do
        local line
        line=$(taskset -c "$CLIENT_CPUS" memtier_benchmark -s 127.0.0.1 -p "${PORTS[$k]}" --hide-histogram \
          --threads 3 --clients 80 --pipeline "$pipe" --distinct-client-seed --random-data \
          --test-time "$TIME" --print-percentiles 50,99 "$@" 2>/dev/null | awk '/^Totals/ { print; exit }')
        if [ -z "$line" ]; then
          echo "check NO-RESULT $example ${NAMES[$k]} P$pipe"
          continue
        fi
        awk -v e="$example" -v r="$round" -v n="${NAMES[$k]}" -v p="$pipe" -v tot="$line" 'BEGIN {
          split(tot, a, " ")
          printf "%s round %d %s P%d ops %.0f misses %s p50 %s p99 %s kb_per_op %.4f\n", e, r, n, p, a[2], a[4], a[6], a[7], a[8] / a[2]
        }'
      done
    done
  done
}

# A site caching rendered pages and API answers: 1 KB values that live for an
# hour or two, nine reads to a write, popular entries read most.
cache() {
  local same=(--data-size 1024 --expiry-range 3600-7200 --key-minimum 1 --key-maximum "$ITEMS")
  local port
  for port in "${PORTS[@]}"; do
    memtier_benchmark -s 127.0.0.1 -p "$port" --hide-histogram --threads 3 --clients 10 --pipeline 32 --random-data \
      "${same[@]}" --ratio 1:0 --key-pattern P:P -n allkeys >/dev/null 2>&1
  done
  note_memory cache "$ITEMS"
  agree DBSIZE
  agree STRLEN memtier-1
  traffic cache "${same[@]}" --ratio 1:9 --key-pattern G:G
}

# A site's logins: a small hash per session. Read on each request, touched now
# and then, its expiry pushed back.
sessions() {
  awk -v n="$ITEMS" 'BEGIN {
    srand(1)
    for (i = 1; i <= n; i++)
      printf "HSET session:%d user %d ip 10.%d.%d.%d agent Mozilla/5.0(Windows-NT-10.0;Win64;x64)Chrome/%d token %d%d at 1730000000\n",
        i, i, rand() * 255, rand() * 255, rand() * 255, 100 + rand() * 40, rand() * 1e9, rand() * 1e9
  }' | to_resp >/tmp/showcase-fill
  fill_from /tmp/showcase-fill
  note_memory sessions "$ITEMS"
  agree DBSIZE
  agree HGETALL session:12345
  traffic sessions --key-prefix= --key-minimum 1 --key-maximum "$ITEMS" \
    --command 'HGETALL session:__key__' --command-ratio 8 --command-key-pattern G \
    --command 'HSET session:__key__ at 1730000001' --command-ratio 1 --command-key-pattern G \
    --command 'EXPIRE session:__key__ 1800' --command-ratio 1 --command-key-pattern G
}

# An API's rate limit, as most libraries write it: a small Lua script counting
# each user's requests in a one-minute window.
limiter() {
  local script="local n = redis.call('INCR', KEYS[1]) if n == 1 then redis.call('EXPIRE', KEYS[1], 60) end return n"
  local port sha=""
  for port in "${PORTS[@]}"; do sha=$(redis-cli -p "$port" SCRIPT LOAD "$script"); done
  agree EVALSHA "$sha" 1 check:1
  agree EVALSHA "$sha" 1 check:1
  traffic limiter --key-prefix= --key-minimum 1 --key-maximum "$USERS" \
    --command "EVALSHA $sha 1 rate:__key__" --command-ratio 1 --command-key-pattern G
}

# A game's leaderboard: one sorted set of a million players. Where am I, a
# score goes up, who's in the top ten.
leaderboard() {
  awk -v n="$ITEMS" 'BEGIN { srand(2); for (i = 1; i <= n; i++) printf "ZADD board %d player:%d\n", rand() * 1e9, i }' |
    to_resp >/tmp/showcase-fill
  fill_from /tmp/showcase-fill
  note_memory leaderboard "$ITEMS"
  agree ZCARD board
  agree ZREVRANK board player:12345
  agree ZREVRANGE board 0 9 WITHSCORES
  traffic leaderboard --key-prefix= --key-minimum 1 --key-maximum "$ITEMS" \
    --command 'ZREVRANK board player:__key__' --command-ratio 5 --command-key-pattern G \
    --command 'ZINCRBY board 5 player:__key__' --command-ratio 3 --command-key-pattern G \
    --command 'ZREVRANGE board 0 9 WITHSCORES' --command-ratio 2 --command-key-pattern G
}

# Each user's recent activity: a list kept to its latest 50, the top 20 shown
# when they look.
feed() {
  awk -v n="$USERS" 'BEGIN {
    for (i = 1; i <= n; i++) {
      printf "RPUSH feed:%d", i
      for (j = 0; j < 50; j++) printf " post-%07d-%02d-abcdefghijklmnopq", i, j
      print ""
    }
  }' | to_resp >/tmp/showcase-fill
  fill_from /tmp/showcase-fill
  note_memory feed $((USERS * 50))
  agree DBSIZE
  agree LLEN feed:12345
  agree LRANGE feed:12345 0 19
  traffic feed --key-prefix= --key-minimum 1 --key-maximum "$USERS" --data-size 32 \
    --command 'LRANGE feed:__key__ 0 19' --command-ratio 8 --command-key-pattern G \
    --command 'LPUSH feed:__key__ __data__' --command-ratio 1 --command-key-pattern G \
    --command 'LTRIM feed:__key__ 0 49' --command-ratio 1 --command-key-pattern G
}

# An event log: a stream capped at a million entries, written as much as the
# latest are read.
events() {
  awk -v n="$ITEMS" 'BEGIN { for (i = 1; i <= n; i++) printf "XADD events * user %d kind page-view page /products/%d\n", i, i % 5000 }' |
    to_resp >/tmp/showcase-fill
  fill_from /tmp/showcase-fill
  note_memory events "$ITEMS"
  agree XLEN events
  traffic events --key-prefix= --key-minimum 1 --key-maximum "$ITEMS" --data-size 32 \
    --command "XADD events MAXLEN ~ $ITEMS * user __key__ kind page-view page __data__" --command-ratio 1 --command-key-pattern G \
    --command 'XREVRANGE events + - COUNT 10' --command-ratio 1 --command-key-pattern G
}

# Unique visitors, as analytics count them: a HyperLogLog per page, sparse at first,
# popular ones turning dense. Each visit is added, and the count read now and then.
uniques() {
  local pages=$((ITEMS / 100))
  awk -v n="$pages" 'BEGIN {
    srand(3)
    for (i = 1; i <= n; i++) {
      printf "PFADD page:%d", i
      for (j = 0; j < 200; j++) printf " visitor-%d", rand() * 1e9
      print ""
    }
  }' | to_resp >/tmp/showcase-fill
  fill_from /tmp/showcase-fill
  note_memory uniques "$pages"
  agree DBSIZE
  agree PFCOUNT page:1234
  agree PFCOUNT page:1 page:2 page:3
  agree PFDEBUG ENCODING page:1234
  traffic uniques --key-prefix= --key-minimum 1 --key-maximum "$pages" --data-size 16 \
    --command 'PFADD page:__key__ __data__' --command-ratio 9 --command-key-pattern G \
    --command 'PFCOUNT page:__key__' --command-ratio 1 --command-key-pattern G
}

# A delivery app: what's near this place, where is it, and couriers reporting where
# they are, kept apart so that what's searched stays the same for every server.
nearby() {
  awk -v n="$ITEMS" 'BEGIN {
    srand(4)
    for (i = 1; i <= n; i++) printf "GEOADD places %.6f %.6f place:%d\n", -10 + rand() * 40, 35 + rand() * 25, i
  }' | to_resp >/tmp/showcase-fill
  fill_from /tmp/showcase-fill
  note_memory nearby "$ITEMS"
  agree ZCARD places
  agree GEOHASH places place:12345
  agree GEODIST places place:1 place:2 km
  agree GEOSEARCH places FROMMEMBER place:12345 BYRADIUS 20 km ASC COUNT 10 WITHDIST
  traffic nearby --key-prefix= --key-minimum 1 --key-maximum "$ITEMS" \
    --command 'GEOSEARCH places FROMMEMBER place:__key__ BYRADIUS 10 km ASC COUNT 10' --command-ratio 8 --command-key-pattern G \
    --command 'GEOPOS places place:__key__' --command-ratio 1 --command-key-pattern G \
    --command 'GEOADD couriers 13.__key__ 52.__key__ courier:__key__' --command-ratio 1 --command-key-pattern G
}

echo "# $(uname -r), $(nproc) CPUs, $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs)"
echo "# $(redis-server --version | cut -d' ' -f1-3); $(valkey-server --version | cut -d' ' -f1-3)"
echo "# server on CPU $SERVER_CPUS, memtier 3 x 80 clients on $CLIENT_CPUS; $ITEMS items, $ROUNDS rounds of $TIME s, persistence off"
for example in "${ONLY[@]}"; do
  start_servers
  [ "$example" = "${ONLY[0]}" ] && echo "# Tantalum $(redis-cli -p 7301 INFO server | tr -d '\r' | awk -F: '$1 == "tantalum_version" { print $2 }')"
  note_before
  echo "# example: $example"
  "$example"
  stop_servers
done
rm -f /tmp/showcase-fill
