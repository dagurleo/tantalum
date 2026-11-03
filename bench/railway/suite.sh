#!/usr/bin/env bash
# The field comparison: tantalum against Redis, Valkey, memcached and Dragonfly,
# all in one container over loopback. Started by run.sh as ROLE=suite.
#
# 1. Memory by value size: each server alone, filled once, RSS after settling.
# 2. Throughput: all servers up at once, alternating, ROUNDS rounds.
# 3. Eviction quality under a 1 GB limit: each server alone, gaussian keys.
# 4. Churn: all servers at once under a 1 GB limit, random keys and mixed
#    value sizes for CHURN_SECONDS, sampling RSS to see if it stays flat.
#
# Every result line starts with "### " so it's easy to pick out of the logs.
set -uo pipefail

ROUNDS=${ROUNDS:-2}
CHURN_SECONDS=${CHURN_SECONDS:-300}
THREADS=4
LIMIT_MB=1024
KINDS=(tantalum redis valkey memcached dragonfly)

# --- servers -----------------------------------------------------------------

# Starts a server of `kind` on `port`, with a memory limit in MB if given.
# Sets PID; returns 1 (after logging why) if it never comes up.
PID=
start() {
  local kind=$1 port=$2 limit=${3:-}
  local redis=(--bind 127.0.0.1 --port "$port" --save '' --appendonly no --protected-mode no)
  local lru=()
  [ -n "$limit" ] && lru=(--maxmemory "${limit}mb" --maxmemory-policy allkeys-lru)
  local cmd
  case $kind in
    tantalum) cmd=(tantalum --bind 127.0.0.1 --port "$port" --threads "$THREADS" "${lru[@]}") ;;
    redis) cmd=(redis-server "${redis[@]}" "${lru[@]}") ;;
    redis-io) cmd=(redis-server "${redis[@]}" --io-threads "$THREADS" "${lru[@]}") ;;
    valkey) cmd=(valkey-server "${redis[@]}" "${lru[@]}") ;;
    # memcached evicts (LRU) whenever it reaches -m; unlimited means "plenty".
    memcached) cmd=(memcached -u root -l 127.0.0.1 -p "$port" -U 0 -t "$THREADS" -c 4096 -m "${limit:-16384}") ;;
    dragonfly)
      cmd=(dragonfly --bind 127.0.0.1 --port "$port" --proactor_threads "$THREADS" --force_epoll --dbfilename=)
      [ -n "$limit" ] && cmd+=(--maxmemory "${limit}mb" --cache_mode=true)
      ;;
  esac
  "${cmd[@]}" >"/tmp/$kind-$port.log" 2>&1 &
  PID=$!
  for _ in $(seq 1 150); do
    ready "$kind" "$port" && return 0
    sleep 0.2
  done
  echo "### SKIP $kind on $port: did not start: $(tail -c 400 "/tmp/$kind-$port.log" | tr '\n' ' ')"
  kill -9 "$PID" 2>/dev/null
  PID=
  return 1
}

stop() {
  kill -9 "$1" 2>/dev/null
  wait "$1" 2>/dev/null
}

# One memcached text-protocol command; prints the reply.
mc() {
  exec 3<>"/dev/tcp/127.0.0.1/$1" || return 1
  printf '%s\r\nquit\r\n' "$2" >&3
  tr -d '\r' <&3
  exec 3<&-
}

ready() {
  if [ "$1" = memcached ]; then
    mc "$2" version 2>/dev/null | grep -q VERSION
  else
    redis-cli -p "$2" PING 2>/dev/null | grep -q PONG
  fi
}

proto() {
  if [ "$1" = memcached ]; then echo memcache_text; else echo redis; fi
}

keys() {
  if [ "$1" = memcached ]; then
    mc "$2" stats | awk '$2 == "curr_items" { print $3 }'
  else
    redis-cli -p "$2" DBSIZE
  fi
}

rss_mb() {
  awk '/^VmRSS:/ { printf "%.1f", $2 / 1024 }' "/proc/$1/status"
}

# CPU time (utime + stime, in clock ticks). The command name in /proc/pid/stat
# can contain spaces, so count fields after its closing paren.
cpu_ticks() {
  sed 's/.*) //' "/proc/$1/stat" | awk '{ print $12 + $13 }'
}

# --- client ------------------------------------------------------------------

# memtier against `kind` on `port`; prints its summary table.
mt() {
  local kind=$1 port=$2
  shift 2
  memtier_benchmark -s 127.0.0.1 -p "$port" --protocol="$(proto "$kind")" --hide-histogram \
    --distinct-client-seed "$@" 2>/dev/null | sed -n '/ALL STATS/,/^Totals/p'
}

# Like mt, plus the server's average CPU use over the run.
measured() {
  local kind=$1 port=$2 pid=$3
  shift 3
  local c0 t0 c1 t1
  c0=$(cpu_ticks "$pid")
  t0=$(date +%s.%N)
  mt "$kind" "$port" "$@"
  c1=$(cpu_ticks "$pid")
  t1=$(date +%s.%N)
  awk -v c="$((c1 - c0))" -v t0="$t0" -v t1="$t1" -v hz="$(getconf CLK_TCK)" \
    'BEGIN { printf "server_cpu_cores: %.2f\n", c / hz / (t1 - t0) }'
}

# Values are random bytes everywhere: memtier's default filler is a repeated
# pattern, which flatters any server that compresses.
fill() { # kind port keys value-size
  mt "$1" "$2" --threads 4 --clients 25 --pipeline 64 --ratio 1:0 --key-pattern P:P \
    --key-minimum 1 --key-maximum "$3" -n allkeys --data-size "$4" --random-data >/dev/null
}

# --- the suite ---------------------------------------------------------------

echo "### client: $(nproc) CPUs, $(lscpu | sed -n 's/^Model name: *//p')"
echo "### versions: redis $(redis-server --version | cut -d' ' -f3), $(valkey-server --version | cut -d' ' -f1-3), \
$(memcached -V), $(dragonfly --version 2>&1 | head -1)"

# 1. Memory by value size. Key counts keep each run's data around 100-200 MB.
for spec in 16:2000000 100:1000000 1024:200000 10240:20000; do
  size=${spec%%:*} count=${spec##*:}
  for kind in "${KINDS[@]}"; do
    start "$kind" 7000 || continue
    sleep 1
    idle=$(rss_mb "$PID")
    fill "$kind" 7000 "$count" "$size"
    sleep 5
    echo "### memory value=$size keys=$count server=$kind idle_mb=$idle rss_mb=$(rss_mb "$PID") stored=$(keys "$kind" 7000)"
    stop "$PID"
  done
done

# 2. Throughput: everyone up at once, 1M keys of 32 bytes, rounds alternate.
declare -A TPID TPORT
port=7100
for kind in "${KINDS[@]}" redis-io; do
  port=$((port + 1))
  start "$kind" "$port" || continue
  TPID[$kind]=$PID
  TPORT[$kind]=$port
  fill "$kind" "$port" 1000000 32
  [ "$kind" = tantalum ] && echo "### versions: $(redis-cli -p "$port" INFO server | tr -d '\r' | grep tantalum_version)"
done
for round in $(seq 1 "$ROUNDS"); do
  for kind in "${KINDS[@]}" redis-io; do
    [ -n "${TPID[$kind]:-}" ] || continue
    for pipeline in 1 16; do
      echo "### throughput round=$round server=$kind pipeline=$pipeline"
      measured "$kind" "${TPORT[$kind]}" "${TPID[$kind]}" --threads 8 --clients 25 --pipeline "$pipeline" \
        --ratio 1:10 --key-pattern R:R --key-minimum 1 --key-maximum 1000000 --data-size 32 --test-time 15
    done
  done
done
for pid in "${TPID[@]}"; do stop "$pid"; done

# 3. Eviction quality under 1 GB: gaussian keys (sigma 7M over 56M) touch far
# more keys than fit, so a good policy keeps the popular middle.
#
# The measured phase needs --randomize. memtier's seeds are otherwise fixed, so
# it would replay the fill phase's key sequence from the start, asking for the
# oldest inserts first. That rewards policies that keep early keys and punishes
# LRU; one server scored above what a perfect cache of its size could.
gauss=(--key-pattern G:G --key-minimum 1 --key-maximum 56000000 --key-stddev 7000000 --data-size 100 --random-data)
for kind in "${KINDS[@]}"; do
  start "$kind" 7200 "$LIMIT_MB" || continue
  mt "$kind" 7200 --threads 4 --clients 25 --pipeline 64 --ratio 1:0 "${gauss[@]}" --test-time 25 >/dev/null
  echo "### eviction server=$kind limit_mb=$LIMIT_MB"
  measured "$kind" 7200 "$PID" --randomize --threads 8 --clients 25 --pipeline 1 --ratio 1:4 "${gauss[@]}" --test-time 30
  echo "### eviction-state server=$kind keys=$(keys "$kind" 7200) rss_mb=$(rss_mb "$PID")"
  stop "$PID"
done

# 4. Churn: everyone at once under 1 GB, random keys over 4M (about 4 GB of
# data), value sizes uniformly 32-2048 bytes, half writes. Fragmentation shows
# up as RSS creeping up over time.
declare -A CPID CPORT
port=7300
for kind in "${KINDS[@]}"; do
  port=$((port + 1))
  start "$kind" "$port" "$LIMIT_MB" || continue
  CPID[$kind]=$PID
  CPORT[$kind]=$port
done
CLIENTS=()
for kind in "${!CPID[@]}"; do
  memtier_benchmark -s 127.0.0.1 -p "${CPORT[$kind]}" --protocol="$(proto "$kind")" --hide-histogram \
    --distinct-client-seed --threads 2 --clients 25 --pipeline 4 --ratio 1:1 --key-pattern R:R \
    --key-minimum 1 --key-maximum 4000000 --data-size-range=32-2048 --data-size-pattern=R --random-data \
    --test-time "$CHURN_SECONDS" >"/tmp/churn-$kind.txt" 2>/dev/null &
  CLIENTS+=($!)
done
for t in $(seq 30 30 "$CHURN_SECONDS"); do
  sleep 30
  line="### churn t=${t}s"
  for kind in "${KINDS[@]}"; do
    [ -n "${CPID[$kind]:-}" ] && line+=" $kind=$(rss_mb "${CPID[$kind]}")MB"
  done
  echo "$line"
done
for pid in "${CLIENTS[@]}"; do wait "$pid"; done
for kind in "${KINDS[@]}"; do
  [ -n "${CPID[$kind]:-}" ] || continue
  echo "### churn-final server=$kind keys=$(keys "$kind" "${CPORT[$kind]}") rss_mb=$(rss_mb "${CPID[$kind]}") \
ops=$(grep '^Totals' "/tmp/churn-$kind.txt" | tr -s ' ' | cut -d' ' -f2)"
done
for pid in "${CPID[@]}"; do stop "$pid"; done

echo "### DONE"
sleep infinity
