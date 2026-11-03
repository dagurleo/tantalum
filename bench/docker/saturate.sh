#!/usr/bin/env bash
# Per-core throughput: every server gets THREADS (default 1) threads and
# memtier the rest of the machine, so the server is the bottleneck and its
# ops/s per core used is directly comparable. With PROFILE=<server>, also
# samples that server with perf during each test and prints the hottest
# functions.
#
#   docker run -d --name tantalum-prof --privileged -v /tmp/tantalum-prof:/prof \
#     -e ROLE=idle tantalum-bench
#   docker exec -i -e PROFILE=tantalum -e TANTALUM=/prof/tantalum tantalum-prof \
#     bash < bench/docker/saturate.sh
#
# --privileged allows both io_uring and perf. TANTALUM can point at a build
# with frame pointers and line tables for readable profiles. TANTALUM_B adds
# a second build as tantalum-b and tantalum-b-uring, for A/B runs.
set -uo pipefail

THREADS=${THREADS:-1}
ROUNDS=${ROUNDS:-3}
TIME=${TIME:-10}
CLIENT_THREADS=${CLIENT_THREADS:-6}
CLIENTS=${CLIENTS:-33}
TANTALUM=${TANTALUM:-tantalum}
PROFILE=${PROFILE:-}
read -ra SERVERS <<<"${SERVERS:-tantalum tantalum-uring memcached valkey}"
read -ra TESTS <<<"${TESTS:-P1:1 P16:16}"

declare -A PID PORT
start() {
  local name=$1 port=$2
  shift 2
  "$@" >"/tmp/$name.log" 2>&1 &
  PID[$name]=$!
  PORT[$name]=$port
}
start tantalum 7961 "$TANTALUM" --port 7961 --threads "$THREADS"
start tantalum-uring 7962 "$TANTALUM" --port 7962 --threads "$THREADS" --io uring
start memcached 7963 memcached -u root -l 127.0.0.1 -p 7963 -U 0 -t "$THREADS" -c 4096 -m 8192
start valkey 7964 valkey-server --port 7964 --save '' --appendonly no
start redis 7967 redis-server --port 7967 --save '' --appendonly no
# Dragonfly on its default I/O: io_uring where allowed (--privileged here).
start dragonfly 7968 dragonfly --bind 127.0.0.1 --port 7968 --proactor_threads "$THREADS" --dbfilename=
if [ -n "${TANTALUM_B:-}" ]; then
  start tantalum-b 7965 "$TANTALUM_B" --port 7965 --threads "$THREADS"
  start tantalum-b-uring 7966 "$TANTALUM_B" --port 7966 --threads "$THREADS" --io uring
fi
sleep 2

proto() { [ "$1" = memcached ] && echo memcache_text || echo redis; }
# utime + stime of a process, in clock ticks.
cpu() { sed 's/.*) //' "/proc/$1/stat" | awk '{ print $12 + $13 }'; }
hz=$(getconf CLK_TCK)

memtier() {
  local s=$1
  shift
  memtier_benchmark -s 127.0.0.1 -p "${PORT[$s]}" --protocol="$(proto "$s")" --hide-histogram \
    --distinct-client-seed --key-minimum 1 --key-maximum ${KEYS:-1000000} --data-size 32 --random-data "$@"
}

echo "# $(uname -r), $(nproc) CPUs; $THREADS server thread(s), memtier $CLIENT_THREADS x $CLIENTS clients;"
echo "# ${KEYS:-1000000} keys x 32 B, 1:10 SET:GET, $TIME s per run. ops/core = ops / server cores used."
echo "# $(head -1 /tmp/tantalum-uring.log)"
for s in "${SERVERS[@]}"; do
  memtier "$s" --threads 4 --clients 25 --pipeline 64 --ratio 1:0 --key-pattern P:P -n allkeys \
    >/dev/null 2>&1
done

for round in $(seq 1 "$ROUNDS"); do
  n=${#SERVERS[@]}
  for i in $(seq 0 $((n - 1))); do
    s=${SERVERS[$(((i + round - 1) % n))]}
    for test in "${TESTS[@]}"; do
      IFS=: read -r name pipe <<<"$test"
      c0=$(cpu "${PID[$s]}")
      t0=$(date +%s.%N)
      tot=$(memtier "$s" --threads "$CLIENT_THREADS" --clients "$CLIENTS" --pipeline "$pipe" \
        --ratio 1:10 --key-pattern R:R --test-time "$TIME" 2>/dev/null | grep '^Totals' | tr -s ' ')
      c1=$(cpu "${PID[$s]}")
      t1=$(date +%s.%N)
      awk -v r="$round" -v s="$s" -v n="$name" -v tot="$tot" -v c=$((c1 - c0)) \
        -v t0="$t0" -v t1="$t1" -v hz="$hz" 'BEGIN {
          split(tot, a, " ")
          cores = c / hz / (t1 - t0)
          printf "round %d %-16s %-4s ops %8.0f p50 %.3f p99 %.3f cpu %.2f ops/core %8.0f\n",
            r, s, n, a[2], a[6], a[7], cores, a[2] / cores
        }'
    done
  done
done

if [ -n "$PROFILE" ]; then
  command -v perf >/dev/null ||
    { apt-get update -qq && apt-get install -y -qq linux-perf; } >/dev/null 2>&1
  # Older Debian ships perf_<version> behind a wrapper that insists on a
  # matching kernel; any perf works.
  perf=$(ls /usr/bin/perf_* 2>/dev/null | head -1)
  perf=${perf:-perf}
  for test in "${TESTS[@]}"; do
    IFS=: read -r name pipe <<<"$test"
    "$perf" record -q -e cpu-clock -F 2999 -g -p "${PID[$PROFILE]}" -o "/tmp/perf-$name.data" \
      -- sleep "$TIME" 2>/dev/null &
    recorder=$!
    memtier "$PROFILE" --threads "$CLIENT_THREADS" --clients "$CLIENTS" --pipeline "$pipe" \
      --ratio 1:10 --key-pattern R:R --test-time "$TIME" >/dev/null 2>&1
    wait "$recorder"
    echo
    echo "# $PROFILE $name: self time by function"
    "$perf" report -i "/tmp/perf-$name.data" --no-children --sort symbol -g none --stdio \
      --percent-limit 0.7 2>/dev/null | grep -v '^#' | grep -v '^$' | head -45
  done
fi
kill -9 "${PID[@]}" 2>/dev/null
