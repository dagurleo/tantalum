#!/usr/bin/env bash
# What the append-only log costs: each server with its log off and on
# (synced every second), one server thread on SERVER_CPUS and memtier on
# CLIENT_CPUS, SET only and 1:10 SET:GET, at pipelines 1 and 16, over KEYS
# keys of 32 B. Then, with the log on, how long a rewrite takes at that size
# and what SETs wait meanwhile. Run in the bench image, as saturate.sh:
#
#   docker exec -e TANTALUM=/prof/tantalum bench bash bench/docker/aof.sh
#
# Redis gets the same: --appendonly yes --appendfsync everysec.
set -uo pipefail

TANTALUM=${TANTALUM:-tantalum}
ROUNDS=${ROUNDS:-3}
TIME=${TIME:-10}
KEYS=${KEYS:-1000000}
SERVER_CPUS=${SERVER_CPUS:-0}
CLIENT_CPUS=${CLIENT_CPUS:-1-3}
CLIENT_THREADS=${CLIENT_THREADS:-3}
CLIENTS=${CLIENTS:-80}
read -ra MODES <<<"${MODES:-tantalum tantalum-aof tantalum-uring tantalum-uring-aof redis redis-aof}"
read -ra TESTS <<<"${TESTS:-set-P1:1:0:1 set-P16:1:0:16 mix-P1:1:10:1 mix-P16:1:10:16}"
PORT=7001
DIR=/tmp/aof-bench
hz=$(getconf CLK_TCK)

server_cmd() {
  case $1 in
    tantalum) echo "$TANTALUM --port $PORT --threads 1 --io per-thread" ;;
    tantalum-aof) echo "$TANTALUM --port $PORT --threads 1 --io per-thread --aof $DIR" ;;
    tantalum-uring) echo "$TANTALUM --port $PORT --threads 1 --io uring" ;;
    tantalum-uring-aof) echo "$TANTALUM --port $PORT --threads 1 --io uring --aof $DIR" ;;
    redis) echo "redis-server --port $PORT --save '' --appendonly no" ;;
    redis-aof) echo "redis-server --port $PORT --save '' --appendonly yes --appendfsync everysec --dir $DIR" ;;
  esac
}
cpu() { sed 's/.*) //' "/proc/$1/stat" | awk '{ print $12 + $13 }'; }
now() { date +%s.%N; }
memtier() {
  taskset -c "$CLIENT_CPUS" memtier_benchmark -s 127.0.0.1 -p $PORT --hide-histogram \
    --distinct-client-seed --key-minimum 1 --key-maximum "$KEYS" --data-size 32 --random-data "$@"
}
start() {
  rm -rf $DIR && mkdir -p $DIR
  taskset -c "$SERVER_CPUS" bash -c "exec $(server_cmd "$1")" >"/tmp/$1.log" 2>&1 &
  pid=$!
  for _ in $(seq 1 100); do redis-cli -p $PORT ping >/dev/null 2>&1 && break; sleep 0.05; done
}
stop() {
  kill -9 "$pid" 2>/dev/null
  wait "$pid" 2>/dev/null
  # An io_uring server's listening socket closes after the process is gone.
  while (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null; do sleep 0.05; done
}

echo "# $(uname -r), $(nproc) CPUs, $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs)"
echo "# server on CPU $SERVER_CPUS, memtier $CLIENT_THREADS x $CLIENTS on $CLIENT_CPUS; $KEYS keys x 32 B, $TIME s per run"
for round in $(seq 1 "$ROUNDS"); do
  n=${#MODES[@]}
  for i in $(seq 0 $((n - 1))); do
    s=${MODES[$(((i + round - 1) % n))]}
    start "$s"
    memtier --threads 3 --clients 25 --pipeline 64 --ratio 1:0 --key-pattern P:P -n allkeys >/dev/null 2>&1
    for test in "${TESTS[@]}"; do
      IFS=: read -r name sets gets pipe <<<"$test"
      c0=$(cpu "$pid")
      t0=$(now)
      tot=$(memtier --threads "$CLIENT_THREADS" --clients "$CLIENTS" --pipeline "$pipe" \
        --ratio "$sets:$gets" --key-pattern R:R --test-time "$TIME" 2>/dev/null | grep '^Totals' | tr -s ' ')
      c1=$(cpu "$pid")
      t1=$(now)
      awk -v r="$round" -v s="$s" -v n="$name" -v tot="$tot" -v c=$((c1 - c0)) \
        -v t0="$t0" -v t1="$t1" -v hz="$hz" 'BEGIN {
          split(tot, a, " ")
          cores = c / hz / (t1 - t0)
          printf "round %d %-18s %-7s ops %8.0f p50 %.3f p99 %.3f cpu %.2f ops/core %8.0f\n",
            r, s, n, a[2], a[6], a[7], cores, a[2] / cores
        }'
    done
    case $s in
      *-aof)
        # A rewrite of what's there now, with SETs (pipeline 1) meanwhile.
        size=$(du -sk $DIR | cut -f1)
        memtier --threads 1 --clients 10 --pipeline 1 --ratio 1:0 --key-pattern R:R --test-time 4 \
          --print-percentiles 50,99,99.9,100 2>/dev/null | grep '^Totals' | tr -s ' ' >/tmp/during &
        load=$!
        sleep 1
        t0=$(now)
        redis-cli -p $PORT bgrewriteaof >/dev/null
        sleep 0.01
        while redis-cli -p $PORT info persistence | grep -q 'aof_rewrite_in_progress:1'; do sleep 0.01; done
        t1=$(now)
        wait $load
        awk -v r="$round" -v s="$s" -v t0="$t0" -v t1="$t1" -v kb="$size" -v d="$(cat /tmp/during)" 'BEGIN {
          split(d, a, " ")
          printf "round %d %-18s rewrite %.2f s (dir was %d MB); SETs meanwhile p50 %.3f p99 %.3f p99.9 %.3f max %.3f ms\n",
            r, s, t1 - t0, kb / 1024, a[6], a[7], a[8], a[9]
        }'
        ;;
    esac
    stop
  done
done
