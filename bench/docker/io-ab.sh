#!/usr/bin/env bash
# I/O backend A/B inside the benchmark image, where io_uring is allowed:
#
#   docker build -t tantalum-bench .
#   docker run -d --name tantalum-io --security-opt seccomp=unconfined \
#     --ulimit memlock=-1 -e ROLE=idle tantalum-bench
#   docker exec -i tantalum-io bash < bench/docker/io-ab.sh
#
# Docker's default seccomp profile blocks io_uring (as Railway's does), so
# without seccomp=unconfined the "uring" servers silently fall back to epoll.
# Every server and memtier share one VM: compare rows within a run only.
set -uo pipefail

THREADS=${THREADS:-4}
ROUNDS=${ROUNDS:-3}
TIME=${TIME:-12}
read -ra SERVERS <<<"${SERVERS:-per-thread uring uring-coop uring-plain memcached dragonfly}"

declare -A PID PORT
start() {
  local name=$1 port=$2
  shift 2
  "$@" >"/tmp/$name.log" 2>&1 &
  PID[$name]=$!
  PORT[$name]=$port
}
start per-thread 7961 tantalum --port 7961 --threads "$THREADS" --io per-thread
start uring 7962 tantalum --port 7962 --threads "$THREADS" --io uring
start uring-coop 7963 env TANTALUM_URING_SETUP=coop tantalum --port 7963 --threads "$THREADS" --io uring
start uring-plain 7964 env TANTALUM_URING_SETUP=plain tantalum --port 7964 --threads "$THREADS" --io uring
start memcached 7965 memcached -u root -l 127.0.0.1 -p 7965 -U 0 -t "$THREADS" -c 4096 -m 8192
start dragonfly 7966 dragonfly --bind 127.0.0.1 --port 7966 --proactor_threads "$THREADS" --dbfilename=
sleep 3

echo "# $(uname -r), $(nproc) CPUs, $THREADS server threads; $(head -1 /tmp/uring.log)"
for s in "${SERVERS[@]}"; do
  rings=$(ls -l "/proc/${PID[$s]}/fd" 2>/dev/null | grep -c 'io_uring')
  echo "# $s: pid ${PID[$s]}, io_uring fds: $rings"
done

proto() { [ "$1" = memcached ] && echo memcache_text || echo redis; }
# utime + stime of a process, in clock ticks.
cpu() { sed 's/.*) //' "/proc/$1/stat" | awk '{ print $12 + $13 }'; }

for s in "${SERVERS[@]}"; do
  memtier_benchmark -s 127.0.0.1 -p "${PORT[$s]}" --protocol="$(proto "$s")" --hide-histogram \
    --distinct-client-seed --threads 4 --clients 25 --pipeline 64 --ratio 1:0 --key-pattern P:P \
    --key-minimum 1 --key-maximum 1000000 -n allkeys --data-size 32 --random-data >/dev/null 2>&1
done

hz=$(getconf CLK_TCK)
echo "# 1M keys x 32 B, 1:10 SET:GET, ${TIME} s per run; P1-200: no pipelining, 200 clients;"
echo "# P1-50: 50 clients; P16-200: pipeline 16. p50/p99 in ms; cpu = server cores used."
for round in $(seq 1 "$ROUNDS"); do
  # Rotate the order so host noise doesn't always hit the same server.
  n=${#SERVERS[@]}
  for i in $(seq 0 $((n - 1))); do
    s=${SERVERS[$(((i + round - 1) % n))]}
    for test in P1-200:8:1 P1-50:2:1 P16-200:8:16; do
      IFS=: read -r name threads pipe <<<"$test"
      c0=$(cpu "${PID[$s]}")
      t0=$(date +%s.%N)
      tot=$(memtier_benchmark -s 127.0.0.1 -p "${PORT[$s]}" --protocol="$(proto "$s")" --hide-histogram \
        --distinct-client-seed --threads "$threads" --clients 25 --pipeline "$pipe" --ratio 1:10 \
        --key-pattern R:R --key-minimum 1 --key-maximum 1000000 --data-size 32 --random-data \
        --test-time "$TIME" 2>/dev/null | grep '^Totals' | tr -s ' ')
      c1=$(cpu "${PID[$s]}")
      t1=$(date +%s.%N)
      awk -v r="$round" -v s="$s" -v n="$name" -v tot="$tot" -v c=$((c1 - c0)) \
        -v t0="$t0" -v t1="$t1" -v hz="$hz" 'BEGIN {
          split(tot, a, " ")
          printf "round %d %-12s %-7s ops %9.0f p50 %.3f p99 %.3f cpu %.2f\n",
            r, s, n, a[2], a[6], a[7], c / hz / (t1 - t0)
        }'
    done
  done
done

# How evenly each server's threads shared the work, in CPU seconds per thread.
for s in "${SERVERS[@]}"; do
  echo "# $s thread cpu s: $(for t in /proc/${PID[$s]}/task/*; do
    sed 's/.*) //' "$t/stat" | awk -v hz="$hz" '{ c = ($12 + $13) / hz; if (c >= 1) printf "%.0f ", c }'
  done)"
done
kill -9 "${PID[@]}" 2>/dev/null
