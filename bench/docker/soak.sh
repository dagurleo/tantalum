#!/usr/bin/env bash
# Soak test: Tantalum under a memory limit with constant eviction, TTL
# churn, counters, big values and connection churn, for MINUTES per I/O
# mode. Samples INFO and RSS every 10 s, then checks that memory stayed
# within the limit, that client bookkeeping went back to baseline, and that
# SIGTERM shuts down cleanly with clients still running.
#
#   docker run -d --name tantalum-soak --security-opt seccomp=unconfined \
#     -e ROLE=idle tantalum-bench
#   docker exec -i tantalum-soak bash < bench/docker/soak.sh
set -uo pipefail

MINUTES=${MINUTES:-5}
MAXMEMORY_MB=${MAXMEMORY_MB:-64}
read -ra MODES <<<"${MODES:-per-thread uring}"
PORT=7990
failed=0

info() { redis-cli -p $PORT INFO 2>/dev/null | tr -d '\r' | grep -E "^$1:" | cut -d: -f2; }
rss_kb() { awk '/VmRSS/ { print $2 }' "/proc/$1/status"; }
check() { # name, condition
  if eval "$2"; then echo "  ok: $1"; else echo "  FAIL: $1 ($2)"; failed=1; fi
}

for mode in "${MODES[@]}"; do
  echo "== $mode: ${MINUTES} min, --maxmemory ${MAXMEMORY_MB}mb allkeys-lru, 4 threads"
  tantalum --port $PORT --threads 4 --io "$mode" --maxmemory "${MAXMEMORY_MB}mb" \
    --maxmemory-policy allkeys-lru >/tmp/soak-server.log 2>&1 &
  server=$!
  sleep 1
  head -1 /tmp/soak-server.log
  baseline_clients=$(info mem_clients_normal)
  secs=$((MINUTES * 60))
  # The load outlasts the sampling, so SIGTERM below lands while it's running.
  common=(-s 127.0.0.1 -p $PORT --hide-histogram --distinct-client-seed --test-time $((secs + 60)))
  # Cache traffic: values of 32 B to 2 KB with TTLs, over far more keys than fit.
  memtier_benchmark "${common[@]}" -t 3 -c 20 --pipeline 4 --key-maximum 5000000 \
    --data-size-range 32-2048 --random-data \
    --command "SET __key__ __data__ EX 120" --command-ratio 2 --command-key-pattern R \
    --command "GET __key__" --command-ratio 8 --command-key-pattern R \
    >/tmp/soak-cache.log 2>&1 &
  # Counters and TTL changes on their own keys, reconnecting every 500 requests.
  memtier_benchmark "${common[@]}" -t 1 -c 10 --key-prefix c: --key-maximum 100000 \
    --reconnect-interval 500 \
    --command "INCRBY __key__ 3" --command-ratio 6 --command-key-pattern R \
    --command "EXPIRE __key__ 30" --command-ratio 1 --command-key-pattern R \
    --command "TTL __key__" --command-ratio 1 --command-key-pattern R \
    --command "PERSIST __key__" --command-ratio 1 --command-key-pattern R \
    --command "DEL __key__" --command-ratio 1 --command-key-pattern R \
    >/tmp/soak-counters.log 2>&1 &
  # A few big values (stored outside segments) coming and going.
  memtier_benchmark "${common[@]}" -t 1 -c 2 --key-prefix big: --key-maximum 2000 \
    --data-size 100000 --random-data --ratio 1:3 --rate-limiting 50 \
    >/tmp/soak-big.log 2>&1 &
  echo "  time  used_mb  rss_mb  clients_kb  conns  keys     evicted   expired"
  max_used=0
  max_rss=0
  for ((t = 10; t <= secs; t += 10)); do
    sleep 10
    if ! kill -0 $server 2>/dev/null; then
      echo "  FAIL: server died"
      failed=1
      break
    fi
    used=$(info used_memory)
    rss=$(($(rss_kb $server) * 1024))
    ((used > max_used)) && max_used=$used
    ((rss > max_rss)) && max_rss=$rss
    keys=$(redis-cli -p $PORT DBSIZE)
    printf "  %4ds  %7d  %6d  %10d  %5s  %-7s  %-8s  %s\n" $t $((used >> 20)) $((rss >> 20)) \
      $(($(info mem_clients_normal) >> 10)) "$(info connected_clients)" "$keys" \
      "$(info evicted_keys)" "$(info expired_keys)"
  done
  limit=$((MAXMEMORY_MB << 20))
  echo "  peak used_memory $((max_used >> 20)) MB, peak RSS $((max_rss >> 20)) MB, limit $MAXMEMORY_MB MB"
  # used_memory may overshoot by the documented batching error (~1.6%) plus
  # one write in flight per connection.
  check "used_memory stayed within 3% of the limit" "((max_used <= limit * 103 / 100))"
  # RSS adds the allocator's and kernel's overheads, the binary and threads.
  check "RSS stayed within 1.5x the limit" "((max_rss <= limit * 3 / 2))"

  # SIGTERM while all three memtiers are still sending.
  sleep 2
  echo "  connections open at SIGTERM: $(info connected_clients)"
  kill -TERM $server
  start=$(date +%s%N)
  wait $server
  code=$?
  took=$((($(date +%s%N) - start) / 1000000))
  echo "  SIGTERM with clients connected: exit $code after ${took} ms; $(tail -1 /tmp/soak-server.log)"
  check "exit code 0" "((code == 0))"
  check "shut down within the 10 s grace period" "((took < 10000))"
  wait
  for log in cache counters big; do
    echo "  memtier $log: $(grep -E '^Totals' /tmp/soak-$log.log | awk '{ print $2 " ops/s" }')"
  done

  # Client bookkeeping goes back to baseline after a burst of short connections.
  tantalum --port $PORT --threads 4 --io "$mode" >/tmp/soak-server.log 2>&1 &
  server=$!
  sleep 1
  memtier_benchmark -s 127.0.0.1 -p $PORT --hide-histogram -t 4 -c 50 -n 200 --reconnect-interval 20 \
    --ratio 1:1 >/dev/null 2>&1
  sleep 1
  check "connected_clients back to 1 (this check)" "(($(info connected_clients) == 1))"
  check "mem_clients_normal back to baseline" "(($(info mem_clients_normal) == baseline_clients))"
  check "total_connections_received counted the churn" "(($(info total_connections_received) > 2000))"
  kill -TERM $server
  wait $server
done
if ((failed)); then echo "SOAK FAILED"; else echo "SOAK PASSED"; fi
