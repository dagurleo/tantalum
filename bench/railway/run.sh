#!/usr/bin/env bash
# Entry point for the Railway benchmark (see Dockerfile).
#
# ROLE=server: runs every server under test on its own port.
# ROLE=bench:  benchmarks the servers on SERVER_HOST in turn, prints results,
#              then idles (unless BENCH_IDLE=0) so the logs stay attached to a
#              running deployment.
# ROLE=local:  starts the servers and benchmarks them over loopback in the same
#              container, ROUNDS times, also reporting each server's CPU use.
# ROLE=suite:  the wider comparison in suite.sh, adding memcached and Dragonfly.
set -uo pipefail

THREADS=${THREADS:-4}
EVICTION_MAXMEMORY=${EVICTION_MAXMEMORY:-64mb}
ROUNDS=${ROUNDS:-1}
declare -A PIDS # port -> server pid, known only when we started the servers

# name:port. Throughput targets also get the plain memory test.
THROUGHPUT_TARGETS=(tantalum:6379 redis:6380 redis-io-threads:6381 valkey:6384)
TTL_TARGETS=(tantalum-ttl:6385 redis-ttl:6386 valkey-ttl:6387)
EVICTION_TARGETS=(tantalum-lru:6382 redis-lru:6383 valkey-lru:6388)

launch() {
  local port=$1
  shift
  "$@" &
  PIDS[$port]=$!
}

start_servers() {
  local common=(--save '' --appendonly no --protected-mode no --bind '* -::*')
  local lru=(--maxmemory "$EVICTION_MAXMEMORY" --maxmemory-policy allkeys-lru)
  launch 6379 tantalum --bind :: --port 6379 --threads "$THREADS"
  launch 6380 redis-server --port 6380 "${common[@]}"
  launch 6381 redis-server --port 6381 "${common[@]}" --io-threads "$THREADS"
  launch 6382 tantalum --bind :: --port 6382 --threads "$THREADS" "${lru[@]}"
  launch 6383 redis-server --port 6383 "${common[@]}" "${lru[@]}"
  launch 6384 valkey-server --port 6384 "${common[@]}"
  launch 6385 tantalum --bind :: --port 6385 --threads "$THREADS"
  launch 6386 redis-server --port 6386 "${common[@]}"
  launch 6387 valkey-server --port 6387 "${common[@]}"
  launch 6388 valkey-server --port 6388 "${common[@]}" "${lru[@]}"
}

# CPU time (utime + stime, in clock ticks) of a process. The command name in
# /proc/pid/stat can contain spaces, so count fields after its closing paren.
cpu_ticks() {
  sed 's/.*) //' "/proc/$1/stat" | awk '{ print $12 + $13 }'
}

server() {
  start_servers
  wait -n
  echo "a server exited; stopping"
  exit 1
}

bench() {
  local host=${SERVER_HOST:?set SERVER_HOST}
  for target in "${THROUGHPUT_TARGETS[@]}" "${TTL_TARGETS[@]}" "${EVICTION_TARGETS[@]}"; do
    local port=${target##*:} tries=0
    until redis-cli -h "$host" -p "$port" PING 2>/dev/null | grep -q PONG; do
      tries=$((tries + 1))
      if [ "$tries" -gt 1200 ]; then
        echo "gave up waiting for $target"
        exit 1
      fi
      sleep 1
    done
  done

  local version='^(redis_version|valkey_version|tantalum_version|os):'
  echo "### client: $(nproc) CPUs, $(lscpu | sed -n 's/^Model name: *//p')"
  for port in 6379 6380 6384; do
    echo "### server: $(redis-cli -h "$host" -p "$port" INFO server | tr -d '\r' | grep -E "$version" | paste -sd' ')"
  done

  # --distinct-client-seed matters: without it every client replays the same
  # key sequence, so 200 clients hammer the same keys in lockstep.
  mt() {
    memtier_benchmark -s "$host" --protocol=redis --hide-histogram --distinct-client-seed "$@" 2>/dev/null |
      sed -n '/ALL STATS/,/^Totals/p'
  }
  # Like mt, plus the server's average CPU use when we know its pid.
  run() {
    local port=$1 pid=${PIDS[$1]:-}
    shift
    if [ -z "$pid" ]; then
      mt -p "$port" "$@"
      return
    fi
    local c0 t0 c1 t1
    c0=$(cpu_ticks "$pid")
    t0=$(date +%s.%N)
    mt -p "$port" "$@"
    c1=$(cpu_ticks "$pid")
    t1=$(date +%s.%N)
    awk -v c="$((c1 - c0))" -v t0="$t0" -v t1="$t1" -v hz="$(getconf CLK_TCK)" \
      'BEGIN { printf "server_cpu_cores: %.2f\n", c / hz / (t1 - t0) }'
  }
  info() {
    redis-cli -h "$host" -p "$1" INFO | tr -d '\r' |
      grep -E '^(used_memory|used_memory_rss|maxmemory|evicted_keys|expired_keys|db0|segment_bytes|segment_live_bytes|external_bytes|table_bytes):'
  }
  prefill() {
    mt -p "$1" --threads 4 --clients 25 --pipeline 64 --ratio 1:0 \
      --key-pattern P:P --key-minimum 1 --key-maximum 1000000 -n allkeys --data-size 32 "${@:2}" >/dev/null
  }

  # Memory: 1M keys with 32-byte values, read after allocators settle.
  for target in "${THROUGHPUT_TARGETS[@]}"; do
    prefill "${target##*:}"
  done
  for target in "${TTL_TARGETS[@]}"; do
    prefill "${target##*:}" --expiry-range 3600-7200
  done
  sleep 5
  for target in "${THROUGHPUT_TARGETS[@]}"; do
    echo "### memory, ${target%%:*}: 1M keys, 32-byte values"
    info "${target##*:}"
  done
  for target in "${TTL_TARGETS[@]}"; do
    echo "### memory, ${target%%:*}: 1M keys, 32-byte values, 1-2 h TTLs"
    info "${target##*:}"
  done

  # Rounds alternate between servers so drift on a shared host hits all of them.
  for round in $(seq 1 "$ROUNDS"); do
    for target in "${THROUGHPUT_TARGETS[@]}"; do
      local name=${target%%:*} port=${target##*:}
      echo "### round $round, $name: 1 client, no pipelining (round-trip latency)"
      run "$port" --threads 1 --clients 1 --pipeline 1 --ratio 1:10 \
        --key-pattern R:R --key-minimum 1 --key-maximum 1000000 --data-size 32 --test-time 10
      for pipeline in 1 16; do
        echo "### round $round, $name: 200 clients, pipeline $pipeline, 1:10 set:get"
        run "$port" --threads 8 --clients 25 --pipeline "$pipeline" --ratio 1:10 \
          --key-pattern R:R --key-minimum 1 --key-maximum 1000000 --data-size 32 --test-time 20
      done
    done
  done

  for target in "${EVICTION_TARGETS[@]}"; do
    local name=${target%%:*} port=${target##*:}
    # Gaussian keys (sigma 100k) touch far more keys than fit, so the cache
    # has to evict, and a good policy keeps the popular middle of the range.
    local workload=(--threads 8 --clients 25 --pipeline 1 --ratio 1:4 --key-pattern G:G
      --key-minimum 1 --key-maximum 4000000 --key-stddev 100000 --data-size 100)
    echo "### $name: warm-up, maxmemory $EVICTION_MAXMEMORY, gaussian keys (sigma 100k), 100-byte values"
    mt -p "$port" "${workload[@]}" --test-time 20 >/dev/null
    echo "### $name: measured run"
    # --randomize: otherwise the fixed seeds replay the warm-up's key sequence
    # (see suite.sh), which skews hit rates by eviction policy.
    run "$port" "${workload[@]}" --randomize --test-time 30
    info "$port"
  done

  echo "### DONE"
  if [ "${BENCH_IDLE:-1}" = 1 ]; then
    sleep infinity
  fi
}

case "${ROLE:-server}" in
  # tantalum against Redis, Valkey, memcached and Dragonfly (see suite.sh).
  suite) exec /usr/local/bin/suite.sh ;;
  # Nothing; for running experiments over `railway ssh`.
  idle) exec sleep infinity ;;
  server) server ;;
  bench) bench ;;
  local)
    start_servers
    SERVER_HOST=127.0.0.1 bench
    ;;
  *) echo "unknown ROLE=$ROLE"; exit 2 ;;
esac
