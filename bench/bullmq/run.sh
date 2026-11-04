#!/usr/bin/env bash
# BullMQ (6.5.4) on each server: jobs per second and the server's CPU time
# per job, adding JOBS jobs (addBulk, 500 at a time) and then processing them
# (WORKERS processes, CONCURRENCY each, doing nothing). Each server runs alone
# on SERVER_CPUS, the clients on CLIENT_CPUS. Server CPU comes from /proc, so
# it's per job even when the clients are what limits the rate.
#
# TANTALUM picks a build; TANTALUM_B adds a second as tantalum-b(-uring).
set -uo pipefail

JOBS=${JOBS:-50000}
WORKERS=${WORKERS:-3}
CONCURRENCY=${CONCURRENCY:-100}
ROUNDS=${ROUNDS:-2}
SERVER_CPUS=${SERVER_CPUS:-0}
CLIENT_CPUS=${CLIENT_CPUS:-1-3}
TANTALUM=${TANTALUM:-tantalum}
read -ra SERVERS <<<"${SERVERS:-tantalum tantalum-uring redis valkey dragonfly}"
PORT=7001
JS=/bullmq/jobs.mjs
hz=$(getconf CLK_TCK)

server_cmd() {
  case $1 in
    tantalum) echo "$TANTALUM --port $PORT --threads 1 --io per-thread" ;;
    tantalum-uring) echo "$TANTALUM --port $PORT --threads 1 --io uring" ;;
    tantalum-b) echo "$TANTALUM_B --port $PORT --threads 1 --io per-thread" ;;
    tantalum-b-uring) echo "$TANTALUM_B --port $PORT --threads 1 --io uring" ;;
    redis) echo "redis-server --port $PORT --save '' --appendonly no" ;;
    valkey) echo "valkey-server --port $PORT --save '' --appendonly no" ;;
    # BullMQ's scripts reach keys they don't declare.
    dragonfly) echo "dragonfly --port $PORT --proactor_threads 1 --dbfilename= --default_lua_flags=allow-undeclared-keys" ;;
  esac
}
cpu() { sed 's/.*) //' "/proc/$1/stat" | awk '{ print $12 + $13 }'; }
now() { date +%s.%N; }
backlog() {
  redis-cli -p $PORT eval "return redis.call('llen', KEYS[1]) + redis.call('llen', KEYS[2])" 2 bull:bench:wait bull:bench:active 2>/dev/null
}

echo "# $(uname -r), $(nproc) CPUs, $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs)"
echo "# BullMQ $(node -p "require('/bullmq/node_modules/bullmq/package.json').version"), node $(node --version);" \
  "$JOBS jobs, $WORKERS workers x $CONCURRENCY; server on CPU $SERVER_CPUS, clients on $CLIENT_CPUS"
for round in $(seq 1 "$ROUNDS"); do
  n=${#SERVERS[@]}
  for i in $(seq 0 $((n - 1))); do
    s=${SERVERS[$(((i + round - 1) % n))]}
    # exec, so $! is the server itself.
    taskset -c "$SERVER_CPUS" bash -c "exec $(server_cmd "$s")" >"/tmp/$s.log" 2>&1 &
    pid=$!
    for _ in $(seq 1 100); do redis-cli -p $PORT ping >/dev/null 2>&1 && break; sleep 0.05; done
    c0=$(cpu $pid)
    added=$(taskset -c "$CLIENT_CPUS" node $JS add $PORT "$JOBS" 2>&1)
    c1=$(cpu $pid)
    add_s=$(echo "$added" | sed -n 's/.*"seconds":\([0-9.]*\).*/\1/p')
    workers=()
    for _ in $(seq 1 "$WORKERS"); do
      taskset -c "$CLIENT_CPUS" node $JS work $PORT "$CONCURRENCY" >>"/tmp/$s.workers.log" 2>&1 &
      workers+=($!)
    done
    # Timed from the first job taken, so worker start-up isn't counted.
    while [ "$(backlog)" = "$JOBS" ]; do sleep 0.005; done
    t2=$(now); c2=$(cpu $pid)
    while [ "$(backlog)" != "0" ]; do sleep 0.02; done
    t3=$(now); c3=$(cpu $pid)
    kill -TERM "${workers[@]}" 2>/dev/null
    wait "${workers[@]}" 2>/dev/null
    awk -v r="$round" -v s="$s" -v j="$JOBS" -v as="$add_s" -v ac=$((c1 - c0)) -v pc=$((c3 - c2)) \
      -v t2="$t2" -v t3="$t3" -v hz="$hz" -v err="$added" 'BEGIN {
        if (as == "") { printf "round %d %-16s failed: %s\n", r, s, substr(err, 1, 200); exit }
        ps = t3 - t2
        printf "round %d %-16s add %7.0f jobs/s %6.1f us/job | process %7.0f jobs/s %6.1f us/job, server %.2f cores\n",
          r, s, j / as, ac / hz / j * 1e6, j / ps, pc / hz / j * 1e6, pc / hz / ps
      }'
    kill -9 $pid 2>/dev/null
    wait $pid 2>/dev/null
  done
done
