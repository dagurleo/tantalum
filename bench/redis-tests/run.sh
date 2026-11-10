#!/usr/bin/env bash
# Redis 7.2.4's own test suite (its tests/ directory, run by its tcl harness) on
# Tantalum and on Redis, both ways the harness runs: against one server we start
# per file (external, as Redis' CI does) and with the servers the harness starts
# itself from a config file (spawn: src/redis-server is redis-server-wrap.py,
# which starts Tantalum with those settings). Results land in $RT_WORK as
# SERVER-MODE.json; compare.py and categorize.py read them.
#
#   run.sh setup MODE WORKERS     once: builds Redis 7.2.4 from $REDIS_TGZ (for
#                                 redis-cli, redis-benchmark and the check tools)
#                                 and makes WORKERS test trees per server
#   run.sh MODE WORKERS [FILE...] both servers side by side, WORKERS files at a
#                                 time each (default: every file but cluster's)
#
# Needs tclsh and python3, $TANTALUM_BIN and $REDIS_BIN (the redis:7.2.4
# image's redis-server). On a 4-CPU Agent37 VM, with 2 workers each, external
# takes about 30 minutes and spawn about 90 (tests that hang wait 3 minutes).
set -uo pipefail

here=$(cd "$(dirname "$0")" && pwd)
export RT_WORK=${RT_WORK:-/root/rt}
export TANTALUM_BIN=${TANTALUM_BIN:-/root/tantalum-rt}
export REDIS_BIN=${REDIS_BIN:-/root/redis-724}
REDIS_TGZ=${REDIS_TGZ:-/root/redis-7.2.4.tar.gz}

if [ "${1:-}" = setup ]; then
  mode=$2 n=$3
  rm -rf "$RT_WORK" && mkdir -p "$RT_WORK/logs" && cd "$RT_WORK" || exit 1
  tar xzf "$REDIS_TGZ" && (cd redis-7.2.4 && make -j4 BUILD_TLS=no > "$RT_WORK/make.log" 2>&1) || exit 1
  python3 -I "$here/patch-helper.py" redis-7.2.4/tests/test_helper.tcl || exit 1
  for srv in tantalum redis; do
    for w in $(seq 0 $((n - 1))); do
      t=$srv-$mode-$w
      mkdir -p "$t/src" && cp -r redis-7.2.4/tests "$t/"
      for b in redis-cli redis-benchmark redis-check-aof redis-check-rdb redis-sentinel; do
        cp "redis-7.2.4/src/$b" "$t/src/"
      done
      if [ $srv = redis ]; then
        cp redis-7.2.4/src/redis-server "$t/src/"
      else
        cp "$here/redis-server-wrap.py" "$t/src/redis-server" && chmod +x "$t/src/redis-server"
      fi
    done
  done
  exit 0
fi

mode=$1 n=$2; shift 2
files=("$@")
if [ ${#files[@]} = 0 ]; then
  # Redis' own list (test_helper.tcl's all_tests), but cluster mode's.
  mapfile -t files < <(awk '/^set ::all_tests/{f=1;next} /^}/{f=0} f{print $1}' \
    "$RT_WORK/redis-7.2.4/tests/test_helper.tcl" | grep -v '^unit/cluster/')
fi
cd "$RT_WORK" || exit 1
python3 -I "$here/rt.py" "$mode" tantalum "$RT_WORK/tantalum-$mode.json" "$n" "${files[@]}" \
  > "$RT_WORK/tantalum-$mode.out" 2>&1 &
python3 -I "$here/rt.py" "$mode" redis "$RT_WORK/redis-$mode.json" "$n" "${files[@]}" \
  > "$RT_WORK/redis-$mode.out" 2>&1
wait
