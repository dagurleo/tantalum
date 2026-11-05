# Consumer groups in memtier: Redis, Valkey and /root/tantalum-groups, each on
# CPU 0 with 3 x 80 clients on CPUs 1-3, each test on all of them back to back.
# Runs on the Agent37 VM, in the bench image. Results: results-groups.txt.
docker rm -f bench >/dev/null 2>&1
docker run -d --name bench --privileged -v /root:/prof -e ROLE=idle tantalum-bench >/dev/null
sleep 2
cat > /root/gm-inside.sh <<'EOF'
command -v python3 >/dev/null || { apt-get update -qq >/dev/null 2>&1; apt-get install -y -qq python3-minimal >/dev/null 2>&1; }
python3 -I - <<'PY'
def cmd(*a):
    return b"*%d\r\n" % len(a) + b"".join(b"$%d\r\n%s\r\n" % (len(x), x) for x in a)
v = b"v" * 32
with open("/tmp/fill.resp", "wb") as f:
    for i in range(1, 5_000_001):
        f.write(cmd(b"XADD", b"s", b"%d-0" % i, b"f", v))
PY
taskset -c 0 redis-server --port 7201 --save '' --appendonly no >/tmp/7201.log 2>&1 &
R=$!
taskset -c 0 valkey-server --port 7202 --save '' --appendonly no >/tmp/7202.log 2>&1 &
V=$!
taskset -c 0 /prof/tantalum-groups --port 7203 --threads 1 --io per-thread >/tmp/7203.log 2>&1 &
T=$!
sleep 2
NAMES=(redis valkey tantalum)
PORTS=(7201 7202 7203)
mtb() { # port pipeline "command" limit...
  local port=$1 pipe=$2 command=$3; shift 3
  taskset -c 1-3 memtier_benchmark -s 127.0.0.1 -p "$port" --hide-histogram --threads 3 --clients 80 --pipeline "$pipe" \
    --command "$command" --command-key-pattern S --key-prefix= --key-minimum "${FROM:-1}" --key-maximum "${TO:-5000000}" --data-size 32 \
    --print-percentiles 50,99 "$@" 2>/dev/null | awk '/^Totals/ { print; exit }'
}
each() { # round name pipeline "command" limit...
  local round=$1 name=$2 pipe=$3 command=$4; shift 4
  for i in 0 1 2; do
    local k=$(( (i + round) % 3 ))
    local line
    line=$(mtb "${PORTS[$k]}" "$pipe" "$command" "$@")
    echo "round $round ${NAMES[$k]} $name P$pipe $line"
    [ -n "${AFTER:-}" ] && redis-cli -p "${PORTS[$k]}" $AFTER >/dev/null
  done
}
READ1='XREADGROUP GROUP g c COUNT 1 STREAMS s >'
READ10='XREADGROUP GROUP g c COUNT 10 STREAMS s >'
for round in 1 2; do
  for p in "${PORTS[@]}"; do
    redis-cli -p "$p" FLUSHALL >/dev/null
    redis-cli -p "$p" XGROUP CREATE s g 0 MKSTREAM >/dev/null
    redis-cli -p "$p" --pipe < /tmp/fill.resp | tail -1
  done
  # 1M, then 2M: each client's share of a fixed count, so none runs out.
  each $round read1 1 "$READ1" --requests=4166
  each $round read1 16 "$READ1" --requests=8333
  each $round pending 1 'XPENDING s g' --test-time=4
  each $round pending 16 'XPENDING s g' --test-time=4
  each $round pending10 1 'XPENDING s g - + 10' --test-time=4
  each $round pending10 16 'XPENDING s g - + 10' --test-time=4
  each $round groups 1 'XINFO GROUPS s' --test-time=4
  each $round groups 16 'XINFO GROUPS s' --test-time=4
  FROM=1 TO=1000000 each $round ack 1 'XACK s g __key__-0' --requests=2500
  FROM=1000001 TO=3000000 each $round ack 16 'XACK s g __key__-0' --requests=8333
  for p in "${PORTS[@]}"; do
    redis-cli -p "$p" XGROUP DESTROY s g >/dev/null
    redis-cli -p "$p" XGROUP CREATE s g 0 >/dev/null
  done
  each $round read10 1 "$READ10" --requests=833
  each $round read10 16 "$READ10" --requests=1250
  AFTER="DEL w" each $round add 1 'XADD w * f __data__' --test-time=4
  AFTER="DEL w" each $round add 16 'XADD w * f __data__' --test-time=4
done
kill -9 $R $V $T
EOF
docker exec bench bash /prof/gm-inside.sh 2>&1
docker stop bench >/dev/null
echo JOB-DONE
