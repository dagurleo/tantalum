#!/bin/bash
# Speed and memory guard: the in-process benchmark (src/bin/bench.rs) run with
# a base commit's build and the working tree's, in alternating rounds, so a
# change can't make Tantalum slower or bigger unnoticed. Every change to src/
# runs it; a slowdown or growth that stays needs a written reason (CLAUDE.md).
#
#   tools/speed-guard.sh [base ref, default HEAD] [rounds, default 12]
#
# Flags a workload when both its median over rounds and its best round are
# more than 3% slower (a real slowdown moves both; noise rarely does), and
# memory per key (or field) when it grows at all (it's exact). Exits 1 if
# anything is flagged. Metrics the base doesn't have yet are shown as new,
# to record as their baseline. Works like Gallium's engine/tools/speed-guard.sh: the base is
# exported with git archive into /tmp (once per tree) and built with its own
# target dir; both binaries run from paths of the same length, and each round
# pads the environment by a different length, because where the stack starts
# moves with them and that alone can shift timings by several percent.
set -euo pipefail
base_ref=${1:-HEAD}
rounds=${2:-12}
root=$(git rev-parse --show-toplevel)
tree=$(git -C "$root" rev-parse "$base_ref^{tree}")
base=/tmp/tantalum-speed-base/$tree
out=/tmp/tantalum-speed-results
rm -rf "$out" && mkdir -p "$out"
if [ ! -d "$base/src" ]; then
  mkdir -p "$base" && git -C "$root" archive "$base_ref" Cargo.toml Cargo.lock src | tar -x -C "$base"
fi
if [ ! -f "$base/src/bin/bench.rs" ]; then
  echo "$base_ref has no benchmark (src/bin/bench.rs): compare against a later commit" >&2
  exit 2
fi
# Modification times of both binaries (macOS stat, then Linux stat).
stamp() {
  local files=(/tmp/tantalum-speed-target/release/bench "$root/target/release/bench")
  { stat -f %m "${files[@]}" 2>/dev/null || stat -c %Y "${files[@]}" 2>/dev/null || true; } | tr '\n' ' '
}
before=$(stamp)
echo "building $base_ref and the working tree…" >&2
(cd "$base" && CARGO_TARGET_DIR=/tmp/tantalum-speed-target cargo build --release --bin bench -q)
(cd "$root" && cargo build --release --bin bench -q)
bins=/tmp/tantalum-speed-bin
# Removed first: macOS kills a signed binary copied over another in place.
mkdir -p "$bins" && rm -f "$bins/base" "$bins/curr"
cp /tmp/tantalum-speed-target/release/bench "$bins/base"
cp "$root/target/release/bench" "$bins/curr"
if [ "$(stamp)" != "$before" ]; then
  echo "compiled: letting the machine settle for 20 s…" >&2
  sleep 20
fi
# Each round alternates which side runs first, so warm-up or heat doesn't
# always fall on one side.
for round in $(seq 1 "$rounds"); do
  if [ $((round % 2)) = 1 ]; then order="base curr"; else order="curr base"; fi
  for side in $order; do
    TANTALUM_SPEED_PAD=$(printf "%$((round * 37 % 61 + 1))s" | tr ' ' x) \
      "$bins/$side" --runs 3 >"$out/$side-$round.json"
  done
  echo "round $round/$rounds done" >&2
done
python3 - "$out" "$rounds" "$base_ref" <<'PY'
import json, statistics, sys
out, rounds, ref = sys.argv[1], int(sys.argv[2]), sys.argv[3]
runs = {side: [json.load(open(f"{out}/{side}-{r}.json")) for r in range(1, rounds + 1)] for side in ("base", "curr")}
flagged = []
print(f"speed guard: {ref} (base) against the working tree, {rounds} rounds")
# Memory metrics are exact; timings are nanoseconds per command per run.
memory = (("memory_per_key", "memory per key", "B"), ("hash_memory_per_field", "hash memory/field", "B"))
timings = (("p1_ns", "no pipelining"), ("p16_ns", "pipeline 16"), ("hash_p16_ns", "hashes, pipe 16"))
new = lambda metric: metric not in runs["base"][0]
for metric, label, unit in memory:
    curr = runs["curr"][0][metric]
    if new(metric):
        print(f"  {label:17s}  new: {curr:8.3f} {unit}")
        continue
    base = runs["base"][0][metric]
    change = (curr / base - 1) * 100
    print(f"  {label:17s}  base {base:8.3f} {unit}   curr {curr:8.3f} {unit}   {change:+6.2f}%")
    if curr > base * 1.001:
        flagged.append(label)
for metric, label in timings:
    per_round = {side: [statistics.median(r[metric]) for r in runs[side]] for side in runs if not (side == "base" and new(metric))}
    med = {side: statistics.median(v) for side, v in per_round.items()}
    best = {side: min(v) for side, v in per_round.items()}
    if new(metric):
        print(f"  {label:17s}  new: median {med['curr']:6.1f} ns, best {best['curr']:6.1f} ns")
        continue
    dm, db = (med["curr"] / med["base"] - 1) * 100, (best["curr"] / best["base"] - 1) * 100
    print(f"  {label:17s}  median {med['base']:6.1f} -> {med['curr']:6.1f} ns ({dm:+5.1f}%)   best {best['base']:6.1f} -> {best['curr']:6.1f} ns ({db:+5.1f}%)")
    if dm > 3 and db > 3:
        flagged.append(label)
if flagged:
    print("FLAGGED: " + ", ".join(flagged) + " (keep it only with a written reason)")
    sys.exit(1)
print("ok: no slowdown, no memory growth")
PY
