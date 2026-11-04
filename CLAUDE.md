# Tantalum: rules for every change

Tantalum's point is speed and memory: a Redis-compatible server that keeps
the most data in the least memory and does the most work per core. Two rules
come before any feature.

## 1. Speed and memory never get worse without a reason

- **Every change to `src/` runs `tools/speed-guard.sh`** against the last
  commit (or the branch point) before it's committed, and it must say `ok`.
  It runs the in-process benchmark (`src/bin/bench.rs`: the server's own
  parsing, prefetching and commands on 1M keys, no sockets) for both builds
  in alternating rounds. It flags a workload when its median and its best
  round are both more than 3% slower, and memory per key when it grows at all.
- **A slowdown or growth that stays needs a written reason, agreed with the
  user**: in the commit message, and if it lasts, in the README's notes. "Feature
  X needed it" counts only if X can't be had without it.
- **Changes to the I/O paths** (`server.rs`'s connection loops, `context.rs`,
  anything done per request or per connection) also get an end-to-end A/B over
  real sockets: `bench/docker/saturate.sh` with `TANTALUM_B` set to the
  previous build, in paired rounds, and on real Linux with io_uring
  (`bench/aws/`) when the change is about I/O itself. The in-process
  benchmark can't see syscalls.
- **CI checks memory budgets** on every push (`memory_budget` in
  `src/store/tests.rs`); raise a budget only with a written reason.
- When a benchmark gets more realistic or a new workload is added, record the
  new baseline in the commit message.

## 2. What exists keeps working, unchanged

- **New features cost nothing when unused.** Commands and data that don't use
  them run the same code and take the same bytes. New data types must not add
  bytes to string entries or work to `GET`/`SET`; put a feature's cost on its
  own commands.
- **Replies match Redis 7 byte for byte**, errors included. Tests check exact
  replies; when in doubt, compare with a real Redis.
- **Faster paths give identical results** to the code they replace. Prove it
  with tests and, for anything stateful, the randomized model test
  (`matches_a_model`) and the fuzzers.
- **Snapshots stay readable.** `main` deploys to production holding real data
  (ContentMod's settings), so a new build must load snapshots written by the
  previous one. Change the format only with a new version number in its
  header, keep reading the old version, and test both.

## Before every commit

1. `cargo fmt`, `cargo clippy --all-targets -- -D warnings`, `cargo test`, and
   when a public API changed, `cd fuzz && cargo +nightly fuzz build` (the fuzz
   target is its own crate, so the rest doesn't compile it).
2. `tools/speed-guard.sh` says `ok`, or the commit says why not.
3. For I/O changes, the end-to-end A/B.
4. The README matches: commands table, design notes, "Not there yet".

CI then runs the tests on Linux, two minutes of fuzzing and the
deploy-image check (macOS is off while the repository is private, so the
`cargo test` above, on a Mac, covers it). Railway deploys `main` to
production only after CI passes.
