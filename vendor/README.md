# Vendored C code

Built by `build.rs` into the server, for Lua scripting (`EVAL`). All of it
comes unchanged from Redis 7.2.4 (BSD-3-Clause, the last release before
Redis changed its license), so scripts run on exactly the Lua Redis runs:

- `lua/src`: Redis' `deps/lua/src`, Lua 5.1.5 (MIT, `lua/COPYRIGHT`) with
  Redis' patches (read-only tables, security fixes) and the libraries
  scripts get: `cjson` (MIT), `cmsgpack` (BSD-2-Clause), `struct` (MIT) and
  `bit` (MIT), each with its license at the top of its file. `lua.c`,
  `luac.c` and `print.c` (the stand-alone interpreter and compiler) are left
  out. One line differs: `lua_cjson.c` no longer includes Redis'
  `src/solarisfixes.h`, which only matters on Solaris.
- `fpconv`: Redis' `deps/fpconv` (Boost Software License, `fpconv/LICENSE.txt`),
  which formats numbers passed to `redis.call` as Redis does.
- `lua/redis_random.c`: Redis' `src/rand.c` and its `math.random` for
  scripts (BSD-3-Clause, notice inside).
