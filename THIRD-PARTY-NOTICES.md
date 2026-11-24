# Third-party notices

Tantalum is MIT-licensed (`LICENSE`). It builds in C code from Redis 7.2.4 and the projects
Redis took it from, listed here with their licenses; `vendor/README.md` says what each part
does. Release archives also carry `THIRD-PARTY-LICENSES.html`, the licenses of the Rust
crates built in, made with [cargo-about](https://github.com/EmbarkStudios/cargo-about)
from `about.toml`.

| Code | Where | License |
|---|---|---|
| Redis 7.2.4: `math.random` for scripts, and what `COMMAND`, `COMMAND DOCS` and `CONFIG GET` say, taken from its replies by `tools/command-table.py` | `vendor/lua/redis_random.c`, `src/command_table.resp`, `src/config_defaults.txt`, `src/config_kinds.txt` | BSD-3-Clause |
| Lua 5.1.5, with Redis' patches | `vendor/lua/src` | MIT |
| lua-cjson, its `strbuf` and `fpconv` | `vendor/lua/src/lua_cjson.c`, `strbuf.*`, `fpconv.*` | MIT |
| lua-cmsgpack | `vendor/lua/src/lua_cmsgpack.c` | MIT |
| Lua `struct` library | `vendor/lua/src/lua_struct.c` | MIT |
| Lua BitOp | `vendor/lua/src/lua_bit.c` | MIT |
| fpconv (Grisu2) | `vendor/fpconv` | BSL-1.0 |
| LibLZF 3.6 | `vendor/lzf` | BSD-2-Clause |

The brand files in `assets/brand` have text set in [Geist](https://vercel.com/font), converted
to outlines; Geist is under the SIL Open Font License 1.1.

## Redis

Copyright (c) 2006-2020, Salvatore Sanfilippo
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are
permitted provided that the following conditions are met:

- Redistributions of source code must retain the above copyright notice, this list of
  conditions and the following disclaimer.
- Redistributions in binary form must reproduce the above copyright notice, this list of
  conditions and the following disclaimer in the documentation and/or other materials
  provided with the distribution.
- Neither the name of Redis nor the names of its contributors may be used to endorse or
  promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS
OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

`vendor/lua/redis_random.c` carries its own notice: Copyright (c) 2010-2012, Salvatore
Sanfilippo, under the same terms.

## MIT-licensed C code

Each of these is under the MIT license below, with its own copyright line:

- Lua 5.1.5: Copyright (C) 1994-2012 Lua.org, PUC-Rio.
- Lua `struct` library: Copyright (C) 2010-2018 Lua.org, PUC-Rio.
- lua-cjson, `strbuf`, and lua-cjson's `fpconv`: Copyright (c) 2010-2012 Mark Pulford.
- lua-cmsgpack: Copyright (C) 2012 Salvatore Sanfilippo.
- Lua BitOp: Copyright (C) 2008-2012 Mike Pall.

Permission is hereby granted, free of charge, to any person obtaining a copy of this
software and associated documentation files (the "Software"), to deal in the Software
without restriction, including without limitation the rights to use, copy, modify, merge,
publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons
to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or
substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
DEALINGS IN THE SOFTWARE.

## fpconv

Copyright (c) 2021, Redis Labs
Copyright (c) 2013-2019, night-shift
Copyright (c) 2009, Florian Loitsch

Boost Software License - Version 1.0 - August 17th, 2003

Permission is hereby granted, free of charge, to any person or organization obtaining a
copy of the software and accompanying documentation covered by this license (the
"Software") to use, reproduce, display, distribute, execute, and transmit the Software, and
to prepare derivative works of the Software, and to permit third-parties to whom the
Software is furnished to do so, all subject to the following:

The copyright notices in the Software and this entire statement, including the above
license grant, this restriction and the following disclaimer, must be included in all
copies of the Software, in whole or in part, and all derivative works of the Software,
unless such copies or derivative works are solely in the form of machine-executable object
code generated by a source language processor.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
PURPOSE, TITLE AND NON-INFRINGEMENT. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR ANYONE
DISTRIBUTING THE SOFTWARE BE LIABLE FOR ANY DAMAGES OR OTHER LIABILITY, WHETHER IN
CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
USE OR OTHER DEALINGS IN THE SOFTWARE.

## LibLZF

Copyright (c) 2000-2008 Marc Alexander Lehmann

Redistribution and use in source and binary forms, with or without modification, are
permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this list of
   conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice, this list of
   conditions and the following disclaimer in the documentation and/or other materials
   provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE AUTHOR "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

LibLZF may also be used under the GNU General Public License version 2 or later; Tantalum
uses it under the BSD terms above.
