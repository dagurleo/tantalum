//! Lua scripting for `EVAL`, `EVALSHA` and `SCRIPT` (`src/cmd/eval.rs`), on
//! Redis 7.2.4's Lua and libraries (built from `vendor/`), set up the way
//! Redis sets them up (`scriptingInit` in its eval.c, `luaRegisterRedisAPI`
//! in script_lua.c): scripts see the same libraries, protected globals and
//! `redis` API, and fail with the same errors.
//!
//! One interpreter serves every connection, made on the first script, so a
//! server that runs none doesn't pay for it. A script runs as a transaction,
//! as `EXEC` does, holding each shard it touches to the end, so it's as if
//! nothing else ran meanwhile, as in Redis, whatever keys it uses, declared or
//! not. Its `redis.call`s run through the command table in a session of
//! their own, and replies become Lua values (and the script's result a
//! reply) by Redis' rules.
//!
//! Lua raises errors with `longjmp`, so the functions it calls here keep no
//! values that need dropping in a frame an error can jump out of: they do
//! their work in helpers that return first, and raise last.

use std::any::Any;
use std::cell::Cell;
use std::collections::HashMap;
use std::ffi::{CStr, c_char, c_int, c_void};
use std::ops::Range;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use mlua_sys::{self as lua, lua_State};
use parking_lot::Mutex;

use crate::bullmq;
use crate::cmd::{self, Args};
use crate::context::{Context, Session};
use crate::resp;
use crate::store::Millis;

unsafe extern "C-unwind" {
    fn luaopen_cjson(l: *mut lua_State) -> c_int;
    fn luaopen_struct(l: *mut lua_State) -> c_int;
    fn luaopen_cmsgpack(l: *mut lua_State) -> c_int;
    fn luaopen_bit(l: *mut lua_State) -> c_int;
    /// Redis' `math.random` and `math.randomseed` (`vendor/lua/redis_random.c`).
    fn tantalum_math_random(l: *mut lua_State) -> c_int;
    fn tantalum_math_randomseed(l: *mut lua_State) -> c_int;
}

unsafe extern "C" {
    /// Redis' addition to Lua: makes a table read-only, or writable again.
    fn lua_enablereadonlytable(l: *mut lua_State, index: c_int, enabled: c_int);
    fn lua_isreadonlytable(l: *mut lua_State, index: c_int) -> c_int;
    /// Shortest round-trip digits of `fp`, as Redis formats numbers.
    fn fpconv_dtoa(fp: f64, dest: *mut c_char) -> c_int;
    /// C's `%.<precision>g` into `dest` (32 bytes), as cjson writes numbers.
    fn fpconv_g_fmt(dest: *mut c_char, num: f64, precision: c_int) -> c_int;
}

/// Reported by `redis.REDIS_VERSION` and `redis.REDIS_VERSION_NUM`.
const REDIS_VERSION_NUM: f64 = ((7 << 16) | (2 << 8) | 4) as f64;

/// Globals scripts may set while the interpreter is set up (Redis'
/// allow lists in script_lua.c); others are dropped, with a warning unless
/// on [`DENIED`].
const ALLOWED: &[&[u8]] = &[
    // Libraries.
    b"string",
    b"cjson",
    b"bit",
    b"cmsgpack",
    b"math",
    b"table",
    b"struct",
    // The `redis` API.
    b"redis",
    b"__redis__err__handler",
    // Lua's built-ins.
    b"xpcall",
    b"tostring",
    b"getfenv",
    b"setmetatable",
    b"next",
    b"assert",
    b"tonumber",
    b"rawequal",
    b"collectgarbage",
    b"getmetatable",
    b"rawset",
    b"pcall",
    b"coroutine",
    b"type",
    b"_G",
    b"select",
    b"unpack",
    b"gcinfo",
    b"pairs",
    b"rawget",
    b"loadstring",
    b"ipairs",
    b"_VERSION",
    b"setfenv",
    b"load",
    b"error",
    // Undocumented.
    b"newproxy",
    // Removed once the error handler has it.
    b"debug",
];

/// Built-ins dropped on purpose.
const DENIED: &[&[u8]] = &[b"dofile", b"loadfile", b"print"];

/// Defines the handler every script runs under, which adds where an error
/// happened (Redis' `scriptingInit`, verbatim).
const ERROR_HANDLER: &[u8] = b"local dbg = debug\n\
debug = nil\n\
function __redis__err__handler(err)\n\
\x20 local i = dbg.getinfo(2,'nSl')\n\
\x20 if i and i.what == 'C' then\n\
\x20   i = dbg.getinfo(3,'nSl')\n\
\x20 end\n\
\x20 if type(err) ~= 'table' then\n\
\x20   err = {err='ERR ' .. tostring(err)}\
\x20 end\
\x20 if i then\n\
\x20   err['source'] = i.source\n\
\x20   err['line'] = i.currentline\n\
\x20 end\
\x20 return err\n\
end\n";

/// Lua instructions between checks for `SCRIPT KILL`, as in Redis.
const HOOK_INSTRUCTIONS: c_int = 100_000;
/// When a script running this long is logged as slow (Redis'
/// `busy-reply-threshold`).
const SLOW_SCRIPT: Duration = Duration::from_secs(5);
/// Scripts between incremental garbage collection steps, as in Redis.
const GC_PERIOD: u32 = 50;

const IDLE: u8 = 0;
const RUNNING: u8 = 1;
/// Running, and a write command ran: `SCRIPT KILL` would leave it half done.
const WROTE: u8 = 2;

/// What a script's shebang line (`#!lua flags=...`) asks for.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Flags {
    /// No shebang: the script runs as before Redis 7 (`EVAL` compatibility).
    pub(crate) compat: bool,
    pub(crate) no_writes: bool,
}

/// The interpreter and the scripts loaded into it.
#[derive(Default)]
pub(crate) struct Scripting {
    engine: Mutex<Option<Engine>>,
    /// Set by `SCRIPT KILL` for the running script.
    kill: AtomicBool,
    /// [`IDLE`], [`RUNNING`] or [`WROTE`].
    state: AtomicU8,
    /// Scripts run natively, and calls a native one sent back to Lua.
    natives: AtomicU64,
    fallbacks: AtomicU64,
    /// Which native scripts are loaded (by `bullmq::find`'s number), so
    /// `EVALSHA` can run them without the interpreter's lock.
    natives_loaded: AtomicU8,
}

impl Scripting {
    /// Runs a script for `EVAL` (`sha` false: `args[1]` is its body) or
    /// `EVALSHA` (its SHA-1), with `numkeys` keys after `args[2]` and then
    /// its arguments, writing its reply in the caller's protocol. For
    /// `EVAL_RO` and `EVALSHA_RO`, `read_only`. Must run as a transaction
    /// (`Store::exclusive`).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn eval(
        &self,
        ctx: &Context,
        caller: &Session,
        try_native: bool,
        now: Millis,
        args: &Args<'_>,
        numkeys: usize,
        sha: bool,
        read_only: bool,
        out: &mut Vec<u8>,
    ) {
        let mut engine = self.engine.lock();
        let engine = engine.get_or_insert_with(Engine::new);
        let name = if sha {
            let mut name = [0; 40];
            name.copy_from_slice(args.get(1));
            name.make_ascii_lowercase();
            name
        } else {
            sha1_hex(args.get(1))
        };
        let flags = match engine.scripts.get(&name) {
            Some(&flags) => flags,
            None if sha => {
                resp::write_error(out, b"NOSCRIPT No matching script. Please use EVAL.");
                return;
            }
            None => match engine.create(args.get(1), &name) {
                Ok(flags) => {
                    self.loaded(&name);
                    flags
                }
                Err(e) => return resp::write_error(out, &e),
            },
        };
        // Redis' `scriptPrepareForRun`: a script with a shebang declares
        // whether it writes.
        if !flags.compat
            && !flags.no_writes
            && let Some(aof) = ctx.aof().filter(|aof| aof.failing())
        {
            resp::write_error(out, aof.failure().as_bytes());
            return;
        }
        if !flags.compat && !flags.no_writes && read_only {
            resp::write_error(
                out,
                b"ERR Can not execute a script with write flag using *_ro command.",
            );
            return;
        }
        let read_only = read_only || (!flags.compat && flags.no_writes);
        // BullMQ's hottest scripts natively, unless one sends this call
        // back to Lua (having written nothing). In a transaction here.
        if try_native
            && ctx.settings.native_scripts
            && !read_only
            && let Some((_, native)) = bullmq::find(&name)
        {
            let keys = (3..3 + numkeys).map(|i| args.get(i)).collect();
            let argv = (3 + numkeys..args.len()).map(|i| args.get(i)).collect();
            let start = out.len();
            let shards = ctx.store.script_shards(&[]);
            let mut call = bullmq::Call::new(ctx, caller.db, now, &name, keys, argv, out, shards);
            if let bullmq::Outcome::Done = native(&mut call) {
                ctx.publish(&mut call.session);
                self.natives.fetch_add(1, Relaxed);
                return;
            }
            // (Not `Retry`, in a transaction.)
            drop(call);
            self.fallbacks.fetch_add(1, Relaxed);
            out.truncate(start);
        }
        let keys = (3..3 + numkeys).map(|i| args.get(i));
        let argv = (3 + numkeys..args.len()).map(|i| args.get(i));
        let mut run = Run {
            ctx,
            session: Session {
                db: caller.db,
                special: caller.special,
                ..Session::for_script()
            },
            now,
            read_only,
            started: Instant::now(),
            warned: false,
            args: Vec::new(),
            ranges: Vec::new(),
            reply: Vec::new(),
            panic: None,
        };
        self.kill.store(false, Relaxed);
        self.state.store(RUNNING, Relaxed);
        RUN.set(&raw mut run as *mut c_void);
        // SAFETY: under the lock, with `RUN` set for the callbacks.
        unsafe { engine.run(&name, keys, argv, caller.resp3, out) };
        RUN.set(std::ptr::null_mut());
        self.state.store(IDLE, Relaxed);
        ctx.publish(&mut run.session);
        if let Some(panic) = run.panic {
            resume_unwind(panic);
        }
    }

    /// `EVALSHA` of a native script (see `bullmq`), holding only the
    /// shards it uses: those it asks for, taken in order (when one it asks
    /// for later can't be taken without waiting, it starts over holding
    /// them all from the start). False if it isn't one, isn't loaded, or
    /// went back to Lua (having written nothing): then `eval` runs it.
    pub(crate) fn run_native(
        &self,
        ctx: &Context,
        db: u32,
        now: Millis,
        args: &Args<'_>,
        numkeys: usize,
        out: &mut Vec<u8>,
    ) -> bool {
        let mut name = [0; 40];
        name.copy_from_slice(args.get(1));
        name.make_ascii_lowercase();
        let Some((index, native)) = bullmq::find(&name) else {
            return false;
        };
        if self.natives_loaded.load(Relaxed) & (1 << index) == 0 {
            return false;
        }
        let mut wanted: Vec<usize> = Vec::new();
        loop {
            let start = out.len();
            let keys = (3..3 + numkeys).map(|i| args.get(i)).collect();
            let argv = (3 + numkeys..args.len()).map(|i| args.get(i)).collect();
            let shards = ctx.store.script_shards(&wanted);
            let mut call = bullmq::Call::new(ctx, db, now, &name, keys, argv, out, shards);
            match native(&mut call) {
                bullmq::Outcome::Done => {
                    // The reads it made count; those of a try it gave up
                    // on, or of one Lua then makes again, don't.
                    ctx.publish(&mut call.session);
                    self.natives.fetch_add(1, Relaxed);
                    return true;
                }
                bullmq::Outcome::Lua => {
                    drop(call);
                    self.fallbacks.fetch_add(1, Relaxed);
                    out.truncate(start);
                    return false;
                }
                bullmq::Outcome::Retry => {
                    wanted = call.shards.wanted().to_vec();
                    drop(call); // lets go of what it held
                    out.truncate(start);
                }
            }
        }
    }

    /// Notes that the script `name` is loaded, if it's a native one.
    fn loaded(&self, name: &[u8; 40]) {
        if let Some((index, _)) = bullmq::find(name) {
            self.natives_loaded.fetch_or(1 << index, Relaxed);
        }
    }

    /// Scripts run natively, and calls a native one sent back to Lua.
    pub(crate) fn native_stats(&self) -> (u64, u64) {
        (self.natives.load(Relaxed), self.fallbacks.load(Relaxed))
    }

    /// `SCRIPT LOAD`: compiles and keeps a script, replying its SHA-1.
    pub(crate) fn load(&self, body: &[u8], out: &mut Vec<u8>) {
        let mut engine = self.engine.lock();
        let engine = engine.get_or_insert_with(Engine::new);
        let name = sha1_hex(body);
        if !engine.scripts.contains_key(&name)
            && let Err(e) = engine.create(body, &name)
        {
            return resp::write_error(out, &e);
        }
        self.loaded(&name);
        resp::write_bulk(out, &name);
    }

    /// `SCRIPT EXISTS`: whether a script with this SHA-1 is loaded.
    pub(crate) fn exists(&self, sha: &[u8]) -> bool {
        let Ok(name) = <[u8; 40]>::try_from(sha) else {
            return false;
        };
        let name = name.map(|b| b.to_ascii_lowercase());
        self.engine
            .lock()
            .as_ref()
            .is_some_and(|engine| engine.scripts.contains_key(&name))
    }

    /// `SCRIPT FLUSH`: forgets every script, with a fresh interpreter, as
    /// Redis does.
    pub(crate) fn flush(&self) {
        let mut engine = self.engine.lock();
        self.natives_loaded.store(0, Relaxed);
        let old = engine.take();
        drop(engine);
        drop(old);
    }

    /// `SCRIPT KILL`: stops the running script, unless it wrote already.
    pub(crate) fn kill(&self, out: &mut Vec<u8>) {
        match self.state.load(Relaxed) {
            IDLE => resp::write_error(out, b"NOTBUSY No scripts in execution right now."),
            WROTE => resp::write_error(
                out,
                b"UNKILLABLE Sorry the script already executed write commands against the \
                  dataset. You can either wait the script termination or kill the server in a \
                  hard way using the SHUTDOWN NOSAVE command.",
            ),
            _ => {
                self.kill.store(true, Relaxed);
                out.extend_from_slice(resp::OK);
            }
        }
    }
}

/// The running script's state, for the functions Lua calls.
struct Run<'a> {
    ctx: &'a Context,
    /// The session its commands run in.
    session: Session,
    /// The time when it started: it stands still while a script runs.
    now: Millis,
    read_only: bool,
    started: Instant,
    /// Whether it was logged as slow.
    warned: bool,
    /// The current `redis.call`'s arguments, back to back, and their ranges.
    args: Vec<u8>,
    ranges: Vec<Range<usize>>,
    /// The current command's reply.
    reply: Vec<u8>,
    /// A command panicked: raised again once the script is unwound.
    panic: Option<Box<dyn Any + Send>>,
}

thread_local! {
    /// The [`Run`] of the script running on this thread.
    static RUN: Cell<*mut c_void> = const { Cell::new(std::ptr::null_mut()) };
}

/// The running script.
///
/// # Safety
///
/// Only from functions Lua calls while [`Scripting::eval`] runs a script.
unsafe fn current<'a>() -> &'a mut Run<'a> {
    let run = RUN.get();
    assert!(!run.is_null(), "no script running");
    // SAFETY: set by `eval` for as long as the script runs, and only one
    // of these is used at a time.
    unsafe { &mut *(run as *mut Run<'a>) }
}

/// The interpreter: Lua's state and the scripts compiled into it.
struct Engine {
    l: *mut lua_State,
    /// By SHA-1 (lowercase hex). Each is a function in Lua's registry,
    /// `f_<sha1>`.
    scripts: HashMap<[u8; 40], Flags>,
    /// Scripts run since the last garbage collection step.
    since_gc: u32,
}

// SAFETY: Lua's state is used only by whoever holds `Scripting::engine`.
unsafe impl Send for Engine {}

impl Drop for Engine {
    fn drop(&mut self) {
        // SAFETY: nothing uses the state any more.
        unsafe { lua::lua_close(self.l) }
    }
}

impl Engine {
    /// Sets up an interpreter as Redis does.
    fn new() -> Engine {
        // SAFETY: a fresh state, set up by Lua's API.
        unsafe {
            // As `luaL_newstate`, but allocating as the server does.
            let l = lua::lua_newstate(allocate, std::ptr::null_mut());
            assert!(!l.is_null(), "out of memory for Lua");
            lua::lua_atpanic(l, unprotected_error);
            // luaRegisterRedisAPI: while libraries load, the globals take
            // only what's on the allow lists.
            lua::lua_pushvalue(l, lua::LUA_GLOBALSINDEX);
            set_metatable_function(l, c"__newindex", new_global);
            lua::lua_pop(l, 1);
            load_lib(l, c"", lua::luaopen_base);
            load_lib(l, c"table", lua::luaopen_table);
            load_lib(l, c"string", lua::luaopen_string);
            load_lib(l, c"math", lua::luaopen_math);
            load_lib(l, c"debug", lua::luaopen_debug);
            load_lib(l, c"cjson", luaopen_cjson);
            load_lib(l, c"struct", luaopen_struct);
            load_lib(l, c"cmsgpack", luaopen_cmsgpack);
            load_lib(l, c"bit", luaopen_bit);
            lua::lua_pushcfunction(l, protected_pcall);
            lua::lua_setglobal(l, c"pcall".as_ptr());

            lua::lua_newtable(l);
            set_function(l, c"call", redis_call);
            set_function(l, c"pcall", redis_pcall);
            set_function(l, c"log", redis_log);
            set_number(l, c"LOG_DEBUG", 0.0);
            set_number(l, c"LOG_VERBOSE", 1.0);
            set_number(l, c"LOG_NOTICE", 2.0);
            set_number(l, c"LOG_WARNING", 3.0);
            set_number(l, c"REDIS_VERSION_NUM", REDIS_VERSION_NUM);
            lua::lua_pushstring(l, c"REDIS_VERSION".as_ptr());
            lua::lua_pushstring(l, c"7.2.4".as_ptr());
            lua::lua_settable(l, -3);
            set_function(l, c"setresp", redis_setresp);
            set_function(l, c"sha1hex", redis_sha1hex);
            set_function(l, c"error_reply", redis_error_reply);
            set_function(l, c"status_reply", redis_status_reply);
            set_function(l, c"set_repl", redis_set_repl);
            set_number(l, c"REPL_NONE", 0.0);
            set_number(l, c"REPL_AOF", 1.0);
            set_number(l, c"REPL_SLAVE", 2.0);
            set_number(l, c"REPL_REPLICA", 2.0);
            set_number(l, c"REPL_ALL", 3.0);
            set_function(l, c"acl_check_cmd", redis_acl_check_cmd);
            lua::lua_setglobal(l, c"redis".as_ptr());

            lua::lua_getglobal(l, c"math".as_ptr());
            set_function(l, c"random", tantalum_math_random);
            set_function(l, c"randomseed", tantalum_math_randomseed);
            lua::lua_setglobal(l, c"math".as_ptr());

            // scriptingInit: the debugger's functions (no debugger here).
            lua::lua_getglobal(l, c"redis".as_ptr());
            set_function(l, c"breakpoint", redis_breakpoint);
            set_function(l, c"debug", redis_debug);
            set_function(l, c"replicate_commands", redis_replicate_commands);
            lua::lua_setglobal(l, c"redis".as_ptr());

            let loaded = lua::luaL_loadbuffer(
                l,
                ERROR_HANDLER.as_ptr().cast(),
                ERROR_HANDLER.len(),
                c"@err_handler_def".as_ptr(),
            );
            assert_eq!(loaded, 0, "the error handler compiles");
            assert_eq!(lua::lua_pcall(l, 0, 0, 0), 0, "the error handler runs");

            // From now on reading a missing global is an error, and the
            // globals and every table they reach are read-only.
            lua::lua_pushvalue(l, lua::LUA_GLOBALSINDEX);
            set_metatable_function(l, c"__index", missing_global);
            protect(l);
            lua::lua_pop(l, 1);
            Engine {
                l,
                scripts: HashMap::new(),
                since_gc: 0,
            }
        }
    }

    /// Compiles a script and keeps it (Redis' `luaCreateFunction`), or
    /// returns the error to reply.
    fn create(&mut self, body: &[u8], name: &[u8; 40]) -> Result<Flags, Vec<u8>> {
        let (flags, shebang_len) = shebang(body)?;
        let l = self.l;
        let code = &body[shebang_len..];
        // SAFETY: the state is ours; this leaves the stack as it was.
        unsafe {
            if lua::luaL_loadbuffer(
                l,
                code.as_ptr().cast(),
                code.len(),
                c"@user_script".as_ptr(),
            ) != 0
            {
                let mut e = b"ERR Error compiling script (new function): ".to_vec();
                e.extend_from_slice(to_bytes(l, -1));
                lua::lua_pop(l, 1);
                return Err(e);
            }
            let key = function_name(name);
            lua::lua_setfield(l, lua::LUA_REGISTRYINDEX, key.as_ptr().cast());
        }
        self.scripts.insert(*name, flags);
        Ok(flags)
    }

    /// Runs a compiled script (Redis' `luaCallFunction`) and writes its
    /// reply.
    ///
    /// # Safety
    ///
    /// With `RUN` set.
    unsafe fn run<'k>(
        &mut self,
        name: &[u8; 40],
        keys: impl ExactSizeIterator<Item = &'k [u8]>,
        argv: impl ExactSizeIterator<Item = &'k [u8]>,
        resp3: bool,
        out: &mut Vec<u8>,
    ) {
        let l = self.l;
        // SAFETY: the state is ours, and `RUN` is set for the callbacks.
        unsafe {
            lua::lua_getglobal(l, c"__redis__err__handler".as_ptr());
            let key = function_name(name);
            lua::lua_getfield(l, lua::LUA_REGISTRYINDEX, key.as_ptr().cast());
            lua::lua_sethook(l, Some(count_hook), lua::LUA_MASKCOUNT, HOOK_INSTRUCTIONS);
            push_array(l, keys);
            lua_enablereadonlytable(l, lua::LUA_GLOBALSINDEX, 0);
            lua::lua_setglobal(l, c"KEYS".as_ptr());
            lua_enablereadonlytable(l, lua::LUA_GLOBALSINDEX, 1);
            push_array(l, argv);
            lua_enablereadonlytable(l, lua::LUA_GLOBALSINDEX, 0);
            lua::lua_setglobal(l, c"ARGV".as_ptr());
            lua_enablereadonlytable(l, lua::LUA_GLOBALSINDEX, 1);
            let failed = lua::lua_pcall(l, 0, 1, -2) != 0;
            self.since_gc += 1;
            if self.since_gc == GC_PERIOD {
                lua::lua_gc(l, lua::LUA_GCSTEP, GC_PERIOD as c_int);
                self.since_gc = 0;
            }
            if failed {
                write_failure(l, name, out);
                lua::lua_pop(l, 1);
            } else {
                let script_resp3 = current().session.resp3;
                write_value(l, out, resp3, script_resp3);
            }
            lua::lua_sethook(l, None, 0, 0);
            // The error handler.
            lua::lua_pop(l, 1);
        }
    }
}

/// Lua's allocator: the server's (mimalloc), as Redis gives its Lua
/// jemalloc, rather than the C library's `malloc`, which took ~17% of a
/// busy BullMQ server's time. Lua passes each block's size back when it
/// frees or resizes it.
unsafe extern "C" fn allocate(
    _: *mut c_void,
    ptr: *mut c_void,
    old: usize,
    new: usize,
) -> *mut c_void {
    use std::alloc::{Layout, alloc, dealloc, realloc};
    // What `malloc` guarantees, which Lua's doubles and pointers need.
    const ALIGN: usize = 16;
    // SAFETY: Lua passes `ptr` with the size it was allocated or resized
    // to, and sizes are far below `isize::MAX`.
    unsafe {
        if new == 0 {
            if !ptr.is_null() {
                dealloc(ptr.cast(), Layout::from_size_align_unchecked(old, ALIGN));
            }
            std::ptr::null_mut()
        } else if ptr.is_null() {
            alloc(Layout::from_size_align_unchecked(new, ALIGN)).cast()
        } else {
            realloc(
                ptr.cast(),
                Layout::from_size_align_unchecked(old, ALIGN),
                new,
            )
            .cast()
        }
    }
}

/// An error outside any protected call, which can't happen here: as Lua's
/// own handler, says so (Lua then exits).
unsafe extern "C-unwind" fn unprotected_error(l: *mut lua_State) -> c_int {
    // SAFETY: the error is on the stack.
    let message = unsafe { to_c_bytes(l, -1) };
    eprintln!(
        "PANIC: unprotected error in call to Lua API ({})",
        String::from_utf8_lossy(message)
    );
    0
}

/// `f_<sha1>`, NUL-terminated: where a script's function is kept.
fn function_name(name: &[u8; 40]) -> [u8; 43] {
    let mut key = [0; 43];
    key[..2].copy_from_slice(b"f_");
    key[2..42].copy_from_slice(name);
    key
}

/// The lowercase hex SHA-1 of `data`, which names scripts.
pub(crate) fn sha1_hex(data: &[u8]) -> [u8; 40] {
    let digest = sha1_smol::Sha1::from(data).digest().bytes();
    let mut hex = [0; 40];
    for (i, byte) in digest.iter().enumerate() {
        hex[2 * i] = b"0123456789abcdef"[usize::from(byte >> 4)];
        hex[2 * i + 1] = b"0123456789abcdef"[usize::from(byte & 15)];
    }
    hex
}

/// A script's shebang flags and the shebang's length (Redis'
/// `evalExtractShebangFlags`), or the error to reply.
fn shebang(body: &[u8]) -> Result<(Flags, usize), Vec<u8>> {
    let mut flags = Flags {
        compat: true,
        no_writes: false,
    };
    if !body.starts_with(b"#!") {
        return Ok((flags, 0));
    }
    let Some(len) = memchr::memchr(b'\n', body) else {
        return Err(b"ERR Invalid script shebang".to_vec());
    };
    let mut parts = body[..len]
        .split(|b| b.is_ascii_whitespace())
        .filter(|part| !part.is_empty());
    let Some(engine) = parts.next() else {
        return Err(b"ERR Invalid engine in script shebang".to_vec());
    };
    if engine != b"#!lua" {
        let mut e = b"ERR Unexpected engine in script shebang: ".to_vec();
        e.extend_from_slice(engine);
        return Err(e);
    }
    flags.compat = false;
    for part in parts {
        let Some(list) = part.strip_prefix(b"flags=") else {
            let mut e = b"ERR Unknown lua shebang option: ".to_vec();
            e.extend_from_slice(part);
            return Err(e);
        };
        for flag in list.split(|&b| b == b',').filter(|f| !f.is_empty()) {
            match flag {
                b"no-writes" => flags.no_writes = true,
                // No OOM checks, replicas or cluster here to relax.
                b"allow-oom" | b"allow-stale" | b"no-cluster" | b"allow-cross-slot-keys" => {}
                _ => {
                    let mut e = b"ERR Unexpected flag in script shebang: ".to_vec();
                    e.extend_from_slice(flag);
                    return Err(e);
                }
            }
        }
    }
    Ok((flags, len))
}

/// The string at `index` (converted in place if a number), or empty.
///
/// # Safety
///
/// The slice lives as long as the value stays on the stack.
unsafe fn to_bytes<'a>(l: *mut lua_State, index: c_int) -> &'a [u8] {
    let mut len = 0;
    // SAFETY: Lua keeps the string while it's on the stack.
    unsafe {
        let s = lua::lua_tolstring(l, index, &mut len);
        if s.is_null() {
            &[]
        } else {
            std::slice::from_raw_parts(s.cast(), len)
        }
    }
}

/// The C string at `index`, as Redis reads strings it copies with `sdsnew`:
/// up to the first NUL.
unsafe fn to_c_bytes<'a>(l: *mut lua_State, index: c_int) -> &'a [u8] {
    // SAFETY: as `to_bytes`.
    let bytes = unsafe { to_bytes(l, index) };
    &bytes[..memchr::memchr(0, bytes).unwrap_or(bytes.len())]
}

unsafe fn load_lib(l: *mut lua_State, name: &CStr, open: lua::lua_CFunction) {
    // SAFETY: Redis' `luaLoadLib`.
    unsafe {
        lua::lua_pushcfunction(l, open);
        lua::lua_pushstring(l, name.as_ptr());
        lua::lua_call(l, 1, 0);
    }
}

unsafe fn set_function(l: *mut lua_State, name: &CStr, f: lua::lua_CFunction) {
    // SAFETY: a table is on top.
    unsafe {
        lua::lua_pushstring(l, name.as_ptr());
        lua::lua_pushcfunction(l, f);
        lua::lua_settable(l, -3);
    }
}

unsafe fn set_number(l: *mut lua_State, name: &CStr, n: f64) {
    // SAFETY: a table is on top.
    unsafe {
        lua::lua_pushstring(l, name.as_ptr());
        lua::lua_pushnumber(l, n);
        lua::lua_settable(l, -3);
    }
}

/// Gives the table on top a new metatable with `field` set to `f`.
unsafe fn set_metatable_function(l: *mut lua_State, field: &CStr, f: lua::lua_CFunction) {
    // SAFETY: a table is on top.
    unsafe {
        lua::lua_newtable(l);
        lua::lua_pushcfunction(l, f);
        lua::lua_setfield(l, -2, field.as_ptr());
        lua::lua_setmetatable(l, -2);
    }
}

/// Makes the table on top, and every table it reaches, read-only (Redis'
/// `luaSetTableProtectionRecursively`).
unsafe fn protect(l: *mut lua_State) {
    // SAFETY: a table is on top; this leaves the stack as it was.
    unsafe {
        if lua_isreadonlytable(l, -1) != 0 {
            return;
        }
        lua_enablereadonlytable(l, -1, 1);
        lua::lua_checkstack(l, 2);
        lua::lua_pushnil(l);
        while lua::lua_next(l, -2) != 0 {
            if lua::lua_istable(l, -1) != 0 {
                protect(l);
            }
            lua::lua_pop(l, 1);
        }
        if lua::lua_getmetatable(l, -1) != 0 {
            protect(l);
            lua::lua_pop(l, 1);
        }
    }
}

/// An array of `items`, on top of the stack.
unsafe fn push_array<'k>(l: *mut lua_State, items: impl ExactSizeIterator<Item = &'k [u8]>) {
    // SAFETY: Lua's API.
    unsafe {
        lua::lua_newtable(l);
        for (i, item) in items.enumerate() {
            lua::lua_pushlstring(l, item.as_ptr().cast(), item.len());
            lua::lua_rawseti(l, -2, i as lua::lua_Integer + 1);
        }
    }
}

/// Pushes an error table, `{err = "<code> <message>"}` (Redis'
/// `luaPushErrorBuff`): from an error reply's text (`-CODE message`), or a
/// bare message (code `ERR`).
unsafe fn push_error(l: *mut lua_State, error: &[u8]) {
    let mut text = Vec::with_capacity(error.len() + 4);
    match error.strip_prefix(b"-") {
        Some(rest) => match memchr::memchr(b' ', error) {
            Some(space) => {
                text.extend_from_slice(&error[1..space]);
                text.push(b' ');
                text.extend_from_slice(trim_newlines(&error[space + 1..]));
            }
            None => {
                text.extend_from_slice(b"ERR ");
                text.extend_from_slice(trim_newlines(rest));
            }
        },
        None => {
            text.extend_from_slice(b"ERR ");
            text.extend_from_slice(trim_newlines(error));
        }
    }
    // Lua copies it as a C string.
    text.truncate(memchr::memchr(0, &text).unwrap_or(text.len()));
    // SAFETY: Lua's API.
    unsafe {
        lua::lua_newtable(l);
        lua::lua_pushstring(l, c"err".as_ptr());
        lua::lua_pushlstring(l, text.as_ptr().cast(), text.len());
        lua::lua_settable(l, -3);
    }
}

/// Marks the error table on top as counted already (as Redis does for a
/// command's errors).
unsafe fn mark_counted(l: *mut lua_State) {
    // SAFETY: an error table is on top.
    unsafe {
        lua::lua_pushstring(l, c"ignore_error_stats_update".as_ptr());
        lua::lua_pushboolean(l, 1);
        lua::lua_settable(l, -3);
    }
}

/// `s` without CR and LF at either end (`sdstrim(s, "\r\n")`).
fn trim_newlines(s: &[u8]) -> &[u8] {
    let newline = |b: &u8| *b == b'\r' || *b == b'\n';
    let start = s.iter().position(|b| !newline(b)).unwrap_or(s.len());
    let end = s.iter().rposition(|b| !newline(b)).map_or(start, |i| i + 1);
    &s[start..end]
}

/// `__newindex` of the globals while the interpreter is set up: keeps only
/// what's allowed (Redis' `luaNewIndexAllowList`).
unsafe extern "C-unwind" fn new_global(l: *mut lua_State) -> c_int {
    // SAFETY: called by Lua with (table, key, value).
    unsafe {
        if lua::lua_gettop(l) != 3 {
            lua::luaL_error(
                l,
                c"Wrong number of arguments to luaNewIndexAllowList".as_ptr(),
            );
        }
        if lua::lua_istable(l, -3) == 0 {
            lua::luaL_error(
                l,
                c"first argument to luaNewIndexAllowList must be a table".as_ptr(),
            );
        }
        if lua::lua_isstring(l, -2) == 0 && lua::lua_isnumber(l, -2) == 0 {
            lua::luaL_error(
                l,
                c"Second argument to luaNewIndexAllowList must be a string or number".as_ptr(),
            );
        }
        let name = to_c_bytes(l, -2);
        if ALLOWED.contains(&name) {
            lua::lua_rawset(l, -3);
        } else if !DENIED.contains(&name) {
            warn_unknown_global(name);
        }
        0
    }
}

fn warn_unknown_global(name: &[u8]) {
    eprintln!(
        "A key '{}' was added to Lua globals which is not on the globals allow list nor \
         listed on the deny list.",
        String::from_utf8_lossy(name)
    );
}

/// `__index` of the globals: reading one that doesn't exist is an error
/// (Redis' `luaProtectedTableError`).
unsafe extern "C-unwind" fn missing_global(l: *mut lua_State) -> c_int {
    // SAFETY: called by Lua with (table, key).
    unsafe {
        if lua::lua_gettop(l) != 2 {
            lua::luaL_error(
                l,
                c"Wrong number of arguments to luaProtectedTableError".as_ptr(),
            );
        }
        if lua::lua_isstring(l, -1) == 0 && lua::lua_isnumber(l, -1) == 0 {
            lua::luaL_error(
                l,
                c"Second argument to luaProtectedTableError must be a string or number".as_ptr(),
            );
        }
        lua::luaL_error(
            l,
            c"Script attempted to access nonexistent global variable '%s'".as_ptr(),
            lua::lua_tostring(l, -1),
        );
        0
    }
}

/// The global `pcall`: Lua's, but an error table becomes its message
/// (Redis' `luaRedisPcall`).
unsafe extern "C-unwind" fn protected_pcall(l: *mut lua_State) -> c_int {
    // SAFETY: Redis' code, step by step.
    unsafe {
        let argc = lua::lua_gettop(l);
        lua::lua_pushboolean(l, 1);
        lua::lua_insert(l, 1);
        if lua::lua_pcall(l, argc - 1, lua::LUA_MULTRET, 0) != 0 {
            lua::lua_remove(l, 1);
            if lua::lua_istable(l, -1) != 0 {
                lua::lua_getfield(l, -1, c"err".as_ptr());
                if lua::lua_isstring(l, -1) != 0 {
                    lua::lua_replace(l, -2);
                }
            }
            lua::lua_pushboolean(l, 0);
            lua::lua_insert(l, 1);
        }
        lua::lua_gettop(l)
    }
}

/// Checks for `SCRIPT KILL` every [`HOOK_INSTRUCTIONS`] (Redis'
/// `luaMaskCountHook`).
unsafe extern "C-unwind" fn count_hook(l: *mut lua_State, _: *mut lua::lua_Debug) {
    // SAFETY: called by Lua while a script runs.
    let killed = unsafe { check_running() };
    if killed {
        // SAFETY: nothing to drop here.
        unsafe {
            lua::lua_sethook(l, Some(count_hook), lua::LUA_MASKLINE, 0);
            push_error(l, b"Script killed by user with SCRIPT KILL...");
            lua::lua_error(l);
        }
    }
}

/// Whether the running script was killed; logs it once as slow.
unsafe fn check_running() -> bool {
    // SAFETY: a script is running.
    let run = unsafe { current() };
    if !run.warned && run.started.elapsed() >= SLOW_SCRIPT {
        run.warned = true;
        eprintln!(
            "Slow script detected: still in execution after {} milliseconds. You can try \
             killing the script using the SCRIPT KILL command.",
            run.started.elapsed().as_millis()
        );
    }
    let killed = run.ctx.scripting.kill.load(Relaxed);
    if killed {
        eprintln!("Lua script killed by user with SCRIPT KILL.");
    }
    killed
}

unsafe extern "C-unwind" fn redis_call(l: *mut lua_State) -> c_int {
    // SAFETY: called by Lua while a script runs.
    if unsafe { command(l) } {
        // SAFETY: the error table is on top; nothing to drop.
        unsafe { lua::lua_error(l) }
    }
    1
}

unsafe extern "C-unwind" fn redis_pcall(l: *mut lua_State) -> c_int {
    // SAFETY: called by Lua while a script runs.
    unsafe { command(l) };
    1
}

/// Takes the arguments of `redis.call` or `redis.acl_check_cmd` into the
/// running script's buffer (Redis' `luaArgsToRedisArgv`); on a bad one,
/// pushes the error and returns false.
unsafe fn take_args(l: *mut lua_State, run: &mut Run<'_>) -> bool {
    // SAFETY: Lua's API.
    unsafe {
        let argc = lua::lua_gettop(l);
        if argc == 0 {
            push_error(
                l,
                b"Please specify at least one argument for this redis lib call",
            );
            return false;
        }
        run.args.clear();
        run.ranges.clear();
        for i in 1..=argc {
            let start = run.args.len();
            if lua::lua_type(l, i) == lua::LUA_TNUMBER {
                let mut digits = [0 as c_char; 24];
                let len = fpconv_dtoa(lua::lua_tonumber(l, i), digits.as_mut_ptr());
                run.args
                    .extend(digits[..len as usize].iter().map(|&c| c as u8));
            } else {
                let mut len = 0;
                let s = lua::lua_tolstring(l, i, &mut len);
                if s.is_null() {
                    break;
                }
                run.args
                    .extend_from_slice(std::slice::from_raw_parts(s.cast(), len));
            }
            run.ranges.push(start..run.args.len());
        }
        lua::lua_pop(l, argc);
        if run.ranges.len() != argc as usize {
            push_error(
                l,
                b"Lua redis lib command arguments must be strings or integers",
            );
            return false;
        }
        true
    }
}

/// Runs the command `redis.call` or `redis.pcall` was given and pushes its
/// reply as a Lua value (Redis' `luaRedisGenericCommand`). Returns whether
/// that's an error, which `redis.call` raises.
unsafe fn command(l: *mut lua_State) -> bool {
    // SAFETY: called by Lua while a script runs.
    let run = unsafe { current() };
    // SAFETY: Lua's API.
    if !unsafe { take_args(l, run) } {
        return true;
    }
    match catch_unwind(AssertUnwindSafe(|| run.call())) {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            // SAFETY: Lua's API.
            unsafe {
                push_error(l, e.as_bytes());
                mark_counted(l);
            }
            return true;
        }
        Err(panic) => {
            run.panic = Some(panic);
            // SAFETY: Lua's API.
            unsafe { push_error(l, b"command panicked") };
            return true;
        }
    }
    let error = run.reply.first() == Some(&b'-');
    let mut reply = &run.reply[..];
    // SAFETY: Lua's API.
    unsafe { push_reply(l, &mut reply) };
    error
}

impl Run<'_> {
    /// Runs the command in `args`, its reply in `reply`, or the error Lua
    /// raises instead.
    fn call(&mut self) -> Result<(), &'static str> {
        let args = Args::new(&self.args, &self.ranges);
        self.reply.clear();
        let wrote = cmd::call_from_script(
            self.ctx,
            &mut self.session,
            self.now,
            &args,
            self.read_only,
            &mut self.reply,
        )?;
        if wrote {
            self.ctx.scripting.state.store(WROTE, Relaxed);
        }
        Ok(())
    }
}

/// Reads a line of a reply, without its CRLF.
fn line<'a>(input: &mut &'a [u8]) -> &'a [u8] {
    let end = memchr::memchr(b'\n', input).unwrap_or(input.len());
    let line = &input[..end];
    *input = &input[(end + 1).min(input.len())..];
    line.strip_suffix(b"\r").unwrap_or(line)
}

fn number(line: &[u8]) -> i64 {
    resp::parse_int(line).unwrap_or(0)
}

/// `n` bytes of a bulk reply, and its CRLF.
fn bulk<'a>(input: &mut &'a [u8], n: usize) -> &'a [u8] {
    let n = n.min(input.len());
    let data = &input[..n];
    *input = &input[(n + 2).min(input.len())..];
    data
}

/// Pushes one reply from `input` as a Lua value, as Redis converts replies
/// for scripts (`redisProtocolToLuaType`), and consumes it.
unsafe fn push_reply(l: *mut lua_State, input: &mut &[u8]) {
    let Some(&kind) = input.first() else {
        // SAFETY: Lua's API.
        unsafe { lua::lua_pushnil(l) };
        return;
    };
    *input = &input[1..];
    let head = line(input);
    // SAFETY: Lua's API.
    unsafe {
        lua::lua_checkstack(l, 4);
        match kind {
            b':' => lua::lua_pushnumber(l, number(head) as f64),
            b'$' => match usize::try_from(number(head)) {
                Ok(n) => {
                    let data = bulk(input, n);
                    lua::lua_pushlstring(l, data.as_ptr().cast(), data.len());
                }
                Err(_) => lua::lua_pushboolean(l, 0),
            },
            b'+' => {
                lua::lua_newtable(l);
                lua::lua_pushstring(l, c"ok".as_ptr());
                lua::lua_pushlstring(l, head.as_ptr().cast(), head.len());
                lua::lua_settable(l, -3);
            }
            b'-' | b'!' => {
                let text = if kind == b'!' {
                    bulk(input, number(head).max(0) as usize)
                } else {
                    head
                };
                let mut error = Vec::with_capacity(text.len() + 1);
                error.push(b'-');
                error.extend_from_slice(text);
                push_error(l, &error);
                drop(error);
                mark_counted(l);
            }
            b'*' | b'>' => match usize::try_from(number(head)) {
                Ok(n) => {
                    lua::lua_newtable(l);
                    for i in 0..n {
                        lua::lua_pushnumber(l, (i + 1) as f64);
                        push_reply(l, input);
                        lua::lua_settable(l, -3);
                    }
                }
                Err(_) => lua::lua_pushboolean(l, 0),
            },
            b'%' => {
                lua::lua_newtable(l);
                lua::lua_pushstring(l, c"map".as_ptr());
                lua::lua_newtable(l);
                for _ in 0..number(head).max(0) {
                    push_reply(l, input);
                    push_reply(l, input);
                    lua::lua_settable(l, -3);
                }
                lua::lua_settable(l, -3);
            }
            b'~' => {
                lua::lua_newtable(l);
                lua::lua_pushstring(l, c"set".as_ptr());
                lua::lua_newtable(l);
                for _ in 0..number(head).max(0) {
                    push_reply(l, input);
                    lua::lua_pushboolean(l, 1);
                    lua::lua_settable(l, -3);
                }
                lua::lua_settable(l, -3);
            }
            b'|' => {
                // Attributes are skipped; the reply follows them.
                for _ in 0..2 * number(head).max(0) {
                    push_reply(l, input);
                    lua::lua_pop(l, 1);
                }
                push_reply(l, input);
            }
            b'=' => {
                let text = bulk(input, number(head).max(0) as usize);
                let (format, string) = text.split_at(text.len().min(4));
                lua::lua_newtable(l);
                lua::lua_pushstring(l, c"verbatim_string".as_ptr());
                lua::lua_newtable(l);
                lua::lua_pushstring(l, c"string".as_ptr());
                lua::lua_pushlstring(l, string.as_ptr().cast(), string.len());
                lua::lua_settable(l, -3);
                lua::lua_pushstring(l, c"format".as_ptr());
                lua::lua_pushlstring(l, format.as_ptr().cast(), format.len().min(3));
                lua::lua_settable(l, -3);
                lua::lua_settable(l, -3);
            }
            b'(' => {
                lua::lua_newtable(l);
                lua::lua_pushstring(l, c"big_number".as_ptr());
                lua::lua_pushlstring(l, head.as_ptr().cast(), head.len());
                lua::lua_settable(l, -3);
            }
            b'_' => lua::lua_pushnil(l),
            b'#' => lua::lua_pushboolean(l, c_int::from(head == b"t")),
            b',' => {
                let d = match head {
                    b"inf" => f64::INFINITY,
                    b"-inf" => f64::NEG_INFINITY,
                    b"nan" | b"-nan" => f64::NAN,
                    _ => std::str::from_utf8(head)
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0.0),
                };
                lua::lua_newtable(l);
                lua::lua_pushstring(l, c"double".as_ptr());
                lua::lua_pushnumber(l, d);
                lua::lua_settable(l, -3);
            }
            _ => lua::lua_pushnil(l),
        }
    }
}

/// A Lua number as a `redis.call` argument: Redis' `fpconv_dtoa`.
pub(crate) fn format_number(n: f64) -> Vec<u8> {
    let mut digits = [0 as c_char; 24];
    // SAFETY: writes at most 24 bytes.
    let len = unsafe { fpconv_dtoa(n, digits.as_mut_ptr()) };
    digits[..len as usize].iter().map(|&c| c as u8).collect()
}

/// A Lua number as Lua itself writes it (`tostring(n)`, `n .. ""`) and as
/// `cjson.encode` does: C's `%.14g`.
pub(crate) fn lua_tostring(n: f64) -> Vec<u8> {
    let mut digits = [0 as c_char; 32];
    // SAFETY: writes at most 32 bytes, its buffer's size.
    let len = unsafe { fpconv_g_fmt(digits.as_mut_ptr(), n, 14) };
    digits[..len as usize].iter().map(|&c| c as u8).collect()
}

/// A Lua number a script returns, as its reply's integer.
pub(crate) fn lua_integer(n: f64) -> i64 {
    to_integer(n)
}

/// A Lua number as a reply's integer: C's `(long long)` cast on x86-64,
/// where out-of-range values (and NaN) become the smallest integer.
fn to_integer(n: f64) -> i64 {
    // 2^63, exactly.
    const LIMIT: f64 = 9_223_372_036_854_775_808.0;
    if (-LIMIT..LIMIT).contains(&n) {
        n as i64
    } else {
        i64::MIN
    }
}

/// `d` as Redis' `d2string` writes doubles in replies.
fn write_double_text(out: &mut Vec<u8>, d: f64) {
    if d.is_nan() {
        out.extend_from_slice(b"nan");
    } else if d.is_infinite() {
        out.extend_from_slice(if d < 0.0 { b"-inf" } else { b"inf" });
    } else if d == 0.0 {
        out.extend_from_slice(if d.is_sign_negative() { b"-0" } else { b"0" });
    } else if d.abs() <= (i64::MAX / 2) as f64 && d.fract() == 0.0 {
        out.extend_from_slice(itoa::Buffer::new().format(d as i64).as_bytes());
    } else {
        let mut digits = [0 as c_char; 24];
        // SAFETY: writes at most 24 bytes.
        let len = unsafe { fpconv_dtoa(d, digits.as_mut_ptr()) };
        out.extend(digits[..len as usize].iter().map(|&c| c as u8));
    }
}

/// `s` with CR and LF as spaces.
fn without_newlines(s: &[u8]) -> impl Iterator<Item = u8> + '_ {
    s.iter()
        .map(|&b| if b == b'\r' || b == b'\n' { b' ' } else { b })
}

/// An error reply, `-` and `s` (Redis' `addReplyErrorFormat` with a `-%s`
/// format): CR and LF trimmed at the ends and spaces between.
fn write_error_text(out: &mut Vec<u8>, s: &[u8]) {
    out.push(b'-');
    out.extend(without_newlines(trim_newlines(s)));
    out.extend_from_slice(b"\r\n");
}

/// Writes an aggregate's header in front of its `count` elements, written
/// from `start` on.
fn insert_header(out: &mut Vec<u8>, start: usize, prefix: u8, count: usize) {
    let mut header = vec![prefix];
    header.extend_from_slice(itoa::Buffer::new().format(count).as_bytes());
    header.extend_from_slice(b"\r\n");
    out.splice(start..start, header);
}

/// Converts the value on top of the stack to a reply in the caller's
/// protocol, and pops it (Redis' `luaReplyToRedisReply`). `script_resp3`:
/// the script asked for RESP3 with `redis.setresp(3)`.
unsafe fn write_value(l: *mut lua_State, out: &mut Vec<u8>, resp3: bool, script_resp3: bool) {
    // SAFETY: Lua's API; a value is on top.
    unsafe {
        if lua::lua_checkstack(l, 4) == 0 {
            resp::write_error(out, b"ERR reached lua stack limit");
            lua::lua_pop(l, 1);
            return;
        }
        match lua::lua_type(l, -1) {
            lua::LUA_TSTRING => resp::write_bulk(out, to_bytes(l, -1)),
            lua::LUA_TBOOLEAN => {
                let b = lua::lua_toboolean(l, -1) != 0;
                if !script_resp3 {
                    if b {
                        resp::write_integer(out, 1);
                    } else {
                        out.extend_from_slice(if resp3 { resp::NULL } else { resp::NULL_BULK });
                    }
                } else if resp3 {
                    out.extend_from_slice(if b { b"#t\r\n" } else { b"#f\r\n" });
                } else {
                    resp::write_integer(out, i64::from(b));
                }
            }
            lua::LUA_TNUMBER => resp::write_integer(out, to_integer(lua::lua_tonumber(l, -1))),
            lua::LUA_TTABLE => return write_table(l, out, resp3, script_resp3),
            _ => out.extend_from_slice(if resp3 { resp::NULL } else { resp::NULL_BULK }),
        }
        lua::lua_pop(l, 1);
    }
}

/// [`write_value`] for a table, which it pops.
unsafe fn write_table(l: *mut lua_State, out: &mut Vec<u8>, resp3: bool, script_resp3: bool) {
    // SAFETY: Lua's API; a table is on top. Each branch leaves the stack as
    // Redis' does.
    unsafe {
        let field = |name: &CStr| {
            lua::lua_pushstring(l, name.as_ptr());
            lua::lua_rawget(l, -2);
            lua::lua_type(l, -1)
        };
        if field(c"err") == lua::LUA_TSTRING {
            lua::lua_pop(l, 1);
            lua::lua_getfield(l, -1, c"err".as_ptr());
            let message = to_c_bytes(l, -1);
            write_error_text(out, message);
            lua::lua_pop(l, 2);
            return;
        }
        lua::lua_pop(l, 1);
        if field(c"ok") == lua::LUA_TSTRING {
            out.push(b'+');
            out.extend(without_newlines(to_c_bytes(l, -1)));
            out.extend_from_slice(b"\r\n");
            lua::lua_pop(l, 2);
            return;
        }
        lua::lua_pop(l, 1);
        if field(c"double") == lua::LUA_TNUMBER {
            let d = lua::lua_tonumber(l, -1);
            if resp3 {
                out.push(b',');
                write_double_text(out, d);
                out.extend_from_slice(b"\r\n");
            } else {
                let mut text = Vec::new();
                write_double_text(&mut text, d);
                resp::write_bulk(out, &text);
            }
            lua::lua_pop(l, 2);
            return;
        }
        lua::lua_pop(l, 1);
        if field(c"big_number") == lua::LUA_TSTRING {
            let number: Vec<u8> = without_newlines(to_bytes(l, -1)).collect();
            if resp3 {
                out.push(b'(');
                out.extend_from_slice(&number);
                out.extend_from_slice(b"\r\n");
            } else {
                resp::write_bulk(out, &number);
            }
            lua::lua_pop(l, 2);
            return;
        }
        lua::lua_pop(l, 1);
        if field(c"verbatim_string") == lua::LUA_TTABLE {
            lua::lua_pushstring(l, c"format".as_ptr());
            lua::lua_rawget(l, -2);
            if lua::lua_type(l, -1) == lua::LUA_TSTRING {
                let format = to_c_bytes(l, -1);
                lua::lua_pushstring(l, c"string".as_ptr());
                lua::lua_rawget(l, -3);
                if lua::lua_type(l, -1) == lua::LUA_TSTRING {
                    let text = to_bytes(l, -1);
                    if resp3 {
                        out.push(b'=');
                        out.extend_from_slice(
                            itoa::Buffer::new().format(text.len() + 4).as_bytes(),
                        );
                        out.extend_from_slice(b"\r\n");
                        for i in 0..3 {
                            out.push(format.get(i).copied().unwrap_or(b' '));
                        }
                        out.push(b':');
                        out.extend_from_slice(text);
                        out.extend_from_slice(b"\r\n");
                    } else {
                        resp::write_bulk(out, text);
                    }
                    lua::lua_pop(l, 4);
                    return;
                }
                lua::lua_pop(l, 1);
            }
            lua::lua_pop(l, 1);
        }
        lua::lua_pop(l, 1);
        if field(c"map") == lua::LUA_TTABLE {
            let start = out.len();
            let mut len = 0;
            lua::lua_pushnil(l);
            while lua::lua_next(l, -2) != 0 {
                lua::lua_pushvalue(l, -2);
                write_value(l, out, resp3, script_resp3);
                write_value(l, out, resp3, script_resp3);
                len += 1;
            }
            if resp3 {
                insert_header(out, start, b'%', len);
            } else {
                insert_header(out, start, b'*', 2 * len);
            }
            lua::lua_pop(l, 2);
            return;
        }
        lua::lua_pop(l, 1);
        if field(c"set") == lua::LUA_TTABLE {
            let start = out.len();
            let mut len = 0;
            lua::lua_pushnil(l);
            while lua::lua_next(l, -2) != 0 {
                lua::lua_pop(l, 1);
                lua::lua_pushvalue(l, -1);
                write_value(l, out, resp3, script_resp3);
                len += 1;
            }
            insert_header(out, start, if resp3 { b'~' } else { b'*' }, len);
            lua::lua_pop(l, 2);
            return;
        }
        lua::lua_pop(l, 1);
        // An array, up to the first nil.
        let start = out.len();
        let mut len = 0;
        loop {
            lua::lua_pushnumber(l, (len + 1) as f64);
            lua::lua_rawget(l, -2);
            if lua::lua_type(l, -1) == lua::LUA_TNIL {
                lua::lua_pop(l, 1);
                break;
            }
            write_value(l, out, resp3, script_resp3);
            len += 1;
        }
        insert_header(out, start, b'*', len);
        lua::lua_pop(l, 1);
    }
}

/// The reply to a script that raised an error, which is on top (Redis'
/// `luaCallFunction`).
unsafe fn write_failure(l: *mut lua_State, name: &[u8; 40], out: &mut Vec<u8>) {
    // SAFETY: Lua's API; the error is on top, and stays.
    unsafe {
        if lua::lua_istable(l, -1) == 0 {
            let message: &[u8] = if lua::lua_isstring(l, -1) != 0 {
                to_c_bytes(l, -1)
            } else {
                b"execution failure"
            };
            let mut text = b"ERR Error running script ".to_vec();
            text.extend_from_slice(name);
            text.extend_from_slice(b", ");
            text.extend_from_slice(&message[..message.len().min(100)]);
            write_error_text(out, &text);
            return;
        }
        let info = |field: &CStr| {
            lua::lua_getfield(l, -1, field.as_ptr());
            let value = (lua::lua_isstring(l, -1) != 0).then(|| to_c_bytes(l, -1).to_vec());
            lua::lua_pop(l, 1);
            value
        };
        let message = info(c"err");
        let source = info(c"source");
        let line = info(c"line");
        out.push(b'-');
        // Without one, Redis 7.2.4 crashes; Redis 8 says this.
        out.extend_from_slice(message.as_deref().unwrap_or(b"ERR unknown error"));
        if let (Some(line), Some(source)) = (line, source) {
            out.extend_from_slice(b" script: ");
            out.extend_from_slice(name);
            out.extend_from_slice(b", on ");
            out.extend_from_slice(&source);
            out.push(b':');
            out.extend_from_slice(&line);
            out.push(b'.');
        }
        out.extend_from_slice(b"\r\n");
    }
}

/// Raises the error table on top.
///
/// # Safety
///
/// Nothing that needs dropping may be live in the caller's frame.
unsafe fn raise(l: *mut lua_State, message: &[u8]) -> ! {
    // SAFETY: as documented.
    unsafe {
        push_error(l, message);
        lua::lua_error(l)
    }
}

unsafe extern "C-unwind" fn redis_log(l: *mut lua_State) -> c_int {
    // SAFETY: called by Lua while a script runs.
    unsafe {
        let argc = lua::lua_gettop(l);
        if argc < 2 {
            raise(l, b"redis.log() requires two arguments or more.");
        }
        if lua::lua_isnumber(l, -argc) == 0 {
            raise(l, b"First argument must be a number (log level).");
        }
        let level = lua::lua_tonumber(l, -argc) as c_int;
        if !(0..=3).contains(&level) {
            raise(l, b"Invalid debug level.");
        }
        // Logged at Redis' default verbosity, notice: notices and warnings.
        if level >= 2 {
            log_message(l, argc);
        }
        0
    }
}

/// Logs `redis.log`'s arguments after the level, separated by spaces.
unsafe fn log_message(l: *mut lua_State, argc: c_int) {
    let mut message = Vec::new();
    for j in 1..argc {
        // SAFETY: the arguments are on the stack.
        let s = unsafe { lua::lua_tolstring(l, -argc + j, &mut 0) };
        if !s.is_null() {
            if j != 1 {
                message.push(b' ');
            }
            // SAFETY: as above.
            message.extend_from_slice(unsafe { to_bytes(l, -argc + j) });
        }
    }
    eprintln!("{}", String::from_utf8_lossy(&message));
}

unsafe extern "C-unwind" fn redis_setresp(l: *mut lua_State) -> c_int {
    // SAFETY: called by Lua while a script runs.
    unsafe {
        let argc = lua::lua_gettop(l);
        if argc != 1 {
            raise(l, b"redis.setresp() requires one argument.");
        }
        let resp = lua::lua_tonumber(l, -argc) as c_int;
        if resp != 2 && resp != 3 {
            raise(l, b"RESP version must be 2 or 3.");
        }
        current().session.resp3 = resp == 3;
        0
    }
}

unsafe extern "C-unwind" fn redis_sha1hex(l: *mut lua_State) -> c_int {
    // SAFETY: called by Lua while a script runs.
    unsafe {
        if lua::lua_gettop(l) != 1 {
            raise(l, b"wrong number of arguments");
        }
        let digest = sha1_hex(to_bytes(l, 1));
        lua::lua_pushlstring(l, digest.as_ptr().cast(), digest.len());
        1
    }
}

unsafe extern "C-unwind" fn redis_error_reply(l: *mut lua_State) -> c_int {
    // SAFETY: called by Lua while a script runs.
    unsafe {
        if lua::lua_gettop(l) != 1 || lua::lua_type(l, -1) != lua::LUA_TSTRING {
            push_error(l, b"wrong number or type of arguments");
            return 1;
        }
        push_error_reply(l);
        1
    }
}

/// `redis.error_reply`'s table, from the string on top.
unsafe fn push_error_reply(l: *mut lua_State) {
    // SAFETY: a string is on top.
    let message = unsafe { to_c_bytes(l, -1) };
    let mut error = Vec::with_capacity(message.len() + 1);
    if !message.starts_with(b"-") {
        error.push(b'-');
    }
    error.extend_from_slice(message);
    // SAFETY: Lua's API.
    unsafe { push_error(l, &error) };
}

unsafe extern "C-unwind" fn redis_status_reply(l: *mut lua_State) -> c_int {
    // SAFETY: called by Lua while a script runs.
    unsafe {
        if lua::lua_gettop(l) != 1 || lua::lua_type(l, -1) != lua::LUA_TSTRING {
            push_error(l, b"wrong number or type of arguments");
            return 1;
        }
        lua::lua_newtable(l);
        lua::lua_pushstring(l, c"ok".as_ptr());
        lua::lua_pushvalue(l, -3);
        lua::lua_settable(l, -3);
        1
    }
}

unsafe extern "C-unwind" fn redis_set_repl(l: *mut lua_State) -> c_int {
    // SAFETY: called by Lua while a script runs.
    unsafe {
        let argc = lua::lua_gettop(l);
        if argc != 1 {
            raise(l, b"redis.set_repl() requires one argument.");
        }
        // Checked, but there's no replication to change.
        let flags = lua::lua_tonumber(l, -1) as c_int;
        if flags & !3 != 0 {
            raise(
                l,
                b"Invalid replication flags. Use REPL_AOF, REPL_REPLICA, REPL_ALL or REPL_NONE.",
            );
        }
        0
    }
}

unsafe extern "C-unwind" fn redis_acl_check_cmd(l: *mut lua_State) -> c_int {
    // SAFETY: called by Lua while a script runs.
    let known = unsafe {
        let run = current();
        if !take_args(l, run) {
            lua::lua_error(l);
        }
        cmd::is_command(Args::new(&run.args, &run.ranges).get(0))
    };
    if !known {
        // SAFETY: nothing to drop.
        unsafe { raise(l, b"Invalid command passed to redis.acl_check_cmd()") }
    }
    // No ACLs: everyone may run everything.
    // SAFETY: Lua's API.
    unsafe { lua::lua_pushboolean(l, 1) };
    1
}

unsafe extern "C-unwind" fn redis_breakpoint(l: *mut lua_State) -> c_int {
    // No debugger: as Redis when it isn't debugging.
    // SAFETY: Lua's API.
    unsafe { lua::lua_pushboolean(l, 0) };
    1
}

unsafe extern "C-unwind" fn redis_debug(_: *mut lua_State) -> c_int {
    0
}

unsafe extern "C-unwind" fn redis_replicate_commands(l: *mut lua_State) -> c_int {
    // Always so since Redis 7.
    // SAFETY: Lua's API.
    unsafe { lua::lua_pushboolean(l, 1) };
    1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn script_names_are_lowercase_sha1() {
        assert_eq!(
            &sha1_hex(b"return 1"),
            b"e0e1f9fabfc9d4800c877a703b823ac0578ff8db"
        );
    }

    #[test]
    fn shebangs_parse_as_redis() {
        assert!(shebang(b"return 1").unwrap().0.compat);
        let (flags, len) = shebang(b"#!lua flags=no-writes,allow-oom\nreturn 1").unwrap();
        assert!(!flags.compat && flags.no_writes);
        assert_eq!(len, 31);
        assert_eq!(
            shebang(b"#!lua").unwrap_err(),
            b"ERR Invalid script shebang"
        );
        assert_eq!(
            shebang(b"#!python\n").unwrap_err(),
            b"ERR Unexpected engine in script shebang: #!python"
        );
        assert_eq!(
            shebang(b"#!lua flags=x\n").unwrap_err(),
            b"ERR Unexpected flag in script shebang: x"
        );
        assert_eq!(
            shebang(b"#!lua name=x\n").unwrap_err(),
            b"ERR Unknown lua shebang option: name=x"
        );
    }

    #[test]
    fn doubles_print_as_d2string() {
        let text = |d: f64| {
            let mut out = Vec::new();
            write_double_text(&mut out, d);
            String::from_utf8(out).unwrap()
        };
        assert_eq!(text(-0.0), "-0");
        assert_eq!(text(3.0), "3");
        assert_eq!(text(0.1), "0.1");
        assert_eq!(text(1e300), "1e+300");
        assert_eq!(text(f64::NEG_INFINITY), "-inf");
    }
}
