//! Functions (`FUNCTION`, `FCALL`): libraries of Lua code, each loaded once
//! (`FUNCTION LOAD`), whose functions run by name, as Redis 7.2.4's
//! functions.c and function_lua.c have them.
//!
//! The libraries run in a Lua state of their own, set up as Redis sets up its
//! function engine: the `redis` API scripts get, with globals read-only behind
//! an empty table whose metatable leads to them. While a library loads, that
//! metatable leads elsewhere instead: to a table with only `redis.register_function`,
//! `redis.log` and the version, so a library's top level can't run commands. The
//! functions it registers are kept in Lua's registry, and here by name.
//!
//! What persists is each library's code: on the store (`Store::functions`), which
//! snapshots and the log's base write and read. When it changes there (a snapshot
//! loaded), the libraries here are made again from it.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use super::*;
use crate::rdb;

/// Function flags, in Redis' order (`scripts_flags_def`).
pub(crate) const FLAG_NAMES: [&str; 5] = [
    "no-writes",
    "allow-oom",
    "allow-stale",
    "no-cluster",
    "allow-cross-slot-keys",
];
pub(crate) const NO_WRITES: u8 = 1;
pub(crate) const ALLOW_OOM: u8 = 2;

const NAME_ERROR: &[u8] =
    b"Library names can only contain letters, numbers, or underscores(_) and \
    must be at least one character long";

/// A function of a library.
pub(crate) struct Function {
    pub(crate) name: Box<[u8]>,
    pub(crate) description: Option<Box<[u8]>>,
    pub(crate) flags: u8,
    /// Its callback, in Lua's registry.
    lua_ref: c_int,
}

pub(crate) struct Library {
    pub(crate) name: Box<[u8]>,
    /// As loaded, shebang and all.
    pub(crate) code: Arc<[u8]>,
    pub(crate) functions: Vec<Function>,
}

/// The libraries, and the Lua state their functions live in.
#[derive(Default)]
pub(crate) struct Functions {
    engine: Option<FnEngine>,
    /// By name: what `FUNCTION LIST` and `DUMP` go through, in this order.
    libraries: BTreeMap<Box<[u8]>, Library>,
    /// Each function's library, by the function's name in lowercase.
    index: HashMap<Vec<u8>, Box<[u8]>>,
    /// The version of the store's code these were made from.
    generation: u64,
}

/// `name` is made of letters, digits and underscores, and isn't empty
/// (Redis' `functionsVerifyName`).
fn valid_name(name: &[u8]) -> bool {
    !name.is_empty() && name.iter().all(|&b| b.is_ascii_alphanumeric() || b == b'_')
}

/// A library's `#!lua name=...` line (Redis' `functionExtractLibMetaData`):
/// its engine, its name, and where its code starts (with the line's newline,
/// so error lines count from the top).
fn metadata(code: &[u8]) -> Result<(Vec<u8>, Vec<u8>, usize), Vec<u8>> {
    // (C strings: what's past a zero byte isn't there.)
    let code = &code[..memchr::memchr(0, code).unwrap_or(code.len())];
    if !code.starts_with(b"#!") {
        return Err(b"Missing library metadata".to_vec());
    }
    let Some(end) = memchr::memchr(b'\n', code) else {
        return Err(b"Invalid library metadata".to_vec());
    };
    let parts = crate::acl::split_args(&code[..end]).unwrap_or_default();
    let Some(first) = parts.first() else {
        return Err(b"Invalid library metadata".to_vec());
    };
    let engine = first.get(2..).unwrap_or_default().to_vec();
    let mut name = None;
    for part in &parts[1..] {
        if part.len() >= 5 && part[..5].eq_ignore_ascii_case(b"name=") {
            if name.is_some() {
                return Err(
                    b"Invalid metadata value, name argument was given multiple times".to_vec(),
                );
            }
            name = Some(part[5..].to_vec());
            continue;
        }
        let mut e = b"Invalid metadata value given: ".to_vec();
        e.extend_from_slice(part);
        return Err(e);
    }
    let Some(name) = name else {
        return Err(b"Library name was not given".to_vec());
    };
    Ok((engine, name, end))
}

/// What a restore does with the libraries already there.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Policy {
    Flush,
    Append,
    Replace,
}

impl Functions {
    fn engine(&mut self) -> &mut FnEngine {
        self.engine.get_or_insert_with(FnEngine::new)
    }

    /// Makes the libraries again from the store's code if that changed
    /// (a snapshot was loaded). A library that doesn't load is left out,
    /// and said so.
    pub(crate) fn follow(&mut self, store: &crate::store::Store) {
        let (generation, codes) = store.function_codes();
        if generation == self.generation {
            return;
        }
        self.clear();
        for code in codes {
            if let Err(e) = self.create(&code, false, None) {
                eprintln!("Failed creating function, {}", String::from_utf8_lossy(&e));
            }
        }
        self.generation = generation;
    }

    /// Puts the libraries' code on the store, for snapshots and the log.
    fn persist(&mut self, store: &crate::store::Store) {
        let codes = self.libraries.values().map(|l| l.code.clone()).collect();
        self.generation = store.set_function_codes(codes);
    }

    fn clear(&mut self) {
        let libraries = std::mem::take(&mut self.libraries);
        self.index.clear();
        if let Some(engine) = &mut self.engine {
            for library in libraries.values() {
                for function in &library.functions {
                    engine.unref(function.lua_ref);
                }
            }
        }
    }

    pub(crate) fn libraries(&self) -> impl Iterator<Item = &Library> {
        self.libraries.values()
    }

    pub(crate) fn counts(&self) -> (usize, usize) {
        (self.libraries.len(), self.index.len())
    }

    /// The function called `name`, in any case.
    pub(crate) fn find(&self, name: &[u8]) -> Option<&Function> {
        let library = self.index.get(&name.to_ascii_lowercase())?;
        self.libraries[library]
            .functions
            .iter()
            .find(|f| f.name.eq_ignore_ascii_case(name))
    }

    fn unlink(&mut self, name: &[u8]) -> Option<Library> {
        let library = self.libraries.remove(name)?;
        for function in &library.functions {
            self.index.remove(&function.name.to_ascii_lowercase());
        }
        Some(library)
    }

    fn link(&mut self, library: Library) {
        for function in &library.functions {
            self.index
                .entry(function.name.to_ascii_lowercase())
                .or_insert_with(|| library.name.clone());
        }
        self.libraries.insert(library.name.clone(), library);
    }

    fn drop_library(&mut self, library: Library) {
        let engine = self.engine();
        for function in &library.functions {
            engine.unref(function.lua_ref);
        }
    }

    /// Loads a library (Redis' `functionsCreateWithLibraryCtx`), replacing
    /// one of its name with `replace`: its name, or the error to reply
    /// (without its `ERR`).
    pub(crate) fn create(
        &mut self,
        code: &[u8],
        replace: bool,
        timeout: Option<Duration>,
    ) -> Result<Vec<u8>, Vec<u8>> {
        let (engine, name, code_at) = metadata(code)?;
        if !valid_name(&name) {
            return Err(NAME_ERROR.to_vec());
        }
        if !engine.eq_ignore_ascii_case(b"LUA") {
            let mut e = b"Engine '".to_vec();
            e.extend_from_slice(&engine);
            e.extend_from_slice(b"' not found");
            return Err(e);
        }
        if self.libraries.contains_key(&name[..]) && !replace {
            let mut e = b"Library '".to_vec();
            e.extend_from_slice(&name);
            e.extend_from_slice(b"' already exists");
            return Err(e);
        }
        let old = self.unlink(&name);
        let compiled = self.engine().create(&code[code_at..], timeout);
        let functions = match compiled {
            Ok(functions) => functions,
            Err(e) => {
                if let Some(old) = old {
                    self.link(old);
                }
                return Err(e);
            }
        };
        let library = Library {
            name: name.clone().into(),
            code: code.into(),
            functions,
        };
        let failure = if library.functions.is_empty() {
            Some(b"No functions registered".to_vec())
        } else {
            library
                .functions
                .iter()
                .find(|f| self.index.contains_key(&f.name.to_ascii_lowercase()))
                .map(|f| {
                    let mut e = b"Function ".to_vec();
                    e.extend_from_slice(&f.name);
                    e.extend_from_slice(b" already exists");
                    e
                })
        };
        if let Some(e) = failure {
            self.drop_library(library);
            if let Some(old) = old {
                self.link(old);
            }
            return Err(e);
        }
        self.link(library);
        if let Some(old) = old {
            self.drop_library(old);
        }
        Ok(name)
    }

    /// `FUNCTION LOAD`.
    pub(crate) fn load(
        &mut self,
        store: &crate::store::Store,
        code: &[u8],
        replace: bool,
        timeout: Option<Duration>,
    ) -> Result<Vec<u8>, Vec<u8>> {
        self.follow(store);
        let name = self.create(code, replace, timeout)?;
        self.persist(store);
        Ok(name)
    }

    /// `FUNCTION DELETE`: whether there was such a library.
    pub(crate) fn delete(&mut self, store: &crate::store::Store, name: &[u8]) -> bool {
        self.follow(store);
        let Some(library) = self.unlink(name) else {
            return false;
        };
        self.drop_library(library);
        self.persist(store);
        true
    }

    /// `FUNCTION FLUSH`.
    pub(crate) fn flush(&mut self, store: &crate::store::Store) {
        self.follow(store);
        self.clear();
        self.persist(store);
    }

    /// `FUNCTION DUMP`'s payload: each library's code after Redis' function
    /// opcode, as RDB strings, then the RDB version and a checksum.
    pub(crate) fn dump(&mut self, store: &crate::store::Store) -> Vec<u8> {
        self.follow(store);
        let mut out = Vec::new();
        for library in self.libraries.values() {
            out.push(rdb::OPCODE_FUNCTION2);
            rdb::put_string(&mut out, &library.code);
        }
        rdb::finish_payload(&mut out);
        out
    }

    /// `FUNCTION RESTORE` (Redis' `functionRestoreCommand`): all or nothing.
    pub(crate) fn restore(
        &mut self,
        store: &crate::store::Store,
        payload: &[u8],
        policy: Policy,
    ) -> Result<(), Vec<u8>> {
        self.follow(store);
        let Some(body) = rdb::payload_body(payload) else {
            return Err(b"DUMP payload version or checksum are wrong".to_vec());
        };
        // Load into a set of their own first, in the same Lua state.
        let mut fresh = Functions {
            engine: self.engine.take(),
            ..Functions::default()
        };
        let loaded = (|| {
            let mut reader = rdb::Reader::new(body);
            while reader.remaining() > 0 {
                match reader.byte() {
                    Some(rdb::OPCODE_FUNCTION_PRE_GA) => {
                        return Err(b"Pre-GA function format not supported".to_vec());
                    }
                    Some(rdb::OPCODE_FUNCTION2) => {}
                    _ => return Err(b"given type is not a function".to_vec()),
                }
                let Some(code) = reader.string() else {
                    return Err(b"Failed loading library payload".to_vec());
                };
                fresh.create(&code, false, None)?;
            }
            Ok(())
        })();
        self.engine = fresh.engine.take();
        if let Err(e) = loaded {
            fresh.engine = self.engine.take();
            fresh.clear();
            self.engine = fresh.engine.take();
            return Err(e);
        }
        let joined = match policy {
            Policy::Flush => {
                self.clear();
                Ok(())
            }
            _ => self.check_join(&fresh, policy == Policy::Replace),
        };
        if let Err(e) = joined {
            fresh.engine = self.engine.take();
            fresh.clear();
            self.engine = fresh.engine.take();
            return Err(e);
        }
        let names: Vec<Box<[u8]>> = fresh.libraries.keys().cloned().collect();
        for name in names {
            if let Some(old) = self.unlink(&name) {
                self.drop_library(old);
            }
            let library = fresh.unlink(&name).expect("there");
            self.link(library);
        }
        self.persist(store);
        Ok(())
    }

    /// Redis' `libraryJoin`'s checks: a library of the same name only with
    /// `replace`, and no function of the same name in another library.
    fn check_join(&self, fresh: &Functions, replace: bool) -> Result<(), Vec<u8>> {
        for name in fresh.libraries.keys() {
            if self.libraries.contains_key(name) && !replace {
                let mut e = b"Library ".to_vec();
                e.extend_from_slice(name);
                e.extend_from_slice(b" already exists");
                return Err(e);
            }
        }
        for function in fresh.libraries.values().flat_map(|l| &l.functions) {
            let lower = function.name.to_ascii_lowercase();
            // Those of a library being replaced go with it.
            let clash = self
                .index
                .get(&lower)
                .is_some_and(|library| !(replace && fresh.libraries.contains_key(library)));
            if clash {
                let mut e = b"Function ".to_vec();
                e.extend_from_slice(&function.name);
                e.extend_from_slice(b" already exists");
                return Err(e);
            }
        }
        Ok(())
    }

    /// Runs `function` (Redis' `luaEngineCall`).
    ///
    /// # Safety
    ///
    /// With `RUN` set.
    pub(crate) unsafe fn call<'k>(
        &mut self,
        name: &[u8],
        keys: impl ExactSizeIterator<Item = &'k [u8]>,
        argv: impl ExactSizeIterator<Item = &'k [u8]>,
        resp3: bool,
        out: &mut Vec<u8>,
    ) {
        let Some(function) = self.find(name) else {
            return;
        };
        let (lua_ref, name) = (function.lua_ref, function.name.clone());
        // SAFETY: as the caller says.
        unsafe { self.engine().call(lua_ref, &name, keys, argv, resp3, out) }
    }
}

/// The Lua state functions run in.
pub(crate) struct FnEngine {
    l: *mut lua_State,
    since_gc: u32,
}

// SAFETY: used only by whoever holds `Scripting::functions`.
unsafe impl Send for FnEngine {}

impl Drop for FnEngine {
    fn drop(&mut self) {
        // SAFETY: nothing uses the state any more.
        unsafe { lua::lua_close(self.l) }
    }
}

/// The handler a function runs under (function_lua.c's, verbatim).
const FN_ERROR_HANDLER: &[u8] = b"local dbg = debug\n\
debug = nil\n\
local error_handler = function (err)\n\
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
end\n\
return error_handler";

/// A library loading on this thread: what it has registered so far, and
/// when it must be done.
struct Loading {
    functions: Vec<Function>,
    started: Instant,
    timeout: Option<Duration>,
}

thread_local! {
    static LOADING: Cell<*mut Loading> = const { Cell::new(std::ptr::null_mut()) };
}

impl FnEngine {
    /// Redis' `luaEngineInitEngine`.
    fn new() -> FnEngine {
        // SAFETY: a fresh state, set up by Lua's API.
        unsafe {
            let l = new_state();
            // What a library's top level sees.
            lua::lua_newtable(l);
            lua::lua_newtable(l);
            set_function(l, c"register_function", register_function);
            set_function(l, c"log", redis_log);
            set_number(l, c"LOG_DEBUG", 0.0);
            set_number(l, c"LOG_VERBOSE", 1.0);
            set_number(l, c"LOG_NOTICE", 2.0);
            set_number(l, c"LOG_WARNING", 3.0);
            set_number(l, c"REDIS_VERSION_NUM", REDIS_VERSION_NUM);
            lua::lua_pushstring(l, c"REDIS_VERSION".as_ptr());
            lua::lua_pushstring(l, c"7.2.4".as_ptr());
            lua::lua_settable(l, -3);
            set_metatable_function(l, c"__index", missing_global);
            lua::lua_setfield(l, -2, c"redis".as_ptr());
            set_metatable_function(l, c"__index", missing_global);
            protect(l);
            lua::lua_setfield(l, lua::LUA_REGISTRYINDEX, c"__LIBRARY_API__".as_ptr());

            lua::lua_pushstring(l, c"__ERROR_HANDLER__".as_ptr());
            let loaded = lua::luaL_loadbuffer(
                l,
                FN_ERROR_HANDLER.as_ptr().cast(),
                FN_ERROR_HANDLER.len(),
                c"@err_handler_def".as_ptr(),
            );
            assert_eq!(loaded, 0, "the error handler compiles");
            assert_eq!(lua::lua_pcall(l, 0, 1, 0), 0, "the error handler runs");
            lua::lua_settable(l, lua::LUA_REGISTRYINDEX);

            lua::lua_pushvalue(l, lua::LUA_GLOBALSINDEX);
            set_metatable_function(l, c"__index", missing_global);
            protect(l);
            lua::lua_pop(l, 1);

            lua::lua_pushvalue(l, lua::LUA_GLOBALSINDEX);
            lua::lua_setfield(l, lua::LUA_REGISTRYINDEX, c"__GLOBALS_API__".as_ptr());

            // New, empty globals, which lead to the real ones.
            lua::lua_newtable(l);
            lua::lua_newtable(l);
            lua::lua_pushvalue(l, lua::LUA_GLOBALSINDEX);
            lua::lua_setfield(l, -2, c"__index".as_ptr());
            lua_enablereadonlytable(l, -1, 1);
            lua::lua_setmetatable(l, -2);
            lua_enablereadonlytable(l, -1, 1);
            lua::lua_replace(l, lua::LUA_GLOBALSINDEX);
            FnEngine { l, since_gc: 0 }
        }
    }

    /// Points the globals' metatable at the registry table `api`, as Redis
    /// does around a load (protecting the globals again, not the metatable).
    unsafe fn lead_globals_to(&mut self, api: &CStr) {
        let l = self.l;
        // SAFETY: the globals have a metatable; this leaves the stack as it was.
        unsafe {
            lua::lua_getmetatable(l, lua::LUA_GLOBALSINDEX);
            lua_enablereadonlytable(l, -1, 0);
            lua::lua_getfield(l, lua::LUA_REGISTRYINDEX, api.as_ptr());
            lua::lua_setfield(l, -2, c"__index".as_ptr());
            lua_enablereadonlytable(l, lua::LUA_GLOBALSINDEX, 1);
            lua::lua_pop(l, 1);
        }
    }

    /// Runs a library's code so it registers its functions (Redis'
    /// `luaEngineCreate`), or returns the error.
    fn create(&mut self, code: &[u8], timeout: Option<Duration>) -> Result<Vec<Function>, Vec<u8>> {
        let l = self.l;
        let mut loading = Loading {
            functions: Vec::new(),
            started: Instant::now(),
            timeout,
        };
        // SAFETY: the state is ours; this leaves the stack as it was.
        let result = unsafe {
            self.lead_globals_to(c"__LIBRARY_API__");
            let result = if lua::luaL_loadbuffer(
                l,
                code.as_ptr().cast(),
                code.len(),
                c"@user_function".as_ptr(),
            ) != 0
            {
                let mut e = b"Error compiling function: ".to_vec();
                e.extend_from_slice(to_c_bytes(l, -1));
                lua::lua_pop(l, 1);
                Err(e)
            } else {
                LOADING.set(&raw mut loading);
                lua::lua_sethook(l, Some(load_hook), lua::LUA_MASKCOUNT, HOOK_INSTRUCTIONS);
                let failed = lua::lua_pcall(l, 0, 0, 0) != 0;
                LOADING.set(std::ptr::null_mut());
                if failed {
                    let mut e = b"Error registering functions: ".to_vec();
                    e.extend_from_slice(&error_message(l));
                    lua::lua_pop(l, 1);
                    Err(e)
                } else {
                    Ok(())
                }
            };
            self.lead_globals_to(c"__GLOBALS_API__");
            lua::lua_sethook(l, None, 0, 0);
            result
        };
        match result {
            Ok(()) => Ok(loading.functions),
            Err(e) => {
                for function in &loading.functions {
                    self.unref(function.lua_ref);
                }
                Err(e)
            }
        }
    }

    fn unref(&mut self, lua_ref: c_int) {
        // SAFETY: a reference this state made.
        unsafe { lua::luaL_unref(self.l, lua::LUA_REGISTRYINDEX, lua_ref) }
    }

    /// Calls a function's callback with its keys and arguments (Redis'
    /// `luaEngineCall` and `luaCallFunction`), writing its reply.
    ///
    /// # Safety
    ///
    /// With `RUN` set.
    unsafe fn call<'k>(
        &mut self,
        lua_ref: c_int,
        name: &[u8],
        keys: impl ExactSizeIterator<Item = &'k [u8]>,
        argv: impl ExactSizeIterator<Item = &'k [u8]>,
        resp3: bool,
        out: &mut Vec<u8>,
    ) {
        let l = self.l;
        // SAFETY: the state is ours, and `RUN` is set for the callbacks.
        unsafe {
            lua::lua_getfield(l, lua::LUA_REGISTRYINDEX, c"__ERROR_HANDLER__".as_ptr());
            lua::lua_rawgeti(l, lua::LUA_REGISTRYINDEX, lua_ref as lua::lua_Integer);
            lua::lua_sethook(l, Some(count_hook), lua::LUA_MASKCOUNT, HOOK_INSTRUCTIONS);
            push_array(l, keys);
            push_array(l, argv);
            let failed = lua::lua_pcall(l, 2, 1, -4) != 0;
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
            lua::lua_pop(l, 1);
        }
    }
}

/// The error on top as Redis' `luaExtractErrorInformation` reads its message.
unsafe fn error_message(l: *mut lua_State) -> Vec<u8> {
    // SAFETY: the error is on top, and stays.
    unsafe {
        if lua::lua_isstring(l, -1) != 0 {
            let mut e = b"ERR ".to_vec();
            e.extend_from_slice(to_c_bytes(l, -1));
            return e;
        }
        if lua::lua_istable(l, -1) == 0 {
            return b"(null)".to_vec();
        }
        lua::lua_getfield(l, -1, c"err".as_ptr());
        let message = if lua::lua_isstring(l, -1) != 0 {
            to_c_bytes(l, -1).to_vec()
        } else {
            b"(null)".to_vec()
        };
        lua::lua_pop(l, 1);
        message
    }
}

/// Stops a library's top level that runs too long (Redis' `luaEngineLoadHook`).
unsafe extern "C-unwind" fn load_hook(l: *mut lua_State, _: *mut lua::lua_Debug) {
    let loading = LOADING.get();
    if loading.is_null() {
        return;
    }
    // SAFETY: set by `create` while the library runs.
    let late = unsafe {
        let loading = &*loading;
        loading
            .timeout
            .is_some_and(|timeout| loading.started.elapsed() > timeout)
    };
    if late {
        // SAFETY: nothing to drop here.
        unsafe {
            lua::lua_sethook(l, Some(load_hook), lua::LUA_MASKLINE, 0);
            push_error(l, b"FUNCTION LOAD timeout");
            lua::lua_error(l);
        }
    }
}

/// `redis.register_function`, which only a library's top level can call.
unsafe extern "C-unwind" fn register_function(l: *mut lua_State) -> c_int {
    // SAFETY: called by Lua; the error, if any, is pushed and raised last.
    let failed = unsafe { register(l) };
    if failed {
        // SAFETY: the error table is on top; nothing to drop.
        unsafe { lua::lua_error(l) }
    }
    0
}

/// Registers the function the arguments describe; on an error, pushes it
/// and returns true.
unsafe fn register(l: *mut lua_State) -> bool {
    let loading = LOADING.get();
    // SAFETY: Lua's API, with `LOADING` set by `create` while a library loads.
    unsafe {
        if loading.is_null() {
            push_error(
                l,
                b"redis.register_function can only be called on FUNCTION LOAD command",
            );
            return true;
        }
        let loading = &mut *loading;
        let described = match lua::lua_gettop(l) {
            1 => named_args(l),
            2 => positional_args(l),
            _ => Err(&b"wrong number of arguments to redis.register_function"[..]),
        };
        let (name, description, flags, lua_ref) = match described {
            Ok(described) => described,
            Err(e) => {
                push_error(l, e);
                return true;
            }
        };
        let refused: Option<&[u8]> = if !valid_name(&name) {
            Some(NAME_ERROR)
        } else if loading.functions.iter().any(|f| *f.name == *name) {
            Some(b"Function already exists in the library")
        } else {
            None
        };
        if let Some(e) = refused {
            lua::luaL_unref(l, lua::LUA_REGISTRYINDEX, lua_ref);
            push_error(l, e);
            return true;
        }
        loading.functions.push(Function {
            name: name.into(),
            description: description.map(Into::into),
            flags,
            lua_ref,
        });
        false
    }
}

type Described = (Vec<u8>, Option<Vec<u8>>, u8, c_int);

/// The string at `index`, if it is one (or a number).
unsafe fn string_at(l: *mut lua_State, index: c_int) -> Option<Vec<u8>> {
    // SAFETY: Lua's API.
    unsafe { (lua::lua_isstring(l, index) != 0).then(|| to_bytes(l, index).to_vec()) }
}

/// `redis.register_function('name', callback)`.
unsafe fn positional_args(l: *mut lua_State) -> Result<Described, &'static [u8]> {
    // SAFETY: Lua's API, with two arguments.
    unsafe {
        let Some(name) = string_at(l, 1) else {
            return Err(b"first argument to redis.register_function must be a string");
        };
        if lua::lua_isfunction(l, 2) == 0 {
            return Err(b"second argument to redis.register_function must be a function");
        }
        let lua_ref = lua::luaL_ref(l, lua::LUA_REGISTRYINDEX);
        Ok((name, None, 0, lua_ref))
    }
}

/// `redis.register_function{function_name=..., callback=..., flags=..., description=...}`.
unsafe fn named_args(l: *mut lua_State) -> Result<Described, &'static [u8]> {
    let (mut name, mut description, mut flags, mut lua_ref) = (None, None, 0, None);
    // SAFETY: Lua's API, with one argument.
    let result = unsafe {
        (|| {
            if lua::lua_istable(l, 1) == 0 {
                return Err(
                    &b"calling redis.register_function with a single argument is only \
                    applicable to Lua table (representing named arguments)."[..],
                );
            }
            lua::lua_pushnil(l);
            while lua::lua_next(l, -2) != 0 {
                if lua::lua_isstring(l, -2) == 0 {
                    return Err(
                        &b"named argument key given to redis.register_function is not a \
                        string"[..],
                    );
                }
                let key = to_c_bytes(l, -2).to_ascii_lowercase();
                match &key[..] {
                    b"function_name" => {
                        name = Some(string_at(l, -1).ok_or(
                            &b"function_name argument given to \
                            redis.register_function must be a string"[..],
                        )?);
                    }
                    b"description" => {
                        description = Some(string_at(l, -1).ok_or(
                            &b"description argument given \
                            to redis.register_function must be a string"[..],
                        )?);
                    }
                    b"callback" => {
                        if lua::lua_isfunction(l, -1) == 0 {
                            return Err(&b"callback argument given to redis.register_function \
                                must be a function"[..]);
                        }
                        if let Some(old) = lua_ref.replace(lua::luaL_ref(l, lua::LUA_REGISTRYINDEX))
                        {
                            lua::luaL_unref(l, lua::LUA_REGISTRYINDEX, old);
                        }
                        continue; // (the reference took the value)
                    }
                    b"flags" => {
                        if lua::lua_istable(l, -1) == 0 {
                            return Err(&b"flags argument to redis.register_function must be a \
                                table representing function flags"[..]);
                        }
                        flags = read_flags(l).ok_or(&b"unknown flag given"[..])?;
                    }
                    _ => return Err(&b"unknown argument given to redis.register_function"[..]),
                }
                lua::lua_pop(l, 1);
            }
            Ok(())
        })()
    };
    let unref = |r: Option<c_int>| {
        if let Some(r) = r {
            // SAFETY: a reference made above.
            unsafe { lua::luaL_unref(l, lua::LUA_REGISTRYINDEX, r) }
        }
    };
    if let Err(e) = result {
        unref(lua_ref);
        return Err(e);
    }
    let Some(name) = name else {
        unref(lua_ref);
        return Err(b"redis.register_function must get a function name argument");
    };
    let Some(lua_ref) = lua_ref else {
        return Err(b"redis.register_function must get a callback argument");
    };
    Ok((name, description, flags, lua_ref))
}

/// The flags in the table on top (Redis' `luaRegisterFunctionReadFlags`).
unsafe fn read_flags(l: *mut lua_State) -> Option<u8> {
    let mut flags = 0;
    // SAFETY: Lua's API; a table is on top, and stays.
    unsafe {
        for j in 1.. {
            lua::lua_pushnumber(l, f64::from(j));
            lua::lua_gettable(l, -2);
            if lua::lua_type(l, -1) == lua::LUA_TNIL {
                lua::lua_pop(l, 1);
                return Some(flags);
            }
            if lua::lua_isstring(l, -1) == 0 {
                lua::lua_pop(l, 1);
                return None;
            }
            let flag = to_c_bytes(l, -1);
            let found = FLAG_NAMES
                .iter()
                .position(|name| name.as_bytes().eq_ignore_ascii_case(flag));
            lua::lua_pop(l, 1);
            flags |= 1 << found?;
        }
    }
    Some(flags)
}
