//! Builds Redis 7.2.4's Lua (with its libraries) and number formatting from
//! `vendor/`, for scripting (`src/script.rs`). See `vendor/README.md`.

fn main() {
    let lua = "vendor/lua/src";
    // Redis' `deps/lua/src/Makefile` (CORE_O and LIB_O): the interpreter
    // and every library Redis links, though scripts get only some of them.
    let lua_files = [
        "lapi",
        "lcode",
        "ldebug",
        "ldo",
        "ldump",
        "lfunc",
        "lgc",
        "llex",
        "lmem",
        "lobject",
        "lopcodes",
        "lparser",
        "lstate",
        "lstring",
        "ltable",
        "ltm",
        "lundump",
        "lvm",
        "lzio",
        "strbuf",
        "fpconv",
        "lauxlib",
        "lbaselib",
        "ldblib",
        "liolib",
        "lmathlib",
        "loslib",
        "ltablib",
        "lstrlib",
        "loadlib",
        "linit",
        "lua_cjson",
        "lua_struct",
        "lua_cmsgpack",
        "lua_bit",
    ];
    let mut build = cc::Build::new();
    // Redis' `deps/Makefile` flags for Lua.
    build
        .include(lua)
        .define("LUA_ANSI", None)
        .define("ENABLE_CJSON_GLOBAL", None)
        .define("REDIS_STATIC", Some(""))
        .define("LUA_USE_MKSTEMP", None)
        .opt_level(2)
        .warnings(false);
    for file in lua_files {
        build.file(format!("{lua}/{file}.c"));
    }
    build
        .file("vendor/lua/redis_random.c")
        .file("vendor/fpconv/fpconv_dtoa.c")
        .compile("redis_lua");
    println!("cargo:rerun-if-changed=vendor");
}
