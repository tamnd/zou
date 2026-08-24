//! The node built ins, in the shape a package reaches for one.
//!
//! These exist because of what a registry serves rather than because a
//! function author asks for them. esm.sh builds a package twice: ask for
//! it as a browser and the platform bits are stubbed out, ask for it as
//! Deno and what comes back imports `node:buffer`, `node:process` and
//! the rest. The Deno build is the one the package author tested on a
//! Deno runtime, and the browser build is a fallback, so the Deno build
//! is the one worth serving and these modules are the price of it.
//!
//! None of this is node. It is the part of each built in that packages
//! actually reach for, written on top of the web platform the prelude
//! already has: a `Buffer` is a `Uint8Array`, a hash is the same op
//! `crypto.subtle.digest` goes through, a stream is an event emitter
//! and a queue. What is missing is missing by name: `node:fs` will read
//! a file and refuse to write one, and a built in that is not in this
//! list is a sentence saying so rather than a module that half works.
//!
//! A shim is javascript and not rust because every one of them is a
//! translation between two javascript shapes, and there is nothing for
//! the host to do in any of it.

/// The source of a node built in, or nothing if this runtime has no
/// such module.
///
/// The name arrives without the `node:` in front of it, and a subpath
/// like `stream/promises` is its own module rather than a lookup into
/// the parent, which is what it is in node too.
pub fn source(name: &str) -> Option<&'static str> {
    Some(match name {
        "assert" => include_str!("node/assert.js"),
        "buffer" => include_str!("node/buffer.js"),
        // A function does not get a process, a thread or a fork, and
        // these three say so from every call that would need one. They
        // are here rather than absent because a package that imports
        // one at the top and calls it in a branch nobody takes runs
        // fine against a stub and not at all against a refused import,
        // which was the difference on seven of the forty in the
        // examples corpus.
        "child_process" => include_str!("node/child_process.js"),
        "cluster" => include_str!("node/cluster.js"),
        "crypto" => include_str!("node/crypto.js"),
        "diagnostics_channel" => include_str!("node/diagnostics_channel.js"),
        "events" => include_str!("node/events.js"),
        "fs" => include_str!("node/fs.js"),
        "fs/promises" => include_str!("node/fs_promises.js"),
        "module" => include_str!("node/module.js"),
        "os" => include_str!("node/os.js"),
        // The three names for the same file: node's `path` is posix
        // here, because the host a function runs on is, and asking for
        // `path/win32` on it gets the same answer node gives a program
        // that asked for posix separators on a posix machine.
        "path" | "path/posix" | "path/win32" => include_str!("node/path.js"),
        "process" => include_str!("node/process.js"),
        "querystring" => include_str!("node/querystring.js"),
        "stream" => include_str!("node/stream.js"),
        "stream/promises" => include_str!("node/stream_promises.js"),
        "stream/web" => include_str!("node/stream_web.js"),
        "string_decoder" => include_str!("node/string_decoder.js"),
        "timers" => include_str!("node/timers.js"),
        "timers/promises" => include_str!("node/timers_promises.js"),
        "url" => include_str!("node/url.js"),
        "util" => include_str!("node/util.js"),
        "util/types" => include_str!("node/util_types.js"),
        "worker_threads" => include_str!("node/worker_threads.js"),
        _ => return None,
    })
}

/// Every name this runtime has a built in under.
///
/// The list is here as well as in the match because something has to be
/// able to say what the whole set is without asking about each name it
/// can think of, and what asks is the registry below.
pub const NAMES: &[&str] = &[
    "assert",
    "buffer",
    "child_process",
    "cluster",
    "crypto",
    "diagnostics_channel",
    "events",
    "fs",
    "fs/promises",
    "module",
    "os",
    "path",
    "path/posix",
    "path/win32",
    "process",
    "querystring",
    "stream",
    "stream/promises",
    "stream/web",
    "string_decoder",
    "timers",
    "timers/promises",
    "url",
    "util",
    "util/types",
    "worker_threads",
];

/// A module that puts every built in where a `require` can reach it.
///
/// A script cannot `import`. It calls `require("path")` in the middle of
/// running, and the answer has to already be a value by then, which for
/// an es module means it was imported before the script started. So the
/// module that stands in for a script imports this one first, and this
/// one imports all of them and leaves them in a map the require reads.
///
/// All of them rather than the ones the script asks for, because what a
/// script asks for is decided while it runs: a `require` inside a branch
/// or built out of a variable is ordinary code in a package that ships
/// two implementations of something. Reading the graph to guess at the
/// set would be a guess that is wrong exactly where it matters, so the
/// price is paid once instead: these modules are small, this binary
/// carries them, and only a function that imports a commonjs package
/// pays it at all.
///
/// The value under each name is the default export, because a built in
/// here is written as an es module whose default is the object node
/// hands a `require`, and the namespace around it is the shape an
/// `import` wants rather than the one a script wants.
pub fn registry() -> String {
    let mut text = String::from("// Generated: every node built in, for a script's require.\n");
    for (nth, name) in NAMES.iter().enumerate() {
        text.push_str(&format!("import * as m{nth} from \"node:{name}\";\n"));
    }
    text.push_str("globalThis.__zouBuiltins = new Map([\n");
    for (nth, name) in NAMES.iter().enumerate() {
        text.push_str(&format!("  [\"node:{name}\", m{nth}.default ?? m{nth}],\n"));
    }
    text.push_str("]);\n");
    text
}

#[cfg(test)]
mod tests {
    use super::source;

    /// The eight the issue named, which are the ones a registry's Deno
    /// build imports.
    #[test]
    fn the_built_ins_a_package_reaches_for_are_all_here() {
        for name in [
            "buffer", "crypto", "events", "stream", "util", "process", "path", "fs",
        ] {
            assert!(source(name).is_some(), "node:{name}");
        }
    }

    #[test]
    fn a_subpath_is_its_own_module() {
        assert!(source("fs/promises").is_some());
        assert!(source("stream/promises").is_some());
        assert!(source("timers/promises").is_some());
        assert!(source("util/types").is_some());
    }

    /// The seven functions the examples corpus lost when it asked the
    /// registry as Deno, which is what these three are for.
    #[test]
    fn the_built_ins_a_deno_build_imports_are_here_too() {
        for name in ["child_process", "diagnostics_channel", "module"] {
            assert!(source(name).is_some(), "node:{name}");
        }
    }

    /// A built in nobody has written yet is nothing, and the caller
    /// turns that into a sentence naming it. The ones that are here
    /// and refuse from every call are a different thing: a package can
    /// import those and not reach them.
    /// The list and the match are two spellings of the same set, and a
    /// name in one and not the other is a module that is imported and
    /// never arrives.
    #[test]
    fn every_name_on_the_list_is_a_built_in_this_binary_carries() {
        for name in super::NAMES {
            assert!(source(name).is_some(), "node:{name}");
        }
        let module = super::registry();
        for name in super::NAMES {
            assert!(
                module.contains(&format!("\"node:{name}\"")),
                "node:{name} is not in the registry"
            );
        }
        assert!(module.contains("globalThis.__zouBuiltins"), "{module}");
    }

    #[test]
    fn a_built_in_that_is_not_here_is_not_pretended_to_be() {
        assert!(source("dgram").is_none());
        assert!(source("v8").is_none());
        assert!(source("path/nonsense").is_none());
    }
}
