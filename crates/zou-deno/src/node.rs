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
        "crypto" => include_str!("node/crypto.js"),
        "events" => include_str!("node/events.js"),
        "fs" => include_str!("node/fs.js"),
        "fs/promises" => include_str!("node/fs_promises.js"),
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
        _ => return None,
    })
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

    /// A built in nobody has written yet is nothing, and the caller
    /// turns that into a sentence naming it.
    #[test]
    fn a_built_in_that_is_not_here_is_not_pretended_to_be() {
        assert!(source("child_process").is_none());
        assert!(source("worker_threads").is_none());
        assert!(source("path/nonsense").is_none());
    }
}
