//! A package's own file, read beside the module the registry served.
//!
//! Upstream resolves an `npm:` specifier into a directory on disk, so a
//! wasm blob next to the javascript that instantiates it is simply a
//! file beside a file. Here a package is a url, so the blob is a url
//! too, and two things have to be true for a function to reach it:
//! `import.meta.resolve` has to answer with where the module landed
//! rather than with the range that was asked for, and `Deno.readFile`
//! has to take an http url.
//!
//! One test binary, because it sets the environment the loader reads
//! and a binary with one test in it has one thread in it. Nothing here
//! reaches the network: the cache is written by hand and the loader is
//! told not to fetch, so what is being tested is the resolution and not
//! a registry's mood.

#![cfg(feature = "isolate")]

use std::path::{Path, PathBuf};

use zou_deno::Isolate;
use zou_functions::{Call, Function, Runtime};

/// The same name the loader will look under: the sha256 of the url, in
/// a directory named for the host.
fn at(cache: &Path, url: &str) -> PathBuf {
    let mut hasher = <sha2::Sha256 as sha2::Digest>::new();
    sha2::Digest::update(&mut hasher, url.as_bytes());
    let digest = sha2::Digest::finalize(hasher);
    let name: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    cache.join("esm.sh").join(name)
}

/// One cache entry, under the url it was asked for, saying the url it
/// was served from and the url the module the registry served is at.
/// Those are all the same url for a file, and none of them is the same
/// for a package: a version range is asked for, a build is served, and
/// the registry says which build it was.
fn keep(cache: &Path, asked: &str, served: &str, content_type: &str, body: &str) {
    let path = at(cache, asked);
    std::fs::create_dir_all(path.parent().expect("a directory")).expect("the cache directory");
    std::fs::write(&path, body).expect("the file");
    std::fs::write(
        path.with_extension("about"),
        format!("{served}\n{content_type}\n{served}\n"),
    )
    .expect("what the file is");
}

fn answer(function: &Function) -> Result<zou_functions::Answer, zou_functions::Failed> {
    Isolate::new().invoke(
        function,
        Call {
            method: "GET".to_string(),
            url: "http://localhost:9000/functions/v1/hello".to_string(),
            headers: Vec::new(),
            body: Vec::new(),
            execution_id: "one".to_string(),
        },
    )
}

#[test]
fn a_package_reads_a_file_of_its_own_from_beside_where_it_landed() {
    let cache = tempfile::tempdir().expect("a temporary directory");
    // Safety: this is the only test in this binary, so there is no
    // other thread to read the environment while it is being set.
    unsafe {
        std::env::set_var("ZOU_MODULE_CACHE", cache.path());
        std::env::set_var("ZOU_MODULE_CACHE_ONLY", "1");
    }
    keep(
        cache.path(),
        "https://esm.sh/wasmy@1",
        "https://esm.sh/wasmy@1.2.0/es2022/wasmy.mjs",
        "application/javascript; charset=utf-8",
        "export const ready = \"yes\";",
    );
    keep(
        cache.path(),
        "https://esm.sh/wasmy@1.2.0/es2022/data.bin",
        "https://esm.sh/wasmy@1.2.0/es2022/data.bin",
        "application/octet-stream",
        "beside",
    );

    let dir = tempfile::tempdir().expect("a temporary directory");
    let entrypoint = dir.path().join("index.ts");
    let function = Function::new("hello", entrypoint.clone());
    std::fs::write(
        &entrypoint,
        r#"
        import { ready } from "npm:wasmy@1";
        const where = import.meta.resolve("npm:wasmy@1");
        const read = new TextDecoder().decode(
          await Deno.readFile(new URL("data.bin", where)),
        );
        const sync = new TextDecoder().decode(
          Deno.readFileSync(new URL("data.bin", where)),
        );
        Deno.serve(() => new Response(`${where} ${ready} ${read} ${sync}`));
        "#,
    )
    .expect("the function's file");
    let served = answer(&function).expect("an answer");
    assert_eq!(
        String::from_utf8(served.bytes().to_vec()).expect("utf-8"),
        "https://esm.sh/wasmy@1.2.0/es2022/wasmy.mjs yes beside beside"
    );

    // And what is not in the cache is not fetched, since this server was
    // told not to, which is the same answer a read gets as an import.
    std::fs::write(
        &entrypoint,
        r#"
        await Deno.readFile(new URL("https://esm.sh/wasmy@1.2.0/es2022/nothing.bin"));
        "#,
    )
    .expect("the function's file");
    let refused = answer(&function).expect_err("a file nothing has");
    assert!(refused.why().contains("nothing.bin"), "{refused}");
    assert!(
        refused.why().contains("not in the module cache"),
        "{refused}"
    );

    // The synchronous spelling at the top of a module is allowed to
    // fetch, because a package reading its own wasm with `readFileSync`
    // while it is being loaded is the ordinary shape and there is
    // nothing else for the isolate to be doing. So what refuses it here
    // is the same thing that refused the await: a server told not to
    // fetch at all.
    std::fs::write(
        &entrypoint,
        r#"
        Deno.readFileSync(new URL("https://esm.sh/wasmy@1.2.0/es2022/nothing.bin"));
        "#,
    )
    .expect("the function's file");
    let refused = answer(&function).expect_err("a file nothing has");
    assert!(refused.why().contains("told not to fetch"), "{refused}");

    // Once the module is loaded the other rule is back: a handler that
    // reads synchronously serves what has been fetched and does not
    // stop the isolate to fetch what has not.
    std::fs::write(
        &entrypoint,
        r#"
        Deno.serve(() => {
          Deno.readFileSync(new URL("https://esm.sh/wasmy@1.2.0/es2022/nothing.bin"));
          return new Response("never");
        });
        "#,
    )
    .expect("the function's file");
    let refused = answer(&function).expect_err("a file nothing has");
    assert!(
        refused.why().contains("a synchronous read will not fetch"),
        "{refused}"
    );

    // A url that is neither a file nor http is still refused, and by
    // what it is rather than by what it is not.
    std::fs::write(
        &entrypoint,
        r#"
        Deno.readFileSync(new URL("data:text/plain,hello"));
        "#,
    )
    .expect("the function's file");
    let refused = answer(&function).expect_err("a url that is not a place");
    assert!(
        refused
            .why()
            .contains("a file may only be read through a file url or an http url, not data:"),
        "{refused}"
    );

    unsafe {
        std::env::remove_var("ZOU_MODULE_CACHE");
        std::env::remove_var("ZOU_MODULE_CACHE_ONLY");
    }
}
