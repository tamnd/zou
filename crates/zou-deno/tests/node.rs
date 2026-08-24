//! The node built ins, run.
//!
//! Every one of these is a function that imports a built in and answers
//! with what it found, which is the only way to test a shim: the thing
//! being checked is what v8 does with the module, not what a rust
//! function thinks is in the file.
//!
//! Nothing here touches the network. A built in is javascript this
//! binary carries, so the loader answers these imports out of memory.

#![cfg(feature = "isolate")]

use zou_deno::Isolate;
use zou_functions::{Call, Function, Runtime};

/// One function, one call, and what it answered with.
fn served(code: &str) -> String {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let entrypoint = dir.path().join("index.ts");
    std::fs::write(&entrypoint, code).expect("the function's file");
    let function = Function::new("hello", entrypoint);
    let answer = Isolate::new()
        .invoke(
            &function,
            Call {
                method: "GET".to_string(),
                url: "http://localhost:9000/functions/v1/hello".to_string(),
                headers: Vec::new(),
                body: Vec::new(),
                execution_id: "one".to_string(),
            },
        )
        .unwrap_or_else(|why| panic!("{why}"));
    String::from_utf8(answer.bytes().to_vec()).expect("utf-8")
}

#[test]
fn a_buffer_is_bytes_with_the_encodings_node_reads_and_writes() {
    let said = served(
        r#"
        import { Buffer } from "node:buffer";
        const said = [
          Buffer.from("zou").toString("base64"),
          Buffer.from("em91", "base64").toString(),
          Buffer.from("7a6f75", "hex").toString(),
          Buffer.from("zou").toString("hex"),
          String(Buffer.byteLength("héllo")),
          Buffer.concat([Buffer.from("a"), Buffer.from("b")]).toString(),
          String(Buffer.isBuffer(Buffer.alloc(1))),
          String(Buffer.from("ab").equals(Buffer.from("ab"))),
          JSON.stringify(Buffer.from("ab").toJSON()),
          Buffer.from(Buffer.from("ab").toJSON()).toString(),
          Buffer.from("ff", "hex").toString("base64url"),
          String(Buffer.from("zou") instanceof Uint8Array),
        ];
        Deno.serve(() => new Response(said.join(" ")));
        "#,
    );
    assert_eq!(
        said,
        "em91 zou zou 7a6f75 6 ab true true \
         {\"type\":\"Buffer\",\"data\":[97,98]} ab _w true"
    );
}

#[test]
fn a_buffer_reads_and_writes_the_fixed_widths() {
    let said = served(
        r#"
        import { Buffer } from "node:buffer";
        const held = Buffer.alloc(8);
        held.writeUInt32BE(0xdeadbeef, 0);
        held.writeUInt16LE(0x0102, 4);
        const said = [
          held.toString("hex"),
          String(held.readUInt32BE(0)),
          String(held.readUInt16LE(4)),
          String(held.readUInt16BE(4)),
          held.subarray(0, 2).toString("hex"),
          String(held.slice(0, 2) instanceof Buffer),
          String(Buffer.from("abcabc").indexOf("cab")),
        ];
        Deno.serve(() => new Response(said.join(" ")));
        "#,
    );
    assert_eq!(said, "deadbeef02010000 3735928559 258 513 dead true 2");
}

#[test]
fn an_emitter_calls_its_listeners_in_order_and_a_once_listener_only_once() {
    let said = served(
        r#"
        import EventEmitter, { once } from "node:events";
        const seen = [];
        const emitter = new EventEmitter();
        emitter.on("go", (what) => seen.push(`on:${what}`));
        emitter.once("go", (what) => seen.push(`once:${what}`));
        const removed = (what) => seen.push(`never:${what}`);
        emitter.on("go", removed);
        emitter.removeListener("go", removed);
        emitter.emit("go", 1);
        emitter.emit("go", 2);
        seen.push(`count:${emitter.listenerCount("go")}`);
        // An error with nobody listening is thrown rather than dropped.
        try {
          emitter.emit("error", new Error("loud"));
        } catch (why) {
          seen.push(`threw:${why.message}`);
        }
        const waited = once(emitter, "later");
        emitter.emit("later", "yes");
        seen.push(`awaited:${(await waited)[0]}`);
        Deno.serve(() => new Response(seen.join(" ")));
        "#,
    );
    assert_eq!(said, "on:1 once:1 on:2 count:1 threw:loud awaited:yes");
}

#[test]
fn a_path_is_joined_and_taken_apart_the_way_node_does_it() {
    let said = served(
        r#"
        import path, { join, basename } from "node:path";
        const said = [
          join("a", "b", "..", "c"),
          path.normalize("/a//b/../c/"),
          path.resolve("/a/b", "../c"),
          path.relative("/a/b/c", "/a/d"),
          path.dirname("/a/b/c.txt"),
          basename("/a/b/c.txt", ".txt"),
          path.extname("/a/b/c.tar.gz"),
          JSON.stringify(path.parse("/a/b/c.txt")),
          String(path.isAbsolute("a/b")),
          path.sep,
          path.format({ dir: "/a", base: "b.txt" }),
        ];
        Deno.serve(() => new Response(said.join(" ")));
        "#,
    );
    assert_eq!(
        said,
        "a/c /a/c/ /a/c ../../d /a/b c .gz \
         {\"root\":\"/\",\"dir\":\"/a/b\",\"base\":\"c.txt\",\"ext\":\".txt\",\"name\":\"c\"} \
         false / /a/b.txt"
    );
}

#[test]
fn process_is_the_global_one_and_the_module_is_the_same_object() {
    let said = served(
        r#"
        import process, { platform } from "node:process";
        const said = [
          String(process === globalThis.process),
          platform,
          process.versions.node.split(".")[0],
          String(process.version.startsWith("v")),
          typeof process.env.NOTHING_IS_SET_HERE,
          String(process.browser),
          process.cwd(),
        ];
        await new Promise((resolve) => process.nextTick(resolve));
        said.push("ticked");
        Deno.serve(() => new Response(said.join(" ")));
        "#,
    );
    assert_eq!(said, "true linux 20 true undefined false / ticked");
}

#[test]
fn a_hash_and_an_hmac_are_the_ones_the_rest_of_this_server_makes() {
    let said = served(
        r#"
        import crypto, { createHash, createHmac, timingSafeEqual } from "node:crypto";
        const said = [
          createHash("sha256").update("zou").digest("hex"),
          createHash("sha256").update("z").update("ou").digest("hex"),
          createHmac("sha256", "key").update("zou").digest("hex"),
          createHash("sha256").update("zou").digest("base64").slice(0, 8),
          String(crypto.randomBytes(16).length),
          String(crypto.randomUUID().length),
          String(timingSafeEqual(new Uint8Array([1, 2]), new Uint8Array([1, 2]))),
          String(timingSafeEqual(new Uint8Array([1, 2]), new Uint8Array([1, 3]))),
          String(crypto.webcrypto === globalThis.crypto),
        ];
        try {
          createHash("md5");
        } catch (why) {
          said.push(why.code);
        }
        Deno.serve(() => new Response(said.join(" ")));
        "#,
    );
    assert_eq!(
        said,
        "b20a7d254bdab4ee822c1973b2dca94197261860c5ad468b401c430a9d2c6ca4 \
         b20a7d254bdab4ee822c1973b2dca94197261860c5ad468b401c430a9d2c6ca4 \
         7fbd98bab015d4341fbcd53463f553eb95b397691decb54888cb5c214d220faa \
         sgp9JUva 16 36 true false true ERR_CRYPTO_INVALID_DIGEST"
    );
}

#[test]
fn util_promisifies_a_callback_and_formats_a_string() {
    let said = served(
        r#"
        import util, { promisify, inherits, format } from "node:util";
        const read = (name, back) => back(null, `read:${name}`);
        const said = [
          await promisify(read)("f"),
          format("%s has %d items: %j", "list", 2, { a: 1 }),
          util.inspect({ a: [1, "two"], b: new Map([["k", 1]]) }),
          String(util.types.isDate(new Date())),
          String(util.types.isTypedArray(new Uint8Array(1))),
          String(util.isDeepStrictEqual({ a: [1] }, { a: [1] })),
          String(util.isDeepStrictEqual({ a: [1] }, { a: [2] })),
        ];
        function Animal() {}
        Animal.prototype.speak = () => "noise";
        function Dog() {}
        inherits(Dog, Animal);
        said.push(new Dog().speak());
        Deno.serve(() => new Response(said.join(" | ")));
        "#,
    );
    assert_eq!(
        said,
        "read:f | list has 2 items: {\"a\":1} | \
         { a: [ 1, 'two' ], b: Map(1) { 'k' => 1 } } | true | true | true | false | noise"
    );
}

#[test]
fn a_stream_is_pushed_into_piped_through_and_read_out_of() {
    let said = served(
        r#"
        import { Readable, Writable, Transform, PassThrough, pipeline } from "node:stream";
        const seen = [];
        const source = Readable.from(["a", "b", "c"]);
        const upper = new Transform({
          transform(chunk, encoding, back) {
            back(null, String(chunk).toUpperCase());
          },
        });
        const held = [];
        const sink = new Writable({
          write(chunk, encoding, back) {
            held.push(String(chunk));
            back();
          },
        });
        await new Promise((resolve, reject) =>
          pipeline(source, upper, sink, (why) => (why ? reject(why) : resolve())),
        );
        seen.push(held.join(""));

        // And the other way of reading one, which is the iterator.
        const counted = new Readable({ objectMode: true });
        counted.push(1);
        counted.push(2);
        counted.push(null);
        const out = [];
        for await (const value of counted) {
          out.push(value);
        }
        seen.push(out.join("+"));

        // The bridge to the streams this runtime actually has.
        const web = new Response("web").body;
        const back = [];
        for await (const chunk of Readable.fromWeb(web)) {
          back.push(new TextDecoder().decode(chunk));
        }
        seen.push(back.join(""));

        const through = new PassThrough();
        const answer = new Response(Readable.toWeb(through));
        through.push(new TextEncoder().encode("piped"));
        through.push(null);
        seen.push(await answer.text());

        Deno.serve(() => new Response(seen.join(" ")));
        "#,
    );
    assert_eq!(said, "ABC 1+2 web piped");
}

#[test]
fn the_filesystem_reads_and_says_so_when_it_will_not_write() {
    let said = served(
        r#"
        import fs from "node:fs";
        import { readFile } from "node:fs/promises";
        const said = [];
        try {
          fs.readFileSync("/nothing/here.txt");
        } catch (why) {
          said.push(why.code);
        }
        try {
          await readFile("/nothing/here.txt");
        } catch (why) {
          said.push(why.code);
        }
        try {
          fs.writeFileSync("/tmp/x", "no");
        } catch (why) {
          said.push(why.message.includes("read only") ? "refused" : why.message);
        }
        said.push(String(fs.existsSync("/nothing/here.txt")));
        Deno.serve(() => new Response(said.join(" ")));
        "#,
    );
    assert_eq!(said, "ENOENT ENOENT refused false");
}

#[test]
fn the_smaller_built_ins_answer_the_way_a_package_expects() {
    let said = served(
        r#"
        import { fileURLToPath, pathToFileURL } from "node:url";
        import querystring from "node:querystring";
        import { StringDecoder } from "node:string_decoder";
        import assert from "node:assert";
        import os from "node:os";
        import { setTimeout as sleep } from "node:timers/promises";
        import { setTimeout as later, clearTimeout as stop } from "node:timers";

        const said = [
          fileURLToPath("file:///a/b c.txt"),
          pathToFileURL("/a/b c.txt").href,
          JSON.stringify(querystring.parse("a=1&b=two&a=3")),
          querystring.stringify({ a: [1, 2], b: "c d" }),
          os.platform() + os.EOL.length,
        ];

        const decoder = new StringDecoder("utf8");
        const bytes = new TextEncoder().encode("héllo");
        said.push(decoder.write(bytes.subarray(0, 2)) + decoder.end(bytes.subarray(2)));

        assert.strictEqual(1, 1);
        assert.deepStrictEqual({ a: [1] }, { a: [1] });
        try {
          assert.strictEqual(1, 2, "not the same");
        } catch (why) {
          said.push(`${why.name}:${why.code}:${why.message}`);
        }

        await sleep(1);
        const never = later(() => said.push("fired"), 5);
        stop(never);
        await sleep(20);
        said.push("slept");

        Deno.serve(() => new Response(said.join(" | ")));
        "#,
    );
    assert_eq!(
        said,
        "/a/b c.txt | file:///a/b%20c.txt | {\"a\":[\"1\",\"3\"],\"b\":\"two\"} | \
         a=1&a=2&b=c%20d | linux1 | héllo | AssertionError:ERR_ASSERTION:not the same | slept"
    );
}

/// A package here is a url rather than a directory somebody unpacked,
/// so the path of a file beside a module is that file's url, and the
/// reads take one. What this is under is `readFileSync(fileURLToPath(
/// new URL('./x.wasm', import.meta.url)))`, which is how every wasm
/// library finds its own wasm.
#[test]
fn the_path_of_a_file_beside_a_package_is_the_url_it_is_at() {
    let said = served(
        r#"
        import { fileURLToPath } from "node:url";
        const said = [
          fileURLToPath("https://esm.sh/x@1/y.wasm"),
          fileURLToPath(new URL("./z.ttf", "https://esm.sh/x@1/a/b.mjs")),
        ];
        try {
          fileURLToPath("data:text/plain,hi");
        } catch (why) {
          said.push(why.code);
        }
        Deno.serve(() => new Response(said.join(" ")));
        "#,
    );
    assert_eq!(
        said,
        "https://esm.sh/x@1/y.wasm https://esm.sh/x@1/a/z.ttf ERR_INVALID_URL_SCHEME"
    );
}

#[test]
fn a_diagnostics_channel_publishes_to_whoever_joined_it() {
    let said = served(
        r#"
        import dc, { channel, hasSubscribers, tracingChannel } from "node:diagnostics_channel";
        const heard = [];
        const one = channel("zou:test");
        const said = [];
        // Nobody is listening, which is the case a library checks for
        // before it builds a message it would throw away.
        said.push(String(one.hasSubscribers), String(hasSubscribers("zou:test")));
        one.subscribe((message, name) => heard.push(`${name}=${message.n}`));
        // The same name is the same channel, which is what makes a
        // subscriber registered anywhere hear a publish from anywhere.
        said.push(String(channel("zou:test") === one));
        dc.channel("zou:test").publish({ n: 1 });
        said.push(String(one.hasSubscribers), heard.join(","));

        // A subscriber that throws does not reach the publisher and
        // does not stop the one after it.
        const two = channel("zou:throws");
        two.subscribe(() => { throw new Error("no"); });
        two.subscribe(() => heard.push("after"));
        two.publish({});
        said.push(heard.at(-1));

        // The tracing wrapper, which is five channels under one name.
        const traced = tracingChannel("zou:traced");
        const seen = [];
        traced.subscribe({
          start: () => seen.push("start"),
          end: () => seen.push("end"),
          error: () => seen.push("error"),
        });
        said.push(String(traced.traceSync(() => 7)));
        said.push(seen.join(","));

        Deno.serve(() => new Response(said.join(" ")));
        "#,
    );
    assert_eq!(said, "false false true true zou:test=1 after 7 start,end");
}

#[test]
fn a_created_require_serves_the_built_ins_and_names_what_it_cannot() {
    let said = served(
        r#"
        import { createRequire, builtinModules, isBuiltin } from "node:module";
        const require = createRequire(import.meta.url);
        const said = [];
        said.push(String(isBuiltin("node:path")), String(isBuiltin("fs")), String(isBuiltin("react")));
        said.push(String(builtinModules.includes("crypto")));
        // The same module a require of it on node would give: the
        // default export, which is where a shim puts what CJS reads.
        const path = require("node:path");
        said.push(path.join("a", "b"));
        said.push(require("path").basename("/a/b.txt"));
        said.push(require.resolve("node:os"));
        try {
          require("some-package");
          said.push("no error");
        } catch (e) {
          said.push(e.message.split(".")[0]);
        }
        Deno.serve(() => new Response(said.join(" ")));
        "#,
    );
    assert_eq!(
        said,
        "true true false true a/b b.txt node:os Cannot find module 'some-package'"
    );
}

/// The three that are here so that a package can import them and not
/// reach them, which is the whole of what they are for.
#[test]
fn a_process_a_thread_and_a_fork_are_importable_and_refuse_when_called() {
    let said = served(
        r#"
        import { spawn, execSync, ChildProcess } from "node:child_process";
        import { Worker, isMainThread, threadId, receiveMessageOnPort } from "node:worker_threads";
        import cluster from "node:cluster";
        const said = [];
        // Importing is the point. Nothing above this line threw.
        said.push(String(typeof spawn), String(isMainThread), String(threadId), String(cluster.isPrimary));
        said.push(String(receiveMessageOnPort()));
        for (const [name, call] of [
          ["spawn", () => spawn("ls")],
          ["execSync", () => execSync("ls")],
          ["ChildProcess", () => new ChildProcess()],
          ["Worker", () => new Worker("./w.js")],
          ["fork", () => cluster.fork()],
        ]) {
          try {
            call();
            said.push(`${name} did not refuse`);
          } catch (e) {
            said.push(`${name}:${e.constructor.name}`);
          }
        }
        Deno.serve(() => new Response(said.join(" ")));
        "#,
    );
    assert_eq!(
        said,
        "function true 0 true undefined \
         spawn:TypeError execSync:TypeError ChildProcess:TypeError Worker:TypeError fork:TypeError"
    );
}

/// And what the refusal actually says, since a sentence naming the
/// reason is the difference between a package author reading this and
/// filing a bug about a missing module.
#[test]
fn the_refusal_says_a_function_has_no_processes() {
    let said = served(
        r#"
        import { spawn } from "node:child_process";
        let message = "no error";
        try {
          spawn("ls");
        } catch (e) {
          message = e.message;
        }
        Deno.serve(() => new Response(message));
        "#,
    );
    assert_eq!(
        said,
        "a function has no processes to start, so node:child_process spawn cannot work here"
    );
}

/// The round trip, in all three shapes: a synchronous call, a call with
/// a callback, and a stream handed one chunk at a time. The stream is
/// the one that matters, because the whole reason the compression is a
/// job with an id is that a transform cannot compress each chunk on
/// its own and call the result a gzip.
#[test]
fn zlib_compresses_and_decompresses_in_the_three_shapes_node_offers() {
    let said = served(
        r#"
        import zlib from "node:zlib";
        import { Buffer } from "node:buffer";
        import { promisify } from "node:util";
        const seen = [];

        const small = zlib.gzipSync("hello hello hello hello hello hello");
        seen.push(small[0] === 0x1f && small[1] === 0x8b ? "gzip" : "not gzip");
        seen.push(String(small.length < 35));
        seen.push(zlib.gunzipSync(small).toString());

        // Every framing, and the one that reads its own framing.
        seen.push(zlib.inflateSync(zlib.deflateSync("deflate")).toString());
        seen.push(zlib.inflateRawSync(zlib.deflateRawSync("raw")).toString());
        seen.push(zlib.unzipSync(zlib.gzipSync("unzip gz")).toString());
        seen.push(zlib.unzipSync(zlib.deflateSync("unzip zl")).toString());

        const back = await promisify(zlib.gzip)("callback");
        seen.push(zlib.gunzipSync(back).toString());

        // A stream, fed in pieces, and read back through the other one.
        const gzipping = zlib.createGzip();
        const parts = [];
        gzipping.on("data", (chunk) => parts.push(chunk));
        const done = new Promise((resolve) => gzipping.on("end", resolve));
        for (const piece of ["one ", "two ", "three"]) {
          gzipping.write(piece);
        }
        gzipping.end();
        await done;
        const whole = Buffer.concat(parts);
        seen.push(String(parts.length >= 1));
        seen.push(zlib.gunzipSync(whole).toString());

        // A body that is not a gzip at all.
        try {
          zlib.gunzipSync(new Uint8Array([1, 2, 3, 4]));
          seen.push("no error");
        } catch (e) {
          seen.push(e.code);
        }

        // And brotli, which this runtime does not have.
        try {
          zlib.brotliCompressSync("x");
          seen.push("no error");
        } catch (e) {
          seen.push(e.message);
        }

        Deno.serve(() => new Response(seen.join("|")));
        "#,
    );
    assert_eq!(
        said,
        "gzip|true|hello hello hello hello hello hello|deflate|raw|unzip gz|unzip zl|callback|true|one two three|Z_DATA_ERROR|node:zlib brotliCompressSync is brotli, which this runtime does not have"
    );
}

/// A stream cut into lines, in the three ways a caller reads them: the
/// event, the iterator and the question. The input here is a web
/// stream, because that is what a body is in this runtime, and the
/// last line has no newline after it, because a file usually does not.
#[test]
fn readline_cuts_an_input_into_lines_however_the_caller_reads_them() {
    let said = served(
        r#"
        import readline from "node:readline";
        import { createInterface } from "node:readline/promises";
        import { Readable } from "node:stream";
        const seen = [];

        const heard = [];
        const first = readline.createInterface({
          input: new Response("one\ntwo\r\nthree").body,
        });
        first.on("line", (line) => heard.push(line));
        await new Promise((resolve) => first.on("close", resolve));
        seen.push(heard.join("+"));

        const out = [];
        const second = readline.createInterface({ input: Readable.from(["a\nb", "\nc\n"]) });
        for await (const line of second) {
          out.push(line);
        }
        seen.push(out.join("+"));

        // A question, answered by the next line, and the output it was
        // written to.
        const written = [];
        const third = createInterface({
          input: Readable.from(["yes\nno\n"]),
          output: { write: (text) => written.push(text) },
        });
        const answer = await third.question("well? ");
        seen.push(answer);
        seen.push(written.join(""));
        third.close();

        Deno.serve(() => new Response(seen.join("|")));
        "#,
    );
    assert_eq!(said, "one+two+three|a+b+c|yes|well? ");
}

/// The store that survives an await, which is the whole of why this
/// built in is here: two chains started one after the other each keep
/// their own request through a timer, and neither of them can see the
/// other's or the one the top level never had.
#[test]
fn an_async_local_storage_follows_a_chain_through_its_awaits() {
    let said = served(
        r#"
        import { AsyncLocalStorage, AsyncResource, executionAsyncId } from "node:async_hooks";
        const held = new AsyncLocalStorage();
        const beside = new AsyncLocalStorage();
        const seen = [];

        async function work(name) {
          await new Promise((resolve) => setTimeout(resolve, 1));
          seen.push(`${name}:${held.getStore()?.id}`);
          await new Promise((resolve) => setTimeout(resolve, 1));
          seen.push(`${name}:${held.getStore()?.id}:${beside.getStore() ?? "none"}`);
          return held.getStore()?.id;
        }

        const both = [
          held.run({ id: "first" }, () => work("one")),
          held.run({ id: "second" }, () => work("two")),
        ];
        seen.push(`top:${held.getStore() ?? "none"}`);
        seen.push((await Promise.all(both)).join("+"));

        // A callback handed away and called later, from nowhere in
        // particular, still runs where it was made.
        const later = held.run({ id: "third" }, () => AsyncLocalStorage.bind(() => held.getStore().id));
        seen.push(later());

        // And one that says the chain is not inside it any more.
        seen.push(held.run({ id: "fourth" }, () => held.exit(() => String(held.getStore()))));

        // A resource, which is the same context with an id on it.
        const resource = held.run({ id: "fifth" }, () => new AsyncResource("thing"));
        seen.push(resource.runInAsyncScope(() => `${held.getStore().id}:${executionAsyncId() === resource.asyncId()}`));
        seen.push(String(executionAsyncId()));

        Deno.serve(() => new Response(seen.join("|")));
        "#,
    );
    assert_eq!(
        said,
        "top:none|one:first|two:second|one:first:none|two:second:none|first+second|third|undefined|fifth:true|1"
    );
}

/// A value written out as bytes and read back, which is what a package
/// asking for `node:v8` wants: the same serializer a clone goes
/// through, so the shapes that survive one survive the other.
#[test]
fn v8_writes_a_value_out_as_bytes_and_reads_it_back() {
    let said = served(
        r#"
        import v8 from "node:v8";
        const seen = [];

        const value = { when: new Date(0), held: new Map([["a", 1]]), big: 7n, bytes: new Uint8Array([1, 2]) };
        value.itself = value;
        const bytes = v8.serialize(value);
        seen.push(String(bytes.length > 0));
        const back = v8.deserialize(bytes);
        seen.push(String(back.when instanceof Date && back.when.getTime() === 0));
        seen.push(String(back.held.get("a")));
        seen.push(String(back.big === 7n));
        seen.push(String(back.bytes[1]));
        seen.push(String(back.itself === back));

        // The classes, which are the same two functions with somewhere
        // for a subclass to hang its hooks.
        const writer = new v8.DefaultSerializer();
        writer.writeValue({ said: "through the class" });
        const reader = new v8.DefaultDeserializer(writer.releaseBuffer());
        reader.readHeader();
        seen.push(reader.readValue().said);

        // What cannot be written, and what a function does not own.
        try {
          v8.serialize(() => 1);
          seen.push("no error");
        } catch (e) {
          seen.push(String(e instanceof Error));
        }
        try {
          v8.getHeapStatistics();
          seen.push("no error");
        } catch (e) {
          seen.push(e.message);
        }

        Deno.serve(() => new Response(seen.join("|")));
        "#,
    );
    assert_eq!(
        said,
        "true|true|1|true|2|true|through the class|true|node:v8 getHeapStatistics is about the isolate, which a function does not own"
    );
}

/// A request through `node:http`, against a listener in a thread beside
/// it, because what is being tested is a call that leaves the isolate:
/// the options node's api takes, turned into a request the host's http
/// client makes, and the answer read back through node's names for it.
#[test]
fn http_makes_a_request_and_reads_the_answer_back_with_nodes_names() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a port");
    let port = listener.local_addr().expect("an address").port();
    let heard = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let writing = std::sync::Arc::clone(&heard);
    std::thread::spawn(move || {
        for stream in listener.incoming().take(2) {
            let mut stream = match stream {
                Ok(stream) => stream,
                Err(_) => return,
            };
            // Until the headers and whatever body was promised have
            // both arrived, because a client is free to write them in
            // as many pieces as it likes.
            let _ = stream.set_read_timeout(Some(std::time::Duration::from_millis(200)));
            let mut asked = Vec::new();
            let mut got = [0u8; 4096];
            loop {
                match std::io::Read::read(&mut stream, &mut got) {
                    Ok(0) | Err(_) => break,
                    Ok(read) => asked.extend_from_slice(&got[..read]),
                }
                let said = String::from_utf8_lossy(&asked).to_string();
                let head = match said.find("\r\n\r\n") {
                    Some(at) => at + 4,
                    None => continue,
                };
                let wants = said
                    .to_lowercase()
                    .split("\r\n")
                    .find_map(|line| line.strip_prefix("content-length: ").map(str::to_string))
                    .and_then(|it| it.trim().parse::<usize>().ok())
                    .unwrap_or(0);
                if asked.len() >= head + wants {
                    break;
                }
            }
            writing
                .lock()
                .expect("the log")
                .push(String::from_utf8_lossy(&asked).to_string());
            let body = "one\ntwo";
            let _ = std::io::Write::write_all(
                &mut stream,
                format!(
                    "HTTP/1.1 201 Created\r\ncontent-type: text/plain\r\ncontent-length: {}\r\nx-said: here\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            );
        }
    });

    let said = served(&format!(
        r#"
        import http from "node:http";
        const seen = [];

        const answer = await new Promise((resolve, reject) => {{
          const call = http.request(
            {{ hostname: "127.0.0.1", port: {port}, path: "/where", method: "POST", headers: {{ "x-asked": "yes" }} }},
            (response) => {{
              const parts = [];
              response.on("data", (chunk) => parts.push(chunk));
              response.on("end", () => resolve({{ response, body: parts.join("") }}));
            }},
          );
          call.on("error", reject);
          call.setHeader("x-late", "also");
          call.write("a body");
          call.end();
        }});
        seen.push(String(answer.response.statusCode));
        seen.push(answer.response.statusMessage);
        seen.push(answer.response.headers["x-said"]);
        seen.push(answer.body.split("\n").join("+"));

        // The other spelling, which is a url and a get with no body.
        const second = await new Promise((resolve, reject) => {{
          http.get(`http://127.0.0.1:{port}/again`, (response) => {{
            response.on("data", () => {{}});
            response.on("end", () => resolve(response.statusCode));
          }}).on("error", reject);
        }});
        seen.push(String(second));

        // And the server this runtime will not give out.
        try {{
          http.createServer(() => {{}}).listen(8080);
          seen.push("no error");
        }} catch (e) {{
          seen.push(e.message);
        }}

        Deno.serve(() => new Response(seen.join("|")));
        "#
    ));
    assert_eq!(
        said,
        "201|Created|here|one+two|201|a function is answered on the server's own socket, so node:http createServer has no port to listen on"
    );
    let heard = heard.lock().expect("the log").join("\n");
    assert!(heard.contains("POST /where HTTP/1.1"), "{heard}");
    assert!(heard.contains("x-asked: yes"), "{heard}");
    assert!(heard.contains("x-late: also"), "{heard}");
    assert!(heard.contains("a body"), "{heard}");
    assert!(heard.contains("GET /again HTTP/1.1"), "{heard}");
}

/// The other module, which is the same one with the other default.
/// Nothing is sent here, because a request is sent when its body is
/// ended and this one never is: what is being checked is which url the
/// options were turned into.
#[test]
fn https_is_http_with_the_other_protocol_in_front_of_it() {
    let said = served(
        r#"
        import https from "node:https";
        import http from "node:http";
        const seen = [];

        seen.push(https.request({ hostname: "example.test", path: "/one" }).protocol);
        seen.push(http.request({ hostname: "example.test", path: "/one" }).protocol);
        // A url that says which it is keeps it.
        seen.push(https.request("http://example.test/two").protocol);
        seen.push(String(https.Agent === http.Agent));
        seen.push(String(https.STATUS_CODES[404]));

        Deno.serve(() => new Response(seen.join("|")));
        "#,
    );
    assert_eq!(said, "https:|http:|http:|true|Not Found");
}
