//! A function asking about a name, against a zone in a thread beside
//! it.
//!
//! The server here is a real one in the sense that matters: it speaks
//! the wire format, on a socket, and the answers it gives come back
//! through the same parsing every answer goes through. It is not a
//! resolver, it does not recurse and it knows four names, which is all
//! a test needs to see that a query went out and an answer came back
//! as the records a package reads.
//!
//! A resolver of the test's own rather than the host's, because a test
//! that asks the internet what a domain's mail servers are is a test
//! that fails when somebody else changes their zone or when the box it
//! runs on has no network at all.

#![cfg(feature = "isolate")]

use std::net::UdpSocket;
use std::thread;

use zou_deno::Isolate;
use zou_functions::{Answer, Call, Function, Runtime};

fn answered(source: &str) -> String {
    let dir = tempfile::tempdir().expect("a temporary directory");
    std::fs::write(dir.path().join("index.ts"), source).expect("the function's file");
    let function = Function::new("hello", dir.path().join("index.ts"));
    let answer: Answer = Isolate::new()
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

/// A name as it goes on the wire, each label with its length in front.
fn name(out: &mut Vec<u8>, said: &str) {
    for label in said.split('.') {
        out.push(label.len() as u8);
        out.extend_from_slice(label.as_bytes());
    }
    out.push(0);
}

/// One record, whose own name is the pointer to the question's, which
/// is how every real answer is written and so is worth answering with.
fn record(out: &mut Vec<u8>, kind: u16, rdata: &[u8]) {
    out.extend_from_slice(&[0xc0, 0x0c]);
    out.extend_from_slice(&kind.to_be_bytes());
    out.extend_from_slice(&1u16.to_be_bytes());
    out.extend_from_slice(&60u32.to_be_bytes());
    out.extend_from_slice(&(rdata.len() as u16).to_be_bytes());
    out.extend_from_slice(rdata);
}

fn mx(preference: u16, exchange: &str) -> Vec<u8> {
    let mut rdata = preference.to_be_bytes().to_vec();
    name(&mut rdata, exchange);
    rdata
}

/// What the zone says, which is one answer per question type and a name
/// that is not in it at all.
fn zone(question: &[u8]) -> Option<Vec<u8>> {
    if question.len() < 13 {
        return None;
    }
    // Past the name, which this writes back verbatim rather than
    // reading: nothing here needs to know what was asked about except
    // whether it was the name that does not exist.
    let end = question[12..].iter().position(|byte| *byte == 0)? + 12;
    let asked = u16::from_be_bytes([*question.get(end + 1)?, *question.get(end + 2)?]);
    let about = String::from_utf8_lossy(&question[12..end]).to_string();

    let mut out = Vec::new();
    out.extend_from_slice(&question[0..2]);
    out.extend_from_slice(&0x8180u16.to_be_bytes());
    out.extend_from_slice(&1u16.to_be_bytes());
    let answers: Vec<(u16, Vec<u8>)> = if about.contains("nowhere") {
        Vec::new()
    } else {
        match asked {
            15 => vec![(15, mx(10, "in1.zone.test")), (15, mx(20, "in2.zone.test"))],
            1 => vec![(1, vec![192, 0, 2, 7])],
            16 => vec![(16, b"\x05first\x06second".to_vec())],
            _ => Vec::new(),
        }
    };
    out.extend_from_slice(&(answers.len() as u16).to_be_bytes());
    out.extend_from_slice(&[0, 0, 0, 0]);
    out.extend_from_slice(&question[12..end + 5]);
    for (kind, rdata) in &answers {
        record(&mut out, *kind, rdata);
    }
    if about.contains("nowhere") {
        // No such name, which is a different answer from a name with
        // nothing of that type.
        out[3] |= 3;
    }
    Some(out)
}

/// The zone, on a port the kernel picked, answering until the test
/// binary ends.
fn answering() -> u16 {
    let socket = UdpSocket::bind("127.0.0.1:0").expect("a port");
    let port = socket.local_addr().expect("bound").port();
    thread::spawn(move || {
        let mut into = [0u8; 512];
        while let Ok((read, from)) = socket.recv_from(&mut into) {
            if let Some(answer) = zone(&into[..read]) {
                let _ = socket.send_to(&answer, from);
            }
        }
    });
    port
}

/// The api a function written for Deno asks with, and the shapes
/// upstream answers in.
#[test]
fn a_function_asks_a_zone_what_it_says_about_a_name() {
    let port = answering();
    let said = answered(&format!(
        r#"
        const at = {{ nameServer: {{ ipAddr: "127.0.0.1", port: {port} }} }};
        const seen = [];
        seen.push(JSON.stringify(await Deno.resolveDns("zone.test", "MX", at)));
        seen.push(JSON.stringify(await Deno.resolveDns("zone.test", "A", at)));
        seen.push(JSON.stringify(await Deno.resolveDns("zone.test", "TXT", at)));
        Deno.serve(() => new Response(seen.join(" | ")));
        "#
    ));
    assert_eq!(
        said,
        concat!(
            r#"[{"preference":10,"exchange":"in1.zone.test"},{"preference":20,"exchange":"in2.zone.test"}]"#,
            r#" | ["192.0.2.7"]"#,
            r#" | [["first","second"]]"#,
        )
    );
}

/// The record a mail sender asks for, in the shape node hands it over:
/// the wire calls it a preference and node calls it a priority, and a
/// package written for node reads the second name.
#[test]
fn node_dns_answers_an_mx_query_in_the_shape_node_answers_it() {
    let port = answering();
    let said = answered(
        &r#"
        import dns from "node:dns/promises";
        import { Resolver } from "node:dns/promises";
        const seen = [];

        dns.setServers(["127.0.0.1:{port}"]);
        seen.push(JSON.stringify(dns.getServers()));
        const found = await dns.resolveMx("zone.test");
        seen.push(JSON.stringify(found));
        // Sorting by preference is the sender's own business, and what
        // matters here is that the field it sorts on is there.
        seen.push(String(found[0].priority < found[1].priority));

        // A resolver of its own, asked about its own servers rather
        // than the module's.
        const mine = new Resolver();
        mine.setServers(["127.0.0.1:{port}"]);
        seen.push(JSON.stringify(await mine.resolve("zone.test", "A")));

        Deno.serve(() => new Response(seen.join(" | ")));
        "#
        .replace("{port}", &port.to_string()),
    );
    assert_eq!(
        said,
        concat!(
            r#"["127.0.0.1:PORT"]"#,
            r#" | [{"priority":10,"exchange":"in1.zone.test"},{"priority":20,"exchange":"in2.zone.test"}]"#,
            " | true",
            r#" | ["192.0.2.7"]"#,
        )
        .replace("PORT", &port.to_string())
    );
}

/// The older calling convention, which is what most packages still use.
#[test]
fn node_dns_answers_the_old_way_too_and_says_when_there_is_no_such_name() {
    let port = answering();
    let said = answered(
        &r#"
        import dns from "node:dns";
        const seen = [];
        dns.setServers(["127.0.0.1:{port}"]);

        seen.push(await new Promise((done) =>
          dns.resolveMx("zone.test", (why, found) =>
            done(why ? `failed ${why.code}` : found.map((one) => one.exchange).join(",")))));

        // A name the zone does not have at all, which node says with a
        // code rather than an empty list.
        seen.push(await new Promise((done) =>
          dns.resolveMx("nowhere.test", (why, found) =>
            done(why ? `${why.code} ${why.syscall} ${why.hostname}` : JSON.stringify(found)))));

        Deno.serve(() => new Response(seen.join(" | ")));
        "#
        .replace("{port}", &port.to_string()),
    );
    assert_eq!(
        said,
        "in1.zone.test,in2.zone.test | ENOTFOUND queryMx nowhere.test"
    );
}

/// The other question, which is not a query at all: the host's own
/// resolution, which is what makes `localhost` mean what it means.
#[test]
fn a_lookup_is_the_hosts_own_resolution_and_an_address_is_itself() {
    let said = answered(
        r#"
        import { lookup } from "node:dns/promises";
        import dns from "node:dns";
        import { isIP } from "node:net";
        const seen = [];

        const here = await lookup("localhost");
        seen.push(`${isIP(here.address) === here.family} ${here.family === 4 || here.family === 6}`);

        // An address handed in where a name goes is itself, which is
        // what a package configured with one relies on.
        seen.push(JSON.stringify(await lookup("192.0.2.7")));
        seen.push(JSON.stringify(await lookup("::1", { all: true })));

        seen.push(await new Promise((done) =>
          dns.lookup("localhost", (why, address, family) =>
            done(why ? `failed ${why.code}` : `${isIP(address) === family}`))));

        // A name that is not going to resolve anywhere. Which failure
        // it is depends on what the box running this has for a
        // resolver, so what is checked is that it failed the way a
        // package catches rather than which of them it was.
        try {
          await lookup("this.name.is.not.in.any.zone.invalid");
          seen.push("resolved");
        } catch (why) {
          seen.push(`${typeof why.code === "string"} ${why.syscall}`);
        }

        Deno.serve(() => new Response(seen.join(" | ")));
        "#,
    );
    assert_eq!(
        said,
        concat!(
            "true true",
            r#" | {"address":"192.0.2.7","family":4}"#,
            r#" | [{"address":"::1","family":6}]"#,
            " | true",
            " | true getaddrinfo",
        )
    );
}

/// What this does not do, said by name where it was asked for rather
/// than answered with something invented.
#[test]
fn what_the_resolver_here_will_not_answer_says_so() {
    let said = answered(
        r#"
        import dns from "node:dns/promises";
        const seen = [];
        const why = async (what) => {
          try {
            await what();
            return "answered";
          } catch (why) {
            return why.message;
          }
        };

        seen.push((await why(() => dns.lookupService("127.0.0.1", 22))).includes("does not have"));
        seen.push((await why(() => dns.resolveNaptr("zone.test"))).includes("NAPTR is not one of them"));
        seen.push((await why(() => dns.resolveAny("zone.test"))).includes("refused by most resolvers"));
        seen.push((await why(() => dns.resolve4("zone.test", { ttl: true }))).includes("how long a record lives"));
        seen.push((await why(() => dns.resolve("zone.test", "HINFO"))).includes("not a record type"));
        seen.push((await why(() => dns.reverse("not an address"))).includes("EINVAL"));

        Deno.serve(() => new Response(seen.join(" ")));
        "#,
    );
    assert_eq!(said, "true true true true true true");
}
