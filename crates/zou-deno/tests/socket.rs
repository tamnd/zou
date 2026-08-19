//! A function with a socket: `Deno.connect`, `Deno.connectTls` and
//! `Deno.startTls`, against a server in a thread beside it.
//!
//! The server is a real listener rather than a mock, because what is
//! being tested is a connection: bytes out of an isolate, through the
//! host's ops, onto a socket somebody else accepted, and back. A stub
//! would only prove the ops call each other.
//!
//! It speaks a line at a time, which is enough to see that a write and
//! a read are the same connection, and it is the shape of every driver
//! that asks for this: write a message, wait to be answered.

#![cfg(feature = "isolate")]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ServerConnection, StreamOwned};
use zou_deno::Isolate;
use zou_functions::{Answer, Call, Function, Runtime};

/// A certificate for `localhost` that runs out in 2126, and its key.
const CERT: &[u8] = include_bytes!("fixtures/tls-localhost.cert.pem");
const KEY: &[u8] = include_bytes!("fixtures/tls-localhost.key.pem");

fn answered(source: &str) -> Answer {
    invoked(source).expect("an answer")
}

fn invoked(source: &str) -> Result<Answer, zou_functions::Failed> {
    let dir = tempfile::tempdir().expect("a temporary directory");
    std::fs::write(dir.path().join("index.ts"), source).expect("the function's file");
    let function = Function::new("hello", dir.path().join("index.ts"));
    Isolate::new().invoke(
        &function,
        Call {
            method: "GET".to_string(),
            url: "http://localhost:9000/functions/v1/hello".to_string(),
            headers: Vec::new(),
            body: Vec::new(),
            execution_id: "one".to_string(),
        },
    )
}

fn body(answer: &Answer) -> String {
    String::from_utf8(answer.bytes().to_vec()).expect("utf-8")
}

/// One line, or nothing at all if the other end has gone.
fn line<S: Read>(io: &mut S) -> Option<String> {
    let mut out = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        match io.read(&mut byte) {
            Ok(0) | Err(_) => return None,
            Ok(_) => {}
        }
        if byte[0] == b'\n' {
            return Some(String::from_utf8_lossy(&out).to_string());
        }
        out.push(byte[0]);
    }
}

/// The whole protocol: a line in, the same line back with `pong` in
/// front of it, and `bye` ends the conversation.
fn talked<S: Read + Write>(mut io: S) {
    while let Some(said) = line(&mut io) {
        let _ = io.write_all(format!("pong {said}\n").as_bytes());
        let _ = io.flush();
        if said == "bye" {
            return;
        }
    }
}

/// A server on a port the kernel picked, in a thread that keeps
/// answering until the test binary ends.
fn talking() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a port");
    let port = listener.local_addr().expect("bound").port();
    thread::spawn(move || {
        while let Ok((stream, _)) = listener.accept() {
            thread::spawn(move || talked(stream));
        }
    });
    port
}

/// The same, with TLS on it from the first byte.
fn talking_tls() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a port");
    let port = listener.local_addr().expect("bound").port();
    let config = Arc::new(serving());
    thread::spawn(move || {
        while let Ok((stream, _)) = listener.accept() {
            let config = Arc::clone(&config);
            thread::spawn(move || talked(secured(stream, config)));
        }
    });
    port
}

/// A server that says one thing and hangs up, which is the end of a
/// stream and not a failure.
fn hanging_up() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a port");
    let port = listener.local_addr().expect("bound").port();
    thread::spawn(move || {
        while let Ok((mut stream, _)) = listener.accept() {
            let _ = stream.write_all(b"and that is all\n");
            let _ = stream.flush();
        }
    });
    port
}

/// A server that accepts and then does nothing, so that every socket a
/// function opened stays open.
fn holding() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a port");
    let port = listener.local_addr().expect("bound").port();
    thread::spawn(move || {
        let mut held = Vec::new();
        while let Ok((stream, _)) = listener.accept() {
            held.push(stream);
        }
    });
    port
}

fn serving() -> rustls::ServerConfig {
    rustls::ServerConfig::builder_with_provider(rustls::crypto::ring::default_provider().into())
        .with_safe_default_protocol_versions()
        .expect("versions")
        .with_no_client_auth()
        .with_single_cert(
            vec![CertificateDer::from_pem_slice(CERT).expect("the certificate")],
            PrivateKeyDer::from_pem_slice(KEY).expect("the key"),
        )
        .expect("the fixture certificate and key belong together")
}

fn secured(stream: TcpStream, config: Arc<rustls::ServerConfig>) -> impl Read + Write {
    let connection = ServerConnection::new(config).expect("a server connection");
    StreamOwned::new(connection, stream)
}

/// A port nothing is listening on, which is a port that was bound and
/// then let go of: asking the kernel for one is the only way to know
/// it was free, and nothing takes it back in the microseconds after.
fn nobody() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a port");
    listener.local_addr().expect("bound").port()
}

/// The whole of it: a connection, a write, a read of what came back,
/// and the two addresses the connection says it has.
#[test]
fn a_function_writes_to_a_socket_and_reads_what_comes_back() {
    let port = talking();
    let answer = answered(&format!(
        r#"
        const conn = await Deno.connect({{ hostname: "127.0.0.1", port: {port} }});
        await conn.write(new TextEncoder().encode("ping\n"));
        const buffer = new Uint8Array(64);
        const read = await conn.read(buffer);
        const said = new TextDecoder().decode(buffer.subarray(0, read)).trim();
        const where = `${{conn.remoteAddr.transport}} ${{conn.remoteAddr.port}} ${{conn.localAddr.hostname}}`;
        conn.close();
        Deno.serve(() => new Response(`${{said}} ${{where}}`));
        "#
    ));
    assert_eq!(body(&answer), format!("pong ping tcp {port} 127.0.0.1"));
}

/// A server that hangs up is the end of the stream, which `read`
/// answers `null` for and which a loop reading until it does relies
/// on.
#[test]
fn the_end_of_a_stream_is_a_null_and_not_a_failure() {
    let port = hanging_up();
    let answer = answered(&format!(
        r#"
        const conn = await Deno.connect({{ port: {port} }});
        const buffer = new Uint8Array(64);
        const first = await conn.read(buffer);
        const said = new TextDecoder().decode(buffer.subarray(0, first)).trim();
        let ended = "no";
        for (let i = 0; i < 8; i++) {{
          if (await conn.read(buffer) === null) {{ ended = "yes"; break; }}
        }}
        conn.close();
        Deno.serve(() => new Response(`${{said}} ${{ended}}`));
        "#
    ));
    assert_eq!(body(&answer), "and that is all yes");
}

/// The two failures a driver branches on, by the names it branches on
/// them with: nobody listening, and a connection that has been closed.
#[test]
fn what_fails_is_a_deno_error_with_the_name_deno_gives_it() {
    let refused = nobody();
    let port = talking();
    let answer = answered(&format!(
        r#"
        let first = "none";
        try {{
          await Deno.connect({{ port: {refused} }});
        }} catch (why) {{
          first = `${{why instanceof Deno.errors.ConnectionRefused}} ${{why.name}}`;
        }}
        const conn = await Deno.connect({{ port: {port} }});
        conn.close();
        let second = "none";
        try {{
          await conn.read(new Uint8Array(8));
        }} catch (why) {{
          second = `${{why instanceof Deno.errors.BadResource}} ${{why.name}}`;
        }}
        Deno.serve(() => new Response(`${{first}} ${{second}}`));
        "#
    ));
    assert_eq!(body(&answer), "true ConnectionRefused true BadResource");
}

/// TLS asked for after the connection is open, which is what postgres
/// does: it asks in the clear whether the server speaks it.
#[test]
fn tls_can_be_put_on_a_connection_that_is_already_open() {
    let port = talking_tls();
    let cert = String::from_utf8_lossy(CERT).to_string();
    let answer = answered(&format!(
        r#"
        const plain = await Deno.connect({{ hostname: "localhost", port: {port} }});
        const conn = await Deno.startTls(plain, {{
          hostname: "localhost",
          caCerts: [{cert:?}],
        }});
        await conn.write(new TextEncoder().encode("over tls\n"));
        const buffer = new Uint8Array(64);
        const read = await conn.read(buffer);
        conn.close();
        const said = new TextDecoder().decode(buffer.subarray(0, read)).trim();
        Deno.serve(() => new Response(said));
        "#
    ));
    assert_eq!(body(&answer), "pong over tls");
}

/// The other spelling, where the handshake happens before the function
/// is given anything to hold.
#[test]
fn a_connection_can_have_tls_on_it_from_the_first_byte() {
    let port = talking_tls();
    let cert = String::from_utf8_lossy(CERT).to_string();
    let answer = answered(&format!(
        r#"
        const conn = await Deno.connectTls({{
          hostname: "localhost",
          port: {port},
          caCerts: [{cert:?}],
        }});
        await conn.write(new TextEncoder().encode("hello\n"));
        const buffer = new Uint8Array(64);
        const read = await conn.read(buffer);
        conn.close();
        Deno.serve(() => new Response(new TextDecoder().decode(buffer.subarray(0, read)).trim()));
        "#
    ));
    assert_eq!(body(&answer), "pong hello");
}

/// A certificate nobody signed for is a handshake that fails, and it
/// fails here rather than being quietly trusted.
#[test]
fn a_server_this_runtime_does_not_trust_is_not_talked_to() {
    let port = talking_tls();
    let answer = answered(&format!(
        r#"
        let why = "none";
        try {{
          await Deno.connectTls({{ hostname: "localhost", port: {port} }});
        }} catch (raised) {{
          why = raised.message;
        }}
        Deno.serve(() => new Response(why));
        "#
    ));
    assert!(
        body(&answer).contains("the TLS handshake with localhost failed"),
        "{}",
        body(&answer)
    );
}

/// The streams a library that pipes rather than reads reaches for,
/// over the same connection and the same protocol.
#[test]
fn the_streams_on_a_connection_carry_the_same_bytes() {
    let port = talking();
    let answer = answered(&format!(
        r#"
        const conn = await Deno.connect({{ port: {port} }});
        const writer = conn.writable.getWriter();
        await writer.write(new TextEncoder().encode("streamed\n"));
        const reader = conn.readable.getReader();
        const {{ value }} = await reader.read();
        conn.close();
        Deno.serve(() => new Response(new TextDecoder().decode(value).trim()));
        "#
    ));
    assert_eq!(body(&answer), "pong streamed");
}

/// The transport this runtime will not open, refused by name and
/// before anything is opened.
#[test]
fn a_unix_socket_is_refused_by_saying_which_transport_it_is() {
    let answer = answered(
        r#"
        let why = "none";
        try {
          await Deno.connect({ transport: "unix", path: "/var/run/postgres/.s.PGSQL.5432" });
        } catch (raised) {
          why = raised.message;
        }
        Deno.serve(() => new Response(why));
        "#,
    );
    assert_eq!(
        body(&answer),
        "a function may only open a tcp connection, and this one asked for unix"
    );
}

/// A function that opens sockets and never closes them is stopped by
/// a number rather than by the box running out of descriptors.
#[test]
fn there_is_a_ceiling_on_how_many_sockets_one_function_holds() {
    let port = holding();
    let answer = answered(&format!(
        r#"
        const held = [];
        let why = "none";
        try {{
          for (let i = 0; i < 300; i++) {{
            held.push(await Deno.connect({{ port: {port} }}));
          }}
        }} catch (raised) {{
          why = raised.message;
        }}
        for (const conn of held) {{ conn.close(); }}
        Deno.serve(() => new Response(`${{held.length}} ${{why}}`));
        "#
    ));
    assert_eq!(
        body(&answer),
        "256 a function may hold 256 sockets open at once, and this one already holds that many"
    );
}
