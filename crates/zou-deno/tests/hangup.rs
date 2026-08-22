//! What the other end of an aborted `fetch` sees.
//!
//! A signal that fires while a call is out is easy to observe from
//! inside a function: the promise rejects with an `AbortError` and the
//! handler carries on. What it does to the connection cannot be seen
//! from in there at all, and that is the half these tests are for. The
//! server says whether the client went away or whether it stayed on the
//! line while nobody was listening.
//!
//! The interesting case is the second one, because the client keeps
//! connections between calls: a call made on a socket that was already
//! open never runs a connector, so a runtime that only knew about
//! sockets it had just opened would end the first call's connection and
//! not the second's.

#![cfg(feature = "isolate")]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::mpsc;
use std::time::Duration;

use zou_deno::Isolate;
use zou_functions::{Answer, Call, Function, Runtime};

/// How long the server waits to be left before it says it was not.
const PATIENCE: Duration = Duration::from_secs(5);

fn answered(source: &str) -> Answer {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let entrypoint = dir.path().join("index.ts");
    std::fs::write(&entrypoint, source).expect("the function's file");
    let function = Function::new("hello", entrypoint);
    Isolate::new()
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
        .expect("an answer")
}

fn said(answer: &Answer) -> serde_json::Value {
    serde_json::from_slice(answer.bytes()).expect("json")
}

/// A server that reports what happened to each connection rather than
/// what was asked of it.
///
/// `/quick` is answered and the connection kept, which is what puts it
/// in the client's pool. `/wait` is never answered: the server sits on
/// the socket until the client leaves or until it runs out of patience,
/// and says which.
fn watching() -> (u16, mpsc::Receiver<String>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("a port");
    let port = listener.local_addr().expect("an address").port();
    let (tell, told) = mpsc::channel();
    std::thread::spawn(move || {
        for (nth, stream) in listener.incoming().enumerate() {
            let Ok(stream) = stream else { continue };
            let tell = tell.clone();
            std::thread::spawn(move || {
                let mut reader =
                    BufReader::new(stream.try_clone().expect("a second handle on the socket"));
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).is_err() || line.trim().is_empty() {
                        return;
                    }
                    let path = line.split_whitespace().nth(1).unwrap_or("/").to_string();
                    loop {
                        let mut header = String::new();
                        if reader.read_line(&mut header).is_err() {
                            return;
                        }
                        if header.trim_end().is_empty() {
                            break;
                        }
                    }
                    if tell.send(format!("{nth} asked {path}")).is_err() {
                        return;
                    }
                    if path == "/wait" {
                        let _ = stream.set_read_timeout(Some(PATIENCE));
                        let mut byte = [0u8; 1];
                        let what = match reader.read(&mut byte) {
                            Ok(0) => "gone",
                            Ok(_) => "noisy",
                            Err(_) => "held",
                        };
                        let _ = tell.send(format!("{nth} {what}"));
                        return;
                    }
                    let mut out = &stream;
                    let head = "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: 5\r\n\r\nquick";
                    if out.write_all(head.as_bytes()).is_err() {
                        return;
                    }
                    let _ = out.flush();
                }
            });
        }
    });
    (port, told)
}

fn next(told: &mpsc::Receiver<String>) -> String {
    told.recv_timeout(PATIENCE + Duration::from_secs(5))
        .expect("the server said something")
}

/// The plain case: one call, one connection, and a signal that fires
/// while the server is still deciding what to answer.
#[test]
fn a_call_that_was_given_up_on_takes_its_connection_with_it() {
    let (port, told) = watching();
    let answer = answered(&format!(
        r#"
        Deno.serve(async () => {{
            try {{
                await fetch("http://127.0.0.1:{port}/wait", {{ signal: AbortSignal.timeout(100) }});
                return Response.json({{ threw: null }});
            }} catch (e) {{
                return Response.json({{ threw: e.name }});
            }}
        }});
        "#
    ));
    assert_eq!(said(&answer)["threw"], "TimeoutError");
    assert_eq!(next(&told), "0 asked /wait");
    assert_eq!(next(&told), "0 gone");
}

/// The case the client's pool makes: the second call is on the first
/// call's socket, which nothing opened for it, and ending it has to end
/// that socket rather than the one this call would have opened.
#[test]
fn a_kept_connection_is_ended_by_the_call_that_was_handed_it() {
    let (port, told) = watching();
    let answer = answered(&format!(
        r#"
        Deno.serve(async () => {{
            const first = await (await fetch("http://127.0.0.1:{port}/quick")).text();
            try {{
                await fetch("http://127.0.0.1:{port}/wait", {{ signal: AbortSignal.timeout(100) }});
                return Response.json({{ first, threw: null }});
            }} catch (e) {{
                return Response.json({{ first, threw: e.name }});
            }}
        }});
        "#
    ));
    let said = said(&answer);
    assert_eq!(said["first"], "quick");
    assert_eq!(said["threw"], "TimeoutError");
    assert_eq!(next(&told), "0 asked /quick");
    // The same connection, which is the whole point of the test: if the
    // client had opened a second one this would say `1`.
    assert_eq!(next(&told), "0 asked /wait");
    assert_eq!(next(&told), "0 gone");
}

/// A call nobody gave up on is left alone, so the connection is still
/// there when the second call wants it.
#[test]
fn a_call_that_answered_leaves_its_connection_where_the_next_one_can_find_it() {
    let (port, told) = watching();
    let answer = answered(&format!(
        r#"
        Deno.serve(async () => {{
            const both = [];
            for (let i = 0; i < 2; i++) {{
                both.push(await (await fetch("http://127.0.0.1:{port}/quick")).text());
            }}
            return Response.json({{ both }});
        }});
        "#
    ));
    assert_eq!(said(&answer)["both"], serde_json::json!(["quick", "quick"]));
    assert_eq!(next(&told), "0 asked /quick");
    assert_eq!(next(&told), "0 asked /quick");
}
