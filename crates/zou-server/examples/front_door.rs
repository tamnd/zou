//! The front door on a port, with no database behind it.
//!
//! `zou dev` is this plus a postmaster, a store and a bootstrap, which
//! is the right thing for every surface that reads rows and the wrong
//! thing when the surface under test touches no rows at all. Realtime
//! broadcast is that surface: it is a socket, a topic and a fan out,
//! and nothing it does needs postgres to be running.
//!
//!   cargo run -p zou-server --example front_door
//!
//! It prints the url and the anon key, and then serves until it is
//! stopped. What connects to it is up to the caller: a browser, a
//! supabase-js client, or the realtime-js check under
//! zou-conformance.
//!
//! Every request that needs the database answers as a server with no
//! database does, which is the honest failure rather than a hang.

use zou_server::{Config, jwt, router};

#[tokio::main]
async fn main() {
    env_logger::init();
    let secret = b"super-secret-jwt-token-with-at-least-32-characters-long".to_vec();
    let port: u16 = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(54321);
    let app = router(Config {
        jwt_secret: secret.clone(),
        ..Config::default()
    })
    .expect("the router builds");
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .expect("the port is free");
    let at = listener.local_addr().expect("the port");
    println!("url  http://{at}");
    println!("anon {}", jwt::mint(&jwt::key_claims("anon"), &secret));
    axum::serve(listener, app).await.expect("serving");
}
