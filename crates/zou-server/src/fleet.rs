//! Every door of a multi tenant node, on one runtime.
//!
//! A node that serves a thousand projects has four listeners and one
//! attached set behind all of them: the http front door, the postgres
//! port, the pooler, and the scrape. They share the registry and the
//! attach manager because they are the same node, and a project brought
//! up by whichever door was asked first is the project the others find
//! already up.
//!
//! One runtime rather than one each. The threads are the reason: four
//! runtimes on an eight core node is four thread pools competing for
//! eight cores, and the density this is built for is a number of
//! tenants, not a number of servers.
//!
//! The listeners are bound by the caller, while a failure to bind is
//! still a sentence a command can print and exit on, and handed here
//! already open.

use std::sync::Arc;
use std::time::Duration;

use crate::attach::Attached;
use crate::tenant::{Registry, Routing};
use crate::wire::Wire;

/// The doors, and what is behind them.
pub struct Doors {
    /// The one project this node serves, if it serves one. Set, and the
    /// routing is not consulted at all and the project is brought up
    /// before the http door starts accepting, so the first request pays
    /// for nothing it did not ask for.
    pub only: Option<String>,
    /// How a request names its project. Both ways off is a node that
    /// serves nothing, since every request would resolve to no tenant.
    pub routing: Routing,
    pub registry: Arc<Registry>,
    pub attached: Arc<Attached>,
    /// What this node does with a project another node is writing.
    ///
    /// None is a node that believes every project on the store is its
    /// own, which is true of one node and is what every deployment was
    /// until a node could be told its own name and address. Set, and a
    /// request for somebody else's project is forwarded to them and a
    /// socket for it is served here on a link to them.
    pub forwarding: Option<Arc<crate::forward::Forwarding>>,
    /// The http front door, on one port or on several. Several is the
    /// same api answering on every one of them, which a node published
    /// at more than one port needs, and which a load generator needs for
    /// a different reason: one client address has about 64k ports to one
    /// destination port, so a run holding more sockets than that has to
    /// spread them over more than one port on this side.
    pub http: Vec<std::net::TcpListener>,
    /// The postgres port, session mode.
    pub pg: Option<std::net::TcpListener>,
    /// The pooler, transaction mode.
    pub pool: Option<std::net::TcpListener>,
    /// The certificate both of those answer an `SSLRequest` with. One
    /// for the two of them, because they are the same credential
    /// crossing the same network, and a node encrypting one of its two
    /// postgres ports would be a node that encrypts neither. None is
    /// the plaintext pair, which declines the request and belongs on a
    /// private network.
    pub pg_tls: Option<tokio_rustls::TlsAcceptor>,
    pub ops: Option<std::net::TcpListener>,
    /// How often to let go of tenants nobody has asked for. A node
    /// that has gone quiet is the one that should be dropping leases
    /// and is also the one with no requests to notice on, which is why
    /// this is a timer and not a check on the request path.
    pub sweep: Duration,
}

impl Doors {
    /// Serve until the http door stops, which it does not do on its
    /// own. The other three are tasks, and one of them failing takes
    /// its door down rather than the node: a scrape port that could
    /// not be served is not a reason to stop answering queries.
    pub fn serve_blocking(self, version: &'static str) -> Result<(), String> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("tokio runtime: {e}"))?;
        rt.block_on(self.serve(version))
    }

    async fn serve(self, version: &'static str) -> Result<(), String> {
        if let Some(listener) = self.pg {
            let wire = Wire::new(Arc::clone(&self.registry), Arc::clone(&self.attached));
            let door = Arc::new(match self.pg_tls.clone() {
                Some(tls) => wire.secured(tls),
                None => wire,
            });
            tokio::spawn(async move {
                if let Err(e) = door.serve(convert(listener)?).await {
                    log::error!("postgres port: {e}");
                }
                Ok::<(), String>(())
            });
        }
        if let Some(listener) = self.pool {
            let wire = Wire::new(Arc::clone(&self.registry), Arc::clone(&self.attached)).pooling();
            let door = Arc::new(match self.pg_tls.clone() {
                Some(tls) => wire.secured(tls),
                None => wire,
            });
            tokio::spawn(async move {
                if let Err(e) = door.serve(convert(listener)?).await {
                    log::error!("pooler: {e}");
                }
                Ok::<(), String>(())
            });
        }
        if let Some(listener) = self.ops {
            // The same set the front door serves out of, so the tenants
            // page lists what is actually up rather than a copy of it.
            let ops_set = Arc::clone(&self.attached);
            tokio::spawn(async move {
                let served = axum::serve(
                    convert(listener)?,
                    crate::ops::ops(version, Some(ops_set)).into_make_service(),
                )
                .await;
                if let Err(e) = served {
                    log::error!("ops port: {e}");
                }
                Ok::<(), String>(())
            });
        }
        let attached = Arc::clone(&self.attached);
        let every = self.sweep;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            // The first tick is immediate, and sweeping a set that has
            // nothing in it yet is the one call that is certainly
            // pointless.
            tick.tick().await;
            loop {
                tick.tick().await;
                attached.sweep().await;
            }
        });

        let ports = self.http;
        let front = match self.only {
            // The listener is already bound, so a request that arrives
            // during this waits in the accept queue rather than being
            // refused, and the wait is the attach it would have paid
            // for itself.
            Some(only) => {
                let started = std::time::Instant::now();
                crate::gateway::preattach(&only, &self.registry, &self.attached).await?;
                log::info!(
                    "{only} is up before the first request, in {:.1} ms",
                    started.elapsed().as_secs_f64() * 1000.0
                );
                crate::gateway::only(only, self.registry, self.attached)
            }
            None => {
                crate::gateway::fleet(self.routing, self.registry, self.attached, self.forwarding)
            }
        };
        serve_http(ports, front).await
    }
}

/// The http api on every port it was given.
///
/// The last one is served on this task, so the caller's future only
/// returns when the front door itself stops, and the others are tasks
/// like the rest of the doors are. One api behind all of them: a request
/// cannot tell which port it arrived on and nothing about it should.
async fn serve_http(
    mut ports: Vec<std::net::TcpListener>,
    api: axum::Router,
) -> Result<(), String> {
    let front_door = ports.pop().ok_or("the http door needs a port")?;
    for extra in ports {
        let api = api.clone();
        tokio::spawn(async move {
            let served = axum::serve(
                convert(extra)?,
                api.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .await;
            if let Err(e) = served {
                log::error!("http door: {e}");
            }
            Ok::<(), String>(())
        });
    }
    axum::serve(
        convert(front_door)?,
        api.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await
    .map_err(|e| format!("http door: {e}"))
}

/// A listener the caller bound, on this runtime.
fn convert(listener: std::net::TcpListener) -> Result<tokio::net::TcpListener, String> {
    listener
        .set_nonblocking(true)
        .map_err(|e| format!("nonblocking: {e}"))?;
    tokio::net::TcpListener::from_std(listener).map_err(|e| format!("listener: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn status(at: std::net::SocketAddr) -> String {
        let mut c = tokio::net::TcpStream::connect(at).await.expect("connect");
        c.write_all(b"GET /where HTTP/1.1\r\nHost: node\r\nConnection: close\r\n\r\n")
            .await
            .expect("request");
        let mut said = String::new();
        c.read_to_string(&mut said).await.expect("response");
        said
    }

    /// A run holding more sockets than one client address has ports to
    /// one port needs the api on several, so all of them have to answer
    /// and answer the same.
    #[tokio::test]
    async fn every_port_the_door_was_given_answers_the_same_api() {
        let api = axum::Router::new().route("/where", axum::routing::get(|| async { "here" }));
        let mut ports = Vec::new();
        let mut at = Vec::new();
        for _ in 0..3 {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a port");
            at.push(listener.local_addr().expect("the port"));
            ports.push(listener);
        }
        tokio::spawn(serve_http(ports, api));
        for one in at {
            let said = status(one).await;
            assert!(said.contains("200 OK"), "{one}: {said}");
            assert!(said.ends_with("here"), "{one}: {said}");
        }
    }

    /// A node with no port to serve on is a mistake worth a sentence
    /// rather than a process that sits there serving nothing.
    #[tokio::test]
    async fn a_door_with_no_port_is_refused() {
        let api = axum::Router::new();
        assert!(serve_http(Vec::new(), api).await.is_err());
    }
}
