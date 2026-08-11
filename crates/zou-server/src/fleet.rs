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
    pub http: std::net::TcpListener,
    /// The postgres port, session mode.
    pub pg: Option<std::net::TcpListener>,
    /// The pooler, transaction mode.
    pub pool: Option<std::net::TcpListener>,
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
            let door = Arc::new(Wire::new(
                Arc::clone(&self.registry),
                Arc::clone(&self.attached),
            ));
            tokio::spawn(async move {
                if let Err(e) = door.serve(convert(listener)?).await {
                    log::error!("postgres port: {e}");
                }
                Ok::<(), String>(())
            });
        }
        if let Some(listener) = self.pool {
            let door = Arc::new(
                Wire::new(Arc::clone(&self.registry), Arc::clone(&self.attached)).pooling(),
            );
            tokio::spawn(async move {
                if let Err(e) = door.serve(convert(listener)?).await {
                    log::error!("pooler: {e}");
                }
                Ok::<(), String>(())
            });
        }
        if let Some(listener) = self.ops {
            tokio::spawn(async move {
                let served = axum::serve(
                    convert(listener)?,
                    crate::ops::ops(version).into_make_service(),
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
            None => crate::gateway::gateway(self.routing, self.registry, self.attached),
        };
        axum::serve(
            convert(self.http)?,
            front.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .map_err(|e| format!("http door: {e}"))
    }
}

/// A listener the caller bound, on this runtime.
fn convert(listener: std::net::TcpListener) -> Result<tokio::net::TcpListener, String> {
    listener
        .set_nonblocking(true)
        .map_err(|e| format!("nonblocking: {e}"))?;
    tokio::net::TcpListener::from_std(listener).map_err(|e| format!("listener: {e}"))
}
