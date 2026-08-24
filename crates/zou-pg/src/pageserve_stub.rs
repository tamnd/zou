//! The page service without unix sockets: a stub that keeps the
//! crate compiling where the real thing cannot run.
//!
//! GetPage rides a unix domain socket and the service worker only
//! exists in the unix build of the postgres patches, so the real
//! module is unix only. Nothing here should ever execute: without
//! the worker no socket exists, `spawn` refuses outright, and a
//! client someone conjures by exporting `ZOU_PAGESERVE` anyway
//! errors on every read instead of pretending to serve.

use std::path::PathBuf;
use std::sync::Arc;

use zou_store::CasStore;
use zou_store::layout::TenantLayout;

use crate::redo::RedoPoolConfig;

/// See [`crate::pageserve::PageClient`] in the unix build.
pub struct PageClient;

impl PageClient {
    pub fn new(_path: PathBuf) -> Self {
        PageClient
    }

    pub fn get_pages(
        &self,
        _spc: u32,
        _db: u32,
        _rel: u32,
        _fork: u32,
        _blks: &[u32],
        _lsn: u64,
    ) -> Result<Vec<Vec<u8>>, String> {
        Err("the page service needs unix sockets".to_string())
    }

    pub fn get_size(
        &self,
        _spc: u32,
        _db: u32,
        _rel: u32,
        _fork: u32,
        _lsn: u64,
    ) -> Result<u32, String> {
        Err("the page service needs unix sockets".to_string())
    }
}

/// Same shape as the unix build so the start path type checks.
pub struct ServerConfig {
    pub store: Arc<dyn CasStore>,
    pub layout: TenantLayout,
    pub tenant: u128,
    pub socket: PathBuf,
    pub data_checksums: bool,
    pub redo: Option<RedoPoolConfig>,
}

pub struct PageServer;

impl PageServer {
    pub fn stop(&mut self) {}
}

pub fn spawn(_cfg: ServerConfig) -> std::io::Result<PageServer> {
    Err(std::io::Error::other("the page service needs unix sockets"))
}
