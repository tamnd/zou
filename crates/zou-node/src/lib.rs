//! The Node binding over [`zou_embed`].
//!
//! Everything here that can take time is a task on the thread pool
//! rather than work on the thread node runs javascript on, because
//! opening a project starts a postmaster and answering a request waits
//! on postgres, and a node process that stops answering for the length
//! of a query is not a node process anybody wants.
//!
//! The javascript side of this is `index.js`, which is what turns a
//! request into a `fetch` supabase-js can be built on. This layer only
//! moves bytes.

#![cfg(unix)]
#![deny(clippy::all)]

use std::path::PathBuf;
use std::sync::Arc;

use napi::bindgen_prelude::{AsyncTask, Buffer};
use napi::{Env, Error, Result, Status, Task};
use napi_derive::napi;

/// What went wrong, with the kind kept as the code so javascript can
/// branch on `err.code` rather than on the shape of a sentence.
fn failed(e: zou_embed::Error) -> Error {
    let code = match e.kind {
        zou_embed::Kind::Options => "ZOU_OPTIONS",
        zou_embed::Kind::Postgres => "ZOU_POSTGRES",
        zou_embed::Kind::Store => "ZOU_STORE",
        zou_embed::Kind::Request => "ZOU_REQUEST",
        zou_embed::Kind::Io => "ZOU_IO",
    };
    Error::new(Status::GenericFailure, format!("{code}: {}", e.message))
}

/// How a project is opened, as one object with everything optional.
///
/// `target` empty is ephemeral: a store of this handle's own that goes
/// away when it closes, which is the one a test suite wants.
#[napi(object)]
pub struct ZouOptions {
    pub target: Option<String>,
    pub tenant: Option<String>,
    pub pg_bin: Option<String>,
    pub runtime: Option<String>,
    pub jwt_secret: Option<String>,
    pub schemas: Option<Vec<String>>,
    /// The role a request with no key of its own runs as, `anon`
    /// unless a suite recorded against something else says otherwise.
    pub anon_role: Option<String>,
    pub shared_buffers: Option<String>,
    /// Cut this database out of the machine's template instead of
    /// making one, which is milliseconds rather than seconds and is
    /// what a fixture per test needs.
    pub fixture: Option<bool>,
    /// The key an S3 client signs with, with its secret below. Without
    /// the pair the S3 protocol surface tells every request the key it
    /// named is not one this project has.
    pub s3_access_key: Option<String>,
    pub s3_secret_key: Option<String>,
    /// Where the project says it is, which a signature is checked
    /// against. `us-east-1` when it is not given.
    pub s3_region: Option<String>,
}

impl From<ZouOptions> for zou_embed::Options {
    fn from(options: ZouOptions) -> zou_embed::Options {
        let mut out = zou_embed::Options::ephemeral();
        if let Some(target) = options.target {
            out.target = target;
        }
        if let Some(tenant) = options.tenant {
            out.tenant = tenant;
        }
        if let Some(pg_bin) = options.pg_bin {
            out.pg_bin = PathBuf::from(pg_bin);
        }
        out.runtime = options.runtime.map(PathBuf::from);
        out.jwt_secret = options.jwt_secret;
        out.schemas = options.schemas.unwrap_or_default();
        out.anon_role = options.anon_role.unwrap_or_default();
        out.shared_buffers = options.shared_buffers;
        out.fixture = options.fixture.unwrap_or(false);
        // Half a pair is not a pair, and a project given one half has
        // said something it meant, so it is left with no credentials
        // rather than a key signed against an empty secret.
        if let (Some(access), Some(secret)) = (options.s3_access_key, options.s3_secret_key) {
            out.s3 = Some(zou_embed::S3Keys {
                access,
                secret,
                region: options.s3_region.unwrap_or_default(),
            });
        }
        out
    }
}

/// One header, on the way in or on the way out.
///
/// A list of pairs rather than an object, because a header may be there
/// more than once and an object quietly keeps the last one.
#[napi(object)]
pub struct ZouHeader {
    pub name: String,
    pub value: String,
}

/// What the router answered, as plain data.
#[napi(object)]
pub struct ZouResponse {
    pub status: u16,
    pub headers: Vec<ZouHeader>,
    pub body: Buffer,
}

/// One open project.
///
/// The handle is an `Arc` because every task takes a copy of it to the
/// thread pool, and because `close` has to be callable while a request
/// somebody else asked for is still in flight.
#[napi]
pub struct Zou {
    inner: Arc<zou_embed::Zou>,
}

pub struct Open {
    options: zou_embed::Options,
}

impl Task for Open {
    type Output = zou_embed::Zou;
    type JsValue = Zou;

    fn compute(&mut self) -> Result<Self::Output> {
        zou_embed::Zou::open(self.options.clone()).map_err(failed)
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
        Ok(Zou {
            inner: Arc::new(output),
        })
    }
}

/// Open a project. Seconds rather than milliseconds, and on a store
/// with no database in it, initdb first.
#[napi(ts_return_type = "Promise<Zou>")]
pub fn open(options: Option<ZouOptions>) -> AsyncTask<Open> {
    let options = options
        .map(zou_embed::Options::from)
        .unwrap_or_else(zou_embed::Options::ephemeral);
    AsyncTask::new(Open { options })
}

pub struct Request {
    zou: Arc<zou_embed::Zou>,
    method: String,
    path: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Task for Request {
    type Output = zou_embed::Response;
    type JsValue = ZouResponse;

    fn compute(&mut self) -> Result<Self::Output> {
        let headers: Vec<(&str, &str)> = self
            .headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect();
        self.zou
            .request(&self.method, &self.path, &headers, &self.body)
            .map_err(failed)
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
        Ok(ZouResponse {
            status: output.status,
            headers: output
                .headers
                .into_iter()
                .map(|(name, value)| ZouHeader { name, value })
                .collect(),
            body: output.body.into(),
        })
    }
}

pub struct Listen {
    zou: Arc<zou_embed::Zou>,
    port: u16,
}

impl Task for Listen {
    type Output = u16;
    type JsValue = u16;

    fn compute(&mut self) -> Result<Self::Output> {
        self.zou.listen(self.port).map_err(failed)
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
        Ok(output)
    }
}

pub struct Branch {
    zou: Arc<zou_embed::Zou>,
    name: String,
}

impl Task for Branch {
    type Output = zou_embed::Zou;
    type JsValue = Zou;

    fn compute(&mut self) -> Result<Self::Output> {
        self.zou.branch(&self.name).map_err(failed)
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
        Ok(Zou {
            inner: Arc::new(output),
        })
    }
}

/// A task with nothing to hand back, for the three calls that are only
/// worth waiting on because they take a moment.
///
/// The task types are public because they appear in the signatures the
/// macro generates, and are of no use to anybody: what javascript sees
/// is the promise.
pub struct Plain {
    zou: Arc<zou_embed::Zou>,
    what: Which,
}

pub enum Which {
    Checkpoint,
    Branchable,
    Close,
}

impl Task for Plain {
    type Output = bool;
    type JsValue = bool;

    fn compute(&mut self) -> Result<Self::Output> {
        match self.what {
            Which::Checkpoint => self.zou.checkpoint().map(|()| true).map_err(failed),
            Which::Branchable => self.zou.branchable().map_err(failed),
            Which::Close => self.zou.shutdown().map(|()| true).map_err(failed),
        }
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
        Ok(output)
    }
}

#[napi]
impl Zou {
    /// Answer one request, in this process. No socket, no port.
    #[napi(ts_return_type = "Promise<ZouResponse>")]
    pub fn request(
        &self,
        method: String,
        path: String,
        headers: Option<Vec<ZouHeader>>,
        body: Option<Buffer>,
    ) -> AsyncTask<Request> {
        AsyncTask::new(Request {
            zou: Arc::clone(&self.inner),
            method,
            path,
            headers: headers
                .unwrap_or_default()
                .into_iter()
                .map(|h| (h.name, h.value))
                .collect(),
            body: body.map(|b| b.to_vec()).unwrap_or_default(),
        })
    }

    /// Put the same front door on a port too, and say which one. Port 0
    /// asks the kernel.
    #[napi(ts_return_type = "Promise<number>")]
    pub fn listen(&self, port: Option<u16>) -> AsyncTask<Listen> {
        AsyncTask::new(Listen {
            zou: Arc::clone(&self.inner),
            port: port.unwrap_or(0),
        })
    }

    /// A copy on write branch, open and ready, as a second handle to
    /// close like the first.
    #[napi(ts_return_type = "Promise<Zou>")]
    pub fn branch(&self, name: String) -> AsyncTask<Branch> {
        AsyncTask::new(Branch {
            zou: Arc::clone(&self.inner),
            name,
        })
    }

    /// Whether a branch of this database would serve yet.
    #[napi(ts_return_type = "Promise<boolean>")]
    pub fn branchable(&self) -> AsyncTask<Plain> {
        AsyncTask::new(Plain {
            zou: Arc::clone(&self.inner),
            what: Which::Branchable,
        })
    }

    /// Push everything committed so far into the store.
    #[napi(ts_return_type = "Promise<boolean>")]
    pub fn checkpoint(&self) -> AsyncTask<Plain> {
        AsyncTask::new(Plain {
            zou: Arc::clone(&self.inner),
            what: Which::Checkpoint,
        })
    }

    /// Stop postgres and remove the running copy. Calling it twice is
    /// fine, which matters in a language where something else may get
    /// there first.
    #[napi(ts_return_type = "Promise<boolean>")]
    pub fn close(&self) -> AsyncTask<Plain> {
        AsyncTask::new(Plain {
            zou: Arc::clone(&self.inner),
            what: Which::Close,
        })
    }

    #[napi(getter)]
    pub fn anon_key(&self) -> String {
        self.inner.keys().anon.clone()
    }

    #[napi(getter)]
    pub fn service_role_key(&self) -> String {
        self.inner.keys().service_role.clone()
    }

    #[napi(getter)]
    pub fn dsn(&self) -> String {
        self.inner.dsn().to_string()
    }

    #[napi(getter)]
    pub fn target(&self) -> String {
        self.inner.target().to_string()
    }

    #[napi(getter)]
    pub fn tenant(&self) -> String {
        self.inner.tenant().to_string()
    }
}
