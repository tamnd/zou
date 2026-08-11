//! The Python binding over [`zou_embed`].
//!
//! Everything here that can take time releases the GIL while it takes
//! it, because opening a project starts a postmaster and answering a
//! request waits on postgres, and a python process where no other thread
//! can run for the length of a query is not a python process anybody
//! wants.
//!
//! The python side of this is `python/zou/__init__.py`, which is what
//! turns a request into an httpx transport supabase-py can be built on.
//! This layer only moves bytes.

#![cfg(unix)]
#![deny(clippy::all)]

use std::path::PathBuf;
use std::sync::Arc;

use pyo3::create_exception;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

create_exception!(
    _zou,
    ZouError,
    PyRuntimeError,
    "Anything zou refused to do, with the kind on `.code`."
);

/// What went wrong, with the kind kept as a code on the exception so
/// python can branch on `err.code` rather than on the shape of a
/// sentence.
fn failed(e: zou_embed::Error) -> PyErr {
    let code = match e.kind {
        zou_embed::Kind::Options => "ZOU_OPTIONS",
        zou_embed::Kind::Postgres => "ZOU_POSTGRES",
        zou_embed::Kind::Store => "ZOU_STORE",
        zou_embed::Kind::Request => "ZOU_REQUEST",
        zou_embed::Kind::Io => "ZOU_IO",
    };
    let err = ZouError::new_err(format!("{code}: {}", e.message));
    Python::attach(|py| {
        let value = err.value(py);
        let _ = value.setattr("code", code);
        let _ = value.setattr("message", e.message.as_str());
    });
    err
}

/// What the router answered, as plain data.
#[pyclass(module = "zou", name = "Response", frozen)]
pub struct Response {
    #[pyo3(get)]
    status: u16,
    #[pyo3(get)]
    headers: Vec<(String, String)>,
    #[pyo3(get)]
    body: Py<PyBytes>,
}

#[pymethods]
impl Response {
    fn __repr__(&self) -> String {
        format!("<zou.Response {}>", self.status)
    }
}

/// One open project.
///
/// The handle is an `Arc` so a request in flight on one thread does not
/// stop `close` from being called on another, which is the same reason
/// the node binding holds one.
#[pyclass(module = "zou", name = "Zou", frozen)]
pub struct Zou {
    inner: Arc<zou_embed::Zou>,
}

/// Open a project, taking the options the python layer assembled.
///
/// An empty target is ephemeral: a store of this handle's own that goes
/// away when it closes, which is the one a test suite wants.
#[pyfunction]
#[pyo3(signature = (
    target = String::new(),
    tenant = None,
    pg_bin = None,
    runtime = None,
    jwt_secret = None,
    schemas = None,
    shared_buffers = None,
    fixture = false,
))]
#[allow(clippy::too_many_arguments)]
fn open(
    py: Python<'_>,
    target: String,
    tenant: Option<String>,
    pg_bin: Option<String>,
    runtime: Option<String>,
    jwt_secret: Option<String>,
    schemas: Option<Vec<String>>,
    shared_buffers: Option<String>,
    fixture: bool,
) -> PyResult<Zou> {
    let mut options = zou_embed::Options::ephemeral();
    options.target = target;
    if let Some(tenant) = tenant {
        options.tenant = tenant;
    }
    if let Some(pg_bin) = pg_bin {
        options.pg_bin = PathBuf::from(pg_bin);
    }
    options.runtime = runtime.map(PathBuf::from);
    options.jwt_secret = jwt_secret;
    options.schemas = schemas.unwrap_or_default();
    options.shared_buffers = shared_buffers;
    options.fixture = fixture;

    let opened = py
        .detach(|| zou_embed::Zou::open(options))
        .map_err(failed)?;
    Ok(Zou {
        inner: Arc::new(opened),
    })
}

#[pymethods]
impl Zou {
    /// Answer one request, in this process. No socket, no port.
    #[pyo3(signature = (method, path, headers = None, body = None))]
    fn request(
        &self,
        py: Python<'_>,
        method: &str,
        path: &str,
        headers: Option<Vec<(String, String)>>,
        body: Option<&[u8]>,
    ) -> PyResult<Response> {
        let headers = headers.unwrap_or_default();
        let body = body.unwrap_or(b"").to_vec();
        let answered = py
            .detach(|| {
                let borrowed: Vec<(&str, &str)> = headers
                    .iter()
                    .map(|(name, value)| (name.as_str(), value.as_str()))
                    .collect();
                self.inner.request(method, path, &borrowed, &body)
            })
            .map_err(failed)?;
        Ok(Response {
            status: answered.status,
            headers: answered.headers,
            body: PyBytes::new(py, &answered.body).unbind(),
        })
    }

    /// Put the same front door on a port too, and say which one. Port 0
    /// asks the kernel.
    #[pyo3(signature = (port = 0))]
    fn listen(&self, py: Python<'_>, port: u16) -> PyResult<u16> {
        py.detach(|| self.inner.listen(port)).map_err(failed)
    }

    /// A copy on write branch, open and ready, as a second handle to
    /// close like the first.
    fn branch(&self, py: Python<'_>, name: &str) -> PyResult<Zou> {
        let child = py.detach(|| self.inner.branch(name)).map_err(failed)?;
        Ok(Zou {
            inner: Arc::new(child),
        })
    }

    /// Whether a branch of this database would serve yet.
    fn branchable(&self, py: Python<'_>) -> PyResult<bool> {
        py.detach(|| self.inner.branchable()).map_err(failed)
    }

    /// Push everything committed so far into the store.
    fn checkpoint(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.inner.checkpoint()).map_err(failed)
    }

    /// Stop postgres and remove the running copy. Calling it twice is
    /// fine, which matters in a language where a finaliser may get there
    /// first.
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.inner.shutdown()).map_err(failed)
    }

    #[getter]
    fn anon_key(&self) -> String {
        self.inner.keys().anon.clone()
    }

    #[getter]
    fn service_role_key(&self) -> String {
        self.inner.keys().service_role.clone()
    }

    #[getter]
    fn dsn(&self) -> String {
        self.inner.dsn().to_string()
    }

    #[getter]
    fn target(&self) -> String {
        self.inner.target().to_string()
    }

    #[getter]
    fn tenant(&self) -> String {
        self.inner.tenant().to_string()
    }
}

#[pymodule]
fn _zou(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("__doc__", "The native half of the zou python package.")?;
    // Named for where a caller imports it from rather than where it is
    // defined, so a traceback says `zou.ZouError`.
    let failure = module.py().get_type::<ZouError>();
    failure.setattr("__module__", "zou")?;
    module.add("ZouError", failure)?;
    module.add_class::<Zou>()?;
    module.add_class::<Response>()?;
    module.add_function(wrap_pyfunction!(open, module)?)?;
    Ok(())
}
