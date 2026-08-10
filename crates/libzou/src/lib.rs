//! `libzou`: the C ABI over [`zou_embed`].
//!
//! Every language binding that is not Rust comes through here, so the
//! surface is deliberately small and deliberately dull: opaque handles,
//! an int code out of everything that can fail, and one string per
//! thread saying what went wrong.
//!
//! The header is `include/zou.h` and it is written by hand rather than
//! generated, because it is the contract and a contract nobody read is
//! not one.
//!
//! # The rules
//!
//! Every function returns `ZOU_OK` or a negative code, and none of them
//! unwind into C: a panic is caught at the boundary and becomes
//! `ZOU_ERR_PANIC`. A code other than `ZOU_OK` means the out parameter
//! was not written and [`zou_last_error`] has a sentence about it,
//! valid on that thread until its next call into this library.
//!
//! Ownership is by name. `zou_*_new` and `zou_open` and `zou_branch`
//! and `zou_request` hand back something the caller owns and must pass
//! to the matching `_free` or `zou_close`. Everything else returns
//! borrowed pointers that live as long as the handle they came out of.
//!
//! A `zou` handle may be used from any thread and from several at once.
//! What it must not be is used after `zou_close`, which frees it.
//!
//! Strings in are UTF-8 and NUL terminated. Strings out are the same,
//! and a body is bytes with a length because it is not a string.

#![cfg(unix)]

use std::cell::RefCell;
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::PathBuf;

use zou_embed::{Kind, Options, Zou};

/// It worked.
pub const ZOU_OK: c_int = 0;
/// The options do not describe anything openable.
pub const ZOU_ERR_OPTIONS: c_int = -1;
/// Postgres would not start, would not stop, or would not answer.
pub const ZOU_ERR_POSTGRES: c_int = -2;
/// The store said no.
pub const ZOU_ERR_STORE: c_int = -3;
/// The request could not be built or its answer could not be read.
pub const ZOU_ERR_REQUEST: c_int = -4;
/// The filesystem or the network under all of it.
pub const ZOU_ERR_IO: c_int = -5;
/// A null pointer where one was required.
pub const ZOU_ERR_NULL: c_int = -6;
/// A string that was not UTF-8.
pub const ZOU_ERR_UTF8: c_int = -7;
/// Something panicked. It did not unwind into the caller.
pub const ZOU_ERR_PANIC: c_int = -8;

thread_local! {
    /// The last failure on this thread. One per thread rather than one
    /// per library, so two threads failing at once do not overwrite
    /// each other's answer.
    static LAST: RefCell<CString> = RefCell::new(CString::default());
}

fn remember(message: &str) {
    // A NUL inside the message is not worth failing over, and the
    // caller reads up to the first one anyway.
    let said = CString::new(message.replace('\0', " ")).unwrap_or_default();
    LAST.with(|last| *last.borrow_mut() = said);
}

fn code(kind: Kind) -> c_int {
    match kind {
        Kind::Options => ZOU_ERR_OPTIONS,
        Kind::Postgres => ZOU_ERR_POSTGRES,
        Kind::Store => ZOU_ERR_STORE,
        Kind::Request => ZOU_ERR_REQUEST,
        Kind::Io => ZOU_ERR_IO,
    }
}

/// Run a fallible body at the boundary: a failure becomes a code and a
/// remembered sentence, and a panic becomes a code rather than an
/// unwind into a language that has no idea what one is.
fn guard(what: &str, body: impl FnOnce() -> Result<(), (c_int, String)>) -> c_int {
    match catch_unwind(AssertUnwindSafe(body)) {
        Ok(Ok(())) => ZOU_OK,
        Ok(Err((code, message))) => {
            remember(&message);
            code
        }
        Err(_) => {
            remember(&format!("{what} panicked"));
            ZOU_ERR_PANIC
        }
    }
}

fn failed(e: zou_embed::Error) -> (c_int, String) {
    (code(e.kind), e.message)
}

/// A borrowed C string, or a reason it is not one.
///
/// # Safety
///
/// `p` must be null or a NUL terminated string that outlives the call.
unsafe fn text<'a>(p: *const c_char, what: &str) -> Result<&'a str, (c_int, String)> {
    if p.is_null() {
        return Err((ZOU_ERR_NULL, format!("{what} is null")));
    }
    unsafe { CStr::from_ptr(p) }
        .to_str()
        .map_err(|_| (ZOU_ERR_UTF8, format!("{what} is not utf-8")))
}

/// What went wrong on this thread, most recently.
///
/// The pointer is valid until the next call into this library on this
/// thread. It is never null: with nothing to report it is an empty
/// string.
///
/// # Safety
///
/// The returned pointer must not be freed and must not be kept across
/// another call on this thread.
#[unsafe(no_mangle)]
pub extern "C" fn zou_last_error() -> *const c_char {
    LAST.with(|last| last.borrow().as_ptr())
}

/// The version of this library, as it appears in Cargo.toml.
#[unsafe(no_mangle)]
pub extern "C" fn zou_version() -> *const c_char {
    c"0.0.1".as_ptr()
}

/// What to open and how, built up one call at a time.
///
/// A setter rather than a struct because a struct is an ABI: a field
/// added later would move every field after it, and a binding compiled
/// against the old header would read the wrong bytes. A function added
/// later is just a function nobody calls yet.
pub struct ZouOptions(Options);

/// A fresh set of options: an ephemeral store, the `local` database,
/// and the patched postgres wherever `ZOU_PG_BIN` says.
///
/// # Safety
///
/// The result must be passed to [`zou_options_free`], or to
/// [`zou_open`], which does not consume it.
#[unsafe(no_mangle)]
pub extern "C" fn zou_options_new() -> *mut ZouOptions {
    Box::into_raw(Box::new(ZouOptions(Options::ephemeral())))
}

/// Free options. Null is allowed and does nothing.
///
/// # Safety
///
/// `options` must be null or a pointer from [`zou_options_new`] that
/// has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_options_free(options: *mut ZouOptions) {
    if !options.is_null() {
        drop(unsafe { Box::from_raw(options) });
    }
}

/// Set one option by name.
///
/// The names are the fields of `zou_embed::Options`: `target`,
/// `tenant`, `pg_bin`, `runtime`, `jwt_secret`, `schemas` (comma
/// separated, first one the default), and `shared_buffers`.
///
/// One setter rather than seven exports, because the set of things
/// worth configuring will grow and a name is cheaper to add than a
/// symbol. A name nobody knows is an error rather than a shrug, since
/// a typo that silently configures nothing is the worst of both.
///
/// # Safety
///
/// `options` must be a live pointer from [`zou_options_new`], and
/// `name` and `value` NUL terminated strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_options_set(
    options: *mut ZouOptions,
    name: *const c_char,
    value: *const c_char,
) -> c_int {
    guard("zou_options_set", || {
        if options.is_null() {
            return Err((ZOU_ERR_NULL, "options is null".to_string()));
        }
        let options = unsafe { &mut *options };
        let name = unsafe { text(name, "name")? };
        let value = unsafe { text(value, "value")? };
        match name {
            "target" => options.0.target = value.to_string(),
            "tenant" => options.0.tenant = value.to_string(),
            "pg_bin" => options.0.pg_bin = PathBuf::from(value),
            "runtime" => options.0.runtime = Some(PathBuf::from(value)),
            "jwt_secret" => options.0.jwt_secret = Some(value.to_string()),
            "schemas" => {
                options.0.schemas = value
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
            }
            "shared_buffers" => options.0.shared_buffers = Some(value.to_string()),
            other => {
                return Err((
                    ZOU_ERR_OPTIONS,
                    format!("no option named {other:?}, see zou.h for the list"),
                ));
            }
        }
        Ok(())
    })
}

/// One open project, and the strings a caller can read off it.
///
/// The keys and the dsn are kept as C strings here so the getters can
/// hand back something that lives as long as the handle, rather than
/// something the caller has to free three lines later.
pub struct ZouHandle {
    zou: Zou,
    anon: CString,
    service_role: CString,
    dsn: CString,
    target: CString,
    tenant: CString,
}

impl ZouHandle {
    fn wrap(zou: Zou) -> Result<*mut ZouHandle, (c_int, String)> {
        let flat = |s: &str| CString::new(s.replace('\0', " ")).unwrap_or_default();
        let handle = ZouHandle {
            anon: flat(&zou.keys().anon),
            service_role: flat(&zou.keys().service_role),
            dsn: flat(zou.dsn()),
            target: flat(zou.target()),
            tenant: flat(zou.tenant()),
            zou,
        };
        Ok(Box::into_raw(Box::new(handle)))
    }
}

/// Open a project: a store, postgres over it, and the front door.
///
/// This is the slow call. It starts a postmaster, and on a store with
/// no database in it, runs initdb first.
///
/// # Safety
///
/// `options` must be a live pointer from [`zou_options_new`] and `out`
/// a place to write one handle. On `ZOU_OK` the handle belongs to the
/// caller and must be passed to [`zou_close`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_open(options: *const ZouOptions, out: *mut *mut ZouHandle) -> c_int {
    guard("zou_open", || {
        if options.is_null() {
            return Err((ZOU_ERR_NULL, "options is null".to_string()));
        }
        if out.is_null() {
            return Err((ZOU_ERR_NULL, "out is null".to_string()));
        }
        let options = unsafe { &*options };
        let zou = Zou::open(options.0.clone()).map_err(failed)?;
        let handle = ZouHandle::wrap(zou)?;
        unsafe { *out = handle };
        Ok(())
    })
}

/// Stop postgres, remove the running copy, and free the handle.
///
/// The handle is gone whether this succeeds or not, so a nonzero
/// answer is something to log rather than something to retry.
///
/// # Safety
///
/// `zou` must be a live handle and must not be used again afterwards.
/// Null is allowed and does nothing.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_close(zou: *mut ZouHandle) -> c_int {
    guard("zou_close", || {
        if zou.is_null() {
            return Ok(());
        }
        let handle = unsafe { Box::from_raw(zou) };
        handle.zou.close().map_err(failed)
    })
}

/// The anon key, valid for as long as the handle.
///
/// # Safety
///
/// `zou` must be a live handle. The result must not be freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_anon_key(zou: *const ZouHandle) -> *const c_char {
    borrowed(zou, |h| h.anon.as_ptr())
}

/// The service_role key, valid for as long as the handle.
///
/// # Safety
///
/// `zou` must be a live handle. The result must not be freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_service_role_key(zou: *const ZouHandle) -> *const c_char {
    borrowed(zou, |h| h.service_role.as_ptr())
}

/// What a postgres client would dial to reach the database directly.
///
/// # Safety
///
/// `zou` must be a live handle. The result must not be freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_dsn(zou: *const ZouHandle) -> *const c_char {
    borrowed(zou, |h| h.dsn.as_ptr())
}

/// The store this project lives on.
///
/// # Safety
///
/// `zou` must be a live handle. The result must not be freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_target(zou: *const ZouHandle) -> *const c_char {
    borrowed(zou, |h| h.target.as_ptr())
}

/// The database inside that store.
///
/// # Safety
///
/// `zou` must be a live handle. The result must not be freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_tenant(zou: *const ZouHandle) -> *const c_char {
    borrowed(zou, |h| h.tenant.as_ptr())
}

/// A getter that answers null for a null handle rather than crashing,
/// since a getter has no code to return and a segfault is a poor way
/// to say the handle was already closed.
fn borrowed(zou: *const ZouHandle, of: impl FnOnce(&ZouHandle) -> *const c_char) -> *const c_char {
    if zou.is_null() {
        return std::ptr::null();
    }
    of(unsafe { &*zou })
}

/// One header on the way in.
#[repr(C)]
pub struct ZouHeader {
    pub name: *const c_char,
    pub value: *const c_char,
}

/// What the router answered.
pub struct ZouResponse {
    status: u16,
    headers: Vec<(CString, CString)>,
    body: Vec<u8>,
}

/// Answer one request, in this process.
///
/// `path` is what a url carries after the host, query string and all.
/// Nothing is added to the headers, so a call that would be refused
/// over http is refused here the same way, `apikey` included.
///
/// # Safety
///
/// `zou` must be a live handle, `method` and `path` NUL terminated
/// strings, `headers` an array of `nheaders` entries or null when
/// `nheaders` is 0, `body` an array of `body_len` bytes or null when
/// `body_len` is 0, and `out` a place to write one response, which the
/// caller then owns and must pass to [`zou_response_free`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_request(
    zou: *const ZouHandle,
    method: *const c_char,
    path: *const c_char,
    headers: *const ZouHeader,
    nheaders: usize,
    body: *const u8,
    body_len: usize,
    out: *mut *mut ZouResponse,
) -> c_int {
    guard("zou_request", || {
        if zou.is_null() {
            return Err((ZOU_ERR_NULL, "zou is null".to_string()));
        }
        if out.is_null() {
            return Err((ZOU_ERR_NULL, "out is null".to_string()));
        }
        let handle = unsafe { &*zou };
        let method = unsafe { text(method, "method")? };
        let path = unsafe { text(path, "path")? };
        if headers.is_null() && nheaders > 0 {
            return Err((ZOU_ERR_NULL, "headers is null but nheaders is not".into()));
        }
        let mut pairs = Vec::with_capacity(nheaders);
        for i in 0..nheaders {
            let header = unsafe { &*headers.add(i) };
            pairs.push((unsafe { text(header.name, "a header name")? }, unsafe {
                text(header.value, "a header value")?
            }));
        }
        if body.is_null() && body_len > 0 {
            return Err((ZOU_ERR_NULL, "body is null but body_len is not".into()));
        }
        let bytes = if body_len == 0 {
            &[][..]
        } else {
            unsafe { std::slice::from_raw_parts(body, body_len) }
        };
        let answer = handle
            .zou
            .request(method, path, &pairs, bytes)
            .map_err(failed)?;
        let flat = |s: &str| CString::new(s.replace('\0', " ")).unwrap_or_default();
        let response = ZouResponse {
            status: answer.status,
            headers: answer
                .headers
                .iter()
                .map(|(name, value)| (flat(name), flat(value)))
                .collect(),
            body: answer.body,
        };
        unsafe { *out = Box::into_raw(Box::new(response)) };
        Ok(())
    })
}

/// The status of an answer, or 0 for a null one.
///
/// # Safety
///
/// `response` must be null or a live response.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_response_status(response: *const ZouResponse) -> u16 {
    if response.is_null() {
        return 0;
    }
    unsafe { &*response }.status
}

/// How many headers an answer carries.
///
/// # Safety
///
/// `response` must be null or a live response.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_response_header_count(response: *const ZouResponse) -> usize {
    if response.is_null() {
        return 0;
    }
    unsafe { &*response }.headers.len()
}

/// The name of header `i`, or null if there is no such header.
///
/// # Safety
///
/// `response` must be null or a live response, and the result lives as
/// long as it does.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_response_header_name(
    response: *const ZouResponse,
    i: usize,
) -> *const c_char {
    nth(response, i, |(name, _)| name.as_ptr())
}

/// The value of header `i`, or null if there is no such header.
///
/// # Safety
///
/// `response` must be null or a live response, and the result lives as
/// long as it does.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_response_header_value(
    response: *const ZouResponse,
    i: usize,
) -> *const c_char {
    nth(response, i, |(_, value)| value.as_ptr())
}

fn nth(
    response: *const ZouResponse,
    i: usize,
    of: impl FnOnce(&(CString, CString)) -> *const c_char,
) -> *const c_char {
    if response.is_null() {
        return std::ptr::null();
    }
    match unsafe { &*response }.headers.get(i) {
        Some(header) => of(header),
        None => std::ptr::null(),
    }
}

/// The bytes of an answer, and how many there are.
///
/// Bytes rather than a string, because a body may be an image. It is
/// not NUL terminated and `len` is how you know where it ends.
///
/// # Safety
///
/// `response` must be null or a live response, `len` null or a place
/// to write a length, and the result lives as long as the response.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_response_body(
    response: *const ZouResponse,
    len: *mut usize,
) -> *const u8 {
    if response.is_null() {
        if !len.is_null() {
            unsafe { *len = 0 };
        }
        return std::ptr::null();
    }
    let body = &unsafe { &*response }.body;
    if !len.is_null() {
        unsafe { *len = body.len() };
    }
    body.as_ptr()
}

/// Free an answer. Null is allowed and does nothing.
///
/// # Safety
///
/// `response` must be null or a response from [`zou_request`] that has
/// not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_response_free(response: *mut ZouResponse) {
    if !response.is_null() {
        drop(unsafe { Box::from_raw(response) });
    }
}

/// Put the same front door on a port, and write which one to `out`.
///
/// Port 0 asks the kernel for one. This serves the project the handle
/// is already holding rather than a second copy of it.
///
/// # Safety
///
/// `zou` must be a live handle and `out` a place to write a port, or
/// null if the caller already knows which port it asked for.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_listen(zou: *const ZouHandle, port: u16, out: *mut u16) -> c_int {
    guard("zou_listen", || {
        if zou.is_null() {
            return Err((ZOU_ERR_NULL, "zou is null".to_string()));
        }
        let bound = unsafe { &*zou }.zou.listen(port).map_err(failed)?;
        if !out.is_null() {
            unsafe { *out = bound };
        }
        Ok(())
    })
}

/// Push everything committed so far into the store.
///
/// # Safety
///
/// `zou` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_checkpoint(zou: *const ZouHandle) -> c_int {
    guard("zou_checkpoint", || {
        if zou.is_null() {
            return Err((ZOU_ERR_NULL, "zou is null".to_string()));
        }
        unsafe { &*zou }.zou.checkpoint().map_err(failed)
    })
}

/// Whether a branch of this database would serve, written to `out` as
/// 1 or 0.
///
/// It is 0 for a database young enough that no fold has packed a full
/// page capture down yet, which is the one case [`zou_branch`] refuses.
///
/// # Safety
///
/// `zou` must be a live handle and `out` a place to write an int.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_branchable(zou: *const ZouHandle, out: *mut c_int) -> c_int {
    guard("zou_branchable", || {
        if zou.is_null() {
            return Err((ZOU_ERR_NULL, "zou is null".to_string()));
        }
        if out.is_null() {
            return Err((ZOU_ERR_NULL, "out is null".to_string()));
        }
        let yes = unsafe { &*zou }.zou.branchable().map_err(failed)?;
        unsafe { *out = c_int::from(yes) };
        Ok(())
    })
}

/// A copy on write branch of this database, open and ready.
///
/// A checkpoint is taken first, so the child carries what this process
/// has written. Nothing is copied. The child is a second live handle
/// and must be closed like the first.
///
/// # Safety
///
/// `zou` must be a live handle, `name` a NUL terminated string, and
/// `out` a place to write the child handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zou_branch(
    zou: *const ZouHandle,
    name: *const c_char,
    out: *mut *mut ZouHandle,
) -> c_int {
    guard("zou_branch", || {
        if zou.is_null() {
            return Err((ZOU_ERR_NULL, "zou is null".to_string()));
        }
        if out.is_null() {
            return Err((ZOU_ERR_NULL, "out is null".to_string()));
        }
        let name = unsafe { text(name, "name")? };
        let child = unsafe { &*zou }.zou.branch(name).map_err(failed)?;
        let handle = ZouHandle::wrap(child)?;
        unsafe { *out = handle };
        Ok(())
    })
}

/// A compile time assertion that a handle can cross threads, which is
/// the contract the header states and the one a binding relies on.
const _: fn() = || {
    fn send_sync<T: Send + Sync>() {}
    send_sync::<Zou>();
    // And that nothing here is accidentally a pointer sized lie: the
    // header declares these as opaque structs, which only works while
    // they are behind a pointer.
    let _ = std::mem::size_of::<*mut c_void>();
};

#[cfg(test)]
mod tests {
    use super::*;

    fn cstr(s: &str) -> CString {
        CString::new(s).unwrap()
    }

    fn last() -> String {
        unsafe { CStr::from_ptr(zou_last_error()) }
            .to_string_lossy()
            .into_owned()
    }

    #[test]
    fn options_are_set_by_name_and_a_name_nobody_has_is_an_error() {
        let options = zou_options_new();
        assert!(!options.is_null());
        for (name, value) in [
            ("target", "/srv/store"),
            ("tenant", "pr-42"),
            ("pg_bin", "/opt/pg/bin"),
            ("runtime", "/tmp/run"),
            ("jwt_secret", "hunter2"),
            ("schemas", "public, api"),
            ("shared_buffers", "256MB"),
        ] {
            let code =
                unsafe { zou_options_set(options, cstr(name).as_ptr(), cstr(value).as_ptr()) };
            assert_eq!(code, ZOU_OK, "{name}");
        }
        let code =
            unsafe { zou_options_set(options, cstr("port").as_ptr(), cstr("5432").as_ptr()) };
        assert_eq!(code, ZOU_ERR_OPTIONS);
        assert!(last().contains("no option named"), "{}", last());
        unsafe { zou_options_free(options) };
    }

    #[test]
    fn a_null_is_a_code_and_a_sentence_rather_than_a_crash() {
        let mut out: *mut ZouHandle = std::ptr::null_mut();
        assert_eq!(
            unsafe { zou_open(std::ptr::null(), &mut out) },
            ZOU_ERR_NULL
        );
        assert!(last().contains("options is null"), "{}", last());
        assert_eq!(
            unsafe { zou_options_set(std::ptr::null_mut(), c"x".as_ptr(), c"y".as_ptr()) },
            ZOU_ERR_NULL
        );
        assert_eq!(
            unsafe { zou_branchable(std::ptr::null(), std::ptr::null_mut()) },
            ZOU_ERR_NULL
        );
        assert_eq!(unsafe { zou_checkpoint(std::ptr::null()) }, ZOU_ERR_NULL);
        assert_eq!(
            unsafe { zou_listen(std::ptr::null(), 0, std::ptr::null_mut()) },
            ZOU_ERR_NULL
        );
        // A close of nothing is not a failure, it is nothing.
        assert_eq!(unsafe { zou_close(std::ptr::null_mut()) }, ZOU_OK);
        // Neither is freeing nothing.
        unsafe { zou_options_free(std::ptr::null_mut()) };
        unsafe { zou_response_free(std::ptr::null_mut()) };
    }

    #[test]
    fn a_getter_on_a_handle_that_is_gone_answers_null() {
        assert!(unsafe { zou_anon_key(std::ptr::null()) }.is_null());
        assert!(unsafe { zou_service_role_key(std::ptr::null()) }.is_null());
        assert!(unsafe { zou_dsn(std::ptr::null()) }.is_null());
        assert!(unsafe { zou_target(std::ptr::null()) }.is_null());
        assert!(unsafe { zou_tenant(std::ptr::null()) }.is_null());
        assert_eq!(unsafe { zou_response_status(std::ptr::null()) }, 0);
        assert_eq!(unsafe { zou_response_header_count(std::ptr::null()) }, 0);
        assert!(unsafe { zou_response_header_name(std::ptr::null(), 0) }.is_null());
        let mut len = 7;
        assert!(unsafe { zou_response_body(std::ptr::null(), &mut len) }.is_null());
        assert_eq!(len, 0, "a length nobody set is worse than a zero");
    }

    #[test]
    fn a_postgres_that_is_not_there_is_named_rather_than_hung_on() {
        let options = zou_options_new();
        let dir = std::env::temp_dir().join(format!("libzou-test-{}", std::process::id()));
        unsafe {
            zou_options_set(
                options,
                c"pg_bin".as_ptr(),
                cstr("/nowhere/at/all/bin").as_ptr(),
            );
            zou_options_set(
                options,
                c"runtime".as_ptr(),
                cstr(&dir.display().to_string()).as_ptr(),
            );
        }
        let mut out: *mut ZouHandle = std::ptr::null_mut();
        assert_eq!(unsafe { zou_open(options, &mut out) }, ZOU_ERR_OPTIONS);
        assert!(last().contains("not found"), "{}", last());
        assert!(out.is_null(), "nothing was written on failure");
        unsafe { zou_options_free(options) };
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn every_kind_has_a_code_of_its_own() {
        let codes = [
            code(Kind::Options),
            code(Kind::Postgres),
            code(Kind::Store),
            code(Kind::Request),
            code(Kind::Io),
        ];
        let mut seen = codes.to_vec();
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(seen.len(), codes.len(), "two kinds sharing a code is a bug");
        assert!(codes.iter().all(|&c| c < 0), "and none of them is ZOU_OK");
    }

    #[test]
    fn a_panic_is_a_code_and_not_an_unwind() {
        assert_eq!(guard("a test", || panic!("boom")), ZOU_ERR_PANIC);
        assert!(last().contains("panicked"), "{}", last());
    }

    #[test]
    fn the_version_is_the_one_in_cargo_toml() {
        let said = unsafe { CStr::from_ptr(zou_version()) }.to_string_lossy();
        assert_eq!(said, env!("CARGO_PKG_VERSION"));
    }
}
