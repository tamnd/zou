// Package zou opens a whole Supabase compatible project inside a Go
// process: a store, a Postgres over it, and the REST, auth, and storage
// surfaces in front, with no daemon to start and no port to pick.
//
// It is cgo over libzou, the C ABI in crates/libzou, which is the same
// library the C and node and python bindings sit on. The seam into Go is
// an [http.RoundTripper]: a request handed to it is answered in this
// process, with no socket under it, so anything that takes an
// *http.Client can be pointed at a database of its own.
//
//	zou, err := zou.Fixture()
//	defer zou.Close()
//	answer, err := zou.Client().Get(zou.URL + "/rest/v1/todos?select=title")
//
// The library has to be built and findable first, which is
// `cargo build -p libzou`. See go/README.md.
//
// Unix only for now, the same as everything else over zou-embed.
package zou

/*
#cgo CFLAGS: -I${SRCDIR}/../crates/libzou/include
#cgo LDFLAGS: -L${SRCDIR}/../target/debug -lzou -Wl,-rpath,${SRCDIR}/../target/debug

#include <stdlib.h>
#include "zou.h"

// The error message belongs to the thread that made the call, and a
// goroutine is not a thread: between two cgo calls Go may well have
// moved this goroutine somewhere else, and the message would be gone.
// So every call that can fail is paired with its message inside one cgo
// call, which is one thread by construction.
static int zou_go_options_set(zou_options *options, const char *name, const char *value,
                              const char **err) {
    int rc = zou_options_set(options, name, value);
    if (rc != ZOU_OK) { *err = zou_last_error(); }
    return rc;
}

static int zou_go_open(const zou_options *options, zou **out, const char **err) {
    int rc = zou_open(options, out);
    if (rc != ZOU_OK) { *err = zou_last_error(); }
    return rc;
}

static int zou_go_request(const zou *handle, const char *method, const char *path,
                          const zou_header *headers, size_t header_count,
                          const uint8_t *body, size_t body_len, zou_response **out,
                          const char **err) {
    int rc = zou_request(handle, method, path, headers, header_count, body, body_len, out);
    if (rc != ZOU_OK) { *err = zou_last_error(); }
    return rc;
}

static int zou_go_listen(const zou *handle, uint16_t port, uint16_t *out, const char **err) {
    int rc = zou_listen(handle, port, out);
    if (rc != ZOU_OK) { *err = zou_last_error(); }
    return rc;
}

static int zou_go_branch(const zou *handle, const char *name, zou **out, const char **err) {
    int rc = zou_branch(handle, name, out);
    if (rc != ZOU_OK) { *err = zou_last_error(); }
    return rc;
}

static int zou_go_branchable(const zou *handle, int *out, const char **err) {
    int rc = zou_branchable(handle, out);
    if (rc != ZOU_OK) { *err = zou_last_error(); }
    return rc;
}

static int zou_go_checkpoint(const zou *handle, const char **err) {
    int rc = zou_checkpoint(handle);
    if (rc != ZOU_OK) { *err = zou_last_error(); }
    return rc;
}

static int zou_go_close(zou *handle, const char **err) {
    int rc = zou_close(handle);
    if (rc != ZOU_OK) { *err = zou_last_error(); }
    return rc;
}

// A zou_header array is built here rather than in Go, because a Go
// slice of structs holding Go pointers may not be passed to C.
static zou_header *zou_go_headers(size_t count) {
    if (count == 0) { return NULL; }
    return (zou_header *)calloc(count, sizeof(zou_header));
}

static void zou_go_header_set(zou_header *headers, size_t i, const char *name,
                              const char *value) {
    headers[i].name = name;
    headers[i].value = value;
}
*/
import "C"

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"unsafe"
)

// ORIGIN is where a project is before anything is listening.
//
// An http client wants an absolute url, so there has to be one even when
// there is no port. Nothing resolves this name and nothing needs to: the
// origin is stripped off again on the way in.
const ORIGIN = "http://zou.embedded"

// Error is anything zou refused to do, with the kind it refused it as.
type Error struct {
	// Code is the C ABI's code, negative.
	Code int
	// Kind is that code as a word: options, postgres, store, request,
	// io, null, utf8, or panic.
	Kind string
	// Message is the sentence the library had about it.
	Message string
}

func (e *Error) Error() string { return e.Kind + ": " + e.Message }

func failed(code C.int, message *C.char) error {
	kind := "unknown"
	switch code {
	case C.ZOU_ERR_OPTIONS:
		kind = "options"
	case C.ZOU_ERR_POSTGRES:
		kind = "postgres"
	case C.ZOU_ERR_STORE:
		kind = "store"
	case C.ZOU_ERR_REQUEST:
		kind = "request"
	case C.ZOU_ERR_IO:
		kind = "io"
	case C.ZOU_ERR_NULL:
		kind = "null"
	case C.ZOU_ERR_UTF8:
		kind = "utf8"
	case C.ZOU_ERR_PANIC:
		kind = "panic"
	}
	said := ""
	if message != nil {
		said = C.GoString(message)
	}
	return &Error{Code: int(code), Kind: kind, Message: said}
}

// Options is how a project is opened, with everything optional.
//
// Dir and URL are two ways of saying where the store is, and neither is
// a store of this handle's own that goes away when it closes, which is
// the one a test wants.
type Options struct {
	// Dir is a directory of objects.
	Dir string
	// URL is a bucket, or sqlite://, or a .zou file.
	URL string
	// Tenant is the database inside the store, default local.
	Tenant string
	// PgBin is the patched postgres install, default $ZOU_PG_BIN.
	PgBin string
	// Runtime is where the running copy lives, removed on close.
	Runtime string
	// JWTSecret is what tokens are signed with, random when unset,
	// which is right for a test and wrong for anything that has to
	// still recognise its own tokens after a restart.
	JWTSecret string
	// Schemas are the ones REST exposes, the first one the default.
	Schemas []string
	// SharedBuffers is for the child postmaster.
	SharedBuffers string
	// Fixture cuts this database out of the machine's template instead
	// of making one, which is milliseconds rather than seconds. See
	// [Fixture].
	Fixture bool
}

// Zou is one open project.
//
// It may be used from any number of goroutines at once. Close is safe to
// call twice, which is what makes it safe in a defer that may or may not
// be the one that got there first.
type Zou struct {
	handle *C.zou
	mu     sync.Mutex
	closed bool
	port   uint16
}

// Open opens a project, which starts a postmaster and, on a store with
// no database in it, runs initdb first, so it is seconds rather than
// milliseconds.
func Open(options Options) (*Zou, error) {
	if options.Dir != "" && options.URL != "" {
		return nil, &Error{
			Code:    int(C.ZOU_ERR_OPTIONS),
			Kind:    "options",
			Message: "Dir and URL are two ways of saying the same thing, pick one",
		}
	}
	target := options.Dir
	if options.URL != "" {
		target = options.URL
	}
	pgBin := options.PgBin
	if pgBin == "" {
		pgBin = os.Getenv("ZOU_PG_BIN")
	}

	settings := [][2]string{
		{"target", target},
		{"tenant", options.Tenant},
		{"pg_bin", pgBin},
		{"runtime", options.Runtime},
		{"jwt_secret", options.JWTSecret},
		{"schemas", strings.Join(options.Schemas, ",")},
		{"shared_buffers", options.SharedBuffers},
	}
	if options.Fixture {
		settings = append(settings, [2]string{"fixture", "1"})
	}

	raw := C.zou_options_new()
	if raw == nil {
		return nil, &Error{Kind: "io", Message: "no memory for the options"}
	}
	defer C.zou_options_free(raw)

	for _, setting := range settings {
		// An option nobody set is not an option: an empty target means
		// ephemeral and is worth saying, the rest are not.
		if setting[1] == "" && setting[0] != "target" {
			continue
		}
		name := C.CString(setting[0])
		value := C.CString(setting[1])
		var message *C.char
		rc := C.zou_go_options_set(raw, name, value, &message)
		C.free(unsafe.Pointer(name))
		C.free(unsafe.Pointer(value))
		if rc != C.ZOU_OK {
			return nil, failed(rc, message)
		}
	}

	var handle *C.zou
	var message *C.char
	if rc := C.zou_go_open(raw, &handle, &message); rc != C.ZOU_OK {
		return nil, failed(rc, message)
	}
	opened := &Zou{handle: handle}
	// A project that goes out of scope with a postmaster still under it
	// would leave the postmaster running, so the last word is here.
	runtime.SetFinalizer(opened, func(z *Zou) { _ = z.Close() })
	return opened, nil
}

// Fixture is a database per test, in the time a test can afford.
//
// An ephemeral project runs initdb, which is seconds. This branches a
// template the machine already built instead, so what is left is the
// postmaster start and it is tens of milliseconds. The first call on a
// cold machine builds the template and pays the initdb once, and
// ZOU_TEMPLATE_CACHE says where it goes if a CI job wants to cache the
// directory.
//
// Fixtures see nothing of each other and closing one takes it off the
// store, so opening one at the top of a test and deferring Close is the
// whole story.
func Fixture() (*Zou, error) {
	return Open(Options{Fixture: true})
}

// AnonKey is the key a browser gets.
func (z *Zou) AnonKey() string { return C.GoString(C.zou_anon_key(z.handle)) }

// ServiceRoleKey is the key that skips row level security. Keep it on
// the server.
func (z *Zou) ServiceRoleKey() string { return C.GoString(C.zou_service_role_key(z.handle)) }

// DSN is the other door, for database/sql or psql on the database
// directly, which is how a test creates its own schema before serving
// it.
func (z *Zou) DSN() string { return C.GoString(C.zou_dsn(z.handle)) }

// Target is the store everything durable is on.
func (z *Zou) Target() string { return C.GoString(C.zou_target(z.handle)) }

// Tenant is the database inside it.
func (z *Zou) Tenant() string { return C.GoString(C.zou_tenant(z.handle)) }

// URL is where this project is, for something that wants a url. It is
// the port once Listen has been called and a name that resolves nowhere
// before that, since before that there is nothing to resolve.
func (z *Zou) URL() string {
	z.mu.Lock()
	defer z.mu.Unlock()
	if z.port == 0 {
		return ORIGIN
	}
	return fmt.Sprintf("http://127.0.0.1:%d", z.port)
}

// Response is what the router answered, as plain data. RoundTrip turns
// one of these into an [http.Response], and most callers want that.
type Response struct {
	Status  int
	Headers [][2]string
	Body    []byte
}

// Request answers one request, in this process. No socket, no port.
//
// Headers are pairs rather than a map because a header may be there more
// than once and a map quietly keeps the last one.
func (z *Zou) Request(method, path string, headers [][2]string, body []byte) (*Response, error) {
	cMethod := C.CString(method)
	defer C.free(unsafe.Pointer(cMethod))
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	cHeaders := C.zou_go_headers(C.size_t(len(headers)))
	if cHeaders != nil {
		defer C.free(unsafe.Pointer(cHeaders))
	}
	for i, header := range headers {
		name := C.CString(header[0])
		value := C.CString(header[1])
		defer C.free(unsafe.Pointer(name))
		defer C.free(unsafe.Pointer(value))
		C.zou_go_header_set(cHeaders, C.size_t(i), name, value)
	}

	var bytesPtr *C.uint8_t
	if len(body) > 0 {
		bytesPtr = (*C.uint8_t)(unsafe.Pointer(&body[0]))
	}

	var answer *C.zou_response
	var message *C.char
	rc := C.zou_go_request(z.handle, cMethod, cPath, cHeaders, C.size_t(len(headers)),
		bytesPtr, C.size_t(len(body)), &answer, &message)
	runtime.KeepAlive(body)
	if rc != C.ZOU_OK {
		return nil, failed(rc, message)
	}
	defer C.zou_response_free(answer)

	out := &Response{Status: int(C.zou_response_status(answer))}
	count := int(C.zou_response_header_count(answer))
	out.Headers = make([][2]string, 0, count)
	for i := 0; i < count; i++ {
		out.Headers = append(out.Headers, [2]string{
			C.GoString(C.zou_response_header_name(answer, C.size_t(i))),
			C.GoString(C.zou_response_header_value(answer, C.size_t(i))),
		})
	}
	var length C.size_t
	raw := C.zou_response_body(answer, &length)
	if length > 0 {
		out.Body = C.GoBytes(unsafe.Pointer(raw), C.int(length))
	}
	return out, nil
}

// RoundTrip answers an [http.Request] in this process, which makes a
// *Zou an [http.RoundTripper] and so the transport of any client that
// takes one.
func (z *Zou) RoundTrip(request *http.Request) (*http.Response, error) {
	headers := make([][2]string, 0, len(request.Header)+1)
	for name, values := range request.Header {
		for _, value := range values {
			headers = append(headers, [2]string{name, value})
		}
	}
	var body []byte
	if request.Body != nil {
		read, err := io.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
		_ = request.Body.Close()
		body = read
	}

	path := request.URL.RequestURI()
	answer, err := z.Request(request.Method, path, headers, body)
	if err != nil {
		return nil, err
	}

	out := &http.Response{
		Status:     fmt.Sprintf("%d %s", answer.Status, http.StatusText(answer.Status)),
		StatusCode: answer.Status,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header, len(answer.Headers)),
		Request:    request,
	}
	for _, header := range answer.Headers {
		out.Header.Add(header[0], header[1])
	}
	out.Body = io.NopCloser(bytes.NewReader(answer.Body))
	out.ContentLength = int64(len(answer.Body))
	return out, nil
}

// Client is an *http.Client whose requests are answered in this process.
//
// It carries no key: a call that needs one carries it the same way it
// would against a hosted project, which is the point.
func (z *Zou) Client() *http.Client {
	return &http.Client{Transport: z}
}

// Listen puts the same front door on a port as well, and says which one.
// Port 0 asks the kernel.
func (z *Zou) Listen(port uint16) (uint16, error) {
	var out C.uint16_t
	var message *C.char
	if rc := C.zou_go_listen(z.handle, C.uint16_t(port), &out, &message); rc != C.ZOU_OK {
		return 0, failed(rc, message)
	}
	z.mu.Lock()
	z.port = uint16(out)
	z.mu.Unlock()
	return uint16(out), nil
}

// Branch takes a copy on write branch, open and ready, as a second
// project to close like the first.
func (z *Zou) Branch(name string) (*Zou, error) {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	var child *C.zou
	var message *C.char
	if rc := C.zou_go_branch(z.handle, cName, &child, &message); rc != C.ZOU_OK {
		return nil, failed(rc, message)
	}
	opened := &Zou{handle: child}
	runtime.SetFinalizer(opened, func(z *Zou) { _ = z.Close() })
	return opened, nil
}

// Branchable says whether a branch of this database would serve yet.
//
// On the object path a database young enough that no fold has packed a
// full page capture down cannot be branched, and a fixture can be from
// the moment it is cut, because the template folded before it was
// published. On the layer path the branch folds an image of its own out
// of the base, so the answer is true from the start.
func (z *Zou) Branchable() (bool, error) {
	var out C.int
	var message *C.char
	if rc := C.zou_go_branchable(z.handle, &out, &message); rc != C.ZOU_OK {
		return false, failed(rc, message)
	}
	return out == 1, nil
}

// Checkpoint pushes everything committed so far into the store.
func (z *Zou) Checkpoint() error {
	var message *C.char
	if rc := C.zou_go_checkpoint(z.handle, &message); rc != C.ZOU_OK {
		return failed(rc, message)
	}
	return nil
}

// Close stops postgres, removes the running copy, and frees the handle.
// Calling it twice is fine, and the second one does nothing.
func (z *Zou) Close() error {
	z.mu.Lock()
	defer z.mu.Unlock()
	if z.closed {
		return nil
	}
	z.closed = true
	runtime.SetFinalizer(z, nil)
	var message *C.char
	if rc := C.zou_go_close(z.handle, &message); rc != C.ZOU_OK {
		return failed(rc, message)
	}
	z.handle = nil
	return nil
}
