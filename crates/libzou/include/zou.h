/* zou.h: a Supabase compatible backend as a shared library.
 *
 * A store, a Postgres over it, and the REST, auth, and storage surfaces
 * in front, all inside the calling process. A request is answered
 * without a socket; zou_listen puts the same surfaces on a port as well
 * when something outside wants in.
 *
 * The rules, once:
 *
 *   Every call that can fail returns ZOU_OK or a negative code, and
 *   none of them unwind: a panic inside becomes ZOU_ERR_PANIC. On a
 *   nonzero code the out parameter was not written and zou_last_error()
 *   has a sentence about it, valid on this thread until the next call
 *   into this library.
 *
 *   Ownership is by name. zou_options_new, zou_open, zou_branch, and
 *   zou_request hand back something you own and must pass to the
 *   matching free or to zou_close. Everything else returns a borrowed
 *   pointer that lives as long as the thing it came from.
 *
 *   A zou handle may be used from any thread, and from several at once.
 *   It must not be used after zou_close, which frees it.
 *
 *   Strings are UTF-8 and NUL terminated. A response body is bytes with
 *   a length, because a body may be an image.
 *
 * Postgres is a child process. It has to be the patched build, so set
 * the pg_bin option or the ZOU_PG_BIN environment variable.
 *
 * Unix only for now.
 */

#ifndef ZOU_H
#define ZOU_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define ZOU_OK 0
#define ZOU_ERR_OPTIONS -1  /* the options do not describe anything openable */
#define ZOU_ERR_POSTGRES -2 /* postgres would not start, stop, or answer */
#define ZOU_ERR_STORE -3    /* the store said no */
#define ZOU_ERR_REQUEST -4  /* the request or its answer could not be built */
#define ZOU_ERR_IO -5       /* the filesystem or the network under all of it */
#define ZOU_ERR_NULL -6     /* a null pointer where one was required */
#define ZOU_ERR_UTF8 -7     /* a string that was not utf-8 */
#define ZOU_ERR_PANIC -8    /* something panicked, and did not unwind here */

/* Opaque. Only ever held behind a pointer. */
typedef struct ZouOptions zou_options;
typedef struct ZouHandle zou;
typedef struct ZouResponse zou_response;

typedef struct {
    const char *name;
    const char *value;
} zou_header;

/* What went wrong on this thread, most recently. Never null: with
 * nothing to report it is an empty string. Do not free it, and do not
 * keep it across another call on this thread. */
const char *zou_last_error(void);

/* The version of this library. */
const char *zou_version(void);

/* Options start as an ephemeral store, the local database, and the
 * postgres ZOU_PG_BIN names. */
zou_options *zou_options_new(void);
void zou_options_free(zou_options *options);

/* Set one option by name. The names:
 *
 *   target          a directory, s3://bucket/prefix, sqlite:///path, or
 *                   a .zou file. Empty is ephemeral, a store of the
 *                   handle's own that goes away on close.
 *   tenant          the database inside the store, default local
 *   pg_bin          the patched postgres install
 *   runtime         where the running copy lives, removed on close
 *   jwt_secret      what tokens are signed with, random when unset
 *   schemas         comma separated, first one the default
 *   shared_buffers  for the child postmaster
 *
 * A name nobody has is ZOU_ERR_OPTIONS rather than a shrug. */
int zou_options_set(zou_options *options, const char *name, const char *value);

/* Open a project. This starts a postmaster, and on a store with no
 * database in it runs initdb first, so it is seconds rather than
 * milliseconds. */
int zou_open(const zou_options *options, zou **out);

/* Stop postgres, remove the running copy, and free the handle. The
 * handle is gone either way, so a nonzero answer is to log rather than
 * to retry. Null is allowed and does nothing. */
int zou_close(zou *handle);

/* Borrowed, and live as long as the handle. */
const char *zou_anon_key(const zou *handle);
const char *zou_service_role_key(const zou *handle);
const char *zou_dsn(const zou *handle);
const char *zou_target(const zou *handle);
const char *zou_tenant(const zou *handle);

/* Answer one request, in this process.
 *
 * path is what a url carries after the host, query string and all.
 * Nothing is added to the headers, so a call with no apikey is refused
 * exactly as it would be over http. headers may be null when nheaders
 * is 0, and body may be null when body_len is 0. */
int zou_request(const zou *handle, const char *method, const char *path,
                const zou_header *headers, size_t nheaders,
                const uint8_t *body, size_t body_len, zou_response **out);

uint16_t zou_response_status(const zou_response *response);
size_t zou_response_header_count(const zou_response *response);
const char *zou_response_header_name(const zou_response *response, size_t i);
const char *zou_response_header_value(const zou_response *response, size_t i);
/* Not NUL terminated. len is how you know where it ends. */
const uint8_t *zou_response_body(const zou_response *response, size_t *len);
void zou_response_free(zou_response *response);

/* Put the same front door on a port too, and write which one to out.
 * Port 0 asks the kernel for one. out may be null. */
int zou_listen(const zou *handle, uint16_t port, uint16_t *out);

/* Push everything committed so far into the store. */
int zou_checkpoint(const zou *handle);

/* Whether a branch of this database would serve, written to out as 1 or
 * 0. It is 0 for a database young enough that no fold has packed a full
 * page capture down yet, which is the one case zou_branch refuses. */
int zou_branchable(const zou *handle, int *out);

/* A copy on write branch, open and ready, as a second handle to close
 * like the first. A checkpoint is taken first so the child carries what
 * this process has written, and nothing is copied. */
int zou_branch(const zou *handle, const char *name, zou **out);

#ifdef __cplusplus
}
#endif

#endif /* ZOU_H */
