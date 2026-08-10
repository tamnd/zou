/* The C ABI, exercised the way somebody embedding it would.
 *
 * Open an ephemeral project, ask it something over the in process door
 * with the anon key, ask again without one, put it on a port too, take
 * a checkpoint, ask about branching and do it or read the refusal, then
 * close. Anything unexpected prints what went wrong and exits nonzero.
 *
 * Build and run it with crates/libzou/tests/smoke.sh, which needs the
 * patched postgres in ZOU_PG_BIN.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "zou.h"

static int failures = 0;

static void ok(int condition, const char *what) {
    if (condition) {
        printf("ok   %s\n", what);
        return;
    }
    printf("FAIL %s: %s\n", what, zou_last_error());
    failures++;
}

/* The body is bytes and not NUL terminated, so anything that wants to
 * printf it has to copy it first. */
static char *dup_body(const zou_response *response) {
    size_t len = 0;
    const uint8_t *bytes = zou_response_body(response, &len);
    char *copy = malloc(len + 1);
    if (copy == NULL) {
        return NULL;
    }
    if (len > 0) {
        memcpy(copy, bytes, len);
    }
    copy[len] = '\0';
    return copy;
}

static const char *header_of(const zou_response *response, const char *name) {
    size_t count = zou_response_header_count(response);
    for (size_t i = 0; i < count; i++) {
        const char *have = zou_response_header_name(response, i);
        if (have != NULL && strcmp(have, name) == 0) {
            return zou_response_header_value(response, i);
        }
    }
    return NULL;
}

int main(void) {
    printf("zou %s\n", zou_version());

    zou_options *options = zou_options_new();
    if (options == NULL) {
        printf("FAIL no options\n");
        return 1;
    }
    ok(zou_options_set(options, "tenant", "smoke") == ZOU_OK, "the tenant is set by name");
    ok(zou_options_set(options, "nonsense", "x") == ZOU_ERR_OPTIONS,
       "an option nobody has is refused");

    const char *pg_bin = getenv("ZOU_PG_BIN");
    if (pg_bin != NULL) {
        ok(zou_options_set(options, "pg_bin", pg_bin) == ZOU_OK, "postgres is pointed at");
    }

    zou *handle = NULL;
    int rc = zou_open(options, &handle);
    zou_options_free(options);
    if (rc != ZOU_OK) {
        printf("FAIL open: %s\n", zou_last_error());
        return 1;
    }
    printf("ok   opened %s on %s\n", zou_tenant(handle), zou_target(handle));

    ok(zou_dsn(handle) != NULL && strlen(zou_dsn(handle)) > 0, "there is a dsn to connect to");
    ok(strcmp(zou_anon_key(handle), zou_service_role_key(handle)) != 0,
       "the two keys are two keys");
    ok(strcmp(zou_tenant(handle), "smoke") == 0, "it is the database that was asked for");

    /* Sign somebody up, which is a request with headers and a body and
     * needs no schema of our own to have been created first. */
    zou_header headers[2];
    headers[0].name = "apikey";
    headers[0].value = zou_anon_key(handle);
    headers[1].name = "content-type";
    headers[1].value = "application/json";
    const char *signup = "{\"email\":\"c@example.com\",\"password\":\"correct horse battery\"}";

    zou_response *answer = NULL;
    rc = zou_request(handle, "POST", "/auth/v1/signup", headers, 2, (const uint8_t *)signup,
                     strlen(signup), &answer);
    ok(rc == ZOU_OK, "a request is answered in this process");
    if (rc == ZOU_OK) {
        ok(zou_response_status(answer) == 200, "the signup went through");
        const char *type = header_of(answer, "content-type");
        ok(type != NULL && strstr(type, "json") != NULL, "and answered in json");
        char *body = dup_body(answer);
        ok(body != NULL && strstr(body, "access_token") != NULL, "with a token in it");
        free(body);
        zou_response_free(answer);
        answer = NULL;
    }

    rc = zou_request(handle, "GET", "/rest/v1/", NULL, 0, NULL, 0, &answer);
    ok(rc == ZOU_OK, "a request with no key is answered too");
    if (rc == ZOU_OK) {
        ok(zou_response_status(answer) == 401, "and refused, exactly as it would be over http");
        zou_response_free(answer);
        answer = NULL;
    }

    uint16_t port = 0;
    ok(zou_listen(handle, 0, &port) == ZOU_OK, "the same project goes on a port as well");
    ok(port != 0, "and the kernel said which one");

    ok(zou_checkpoint(handle) == ZOU_OK, "a checkpoint reaches the store");

    int branchable = -1;
    ok(zou_branchable(handle, &branchable) == ZOU_OK, "whether it can be branched is answerable");
    if (branchable == 1) {
        zou *child = NULL;
        ok(zou_branch(handle, "smoke-branch", &child) == ZOU_OK, "and a branch opens");
        if (child != NULL) {
            ok(strcmp(zou_tenant(child), "smoke-branch") == 0, "as a database of its own");
            ok(zou_close(child) == ZOU_OK, "and closes on its own");
        }
    } else {
        /* A database this young has no full capture folded down yet, so
         * a branch of it would start and then die on its first page
         * read. Being told that is the right answer, not a failure. */
        zou *child = NULL;
        rc = zou_branch(handle, "smoke-branch", &child);
        ok(rc == ZOU_ERR_STORE, "a branch too early is refused");
        ok(strstr(zou_last_error(), "cannot be branched yet") != NULL, "and says why");
        ok(child == NULL, "and hands back nothing");
    }

    /* A null handle answers rather than segfaults, which is the half of
     * the error model a binding leans on hardest. */
    ok(zou_anon_key(NULL) == NULL, "a null handle has no key");
    ok(zou_checkpoint(NULL) == ZOU_ERR_NULL, "and null is a code, not a crash");
    ok(zou_close(NULL) == ZOU_OK, "and closing nothing is nothing");

    ok(zou_close(handle) == ZOU_OK, "closing takes postgres with it");

    if (failures > 0) {
        printf("\n%d failed\n", failures);
        return 1;
    }
    printf("\nall good\n");
    return 0;
}
