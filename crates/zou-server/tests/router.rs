//! The front door as a whole: the four Supabase prefixes, what answers
//! under each of them, and what happens outside all four.
//!
//! Everything else in this crate tests one surface. This tests that the
//! surfaces are wired to the paths a Supabase client already calls,
//! which is a different claim and one nothing else makes. A client
//! configured with a zou url and no other changes reaches /rest/v1 for
//! data, /auth/v1 for sessions, /storage/v1 for buckets and objects,
//! and
//! gets an honest 501 rather than a confusing 404 everywhere a later
//! milestone still has to fill in.
//!
//! Which side of the apikey gate each route sits on is part of the
//! wiring and is tested here too. Four auth routes are deliberately
//! outside it, and every one of them is outside it because the caller
//! cannot reasonably be holding a key: a token verifier, a link in an
//! email, and the two halves of a social sign in. The storage routes
//! are outside it for a different reason, which is that storage-api
//! answers a keyless request itself rather than letting the gateway do
//! it, and zou is both of those at once.
//!
//! No database. Nothing here gets far enough to need one.

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;
use zou_server::{Config, jwt, router};

const SECRET: &[u8] = b"super-secret-jwt-token-with-at-least-32-characters-long";

fn app() -> axum::Router {
    router(Config {
        jwt_secret: SECRET.to_vec(),
        ..Config::default()
    })
    .expect("router builds")
}

fn anon_key() -> String {
    jwt::mint(&jwt::key_claims("anon"), SECRET)
}

struct Answer {
    status: StatusCode,
    request_id: String,
    content_type: String,
    /// What axum offered instead of the method that was asked for, on
    /// the answers that carry it. Absent is a claim of its own: a route
    /// that names the methods it does have is naming code somebody has
    /// to keep true.
    allow: String,
    /// The bytes, for the two or three places where the order the
    /// fields were written in is the thing being tested.
    written: String,
    body: serde_json::Value,
}

impl Answer {
    fn message(&self) -> &str {
        self.body["message"].as_str().unwrap_or("<none>")
    }
}

/// A request with an apikey, which is what a configured client sends.
async fn keyed(app: &axum::Router, method: &str, path: &str) -> Answer {
    send(app, method, path, true).await
}

/// A request with none, which is what a mail client and a provider
/// redirect send.
async fn bare(app: &axum::Router, method: &str, path: &str) -> Answer {
    send(app, method, path, false).await
}

/// A request carrying one header and no key, which on the S3 surface is
/// the difference between two requests that share a verb and a route.
async fn carrying(app: &axum::Router, method: &str, path: &str, header: (&str, &str)) -> Answer {
    sent(app, method, path, false, Some(header)).await
}

async fn send(app: &axum::Router, method: &str, path: &str, key: bool) -> Answer {
    sent(app, method, path, key, None).await
}

async fn sent(
    app: &axum::Router,
    method: &str,
    path: &str,
    key: bool,
    header: Option<(&str, &str)>,
) -> Answer {
    let mut req = Request::builder().method(method).uri(path);
    if key {
        req = req.header("apikey", anon_key());
    }
    if let Some((name, value)) = header {
        req = req.header(name, value);
    }
    let res = app
        .clone()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .expect("router answers");
    let status = res.status();
    let request_id = res
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let content_type = res
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let allow = res
        .headers()
        .get("allow")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let bytes = to_bytes(res.into_body(), 1 << 20).await.unwrap();
    let body = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    Answer {
        status,
        request_id,
        content_type,
        allow,
        written: String::from_utf8_lossy(&bytes).to_string(),
        body,
    }
}

/// The words the hosted edge answers an unmatched route with.
const NO_ROUTE: &str = "no Route matched with those values";

#[tokio::test]
async fn all_four_prefixes_are_routed() {
    let app = app();
    // The claim is only that each prefix reaches something of its own
    // rather than the fallback. What each of them then says is the
    // subject of the tests below and of the other suites.
    for path in [
        "/rest/v1/todos",
        "/rest/v1/",
        "/rest/v1/rpc/whatever",
        "/auth/v1/health",
        "/auth/v1/settings",
        "/storage/v1/bucket",
        "/storage/v1/object/pics/cat.png",
        "/storage/v1/object/public/pics/cat.png",
        "/storage/v1/render/image/authenticated/pics/cat.png",
        "/storage/v1/render/image/public/pics/cat.png",
        "/storage/v1/render/image/sign/pics/cat.png",
        "/realtime/v1/websocket",
    ] {
        let answer = keyed(&app, "GET", path).await;
        assert_ne!(
            answer.message(),
            NO_ROUTE,
            "{path} fell through to the fallback",
        );
    }
}

#[tokio::test]
async fn the_two_stubbed_surfaces_say_so_rather_than_pretending_to_be_missing() {
    let app = app();
    // A 404 here would read as a wrong url and send somebody looking
    // for a typo. A 501 that names the surface and says where it is
    // tracked is the difference between a bug report and a wait.
    let storage = keyed(&app, "GET", "/storage/v1/analytics/pics/whichever").await;
    assert_eq!(storage.status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(
        storage.message(),
        "the storage surface is not implemented yet, tracked in tamnd/zou milestones",
    );

    let realtime = keyed(&app, "GET", "/realtime/v1/websocket").await;
    assert_eq!(realtime.status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(
        realtime.message(),
        "the realtime surface is not implemented yet, tracked in tamnd/zou milestones",
    );
}

#[tokio::test]
async fn a_stub_answers_whatever_method_it_is_asked() {
    let app = app();
    // Both are routed with `any`, so an upload and a subscribe get the
    // same answer as a read. A method that 405s under a stubbed
    // surface would be a second wrong answer on top of the first.
    for method in ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"] {
        for path in [
            "/storage/v1/analytics/pics/whichever",
            "/realtime/v1/websocket",
        ] {
            let answer = keyed(&app, method, path).await;
            assert_eq!(
                answer.status,
                StatusCode::NOT_IMPLEMENTED,
                "{method} {path}",
            );
        }
    }
}

#[tokio::test]
async fn an_auth_endpoint_nobody_wrote_yet_says_which_it_was() {
    let app = app();
    // The catch all under /auth/v1 is the same idea one prefix down.
    // GoTrue has endpoints zou does not serve, and a client calling
    // one should hear that rather than doubt the url.
    let answer = keyed(&app, "POST", "/auth/v1/sso").await;
    assert_eq!(answer.status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(
        answer.message(),
        "this auth endpoint is not implemented yet, tracked in tamnd/zou milestones",
    );
}

#[tokio::test]
async fn anything_outside_the_four_prefixes_is_the_edges_own_404() {
    let app = app();
    for path in ["/", "/rest", "/auth", "/graphql/v1", "/functions/v1/hello"] {
        let answer = keyed(&app, "GET", path).await;
        assert_eq!(answer.status, StatusCode::NOT_FOUND, "{path}");
        assert_eq!(
            answer.message(),
            NO_ROUTE,
            "{path} should be answered in the hosted edge's words",
        );
    }
}

#[tokio::test]
async fn the_four_open_routes_need_no_apikey() {
    let app = app();
    // Every one of these is reachable by somebody who cannot be
    // holding a key. A verifier fetching the public half of the
    // signing keys, a link clicked in a mail client, and the two
    // halves of a social sign in, which are navigations rather than
    // fetches.
    for (method, path) in [
        ("GET", "/auth/v1/.well-known/jwks.json"),
        ("GET", "/auth/v1/verify?token=nope&type=signup"),
        ("POST", "/auth/v1/verify"),
        ("GET", "/auth/v1/authorize?provider=github"),
        ("GET", "/auth/v1/callback"),
    ] {
        let answer = bare(&app, method, path).await;
        assert_ne!(
            answer.status,
            StatusCode::UNAUTHORIZED,
            "{method} {path} was refused for want of a key it cannot have",
        );
        assert_ne!(answer.message(), "No API key found in request");
    }
}

#[tokio::test]
async fn the_storage_surface_refuses_in_its_own_words_rather_than_the_gates() {
    let app = app();
    // The storage routes are outside the gate for a reason none of the
    // four above share. storage-api runs behind the same hosted gateway
    // everything else does, and a request to it with no key is still
    // answered by storage-api rather than by the gateway, because the
    // gateway is not what reads the token there. zou is both ends at
    // once, so the only way to answer that request in the recorded
    // words is to let it past the gate and refuse it in the handler.
    for (method, path) in [
        ("GET", "/storage/v1/bucket"),
        ("POST", "/storage/v1/bucket"),
        ("GET", "/storage/v1/bucket/photos"),
        ("PUT", "/storage/v1/bucket/photos"),
        ("DELETE", "/storage/v1/bucket/photos"),
        ("POST", "/storage/v1/bucket/photos/empty"),
        // Every object route that writes reads its token before it
        // reads anything else, so the same sentence comes back from
        // this half of the surface without a database behind it.
        ("POST", "/storage/v1/object/photos/cat.png"),
        ("PUT", "/storage/v1/object/photos/cat.png"),
        ("DELETE", "/storage/v1/object/photos/cat.png"),
        ("DELETE", "/storage/v1/object/photos"),
        // And both listings, which read theirs before they read the
        // body. A bucket called `list` would land here rather than on
        // the route that carries bytes, which is upstream's arrangement
        // too and the reason these two paths are worth naming.
        ("POST", "/storage/v1/object/list/photos"),
        ("POST", "/storage/v1/object/list-v2/photos"),
        // And move and copy, which name no bucket in the path at all
        // and are refused before their body is read.
        ("POST", "/storage/v1/object/move"),
        ("POST", "/storage/v1/object/copy"),
    ] {
        let answer = bare(&app, method, path).await;
        assert_eq!(answer.status, StatusCode::BAD_REQUEST, "{method} {path}");
        assert_eq!(answer.message(), "Invalid Compact JWS", "{method} {path}");
        // The status a caller acts on is in the body and it is a
        // string, which is the shape of every storage-api failure.
        assert_eq!(answer.body["statusCode"], "403", "{method} {path}");
        assert_eq!(answer.body["code"], "AccessDenied", "{method} {path}");
        // Charset and all. The conformance differ compares this header,
        // so a bucket answer sending zou's usual bare json would be a
        // difference against the recording.
        assert_eq!(
            answer.content_type, "application/json; charset=utf-8",
            "{method} {path}",
        );
    }
}

#[tokio::test]
async fn the_resumable_routes_refuse_in_the_resumable_order() {
    let app = app();
    // Same side of the gate as the rest of storage, same sentence, and
    // the four fields in a different order than the test above them.
    // Upstream has two serializers for one error object and this door
    // reaches the second, which nothing but a byte comparison sees.
    for (method, path) in [
        ("POST", "/storage/v1/upload/resumable"),
        ("PATCH", "/storage/v1/upload/resumable/bm90ZXMvYS50eHQ"),
        ("DELETE", "/storage/v1/upload/resumable/bm90ZXMvYS50eHQ"),
    ] {
        let answer = bare(&app, method, path).await;
        assert_eq!(answer.status, StatusCode::BAD_REQUEST, "{method} {path}");
        assert_eq!(
            answer.written,
            r#"{"statusCode":"403","code":"AccessDenied","error":"Unauthorized","message":"Invalid Compact JWS"}"#,
            "{method} {path}",
        );
        assert_eq!(
            answer.content_type, "application/json; charset=utf-8",
            "{method} {path}",
        );
    }
}

#[tokio::test]
async fn a_method_the_resumable_protocol_has_no_use_for_offers_no_others() {
    let app = app();
    // Both routes are one `any` that reads the method itself, and this
    // is the reason. A `MethodRouter` adds an Allow header on a method
    // it does not have even when the miss goes to a fallback, upstream
    // sends none, and the recording compares headers.
    for (method, path) in [
        ("GET", "/storage/v1/upload/resumable"),
        ("PUT", "/storage/v1/upload/resumable"),
        ("GET", "/storage/v1/upload/resumable/bm90ZXMvYS50eHQ"),
        ("POST", "/storage/v1/upload/resumable/bm90ZXMvYS50eHQ"),
    ] {
        let answer = keyed(&app, method, path).await;
        assert_eq!(answer.status, StatusCode::NOT_FOUND, "{method} {path}");
        assert_eq!(answer.allow, "", "{method} {path}");
        assert_eq!(answer.body["error"], "Not Found", "{method} {path}");
    }
}

#[tokio::test]
async fn a_path_under_storage_that_is_neither_a_bucket_nor_an_object_is_still_stubbed() {
    let app = app();
    // A literal segment beats a wildcard one, so the bucket and object
    // routes take what they name and the catch all keeps the rest.
    // Worth a test because the two live in different routers and are
    // merged, and a merge that shadowed the catch all would leave every
    // unbuilt storage path answering about a token instead.
    let answer = keyed(&app, "GET", "/storage/v1/analytics/pics/whichever").await;
    assert_eq!(answer.status, StatusCode::NOT_IMPLEMENTED);
}

#[tokio::test]
async fn the_s3_door_answers_a_request_with_no_key_on_it() {
    let app = app();
    // Outside the gate, like the rest of storage and for a stronger
    // reason: a client speaking this protocol signs the request and
    // sends no apikey at all, so a gate in front of it would refuse
    // every correct client. What it hears instead is the surface's own
    // refusal, in xml, about the authorization header it did send.
    for path in ["/storage/v1/s3", "/storage/v1/s3/", "/storage/v1/s3/pics"] {
        let answer = bare(&app, "GET", path).await;
        assert_ne!(answer.status, StatusCode::UNAUTHORIZED, "{path}");
    }
    let answer = bare(&app, "GET", "/storage/v1/s3").await;
    assert_eq!(answer.status, StatusCode::BAD_REQUEST);
    assert_eq!(answer.content_type, "application/xml; charset=utf-8");
    assert!(
        answer.written.contains("<Code>InvalidSignature</Code>"),
        "{}",
        answer.written
    );
    assert!(
        answer
            .written
            .contains("<Message>Unsupported authorization type</Message>"),
        "{}",
        answer.written
    );
}

#[tokio::test]
async fn an_s3_call_this_surface_has_no_answer_for_yet_says_so_without_naming_methods() {
    let app = app();
    // Every verb this route answers is dispatched inside one handler,
    // because the puts on it are told apart by a header and a pair of
    // query parameters and not by anything a router could look at. So
    // the verbs it does not answer end up in the same handler too, and
    // what they hear has to be this rather than the 405 with a list of
    // verbs a method router would have given: a list that named the
    // ones that are written would read as a claim about the protocol
    // rather than about how much of it is here.
    let path = "/storage/v1/s3/pics/big?partNumber=1&uploadId=whichever";
    let answer = carrying(&app, "PATCH", path, ("x-amz-copy-source", "pics/small")).await;
    assert_eq!(answer.status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(answer.allow, "");
    assert_eq!(
        answer.message(),
        "the storage surface is not implemented yet, tracked in tamnd/zou milestones",
    );
}

#[tokio::test]
async fn everything_else_is_behind_the_gate() {
    let app = app();
    for (method, path) in [
        ("GET", "/rest/v1/todos"),
        ("GET", "/auth/v1/health"),
        ("GET", "/auth/v1/settings"),
        ("POST", "/auth/v1/token"),
        ("POST", "/auth/v1/signup"),
        ("GET", "/auth/v1/user"),
        ("GET", "/auth/v1/admin/users"),
        ("POST", "/auth/v1/factors"),
        ("GET", "/realtime/v1/websocket"),
    ] {
        let answer = bare(&app, method, path).await;
        assert_eq!(
            answer.status,
            StatusCode::UNAUTHORIZED,
            "{method} {path} answered without a key",
        );
        assert_eq!(answer.message(), "No API key found in request");
    }
}

#[tokio::test]
async fn the_gate_is_reached_before_a_stub_is() {
    let app = app();
    // Ordering, not routing. The stubs sit inside the gated router, so
    // an unkeyed request to an unbuilt surface hears about the key
    // rather than about the surface. A stub outside the gate would be
    // a hole that grows when the surface is built.
    let answer = bare(&app, "GET", "/storage/v1/analytics/pics/whichever").await;
    assert_eq!(answer.status, StatusCode::UNAUTHORIZED);
    assert_eq!(answer.message(), "No API key found in request");
}

#[tokio::test]
async fn every_answer_carries_a_request_id() {
    let app = app();
    // The id layer is outermost on purpose, so a 404 from the
    // fallback and a 401 from the gate carry one as surely as a 200
    // does. It is the only thing an operator has to join a complaint
    // to a log line.
    for (keyed_request, method, path) in [
        (true, "GET", "/auth/v1/health"),
        (true, "GET", "/storage/v1/bucket"),
        (true, "GET", "/nowhere"),
        (false, "GET", "/rest/v1/todos"),
        (false, "GET", "/auth/v1/.well-known/jwks.json"),
    ] {
        let answer = send(&app, method, path, keyed_request).await;
        assert!(
            !answer.request_id.is_empty(),
            "{method} {path} came back with no id",
        );
    }
}

#[tokio::test]
async fn a_preflight_never_reaches_the_gate() {
    let app = app();
    // CORS sits outside the gate, so a browser asking permission gets
    // an answer rather than a 401. A preflight carries no apikey by
    // definition: it is the browser asking, not the application.
    let res = app
        .oneshot(
            Request::builder()
                .method("OPTIONS")
                .uri("/rest/v1/todos")
                .header("origin", "https://app.zou.test")
                .header("access-control-request-method", "POST")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("router answers");
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        res.headers()["access-control-allow-origin"],
        "https://app.zou.test",
    );
}

#[tokio::test]
async fn a_stubbed_surface_is_stubbed_all_the_way_to_its_root() {
    let app = app();
    // A wildcard segment matches neither the bare prefix nor the empty
    // path after the slash, so both of those spellings need routes of
    // their own or they answer as a url that does not exist rather
    // than as a surface that does not exist yet.
    for path in ["/storage/v1", "/storage/v1/"] {
        let answer = keyed(&app, "GET", path).await;
        assert_eq!(answer.status, StatusCode::NOT_IMPLEMENTED, "{path}");
        assert_eq!(
            answer.message(),
            "the storage surface is not implemented yet, tracked in tamnd/zou milestones",
        );
    }
    for path in ["/realtime/v1", "/realtime/v1/"] {
        let answer = keyed(&app, "GET", path).await;
        assert_eq!(answer.status, StatusCode::NOT_IMPLEMENTED, "{path}");
        assert_eq!(
            answer.message(),
            "the realtime surface is not implemented yet, tracked in tamnd/zou milestones",
        );
    }
}

#[tokio::test]
async fn a_url_that_missed_the_rest_surface_is_told_so_by_the_rest_surface() {
    let app = app();
    // Three ways to miss it: too many segments, a prefix that is not a
    // segment at all because the slash was dropped, and a rpc name with
    // a path stuck on the end. None of them is a table, and all of them
    // are this api, so the caller hears this api's error rather than the
    // gateway's.
    for path in [
        "/rest/v1/first/second/third",
        "/rest/v1todos",
        "/rest/v1/rpc/add_them/again",
    ] {
        let answer = keyed(&app, "GET", path).await;
        assert_eq!(answer.status, StatusCode::NOT_FOUND, "{path}");
        assert_eq!(answer.body["code"], "PGRST125", "{path}");
        assert_eq!(
            answer.message(),
            "Invalid path specified in request URL",
            "{path}",
        );
        assert_eq!(
            answer.content_type, "application/json; charset=utf-8",
            "{path}"
        );
    }
}

#[tokio::test]
async fn the_two_built_prefixes_keep_their_own_shape() {
    let app = app();
    // The same treatment would be wrong here. /rest/v1/ is the OpenAPI
    // document and is routed, /rest/v1 is not a url PostgREST serves,
    // and /auth/v1 is not an endpoint GoTrue has. A prefix with real
    // endpoints under it does not own its own root just by existing.
    for path in ["/rest/v1", "/auth/v1", "/auth/v1/"] {
        let answer = keyed(&app, "GET", path).await;
        assert_eq!(answer.status, StatusCode::NOT_FOUND, "{path}");
        assert_eq!(answer.message(), NO_ROUTE, "{path}");
    }
}
