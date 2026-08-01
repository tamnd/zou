//! The admin surface, GoTrue's `/auth/v1/admin/users`, `/generate_link`
//! and `/invite`.
//!
//! Everything here is the service role acting on somebody else's
//! account, which is a different shape of endpoint from the rest of the
//! auth surface: there is no session to prove anything with, no mail to
//! wait for, and no rule about what an account may do to itself. The
//! bearer token carries a role rather than a person, and holding it is
//! the whole of the authorisation.
//!
//! The refusals are upstream's, because a dashboard or a migration
//! script branches on them. A user id that is not a uuid is answered
//! with a 404 rather than a 400, which reads like a slip and is one, and
//! it is here as it is for the same reason every other upstream slip is
//! kept: somebody has already matched on it.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::response::Response;

use crate::auth::{
    Caller, Code, Error, Outgoing, Post, caller, confirm_address, denied, error_body, field,
    hash_off_thread, identities_join, is_uuid, keep_token, landing, link_target, merge_metadata,
    no_database, posting, query_escape, query_object, read_json, refusal, refused, requested_aud,
    send_code, six_digits, still_there, taken, token_hash, unguessable_password, user_json,
    user_object, validate_email, validate_password,
};
use crate::json_body;
use crate::sql;

/// The nil uuid, which is both the instance every row carries and the id
/// a custom id is not allowed to be.
const NOBODY: &str = "00000000-0000-0000-0000-000000000000";

/// GoTrue's JWT_DEFAULT_GROUP_NAME, the role an account gets when the
/// request does not name one.
const DEFAULT_ROLE: &str = "authenticated";

/// GoTrue's defaultPerPage.
const PER_PAGE: u64 = 50;

const DUPLICATE_EMAIL: &str = "A user with this email address has already been registered";

/// Who is asking, for the endpoints where that is a role rather than a
/// person. `holder` is filled only when the token names one, which an
/// ordinary service_role key does not: it carries no sub claim and never
/// did.
struct Admin {
    role: String,
    aud: String,
    holder: Option<Caller>,
}

/// GoTrue's requireAdminCredentials, minus the ordering it cannot keep.
///
/// Upstream looks up the session behind the token before it looks at the
/// role, which costs a query on every admin request to catch a case that
/// only arises when a signed in person happens to hold an admin role.
/// Here the role is judged first and the session is judged in the
/// handler, where a connection is open anyway. The two orders differ
/// only for a caller who is both not an admin and logged out, and that
/// caller is refused either way.
fn admin(req: &Request<Body>) -> Result<Admin, Box<Response>> {
    if !req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("Bearer "))
    {
        return Err(Box::new(error_body(
            StatusCode::UNAUTHORIZED,
            "no_authorization",
            "This endpoint requires a valid Bearer token",
        )));
    }
    let Some(ctx) = req.extensions().get::<crate::AuthContext>() else {
        return Err(Box::new(not_allowed()));
    };
    if ctx.role != "service_role" && ctx.role != "supabase_admin" {
        return Err(Box::new(not_allowed()));
    }
    // A key is not a person, so most admin tokens name nobody. One that
    // does is a signed in account whose role happens to be an admin one,
    // and that account has to still exist and still hold the session it
    // names, the same as anywhere else.
    let holder = match field(&ctx.claims, "sub") {
        "" => None,
        _ => caller(req).ok(),
    };
    Ok(Admin {
        role: ctx.role.clone(),
        aud: field(&ctx.claims, "aud").to_string(),
        holder,
    })
}

fn not_allowed() -> Response {
    error_body(StatusCode::FORBIDDEN, "not_admin", "User not allowed")
}

/// The session check, for the admin tokens that name a person.
async fn holder_still_there(sess: &sql::Session, admin: &Admin) -> Result<(), Error> {
    match &admin.holder {
        Some(caller) => still_there(sess, caller).await,
        None => Ok(()),
    }
}

/// The account a route is about, which upstream loads before the handler
/// runs and refuses in the router's own words.
async fn addressed(sess: &sql::Session, user_id: &str) -> Result<(), Error> {
    if !is_uuid(user_id) {
        // A 404 rather than a 400, which is upstream's, and load bearing
        // for anybody who has matched on it.
        return Err(refused(
            StatusCode::NOT_FOUND,
            "validation_failed",
            "user_id must be an UUID",
        ));
    }
    let rows = sess
        .query(
            "select 1 from auth.users where id = $1::text::uuid",
            &[&user_id],
        )
        .await?;
    if rows.is_empty() {
        return Err(refused(
            StatusCode::NOT_FOUND,
            "user_not_found",
            "User not found",
        ));
    }
    Ok(())
}

/// How the list was asked for: which page, how big, ordered how, and
/// filtered by what.
struct Listing {
    page: u64,
    per_page: u64,
    ascending: bool,
    filter: String,
}

/// Upstream's sort and paginate, in that order, because both can refuse
/// and a request with two bad parameters sees the first of them. Both
/// put the parser's own error in the message, so the Go errors this can
/// produce are spelled out rather than paraphrased.
fn listing(query: &serde_json::Value) -> Result<Listing, Error> {
    let ascending =
        direction(field(query, "sort")).map_err(|e| bad_parameters("Bad Sort Parameters", &e))?;
    let page = number(field(query, "page"), 1)
        .map_err(|e| bad_parameters("Bad Pagination Parameters", &e))?;
    let per_page = number(field(query, "per_page"), PER_PAGE)
        .map_err(|e| bad_parameters("Bad Pagination Parameters", &e))?;
    Ok(Listing {
        page,
        per_page,
        ascending,
        filter: field(query, "filter").to_string(),
    })
}

fn bad_parameters(what: &str, why: &str) -> Error {
    refused(
        StatusCode::BAD_REQUEST,
        "validation_failed",
        &format!("{what}: {why}"),
    )
}

/// A page number, in Go's words when it is not one.
fn number(raw: &str, fallback: u64) -> Result<u64, String> {
    if raw.is_empty() {
        return Ok(fallback);
    }
    if raw.chars().all(|c| c.is_ascii_digit()) {
        if let Ok(n) = raw.parse::<u64>() {
            return Ok(n);
        }
        return Err(format!(
            "strconv.ParseUint: parsing {raw:?}: value out of range"
        ));
    }
    Err(format!(
        "strconv.ParseUint: parsing {raw:?}: invalid syntax"
    ))
}

/// The sort, of which upstream allows exactly one field: created_at,
/// descending unless the request says otherwise. Anything else is a
/// refusal that quotes back what was asked for.
fn direction(raw: &str) -> Result<bool, String> {
    if raw.is_empty() {
        return Ok(false);
    }
    let (name, dir) = match raw.split_once(' ') {
        Some((name, dir)) => (name, dir),
        None => (raw, ""),
    };
    if name != "created_at" {
        return Err(format!("bad field for sort '{name}'"));
    }
    match dir.to_ascii_uppercase().as_str() {
        "" | "DESC" => Ok(false),
        "ASC" => Ok(true),
        _ => Err(format!(
            "bad direction for sort '{dir}', only 'asc' and 'desc' allowed"
        )),
    }
}

/// GET /auth/v1/admin/users.
pub async fn users(
    axum::extract::State(app): axum::extract::State<Arc<crate::App>>,
    req: Request<Body>,
) -> Response {
    let Some(pool) = &app.pool else {
        return no_database();
    };
    let admin = match admin(&req) {
        Ok(admin) => admin,
        Err(res) => return *res,
    };
    let asked = requested_aud(&req, &admin.role, &admin.aud);
    let listing = match listing(&query_object(req.uri().query().unwrap_or_default())) {
        Ok(listing) => listing,
        Err(e) => return refusal(e, "admin users"),
    };
    let sess = match pool.admin().await {
        Ok(sess) => sess,
        Err(e) => return refusal(Error::Db(e), "admin users"),
    };
    let out = list(&sess, &admin, &asked, &listing).await;
    match out {
        Ok((users, total)) => match sess.commit().await {
            Ok(()) => paged(
                req.uri(),
                &listing,
                total,
                json_body(
                    StatusCode::OK,
                    serde_json::json!({"users": users, "aud": asked}),
                ),
            ),
            Err(e) => refusal(Error::Db(e), "admin users"),
        },
        Err(e) => {
            let _ = sess.rollback().await;
            refusal(e, "admin users")
        }
    }
}

/// The page, and how many rows there are in total, which the headers
/// need and the body does not carry.
///
/// One divergence from upstream, deliberately: the identity list is
/// filled in. Upstream answers `"identities": null` here because its ORM
/// does not load the association on this query, and a client that wants
/// them has to fetch each account again. Nothing can be branching on a
/// null that the single account fetch never returns, so this end fills
/// it in.
async fn list(
    sess: &sql::Session,
    admin: &Admin,
    aud: &str,
    listing: &Listing,
) -> Result<(Vec<serde_json::Value>, i64), Error> {
    holder_still_there(sess, admin).await?;
    // Upstream's filter, which is a LIKE on the address and an ILIKE on
    // the one metadata key it knows the name of.
    let matching = "and ($2 = '' or (u.email like '%' || $2 || '%'
                        or u.raw_user_meta_data->>'full_name' ilike '%' || $2 || '%'))";
    let order = match listing.ascending {
        true => "asc",
        false => "desc",
    };
    let offset = listing
        .page
        .saturating_sub(1)
        .saturating_mul(listing.per_page);
    let rows = sess
        .query(
            &format!(
                "select {user}::text
                   from auth.users u {ids}
                  where u.instance_id = '{NOBODY}' and u.aud = $1 {matching}
                  order by u.created_at {order}
                  limit $3 offset $4",
                user = user_object(),
                ids = identities_join(),
            ),
            &[
                &aud,
                &listing.filter,
                &(listing.per_page as i64),
                &(offset as i64),
            ],
        )
        .await?;
    let users = rows
        .iter()
        .map(|row| {
            serde_json::from_str(row.get::<_, &str>(0))
                .expect("jsonb_build_object always produces json")
        })
        .collect();
    let counted = sess
        .query(
            &format!(
                "select count(*) from auth.users u
                  where u.instance_id = '{NOBODY}' and u.aud = $1 {matching}"
            ),
            &[&aud, &listing.filter],
        )
        .await?;
    Ok((users, counted[0].get(0)))
}

/// Upstream's pagination headers: how many rows there are in total, and
/// where the next and the last page are. The link is the request's own
/// path with the page swapped, and its query comes out sorted by key,
/// which is what Go's url.Values.Encode does.
fn paged(uri: &axum::http::Uri, listing: &Listing, total: i64, mut res: Response) -> Response {
    let pages = match listing.per_page {
        0 => 0,
        per => (total as u64).div_ceil(per),
    };
    let mut header = String::new();
    if pages > listing.page {
        header.push_str(&format!(
            "<{}>; rel=\"next\", ",
            with_page(uri, listing.page + 1)
        ));
    }
    header.push_str(&format!("<{}>; rel=\"last\"", with_page(uri, pages)));
    if let Ok(value) = axum::http::HeaderValue::from_str(&header) {
        res.headers_mut().insert("link", value);
    }
    if let Ok(value) = axum::http::HeaderValue::from_str(&total.to_string()) {
        res.headers_mut().insert("x-total-count", value);
    }
    res
}

fn with_page(uri: &axum::http::Uri, page: u64) -> String {
    let mut pairs: Vec<(String, String)> = uri
        .query()
        .unwrap_or_default()
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| match pair.split_once('=') {
            Some((key, value)) => (key.to_string(), value.to_string()),
            None => (pair.to_string(), String::new()),
        })
        .filter(|(key, _)| key != "page")
        .collect();
    pairs.push(("page".to_string(), page.to_string()));
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    let query: Vec<String> = pairs
        .iter()
        .map(|(key, value)| format!("{}={}", query_escape(key), query_escape(value)))
        .collect();
    format!("{}?{}", uri.path(), query.join("&"))
}

/// GET /auth/v1/admin/users/{user_id}.
pub async fn user_get(
    axum::extract::State(app): axum::extract::State<Arc<crate::App>>,
    axum::extract::Path(user_id): axum::extract::Path<String>,
    req: Request<Body>,
) -> Response {
    let Some(pool) = &app.pool else {
        return no_database();
    };
    let admin = match admin(&req) {
        Ok(admin) => admin,
        Err(res) => return *res,
    };
    let sess = match pool.admin().await {
        Ok(sess) => sess,
        Err(e) => return refusal(Error::Db(e), "admin user get"),
    };
    let out = read(&sess, &admin, &user_id).await;
    finish(sess, out, "admin user get").await
}

async fn read(
    sess: &sql::Session,
    admin: &Admin,
    user_id: &str,
) -> Result<serde_json::Value, Error> {
    holder_still_there(sess, admin).await?;
    addressed(sess, user_id).await?;
    Ok(user_json(sess, user_id).await?)
}

/// POST /auth/v1/admin/users, an account made by somebody other than the
/// person it belongs to.
pub async fn user_create(
    axum::extract::State(app): axum::extract::State<Arc<crate::App>>,
    req: Request<Body>,
) -> Response {
    let Some(pool) = &app.pool else {
        return no_database();
    };
    let admin = match admin(&req) {
        Ok(admin) => admin,
        Err(res) => return *res,
    };
    let asked = requested_aud(&req, &admin.role, &admin.aud);
    let body = match read_json(req.into_body()).await {
        Ok(body) => body,
        Err(res) => return res,
    };
    let aud = match field(&body, "aud") {
        "" => asked,
        given => given.to_string(),
    };
    let sess = match pool.admin().await {
        Ok(sess) => sess,
        Err(e) => return refusal(Error::Db(e), "admin user create"),
    };
    let out = create(&sess, &admin, &body, &aud).await;
    finish(sess, out, "admin user create").await
}

/// The password as it will be stored: the hash of the one that was sent,
/// the hash that was sent, or the hash of one nobody will ever know.
/// Upstream generates that last one rather than leaving the column
/// empty, so an account made with no password cannot be signed in to
/// until somebody sets one.
async fn secret(body: &serde_json::Value) -> Result<String, Error> {
    let password = field(body, "password");
    let given = field(body, "password_hash");
    if !password.is_empty() && !given.is_empty() {
        return denied(
            "validation_failed",
            "Only a password or a password hash should be provided",
        );
    }
    if !given.is_empty() {
        return Ok(given.to_string());
    }
    if password.is_empty() {
        return Ok(hash_off_thread(&unguessable_password()).await);
    }
    validate_password(password)?;
    Ok(hash_off_thread(password).await)
}

async fn create(
    sess: &sql::Session,
    admin: &Admin,
    body: &serde_json::Value,
    aud: &str,
) -> Result<serde_json::Value, Error> {
    holder_still_there(sess, admin).await?;
    let (email, phone) = (field(body, "email"), field(body, "phone"));
    if email.is_empty() && phone.is_empty() {
        return denied(
            "validation_failed",
            "Cannot create a user without either an email or phone",
        );
    }
    let email = validate_email(email)?;
    if taken(sess, &email, NOBODY).await? {
        return Err(refused(
            StatusCode::UNPROCESSABLE_ENTITY,
            "email_exists",
            DUPLICATE_EMAIL,
        ));
    }
    if !phone.is_empty() {
        return Err(Error::NotYet("creating a user with a phone number"));
    }
    let hash = secret(body).await?;
    let id = match field(body, "id") {
        "" => None,
        given if !is_uuid(given) => {
            return denied("validation_failed", "ID must conform to the uuid v4 format");
        }
        given if given == NOBODY => {
            return denied("validation_failed", "ID cannot be a nil uuid");
        }
        given => Some(given.to_string()),
    };
    let ban = ban_seconds(field(body, "ban_duration"))?;
    let role = match field(body, "role") {
        "" => DEFAULT_ROLE,
        given => given,
    };
    let confirm = flag(body, "email_confirm");
    let user_data = match object(body, "user_metadata") {
        serde_json::Value::Null => serde_json::json!({}),
        data => data,
    };
    let user_id = insert_account(
        sess,
        &NewAccount {
            email: &email,
            aud,
            hash: &hash,
            data: &user_data,
            role,
            id,
        },
    )
    .await?;
    // The identity is what says the account belongs to the email
    // provider. It goes in unverified whatever the request said, and
    // email_confirm below is what verifies it, which is the order
    // upstream writes them in too.
    insert_identity(sess, &user_id, &email).await?;
    let app_data = object(body, "app_metadata");
    if !app_data.is_null() {
        merge_metadata(sess, &user_id, "raw_app_meta_data", &app_data).await?;
    }
    if confirm {
        confirm_address(sess, &user_id, false).await?;
    }
    ban_account(sess, &user_id, ban).await?;
    Ok(user_json(sess, &user_id).await?)
}

/// An account about to be written, which three of these endpoints make:
/// the create, the invite, and the link generated for an address nobody
/// has signed up with yet.
struct NewAccount<'a> {
    email: &'a str,
    aud: &'a str,
    /// The stored password, which for an invited account is empty,
    /// because there is nothing to sign in with until the invitation is
    /// followed.
    hash: &'a str,
    data: &'a serde_json::Value,
    role: &'a str,
    /// The id the request asked for, when it asked for one.
    id: Option<String>,
}

async fn insert_account(sess: &sql::Session, new: &NewAccount<'_>) -> Result<String, sql::Error> {
    let rows = sess
        .query(
            "insert into auth.users
                 (instance_id, id, aud, role, email, encrypted_password,
                  raw_app_meta_data, raw_user_meta_data,
                  confirmation_token, recovery_token, email_change_token_current,
                  email_change_token_new, email_change, phone_change, phone_change_token,
                  reauthentication_token, created_at, updated_at,
                  is_anonymous, is_sso_user)
             values ($7::text::uuid,
                     coalesce($6::text::uuid, gen_random_uuid()),
                     $2, $5, $1, $3, jsonb_build_object('provider', 'email',
                                                        'providers',
                                                        jsonb_build_array('email')),
                     $4::jsonb, '', '', '', '', '', '', '', '', now(), now(), false, false)
             returning id::text",
            &[
                &new.email, &new.aud, &new.hash, &new.data, &new.role, &new.id, &NOBODY,
            ],
        )
        .await?;
    Ok(rows[0].get(0))
}

/// The email identity, unverified, and only when the account has none.
async fn insert_identity(
    sess: &sql::Session,
    user_id: &str,
    email: &str,
) -> Result<(), sql::Error> {
    sess.execute(
        "insert into auth.identities
             (provider_id, user_id, identity_data, provider,
              last_sign_in_at, created_at, updated_at)
         select $1::text::uuid::text, $1::text::uuid,
                jsonb_build_object('sub', $1::text, 'email', $2::text,
                                   'email_verified', false, 'phone_verified', false),
                'email', now(), now(), now()
          where not exists (select 1 from auth.identities
                             where user_id = $1::text::uuid and provider = 'email')",
        &[&user_id, &email],
    )
    .await?;
    Ok(())
}

/// PUT /auth/v1/admin/users/{user_id}.
pub async fn user_update(
    axum::extract::State(app): axum::extract::State<Arc<crate::App>>,
    axum::extract::Path(user_id): axum::extract::Path<String>,
    req: Request<Body>,
) -> Response {
    let Some(pool) = &app.pool else {
        return no_database();
    };
    let admin = match admin(&req) {
        Ok(admin) => admin,
        Err(res) => return *res,
    };
    let body = match read_json(req.into_body()).await {
        Ok(body) => body,
        Err(res) => return res,
    };
    let sess = match pool.admin().await {
        Ok(sess) => sess,
        Err(e) => return refusal(Error::Db(e), "admin user update"),
    };
    let out = amend(&sess, &admin, &user_id, &body).await;
    finish(sess, out, "admin user update").await
}

/// Change somebody else's account, which is every field at once and no
/// proof asked for.
///
/// The difference from PUT /user is not the fields but the ceremony
/// around them. A person changing their own address is mailed a link and
/// the change waits for it; an admin changing it with email_confirm has
/// it changed there and then, because the admin is the one asserting the
/// address and there is nobody to mail a link to who is more trusted
/// than the caller already is.
async fn amend(
    sess: &sql::Session,
    admin: &Admin,
    user_id: &str,
    body: &serde_json::Value,
) -> Result<serde_json::Value, Error> {
    holder_still_there(sess, admin).await?;
    addressed(sess, user_id).await?;
    let email = match field(body, "email") {
        "" => String::new(),
        given => validate_email(given)?,
    };
    if !field(body, "phone").is_empty() {
        return Err(Error::NotYet("changing somebody else's phone number"));
    }
    let ban = ban_seconds(field(body, "ban_duration"))?;
    let hash = match body.get("password").and_then(|v| v.as_str()) {
        None => None,
        Some(password) => {
            validate_password(password)?;
            Some(hash_off_thread(password).await)
        }
    };
    let confirm = flag(body, "email_confirm");
    if let Some(role) = body
        .get("role")
        .and_then(|v| v.as_str())
        .filter(|role| !role.is_empty())
    {
        sess.execute(
            "update auth.users set role = $2, updated_at = now() where id = $1::text::uuid",
            &[&user_id, &role.trim()],
        )
        .await?;
    }
    if confirm {
        confirm_address(sess, user_id, false).await?;
    }
    if let Some(hash) = &hash {
        // Upstream's UpdatePassword throws away every session on the
        // account, and this end does the same, because a password
        // changed by an admin is usually a password being taken away
        // from whoever had it.
        sess.execute(
            "update auth.users
                set encrypted_password = $2, updated_at = now()
              where id = $1::text::uuid",
            &[&user_id, hash],
        )
        .await?;
        sess.execute(
            "delete from auth.sessions where user_id = $1::text::uuid",
            &[&user_id],
        )
        .await?;
    }
    if !email.is_empty() {
        move_address(sess, user_id, &email, confirm).await?;
    }
    let app_data = object(body, "app_metadata");
    if !app_data.is_null() {
        merge_metadata(sess, user_id, "raw_app_meta_data", &app_data).await?;
    }
    let user_data = object(body, "user_metadata");
    if !user_data.is_null() {
        merge_metadata(sess, user_id, "raw_user_meta_data", &user_data).await?;
    }
    ban_account(sess, user_id, ban).await?;
    Ok(user_json(sess, user_id).await?)
}

/// Put the address on the account and on its email identity, making the
/// identity when there is none, which is what an account that signed in
/// through a provider or signed in as nobody has.
async fn move_address(
    sess: &sql::Session,
    user_id: &str,
    email: &str,
    confirmed: bool,
) -> Result<(), Error> {
    sess.execute(
        "insert into auth.identities
             (provider_id, user_id, identity_data, provider,
              last_sign_in_at, created_at, updated_at)
         select $1::text::uuid::text, $1::text::uuid,
                jsonb_build_object('sub', $1::text, 'email', $2::text,
                                   'email_verified', $3::bool, 'phone_verified', false),
                'email', now(), now(), now()
          where not exists (select 1 from auth.identities
                             where user_id = $1::text::uuid and provider = 'email')",
        &[&user_id, &email, &confirmed],
    )
    .await?;
    sess.execute(
        "update auth.identities
            set identity_data = identity_data
                                || jsonb_build_object('email', $2::text,
                                                      'email_verified', $3::bool),
                updated_at = now()
          where user_id = $1::text::uuid and provider = 'email'",
        &[&user_id, &email, &confirmed],
    )
    .await?;
    // An account nobody vouched for stops being one, but only once
    // somebody has vouched for the address it is taking.
    sess.execute(
        "update auth.users
            set email = $2,
                is_anonymous = case when $3::bool then false else is_anonymous end,
                updated_at = now()
          where id = $1::text::uuid",
        &[&user_id, &email, &confirmed],
    )
    .await?;
    Ok(())
}

/// Ban the account for a while, or lift the ban. A duration of `none`
/// lifts it, which is upstream's rule and the only way its clients have
/// of undoing one.
async fn ban_account(sess: &sql::Session, user_id: &str, ban: Option<f64>) -> Result<(), Error> {
    let Some(seconds) = ban else {
        return Ok(());
    };
    if seconds == 0.0 {
        sess.execute(
            "update auth.users set banned_until = null, updated_at = now()
              where id = $1::text::uuid",
            &[&user_id],
        )
        .await?;
        return Ok(());
    }
    sess.execute(
        "update auth.users
            set banned_until = now() + make_interval(secs => $2::double precision),
                updated_at = now()
          where id = $1::text::uuid",
        &[&user_id, &seconds],
    )
    .await?;
    Ok(())
}

/// How long a ban lasts, in seconds, or None when the request said
/// nothing about banning at all. Zero is a ban being lifted.
fn ban_seconds(raw: &str) -> Result<Option<f64>, Error> {
    if raw.is_empty() {
        return Ok(None);
    }
    if raw == "none" {
        return Ok(Some(0.0));
    }
    match go_duration(raw) {
        Some(seconds) => Ok(Some(seconds)),
        None => denied(
            "validation_failed",
            &format!("invalid format for ban duration: time: invalid duration {raw:?}"),
        ),
    }
}

/// Go's time.ParseDuration, enough of it to read what a client sends: a
/// run of number and unit pairs, `24h`, `1h30m`, `1.5h`, with the units
/// Go names. Anything else is not a duration, which is the only other
/// answer this has to give, because the caller turns it into upstream's
/// refusal.
fn go_duration(raw: &str) -> Option<f64> {
    let (sign, rest) = match raw.strip_prefix('-') {
        Some(rest) => (-1.0, rest),
        None => (1.0, raw.strip_prefix('+').unwrap_or(raw)),
    };
    if rest == "0" {
        return Some(0.0);
    }
    if rest.is_empty() {
        return None;
    }
    let mut seconds = 0f64;
    let mut left = rest;
    while !left.is_empty() {
        let digits = left
            .find(|c: char| !c.is_ascii_digit() && c != '.')
            .unwrap_or(left.len());
        if digits == 0 {
            return None;
        }
        let value: f64 = left[..digits].parse().ok()?;
        left = &left[digits..];
        let unit = left
            .find(|c: char| c.is_ascii_digit())
            .unwrap_or(left.len());
        let scale = match &left[..unit] {
            "ns" => 1e-9,
            "us" | "\u{b5}s" => 1e-6,
            "ms" => 1e-3,
            "s" => 1.0,
            "m" => 60.0,
            "h" => 3600.0,
            _ => return None,
        };
        left = &left[unit..];
        seconds += value * scale;
    }
    Some(sign * seconds)
}

/// DELETE /auth/v1/admin/users/{user_id}.
pub async fn user_delete(
    axum::extract::State(app): axum::extract::State<Arc<crate::App>>,
    axum::extract::Path(user_id): axum::extract::Path<String>,
    req: Request<Body>,
) -> Response {
    let Some(pool) = &app.pool else {
        return no_database();
    };
    let admin = match admin(&req) {
        Ok(admin) => admin,
        Err(res) => return *res,
    };
    // Upstream reads the body only when there is one, so a delete sent
    // with no body at all is a hard delete rather than a parse error.
    let body = match read_json(req.into_body()).await {
        Ok(body) => body,
        Err(_) => serde_json::json!({}),
    };
    let soft = flag(&body, "should_soft_delete");
    let sess = match pool.admin().await {
        Ok(sess) => sess,
        Err(e) => return refusal(Error::Db(e), "admin user delete"),
    };
    let out = remove(&sess, &admin, &user_id, soft).await;
    finish(sess, out, "admin user delete").await
}

/// Delete the account, or leave a tombstone where it was.
///
/// A soft delete keeps the row and takes out of it everything that
/// identifies anybody: the address and the number are replaced by a hash
/// of themselves, which is one way, so the row can still be recognised
/// as having held that address by somebody who already knows the
/// address, and by nobody else. What it buys is a foreign key that still
/// resolves, which is why an application with its own rows pointing at
/// auth.users wants this one.
async fn remove(
    sess: &sql::Session,
    admin: &Admin,
    user_id: &str,
    soft: bool,
) -> Result<serde_json::Value, Error> {
    holder_still_there(sess, admin).await?;
    addressed(sess, user_id).await?;
    if !soft {
        sess.execute(
            "delete from auth.users where id = $1::text::uuid",
            &[&user_id],
        )
        .await?;
        return Ok(serde_json::json!({}));
    }
    // Already a tombstone, so there is nothing left to take out of it
    // and nothing to say about that.
    let rows = sess
        .query(
            "select coalesce(email, ''), coalesce(phone, ''),
                    coalesce(email_change, ''), coalesce(phone_change, '')
               from auth.users
              where id = $1::text::uuid and deleted_at is null",
            &[&user_id],
        )
        .await?;
    let Some(row) = rows.first() else {
        return Ok(serde_json::json!({}));
    };
    // The hash is over the id and the value together, so one address on
    // two accounts leaves two different tombstones. The phone columns
    // are cut to fifteen characters because the column used to be
    // varchar(15) and upstream still cuts them.
    let email = obfuscated(user_id, row.get(0));
    let phone: String = obfuscated(user_id, row.get(1)).chars().take(15).collect();
    let email_change = obfuscated(user_id, row.get(2));
    let phone_change: String = obfuscated(user_id, row.get(3)).chars().take(15).collect();
    sess.execute(
        "update auth.users
            set email = $2, phone = $3, email_change = $4, phone_change = $5,
                encrypted_password = null,
                confirmation_token = '',
                recovery_token = '',
                email_change_token_current = '',
                email_change_token_new = '',
                phone_change_token = '',
                raw_user_meta_data = '{}'::jsonb,
                raw_app_meta_data = '{}'::jsonb,
                deleted_at = now(),
                updated_at = now()
          where id = $1::text::uuid",
        &[&user_id, &email, &phone, &email_change, &phone_change],
    )
    .await?;
    sess.execute(
        "delete from auth.one_time_tokens where user_id = $1::text::uuid",
        &[&user_id],
    )
    .await?;
    // The identity keeps its row and loses everything it said, so an
    // account that is gone cannot be found through the provider id it
    // used to hold either.
    let identities = sess
        .query(
            "select id::text, provider, provider_id
               from auth.identities where user_id = $1::text::uuid",
            &[&user_id],
        )
        .await?;
    for identity in &identities {
        let id: String = identity.get(0);
        let provider: String = identity.get(1);
        let provider_id: String = identity.get(2);
        let hidden = obfuscated(user_id, &format!("{provider}:{provider_id}"));
        sess.execute(
            "update auth.identities
                set identity_data = '{}'::jsonb, provider_id = $2, updated_at = now()
              where id = $1::text::uuid",
            &[&id, &hidden],
        )
        .await?;
    }
    sess.execute(
        "delete from auth.mfa_factors where user_id = $1::text::uuid",
        &[&user_id],
    )
    .await?;
    sess.execute(
        "delete from auth.sessions where user_id = $1::text::uuid",
        &[&user_id],
    )
    .await?;
    Ok(serde_json::json!({}))
}

/// GoTrue's obfuscateValue: the account id and the value hashed
/// together, base64url with no padding.
fn obfuscated(user_id: &str, value: &str) -> String {
    use base64ct::Encoding;
    use sha2::Digest;
    base64ct::Base64UrlUnpadded::encode_string(&sha2::Sha256::digest(
        format!("{user_id}{value}").as_bytes(),
    ))
}

/// An account found by its address, for the two endpoints that are given
/// one rather than an id.
struct Account {
    id: String,
    confirmed: bool,
}

async fn by_address(
    sess: &sql::Session,
    email: &str,
    aud: &str,
) -> Result<Option<Account>, sql::Error> {
    let rows = sess
        .query(
            "select id::text, email_confirmed_at is not null
               from auth.users
              where email = $1 and aud = $2 and is_sso_user = false and deleted_at is null
              limit 1",
            &[&email, &aud],
        )
        .await?;
    Ok(rows.first().map(|row| Account {
        id: row.get(0),
        confirmed: row.get(1),
    }))
}

fn already_registered() -> Error {
    refused(
        StatusCode::UNPROCESSABLE_ENTITY,
        "email_exists",
        DUPLICATE_EMAIL,
    )
}

/// POST /auth/v1/admin/generate_link.
///
/// The whole of an email flow except the email. Everything the ordinary
/// flow writes down gets written down, and then the link that would have
/// been mailed is handed back instead, along with the code inside it. It
/// is what a project uses when it sends its own mail, and what a test
/// suite uses to follow a flow without a mailbox.
///
/// Nothing goes out from here, so none of the send frequency rules apply
/// and none of them are checked, which is upstream's behaviour too: this
/// endpoint is already behind the service role.
pub async fn generate_link(
    axum::extract::State(app): axum::extract::State<Arc<crate::App>>,
    req: Request<Body>,
) -> Response {
    let Some(pool) = &app.pool else {
        return no_database();
    };
    let admin = match admin(&req) {
        Ok(admin) => admin,
        Err(res) => return *res,
    };
    let asked = requested_aud(&req, &admin.role, &admin.aud);
    let (wanted, from) = link_target(&req);
    let body = match read_json(req.into_body()).await {
        Ok(body) => body,
        Err(res) => return res,
    };
    let mut post = posting(&app, &wanted, &from);
    // Upstream reads the redirect from the body here as well as from the
    // query, and the body wins when it names somewhere this project owns.
    post.referrer = landing(&app, field(&body, "redirect_to"), &post.referrer);
    let sess = match pool.admin().await {
        Ok(sess) => sess,
        Err(e) => return refusal(Error::Db(e), "admin generate link"),
    };
    let out = link_for(
        &sess,
        &admin,
        &body,
        &asked,
        &post,
        app.cfg.secure_email_change,
    )
    .await;
    finish(sess, out, "admin generate link").await
}

async fn link_for(
    sess: &sql::Session,
    admin: &Admin,
    body: &serde_json::Value,
    aud: &str,
    post: &Post<'_>,
    secure_change: bool,
) -> Result<serde_json::Value, Error> {
    holder_still_there(sess, admin).await?;
    let email = validate_email(field(body, "email"))?;
    let found = by_address(sess, &email, aud).await?;
    let mut kind = field(body, "type").to_string();
    let mut password = field(body, "password").to_string();
    if found.is_none() {
        match kind.as_str() {
            // A magic link for an address nobody has signed up with is a
            // signup, which is how a project invites somebody without
            // saying so. The password is one nobody will ever know, so
            // the account exists and only the link gets into it.
            "magiclink" => {
                kind = "signup".to_string();
                password = unguessable_password();
            }
            "recovery" | "email_change_current" | "email_change_new" => {
                return Err(refused(
                    StatusCode::NOT_FOUND,
                    "user_not_found",
                    "User with this email not found",
                ));
            }
            _ => {}
        }
    }
    let otp = six_digits();
    let hashed = token_hash(&email, &otp);
    let (user_id, carried) = match kind.as_str() {
        "magiclink" | "recovery" => {
            let account = found.ok_or_else(|| {
                refused(
                    StatusCode::NOT_FOUND,
                    "user_not_found",
                    "User with this email not found",
                )
            })?;
            sess.execute(
                "update auth.users
                    set recovery_token = $2, recovery_sent_at = now(), updated_at = now()
                  where id = $1::text::uuid",
                &[&account.id, &hashed],
            )
            .await?;
            keep_token(sess, &account.id, "recovery_token", &hashed, &email).await?;
            (account.id, hashed.clone())
        }
        "invite" => {
            let user_id = match found {
                Some(account) if account.confirmed => return Err(already_registered()),
                Some(account) => account.id,
                None => new_account(sess, &email, aud, "", &object_or_empty(body, "data")).await?,
            };
            invited(sess, &user_id, &email, &hashed).await?;
            (user_id, hashed.clone())
        }
        "signup" => {
            let user_id = match found {
                Some(account) if account.confirmed => return Err(already_registered()),
                Some(account) => {
                    let data = object(body, "data");
                    if !data.is_null() {
                        merge_metadata(sess, &account.id, "raw_user_meta_data", &data).await?;
                    }
                    account.id
                }
                None => {
                    validate_password(&password)?;
                    let hash = hash_off_thread(&password).await;
                    new_account(sess, &email, aud, &hash, &object_or_empty(body, "data")).await?
                }
            };
            awaiting_confirmation(sess, &user_id, &email, &hashed).await?;
            (user_id, hashed.clone())
        }
        "email_change_current" | "email_change_new" => {
            let account = found.ok_or_else(|| {
                refused(
                    StatusCode::NOT_FOUND,
                    "user_not_found",
                    "User with this email not found",
                )
            })?;
            if !secure_change && kind == "email_change_current" {
                return denied(
                    "validation_failed",
                    "Enable secure email change to generate link for current email",
                );
            }
            let new = validate_email(field(body, "new_email"))?;
            if taken(sess, &new, &account.id).await? {
                return Err(already_registered());
            }
            // The code the new address is sent is hashed against the new
            // address, because that is where it will be typed in.
            let for_new = token_hash(&new, &otp);
            staged_change(sess, &account.id, &kind, &new, &hashed, &for_new).await?;
            let carried = match kind.as_str() {
                "email_change_new" => for_new,
                _ => hashed.clone(),
            };
            (account.id, carried)
        }
        other => {
            return denied(
                "validation_failed",
                &format!("Invalid email action link type requested: {other}"),
            );
        }
    };
    let mut out = user_json(sess, &user_id).await?;
    let link = crate::mail::action_link(
        &post.external,
        post.settings.path(template_of(&kind)),
        carried_type(&kind),
        &carried,
        &post.referrer,
    );
    if let Some(out) = out.as_object_mut() {
        out.insert("action_link".to_string(), link.into());
        out.insert("email_otp".to_string(), otp.into());
        // The hash of the code against the address the request named,
        // which for a change to a new address is not the hash the link
        // carries. That is upstream's, and a client that wants to verify
        // the code itself uses the address it sent with it.
        out.insert("hashed_token".to_string(), hashed.into());
        out.insert("verification_type".to_string(), kind.into());
        out.insert("redirect_to".to_string(), post.referrer.clone().into());
    }
    Ok(out)
}

/// Which template's configured path the link points at, which is not
/// always the template that would have carried it: a magic link is sent
/// under the recovery path, the way upstream sends it.
fn template_of(kind: &str) -> &'static str {
    match kind {
        "invite" => crate::mail::INVITE,
        "recovery" => crate::mail::RECOVERY,
        "magiclink" => crate::mail::MAGIC_LINK,
        "email_change_current" | "email_change_new" => crate::mail::EMAIL_CHANGE,
        _ => crate::mail::CONFIRMATION,
    }
}

/// The type the link carries, which verify branches on. Both halves of a
/// change of address are one type there, because verify tells them apart
/// by which column the code is in.
fn carried_type(kind: &str) -> &str {
    match kind {
        "email_change_current" | "email_change_new" => "email_change",
        other => other,
    }
}

/// An account with no session and nothing signed in to it, which is what
/// an invitation and a generated signup link both leave behind.
async fn new_account(
    sess: &sql::Session,
    email: &str,
    aud: &str,
    hash: &str,
    data: &serde_json::Value,
) -> Result<String, Error> {
    let user_id = insert_account(
        sess,
        &NewAccount {
            email,
            aud,
            hash,
            data,
            role: DEFAULT_ROLE,
            id: None,
        },
    )
    .await?;
    insert_identity(sess, &user_id, email).await?;
    Ok(user_id)
}

/// Write down the code an invitation is followed with, and the fact that
/// the account was invited rather than signed up for.
async fn invited(
    sess: &sql::Session,
    user_id: &str,
    email: &str,
    hashed: &str,
) -> Result<(), Error> {
    sess.execute(
        "update auth.users
            set confirmation_token = $2, confirmation_sent_at = now(),
                invited_at = now(), updated_at = now()
          where id = $1::text::uuid",
        &[&user_id, &hashed],
    )
    .await?;
    keep_token(sess, user_id, "confirmation_token", hashed, email).await?;
    Ok(())
}

/// The same, for a signup that is waiting on its confirmation.
async fn awaiting_confirmation(
    sess: &sql::Session,
    user_id: &str,
    email: &str,
    hashed: &str,
) -> Result<(), Error> {
    sess.execute(
        "update auth.users
            set confirmation_token = $2, confirmation_sent_at = now(), updated_at = now()
          where id = $1::text::uuid",
        &[&user_id, &hashed],
    )
    .await?;
    keep_token(sess, user_id, "confirmation_token", hashed, email).await?;
    Ok(())
}

/// Put a change of address in flight with only the half of it this link
/// is for. The other column is left as it stands, so a project that
/// generates both links one after the other ends up with both codes
/// live, which is what a secure change of address needs.
async fn staged_change(
    sess: &sql::Session,
    user_id: &str,
    kind: &str,
    new: &str,
    for_current: &str,
    for_new: &str,
) -> Result<(), Error> {
    let column = match kind {
        "email_change_new" => "email_change_token_new",
        _ => "email_change_token_current",
    };
    let hashed = match kind {
        "email_change_new" => for_new,
        _ => for_current,
    };
    sess.execute(
        &format!(
            "update auth.users
                set {column} = $2,
                    email_change = $3,
                    email_change_confirm_status = 0,
                    email_change_sent_at = now(),
                    updated_at = now()
              where id = $1::text::uuid"
        ),
        &[&user_id, &hashed, &new],
    )
    .await?;
    // Both live codes are indexed, not just the one this call wrote,
    // because the column the other one is in was left alone and the
    // index is what verify looks in.
    let rows = sess
        .query(
            "select email_change_token_current, email_change_token_new, coalesce(email, '')
               from auth.users where id = $1::text::uuid",
            &[&user_id],
        )
        .await?;
    let Some(row) = rows.first() else {
        return Ok(());
    };
    let (current, fresh, address): (String, String, String) = (row.get(0), row.get(1), row.get(2));
    if !current.is_empty() {
        keep_token(
            sess,
            user_id,
            "email_change_token_current",
            &current,
            &address,
        )
        .await?;
    }
    if !fresh.is_empty() {
        keep_token(sess, user_id, "email_change_token_new", &fresh, new).await?;
    }
    Ok(())
}

/// POST /auth/v1/invite.
///
/// An account made for somebody who has not asked for one, with a link
/// in the post inviting them to take it. It is not under `/admin` in
/// upstream's routing and it is not here either, but it is behind the
/// same service role, because inviting somebody is making an account in
/// their name.
pub async fn invite(
    axum::extract::State(app): axum::extract::State<Arc<crate::App>>,
    req: Request<Body>,
) -> Response {
    let Some(pool) = &app.pool else {
        return no_database();
    };
    let admin = match admin(&req) {
        Ok(admin) => admin,
        Err(res) => return *res,
    };
    let asked = requested_aud(&req, &admin.role, &admin.aud);
    let (wanted, from) = link_target(&req);
    let body = match read_json(req.into_body()).await {
        Ok(body) => body,
        Err(res) => return res,
    };
    let post = posting(&app, &wanted, &from);
    let sess = match pool.admin().await {
        Ok(sess) => sess,
        Err(e) => return refusal(Error::Db(e), "invite"),
    };
    let out = inviting(&sess, &admin, &body, &asked, &post).await;
    finish(sess, out, "invite").await
}

async fn inviting(
    sess: &sql::Session,
    admin: &Admin,
    body: &serde_json::Value,
    aud: &str,
    post: &Post<'_>,
) -> Result<serde_json::Value, Error> {
    holder_still_there(sess, admin).await?;
    let email = validate_email(field(body, "email"))?;
    let user_id = match by_address(sess, &email, aud).await? {
        // Somebody who has already proved they hold the address cannot
        // be invited to it, and the refusal is the one a signup on a
        // taken address gets.
        Some(account) if account.confirmed => return Err(already_registered()),
        // An account that never confirmed is invited as it stands, so
        // the invitation replaces whatever code it was holding rather
        // than making a second account on the same address.
        Some(account) => account.id,
        // No password at all, which is what upstream writes: until the
        // invitation is followed and a password set, there is nothing to
        // sign in with.
        None => new_account(sess, &email, aud, "", &object_or_empty(body, "data")).await?,
    };
    let otp = six_digits();
    let code = Code {
        hash: token_hash(&email, &otp),
        code: otp,
    };
    invited(sess, &user_id, &email, &code.hash).await?;
    send_code(
        sess,
        post,
        &user_id,
        Outgoing {
            template: crate::mail::INVITE,
            kind: "invite",
            to: &email,
            code: &code,
            new_email: "",
        },
    )
    .await?;
    Ok(user_json(sess, &user_id).await?)
}

/// One of the two metadata objects on a request, or null when it was not
/// sent at all, which is the difference between leaving the column alone
/// and merging an empty object into it.
fn object(body: &serde_json::Value, key: &str) -> serde_json::Value {
    match body.get(key) {
        Some(value) if value.is_object() => value.clone(),
        _ => serde_json::Value::Null,
    }
}

/// The same, for the endpoints that write the metadata out rather than
/// merging it, where nothing sent is an empty object.
fn object_or_empty(body: &serde_json::Value, key: &str) -> serde_json::Value {
    match object(body, key) {
        serde_json::Value::Null => serde_json::json!({}),
        data => data,
    }
}

fn flag(body: &serde_json::Value, key: &str) -> bool {
    body.get(key).and_then(|v| v.as_bool()).unwrap_or(false)
}

/// Commit or roll back, and answer. Every admin handler but the list
/// ends this way.
async fn finish(
    sess: sql::Session,
    out: Result<serde_json::Value, Error>,
    doing: &str,
) -> Response {
    match out {
        Ok(body) => match sess.commit().await {
            Ok(()) => json_body(StatusCode::OK, body),
            Err(e) => refusal(Error::Db(e), doing),
        },
        Err(e) => {
            let _ = sess.rollback().await;
            refusal(e, doing)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_duration_is_read_the_way_go_reads_it() {
        assert_eq!(go_duration("24h"), Some(86_400.0));
        assert_eq!(go_duration("1h30m"), Some(5_400.0));
        assert_eq!(go_duration("1.5h"), Some(5_400.0));
        assert_eq!(go_duration("-1h"), Some(-3_600.0));
        assert_eq!(go_duration("0"), Some(0.0));
        assert_eq!(go_duration("2h45m30s"), Some(9_930.0));
        assert_eq!(go_duration("300ms"), Some(0.3));
    }

    #[test]
    fn anything_that_is_not_a_duration_is_refused() {
        assert_eq!(go_duration("forever"), None);
        assert_eq!(go_duration("24"), None);
        assert_eq!(go_duration("24q"), None);
        assert_eq!(go_duration(""), None);
        assert_eq!(go_duration("h"), None);
    }

    #[test]
    fn the_sort_allows_one_field_in_two_directions() {
        assert_eq!(direction(""), Ok(false));
        assert_eq!(direction("created_at"), Ok(false));
        assert_eq!(direction("created_at asc"), Ok(true));
        assert_eq!(direction("created_at DESC"), Ok(false));
        assert_eq!(
            direction("email asc"),
            Err("bad field for sort 'email'".to_string())
        );
        assert_eq!(
            direction("created_at sideways"),
            Err("bad direction for sort 'sideways', only 'asc' and 'desc' allowed".to_string())
        );
    }

    #[test]
    fn a_page_that_is_not_a_number_is_named_back_in_gos_words() {
        assert_eq!(number("", 1), Ok(1));
        assert_eq!(number("3", 1), Ok(3));
        assert_eq!(
            number("many", 1),
            Err("strconv.ParseUint: parsing \"many\": invalid syntax".to_string())
        );
        assert_eq!(
            number("-1", 1),
            Err("strconv.ParseUint: parsing \"-1\": invalid syntax".to_string())
        );
    }

    #[test]
    fn the_next_and_last_links_carry_the_rest_of_the_query() {
        let uri: axum::http::Uri = "/auth/v1/admin/users?per_page=2&page=1".parse().unwrap();
        assert_eq!(with_page(&uri, 2), "/auth/v1/admin/users?page=2&per_page=2");
    }
}
