//! Sessions and access tokens, the GoTrue half of the auth surface.
//!
//! A session is three rows: one in auth.sessions, one in
//! auth.mfa_amr_claims recording how the user proved who they are, and
//! one in auth.refresh_tokens. The access token is signed from those
//! rows rather than from anything held in memory, so a token outlives
//! nothing: revoke the session and the next refresh fails.
//!
//! The claim set is GoTrue's, field for field, because it is the
//! contract every Supabase client and every RLS policy already reads.
//! `sub`, `role` and `email` are what auth.uid(), auth.role() and
//! auth.email() return once the token comes back through the gate, and
//! `session_id` is what a logout has to match.
//!
//! Refresh follows GoTrue's rotation: every use revokes the token it
//! was given and issues a child pointing at it. Presenting a revoked
//! token is either a client that lost the response to its last refresh,
//! which is allowed and answered with the token that was already
//! issued, or a stolen token, which revokes every token in the session
//! and refuses. The two are told apart by parentage, not by guessing.

use std::sync::Arc;

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use axum::response::Response;

use crate::sql::{self, Pool};
use crate::{App, json_body, not_yet};

/// GoTrue's JWT_EXP default, the lifetime of an access token.
const ACCESS_TTL: i64 = 3600;

/// GoTrue's GOTRUE_SECURITY_REFRESH_TOKEN_REUSE_INTERVAL, which has no
/// default and therefore is zero: a revoked token that is not the
/// parent of the active one is refused the moment it is presented.
/// Raising it here is the same knob, a grace window for clients that
/// send two refreshes at once.
const REUSE_INTERVAL: i64 = 0;

/// GoTrue's default audience, and the value of the `aud` claim for
/// every user it creates.
pub const AUD: &str = "authenticated";

/// A minted session: the token pair, when the access token dies, and
/// the user as the client expects to see it.
pub struct Issued {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_in: i64,
    pub expires_at: i64,
    pub user: serde_json::Value,
}

impl Issued {
    /// GoTrue's AccessTokenResponse, the body supabase-js parses into a
    /// Session.
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "access_token": self.access_token,
            "token_type": "bearer",
            "expires_in": self.expires_in,
            "expires_at": self.expires_at,
            "refresh_token": self.refresh_token,
            "user": self.user,
        })
    }
}

/// Why a grant failed. `Denied` carries GoTrue's own error_code and
/// message so a client that branches on either sees what it would see
/// against hosted Supabase.
#[derive(Debug)]
pub enum Error {
    Db(sql::Error),
    Denied {
        code: &'static str,
        msg: &'static str,
    },
}

impl From<sql::Error> for Error {
    fn from(e: sql::Error) -> Error {
        Error::Db(e)
    }
}

fn denied<T>(code: &'static str, msg: &'static str) -> Result<T, Error> {
    Err(Error::Denied { code, msg })
}

/// Seconds since the epoch.
fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// A refresh token, shaped like GoTrue's: twelve lowercase base32
/// characters over eight bytes from the os rng. The value is opaque,
/// it is only ever compared against the column it was stored in, so
/// what matters is that it cannot be guessed and fits varchar(255).
fn fresh_token() -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz234567";
    let mut raw = [0u8; 8];
    getrandom::fill(&mut raw).expect("the os rng never fails");
    let mut bits = 0u32;
    let mut held = 0u32;
    let mut out = String::with_capacity(12);
    for byte in raw {
        bits = (bits << 8) | byte as u32;
        held += 8;
        while held >= 5 {
            held -= 5;
            let index = (bits >> held) & 0x1f;
            out.push(ALPHABET[index as usize] as char);
        }
    }
    out.truncate(12);
    out
}

/// A timestamptz as RFC 3339 in UTC, the format Go marshals time.Time
/// into and therefore the one every Supabase client has parsed since
/// the beginning.
fn ts(col: &str) -> String {
    format!(r#"to_char({col} at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')"#)
}

/// A key that is present only when the column is, which is what Go's
/// omitempty leaves out of the json for a nil timestamp.
fn opt_ts(key: &str, col: &str) -> String {
    format!(
        "case when {col} is null then '{{}}'::jsonb
              else jsonb_build_object('{key}', {}) end",
        ts(col)
    )
}

/// The claims and the user, both built in one query so a session is
/// described by the database rather than by whatever the caller
/// happened to have in hand.
///
/// The claim set is GoTrue's: the registered claims, the identity
/// fields an RLS policy reads through auth.jwt(), and the session
/// fields a client needs to reason about its own session. `aal` and
/// `amr` come from the session and its amr rows, so a session that
/// later passes MFA describes itself correctly without this query
/// changing.
async fn describe(
    sess: &sql::Session,
    session_id: &str,
    iat: i64,
    exp: i64,
    issuer: &str,
) -> Result<(serde_json::Value, serde_json::Value), sql::Error> {
    let sql = format!(
        "select jsonb_build_object(
                    'iss', $2::text,
                    'sub', u.id::text,
                    'aud', coalesce(u.aud, ''),
                    'iat', $3::bigint,
                    'exp', $4::bigint,
                    'role', coalesce(u.role, ''),
                    'email', coalesce(u.email, ''),
                    'phone', coalesce(u.phone, ''),
                    'app_metadata', coalesce(u.raw_app_meta_data, '{{}}'::jsonb),
                    'user_metadata', coalesce(u.raw_user_meta_data, '{{}}'::jsonb),
                    'session_id', s.id::text,
                    'aal', coalesce(s.aal::text, 'aal1'),
                    'amr', coalesce(amr.list, '[]'::jsonb),
                    'is_anonymous', u.is_anonymous
                )::text,
                (jsonb_build_object(
                    'id', u.id::text,
                    'aud', coalesce(u.aud, ''),
                    'role', coalesce(u.role, ''),
                    'email', coalesce(u.email, ''),
                    'phone', coalesce(u.phone, ''),
                    'app_metadata', coalesce(u.raw_app_meta_data, '{{}}'::jsonb),
                    'user_metadata', coalesce(u.raw_user_meta_data, '{{}}'::jsonb),
                    'identities', coalesce(ids.list, '[]'::jsonb),
                    'created_at', {created},
                    'updated_at', {updated},
                    'is_anonymous', u.is_anonymous
                ) || {confirmed_at} || {email_confirmed} || {phone_confirmed}
                  || {last_sign_in} || {invited} || {banned} || {deleted})::text
         from auth.sessions s
         join auth.users u on u.id = s.user_id
         left join lateral (
             select jsonb_agg(jsonb_build_object(
                        'method', a.authentication_method,
                        'timestamp', floor(extract(epoch from a.created_at))::bigint
                    ) order by a.created_at) as list
             from auth.mfa_amr_claims a where a.session_id = s.id
         ) amr on true
         left join lateral (
             select jsonb_agg(jsonb_build_object(
                        'identity_id', i.id::text,
                        'id', i.provider_id,
                        'user_id', i.user_id::text,
                        'identity_data', i.identity_data,
                        'provider', i.provider,
                        'created_at', {i_created},
                        'updated_at', {i_updated}
                    ) order by i.created_at) as list
             from auth.identities i where i.user_id = u.id
         ) ids on true
         where s.id = $1::text::uuid",
        created = ts("u.created_at"),
        updated = ts("u.updated_at"),
        i_created = ts("i.created_at"),
        i_updated = ts("i.updated_at"),
        confirmed_at = opt_ts("confirmed_at", "u.confirmed_at"),
        email_confirmed = opt_ts("email_confirmed_at", "u.email_confirmed_at"),
        phone_confirmed = opt_ts("phone_confirmed_at", "u.phone_confirmed_at"),
        last_sign_in = opt_ts("last_sign_in_at", "u.last_sign_in_at"),
        invited = opt_ts("invited_at", "u.invited_at"),
        banned = opt_ts("banned_until", "u.banned_until"),
        deleted = opt_ts("deleted_at", "u.deleted_at"),
    );
    let rows = sess
        .query(&sql, &[&session_id, &issuer, &iat, &exp])
        .await?;
    let claims: serde_json::Value = serde_json::from_str(rows[0].get::<_, &str>(0))
        .expect("jsonb_build_object always produces json");
    let user: serde_json::Value = serde_json::from_str(rows[0].get::<_, &str>(1))
        .expect("jsonb_build_object always produces json");
    Ok((claims, user))
}

/// Sign the access token for a session and describe it, the last step
/// of every grant.
async fn mint_for(
    sess: &sql::Session,
    session_id: &str,
    refresh_token: String,
    signer: &crate::jwt::Signer<'_>,
    issuer: &str,
) -> Result<Issued, Error> {
    let iat = now();
    let exp = iat + ACCESS_TTL;
    let (claims, user) = describe(sess, session_id, iat, exp, issuer).await?;
    Ok(Issued {
        access_token: signer.sign(&claims),
        refresh_token,
        expires_in: ACCESS_TTL,
        expires_at: exp,
        user,
    })
}

/// Start a session for a user who has just proved who they are, and
/// hand back the token pair. `method` is the amr entry, the words
/// GoTrue uses: password, otp, oauth, magiclink, anonymous.
///
/// The caller owns the proof. This end only writes it down, so every
/// flow that lands later, password, otp, oauth, ends up with sessions
/// that look the same to a client and to an RLS policy.
pub async fn issue(
    pool: &Pool,
    user_id: &str,
    method: &str,
    signer: &crate::jwt::Signer<'_>,
    issuer: &str,
) -> Result<Issued, Error> {
    let sess = pool.admin().await?;
    let issued = start(&sess, user_id, method, signer, issuer).await;
    match issued {
        Ok(issued) => {
            sess.commit().await?;
            Ok(issued)
        }
        Err(e) => {
            let _ = sess.rollback().await;
            Err(e)
        }
    }
}

async fn start(
    sess: &sql::Session,
    user_id: &str,
    method: &str,
    signer: &crate::jwt::Signer<'_>,
    issuer: &str,
) -> Result<Issued, Error> {
    let rows = sess
        .query(
            "insert into auth.sessions (id, user_id, created_at, updated_at, refreshed_at, aal)
             values (gen_random_uuid(), $1::text::uuid, now(), now(), now() at time zone 'utc', 'aal1')
             returning id::text",
            &[&user_id],
        )
        .await?;
    let session_id: String = rows[0].get(0);
    sess.execute(
        "insert into auth.mfa_amr_claims
             (id, session_id, created_at, updated_at, authentication_method)
         values (gen_random_uuid(), $1::text::uuid, now(), now(), $2)",
        &[&session_id, &method],
    )
    .await?;
    let token = fresh_token();
    sess.execute(
        "insert into auth.refresh_tokens
             (token, user_id, revoked, created_at, updated_at, parent, session_id)
         values ($1, $2, false, now(), now(), '', $3::text::uuid)",
        &[&token, &user_id, &session_id],
    )
    .await?;
    sess.execute(
        "update auth.users set last_sign_in_at = now(), updated_at = now() where id = $1::text::uuid",
        &[&user_id],
    )
    .await?;
    mint_for(sess, &session_id, token, signer, issuer).await
}

/// What the presented refresh token turned out to be.
struct Presented {
    id: i64,
    token: String,
    revoked: bool,
    session_id: Option<String>,
    banned: bool,
    session_gone: bool,
    session_expired: bool,
    /// Seconds since the token was last written, which is how long ago
    /// it was revoked when it is revoked.
    age: i64,
}

/// The refresh_token grant. Rotation with reuse detection, in GoTrue's
/// order: find, judge, rotate, mint.
pub async fn refresh(
    pool: &Pool,
    token: &str,
    signer: &crate::jwt::Signer<'_>,
    issuer: &str,
) -> Result<Issued, Error> {
    let sess = pool.admin().await?;
    let out = rotate(&sess, token, signer, issuer).await;
    // A refusal can still have written: an orphaned token is deleted
    // and a stolen one takes its whole family down with it. Both have
    // to survive the response, so the transaction commits either way
    // and only a database error rolls back.
    match out {
        Err(Error::Db(e)) => {
            let _ = sess.rollback().await;
            Err(Error::Db(e))
        }
        other => {
            sess.commit().await?;
            other
        }
    }
}

async fn rotate(
    sess: &sql::Session,
    token: &str,
    signer: &crate::jwt::Signer<'_>,
    issuer: &str,
) -> Result<Issued, Error> {
    if token.is_empty() {
        return denied("validation_failed", "refresh_token required");
    }
    let rows = sess
        .query(
            "select t.id,
                    t.token,
                    coalesce(t.revoked, false),
                    s.id::text,
                    coalesce(u.banned_until > now(), false),
                    s.id is null,
                    coalesce(s.not_after < now(), false),
                    coalesce(floor(extract(epoch from now() - t.updated_at))::bigint, 0)
             from auth.refresh_tokens t
             left join auth.sessions s on s.id = t.session_id
             left join auth.users u on u.id::text = t.user_id
             where t.token = $1
             for no key update of t",
            &[&token],
        )
        .await?;
    let Some(row) = rows.first() else {
        return denied(
            "refresh_token_not_found",
            "Invalid Refresh Token: Refresh Token Not Found",
        );
    };
    let found = Presented {
        id: row.get(0),
        token: row.get(1),
        revoked: row.get(2),
        session_id: row.get(3),
        banned: row.get(4),
        session_gone: row.get(5),
        session_expired: row.get(6),
        age: row.get(7),
    };

    if found.banned {
        return denied("user_banned", "Invalid Refresh Token: User Banned");
    }
    if found.session_gone {
        // A token with no session predates the sessions table upstream.
        // Here it can only mean the session was deleted out from under
        // it, and either way the token is useless, so it goes.
        sess.execute(
            "delete from auth.refresh_tokens where id = $1",
            &[&found.id],
        )
        .await?;
        return denied(
            "session_not_found",
            "Invalid Refresh Token: No Valid Session Found",
        );
    }
    if found.session_expired {
        return denied("session_expired", "Invalid Refresh Token: Session Expired");
    }
    let session_id = found.session_id.clone().expect("checked session_gone");

    let issued = if found.revoked {
        match reused(sess, &session_id, &found).await? {
            Some(active) => active,
            None => {
                // Nothing legitimate explains this: the token was
                // revoked, it is not the parent of the live one, and
                // the grace window is past. The whole family goes,
                // which logs out whoever holds the stolen token and
                // whoever it was stolen from, deliberately.
                sess.execute(
                    "update auth.refresh_tokens set revoked = true, updated_at = now()
                     where session_id = $1::text::uuid and revoked = false",
                    &[&session_id],
                )
                .await?;
                return denied(
                    "refresh_token_already_used",
                    "Invalid Refresh Token: Already Used",
                );
            }
        }
    } else {
        swap(sess, &found).await?
    };

    sess.execute(
        "update auth.sessions
            set updated_at = now(), refreshed_at = now() at time zone 'utc'
          where id = $1::text::uuid",
        &[&session_id],
    )
    .await?;
    mint_for(sess, &session_id, issued, signer, issuer).await
}

/// A revoked token was presented. Either it is the parent of the
/// session's live token, which means the client never received the
/// answer to its last refresh and gets that same answer again, or it
/// falls inside the reuse window and rotates normally. Anything else
/// is None, and the caller treats it as theft.
async fn reused(
    sess: &sql::Session,
    session_id: &str,
    found: &Presented,
) -> Result<Option<String>, Error> {
    let rows = sess
        .query(
            // The newest live token is the session's current one. There
            // is normally exactly one, but a grace window swap leaves
            // two behind for as long as the window lasts, and it is the
            // child that the client is holding.
            "select token, coalesce(parent, '')
             from auth.refresh_tokens
             where session_id = $1::text::uuid and revoked = false
             order by created_at desc, id desc
             limit 1",
            &[&session_id],
        )
        .await?;
    if let Some(row) = rows.first() {
        let active: String = row.get(0);
        let parent: String = row.get(1);
        if parent == found.token {
            return Ok(Some(active));
        }
    }
    // Zero is not a one second window, it is no window at all, which is
    // why the interval has to be positive before age is even asked.
    if REUSE_INTERVAL > 0 && found.age < REUSE_INTERVAL {
        return Ok(Some(swap(sess, found).await?));
    }
    Ok(None)
}

/// Revoke the presented token and issue its child. The parent link is
/// what lets the next request tell a lost response from a stolen
/// token, so it is written even though nothing reads it on the happy
/// path.
async fn swap(sess: &sql::Session, found: &Presented) -> Result<String, Error> {
    sess.execute(
        "update auth.refresh_tokens set revoked = true, updated_at = now() where id = $1",
        &[&found.id],
    )
    .await?;
    let token = fresh_token();
    sess.execute(
        "insert into auth.refresh_tokens
             (token, user_id, revoked, created_at, updated_at, parent, session_id)
         select $1, user_id, false, now(), now(), token, session_id
         from auth.refresh_tokens where id = $2",
        &[&token, &found.id],
    )
    .await?;
    Ok(token)
}

/// GoTrue's error body, the shape every supabase client branches on:
/// the http status repeated in code, the machine readable error_code,
/// and the human message under msg.
fn error_body(status: StatusCode, code: &str, msg: &str) -> Response {
    json_body(
        status,
        serde_json::json!({
            "code": status.as_u16(),
            "error_code": code,
            "msg": msg,
        }),
    )
}

/// The largest token request worth reading. A refresh grant carries one
/// short string, a password grant an email and a password, so anything
/// past this is not a request that was going to succeed.
const MAX_BODY: usize = 64 * 1024;

/// The grant_type from the query string, which is where supabase-js
/// puts it. GoTrue reads it as a form value, so a form encoded body
/// works there too, but nothing a Supabase client sends uses that.
fn grant_type(uri: &axum::http::Uri) -> Option<String> {
    uri.query()?.split('&').find_map(|pair| {
        pair.strip_prefix("grant_type=")
            .map(|v| v.split('#').next().unwrap_or(v).to_string())
    })
}

/// POST /auth/v1/token, the OAuth2 token endpoint GoTrue serves.
///
/// Only the refresh_token grant is here. The grants that need a
/// credential to check, password and the rest, answer 501 rather than
/// pretending, because a grant that always fails is worse for a client
/// than one that says it does not exist yet.
pub async fn token(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    req: Request<Body>,
) -> Response {
    let grant = grant_type(req.uri()).unwrap_or_default();
    match grant.as_str() {
        "refresh_token" => {}
        "password" | "id_token" | "pkce" | "web3" => {
            return not_yet(&format!("the {grant} grant"));
        }
        _ => {
            return error_body(
                StatusCode::BAD_REQUEST,
                "invalid_credentials",
                "unsupported_grant_type",
            );
        }
    }
    let Some(pool) = &app.pool else {
        return error_body(
            StatusCode::INTERNAL_SERVER_ERROR,
            "unexpected_failure",
            "no database is configured",
        );
    };
    let body = match to_bytes(req.into_body(), MAX_BODY).await {
        Ok(b) => b,
        Err(_) => {
            return error_body(
                StatusCode::BAD_REQUEST,
                "bad_json",
                "Could not read the request body",
            );
        }
    };
    let parsed: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => {
            return error_body(
                StatusCode::BAD_REQUEST,
                "bad_json",
                "Could not parse request body as JSON",
            );
        }
    };
    let presented = parsed
        .get("refresh_token")
        .and_then(|v| v.as_str())
        .unwrap_or_default();

    match refresh(pool, presented, &app.signer(), &app.issuer()).await {
        Ok(issued) => json_body(StatusCode::OK, issued.json()),
        Err(Error::Denied { code, msg }) => error_body(StatusCode::BAD_REQUEST, code, msg),
        Err(Error::Db(e)) => {
            log::error!("refresh token grant: {e}");
            error_body(
                StatusCode::INTERNAL_SERVER_ERROR,
                "unexpected_failure",
                "Unexpected failure, please check server logs for more information",
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fresh_token_is_twelve_base32_characters() {
        let token = fresh_token();
        assert_eq!(token.len(), 12);
        assert!(
            token
                .chars()
                .all(|c| c.is_ascii_lowercase() || ('2'..='7').contains(&c)),
            "outside base32: {token}"
        );
        assert_ne!(token, fresh_token(), "two draws are not the same token");
    }
}
