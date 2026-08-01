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

/// Why a request failed. `Denied` carries GoTrue's own status,
/// error_code and message so a client that branches on any of them sees
/// what it would see against hosted Supabase. `Weak` is the one refusal
/// with a payload of its own, the reasons a password was rejected.
#[derive(Debug)]
pub enum Error {
    Db(sql::Error),
    Denied {
        status: StatusCode,
        code: &'static str,
        msg: String,
    },
    Weak(crate::password::Weak),
}

impl From<sql::Error> for Error {
    fn from(e: sql::Error) -> Error {
        Error::Db(e)
    }
}

/// A refusal on GoTrue's usual status for a bad request.
fn denied<T>(code: &'static str, msg: &str) -> Result<T, Error> {
    Err(refused(StatusCode::BAD_REQUEST, code, msg))
}

fn refused(status: StatusCode, code: &'static str, msg: &str) -> Error {
    Error::Denied {
        status,
        code,
        msg: msg.to_string(),
    }
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

/// The same for a string column that is empty rather than null when it
/// has nothing to say, which is what Go's zero value writes.
fn opt_text(key: &str, col: &str) -> String {
    format!(
        "case when coalesce({col}, '') = '' then '{{}}'::jsonb
              else jsonb_build_object('{key}', {col}) end"
    )
}

/// The user object GoTrue marshals into every auth response, as a jsonb
/// expression over `u` in auth.users with `ids` carrying the identity
/// list. Every optional timestamp follows Go's omitempty: a null column
/// is not a null key, it is no key at all. new_email and new_phone are
/// the same story for the two string columns that carry a pending
/// change, which are empty rather than null when there is none.
fn user_object() -> String {
    format!(
        "(jsonb_build_object(
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
            || {last_sign_in} || {invited} || {banned} || {deleted}
            || {confirmation_sent} || {recovery_sent} || {email_change_sent}
            || {phone_change_sent} || {reauth_sent}
            || {new_email} || {new_phone})",
        created = ts("u.created_at"),
        updated = ts("u.updated_at"),
        confirmed_at = opt_ts("confirmed_at", "u.confirmed_at"),
        email_confirmed = opt_ts("email_confirmed_at", "u.email_confirmed_at"),
        phone_confirmed = opt_ts("phone_confirmed_at", "u.phone_confirmed_at"),
        last_sign_in = opt_ts("last_sign_in_at", "u.last_sign_in_at"),
        invited = opt_ts("invited_at", "u.invited_at"),
        banned = opt_ts("banned_until", "u.banned_until"),
        deleted = opt_ts("deleted_at", "u.deleted_at"),
        confirmation_sent = opt_ts("confirmation_sent_at", "u.confirmation_sent_at"),
        recovery_sent = opt_ts("recovery_sent_at", "u.recovery_sent_at"),
        email_change_sent = opt_ts("email_change_sent_at", "u.email_change_sent_at"),
        phone_change_sent = opt_ts("phone_change_sent_at", "u.phone_change_sent_at"),
        reauth_sent = opt_ts("reauthentication_sent_at", "u.reauthentication_sent_at"),
        new_email = opt_text("new_email", "u.email_change"),
        new_phone = opt_text("new_phone", "u.phone_change"),
    )
}

/// The identity list, joined the way both user queries need it.
fn identities_join() -> String {
    format!(
        "left join lateral (
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
         ) ids on true",
        i_created = ts("i.created_at"),
        i_updated = ts("i.updated_at"),
    )
}

/// The user as a client sees it, for the answers that carry a user
/// without a session: a signup that still needs confirming, and the
/// user endpoints when they land.
async fn user_json(sess: &sql::Session, user_id: &str) -> Result<serde_json::Value, sql::Error> {
    let sql = format!(
        "select {user}::text from auth.users u {ids} where u.id = $1::text::uuid",
        user = user_object(),
        ids = identities_join(),
    );
    let rows = sess.query(&sql, &[&user_id]).await?;
    Ok(serde_json::from_str(rows[0].get::<_, &str>(0))
        .expect("jsonb_build_object always produces json"))
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
                {user}::text
         from auth.sessions s
         join auth.users u on u.id = s.user_id
         left join lateral (
             select jsonb_agg(jsonb_build_object(
                        'method', a.authentication_method,
                        'timestamp', floor(extract(epoch from a.created_at))::bigint
                    ) order by a.created_at) as list
             from auth.mfa_amr_claims a where a.session_id = s.id
         ) amr on true
         {ids}
         where s.id = $1::text::uuid",
        user = user_object(),
        ids = identities_join(),
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

/// A six digit one time code, GoTrue's MAILER_OTP_LENGTH default. It is
/// what the confirmation email carries, and it is drawn uniformly
/// rather than from the low bits of a timestamp.
fn otp() -> String {
    let mut raw = [0u8; 4];
    getrandom::fill(&mut raw).expect("the os rng never fails");
    // A million does not divide 2^32, so the top of the range is
    // redrawn rather than folded, which would favour the low codes.
    let mut value = u32::from_be_bytes(raw);
    while value >= 4_294_000_000 {
        getrandom::fill(&mut raw).expect("the os rng never fails");
        value = u32::from_be_bytes(raw);
    }
    format!("{:06}", value % 1_000_000)
}

/// What is stored for a one time code: the hex of sha224 over the
/// address and the code together, which is GoTrue's GenerateTokenHash.
/// The code itself is never written down, so a database that leaks does
/// not hand out working confirmation links.
fn token_hash(email: &str, otp: &str) -> String {
    use sha2::Digest;
    let digest = sha2::Sha224::digest(format!("{email}{otp}").as_bytes());
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

/// What a signup turned into. A project that confirms its own signups
/// gets a session straight away, one that mails a confirmation gets the
/// user and nothing else, which is the difference a client watches for.
pub enum SignedUp {
    Session(Box<Issued>),
    Pending(serde_json::Value),
}

/// GoTrue's email address check: present, short enough, one address,
/// lowercased. The wording of every refusal is upstream's, because a
/// client that surfaces the message to a person shows the same words.
///
/// The format rule is the shape of upstream's regex rather than the
/// regex itself: exactly one @, something either side of it, no
/// whitespace anywhere, and a domain that is dotted, which is what
/// keeps user@localhost out.
fn validate_email(email: &str) -> Result<String, Error> {
    if email.is_empty() {
        return denied("validation_failed", "An email address is required");
    }
    if email.len() > 255 {
        return denied("validation_failed", "An email address is too long");
    }
    let mut parts = email.split('@');
    let local = parts.next().unwrap_or_default();
    let domain = parts.next().unwrap_or_default();
    let dotted = domain.split('.').filter(|label| !label.is_empty()).count() > 1
        && !domain.starts_with('.')
        && !domain.ends_with('.');
    let malformed = parts.next().is_some()
        || local.is_empty()
        || !dotted
        || email.chars().any(char::is_whitespace);
    if malformed {
        return denied(
            "validation_failed",
            "Unable to validate email address: invalid format",
        );
    }
    Ok(email.to_lowercase())
}

/// GoTrue's password rules, in its order: the bcrypt ceiling first
/// because a password over it is not weak but unusable, then strength.
fn validate_password(password: &str) -> Result<(), Error> {
    if password.is_empty() {
        return denied("validation_failed", "Signup requires a valid password");
    }
    if password.len() > crate::password::MAX_LENGTH {
        return denied(
            "validation_failed",
            &format!(
                "Password cannot be longer than {} characters",
                crate::password::MAX_LENGTH
            ),
        );
    }
    crate::password::strength(password).map_err(Error::Weak)
}

/// Create the user, or pick up the one already there that never
/// confirmed, and either confirm it here or leave a confirmation token
/// for the verify flow to consume.
///
/// The whole thing is one transaction. A user row without its identity
/// row is a user that no provider owns, and a confirmed user without a
/// session is a signup that answered with nothing.
async fn register(
    sess: &sql::Session,
    email: &str,
    hash: &str,
    data: &serde_json::Value,
    autoconfirm: bool,
    signer: &crate::jwt::Signer<'_>,
    issuer: &str,
) -> Result<SignedUp, Error> {
    let rows = sess
        .query(
            "select id::text, email_confirmed_at is not null
             from auth.users
             where email = $1 and aud = $2 and is_sso_user = false and deleted_at is null
             limit 1",
            &[&email, &AUD],
        )
        .await?;
    let existing: Option<(String, bool)> = rows.first().map(|r| (r.get(0), r.get(1)));

    let user_id = match existing {
        // Someone already signed up on this address and proved they
        // hold it. A project that mails confirmations cannot say so
        // without telling anyone who asks which addresses are
        // registered, so it answers with a user shaped object that
        // belongs to nobody. A project that confirms its own signups
        // has no such secret to keep and says it plainly.
        Some((_, true)) => {
            if autoconfirm {
                return Err(refused(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "user_already_exists",
                    "User already registered",
                ));
            }
            return Ok(SignedUp::Pending(sanitized(sess, email, data).await?));
        }
        // The address is claimed but unproven, so the row is reused as
        // it stands. The password is deliberately left alone: whoever
        // is asking has not shown they are the one who started it.
        Some((id, false)) => id,
        None => {
            let rows = sess
                .query(
                    "insert into auth.users
                         (instance_id, id, aud, role, email, encrypted_password,
                          raw_app_meta_data, raw_user_meta_data,
                          confirmation_token, recovery_token,
                          email_change_token_new, email_change,
                          created_at, updated_at, is_anonymous, is_sso_user)
                     values ('00000000-0000-0000-0000-000000000000', gen_random_uuid(),
                             $2, 'authenticated', $1, $3,
                             jsonb_build_object('provider', 'email',
                                                'providers', jsonb_build_array('email')),
                             $4::jsonb, '', '', '', '', now(), now(), false, false)
                     returning id::text",
                    &[&email, &AUD, &hash, &data],
                )
                .await?;
            rows[0].get(0)
        }
    };

    // The identity is what says this user belongs to the email
    // provider. email_verified is false even when the signup is
    // confirmed here, because it describes what the provider asserted,
    // and this provider asserted nothing.
    sess.execute(
        "insert into auth.identities
             (provider_id, user_id, identity_data, provider,
              last_sign_in_at, created_at, updated_at)
         select $1::text::uuid::text, $1::text::uuid,
                $3::jsonb || jsonb_build_object(
                    'sub', $1::text, 'email', $2::text,
                    'email_verified', false, 'phone_verified', false),
                'email', now(), now(), now()
         where not exists (
             select 1 from auth.identities
             where user_id = $1::text::uuid and provider = 'email'
         )",
        &[&user_id, &email, &data],
    )
    .await?;

    if !autoconfirm {
        let code = otp();
        let hashed = token_hash(email, &code);
        sess.execute(
            "update auth.users
                set confirmation_token = $2, confirmation_sent_at = now(), updated_at = now()
              where id = $1::text::uuid",
            &[&user_id, &hashed],
        )
        .await?;
        // One live confirmation per user, upstream's rule: a second
        // signup on the same unconfirmed address replaces the first
        // code rather than leaving two that both work.
        sess.execute(
            "delete from auth.one_time_tokens
              where user_id = $1::text::uuid and token_type = 'confirmation_token'",
            &[&user_id],
        )
        .await?;
        sess.execute(
            "insert into auth.one_time_tokens
                 (id, user_id, token_type, token_hash, relates_to, created_at, updated_at)
             values (gen_random_uuid(), $1::text::uuid, 'confirmation_token', $2, $3,
                     now(), now())",
            &[&user_id, &hashed, &email],
        )
        .await?;
        return Ok(SignedUp::Pending(user_json(sess, &user_id).await?));
    }

    sess.execute(
        "update auth.users
            set email_confirmed_at = now(),
                confirmation_token = '',
                updated_at = now(),
                raw_user_meta_data = coalesce(raw_user_meta_data, '{}'::jsonb)
                                     || jsonb_build_object('email_verified', true)
          where id = $1::text::uuid",
        &[&user_id],
    )
    .await?;
    sess.execute(
        "delete from auth.one_time_tokens where user_id = $1::text::uuid",
        &[&user_id],
    )
    .await?;
    Ok(SignedUp::Session(Box::new(
        start(sess, &user_id, "password", signer, issuer).await?,
    )))
}

/// The answer a project that mails confirmations gives when the address
/// is already taken: a user object that belongs to nobody. The id is
/// fresh, there are no identities, and the timestamps are all now, so
/// nothing in it distinguishes a taken address from a free one.
async fn sanitized(
    sess: &sql::Session,
    email: &str,
    data: &serde_json::Value,
) -> Result<serde_json::Value, sql::Error> {
    let sql = format!(
        "select jsonb_build_object(
                    'id', gen_random_uuid()::text,
                    'aud', $2::text,
                    'role', '',
                    'email', $1::text,
                    'phone', '',
                    'app_metadata', jsonb_build_object(
                        'provider', 'email',
                        'providers', jsonb_build_array('email')),
                    'user_metadata', $3::jsonb,
                    'identities', '[]'::jsonb,
                    'created_at', {now},
                    'updated_at', {now},
                    'confirmation_sent_at', {now},
                    'is_anonymous', false
                )::text",
        now = ts("now()"),
    );
    let rows = sess.query(&sql, &[&email, &AUD, &data]).await?;
    Ok(serde_json::from_str(rows[0].get::<_, &str>(0))
        .expect("jsonb_build_object always produces json"))
}

/// POST /auth/v1/signup with an email and a password.
pub async fn sign_up(
    pool: &Pool,
    email: &str,
    password: &str,
    data: &serde_json::Value,
    autoconfirm: bool,
    signer: &crate::jwt::Signer<'_>,
    issuer: &str,
) -> Result<SignedUp, Error> {
    let email = validate_email(email)?;
    validate_password(password)?;
    // Cost 10 is tens of milliseconds of pure cpu, and it happens
    // before the connection is taken so a slow hash never holds one.
    let hash = hash_off_thread(password).await;
    let sess = pool.admin().await?;
    let out = register(&sess, &email, &hash, data, autoconfirm, signer, issuer).await;
    match out {
        Ok(done) => {
            sess.commit().await?;
            Ok(done)
        }
        Err(e) => {
            let _ = sess.rollback().await;
            Err(e)
        }
    }
}

async fn hash_off_thread(password: &str) -> String {
    let password = password.to_string();
    tokio::task::spawn_blocking(move || crate::password::hash(&password))
        .await
        .expect("hashing does not panic")
}

async fn matches_off_thread(password: &str, hash: &str) -> bool {
    let (password, hash) = (password.to_string(), hash.to_string());
    tokio::task::spawn_blocking(move || crate::password::matches(&password, &hash))
        .await
        .expect("verifying does not panic")
}

/// The password grant. Every refusal that is about the credential says
/// the same thing, invalid_credentials, whether the address is unknown,
/// the password is wrong, or the account has no password at all,
/// because saying which would answer a question the caller did not get
/// to ask.
pub async fn password_grant(
    pool: &Pool,
    email: &str,
    password: &str,
    signer: &crate::jwt::Signer<'_>,
    issuer: &str,
) -> Result<Issued, Error> {
    let sess = pool.admin().await?;
    let out = sign_in(&sess, email, password, signer, issuer).await;
    match out {
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

async fn sign_in(
    sess: &sql::Session,
    email: &str,
    password: &str,
    signer: &crate::jwt::Signer<'_>,
    issuer: &str,
) -> Result<Issued, Error> {
    let email = email.to_lowercase();
    let rows = sess
        .query(
            "select id::text,
                    coalesce(encrypted_password, ''),
                    coalesce(banned_until > now(), false),
                    email_confirmed_at is not null
             from auth.users
             where email = $1 and aud = $2 and is_sso_user = false and deleted_at is null
             limit 1",
            &[&email, &AUD],
        )
        .await?;
    let Some(row) = rows.first() else {
        return denied("invalid_credentials", INVALID_LOGIN);
    };
    let user_id: String = row.get(0);
    let hash: String = row.get(1);
    let banned: bool = row.get(2);
    let confirmed: bool = row.get(3);

    if hash.is_empty() {
        return denied("invalid_credentials", INVALID_LOGIN);
    }
    // Before the password is checked, as upstream does: a banned user
    // is told they are banned whether or not they still know it.
    if banned {
        return denied("user_banned", "User is banned");
    }
    if !matches_off_thread(password, &hash).await {
        return denied("invalid_credentials", INVALID_LOGIN);
    }
    // Last, so an unconfirmed address is only ever revealed to whoever
    // proved they hold the password for it.
    if !confirmed {
        return denied("email_not_confirmed", "Email not confirmed");
    }
    start(sess, &user_id, "password", signer, issuer).await
}

/// The one message every bad credential gets, GoTrue's wording.
const INVALID_LOGIN: &str = "Invalid login credentials";

/// GoTrue's error body, the shape every supabase client branches on:
/// the http status repeated in code, the machine readable error_code,
/// and the human message under msg. The same code rides in a response
/// header, which is where the newer clients read it from without
/// parsing the body at all.
fn error_body(status: StatusCode, code: &str, msg: &str) -> Response {
    body_with(
        status,
        serde_json::json!({
            "code": status.as_u16(),
            "error_code": code,
            "msg": msg,
        }),
        code,
    )
}

fn body_with(status: StatusCode, body: serde_json::Value, code: &str) -> Response {
    let mut res = json_body(status, body);
    if let Ok(value) = axum::http::HeaderValue::from_str(code) {
        res.headers_mut().insert("x-sb-error-code", value);
    }
    res
}

/// Turn a refusal into the response GoTrue would have sent. A weak
/// password is the one refusal with more to say than a message: the
/// reasons ride alongside so a client can point at the rule that was
/// broken rather than re-deriving it from english.
fn refusal(e: Error, doing: &str) -> Response {
    match e {
        Error::Denied { status, code, msg } => error_body(status, code, &msg),
        Error::Weak(weak) => body_with(
            StatusCode::UNPROCESSABLE_ENTITY,
            serde_json::json!({
                "code": StatusCode::UNPROCESSABLE_ENTITY.as_u16(),
                "error_code": "weak_password",
                "msg": weak.message,
                "weak_password": {"reasons": weak.reasons},
            }),
            "weak_password",
        ),
        Error::Db(e) => {
            log::error!("{doing}: {e}");
            error_body(
                StatusCode::INTERNAL_SERVER_ERROR,
                "unexpected_failure",
                "Unexpected failure, please check server logs for more information",
            )
        }
    }
}

/// The json body of a request, or the response that says why it could
/// not be read. Both failures are GoTrue's own.
async fn read_json(body: Body) -> Result<serde_json::Value, Response> {
    let bytes = to_bytes(body, MAX_BODY).await.map_err(|_| {
        error_body(
            StatusCode::BAD_REQUEST,
            "bad_json",
            "Could not read the request body",
        )
    })?;
    serde_json::from_slice(&bytes).map_err(|_| {
        error_body(
            StatusCode::BAD_REQUEST,
            "bad_json",
            "Could not parse request body as JSON",
        )
    })
}

fn field<'a>(body: &'a serde_json::Value, key: &str) -> &'a str {
    body.get(key).and_then(|v| v.as_str()).unwrap_or_default()
}

/// The user_metadata a signup carries, GoTrue's `data`. Anything that
/// is not an object is nothing, which is what Go's decode into a map
/// leaves behind for a null and what it refuses outright for a scalar.
fn metadata(body: &serde_json::Value) -> serde_json::Value {
    match body.get("data") {
        Some(v) if v.is_object() => v.clone(),
        _ => serde_json::json!({}),
    }
}

/// POST /auth/v1/signup.
///
/// Email and password only. Phone signups need an SMS provider to be
/// worth serving, so they say so rather than writing a user nobody can
/// ever confirm.
pub async fn signup(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    req: Request<Body>,
) -> Response {
    let Some(pool) = &app.pool else {
        return no_database();
    };
    let body = match read_json(req.into_body()).await {
        Ok(v) => v,
        Err(res) => return res,
    };
    let (email, phone, password) = (
        field(&body, "email"),
        field(&body, "phone"),
        field(&body, "password"),
    );
    // Upstream's order: the password is judged before anyone asks what
    // is being signed up, so a weak password is called weak whether or
    // not the address was going to be accepted.
    if let Err(e) = validate_password(password) {
        return refusal(e, "signup");
    }
    if !email.is_empty() && !phone.is_empty() {
        return error_body(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            "Only an email address or phone number should be provided on signup.",
        );
    }
    if !phone.is_empty() {
        return not_yet("phone signup");
    }
    if email.is_empty() {
        return error_body(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            "Sign up only available with email or phone provider",
        );
    }

    let data = metadata(&body);
    let autoconfirm = app.cfg.mailer_autoconfirm;
    match sign_up(
        pool,
        email,
        password,
        &data,
        autoconfirm,
        &app.signer(),
        &app.issuer(),
    )
    .await
    {
        Ok(SignedUp::Session(issued)) => json_body(StatusCode::OK, issued.json()),
        Ok(SignedUp::Pending(user)) => json_body(StatusCode::OK, user),
        Err(e) => refusal(e, "signup"),
    }
}

fn no_database() -> Response {
    error_body(
        StatusCode::INTERNAL_SERVER_ERROR,
        "unexpected_failure",
        "no database is configured",
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
/// The refresh_token and password grants are here. The rest need a
/// credential this end cannot check yet, an id token from a provider or
/// a signed challenge, and they answer 501 rather than pretending,
/// because a grant that always fails is worse for a client than one
/// that says it does not exist yet.
pub async fn token(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    req: Request<Body>,
) -> Response {
    let grant = grant_type(req.uri()).unwrap_or_default();
    match grant.as_str() {
        "refresh_token" | "password" => {}
        "id_token" | "pkce" | "web3" => {
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
        return no_database();
    };
    let body = match read_json(req.into_body()).await {
        Ok(v) => v,
        Err(res) => return res,
    };

    let issued = if grant == "password" {
        let (email, phone) = (field(&body, "email"), field(&body, "phone"));
        if !email.is_empty() && !phone.is_empty() {
            return error_body(
                StatusCode::BAD_REQUEST,
                "validation_failed",
                "Only an email address or phone number should be provided on login.",
            );
        }
        if email.is_empty() {
            // The phone grant is the same query against a different
            // column, and it waits for the SMS side of the surface.
            if !phone.is_empty() {
                return not_yet("the phone password grant");
            }
            return error_body(
                StatusCode::BAD_REQUEST,
                "validation_failed",
                "missing email or phone",
            );
        }
        password_grant(
            pool,
            email,
            field(&body, "password"),
            &app.signer(),
            &app.issuer(),
        )
        .await
    } else {
        refresh(
            pool,
            field(&body, "refresh_token"),
            &app.signer(),
            &app.issuer(),
        )
        .await
    };

    match issued {
        Ok(issued) => json_body(StatusCode::OK, issued.json()),
        Err(e) => refusal(e, &format!("{grant} grant")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The three things a refusal says, so a test states them in one
    /// line instead of destructuring the enum every time.
    fn refusal(e: Error) -> (u16, &'static str, String) {
        match e {
            Error::Denied { status, code, msg } => (status.as_u16(), code, msg),
            other => panic!("not a refusal: {other:?}"),
        }
    }

    #[test]
    fn the_token_hash_is_the_one_gotrue_stores() {
        // Produced by crypto/sha256.Sum224 in Go over the address and
        // the code concatenated, which is GenerateTokenHash. A
        // confirmation link minted by a real GoTrue has to be
        // consumable here and the other way round, and that is only
        // true if the stored hash is byte for byte the same.
        assert_eq!(
            token_hash("person@zou.test", "123456"),
            "f50ddc2444b919be70a2b89c5ed3df9b15e0e68df65ef14f4a19b0ec"
        );
        assert_ne!(
            token_hash("person@zou.test", "123457"),
            token_hash("person@zou.test", "123456")
        );
        // The address is in the hash, so the same code sent to two
        // people does not confirm both of them.
        assert_ne!(
            token_hash("other@zou.test", "123456"),
            token_hash("person@zou.test", "123456")
        );
    }

    #[test]
    fn a_one_time_code_is_six_digits_and_not_always_the_same_one() {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..200 {
            let code = otp();
            assert_eq!(code.len(), 6, "MAILER_OTP_LENGTH is 6: {code}");
            assert!(
                code.chars().all(|c| c.is_ascii_digit()),
                "not digits: {code}"
            );
            seen.insert(code);
        }
        assert!(
            seen.len() > 190,
            "200 draws collided {} times",
            200 - seen.len()
        );
    }

    #[test]
    fn an_address_is_lowercased_once_it_passes() {
        assert_eq!(
            validate_email("Person@Zou.Test").unwrap(),
            "person@zou.test"
        );
        assert_eq!(
            validate_email("a.b+tag@sub.zou.test").unwrap(),
            "a.b+tag@sub.zou.test"
        );
    }

    #[test]
    fn the_addresses_gotrue_refuses_are_refused_here() {
        let format = "Unable to validate email address: invalid format";
        for bad in [
            "no-at-sign",
            "@zou.test",
            "person@",
            "two@@zou.test",
            "person@localhost",
            "person@zou.",
            "spaced out@zou.test",
        ] {
            assert_eq!(
                refusal(validate_email(bad).unwrap_err()),
                (400, "validation_failed", format.to_string()),
                "for {bad}"
            );
        }
        assert_eq!(
            refusal(validate_email("").unwrap_err()).2,
            "An email address is required"
        );
        let long = format!("{}@zou.test", "x".repeat(250));
        assert_eq!(
            refusal(validate_email(&long).unwrap_err()).2,
            "An email address is too long"
        );
    }

    #[test]
    fn the_bcrypt_ceiling_is_a_validation_failure_and_not_a_weak_password() {
        // Weak and unusable are different answers with different
        // statuses, and a client that offers to fix a weak password
        // must not be told to lengthen one that is already too long.
        assert_eq!(
            refusal(validate_password(&"x".repeat(73)).unwrap_err()),
            (
                400,
                "validation_failed",
                "Password cannot be longer than 72 characters".to_string()
            )
        );
        assert!(validate_password(&"x".repeat(72)).is_ok());
        assert_eq!(
            refusal(validate_password("").unwrap_err()).2,
            "Signup requires a valid password"
        );
        assert!(matches!(validate_password("12345"), Err(Error::Weak(_))));
    }

    #[test]
    fn metadata_is_an_object_or_it_is_nothing() {
        let empty = serde_json::json!({});
        assert_eq!(metadata(&serde_json::json!({})), empty);
        assert_eq!(metadata(&serde_json::json!({"data": null})), empty);
        assert_eq!(metadata(&serde_json::json!({"data": "nickname"})), empty);
        assert_eq!(metadata(&serde_json::json!({"data": [1, 2]})), empty);
        assert_eq!(
            metadata(&serde_json::json!({"data": {"nickname": "tester"}})),
            serde_json::json!({"nickname": "tester"})
        );
    }

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
