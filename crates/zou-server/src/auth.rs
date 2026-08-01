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
//!
//! The email flows here draw a six digit code, write down the hash of
//! the address and the code together, and hand the code to the mailer.
//! There is no mailer yet, so the code goes nowhere and every one of
//! these endpoints answers with an acknowledgement and nothing else,
//! which is what they answer upstream too. What is missing with it is
//! the frequency limit GoTrue applies to sending: that is a property of
//! the send rather than of the flow, so it lands with the sender.

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
    /// A branch upstream serves and this end does not yet, refused from
    /// inside the flow so that the checks before it still run in
    /// upstream's order.
    NotYet(&'static str),
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
        // The code goes in the email and its hash goes here, one live
        // confirmation per user, which is upstream's rule: a second
        // signup on the same unconfirmed address replaces the first
        // code rather than leaving two that both work.
        mint_code(sess, &user_id, email, "confirmation_token").await?;
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

/// GoTrue's MAILER_OTP_EXP default: a code that went out in an email is
/// good for a day.
const OTP_EXP: i64 = 86400;

/// GoTrue's window for a session to count as recent enough that setting
/// a new password does not ask for the address to be proved again.
const REAUTH_WINDOW: &str = "24 hours";

/// What a verify answers with when the first of two email change
/// confirmations lands. The address has not moved yet.
const SINGLE_CONFIRMATION: &str =
    "Confirmation link accepted. Please proceed to confirm link sent to the other email";

/// The refusal a link that was followed too late gets, and the one a
/// link that never existed gets, deliberately the same words.
const LINK_EXPIRED: &str = "Email link is invalid or has expired";

/// The same refusal for the path where the code and the address were
/// posted rather than followed, which upstream words differently.
const TOKEN_EXPIRED: &str = "Token has expired or is invalid";

/// One live token of each kind per user, which is upstream's rule: a
/// new code replaces the one before it rather than leaving two that
/// both work. `relates_to` is the address the code was sent to, which
/// is what stops a code from being worth anything anywhere else.
async fn keep_token(
    sess: &sql::Session,
    user_id: &str,
    token_type: &str,
    hash: &str,
    relates_to: &str,
) -> Result<(), sql::Error> {
    sess.execute(
        "delete from auth.one_time_tokens
          where user_id = $1::text::uuid and token_type::text = $2",
        &[&user_id, &token_type],
    )
    .await?;
    sess.execute(
        "insert into auth.one_time_tokens
             (id, user_id, token_type, token_hash, relates_to, created_at, updated_at)
         values (gen_random_uuid(), $1::text::uuid, $2::text::auth.one_time_token_type,
                 $3, $4, now(), now())",
        &[&user_id, &token_type, &hash, &relates_to],
    )
    .await?;
    Ok(())
}

/// Draw a code for one of the email flows and write down its hash: into
/// the column on auth.users the flow reads, into that flow's sent_at,
/// and into auth.one_time_tokens. The code itself is returned for the
/// mailer to carry, and until there is a mailer it is returned to
/// nobody, which is why every one of these flows answers with no more
/// than an acknowledgement.
async fn mint_code(
    sess: &sql::Session,
    user_id: &str,
    email: &str,
    token_type: &str,
) -> Result<String, sql::Error> {
    let (column, sent) = match token_type {
        "recovery_token" => ("recovery_token", "recovery_sent_at"),
        "reauthentication_token" => ("reauthentication_token", "reauthentication_sent_at"),
        _ => ("confirmation_token", "confirmation_sent_at"),
    };
    let code = otp();
    let hashed = token_hash(email, &code);
    sess.execute(
        &format!(
            "update auth.users
                set {column} = $2, {sent} = now(), updated_at = now()
              where id = $1::text::uuid"
        ),
        &[&user_id, &hashed],
    )
    .await?;
    keep_token(sess, user_id, token_type, &hashed, email).await?;
    Ok(code)
}

/// A verify request, GoTrue's VerifyParams. The code arrives either as
/// the digits that were mailed, together with the address they went to,
/// or as its hash, which is what the link in the email carries.
struct Asked {
    kind: String,
    token: String,
    hash: String,
    email: String,
}

/// Upstream's validation of a verify request, in its order and its
/// wording. A GET is a link being followed, so it carries the hash in
/// the token parameter and the address is not part of it; a POST is a
/// client and may send either form.
fn asked(body: &serde_json::Value, followed: bool) -> Result<Asked, Error> {
    let kind = field(body, "type").to_string();
    if kind.is_empty() {
        return denied("validation_failed", "Verify requires a verification type");
    }
    let token = field(body, "token").to_string();
    let hash = field(body, "token_hash").to_string();
    let phone = field(body, "phone").to_string();
    if followed {
        // A link carries the hash under either name, because the older
        // templates put it in token and the newer ones in token_hash,
        // and both are out there in mailboxes that have not been opened
        // yet.
        let carried = if token.is_empty() { hash } else { token };
        if carried.is_empty() {
            return denied(
                "validation_failed",
                "Verify requires a token or a token hash",
            );
        }
        return Ok(Asked {
            kind,
            token: String::new(),
            hash: carried,
            email: String::new(),
        });
    }
    if token.is_empty() == hash.is_empty() {
        return denied(
            "validation_failed",
            "Verify requires either a token or a token hash",
        );
    }
    if kind == "sms" || kind == "phone_change" || !phone.is_empty() {
        return Err(Error::NotYet("verifying a phone number"));
    }
    if token.is_empty() {
        if !field(body, "email").is_empty() || !field(body, "redirect_to").is_empty() {
            return denied(
                "validation_failed",
                "Only the token_hash and type should be provided",
            );
        }
        return Ok(Asked {
            kind,
            token,
            hash,
            email: String::new(),
        });
    }
    let email = field(body, "email");
    if email.is_empty() {
        return denied(
            "validation_failed",
            "Only an email address or phone number should be provided on verify",
        );
    }
    // The address is judged more harshly here than on a signup: a
    // malformed one is 422 rather than 400, which is upstream's own
    // split and the one a client branches on.
    let email = validate_email(email).map_err(|_| {
        refused(
            StatusCode::UNPROCESSABLE_ENTITY,
            "validation_failed",
            "Invalid email format",
        )
    })?;
    let hash = token_hash(&email, &token);
    Ok(Asked {
        kind,
        token,
        hash,
        email,
    })
}

/// Whose code it was, and what it turned out to be for. The `email`
/// type does not say which flow it is verifying, so the kind is
/// resolved from whichever of the two codes matched.
struct Holder {
    user_id: String,
    kind: String,
}

fn expired<T>(msg: &str) -> Result<T, Error> {
    Err(refused(StatusCode::FORBIDDEN, "otp_expired", msg))
}

fn banned<T>() -> Result<T, Error> {
    Err(refused(
        StatusCode::FORBIDDEN,
        "user_banned",
        "User is banned",
    ))
}

/// Find the user from the hash alone, which is what a followed link and
/// a client holding a token_hash both have. The row in
/// auth.one_time_tokens is the index: the columns on auth.users carry
/// the same hash, but only the token table can be searched without
/// knowing whose it is.
async fn by_hash(
    sess: &sql::Session,
    kind: &str,
    hash: &str,
    missing: &str,
) -> Result<Holder, Error> {
    let types = match kind {
        "signup" | "invite" => "'confirmation_token'",
        "recovery" | "magiclink" => "'recovery_token'",
        "email_change" => "'email_change_token_current','email_change_token_new'",
        "email" => "'confirmation_token','recovery_token'",
        _ => return denied("validation_failed", "Invalid email verification type"),
    };
    let sql = format!(
        "select t.user_id::text,
                t.token_type::text,
                coalesce(u.banned_until > now(), false),
                coalesce(case t.token_type::text
                             when 'confirmation_token' then u.confirmation_sent_at
                             when 'recovery_token' then u.recovery_sent_at
                             else u.email_change_sent_at
                         end > now() - interval '{OTP_EXP} seconds', false)
           from auth.one_time_tokens t
           join auth.users u on u.id = t.user_id
          where t.token_hash = $1 and t.token_type::text in ({types})
          limit 1"
    );
    let rows = sess.query(&sql, &[&hash]).await?;
    let Some(row) = rows.first() else {
        return expired(missing);
    };
    let user_id: String = row.get(0);
    let matched: String = row.get(1);
    if row.get::<_, bool>(2) {
        return banned();
    }
    if !row.get::<_, bool>(3) {
        return expired(missing);
    }
    Ok(Holder {
        user_id,
        kind: resolved(kind, &matched),
    })
}

/// Find the user from the address, which is what a client that has the
/// six digits sends. The code is checked against the column for the
/// flow rather than searched for, so a code that belongs to someone
/// else is simply not this user's code.
async fn by_email(
    sess: &sql::Session,
    kind: &str,
    email: &str,
    hash: &str,
) -> Result<Holder, Error> {
    if kind == "email_change" {
        // The address on the request is the new one while the row still
        // has the old, so even here it is the token that finds the user.
        return by_hash(sess, kind, hash, TOKEN_EXPIRED).await;
    }
    let sql = format!(
        "select u.id::text,
                coalesce(u.banned_until > now(), false),
                coalesce(u.confirmation_token = $2
                         and u.confirmation_sent_at > now() - interval '{OTP_EXP} seconds', false),
                coalesce(u.recovery_token = $2
                         and u.recovery_sent_at > now() - interval '{OTP_EXP} seconds', false)
           from auth.users u
          where u.email = $1 and u.aud = $3 and u.deleted_at is null
          limit 1"
    );
    let rows = sess.query(&sql, &[&email, &hash, &AUD]).await?;
    let Some(row) = rows.first() else {
        return expired(TOKEN_EXPIRED);
    };
    let user_id: String = row.get(0);
    if row.get::<_, bool>(1) {
        return banned();
    }
    let confirmation: bool = row.get(2);
    let recovery: bool = row.get(3);
    let matched = match kind {
        "signup" | "invite" if confirmation => "confirmation_token",
        "recovery" | "magiclink" if recovery => "recovery_token",
        "email" if confirmation => "confirmation_token",
        "email" if recovery => "recovery_token",
        _ => return expired(TOKEN_EXPIRED),
    };
    Ok(Holder {
        user_id,
        kind: resolved(kind, matched),
    })
}

/// What a verification of type `email` turned out to be, which is
/// whichever code matched: the one a signup left behind, or the one a
/// recovery or a magic link did.
fn resolved(kind: &str, matched: &str) -> String {
    if kind != "email" {
        return kind.to_string();
    }
    match matched {
        "confirmation_token" => "signup".to_string(),
        _ => "magiclink".to_string(),
    }
}

/// Confirm the address. `only_unconfirmed` is what the recovery path
/// wants: following a recovery link proves the address as surely as
/// following a confirmation link does, but an account that was already
/// confirmed does not have its confirmation moved to today.
async fn confirm_address(
    sess: &sql::Session,
    user_id: &str,
    only_unconfirmed: bool,
) -> Result<(), sql::Error> {
    let guard = if only_unconfirmed {
        "and u.email_confirmed_at is null"
    } else {
        ""
    };
    sess.execute(
        &format!(
            "update auth.users u
                set confirmation_token = '',
                    email_confirmed_at = now(),
                    updated_at = now(),
                    raw_user_meta_data = coalesce(u.raw_user_meta_data, '{{}}'::jsonb)
                                         || jsonb_build_object('email_verified', true)
              where u.id = $1::text::uuid {guard}"
        ),
        &[&user_id],
    )
    .await?;
    // The identity says what the provider asserted, and the email
    // provider has now asserted the address, which is the one thing it
    // was ever going to assert.
    sess.execute(
        "update auth.identities i
            set identity_data = i.identity_data
                                || jsonb_build_object('email_verified', true),
                updated_at = now()
           from auth.users u
          where i.user_id = u.id and u.id = $1::text::uuid
            and i.identity_data->>'email' = u.email",
        &[&user_id],
    )
    .await?;
    Ok(())
}

async fn forget_tokens(sess: &sql::Session, user_id: &str) -> Result<(), sql::Error> {
    sess.execute(
        "delete from auth.one_time_tokens where user_id = $1::text::uuid",
        &[&user_id],
    )
    .await?;
    Ok(())
}

/// Spend the code. The answer is whether the flow finished: an email
/// change under double confirmation says no the first time, because one
/// of the two addresses has answered and the other has not.
async fn consume(
    sess: &sql::Session,
    holder: &Holder,
    hash: &str,
    secure_change: bool,
    autoconfirm: bool,
) -> Result<bool, Error> {
    match holder.kind.as_str() {
        "signup" | "invite" => {
            confirm_address(sess, &holder.user_id, false).await?;
            forget_tokens(sess, &holder.user_id).await?;
        }
        "recovery" | "magiclink" => {
            sess.execute(
                "update auth.users
                    set recovery_token = '', updated_at = now()
                  where id = $1::text::uuid",
                &[&holder.user_id],
            )
            .await?;
            confirm_address(sess, &holder.user_id, true).await?;
            forget_tokens(sess, &holder.user_id).await?;
        }
        "email_change" => {
            return change_address(sess, holder, hash, secure_change, autoconfirm).await;
        }
        _ => return denied("validation_failed", "Unsupported verification type"),
    }
    Ok(true)
}

/// Move the address, or record that one of the two links has been
/// followed. Under double confirmation the first answer changes
/// nothing but a status and the token that was spent, so an attacker
/// holding one of the two links moves nobody's account.
async fn change_address(
    sess: &sql::Session,
    holder: &Holder,
    hash: &str,
    secure_change: bool,
    autoconfirm: bool,
) -> Result<bool, Error> {
    let rows = sess
        .query(
            "select coalesce(email, ''),
                    coalesce(email_change_confirm_status, 0)::int,
                    coalesce(email_change_token_current, ''),
                    coalesce(email_change_token_new, '')
               from auth.users where id = $1::text::uuid",
            &[&holder.user_id],
        )
        .await?;
    let row = &rows[0];
    let current: String = row.get(0);
    let status: i32 = row.get(1);
    let token_current: String = row.get(2);
    let token_new: String = row.get(3);

    if !autoconfirm && secure_change && status == 0 && !current.is_empty() {
        let spent = if hash == token_current {
            "email_change_token_current"
        } else if hash == token_new {
            "email_change_token_new"
        } else {
            // Neither column matches, which can only happen when the
            // token row outlived the column. Nothing is spent and the
            // other link still has to be followed.
            ""
        };
        if !spent.is_empty() {
            sess.execute(
                &format!(
                    "update auth.users
                        set {spent} = '', email_change_confirm_status = 1, updated_at = now()
                      where id = $1::text::uuid"
                ),
                &[&holder.user_id],
            )
            .await?;
            sess.execute(
                "delete from auth.one_time_tokens
                  where user_id = $1::text::uuid and token_type::text = $2",
                &[&holder.user_id, &spent],
            )
            .await?;
        }
        return Ok(false);
    }

    sess.execute(
        "update auth.identities i
            set identity_data = i.identity_data
                                || jsonb_build_object('email', u.email_change,
                                                      'email_verified', true),
                updated_at = now()
           from auth.users u
          where i.user_id = u.id and u.id = $1::text::uuid and i.provider = 'email'",
        &[&holder.user_id],
    )
    .await?;
    sess.execute(
        "update auth.users
            set email = email_change,
                email_change = '',
                email_change_token_current = '',
                email_change_token_new = '',
                email_change_confirm_status = 0,
                updated_at = now()
          where id = $1::text::uuid",
        &[&holder.user_id],
    )
    .await?;
    confirm_address(sess, &holder.user_id, true).await?;
    forget_tokens(sess, &holder.user_id).await?;
    Ok(true)
}

/// The whole of a verify, in one transaction: find whose code it was,
/// spend it, and start a session on the strength of it. The amr method
/// is `otp`, which is what tells a later request that this session was
/// started by a link in an email rather than by a password.
async fn verified(
    sess: &sql::Session,
    asked: &Asked,
    secure_change: bool,
    autoconfirm: bool,
    signer: &crate::jwt::Signer<'_>,
    issuer: &str,
) -> Result<Option<Issued>, Error> {
    let holder = if asked.token.is_empty() {
        by_hash(sess, &asked.kind, &asked.hash, LINK_EXPIRED).await?
    } else {
        by_email(sess, &asked.kind, &asked.email, &asked.hash).await?
    };
    if !consume(sess, &holder, &asked.hash, secure_change, autoconfirm).await? {
        return Ok(None);
    }
    Ok(Some(
        start(sess, &holder.user_id, "otp", signer, issuer).await?,
    ))
}

async fn confirm(
    pool: &Pool,
    asked: &Asked,
    secure_change: bool,
    autoconfirm: bool,
    signer: &crate::jwt::Signer<'_>,
    issuer: &str,
) -> Result<Option<Issued>, Error> {
    let sess = pool.admin().await?;
    let out = verified(&sess, asked, secure_change, autoconfirm, signer, issuer).await;
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

/// Write down a recovery code for an address, if it belongs to anyone.
/// An address nobody signed up with is answered exactly like one that
/// did, because on this endpoint the answer is the whole information:
/// anything else turns a password reset form into a list of who has an
/// account here.
async fn recovery_for(sess: &sql::Session, email: &str) -> Result<(), Error> {
    let rows = sess
        .query(
            "select id::text from auth.users
              where email = $1 and aud = $2 and deleted_at is null limit 1",
            &[&email, &AUD],
        )
        .await?;
    let Some(row) = rows.first() else {
        return Ok(());
    };
    let user_id: String = row.get(0);
    mint_code(sess, &user_id, email, "recovery_token").await?;
    Ok(())
}

/// POST /auth/v1/recover, the start of a password reset.
pub async fn send_recovery(pool: &Pool, email: &str) -> Result<(), Error> {
    let email = validate_email(email)?;
    let sess = pool.admin().await?;
    let out = recovery_for(&sess, &email).await;
    match out {
        Ok(()) => {
            sess.commit().await?;
            Ok(())
        }
        Err(e) => {
            let _ = sess.rollback().await;
            Err(e)
        }
    }
}

/// Write down a code that proves the person holding this session is
/// still the person who owns the address, which is what a password
/// change asks for when the session is old.
async fn reauth_for(sess: &sql::Session, user_id: &str) -> Result<(), Error> {
    let rows = sess
        .query(
            "select coalesce(email, ''), email_confirmed_at is not null
               from auth.users where id = $1::text::uuid and deleted_at is null",
            &[&user_id],
        )
        .await?;
    let Some(row) = rows.first() else {
        return Err(refused(
            StatusCode::FORBIDDEN,
            "user_not_found",
            "User from sub claim in JWT does not exist",
        ));
    };
    let email: String = row.get(0);
    let confirmed: bool = row.get(1);
    if email.is_empty() {
        return denied(
            "validation_failed",
            "Reauthentication requires the user to have an email or a phone number",
        );
    }
    if !confirmed {
        return Err(refused(
            StatusCode::UNPROCESSABLE_ENTITY,
            "email_not_confirmed",
            "Please verify your email first.",
        ));
    }
    mint_code(sess, user_id, &email, "reauthentication_token").await?;
    Ok(())
}

/// POST /auth/v1/reauthenticate.
pub async fn send_reauthentication(pool: &Pool, user_id: &str) -> Result<(), Error> {
    let sess = pool.admin().await?;
    let out = reauth_for(&sess, user_id).await;
    match out {
        Ok(()) => {
            sess.commit().await?;
            Ok(())
        }
        Err(e) => {
            let _ = sess.rollback().await;
            Err(e)
        }
    }
}

/// Stage a change of address: the new address and the codes that will
/// prove it, one mailed to the new address and, when the project
/// confirms both ends, one to the old. Nothing on the account moves
/// until they come back.
async fn stage_change(
    sess: &sql::Session,
    user_id: &str,
    current: &str,
    new: &str,
    secure_change: bool,
) -> Result<(), sql::Error> {
    let to_new = token_hash(new, &otp());
    let to_current = if secure_change && !current.is_empty() {
        token_hash(current, &otp())
    } else {
        String::new()
    };
    sess.execute(
        "update auth.users
            set email_change = $2,
                email_change_token_new = $3,
                email_change_token_current = $4,
                email_change_confirm_status = 0,
                email_change_sent_at = now(),
                updated_at = now()
          where id = $1::text::uuid",
        &[&user_id, &new, &to_new, &to_current],
    )
    .await?;
    keep_token(sess, user_id, "email_change_token_new", &to_new, new).await?;
    if !to_current.is_empty() {
        keep_token(
            sess,
            user_id,
            "email_change_token_current",
            &to_current,
            current,
        )
        .await?;
    }
    Ok(())
}

/// The password rules as an update applies them, which differ from a
/// signup in one place: an empty password is not called out as missing,
/// it is simply too short, because on an update there was no
/// requirement to send one at all.
fn validate_new_password(password: &str) -> Result<(), Error> {
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

/// Check the code from a reauthenticate against the one written down,
/// and spend it. The window is the same day a mailed code lives for.
async fn check_nonce(sess: &sql::Session, user_id: &str, nonce: &str) -> Result<(), Error> {
    let invalid = || {
        Err(refused(
            StatusCode::UNPROCESSABLE_ENTITY,
            "reauthentication_not_valid",
            "Nonce has expired or is invalid",
        ))
    };
    if nonce.is_empty() {
        return invalid();
    }
    let sql = format!(
        "select coalesce(reauthentication_token, ''),
                coalesce(reauthentication_sent_at > now() - interval '{OTP_EXP} seconds', false),
                coalesce(email, '')
           from auth.users where id = $1::text::uuid"
    );
    let rows = sess.query(&sql, &[&user_id]).await?;
    let row = &rows[0];
    let token: String = row.get(0);
    let fresh: bool = row.get(1);
    let email: String = row.get(2);
    if token.is_empty() || !fresh || token_hash(&email, nonce) != token {
        return invalid();
    }
    sess.execute(
        "update auth.users
            set reauthentication_token = '', updated_at = now()
          where id = $1::text::uuid",
        &[&user_id],
    )
    .await?;
    sess.execute(
        "delete from auth.one_time_tokens
          where user_id = $1::text::uuid and token_type::text = 'reauthentication_token'",
        &[&user_id],
    )
    .await?;
    Ok(())
}

/// Set the password and clear everything that could be used to set it
/// again: every outstanding code, and every session but the one asking.
/// A password change is what someone whose account was taken does, so
/// it has to be the thing that ends the intruder's access rather than
/// one more state the intruder can sit through.
async fn set_password(
    sess: &sql::Session,
    user_id: &str,
    hash: &str,
    keep_session: Option<&str>,
) -> Result<(), sql::Error> {
    sess.execute(
        "update auth.users
            set encrypted_password = $2,
                confirmation_token = '', confirmation_sent_at = null,
                recovery_token = '', recovery_sent_at = null,
                email_change_token_current = '', email_change_token_new = '',
                email_change_sent_at = null,
                phone_change_token = '', phone_change_sent_at = null,
                reauthentication_token = '', reauthentication_sent_at = null,
                updated_at = now()
          where id = $1::text::uuid",
        &[&user_id, &hash],
    )
    .await?;
    forget_tokens(sess, user_id).await?;
    // The refresh tokens and the amr rows cascade from the session, so
    // deleting the session is the whole of the logout.
    let keep = keep_session.map(str::to_string);
    sess.execute(
        "delete from auth.sessions
          where user_id = $1::text::uuid
            and ($2::text is null or id <> $2::text::uuid)",
        &[&user_id, &keep],
    )
    .await?;
    Ok(())
}

/// Whatever a PUT /user asked to change, applied in GoTrue's order.
/// The order is the contract: a client that sends a bad address and a
/// weak password together sees the same one of the two complained about
/// that it would see against hosted Supabase.
async fn update_user(
    sess: &sql::Session,
    caller: &Caller,
    body: &serde_json::Value,
    reauth_required: bool,
    secure_change: bool,
) -> Result<serde_json::Value, Error> {
    let email = match field(body, "email") {
        "" => None,
        given => Some(validate_email(given)?),
    };
    if !field(body, "phone").is_empty() {
        return Err(Error::NotYet("changing a phone number"));
    }
    let password = body.get("password").and_then(|v| v.as_str());
    if let Some(password) = password {
        validate_new_password(password)?;
    }
    let app_metadata = body.get("app_metadata").filter(|v| v.is_object());
    if app_metadata.is_some() && caller.role != "service_role" && caller.role != "supabase_admin" {
        return Err(refused(
            StatusCode::FORBIDDEN,
            "not_admin",
            "Updating app_metadata requires admin privileges",
        ));
    }

    let rows = sess
        .query(
            "select coalesce(email, ''), coalesce(encrypted_password, ''), is_sso_user
               from auth.users where id = $1::text::uuid and deleted_at is null",
            &[&caller.user_id],
        )
        .await?;
    let Some(row) = rows.first() else {
        return Err(refused(
            StatusCode::FORBIDDEN,
            "user_not_found",
            "User from sub claim in JWT does not exist",
        ));
    };
    let current: String = row.get(0);
    let stored: String = row.get(1);
    let sso: bool = row.get(2);
    if sso && (email.is_some() || password.is_some()) {
        return Err(refused(
            StatusCode::UNPROCESSABLE_ENTITY,
            "user_sso_managed",
            "Updating email, phone, password of a SSO account only possible via SSO",
        ));
    }
    if let Some(wanted) = &email
        && taken(sess, wanted, &caller.user_id).await?
    {
        return Err(refused(
            StatusCode::UNPROCESSABLE_ENTITY,
            "email_exists",
            "A user with this email address has already been registered",
        ));
    }

    if let Some(password) = password {
        if reauth_required && !recent(sess, caller.session_id.as_deref()).await? {
            let nonce = field(body, "nonce");
            if nonce.is_empty() {
                return denied(
                    "reauthentication_needed",
                    "Password update requires reauthentication",
                );
            }
            check_nonce(sess, &caller.user_id, nonce).await?;
        }
        if !stored.is_empty() && matches_off_thread(password, &stored).await {
            return Err(refused(
                StatusCode::UNPROCESSABLE_ENTITY,
                "same_password",
                "New password should be different from the old password.",
            ));
        }
        let hash = hash_off_thread(password).await;
        set_password(sess, &caller.user_id, &hash, caller.session_id.as_deref()).await?;
    }

    if let Some(data) = body.get("data").filter(|v| v.is_object()) {
        merge_metadata(sess, &caller.user_id, "raw_user_meta_data", data).await?;
    }
    if let Some(data) = app_metadata {
        merge_metadata(sess, &caller.user_id, "raw_app_meta_data", data).await?;
    }

    if let Some(wanted) = &email
        && wanted != &current
    {
        stage_change(sess, &caller.user_id, &current, wanted, secure_change).await?;
    }

    Ok(user_json(sess, &caller.user_id).await?)
}

/// Whether the address belongs to somebody else already.
async fn taken(sess: &sql::Session, email: &str, user_id: &str) -> Result<bool, sql::Error> {
    let rows = sess
        .query(
            "select 1 from auth.users
              where email = $1 and id <> $2::text::uuid and deleted_at is null limit 1",
            &[&email, &user_id],
        )
        .await?;
    Ok(!rows.is_empty())
}

/// Whether the session asking is new enough that its holder was seen
/// recently. No session at all is not recent: that is a token minted
/// from somewhere this server cannot ask about.
async fn recent(sess: &sql::Session, session_id: Option<&str>) -> Result<bool, sql::Error> {
    let Some(id) = session_id else {
        return Ok(false);
    };
    let sql = format!(
        "select coalesce(created_at > now() - interval '{REAUTH_WINDOW}', false)
           from auth.sessions where id = $1::text::uuid"
    );
    let rows = sess.query(&sql, &[&id]).await?;
    Ok(rows.first().map(|r| r.get(0)).unwrap_or(false))
}

/// Merge an object into one of the metadata columns. A key sent as null
/// is a deletion rather than a null value, which is GoTrue's rule and
/// the only way its clients have of removing a key at all.
async fn merge_metadata(
    sess: &sql::Session,
    user_id: &str,
    column: &str,
    data: &serde_json::Value,
) -> Result<(), sql::Error> {
    sess.execute(
        &format!(
            "update auth.users
                set {column} = (coalesce({column}, '{{}}'::jsonb) || $2::jsonb)
                               - coalesce((select array_agg(e.k)
                                             from jsonb_each($2::jsonb) as e(k, v)
                                            where e.v = 'null'::jsonb), '{{}}'::text[]),
                    updated_at = now()
              where id = $1::text::uuid"
        ),
        &[&user_id, &data],
    )
    .await?;
    Ok(())
}

/// Who is asking, from the bearer token the gate already verified.
pub struct Caller {
    pub user_id: String,
    pub session_id: Option<String>,
    pub role: String,
}

/// GoTrue's requireAuthentication, in its wording. The gate has already
/// refused a token this server cannot verify, so what is left to check
/// is that a token was sent at all and that it says who it is for.
fn caller(req: &Request<Body>) -> Result<Caller, Box<Response>> {
    let bad = |status: StatusCode, msg: &str| Box::new(error_body(status, "bad_jwt", msg));
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
        return Err(bad(
            StatusCode::FORBIDDEN,
            "invalid claim: missing sub claim",
        ));
    };
    let sub = field(&ctx.claims, "sub");
    if sub.is_empty() {
        return Err(bad(
            StatusCode::FORBIDDEN,
            "invalid claim: missing sub claim",
        ));
    }
    if !is_uuid(sub) {
        return Err(bad(
            StatusCode::BAD_REQUEST,
            "invalid claim: sub claim must be a UUID",
        ));
    }
    let session_id = match field(&ctx.claims, "session_id") {
        "" => None,
        id if is_uuid(id) => Some(id.to_string()),
        _ => None,
    };
    Ok(Caller {
        user_id: sub.to_string(),
        session_id,
        role: ctx.role.clone(),
    })
}

/// A cheap shape check, so a claim that was never a uuid is a refusal
/// rather than a database error further in.
fn is_uuid(s: &str) -> bool {
    s.len() == 36
        && s.chars().enumerate().all(|(i, c)| match i {
            8 | 13 | 18 | 23 => c == '-',
            _ => c.is_ascii_hexdigit(),
        })
}

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
        Error::NotYet(surface) => not_yet(surface),
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

/// POST /auth/v1/verify, where a client that holds the code, or the
/// hash from the link, trades it for a session.
pub async fn verify(
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
    let asked = match asked(&body, false) {
        Ok(v) => v,
        Err(e) => return refusal(e, "verify"),
    };
    match confirm(
        pool,
        &asked,
        app.cfg.secure_email_change,
        app.cfg.mailer_autoconfirm,
        &app.signer(),
        &app.issuer(),
    )
    .await
    {
        Ok(Some(issued)) => json_body(StatusCode::OK, issued.json()),
        // One of the two addresses has answered. The other one still
        // has to, so there is no session to hand out yet.
        Ok(None) => json_body(
            StatusCode::OK,
            serde_json::json!({"msg": SINGLE_CONFIRMATION, "code": "200"}),
        ),
        Err(e) => refusal(e, "verify"),
    }
}

/// GET /auth/v1/verify, the link in the email itself.
///
/// The answer is a redirect either way, because what follows this link
/// is a browser and what it needs is to land on the application. The
/// session rides in the url fragment, which is the one part of a url a
/// browser does not send anywhere, so the tokens reach the page without
/// reaching the server that serves it.
pub async fn verify_get(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    req: Request<Body>,
) -> Response {
    let Some(pool) = &app.pool else {
        return no_database();
    };
    let query = query_object(req.uri().query().unwrap_or_default());
    let asked = match asked(&query, true) {
        Ok(v) => v,
        // A malformed link never reaches the flow, so there is nowhere
        // trustworthy to send it and it is answered plainly, which is
        // what upstream does too.
        Err(e) => return refusal(e, "verify"),
    };
    let referrer = req
        .headers()
        .get(axum::http::header::REFERER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    let target = landing(&app, field(&query, "redirect_to"), referrer);

    match confirm(
        pool,
        &asked,
        app.cfg.secure_email_change,
        app.cfg.mailer_autoconfirm,
        &app.signer(),
        &app.issuer(),
    )
    .await
    {
        Ok(Some(issued)) => {
            let fragment = vec![
                ("access_token", issued.access_token.clone()),
                ("expires_at", issued.expires_at.to_string()),
                ("expires_in", issued.expires_in.to_string()),
                ("refresh_token", issued.refresh_token.clone()),
                ("sb", String::new()),
                ("token_type", "bearer".to_string()),
                ("type", asked.kind.clone()),
            ];
            redirect(&target, &fragment)
        }
        Ok(None) => redirect(
            &target,
            &vec![
                ("message", SINGLE_CONFIRMATION.to_string()),
                ("sb", String::new()),
            ],
        ),
        Err(e) => {
            let (status, code, msg) = match e {
                Error::Denied { status, code, msg } => (status, code, msg),
                Error::NotYet(surface) => return not_yet(surface),
                Error::Weak(_) => unreachable!("a verify never judges a password"),
                Error::Db(e) => {
                    log::error!("verify: {e}");
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "unexpected_failure",
                        "Unexpected failure, please check server logs for more information"
                            .to_string(),
                    )
                }
            };
            redirect(
                &target,
                &vec![
                    ("error", oauth_error(status).to_string()),
                    ("error_code", code.to_string()),
                    ("error_description", msg),
                    ("sb", String::new()),
                ],
            )
        }
    }
}

/// The OAuth error name a status maps to, GoTrue's own table. The
/// fragment carries both this and the finer error_code, because the
/// older clients read one and the newer ones the other.
fn oauth_error(status: StatusCode) -> &'static str {
    match status {
        StatusCode::UNAUTHORIZED => "unauthorized_client",
        StatusCode::FORBIDDEN => "access_denied",
        StatusCode::INTERNAL_SERVER_ERROR => "server_error",
        StatusCode::SERVICE_UNAVAILABLE => "temporarily_unavailable",
        _ => "invalid_request",
    }
}

/// Where a followed link lands: the redirect_to it carries when that is
/// somewhere this project owns, otherwise wherever the click came from
/// on the same terms, otherwise the site url. An open redirect here
/// would turn every confirmation email into a phishing hop, so an
/// address that is not ours is dropped rather than refused.
fn landing(app: &App, wanted: &str, referrer: &str) -> String {
    let site = app.site_url();
    for candidate in [wanted, referrer] {
        if !candidate.is_empty() && same_site(&site, candidate) {
            return candidate.to_string();
        }
    }
    site
}

/// Whether two urls are the same site: same scheme, same host, same
/// port. The port is ignored on loopback, where the api and the app
/// are two ports of the same laptop and always will be.
fn same_site(site: &str, candidate: &str) -> bool {
    let (Ok(site), Ok(candidate)) = (site.parse::<axum::http::Uri>(), candidate.parse()) else {
        return false;
    };
    let candidate: axum::http::Uri = candidate;
    if site.scheme_str() != candidate.scheme_str() || site.host() != candidate.host() {
        return false;
    }
    let loopback = matches!(
        site.host(),
        Some("localhost" | "127.0.0.1" | "::1" | "[::1]")
    );
    loopback || site.port_u16() == candidate.port_u16()
}

/// A 303 to the target with the pairs as its fragment, encoded the way
/// Go's url.Values encodes them: sorted by key, spaces as plus. The
/// supabase clients parse the fragment themselves, so matching Go here
/// is what keeps them parsing it.
fn redirect(target: &str, pairs: &Vec<(&str, String)>) -> Response {
    let mut pairs: Vec<&(&str, String)> = pairs.iter().collect();
    pairs.sort_by_key(|(k, _)| *k);
    let fragment = pairs
        .iter()
        .map(|(k, v)| format!("{}={}", query_escape(k), query_escape(v)))
        .collect::<Vec<_>>()
        .join("&");
    let location = format!("{target}#{fragment}");
    match axum::http::HeaderValue::from_str(&location) {
        Ok(value) => {
            let mut res = Response::new(Body::empty());
            *res.status_mut() = StatusCode::SEE_OTHER;
            res.headers_mut()
                .insert(axum::http::header::LOCATION, value);
            res
        }
        Err(_) => error_body(
            StatusCode::INTERNAL_SERVER_ERROR,
            "unexpected_failure",
            "Unexpected failure, please check server logs for more information",
        ),
    }
}

/// Go's url.QueryEscape: unreserved characters through, a space as a
/// plus, everything else percent encoded.
fn query_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// A query string as the same object shape a posted body has, so both
/// halves of verify read their parameters through one path.
fn query_object(query: &str) -> serde_json::Value {
    let mut out = serde_json::Map::new();
    for pair in query.split('&').filter(|p| !p.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        out.entry(crate::rest::decode(key))
            .or_insert_with(|| serde_json::Value::String(crate::rest::decode(value)));
    }
    serde_json::Value::Object(out)
}

/// POST /auth/v1/recover, the start of a password reset.
///
/// The answer is the same whether or not the address is registered, and
/// it carries nothing, because the code goes to the address rather than
/// to whoever asked.
pub async fn recover(
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
    let email = field(&body, "email");
    if email.is_empty() {
        return error_body(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            "Password recovery requires an email",
        );
    }
    match send_recovery(pool, email).await {
        Ok(()) => json_body(StatusCode::OK, serde_json::json!({})),
        Err(e) => refusal(e, "recover"),
    }
}

/// POST /auth/v1/reauthenticate, which mails a code that proves the
/// person holding this session still reads the address on it.
pub async fn reauthenticate(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    req: Request<Body>,
) -> Response {
    let Some(pool) = &app.pool else {
        return no_database();
    };
    let caller = match caller(&req) {
        Ok(v) => v,
        Err(res) => return *res,
    };
    match send_reauthentication(pool, &caller.user_id).await {
        Ok(()) => json_body(StatusCode::OK, serde_json::json!({})),
        Err(e) => refusal(e, "reauthenticate"),
    }
}

/// PUT /auth/v1/user, where the person holding a session changes their
/// own password, address or metadata.
pub async fn user_update(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    req: Request<Body>,
) -> Response {
    let Some(pool) = &app.pool else {
        return no_database();
    };
    let caller = match caller(&req) {
        Ok(v) => v,
        Err(res) => return *res,
    };
    let body = match read_json(req.into_body()).await {
        Ok(v) => v,
        Err(res) => return res,
    };
    let sess = match pool.admin().await {
        Ok(v) => v,
        Err(e) => return refusal(Error::Db(e), "user update"),
    };
    let out = update_user(
        &sess,
        &caller,
        &body,
        app.cfg.reauthentication_required,
        app.cfg.secure_email_change,
    )
    .await;
    match out {
        Ok(user) => match sess.commit().await {
            Ok(()) => json_body(StatusCode::OK, user),
            Err(e) => refusal(Error::Db(e), "user update"),
        },
        Err(e) => {
            let _ = sess.rollback().await;
            refusal(e, "user update")
        }
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
