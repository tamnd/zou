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
//! the address and the code together, and hand the code to the mailer,
//! which posts it or keeps it in the dev inbox. Every one of these
//! endpoints answers with an acknowledgement and nothing else whether
//! or not there was anything to send, which is what they answer
//! upstream too, and what keeps them from saying who has an account
//! here.
//!
//! Every one of these flows writes down what it did. The entries are in
//! `audit`, on the connection the flow is already holding, so a flow
//! that rolled back left no trace of having happened.

use std::sync::Arc;

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use axum::response::Response;

use crate::audit::{self, Action, Actor};
use crate::sql::{self, Pool};
use crate::{App, json_body, no_content, not_yet};

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

/// What a grant needs in order to hand back a session, beyond the
/// rows it writes: what signs the access token, who it says issued it,
/// the project's hooks, and the address the request came from, which
/// is the one thing a hook is told that the database cannot answer for
/// itself.
pub struct Mint<'a> {
    signer: crate::jwt::Signer<'a>,
    issuer: String,
    hook: &'a crate::hook::Settings,
    ip: String,
}

/// A project with nothing hooked, which is what a caller that has no
/// configuration to hand over gets.
static NO_HOOKS: crate::hook::Settings = crate::hook::Settings::none();

impl<'a> Mint<'a> {
    /// What this server mints with, for this request.
    pub(crate) fn of(app: &'a App, req: &Request<Body>) -> Mint<'a> {
        Mint::at(app, client_ip(req))
    }

    /// The same, for a flow that has already read the address off the
    /// request and left the request behind.
    pub(crate) fn at(app: &'a App, ip: String) -> Mint<'a> {
        Mint {
            signer: app.signer(),
            issuer: app.issuer(),
            hook: &app.cfg.hook,
            ip,
        }
    }

    /// A mint with no hooks and no request behind it, for a caller
    /// outside the HTTP surface.
    pub fn plain(signer: crate::jwt::Signer<'a>, issuer: &str) -> Mint<'a> {
        Mint {
            signer,
            issuer: issuer.to_string(),
            hook: &NO_HOOKS,
            ip: String::new(),
        }
    }
}

/// The address the request came from, as GoTrue reads it: the first
/// address in X-Forwarded-For that parses, then the peer. Nothing in
/// front of this server is trusted to be there, so an unproxied request
/// with no peer address falls back to the unspecified address, and two
/// requests that both fall back match each other.
///
/// It is what an mfa challenge is pinned to and what a hook is told,
/// and both want the same answer to the same question.
pub(crate) fn client_ip(req: &Request<Body>) -> String {
    if let Some(header) = req
        .headers()
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
    {
        for candidate in header.split(',') {
            if let Ok(ip) = candidate.trim().parse::<std::net::IpAddr>() {
                return ip.to_string();
            }
        }
    }
    req.extensions()
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
        .map(|peer| peer.0.ip().to_string())
        .unwrap_or_else(|| "0.0.0.0".to_string())
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
    /// The project's custom access token hook refused, or broke. It
    /// answers like `Denied` and it is told apart from one because of
    /// when it happens: the grant has already written everything it
    /// was going to write, so the transaction has to roll back rather
    /// than commit a session nobody was given.
    Hook {
        status: StatusCode,
        code: &'static str,
        msg: String,
    },
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
pub(crate) fn denied<T>(code: &'static str, msg: &str) -> Result<T, Error> {
    Err(refused(StatusCode::BAD_REQUEST, code, msg))
}

pub(crate) fn refused(status: StatusCode, code: &'static str, msg: &str) -> Error {
    Error::Denied {
        status,
        code,
        msg: msg.to_string(),
    }
}

/// GoTrue's send frequency limit: one account, one code, then a wait.
/// The wording and the arithmetic are upstream's, seconds truncated
/// rather than rounded, because a client shows this string to a person
/// staring at a form.
fn too_soon(code: &'static str, seconds: i64) -> Error {
    Error::Denied {
        status: StatusCode::TOO_MANY_REQUESTS,
        code,
        msg: format!("For security purposes, you can only request this after {seconds} seconds."),
    }
}

/// The two codes the frequency limit is refused under. The words are
/// the same either way, the code is what a client branches on.
const TOO_SOON_MAIL: &str = "over_email_send_rate_limit";
const TOO_SOON_SMS: &str = "over_sms_send_rate_limit";

/// Everything a flow needs to get a code to the person it belongs to:
/// what to send it with, and where the link in it should point.
pub struct Post<'a> {
    pub sender: &'a Arc<dyn crate::mail::Sender>,
    pub settings: &'a crate::mail::Settings,
    /// The same pair for text messages. They travel together because
    /// half of these flows can be reached with either an address or a
    /// number and the flow itself should not have to ask the app which
    /// it is holding.
    pub texter: &'a Arc<dyn crate::sms::Sender>,
    pub sms: &'a crate::sms::Settings,
    /// The base every link is built on, this server's external url.
    pub external: String,
    pub site: String,
    /// Where a followed link should land, already checked against the
    /// site url, which is what upstream's getReferrer hands the mailer.
    pub referrer: String,
    /// Whether the project confirms its own signups, which decides
    /// whether a confirmation is posted at all.
    pub autoconfirm: bool,
    /// How much mail and how many text messages the whole server will
    /// send an hour. It travels with the sender because every flow that
    /// posts anything already carries this.
    pub limits: &'a crate::limit::Limits,
}

/// Build one from the app and from where this request asked a followed
/// link to land. The wanted target is honoured on the same terms the
/// followed link itself is: same scheme and host as the site url, or
/// the site url instead.
pub fn posting<'a>(app: &'a App, wanted: &str, referer: &str) -> Post<'a> {
    Post {
        sender: &app.mailer,
        settings: &app.cfg.mail,
        texter: &app.texter,
        sms: &app.cfg.sms,
        external: app
            .cfg
            .external_url
            .clone()
            .unwrap_or_else(|| "http://localhost:9999".to_string()),
        site: app.site_url(),
        referrer: landing(app, wanted, referer),
        autoconfirm: app.cfg.mailer_autoconfirm,
        limits: &app.limits,
    }
}

/// The Referer of a request, which is the second place a link target
/// is looked for and has to be read before the body is consumed.
fn referer(req: &Request<Body>) -> String {
    req.headers()
        .get("referer")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string()
}

/// Where this request asked a followed link to land, and where it came
/// from, both read before the body is consumed. Upstream takes the
/// redirect from the query string on these endpoints rather than from
/// the body, and falls back to the Referer.
pub(crate) fn link_target(req: &Request<Body>) -> (String, String) {
    let query = query_object(req.uri().query().unwrap_or_default());
    (field(&query, "redirect_to").to_string(), referer(req))
}

/// One code, as it is held: the digits that go in the email and the
/// hash of them that goes in the database.
pub(crate) struct Code {
    pub(crate) code: String,
    pub(crate) hash: String,
}

/// What one outgoing code is about.
pub(crate) struct Outgoing<'a> {
    /// Which template renders it.
    pub(crate) template: &'a str,
    /// The type the link carries, which is what verify branches on and
    /// is not always the template's own name.
    pub(crate) kind: &'a str,
    /// Where it goes, which for a change of address is not the address
    /// on the account.
    pub(crate) to: &'a str,
    pub(crate) code: &'a Code,
    /// The address being moved to, for the change of address templates.
    pub(crate) new_email: &'a str,
}

/// Render one code into its email and hand it to the sender.
///
/// This runs inside the flow's transaction on purpose. A send that
/// fails takes the token it was carrying with it, so the account is
/// never left holding a code that nobody was told, and the next
/// attempt draws a fresh one.
pub(crate) async fn send_code(
    sess: &sql::Session,
    post: &Post<'_>,
    user_id: &str,
    out: Outgoing<'_>,
) -> Result<(), Error> {
    // The whole server's mail budget, spent where upstream spends it,
    // in the one function every outgoing email goes through.
    post.limits.email_sent(post.autoconfirm)?;
    let rows = sess
        .query(
            "select coalesce(email, ''), coalesce(raw_user_meta_data, '{}'::jsonb)
               from auth.users where id = $1::text::uuid",
            &[&user_id],
        )
        .await?;
    let (email, data): (String, serde_json::Value) = match rows.first() {
        Some(row) => (row.get(0), row.get(1)),
        None => (String::new(), serde_json::json!({})),
    };
    let vars = crate::mail::Vars {
        site_url: post.site.clone(),
        confirmation_url: crate::mail::action_link(
            &post.external,
            post.settings.path(out.template),
            out.kind,
            &out.code.hash,
            &post.referrer,
        ),
        email,
        new_email: out.new_email.to_string(),
        sending_to: out.to.to_string(),
        token: out.code.code.clone(),
        token_hash: out.code.hash.clone(),
        redirect_to: post.referrer.clone(),
        data,
    };
    let mail = crate::mail::compose(post.settings, out.template, out.to, &vars);
    match crate::mail::post(post.sender, mail).await {
        Ok(()) => Ok(()),
        Err(e) => {
            log::error!("sending the {} email failed: {e}", out.template);
            Err(refused(
                StatusCode::INTERNAL_SERVER_ERROR,
                "unexpected_failure",
                sending_failed(out.template),
            ))
        }
    }
}

/// Upstream's message for a send that did not happen, per template.
fn sending_failed(template: &str) -> &'static str {
    match template {
        crate::mail::RECOVERY => "Error sending recovery email",
        crate::mail::MAGIC_LINK => "Error sending magic link email",
        crate::mail::EMAIL_CHANGE => "Error sending email change email",
        crate::mail::REAUTHENTICATION => "Error sending reauthentication email",
        crate::mail::INVITE => "Error sending invite email",
        _ => "Error sending confirmation email",
    }
}

/// How long this account still has to wait before another code may go
/// out on this column, refused when that is any time at all.
async fn within_limit(
    sess: &sql::Session,
    user_id: &str,
    column: &str,
    max_frequency: u64,
    code: &'static str,
) -> Result<(), Error> {
    if max_frequency == 0 {
        return Ok(());
    }
    let rows = sess
        .query(
            &format!(
                "select trunc(extract(epoch from
                          ({column} + make_interval(secs => $2::int) - now())))::int
                   from auth.users where id = $1::text::uuid and {column} is not null"
            ),
            &[&user_id, &(max_frequency as i32)],
        )
        .await?;
    match rows.first().map(|r| r.get::<_, i32>(0)) {
        Some(left) if left > 0 => Err(too_soon(code, left as i64)),
        _ => Ok(()),
    }
}

/// GoTrue's validatePhone: the plus and the spaces come off, and what
/// is left has to be E.164. The column holds the stripped form, so two
/// people who typed the same number two ways are one account.
pub(crate) fn validate_phone(phone: &str) -> Result<String, Error> {
    let phone = crate::sms::strip(phone);
    if !crate::sms::e164(&phone) {
        return denied(
            "validation_failed",
            "Invalid phone number format (E.164 required)",
        );
    }
    Ok(phone)
}

/// Upstream's InvalidChannelError, word for word, including the two
/// provider names, because it is what a client shows a person.
const INVALID_CHANNEL: &str = "Invalid channel, supported values are 'sms' or 'whatsapp'. 'whatsapp' is only supported if Twilio or Twilio Verify is used as the provider.";

/// Which channel a phone request asked for. Unset is sms, which is
/// upstream's backwards compatible default, and whatever is asked for
/// has to be something the configured provider actually carries.
fn channel_of(body: &serde_json::Value, post: &Post<'_>) -> Result<String, Error> {
    let channel = match field(body, "channel") {
        "" => crate::sms::SMS,
        other => other,
    };
    if !post.texter.carries(channel) {
        return denied("validation_failed", INVALID_CHANNEL);
    }
    Ok(channel.to_string())
}

/// What one outgoing text is about. The otp type is upstream's own
/// name for it and it decides three things at once: which column holds
/// the code, which sent_at the frequency limit reads, and which words
/// a failed send is refused with.
pub(crate) struct Texting<'a> {
    pub(crate) otp_type: &'a str,
    pub(crate) to: &'a str,
    pub(crate) channel: &'a str,
}

/// The phone otp types, upstream's strings.
pub(crate) const PHONE_CONFIRMATION: &str = "confirmation";
pub(crate) const PHONE_CHANGE: &str = "phone_change";
pub(crate) const PHONE_REAUTHENTICATION: &str = "reauthentication";

/// Draw a code, write it down, and text it. The answer is the
/// provider's id for the message, which the otp endpoint hands back and
/// which is empty for the dev sink.
///
/// Like the mail side this runs inside the flow's transaction, so a
/// send that fails takes its token with it and the next attempt draws a
/// fresh one.
pub(crate) async fn send_phone_code(
    sess: &sql::Session,
    post: &Post<'_>,
    user_id: &str,
    out: Texting<'_>,
) -> Result<String, Error> {
    let (token_type, sent) = match out.otp_type {
        PHONE_CHANGE => ("phone_change_token", "phone_change_sent_at"),
        PHONE_REAUTHENTICATION => ("reauthentication_token", "reauthentication_sent_at"),
        _ => ("confirmation_token", "confirmation_sent_at"),
    };
    within_limit(sess, user_id, sent, post.sms.max_frequency, TOO_SOON_SMS).await?;
    if out.otp_type == PHONE_CHANGE {
        // The number being moved to is staged before the code that
        // proves it, because the code is hashed against it and verify
        // finds the account by it.
        sess.execute(
            "update auth.users set phone_change = $2, updated_at = now()
              where id = $1::text::uuid",
            &[&user_id, &out.to],
        )
        .await?;
    }
    // The same for text messages, before the code is drawn, because a
    // code drawn and not sent is a code the account is left holding.
    post.limits.sms_sent(post.sms.autoconfirm)?;
    let code = mint_digits(sess, user_id, out.to, token_type, post.sms.digits()).await?;
    let text = crate::sms::Text {
        to: out.to.to_string(),
        body: post.sms.body(&code.code),
        code: code.code.clone(),
        channel: out.channel.to_string(),
        at: now(),
    };
    match crate::sms::post(post.texter, text).await {
        Ok(id) => Ok(id),
        Err(e) => {
            log::error!("sending the {} sms failed: {e}", out.otp_type);
            Err(refused(
                StatusCode::UNPROCESSABLE_ENTITY,
                "sms_send_failed",
                &format!("Error sending {} OTP to provider: {e}", out.otp_type),
            ))
        }
    }
}

/// Seconds since the epoch.
pub(crate) fn now() -> i64 {
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
pub(crate) fn user_object() -> String {
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
            || {new_email} || {new_phone} || {factors})",
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
        factors = factors_of(),
    )
}

/// The factor list, as the key that is only there when the account has
/// one. GoTrue eager loads factors with the user and Go's omitempty
/// leaves an empty slice out entirely, so a client that has never
/// touched MFA sees exactly what it saw before.
///
/// This is a subquery rather than a join because every caller of
/// [`user_object`] already carries the identity join and none of them
/// should have to learn about a second one.
fn factors_of() -> String {
    format!(
        "(select case when count(*) = 0 then '{{}}'::jsonb
                      else jsonb_build_object('factors', jsonb_agg(
                               jsonb_build_object(
                                   'id', f.id::text,
                                   'created_at', {created},
                                   'updated_at', {updated},
                                   'status', f.status::text,
                                   'factor_type', f.factor_type::text,
                                   'phone', coalesce(f.phone, ''),
                                   'last_challenged_at', case
                                       when f.last_challenged_at is null then null
                                       else {challenged} end
                               ) || {friendly}
                               order by f.created_at)) end
            from auth.mfa_factors f where f.user_id = u.id)",
        created = ts("f.created_at"),
        updated = ts("f.updated_at"),
        challenged = ts("f.last_challenged_at"),
        friendly = opt_text("friendly_name", "f.friendly_name"),
    )
}

/// The identity list, joined the way both user queries need it.
pub(crate) fn identities_join() -> String {
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
pub(crate) async fn user_json(
    sess: &sql::Session,
    user_id: &str,
) -> Result<serde_json::Value, sql::Error> {
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
/// changing. The amr entries are ordered most recent first and stamped
/// with `updated_at` rather than `created_at`, which is what upstream's
/// CalculateAALAndAMR does: a method proved again moves up the list
/// rather than staying where it first appeared.
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
                        'timestamp', floor(extract(epoch from a.updated_at))::bigint
                    ) order by a.updated_at desc) as list
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
/// of every grant, and the one place a token is signed at all.
///
/// `method` is how the person proved themselves on this request, which
/// is not always what the session's amr rows say: a refresh proves
/// nothing new, and the hook is told so.
///
/// The project's custom access token hook runs here, between the
/// claims being built and the token being signed, which is where
/// upstream runs it. What it hands back replaces the claim set rather
/// than being merged into it. `expires_at` in the answer is still this
/// server's arithmetic, upstream's too: a hook that rewrites `exp`
/// changes the token without changing the body around it.
pub(crate) async fn mint_for(
    sess: &sql::Session,
    session_id: &str,
    refresh_token: String,
    method: &str,
    mint: &Mint<'_>,
) -> Result<Issued, Error> {
    let iat = now();
    let exp = iat + ACCESS_TTL;
    let (mut claims, user) = describe(sess, session_id, iat, exp, &mint.issuer).await?;
    let point = &mint.hook.custom_access_token;
    if point.live() {
        let input = crate::hook::input(&claims, method, &mint.ip);
        claims = crate::hook::customize(sess, point, &input).await?;
    }
    Ok(Issued {
        access_token: mint.signer.sign(&claims),
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
    mint: &Mint<'_>,
) -> Result<Issued, Error> {
    let sess = pool.admin().await?;
    let issued = start(&sess, user_id, method, mint).await;
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
    mint: &Mint<'_>,
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
    mint_for(sess, &session_id, token, method, mint).await
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
    /// Who the token belongs to, carried for the audit entry and for
    /// nothing else.
    user_id: String,
}

/// The refresh_token grant. Rotation with reuse detection, in GoTrue's
/// order: find, judge, rotate, mint.
pub async fn refresh(pool: &Pool, token: &str, mint: &Mint<'_>) -> Result<Issued, Error> {
    let sess = pool.admin().await?;
    let out = rotate(&sess, token, mint).await;
    // A refusal can still have written: an orphaned token is deleted
    // and a stolen one takes its whole family down with it. Both have
    // to survive the response, so the transaction commits either way
    // and only a database error rolls back.
    //
    // A hook that refused is the other rollback, and it has to be: the
    // rotation has already happened by the time the hook runs, so
    // committing it would spend the client's refresh token on a token
    // it never received and lock it out of a session it is still
    // entitled to.
    match out {
        Err(e @ (Error::Db(_) | Error::Hook { .. })) => {
            let _ = sess.rollback().await;
            Err(e)
        }
        other => {
            sess.commit().await?;
            other
        }
    }
}

async fn rotate(sess: &sql::Session, token: &str, mint: &Mint<'_>) -> Result<Issued, Error> {
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
                    coalesce(floor(extract(epoch from now() - t.updated_at))::bigint, 0),
                    coalesce(t.user_id, '')
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
        user_id: row.get(8),
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

    let already = if found.revoked {
        match reused(sess, &session_id, &found).await? {
            Revoked::Answered(active) => Some(active),
            Revoked::Rotate => None,
            Revoked::Stolen => {
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
        None
    };

    // Every refresh that is going to be answered says so, including the
    // one that hands back a token it has already issued. The theft path
    // returned before reaching here, so a refused refresh leaves a trail
    // of the revocations it caused and no refreshed entry at all.
    //
    // Before the swap rather than after, because the swap writes the
    // revoked entry, and refreshed then revoked is the pair upstream
    // leaves behind.
    audit::record(
        sess,
        Actor::Account(&found.user_id),
        Action::TokenRefreshed,
        "",
        None,
    )
    .await?;

    let issued = match already {
        Some(active) => active,
        None => swap(sess, found.id).await?,
    };

    sess.execute(
        "update auth.sessions
            set updated_at = now(), refreshed_at = now() at time zone 'utc'
          where id = $1::text::uuid",
        &[&session_id],
    )
    .await?;
    // A refresh proves nothing new about who is asking, and
    // upstream tells the hook exactly that rather than repeating
    // whatever the session was first proved with.
    mint_for(sess, &session_id, issued, "token_refresh", mint).await
}

/// What a revoked token turned out to be.
enum Revoked {
    /// The parent of the session's live token, which means the client
    /// never received the answer to its last refresh. It gets that same
    /// answer again, and nothing rotates.
    Answered(String),
    /// Inside the reuse window, so it rotates like any other.
    Rotate,
    /// None of the above.
    Stolen,
}

/// Judge a revoked token that was presented anyway. It is an enum rather
/// than the rotation itself because the caller has an entry to write
/// between the judgement and the rotation.
async fn reused(
    sess: &sql::Session,
    session_id: &str,
    found: &Presented,
) -> Result<Revoked, Error> {
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
            return Ok(Revoked::Answered(active));
        }
    }
    // Zero is not a one second window, it is no window at all, which is
    // why the interval has to be positive before age is even asked.
    if REUSE_INTERVAL > 0 && found.age < REUSE_INTERVAL {
        return Ok(Revoked::Rotate);
    }
    Ok(Revoked::Stolen)
}

/// Revoke the presented token and issue its child. The parent link is
/// what lets the next request tell a lost response from a stolen
/// token, so it is written even though nothing reads it on the happy
/// path.
///
/// The revoked entry is written here rather than at the two call sites,
/// which is where upstream keeps it too: a swap is the only thing that
/// revokes a token that its holder was still entitled to, and a refresh
/// that handed back the token it had already issued is not one, so it
/// writes no revoked entry.
pub(crate) async fn swap(sess: &sql::Session, id: i64) -> Result<String, Error> {
    let rows = sess
        .query(
            "select coalesce(user_id, '') from auth.refresh_tokens where id = $1",
            &[&id],
        )
        .await?;
    if let Some(row) = rows.first() {
        let user_id: String = row.get(0);
        audit::record(
            sess,
            Actor::Account(&user_id),
            Action::TokenRevoked,
            "",
            None,
        )
        .await?;
    }
    sess.execute(
        "update auth.refresh_tokens set revoked = true, updated_at = now() where id = $1",
        &[&id],
    )
    .await?;
    let token = fresh_token();
    sess.execute(
        "insert into auth.refresh_tokens
             (token, user_id, revoked, created_at, updated_at, parent, session_id)
         select $1, user_id, false, now(), now(), token, session_id
         from auth.refresh_tokens where id = $2",
        &[&token, &id],
    )
    .await?;
    Ok(token)
}

/// A six digit one time code, GoTrue's MAILER_OTP_LENGTH default. It is
/// what the confirmation email carries, and it is drawn uniformly
/// rather than from the low bits of a timestamp.
pub(crate) fn six_digits() -> String {
    code_of(6)
}

/// The same for a code of any length, which is what the SMS side needs:
/// GOTRUE_SMS_OTP_LENGTH is six by default and an operator may take it
/// up to ten.
///
/// Each digit is drawn on its own rather than one number being taken
/// modulo a power of ten, because ten does not divide 256 either and
/// the fold would favour the low digits. A byte in the top of the
/// range is thrown away instead.
pub(crate) fn code_of(digits: usize) -> String {
    let mut out = String::with_capacity(digits);
    let mut raw = [0u8; 1];
    for _ in 0..digits {
        loop {
            getrandom::fill(&mut raw).expect("the os rng never fails");
            if raw[0] < 250 {
                break;
            }
        }
        out.push(char::from(b'0' + raw[0] % 10));
    }
    out
}

/// What is stored for a one time code: the hex of sha224 over the
/// address and the code together, which is GoTrue's GenerateTokenHash.
/// The code itself is never written down, so a database that leaks does
/// not hand out working confirmation links.
pub(crate) fn token_hash(email: &str, otp: &str) -> String {
    use sha2::Digest;
    let digest = sha2::Sha224::digest(format!("{email}{otp}").as_bytes());
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

/// What a signup turned into. A project that confirms its own signups
/// gets a session straight away, one that mails a confirmation gets the
/// user and nothing else, which is the difference a client watches for.
///
/// `Taken` is the refusal, and it is an outcome rather than an error
/// because of the audit entry: upstream records a repeated signup and
/// then refuses, in that order, so the transaction has to commit before
/// the 422 is written. Nothing else is in it by the time it commits,
/// because the account already existed and the check that found it runs
/// before anything is written.
pub enum SignedUp {
    Session(Box<Issued>),
    Pending(serde_json::Value),
    Taken,
}

/// GoTrue's email address check: present, short enough, one address,
/// lowercased. The wording of every refusal is upstream's, because a
/// client that surfaces the message to a person shows the same words.
///
/// The format rule is the shape of upstream's regex rather than the
/// regex itself: exactly one @, something either side of it, no
/// whitespace anywhere, and a domain that is dotted, which is what
/// keeps user@localhost out.
pub(crate) fn validate_email(email: &str) -> Result<String, Error> {
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
pub(crate) fn validate_password(password: &str) -> Result<(), Error> {
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
    mint: &Mint<'_>,
    post: &Post<'_>,
) -> Result<SignedUp, Error> {
    let autoconfirm = post.autoconfirm;
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
        Some((id, true)) => {
            // The entry is written either way. It is the only thing this
            // branch writes, which is what lets the refusal commit it.
            audit::record(
                sess,
                Actor::Account(&id),
                Action::UserRepeatedSignUp,
                "",
                Some(serde_json::json!({ "provider": "email" })),
            )
            .await?;
            if autoconfirm || post.sms.autoconfirm {
                return Ok(SignedUp::Taken);
            }
            return Ok(SignedUp::Pending(
                sanitized(sess, "email", email, data).await?,
            ));
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
        audit::record(
            sess,
            Actor::Account(&user_id),
            Action::UserConfirmationRequested,
            "",
            Some(serde_json::json!({ "provider": "email" })),
        )
        .await?;
        // The code goes in the email and its hash goes here, one live
        // confirmation per user, which is upstream's rule: a second
        // signup on the same unconfirmed address replaces the first
        // code rather than leaving two that both work.
        within_limit(
            sess,
            &user_id,
            "confirmation_sent_at",
            post.settings.max_frequency,
            TOO_SOON_MAIL,
        )
        .await?;
        let code = mint_code(sess, &user_id, email, "confirmation_token").await?;
        send_code(
            sess,
            post,
            &user_id,
            Outgoing {
                template: crate::mail::CONFIRMATION,
                kind: "signup",
                to: email,
                code: &code,
                new_email: "",
            },
        )
        .await?;
        return Ok(SignedUp::Pending(user_json(sess, &user_id).await?));
    }

    audit::record(
        sess,
        Actor::Account(&user_id),
        Action::UserSignedUp,
        "",
        Some(serde_json::json!({ "provider": "email" })),
    )
    .await?;
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
    // Upstream signs the person in from a second transaction once the
    // signup has committed, so the trail holds a signup and then a
    // login. Here they are one transaction, which is the same two
    // entries in the same order and no window where the account exists
    // and the session does not.
    audit::record(
        sess,
        Actor::Account(&user_id),
        Action::Login,
        "",
        Some(serde_json::json!({ "provider": "email" })),
    )
    .await?;
    Ok(SignedUp::Session(Box::new(
        start(sess, &user_id, "password", mint).await?,
    )))
}

/// The answer a project that sends its confirmations gives when the
/// address or the number is already taken: a user object that belongs to
/// nobody. The id is fresh, there are no identities, and the timestamps
/// are all now, so nothing in it distinguishes a taken address from a
/// free one.
async fn sanitized(
    sess: &sql::Session,
    provider: &str,
    to: &str,
    data: &serde_json::Value,
) -> Result<serde_json::Value, sql::Error> {
    let sql = format!(
        "select jsonb_build_object(
                    'id', gen_random_uuid()::text,
                    'aud', $2::text,
                    'role', '',
                    'email', case when $4::text = 'email' then $1::text else '' end,
                    'phone', case when $4::text = 'phone' then $1::text else '' end,
                    'app_metadata', jsonb_build_object(
                        'provider', $4::text,
                        'providers', jsonb_build_array($4::text)),
                    'user_metadata', $3::jsonb,
                    'identities', '[]'::jsonb,
                    'created_at', {now},
                    'updated_at', {now},
                    'confirmation_sent_at', {now},
                    'is_anonymous', false
                )::text",
        now = ts("now()"),
    );
    let rows = sess.query(&sql, &[&to, &AUD, &data, &provider]).await?;
    Ok(serde_json::from_str(rows[0].get::<_, &str>(0))
        .expect("jsonb_build_object always produces json"))
}

/// The phone half of a signup, which is the email half with the columns
/// swapped and the code going out by text. The account is unusable until
/// the number answers, exactly as an unconfirmed address is.
///
/// One argument longer than `register` because a text has a channel and
/// a mail does not.
#[allow(clippy::too_many_arguments)]
async fn register_phone(
    sess: &sql::Session,
    phone: &str,
    hash: &str,
    channel: &str,
    data: &serde_json::Value,
    mint: &Mint<'_>,
    post: &Post<'_>,
) -> Result<SignedUp, Error> {
    let rows = sess
        .query(
            "select id::text, phone_confirmed_at is not null
             from auth.users
             where phone = $1 and aud = $2 and is_sso_user = false and deleted_at is null
             limit 1",
            &[&phone, &AUD],
        )
        .await?;
    let existing: Option<(String, bool)> = rows.first().map(|r| (r.get(0), r.get(1)));

    let user_id = match existing {
        Some((id, true)) => {
            audit::record(
                sess,
                Actor::Account(&id),
                Action::UserRepeatedSignUp,
                "",
                Some(serde_json::json!({ "provider": "phone" })),
            )
            .await?;
            if post.autoconfirm || post.sms.autoconfirm {
                return Ok(SignedUp::Taken);
            }
            return Ok(SignedUp::Pending(
                sanitized(sess, "phone", phone, data).await?,
            ));
        }
        Some((id, false)) => id,
        None => {
            let rows = sess
                .query(
                    "insert into auth.users
                         (instance_id, id, aud, role, phone, encrypted_password,
                          raw_app_meta_data, raw_user_meta_data,
                          confirmation_token, recovery_token,
                          email_change_token_new, email_change,
                          phone_change, phone_change_token,
                          created_at, updated_at, is_anonymous, is_sso_user)
                     values ('00000000-0000-0000-0000-000000000000', gen_random_uuid(),
                             $2, 'authenticated', $1, $3,
                             jsonb_build_object('provider', 'phone',
                                                'providers', jsonb_build_array('phone')),
                             $4::jsonb, '', '', '', '', '', '',
                             now(), now(), false, false)
                     returning id::text",
                    &[&phone, &AUD, &hash, &data],
                )
                .await?;
            rows[0].get(0)
        }
    };

    // Upstream leaves the number off this identity entirely, because its
    // claims struct drops empty fields and nothing fills the phone one
    // on a signup. zou writes it, the same way the email identity
    // carries the address, so an identity always says what it is for.
    sess.execute(
        "insert into auth.identities
             (provider_id, user_id, identity_data, provider,
              last_sign_in_at, created_at, updated_at)
         select $1::text::uuid::text, $1::text::uuid,
                $3::jsonb || jsonb_build_object(
                    'sub', $1::text, 'phone', $2::text, 'phone_verified', false),
                'phone', now(), now(), now()
         where not exists (
             select 1 from auth.identities
             where user_id = $1::text::uuid and provider = 'phone'
         )",
        &[&user_id, &phone, &data],
    )
    .await?;

    if !post.sms.autoconfirm {
        // The channel is not in this one, only in the confirmed branch
        // below, which is upstream's asymmetry rather than an omission
        // here.
        audit::record(
            sess,
            Actor::Account(&user_id),
            Action::UserConfirmationRequested,
            "",
            Some(serde_json::json!({ "provider": "phone" })),
        )
        .await?;
        send_phone_code(
            sess,
            post,
            &user_id,
            Texting {
                otp_type: PHONE_CONFIRMATION,
                to: phone,
                channel,
            },
        )
        .await?;
        return Ok(SignedUp::Pending(user_json(sess, &user_id).await?));
    }

    audit::record(
        sess,
        Actor::Account(&user_id),
        Action::UserSignedUp,
        "",
        Some(serde_json::json!({ "provider": "phone", "channel": channel })),
    )
    .await?;
    confirm_phone(sess, &user_id).await?;
    forget_tokens(sess, &user_id).await?;
    audit::record(
        sess,
        Actor::Account(&user_id),
        Action::Login,
        "",
        Some(serde_json::json!({ "provider": "phone" })),
    )
    .await?;
    Ok(SignedUp::Session(Box::new(
        start(sess, &user_id, "password", mint).await?,
    )))
}

/// POST /auth/v1/signup with a number and a password. One argument
/// longer than `sign_up` for the same reason `register_phone` is.
#[allow(clippy::too_many_arguments)]
pub async fn sign_up_by_phone(
    pool: &Pool,
    phone: &str,
    password: &str,
    channel: &str,
    data: &serde_json::Value,
    mint: &Mint<'_>,
    post: &Post<'_>,
) -> Result<SignedUp, Error> {
    let phone = validate_phone(phone)?;
    validate_password(password)?;
    let hash = hash_off_thread(password).await;
    let sess = pool.admin().await?;
    let out = register_phone(&sess, &phone, &hash, channel, data, mint, post).await;
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

/// POST /auth/v1/signup with an email and a password.
pub async fn sign_up(
    pool: &Pool,
    email: &str,
    password: &str,
    data: &serde_json::Value,
    mint: &Mint<'_>,
    post: &Post<'_>,
) -> Result<SignedUp, Error> {
    let email = validate_email(email)?;
    validate_password(password)?;
    // Cost 10 is tens of milliseconds of pure cpu, and it happens
    // before the connection is taken so a slow hash never holds one.
    let hash = hash_off_thread(password).await;
    let sess = pool.admin().await?;
    let out = register(&sess, &email, &hash, data, mint, post).await;
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

/// POST /auth/v1/signup carrying neither an address nor a number,
/// which is how a supabase client asks for an anonymous account.
///
/// There is nothing to prove, so there is nothing to mail and no state
/// to wait in: the answer is a session. What makes the account
/// anonymous is what it does not have. No address, no identity row, and
/// an empty app_metadata, because every other signup writes down the
/// provider that owns the account and this one has no provider. Nobody
/// asserted anything here, which is the whole of what the flag says.
pub async fn sign_up_anonymously(
    pool: &Pool,
    data: &serde_json::Value,
    mint: &Mint<'_>,
) -> Result<Issued, Error> {
    let sess = pool.admin().await?;
    let out = anonymously(&sess, data, mint).await;
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

async fn anonymously(
    sess: &sql::Session,
    data: &serde_json::Value,
    mint: &Mint<'_>,
) -> Result<Issued, Error> {
    let rows = sess
        .query(
            "insert into auth.users
                 (instance_id, id, aud, role, encrypted_password,
                  raw_app_meta_data, raw_user_meta_data,
                  confirmation_token, recovery_token,
                  email_change_token_new, email_change,
                  created_at, updated_at, is_anonymous, is_sso_user)
             values ('00000000-0000-0000-0000-000000000000', gen_random_uuid(),
                     $1, 'authenticated', '',
                     '{}'::jsonb, $2::jsonb, '', '', '', '', now(), now(), true, false)
             returning id::text",
            &[&AUD, &data],
        )
        .await?;
    let user_id: String = rows[0].get(0);
    start(sess, &user_id, "anonymous", mint).await
}

pub(crate) async fn hash_off_thread(password: &str) -> String {
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
    column: &str,
    held: &str,
    password: &str,
    mint: &Mint<'_>,
) -> Result<Issued, Error> {
    let sess = pool.admin().await?;
    let out = sign_in(&sess, column, held, password, mint).await;
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
    column: &str,
    held: &str,
    password: &str,
    mint: &Mint<'_>,
) -> Result<Issued, Error> {
    // A number is already in its stored form by the time it gets here.
    // An address is not, because people type them in whatever case they
    // like and there is only one account behind all of them.
    let held = if column == "email" {
        held.to_lowercase()
    } else {
        held.to_string()
    };
    let rows = sess
        .query(
            &format!(
                "select id::text,
                        coalesce(encrypted_password, ''),
                        coalesce(banned_until > now(), false),
                        {column}_confirmed_at is not null
                 from auth.users
                 where {column} = $1 and aud = $2 and is_sso_user = false
                   and deleted_at is null
                 limit 1"
            ),
            &[&held, &AUD],
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
        return match column {
            "phone" => denied("phone_not_confirmed", "Phone not confirmed"),
            _ => denied("email_not_confirmed", "Email not confirmed"),
        };
    }
    // The column the account was found by is the provider that signed
    // them in, which is upstream's own reasoning for the trait.
    audit::record(
        sess,
        Actor::Account(&user_id),
        Action::Login,
        "",
        Some(serde_json::json!({ "provider": column })),
    )
    .await?;
    start(sess, &user_id, "password", mint).await
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
pub(crate) async fn keep_token(
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
) -> Result<Code, sql::Error> {
    mint_digits(sess, user_id, email, token_type, 6).await
}

/// The same over a code of a given length, which is what the phone
/// flows draw. `to` is the address or the number the code is hashed
/// against, and therefore the only place it is worth anything.
async fn mint_digits(
    sess: &sql::Session,
    user_id: &str,
    to: &str,
    token_type: &str,
    digits: usize,
) -> Result<Code, sql::Error> {
    let (column, sent) = match token_type {
        "recovery_token" => ("recovery_token", "recovery_sent_at"),
        "reauthentication_token" => ("reauthentication_token", "reauthentication_sent_at"),
        "phone_change_token" => ("phone_change_token", "phone_change_sent_at"),
        _ => ("confirmation_token", "confirmation_sent_at"),
    };
    let code = code_of(digits);
    let hashed = token_hash(to, &code);
    sess.execute(
        &format!(
            "update auth.users
                set {column} = $2, {sent} = now(), updated_at = now()
              where id = $1::text::uuid"
        ),
        &[&user_id, &hashed],
    )
    .await?;
    keep_token(sess, user_id, token_type, &hashed, to).await?;
    Ok(Code { code, hash: hashed })
}

/// A verify request, GoTrue's VerifyParams. The code arrives either as
/// the digits that were sent, together with the address or the number
/// they went to, or as its hash, which is what the link in the email
/// carries. A link is only ever an email thing, so a hash on its own is
/// never a phone verification.
struct Asked {
    kind: String,
    token: String,
    hash: String,
    email: String,
    phone: String,
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
            phone: String::new(),
        });
    }
    if token.is_empty() == hash.is_empty() {
        return denied(
            "validation_failed",
            "Verify requires either a token or a token hash",
        );
    }
    if token.is_empty() {
        if !field(body, "email").is_empty()
            || !phone.is_empty()
            || !field(body, "redirect_to").is_empty()
        {
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
            phone: String::new(),
        });
    }
    let email = field(body, "email");
    // Which of the two it is comes from the request and not from the
    // type, so a number sent with type `signup` is a phone signup being
    // verified, which is exactly what the phone signup flow sends.
    if !phone.is_empty() && email.is_empty() {
        let phone = validate_phone(&phone)?;
        let hash = token_hash(&phone, &token);
        return Ok(Asked {
            kind,
            token,
            hash,
            email: String::new(),
            phone,
        });
    }
    // One or the other, never both and never neither: with both there is
    // no saying which one the code was hashed against.
    if email.is_empty() || !phone.is_empty() {
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
        phone: String::new(),
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

/// Find the user from the number, which is the only way a phone code is
/// ever verified: there is no link to follow, so there is no hash on its
/// own. A phone change is found by the number being moved to rather
/// than the one held, because the account keeps the old one until this
/// code is spent.
async fn by_phone(
    sess: &sql::Session,
    kind: &str,
    phone: &str,
    hash: &str,
    sms_exp: i64,
) -> Result<Holder, Error> {
    let (held, token, sent) = match kind {
        "sms" => ("phone", "confirmation_token", "confirmation_sent_at"),
        "phone_change" => ("phone_change", "phone_change_token", "phone_change_sent_at"),
        // Any other type sent with a number verifies nothing, because
        // no flow ever wrote a code against it.
        _ => return expired(TOKEN_EXPIRED),
    };
    let sql = format!(
        "select u.id::text,
                coalesce(u.banned_until > now(), false),
                coalesce(u.{token} = $2
                         and u.{sent} > now() - make_interval(secs => $4::int), false)
           from auth.users u
          where u.{held} = $1 and u.aud = $3 and u.deleted_at is null
          limit 1"
    );
    let rows = sess
        .query(&sql, &[&phone, &hash, &AUD, &(sms_exp as i32)])
        .await?;
    let Some(row) = rows.first() else {
        return expired(TOKEN_EXPIRED);
    };
    let user_id: String = row.get(0);
    if row.get::<_, bool>(1) {
        return banned();
    }
    if !row.get::<_, bool>(2) {
        return expired(TOKEN_EXPIRED);
    }
    Ok(Holder {
        user_id,
        kind: kind.to_string(),
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
pub(crate) async fn confirm_address(
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

/// Confirm the number. Upstream writes the two columns and stops, which
/// leaves the phone identity saying the number is unverified for ever.
/// That is the same gap zou closed on the email side, so the identity is
/// marked here too.
pub(crate) async fn confirm_phone(sess: &sql::Session, user_id: &str) -> Result<(), sql::Error> {
    sess.execute(
        "update auth.users
            set confirmation_token = '', phone_confirmed_at = now(), updated_at = now()
          where id = $1::text::uuid",
        &[&user_id],
    )
    .await?;
    sess.execute(
        "update auth.identities i
            set identity_data = i.identity_data
                                || jsonb_build_object('phone_verified', true),
                updated_at = now()
           from auth.users u
          where i.user_id = u.id and u.id = $1::text::uuid
            and i.provider = 'phone' and i.identity_data->>'phone' = u.phone",
        &[&user_id],
    )
    .await?;
    Ok(())
}

/// Move the number. The staged one becomes the held one, the identity
/// follows it, and both of the change columns are emptied so the code
/// that did this cannot do it twice.
async fn change_number(sess: &sql::Session, user_id: &str) -> Result<(), sql::Error> {
    // The identity is written before the move, while phone_change still
    // holds the number being taken.
    sess.execute(
        "insert into auth.identities
             (provider_id, user_id, identity_data, provider,
              last_sign_in_at, created_at, updated_at)
         select u.id::text, u.id,
                jsonb_build_object('sub', u.id::text, 'phone', u.phone_change,
                                   'phone_verified', true),
                'phone', now(), now(), now()
           from auth.users u
          where u.id = $1::text::uuid
            and not exists (select 1 from auth.identities i
                             where i.user_id = u.id and i.provider = 'phone')",
        &[&user_id],
    )
    .await?;
    sess.execute(
        "update auth.identities i
            set identity_data = i.identity_data
                                || jsonb_build_object('phone', u.phone_change,
                                                      'phone_verified', true),
                updated_at = now()
           from auth.users u
          where i.user_id = u.id and u.id = $1::text::uuid and i.provider = 'phone'",
        &[&user_id],
    )
    .await?;
    sess.execute(
        "update auth.users
            set phone = phone_change,
                phone_change = '',
                phone_change_token = '',
                phone_confirmed_at = now(),
                updated_at = now()
          where id = $1::text::uuid",
        &[&user_id],
    )
    .await?;
    Ok(())
}

/// An account that has proved an address or a number is not anonymous
/// any more, which is how a temporary account becomes a real one.
async fn not_anonymous(sess: &sql::Session, user_id: &str) -> Result<(), sql::Error> {
    sess.execute(
        "update auth.users set is_anonymous = false, updated_at = now()
          where id = $1::text::uuid and is_anonymous",
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

/// What a verify has to consult about the project. They travel together
/// because four call levels of bare bools in a row is where the wrong
/// one gets passed and nothing looks wrong.
pub(crate) struct Rules {
    pub(crate) secure_change: bool,
    pub(crate) autoconfirm: bool,
    pub(crate) sms_exp: i64,
}

/// The project's settings as a verify reads them.
fn rules(app: &App) -> Rules {
    Rules {
        secure_change: app.cfg.secure_email_change,
        autoconfirm: app.cfg.mailer_autoconfirm,
        sms_exp: app.cfg.sms.otp_exp,
    }
}

/// Spend the code. The answer is whether the flow finished: an email
/// change under double confirmation says no the first time, because one
/// of the two addresses has answered and the other has not.
async fn consume(
    sess: &sql::Session,
    holder: &Holder,
    hash: &str,
    rules: &Rules,
) -> Result<bool, Error> {
    match holder.kind.as_str() {
        "signup" | "invite" => {
            audit::record(
                sess,
                Actor::Account(&holder.user_id),
                Action::UserSignedUp,
                "",
                Some(serde_json::json!({ "provider": "email" })),
            )
            .await?;
            confirm_address(sess, &holder.user_id, false).await?;
            forget_tokens(sess, &holder.user_id).await?;
        }
        "recovery" | "magiclink" => {
            // A recovery link followed by an account that never proved
            // its address is the signup finishing, and is filed as one.
            // Only an account that was already confirmed is signing in,
            // and that has to be asked before the link confirms it.
            let confirmed = confirmed_address(sess, &holder.user_id).await?;
            if confirmed {
                audit::record(
                    sess,
                    Actor::Account(&holder.user_id),
                    Action::Login,
                    "",
                    None,
                )
                .await?;
            } else {
                audit::record(
                    sess,
                    Actor::Account(&holder.user_id),
                    Action::UserSignedUp,
                    "",
                    Some(serde_json::json!({ "provider": "email" })),
                )
                .await?;
            }
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
            return change_address(sess, &holder.user_id, hash, rules).await;
        }
        "sms" => {
            audit::record(
                sess,
                Actor::Account(&holder.user_id),
                Action::UserSignedUp,
                "",
                Some(serde_json::json!({ "provider": "phone" })),
            )
            .await?;
            confirm_phone(sess, &holder.user_id).await?;
            not_anonymous(sess, &holder.user_id).await?;
            forget_tokens(sess, &holder.user_id).await?;
        }
        "phone_change" => {
            audit::record(
                sess,
                Actor::Account(&holder.user_id),
                Action::UserModified,
                "",
                None,
            )
            .await?;
            change_number(sess, &holder.user_id).await?;
            not_anonymous(sess, &holder.user_id).await?;
            forget_tokens(sess, &holder.user_id).await?;
        }
        _ => return denied("validation_failed", "Unsupported verification type"),
    }
    Ok(true)
}

/// Whether the account has proved its address, asked before something is
/// about to prove it.
async fn confirmed_address(sess: &sql::Session, user_id: &str) -> Result<bool, sql::Error> {
    let rows = sess
        .query(
            "select email_confirmed_at is not null from auth.users
              where id = $1::text::uuid",
            &[&user_id],
        )
        .await?;
    Ok(rows.first().is_some_and(|r| r.get(0)))
}

/// Move the address, or record that one of the two links has been
/// followed. Under double confirmation the first answer changes
/// nothing but a status and the token that was spent, so an attacker
/// holding one of the two links moves nobody's account.
async fn change_address(
    sess: &sql::Session,
    user_id: &str,
    hash: &str,
    rules: &Rules,
) -> Result<bool, Error> {
    let rows = sess
        .query(
            "select coalesce(email, ''),
                    coalesce(email_change_confirm_status, 0)::int,
                    coalesce(email_change_token_current, ''),
                    coalesce(email_change_token_new, '')
               from auth.users where id = $1::text::uuid",
            &[&user_id],
        )
        .await?;
    let row = &rows[0];
    let current: String = row.get(0);
    let status: i32 = row.get(1);
    let token_current: String = row.get(2);
    let token_new: String = row.get(3);

    if !rules.autoconfirm && rules.secure_change && status == 0 && !current.is_empty() {
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
                &[&user_id],
            )
            .await?;
            sess.execute(
                "delete from auth.one_time_tokens
                  where user_id = $1::text::uuid and token_type::text = $2",
                &[&user_id, &spent],
            )
            .await?;
        }
        return Ok(false);
    }

    // Past the half confirmation, so the address is actually moving.
    // The entry goes here rather than at the top of this function
    // because a first link that changed nothing but a status is not a
    // modification of the account.
    audit::record(
        sess,
        Actor::Account(user_id),
        Action::UserModified,
        "",
        None,
    )
    .await?;

    // Upstream's createNewIdentity on this path. An account that has
    // never held an address has no email identity either, and the
    // address it is about to hold needs one that says who asserted it.
    // This is the anonymous account being turned into a real one.
    sess.execute(
        "insert into auth.identities
             (provider_id, user_id, identity_data, provider,
              last_sign_in_at, created_at, updated_at)
         select u.id::text, u.id,
                jsonb_build_object('sub', u.id::text, 'email', u.email_change,
                                   'email_verified', true, 'phone_verified', false),
                'email', now(), now(), now()
           from auth.users u
          where u.id = $1::text::uuid
            and not exists (select 1 from auth.identities i
                             where i.user_id = u.id and i.provider = 'email')",
        &[&user_id],
    )
    .await?;
    sess.execute(
        "update auth.identities i
            set identity_data = i.identity_data
                                || jsonb_build_object('email', u.email_change,
                                                      'email_verified', true),
                updated_at = now()
           from auth.users u
          where i.user_id = u.id and u.id = $1::text::uuid and i.provider = 'email'",
        &[&user_id],
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
        &[&user_id],
    )
    .await?;
    // Whichever way it got here: the address was taken on the spot
    // because the project confirms its own, or a link was followed.
    not_anonymous(sess, user_id).await?;
    confirm_address(sess, user_id, true).await?;
    forget_tokens(sess, user_id).await?;
    Ok(true)
}

/// The whole of a verify, in one transaction: find whose code it was,
/// spend it, and start a session on the strength of it. The amr method
/// is `otp`, which is what tells a later request that this session was
/// started by a link in an email rather than by a password.
async fn verified(
    sess: &sql::Session,
    asked: &Asked,
    rules: &Rules,
    mint: &Mint<'_>,
) -> Result<Option<Issued>, Error> {
    let holder = if asked.token.is_empty() {
        by_hash(sess, &asked.kind, &asked.hash, LINK_EXPIRED).await?
    } else if !asked.phone.is_empty() {
        by_phone(sess, &asked.kind, &asked.phone, &asked.hash, rules.sms_exp).await?
    } else {
        by_email(sess, &asked.kind, &asked.email, &asked.hash).await?
    };
    if !consume(sess, &holder, &asked.hash, rules).await? {
        return Ok(None);
    }
    Ok(Some(start(sess, &holder.user_id, "otp", mint).await?))
}

async fn confirm(
    pool: &Pool,
    asked: &Asked,
    rules: &Rules,
    mint: &Mint<'_>,
) -> Result<Option<Issued>, Error> {
    let sess = pool.admin().await?;
    let out = verified(&sess, asked, rules, mint).await;
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
async fn recovery_for(sess: &sql::Session, email: &str, post: &Post<'_>) -> Result<(), Error> {
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
    audit::record(
        sess,
        Actor::Account(&user_id),
        Action::UserRecoveryRequested,
        "",
        None,
    )
    .await?;
    within_limit(
        sess,
        &user_id,
        "recovery_sent_at",
        post.settings.max_frequency,
        TOO_SOON_MAIL,
    )
    .await?;
    let code = mint_code(sess, &user_id, email, "recovery_token").await?;
    send_code(
        sess,
        post,
        &user_id,
        Outgoing {
            template: crate::mail::RECOVERY,
            kind: "recovery",
            to: email,
            code: &code,
            new_email: "",
        },
    )
    .await?;
    Ok(())
}

/// POST /auth/v1/recover, the start of a password reset.
pub async fn send_recovery(pool: &Pool, email: &str, post: &Post<'_>) -> Result<(), Error> {
    let email = validate_email(email)?;
    let sess = pool.admin().await?;
    let out = recovery_for(&sess, &email, post).await;
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

/// A password nobody will ever type, for the account a magic link
/// creates. GoTrue draws 33 characters, and the point of it is that the
/// row has a password column that no password grant can ever satisfy.
pub(crate) fn unguessable_password() -> String {
    unguessable(33)
}

/// The same for the phone otp, which draws 64 because that is what
/// upstream draws there. The two lengths are upstream's and there is no
/// reason behind either beyond being far past guessing.
pub(crate) fn unguessable(len: usize) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut raw = vec![0u8; len];
    getrandom::fill(&mut raw).expect("the os rng never fails");
    raw.iter()
        .map(|b| ALPHABET[*b as usize % ALPHABET.len()] as char)
        .collect()
}

/// A magic link, which is a recovery code under a different name and a
/// different email template. It is the same column, the same token type
/// and the same verify, which is upstream's own arrangement: a link
/// that signs someone in and a link that lets them set a new password
/// are the same thing wearing different words.
///
/// An address nobody has signed up with is signed up here, because a
/// magic link is how a project without passwords registers people at
/// all. That is also why this endpoint says nothing about whether the
/// address was already known.
async fn magic_for(
    sess: &sql::Session,
    email: &str,
    data: &serde_json::Value,
    mint: &Mint<'_>,
    post: &Post<'_>,
) -> Result<(), Error> {
    let rows = sess
        .query(
            "select email_confirmed_at is not null from auth.users
              where email = $1 and aud = $2 and deleted_at is null limit 1",
            &[&email, &AUD],
        )
        .await?;
    let known = rows.first().map(|r| r.get::<_, bool>(0)).unwrap_or(false);
    if !known {
        // Either nobody has this address or whoever has it never proved
        // it, and both are a signup as far as this endpoint is
        // concerned. The password is drawn and thrown away.
        let hash = hash_off_thread(&unguessable_password()).await;
        register(sess, email, &hash, data, mint, post).await?;
        if !post.autoconfirm {
            // The confirmation this just wrote is the link, so there is
            // nothing else to send.
            return Ok(());
        }
    }
    let rows = sess
        .query(
            "select id::text from auth.users
              where email = $1 and aud = $2 and deleted_at is null limit 1",
            &[&email, &AUD],
        )
        .await?;
    let user_id: String = rows[0].get(0);
    // A magic link is a recovery request in the trail as well as in the
    // schema, so a signup that came in through this endpoint leaves the
    // signup entries `register` wrote and then this one.
    audit::record(
        sess,
        Actor::Account(&user_id),
        Action::UserRecoveryRequested,
        "",
        None,
    )
    .await?;
    within_limit(
        sess,
        &user_id,
        "recovery_sent_at",
        post.settings.max_frequency,
        TOO_SOON_MAIL,
    )
    .await?;
    let code = mint_code(sess, &user_id, email, "recovery_token").await?;
    send_code(
        sess,
        post,
        &user_id,
        Outgoing {
            template: crate::mail::MAGIC_LINK,
            kind: "magiclink",
            to: email,
            code: &code,
            new_email: "",
        },
    )
    .await?;
    Ok(())
}

/// POST /auth/v1/magiclink, and the email half of POST /auth/v1/otp.
pub async fn send_magic_link(
    pool: &Pool,
    email: &str,
    data: &serde_json::Value,
    mint: &Mint<'_>,
    post: &Post<'_>,
) -> Result<(), Error> {
    let email = validate_email(email)?;
    let sess = pool.admin().await?;
    let out = magic_for(&sess, &email, data, mint, post).await;
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

/// Whether anyone holds this address or this number, which is the one
/// question the otp endpoint asks before it refuses to create somebody.
async fn is_registered(pool: &Pool, column: &str, held: &str) -> Result<bool, Error> {
    let sess = pool.admin().await?;
    let rows = sess
        .query(
            &format!(
                "select 1 from auth.users
                  where {column} = $1 and aud = $2 and deleted_at is null limit 1"
            ),
            &[&held, &AUD],
        )
        .await;
    let found = match rows {
        Ok(rows) => !rows.is_empty(),
        Err(e) => {
            let _ = sess.rollback().await;
            return Err(Error::Db(e));
        }
    };
    sess.commit().await?;
    Ok(found)
}

/// Write down a code that proves the person holding this session is
/// still the person who owns the address, which is what a password
/// change asks for when the session is old.
async fn reauth_for(sess: &sql::Session, user_id: &str, post: &Post<'_>) -> Result<String, Error> {
    let rows = sess
        .query(
            "select coalesce(email, ''), email_confirmed_at is not null,
                    coalesce(phone, ''), phone_confirmed_at is not null
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
    let phone: String = row.get(2);
    let phone_confirmed: bool = row.get(3);
    if email.is_empty() && phone.is_empty() {
        return denied(
            "validation_failed",
            "Reauthentication requires the user to have an email or a phone number",
        );
    }
    // The address wins when there is one, which is upstream's order and
    // not a preference: an account with both is asked at the address.
    if email.is_empty() {
        if !phone_confirmed {
            return Err(refused(
                StatusCode::UNPROCESSABLE_ENTITY,
                "phone_not_confirmed",
                "Please verify your phone first.",
            ));
        }
        // Once per transport, after its own check, because upstream
        // writes the entry only once both checks have passed and the two
        // transports fail those checks in different places.
        reauth_requested(sess, user_id).await?;
        return send_phone_code(
            sess,
            post,
            user_id,
            Texting {
                otp_type: PHONE_REAUTHENTICATION,
                to: &phone,
                channel: crate::sms::SMS,
            },
        )
        .await;
    }
    if !confirmed {
        return Err(refused(
            StatusCode::UNPROCESSABLE_ENTITY,
            "email_not_confirmed",
            "Please verify your email first.",
        ));
    }
    reauth_requested(sess, user_id).await?;
    within_limit(
        sess,
        user_id,
        "reauthentication_sent_at",
        post.settings.max_frequency,
        TOO_SOON_MAIL,
    )
    .await?;
    let code = mint_code(sess, user_id, &email, "reauthentication_token").await?;
    send_code(
        sess,
        post,
        user_id,
        Outgoing {
            template: crate::mail::REAUTHENTICATION,
            // There is no link in this one at all, only the code, so
            // the type is the one the client posts back with it.
            kind: "reauthentication",
            to: &email,
            code: &code,
            new_email: "",
        },
    )
    .await?;
    Ok(String::new())
}

/// The one entry `reauth_for` writes, named so the two transports write
/// the same thing rather than nearly the same thing.
async fn reauth_requested(sess: &sql::Session, user_id: &str) -> Result<(), sql::Error> {
    audit::record(
        sess,
        Actor::Account(user_id),
        Action::UserReauthenticateRequested,
        "",
        None,
    )
    .await
}

/// POST /auth/v1/reauthenticate.
pub async fn send_reauthentication(
    pool: &Pool,
    user_id: &str,
    post: &Post<'_>,
) -> Result<String, Error> {
    let sess = pool.admin().await?;
    let out = reauth_for(&sess, user_id, post).await;
    match out {
        Ok(id) => {
            sess.commit().await?;
            Ok(id)
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
    post: &Post<'_>,
) -> Result<(), Error> {
    within_limit(
        sess,
        user_id,
        "email_change_sent_at",
        post.settings.max_frequency,
        TOO_SOON_MAIL,
    )
    .await?;
    let for_new = {
        let code = six_digits();
        Code {
            hash: token_hash(new, &code),
            code,
        }
    };
    let for_current = match secure_change && !current.is_empty() {
        true => {
            let code = six_digits();
            Some(Code {
                hash: token_hash(current, &code),
                code,
            })
        }
        false => None,
    };
    let to_new = for_new.hash.clone();
    let to_current = match &for_current {
        Some(code) => code.hash.clone(),
        None => String::new(),
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
    // The new address always hears about it. The old one hears too when
    // the project confirms both ends, and it is a different code in a
    // different email, because the whole point is that whoever asked
    // has to be able to read both mailboxes.
    send_code(
        sess,
        post,
        user_id,
        Outgoing {
            template: crate::mail::EMAIL_CHANGE,
            kind: "email_change",
            to: new,
            code: &for_new,
            new_email: new,
        },
    )
    .await?;
    if let Some(code) = &for_current {
        send_code(
            sess,
            post,
            user_id,
            Outgoing {
                template: crate::mail::EMAIL_CHANGE,
                kind: "email_change",
                to: current,
                code,
                new_email: new,
            },
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
async fn check_nonce(
    sess: &sql::Session,
    user_id: &str,
    nonce: &str,
    sms_exp: i64,
) -> Result<(), Error> {
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
    // The code that went out by text lives a minute by default and the
    // one that went out by mail lives a day, so which window applies
    // depends on which one was sent, which is what the account holds.
    let sql = format!(
        "select coalesce(reauthentication_token, ''),
                coalesce(reauthentication_sent_at > now() - interval '{OTP_EXP} seconds', false),
                coalesce(email, ''),
                coalesce(phone, ''),
                coalesce(reauthentication_sent_at
                         > now() - make_interval(secs => $2::int), false)
           from auth.users where id = $1::text::uuid"
    );
    let rows = sess.query(&sql, &[&user_id, &(sms_exp as i32)]).await?;
    let row = &rows[0];
    let token: String = row.get(0);
    let fresh: bool = row.get(1);
    let email: String = row.get(2);
    let phone: String = row.get(3);
    let fresh_text: bool = row.get(4);
    // An account holding no code at all is asked before it is asked
    // what the code would have been hashed against, which is upstream's
    // order and the one a client branches on.
    if token.is_empty() {
        return invalid();
    }
    let (against, fresh) = if email.is_empty() {
        (phone, fresh_text)
    } else {
        (email, fresh)
    };
    if against.is_empty() {
        return Err(refused(
            StatusCode::UNPROCESSABLE_ENTITY,
            "reauthentication_not_valid",
            "Reauthentication requires an email or a phone number",
        ));
    }
    if token.is_empty() || !fresh || token_hash(&against, nonce) != token {
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
    post: &Post<'_>,
) -> Result<serde_json::Value, Error> {
    let email = match field(body, "email") {
        "" => None,
        given => Some(validate_email(given)?),
    };
    let phone = match field(body, "phone") {
        "" => None,
        given => Some(validate_phone(given)?),
    };
    let channel = match &phone {
        Some(_) => channel_of(body, post)?,
        None => String::new(),
    };
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

    still_there(sess, caller).await?;
    let rows = sess
        .query(
            "select coalesce(email, ''), coalesce(encrypted_password, ''),
                    is_sso_user, is_anonymous, coalesce(phone, '')
               from auth.users where id = $1::text::uuid and deleted_at is null",
            &[&caller.user_id],
        )
        .await?;
    let row = &rows[0];
    let current: String = row.get(0);
    let stored: String = row.get(1);
    let sso: bool = row.get(2);
    let anonymous: bool = row.get(3);
    let held: String = row.get(4);
    // An anonymous account with a password and no address is an account
    // nobody could ever sign in to again, because there is nothing to
    // present the password with.
    if anonymous && password.is_some_and(|p| !p.is_empty()) && email.is_none() && phone.is_none() {
        return Err(refused(
            StatusCode::UNPROCESSABLE_ENTITY,
            "validation_failed",
            "Updating password of an anonymous user without an email or phone is not allowed",
        ));
    }
    if sso && (email.is_some() || password.is_some() || phone.as_deref().is_some_and(|p| p != held))
    {
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
    if let Some(wanted) = &phone
        && wanted != &held
        && held_by_another(sess, wanted, &caller.user_id).await?
    {
        return Err(refused(
            StatusCode::UNPROCESSABLE_ENTITY,
            "phone_exists",
            DUPLICATE_PHONE,
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
            check_nonce(sess, &caller.user_id, nonce, post.sms.otp_exp).await?;
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
        audit::record(
            sess,
            Actor::Account(&caller.user_id),
            Action::UserUpdatedPassword,
            "",
            None,
        )
        .await?;
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
        // An anonymous account has no address to be asked from, so a
        // project that confirms its own signups takes this one the same
        // way it takes a signup's, on the spot. Every other account
        // proves the new address first, and so does this one when the
        // project mails its confirmations.
        if anonymous && post.autoconfirm {
            sess.execute(
                "update auth.users set email_change = $2, updated_at = now()
                  where id = $1::text::uuid",
                &[&caller.user_id, wanted],
            )
            .await?;
            change_address(
                sess,
                &caller.user_id,
                "",
                &Rules {
                    secure_change: false,
                    autoconfirm: true,
                    sms_exp: 0,
                },
            )
            .await?;
        } else {
            stage_change(sess, &caller.user_id, &current, wanted, secure_change, post).await?;
        }
    }

    if let Some(wanted) = &phone
        && wanted != &held
    {
        if post.sms.autoconfirm {
            // The project asked for no proof, so the number moves on the
            // spot through the same code path a verified one takes,
            // including the entry that path writes.
            audit::record(
                sess,
                Actor::Account(&caller.user_id),
                Action::UserModified,
                "",
                None,
            )
            .await?;
            sess.execute(
                "update auth.users set phone_change = $2, updated_at = now()
                  where id = $1::text::uuid",
                &[&caller.user_id, wanted],
            )
            .await?;
            change_number(sess, &caller.user_id).await?;
            not_anonymous(sess, &caller.user_id).await?;
            forget_tokens(sess, &caller.user_id).await?;
        } else {
            send_phone_code(
                sess,
                post,
                &caller.user_id,
                Texting {
                    otp_type: PHONE_CHANGE,
                    to: wanted,
                    channel: &channel,
                },
            )
            .await?;
        }
    }

    // Last, and whatever was asked for, including nothing: upstream
    // writes this at the end of the transaction unconditionally, so a
    // PUT with an empty body still leaves a modified entry behind.
    audit::record(
        sess,
        Actor::Account(&caller.user_id),
        Action::UserModified,
        "",
        None,
    )
    .await?;
    Ok(user_json(sess, &caller.user_id).await?)
}

/// Upstream's DuplicatePhoneMsg.
pub(crate) const DUPLICATE_PHONE: &str =
    "A user with this phone number has already been registered";

/// Whether the number belongs to somebody else already.
pub(crate) async fn held_by_another(
    sess: &sql::Session,
    phone: &str,
    user_id: &str,
) -> Result<bool, sql::Error> {
    let rows = sess
        .query(
            "select 1 from auth.users
              where phone = $1 and aud = $2 and id <> $3::text::uuid
                and deleted_at is null limit 1",
            &[&phone, &AUD, &user_id],
        )
        .await?;
    Ok(!rows.is_empty())
}

/// Whether the address belongs to somebody else already.
pub(crate) async fn taken(
    sess: &sql::Session,
    email: &str,
    user_id: &str,
) -> Result<bool, sql::Error> {
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
pub(crate) async fn merge_metadata(
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
    /// The `aud` claim as the token carries it, which is the only
    /// endpoint input that decides whether this token was minted for
    /// the audience the request is being made against.
    pub aud: String,
    /// The `is_anonymous` claim. An account with no way in of its own
    /// is turned away from the endpoints that assume there is somebody
    /// to fall back on, which is what GoTrue's requireNotAnonymous is
    /// for.
    pub anonymous: bool,
}

/// GoTrue's requireAuthentication, in its wording. The gate has already
/// refused a token this server cannot verify, so what is left to check
/// is that a token was sent at all and that it says who it is for.
pub(crate) fn caller(req: &Request<Body>) -> Result<Caller, Box<Response>> {
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
        aud: field(&ctx.claims, "aud").to_string(),
        anonymous: ctx.claims["is_anonymous"].as_bool().unwrap_or(false),
    })
}

/// The half of GoTrue's requireAuthentication that needs the database:
/// the account the token names is still there, and so is the session it
/// names.
///
/// The session half is what makes logging out mean anything. Nothing
/// revokes an access token, it stays signed and stays inside its hour,
/// so a token whose session has been deleted is refused here or it is
/// not refused at all.
pub(crate) async fn still_there(sess: &sql::Session, caller: &Caller) -> Result<(), Error> {
    let rows = sess
        .query(
            "select 1 from auth.users
              where id = $1::text::uuid and deleted_at is null",
            &[&caller.user_id],
        )
        .await?;
    if rows.is_empty() {
        return Err(refused(
            StatusCode::FORBIDDEN,
            "user_not_found",
            "User from sub claim in JWT does not exist",
        ));
    }
    let Some(session_id) = &caller.session_id else {
        return Ok(());
    };
    let rows = sess
        .query(
            "select 1 from auth.sessions where id = $1::text::uuid",
            &[session_id],
        )
        .await?;
    if rows.is_empty() {
        return Err(refused(
            StatusCode::FORBIDDEN,
            "session_not_found",
            "Session from session_id claim in JWT does not exist",
        ));
    }
    Ok(())
}

/// GoTrue's requestAud, which decides what audience this request is
/// being made against: the header if one was sent, then the token's own
/// claim unless the caller holds an admin role, because those tokens
/// never had an aud claim to begin with, and the project's default
/// otherwise.
pub(crate) fn requested_aud(req: &Request<Body>, role: &str, claim: &str) -> String {
    if let Some(header) = req
        .headers()
        .get("x-jwt-aud")
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty())
    {
        return header.to_string();
    }
    if role != "service_role" && role != "supabase_admin" && !claim.is_empty() {
        return claim.to_string();
    }
    AUD.to_string()
}

/// A cheap shape check, so a claim that was never a uuid is a refusal
/// rather than a database error further in.
pub(crate) fn is_uuid(s: &str) -> bool {
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
pub(crate) fn error_body(status: StatusCode, code: &str, msg: &str) -> Response {
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
pub(crate) fn refusal(e: Error, doing: &str) -> Response {
    match e {
        Error::Denied { status, code, msg } | Error::Hook { status, code, msg } => {
            error_body(status, code, &msg)
        }
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
pub(crate) async fn read_json(body: Body) -> Result<serde_json::Value, Response> {
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

pub(crate) fn field<'a>(body: &'a serde_json::Value, key: &str) -> &'a str {
    body.get(key).and_then(|v| v.as_str()).unwrap_or_default()
}

/// GET /auth/v1/settings, which is how a sign in screen learns what to
/// draw: which social buttons to offer, whether there is a password
/// field at all, whether to show a link to sign up.
///
/// Every field GoTrue publishes is here, and a provider this project
/// does not serve says false rather than being left out. A client
/// reading a field that is not there cannot tell not offered from not
/// known, and the ones that guess guess wrong.
pub async fn settings(axum::extract::State(app): axum::extract::State<Arc<App>>) -> Response {
    let configured = |name: &str| app.cfg.oauth.get(name).is_some();
    json_body(
        StatusCode::OK,
        serde_json::json!({
            "external": {
                "anonymous_users": app.cfg.anonymous_users,
                "apple": configured("apple"),
                "azure": false,
                "bitbucket": false,
                "discord": false,
                "facebook": false,
                "snapchat": false,
                "figma": false,
                "fly": false,
                "github": configured("github"),
                "gitlab": false,
                "google": configured("google"),
                "keycloak": false,
                "kakao": false,
                "linkedin": false,
                "linkedin_oidc": false,
                "notion": false,
                "spotify": false,
                "slack": false,
                "slack_oidc": false,
                "workos": false,
                "twitch": false,
                "twitter": false,
                "email": app.cfg.email_enabled,
                "phone": app.cfg.phone_enabled,
                "zoom": false,
            },
            "disable_signup": app.cfg.disable_signup,
            "mailer_autoconfirm": app.cfg.mailer_autoconfirm,
            "phone_autoconfirm": app.cfg.sms.autoconfirm,
            "sms_provider": app.texter.provider(),
            // None of the three are built. They say so here rather than
            // going missing, for the same reason the providers do.
            "saml_enabled": false,
            "saml_private_key_next_configured": false,
            "passkeys_enabled": false,
        }),
    )
}

/// The header a client uses to ask for a newer error shape, and the one
/// this answers with when it grants the ask.
const API_VERSION: &str = "x-supabase-api-version";

/// The one version there is to ask for. GoTrue has exactly two error
/// shapes, the original and this one, and everything dated on or after
/// it gets the newer of the two.
const V2024: (u32, u32, u32) = (2024, 1, 1);

/// The envelope every auth error leaves in, GoTrue's HandleResponseError.
///
/// Two things happen here and nowhere else, because upstream does both
/// of them in one place too. A refusal a client asked for on the newer
/// api version is rewritten from `{code, error_code, msg}` into
/// `{code, message}`, where code is now the machine readable string
/// rather than the http status repeated, and the version that was
/// granted is echoed back. And a failure that is this server's own
/// fault carries the request id, so a person holding a 500 and a person
/// holding the logs have the same thing to search for.
///
/// It sits here rather than in each handler for the reason it sits in
/// one function upstream: an error shape decided in forty places is
/// forty places to get it wrong.
pub(crate) async fn envelope(req: Request<Body>, next: axum::middleware::Next) -> Response {
    let ours = req.uri().path().starts_with("/auth/v1/");
    let asked = req
        .headers()
        .get(API_VERSION)
        .and_then(|v| v.to_str().ok())
        .is_some_and(from_2024);
    let id = req
        .extensions()
        .get::<crate::edge::RequestId>()
        .map(|v| v.0.to_string());
    let res = next.run(req).await;
    // A body is only ever read back when something has to change in it,
    // which is a refusal on the newer version or a failure that owes
    // the caller a request id. Everything else is handed straight on.
    let ours_to_fill = res.status().is_server_error() && id.is_some();
    let refused = res.status().is_client_error() || res.status().is_server_error();
    if !(ours && refused && (asked || ours_to_fill)) {
        return res;
    }
    let (mut parts, body) = res.into_parts();
    let Ok(bytes) = to_bytes(body, MAX_BODY).await else {
        return error_body(
            StatusCode::INTERNAL_SERVER_ERROR,
            "unexpected_failure",
            "Unexpected failure, please check server logs for more information",
        );
    };
    let Some(mut fields) = gotrue_error(&bytes) else {
        return Response::from_parts(parts, Body::from(bytes));
    };
    let status = parts.status;
    if ours_to_fill && !fields.contains_key("error_id") {
        fields.insert("error_id".into(), id.unwrap_or_default().into());
    }
    if asked {
        let code = match fields.get("error_code").and_then(|v| v.as_str()) {
            Some(code) if !code.is_empty() => code.to_string(),
            // Upstream fills a missing code in rather than leaving the
            // field out, and which one it fills in depends on whose
            // fault the failure was.
            _ if status.is_server_error() => "unexpected_failure".to_string(),
            _ => "unknown".to_string(),
        };
        let mut newer = serde_json::Map::new();
        newer.insert("code".into(), code.into());
        newer.insert(
            "message".into(),
            fields.get("msg").cloned().unwrap_or_default(),
        );
        // A weak password says which rule it broke, and that survives
        // the rewrite because it is the only refusal with anything to
        // say beyond a sentence.
        if let Some(weak) = fields.get("weak_password") {
            newer.insert("weak_password".into(), weak.clone());
        }
        fields = newer;
        if let Ok(v) = axum::http::HeaderValue::from_str("2024-01-01") {
            parts.headers.insert(API_VERSION, v);
        }
    }
    let out = serde_json::Value::Object(fields).to_string();
    // The body is a different length now, and a stale content-length is
    // a truncated response rather than a wrong header.
    parts.headers.remove(axum::http::header::CONTENT_LENGTH);
    Response::from_parts(parts, Body::from(out))
}

/// The fields of a GoTrue error body, or nothing when this is some other
/// answer that happens to be json. The http status repeated under `code`
/// and a sentence under `msg` is the shape, and nothing else this server
/// sends looks like it.
fn gotrue_error(bytes: &[u8]) -> Option<serde_json::Map<String, serde_json::Value>> {
    let serde_json::Value::Object(fields) = serde_json::from_slice(bytes).ok()? else {
        return None;
    };
    let shaped = fields.get("code").is_some_and(serde_json::Value::is_number)
        && fields.get("msg").is_some_and(serde_json::Value::is_string);
    shaped.then_some(fields)
}

/// Whether an X-Supabase-Api-Version header names 2024-01-01 or later.
///
/// Upstream parses it as a plain date and falls back to the original
/// version for anything it cannot read, so a header that is not a date
/// and a header naming 2023 mean the same thing: the older shape. The
/// day is checked against the month because Go's parser checks it, and
/// a client sending the thirty first of February should get the same
/// answer from both.
fn from_2024(date: &str) -> bool {
    let raw = date.as_bytes();
    if raw.len() != 10 || raw[4] != b'-' || raw[7] != b'-' {
        return false;
    }
    if !raw
        .iter()
        .enumerate()
        .all(|(i, c)| i == 4 || i == 7 || c.is_ascii_digit())
    {
        return false;
    }
    let read = |from: usize, to: usize| date[from..to].parse::<u32>().unwrap_or(0);
    let (year, month, day) = (read(0, 4), read(5, 7), read(8, 10));
    if !(1..=12).contains(&month) || day < 1 || day > days_in(year, month) {
        return false;
    }
    (year, month, day) >= V2024
}

fn days_in(year: u32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400)) => {
            29
        }
        2 => 28,
        _ => 0,
    }
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
    let (wanted, from) = link_target(&req);
    let mint = Mint::of(&app, &req);
    // Signup is the one endpoint whose budget depends on its body: an
    // anonymous sign in spends a different one. So who this counts
    // against is read here, while the request is still whole, and spent
    // below once the body has said which of the two this is.
    let who = app.limits.who(req.headers(), crate::limit::peer(&req));
    let body = match read_json(req.into_body()).await {
        Ok(v) => v,
        Err(res) => return res,
    };
    let (email, phone, password) = (
        field(&body, "email"),
        field(&body, "phone"),
        field(&body, "password"),
    );
    // Upstream splits this off in the route rather than in the handler,
    // so it happens before anything else is judged: a body with neither
    // an address nor a number is an anonymous sign in whatever else it
    // carried, and the password on it is not read at all.
    if email.is_empty() && phone.is_empty() {
        if !app.cfg.anonymous_users {
            return error_body(
                StatusCode::UNPROCESSABLE_ENTITY,
                "anonymous_provider_disabled",
                "Anonymous sign-ins are disabled",
            );
        }
        // The budget is spent here, where upstream spends it: after
        // the ask has been established as an anonymous one and before
        // anything else is judged about it.
        if !app
            .limits
            .allow(crate::limit::Point::Anonymous, who.as_deref())
        {
            return crate::limit::refused();
        }
        // Upstream's order, which is worth keeping: a project with
        // anonymous sign in off says so even when signups are off too,
        // because the two settings are turned on in different places
        // and the one that is nearer the ask is the useful answer.
        if app.cfg.disable_signup {
            return error_body(
                StatusCode::UNPROCESSABLE_ENTITY,
                "signup_disabled",
                "Signups not allowed for this instance",
            );
        }
        let data = metadata(&body);
        return match sign_up_anonymously(pool, &data, &mint).await {
            Ok(issued) => json_body(StatusCode::OK, issued.json()),
            Err(e) => refusal(e, "anonymous signup"),
        };
    }
    // The other half of the split, on the otp budget rather than the
    // anonymous one, and spent before the handler upstream calls the
    // handler at all.
    if !app
        .limits
        .allow(crate::limit::Point::Signup, who.as_deref())
    {
        return crate::limit::refused();
    }
    // The first thing the signup handler asks upstream, before the
    // password is even looked at, so a project that is closed says it is
    // closed rather than grading the password of someone it is not going
    // to let in.
    if app.cfg.disable_signup {
        return error_body(
            StatusCode::UNPROCESSABLE_ENTITY,
            "signup_disabled",
            "Signups not allowed for this instance",
        );
    }
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
    let data = metadata(&body);
    if !phone.is_empty() {
        // The channel is judged before the provider is, which is
        // upstream's order: a request naming a channel nobody carries is
        // malformed whether or not phone signups are on at all.
        let post = posting(&app, &wanted, &from);
        let channel = match channel_of(&body, &post) {
            Ok(v) => v,
            Err(e) => return refusal(e, "signup"),
        };
        if !app.cfg.phone_enabled {
            return error_body(
                StatusCode::BAD_REQUEST,
                "phone_provider_disabled",
                "Phone signups are disabled",
            );
        }
        return match sign_up_by_phone(pool, phone, password, &channel, &data, &mint, &post).await {
            Ok(SignedUp::Session(issued)) => json_body(StatusCode::OK, issued.json()),
            Ok(SignedUp::Pending(user)) => json_body(StatusCode::OK, user),
            Ok(SignedUp::Taken) => already_registered(),
            Err(e) => refusal(e, "signup"),
        };
    }

    if !app.cfg.email_enabled {
        return error_body(
            StatusCode::BAD_REQUEST,
            "email_provider_disabled",
            "Email signups are disabled",
        );
    }
    match sign_up(
        pool,
        email,
        password,
        &data,
        &mint,
        &posting(&app, &wanted, &from),
    )
    .await
    {
        Ok(SignedUp::Session(issued)) => json_body(StatusCode::OK, issued.json()),
        Ok(SignedUp::Pending(user)) => json_body(StatusCode::OK, user),
        Ok(SignedUp::Taken) => already_registered(),
        Err(e) => refusal(e, "signup"),
    }
}

/// The answer to a signup on an address that is already confirmed, for a
/// project that confirms its own signups and so has no secret to keep.
/// It is written here rather than raised as a refusal from `register`,
/// because the repeated signup entry has to commit first.
fn already_registered() -> Response {
    error_body(
        StatusCode::UNPROCESSABLE_ENTITY,
        "user_already_exists",
        "User already registered",
    )
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
    let mint = Mint::of(&app, &req);
    let body = match read_json(req.into_body()).await {
        Ok(v) => v,
        Err(res) => return res,
    };
    let asked = match asked(&body, false) {
        Ok(v) => v,
        Err(e) => return refusal(e, "verify"),
    };
    match confirm(pool, &asked, &rules(&app), &mint).await {
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
    let mint = Mint::of(&app, &req);
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

    match confirm(pool, &asked, &rules(&app), &mint).await {
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
                Error::Denied { status, code, msg } | Error::Hook { status, code, msg } => {
                    (status, code, msg)
                }
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
pub(crate) fn landing(app: &App, wanted: &str, referrer: &str) -> String {
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
    to(
        StatusCode::SEE_OTHER,
        &format!("{target}#{}", encoded(pairs)),
    )
}

/// Go's url.Values.Encode: sorted by key, each side escaped.
pub(crate) fn encoded(pairs: &[(&str, String)]) -> String {
    let mut pairs: Vec<&(&str, String)> = pairs.iter().collect();
    pairs.sort_by_key(|(k, _)| *k);
    pairs
        .iter()
        .map(|(k, v)| format!("{}={}", query_escape(k), query_escape(v)))
        .collect::<Vec<_>>()
        .join("&")
}

/// A redirect, or a 500 when the target is not something that fits in
/// a header, which is the only way building one fails.
fn to(status: StatusCode, location: &str) -> Response {
    match axum::http::HeaderValue::from_str(location) {
        Ok(value) => {
            let mut res = Response::new(Body::empty());
            *res.status_mut() = status;
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
pub(crate) fn query_escape(s: &str) -> String {
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
pub(crate) fn query_object(query: &str) -> serde_json::Value {
    let mut out = serde_json::Map::new();
    for pair in query.split('&').filter(|p| !p.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        out.entry(crate::rest::decode(key))
            .or_insert_with(|| serde_json::Value::String(crate::rest::decode(value)));
    }
    serde_json::Value::Object(out)
}

/// GoTrue's GOTRUE_EXTERNAL_FLOW_STATE_EXPIRY_DURATION, which has a
/// floor of five minutes and a default of the same: how long there is
/// between being sent to a provider and coming back with something to
/// trade.
const FLOW_TTL: f64 = 300.0;

/// GET /auth/v1/authorize, where a social sign in starts.
///
/// The answer is a redirect to the provider carrying a state parameter,
/// and the state is the id of a row written here. Upstream used to sign
/// a JWT for this and now writes a row instead, which is what makes a
/// state single use: the row records that it has been spent, and a
/// signature cannot.
pub async fn authorize(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    req: Request<Body>,
) -> Response {
    let Some(pool) = &app.pool else {
        return no_database();
    };
    let query = query_object(req.uri().query().unwrap_or_default());
    let referrer = landing(&app, field(&query, "redirect_to"), &referer(&req));
    match sending_off(&app, pool, &query, &referrer, "").await {
        // 302 rather than the 303 a followed confirmation link gets,
        // because that is what upstream sends and what every provider's
        // registered redirect was tested against.
        Ok(url) => to(StatusCode::FOUND, &url),
        Err(e) => refusal(e, "authorize"),
    }
}

/// GET /auth/v1/user/identities/authorize, the same start with a
/// signed in person behind it. Upstream is one function taking an
/// optional user, and the only thing that user changes is the
/// linking_target_id on the row, so this is that function with the
/// target filled in.
///
/// The answer is a redirect like /authorize's, unless the caller asks
/// for the url instead: a client that is not navigating the top level
/// window cannot follow a redirect to a provider and needs the address
/// to open itself.
pub async fn link_identity(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    req: Request<Body>,
) -> Response {
    if let Some(off) = linking_off(&app) {
        return off;
    }
    let Some(pool) = &app.pool else {
        return no_database();
    };
    let caller = match caller(&req) {
        Ok(v) => v,
        Err(res) => return *res,
    };
    let query = query_object(req.uri().query().unwrap_or_default());
    let referrer = landing(&app, field(&query, "redirect_to"), &referer(&req));
    let url = match sending_off(&app, pool, &query, &referrer, &caller.user_id).await {
        Ok(url) => url,
        Err(e) => return refusal(e, "link identity"),
    };
    match field(&query, "skip_http_redirect") == "true" {
        true => json_body(StatusCode::OK, serde_json::json!({ "url": url })),
        false => to(StatusCode::FOUND, &url),
    }
}

/// Upstream's requireManualLinkingEnabled. A project that has not
/// turned linking on does not have these endpoints, which is a 404
/// rather than a 403 because the shape of the surface is the answer.
fn linking_off(app: &App) -> Option<Response> {
    match app.cfg.manual_linking {
        true => None,
        false => Some(error_body(
            StatusCode::NOT_FOUND,
            "manual_linking_disabled",
            "Manual linking is disabled",
        )),
    }
}

/// Write the flow down and work out where the provider should be asked
/// to send the person, which is all both /authorize endpoints do.
async fn sending_off(
    app: &App,
    pool: &Pool,
    query: &serde_json::Value,
    referrer: &str,
    target: &str,
) -> Result<String, Error> {
    let name = field(query, "provider");
    let Some(provider) = app.cfg.oauth.get(name) else {
        // Upstream prints the underlying error into this message, and
        // the two it can be say different things: one means the name is
        // not a provider at all, the other that it is one nobody
        // configured. A client debugging a typo needs to be told which.
        let why = match crate::oauth::Provider::named(&name.to_ascii_lowercase()) {
            Some(_) => "provider is not enabled".to_string(),
            None => format!("Provider {name} could not be found"),
        };
        return denied("validation_failed", &format!("Unsupported provider: {why}"));
    };
    let challenge = field(query, "code_challenge");
    let method = field(query, "code_challenge_method");
    validate_pkce(method, challenge)?;
    let state = new_flow(pool, &provider.name, challenge, method, referrer, target).await?;
    Ok(provider.authorize_url(&callback_url(app, provider), &state, field(query, "scopes")))
}

/// Where the provider sends the person back: whatever this provider was
/// registered with, or this server's own callback.
fn callback_url(app: &App, provider: &crate::oauth::Provider) -> String {
    match provider.redirect_uri.is_empty() {
        true => format!("{}/callback", app.issuer()),
        false => provider.redirect_uri.clone(),
    }
}

/// Upstream's validatePKCEParams. Both parameters or neither, and a
/// challenge that is the right length and the right alphabet, which is
/// RFC 7636 section 4.2.
fn validate_pkce(method: &str, challenge: &str) -> Result<(), Error> {
    if challenge.is_empty() != method.is_empty() {
        return denied(
            "validation_failed",
            "PKCE flow requires code_challenge_method and code_challenge",
        );
    }
    if challenge.is_empty() {
        return Ok(());
    }
    if challenge.len() < 43 || challenge.len() > 128 {
        return denied(
            "validation_failed",
            "code challenge has to be between 43 and 128 characters",
        );
    }
    if !challenge
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'~' | b'-'))
    {
        return denied(
            "validation_failed",
            "code challenge can only contain alphanumeric characters, hyphens, periods, underscores and tildes",
        );
    }
    match method.to_ascii_lowercase().as_str() {
        "s256" | "plain" => Ok(()),
        _ => denied("validation_failed", "Invalid code_challenge_method"),
    }
}

/// Write down what this flow is, and hand back the id that goes to the
/// provider as the state.
///
/// A flow with no challenge is the implicit flow, and it gets a row
/// too: the row is what says which provider a callback belongs to and
/// where its answer should land, neither of which can be taken from the
/// callback request itself without trusting it.
async fn new_flow(
    pool: &Pool,
    provider: &str,
    challenge: &str,
    method: &str,
    referrer: &str,
    target: &str,
) -> Result<String, Error> {
    let method = method.to_ascii_lowercase();
    let sess = pool.admin().await?;
    let found = sess
        .query(
            "insert into auth.flow_state
                 (id, auth_code, code_challenge, code_challenge_method,
                  provider_type, authentication_method, referrer,
                  linking_target_id, created_at, updated_at)
             select gen_random_uuid(),
                    case when $1::text = '' then null
                         else gen_random_uuid()::text end,
                    nullif($1::text, ''),
                    case when $1::text = '' then null
                         else $2::text::auth.code_challenge_method end,
                    $3::text, 'oauth', $4::text,
                    nullif($5::text, '')::uuid, now(), now()
             returning id::text",
            &[&challenge, &method, &provider, &referrer, &target],
        )
        .await;
    let rows = match found {
        Ok(rows) => rows,
        Err(e) => {
            let _ = sess.rollback().await;
            return Err(e.into());
        }
    };
    let state: String = rows[0].get(0);
    sess.commit().await?;
    Ok(state)
}

/// The flow a callback is resuming.
struct Flow {
    id: String,
    provider: String,
    referrer: String,
    /// The challenge and its method, when this is a PKCE flow. None is
    /// the implicit flow, which gets its session on the redirect.
    challenge: Option<(String, String)>,
    auth_code: String,
    /// The account this identity is being attached to, when the flow
    /// was started by somebody who is already signed in. Empty is an
    /// ordinary sign in, where the account is whatever the identity
    /// turns out to belong to.
    target: String,
}

/// GET /auth/v1/callback, where the provider sends the person back.
///
/// Everything that can go wrong here goes back to the app as a redirect
/// rather than as a status, because what is looking at this response is
/// a browser mid navigation with no way to read a json body.
pub async fn callback(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    req: Request<Body>,
) -> Response {
    let query = query_object(req.uri().query().unwrap_or_default());
    came_back(app, query, client_ip(&req)).await
}

/// POST /auth/v1/callback, which is the same thing arriving as a form.
///
/// Apple is asked for a name and an address, and asking for either of
/// those makes it answer with response_mode=form_post rather than with
/// a redirect, so the browser posts a form here instead of following a
/// location. The fields are the same fields.
pub async fn callback_form(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    req: Request<Body>,
) -> Response {
    let query = query_object(req.uri().query().unwrap_or_default());
    let ip = client_ip(&req);
    let bytes = match to_bytes(req.into_body(), MAX_BODY).await {
        Ok(bytes) => bytes,
        Err(_) => {
            return oauth_refusal(
                &app.site_url(),
                refused(
                    StatusCode::BAD_REQUEST,
                    "bad_oauth_callback",
                    "Could not read the request body",
                ),
            );
        }
    };
    let posted = query_object(&String::from_utf8_lossy(&bytes));
    // The body wins, because that is where the provider put them. The
    // query is still read so that a redirect uri carrying its own
    // parameters keeps working.
    let mut fields = query.as_object().cloned().unwrap_or_default();
    if let Some(posted) = posted.as_object() {
        for (key, value) in posted {
            fields.insert(key.clone(), value.clone());
        }
    }
    came_back(app, serde_json::Value::Object(fields), ip).await
}

/// The callback itself, once its parameters have been found wherever
/// the provider chose to put them.
async fn came_back(app: Arc<App>, query: serde_json::Value, ip: String) -> Response {
    let Some(pool) = &app.pool else {
        return no_database();
    };
    let site = app.site_url();

    // The flow is loaded first because everything after it, including
    // where a failure is sent, comes out of that row. A callback whose
    // state does not load has nowhere of its own to go, so it goes to
    // the site url.
    let flow = match load_flow(pool, field(&query, "state")).await {
        Ok(flow) => flow,
        Err(e) => return oauth_refusal(&site, e),
    };
    let target = match flow.referrer.is_empty() {
        true => site,
        false => flow.referrer.clone(),
    };

    // The provider refused, or the person changed their mind at the
    // consent screen. Its own words go back rather than ours, because
    // access_denied from Google means what Google says it means.
    let said_no = field(&query, "error");
    if !said_no.is_empty() {
        return oauth_redirect(&target, said_no, "", field(&query, "error_description"));
    }
    let code = field(&query, "code");
    if code.is_empty() {
        return oauth_refusal(
            &target,
            refused(
                StatusCode::BAD_REQUEST,
                "bad_oauth_callback",
                "OAuth callback with missing authorization code missing",
            ),
        );
    }
    let Some(provider) = app.cfg.oauth.get(&flow.provider) else {
        // The row names a provider that is no longer configured, which
        // is a project that changed its mind between the two halves of
        // one sign in.
        return oauth_refusal(
            &target,
            refused(
                StatusCode::BAD_REQUEST,
                "oauth_provider_not_supported",
                "Unsupported provider: provider is not enabled",
            ),
        );
    };

    let (mut person, tokens) = match ask_provider(&app, provider, code).await {
        Ok(pair) => pair,
        Err(e) => return oauth_refusal(&target, e),
    };
    // Apple sends the name in a form field, on the first sign in and
    // never again, so it is taken whenever it turns up.
    named(&mut person, field(&query, "user"));
    if person.email.is_empty() {
        return oauth_refusal(
            &target,
            refused(
                StatusCode::INTERNAL_SERVER_ERROR,
                "unexpected_failure",
                "Error getting user email from external provider",
            ),
        );
    }

    let post = posting(&app, &flow.referrer, "");
    match land(
        &app,
        pool,
        &flow,
        provider,
        &person,
        &tokens,
        &post,
        &Mint::at(&app, ip),
    )
    .await
    {
        Ok(response) => response,
        Err(e) => oauth_refusal(&target, e),
    }
}

/// The `user` field Apple posts alongside the code: the only time it
/// will ever say what somebody is called. Anything that is not the
/// shape it documents is ignored rather than refused, because a name
/// is not worth failing a sign in over.
fn named(person: &mut crate::oauth::Person, posted: &str) {
    if posted.is_empty() {
        return;
    }
    let Ok(user) = serde_json::from_str::<serde_json::Value>(posted) else {
        return;
    };
    let first = field(&user["name"], "firstName");
    let last = field(&user["name"], "lastName");
    let full = format!("{first} {last}");
    let full = full.trim();
    if full.is_empty() {
        return;
    }
    if let Some(claims) = person.claims.as_object_mut() {
        claims.insert("name".to_string(), full.into());
        claims.insert("full_name".to_string(), full.into());
    }
}

/// Trade the code for a token and read the profile it opens, both on a
/// blocking thread because the client underneath is a blocking one and
/// a provider on the far side of the internet is not quick.
async fn ask_provider(
    app: &App,
    provider: &crate::oauth::Provider,
    code: &str,
) -> Result<(crate::oauth::Person, crate::oauth::Tokens), Error> {
    let http = Arc::clone(&app.web);
    let redirect = callback_url(app, provider);
    let provider = provider.clone();
    let code = code.to_string();
    let out = tokio::task::spawn_blocking(move || {
        let tokens = crate::oauth::exchange(&provider, http.as_ref(), &code, &redirect)?;
        let person = crate::oauth::person(&provider, http.as_ref(), &tokens)?;
        Ok::<_, String>((person, tokens))
    })
    .await;
    match out {
        Ok(Ok(pair)) => Ok(pair),
        Ok(Err(e)) => {
            log::error!("oauth callback: {e}");
            Err(refused(
                StatusCode::INTERNAL_SERVER_ERROR,
                "unexpected_failure",
                &e,
            ))
        }
        Err(e) => {
            log::error!("oauth callback panicked: {e}");
            Err(refused(
                StatusCode::INTERNAL_SERVER_ERROR,
                "unexpected_failure",
                "Unexpected failure, please check server logs for more information",
            ))
        }
    }
}

/// Read the flow back, in upstream's order of complaints. A state that
/// is missing, malformed, unknown, expired or spent each says so
/// differently, because a client debugging one of these needs to be
/// told which it is.
async fn load_flow(pool: &Pool, state: &str) -> Result<Flow, Error> {
    if state.is_empty() {
        return Err(refused(
            StatusCode::BAD_REQUEST,
            "bad_oauth_callback",
            "OAuth state parameter missing",
        ));
    }
    if !is_uuid(state) {
        return Err(refused(
            StatusCode::BAD_REQUEST,
            "bad_oauth_state",
            "OAuth state parameter is invalid",
        ));
    }
    let sess = pool.admin().await?;
    let found = sess
        .query(
            "select id::text, provider_type, coalesce(referrer, ''),
                    coalesce(code_challenge, ''),
                    coalesce(code_challenge_method::text, ''),
                    coalesce(auth_code, ''),
                    created_at < now() - make_interval(secs => $2::double precision),
                    user_id is not null,
                    coalesce(linking_target_id::text, ''),
                    exists (select 1 from auth.users u
                             where u.id = f.linking_target_id
                               and u.deleted_at is null)
               from auth.flow_state f where id = $1::text::uuid",
            &[&state, &FLOW_TTL],
        )
        .await;
    let rows = match found {
        Ok(rows) => rows,
        Err(e) => {
            let _ = sess.rollback().await;
            return Err(e.into());
        }
    };
    let out = read_flow(rows.first());
    sess.commit().await?;
    out
}

fn read_flow(row: Option<&tokio_postgres::Row>) -> Result<Flow, Error> {
    let Some(row) = row else {
        return Err(refused(
            StatusCode::BAD_REQUEST,
            "bad_oauth_state",
            "OAuth state not found or expired",
        ));
    };
    if row.get::<_, bool>(6) {
        return Err(refused(
            StatusCode::BAD_REQUEST,
            "bad_oauth_state",
            "OAuth state has expired",
        ));
    }
    let challenge: String = row.get(3);
    let flow = Flow {
        id: row.get(0),
        provider: row.get(1),
        referrer: row.get(2),
        challenge: match challenge.is_empty() {
            true => None,
            false => Some((challenge, row.get(4))),
        },
        auth_code: row.get(5),
        target: row.get(8),
    };
    // A spent PKCE flow is one whose callback already ran, with the
    // code waiting to be traded. Running it again would issue a second
    // identity for the same consent, so it is refused rather than
    // replayed.
    if flow.challenge.is_some() && row.get::<_, bool>(7) {
        return Err(refused(
            StatusCode::BAD_REQUEST,
            "flow_state_already_used",
            "State has already been used",
        ));
    }
    // The account this was going to be attached to has been deleted
    // since the flow started, which is asked here rather than at the
    // end so that nothing is traded with a provider for an identity
    // that has nowhere to go.
    if !flow.target.is_empty() && !row.get::<_, bool>(9) {
        return Err(refused(
            StatusCode::UNPROCESSABLE_ENTITY,
            "user_not_found",
            "Linking target user not found",
        ));
    }
    Ok(flow)
}

/// Everything the callback does once the provider has been believed:
/// find or make the account, then either mark the flow for a client
/// that will come back with a verifier, or hand out a session on the
/// redirect itself.
#[allow(clippy::too_many_arguments)]
async fn land(
    app: &App,
    pool: &Pool,
    flow: &Flow,
    provider: &crate::oauth::Provider,
    person: &crate::oauth::Person,
    tokens: &crate::oauth::Tokens,
    post: &Post<'_>,
    mint: &Mint<'_>,
) -> Result<Response, Error> {
    let sess = pool.admin().await?;
    let out = settle(&sess, app, flow, provider, person, tokens, post, mint).await;
    match out {
        Ok(landed) => {
            // The unverified address branch commits and then refuses,
            // because what it did before refusing was send a
            // confirmation email, and a rollback would leave somebody
            // holding a link that matches nothing.
            sess.commit().await?;
            match landed {
                Landed::Answer(response) => Ok(*response),
                Landed::Unverified(code, provider) => Err(refused(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    code,
                    &format!(
                        "Unverified email with {provider}. A confirmation email has been sent to your {provider} email"
                    ),
                )),
            }
        }
        Err(e) => {
            let _ = sess.rollback().await;
            Err(e)
        }
    }
}

enum Landed {
    Answer(Box<Response>),
    /// The error code and the provider it names. Upstream calls this
    /// provider_email_needs_verification when the account is being made
    /// and email_not_confirmed when the identity is joining one that
    /// exists, and the sentence is the same both times.
    Unverified(&'static str, String),
}

#[allow(clippy::too_many_arguments)]
async fn settle(
    sess: &sql::Session,
    app: &App,
    flow: &Flow,
    provider: &crate::oauth::Provider,
    person: &crate::oauth::Person,
    tokens: &crate::oauth::Tokens,
    post: &Post<'_>,
    mint: &Mint<'_>,
) -> Result<Landed, Error> {
    // A flow that names a target is a manual link: the account is
    // already known and the question is only whether this identity may
    // join it. Everything after this point is the same either way.
    let attached = match flow.target.is_empty() {
        true => attach(sess, &provider.name, person, post, app.cfg.disable_signup).await?,
        false => link_to(sess, &flow.target, &provider.name, person, post).await?,
    };
    let user_id = match attached {
        Attached::User(id) => id,
        Attached::Unverified(code) => {
            return Ok(Landed::Unverified(code, provider.name.clone()));
        }
    };
    let target = match flow.referrer.is_empty() {
        true => app.site_url(),
        false => flow.referrer.clone(),
    };

    if flow.challenge.is_none() {
        // Implicit: the session rides back in the fragment, and the
        // flow row has done its job.
        let issued = start(sess, &user_id, "oauth", mint).await?;
        sess.execute(
            "delete from auth.flow_state where id = $1::text::uuid",
            &[&flow.id],
        )
        .await?;
        let mut fragment = vec![
            ("access_token", issued.access_token.clone()),
            ("expires_at", issued.expires_at.to_string()),
            ("expires_in", issued.expires_in.to_string()),
            ("refresh_token", issued.refresh_token.clone()),
            ("provider_token", tokens.access_token.clone()),
            ("sb", String::new()),
            ("token_type", "bearer".to_string()),
        ];
        // Not every provider hands one out, and a client cannot tell an
        // absent refresh token from an empty one if it is always there.
        if !tokens.refresh_token.is_empty() {
            fragment.push(("provider_refresh_token", tokens.refresh_token.clone()));
        }
        let location = format!("{target}#{}", encoded(&fragment));
        return Ok(Landed::Answer(Box::new(to(StatusCode::FOUND, &location))));
    }

    // PKCE: the flow keeps the provider's tokens until a client comes
    // back with the verifier, and the redirect carries only the code.
    let claimed = sess
        .execute(
            "update auth.flow_state
                set user_id = $2::text::uuid,
                    provider_access_token = $3,
                    provider_refresh_token = $4,
                    auth_code_issued_at = now(),
                    updated_at = now()
              where id = $1::text::uuid and user_id is null",
            &[
                &flow.id,
                &user_id,
                &tokens.access_token,
                &tokens.refresh_token,
            ],
        )
        .await?;
    if claimed == 0 {
        // Two callbacks for one state raced, and this is the loser.
        return Err(refused(
            StatusCode::BAD_REQUEST,
            "flow_state_already_used",
            "State has already been used",
        ));
    }
    Ok(Landed::Answer(Box::new(to(
        StatusCode::FOUND,
        &with_code(&target, &flow.auth_code),
    ))))
}

/// The PKCE redirect: the code goes in the query string rather than the
/// fragment, because a client has to send it to a server and a fragment
/// never leaves the browser.
fn with_code(target: &str, code: &str) -> String {
    let (base, fragment) = target.split_once('#').unwrap_or((target, ""));
    let separator = match base.contains('?') {
        true => "&",
        false => "?",
    };
    let mut out = format!("{base}{separator}code={}", query_escape(code));
    if !fragment.is_empty() {
        out.push('#');
        out.push_str(fragment);
    }
    out
}

/// What the account came out as.
enum Attached {
    User(String),
    /// The provider will not say the address is verified and this
    /// project will not take its word for it, so a confirmation has
    /// gone out instead and there is no session. The string is the
    /// error code that says which of the two paths asked for it.
    Unverified(&'static str),
}

/// Which account an external identity belongs to, upstream's
/// DetermineAccountLinking. Every provider here is in the one linking
/// domain upstream calls default, which is what makes this readable: an
/// identity that already exists wins, then a verified address that
/// matches something, then a new account.
enum Whose {
    /// This identity has signed in before.
    Known(String),
    /// A different identity, or a plain account, already holds this
    /// verified address.
    Link(String),
    /// Nobody, so an account is made.
    Fresh,
    /// More than one account holds it, which the schema is supposed to
    /// prevent and which is never resolved by guessing.
    Several,
}

async fn decide(
    sess: &sql::Session,
    provider: &str,
    person: &crate::oauth::Person,
    autoconfirm: bool,
) -> Result<Whose, Error> {
    let rows = sess
        .query(
            "select user_id::text from auth.identities
              where provider_id = $1 and provider = $2",
            &[&person.sub, &provider],
        )
        .await?;
    if let Some(row) = rows.first() {
        return Ok(Whose::Known(row.get(0)));
    }
    // An address the provider will not vouch for links to nothing. This
    // is the whole of the pre-account-takeover rule: sign up at a
    // provider with somebody else's address, and without this you are
    // handed their account.
    if !person.email_verified && !autoconfirm {
        return Ok(Whose::Fresh);
    }
    let email = person.email.to_ascii_lowercase();
    let rows = sess
        .query(
            "select distinct user_id::text from auth.identities where email = $1",
            &[&email],
        )
        .await?;
    if rows.len() > 1 {
        return Ok(Whose::Several);
    }
    if let Some(row) = rows.first() {
        return Ok(Whose::Link(row.get(0)));
    }
    // No identity carries it, but an account might: a project whose
    // users signed up before identities were backfilled, and the
    // account an invite creates before it is accepted.
    let rows = sess
        .query(
            "select id::text from auth.users
              where email = $1 and is_sso_user = false and deleted_at is null",
            &[&email],
        )
        .await?;
    match rows.len() {
        0 => Ok(Whose::Fresh),
        1 => Ok(Whose::Link(rows[0].get(0))),
        _ => Ok(Whose::Several),
    }
}

/// Find or make the account this identity belongs to, and leave it in
/// the state a session can be issued from.
async fn attach(
    sess: &sql::Session,
    provider: &str,
    person: &crate::oauth::Person,
    post: &Post<'_>,
    closed: bool,
) -> Result<Attached, Error> {
    let whose = decide(sess, provider, person, post.autoconfirm).await?;
    let email = person.email.to_ascii_lowercase();
    let user_id = match whose {
        Whose::Several => {
            // The schema has a partial unique index that is supposed to
            // make this impossible, so it is a state to report rather
            // than one to pick a way out of.
            log::error!("two accounts hold {email} in the same linking domain");
            return Err(refused(
                StatusCode::INTERNAL_SERVER_ERROR,
                "unexpected_failure",
                "Multiple accounts with the same email address in the same linking domain detected: default",
            ));
        }
        Whose::Known(user_id) => {
            sess.execute(
                "update auth.identities
                    set identity_data = $3::jsonb,
                        last_sign_in_at = now(), updated_at = now()
                  where provider_id = $1 and provider = $2",
                &[&person.sub, &provider, &person.claims],
            )
            .await?;
            user_id
        }
        Whose::Link(user_id) => {
            new_identity(sess, &user_id, provider, person).await?;
            user_id
        }
        Whose::Fresh => {
            // The only branch that makes an account, so the only one a
            // closed project turns away. Signing in through a provider
            // as somebody the project already knows keeps working, which
            // is the point of closing it rather than turning the
            // provider off.
            if closed {
                return Err(refused(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "signup_disabled",
                    "Signups not allowed for this instance",
                ));
            }
            // A verified address that already belongs to somebody, on a
            // provider that would not link, leaves the new account with
            // no address at all rather than with a claim on theirs.
            let taken: bool = sess
                .query(
                    "select exists (
                         select 1 from auth.users
                          where email = $1 and is_sso_user = false and deleted_at is null)",
                    &[&email],
                )
                .await?[0]
                .get(0);
            let address = match taken {
                true => String::new(),
                false => email.clone(),
            };
            let rows = sess
                .query(
                    "insert into auth.users
                         (instance_id, id, aud, role, email,
                          raw_app_meta_data, raw_user_meta_data,
                          confirmation_token, recovery_token,
                          email_change_token_new, email_change,
                          created_at, updated_at, is_anonymous, is_sso_user)
                     values ('00000000-0000-0000-0000-000000000000', gen_random_uuid(),
                             $2, 'authenticated', nullif($1::text, ''),
                             jsonb_build_object('provider', $3::text,
                                                'providers', jsonb_build_array($3::text)),
                             $4::jsonb, '', '', '', '', now(), now(), false, false)
                     returning id::text",
                    &[&address, &AUD, &provider, &person.claims],
                )
                .await?;
            let user_id: String = rows[0].get(0);
            new_identity(sess, &user_id, provider, person).await?;
            user_id
        }
    };

    let state = sess
        .query(
            "select coalesce(email, ''), email_confirmed_at is not null,
                    banned_until is not null and banned_until > now()
               from auth.users where id = $1::text::uuid",
            &[&user_id],
        )
        .await?;
    let (address, confirmed, banned): (String, bool, bool) =
        (state[0].get(0), state[0].get(1), state[0].get(2));
    if banned {
        return Err(refused(
            StatusCode::FORBIDDEN,
            "user_banned",
            "User is banned",
        ));
    }

    if address.is_empty() || confirmed {
        // Nothing here is a signup even when the account was made a few
        // statements ago, because an account whose address was already
        // proved elsewhere is being signed in to rather than started.
        audit::record(
            sess,
            Actor::Account(&user_id),
            Action::Login,
            "",
            Some(serde_json::json!({ "provider": provider })),
        )
        .await?;
        merge_claims(sess, &user_id, &person.claims).await?;
        providers_of(sess, &user_id).await?;
        return Ok(Attached::User(user_id));
    }

    // The account is unconfirmed, so nothing on it has been proved and
    // everything else attached to it is dropped: an unconfirmed signup
    // somebody else started on this address must not survive as a way
    // back in once the address is confirmed here.
    sess.execute(
        "update auth.users set encrypted_password = null where id = $1::text::uuid",
        &[&user_id],
    )
    .await?;
    sess.execute(
        "update auth.users
            set raw_user_meta_data = $2::jsonb, updated_at = now()
          where id = $1::text::uuid",
        &[&user_id, &person.claims],
    )
    .await?;
    sess.execute(
        "delete from auth.identities
          where user_id = $1::text::uuid
            and not (provider_id = $2 and provider = $3)",
        &[&user_id, &person.sub, &provider],
    )
    .await?;
    providers_of(sess, &user_id).await?;

    if person.email_verified || post.autoconfirm {
        audit::record(
            sess,
            Actor::Account(&user_id),
            Action::UserSignedUp,
            "",
            Some(serde_json::json!({ "provider": provider })),
        )
        .await?;
        sess.execute(
            "update auth.users
                set email_confirmed_at = now(), confirmation_token = '',
                    updated_at = now()
              where id = $1::text::uuid and email_confirmed_at is null",
            &[&user_id],
        )
        .await?;
        return Ok(Attached::User(user_id));
    }

    // The remaining branch writes nothing. It sends a confirmation and
    // refuses, and upstream records neither the send nor the refusal,
    // which leaves the one social sign in that fails silent in the
    // trail.

    // The provider will not vouch for the address, so it is proved the
    // same way a password signup proves one, and there is no session
    // until it is.
    within_limit(
        sess,
        &user_id,
        "confirmation_sent_at",
        post.settings.max_frequency,
        TOO_SOON_MAIL,
    )
    .await?;
    let code = mint_code(sess, &user_id, &address, "confirmation_token").await?;
    send_code(
        sess,
        post,
        &user_id,
        Outgoing {
            template: crate::mail::CONFIRMATION,
            kind: "signup",
            to: &address,
            code: &code,
            new_email: "",
        },
    )
    .await?;
    Ok(Attached::Unverified("provider_email_needs_verification"))
}

/// Attach this identity to an account that already exists and whose
/// owner asked for it, upstream's linkIdentityToUser.
///
/// None of the linking rules apply here and that is the point: the
/// person is signed in to the target account, so what the provider
/// says about an address is not what decides where the identity goes.
/// What is still asked is whether this identity is spoken for.
async fn link_to(
    sess: &sql::Session,
    target: &str,
    provider: &str,
    person: &crate::oauth::Person,
    post: &Post<'_>,
) -> Result<Attached, Error> {
    let rows = sess
        .query(
            "select user_id::text from auth.identities
              where provider_id = $1 and provider = $2",
            &[&person.sub, &provider],
        )
        .await?;
    if let Some(row) = rows.first() {
        let owner: String = row.get(0);
        return Err(refused(
            StatusCode::UNPROCESSABLE_ENTITY,
            "identity_already_exists",
            match owner == target {
                true => "Identity is already linked",
                false => "Identity is already linked to another user",
            },
        ));
    }
    new_identity(sess, target, provider, person).await?;

    // An account that holds an address keeps it. The provider has
    // vouched for nothing that would justify moving somebody's account
    // to a different address behind a link button.
    let address: String = sess
        .query(
            "select coalesce(email, '') from auth.users where id = $1::text::uuid",
            &[&target],
        )
        .await?[0]
        .get(0);
    if address.is_empty() {
        let Some(address) = email_from_identities(sess, target).await? else {
            return Err(refused(
                StatusCode::BAD_REQUEST,
                "email_exists",
                "A user with this email address has already been registered",
            ));
        };
        if !person.email_verified {
            // Upstream does not consult autoconfirm here the way the
            // signup path does, and this follows it: the account
            // exists and is being given an address, so the address is
            // proved rather than assumed.
            within_limit(
                sess,
                target,
                "confirmation_sent_at",
                post.settings.max_frequency,
                TOO_SOON_MAIL,
            )
            .await?;
            let code = mint_code(sess, target, &address, "confirmation_token").await?;
            send_code(
                sess,
                post,
                target,
                Outgoing {
                    template: crate::mail::CONFIRMATION,
                    kind: "signup",
                    to: &address,
                    code: &code,
                    new_email: "",
                },
            )
            .await?;
            providers_of(sess, target).await?;
            return Ok(Attached::Unverified("email_not_confirmed"));
        }
        sess.execute(
            "update auth.users
                set email_confirmed_at = coalesce(email_confirmed_at, now()),
                    confirmation_token = '', is_anonymous = false,
                    updated_at = now()
              where id = $1::text::uuid",
            &[&target],
        )
        .await?;
    }
    providers_of(sess, target).await?;
    Ok(Attached::User(target.to_string()))
}

/// Upstream's UpdateUserEmailFromIdentities: an account with no
/// address of its own takes one from an identity, and an account whose
/// address one of its identities already carries keeps it.
///
/// None is the conflict: every address on offer belongs to somebody
/// else. That is not a state to pick a way out of, because either
/// answer would hand one person's address to another.
async fn email_from_identities(
    sess: &sql::Session,
    user_id: &str,
) -> Result<Option<String>, sql::Error> {
    let held: bool = sess
        .query(
            "select exists (
                 select 1 from auth.identities i, auth.users u
                  where i.user_id = $1::text::uuid and u.id = $1::text::uuid
                    and coalesce(i.email, '') = coalesce(u.email, ''))",
            &[&user_id],
        )
        .await?[0]
        .get(0);
    if held {
        let address: String = sess
            .query(
                "select coalesce(email, '') from auth.users where id = $1::text::uuid",
                &[&user_id],
            )
            .await?[0]
            .get(0);
        return Ok(Some(address));
    }
    // The oldest identity whose address nobody else holds. Ordering by
    // when the identity arrived is what makes this answer the same
    // twice rather than whatever the planner felt like.
    let rows = sess
        .query(
            "select coalesce(i.email, '') from auth.identities i
              where i.user_id = $1::text::uuid
                and not exists (
                    select 1 from auth.users u
                     where u.email = i.email and u.id <> i.user_id
                       and u.is_sso_user = false and u.deleted_at is null)
              order by i.created_at, i.id
              limit 1",
            &[&user_id],
        )
        .await?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    let address: String = row.get(0);
    // An identity with no address of its own leaves the account with
    // none, and an account with no address has nothing confirmed.
    sess.execute(
        "update auth.users
            set email = nullif($2::text, ''),
                email_confirmed_at = case when $2::text = ''
                                          then null else email_confirmed_at end,
                updated_at = now()
          where id = $1::text::uuid",
        &[&user_id, &address],
    )
    .await?;
    Ok(Some(address))
}

/// DELETE /auth/v1/user/identities/{identity_id}, upstream's
/// DeleteIdentity. The last identity cannot go, because an account
/// with none is one nobody can sign in to again.
pub async fn unlink_identity(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    axum::extract::Path(identity_id): axum::extract::Path<String>,
    req: Request<Body>,
) -> Response {
    if let Some(off) = linking_off(&app) {
        return off;
    }
    let Some(pool) = &app.pool else {
        return no_database();
    };
    let caller = match caller(&req) {
        Ok(v) => v,
        Err(res) => return *res,
    };
    let sess = match pool.admin().await {
        Ok(v) => v,
        Err(e) => return refusal(Error::Db(e), "unlink identity"),
    };
    let out = unlink(&sess, &caller.user_id, &identity_id).await;
    match out {
        Ok(()) => match sess.commit().await {
            Ok(()) => json_body(StatusCode::OK, serde_json::json!({})),
            Err(e) => refusal(Error::Db(e), "unlink identity"),
        },
        Err(e) => {
            let _ = sess.rollback().await;
            refusal(e, "unlink identity")
        }
    }
}

async fn unlink(sess: &sql::Session, user_id: &str, identity_id: &str) -> Result<(), Error> {
    // A 404 for a malformed id rather than a 400, which reads oddly
    // and is what upstream answers.
    if !is_uuid(identity_id) {
        return Err(refused(
            StatusCode::NOT_FOUND,
            "validation_failed",
            "identity_id must be an UUID",
        ));
    }
    let rows = sess
        .query(
            "select id::text, provider, provider_id from auth.identities
              where user_id = $1::text::uuid for update",
            &[&user_id],
        )
        .await?;
    if rows.len() <= 1 {
        return Err(refused(
            StatusCode::UNPROCESSABLE_ENTITY,
            "single_identity_not_deletable",
            "User must have at least 1 identity after unlinking",
        ));
    }
    let Some(row) = rows
        .iter()
        .find(|row| row.get::<_, String>(0) == identity_id)
    else {
        return Err(refused(
            StatusCode::UNPROCESSABLE_ENTITY,
            "identity_not_found",
            "Identity doesn't exist",
        ));
    };
    let provider: String = row.get(1);
    let provider_id: String = row.get(2);
    // Before the delete, because the traits name the identity that is
    // about to stop existing.
    audit::record(
        sess,
        Actor::Account(user_id),
        Action::IdentityUnlinked,
        "",
        Some(serde_json::json!({
            "identity_id": identity_id,
            "provider": provider,
            "provider_id": provider_id,
        })),
    )
    .await?;
    sess.execute(
        "delete from auth.identities where id = $1::text::uuid",
        &[&identity_id],
    )
    .await?;
    match provider.as_str() {
        // The phone identity is the number, so unlinking it is what
        // gives the number up.
        "phone" => {
            sess.execute(
                "update auth.users
                    set phone = null, phone_confirmed_at = null, updated_at = now()
                  where id = $1::text::uuid",
                &[&user_id],
            )
            .await?;
        }
        _ => {
            if email_from_identities(sess, user_id).await?.is_none() {
                return Err(refused(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "email_conflict_identity_not_deletable",
                    "Unable to unlink identity due to email conflict",
                ));
            }
        }
    }
    providers_of(sess, user_id).await?;
    Ok(())
}

async fn new_identity(
    sess: &sql::Session,
    user_id: &str,
    provider: &str,
    person: &crate::oauth::Person,
) -> Result<(), sql::Error> {
    sess.execute(
        "insert into auth.identities
             (provider_id, user_id, identity_data, provider,
              last_sign_in_at, created_at, updated_at)
         values ($1, $2::text::uuid, $3::jsonb, $4, now(), now(), now())",
        &[&person.sub, &user_id, &person.claims, &provider],
    )
    .await?;
    Ok(())
}

/// What the provider said, folded into the user metadata, which is
/// where a client reads a name and an avatar from.
async fn merge_claims(
    sess: &sql::Session,
    user_id: &str,
    claims: &serde_json::Value,
) -> Result<(), sql::Error> {
    sess.execute(
        "update auth.users
            set raw_user_meta_data = coalesce(raw_user_meta_data, '{}'::jsonb) || $2::jsonb,
                updated_at = now()
          where id = $1::text::uuid",
        &[&user_id, &claims],
    )
    .await?;
    Ok(())
}

/// Rewrite app_metadata.providers from the identities that exist, which
/// is upstream's UpdateAppMetaDataProviders. It is derived rather than
/// appended to, so an identity that was just dropped stops being
/// advertised.
async fn providers_of(sess: &sql::Session, user_id: &str) -> Result<(), sql::Error> {
    sess.execute(
        "update auth.users u
            set raw_app_meta_data = coalesce(u.raw_app_meta_data, '{}'::jsonb)
                || jsonb_build_object('providers', p.list)
                || case when jsonb_array_length(p.list) > 0
                        then jsonb_build_object('provider', p.list->>0)
                        else '{}'::jsonb end,
                updated_at = now()
           from (select coalesce(jsonb_agg(i.provider order by i.created_at), '[]'::jsonb) as list
                   from auth.identities i where i.user_id = $1::text::uuid) p
          where u.id = $1::text::uuid",
        &[&user_id],
    )
    .await?;
    Ok(())
}

/// A callback that failed, sent back to the app. The parameters go in
/// the query string and again in the fragment, because upstream writes
/// both and there are clients reading each.
fn oauth_redirect(target: &str, error: &str, code: &str, description: &str) -> Response {
    let mut pairs = vec![
        ("error", error.to_string()),
        ("error_description", description.to_string()),
    ];
    if !code.is_empty() {
        pairs.push(("error_code", code.to_string()));
    }
    let query = encoded(&pairs);
    pairs.push(("sb", String::new()));
    let fragment = encoded(&pairs);
    let (base, _) = target.split_once('#').unwrap_or((target, ""));
    let separator = match base.contains('?') {
        true => "&",
        false => "?",
    };
    to(
        StatusCode::FOUND,
        &format!("{base}{separator}{query}#{fragment}"),
    )
}

fn oauth_refusal(target: &str, e: Error) -> Response {
    let (status, code, msg) = match e {
        Error::Denied { status, code, msg } | Error::Hook { status, code, msg } => {
            (status, code, msg)
        }
        Error::NotYet(surface) => return not_yet(surface),
        Error::Weak(_) => unreachable!("a callback never judges a password"),
        Error::Db(e) => {
            log::error!("oauth callback: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "unexpected_failure",
                "Unexpected failure, please check server logs for more information".to_string(),
            )
        }
    };
    // Upstream names three refusals access_denied whatever their
    // status, because to the app they are the same thing: the person
    // did not get in.
    let named = match code {
        "signup_disabled" | "user_banned" | "provider_email_needs_verification" => "access_denied",
        _ => oauth_error(status),
    };
    oauth_redirect(target, named, code, &msg)
}

/// The pkce grant: a client that started a flow trades the code it was
/// redirected with, plus the verifier it never sent anywhere, for a
/// session.
pub async fn pkce_grant(
    pool: &Pool,
    auth_code: &str,
    verifier: &str,
    mint: &Mint<'_>,
) -> Result<(Issued, String, String), Error> {
    if auth_code.is_empty() || verifier.is_empty() {
        return denied(
            "validation_failed",
            "invalid request: both auth code and code verifier should be non-empty",
        );
    }
    let sess = pool.admin().await?;
    let out = redeem(&sess, auth_code, verifier, mint).await;
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

async fn redeem(
    sess: &sql::Session,
    auth_code: &str,
    verifier: &str,
    mint: &Mint<'_>,
) -> Result<(Issued, String, String), Error> {
    // The row is locked for the length of the trade, so two clients
    // sending the same code cannot both walk away with a session.
    let rows = sess
        .query(
            "select id::text, user_id::text,
                    coalesce(code_challenge, ''),
                    coalesce(code_challenge_method::text, ''),
                    coalesce(provider_access_token, ''),
                    coalesce(provider_refresh_token, ''),
                    created_at < now() - make_interval(secs => $2::double precision),
                    authentication_method,
                    provider_type
               from auth.flow_state
              where auth_code = $1 and user_id is not null
              for update",
            &[&auth_code, &FLOW_TTL],
        )
        .await?;
    let Some(row) = rows.first() else {
        // A code that was never issued and a flow whose callback has
        // not run yet get the same answer, because telling them apart
        // would say whether a code exists to somebody who does not hold
        // the verifier.
        return Err(refused(
            StatusCode::NOT_FOUND,
            "flow_state_not_found",
            "invalid flow state, no valid flow state found",
        ));
    };
    if row.get::<_, bool>(6) {
        return Err(refused(
            StatusCode::UNPROCESSABLE_ENTITY,
            "flow_state_expired",
            "invalid flow state, flow state has expired",
        ));
    }
    let (id, user_id): (String, String) = (row.get(0), row.get(1));
    let (challenge, method): (String, String) = (row.get(2), row.get(3));
    let (access, refresh): (String, String) = (row.get(4), row.get(5));
    let proved: String = row.get(7);
    let provider_type: String = row.get(8);
    verify_pkce(&challenge, &method, verifier)?;

    // The one login entry whose trait is named provider_type rather than
    // provider, and whose value is the flow's provider rather than the
    // method that proved anything. Both are upstream's.
    audit::record(
        sess,
        Actor::Account(&user_id),
        Action::Login,
        "",
        Some(serde_json::json!({ "provider_type": provider_type })),
    )
    .await?;
    let issued = start(sess, &user_id, &proved, mint).await?;
    sess.execute(
        "delete from auth.flow_state where id = $1::text::uuid",
        &[&id],
    )
    .await?;
    Ok((issued, access, refresh))
}

/// RFC 7636 section 4.6. s256 is the only method worth using and plain
/// is the only one a client with no hash to hand can manage, so both
/// are here, and neither is compared with an early return.
fn verify_pkce(challenge: &str, method: &str, verifier: &str) -> Result<(), Error> {
    let expected = match method.to_ascii_lowercase().as_str() {
        "s256" => {
            use base64ct::Encoding;
            use sha2::Digest;
            base64ct::Base64UrlUnpadded::encode_string(&sha2::Sha256::digest(verifier.as_bytes()))
        }
        "plain" => verifier.to_string(),
        _ => {
            return Err(refused(
                StatusCode::BAD_REQUEST,
                "bad_code_verifier",
                "code challenge method not supported",
            ));
        }
    };
    match same_bytes(challenge.as_bytes(), expected.as_bytes()) {
        true => Ok(()),
        false => Err(refused(
            StatusCode::BAD_REQUEST,
            "bad_code_verifier",
            "code challenge does not match previously saved code verifier",
        )),
    }
}

/// A comparison that takes the same time whatever the answer, so the
/// number of leading bytes that matched is not something a caller can
/// measure its way to.
fn same_bytes(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    diff == 0
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
    // Upstream guards this one in the route rather than in the handler,
    // so the refusal comes before the body is read at all and a request
    // with nothing readable in it still hears why it was turned away.
    if !app.cfg.email_enabled {
        return error_body(
            StatusCode::BAD_REQUEST,
            "email_provider_disabled",
            "Email logins are disabled",
        );
    }
    let (wanted, from) = link_target(&req);
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
    match send_recovery(pool, email, &posting(&app, &wanted, &from)).await {
        Ok(()) => json_body(StatusCode::OK, serde_json::json!({})),
        Err(e) => refusal(e, "recover"),
    }
}

/// POST /auth/v1/magiclink, a link that signs someone in without a
/// password, and signs them up first if nobody holds the address.
pub async fn magiclink(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    req: Request<Body>,
) -> Response {
    let Some(pool) = &app.pool else {
        return no_database();
    };
    // The magic link handler asks this first thing upstream, before the
    // body, and at a different status from the one recover uses for the
    // same sentence. Both are kept, because a client branches on both.
    if !app.cfg.email_enabled {
        return error_body(
            StatusCode::UNPROCESSABLE_ENTITY,
            "email_provider_disabled",
            "Email logins are disabled",
        );
    }
    let (wanted, from) = link_target(&req);
    let mint = Mint::of(&app, &req);
    let body = match read_json(req.into_body()).await {
        Ok(v) => v,
        Err(res) => return res,
    };
    magic(pool, &body, &posting(&app, &wanted, &from), &mint).await
}

/// POST /auth/v1/otp, which is the magic link endpoint with a phone
/// branch and one extra rule: `create_user: false` turns it into a sign
/// in for people who already have an account.
pub async fn otp(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    req: Request<Body>,
) -> Response {
    let Some(pool) = &app.pool else {
        return no_database();
    };
    let (wanted, from) = link_target(&req);
    let mint = Mint::of(&app, &req);
    let body = match read_json(req.into_body()).await {
        Ok(v) => v,
        Err(res) => return res,
    };
    let (email, phone) = (field(&body, "email"), field(&body, "phone"));
    if !email.is_empty() && !phone.is_empty() {
        return error_body(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            "Only an email address or phone number should be provided",
        );
    }
    if !email.is_empty() && !field(&body, "channel").is_empty() {
        return error_body(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            "Channel should only be specified with Phone OTP",
        );
    }
    // Upstream asks this before it looks at which of the two was sent,
    // so an address that is refused here is refused whatever else the
    // request said.
    let create = body
        .get("create_user")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    if !create && !(email.is_empty() && phone.is_empty()) {
        let (column, held) = if email.is_empty() {
            match validate_phone(phone) {
                Ok(v) => ("phone", v),
                Err(e) => return refusal(e, "otp"),
            }
        } else {
            match validate_email(email) {
                Ok(v) => ("email", v),
                Err(e) => return refusal(e, "otp"),
            }
        };
        match is_registered(pool, column, &held).await {
            Ok(false) => {
                return error_body(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "otp_disabled",
                    "Signups not allowed for otp",
                );
            }
            Ok(true) => {}
            Err(e) => return refusal(e, "otp"),
        }
    }
    if !phone.is_empty() {
        return sms_otp(&app, pool, &body, &posting(&app, &wanted, &from), &mint).await;
    }
    if email.is_empty() {
        return error_body(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            "One of email or phone must be set",
        );
    }
    // Upstream reaches the magic link handler from here, so the address
    // half of this endpoint refuses in that handler's words and at its
    // status, which is the last thing either of them asks rather than
    // the first.
    if !app.cfg.email_enabled {
        return error_body(
            StatusCode::UNPROCESSABLE_ENTITY,
            "email_provider_disabled",
            "Email logins are disabled",
        );
    }
    magic(pool, &body, &posting(&app, &wanted, &from), &mint).await
}

/// The body both of them share. The answer is empty and the same
/// whether the address was known, which is what keeps either endpoint
/// from being asked who has an account here.
async fn magic(
    pool: &Pool,
    body: &serde_json::Value,
    post: &Post<'_>,
    mint: &Mint<'_>,
) -> Response {
    let email = field(body, "email");
    if email.is_empty() {
        // Upstream's wording, copied from recover, and its status,
        // which is not recover's. Both are kept because a client
        // branches on both.
        return error_body(
            StatusCode::UNPROCESSABLE_ENTITY,
            "validation_failed",
            "Password recovery requires an email",
        );
    }
    let data = metadata(body);
    match send_magic_link(pool, email, &data, mint, post).await {
        Ok(()) => json_body(StatusCode::OK, serde_json::json!({})),
        Err(e) => refusal(e, "magic link"),
    }
}

/// The phone half of the otp endpoint, GoTrue's SmsOtp.
///
/// A number nobody has signed up with is signed up here, with a password
/// nobody will ever hold, which is how a project with no passwords at
/// all registers people. Whether that signup sends the code or this does
/// depends on autoconfirm, and so does whether the answer carries the
/// provider's message id.
async fn sms_otp(
    app: &App,
    pool: &Pool,
    body: &serde_json::Value,
    post: &Post<'_>,
    mint: &Mint<'_>,
) -> Response {
    if !app.cfg.phone_enabled {
        return error_body(
            StatusCode::BAD_REQUEST,
            "phone_provider_disabled",
            "Unsupported phone provider",
        );
    }
    let phone = match validate_phone(field(body, "phone")) {
        Ok(v) => v,
        Err(e) => return refusal(e, "otp"),
    };
    let channel = match channel_of(body, post) {
        Ok(v) => v,
        Err(e) => return refusal(e, "otp"),
    };
    let data = metadata(body);

    // A number that has never answered is not being signed in, it is
    // being signed up, and an account halfway through a signup is in
    // the same position as one that does not exist yet.
    let confirmed = match phone_confirmed(pool, &phone).await {
        Ok(v) => v,
        Err(e) => return refusal(e, "otp"),
    };
    if !confirmed {
        let signed =
            sign_up_by_phone(pool, &phone, &unguessable(64), &channel, &data, mint, post).await;
        if let Err(e) = signed {
            return refusal(e, "otp");
        }
        // Without autoconfirm the signup itself has just texted the
        // code, so there is nothing left to send and nothing to say.
        if !post.sms.autoconfirm {
            return json_body(StatusCode::OK, serde_json::json!({}));
        }
        // With it the account is confirmed and holds no code at all, so
        // the code this endpoint was asked for still has to go out.
    }
    match texted(pool, &phone, &channel, post).await {
        Ok(id) if id.is_empty() => json_body(StatusCode::OK, serde_json::json!({})),
        Ok(id) => json_body(StatusCode::OK, serde_json::json!({"message_id": id})),
        Err(e) => refusal(e, "otp"),
    }
}

/// Whether this number has ever answered a code.
async fn phone_confirmed(pool: &Pool, phone: &str) -> Result<bool, Error> {
    let sess = pool.admin().await?;
    let rows = sess
        .query(
            "select phone_confirmed_at is not null from auth.users
              where phone = $1 and aud = $2 and deleted_at is null limit 1",
            &[&phone, &AUD],
        )
        .await;
    let confirmed = match rows {
        Ok(rows) => rows.first().is_some_and(|r| r.get::<_, bool>(0)),
        Err(e) => {
            let _ = sess.rollback().await;
            return Err(Error::Db(e));
        }
    };
    sess.commit().await?;
    Ok(confirmed)
}

/// Text a fresh sign in code to a number that already answered once.
async fn texted(pool: &Pool, phone: &str, channel: &str, post: &Post<'_>) -> Result<String, Error> {
    let sess = pool.admin().await?;
    let out = async {
        let rows = sess
            .query(
                "select id::text from auth.users
                  where phone = $1 and aud = $2 and deleted_at is null limit 1",
                &[&phone, &AUD],
            )
            .await?;
        let Some(row) = rows.first() else {
            // The account was there a moment ago. Nothing to send to
            // and nothing to say about it.
            return Ok(String::new());
        };
        let user_id: String = row.get(0);
        // A sign in code to a number is filed as a recovery request, the
        // same as a magic link is, and it is the one send that carries
        // the channel in its traits.
        audit::record(
            &sess,
            Actor::Account(&user_id),
            Action::UserRecoveryRequested,
            "",
            Some(serde_json::json!({ "channel": channel })),
        )
        .await?;
        send_phone_code(
            &sess,
            post,
            &user_id,
            Texting {
                otp_type: PHONE_CONFIRMATION,
                to: phone,
                channel,
            },
        )
        .await
    }
    .await;
    match out {
        Ok(id) => {
            sess.commit().await?;
            Ok(id)
        }
        Err(e) => {
            let _ = sess.rollback().await;
            Err(e)
        }
    }
}

/// POST /auth/v1/resend, which sends the last code again.
///
/// It is not a way to start anything. The account has to exist and it
/// has to be waiting for exactly the thing being asked for, and when it
/// is not, the answer is an empty 200 rather than a refusal, because
/// otherwise this endpoint answers the question of who has an account
/// here and what they are in the middle of.
pub async fn resend(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    req: Request<Body>,
) -> Response {
    let Some(pool) = &app.pool else {
        return no_database();
    };
    let (wanted, from) = link_target(&req);
    let body = match read_json(req.into_body()).await {
        Ok(v) => v,
        Err(res) => return res,
    };
    match again(
        pool,
        &body,
        app.cfg.secure_email_change,
        Media {
            email: app.cfg.email_enabled,
            phone: app.cfg.phone_enabled,
        },
        &posting(&app, &wanted, &from),
    )
    .await
    {
        Ok(id) if id.is_empty() => json_body(StatusCode::OK, serde_json::json!({})),
        Ok(id) => json_body(StatusCode::OK, serde_json::json!({"message_id": id})),
        Err(e) => refusal(e, "resend"),
    }
}

/// Which of the two media a project serves at all. They are carried
/// together because resend is the one endpoint that has to answer for
/// both, and two bare bools next to each other are two bools waiting to
/// be passed the wrong way round.
#[derive(Clone, Copy)]
pub(crate) struct Media {
    pub email: bool,
    pub phone: bool,
}

/// Upstream's ResendConfirmationParams.Validate, in its order, and then
/// the send itself.
async fn again(
    pool: &Pool,
    body: &serde_json::Value,
    secure_change: bool,
    serves: Media,
    post: &Post<'_>,
) -> Result<String, Error> {
    let kind = field(body, "type");
    match kind {
        "signup" | "email_change" => validate_pkce(
            field(body, "code_challenge_method"),
            field(body, "code_challenge"),
        )?,
        "sms" | "phone_change" => {}
        _ => {
            return denied(
                "validation_failed",
                "Missing one of these types: signup, email_change, sms, phone_change",
            );
        }
    }
    let (email, phone) = (field(body, "email"), field(body, "phone"));
    if email.is_empty() && kind == "signup" {
        return denied(
            "validation_failed",
            "Type provided requires an email address",
        );
    }
    if phone.is_empty() && kind == "sms" {
        return denied("validation_failed", "Type provided requires a phone number");
    }
    if !email.is_empty() && !phone.is_empty() {
        return denied(
            "validation_failed",
            "Only an email address or phone number should be provided.",
        );
    }
    let (column, held) = if !email.is_empty() {
        if !serves.email {
            return denied("email_provider_disabled", "Email logins are disabled");
        }
        ("email", validate_email(email)?)
    } else if !phone.is_empty() {
        if !serves.phone {
            return denied("phone_provider_disabled", "Phone logins are disabled");
        }
        ("phone", validate_phone(phone)?)
    } else {
        return denied("validation_failed", "Missing email address or phone number");
    };

    let sess = pool.admin().await?;
    let out = resent(&sess, kind, column, &held, secure_change, post).await;
    match out {
        Ok(id) => {
            sess.commit().await?;
            Ok(id)
        }
        Err(e) => {
            let _ = sess.rollback().await;
            Err(e)
        }
    }
}

/// The answer is the provider's message id when a text went out, which
/// is the only thing this endpoint ever says about what it did.
async fn resent(
    sess: &sql::Session,
    kind: &str,
    column: &str,
    held: &str,
    secure_change: bool,
    post: &Post<'_>,
) -> Result<String, Error> {
    let rows = sess
        .query(
            &format!(
                "select id::text, email_confirmed_at is not null, coalesce(email_change, ''),
                        phone_confirmed_at is not null, coalesce(phone_change, ''),
                        coalesce(email, '')
                   from auth.users
                  where {column} = $1 and aud = $2 and is_sso_user = false
                    and deleted_at is null
                  limit 1"
            ),
            &[&held, &AUD],
        )
        .await?;
    // Nobody holds the address or the number. Upstream answers the same
    // empty object it answers a confirmed account with, and so does this.
    let Some(row) = rows.first() else {
        return Ok(String::new());
    };
    let user_id: String = row.get(0);
    let confirmed: bool = row.get(1);
    let pending: String = row.get(2);
    let phone_confirmed: bool = row.get(3);
    let staged_phone: String = row.get(4);
    let email: String = row.get(5);

    match kind {
        "sms" => {
            if phone_confirmed {
                return Ok(String::new());
            }
            audit::record(
                sess,
                Actor::Account(&user_id),
                Action::UserRecoveryRequested,
                "",
                None,
            )
            .await?;
            // Resend has no channel of its own, so it goes out the way
            // upstream sends it, which is always plain sms.
            return send_phone_code(
                sess,
                post,
                &user_id,
                Texting {
                    otp_type: PHONE_CONFIRMATION,
                    to: held,
                    channel: crate::sms::SMS,
                },
            )
            .await;
        }
        "phone_change" => {
            if staged_phone.is_empty() {
                return Ok(String::new());
            }
            return send_phone_code(
                sess,
                post,
                &user_id,
                Texting {
                    otp_type: PHONE_CHANGE,
                    to: &staged_phone,
                    channel: crate::sms::SMS,
                },
            )
            .await;
        }
        _ => {}
    }
    let email = email.as_str();

    match kind {
        "signup" => {
            if confirmed {
                return Ok(String::new());
            }
            // The two resends that write anything are the two that
            // start something over. A resent address change or number
            // change writes nothing, which is upstream's, and is
            // defensible: the change itself was already recorded when
            // it was staged.
            audit::record(
                sess,
                Actor::Account(&user_id),
                Action::UserConfirmationRequested,
                "",
                None,
            )
            .await?;
            within_limit(
                sess,
                &user_id,
                "confirmation_sent_at",
                post.settings.max_frequency,
                TOO_SOON_MAIL,
            )
            .await?;
            let code = mint_code(sess, &user_id, email, "confirmation_token").await?;
            send_code(
                sess,
                post,
                &user_id,
                Outgoing {
                    template: crate::mail::CONFIRMATION,
                    kind: "signup",
                    to: email,
                    code: &code,
                    new_email: "",
                },
            )
            .await?;
        }
        "email_change" => {
            if pending.is_empty() {
                return Ok(String::new());
            }
            // Both codes are drawn again, which is what upstream's
            // sendEmailChange does when it is called a second time: the
            // pair that was mailed is replaced rather than repeated, so
            // whoever half finished the change starts it over.
            stage_change(sess, &user_id, email, &pending, secure_change, post).await?;
        }
        _ => {}
    }
    Ok(String::new())
}

/// GET /auth/v1/user, the account as it stands.
pub async fn user_get(
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
    let asked = requested_aud(&req, &caller.role, &caller.aud);
    let sess = match pool.admin().await {
        Ok(v) => v,
        Err(e) => return refusal(Error::Db(e), "user get"),
    };
    let out = read_user(&sess, &caller, &asked).await;
    match out {
        Ok(user) => match sess.commit().await {
            Ok(()) => json_body(StatusCode::OK, user),
            Err(e) => refusal(Error::Db(e), "user get"),
        },
        Err(e) => {
            let _ = sess.rollback().await;
            refusal(e, "user get")
        }
    }
}

async fn read_user(
    sess: &sql::Session,
    caller: &Caller,
    asked: &str,
) -> Result<serde_json::Value, Error> {
    still_there(sess, caller).await?;
    // A token minted for one audience does not describe an account in
    // another, and a token with no audience at all describes nobody.
    // That is what refuses a service_role key here: it was never minted
    // for a person.
    if caller.aud.is_empty() || asked != caller.aud {
        return denied(
            "validation_failed",
            "Token audience doesn't match request audience",
        );
    }
    Ok(user_json(sess, &caller.user_id).await?)
}

/// Which sessions a logout is about.
#[derive(Clone, Copy)]
enum Scope {
    Global,
    Local,
    Others,
}

/// POST /auth/v1/logout, which deletes sessions rather than tokens.
///
/// Nothing here can revoke the access token that was presented: it is
/// signed, it says what it says, and it stays inside its hour. What
/// this does is take away the session it names, which every endpoint
/// that requires a session then refuses to find, and the refresh tokens
/// that hang off it, which is what stops the client renewing. The
/// answer is 204 and carries nothing, including when there was nothing
/// left to delete.
pub async fn logout(
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
    let asked = field(
        &query_object(req.uri().query().unwrap_or_default()),
        "scope",
    )
    .to_string();
    let scope = match asked.as_str() {
        "" | "global" => Scope::Global,
        "local" => Scope::Local,
        "others" => Scope::Others,
        // Go's %q and Rust's {:?} quote a plain string the same way,
        // which is what this message is made of.
        other => {
            return error_body(
                StatusCode::BAD_REQUEST,
                "validation_failed",
                &format!("Unsupported logout scope {other:?}"),
            );
        }
    };
    let sess = match pool.admin().await {
        Ok(v) => v,
        Err(e) => return refusal(Error::Db(e), "logout"),
    };
    let out = log_out(&sess, &caller, scope).await;
    match out {
        Ok(()) => match sess.commit().await {
            Ok(()) => no_content(),
            Err(e) => refusal(Error::Db(e), "logout"),
        },
        Err(e) => {
            let _ = sess.rollback().await;
            refusal(e, "logout")
        }
    }
}

async fn log_out(sess: &sql::Session, caller: &Caller, scope: Scope) -> Result<(), Error> {
    still_there(sess, caller).await?;
    // One entry whatever the scope, which is upstream's: the trail says
    // somebody logged out and not how much of them logged out.
    audit::record(
        sess,
        Actor::Account(&caller.user_id),
        Action::Logout,
        "",
        None,
    )
    .await?;
    // A token that names no session cannot say which one to keep or
    // which one to drop, so both of the narrow scopes fall back to the
    // wide one, which is upstream's behaviour and the safe direction to
    // be wrong in.
    match (scope, caller.session_id.as_deref()) {
        (Scope::Local, Some(id)) => {
            sess.execute(
                "delete from auth.sessions where id = $1::text::uuid",
                &[&id],
            )
            .await?;
        }
        (Scope::Others, Some(id)) => {
            sess.execute(
                "delete from auth.sessions
                  where user_id = $1::text::uuid and id <> $2::text::uuid",
                &[&caller.user_id, &id],
            )
            .await?;
        }
        _ => {
            sess.execute(
                "delete from auth.sessions where user_id = $1::text::uuid",
                &[&caller.user_id],
            )
            .await?;
        }
    }
    Ok(())
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
    let (wanted, from) = link_target(&req);
    match send_reauthentication(pool, &caller.user_id, &posting(&app, &wanted, &from)).await {
        Ok(id) if id.is_empty() => json_body(StatusCode::OK, serde_json::json!({})),
        Ok(id) => json_body(StatusCode::OK, serde_json::json!({"message_id": id})),
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
    let (wanted, from) = link_target(&req);
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
        &posting(&app, &wanted, &from),
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

pub(crate) fn no_database() -> Response {
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
/// The refresh_token, password and pkce grants are here. The rest need
/// a credential this end cannot check yet, an id token from a provider
/// or a signed challenge, and they answer 501 rather than pretending,
/// because a grant that always fails is worse for a client than one
/// that says it does not exist yet.
pub async fn token(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    req: Request<Body>,
) -> Response {
    let grant = grant_type(req.uri()).unwrap_or_default();
    match grant.as_str() {
        "refresh_token" | "password" | "pkce" => {}
        "id_token" | "web3" => {
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
    let mint = Mint::of(&app, &req);
    let body = match read_json(req.into_body()).await {
        Ok(v) => v,
        Err(res) => return res,
    };

    if grant == "pkce" {
        return match pkce_grant(
            pool,
            field(&body, "auth_code"),
            field(&body, "code_verifier"),
            &mint,
        )
        .await
        {
            Ok((issued, access, refresh)) => {
                // The provider's own tokens ride along, which is how a
                // client that wants to call Google as this person gets
                // something to call it with.
                let mut answer = issued.json();
                if !access.is_empty() {
                    answer["provider_token"] = access.into();
                }
                if !refresh.is_empty() {
                    answer["provider_refresh_token"] = refresh.into();
                }
                json_body(StatusCode::OK, answer)
            }
            Err(e) => refusal(e, "pkce grant"),
        };
    }

    let issued = if grant == "password" {
        let (email, phone) = (field(&body, "email"), field(&body, "phone"));
        if !email.is_empty() && !phone.is_empty() {
            return error_body(
                StatusCode::BAD_REQUEST,
                "validation_failed",
                "Only an email address or phone number should be provided on login.",
            );
        }
        // The phone grant is the same query against a different column.
        // The number is only stripped, not judged: a malformed one is
        // simply a number nobody holds, which is what upstream does here
        // and the right answer for a sign in either way.
        let (column, held) = if !email.is_empty() {
            if !app.cfg.email_enabled {
                return error_body(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "email_provider_disabled",
                    "Email logins are disabled",
                );
            }
            ("email", email.to_string())
        } else if !phone.is_empty() {
            if !app.cfg.phone_enabled {
                return error_body(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "phone_provider_disabled",
                    "Phone logins are disabled",
                );
            }
            ("phone", crate::sms::strip(phone))
        } else {
            return error_body(
                StatusCode::BAD_REQUEST,
                "validation_failed",
                "missing email or phone",
            );
        };
        password_grant(pool, column, &held, field(&body, "password"), &mint).await
    } else {
        refresh(pool, field(&body, "refresh_token"), &mint).await
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
            let code = six_digits();
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
    fn the_password_nobody_types_is_a_different_one_every_time() {
        // The account a magic link or an admin create leaves behind has
        // a password column nobody can satisfy, and the only thing that
        // makes that true is that the value is long and drawn fresh.
        let mut seen = std::collections::HashSet::new();
        for _ in 0..100 {
            let drawn = unguessable_password();
            assert_eq!(drawn.len(), 33, "GoTrue draws 33: {drawn}");
            assert!(
                drawn.chars().all(|c| c.is_ascii_alphanumeric()),
                "not from the alphabet: {drawn}"
            );
            seen.insert(drawn);
        }
        assert_eq!(seen.len(), 100, "the same password came up twice");
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

    /// The envelope reads whatever the handler under it wrote, and
    /// nothing in this server writes a refusal without a code. Upstream
    /// still fills one in for a body that has none, because the newer
    /// shape puts the code where the http status used to be and a client
    /// branching on it would otherwise get null. The two fallbacks are
    /// only reachable from a body written by hand, so that is what this
    /// hands the middleware.
    /// A handler that writes the shape by hand rather than through
    /// error_body, which is the only way to reach the parts of the
    /// envelope no route in this server can reach.
    async fn handwritten(status: StatusCode) -> Response {
        json_body(
            status,
            serde_json::json!({"code": status.as_u16(), "msg": "no code"}),
        )
    }

    async fn under_the_envelope(uri: &str) -> (StatusCode, Option<String>, serde_json::Value) {
        use tower::ServiceExt;
        let app = axum::Router::new()
            .route(
                "/auth/v1/theirs",
                axum::routing::get(|| handwritten(StatusCode::BAD_GATEWAY)),
            )
            .route(
                "/auth/v1/ours",
                axum::routing::get(|| handwritten(StatusCode::BAD_REQUEST)),
            )
            .route(
                "/auth/v1/fine",
                axum::routing::get(|| handwritten(StatusCode::OK)),
            )
            .route(
                "/rest/v1/elsewhere",
                axum::routing::get(|| handwritten(StatusCode::BAD_REQUEST)),
            )
            .layer(axum::middleware::from_fn(envelope));
        let req = Request::builder()
            .uri(uri)
            .header(API_VERSION, "2024-01-01")
            .body(Body::empty())
            .unwrap();
        let res = app.oneshot(req).await.expect("router answers");
        let status = res.status();
        let version = res
            .headers()
            .get(API_VERSION)
            .map(|v| v.to_str().expect("ascii").to_string());
        let bytes = to_bytes(res.into_body(), MAX_BODY).await.expect("a body");
        (
            status,
            version,
            serde_json::from_slice(&bytes).expect("json"),
        )
    }

    /// The envelope reads whatever the handler under it wrote, and
    /// nothing in this server writes a refusal without a code. Upstream
    /// still fills one in for a body that has none, because the newer
    /// shape puts the code where the http status used to be and a client
    /// branching on it would otherwise get null.
    #[tokio::test]
    async fn a_refusal_with_no_code_of_its_own_is_given_one() {
        for (path, code) in [("theirs", "unexpected_failure"), ("ours", "unknown")] {
            let (_, version, body) = under_the_envelope(&format!("/auth/v1/{path}")).await;
            assert_eq!(body["code"], code, "{path}");
            assert_eq!(body["message"], "no code");
            assert_eq!(version.as_deref(), Some("2024-01-01"));
        }
    }

    /// The two things the envelope will not touch whatever it is asked
    /// for: an answer that worked, and an answer from a surface that is
    /// not this one. Neither is reachable through a route today, because
    /// nothing outside the auth handlers writes this shape, and both are
    /// what stops the middleware becoming the whole server's problem the
    /// first time something does.
    #[tokio::test]
    async fn the_envelope_leaves_alone_what_is_not_an_auth_refusal() {
        for path in ["/auth/v1/fine", "/rest/v1/elsewhere"] {
            let (_, version, body) = under_the_envelope(path).await;
            assert!(body["code"].is_number(), "{path} was rewritten: {body}");
            assert_eq!(body["msg"], "no code", "{path}");
            assert_eq!(version, None, "{path} was answered as a version it is not");
        }
    }
}
