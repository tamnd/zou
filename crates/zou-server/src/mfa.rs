//! A second factor, GoTrue's four endpoints and the two claims they
//! move.
//!
//! Enrolling writes a factor row and hands back a secret. Challenging
//! writes a challenge row and starts its five minute clock. Verifying
//! spends the challenge, marks the factor verified the first time, and
//! then does the part that matters: it adds `totp` to the session's amr
//! rows, which makes the session aal2, and issues a fresh token pair
//! that says so. Unenrolling takes the factor away and puts every
//! session it lifted back down to aal1.
//!
//! `aal` and `amr` are not decorations. An RLS policy reads them
//! through auth.jwt() to decide whether this session may see the table
//! at all, so what is written here is what a database policy elsewhere
//! trusts. That is why verifying deletes the account's other sessions
//! rather than leaving them: an account that has just turned on a
//! second factor should not still have aal1 sessions lying around that
//! never had to pass it.
//!
//! Every one of the four writes an audit entry, and these are the only
//! entries in the trail whose `ip_address` column is filled in: upstream
//! passes the address here and nowhere else, so a query that asks where
//! a factor was enrolled from can be answered and the same query about a
//! login cannot.
//!
//! Only TOTP is built. Phone and webauthn factors are refused with the
//! codes upstream refuses them with when they are switched off, which
//! is what an unconfigured GoTrue answers too.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::response::Response;

use crate::audit::{self, Action, Actor};
use crate::auth::{
    Caller, Error, Mint, caller, client_ip, denied, error_body, field, is_uuid, mint_for,
    no_database, now, read_json, refusal, refused, still_there,
};
use crate::{App, json_body, sql};

/// What a project may change about its second factors. Every default
/// here is GoTrue's, including the two floors it applies after reading
/// the environment.
#[derive(Debug)]
pub struct Settings {
    /// GOTRUE_MFA_TOTP_ENROLL_ENABLED, on by default.
    pub totp_enroll: bool,
    /// GOTRUE_MFA_TOTP_VERIFY_ENABLED, on by default. Off, and an
    /// account that already enrolled cannot use what it has, which is
    /// the switch for turning MFA off without deleting anybody's
    /// factors.
    pub totp_verify: bool,
    /// GOTRUE_MFA_MAX_ENROLLED_FACTORS, counting the unverified ones.
    pub max_enrolled: i64,
    /// GOTRUE_MFA_MAX_VERIFIED_FACTORS, counting only the verified.
    pub max_verified: i64,
    /// How long a challenge is good for, in seconds. Five minutes, and
    /// upstream floors it there whatever the environment says.
    pub challenge_expiry: i64,
    /// How long an unverified factor nobody ever challenged is left
    /// lying around. Same five minutes and the same floor.
    pub factor_expiry: i64,
}

impl Default for Settings {
    fn default() -> Settings {
        Settings {
            totp_enroll: true,
            totp_verify: true,
            max_enrolled: 10,
            max_verified: 10,
            challenge_expiry: 300,
            factor_expiry: 300,
        }
    }
}

impl Settings {
    /// The floor upstream applies in configuration.ApplyDefaults, which
    /// is a floor rather than a default: a project that asks for a
    /// thirty second challenge gets five minutes.
    fn challenge_expiry(&self) -> i64 {
        self.challenge_expiry.max(300)
    }

    fn factor_expiry(&self) -> i64 {
        self.factor_expiry.max(300)
    }
}

/// The settings the environment asks for, GOTRUE_ swapped for ZOU_.
/// Anything unset is the default above.
pub fn from_env() -> Result<Settings, String> {
    configured(&|name| std::env::var(name).unwrap_or_default())
}

/// The same, over anything that can look a name up, so the rules are
/// testable without touching the environment the tests run in.
pub fn configured(var: &dyn Fn(&str) -> String) -> Result<Settings, String> {
    let stock = Settings::default();
    Ok(Settings {
        totp_enroll: switch(var, "ZOU_MFA_TOTP_ENROLL_ENABLED", stock.totp_enroll)?,
        totp_verify: switch(var, "ZOU_MFA_TOTP_VERIFY_ENABLED", stock.totp_verify)?,
        max_enrolled: count(var, "ZOU_MFA_MAX_ENROLLED_FACTORS", stock.max_enrolled)?,
        max_verified: count(var, "ZOU_MFA_MAX_VERIFIED_FACTORS", stock.max_verified)?,
        challenge_expiry: seconds(
            var,
            "ZOU_MFA_CHALLENGE_EXPIRY_DURATION",
            stock.challenge_expiry,
        )?,
        factor_expiry: seconds(var, "ZOU_MFA_FACTOR_EXPIRY_DURATION", stock.factor_expiry)?,
    })
}

fn switch(var: &dyn Fn(&str) -> String, name: &str, stock: bool) -> Result<bool, String> {
    match var(name).as_str() {
        "" => Ok(stock),
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        other => Err(format!("{name} is {other:?}, which is not true or false")),
    }
}

fn count(var: &dyn Fn(&str) -> String, name: &str, stock: i64) -> Result<i64, String> {
    let raw = var(name);
    let text = raw.trim();
    if text.is_empty() {
        return Ok(stock);
    }
    text.parse()
        .map_err(|_| format!("{name} is {text:?}, which is not a number"))
}

/// A number of seconds. Upstream writes these as Go durations, so a
/// trailing `s` is accepted and means what it says, and nothing else
/// is: a project that wrote `5m` should hear about it rather than
/// quietly get five seconds.
fn seconds(var: &dyn Fn(&str) -> String, name: &str, stock: i64) -> Result<i64, String> {
    let raw = var(name);
    let text = raw.trim();
    if text.is_empty() {
        return Ok(stock);
    }
    text.strip_suffix('s')
        .unwrap_or(text)
        .parse()
        .map_err(|_| format!("{name} is {text:?}, which is not a number of seconds"))
}

/// The amr entry a verified TOTP factor writes, and one of the three
/// methods that make a session aal2.
const TOTP: &str = "totp";

/// POST /auth/v1/factors, which draws a secret and writes down an
/// unverified factor.
///
/// Nothing is proved here. The account has a factor it has not used
/// yet, the session it enrolled from is still aal1, and it stays that
/// way until a code comes back through verify.
pub async fn enroll(
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
    if caller.anonymous {
        return error_body(
            StatusCode::FORBIDDEN,
            "no_authorization",
            "Anonymous user not allowed to perform these actions",
        );
    }
    let Some(session_id) = caller.session_id.clone() else {
        return no_session();
    };
    let ip = client_ip(&req);
    let body = match read_json(req.into_body()).await {
        Ok(v) => v,
        Err(res) => return res,
    };
    // The issuer is what an authenticator app writes above the code.
    // Unset, it is the host the project answers on, which is what
    // upstream takes out of its site url.
    let issuer = match field(&body, "issuer") {
        "" => host_of(&app.site_url()),
        given => given.to_string(),
    };
    let sess = match pool.admin().await {
        Ok(v) => v,
        Err(e) => return refusal(Error::Db(e), "enroll factor"),
    };
    let out = enrolling(pool, &sess, &app, &caller, &session_id, &body, &issuer, &ip).await;
    finish(sess, out, "enroll factor").await
}

#[allow(clippy::too_many_arguments)]
async fn enrolling(
    pool: &sql::Pool,
    sess: &sql::Session,
    app: &App,
    caller: &Caller,
    session_id: &str,
    body: &serde_json::Value,
    issuer: &str,
    ip: &str,
) -> Result<serde_json::Value, Error> {
    still_there(sess, caller).await?;
    // The switch is read before the body is judged, which is why a
    // project with TOTP off says so rather than complaining about the
    // friendly name of a factor it was never going to write.
    match field(body, "factor_type") {
        "totp" if !app.cfg.mfa.totp_enroll => {
            return unprocessable(
                "mfa_totp_enroll_not_enabled",
                "MFA enroll is disabled for TOTP",
            );
        }
        "totp" => {}
        "phone" => {
            return unprocessable(
                "mfa_phone_enroll_not_enabled",
                "MFA enroll is disabled for Phone",
            );
        }
        "webauthn" => {
            return unprocessable(
                "mfa_webauthn_enroll_not_enabled",
                "MFA enroll is disabled for WebAuthn",
            );
        }
        _ => {
            return denied(
                "validation_failed",
                "factor_type needs to be totp, phone, or webauthn",
            );
        }
    }
    let name = field(body, "friendly_name");
    room_for_another(pool, sess, app, &caller.user_id, session_id, name).await?;

    // The account name in the url is the address, and an account with
    // no address has nothing to put there. Upstream fails the same way
    // and calls it a QR code problem, because that is where the error
    // surfaces.
    let rows = sess
        .query(
            "select coalesce(u.email, '') from auth.users u where u.id = $1::text::uuid",
            &[&caller.user_id],
        )
        .await?;
    let account: String = rows[0].get(0);
    if account.is_empty() {
        return Err(refused(
            StatusCode::INTERNAL_SERVER_ERROR,
            "unexpected_failure",
            "Error generating QR Code",
        ));
    }

    let secret = crate::totp::secret();
    let uri = crate::totp::uri(issuer, &account, &secret);
    let rows = sess
        .query(
            "insert into auth.mfa_factors
                 (id, user_id, friendly_name, factor_type, status,
                  created_at, updated_at, secret)
             values (gen_random_uuid(), $1::text::uuid, $2, 'totp', 'unverified',
                     now(), now(), $3)
             returning id::text",
            &[&caller.user_id, &name, &secret],
        )
        .await?;
    let id: String = rows[0].get(0);
    // No factor_type in the traits. Upstream carries one on the phone
    // and webauthn enrollments and leaves it off the TOTP one, so a
    // reader who filters on it loses exactly the factors this server
    // makes.
    audit::record(
        sess,
        Actor::Account(&caller.user_id),
        Action::FactorInProgress,
        ip,
        Some(serde_json::json!({ "factor_id": id })),
    )
    .await?;
    Ok(serde_json::json!({
        "id": id,
        "type": "totp",
        "friendly_name": name,
        "totp": {
            "qr_code": crate::totp::qr_svg(&uri).unwrap_or_default(),
            "secret": secret,
            "uri": uri,
        },
    }))
}

/// GoTrue's validateFactors: the expired ones go, the name has to be
/// free, there is a ceiling on how many an account may hold, and an
/// account that already has a working factor has to be holding an aal2
/// session to add another.
///
/// The order is upstream's and it is visible: an account at the ceiling
/// with a duplicate name hears about the name.
async fn room_for_another(
    pool: &sql::Pool,
    sess: &sql::Session,
    app: &App,
    user_id: &str,
    session_id: &str,
    name: &str,
) -> Result<(), Error> {
    // An unverified factor nobody ever challenged is somebody who shut
    // the tab halfway through, and it holds a friendly name hostage
    // until it is cleared out.
    housekeeping(
        pool,
        &format!(
            "delete from auth.mfa_factors f
              where f.status <> 'verified'
                and not exists (select 1 from auth.mfa_challenges c where c.factor_id = f.id)
                and f.created_at + interval '{} seconds' < current_timestamp",
            app.cfg.mfa.factor_expiry()
        ),
        &[],
    )
    .await?;
    let rows = sess
        .query(
            "select count(*),
                    count(*) filter (where f.status = 'verified'),
                    count(*) filter (where coalesce(f.friendly_name, '') = $2)
               from auth.mfa_factors f where f.user_id = $1::text::uuid",
            &[&user_id, &name],
        )
        .await?;
    let (held, verified, same_name): (i64, i64, i64) =
        (rows[0].get(0), rows[0].get(1), rows[0].get(2));
    if same_name > 0 {
        return unprocessable_owned(
            "mfa_factor_name_conflict",
            format!("A factor with the friendly name {name:?} for this user already exists"),
        );
    }
    if held >= app.cfg.mfa.max_enrolled || verified >= app.cfg.mfa.max_verified {
        return unprocessable(
            "too_many_enrolled_mfa_factors",
            "Maximum number of verified factors reached, unenroll to continue",
        );
    }
    if verified > 0 && !is_aal2(sess, session_id).await? {
        return Err(refused(
            StatusCode::FORBIDDEN,
            "insufficient_aal",
            "AAL2 required to enroll a new factor",
        ));
    }
    Ok(())
}

/// POST /auth/v1/factors/{id}/challenge, which starts the five minutes
/// a code is expected in.
pub async fn challenge(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    axum::extract::Path(factor_id): axum::extract::Path<String>,
    req: Request<Body>,
) -> Response {
    let Some(pool) = &app.pool else {
        return no_database();
    };
    let caller = match caller(&req) {
        Ok(v) => v,
        Err(res) => return *res,
    };
    if caller.anonymous {
        return anonymous();
    }
    let ip = client_ip(&req);
    let sess = match pool.admin().await {
        Ok(v) => v,
        Err(e) => return refusal(Error::Db(e), "challenge factor"),
    };
    let out = challenging(&sess, &app, &caller, &factor_id, &ip).await;
    finish(sess, out, "challenge factor").await
}

async fn challenging(
    sess: &sql::Session,
    app: &App,
    caller: &Caller,
    factor_id: &str,
    ip: &str,
) -> Result<serde_json::Value, Error> {
    still_there(sess, caller).await?;
    let factor = owned_factor(sess, &caller.user_id, factor_id).await?;
    if !app.cfg.mfa.totp_verify {
        return unprocessable(
            "mfa_totp_verify_not_enabled",
            "MFA verification is disabled for TOTP",
        );
    }
    let rows = sess
        .query(
            &format!(
                "insert into auth.mfa_challenges (id, factor_id, created_at, ip_address)
                 values (gen_random_uuid(), $1::text::uuid, now(), $2::text::inet)
                 returning id::text,
                           floor(extract(epoch from created_at))::bigint + {}",
                app.cfg.mfa.challenge_expiry()
            ),
            &[&factor.id, &ip],
        )
        .await?;
    let (id, expires_at): (String, i64) = (rows[0].get(0), rows[0].get(1));
    // The status the factor held when it was challenged, not the one it
    // will hold after a code comes back, which is what makes a trail of
    // these readable: the first challenge against a factor says
    // unverified and every one after it says verified.
    audit::record(
        sess,
        Actor::Account(&caller.user_id),
        Action::ChallengeCreated,
        ip,
        Some(serde_json::json!({
            "factor_id": factor.id,
            "factor_status": factor.status,
        })),
    )
    .await?;
    // clock_timestamp rather than now, because the column is unique
    // across the whole table upstream and two challenges in one
    // transaction would otherwise be the same instant.
    sess.execute(
        "update auth.mfa_factors
            set last_challenged_at = clock_timestamp(), updated_at = now()
          where id = $1::text::uuid",
        &[&factor.id],
    )
    .await?;
    Ok(serde_json::json!({
        "id": id,
        "type": factor.kind,
        "expires_at": expires_at,
    }))
}

/// POST /auth/v1/factors/{id}/verify, where a code turns into an aal2
/// session.
pub async fn verify(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    axum::extract::Path(factor_id): axum::extract::Path<String>,
    req: Request<Body>,
) -> Response {
    let Some(pool) = &app.pool else {
        return no_database();
    };
    let caller = match caller(&req) {
        Ok(v) => v,
        Err(res) => return *res,
    };
    if caller.anonymous {
        return anonymous();
    }
    let ip = client_ip(&req);
    let body = match read_json(req.into_body()).await {
        Ok(v) => v,
        Err(res) => return res,
    };
    let Some(session_id) = caller.session_id.clone() else {
        return no_session();
    };
    let sess = match pool.admin().await {
        Ok(v) => v,
        Err(e) => return refusal(Error::Db(e), "verify factor"),
    };
    let out = verifying(
        pool,
        &sess,
        &app,
        &caller,
        &session_id,
        &factor_id,
        &body,
        &ip,
    )
    .await;
    finish(sess, out, "verify factor").await
}

#[allow(clippy::too_many_arguments)]
async fn verifying(
    pool: &sql::Pool,
    sess: &sql::Session,
    app: &App,
    caller: &Caller,
    session_id: &str,
    factor_id: &str,
    body: &serde_json::Value,
    ip: &str,
) -> Result<serde_json::Value, Error> {
    still_there(sess, caller).await?;
    let factor = owned_factor(sess, &caller.user_id, factor_id).await?;
    let code = field(body, "code");
    if code.is_empty() {
        return denied("validation_failed", "Code needs to be non-empty");
    }
    if !app.cfg.mfa.totp_verify {
        return unprocessable(
            "mfa_totp_verify_not_enabled",
            "MFA verification is disabled for TOTP",
        );
    }
    let challenge = spend(pool, sess, app, &factor, field(body, "challenge_id"), ip).await?;
    if !crate::totp::valid(code, &factor.secret, now()) {
        return unprocessable("mfa_verification_failed", "Invalid TOTP code entered");
    }
    // Attempted, and upstream means the word loosely: the entry is only
    // written once the code has already checked out, so a wrong code
    // leaves nothing behind and the trail cannot be used to count
    // guesses against a factor.
    audit::record(
        sess,
        Actor::Account(&caller.user_id),
        Action::VerificationAttempted,
        ip,
        Some(serde_json::json!({
            "factor_id": factor.id,
            "challenge_id": challenge,
            "factor_type": factor.kind,
        })),
    )
    .await?;
    sess.execute(
        "update auth.mfa_challenges set verified_at = now() where id = $1::text::uuid",
        &[&challenge],
    )
    .await?;
    if factor.status != "verified" {
        sess.execute(
            "update auth.mfa_factors set status = 'verified', updated_at = now()
              where id = $1::text::uuid",
            &[&factor.id],
        )
        .await?;
    }
    // The amr row is what makes the session aal2, and a second pass
    // through the same factor moves its timestamp rather than writing a
    // second row.
    sess.execute(
        "insert into auth.mfa_amr_claims
             (id, session_id, created_at, updated_at, authentication_method)
         values (gen_random_uuid(), $1::text::uuid, now(), now(), $2)
         on conflict (session_id, authentication_method)
         do update set updated_at = now()",
        &[&session_id, &TOTP],
    )
    .await?;
    // Upstream swaps the session's refresh token here rather than
    // leaving the aal1 one working, so a client that kept the token it
    // had before cannot refresh its way back into an aal2 session it
    // never proved anything for.
    let issued = swap_current(sess, session_id).await?;
    sess.execute(
        "update auth.sessions
            set aal = 'aal2', factor_id = $2::text::uuid, updated_at = now()
          where id = $1::text::uuid",
        &[&session_id, &factor.id],
    )
    .await?;
    // Every other session this account holds is one that never passed
    // the factor, and upstream takes them all away rather than leaving
    // a way in that skips it.
    sess.execute(
        "delete from auth.sessions where user_id = $1::text::uuid and aal < 'aal2'",
        &[&caller.user_id],
    )
    .await?;
    sess.execute(
        "delete from auth.mfa_factors
          where user_id = $1::text::uuid and status = 'unverified' and factor_type = 'totp'",
        &[&caller.user_id],
    )
    .await?;
    // The person just proved a second factor, which is what the hook
    // is told this token was minted for, whatever the session was
    // first proved with.
    let issued = mint_for(
        sess,
        session_id,
        issued,
        TOTP,
        &Mint::at(app, ip.to_string()),
    )
    .await?;
    Ok(issued.json())
}

/// GoTrue's validateChallenge: the challenge has to belong to this
/// factor, be unspent, come from the address it was created from, and
/// still be inside its five minutes.
async fn spend(
    pool: &sql::Pool,
    sess: &sql::Session,
    app: &App,
    factor: &Factor,
    challenge_id: &str,
    ip: &str,
) -> Result<String, Error> {
    if !is_uuid(challenge_id) {
        // A challenge id that was never a uuid finds nothing, which is
        // the same answer as an id that named somebody else's
        // challenge, deliberately.
        return not_found_challenge();
    }
    let rows = sess
        .query(
            &format!(
                "select c.id::text,
                        c.verified_at is not null,
                        host(c.ip_address),
                        c.created_at + interval '{} seconds' < now()
                   from auth.mfa_challenges c
                  where c.id = $1::text::uuid and c.factor_id = $2::text::uuid",
                app.cfg.mfa.challenge_expiry()
            ),
            &[&challenge_id, &factor.id],
        )
        .await?;
    let Some(row) = rows.first() else {
        return not_found_challenge();
    };
    let id: String = row.get(0);
    let (spent, from, expired): (bool, String, bool) = (row.get(1), row.get(2), row.get(3));
    if spent || from != ip {
        return Err(refused(
            StatusCode::UNPROCESSABLE_ENTITY,
            "mfa_ip_address_mismatch",
            "Challenge and verify IP addresses mismatch.",
        ));
    }
    if expired {
        housekeeping(
            pool,
            "delete from auth.mfa_challenges where id = $1::text::uuid",
            &[&id],
        )
        .await?;
        return Err(refused(
            StatusCode::UNPROCESSABLE_ENTITY,
            "mfa_challenge_expired",
            &format!(
                "MFA challenge {id} has expired, verify against another challenge \
                 or create a new challenge."
            ),
        ));
    }
    Ok(id)
}

/// Revoke the session's live refresh token and issue its child, which
/// is upstream's FindTokenBySessionID then GrantRefreshTokenSwap.
async fn swap_current(sess: &sql::Session, session_id: &str) -> Result<String, Error> {
    let rows = sess
        .query(
            "select id from auth.refresh_tokens
              where session_id = $1::text::uuid and revoked = false
              order by created_at, id
              limit 1",
            &[&session_id],
        )
        .await?;
    let Some(row) = rows.first() else {
        return denied(
            "refresh_token_not_found",
            "Invalid Refresh Token: Refresh Token Not Found",
        );
    };
    crate::auth::swap(sess, row.get(0)).await
}

/// DELETE /auth/v1/factors/{id}, which takes the factor away and puts
/// the sessions it lifted back down.
pub async fn unenroll(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    axum::extract::Path(factor_id): axum::extract::Path<String>,
    req: Request<Body>,
) -> Response {
    let Some(pool) = &app.pool else {
        return no_database();
    };
    let caller = match caller(&req) {
        Ok(v) => v,
        Err(res) => return *res,
    };
    if caller.anonymous {
        return anonymous();
    }
    let Some(session_id) = caller.session_id.clone() else {
        return no_session();
    };
    let ip = client_ip(&req);
    let sess = match pool.admin().await {
        Ok(v) => v,
        Err(e) => return refusal(Error::Db(e), "unenroll factor"),
    };
    let out = unenrolling(&sess, &caller, &session_id, &factor_id, &ip).await;
    finish(sess, out, "unenroll factor").await
}

async fn unenrolling(
    sess: &sql::Session,
    caller: &Caller,
    session_id: &str,
    factor_id: &str,
    ip: &str,
) -> Result<serde_json::Value, Error> {
    still_there(sess, caller).await?;
    let factor = owned_factor(sess, &caller.user_id, factor_id).await?;
    if factor.status == "verified" && !is_aal2(sess, session_id).await? {
        return unprocessable(
            "insufficient_aal",
            "AAL2 required to unenroll verified factor",
        );
    }
    // The amr rows go before the factor does, because they are found
    // through the sessions that point at it and nothing points at it
    // afterwards.
    sess.execute(
        "delete from auth.mfa_amr_claims a
          using auth.sessions s
          where a.session_id = s.id
            and s.factor_id = $1::text::uuid
            and a.authentication_method = $2",
        &[&factor.id, &TOTP],
    )
    .await?;
    sess.execute(
        "update auth.sessions set aal = 'aal1', factor_id = null, updated_at = now()
          where user_id = $1::text::uuid and factor_id = $2::text::uuid",
        &[&caller.user_id, &factor.id],
    )
    .await?;
    sess.execute(
        "delete from auth.mfa_factors where id = $1::text::uuid",
        &[&factor.id],
    )
    .await?;
    // The session that did the unenrolling, which is the one trait here
    // that is about the person rather than the factor, and the reason
    // this is the only factor entry that carries one.
    audit::record(
        sess,
        Actor::Account(&caller.user_id),
        Action::FactorUnenrolled,
        ip,
        Some(serde_json::json!({
            "factor_id": factor.id,
            "factor_status": factor.status,
            "session_id": session_id,
        })),
    )
    .await?;
    Ok(serde_json::json!({"id": factor.id}))
}

/// A factor as the endpoints need it, which is only ever one the caller
/// owns.
struct Factor {
    id: String,
    kind: String,
    status: String,
    secret: String,
}

/// GoTrue's loadFactor. A factor id that is not a uuid and a factor id
/// that belongs to somebody else are both 404, and neither says which,
/// which is what stops the endpoint being a way to ask whether an id
/// exists.
async fn owned_factor(
    sess: &sql::Session,
    user_id: &str,
    factor_id: &str,
) -> Result<Factor, Error> {
    if !is_uuid(factor_id) {
        return Err(refused(
            StatusCode::NOT_FOUND,
            "validation_failed",
            "factor_id must be an UUID",
        ));
    }
    let rows = sess
        .query(
            "select f.id::text, f.factor_type::text, f.status::text, coalesce(f.secret, '')
               from auth.mfa_factors f
              where f.id = $1::text::uuid and f.user_id = $2::text::uuid",
            &[&factor_id, &user_id],
        )
        .await?;
    let Some(row) = rows.first() else {
        return Err(refused(
            StatusCode::NOT_FOUND,
            "mfa_factor_not_found",
            "Factor not found",
        ));
    };
    Ok(Factor {
        id: row.get(0),
        kind: row.get(1),
        status: row.get(2),
        secret: row.get(3),
    })
}

/// Whether the session the caller is holding has already passed a
/// factor. A caller with no session at all has not.
async fn is_aal2(sess: &sql::Session, session_id: &str) -> Result<bool, Error> {
    let rows = sess
        .query(
            "select coalesce(s.aal::text, '') = 'aal2' from auth.sessions s
              where s.id = $1::text::uuid",
            &[&session_id],
        )
        .await?;
    Ok(rows.first().map(|row| row.get(0)).unwrap_or(false))
}

/// The host of a url, which is what an authenticator app is told it is
/// holding a code for.
fn host_of(url: &str) -> String {
    url.parse::<axum::http::Uri>()
        .ok()
        .and_then(|uri| uri.authority().map(|a| a.as_str().to_string()))
        .unwrap_or_default()
}

/// A write that is housekeeping rather than part of the answer:
/// clearing out a factor somebody abandoned, throwing away a challenge
/// that ran out of time. It goes in its own transaction because the
/// request it is riding on is usually about to refuse, and rolling the
/// answer back should not quietly put the rubbish back. Upstream does
/// these as their own database calls outside the transaction for the
/// same reason.
async fn housekeeping(
    pool: &sql::Pool,
    sql: &str,
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
) -> Result<(), Error> {
    let own = pool.admin().await?;
    match own.execute(sql, params).await {
        Ok(_) => Ok(own.commit().await?),
        Err(e) => {
            let _ = own.rollback().await;
            Err(Error::Db(e))
        }
    }
}

/// Commit, answer, or roll back and say why. Every one of these
/// endpoints ends the same way.
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

fn unprocessable<T>(code: &'static str, msg: &str) -> Result<T, Error> {
    Err(refused(StatusCode::UNPROCESSABLE_ENTITY, code, msg))
}

fn unprocessable_owned<T>(code: &'static str, msg: String) -> Result<T, Error> {
    Err(refused(StatusCode::UNPROCESSABLE_ENTITY, code, &msg))
}

fn not_found_challenge<T>() -> Result<T, Error> {
    unprocessable(
        "mfa_factor_not_found",
        "MFA factor with the provided challenge ID not found",
    )
}

fn anonymous() -> Response {
    error_body(
        StatusCode::FORBIDDEN,
        "no_authorization",
        "Anonymous user not allowed to perform these actions",
    )
}

/// Upstream calls a factor endpoint reached without a session an
/// internal error, because its own middleware should have made it
/// impossible, and answers 500. A token minted for a service role has
/// no session and lands here.
fn no_session() -> Response {
    error_body(
        StatusCode::INTERNAL_SERVER_ERROR,
        "unexpected_failure",
        "A valid session and a registered user are required to enroll a factor",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> String + use<> {
        let pairs: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name| {
            pairs
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
                .unwrap_or_default()
        }
    }

    #[test]
    fn an_empty_environment_is_gotrues_defaults() {
        let cfg = configured(&env(&[])).expect("nothing to read");
        assert!(cfg.totp_enroll && cfg.totp_verify);
        assert_eq!((cfg.max_enrolled, cfg.max_verified), (10, 10));
        assert_eq!((cfg.challenge_expiry, cfg.factor_expiry), (300, 300));
    }

    #[test]
    fn the_switches_and_the_numbers_are_read() {
        let cfg = configured(&env(&[
            ("ZOU_MFA_TOTP_ENROLL_ENABLED", "false"),
            ("ZOU_MFA_TOTP_VERIFY_ENABLED", "0"),
            ("ZOU_MFA_MAX_ENROLLED_FACTORS", "3"),
            ("ZOU_MFA_MAX_VERIFIED_FACTORS", "2"),
            // Upstream writes the durations as Go durations, and the
            // seconds suffix is the only one that means seconds.
            ("ZOU_MFA_CHALLENGE_EXPIRY_DURATION", "900"),
            ("ZOU_MFA_FACTOR_EXPIRY_DURATION", "600s"),
        ]))
        .expect("all readable");
        assert!(!cfg.totp_enroll && !cfg.totp_verify);
        assert_eq!((cfg.max_enrolled, cfg.max_verified), (3, 2));
        assert_eq!((cfg.challenge_expiry, cfg.factor_expiry), (900, 600));
        // And the floor is still a floor whatever was asked for.
        assert_eq!(cfg.challenge_expiry(), 900);
        let low = configured(&env(&[("ZOU_MFA_CHALLENGE_EXPIRY_DURATION", "30")])).expect("read");
        assert_eq!((low.challenge_expiry, low.challenge_expiry()), (30, 300));
    }

    #[test]
    fn a_setting_that_is_not_what_it_should_be_says_which_one() {
        assert_eq!(
            configured(&env(&[("ZOU_MFA_TOTP_VERIFY_ENABLED", "off")])).unwrap_err(),
            "ZOU_MFA_TOTP_VERIFY_ENABLED is \"off\", which is not true or false"
        );
        assert_eq!(
            configured(&env(&[("ZOU_MFA_MAX_ENROLLED_FACTORS", "lots")])).unwrap_err(),
            "ZOU_MFA_MAX_ENROLLED_FACTORS is \"lots\", which is not a number"
        );
        assert_eq!(
            configured(&env(&[("ZOU_MFA_FACTOR_EXPIRY_DURATION", "5m")])).unwrap_err(),
            "ZOU_MFA_FACTOR_EXPIRY_DURATION is \"5m\", which is not a number of seconds"
        );
    }
}
