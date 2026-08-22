//! The audit trail, GoTrue's `auth.audit_log_entries`.
//!
//! One row per auth event, written on the connection the flow is already
//! holding, so an entry commits with the thing it describes or does not
//! exist. A signup whose transaction rolled back has no signup entry,
//! and that is the property that makes the trail worth reading at all.
//!
//! The row is mostly one json payload. Five keys are always there:
//!
//! - `actor_id`, `actor_username` and `actor_via_sso`, which say who did
//!   it, read out of the actor's own row rather than carried in from the
//!   handler, because the handler usually holds an id and nothing else.
//! - `action`, the event, and `log_type`, the family it belongs to.
//!   Upstream keeps that mapping in one table and so does this, because
//!   `log_type` is what the dashboard groups by.
//!
//! Two more appear when there is something to put in them: `actor_name`,
//! when the account has a `full_name` in its metadata, and `traits`,
//! which is whatever the event has to say for itself.
//!
//! Every entry is written twice. The row is one copy and a line on the
//! log stream is the other, which is upstream's `auth_audit_event` and
//! is the copy an operator ships somewhere. The two are written from
//! one statement so they cannot disagree: the payload the line carries
//! is the payload postgres built, `actor_username` and `actor_name`
//! included, rather than a second guess at it assembled here.
//!
//! Three upstream details are worth knowing before reading any of this,
//! because all three look like bugs and all three are load bearing for
//! anybody who has queried this table:
//!
//! - Most entries have an empty `ip_address` column. The ones that fill
//!   it are the four factor events and the two identity linking events,
//!   which is exactly the set upstream fills and no larger. The address
//!   is in the request for all of them, so the empty column is upstream
//!   forgetting to pass it rather than deciding not to, but a query
//!   counting distinct addresses would start seeing different numbers if
//!   this end filled them all in.
//! - An admin acting on somebody else's account is not a person. The
//!   actor is a synthetic user whose id is the nil uuid and whose
//!   username is the role name, so every service_role action in the
//!   trail is attributed to `service_role` rather than to whoever holds
//!   the key.
//! - An anonymous sign in writes nothing at all. It is the one grant
//!   with no entry of its own.

use crate::sql;
use std::time::Duration;

/// What a project has said to do with its trail.
///
/// Both of these are about the table and neither is about the log
/// stream, which is always written: a project that has turned the rows
/// off has done so because something else is holding them, and a
/// project that prunes wants the old ones gone from postgres rather
/// than gone from whatever it shipped them to.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Settings {
    /// `ZOU_AUDIT_LOG_DISABLE_POSTGRES`, GoTrue's
    /// GOTRUE_AUDIT_LOG_DISABLE_POSTGRES. On, and no row is written and
    /// the log line is the whole trail. Off by default, as upstream.
    pub disable_postgres: bool,
    /// `ZOU_AUDIT_LOG_RETENTION`, which upstream has no equivalent of
    /// because upstream is run by somebody who prunes their own
    /// database. None keeps every row forever, and is the default,
    /// because deleting somebody's audit trail on their behalf because
    /// they did not read a release note is the wrong failure to have.
    pub retention: Option<Duration>,
}

/// What the environment says, GoTrue's name with GOTRUE_ swapped for
/// ZOU_ where there is a GoTrue name to swap.
pub fn from_env() -> Result<Settings, String> {
    configured(&|name| std::env::var(name).unwrap_or_default())
}

pub fn configured(var: &dyn Fn(&str) -> String) -> Result<Settings, String> {
    let disable_postgres = match var("ZOU_AUDIT_LOG_DISABLE_POSTGRES").trim() {
        "" | "false" | "0" => false,
        "true" | "1" => true,
        other => {
            return Err(format!(
                "ZOU_AUDIT_LOG_DISABLE_POSTGRES is {other:?}, which is not true or false"
            ));
        }
    };
    let retention = match var("ZOU_AUDIT_LOG_RETENTION").trim() {
        "" => None,
        text => match crate::limit::duration(text) {
            // An hour is the shortest that means anything: the pruner
            // wakes hourly, so anything under it is a retention the
            // server cannot honour and would be read as one it does.
            Some(keep) if keep >= SWEEP => Some(keep),
            Some(_) => {
                return Err(format!(
                    "ZOU_AUDIT_LOG_RETENTION is {text:?}, which is shorter than the hour between sweeps"
                ));
            }
            None => {
                return Err(format!(
                    "ZOU_AUDIT_LOG_RETENTION is {text:?}, which is not a duration like 720h"
                ));
            }
        },
    };
    Ok(Settings {
        disable_postgres,
        retention,
    })
}

/// What happened. Upstream's AuditAction, minus the actions for
/// endpoints this server does not serve: the passkey three, and
/// `factor_deleted` and `factor_updated`, which belong to the admin
/// factor endpoints rather than to the four an account uses on itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Login,
    Logout,
    InviteAccepted,
    UserSignedUp,
    UserInvited,
    UserDeleted,
    UserModified,
    UserRecoveryRequested,
    UserReauthenticateRequested,
    UserConfirmationRequested,
    UserRepeatedSignUp,
    UserUpdatedPassword,
    TokenRevoked,
    TokenRefreshed,
    FactorInProgress,
    FactorUnenrolled,
    ChallengeCreated,
    VerificationAttempted,
    IdentityLinked,
    IdentityUnlinked,
}

/// Every action there is, which is what the completeness test walks
/// rather than a list written out again next to it.
///
/// A list written by hand goes stale, so `place` below is the thing
/// that stops this one from doing it: it matches on the action
/// exhaustively, so a variant added to the enum and left out of here
/// does not compile, and the test that every entry answers with its own
/// position is what catches an arm that was added pointing at somebody
/// else's slot.
pub const EVERY: [Action; 20] = [
    Action::Login,
    Action::Logout,
    Action::InviteAccepted,
    Action::UserSignedUp,
    Action::UserInvited,
    Action::UserDeleted,
    Action::UserModified,
    Action::UserRecoveryRequested,
    Action::UserReauthenticateRequested,
    Action::UserConfirmationRequested,
    Action::UserRepeatedSignUp,
    Action::UserUpdatedPassword,
    Action::TokenRevoked,
    Action::TokenRefreshed,
    Action::FactorInProgress,
    Action::FactorUnenrolled,
    Action::ChallengeCreated,
    Action::VerificationAttempted,
    Action::IdentityLinked,
    Action::IdentityUnlinked,
];

impl Action {
    /// Where this action sits in `EVERY`. Only the test build has it,
    /// because guarding a hand written list is all it is for, and that
    /// guard bites when the tests are compiled.
    #[cfg(test)]
    const fn place(self) -> usize {
        match self {
            Action::Login => 0,
            Action::Logout => 1,
            Action::InviteAccepted => 2,
            Action::UserSignedUp => 3,
            Action::UserInvited => 4,
            Action::UserDeleted => 5,
            Action::UserModified => 6,
            Action::UserRecoveryRequested => 7,
            Action::UserReauthenticateRequested => 8,
            Action::UserConfirmationRequested => 9,
            Action::UserRepeatedSignUp => 10,
            Action::UserUpdatedPassword => 11,
            Action::TokenRevoked => 12,
            Action::TokenRefreshed => 13,
            Action::FactorInProgress => 14,
            Action::FactorUnenrolled => 15,
            Action::ChallengeCreated => 16,
            Action::VerificationAttempted => 17,
            Action::IdentityLinked => 18,
            Action::IdentityUnlinked => 19,
        }
    }

    /// The string in the payload, which is what a query filters on.
    pub fn name(self) -> &'static str {
        match self {
            Action::Login => "login",
            Action::Logout => "logout",
            Action::InviteAccepted => "invite_accepted",
            Action::UserSignedUp => "user_signedup",
            Action::UserInvited => "user_invited",
            Action::UserDeleted => "user_deleted",
            Action::UserModified => "user_modified",
            Action::UserRecoveryRequested => "user_recovery_requested",
            Action::UserReauthenticateRequested => "user_reauthenticate_requested",
            Action::UserConfirmationRequested => "user_confirmation_requested",
            Action::UserRepeatedSignUp => "user_repeated_signup",
            Action::UserUpdatedPassword => "user_updated_password",
            Action::TokenRevoked => "token_revoked",
            Action::TokenRefreshed => "token_refreshed",
            Action::FactorInProgress => "factor_in_progress",
            Action::FactorUnenrolled => "factor_unenrolled",
            Action::ChallengeCreated => "challenge_created",
            Action::VerificationAttempted => "verification_attempted",
            Action::IdentityLinked => "identity_linked",
            Action::IdentityUnlinked => "identity_unlinked",
        }
    }

    /// Which family the event belongs to, upstream's ActionLogTypeMap.
    /// The names read oddly in places, `user_signedup` being a `team`
    /// event and `login` being an `account` one, and they are upstream's
    /// exactly because a dashboard groups by this.
    pub fn log_type(self) -> &'static str {
        match self {
            Action::Login | Action::Logout | Action::InviteAccepted => "account",
            Action::UserSignedUp | Action::UserInvited | Action::UserDeleted => "team",
            Action::TokenRevoked | Action::TokenRefreshed => "token",
            Action::UserModified
            | Action::UserRecoveryRequested
            | Action::UserReauthenticateRequested
            | Action::UserConfirmationRequested
            | Action::UserRepeatedSignUp
            | Action::UserUpdatedPassword => "user",
            Action::FactorInProgress
            | Action::FactorUnenrolled
            | Action::ChallengeCreated
            | Action::VerificationAttempted => "factor",
            // Upstream files both linking events under the user rather
            // than under the account, which is the odd one in the table.
            Action::IdentityLinked | Action::IdentityUnlinked => "user",
        }
    }
}

/// Who did it. An account, named by id, or a role, which is what an
/// admin token carries instead of a person.
#[derive(Debug, Clone, Copy)]
pub enum Actor<'a> {
    Account(&'a str),
    Role(&'a str),
}

/// The nil uuid, the instance every row in this schema carries and the
/// actor id of every entry a role wrote.
const NOBODY: &str = "00000000-0000-0000-0000-000000000000";

/// An entry an account wrote. The username, the SSO flag and the name
/// are read out of the actor's own row inside the statement, so a call
/// site has to carry nothing but the id it already has.
///
/// A left join rather than a select from auth.users, so an actor whose
/// row is not there still gets an entry: upstream writes one either way,
/// because it holds the account in memory rather than reading it back,
/// and an event that quietly did not happen is the last thing a trail
/// should do.
///
/// `traits` is left out of the payload entirely when there are none,
/// rather than written as null, because upstream only sets the key when
/// it has something to put in it, and `payload ? 'traits'` should mean
/// the same thing here as it does there.
///
/// `clock_timestamp` rather than `now`, which is the one place this
/// deliberately does not use the transaction's clock. A flow that writes
/// two entries writes them in an order that means something, a signup
/// and then the login it turned into, and `now` would stamp both with
/// the transaction's start and leave a reader sorting by `created_at`
/// with a tie it cannot break.
const BY_ACCOUNT: &str = "
select jsonb_build_object(
           'actor_id', $1::text,
           'actor_via_sso', coalesce(u.is_sso_user, false),
           'actor_username', coalesce(nullif(u.phone, ''), nullif(u.email, ''), ''),
           'action', $2::text,
           'log_type', $3::text)
       || case when u.raw_user_meta_data ? 'full_name'
               then jsonb_build_object('actor_name', u.raw_user_meta_data -> 'full_name')
               else '{}'::jsonb end
       || case when $4::jsonb is null then '{}'::jsonb
               else jsonb_build_object('traits', $4::jsonb) end as payload
  from (select 1) as one
  left join auth.users u on u.id = $1::text::uuid";

/// An entry a role wrote. A role has no row to read, no metadata and no
/// SSO to speak of, and its id is nobody's: upstream builds a synthetic
/// user out of the claim rather than looking one up, so this is that
/// user rather than a lookup that failed.
const BY_ROLE: &str = "
select jsonb_build_object(
           'actor_id', '00000000-0000-0000-0000-000000000000'::text,
           'actor_via_sso', false,
           'actor_username', $1::text,
           'action', $2::text,
           'log_type', $3::text)
       || case when $4::jsonb is null then '{}'::jsonb
               else jsonb_build_object('traits', $4::jsonb) end as payload";

/// The row, and the four things the log line needs, handed back so that
/// the line says what the row says rather than what this end guessed it
/// would say.
///
/// The timestamp comes back already spelled the way upstream's stream
/// copy spells it, which is a `to_char` here rather than a date library
/// in the binary for the sake of one field.
const WRITTEN: &str = "
insert into auth.audit_log_entries (instance_id, id, payload, created_at, ip_address)
select '00000000-0000-0000-0000-000000000000', gen_random_uuid(), payload,
       clock_timestamp(), $5::text
  from entry
returning id::text,
          payload,
          to_char(created_at at time zone 'utc', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),
          ip_address";

/// The same four things, without the row.
///
/// It selects the ip back out of the parameter it was never going to
/// write, which looks pointless and is not: the two tails have to take
/// the same five parameters, because the statement is chosen at the last
/// moment and the caller has already bound them.
const UNWRITTEN: &str = "
select gen_random_uuid()::text,
       payload,
       to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),
       $5::text
  from entry";

/// One entry, on the log stream and in the table unless the project has
/// turned the table off.
///
/// A failure to write is the flow's failure. That is deliberate and it
/// is upstream's: an audit entry that could not be written and was
/// swallowed leaves a trail that is silently incomplete, which is worse
/// than a signup that returns an error somebody can retry.
pub async fn record(
    sess: &sql::Session,
    actor: Actor<'_>,
    action: Action,
    ip: &str,
    traits: Option<serde_json::Value>,
) -> Result<(), sql::Error> {
    let (payload, who) = match actor {
        Actor::Account(id) => (BY_ACCOUNT, uuid_or_nil(id)),
        Actor::Role(role) => (BY_ROLE, role.to_string()),
    };
    let tail = match sess.pool().audit_rows() {
        true => WRITTEN,
        false => UNWRITTEN,
    };
    let name = action.name();
    let log_type = action.log_type();
    let rows = sess
        .query(
            &format!("with entry as ({payload}) {tail}"),
            &[&who, &name, &log_type, &traits, &ip],
        )
        .await?;
    // One row either way, and both tails select from a CTE that is one
    // row by construction.
    if let Some(row) = rows.first() {
        streamed(row.get(0), row.get(1), row.get(2), row.get(3));
    }
    Ok(())
}

/// The log stream's copy of an entry, upstream's `auth_audit_event`.
///
/// It carries the payload plus the three things the payload does not:
/// which row this is, where it came from, and when. Upstream adds a
/// request id and a user agent here as well. The request id has a
/// better answer on this end already, since a json log line carries the
/// trace and span the record was written under and those open the whole
/// request rather than only naming it, and the user agent is not
/// something the trail has ever been able to filter on, so neither is
/// invented here.
fn streamed(id: String, payload: serde_json::Value, at: String, ip: String) {
    // Nothing below info, because this is the copy somebody keeps.
    if !log::log_enabled!(log::Level::Info) {
        return;
    }
    if let Some(event) = event(id, payload, at, ip) {
        log::info!("{event}");
    }
}

/// The line itself, built apart from the logging so a test can read it.
/// A payload that is not an object is not something postgres can have
/// handed back, and is dropped rather than wrapped in something that
/// would not parse as an event on the other end.
fn event(
    id: String,
    payload: serde_json::Value,
    at: String,
    ip: String,
) -> Option<serde_json::Value> {
    let serde_json::Value::Object(mut event) = payload else {
        return None;
    };
    event.insert("audit_log_id".to_string(), id.into());
    event.insert("ip_address".to_string(), ip.into());
    event.insert("created_at".to_string(), at.into());
    Some(serde_json::Value::Object(event))
}

/// How often the pruner wakes. An hour rather than a day because a
/// sweep that finds nothing is one statement, and because a node that
/// is restarted every few hours would otherwise never get round to it.
const SWEEP: Duration = Duration::from_secs(60 * 60);

/// How many rows one statement deletes, and how many statements one
/// sweep runs. The product is the ceiling on a single sweep, which
/// matters on the first one after retention is turned on against a
/// table that has been filling for a year: without a ceiling that sweep
/// is one transaction holding locks on every row it is deleting for as
/// long as it takes, on a table the auth surface is still inserting
/// into.
const BATCH: i64 = 5_000;
const BATCHES: usize = 40;

/// The advisory lock the pruning node holds. Session scoped locks are
/// what the cron ticker uses because it holds one for its whole life;
/// this one is transaction scoped, so it goes back when the sweep's
/// transaction ends however it ends, including by the process dying.
const LOCK: i64 = 730_517;

/// Rows go by age, oldest first, which is the order a bounded delete has
/// to work in or it never reaches the oldest ones.
///
/// There is deliberately no index behind this. The only index upstream's
/// schema puts on the table is on `instance_id`, which holds the nil
/// uuid in every row and so answers nothing, and adding one on
/// `created_at` would be a write on the hot path of every login,
/// refresh and revoke to save a sequential scan that happens once an
/// hour on a table this statement is keeping small. A project that
/// wants the index for its own dashboard queries can add one, and this
/// gets faster for free.
const OLDEST: &str = "
delete from auth.audit_log_entries
 where id in (select id from auth.audit_log_entries
               where created_at < now() - make_interval(secs => $1::double precision)
               order by created_at
               limit $2)";

/// Delete what the project said not to keep, until the process ends.
///
/// Started from the gate for the same reason the cron ticker is: a
/// router can be built outside a runtime.
pub fn prune(app: std::sync::Arc<crate::App>) {
    let Some(keep) = app.cfg.audit.retention else {
        return;
    };
    let Some(pool) = app.pool.clone() else {
        return;
    };
    tokio::spawn(async move {
        loop {
            match sweep(&pool, keep).await {
                Ok(0) => {}
                Ok(gone) => log::info!("audit: {gone} entries older than the retention are gone"),
                Err(e) => log::warn!("audit: the sweep did not finish: {e}"),
            }
            tokio::time::sleep(SWEEP).await;
        }
    });
}

/// One sweep, and how many rows it removed.
///
/// Public because the loop above is an hour long and a test cannot wait
/// for it. What the loop does is call this, so a test that calls it is
/// testing the pruning rather than a stand in for it.
pub async fn sweep(pool: &sql::Pool, keep: Duration) -> Result<u64, sql::Error> {
    let sess = pool.admin().await?;
    let out = sweeping(&sess, keep).await;
    match out {
        Ok(gone) => sess.commit().await.map(|()| gone),
        Err(e) => {
            let _ = sess.rollback().await;
            Err(e)
        }
    }
}

async fn sweeping(sess: &sql::Session, keep: Duration) -> Result<u64, sql::Error> {
    // One node deletes. Every other node asks, is told no, and comes
    // back in an hour, which is what a fleet of forty nodes all holding
    // the same project should do instead of forty overlapping deletes
    // of the same rows.
    let mine: bool = sess
        .query("select pg_try_advisory_xact_lock($1)", &[&LOCK])
        .await?[0]
        .get(0);
    if !mine {
        return Ok(0);
    }
    let secs = keep.as_secs_f64();
    let mut gone = 0;
    for round in 0..BATCHES {
        let batch = sess.execute(OLDEST, &[&secs, &BATCH]).await?;
        gone += batch;
        if batch < BATCH as u64 {
            return Ok(gone);
        }
        if round + 1 == BATCHES {
            // Said out loud rather than left to be inferred from a
            // table that is not shrinking as fast as somebody expected.
            log::info!("audit: {gone} entries gone and more to go, the rest go on the next sweep");
        }
    }
    Ok(gone)
}

/// An actor id that postgres will accept as a uuid. Every caller has one
/// already, this is the guard for the one that does not: a malformed id
/// would fail the cast and take the whole flow down with it, and losing
/// a signup because its audit entry could not name the actor is worse
/// than an entry with an empty username.
fn uuid_or_nil(id: &str) -> String {
    if crate::auth::is_uuid(id) {
        id.to_string()
    } else {
        NOBODY.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_action_is_upstreams_string() {
        // The spelling matters more than anything else in this module:
        // these strings are what a saved query filters on.
        assert_eq!(Action::Login.name(), "login");
        assert_eq!(Action::UserSignedUp.name(), "user_signedup");
        assert_eq!(
            Action::UserReauthenticateRequested.name(),
            "user_reauthenticate_requested"
        );
        assert_eq!(Action::UserUpdatedPassword.name(), "user_updated_password");
        assert_eq!(Action::FactorInProgress.name(), "factor_in_progress");
        assert_eq!(
            Action::VerificationAttempted.name(),
            "verification_attempted"
        );
        assert_eq!(Action::IdentityUnlinked.name(), "identity_unlinked");
    }

    #[test]
    fn the_log_type_of_an_action_is_upstreams_table() {
        assert_eq!(Action::Login.log_type(), "account");
        assert_eq!(Action::Logout.log_type(), "account");
        assert_eq!(Action::InviteAccepted.log_type(), "account");
        assert_eq!(Action::UserSignedUp.log_type(), "team");
        assert_eq!(Action::UserInvited.log_type(), "team");
        assert_eq!(Action::UserDeleted.log_type(), "team");
        assert_eq!(Action::TokenRefreshed.log_type(), "token");
        assert_eq!(Action::TokenRevoked.log_type(), "token");
        assert_eq!(Action::UserModified.log_type(), "user");
        assert_eq!(Action::UserRepeatedSignUp.log_type(), "user");
        assert_eq!(Action::IdentityLinked.log_type(), "user");
        assert_eq!(Action::IdentityUnlinked.log_type(), "user");
        assert_eq!(Action::ChallengeCreated.log_type(), "factor");
        assert_eq!(Action::FactorUnenrolled.log_type(), "factor");
    }

    /// What keeps `EVERY` from going stale. `place` is an exhaustive
    /// match, so an action nobody added to the list still has to be
    /// given a slot, and this is what says the slot it was given is its
    /// own rather than somebody else's.
    #[test]
    fn every_action_is_in_the_list_once_and_in_its_own_place() {
        for (at, action) in EVERY.iter().enumerate() {
            assert_eq!(
                action.place(),
                at,
                "{} is at {at} and says it is at {}",
                action.name(),
                action.place()
            );
        }
    }

    /// The three files that write the trail. Read at compile time
    /// rather than opened, so this cannot be a test that quietly passes
    /// because it looked in the wrong directory.
    const WRITERS: [&str; 3] = [
        include_str!("auth.rs"),
        include_str!("mfa.rs"),
        include_str!("admin.rs"),
    ];

    /// Actions this server names and writes nowhere, with the reason.
    ///
    /// Empty, and the intent is that it stays that way. It held
    /// `invite_accepted` until zou #517 gave `/authorize` an
    /// `invite_token`, which was the flow that had nowhere to write
    /// from. Anything added here needs the reason next to it, because
    /// this is the list a reviewer reads to find out what the trail
    /// does not cover.
    const UNWRITTEN: [Action; 0] = [];

    /// The completeness review, as a test rather than as a paragraph
    /// somebody wrote once.
    ///
    /// It reads the writers rather than driving the flows because what
    /// it is asking is not whether a given flow works, which the live
    /// suite next door asks one flow at a time. It is whether the set
    /// has a hole: an action that exists, that a dashboard has a tab
    /// for, and that nothing in this server ever produces. That is the
    /// failure that is invisible from inside any one flow's test.
    #[test]
    fn every_action_is_written_by_something_or_is_known_not_to_be() {
        let mut orphaned = Vec::new();
        let mut stale = Vec::new();
        for action in EVERY {
            let needle = format!("Action::{action:?}");
            let written = WRITERS.iter().any(|src| src.contains(&needle));
            let excused = UNWRITTEN.contains(&action);
            match (written, excused) {
                (false, false) => orphaned.push(action.name()),
                // The list is a record of what is not built yet, so an
                // entry on it that somebody has since built is a list
                // that has started lying about the server.
                (true, true) => stale.push(action.name()),
                _ => {}
            }
        }
        assert!(
            orphaned.is_empty(),
            "these actions exist and nothing writes them: {orphaned:?}"
        );
        assert!(
            stale.is_empty(),
            "these are written now and still listed as not written: {stale:?}"
        );
    }

    #[test]
    fn the_stream_copy_is_the_row_plus_the_three_things_the_row_holds_in_columns() {
        let payload = serde_json::json!({
            "action": "login",
            "actor_id": "6b3f2e3c-2c3a-4d1e-9c9a-0b1f2d3e4a5b",
            "actor_username": "somebody@zou.test",
            "actor_via_sso": false,
            "log_type": "account",
            "traits": {"provider": "email"},
        });
        let line = event(
            "9f1c8a7e-1111-4222-8333-444455556666".to_string(),
            payload.clone(),
            "2026-08-19T10:11:12.345Z".to_string(),
            "198.51.100.4".to_string(),
        )
        .expect("a payload postgres built is an object");

        // Everything the row's payload said, unchanged. The line is the
        // row rather than a second guess at what the row would say, and
        // an operator diffing the two should find nothing.
        for (key, was) in payload.as_object().expect("an object") {
            assert_eq!(&line[key], was, "the line changed {key}");
        }
        assert_eq!(line["audit_log_id"], "9f1c8a7e-1111-4222-8333-444455556666");
        assert_eq!(line["created_at"], "2026-08-19T10:11:12.345Z");
        assert_eq!(line["ip_address"], "198.51.100.4");

        assert!(
            event(
                serde_json::json!("x").to_string(),
                serde_json::json!("not an object"),
                String::new(),
                String::new(),
            )
            .is_none(),
            "nothing that would not parse as an event goes on the stream",
        );
    }

    #[test]
    fn no_two_actions_share_a_name() {
        let mut names: Vec<&str> = EVERY.iter().map(|a| a.name()).collect();
        names.sort_unstable();
        let mut unique = names.clone();
        unique.dedup();
        assert_eq!(names, unique);
        // And every one of them lands in one of upstream's five
        // families, which is what a dashboard has tabs for.
        for action in EVERY {
            assert!(
                ["account", "team", "token", "user", "factor"].contains(&action.log_type()),
                "{} has a log type nobody groups by",
                action.name()
            );
        }
    }

    #[test]
    fn an_actor_id_that_is_not_an_id_becomes_nobody() {
        assert_eq!(
            uuid_or_nil("0f8fad5b-d9cb-469f-a165-70867728950e"),
            "0f8fad5b-d9cb-469f-a165-70867728950e"
        );
        assert_eq!(uuid_or_nil(""), NOBODY);
        assert_eq!(uuid_or_nil("not-an-id"), NOBODY);
    }

    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> String + use<> {
        let pairs: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name: &str| {
            pairs
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
                .unwrap_or_default()
        }
    }

    #[test]
    fn an_unconfigured_project_writes_every_row_and_keeps_it() {
        let settings = configured(&env(&[])).expect("the empty environment is a valid one");
        assert_eq!(settings, Settings::default());
        assert!(!settings.disable_postgres);
        assert_eq!(settings.retention, None);
    }

    #[test]
    fn the_retention_is_a_duration_written_the_way_go_writes_one() {
        let month = configured(&env(&[("ZOU_AUDIT_LOG_RETENTION", "720h")]))
            .expect("720h is a duration")
            .retention;
        assert_eq!(month, Some(Duration::from_secs(720 * 60 * 60)));
        let mixed = configured(&env(&[("ZOU_AUDIT_LOG_RETENTION", "24h30m")]))
            .expect("24h30m is a duration")
            .retention;
        assert_eq!(mixed, Some(Duration::from_secs(24 * 3600 + 30 * 60)));
    }

    #[test]
    fn a_retention_the_pruner_could_not_honour_is_refused_at_startup() {
        // Rather than accepted and then rounded up to the sweep, which
        // would be a server keeping rows for an hour while its operator
        // believes it is keeping them for a minute.
        let too_short = configured(&env(&[("ZOU_AUDIT_LOG_RETENTION", "1m")]));
        assert!(
            too_short.is_err_and(|e| e.contains("shorter than the hour")),
            "a minute is shorter than a sweep"
        );
        let nonsense = configured(&env(&[("ZOU_AUDIT_LOG_RETENTION", "a fortnight")]));
        assert!(
            nonsense.is_err_and(|e| e.contains("not a duration")),
            "a fortnight is not a duration"
        );
    }

    #[test]
    fn the_postgres_switch_takes_upstreams_spellings_and_refuses_the_rest() {
        for on in ["true", "1"] {
            let settings = configured(&env(&[("ZOU_AUDIT_LOG_DISABLE_POSTGRES", on)]))
                .expect("a spelling of true");
            assert!(settings.disable_postgres, "{on}");
        }
        for off in ["", "false", "0"] {
            let settings = configured(&env(&[("ZOU_AUDIT_LOG_DISABLE_POSTGRES", off)]))
                .expect("a spelling of false");
            assert!(!settings.disable_postgres, "{off:?}");
        }
        // Not silently false, which is a project that thinks it turned
        // the table off and is still writing to it.
        assert!(configured(&env(&[("ZOU_AUDIT_LOG_DISABLE_POSTGRES", "yes")])).is_err());
    }
}
