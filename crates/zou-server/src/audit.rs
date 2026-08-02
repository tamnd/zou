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
//! Three upstream details are worth knowing before reading any of this,
//! because all three look like bugs and all three are load bearing for
//! anybody who has queried this table:
//!
//! - Almost every entry has an empty `ip_address` column. Only the
//!   factor events fill it. The address is in the request either way, so
//!   this is upstream forgetting to pass it rather than deciding not to,
//!   but a query that counts distinct addresses would start seeing
//!   different numbers if this end filled them all in.
//! - An admin acting on somebody else's account is not a person. The
//!   actor is a synthetic user whose id is the nil uuid and whose
//!   username is the role name, so every service_role action in the
//!   trail is attributed to `service_role` rather than to whoever holds
//!   the key.
//! - An anonymous sign in writes nothing at all. It is the one grant
//!   with no entry of its own.

use crate::sql;

/// What happened. Upstream's AuditAction, minus the actions for
/// endpoints this server does not serve yet: the passkey three, the two
/// recovery code ones, and `mfa_code_login`, which upstream defines and
/// never writes.
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
    IdentityUnlinked,
}

impl Action {
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
            // Upstream files the unlink under the user rather than under
            // the account, which is the odd one in the table.
            Action::IdentityUnlinked => "user",
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
insert into auth.audit_log_entries (instance_id, id, payload, created_at, ip_address)
select '00000000-0000-0000-0000-000000000000', gen_random_uuid(),
       jsonb_build_object(
           'actor_id', $1::text,
           'actor_via_sso', coalesce(u.is_sso_user, false),
           'actor_username', coalesce(nullif(u.phone, ''), nullif(u.email, ''), ''),
           'action', $2::text,
           'log_type', $3::text)
       || case when u.raw_user_meta_data ? 'full_name'
               then jsonb_build_object('actor_name', u.raw_user_meta_data -> 'full_name')
               else '{}'::jsonb end
       || case when $4::jsonb is null then '{}'::jsonb
               else jsonb_build_object('traits', $4::jsonb) end,
       clock_timestamp(), $5::text
  from (select 1) as one
  left join auth.users u on u.id = $1::text::uuid";

/// An entry a role wrote. A role has no row to read, no metadata and no
/// SSO to speak of, and its id is nobody's: upstream builds a synthetic
/// user out of the claim rather than looking one up, so this is that
/// user rather than a lookup that failed.
const BY_ROLE: &str = "
insert into auth.audit_log_entries (instance_id, id, payload, created_at, ip_address)
values ('00000000-0000-0000-0000-000000000000', gen_random_uuid(),
        jsonb_build_object(
            'actor_id', '00000000-0000-0000-0000-000000000000'::text,
            'actor_via_sso', false,
            'actor_username', $1::text,
            'action', $2::text,
            'log_type', $3::text)
        || case when $4::jsonb is null then '{}'::jsonb
                else jsonb_build_object('traits', $4::jsonb) end,
        clock_timestamp(), $5::text)";

/// One entry.
pub async fn record(
    sess: &sql::Session,
    actor: Actor<'_>,
    action: Action,
    ip: &str,
    traits: Option<serde_json::Value>,
) -> Result<(), sql::Error> {
    let (sql, who) = match actor {
        Actor::Account(id) => (BY_ACCOUNT, uuid_or_nil(id)),
        Actor::Role(role) => (BY_ROLE, role.to_string()),
    };
    let name = action.name();
    let log_type = action.log_type();
    sess.execute(sql, &[&who, &name, &log_type, &traits, &ip])
        .await?;
    Ok(())
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
        assert_eq!(Action::IdentityUnlinked.log_type(), "user");
        assert_eq!(Action::ChallengeCreated.log_type(), "factor");
        assert_eq!(Action::FactorUnenrolled.log_type(), "factor");
    }

    #[test]
    fn no_two_actions_share_a_name() {
        let all = [
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
            Action::IdentityUnlinked,
        ];
        let mut names: Vec<&str> = all.iter().map(|a| a.name()).collect();
        names.sort_unstable();
        let mut unique = names.clone();
        unique.dedup();
        assert_eq!(names, unique);
        // And every one of them lands in one of upstream's six families.
        for action in all {
            assert!(
                [
                    "account",
                    "team",
                    "token",
                    "user",
                    "factor",
                    "recovery_codes"
                ]
                .contains(&action.log_type()),
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
}
