//! The external identity providers: where to send a person, and what
//! to ask the provider about them when they come back.
//!
//! This file is the provider half of the OAuth flow. The flow itself,
//! the flow state row, the account linking and the session, is in
//! `auth`. What lives here is everything that involves talking to
//! somebody else's server: the authorize url, the code exchange, and
//! reading a profile out of whatever shape the provider answers in.
//!
//! The calls go through a trait rather than straight at ureq, for the
//! same reason the mailer does: a test that cannot answer as Google is
//! a test that can only assert what the code passes to itself.

use std::collections::HashMap;
use std::io::Read;

/// One provider: where it lives and who we are to it. The environment
/// variable names are GoTrue's with GOTRUE_ swapped for ZOU_.
#[derive(Clone, Debug)]
pub struct Provider {
    pub name: String,
    pub client_id: String,
    pub secret: String,
    /// Where the provider sends the person back. Empty means the
    /// callback on this server, which is the usual case.
    pub redirect_uri: String,
    pub authorize_url: String,
    pub token_url: String,
    /// The profile document, or for github the first of two calls.
    pub user_url: String,
    pub scopes: Vec<String>,
}

impl Provider {
    /// The defaults for a provider this file knows, with the
    /// credentials filled in by the caller.
    pub fn named(name: &str) -> Option<Provider> {
        let (authorize_url, token_url, user_url, scopes) = match name {
            "google" => (
                "https://accounts.google.com/o/oauth2/v2/auth",
                "https://oauth2.googleapis.com/token",
                "https://www.googleapis.com/oauth2/v3/userinfo",
                vec!["email", "profile"],
            ),
            "github" => (
                "https://github.com/login/oauth/authorize",
                "https://github.com/login/oauth/access_token",
                "https://api.github.com/user",
                vec!["user:email"],
            ),
            _ => return None,
        };
        Some(Provider {
            name: name.to_string(),
            client_id: String::new(),
            secret: String::new(),
            redirect_uri: String::new(),
            authorize_url: authorize_url.to_string(),
            token_url: token_url.to_string(),
            user_url: user_url.to_string(),
            scopes: scopes.into_iter().map(str::to_string).collect(),
        })
    }

    /// Where to send the person, with our state along for the ride.
    /// `extra` is the scopes the caller asked for on top of ours,
    /// comma separated the way the supabase clients send them.
    pub fn authorize_url(&self, redirect_uri: &str, state: &str, extra: &str) -> String {
        let mut scopes = self.scopes.clone();
        scopes.extend(
            extra
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string),
        );
        let joined = scopes.join(" ");
        let mut url = format!(
            "{}?client_id={}&redirect_uri={}&response_type=code&scope={}&state={}",
            self.authorize_url,
            crate::auth::query_escape(&self.client_id),
            crate::auth::query_escape(redirect_uri),
            crate::auth::query_escape(&joined),
            crate::auth::query_escape(state),
        );
        if self.name == "google" {
            // Without this Google hands back an access token that
            // expires in an hour and no way to renew it, and the
            // provider_refresh_token a client is promised is empty.
            url.push_str("&access_type=offline");
        }
        url
    }
}

/// Everything configured, by name. Empty is the normal state for a
/// project that does not use social login.
#[derive(Default, Debug)]
pub struct Providers(HashMap<String, Provider>);

impl Providers {
    pub fn get(&self, name: &str) -> Option<&Provider> {
        self.0.get(&name.to_ascii_lowercase())
    }

    pub fn insert(&mut self, provider: Provider) {
        self.0.insert(provider.name.clone(), provider);
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The names configured, sorted, which is what /settings publishes.
    pub fn names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.0.keys().cloned().collect();
        names.sort();
        names
    }
}

/// Providers read out of the environment. A provider is configured
/// when it has both a client id and a secret, which is the same bar
/// GoTrue sets before it will offer one.
pub fn from_env() -> Result<Providers, String> {
    from_vars(|name| std::env::var(name).ok().filter(|v| !v.is_empty()))
}

pub fn from_vars(var: impl Fn(&str) -> Option<String>) -> Result<Providers, String> {
    let mut out = Providers::default();
    for name in ["google", "github"] {
        let upper = name.to_ascii_uppercase();
        let id = var(&format!("ZOU_EXTERNAL_{upper}_CLIENT_ID"));
        let secret = var(&format!("ZOU_EXTERNAL_{upper}_SECRET"));
        let (id, secret) = match (id, secret) {
            (Some(id), Some(secret)) => (id, secret),
            (None, None) => continue,
            // Half a credential is a typo, and a provider that is
            // offered and then fails at the token exchange is a worse
            // way to find out than a refusal at startup.
            _ => {
                return Err(format!(
                    "{name} needs both ZOU_EXTERNAL_{upper}_CLIENT_ID and ZOU_EXTERNAL_{upper}_SECRET"
                ));
            }
        };
        let mut provider = Provider::named(name).expect("a provider this file knows");
        provider.client_id = id;
        provider.secret = secret;
        if let Some(uri) = var(&format!("ZOU_EXTERNAL_{upper}_REDIRECT_URI")) {
            provider.redirect_uri = uri;
        }
        out.insert(provider);
    }
    Ok(out)
}

/// One request to a provider. A form makes it a POST, and everything
/// else is a GET.
pub struct Ask {
    pub url: String,
    pub form: Vec<(String, String)>,
    pub bearer: String,
}

pub struct Answer {
    pub status: u16,
    pub body: String,
}

/// The provider calls, behind a trait so a test can be Google.
pub trait Http: Send + Sync {
    fn call(&self, ask: &Ask) -> Result<Answer, String>;
}

/// The real one.
pub struct Web {
    agent: ureq::Agent,
}

impl Default for Web {
    fn default() -> Web {
        Web {
            // A provider answering 4xx is telling us something worth
            // reading, so statuses come back as answers rather than as
            // transport errors.
            agent: ureq::Agent::config_builder()
                .http_status_as_error(false)
                .timeout_global(Some(std::time::Duration::from_secs(10)))
                .build()
                .into(),
        }
    }
}

impl Http for Web {
    fn call(&self, ask: &Ask) -> Result<Answer, String> {
        let result = match ask.form.is_empty() {
            true => {
                let mut req = self
                    .agent
                    .get(&ask.url)
                    .header("accept", "application/json");
                if !ask.bearer.is_empty() {
                    req = req.header("authorization", format!("Bearer {}", ask.bearer));
                }
                // Github refuses a request with no user agent.
                req.header("user-agent", "zou").call()
            }
            false => {
                let form: Vec<(&str, &str)> = ask
                    .form
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_str()))
                    .collect();
                self.agent
                    .post(&ask.url)
                    .header("accept", "application/json")
                    .header("user-agent", "zou")
                    .send_form(form)
            }
        };
        let mut res = result.map_err(|e| format!("calling {}: {e}", ask.url))?;
        let status = res.status().as_u16();
        let mut body = String::new();
        res.body_mut()
            .as_reader()
            .take(1 << 20)
            .read_to_string(&mut body)
            .map_err(|e| format!("reading {}: {e}", ask.url))?;
        Ok(Answer { status, body })
    }
}

/// What the provider handed back for the code.
#[derive(Debug, Default)]
pub struct Tokens {
    pub access_token: String,
    pub refresh_token: String,
}

/// The code exchange. The credentials go in the form rather than in a
/// basic auth header, which is the half of RFC 6749 every provider
/// here accepts.
pub fn exchange(
    provider: &Provider,
    http: &dyn Http,
    code: &str,
    redirect_uri: &str,
) -> Result<Tokens, String> {
    let ask = Ask {
        url: provider.token_url.clone(),
        form: vec![
            ("grant_type".to_string(), "authorization_code".to_string()),
            ("code".to_string(), code.to_string()),
            ("client_id".to_string(), provider.client_id.clone()),
            ("client_secret".to_string(), provider.secret.clone()),
            ("redirect_uri".to_string(), redirect_uri.to_string()),
        ],
        bearer: String::new(),
    };
    let answer = http.call(&ask)?;
    let body: serde_json::Value = serde_json::from_str(&answer.body).map_err(|_| {
        format!(
            "{} answered {}: {}",
            provider.name,
            answer.status,
            snippet(&answer.body)
        )
    })?;
    // Github answers 200 with an error field rather than a status, so
    // the body is what decides, not the code.
    if let Some(error) = body["error"].as_str() {
        let described = body["error_description"].as_str().unwrap_or(error);
        return Err(format!("{} refused the code: {described}", provider.name));
    }
    match body["access_token"].as_str() {
        Some(access_token) => Ok(Tokens {
            access_token: access_token.to_string(),
            refresh_token: body["refresh_token"].as_str().unwrap_or("").to_string(),
        }),
        None => Err(format!(
            "{} answered {} with no access token",
            provider.name, answer.status
        )),
    }
}

/// Who the provider says this is.
#[derive(Debug)]
pub struct Person {
    /// The provider's own id for them, which is what the identity is
    /// keyed by and never the email address.
    pub sub: String,
    pub email: String,
    pub email_verified: bool,
    /// Everything the provider said, in the shape GoTrue stores in
    /// identities.identity_data.
    pub claims: serde_json::Value,
}

/// Read a profile. Github needs two calls because its user document
/// only carries a public email, which is usually null, so the address
/// comes from the emails endpoint where the verified flag lives too.
pub fn person(provider: &Provider, http: &dyn Http, token: &str) -> Result<Person, String> {
    let profile = fetch(http, &provider.user_url, token, &provider.name)?;
    match provider.name.as_str() {
        "github" => {
            let emails = fetch(
                http,
                &format!("{}/emails", provider.user_url),
                token,
                &provider.name,
            )?;
            Ok(from_github(provider, &profile, &emails))
        }
        _ => Ok(from_openid(provider, &profile)),
    }
}

fn fetch(
    http: &dyn Http,
    url: &str,
    token: &str,
    provider: &str,
) -> Result<serde_json::Value, String> {
    let answer = http.call(&Ask {
        url: url.to_string(),
        form: Vec::new(),
        bearer: token.to_string(),
    })?;
    if answer.status != 200 {
        return Err(format!(
            "{provider} answered {} for the profile: {}",
            answer.status,
            snippet(&answer.body)
        ));
    }
    serde_json::from_str(&answer.body)
        .map_err(|e| format!("{provider} answered with something that is not json: {e}"))
}

/// The OpenID Connect shape, which is Google and everything modelled
/// on it.
fn from_openid(provider: &Provider, profile: &serde_json::Value) -> Person {
    // The userinfo document calls it sub, the older Google endpoint
    // calls it id, and the same account has to land on the same
    // identity whichever one answered.
    let sub = text(profile, "sub");
    let sub = match sub.is_empty() {
        true => text(profile, "id"),
        false => sub,
    };
    let email = text(profile, "email");
    // Two spellings of the same claim, for the same reason.
    let verified = profile["email_verified"].as_bool().unwrap_or(false)
        || profile["verified_email"].as_bool().unwrap_or(false);
    let name = text(profile, "name");
    let picture = text(profile, "picture");
    Person {
        claims: serde_json::json!({
            "iss": provider.user_url,
            "sub": sub,
            "name": name,
            "email": email,
            "picture": picture,
            "full_name": name,
            "avatar_url": picture,
            "provider_id": sub,
            "email_verified": verified,
            "phone_verified": false,
        }),
        sub,
        email,
        email_verified: verified,
    }
}

/// Github, which answers its own shape and puts the address somewhere
/// else entirely.
fn from_github(
    provider: &Provider,
    profile: &serde_json::Value,
    emails: &serde_json::Value,
) -> Person {
    let sub = match profile["id"].as_i64() {
        Some(id) => id.to_string(),
        None => text(profile, "id"),
    };
    let login = text(profile, "login");
    let name = text(profile, "name");
    let avatar = text(profile, "avatar_url");
    // The primary address wins, and any other verified one is better
    // than nothing, which is the order GoTrue reads them in.
    let listed = emails.as_array().cloned().unwrap_or_default();
    let mut email = String::new();
    let mut verified = false;
    for entry in &listed {
        let address = text(entry, "email");
        if address.is_empty() {
            continue;
        }
        let primary = entry["primary"].as_bool().unwrap_or(false);
        if email.is_empty() || primary {
            email = address;
            verified = entry["verified"].as_bool().unwrap_or(false);
        }
        if primary {
            break;
        }
    }
    Person {
        claims: serde_json::json!({
            "iss": provider.user_url,
            "sub": sub,
            "name": name,
            "email": email,
            "preferred_username": login,
            "user_name": login,
            "full_name": name,
            "avatar_url": avatar,
            "provider_id": sub,
            "email_verified": verified,
            "phone_verified": false,
        }),
        sub,
        email,
        email_verified: verified,
    }
}

fn text(value: &serde_json::Value, key: &str) -> String {
    value[key].as_str().unwrap_or("").to_string()
}

/// Enough of a body to say what went wrong, and not enough to put a
/// provider's whole answer in a log line.
fn snippet(body: &str) -> String {
    body.chars().take(200).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// A provider that answers from a script, and writes down what it
    /// was asked.
    struct Fake {
        answers: Mutex<Vec<(String, u16, String)>>,
        asked: Mutex<Vec<String>>,
    }

    impl Fake {
        fn new(answers: &[(&str, u16, &str)]) -> Fake {
            Fake {
                answers: Mutex::new(
                    answers
                        .iter()
                        .map(|(u, s, b)| (u.to_string(), *s, b.to_string()))
                        .collect(),
                ),
                asked: Mutex::new(Vec::new()),
            }
        }
    }

    impl Http for Fake {
        fn call(&self, ask: &Ask) -> Result<Answer, String> {
            let form = ask
                .form
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join("&");
            self.asked
                .lock()
                .unwrap()
                .push(format!("{} {form} {}", ask.url, ask.bearer));
            let answers = self.answers.lock().unwrap();
            let found = answers
                .iter()
                .find(|(url, _, _)| *url == ask.url)
                .unwrap_or_else(|| panic!("nothing scripted for {}", ask.url));
            Ok(Answer {
                status: found.1,
                body: found.2.clone(),
            })
        }
    }

    fn google() -> Provider {
        let mut p = Provider::named("google").expect("google is known");
        p.client_id = "id.apps.googleusercontent.com".to_string();
        p.secret = "shh".to_string();
        p
    }

    fn github() -> Provider {
        let mut p = Provider::named("github").expect("github is known");
        p.client_id = "Iv1.deadbeef".to_string();
        p.secret = "shh".to_string();
        p
    }

    #[test]
    fn the_authorize_url_carries_the_state_and_the_scopes() {
        let url = google().authorize_url("https://zou.test/auth/v1/callback", "state-1", "");
        assert!(
            url.starts_with("https://accounts.google.com/o/oauth2/v2/auth?"),
            "{url}"
        );
        assert!(
            url.contains("&redirect_uri=https%3A%2F%2Fzou.test%2Fauth%2Fv1%2Fcallback"),
            "{url}"
        );
        assert!(url.contains("&scope=email+profile"), "{url}");
        assert!(url.contains("&state=state-1"), "{url}");
        assert!(url.contains("&response_type=code"), "{url}");
        assert!(
            url.contains("&access_type=offline"),
            "without it there is no refresh token to hand a client: {url}"
        );
        // What the caller asks for is added to what the provider needs,
        // never instead of it.
        let url = google().authorize_url("https://zou.test/cb", "s", "drive.readonly, calendar");
        assert!(
            url.contains("&scope=email+profile+drive.readonly+calendar"),
            "{url}"
        );
    }

    #[test]
    fn a_provider_needs_both_halves_of_its_credentials() {
        let complete = from_vars(|name| match name {
            "ZOU_EXTERNAL_GOOGLE_CLIENT_ID" => Some("id".to_string()),
            "ZOU_EXTERNAL_GOOGLE_SECRET" => Some("shh".to_string()),
            _ => None,
        })
        .expect("configured");
        assert_eq!(complete.names(), vec!["google"]);
        assert!(
            complete.get("GOOGLE").is_some(),
            "the name is not case sensitive"
        );

        let half = from_vars(|name| match name {
            "ZOU_EXTERNAL_GITHUB_CLIENT_ID" => Some("id".to_string()),
            _ => None,
        });
        assert!(
            half.expect_err("half a credential")
                .contains("ZOU_EXTERNAL_GITHUB_SECRET"),
            "a provider that fails at the token exchange is a worse way to find out"
        );

        assert!(from_vars(|_| None).expect("nothing configured").is_empty());
    }

    #[test]
    fn the_code_exchange_reads_the_token_out_of_the_answer() {
        let http = Fake::new(&[(
            "https://oauth2.googleapis.com/token",
            200,
            r#"{"access_token":"at-1","refresh_token":"rt-1","expires_in":3599}"#,
        )]);
        let tokens = exchange(&google(), &http, "code-1", "https://zou.test/cb").expect("tokens");
        assert_eq!(tokens.access_token, "at-1");
        assert_eq!(tokens.refresh_token, "rt-1");
        let asked = http.asked.lock().unwrap()[0].clone();
        assert!(asked.contains("grant_type=authorization_code"), "{asked}");
        assert!(asked.contains("code=code-1"), "{asked}");
        assert!(asked.contains("client_secret=shh"), "{asked}");
        assert!(
            asked.contains("redirect_uri=https://zou.test/cb"),
            "the provider checks it against the one it was registered with: {asked}"
        );
    }

    #[test]
    fn a_refusal_is_read_out_of_the_body_and_not_out_of_the_status() {
        // Github answers 200 with an error in the body, which is the
        // case that goes unnoticed if only the status is read.
        let http = Fake::new(&[(
            "https://github.com/login/oauth/access_token",
            200,
            r#"{"error":"bad_verification_code","error_description":"The code passed is incorrect or expired."}"#,
        )]);
        let refusal = exchange(&github(), &http, "stale", "https://zou.test/cb")
            .expect_err("no tokens out of a refusal");
        assert!(
            refusal.contains("The code passed is incorrect"),
            "{refusal}"
        );
    }

    #[test]
    fn google_answers_openid_claims() {
        let http = Fake::new(&[(
            "https://www.googleapis.com/oauth2/v3/userinfo",
            200,
            r#"{"sub":"106","email":"someone@gmail.com","email_verified":true,
                "name":"Some One","picture":"https://lh3.example/photo"}"#,
        )]);
        let person = person(&google(), &http, "at-1").expect("a profile");
        assert_eq!(person.sub, "106");
        assert_eq!(person.email, "someone@gmail.com");
        assert!(person.email_verified);
        assert_eq!(person.claims["full_name"], "Some One");
        assert_eq!(person.claims["avatar_url"], "https://lh3.example/photo");
        assert_eq!(
            person.claims["provider_id"], "106",
            "the identity is keyed by the provider's id, never by the address"
        );
        assert_eq!(person.claims["phone_verified"], false);
    }

    #[test]
    fn github_keeps_its_address_somewhere_else() {
        let http = Fake::new(&[
            (
                "https://api.github.com/user",
                200,
                r#"{"id":42,"login":"octocat","name":"Mona","avatar_url":"https://gh/av","email":null}"#,
            ),
            (
                "https://api.github.com/user/emails",
                200,
                r#"[{"email":"old@zou.test","primary":false,"verified":true},
                    {"email":"mona@zou.test","primary":true,"verified":true}]"#,
            ),
        ]);
        let person = person(&github(), &http, "at-1").expect("a profile");
        assert_eq!(
            person.sub, "42",
            "a number, as a string, the way GoTrue stores it"
        );
        assert_eq!(
            person.email, "mona@zou.test",
            "the primary address wins even when it is not the first one listed"
        );
        assert!(person.email_verified);
        assert_eq!(person.claims["user_name"], "octocat");
        assert_eq!(person.claims["preferred_username"], "octocat");
    }

    #[test]
    fn an_unverified_address_says_so_rather_than_being_dropped() {
        let http = Fake::new(&[
            (
                "https://api.github.com/user",
                200,
                r#"{"id":42,"login":"octocat"}"#,
            ),
            (
                "https://api.github.com/user/emails",
                200,
                r#"[{"email":"mona@zou.test","primary":true,"verified":false}]"#,
            ),
        ]);
        let person = person(&github(), &http, "at-1").expect("a profile");
        assert_eq!(person.email, "mona@zou.test");
        assert!(
            !person.email_verified,
            "the flow decides what to do about it, this only reports it"
        );
    }

    #[test]
    fn a_provider_nobody_configured_is_not_a_provider() {
        assert!(Provider::named("myspace").is_none());
        assert!(Providers::default().get("google").is_none());
    }
}
