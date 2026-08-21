//! What the email flows actually send, and where it goes when nobody
//! has configured a mail server.
//!
//! Every one of the auth email flows draws a six digit code, writes
//! down the hash of the address and the code together, and then has to
//! get the code to the person whose address it is. This is that last
//! step. Until it existed the codes went nowhere, which made the flows
//! look finished and left them unusable.
//!
//! The templates are GoTrue's, defaults and variable names both, because
//! a project migrating across brings its own templates with it and they
//! are written against `{{ .ConfirmationURL }}` and its neighbours. The
//! subjects are GoTrue's too, including the reauthentication one, which
//! puts the code in the subject line so a phone shows it on the lock
//! screen.
//!
//! Sending is deliberately behind a trait. There is one implementation
//! here, the dev inbox, which keeps what it was given in memory and logs
//! the link. That is the whole of the local loop: no container, no
//! second port, no mail server. An SMTP transport slots in beside it,
//! and so does an SMS sender later, which is why the trait talks about
//! delivering a message rather than about mail servers.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

/// The template names GoTrue uses, which are also the keys an operator
/// overrides a subject or a body under.
pub const INVITE: &str = "invite";
pub const CONFIRMATION: &str = "confirmation";
pub const RECOVERY: &str = "recovery";
pub const MAGIC_LINK: &str = "magic_link";
pub const EMAIL_CHANGE: &str = "email_change";
pub const REAUTHENTICATION: &str = "reauthentication";

/// Where the link in each kind of email points. GoTrue lets these be
/// configured one by one and defaults them all to `/verify`, which is
/// right for a server that answers at the root. This one answers under
/// `/auth/v1`, the same place the hosted stack puts it once Kong has
/// stripped nothing, so that is the default here.
#[derive(Clone, Debug)]
pub struct UrlPaths {
    pub invite: String,
    pub confirmation: String,
    pub recovery: String,
    pub email_change: String,
}

impl Default for UrlPaths {
    fn default() -> UrlPaths {
        UrlPaths {
            invite: VERIFY.to_string(),
            confirmation: VERIFY.to_string(),
            recovery: VERIFY.to_string(),
            email_change: VERIFY.to_string(),
        }
    }
}

const VERIFY: &str = "/auth/v1/verify";

/// Everything about mail that an operator can set.
#[derive(Clone, Debug)]
pub struct Settings {
    pub paths: UrlPaths,
    /// Subject overrides, keyed by template name. Anything absent takes
    /// the default below.
    pub subjects: BTreeMap<String, String>,
    /// Body overrides, keyed by template name.
    pub bodies: BTreeMap<String, String>,
    /// GoTrue's `GOTRUE_SMTP_MAX_FREQUENCY`, one minute there and here.
    /// How long after sending one code the same account has to wait
    /// before it can ask for another.
    pub max_frequency: u64,
}

impl Default for Settings {
    fn default() -> Settings {
        Settings {
            paths: UrlPaths::default(),
            subjects: BTreeMap::new(),
            bodies: BTreeMap::new(),
            max_frequency: 60,
        }
    }
}

impl Settings {
    /// The subject for a template, an operator's if they set one.
    pub fn subject(&self, template: &str) -> &str {
        match self.subjects.get(template) {
            Some(s) => s,
            None => default_subject(template),
        }
    }

    /// The body for a template, an operator's if they set one.
    pub fn body(&self, template: &str) -> &str {
        match self.bodies.get(template) {
            Some(s) => s,
            None => default_body(template),
        }
    }

    /// Which path the link in this kind of mail points at.
    pub fn path(&self, template: &str) -> &str {
        match template {
            INVITE => &self.paths.invite,
            RECOVERY | MAGIC_LINK => &self.paths.recovery,
            EMAIL_CHANGE => &self.paths.email_change,
            _ => &self.paths.confirmation,
        }
    }
}

/// One message, rendered and ready to leave.
#[derive(Clone, Debug)]
pub struct Mail {
    pub to: String,
    pub subject: String,
    pub body: String,
    /// The template it came from, which is what the dev inbox groups by
    /// and what a transport puts in its message type header.
    pub template: String,
    /// When it was handed over, unix seconds.
    pub at: i64,
}

impl Mail {
    /// The first link in the body, which is the one thing a person
    /// reading a dev inbox actually wants. Href values are html
    /// escaped on the way in, the way a template engine escapes them,
    /// so they are unescaped on the way back out: this is what a mail
    /// client would hand the browser.
    pub fn link(&self) -> Option<String> {
        let start = self.body.find("href=\"")? + 6;
        let rest = &self.body[start..];
        let end = rest.find('"')?;
        Some(unescape(&rest[..end]))
    }

    pub fn as_json(&self) -> serde_json::Value {
        serde_json::json!({
            "to": self.to,
            "subject": self.subject,
            "body": self.body,
            "template": self.template,
            "at": self.at,
            "link": self.link(),
        })
    }
}

/// Where rendered mail goes. Delivering is blocking network work for
/// anything but the dev inbox, so it is called from the blocking pool
/// and the trait is free to sit there and wait on a socket.
pub trait Sender: Send + Sync {
    fn deliver(&self, mail: &Mail) -> Result<(), String>;

    /// The dev inbox is the only sender that keeps anything, and the
    /// only one the inbox endpoint has anything to read out of.
    fn inbox(&self) -> Option<&Inbox> {
        None
    }

    /// What to call this in a log line.
    fn describe(&self) -> String;
}

/// Hand a message to the sender without blocking the request thread.
pub async fn post(sender: &Arc<dyn Sender>, mail: Mail) -> Result<(), String> {
    let sender = Arc::clone(sender);
    match tokio::task::spawn_blocking(move || sender.deliver(&mail)).await {
        Ok(out) => out,
        Err(e) => Err(format!("the mail task died: {e}")),
    }
}

/// The dev inbox: mail nobody sends anywhere, kept in memory and
/// written to the log.
///
/// This is what a project with no mail server configured gets, and it
/// is the point of the local loop. GoTrue in that position uses a noop
/// client and the code is simply lost, so the Supabase CLI runs a
/// separate mail catcher in a container next to it. Keeping the last
/// few messages in the server that made them costs a mutex and removes
/// the container.
pub struct Inbox {
    kept: Mutex<VecDeque<Mail>>,
    limit: usize,
}

impl Inbox {
    pub fn new(limit: usize) -> Inbox {
        Inbox {
            kept: Mutex::new(VecDeque::new()),
            limit,
        }
    }

    /// Everything it still holds, oldest first.
    pub fn kept(&self) -> Vec<Mail> {
        match self.kept.lock() {
            Ok(kept) => kept.iter().cloned().collect(),
            Err(poisoned) => poisoned.into_inner().iter().cloned().collect(),
        }
    }

    pub fn clear(&self) {
        if let Ok(mut kept) = self.kept.lock() {
            kept.clear();
        }
    }
}

impl Default for Inbox {
    fn default() -> Inbox {
        Inbox::new(100)
    }
}

impl Sender for Inbox {
    fn deliver(&self, mail: &Mail) -> Result<(), String> {
        // The link on its own line, because the whole reason a person
        // reads this log is to click it.
        match mail.link() {
            Some(link) => log::info!(
                "mail to {} kept in the dev inbox: {} <{}>",
                mail.to,
                mail.subject,
                link
            ),
            None => log::info!(
                "mail to {} kept in the dev inbox: {}",
                mail.to,
                mail.subject
            ),
        }
        let mut kept = match self.kept.lock() {
            Ok(kept) => kept,
            Err(poisoned) => poisoned.into_inner(),
        };
        if kept.len() == self.limit {
            kept.pop_front();
        }
        kept.push_back(mail.clone());
        Ok(())
    }

    fn inbox(&self) -> Option<&Inbox> {
        Some(self)
    }

    fn describe(&self) -> String {
        "the dev inbox".to_string()
    }
}

/// What a template can refer to. These are GoTrue's names, and a
/// project's own templates are written against them.
#[derive(Default)]
pub struct Vars {
    pub site_url: String,
    pub confirmation_url: String,
    pub email: String,
    pub new_email: String,
    pub sending_to: String,
    pub token: String,
    pub token_hash: String,
    pub redirect_to: String,
    pub data: serde_json::Value,
}

impl Vars {
    fn lookup(&self, name: &str) -> Option<String> {
        let out = match name {
            "SiteURL" => &self.site_url,
            "ConfirmationURL" => &self.confirmation_url,
            "Email" => &self.email,
            "NewEmail" => &self.new_email,
            "SendingTo" => &self.sending_to,
            "Token" => &self.token,
            "TokenHash" => &self.token_hash,
            "RedirectTo" => &self.redirect_to,
            other => {
                let key = other.strip_prefix("Data.")?;
                return Some(scalar(self.data.get(key)));
            }
        };
        Some(out.clone())
    }
}

/// A json value as a template would print it: a string without its
/// quotes, anything else as itself, a missing key as nothing.
fn scalar(value: Option<&serde_json::Value>) -> String {
    match value {
        None | Some(serde_json::Value::Null) => String::new(),
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
    }
}

/// Render one template. This understands the shape of Go template that
/// GoTrue's own defaults and every Supabase template in the wild use:
/// a field of the dot, optionally under Data, and nothing else.
///
/// Anything it does not understand is left exactly as it found it,
/// which is the honest answer for a template written against a feature
/// that is not here: a conditional that silently rendered nothing would
/// be a confirmation link quietly missing from an email.
pub fn render(template: &str, vars: &Vars) -> String {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(open) = rest.find("{{") {
        out.push_str(&rest[..open]);
        let after = &rest[open + 2..];
        let Some(close) = after.find("}}") else {
            // An unclosed action is not an action.
            out.push_str(&rest[open..]);
            return out;
        };
        let name = after[..close].trim();
        match name.strip_prefix('.').and_then(|n| vars.lookup(n)) {
            Some(value) => out.push_str(&escape(&value)),
            None => out.push_str(&rest[open..open + 2 + close + 2]),
        }
        rest = &after[close + 2..];
    }
    out.push_str(rest);
    out
}

/// What Go's html/template does to a value before it goes in the page,
/// which matters here because an address is user supplied and the body
/// is html.
fn escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&#34;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

fn unescape(value: &str) -> String {
    value
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&#34;", "\"")
        .replace("&#39;", "'")
        .replace("&amp;", "&")
}

/// The link that carries the token hash back to this server.
///
/// The hash goes in as `token`, which is the name the older templates
/// use and the one `/verify` still reads, and the type is what tells
/// verify which column to look in.
pub fn action_link(
    external: &str,
    path: &str,
    kind: &str,
    token_hash: &str,
    redirect_to: &str,
) -> String {
    format!(
        "{}{}?token={}&type={}&redirect_to={}",
        external.trim_end_matches('/'),
        path,
        crate::auth::query_escape(token_hash),
        crate::auth::query_escape(kind),
        encode_redirect(redirect_to),
    )
}

/// GoTrue escapes the redirect only when it looks unescaped, so a
/// caller that already escaped theirs does not get it escaped twice.
fn encode_redirect(redirect_to: &str) -> String {
    if redirect_to.contains(['&', '=', '#']) {
        crate::auth::query_escape(redirect_to)
    } else {
        redirect_to.to_string()
    }
}

/// Put a message together: subject and body from the template, both
/// rendered, because the reauthentication subject carries the code.
pub fn compose(settings: &Settings, template: &str, to: &str, vars: &Vars) -> Mail {
    Mail {
        to: to.to_string(),
        subject: render(settings.subject(template), vars),
        body: render(settings.body(template), vars),
        template: template.to_string(),
        at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0),
    }
}

/// GoTrue's default subjects, word for word.
fn default_subject(template: &str) -> &'static str {
    match template {
        INVITE => "You've been invited",
        RECOVERY => "Reset your password",
        MAGIC_LINK => "Your sign-in link",
        EMAIL_CHANGE => "Confirm your new email address",
        REAUTHENTICATION => "{{ .Token }} is your verification code",
        _ => "Confirm your email address",
    }
}

/// GoTrue's default bodies, word for word. A project that has never
/// written a template gets the same email from zou that it gets from
/// GoTrue, which is the point.
fn default_body(template: &str) -> &'static str {
    match template {
        INVITE => {
            "<h2>You've been invited</h2>\n\n\
             <p>You've been invited to create an account. Follow the link below to accept.</p>\n\
             <p><a href=\"{{ .ConfirmationURL }}\">Accept invitation</a></p>"
        }
        RECOVERY => {
            "<h2>Reset your password</h2>\n\n\
             <p>We received a request to reset your password. Follow the link below to choose a new one.</p>\n\
             <p><a href=\"{{ .ConfirmationURL }}\">Reset password</a></p>\n\
             <p>If you didn't request this, you can safely ignore this email.</p>"
        }
        MAGIC_LINK => {
            "<h2>Your sign-in link</h2>\n\n\
             <p>Follow the link below to sign in. This link expires shortly and can only be used once.</p>\n\
             <p><a href=\"{{ .ConfirmationURL }}\">Sign in</a></p>"
        }
        EMAIL_CHANGE => {
            "<h2>Confirm your new email address</h2>\n\n\
             <p>Follow the link below to confirm {{ .NewEmail }} as your new email address.</p>\n\
             <p><a href=\"{{ .ConfirmationURL }}\">Confirm new email address</a></p>\n\
             <p>If you didn't request this change, you can safely ignore this email.</p>"
        }
        REAUTHENTICATION => {
            "<h2>Your verification code</h2>\n\n\
             <p>Use the code below to verify your identity. It expires shortly.</p>\n\
             <p>{{ .Token }}</p>"
        }
        _ => {
            "<h2>Confirm your email address</h2>\n\n\
             <p>Follow the link below to confirm this email address and finish signing up.</p>\n\
             <p><a href=\"{{ .ConfirmationURL }}\">Confirm email address</a></p>\n"
        }
    }
}

/// The six template names in the order GoTrue lists them, paired with
/// the tail of the env var that names each one.
const NAMED: [(&str, &str); 6] = [
    (INVITE, "INVITE"),
    (CONFIRMATION, "CONFIRMATION"),
    (RECOVERY, "RECOVERY"),
    (MAGIC_LINK, "MAGIC_LINK"),
    (EMAIL_CHANGE, "EMAIL_CHANGE"),
    (REAUTHENTICATION, "REAUTHENTICATION"),
];

/// The mail settings from the environment, GoTrue's `GOTRUE_MAILER_*`
/// and `GOTRUE_SMTP_MAX_FREQUENCY` with the prefix swapped.
pub fn settings_from_env() -> Result<Settings, String> {
    settings(&|name| std::env::var(name).unwrap_or_default(), &read)
}

/// The same over anything that can look a name up, and anything that
/// can fetch a template. The second one is split out because upstream's
/// template settings name a location rather than holding a template,
/// so reading this configuration is what does the io.
pub fn settings(
    var: &dyn Fn(&str) -> String,
    fetch: &dyn Fn(&str) -> Result<String, String>,
) -> Result<Settings, String> {
    let stock = Settings::default();
    let mut subjects = BTreeMap::new();
    let mut bodies = BTreeMap::new();
    for (template, tail) in NAMED {
        let subject = var(&format!("ZOU_MAILER_SUBJECTS_{tail}"));
        if !subject.trim().is_empty() {
            subjects.insert(template.to_string(), subject.trim().to_string());
        }
        let at = var(&format!("ZOU_MAILER_TEMPLATES_{tail}"));
        if !at.trim().is_empty() {
            bodies.insert(template.to_string(), fetch(at.trim())?);
        }
    }
    Ok(Settings {
        paths: UrlPaths {
            invite: path(var, "INVITE", &stock.paths.invite),
            confirmation: path(var, "CONFIRMATION", &stock.paths.confirmation),
            recovery: path(var, "RECOVERY", &stock.paths.recovery),
            email_change: path(var, "EMAIL_CHANGE", &stock.paths.email_change),
        },
        subjects,
        bodies,
        max_frequency: crate::limit::seconds(var, "ZOU_SMTP_MAX_FREQUENCY", stock.max_frequency)?,
    })
}

fn path(var: &dyn Fn(&str) -> String, tail: &str, stock: &str) -> String {
    match var(&format!("ZOU_MAILER_URLPATHS_{tail}")).trim() {
        "" => stock.to_string(),
        set => set.to_string(),
    }
}

/// A template from wherever the setting pointed. Upstream's value is a
/// url it fetches, so an http one is fetched here too, once, at
/// startup rather than per message. Anything else is a path on disk,
/// which is what a project running zou on its own box has.
///
/// A template that cannot be read is an error and not a warning. The
/// alternative is sending GoTrue's stock email to somebody who wrote
/// their own and never hearing about it.
pub fn read(at: &str) -> Result<String, String> {
    if at.starts_with("http://") || at.starts_with("https://") {
        use std::io::Read;
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .timeout_global(Some(std::time::Duration::from_secs(10)))
            .build()
            .into();
        let mut res = agent
            .get(at)
            .header("user-agent", "zou")
            .call()
            .map_err(|e| format!("fetching the mail template at {at}: {e}"))?;
        let mut body = String::new();
        res.body_mut()
            .as_reader()
            .take(1 << 20)
            .read_to_string(&mut body)
            .map_err(|e| format!("reading the mail template at {at}: {e}"))?;
        return Ok(body);
    }
    std::fs::read_to_string(at.strip_prefix("file://").unwrap_or(at))
        .map_err(|e| format!("reading the mail template at {at}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars() -> Vars {
        Vars {
            site_url: "https://app.zou.test".to_string(),
            confirmation_url: "https://zou.test/auth/v1/verify?token=abc&type=signup".to_string(),
            email: "someone@zou.test".to_string(),
            token: "123456".to_string(),
            data: serde_json::json!({"nickname": "tester", "age": 7}),
            ..Vars::default()
        }
    }

    #[test]
    fn a_field_of_the_dot_is_substituted_however_it_is_spaced() {
        let out = render("{{ .Token }}/{{.Token}}/{{   .Token   }}", &vars());
        assert_eq!(out, "123456/123456/123456");
    }

    #[test]
    fn metadata_is_reachable_and_a_missing_key_is_nothing() {
        assert_eq!(render("{{ .Data.nickname }}", &vars()), "tester");
        assert_eq!(render("{{ .Data.age }}", &vars()), "7");
        assert_eq!(render("[{{ .Data.nothing }}]", &vars()), "[]");
    }

    #[test]
    fn the_link_is_escaped_the_way_a_page_needs_it_and_reads_back_whole() {
        let mail = compose(
            &Settings::default(),
            CONFIRMATION,
            "someone@zou.test",
            &vars(),
        );
        assert!(
            mail.body.contains("token=abc&amp;type=signup"),
            "the ampersand belongs to the html, not to the url: {}",
            mail.body
        );
        assert_eq!(
            mail.link().as_deref(),
            Some("https://zou.test/auth/v1/verify?token=abc&type=signup"),
            "and a mail client hands the browser the url that went in"
        );
    }

    #[test]
    fn an_address_cannot_close_the_tag_it_sits_in() {
        let mut vars = vars();
        vars.email = "<script>alert(1)</script>@zou.test".to_string();
        let out = render("<p>{{ .Email }}</p>", &vars);
        assert_eq!(
            out, "<p>&lt;script&gt;alert(1)&lt;/script&gt;@zou.test</p>",
            "an address is user supplied and the body is html"
        );
    }

    #[test]
    fn something_this_does_not_understand_is_left_alone() {
        // A quietly dropped conditional is a link quietly missing from
        // an email, so anything unrecognised stays visible.
        let out = render("{{ if .Token }}x{{ end }}{{ .Token }}", &vars());
        assert_eq!(out, "{{ if .Token }}x{{ end }}123456");
        assert_eq!(render("{{ .Token", &vars()), "{{ .Token");
    }

    #[test]
    fn the_reauthentication_subject_carries_the_code() {
        let mail = compose(
            &Settings::default(),
            REAUTHENTICATION,
            "someone@zou.test",
            &vars(),
        );
        assert_eq!(mail.subject, "123456 is your verification code");
        assert!(mail.body.contains("123456"));
        assert_eq!(mail.link(), None, "there is no link in this one");
    }

    #[test]
    fn an_operator_can_replace_a_subject_or_a_body() {
        let mut settings = Settings::default();
        settings.subjects.insert(
            CONFIRMATION.to_string(),
            "Welcome to {{ .Data.nickname }}".to_string(),
        );
        settings.bodies.insert(
            CONFIRMATION.to_string(),
            "<a href=\"{{ .ConfirmationURL }}\">go</a>".to_string(),
        );
        let mail = compose(&settings, CONFIRMATION, "someone@zou.test", &vars());
        assert_eq!(mail.subject, "Welcome to tester");
        assert_eq!(
            mail.body,
            "<a href=\"https://zou.test/auth/v1/verify?token=abc&amp;type=signup\">go</a>"
        );
    }

    #[test]
    fn the_link_points_where_verify_answers() {
        let settings = Settings::default();
        let link = action_link(
            "https://zou.test/",
            settings.path(RECOVERY),
            "magiclink",
            "deadbeef",
            "https://app.zou.test/welcome",
        );
        // The redirect goes in as it arrived, because it has no &, =
        // or # in it to be confused by. That is GoTrue's rule and it is
        // what the templates in the wild show.
        assert_eq!(
            link,
            "https://zou.test/auth/v1/verify?token=deadbeef&type=magiclink&redirect_to=https://app.zou.test/welcome"
        );
    }

    #[test]
    fn a_redirect_that_is_already_escaped_is_not_escaped_twice() {
        // GoTrue's rule, and its reason: a caller who escaped theirs
        // sent something with no & = or # left in it.
        let once = action_link(
            "https://zou.test",
            "/auth/v1/verify",
            "signup",
            "t",
            "https%3A%2F%2Fapp.zou.test",
        );
        assert!(
            once.ends_with("redirect_to=https%3A%2F%2Fapp.zou.test"),
            "{once}"
        );
    }

    #[test]
    fn the_inbox_keeps_the_last_few_and_drops_the_rest() {
        let inbox = Inbox::new(2);
        for n in 0..3 {
            let mail = Mail {
                to: format!("{n}@zou.test"),
                subject: "hello".to_string(),
                body: String::new(),
                template: CONFIRMATION.to_string(),
                at: 0,
            };
            inbox.deliver(&mail).expect("the dev inbox never refuses");
        }
        let kept = inbox.kept();
        assert_eq!(kept.len(), 2);
        assert_eq!(kept[0].to, "1@zou.test", "the oldest went first");
        assert_eq!(kept[1].to, "2@zou.test");
        inbox.clear();
        assert!(inbox.kept().is_empty());
    }

    #[test]
    fn the_settings_are_gotrues_with_the_prefix_swapped() {
        let env = |pairs: &[(&str, &str)]| {
            let pairs: Vec<(String, String)> = pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            settings(
                &move |name| {
                    pairs
                        .iter()
                        .find(|(k, _)| k == name)
                        .map(|(_, v)| v.clone())
                        .unwrap_or_default()
                },
                // Whatever the setting pointed at, said back, so what
                // is under test here is which name was looked up and
                // not what a file server had that day.
                &|at| Ok(format!("<p>{at}</p>")),
            )
        };
        let stock = env(&[]).expect("nothing set is not an error");
        assert_eq!(stock.max_frequency, 60);
        assert_eq!(stock.subject(RECOVERY), "Reset your password");
        assert_eq!(stock.path(INVITE), "/auth/v1/verify");
        assert!(
            stock
                .body(CONFIRMATION)
                .contains("Confirm your email address")
        );

        let set = env(&[
            ("ZOU_MAILER_SUBJECTS_RECOVERY", "Reset it"),
            ("ZOU_MAILER_TEMPLATES_RECOVERY", "/etc/zou/recovery.html"),
            ("ZOU_MAILER_URLPATHS_RECOVERY", "/reset"),
            ("ZOU_SMTP_MAX_FREQUENCY", "5s"),
        ])
        .expect("all of it is readable");
        assert_eq!(set.subject(RECOVERY), "Reset it");
        assert_eq!(set.body(RECOVERY), "<p>/etc/zou/recovery.html</p>");
        assert_eq!(set.path(RECOVERY), "/reset");
        assert_eq!(set.max_frequency, 5);
        // A magic link takes the recovery path upstream and here, and
        // the one that was not named keeps the default.
        assert_eq!(set.path(MAGIC_LINK), "/reset");
        assert_eq!(set.path(CONFIRMATION), "/auth/v1/verify");
        assert_eq!(set.subject(INVITE), "You've been invited");

        assert!(env(&[("ZOU_SMTP_MAX_FREQUENCY", "often")]).is_err());
    }

    #[test]
    fn a_template_that_cannot_be_read_stops_the_start() {
        let out = settings(
            &|name| match name {
                "ZOU_MAILER_TEMPLATES_INVITE" => "/no/such/template.html".to_string(),
                _ => String::new(),
            },
            &read,
        );
        let Err(e) = out else {
            panic!("a template nobody can read is not a working configuration");
        };
        assert!(
            e.starts_with("reading the mail template at /no/such/template.html"),
            "{e}"
        );
    }
}
