//! What the phone flows send, and where it goes when nobody has
//! configured a provider.
//!
//! This is `mail.rs` for text messages, and it is deliberately the same
//! shape: a template with one variable in it, a trait that says how a
//! message leaves, and a dev sink that keeps the last few in memory and
//! logs them. A laptop can sign in with a phone number and no account
//! anywhere, which is the whole point: GoTrue with no provider set
//! refuses every phone request, so a person writing a phone sign in
//! screen has to buy a Twilio number before they can see their own
//! form work once.
//!
//! The providers are split in two on purpose. `request` builds the call
//! and `read` judges the answer, both pure, and `deliver` is the three
//! lines that put a socket between them. That is what makes the exact
//! form fields and the exact error strings testable without a network
//! and without a fake HTTP server.
//!
//! Four of GoTrue's five providers are here: Twilio, MessageBird,
//! Vonage and TextLocal. Twilio Verify is not, and it is not the same
//! shape as these: it draws and checks the code itself, which inverts
//! the flow rather than adding a provider to it.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

/// The two channels a message can go out on. GoTrue's names, which are
/// also what a client sends in `channel`.
pub const SMS: &str = "sms";
pub const WHATSAPP: &str = "whatsapp";

/// Everything about text messages an operator can set. The defaults are
/// GoTrue's, which are not the mail defaults: a code that arrives in
/// seconds is worth a minute, not a day.
#[derive(Clone, Debug)]
pub struct Settings {
    /// GOTRUE_SMS_TEMPLATE. One variable, `{{ .Code }}`.
    pub template: String,
    /// GOTRUE_SMS_OTP_LENGTH, six there and here, clamped to 6..=10 the
    /// way upstream clamps it.
    pub otp_length: usize,
    /// GOTRUE_SMS_OTP_EXP in seconds. A minute, because the code is on
    /// the screen of the phone that asked for it.
    pub otp_exp: i64,
    /// GOTRUE_SMS_MAX_FREQUENCY in seconds, how long an account waits
    /// before it may ask for another code.
    pub max_frequency: u64,
    /// GOTRUE_SMS_AUTOCONFIRM. True and a phone signup is confirmed
    /// where it stands, which is what a project without a provider
    /// wants and what the mail side calls mailer_autoconfirm.
    pub autoconfirm: bool,
}

impl Default for Settings {
    fn default() -> Settings {
        Settings {
            template: String::new(),
            otp_length: 6,
            otp_exp: 60,
            max_frequency: 60,
            autoconfirm: false,
        }
    }
}

impl Settings {
    /// The message for a code. An operator's template when they set one,
    /// GoTrue's when they did not.
    pub fn body(&self, code: &str) -> String {
        let template = match self.template.is_empty() {
            true => "Your code is {{ .Code }}",
            false => &self.template,
        };
        render(template, code)
    }

    /// How many digits a code has. Upstream refuses to go under six or
    /// over ten and quietly takes six instead, which is the right way
    /// round: a four digit code is a guessable one.
    pub fn digits(&self) -> usize {
        match self.otp_length {
            6..=10 => self.otp_length,
            _ => 6,
        }
    }
}

/// The one substitution an SMS template has. Anything else is left
/// exactly as it was found, for the same reason the mail templates
/// leave it: a dropped variable is a message with no code in it.
///
/// Nothing is escaped here. A text message is text, and Go reaches for
/// text/template rather than html/template on this path too.
pub fn render(template: &str, code: &str) -> String {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(open) = rest.find("{{") {
        out.push_str(&rest[..open]);
        let after = &rest[open + 2..];
        let Some(close) = after.find("}}") else {
            out.push_str(&rest[open..]);
            return out;
        };
        match after[..close].trim() {
            ".Code" => out.push_str(code),
            _ => out.push_str(&rest[open..open + 2 + close + 2]),
        }
        rest = &after[close + 2..];
    }
    out.push_str(rest);
    out
}

/// One message, rendered and ready to leave.
#[derive(Clone, Debug)]
pub struct Text {
    /// The number in E.164 without the plus, which is how every one of
    /// these APIs wants it and how the column holds it.
    pub to: String,
    pub body: String,
    /// The code on its own, because Twilio's WhatsApp templates take it
    /// as a variable rather than taking the rendered message.
    pub code: String,
    pub channel: String,
    /// When it was handed over, unix seconds.
    pub at: i64,
}

impl Text {
    pub fn as_json(&self) -> serde_json::Value {
        serde_json::json!({
            "to": self.to,
            "body": self.body,
            "code": self.code,
            "channel": self.channel,
            "at": self.at,
        })
    }
}

/// Where rendered messages go. The answer is the provider's own id for
/// the message, which the otp endpoint hands back to the client, and
/// which is empty for a sender that has no such thing.
pub trait Sender: Send + Sync {
    fn deliver(&self, text: &Text) -> Result<String, String>;

    /// Whether this sender can carry a channel at all. Only Twilio does
    /// WhatsApp, which is upstream's rule and is why the refusal names
    /// the provider rather than the channel.
    fn carries(&self, channel: &str) -> bool {
        channel == SMS
    }

    /// The dev sink is the only sender that keeps anything.
    fn sink(&self) -> Option<&Sink> {
        None
    }

    /// What to call this in a log line.
    fn describe(&self) -> String;

    /// GOTRUE_SMS_PROVIDER, the name a client reads back from
    /// /settings to know how the codes travel. Empty for the dev sink,
    /// because a project with nothing configured has no provider and
    /// upstream leaves the setting empty rather than inventing one.
    fn provider(&self) -> &'static str {
        ""
    }
}

/// Hand a message to the sender without blocking the request thread.
pub async fn post(sender: &Arc<dyn Sender>, text: Text) -> Result<String, String> {
    let sender = Arc::clone(sender);
    match tokio::task::spawn_blocking(move || sender.deliver(&text)).await {
        Ok(out) => out,
        Err(e) => Err(format!("the sms task died: {e}")),
    }
}

/// The dev sink: messages nobody sends anywhere, kept in memory and
/// written to the log.
///
/// The code goes in the log line on purpose. That is the only way a
/// person signing in with their own number on a laptop gets the six
/// digits, and it is the same trade the dev inbox already makes.
pub struct Sink {
    kept: Mutex<VecDeque<Text>>,
    limit: usize,
}

impl Sink {
    pub fn new(limit: usize) -> Sink {
        Sink {
            kept: Mutex::new(VecDeque::new()),
            limit,
        }
    }

    /// Everything it still holds, oldest first.
    pub fn kept(&self) -> Vec<Text> {
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

impl Default for Sink {
    fn default() -> Sink {
        Sink::new(100)
    }
}

impl Sender for Sink {
    fn deliver(&self, text: &Text) -> Result<String, String> {
        log::info!(
            "sms to {} kept in the dev inbox: {}",
            text.to,
            text.body.replace('\n', " ")
        );
        let mut kept = match self.kept.lock() {
            Ok(kept) => kept,
            Err(poisoned) => poisoned.into_inner(),
        };
        if kept.len() == self.limit {
            kept.pop_front();
        }
        kept.push_back(text.clone());
        Ok(String::new())
    }

    fn sink(&self) -> Option<&Sink> {
        Some(self)
    }

    fn describe(&self) -> String {
        "the dev inbox".to_string()
    }
}

/// One call to a provider. All of them are form posts, they differ only
/// in what they put in the authorization header.
#[derive(Debug, PartialEq, Eq)]
pub struct Call {
    pub url: String,
    pub form: Vec<(String, String)>,
    pub headers: Vec<(String, String)>,
}

impl Call {
    /// One form field, for a test that wants to assert on a field
    /// rather than on the whole body.
    pub fn field(&self, name: &str) -> &str {
        match self.form.iter().find(|(k, _)| k == name) {
            Some((_, v)) => v,
            None => "",
        }
    }

    pub fn header(&self, name: &str) -> &str {
        match self.headers.iter().find(|(k, _)| k == name) {
            Some((_, v)) => v,
            None => "",
        }
    }
}

#[derive(Debug)]
pub struct Reply {
    pub status: u16,
    pub body: String,
}

/// What puts a provider call on the wire, behind a trait for the same
/// reason the oauth one is: a test cannot be Twilio.
pub trait Wire: Send + Sync {
    fn post(&self, call: &Call) -> Result<Reply, String>;
}

/// The real one.
pub struct Web {
    agent: ureq::Agent,
}

impl Default for Web {
    fn default() -> Web {
        Web {
            // A provider answering 4xx is saying something worth
            // reading, and every one of these reads its own error body,
            // so statuses come back as replies rather than as transport
            // errors.
            agent: ureq::Agent::config_builder()
                .http_status_as_error(false)
                .timeout_global(Some(std::time::Duration::from_secs(10)))
                .build()
                .into(),
        }
    }
}

impl Wire for Web {
    fn post(&self, call: &Call) -> Result<Reply, String> {
        use std::io::Read;
        let mut req = self.agent.post(&call.url).header("user-agent", "zou");
        for (name, value) in &call.headers {
            req = req.header(name, value);
        }
        let form: Vec<(&str, &str)> = call
            .form
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let mut res = req
            .send_form(form)
            .map_err(|e| format!("calling {}: {e}", call.url))?;
        let status = res.status().as_u16();
        let mut body = String::new();
        res.body_mut()
            .as_reader()
            .take(1 << 20)
            .read_to_string(&mut body)
            .map_err(|e| format!("reading {}: {e}", call.url))?;
        Ok(Reply { status, body })
    }
}

/// A basic auth header value, which is what Twilio wants and what
/// nothing else in this tree needed until now.
fn basic(user: &str, pass: &str) -> String {
    use base64ct::Encoding;
    format!(
        "Basic {}",
        base64ct::Base64::encode_string(format!("{user}:{pass}").as_bytes())
    )
}

/// A json field as a string, however the provider typed it, because
/// Twilio sends its error code as a number and its status as a string.
fn text_of(value: &serde_json::Value, key: &str) -> String {
    match value.get(key) {
        None | Some(serde_json::Value::Null) => String::new(),
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
    }
}

/// Twilio's Programmable Messaging, GoTrue's default provider and the
/// one every Supabase project that sends an SMS is using.
pub struct Twilio {
    pub account_sid: String,
    pub auth_token: String,
    /// GOTRUE_SMS_TWILIO_MESSAGE_SERVICE_SID, which is the sender: a
    /// messaging service sid or a number, Twilio takes either in From.
    pub message_service_sid: String,
    /// GOTRUE_SMS_TWILIO_CONTENT_SID, a WhatsApp authentication
    /// template. With one set the code goes as a template variable
    /// instead of as a body, which is what WhatsApp requires of a
    /// message a person did not reply to.
    pub content_sid: String,
    /// The api root, so a test can point it somewhere that is not
    /// Twilio.
    pub base: String,
    pub wire: Arc<dyn Wire>,
}

impl Twilio {
    pub fn new(account_sid: &str, auth_token: &str, message_service_sid: &str) -> Twilio {
        Twilio {
            account_sid: account_sid.to_string(),
            auth_token: auth_token.to_string(),
            message_service_sid: message_service_sid.to_string(),
            content_sid: String::new(),
            base: "https://api.twilio.com".to_string(),
            wire: Arc::new(Web::default()),
        }
    }

    pub fn request(&self, text: &Text) -> Call {
        let url = format!(
            "{}/2010-04-01/Accounts/{}/Messages.json",
            self.base.trim_end_matches('/'),
            self.account_sid
        );
        // Twilio wants the plus back on, whatever the column holds.
        let mut to = format!("+{}", text.to);
        let mut from = self.message_service_sid.clone();
        let mut form = Vec::new();
        if text.channel == WHATSAPP {
            to = format!("{WHATSAPP}:{to}");
            // A messaging service sid is not a number and is not
            // prefixed, a number is.
            if e164(&strip(&from)) {
                from = format!("{WHATSAPP}:{from}");
            }
            form.push(("To".to_string(), to));
            form.push(("Channel".to_string(), text.channel.clone()));
            form.push(("From".to_string(), from));
            if self.content_sid.is_empty() {
                form.push(("Body".to_string(), text.body.clone()));
            } else {
                form.push(("ContentSid".to_string(), self.content_sid.clone()));
                form.push((
                    "ContentVariables".to_string(),
                    serde_json::json!({"1": text.code}).to_string(),
                ));
            }
        } else {
            form.push(("To".to_string(), to));
            form.push(("Channel".to_string(), text.channel.clone()));
            form.push(("From".to_string(), from));
            form.push(("Body".to_string(), text.body.clone()));
        }
        Call {
            url,
            form,
            headers: vec![(
                "authorization".to_string(),
                basic(&self.account_sid, &self.auth_token),
            )],
        }
    }

    /// What Twilio said, in its own words. An accepted message that
    /// Twilio then failed to deliver is still a failure here, which is
    /// upstream's reading and the right one: the person never got the
    /// code.
    pub fn read(reply: &Reply) -> Result<String, String> {
        let body: serde_json::Value = serde_json::from_str(&reply.body)
            .map_err(|e| format!("twilio sent something that is not json: {e}"))?;
        if reply.status != 200 && reply.status != 201 {
            return Err(format!(
                "{} More information: {}",
                text_of(&body, "message"),
                text_of(&body, "more_info")
            ));
        }
        let sid = text_of(&body, "sid");
        let status = text_of(&body, "status");
        if status == "failed" || status == "undelivered" {
            return Err(format!(
                "twilio error: {} {} for message {sid}",
                text_of(&body, "error_message"),
                text_of(&body, "error_code")
            ));
        }
        Ok(sid)
    }
}

impl Sender for Twilio {
    fn deliver(&self, text: &Text) -> Result<String, String> {
        let call = self.request(text);
        Twilio::read(&self.wire.post(&call)?)
    }

    fn carries(&self, channel: &str) -> bool {
        channel == SMS || channel == WHATSAPP
    }

    fn describe(&self) -> String {
        format!("twilio, account {}", self.account_sid)
    }

    fn provider(&self) -> &'static str {
        "twilio"
    }
}

/// MessageBird, which is SMS and nothing else.
pub struct MessageBird {
    pub access_key: String,
    /// Who the message is from: a number, or an alphanumeric sender id
    /// where the country allows one.
    pub originator: String,
    pub base: String,
    pub wire: Arc<dyn Wire>,
}

impl MessageBird {
    pub fn new(access_key: &str, originator: &str) -> MessageBird {
        MessageBird {
            access_key: access_key.to_string(),
            originator: originator.to_string(),
            base: "https://rest.messagebird.com".to_string(),
            wire: Arc::new(Web::default()),
        }
    }

    pub fn request(&self, text: &Text) -> Call {
        Call {
            url: format!("{}/messages", self.base.trim_end_matches('/')),
            form: vec![
                ("originator".to_string(), self.originator.clone()),
                ("body".to_string(), text.body.clone()),
                ("recipients".to_string(), text.to.clone()),
                ("type".to_string(), "sms".to_string()),
                // Unicode, so a template with anything but ascii in it
                // arrives as it was written.
                ("datacoding".to_string(), "unicode".to_string()),
            ],
            headers: vec![(
                "authorization".to_string(),
                format!("AccessKey {}", self.access_key),
            )],
        }
    }

    pub fn read(reply: &Reply) -> Result<String, String> {
        let body: serde_json::Value = serde_json::from_str(&reply.body)
            .map_err(|e| format!("messagebird sent something that is not json: {e}"))?;
        if matches!(reply.status, 400 | 401 | 403 | 422) {
            let first = body
                .get("errors")
                .and_then(|e| e.get(0))
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            return Err(text_of(&first, "description"));
        }
        // MessageBird answers 200 for a request it accepted and then
        // sent to nobody, so the count is the only thing that says the
        // message went anywhere.
        let sent = body
            .get("recipients")
            .and_then(|r| r.get("totalSentCount"))
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        if sent == 0 {
            return Err("messagebird error: total sent count is 0".to_string());
        }
        Ok(text_of(&body, "id"))
    }
}

impl Sender for MessageBird {
    fn deliver(&self, text: &Text) -> Result<String, String> {
        let call = self.request(text);
        MessageBird::read(&self.wire.post(&call)?)
    }

    fn describe(&self) -> String {
        format!("messagebird, originator {}", self.originator)
    }

    fn provider(&self) -> &'static str {
        "messagebird"
    }
}

/// Vonage, which used to be Nexmo and still answers on that hostname.
pub struct Vonage {
    pub api_key: String,
    pub api_secret: String,
    /// Who the message is from: a number, or an alphanumeric sender id
    /// where the country allows one.
    pub from: String,
    pub base: String,
    pub wire: Arc<dyn Wire>,
}

impl Vonage {
    pub fn new(api_key: &str, api_secret: &str, from: &str) -> Vonage {
        Vonage {
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            from: from.to_string(),
            base: "https://rest.nexmo.com".to_string(),
            wire: Arc::new(Web::default()),
        }
    }

    pub fn request(&self, text: &Text) -> Call {
        Call {
            url: format!("{}/sms/json", self.base.trim_end_matches('/')),
            // The credentials are form fields rather than a header on
            // this one, which is why there is no authorization here to
            // forget.
            form: vec![
                ("api_key".to_string(), self.api_key.clone()),
                ("api_secret".to_string(), self.api_secret.clone()),
                ("from".to_string(), self.from.clone()),
                ("to".to_string(), text.to.clone()),
                ("text".to_string(), text.body.clone()),
                ("type".to_string(), "unicode".to_string()),
            ],
            headers: Vec::new(),
        }
    }

    /// Vonage answers 200 whatever happened and puts the outcome in the
    /// message, so the status field is the only thing worth reading and
    /// `"0"` is the only value that means sent.
    pub fn read(reply: &Reply) -> Result<String, String> {
        let body: serde_json::Value = serde_json::from_str(&reply.body)
            .map_err(|e| format!("vonage sent something that is not json: {e}"))?;
        let Some(first) = body.get("messages").and_then(|m| m.get(0)) else {
            return Err("vonage error: no messages found in response".to_string());
        };
        let status = text_of(first, "status");
        if status != "0" {
            return Err(format!(
                "vonage error: {} (status: {status})",
                text_of(first, "error-text")
            ));
        }
        Ok(text_of(first, "message-id"))
    }
}

impl Sender for Vonage {
    fn deliver(&self, text: &Text) -> Result<String, String> {
        let call = self.request(text);
        Vonage::read(&self.wire.post(&call)?)
    }

    fn describe(&self) -> String {
        format!("vonage, from {}", self.from)
    }

    fn provider(&self) -> &'static str {
        "vonage"
    }
}

/// TextLocal, which sends to India and takes a whole list of numbers at
/// once. Only ever one here, because a code goes to one person.
pub struct TextLocal {
    pub api_key: String,
    /// The sender id, which TextLocal registers rather than allocating,
    /// so it is a name and not a number.
    pub sender: String,
    pub base: String,
    pub wire: Arc<dyn Wire>,
}

impl TextLocal {
    pub fn new(api_key: &str, sender: &str) -> TextLocal {
        TextLocal {
            api_key: api_key.to_string(),
            sender: sender.to_string(),
            base: "https://api.textlocal.in".to_string(),
            wire: Arc::new(Web::default()),
        }
    }

    pub fn request(&self, text: &Text) -> Call {
        Call {
            url: format!("{}/send", self.base.trim_end_matches('/')),
            form: vec![
                ("apikey".to_string(), self.api_key.clone()),
                ("sender".to_string(), self.sender.clone()),
                ("numbers".to_string(), text.to.clone()),
                ("message".to_string(), text.body.clone()),
            ],
            headers: Vec::new(),
        }
    }

    pub fn read(reply: &Reply) -> Result<String, String> {
        let body: serde_json::Value = serde_json::from_str(&reply.body)
            .map_err(|e| format!("textlocal sent something that is not json: {e}"))?;
        if text_of(&body, "status") != "success" {
            let first = body
                .get("errors")
                .and_then(|e| e.get(0))
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            let message = text_of(&first, "message");
            if message.is_empty() {
                return Err("textlocal error: internal error".to_string());
            }
            return Err(format!(
                "textlocal error: {message} (code: {})",
                text_of(&first, "code")
            ));
        }
        // A success with nothing in it is not one this can name, and
        // upstream reads the first message's id the same way.
        let first = body
            .get("messages")
            .and_then(|m| m.get(0))
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        Ok(text_of(&first, "id"))
    }
}

impl Sender for TextLocal {
    fn deliver(&self, text: &Text) -> Result<String, String> {
        let call = self.request(text);
        TextLocal::read(&self.wire.post(&call)?)
    }

    fn describe(&self) -> String {
        format!("textlocal, sender {}", self.sender)
    }

    fn provider(&self) -> &'static str {
        "textlocal"
    }
}

/// A number with the plus and the spaces taken off, which is the form
/// everything here holds and sends.
pub fn strip(phone: &str) -> String {
    phone.trim_start_matches('+').replace([' ', '\t'], "")
}

/// E.164 as GoTrue checks it: a leading digit that is not zero, then
/// one to fourteen more, and nothing else.
pub fn e164(phone: &str) -> bool {
    let bytes = phone.as_bytes();
    if bytes.len() < 2 || bytes.len() > 15 {
        return false;
    }
    if !(b'1'..=b'9').contains(&bytes[0]) {
        return false;
    }
    bytes[1..].iter().all(u8::is_ascii_digit)
}

/// The provider an operator configured, from the environment, the way
/// `smtp::from_env` reads the mail one. Nothing set is None, which is
/// the dev sink.
pub fn from_env() -> Result<Option<Arc<dyn Sender>>, String> {
    configured(&|name| std::env::var(name).unwrap_or_default())
}

/// The same, over anything that can look a name up, which is what makes
/// the credential rules testable without touching the environment of
/// the process the tests run in.
pub fn configured(var: &dyn Fn(&str) -> String) -> Result<Option<Arc<dyn Sender>>, String> {
    let provider = var("ZOU_SMS_PROVIDER");
    match provider.as_str() {
        "" => Ok(None),
        "twilio" => {
            let (sid, token, service) = (
                var("ZOU_SMS_TWILIO_ACCOUNT_SID"),
                var("ZOU_SMS_TWILIO_AUTH_TOKEN"),
                var("ZOU_SMS_TWILIO_MESSAGE_SERVICE_SID"),
            );
            if sid.is_empty() || token.is_empty() || service.is_empty() {
                return Err("twilio needs ZOU_SMS_TWILIO_ACCOUNT_SID, ZOU_SMS_TWILIO_AUTH_TOKEN and ZOU_SMS_TWILIO_MESSAGE_SERVICE_SID".to_string());
            }
            let mut twilio = Twilio::new(&sid, &token, &service);
            twilio.content_sid = var("ZOU_SMS_TWILIO_CONTENT_SID");
            Ok(Some(Arc::new(twilio)))
        }
        "messagebird" => {
            let (key, originator) = (
                var("ZOU_SMS_MESSAGEBIRD_ACCESS_KEY"),
                var("ZOU_SMS_MESSAGEBIRD_ORIGINATOR"),
            );
            if key.is_empty() || originator.is_empty() {
                return Err(
                    "messagebird needs ZOU_SMS_MESSAGEBIRD_ACCESS_KEY and ZOU_SMS_MESSAGEBIRD_ORIGINATOR"
                        .to_string(),
                );
            }
            Ok(Some(Arc::new(MessageBird::new(&key, &originator))))
        }
        "vonage" => {
            let (key, secret, from) = (
                var("ZOU_SMS_VONAGE_API_KEY"),
                var("ZOU_SMS_VONAGE_API_SECRET"),
                var("ZOU_SMS_VONAGE_FROM"),
            );
            if key.is_empty() || secret.is_empty() || from.is_empty() {
                return Err(
                    "vonage needs ZOU_SMS_VONAGE_API_KEY, ZOU_SMS_VONAGE_API_SECRET and ZOU_SMS_VONAGE_FROM"
                        .to_string(),
                );
            }
            Ok(Some(Arc::new(Vonage::new(&key, &secret, &from))))
        }
        "textlocal" => {
            let (key, sender) = (
                var("ZOU_SMS_TEXTLOCAL_API_KEY"),
                var("ZOU_SMS_TEXTLOCAL_SENDER"),
            );
            if key.is_empty() || sender.is_empty() {
                return Err(
                    "textlocal needs ZOU_SMS_TEXTLOCAL_API_KEY and ZOU_SMS_TEXTLOCAL_SENDER"
                        .to_string(),
                );
            }
            Ok(Some(Arc::new(TextLocal::new(&key, &sender))))
        }
        other => Err(format!(
            "sms provider {other} is not one of twilio, messagebird, vonage, textlocal"
        )),
    }
}

/// The text message settings from the environment, GoTrue's
/// `GOTRUE_SMS_*` with the prefix swapped, so a project moving across
/// brings its own template and its own ceilings with it.
pub fn settings_from_env() -> Result<Settings, String> {
    settings(&|name| std::env::var(name).unwrap_or_default())
}

/// The same over anything that can look a name up.
pub fn settings(var: &dyn Fn(&str) -> String) -> Result<Settings, String> {
    let stock = Settings::default();
    Ok(Settings {
        template: var("ZOU_SMS_TEMPLATE").trim().to_string(),
        otp_length: count(var, "ZOU_SMS_OTP_LENGTH", stock.otp_length)?,
        // Upstream holds this one as a plain number of seconds rather
        // than as a duration, but a project that wrote `60s` meant the
        // same minute, so both are read.
        otp_exp: crate::limit::seconds(var, "ZOU_SMS_OTP_EXP", stock.otp_exp as u64)? as i64,
        max_frequency: crate::limit::seconds(var, "ZOU_SMS_MAX_FREQUENCY", stock.max_frequency)?,
        autoconfirm: switch(var, "ZOU_SMS_AUTOCONFIRM", stock.autoconfirm)?,
    })
}

/// A whole number, refused rather than defaulted when it is not one.
fn count(var: &dyn Fn(&str) -> String, name: &str, stock: usize) -> Result<usize, String> {
    let raw = var(name);
    let text = raw.trim();
    if text.is_empty() {
        return Ok(stock);
    }
    text.parse()
        .map_err(|_| format!("{name} is {text:?}, which is not a number"))
}

fn switch(var: &dyn Fn(&str) -> String, name: &str, stock: bool) -> Result<bool, String> {
    match var(name).trim() {
        "" => Ok(stock),
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        other => Err(format!("{name} is {other:?}, which is not true or false")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(channel: &str) -> Text {
        Text {
            to: "15551234567".to_string(),
            body: "Your code is 123456".to_string(),
            code: "123456".to_string(),
            channel: channel.to_string(),
            at: 0,
        }
    }

    #[test]
    fn the_code_is_substituted_however_it_is_spaced() {
        assert_eq!(render("{{ .Code }}", "123456"), "123456");
        assert_eq!(render("{{.Code}}/{{   .Code   }}", "42"), "42/42");
    }

    #[test]
    fn something_this_does_not_understand_is_left_alone() {
        // The same rule the mail templates follow, for the same reason:
        // a quietly dropped variable is a text message with no code in
        // it.
        assert_eq!(render("{{ .Token }}x", "1"), "{{ .Token }}x");
        assert_eq!(render("{{ .Code", "1"), "{{ .Code");
    }

    #[test]
    fn an_unset_template_is_gotrues_own() {
        let settings = Settings::default();
        assert_eq!(settings.body("123456"), "Your code is 123456");
        let mine = Settings {
            template: "zou: {{ .Code }}".to_string(),
            ..Settings::default()
        };
        assert_eq!(mine.body("123456"), "zou: 123456");
    }

    #[test]
    fn a_code_length_outside_the_range_takes_six() {
        let at = |otp_length| {
            Settings {
                otp_length,
                ..Settings::default()
            }
            .digits()
        };
        assert_eq!(at(6), 6);
        assert_eq!(at(10), 10);
        // Upstream clamps rather than refuses, and four digits is a
        // guessable code.
        assert_eq!(at(4), 6);
        assert_eq!(at(11), 6);
        assert_eq!(at(0), 6);
    }

    #[test]
    fn e164_is_the_shape_upstream_checks_for() {
        assert!(e164("15551234567"));
        assert!(e164("12"));
        assert!(!e164("1"), "one digit is not a number");
        assert!(!e164("0155512345"), "no leading zero");
        assert!(!e164("1555123456789012"), "sixteen digits is too many");
        assert!(!e164("+15551234567"), "the plus comes off first");
        assert!(!e164("1555 123"), "and so do the spaces");
        assert!(!e164("155512345a"));
        assert!(!e164(""));
    }

    #[test]
    fn a_number_arrives_in_every_shape_a_person_types_it() {
        assert_eq!(strip("+1 555 123 4567"), "15551234567");
        assert_eq!(strip("15551234567"), "15551234567");
        assert_eq!(strip("+15551234567"), "15551234567");
    }

    #[test]
    fn twilio_sends_the_number_with_its_plus_back_on() {
        let twilio = Twilio::new("AC1", "secret", "MG9");
        let call = twilio.request(&text(SMS));
        assert_eq!(
            call.url,
            "https://api.twilio.com/2010-04-01/Accounts/AC1/Messages.json"
        );
        assert_eq!(call.field("To"), "+15551234567");
        assert_eq!(call.field("From"), "MG9");
        assert_eq!(call.field("Channel"), "sms");
        assert_eq!(call.field("Body"), "Your code is 123456");
        // The account sid is the username and the token is the
        // password, which is base64 of "AC1:secret".
        assert_eq!(call.header("authorization"), "Basic QUMxOnNlY3JldA==");
    }

    #[test]
    fn whatsapp_prefixes_both_ends_when_the_sender_is_a_number() {
        let twilio = Twilio::new("AC1", "secret", "15559999999");
        let call = twilio.request(&text(WHATSAPP));
        assert_eq!(call.field("To"), "whatsapp:+15551234567");
        assert_eq!(call.field("From"), "whatsapp:15559999999");

        // A messaging service sid is not a number, so it is left alone.
        let service = Twilio::new("AC1", "secret", "MG9");
        assert_eq!(service.request(&text(WHATSAPP)).field("From"), "MG9");
    }

    #[test]
    fn a_whatsapp_template_carries_the_code_as_a_variable() {
        let mut twilio = Twilio::new("AC1", "secret", "MG9");
        twilio.content_sid = "HX1".to_string();
        let call = twilio.request(&text(WHATSAPP));
        assert_eq!(call.field("ContentSid"), "HX1");
        assert_eq!(call.field("ContentVariables"), r#"{"1":"123456"}"#);
        assert_eq!(
            call.field("Body"),
            "",
            "WhatsApp refuses a free text message nobody replied to"
        );
        // Without one it is an ordinary body, which is what the older
        // projects are still on.
        assert_eq!(
            Twilio::new("AC1", "secret", "MG9")
                .request(&text(WHATSAPP))
                .field("Body"),
            "Your code is 123456"
        );
    }

    #[test]
    fn twilio_says_what_went_wrong_in_its_own_words() {
        let queued = Reply {
            status: 201,
            body: r#"{"sid":"SM1","status":"queued"}"#.to_string(),
        };
        assert_eq!(Twilio::read(&queued), Ok("SM1".to_string()));

        let refused = Reply {
            status: 400,
            body: r#"{"code":21211,"message":"Invalid 'To' Phone Number",
                     "more_info":"https://www.twilio.com/docs/errors/21211"}"#
                .to_string(),
        };
        assert_eq!(
            Twilio::read(&refused),
            Err("Invalid 'To' Phone Number More information: https://www.twilio.com/docs/errors/21211".to_string())
        );

        // Accepted and then not delivered is still a person who never
        // got the code.
        let undelivered = Reply {
            status: 201,
            body: r#"{"sid":"SM2","status":"undelivered","error_code":30006,
                     "error_message":"Landline or unreachable carrier"}"#
                .to_string(),
        };
        assert_eq!(
            Twilio::read(&undelivered),
            Err("twilio error: Landline or unreachable carrier 30006 for message SM2".to_string())
        );
    }

    #[test]
    fn messagebird_posts_the_number_without_a_plus() {
        let bird = MessageBird::new("key", "zou");
        let call = bird.request(&text(SMS));
        assert_eq!(call.url, "https://rest.messagebird.com/messages");
        assert_eq!(call.field("recipients"), "15551234567");
        assert_eq!(call.field("originator"), "zou");
        assert_eq!(call.field("body"), "Your code is 123456");
        assert_eq!(call.field("type"), "sms");
        assert_eq!(call.field("datacoding"), "unicode");
        assert_eq!(call.header("authorization"), "AccessKey key");
    }

    #[test]
    fn messagebird_answering_two_hundred_can_still_have_sent_nothing() {
        let sent = Reply {
            status: 200,
            body: r#"{"id":"mb1","recipients":{"totalSentCount":1}}"#.to_string(),
        };
        assert_eq!(MessageBird::read(&sent), Ok("mb1".to_string()));

        let none = Reply {
            status: 200,
            body: r#"{"id":"mb2","recipients":{"totalSentCount":0}}"#.to_string(),
        };
        assert_eq!(
            MessageBird::read(&none),
            Err("messagebird error: total sent count is 0".to_string())
        );

        let refused = Reply {
            status: 401,
            body: r#"{"errors":[{"code":2,"description":"Request not allowed (incorrect access_key)"}]}"#.to_string(),
        };
        assert_eq!(
            MessageBird::read(&refused),
            Err("Request not allowed (incorrect access_key)".to_string())
        );
    }

    #[test]
    fn only_twilio_carries_whatsapp() {
        assert!(Twilio::new("AC1", "s", "MG9").carries(WHATSAPP));
        assert!(Twilio::new("AC1", "s", "MG9").carries(SMS));
        assert!(!MessageBird::new("k", "zou").carries(WHATSAPP));
        assert!(MessageBird::new("k", "zou").carries(SMS));
        assert!(!Sink::default().carries(WHATSAPP));
        assert!(Sink::default().carries(SMS));
    }

    #[test]
    fn the_sink_keeps_the_last_few_and_drops_the_rest() {
        let sink = Sink::new(2);
        for n in 0..3 {
            let mut text = text(SMS);
            text.to = format!("1555000000{n}");
            sink.deliver(&text).expect("the dev sink never refuses");
        }
        let kept = sink.kept();
        assert_eq!(kept.len(), 2);
        assert_eq!(kept[0].to, "15550000001", "the oldest went first");
        assert_eq!(kept[1].to, "15550000002");
        sink.clear();
        assert!(sink.kept().is_empty());
    }

    struct Recorded {
        reply: Mutex<Option<Reply>>,
        seen: Mutex<Vec<String>>,
    }

    impl Wire for Recorded {
        fn post(&self, call: &Call) -> Result<Reply, String> {
            self.seen
                .lock()
                .expect("not poisoned")
                .push(call.url.clone());
            match self.reply.lock().expect("not poisoned").take() {
                Some(reply) => Ok(reply),
                None => Err("nothing left to answer with".to_string()),
            }
        }
    }

    #[test]
    fn delivering_is_the_request_the_wire_and_the_answer() {
        let wire = Arc::new(Recorded {
            reply: Mutex::new(Some(Reply {
                status: 201,
                body: r#"{"sid":"SM7","status":"queued"}"#.to_string(),
            })),
            seen: Mutex::new(Vec::new()),
        });
        let mut twilio = Twilio::new("AC1", "secret", "MG9");
        twilio.wire = wire.clone();
        assert_eq!(twilio.deliver(&text(SMS)), Ok("SM7".to_string()));
        assert_eq!(
            wire.seen.lock().expect("not poisoned").as_slice(),
            ["https://api.twilio.com/2010-04-01/Accounts/AC1/Messages.json"]
        );
        // A wire that cannot answer is the provider being unreachable,
        // and that reaches the caller rather than being swallowed.
        assert!(twilio.deliver(&text(SMS)).is_err());
    }

    #[test]
    fn a_provider_needs_all_of_its_credentials_or_none() {
        let env = |pairs: &[(&str, &str)]| {
            let pairs: Vec<(String, String)> = pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            configured(&move |name| {
                pairs
                    .iter()
                    .find(|(k, _)| k == name)
                    .map(|(_, v)| v.clone())
                    .unwrap_or_default()
            })
        };
        // Nothing configured is the dev sink, which is the whole local
        // loop.
        assert!(env(&[]).expect("nothing set is not an error").is_none());
        let twilio = env(&[
            ("ZOU_SMS_PROVIDER", "twilio"),
            ("ZOU_SMS_TWILIO_ACCOUNT_SID", "AC1"),
            ("ZOU_SMS_TWILIO_AUTH_TOKEN", "secret"),
            ("ZOU_SMS_TWILIO_MESSAGE_SERVICE_SID", "MG9"),
        ])
        .expect("all three is a provider")
        .expect("and it is there");
        assert_eq!(twilio.describe(), "twilio, account AC1");
        // Half configured is a project that thinks it can send and
        // cannot, so it is a startup error rather than a silent sink.
        assert!(
            env(&[
                ("ZOU_SMS_PROVIDER", "twilio"),
                ("ZOU_SMS_TWILIO_ACCOUNT_SID", "AC1"),
            ])
            .is_err()
        );
        let bird = env(&[
            ("ZOU_SMS_PROVIDER", "messagebird"),
            ("ZOU_SMS_MESSAGEBIRD_ACCESS_KEY", "key"),
            ("ZOU_SMS_MESSAGEBIRD_ORIGINATOR", "zou"),
        ])
        .expect("both is a provider")
        .expect("and it is there");
        assert_eq!(bird.describe(), "messagebird, originator zou");
        assert!(env(&[("ZOU_SMS_PROVIDER", "messagebird")]).is_err());
        let vonage = env(&[
            ("ZOU_SMS_PROVIDER", "vonage"),
            ("ZOU_SMS_VONAGE_API_KEY", "key"),
            ("ZOU_SMS_VONAGE_API_SECRET", "secret"),
            ("ZOU_SMS_VONAGE_FROM", "zou"),
        ])
        .expect("all three is a provider")
        .expect("and it is there");
        assert_eq!(vonage.describe(), "vonage, from zou");
        assert!(env(&[("ZOU_SMS_PROVIDER", "vonage")]).is_err());
        let textlocal = env(&[
            ("ZOU_SMS_PROVIDER", "textlocal"),
            ("ZOU_SMS_TEXTLOCAL_API_KEY", "key"),
            ("ZOU_SMS_TEXTLOCAL_SENDER", "zou"),
        ])
        .expect("both is a provider")
        .expect("and it is there");
        assert_eq!(textlocal.describe(), "textlocal, sender zou");
        assert!(env(&[("ZOU_SMS_PROVIDER", "textlocal")]).is_err());
        assert!(
            env(&[("ZOU_SMS_PROVIDER", "plivo")]).is_err(),
            "a provider that is not here says so rather than sending nothing"
        );
    }

    #[test]
    fn vonage_sends_the_credentials_in_the_form_and_reads_the_first_message() {
        let vonage = Vonage::new("key", "secret", "zou");
        let call = vonage.request(&text(SMS));
        assert_eq!(call.url, "https://rest.nexmo.com/sms/json");
        assert_eq!(call.field("api_key"), "key");
        assert_eq!(call.field("api_secret"), "secret");
        assert_eq!(call.field("from"), "zou");
        // The number goes without the plus, which is the form the
        // column holds and what upstream sends.
        assert_eq!(call.field("to"), "15551234567");
        assert_eq!(call.field("text"), "Your code is 123456");
        assert_eq!(call.field("type"), "unicode");
        assert!(
            call.headers.is_empty(),
            "the credentials are the form, so there is no header to get wrong"
        );

        let sent = |body: &str| {
            Vonage::read(&Reply {
                status: 200,
                body: body.to_string(),
            })
        };
        assert_eq!(
            sent(r#"{"messages":[{"status":"0","message-id":"0A"}]}"#),
            Ok("0A".to_string())
        );
        // Vonage answers 200 on a refusal too, so a status that is not
        // zero is the failure and the text beside it is what happened.
        assert_eq!(
            sent(r#"{"messages":[{"status":"4","error-text":"Bad Credentials"}]}"#),
            Err("vonage error: Bad Credentials (status: 4)".to_string())
        );
        assert_eq!(
            sent(r#"{"messages":[]}"#),
            Err("vonage error: no messages found in response".to_string())
        );
        assert!(sent("not json").is_err());
    }

    #[test]
    fn textlocal_reads_the_status_word_rather_than_the_http_one() {
        let textlocal = TextLocal::new("key", "zou");
        let call = textlocal.request(&text(SMS));
        assert_eq!(call.url, "https://api.textlocal.in/send");
        assert_eq!(call.field("apikey"), "key");
        assert_eq!(call.field("sender"), "zou");
        assert_eq!(call.field("numbers"), "15551234567");
        assert_eq!(call.field("message"), "Your code is 123456");

        let sent = |body: &str| {
            TextLocal::read(&Reply {
                status: 200,
                body: body.to_string(),
            })
        };
        assert_eq!(
            sent(r#"{"status":"success","messages":[{"id":"77"}]}"#),
            Ok("77".to_string())
        );
        assert_eq!(
            sent(r#"{"status":"failure","errors":[{"code":3,"message":"Invalid login details"}]}"#),
            Err("textlocal error: Invalid login details (code: 3)".to_string())
        );
        // A failure it did not explain is still a failure, and saying
        // so is better than reading an id that is not there.
        assert_eq!(
            sent(r#"{"status":"failure"}"#),
            Err("textlocal error: internal error".to_string())
        );
        assert!(sent("not json").is_err());
    }

    #[test]
    fn neither_of_the_two_new_providers_carries_whatsapp() {
        assert!(!Vonage::new("k", "s", "zou").carries(WHATSAPP));
        assert!(Vonage::new("k", "s", "zou").carries(SMS));
        assert!(!TextLocal::new("k", "zou").carries(WHATSAPP));
        assert!(TextLocal::new("k", "zou").carries(SMS));
    }

    #[test]
    fn the_settings_are_gotrues_with_the_prefix_swapped() {
        let env = |pairs: &[(&str, &str)]| {
            let pairs: Vec<(String, String)> = pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            settings(&move |name| {
                pairs
                    .iter()
                    .find(|(k, _)| k == name)
                    .map(|(_, v)| v.clone())
                    .unwrap_or_default()
            })
        };
        let stock = env(&[]).expect("nothing set is not an error");
        assert_eq!(stock.otp_length, 6);
        assert_eq!(stock.otp_exp, 60);
        assert_eq!(stock.max_frequency, 60);
        assert!(!stock.autoconfirm);
        assert_eq!(stock.body("123456"), "Your code is 123456");

        let set = env(&[
            ("ZOU_SMS_TEMPLATE", "{{ .Code }} is your zou code"),
            ("ZOU_SMS_OTP_LENGTH", "8"),
            ("ZOU_SMS_OTP_EXP", "300"),
            // Upstream holds this one as a Go duration and a hosted
            // config is full of them, so it has to read as one.
            ("ZOU_SMS_MAX_FREQUENCY", "1m30s"),
            ("ZOU_SMS_AUTOCONFIRM", "true"),
        ])
        .expect("all of it is readable");
        assert_eq!(set.body("42"), "42 is your zou code");
        assert_eq!(set.digits(), 8);
        assert_eq!(set.otp_exp, 300);
        assert_eq!(set.max_frequency, 90);
        assert!(set.autoconfirm);

        // A value nobody can act on is a startup error rather than the
        // default arriving in its place, which is the whole reason
        // these are read here and not with a parse and an unwrap_or.
        assert!(env(&[("ZOU_SMS_OTP_LENGTH", "six")]).is_err());
        assert!(env(&[("ZOU_SMS_MAX_FREQUENCY", "1 minute")]).is_err());
        assert!(env(&[("ZOU_SMS_AUTOCONFIRM", "yes")]).is_err());
    }
}
