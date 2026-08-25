//! The two questions a function asks about a name: what address is it,
//! and what does its zone say about it.
//!
//! The thing that asks is a mail sender. `send-email-smtp` in the
//! Supabase examples looks up the MX records of the domain it is
//! sending to and opens a socket to the best of them, and there is no
//! way to do that with the address lookup alone: an MX record is not an
//! address and the host's resolver library will not hand one over.
//!
//! So there are two ops rather than one, because they are two different
//! things and only one of them is a name lookup. [`op_zou_dns_lookup`]
//! is the host's own resolution, `getaddrinfo` through tokio, which is
//! what `node:dns` `lookup` is and what makes `localhost`, an
//! `/etc/hosts` entry and a search domain work the way they do
//! everywhere else on the machine. [`op_zou_dns_resolve`] is a real DNS
//! query put on the wire here, because a record that is not an address
//! has to be asked for by type.
//!
//! The query is written out rather than taken from a resolver crate.
//! What is needed is one question, one answer and the record types a
//! package actually reads, which is a few hundred lines of a format
//! that has not changed since 1987, against a dependency that carries a
//! recursive resolver, a cache, a zone parser and DNSSEC. The parsing
//! is the part worth being careful about and it is careful: a
//! compressed name may only ever point backwards, every read is bounds
//! checked against the packet it came in, and a record whose length
//! does not match its contents is dropped rather than read past.
//!
//! Which resolver is asked is the host's business and not the
//! function's. `ZOU_DNS_RESOLVER` names one, or several separated by
//! commas, and otherwise the nameservers in `/etc/resolv.conf` are used
//! in the order they are written. A host with neither says so, rather
//! than reaching for a public resolver nobody asked it to send the
//! project's lookups to.

use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use deno_core::op2;
use tokio::net::{TcpStream, UdpSocket};

/// How long one resolver has to answer before the next one is asked.
/// A name that cannot be resolved is a function that fails, and a
/// function that hangs on a resolver nobody is running is worse.
const ANSWER: Duration = Duration::from_secs(5);

/// The most a DNS message may be over UDP without saying otherwise, and
/// what this asks for. An answer that did not fit says so with the
/// truncation bit and is asked again over TCP.
const DATAGRAM: usize = 512;

/// The record types this asks for, by the numbers on the wire.
const A: u16 = 1;
const NS: u16 = 2;
const CNAME: u16 = 5;
const SOA: u16 = 6;
const PTR: u16 = 12;
const MX: u16 = 15;
const TXT: u16 = 16;
const AAAA: u16 = 28;
const SRV: u16 = 33;
const CAA: u16 = 257;

/// One address the host resolved a name to, in the shape `node:dns`
/// `lookup` hands back.
#[derive(serde::Serialize)]
pub struct Address {
    address: String,
    family: u8,
}

/// What a name lookup did. The failure carries the code node's own
/// errors carry, because a package that catches one branches on it.
#[derive(serde::Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum Looked {
    Addresses { addresses: Vec<Address> },
    Failed { code: &'static str, why: String },
}

/// What a query did. The records are already in the shape the record
/// type has, because a record is only useful once it has been read and
/// there is nothing javascript could do with the bytes.
#[derive(serde::Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum Found {
    Answers { records: serde_json::Value },
    Failed { code: &'static str, why: String },
}

/// The addresses a name has, asked the way the rest of the host asks.
///
/// This is `getaddrinfo` and not a query, which is the difference that
/// matters: `localhost`, a name in `/etc/hosts` and a search domain are
/// all things the resolver library knows and the wire does not.
#[op2(async(lazy))]
#[serde]
pub async fn op_zou_dns_lookup(#[string] hostname: String) -> Looked {
    // The port is not part of the question and nothing is opened here,
    // but `getaddrinfo` takes a service alongside the name, so it is
    // asked with one and told about it.
    let asked = tokio::net::lookup_host((hostname.as_str(), 0));
    let answered = match tokio::time::timeout(ANSWER, asked).await {
        Err(_) => {
            return Looked::Failed {
                code: "ETIMEOUT",
                why: format!(
                    "resolving {hostname} took longer than {}s",
                    ANSWER.as_secs()
                ),
            };
        }
        Ok(answered) => answered,
    };
    let addresses = match answered {
        Ok(found) => found,
        Err(e) => {
            return Looked::Failed {
                code: "ENOTFOUND",
                why: format!("resolving {hostname}: {e}"),
            };
        }
    };
    let addresses: Vec<Address> = addresses
        .map(|addr| Address {
            address: addr.ip().to_string(),
            family: if addr.is_ipv6() { 6 } else { 4 },
        })
        .collect();
    if addresses.is_empty() {
        return Looked::Failed {
            code: "ENODATA",
            why: format!("{hostname} resolved to no addresses"),
        };
    }
    Looked::Addresses { addresses }
}

/// The records of one type a name has, asked on the wire.
///
/// `server` is the resolver the caller named, empty for the host's own.
#[op2(async(lazy))]
#[serde]
pub async fn op_zou_dns_resolve(
    #[string] name: String,
    #[string] kind: String,
    #[string] server: String,
) -> Found {
    let Some(asked) = numbered(&kind) else {
        return Found::Failed {
            code: "EBADRESP",
            why: format!("{kind} is not a record type this runtime asks for"),
        };
    };
    let question = match question(&name, asked) {
        Ok(question) => question,
        Err(why) => {
            return Found::Failed {
                code: "EBADNAME",
                why,
            };
        }
    };
    let servers = match resolvers(&server).await {
        Ok(servers) => servers,
        Err(why) => {
            return Found::Failed {
                code: "ESERVFAIL",
                why,
            };
        }
    };
    let mut last = String::new();
    for server in &servers {
        match asked_of(*server, &question, asked, &name).await {
            Ok(found) => return found,
            // A resolver that did not answer at all is a resolver that
            // is skipped. One that answered with a refusal answered,
            // and that is what the function is told.
            Err(why) => last = why,
        }
    }
    Found::Failed {
        code: "ESERVFAIL",
        why: if last.is_empty() {
            format!("no resolver answered for {name}")
        } else {
            last
        },
    }
}

/// One resolver, over a datagram and then over a stream if the answer
/// did not fit in one.
async fn asked_of(
    server: SocketAddr,
    question: &[u8],
    asked: u16,
    name: &str,
) -> Result<Found, String> {
    let packet = over_udp(server, question).await?;
    let packet = if truncated(&packet) {
        over_tcp(server, question).await?
    } else {
        packet
    };
    Ok(read(&packet, question, asked, name))
}

async fn over_udp(server: SocketAddr, question: &[u8]) -> Result<Vec<u8>, String> {
    let here: SocketAddr = if server.is_ipv6() {
        "[::]:0".parse().expect("a v6 wildcard")
    } else {
        "0.0.0.0:0".parse().expect("a v4 wildcard")
    };
    let socket = UdpSocket::bind(here)
        .await
        .map_err(|e| format!("opening a socket to ask {server}: {e}"))?;
    socket
        .connect(server)
        .await
        .map_err(|e| format!("asking {server}: {e}"))?;
    socket
        .send(question)
        .await
        .map_err(|e| format!("asking {server}: {e}"))?;
    let mut into = vec![0u8; DATAGRAM];
    let read = tokio::time::timeout(ANSWER, socket.recv(&mut into))
        .await
        .map_err(|_| format!("{server} did not answer within {}s", ANSWER.as_secs()))?
        .map_err(|e| format!("reading the answer from {server}: {e}"))?;
    into.truncate(read);
    Ok(into)
}

/// The same question over TCP, which is where an answer too large for a
/// datagram is asked for again. The length goes in front of the message
/// in both directions, which is the whole of the difference.
async fn over_tcp(server: SocketAddr, question: &[u8]) -> Result<Vec<u8>, String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let connecting = TcpStream::connect(server);
    let mut stream = tokio::time::timeout(ANSWER, connecting)
        .await
        .map_err(|_| format!("{server} did not accept within {}s", ANSWER.as_secs()))?
        .map_err(|e| format!("asking {server} over tcp: {e}"))?;
    let mut out = Vec::with_capacity(question.len() + 2);
    let size =
        u16::try_from(question.len()).map_err(|_| "that question is too long".to_string())?;
    out.extend_from_slice(&size.to_be_bytes());
    out.extend_from_slice(question);
    let talking = async {
        stream.write_all(&out).await?;
        stream.flush().await?;
        let mut header = [0u8; 2];
        stream.read_exact(&mut header).await?;
        let mut into = vec![0u8; u16::from_be_bytes(header) as usize];
        stream.read_exact(&mut into).await?;
        Ok::<Vec<u8>, std::io::Error>(into)
    };
    tokio::time::timeout(ANSWER, talking)
        .await
        .map_err(|_| {
            format!(
                "{server} did not answer over tcp within {}s",
                ANSWER.as_secs()
            )
        })?
        .map_err(|e| format!("reading the answer from {server} over tcp: {e}"))
}

fn truncated(packet: &[u8]) -> bool {
    packet.len() >= 4 && packet[2] & 0x02 != 0
}

/// The resolvers to ask, in the order they are to be asked.
///
/// The caller's own comes first if it named one, which is what
/// `nameServer` is for: a function that knows which resolver holds the
/// answer, and a test that is holding one itself.
async fn resolvers(named_by_caller: &str) -> Result<Vec<SocketAddr>, String> {
    if !named_by_caller.trim().is_empty() {
        return match addressed(named_by_caller) {
            Some(server) => Ok(vec![server]),
            None => Err(format!(
                "{named_by_caller} is not an address a resolver could be at"
            )),
        };
    }
    if let Ok(named) = std::env::var("ZOU_DNS_RESOLVER") {
        let servers: Vec<SocketAddr> = named.split(',').filter_map(addressed).collect();
        if servers.is_empty() {
            return Err(format!(
                "ZOU_DNS_RESOLVER is set to {named}, and none of that is an address a resolver could be at"
            ));
        }
        return Ok(servers);
    }
    let conf = tokio::fs::read_to_string("/etc/resolv.conf")
        .await
        .map_err(|e| {
            format!(
                "this host has no resolver to ask: /etc/resolv.conf: {e}. Name ZOU_DNS_RESOLVER to say which one to use"
            )
        })?;
    let servers: Vec<SocketAddr> = conf
        .lines()
        .map(|line| line.split('#').next().unwrap_or("").trim())
        .filter_map(|line| line.strip_prefix("nameserver"))
        .filter_map(addressed)
        .collect();
    if servers.is_empty() {
        return Err(
            "this host has no resolver to ask: /etc/resolv.conf names no nameserver. Name ZOU_DNS_RESOLVER to say which one to use"
                .to_string(),
        );
    }
    Ok(servers)
}

/// A resolver as it is written, which is an address on its own or an
/// address with the port it answers on.
fn addressed(said: &str) -> Option<SocketAddr> {
    let said = said.trim();
    if said.is_empty() {
        return None;
    }
    if let Ok(addr) = said.parse::<SocketAddr>() {
        return Some(addr);
    }
    said.parse::<IpAddr>()
        .ok()
        .map(|ip| SocketAddr::new(ip, 53))
}

fn numbered(kind: &str) -> Option<u16> {
    Some(match kind {
        "A" => A,
        "AAAA" => AAAA,
        "CAA" => CAA,
        "CNAME" => CNAME,
        "MX" => MX,
        "NS" => NS,
        "PTR" => PTR,
        "SOA" => SOA,
        "SRV" => SRV,
        "TXT" => TXT,
        _ => return None,
    })
}

/// The question, which is the whole of what goes out: a header saying
/// there is one question and that the resolver is to do the recursion,
/// then the name and what is being asked about it.
fn question(name: &str, asked: u16) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(name.len() + 18);
    out.extend_from_slice(&id().to_be_bytes());
    // Recursion desired, which is the only flag a stub resolver sets.
    out.extend_from_slice(&0x0100u16.to_be_bytes());
    out.extend_from_slice(&1u16.to_be_bytes());
    out.extend_from_slice(&[0, 0, 0, 0, 0, 0]);
    written(&mut out, name)?;
    out.extend_from_slice(&asked.to_be_bytes());
    // IN, the internet class, which is the only one anybody uses.
    out.extend_from_slice(&1u16.to_be_bytes());
    Ok(out)
}

/// A name on the wire, which is its labels each with its length in
/// front of it and a zero at the end.
fn written(out: &mut Vec<u8>, name: &str) -> Result<(), String> {
    let name = name.strip_suffix('.').unwrap_or(name);
    if name.is_empty() {
        return Err("the root is not a name to ask about".to_string());
    }
    if name.len() > 253 {
        return Err(format!("{name} is longer than a name may be"));
    }
    for label in name.split('.') {
        if label.is_empty() || label.len() > 63 {
            return Err(format!(
                "{name} is not a name: one of its labels is not one"
            ));
        }
        out.push(label.len() as u8);
        out.extend_from_slice(label.as_bytes());
    }
    out.push(0);
    Ok(())
}

/// The number this question is answered under, which is what makes an
/// answer to somebody else's question not an answer to this one.
fn id() -> u16 {
    let mut bytes = [0u8; 2];
    // A host with no randomness to give is a host that cannot do TLS
    // either, and one predictable query id is the smaller of those two
    // problems.
    let _ = getrandom::fill(&mut bytes);
    u16::from_be_bytes(bytes)
}

/// Somewhere to read a packet from, which is the packet and how far
/// into it we are, with every read bounds checked.
struct Reading<'a> {
    packet: &'a [u8],
    at: usize,
}

impl<'a> Reading<'a> {
    fn new(packet: &'a [u8], at: usize) -> Reading<'a> {
        Reading { packet, at }
    }

    fn byte(&mut self) -> Option<u8> {
        let byte = *self.packet.get(self.at)?;
        self.at += 1;
        Some(byte)
    }

    fn pair(&mut self) -> Option<u16> {
        Some(u16::from_be_bytes([self.byte()?, self.byte()?]))
    }

    fn four(&mut self) -> Option<u32> {
        Some(u32::from_be_bytes([
            self.byte()?,
            self.byte()?,
            self.byte()?,
            self.byte()?,
        ]))
    }

    fn bytes(&mut self, many: usize) -> Option<&'a [u8]> {
        let taken = self.packet.get(self.at..self.at.checked_add(many)?)?;
        self.at += many;
        Some(taken)
    }

    /// A name, following the pointers a compressed one is made of.
    ///
    /// A pointer may only point backwards, at something already read,
    /// which is what makes this terminate: every jump is to a smaller
    /// offset than the one it was found at, and there are finitely many
    /// of those. A pointer that goes forwards is a packet lying about
    /// itself and is refused.
    fn name(&mut self) -> Option<String> {
        let mut name = String::new();
        let mut at = self.at;
        let mut after = None;
        // Every jump has to land before the last place we jumped from,
        // starting with the place this name began. That is what a
        // compressed name is, an earlier occurrence of the same suffix,
        // and it is also what makes this loop finish.
        let mut furthest = self.at;
        loop {
            let size = *self.packet.get(at)? as usize;
            if size & 0xc0 == 0xc0 {
                let low = *self.packet.get(at + 1)? as usize;
                let to = ((size & 0x3f) << 8) | low;
                if to >= furthest {
                    return None;
                }
                furthest = to;
                after = after.or(Some(at + 2));
                at = to;
                continue;
            }
            if size & 0xc0 != 0 {
                return None;
            }
            at += 1;
            if size == 0 {
                break;
            }
            let label = self.packet.get(at..at + size)?;
            if !name.is_empty() {
                name.push('.');
            }
            name.push_str(&String::from_utf8_lossy(label));
            at += size;
        }
        self.at = after.unwrap_or(at);
        Some(if name.is_empty() {
            ".".to_string()
        } else {
            name
        })
    }

    /// Past a name without keeping it, which is what the question at
    /// the top of an answer is.
    fn past_name(&mut self) -> Option<()> {
        self.name().map(|_| ())
    }
}

/// The answer, read into the records the type it asked about has.
fn read(packet: &[u8], question: &[u8], asked: u16, name: &str) -> Found {
    let short = || Found::Failed {
        code: "EBADRESP",
        why: format!("the answer about {name} was not a whole DNS message"),
    };
    if packet.len() < 12 || question.len() < 2 || packet[0..2] != question[0..2] {
        return Found::Failed {
            code: "EBADRESP",
            why: format!("that answer was not the answer to the question asked about {name}"),
        };
    }
    let code = packet[3] & 0x0f;
    if code != 0 {
        return Found::Failed {
            code: match code {
                1 => "EFORMERR",
                2 => "ESERVFAIL",
                3 => "ENOTFOUND",
                4 => "ENOTIMP",
                5 => "EREFUSED",
                _ => "EBADRESP",
            },
            why: format!("the resolver answered about {name} with rcode {code}"),
        };
    }
    let mut reading = Reading::new(packet, 4);
    let (Some(questions), Some(answers)) = (reading.pair(), reading.pair()) else {
        return short();
    };
    reading.at = 12;
    for _ in 0..questions {
        if reading.past_name().is_none() || reading.pair().is_none() || reading.pair().is_none() {
            return short();
        }
    }
    let mut records = Vec::new();
    for _ in 0..answers {
        if reading.past_name().is_none() {
            return short();
        }
        let (Some(kind), Some(class), Some(_ttl), Some(size)) = (
            reading.pair(),
            reading.pair(),
            reading.four(),
            reading.pair(),
        ) else {
            return short();
        };
        let Some(rdata) = reading.bytes(size as usize) else {
            return short();
        };
        // A record of another type in the answer is ordinary: a CNAME
        // in front of the addresses is the usual case. It is passed
        // over rather than being an error.
        if kind != asked || class != 1 {
            continue;
        }
        let at = reading.at - size as usize;
        if let Some(record) = record(packet, at, rdata, asked) {
            records.push(record);
        }
    }
    if records.is_empty() {
        return Found::Failed {
            code: "ENODATA",
            why: format!("{name} has no {} record", named(asked)),
        };
    }
    Found::Answers {
        records: serde_json::Value::Array(records),
    }
}

fn named(asked: u16) -> &'static str {
    match asked {
        A => "A",
        AAAA => "AAAA",
        CAA => "CAA",
        CNAME => "CNAME",
        MX => "MX",
        NS => "NS",
        PTR => "PTR",
        SOA => "SOA",
        SRV => "SRV",
        TXT => "TXT",
        _ => "unknown",
    }
}

/// One record, in the shape its type has.
///
/// The whole packet is handed in alongside the record's own bytes
/// because a name inside a record may be compressed, and a compressed
/// name is an offset into the packet rather than anything the record
/// carries.
fn record(packet: &[u8], at: usize, rdata: &[u8], asked: u16) -> Option<serde_json::Value> {
    use serde_json::{Value, json};

    Some(match asked {
        A => {
            let octets: [u8; 4] = rdata.try_into().ok()?;
            Value::String(std::net::Ipv4Addr::from(octets).to_string())
        }
        AAAA => {
            let octets: [u8; 16] = rdata.try_into().ok()?;
            Value::String(std::net::Ipv6Addr::from(octets).to_string())
        }
        CNAME | NS | PTR => Value::String(Reading::new(packet, at).name()?),
        MX => {
            let mut reading = Reading::new(packet, at);
            let preference = reading.pair()?;
            json!({ "preference": preference, "exchange": reading.name()? })
        }
        TXT => {
            // A TXT record is a list of strings rather than one string,
            // and node hands back the list: a value longer than 255
            // bytes arrives as several and the package that put it
            // there is the one that knows how to join them.
            let mut said = Vec::new();
            let mut reading = Reading::new(rdata, 0);
            while reading.at < rdata.len() {
                let size = reading.byte()? as usize;
                said.push(Value::String(
                    String::from_utf8_lossy(reading.bytes(size)?).into_owned(),
                ));
            }
            Value::Array(said)
        }
        SRV => {
            let mut reading = Reading::new(packet, at);
            let priority = reading.pair()?;
            let weight = reading.pair()?;
            let port = reading.pair()?;
            json!({
                "priority": priority,
                "weight": weight,
                "port": port,
                "target": reading.name()?,
            })
        }
        SOA => {
            let mut reading = Reading::new(packet, at);
            let mname = reading.name()?;
            let rname = reading.name()?;
            json!({
                "mname": mname,
                "rname": rname,
                "serial": reading.four()?,
                "refresh": reading.four()?,
                "retry": reading.four()?,
                "expire": reading.four()?,
                "minimum": reading.four()?,
            })
        }
        CAA => {
            let mut reading = Reading::new(rdata, 0);
            let flags = reading.byte()?;
            let size = reading.byte()? as usize;
            let tag = String::from_utf8_lossy(reading.bytes(size)?).into_owned();
            let value =
                String::from_utf8_lossy(reading.bytes(rdata.len() - reading.at)?).into_owned();
            json!({ "critical": flags & 0x80 != 0, "tag": tag, "value": value })
        }
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A packet built the way a resolver builds one, so the reading
    /// below is reading a real answer rather than the writing above
    /// read back.
    fn answering(id: [u8; 2], answers: &[(&str, u16, Vec<u8>)]) -> Vec<u8> {
        let mut packet = Vec::new();
        packet.extend_from_slice(&id);
        // An answer, with recursion available.
        packet.extend_from_slice(&0x8180u16.to_be_bytes());
        packet.extend_from_slice(&1u16.to_be_bytes());
        packet.extend_from_slice(&(answers.len() as u16).to_be_bytes());
        packet.extend_from_slice(&[0, 0, 0, 0]);
        written(&mut packet, "example.com").expect("a name");
        packet.extend_from_slice(&15u16.to_be_bytes());
        packet.extend_from_slice(&1u16.to_be_bytes());
        for (name, kind, rdata) in answers {
            written(&mut packet, name).expect("a name");
            packet.extend_from_slice(&kind.to_be_bytes());
            packet.extend_from_slice(&1u16.to_be_bytes());
            packet.extend_from_slice(&300u32.to_be_bytes());
            packet.extend_from_slice(&(rdata.len() as u16).to_be_bytes());
            packet.extend_from_slice(rdata);
        }
        packet
    }

    fn mx(preference: u16, exchange: &str) -> Vec<u8> {
        let mut rdata = preference.to_be_bytes().to_vec();
        written(&mut rdata, exchange).expect("a name");
        rdata
    }

    #[test]
    fn the_question_is_the_name_and_what_is_being_asked_about_it() {
        let asked = question("mail.example.com", MX).expect("a question");
        assert_eq!(&asked[2..4], &0x0100u16.to_be_bytes(), "recursion desired");
        assert_eq!(&asked[4..6], &1u16.to_be_bytes(), "one question");
        assert_eq!(
            &asked[12..],
            b"\x04mail\x07example\x03com\x00\x00\x0f\x00\x01"
        );
    }

    #[test]
    fn a_name_that_is_not_one_is_refused_before_anything_is_sent() {
        for name in ["", ".", "a..b", &"x".repeat(64), &"y.".repeat(200)] {
            assert!(question(name, A).is_err(), "{name}");
        }
    }

    /// The record a mail sender is asking for, which is the reason this
    /// module exists.
    #[test]
    fn an_mx_answer_is_read_into_its_preference_and_its_exchange() {
        let question = question("example.com", MX).expect("a question");
        let packet = answering(
            [question[0], question[1]],
            &[
                ("example.com", MX, mx(10, "in1.example.com")),
                ("example.com", MX, mx(20, "in2.example.com")),
            ],
        );
        let Found::Answers { records } = read(&packet, &question, MX, "example.com") else {
            panic!("no answers");
        };
        assert_eq!(
            records,
            serde_json::json!([
                { "preference": 10, "exchange": "in1.example.com" },
                { "preference": 20, "exchange": "in2.example.com" },
            ])
        );
    }

    /// Every answer to somebody else's question is not an answer to
    /// this one, which is the whole of what the id is for.
    #[test]
    fn an_answer_to_another_question_is_not_this_answer() {
        let question = question("example.com", MX).expect("a question");
        let packet = answering(
            [question[0] ^ 0xff, question[1]],
            &[("example.com", MX, mx(10, "in1.example.com"))],
        );
        let Found::Failed { code, .. } = read(&packet, &question, MX, "example.com") else {
            panic!("that was somebody else's answer");
        };
        assert_eq!(code, "EBADRESP");
    }

    /// A name that exists with no record of the type asked about is a
    /// different thing from a name that does not exist, and node's two
    /// codes for them are the two a package branches on.
    #[test]
    fn nothing_of_that_type_and_no_such_name_are_two_answers() {
        let question = question("example.com", MX).expect("a question");
        let empty = answering([question[0], question[1]], &[]);
        let Found::Failed { code, .. } = read(&empty, &question, MX, "example.com") else {
            panic!("there was nothing there");
        };
        assert_eq!(code, "ENODATA");

        let mut missing = empty.clone();
        missing[3] |= 3;
        let Found::Failed { code, .. } = read(&missing, &question, MX, "example.com") else {
            panic!("there is no such name");
        };
        assert_eq!(code, "ENOTFOUND");
    }

    /// A record of another type in the answer is the ordinary case, not
    /// a failure: a CNAME in front of the addresses is how half the
    /// internet is written.
    #[test]
    fn a_record_of_another_type_is_passed_over() {
        let question = question("example.com", A).expect("a question");
        let mut alias = Vec::new();
        written(&mut alias, "elsewhere.example.com").expect("a name");
        let packet = answering(
            [question[0], question[1]],
            &[
                ("example.com", CNAME, alias),
                ("elsewhere.example.com", A, vec![93, 184, 216, 34]),
            ],
        );
        let Found::Answers { records } = read(&packet, &question, A, "example.com") else {
            panic!("no answers");
        };
        assert_eq!(records, serde_json::json!(["93.184.216.34"]));
    }

    /// A compressed name is an offset into the packet, and one that
    /// points at itself or forwards is a packet that would be read
    /// forever. It is refused instead.
    #[test]
    fn a_pointer_that_does_not_point_backwards_is_refused() {
        // Two bytes of pointer, at offset zero, pointing at offset zero.
        let round = [0xc0u8, 0x00];
        assert!(Reading::new(&round, 0).name().is_none());
        let forwards = [0xc0u8, 0x04, 0x00, 0x00, 0x01, b'a', 0x00];
        assert!(Reading::new(&forwards, 0).name().is_none());
    }

    /// A backwards pointer is the ordinary case and is followed, which
    /// is what every answer larger than one record relies on.
    #[test]
    fn a_pointer_backwards_is_followed_to_the_name_it_names() {
        let mut packet = Vec::new();
        written(&mut packet, "example.com").expect("a name");
        let at = packet.len();
        packet.push(4);
        packet.extend_from_slice(b"mail");
        packet.extend_from_slice(&[0xc0, 0x00]);
        assert_eq!(
            Reading::new(&packet, at).name().as_deref(),
            Some("mail.example.com")
        );
    }

    /// A record whose length runs past the end of what arrived is a
    /// packet that was cut off, and reading it is the bug this is
    /// checked for. It says the message was not whole.
    #[test]
    fn a_record_longer_than_the_packet_is_not_read_past() {
        let question = question("example.com", MX).expect("a question");
        let mut packet = answering(
            [question[0], question[1]],
            &[("example.com", MX, mx(10, "in1.example.com"))],
        );
        packet.truncate(packet.len() - 4);
        let Found::Failed { code, .. } = read(&packet, &question, MX, "example.com") else {
            panic!("that packet was cut off");
        };
        assert_eq!(code, "EBADRESP");
    }

    /// A TXT record is a list of strings and not a string, which is
    /// what a long value arrives as and what node hands back.
    #[test]
    fn a_txt_record_is_the_strings_it_is_made_of() {
        let question = question("example.com", TXT).expect("a question");
        let mut rdata = vec![3u8];
        rdata.extend_from_slice(b"one");
        rdata.push(3);
        rdata.extend_from_slice(b"two");
        let packet = answering([question[0], question[1]], &[("example.com", TXT, rdata)]);
        let Found::Answers { records } = read(&packet, &question, TXT, "example.com") else {
            panic!("no answers");
        };
        assert_eq!(records, serde_json::json!([["one", "two"]]));
    }

    #[test]
    fn a_resolver_is_an_address_with_or_without_the_port_it_answers_on() {
        assert_eq!(
            addressed(" 10.0.0.1 "),
            Some("10.0.0.1:53".parse().expect("an address"))
        );
        assert_eq!(
            addressed("10.0.0.1:5353"),
            Some("10.0.0.1:5353".parse().expect("an address"))
        );
        assert_eq!(
            addressed("::1"),
            Some("[::1]:53".parse().expect("an address"))
        );
        assert_eq!(addressed("not an address"), None);
        assert_eq!(addressed(""), None);
    }
}
