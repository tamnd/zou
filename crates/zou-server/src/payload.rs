//! What a subscriber is told a change was.
//!
//! [`crate::pgoutput`] hands over a row as postgres wrote it: cells of
//! text, one per column, with a type oid each. What a client is owed is
//! json, and not json of this server's choosing. A `postgres_changes`
//! payload has a shape every Supabase client already parses, so an
//! integer has to arrive as a number, a boolean as a boolean, a jsonb
//! column as the json it holds rather than as a string of it, and a
//! timestamp in the format upstream sends rather than the one postgres
//! prints.
//!
//! Upstream gets there by casting in the database: walrus calls
//! `to_jsonb(value::type)` per column, which makes postgres's own
//! json conversion the definition of what a payload holds. Doing that
//! per row is a query per row, so this does the conversion here and
//! takes `to_jsonb` as the specification: every rule below is a rule
//! about matching what that function would have returned, and the live
//! tests check it by asking postgres for `to_jsonb(t.*)` and comparing.
//!
//! What that conversion needs beyond the row is the type's name, which
//! the message does not carry: pgoutput sends an oid. So there is a
//! catalog here, filled from `pg_type` once per type ever seen, which
//! is also where a domain is resolved to what it is underneath and an
//! array to what it is an array of.
//!
//! Three places where matching upstream means not doing the obvious
//! thing, all of them proved in the tests:
//!
//! A bytea arrives from postgres as `\x0102` and upstream sends
//! `0102`, because wal2json writes hex without the prefix. The prefix
//! comes off.
//!
//! An old record under the default replica identity is the key and
//! nothing else. Postgres pads the rest of that tuple with nulls,
//! which would be a payload saying every other column became null, so
//! the padding is dropped rather than sent. That cut and the one a
//! delete under row level security gets are different cuts, which the
//! `keep` below is about.
//!
//! A row too large to send is not an error and not a truncation. It is
//! the payload with every value over sixty four bytes left out and an
//! `errors` entry saying so, which is what upstream does and what the
//! clients already handle.

use std::collections::HashMap;

use serde_json::{Map, Value};

use crate::pgoutput::{Cell, Change, Op, Relation};

/// How big a change can be before its values are left out, which is
/// upstream's `max_record_bytes` and its default.
pub const MOST: usize = 1024 * 1024;

/// How much of a value survives a change that was too large, which is
/// upstream's number and small enough that what survives is an id
/// rather than a paragraph.
const KEPT: usize = 64;

/// What upstream puts in `errors` when the change was too large.
const TOO_LARGE: &str = "Error 413: Payload Too Large";

/// What upstream says about a change to a table with no primary key,
/// which it cannot check against a policy because it has no way to name
/// the row that changed.
pub const NO_KEY: &str = "Error 400: Bad Request, no primary key";

/// What upstream says when the subscriber may not select the columns
/// that identify the row, which is the same problem seen from the other
/// side.
pub const UNAUTHORIZED: &str = "Error 401: Unauthorized";

/// What one subscriber may see of one change.
///
/// A payload is not the same for everybody. Column privileges decide
/// which columns are in it, row level security decides whether there is
/// one at all, and a delete under row level security is published as
/// the key alone because there is no row left to check a policy
/// against. All three are the database's answers rather than this
/// server's, and [`crate::visible`] is where they are asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Seen {
    /// Whether this subscriber is told about the change at all.
    pub row: bool,
    /// Which of the relation's columns this subscriber may select, in
    /// the relation's own order.
    pub columns: Vec<bool>,
    /// Which of the relation's columns are the primary key, in the same
    /// order, as the catalog has it rather than as the change flags it.
    pub keys: Vec<bool>,
    /// Whether the old record is cut down to the key, which is what
    /// upstream does with a delete on a table that has row level
    /// security on it.
    pub keys_only: bool,
    /// What to say instead of a record, when there is a reason there is
    /// no record rather than a policy that refused one.
    pub error: Option<&'static str>,
}

impl Seen {
    /// Everything, which is what a table with no row level security on
    /// it and no column privileges revoked comes to.
    pub fn all(relation: &Relation) -> Seen {
        Seen {
            row: true,
            columns: vec![true; relation.columns.len()],
            keys: relation.columns.iter().map(|column| column.key).collect(),
            keys_only: false,
            error: None,
        }
    }
}

/// How a value of some type turns into json.
///
/// This is `to_jsonb`'s behaviour for that type, grouped: everything
/// postgres writes as a json number, everything it writes as a bare
/// json value, and the long tail that is a string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    /// A json number, which is every integer, float and numeric.
    Number,
    /// `true` or `false`, from postgres's `t` and `f`.
    Bool,
    /// json and jsonb, which are already json and go through whole.
    Json,
    /// A bytea, which upstream sends as hex without the `\x`.
    Bytes,
    /// A timestamp, which postgres prints with a space and json wants
    /// with a `T`.
    Stamp,
    /// An array, carrying what it is an array of.
    Array(u32),
    /// A json string, which is text and uuid and dates and everything
    /// else postgres has an output function for.
    Text,
}

/// A type, as much of it as a payload needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Kind {
    /// What `pg_type` calls it, which is what goes in the payload's
    /// `columns`. For a domain this is the domain's own name, since
    /// that is the name the column has.
    pub name: String,
    pub shape: Shape,
    /// What separates two elements when this is an array's element
    /// type, which is a comma for everything except `box`.
    pub delim: u8,
}

/// The types seen so far.
///
/// One of these per tap. A relation names its columns' oids and
/// nothing else, so the first time a table publishes a row this has to
/// ask the database what those oids are; every row after that is a
/// hash lookup. Types do not change under an oid, so nothing here ever
/// goes stale: a `drop type` frees the oid but a relation that used it
/// is gone too.
#[derive(Debug, Default)]
pub struct Types {
    kinds: HashMap<u32, Kind>,
}

impl Types {
    pub fn new() -> Types {
        Types::default()
    }

    /// The oids on this relation that have not been looked up, which
    /// is what a caller hands to [`Types::learn`].
    pub fn missing(&self, relation: &Relation) -> Vec<u32> {
        let mut want: Vec<u32> = relation
            .columns
            .iter()
            .map(|c| c.type_oid)
            .filter(|oid| !self.kinds.contains_key(oid))
            .collect();
        want.sort_unstable();
        want.dedup();
        want
    }

    /// Look up types, and whatever those types are made of.
    ///
    /// A domain is asked about again as its base type and an array as
    /// its element type, because neither can be turned into json
    /// without the thing underneath it. That is a second round trip on
    /// the first sighting of a domain and none after.
    pub async fn learn(
        &mut self,
        client: &tokio_postgres::Client,
        oids: &[u32],
    ) -> Result<(), String> {
        let mut want: Vec<i64> = oids
            .iter()
            .filter(|oid| !self.kinds.contains_key(oid))
            .map(|oid| *oid as i64)
            .collect();
        let mut domains: Vec<(u32, u32)> = Vec::new();
        // A domain of a domain is legal, so this is a loop rather than
        // two queries. It ends because every round asks about types it
        // has not seen and there are finitely many.
        while !want.is_empty() {
            let rows = client
                .query(
                    "select t.oid::int8, t.typname, t.typtype::text, t.typbasetype::int8, \
                     t.typelem::int8, t.typcategory::text, t.typdelim::text \
                     from pg_type t where t.oid = any($1::int8[])",
                    &[&want],
                )
                .await
                .map_err(|e| e.to_string())?;
            let mut next = Vec::new();
            for row in &rows {
                let oid: i64 = row.get(0);
                let name: String = row.get(1);
                let typtype: String = row.get(2);
                let base: i64 = row.get(3);
                let elem: i64 = row.get(4);
                let category: String = row.get(5);
                let delim: String = row.get(6);
                let shape = if typtype == "d" {
                    // A domain has the shape of what it is a domain of,
                    // which may not have been looked up yet. It is
                    // remembered and settled once everything is in.
                    next.push(base);
                    domains.push((oid as u32, base as u32));
                    Shape::Text
                } else if category == "A" && elem != 0 {
                    next.push(elem);
                    Shape::Array(elem as u32)
                } else {
                    shape(oid as u32)
                };
                self.kinds.insert(
                    oid as u32,
                    Kind {
                        name,
                        shape,
                        delim: delim.as_bytes().first().copied().unwrap_or(b','),
                    },
                );
            }
            want = next
                .into_iter()
                .filter(|oid| *oid != 0 && !self.kinds.contains_key(&(*oid as u32)))
                .collect();
            want.sort_unstable();
            want.dedup();
        }
        self.settle(&domains);
        Ok(())
    }

    /// Give every domain the shape of what it is a domain of, now that
    /// everything underneath has been looked up.
    ///
    /// A pass per level of nesting, deepest last, which for the domain
    /// of a domain of an int takes two. The bound is what stops a
    /// catalog that somehow describes a cycle from being read forever.
    fn settle(&mut self, domains: &[(u32, u32)]) {
        for _ in 0..domains.len().min(16) {
            let mut moved = false;
            for (oid, base) in domains {
                let shape = match self.kinds.get(base) {
                    Some(kind) => kind.shape,
                    None => continue,
                };
                if let Some(kind) = self.kinds.get_mut(oid)
                    && kind.shape != shape
                {
                    kind.shape = shape;
                    moved = true;
                }
            }
            if !moved {
                break;
            }
        }
    }

    pub fn get(&self, oid: u32) -> Option<&Kind> {
        self.kinds.get(&oid)
    }

    /// What to call a type nothing looked up, which is a diagnosable
    /// name rather than a guess at what the type was.
    fn name(&self, oid: u32) -> String {
        match self.kinds.get(&oid) {
            Some(kind) => kind.name.clone(),
            None => format!("oid_{oid}"),
        }
    }

    fn shape(&self, oid: u32) -> Shape {
        match self.kinds.get(&oid) {
            Some(kind) => kind.shape,
            // Nothing looked it up, so the honest conversion is the one
            // that cannot be wrong about what the value means.
            None => Shape::Text,
        }
    }
}

/// What `to_jsonb` does with a type, by oid, for the types that are
/// not a string.
///
/// By oid rather than by name because these are the oids postgres
/// pins: they are the same in every installation and are what a
/// pgoutput message carries. An extension's types are allocated at
/// install time and have no fixed oid, and none of them is a number or
/// a boolean, so they are the string every unlisted type is.
fn shape(oid: u32) -> Shape {
    match oid {
        // int2, int4, int8, oid, float4, float8, numeric.
        21 | 23 | 20 | 26 | 700 | 701 | 1700 => Shape::Number,
        16 => Shape::Bool,
        // json, jsonb.
        114 | 3802 => Shape::Json,
        17 => Shape::Bytes,
        // timestamp, timestamptz.
        1114 | 1184 => Shape::Stamp,
        _ => Shape::Text,
    }
}

/// One change, as the `data` half of a `postgres_changes` payload.
///
/// The other half is the ids of whoever asked for it, which is the
/// transport's to fill in.
///
/// This is per subscriber rather than per change, because `seen` is:
/// two subscribers on the same row with different column privileges
/// are owed different payloads, and upstream builds one per role for
/// the same reason. Where they are owed the same one, which is the
/// common case of a table nobody has revoked anything on, `seen` is
/// equal and so is the result, so the caller can build it once.
pub fn data(change: &Change, types: &Types, seen: &Seen) -> Value {
    let relation = &change.relation;
    let mut data = Map::new();
    data.insert("schema".into(), relation.schema.clone().into());
    data.insert("table".into(), relation.table.clone().into());
    data.insert("type".into(), change.op.event().into());

    // A change nothing can be said about is still sent, with the
    // reason in it and nothing else. Upstream builds the same three
    // keys and lets the rest default, which is how a client comes to
    // see an empty record next to an error rather than silence.
    if let Some(why) = seen.error {
        data.insert("commit_timestamp".into(), Value::Null);
        data.insert("columns".into(), Value::Array(Vec::new()));
        if change.op != Op::Delete {
            data.insert("record".into(), Value::Object(Map::new()));
        }
        if change.op != Op::Insert {
            data.insert("old_record".into(), Value::Object(Map::new()));
        }
        data.insert("errors".into(), Value::Array(vec![why.into()]));
        return Value::Object(data);
    }

    data.insert("commit_timestamp".into(), stamp(change.commit_ts).into());
    data.insert("columns".into(), columns(relation, types, seen));

    let large = weight(change) > MOST;
    match change.op {
        Op::Insert => {
            data.insert(
                "record".into(),
                row(
                    relation,
                    &change.record,
                    change.old.as_deref(),
                    None,
                    types,
                    seen,
                    large,
                ),
            );
        }
        Op::Update => {
            data.insert(
                "record".into(),
                row(
                    relation,
                    &change.record,
                    change.old.as_deref(),
                    None,
                    types,
                    seen,
                    large,
                ),
            );
            data.insert("old_record".into(), old(change, types, seen, large));
        }
        Op::Delete => {
            data.insert("old_record".into(), old(change, types, seen, large));
        }
    }
    data.insert(
        "errors".into(),
        match large {
            true => Value::Array(vec![TOO_LARGE.into()]),
            false => Value::Null,
        },
    );
    Value::Object(data)
}

/// The names and types of the table's columns, in the order postgres
/// declared them, which is what a client uses to know what it is
/// looking at.
fn columns(relation: &Relation, types: &Types, seen: &Seen) -> Value {
    Value::Array(
        relation
            .columns
            .iter()
            .enumerate()
            .filter(|(at, _)| seen.columns.get(*at).copied().unwrap_or(false))
            .map(|(_, column)| {
                let mut one = Map::new();
                one.insert("name".into(), column.name.clone().into());
                one.insert("type".into(), types.name(column.type_oid).into());
                Value::Object(one)
            })
            .collect(),
    )
}

/// The row before the change, which is only as much of it as the
/// table's replica identity publishes.
fn old(change: &Change, types: &Types, seen: &Seen, large: bool) -> Value {
    match &change.old {
        Some(cells) => row(
            &change.relation,
            cells,
            None,
            keep(change, seen).as_deref(),
            types,
            seen,
            large,
        ),
        // Postgres published nothing about the row that was there,
        // which under the default replica identity is what an update
        // that left the key alone looks like: the key did not move, so
        // there was no old tuple worth writing down.
        //
        // Upstream still says which row it was, and it can, because the
        // key it would have named is in the new row unchanged. So an
        // update carries the key here too, taken from the record, and
        // that is what a client comparing old and new is given to
        // compare on. A delete is not this case, because a delete
        // always publishes a tuple.
        None if change.op == Op::Update && change.relation.columns.iter().any(|c| c.key) => {
            let keys: Vec<bool> = change
                .relation
                .columns
                .iter()
                .enumerate()
                .map(|(at, column)| {
                    column.key && (!seen.keys_only || seen.keys.get(at).copied().unwrap_or(false))
                })
                .collect();
            row(
                &change.relation,
                &change.record,
                None,
                Some(&keys),
                types,
                seen,
                large,
            )
        }
        // A table with nothing to name a row by, where upstream sends
        // an empty object rather than a null, which is the difference
        // between a client seeing nothing and a client seeing that
        // there is nothing.
        None => Value::Object(Map::new()),
    }
}

/// Which columns of an old row are kept, or all of them.
///
/// Two cuts land in the same place and they are not the same cut. The
/// change's own flags say which cells postgres actually sent, so under
/// the default replica identity everything else in that tuple is null
/// padding rather than a value. `keys_only` is a security cut, made on
/// the key the catalog has, and it is the one that decides what a
/// subscriber learns from a delete they were not allowed to read.
///
/// The difference shows on a table with `replica identity full` and row
/// level security on, where the flags mark every column and the catalog
/// marks the key: cutting on the flags there would publish the whole
/// deleted row to everybody.
fn keep(change: &Change, seen: &Seen) -> Option<Vec<bool>> {
    if !change.old_key && !seen.keys_only {
        return None;
    }
    Some(
        change
            .relation
            .columns
            .iter()
            .enumerate()
            .map(|(at, column)| {
                (!change.old_key || column.key)
                    && (!seen.keys_only || seen.keys.get(at).copied().unwrap_or(false))
            })
            .collect(),
    )
}

/// A row of cells as the object a client reads.
///
/// `fallback` is the old row, which is where an unchanged toasted
/// value is found when the table publishes one. `keep` is the cut from
/// [`keep`], and nothing outside it is in the payload.
fn row(
    relation: &Relation,
    cells: &[Cell],
    fallback: Option<&[Cell]>,
    keep: Option<&[bool]>,
    types: &Types,
    seen: &Seen,
    large: bool,
) -> Value {
    let mut out = Map::new();
    for (at, column) in relation.columns.iter().enumerate() {
        if let Some(keep) = keep
            && !keep.get(at).copied().unwrap_or(false)
        {
            continue;
        }
        if !seen.columns.get(at).copied().unwrap_or(false) {
            continue;
        }
        let cell = match cells.get(at) {
            Some(Cell::Unchanged) => match fallback.and_then(|old| old.get(at)) {
                // Postgres left the value out because nobody wrote it,
                // and the old row has what it still is.
                Some(had) if *had != Cell::Unchanged => had,
                // Nothing has it, so the payload says nothing about it
                // rather than saying it is null.
                _ => continue,
            },
            Some(cell) => cell,
            None => continue,
        };
        let value = match cell {
            Cell::Null => Value::Null,
            Cell::Unchanged => continue,
            Cell::Text(text) => {
                if large && text.len() > KEPT {
                    continue;
                }
                value(text, types.shape(column.type_oid), types)
            }
            Cell::Bytes(bytes) => {
                if large && bytes.len() > KEPT {
                    continue;
                }
                Value::String(hex(bytes))
            }
        };
        out.insert(column.name.clone(), value);
    }
    Value::Object(out)
}

/// What the change would cost to send, near enough to decide whether
/// it is too large.
///
/// Upstream measures the json wal2json produced, which is the values
/// plus the names plus the punctuation of a shape that is not this
/// one. This measures the values, which is what a large change is
/// large because of.
fn weight(change: &Change) -> usize {
    let mut bytes = 0;
    for cells in [Some(&change.record), change.old.as_ref()]
        .into_iter()
        .flatten()
    {
        for cell in cells {
            bytes += match cell {
                Cell::Text(text) => text.len(),
                Cell::Bytes(raw) => raw.len() * 2,
                _ => 0,
            };
        }
    }
    bytes
}

/// One value, as `to_jsonb` of that type would have written it.
fn value(text: &str, shape: Shape, types: &Types) -> Value {
    match shape {
        Shape::Number => match serde_json::from_str::<serde_json::Number>(text) {
            Ok(number) => Value::Number(number),
            // NaN and infinity are numbers postgres has and json does
            // not, and postgres itself refuses to put them in a jsonb.
            // A string is the answer that loses least.
            Err(_) => Value::String(text.to_string()),
        },
        Shape::Bool => Value::Bool(text == "t" || text == "true"),
        Shape::Json => {
            serde_json::from_str(text).unwrap_or_else(|_| Value::String(text.to_string()))
        }
        Shape::Bytes => Value::String(text.strip_prefix("\\x").unwrap_or(text).to_string()),
        Shape::Stamp => Value::String(iso(text)),
        Shape::Array(elem) => array(text, elem, types),
        Shape::Text => Value::String(text.to_string()),
    }
}

/// A timestamp as json writes it rather than as postgres prints it.
///
/// The two differ in exactly two places: the date and the time are
/// separated by a space rather than a `T`, and the zone offset is
/// printed without its minutes when they are zero. Anything that does
/// not look like a timestamp, which is `infinity` and `-infinity`,
/// goes through as it came.
fn iso(text: &str) -> String {
    let Some(space) = text.find(' ') else {
        return text.to_string();
    };
    let mut out = String::with_capacity(text.len() + 3);
    out.push_str(&text[..space]);
    out.push('T');
    let rest = &text[space + 1..];
    out.push_str(rest);
    // An offset postgres wrote as +07 is +07:00 in json, and one it
    // wrote with minutes already has them.
    let sign = rest.rfind(['+', '-']);
    if let Some(at) = sign {
        let zone = &rest[at + 1..];
        if zone.len() == 2 && zone.bytes().all(|b| b.is_ascii_digit()) {
            out.push_str(":00");
        }
    }
    out
}

/// A postgres array literal as the json array `to_jsonb` would make of
/// it.
///
/// The literal is braces, commas, and quotes around anything that
/// contains one of those, with a backslash escaping the next
/// character. An unquoted `NULL` is a null and a quoted one is the
/// four letters. Multiple dimensions nest, and a lower bound other
/// than one is written as a `[0:2]=` prefix that says nothing json can
/// carry, so it is read past.
///
/// Anything that does not parse comes back as the string it was, which
/// is a payload that is wrong in the way a client can see rather than
/// a change nobody receives.
fn array(text: &str, elem: u32, types: &Types) -> Value {
    let shape = types.shape(elem);
    let delim = types.get(elem).map(|kind| kind.delim).unwrap_or(b',');
    let body = match text.starts_with('[') {
        true => match text.find('=') {
            Some(at) => &text[at + 1..],
            None => return Value::String(text.to_string()),
        },
        false => text,
    };
    let bytes = body.as_bytes();
    let mut at = 0;
    match items(bytes, &mut at, delim, shape, types) {
        Some(value) if at == bytes.len() => value,
        _ => Value::String(text.to_string()),
    }
}

/// One `{...}` of an array literal, which holds either more of them or
/// the elements themselves.
fn items(bytes: &[u8], at: &mut usize, delim: u8, shape: Shape, types: &Types) -> Option<Value> {
    if bytes.get(*at) != Some(&b'{') {
        return None;
    }
    *at += 1;
    let mut out = Vec::new();
    if bytes.get(*at) == Some(&b'}') {
        *at += 1;
        return Some(Value::Array(out));
    }
    loop {
        let item = match bytes.get(*at) {
            Some(b'{') => items(bytes, at, delim, shape, types)?,
            _ => {
                let (text, quoted) = element(bytes, at, delim)?;
                match !quoted && text.eq_ignore_ascii_case("NULL") {
                    true => Value::Null,
                    false => value(&text, shape, types),
                }
            }
        };
        out.push(item);
        match bytes.get(*at) {
            Some(b) if *b == delim => *at += 1,
            Some(b'}') => {
                *at += 1;
                return Some(Value::Array(out));
            }
            _ => return None,
        }
    }
}

/// One element of an array literal, and whether it was quoted, which
/// is what tells a null from the word null.
fn element(bytes: &[u8], at: &mut usize, delim: u8) -> Option<(String, bool)> {
    let mut out = String::new();
    let quoted = bytes.get(*at) == Some(&b'"');
    if quoted {
        *at += 1;
        loop {
            match bytes.get(*at)? {
                b'\\' => {
                    *at += 1;
                    out.push(*bytes.get(*at)? as char);
                    *at += 1;
                }
                b'"' => {
                    *at += 1;
                    return Some((out, true));
                }
                _ => {
                    let from = *at;
                    while let Some(b) = bytes.get(*at) {
                        if *b == b'\\' || *b == b'"' {
                            break;
                        }
                        *at += 1;
                    }
                    out.push_str(std::str::from_utf8(&bytes[from..*at]).ok()?);
                }
            }
        }
    }
    let from = *at;
    while let Some(b) = bytes.get(*at) {
        if *b == delim || *b == b'}' {
            break;
        }
        *at += 1;
    }
    Some((
        std::str::from_utf8(&bytes[from..*at]).ok()?.to_string(),
        false,
    ))
}

/// Bytes as the hex a payload carries, which is lowercase and has no
/// prefix on it.
fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// When the transaction committed, in the format upstream sends:
/// milliseconds, no offset, and a `Z` on the end.
///
/// Millisecond precision is upstream's `to_char` format string rather
/// than a rounding decision here, and the microseconds postgres has
/// are dropped rather than rounded, which is what truncation in that
/// format does.
fn stamp(micros: i64) -> String {
    let millis = micros.div_euclid(1_000);
    let secs = millis.div_euclid(1_000);
    let ms = millis.rem_euclid(1_000);
    let days = secs.div_euclid(86_400);
    let time = secs.rem_euclid(86_400);
    let (year, month, day) = crate::smtp::civil(days);
    let (hour, minute, second) = (time / 3600, (time / 60) % 60, time % 60);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{ms:03}Z")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pgoutput::{Column, Replica};
    use std::sync::Arc;

    fn types() -> Types {
        let mut types = Types::new();
        for (oid, name) in [
            (23u32, "int4"),
            (20, "int8"),
            (25, "text"),
            (16, "bool"),
            (17, "bytea"),
            (3802, "jsonb"),
            (1184, "timestamptz"),
            (1114, "timestamp"),
            (1700, "numeric"),
            (2950, "uuid"),
        ] {
            types.kinds.insert(
                oid,
                Kind {
                    name: name.to_string(),
                    shape: shape(oid),
                    delim: b',',
                },
            );
        }
        types.kinds.insert(
            1009,
            Kind {
                name: "_text".to_string(),
                shape: Shape::Array(25),
                delim: b',',
            },
        );
        types.kinds.insert(
            1007,
            Kind {
                name: "_int4".to_string(),
                shape: Shape::Array(23),
                delim: b',',
            },
        );
        types
    }

    fn todos() -> Arc<Relation> {
        Arc::new(Relation {
            oid: 16_384,
            schema: "public".into(),
            table: "todos".into(),
            replica: Replica::Default,
            columns: vec![
                Column {
                    name: "id".into(),
                    type_oid: 23,
                    key: true,
                },
                Column {
                    name: "details".into(),
                    type_oid: 25,
                    key: false,
                },
                Column {
                    name: "done".into(),
                    type_oid: 16,
                    key: false,
                },
            ],
        })
    }

    fn change(op: Op, record: Vec<Cell>, old: Option<Vec<Cell>>, old_key: bool) -> Change {
        Change {
            relation: todos(),
            op,
            record,
            old,
            old_key,
            commit_ts: 1_636_132_851_524_060,
            lsn: 42,
        }
    }

    fn text(of: &str) -> Cell {
        Cell::Text(of.to_string())
    }

    #[test]
    fn an_insert_is_the_shape_every_client_already_parses() {
        let change = change(
            Op::Insert,
            vec![text("12"), text("wash up"), text("f")],
            None,
            false,
        );
        assert_eq!(
            data(&change, &types(), &Seen::all(&change.relation)),
            serde_json::json!({
                "schema": "public",
                "table": "todos",
                "commit_timestamp": "2021-11-05T17:20:51.524Z",
                "type": "INSERT",
                "columns": [
                    {"name": "id", "type": "int4"},
                    {"name": "details", "type": "text"},
                    {"name": "done", "type": "bool"},
                ],
                "record": {"id": 12, "details": "wash up", "done": false},
                "errors": null,
            })
        );
    }

    #[test]
    fn a_delete_says_what_the_row_was_and_not_what_it_became() {
        let change = change(
            Op::Delete,
            vec![],
            Some(vec![text("12"), Cell::Null, Cell::Null]),
            true,
        );
        let data = data(&change, &types(), &Seen::all(&change.relation));
        assert_eq!(data["type"], "DELETE");
        assert_eq!(data.get("record"), None, "a deleted row has no record");
        assert_eq!(
            data["old_record"],
            serde_json::json!({"id": 12}),
            "postgres pads the key tuple with nulls and a payload that carried them would say \
             every other column became null"
        );
    }

    #[test]
    fn an_update_of_a_table_that_publishes_its_old_row_carries_both() {
        let change = change(
            Op::Update,
            vec![text("12"), text("washed up"), text("t")],
            Some(vec![text("12"), text("wash up"), text("f")]),
            false,
        );
        let data = data(&change, &types(), &Seen::all(&change.relation));
        assert_eq!(
            data["record"],
            serde_json::json!({"id": 12, "details": "washed up", "done": true})
        );
        assert_eq!(
            data["old_record"],
            serde_json::json!({"id": 12, "details": "wash up", "done": false})
        );
    }

    /// The ordinary update, which is the one most tables get: default
    /// replica identity, a key nobody touched, and so no old tuple in
    /// the write ahead log at all.
    #[test]
    fn an_update_that_published_no_old_row_still_says_which_row_it_was() {
        let change = change(
            Op::Update,
            vec![text("12"), text("washed up"), text("t")],
            None,
            false,
        );
        assert_eq!(
            data(&change, &types(), &Seen::all(&change.relation))["old_record"],
            serde_json::json!({"id": 12}),
            "the key did not move, so it is in the new row, and upstream names the row rather \
             than sending an empty object"
        );

        let mut seen = Seen::all(&change.relation);
        seen.keys_only = true;
        seen.keys = vec![true, false, false];
        assert_eq!(
            data(&change, &types(), &seen)["old_record"],
            serde_json::json!({"id": 12}),
            "and the cut a policy makes is the same cut, since the key is all there was"
        );
    }

    #[test]
    fn a_toasted_column_nobody_wrote_comes_from_the_old_row_or_not_at_all() {
        let with = change(
            Op::Update,
            vec![text("12"), Cell::Unchanged, text("t")],
            Some(vec![text("12"), text("the long one"), text("f")]),
            false,
        );
        assert_eq!(
            data(&with, &types(), &Seen::all(&with.relation))["record"]["details"],
            "the long one",
            "the value did not change, so the old row is what it still is"
        );

        let without = change(
            Op::Update,
            vec![text("12"), Cell::Unchanged, text("t")],
            None,
            false,
        );
        let record = &data(&without, &types(), &Seen::all(&without.relation))["record"];
        assert_eq!(record.get("details"), None, "nothing knows what it is");
        assert_eq!(record["id"], 12);
    }

    #[test]
    fn a_column_a_subscriber_may_not_select_is_in_neither_half_of_the_payload() {
        let change = change(
            Op::Update,
            vec![text("12"), text("washed up"), text("t")],
            Some(vec![text("12"), text("wash up"), text("f")]),
            false,
        );
        let seen = Seen {
            row: true,
            columns: vec![true, false, true],
            keys: vec![true, false, false],
            keys_only: false,
            error: None,
        };
        let data = data(&change, &types(), &seen);
        assert_eq!(
            data["columns"],
            serde_json::json!([
                {"name": "id", "type": "int4"},
                {"name": "done", "type": "bool"},
            ]),
            "the metadata says what the payload has in it and not what the table has in it"
        );
        assert_eq!(
            data["record"],
            serde_json::json!({"id": 12, "done": true}),
            "a grant of select on some columns is a grant of those columns"
        );
        assert_eq!(
            data["old_record"],
            serde_json::json!({"id": 12, "done": false}),
            "the old row is the same row and the same privileges"
        );
    }

    #[test]
    fn a_change_nothing_can_be_checked_against_says_why_and_carries_nothing() {
        let change = change(
            Op::Insert,
            vec![text("12"), text("wash up"), text("f")],
            None,
            false,
        );
        let mut seen = Seen::all(&change.relation);
        seen.error = Some(NO_KEY);
        assert_eq!(
            data(&change, &types(), &seen),
            serde_json::json!({
                "schema": "public",
                "table": "todos",
                "type": "INSERT",
                "commit_timestamp": null,
                "columns": [],
                "record": {},
                "errors": ["Error 400: Bad Request, no primary key"],
            }),
            "the containers are empty rather than absent, which is what upstream's own coalesce \
             leaves behind"
        );
    }

    #[test]
    fn a_delete_under_row_level_security_is_the_key_and_nothing_else() {
        let change = change(
            Op::Delete,
            vec![],
            Some(vec![text("12"), text("wash up"), text("f")]),
            false,
        );
        let mut seen = Seen::all(&change.relation);
        seen.keys_only = true;
        assert_eq!(
            data(&change, &types(), &seen)["old_record"],
            serde_json::json!({"id": 12}),
            "there is no row left to check a policy against, so what a subscriber learns is that a \
             row with that key is gone"
        );
        assert_eq!(
            data(&change, &types(), &Seen::all(&change.relation))["old_record"],
            serde_json::json!({"id": 12, "details": "wash up", "done": false}),
            "and a table with no policies on it publishes what it published"
        );
    }

    /// The one a table with `replica identity full` gets, which is the
    /// case where the change's flags and the catalog's key disagree.
    #[test]
    fn a_delete_of_a_row_that_published_all_of_itself_is_still_the_key() {
        let mut relation = Relation::clone(&todos());
        relation.replica = Replica::Full;
        for column in &mut relation.columns {
            column.key = true;
        }
        let relation = Arc::new(relation);
        let change = Change {
            relation: Arc::clone(&relation),
            op: Op::Delete,
            record: vec![],
            old: Some(vec![text("12"), text("the combination"), text("f")]),
            // Postgres sent the whole row rather than a key tuple, so
            // there is no padding to drop and nothing about the flags
            // says which column names the row.
            old_key: false,
            commit_ts: 1_636_132_851_524_060,
            lsn: 42,
        };
        let mut seen = Seen::all(&relation);
        seen.keys = vec![true, false, false];
        seen.keys_only = true;
        assert_eq!(
            data(&change, &types(), &seen)["old_record"],
            serde_json::json!({"id": 12}),
            "the flags mark every column of such a table, so a cut made on them would publish a \
             deleted row to the subscribers a policy hides it from"
        );
    }

    #[test]
    fn a_value_is_the_json_postgres_would_have_made_of_it() {
        let types = types();
        assert_eq!(value("12", Shape::Number, &types), 12);
        assert_eq!(value("1.5", Shape::Number, &types), 1.5);
        assert_eq!(
            value("NaN", Shape::Number, &types),
            "NaN",
            "json has no NaN and neither does jsonb"
        );
        assert_eq!(value("t", Shape::Bool, &types), true);
        assert_eq!(value("f", Shape::Bool, &types), false);
        assert_eq!(
            value(r#"{"a": [1]}"#, Shape::Json, &types),
            serde_json::json!({"a": [1]}),
            "a jsonb column holds json, not a string of json"
        );
        assert_eq!(
            value("\\x0102030405", Shape::Bytes, &types),
            "0102030405",
            "upstream sends what wal2json writes, which has no prefix on it"
        );
        assert_eq!(
            value("2021-11-05 17:20:51.524+00", Shape::Stamp, &types),
            "2021-11-05T17:20:51.524+00:00"
        );
        assert_eq!(
            value("2021-11-05 17:20:51.524", Shape::Stamp, &types),
            "2021-11-05T17:20:51.524"
        );
        assert_eq!(
            value("2021-11-05 17:20:51.524+05:30", Shape::Stamp, &types),
            "2021-11-05T17:20:51.524+05:30"
        );
        assert_eq!(value("infinity", Shape::Stamp, &types), "infinity");
        assert_eq!(
            value("a-uuid-shaped-thing", Shape::Text, &types),
            "a-uuid-shaped-thing"
        );
    }

    #[test]
    fn an_array_is_an_array_and_not_the_braces_postgres_wrote() {
        let types = types();
        assert_eq!(
            value("{1,2,3}", Shape::Array(23), &types),
            serde_json::json!([1, 2, 3])
        );
        assert_eq!(value("{}", Shape::Array(23), &types), serde_json::json!([]));
        assert_eq!(
            value(r#"{a,"b,c",NULL,"NULL"}"#, Shape::Array(25), &types),
            serde_json::json!(["a", "b,c", null, "NULL"]),
            "a quoted null is the word and an unquoted one is the value"
        );
        assert_eq!(
            value(
                r#"{"he said \"no\"","back\\slash"}"#,
                Shape::Array(25),
                &types
            ),
            serde_json::json!(["he said \"no\"", "back\\slash"])
        );
        assert_eq!(
            value("{{1,2},{3,4}}", Shape::Array(23), &types),
            serde_json::json!([[1, 2], [3, 4]]),
            "two dimensions nest"
        );
        assert_eq!(
            value("[0:1]={1,2}", Shape::Array(23), &types),
            serde_json::json!([1, 2]),
            "json has no lower bound to carry"
        );
        assert_eq!(
            value("{1,2", Shape::Array(23), &types),
            "{1,2",
            "what does not parse is sent as what it was rather than dropped"
        );
    }

    #[test]
    fn a_change_too_large_to_send_is_sent_without_the_large_parts() {
        let long = "x".repeat(MOST + 1);
        let change = change(
            Op::Insert,
            vec![text("12"), text(&long), text("f")],
            None,
            false,
        );
        let data = data(&change, &types(), &Seen::all(&change.relation));
        assert_eq!(
            data["errors"],
            serde_json::json!(["Error 413: Payload Too Large"])
        );
        assert_eq!(data["record"]["id"], 12, "the id is what is worth keeping");
        assert_eq!(
            data["record"].get("details"),
            None,
            "the value that made it too large is the one left out"
        );
    }

    #[test]
    fn a_commit_timestamp_is_milliseconds_and_zulu() {
        assert_eq!(stamp(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(stamp(1_636_132_851_524_060), "2021-11-05T17:20:51.524Z");
        assert_eq!(
            stamp(1_636_132_851_524_999),
            "2021-11-05T17:20:51.524Z",
            "to_char truncates the microseconds rather than rounding them"
        );
    }

    #[test]
    fn a_type_nothing_looked_up_is_a_string_and_says_so() {
        let types = Types::new();
        let change = change(
            Op::Insert,
            vec![text("12"), text("wash up"), text("f")],
            None,
            false,
        );
        let data = data(&change, &types, &Seen::all(&change.relation));
        assert_eq!(
            data["record"]["id"], "12",
            "a value of an unknown type is what postgres printed, which is never wrong about what \
             it says"
        );
        assert_eq!(data["columns"][0]["type"], "oid_23");
    }

    #[test]
    fn what_a_relation_needs_looked_up_is_asked_for_once() {
        let mut types = Types::new();
        assert_eq!(types.missing(&todos()), vec![16, 23, 25]);
        types.kinds.insert(
            23,
            Kind {
                name: "int4".into(),
                shape: Shape::Number,
                delim: b',',
            },
        );
        assert_eq!(types.missing(&todos()), vec![16, 25]);
    }
}
