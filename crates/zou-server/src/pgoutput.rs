//! What postgres says changed, decoded.
//!
//! `postgres_changes` needs the rows a transaction wrote, not the
//! pages it dirtied, and the only thing that knows how to turn one
//! into the other is postgres itself. Its logical decoder does that,
//! and `pgoutput` is the output plugin every postgres has built in, so
//! this reads that rather than asking a project to install a plugin
//! that carries json.
//!
//! There is no io here. A message goes in as the bytes postgres
//! handed over and a change comes out, which makes the whole format
//! testable without a database: the fixtures in the tests below are
//! the bytes, written by hand from the protocol documentation.
//!
//! The format is the logical replication message format, protocol
//! version 1, which is the one every postgres from 10 on speaks. Ints
//! are big endian, strings are null terminated, and timestamps are
//! microseconds since the postgres epoch, which is the first of
//! January 2000 rather than 1970.
//!
//! What is decoded is what a change event needs: begin for the commit
//! timestamp, relation for the names and the column types, and insert,
//! update and delete for the rows. Origin, type, truncate, streaming
//! and the logical decoding message are read past rather than
//! decoded, because nothing above this asks about them yet and
//! skipping a message this does not want is the difference between a
//! tap that keeps up and one that stops on the first thing it has not
//! seen.

use std::collections::HashMap;
use std::sync::Arc;

/// Microseconds between the unix epoch and the postgres one, which is
/// the first of January 2000.
const POSTGRES_EPOCH: i64 = 946_684_800_000_000;

/// What happened to a row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Insert,
    Update,
    Delete,
}

impl Op {
    /// What Supabase calls it in a `postgres_changes` payload, which
    /// is also what a client asks for in its join.
    pub fn event(&self) -> &'static str {
        match self {
            Op::Insert => "INSERT",
            Op::Update => "UPDATE",
            Op::Delete => "DELETE",
        }
    }
}

/// How much of the old row a table publishes, which is its replica
/// identity and the reason an update sometimes carries an old record
/// and sometimes carries a primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Replica {
    /// The primary key, which is postgres's default and what most
    /// tables have.
    Default,
    /// Nothing at all, so an update and a delete say only what the row
    /// became.
    Nothing,
    /// The whole row before the change, which is what a project sets
    /// when it wants `old_record` to be worth reading.
    Full,
    /// A unique index somebody named, which behaves like the default
    /// with different columns in it.
    Index,
}

impl Replica {
    fn from(byte: u8) -> Replica {
        match byte {
            b'n' => Replica::Nothing,
            b'f' => Replica::Full,
            b'i' => Replica::Index,
            _ => Replica::Default,
        }
    }
}

/// One column of a published relation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    /// The type's oid. The name a payload carries is looked up from
    /// this against pg_type, since the message does not carry one.
    pub type_oid: u32,
    /// Whether this column is part of what identifies the row, which
    /// is the primary key under the default replica identity.
    pub key: bool,
}

/// A table, as postgres described it the last time it published one of
/// its rows.
///
/// Postgres sends this once per relation per connection and then
/// refers to it by oid, so a decoder that forgot one could not read
/// the rows that follow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relation {
    pub oid: u32,
    pub schema: String,
    pub table: String,
    pub replica: Replica,
    pub columns: Vec<Column>,
}

/// One column's value in a row postgres published.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cell {
    /// The column is null.
    Null,
    /// The column was not written and is stored out of line, so
    /// postgres did not send it. Upstream carries these through as the
    /// value the client already had, and a client that never had one
    /// sees nothing.
    Unchanged,
    /// The value in its text output form, which is what pgoutput sends
    /// unless a binary connection asked otherwise.
    Text(String),
    /// The value in its binary form, which arrives only if something
    /// asked for binary and is carried rather than parsed.
    Bytes(Vec<u8>),
}

/// One row that changed, with everything a payload needs about it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    pub relation: Arc<Relation>,
    pub op: Op,
    /// The row as it is now, empty for a delete.
    pub record: Vec<Cell>,
    /// The row as it was, when the table's replica identity publishes
    /// one. Under the default identity this is the key columns and
    /// nothing else, which is why `old_key` says which it is.
    pub old: Option<Vec<Cell>>,
    /// Whether `old` is the identifying columns rather than the whole
    /// row before the change.
    pub old_key: bool,
    /// When the transaction that wrote this committed, microseconds
    /// since the unix epoch, which is what a payload's
    /// `commit_timestamp` is made from.
    pub commit_ts: i64,
    /// Where this change is in the write ahead log, which is what a
    /// reconnecting tap resumes from.
    pub lsn: u64,
}

/// The relations seen so far and the transaction being read.
///
/// One of these per tap. It is not a stream and it holds no io: the
/// caller hands it messages in the order postgres wrote them and takes
/// whatever changes come back.
#[derive(Debug, Default)]
pub struct Decoder {
    relations: HashMap<u32, Arc<Relation>>,
    /// The commit timestamp of the transaction being read, out of its
    /// begin message. Every change in a transaction carries the
    /// transaction's timestamp rather than its own, which is what
    /// upstream sends and the only timestamp postgres knows: a row has
    /// no time of its own until the transaction it is in commits.
    commit_ts: i64,
}

impl Decoder {
    pub fn new() -> Decoder {
        Decoder::default()
    }

    /// Read one message. `lsn` is where postgres said it was, which is
    /// carried through onto the change.
    ///
    /// Nothing comes back for the messages that are not rows, which is
    /// most of them: a begin is remembered, a relation is remembered,
    /// and everything else this does not read is skipped.
    pub fn message(&mut self, lsn: u64, bytes: &[u8]) -> Result<Option<Change>, String> {
        let mut read = Reader::new(bytes);
        match read.byte()? {
            b'B' => {
                // Final lsn, commit timestamp, xid. The timestamp is
                // the only part anything above this asks for.
                read.u64()?;
                self.commit_ts = unix_micros(read.i64()?);
                Ok(None)
            }
            b'R' => {
                let relation = self.relation(&mut read)?;
                self.relations.insert(relation.oid, Arc::new(relation));
                Ok(None)
            }
            b'I' => {
                let relation = self.known(read.u32()?)?;
                // The tuple is always preceded by its kind, which for
                // an insert is always the new one.
                expect(&mut read, b'N')?;
                let record = self.tuple(&mut read)?;
                Ok(Some(Change {
                    relation,
                    op: Op::Insert,
                    record,
                    old: None,
                    old_key: false,
                    commit_ts: self.commit_ts,
                    lsn,
                }))
            }
            b'U' => {
                let relation = self.known(read.u32()?)?;
                // An update carries the old row first when the
                // relation publishes one: K for the identifying
                // columns, O for the whole row. Neither is sent when
                // the identity is nothing or when the key did not
                // change, which is postgres's rule and not this one.
                let (old, old_key) = match read.byte()? {
                    b'K' => (Some(self.tuple(&mut read)?), true),
                    b'O' => (Some(self.tuple(&mut read)?), false),
                    b'N' => {
                        let record = self.tuple(&mut read)?;
                        return Ok(Some(Change {
                            relation,
                            op: Op::Update,
                            record,
                            old: None,
                            old_key: false,
                            commit_ts: self.commit_ts,
                            lsn,
                        }));
                    }
                    other => return Err(format!("an update carries {}", char::from(other))),
                };
                expect(&mut read, b'N')?;
                let record = self.tuple(&mut read)?;
                Ok(Some(Change {
                    relation,
                    op: Op::Update,
                    record,
                    old,
                    old_key,
                    commit_ts: self.commit_ts,
                    lsn,
                }))
            }
            b'D' => {
                let relation = self.known(read.u32()?)?;
                let old_key = match read.byte()? {
                    b'K' => true,
                    b'O' => false,
                    other => return Err(format!("a delete carries {}", char::from(other))),
                };
                let old = self.tuple(&mut read)?;
                Ok(Some(Change {
                    relation,
                    op: Op::Delete,
                    record: Vec::new(),
                    old: Some(old),
                    old_key,
                    commit_ts: self.commit_ts,
                    lsn,
                }))
            }
            // Commit, origin, type, truncate, and everything a newer
            // protocol version adds. A tap that stopped on one of
            // these would stop on the first project that ran truncate.
            _ => Ok(None),
        }
    }

    /// What this decoder knows about a table, which it must: postgres
    /// sends the relation before any row of it and a decoder that does
    /// not have one is a decoder that dropped a message.
    fn known(&self, oid: u32) -> Result<Arc<Relation>, String> {
        match self.relations.get(&oid) {
            Some(relation) => Ok(Arc::clone(relation)),
            None => Err(format!("relation {oid} was never described")),
        }
    }

    fn relation(&self, read: &mut Reader) -> Result<Relation, String> {
        let oid = read.u32()?;
        let schema = read.string()?;
        let table = read.string()?;
        let replica = Replica::from(read.byte()?);
        let count = read.u16()?;
        let mut columns = Vec::with_capacity(count as usize);
        for _ in 0..count {
            // The only flag there is says the column is part of the
            // replica identity.
            let key = read.byte()? & 1 == 1;
            let name = read.string()?;
            let type_oid = read.u32()?;
            // The type modifier, which is the length of a varchar and
            // the precision of a numeric. Nothing here needs it: the
            // payload carries the type's name.
            read.u32()?;
            columns.push(Column {
                name,
                type_oid,
                key,
            });
        }
        Ok(Relation {
            oid,
            schema,
            table,
            replica,
            columns,
        })
    }

    fn tuple(&self, read: &mut Reader) -> Result<Vec<Cell>, String> {
        let count = read.u16()?;
        let mut cells = Vec::with_capacity(count as usize);
        for _ in 0..count {
            cells.push(match read.byte()? {
                b'n' => Cell::Null,
                b'u' => Cell::Unchanged,
                b't' => {
                    let raw = read.bytes()?;
                    match String::from_utf8(raw) {
                        Ok(text) => Cell::Text(text),
                        // Postgres sends the text output of the type
                        // in the database's encoding, and a database
                        // that is not utf8 can send bytes that are not
                        // a rust string. Carrying them is better than
                        // failing the whole transaction over one
                        // column.
                        Err(raw) => Cell::Bytes(raw.into_bytes()),
                    }
                }
                b'b' => Cell::Bytes(read.bytes()?),
                other => return Err(format!("a column value is {}", char::from(other))),
            });
        }
        Ok(cells)
    }
}

/// The postgres epoch is the first of January 2000, and everything
/// above this counts from 1970.
fn unix_micros(postgres: i64) -> i64 {
    postgres + POSTGRES_EPOCH
}

fn expect(read: &mut Reader, want: u8) -> Result<(), String> {
    match read.byte()? {
        got if got == want => Ok(()),
        got => Err(format!(
            "expected {} and found {}",
            char::from(want),
            char::from(got)
        )),
    }
}

/// A cursor over one message, which answers a short read with a
/// sentence rather than a panic.
struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Reader<'a> {
        Reader { bytes, at: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        let end = self.at + n;
        if end > self.bytes.len() {
            return Err(format!(
                "a message of {} bytes has no {n} at {}",
                self.bytes.len(),
                self.at
            ));
        }
        let slice = &self.bytes[self.at..end];
        self.at = end;
        Ok(slice)
    }

    fn byte(&mut self) -> Result<u8, String> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, String> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().expect("two")))
    }

    fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().expect("four")))
    }

    fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().expect("eight")))
    }

    fn i64(&mut self) -> Result<i64, String> {
        Ok(self.u64()? as i64)
    }

    /// A null terminated string.
    fn string(&mut self) -> Result<String, String> {
        let start = self.at;
        while self.at < self.bytes.len() {
            if self.bytes[self.at] == 0 {
                let text = String::from_utf8_lossy(&self.bytes[start..self.at]).into_owned();
                self.at += 1;
                return Ok(text);
            }
            self.at += 1;
        }
        Err("a string with no end in it".to_string())
    }

    /// A length prefixed value.
    fn bytes(&mut self) -> Result<Vec<u8>, String> {
        let len = self.u32()? as usize;
        Ok(self.take(len)?.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The bytes postgres sends, built the way it builds them.
    #[derive(Default)]
    struct Message(Vec<u8>);

    impl Message {
        fn new(kind: u8) -> Message {
            Message(vec![kind])
        }

        fn byte(mut self, byte: u8) -> Message {
            self.0.push(byte);
            self
        }

        fn u16(mut self, n: u16) -> Message {
            self.0.extend_from_slice(&n.to_be_bytes());
            self
        }

        fn u32(mut self, n: u32) -> Message {
            self.0.extend_from_slice(&n.to_be_bytes());
            self
        }

        fn u64(mut self, n: u64) -> Message {
            self.0.extend_from_slice(&n.to_be_bytes());
            self
        }

        fn string(mut self, text: &str) -> Message {
            self.0.extend_from_slice(text.as_bytes());
            self.0.push(0);
            self
        }

        fn text(self, value: &str) -> Message {
            self.byte(b't')
                .u32(value.len() as u32)
                .string(value)
                // string() writes a terminator a length prefixed value
                // does not have, so take it off again.
                .chop()
        }

        fn chop(mut self) -> Message {
            self.0.pop();
            self
        }
    }

    /// A begin whose commit timestamp is the first of January 2000,
    /// which is zero in postgres's own count.
    fn begin(micros: i64) -> Vec<u8> {
        Message::new(b'B').u64(0).u64(micros as u64).u32(7).0
    }

    /// public.todos with an id and a title, keyed on the id.
    fn todos() -> Vec<u8> {
        Message::new(b'R')
            .u32(16_384)
            .string("public")
            .string("todos")
            .byte(b'd')
            .u16(2)
            .byte(1)
            .string("id")
            .u32(23)
            .u32(0xffff_ffff)
            .byte(0)
            .string("title")
            .u32(25)
            .u32(0xffff_ffff)
            .0
    }

    fn decoder() -> Decoder {
        let mut decoder = Decoder::new();
        assert!(decoder.message(0, &begin(0)).expect("a begin").is_none());
        assert!(decoder.message(0, &todos()).expect("a relation").is_none());
        decoder
    }

    #[test]
    fn a_relation_names_the_table_and_says_what_identifies_a_row() {
        let mut decoder = decoder();
        let insert = Message::new(b'I')
            .u32(16_384)
            .byte(b'N')
            .u16(2)
            .text("1")
            .text("wash up")
            .0;
        let change = decoder
            .message(24, &insert)
            .expect("an insert")
            .expect("a change");
        assert_eq!(change.relation.schema, "public");
        assert_eq!(change.relation.table, "todos");
        assert_eq!(change.relation.replica, Replica::Default);
        assert_eq!(
            change.relation.columns,
            vec![
                Column {
                    name: "id".into(),
                    type_oid: 23,
                    key: true,
                },
                Column {
                    name: "title".into(),
                    type_oid: 25,
                    key: false,
                },
            ]
        );
        assert_eq!(change.op, Op::Insert);
        assert_eq!(
            change.record,
            vec![Cell::Text("1".into()), Cell::Text("wash up".into())]
        );
        assert_eq!(change.old, None);
        assert_eq!(change.lsn, 24);
    }

    /// Under the default replica identity an update publishes the key
    /// and not the row, which is why a payload's `old_record` is a
    /// primary key on most tables and the whole row on none of them
    /// until somebody says so.
    #[test]
    fn an_update_under_the_default_identity_carries_the_key() {
        let mut decoder = decoder();
        let update = Message::new(b'U')
            .u32(16_384)
            .byte(b'K')
            .u16(2)
            .text("1")
            .byte(b'n')
            .byte(b'N')
            .u16(2)
            .text("2")
            .text("wash up")
            .0;
        let change = decoder
            .message(48, &update)
            .expect("an update")
            .expect("a change");
        assert_eq!(change.op, Op::Update);
        assert!(change.old_key);
        assert_eq!(
            change.old,
            Some(vec![Cell::Text("1".into()), Cell::Null]),
            "the key columns and nothing else"
        );
        assert_eq!(
            change.record,
            vec![Cell::Text("2".into()), Cell::Text("wash up".into())]
        );
    }

    /// With the identity set to full the whole row before the change
    /// arrives, which is the only way `old_record` is worth reading.
    #[test]
    fn an_update_under_full_identity_carries_the_row_it_replaced() {
        let mut decoder = decoder();
        let update = Message::new(b'U')
            .u32(16_384)
            .byte(b'O')
            .u16(2)
            .text("1")
            .text("wash up")
            .byte(b'N')
            .u16(2)
            .text("1")
            .text("washed up")
            .0;
        let change = decoder
            .message(64, &update)
            .expect("an update")
            .expect("a change");
        assert!(!change.old_key);
        assert_eq!(
            change.old,
            Some(vec![Cell::Text("1".into()), Cell::Text("wash up".into())])
        );
    }

    /// An update that touched neither the key nor the identity sends
    /// only what the row became, which is postgres saving the bytes
    /// rather than a change with something missing from it.
    #[test]
    fn an_update_that_published_no_old_row_still_says_what_the_row_is_now() {
        let mut decoder = decoder();
        let update = Message::new(b'U')
            .u32(16_384)
            .byte(b'N')
            .u16(2)
            .text("1")
            .text("washed up")
            .0;
        let change = decoder
            .message(80, &update)
            .expect("an update")
            .expect("a change");
        assert_eq!(change.old, None);
        assert_eq!(
            change.record,
            vec![Cell::Text("1".into()), Cell::Text("washed up".into())]
        );
    }

    #[test]
    fn a_delete_says_what_is_gone_and_nothing_about_what_is_there() {
        let mut decoder = decoder();
        let delete = Message::new(b'D')
            .u32(16_384)
            .byte(b'K')
            .u16(2)
            .text("1")
            .byte(b'n')
            .0;
        let change = decoder
            .message(96, &delete)
            .expect("a delete")
            .expect("a change");
        assert_eq!(change.op, Op::Delete);
        assert!(change.record.is_empty());
        assert_eq!(change.old, Some(vec![Cell::Text("1".into()), Cell::Null]));
    }

    /// A value stored out of line that was not written is not sent,
    /// and it is a different thing from a value that is null: the
    /// client already has one and has neither.
    #[test]
    fn a_toasted_column_nobody_touched_is_not_the_same_as_a_null_one() {
        let mut decoder = decoder();
        let update = Message::new(b'U')
            .u32(16_384)
            .byte(b'N')
            .u16(2)
            .text("1")
            .byte(b'u')
            .0;
        let change = decoder
            .message(112, &update)
            .expect("an update")
            .expect("a change");
        assert_eq!(change.record, vec![Cell::Text("1".into()), Cell::Unchanged]);
    }

    /// Every row of a transaction carries the transaction's commit
    /// time, because a row has no time of its own until the
    /// transaction it is in commits.
    #[test]
    fn a_change_is_stamped_with_the_transaction_that_wrote_it() {
        let mut decoder = Decoder::new();
        // One second past the postgres epoch, which is thirty years
        // and a second past the unix one.
        decoder
            .message(0, &begin(1_000_000))
            .expect("a begin")
            .expect_none();
        decoder.message(0, &todos()).expect("a relation");
        let insert = Message::new(b'I')
            .u32(16_384)
            .byte(b'N')
            .u16(2)
            .text("1")
            .text("wash up")
            .0;
        let change = decoder
            .message(8, &insert)
            .expect("an insert")
            .expect("a change");
        assert_eq!(change.commit_ts, POSTGRES_EPOCH + 1_000_000);
    }

    /// The messages this does not read are skipped rather than
    /// refused, so a project that runs truncate does not stop the tap.
    #[test]
    fn a_message_this_does_not_read_is_stepped_over() {
        let mut decoder = decoder();
        for message in [
            Message::new(b'C').byte(0).u64(1).u64(2).u64(3).0,
            Message::new(b'T').u32(1).byte(0).u32(16_384).0,
            Message::new(b'O').u64(1).string("somewhere").0,
            Message::new(b'Y').u32(1).string("public").string("mood").0,
        ] {
            assert_eq!(decoder.message(0, &message), Ok(None));
        }
    }

    /// A row of a table nobody described is a dropped message, and a
    /// tap that guessed would put one table's values under another
    /// table's column names.
    #[test]
    fn a_row_of_a_table_this_never_heard_of_is_an_error() {
        let mut decoder = Decoder::new();
        let insert = Message::new(b'I').u32(16_384).byte(b'N').u16(0).0;
        assert!(decoder.message(0, &insert).is_err());
    }

    #[test]
    fn a_message_that_was_cut_short_says_so_rather_than_panicking() {
        let mut decoder = Decoder::new();
        assert!(decoder.message(0, &[b'B', 0, 0]).is_err());
        assert!(decoder.message(0, &[]).is_err());
    }

    trait ExpectNone {
        fn expect_none(self);
    }

    impl<T: std::fmt::Debug> ExpectNone for Option<T> {
        fn expect_none(self) {
            assert!(self.is_none(), "{self:?}");
        }
    }
}
