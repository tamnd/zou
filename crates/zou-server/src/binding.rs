//! Who wanted which change.
//!
//! A client asks for postgres changes in its join, as a list of
//! bindings: an event, a schema, usually a table, and sometimes a
//! filter. A server with one socket on it could answer that by walking
//! the list for every row that changed. A server with ten thousand
//! subscriptions cannot, because every changed row would cost ten
//! thousand string comparisons and the thing that changed the row is
//! waiting.
//!
//! So the bindings are held the way they are asked: by the table they
//! are about and the event they are for. A change looks up its own
//! table, in its own event, and sees only the subscriptions that could
//! possibly match. What is left after that lookup is the filters, which
//! are the only part that has to be evaluated per subscription, and a
//! filter is one column compared with one value.
//!
//! There is no io here and no sockets either. What comes back from a
//! match is the ids the caller put in, and mapping an id to whoever is
//! listening is the transport's business.
//!
//! The comparison is typed rather than textual, because postgres sends
//! a row's values as text and `id=lt.10` against a text comparison
//! would put 9 outside it. A column's type oid comes from the relation
//! the change carries, which is what makes that possible without
//! asking the database anything.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;

use crate::pgoutput::{Cell, Change, Op, Relation};

/// Which changes a binding is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Wants {
    Insert,
    Update,
    Delete,
    /// `*`, which is the one most clients ask for.
    Any,
}

impl Wants {
    fn of(event: &str) -> Result<Wants, String> {
        match event.to_ascii_uppercase().as_str() {
            "INSERT" => Ok(Wants::Insert),
            "UPDATE" => Ok(Wants::Update),
            "DELETE" => Ok(Wants::Delete),
            "*" => Ok(Wants::Any),
            other => Err(format!("{other} is not an event")),
        }
    }

    /// The word a client sent for this, which is the word it goes back
    /// out as when a subscription for it could not be made.
    pub fn named(&self) -> &'static str {
        match self {
            Wants::Insert => "INSERT",
            Wants::Update => "UPDATE",
            Wants::Delete => "DELETE",
            Wants::Any => "*",
        }
    }

    /// Which of the three lists a binding goes in. `Any` goes in all of
    /// them, so that a change reads one list rather than two.
    fn lists(&self) -> &'static [usize] {
        match self {
            Wants::Insert => &[0],
            Wants::Update => &[1],
            Wants::Delete => &[2],
            Wants::Any => &[0, 1, 2],
        }
    }
}

/// How a filter compares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compare {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    In,
}

impl Compare {
    fn of(op: &str) -> Result<Compare, String> {
        match op {
            "eq" => Ok(Compare::Eq),
            "neq" => Ok(Compare::Neq),
            "lt" => Ok(Compare::Lt),
            "lte" => Ok(Compare::Lte),
            "gt" => Ok(Compare::Gt),
            "gte" => Ok(Compare::Gte),
            "in" => Ok(Compare::In),
            other => Err(format!("{other} is not a filter operator")),
        }
    }
}

/// One column compared with one value, which is all a postgres changes
/// filter has ever been.
///
/// Upstream's filter is the same shape as PostgREST's `column=op.value`
/// and a much smaller language: one column, seven operators, no `or`,
/// no second condition. That is not an accident of implementation. A
/// filter is evaluated once per subscription per changed row, so a
/// language with joins in it would be a language that decides how fast
/// the database can be written to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Filter {
    pub column: String,
    pub compare: Compare,
    /// The value as the client wrote it, compared against the column's
    /// own type at match time.
    pub value: String,
}

impl Compare {
    /// The word the client wrote, which a refusal names it by.
    pub fn named(&self) -> &'static str {
        match self {
            Compare::Eq => "eq",
            Compare::Neq => "neq",
            Compare::Lt => "lt",
            Compare::Lte => "lte",
            Compare::Gt => "gt",
            Compare::Gte => "gte",
            Compare::In => "in",
        }
    }
}

impl Filter {
    /// `column=op.value`, or `column=in.(a,b,c)`.
    pub fn of(text: &str) -> Result<Filter, String> {
        let (column, rest) = text
            .split_once('=')
            .ok_or_else(|| format!("{text} is not a filter"))?;
        let (op, value) = rest
            .split_once('.')
            .ok_or_else(|| format!("{text} is not a filter"))?;
        if column.is_empty() {
            return Err(format!("{text} filters no column"));
        }
        let compare = Compare::of(op)?;
        if compare == Compare::In && !(value.starts_with('(') && value.ends_with(')')) {
            return Err(format!("{value} is not a list"));
        }
        Ok(Filter {
            column: column.to_string(),
            compare,
            value: value.to_string(),
        })
    }

    /// Whether this row passes.
    ///
    /// A column the filter names and the relation does not have is a
    /// no, rather than an error: the table was altered under a live
    /// subscription, and a subscriber hearing nothing is a better wrong
    /// answer than a subscriber hearing everything.
    ///
    /// The same goes for a value postgres did not send. A large value
    /// nobody wrote is stored out of line and left out of the message,
    /// so there is nothing to compare, and a filter that cannot be
    /// evaluated does not pass.
    fn passes(&self, relation: &Relation, cells: &[Cell]) -> bool {
        let Some(at) = relation
            .columns
            .iter()
            .position(|column| column.name == self.column)
        else {
            return false;
        };
        let Some(cell) = cells.get(at) else {
            return false;
        };
        let oid = relation.columns[at].type_oid;
        let value = match cell {
            Cell::Text(text) => text.as_str(),
            // A null is not less than, greater than, or equal to
            // anything, which is postgres's own answer and upstream's.
            Cell::Null => return false,
            Cell::Unchanged | Cell::Bytes(_) => return false,
        };
        match self.compare {
            Compare::In => {
                list(&self.value).any(|one| compare(oid, value, one) == Some(Ordering::Equal))
            }
            _ => {
                let Some(how) = compare(oid, value, &self.value) else {
                    return false;
                };
                match self.compare {
                    Compare::Eq => how == Ordering::Equal,
                    Compare::Neq => how != Ordering::Equal,
                    Compare::Lt => how == Ordering::Less,
                    Compare::Lte => how != Ordering::Greater,
                    Compare::Gt => how == Ordering::Greater,
                    Compare::Gte => how != Ordering::Less,
                    Compare::In => unreachable!("handled above"),
                }
            }
        }
    }
}

/// What one entry of a client's `postgres_changes` list asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binding {
    pub wants: Wants,
    pub schema: String,
    /// The table, or nothing for every table in the schema, which is
    /// what a client asking to hear a whole schema sends.
    pub table: Option<String>,
    pub filter: Option<Filter>,
}

impl Binding {
    /// One entry of the list, the way realtime-js writes it.
    ///
    /// A missing schema is `public`, which is what every client that
    /// leaves it out means, and a table of `*` is every table, which is
    /// the same thing as leaving it out.
    pub fn of(value: &Value) -> Result<Binding, String> {
        let text = |name: &str| {
            value
                .get(name)
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
        };
        let wants = Wants::of(text("event").unwrap_or("*"))?;
        let schema = text("schema").unwrap_or("public").to_string();
        let table = text("table").filter(|t| *t != "*").map(str::to_string);
        let filter = match text("filter") {
            Some(text) => Some(Filter::of(text)?),
            None => None,
        };
        // A filter with no table names a column of no table in
        // particular. Upstream refuses it and so does this, because the
        // alternative is a subscription that quietly matches nothing.
        if filter.is_some() && table.is_none() {
            return Err("a filter needs a table".to_string());
        }
        Ok(Binding {
            wants,
            schema,
            table,
            filter,
        })
    }
}

#[derive(Debug)]
struct Entry {
    id: u64,
    binding: Binding,
}

/// The three lists a table or a schema keeps, one per event, so that a
/// change reads only the subscriptions that could match it.
type Lists = [Vec<Arc<Entry>>; 3];

fn list_of(op: Op) -> usize {
    match op {
        Op::Insert => 0,
        Op::Update => 1,
        Op::Delete => 2,
    }
}

/// Every binding anybody has asked for, indexed by what it is about.
///
/// Two indexes rather than one: bindings about a table, and bindings
/// about every table in a schema. A change reads both, which is two
/// hash lookups whatever the number of subscriptions, and then walks
/// only what it found.
#[derive(Debug, Default)]
pub struct Subscriptions {
    tables: HashMap<(String, String), Lists>,
    schemas: HashMap<String, Lists>,
    /// Where each id was put, so that removing one does not walk every
    /// bucket. A socket that hangs up removes its own and nothing else.
    placed: HashMap<u64, Binding>,
}

impl Subscriptions {
    pub fn new() -> Subscriptions {
        Subscriptions::default()
    }

    /// Remember a binding under an id the caller chose, which is what
    /// comes back when a change matches it.
    ///
    /// Adding an id that is already here replaces it, so a client that
    /// rejoins a channel does not end up subscribed twice.
    pub fn add(&mut self, id: u64, binding: Binding) {
        self.remove(id);
        let entry = Arc::new(Entry {
            id,
            binding: binding.clone(),
        });
        let lists = match &binding.table {
            Some(table) => self
                .tables
                .entry((binding.schema.clone(), table.clone()))
                .or_default(),
            None => self.schemas.entry(binding.schema.clone()).or_default(),
        };
        for at in binding.wants.lists() {
            lists[*at].push(Arc::clone(&entry));
        }
        self.placed.insert(id, binding);
    }

    /// Forget one, which is a channel leaving or a socket hanging up.
    pub fn remove(&mut self, id: u64) {
        let Some(binding) = self.placed.remove(&id) else {
            return;
        };
        let key = binding
            .table
            .as_ref()
            .map(|table| (binding.schema.clone(), table.clone()));
        let lists = match &key {
            Some(key) => self.tables.get_mut(key),
            None => self.schemas.get_mut(&binding.schema),
        };
        if let Some(lists) = lists {
            for at in binding.wants.lists() {
                lists[*at].retain(|entry| entry.id != id);
            }
            if lists.iter().all(Vec::is_empty) {
                match &key {
                    Some(key) => {
                        self.tables.remove(key);
                    }
                    None => {
                        self.schemas.remove(&binding.schema);
                    }
                }
            }
        }
    }

    /// How many bindings are held, which is what a quota would count.
    pub fn len(&self) -> usize {
        self.placed.len()
    }

    pub fn is_empty(&self) -> bool {
        self.placed.is_empty()
    }

    /// The ids that asked for this change, in the order they were
    /// added.
    ///
    /// A delete is filtered on what postgres published of the row that
    /// is gone, which under the default replica identity is the primary
    /// key and nothing else. So a filter on any other column matches no
    /// delete on a table that has not been told to publish its old
    /// rows, which is upstream's behaviour and worth knowing before
    /// somebody wonders where their deletes went.
    pub fn matching(&self, change: &Change) -> Vec<u64> {
        let at = list_of(change.op);
        let schema = &change.relation.schema;
        let table = &change.relation.table;
        let cells = match change.op {
            Op::Delete => change.old.as_deref().unwrap_or(&[]),
            _ => &change.record,
        };
        let mut ids = Vec::new();
        let mut take = |lists: &Lists| {
            for entry in &lists[at] {
                let passes = match &entry.binding.filter {
                    Some(filter) => filter.passes(&change.relation, cells),
                    None => true,
                };
                if passes {
                    ids.push(entry.id);
                }
            }
        };
        if let Some(lists) = self.tables.get(&(schema.clone(), table.clone())) {
            take(lists);
        }
        if let Some(lists) = self.schemas.get(schema) {
            take(lists);
        }
        ids
    }
}

/// The elements of an `in.(a,b,c)` list.
fn list(value: &str) -> impl Iterator<Item = &str> {
    value
        .trim_start_matches('(')
        .trim_end_matches(')')
        .split(',')
        .map(|one| one.trim().trim_matches('"').trim_matches('\''))
        .filter(|one| !one.is_empty())
}

/// Compare a value postgres sent with a value a client wrote, in the
/// column's own type.
///
/// Postgres sends every value as the text its output function
/// produced, so a textual comparison would answer `id=lt.10` with
/// every id starting with a 1 and none of the single digits. What
/// makes the typed comparison possible without asking the database
/// anything is the type oid on the column, which the relation message
/// carried.
///
/// Nothing comes back when the client's value is not of the column's
/// type, which is a filter that can never match rather than an error:
/// the subscription was accepted at join time, and refusing a row now
/// because of it would be refusing the wrong thing.
fn compare(oid: u32, value: &str, against: &str) -> Option<Ordering> {
    match oid {
        // int2, int4, int8, oid.
        21 | 23 | 20 | 26 => {
            let left: i128 = value.parse().ok()?;
            let right: i128 = against.parse().ok()?;
            Some(left.cmp(&right))
        }
        // float4, float8, numeric. A numeric wider than a double is
        // compared as a double, which is what a filter on a value that
        // wide deserves.
        700 | 701 | 1700 => {
            let left: f64 = value.parse().ok()?;
            let right: f64 = against.parse().ok()?;
            left.partial_cmp(&right)
        }
        // bool, which postgres writes as t and f and a client writes as
        // true and false.
        16 => Some(truth(value)?.cmp(&truth(against)?)),
        // Everything else compares as text, which is the right answer
        // for text and uuid, and the right answer for a timestamp too:
        // postgres writes them widest field first, so their text order
        // is their order.
        _ => Some(value.cmp(against)),
    }
}

fn truth(value: &str) -> Option<bool> {
    match value {
        "t" | "true" | "TRUE" | "yes" | "on" | "1" => Some(true),
        "f" | "false" | "FALSE" | "no" | "off" | "0" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pgoutput::{Column, Replica};

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
                    name: "title".into(),
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

    fn change(op: Op, id: &str, title: &str, done: &str) -> Change {
        let cells = vec![
            Cell::Text(id.into()),
            Cell::Text(title.into()),
            Cell::Text(done.into()),
        ];
        Change {
            relation: todos(),
            op,
            record: if op == Op::Delete {
                Vec::new()
            } else {
                cells.clone()
            },
            old: (op == Op::Delete).then_some(cells),
            old_key: op == Op::Delete,
            commit_ts: 0,
            lsn: 0,
        }
    }

    fn binding(json: &str) -> Binding {
        Binding::of(&serde_json::from_str(json).expect("json")).expect("a binding")
    }

    #[test]
    fn a_binding_takes_the_defaults_a_client_leaves_out() {
        let asked = binding(r#"{"event":"*"}"#);
        assert_eq!(asked.wants, Wants::Any);
        assert_eq!(asked.schema, "public");
        assert_eq!(asked.table, None, "every table in the schema");
        assert_eq!(asked.filter, None);
        assert_eq!(
            binding(r#"{"event":"insert","schema":"app","table":"*"}"#),
            Binding {
                wants: Wants::Insert,
                schema: "app".into(),
                table: None,
                filter: None,
            },
            "a table of star is every table"
        );
    }

    #[test]
    fn a_binding_that_makes_no_sense_is_refused_rather_than_kept() {
        for asked in [
            r#"{"event":"CHANGED"}"#,
            r#"{"event":"*","table":"todos","filter":"id"}"#,
            r#"{"event":"*","table":"todos","filter":"id=like.wash%"}"#,
            r#"{"event":"*","table":"todos","filter":"=eq.1"}"#,
            r#"{"event":"*","table":"todos","filter":"id=in.1,2"}"#,
            r#"{"event":"*","filter":"id=eq.1"}"#,
        ] {
            let value: Value = serde_json::from_str(asked).expect("json");
            assert!(Binding::of(&value).is_err(), "{asked}");
        }
    }

    #[test]
    fn a_change_reaches_everyone_who_asked_for_that_table() {
        let mut subs = Subscriptions::new();
        subs.add(1, binding(r#"{"event":"*","table":"todos"}"#));
        subs.add(2, binding(r#"{"event":"INSERT","table":"todos"}"#));
        subs.add(3, binding(r#"{"event":"DELETE","table":"todos"}"#));
        subs.add(4, binding(r#"{"event":"*","table":"lists"}"#));
        subs.add(
            5,
            binding(r#"{"event":"*","schema":"app","table":"todos"}"#),
        );
        assert_eq!(
            subs.matching(&change(Op::Insert, "1", "wash up", "f")),
            vec![1, 2]
        );
        assert_eq!(
            subs.matching(&change(Op::Update, "1", "wash up", "f")),
            vec![1]
        );
        assert_eq!(
            subs.matching(&change(Op::Delete, "1", "wash up", "f")),
            vec![1, 3]
        );
    }

    /// A client can ask for a schema rather than a table, and one that
    /// did hears about a table it never named.
    #[test]
    fn a_client_listening_to_a_schema_hears_every_table_in_it() {
        let mut subs = Subscriptions::new();
        subs.add(1, binding(r#"{"event":"*"}"#));
        subs.add(2, binding(r#"{"event":"*","schema":"app"}"#));
        subs.add(3, binding(r#"{"event":"*","table":"todos"}"#));
        assert_eq!(
            subs.matching(&change(Op::Insert, "1", "wash up", "f")),
            vec![3, 1],
            "the table's own subscribers first, then the schema's"
        );
    }

    #[test]
    fn a_filter_is_compared_in_the_column_s_own_type() {
        let mut subs = Subscriptions::new();
        subs.add(
            1,
            binding(r#"{"event":"*","table":"todos","filter":"id=eq.9"}"#),
        );
        subs.add(
            2,
            binding(r#"{"event":"*","table":"todos","filter":"id=lt.10"}"#),
        );
        subs.add(
            3,
            binding(r#"{"event":"*","table":"todos","filter":"id=gt.10"}"#),
        );
        subs.add(
            4,
            binding(r#"{"event":"*","table":"todos","filter":"title=eq.wash up"}"#),
        );
        subs.add(
            5,
            binding(r#"{"event":"*","table":"todos","filter":"done=eq.true"}"#),
        );
        subs.add(
            6,
            binding(r#"{"event":"*","table":"todos","filter":"id=in.(7,9,11)"}"#),
        );
        assert_eq!(
            subs.matching(&change(Op::Insert, "9", "wash up", "f")),
            vec![1, 2, 4, 6],
            "nine is less than ten as a number and not as a word"
        );
        assert_eq!(
            subs.matching(&change(Op::Insert, "11", "sweep", "t")),
            vec![3, 5, 6],
            "eleven is in the list and is not the eleven a text comparison would look for"
        );
    }

    /// A filter naming a column that is not there, a value that is not
    /// of the column's type, or a null all fail to match rather than
    /// failing the change.
    #[test]
    fn a_filter_that_cannot_be_evaluated_does_not_match() {
        let mut subs = Subscriptions::new();
        subs.add(
            1,
            binding(r#"{"event":"*","table":"todos","filter":"nothing=eq.1"}"#),
        );
        subs.add(
            2,
            binding(r#"{"event":"*","table":"todos","filter":"id=eq.one"}"#),
        );
        subs.add(
            3,
            binding(r#"{"event":"*","table":"todos","filter":"title=eq.x"}"#),
        );
        let mut nulled = change(Op::Insert, "1", "", "f");
        nulled.record[1] = Cell::Null;
        assert!(subs.matching(&nulled).is_empty());

        let mut toasted = change(Op::Update, "1", "", "f");
        toasted.record[1] = Cell::Unchanged;
        assert!(subs.matching(&toasted).is_empty());
    }

    /// Under the default replica identity a delete publishes the key
    /// and nothing else, so a filter on anything else matches no
    /// delete. That is upstream's behaviour and the reason a project
    /// that wants filtered deletes sets its replica identity to full.
    #[test]
    fn a_delete_is_filtered_on_what_was_published_of_the_row() {
        let mut subs = Subscriptions::new();
        subs.add(
            1,
            binding(r#"{"event":"DELETE","table":"todos","filter":"id=eq.1"}"#),
        );
        subs.add(
            2,
            binding(r#"{"event":"DELETE","table":"todos","filter":"title=eq.wash up"}"#),
        );
        let mut gone = change(Op::Delete, "1", "", "f");
        // What postgres publishes of a deleted row under the default
        // identity: the key, and nulls where the rest of it was.
        gone.old = Some(vec![Cell::Text("1".into()), Cell::Null, Cell::Null]);
        assert_eq!(subs.matching(&gone), vec![1]);
    }

    #[test]
    fn a_subscription_that_left_hears_nothing_more() {
        let mut subs = Subscriptions::new();
        subs.add(1, binding(r#"{"event":"*","table":"todos"}"#));
        subs.add(2, binding(r#"{"event":"*","table":"todos"}"#));
        assert_eq!(subs.len(), 2);
        subs.remove(1);
        assert_eq!(
            subs.matching(&change(Op::Insert, "1", "wash up", "f")),
            vec![2]
        );
        subs.remove(2);
        assert!(subs.is_empty());
        assert!(
            subs.matching(&change(Op::Insert, "1", "wash up", "f"))
                .is_empty()
        );
        // Removing something that is not there is what a socket closing
        // twice looks like.
        subs.remove(1);
    }

    /// A client that rejoins a channel is subscribed once, not twice.
    #[test]
    fn adding_an_id_twice_replaces_what_it_asked_for() {
        let mut subs = Subscriptions::new();
        subs.add(1, binding(r#"{"event":"*","table":"todos"}"#));
        subs.add(1, binding(r#"{"event":"*","table":"lists"}"#));
        assert_eq!(subs.len(), 1);
        assert!(
            subs.matching(&change(Op::Insert, "1", "wash up", "f"))
                .is_empty()
        );
    }
}
