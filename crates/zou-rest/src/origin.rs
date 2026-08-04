//! Where a view's columns came from.
//!
//! A view has no foreign keys of its own. Postgres records none for
//! it and never will, so a client that embeds `books` inside
//! `authors_view` is asking about a relationship that exists on the
//! tables underneath and nowhere in the catalog. PostgREST answers
//! those anyway, and so does this: if a view selects the two columns
//! a foreign key is made of, the view has that key as surely as the
//! table does, under the same constraint name, and an embed can join
//! on it.
//!
//! Finding out which column of a view is which column of a table is
//! the whole difficulty. The name is no help, since a view renames
//! freely, and `pg_depend` only says that a view reads a column, not
//! which of its own columns that came out as. The one place postgres
//! keeps the mapping is the parse tree of the view's rule, where
//! every target entry carries `resorigtbl` and `resorigcol`, the
//! relation and column its value originally came from. That tree is
//! `pg_rewrite.ev_action`, printed in postgres's own node format, so
//! [`origins`] reads that format and pulls the target list out of it.
//!
//! One step at a time, because `resorigtbl` names whatever the view
//! selected from, which may be another view. [`derive`] walks the
//! chain until it reaches a relation that is not a view here, which
//! is how a view over a view over a table ends up with the table's
//! keys.
//!
//! What comes out is [`FkRow`] values, the same shape introspection
//! hands over for real foreign keys, so everything downstream, the
//! embed resolution, the many to many detection, the hints, treats a
//! view exactly like a table without knowing that it is one.

use std::collections::BTreeMap;

use crate::catalog::FkRow;

/// Every view in the schema, and every view those are built on, with
/// the parse tree of each one's rule. Bind the schema name as $1.
///
/// The recursion is not decoration. A view in the exposed schema may
/// select from a view in a schema nobody exposes, and that hidden
/// view is the only thing that knows which table the column came
/// from, so it has to be read even though no request will ever name
/// it.
pub const VIEWS_SQL: &str = "\
with recursive seen as (
  select c.oid
    from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
   where c.relkind in ('v', 'm')
     and n.nspname = $1
  union
  select b.oid
    from seen
    join pg_rewrite r on r.ev_class = seen.oid
    join pg_depend d
      on d.classid = 'pg_rewrite'::regclass
     and d.objid = r.oid
     and d.refclassid = 'pg_class'::regclass
     and d.refobjid <> seen.oid
    join pg_class b on b.oid = d.refobjid and b.relkind in ('v', 'm')
)
select c.oid::int8,
       n.nspname::text,
       c.relname::text,
       (select array_agg(a.attnum::int4 order by a.attnum)
          from pg_attribute a
         where a.attrelid = c.oid and a.attnum > 0 and not a.attisdropped),
       (select array_agg(a.attname::text order by a.attnum)
          from pg_attribute a
         where a.attrelid = c.oid and a.attnum > 0 and not a.attisdropped),
       r.ev_action::text
  from seen
  join pg_class c on c.oid = seen.oid
  join pg_namespace n on n.oid = c.relnamespace
  join pg_rewrite r on r.ev_class = c.oid
 order by c.oid";

/// Every foreign key that touches a relation the schema's views are
/// built on, by oid and attribute number rather than by name. Bind
/// the schema name as $1.
///
/// The catalog's own introspection keeps to the exposed schema
/// because that is where the tables a request can name live. This
/// one cannot: the table a view hides may sit in a schema the
/// request has no access to, and the key on it is still the key the
/// view inherits. It stays bounded by only asking for keys on
/// relations some view here actually reads.
pub const VIEW_KEYS_SQL: &str = "\
with recursive uses as (
  select c.oid
    from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
   where c.relkind in ('v', 'm')
     and n.nspname = $1
  union
  select d.refobjid
    from uses
    join pg_rewrite r on r.ev_class = uses.oid
    join pg_depend d
      on d.classid = 'pg_rewrite'::regclass
     and d.objid = r.oid
     and d.refclassid = 'pg_class'::regclass
     and d.refobjid <> uses.oid
)
select c.conname::text,
       cns.nspname::text,
       child.relname::text,
       child.oid::int8,
       (select array_agg(k.attnum::int4 order by k.ord)
          from unnest(c.conkey) with ordinality k(attnum, ord)),
       (select array_agg(a.attname::text order by k.ord)
          from unnest(c.conkey) with ordinality k(attnum, ord)
          join pg_attribute a
            on a.attrelid = c.conrelid and a.attnum = k.attnum),
       pns.nspname::text,
       parent.relname::text,
       parent.oid::int8,
       (select array_agg(k.attnum::int4 order by k.ord)
          from unnest(c.confkey) with ordinality k(attnum, ord)),
       (select array_agg(a.attname::text order by k.ord)
          from unnest(c.confkey) with ordinality k(attnum, ord)
          join pg_attribute a
            on a.attrelid = c.confrelid and a.attnum = k.attnum),
       exists (select 1 from pg_constraint u
                where u.conrelid = c.conrelid
                  and u.contype in ('p', 'u')
                  and (select array_agg(x order by x) from unnest(u.conkey) x)
                    = (select array_agg(x order by x) from unnest(c.conkey) x)),
       coalesce((select c.conkey <@ p.conkey from pg_constraint p
                  where p.conrelid = c.conrelid and p.contype = 'p'),
                false)
  from pg_constraint c
  join pg_class child on child.oid = c.conrelid
  join pg_namespace cns on cns.oid = child.relnamespace
  join pg_class parent on parent.oid = c.confrelid
  join pg_namespace pns on pns.oid = parent.relnamespace
 where c.contype = 'f'
   and (c.conrelid in (select oid from uses)
     or c.confrelid in (select oid from uses))
 order by c.conname, child.relname";

/// One view as [`VIEWS_SQL`] hands it over.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewRow {
    pub oid: i64,
    pub schema: String,
    pub name: String,
    /// The attribute numbers of its columns, and their names in the
    /// same order. A view's column `n` is its target entry `n`, so
    /// the attribute number is what the parse tree calls `resno`.
    pub attnums: Vec<i32>,
    pub columns: Vec<String>,
    /// `pg_rewrite.ev_action`, the printed parse tree.
    pub tree: String,
}

/// One foreign key as [`VIEW_KEYS_SQL`] hands it over, which is the
/// catalog's [`FkRow`] with the oids and attribute numbers kept, the
/// only form in which a view's column can be matched against it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyRow {
    pub constraint: String,
    pub schema: String,
    pub table: String,
    pub oid: i64,
    pub attnums: Vec<i32>,
    pub columns: Vec<String>,
    pub ref_schema: String,
    pub ref_table: String,
    pub ref_oid: i64,
    pub ref_attnums: Vec<i32>,
    pub ref_columns: Vec<String>,
    pub unique: bool,
    pub in_pk: bool,
}

/// Where one column of a view came from, one step back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Origin {
    /// The view's own column, its `resno`.
    pub column: i32,
    /// The relation the value came out of, and its column there.
    pub table: i64,
    pub table_column: i32,
}

/// The foreign keys the schema's views inherit from the tables they
/// select from: the view as the child of a key, as the parent of
/// one, and both ends at once when two views are built on the two
/// ends of the same key.
///
/// Keys whose table end is in another schema still make a
/// relationship, but only through the view: a request cannot name a
/// table it cannot see, so a row is only written for an end that is
/// either a view here or a table here.
pub fn derive(schema: &str, views: &[ViewRow], keys: &[KeyRow]) -> Vec<FkRow> {
    // Every view's columns traced back as far as they go, keyed by
    // where they landed, since that is the direction the keys are
    // looked up in.
    let steps: BTreeMap<i64, Vec<Origin>> = views
        .iter()
        .map(|view| (view.oid, origins(&view.tree)))
        .collect();
    let mut from: BTreeMap<(i64, i32), Vec<(usize, String)>> = BTreeMap::new();
    for (at, view) in views.iter().enumerate() {
        if view.schema != schema {
            continue;
        }
        for origin in steps.get(&view.oid).into_iter().flatten() {
            let Some(name) = view.column(origin.column) else {
                continue;
            };
            for (table, column) in traced(&steps, *origin) {
                from.entry((table, column))
                    .or_default()
                    .push((at, name.to_string()));
            }
        }
    }

    // The ones pointing at a table come first and the ones pointing
    // at a view after, because the openapi document notes a column's
    // key by the first relationship it finds and upstream sorts the
    // views to the back before looking.
    let mut rows = Vec::new();
    let mut onto_views = Vec::new();
    for key in keys {
        let children = ends(&from, key.oid, &key.attnums);
        let parents = ends(&from, key.ref_oid, &key.ref_attnums);
        for (at, columns) in &children {
            if key.ref_schema == schema {
                rows.push(FkRow {
                    constraint: key.constraint.clone(),
                    table: views[*at].name.clone(),
                    columns: columns.clone(),
                    ref_table: key.ref_table.clone(),
                    ref_columns: key.ref_columns.clone(),
                    unique: key.unique,
                    in_pk: key.in_pk,
                });
            }
            for (to, ref_columns) in &parents {
                onto_views.push(FkRow {
                    constraint: key.constraint.clone(),
                    table: views[*at].name.clone(),
                    columns: columns.clone(),
                    ref_table: views[*to].name.clone(),
                    ref_columns: ref_columns.clone(),
                    unique: key.unique,
                    in_pk: key.in_pk,
                });
            }
        }
        if key.schema != schema {
            continue;
        }
        for (to, ref_columns) in &parents {
            onto_views.push(FkRow {
                constraint: key.constraint.clone(),
                table: key.table.clone(),
                columns: key.columns.clone(),
                ref_table: views[*to].name.clone(),
                ref_columns: ref_columns.clone(),
                unique: key.unique,
                in_pk: key.in_pk,
            });
        }
    }
    rows.append(&mut onto_views);
    rows
}

impl ViewRow {
    /// The name of the column with that attribute number.
    fn column(&self, attnum: i32) -> Option<&str> {
        let at = self.attnums.iter().position(|n| *n == attnum)?;
        self.columns.get(at).map(String::as_str)
    }
}

/// Every relation and column one step can be followed back to,
/// including the step itself, since a view over a view over a table
/// holds the table's key and the middle view holds nothing.
///
/// A step is only taken once, which is what stops a recursive view,
/// whose column comes from itself, from being followed forever.
fn traced(steps: &BTreeMap<i64, Vec<Origin>>, origin: Origin) -> Vec<(i64, i32)> {
    let mut out = vec![(origin.table, origin.table_column)];
    let mut at = 0;
    while at < out.len() {
        let (table, column) = out[at];
        at += 1;
        for next in steps.get(&table).into_iter().flatten() {
            let step = (next.table, next.table_column);
            if next.column == column && !out.contains(&step) {
                out.push(step);
            }
        }
    }
    out
}

/// Which views hold a whole key of a table, and under which of their
/// own column names.
///
/// A view may select the same column twice under two names, in which
/// case it holds the key twice over and every combination of those
/// names is a relationship of its own. A view holding only part of a
/// key holds no key at all.
fn ends(
    from: &BTreeMap<(i64, i32), Vec<(usize, String)>>,
    table: i64,
    attnums: &[i32],
) -> Vec<(usize, Vec<String>)> {
    let mut out: Vec<(usize, Vec<String>)> = Vec::new();
    let mut views: Vec<usize> = Vec::new();
    for attnum in attnums {
        for (at, _) in from.get(&(table, *attnum)).into_iter().flatten() {
            if !views.contains(at) {
                views.push(*at);
            }
        }
    }
    for view in views {
        let mut each: Vec<Vec<String>> = Vec::new();
        for attnum in attnums {
            let names: Vec<String> = from
                .get(&(table, *attnum))
                .into_iter()
                .flatten()
                .filter(|(at, _)| *at == view)
                .map(|(_, name)| name.clone())
                .collect();
            each.push(names);
        }
        if each.iter().any(Vec::is_empty) {
            continue;
        }
        for columns in spread(&each) {
            out.push((view, columns));
        }
    }
    out
}

/// One name per position, every way round.
fn spread(each: &[Vec<String>]) -> Vec<Vec<String>> {
    let mut out: Vec<Vec<String>> = vec![Vec::new()];
    for names in each {
        let mut next = Vec::new();
        for so_far in &out {
            for name in names {
                let mut one = so_far.clone();
                one.push(name.clone());
                next.push(one);
            }
        }
        out = next;
    }
    out
}

/// The target list of a printed parse tree: which column of the view
/// came from which column of what.
///
/// Entries postgres could not trace back to a column carry a zero
/// `resorigtbl`, an expression or a literal or a count, and there is
/// nothing to inherit from those. Junk entries are not columns of
/// the view at all.
pub fn origins(tree: &str) -> Vec<Origin> {
    let mut out = Vec::new();
    let root = parse(tree);
    let Some(query) = root.find("QUERY") else {
        return out;
    };
    let Some(list) = query.field("targetList") else {
        return out;
    };
    for entry in list.items() {
        if entry.tag() != Some("TARGETENTRY") || entry.number("resjunk") == Some(1) {
            continue;
        }
        let (Some(column), Some(table), Some(table_column)) = (
            entry.number("resno"),
            entry.number("resorigtbl"),
            entry.number("resorigcol"),
        ) else {
            continue;
        };
        if table == 0 {
            continue;
        }
        out.push(Origin {
            column: column as i32,
            table,
            table_column: table_column as i32,
        });
    }
    out
}

/// A printed parse tree, which is three things: a word, a list of
/// things in parentheses, and a node in braces whose first word is
/// its type and whose fields are named with a leading colon.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Node {
    Word(String),
    List(Vec<Node>),
    Named(String, Vec<(String, Node)>),
}

impl Node {
    fn tag(&self) -> Option<&str> {
        match self {
            Node::Named(tag, _) => Some(tag),
            _ => None,
        }
    }

    fn field(&self, name: &str) -> Option<&Node> {
        match self {
            Node::Named(_, fields) => fields
                .iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value),
            _ => None,
        }
    }

    /// A field whose value is a number, which is every field read
    /// here: an oid, an attribute number, a boolean written as a
    /// word.
    fn number(&self, name: &str) -> Option<i64> {
        match self.field(name)? {
            Node::Word(word) => match word.as_str() {
                "true" => Some(1),
                "false" => Some(0),
                word => word.parse().ok(),
            },
            _ => None,
        }
    }

    fn items(&self) -> &[Node] {
        match self {
            Node::List(items) => items,
            _ => &[],
        }
    }

    /// The first node of that type anywhere in here. A rule's action
    /// is a list of queries and the one that matters is the first,
    /// which is where a view keeps its select.
    fn find(&self, tag: &str) -> Option<&Node> {
        if self.tag() == Some(tag) {
            return Some(self);
        }
        match self {
            Node::List(items) => items.iter().find_map(|item| item.find(tag)),
            _ => None,
        }
    }
}

fn parse(text: &str) -> Node {
    let mut chars = text.chars().peekable();
    read(&mut chars).unwrap_or(Node::Word(String::new()))
}

type Chars<'a> = std::iter::Peekable<std::str::Chars<'a>>;

fn read(chars: &mut Chars) -> Option<Node> {
    skip(chars);
    match chars.peek()? {
        '(' => {
            chars.next();
            let mut items = Vec::new();
            loop {
                skip(chars);
                match chars.peek() {
                    Some(')') | None => {
                        chars.next();
                        return Some(Node::List(items));
                    }
                    _ => items.push(read(chars)?),
                }
            }
        }
        '{' => {
            chars.next();
            let tag = word(chars);
            let mut fields = Vec::new();
            loop {
                skip(chars);
                match chars.peek() {
                    Some('}') | None => {
                        chars.next();
                        return Some(Node::Named(tag, fields));
                    }
                    Some(':') => {
                        chars.next();
                        let name = word(chars);
                        let value = read(chars)?;
                        fields.push((name, value));
                    }
                    // A node whose fields are not all named, which
                    // nothing read here has, but a tree is not
                    // allowed to end the parse early either.
                    _ => {
                        read(chars)?;
                    }
                }
            }
        }
        _ => Some(Node::Word(word(chars))),
    }
}

fn skip(chars: &mut Chars) {
    while chars.peek().is_some_and(|c| c.is_whitespace()) {
        chars.next();
    }
}

/// One token. Anything the printer had to protect is behind a
/// backslash, spaces and brackets included, so a backslash means the
/// character after it is part of the word whatever it is.
fn word(chars: &mut Chars) -> String {
    let mut out = String::new();
    while let Some(c) = chars.peek() {
        match c {
            '\\' => {
                chars.next();
                if let Some(c) = chars.next() {
                    out.push(c);
                }
            }
            c if c.is_whitespace() || "(){}:".contains(*c) => break,
            c => {
                out.push(*c);
                chars.next();
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The shape of a real one, cut down to the fields that are
    /// read. `vid` comes from column 1 of table 6474601 and `name`
    /// from column 2, which is what a view renaming a column looks
    /// like from here.
    const TREE: &str = "({QUERY :commandType 1 :utilityStmt <> \
        :rtable ({RANGETBLENTRY :eref {ALIAS :aliasname t :colnames (\"id\" \"name\")} \
        :relid 6474601 :inh true}) \
        :targetList ({TARGETENTRY :expr {VAR :varno 1 :varattno 1 :location -1} \
        :resno 1 :resname vid :resorigtbl 6474601 :resorigcol 1 :resjunk false} \
        {TARGETENTRY :expr {VAR :varno 1 :varattno 2 :location -1} \
        :resno 2 :resname name :resorigtbl 6474601 :resorigcol 2 :resjunk false}) \
        :override 0})";

    fn view(oid: i64, name: &str, columns: &[&str], tree: &str) -> ViewRow {
        ViewRow {
            oid,
            schema: "test".to_string(),
            name: name.to_string(),
            attnums: (1..=columns.len() as i32).collect(),
            columns: columns.iter().map(|c| c.to_string()).collect(),
            tree: tree.to_string(),
        }
    }

    fn key(
        constraint: &str,
        table: (i64, &str, i32, &str),
        parent: (i64, &str, i32, &str),
    ) -> KeyRow {
        KeyRow {
            constraint: constraint.to_string(),
            schema: "test".to_string(),
            table: table.1.to_string(),
            oid: table.0,
            attnums: vec![table.2],
            columns: vec![table.3.to_string()],
            ref_schema: "test".to_string(),
            ref_table: parent.1.to_string(),
            ref_oid: parent.0,
            ref_attnums: vec![parent.2],
            ref_columns: vec![parent.3.to_string()],
            unique: false,
            in_pk: false,
        }
    }

    #[test]
    fn a_target_list_says_which_column_came_from_where() {
        assert_eq!(
            origins(TREE),
            vec![
                Origin {
                    column: 1,
                    table: 6474601,
                    table_column: 1
                },
                Origin {
                    column: 2,
                    table: 6474601,
                    table_column: 2
                },
            ]
        );
    }

    /// A count or a literal has nothing behind it, and postgres says
    /// so by leaving the origin at zero.
    #[test]
    fn a_column_that_came_from_no_column_is_left_out() {
        let tree = "({QUERY :targetList ({TARGETENTRY :resno 1 :resname n \
            :resorigtbl 0 :resorigcol 0 :resjunk false})})";
        assert_eq!(origins(tree), Vec::new());
    }

    #[test]
    fn a_junk_entry_is_not_a_column_of_the_view() {
        let tree = "({QUERY :targetList ({TARGETENTRY :resno 1 :resorigtbl 9 \
            :resorigcol 1 :resjunk true})})";
        assert_eq!(origins(tree), Vec::new());
    }

    /// The target list of a subquery is inside the range table, and
    /// it is not this view's target list.
    #[test]
    fn only_the_outer_target_list_counts() {
        let tree = "({QUERY :rtable ({RANGETBLENTRY :subquery {QUERY :targetList \
            ({TARGETENTRY :resno 1 :resorigtbl 7 :resorigcol 7 :resjunk false})}}) \
            :targetList ({TARGETENTRY :resno 1 :resorigtbl 9 :resorigcol 4 :resjunk false})})";
        assert_eq!(
            origins(tree),
            vec![Origin {
                column: 1,
                table: 9,
                table_column: 4
            }]
        );
    }

    /// Every space and bracket inside a name is behind a backslash
    /// when postgres prints it, so a column called `a (b)` is one
    /// word and does not close the node it sits in.
    #[test]
    fn a_name_with_brackets_in_it_does_not_end_the_node() {
        let tree = "({QUERY :targetList ({TARGETENTRY :resno 1 :resname a\\ \\(b\\) \
            :resorigtbl 9 :resorigcol 1 :resjunk false})})";
        assert_eq!(
            origins(tree),
            vec![Origin {
                column: 1,
                table: 9,
                table_column: 1
            }]
        );
    }

    #[test]
    fn a_view_takes_the_key_of_the_table_it_selects_from() {
        let views = vec![view(10, "books_view", &["ident", "written_by"], TREE)];
        let keys = vec![key(
            "books_author_id_fkey",
            (6474601, "books", 2, "author_id"),
            (99, "authors", 1, "id"),
        )];
        let rows = derive("test", &views, &keys);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].table, "books_view");
        assert_eq!(rows[0].columns, vec!["written_by"]);
        assert_eq!(rows[0].ref_table, "authors");
        assert_eq!(rows[0].ref_columns, vec!["id"]);
        assert_eq!(rows[0].constraint, "books_author_id_fkey");
    }

    /// The other end of the same key: a view over the parent is what
    /// the child table points at.
    #[test]
    fn a_view_over_the_parent_is_pointed_at_by_the_table() {
        let views = vec![view(10, "authors_view", &["ident", "who"], TREE)];
        let keys = vec![key(
            "books_author_id_fkey",
            (99, "books", 2, "author_id"),
            (6474601, "authors", 1, "id"),
        )];
        let rows = derive("test", &views, &keys);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].table, "books");
        assert_eq!(rows[0].columns, vec!["author_id"]);
        assert_eq!(rows[0].ref_table, "authors_view");
        assert_eq!(rows[0].ref_columns, vec!["ident"]);
    }

    /// Both ends at once, which is the case a view only schema is
    /// made of.
    #[test]
    fn two_views_over_the_two_ends_are_related_to_each_other() {
        let books = "({QUERY :targetList ({TARGETENTRY :resno 1 :resorigtbl 1 \
            :resorigcol 2 :resjunk false})})";
        let authors = "({QUERY :targetList ({TARGETENTRY :resno 1 :resorigtbl 2 \
            :resorigcol 1 :resjunk false})})";
        let views = vec![
            view(10, "books_view", &["written_by"], books),
            view(11, "authors_view", &["ident"], authors),
        ];
        let keys = vec![key(
            "books_author_id_fkey",
            (1, "books", 2, "author_id"),
            (2, "authors", 1, "id"),
        )];
        let rows = derive("test", &views, &keys);
        let pairs: Vec<(&str, &str)> = rows
            .iter()
            .map(|r| (r.table.as_str(), r.ref_table.as_str()))
            .collect();
        assert_eq!(
            pairs,
            vec![
                ("books_view", "authors"),
                ("books_view", "authors_view"),
                ("books", "authors_view"),
            ]
        );
    }

    /// A view over a view over a table still holds the table's key.
    #[test]
    fn the_chain_is_followed_to_the_table() {
        let middle = "({QUERY :targetList ({TARGETENTRY :resno 1 :resorigtbl 1 \
            :resorigcol 2 :resjunk false})})";
        let outer = "({QUERY :targetList ({TARGETENTRY :resno 1 :resorigtbl 10 \
            :resorigcol 1 :resjunk false})})";
        let views = vec![
            view(10, "middle", &["written_by"], middle),
            view(11, "outer", &["by"], outer),
        ];
        let keys = vec![key(
            "books_author_id_fkey",
            (1, "books", 2, "author_id"),
            (2, "authors", 1, "id"),
        )];
        let rows = derive("test", &views, &keys);
        let names: Vec<(&str, &str)> = rows
            .iter()
            .map(|r| (r.table.as_str(), r.columns[0].as_str()))
            .collect();
        assert_eq!(names, vec![("middle", "written_by"), ("outer", "by")]);
    }

    /// The view the chain goes through may be in a schema nobody
    /// exposes, and it is read for the sake of the one that is.
    #[test]
    fn a_view_from_another_schema_is_a_step_and_not_a_relationship() {
        let hidden = "({QUERY :targetList ({TARGETENTRY :resno 1 :resorigtbl 1 \
            :resorigcol 2 :resjunk false})})";
        let shown = "({QUERY :targetList ({TARGETENTRY :resno 1 :resorigtbl 10 \
            :resorigcol 1 :resjunk false})})";
        let mut inner = view(10, "hidden", &["written_by"], hidden);
        inner.schema = "private".to_string();
        let views = vec![inner, view(11, "shown", &["by"], shown)];
        let keys = vec![key(
            "books_author_id_fkey",
            (1, "books", 2, "author_id"),
            (2, "authors", 1, "id"),
        )];
        let rows = derive("test", &views, &keys);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].table, "shown");
    }

    /// A table in a schema the request cannot see is not a name it
    /// can embed, so the key it holds only counts through a view.
    #[test]
    fn a_table_in_another_schema_is_not_an_end() {
        let views = vec![view(10, "books_view", &["ident", "written_by"], TREE)];
        let mut hidden = key(
            "books_author_id_fkey",
            (6474601, "books", 2, "author_id"),
            (99, "authors", 1, "id"),
        );
        hidden.ref_schema = "private".to_string();
        assert_eq!(derive("test", &views, &[hidden]), Vec::new());
    }

    /// Selecting the same column twice is holding the key twice, and
    /// an embed on either name is a relationship of its own.
    #[test]
    fn a_column_selected_twice_is_two_relationships() {
        let tree = "({QUERY :targetList ({TARGETENTRY :resno 1 :resorigtbl 1 \
            :resorigcol 2 :resjunk false} {TARGETENTRY :resno 2 :resorigtbl 1 \
            :resorigcol 2 :resjunk false})})";
        let views = vec![view(10, "books_view", &["by", "author"], tree)];
        let keys = vec![key(
            "books_author_id_fkey",
            (1, "books", 2, "author_id"),
            (2, "authors", 1, "id"),
        )];
        let rows = derive("test", &views, &keys);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].columns, vec!["by"]);
        assert_eq!(rows[1].columns, vec!["author"]);
    }

    /// Half of a composite key is not the key, and a relationship
    /// that joined on half of one would join wrongly.
    #[test]
    fn part_of_a_key_is_no_key() {
        let tree = "({QUERY :targetList ({TARGETENTRY :resno 1 :resorigtbl 1 \
            :resorigcol 2 :resjunk false})})";
        let views = vec![view(10, "half", &["one"], tree)];
        let mut composite = key(
            "two_column_fkey",
            (1, "child", 2, "a"),
            (2, "parent", 1, "x"),
        );
        composite.attnums = vec![2, 3];
        composite.columns = vec!["a".to_string(), "b".to_string()];
        composite.ref_attnums = vec![1, 2];
        composite.ref_columns = vec!["x".to_string(), "y".to_string()];
        assert_eq!(derive("test", &views, &[composite]), Vec::new());
    }
}
