//! The compiled WHERE clauses against a live postgres.
//!
//! The compiler's whole type story is that every literal binds as a
//! text format parameter and the server infers the type from the
//! column it faces, so the proof has to run on a real server with
//! typed columns: integers, text, arrays, jsonb, tsvector, and a
//! range type, one probe per operator family.
//!
//! Gated on ZOU_PG_TEST_DSN like the zou-server suites, skips when
//! unset.

use tokio_postgres::NoTls;
use tokio_postgres::types::{Format, IsNull, ToSql, Type, to_sql_checked};
use zou_rest::catalog::{Catalog, FkRow, INTROSPECT_SQL, Kind};
use zou_rest::filter::{Node, Parsed, parse_pair};
use zou_rest::sql::where_clause;

/// A parameter sent in text format and accepted for any type, which
/// hands the parse to the server exactly like an inline unknown
/// literal, minus the injection surface.
#[derive(Debug)]
struct Text(String);

impl ToSql for Text {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.extend_from_slice(self.0.as_bytes());
        Ok(IsNull::No)
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }

    fn encode_format(&self, _ty: &Type) -> Format {
        Format::Text
    }

    to_sql_checked!();
}

async fn client() -> Option<tokio_postgres::Client> {
    let dsn = match std::env::var("ZOU_PG_TEST_DSN") {
        Ok(v) if !v.is_empty() => v,
        _ => {
            eprintln!("skipping: ZOU_PG_TEST_DSN not set");
            return None;
        }
    };
    let (client, conn) = tokio_postgres::connect(&dsn, NoTls).await.expect("connect");
    tokio::spawn(conn);
    client
        .batch_execute(
            "create temp table people (
                 id int primary key,
                 name text,
                 age int,
                 deleted_at timestamptz,
                 tags text[],
                 data jsonb,
                 body tsvector,
                 span int4range
             );
             insert into people values
               (1, 'John', 25, null, '{a,b}', '{\"city\":\"NYC\"}',
                to_tsvector('english', 'the fat cat'), '[1,5)'),
               (2, 'Jane', 65, now(), '{b,c}', '{\"city\":\"LA\"}',
                to_tsvector('english', 'a quick dog'), '[10,20)'),
               (3, 'Bob', 40, null, '{}', null,
                null, null);",
        )
        .await
        .expect("schema");
    Some(client)
}

fn nodes(pairs: &[(&str, &str)]) -> Vec<Node> {
    pairs
        .iter()
        .map(
            |(k, v)| match parse_pair(k, v).unwrap_or_else(|e| panic!("{k}={v} failed: {e}")) {
                Parsed::Filter(c) => Node::Cond(c),
                Parsed::Logic {
                    embed,
                    op,
                    negated,
                    kids,
                } => {
                    assert!(embed.is_empty());
                    Node::Group { op, negated, kids }
                }
            },
        )
        .collect()
}

async fn ids(client: &tokio_postgres::Client, pairs: &[(&str, &str)]) -> Vec<i32> {
    let sql = where_clause(&nodes(pairs)).expect("compile");
    let text = format!("select id from people where {} order by id", sql.text);
    let params: Vec<Text> = sql.params.into_iter().map(Text).collect();
    let refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
    let rows = client
        .query(&text, &refs)
        .await
        .unwrap_or_else(|e| panic!("{text}: {e}"));
    rows.iter().map(|r| r.get(0)).collect()
}

#[tokio::test]
async fn the_server_infers_every_parameter_type() {
    let Some(c) = client().await else { return };

    // Integers through comparison and lists.
    assert_eq!(ids(&c, &[("age", "gte.40")]).await, vec![2, 3]);
    assert_eq!(ids(&c, &[("age", "in.(25,65)")]).await, vec![1, 2]);
    assert_eq!(ids(&c, &[("age", "eq(any).{25,40}")]).await, vec![1, 3]);

    // Text through patterns, both directions of the star.
    assert_eq!(ids(&c, &[("name", "like.J*")]).await, vec![1, 2]);
    assert_eq!(ids(&c, &[("name", "ilike.*OB")]).await, vec![3]);
    assert_eq!(ids(&c, &[("name", "like(all).{J*,*n}")]).await, vec![1]);
    assert_eq!(ids(&c, &[("name", "match.^J.*n$")]).await, vec![1]);

    // Booleans and null through is.
    assert_eq!(ids(&c, &[("deleted_at", "is.null")]).await, vec![1, 3]);
    assert_eq!(ids(&c, &[("deleted_at", "not.is.null")]).await, vec![2]);

    // Arrays through contains and overlap.
    assert_eq!(ids(&c, &[("tags", "cs.{a}")]).await, vec![1]);
    assert_eq!(ids(&c, &[("tags", "ov.{a,c}")]).await, vec![1, 2]);
    assert_eq!(ids(&c, &[("tags", "cd.{a,b,c}")]).await, vec![1, 2, 3]);

    // Jsonb through arrows, the text extraction lands as text.
    assert_eq!(ids(&c, &[("data->>city", "eq.NYC")]).await, vec![1]);
    assert_eq!(ids(&c, &[("data", "cs.{\"city\":\"LA\"}")]).await, vec![2]);

    // Full text search with and without a configuration.
    assert_eq!(ids(&c, &[("body", "fts.cat")]).await, vec![1]);
    assert_eq!(ids(&c, &[("body", "plfts(english).dogs")]).await, vec![2]);
    assert_eq!(ids(&c, &[("body", "wfts.fat or quick")]).await, vec![1, 2]);

    // Ranges through the geometric family, which pins the &< and &>
    // spellings to their PostgREST meanings.
    assert_eq!(ids(&c, &[("span", "sl.[6,8)")]).await, vec![1]);
    assert_eq!(ids(&c, &[("span", "sr.[6,8)")]).await, vec![2]);
    assert_eq!(ids(&c, &[("span", "nxr.[0,18)")]).await, vec![1]);
    assert_eq!(ids(&c, &[("span", "nxl.[3,30)")]).await, vec![2]);
    assert_eq!(ids(&c, &[("span", "adj.[5,7)")]).await, vec![1]);

    // isdistinct treats null as a comparable value.
    assert_eq!(ids(&c, &[("age", "isdistinct.25")]).await, vec![2, 3]);

    // Logic trees and the implicit and across pairs.
    assert_eq!(
        ids(&c, &[("or", "(age.lt.30,age.gt.60)")]).await,
        vec![1, 2]
    );
    assert_eq!(
        ids(&c, &[("not.and", "(name.like.J*,age.lt.30)")]).await,
        vec![2, 3]
    );
    assert_eq!(
        ids(&c, &[("age", "gte.30"), ("name", "like.J*")]).await,
        vec![2]
    );
}

/// The introspection query against real constraints: temp tables
/// live in pg_temp, so this one builds a throwaway schema, loads the
/// catalog from it, and checks the resolutions the unit tests pin
/// with hand written rows.
#[tokio::test]
async fn the_introspection_query_reads_the_fk_graph() {
    let Some(c) = client().await else { return };

    c.batch_execute(
        "drop schema if exists zou_embed cascade;
         create schema zou_embed;
         set search_path to zou_embed;
         create table users (id int primary key);
         create table profiles (
             id int primary key,
             user_id int not null unique references users
         );
         create table addresses (id int primary key);
         create table orders (
             id int primary key,
             user_id int references users,
             billing_address_id int references addresses,
             shipping_address_id int references addresses
         );
         create table products (id int primary key);
         create table order_items (
             order_id int references orders,
             product_id int references products,
             primary key (order_id, product_id)
         );",
    )
    .await
    .expect("embed schema");

    let rows = c
        .query(INTROSPECT_SQL, &[&"zou_embed"])
        .await
        .expect("introspect");
    let fks: Vec<FkRow> = rows
        .iter()
        .map(|r| FkRow {
            constraint: r.get(0),
            table: r.get(1),
            columns: r.get(2),
            ref_table: r.get(3),
            ref_columns: r.get(4),
            unique: r.get(5),
            in_pk: r.get(6),
        })
        .collect();
    assert_eq!(fks.len(), 6, "six fks in the schema: {fks:?}");
    let catalog = Catalog::new(fks);

    let r = catalog.resolve("users", "orders", None).expect("to many");
    assert_eq!(r.kind, Kind::ToMany);
    assert_eq!(r.join, vec![("id".into(), "user_id".into())]);

    let r = catalog
        .resolve("users", "profiles", None)
        .expect("one to one");
    assert_eq!(r.kind, Kind::ToOne, "the unique fk flag came through");

    let r = catalog
        .resolve("orders", "products", None)
        .expect("many to many");
    assert_eq!(r.kind, Kind::ToMany);
    assert_eq!(
        r.via.expect("a junction").table,
        "order_items",
        "the in_pk flags marked the junction"
    );

    let e = catalog.resolve("orders", "addresses", None).unwrap_err();
    assert_eq!(e.code, "PGRST201");

    let r = catalog
        .resolve("orders", "addresses", Some("billing_address_id"))
        .expect("the column hint");
    assert_eq!(r.join, vec![("billing_address_id".into(), "id".into())]);

    c.batch_execute("drop schema zou_embed cascade")
        .await
        .expect("cleanup");
}

/// A partitioned table's keys, which only postgres can tell apart. A
/// partition carries a copy of every key its parent declared, and the
/// copy is a row of pg_constraint like any other: nothing about the
/// name, the columns or the tables says it was inherited, only
/// conparentid does. Left in, a partitioned junction is one
/// relationship per partition and every embed through it is
/// ambiguous.
#[tokio::test]
async fn an_inherited_key_is_not_a_relationship_of_its_own() {
    let Some(c) = client().await else { return };

    c.batch_execute(
        "drop schema if exists zou_part cascade;
         create schema zou_part;
         set search_path to zou_part;
         create table models (id int primary key);
         create table sales (
             id int,
             model_id int references models,
             sold_on date not null,
             primary key (id, sold_on)
         ) partition by range (sold_on);
         create table sales_2101 partition of sales
             for values from ('2021-01-01') to ('2021-02-01');
         create table sales_2102 partition of sales
             for values from ('2021-02-01') to ('2021-03-01');",
    )
    .await
    .expect("partitioned schema");

    let rows = c
        .query(INTROSPECT_SQL, &[&"zou_part"])
        .await
        .expect("introspect");
    let fks: Vec<FkRow> = rows
        .iter()
        .map(|r| FkRow {
            constraint: r.get(0),
            table: r.get(1),
            columns: r.get(2),
            ref_table: r.get(3),
            ref_columns: r.get(4),
            unique: r.get(5),
            in_pk: r.get(6),
        })
        .collect();
    assert_eq!(fks.len(), 1, "the declared key and no copies: {fks:?}");
    assert_eq!(fks[0].table, "sales");

    let catalog = Catalog::new(fks).with_schema("zou_part");
    assert_eq!(
        catalog.resolve("models", "sales", None).unwrap().kind,
        Kind::ToMany
    );

    // The partition is not a relation the api has, so an embed of it
    // is a relationship nobody has, spelled near enough to the one
    // they meant to say so.
    let e = catalog.resolve("models", "sales_2101", None).unwrap_err();
    assert_eq!(e.code, "PGRST200");
    assert_eq!(
        e.hint,
        Some("Perhaps you meant 'sales' instead of 'sales_2101'.".to_string())
    );

    c.batch_execute("drop schema zou_part cascade")
        .await
        .expect("cleanup");
}

/// The computed relationship query against real functions, and the
/// call it plans executed. What makes this worth a live server is
/// that none of it can be checked anywhere else: which functions
/// count as relationships is a question about pg_proc, and whether
/// the call postgres accepts is a question for postgres.
#[tokio::test]
async fn the_computed_query_reads_the_functions_over_a_relation() {
    use zou_rest::catalog::{COMPUTED_SQL, ComputedRow};
    use zou_rest::{plan, select};

    let Some(c) = client().await else { return };

    c.batch_execute(
        "drop schema if exists zou_computed cascade;
         create schema zou_computed;
         set search_path to zou_computed;
         create table users (id int primary key, name text);
         create table orders (id int primary key, user_id int, total int);
         insert into users values (1, 'ann'), (2, 'bob');
         insert into orders values (10, 1, 100), (11, 1, 50), (12, 2, 75);
         create function big_orders(users) returns setof orders as $$
           select * from orders where user_id = $1.id and total > 60
         $$ language sql stable;
         create function buyer(orders) returns setof users rows 1 as $$
           select * from users where id = $1.user_id
         $$ language sql stable;
         -- One argument but not a relation's row, so not a
         -- relationship however it is spelled.
         create function louder(text) returns text as $$
           select upper($1)
         $$ language sql immutable;",
    )
    .await
    .expect("computed schema");

    let rows = c
        .query(COMPUTED_SQL, &[&"zou_computed"])
        .await
        .expect("introspect");
    let computed: Vec<ComputedRow> = rows
        .iter()
        .map(|r| ComputedRow {
            function: r.get(0),
            table: r.get(1),
            ftable: r.get(2),
            single: r.get(3),
        })
        .collect();
    assert_eq!(
        computed,
        vec![
            ComputedRow {
                function: "big_orders".into(),
                table: "users".into(),
                ftable: "orders".into(),
                single: false,
            },
            ComputedRow {
                function: "buyer".into(),
                table: "orders".into(),
                ftable: "users".into(),
                single: true,
            },
        ],
    );

    // The call itself, run: the parent row goes in as the argument
    // and there is no join condition anywhere in the statement.
    let catalog = Catalog::new(Vec::new()).with_computed(computed);
    let q = plan::Query {
        table: "users".into(),
        select: select::parse("name,big_orders(total)").expect("select"),
        ..plan::Query::default()
    };
    let sql = plan::plan(&catalog, &q).expect("plan");
    let text = format!(
        "select to_jsonb(t)::text from ({}) as t order by 1",
        sql.text
    );
    let rows = c
        .query(&text, &[])
        .await
        .unwrap_or_else(|e| panic!("{text}: {e}"));
    let got: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(
        got,
        vec![
            r#"{"name": "ann", "big_orders": [{"total": 100}]}"#,
            r#"{"name": "bob", "big_orders": [{"total": 75}]}"#,
        ]
    );

    c.batch_execute("drop schema zou_computed cascade")
        .await
        .expect("cleanup");
}

/// The planner's output executed for real: every lateral shape runs
/// and the rows come back as the json a PostgREST client expects.
/// Its own schema name, the introspection test drops zou_embed while
/// this one runs.
#[tokio::test]
async fn planned_queries_return_postgrest_shaped_rows() {
    use zou_rest::filter::{Node, Parsed, parse_pair};
    use zou_rest::{order, plan, select};

    let Some(c) = client().await else { return };

    c.batch_execute(
        "drop schema if exists zou_plan cascade;
         create schema zou_plan;
         set search_path to zou_plan;
         create table users (id int primary key, name text);
         create table profiles (
             id int primary key,
             user_id int not null unique references users,
             bio text
         );
         create table orders (
             id int primary key,
             user_id int references users,
             total int
         );
         create table products (id int primary key);
         create table order_items (
             order_id int references orders,
             product_id int references products,
             primary key (order_id, product_id)
         );
         insert into users values (1, 'ann'), (2, 'bob');
         insert into profiles values (1, 1, 'ann bio');
         insert into orders values (10, 1, 100), (11, 1, 50), (12, 2, 75);
         insert into products values (1), (2);
         insert into order_items values (10, 1), (10, 2), (11, 1);",
    )
    .await
    .expect("plan schema");

    let rows = c
        .query(zou_rest::catalog::INTROSPECT_SQL, &[&"zou_plan"])
        .await
        .expect("introspect");
    let catalog = Catalog::new(
        rows.iter()
            .map(|r| FkRow {
                constraint: r.get(0),
                table: r.get(1),
                columns: r.get(2),
                ref_table: r.get(3),
                ref_columns: r.get(4),
                unique: r.get(5),
                in_pk: r.get(6),
            })
            .collect(),
    );

    let run = async |q: &plan::Query, expect: &[&str]| {
        let sql = plan::plan(&catalog, q).unwrap_or_else(|e| panic!("{e}"));
        let text = format!("select to_jsonb(t)::text from ({}) as t", sql.text);
        let params: Vec<Text> = sql.params.into_iter().map(Text).collect();
        let refs: Vec<&(dyn ToSql + Sync)> =
            params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
        let rows = c
            .query(&text, &refs)
            .await
            .unwrap_or_else(|e| panic!("{text}: {e}"));
        let got: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
        assert_eq!(got, expect, "for {text}");
    };

    let query = |table: &str, sel: &str| plan::Query {
        table: table.into(),
        select: select::parse(sel).unwrap(),
        ..plan::Query::default()
    };
    let filt = |q: &mut plan::Query, key: &str, value: &str| match parse_pair(key, value).unwrap() {
        Parsed::Filter(cond) => q.filters.push((Vec::new(), Node::Cond(cond))),
        Parsed::Logic {
            embed,
            op,
            negated,
            kids,
        } => q.filters.push((embed, Node::Group { op, negated, kids })),
    };
    let root_order = |q: &mut plan::Query, terms: &str| {
        q.order.push((Vec::new(), order::parse(terms).unwrap()));
    };

    // To many with nested order and limit.
    let mut q = query("users", "id,orders(total)");
    root_order(&mut q, "id");
    q.order
        .push((vec!["orders".into()], order::parse("total.desc").unwrap()));
    q.limit.push((vec!["orders".into()], 1));
    run(
        &q,
        &[
            r#"{"id": 1, "orders": [{"total": 100}]}"#,
            r#"{"id": 2, "orders": [{"total": 75}]}"#,
        ],
    )
    .await;

    // To one, and the empty side of it.
    let mut q = query("users", "id,profiles(bio)");
    root_order(&mut q, "id");
    run(
        &q,
        &[
            r#"{"id": 1, "profiles": {"bio": "ann bio"}}"#,
            r#"{"id": 2, "profiles": null}"#,
        ],
    )
    .await;

    // An inner empty embed with a routed filter is pure existence.
    let mut q = query("users", "id,orders!inner()");
    filt(&mut q, "orders.total", "gte.100");
    run(&q, &[r#"{"id": 1}"#]).await;

    // Many to many through the junction, and the empty array.
    let mut q = query("orders", "id,products(id)");
    root_order(&mut q, "id");
    q.order
        .push((vec!["products".into()], order::parse("id").unwrap()));
    run(
        &q,
        &[
            r#"{"id": 10, "products": [{"id": 1}, {"id": 2}]}"#,
            r#"{"id": 11, "products": [{"id": 1}]}"#,
            r#"{"id": 12, "products": []}"#,
        ],
    )
    .await;

    // A spread folds the to one columns into the parent.
    let mut q = query("orders", "id,...users(name)");
    filt(&mut q, "id", "eq.12");
    run(&q, &[r#"{"id": 12, "name": "bob"}"#]).await;

    // Aggregates group by the plain columns.
    let mut q = query("orders", "user_id,total.sum()");
    root_order(&mut q, "user_id");
    run(
        &q,
        &[
            r#"{"sum": 150, "user_id": 1}"#,
            r#"{"sum": 75, "user_id": 2}"#,
        ],
    )
    .await;

    c.batch_execute("drop schema zou_plan cascade")
        .await
        .expect("cleanup");
}

/// Every mutation shape executed for real: the payload binds as one
/// json parameter, the conflict clauses resolve, RETURNING hands
/// rows back, and the representation CTE feeds the planner with an
/// embed resolving against the real table.
#[tokio::test]
async fn mutations_execute_and_read_back_through_the_planner() {
    use zou_rest::mutate::{self, Conflict, Missing, Returning};
    use zou_rest::{plan, select};

    let Some(c) = client().await else { return };

    c.batch_execute(
        "drop schema if exists zou_mut cascade;
         create schema zou_mut;
         set search_path to zou_mut;
         create table authors (id int primary key, name text);
         create table books (
             id int primary key,
             author_id int references authors,
             title text,
             price int default 7
         );
         insert into authors values (1, 'ann'), (2, 'bob');",
    )
    .await
    .expect("mut schema");

    let cols = |names: &[&str]| names.iter().map(|s| s.to_string()).collect::<Vec<String>>();
    let run = async |sql: &zou_rest::sql::Sql| -> Vec<tokio_postgres::Row> {
        let params: Vec<Text> = sql.params.iter().cloned().map(Text).collect();
        let refs: Vec<&(dyn ToSql + Sync)> =
            params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
        c.query(&sql.text, &refs)
            .await
            .unwrap_or_else(|e| panic!("{}: {e}", sql.text))
    };

    // Insert unpacks the array against the row type and the default
    // fills the absent price.
    let s = mutate::insert(
        "books",
        None,
        &cols(&["id", "author_id", "title"]),
        r#"[{"id":1,"author_id":1,"title":"a1"},{"id":2,"author_id":2,"title":"b1"}]"#.into(),
        Missing::Null,
        None,
        &Returning::Cols(cols(&["id", "price"])),
    )
    .unwrap();
    let rows = run(&s).await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>("price"), 7, "the default filled in");

    // merge-duplicates overwrites the clashing row in place and
    // still inserts the fresh one.
    let s = mutate::insert(
        "books",
        None,
        &cols(&["id", "author_id", "title"]),
        r#"[{"id":2,"author_id":2,"title":"b2"},{"id":3,"author_id":1,"title":"a2"}]"#.into(),
        Missing::Null,
        Some(&Conflict::Merge {
            target: cols(&["id"]),
            set: cols(&["author_id", "title"]),
        }),
        &Returning::Cols(cols(&["id", "title"])),
    )
    .unwrap();
    let rows = run(&s).await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>("title"), "b2");

    // ignore-duplicates drops the clash instead.
    let s = mutate::insert(
        "books",
        None,
        &cols(&["id", "author_id", "title"]),
        r#"[{"id":3,"author_id":2,"title":"zzz"},{"id":4,"author_id":2,"title":"b3"}]"#.into(),
        Missing::Null,
        Some(&Conflict::Ignore {
            target: cols(&["id"]),
        }),
        &Returning::Cols(cols(&["id"])),
    )
    .unwrap();
    let rows = run(&s).await;
    assert_eq!(rows.len(), 1, "only the fresh row landed");
    assert_eq!(rows[0].get::<_, i32>("id"), 4);

    // An update binds the payload as $1 and the filter after it,
    // and the qualified RETURNING dodges the payload columns.
    let s = mutate::update(
        "books",
        None,
        &cols(&["price"]),
        r#"{"price":30}"#.into(),
        Missing::Null,
        &nodes(&[("id", "gte.3")]),
        &Returning::Star,
    )
    .unwrap();
    let rows = run(&s).await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>("price"), 30);

    // Delete hands back what it removed.
    let s = mutate::delete(
        "books",
        None,
        &nodes(&[("id", "eq.4")]),
        &Returning::Cols(cols(&["id"])),
    )
    .unwrap();
    let rows = run(&s).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>("id"), 4);

    // The representation: an insert mounts as the CTE, the planner
    // reads the returned rows at the root, and the embed still
    // resolves and joins against the real authors table.
    let fk_rows = c
        .query(INTROSPECT_SQL, &[&"zou_mut"])
        .await
        .expect("introspect");
    let catalog = Catalog::new(
        fk_rows
            .iter()
            .map(|r| FkRow {
                constraint: r.get(0),
                table: r.get(1),
                columns: r.get(2),
                ref_table: r.get(3),
                ref_columns: r.get(4),
                unique: r.get(5),
                in_pk: r.get(6),
            })
            .collect(),
    );
    let m = mutate::insert(
        "books",
        None,
        &cols(&["id", "author_id", "title"]),
        r#"[{"id":9,"author_id":1,"title":"a9"}]"#.into(),
        Missing::Null,
        None,
        &Returning::Star,
    )
    .unwrap();
    let mut q = plan::Query {
        table: "books".into(),
        select: select::parse("id,title,authors(name)").unwrap(),
        ..plan::Query::default()
    };
    let r = mutate::representation(&catalog, m, &mut q).unwrap();
    let s = zou_rest::sql::Sql {
        text: format!(
            "with {} select to_jsonb(t)::text from ({}) as t",
            r.cte, r.select.text
        ),
        params: r.select.params,
    };
    let rows = run(&s).await;
    let got: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(
        got,
        vec![r#"{"id": 9, "title": "a9", "authors": {"name": "ann"}}"#.to_string()]
    );

    c.batch_execute("drop schema zou_mut cascade")
        .await
        .expect("cleanup");
}
