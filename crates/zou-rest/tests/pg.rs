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
