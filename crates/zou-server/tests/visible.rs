//! What a subscriber may see of a change, against a live postgres.
//!
//! There is nothing to unit test here worth the name: every answer is
//! the database's, and a fake that agreed with this code about what a
//! policy means would only prove the two agree. So these write real
//! policies and real grants, change a row, and ask what a subscriber
//! with a given role and a given `sub` claim is owed of it.
//!
//! The claim that matters is that a subscriber sees exactly what the
//! same policy would have shown them through the rest api, since that
//! is what a project's policies were written against and the reason
//! this is asked of the database rather than decided here.
//!
//! Gated on ZOU_PG_TEST_DSN and skipped when that database is not
//! logical, the same as the tap's own suite.
//!
//!   ZOU_PG_TEST_DSN="host=127.0.0.1 port=5432 user=postgres dbname=postgres" \
//!     cargo test -p zou-server --test visible

use tokio_postgres::{Client, NoTls};
use zou_server::cdc::{Closed, PUBLICATION, Tap};
use zou_server::payload::{NO_KEY, Seen, UNAUTHORIZED};
use zou_server::pgoutput::Change;
use zou_server::sql::Pool;
use zou_server::visible::{Asker, Catalog, seen};

/// Two people, so a policy has somebody to keep a row from.
const ANA: &str = "11111111-1111-1111-1111-111111111111";
const BEN: &str = "22222222-2222-2222-2222-222222222222";

fn dsn() -> Option<String> {
    match std::env::var("ZOU_PG_TEST_DSN") {
        Ok(v) if !v.is_empty() => Some(v),
        _ => {
            eprintln!("skipping: ZOU_PG_TEST_DSN not set");
            None
        }
    }
}

async fn connect(dsn: &str) -> Client {
    let (client, connection) = dsn
        .parse::<tokio_postgres::Config>()
        .expect("a dsn")
        .connect(NoTls)
        .await
        .expect("a connection");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    zou_server::sql::bootstrap(&client)
        .await
        .expect("the bootstrap contract");
    client
}

/// A signed in subscriber, which is the role a token names and the
/// claims a policy reads out of it.
fn user(sub: &str) -> Asker {
    Asker {
        role: "authenticated".into(),
        claims: serde_json::json!({"sub": sub, "role": "authenticated"}),
    }
}

fn anon() -> Asker {
    Asker {
        role: "anon".into(),
        claims: serde_json::json!({"role": "anon"}),
    }
}

/// What a subscriber may see, catalog read and all, which is the two
/// halves the server keeps apart put back together for a test: the
/// facts about the table and the role, then the question about the row.
/// A cache of its own per call, so no answer here is one an earlier
/// test left behind.
async fn seen_by(pool: &Pool, asker: &Asker, change: &Change) -> Result<Seen, String> {
    let mut catalog = Catalog::new(std::sync::Arc::new(std::sync::atomic::AtomicU64::new(1)));
    let facts = catalog
        .facts(pool, &change.relation, &asker.role)
        .await
        .expect("the catalog");
    seen(pool, &facts, asker, change).await
}

/// A table in the publication, dropped and rebuilt so a rerun starts
/// where the last run did.
async fn published(client: &Client, table: &str, ddl: &str) {
    client
        .batch_execute(&format!(
            "drop table if exists {table};
             {ddl}
             alter publication {PUBLICATION} add table {table}"
        ))
        .await
        .expect("a table in the publication");
}

async fn tapping(dsn: &str) -> Option<Tap> {
    match Tap::open(dsn, PUBLICATION).await {
        Ok(tap) => Some(tap),
        Err(Closed::NotLogical(level)) => {
            eprintln!("skipping: wal_level is {level}");
            None
        }
        Err(other) => panic!("no tap: {other}"),
    }
}

/// Poll until this test's own tables have said everything it is waiting
/// for.
///
/// Everything read is kept, because a read comes back with whatever the
/// slot had in it and a test that wanted two tables would otherwise
/// throw the second one away while waiting for the first.
async fn until(tap: &mut Tap, want: &[(&str, usize)]) -> Vec<Change> {
    let mut changes: Vec<Change> = Vec::new();
    let counted = |changes: &Vec<Change>, table: &str| {
        changes
            .iter()
            .filter(|change| change.relation.table == table)
            .count()
    };
    for _ in 0..100 {
        changes.extend(tap.changes(0).await.expect("a read"));
        if want
            .iter()
            .all(|(table, many)| counted(&changes, table) >= *many)
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    changes
}

/// This table's changes, in the order postgres wrote them.
fn of(changes: &[Change], table: &str) -> Vec<Change> {
    changes
        .iter()
        .filter(|change| change.relation.table == table)
        .cloned()
        .collect()
}

/// Which of a relation's columns a subscriber may select, by name,
/// which is easier to read in a failure than a vector of bools.
fn columns(change: &Change, seen: &Seen) -> Vec<String> {
    change
        .relation
        .columns
        .iter()
        .zip(seen.columns.iter())
        .filter(|(_, may)| **may)
        .map(|(column, _)| column.name.clone())
        .collect()
}

#[tokio::test]
async fn a_policy_decides_who_is_told_about_a_row() {
    let Some(dsn) = dsn() else { return };
    let client = connect(&dsn).await;
    published(
        &client,
        "vis_own",
        "create table vis_own (id int primary key, owner uuid, details text);
         grant select on vis_own to anon, authenticated;
         alter table vis_own enable row level security;
         create policy mine on vis_own for select using (owner = auth.uid());",
    )
    .await;
    let Some(mut tap) = tapping(&dsn).await else {
        return;
    };
    let pool = Pool::new(&dsn, 4).expect("a pool");

    client
        .batch_execute(&format!(
            "insert into vis_own values (1, '{ANA}', 'hers'), (2, '{BEN}', 'his')"
        ))
        .await
        .expect("two rows");
    let changes = of(&until(&mut tap, &[("vis_own", 2)]).await, "vis_own");
    assert_eq!(changes.len(), 2, "two inserts");

    let hers = seen_by(&pool, &user(ANA), &changes[0])
        .await
        .expect("an answer");
    assert!(hers.row, "the policy is what lets her see her own row");
    let his = seen_by(&pool, &user(ANA), &changes[1])
        .await
        .expect("an answer");
    assert!(
        !his.row,
        "and the same policy is what keeps her from seeing his, which is the whole reason to ask"
    );

    let theirs = seen_by(&pool, &anon(), &changes[0])
        .await
        .expect("an answer");
    assert!(
        !theirs.row,
        "a subscriber with no subject matches no row this policy allows"
    );
}

#[tokio::test]
async fn an_update_is_checked_against_the_row_it_became() {
    let Some(dsn) = dsn() else { return };
    let client = connect(&dsn).await;
    published(
        &client,
        "vis_moved",
        "create table vis_moved (id int primary key, owner uuid, details text);
         grant select on vis_moved to anon, authenticated;
         alter table vis_moved enable row level security;
         create policy mine on vis_moved for select using (owner = auth.uid());",
    )
    .await;
    let Some(mut tap) = tapping(&dsn).await else {
        return;
    };
    let pool = Pool::new(&dsn, 4).expect("a pool");

    client
        .batch_execute(&format!(
            "insert into vis_moved values (1, '{ANA}', 'hers');
             update vis_moved set owner = '{BEN}' where id = 1"
        ))
        .await
        .expect("a row that changed hands");
    let changes = of(&until(&mut tap, &[("vis_moved", 2)]).await, "vis_moved");
    assert_eq!(changes.len(), 2, "an insert and an update");

    let hers = seen_by(&pool, &user(ANA), &changes[1])
        .await
        .expect("an answer");
    assert!(
        !hers.row,
        "the check is a select of the row as it is now, so an update that moves a row out of a \
         policy's view is not told to the person it moved away from, which is upstream's \
         behaviour and worth knowing rather than worth relying on"
    );
    let his = seen_by(&pool, &user(BEN), &changes[1])
        .await
        .expect("an answer");
    assert!(his.row, "and it is told to the person it moved to");
}

#[tokio::test]
async fn a_table_with_no_policies_is_seen_by_anybody_the_grant_lets_read_it() {
    let Some(dsn) = dsn() else { return };
    let client = connect(&dsn).await;
    published(
        &client,
        "vis_open",
        "create table vis_open (id int primary key, details text);
         grant select on vis_open to anon, authenticated;",
    )
    .await;
    let Some(mut tap) = tapping(&dsn).await else {
        return;
    };
    let pool = Pool::new(&dsn, 4).expect("a pool");

    client
        .batch_execute("insert into vis_open values (1, 'public knowledge')")
        .await
        .expect("a row");
    let changes = of(&until(&mut tap, &[("vis_open", 1)]).await, "vis_open");
    let answer = seen_by(&pool, &anon(), &changes[0])
        .await
        .expect("an answer");
    assert!(
        answer.row,
        "row level security off means the grant is the whole answer, and asking a policy that \
         does not exist would be inventing one"
    );
    assert_eq!(columns(&changes[0], &answer), vec!["id", "details"]);
}

#[tokio::test]
async fn a_column_nobody_granted_is_not_in_what_a_subscriber_may_see() {
    let Some(dsn) = dsn() else { return };
    let client = connect(&dsn).await;
    published(
        &client,
        "vis_cols",
        "create table vis_cols (id int primary key, details text, secret text);
         revoke all on vis_cols from anon, authenticated;
         grant select (id, details) on vis_cols to authenticated;",
    )
    .await;
    let Some(mut tap) = tapping(&dsn).await else {
        return;
    };
    let pool = Pool::new(&dsn, 4).expect("a pool");

    client
        .batch_execute("insert into vis_cols values (1, 'shown', 'not shown')")
        .await
        .expect("a row");
    let changes = of(&until(&mut tap, &[("vis_cols", 1)]).await, "vis_cols");
    let answer = seen_by(&pool, &user(ANA), &changes[0])
        .await
        .expect("an answer");
    assert_eq!(
        columns(&changes[0], &answer),
        vec!["id", "details"],
        "a grant of select on some columns is a subscription to those columns"
    );
    assert!(answer.row, "the row itself has no policy keeping it back");
    assert_eq!(answer.error, None);
}

#[tokio::test]
async fn a_subscriber_who_may_not_select_the_key_is_told_why() {
    let Some(dsn) = dsn() else { return };
    let client = connect(&dsn).await;
    published(
        &client,
        "vis_key",
        "create table vis_key (id int primary key, details text);
         revoke all on vis_key from anon, authenticated;
         grant select (details) on vis_key to authenticated;",
    )
    .await;
    let Some(mut tap) = tapping(&dsn).await else {
        return;
    };
    let pool = Pool::new(&dsn, 4).expect("a pool");

    client
        .batch_execute("insert into vis_key values (1, 'a detail')")
        .await
        .expect("a row");
    let changes = of(&until(&mut tap, &[("vis_key", 1)]).await, "vis_key");
    let answer = seen_by(&pool, &user(ANA), &changes[0])
        .await
        .expect("an answer");
    assert_eq!(
        answer.error,
        Some(UNAUTHORIZED),
        "the row cannot be checked without reading the column that names it, and a check that \
         cannot run is not a check that passed"
    );
}

#[tokio::test]
async fn a_table_with_no_primary_key_is_an_error_and_not_a_row() {
    let Some(dsn) = dsn() else { return };
    let client = connect(&dsn).await;
    published(
        &client,
        "vis_nokey",
        "create table vis_nokey (details text);
         grant select on vis_nokey to anon, authenticated;
         alter table vis_nokey enable row level security;
         create policy everybody on vis_nokey for select using (true);",
    )
    .await;
    let Some(mut tap) = tapping(&dsn).await else {
        return;
    };
    let pool = Pool::new(&dsn, 4).expect("a pool");

    client
        .batch_execute("insert into vis_nokey values ('nothing names me')")
        .await
        .expect("a row");
    let changes = of(&until(&mut tap, &[("vis_nokey", 1)]).await, "vis_nokey");
    let answer = seen_by(&pool, &user(ANA), &changes[0])
        .await
        .expect("an answer");
    assert_eq!(
        answer.error,
        Some(NO_KEY),
        "there is no way to select the row back, so there is no way to ask the policy about it, \
         and upstream says so rather than guessing"
    );
}

#[tokio::test]
async fn a_delete_reaches_everybody_and_says_only_its_key_where_there_are_policies() {
    let Some(dsn) = dsn() else { return };
    let client = connect(&dsn).await;
    published(
        &client,
        "vis_del",
        "create table vis_del (id int primary key, owner uuid, details text);
         grant select on vis_del to anon, authenticated;
         alter table vis_del enable row level security;
         create policy mine on vis_del for select using (owner = auth.uid());
         alter table vis_del replica identity full;",
    )
    .await;
    published(
        &client,
        "vis_del_open",
        "create table vis_del_open (id int primary key, details text);
         grant select on vis_del_open to anon, authenticated;
         alter table vis_del_open replica identity full;",
    )
    .await;
    let Some(mut tap) = tapping(&dsn).await else {
        return;
    };
    let pool = Pool::new(&dsn, 4).expect("a pool");

    client
        .batch_execute(&format!(
            "insert into vis_del values (1, '{ANA}', 'hers');
             delete from vis_del where id = 1;
             insert into vis_del_open values (1, 'anybody');
             delete from vis_del_open where id = 1"
        ))
        .await
        .expect("a row that came and went, twice");
    let read = until(&mut tap, &[("vis_del", 2), ("vis_del_open", 2)]).await;
    let guarded = of(&read, "vis_del");
    let open = of(&read, "vis_del_open");

    let answer = seen_by(&pool, &user(BEN), &guarded[1])
        .await
        .expect("an answer");
    assert!(
        answer.row,
        "the row is gone, so no policy can be asked about it, and upstream publishes the delete \
         to everybody rather than to nobody"
    );
    assert!(
        answer.keys_only,
        "what a subscriber is owed is that a row with that key is gone, not what was in it"
    );

    let answer = seen_by(&pool, &anon(), &open[1]).await.expect("an answer");
    assert!(
        !answer.keys_only,
        "a table with no policies on it publishes what its replica identity publishes"
    );
}

#[tokio::test]
async fn a_role_the_database_does_not_have_sees_nothing() {
    let Some(dsn) = dsn() else { return };
    let client = connect(&dsn).await;
    published(
        &client,
        "vis_gone",
        "create table vis_gone (id int primary key, details text);
         grant select on vis_gone to anon, authenticated;",
    )
    .await;
    let Some(mut tap) = tapping(&dsn).await else {
        return;
    };
    let pool = Pool::new(&dsn, 4).expect("a pool");

    client
        .batch_execute("insert into vis_gone values (1, 'a detail')")
        .await
        .expect("a row");
    let changes = of(&until(&mut tap, &[("vis_gone", 1)]).await, "vis_gone");
    let asker = Asker {
        role: "a_role_nobody_created".into(),
        claims: serde_json::json!({"sub": ANA, "role": "a_role_nobody_created"}),
    };
    let answer = seen_by(&pool, &asker, &changes[0])
        .await
        .expect("an answer");
    assert_eq!(
        columns(&changes[0], &answer),
        Vec::<String>::new(),
        "a token naming a role somebody dropped is a token with no privileges, and the safe \
         reading of no privileges is none rather than all"
    );
    assert_eq!(answer.error, Some(UNAUTHORIZED));
}

#[tokio::test]
async fn a_role_that_bypasses_row_level_security_is_told_about_the_row() {
    let Some(dsn) = dsn() else { return };
    let client = connect(&dsn).await;
    published(
        &client,
        "vis_svc",
        "create table vis_svc (id int primary key, owner uuid, details text);
         grant select on vis_svc to anon, authenticated, service_role;
         alter table vis_svc enable row level security;
         create policy mine on vis_svc for select using (owner = auth.uid());",
    )
    .await;
    let Some(mut tap) = tapping(&dsn).await else {
        return;
    };
    let pool = Pool::new(&dsn, 4).expect("a pool");

    client
        .batch_execute(&format!("insert into vis_svc values (1, '{BEN}', 'his')"))
        .await
        .expect("a row");
    let changes = of(&until(&mut tap, &[("vis_svc", 1)]).await, "vis_svc");
    let service = Asker {
        role: "service_role".into(),
        claims: serde_json::json!({"role": "service_role"}),
    };
    let answer = seen_by(&pool, &service, &changes[0])
        .await
        .expect("an answer");
    assert!(
        answer.row,
        "the role is created with bypassrls, so the database itself answers yes, and a check that \
         asks the database inherits that without a case for it"
    );
}

#[tokio::test]
async fn the_catalog_half_of_the_answer_is_read_once_per_table_and_role() {
    let Some(dsn) = dsn() else { return };
    let client = connect(&dsn).await;
    published(
        &client,
        "vis_cached",
        "create table vis_cached (id int primary key, details text);
         grant select on vis_cached to anon, authenticated;",
    )
    .await;
    let Some(mut tap) = tapping(&dsn).await else {
        return;
    };
    let pool = Pool::new(&dsn, 4).expect("a pool");

    client
        .batch_execute("insert into vis_cached values (1, 'a detail')")
        .await
        .expect("a row");
    let changes = of(&until(&mut tap, &[("vis_cached", 1)]).await, "vis_cached");
    let epoch = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(1));
    let mut catalog = Catalog::new(std::sync::Arc::clone(&epoch));

    let first = catalog
        .facts(&pool, &changes[0].relation, "authenticated")
        .await
        .expect("the catalog");
    assert!(!first.rls, "nothing has enabled it yet");
    catalog
        .facts(&pool, &changes[0].relation, "authenticated")
        .await
        .expect("the catalog");
    assert_eq!(
        catalog.held(),
        1,
        "the second ask is the same table and the same role, which is the ask a hundred thousand \
         subscribers make"
    );
    catalog
        .facts(&pool, &changes[0].relation, "anon")
        .await
        .expect("the catalog");
    assert_eq!(
        catalog.held(),
        2,
        "another role is another set of privileges and another answer"
    );

    client
        .batch_execute("alter table vis_cached enable row level security")
        .await
        .expect("row level security on");
    let stale = catalog
        .facts(&pool, &changes[0].relation, "authenticated")
        .await
        .expect("the catalog");
    assert!(
        !stale.rls,
        "still what it said, which is what makes this a cache rather than a query with a struct \
         around it"
    );

    epoch.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let fresh = catalog
        .facts(&pool, &changes[0].relation, "authenticated")
        .await
        .expect("the catalog");
    assert!(
        fresh.rls,
        "and the epoch the ddl moves is what makes it read again"
    );
    assert_eq!(catalog.held(), 1, "everything older than the epoch went");
}

#[tokio::test]
async fn a_table_with_no_row_level_security_is_answered_without_the_database() {
    let Some(dsn) = dsn() else { return };
    let client = connect(&dsn).await;
    published(
        &client,
        "vis_free",
        "create table vis_free (id int primary key, details text);
         grant select on vis_free to anon, authenticated;",
    )
    .await;
    let Some(mut tap) = tapping(&dsn).await else {
        return;
    };
    let pool = Pool::new(&dsn, 4).expect("a pool");

    client
        .batch_execute("insert into vis_free values (1, 'a detail')")
        .await
        .expect("a row");
    let changes = of(&until(&mut tap, &[("vis_free", 1)]).await, "vis_free");
    let mut catalog = Catalog::new(std::sync::Arc::new(std::sync::atomic::AtomicU64::new(1)));
    let facts = catalog
        .facts(&pool, &changes[0].relation, "authenticated")
        .await
        .expect("the catalog");

    // A pool pointed at a port nothing is listening on. Every question
    // asked of it fails, so a check that answers on it is a check that
    // asked nothing, which is the claim: a subscriber to a table
    // without row level security costs the database nothing per change.
    let closed = Pool::new("host=127.0.0.1 port=1 user=postgres dbname=postgres", 4)
        .expect("a pool that cannot connect");
    let answer = seen(&closed, &facts, &user(ANA), &changes[0])
        .await
        .expect("an answer without a database");
    assert!(
        answer.row,
        "nothing hides a row on a table with no policies"
    );
    assert_eq!(
        columns(&changes[0], &answer),
        vec!["id".to_string(), "details".to_string()],
        "and the columns are the ones the grant allows, which came out of the catalog once"
    );
}
