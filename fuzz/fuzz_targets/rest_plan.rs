//! Run with: cargo +nightly fuzz run rest_plan
//!
//! Any select tree the grammar accepts, with any filter pair routed
//! at it, must either plan without panicking or be refused with a
//! clean error, and a planned statement must reference every
//! parameter it collected, $1 through $n with no gaps.

#![no_main]

use libfuzzer_sys::fuzz_target;
use zou_rest::catalog::{Catalog, FkRow};
use zou_rest::filter::{Node, Parsed, parse_pair};
use zou_rest::{plan, select};

fn fk(constraint: &str, table: &str, column: &str, ref_table: &str, in_pk: bool) -> FkRow {
    FkRow {
        constraint: constraint.into(),
        table: table.into(),
        columns: vec![column.into()],
        ref_table: ref_table.into(),
        ref_columns: vec!["id".into()],
        unique: false,
        in_pk,
    }
}

fuzz_target!(|data: &str| {
    let (sel, rest) = data.split_once('\n').unwrap_or((data, ""));
    let Ok(items) = select::parse(sel) else {
        return;
    };
    let mut query = plan::Query {
        table: "users".into(),
        select: items,
        ..plan::Query::default()
    };
    if let Some((key, value)) = rest.split_once('=')
        && let Ok(parsed) = parse_pair(key, value)
    {
        match parsed {
            Parsed::Filter(c) => query.filters.push((Vec::new(), Node::Cond(c))),
            Parsed::Logic {
                embed,
                op,
                negated,
                kids,
            } => query
                .filters
                .push((embed, Node::Group { op, negated, kids })),
        }
    }
    let catalog = Catalog::new(vec![
        fk("orders_user_id_fkey", "orders", "user_id", "users", false),
        fk("items_order_id_fkey", "items", "order_id", "orders", true),
        fk("items_product_id_fkey", "items", "product_id", "products", true),
        fk("users_manager_id_fkey", "users", "manager_id", "users", false),
    ]);
    if let Ok(sql) = plan::plan(&catalog, &query) {
        // Density only, the same direction the rest_sql target
        // checks: quoted identifiers can contain $n lookalikes.
        for i in 1..=sql.params.len() {
            assert!(
                sql.text.contains(&format!("${i}")),
                "missing ${i} in {}",
                sql.text
            );
        }
    }
});
