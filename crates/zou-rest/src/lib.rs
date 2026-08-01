//! The PostgREST compatible REST layer.
//!
//! Everything under /rest/v1 reduces to one job: turn a query string
//! written in PostgREST's grammar into parameterized SQL that runs
//! under the caller's RLS context. This crate grows module by module
//! along the zou-rest checklist in milestone 2, starting from the
//! bottom: the filter grammar, then select parsing, then the SQL
//! compiler and the embedding planner on top.

pub mod catalog;
pub mod filter;
pub mod order;
pub mod page;
pub mod plan;
mod scan;
pub mod select;
pub mod sql;
