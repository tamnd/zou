//! `zou branch <target> <src> <dst> [--at <ts|lsn>]`: copy on write
//! branch of a tenant inside a store.
//!
//! With no flag the branch is taken at the source's newest checkpoint.
//! An `--at` value shaped like a Postgres LSN, `X/Y` or `0x` hex, must
//! name a checkpoint lsn exactly, branch points are checkpoint lsns in
//! this release. Plain digits are a unix second and materialize the
//! newest manifest history snapshot at or before it. Either way the
//! child only references the parent's objects, nothing is copied, so
//! the call finishes in the time of two manifest round trips.

use std::time::{SystemTime, UNIX_EPOCH};

use zou_store::{Lsn, branch, materialize_at, open_store};

pub const USAGE: &str = "usage: zou branch <target> <src> <dst> [--at <ts|lsn>]";

enum At {
    Head,
    Lsn(u64),
    Ts(u64),
}

fn parse_at(value: &str) -> Result<At, String> {
    if let Some((hi, lo)) = value.split_once('/') {
        let hi = u64::from_str_radix(hi, 16);
        let lo = u64::from_str_radix(lo, 16);
        return match (hi, lo) {
            (Ok(hi), Ok(lo)) => Ok(At::Lsn((hi << 32) | lo)),
            _ => Err(format!("bad lsn {value:?}")),
        };
    }
    if let Some(hex) = value.strip_prefix("0x") {
        return u64::from_str_radix(hex, 16)
            .map(At::Lsn)
            .map_err(|_| format!("bad lsn {value:?}"));
    }
    value
        .parse()
        .map(At::Ts)
        .map_err(|_| format!("bad timestamp {value:?}"))
}

pub fn run(argv: &[String]) -> Result<(), String> {
    let (target, src, dst, at) = match argv {
        [target, src, dst] => (target, src, dst, At::Head),
        [target, src, dst, flag, value] if flag == "--at" => (target, src, dst, parse_at(value)?),
        _ => return Err(USAGE.into()),
    };
    let store = open_store(target)?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let manifest = match at {
        At::Head => branch(&*store, src, dst, None, now),
        At::Lsn(lsn) => branch(&*store, src, dst, Some(Lsn(lsn)), now),
        At::Ts(ts) => materialize_at(&*store, src, dst, ts, now),
    }
    .map_err(|e| e.to_string())?;
    let of = manifest
        .branch_of
        .as_ref()
        .ok_or("a child names its parent")?;
    println!(
        "branched {src} into {dst} at {:#X}, {} checkpoints inherited",
        of.at_lsn.0,
        manifest.checkpoints.len()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn at_values_disambiguate_by_shape() {
        assert!(matches!(parse_at("1/2A"), Ok(At::Lsn(0x1_0000_002A))));
        assert!(matches!(parse_at("0x1F4C118"), Ok(At::Lsn(0x1F4C118))));
        assert!(matches!(parse_at("1767100000"), Ok(At::Ts(1767100000))));
        assert!(parse_at("hot").is_err());
        assert!(parse_at("1/hot").is_err());
        assert!(parse_at("0xzz").is_err());
    }
}
