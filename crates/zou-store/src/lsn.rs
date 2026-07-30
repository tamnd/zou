use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// A Postgres log sequence number.
///
/// Serialized in the familiar `X/XXXXXXXX` form so manifests stay readable
/// next to `pg_waldump` output and `pg_lsn` values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Lsn(pub u64);

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xFFFF_FFFF)
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("invalid lsn {0:?}, expected the X/XXXXXXXX form")]
pub struct ParseLsnError(String);

impl FromStr for Lsn {
    type Err = ParseLsnError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (hi, lo) = s.split_once('/').ok_or_else(|| ParseLsnError(s.into()))?;
        let hi = u64::from_str_radix(hi, 16).map_err(|_| ParseLsnError(s.into()))?;
        let lo = u64::from_str_radix(lo, 16).map_err(|_| ParseLsnError(s.into()))?;
        if hi > u64::from(u32::MAX) || lo > u64::from(u32::MAX) {
            return Err(ParseLsnError(s.into()));
        }
        Ok(Lsn(hi << 32 | lo))
    }
}

impl Serialize for Lsn {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        ser.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Lsn {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let s = String::deserialize(de)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_display_and_parse() {
        for lsn in [
            Lsn(0),
            Lsn(0x8A21_1000),
            Lsn(0x0000_0003_8B00_0000),
            Lsn(u64::MAX),
        ] {
            assert_eq!(lsn.to_string().parse::<Lsn>().unwrap(), lsn);
        }
        assert_eq!(Lsn(0x8B00_0000).to_string(), "0/8B000000");
        assert_eq!(
            "3/8B000000".parse::<Lsn>().unwrap(),
            Lsn(0x0000_0003_8B00_0000)
        );
    }

    #[test]
    fn rejects_garbage() {
        for bad in ["", "zz", "1", "1/zz", "FFFFFFFFF/0", "1/FFFFFFFFF"] {
            assert!(bad.parse::<Lsn>().is_err(), "{bad} should not parse");
        }
    }
}
