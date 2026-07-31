//! The manifest, the root of truth for one logical database.
//!
//! A manifest names the Postgres version, the current checkpoint set, the
//! WAL tail, the writer lease, and the branch provenance. It is small,
//! human readable JSON on purpose: debugging a production database should
//! start with `curl` and eyes, not a hex dump.

use serde::{Deserialize, Serialize};

use crate::lsn::Lsn;

/// Current manifest format. Readers refuse anything newer with a clear
/// error instead of misreading it, and transparently accept anything older.
pub const MANIFEST_FORMAT: u32 = 1;

#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    #[error(
        "manifest format {found} is newer than this binary supports ({MANIFEST_FORMAT}), upgrade zou"
    )]
    FormatTooNew { found: u32 },
    #[error("invalid manifest json: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub format: u32,
    /// Tenant reference, unique within the store.
    #[serde(rename = "ref")]
    pub tenant_ref: String,
    /// Incremented on every lease acquisition. WAL directories are keyed by
    /// epoch, so a fenced-out writer can never corrupt live history.
    pub epoch: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease: Option<Lease>,
    pub pg: PgInfo,
    #[serde(default)]
    pub checkpoints: Vec<CheckpointRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wal_tail: Option<WalTail>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch_of: Option<BranchOf>,
    /// WAL inherited from ancestors at branch time, replayed before the
    /// own tail. Static: the referenced segments are sealed and the list
    /// never grows, it only goes away wholesale once a fold learns to
    /// repack the window it covers.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parent_tail: Vec<ParentTail>,
    /// Unix seconds of the last state changing publish. Rides into the
    /// manifest history copy, which is what PITR picks a snapshot by.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published_unix: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    /// Opaque id of the node holding the write lease.
    pub holder: String,
    /// Unix seconds after which the lease may be stolen.
    pub expires_unix: u64,
    /// Fence token, stamped into every WAL frame the holder writes.
    pub fence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgInfo {
    pub version: u32,
    pub timeline: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointKind {
    /// A complete image of every page at its LSN.
    Full,
    /// Only the pages dirtied since the previous checkpoint.
    Delta,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointRef {
    pub id: String,
    pub lsn: Lsn,
    pub kind: CheckpointKind,
    /// Tenant whose prefix holds the objects, absent for our own. A
    /// branch copies its parent's refs and tags them, so a chain can
    /// reference captures across any number of ancestors without
    /// copying a byte.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalTail {
    /// Epoch of the session that last published this tail. Segments carry
    /// their own epoch in their name, so the list can span sessions.
    pub epoch_dir: u64,
    /// Replay starts here, at or after the newest checkpoint LSN.
    pub from_lsn: Lsn,
    /// Sealed segment paths relative to wal/, oldest first, each of the
    /// form `<epoch>/<start-lsn>.wal`. The list chains across writer
    /// sessions until a checkpoint folds it down.
    #[serde(default)]
    pub segments: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchOf {
    #[serde(rename = "ref")]
    pub tenant_ref: String,
    pub at_lsn: Lsn,
}

/// One ancestor's WAL tail as inherited at branch time. Segment names
/// are the usual `<epoch>/<start-lsn>.wal`, resolved under the named
/// tenant's prefix, not ours.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParentTail {
    #[serde(rename = "ref")]
    pub tenant_ref: String,
    pub from_lsn: Lsn,
    #[serde(default)]
    pub segments: Vec<String>,
}

impl Manifest {
    /// A fresh manifest for a brand new database.
    pub fn new(tenant_ref: &str, pg_version: u32) -> Self {
        Self {
            format: MANIFEST_FORMAT,
            tenant_ref: tenant_ref.to_string(),
            epoch: 0,
            lease: None,
            pg: PgInfo {
                version: pg_version,
                timeline: 1,
            },
            checkpoints: Vec::new(),
            wal_tail: None,
            branch_of: None,
            parent_tail: Vec::new(),
            published_unix: None,
        }
    }

    pub fn to_json(&self) -> Vec<u8> {
        let mut out = serde_json::to_vec_pretty(self).expect("manifest serializes");
        out.push(b'\n');
        out
    }

    pub fn from_json(data: &[u8]) -> Result<Self, ManifestError> {
        // Peek at the format before full deserialization so a newer manifest
        // fails with "upgrade zou" instead of a random missing-field error.
        #[derive(Deserialize)]
        struct FormatOnly {
            format: u32,
        }
        let f: FormatOnly = serde_json::from_slice(data)?;
        if f.format > MANIFEST_FORMAT {
            return Err(ManifestError::FormatTooNew { found: f.format });
        }
        Ok(serde_json::from_slice(data)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Manifest {
        Manifest {
            format: MANIFEST_FORMAT,
            tenant_ref: "acme-prod".into(),
            epoch: 42,
            lease: Some(Lease {
                holder: "node-7f3a".into(),
                expires_unix: 1_767_100_000,
                fence: 1042,
            }),
            pg: PgInfo {
                version: 18,
                timeline: 3,
            },
            checkpoints: vec![
                CheckpointRef {
                    id: "chk-000121".into(),
                    lsn: "0/8A211000".parse().unwrap(),
                    kind: CheckpointKind::Full,
                    owner: None,
                },
                CheckpointRef {
                    id: "chk-000122".into(),
                    lsn: "0/8B000000".parse().unwrap(),
                    kind: CheckpointKind::Delta,
                    owner: None,
                },
            ],
            wal_tail: Some(WalTail {
                epoch_dir: 42,
                from_lsn: "0/8B000000".parse().unwrap(),
                segments: vec!["000000008B000000.wal".into()],
            }),
            branch_of: None,
            parent_tail: Vec::new(),
            published_unix: None,
        }
    }

    #[test]
    fn round_trips() {
        let m = sample();
        assert_eq!(Manifest::from_json(&m.to_json()).unwrap(), m);
    }

    #[test]
    fn matches_the_golden_file() {
        let golden = include_str!("../testdata/manifest_v1.json");
        let m = Manifest::from_json(golden.as_bytes()).unwrap();
        assert_eq!(m, sample());
        // And what we write is exactly what the golden file holds, so any
        // accidental format change shows up as a readable diff.
        assert_eq!(String::from_utf8(sample().to_json()).unwrap(), golden);
    }

    #[test]
    fn refuses_newer_formats() {
        let mut m = sample();
        m.format = MANIFEST_FORMAT + 1;
        let err = Manifest::from_json(&m.to_json()).unwrap_err();
        assert!(
            matches!(err, ManifestError::FormatTooNew { found } if found == MANIFEST_FORMAT + 1)
        );
        assert!(err.to_string().contains("upgrade zou"));
    }

    #[test]
    fn branch_fields_round_trip_and_stay_off_the_wire_when_absent() {
        let mut m = sample();
        m.branch_of = Some(BranchOf {
            tenant_ref: "acme-prod".into(),
            at_lsn: "0/8B000000".parse().unwrap(),
        });
        m.checkpoints[0].owner = Some("acme-prod".into());
        m.parent_tail.push(ParentTail {
            tenant_ref: "acme-prod".into(),
            from_lsn: "0/8B000000".parse().unwrap(),
            segments: vec!["0000000000000042/000000008B000000.wal".into()],
        });
        m.published_unix = Some(1_767_100_123);
        assert_eq!(Manifest::from_json(&m.to_json()).unwrap(), m);
        // The plain sample serializes without any of the new keys, which
        // is what keeps the golden file byte for byte stable.
        let text = String::from_utf8(sample().to_json()).unwrap();
        for key in ["owner", "parent_tail", "published_unix"] {
            assert!(!text.contains(key), "{key} leaked into a plain manifest");
        }
    }

    #[test]
    fn optional_sections_may_be_absent() {
        let json = format!(
            r#"{{"format":{MANIFEST_FORMAT},"ref":"t1","epoch":0,"pg":{{"version":18,"timeline":1}}}}"#
        );
        let m = Manifest::from_json(json.as_bytes()).unwrap();
        assert_eq!(m, Manifest::new("t1", 18));
    }
}
