//! The manifest, the root of truth for one logical database.
//!
//! A manifest names the Postgres version, the current checkpoint set, the
//! fold watermark, the writer lease, and the branch provenance. It is small,
//! human readable JSON on purpose: debugging a production database should
//! start with `curl` and eyes, not a hex dump.

use serde::{Deserialize, Serialize};

use crate::lsn::Lsn;

/// Current manifest format. Readers refuse anything newer with a clear
/// error instead of misreading it, and transparently accept anything older.
/// Format 2 dropped the per tenant WAL tail in favor of the shared log.
/// Format 3 added the page shard count and the split lineage; a tenant
/// writes it on its first split, so a binary that predates sharding
/// refuses the whole tenant instead of quietly serving shard zero only.
pub const MANIFEST_FORMAT: u32 = 3;

#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    #[error(
        "manifest format {found} is newer than this binary supports ({MANIFEST_FORMAT}), upgrade zou"
    )]
    FormatTooNew { found: u32 },
    #[error(
        "this store still carries a v1 per tenant WAL tail, fold it down with the previous zou binary before upgrading"
    )]
    V1WalTail,
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
    /// Pg lsn the checkpoint chain covers through, the redo of the last
    /// fold. Frames of the shared log at or below this never need replay.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub folded_upto: Option<Lsn>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch_of: Option<BranchOf>,
    /// Page shard count, a power of two (spec 04 section 1). One until
    /// the first split, and off the wire then, so a tenant that never
    /// split keeps its manifest byte identical to before sharding.
    #[serde(default = "one_shard", skip_serializing_if = "is_one_shard")]
    pub shards: u32,
    /// Split lineage, oldest first, a chain: each entry's `from` is the
    /// previous entry's `to`. A shard the newest change created serves
    /// its ancestors' layers until compaction physically separates
    /// them, and this list is how a reader knows which eras to consult
    /// and where a fresh shard's ingest resumes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub shard_history: Vec<ShardChange>,
    /// Unix seconds of the last state changing publish. Rides into the
    /// manifest history copy, which is what PITR picks a snapshot by.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published_unix: Option<u64>,
}

fn one_shard() -> u32 {
    1
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_one_shard(n: &u32) -> bool {
    *n == 1
}

/// One shard count change, the record a split or merge leaves behind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardChange {
    pub from: u32,
    pub to: u32,
    /// The safe resume floor for shards this change created: every
    /// record at or below it, for any key, sits in a published layer
    /// of the pre change era. A shard with no manifest of its own yet
    /// starts ingesting here.
    pub at: Lsn,
    /// When the change was published, for eyes and audit only.
    pub unix: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    /// Opaque id of the node holding the write lease.
    pub holder: String,
    /// Unix seconds after which the lease may be stolen.
    pub expires_unix: u64,
    /// Fence token, stamped into every WAL frame the holder writes.
    pub fence: u64,
    /// Where another node can reach the holder, scheme and authority,
    /// `http://10.0.0.4:8000`. This is how a node that is not the writer
    /// finds the one that is: the manifest is already the only thing
    /// every node reads, so holder discovery costs nothing new.
    ///
    /// Absent when the holder is not reachable, which is an embedded
    /// build, a one shot command and any single node deployment, and
    /// absent is what every manifest written before this field says.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
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
pub struct BranchOf {
    #[serde(rename = "ref")]
    pub tenant_ref: String,
    pub at_lsn: Lsn,
}

impl Manifest {
    /// A fresh manifest for a brand new database.
    pub fn new(tenant_ref: &str, pg_version: u32) -> Self {
        Self {
            // Base format on purpose, not the ceiling: a tenant that
            // never splits keeps writing format 2, only the first
            // split writes the format 3 that carries sharding.
            format: 2,
            tenant_ref: tenant_ref.to_string(),
            epoch: 0,
            lease: None,
            pg: PgInfo {
                version: pg_version,
                timeline: 1,
            },
            checkpoints: Vec::new(),
            folded_upto: None,
            branch_of: None,
            shards: 1,
            shard_history: Vec::new(),
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
        // fails with "upgrade zou" instead of a random missing-field error,
        // and a v1 store with unfolded WAL fails with a migration message
        // instead of silently losing its tail.
        #[derive(Deserialize)]
        struct Peek {
            format: u32,
            #[serde(default)]
            wal_tail: Option<serde_json::Value>,
            #[serde(default)]
            parent_tail: Vec<serde_json::Value>,
        }
        let p: Peek = serde_json::from_slice(data)?;
        if p.format > MANIFEST_FORMAT {
            return Err(ManifestError::FormatTooNew { found: p.format });
        }
        if p.wal_tail.as_ref().is_some_and(|v| !v.is_null()) || !p.parent_tail.is_empty() {
            return Err(ManifestError::V1WalTail);
        }
        Ok(serde_json::from_slice(data)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Manifest {
        Manifest {
            format: 2,
            tenant_ref: "acme-prod".into(),
            epoch: 42,
            lease: Some(Lease {
                holder: "node-7f3a".into(),
                expires_unix: 1_767_100_000,
                fence: 1042,
                endpoint: None,
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
            folded_upto: Some("0/8B000000".parse().unwrap()),
            branch_of: None,
            shards: 1,
            shard_history: Vec::new(),
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
        let golden = include_str!("../testdata/manifest_v2.json");
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
        m.published_unix = Some(1_767_100_123);
        assert_eq!(Manifest::from_json(&m.to_json()).unwrap(), m);
        // The plain sample serializes without any of the new keys, which
        // is what keeps the golden file byte for byte stable.
        let text = String::from_utf8(sample().to_json()).unwrap();
        for key in ["owner", "published_unix"] {
            assert!(!text.contains(key), "{key} leaked into a plain manifest");
        }
    }

    #[test]
    fn shard_fields_round_trip_and_stay_off_the_wire_when_unsplit() {
        let text = String::from_utf8(sample().to_json()).unwrap();
        for key in ["shards", "shard_history"] {
            assert!(!text.contains(key), "{key} leaked into an unsplit manifest");
        }

        let mut m = sample();
        m.format = 3;
        m.shards = 4;
        m.shard_history = vec![
            ShardChange {
                from: 1,
                to: 2,
                at: Lsn(0x100),
                unix: 1_767_100_200,
            },
            ShardChange {
                from: 2,
                to: 4,
                at: Lsn(0x900),
                unix: 1_767_100_300,
            },
        ];
        assert_eq!(Manifest::from_json(&m.to_json()).unwrap(), m);
    }

    #[test]
    fn refuses_a_v1_manifest_with_an_unfolded_wal_tail() {
        let json = r#"{
            "format": 1,
            "ref": "t1",
            "epoch": 3,
            "pg": {"version": 18, "timeline": 1},
            "wal_tail": {"epoch_dir": 3, "from_lsn": "0/0", "segments": []}
        }"#;
        let err = Manifest::from_json(json.as_bytes()).unwrap_err();
        assert!(matches!(err, ManifestError::V1WalTail));
        assert!(err.to_string().contains("previous zou binary"));

        let json = r#"{
            "format": 1,
            "ref": "t1",
            "epoch": 3,
            "pg": {"version": 18, "timeline": 1},
            "parent_tail": [{"ref": "p", "from_lsn": "0/0", "segments": []}]
        }"#;
        let err = Manifest::from_json(json.as_bytes()).unwrap_err();
        assert!(matches!(err, ManifestError::V1WalTail));
    }

    #[test]
    fn accepts_a_folded_v1_manifest() {
        let json = r#"{"format":1,"ref":"t1","epoch":0,"pg":{"version":18,"timeline":1}}"#;
        let m = Manifest::from_json(json.as_bytes()).unwrap();
        assert_eq!(m.tenant_ref, "t1");
        assert_eq!(m.format, 1);
    }

    #[test]
    fn optional_sections_may_be_absent() {
        let json = format!(
            r#"{{"format":{MANIFEST_FORMAT},"ref":"t1","epoch":0,"pg":{{"version":18,"timeline":1}}}}"#
        );
        let m = Manifest::from_json(json.as_bytes()).unwrap();
        assert_eq!(
            m,
            Manifest {
                format: MANIFEST_FORMAT,
                ..Manifest::new("t1", 18)
            }
        );
    }
}
