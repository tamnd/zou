//! SQLite backend, spec 07-sqlite-store.md, behind the `sqlite` feature.
//!
//! The whole store is one SQLite database. Where the .zou backend bets
//! on a purpose built format, this one bets on the most deployed
//! storage engine there is: its WAL, its crash safety, and its tooling,
//! `sqlite3 store.db` inspects a live store and `.backup` takes a
//! consistent hot copy. The CAS contract maps straight onto SQL, a
//! conditional PUT is an UPDATE guarded by the expected version with
//! the row count as the verdict.
//!
//! Versions are per key integers starting at 1, bumped on every write,
//! carried as decimal strings through the opaque Version type. Values
//! of 512 bytes and up are stored lz4 compressed when that saves at
//! least an eighth, the same rule as the .zou backend so the two stay
//! comparable, recorded in the encoding column.

use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use rusqlite::{Connection, OptionalExtension, params};

use crate::cas::{CasError, CasStore, Version};

const FORMAT: i64 = 1;

const ENC_RAW: i64 = 0;
const ENC_LZ4: i64 = 1;

/// Values below this many bytes are never worth a compression attempt.
const COMPRESS_MIN: usize = 512;

pub struct SqliteStore {
    conn: Mutex<Connection>,
}

fn io_err(key: &str, e: rusqlite::Error) -> CasError {
    CasError::Io {
        key: key.to_string(),
        source: std::io::Error::other(e.to_string()),
    }
}

/// Compress when it saves at least an eighth, otherwise store raw.
fn encode_value(data: &[u8]) -> (i64, Vec<u8>) {
    if data.len() >= COMPRESS_MIN {
        let packed = lz4_flex::compress(data);
        if packed.len() <= data.len() - data.len() / 8 {
            return (ENC_LZ4, packed);
        }
    }
    (ENC_RAW, data.to_vec())
}

fn decode_value(key: &str, enc: i64, raw_len: i64, stored: Vec<u8>) -> Result<Vec<u8>, CasError> {
    if enc == ENC_LZ4 {
        lz4_flex::decompress(&stored, raw_len as usize).map_err(|e| CasError::Io {
            key: key.to_string(),
            source: std::io::Error::other(format!("lz4: {e}")),
        })
    } else {
        Ok(stored)
    }
}

impl SqliteStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, String> {
        let path = path.into();
        let context =
            |what: &str, e: &dyn std::fmt::Display| format!("{}: {what}: {e}", path.display());
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(|e| context("create dir", &e))?;
        }
        let conn = Connection::open(&path).map_err(|e| context("open", &e))?;
        conn.busy_timeout(Duration::from_secs(5))
            .map_err(|e| context("busy_timeout", &e))?;
        // journal_mode answers with a row, so it goes through query_row.
        let _: String = conn
            .query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))
            .map_err(|e| context("wal mode", &e))?;
        // FULL is the durability bar every backend meets, an acked put
        // survives power loss. normal is a documented benchmark knob.
        let sync = match crate::setting::word("ZOU_SQLITE_SYNC", &["full", "normal"]) {
            Some("normal") => "NORMAL",
            _ => "FULL",
        };
        conn.pragma_update(None, "synchronous", sync)
            .map_err(|e| context("synchronous", &e))?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS objects(
                 key TEXT PRIMARY KEY,
                 version INTEGER NOT NULL,
                 encoding INTEGER NOT NULL,
                 raw_len INTEGER NOT NULL,
                 data BLOB NOT NULL
             ) WITHOUT ROWID;
             INSERT INTO meta(k, v) VALUES('format', '1')
                 ON CONFLICT(k) DO NOTHING;",
        )
        .map_err(|e| context("schema", &e))?;
        let format: i64 = conn
            .query_row("SELECT v FROM meta WHERE k='format'", [], |r| {
                r.get::<_, String>(0)
            })
            .map_err(|e| context("format", &e))?
            .parse()
            .map_err(|e| context("format", &e))?;
        if format > FORMAT {
            return Err(context(
                "format",
                &format!("{format} is newer than this build"),
            ));
        }
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }
}

impl CasStore for SqliteStore {
    fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, CasError> {
        let conn = self.conn.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT version, encoding, raw_len, data FROM objects WHERE key = ?1",
                params![key],
                |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get::<_, Vec<u8>>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(|e| io_err(key, e))?;
        match row {
            Some((version, enc, raw_len, stored)) => {
                let data = decode_value(key, enc, raw_len, stored)?;
                Ok(Some((data, Version::from_backend(version.to_string()))))
            }
            None => Ok(None),
        }
    }

    fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Option<Vec<u8>>, CasError> {
        let conn = self.conn.lock().unwrap();
        // Raw values slice inside SQLite so only the range crosses the
        // boundary, compressed ones come out whole and inflate first.
        let start = i64::try_from(offset.saturating_add(1)).unwrap_or(i64::MAX);
        let count = i64::try_from(len).unwrap_or(i64::MAX);
        let row = conn
            .query_row(
                "SELECT encoding, raw_len,
                        CASE WHEN encoding = 0 THEN substr(data, ?2, ?3) ELSE data END
                 FROM objects WHERE key = ?1",
                params![key, start, count],
                |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, Vec<u8>>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(|e| io_err(key, e))?;
        match row {
            Some((enc, _, slice)) if enc == ENC_RAW => Ok(Some(slice)),
            Some((enc, raw_len, stored)) => {
                let data = decode_value(key, enc, raw_len, stored)?;
                let from = (offset as usize).min(data.len());
                let to = (offset.saturating_add(len) as usize).min(data.len());
                Ok(Some(data[from..to].to_vec()))
            }
            None => Ok(None),
        }
    }

    fn put_if_match(
        &self,
        key: &str,
        data: &[u8],
        expected: Option<&Version>,
    ) -> Result<Version, CasError> {
        let (enc, stored) = encode_value(data);
        let raw_len = data.len() as i64;
        let conn = self.conn.lock().unwrap();
        let conflict = || CasError::Conflict {
            key: key.to_string(),
        };
        let version = match expected {
            None => conn
                .query_row(
                    "INSERT INTO objects(key, version, encoding, raw_len, data)
                     VALUES(?1, 1, ?2, ?3, ?4)
                     ON CONFLICT(key) DO NOTHING
                     RETURNING version",
                    params![key, enc, raw_len, stored],
                    |r| r.get::<_, i64>(0),
                )
                .optional()
                .map_err(|e| io_err(key, e))?
                .ok_or_else(conflict)?,
            Some(v) => {
                // A token this backend never issued cannot match.
                let want: i64 = v.as_str().parse().map_err(|_| conflict())?;
                conn.query_row(
                    "UPDATE objects
                     SET version = version + 1, encoding = ?3, raw_len = ?4, data = ?5
                     WHERE key = ?1 AND version = ?2
                     RETURNING version",
                    params![key, want, enc, raw_len, stored],
                    |r| r.get::<_, i64>(0),
                )
                .optional()
                .map_err(|e| io_err(key, e))?
                .ok_or_else(conflict)?
            }
        };
        Ok(Version::from_backend(version.to_string()))
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<Version, CasError> {
        let (enc, stored) = encode_value(data);
        let raw_len = data.len() as i64;
        let conn = self.conn.lock().unwrap();
        let version = conn
            .query_row(
                "INSERT INTO objects(key, version, encoding, raw_len, data)
                 VALUES(?1, 1, ?2, ?3, ?4)
                 ON CONFLICT(key) DO UPDATE SET
                     version = objects.version + 1,
                     encoding = excluded.encoding,
                     raw_len = excluded.raw_len,
                     data = excluded.data
                 RETURNING version",
                params![key, enc, raw_len, stored],
                |r| r.get::<_, i64>(0),
            )
            .map_err(|e| io_err(key, e))?;
        Ok(Version::from_backend(version.to_string()))
    }

    fn delete(&self, key: &str) -> Result<(), CasError> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM objects WHERE key = ?1", params![key])
            .map_err(|e| io_err(key, e))?;
        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, CasError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare_cached("SELECT key FROM objects WHERE key >= ?1 ORDER BY key")
            .map_err(|e| io_err(prefix, e))?;
        let rows = stmt
            .query_map(params![prefix], |r| r.get::<_, String>(0))
            .map_err(|e| io_err(prefix, e))?;
        let mut keys = Vec::new();
        for row in rows {
            let key = row.map_err(|e| io_err(prefix, e))?;
            if !key.starts_with(prefix) {
                break;
            }
            keys.push(key);
        }
        Ok(keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_and_versions_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.db");
        let v1;
        {
            let store = SqliteStore::open(&path).unwrap();
            v1 = store.put_if_match("a/k", b"hello", None).unwrap();
            store.put("a/mut", b"first").unwrap();
            store.put("a/mut", b"second").unwrap();
            store.put("gone", b"bye").unwrap();
            store.delete("gone").unwrap();
        }
        let store = SqliteStore::open(&path).unwrap();
        let (data, version) = store.get("a/k").unwrap().unwrap();
        assert_eq!(data, b"hello");
        assert_eq!(version, v1);
        assert_eq!(store.get("a/mut").unwrap().unwrap().0, b"second");
        assert!(store.get("gone").unwrap().is_none());
        assert_eq!(store.list("a/").unwrap(), vec!["a/k", "a/mut"]);
    }

    #[test]
    fn compressible_values_shrink_and_range_reads_slice() {
        let dir = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(dir.path().join("t.db")).unwrap();
        let value = b"zou ".repeat(64 * 1024);
        store.put("big", &value).unwrap();
        assert_eq!(store.get("big").unwrap().unwrap().0, value);
        assert_eq!(
            store.get_range("big", 4, 4).unwrap().unwrap(),
            b"zou ".to_vec()
        );
        let raw = b"x".repeat(4096);
        store.put("raw", &raw).unwrap();
        assert_eq!(store.get_range("raw", 4090, 100).unwrap().unwrap().len(), 6);
        let stored: i64 = {
            let conn = store.conn.lock().unwrap();
            conn.query_row(
                "SELECT length(data) FROM objects WHERE key = 'big'",
                [],
                |r| r.get(0),
            )
            .unwrap()
        };
        assert!(stored < value.len() as i64 / 2, "{stored}");
    }

    #[test]
    fn a_foreign_version_token_conflicts_instead_of_erroring() {
        let dir = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(dir.path().join("t.db")).unwrap();
        store.put("k", b"v").unwrap();
        let alien = Version::from_backend("\"an-etag-from-s3\"");
        assert!(matches!(
            store.put_if_match("k", b"w", Some(&alien)),
            Err(CasError::Conflict { .. })
        ));
    }

    #[test]
    fn a_newer_format_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.db");
        {
            let store = SqliteStore::open(&path).unwrap();
            let conn = store.conn.lock().unwrap();
            conn.execute("UPDATE meta SET v='99' WHERE k='format'", [])
                .unwrap();
        }
        let err = SqliteStore::open(&path).map(|_| ()).unwrap_err();
        assert!(err.contains("newer"), "{err}");
    }
}
