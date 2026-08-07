//! Where the bytes of an object live.
//!
//! The row is in postgres and the bytes are not. Everything the storage
//! api answers about an object except its content comes out of
//! `storage.objects`, and the content comes from here: a directory on
//! a laptop, or a prefix on an object store, which are the same two
//! things the engine itself runs on and are opened by the same code.
//!
//! Nothing about this layout is compatibility surface. storage-api
//! keys its own bucket by tenant, bucket, name and version, and a
//! client never sees any of that, so what is used here is the pair
//! nothing else can collide with: the row's id and the version written
//! next to it. An upload writes a new version and leaves the old bytes
//! alone, which is what makes a replacement safe to read across while
//! it happens, and it is also why deleting a row has to delete the
//! bytes it named rather than a path built out of the object's name.
//!
//! The store is a blocking interface, deliberately: it is the same
//! trait the page reader uses, and a page reader that had to be async
//! would be an async postgres. So every call here goes through
//! `spawn_blocking`, which is the honest thing to do with a read that
//! may be a round trip to S3.

use std::sync::Arc;

use zou_store::{CasError, CasStore, open_store};

/// The bytes behind the object rows.
#[derive(Clone)]
pub struct Blobs {
    store: Arc<dyn CasStore>,
}

impl Blobs {
    /// Open the store a target string names: a directory, or an
    /// `s3://bucket/prefix` url. The same strings the engine takes,
    /// because on a real deployment it is the same bucket.
    pub fn open(target: &str) -> Result<Blobs, String> {
        Ok(Blobs {
            store: Arc::from(open_store(target)?),
        })
    }

    /// A store made from something already open, which is how a test
    /// puts bytes in memory and how a multi tenant server will hand
    /// out one store per tenant prefix.
    pub fn from_store(store: Arc<dyn CasStore>) -> Blobs {
        Blobs { store }
    }

    pub async fn put(&self, key: String, bytes: Vec<u8>) -> Result<(), CasError> {
        let store = self.store.clone();
        blocking(move || store.put(&key, &bytes).map(|_| ())).await
    }

    pub async fn get(&self, key: String) -> Result<Option<Vec<u8>>, CasError> {
        let store = self.store.clone();
        blocking(move || store.get(&key).map(|found| found.map(|(bytes, _)| bytes))).await
    }

    /// `len` bytes from `offset`, clamped to the end of the object, so
    /// a range that runs off the end comes back short rather than
    /// empty. `None` when there is no such object at all.
    pub async fn get_range(
        &self,
        key: String,
        offset: u64,
        len: u64,
    ) -> Result<Option<Vec<u8>>, CasError> {
        let store = self.store.clone();
        blocking(move || store.get_range(&key, offset, len)).await
    }

    /// Deleting bytes that are not there succeeds. A row whose upload
    /// failed halfway is a row with no bytes behind it, and removing it
    /// should answer the same as removing one with bytes.
    pub async fn delete(&self, key: String) -> Result<(), CasError> {
        let store = self.store.clone();
        blocking(move || store.delete(&key)).await
    }
}

/// The key an object's bytes are under.
///
/// The row's id and its version, and nothing a client sends. A name
/// with a slash in it is a name rather than a path here, so nothing has
/// to be escaped and nothing can climb out of the prefix.
pub fn key(id: &str, version: &str) -> String {
    format!("objects/{id}/{version}")
}

/// Where one request's worth of a resumable upload lives until the
/// upload finishes.
///
/// The upload's own id and the number of the request within it, so the
/// parts of one upload sort next to each other and two uploads of the
/// same name cannot collide. The id is base64url of what a client never
/// sees, so nothing a client sends reaches this key either.
pub fn part_key(id: &str, part: i32) -> String {
    format!("uploads/{id}/{part}")
}

/// Run a store call off the async threads.
///
/// A join error means the pool is going down or the call panicked, and
/// neither is something a request can do anything with, so it is
/// reported as io on a key nobody named rather than unwrapped.
async fn blocking<T, F>(work: F) -> Result<T, CasError>
where
    F: FnOnce() -> Result<T, CasError> + Send + 'static,
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(work).await {
        Ok(answer) => answer,
        Err(e) => Err(CasError::Io {
            key: String::new(),
            source: std::io::Error::other(e.to_string()),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zou_store::MemStore;

    fn blobs() -> Blobs {
        Blobs::from_store(Arc::new(MemStore::new()))
    }

    #[tokio::test]
    async fn what_goes_in_comes_out() {
        let blobs = blobs();
        let at = key("2c0d1e4f-5a6b-4c7d-8e9f-0a1b2c3d4e5f", "1");
        blobs
            .put(at.clone(), b"hello world".to_vec())
            .await
            .unwrap();
        assert_eq!(
            blobs.get(at.clone()).await.unwrap().as_deref(),
            Some(&b"hello world"[..])
        );
        assert_eq!(
            blobs.get_range(at.clone(), 0, 4).await.unwrap().as_deref(),
            Some(&b"hell"[..])
        );
        // Past the end is short rather than an error, which is what a
        // range header asking for more than there is has to answer.
        assert_eq!(
            blobs
                .get_range(at.clone(), 6, 100)
                .await
                .unwrap()
                .as_deref(),
            Some(&b"world"[..])
        );
        blobs.delete(at.clone()).await.unwrap();
        assert!(blobs.get(at).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn a_new_version_leaves_the_old_bytes_where_they_were() {
        let blobs = blobs();
        let id = "2c0d1e4f-5a6b-4c7d-8e9f-0a1b2c3d4e5f";
        blobs.put(key(id, "1"), b"first".to_vec()).await.unwrap();
        blobs.put(key(id, "2"), b"second".to_vec()).await.unwrap();
        assert_eq!(
            blobs.get(key(id, "1")).await.unwrap().as_deref(),
            Some(&b"first"[..]),
            "a reader that started before the replacement still has something to read"
        );
    }

    #[tokio::test]
    async fn nothing_a_client_sends_reaches_the_key() {
        // The name is not in it at all, so a name that is a path, or
        // one with a .. in it, is a name rather than somewhere else on
        // the store.
        assert_eq!(key("an-id", "a-version"), "objects/an-id/a-version");
    }

    #[tokio::test]
    async fn deleting_what_was_never_there_is_fine() {
        blobs().delete(key("nobody", "nothing")).await.unwrap();
    }
}
