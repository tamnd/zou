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
//! All of it sits under the tenant, at `tenants/<ref>/files/`, next to
//! the pages and the layers of the same tenant's database. One prefix
//! per tenant is what makes a shared bucket safe: a key built here can
//! only ever name bytes belonging to the tenant the server was started
//! for, whatever a handler above it got wrong, and dropping a tenant is
//! dropping a prefix rather than a query.
//!
//! A branch inherits its parent's bytes by reading through. The rows
//! come across because the rows are in the database and the database
//! was branched, so they name ids and versions the child's own prefix
//! has never held, and a read that misses at home tries the parent, and
//! its parent, in order. Writes never do that: an upload into a branch
//! lands in the branch, and a delete in a branch removes the branch's
//! own bytes and nothing else, because the parent is a live database
//! somebody else is using and a child cannot be allowed to empty it.
//! The cost of that is bytes a branch has stopped referencing but
//! cannot remove, which is the same cost branching already pays for
//! pages.
//!
//! The store is a blocking interface, deliberately: it is the same
//! trait the page reader uses, and a page reader that had to be async
//! would be an async postgres. So every call here goes through
//! `spawn_blocking`, which is the honest thing to do with a read that
//! may be a round trip to S3.

use std::sync::Arc;

use zou_store::{CasError, CasStore, layout::TenantLayout, lineage, open_store};

/// The tenant a server serves when nothing named one. The same ref the
/// engine defaults to, because a single tenant deployment is one
/// database and its files rather than two things that happen to share a
/// bucket.
pub const LOCAL: &str = "local";

/// The bytes behind the object rows.
#[derive(Clone)]
pub struct Blobs {
    store: Arc<dyn CasStore>,
    /// Where this tenant's own bytes go. Every put and every delete is
    /// under it, and it is the first place a read looks.
    home: String,
    /// The prefixes a read falls back to when home has nothing, parent
    /// first. Empty for a tenant that is not a branch, which is every
    /// tenant until somebody branches one.
    inherited: Vec<String>,
}

impl Blobs {
    /// Open the store a target string names, for one tenant: a
    /// directory, or an `s3://bucket/prefix` url. The same strings the
    /// engine takes, because on a real deployment it is the same
    /// bucket.
    ///
    /// The branch chain is read here, once, rather than per request.
    /// It is a property of the tenant and a tenant does not become a
    /// branch of something else while it is being served: branching
    /// writes the child, and the child is what a later server is
    /// started for.
    pub fn open(target: &str, tenant_ref: &str) -> Result<Blobs, String> {
        let store: Arc<dyn CasStore> = Arc::from(open_store(target)?);
        let chain = lineage(store.as_ref(), tenant_ref).map_err(|e| e.to_string())?;
        Ok(Blobs::over(store, &chain))
    }

    /// A store made from something already open, for a tenant with no
    /// parents. This is how a test puts bytes in memory.
    pub fn from_store(store: Arc<dyn CasStore>) -> Blobs {
        Blobs::over(store, &[LOCAL.to_string()])
    }

    /// The chain as prefixes: the first is home and the rest are read
    /// through, in the order they were given.
    fn over(store: Arc<dyn CasStore>, chain: &[String]) -> Blobs {
        let mut prefixes = chain.iter().map(|r| TenantLayout::new(r).files_prefix());
        Blobs {
            home: prefixes.next().expect("a chain is never empty"),
            inherited: prefixes.collect(),
            store,
        }
    }

    pub async fn put(&self, key: String, bytes: Vec<u8>) -> Result<(), CasError> {
        let store = self.store.clone();
        let key = format!("{}{key}", self.home);
        blocking(move || store.put(&key, &bytes).map(|_| ())).await
    }

    pub async fn get(&self, key: String) -> Result<Option<Vec<u8>>, CasError> {
        for at in self.everywhere(&key) {
            let store = self.store.clone();
            let found = blocking(move || store.get(&at)).await?;
            if let Some((bytes, _)) = found {
                return Ok(Some(bytes));
            }
        }
        Ok(None)
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
        for at in self.everywhere(&key) {
            let store = self.store.clone();
            let found = blocking(move || store.get_range(&at, offset, len)).await?;
            if found.is_some() {
                return Ok(found);
            }
        }
        Ok(None)
    }

    /// Deleting bytes that are not there succeeds. A row whose upload
    /// failed halfway is a row with no bytes behind it, and removing it
    /// should answer the same as removing one with bytes.
    ///
    /// Only home is touched. A branch that deletes an object it
    /// inherited is deleting the row, which is its own, and leaving the
    /// bytes, which are not.
    pub async fn delete(&self, key: String) -> Result<(), CasError> {
        let store = self.store.clone();
        let key = format!("{}{key}", self.home);
        blocking(move || store.delete(&key)).await
    }

    /// Every place a read looks, home first.
    fn everywhere(&self, key: &str) -> Vec<String> {
        std::iter::once(&self.home)
            .chain(&self.inherited)
            .map(|prefix| format!("{prefix}{key}"))
            .collect()
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

/// Where a rendered copy of an object is kept.
///
/// Its own prefix rather than a path under the object, because the
/// object's key names bytes and bytes have no room underneath them: on
/// a directory store the parent is a file, and everything asked for
/// below it comes back `Not a directory`. The id and version are the
/// same pair the source is under, so an object that was replaced never
/// reads a transform of what it replaced, and the recipe is what the
/// caller asked for boiled down to a hash by the renderer.
pub fn render_key(id: &str, version: &str, recipe: &str, format: &str) -> String {
    format!("renders/{id}/{version}.{recipe}.{format}")
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

/// Where one piece of a multipart upload lives until the pieces are put
/// together.
///
/// The same prefix the resumable parts use, since both are bytes an
/// upload is holding and neither is an object yet. The upload ids
/// cannot collide: one is a uuid and the other is base64url of what a
/// client never sees.
///
/// Named by the row rather than by the number on it, because a client
/// that sends the same number twice has sent two pieces and the second
/// does not replace the first. Which of them the object is made of is
/// decided later, by the etag the completion names, and both have to
/// still be there for that to be a decision. Recorded.
pub fn piece_key(upload: &str, part: &str) -> String {
    format!("uploads/{upload}/{part}")
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

    #[tokio::test]
    async fn the_bytes_land_under_the_tenant() {
        let store = Arc::new(MemStore::new());
        let blobs = Blobs::over(store.clone(), &["acme-prod".to_string()]);
        blobs.put(key("an-id", "1"), b"x".to_vec()).await.unwrap();
        assert_eq!(
            store.list("").unwrap(),
            vec!["tenants/acme-prod/files/objects/an-id/1"],
            "one prefix per tenant is the whole reason a shared bucket is safe"
        );
    }

    #[tokio::test]
    async fn a_branch_reads_what_it_inherited_and_writes_its_own() {
        let store = Arc::new(MemStore::new());
        let parent = Blobs::over(store.clone(), &["prod".to_string()]);
        let child = Blobs::over(store.clone(), &["pr-14".to_string(), "prod".to_string()]);
        let inherited = key("an-id", "1");
        parent
            .put(inherited.clone(), b"from prod".to_vec())
            .await
            .unwrap();

        // The row came across with the database, so the branch asks for
        // a key its own prefix has never held.
        assert_eq!(
            child.get(inherited.clone()).await.unwrap().as_deref(),
            Some(&b"from prod"[..])
        );
        assert_eq!(
            child.get_range(inherited.clone(), 5, 4).await.unwrap(),
            Some(b"prod".to_vec()),
            "a range reads through too, and stops at the first prefix that has it"
        );

        let own = key("another-id", "1");
        child
            .put(own.clone(), b"from the branch".to_vec())
            .await
            .unwrap();
        assert!(
            parent.get(own).await.unwrap().is_none(),
            "an upload into a branch is not an upload into what it branched from"
        );
    }

    #[tokio::test]
    async fn a_branch_cannot_delete_what_it_inherited() {
        let store = Arc::new(MemStore::new());
        let parent = Blobs::over(store.clone(), &["prod".to_string()]);
        let child = Blobs::over(store.clone(), &["pr-14".to_string(), "prod".to_string()]);
        let at = key("an-id", "1");
        parent.put(at.clone(), b"from prod".to_vec()).await.unwrap();

        // Removing the object in the branch removes the row, which is
        // the branch's, and asks for the bytes, which are not.
        child.delete(at.clone()).await.unwrap();
        assert_eq!(
            parent.get(at).await.unwrap().as_deref(),
            Some(&b"from prod"[..]),
            "the parent is a live database somebody else is using"
        );
    }

    #[tokio::test]
    async fn a_branch_that_replaced_an_object_reads_its_own_copy() {
        let store = Arc::new(MemStore::new());
        let parent = Blobs::over(store.clone(), &["prod".to_string()]);
        let child = Blobs::over(store.clone(), &["pr-14".to_string(), "prod".to_string()]);
        // Upsert keeps the row and writes a new version, so this is the
        // one case where the same key exists in both prefixes at once.
        let at = key("an-id", "1");
        parent.put(at.clone(), b"from prod".to_vec()).await.unwrap();
        child.put(at.clone(), b"replaced".to_vec()).await.unwrap();
        assert_eq!(
            child.get(at).await.unwrap().as_deref(),
            Some(&b"replaced"[..]),
            "home comes first, or a branch could never change anything"
        );
    }
}
