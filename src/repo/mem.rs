//! Volatile memory backed repo
use crate::error::Error;
use crate::repo::{
    BlockPut, BlockStore, Column, DataStore, Lock, LockError, PinKind, PinMode, PinModeRequirement,
    PinStore,
};
use crate::Block;
use async_trait::async_trait;
use cid::Cid;
use std::convert::TryFrom;
use std::path::PathBuf;
use tokio::sync::{Mutex, OwnedMutexGuard};

use super::{BlockRm, BlockRmError, RepoCid};
use std::collections::hash_map::Entry;

// FIXME: Transition to Persistent Map to make iterating more consistent
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Describes an in-memory block store.
///
/// Blocks are stored as a `HashMap` of the `Cid` and `Block`.
#[derive(Debug, Default)]
pub struct MemBlockStore {
    blocks: Mutex<HashMap<RepoCid, Block>>,
}

#[async_trait]
impl BlockStore for MemBlockStore {
    fn new(_path: PathBuf) -> Self {
        Default::default()
    }

    async fn init(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn open(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn contains(&self, cid: &Cid) -> Result<bool, Error> {
        let contains = self
            .blocks
            .lock()
            .await
            .contains_key(&RepoCid(cid.to_owned()));
        Ok(contains)
    }

    async fn get(&self, cid: &Cid) -> Result<Option<Block>, Error> {
        let block = self
            .blocks
            .lock()
            .await
            .get(&RepoCid(cid.to_owned()))
            .map(|block| block.to_owned());
        Ok(block)
    }

    async fn put(&self, block: Block) -> Result<(Cid, BlockPut), Error> {
        use std::collections::hash_map::Entry;
        let mut g = self.blocks.lock().await;
        match g.entry(RepoCid(block.cid.clone())) {
            Entry::Occupied(_) => {
                trace!("already existing block");
                Ok((block.cid, BlockPut::Existed))
            }
            Entry::Vacant(ve) => {
                trace!("new block");
                let cid = ve.key().0.clone();
                ve.insert(block);
                Ok((cid, BlockPut::NewBlock))
            }
        }
    }

    async fn remove(&self, cid: &Cid) -> Result<Result<BlockRm, BlockRmError>, Error> {
        match self.blocks.lock().await.remove(&RepoCid(cid.to_owned())) {
            Some(_block) => Ok(Ok(BlockRm::Removed(cid.clone()))),
            None => Ok(Err(BlockRmError::NotFound(cid.clone()))),
        }
    }

    async fn list(&self) -> Result<Vec<Cid>, Error> {
        let guard = self.blocks.lock().await;
        Ok(guard.iter().map(|(cid, _block)| cid.0.clone()).collect())
    }

    async fn wipe(&self) {
        self.blocks.lock().await.clear();
    }
}

/// Describes an in-memory `DataStore`.
#[derive(Debug, Default)]
pub struct MemDataStore {
    ipns: Mutex<HashMap<Vec<u8>, Vec<u8>>>,
    // this could also be PinDocument however doing any serialization allows to see the required
    // error types easier
    pin: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>,
}

impl MemDataStore {
    /// Returns true if the pin document was changed, false otherwise.
    fn insert_pin<'a>(
        g: &mut OwnedMutexGuard<HashMap<Vec<u8>, Vec<u8>>>,
        target: &'a Cid,
        kind: &'a PinKind<&'_ Cid>,
    ) -> Result<bool, Error> {
        // rationale for storing as Cid: the same multihash can be pinned with different codecs.
        // even if there aren't many polyglot documents known, pair of raw and the actual codec is
        // always a possibility.
        let key = target.to_bytes();

        match g.entry(key) {
            Entry::Occupied(mut oe) => {
                let mut doc: PinDocument = serde_json::from_slice(oe.get())?;
                if doc.update(true, kind)? {
                    let vec = oe.get_mut();
                    vec.clear();
                    serde_json::to_writer(vec, &doc)?;
                    trace!(doc = ?doc, kind = ?kind, "updated on insert");
                    Ok(true)
                } else {
                    trace!(doc = ?doc, kind = ?kind, "update not needed on insert");
                    Ok(false)
                }
            }
            Entry::Vacant(ve) => {
                let mut doc = PinDocument {
                    version: 0,
                    direct: false,
                    recursive: Recursive::Not,
                    cid_version: match target.version() {
                        cid::Version::V0 => 0,
                        cid::Version::V1 => 1,
                    },
                    indirect_by: Vec::new(),
                };

                doc.update(true, &kind).unwrap();
                let vec = serde_json::to_vec(&doc)?;
                ve.insert(vec);
                trace!(doc = ?doc, kind = ?kind, "created on insert");
                Ok(true)
            }
        }
    }

    /// Returns true if the pin document was changed, false otherwise.
    fn remove_pin<'a>(
        g: &mut OwnedMutexGuard<HashMap<Vec<u8>, Vec<u8>>>,
        target: &'a Cid,
        kind: &'a PinKind<&'_ Cid>,
    ) -> Result<bool, Error> {
        // see cid vs. multihash from [`insert_direct_pin`]
        let key = target.to_bytes();

        match g.entry(key) {
            Entry::Occupied(mut oe) => {
                let mut doc: PinDocument = serde_json::from_slice(oe.get())?;
                if !doc.update(false, kind)? {
                    trace!(doc = ?doc, kind = ?kind, "update not needed on removal");
                    return Ok(false);
                }

                if doc.can_remove() {
                    oe.remove();
                } else {
                    let vec = oe.get_mut();
                    vec.clear();
                    serde_json::to_writer(vec, &doc)?;
                }

                Ok(true)
            }
            Entry::Vacant(_) => Err(anyhow::anyhow!("not pinned")),
        }
    }
}

#[async_trait]
impl PinStore for MemDataStore {
    async fn is_pinned(&self, block: &Cid) -> Result<bool, Error> {
        let key = block.to_bytes();

        let g = self.pin.lock().await;

        // the use of PinKind::RecursiveIntention necessitates the only return fast for
        // only the known pins; we should somehow now query to see if there are any
        // RecursiveIntention's. If there are any, we must walk the refs of each to see if the
        // `block` is amongst of those recursive references which are not yet written to disk.
        //
        // doing this without holding a repo lock is not possible, so leaving this as partial
        // implementation right now.
        Ok(g.contains_key(&key))
    }

    async fn insert_direct_pin(&self, target: &Cid) -> Result<(), Error> {
        let mut g = Mutex::lock_owned(Arc::clone(&self.pin)).await;
        Self::insert_pin(&mut g, target, &PinKind::Direct)?;
        Ok(())
    }

    async fn remove_direct_pin(&self, target: &Cid) -> Result<(), Error> {
        let mut g = Mutex::lock_owned(Arc::clone(&self.pin)).await;
        Self::remove_pin(&mut g, target, &PinKind::Direct)?;
        Ok(())
    }

    async fn insert_recursive_pin(
        &self,
        target: &Cid,
        mut refs: super::References<'_>,
    ) -> Result<(), Error> {
        use futures::stream::TryStreamExt;

        let mut g = Mutex::lock_owned(Arc::clone(&self.pin)).await;

        // this must fail if it is already fully pinned
        Self::insert_pin(&mut g, target, &PinKind::RecursiveIntention)?;

        let target_v1 = if target.version() == cid::Version::V1 {
            target.to_owned()
        } else {
            // this is one more allocation
            Cid::new_v1(target.codec(), target.hash().to_owned())
        };

        // collect these before even if they are many ... not sure if this is a good idea but, the
        // inmem version doesn't need to be all that great. this could be for nothing, if the root
        // was already pinned.

        let mut count = 0;
        let kind = PinKind::IndirectFrom(&target_v1);
        while let Some(next) = refs.try_next().await? {
            // no rollback, nothing
            Self::insert_pin(&mut g, &next, &kind)?;
            count += 1;
        }

        let kind = PinKind::Recursive(count as u64);
        Self::insert_pin(&mut g, target, &kind)?;

        Ok(())
    }

    async fn remove_recursive_pin(
        &self,
        target: &Cid,
        mut refs: super::References<'_>,
    ) -> Result<(), Error> {
        use futures::stream::TryStreamExt;

        let mut g = Mutex::lock_owned(Arc::clone(&self.pin)).await;

        let doc: PinDocument = match g.get(&target.to_bytes()) {
            Some(raw) => match serde_json::from_slice(raw) {
                Ok(doc) => doc,
                Err(e) => return Err(e.into()),
            },
            // well we know it's not pinned at all but this is the general error message
            None => return Err(anyhow::anyhow!("not pinned or pinned indirectly")),
        };

        let kind = match doc.pick_kind() {
            Some(Ok(kind @ PinKind::Recursive(_)))
            | Some(Ok(kind @ PinKind::RecursiveIntention)) => kind,
            Some(Ok(PinKind::Direct)) => {
                Self::remove_pin(&mut g, target, &PinKind::Direct)?;
                return Ok(());
            }
            Some(Ok(PinKind::IndirectFrom(cid))) => {
                return Err(anyhow::anyhow!("pinned indirectly through {}", cid))
            }
            // same here as above with the same message
            _ => return Err(anyhow::anyhow!("not pinned or pinned indirectly")),
        };

        // this must fail if it is already fully pinned
        Self::remove_pin(&mut g, target, &kind.as_ref())?;

        let target_v1 = if target.version() == cid::Version::V1 {
            target.to_owned()
        } else {
            // this is one more allocation
            Cid::new_v1(target.codec(), target.hash().to_owned())
        };

        let kind = PinKind::IndirectFrom(&target_v1);
        while let Some(next) = refs.try_next().await? {
            // no rollback, nothing
            Self::remove_pin(&mut g, &next, &kind)?;
        }

        Ok(())
    }

    async fn list(
        &self,
        requirement: Option<PinMode>,
    ) -> futures::stream::BoxStream<'static, Result<(Cid, PinMode), Error>> {
        use futures::stream::StreamExt;
        use std::convert::TryFrom;
        let g = self.pin.lock().await;

        let requirement = PinModeRequirement::from(requirement);

        let copy = g
            .iter()
            .map(|(key, value)| {
                let cid = Cid::try_from(key.as_slice())?;
                let doc: PinDocument = serde_json::from_slice(value)?;
                let mode = doc.mode().ok_or_else(|| anyhow::anyhow!("invalid mode"))?;

                Ok((cid, mode))
            })
            .filter(move |res| {
                // could return just two different boxed streams
                match res {
                    Ok((_, mode)) => requirement.matches(mode),
                    Err(_) => true,
                }
            })
            .collect::<Vec<_>>();

        futures::stream::iter(copy).boxed()
    }

    async fn query(
        &self,
        cids: Vec<Cid>,
        requirement: Option<PinMode>,
    ) -> Result<Vec<(Cid, PinKind<Cid>)>, Error> {
        let g = self.pin.lock().await;

        let requirement = PinModeRequirement::from(requirement);

        cids.into_iter()
            .map(move |cid| {
                match g.get(&cid.to_bytes()) {
                    Some(raw) => {
                        let doc: PinDocument = match serde_json::from_slice(raw) {
                            Ok(doc) => doc,
                            Err(e) => return Err(e.into()),
                        };
                        // None from document is bad result, since the document shouldn't exist in the
                        // first place
                        let mode = match doc.pick_kind() {
                            Some(Ok(kind)) => kind,
                            Some(Err(invalid_cid)) => return Err(Error::new(invalid_cid)),
                            None => {
                                trace!(doc = ?doc, "could not pick pin kind");
                                return Err(anyhow::anyhow!("{} is not pinned", cid));
                            }
                        };

                        // would be more clear if this business was in a separate map; quite awful
                        // as it is now

                        let matches = requirement.matches(&mode);

                        if matches {
                            trace!(cid = %cid, req = ?requirement, "pin matches");
                            return Ok((cid, mode));
                        } else {
                            // FIXME: this error is about the same as http api expects
                            return Err(anyhow::anyhow!(
                                "{} is not pinned as {:?}",
                                cid,
                                requirement
                                    .required()
                                    .expect("matches is never false if requirement is none")
                            ));
                        }
                    }
                    None => {
                        trace!(cid = %cid, "no record found");
                    }
                }

                // FIXME: this error is expected on http interface
                Err(anyhow::anyhow!("{} is not pinned", cid))
            })
            .collect::<Result<Vec<_>, _>>()
    }
}

#[async_trait]
impl DataStore for MemDataStore {
    fn new(_path: PathBuf) -> Self {
        Default::default()
    }

    async fn init(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn open(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn contains(&self, col: Column, key: &[u8]) -> Result<bool, Error> {
        let map = match col {
            Column::Ipns => &self.ipns,
        };
        let contains = map.lock().await.contains_key(key);
        Ok(contains)
    }

    async fn get(&self, col: Column, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let map = match col {
            Column::Ipns => &self.ipns,
        };
        let value = map.lock().await.get(key).map(|value| value.to_owned());
        Ok(value)
    }

    async fn put(&self, col: Column, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let map = match col {
            Column::Ipns => &self.ipns,
        };
        map.lock().await.insert(key.to_owned(), value.to_owned());
        Ok(())
    }

    async fn remove(&self, col: Column, key: &[u8]) -> Result<(), Error> {
        let map = match col {
            Column::Ipns => &self.ipns,
        };
        map.lock().await.remove(key);
        Ok(())
    }

    async fn wipe(&self) {
        self.ipns.lock().await.clear();
        self.pin.lock().await.clear();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Recursive {
    /// Persistent record of **completed** recursive pinning. All references now have indirect pins
    /// recorded.
    Count(u64),
    /// Persistent record of intent to add recursive pins to all indirect blocks or even not to
    /// keep the go-ipfs way which might not be a bad idea after all. Adding all the indirect pins
    /// on disk will cause massive write amplification in the end, but lets keep that way until we
    /// get everything working at least.
    Intent,
    /// Not pinned recursively.
    Not,
}

impl Recursive {
    fn is_set(&self) -> bool {
        match self {
            Recursive::Count(_) | Recursive::Intent => true,
            Recursive::Not => false,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct PinDocument {
    version: u8,
    direct: bool,
    // how many descendants; something to check when walking
    recursive: Recursive,
    // no further metadata necessary; cids are pinned by full cid
    cid_version: u8,
    // using the cidv1 versions of all cids here, not sure if that makes sense or is important
    indirect_by: Vec<String>,
}

impl PinDocument {
    fn update(&mut self, add: bool, kind: &PinKind<&'_ Cid>) -> Result<bool, PinUpdateError> {
        // these update rules are a bit complex and there are cases we don't need to handle.
        // Updating on upon `PinKind` forces the caller to inspect what the current state is for
        // example to handle the case of failing "unpin currently recursively pinned as direct".
        // the ruleset seems quite strange to be honest.
        match kind {
            PinKind::IndirectFrom(root) => {
                let root = if root.version() == cid::Version::V1 {
                    root.to_string()
                } else {
                    // this is one more allocation
                    Cid::new_v1(root.codec(), (*root).hash().to_owned()).to_string()
                };

                let modified = if self.indirect_by.is_empty() {
                    if add {
                        self.indirect_by.push(root);
                        true
                    } else {
                        false
                    }
                } else {
                    let mut set = self
                        .indirect_by
                        .drain(..)
                        .collect::<std::collections::BTreeSet<_>>();

                    let modified = if add {
                        set.insert(root)
                    } else {
                        set.remove(&root)
                    };

                    self.indirect_by.extend(set.into_iter());
                    modified
                };

                Ok(modified)
            }
            PinKind::Direct => {
                if self.recursive.is_set() && !self.direct && add {
                    // go-ipfs: cannot make recursive pin also direct
                    // not really sure why does this rule exist; the other way around is allowed
                    return Err(PinUpdateError::AlreadyPinnedRecursive);
                }

                if !add && !self.direct {
                    if !self.recursive.is_set() {
                        return Err(PinUpdateError::CannotUnpinUnpinned);
                    } else {
                        return Err(PinUpdateError::CannotUnpinDirectOnRecursivelyPinned);
                    }
                }

                let modified = self.direct != add;
                self.direct = add;
                Ok(modified)
            }
            PinKind::RecursiveIntention => {
                let modified = if add {
                    match self.recursive {
                        Recursive::Count(_) => return Err(PinUpdateError::AlreadyPinnedRecursive),
                        // can overwrite Intent with another Intent, as Ipfs::insert_pin is now moving to fix
                        // the Intent into the "final form" of Recursive::Count.
                        Recursive::Intent => false,
                        Recursive::Not => {
                            self.recursive = Recursive::Intent;
                            self.direct = false;
                            true
                        }
                    }
                } else {
                    match self.recursive {
                        Recursive::Count(_) | Recursive::Intent => {
                            self.recursive = Recursive::Not;
                            true
                        }
                        Recursive::Not => false,
                    }
                };

                Ok(modified)
            }
            PinKind::Recursive(descendants) => {
                let descendants = *descendants;
                let modified = if add {
                    match self.recursive {
                        Recursive::Count(other) if other != descendants => {
                            return Err(PinUpdateError::UnexpectedNumberOfDescendants(
                                other,
                                descendants,
                            ))
                        }
                        Recursive::Count(_) => false,
                        Recursive::Intent | Recursive::Not => {
                            self.recursive = Recursive::Count(descendants);
                            // the previously direct has now been upgraded to recursive, it can
                            // still be indirect though
                            self.direct = false;
                            true
                        }
                    }
                } else {
                    match self.recursive {
                        Recursive::Count(other) if other != descendants => {
                            return Err(PinUpdateError::UnexpectedNumberOfDescendants(
                                other,
                                descendants,
                            ))
                        }
                        Recursive::Count(_) | Recursive::Intent => {
                            self.recursive = Recursive::Not;
                            true
                        }
                        Recursive::Not => return Err(PinUpdateError::NotPinnedRecursive),
                    }
                    // FIXME: removing ... not sure if this is an issue; was thinking that maybe
                    // the update might need to be split to allow different api for removal than
                    // addition.
                };
                Ok(modified)
            }
        }
    }

    fn can_remove(&self) -> bool {
        !self.direct && !self.recursive.is_set() && self.indirect_by.is_empty()
    }

    fn mode(&self) -> Option<PinMode> {
        if self.recursive.is_set() {
            Some(PinMode::Recursive)
        } else if !self.indirect_by.is_empty() {
            Some(PinMode::Indirect)
        } else if self.direct {
            Some(PinMode::Direct)
        } else {
            None
        }
    }

    fn pick_kind(&self) -> Option<Result<PinKind<Cid>, cid::Error>> {
        self.mode().map(|p| {
            Ok(match p {
                PinMode::Recursive => match self.recursive {
                    Recursive::Intent => PinKind::RecursiveIntention,
                    Recursive::Count(total) => PinKind::Recursive(total),
                    _ => unreachable!("mode should not have returned PinKind::Recursive"),
                },
                PinMode::Indirect => {
                    // go-ipfs does seem to be doing a fifo looking, perhaps this is a list there, or
                    // the indirect pins aren't being written down anywhere and they just refs from
                    // recursive roots.
                    let cid = Cid::try_from(self.indirect_by[0].as_str())?;
                    PinKind::IndirectFrom(cid)
                }
                PinMode::Direct => PinKind::Direct,
            })
        })
    }
}

/// Describes the error variants for updates to object pinning.
#[derive(Debug, thiserror::Error)]
pub enum PinUpdateError {
    /// The current and expected descendants of an already recursively pinned object don't match.
    #[error("unexpected number of descendants ({}), found {}", .1, .0)]
    UnexpectedNumberOfDescendants(u64, u64),
    /// Recursive update fails as it wasn't pinned recursively.
    #[error("not pinned recursively")]
    NotPinnedRecursive,
    /// Not allowed: Adding direct pin while pinned recursive.
    #[error("already pinned recursively")]
    AlreadyPinnedRecursive,
    /// Can't unpin already inpinned.
    #[error("not pinned or pinned indirectly")]
    CannotUnpinUnpinned,
    // go-ipfs prepends the ipfspath here
    /// Can't unpin direct on a recursively pinned object.
    #[error("is pinned recursively")]
    CannotUnpinDirectOnRecursivelyPinned,
}

// Used for in memory repos, currently not implementing any true locking.
#[derive(Debug)]
pub struct MemLock;

impl Lock for MemLock {
    fn new(_path: PathBuf) -> Self {
        Self
    }

    fn try_exclusive(&mut self) -> Result<(), LockError> {
        Ok(())
    }
}

#[cfg(test)]
crate::pinstore_interface_tests!(common_tests, crate::repo::mem::MemDataStore::new);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Block;
    use cid::{Cid, Codec};
    use multihash::Sha2_256;
    use std::env::temp_dir;

    #[tokio::test]
    async fn test_mem_blockstore() {
        let tmp = temp_dir();
        let store = MemBlockStore::new(tmp);
        let data = b"1".to_vec().into_boxed_slice();
        let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&data));
        let block = Block::new(data, cid.clone());

        store.init().await.unwrap();
        store.open().await.unwrap();

        let contains = store.contains(&cid);
        assert_eq!(contains.await.unwrap(), false);
        let get = store.get(&cid);
        assert_eq!(get.await.unwrap(), None);
        if store.remove(&cid).await.unwrap().is_ok() {
            panic!("block should not be found")
        }

        let put = store.put(block.clone());
        assert_eq!(put.await.unwrap().0, cid.to_owned());
        let contains = store.contains(&cid);
        assert_eq!(contains.await.unwrap(), true);
        let get = store.get(&cid);
        assert_eq!(get.await.unwrap(), Some(block.clone()));

        store.remove(&cid).await.unwrap().unwrap();
        let contains = store.contains(&cid);
        assert_eq!(contains.await.unwrap(), false);
        let get = store.get(&cid);
        assert_eq!(get.await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_mem_blockstore_list() {
        let tmp = temp_dir();
        let mem_store = MemBlockStore::new(tmp);

        mem_store.init().await.unwrap();
        mem_store.open().await.unwrap();

        for data in &[b"1", b"2", b"3"] {
            let data_slice = data.to_vec().into_boxed_slice();
            let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&data_slice));
            let block = Block::new(data_slice, cid);
            mem_store.put(block.clone()).await.unwrap();
            assert!(mem_store.contains(block.cid()).await.unwrap());
        }

        let cids = mem_store.list().await.unwrap();
        assert_eq!(cids.len(), 3);
        for cid in cids.iter() {
            assert!(mem_store.contains(cid).await.unwrap());
        }
    }

    #[tokio::test]
    async fn test_mem_datastore() {
        let tmp = temp_dir();
        let store = MemDataStore::new(tmp);
        let col = Column::Ipns;
        let key = [1, 2, 3, 4];
        let value = [5, 6, 7, 8];

        store.init().await.unwrap();
        store.open().await.unwrap();

        let contains = store.contains(col, &key);
        assert_eq!(contains.await.unwrap(), false);
        let get = store.get(col, &key);
        assert_eq!(get.await.unwrap(), None);
        store.remove(col, &key).await.unwrap();

        let put = store.put(col, &key, &value);
        put.await.unwrap();
        let contains = store.contains(col, &key);
        assert_eq!(contains.await.unwrap(), true);
        let get = store.get(col, &key);
        assert_eq!(get.await.unwrap(), Some(value.to_vec()));

        store.remove(col, &key).await.unwrap();
        let contains = store.contains(col, &key);
        assert_eq!(contains.await.unwrap(), false);
        let get = store.get(col, &key);
        assert_eq!(get.await.unwrap(), None);
    }

    #[test]
    fn pindocument_on_direct_pin() {
        let mut doc = PinDocument {
            version: 0,
            direct: false,
            recursive: Recursive::Not,
            cid_version: 0,
            indirect_by: Vec::new(),
        };

        assert!(doc.update(true, &PinKind::Direct).unwrap());

        assert_eq!(doc.mode(), Some(PinMode::Direct));
        assert_eq!(doc.pick_kind().unwrap().unwrap(), PinKind::Direct);
    }
}
