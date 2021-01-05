use super::{Column, DataStore, PinModeRequirement};
use crate::error::Error;
use crate::repo::{PinKind, PinMode, PinStore, References};
use async_trait::async_trait;
use cid::{self, Cid};
use futures::stream::{StreamExt, TryStreamExt};
use once_cell::sync::OnceCell;
use sled::{self, Config as DbConfig, Db, Mode as DbMode};
use std::collections::BTreeSet;
use std::convert::Into;
use std::path::PathBuf;
use std::str::{self, FromStr};
use tracing_futures::Instrument;

#[derive(Debug)]
pub struct KvDataStore {
    path: PathBuf,
    // it is a trick for not modifying the Data:init
    db: OnceCell<Db>,
}

impl KvDataStore {
    // unused for now, but might be needed if we implement the DataStore api
    fn _put(&self, key: &str, value: &str) -> Result<(), Error> {
        let db = self.get_db();

        let _ = db.insert(key, value)?;

        Ok(())
    }

    fn remove(&self, key: &str) -> Result<(), Error> {
        let db = self.get_db();

        match db.remove(key) {
            Ok(_) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    fn apply_batch(&self, batch: sled::Batch) -> Result<(), Error> {
        let db = self.get_db();

        Ok(db.apply_batch(batch)?)
    }

    fn get_db(&self) -> &Db {
        self.db.get().unwrap()
    }
}

#[async_trait]
impl DataStore for KvDataStore {
    fn new(root: PathBuf) -> KvDataStore {
        KvDataStore {
            path: root,
            db: Default::default(),
        }
    }

    async fn init(&self) -> Result<(), Error> {
        let config = DbConfig::new();

        let db = config
            .mode(DbMode::HighThroughput)
            .path(self.path.as_path())
            .open()?;

        match self.db.set(db) {
            Ok(()) => Ok(()),
            Err(_) => Err(anyhow::anyhow!("failed to init sled")),
        }
    }

    async fn open(&self) -> Result<(), Error> {
        Ok(())
    }

    /// Checks if a key is present in the datastore.
    async fn contains(&self, _col: Column, _key: &[u8]) -> Result<bool, Error> {
        Err(anyhow::anyhow!("not implemented"))
    }

    /// Returns the value associated with a key from the datastore.
    async fn get(&self, _col: Column, _key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        Err(anyhow::anyhow!("not implemented"))
    }

    /// Puts the value under the key in the datastore.
    async fn put(&self, _col: Column, _key: &[u8], _value: &[u8]) -> Result<(), Error> {
        Err(anyhow::anyhow!("not implemented"))
    }

    /// Removes a key-value pair from the datastore.
    async fn remove(&self, _col: Column, _key: &[u8]) -> Result<(), Error> {
        Err(anyhow::anyhow!("not implemented"))
    }

    /// Wipes the datastore.
    async fn wipe(&self) {
        todo!()
    }
}

#[async_trait]
impl PinStore for KvDataStore {
    async fn is_pinned(&self, block: &Cid) -> Result<bool, Error> {
        is_pinned(self, block)
    }

    async fn insert_direct_pin(&self, target: &Cid) -> Result<(), Error> {
        let already_pinned = get_pinned_mode(self, target)?;

        let mut batch = sled::Batch::default();

        match already_pinned {
            Some(PinMode::Direct) => return Ok(()),
            Some(PinMode::Recursive) => return Err(anyhow::anyhow!("already pinned recursively")),
            Some(PinMode::Indirect) => {
                let pin_key = get_pin_key(target, &PinMode::Indirect);
                batch.remove(pin_key.as_str());
            }
            None => {}
        }

        let direct_key = get_pin_key(target, &PinMode::Direct);

        batch.insert(direct_key.as_str(), "");

        Ok(self.apply_batch(batch)?)
    }

    async fn insert_recursive_pin(
        &self,
        target: &Cid,
        referenced: References<'_>,
    ) -> Result<(), Error> {
        // TODO: this set is probably unnecessary, since we could just write everything to the
        // batch directly
        let set = referenced.try_collect::<BTreeSet<_>>().await?;

        let mut batch = sled::Batch::default();
        let already_pinned = get_pinned_mode(self, target)?;

        match already_pinned {
            Some(PinMode::Recursive) => return Ok(()),
            Some(mode @ PinMode::Direct) | Some(mode @ PinMode::Indirect) => {
                let key = get_pin_key(target, &mode);
                batch.remove(key.as_str());
            }
            None => {}
        }

        let recursive_key = get_pin_key(target, &PinMode::Recursive);
        batch.insert(recursive_key.as_str(), "");

        for cid in &set {
            let indirect_key = get_pin_key(cid, &PinMode::Indirect);

            if let Ok(true) = is_pinned(self, cid) {
                // FIXME: overall I think we should always propagate errors up, and not allow there
                // to be any unparseable keys
                continue;
            }

            // value is for get information like "Qmd9WDTA2Kph4MKiDDiaZdiB4HJQpKcxjnJQfQmM5rHhYK indirect through QmXr1XZBg1CQv17BPvSWRmM7916R6NLL7jt19rhCPdVhc5"
            // FIXME: this will not work with multiple blocks linking to the same block? also the
            // test is probably missing as well
            batch.insert(indirect_key.as_str(), target.to_string().as_str());
        }

        Ok(self.apply_batch(batch)?)
    }

    async fn remove_direct_pin(&self, target: &Cid) -> Result<(), Error> {
        if is_not_pinned_or_pinned_indirectly(self, target)? {
            return Err(anyhow::anyhow!("not pinned or pinned indirectly"));
        }

        let key = get_pin_key(target, &PinMode::Direct);

        Ok(self.remove(&key)?)
    }

    async fn remove_recursive_pin(
        &self,
        target: &Cid,
        referenced: References<'_>,
    ) -> Result<(), Error> {
        // TODO: is this "in the same transaction" as the batch which is created?
        if is_not_pinned_or_pinned_indirectly(self, target)? {
            return Err(anyhow::anyhow!("not pinned or pinned indirectly"));
        }

        // TODO: same here, could probably build the batch directly
        let set = referenced.try_collect::<BTreeSet<_>>().await?;

        let mut batch = sled::Batch::default();

        let recursive_key = get_pin_key(target, &PinMode::Recursive);
        batch.remove(recursive_key.as_str());

        for cid in &set {
            let already_pinned = get_pinned_mode(self, cid)?;

            match already_pinned {
                Some(PinMode::Recursive) | Some(PinMode::Direct) => continue, // this should be unreachable
                Some(PinMode::Indirect) => {
                    let indirect_key = get_pin_key(cid, &PinMode::Indirect);
                    batch.remove(indirect_key.as_str());
                }
                None => {}
            }
        }

        Ok(self.apply_batch(batch)?)
    }

    async fn list(
        &self,
        requirement: Option<PinMode>,
    ) -> futures::stream::BoxStream<'static, Result<(Cid, PinMode), Error>> {
        let db = self.get_db();

        // FIXME: this is still blocking ... might not be a way without a channel
        let iter = db.range::<String, std::ops::RangeFull>(..);

        let requirement = PinModeRequirement::from(requirement);

        let adapted = iter
            .map(|res| res.map_err(|e| Error::from(e)))
            .filter_map(move |res| match res {
                Ok((k, _v)) => {
                    if !k.starts_with(b"pin.") || k.len() < 7 {
                        return Some(Err(anyhow::anyhow!(
                            "invalid pin: {:?}",
                            String::from_utf8_lossy(&*k)
                        )));
                    }
                    let mode = match k[4] {
                        b'd' => PinMode::Direct,
                        b'r' => PinMode::Recursive,
                        b'i' => PinMode::Indirect,
                        x => return Some(Err(anyhow::anyhow!("invalid pinmode: {}", x as char))),
                    };

                    if !requirement.matches(&mode) {
                        None
                    } else {
                        let cid = std::str::from_utf8(&k[6..]).map_err(Error::from);
                        let cid = cid.and_then(|x| Cid::from_str(x).map_err(Error::from));
                        Some(cid.map(move |cid| (cid, mode)))
                    }
                }
                Err(e) => Some(Err(e)),
            });

        futures::stream::iter(adapted).in_current_span().boxed()
    }

    async fn query(
        &self,
        ids: Vec<Cid>,
        requirement: Option<PinMode>,
    ) -> Result<Vec<(Cid, PinKind<Cid>)>, Error> {
        let mut res = Vec::with_capacity(ids.len());

        let requirement = PinModeRequirement::from(requirement);

        let db = self.get_db();

        for id in ids {
            // FIXME: this is blocking
            match get_pinned_mode(self, &id)? {
                Some(pin_mode) => {
                    if !requirement.matches(&pin_mode) {
                        continue;
                    }

                    match pin_mode {
                        PinMode::Direct => res.push((id, PinKind::Direct)),
                        PinMode::Recursive => res.push((id, PinKind::Recursive(0))),
                        PinMode::Indirect => {
                            let pin_key = get_pin_key(&id, &PinMode::Indirect);

                            match db.get(pin_key.as_str())? {
                                Some(indirect_from_raw) => {
                                    let indirect_from_str =
                                        str::from_utf8(indirect_from_raw.as_ref())?;

                                    match Cid::from_str(indirect_from_str) {
                                        Ok(indirect_from_cid) => {
                                            res.push((id, PinKind::IndirectFrom(indirect_from_cid)))
                                        }
                                        Err(_) => {
                                            warn!("invalid indirect from cid of {}", id);
                                            continue;
                                        }
                                    }
                                }
                                None => {}
                            }
                        }
                    }
                }
                None => {}
            }
        }

        Ok(res)
    }
}

fn pin_mode_literal(pin_mode: &PinMode) -> &'static str {
    match pin_mode {
        PinMode::Direct => "d",
        PinMode::Indirect => "i",
        PinMode::Recursive => "r",
    }
}

fn get_pin_key(cid: &Cid, pin_mode: &PinMode) -> String {
    format!("pin.{}.{}", pin_mode_literal(pin_mode), cid)
}

fn get_pinned_mode(kv_db: &KvDataStore, block: &Cid) -> Result<Option<PinMode>, Error> {
    // FIXME: write this as async?
    for mode in &[PinMode::Direct, PinMode::Recursive, PinMode::Indirect] {
        let key = get_pin_key(block, mode);

        let db = kv_db.get_db();

        match db.get(key.as_str())? {
            Some(_) => return Ok(Some(*mode)),
            None => {}
        }
    }

    Ok(None)
}

fn is_pinned(db: &KvDataStore, cid: &Cid) -> Result<bool, Error> {
    match get_pinned_mode(db, cid)? {
        Some(_) => Ok(true),
        None => Ok(false),
    }
}

fn is_not_pinned_or_pinned_indirectly(db: &KvDataStore, block: &Cid) -> Result<bool, Error> {
    match get_pinned_mode(db, block)? {
        Some(PinMode::Indirect) | None => Ok(true),
        _ => Ok(false),
    }
}

#[cfg(test)]
crate::pinstore_interface_tests!(common_tests, crate::repo::kv::KvDataStore::new);
