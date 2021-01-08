use super::{Column, DataStore, PinModeRequirement};
use crate::error::Error;
use crate::repo::{PinKind, PinMode, PinStore, References};
use async_trait::async_trait;
use cid::{self, Cid};
use futures::stream::{StreamExt, TryStreamExt};
use once_cell::sync::OnceCell;
use sled::{
    self,
    transaction::{
        ConflictableTransactionError, TransactionError, TransactionResult, TransactionalTree,
        UnabortableTransactionError,
    },
    Config as DbConfig, Db, Mode as DbMode,
};
use std::collections::BTreeSet;
use std::convert::Infallible;
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
    fn get_db(&self) -> &Db {
        self.db.get().unwrap()
    }

    fn flush_async(&self) -> impl std::future::Future<Output = Result<(), Error>> + Send + '_ {
        use futures::future::TryFutureExt;
        self.db
            .get()
            .unwrap()
            .flush_async()
            .map_ok(|_| ())
            .map_err(Error::from)
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

// in the transactional parts of the [`Infallible`] is used to signal there is no additional
// custom error, not that the transaction was infallible in itself.

#[async_trait]
impl PinStore for KvDataStore {
    async fn is_pinned(&self, cid: &Cid) -> Result<bool, Error> {
        Ok(self.get_db().transaction::<_, _, Infallible>(|tree| {
            match get_pinned_mode(tree, cid)? {
                Some(_) => Ok(true),
                None => Ok(false),
            }
        })?)
    }

    async fn insert_direct_pin(&self, target: &Cid) -> Result<(), Error> {
        use ConflictableTransactionError::Abort;
        let res = self.get_db().transaction(|tx_tree| {
            let already_pinned = get_pinned_mode(&tx_tree, target)?;

            match already_pinned {
                Some(PinMode::Direct) => return Ok(false),
                Some(PinMode::Recursive) => {
                    return Err(Abort(anyhow::anyhow!("already pinned recursively")))
                }
                Some(PinMode::Indirect) => {
                    // TODO: I think the direct should live alongside the indirect?
                    let pin_key = get_pin_key(target, &PinMode::Indirect);
                    tx_tree.remove(pin_key.as_str())?;
                }
                None => {}
            }

            let direct_key = get_pin_key(target, &PinMode::Direct);
            tx_tree.insert(direct_key.as_str(), direct_value())?;

            Ok(true)
        });

        if launder(res)? {
            self.flush_async().await
        } else {
            Ok(())
        }
    }

    async fn insert_recursive_pin(
        &self,
        target: &Cid,
        referenced: References<'_>,
    ) -> Result<(), Error> {
        // since the transaction can be retried multiple times, we need to collect these and keep
        // iterating it until there is no conflict.
        let set = referenced.try_collect::<BTreeSet<_>>().await?;

        // the transaction is not infallible but there is no additional error we return
        let modified = self
            .get_db()
            .transaction::<_, _, Infallible>(move |tx_tree| {
                let already_pinned = get_pinned_mode(tx_tree, target)?;

                match already_pinned {
                    Some(PinMode::Recursive) => return Ok(false),
                    Some(mode @ PinMode::Direct) | Some(mode @ PinMode::Indirect) => {
                        // FIXME: this is probably another lapse in tests that both direct and
                        // indirect can be removed when inserting recursive?
                        let key = get_pin_key(target, &mode);
                        tx_tree.remove(key.as_str())?;
                    }
                    None => {}
                }
                let recursive_key = get_pin_key(target, &PinMode::Recursive);
                tx_tree.insert(recursive_key.as_str(), recursive_value())?;

                let target_value = indirect_value(target);

                // cannot use into_iter here as the transactions are retryable
                for cid in set.iter() {
                    let indirect_key = get_pin_key(&cid, &PinMode::Indirect);

                    if matches!(get_pinned_mode(tx_tree, &cid)?, Some(_)) {
                        // TODO: quite costly to do the get_pinned_mode here
                        continue;
                    }

                    // value is for get information like "Qmd9WDTA2Kph4MKiDDiaZdiB4HJQpKcxjnJQfQmM5rHhYK indirect through QmXr1XZBg1CQv17BPvSWRmM7916R6NLL7jt19rhCPdVhc5"
                    // FIXME: this will not work with multiple blocks linking to the same block? also the
                    // test is probably missing as well
                    tx_tree.insert(indirect_key.as_str(), target_value.as_str())?;
                }

                Ok(true)
            })?;

        if modified {
            self.flush_async().await
        } else {
            Ok(())
        }
    }

    async fn remove_direct_pin(&self, target: &Cid) -> Result<(), Error> {
        use ConflictableTransactionError::Abort;
        let res = self.get_db().transaction::<_, _, Error>(|tx_tree| {
            if is_not_pinned_or_pinned_indirectly(tx_tree, target)? {
                return Err(Abort(anyhow::anyhow!("not pinned or pinned indirectly")));
            }

            let key = get_pin_key(target, &PinMode::Direct);
            tx_tree.remove(key.as_str())?;
            Ok(())
        });

        launder(res)?;

        self.flush_async().await
    }

    async fn remove_recursive_pin(
        &self,
        target: &Cid,
        referenced: References<'_>,
    ) -> Result<(), Error> {
        use ConflictableTransactionError::Abort;
        // TODO: is this "in the same transaction" as the batch which is created?
        let set = referenced.try_collect::<BTreeSet<_>>().await?;

        let res = self.get_db().transaction(|tx_tree| {
            if is_not_pinned_or_pinned_indirectly(tx_tree, target)? {
                return Err(Abort(anyhow::anyhow!("not pinned or pinned indirectly")));
            }

            let recursive_key = get_pin_key(target, &PinMode::Recursive);
            tx_tree.remove(recursive_key.as_str())?;

            for cid in &set {
                let already_pinned = get_pinned_mode(tx_tree, cid)?;

                match already_pinned {
                    Some(PinMode::Recursive) | Some(PinMode::Direct) => continue, // this should be unreachable
                    Some(PinMode::Indirect) => {
                        // FIXME: not really sure of this but it might be that recursive removed
                        // the others...?
                        let indirect_key = get_pin_key(cid, &PinMode::Indirect);
                        tx_tree.remove(indirect_key.as_str())?;
                    }
                    None => {}
                }
            }

            Ok(())
        });

        launder(res)?;

        self.flush_async().await
    }

    async fn list(
        &self,
        requirement: Option<PinMode>,
    ) -> futures::stream::BoxStream<'static, Result<(Cid, PinMode), Error>> {
        let db = self.get_db();

        // FIXME: this is still blocking ... might not be a way without a channel
        // this probably doesn't need to be transactional? well, perhaps transactional reads would
        // be the best, not sure what is the guaratee for in-sequence key reads.
        let iter = db.range::<String, std::ops::RangeFull>(..);

        let requirement = PinModeRequirement::from(requirement);

        let adapted = iter
            .map(|res| res.map_err(Error::from))
            .filter_map(move |res| match res {
                Ok((k, _v)) => {
                    if !k.starts_with(b"pin.") || k.len() < 7 {
                        return Some(Err(anyhow::anyhow!(
                            "invalid pin: {:?}",
                            &*String::from_utf8_lossy(&*k)
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
                        let cid = cid.map_err(|e| {
                            e.context(format!(
                                "failed to read pin: {:?}",
                                &*String::from_utf8_lossy(&*k)
                            ))
                        });
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
        use ConflictableTransactionError::Abort;
        let requirement = PinModeRequirement::from(requirement);

        let db = self.get_db();

        let result = db.transaction::<_, _, Error>(|tx_tree| {
            // since its an Fn closure this cannot be reserved once ... not sure why it couldn't be
            // FnMut?
            let mut res = Vec::with_capacity(ids.len());

            // as we might loop over an over on the tx we might need this over and over, cannot
            // take ownership.
            for id in ids.iter() {
                // FIXME: this is blocking ...
                let mode = get_pinned_mode(tx_tree, &id)?;

                let matched = match mode {
                    Some(pin_mode) if requirement.matches(&pin_mode) => match pin_mode {
                        PinMode::Direct => Some(PinKind::Direct),
                        PinMode::Recursive => Some(PinKind::Recursive(0)),
                        PinMode::Indirect => {
                            let pin_key = get_pin_key(&id, &PinMode::Indirect);

                            tx_tree
                                .get(pin_key.as_str())?
                                .map(|root| {
                                    cid_from_indirect_value(&*root)
                                        .map(|cid| PinKind::IndirectFrom(cid))
                                        .map_err(|e| {
                                            Abort(e.context(format!(
                                                "failed to read indirect pin source: {:?}",
                                                String::from_utf8_lossy(root.as_ref()).as_ref(),
                                            )))
                                        })
                                })
                                .transpose()?
                        }
                    },
                    Some(_) | None => None,
                };

                res.push(matched);
            }

            Ok(res)
        });

        let indices = launder(result)?;

        assert_eq!(indices.len(), ids.len());

        // for each original id in ids, there is a corresponding Option<PinMode<_>>
        // indices.len() is the upper bound of Some values, could be counted as well.

        let mut cids = Vec::with_capacity(indices.len());

        for (cid, maybe_mode) in ids.into_iter().zip(indices.into_iter()) {
            if let Some(mode) = maybe_mode {
                cids.push((cid, mode));
            }
        }

        Ok(cids)
    }
}

/// Name the empty value stored for direct pins; the pin key itself describes the mode and the cid.
fn direct_value() -> &'static [u8] {
    Default::default()
}

/// Name the empty value stored for recursive pins at the top.
fn recursive_value() -> &'static [u8] {
    Default::default()
}

/// Name the value stored for indirect pins, currently only the most recent recursive pin.
fn indirect_value(recursively_pinned: &Cid) -> String {
    recursively_pinned.to_string()
}

/// Inverse of [`indirect_value`].
fn cid_from_indirect_value(bytes: &[u8]) -> Result<Cid, Error> {
    str::from_utf8(bytes.as_ref())
        .map_err(Error::from)
        .and_then(|s| Cid::from_str(s).map_err(Error::from))
}

/// Helper needed as the error cannot just `?` converted.
fn launder<T>(res: TransactionResult<T, Error>) -> Result<T, Error> {
    use TransactionError::*;
    match res {
        Ok(t) => Ok(t),
        Err(Abort(e)) => Err(e),
        Err(Storage(e)) => Err(e.into()),
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
    // TODO: get_pinned_mode could be range query if the pin modes were suffixes, keys would need
    // to be cid.to_bytes().push(pin_mode_literal(pin_mode))? ... since the cid bytes
    // representation already contains the length we should be good to go in all cases.
    //
    // for storing multiple targets then the last could be found by doing a query as well. in the
    // case of multiple indirect pins they'd have to be with another suffix.
    //
    // TODO: check if such representation would really order properly
    format!("pin.{}.{}", pin_mode_literal(pin_mode), cid)
}

fn get_pinned_mode(
    tree: &TransactionalTree,
    block: &Cid,
) -> Result<Option<PinMode>, UnabortableTransactionError> {
    // FIXME: write this as async?
    for mode in &[PinMode::Direct, PinMode::Recursive, PinMode::Indirect] {
        let key = get_pin_key(block, mode);

        if tree.get(key.as_str())?.is_some() {
            return Ok(Some(*mode));
        }
    }

    Ok(None)
}

fn is_not_pinned_or_pinned_indirectly(
    tree: &TransactionalTree,
    block: &Cid,
) -> Result<bool, UnabortableTransactionError> {
    match get_pinned_mode(tree, block)? {
        Some(PinMode::Indirect) | None => Ok(true),
        _ => Ok(false),
    }
}

#[cfg(test)]
crate::pinstore_interface_tests!(common_tests, crate::repo::kv::KvDataStore::new);
