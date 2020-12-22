#![allow(unused_variables)]
#![allow(unused_imports)]
#![allow(dead_code)]

use leveldb::database::Database;
use leveldb::options::{Options, ReadOptions, WriteOptions};
use leveldb::batch::{WriteBatch, Batch};
use leveldb::iterator::{Iterable, KeyIterator};
use async_trait::async_trait;
use std::path::PathBuf;
use std::str::FromStr;
use cid::{self, Cid};
use std::sync::{Mutex, Arc};
use std::cell::{RefCell, Ref};
use futures::stream::{StreamExt, TryStreamExt};
use crate::error::Error;
use crate::repo::{PinStore, References, PinMode, PinKind};
use super::{Column, DataStore};
use tracing_futures::Instrument;


#[derive(Debug)]
pub struct LeveldbStore {
    path: PathBuf,
    database: Option<Database>,
    read_options: ReadOptions,
    write_options: WriteOptions,
}

impl LeveldbStore {
    fn get_u8(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let db = self.get_database();
        let value = db.get_u8(&self.read_options, key)?;

        Ok(value)
    }

    fn put_u8(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let db = self.get_database();

        match db.put_u8(&self.write_options, key, value) {
            Ok(_) => Ok(()),
            Err(e) => Err(e.into())
        }
    }

    fn write_batch(&self, batch: &WriteBatch) -> Result<(), Error> {
        let db = self.get_database();

        match db.write(&self.write_options, batch) {
            Ok(_) => Ok(()),
            Err(e) => Err(e.into())
        }
    }

    fn delete(&self, key: &[u8]) -> Result<(), Error> {
        let db = self.get_database();

        match db.delete_u8(&self.write_options, key) {
            Ok(_) => Ok(()),
            Err(e) => Err(e.into())
        }
    }

    fn get_database(&self) -> &Database {
        self.database.as_ref().unwrap()
    }

    fn key_iter(&self) -> KeyIterator {
        let db = self.get_database();

        db.keys_iter(&self.read_options)
    }

}

#[async_trait]
impl DataStore for LeveldbStore {
    fn new(root: PathBuf) -> LeveldbStore {
        LeveldbStore {
            path: root,
            database: None,
            read_options: ReadOptions::new(),
            write_options: WriteOptions::new(),
        }
    }

    async fn init(&self) -> Result<(), Error> {
        let mut options = Options::new();
        options.create_if_missing = true;

        let database = Database::open(self.path.as_path(), &options)?;

        unsafe {
            let db_ref = self as * const LeveldbStore;
            let db_mut = db_ref as * mut LeveldbStore;
            (*db_mut).database = Some(database);
        }

        Ok(())
    }

    async fn open(&self) -> Result<(), Error> {
        Ok(())
    }

    /// Checks if a key is present in the datastore.
    async fn contains(&self, col: Column, key: &[u8]) -> Result<bool, Error> {
        unimplemented!("")
    }

    /// Returns the value associated with a key from the datastore.
    async fn get(&self, col: Column, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        unimplemented!("")
    }

    /// Puts the value under the key in the datastore.
    async fn put(&self, col: Column, key: &[u8], value: &[u8]) -> Result<(), Error> {
        unimplemented!("")
    }

    /// Removes a key-value pair from the datastore.
    async fn remove(&self, col: Column, key: &[u8]) -> Result<(), Error> {
        unimplemented!("")
    }

    /// Wipes the datastore.
    async fn wipe(&self) {
        unimplemented!("")
    }
}


#[async_trait]
impl PinStore for LeveldbStore {
    async fn is_pinned(&self, block: &Cid) -> Result<bool, Error> {
        is_pinned(self, block)
    }

    async fn insert_direct_pin(&self, target: &Cid) -> Result<(), Error> {
        let already_pinned = get_pinned_mode(self, target)?;

        let batch = WriteBatch::new();

        match already_pinned {
            Some(PinMode::Direct) => return Ok(()),
            Some(PinMode::Recursive) => return Err(anyhow::anyhow!("pin: {} already pinned recursively", target.to_string())),
            Some(PinMode::Indirect) => batch.delete_u8(get_pin_key(target, &PinMode::Indirect).as_bytes()),
            _ => {}
        }

        let direct_key = get_pin_key(target, &PinMode::Direct);

        batch.put_u8(direct_key.as_bytes(), &[][..]);

        match self.write_batch(&batch) {
            Ok(_) => Ok(()),
            Err(e) => Err(e.into())
        }
    }

    async fn insert_recursive_pin(&self, target: &Cid, referenced: References<'_>, )
                                  -> Result<(), Error>
    {

        let set = referenced.try_collect::<std::collections::BTreeSet<_>>()
            .await?;

        let batch = WriteBatch::new();
        let already_pinned = get_pinned_mode(self, target)?;

        match already_pinned {
            Some(PinMode::Recursive) => return Ok(()),
            Some(mode@PinMode::Direct) | Some(mode@PinMode::Indirect) => {
                let key = get_pin_key(target, &mode);
                batch.delete_u8(key.as_bytes());
            }
            _ => {}
        }


        let recursive_key = get_pin_key(target, &PinMode::Recursive);
        batch.put_u8(recursive_key.as_bytes(), &[][..]);

        for cid in &set {
            let indirect_key = get_pin_key(cid, &PinMode::Indirect);

            let is_already_pinned = is_pinned(self,target);

            match is_already_pinned {
                Ok(true) => continue,
                _ => {},
            }

            // value is for get information like Qmd9WDTA2Kph4MKiDDiaZdiB4HJQpKcxjnJQfQmM5rHhYK indirect through QmXr1XZBg1CQv17BPvSWRmM7916R6NLL7jt19rhCPdVhc5
            batch.put_u8(indirect_key.as_bytes(), target.to_string().as_bytes());
        }

        match self.write_batch(&batch) {
            Ok(_) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn remove_direct_pin(&self, target: &Cid) -> Result<(), Error> {
        let key = get_pin_key(target, &PinMode::Direct);

        match self.delete(key.as_bytes()) {
            Ok(_) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn remove_recursive_pin(&self, target: &Cid, referenced: References<'_>, ) -> Result<(), Error> {
        let set = referenced.try_collect::<std::collections::BTreeSet<_>>()
            .await?;
        let cid_str = target.to_string();

        let batch = WriteBatch::new();

        batch.delete_u8(get_pin_key(target, &PinMode::Recursive).as_bytes());

        for cid in &set {
            let already_pinned = get_pinned_mode(self, cid)?;

            match already_pinned {
                Some(PinMode::Recursive) | Some(PinMode::Direct) => continue,
                Some(PinMode::Indirect) => {
                    let indirect_key = get_pin_key(cid, &PinMode::Indirect);

                    // cid is used by other block
                    match self.get_u8(indirect_key.as_bytes()) {
                        Ok(Some(value)) => {
                            let value = String::from_utf8_lossy(value.as_slice());

                            if cid_str == value {
                                batch.delete_u8(get_pin_key(cid, &PinMode::Indirect).as_bytes());
                            }
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
        }

        match self.write_batch(&batch) {
            Ok(_) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn list(&self, expected_mode: Option<PinMode>) -> futures::stream::BoxStream<'static, Result<(Cid, PinMode), Error>> {
        let iter = self.key_iter();

        let mut all_keys = vec![];

        for item in iter {
            all_keys.push(item);
        }

        let st = async_stream::try_stream! {
            for raw_key in all_keys.iter() {
                let key = String::from_utf8_lossy(raw_key.as_slice());

                if key.starts_with("pin.") {
                    let cid_str_with_prefix = &key[4..];

                    let (cid_str, pin_mode) = match cid_str_with_prefix {
                        direct_str if cid_str_with_prefix.starts_with("direct") => {
                            (&cid_str_with_prefix["direct".len() + 1..], PinMode::Direct)
                        },

                        recursive_str if cid_str_with_prefix.starts_with("recursive") => {
                            (&cid_str_with_prefix["recursive".len() + 1..], PinMode::Recursive)
                        }

                        indirect_str if cid_str_with_prefix.starts_with("indirect") => {
                            (&cid_str_with_prefix["indirect".len() + 1..], PinMode::Indirect)
                        }

                        _ =>  continue,
                    };

                    match Cid::from_str(cid_str) {
                        Ok(cid) =>  {
                            match expected_mode {
                                Some(ref expected) => if pin_mode == *expected {
                                    yield (cid, pin_mode);
                                }
                                _ => yield (cid, pin_mode),
                            }
                        }
                        Err(_) => {}
                    }
                }
           }
        };

        st.in_current_span().boxed()
    }


    async fn query(&self, ids: Vec<Cid>, requirement: Option<PinMode>) -> Result<Vec<(Cid, PinKind<Cid>)>, Error> {
        let mut res = Vec::<(Cid, PinKind<Cid>)>::new();

        let pin_mode_matches = |cid: &Cid, pin_mode: &PinMode| {
            match requirement {
                Some(ref expected) => *expected == *pin_mode,
                None => true
            }
        };

        for id in ids.iter() {
            match get_pinned_mode(self, id) {
                Ok(Some(pin_mode)) => {
                    if !pin_mode_matches(id, &pin_mode) {
                        continue
                    }

                    match pin_mode {
                        PinMode::Direct => res.push((id.clone(), PinKind::Direct)),
                        PinMode::Recursive => res.push((id.clone(), PinKind::Recursive(0))),
                        PinMode::Indirect => {
                            let pin_key = get_pin_key(id, &PinMode::Indirect);

                            match self.get_u8(pin_key.as_bytes()) {
                                Ok(Some(indirect_from_raw)) => {
                                    let indirect_from_str = String::from_utf8_lossy(indirect_from_raw.as_slice());

                                    match Cid::from_str(&indirect_from_str) {
                                        Ok(indirect_from_cid) =>
                                            res.push((id.clone(), PinKind::IndirectFrom(indirect_from_cid))),
                                        _ => {
                                            warn!("invalid indirect from cid of {}", id);
                                            continue
                                        }
                                    }
                                }
                                Ok(_) => {}
                                Err(e) => return Err(e.into())
                            }
                        }
                    }
                }
                Ok(_) => {}
                Err(e) => return Err(e.into())
            }
        }

        Ok(res)
    }
}


fn pin_mode_literal(pin_mode: &PinMode) -> &'static str {
    match pin_mode {
        PinMode::Direct => "direct",
        PinMode::Indirect => "indirect",
        PinMode::Recursive => "recursive",
    }
}

fn get_pin_key(cid: &Cid, pin_mode: &PinMode) -> String {
    format!("pin.{}.{}", pin_mode_literal(pin_mode), cid.to_string())
}

fn get_pinned_mode(database: &LeveldbStore, block: &Cid) -> Result<Option<PinMode>, Error> {
    for mode in &[PinMode::Direct, PinMode::Recursive, PinMode::Indirect] {
        let key =  get_pin_key(block, mode);

        match database.get_u8(key.as_bytes()) {
            Ok(Some(_)) => return Ok(Some(mode.clone())),
            Ok(_) => {}
            Err(e) => return Err(e.into())
        }
    }

    Ok(None)
}

fn is_pinned(database: &LeveldbStore, block: &Cid) -> Result<bool, Error> {
    match get_pinned_mode(database, block) {
        Ok(Some(_)) => return Ok(true),
        Ok(_) => return Ok(false),
        Err(e) => Err(e.into())
    }
}