//! IPFS repo
use crate::error::Error;
use async_std::path::PathBuf;
use async_std::stream::Stream;
use async_trait::async_trait;
use core::fmt::Debug;
use libipld::cid::Cid;

pub mod fs;
pub mod mem;

#[derive(Clone, Debug)]
pub enum BlockStoreEvent {
    Get(Cid, Result<Option<Box<[u8]>>, Error>),
    Put(Cid, Result<(), Error>),
    Remove(Cid, Result<(), Error>),
}

impl PartialEq for BlockStoreEvent {
    fn eq(&self, other: &Self) -> bool {
        use BlockStoreEvent::*;
        match (self, other) {
            (Get(cid1, Ok(data1)), Get(cid2, Ok(data2))) => cid1 == cid2 && data1 == data2,
            (Put(cid1, Ok(())), Put(cid2, Ok(()))) => cid1 == cid2,
            (Remove(cid1, Ok(())), Remove(cid2, Ok(()))) => cid1 == cid2,
            (Get(cid1, Err(_)), Get(cid2, Err(_))) => cid1 == cid2,
            (Put(cid1, Err(_)), Put(cid2, Err(_))) => cid1 == cid2,
            (Remove(cid1, Err(_)), Remove(cid2, Err(_))) => cid1 == cid2,
            _ => false,
        }
    }
}

#[async_trait]
pub trait BlockStore:
    Debug + Send + Sized + Stream<Item = BlockStoreEvent> + Unpin + 'static
{
    async fn open(path: PathBuf) -> Result<Self, Error>;
    fn contains(&mut self, cid: &Cid) -> bool;
    fn get(&mut self, cid: Cid);
    fn put(&mut self, cid: Cid, data: Box<[u8]>);
    fn remove(&mut self, cid: Cid);
}

#[async_trait]
pub trait DataStore: Debug + Clone + Send + Sync + Unpin + 'static {
    fn new(path: PathBuf) -> Self;
    async fn init(&self) -> Result<(), Error>;
    async fn open(&self) -> Result<(), Error>;
    async fn contains(&self, col: Column, key: &[u8]) -> Result<bool, Error>;
    async fn get(&self, col: Column, key: &[u8]) -> Result<Option<Vec<u8>>, Error>;
    async fn put(&self, col: Column, key: &[u8], value: &[u8]) -> Result<(), Error>;
    async fn remove(&self, col: Column, key: &[u8]) -> Result<(), Error>;
}

#[derive(Clone, Copy, Debug)]
pub enum Column {
    Ipns,
}
