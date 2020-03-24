use crate::error::Error;
use crate::options::{IpfsOptions, IpfsTypes};
use crate::p2p::Connection;
use crate::p2p::{create_swarm, BehaviourEvent, TSwarm};
use crate::registry::{Channel, LocalRegistry, RemoteRegistry};
use crate::repo::{BlockStore, BlockStoreEvent};
use async_std::task;
use async_trait::async_trait;
use bitswap::Block;
use core::fmt::Debug;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use futures::channel::{mpsc, oneshot};
use futures::sink::SinkExt;
use futures::stream::{Fuse, StreamExt};
use libipld::cid::Cid;
use libipld::error::Result as StoreResult;
use libipld::store::Store;
use libp2p::core::{Multiaddr, PeerId, PublicKey};
use std::path::Path;
use std::sync::Arc;

/// Events used internally to communicate with the swarm, which is executed in the the background
/// task.
#[derive(Debug)]
pub enum IpfsEvent {
    /// Connect
    Connect(Multiaddr, Channel<()>),
    /// Addresses
    Addresses(Channel<Vec<(PeerId, Vec<Multiaddr>)>>),
    /// Local addresses
    Listeners(Channel<Vec<Multiaddr>>),
    /// Connections
    Connections(Channel<Vec<Connection>>),
    /// Disconnect
    Disconnect(Multiaddr, Channel<()>),
    /// Request background task to return the listened and external addresses
    Identity(Channel<(PublicKey, Vec<Multiaddr>)>),
    /// Returns the block with cid.
    GetBlock(Cid, Channel<Box<[u8]>>),
    /// Writes the block with cid.
    PutBlock(Cid, Box<[u8]>, Channel<()>),
    /// Removes the block with cid.
    RemoveBlock(Cid, Channel<()>),
    /// Exit the ipfs background process.
    Exit,
}

/// Ipfs struct creates a new IPFS node and is the main entry point
/// for interacting with IPFS.
#[derive(Clone, Debug)]
pub struct Ipfs {
    sender: mpsc::Sender<IpfsEvent>,
    exit_signal: Arc<ExitSignal>,
}

impl Ipfs {
    pub async fn new<T: IpfsTypes>(options: IpfsOptions) -> Result<Self, Error> {
        let (tx, rx) = mpsc::channel(1);
        let daemon = IpfsDaemon::<T>::new(options, rx).await?;
        task::spawn(daemon);
        let exit_signal = Arc::new(ExitSignal { sender: tx.clone() });
        Ok(Self {
            sender: tx,
            exit_signal,
        })
    }

    pub async fn connect(&self, addr: Multiaddr) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .clone()
            .send(IpfsEvent::Connect(addr, tx))
            .await?;
        rx.await?
    }

    pub async fn addrs(&self) -> Result<Vec<(PeerId, Vec<Multiaddr>)>, Error> {
        let (tx, rx) = oneshot::channel();
        self.sender.clone().send(IpfsEvent::Addresses(tx)).await?;
        rx.await?
    }

    pub async fn addrs_local(&self) -> Result<Vec<Multiaddr>, Error> {
        let (tx, rx) = oneshot::channel();
        self.sender.clone().send(IpfsEvent::Listeners(tx)).await?;
        rx.await?
    }

    pub async fn peers(&self) -> Result<Vec<Connection>, Error> {
        let (tx, rx) = oneshot::channel();
        self.sender.clone().send(IpfsEvent::Connections(tx)).await?;
        rx.await?
    }

    pub async fn disconnect(&self, addr: Multiaddr) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .clone()
            .send(IpfsEvent::Disconnect(addr, tx))
            .await?;
        rx.await?
    }

    pub async fn identity(&self) -> Result<(PublicKey, Vec<Multiaddr>), Error> {
        let (tx, rx) = oneshot::channel();
        self.sender.clone().send(IpfsEvent::Identity(tx)).await?;
        rx.await?
    }

    pub async fn get_block(&self, cid: Cid) -> Result<Box<[u8]>, Error> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .clone()
            .send(IpfsEvent::GetBlock(cid, tx))
            .await?;
        rx.await?
    }

    pub async fn put_block(&self, cid: Cid, data: Box<[u8]>) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .clone()
            .send(IpfsEvent::PutBlock(cid, data, tx))
            .await?;
        rx.await?
    }

    pub async fn remove_block(&self, cid: Cid) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .clone()
            .send(IpfsEvent::RemoveBlock(cid, tx))
            .await?;
        rx.await?
    }
}

#[async_trait]
impl Store for Ipfs {
    async fn read(&self, cid: &Cid) -> StoreResult<Option<Box<[u8]>>> {
        Ok(Some(self.get_block(cid.clone()).await?))
    }

    async fn write(&self, cid: &Cid, data: Box<[u8]>) -> StoreResult<()> {
        Ok(self.put_block(cid.clone(), data).await?)
    }

    async fn flush(&self) -> StoreResult<()> {
        Ok(())
    }

    async fn gc(&self) -> StoreResult<()> {
        Ok(())
    }

    async fn pin(&self, _cid: &Cid) -> StoreResult<()> {
        Ok(())
    }

    async fn unpin(&self, _cid: &Cid) -> StoreResult<()> {
        Ok(())
    }

    async fn autopin(&self, _cid: &Cid, _auto_path: &Path) -> StoreResult<()> {
        Ok(())
    }

    async fn write_link(&self, _label: &str, _cid: &Cid) -> StoreResult<()> {
        Ok(())
    }

    async fn read_link(&self, _label: &str) -> StoreResult<Option<Cid>> {
        Ok(None)
    }

    async fn remove_link(&self, _label: &str) -> StoreResult<()> {
        Ok(())
    }
}

#[derive(Debug)]
struct ExitSignal {
    sender: mpsc::Sender<IpfsEvent>,
}

impl Drop for ExitSignal {
    fn drop(&mut self) {
        let mut sender = self.sender.clone();
        task::spawn(async move {
            sender.send(IpfsEvent::Exit).await.ok();
        });
    }
}

/// Background task of `Ipfs` created when calling `UninitializedIpfs::start`.
// The receivers are Fuse'd so that we don't have to manage state on them being exhausted.
struct IpfsDaemon<T: IpfsTypes> {
    options: IpfsOptions,
    swarm: TSwarm,
    block_store: T::TBlockStore,
    read_registry: LocalRegistry<Cid, Box<[u8]>>,
    write_registry: LocalRegistry<Cid, ()>,
    remove_registry: LocalRegistry<Cid, ()>,
    bitswap_registry: RemoteRegistry,
    from_facade: Fuse<mpsc::Receiver<IpfsEvent>>,
}

impl<T: IpfsTypes> IpfsDaemon<T> {
    pub async fn new(
        options: IpfsOptions,
        receiver: mpsc::Receiver<IpfsEvent>,
    ) -> Result<Self, Error> {
        let swarm = create_swarm(&options);
        let block_store = T::TBlockStore::open(options.ipfs_path.join("blockstore")).await?;
        Ok(Self {
            options,
            swarm,
            block_store,
            read_registry: Default::default(),
            write_registry: Default::default(),
            remove_registry: Default::default(),
            bitswap_registry: Default::default(),
            from_facade: receiver.fuse(),
        })
    }
}

impl<T: IpfsTypes> Future for IpfsDaemon<T> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        use futures::Stream;
        use libp2p::{swarm::SwarmEvent, Swarm};

        // begin by polling the swarm so that initially it'll first have chance to bind listeners
        // and such. TODO: this no longer needs to be a swarm event but perhaps we should
        // consolidate logging of these events here, if necessary?
        loop {
            let inner = {
                let next = self.swarm.next_event();
                futures::pin_mut!(next);
                match next.poll(ctx) {
                    Poll::Ready(inner) => inner,
                    Poll::Pending => break,
                }
            };
            if let SwarmEvent::Behaviour(event) = inner {
                match event {
                    BehaviourEvent::ReceivedBlock(_peer_id, block) => {
                        let cid = block.cid().clone();
                        let data = block.data().to_vec().into_boxed_slice();
                        self.read_registry.consume(&cid, &Ok(data.clone()));
                        // Safe to do as struct fields are disjoint.
                        let bitswap_registry: &mut RemoteRegistry =
                            unsafe { &mut *(&mut self.bitswap_registry as *mut _) };
                        bitswap_registry.consume(&mut self.swarm, block);
                        self.block_store.put(cid, data);
                    }
                    BehaviourEvent::ReceivedWant(peer_id, cid) => {
                        self.bitswap_registry.register(cid.clone(), peer_id);
                        self.block_store.get(cid);
                    }
                }
            }
        }

        loop {
            let next = self.block_store.next();
            futures::pin_mut!(next);
            match next.poll(ctx) {
                Poll::Ready(Some(res)) => {
                    match res {
                        BlockStoreEvent::Get(cid, res) => {
                            let res = match res {
                                Ok(Some(data)) => Some(Ok(data)),
                                Ok(None) => None,
                                Err(err) => Some(Err(err)),
                            };
                            if let Some(res) = res {
                                self.read_registry.consume(&cid, &res);
                                if let Ok(data) = res {
                                    // Safe to do as struct fields are disjoint.
                                    let bitswap_registry: &mut RemoteRegistry =
                                        unsafe { &mut *(&mut self.bitswap_registry as *mut _) };
                                    let block = Block::new(data, cid);
                                    bitswap_registry.consume(&mut self.swarm, block);
                                }
                            } else {
                                self.swarm.want_block(cid);
                            }
                        }
                        BlockStoreEvent::Put(cid, res) => {
                            self.write_registry.consume(&cid, &res);
                            self.swarm.provide_block(cid);
                        }
                        BlockStoreEvent::Remove(cid, res) => {
                            self.remove_registry.consume(&cid, &res);
                            self.swarm.stop_providing_block(&cid);
                        }
                    }
                }
                Poll::Ready(None) => unreachable!(),
                Poll::Pending => break,
            }
        }

        // temporary pinning of the receivers should be safe as we are pinning through the
        // already pinned self. with the receivers we can also safely ignore exhaustion
        // as those are fused.
        loop {
            let inner = match Pin::new(&mut self.from_facade).poll_next(ctx) {
                Poll::Ready(Some(evt)) => evt,
                // doing teardown also after the `Ipfs` has been dropped
                Poll::Ready(None) => IpfsEvent::Exit,
                Poll::Pending => break,
            };

            match inner {
                IpfsEvent::Connect(addr, ret) => {
                    self.swarm.connect(addr, ret);
                }
                IpfsEvent::Addresses(ret) => {
                    let addrs = self.swarm.addrs();
                    ret.send(Ok(addrs)).ok();
                }
                IpfsEvent::Listeners(ret) => {
                    let listeners = Swarm::listeners(&self.swarm).cloned().collect();
                    ret.send(Ok(listeners)).ok();
                }
                IpfsEvent::Connections(ret) => {
                    let connections = self.swarm.connections();
                    ret.send(Ok(connections)).ok();
                }
                IpfsEvent::Disconnect(addr, ret) => {
                    if let Some(disconnector) = self.swarm.disconnect(addr) {
                        disconnector.disconnect(&mut self.swarm);
                    }
                    ret.send(Ok(())).ok();
                }
                IpfsEvent::GetBlock(cid, ret) => {
                    self.read_registry.register(cid.clone(), ret);
                    self.block_store.get(cid);
                }
                IpfsEvent::PutBlock(cid, data, ret) => {
                    self.write_registry.register(cid.clone(), ret);
                    self.block_store.put(cid, data);
                }
                IpfsEvent::RemoveBlock(cid, ret) => {
                    self.remove_registry.register(cid.clone(), ret);
                    self.block_store.remove(cid);
                }
                IpfsEvent::Identity(ret) => {
                    // perhaps this could be moved under `IpfsEvent` or free functions?
                    let mut addresses = Vec::new();
                    addresses.extend(Swarm::listeners(&self.swarm).cloned());
                    addresses.extend(Swarm::external_addresses(&self.swarm).cloned());
                    // ignore error, perhaps caller went away already
                    ret.send(Ok((self.options.keypair.public(), addresses)))
                        .ok();
                }
                IpfsEvent::Exit => {
                    // FIXME: we could do a proper teardown
                    return Poll::Ready(());
                }
            }
        }

        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::options::TestTypes;
    use libipld::cid::Codec;
    use multihash::Sha2_256;

    #[async_std::test]
    async fn test_put_and_get_block() {
        let options = IpfsOptions::inmemory_with_generated_keys(true);
        let ipfs = Ipfs::new::<TestTypes>(options).await.unwrap();

        let data = b"hello block\n".to_vec().into_boxed_slice();
        let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&data));

        ipfs.put_block(cid.clone(), data.clone()).await.unwrap();
        let new_data = ipfs.get_block(cid).await.unwrap();
        assert_eq!(data, new_data);
    }

    /*#[async_std::test]
    async fn test_put_and_get_dag() {
        let options = IpfsOptions::inmemory_with_generated_keys(true);
        let ipfs = Ipfs::new::<TestTypes>(options).await.unwrap();

        let data = ipld!([-1, -2, -3]);
        let cid = ipfs.put_dag(data.clone()).await.unwrap();
        let new_data = ipfs.get_dag(cid.into()).await.unwrap();
        assert_eq!(data, new_data);
    }*/
}
