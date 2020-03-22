//! IPFS node implementation
//#![deny(missing_docs)]

#![cfg_attr(feature = "nightly", feature(external_doc))]
#![cfg_attr(feature = "nightly", doc(include = "../README.md"))]

#[macro_use]
extern crate log;

use anyhow::format_err;
use async_std::path::PathBuf;
pub use bitswap::Block;
use futures::channel::mpsc::{channel, Receiver, Sender};
use futures::channel::oneshot::{channel as oneshot_channel, Sender as OneshotSender};
use futures::sink::SinkExt;
use futures::stream::Fuse;
pub use libipld::cid::Cid;
use libipld::cid::Codec;
pub use libipld::ipld::Ipld;
pub use libp2p::core::{ConnectedPoint, Multiaddr, PeerId, PublicKey};
pub use libp2p::identity::Keypair;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

mod config;
mod dag;
mod error;
mod ipns;
mod options;
mod p2p;
mod path;
mod repo;
mod subscription;
mod unixfs;

pub use crate::options::{IpfsOptions, IpfsTypes, TestTypes, Types};

use self::dag::IpldDag;
pub use self::error::Error;
use self::ipns::Ipns;
use self::options::DebuggableKeypair;
pub use self::p2p::Connection;
use self::p2p::{create_swarm, TSwarm};
pub use self::path::IpfsPath;
use self::repo::Repo;
use self::subscription::SubscriptionFuture;
use self::unixfs::File;

/// Ipfs struct creates a new IPFS node and is the main entry point
/// for interacting with IPFS.
#[derive(Clone, Debug)]
pub struct Ipfs<Types: IpfsTypes> {
    repo: Arc<Repo<Types>>,
    dag: IpldDag<Types>,
    ipns: Ipns<Types>,
    keys: DebuggableKeypair<Keypair>,
    to_task: Sender<IpfsEvent>,
}

type Channel<T> = OneshotSender<Result<T, Error>>;

/// Events used internally to communicate with the swarm, which is executed in the the background
/// task.
#[derive(Debug)]
pub(crate) enum IpfsEvent {
    /// Connect
    Connect(
        Multiaddr,
        OneshotSender<SubscriptionFuture<Result<(), String>>>,
    ),
    /// Addresses
    Addresses(Channel<Vec<(PeerId, Vec<Multiaddr>)>>),
    /// Local addresses
    Listeners(Channel<Vec<Multiaddr>>),
    /// Connections
    Connections(Channel<Vec<Connection>>),
    /// Disconnect
    Disconnect(Multiaddr, Channel<()>),
    /// Request background task to return the listened and external addresses
    GetAddresses(OneshotSender<Vec<Multiaddr>>),
    WantBlock(Cid),
    ProvideBlock(Cid),
    UnprovideBlock(Cid),
    Exit,
}

/// Configured Ipfs instace or value which can be only initialized.
pub struct UninitializedIpfs<T: IpfsTypes> {
    repo: Arc<Repo<T>>,
    dag: IpldDag<T>,
    ipns: Ipns<T>,
    keys: Keypair,
    moved_on_init: Option<(Sender<IpfsEvent>, Receiver<IpfsEvent>, TSwarm<T>)>,
}

impl<T: IpfsTypes> UninitializedIpfs<T> {
    /// Configures a new UninitializedIpfs with from the given options.
    pub async fn new(options: IpfsOptions) -> Self {
        let keys = options.keypair.clone();
        let (tx, rx) = channel::<IpfsEvent>(1);
        let repo = Arc::new(Repo::new(&options, tx.clone()));
        let swarm = create_swarm(&options, repo.clone()).await;
        let dag = IpldDag::new(repo.clone());
        let ipns = Ipns::new(repo.clone());

        UninitializedIpfs {
            repo,
            dag,
            ipns,
            keys,
            moved_on_init: Some((tx, rx, swarm)),
        }
    }

    /// Initialize the ipfs node. The returned `Ipfs` value is cloneable, send and sync, and the
    /// future should be spawned on a executor as soon as possible.
    pub async fn start(
        mut self,
    ) -> Result<(Ipfs<T>, impl std::future::Future<Output = ()>), Error> {
        use futures::stream::StreamExt;

        let (tx, rx, swarm) = self
            .moved_on_init
            .take()
            .expect("start cannot be called twice");

        self.repo.init().await?;
        self.repo.init().await?;

        let fut = IpfsFuture {
            from_facade: rx.fuse(),
            swarm,
        };

        let UninitializedIpfs {
            repo,
            dag,
            ipns,
            keys,
            ..
        } = self;

        Ok((
            Ipfs {
                repo,
                dag,
                ipns,
                keys: DebuggableKeypair(keys),
                to_task: tx,
            },
            fut,
        ))
    }
}

impl<T: IpfsTypes> Ipfs<T> {
    /// Puts a block into the ipfs repo.
    pub async fn put_block(&mut self, block: Block) -> Result<Cid, Error> {
        Ok(self.repo.put_block(block).await?)
    }

    /// Retrives a block from the ipfs repo.
    pub async fn get_block(&mut self, cid: &Cid) -> Result<Block, Error> {
        Ok(self.repo.get_block(cid).await?)
    }

    /// Remove block from the ipfs repo.
    pub async fn remove_block(&mut self, cid: &Cid) -> Result<(), Error> {
        Ok(self.repo.remove_block(cid).await?)
    }

    /// Puts an ipld dag node into the ipfs repo.
    pub async fn put_dag(&self, ipld: Ipld) -> Result<Cid, Error> {
        Ok(self.dag.put(ipld, Codec::DagCBOR).await?)
    }

    /// Gets an ipld dag node from the ipfs repo.
    pub async fn get_dag(&self, path: IpfsPath) -> Result<Ipld, Error> {
        Ok(self.dag.get(path).await?)
    }

    /// Adds a file into the ipfs repo.
    pub async fn add(&self, path: PathBuf) -> Result<Cid, Error> {
        let dag = self.dag.clone();
        let file = File::new(path).await?;
        let path = file.put_unixfs_v1(&dag).await?;
        Ok(path)
    }

    /// Gets a file from the ipfs repo.
    pub async fn get(&self, path: IpfsPath) -> Result<File, Error> {
        Ok(File::get_unixfs_v1(&self.dag, path).await?)
    }

    /// Resolves a ipns path to an ipld path.
    pub async fn resolve_ipns(&self, path: &IpfsPath) -> Result<IpfsPath, Error> {
        Ok(self.ipns.resolve(path).await?)
    }

    /// Publishes an ipld path.
    pub async fn publish_ipns(&self, key: &PeerId, path: &IpfsPath) -> Result<IpfsPath, Error> {
        Ok(self.ipns.publish(key, path).await?)
    }

    /// Cancel an ipns path.
    pub async fn cancel_ipns(&self, key: &PeerId) -> Result<(), Error> {
        self.ipns.cancel(key).await?;
        Ok(())
    }

    pub async fn connect(&self, addr: Multiaddr) -> Result<(), Error> {
        let (tx, rx) = oneshot_channel();
        self.to_task
            .clone()
            .send(IpfsEvent::Connect(addr, tx))
            .await?;
        let subscription = rx.await?;
        subscription.await.map_err(|e| format_err!("{}", e))
    }

    pub async fn addrs(&self) -> Result<Vec<(PeerId, Vec<Multiaddr>)>, Error> {
        let (tx, rx) = oneshot_channel();
        self.to_task.clone().send(IpfsEvent::Addresses(tx)).await?;
        rx.await?
    }

    pub async fn addrs_local(&self) -> Result<Vec<Multiaddr>, Error> {
        let (tx, rx) = oneshot_channel();
        self.to_task.clone().send(IpfsEvent::Listeners(tx)).await?;
        rx.await?
    }

    pub async fn peers(&self) -> Result<Vec<Connection>, Error> {
        let (tx, rx) = oneshot_channel();
        self.to_task
            .clone()
            .send(IpfsEvent::Connections(tx))
            .await?;
        rx.await?
    }

    pub async fn disconnect(&self, addr: Multiaddr) -> Result<(), Error> {
        let (tx, rx) = oneshot_channel();
        self.to_task
            .clone()
            .send(IpfsEvent::Disconnect(addr, tx))
            .await?;
        rx.await?
    }

    pub async fn identity(&self) -> Result<(PublicKey, Vec<Multiaddr>), Error> {
        let (tx, rx) = oneshot_channel();

        self.to_task
            .clone()
            .send(IpfsEvent::GetAddresses(tx))
            .await?;
        let addresses = rx.await?;
        Ok((self.keys.as_ref().public(), addresses))
    }

    /// Exit daemon.
    pub async fn exit_daemon(mut self) {
        // ignoring the error because it'd mean that the background task would had already been
        // dropped
        let _ = self.to_task.send(IpfsEvent::Exit).await;
    }
}

/// Background task of `Ipfs` created when calling `UninitializedIpfs::start`.
// The receivers are Fuse'd so that we don't have to manage state on them being exhausted.
struct IpfsFuture<T: IpfsTypes> {
    swarm: TSwarm<T>,
    from_facade: Fuse<Receiver<IpfsEvent>>,
}

impl<T: IpfsTypes> Future for IpfsFuture<T> {
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
            match inner {
                SwarmEvent::Behaviour(()) => {}
                SwarmEvent::Connected(_peer_id) => {}
                SwarmEvent::Disconnected(_peer_id) => {}
                SwarmEvent::NewListenAddr(_addr) => {}
                SwarmEvent::ExpiredListenAddr(_addr) => {}
                SwarmEvent::UnreachableAddr {
                    peer_id: _peer_id,
                    address: _address,
                    error: _error,
                } => {}
                SwarmEvent::StartConnect(_peer_id) => {}
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
                    ret.send(self.swarm.connect(addr)).ok();
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
                IpfsEvent::WantBlock(cid) => self.swarm.want_block(cid),
                IpfsEvent::ProvideBlock(cid) => self.swarm.provide_block(cid),
                IpfsEvent::UnprovideBlock(cid) => self.swarm.stop_providing_block(&cid),
                IpfsEvent::GetAddresses(ret) => {
                    // perhaps this could be moved under `IpfsEvent` or free functions?
                    let mut addresses = Vec::new();
                    addresses.extend(Swarm::listeners(&self.swarm).cloned());
                    addresses.extend(Swarm::external_addresses(&self.swarm).cloned());
                    // ignore error, perhaps caller went away already
                    let _ = ret.send(addresses);
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
    use async_std::task;
    use libipld::ipld;
    use multihash::Sha2_256;

    #[async_std::test]
    async fn test_put_and_get_block() {
        let options = IpfsOptions::<TestTypes>::default();
        let data = b"hello block\n".to_vec().into_boxed_slice();
        let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&data));
        let block = Block::new(data, cid);
        let ipfs = UninitializedIpfs::new(options).await;
        let (mut ipfs, fut) = ipfs.start().await.unwrap();
        task::spawn(fut);

        let cid: Cid = ipfs.put_block(block.clone()).await.unwrap();
        let new_block = ipfs.get_block(&cid).await.unwrap();
        assert_eq!(block, new_block);

        ipfs.exit_daemon().await;
    }

    #[async_std::test]
    async fn test_put_and_get_dag() {
        let options = IpfsOptions::<TestTypes>::default();

        let (ipfs, fut) = UninitializedIpfs::new(options).await.start().await.unwrap();
        task::spawn(fut);

        let data = ipld!([-1, -2, -3]);
        let cid = ipfs.put_dag(data.clone()).await.unwrap();
        let new_data = ipfs.get_dag(cid.into()).await.unwrap();
        assert_eq!(data, new_data);

        ipfs.exit_daemon().await;
    }
}
