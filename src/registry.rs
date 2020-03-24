use crate::error::Error;
use crate::p2p::TSwarm;
use bitswap::Block;
use core::fmt::Debug;
use core::hash::Hash;
use futures::channel::oneshot::Sender;
use libipld::cid::Cid;
use libp2p::PeerId;
use std::collections::HashMap;

pub type Channel<T> = Sender<Result<T, Error>>;

#[derive(Debug)]
pub struct LocalRegistry<K: Debug + Eq + Hash, V: Clone + Debug> {
    registry: HashMap<K, Vec<Channel<V>>>,
}

impl<K: Debug + Eq + Hash, V: Clone + Debug> Default for LocalRegistry<K, V> {
    fn default() -> Self {
        Self {
            registry: Default::default(),
        }
    }
}

impl<K: Debug + Eq + Hash, V: Clone + Debug> LocalRegistry<K, V> {
    pub fn register(&mut self, key: K, channel: Channel<V>) {
        self.registry.entry(key).or_default().push(channel);
    }

    pub fn consume(&mut self, key: &K, value: &Result<V, Error>) {
        if let Some(channels) = self.registry.remove(key) {
            for channel in channels.into_iter() {
                channel.send(value.to_owned()).ok();
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct RemoteRegistry {
    registry: HashMap<Cid, Vec<PeerId>>,
}

impl RemoteRegistry {
    pub fn register(&mut self, cid: Cid, peer_id: PeerId) {
        self.registry.entry(cid).or_default().push(peer_id);
    }

    pub fn consume(&mut self, swarm: &mut TSwarm, block: Block) {
        if let Some(peers) = self.registry.remove(block.cid()) {
            for peer in peers.into_iter() {
                swarm.send_block(&peer, block.clone());
            }
        }
    }
}
