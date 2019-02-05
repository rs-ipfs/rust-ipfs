use crate::bitswap::{Bitswap, BitswapEvent};
use crate::block::{Block, Cid};
use crate::config::NetworkConfig;
use libp2p::{NetworkBehaviour, PeerId};
use libp2p::core::swarm::NetworkBehaviourEventProcess;
use libp2p::core::muxing::{StreamMuxerBox, SubstreamRef};
use libp2p::kad::{Kademlia, KademliaOut as KademliaEvent};
use parity_multihash::Multihash;
use std::sync::Arc;
use tokio::prelude::*;

/// Behaviour type.
#[derive(NetworkBehaviour)]
pub struct Behaviour<TSubstream: AsyncRead + AsyncWrite> {
    kademlia: Kademlia<TSubstream>,
    bitswap: Bitswap<TSubstream>,
}

impl<TSubstream: AsyncRead + AsyncWrite>
    NetworkBehaviourEventProcess<KademliaEvent> for
    Behaviour<TSubstream>
{
    fn inject_event(&mut self, event: KademliaEvent) {
        match event {
            KademliaEvent::Discovered { .. } => {

            }
            KademliaEvent::FindNodeResult { .. } => {

            }
            KademliaEvent::GetProvidersResult {
                key,
                provider_peers,
                ..
            } => {
                println!("key: {}", PeerId::from_multihash(key).unwrap().to_base58());
                for peer in provider_peers {
                    println!("provided by: {}", peer.to_base58());
                    self.bitswap.connect(peer);
                }
            }
        }
    }
}

impl<TSubstream: AsyncRead + AsyncWrite>
    NetworkBehaviourEventProcess<BitswapEvent> for
    Behaviour<TSubstream>
{
    fn inject_event(&mut self, event: BitswapEvent) {
        match event {
            BitswapEvent::Block { block } => {
                println!("Received block with contents: '{:?}'",
                         String::from_utf8_lossy(&block.data()));
            }
            BitswapEvent::Want { peer_id, cid, priority } => {
                println!("Peer {:?} wants block {:?} with priority {}",
                         peer_id, cid.to_string(), priority);
            }
        }
    }
}

impl<TSubstream: AsyncRead + AsyncWrite> Behaviour<TSubstream>
{
    /// Create a Kademlia behaviour with the IPFS bootstrap nodes.
    pub fn new(config: &NetworkConfig) -> Self {
        println!("Local peer id: {}", config.peer_id.to_base58());

        let mut kademlia = Kademlia::new(config.peer_id.to_owned());

        for (addr, peer_id) in &config.bootstrap {
            kademlia.add_address(peer_id, addr.to_owned());
        }

        let bitswap = Bitswap::new();

        Behaviour {
            kademlia,
            bitswap,
        }
    }

    pub fn send_block(&mut self, peer_id: PeerId, block: Block) {
        self.bitswap.send_block(peer_id, block);
    }

    pub fn want_block(&mut self, cid: Cid) {
        let hash = Multihash::from_bytes(cid.hash.clone()).unwrap();
        self.kademlia.get_providers(hash);
        self.bitswap.want_block(cid, 1);
    }

    pub fn provide_block(&mut self, cid: &Cid) {
        let hash = Multihash::from_bytes(cid.hash.clone()).unwrap();
        self.kademlia.add_providing(PeerId::from_multihash(hash).unwrap());
    }

    pub fn stop_providing_block(&mut self, cid: &Cid) {
        let hash = Multihash::from_bytes(cid.hash.clone()).unwrap();
        self.kademlia.remove_providing(&hash);
    }
}

/// Behaviour type.
pub type TBehaviour = Behaviour<SubstreamRef<Arc<StreamMuxerBox>>>;

/// Create a IPFS behaviour with the IPFS bootstrap nodes.
pub fn build_behaviour(config: &NetworkConfig) -> TBehaviour {
    Behaviour::new(config)
}
