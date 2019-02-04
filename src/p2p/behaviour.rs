use crate::bitswap::{Bitswap, BitswapEvent};
use crate::block::{Block, Cid};
use libp2p::{NetworkBehaviour, PeerId};
use libp2p::core::swarm::NetworkBehaviourEventProcess;
use libp2p::core::muxing::{StreamMuxerBox, SubstreamRef};
use libp2p::kad::{Kademlia, KademliaOut as KademliaEvent};
use parity_multihash::Multihash;
use std::sync::Arc;
use tokio::prelude::*;

/// IPFS bootstrap nodes.
const BOOTSTRAP_NODES: &[(&'static str, &'static str)] = &[
    (
        "QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
        "/ip4/104.131.131.82/tcp/4001",
    ),
    (
        "QmSoLPppuBtQSGwKDZT2M73ULpjvfd3aZ6ha4oFGL1KrGM",
        "/ip4/104.236.179.241/tcp/4001",
    ),
    (
        "QmSoLV4Bbm51jM9C4gDYZQ9Cy3U6aXMJDAbzgu2fzaDs64",
        "/ip4/104.236.76.40/tcp/4001",
    ),
    (
        "QmSoLSafTMBsPKadTEgaXctDQVcqN88CNLHXMkTNwMKPnu",
        "/ip4/128.199.219.111/tcp/4001",
    ),
    (
        "QmSoLer265NRgSp2LA3dPaeykiS1J6DifTC88f5uVQKNAd",
        "/ip4/178.62.158.247/tcp/4001",
    ),
    /*(
        "QmSoLSafTMBsPKadTEgaXctDQVcqN88CNLHXMkTNwMKPnu",
        "/ip6/2400:6180:0:d0::151:6001/tcp/4001",
    ),
    (
        "QmSoLPppuBtQSGwKDZT2M73ULpjvfd3aZ6ha4oFGL1KrGM",
        "/ip6/2604:a880:1:20::203:d001/tcp/4001",
    ),
    (
        "QmSoLV4Bbm51jM9C4gDYZQ9Cy3U6aXMJDAbzgu2fzaDs64",
        "/ip6/2604:a880:800:10::4a:5001/tcp/4001",
    ),
    (
        "QmSoLer265NRgSp2LA3dPaeykiS1J6DifTC88f5uVQKNAd",
        "/ip6/2a03:b0c0:0:1010::23:1001/tcp/4001",
    ),*/
];

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
                provider_peers,
                ..
            } => {
                for peer in provider_peers {
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
    pub fn new(local_peer_id: PeerId) -> Self {
        // Note that normally the Kademlia process starts by performing lots of
        // request in order to insert our local node in the DHT. However here we use
        // `without_init` because this example is very ephemeral and we don't want
        // to pollute the DHT. In a real world application, you want to use `new`
        // instead.
        let mut kademlia = Kademlia::without_init(local_peer_id);

        for (identity, location) in BOOTSTRAP_NODES {
            kademlia.add_address(
                &identity.parse().unwrap(),
                location.parse().unwrap(),
            );
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
        self.kademlia.add_providing(hash);
    }

    pub fn stop_providing_block(&mut self, cid: &Cid) {
        let hash = Multihash::from_bytes(cid.hash.clone()).unwrap();
        self.kademlia.remove_providing(&hash);
    }
}

/// Behaviour type.
pub type TBehaviour = Behaviour<SubstreamRef<Arc<StreamMuxerBox>>>;

/// Create a IPFS behaviour with the IPFS bootstrap nodes.
pub fn build_behaviour(local_peer_id: PeerId) -> TBehaviour {
    Behaviour::new(local_peer_id)
}
