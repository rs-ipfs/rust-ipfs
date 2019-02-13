use crate::bitswap::{Bitswap, Strategy};
use crate::block::Cid;
use crate::config::NetworkConfig;
use libp2p::NetworkBehaviour;
use libp2p::core::swarm::NetworkBehaviourEventProcess;
use libp2p::core::muxing::{StreamMuxerBox, SubstreamRef};
//use libp2p::kad::{Kademlia, KademliaOut as KademliaEvent};
use libp2p::mdns::{Mdns, MdnsEvent};
//use parity_multihash::Multihash;
use std::sync::Arc;
use tokio::prelude::*;

/// Behaviour type.
#[derive(NetworkBehaviour)]
pub struct Behaviour<TSubstream: AsyncRead + AsyncWrite, TStrategy: Strategy> {
    mdns: Mdns<TSubstream>,
    //kademlia: Kademlia<TSubstream>,
    bitswap: Bitswap<TSubstream, TStrategy>,
}

impl<TSubstream: AsyncRead + AsyncWrite, TStrategy: Strategy>
    NetworkBehaviourEventProcess<MdnsEvent> for
    Behaviour<TSubstream, TStrategy>
{
    fn inject_event(&mut self, event: MdnsEvent) {
        match event {
            MdnsEvent::Discovered(list) => {
                for (peer, _) in list {
                    println!("mdns: Discovered peer {}", peer.to_base58());
                    self.bitswap.connect(peer);
                }
            }
            MdnsEvent::Expired(list) => {
                for (peer, _) in list {
                    if !self.mdns.has_node(&peer) {
                        println!("mdns: Expired peer {}", peer.to_base58());
                    }
                }
            }
        }
    }
}

/*impl<TSubstream: AsyncRead + AsyncWrite, TStrategy: Strategy>
    NetworkBehaviourEventProcess<KademliaEvent> for
    Behaviour<TSubstream, TStrategy>
{
    fn inject_event(&mut self, event: KademliaEvent) {
        match event {
            KademliaEvent::Discovered { peer_id: _, addresses: _, ty: _ } => {
                //println!("kad: Discovered peer {} {:?}", peer_id.to_base58(), ty);
            }
            KademliaEvent::FindNodeResult { key, closer_peers } => {
                if closer_peers.is_empty() {
                    println!("kad: Could not find closer peer to {}", key.to_base58());
                }
                for peer in closer_peers {
                    println!("kad: Found closer peer {} to {}", peer.to_base58(), key.to_base58());
                }
            }
            KademliaEvent::GetProvidersResult {
                key,
                provider_peers,
                ..
            } => {
                let cid = PeerId::from_multihash(key).unwrap().to_base58();
                if provider_peers.is_empty() {
                    println!("kad: Could not find provider for {}", cid);
                } else {
                    for peer in provider_peers {
                        println!("kad: {} provided by {}", cid, peer.to_base58());
                        self.bitswap.connect(peer);
                    }
                }
            }
        }
    }
}*/

impl<TSubstream: AsyncRead + AsyncWrite, TStrategy: Strategy>
    NetworkBehaviourEventProcess<()> for
    Behaviour<TSubstream, TStrategy>
{
    fn inject_event(&mut self, _event: ()) {}
}

impl<TSubstream: AsyncRead + AsyncWrite, TStrategy: Strategy> Behaviour<TSubstream, TStrategy>
{
    /// Create a Kademlia behaviour with the IPFS bootstrap nodes.
    pub fn new(config: NetworkConfig<TStrategy>) -> Self {
        println!("Local peer id: {}", config.peer_id.to_base58());

        let mdns = Mdns::new().expect("Failed to create mDNS service");

        /*let mut kademlia = Kademlia::new(config.peer_id.to_owned());
        for (addr, peer_id) in &config.bootstrap {
            kademlia.add_address(peer_id, addr.to_owned());
        }*/

        let bitswap = Bitswap::new(config.strategy);

        Behaviour {
            mdns,
            //kademlia,
            bitswap,
        }
    }

    pub fn want_block(&mut self, cid: Cid) {
        println!("Want block {}", cid.to_string());
        //let hash = Multihash::from_bytes(cid.to_bytes()).unwrap();
        //self.kademlia.get_providers(hash);
        self.bitswap.want_block(cid, 1);
    }

    pub fn provide_block(&mut self, cid: &Cid) {
        println!("Providing block {}", cid.to_string());
        //let hash = Multihash::from_bytes(cid.hash.clone()).unwrap();
        //self.kademlia.add_providing(PeerId::from_multihash(hash).unwrap());
    }

    pub fn stop_providing_block(&mut self, cid: &Cid) {
        println!("Finished providing block {}", cid.to_string());
        //let hash = Multihash::from_bytes(cid.hash.clone()).unwrap();
        //self.kademlia.remove_providing(&hash);
    }
}

/// Behaviour type.
pub type TBehaviour<TStrategy> = Behaviour<SubstreamRef<Arc<StreamMuxerBox>>, TStrategy>;

/// Create a IPFS behaviour with the IPFS bootstrap nodes.
pub fn build_behaviour<TStrategy: Strategy>(config: NetworkConfig<TStrategy>) -> TBehaviour<TStrategy> {
    Behaviour::new(config)
}
