use crate::bitswap::{Bitswap, Strategy};
use crate::block::Cid;
use crate::p2p::{SwarmOptions, SwarmTypes};
use crate::repo::Repo;
use libp2p::{NetworkBehaviour, PeerId};
use libp2p::core::swarm::NetworkBehaviourEventProcess;
use libp2p::core::muxing::{StreamMuxerBox, SubstreamRef};
use libp2p::identify::{Identify, IdentifyEvent};
use libp2p::kad::{Kademlia, KademliaOut as KademliaEvent};
use libp2p::mdns::{Mdns, MdnsEvent};
use libp2p::ping::{Ping, PingEvent};
use libp2p::floodsub::{Floodsub, FloodsubEvent};
//use parity_multihash::Multihash;
use std::sync::Arc;
use tokio::prelude::*;

/// Behaviour type.
#[derive(NetworkBehaviour)]
pub struct Behaviour<TSubstream: AsyncRead + AsyncWrite, TSwarmTypes: SwarmTypes> {
    mdns: Mdns<TSubstream>,
    kademlia: Kademlia<TSubstream>,
    bitswap: Bitswap<TSubstream, TSwarmTypes>,
    ping: Ping<TSubstream>,
    identify: Identify<TSubstream>,
    floodsub: Floodsub<TSubstream>,
}

impl<TSubstream: AsyncRead + AsyncWrite, TSwarmTypes: SwarmTypes>
    NetworkBehaviourEventProcess<MdnsEvent> for
    Behaviour<TSubstream, TSwarmTypes>
{
    fn inject_event(&mut self, event: MdnsEvent) {
        match event {
            MdnsEvent::Discovered(list) => {
                for (peer, _) in list {
                    debug!("mdns: Discovered peer {}", peer.to_base58());
                    self.bitswap.connect(peer.clone());
                    self.floodsub.add_node_to_partial_view(peer);
                }
            }
            MdnsEvent::Expired(list) => {
                for (peer, _) in list {
                    if !self.mdns.has_node(&peer) {
                        debug!("mdns: Expired peer {}", peer.to_base58());
                        self.floodsub.remove_node_from_partial_view(&peer);
                    }
                }
            }
        }
    }
}

impl<TSubstream: AsyncRead + AsyncWrite, TSwarmTypes: SwarmTypes>
    NetworkBehaviourEventProcess<KademliaEvent> for
    Behaviour<TSubstream, TSwarmTypes>
{
    fn inject_event(&mut self, event: KademliaEvent) {
        match event {
            KademliaEvent::Discovered { peer_id, addresses: _, ty } => {
                debug!("kad: Discovered peer {} {:?}", peer_id.to_base58(), ty);
            }
            KademliaEvent::FindNodeResult { key, closer_peers } => {
                if closer_peers.is_empty() {
                    info!("kad: Could not find closer peer to {}", key.to_base58());
                }
                for peer in closer_peers {
                    info!("kad: Found closer peer {} to {}", peer.to_base58(), key.to_base58());
                }
            }
            KademliaEvent::GetProvidersResult {
                key,
                provider_peers,
                ..
            } => {
                let cid = PeerId::from_multihash(key).unwrap().to_base58();
                if provider_peers.is_empty() {
                    info!("kad: Could not find provider for {}", cid);
                } else {
                    for peer in provider_peers {
                        info!("kad: {} provided by {}", cid, peer.to_base58());
                        self.bitswap.connect(peer);
                    }
                }
            }
        }
    }
}

impl<TSubstream: AsyncRead + AsyncWrite, TSwarmTypes: SwarmTypes>
    NetworkBehaviourEventProcess<()> for
    Behaviour<TSubstream, TSwarmTypes>
{
    fn inject_event(&mut self, _event: ()) {}
}

impl<TSubstream: AsyncRead + AsyncWrite, TSwarmTypes: SwarmTypes>
    NetworkBehaviourEventProcess<PingEvent> for
    Behaviour<TSubstream, TSwarmTypes>
{
    fn inject_event(&mut self, event: PingEvent) {
        match event {
            PingEvent::PingSuccess { peer, time } => {
                debug!("ping: rtt to {} is {} ms", peer.to_base58(), time.as_millis());
            }
        }
    }
}

impl<TSubstream: AsyncRead + AsyncWrite, TSwarmTypes: SwarmTypes>
    NetworkBehaviourEventProcess<IdentifyEvent> for
    Behaviour<TSubstream, TSwarmTypes>
{
    fn inject_event(&mut self, event: IdentifyEvent) {
        debug!("identify: {:?}", event);
    }
}

impl<TSubstream: AsyncRead + AsyncWrite, TSwarmTypes: SwarmTypes>
    NetworkBehaviourEventProcess<FloodsubEvent> for
    Behaviour<TSubstream, TSwarmTypes>
{
    fn inject_event(&mut self, event: FloodsubEvent) {
        debug!("floodsub: {:?}", event);
    }
}

impl<TSubstream: AsyncRead + AsyncWrite, TSwarmTypes: SwarmTypes> Behaviour<TSubstream, TSwarmTypes>
{
    /// Create a Kademlia behaviour with the IPFS bootstrap nodes.
    pub fn new(options: SwarmOptions<TSwarmTypes>, repo: Repo<TSwarmTypes>) -> Self {
        info!("Local peer id: {}", options.peer_id.to_base58());

        let mdns = Mdns::new_with_legacy()
            .expect("Failed to create mDNS service. Are you connected to a Network?");

        let mut kademlia = Kademlia::new(options.peer_id.to_owned());
        for (addr, peer_id) in &options.bootstrap {
            kademlia.add_not_connected_address(peer_id, addr.to_owned());
        }

        let strategy = TSwarmTypes::TStrategy::new(repo);
        let bitswap = Bitswap::new(strategy);
        let ping = Ping::new();
        let identify = Identify::new(
            "/ipfs/0.1.0".into(),
            "rust-ipfs".into(),
            options.key_pair.to_public_key(),
        );
        let floodsub = Floodsub::new(options.peer_id.to_owned());

        Behaviour {
            mdns,
            kademlia,
            bitswap,
            ping,
            identify,
            floodsub,
        }
    }

    pub fn want_block(&mut self, cid: Cid) {
        info!("Want block {}", cid.to_string());
        //let hash = Multihash::from_bytes(cid.to_bytes()).unwrap();
        //self.kademlia.get_providers(hash);
        self.bitswap.want_block(cid, 1);
    }

    pub fn provide_block(&mut self, cid: Cid) {
        info!("Providing block {}", cid.to_string());
        //let hash = Multihash::from_bytes(cid.to_bytes()).unwrap();
        //self.kademlia.add_providing(PeerId::from_multihash(hash).unwrap());
    }

    pub fn stop_providing_block(&mut self, cid: &Cid) {
        info!("Finished providing block {}", cid.to_string());
        //let hash = Multihash::from_bytes(cid.to_bytes()).unwrap();
        //self.kademlia.remove_providing(&hash);
    }
}

/// Behaviour type.
pub(crate) type TBehaviour<TSwarmTypes> = Behaviour<SubstreamRef<Arc<StreamMuxerBox>>, TSwarmTypes>;

/// Create a IPFS behaviour with the IPFS bootstrap nodes.
pub fn build_behaviour<TSwarmTypes: SwarmTypes>(options: SwarmOptions<TSwarmTypes>, repo: Repo<TSwarmTypes>) -> TBehaviour<TSwarmTypes> {
    Behaviour::new(options, repo)
}
