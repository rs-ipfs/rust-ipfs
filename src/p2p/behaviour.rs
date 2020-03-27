use super::swarm::{Connection, Disconnector, SwarmApi};
use crate::options::IpfsOptions;
use crate::registry::Channel;
use bitswap::{Bitswap, BitswapEvent, Block, Priority};
use core::task::{Context, Poll};
use libipld::cid::Cid;
use libp2p::core::{Multiaddr, PeerId};
use libp2p::floodsub::{Floodsub, FloodsubEvent};
use libp2p::identify::{Identify, IdentifyEvent};
use libp2p::kad::record::store::MemoryStore;
use libp2p::kad::{Kademlia, KademliaEvent};
use libp2p::mdns::{Mdns, MdnsEvent};
use libp2p::ping::{Ping, PingEvent};
use libp2p::swarm::toggle::Toggle;
use libp2p::swarm::{
    NetworkBehaviour, NetworkBehaviourAction, NetworkBehaviourEventProcess, PollParameters,
};
use libp2p::NetworkBehaviour;
use std::collections::VecDeque;

/// Behaviour event.
#[derive(Debug)]
pub enum BehaviourEvent {
    ReceivedBlock(PeerId, Block),
    ReceivedWant(PeerId, Cid),
}

/// Behaviour type.
#[derive(NetworkBehaviour)]
#[behaviour(poll_method = "custom_poll", out_event = "BehaviourEvent")]
pub struct Behaviour {
    mdns: Toggle<Mdns>,
    kademlia: Kademlia<MemoryStore>,
    bitswap: Bitswap,
    ping: Ping,
    identify: Identify,
    floodsub: Floodsub,
    swarm: SwarmApi,
    #[behaviour(ignore)]
    events: VecDeque<BehaviourEvent>,
    #[behaviour(ignore)]
    blocks_sent: u64,
    #[behaviour(ignore)]
    data_sent: u64,
}

impl NetworkBehaviourEventProcess<void::Void> for Behaviour {
    fn inject_event(&mut self, _event: void::Void) {}
}

impl NetworkBehaviourEventProcess<MdnsEvent> for Behaviour {
    fn inject_event(&mut self, event: MdnsEvent) {
        match event {
            MdnsEvent::Discovered(list) => {
                for (peer, _) in list {
                    log::trace!("mdns: Discovered peer {}", peer.to_base58());
                    self.add_peer(peer);
                }
            }
            MdnsEvent::Expired(list) => {
                for (peer, _) in list {
                    log::trace!("mdns: Expired peer {}", peer.to_base58());
                    self.remove_peer(&peer);
                }
            }
        }
    }
}

impl NetworkBehaviourEventProcess<KademliaEvent> for Behaviour {
    fn inject_event(&mut self, event: KademliaEvent) {
        use libp2p::kad::{GetProvidersError, GetProvidersOk};

        match event {
            KademliaEvent::Discovered { peer_id, ty, .. } => {
                log::trace!("kad: Discovered peer {} {:?}", peer_id.to_base58(), ty);
                self.add_peer(peer_id);
            }
            KademliaEvent::GetProvidersResult(Ok(GetProvidersOk {
                key,
                providers,
                closest_peers,
            })) => {
                // FIXME: really wasteful to run this through Vec
                let cid = PeerId::from_bytes(key.to_vec()).unwrap().to_base58();
                if providers.is_empty() {
                    // FIXME: not sure if this is possible
                    info!("kad: Could not find provider for {}", cid);
                } else {
                    for peer in closest_peers {
                        info!("kad: {} provided by {}", cid, peer.to_base58());
                        self.bitswap.connect(peer);
                    }
                }
            }
            KademliaEvent::GetProvidersResult(Err(GetProvidersError::Timeout { key, .. })) => {
                // FIXME: really wasteful to run this through Vec
                let cid = PeerId::from_bytes(key.to_vec()).unwrap().to_base58();
                warn!("kad: timed out get providers query for {}", cid);
            }
            event => {
                log::trace!("kad: {:?}", event);
            }
        }
    }
}

impl NetworkBehaviourEventProcess<BitswapEvent> for Behaviour {
    fn inject_event(&mut self, event: BitswapEvent) {
        let event = match event {
            BitswapEvent::ReceivedBlock(peer_id, block) => {
                BehaviourEvent::ReceivedBlock(peer_id, block)
            }
            BitswapEvent::ReceivedWant(peer_id, cid, _) => {
                BehaviourEvent::ReceivedWant(peer_id, cid)
            }
            BitswapEvent::ReceivedCancel(_, _) => return,
        };
        self.events.push_back(event);
    }
}

impl NetworkBehaviourEventProcess<PingEvent> for Behaviour {
    fn inject_event(&mut self, event: PingEvent) {
        use libp2p::ping::handler::{PingFailure, PingSuccess};
        match event {
            PingEvent {
                peer,
                result: Result::Ok(PingSuccess::Ping { rtt }),
            } => {
                log::trace!(
                    "ping: rtt to {} is {} ms",
                    peer.to_base58(),
                    rtt.as_millis()
                );
                self.swarm.set_rtt(&peer, rtt);
            }
            PingEvent {
                peer,
                result: Result::Ok(PingSuccess::Pong),
            } => {
                log::trace!("ping: pong from {}", peer.to_base58());
            }
            PingEvent {
                peer,
                result: Result::Err(PingFailure::Timeout),
            } => {
                log::trace!("ping: timeout to {}", peer.to_base58());
                self.remove_peer(&peer);
            }
            PingEvent {
                peer,
                result: Result::Err(PingFailure::Other { error }),
            } => {
                log::error!("ping: failure with {}: {}", peer.to_base58(), error);
            }
        }
    }
}

impl NetworkBehaviourEventProcess<IdentifyEvent> for Behaviour {
    fn inject_event(&mut self, event: IdentifyEvent) {
        log::trace!("identify: {:?}", event);
    }
}

impl NetworkBehaviourEventProcess<FloodsubEvent> for Behaviour {
    fn inject_event(&mut self, event: FloodsubEvent) {
        log::trace!("floodsub: {:?}", event);
    }
}

impl Behaviour {
    /// Create a Kademlia behaviour with the IPFS bootstrap nodes.
    pub fn new(options: &IpfsOptions) -> Self {
        info!("Local peer id: {}", options.peer_id().to_base58());

        let mdns = if options.mdns {
            Some(Mdns::new().expect("Failed to create mDNS service"))
        } else {
            None
        }
        .into();

        let store = MemoryStore::new(options.peer_id());
        let mut kademlia = Kademlia::new(options.peer_id(), store);
        for (addr, peer_id) in &options.bootstrap {
            kademlia.add_address(peer_id, addr.to_owned());
        }

        let bitswap = Bitswap::new();
        let ping = Ping::default();
        let identify = Identify::new(
            "/ipfs/0.1.0".into(),
            "rust-ipfs".into(),
            options.keypair.public(),
        );
        let floodsub = Floodsub::new(options.peer_id());
        let swarm = SwarmApi::new();

        Behaviour {
            mdns,
            kademlia,
            bitswap,
            ping,
            identify,
            floodsub,
            swarm,
            events: Default::default(),
            blocks_sent: 0,
            data_sent: 0,
        }
    }

    pub fn add_peer(&mut self, peer: PeerId) {
        self.swarm.add_peer(peer.clone());
        self.floodsub.add_node_to_partial_view(peer);
        // TODO self.bitswap.add_node_to_partial_view(peer);
    }

    pub fn remove_peer(&mut self, peer: &PeerId) {
        self.swarm.remove_peer(&peer);
        self.floodsub.remove_node_from_partial_view(&peer);
        // TODO self.bitswap.remove_peer(&peer);
    }

    pub fn addrs(&mut self) -> Vec<(PeerId, Vec<Multiaddr>)> {
        let peers = self.swarm.peers().cloned().collect::<Vec<_>>();
        let mut addrs = Vec::with_capacity(peers.len());
        for peer_id in peers.into_iter() {
            let peer_addrs = self.addresses_of_peer(&peer_id);
            addrs.push((peer_id, peer_addrs));
        }
        addrs
    }

    pub fn connections(&self) -> Vec<Connection> {
        self.swarm.connections().cloned().collect()
    }

    pub fn connect(&mut self, addr: Multiaddr, ret: Channel<()>) {
        self.swarm.connect(addr, ret);
    }

    pub fn disconnect(&mut self, addr: Multiaddr) -> Option<Disconnector> {
        self.swarm.disconnect(addr)
    }

    pub fn send_block(&mut self, peer_id: &PeerId, block: Block) {
        self.blocks_sent += 1;
        self.data_sent += block.data().len() as u64;
        self.bitswap.send_block(peer_id, block);
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

    pub fn bitswap_wantlist(&self, peer_id: Option<&PeerId>) -> Vec<(Cid, Priority)> {
        self.bitswap.wantlist(peer_id)
    }

    pub fn bitswap_peers(&self) -> Vec<PeerId> {
        self.bitswap.peers()
    }

    pub fn bitswap_blocks_sent(&self) -> u64 {
        self.blocks_sent
    }

    pub fn bitswap_data_sent(&self) -> u64 {
        self.data_sent
    }

    pub fn custom_poll<T>(
        &mut self,
        _: &mut Context,
        _: &mut impl PollParameters,
    ) -> Poll<NetworkBehaviourAction<T, BehaviourEvent>> {
        if let Some(event) = self.events.pop_front() {
            Poll::Ready(NetworkBehaviourAction::GenerateEvent(event))
        } else {
            Poll::Pending
        }
    }
}
