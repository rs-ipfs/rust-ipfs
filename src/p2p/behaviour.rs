use super::pubsub::Pubsub;
use super::swarm::{Connection, Disconnector, SwarmApi};
use crate::p2p::{SwarmOptions, SwarmTypes};
use crate::repo::Repo;
use crate::subscription::SubscriptionFuture;
use bitswap::{Bitswap, Strategy};
use libipld::cid::Cid;
use libp2p::core::{Multiaddr, PeerId};
use libp2p::identify::{Identify, IdentifyEvent};
use libp2p::kad::record::store::MemoryStore;
use libp2p::kad::{Kademlia, KademliaEvent};
use libp2p::mdns::{Mdns, MdnsEvent};
use libp2p::ping::{Ping, PingEvent};
use libp2p::swarm::toggle::Toggle;
use libp2p::swarm::{NetworkBehaviour, NetworkBehaviourEventProcess};
use libp2p::NetworkBehaviour;
use std::sync::Arc;

/// Behaviour type.
#[derive(NetworkBehaviour)]
pub struct Behaviour<TSwarmTypes: SwarmTypes> {
    mdns: Toggle<Mdns>,
    kademlia: Kademlia<MemoryStore>,
    bitswap: Bitswap<TSwarmTypes::TStrategy>,
    ping: Ping,
    identify: Identify,
    pubsub: Pubsub,
    swarm: SwarmApi,
}

impl<TSwarmTypes: SwarmTypes> NetworkBehaviourEventProcess<()> for Behaviour<TSwarmTypes> {
    fn inject_event(&mut self, _event: ()) {}
}
impl<TSwarmTypes: SwarmTypes> NetworkBehaviourEventProcess<void::Void> for Behaviour<TSwarmTypes> {
    fn inject_event(&mut self, _event: void::Void) {}
}

impl<TSwarmTypes: SwarmTypes> NetworkBehaviourEventProcess<MdnsEvent> for Behaviour<TSwarmTypes> {
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

impl<TSwarmTypes: SwarmTypes> NetworkBehaviourEventProcess<KademliaEvent>
    for Behaviour<TSwarmTypes>
{
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

impl<TSwarmTypes: SwarmTypes> NetworkBehaviourEventProcess<PingEvent> for Behaviour<TSwarmTypes> {
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

impl<TSwarmTypes: SwarmTypes> NetworkBehaviourEventProcess<IdentifyEvent>
    for Behaviour<TSwarmTypes>
{
    fn inject_event(&mut self, event: IdentifyEvent) {
        log::trace!("identify: {:?}", event);
    }
}

impl<TSwarmTypes: SwarmTypes> Behaviour<TSwarmTypes> {
    /// Create a Kademlia behaviour with the IPFS bootstrap nodes.
    pub async fn new(options: SwarmOptions<TSwarmTypes>, repo: Arc<Repo<TSwarmTypes>>) -> Self {
        info!("Local peer id: {}", options.peer_id.to_base58());

        let mdns = if options.mdns {
            Some(Mdns::new().expect("Failed to create mDNS service"))
        } else {
            None
        }
        .into();

        let store = MemoryStore::new(options.peer_id.to_owned());
        let mut kademlia = Kademlia::new(options.peer_id.to_owned(), store);
        for (addr, peer_id) in &options.bootstrap {
            kademlia.add_address(peer_id, addr.to_owned());
        }

        let strategy = TSwarmTypes::TStrategy::new(repo);
        let bitswap = Bitswap::new(strategy);
        let ping = Ping::default();
        let identify = Identify::new(
            "/ipfs/0.1.0".into(),
            "rust-ipfs".into(),
            options.keypair.public(),
        );
        let pubsub = Pubsub::new(options.peer_id);
        let swarm = SwarmApi::new();

        Behaviour {
            mdns,
            kademlia,
            bitswap,
            ping,
            identify,
            pubsub,
            swarm,
        }
    }

    pub fn add_peer(&mut self, peer: PeerId) {
        self.swarm.add_peer(peer.clone());
        self.pubsub.add_node_to_partial_view(peer);
        // TODO self.bitswap.add_node_to_partial_view(peer);
    }

    pub fn remove_peer(&mut self, peer: &PeerId) {
        self.swarm.remove_peer(&peer);
        self.pubsub.remove_node_from_partial_view(&peer);
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
        self.swarm.connections()
    }

    pub fn connect(&mut self, addr: Multiaddr) -> SubscriptionFuture<Result<(), String>> {
        self.swarm.connect(addr)
    }

    pub fn disconnect(&mut self, addr: Multiaddr) -> Option<Disconnector> {
        self.swarm.disconnect(addr)
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

    pub fn pubsub(&mut self) -> &mut Pubsub {
        &mut self.pubsub
    }

    pub fn bitswap(&mut self) -> &mut Bitswap<TSwarmTypes::TStrategy> {
        &mut self.bitswap
    }
}

/// Create a IPFS behaviour with the IPFS bootstrap nodes.
pub async fn build_behaviour<TSwarmTypes: SwarmTypes>(
    options: SwarmOptions<TSwarmTypes>,
    repo: Arc<Repo<TSwarmTypes>>,
) -> Behaviour<TSwarmTypes> {
    Behaviour::new(options, repo).await
}
