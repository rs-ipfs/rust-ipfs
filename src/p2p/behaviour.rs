use super::pubsub::Pubsub;
use super::swarm::{Connection, Disconnector, SwarmApi};
use crate::p2p::{SwarmOptions, SwarmTypes};
use crate::repo::BlockPut;
use crate::subscription::SubscriptionFuture;
use crate::{Ipfs, IpfsTypes};
use async_std::task;
use bitswap::{Bitswap, BitswapEvent};
use cid::Cid;
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
pub struct Behaviour<Types: IpfsTypes> {
    #[behaviour(ignore)]
    ipfs: Ipfs<Types>,
    mdns: Toggle<Mdns>,
    kademlia: Kademlia<MemoryStore>,
    bitswap: Bitswap,
    ping: Ping,
    identify: Identify,
    pubsub: Pubsub,
    swarm: SwarmApi,
}

impl<Types: IpfsTypes> NetworkBehaviourEventProcess<()> for Behaviour<Types> {
    fn inject_event(&mut self, _event: ()) {}
}
impl<Types: IpfsTypes> NetworkBehaviourEventProcess<void::Void> for Behaviour<Types> {
    fn inject_event(&mut self, _event: void::Void) {}
}

impl<Types: IpfsTypes> NetworkBehaviourEventProcess<MdnsEvent> for Behaviour<Types> {
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

impl<Types: IpfsTypes> NetworkBehaviourEventProcess<KademliaEvent> for Behaviour<Types> {
    fn inject_event(&mut self, event: KademliaEvent) {
        use libp2p::kad::{GetProvidersError, GetProvidersOk, QueryResult};

        match event {
            KademliaEvent::QueryResult {
                result:
                    QueryResult::GetProviders(Ok(GetProvidersOk {
                        key,
                        providers,
                        closest_peers,
                    })),
                ..
            } => {
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
            KademliaEvent::QueryResult {
                result: QueryResult::GetProviders(Err(GetProvidersError::Timeout { key, .. })),
                ..
            } => {
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

impl<Types: IpfsTypes> NetworkBehaviourEventProcess<BitswapEvent> for Behaviour<Types> {
    fn inject_event(&mut self, event: BitswapEvent) {
        match event {
            BitswapEvent::ReceivedBlock(peer_id, block) => {
                let ipfs = self.ipfs.clone();
                let peer_stats =
                    Arc::clone(&self.bitswap.connected_peers.get(&peer_id).unwrap().stats);
                task::spawn(async move {
                    let bytes = block.data().len() as u64;
                    let res = ipfs.repo.put_block(block.clone()).await;
                    match res {
                        Ok((_, uniqueness)) => match uniqueness {
                            BlockPut::NewBlock => peer_stats.update_incoming_unique(bytes),
                            BlockPut::Existed => peer_stats.update_incoming_duplicate(bytes),
                        },
                        Err(e) => {
                            debug!(
                                "Got block {} from peer {} but failed to store it: {}",
                                block.cid,
                                peer_id.to_base58(),
                                e
                            );
                            return;
                        }
                    };
                });
            }
            BitswapEvent::ReceivedWant(peer_id, cid, priority) => {
                info!(
                    "Peer {} wants block {} with priority {}",
                    peer_id.to_base58(),
                    cid,
                    priority
                );

                match self.ipfs.repo.get_block_now(&cid) {
                    Ok(Some(block)) => {
                        self.bitswap()
                            .queued_blocks
                            .lock()
                            .unwrap()
                            .push((peer_id, block));
                    }
                    Ok(None) => {}
                    Err(err) => {
                        warn!(
                            "Peer {} wanted block {} but we failed: {}",
                            peer_id.to_base58(),
                            cid,
                            err,
                        );
                    }
                };
            }
            BitswapEvent::ReceivedCancel(..) => {}
        }
    }
}

impl<Types: IpfsTypes> NetworkBehaviourEventProcess<PingEvent> for Behaviour<Types> {
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
                log::trace!("ping: pong from {}", peer);
            }
            PingEvent {
                peer,
                result: Result::Err(PingFailure::Timeout),
            } => {
                log::trace!("ping: timeout to {}", peer);
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

impl<Types: IpfsTypes> NetworkBehaviourEventProcess<IdentifyEvent> for Behaviour<Types> {
    fn inject_event(&mut self, event: IdentifyEvent) {
        log::trace!("identify: {:?}", event);
    }
}

impl<Types: IpfsTypes> Behaviour<Types> {
    /// Create a Kademlia behaviour with the IPFS bootstrap nodes.
    pub async fn new<TSwarmTypes: SwarmTypes>(
        options: SwarmOptions<TSwarmTypes>,
        ipfs: Ipfs<Types>,
    ) -> Self {
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

        let bitswap = Bitswap::default();
        let ping = Ping::default();
        let identify = Identify::new(
            "/ipfs/0.1.0".into(),
            "rust-ipfs".into(),
            options.keypair.public(),
        );
        let pubsub = Pubsub::new(options.peer_id);
        let swarm = SwarmApi::new();

        Behaviour {
            ipfs,
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

    pub fn connections(&self) -> impl Iterator<Item = Connection> + '_ {
        self.swarm.connections()
    }

    pub fn connect(&mut self, addr: Multiaddr) -> SubscriptionFuture<Result<(), String>> {
        self.swarm.connect(addr)
    }

    pub fn disconnect(&mut self, addr: Multiaddr) -> Option<Disconnector> {
        self.swarm.disconnect(addr)
    }

    pub fn want_block(&mut self, cid: Cid) {
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

    pub fn bitswap(&mut self) -> &mut Bitswap {
        &mut self.bitswap
    }
}

/// Create a IPFS behaviour with the IPFS bootstrap nodes.
pub async fn build_behaviour<TSwarmTypes: SwarmTypes>(
    options: SwarmOptions<TSwarmTypes>,
    ipfs: Ipfs<TSwarmTypes>,
) -> Behaviour<TSwarmTypes> {
    Behaviour::new(options, ipfs).await
}
