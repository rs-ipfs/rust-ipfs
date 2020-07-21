use super::pubsub::Pubsub;
use super::swarm::{Connection, Disconnector, SwarmApi};
use crate::p2p::{SwarmOptions, SwarmTypes};
use crate::repo::BlockPut;
use crate::subscription::{SubscriptionFuture, SubscriptionRegistry};
use crate::{Ipfs, IpfsTypes};
use anyhow::anyhow;
use async_std::task;
use bitswap::{Bitswap, BitswapEvent};
use cid::Cid;
use libp2p::core::{Multiaddr, PeerId};
use libp2p::identify::{Identify, IdentifyEvent};
use libp2p::kad::record::store::MemoryStore;
use libp2p::kad::{Kademlia, KademliaConfig, KademliaEvent};
use libp2p::mdns::{Mdns, MdnsEvent};
use libp2p::ping::{Ping, PingEvent};
use libp2p::swarm::toggle::Toggle;
use libp2p::swarm::{NetworkBehaviour, NetworkBehaviourEventProcess};
use libp2p::NetworkBehaviour;
use multibase::Base;
use std::sync::Arc;

/// Behaviour type.
#[derive(NetworkBehaviour)]
pub struct Behaviour<Types: IpfsTypes> {
    #[behaviour(ignore)]
    ipfs: Ipfs<Types>,
    mdns: Toggle<Mdns>,
    kademlia: Kademlia<MemoryStore>,
    #[behaviour(ignore)]
    kad_subscriptions: SubscriptionRegistry<Result<(), String>>,
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
                for (peer, addr) in list {
                    log::trace!("mdns: Discovered peer {}", peer.to_base58());
                    self.add_peer(peer, addr);
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
        use libp2p::kad::{
            AddProviderError, AddProviderOk, BootstrapError, BootstrapOk, GetClosestPeersError,
            GetClosestPeersOk, GetProvidersError, GetProvidersOk, GetRecordError, GetRecordOk,
            KademliaEvent::*, PutRecordError, PutRecordOk, QueryResult::*,
        };

        match event {
            QueryResult { result, id, .. } => {
                self.kad_subscriptions
                    .finish_subscription(id.into(), Ok(()));

                match result {
                    Bootstrap(Ok(BootstrapOk { .. })) => {
                        debug!("kad: finished bootstrapping");
                    }
                    Bootstrap(Err(BootstrapError::Timeout { .. })) => {
                        warn!("kad: failed to bootstrap");
                    }
                    GetClosestPeers(Ok(GetClosestPeersOk { key: _, peers })) => {
                        for peer in peers {
                            info!("kad: peer {} is close", peer);
                        }
                    }
                    GetClosestPeers(Err(GetClosestPeersError::Timeout { key: _, peers })) => {
                        warn!(
                            "kad: timed out trying to find all closest peers; got the following:"
                        );
                        for peer in peers {
                            info!("kad: peer {} is close", peer);
                        }
                    }
                    GetProviders(Ok(GetProvidersOk { key, providers, .. })) => {
                        let key = multibase::encode(Base::Base58Btc, key);
                        if providers.is_empty() {
                            // FIXME: not sure if this is possible
                            warn!("kad: could not find a provider for {}", key);
                        } else {
                            for peer in providers {
                                info!("kad: {} provided by {}", key, peer);
                                self.bitswap.connect(peer);
                            }
                        }
                    }
                    GetProviders(Err(GetProvidersError::Timeout { key, .. })) => {
                        let key = multibase::encode(Base::Base58Btc, key);
                        warn!("kad: timed out trying to get providers for {}", key);
                    }
                    StartProviding(Ok(AddProviderOk { key })) => {
                        let key = multibase::encode(Base::Base58Btc, key);
                        info!("kad: added provider {}", key);
                    }
                    StartProviding(Err(AddProviderError::Timeout { key })) => {
                        let key = multibase::encode(Base::Base58Btc, key);
                        warn!("kad: timed out trying to add provider {}", key);
                    }
                    RepublishProvider(Ok(AddProviderOk { key })) => {
                        let key = multibase::encode(Base::Base58Btc, key);
                        info!("kad: republished provider {}", key);
                    }
                    RepublishProvider(Err(AddProviderError::Timeout { key })) => {
                        let key = multibase::encode(Base::Base58Btc, key);
                        warn!("kad: timed out trying to republish provider {}", key);
                    }
                    GetRecord(Ok(GetRecordOk { records })) => {
                        for record in records {
                            let key = multibase::encode(Base::Base58Btc, record.record.key);
                            info!("kad: got record {}:{:?}", key, record.record.value);
                        }
                    }
                    GetRecord(Err(GetRecordError::NotFound {
                        key,
                        closest_peers: _,
                    })) => {
                        let key = multibase::encode(Base::Base58Btc, key);
                        warn!("kad: couldn't find record {}", key);
                    }
                    GetRecord(Err(GetRecordError::QuorumFailed {
                        key,
                        records,
                        quorum,
                    })) => {
                        let key = multibase::encode(Base::Base58Btc, key);

                        warn!(
                            "kad: quorum failed {} trying to get key {}; got the following:",
                            quorum, key
                        );
                        for record in records {
                            let key = multibase::encode(Base::Base58Btc, record.record.key);
                            info!("kad: got record {}:{:?}", key, record.record.value);
                        }
                    }
                    GetRecord(Err(GetRecordError::Timeout {
                        key,
                        records,
                        quorum: _,
                    })) => {
                        let key = multibase::encode(Base::Base58Btc, key);

                        warn!(
                            "kad: timed out trying to get key {}; got the following:",
                            key
                        );
                        for record in records {
                            let key = multibase::encode(Base::Base58Btc, record.record.key);
                            info!("kad: got record {}:{:?}", key, record.record.value);
                        }
                    }
                    PutRecord(Ok(PutRecordOk { key }))
                    | RepublishRecord(Ok(PutRecordOk { key })) => {
                        let key = multibase::encode(Base::Base58Btc, key);
                        info!("kad: successfully put record {}", key);
                    }
                    PutRecord(Err(PutRecordError::QuorumFailed {
                        key,
                        success: _,
                        quorum,
                    }))
                    | RepublishRecord(Err(PutRecordError::QuorumFailed {
                        key,
                        success: _,
                        quorum,
                    })) => {
                        let key = multibase::encode(Base::Base58Btc, key);
                        info!(
                            "kad: quorum failed ({}) trying to put record {}",
                            quorum, key
                        );
                    }
                    PutRecord(Err(PutRecordError::Timeout {
                        key,
                        success: _,
                        quorum: _,
                    }))
                    | RepublishRecord(Err(PutRecordError::Timeout {
                        key,
                        success: _,
                        quorum: _,
                    })) => {
                        let key = multibase::encode(Base::Base58Btc, key);
                        info!("kad: timed out trying to put record {}", key);
                    }
                }
            }
            RoutingUpdated {
                peer,
                addresses,
                old_peer: _,
            } => {
                trace!("kad: routing updated; {}: {:?}", peer, addresses);
            }
            UnroutablePeer { peer } => {
                warn!("kad: peer {} is unroutable", peer);
            }
            RoutablePeer { peer, address } => {
                trace!("kad: peer {} ({}) is routable", peer, address);
            }
            PendingRoutablePeer { peer, address } => {
                trace!("kad: pending routable peer {} ({})", peer, address);
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

                let queued_blocks = self.bitswap().queued_blocks.clone();
                let ipfs = self.ipfs.clone();

                task::spawn(async move {
                    match ipfs.repo.get_block_now(&cid).await {
                        Ok(Some(block)) => {
                            let _ = queued_blocks.unbounded_send((peer_id, block));
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
                    }
                });
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
        info!("net: starting with peer id {}", options.peer_id);

        let mdns = if options.mdns {
            Some(Mdns::new().expect("Failed to create mDNS service"))
        } else {
            None
        }
        .into();

        let store = MemoryStore::new(options.peer_id.to_owned());

        let mut kad_config = KademliaConfig::default();
        kad_config.disjoint_query_paths(true);
        kad_config.set_query_timeout(std::time::Duration::from_secs(300));
        if let Some(protocol) = options.kad_protocol {
            kad_config.set_protocol_name(protocol.into_bytes());
        }
        let mut kademlia = Kademlia::with_config(options.peer_id.to_owned(), store, kad_config);

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
            kad_subscriptions: Default::default(),
            bitswap,
            ping,
            identify,
            pubsub,
            swarm,
        }
    }

    pub fn add_peer(&mut self, peer: PeerId, addr: Multiaddr) {
        self.kademlia.add_address(&peer, addr);
        /*
                match self.kademlia.start_providing(peer.to_base58().into_bytes().into()) {
                    Ok(id) => {
                        self.kad_queries.insert(id);
                    },
                    Err(e) => error!("kad: can't provide peer {}: {:?}", peer, e),
                }
        */
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

    pub fn bootstrap(&mut self) -> Result<SubscriptionFuture<Result<(), String>>, anyhow::Error> {
        match self.kademlia.bootstrap() {
            Ok(id) => Ok(self.kad_subscriptions.create_subscription(id.into(), None)),
            Err(e) => {
                error!("kad: can't bootstrap the node: {:?}", e);
                Err(anyhow!("kad: can't bootstrap the node: {:?}", e))
            }
        }
    }

    pub fn get_closest_peers(&mut self, id: PeerId) -> SubscriptionFuture<Result<(), String>> {
        let id = id.to_base58();

        self.kad_subscriptions
            .create_subscription(self.kademlia.get_closest_peers(id.as_bytes()).into(), None)
    }
}

/// Create a IPFS behaviour with the IPFS bootstrap nodes.
pub async fn build_behaviour<TSwarmTypes: SwarmTypes>(
    options: SwarmOptions<TSwarmTypes>,
    ipfs: Ipfs<TSwarmTypes>,
) -> Behaviour<TSwarmTypes> {
    Behaviour::new(options, ipfs).await
}
