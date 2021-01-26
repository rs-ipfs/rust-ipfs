use super::pubsub::Pubsub;
use super::swarm::{Connection, Disconnector, SwarmApi};
use crate::config::BOOTSTRAP_NODES;
use crate::p2p::{MultiaddrWithPeerId, SwarmOptions};
use crate::repo::{BlockPut, Repo};
use crate::subscription::{SubscriptionFuture, SubscriptionRegistry};
use crate::IpfsTypes;
use anyhow::anyhow;
use cid::Cid;
use ipfs_bitswap::{Bitswap, BitswapEvent};
use libp2p::core::{Multiaddr, PeerId};
use libp2p::identify::{Identify, IdentifyEvent};
use libp2p::kad::record::{store::MemoryStore, Key, Record};
use libp2p::kad::{Kademlia, KademliaConfig, KademliaEvent, Quorum};
// use libp2p::mdns::{MdnsEvent, TokioMdns};
use libp2p::ping::{Ping, PingEvent};
// use libp2p::swarm::toggle::Toggle;
use libp2p::swarm::{NetworkBehaviour, NetworkBehaviourEventProcess};
use multibase::Base;
use std::{convert::TryInto, sync::Arc};
use tokio::task;

/// Behaviour type.
#[derive(libp2p::NetworkBehaviour)]
pub struct Behaviour<Types: IpfsTypes> {
    #[behaviour(ignore)]
    repo: Arc<Repo<Types>>,
    // mdns: Toggle<TokioMdns>,
    kademlia: Kademlia<MemoryStore>,
    #[behaviour(ignore)]
    kad_subscriptions: SubscriptionRegistry<KadResult, String>,
    bitswap: Bitswap,
    ping: Ping,
    identify: Identify,
    pubsub: Pubsub,
    pub swarm: SwarmApi,
}

/// Represents the result of a Kademlia query.
#[derive(Debug, Clone, PartialEq)]
pub enum KadResult {
    /// The query has been exhausted.
    Complete,
    /// The query successfully returns `GetClosestPeers` or `GetProviders` results.
    Peers(Vec<PeerId>),
    /// The query successfully returns a `GetRecord` result.
    Records(Vec<Record>),
}

impl<Types: IpfsTypes> NetworkBehaviourEventProcess<()> for Behaviour<Types> {
    fn inject_event(&mut self, _event: ()) {}
}
impl<Types: IpfsTypes> NetworkBehaviourEventProcess<void::Void> for Behaviour<Types> {
    fn inject_event(&mut self, _event: void::Void) {}
}

/*
impl<Types: IpfsTypes> NetworkBehaviourEventProcess<MdnsEvent> for Behaviour<Types> {
    fn inject_event(&mut self, event: MdnsEvent) {
        match event {
            MdnsEvent::Discovered(list) => {
                for (peer, addr) in list {
                    trace!("mdns: Discovered peer {}", peer.to_base58());
                    self.add_peer(peer, addr);
                }
            }
            MdnsEvent::Expired(list) => {
                for (peer, _) in list {
                    trace!("mdns: Expired peer {}", peer.to_base58());
                    self.remove_peer(&peer);
                }
            }
        }
    }
}
*/

impl<Types: IpfsTypes> NetworkBehaviourEventProcess<KademliaEvent> for Behaviour<Types> {
    fn inject_event(&mut self, event: KademliaEvent) {
        use libp2p::kad::{
            AddProviderError, AddProviderOk, BootstrapError, BootstrapOk, GetClosestPeersError,
            GetClosestPeersOk, GetProvidersError, GetProvidersOk, GetRecordError, GetRecordOk,
            KademliaEvent::*, PutRecordError, PutRecordOk, QueryResult::*,
        };

        match event {
            QueryResult { result, id, .. } => {
                // make sure the query is exhausted
                if self.kademlia.query(&id).is_none() {
                    match result {
                        // these subscriptions return actual values
                        GetClosestPeers(_) | GetProviders(_) | GetRecord(_) => {}
                        // we want to return specific errors for the following
                        Bootstrap(Err(_)) | StartProviding(Err(_)) | PutRecord(Err(_)) => {}
                        // and the rest can just return a general KadResult::Complete
                        _ => {
                            self.kad_subscriptions
                                .finish_subscription(id.into(), Ok(KadResult::Complete));
                        }
                    }
                }

                match result {
                    Bootstrap(Ok(BootstrapOk {
                        peer,
                        num_remaining,
                    })) => {
                        debug!(
                            "kad: bootstrapped with {}, {} peers remain",
                            peer, num_remaining
                        );
                    }
                    Bootstrap(Err(BootstrapError::Timeout { .. })) => {
                        warn!("kad: timed out while trying to bootstrap");

                        if self.kademlia.query(&id).is_none() {
                            self.kad_subscriptions.finish_subscription(
                                id.into(),
                                Err("kad: timed out while trying to bootstrap".into()),
                            );
                        }
                    }
                    GetClosestPeers(Ok(GetClosestPeersOk { key: _, peers })) => {
                        if self.kademlia.query(&id).is_none() {
                            self.kad_subscriptions
                                .finish_subscription(id.into(), Ok(KadResult::Peers(peers)));
                        }
                    }
                    GetClosestPeers(Err(GetClosestPeersError::Timeout { key: _, peers: _ })) => {
                        // don't mention the key here, as this is just the id of our node
                        warn!("kad: timed out while trying to find all closest peers");

                        if self.kademlia.query(&id).is_none() {
                            self.kad_subscriptions.finish_subscription(
                                id.into(),
                                Err("timed out while trying to get providers for the given key"
                                    .into()),
                            );
                        }
                    }
                    GetProviders(Ok(GetProvidersOk {
                        key: _,
                        providers,
                        closest_peers: _,
                    })) => {
                        if self.kademlia.query(&id).is_none() {
                            let providers = providers.into_iter().collect::<Vec<_>>();

                            self.kad_subscriptions
                                .finish_subscription(id.into(), Ok(KadResult::Peers(providers)));
                        }
                    }
                    GetProviders(Err(GetProvidersError::Timeout { key, .. })) => {
                        let key = multibase::encode(Base::Base32Lower, key);
                        warn!("kad: timed out while trying to get providers for {}", key);

                        if self.kademlia.query(&id).is_none() {
                            self.kad_subscriptions.finish_subscription(
                                id.into(),
                                Err("timed out while trying to get providers for the given key"
                                    .into()),
                            );
                        }
                    }
                    StartProviding(Ok(AddProviderOk { key })) => {
                        let key = multibase::encode(Base::Base32Lower, key);
                        debug!("kad: providing {}", key);
                    }
                    StartProviding(Err(AddProviderError::Timeout { key })) => {
                        let key = multibase::encode(Base::Base32Lower, key);
                        warn!("kad: timed out while trying to provide {}", key);

                        if self.kademlia.query(&id).is_none() {
                            self.kad_subscriptions.finish_subscription(
                                id.into(),
                                Err("kad: timed out while trying to provide the record".into()),
                            );
                        }
                    }
                    RepublishProvider(Ok(AddProviderOk { key })) => {
                        let key = multibase::encode(Base::Base32Lower, key);
                        debug!("kad: republished provider {}", key);
                    }
                    RepublishProvider(Err(AddProviderError::Timeout { key })) => {
                        let key = multibase::encode(Base::Base32Lower, key);
                        warn!("kad: timed out while trying to republish provider {}", key);
                    }
                    GetRecord(Ok(GetRecordOk { records })) => {
                        if self.kademlia.query(&id).is_none() {
                            let records = records.into_iter().map(|rec| rec.record).collect();
                            self.kad_subscriptions
                                .finish_subscription(id.into(), Ok(KadResult::Records(records)));
                        }
                    }
                    GetRecord(Err(GetRecordError::NotFound {
                        key,
                        closest_peers: _,
                    })) => {
                        let key = multibase::encode(Base::Base32Lower, key);
                        warn!("kad: couldn't find record {}", key);

                        if self.kademlia.query(&id).is_none() {
                            self.kad_subscriptions.finish_subscription(
                                id.into(),
                                Err("couldn't find a record for the given key".into()),
                            );
                        }
                    }
                    GetRecord(Err(GetRecordError::QuorumFailed {
                        key,
                        records: _,
                        quorum,
                    })) => {
                        let key = multibase::encode(Base::Base32Lower, key);
                        warn!(
                            "kad: quorum failed {} when trying to get key {}",
                            quorum, key
                        );

                        if self.kademlia.query(&id).is_none() {
                            self.kad_subscriptions.finish_subscription(
                                id.into(),
                                Err("quorum failed when trying to obtain a record for the given key"
                                    .into()),
                            );
                        }
                    }
                    GetRecord(Err(GetRecordError::Timeout {
                        key,
                        records: _,
                        quorum: _,
                    })) => {
                        let key = multibase::encode(Base::Base32Lower, key);
                        warn!("kad: timed out while trying to get key {}", key);

                        if self.kademlia.query(&id).is_none() {
                            self.kad_subscriptions.finish_subscription(
                                id.into(),
                                Err("timed out while trying to get a record for the given key"
                                    .into()),
                            );
                        }
                    }
                    PutRecord(Ok(PutRecordOk { key }))
                    | RepublishRecord(Ok(PutRecordOk { key })) => {
                        let key = multibase::encode(Base::Base32Lower, key);
                        debug!("kad: successfully put record {}", key);
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
                        let key = multibase::encode(Base::Base32Lower, key);
                        warn!(
                            "kad: quorum failed ({}) when trying to put record {}",
                            quorum, key
                        );

                        if self.kademlia.query(&id).is_none() {
                            self.kad_subscriptions.finish_subscription(
                                id.into(),
                                Err("kad: quorum failed when trying to put the record".into()),
                            );
                        }
                    }
                    PutRecord(Err(PutRecordError::Timeout {
                        key,
                        success: _,
                        quorum: _,
                    })) => {
                        let key = multibase::encode(Base::Base32Lower, key);
                        warn!("kad: timed out while trying to put record {}", key);

                        if self.kademlia.query(&id).is_none() {
                            self.kad_subscriptions.finish_subscription(
                                id.into(),
                                Err("kad: timed out while trying to put the record".into()),
                            );
                        }
                    }
                    RepublishRecord(Err(PutRecordError::Timeout {
                        key,
                        success: _,
                        quorum: _,
                    })) => {
                        let key = multibase::encode(Base::Base32Lower, key);
                        warn!("kad: timed out while trying to republish record {}", key);
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
                trace!("kad: peer {} is unroutable", peer);
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
                let repo = self.repo.clone();
                let peer_stats = Arc::clone(&self.bitswap.stats.get(&peer_id).unwrap());
                task::spawn(async move {
                    let bytes = block.data().len() as u64;
                    let res = repo.put_block(block.clone()).await;
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
                        }
                    };
                });
            }
            BitswapEvent::ReceivedWant(peer_id, cid, priority) => {
                info!(
                    "Peer {} wants block {} with priority {}",
                    peer_id, cid, priority
                );

                let queued_blocks = self.bitswap().queued_blocks.clone();
                let repo = self.repo.clone();

                task::spawn(async move {
                    match repo.get_block_now(&cid).await {
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
                trace!(
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
                trace!("ping: pong from {}", peer);
            }
            PingEvent {
                peer,
                result: Result::Err(PingFailure::Timeout),
            } => {
                trace!("ping: timeout to {}", peer);
                self.remove_peer(&peer);
            }
            PingEvent {
                peer,
                result: Result::Err(PingFailure::Other { error }),
            } => {
                error!("ping: failure with {}: {}", peer.to_base58(), error);
            }
        }
    }
}

impl<Types: IpfsTypes> NetworkBehaviourEventProcess<IdentifyEvent> for Behaviour<Types> {
    fn inject_event(&mut self, event: IdentifyEvent) {
        trace!("identify: {:?}", event);
    }
}

impl<Types: IpfsTypes> Behaviour<Types> {
    /// Create a Kademlia behaviour with the IPFS bootstrap nodes.
    pub async fn new(options: SwarmOptions, repo: Arc<Repo<Types>>) -> Self {
        info!("net: starting with peer id {}", options.peer_id);

        /*
        let mdns = if options.mdns {
            Some(TokioMdns::new().expect("Failed to create mDNS service"))
        } else {
            None
        }
        .into();
        */

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
        let mut swarm = SwarmApi::default();

        for (addr, _peer_id) in &options.bootstrap {
            if let Ok(addr) = addr.to_owned().try_into() {
                swarm.bootstrappers.insert(addr);
            }
        }

        Behaviour {
            repo,
            // mdns,
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
        self.swarm.add_peer(peer);
        // FIXME: the call below automatically performs a dial attempt
        // to the given peer; it is unsure that we want it done within
        // add_peer, especially since that peer might not belong to the
        // expected identify protocol
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

    pub fn connect(&mut self, addr: MultiaddrWithPeerId) -> Option<SubscriptionFuture<(), String>> {
        self.swarm.connect(addr)
    }

    pub fn disconnect(&mut self, addr: MultiaddrWithPeerId) -> Option<Disconnector> {
        self.swarm.disconnect(addr)
    }

    // FIXME: it would be best if get_providers is called only in case the already connected
    // peers don't have it
    pub fn want_block(&mut self, cid: Cid) {
        let key = cid.hash().as_bytes().to_owned();
        self.kademlia.get_providers(key.into());
        self.bitswap.want_block(cid, 1);
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

    pub fn bootstrap(&mut self) -> Result<SubscriptionFuture<KadResult, String>, anyhow::Error> {
        match self.kademlia.bootstrap() {
            Ok(id) => Ok(self.kad_subscriptions.create_subscription(id.into(), None)),
            Err(e) => {
                error!("kad: can't bootstrap the node: {:?}", e);
                Err(anyhow!("kad: can't bootstrap the node: {:?}", e))
            }
        }
    }

    pub fn kademlia(&mut self) -> &mut Kademlia<MemoryStore> {
        &mut self.kademlia
    }

    pub fn get_closest_peers(&mut self, id: PeerId) -> SubscriptionFuture<KadResult, String> {
        // TODO: why was this base58?
        // let id = id.to_base58();

        self.kad_subscriptions
            .create_subscription(self.kademlia.get_closest_peers(id).into(), None)
    }

    pub fn get_providers(&mut self, cid: Cid) -> SubscriptionFuture<KadResult, String> {
        let key = Key::from(cid.hash().as_bytes().to_owned());
        self.kad_subscriptions
            .create_subscription(self.kademlia.get_providers(key).into(), None)
    }

    pub fn start_providing(
        &mut self,
        cid: Cid,
    ) -> Result<SubscriptionFuture<KadResult, String>, anyhow::Error> {
        let key = Key::from(cid.hash().as_bytes().to_owned());
        match self.kademlia.start_providing(key) {
            Ok(id) => Ok(self.kad_subscriptions.create_subscription(id.into(), None)),
            Err(e) => {
                error!("kad: can't provide a key: {:?}", e);
                Err(anyhow!("kad: can't provide the key: {:?}", e))
            }
        }
    }

    pub fn dht_get(&mut self, key: Key, quorum: Quorum) -> SubscriptionFuture<KadResult, String> {
        self.kad_subscriptions
            .create_subscription(self.kademlia.get_record(&key, quorum).into(), None)
    }

    pub fn dht_put(
        &mut self,
        key: Key,
        value: Vec<u8>,
        quorum: Quorum,
    ) -> Result<SubscriptionFuture<KadResult, String>, anyhow::Error> {
        let record = Record {
            key,
            value,
            publisher: None,
            expires: None,
        };
        match self.kademlia.put_record(record, quorum) {
            Ok(id) => Ok(self.kad_subscriptions.create_subscription(id.into(), None)),
            Err(e) => {
                error!("kad: can't put a record: {:?}", e);
                Err(anyhow!("kad: can't provide the record: {:?}", e))
            }
        }
    }

    pub fn get_bootstrappers(&self) -> Vec<Multiaddr> {
        self.swarm
            .bootstrappers
            .iter()
            .cloned()
            .map(|a| a.into())
            .collect()
    }

    pub fn add_bootstrapper(
        &mut self,
        addr: MultiaddrWithPeerId,
    ) -> Result<Multiaddr, anyhow::Error> {
        let ret = addr.clone().into();
        if self.swarm.bootstrappers.insert(addr.clone()) {
            let MultiaddrWithPeerId {
                multiaddr: ma,
                peer_id,
            } = addr;
            self.kademlia.add_address(&peer_id, ma.into());
            // the return value of add_address doesn't implement Debug
            trace!(peer_id=%peer_id, "tried to add a bootstrapper");
        }
        Ok(ret)
    }

    pub fn remove_bootstrapper(
        &mut self,
        addr: MultiaddrWithPeerId,
    ) -> Result<Multiaddr, anyhow::Error> {
        let ret = addr.clone().into();
        if self.swarm.bootstrappers.remove(&addr) {
            let peer_id = addr.peer_id;
            let prefix: Multiaddr = addr.multiaddr.into();

            if let Some(e) = self.kademlia.remove_address(&peer_id, &prefix) {
                info!(peer_id=%peer_id, status=?e.status, "removed bootstrapper");
            } else {
                warn!(peer_id=%peer_id, "attempted to remove an unknown bootstrapper");
            }
        }
        Ok(ret)
    }

    pub fn clear_bootstrappers(&mut self) -> Vec<Multiaddr> {
        let removed = self.swarm.bootstrappers.drain();
        let mut ret = Vec::with_capacity(removed.len());

        for addr_with_peer_id in removed {
            let peer_id = &addr_with_peer_id.peer_id;
            let prefix: Multiaddr = addr_with_peer_id.multiaddr.clone().into();

            if let Some(e) = self.kademlia.remove_address(peer_id, &prefix) {
                info!(peer_id=%peer_id, status=?e.status, "cleared bootstrapper");
                ret.push(addr_with_peer_id.into());
            } else {
                error!(peer_id=%peer_id, "attempted to clear an unknown bootstrapper");
            }
        }

        ret
    }

    pub fn restore_bootstrappers(&mut self) -> Result<Vec<Multiaddr>, anyhow::Error> {
        let mut ret = Vec::new();

        for addr in BOOTSTRAP_NODES {
            let addr = addr
                .parse::<MultiaddrWithPeerId>()
                .expect("see test bootstrap_nodes_are_multiaddr_with_peerid");
            if self.swarm.bootstrappers.insert(addr.clone()) {
                let MultiaddrWithPeerId {
                    multiaddr: ma,
                    peer_id,
                } = addr.clone();

                // this is intentionally the multiaddr without peerid turned into plain multiaddr:
                // libp2p cannot dial addresses which include peerids.
                let ma: Multiaddr = ma.into();

                // same as with add_bootstrapper: the return value from kademlia.add_address
                // doesn't implement Debug
                self.kademlia.add_address(&peer_id, ma.clone());
                trace!(peer_id=%peer_id, "tried to restore a bootstrapper");

                // report with the peerid
                let reported: Multiaddr = addr.into();
                ret.push(reported);
            }
        }

        Ok(ret)
    }
}

/// Create a IPFS behaviour with the IPFS bootstrap nodes.
pub async fn build_behaviour<TIpfsTypes: IpfsTypes>(
    options: SwarmOptions,
    repo: Arc<Repo<TIpfsTypes>>,
) -> Behaviour<TIpfsTypes> {
    Behaviour::new(options, repo).await
}
