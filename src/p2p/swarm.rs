use crate::subscription::{SubscriptionFuture, SubscriptionRegistry};
use core::task::{Context, Poll};
use libp2p::core::{connection::ConnectionId, ConnectedPoint, Multiaddr, PeerId};
use libp2p::swarm::protocols_handler::{
    DummyProtocolsHandler, IntoProtocolsHandler, ProtocolsHandler,
};
use libp2p::swarm::{self, NetworkBehaviour, PollParameters, Swarm};
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration;

/// A description of currently active connection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Connection {
    /// The connected peer.
    pub peer_id: PeerId,
    /// Any connecting address of the peer as peers can have multiple connections to
    pub address: Multiaddr,
    /// Latest ping report on any of the connections
    pub rtt: Option<Duration>,
}

/// Disconnected will use banning to disconnect a node. Disconnecting a single peer connection is
/// not supported at the moment.
pub struct Disconnector {
    peer_id: PeerId,
}

impl Disconnector {
    pub fn disconnect<T: NetworkBehaviour>(self, swarm: &mut Swarm<T>)
        where <<<T as NetworkBehaviour>::ProtocolsHandler as IntoProtocolsHandler>::Handler as ProtocolsHandler>::InEvent: std::clone::Clone
    {
        Swarm::ban_peer_id(swarm, self.peer_id.clone());
        Swarm::unban_peer_id(swarm, self.peer_id);
    }
}

// Currently this is swarm::NetworkBehaviourAction<Void, Void>
type NetworkBehaviourAction = swarm::NetworkBehaviourAction<<<<SwarmApi as NetworkBehaviour>::ProtocolsHandler as IntoProtocolsHandler>::Handler as ProtocolsHandler>::InEvent, <SwarmApi as NetworkBehaviour>::OutEvent>;

#[derive(Debug, Default)]
pub struct SwarmApi {
    events: VecDeque<NetworkBehaviourAction>,
    peers: HashSet<PeerId>,
    connect_registry: SubscriptionRegistry<Multiaddr, Result<(), String>>,
    connections: HashMap<Multiaddr, PeerId>,
    roundtrip_times: HashMap<PeerId, Duration>,
    connected_peers: HashMap<PeerId, Vec<Multiaddr>>,
}

impl SwarmApi {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_peer(&mut self, peer_id: PeerId) {
        self.peers.insert(peer_id);
    }

    pub fn peers(&self) -> impl Iterator<Item = &PeerId> {
        self.peers.iter()
    }

    pub fn remove_peer(&mut self, peer_id: &PeerId) {
        self.peers.remove(peer_id);
    }

    pub fn connections(&self) -> impl Iterator<Item = Connection> + '_ {
        self.connected_peers
            .iter()
            .filter_map(move |(peer, conns)| {
                let rtt = self.roundtrip_times.get(peer).cloned();

                if let Some(any) = conns.first() {
                    Some(Connection {
                        peer_id: peer.clone(),
                        address: any.clone(),
                        rtt,
                    })
                } else {
                    None
                }
            })
    }

    pub fn set_rtt(&mut self, peer_id: &PeerId, rtt: Duration) {
        // FIXME: this is for any connection
        self.roundtrip_times.insert(peer_id.clone(), rtt);
    }

    pub fn connect(&mut self, address: Multiaddr) -> SubscriptionFuture<Result<(), String>> {
        log::trace!("starting to connect to {}", address);
        self.events.push_back(NetworkBehaviourAction::DialAddress {
            address: address.clone(),
        });
        self.connect_registry.create_subscription(address)
    }

    pub fn disconnect(&mut self, address: Multiaddr) -> Option<Disconnector> {
        log::trace!("disconnect {}", address);
        // FIXME: closing a single specific connection would be allowed for ProtocolHandlers
        let peer_id = self.connections.get(&address).cloned();

        if let Some(peer_id) = peer_id {
            self.connected_peers.remove(&peer_id);
            Some(Disconnector { peer_id })
        } else {
            None
        }
    }
}

impl NetworkBehaviour for SwarmApi {
    type ProtocolsHandler = DummyProtocolsHandler;
    type OutEvent = void::Void;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        log::trace!("new_handler");
        Default::default()
    }

    fn addresses_of_peer(&mut self, peer_id: &PeerId) -> Vec<Multiaddr> {
        log::trace!("addresses_of_peer {}", peer_id);
        if let Some(addr) = self.connected_peers.get(peer_id).cloned() {
            addr
        } else {
            Default::default()
        }
    }

    fn inject_connection_established(
        &mut self,
        peer_id: &PeerId,
        _id: &ConnectionId,
        cp: &ConnectedPoint,
    ) {
        // TODO: could be that the connection is not yet fully established at this point
        log::trace!("inject_connected {} {:?}", peer_id.to_string(), cp);
        let addr = connection_point_addr(cp);
        self.peers.insert(peer_id.clone());
        let connections = self
            .connected_peers
            .entry(peer_id.clone())
            .or_insert_with(Vec::new);

        connections.push(addr.clone());

        self.connections.insert(addr.clone(), peer_id.clone());
        self.connect_registry.finish_subscription(&addr, Ok(()));
    }

    fn inject_connected(&mut self, _peer_id: &PeerId) {
        // we have at least one fully open connection and handler is running
    }

    fn inject_connection_closed(
        &mut self,
        peer_id: &PeerId,
        _id: &ConnectionId,
        cp: &ConnectedPoint,
    ) {
        log::trace!("inject_disconnected {} {:?}", peer_id.to_string(), cp);
        let closed_addr = connection_point_addr(cp);
        let became_empty = if let Some(connections) = self.connected_peers.get_mut(peer_id) {
            if let Some(index) = connections.iter().position(|addr| addr == closed_addr) {
                connections.swap_remove(index);
            }
            connections.is_empty()
        } else {
            false
        };
        if became_empty {
            self.connected_peers.remove(peer_id);
        }
        self.connections.remove(closed_addr);
        // FIXME: should be an error
        self.connect_registry
            .finish_subscription(closed_addr, Ok(()));
    }

    fn inject_disconnected(&mut self, peer_id: &PeerId) {
        for address in self
            .connected_peers
            .remove(peer_id)
            .into_iter()
            .flat_map(|i| i.into_iter())
        {
            self.connections.remove(&address);
        }
        self.roundtrip_times.remove(peer_id);
    }

    fn inject_event(&mut self, _peer_id: PeerId, _connection: ConnectionId, _event: void::Void) {}

    fn inject_addr_reach_failure(
        &mut self,
        _peer_id: Option<&PeerId>,
        addr: &Multiaddr,
        error: &dyn std::error::Error,
    ) {
        log::trace!("inject_addr_reach_failure {} {}", addr, error);
        self.connect_registry
            .finish_subscription(addr, Err(format!("{}", error)));
    }

    fn poll(
        &mut self,
        _: &mut Context,
        _: &mut impl PollParameters,
    ) -> Poll<NetworkBehaviourAction> {
        if let Some(event) = self.events.pop_front() {
            Poll::Ready(event)
        } else {
            Poll::Pending
        }
    }
}

fn connection_point_addr(cp: &ConnectedPoint) -> &Multiaddr {
    match cp {
        ConnectedPoint::Dialer { address } => address,
        ConnectedPoint::Listener { send_back_addr, .. } => send_back_addr,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::p2p::transport::{build_transport, TTransport};
    use async_std::task;
    use futures::channel::mpsc;
    use futures::future::{select, FutureExt};
    use futures::sink::SinkExt;
    use futures::stream::StreamExt;
    use libp2p::identity::Keypair;
    use libp2p::swarm::Swarm;

    #[test]
    fn swarm_api() {
        env_logger::init();

        let (peer1_id, trans) = mk_transport();
        let mut swarm1 = Swarm::new(trans, SwarmApi::new(), peer1_id);

        let (peer2_id, trans) = mk_transport();
        let mut swarm2 = Swarm::new(trans, SwarmApi::new(), peer2_id);

        let (mut tx, mut rx) = mpsc::channel::<Multiaddr>(1);
        Swarm::listen_on(&mut swarm1, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();

        let peer1 = async move {
            while let Some(_) = swarm1.next().now_or_never() {}

            for l in Swarm::listeners(&swarm1) {
                tx.send(l.clone()).await.unwrap();
            }

            loop {
                swarm1.next().await;
            }
        };

        let peer2 = async move {
            let future = swarm2.connect(rx.next().await.unwrap());

            let poll_swarm = async move {
                loop {
                    swarm2.next().await;
                }
            };

            select(Box::pin(future), Box::pin(poll_swarm)).await;
        };

        let result = select(Box::pin(peer1), Box::pin(peer2));
        task::block_on(result);
    }

    fn mk_transport() -> (PeerId, TTransport) {
        let key = Keypair::generate_ed25519();
        let peer_id = key.public().into_peer_id();
        let transport = build_transport(key);
        (peer_id, transport)
    }
}
