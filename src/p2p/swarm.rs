use crate::subscription::{SubscriptionFuture, SubscriptionRegistry};
use anyhow::anyhow;
use core::task::{Context, Poll};
use libp2p::core::{
    connection::ConnectionId, multiaddr::Protocol, ConnectedPoint, Multiaddr, PeerId,
};
use libp2p::swarm::protocols_handler::{
    DummyProtocolsHandler, IntoProtocolsHandler, ProtocolsHandler,
};
use libp2p::swarm::{self, DialPeerCondition, NetworkBehaviour, PollParameters, Swarm};
use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::TryFrom;
use std::time::Duration;
use std::{fmt, str::FromStr};

/// A wrapper for `Multiaddr` that does **not** contain `Protocol::P2p`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultiaddrWoPeerId(Multiaddr);

impl From<Multiaddr> for MultiaddrWoPeerId {
    fn from(addr: Multiaddr) -> Self {
        Self(
            addr.into_iter()
                .filter(|p| !matches!(p, Protocol::P2p(_)))
                .collect(),
        )
    }
}

impl FromStr for MultiaddrWoPeerId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let multiaddr = s.parse::<Multiaddr>()?;
        Ok(multiaddr.into())
    }
}

/// A `Multiaddr` paired with a discrete `PeerId`. The `Multiaddr` can contain a
/// `Protocol::P2p`, but it's not as easy to work with, and some functionalities
/// don't support it being contained within the `Multiaddr`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MultiaddrWithPeerId {
    multiaddr: Multiaddr,
    peer_id: PeerId,
}

impl From<(Multiaddr, PeerId)> for MultiaddrWithPeerId {
    fn from((multiaddr, peer_id): (Multiaddr, PeerId)) -> Self {
        Self { multiaddr, peer_id }
    }
}

impl TryFrom<Multiaddr> for MultiaddrWithPeerId {
    type Error = anyhow::Error;

    fn try_from(mut multiaddr: Multiaddr) -> Result<Self, Self::Error> {
        if let Some(Protocol::P2p(hash)) = multiaddr.pop() {
            let peer_id = PeerId::from_multihash(hash)
                .map_err(|_| anyhow!("Invalid Multihash in Protocol::P2p"))?;
            Ok(Self { multiaddr, peer_id })
        } else {
            Err(anyhow!("Missing Protocol::P2p in the Multiaddr"))
        }
    }
}

impl FromStr for MultiaddrWithPeerId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let multiaddr = s.parse::<Multiaddr>()?;
        Self::try_from(multiaddr)
    }
}

impl fmt::Display for MultiaddrWithPeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/p2p/{}", self.multiaddr, self.peer_id)
    }
}

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
    connect_registry: SubscriptionRegistry<(), String>,
    connections: HashMap<Multiaddr, PeerId>,
    roundtrip_times: HashMap<PeerId, Duration>,
    connected_peers: HashMap<PeerId, Vec<Multiaddr>>,
}

impl SwarmApi {
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

    pub fn connect(&mut self, mut address: Multiaddr) -> Option<SubscriptionFuture<(), String>> {
        if self.connections.contains_key(&address) {
            return None;
        }

        trace!("Connecting to {:?}", address);

        let subscription = self
            .connect_registry
            .create_subscription(address.clone().into(), None);

        // libp2p currently doesn't support dialing with the P2p protocol
        let peer_id = if let Some(Protocol::P2p(peer_id)) = address.pop() {
            PeerId::from_multihash(peer_id).ok()?
        } else {
            return None;
        };

        if address.iter().next().is_some() {
            self.events
                .push_back(NetworkBehaviourAction::DialAddress { address });
        } else {
            self.events.push_back(NetworkBehaviourAction::DialPeer {
                peer_id,
                condition: DialPeerCondition::Disconnected,
            });
        }

        Some(subscription)
    }

    pub fn disconnect(&mut self, address: Multiaddr) -> Option<Disconnector> {
        trace!("disconnect {}", address);
        // FIXME: closing a single specific connection would be allowed for ProtocolHandlers
        let peer_id = self.connections.remove(&address);

        if let Some(peer_id) = peer_id {
            // wasted some time wondering if the peer should be removed here or not; it should. the
            // API is a bit ackward since we can't tolerate the Disconnector::disconnect **not**
            // being called.
            //
            // there are currently no events being fired from the closing of connections to banned
            // peer, so we need to modify the accounting even before the banning happens.
            self.mark_disconnected(&peer_id);
            Some(Disconnector { peer_id })
        } else {
            None
        }
    }

    fn mark_disconnected(&mut self, peer_id: &PeerId) {
        for address in self.connected_peers.remove(peer_id).into_iter().flatten() {
            self.connections.remove(&address);
        }
        self.roundtrip_times.remove(peer_id);
    }
}

impl NetworkBehaviour for SwarmApi {
    type ProtocolsHandler = DummyProtocolsHandler;
    type OutEvent = void::Void;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        trace!("new_handler");
        Default::default()
    }

    fn addresses_of_peer(&mut self, peer_id: &PeerId) -> Vec<Multiaddr> {
        trace!("addresses_of_peer {}", peer_id);
        self.connected_peers
            .get(peer_id)
            .cloned()
            .unwrap_or_default()
    }

    fn inject_connection_established(
        &mut self,
        peer_id: &PeerId,
        _id: &ConnectionId,
        cp: &ConnectedPoint,
    ) {
        // TODO: could be that the connection is not yet fully established at this point
        trace!("inject_connected {} {:?}", peer_id, cp);
        let mut addr = connection_point_addr(cp).to_owned();

        if !addr
            .iter()
            .any(|protocol| matches!(protocol, Protocol::P2p(_)))
        {
            let protocol = Protocol::P2p(peer_id.to_owned().into());
            addr.push(protocol);
        }

        self.peers.insert(peer_id.clone());
        let connections = self.connected_peers.entry(peer_id.clone()).or_default();

        connections.push(addr.clone());

        self.connections.insert(addr.clone(), peer_id.clone());

        if let ConnectedPoint::Dialer { .. } = cp {
            self.connect_registry
                .finish_subscription(addr.into(), Ok(()));
        }
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
        trace!("inject_connection_closed {} {:?}", peer_id, cp);
        let mut closed_addr = connection_point_addr(cp).to_owned();
        closed_addr.push(Protocol::P2p(peer_id.to_owned().into()));

        let became_empty = if let Some(connections) = self.connected_peers.get_mut(peer_id) {
            if let Some(index) = connections.iter().position(|addr| *addr == closed_addr) {
                connections.swap_remove(index);
            }
            connections.is_empty()
        } else {
            false
        };
        if became_empty {
            self.connected_peers.remove(peer_id);
        }
        self.connections.remove(&closed_addr);

        if let ConnectedPoint::Dialer { .. } = cp {
            self.connect_registry.finish_subscription(
                closed_addr.into(),
                Err("Connection reset by peer".to_owned()),
            );
        }
    }

    fn inject_disconnected(&mut self, peer_id: &PeerId) {
        // in rust-libp2p 0.19 this at least will not be invoked for a peer we boot by banning it.
        trace!("inject_disconnected: {}", peer_id);
        self.mark_disconnected(peer_id);
    }

    fn inject_event(&mut self, _peer_id: PeerId, _connection: ConnectionId, _event: void::Void) {}

    fn inject_addr_reach_failure(
        &mut self,
        _peer_id: Option<&PeerId>,
        addr: &Multiaddr,
        error: &dyn std::error::Error,
    ) {
        trace!("inject_addr_reach_failure {} {}", addr, error);
        self.connect_registry
            .finish_subscription(addr.clone().into(), Err(error.to_string()));
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
    use libp2p::identity::Keypair;
    use libp2p::swarm::Swarm;

    #[test]
    fn connection_targets() {
        let peer_id = "QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ";
        let multiaddr_wo_peer = "/ip4/104.131.131.82/tcp/4001";
        let multiaddr_with_peer = format!("{}/p2p/{}", multiaddr_wo_peer, peer_id);
        let p2p_peer = format!("/p2p/{}", peer_id);
        // note: /ipfs/peer_id doesn't properly parse as a Multiaddr

        assert!(multiaddr_wo_peer.parse::<MultiaddrWoPeerId>().is_ok());
        assert!(multiaddr_with_peer.parse::<MultiaddrWithPeerId>().is_ok());
        assert!(p2p_peer.parse::<Multiaddr>().is_ok());
    }

    #[tokio::test(max_threads = 1)]
    async fn swarm_api() {
        let (peer1_id, trans) = mk_transport();
        let mut swarm1 = Swarm::new(trans, SwarmApi::default(), peer1_id);

        let (peer2_id, trans) = mk_transport();
        let mut swarm2 = Swarm::new(trans, SwarmApi::default(), peer2_id);

        Swarm::listen_on(&mut swarm1, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();

        for l in Swarm::listeners(&swarm1) {
            if let Some(fut) = swarm2.connect(l.to_owned()) {
                fut.await.unwrap();
            }
        }
    }

    fn mk_transport() -> (PeerId, TTransport) {
        let key = Keypair::generate_ed25519();
        let peer_id = key.public().into_peer_id();
        let transport = build_transport(key);
        (peer_id, transport)
    }
}
