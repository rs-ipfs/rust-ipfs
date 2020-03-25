use crate::error::Error;
use crate::registry::{Channel, LocalRegistry};
use core::task::{Context, Poll};
use libp2p::core::{ConnectedPoint, Multiaddr, PeerId};
use libp2p::swarm::protocols_handler::{
    DummyProtocolsHandler, IntoProtocolsHandler, ProtocolsHandler,
};
use libp2p::swarm::{self, NetworkBehaviour, PollParameters, Swarm};
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct Connection {
    pub peer_id: PeerId,
    pub address: Multiaddr,
    pub rtt: Option<Duration>,
}

pub struct Disconnector {
    peer_id: PeerId,
}

impl Disconnector {
    pub fn disconnect<T: NetworkBehaviour>(self, swarm: &mut Swarm<T>) {
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
    connect_registry: LocalRegistry<Multiaddr, ()>,
    connections: HashMap<Multiaddr, Connection>,
    connected_peers: HashMap<PeerId, Multiaddr>,
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

    pub fn connections(&self) -> impl Iterator<Item = &Connection> {
        self.connections.iter().map(|(_, conn)| conn)
    }

    pub fn set_rtt(&mut self, peer_id: &PeerId, rtt: Duration) {
        if let Some(addr) = self.connected_peers.get(peer_id) {
            if let Some(mut conn) = self.connections.get_mut(addr) {
                conn.rtt = Some(rtt);
            }
        }
    }

    pub fn connect(&mut self, address: Multiaddr, ret: Channel<()>) {
        log::trace!("starting to connect to {}", address);
        self.connect_registry.register(address.clone(), ret);
        self.events
            .push_back(NetworkBehaviourAction::DialAddress { address });
    }

    pub fn disconnect(&mut self, address: Multiaddr) -> Option<Disconnector> {
        log::trace!("disconnect {}", address);
        self.connections.remove(&address);
        let peer_id = self
            .connections
            .get(&address)
            .map(|conn| conn.peer_id.clone());
        if let Some(peer_id) = &peer_id {
            self.connected_peers.remove(peer_id);
        }
        peer_id.map(|peer_id| Disconnector { peer_id })
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
            vec![addr]
        } else {
            Default::default()
        }
    }

    fn inject_connected(&mut self, peer_id: PeerId, cp: ConnectedPoint) {
        log::trace!("inject_connected {} {:?}", peer_id.to_string(), cp);
        let addr = match cp {
            ConnectedPoint::Dialer { address } => address,
            ConnectedPoint::Listener { send_back_addr, .. } => send_back_addr,
        };
        let conn = Connection {
            peer_id: peer_id.clone(),
            address: addr.clone(),
            rtt: None,
        };
        self.peers.insert(peer_id.clone());
        self.connected_peers.insert(peer_id, addr.clone());
        self.connections.insert(addr.clone(), conn);
        self.connect_registry.consume(&addr, &Ok(()));
    }

    fn inject_disconnected(&mut self, peer_id: &PeerId, cp: ConnectedPoint) {
        log::trace!("inject_disconnected {} {:?}", peer_id.to_string(), cp);
        let addr = match cp {
            ConnectedPoint::Dialer { address } => address,
            ConnectedPoint::Listener { send_back_addr, .. } => send_back_addr,
        };
        self.connected_peers.remove(peer_id);
        self.connections.remove(&addr);
    }

    fn inject_node_event(&mut self, _peer_id: PeerId, _event: void::Void) {}

    fn inject_addr_reach_failure(
        &mut self,
        _peer_id: Option<&PeerId>,
        addr: &Multiaddr,
        error: &dyn std::error::Error,
    ) {
        log::trace!("inject_addr_reach_failure {} {}", addr, error);
        self.connect_registry
            .consume(addr, &Err(Error::Connect(format!("{}", error))));
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::p2p::transport::{build_transport, TTransport};
    use futures::channel::oneshot;
    use futures::future::{select, FutureExt};
    use libp2p::identity::Keypair;
    use libp2p::swarm::Swarm;

    #[async_std::test]
    async fn swarm_api() {
        env_logger::init();

        let (peer1_id, trans) = mk_transport();
        let mut swarm1 = Swarm::new(trans, SwarmApi::new(), peer1_id.clone());

        let (peer2_id, trans) = mk_transport();
        let mut swarm2 = Swarm::new(trans, SwarmApi::new(), peer2_id.clone());

        let (tx, rx) = oneshot::channel::<Multiaddr>();
        Swarm::listen_on(&mut swarm1, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();

        let peer1 = async move {
            while let Some(_) = swarm1.next().now_or_never() {}

            let addr = Swarm::listeners(&swarm1).next().unwrap();
            tx.send(addr.clone()).unwrap();

            loop {
                swarm1.next().await;
            }
        };

        let peer2 = async move {
            let (tx, ret) = oneshot::channel();
            swarm2.connect(rx.await.unwrap(), tx);

            let poll_swarm = async move {
                loop {
                    swarm2.next().await;
                }
            };

            select(Box::pin(ret), Box::pin(poll_swarm)).await;
        };

        select(Box::pin(peer1), Box::pin(peer2)).await;
    }

    fn mk_transport() -> (PeerId, TTransport) {
        let key = Keypair::generate_ed25519();
        let peer_id = key.public().into_peer_id();
        let transport = build_transport(key);
        (peer_id, transport)
    }
}
