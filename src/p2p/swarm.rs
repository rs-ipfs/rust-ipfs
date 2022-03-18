use crate::p2p::{MultiaddrWithPeerId, MultiaddrWithoutPeerId};
use crate::subscription::{SubscriptionFuture, SubscriptionRegistry};
use core::task::{Context, Poll};
use libp2p::core::{connection::ConnectionId, ConnectedPoint, Multiaddr, PeerId};
use libp2p::swarm::handler::DummyConnectionHandler;
use libp2p::swarm::{
    self,
    dial_opts::{DialOpts, PeerCondition},
    ConnectionHandler, DialError, NetworkBehaviour, PollParameters, Swarm,
};
use std::collections::{hash_map::Entry, HashMap, HashSet, VecDeque};
use std::convert::{TryFrom, TryInto};
use std::time::Duration;

/// A description of currently active connection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Connection {
    /// The connected peer along with its address.
    pub addr: MultiaddrWithPeerId,
    /// Latest ping report on any of the connections.
    pub rtt: Option<Duration>,
}

/// Disconnected will use banning to disconnect a node. Disconnecting a single peer connection is
/// not supported at the moment.
pub struct Disconnector {
    peer_id: PeerId,
}

impl Disconnector {
    pub fn disconnect<T: NetworkBehaviour>(self, swarm: &mut Swarm<T>) {
        Swarm::ban_peer_id(swarm, self.peer_id);
        Swarm::unban_peer_id(swarm, self.peer_id);
    }
}

// Currently this is swarm::NetworkBehaviourAction<Void, Void>
type NetworkBehaviourAction = swarm::NetworkBehaviourAction<
    <<SwarmApi as NetworkBehaviour>::ConnectionHandler as ConnectionHandler>::OutEvent,
    <SwarmApi as NetworkBehaviour>::ConnectionHandler,
>;

#[derive(Debug, Default)]
pub struct SwarmApi {
    events: VecDeque<NetworkBehaviourAction>,

    // FIXME: anything related to this is probably wrong, and doesn't behave as one would expect
    // from the method names
    peers: HashSet<PeerId>,
    connect_registry: SubscriptionRegistry<(), String>,
    connections: HashMap<MultiaddrWithoutPeerId, PeerId>,
    roundtrip_times: HashMap<PeerId, Duration>,
    connected_peers: HashMap<PeerId, Vec<MultiaddrWithoutPeerId>>,

    /// The connections which have been requested, but the swarm/network is yet to ask for
    /// addresses; currently filled in the order of adding, with the default size of one.
    pending_addresses: HashMap<PeerId, Vec<MultiaddrWithPeerId>>,

    /// The connections which have been requested, and the swarm/network has requested the
    /// addresses of. Used to keep finishing all of the subscriptions.
    pending_connections: HashMap<PeerId, Vec<MultiaddrWithPeerId>>,

    pub(crate) bootstrappers: HashSet<MultiaddrWithPeerId>,
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

                conns.first().map(|any| Connection {
                    addr: MultiaddrWithPeerId::from((any.clone(), *peer)),
                    rtt,
                })
            })
    }

    pub fn set_rtt(&mut self, peer_id: &PeerId, rtt: Duration) {
        // NOTE: this is for any connection
        self.roundtrip_times.insert(*peer_id, rtt);
    }

    pub fn connect(&mut self, addr: MultiaddrWithPeerId) -> Option<SubscriptionFuture<(), String>> {
        let connected_already = self
            .connected_peers
            .get(&addr.peer_id)
            .map(|conns| conns.iter().any(|wo| wo == &addr.multiaddr))
            .unwrap_or(false);

        if connected_already {
            return None;
        }

        trace!("Connecting to {:?}", addr);

        let subscription = self
            .connect_registry
            .create_subscription(addr.clone().into(), None);

        let handler = self.new_handler();
        self.events.push_back(NetworkBehaviourAction::Dial {
            // rationale: this is sort of explicit command, perhaps the old address is no longer
            // valid. Always would be even better but it's bugged at the moment.
            opts: DialOpts::peer_id(addr.peer_id)
                .condition(PeerCondition::NotDialing)
                .build(),
            handler,
        });

        self.pending_addresses
            .entry(addr.peer_id)
            .or_insert_with(|| Vec::with_capacity(1))
            .push(addr);

        Some(subscription)
    }

    pub fn disconnect(&mut self, addr: MultiaddrWithPeerId) -> Option<Disconnector> {
        trace!("request to disconnect {}", addr);
        if let Some(&peer_id) = self.connections.get(&addr.multiaddr) {
            Some(Disconnector { peer_id })
        } else {
            None
        }
    }

    pub fn connections_to(&self, peer_id: &PeerId) -> Vec<Multiaddr> {
        self.connected_peers
            .get(peer_id)
            .cloned()
            .map(|addrs| addrs.into_iter().map(From::from).collect())
            .unwrap_or_default()
    }
}

impl NetworkBehaviour for SwarmApi {
    type ConnectionHandler = DummyConnectionHandler;
    type OutEvent = void::Void;

    fn new_handler(&mut self) -> Self::ConnectionHandler {
        Default::default()
    }

    fn addresses_of_peer(&mut self, peer_id: &PeerId) -> Vec<Multiaddr> {
        // when libp2p starts dialing, it'll collect these from all of known addresses for the peer
        // from the behaviour and dial them all through, ending with calls to inject_connected or
        // inject_addr_reach_failure.
        let addresses = self.pending_addresses.remove(peer_id).unwrap_or_default();

        // store the "given out" addresses as we have created the subscriptions for them
        self.pending_connections
            .entry(*peer_id)
            .or_default()
            .extend(addresses.iter().cloned());

        addresses.into_iter().map(|a| a.into()).collect()
    }

    fn inject_connection_established(
        &mut self,
        peer_id: &PeerId,
        _connection_id: &ConnectionId,
        endpoint: &ConnectedPoint,
        _failed_addresses: Option<&Vec<Multiaddr>>,
        _other_established: usize,
    ) {
        // TODO: could be that the connection is not yet fully established at this point
        trace!("inject_connection_established {} {:?}", peer_id, endpoint);
        let addr = connection_point_addr(endpoint);

        self.peers.insert(*peer_id);
        let connections = self.connected_peers.entry(*peer_id).or_default();
        connections.push(addr.clone());

        let prev = self.connections.insert(addr.clone(), *peer_id);

        if let Some(prev) = prev {
            error!(
                "tracked connection was replaced from {} => {}: {}",
                prev, peer_id, addr
            );
        }

        if let ConnectedPoint::Dialer {
            address,
            role_override: _,
        } = endpoint
        {
            // we dialed to the `address`
            match self.pending_connections.entry(*peer_id) {
                Entry::Occupied(mut oe) => {
                    let addresses = oe.get_mut();
                    let address: MultiaddrWithPeerId = address
                        .clone()
                        .try_into()
                        .expect("dialed address contains peerid in libp2p 0.38");
                    let just_connected = addresses.iter().position(|x| *x == address);
                    if let Some(just_connected) = just_connected {
                        addresses.swap_remove(just_connected);
                        if addresses.is_empty() {
                            oe.remove();
                        }

                        self.connect_registry
                            .finish_subscription(address.into(), Ok(()));
                    }
                }
                Entry::Vacant(_) => {
                    // we not connecting to this peer through this api, must be libp2p_kad or
                    // something else.
                }
            }
        }

        // we have at least one fully open connection and handler is running
        //
        // just finish all of the subscriptions that remain.
        trace!("inject connected {}", peer_id);

        let all_subs = self
            .pending_addresses
            .remove(peer_id)
            .unwrap_or_default()
            .into_iter()
            .chain(
                self.pending_connections
                    .remove(peer_id)
                    .unwrap_or_default()
                    .into_iter(),
            );

        for addr in all_subs {
            // fail the other than already connected subscriptions in
            // inject_connection_established. while the whole swarmapi is quite unclear on the
            // actual use cases, assume that connecting one is good enough for all outstanding
            // connection requests.
            self.connect_registry.finish_subscription(
                addr.into(),
                Err("finished connecting to another address".into()),
            );
        }
    }

    fn inject_connection_closed(
        &mut self,
        peer_id: &PeerId,
        _id: &ConnectionId,
        endpoint: &ConnectedPoint,
        _handler: Self::ConnectionHandler,
        _remaining_established: usize,
    ) {
        trace!("inject_connection_closed {} {:?}", peer_id, endpoint);
        let closed_addr = connection_point_addr(endpoint);

        match self.connected_peers.entry(*peer_id) {
            Entry::Occupied(mut oe) => {
                let connections = oe.get_mut();
                let pos = connections.iter().position(|addr| *addr == closed_addr);

                if let Some(pos) = pos {
                    connections.swap_remove(pos);
                }

                if connections.is_empty() {
                    oe.remove();
                }
            }

            Entry::Vacant(_) => {}
        }

        let removed = self.connections.remove(&closed_addr);

        debug_assert!(
            removed.is_some(),
            "connection was not tracked but it should had been: {}",
            closed_addr
        );

        if let ConnectedPoint::Dialer { .. } = endpoint {
            let addr = MultiaddrWithPeerId::from((closed_addr, peer_id.to_owned()));

            match self.pending_connections.entry(*peer_id) {
                Entry::Occupied(mut oe) => {
                    let connections = oe.get_mut();
                    let pos = connections.iter().position(|x| addr == *x);

                    if let Some(pos) = pos {
                        connections.swap_remove(pos);

                        // this needs to be guarded, so that the connect test case doesn't cause a
                        // panic following inject_connection_established, inject_connection_closed
                        // if there's only the DummyConnectionHandler, which doesn't open a
                        // substream and closes up immediatedly.
                        self.connect_registry.finish_subscription(
                            addr.into(),
                            Err("Connection reset by peer".to_owned()),
                        );
                    }

                    if connections.is_empty() {
                        oe.remove();
                    }
                }
                Entry::Vacant(_) => {}
            }
        } else {
            // we were not dialing to the peer, thus we cannot have a pending subscription to
            // finish.
        }

        trace!("inject_disconnected: {}", peer_id);
        assert!(!self.connected_peers.contains_key(peer_id));
        self.roundtrip_times.remove(peer_id);

        let failed = self
            .pending_addresses
            .remove(peer_id)
            .unwrap_or_default()
            .into_iter()
            .chain(
                self.pending_connections
                    .remove(peer_id)
                    .unwrap_or_default()
                    .into_iter(),
            );

        for addr in failed {
            self.connect_registry
                .finish_subscription(addr.into(), Err("disconnected".into()));
        }
    }

    fn inject_event(&mut self, _peer_id: PeerId, _connection: ConnectionId, _event: void::Void) {}

    fn inject_dial_failure(
        &mut self,
        peer_id: Option<PeerId>,
        _handler: Self::ConnectionHandler,
        error: &DialError,
    ) {
        trace!("inject_dial_failure: {:?} ({})", peer_id, error);
        if let Some(peer_id) = peer_id {
            if self.pending_addresses.contains_key(&peer_id) {
                // it is possible that these addresses have not been tried yet; they will be asked
                // for soon.
                let handler = self.new_handler();
                self.events.push_back(swarm::NetworkBehaviourAction::Dial {
                    opts: DialOpts::peer_id(peer_id)
                        .condition(PeerCondition::NotDialing)
                        .build(),
                    handler,
                });
            }

            // this should not be executed once, but probably will be in case unsupported addresses or something
            // surprising happens.
            for failed in self
                .pending_connections
                .remove(&peer_id)
                .unwrap_or_default()
            {
                self.connect_registry
                    .finish_subscription(failed.into(), Err("addresses exhausted".into()));
            }
        }
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

fn connection_point_addr(cp: &ConnectedPoint) -> MultiaddrWithoutPeerId {
    match cp {
        ConnectedPoint::Dialer {
            address,
            role_override: _,
        } => MultiaddrWithPeerId::try_from(address.to_owned())
            .expect("dialed address contains peerid in libp2p 0.38")
            .into(),
        ConnectedPoint::Listener { send_back_addr, .. } => send_back_addr
            .to_owned()
            .try_into()
            .expect("send back address does not contain peerid in libp2p 0.38"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::p2p::transport::build_transport;
    use futures::{
        stream::{StreamExt, TryStreamExt},
        TryFutureExt,
    };
    use libp2p::identity::Keypair;
    use libp2p::swarm::SwarmEvent;
    use libp2p::{multiaddr::Protocol, multihash::Multihash, swarm::Swarm, swarm::SwarmBuilder};
    use std::convert::TryInto;

    #[tokio::test]
    async fn swarm_api() {
        let (peer1_id, mut swarm1) = build_swarm();
        let (peer2_id, mut swarm2) = build_swarm();

        Swarm::listen_on(&mut swarm1, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();

        loop {
            if let Some(SwarmEvent::NewListenAddr { .. }) = swarm1.next().await {
                break;
            }
        }

        let listeners = Swarm::listeners(&swarm1).cloned().collect::<Vec<_>>();

        for mut addr in listeners {
            addr.push(Protocol::P2p(
                Multihash::from_bytes(&peer1_id.to_bytes()).unwrap(),
            ));

            let mut sub = swarm2
                .behaviour_mut()
                .connect(addr.try_into().unwrap())
                .unwrap();

            loop {
                tokio::select! {
                    _ = (&mut swarm1).next() => {},
                    _ = (&mut swarm2).next() => {},
                    res = (&mut sub) => {
                        // this is currently a success even though the connection is never really
                        // established, the DummyConnectionHandler doesn't do anything nor want the
                        // connection to be kept alive and thats it.
                        //
                        // it could be argued that this should be `Err("keepalive disconnected")`
                        // or something and I'd agree, but I also agree this can be an `Ok(())`;
                        // it's the sort of difficulty with the cli functionality in general: what
                        // does it mean to connect to a peer? one way to look at it would be to
                        // make the peer a "pinned peer" or "friend" and to keep the connection
                        // alive at all costs. perhaps that is something for the next round.
                        // another aspect would be to fail this future because there was no
                        // `inject_connected`, only `inject_connection_established`. taking that
                        // route would be good; it does however leave the special case of adding
                        // another connection, which does add even more complexity than it exists
                        // at the present.
                        res.unwrap();

                        // just to confirm that there are no connections.
                        assert_eq!(Vec::<Multiaddr>::new(), swarm1.behaviour().connections_to(&peer2_id));
                        break;
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn wrong_peerid() {
        let (_, mut swarm1) = build_swarm();
        let (_, mut swarm2) = build_swarm();

        let peer3_id = Keypair::generate_ed25519().public().to_peer_id();

        Swarm::listen_on(&mut swarm1, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();

        let addr;

        loop {
            if let Some(SwarmEvent::NewListenAddr { address, .. }) = swarm1.next().await {
                // wonder if there should be a timeout?
                addr = address;
                break;
            }
        }

        let mut fut = swarm2
            .behaviour_mut()
            .connect(
                MultiaddrWithoutPeerId::try_from(addr)
                    .unwrap()
                    .with(peer3_id),
            )
            .unwrap()
            // remove the private type wrapper
            .map_err(|e| e.into_inner());

        loop {
            tokio::select! {
                _ = swarm1.next() => {},
                _ = swarm2.next() => {},
                res = &mut fut => {
                    assert_eq!(res.unwrap_err(), Some("Pending connection: Invalid peer ID.".into()));
                    return;
                }
            }
        }
    }

    #[tokio::test]
    async fn racy_connecting_attempts() {
        let (peer1_id, mut swarm1) = build_swarm();
        let (_, mut swarm2) = build_swarm();

        Swarm::listen_on(&mut swarm1, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();
        Swarm::listen_on(&mut swarm1, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();

        let mut addr = Vec::with_capacity(2);

        while addr.len() < 2 {
            if let Some(SwarmEvent::NewListenAddr { address, .. }) = swarm1.next().await {
                addr.push(address);
            }
        }

        let targets = (
            MultiaddrWithoutPeerId::try_from(addr[0].clone())
                .unwrap()
                .with(peer1_id),
            MultiaddrWithoutPeerId::try_from(addr[1].clone())
                .unwrap()
                .with(peer1_id),
        );

        let mut connections = futures::stream::FuturesOrdered::new();
        // these two should be attempted in parallel. since we know both of them work, and they are
        // given in this order, we know that in libp2p 0.34 only the first should win, however
        // both should always be finished.
        connections.push(swarm2.behaviour_mut().connect(targets.0).unwrap());
        connections.push(swarm2.behaviour_mut().connect(targets.1).unwrap());
        let ready = connections
            // turn the private error type into Option
            .map_err(|e| e.into_inner())
            .collect::<Vec<_>>();

        tokio::pin!(ready);

        loop {
            tokio::select! {
                _ = swarm1.next() => {}
                _ = swarm2.next() => {}
                res = &mut ready => {

                    assert_eq!(
                        res,
                        vec![
                            Ok(()),
                            Err(Some("finished connecting to another address".into()))
                        ]);

                    break;
                }
            }
        }
    }

    fn build_swarm() -> (PeerId, libp2p::swarm::Swarm<SwarmApi>) {
        let key = Keypair::generate_ed25519();
        let peer_id = key.public().to_peer_id();
        let transport = build_transport(key).unwrap();

        let swarm = SwarmBuilder::new(transport, SwarmApi::default(), peer_id)
            .executor(Box::new(ThreadLocalTokio))
            .build();
        (peer_id, swarm)
    }

    use std::future::Future;
    use std::pin::Pin;

    // can only be used from within tokio context. this is required since otherwise libp2p-tcp will
    // use tokio, but from a futures-executor threadpool, which is outside of tokio context.
    struct ThreadLocalTokio;

    impl libp2p::core::Executor for ThreadLocalTokio {
        fn exec(&self, future: Pin<Box<dyn Future<Output = ()> + Send + 'static>>) {
            tokio::task::spawn(future);
        }
    }
}
