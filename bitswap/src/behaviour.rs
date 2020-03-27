//! Handles the `/ipfs/bitswap/1.0.0` and `/ipfs/bitswap/1.1.0` protocols. This
//! allows exchanging IPFS blocks.
//!
//! # Usage
//!
//! The `Bitswap` struct implements the `NetworkBehaviour` trait. When used, it
//! will allow providing and reciving IPFS blocks.
use crate::block::Block;
use crate::ledger::Ledger;
use crate::message::{BitswapMessage, Priority};
use crate::protocol::BitswapConfig;
use fnv::FnvHashSet;
use futures::task::Context;
use futures::task::Poll;
use libipld::cid::Cid;
use libp2p_core::{ConnectedPoint, Multiaddr, PeerId};
use libp2p_swarm::protocols_handler::{IntoProtocolsHandler, OneShotHandler, ProtocolsHandler};
use libp2p_swarm::{NetworkBehaviour, NetworkBehaviourAction, PollParameters};
use std::collections::{HashMap, VecDeque};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BitswapEvent {
    ReceivedBlock(PeerId, Block),
    ReceivedWant(PeerId, Cid, Priority),
    ReceivedCancel(PeerId, Cid),
}

/// Network behaviour that handles sending and receiving IPFS blocks.
#[derive(Default)]
pub struct Bitswap {
    /// Queue of events to report to the user.
    events: VecDeque<NetworkBehaviourAction<BitswapMessage, BitswapEvent>>,
    /// List of peers to send messages to.
    target_peers: FnvHashSet<PeerId>,
    /// Ledger
    connected_peers: HashMap<PeerId, Ledger>,
    /// Wanted blocks
    wanted_blocks: HashMap<Cid, Priority>,
}

impl Bitswap {
    /// Creates a new `Bitswap`.
    pub fn new() -> Self {
        Default::default()
    }

    fn ledger(&mut self, peer_id: &PeerId) -> &mut Ledger {
        self.connected_peers.get_mut(peer_id).unwrap()
    }

    /// Connect to peer.
    ///
    /// Called from Kademlia behaviour.
    pub fn connect(&mut self, peer_id: PeerId) {
        log::trace!("bitswap: connect");
        if !self.target_peers.insert(peer_id.clone()) {
            return;
        }
        log::trace!("  queuing dial_peer to {}", peer_id.to_base58());
        self.events
            .push_back(NetworkBehaviourAction::DialPeer { peer_id });
    }

    /// Sends a block to the peer.
    ///
    /// Called from a Strategy.
    pub fn send_block(&mut self, peer_id: &PeerId, block: Block) {
        log::trace!("bitswap: send_block");
        log::trace!("  queuing block for {}", peer_id.to_base58());
        self.ledger(peer_id).add_block(block);
    }

    /// Sends the wantlist to the peer.
    fn send_want_list(&mut self, peer_id: &PeerId) {
        log::trace!("bitswap: send_want_list");
        if self.wanted_blocks.is_empty() {
            return;
        }
        log::trace!("  queuing wanted blocks");
        let ledger = self.connected_peers.get_mut(peer_id).unwrap();
        for (cid, priority) in &self.wanted_blocks {
            ledger.want(cid, *priority);
        }
    }

    /// Queues the wanted block for all peers.
    ///
    /// A user request
    pub fn want_block(&mut self, cid: Cid, priority: Priority) {
        log::trace!("bitswap: want_block");
        for (peer_id, ledger) in self.connected_peers.iter_mut() {
            log::trace!("  queuing want for {}", peer_id.to_base58());
            ledger.want(&cid, priority);
        }
        self.wanted_blocks.insert(cid, priority);
    }

    /// Removes the block from our want list and updates all peers.
    ///
    /// Can be either a user request or be called when the block
    /// was received.
    pub fn cancel_block(&mut self, cid: &Cid) {
        log::trace!("bitswap: cancel_block");
        for (peer_id, ledger) in self.connected_peers.iter_mut() {
            log::trace!("  queuing cancel for {}", peer_id.to_base58());
            ledger.cancel(cid);
        }
        self.wanted_blocks.remove(cid);
    }

    /// Retrieves the want list of a peer.
    pub fn wantlist(&self, peer_id: Option<&PeerId>) -> Vec<(Cid, Priority)> {
        if let Some(peer_id) = peer_id {
            self.connected_peers
                .get(peer_id)
                .map(|ledger| ledger.wantlist().collect())
                .unwrap_or_default()
        } else {
            self.wanted_blocks
                .iter()
                .map(|(cid, priority)| (cid.clone(), *priority))
                .collect()
        }
    }

    /// Retrieves the connected bitswap peers.
    pub fn peers(&self) -> Vec<PeerId> {
        self.connected_peers
            .iter()
            .map(|(peer_id, _)| peer_id)
            .cloned()
            .collect()
    }
}

impl NetworkBehaviour for Bitswap {
    type ProtocolsHandler = OneShotHandler<BitswapConfig, BitswapMessage, BitswapMessage>;
    type OutEvent = BitswapEvent;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        log::trace!("bitswap: new_handler");
        Default::default()
    }

    fn addresses_of_peer(&mut self, _peer_id: &PeerId) -> Vec<Multiaddr> {
        log::trace!("bitswap: addresses_of_peer");
        Default::default()
    }

    fn inject_connected(&mut self, peer_id: PeerId, cp: ConnectedPoint) {
        log::trace!("bitswap: inject_connected");
        log::trace!("  peer_id: {}", peer_id.to_base58());
        log::trace!("  connected_point: {:?}", cp);
        let ledger = Ledger::new();
        self.connected_peers.insert(peer_id.clone(), ledger);
        self.send_want_list(&peer_id);
        log::trace!("");
    }

    fn inject_disconnected(&mut self, peer_id: &PeerId, cp: ConnectedPoint) {
        log::trace!("bitswap: inject_disconnected {:?}", cp);
        log::trace!("  peer_id: {}", peer_id.to_base58());
        log::trace!("  connected_point: {:?}", cp);
        log::trace!("");
        self.connected_peers.remove(peer_id);
    }

    fn inject_node_event(&mut self, peer_id: PeerId, message: BitswapMessage) {
        log::trace!("bitswap: inject_node_event");
        log::trace!("  received message");
        log::trace!("{:?}", message);

        // Update the ledger.
        self.ledger(&peer_id).receive(&message);

        // Process incoming messages.
        for block in message.blocks() {
            // Cancel the block.
            self.cancel_block(&block.cid());
            let event = BitswapEvent::ReceivedBlock(peer_id.clone(), block.clone());
            self.events
                .push_back(NetworkBehaviourAction::GenerateEvent(event));
        }
        for (cid, priority) in message.want() {
            let event = BitswapEvent::ReceivedWant(peer_id.clone(), cid.clone(), *priority);
            self.events
                .push_back(NetworkBehaviourAction::GenerateEvent(event));
        }
        for cid in message.cancel() {
            let event = BitswapEvent::ReceivedCancel(peer_id.clone(), cid.clone());
            self.events
                .push_back(NetworkBehaviourAction::GenerateEvent(event));
        }
    }

    #[allow(clippy::type_complexity)]
    fn poll(&mut self, _: &mut Context, _: &mut impl PollParameters)
        -> Poll<NetworkBehaviourAction<<<Self::ProtocolsHandler as IntoProtocolsHandler>::Handler as ProtocolsHandler>::InEvent, Self::OutEvent>>
    {
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(event);
        }
        for (peer_id, ledger) in &mut self.connected_peers {
            if let Some(message) = ledger.send() {
                return Poll::Ready(NetworkBehaviourAction::SendEvent {
                    peer_id: peer_id.clone(),
                    event: message,
                });
            }
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::tests::create_block;
    use futures::channel::mpsc;
    use futures::prelude::*;
    use libp2p::core::muxing::StreamMuxerBox;
    use libp2p::core::transport::boxed::Boxed;
    use libp2p::core::transport::upgrade::Version;
    use libp2p::identity::Keypair;
    use libp2p::secio::SecioConfig;
    use libp2p::tcp::TcpConfig;
    use libp2p::yamux::Config as YamuxConfig;
    use libp2p::{PeerId, Swarm, Transport};
    use std::io::{Error, ErrorKind};
    use std::time::Duration;

    fn mk_transport() -> (PeerId, Boxed<(PeerId, StreamMuxerBox), Error>) {
        let key = Keypair::generate_ed25519();
        let peer_id = key.public().into_peer_id();
        let transport = TcpConfig::new()
            .nodelay(true)
            .upgrade(Version::V1)
            .authenticate(SecioConfig::new(key))
            .multiplex(YamuxConfig::default())
            .timeout(Duration::from_secs(20))
            .map(|(peer_id, muxer), _| (peer_id, StreamMuxerBox::new(muxer)))
            .map_err(|err| Error::new(ErrorKind::Other, err))
            .boxed();
        (peer_id, transport)
    }

    #[async_std::test]
    async fn test_bitswap_behaviour() {
        env_logger::init();

        let (peer1_id, trans) = mk_transport();
        let mut swarm1 = Swarm::new(trans, Bitswap::new(), peer1_id.clone());

        let (peer2_id, trans) = mk_transport();
        let mut swarm2 = Swarm::new(trans, Bitswap::new(), peer2_id.clone());

        let (mut tx, mut rx) = mpsc::channel::<Multiaddr>(1);
        Swarm::listen_on(&mut swarm1, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();

        let block = create_block(b"hello world");
        let cid = block.cid().clone();

        let peer1 = async move {
            while let Some(_) = swarm1.next().now_or_never() {}

            for l in Swarm::listeners(&swarm1) {
                tx.send(l.clone()).await.unwrap();
            }

            loop {
                match swarm1.next().await {
                    BitswapEvent::ReceivedWant(peer_id, cid, _) => {
                        if &cid == block.cid() {
                            swarm1.send_block(&peer_id, block.clone());
                        }
                    }
                    _ => {}
                }
            }
        };

        let peer2 = async move {
            Swarm::dial_addr(&mut swarm2, rx.next().await.unwrap()).unwrap();
            swarm2.want_block(cid, 1000);

            loop {
                match swarm2.next().await {
                    BitswapEvent::ReceivedBlock(_, block) => return block,
                    _ => {}
                }
            }
        };

        let block = future::select(Box::pin(peer1), Box::pin(peer2))
            .await
            .factor_first()
            .0;
        assert_eq!(block.data(), b"hello world");
    }
}
