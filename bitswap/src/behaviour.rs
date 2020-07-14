//! Handles the `/ipfs/bitswap/1.0.0` and `/ipfs/bitswap/1.1.0` protocols. This
//! allows exchanging IPFS blocks.
//!
//! # Usage
//!
//! The `Bitswap` struct implements the `NetworkBehaviour` trait. When used, it
//! will allow providing and reciving IPFS blocks.
use crate::block::Block;
use crate::ledger::{Ledger, Message, Priority, Stats};
use crate::protocol::BitswapConfig;
use cid::Cid;
use fnv::FnvHashSet;
use futures::task::Context;
use futures::task::Poll;
use libp2p_core::{connection::ConnectionId, Multiaddr, PeerId};
use libp2p_swarm::protocols_handler::{IntoProtocolsHandler, OneShotHandler, ProtocolsHandler};
use libp2p_swarm::{
    DialPeerCondition, NetworkBehaviour, NetworkBehaviourAction, NotifyHandler, PollParameters,
};
use std::{
    collections::{HashMap, VecDeque},
    mem,
    sync::{Arc, Mutex},
};

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
    events: VecDeque<NetworkBehaviourAction<Message, BitswapEvent>>,
    /// List of peers to send messages to.
    target_peers: FnvHashSet<PeerId>,
    /// Ledger
    pub connected_peers: HashMap<PeerId, Ledger>,
    /// Wanted blocks
    wanted_blocks: HashMap<Cid, Priority>,
    /// Blocks queued to be sent
    pub queued_blocks: Arc<Mutex<Vec<(PeerId, Block)>>>,
}

impl Bitswap {
    /// Return the wantlist of the local node
    pub fn local_wantlist(&self) -> Vec<(Cid, Priority)> {
        self.wanted_blocks
            .iter()
            .map(|(cid, prio)| (cid.clone(), *prio))
            .collect()
    }

    /// Return the wantlist of a peer, if known
    pub fn peer_wantlist(&self, peer: &PeerId) -> Option<Vec<(Cid, Priority)>> {
        self.connected_peers.get(peer).map(Ledger::wantlist)
    }

    pub fn stats(&self) -> Stats {
        // we currently do not remove ledgers so this is ... good enough
        self.connected_peers
            .values()
            .fold(Stats::default(), |acc, peer_ledger| {
                acc.add_assign(&peer_ledger.stats);
                acc
            })
    }

    pub fn peers(&self) -> Vec<PeerId> {
        self.connected_peers.keys().cloned().collect()
    }

    /// Connect to peer.
    ///
    /// Called from Kademlia behaviour.
    pub fn connect(&mut self, peer_id: PeerId) {
        debug!("bitswap: connect");
        if self.target_peers.insert(peer_id.clone()) {
            debug!("  queuing dial_peer to {}", peer_id.to_base58());
            self.events.push_back(NetworkBehaviourAction::DialPeer {
                peer_id,
                condition: DialPeerCondition::Disconnected,
            });
        }
    }

    /// Sends a block to the peer.
    ///
    /// Called from a Strategy.
    pub fn send_block(&mut self, peer_id: PeerId, block: Block) {
        debug!("bitswap: send_block");
        let ledger = self
            .connected_peers
            .get_mut(&peer_id)
            .expect("Peer not in ledger?!");
        ledger.add_block(block);
        debug!("  queuing block for {}", peer_id.to_base58());
    }

    /// Sends the wantlist to the peer.
    fn send_want_list(&mut self, peer_id: PeerId) {
        debug!("bitswap: send_want_list");
        if !self.wanted_blocks.is_empty() {
            let mut message = Message::default();
            for (cid, priority) in &self.wanted_blocks {
                message.want_block(cid, *priority);
            }
            debug!("  queuing wanted blocks");
            self.events
                .push_back(NetworkBehaviourAction::NotifyHandler {
                    peer_id,
                    event: message,
                    handler: NotifyHandler::Any,
                });
        }
    }

    /// Queues the wanted block for all peers.
    ///
    /// A user request
    pub fn want_block(&mut self, cid: Cid, priority: Priority) {
        debug!("bitswap: want_block");
        for (peer_id, ledger) in self.connected_peers.iter_mut() {
            ledger.want_block(&cid, priority);
            debug!("  queuing want for {}", peer_id.to_base58());
        }
        self.wanted_blocks.insert(cid, priority);
        debug!("");
    }

    /// Removes the block from our want list and updates all peers.
    ///
    /// Can be either a user request or be called when the block
    /// was received.
    pub fn cancel_block(&mut self, cid: &Cid) {
        debug!("bitswap: cancel_block");
        for (_peer_id, ledger) in self.connected_peers.iter_mut() {
            ledger.cancel_block(cid);
        }
        self.wanted_blocks.remove(cid);
        debug!("");
    }
}

impl NetworkBehaviour for Bitswap {
    type ProtocolsHandler = OneShotHandler<BitswapConfig, Message, Message>;
    type OutEvent = BitswapEvent;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        debug!("bitswap: new_handler");
        Default::default()
    }

    fn addresses_of_peer(&mut self, _peer_id: &PeerId) -> Vec<Multiaddr> {
        debug!("bitswap: addresses_of_peer");
        Vec::new()
    }

    fn inject_connected(&mut self, peer_id: &PeerId) {
        debug!("bitswap: inject_connected {}", peer_id);
        let ledger = Ledger::new();
        self.connected_peers.insert(peer_id.clone(), ledger);
        self.send_want_list(peer_id.clone());
    }

    fn inject_disconnected(&mut self, peer_id: &PeerId) {
        debug!("bitswap: inject_disconnected {:?}", peer_id);
        //self.connected_peers.remove(peer_id);
    }

    fn inject_event(&mut self, source: PeerId, connection: ConnectionId, mut message: Message) {
        debug!(
            "bitswap: inject_event from {}/{:?}",
            source.to_base58(),
            connection
        );
        debug!("{:?}", message);

        let ledger = self
            .connected_peers
            .get_mut(&source)
            .expect("Peer not in ledger?!");

        // Process the incoming cancel list.
        for cid in message.cancel() {
            ledger.received_want_list.remove(cid);

            let event = BitswapEvent::ReceivedCancel(source.clone(), cid.clone());
            self.events
                .push_back(NetworkBehaviourAction::GenerateEvent(event));
        }

        // Process the incoming wantlist.
        for (cid, priority) in message.want() {
            ledger.received_want_list.insert(cid.to_owned(), *priority);

            let event = BitswapEvent::ReceivedWant(source.clone(), cid.clone(), *priority);
            self.events
                .push_back(NetworkBehaviourAction::GenerateEvent(event));
        }

        // Process the incoming blocks.
        for block in mem::take(&mut message.blocks) {
            self.cancel_block(&block.cid());

            let event = BitswapEvent::ReceivedBlock(source.clone(), block);
            self.events
                .push_back(NetworkBehaviourAction::GenerateEvent(event));
        }

        debug!("");
    }

    #[allow(clippy::type_complexity)]
    fn poll(&mut self, _: &mut Context, _: &mut impl PollParameters)
        -> Poll<NetworkBehaviourAction<<<Self::ProtocolsHandler as IntoProtocolsHandler>::Handler as ProtocolsHandler>::InEvent, Self::OutEvent>>
    {
        let queued_blocks = mem::take(&mut *self.queued_blocks.lock().unwrap());
        for (peer_id, block) in queued_blocks.into_iter() {
            self.send_block(peer_id, block);
        }

        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(event);
        }
        for (peer_id, ledger) in &mut self.connected_peers {
            if let Some(message) = ledger.send() {
                return Poll::Ready(NetworkBehaviourAction::NotifyHandler {
                    peer_id: peer_id.clone(),
                    handler: NotifyHandler::Any,
                    event: message,
                });
            }
        }
        Poll::Pending
    }
}
