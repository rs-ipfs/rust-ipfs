//! Handles the `/ipfs/bitswap/1.0.0` and `/ipfs/bitswap/1.1.0` protocols. This
//! allows exchanging IPFS blocks.
//!
//! # Usage
//!
//! The `Bitswap` struct implements the `NetworkBehaviour` trait. When used, it
//! will allow providing and reciving IPFS blocks.
use crate::bitswap::ledger::{BitswapEvent, Ledger, Message, Priority, I, O};
use crate::bitswap::protocol::BitswapConfig;
use crate::block::{Block, Cid};
use futures::prelude::*;
use libp2p::core::swarm::{
    ConnectedPoint, NetworkBehaviour, NetworkBehaviourAction, PollParameters,
};
use libp2p::core::protocols_handler::{OneShotHandler, ProtocolsHandler};
use libp2p::{Multiaddr, PeerId};
use std::collections::{HashMap, VecDeque};
use std::marker::PhantomData;
use tokio::prelude::*;

/// Network behaviour that handles sending and receiving IPFS blocks.
pub struct Bitswap<TSubstream> {
    /// Marker to pin the generics.
    marker: PhantomData<TSubstream>,
    /// Queue of events to report to the user.
    events: VecDeque<NetworkBehaviourAction<Message<O>, BitswapEvent>>,
    /// Ledger
    peers: HashMap<PeerId, Ledger>,
    /// Wanted blocks
    wanted_blocks: HashMap<Cid, Priority>
}

impl<TSubstream> Bitswap<TSubstream> {
    /// Creates a `Bitswap`.
    pub fn new() -> Self {
        Bitswap {
            marker: PhantomData,
            events: VecDeque::new(),
            peers: HashMap::new(),
            wanted_blocks: HashMap::new(),
        }
    }

    /// Connect to peer.
    ///
    /// Called from Kademlia behaviour.
    pub fn connect(&mut self, peer_id: PeerId) {
        self.events.push_back(NetworkBehaviourAction::DialPeer { peer_id });
    }

    /// Sends a block to the peer.
    ///
    /// Called from a Strategy.
    pub fn send_block(&mut self, peer_id: PeerId, block: Block) {
        let ledger = self.peers.get_mut(&peer_id).expect("Peer not in ledger?!");
        let message = ledger.send_block(block);
        self.events.push_back(NetworkBehaviourAction::SendEvent {
            peer_id,
            event: message,
        });
    }

    /// Queues the wanted block for all peers.
    ///
    /// A user request
    pub fn want_block(&mut self, cid: Cid, priority: u8) {
        for (peer_id, ledger) in self.peers.iter_mut() {
            let message = ledger.want_block(&cid, priority);
            self.events.push_back(NetworkBehaviourAction::SendEvent {
                peer_id: peer_id.to_owned(),
                event: message,
            });
        }
        self.wanted_blocks.insert(cid, priority);
    }

    /// Removes the block from our want list and updates all peers.
    ///
    /// Can be either a user request or be called when the block
    /// was received.
    pub fn cancel_block(&mut self, cid: &Cid) {
        for (peer_id, ledger) in self.peers.iter_mut() {
            ledger.cancel_block(cid);
            let message = ledger.cancel_block(cid);
            if message.is_some() {
                self.events.push_back(NetworkBehaviourAction::SendEvent {
                    peer_id: peer_id.to_owned(),
                    event: message.unwrap(),
                });
            }
        }
        self.wanted_blocks.remove(cid);
    }
}

impl<TSubstream> Default for Bitswap<TSubstream> {
    #[inline]
    fn default() -> Self {
        Bitswap::new()
    }
}

impl<TSubstream> NetworkBehaviour for Bitswap<TSubstream>
where
    TSubstream: AsyncRead + AsyncWrite,
{
    type ProtocolsHandler = OneShotHandler<TSubstream, BitswapConfig, Message<O>, InnerMessage>;
    type OutEvent = BitswapEvent;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        Default::default()
    }

    fn addresses_of_peer(&mut self, _peer_id: &PeerId) -> Vec<Multiaddr> {
        Vec::new()
    }

    fn inject_connected(&mut self, peer_id: PeerId, _: ConnectedPoint) {
        let ledger = Ledger::new();
        let mut message = Message::new();
        for (cid, priority) in &self.wanted_blocks {
            message.want_block(cid, *priority);
        }
        self.events.push_back(NetworkBehaviourAction::SendEvent {
            peer_id: peer_id.clone(),
            event: message,
        });
        self.peers.insert(peer_id, ledger);
    }

    fn inject_disconnected(&mut self, peer_id: &PeerId, _: ConnectedPoint) {
        self.peers.remove(peer_id);
    }

    fn inject_node_event(
        &mut self,
        source: PeerId,
        event: InnerMessage,
    ) {
        let message = match event {
            InnerMessage::Rx(message) => {
                message
            },
            InnerMessage::Tx => {
                return;
            },
        };

        // Update the ledger.
        let ledger = self.peers.get_mut(&source).expect("Peer not in ledger?!");
        ledger.update_incoming_stats(&message);

        // Process incoming messages.
        for block in message.blocks() {
            // Cancel the block.
            self.cancel_block(&block.cid());
            // Add block to received blocks
            self.events.push_back(NetworkBehaviourAction::GenerateEvent(
                BitswapEvent::Block {
                    block: block.to_owned(),
                }));
        }
        for (cid, priority) in message.want() {
            self.events.push_back(NetworkBehaviourAction::GenerateEvent(
                BitswapEvent::Want {
                    peer_id: source.clone(),
                    cid: cid.to_owned(),
                    priority: *priority,
                }));
        }
        // TODO: Remove cancelled `Want` events from the queue.
        // TODO: Remove cancelled blocks from `SendEvent`.
    }

    fn poll(
        &mut self,
        _: &mut PollParameters,
    ) -> Async<NetworkBehaviourAction<
            <Self::ProtocolsHandler as ProtocolsHandler>::InEvent, Self::OutEvent>> {
        // TODO concat messages to same destination to reduce traffic.
        if let Some(event) = self.events.pop_front() {
            if let NetworkBehaviourAction::SendEvent { peer_id, event } = &event {
                let ledger = self.peers.get_mut(&peer_id)
                    .expect("Peer not in ledger?!");
                ledger.update_outgoing_stats(&event);
            }
            return Async::Ready(event);
        }

        Async::NotReady
    }
}

/// Transmission between the `OneShotHandler` and the `BitswapHandler`.
#[derive(Debug)]
pub enum InnerMessage {
    /// We received a `Message` from a remote.
    Rx(Message<I>),
    /// We successfully sent a `Message`.
    Tx,
}

impl From<Message<I>> for InnerMessage {
    #[inline]
    fn from(message: Message<I>) -> InnerMessage {
        InnerMessage::Rx(message)
    }
}

impl From<()> for InnerMessage {
    #[inline]
    fn from(_: ()) -> InnerMessage {
        InnerMessage::Tx
    }
}
