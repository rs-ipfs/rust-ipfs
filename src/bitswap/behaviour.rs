//! Handles the `/ipfs/bitswap/1.0.0` and `/ipfs/bitswap/1.1.0` protocols. This
//! allows exchanging IPFS blocks.
//!
//! # Usage
//!
//! The `Bitswap` struct implements the `NetworkBehaviour` trait. When used, it
//! will allow providing and reciving IPFS blocks.
use crate::bitswap::ledger::{BitswapEvent, Ledger, Message};
use crate::bitswap::protocol::BitswapConfig;
use futures::prelude::*;
use libp2p::core::swarm::{
    ConnectedPoint, NetworkBehaviour, NetworkBehaviourAction, PollParameters,
};
use libp2p::core::protocols_handler::{OneShotHandler, ProtocolsHandler};
use libp2p::{Multiaddr, PeerId};
use std::collections::VecDeque;
use std::marker::PhantomData;
use tokio::prelude::*;

/// Network behaviour that handles sending and receiving IPFS blocks.
pub struct Bitswap<TSubstream> {
    /// Marker to pin the generics.
    marker: PhantomData<TSubstream>,
    /// Queue of events to report to the user.
    events: VecDeque<NetworkBehaviourAction<Message, BitswapEvent>>,
    /// Ledger
    ledger: Ledger,
}

impl<TSubstream> Bitswap<TSubstream> {
    /// Creates a `Bitswap`.
    pub fn new() -> Self {
        Bitswap {
            marker: PhantomData,
            events: VecDeque::new(),
            ledger: Ledger::new(),
        }
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
    type ProtocolsHandler = OneShotHandler<TSubstream, BitswapConfig, Message, InnerMessage>;
    type OutEvent = BitswapEvent;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        Default::default()
    }

    fn addresses_of_peer(&mut self, _peer_id: &PeerId) -> Vec<Multiaddr> {
        Vec::new()
    }

    fn inject_connected(&mut self, peer_id: PeerId, _: ConnectedPoint) {
        self.ledger.peer_connected(peer_id);
    }

    fn inject_disconnected(&mut self, peer_id: &PeerId, _: ConnectedPoint) {
        self.ledger.peer_disconnected(peer_id);
    }

    fn inject_node_event(
        &mut self,
        source: PeerId,
        event: InnerMessage,
    ) {
        // We ignore successful send events.
        let message = match event {
            InnerMessage::Rx(message) => message,
            InnerMessage::Sent => return,
        };

        for event in self.ledger.receive_message(&source, message) {
            self.events.push_back(NetworkBehaviourAction::GenerateEvent(event));
        }
    }

    fn poll(
        &mut self,
        _: &mut PollParameters,
    ) -> Async<NetworkBehaviourAction<
            <Self::ProtocolsHandler as ProtocolsHandler>::InEvent, Self::OutEvent>> {
        if let Some(event) = self.events.pop_front() {
            return Async::Ready(event);
        }

        Async::NotReady
    }
}

/// Transmission between the `OneShotHandler` and the `BitswapHandler`.
pub enum InnerMessage {
    /// We received a `Message` from a remote.
    Rx(Message),
    /// We successfully sent a `Message`.
    Sent,
}

impl From<Message> for InnerMessage {
    #[inline]
    fn from(message: Message) -> InnerMessage {
        InnerMessage::Rx(message)
    }
}

impl From<()> for InnerMessage {
    #[inline]
    fn from(_: ()) -> InnerMessage {
        InnerMessage::Sent
    }
}
