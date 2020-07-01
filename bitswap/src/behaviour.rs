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
use crate::strategy::{AltruisticStrategy, StrategyEvent};
use cid::Cid;
use fnv::FnvHashSet;
use futures::task::Context;
use futures::task::Poll;
use libp2p_core::{connection::ConnectionId, Multiaddr, PeerId};
use libp2p_swarm::protocols_handler::{IntoProtocolsHandler, OneShotHandler, ProtocolsHandler};
use libp2p_swarm::{
    DialPeerCondition, NetworkBehaviour, NetworkBehaviourAction, NotifyHandler, PollParameters,
};
use std::collections::{HashMap, VecDeque};

/// Network behaviour that handles sending and receiving IPFS blocks.
pub struct Bitswap {
    /// Queue of events to report to the user.
    events: VecDeque<NetworkBehaviourAction<Message, ()>>,
    /// List of peers to send messages to.
    target_peers: FnvHashSet<PeerId>,
    /// Ledger
    connected_peers: HashMap<PeerId, Ledger>,
    /// Wanted blocks
    wanted_blocks: HashMap<Cid, Priority>,
    // TODO: remove the strategy
    strategy: AltruisticStrategy,
}

impl Bitswap {
    /// Creates a `Bitswap`.
    pub fn new(strategy: AltruisticStrategy) -> Self {
        debug!("bitswap: new");
        Bitswap {
            events: Default::default(),
            target_peers: Default::default(),
            connected_peers: Default::default(),
            wanted_blocks: Default::default(),
            strategy,
        }
    }

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
            .fold(Stats::default(), |mut acc, ledger| {
                acc.sent_blocks += ledger.stats.sent_blocks;
                acc.sent_data += ledger.stats.sent_data;
                acc.received_blocks += ledger.stats.received_blocks;
                acc.received_data += ledger.stats.received_data;
                acc.duplicate_blocks += ledger.stats.duplicate_blocks;
                acc.duplicate_data += ledger.stats.duplicate_data;
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
        let message = ledger.send_block(block);
        debug!("  queuing block for {}", peer_id.to_base58());
        self.events
            .push_back(NetworkBehaviourAction::NotifyHandler {
                peer_id,
                event: message,
                handler: NotifyHandler::Any,
            });
        debug!("");
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
            let message = ledger.want_block(&cid, priority);
            debug!("  queuing want for {}", peer_id.to_base58());
            self.events
                .push_back(NetworkBehaviourAction::NotifyHandler {
                    peer_id: peer_id.to_owned(),
                    event: message,
                    handler: NotifyHandler::Any,
                });
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
        for (peer_id, ledger) in self.connected_peers.iter_mut() {
            let message = ledger.cancel_block(cid);
            if let Some(event) = message {
                let peer_id = peer_id.to_owned();
                debug!("  queuing cancel for {}", peer_id.to_base58());
                self.events
                    .push_back(NetworkBehaviourAction::NotifyHandler {
                        peer_id,
                        event,
                        handler: NotifyHandler::Any,
                    });
            }
        }
        self.wanted_blocks.remove(cid);
        debug!("");
    }
}

impl NetworkBehaviour for Bitswap {
    type ProtocolsHandler = OneShotHandler<BitswapConfig, Message, InnerMessage>;
    type OutEvent = ();

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

    fn inject_event(&mut self, source: PeerId, _connection: ConnectionId, event: InnerMessage) {
        debug!("bitswap: inject_node_event");
        debug!("{:?}", event);
        let message = match event {
            InnerMessage::Rx(message) => message,
            InnerMessage::Tx => {
                return;
            }
        };
        debug!("  received message");

        let ledger = self
            .connected_peers
            .get_mut(&source)
            .expect("Peer not in ledger?!");
        ledger.update_incoming_stats(&message);

        // Process incoming messages.
        for block in message.blocks() {
            // Cancel the block.
            self.cancel_block(&block.cid());
            self.strategy
                .process_block(source.clone(), block.to_owned());
        }
        for (cid, priority) in message.want() {
            self.strategy
                .process_want(source.clone(), cid.to_owned(), *priority);
        }
        // TODO: Remove cancelled `Want` events from the queue.
        // TODO: Remove cancelled blocks from `NotifyHandler`.
        debug!("");
    }

    #[allow(clippy::type_complexity)]
    fn poll(&mut self, ctx: &mut Context, _: &mut impl PollParameters)
        -> Poll<NetworkBehaviourAction<<<Self::ProtocolsHandler as IntoProtocolsHandler>::Handler as ProtocolsHandler>::InEvent, Self::OutEvent>>
    {
        // TODO concat messages to same destination to reduce traffic.
        if let Some(event) = self.events.pop_front() {
            if let NetworkBehaviourAction::NotifyHandler {
                peer_id,
                event,
                handler,
            } = event
            {
                match self.connected_peers.get_mut(&peer_id) {
                    None => {
                        debug!("  requeueing send event to {}", peer_id);
                        // FIXME: I wonder if this should be
                        self.events
                            .push_back(NetworkBehaviourAction::NotifyHandler {
                                peer_id,
                                event,
                                handler,
                            })
                    }
                    Some(ref mut ledger) => {
                        // FIXME: this is a bit early to update stats as the block hasn't been sent
                        // to anywhere at this point.
                        ledger.update_outgoing_stats(&event);
                        debug!("  send_message to {}", peer_id);
                        return Poll::Ready(NetworkBehaviourAction::NotifyHandler {
                            peer_id,
                            event,
                            handler,
                        });
                    }
                }
            } else {
                debug!("{:?}", event);
                debug!("");
                return Poll::Ready(event);
            }
        }

        let inner = match self.strategy.poll(ctx) {
            Poll::Ready(Some(inner)) => inner,
            Poll::Ready(None) => return Poll::Pending,
            Poll::Pending => {
                return Poll::Pending;
            }
        };

        match inner {
            StrategyEvent::Send { peer_id, block } => self.send_block(peer_id, block),
            StrategyEvent::NewBlockStored { source, bytes } => {
                if let Some(ledger) = self.connected_peers.get_mut(&source) {
                    ledger.update_incoming_stored(bytes)
                }
            }
            StrategyEvent::DuplicateBlockReceived { source, bytes } => {
                if let Some(ledger) = self.connected_peers.get_mut(&source) {
                    ledger.update_incoming_duplicate(bytes)
                }
            }
        }

        Poll::Pending
    }
}

/// Transmission between the `OneShotHandler` and the `BitswapHandler`.
#[derive(Debug)]
pub enum InnerMessage {
    /// We received a `Message` from a remote.
    Rx(Message),
    /// We successfully sent a `Message`.
    Tx,
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
        InnerMessage::Tx
    }
}
