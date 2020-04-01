//! Handles the `/ipfs/bitswap/1.0.0` and `/ipfs/bitswap/1.1.0` protocols. This
//! allows exchanging IPFS blocks.
//!
//! # Usage
//!
//! The `Bitswap` struct implements the `NetworkBehaviour` trait. When used, it
//! will allow providing and reciving IPFS blocks.
use crate::block::Block;
use crate::ledger::{Ledger, Message, Priority, I, O};
use crate::protocol::BitswapConfig;
use crate::strategy::{Strategy, StrategyEvent};
use fnv::FnvHashSet;
use futures::task::Context;
use futures::task::Poll;
use libipld::cid::Cid;
use libp2p_core::{ConnectedPoint, Multiaddr, PeerId};
use libp2p_swarm::protocols_handler::{IntoProtocolsHandler, OneShotHandler, ProtocolsHandler};
use libp2p_swarm::{NetworkBehaviour, NetworkBehaviourAction, PollParameters};
use std::collections::{HashMap, VecDeque};

/// Network behaviour that handles sending and receiving IPFS blocks.
pub struct Bitswap<TStrategy> {
    /// Queue of events to report to the user.
    events: VecDeque<NetworkBehaviourAction<Message<O>, ()>>,
    /// List of peers to send messages to.
    target_peers: FnvHashSet<PeerId>,
    /// Ledger
    connected_peers: HashMap<PeerId, Ledger>,
    /// Wanted blocks
    wanted_blocks: HashMap<Cid, Priority>,
    /// Strategy
    strategy: TStrategy,
}

#[derive(Debug, Default)]
pub struct Stats {
    pub sent_blocks: u64,
    pub sent_data: u64,
    pub received_blocks: u64,
    pub received_data: u64,
    pub duplicate_blocks: u64,
    pub duplicate_data: u64,
}

impl<TStrategy> Bitswap<TStrategy> {
    /// Creates a `Bitswap`.
    pub fn new(strategy: TStrategy) -> Self {
        debug!("bitswap: new");
        Bitswap {
            events: VecDeque::new(),
            target_peers: FnvHashSet::default(),
            connected_peers: HashMap::new(),
            wanted_blocks: HashMap::new(),
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
                acc.sent_blocks += ledger.sent_blocks;
                acc.sent_data += ledger.sent_data;
                acc.received_blocks += ledger.received_blocks;
                acc.received_data += ledger.received_data;
                acc.duplicate_blocks += ledger.duplicate_blocks;
                acc.duplicate_data += ledger.duplicate_data;
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
            self.events
                .push_back(NetworkBehaviourAction::DialPeer { peer_id });
        }
        debug!("");
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
        self.events.push_back(NetworkBehaviourAction::SendEvent {
            peer_id,
            event: message,
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
            self.events.push_back(NetworkBehaviourAction::SendEvent {
                peer_id,
                event: message,
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
            self.events.push_back(NetworkBehaviourAction::SendEvent {
                peer_id: peer_id.to_owned(),
                event: message,
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
                    .push_back(NetworkBehaviourAction::SendEvent { peer_id, event });
            }
        }
        self.wanted_blocks.remove(cid);
        debug!("");
    }
}

impl<TStrategy: Strategy> NetworkBehaviour for Bitswap<TStrategy> {
    type ProtocolsHandler = OneShotHandler<BitswapConfig, Message<O>, InnerMessage>;
    type OutEvent = ();

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        debug!("bitswap: new_handler");
        Default::default()
    }

    fn addresses_of_peer(&mut self, _peer_id: &PeerId) -> Vec<Multiaddr> {
        debug!("bitswap: addresses_of_peer");
        Vec::new()
    }

    fn inject_connected(&mut self, peer_id: PeerId, cp: ConnectedPoint) {
        debug!("bitswap: inject_connected");
        debug!("  peer_id: {}", peer_id.to_base58());
        debug!("  connected_point: {:?}", cp);
        let ledger = Ledger::new();
        self.connected_peers.insert(peer_id.clone(), ledger);
        self.send_want_list(peer_id);
        debug!("");
    }

    fn inject_disconnected(&mut self, peer_id: &PeerId, cp: ConnectedPoint) {
        debug!("bitswap: inject_disconnected {:?}", cp);
        debug!("  peer_id: {}", peer_id.to_base58());
        debug!("  connected_point: {:?}", cp);
        debug!("");
        //self.connected_peers.remove(peer_id);
    }

    fn inject_node_event(&mut self, source: PeerId, event: InnerMessage) {
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
        // TODO: Remove cancelled blocks from `SendEvent`.
        debug!("");
    }

    #[allow(clippy::type_complexity)]
    fn poll(&mut self, ctx: &mut Context, _: &mut impl PollParameters)
        -> Poll<NetworkBehaviourAction<<<Self::ProtocolsHandler as IntoProtocolsHandler>::Handler as ProtocolsHandler>::InEvent, Self::OutEvent>>
    {
        // TODO concat messages to same destination to reduce traffic.
        if let Some(event) = self.events.pop_front() {
            if let NetworkBehaviourAction::SendEvent { peer_id, event } = event {
                match self.connected_peers.get_mut(&peer_id) {
                    None => {
                        debug!("  requeueing send event to {}", peer_id.to_base58());
                        self.events
                            .push_back(NetworkBehaviourAction::SendEvent { peer_id, event })
                    }
                    Some(ref mut ledger) => {
                        // FIXME: this is a bit early to update stats as the block hasn't been sent
                        // to anywhere at this point.
                        ledger.update_outgoing_stats(&event);
                        debug!("  send_message to {}", peer_id.to_base58());
                        return Poll::Ready(NetworkBehaviourAction::SendEvent { peer_id, event });
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
