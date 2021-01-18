//! Handles the `/ipfs/bitswap/1.0.0` and `/ipfs/bitswap/1.1.0` protocols. This
//! allows exchanging IPFS blocks.
//!
//! # Usage
//!
//! The `Bitswap` struct implements the `NetworkBehaviour` trait. When used, it
//! will allow providing and reciving IPFS blocks.
use crate::block::Block;
use crate::ledger::{Ledger, Message, Priority};
use crate::protocol::{BitswapConfig, MessageWrapper};
use cid::Cid;
use fnv::FnvHashSet;
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use libp2p_core::{connection::ConnectionId, Multiaddr, PeerId};
use libp2p_swarm::protocols_handler::{IntoProtocolsHandler, OneShotHandler, ProtocolsHandler};
use libp2p_swarm::{
    DialPeerCondition, NetworkBehaviour, NetworkBehaviourAction, NotifyHandler, PollParameters,
};
use std::task::{Context, Poll};
use std::{
    collections::{HashMap, VecDeque},
    mem,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

/// Event used to communicate with the swarm or the higher level behaviour.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BitswapEvent {
    ReceivedBlock(PeerId, Block),
    ReceivedWant(PeerId, Cid, Priority),
    ReceivedCancel(PeerId, Cid),
}

/// Bitswap statistics.
#[derive(Debug, Default)]
pub struct Stats {
    pub sent_blocks: AtomicU64,
    pub sent_data: AtomicU64,
    pub received_blocks: AtomicU64,
    pub received_data: AtomicU64,
    pub duplicate_blocks: AtomicU64,
    pub duplicate_data: AtomicU64,
}

impl Stats {
    pub fn update_outgoing(&self, num_blocks: u64) {
        self.sent_blocks.fetch_add(num_blocks, Ordering::Relaxed);
    }

    pub fn update_incoming_unique(&self, bytes: u64) {
        self.received_blocks.fetch_add(1, Ordering::Relaxed);
        self.received_data.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn update_incoming_duplicate(&self, bytes: u64) {
        self.duplicate_blocks.fetch_add(1, Ordering::Relaxed);
        self.duplicate_data.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn add_assign(&self, other: &Stats) {
        self.sent_blocks
            .fetch_add(other.sent_blocks.load(Ordering::Relaxed), Ordering::Relaxed);
        self.sent_data
            .fetch_add(other.sent_data.load(Ordering::Relaxed), Ordering::Relaxed);
        self.received_blocks.fetch_add(
            other.received_blocks.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.received_data.fetch_add(
            other.received_data.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.duplicate_blocks.fetch_add(
            other.duplicate_blocks.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.duplicate_data.fetch_add(
            other.duplicate_data.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
    }
}

/// Network behaviour that handles sending and receiving IPFS blocks.
pub struct Bitswap {
    /// Queue of events to report to the user.
    events: VecDeque<NetworkBehaviourAction<Message, BitswapEvent>>,
    /// List of prospect peers to connect to.
    target_peers: FnvHashSet<PeerId>,
    /// Ledger
    pub connected_peers: HashMap<PeerId, Ledger>,
    /// Wanted blocks
    wanted_blocks: HashMap<Cid, Priority>,
    /// Blocks queued to be sent
    pub queued_blocks: UnboundedSender<(PeerId, Block)>,
    ready_blocks: UnboundedReceiver<(PeerId, Block)>,
    /// Statistics related to peers.
    pub stats: HashMap<PeerId, Arc<Stats>>,
}

impl Default for Bitswap {
    fn default() -> Self {
        let (tx, rx) = unbounded();

        Bitswap {
            events: Default::default(),
            target_peers: Default::default(),
            connected_peers: Default::default(),
            wanted_blocks: Default::default(),
            queued_blocks: tx,
            ready_blocks: rx,
            stats: Default::default(),
        }
    }
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
        self.stats
            .values()
            .fold(Stats::default(), |acc, peer_stats| {
                acc.add_assign(&peer_stats);
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
        if self.target_peers.insert(peer_id) {
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
        trace!("queueing block to be sent to {}: {}", peer_id, block.cid);
        if let Some(ledger) = self.connected_peers.get_mut(&peer_id) {
            ledger.add_block(block);
        }
    }

    /// Sends the wantlist to the peer.
    fn send_want_list(&mut self, peer_id: PeerId) {
        if !self.wanted_blocks.is_empty() {
            // FIXME: this can produce too long a message
            // FIXME: we should shard these across all of our peers by some logic; also, peers may
            // have been discovered to provide some specific wantlist item
            let mut message = Message::default();
            for (cid, priority) in &self.wanted_blocks {
                message.want_block(cid, *priority);
            }
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
        for (_peer_id, ledger) in self.connected_peers.iter_mut() {
            ledger.want_block(&cid, priority);
        }
        self.wanted_blocks.insert(cid, priority);
    }

    /// Removes the block from our want list and updates all peers.
    ///
    /// Can be either a user request or be called when the block
    /// was received.
    pub fn cancel_block(&mut self, cid: &Cid) {
        for (_peer_id, ledger) in self.connected_peers.iter_mut() {
            ledger.cancel_block(cid);
        }
        self.wanted_blocks.remove(cid);
    }
}

impl NetworkBehaviour for Bitswap {
    type ProtocolsHandler = OneShotHandler<BitswapConfig, Message, MessageWrapper>;
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
        self.stats.entry(*peer_id).or_default();
        self.connected_peers.insert(*peer_id, ledger);
        self.send_want_list(*peer_id);
    }

    fn inject_disconnected(&mut self, peer_id: &PeerId) {
        debug!("bitswap: inject_disconnected {:?}", peer_id);
        self.connected_peers.remove(peer_id);
        // the related stats are not dropped, so that they
        // persist for peers regardless of disconnects
    }

    fn inject_event(&mut self, source: PeerId, _connection: ConnectionId, message: MessageWrapper) {
        let mut message = match message {
            // we just sent an outgoing bitswap message, nothing to do here
            // FIXME: we could commit any pending stats accounting for this peer now
            // that the message may have sent, if we'd do such accounting
            MessageWrapper::Tx => return,
            // we've received a bitswap message, process it
            MessageWrapper::Rx(msg) => msg,
        };

        debug!("bitswap: inject_event from {}: {:?}", source, message);

        let current_wantlist = self.local_wantlist();

        let ledger = self
            .connected_peers
            .get_mut(&source)
            .expect("Peer not in ledger?!");

        // Process the incoming cancel list.
        for cid in message.cancel() {
            ledger.received_want_list.remove(cid);

            let event = BitswapEvent::ReceivedCancel(source, cid.clone());
            self.events
                .push_back(NetworkBehaviourAction::GenerateEvent(event));
        }

        // Process the incoming wantlist.
        for (cid, priority) in message
            .want()
            .iter()
            .filter(|&(cid, _)| !current_wantlist.iter().map(|(c, _)| c).any(|c| c == cid))
        {
            ledger.received_want_list.insert(cid.to_owned(), *priority);

            let event = BitswapEvent::ReceivedWant(source, cid.clone(), *priority);
            self.events
                .push_back(NetworkBehaviourAction::GenerateEvent(event));
        }

        // Process the incoming blocks.
        for block in mem::take(&mut message.blocks) {
            self.cancel_block(&block.cid());

            let event = BitswapEvent::ReceivedBlock(source, block);
            self.events
                .push_back(NetworkBehaviourAction::GenerateEvent(event));
        }
    }

    #[allow(clippy::type_complexity)]
    fn poll(&mut self, ctx: &mut Context, _: &mut impl PollParameters)
        -> Poll<NetworkBehaviourAction<<<Self::ProtocolsHandler as IntoProtocolsHandler>::Handler as ProtocolsHandler>::InEvent, Self::OutEvent>>
    {
        use futures::stream::StreamExt;

        while let Poll::Ready(Some((peer_id, block))) = self.ready_blocks.poll_next_unpin(ctx) {
            self.send_block(peer_id, block);
        }

        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(event);
        }

        for (peer_id, ledger) in &mut self.connected_peers {
            if let Some(message) = ledger.send() {
                if let Some(peer_stats) = self.stats.get_mut(peer_id) {
                    peer_stats.update_outgoing(message.blocks.len() as u64);
                }

                return Poll::Ready(NetworkBehaviourAction::NotifyHandler {
                    peer_id: *peer_id,
                    handler: NotifyHandler::Any,
                    event: message,
                });
            }
        }
        Poll::Pending
    }
}
