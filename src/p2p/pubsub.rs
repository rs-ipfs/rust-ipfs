use futures::channel::mpsc as channel;
use std::collections::HashMap;
use std::sync::Arc;
use std::task::{Context, Poll};

use libp2p::floodsub::{Floodsub, Topic, FloodsubMessage, FloodsubEvent};
use libp2p::core::{ConnectedPoint, Multiaddr, PeerId};
use libp2p::swarm::{NetworkBehaviour, NetworkBehaviourAction, PollParameters, ProtocolsHandler};

/// Currently a thin wrapper around Floodsub, perhaps supporting both Gossipsub and Floodsub later.
/// Allows single subscription to a topic with only unbounded senders. Tracks the peers subscribed
/// to different topics.
pub struct Pubsub {
    streams: HashMap<Topic, channel::UnboundedSender<Arc<PubsubMessage>>>,
    peers: HashMap<PeerId, Vec<Topic>>,
    floodsub: Floodsub,
}

/// Adaptation hopefully supporting somehow both Floodsub and Gossipsub Messages in the future
#[derive(Debug, PartialEq, Eq)]
pub struct PubsubMessage {
    pub source: PeerId,
    pub data: Vec<u8>,
    // this could be an enum for gossipsub message compat, it uses u64
    pub sequence_number: Vec<u8>,
    // TODO: gossipsub uses topichashes, haven't checked if we could have some unifying abstraction
    // or if we should have a hash to name mapping internally?
    pub topics: Vec<String>,
}

impl From<FloodsubMessage> for PubsubMessage {
    fn from(FloodsubMessage { source, data, sequence_number, topics }: FloodsubMessage) -> Self {
        PubsubMessage {
            source,
            data,
            sequence_number,
            topics: topics.into_iter().map(String::from).collect(),
        }
    }
}

impl Pubsub {
    pub fn new(peer_id: PeerId) -> Self {
        Pubsub {
            streams: HashMap::new(),
            peers: HashMap::new(),
            floodsub: Floodsub::new(peer_id),
        }
    }

    /// Subscribes to an currently unsubscribed topic.
    /// Returns a receiver for messages sent to the topic or `None` if subscription existed already
    pub fn subscribe(&mut self, topic: impl Into<String>) -> Option<channel::UnboundedReceiver<Arc<PubsubMessage>>> {
        use std::collections::hash_map::Entry;

        let topic = Topic::new(topic);

        match self.streams.entry(topic) {
            Entry::Vacant(ve) => {
                let (tx, rx) = channel::unbounded();

                // TODO: unsure on what could be the limits here
                assert!(
                    self.floodsub.subscribe(ve.key().clone()),
                    "subscribing to a unsubscribed topic should have succeeded");

                ve.insert(tx);
                Some(rx)
            },
            Entry::Occupied(_) => None,
        }
    }

    /// Unsubscribes from a topic.
    /// Returns true if an existing subscription was dropped
    pub fn unsubscribe(&mut self, topic: impl Into<String>) -> bool {
        let topic = Topic::new(topic);
        if self.streams.remove(&topic).is_some() {
            assert!(self.floodsub.unsubscribe(topic), "sender removed but unsubscription failed");
            true
        } else {
            false
        }
    }

    /// See [`Floodsub::publish_any`]
    pub fn publish(&mut self, topic: impl Into<String>, data: impl Into<Vec<u8>>) {
        self.floodsub.publish_any(Topic::new(topic), data);
    }

    /// Returns the known peers subscribed to any topic
    pub fn known_peers(&self) -> Vec<PeerId> {
        self.peers.keys().cloned().collect()
    }

    /// Returns the peers known to subscribe to the given topic
    pub fn subscribed_peers(&self, topic: &Topic) -> Vec<PeerId> {
        self.peers.iter().filter_map(|(k, v)| if v.contains(topic) { Some(k.clone()) } else { None }).collect()
    }

    /// Returns the list of currently subscribed topics. This can contain topics for which stream
    /// has been dropped but no messages have yet been received on the topics after the drop.
    pub fn subscribed_topics(&self) -> Vec<String> {
        self.streams.keys().map(Topic::id).map(String::from).collect()
    }

    /// See [`Floodsub::add_node_from_partial_view`]
    pub fn add_node_to_partial_view(&mut self, peer_id: PeerId) {
        self.floodsub.add_node_to_partial_view(peer_id);
    }

    /// See [`Floodsub::remove_node_from_partial_view`]
    pub fn remove_node_from_partial_view(&mut self, peer_id: &PeerId) {
        self.floodsub.remove_node_from_partial_view(peer_id);
    }
}

type PubsubNetworkBehaviourAction = NetworkBehaviourAction<
    <<Pubsub as NetworkBehaviour>::ProtocolsHandler as ProtocolsHandler>::InEvent,
    <Pubsub as NetworkBehaviour>::OutEvent>;

impl NetworkBehaviour for Pubsub {
    type ProtocolsHandler = <Floodsub as NetworkBehaviour>::ProtocolsHandler;
    type OutEvent = void::Void;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        self.floodsub.new_handler()
    }

    fn addresses_of_peer(&mut self, peer_id: &PeerId) -> Vec<Multiaddr> {
        self.floodsub.addresses_of_peer(peer_id)
    }

    fn inject_connected(&mut self, peer_id: PeerId, cp: ConnectedPoint) {
        self.floodsub.inject_connected(peer_id, cp)
    }

    fn inject_disconnected(&mut self, peer_id: &PeerId, cp: ConnectedPoint) {
        self.floodsub.inject_disconnected(peer_id, cp)
    }

    fn inject_node_event(&mut self, peer_id: PeerId, event: <Self::ProtocolsHandler as ProtocolsHandler>::OutEvent) {
        self.floodsub.inject_node_event(peer_id, event)
    }

    fn inject_addr_reach_failure(
        &mut self,
        peer_id: Option<&PeerId>,
        addr: &Multiaddr,
        error: &dyn std::error::Error,
    ) {
        self.floodsub.inject_addr_reach_failure(peer_id, addr, error)
    }

    fn poll(
        &mut self,
        ctx: &mut Context,
        poll: &mut impl PollParameters,
    ) -> Poll<PubsubNetworkBehaviourAction> {
        use std::collections::hash_map::Entry;

        match futures::ready!(self.floodsub.poll(ctx, poll)) {
            NetworkBehaviourAction::GenerateEvent(FloodsubEvent::Message(msg)) => {
                let topics = msg.topics.clone();
                let msg = Arc::new(PubsubMessage::from(msg));
                let mut buffer = None;

                for topic in topics {
                    match self.streams.entry(topic) {
                        Entry::Occupied(oe) => {
                            let sent = buffer.take().unwrap_or_else(|| Arc::clone(&msg));
                            if let Some(se) = oe.get().unbounded_send(sent).err() {
                                // receiver has dropped
                                let (topic, _) = oe.remove_entry();
                                assert!(self.floodsub.unsubscribe(topic.clone()), "Closed sender didnt allow unsubscribing {:?}", topic);
                                buffer = Some(se.into_inner());
                            }
                        }
                        Entry::Vacant(ve) => {
                            panic!("we received a message to a topic we havent subscribed to {:?}, streams are probably out of sync", ve.key());
                        }
                    }
                }
                Poll::Pending
            },
            NetworkBehaviourAction::GenerateEvent(FloodsubEvent::Subscribed { peer_id, topic }) => {
                let topics = self.peers.entry(peer_id).or_insert_with(Vec::new);
                if topics.iter().find(|&t| t == &topic).is_none() {
                    topics.push(topic);
                }
                Poll::Pending
            },
            NetworkBehaviourAction::GenerateEvent(FloodsubEvent::Unsubscribed { peer_id, topic }) => {
                match self.peers.entry(peer_id) {
                    Entry::Occupied(mut oe) => {
                        let topics = oe.get_mut();
                        topics.retain(|t| t != &topic);
                    }
                    _ => {},
                }

                Poll::Pending
            },
            NetworkBehaviourAction::DialAddress { address } => Poll::Ready(NetworkBehaviourAction::DialAddress { address }),
            NetworkBehaviourAction::DialPeer { peer_id } => Poll::Ready(NetworkBehaviourAction::DialPeer { peer_id }),
            NetworkBehaviourAction::SendEvent { peer_id, event } => Poll::Ready(NetworkBehaviourAction::SendEvent { peer_id, event }),
            NetworkBehaviourAction::ReportObservedAddr { address } => Poll::Ready(NetworkBehaviourAction::ReportObservedAddr { address }),
        }
    }
}
