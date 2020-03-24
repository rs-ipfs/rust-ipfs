use futures::channel::mpsc as channel;
use futures::stream::Stream;

use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use libp2p::core::{ConnectedPoint, Multiaddr, PeerId};
use libp2p::floodsub::{Floodsub, FloodsubEvent, FloodsubMessage, Topic};
use libp2p::swarm::{NetworkBehaviour, NetworkBehaviourAction, PollParameters, ProtocolsHandler};

/// Currently a thin wrapper around Floodsub, perhaps supporting both Gossipsub and Floodsub later.
/// Allows single subscription to a topic with only unbounded senders. Tracks the peers subscribed
/// to different topics. The messages in the streams are wrapped in `Arc` as they technically could
/// be sent to multiple topics, but this api is not provided.
pub struct Pubsub {
    streams: HashMap<Topic, channel::UnboundedSender<Arc<PubsubMessage>>>,
    peers: HashMap<PeerId, Vec<Topic>>,
    floodsub: Floodsub,
    // the subscription streams implement Drop and will send out their topic name through the
    // sender cloned from here if they are dropped before the stream has ended.
    unsubscriptions: (
        channel::UnboundedSender<String>,
        channel::UnboundedReceiver<String>,
    ),
}

/// Adaptation hopefully supporting somehow both Floodsub and Gossipsub Messages in the future
#[derive(Debug, PartialEq, Eq)]
pub struct PubsubMessage {
    pub source: PeerId,
    pub data: Vec<u8>,
    // this could be an enum for gossipsub message compat, it uses u64, though the floodsub
    // sequence numbers looked like 8 bytes in testing..
    pub sequence_number: Vec<u8>,
    // TODO: gossipsub uses topichashes, haven't checked if we could have some unifying abstraction
    // or if we should have a hash to name mapping internally?
    pub topics: Vec<String>,
}

impl From<FloodsubMessage> for PubsubMessage {
    fn from(
        FloodsubMessage {
            source,
            data,
            sequence_number,
            topics,
        }: FloodsubMessage,
    ) -> Self {
        PubsubMessage {
            source,
            data,
            sequence_number,
            topics: topics.into_iter().map(String::from).collect(),
        }
    }
}

pub(crate) type StreamImpl = UnsubscribeOnDrop<channel::UnboundedReceiver<Arc<PubsubMessage>>>;

pub struct UnsubscribeOnDrop<T>(channel::UnboundedSender<String>, Option<String>, T);

impl<T> Drop for UnsubscribeOnDrop<T> {
    fn drop(&mut self) {
        // the topic option allows us to disable this unsubscribe on drop once the stream has
        // ended. TODO: it would also be easy to implement FusedStream based on the state of the
        // option, if that is ever needed.
        if let Some(topic) = self.1.take() {
            // ignore errors
            let _ = self.0.unbounded_send(topic);
        }
    }
}

impl<T> fmt::Debug for UnsubscribeOnDrop<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "UnsubscribeOnDrop<{}>({:?})",
            std::any::type_name::<T>(),
            self.1
        )
    }
}

impl<T: Stream + Unpin> Stream for UnsubscribeOnDrop<T> {
    type Item = T::Item;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Option<Self::Item>> {
        let inner = &mut self.as_mut().2;
        let inner = Pin::new(inner);
        match inner.poll_next(ctx) {
            Poll::Ready(None) => {
                // no need to unsubscribe on drop as the stream has already ended, likely via
                // unsubscribe call.
                self.1.take();
                Poll::Ready(None)
            }
            other => other,
        }
    }
}

impl Pubsub {
    /// Delegates the `peer_id` over to [`Floodsub::new`] and internally only does accounting on
    /// top of the floodsub.
    pub fn new(peer_id: PeerId) -> Self {
        let (tx, rx) = channel::unbounded();
        Pubsub {
            streams: HashMap::new(),
            peers: HashMap::new(),
            floodsub: Floodsub::new(peer_id),
            unsubscriptions: (tx, rx),
        }
    }

    /// Subscribes to an currently unsubscribed topic.
    /// Returns a receiver for messages sent to the topic or `None` if subscription existed already
    pub fn subscribe(&mut self, topic: impl Into<String>) -> Option<StreamImpl> {
        use std::collections::hash_map::Entry;

        let topic = Topic::new(topic);

        match self.streams.entry(topic) {
            Entry::Vacant(ve) => {
                // TODO: this could also be bounded; we could send the message and drop the
                // subscription if it ever became full.
                let (tx, rx) = channel::unbounded();

                // there are probably some invariants which need to hold for the topic...
                assert!(
                    self.floodsub.subscribe(ve.key().clone()),
                    "subscribing to a unsubscribed topic should have succeeded"
                );

                let name = ve.key().id().to_string();
                ve.insert(tx);
                Some(UnsubscribeOnDrop(
                    self.unsubscriptions.0.clone(),
                    Some(name),
                    rx,
                ))
            }
            Entry::Occupied(_) => None,
        }
    }

    /// Unsubscribes from a topic.
    /// Returns true if an existing subscription was dropped
    pub fn unsubscribe(&mut self, topic: impl Into<String>) -> bool {
        let topic = Topic::new(topic);
        if self.streams.remove(&topic).is_some() {
            assert!(
                self.floodsub.unsubscribe(topic),
                "sender removed but unsubscription failed"
            );
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
        self.peers
            .iter()
            .filter_map(|(k, v)| {
                if v.contains(topic) {
                    Some(k.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Returns the list of currently subscribed topics. This can contain topics for which stream
    /// has been dropped but no messages have yet been received on the topics after the drop.
    pub fn subscribed_topics(&self) -> Vec<String> {
        self.streams
            .keys()
            .map(Topic::id)
            .map(String::from)
            .collect()
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
    <Pubsub as NetworkBehaviour>::OutEvent,
>;

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

    fn inject_node_event(
        &mut self,
        peer_id: PeerId,
        event: <Self::ProtocolsHandler as ProtocolsHandler>::OutEvent,
    ) {
        self.floodsub.inject_node_event(peer_id, event)
    }

    fn inject_addr_reach_failure(
        &mut self,
        peer_id: Option<&PeerId>,
        addr: &Multiaddr,
        error: &dyn std::error::Error,
    ) {
        self.floodsub
            .inject_addr_reach_failure(peer_id, addr, error)
    }

    fn poll(
        &mut self,
        ctx: &mut Context,
        poll: &mut impl PollParameters,
    ) -> Poll<PubsubNetworkBehaviourAction> {
        use futures::stream::StreamExt;
        use std::collections::hash_map::Entry;

        loop {
            match self.unsubscriptions.1.poll_next_unpin(ctx) {
                Poll::Ready(Some(dropped)) => {
                    let topic = Topic::new(dropped);
                    if self.streams.remove(&topic).is_some() {
                        log::debug!("unsubscribing via drop from {:?}", topic.id());
                        assert!(
                            self.floodsub.unsubscribe(topic),
                            "Failed to unsubscribe a dropped subscription"
                        );
                    } else {
                        // unsubscribed already by `unsubscribe`
                        // TODO: not sure if the unsubscribe functionality is needed as the
                        // unsubscribe on drop seems to work
                    }
                }
                Poll::Ready(None) => unreachable!("we own the sender"),
                Poll::Pending => break,
            }
        }

        match futures::ready!(self.floodsub.poll(ctx, poll)) {
            NetworkBehaviourAction::GenerateEvent(FloodsubEvent::Message(msg)) => {
                let topics = msg.topics.clone();
                let msg = Arc::new(PubsubMessage::from(msg));
                let mut buffer = None;

                for topic in topics {
                    match self.streams.entry(topic) {
                        Entry::Occupied(oe) => {
                            let sent = buffer.take().unwrap_or_else(|| Arc::clone(&msg));
                            if let Err(se) = oe.get().unbounded_send(sent) {
                                // receiver has dropped
                                let (topic, _) = oe.remove_entry();
                                log::debug!("unsubscribing via SendError from {:?}", topic.id());
                                assert!(
                                    self.floodsub.unsubscribe(topic.clone()),
                                    "Closed sender didnt allow unsubscribing {:?}",
                                    topic
                                );
                                buffer = Some(se.into_inner());
                            }
                        }
                        Entry::Vacant(ve) => {
                            panic!(
                                "we received a message to a topic we havent subscribed to {:?},
                                streams are probably out of sync. Please report this as a bug.",
                                ve.key()
                            );
                        }
                    }
                }
                Poll::Pending
            }
            NetworkBehaviourAction::GenerateEvent(FloodsubEvent::Subscribed { peer_id, topic }) => {
                let topics = self.peers.entry(peer_id).or_insert_with(Vec::new);
                if topics.iter().find(|&t| t == &topic).is_none() {
                    topics.push(topic);
                }
                Poll::Pending
            }
            NetworkBehaviourAction::GenerateEvent(FloodsubEvent::Unsubscribed {
                peer_id,
                topic,
            }) => {
                if let Entry::Occupied(mut oe) = self.peers.entry(peer_id) {
                    let topics = oe.get_mut();
                    if let Some(pos) = topics.iter().position(|t| t == &topic) {
                        topics.swap_remove(pos);
                    }
                    if topics.is_empty() {
                        oe.remove();
                    }
                }

                Poll::Pending
            }
            NetworkBehaviourAction::DialAddress { address } => {
                Poll::Ready(NetworkBehaviourAction::DialAddress { address })
            }
            NetworkBehaviourAction::DialPeer { peer_id } => {
                Poll::Ready(NetworkBehaviourAction::DialPeer { peer_id })
            }
            NetworkBehaviourAction::SendEvent { peer_id, event } => {
                Poll::Ready(NetworkBehaviourAction::SendEvent { peer_id, event })
            }
            NetworkBehaviourAction::ReportObservedAddr { address } => {
                Poll::Ready(NetworkBehaviourAction::ReportObservedAddr { address })
            }
        }
    }
}
