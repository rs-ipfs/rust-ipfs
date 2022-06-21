use futures::channel::mpsc as channel;
use futures::stream::{FusedStream, Stream};
use libp2p::gossipsub::error::PublishError;
use libp2p::identity::Keypair;
use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::error::Error;

use libp2p::core::{
    connection::{ConnectedPoint, ConnectionId, ListenerId},
    Multiaddr, PeerId,
};

use libp2p::gossipsub::{
    self, Gossipsub, GossipsubEvent, GossipsubMessage, IdentTopic as Topic, MessageAuthenticity,
    MessageId, TopicHash,
};
use libp2p::swarm::{
    ConnectionHandler, DialError, NetworkBehaviour, NetworkBehaviourAction, PollParameters,
};

/// Currently a thin wrapper around Gossipsub.
/// Allows single subscription to a topic with only unbounded senders. Tracks the peers subscribed
/// to different topics. The messages in the streams are wrapped in `Arc` as they technically could
/// be sent to multiple topics, but this api is not provided.
pub struct Pubsub {
    // Tracks the topic subscriptions.
    streams: HashMap<TopicHash, channel::UnboundedSender<Arc<PubsubMessage>>>,

    // A collection of peers and the topics they are subscribed to.
    peers: HashMap<PeerId, Vec<TopicHash>>,

    gossipsub: Gossipsub,
    // the subscription streams implement Drop and will send out their topic through the
    // sender cloned from here if they are dropped before the stream has ended.
    unsubscriptions: (
        channel::UnboundedSender<TopicHash>,
        channel::UnboundedReceiver<TopicHash>,
    ),
}

/// Adaptation hopefully supporting both Floodsub and Gossipsub Messages in the future
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PubsubMessage {
    /// Peer address of the message sender.
    pub source: Option<PeerId>,
    /// The message data.
    pub data: Vec<u8>,
    /// The sequence number of the message.
    pub sequence_number: Option<u64>,
    /// The recepient of the message.
    pub topic: TopicHash,
}

impl From<GossipsubMessage> for PubsubMessage {
    fn from(
        GossipsubMessage {
            source,
            data,
            sequence_number,
            topic,
        }: GossipsubMessage,
    ) -> Self {
        PubsubMessage {
            source,
            data,
            sequence_number,
            topic,
        }
    }
}

/// Stream of a pubsub messages. Implements [`FusedStream`].
pub struct SubscriptionStream {
    on_drop: Option<channel::UnboundedSender<TopicHash>>,
    topic: Option<TopicHash>,
    inner: channel::UnboundedReceiver<Arc<PubsubMessage>>,
}

impl Drop for SubscriptionStream {
    fn drop(&mut self) {
        // the on_drop option allows us to disable this unsubscribe on drop once the stream has
        // ended.
        if let Some(sender) = self.on_drop.take() {
            if let Some(topic) = self.topic.take() {
                let _ = sender.unbounded_send(topic);
            }
        }
    }
}

impl fmt::Debug for SubscriptionStream {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        if let Some(topic) = self.topic.as_ref() {
            write!(
                fmt,
                "SubscriptionStream {{ topic: {:?}, is_terminated: {} }}",
                topic,
                self.is_terminated()
            )
        } else {
            write!(
                fmt,
                "SubscriptionStream {{ is_terminated: {} }}",
                self.is_terminated()
            )
        }
    }
}

impl Stream for SubscriptionStream {
    type Item = Arc<PubsubMessage>;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Option<Self::Item>> {
        use futures::stream::StreamExt;
        let inner = &mut self.as_mut().inner;
        match inner.poll_next_unpin(ctx) {
            Poll::Ready(None) => {
                // no need to unsubscribe on drop as the stream has already ended, likely via
                // unsubscribe call.
                self.on_drop.take();
                Poll::Ready(None)
            }
            other => other,
        }
    }
}

impl FusedStream for SubscriptionStream {
    fn is_terminated(&self) -> bool {
        self.on_drop.is_none()
    }
}

impl Pubsub {
    /// Delegates the `peer_id` over to [`Gossipsub`] and internally only does accounting on
    /// top of the gossip.
    pub fn new(keypair: Keypair) -> Result<Self, Error> {
        let (tx, rx) = channel::unbounded();
        let config = gossipsub::GossipsubConfigBuilder::default()
            .build()
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        Ok(Pubsub {
            streams: HashMap::new(),
            peers: HashMap::new(),
            gossipsub: Gossipsub::new(MessageAuthenticity::Signed(keypair), config)
                .map_err(|e| anyhow::anyhow!("{}", e))?,
            unsubscriptions: (tx, rx),
        })
    }

    /// Subscribes to a currently unsubscribed topic.
    /// Returns a receiver for messages sent to the topic or `None` if subscription existed
    /// already.
    pub fn subscribe(&mut self, topic: impl Into<String>) -> Option<SubscriptionStream> {
        use std::collections::hash_map::Entry;
        let topic = Topic::new(topic);

        match self.streams.entry(topic.hash()) {
            Entry::Vacant(ve) => {
                match self.gossipsub.subscribe(&topic) {
                    Ok(true) => {
                        // TODO: this could also be bounded; we could send the message and drop the
                        // subscription if it ever became full.
                        let (tx, rx) = channel::unbounded();
                        let key = ve.key().clone();
                        ve.insert(tx);
                        Some(SubscriptionStream {
                            on_drop: Some(self.unsubscriptions.0.clone()),
                            topic: Some(key),
                            inner: rx,
                        })
                    }
                    Ok(false) => None,
                    Err(e) => {
                        error!("Error subscribing to topic: {}", e); //"subscribing to a unsubscribed topic should have succeeded"
                        None
                    }
                }
            }
            Entry::Occupied(_) => None,
        }
    }

    /// Unsubscribes from a topic. Unsubscription is usually done through dropping the
    /// SubscriptionStream.
    ///
    /// Returns true if an existing subscription was dropped, false otherwise
    pub fn unsubscribe(&mut self, topic: impl Into<String>) -> Result<bool, Error> {
        let topic = Topic::new(topic.into());
        if self.streams.remove(&topic.hash()).is_some() {
            Ok(self.gossipsub.unsubscribe(&topic)?)
        } else {
            anyhow::bail!("sender removed but unsubscription failed")
        }
    }

    /// See [`Gossipsub::publish`]
    pub fn publish(
        &mut self,
        topic: impl Into<String>,
        data: impl Into<Vec<u8>>,
    ) -> Result<MessageId, PublishError> {
        self.gossipsub.publish(Topic::new(topic), data)
    }

    /// Returns the known peers subscribed to any topic
    pub fn known_peers(&self) -> Vec<PeerId> {
        self.peers.keys().cloned().collect()
    }

    /// Returns the peers known to subscribe to the given topic
    pub fn subscribed_peers(&self, topic: &str) -> Vec<PeerId> {
        let topic = Topic::new(topic);
        self.peers
            .iter()
            .filter_map(|(k, v)| {
                if v.contains(&topic.hash()) {
                    Some(*k)
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
            .into_iter()
            .map(|t| t.to_string())
            .collect()
    }

    /// See [`Gossipsub::add_explicit_peer`]
    pub fn add_explicit_peer(&mut self, peer_id: &PeerId) {
        self.gossipsub.add_explicit_peer(peer_id);
    }

    /// See [`Gossipsub::remove_explicit_peer`]
    pub fn remove_explicit_peer(&mut self, peer_id: &PeerId) {
        self.gossipsub.remove_explicit_peer(peer_id);
    }
}

type PubsubNetworkBehaviourAction = NetworkBehaviourAction<
    <Gossipsub as NetworkBehaviour>::OutEvent,
    <Pubsub as NetworkBehaviour>::ConnectionHandler,
    <<Pubsub as NetworkBehaviour>::ConnectionHandler as ConnectionHandler>::InEvent,
>;

impl NetworkBehaviour for Pubsub {
    type ConnectionHandler = <Gossipsub as NetworkBehaviour>::ConnectionHandler;
    type OutEvent = GossipsubEvent;

    fn new_handler(&mut self) -> Self::ConnectionHandler {
        self.gossipsub.new_handler()
    }

    fn addresses_of_peer(&mut self, peer_id: &PeerId) -> Vec<Multiaddr> {
        self.gossipsub.addresses_of_peer(peer_id)
    }

    fn inject_connection_established(
        &mut self,
        peer_id: &PeerId,
        connection_id: &ConnectionId,
        endpoint: &ConnectedPoint,
        failed_addresses: Option<&Vec<Multiaddr>>,
        other_established: usize,
    ) {
        self.gossipsub.inject_connection_established(
            peer_id,
            connection_id,
            endpoint,
            failed_addresses,
            other_established,
        )
    }

    fn inject_connection_closed(
        &mut self,
        peer_id: &PeerId,
        connection_id: &ConnectionId,
        endpoint: &ConnectedPoint,
        handler: Self::ConnectionHandler,
        remaining_established: usize,
    ) {
        self.gossipsub.inject_connection_closed(
            peer_id,
            connection_id,
            endpoint,
            handler,
            remaining_established,
        )
    }

    fn inject_event(
        &mut self,
        peer_id: PeerId,
        connection: ConnectionId,
        event: <Self::ConnectionHandler as ConnectionHandler>::OutEvent,
    ) {
        self.gossipsub.inject_event(peer_id, connection, event)
    }

    fn inject_dial_failure(
        &mut self,
        peer_id: Option<PeerId>,
        handler: Self::ConnectionHandler,
        error: &DialError,
    ) {
        self.gossipsub.inject_dial_failure(peer_id, handler, error)
    }

    fn inject_new_listen_addr(&mut self, id: ListenerId, addr: &Multiaddr) {
        self.gossipsub.inject_new_listen_addr(id, addr)
    }

    fn inject_expired_listen_addr(&mut self, id: ListenerId, addr: &Multiaddr) {
        self.gossipsub.inject_expired_listen_addr(id, addr)
    }

    fn inject_new_external_addr(&mut self, addr: &Multiaddr) {
        self.gossipsub.inject_new_external_addr(addr)
    }

    fn inject_listener_error(&mut self, id: ListenerId, err: &(dyn std::error::Error + 'static)) {
        self.gossipsub.inject_listener_error(id, err)
    }

    fn inject_address_change(
        &mut self,
        peer: &PeerId,
        id: &ConnectionId,
        old: &ConnectedPoint,
        new: &ConnectedPoint,
    ) {
        self.gossipsub.inject_address_change(peer, id, old, new)
    }

    fn inject_listen_failure(
        &mut self,
        local_addr: &Multiaddr,
        send_back_addr: &Multiaddr,
        handler: Self::ConnectionHandler,
    ) {
        self.gossipsub
            .inject_listen_failure(local_addr, send_back_addr, handler)
    }

    fn inject_new_listener(&mut self, id: ListenerId) {
        self.gossipsub.inject_new_listener(id)
    }

    fn inject_listener_closed(&mut self, id: ListenerId, reason: Result<(), &std::io::Error>) {
        self.gossipsub.inject_listener_closed(id, reason)
    }

    fn inject_expired_external_addr(&mut self, addr: &Multiaddr) {
        self.gossipsub.inject_expired_external_addr(addr)
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
                    if self.streams.remove(&dropped).is_some() {
                        debug!("unsubscribing via drop from {:?}", dropped);
                        assert!(
                            self.gossipsub
                                .unsubscribe(&Topic::new(dropped.to_string()))
                                .unwrap_or_default(),
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

        loop {
            match futures::ready!(self.gossipsub.poll(ctx, poll)) {
                NetworkBehaviourAction::GenerateEvent(GossipsubEvent::Message {
                    message, ..
                }) => {
                    let topic = message.topic.clone();
                    let msg = Arc::new(PubsubMessage::from(message));
                    let mut buffer = None;
                    if let Entry::Occupied(oe) = self.streams.entry(topic.clone()) {
                        let sent = buffer.take().unwrap_or_else(|| Arc::clone(&msg));

                        if let Err(se) = oe.get().unbounded_send(sent) {
                            // receiver has dropped
                            let (topic, _) = oe.remove_entry();
                            debug!("unsubscribing via SendError from {:?}", &topic);
                            assert!(
                                self.gossipsub
                                    .unsubscribe(&Topic::new(topic.to_string()))
                                    .unwrap_or_default(),
                                "Failed to unsubscribe following SendError"
                            );
                            buffer = Some(se.into_inner());
                        }
                    } else {
                        // we had unsubscribed from the topic after Gossipsub had received the
                        // message
                    }

                    continue;
                }
                NetworkBehaviourAction::GenerateEvent(GossipsubEvent::Subscribed {
                    peer_id,
                    topic,
                }) => {
                    self.add_explicit_peer(&peer_id);
                    let topics = self.peers.entry(peer_id).or_insert_with(Vec::new);

                    let appeared = topics.is_empty();

                    if !topics.contains(&topic) {
                        topics.push(topic);
                    }

                    if appeared {
                        debug!("peer appeared as pubsub subscriber: {}", peer_id);
                    }

                    continue;
                }
                NetworkBehaviourAction::GenerateEvent(GossipsubEvent::Unsubscribed {
                    peer_id,
                    topic,
                }) => {
                    if let Entry::Occupied(mut oe) = self.peers.entry(peer_id) {
                        let topics = oe.get_mut();
                        if let Some(pos) = topics.iter().position(|t| t == &topic) {
                            topics.swap_remove(pos);
                        }
                        if topics.is_empty() {
                            debug!("peer disappeared as pubsub subscriber: {}", peer_id);
                            oe.remove();
                            self.remove_explicit_peer(&peer_id);
                        }
                    }

                    continue;
                }
                NetworkBehaviourAction::GenerateEvent(GossipsubEvent::GossipsubNotSupported {
                    peer_id,
                }) => {
                    warn!("Not supported for {}", peer_id);
                    continue;
                }
                action @ NetworkBehaviourAction::Dial { .. } => {
                    return Poll::Ready(action);
                }
                NetworkBehaviourAction::NotifyHandler {
                    peer_id,
                    event,
                    handler,
                } => {
                    return Poll::Ready(NetworkBehaviourAction::NotifyHandler {
                        peer_id,
                        event,
                        handler,
                    });
                }
                NetworkBehaviourAction::ReportObservedAddr { address, score } => {
                    return Poll::Ready(NetworkBehaviourAction::ReportObservedAddr {
                        address,
                        score,
                    });
                }
                NetworkBehaviourAction::CloseConnection {
                    peer_id,
                    connection,
                } => {
                    return Poll::Ready(NetworkBehaviourAction::CloseConnection {
                        peer_id,
                        connection,
                    });
                }
            }
        }
    }
}
