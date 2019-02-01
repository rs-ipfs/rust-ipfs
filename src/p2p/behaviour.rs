use libp2p::PeerId;
use libp2p::core::muxing::{StreamMuxerBox, SubstreamRef};
use libp2p::kad::Kademlia;
use std::sync::Arc;

/// Behaviour type.
pub type TBehaviour = Kademlia<SubstreamRef<Arc<StreamMuxerBox>>>;

/// Create a Kademlia behaviour.
pub fn build_behaviour(peer_id: PeerId) -> TBehaviour {
    // Note that normally the Kademlia process starts by performing lots of
    // request in order to insert our local node in the DHT. However here we use
    // `without_init` because this example is very ephemeral and we don't want
    // to pollute the DHT. In a real world application, you want to use `new`
    // instead.
    Kademlia::without_init(peer_id)
}
