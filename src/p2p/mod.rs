//! P2P handling for IPFS nodes.
use crate::{Ipfs, IpfsTypes};
use libp2p::Swarm;
use tracing::Span;

mod behaviour;
pub(crate) mod pubsub;
mod swarm;
mod transport;

pub use swarm::{Connection, ConnectionTarget};

pub type TSwarm<T> = Swarm<behaviour::Behaviour<T>>;

/// Creates a new IPFS swarm.
pub async fn create_swarm<TIpfsTypes: IpfsTypes>(ipfs: Ipfs<TIpfsTypes>) -> TSwarm<TIpfsTypes> {
    let peer_id = ipfs.options.keypair.public().into_peer_id();

    // Set up an encrypted TCP transport over the Mplex protocol.
    let transport = transport::build_transport(ipfs.options.keypair.clone());

    let swarm_span = ipfs.0.span.clone();

    // Create a Kademlia behaviour
    let behaviour = behaviour::build_behaviour(ipfs).await;

    // Create a Swarm
    let mut swarm = libp2p::swarm::SwarmBuilder::new(transport, behaviour, peer_id)
        .executor(Box::new(SpannedExecutor(swarm_span)))
        .build();

    // Listen on all interfaces and whatever port the OS assigns
    Swarm::listen_on(&mut swarm, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();

    swarm
}

struct SpannedExecutor(Span);

impl libp2p::core::Executor for SpannedExecutor {
    fn exec(
        &self,
        future: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'static + Send>>,
    ) {
        use tracing_futures::Instrument;
        async_std::task::spawn(future.instrument(self.0.clone()));
    }
}
