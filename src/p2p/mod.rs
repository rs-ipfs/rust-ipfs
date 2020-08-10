//! P2P handling for IPFS nodes.
use crate::{Ipfs, IpfsOptions, IpfsTypes};
use libp2p::identity::Keypair;
use libp2p::Swarm;
use libp2p::{Multiaddr, PeerId};
use tracing::Span;

mod behaviour;
pub(crate) mod pubsub;
mod swarm;
mod transport;

pub use swarm::{Connection, ConnectionTarget};

pub type TSwarm<T> = Swarm<behaviour::Behaviour<T>>;

pub struct SwarmOptions {
    pub keypair: Keypair,
    pub peer_id: PeerId,
    pub bootstrap: Vec<(Multiaddr, PeerId)>,
    pub mdns: bool,
    pub kad_protocol: Option<String>,
}

impl From<IpfsOptions> for SwarmOptions {
    fn from(options: IpfsOptions) -> Self {
        let IpfsOptions {
            keypair,
            bootstrap,
            mdns,
            kad_protocol,
            ..
        } = options;

        let peer_id = keypair.public().into_peer_id();

        SwarmOptions {
            keypair,
            peer_id,
            bootstrap,
            mdns,
            kad_protocol,
        }
    }
}

/// Creates a new IPFS swarm.
pub async fn create_swarm<TIpfsTypes: IpfsTypes>(
    options: SwarmOptions,
    ipfs: Ipfs<TIpfsTypes>,
) -> TSwarm<TIpfsTypes> {
    let peer_id = options.peer_id.clone();

    // Set up an encrypted TCP transport over the Mplex protocol.
    let transport = transport::build_transport(options.keypair.clone());

    let swarm_span = ipfs.0.span.clone();

    // Create a Kademlia behaviour
    let behaviour = behaviour::build_behaviour(options, ipfs).await;

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
