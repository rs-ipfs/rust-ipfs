//! P2P handling for IPFS nodes.
use crate::options::{IpfsOptions, IpfsTypes};
use crate::repo::Repo;
use libp2p::Swarm;
use std::sync::Arc;

mod behaviour;
mod swarm;
mod transport;

pub use swarm::Connection;

pub type TSwarm<T> = Swarm<behaviour::Behaviour<T>>;

/// Creates a new IPFS swarm.
pub async fn create_swarm<T: IpfsTypes>(options: &IpfsOptions, repo: Arc<Repo<T>>) -> TSwarm<T> {
    // Set up an encrypted TCP transport over the Mplex protocol.
    let transport = transport::build_transport(options.keypair.clone());

    // Create a Kademlia behaviour
    let behaviour = behaviour::Behaviour::new(&options, repo).await;

    // Create a Swarm
    let mut swarm = libp2p::Swarm::new(transport, behaviour, options.peer_id());

    // Listen on all interfaces and whatever port the OS assigns
    let addr = Swarm::listen_on(&mut swarm, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();
    info!("Listening on {:?}", addr);

    swarm
}
