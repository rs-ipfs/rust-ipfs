use libp2p::PeerId;
use libp2p::core::muxing::{StreamMuxerBox, SubstreamRef};
use libp2p::kad::Kademlia;
use std::sync::Arc;

/// IPFS bootstrap nodes.
const BOOTSTRAP_NODES: &[(&'static str, &'static str)] = &[
    (
        "QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
        "/ip4/104.131.131.82/tcp/4001",
    ),
    (
        "QmSoLPppuBtQSGwKDZT2M73ULpjvfd3aZ6ha4oFGL1KrGM",
        "/ip4/104.236.179.241/tcp/4001",
    ),
    (
        "QmSoLV4Bbm51jM9C4gDYZQ9Cy3U6aXMJDAbzgu2fzaDs64",
        "/ip4/104.236.76.40/tcp/4001",
    ),
    (
        "QmSoLSafTMBsPKadTEgaXctDQVcqN88CNLHXMkTNwMKPnu",
        "/ip4/128.199.219.111/tcp/4001",
    ),
    (
        "QmSoLer265NRgSp2LA3dPaeykiS1J6DifTC88f5uVQKNAd",
        "/ip4/178.62.158.247/tcp/4001",
    ),
    /*(
        "QmSoLSafTMBsPKadTEgaXctDQVcqN88CNLHXMkTNwMKPnu",
        "/ip6/2400:6180:0:d0::151:6001/tcp/4001",
    ),
    (
        "QmSoLPppuBtQSGwKDZT2M73ULpjvfd3aZ6ha4oFGL1KrGM",
        "/ip6/2604:a880:1:20::203:d001/tcp/4001",
    ),
    (
        "QmSoLV4Bbm51jM9C4gDYZQ9Cy3U6aXMJDAbzgu2fzaDs64",
        "/ip6/2604:a880:800:10::4a:5001/tcp/4001",
    ),
    (
        "QmSoLer265NRgSp2LA3dPaeykiS1J6DifTC88f5uVQKNAd",
        "/ip6/2a03:b0c0:0:1010::23:1001/tcp/4001",
    ),*/
];

/// Behaviour type.
pub type TBehaviour = Kademlia<SubstreamRef<Arc<StreamMuxerBox>>>;

/// Create a Kademlia behaviour with the IPFS bootstrap nodes.
pub fn build_behaviour(local_peer_id: PeerId) -> TBehaviour {
    // Note that normally the Kademlia process starts by performing lots of
    // request in order to insert our local node in the DHT. However here we use
    // `without_init` because this example is very ephemeral and we don't want
    // to pollute the DHT. In a real world application, you want to use `new`
    // instead.
    let mut behaviour = Kademlia::without_init(local_peer_id);

    for (identity, location) in BOOTSTRAP_NODES {
        behaviour.add_address(
            &identity.parse().unwrap(),
            location.parse().unwrap(),
        );
    }

    behaviour
}
