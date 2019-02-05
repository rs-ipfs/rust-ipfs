use crate::config::NetworkConfig;
use futures::future::Future;
use libp2p::{PeerId, Transport};
use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::boxed::Boxed;
use libp2p::core::upgrade::{self, InboundUpgradeExt, OutboundUpgradeExt};
use libp2p::mplex::MplexConfig;
use libp2p::secio::SecioConfig;
use libp2p::tcp::TcpConfig;
use std::io::{Error, ErrorKind};
use std::time::Duration;

/// Transport type.
pub type TTransport = Boxed<(PeerId, StreamMuxerBox), Error>;

/// Builds the transport that serves as a common ground for all connections.
///
/// Set up an encrypted TCP transport over the Mplex protocol.
pub fn build_transport(config: &NetworkConfig) -> TTransport {
    let transport = TcpConfig::new();
    let secio_config = SecioConfig::new(config.key_pair.to_owned());
    let mplex_config = MplexConfig::new();

    transport
        .with_upgrade(secio_config)
        .and_then(move |out, endpoint| {
            let peer_id = out.remote_key.into_peer_id();
            let peer_id2 = peer_id.clone();
            let upgrade = mplex_config
                .map_inbound(move |muxer| (peer_id, muxer))
                .map_outbound(move |muxer| (peer_id2, muxer));

            upgrade::apply(out.stream, upgrade, endpoint)
                .map(|(peer_id, muxer)| (peer_id, StreamMuxerBox::new(muxer)))
        })
        .with_timeout(Duration::from_secs(20))
        .map_err(|err| Error::new(ErrorKind::Other, err))
        .boxed()
}
