use crate::p2p::{SwarmOptions, SwarmTypes};
use libp2p::{PeerId, Transport};
use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::boxed::Boxed;
use libp2p::core::transport::upgrade::Version;
use libp2p::mplex::MplexConfig;
use libp2p::secio::SecioConfig;
use libp2p::tcp::TcpConfig;
use libp2p::yamux::Config as YamuxConfig;
use std::io::{Error, ErrorKind};
use std::time::Duration;

/// Transport type.
pub(crate) type TTransport = Boxed<(PeerId, StreamMuxerBox), Error>;

/// Builds the transport that serves as a common ground for all connections.
///
/// Set up an encrypted TCP transport over the Mplex protocol.
pub fn build_transport<TSwarmTypes: SwarmTypes>(options: &SwarmOptions<TSwarmTypes>) -> TTransport {
    let secio_config = SecioConfig::new(options.key_pair.to_owned());
    let yamux_config = YamuxConfig::default();
    let mplex_config = MplexConfig::new();

    TcpConfig::new()
        .nodelay(true)
        .upgrade(Version::V1)
        .authenticate(secio_config)
        .multiplex(libp2p::core::upgrade::SelectUpgrade::new(yamux_config, mplex_config))
        .timeout(Duration::from_secs(20))
        .map(|(peer_id, muxer), _| (peer_id, StreamMuxerBox::new(muxer)))
        .map_err(|err| Error::new(ErrorKind::Other, err))
        .boxed()
}
