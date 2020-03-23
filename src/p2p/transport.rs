use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::boxed::Boxed;
use libp2p::core::transport::upgrade::Version;
use libp2p::core::upgrade::SelectUpgrade;
use libp2p::identity::Keypair;
use libp2p::mplex::MplexConfig;
use libp2p::secio::SecioConfig;
use libp2p::tcp::TcpConfig;
use libp2p::yamux::Config as YamuxConfig;
use libp2p::{PeerId, Transport};
use std::io::{Error, ErrorKind};
use std::time::Duration;

/// Transport type.
pub(crate) type TTransport = Boxed<(PeerId, StreamMuxerBox), Error>;

/// Builds the transport that serves as a common ground for all connections.
///
/// Set up an encrypted TCP transport over the Mplex protocol.
pub fn build_transport(key: Keypair) -> TTransport {
    TcpConfig::new()
        .nodelay(true)
        .upgrade(Version::V1)
        .authenticate(SecioConfig::new(key))
        .multiplex(SelectUpgrade::new(
            YamuxConfig::default(),
            MplexConfig::new(),
        ))
        .timeout(Duration::from_secs(20))
        .map(|(peer_id, muxer), _| (peer_id, StreamMuxerBox::new(muxer)))
        .map_err(|err| Error::new(ErrorKind::Other, err))
        .boxed()
}
