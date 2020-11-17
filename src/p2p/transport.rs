use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::upgrade::Version;
use libp2p::core::transport::Boxed;
use libp2p::core::upgrade::SelectUpgrade;
use libp2p::dns::DnsConfig;
use libp2p::identity;
use libp2p::mplex::MplexConfig;
use libp2p::noise::{self, NoiseConfig};
use libp2p::tcp::TokioTcpConfig;
use libp2p::yamux::YamuxConfig;
use libp2p::{PeerId, Transport};
use std::io::{self, Error, ErrorKind};
use std::time::Duration;

/// Transport type.
pub(crate) type TTransport = Boxed<(PeerId, StreamMuxerBox)>;

/// Builds the transport that serves as a common ground for all connections.
///
/// Set up an encrypted TCP transport over the Mplex protocol.
pub fn build_transport(keypair: identity::Keypair) -> io::Result<TTransport> {
    let xx_keypair = noise::Keypair::<noise::X25519Spec>::new()
        .into_authentic(&keypair)
        .unwrap();
    let noise_config = NoiseConfig::xx(xx_keypair).into_authenticated();

    Ok(DnsConfig::new(TokioTcpConfig::new().nodelay(true))?
        .upgrade(Version::V1)
        .authenticate(noise_config)
        .multiplex(SelectUpgrade::new(
            YamuxConfig::default(),
            MplexConfig::new(),
        ))
        .timeout(Duration::from_secs(20))
        .map(|(peer_id, muxer), _| (peer_id, StreamMuxerBox::new(muxer)))
        .map_err(|err| Error::new(ErrorKind::Other, err))
        .boxed())
}
