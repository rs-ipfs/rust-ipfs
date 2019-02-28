use crate::p2p::{SwarmOptions, SwarmTypes};
use libp2p::{PeerId, Transport};
use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::boxed::Boxed;
use libp2p::core::upgrade::{self, InboundUpgradeExt, OutboundUpgradeExt,
                            SelectUpgrade};
use libp2p::mplex::MplexConfig;
use libp2p::secio::SecioConfig;
use libp2p::tcp::TcpConfig;
use libp2p::yamux::Config as YamuxConfig;
use std::io::{Error, ErrorKind};
use std::time::Duration;
use tokio::prelude::*;

/// Transport type.
pub(crate) type TTransport = Boxed<(PeerId, StreamMuxerBox), Error>;

/// Builds the transport that serves as a common ground for all connections.
///
/// Set up an encrypted TCP transport over the Mplex protocol.
pub fn build_transport<TSwarmTypes: SwarmTypes>(options: &SwarmOptions<TSwarmTypes>) -> TTransport {
    let transport = TcpConfig::new();
    let secio_config = SecioConfig::new(options.key_pair.to_owned());
    let yamux_config = YamuxConfig::default();
    let mplex_config = MplexConfig::new();

    transport
        .with_upgrade(secio_config)
        .and_then(move |out, endpoint| {
            let peer_id = out.remote_key.into_peer_id();
            let peer_id2 = peer_id.clone();
            let upgrade = SelectUpgrade::new(mplex_config, yamux_config)
                .map_inbound(move |muxer| (peer_id, muxer))
                .map_outbound(move |muxer| (peer_id2, muxer));

            upgrade::apply(out.stream, upgrade, endpoint)
                .map(|(peer_id, muxer)| (peer_id, StreamMuxerBox::new(muxer)))
        })
        .with_timeout(Duration::from_secs(20))
        .map_err(|err| Error::new(ErrorKind::Other, err))
        .boxed()
}
