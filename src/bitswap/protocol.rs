/// Reperesents a prototype for an upgrade to handle the bitswap protocol.
///
/// The protocol works the following way:
///
/// - TODO
use crate::bitswap::ledger::{Message, I, O};
use crate::error::Error;
use futures::future::Future;
use futures::io::{AsyncRead, AsyncWrite};
use libp2p::core::{upgrade, InboundUpgrade, OutboundUpgrade, UpgradeInfo};
use std::pin::Pin;
use std::{io, iter};

// Undocumented, but according to JS we our messages have a max size of 512*1024
// https://github.com/ipfs/js-ipfs-bitswap/blob/d8f80408aadab94c962f6b88f343eb9f39fa0fcc/src/decision-engine/index.js#L16
const MAX_BUF_SIZE: usize = 524_288;

#[derive(Clone, Debug, Default)]
pub struct BitswapConfig {}

impl UpgradeInfo for BitswapConfig {
    type Info = &'static [u8];
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        // b"/ipfs/bitswap", b"/ipfs/bitswap/1.0.0"
        iter::once(b"/ipfs/bitswap/1.1.0")
    }
}

impl<TSocket> InboundUpgrade<TSocket> for BitswapConfig
where
    TSocket: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Output = Message<I>;
    type Error = Error;
    #[allow(clippy::type_complexity)]
    type Future = Pin<Box<dyn Future<Output = Result<Self::Output, Self::Error>> + Send>>;

    #[inline]
    fn upgrade_inbound(self, mut socket: TSocket, info: Self::Info) -> Self::Future {
        Box::pin(async move {
            debug!("upgrade_inbound: {}", std::str::from_utf8(info).unwrap());
            let packet = upgrade::read_one(&mut socket, MAX_BUF_SIZE).await?;
            let message = Message::from_bytes(&packet)?;
            debug!("inbound message: {:?}", message);
            Ok(message)
        })
    }
}

#[derive(Debug)]
pub enum BitswapError {
    ReadError(upgrade::ReadOneError),
    ProtobufError(prost::DecodeError),
}

impl From<upgrade::ReadOneError> for BitswapError {
    #[inline]
    fn from(err: upgrade::ReadOneError) -> Self {
        BitswapError::ReadError(err)
    }
}

impl From<prost::DecodeError> for BitswapError {
    #[inline]
    fn from(err: prost::DecodeError) -> Self {
        BitswapError::ProtobufError(err)
    }
}

impl std::fmt::Display for BitswapError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            BitswapError::ReadError(ref err) => {
                write!(f, "Error while reading from socket: {}", err)
            }
            BitswapError::ProtobufError(ref err) => {
                write!(f, "Error while decoding protobuf: {}", err)
            }
        }
    }
}

impl std::error::Error for BitswapError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match *self {
            BitswapError::ReadError(ref err) => Some(err),
            BitswapError::ProtobufError(ref err) => Some(err),
        }
    }
}

impl UpgradeInfo for Message<O> {
    type Info = &'static [u8];
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        // b"/ipfs/bitswap", b"/ipfs/bitswap/1.0.0"
        iter::once(b"/ipfs/bitswap/1.1.0")
    }
}

impl<TSocket> OutboundUpgrade<TSocket> for Message<O>
where
    TSocket: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Output = ();
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Output, Self::Error>> + Send>>;

    #[inline]
    fn upgrade_outbound(self, mut socket: TSocket, info: Self::Info) -> Self::Future {
        Box::pin(async move {
            debug!("upgrade_outbound: {}", std::str::from_utf8(info).unwrap());
            let bytes = self.to_bytes();
            upgrade::write_one(&mut socket, bytes).await?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    /*
    use futures::prelude::*;
    use libp2p::core::upgrade;
    use super::*;
    use tokio::net::{TcpListener, TcpStream};

    // TODO: rewrite tests with the MemoryTransport
    // TODO: figure out why it doesn't exit
    #[test]
    #[ignore]
    fn test_upgrade() {
        // yeah this probably did not work before
        let listener = TcpListener::bind(&"127.0.0.1:0".parse().unwrap()).unwrap();
        let listener_addr = listener.local_addr().unwrap();

        let _server = listener
            .incoming()
            .into_future()
            .map_err(|(e, _)| e)
            .and_then(|(c, _)| {
                println!("upgrading server");
                upgrade::apply_inbound(c.unwrap(), BitswapConfig::default())
                    .map_err(|_| panic!())
            })
            .map(|_| ());

        let _client = TcpStream::connect(&listener_addr)
            .and_then(|c| {
                println!("upgrading client");
                upgrade::apply_outbound(c, Message::new())
                    .map_err(|_| panic!())
            });

        //tokio::run(server.select(client).map(|_| ()).map_err(|_| panic!()));
    }
    */
}
