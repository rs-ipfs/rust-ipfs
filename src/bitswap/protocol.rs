/// Reperesents a prototype for an upgrade to handle the bitswap protocol.
///
/// The protocol works the following way:
///
/// - TODO

use crate::bitswap::ledger::{Message, I, O};
use crate::error::Error;
use libp2p::core::{InboundUpgrade, OutboundUpgrade, UpgradeInfo, upgrade::{self, Negotiated}};
use protobuf::ProtobufError;
use std::{io, iter};
use tokio::prelude::*;

// Undocumented, but according to JS we our messages have a max size of 512*1024
// https://github.com/ipfs/js-ipfs-bitswap/blob/d8f80408aadab94c962f6b88f343eb9f39fa0fcc/src/decision-engine/index.js#L16
const MAX_BUF_SIZE : usize = 524288;

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

impl<C> InboundUpgrade<C> for BitswapConfig
    where C: AsyncRead + AsyncWrite
{
    type Output = Message<I>;
    type Error = Error;
    type Future = upgrade::ReadOneThen<Negotiated<C>, (), fn(Vec<u8>, ()) -> Result<Self::Output, Self::Error>>;

    #[inline]
    fn upgrade_inbound(self, socket: Negotiated<C>, info: Self::Info) -> Self::Future {
        debug!("upgrade_inbound: {}", std::str::from_utf8(info).unwrap());
        upgrade::read_one_then(socket, MAX_BUF_SIZE, (), |packet, ()| {
            let message = Message::from_bytes(&packet)?;
            debug!("inbound message: {:?}", message);
            Ok(message)
        })
    }
}

#[derive(Debug)]
pub enum BitswapError {
    ReadError(upgrade::ReadOneError),
    ProtobufError(ProtobufError),
}

impl From<upgrade::ReadOneError> for BitswapError {
    #[inline]
    fn from(err: upgrade::ReadOneError) -> Self {
        BitswapError::ReadError(err)
    }
}

impl From<ProtobufError> for BitswapError {
    #[inline]
    fn from(err: ProtobufError) -> Self {
        BitswapError::ProtobufError(err)
    }
}

impl std::fmt::Display for BitswapError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            BitswapError::ReadError(ref err) =>
                write!(f, "Error while reading from socket: {}", err),
            BitswapError::ProtobufError(ref err) =>
                write!(f, "Error while decoding protobuf: {}", err),
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

impl<C> OutboundUpgrade<C> for Message<O>
where
    C: AsyncRead + AsyncWrite,
{
    type Output = ();
    type Error = io::Error;
    type Future = upgrade::WriteOne<Negotiated<C>>;

    #[inline]
    fn upgrade_outbound(self, socket: Negotiated<C>, info: Self::Info) -> Self::Future {
        debug!("upgrade_outbound: {}", std::str::from_utf8(info).unwrap());
        let bytes = self.into_bytes();
        upgrade::write_one(socket, bytes)
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
    fn test_upgrade() {
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
    }*/
}
