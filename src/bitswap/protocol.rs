/// Reperesents a prototype for an upgrade to handle the bitswap protocol.
///
/// The protocol works the following way:
///
/// - TODO
use libp2p::core::{InboundUpgrade, OutboundUpgrade, UpgradeInfo};
use std::{io, iter};
use tokio::prelude::*;

#[derive(Default, Debug, Copy, Clone)]
pub struct Bitswap;

impl UpgradeInfo for Bitswap {
    type Info = &'static [u8];
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(b"/ipfs/bitswap/1.0.0", b"/ipfs/bitswap/1.1.0")
    }
}

impl<TSocket> InboundUpgrade<TSocket> for Bitswap
where
    TSocket: AsyncRead + AsyncWrite,
{
    type Output = ();
    type Error = io::Error;
    type Future = ();

    #[inline]
    fn upgrade_inbound(self, socket: TSocket, _: Self::Info) -> Self::Future {

    }
}

impl<TSocket> OutboundUpgrade<TSocket> for Bitswap
where
    TSocket: AsyncRead + AsyncWrite,
{
    type Output = ();
    type Error = io::Error;
    type Future = ();

    #[inline]
    fn upgrade_outbound(self, socket: TSocket, _: Self::Info) -> Self::Future {

    }
}

#[cfg(test)]
mod tests {
    use tokio_tcp::{TcpListener, TcpStream};
    use super::Bitswap;
    use futures::{Future, Stream};
    use libp2p::core::upgrade;

    // TODO: rewrite tests with the MemoryTransport

    #[test]
    fn want_receive() {
        let listener = TcpListener::bind(&"127.0.0.1:0".parse().unwrap()).unwrap();
        let listener_addr = listener.local_addr().unwrap();

        let server = listener
            .incoming()
            .into_future()
            .map_err(|(e, _)| e)
            .and_then(|(c, _)| {
                upgrade::apply_inbound(c.unwrap(), Bitswap::default())
                    .map_err(|_| panic!())
            });

        let client = TcpStream::connect(&listener_addr)
            .and_then(|c| {
                upgrade::apply_outbound(c, Bitswap::default())
                    .map_err(|_| panic!())
            })
            .map(|_| ());

        let mut runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(server.select(client).map_err(|_| panic!())).unwrap();
    }

    #[test]
    fn provide_want_send() {

    }
}
