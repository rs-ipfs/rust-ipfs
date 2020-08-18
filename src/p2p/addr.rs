use libp2p::{
    multiaddr::{self, Protocol},
    Multiaddr, PeerId,
};
use std::{
    convert::{TryFrom, TryInto},
    fmt,
    str::FromStr,
};

/// An error that can be thrown when converting to `MultiaddrWithPeerId` and
/// `MultiaddrWithoutPeerId`.
#[derive(Debug)]
pub enum MultiaddrWrapperError {
    /// The source `Multiaddr` unexpectedly contains `Protocol::P2p`.
    ContainsProtocolP2p,
    /// The provided `Multiaddr` is invalid.
    InvalidMultiaddr(multiaddr::Error),
    /// The `PeerId` created based on the `Protocol::P2p` is invalid.
    InvalidPeerId,
    /// The `Protocol::P2p` is unexpectedly missing from the source `Multiaddr`.
    MissingProtocolP2p,
}

impl fmt::Display for MultiaddrWrapperError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for MultiaddrWrapperError {}

/// A wrapper for `Multiaddr` that does **not** contain `Protocol::P2p`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MultiaddrWithoutPeerId(Multiaddr);

impl TryFrom<Multiaddr> for MultiaddrWithoutPeerId {
    type Error = MultiaddrWrapperError;

    fn try_from(addr: Multiaddr) -> Result<Self, Self::Error> {
        if addr.iter().any(|p| matches!(p, Protocol::P2p(_))) {
            Err(MultiaddrWrapperError::ContainsProtocolP2p)
        } else {
            Ok(Self(addr))
        }
    }
}

impl From<MultiaddrWithoutPeerId> for Multiaddr {
    fn from(addr: MultiaddrWithoutPeerId) -> Self {
        let MultiaddrWithoutPeerId(multiaddr) = addr;
        multiaddr
    }
}

impl AsRef<Multiaddr> for MultiaddrWithoutPeerId {
    fn as_ref(&self) -> &Multiaddr {
        &self.0
    }
}

impl FromStr for MultiaddrWithoutPeerId {
    type Err = MultiaddrWrapperError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let multiaddr = s
            .parse::<Multiaddr>()
            .map_err(MultiaddrWrapperError::InvalidMultiaddr)?;
        multiaddr.try_into()
    }
}

/// A `Multiaddr` paired with a discrete `PeerId`. The `Multiaddr` can contain a
/// `Protocol::P2p`, but it's not as easy to work with, and some functionalities
/// don't support it being contained within the `Multiaddr`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MultiaddrWithPeerId {
    pub multiaddr: MultiaddrWithoutPeerId,
    pub peer_id: PeerId,
}

impl From<(MultiaddrWithoutPeerId, PeerId)> for MultiaddrWithPeerId {
    fn from((multiaddr, peer_id): (MultiaddrWithoutPeerId, PeerId)) -> Self {
        Self { multiaddr, peer_id }
    }
}

impl TryFrom<Multiaddr> for MultiaddrWithPeerId {
    type Error = MultiaddrWrapperError;

    fn try_from(mut multiaddr: Multiaddr) -> Result<Self, Self::Error> {
        if let Some(Protocol::P2p(hash)) = multiaddr.pop() {
            let multiaddr = MultiaddrWithoutPeerId(multiaddr);
            let peer_id =
                PeerId::from_multihash(hash).map_err(|_| MultiaddrWrapperError::InvalidPeerId)?;
            Ok(Self { multiaddr, peer_id })
        } else {
            Err(MultiaddrWrapperError::MissingProtocolP2p)
        }
    }
}

impl FromStr for MultiaddrWithPeerId {
    type Err = MultiaddrWrapperError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let multiaddr = s
            .parse::<Multiaddr>()
            .map_err(MultiaddrWrapperError::InvalidMultiaddr)?;
        Self::try_from(multiaddr)
    }
}

impl fmt::Display for MultiaddrWithPeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/p2p/{}", self.multiaddr.as_ref(), self.peer_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_targets() {
        let peer_id = "QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ";
        let multiaddr_wo_peer = "/ip4/104.131.131.82/tcp/4001";
        let multiaddr_with_peer = format!("{}/p2p/{}", multiaddr_wo_peer, peer_id);
        let p2p_peer = format!("/p2p/{}", peer_id);
        // note: /ipfs/peer_id doesn't properly parse as a Multiaddr

        assert!(multiaddr_wo_peer.parse::<MultiaddrWithoutPeerId>().is_ok());
        assert!(multiaddr_with_peer.parse::<MultiaddrWithPeerId>().is_ok());
        assert!(p2p_peer.parse::<Multiaddr>().is_ok());
    }
}
