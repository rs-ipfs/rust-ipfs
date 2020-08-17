use anyhow::anyhow;
use libp2p::{multiaddr::Protocol, Multiaddr, PeerId};
use std::{convert::TryFrom, fmt, str::FromStr};

/// A wrapper for `Multiaddr` that does **not** contain `Protocol::P2p`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MultiaddrWithoutPeerId(Multiaddr);

impl From<Multiaddr> for MultiaddrWithoutPeerId {
    fn from(addr: Multiaddr) -> Self {
        Self(
            addr.into_iter()
                .filter(|p| !matches!(p, Protocol::P2p(_)))
                .collect(),
        )
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
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let multiaddr = s.parse::<Multiaddr>()?;
        Ok(multiaddr.into())
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
    type Error = anyhow::Error;

    fn try_from(mut multiaddr: Multiaddr) -> Result<Self, Self::Error> {
        if let Some(Protocol::P2p(hash)) = multiaddr.pop() {
            let multiaddr = MultiaddrWithoutPeerId(multiaddr);
            let peer_id = PeerId::from_multihash(hash)
                .map_err(|_| anyhow!("Invalid Multihash in Protocol::P2p"))?;
            Ok(Self { multiaddr, peer_id })
        } else {
            Err(anyhow!("Missing Protocol::P2p in the Multiaddr"))
        }
    }
}

impl FromStr for MultiaddrWithPeerId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let multiaddr = s.parse::<Multiaddr>()?;
        Self::try_from(multiaddr)
    }
}

impl fmt::Display for MultiaddrWithPeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/p2p/{}", self.multiaddr.as_ref(), self.peer_id)
    }
}
