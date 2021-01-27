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

impl fmt::Display for MultiaddrWithoutPeerId {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, fmt)
    }
}

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

impl From<MultiaddrWithPeerId> for MultiaddrWithoutPeerId {
    fn from(addr: MultiaddrWithPeerId) -> Self {
        let MultiaddrWithPeerId { multiaddr, .. } = addr;
        MultiaddrWithoutPeerId(multiaddr.into())
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

impl PartialEq<Multiaddr> for MultiaddrWithoutPeerId {
    fn eq(&self, other: &Multiaddr) -> bool {
        &self.0 == other
    }
}

impl MultiaddrWithoutPeerId {
    /// Adds the peer_id information to this address without peer_id, turning it into
    /// [`MultiaddrWithPeerId`].
    pub fn with(self, peer_id: PeerId) -> MultiaddrWithPeerId {
        (self, peer_id).into()
    }
}

/// A `Multiaddr` paired with a discrete `PeerId`. The `Multiaddr` can contain a
/// `Protocol::P2p`, but it's not as easy to work with, and some functionalities
/// don't support it being contained within the `Multiaddr`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MultiaddrWithPeerId {
    /// The [`Multiaddr`] without the [`Protocol::P2p`] suffix.
    pub multiaddr: MultiaddrWithoutPeerId,
    /// The peer id from the [`Protocol::P2p`] suffix.
    pub peer_id: PeerId,
}

impl From<(MultiaddrWithoutPeerId, PeerId)> for MultiaddrWithPeerId {
    fn from((multiaddr, peer_id): (MultiaddrWithoutPeerId, PeerId)) -> Self {
        Self { multiaddr, peer_id }
    }
}

impl From<MultiaddrWithPeerId> for Multiaddr {
    fn from(addr: MultiaddrWithPeerId) -> Self {
        let MultiaddrWithPeerId { multiaddr, peer_id } = addr;
        let mut multiaddr: Multiaddr = multiaddr.into();
        multiaddr.push(Protocol::P2p(peer_id.into()));

        multiaddr
    }
}

impl TryFrom<Multiaddr> for MultiaddrWithPeerId {
    type Error = MultiaddrWrapperError;

    fn try_from(multiaddr: Multiaddr) -> Result<Self, Self::Error> {
        if let Some(Protocol::P2p(hash)) = multiaddr.iter().find(|p| matches!(p, Protocol::P2p(_)))
        {
            // FIXME: we've had a case where the PeerId was not the last part of the Multiaddr, which
            // is unexpected; it is hard to trigger, hence this debug-only assertion so we might be
            // able to catch it sometime during tests
            debug_assert!(
                matches!(multiaddr.iter().last(), Some(Protocol::P2p(_)) | Some(Protocol::P2pCircuit)),
                "unexpected Multiaddr format: {}",
                multiaddr
            );

            let multiaddr = MultiaddrWithoutPeerId(
                multiaddr
                    .into_iter()
                    .filter(|p| !matches!(p, Protocol::P2p(_) | Protocol::P2pCircuit))
                    .collect(),
            );
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
        write!(f, "{}/p2p/{}", self.multiaddr, self.peer_id)
    }
}

// Checks if the multiaddr starts with ip4 or ip6 unspecified address, like 0.0.0.0
pub(crate) fn starts_unspecified(addr: &Multiaddr) -> bool {
    match addr.iter().next() {
        Some(Protocol::Ip4(ip4)) if ip4.is_unspecified() => true,
        Some(Protocol::Ip6(ip6)) if ip6.is_unspecified() => true,
        _ => false,
    }
}

pub(crate) fn could_be_bound_from_ephemeral(
    skip: usize,
    bound: &Multiaddr,
    may_have_ephemeral: &Multiaddr,
) -> bool {
    if bound.len() != may_have_ephemeral.len() {
        // no zip_longest in std
        false
    } else {
        // this is could be wrong at least in the future; /p2p/peerid is not a
        // valid suffix but I could imagine some kind of ws or webrtc could
        // give us issues in the long future?
        bound
            .iter()
            .skip(skip)
            .zip(may_have_ephemeral.iter().skip(skip))
            .all(|(left, right)| match (right, left) {
                (Protocol::Tcp(0), Protocol::Tcp(x))
                | (Protocol::Udp(0), Protocol::Udp(x))
                | (Protocol::Sctp(0), Protocol::Sctp(x)) => {
                    assert_ne!(x, 0, "cannot have bound to port 0");
                    true
                }
                (Protocol::Memory(0), Protocol::Memory(x)) => {
                    assert_ne!(x, 0, "cannot have bound to port 0");
                    true
                }
                (right, left) => right == left,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libp2p::build_multiaddr;

    #[test]
    fn connection_targets() {
        let peer_id = "QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ";
        let multiaddr_wo_peer = "/ip4/104.131.131.82/tcp/4001";
        let multiaddr_with_peer = format!("{}/p2p/{}", multiaddr_wo_peer, peer_id);
        let p2p_peer = format!("/p2p/{}", peer_id);
        // note: /ipfs/peer_id doesn't properly parse as a Multiaddr
        let mwp = multiaddr_with_peer.parse::<MultiaddrWithPeerId>().unwrap();

        assert!(multiaddr_wo_peer.parse::<MultiaddrWithoutPeerId>().is_ok());
        assert_eq!(
            Multiaddr::from(mwp),
            multiaddr_with_peer.parse::<Multiaddr>().unwrap()
        );
        assert!(p2p_peer.parse::<Multiaddr>().is_ok());
    }

    #[test]
    fn unspecified_multiaddrs() {
        assert!(starts_unspecified(&build_multiaddr!(
            Ip4([0, 0, 0, 0]),
            Tcp(1u16)
        )));
        assert!(starts_unspecified(&build_multiaddr!(
            Ip6([0, 0, 0, 0, 0, 0, 0, 0]),
            Tcp(1u16)
        )));
    }

    #[test]
    fn localhost_multiaddrs_are_not_unspecified() {
        assert!(!starts_unspecified(&build_multiaddr!(
            Ip4([127, 0, 0, 1]),
            Tcp(1u16)
        )));
        assert!(!starts_unspecified(&build_multiaddr!(
            Ip6([0, 0, 0, 0, 0, 0, 0, 1]),
            Tcp(1u16)
        )));
    }

    #[test]
    fn bound_ephemerals() {
        assert!(could_be_bound_from_ephemeral(
            0,
            &build_multiaddr!(Ip4([127, 0, 0, 1]), Tcp(55555u16)),
            &build_multiaddr!(Ip4([127, 0, 0, 1]), Tcp(0u16))
        ));
        assert!(could_be_bound_from_ephemeral(
            1,
            &build_multiaddr!(Ip4([127, 0, 0, 1]), Tcp(55555u16)),
            &build_multiaddr!(Ip4([127, 0, 0, 1]), Tcp(0u16))
        ));
        assert!(could_be_bound_from_ephemeral(
            1,
            &build_multiaddr!(Ip4([127, 0, 0, 1]), Tcp(55555u16)),
            &build_multiaddr!(Ip4([0, 0, 0, 0]), Tcp(0u16))
        ));
        assert!(could_be_bound_from_ephemeral(
            1,
            &build_multiaddr!(Ip4([127, 0, 0, 1]), Tcp(55555u16)),
            &build_multiaddr!(Ip4([0, 0, 0, 0]), Tcp(0u16))
        ));

        assert!(!could_be_bound_from_ephemeral(
            0,
            &build_multiaddr!(Ip4([192, 168, 0, 1]), Tcp(55555u16)),
            &build_multiaddr!(Ip4([127, 0, 0, 1]), Tcp(0u16))
        ));
        assert!(could_be_bound_from_ephemeral(
            1,
            &build_multiaddr!(Ip4([192, 168, 0, 1]), Tcp(55555u16)),
            &build_multiaddr!(Ip4([127, 0, 0, 1]), Tcp(0u16))
        ));

        assert!(!could_be_bound_from_ephemeral(
            1,
            &build_multiaddr!(Ip4([192, 168, 0, 1]), Tcp(55555u16)),
            &build_multiaddr!(Ip4([127, 0, 0, 1]), Tcp(44444u16))
        ));
    }
}
