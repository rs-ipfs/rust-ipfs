//! Static configuration (the bootstrap node(s)).

/// The supported bootstrap nodes (/dnsaddr is not yet supported). This will be updated to contain
/// the latest known supported IPFS bootstrap peers.
// FIXME: it would be nice to parse these into MultiaddrWithPeerId with const fn.
pub const BOOTSTRAP_NODES: &[&str] =
    &["/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ"];

#[cfg(test)]
mod tests {
    use crate::p2p::MultiaddrWithPeerId;

    #[test]
    fn bootstrap_nodes_are_multiaddr_with_peerid() {
        super::BOOTSTRAP_NODES
            .iter()
            .try_for_each(|s| s.parse::<MultiaddrWithPeerId>().map(|_| ()))
            .unwrap();
    }
}
