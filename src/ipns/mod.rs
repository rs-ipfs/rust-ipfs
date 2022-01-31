//! IPNS functionality around [`Ipfs`].

use crate::error::Error;
use crate::path::{IpfsPath, PathRoot};
use crate::repo::RepoTypes;
use crate::Ipfs;

mod dnslink;

/// IPNS facade around [`Ipns`].
#[derive(Clone, Debug)]
pub struct Ipns<Types: RepoTypes> {
    // FIXME(unused): scaffolding while ipns functionality as a whole suggests we should have dht
    // queries etc. here (currently unimplemented).
    _ipfs: Ipfs<Types>,
}

impl<Types: RepoTypes> Ipns<Types> {
    pub fn new(_ipfs: Ipfs<Types>) -> Self {
        Ipns { _ipfs }
    }

    /// Resolves a ipns path to an ipld path.
    pub async fn resolve(&self, path: &IpfsPath) -> Result<IpfsPath, Error> {
        let path = path.to_owned();
        match path.root() {
            PathRoot::Ipld(_) => Ok(path),
            PathRoot::Ipns(_) => Err(anyhow::anyhow!("unimplemented")),
            PathRoot::Dns(domain) => Ok(dnslink::resolve(domain).await?),
        }
    }
}
