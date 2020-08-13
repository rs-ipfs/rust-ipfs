use crate::dag::IpldDag;
use crate::error::Error;
use crate::ipld::{dag_pb::PbNode, Ipld};
use crate::path::IpfsPath;
use crate::repo::RepoTypes;
use async_std::fs;
use async_std::io::ReadExt;
use async_std::path::PathBuf;
use cid::{Cid, Codec};
use std::collections::BTreeMap;
use std::convert::TryInto;

pub use ipfs_unixfs as ll;

mod cat;
pub use cat::{cat, TraversalFailed};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_cid() {
        // note: old versions of `ipfs::unixfs::File` was an interface where user would provide the
        // unixfs encoded data. this test case has been migrated to put the "content" as the the
        // file data instead of the unixfs encoding. the previous way used to produce
        // QmSy5pnHk1EnvE5dmJSyFKG5unXLGjPpBuJJCBQkBTvBaW.
        let content = "\u{8}\u{2}\u{12}\u{12}Here is some data\n\u{18}\u{12}";

        let mut adder = ipfs_unixfs::file::adder::FileAdder::default();
        let (mut blocks, consumed) = adder.push(content.as_bytes());
        assert_eq!(consumed, content.len(), "should had consumed all content");
        assert_eq!(
            blocks.next(),
            None,
            "should not had produced any blocks yet"
        );

        let mut blocks = adder.finish();

        let (cid, _block) = blocks.next().unwrap();
        assert_eq!(blocks.next(), None, "should had been the last");

        assert_eq!(
            "QmQZE72h2Vdm3F5gWr9RLuzSw3rUJEkKedWEa8t8XVygT5",
            cid.to_string(),
            "matches cid from go-ipfs 0.6.0"
        );
    }
}
