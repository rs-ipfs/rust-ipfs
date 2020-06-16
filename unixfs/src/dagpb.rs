///! dag-pb support operations. Placing this module inside unixfs module is a bit unfortunate but
///! follows from the inseparability of dag-pb and UnixFS.
use crate::pb::PBNode;
use std::borrow::Cow;
use std::convert::TryFrom;

/// Extracts the PBNode::Data field from the block as it appears on the block.
pub fn node_data(block: &[u8]) -> Result<Option<&[u8]>, quick_protobuf::Error> {
    let doc = PBNode::try_from(block)?;
    Ok(match doc.Data {
        Some(Cow::Borrowed(slice)) => Some(slice),
        Some(Cow::Owned(_)) => unreachable!("never converted to owned"),
        None => None,
    })
}
