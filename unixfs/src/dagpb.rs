use crate::pb::PBNode;
use crate::InvalidCidInLink;
use cid::Cid;
use std::borrow::Cow;
///! dag-pb support operations. Placing this module inside unixfs module is a bit unfortunate but
///! follows from the inseparability of dag-pb and UnixFS.
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

/// Extracts the PBNode::Links as Cids usable for the ipfs `refs` operation.
pub fn nameless_links(
    block: &[u8],
) -> Result<Result<Vec<Cid>, InvalidCidInLink>, quick_protobuf::Error> {
    let doc = PBNode::try_from(block)?;

    Ok(doc
        .Links
        .into_iter()
        .enumerate()
        .map(|(nth, link)| {
            let hash = link.Hash.as_deref().unwrap_or_default();
            Cid::try_from(hash).map_err(|e| InvalidCidInLink::from((nth, link, e)))
        })
        .collect::<Result<Vec<_>, _>>())
}
