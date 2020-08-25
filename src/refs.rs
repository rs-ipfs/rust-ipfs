use crate::ipld::{decode_ipld, Ipld};
use crate::{Block, Ipfs, IpfsTypes};
use async_stream::stream;
use cid::{self, Cid};
use futures::stream::Stream;
use std::borrow::Borrow;
use std::collections::HashSet;
use std::collections::VecDeque;

/// Gather links as edges between two documents from all of the `iplds` which represent the
/// document and it's original `Cid`, as the `Ipld` can be a subtree of the document.
///
/// **Stream** does not stop on **error**.
///
/// # Differences from other implementations
///
/// `js-ipfs` does seem to do a recursive descent on all links. Looking at the tests it would
/// appear that `go-ipfs` implements this in similar fashion. This implementation is breadth-first
/// to be simpler at least.
///
/// Related: https://github.com/ipfs/js-ipfs/pull/2982
///
/// # Lifetime of returned stream
///
/// Depending on how this function is called, the lifetime will be tied to the lifetime of given
/// `&Ipfs` or `'static` when given ownership of `Ipfs`.
pub fn iplds_refs<'a, Types, MaybeOwned, Iter>(
    ipfs: MaybeOwned,
    iplds: Iter,
    max_depth: Option<u64>,
    unique: bool,
) -> impl Stream<Item = Result<(Cid, Cid, Option<String>), String>> + Send + 'a
where
    Types: IpfsTypes,
    MaybeOwned: Borrow<Ipfs<Types>> + Send + 'a,
    Iter: IntoIterator<Item = (Cid, Ipld)>,
{
    let mut work = VecDeque::new();
    let mut queued_or_visited = HashSet::new();

    // double check the max_depth before filling the work and queued_or_visited up just in case we
    // are going to be returning an empty stream
    if max_depth.map(|n| n > 0).unwrap_or(true) {
        for (origin, ipld) in iplds {
            for (link_name, next_cid) in ipld_links(&origin, ipld) {
                if unique && !queued_or_visited.insert(next_cid.clone()) {
                    trace!("skipping already queued {}", next_cid);
                    continue;
                }
                work.push_back((0, next_cid, origin.clone(), link_name));
            }
        }
    }

    stream! {
        if let Some(0) = max_depth {
            return;
        }

        while let Some((depth, cid, source, link_name)) = work.pop_front() {
            let traverse_links = match max_depth {
                Some(d) if d <= depth => {
                    // important to continue instead of stopping
                    continue;
                },
                // no need to list links which would be filtered out
                Some(d) if d + 1 == depth => false,
                _ => true
            };

            // if this is not bound to a local variable it'll introduce a Sync requirement on
            // `MaybeOwned` which we don't necessarily need.
            let borrowed = ipfs.borrow();

            let data = match borrowed.get_block(&cid).await {
                Ok(Block { data, .. }) => data,
                Err(e) => {
                    warn!("failed to load {}, linked from {}: {}", cid, source, e);
                    // TODO: yield error msg
                    // unsure in which cases this happens, because we'll start to search the content
                    // and stop only when request has been cancelled (FIXME: no way to stop this
                    // operation)
                    continue;
                }
            };

            let ipld = match decode_ipld(&cid, &data) {
                Ok(ipld) => ipld,
                Err(e) => {
                    warn!("failed to parse {}, linked from {}: {}", cid, source, e);
                    // go-ipfs on raw Qm hash:
                    // > failed to decode Protocol Buffers: incorrectly formatted merkledag node: unmarshal failed. proto: illegal wireType 6
                    yield Err(e.to_string());
                    continue;
                }
            };

            if traverse_links {
                for (link_name, next_cid) in ipld_links(&cid, ipld) {
                    if unique && !queued_or_visited.insert(next_cid.clone()) {
                        trace!("skipping already queued {}", next_cid);
                        continue;
                    }

                    work.push_back((depth + 1, next_cid, cid.clone(), link_name));
                }
            }

            yield Ok((source, cid, link_name));
        }
    }
}

fn ipld_links(
    cid: &Cid,
    ipld: Ipld,
) -> impl Iterator<Item = (Option<String>, Cid)> + Send + 'static {
    // a wrapping iterator without there being a libipld_base::IpldIntoIter might not be doable
    // with safe code
    let items = if cid.codec() == cid::Codec::DagProtobuf {
        dagpb_links(ipld)
    } else {
        ipld.iter()
            .filter_map(|val| match val {
                Ipld::Link(cid) => Some(cid),
                _ => None,
            })
            .cloned()
            // only dag-pb ever has any link names, probably because in cbor the "name" on the LHS
            // might have a different meaning from a "link name" in dag-pb ... Doesn't seem
            // immediatedly obvious why this is done.
            .map(|cid| (None, cid))
            .collect::<Vec<(Option<String>, Cid)>>()
    };

    items.into_iter()
}

/// Special handling for the structure created while loading dag-pb as ipld.
///
/// # Panics
///
/// If the dag-pb ipld tree doesn't conform to expectations, as in, we are out of sync with the
/// libipld crate. This is on purpose.
fn dagpb_links(ipld: Ipld) -> Vec<(Option<String>, Cid)> {
    let links = match ipld {
        Ipld::Map(mut m) => m.remove("Links"),
        // lets assume this means "no links"
        _ => return Vec::new(),
    };

    let links = match links {
        Some(Ipld::List(v)) => v,
        x => panic!("Expected dag-pb2ipld \"Links\" to be a list, got: {:?}", x),
    };

    links
        .into_iter()
        .enumerate()
        .filter_map(|(i, ipld)| {
            match ipld {
                Ipld::Map(mut m) => {
                    let link = match m.remove("Hash") {
                        Some(Ipld::Link(cid)) => cid,
                        Some(x) => panic!(
                            "Expected dag-pb2ipld \"Links[{}]/Hash\" to be a link, got: {:?}",
                            i, x
                        ),
                        None => return None,
                    };
                    let name = match m.remove("Name") {
                        // not sure of this, not covered by tests, though these are only
                        // present for multi-block files so maybe it's better to panic
                        Some(Ipld::String(s)) if s == "/" => {
                            unimplemented!("Slashes as the name of link")
                        }
                        Some(Ipld::String(s)) => Some(s),
                        Some(x) => panic!(
                            "Expected dag-pb2ipld \"Links[{}]/Name\" to be a string, got: {:?}",
                            i, x
                        ),
                        // not too sure of this, this could be the index as string as well?
                        None => unimplemented!(
                            "Default name for dag-pb2ipld links, should it be index?"
                        ),
                    };

                    Some((name, link))
                }
                x => panic!(
                    "Expected dag-pb2ipld \"Links[{}]\" to be a map, got: {:?}",
                    i, x
                ),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::ipld_links;
    use crate::ipld::decode_ipld;
    use cid::Cid;
    use hex_literal::hex;
    use std::convert::TryFrom;

    #[test]
    fn dagpb_links() {
        // this is the same as in ipfs-http::v0::refs::path::tests::walk_dagpb_links
        let payload = hex!(
            "12330a2212206aad27d7e2fc815cd15bf679535062565dc927a831547281
            fc0af9e5d7e67c74120b6166726963616e2e747874180812340a221220fd
            36ac5279964db0cba8f7fa45f8c4c44ef5e2ff55da85936a378c96c9c632
            04120c616d6572696361732e747874180812360a2212207564c20415869d
            77a8a40ca68a9158e397dd48bdff1325cdb23c5bcd181acd17120e617573
            7472616c69616e2e7478741808"
        );

        let cid = Cid::try_from("QmbrFTo4s6H23W6wmoZKQC2vSogGeQ4dYiceSqJddzrKVa").unwrap();

        let decoded = decode_ipld(&cid, &payload).unwrap();

        let links = ipld_links(&cid, decoded)
            .map(|(name, _)| name.unwrap())
            .collect::<Vec<_>>();

        assert_eq!(links, ["african.txt", "americas.txt", "australian.txt",]);
    }
}
