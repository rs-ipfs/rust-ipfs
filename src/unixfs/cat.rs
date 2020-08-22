use crate::{Error, Ipfs, IpfsTypes};
use async_stream::stream;
use bitswap::Block;
use cid::Cid;
use core::borrow::Borrow;
use core::fmt;
use core::ops::Range;
use futures::stream::Stream;
use ipfs_unixfs::file::{visit::IdleFileVisit, FileReadFailed};

/// IPFS cat operation, producing a stream of file bytes. This is generic over the different kinds
/// of ways to own an `Ipfs` value in order to support both operating with borrowed `Ipfs` value
/// and an owned value. Passing an owned value allows the return value to be `'static`, which can
/// be helpful in some contexts, like the http.
///
/// Returns a stream of bytes on the file pointed with the Cid.
pub async fn cat<'a, Types, MaybeOwned>(
    ipfs: MaybeOwned,
    starting_point: impl Into<StartingPoint>,
    range: Option<Range<u64>>,
) -> Result<impl Stream<Item = Result<Vec<u8>, TraversalFailed>> + Send + 'a, TraversalFailed>
where
    Types: IpfsTypes,
    MaybeOwned: Borrow<Ipfs<Types>> + Send + 'a,
{
    let mut visit = IdleFileVisit::default();
    if let Some(range) = range {
        visit = visit.with_target_range(range);
    }

    // Get the root block to start the traversal. The stream does not expose any of the file
    // metadata. To get to it the user needs to create a Visitor over the first block.
    let Block { cid, data } = match starting_point.into() {
        StartingPoint::Left(cid) => {
            let borrow = ipfs.borrow();
            match borrow.get_block(&cid).await {
                Ok(block) => block,
                Err(e) => {
                    return Err(TraversalFailed::Loading(cid, e));
                }
            }
        }
        StartingPoint::Right(block) => block,
    };

    let mut cache = None;
    // Start the visit from the root block. We need to move the both components as Options into the
    // stream as we can't yet return them from this Future context.
    let (visit, bytes) = match visit.start(&data) {
        Ok((bytes, _, _, visit)) => {
            let bytes = if !bytes.is_empty() {
                Some(bytes.to_vec())
            } else {
                None
            };

            (visit, bytes)
        }
        Err(e) => {
            return Err(TraversalFailed::Walking(cid, e));
        }
    };

    // FIXME: we could use the above file_size to set the content-length ... but calculating it
    // with the ranges is not ... trivial?

    // using async_stream here at least to get on faster; writing custom streams is not too easy
    // but this might be easy enough to write open.
    Ok(stream! {

        if let Some(bytes) = bytes {
            yield Ok(bytes);
        }

        let mut visit = match visit {
            Some(visit) => visit,
            None => return,
        };

        loop {
            // TODO: if it was possible, it would make sense to start downloading N of these
            // we could just create an FuturesUnordered which would drop the value right away. that
            // would probably always cost many unnecessary clones, but it would be nice to "shut"
            // the subscriber so that it will only resolve to a value but still keep the operation
            // going. Not that we have any "operation" concept of the Want yet.
            let (next, _) = visit.pending_links();

            let borrow = ipfs.borrow();
            let Block { cid, data } = match borrow.get_block(&next).await {
                Ok(block) => block,
                Err(e) => {
                    yield Err(TraversalFailed::Loading(next.to_owned(), e));
                    return;
                },
            };

            match visit.continue_walk(&data, &mut cache) {
                Ok((bytes, next_visit)) => {
                    if !bytes.is_empty() {
                        // TODO: manual implementation could allow returning just the slice
                        yield Ok(bytes.to_vec());
                    }

                    match next_visit {
                        Some(v) => visit = v,
                        None => return,
                    }
                }
                Err(e) => {
                    yield Err(TraversalFailed::Walking(cid, e));
                    return;
                }
            }
        }
    })
}

pub enum StartingPoint {
    Left(Cid),
    Right(Block),
}

impl From<Cid> for StartingPoint {
    fn from(a: Cid) -> Self {
        Self::Left(a)
    }
}

impl From<Block> for StartingPoint {
    fn from(b: Block) -> Self {
        Self::Right(b)
    }
}

/// Types of failures which can occur while walking the UnixFS graph.
#[derive(Debug)]
pub enum TraversalFailed {
    /// Failure to load the block
    Loading(Cid, Error),
    /// Processing of the block failed
    Walking(Cid, FileReadFailed),
}

impl fmt::Display for TraversalFailed {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        use TraversalFailed::*;
        match self {
            Loading(cid, e) => write!(fmt, "loading of {} failed: {}", cid, e),
            Walking(cid, e) => write!(fmt, "failed to walk {}: {}", cid, e),
        }
    }
}

impl std::error::Error for TraversalFailed {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        use TraversalFailed::*;

        match self {
            Loading(_, _) => {
                // FIXME: anyhow::Error cannot be given out as source.
                None
            }
            Walking(_, e) => Some(e),
        }
    }
}
