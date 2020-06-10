use std::convert::TryFrom;
use crate::v0::support::unshared::Unshared;
use crate::v0::support::{with_ipfs, StringError, StreamResponse};
use libipld::cid::Codec;
use ipfs::{IpfsTypes, Ipfs};
use serde::Deserialize;
use warp::{path, query, Reply, Rejection, Filter};

#[derive(Debug, Deserialize)]
pub struct CatArgs {
    // this could be an ipfs path
    arg: String,
    offset: Option<u64>,
    length: Option<u64>,
    // timeout: Option<?> // added in latest iterations
}

pub fn cat<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    path!("cat")
        .and(with_ipfs(ipfs))
        .and(query::<CatArgs>())
        .and_then(cat_inner)
}

async fn cat_inner<T: IpfsTypes>(ipfs: Ipfs<T>, args: CatArgs) -> Result<impl Reply, Rejection> {
    use crate::v0::refs::{IpfsPath, walk_path};
    use ipfs::unixfs::{TraversalFailed, ll::file::FileReadFailed};

    let path = IpfsPath::try_from(args.arg.as_str()).map_err(StringError::from)?;

    let range = match (args.offset, args.length) {
        (Some(start), Some(len)) => Some(start..(start + len)),
        (Some(_start), None) => { todo!("need to abstract over the range") },
        (None, Some(len)) => Some(0..len),
        (None, None) => None,
    };

    // FIXME: this is here until we have IpfsPath back at ipfs

    let (cid, _) = walk_path(&ipfs, path).await.map_err(StringError::from)?;

    if cid.codec() != Codec::DagProtobuf {
        return Err(StringError::from("unknown node type").into());
    }

    // TODO: timeout
    let stream = match ipfs::unixfs::cat(ipfs, cid, range).await {
        Ok(stream) => stream,
        Err(TraversalFailed::Walking(_, FileReadFailed::UnexpectedType(ut))) if ut.is_directory() => {
            return Err(StringError::from("this dag node is a directory").into())
        },
        Err(e) => return Err(StringError::from(e).into()),
    };

    Ok(StreamResponse(Unshared::new(stream)))
}
