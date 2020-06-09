use crate::v0::support::unshared::Unshared;
use crate::v0::support::{with_ipfs, StringError, StreamResponse};
use libipld::cid::Cid;
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
    // FIXME: this could be an ipfs path as well
    let cid: Cid = args.arg.parse().map_err(StringError::from)?;

    let range = match (args.offset, args.length) {
        (Some(start), Some(len)) => Some(start..(start + len)),
        (Some(_start), None) => { todo!("need to abstract over the range") },
        (None, Some(len)) => Some(0..len),
        (None, None) => None,
    };

    // TODO: timeout
    // TODO: the second parameter should be impl Into<IpfsPath> to support infallible conversions
    // from cid -> IpfsPath
    let stream = ipfs::unixfs::cat(ipfs, cid, range);

    Ok(StreamResponse(Unshared::new(stream)))
}
