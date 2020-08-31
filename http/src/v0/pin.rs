use crate::v0::support::option_parsing::ParseError;
use crate::v0::support::{with_ipfs, StringError, StringSerialized};
use futures::future::try_join_all;
use ipfs::{Cid, Ipfs, IpfsTypes};
use serde::Serialize;
use std::convert::TryFrom;
use warp::{reply, Filter, Rejection, Reply};

mod add;

/// `pin/add` per https://docs.ipfs.io/reference/http/api/#api-v0-pin-add or the
/// interface-ipfs-http test suite.
pub fn add<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    with_ipfs(ipfs)
        .and(add::add_request())
        .and_then(add::add_inner)
}

pub fn list<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    with_ipfs(ipfs).and_then(list_inner)
}

async fn list_inner<T: IpfsTypes>(_ipfs: Ipfs<T>) -> Result<impl Reply, Rejection> {
    // interestingly conformance tests call this with `paths=cid&stream=true&arg=cid`
    // this needs to be a stream of the listing
    Err::<&'static str, _>(crate::v0::NotImplemented.into())
}

pub fn rm<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    with_ipfs(ipfs).and_then(rm_inner)
}

async fn rm_inner<T: IpfsTypes>(_ipfs: Ipfs<T>) -> Result<impl Reply, Rejection> {
    Err::<&'static str, _>(crate::v0::NotImplemented.into())
}
