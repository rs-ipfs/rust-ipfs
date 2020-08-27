use crate::v0::support::{with_ipfs, StringError};
use futures::future::join_all;
use ipfs::{Cid, Ipfs, IpfsTypes};
use std::convert::TryFrom;
use std::str::FromStr;
use warp::{reply, Filter, Rejection, Reply};

#[derive(Debug)]
struct AddRequest {
    args: Vec<String>,
    recursive: bool,
    progress: bool,
}

async fn pin_block_request<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    request: AddRequest,
) -> Result<impl Reply, Rejection> {
    let cids: Vec<Cid> = request
        .args
        .iter()
        .map(|x| Cid::from_str(&x))
        .collect::<Result<Vec<Cid>, _>>()
        .map_err(StringError::from)?;
    let dispatched_pins = cids.iter().map(|x| ipfs.pin_block(&x));
    let completed = join_all(dispatched_pins).await;
    let pins = completed
        .iter()
        .zip(cids)
        .filter(|x| x.0.is_ok())
        .map(|x| x.1.to_string())
        .map(serde_json::Value::from)
        .collect::<Vec<_>>();
    Ok(reply::json(&serde_json::json!({
        "pins": pins,
        "progress": 100,
    })))
}

pub fn add_pin<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    with_ipfs(ipfs)
        .and(add_request())
        .and_then(pin_block_request)
}

pub fn list<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    with_ipfs(ipfs).and_then(list_inner)
}

async fn list_inner<T: IpfsTypes>(ipfs: Ipfs<T>) -> Result<impl Reply, Rejection> {
    Err::<&'static str, _>(crate::v0::NotImplemented.into())
}

impl<'a> TryFrom<&'a str> for AddRequest {
    type Error = crate::v0::support::option_parsing::ParseError<'a>;

    fn try_from(q: &'a str) -> Result<Self, Self::Error> {
        todo!()
    }
}

/// Filter to perform custom `warp::query::<AddRequest>`. This needs to be copypasted around as
/// HRTB is not quite usable yet.
fn add_request() -> impl Filter<Extract = (AddRequest,), Error = Rejection> + Clone {
    warp::filters::query::raw().and_then(|q: String| {
        let res = AddRequest::try_from(q.as_str())
            .map_err(StringError::from)
            .map_err(warp::reject::custom);

        futures::future::ready(res)
    })
}
