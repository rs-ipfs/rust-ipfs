use crate::v0::support::with_ipfs;
use futures::future::join_all;
use ipfs::{Cid, Ipfs, IpfsTypes, Multiaddr};
use serde::{Deserialize, Serialize};
use warp::{path, query, reply, Filter, Rejection, Reply};

#[derive(Debug, Deserialize)]
struct AddRequest {
    //arg: Vec<Cid>,
    progress: Option<bool>,
}

#[derive(Debug, Serialize)]
struct AddResponse {
    //pins: Vec<Cid>,
    progress: usize,
}

async fn pin_block_request<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    request: AddRequest,
) -> Result<impl Reply, Rejection> {
    let request_arg = Vec::new();
    let dispatched_pins = request_arg.iter().map(|x| ipfs.pin_block(&x));
    let completed = join_all(dispatched_pins).await;
    let pins: Vec<Cid> = completed
        .iter()
        .zip(request_arg)
        .filter(|x| x.0.is_ok())
        .map(|x| x.1)
        .collect();
    let response = AddResponse {
        //pins,
        progress: 100,
    };
    Ok(reply::json(&response))
}

pub fn add_pin<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    with_ipfs(ipfs)
        .and(query::<AddRequest>())
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
