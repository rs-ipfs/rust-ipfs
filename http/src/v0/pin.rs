use futures::future::join_all;
use crate::v0::support::{with_ipfs};
use ipfs::{Cid, Ipfs, IpfsTypes, Multiaddr};
use serde::{Deserialize, Serialize};
use warp::{reply, Rejection, Reply, Filter, query, path};

#[derive(Debug, Deserialize)]
struct AddRequest {
    arg: Vec<Cid>,
    progress: Option<bool>,
}

#[derive(Debug, Serialize)]
struct AddResponse {
    pins: Vec<Cid>,
    progress: usize,
}

async fn pin_block_request<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    request: AddRequest,
) -> Result<impl Reply, Rejection> {
    let dispatched_pins = request.arg.iter().map(|x| ipfs.pin_block(&x));
    let completed = join_all(dispatched_pins).await;
    let pins: Vec<Cid> = completed
        .iter()
        .zip(request.arg)
        .filter(|x| x.0.is_ok())
        .map(|x| x.1)
        .collect();
    let response = AddResponse {
        pins,
        progress: 100,
    };
    Ok(reply::json(&response))
}

pub fn add_pin<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    path!("pin" / "add").and(with_ipfs(ipfs)).and(query::<AddRequest>()).and_then(pin_block_request)
}
