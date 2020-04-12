use futures::future::join_all;
use futures::prelude::*;
use ipfs::{Cid, Ipfs, IpfsTypes, Multiaddr};
use serde::{Deserialize, Serialize};
use warp::{reply, Rejection, Reply};

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
