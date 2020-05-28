use crate::v0::support::with_ipfs;
use futures::future::join_all;
use ipfs::{Cid, Ipfs, IpfsTypes};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::str::FromStr;
use warp::{path, query, reply, Filter, Rejection, Reply};

#[derive(Debug, Deserialize)]
struct AddRequest {
    args: Vec<String>,
    recursive: bool,
    progress: bool,
}

#[derive(Debug)]
struct AddResponse {
    pins: Vec<String>,
    progress: usize,
}

#[derive(Debug)]
struct InvalidCID;
impl warp::reject::Reject for InvalidCID {}

impl Serialize for AddResponse {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("AddResponse", 2)?;
        let serialized_pins: Vec<String> = self.pins.iter().map(|x| x.to_string()).collect();
        state.serialize_field("pins", &serialized_pins)?;
        state.serialize_field("progress", &self.progress)?;
        state.end()
    }
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
        .map_err(|_err| warp::reject::custom(InvalidCID))?;
    let dispatched_pins = cids.iter().map(|x| ipfs.pin_block(&x));
    let completed = join_all(dispatched_pins).await;
    let pins: Vec<String> = completed
        .iter()
        .zip(cids)
        .filter(|x| x.0.is_ok())
        .map(|x| x.1.to_string())
        .collect();
    let response: AddResponse = AddResponse {
        pins,
        progress: 100,
    };
    Ok(reply::json(&response))
}

pub fn add_pin<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    path!("pin" / "add")
        .and(with_ipfs(ipfs))
        .and(query::<AddRequest>())
        .and_then(pin_block_request)
}
