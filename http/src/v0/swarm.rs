use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Deserialize)]
pub struct ConnectQuery {
    arg: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ConnectResponse {
    strings: Vec<String>,
}

pub async fn connect(_query: ConnectQuery) -> Result<impl warp::Reply, std::convert::Infallible> {
    let response = ConnectResponse { strings: vec![] };
    Ok(warp::reply::json(&response))
}

#[derive(Debug, Deserialize)]
pub struct PeersQuery {
    verbose: Option<bool>,
    streams: Option<bool>,
    latency: Option<bool>,
    direction: Option<bool>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct PeersResponse {
    peers: Vec<Peer>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct Peer {
    addr: String,
    direction: u32,
    latency: String,
    muxer: String,
    peer: String,
    streams: Vec<Stream>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct Stream {
    protocol: String,
}

pub async fn peers(_query: PeersQuery) -> Result<impl warp::Reply, std::convert::Infallible> {
    let response = PeersResponse { peers: vec![] };
    Ok(warp::reply::json(&response))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct AddrsResponse {
    addrs: BTreeMap<String, Vec<String>>,
}

pub async fn addrs() -> Result<impl warp::Reply, std::convert::Infallible> {
    let response = AddrsResponse {
        addrs: BTreeMap::new(),
    };
    Ok(warp::reply::json(&response))
}

#[derive(Debug, Deserialize)]
pub struct AddrsLocalQuery {
    id: Option<bool>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct AddrsLocalResponse {
    strings: Vec<String>,
}

pub async fn addrs_local(
    _query: AddrsLocalQuery,
) -> Result<impl warp::Reply, std::convert::Infallible> {
    let response = AddrsLocalResponse { strings: vec![] };
    Ok(warp::reply::json(&response))
}

#[derive(Debug, Deserialize)]
pub struct DisconnectQuery {
    arg: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct DisconnectResponse {
    strings: Vec<String>,
}

pub async fn disconnect(
    _query: DisconnectQuery,
) -> Result<impl warp::Reply, std::convert::Infallible> {
    let response = DisconnectResponse { strings: vec![] };
    Ok(warp::reply::json(&response))
}
