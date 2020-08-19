use crate::v0::support::{with_ipfs, MaybeTimeoutExt, StringError, StringSerialized};
use ipfs::{Ipfs, IpfsTypes, PeerId};
use serde::{Deserialize, Serialize};
use warp::{query, Filter, Rejection, Reply};

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
struct Response {
    // blank
    extra: String,
    // blank
    #[serde(rename = "ID")]
    id: String,
    // the actual response
    responses: Vec<ResponsesMember>,
    // TODO: what's this?
    r#type: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
struct ResponsesMember {
    // Multiaddrs
    addrs: Vec<String>,
    // PeerId
    #[serde(rename = "ID")]
    id: String,
}

#[derive(Debug, Deserialize)]
pub struct FindPeerQuery {
    arg: String,
    // FIXME: doesn't seem to be used at the moment
    verbose: Option<bool>,
    timeout: Option<StringSerialized<humantime::Duration>>,
}

async fn find_peer_query<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    query: FindPeerQuery,
) -> Result<impl Reply, Rejection> {
    let FindPeerQuery {
        arg,
        verbose: _,
        timeout,
    } = query;
    let peer_id = arg.parse::<PeerId>().map_err(StringError::from)?;
    let addrs = ipfs
        .find_peer(peer_id.clone())
        .maybe_timeout(timeout.map(StringSerialized::into_inner))
        .await
        .map_err(StringError::from)?
        .map_err(StringError::from)?
        .into_iter()
        .map(|addr| addr.to_string())
        .collect();
    let id = peer_id.to_string();

    let response = Response {
        extra: Default::default(),
        id: Default::default(),
        responses: vec![ResponsesMember { addrs, id }],
        r#type: 2,
    };

    Ok(warp::reply::json(&response))
}

pub fn find_peer<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    with_ipfs(ipfs)
        .and(query::<FindPeerQuery>())
        .and_then(find_peer_query)
}
