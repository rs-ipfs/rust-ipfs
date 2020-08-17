use super::{with_ipfs, InvalidPeerId, NotImplemented, StringError};
use ipfs::{Ipfs, IpfsTypes, PeerId};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use warp::{query, Filter};

pub fn identity<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    with_ipfs(ipfs)
        .and(optional_peer_id())
        .and_then(identity_query)
}

fn optional_peer_id() -> impl Filter<Extract = (Option<PeerId>,), Error = warp::Rejection> + Copy {
    query::<Query>().and_then(|mut q: Query| async move {
        q.arg
            .take()
            .map(|arg| PeerId::from_str(&arg))
            .map_or(Ok(None), |parsed| parsed.map(Some))
            .map_err(|_| warp::reject::custom(InvalidPeerId))
    })
}

// FIXME: /api/v0/id has argument `arg: PeerId` which is not implemented.
//
// https://docs.ipfs.io/reference/api/http/#api-v0-id
async fn identity_query<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    peer: Option<PeerId>,
) -> Result<impl warp::Reply, warp::reject::Rejection> {
    use multibase::Base::Base64Pad;

    if peer.is_some() {
        // TODO: this reply has Id, no public key, addresses and no versions. "no" as in empty
        // string
        return Err(warp::reject::custom(NotImplemented));
    }

    match ipfs.identity().await {
        Ok((public_key, addresses)) => {
            let peer_id = public_key.clone().into_peer_id();
            let id = peer_id.to_string();
            let public_key = Base64Pad.encode(public_key.into_protobuf_encoding());

            let addresses = addresses.into_iter().map(|addr| addr.to_string()).collect();

            let response = Response {
                id,
                public_key,
                addresses,
                agent_version: "rust-ipfs/0.1.0",
                protocol_version: "ipfs/0.1.0",
            };

            Ok(warp::reply::json(&response))
        }
        Err(e) => Err(warp::reject::custom(StringError::from(e))),
    }
}

/// Query string of /api/v0/id?arg=peerid&format=notsure
#[derive(Debug, Deserialize)]
pub struct Query {
    // the peer id to query
    arg: Option<String>,
    // this does not seem to be reacted to by go-ipfs
    format: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
struct Response {
    // PeerId
    #[serde(rename = "ID")]
    id: String,
    // looks like Base64
    public_key: String,
    // Multiaddrs
    addresses: Vec<String>,
    // Multiaddr alike <agent_name>/<version>, like rust-ipfs/0.0.1
    agent_version: &'static str,
    // Multiaddr alike ipfs/0.1.0 ... not sure if there are plans to bump this anytime soon
    protocol_version: &'static str,
}
