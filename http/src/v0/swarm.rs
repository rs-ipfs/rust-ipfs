use super::support::{with_ipfs, StringError};
use ipfs::{Ipfs, Multiaddr};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::BTreeMap;
use warp::{query, Filter};

#[derive(Debug, Deserialize)]
struct ConnectQuery {
    arg: Multiaddr,
}

async fn connect_query(
    ipfs: Ipfs,
    query: ConnectQuery,
) -> Result<impl warp::Reply, warp::Rejection> {
    ipfs.connect(query.arg)
        .await
        .map_err(|e| warp::reject::custom(StringError::from(e)))?;
    let response: &[&str] = &[];
    Ok(warp::reply::json(&response))
}

pub fn connect(
    ipfs: &Ipfs,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("swarm" / "connect")
        .and(with_ipfs(ipfs))
        .and(query::<ConnectQuery>())
        .and_then(connect_query)
}

#[derive(Debug, Deserialize)]
struct PeersQuery {
    verbose: Option<bool>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
struct PeersResponse {
    peers: Vec<Peer>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
struct Peer {
    addr: String,
    peer: String,
    latency: Option<Cow<'static, str>>,
}

async fn peers_query(ipfs: Ipfs, query: PeersQuery) -> Result<impl warp::Reply, warp::Rejection> {
    let peers = ipfs
        .peers()
        .await
        .map_err(|e| warp::reject::custom(StringError::from(e)))?
        .into_iter()
        .map(|conn| {
            let latency = if let Some(true) = query.verbose {
                // https://github.com/ipfs/js-ipfs/blob/343bd451ce7318751aab9934981e3727c6025234/packages/ipfs/src/core/components/swarm/peers.js#L25
                // suggests that "n/a" is an ok value for latency. we could still follow up on
                // #178 to give the best latency value we can.
                let latency_text = conn
                    .rtt
                    .map(|d| format!("{}ms", d.as_millis() / 2))
                    .map(Cow::Owned)
                    .unwrap_or(Cow::Borrowed("n/a"));

                // as documented in issue #178 the tests will sometimes fail if there is no value
                // for latency (null is output for None, but it isn't truthy as inspected by the
                // js-ipfs-http-client).
                Some(latency_text)
            } else {
                None
            };
            Peer {
                addr: conn.address.to_string(),
                peer: conn.peer_id.to_string(),
                latency,
            }
        })
        .collect();
    let response = PeersResponse { peers };
    Ok(warp::reply::json(&response))
}

pub fn peers(
    ipfs: &Ipfs,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("swarm" / "peers")
        .and(with_ipfs(ipfs))
        .and(query::<PeersQuery>())
        .and_then(peers_query)
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
struct AddrsResponse {
    addrs: BTreeMap<String, Vec<String>>,
}

async fn addrs_query(ipfs: Ipfs) -> Result<impl warp::Reply, warp::Rejection> {
    let addresses = ipfs
        .addrs()
        .await
        .map_err(|e| warp::reject::custom(StringError::from(e)))?;
    let mut res = BTreeMap::new();
    for (peer_id, addrs) in addresses {
        res.insert(
            peer_id.to_string(),
            addrs.into_iter().map(|a| a.to_string()).collect(),
        );
    }
    let response = AddrsResponse { addrs: res };
    Ok(warp::reply::json(&response))
}

pub fn addrs(
    ipfs: &Ipfs,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("swarm" / "addrs")
        .and(with_ipfs(ipfs))
        .and_then(addrs_query)
}

#[derive(Debug, Deserialize)]
struct AddrsLocalQuery {
    id: Option<bool>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
struct AddrsLocalResponse {
    strings: Vec<String>,
}

async fn addrs_local_query(
    ipfs: Ipfs,
    _query: AddrsLocalQuery,
) -> Result<impl warp::Reply, warp::Rejection> {
    let addresses = ipfs
        .addrs_local()
        .await
        .map_err(|e| warp::reject::custom(StringError::from(e)))?
        .into_iter()
        .map(|a| a.to_string())
        .collect();
    let response = AddrsLocalResponse { strings: addresses };
    Ok(warp::reply::json(&response))
}

pub fn addrs_local(
    ipfs: &Ipfs,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("swarm" / "addrs" / "local")
        .and(with_ipfs(ipfs))
        .and(query::<AddrsLocalQuery>())
        .and_then(addrs_local_query)
}

#[derive(Debug, Deserialize)]
struct DisconnectQuery {
    arg: Multiaddr,
}

async fn disconnect_query(
    ipfs: Ipfs,
    query: DisconnectQuery,
) -> Result<impl warp::Reply, warp::Rejection> {
    ipfs.disconnect(query.arg)
        .await
        .map_err(|e| warp::reject::custom(StringError::from(e)))?;
    let response: &[&str] = &[];
    Ok(warp::reply::json(&response))
}

pub fn disconnect(
    ipfs: &Ipfs,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("swarm" / "disconnect")
        .and(with_ipfs(ipfs))
        .and(query::<DisconnectQuery>())
        .and_then(disconnect_query)
}
