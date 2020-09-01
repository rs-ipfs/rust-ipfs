use crate::v0::support::{with_ipfs, StringError, StringSerialized};
use ipfs::{Ipfs, IpfsPath, IpfsTypes};
use serde::{Deserialize, Serialize};
use warp::{query, Filter, Rejection, Reply};

#[derive(Debug, Deserialize)]
pub struct ResolveQuery {
    // the name to resolve
    arg: StringSerialized<IpfsPath>,
    #[serde(rename = "dht-record-count")]
    dht_record_count: Option<usize>,
    #[serde(rename = "dht-timeout")]
    dht_timeout: Option<String>,
}

pub fn resolve<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    with_ipfs(ipfs)
        .and(query::<ResolveQuery>())
        .and_then(resolve_query)
}

async fn resolve_query<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    query: ResolveQuery,
) -> Result<impl Reply, Rejection> {
    let ResolveQuery { arg, .. } = query;
    let name = arg.into_inner();
    let path = ipfs
        .resolve(&name)
        .await
        .map_err(StringError::from)?
        .to_string();

    let response = ResolveResponse { path };

    Ok(warp::reply::json(&response))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
struct ResolveResponse {
    path: String,
}

#[derive(Debug, Deserialize)]
pub struct DnsQuery {
    // the name to resolve
    arg: StringSerialized<IpfsPath>,
}

pub fn dns<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    with_ipfs(ipfs).and(query::<DnsQuery>()).and_then(dns_query)
}

async fn dns_query<T: IpfsTypes>(ipfs: Ipfs<T>, query: DnsQuery) -> Result<impl Reply, Rejection> {
    let DnsQuery { arg, .. } = query;
    let path = ipfs
        .resolve(&arg.into_inner())
        .await
        .map_err(StringError::from)?
        .to_string();

    let response = DnsResponse { path };

    Ok(warp::reply::json(&response))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
struct DnsResponse {
    path: String,
}
