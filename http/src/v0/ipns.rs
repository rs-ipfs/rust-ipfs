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
        .resolve_ipns(&name, false)
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
    arg: String,
    recursive: Option<bool>,
}

pub fn dns<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    with_ipfs(ipfs).and(query::<DnsQuery>()).and_then(dns_query)
}

async fn dns_query<T: IpfsTypes>(ipfs: Ipfs<T>, query: DnsQuery) -> Result<impl Reply, Rejection> {
    let DnsQuery { arg, recursive } = query;
    // attempt to parse the argument prepended with "/ipns/" if it fails to parse like a compliant
    // IpfsPath and there is no leading slash
    let path = if !arg.starts_with('/') {
        if let Ok(parsed) = arg.parse() {
            Ok(parsed)
        } else {
            format!("/ipns/{}", arg).parse()
        }
    } else {
        arg.parse()
    }
    .map_err(StringError::from)?;

    let path = ipfs
        .resolve_ipns(&path, recursive.unwrap_or(false))
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
