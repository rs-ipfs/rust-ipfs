use crate::v0::support::{with_ipfs, MaybeTimeoutExt, StringError, StringSerialized};
use ipfs::{Ipfs, IpfsTypes, MultiaddrWithPeerId};
use serde::{Deserialize, Serialize};
use warp::{query, Filter, Rejection, Reply};

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
struct Response<S: AsRef<str>> {
    peers: Vec<S>,
}

#[derive(Debug, Deserialize)]
pub struct BootstrapQuery {
    timeout: Option<StringSerialized<humantime::Duration>>,
}

async fn bootstrap_query<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    query: BootstrapQuery,
) -> Result<impl Reply, Rejection> {
    let peers = ipfs
        .get_bootstrappers()
        .maybe_timeout(query.timeout.map(StringSerialized::into_inner))
        .await
        .map_err(StringError::from)?
        .map_err(StringError::from)?
        .into_iter()
        .map(|addr| addr.to_string())
        .collect();

    let response = Response { peers };

    Ok(warp::reply::json(&response))
}

pub fn bootstrap_list<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    with_ipfs(ipfs)
        .and(query::<BootstrapQuery>())
        .and_then(bootstrap_query)
}

#[derive(Debug, Deserialize)]
pub struct BootstrapAddQuery {
    arg: Option<StringSerialized<MultiaddrWithPeerId>>,
    default: Option<bool>,
    timeout: Option<StringSerialized<humantime::Duration>>,
}

// optionally timed-out wrapper around [`Ipfs::restore_bootstrappers`] with stringified errors, used
// in both bootstrap_add_query and bootstrap_restore_query
async fn restore_helper<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    timeout: &Option<StringSerialized<humantime::Duration>>,
) -> Result<Vec<String>, Rejection> {
    Ok(ipfs
        .restore_bootstrappers()
        .maybe_timeout(timeout.map(StringSerialized::into_inner))
        .await
        .map_err(StringError::from)?
        .map_err(StringError::from)?
        .into_iter()
        .map(|addr| addr.to_string())
        .collect())
}

async fn bootstrap_add_query<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    query: BootstrapAddQuery,
) -> Result<impl Reply, Rejection> {
    let BootstrapAddQuery {
        arg,
        default,
        timeout,
    } = query;
    let peers = if let Some(arg) = arg {
        vec![ipfs
            .add_bootstrapper(arg.into_inner())
            .maybe_timeout(timeout.map(StringSerialized::into_inner))
            .await
            .map_err(StringError::from)?
            .map_err(StringError::from)?
            .to_string()]
    } else if default == Some(true) {
        // HTTP api documents `?default=true` as deprecated
        let _ = restore_helper(ipfs, &timeout).await?;

        // return a list of all known bootstrap nodes as js-ipfs does
        ipfs::config::BOOTSTRAP_NODES
            .iter()
            .map(|&s| String::from(s))
            .collect()
    } else {
        return Err(warp::reject::custom(StringError::from(
            "invalid query string",
        )));
    };

    let response = Response { peers };

    Ok(warp::reply::json(&response))
}

/// https://docs.ipfs.io/reference/http/api/#api-v0-bootstrap-add
pub fn bootstrap_add<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    with_ipfs(ipfs)
        .and(query::<BootstrapAddQuery>())
        .and_then(bootstrap_add_query)
}

#[derive(Debug, Deserialize)]
pub struct BootstrapClearQuery {
    timeout: Option<StringSerialized<humantime::Duration>>,
}

// optionally timed-out wrapper over [`Ipfs::clear_bootstrappers`] used in both
// `bootstrap_clear_query` and `bootstrap_rm_query`.
async fn clear_helper<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    timeout: &Option<StringSerialized<humantime::Duration>>,
) -> Result<Vec<String>, Rejection> {
    Ok(ipfs
        .clear_bootstrappers()
        .maybe_timeout(timeout.map(StringSerialized::into_inner))
        .await
        .map_err(StringError::from)?
        .map_err(StringError::from)?
        .into_iter()
        .map(|addr| addr.to_string())
        .collect())
}

async fn bootstrap_clear_query<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    query: BootstrapClearQuery,
) -> Result<impl Reply, Rejection> {
    let peers = clear_helper(ipfs, &query.timeout).await?;
    let response = Response { peers };

    Ok(warp::reply::json(&response))
}

pub fn bootstrap_clear<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    with_ipfs(ipfs)
        .and(query::<BootstrapClearQuery>())
        .and_then(bootstrap_clear_query)
}

#[derive(Debug, Deserialize)]
pub struct BootstrapRmQuery {
    arg: Option<StringSerialized<MultiaddrWithPeerId>>,
    all: Option<bool>,
    timeout: Option<StringSerialized<humantime::Duration>>,
}

async fn bootstrap_rm_query<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    query: BootstrapRmQuery,
) -> Result<impl Reply, Rejection> {
    let BootstrapRmQuery { arg, all, timeout } = query;

    let peers = if let Some(arg) = arg {
        vec![ipfs
            .remove_bootstrapper(arg.into_inner())
            .maybe_timeout(timeout.map(StringSerialized::into_inner))
            .await
            .map_err(StringError::from)?
            .map_err(StringError::from)?
            .to_string()]
    } else if all == Some(true) {
        clear_helper(ipfs, &timeout).await?
    } else {
        return Err(warp::reject::custom(StringError::from(
            "invalid query string",
        )));
    };

    let response = Response { peers };

    Ok(warp::reply::json(&response))
}

pub fn bootstrap_rm<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    with_ipfs(ipfs)
        .and(query::<BootstrapRmQuery>())
        .and_then(bootstrap_rm_query)
}

#[derive(Debug, Deserialize)]
pub struct BootstrapRestoreQuery {
    timeout: Option<StringSerialized<humantime::Duration>>,
}

async fn bootstrap_restore_query<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    query: BootstrapRestoreQuery,
) -> Result<impl Reply, Rejection> {
    let _ = restore_helper(ipfs, &query.timeout).await?;

    // similar to add?default=true; returns a list of all bootstrap nodes, not only the added ones
    let peers = ipfs::config::BOOTSTRAP_NODES.to_vec();
    let response = Response { peers };

    Ok(warp::reply::json(&response))
}

/// https://docs.ipfs.io/reference/http/api/#api-v0-bootstrap-add-default, similar functionality
/// also available via /bootstrap/add?default=true through [`bootstrap_add`].
pub fn bootstrap_restore<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    with_ipfs(ipfs)
        .and(query::<BootstrapRestoreQuery>())
        .and_then(bootstrap_restore_query)
}
