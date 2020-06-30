use crate::v0::support::{
    with_ipfs, HandledErr, InvalidMultipartFormData, StreamResponse, StringError,
};
use futures::stream::FuturesOrdered;
use futures::stream::StreamExt;
use ipfs::error::Error;
use ipfs::{Ipfs, IpfsTypes};
use libipld::cid::{Cid, Codec, Version};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use warp::{http::Response, multipart, path, query, reply, Buf, Filter, Rejection, Reply};

mod options;
use options::RmOptions;

#[derive(Debug, Deserialize)]
pub struct GetQuery {
    arg: String,
}

async fn get_query<T: IpfsTypes>(ipfs: Ipfs<T>, query: GetQuery) -> Result<impl Reply, Rejection> {
    let cid: Cid = query.arg.parse().map_err(StringError::from)?;
    let data = ipfs
        .get_block(&cid)
        .await
        .map_err(StringError::from)?
        .into_vec();

    let response = Response::builder().body(data);
    Ok(response)
}

pub fn get<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    path!("block" / "get")
        .and(with_ipfs(ipfs))
        .and(query::<GetQuery>())
        .and_then(get_query)
}

#[derive(Debug, Deserialize)]
pub struct PutQuery {
    format: Option<String>,
    mhtype: Option<String>,
    version: Option<u8>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct PutResponse {
    key: String,
    size: usize,
}

async fn put_query<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    query: PutQuery,
    mut form: multipart::FormData,
) -> Result<impl Reply, Rejection> {
    let format = match query.format.as_deref().unwrap_or("dag-pb") {
        "dag-cbor" => Codec::DagCBOR,
        "dag-pb" => Codec::DagProtobuf,
        "dag-json" => Codec::DagJSON,
        "raw" => Codec::Raw,
        _ => return Err(StringError::from("unknown codec").into()),
    };
    let hasher = match query.mhtype.as_deref().unwrap_or("sha2-256") {
        "sha2-256" => multihash::Sha2_256::digest,
        "sha2-512" => multihash::Sha2_512::digest,
        _ => return Err(StringError::from("unknown hash").into()),
    };
    let version = match query.version.unwrap_or(0) {
        0 => Version::V0,
        1 => Version::V1,
        _ => return Err(StringError::from("invalid cid version").into()),
    };
    let buf = form
        .next()
        .await
        .ok_or(InvalidMultipartFormData)?
        .map_err(|_| InvalidMultipartFormData)?
        .data()
        .await
        .ok_or(InvalidMultipartFormData)?
        .map_err(|_| InvalidMultipartFormData)?;
    let data = Box::from(buf.bytes());
    let digest = hasher(&data);
    let cid = Cid::new(version, format, digest).map_err(StringError::from)?;
    let response = PutResponse {
        key: cid.to_string(),
        size: data.len(),
    };
    let block = ipfs::Block { cid, data };
    ipfs.put_block(block).await.map_err(StringError::from)?;
    Ok(reply::json(&response))
}

pub fn put<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    path!("block" / "put")
        .and(with_ipfs(ipfs))
        .and(query::<PutQuery>())
        .and(multipart::form())
        .and_then(put_query)
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct RmResponse {
    error: String,
    hash: String,
}

#[derive(Serialize, Deserialize)]
pub struct EmptyResponse {}

pub fn rm<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    path!("block" / "rm")
        .and(with_ipfs(ipfs))
        .and(rm_options())
        .and_then(rm_query)
}

fn rm_options() -> impl Filter<Extract = (RmOptions,), Error = Rejection> + Clone {
    warp::filters::query::raw().and_then(|q: String| {
        let res = RmOptions::try_from(q.as_str())
            .map_err(StringError::from)
            .map_err(warp::reject::custom);

        futures::future::ready(res)
    })
}

async fn rm_query<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    options: RmOptions,
) -> Result<impl Reply, Rejection> {
    use futures::future::TryFutureExt;

    let RmOptions { args, force, quiet } = options;

    let cids = args
        .into_iter()
        .map(|s| Cid::try_from(s.as_str()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(StringError::from)?;

    let futs: FuturesOrdered<_> = cids
        .into_iter()
        .map(|cid| ipfs.remove_block(cid.clone()).map_err(move |e| (cid, e)))
        .collect();

    let responses = futs
        .collect::<Vec<Result<Cid, (Cid, Error)>>>()
        .await
        .into_iter()
        .map(move |result| match result {
            Ok(cid) => RmResponse {
                hash: cid.to_string(),
                error: "".to_string(),
            },
            Err((cid, e)) => RmResponse {
                hash: cid.to_string(),
                error: if force { "".to_string() } else { e.to_string() },
            },
        })
        .map(|response: RmResponse| serde_json::to_string(&response))
        .map(move |result| match result {
            Ok(mut string) => {
                if quiet {
                    string = "".to_string();
                } else {
                    string.push('\n');
                }
                Ok(string.into_bytes())
            }
            Err(e) => {
                log::error!("edge serialization failed: {}", e);
                Err(HandledErr)
            }
        });

    let st = futures::stream::iter(responses);
    Ok(StreamResponse(st))
}

#[derive(Debug, Deserialize)]
pub struct StatQuery {
    arg: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct StatResponse {
    key: String,
    size: usize,
}

async fn stat_query<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    query: StatQuery,
) -> Result<impl Reply, Rejection> {
    let cid: Cid = query.arg.parse().map_err(StringError::from)?;
    let block = ipfs.get_block(&cid).await.map_err(StringError::from)?;
    let response = StatResponse {
        key: query.arg,
        size: block.data().len(),
    };
    Ok(reply::json(&response))
}

pub fn stat<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    path!("block" / "stat")
        .and(with_ipfs(ipfs))
        .and(query::<StatQuery>())
        .and_then(stat_query)
}
