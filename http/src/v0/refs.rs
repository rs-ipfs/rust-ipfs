use futures::stream;
use ipfs::error::Error;
use ipfs::{Ipfs, IpfsTypes};
use serde::Serialize;
use warp::hyper::Body;
use warp::{path, query, Filter, Rejection, Reply};
use crate::v0::support::{with_ipfs, StringError};
use serde::Deserialize;

#[derive(Serialize, Debug)]
struct RefsResponseItem {
    #[serde(rename = "Err")]
    err: String,

    #[serde(rename = "Ref")]
    refs: String,
}

/// https://docs-beta.ipfs.io/reference/http/api/#api-v0-refs
pub fn refs<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    path!("refs")
        .and(with_ipfs(ipfs))
        .and(query::<RefsOptions>())
        .and_then(refs_inner)
}

async fn refs_inner<T: IpfsTypes>(
    _ipfs: Ipfs<T>,
    _opts: RefsOptions
) -> Result<impl Reply, Rejection> {
    Ok("foo")
}

#[derive(Debug, Deserialize)]
struct RefsOptions {
    /// Ipfs path like `/ipfs/cid[/link]`
    arg: String,
    format: Option<String>,
    #[serde(default)]
    edges: bool,
    #[serde(default)]
    unique: bool,
    #[serde(default)]
    recursive: bool,
    // `int` in the docs apparently is platform specific
    // This should accepted only when recursive.
    #[serde(rename = "max-depth")]
    max_depth: Option<isize>,
}

/// Handling of https://docs-beta.ipfs.io/reference/http/api/#api-v0-refs-local
pub fn local<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    path!("refs" / "local")
        .and(with_ipfs(ipfs))
        .and_then(inner_local)
}

async fn inner_local<T: IpfsTypes>(ipfs: Ipfs<T>) -> Result<impl Reply, Rejection> {
    let refs: Vec<Result<String, Error>> = ipfs
        .refs_local()
        .await
        .map_err(StringError::from)?
        .into_iter()
        .map(|cid| cid.to_string())
        .map(|refs| RefsResponseItem {
            refs,
            err: "".to_string(),
        })
        .map(|response| {
            serde_json::to_string(&response)
                .map_err(|e| {
                    eprintln!("error from serde_json: {}", e);
                    HandledErr
                })
                .unwrap()
        })
        .map(|ref_json| Ok(format!("{}{}", ref_json, "\n")))
        .collect();

    let stream = stream::iter(refs);
    Ok(warp::reply::Response::new(Body::wrap_stream(stream)))
}

#[derive(Debug)]
struct HandledErr;

impl std::error::Error for HandledErr {}

use std::fmt;

impl fmt::Display for HandledErr {
    fn fmt(&self, _fmt: &mut fmt::Formatter) -> fmt::Result {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::inner_local;
    use ipfs::Block;
    use libipld::cid::Cid;
    use libipld::cid::Codec;
    use multihash::Sha2_256;

    #[tokio::test]
    async fn test_inner_local() {
        use ipfs::{IpfsOptions, UninitializedIpfs};

        let options = IpfsOptions::inmemory_with_generated_keys(false);

        let (ipfs, fut) = UninitializedIpfs::new(options).await.start().await.unwrap();
        drop(fut);

        for data in &[b"1", b"2", b"3"] {
            let data_slice = data.to_vec().into_boxed_slice();
            let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&data_slice));
            let block = Block::new(data_slice, cid);
            ipfs.put_block(block.clone()).await.unwrap();
        }

        let _result = inner_local(ipfs).await;
        // println!("{:?}", result.unwrap());
    }
}
