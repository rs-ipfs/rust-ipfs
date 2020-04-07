use futures::stream;
use ipfs::{Ipfs, IpfsTypes};
use serde::Serialize;
use warp::hyper::Body;
use futures::stream::Stream;
use ipfs::{Block, Error};
use ipfs::{Ipfs, IpfsTypes};
use libipld::cid::Cid;
use serde::Deserialize;
use std::collections::VecDeque;
use std::convert::TryFrom;
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
    _opts: RefsOptions,
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
    max_depth: Option<i64>,
}

// RefsStream which goes around in bfs
//
// - unsure if it should track duplicates, what if we get a loop
//
// Should return all in some order, probably in the order of discovery. Tests sort the values
// either way. Probably something like struct Edge { source: Cid, destination: Cid }.

fn edges<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    start: Cid,
    max_depth: Option<u64>,
) -> impl Stream<Item = Result<(Cid, Cid), Error>> {
    // FIXME: there should be an optional path
    use async_stream::try_stream;

    try_stream! {
        let mut work = VecDeque::new();
        work.push_back((0u64, start, None));

        while let Some((depth, cid, source)) = work.pop_front() {

            if let Some(max_depth) = max_depth.as_ref() {
                if *max_depth > depth {
                    return;
                }
            }

            let block = ipfs.get_block(&cid).await?;

            let links = block_links(block)?;

            for next_cid in links {
                println!("found link {} => {}", cid, next_cid);
                work.push_back((depth + 1, next_cid, Some(cid.clone())));
            }

            if let Some(source) = source {
                yield (source, cid);
            }
        }
    }
}

fn block_links(Block { cid, data }: Block) -> Result<impl Iterator<Item = Cid>, Error> {
    use libipld::Ipld;

    let ipld = libipld::block::decode_ipld(&cid, &data)?;
    // a wrapping iterator without there being a libipld_base::IpldIntoIter might not be doable
    // with safe code
    Ok(ipld
        .iter()
        .filter_map(|val| match val {
            Ipld::Link(cid) => Some(cid),
            _ => None,
        })
        .cloned()
        .collect::<Vec<_>>()
        .into_iter())
}

#[tokio::test]
async fn stack_refs() {
    use futures::stream::TryStreamExt;
    use libipld::block::{decode_ipld, validate};

    let options = ipfs::IpfsOptions::inmemory_with_generated_keys(false);
    let (ipfs, _) = ipfs::UninitializedIpfs::new(options)
        .await
        .start()
        .await
        .unwrap();

    let blocks = [
        (
            // echo -n '{ "foo": { "/": "bafyreibvjvcv745gig4mvqs4hctx4zfkono4rjejm2ta6gtyzkqxfjeily" }, "bar": { "/": "QmPJ4A6Su27ABvvduX78x2qdWMzkdAYxqeH5TVrHeo3xyy" } }' | /ipfs dag put
            "bafyreidquig3arts3bmee53rutt463hdyu6ff4zeas2etf2h2oh4dfms44",
            "a263626172d82a58230012200e317512b6f9f86e015a154cb97a9ddcdc7e372cccceb3947921634953c6537463666f6fd82a58250001711220354d455ff3a641b8cac25c38a77e64aa735dc8a48966a60f1a78caa172a4885e"
        ),
        (
            // echo barfoo > file2 && ipfs add file2
            "QmPJ4A6Su27ABvvduX78x2qdWMzkdAYxqeH5TVrHeo3xyy",
            "0a0d08021207626172666f6f0a1807"
        ),
        (
            // echo -n '{ "foo": { "/": "QmRgutAxd8t7oGkSm4wmeuByG6M51wcTso6cubDdQtuEfL" } }' | ipfs dag put
            "bafyreibvjvcv745gig4mvqs4hctx4zfkono4rjejm2ta6gtyzkqxfjeily",
            "a163666f6fd82a582300122031c3d57080d8463a3c63b2923df5a1d40ad7a73eae5a14af584213e5f504ac33"),
        (
            // echo foobar > file1 && ipfs add file1
            "QmRgutAxd8t7oGkSm4wmeuByG6M51wcTso6cubDdQtuEfL",
            "0a0d08021207666f6f6261720a1807"
        ),
    ];

    for (cid_str, hex_str) in blocks.iter() {
        let cid = Cid::try_from(*cid_str).unwrap();
        let data = hex::decode(hex_str).unwrap();

        validate(&cid, &data).unwrap();
        decode_ipld(&cid, &data).unwrap();

        let block = Block {
            cid,
            data: data.into(),
        };

        ipfs.put_block(block).await.unwrap();
    }

    let root = Cid::try_from(blocks[0].0).unwrap();

    let all_edges: Vec<_> = edges(ipfs, root, None)
        .map_ok(|(source, dest)| (source.to_string(), dest.to_string()))
        .try_collect()
        .await
        .unwrap();

    let expected = [
        (
            "bafyreidquig3arts3bmee53rutt463hdyu6ff4zeas2etf2h2oh4dfms44",
            "QmPJ4A6Su27ABvvduX78x2qdWMzkdAYxqeH5TVrHeo3xyy",
        ),
        (
            "bafyreidquig3arts3bmee53rutt463hdyu6ff4zeas2etf2h2oh4dfms44",
            "bafyreibvjvcv745gig4mvqs4hctx4zfkono4rjejm2ta6gtyzkqxfjeily",
        ),
        (
            "bafyreibvjvcv745gig4mvqs4hctx4zfkono4rjejm2ta6gtyzkqxfjeily",
            "QmRgutAxd8t7oGkSm4wmeuByG6M51wcTso6cubDdQtuEfL",
        ),
    ];

    let expected: Vec<_> = expected
        .iter()
        .map(|(source, dest)| (String::from(*source), String::from(*dest)))
        .collect();

    assert_eq!(expected, all_edges);
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
