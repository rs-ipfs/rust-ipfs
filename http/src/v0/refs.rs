use crate::v0::support::{with_ipfs, MaybeTimeoutExt, StringError};
use cid::{self, Cid};
use futures::future::ready;
use futures::stream::{self, FuturesOrdered, Stream, StreamExt, TryStreamExt};
use ipfs::ipld::{decode_ipld, Ipld};
use ipfs::{Ipfs, IpfsTypes};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::convert::TryFrom;
use warp::hyper::Body;
use warp::{Filter, Rejection, Reply};

mod options;
use options::RefsOptions;

mod format;
use format::EdgeFormatter;

use ipfs::dag::ResolveError;
pub use ipfs::path::IpfsPath;

use crate::v0::support::{HandledErr, StreamResponse};

/// https://docs-beta.ipfs.io/reference/http/api/#api-v0-refs
pub fn refs<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    with_ipfs(ipfs).and(refs_options()).and_then(refs_inner)
}

async fn refs_inner<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    opts: RefsOptions,
) -> Result<impl Reply, Rejection> {
    let max_depth = opts.max_depth();
    let formatter = EdgeFormatter::from_options(opts.edges, opts.format.as_deref())
        .map_err(StringError::from)?;

    trace!(
        "refs on {:?} to depth {:?} with formatter: {:?}",
        opts.arg,
        max_depth,
        formatter
    );

    let paths = opts
        .arg
        .iter()
        .map(|s| IpfsPath::try_from(s.as_str()).map_err(StringError::from))
        .collect::<Result<Vec<_>, _>>()?;

    let st = refs_paths(ipfs, paths, max_depth, opts.unique)
        .maybe_timeout(opts.timeout)
        .await
        .map_err(StringError::from)?
        .map_err(StringError::from)?;

    // FIXME: there should be a total timeout arching over path walking to the stream completion.
    // hyper can't do trailer errors on chunked bodies so ... we can't do much.

    // FIXME: the test case 'should print nothing for non-existent hashes' is problematic as it
    // expects the headers to be blocked before the timeout expires.

    let st = st.map(move |res| {
        // FIXME: strings are allocated for nothing, could just use a single BytesMut for the
        // rendering
        let res = match res {
            Ok(ipfs::refs::Edge {
                source,
                destination,
                name,
            }) => {
                let ok = formatter.format(source, destination, name);
                serde_json::to_string(&Edge {
                    ok: ok.into(),
                    err: "".into(),
                })
            }
            Err(e) => serde_json::to_string(&Edge {
                ok: "".into(),
                err: e.to_string().into(),
            }),
        };

        match res {
            Ok(mut s) => {
                s.push('\n');
                Ok(s.into_bytes())
            }
            Err(e) => {
                error!("edge serialization failed: {}", e);
                Err(HandledErr)
            }
        }
    });

    Ok(StreamResponse(st))
}

#[derive(Debug, Serialize, Deserialize)]
struct Edge {
    #[serde(rename = "Ref")]
    ok: Cow<'static, str>,
    #[serde(rename = "Err")]
    err: Cow<'static, str>,
}

/// Filter to perform custom `warp::query<RefsOptions>`
fn refs_options() -> impl Filter<Extract = (RefsOptions,), Error = Rejection> + Clone {
    warp::filters::query::raw().and_then(|q: String| {
        let res = RefsOptions::try_from(q.as_str())
            .map_err(StringError::from)
            .map_err(warp::reject::custom);

        ready(res)
    })
}

/// Refs similar to go-ipfs `refs` which will first walk the path and then continue streaming the
/// results after first walking the path. This resides currently over at `ipfs-http` instead of
/// `ipfs` as I can't see this as an usable API call due to the multiple `paths` iterated. This
/// does make for a good overall test, which why we wanted to include this in the grant 1 phase.
async fn refs_paths<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    paths: Vec<IpfsPath>,
    max_depth: Option<u64>,
    unique: bool,
) -> Result<
    impl Stream<Item = Result<ipfs::refs::Edge, ipfs::ipld::BlockError>> + Send + 'static,
    ResolveError,
> {
    use ipfs::dag::ResolvedNode;

    let dag = ipfs.dag();

    // added braces to spell it out for borrowck that dag does not outlive this fn
    let iplds: Vec<(Cid, Ipld)> = {
        // the assumption is that futuresordered will poll the first N items until the first completes,
        // buffering the others. it might not be 100% parallel but it's probably enough.
        let mut walks = FuturesOrdered::new();

        for path in paths {
            walks.push(dag.resolve(path, true));
        }

        walks
            // strip out the path inside last document, we don't need it
            .try_filter_map(|(resolved, _)| {
                ready(match resolved {
                    // filter out anything scoped to /Data on a dag-pb node; those cannot contain
                    // links as all links for a dag-pb are under /Links
                    ResolvedNode::DagPbData(_, _) => Ok(None),
                    ResolvedNode::Link(..) => unreachable!("followed links"),
                    // decode and hope for the best; this of course does a lot of wasted effort;
                    // hopefully one day we can do "projectioned decoding", like here we'd only
                    // need all of the links of the block
                    ResolvedNode::Block(b) => match decode_ipld(b.cid(), b.data()) {
                        Ok(ipld) => Ok(Some((b.cid, ipld))),
                        Err(e) => Err(ResolveError::UnsupportedDocument(b.cid, e.into())),
                    },
                    // the most straight-forward variant with pre-projected document
                    ResolvedNode::Projection(cid, ipld) => Ok(Some((cid, ipld))),
                })
            })
            // TODO: collecting here is actually a quite unnecessary, if only we could make this into a
            // stream.. however there may have been the case that all paths need to resolve before
            // http status code is determined so perhaps this is the only way.
            .try_collect()
            .await?
    };

    Ok(ipfs::refs::iplds_refs(ipfs, iplds, max_depth, unique))
}

/// Handling of https://docs-beta.ipfs.io/reference/http/api/#api-v0-refs-local
pub fn local<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    with_ipfs(ipfs).and_then(inner_local)
}

async fn inner_local<T: IpfsTypes>(ipfs: Ipfs<T>) -> Result<impl Reply, Rejection> {
    let refs = ipfs
        .refs_local()
        .await
        .map_err(StringError::from)?
        .into_iter()
        .map(|cid| cid.to_string())
        .map(|refs| Edge {
            ok: refs.into(),
            err: "".into(),
        })
        .map(|response| {
            serde_json::to_string(&response)
                .map(|mut s| {
                    s.push('\n');
                    s
                })
                .map_err(|e| {
                    eprintln!("error from serde_json: {}", e);
                    HandledErr
                })
        });

    let stream = stream::iter(refs);
    Ok(warp::reply::Response::new(Body::wrap_stream(stream)))
}

#[cfg(test)]
mod tests {
    use super::{local, refs_paths, Edge, IpfsPath};
    use cid::{self, Cid};
    use futures::stream::TryStreamExt;
    use ipfs::ipld::{decode_ipld, validate};
    use ipfs::{Block, Node};
    use std::collections::HashSet;
    use std::convert::TryFrom;

    #[tokio::test]
    async fn test_inner_local() {
        let filter = local(&*preloaded_testing_ipfs().await);

        let response = warp::test::request()
            .path("/refs/local")
            .reply(&filter)
            .await;

        assert_eq!(response.status(), 200);
        let body = response.body().as_ref();

        let destinations = body
            .split(|&byte| byte == b'\n')
            .filter(|bytes| !bytes.is_empty())
            .map(|bytes| match serde_json::from_slice::<Edge>(bytes) {
                Ok(Edge { ok, err }) if err.is_empty() => Ok(ok.into_owned()),
                Ok(Edge { err, .. }) => panic!("a block failed to list: {:?}", err),
                Err(x) => {
                    println!("failed to parse: {:02x?}", bytes);
                    Err(x)
                }
            })
            .collect::<Result<HashSet<_>, _>>()
            .unwrap();

        let expected = [
            "bafyreidquig3arts3bmee53rutt463hdyu6ff4zeas2etf2h2oh4dfms44",
            "QmPJ4A6Su27ABvvduX78x2qdWMzkdAYxqeH5TVrHeo3xyy",
            "bafyreibvjvcv745gig4mvqs4hctx4zfkono4rjejm2ta6gtyzkqxfjeily",
            "QmRgutAxd8t7oGkSm4wmeuByG6M51wcTso6cubDdQtuEfL",
            "bafyreihpc3vupfos5yqnlakgpjxtyx3smkg26ft7e2jnqf3qkyhromhb64",
        ]
        .iter()
        .map(|&s| {
            let cid = Cid::try_from(s).expect("they are good cids");
            cid.to_string()
        })
        .collect::<HashSet<_>>();

        let diff = destinations
            .symmetric_difference(&expected)
            .collect::<Vec<_>>();

        assert!(diff.is_empty(), "{:?}", diff);
    }

    #[tokio::test]
    async fn refs_with_path() {
        let ipfs = preloaded_testing_ipfs().await;

        let paths = [
            "/ipfs/bafyreidquig3arts3bmee53rutt463hdyu6ff4zeas2etf2h2oh4dfms44/foo",
            "bafyreidquig3arts3bmee53rutt463hdyu6ff4zeas2etf2h2oh4dfms44/foo",
            "bafyreihpc3vupfos5yqnlakgpjxtyx3smkg26ft7e2jnqf3qkyhromhb64/0/foo",
            "bafyreihpc3vupfos5yqnlakgpjxtyx3smkg26ft7e2jnqf3qkyhromhb64/0/foo/",
        ];

        for path in paths.iter() {
            let path = IpfsPath::try_from(*path).unwrap();
            let all_edges: Vec<_> = refs_paths(ipfs.clone(), vec![path], None, false)
                .await
                .unwrap()
                .map_ok(
                    |ipfs::refs::Edge {
                         source,
                         destination,
                         ..
                     }| (source.to_string(), destination.to_string()),
                )
                .try_collect()
                .await
                .unwrap();

            let expected = [(
                "bafyreibvjvcv745gig4mvqs4hctx4zfkono4rjejm2ta6gtyzkqxfjeily",
                "QmRgutAxd8t7oGkSm4wmeuByG6M51wcTso6cubDdQtuEfL",
            )];

            assert_edges(&expected, &all_edges);
        }
    }

    fn assert_edges(expected: &[(&str, &str)], actual: &[(String, String)]) {
        let expected: HashSet<_> = expected.iter().map(|&(a, b)| (a, b)).collect();

        let actual: HashSet<_> = actual
            .iter()
            .map(|(a, b)| (a.as_str(), b.as_str()))
            .collect();

        let diff: Vec<_> = expected.symmetric_difference(&actual).collect();

        assert!(diff.is_empty(), "{:#?}", diff);
    }

    async fn preloaded_testing_ipfs() -> Node {
        use hex_literal::hex;
        let ipfs = Node::new("test_node").await;

        let blocks = [
            (
                // echo -n '{ "foo": { "/": "bafyreibvjvcv745gig4mvqs4hctx4zfkono4rjejm2ta6gtyzkqxfjeily" }, "bar": { "/": "QmPJ4A6Su27ABvvduX78x2qdWMzkdAYxqeH5TVrHeo3xyy" } }' | /ipfs dag put
                "bafyreidquig3arts3bmee53rutt463hdyu6ff4zeas2etf2h2oh4dfms44",
                &hex!("a263626172d82a58230012200e317512b6f9f86e015a154cb97a9ddcdc7e372cccceb3947921634953c6537463666f6fd82a58250001711220354d455ff3a641b8cac25c38a77e64aa735dc8a48966a60f1a78caa172a4885e")[..]
            ),
            (
                // echo barfoo > file2 && ipfs add file2
                "QmPJ4A6Su27ABvvduX78x2qdWMzkdAYxqeH5TVrHeo3xyy",
                &hex!("0a0d08021207626172666f6f0a1807")[..]
            ),
            (
                // echo -n '{ "foo": { "/": "QmRgutAxd8t7oGkSm4wmeuByG6M51wcTso6cubDdQtuEfL" } }' | ipfs dag put
                "bafyreibvjvcv745gig4mvqs4hctx4zfkono4rjejm2ta6gtyzkqxfjeily",
                &hex!("a163666f6fd82a582300122031c3d57080d8463a3c63b2923df5a1d40ad7a73eae5a14af584213e5f504ac33")[..]
            ),
            (
                // echo foobar > file1 && ipfs add file1
                "QmRgutAxd8t7oGkSm4wmeuByG6M51wcTso6cubDdQtuEfL",
                &hex!("0a0d08021207666f6f6261720a1807")[..]
            ),
            (
                // echo -e '[{"/":"bafyreidquig3arts3bmee53rutt463hdyu6ff4zeas2etf2h2oh4dfms44"},{"/":"QmPJ4A6Su27ABvvduX78x2qdWMzkdAYxqeH5TVrHeo3xyy"},{"/":"bafyreibvjvcv745gig4mvqs4hctx4zfkono4rjejm2ta6gtyzkqxfjeily"},{"/":"QmRgutAxd8t7oGkSm4wmeuByG6M51wcTso6cubDdQtuEfL"}]' | ./ipfs dag put
                "bafyreihpc3vupfos5yqnlakgpjxtyx3smkg26ft7e2jnqf3qkyhromhb64",
                &hex!("84d82a5825000171122070a20db04672d858427771a4e7cf6ce3c53c52f32404b4499747d38fc19592e7d82a58230012200e317512b6f9f86e015a154cb97a9ddcdc7e372cccceb3947921634953c65374d82a58250001711220354d455ff3a641b8cac25c38a77e64aa735dc8a48966a60f1a78caa172a4885ed82a582300122031c3d57080d8463a3c63b2923df5a1d40ad7a73eae5a14af584213e5f504ac33")[..]
            )
        ];

        for (cid_str, data) in blocks.iter() {
            let cid = Cid::try_from(*cid_str).unwrap();
            validate(&cid, &data).unwrap();
            decode_ipld(&cid, &data).unwrap();

            let block = Block {
                cid,
                data: (*data).into(),
            };

            ipfs.put_block(block).await.unwrap();
        }

        ipfs
    }
}
