use crate::v0::support::{with_ipfs, StringError};
use futures::stream;
use futures::stream::Stream;
use ipfs::{Block, Error};
use ipfs::{Ipfs, IpfsTypes};
use libipld::cid::{self, Cid};
use libipld::{block::decode_ipld, Ipld};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::VecDeque;
use std::convert::TryFrom;
use warp::hyper::Body;
use warp::{Filter, Rejection, Reply};

mod options;
use options::RefsOptions;

mod format;
use format::EdgeFormatter;

mod path;
use path::{IpfsPath, WalkSuccess};

mod unshared;
use unshared::Unshared;

mod support;
use support::{HandledErr, StreamResponse};

/// https://docs-beta.ipfs.io/reference/http/api/#api-v0-refs
pub fn refs<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::path!("refs")
        .and(with_ipfs(ipfs))
        .and(refs_options())
        .and_then(refs_inner)
}

async fn refs_inner<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    opts: RefsOptions,
) -> Result<impl Reply, Rejection> {
    use futures::stream::StreamExt;

    let max_depth = opts.max_depth();
    let formatter = EdgeFormatter::from_options(opts.edges, opts.format.as_deref())
        .map_err(StringError::from)?;

    log::trace!(
        "refs on {:?} to depth {:?} with {:?}",
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
        .await
        .map_err(|e| {
            log::warn!("refs path on {:?} failed with {}", &opts.arg, e);
            e
        })
        .map_err(StringError::from)?;

    let st = st.map(move |res| {
        let res = match res {
            Ok((source, dest, link_name)) => {
                let ok = formatter.format(source, dest, link_name);
                serde_json::to_string(&Edge {
                    ok: ok.into(),
                    err: "".into(),
                })
            }
            Err(e) => serde_json::to_string(&Edge {
                ok: "".into(),
                err: e.into(),
            }),
        };

        match res {
            Ok(mut s) => {
                s.push('\n');
                Ok(s.into_bytes())
            }
            Err(e) => {
                log::error!("edge serialization failed: {}", e);
                Err(HandledErr)
            }
        }
    });

    // Note: Unshared has the unsafe impl Sync which sadly is needed.
    // See documentation for `Unshared` for more information.
    Ok(StreamResponse(Unshared::new(st)))
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

        futures::future::ready(res)
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
) -> Result<impl Stream<Item = Result<(Cid, Cid, Option<String>), String>> + Send + 'static, Error>
{
    use futures::stream::FuturesOrdered;
    use futures::stream::TryStreamExt;

    // the assumption is that futuresordered will poll the first N items until the first completes,
    // buffering the others. it might not be 100% parallel but it's probably enough.
    let mut walks = FuturesOrdered::new();

    for path in paths {
        walks.push(walk_path(&ipfs, path));
    }

    let iplds = walks.try_collect().await?;

    Ok(iplds_refs(ipfs, iplds, max_depth, unique))
}

/// Walks the `path` while loading the links.
///
/// # Panics
///
/// If there are dag-pb nodes and the libipld has changed it's dag-pb tree structure.
async fn walk_path<T: IpfsTypes>(ipfs: &Ipfs<T>, mut path: IpfsPath) -> Result<(Cid, Ipld), Error> {
    let mut current = path.take_root().unwrap();

    loop {
        let Block { data, .. } = ipfs.get_block(&current).await?;
        let ipld = decode_ipld(&current, &data)?;

        match path.walk(&current, ipld)? {
            WalkSuccess::EmptyPath(ipld) | WalkSuccess::AtDestination(ipld) => {
                return Ok((current, ipld))
            }
            WalkSuccess::Link(_key, next_cid) => current = next_cid,
        };
    }
}

/// Gather links as edges between two documents from all of the `iplds` which represent the
/// document and it's original `Cid`, as the `Ipld` can be a subtree of the document.
///
/// # Differences from other implementations
///
/// `js-ipfs` does seem to do a recursive descent on all links. Looking at the tests it would
/// appear that `go-ipfs` implements this in similar fashion. This implementation is breadth-first
/// to be simpler at least.
///
/// Related: https://github.com/ipfs/js-ipfs/pull/2982
///
/// # Panics
///
/// If there are dag-pb nodes and the libipld has changed it's dag-pb tree structure.
fn iplds_refs<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    iplds: Vec<(Cid, Ipld)>,
    max_depth: Option<u64>,
    unique: bool,
) -> impl Stream<Item = Result<(Cid, Cid, Option<String>), String>> + Send + 'static {
    use async_stream::stream;
    use std::collections::HashSet;

    stream! {
        if let Some(0) = max_depth {
            return;
        }

        let mut visited = HashSet::new();
        let mut work = VecDeque::new();

        for (origin, ipld) in iplds {
            for (link_name, next_cid) in ipld_links(&origin, ipld) {
                work.push_back((0, next_cid, origin.clone(), link_name));
            }
        }

        while let Some((depth, cid, source, link_name)) = work.pop_front() {
            let traverse_links = match max_depth {
                Some(d) if d <= depth => {
                    // important to continue instead of stopping
                    continue;
                },
                // no need to list links which would be filtered out
                Some(d) if d + 1 == depth => false,
                _ => true
            };

            if unique && !visited.insert(cid.clone()) {
                log::trace!("skipping already visited {}", cid);
                continue;
            }

            let data = match ipfs.get_block(&cid).await {
                Ok(Block { data, .. }) => data,
                Err(e) => {
                    log::warn!("failed to load {}, linked from {}: {}", cid, source, e);
                    // TODO: yield error msg
                    // unsure in which cases this happens, because we'll start to search the content
                    // and stop only when request has been cancelled (FIXME: not yet, because dropping
                    // all subscriptions doesn't "stop the operation.")
                    continue;
                }
            };

            let mut ipld = match decode_ipld(&cid, &data) {
                Ok(ipld) => ipld,
                Err(e) => {
                    log::warn!("failed to parse {}, linked from {}: {}", cid, source, e);
                    // TODO: yield error msg
                    // go-ipfs on raw Qm hash:
                    // > failed to decode Protocol Buffers: incorrectly formatted merkledag node: unmarshal failed. proto: illegal wireType 6
                    yield Err(e.to_string());
                    continue;
                }
            };

            if traverse_links {
                for (link_name, next_cid) in ipld_links(&cid, ipld) {
                    if unique && visited.contains(&next_cid) {
                        // skip adding already yielded documents
                        continue;
                    }
                    // TODO: could also have a hashset for queued destinations ...
                    work.push_back((depth + 1, next_cid, cid.clone(), link_name));
                }
            }

            yield Ok((source, cid, link_name));
        }
    }
}

fn ipld_links(
    cid: &Cid,
    ipld: Ipld,
) -> impl Iterator<Item = (Option<String>, Cid)> + Send + 'static {
    // a wrapping iterator without there being a libipld_base::IpldIntoIter might not be doable
    // with safe code
    let items = if cid.codec() == cid::Codec::DagProtobuf {
        dagpb_links(ipld)
    } else {
        ipld.iter()
            .filter_map(|val| match val {
                Ipld::Link(cid) => Some(cid),
                _ => None,
            })
            .cloned()
            // only dag-pb ever has any link names, probably because in cbor the "name" on the LHS
            // might have a different meaning from a "link name" in dag-pb ... Doesn't seem
            // immediatedly obvious why this is done.
            .map(|cid| (None, cid))
            .collect::<Vec<(Option<String>, Cid)>>()
    };

    items.into_iter()
}

/// Special handling for the structure created while loading dag-pb as ipld.
///
/// # Panics
///
/// If the dag-pb ipld tree doesn't conform to expectations, as in, we are out of sync with the
/// libipld crate. This is on purpose.
fn dagpb_links(ipld: Ipld) -> Vec<(Option<String>, Cid)> {
    let links = match ipld {
        Ipld::Map(mut m) => m.remove("Links"),
        // lets assume this means "no links"
        _ => return Vec::new(),
    };

    let links = match links {
        Some(Ipld::List(v)) => v,
        x => panic!("Expected dag-pb2ipld \"Links\" to be a list, got: {:?}", x),
    };

    links
        .into_iter()
        .enumerate()
        .filter_map(|(i, ipld)| {
            match ipld {
                Ipld::Map(mut m) => {
                    let link = match m.remove("Hash") {
                        Some(Ipld::Link(cid)) => cid,
                        Some(x) => panic!(
                            "Expected dag-pb2ipld \"Links[{}]/Hash\" to be a link, got: {:?}",
                            i, x
                        ),
                        None => return None,
                    };
                    let name = match m.remove("Name") {
                        // not sure of this, not covered by tests, though these are only
                        // present for multi-block files so maybe it's better to panic
                        Some(Ipld::String(s)) if s == "/" => {
                            unimplemented!("Slashes as the name of link")
                        }
                        Some(Ipld::String(s)) => Some(s),
                        Some(x) => panic!(
                            "Expected dag-pb2ipld \"Links[{}]/Name\" to be a string, got: {:?}",
                            i, x
                        ),
                        // not too sure of this, this could be the index as string as well?
                        None => unimplemented!(
                            "Default name for dag-pb2ipld links, should it be index?"
                        ),
                    };

                    Some((name, link))
                }
                x => panic!(
                    "Expected dag-pb2ipld \"Links[{}]\" to be a map, got: {:?}",
                    i, x
                ),
            }
        })
        .collect()
}

/// Handling of https://docs-beta.ipfs.io/reference/http/api/#api-v0-refs-local
pub fn local<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::path!("refs" / "local")
        .and(with_ipfs(ipfs))
        .and_then(inner_local)
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
    use futures::stream::TryStreamExt;
    use ipfs::{Block, Ipfs};
    use libipld::block::{decode_ipld, validate};
    use libipld::cid::Cid;
    use std::collections::HashSet;
    use std::convert::TryFrom;

    #[tokio::test]
    async fn test_inner_local() {
        let filter = local(&preloaded_testing_ipfs().await);

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
        .map(|&s| String::from(s))
        .collect::<HashSet<_>>();

        let diff = destinations
            .symmetric_difference(&expected)
            .collect::<Vec<_>>();

        assert!(diff.is_empty(), "{:?}", diff);
    }

    #[tokio::test]
    async fn all_refs_from_root() {
        let ipfs = preloaded_testing_ipfs().await;

        let (root, dag0, unixfs0, dag1, unixfs1) = (
            // this is the dag with content: [dag0, unixfs0, dag1, unixfs1]
            "bafyreihpc3vupfos5yqnlakgpjxtyx3smkg26ft7e2jnqf3qkyhromhb64",
            // {foo: dag1, bar: unixfs0}
            "bafyreidquig3arts3bmee53rutt463hdyu6ff4zeas2etf2h2oh4dfms44",
            "QmPJ4A6Su27ABvvduX78x2qdWMzkdAYxqeH5TVrHeo3xyy",
            // {foo: unixfs1}
            "bafyreibvjvcv745gig4mvqs4hctx4zfkono4rjejm2ta6gtyzkqxfjeily",
            "QmRgutAxd8t7oGkSm4wmeuByG6M51wcTso6cubDdQtuEfL",
        );

        let all_edges: Vec<_> =
            refs_paths(ipfs, vec![IpfsPath::try_from(root).unwrap()], None, false)
                .await
                .unwrap()
                .map_ok(|(source, dest, _)| (source.to_string(), dest.to_string()))
                .try_collect()
                .await
                .unwrap();

        // not sure why go-ipfs outputs this order, this is more like dfs?
        let expected = [
            (root, dag0),
            (dag0, unixfs0),
            (dag0, dag1),
            (dag1, unixfs1),
            (root, unixfs0),
            (root, dag1),
            (dag1, unixfs1),
            (root, unixfs1),
        ];

        println!("found edges:\n{:#?}", all_edges);

        assert_edges(&expected, all_edges.as_slice());
    }

    #[tokio::test]
    async fn all_unique_refs_from_root() {
        let ipfs = preloaded_testing_ipfs().await;

        let (root, dag0, unixfs0, dag1, unixfs1) = (
            // this is the dag with content: [dag0, unixfs0, dag1, unixfs1]
            "bafyreihpc3vupfos5yqnlakgpjxtyx3smkg26ft7e2jnqf3qkyhromhb64",
            // {foo: dag1, bar: unixfs0}
            "bafyreidquig3arts3bmee53rutt463hdyu6ff4zeas2etf2h2oh4dfms44",
            "QmPJ4A6Su27ABvvduX78x2qdWMzkdAYxqeH5TVrHeo3xyy",
            // {foo: unixfs1}
            "bafyreibvjvcv745gig4mvqs4hctx4zfkono4rjejm2ta6gtyzkqxfjeily",
            "QmRgutAxd8t7oGkSm4wmeuByG6M51wcTso6cubDdQtuEfL",
        );

        let destinations: HashSet<_> =
            refs_paths(ipfs, vec![IpfsPath::try_from(root).unwrap()], None, true)
                .await
                .unwrap()
                .map_ok(|(_, dest, _)| dest.to_string())
                .try_collect()
                .await
                .unwrap();

        // go-ipfs output:
        // bafyreihpc3vupfos5yqnlakgpjxtyx3smkg26ft7e2jnqf3qkyhromhb64 -> bafyreidquig3arts3bmee53rutt463hdyu6ff4zeas2etf2h2oh4dfms44
        // bafyreihpc3vupfos5yqnlakgpjxtyx3smkg26ft7e2jnqf3qkyhromhb64 -> QmPJ4A6Su27ABvvduX78x2qdWMzkdAYxqeH5TVrHeo3xyy
        // bafyreihpc3vupfos5yqnlakgpjxtyx3smkg26ft7e2jnqf3qkyhromhb64 -> bafyreibvjvcv745gig4mvqs4hctx4zfkono4rjejm2ta6gtyzkqxfjeily
        // bafyreihpc3vupfos5yqnlakgpjxtyx3smkg26ft7e2jnqf3qkyhromhb64 -> QmRgutAxd8t7oGkSm4wmeuByG6M51wcTso6cubDdQtuEfL

        let expected = [dag0, unixfs0, dag1, unixfs1]
            .iter()
            .map(|&s| String::from(s))
            .collect::<HashSet<_>>();

        let diff = destinations
            .symmetric_difference(&expected)
            .map(|s| s.as_str())
            .collect::<Vec<&str>>();

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
                .map_ok(|(source, dest, _)| (source.to_string(), dest.to_string()))
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

    async fn preloaded_testing_ipfs() -> Ipfs<ipfs::TestTypes> {
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
            (
                // echo -e '[{"/":"bafyreidquig3arts3bmee53rutt463hdyu6ff4zeas2etf2h2oh4dfms44"},{"/":"QmPJ4A6Su27ABvvduX78x2qdWMzkdAYxqeH5TVrHeo3xyy"},{"/":"bafyreibvjvcv745gig4mvqs4hctx4zfkono4rjejm2ta6gtyzkqxfjeily"},{"/":"QmRgutAxd8t7oGkSm4wmeuByG6M51wcTso6cubDdQtuEfL"}]' | ./ipfs dag put
                "bafyreihpc3vupfos5yqnlakgpjxtyx3smkg26ft7e2jnqf3qkyhromhb64",
                "84d82a5825000171122070a20db04672d858427771a4e7cf6ce3c53c52f32404b4499747d38fc19592e7d82a58230012200e317512b6f9f86e015a154cb97a9ddcdc7e372cccceb3947921634953c65374d82a58250001711220354d455ff3a641b8cac25c38a77e64aa735dc8a48966a60f1a78caa172a4885ed82a582300122031c3d57080d8463a3c63b2923df5a1d40ad7a73eae5a14af584213e5f504ac33"
            )
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

        ipfs
    }
}
