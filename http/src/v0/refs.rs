use crate::v0::support::{with_ipfs, MaybeTimeoutExt, StringError};
use cid::{self, Cid};
use futures::stream;
use futures::stream::Stream;
use ipfs::Ipfs;
use ipfs::{Block, Error};
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

pub(crate) mod path;
pub use path::{IpfsPath, WalkSuccess};

use crate::v0::support::{HandledErr, StreamResponse};

/// https://docs-beta.ipfs.io/reference/http/api/#api-v0-refs
pub fn refs(ipfs: &Ipfs) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::path!("refs")
        .and(with_ipfs(ipfs))
        .and(refs_options())
        .and_then(refs_inner)
}

async fn refs_inner(ipfs: Ipfs, opts: RefsOptions) -> Result<impl Reply, Rejection> {
    use futures::stream::StreamExt;

    let max_depth = opts.max_depth();
    let formatter = EdgeFormatter::from_options(opts.edges, opts.format.as_deref())
        .map_err(StringError::from)?;

    log::trace!(
        "refs on {:?} to depth {:?} with formatter: {:?}",
        opts.arg,
        max_depth,
        formatter
    );

    let mut paths = opts
        .arg
        .iter()
        .map(|s| IpfsPath::try_from(s.as_str()).map_err(StringError::from))
        .collect::<Result<Vec<_>, _>>()?;

    for path in paths.iter_mut() {
        // this is needed because the paths should not error on matching on the final Data segment,
        // it just becomes projected as `Loaded::Raw(_)`, however such items can have no links.
        path.set_follow_dagpb_data(true);
    }

    let st = refs_paths(ipfs, paths, max_depth, opts.unique)
        .maybe_timeout(opts.timeout)
        .await
        .map_err(StringError::from)?
        .map_err(|e| {
            log::warn!("refs path on {:?} failed with {}", &opts.arg, e);
            e
        })
        .map_err(StringError::from)?;

    // FIXME: there should be a total timeout arching over path walking to the stream completion.
    // hyper can't do trailer errors on chunked bodies so ... we can't do much.

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

        futures::future::ready(res)
    })
}

/// Refs similar to go-ipfs `refs` which will first walk the path and then continue streaming the
/// results after first walking the path. This resides currently over at `ipfs-http` instead of
/// `ipfs` as I can't see this as an usable API call due to the multiple `paths` iterated. This
/// does make for a good overall test, which why we wanted to include this in the grant 1 phase.
async fn refs_paths(
    ipfs: Ipfs,
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

    // strip out the path inside last document, we don't need it
    let iplds = walks
        .map_ok(|(cid, maybe_ipld, _)| (cid, maybe_ipld))
        .try_collect()
        .await?;

    Ok(iplds_refs(ipfs, iplds, max_depth, unique))
}

#[derive(Debug)]
pub struct WalkError {
    pub(crate) last_cid: Cid,
    pub(crate) reason: WalkFailed,
}

#[derive(Debug)]
pub enum WalkFailed {
    Loading(Error),
    Parsing(libipld::error::BlockError),
    DagPb(ipfs::unixfs::ll::ResolveError),
    IpldWalking(path::WalkFailed),
}

use std::fmt;

impl fmt::Display for WalkError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        use WalkFailed::*;
        match &self.reason {
            Loading(e) => write!(fmt, "loading of {} failed: {}", self.last_cid, e),
            Parsing(e) => write!(fmt, "failed to parse {} as IPLD: {}", self.last_cid, e),
            DagPb(e) => write!(
                fmt,
                "failed to resolve {} over dag-pb: {}",
                self.last_cid, e
            ),
            // this is asserted in the conformance tests and I don't really want to change the
            // tests for this
            IpldWalking(e) => write!(fmt, "{} under {}", e, self.last_cid),
        }
    }
}

impl std::error::Error for WalkError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        use WalkFailed::*;
        match &self.reason {
            Loading(_) => None, // TODO: anyhow
            Parsing(e) => Some(e),
            DagPb(e) => Some(e),
            IpldWalking(e) => Some(e),
        }
    }
}

impl From<Error> for WalkFailed {
    fn from(e: Error) -> Self {
        WalkFailed::Loading(e)
    }
}

impl From<libipld::error::BlockError> for WalkFailed {
    fn from(e: libipld::error::BlockError) -> Self {
        WalkFailed::Parsing(e)
    }
}

impl From<ipfs::unixfs::ll::ResolveError> for WalkFailed {
    fn from(e: ipfs::unixfs::ll::ResolveError) -> Self {
        WalkFailed::DagPb(e)
    }
}

impl From<path::WalkFailed> for WalkFailed {
    fn from(e: path::WalkFailed) -> Self {
        WalkFailed::IpldWalking(e)
    }
}

impl From<(WalkFailed, Cid)> for WalkError {
    fn from((reason, last_cid): (WalkFailed, Cid)) -> Self {
        WalkError { last_cid, reason }
    }
}

/// The IpfsPath walk can end in with the target block loaded or parsed and optionally projected as
/// an Ipld.
#[derive(Debug)]
pub enum Loaded {
    /// The raw block from `ipfs.get_block`
    Raw(Box<[u8]>),
    /// Possibly projected IPLD value.
    Ipld(Ipld),
}

/// Walks the `path` while loading the links.
///
/// Returns the Cid where we ended up, and an optional Ipld structure if one was projected, and the
/// path inside the last document we walked.
pub async fn walk_path(
    ipfs: &Ipfs,
    mut path: IpfsPath,
) -> Result<(Cid, Loaded, Vec<String>), WalkError> {
    use ipfs::unixfs::ll::{MaybeResolved, ResolveError};

    let mut current = path.take_root().unwrap();

    // cache for any datastructure used in repeated hamt lookups
    let mut cache = None;

    // the path_inside_last applies only in the IPLD projection case and its main consumer is the
    // `/dag/resolve` API where the response is the returned cid and the "remaining path".
    let mut path_inside_last = Vec::new();

    // important: on the `/refs` path we need to fetch the first block to fail deterministically so we
    // need to load it either way here; if the response gets processed to the stream phase, it'll
    // always fire up a response and the test 'should print nothing for non-existent hashes' fails.
    // Not sure how correct that is, but that is the test.
    'outer: loop {
        let Block { data, .. } = match ipfs.get_block(&current).await {
            Ok(block) => block,
            Err(e) => return Err(WalkError::from((WalkFailed::from(e), current))),
        };

        // needs to be mutable because the Ipld walk will overwrite it to project down in the
        // document
        let mut needle = if let Some(needle) = path.next() {
            needle
        } else {
            return Ok((current, Loaded::Raw(data), Vec::new()));
        };

        if current.codec() == cid::Codec::DagProtobuf {
            let mut lookup = match ipfs::unixfs::ll::resolve(&data, &needle, &mut cache) {
                Ok(MaybeResolved::NeedToLoadMore(lookup)) => lookup,
                Ok(MaybeResolved::Found(cid)) => {
                    current = cid;
                    continue;
                }
                Ok(MaybeResolved::NotFound) => {
                    return handle_dagpb_not_found(current, &data, needle, &path)
                }
                Err(ResolveError::UnexpectedType(_)) => {
                    // the conformance tests use a path which would end up going through a file
                    // and the returned error string is tested against listed alternatives.
                    // unexpected type is not one of them.
                    let e = WalkFailed::from(path::WalkFailed::UnmatchedNamedLink(needle));
                    return Err(WalkError::from((e, current)));
                }
                Err(e) => return Err(WalkError::from((WalkFailed::from(e), current))),
            };

            loop {
                let (next, _) = lookup.pending_links();

                // need to take ownership in order to enrich the error, next is invalidaded on
                // lookup.continue_walk.
                let next = next.to_owned();

                let Block { data, .. } = match ipfs.get_block(&next).await {
                    Ok(block) => block,
                    Err(e) => return Err(WalkError::from((WalkFailed::from(e), next))),
                };

                match lookup.continue_walk(&data, &mut cache) {
                    Ok(MaybeResolved::NeedToLoadMore(next)) => lookup = next,
                    Ok(MaybeResolved::Found(cid)) => {
                        current = cid;
                        break;
                    }
                    Ok(MaybeResolved::NotFound) => {
                        return handle_dagpb_not_found(next, &data, needle, &path)
                    }
                    Err(e) => {
                        return Err(WalkError::from((
                            WalkFailed::from(e.into_resolve_error()),
                            next,
                        )))
                    }
                }
            }
        } else {
            path_inside_last.clear();

            let mut ipld = match decode_ipld(&current, &data) {
                Ok(ipld) => ipld,
                Err(e) => return Err(WalkError::from((WalkFailed::from(e), current))),
            };

            loop {
                // this needs to be stored at least temporarily to recover the path_inside_last or
                // the "remaining path"
                let tmp = needle.clone();
                ipld = match IpfsPath::resolve_segment(needle, ipld) {
                    Ok(WalkSuccess::EmptyPath(_)) => unreachable!(),
                    Ok(WalkSuccess::AtDestination(ipld)) => {
                        path_inside_last.push(tmp);
                        ipld
                    }
                    Ok(WalkSuccess::Link(_, next_cid)) => {
                        current = next_cid;
                        continue 'outer;
                    }
                    Err(e) => return Err(WalkError::from((WalkFailed::from(e), current))),
                };

                // we might resolve multiple segments inside a single document
                needle = match path.next() {
                    Some(needle) => needle,
                    None => break,
                };
            }

            if path.len() == 0 {
                // when done with the remaining IpfsPath we should be set with the projected Ipld
                // document
                path_inside_last.shrink_to_fit();
                return Ok((current, Loaded::Ipld(ipld), path_inside_last));
            }
        }
    }
}

fn handle_dagpb_not_found(
    at: Cid,
    data: &[u8],
    needle: String,
    path: &IpfsPath,
) -> Result<(Cid, Loaded, Vec<String>), WalkError> {
    use ipfs::unixfs::ll::dagpb::node_data;

    if needle == "Data" && path.len() == 0 && path.follow_dagpb_data() {
        // /dag/resolve needs to "resolve through" a dag-pb node down to the "just data" even
        // though we do not need to extract it ... however this might be good to just filter with
        // refs, as no refs of such path can exist as the links are in the outer structure.
        //
        // testing with go-ipfs 0.5 reveals that dag resolve only follows links
        // which are actually present in the dag-pb, not numeric links like Links/5
        // or links/5, even if such are present in the `dag get` output.
        //
        // comment on this special casing: there cannot be any other such
        // special case as the Links do not work like Data so while this is not
        // pretty, it's not terrible.
        let data = node_data(&data)
            .expect("already parsed once, second time cannot fail")
            .unwrap_or_default();
        Ok((at, Loaded::Ipld(Ipld::Bytes(data.to_vec())), vec![needle]))
    } else {
        let e = WalkFailed::from(path::WalkFailed::UnmatchedNamedLink(needle));
        Err(WalkError::from((e, at)))
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
fn iplds_refs(
    ipfs: Ipfs,
    iplds: Vec<(Cid, Loaded)>,
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

        for (origin, maybe_ipld) in iplds {

            let ipld = match maybe_ipld {
                Loaded::Ipld(ipld) => ipld,
                Loaded::Raw(data) => {
                    decode_ipld(&origin, &data).map_err(|e| e.to_string())?
                }
            };

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
pub fn local(ipfs: &Ipfs) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::path!("refs" / "local")
        .and(with_ipfs(ipfs))
        .and_then(inner_local)
}

async fn inner_local(ipfs: Ipfs) -> Result<impl Reply, Rejection> {
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
    use super::{ipld_links, local, refs_paths, Edge, IpfsPath};
    use cid::{self, Cid};
    use futures::stream::TryStreamExt;
    use ipfs::{Block, Ipfs};
    use libipld::block::{decode_ipld, validate};
    use std::collections::HashSet;
    use std::convert::TryFrom;

    #[tokio::test]
    async fn test_inner_local() {
        let (ipfs, _fut) = preloaded_testing_ipfs().await;
        let filter = local(&ipfs);

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
    async fn all_refs_from_root() {
        let (ipfs, _fut) = preloaded_testing_ipfs().await;

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
        let (ipfs, _fut) = preloaded_testing_ipfs().await;

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
        let (ipfs, _fut) = preloaded_testing_ipfs().await;

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

    async fn preloaded_testing_ipfs() -> (Ipfs, async_std::task::JoinHandle<()>) {
        let options = ipfs::IpfsOptions::inmemory_with_generated_keys();
        let (ipfs, fut) = ipfs::UninitializedIpfs::new(options)
            .await
            .start()
            .await
            .unwrap();
        let fut = async_std::task::spawn(fut);

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

        (ipfs, fut)
    }

    #[test]
    fn dagpb_links() {
        // this is the same as in v0::refs::path::tests::walk_dagpb_links
        let payload = hex::decode(
            "12330a2212206aad27d7e2fc815cd15bf679535062565dc927a831547281\
            fc0af9e5d7e67c74120b6166726963616e2e747874180812340a221220fd\
            36ac5279964db0cba8f7fa45f8c4c44ef5e2ff55da85936a378c96c9c632\
            04120c616d6572696361732e747874180812360a2212207564c20415869d\
            77a8a40ca68a9158e397dd48bdff1325cdb23c5bcd181acd17120e617573\
            7472616c69616e2e7478741808",
        )
        .unwrap();

        let cid = Cid::try_from("QmbrFTo4s6H23W6wmoZKQC2vSogGeQ4dYiceSqJddzrKVa").unwrap();

        let decoded = decode_ipld(&cid, &payload).unwrap();

        let links = ipld_links(&cid, decoded)
            .map(|(name, _)| name.unwrap())
            .collect::<Vec<_>>();

        assert_eq!(links, ["african.txt", "americas.txt", "australian.txt",]);
    }
}
