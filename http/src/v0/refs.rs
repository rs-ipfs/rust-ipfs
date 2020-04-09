use futures::stream;
use ipfs::{Ipfs, IpfsTypes};
use serde::Serialize;
use warp::hyper::Body;
use futures::stream::Stream;
use ipfs::{Block, Error};
use ipfs::{Ipfs, IpfsTypes};
use libipld::cid::Cid;
use serde::Deserialize;
use std::collections::{VecDeque, BTreeMap};
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
    opts: RefsOptions,
) -> Result<impl Reply, Rejection> {
    let max_depth = opts.max_depth();

    Ok("foo")
}

#[derive(Debug, Deserialize)]
struct RefsOptions {
    /// This can start with /ipfs/ but doesn't have to, can continue with paths, if a link cannot
    /// be found it's an json error from go-ipfs
    arg: String,
    /// This can be used to format the output string into the `{ "Ref": "here" .. }`
    format: Option<String>,
    /// This cannot be used with `format`, prepends "source -> " to the `Ref` response
    #[serde(default)]
    edges: bool,
    /// Not sure if this is tested by conformance testing but I'd assume this destinatinos on their
    /// first linking.
    #[serde(default)]
    unique: bool,
    #[serde(default)]
    recursive: bool,
    // `int` in the docs apparently is platform specific
    // go-ipfs only honors this when `recursive` is true.
    // go-ipfs treats -2 as -1 when `recursive` is true.
    // go-ipfs doesn't use the json return value if this value is too large or non-int
    #[serde(rename = "max-depth")]
    max_depth: Option<i64>,
}

impl RefsOptions {
    fn max_depth(&self) -> Option<u64> {
        if self.recursive {
            match self.max_depth {
                // zero means do nothing
                Some(x) if x >= 0 => Some(x as u64),
                _ => None,
            }
        } else {
            // only immediate links after the path
            Some(1)
        }
    }
}

#[derive(Debug)]
enum PathError {
    InvalidCid(libipld::cid::Error),
    InvalidPath,
}

use std::fmt;

impl fmt::Display for PathError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            PathError::InvalidCid(e) => write!(fmt, "{}", e),
            PathError::InvalidPath => write!(fmt, "invalid path"),
        }
    }
}

impl std::error::Error for PathError {}

/// Following https://github.com/ipfs/go-path/
struct IpfsPath {
    /// Option to support moving the cid
    root: Option<Cid>,
    path: std::vec::IntoIter<String>,
}

use std::convert::TryFrom;

impl From<Cid> for IpfsPath {
    fn from(root: Cid) -> IpfsPath {
        IpfsPath {
            root: Some(root),
            path: Vec::new().into_iter(),
        }
    }
}

impl TryFrom<&str> for IpfsPath {
    type Error = PathError;

    fn try_from(path: &str) -> Result<Self, Self::Error> {
        let mut split = path.splitn(2, "/ipfs/");
        let first = split.next();
        let (root, path) = match first {
            Some("") => {
                /* started with /ipfs/ */
                if let Some(x) = split.next() {
                    // was /ipfs/x
                    ("ipfs", x)
                } else {
                    // just the /ipfs/
                    return Err(PathError::InvalidPath);
                }
            }
            Some(x) => {
                /* maybe didn't start with /ipfs/, need to check second */
                if let Some(_) = split.next() {
                    // x/ipfs/_
                    return Err(PathError::InvalidPath);
                }

                ("", x)
            }
            None => return Err(PathError::InvalidPath),
        };

        let mut split = path.splitn(2, '/');
        log::trace!("splitting {:?} per path", path);
        let root = split
            .next()
            .expect("first value from splitn(2, _) must exist");

        let path = split
            .next()
            .iter()
            .flat_map(|s| s.split('/').filter(|s| !s.is_empty()).map(String::from))
            .collect::<Vec<_>>()
            .into_iter();

        let root = Some(Cid::try_from(root).map_err(PathError::InvalidCid)?);

        Ok(IpfsPath { root, path })
    }
}

impl IpfsPath {
    pub fn new(path: &str) -> Result<Self, PathError> {
        Self::try_from(path)
    }

    pub fn root(&self) -> Option<&Cid> {
        self.root.as_ref()
    }

    pub fn take_root(&mut self) -> Option<Cid> {
        self.root.take()
    }

    pub fn walk(&mut self, mut ipld: Ipld) -> Result<WalkSuccess, WalkFailed> {
        if self.len() == 0 {
            return Ok(WalkSuccess::EmptyPath(ipld));
        }
        while let Some(key) = self.next() {
            // FIXME: can you have an ipld document which is only a link? well if it was
            // possible, we can handle it with the default case?
            ipld = match ipld {
                Ipld::Map(mut m) if m.contains_key(&key) => {
                    if let Some(ipld) = m.remove(&key) {
                        ipld
                    } else {
                        return Err(WalkFailed::UnmatchedMapProperty(m, key));
                    }
                }
                Ipld::List(mut l) => {
                    if let Ok(index) = key.parse::<usize>() {
                        if index < l.len() {
                            l.swap_remove(index)
                        } else {
                            return Err(WalkFailed::ListIndexOutOfRange(l, index));
                        }
                    } else {
                        return Err(WalkFailed::UnparseableListIndex(l, key));
                    }
                }
                x => return Err(WalkFailed::UnmatchableSegment(x, key)),
            };

            if let Ipld::Link(next_cid) = ipld {
                return Ok(WalkSuccess::Link(key, next_cid));
            }
        }

        Ok(WalkSuccess::AtDestination(ipld))
    }
}

pub enum WalkSuccess {
    /// IpfsPath was already empty, or became empty during previous walk
    EmptyPath(Ipld),
    /// IpfsPath arrived at destination, following walk attempts will return EmptyPath
    AtDestination(Ipld),
    /// Path segment lead to a link which needs to be loaded to continue the walk
    Link(String, Cid),
}

pub enum WalkFailed {
    /// Map key was not found
    UnmatchedMapProperty(BTreeMap<String, Ipld>, String),
    /// Segment could not be parsed as index
    UnparseableListIndex(Vec<Ipld>, String),
    /// Segment was out of range for the list
    ListIndexOutOfRange(Vec<Ipld>, usize),
    /// Catch-all failure for example when walking a segment on integer
    UnmatchableSegment(Ipld, String),
}

impl Iterator for IpfsPath {
    type Item = String;

    fn next(&mut self) -> Option<String> {
        self.path.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.path.size_hint()
    }
}

impl ExactSizeIterator for IpfsPath {
    fn len(&self) -> usize {
        self.path.len()
    }
}

// RefsStream which goes around in bfs
//
// - unsure if it should track duplicates, what if we get a loop
//
// Should return all in some order, probably in the order of discovery. Tests sort the values
// either way. Probably something like struct Edge { source: Cid, destination: Cid }.

fn edges<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    mut path: IpfsPath,
    max_depth: Option<u64>,
) -> impl Stream<Item = Result<(Cid, Cid), Error>> {
    use async_stream::try_stream;

    // this looks great but the current implementation turns this into a future which yields
    // through a oneshot channel. I think it's worth it as the by-hand stream implementation is
    // quite a lot of work, at least until doing one. This is very slow to compile though.
    try_stream! {
        if let Some(0) = max_depth {
            // go-ipfs returns immediatedly without checking if the cid is available
            return;
        }

        let mut work = VecDeque::new();
        work.push_back((0u64, path.take_root().unwrap(), None));

        'work: while let Some((depth, cid, source)) = work.pop_front() {
            match source.as_ref() {
                Some(src) => log::trace!("depth={}, cid={}, source={}", depth, cid, src),
                _ => log::trace!("depth={}, cid={}, source=None", depth, cid),
            }

            match max_depth {
                Some(d) if d <= depth => {
                    log::trace!("stopping at target depth {}", depth);
                    return;
                },
                _ => {}
            }

            // this cannot block the processing, just adjust this result
            let Block { cid, data } = if let Ok(block) = ipfs.get_block(&cid).await {
                block
            } else {
                // TODO: yield error msg
                continue;
            };

            let mut ipld = if let Ok(ipld) = decode_ipld(&cid, &data) {
                ipld
            } else {
                // TODO: yield error msg
                continue;
            };

            let ipld = match path.walk(ipld) {
                Ok(WalkSuccess::EmptyPath(ipld)) | Ok(WalkSuccess::AtDestination(ipld)) => ipld,
                Ok(WalkSuccess::Link(_key, next_cid)) => {
                    work.push_back((depth, next_cid, Some(cid)));
                    continue;
                }

                // go-ipfs: if the path was unmatchable, or invalid int ("foo", or out of range, or
                // negative) a 500 and internal server error is returned ... feels a bit wrong. but
                // then again, I just spliced this structure wrong.
                //
                // there needs to be a path_refs and an ipld_refs, latter returns a stream and
                // first is a future. it'll help with DFS and BFS concerns as well.
                Err(_) => return,
            };

            assert_eq!(path.len(), 0, "here we should be done with the path and depth first search");

            let links = block_links(ipld)?;

            for next_cid in links {
                work.push_back((depth + 1, next_cid, Some(cid.clone())));
            }

            if let Some(source) = source {
                // depth == 0 means that we are still traversing the path or have just completed
                // it, so the source can be both None and Some(_) depending on if this is the root
                // or some followed link.
                if depth > 0 {
                    log::trace!("yielding {} -> {}", source, cid);
                    yield (source, cid);
                }
            }
        }
    }
}

use libipld::{block::decode_ipld, Ipld};

fn block_links(ipld: Ipld) -> Result<impl Iterator<Item = Cid>, Error> {
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

#[cfg(test)]
async fn preloaded_testing_ipfs() -> Ipfs<ipfs::TestTypes> {
    use libipld::block::validate;

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

#[cfg(test)]
fn assert_edges(expected: &[(&str, &str)], actual: &[(String, String)]) {
    use std::collections::HashSet;
    let expected: HashSet<_> = expected.iter().map(|&(a, b)| (a, b)).collect();

    let actual: HashSet<_> = actual
        .iter()
        .map(|(a, b)| (a.as_str(), b.as_str()))
        .collect();

    let diff: Vec<_> = expected.symmetric_difference(&actual).collect();

    assert!(diff.is_empty(), "{:#?}", diff);
}

#[tokio::test]
async fn all_refs_from_root() {
    use futures::stream::TryStreamExt;
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

    let all_edges: Vec<_> = edges(ipfs, IpfsPath::try_from(root).unwrap(), None)
        .map_ok(|(source, dest)| (source.to_string(), dest.to_string()))
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
#[ignore]
async fn all_unique_refs_from_root() {
    use futures::stream::TryStreamExt;
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

    let all_edges: Vec<_> = edges(ipfs, IpfsPath::try_from(root).unwrap(), None)
        .map_ok(|(source, dest)| (source.to_string(), dest.to_string()))
        .try_collect()
        .await
        .unwrap();

    // go-ipfs output:
    // bafyreihpc3vupfos5yqnlakgpjxtyx3smkg26ft7e2jnqf3qkyhromhb64 -> bafyreidquig3arts3bmee53rutt463hdyu6ff4zeas2etf2h2oh4dfms44
    // bafyreihpc3vupfos5yqnlakgpjxtyx3smkg26ft7e2jnqf3qkyhromhb64 -> QmPJ4A6Su27ABvvduX78x2qdWMzkdAYxqeH5TVrHeo3xyy
    // bafyreihpc3vupfos5yqnlakgpjxtyx3smkg26ft7e2jnqf3qkyhromhb64 -> bafyreibvjvcv745gig4mvqs4hctx4zfkono4rjejm2ta6gtyzkqxfjeily
    // bafyreihpc3vupfos5yqnlakgpjxtyx3smkg26ft7e2jnqf3qkyhromhb64 -> QmRgutAxd8t7oGkSm4wmeuByG6M51wcTso6cubDdQtuEfL
    //
    // conformance tests test this with <linkname> rendering on dagpb, on dagcbor linknames are
    // always empty?
    todo!("this test needs all fixtures in dagpb format as <linkname> from cbor is empty str for go-ipfs?")
}

#[tokio::test]
async fn refs_with_path() {
    use futures::stream::TryStreamExt;
    env_logger::init();

    let ipfs = preloaded_testing_ipfs().await;

    let paths = [
        "/ipfs/bafyreidquig3arts3bmee53rutt463hdyu6ff4zeas2etf2h2oh4dfms44/foo",
        "bafyreidquig3arts3bmee53rutt463hdyu6ff4zeas2etf2h2oh4dfms44/foo",
        "bafyreihpc3vupfos5yqnlakgpjxtyx3smkg26ft7e2jnqf3qkyhromhb64/0/foo",
        "bafyreihpc3vupfos5yqnlakgpjxtyx3smkg26ft7e2jnqf3qkyhromhb64/0/foo/",
    ];

    for path in paths.iter() {
        let path = IpfsPath::try_from(*path).unwrap();
        let all_edges: Vec<_> = edges(ipfs.clone(), path, None)
            .map_ok(|(source, dest)| (source.to_string(), dest.to_string()))
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
