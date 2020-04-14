use futures::stream;
use ipfs::{Ipfs, IpfsTypes};
use warp::hyper::Body;
use futures::stream::Stream;
use ipfs::{Block, Error};
use libipld::cid::{Cid, Codec as CidCodec};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::borrow::Cow;
use warp::{path, query, Filter, Rejection, Reply};
use std::convert::TryFrom;
use std::fmt;
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
        .and(refs_options())
        .and_then(refs_inner)
}

async fn refs_inner<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    opts: RefsOptions,
) -> Result<impl Reply, Rejection> {
    use futures::stream::StreamExt;

    let max_depth = opts.max_depth();
    let formatter = opts.formatter()?;

    log::trace!("refs on {:?} to depth {:?} with {:?}", opts.arg, max_depth, formatter);

    let paths = opts.arg.iter()
        .map(|s| IpfsPath::try_from(s.as_str()).map_err(StringError::from))
        .collect::<Result<Vec<_>, _>>()?;

    let st = refs_paths(ipfs, paths, max_depth)
        .await
        .map_err(|e| { log::warn!("refs path on {:?} failed with {}", &opts.arg, e); e })
        .map_err(StringError::from)?;

    let st = st.map(move |res| {
        let res = match res {
            Ok((source, dest, link_name)) => {
                let ok = formatter.format(source, dest, link_name);
                serde_json::to_string(&Edge { ok: ok.into(), err: "".into() })
            },
            Err(e) => {
                serde_json::to_string(&Edge { ok: "".into(), err: e.to_string().into() })
            }
        };

        let res = match res {
            Ok(mut s) => {
                s.push('\n');
                Ok(s.into_bytes())
            },
            Err(e) => {
                log::error!("edge serialization failed: {}", e);
                Err(HandledErr)
            }
        };

        res
    });

    Ok(StreamResponse(Unshared::new(st)))
}

use pin_project::pin_project;

/// Copied from https://docs.rs/crate/async-compression/0.3.2/source/src/unshared.rs ... Did not
/// keep the safety discussion comment because I am unsure if this is safe with the pinned
/// projections.
///
/// The reason why this is needed is because `warp` or `hyper` needs it. `hyper` needs it because
/// of compiler bug https://github.com/hyperium/hyper/issues/2159 and the future or stream we
/// combine up with `async-stream` is not `Sync`, because the `async_trait` builds up a
/// `Pin<Box<dyn std::future::Future<Output = _> + Send + '_>>`. The lifetime of those futures is
/// not an issue, because at higher level (`refs_path`) those are within the owned values that
/// method receives. It is unclear for me at least if the compiler is too strict with the `Sync`
/// requirement which is derives for any reference or if the root cause here is that `hyper`
/// suffers from that compiler issue.
///
/// Related: https://internals.rust-lang.org/t/what-shall-sync-mean-across-an-await/12020
/// Related: https://github.com/dtolnay/async-trait/issues/77
#[pin_project]
struct Unshared<T> {
    #[pin]
    inner: T,
}

#[allow(dead_code)]
impl<T> Unshared<T> {
    pub fn new(inner: T) -> Self {
        Unshared { inner }
    }
}

unsafe impl<T> Sync for Unshared<T> {}

impl<T> fmt::Debug for Unshared<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(core::any::type_name::<T>()).finish()
    }
}

use std::{pin::Pin, task::{Context, Poll}};

impl<S> futures::stream::Stream for Unshared<S>
where S: futures::stream::Stream {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Option<Self::Item>> {
        let this = self.project();
        this.inner.poll_next(ctx)
    }
}

struct StreamResponse<S>(S);

#[derive(Debug)]
struct HandledErr;

impl std::error::Error for HandledErr {}

impl fmt::Display for HandledErr {
    fn fmt(&self, _fmt: &mut fmt::Formatter) -> fmt::Result {
        Ok(())
    }
}

impl<S> warp::Reply for StreamResponse<S>
    where S: futures::stream::TryStream + Send + Sync + 'static,
          S::Ok: Into<warp::hyper::body::Bytes>,
          S::Error: std::error::Error + Send + Sync + 'static
{
    fn into_response(self) -> warp::reply::Response {
        use futures::stream::TryStreamExt;

        let res = warp::reply::Response::new(Body::wrap_stream(self.0.into_stream()));

        res
    }
}

#[derive(Debug, Serialize)]
struct Edge {
    #[serde(rename = "Ref")]
    ok: Cow<'static, str>,
    #[serde(rename = "Err")]
    err: Cow<'static, str>,
}

#[derive(Debug)]
struct RefsOptions {
    /// This can start with /ipfs/ but doesn't have to, can continue with paths, if a link cannot
    /// be found it's an json error from go-ipfs
    arg: Vec<String>,
    /// This can be used to format the output string into the `{ "Ref": "here" .. }`
    format: Option<String>,
    /// This cannot be used with `format`, prepends "source -> " to the `Ref` response
    edges: bool,
    /// Not sure if this is tested by conformance testing but I'd assume this destinatinos on their
    /// first linking.
    unique: bool,
    recursive: bool,
    // `int` in the docs apparently is platform specific
    // go-ipfs only honors this when `recursive` is true.
    // go-ipfs treats -2 as -1 when `recursive` is true.
    // go-ipfs doesn't use the json return value if this value is too large or non-int
    max_depth: Option<i64>,
}

#[derive(Debug)]
enum RefOptionsParseError<'a> {
    DuplicateField(Cow<'a, str>),
    MissingArg,
    InvalidNumber(Cow<'a, str>, Cow<'a, str>),
    InvalidBoolean(Cow<'a, str>, Cow<'a, str>),
}

impl<'a> fmt::Display for RefOptionsParseError<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        use RefOptionsParseError::*;
        match *self {
            DuplicateField(ref s) => write!(fmt, "field {:?} was duplicated", *s),
            MissingArg => write!(fmt, "required field \"arg\" missing"),
            InvalidNumber(ref k, ref v) => write!(fmt, "field {:?} invalid number: {:?}", *k, *v),
            InvalidBoolean(ref k, ref v) => write!(fmt, "field {:?} invalid boolean: {:?}", *k, *v),
        }
    }
}

impl<'a> std::error::Error for RefOptionsParseError<'a> {}

impl<'a> TryFrom<&'a str> for RefsOptions {
    type Error = RefOptionsParseError<'a>;

    fn try_from(q: &'a str) -> Result<Self, Self::Error> {
        use RefOptionsParseError::*;

        // TODO: check how go-ipfs handles duplicate parameters for non-Vec fields
        //
        // this manual deserialization is required because `serde_urlencoded` (used by
        // warp::query) does not support multiple instances of the same field, nor does
        // `serde_qs` (it would support arg[]=...). supporting this in `serde_urlencoded` is
        // out of scope; not sure of `serde_qs`.
        let parse = url::form_urlencoded::parse(q.as_bytes());

        let mut args = Vec::new();
        let mut format = None;
        let mut edges = None;
        let mut unique = None;
        let mut recursive = None;
        let mut max_depth = None;

        for (key, value) in parse {
            let target = match &*key {
                "arg" => {
                    args.push(value.into_owned());
                    continue;
                },
                "format" => {
                    if format.is_none() {
                        // not parsing this the whole way as there might be hope to have this
                        // function removed in the future.
                        format = Some(value.into_owned());
                        continue;
                    } else {
                        return Err(DuplicateField(key));
                    }
                },
                "max-depth" => {
                    if max_depth.is_none() {
                        max_depth = match value.parse::<i64>() {
                            Ok(max_depth) => Some(max_depth),
                            Err(_) => return Err(InvalidNumber(key, value)),
                        };
                        continue;
                    } else {
                        return Err(DuplicateField(key));
                    }
                },
                "edges" => &mut edges,
                "unique" => &mut unique,
                "recursive" => &mut recursive,
                _ => {
                    // ignore unknown fields
                    continue;
                }
            };

            // common bool field handling
            if target.is_none() {
                match value.parse::<bool>() {
                    Ok(value) => *target = Some(value),
                    Err(_) => return Err(InvalidBoolean(key, value)),
                }
            } else {
                return Err(DuplicateField(key));
            }
        }

        if args.is_empty() {
            return Err(MissingArg);
        }

        Ok(RefsOptions {
            arg: args,
            format,
            edges: edges.unwrap_or(false),
            unique: unique.unwrap_or(false),
            recursive: recursive.unwrap_or(false),
            max_depth
        })
    }
}

fn refs_options() -> impl Filter<Extract = (RefsOptions,), Error = Rejection> + Clone {
    warp::filters::query::raw()
        .and_then(|q: String| {

            let res = RefsOptions::try_from(q.as_str())
                .map_err(StringError::from)
                .map_err(|e| warp::reject::custom(e));

            futures::future::ready(res)
        })
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

    fn formatter(&self) -> Result<EdgeFormatter, StringError> {
        if self.edges && self.format.is_some() {
            // msg from go-ipfs
            return Err(StringError::new("using format argument with edges is not allowed".into()));
        }

        if self.edges {
            Ok(EdgeFormatter::Arrow)
        } else if let Some(formatstr) = self.format.as_deref() {
            let parts = parse_format(formatstr).map_err(StringError::from)?;
            Ok(EdgeFormatter::FormatString(parts))
        } else {
            Ok(EdgeFormatter::Destination)
        }
    }
}


#[derive(Debug)]
enum EdgeFormatter {
    Destination,
    Arrow,
    FormatString(Vec<FormattedPart>)
}

/// Different parts of the format string
#[derive(Debug, PartialEq, Eq)]
enum FormattedPart {
    Static(String),
    Source,
    Destination,
    LinkName
}

impl FormattedPart {
    fn format(&self, out: &mut String, src: &Cid, dst: &Cid, linkname: Option<&str>) {
        use FormattedPart::*;
        use fmt::Write;
        match *self {
            Static(ref s) => out.push_str(s),
            Source => write!(out, "{}", src).expect("String writing shouldn't fail"),
            Destination => write!(out, "{}", dst).expect("String writing shouldn't fail"),
            LinkName => if let Some(s) = linkname {
                out.push_str(s)
            },
        }
    }
}

impl EdgeFormatter {
    fn format(&self, src: Cid, dst: Cid, link_name: Option<String>) -> String {
        match *self {
            EdgeFormatter::Destination => dst.to_string(),
            EdgeFormatter::Arrow => format!("{} -> {}", src, dst),
            EdgeFormatter::FormatString(ref parts) => {
                let mut out = String::new();
                for part in parts {
                    part.format(&mut out, &src, &dst, link_name.as_deref());
                }
                out.shrink_to_fit();
                out
            },
        }
    }
}

#[derive(Debug)]
enum FormatError<'a> {
    UnsupportedTag(&'a str),
    UnterminatedTag(usize)
}

impl<'a> fmt::Display for FormatError<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        use FormatError::*;
        match *self {
            UnsupportedTag(tag) => write!(fmt, "unsupported tag: {:?}", tag),
            UnterminatedTag(index) => write!(fmt, "unterminated tag at index: {}", index),
        }
    }
}

impl<'a> std::error::Error for FormatError<'a> {}

fn parse_format(s: &str) -> Result<Vec<FormattedPart>, FormatError> {
    use std::mem;

    let mut buffer = String::new();
    let mut ret = Vec::new();
    let mut chars = s.char_indices();

    loop {
        match chars.next() {
            Some((index, '<')) => {
                if !buffer.is_empty() {
                    ret.push(FormattedPart::Static(mem::take(&mut buffer)));
                }

                let remaining = chars.as_str();
                let end = remaining.find('>').ok_or_else(|| FormatError::UnterminatedTag(index))?;

                // the use of string indices here is ok as the angle brackets are ascii and
                // cannot be in the middle of multibyte char boundaries
                let inside = &remaining[0..end];

                // TODO: this might need to be case insensitive
                let part = match inside {
                    "src" => FormattedPart::Source,
                    "dst" => FormattedPart::Destination,
                    "linkname" => FormattedPart::LinkName,
                    tag => return Err(FormatError::UnsupportedTag(tag)),
                };

                ret.push(part);

                // the one here is to ignore the '>', which cannot be a codepoint boundary
                chars = remaining[end + 1..].char_indices();
            }
            Some((_, ch)) => buffer.push(ch),
            None => {
                if !buffer.is_empty() {
                    ret.push(FormattedPart::Static(buffer));
                }
                return Ok(ret);
            }
        }
    }
}

#[test]
fn parse_good_formats() {
    use FormattedPart::*;

    let examples = &[
        ("<linkname>: <src> -> <dst>", vec![LinkName, Static(": ".into()), Source, Static(" -> ".into()), Destination]),
        ("-<linkname>", vec![Static("-".into()), LinkName]),
        ("<linkname>-", vec![LinkName, Static("-".into())]),
    ];

    for (input, expected) in examples {
        assert_eq!(&parse_format(input).unwrap(), expected);
    }
}

#[derive(Debug)]
enum PathError {
    InvalidCid(libipld::cid::Error),
    InvalidPath,
}

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
#[derive(Debug)]
struct IpfsPath {
    /// Option to support moving the cid
    root: Option<Cid>,
    path: std::vec::IntoIter<String>,
}

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
        let (_root, path) = match first {
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
    pub fn take_root(&mut self) -> Option<Cid> {
        self.root.take()
    }

    pub fn walk(&mut self, current: &Cid, mut ipld: Ipld) -> Result<WalkSuccess, WalkFailed> {
        if self.len() == 0 {
            return Ok(WalkSuccess::EmptyPath(ipld));
        }
        while let Some(key) = self.next() {

            if current.codec() == CidCodec::DagProtobuf {
                // this "specialization" serves as a workaround until the dag-pb
                // as Ipld is up to speed with {go,js}-ipfs counterparts

                // the current dag-pb represents the links of dag-pb nodes under "/Links"
                // panicking instead of errors since we want to change this is dag-pb2ipld
                // structure changes.

                let (map, links) = match ipld {
                    Ipld::Map(mut m) => { let links = m.remove("Links"); (m, links) },
                    x => panic!("Expected dag-pb2ipld have top-level \"Links\", was: {:?}", x),
                };

                let mut links = match links {
                    Some(Ipld::List(vec)) => vec,
                    Some(x) => panic!("Expected dag-pb2ipld top-level \"Links\" to be List, was: {:?}", x),
                    // assume this means that the list was empty, and as such wasn't created
                    None => return Err(WalkFailed::UnmatchableSegment(Ipld::Map(map), key)),
                };

                let index = if let Ok(index) = key.parse::<usize>() {
                    if index >= links.len() {
                        return Err(WalkFailed::ListIndexOutOfRange(links, index));
                    }
                    index
                } else {
                    let index = links.iter().enumerate().position(|(i, link)| match link.get("Name") {
                        Some(Ipld::String(s)) => s == &key,
                        Some(x) => panic!("Expected dag-pb2ipld \"Links[{}]/Name\" to be an optional String, was: {:?}", i, x),
                        None => false,
                    });

                    match index {
                        Some(index) => index,
                        None => return Err(WalkFailed::UnmatchableSegment(Ipld::List(links), key)),
                    }
                };

                let link = links.swap_remove(index);

                match link {
                    Ipld::Map(mut m) => {
                        let link = match m.remove("Hash") {
                            Some(Ipld::Link(link)) => link,
                            Some(x) => panic!("Expected dag-pb2ipld \"Links[{}]/Hash\" to be a link, was: {:?}", index, x),
                            None => panic!("Expected dag-pb2ipld \"Links[{}]/Hash\" to exist", index),
                        };

                        return Ok(WalkSuccess::Link(key, link));
                    }
                    x => panic!("Expected dag-pb2ipld \"Links[{}]\" to be a Map, was: {:?}", index, x),
                }
            }

            ipld = match ipld {
                Ipld::Link(cid) if key == "." => {
                    // go-ipfs: allows this to be skipped. lets require the dot for now.
                    // FIXME: this would require the iterator to be peekable in addition.
                    return Ok(WalkSuccess::Link(key, cid));
                }
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

    fn debug<'a>(&'a self, current: &'a Cid) -> DebuggableIpfsPath<'a> {
        DebuggableIpfsPath {
            current,
            segments: self.path.as_slice(),
        }
    }
}

struct DebuggableIpfsPath<'a> {
    current: &'a Cid,
    segments: &'a [String],
}

impl<'a> fmt::Debug for DebuggableIpfsPath<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", self.current)?;
        if !self.segments.is_empty() {
            write!(fmt, "/...")?;
        }

        for seg in self.segments {
            write!(fmt, "/{}", seg)?;
        }
        Ok(())
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

// FIXME: this probably needs to result in http 40x error? Currently converted to a stringerror
// which is 500.
#[derive(Debug)]
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

impl fmt::Display for WalkFailed {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            // go-ipfs: no such link found
            WalkFailed::UnmatchedMapProperty(_, ref key) => {
                write!(fmt, "No such link found: {:?}", key)
            }
            // go-ipfs: strconv.Atoi: parsing {:?}: invalid syntax
            WalkFailed::UnparseableListIndex(_, ref segment) => {
                write!(fmt, "Invalid list index: {:?}", segment)
            }
            // go-ipfs: array index out of range
            WalkFailed::ListIndexOutOfRange(ref list, index) => write!(
                fmt,
                "List index out of range: the length is {} but the index is {}",
                list.len(),
                index
            ),
            // go-ipfs: tried to resolve through object that had no links
            WalkFailed::UnmatchableSegment(_, _) => {
                write!(fmt, "Tried to resolve through object that had no links")
            }
        }
    }
}

impl std::error::Error for WalkFailed {}

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

/// Refs similar to go-ipfs `refs` which will first walk the path and then continue streaming the
/// results after first walking the path.
async fn refs_paths<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    paths: Vec<IpfsPath>,
    max_depth: Option<u64>,
) -> Result<impl Stream<Item = Result<(Cid, Cid, Option<String>), String>> + Send + 'static, Error> {
    use futures::stream::FuturesOrdered;
    use futures::stream::TryStreamExt;

    let mut walks = FuturesOrdered::new(); // Vec::with_capacity(paths.len());

    for path in paths {
        walks.push(walk_path(&ipfs, path));
    }

    let iplds = walks.try_collect().await?;

    Ok(iplds_refs(ipfs, iplds, max_depth))
}

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

fn iplds_refs<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    iplds: Vec<(Cid, Ipld)>,
    max_depth: Option<u64>,
) -> impl Stream<Item = Result<(Cid, Cid, Option<String>), String>> + Send + 'static {
    use async_stream::stream;

    stream! {
        if let Some(0) = max_depth {
            return;
        }

        let mut work = VecDeque::new();

        for (origin, ipld) in iplds {
            for (link_name, next_cid) in ipld_links(ipld) {
                work.push_back((0, next_cid, origin.clone(), link_name));
            }
        }

        while let Some((depth, cid, source, link_name)) = work.pop_front() {
            match max_depth {
                Some(d) if d <= depth => {
                    return;
                },
                _ => {}
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
                    // TODO: yield error msg
                    // go-ipfs on raw Qm hash:
                    // > failed to decode Protocol Buffers: incorrectly formatted merkledag node: unmarshal failed. proto: illegal wireType 6
                    continue;
                }
            };

            for (link_name, next_cid) in ipld_links(ipld) {
                if cid.codec() == CidCodec::DagProtobuf {
                    log::trace!("dag-pb link: {} -> {}", cid, next_cid);
                }
                work.push_back((depth + 1, next_cid, cid.clone(), link_name));
            }

            yield Ok((source, cid, link_name));
        }
    }
}

use libipld::{block::decode_ipld, Ipld};

fn ipld_links(ipld: Ipld) -> impl Iterator<Item = (Option<String>, Cid)> + Send + 'static {
    // a wrapping iterator without there being a libipld_base::IpldIntoIter might not be doable
    // with safe code
    ipld.iter()
        .filter_map(|val| match val {
            Ipld::Link(cid) => Some(cid),
            _ => None,
        })
        .cloned()
        .map(|cid| (None, cid))
        .collect::<Vec<(Option<String>, Cid)>>()
        .into_iter()
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
    use futures::stream::{TryStreamExt, StreamExt};
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

    let all_edges: Vec<_> = refs_paths(ipfs, vec![IpfsPath::try_from(root).unwrap()], None, false)
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
#[ignore]
async fn all_unique_refs_from_root() {
    use futures::stream::{StreamExt, TryStreamExt};
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

    let all_edges: Vec<_> = refs_paths(ipfs, vec![IpfsPath::try_from(root).unwrap()], None, false)
        .await
        .unwrap()
        .map_ok(|(source, dest, _)| (source.to_string(), dest.to_string()))
        .try_collect()
        .await
        .unwrap();

    // if this test would have only the <dst> it might work?

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
    use futures::stream::{StreamExt, TryStreamExt};
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
