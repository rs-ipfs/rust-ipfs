use crate::v0::refs::{walk_path, IpfsPath};
use crate::v0::support::unshared::Unshared;
use crate::v0::support::{with_ipfs, StreamResponse, StringError};
use async_stream::try_stream;
use bytes::Bytes;
use futures::stream::TryStream;
use ipfs::unixfs::ll::walk::{self, ContinuedWalk, Walker};
use ipfs::unixfs::{ll::file::FileReadFailed, TraversalFailed};
use ipfs::Block;
use ipfs::{Ipfs, IpfsTypes};
use libipld::cid::{Cid, Codec};
use serde::Deserialize;
use std::convert::TryFrom;
use std::fmt;
use std::path::Path;
use warp::{path, query, Filter, Rejection, Reply};

mod tar_helper;
use tar_helper::TarHelper;

#[derive(Debug, Deserialize)]
pub struct CatArgs {
    // this could be an ipfs path
    arg: String,
    offset: Option<u64>,
    length: Option<u64>,
    // timeout: Option<?> // added in latest iterations
}

pub fn cat<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    path!("cat")
        .and(with_ipfs(ipfs))
        .and(query::<CatArgs>())
        .and_then(cat_inner)
}

async fn cat_inner<T: IpfsTypes>(ipfs: Ipfs<T>, args: CatArgs) -> Result<impl Reply, Rejection> {
    let mut path = IpfsPath::try_from(args.arg.as_str()).map_err(StringError::from)?;
    path.set_follow_dagpb_data(false);

    let range = match (args.offset, args.length) {
        (Some(start), Some(len)) => Some(start..(start + len)),
        (Some(_start), None) => todo!("need to abstract over the range"),
        (None, Some(len)) => Some(0..len),
        (None, None) => None,
    };

    // FIXME: this is here until we have IpfsPath back at ipfs

    let (cid, _, _) = walk_path(&ipfs, path).await.map_err(StringError::from)?;

    if cid.codec() != Codec::DagProtobuf {
        return Err(StringError::from("unknown node type").into());
    }

    // TODO: timeout
    let stream = match ipfs::unixfs::cat(ipfs, cid, range).await {
        Ok(stream) => stream,
        Err(TraversalFailed::Walking(_, FileReadFailed::UnexpectedType(ut)))
            if ut.is_directory() =>
        {
            return Err(StringError::from("this dag node is a directory").into())
        }
        Err(e) => return Err(StringError::from(e).into()),
    };

    Ok(StreamResponse(Unshared::new(stream)))
}

#[derive(Deserialize)]
struct GetArgs {
    // this could be an ipfs path again
    arg: String,
}

pub fn get<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    path!("get")
        .and(with_ipfs(ipfs))
        .and(query::<GetArgs>())
        .and_then(get_inner)
}

async fn get_inner<T: IpfsTypes>(ipfs: Ipfs<T>, args: GetArgs) -> Result<impl Reply, Rejection> {
    use futures::stream::TryStreamExt;

    let mut path = IpfsPath::try_from(args.arg.as_str()).map_err(StringError::from)?;
    path.set_follow_dagpb_data(false);

    // FIXME: this is here until we have IpfsPath back at ipfs
    let (cid, _, _) = walk_path(&ipfs, path).await.map_err(StringError::from)?;

    if cid.codec() != Codec::DagProtobuf {
        return Err(StringError::from("unknown node type").into());
    }

    Ok(StreamResponse(Unshared::new(walk(ipfs, cid).into_stream())))
}

fn walk<Types: IpfsTypes>(
    ipfs: Ipfs<Types>,
    root: Cid,
) -> impl TryStream<Ok = Bytes, Error = GetError> + 'static {
    let mut cache = None;
    let mut tar_helper = TarHelper::with_buffer_sizes(16 * 1024);

    let mut root = Some(root);
    let mut maybe_walker: Option<Walker> = None;

    try_stream! {
        loop {
            // this mangling with the root and maybe_walker looks like this mainly because
            //
            // a) I could not come up with a simpler solution, as we cannot refactor the big match
            // to a function given the non-uniform yield points
            //
            // b) adding more code here, like with an tri-state enum, goes quickly over the current
            // #[recursion_limit = "512"], which is required due to how async_stream needs to parse
            // this function (tt-muncher).
            //
            // the next will be the root on first round, and pending_links on the next rounds. root
            // is read with `as_ref` and later dropped by assigning `None` to it on every
            // iteration.
            let next = root.as_ref()
                .or_else(|| maybe_walker.as_ref().map(|w| w.pending_links().0));

            // we either have a cid reference, or we are done
            let next = match next {
                Some(cid_ref) => cid_ref,
                None => return,
            };

            let Block { data, .. } = ipfs.get_block(next).await?;

            let res = match maybe_walker {
                None => {
                    // the HTTP api uses the final Cid name as the root name in the generated tar
                    // archive; it will be copied to Walker internally so it can be temporary.
                    let root_name = next.to_string();
                    Walker::start(&data, &root_name, &mut cache)?
                },
                Some(walker) => walker.continue_walk(&data, &mut cache)?,
            };

            // make sure only first round uses the `root` cid.
            root = None;

            let next_walker = match res {
                ContinuedWalk::File(segment, item) => {
                    let total_size = item.as_entry()
                        .total_file_size()
                        .expect("files do have total_size");

                    if segment.is_first() {
                        let path = item.as_entry().path();
                        let metadata = item
                            .as_entry()
                            .metadata()
                            .expect("files must have metadata");

                        for mut bytes in tar_helper.apply_file(path, metadata, total_size)?.iter_mut() {
                            if let Some(bytes) = bytes.take() {
                                yield bytes;
                            }
                        }
                    }

                    // even if the largest of files can have 256 kB blocks and about the same
                    // amount of content, try to consume it in small parts not to grow the buffers
                    // too much.

                    let mut n = 0usize;
                    let slice = segment.as_ref();
                    let total = slice.len();

                    while n < total {
                        let next = tar_helper.buffer_file_contents(&slice[n..]);
                        n += next.len();
                        yield next;
                    }

                    if segment.is_last() {
                        if let Some(zeroes) = tar_helper.pad(total_size) {
                            yield zeroes;
                        }
                    }

                    item.into_inner()
                },
                ContinuedWalk::Directory(item) => {

                    // only first instances of directorys will have the metadata
                    if let Some(metadata) = item.as_entry().metadata() {
                        let path = item.as_entry().path();

                        // TODO: this is still wrong
                        assert_ne!(path, Path::new(""), "had metadata but name was empty");

                        for mut bytes in tar_helper.apply_directory(path, metadata)?.iter_mut() {
                            if let Some(bytes) = bytes.take() {
                                yield bytes;
                            }
                        }
                    }

                    item.into_inner()
                },
                ContinuedWalk::Symlink(bytes, item) => {
                    let path = item.as_entry().path();
                    let target = std::str::from_utf8(bytes).map_err(|_| GetError::NonUtf8Symlink)?;
                    let target = Path::new(target);
                    let metadata = item.as_entry().metadata().expect("symlink must have metadata");

                    for mut bytes in tar_helper.apply_symlink(path, target, metadata)?.iter_mut() {
                        if let Some(bytes) = bytes.take() {
                            yield bytes;
                        }
                    }

                    item.into_inner()
                },
            };

            maybe_walker = next_walker;
        }
    }
}

#[derive(Debug)]
enum GetError {
    NonUtf8Symlink,
    InvalidFileName(Vec<u8>),
    Walk(walk::Error),
    Loading(ipfs::Error),
}

impl From<ipfs::Error> for GetError {
    fn from(e: ipfs::Error) -> Self {
        GetError::Loading(e)
    }
}

impl From<walk::Error> for GetError {
    fn from(e: walk::Error) -> Self {
        GetError::Walk(e)
    }
}

impl fmt::Display for GetError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        use GetError::*;
        match self {
            NonUtf8Symlink => write!(fmt, "symlink target could not be converted to utf-8"),
            Walk(e) => write!(fmt, "{}", e),
            Loading(e) => write!(fmt, "loading failed: {}", e),
            InvalidFileName(x) => write!(fmt, "filename cannot be put inside tar: {:?}", x),
        }
    }
}

impl std::error::Error for GetError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            GetError::Walk(e) => Some(e),
            _ => None,
        }
    }
}
