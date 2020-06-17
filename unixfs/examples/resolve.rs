use cid::Cid;
use ipfs_unixfs::dir::{resolve, LookupError, ResolveError};
use std::convert::TryFrom;
use std::fmt;
use std::io::{Error as IoError, Read};
use std::path::PathBuf;

fn main() {
    let path = match std::env::args()
        .nth(1)
        .map(|s| IpfsPath::try_from(s.as_str()))
    {
        Some(Ok(path)) => path,
        Some(Err(e)) => {
            eprintln!("Invalid path given as argument: {}", e);
            std::process::exit(1);
        }
        None => {
            eprintln!("USAGE: {} IPFSPATH\n", std::env::args().next().unwrap());
            eprintln!(
                "Will resolve the given IPFSPATH to a CID through any UnixFS \
                directories or HAMT shards from default go-ipfs 0.5 \
                configuration flatfs blockstore and write the final CID into \
                stdout"
            );
            std::process::exit(0);
        }
    };

    let ipfs_path = match std::env::var("IPFS_PATH") {
        Ok(s) => s,
        Err(e) => {
            eprintln!("IPFS_PATH is not set or could not be read: {}", e);
            std::process::exit(1);
        }
    };

    let mut blocks = PathBuf::from(ipfs_path);
    blocks.push("blocks");

    let blockstore = ShardedBlockStore { root: blocks };

    match walk(blockstore, path) {
        Ok(Some(cid)) => {
            println!("{}", cid);
        }
        Ok(None) => {
            eprintln!("not found");
        }
        Err(Error::OpeningFailed(e)) => {
            eprintln!("{}\n", e);
            eprintln!("This is likely caused by either:");
            eprintln!(" - ipfs does not have the block");
            eprintln!(" - ipfs is configured to use non-flatfs storage");
            eprintln!(" - ipfs is configured to use flatfs with different sharding");
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("Failed to walk the merkle tree: {}", e);
            std::process::exit(1);
        }
    }
}

#[derive(Debug)]
pub enum PathError {
    InvalidCid(cid::Error),
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

/// Ipfs path following https://github.com/ipfs/go-path/
#[derive(Debug)]
pub struct IpfsPath {
    /// Option to support moving the cid
    root: Option<Cid>,
    path: std::vec::IntoIter<String>,
}

impl From<Cid> for IpfsPath {
    /// Creates a new `IpfsPath` from just the `Cid`, which is the same as parsing from a string
    /// representation of a `Cid`, but cannot fail.
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
                if split.next().is_some() {
                    // x/ipfs/_
                    return Err(PathError::InvalidPath);
                }

                ("", x)
            }
            None => return Err(PathError::InvalidPath),
        };

        let mut split = path.splitn(2, '/');
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
}

fn walk(blocks: ShardedBlockStore, mut path: IpfsPath) -> Result<Option<Cid>, Error> {
    use ipfs_unixfs::dir::MaybeResolved::*;

    let mut buf = Vec::new();
    let mut root = path.take_root().unwrap();

    let mut cache = None;

    for segment in path.path {
        println!("cache {:?}", cache);
        buf.clear();
        eprintln!("reading {} to resolve {:?}", root, segment);
        blocks.as_file(&root.to_bytes())?.read_to_end(&mut buf)?;

        let mut walker = match resolve(&buf, segment.as_str(), &mut cache)? {
            Found(cid) => {
                // either root was a Directory or we got lucky with a HAMT directory.
                // With HAMTDirectories the top level can contain a direct link to the target, but
                // it's more likely it will be found under some bucket, which would be the third
                // case in this match.
                println!("got lucky: found {} for {:?}", cid, segment);
                println!("cache {:?}", cache);
                root = cid;
                continue;
            }

            NotFound => return Ok(None),

            // when we stumble upon a HAMT shard, we'll need to look up other blocks in order to
            // find the final link. The current implementation cannot search for the directory by
            // hashing the name and looking it up, but the implementation can be changed underneath
            // without changes to the API.
            //
            // HAMTDirecotories or HAMT shards are multi-block directories where the entires are
            // bucketed per their hash value.
            NeedToLoadMore(walker) => walker,
        };

        eprintln!("walking {} on {:?}", root, segment);

        let mut other_blocks = 1;

        loop {
            let (first, _) = walker.pending_links();
            buf.clear();
            eprintln!("  -> reading {} while searching for {:?}", first, segment);
            blocks.as_file(&first.to_bytes())?.read_to_end(&mut buf)?;

            match walker.continue_walk(&buf, &mut cache)? {
                NotFound => {
                    println!("cache {:?}", cache);
                    return Ok(None);
                }
                Found(cid) => {
                    eprintln!(
                        "     resolved {} from {} after {} blocks to {}",
                        segment, root, other_blocks, cid
                    );
                    root = cid;
                    break;
                }
                NeedToLoadMore(next) => walker = next,
            }
            other_blocks += 1;
        }
    }

    println!("cache {:?}", cache);
    Ok(Some(root))
}

enum Error {
    OpeningFailed(IoError),
    Other(IoError),
    Traversal(ResolveError),
}

impl From<IoError> for Error {
    fn from(e: IoError) -> Error {
        Error::Other(e)
    }
}

impl From<ResolveError> for Error {
    fn from(e: ResolveError) -> Error {
        Error::Traversal(e)
    }
}

impl From<LookupError> for Error {
    fn from(e: LookupError) -> Error {
        Error::Traversal(e.into())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Error::*;
        match self {
            OpeningFailed(e) => write!(fmt, "File opening failed: {}", e),
            Other(e) => write!(fmt, "Other file related io error: {}", e),
            Traversal(e) => write!(fmt, "Walking failed, please report this as a bug: {:?}", e),
        }
    }
}

struct ShardedBlockStore {
    root: PathBuf,
}

impl ShardedBlockStore {
    fn as_path(&self, key: &[u8]) -> PathBuf {
        // assume that we have a block store with second-to-last/2 sharding
        // files in Base32Upper

        let encoded = multibase::Base::Base32Upper.encode(key);
        let len = encoded.len();

        // this is safe because base32 is ascii
        let dir = &encoded[(len - 3)..(len - 1)];
        assert_eq!(dir.len(), 2);

        let mut path = self.root.clone();
        path.push(dir);
        path.push(encoded);
        path.set_extension("data");
        path
    }

    fn as_file(&self, key: &[u8]) -> Result<std::fs::File, Error> {
        let path = self.as_path(key);

        std::fs::OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(Error::OpeningFailed)
    }
}
