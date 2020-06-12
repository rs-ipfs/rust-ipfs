use cid::Cid;
use ipfs_unixfs::file::{FileReadFailed};
use std::convert::TryFrom;
use std::fmt;
use std::io::{Error as IoError, Read};
use std::path::PathBuf;

fn main() {
    let cid = match std::env::args().nth(1).map(Cid::try_from) {
        Some(Ok(cid)) => cid,
        Some(Err(e)) => {
            eprintln!("Invalid cid given as argument: {}", e);
            std::process::exit(1);
        }
        None => {
            eprintln!("USAGE: {} CID\n", std::env::args().next().unwrap());
            eprintln!(
                "Will walk the unixfs file pointed out by the CID from default go-ipfs 0.5 \
                configuration flatfs blockstore and write all content to stdout."
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

    match walk(blockstore, &cid) {
        Ok(()) => {
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

fn walk(blocks: ShardedBlockStore, start: &Cid) -> Result<(), Error> {
    use ipfs_unixfs::dir::walk::{Walker, Walk, ContinuedWalk};

    // The blockstore specific way of reading the block. Here we assume go-ipfs 0.5 default flatfs
    // configuration, which puts the files at sharded directories and names the blocks as base32
    // upper and a suffix of "data".
    //
    // For the ipfs-unixfs it is important that the raw block data lives long enough that the
    // possible content gets to be processed, at minimum one step of the walk as shown in this
    // example.
    let mut buf = Vec::new();
    blocks.as_file(&start.to_bytes())?.read_to_end(&mut buf)?;

    let mut cache = None;

    let mut visit = match Walker::start(&buf, &mut cache).unwrap() {
        Walk::Walker(walker) => {
            println!("d {:?}", walker.as_entry().path());
            Some(walker)
        },
        x => unreachable!("{:?}", x),
    };

    while let Some(walker) = visit {
        buf.clear();
        let (next, _) = walker.pending_links();
        blocks.as_file(&next.to_bytes())?.read_to_end(&mut buf)?;
        visit = match walker.continue_walk(&buf, &mut cache).unwrap() {
            ContinuedWalk::File(bytes, item) => {
                // this will be called for every file related block in file order; the `bytes` can
                // be empty.
                println!("f {:?} {} bytes", item.as_entry().path(), bytes.as_ref().len());
                item.into_inner()
            },
            ContinuedWalk::Directory(item) => {
                println!("d {:?}", item.as_entry().path());
                item.into_inner()
            }
            ContinuedWalk::Symlink(_, item) => {
                // FIXME: the symlinks seem to be prefixed with some valid unicode codepoints
                // but need further deserialization
                println!("s {:?} -> ???", item.as_entry().path());
                item.into_inner()
            }
        };
    }

    Ok(())
}

enum Error {
    OpeningFailed(IoError),
    Other(IoError),
    Traversal(ipfs_unixfs::file::FileReadFailed),
}

impl From<IoError> for Error {
    fn from(e: IoError) -> Error {
        Error::Other(e)
    }
}

impl From<FileReadFailed> for Error {
    fn from(e: FileReadFailed) -> Error {
        Error::Traversal(e)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Error::*;
        match self {
            OpeningFailed(e) => write!(fmt, "File opening failed: {}", e),
            Other(e) => write!(fmt, "Other file related io error: {}", e),
            Traversal(e) => write!(fmt, "Traversal failed, please report this as a bug: {}", e),
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
