use cid::Cid;
use std::convert::TryFrom;
use std::fmt;
use std::io::{Error as IoError, Read};
use std::path::{Path, PathBuf};

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
                configuration flatfs blockstore and write listing to stdout."
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
        Ok(()) => {}
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
    use ipfs_unixfs::walk::{ContinuedWalk, Walker};

    let mut buf = Vec::new();
    let mut cache = None;
    let mut walker = Walker::new(start.to_owned(), String::new());

    while walker.should_continue() {
        buf.clear();

        // Note: if you bind the pending or the "prefetchable", it must be dropped before the next
        // call to continue_walk.
        let (next, _) = walker.pending_links();
        blocks.as_file(&next.to_bytes())?.read_to_end(&mut buf)?;

        match walker.next(&buf, &mut cache)? {
            ContinuedWalk::Bucket(..) => {
                // Continuation of a HAMT shard directory that is usually ignored
            }
            ContinuedWalk::File(segment, _, path, metadata, size) => {
                if segment.is_first() {
                    // this is set on the root block, no actual bytes are present for multiblock
                    // files
                }
                if segment.is_last() {
                    let mode = metadata.mode().unwrap_or(0o0644) & 0o7777;
                    let (seconds, _) = metadata.mtime().unwrap_or((0, 0));
                    println!("f {:o} {:>12} {:>16} {:?}", mode, seconds, size, path);
                }
            }
            ContinuedWalk::Directory(_, path, metadata)
            | ContinuedWalk::RootDirectory(_, path, metadata) => {
                let mode = metadata.mode().unwrap_or(0o0755) & 0o7777;
                let (seconds, _) = metadata.mtime().unwrap_or((0, 0));
                println!("d {:o} {:>12} {:>16} {:?}", mode, seconds, "-", path);
            }
            ContinuedWalk::Symlink(bytes, _, path, metadata) => {
                let target = Path::new(std::str::from_utf8(bytes).unwrap());
                let mode = metadata.mode().unwrap_or(0o0755) & 0o7777;
                let (seconds, _) = metadata.mtime().unwrap_or((0, 0));
                println!(
                    "s {:o} {:>12} {:>16} {:?} -> {:?}",
                    mode, seconds, "-", path, target
                );
            }
        };
    }

    Ok(())
}

enum Error {
    OpeningFailed(IoError),
    Other(IoError),
    Walk(ipfs_unixfs::walk::Error),
}

impl From<IoError> for Error {
    fn from(e: IoError) -> Error {
        Error::Other(e)
    }
}

impl From<ipfs_unixfs::walk::Error> for Error {
    fn from(e: ipfs_unixfs::walk::Error) -> Error {
        Error::Walk(e)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Error::*;
        match self {
            OpeningFailed(e) => write!(fmt, "Failed to open file: {}", e),
            Other(e) => write!(fmt, "A file-related IO error: {}", e),
            Walk(e) => write!(fmt, "Walk failed, please report this as a bug: {}", e),
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
