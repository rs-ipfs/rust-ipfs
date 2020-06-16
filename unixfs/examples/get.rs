use cid::Cid;
use ipfs_unixfs::Metadata;
use std::borrow::Cow;
use std::convert::TryFrom;
use std::fmt;
use std::io;
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
                configuration flatfs blockstore and write tar to stdout."
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
    use std::io::{stdout, Write};

    let stdout = stdout();
    let mut stdout = stdout.lock();

    // The blockstore-specific way of reading the block. Here we assume go-ipfs 0.5 default flatfs
    // configuration, which puts the files at sharded directories and names the blocks as base32
    // upper and a suffix of "data".
    //
    // For the ipfs-unixfs it is important that the raw block data lives long enough that the
    // possible content gets to be processed, at minimum one step of the walk as shown in this
    // example.
    let mut buf = Vec::new();
    blocks.as_file(&start.to_bytes())?.read_to_end(&mut buf)?;

    let mut cache = None;

    let mut visit = match Walker::start(&buf, "", &mut cache)? {
        ContinuedWalk::Directory(item) => item.into_inner(),
        x => todo!(
            "Only root level directories are supported in this exporter, not: {:?}",
            x
        ),
    };

    let mut header = tar::Header::new_gnu();
    header.set_mtime(0);
    header.set_uid(0);
    header.set_gid(0);

    let mut long_filename_header = tar::Header::new_gnu();
    long_filename_header.set_mode(0o644);

    {
        let name = b"././@LongLink";
        let gnu_header = long_filename_header.as_gnu_mut().unwrap();
        // since we are reusing the header, zero out all of the bytes
        let written = name
            .iter()
            .copied()
            .chain(std::iter::repeat(0))
            .enumerate()
            .take(gnu_header.name.len());
        // FIXME: there must be a better way to do this
        for (i, b) in written {
            gnu_header.name[i] = b;
        }
    }

    long_filename_header.set_mtime(0);
    long_filename_header.set_uid(0);
    long_filename_header.set_gid(0);

    let zeroes = [0; 512];

    while let Some(walker) = visit {
        buf.clear();

        // Note: if you bind the pending or the "prefetchable", it must be dropped before the next
        // call to continue_walk.
        let (next, _) = walker.pending_links();
        blocks.as_file(&next.to_bytes())?.read_to_end(&mut buf)?;
        visit = match walker.continue_walk(&buf, &mut cache)? {
            ContinuedWalk::File(segment, item) => {
                let total_size = item.as_entry().total_file_size().unwrap();

                if segment.is_first() {
                    // first write out the headers

                    let path = item.as_entry().path();
                    if let Err(e) = header.set_path(path) {
                        let data =
                            prepare_long_header(&mut header, &mut long_filename_header, path, e);

                        stdout.write_all(long_filename_header.as_bytes()).unwrap();
                        stdout.write_all(data).unwrap();
                        stdout.write_all(&[0]).unwrap();

                        let payload_bytes = data.len() + 1;

                        let padding = 512 - (payload_bytes % 512);
                        if padding < 512 {
                            stdout.write_all(&zeroes[..padding]).unwrap();
                        }
                    }

                    let metadata = item
                        .as_entry()
                        .metadata()
                        .expect("files must have metadata");

                    apply_file(&mut header, metadata, total_size);

                    header.set_cksum();

                    stdout.write_all(header.as_bytes()).unwrap();
                }

                stdout.write_all(segment.as_ref()).unwrap();

                if segment.is_last() {
                    // write out the last data, then write padding
                    let padding = 512 - (total_size % 512);
                    if padding < 512 {
                        stdout.write_all(&zeroes[..padding as usize]).unwrap();
                    }
                }

                item.into_inner()
            }
            ContinuedWalk::Directory(item) => {
                // sibling buckets do not have metadata
                if let Some(metadata) = item.as_entry().metadata() {
                    // create header and empty entry ... we also need to remember the last directory
                    // path not to create duplicate entries
                    let path = item.as_entry().path();

                    // skip empty paths; this might need to be the CID though
                    if path != Path::new("") {
                        if let Err(e) = header.set_path(path) {
                            let data = prepare_long_header(
                                &mut header,
                                &mut long_filename_header,
                                path,
                                e,
                            );

                            stdout.write_all(long_filename_header.as_bytes()).unwrap();
                            stdout.write_all(data).unwrap();
                            stdout.write_all(&[0]).unwrap();

                            let payload_bytes = data.len() + 1;

                            let padding = 512 - (payload_bytes % 512);
                            stdout.write_all(&zeroes[..padding]).unwrap();
                        }

                        // dirs are appended: header, no additional padding

                        header
                            .set_mode(metadata.mode().map(|mode| mode & 0o7777).unwrap_or(0o0755));

                        header.set_mtime(
                            metadata
                                .mtime()
                                .and_then(|(seconds, _)| {
                                    if seconds >= 0 {
                                        Some(seconds as u64)
                                    } else {
                                        None
                                    }
                                })
                                .unwrap_or(0),
                        );

                        header.set_size(0);
                        header.set_entry_type(tar::EntryType::Directory);
                        header.set_cksum();

                        stdout.write_all(header.as_bytes()).unwrap();
                    }
                }

                item.into_inner()
            }
            ContinuedWalk::Symlink(bytes, item) => {
                // create symlink header and entry. I am guessing the serialization format is right
                // away tar compatible.

                // FIXME: we could get away from the unwraps by refining the continuedwalk type
                let metadata = item
                    .as_entry()
                    .metadata()
                    .expect("symlink must have metadata");

                let path = item.as_entry().path();
                if let Err(e) = header.set_path(path) {
                    let data = prepare_long_header(&mut header, &mut long_filename_header, path, e);

                    stdout.write_all(long_filename_header.as_bytes()).unwrap();
                    stdout.write_all(data).unwrap();
                    stdout.write_all(&[0]).unwrap();

                    let payload_bytes = data.len() + 1;

                    let padding = 512 - (payload_bytes % 512);
                    if padding < 512 {
                        stdout.write_all(&zeroes[..padding]).unwrap();
                    }
                }

                let target = Path::new(std::str::from_utf8(bytes).unwrap());
                if let Err(e) = header.set_link_name(&target) {
                    let data = path2bytes(target);
                    if data.len() < header.as_old().linkname.len() {
                        // this might be an /ipfs/QmFoo which we should error and not allow
                        panic!("invalid link target: {:?} ({})", target, e)
                    }

                    long_filename_header.set_size(data.len() as u64 + 1);
                    long_filename_header.set_entry_type(tar::EntryType::new(b'K'));
                    long_filename_header.set_cksum();

                    stdout.write_all(long_filename_header.as_bytes()).unwrap();
                    stdout.write_all(data).unwrap();
                    stdout.write_all(&[0]).unwrap();

                    let payload_bytes = data.len() + 1;

                    let padding = 512 - (payload_bytes % 512);
                    if padding < 512 {
                        stdout.write_all(&zeroes[..padding]).unwrap();
                    }
                }

                header.set_mode(metadata.mode().map(|mode| mode & 0o7777).unwrap_or(0o0644));

                header.set_mtime(
                    metadata
                        .mtime()
                        .and_then(|(seconds, _)| {
                            if seconds >= 0 {
                                Some(seconds as u64)
                            } else {
                                None
                            }
                        })
                        .unwrap_or(0),
                );

                header.set_size(0);
                header.set_entry_type(tar::EntryType::Symlink);
                header.set_cksum();

                stdout.write_all(header.as_bytes()).unwrap();

                // no padding for the remaining

                item.into_inner()
            }
        };
    }

    Ok(())
}

fn apply_file(header: &mut tar::Header, metadata: &Metadata, total_size: u64) {
    header.set_mode(metadata.mode().map(|mode| mode & 0o7777).unwrap_or(0o0644));

    header.set_mtime(
        metadata
            .mtime()
            .and_then(|(seconds, _)| {
                if seconds >= 0 {
                    Some(seconds as u64)
                } else {
                    None
                }
            })
            .unwrap_or(0),
    );

    header.set_size(total_size);
    header.set_entry_type(tar::EntryType::Regular);
}

/// Returns the raw bytes we need to write as a new entry into the tar
fn prepare_long_header<'a>(
    header: &mut tar::Header,
    long_filename_header: &mut tar::Header,
    path: &'a Path,
    error: std::io::Error,
) -> &'a [u8] {
    #[cfg(unix)]
    /// On unix this operation can never fail.
    pub fn bytes2path(bytes: Cow<[u8]>) -> io::Result<Cow<Path>> {
        use std::ffi::{OsStr, OsString};
        use std::os::unix::prelude::*;

        Ok(match bytes {
            Cow::Borrowed(bytes) => Cow::Borrowed(Path::new(OsStr::from_bytes(bytes))),
            Cow::Owned(bytes) => Cow::Owned(PathBuf::from(OsString::from_vec(bytes))),
        })
    }

    #[cfg(windows)]
    /// On windows we cannot accept non-Unicode bytes because it
    /// is impossible to convert it to UTF-16.
    pub fn bytes2path(bytes: Cow<[u8]>) -> io::Result<Cow<Path>> {
        use std::ffi::{OsStr, OsString};
        use std::os::windows::prelude::*;

        return match bytes {
            Cow::Borrowed(bytes) => {
                let s = str::from_utf8(bytes).map_err(|_| not_unicode(bytes))?;
                Ok(Cow::Borrowed(Path::new(s)))
            }
            Cow::Owned(bytes) => {
                let s = String::from_utf8(bytes).map_err(|uerr| not_unicode(&uerr.into_bytes()))?;
                Ok(Cow::Owned(PathBuf::from(s)))
            }
        };

        fn not_unicode(v: &[u8]) -> io::Error {
            other(&format!(
                "only Unicode paths are supported on Windows: {}",
                String::from_utf8_lossy(v)
            ))
        }
    }

    // we **only** have utf8 paths as protobuf has already parsed this file
    // name and all of the previous as utf8.

    let data = path2bytes(path);

    let max = header.as_old().name.len();

    if data.len() < max {
        panic!("path cannot be put into tar: {:?} ({})", path, error);
    }

    // the plus one is documented as compliance to GNU tar, probably the null byte
    // termination?
    long_filename_header.set_size(data.len() as u64 + 1);
    long_filename_header.set_entry_type(tar::EntryType::new(b'L'));
    long_filename_header.set_cksum();

    // we still need to figure out the truncated path we put into the header
    let path = bytes2path(Cow::Borrowed(&data[..max]))
        .expect("quite certain we have no non-utf8 paths here");
    header
        .set_path(&path)
        .expect("we already made sure the path is of fitting length");

    data
}

#[cfg(unix)]
fn path2bytes(p: &Path) -> &[u8] {
    use std::os::unix::prelude::*;
    p.as_os_str().as_bytes()
}

#[cfg(windows)]
fn path2bytes(p: &Path) -> &[u8] {
    use std::os::windows::prelude::*;
    p.as_os_str()
        .to_str()
        .expect("we should only have unicode compatible bytes even on windows")
        .as_bytes()
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
            OpeningFailed(e) => write!(fmt, "File opening failed: {}", e),
            Other(e) => write!(fmt, "Other file related io error: {}", e),
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
