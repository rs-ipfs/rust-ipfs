///! Tar helper is internal to `/get` implementation. It uses some private parts of the `tar-rs`
///! crate to provide a `BytesMut` writing implementation instead of one using `std::io` interfaces.
///!
///! Code was originally taken and modified from the dependency version of `tar-rs`. The most
///! important copied parts are related to the long file name and long link name support. Issue
///! will be opened on the `tar-rs` to discuss if these could be made public, leaving us only the
///! `Bytes` (copying) code.
use super::GetError;
use bytes::{buf::BufMut, Bytes, BytesMut};
use ipfs::unixfs::ll::Metadata;
use std::borrow::Cow;
use std::path::{Path, PathBuf};
use tar::{EntryType, Header};

/// Tar helper is internal to `get` implementation. It uses some private parts of the `tar-rs`
/// crate to append the headers and the contents to a pair of `bytes::Bytes` operated in a
/// round-robin fashion.
pub(super) struct TarHelper {
    bufsize: usize,
    bytes: BytesMut,
    header: Header,
    long_filename_header: Header,
    zeroes: Bytes,
}

impl TarHelper {
    pub(super) fn with_capacity(n: usize) -> Self {
        let bytes = BytesMut::with_capacity(n);

        // these are 512 a piece
        let header = Self::new_default_header();
        let long_filename_header = Self::new_long_filename_header();
        let mut zeroes = BytesMut::with_capacity(512);
        for _ in 0..(512 / 8) {
            zeroes.put_u64(0);
        }
        assert_eq!(zeroes.len(), 512);
        let zeroes = zeroes.freeze();

        Self {
            bufsize: n,
            bytes,
            header,
            long_filename_header,
            zeroes,
        }
    }

    fn new_default_header() -> tar::Header {
        let mut header = tar::Header::new_gnu();
        header.set_mtime(0);
        header.set_uid(0);
        header.set_gid(0);

        header
    }

    fn new_long_filename_header() -> tar::Header {
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
            // FIXME: could revert back to the slice copying code since we never change this
            for (i, b) in written {
                gnu_header.name[i] = b;
            }
        }

        long_filename_header.set_mtime(0);
        long_filename_header.set_uid(0);
        long_filename_header.set_gid(0);

        long_filename_header
    }

    pub(super) fn apply_file(
        &mut self,
        path: &Path,
        metadata: &Metadata,
        total_size: u64,
    ) -> Result<[Option<Bytes>; 3], GetError> {
        let mut ret: [Option<Bytes>; 3] = Default::default();

        if let Err(e) = self.header.set_path(path) {
            let data =
                prepare_long_header(&mut self.header, &mut self.long_filename_header, path, e)?;

            self.bytes.put_slice(self.long_filename_header.as_bytes());
            self.bytes.put_slice(data);
            self.bytes.put_u8(0);
            ret[0] = Some(self.bytes.split().freeze());

            ret[1] = self.pad(data.len() as u64 + 1);
        }

        self.header.set_size(total_size);
        self.header.set_entry_type(EntryType::Regular);
        Self::set_metadata(&mut self.header, metadata, 0o0644);
        self.header.set_cksum();

        self.bytes.put_slice(self.header.as_bytes());

        ret[2] = Some(self.bytes.split().freeze());
        Ok(ret)
    }

    pub(super) fn buffer_file_contents(&mut self, contents: &[u8]) -> Bytes {
        assert!(!contents.is_empty());
        let remaining = contents.len();
        let taken = self.bufsize.min(remaining);

        // was initially thinking to check the capacity but we are round robining the buffers to
        // get a lucky chance at either of them being empty at this point
        self.bytes.put_slice(&contents[..taken]);
        self.bytes.split().freeze()
    }

    pub(super) fn apply_directory(
        &mut self,
        path: &Path,
        metadata: &Metadata,
    ) -> Result<[Option<Bytes>; 3], GetError> {
        let mut ret: [Option<Bytes>; 3] = Default::default();

        if let Err(e) = self.header.set_path(path) {
            let data =
                prepare_long_header(&mut self.header, &mut self.long_filename_header, path, e)?;

            self.bytes.put_slice(self.long_filename_header.as_bytes());
            self.bytes.put_slice(data);
            self.bytes.put_u8(0);
            ret[0] = Some(self.bytes.split().freeze());
            ret[1] = self.pad(data.len() as u64 + 1);
        }

        self.header.set_size(0);
        self.header.set_entry_type(EntryType::Directory);
        Self::set_metadata(&mut self.header, metadata, 0o0755);

        self.header.set_cksum();
        self.bytes.put_slice(self.header.as_bytes());

        ret[2] = Some(self.bytes.split().freeze());

        Ok(ret)
    }

    pub(super) fn apply_symlink(
        &mut self,
        path: &Path,
        target: &Path,
        metadata: &Metadata,
    ) -> Result<[Option<Bytes>; 5], GetError> {
        let mut ret: [Option<Bytes>; 5] = Default::default();

        if let Err(e) = self.header.set_path(path) {
            let data =
                prepare_long_header(&mut self.header, &mut self.long_filename_header, path, e)?;

            self.bytes.put_slice(self.long_filename_header.as_bytes());
            self.bytes.put_slice(data);
            self.bytes.put_u8(0);
            ret[0] = Some(self.bytes.split().freeze());

            ret[1] = self.pad(data.len() as u64 + 1);
        }

        if self.header.set_link_name(target).is_err() {
            let data = path2bytes(target);

            if data.len() < self.header.as_old().linkname.len() {
                return Err(GetError::InvalidLinkName(data.to_vec()));
            }

            // this is another long header trick, but this time we have a different entry type and
            // similarly the long file name is written as a separate entry with its own headers.

            self.long_filename_header.set_size(data.len() as u64 + 1);
            self.long_filename_header
                .set_entry_type(tar::EntryType::new(b'K'));
            self.long_filename_header.set_cksum();

            self.bytes.put_slice(self.long_filename_header.as_bytes());
            self.bytes.put_slice(data);
            self.bytes.put_u8(0);
            ret[2] = Some(self.bytes.split().freeze());

            ret[3] = self.pad(data.len() as u64 + 1);
        }

        Self::set_metadata(&mut self.header, metadata, 0o0644);
        self.header.set_size(0);
        self.header.set_entry_type(tar::EntryType::Symlink);
        self.header.set_cksum();

        self.bytes.put_slice(self.header.as_bytes());
        ret[4] = Some(self.bytes.split().freeze());

        Ok(ret)
    }

    /// Content in tar is padded to 512 byte sectors which might be configurable as well.
    pub(super) fn pad(&self, total_size: u64) -> Option<Bytes> {
        let padding = 512 - (total_size % 512);
        if padding < 512 {
            Some(self.zeroes.slice(..padding as usize))
        } else {
            None
        }
    }

    fn set_metadata(header: &mut tar::Header, metadata: &Metadata, default_mode: u32) {
        header.set_mode(
            metadata
                .mode()
                .map(|mode| mode & 0o7777)
                .unwrap_or(default_mode),
        );

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
    }
}

/// Returns the raw bytes we need to write as a new entry into the tar.
fn prepare_long_header<'a>(
    header: &mut tar::Header,
    long_filename_header: &mut tar::Header,
    path: &'a Path,
    _error: std::io::Error,
) -> Result<&'a [u8], GetError> {
    #[cfg(unix)]
    /// On unix this operation can never fail.
    pub(super) fn bytes2path(bytes: Cow<[u8]>) -> std::io::Result<Cow<Path>> {
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
    pub(super) fn bytes2path(bytes: Cow<[u8]>) -> std::io::Result<Cow<Path>> {
        match bytes {
            Cow::Borrowed(bytes) => {
                let s = std::str::from_utf8(bytes).map_err(|_| not_unicode(bytes))?;
                Ok(Cow::Borrowed(Path::new(s)))
            }
            Cow::Owned(bytes) => {
                let s = String::from_utf8(bytes).map_err(|uerr| not_unicode(&uerr.into_bytes()))?;
                Ok(Cow::Owned(PathBuf::from(s)))
            }
        }
    }

    // Used with windows.
    #[allow(dead_code)]
    fn not_unicode(v: &[u8]) -> std::io::Error {
        use std::io::{Error, ErrorKind};

        Error::new(
            ErrorKind::Other,
            format!(
                "only Unicode paths are supported on Windows: {}",
                String::from_utf8_lossy(v)
            ),
        )
    }

    // we **only** have utf8 paths as protobuf has already parsed this file
    // name and all of the previous ones as utf8.

    let data = path2bytes(path);

    let max = header.as_old().name.len();

    if data.len() < max {
        return Err(GetError::InvalidFileName(data.to_vec()));
    }

    // the plus one is documented as compliance with GNU tar, probably the null byte
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

    Ok(data)
}

#[cfg(unix)]
fn path2bytes(p: &Path) -> &[u8] {
    use std::os::unix::prelude::*;
    p.as_os_str().as_bytes()
}

#[cfg(windows)]
fn path2bytes(p: &Path) -> &[u8] {
    p.as_os_str()
        .to_str()
        .expect("we should only have unicode compatible bytes even on windows")
        .as_bytes()
}
