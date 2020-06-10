#![warn(rust_2018_idioms, missing_docs)]
//! ipfs-unixfs

use std::borrow::Cow;
use std::fmt;

/// UnixFS file support.
pub mod file;

/// UnixFS directory support.
pub mod dir;

pub use dir::{resolve, LookupError, MaybeResolved, ResolveError};

mod pb;
use crate::pb::UnixFsType;

/// A link could not be transformed into a Cid.
#[derive(Debug)]
pub struct InvalidCidInLink {
    /// The index of this link, from zero
    pub nth: usize,
    /// Hash which could not be turned into a Cid
    pub hash: Cow<'static, [u8]>,
    /// Name of the link, most likely empty when this originates from a file, most likely non-empty
    /// for other kinds.
    pub name: Cow<'static, str>,
    /// Error from the attempted conversion
    pub source: cid::Error,
    /// This is to deny creating these outside of the crate
    hidden: (),
}

impl<'a> From<(usize, pb::PBLink<'a>, cid::Error)> for InvalidCidInLink {
    fn from((nth, link, source): (usize, pb::PBLink<'a>, cid::Error)) -> Self {
        let hash = match link.Hash {
            Some(Cow::Borrowed(x)) if !x.is_empty() => Cow::Owned(x.to_vec()),
            Some(Cow::Borrowed(_)) | None => Cow::Borrowed(&[][..]),
            Some(Cow::Owned(x)) => Cow::Owned(x),
        };

        let name = match link.Name {
            Some(Cow::Borrowed(x)) if !x.is_empty() => Cow::Owned(x.to_string()),
            Some(Cow::Borrowed(_)) | None => Cow::Borrowed(""),
            Some(Cow::Owned(x)) => Cow::Owned(x),
        };

        InvalidCidInLink {
            nth,
            hash,
            name,
            source,
            hidden: (),
        }
    }
}

impl fmt::Display for InvalidCidInLink {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            fmt,
            "failed to convert link #{} ({:?}) to Cid: {}",
            self.nth, self.name, self.source
        )
    }
}

impl std::error::Error for InvalidCidInLink {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// Wrapper around the unexpected UnixFs node type, allowing access to querying what is known about
/// the type.
#[derive(Debug)]
pub struct UnexpectedNodeType(i32);

impl From<UnixFsType> for UnexpectedNodeType {
    fn from(t: UnixFsType) -> UnexpectedNodeType {
        UnexpectedNodeType(t.into())
    }
}

impl UnexpectedNodeType {
    /// Returns true if the type represents some directory type
    pub fn is_directory(&self) -> bool {
        match UnixFsType::from(self.0) {
            UnixFsType::Directory | UnixFsType::HAMTShard => true,
            _ => false,
        }
    }
}
