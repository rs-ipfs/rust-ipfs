#![warn(rust_2018_idioms, missing_docs)]
//! ipfs-unixfs

use std::borrow::Cow;
use std::fmt;

/// UnixFS file support.
pub mod file;

/// UnixFS directory support.
pub mod dir;
mod pb;

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
        write!(fmt, "failed to convert link #{} ({:?}) to Cid: {}", self.nth, self.name, self.source)
    }
}

impl std::error::Error for InvalidCidInLink {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
