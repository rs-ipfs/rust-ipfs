use crate::pb::FlatUnixFs;
use core::fmt;

/// Ensures the directory looks like something we actually support.
pub(crate) fn check_directory_supported(
    flat: FlatUnixFs<'_>,
) -> Result<FlatUnixFs<'_>, UnexpectedDirectoryProperties> {
    let data = flat.data.Data.as_deref();
    if flat.data.filesize.is_some()
        || !flat.data.blocksizes.is_empty()
        || flat.data.hashType.is_some()
        || flat.data.fanout.is_some()
        || !data.unwrap_or_default().is_empty()
    {
        let data = data.map(|s| s.to_vec());
        Err(UnexpectedDirectoryProperties {
            filesize: flat.data.filesize,
            blocksizes: flat.data.blocksizes,
            hash_type: flat.data.hashType,
            fanout: flat.data.fanout,
            data,
        })
    } else {
        Ok(flat)
    }
}

/// Error case for checking if we support this directory.
#[derive(Debug)]
pub struct UnexpectedDirectoryProperties {
    /// filesize is a property of Files
    filesize: Option<u64>,
    /// blocksizes is a property of Files
    blocksizes: Vec<u64>,
    /// hash_type is a property of HAMT Shards
    hash_type: Option<u64>,
    /// fanout is a property of HAMT shards
    fanout: Option<u64>,
    /// directories should have no Data
    data: Option<Vec<u8>>,
}

impl fmt::Display for UnexpectedDirectoryProperties {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            fmt,
            "filesize={:?}, {} blocksizes, hash_type={:?}, fanout={:?}, data=[",
            self.filesize,
            self.blocksizes.len(),
            self.hash_type,
            self.fanout,
        )?;

        for b in self.data.as_deref().unwrap_or_default() {
            write!(fmt, "{:02x}", b)?;
        }

        write!(fmt, "]")
    }
}
