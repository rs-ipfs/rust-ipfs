//! UnixFS symlink support. UnixFS symlinks are UnixFS messages similar to single block files, but
//! the link name or target path is encoded in the UnixFS::Data field. This means that the target
//! path could be in any encoding, however it is always treated as an utf8 Unix path. Could be that
//! this is wrong.

use crate::pb::{FlatUnixFs, UnixFs, UnixFsType};
use quick_protobuf::{MessageWrite, Writer};
use std::borrow::Cow;

/// Appends a dag-pb block for for a symlink to the given target_path. It is expected that the
/// target_path is valid relative unix path but targets validity cannot really be judged.
pub fn serialize_symlink_block(target_path: &str, block_buffer: &mut Vec<u8>) {
    // should this fail or not? protobuf encoding cannot fail here, however we might create a too
    // large block but what's the limit?
    //
    // why not return a (Cid, Vec<u8>) like usually with cidv0? well...

    let node = FlatUnixFs {
        links: Vec::new(),
        data: UnixFs {
            Type: UnixFsType::Symlink,
            Data: Some(Cow::Borrowed(target_path.as_bytes())),
            ..Default::default()
        },
    };

    let mut writer = Writer::new(block_buffer);
    node.write_message(&mut writer).expect("unexpected failure");
}
