///! dag-pb support operations. Placing this module inside unixfs module is a bit unfortunate but
///! follows from the inseparability of dag-pb and UnixFS.
use crate::pb::PBNode;
use alloc::borrow::Cow;
use core::convert::TryFrom;
use core::fmt;
use core::ops::Range;

/// Extracts the PBNode::Data field from the block as it appears on the block.
pub fn node_data(block: &[u8]) -> Result<Option<&[u8]>, quick_protobuf::Error> {
    let doc = PBNode::try_from(block)?;
    Ok(match doc.Data {
        Some(Cow::Borrowed(slice)) => Some(slice),
        Some(Cow::Owned(_)) => unreachable!("never converted to owned"),
        None => None,
    })
}

/// Creates a wrapper around the given block representation which does not consume the block
/// representation but allows accessing the dag-pb node Data.
pub fn wrap_node_data<T>(block: T) -> Result<NodeData<T>, quick_protobuf::Error>
where
    T: AsRef<[u8]>,
{
    let full = block.as_ref();
    let range = node_data(full)?
        .map(|data| subslice_to_range(full, data).expect("this has to be in range"));
    Ok(NodeData {
        inner: block,
        range,
    })
}

fn subslice_to_range<T>(full: &[T], sub: &[T]) -> Option<Range<usize>> {
    use std::mem;

    if full.len() < sub.len() {
        return None;
    }
    let max = full.len();
    let amt = sub.len();

    let full = full.as_ptr() as usize;
    let sub = sub.as_ptr() as usize;

    sub.checked_sub(full)
        .map(|diff| diff / mem::size_of::<T>())
        // if there are two slices of a continious chunk, [A|B] we need to make sure B will not be
        // calculated as subslice of A
        .and_then(|start| if start >= max { None } else { Some(start) })
        .map(|start| start..(start + amt))
}

/// The wrapper returned from [`wrap_node_data`], allows accessing dag-pb nodes Data.
#[derive(PartialEq)]
pub struct NodeData<T> {
    inner: T,
    range: Option<Range<usize>>,
}

impl<T: AsRef<[u8]>> fmt::Debug for NodeData<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            fmt,
            "NodeData<{}> {{ inner.len: {}, range: {:?} }}",
            std::any::type_name::<T>(),
            self.inner.as_ref().len(),
            self.range
        )
    }
}

impl<T: AsRef<[u8]>> NodeData<T> {
    /// Returns the dag-pb nodes Data field as slice
    pub fn node_data(&self) -> &[u8] {
        if let Some(range) = self.range.as_ref() {
            &self.inner.as_ref()[range.clone()]
        } else {
            &[][..]
        }
    }

    /// Returns access to the wrapped block representation
    pub fn get_ref(&self) -> &T {
        &self.inner
    }

    /// Consumes self and returns the block representation
    pub fn into_inner(self) -> T {
        self.inner
    }
}

impl<T: AsRef<[u8]>, B: AsRef<[u8]>> PartialEq<B> for NodeData<T> {
    fn eq(&self, other: &B) -> bool {
        self.node_data() == other.as_ref()
    }
}
