use bytes::Buf;
use futures::stream::{Stream, TryStreamExt};
use mpart_async::server::{MultipartError, MultipartStream};
use std::fmt;

#[derive(Debug)]
pub enum OnlyMultipartFailure {
    UnparseableFieldName,
    MultipleValues,
    TooLargeValue,
    NotFound,
    ZeroRead,
    IO(MultipartError),
}

impl fmt::Display for OnlyMultipartFailure {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        use OnlyMultipartFailure::*;
        match self {
            UnparseableFieldName => write!(fmt, "multipart field name could not be parsed"),
            MultipleValues => write!(fmt, "multiple values of the matching name, expected one"),
            TooLargeValue => write!(fmt, "value is too long"),
            NotFound => write!(fmt, "value not found"),
            ZeroRead => write!(fmt, "internal error: read zero"),
            IO(e) => write!(fmt, "parsing failed: {}", e),
        }
    }
}

impl std::error::Error for OnlyMultipartFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        use OnlyMultipartFailure::*;
        match self {
            IO(e) => Some(e),
            _ => None,
        }
    }
}

impl From<MultipartError> for OnlyMultipartFailure {
    fn from(e: MultipartError) -> Self {
        OnlyMultipartFailure::IO(e)
    }
}

pub async fn try_only_named_multipart<'a>(
    allowed_names: &'a [impl AsRef<str> + 'a],
    size_limit: usize,
    boundary: String,
    st: impl Stream<Item = Result<impl Buf, warp::Error>> + Unpin + 'a,
) -> Result<Vec<u8>, OnlyMultipartFailure> {
    use bytes::Bytes;
    let mut stream = MultipartStream::new(
        Bytes::from(boundary),
        st.map_ok(|mut buf| buf.copy_to_bytes(buf.remaining())),
    );

    // store the first good field here; optimally this would just be an Option but couldn't figure
    // out a way to handle the "field matched", "field not matched" cases while supporting empty
    // fields.
    let mut buffer = Vec::new();
    let mut matched = false;

    while let Some(mut field) = stream.try_next().await? {
        // [ipfs http api] says we should expect a "data" but examples use "file" as the
        // form field name. newer conformance tests also use former, older latter.
        //
        // [ipfs http api]: https://docs.ipfs.io/reference/http/api/#api-v0-block-put

        let name = field
            .name()
            .map_err(|_| OnlyMultipartFailure::UnparseableFieldName)?;

        let mut target = if allowed_names.iter().any(|s| s.as_ref() == name) {
            Some(&mut buffer)
        } else {
            None
        };

        if matched {
            // per spec: only one block should be uploaded at once
            return Err(OnlyMultipartFailure::MultipleValues);
        }

        matched = target.is_some();

        loop {
            let next = field.try_next().await?;

            match (next, target.as_mut()) {
                (Some(bytes), Some(target)) => {
                    if target.len() + bytes.len() > size_limit {
                        return Err(OnlyMultipartFailure::TooLargeValue);
                    } else if target.is_empty() {
                        target.reserve(size_limit);
                    }
                    target.extend_from_slice(bytes.as_ref());
                }
                (Some(bytes), None) => {
                    // noop: we must fully consume the part before moving on to next.
                    if bytes.is_empty() {
                        // this technically shouldn't be happening any more but erroring
                        // out instead of spinning wildly is much better.
                        return Err(OnlyMultipartFailure::ZeroRead);
                    }
                }
                (None, _) => break,
            }
        }
    }

    if !matched {
        return Err(OnlyMultipartFailure::NotFound);
    }

    Ok(buffer)
}
