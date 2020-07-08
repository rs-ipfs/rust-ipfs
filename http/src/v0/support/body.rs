use crate::v0::support::StringError;
use bytes::Buf;
use futures::stream::{Stream, TryStreamExt};
use mpart_async::server::MultipartStream;
use warp::Rejection;

pub async fn try_only_named_multipart<'a>(
    allowed_names: &'a [impl AsRef<str> + 'a],
    size_limit: usize,
    boundary: String,
    st: impl Stream<Item = Result<impl Buf, warp::Error>> + Unpin + 'a,
) -> Result<Vec<u8>, Rejection> {
    use bytes::Bytes;
    let mut stream =
        MultipartStream::new(Bytes::from(boundary), st.map_ok(|mut buf| buf.to_bytes()));

    // store the first good field here; optimally this would just be an Option but couldn't figure
    // out a way to handle the "field matched", "field not matched" cases while supporting empty
    // fields.
    let mut buffer = Vec::new();
    let mut matched = false;

    while let Some(mut field) = stream.try_next().await.map_err(StringError::from)? {
        // [ipfs http api] says we should expect a "data" but examples use "file" as the
        // form field name. newer conformance tests also use former, older latter.
        //
        // [ipfs http api]: https://docs.ipfs.io/reference/http/api/#api-v0-block-put

        let name = field
            .name()
            .map_err(|_| StringError::from("invalid field name"))?;

        let mut target = if allowed_names.iter().any(|s| s.as_ref() == name) {
            Some(&mut buffer)
        } else {
            None
        };

        if matched {
            // per spec: only one block should be uploaded at once
            return Err(StringError::from("multiple blocks (expecting at most one)").into());
        }

        matched = target.is_some();

        loop {
            let next = field.try_next().await.map_err(|e| {
                StringError::from(format!("IO error while reading field bytes: {}", e))
            })?;

            match (next, target.as_mut()) {
                (Some(bytes), Some(target)) => {
                    if target.len() + bytes.len() > size_limit {
                        return Err(StringError::from("block is too large").into());
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
                        return Err(StringError::from("internal error: zero read").into());
                    }
                }
                (None, _) => break,
            }
        }
    }

    if !matched {
        return Err(StringError::from("missing field: \"data\" (or \"file\")").into());
    }

    Ok(buffer)
}
