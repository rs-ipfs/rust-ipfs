//! Persistent filesystem backed pin store. See [`FsDataStore`] for more information.
use super::{filestem_to_pin_cid, pin_path, FsDataStore};
use crate::error::Error;
use crate::repo::{PinKind, PinMode, PinModeRequirement, PinStore, References};
use async_trait::async_trait;
use cid::Cid;
use core::convert::TryFrom;
use futures::stream::TryStreamExt;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::Semaphore;
use tokio_stream::{empty, wrappers::ReadDirStream, StreamExt};
use tokio_util::either::Either;

// PinStore is a trait from ipfs::repo implemented on FsDataStore defined at ipfs::repo::fs or
// parent module.

#[async_trait]
impl PinStore for FsDataStore {
    async fn is_pinned(&self, cid: &Cid) -> Result<bool, Error> {
        let path = pin_path(self.path.clone(), cid);

        if read_direct_or_recursive(path).await?.is_some() {
            return Ok(true);
        }

        let st = self.list_pinfiles().await.try_filter_map(|(cid, mode)| {
            futures::future::ready(if mode == PinMode::Recursive {
                Ok(Some(cid))
            } else {
                Ok(None)
            })
        });

        futures::pin_mut!(st);

        while let Some(recursive) = StreamExt::try_next(&mut st).await? {
            // TODO: it might be much better to just deserialize the vec one by one and comparing while
            // going
            let (_, references) = read_recursively_pinned(self.path.clone(), recursive).await?;

            // if we always wrote down the cids in some order we might be able to binary search?
            if references.into_iter().any(move |x| x == *cid) {
                return Ok(true);
            }
        }

        Ok(false)
    }

    async fn insert_direct_pin(&self, target: &Cid) -> Result<(), Error> {
        let permit = Semaphore::acquire_owned(Arc::clone(&self.lock)).await;

        let mut path = pin_path(self.path.clone(), target);

        let span = tracing::Span::current();

        tokio::task::spawn_blocking(move || {
            // move the permit to the blocking thread to ensure we keep it as long as needed
            let _permit = permit;
            let _entered = span.enter();

            std::fs::create_dir_all(path.parent().expect("shard parent has to exist"))?;
            path.set_extension("recursive");
            if path.is_file() {
                return Err(anyhow::anyhow!("already pinned recursively"));
            }

            path.set_extension("direct");
            let f = std::fs::File::create(path)?;
            f.sync_all()?;
            Ok(())
        })
        .await??;

        Ok(())
    }

    async fn insert_recursive_pin(
        &self,
        target: &Cid,
        referenced: References<'_>,
    ) -> Result<(), Error> {
        let set = referenced
            .try_collect::<std::collections::BTreeSet<_>>()
            .await?;

        let permit = Semaphore::acquire_owned(Arc::clone(&self.lock)).await;

        let mut path = pin_path(self.path.clone(), target);

        let span = tracing::Span::current();

        tokio::task::spawn_blocking(move || {
            let _permit = permit; // again move to the threadpool thread
            let _entered = span.enter();

            std::fs::create_dir_all(path.parent().expect("shard parent has to exist"))?;
            let count = set.len();
            let cids = set.into_iter().map(|cid| cid.to_string());

            path.set_extension("recursive_temp");

            let file = std::fs::File::create(&path)?;

            match sync_write_recursive_pin(file, count, cids) {
                Ok(_) => {
                    let final_path = path.with_extension("recursive");
                    std::fs::rename(&path, final_path)?
                }
                Err(e) => {
                    let removed = std::fs::remove_file(&path);

                    match removed {
                        Ok(_) => debug!("cleaned up ok after botched recursive pin write"),
                        Err(e) => warn!("failed to cleanup temporary file: {}", e),
                    }

                    return Err(e);
                }
            }

            // if we got this far, we have now written and renamed the recursive_temp into place.
            // now we just need to remove the direct pin, if it exists

            path.set_extension("direct");

            match std::fs::remove_file(&path) {
                Ok(_) => { /* good */ }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => { /* good as well */ }
                Err(e) => {
                    warn!(
                        "failed to remove direct pin when adding recursive {:?}: {}",
                        path, e
                    );
                }
            }

            Ok::<_, Error>(())
        })
        .await??;

        Ok(())
    }

    async fn remove_direct_pin(&self, target: &Cid) -> Result<(), Error> {
        let permit = Semaphore::acquire_owned(Arc::clone(&self.lock)).await;

        let mut path = pin_path(self.path.clone(), target);

        let span = tracing::Span::current();

        tokio::task::spawn_blocking(move || {
            let _permit = permit; // move in to threadpool thread
            let _entered = span.enter();

            path.set_extension("recursive");

            if path.is_file() {
                return Err(anyhow::anyhow!("is pinned recursively"));
            }

            path.set_extension("direct");

            match std::fs::remove_file(&path) {
                Ok(_) => {
                    trace!("direct pin removed");
                    Ok(())
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    Err(anyhow::anyhow!("not pinned or pinned indirectly"))
                }
                Err(e) => Err(e.into()),
            }
        })
        .await??;

        Ok(())
    }

    async fn remove_recursive_pin(&self, target: &Cid, _: References<'_>) -> Result<(), Error> {
        let permit = Semaphore::acquire_owned(Arc::clone(&self.lock)).await;

        let mut path = pin_path(self.path.clone(), target);

        let span = tracing::Span::current();

        tokio::task::spawn_blocking(move || {
            let _permit = permit; // move into threadpool thread
            let _entered = span.enter();

            path.set_extension("direct");

            let mut any = false;

            match std::fs::remove_file(&path) {
                Ok(_) => {
                    trace!("direct pin removed");
                    any |= true;
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // nevermind, we are just trying to remove the direct as it should go, if it
                    // was left by mistake
                }
                // Error::new instead of e.into() to help out the type inference
                Err(e) => return Err(Error::new(e)),
            }

            path.set_extension("recursive");

            match std::fs::remove_file(&path) {
                Ok(_) => {
                    trace!("recursive pin removed");
                    any |= true;
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // we may have removed only the direct pin, but if we cleaned out a direct pin
                    // this would have been a success
                }
                Err(e) => return Err(e.into()),
            }

            if !any {
                Err(anyhow::anyhow!("not pinned or pinned indirectly"))
            } else {
                Ok(())
            }
        })
        .await??;

        Ok(())
    }

    async fn list(
        &self,
        requirement: Option<PinMode>,
    ) -> futures::stream::BoxStream<'static, Result<(Cid, PinMode), Error>> {
        // no locking, dirty reads are probably good enough until gc
        let cids = self.list_pinfiles().await;

        let path = self.path.clone();

        let requirement = PinModeRequirement::from(requirement);

        // depending on what was queried we must iterate through the results in the order of
        // recursive, direct and indirect.
        //
        // if only one kind is required, we must return only those, which may or may not be
        // easier than doing all of the work. this implementation follows:
        //
        // https://github.com/ipfs/go-ipfs/blob/2ae5c52f4f0f074864ea252e90e72e8d5999caba/core/coreapi/pin.go#L222
        let st = async_stream::try_stream! {

            // keep track of all returned not to give out duplicate cids
            let mut returned: HashSet<Cid> = HashSet::new();

            // the set of recursive will be interesting after all others
            let mut recursive: HashSet<Cid> = HashSet::new();
            let mut direct: HashSet<Cid> = HashSet::new();

            let collect_recursive_for_indirect = requirement.is_indirect_or_any();

            futures::pin_mut!(cids);

            while let Some((cid, mode)) = StreamExt::try_next(&mut cids).await? {

                let matches = requirement.matches(&mode);

                if mode == PinMode::Recursive {
                    if collect_recursive_for_indirect {
                        recursive.insert(cid.clone());
                    }
                    if matches && returned.insert(cid.clone()) {
                        // the recursive pins can always be returned right away since they have
                        // the highest priority in this listing or output
                        yield (cid, mode);
                    }
                } else if mode == PinMode::Direct && matches {
                    direct.insert(cid);
                }
            }

            trace!(unique = returned.len(), "completed listing recursive");

            // now that the recursive are done, next up in priority order are direct. the set
            // of directly pinned and recursively pinned should be disjoint, but probably there
            // are times when 100% accurate results are not possible... Nor needed.
            for cid in direct {
                if returned.insert(cid.clone()) {
                    yield (cid, PinMode::Direct)
                }
            }

            trace!(unique = returned.len(), "completed listing direct");

            if !collect_recursive_for_indirect {
                // we didn't collect the recursive to list the indirect so, done.
                return;
            }

            // the threadpool passing adds probably some messaging latency, maybe run small
            // amount in parallel?
            let mut recursive = futures::stream::iter(recursive.into_iter().map(Ok))
                .map_ok(move |cid| read_recursively_pinned(path.clone(), cid))
                .try_buffer_unordered(4);

            while let Some((_, next_batch)) = StreamExt::try_next(&mut recursive).await? {
                for indirect in next_batch {
                    if returned.insert(indirect.clone()) {
                        yield (indirect, PinMode::Indirect);
                    }
                }

                trace!(unique = returned.len(), "completed batch of indirect");
            }
        };

        Box::pin(st)
    }

    async fn query(
        &self,
        ids: Vec<Cid>,
        requirement: Option<PinMode>,
    ) -> Result<Vec<(Cid, PinKind<Cid>)>, Error> {
        // response vec gets written to whenever we find out what the pin is
        let mut response = Vec::with_capacity(ids.len());
        for _ in 0..ids.len() {
            response.push(None);
        }

        let mut remaining = HashMap::new();

        let (check_direct, searched_suffix, gather_indirect) = match requirement {
            Some(PinMode::Direct) => (true, Some(PinMode::Direct), false),
            Some(PinMode::Recursive) => (true, Some(PinMode::Recursive), false),
            Some(PinMode::Indirect) => (false, None, true),
            None => (true, None, true),
        };

        let searched_suffix = PinModeRequirement::from(searched_suffix);

        let (mut response, mut remaining) = if check_direct {
            // find the recursive and direct ones by just seeing if the files exist
            let base = self.path.clone();
            tokio::task::spawn_blocking(move || {
                for (i, cid) in ids.into_iter().enumerate() {
                    let mut path = pin_path(base.clone(), &cid);

                    if let Some(mode) = sync_read_direct_or_recursive(&mut path) {
                        if searched_suffix.matches(&mode) {
                            response[i] = Some((
                                cid,
                                match mode {
                                    PinMode::Direct => PinKind::Direct,
                                    // FIXME: eech that recursive count is now out of place
                                    PinMode::Recursive => PinKind::Recursive(0),
                                    // FIXME: this is also quite unfortunate, should make an enum
                                    // of two?
                                    _ => unreachable!(),
                                },
                            ));
                            continue;
                        }
                    }

                    if !gather_indirect {
                        // if we are only trying to find recursive or direct, we clearly have not
                        // found what we were looking for
                        return Err(anyhow::anyhow!("{} is not pinned", cid));
                    }

                    // use entry api to discard duplicate cids in input
                    remaining.entry(cid).or_insert(i);
                }

                Ok((response, remaining))
            })
            .await??
        } else {
            for (i, cid) in ids.into_iter().enumerate() {
                remaining.entry(cid).or_insert(i);
            }
            (response, remaining)
        };

        // now remaining must have all of the cids => first_index mappings which were not found to
        // be recursive or direct.

        if !remaining.is_empty() {
            assert!(gather_indirect);

            trace!(
                remaining = remaining.len(),
                "query trying to find remaining indirect pins"
            );

            let recursives = self
                .list_pinfiles()
                .await
                .try_filter_map(|(cid, mode)| {
                    futures::future::ready(if mode == PinMode::Recursive {
                        Ok(Some(cid))
                    } else {
                        Ok(None)
                    })
                })
                .map_ok(|cid| read_recursively_pinned(self.path.clone(), cid))
                .try_buffer_unordered(4);

            futures::pin_mut!(recursives);

            'out: while let Some((referring, references)) =
                StreamExt::try_next(&mut recursives).await?
            {
                // FIXME: maybe binary search?
                for cid in references {
                    if let Some(index) = remaining.remove(&cid) {
                        response[index] = Some((cid, PinKind::IndirectFrom(referring.clone())));

                        if remaining.is_empty() {
                            break 'out;
                        }
                    }
                }
            }
        }

        if let Some((cid, _)) = remaining.into_iter().next() {
            // the error can be for any of these
            return Err(anyhow::anyhow!("{} is not pinned", cid));
        }

        // the input can of course contain duplicate cids so handle them by just giving responses
        // for the first of the duplicates
        Ok(response.into_iter().filter_map(|each| each).collect())
    }
}

impl FsDataStore {
    async fn list_pinfiles(
        &self,
    ) -> impl futures::stream::Stream<Item = Result<(Cid, PinMode), Error>> + 'static {
        let stream = match tokio::fs::read_dir(self.path.clone()).await {
            Ok(st) => Either::Left(ReadDirStream::new(st)),
            // make this into a stream which will only yield the initial error
            Err(e) => Either::Right(futures::stream::once(futures::future::ready(Err(e)))),
        };

        stream
            .and_then(|d| async move {
                // map over the shard directories
                Ok(if d.file_type().await?.is_dir() {
                    Either::Left(ReadDirStream::new(fs::read_dir(d.path()).await?))
                } else {
                    Either::Right(empty())
                })
            })
            // flatten each
            .try_flatten()
            .map_err(Error::new)
            // convert the paths ending in ".data" into cid
            .try_filter_map(|d| {
                let name = d.file_name();
                let path: &std::path::Path = name.as_ref();

                let mode = if path.extension() == Some("recursive".as_ref()) {
                    Some(PinMode::Recursive)
                } else if path.extension() == Some("direct".as_ref()) {
                    Some(PinMode::Direct)
                } else {
                    None
                };

                let maybe_tuple = mode.and_then(move |mode| {
                    filestem_to_pin_cid(path.file_stem()).map(move |cid| (cid, mode))
                });

                futures::future::ready(Ok(maybe_tuple))
            })
    }
}

/// Reads our serialized format for recusive pins, which is JSON array of stringified Cids.
///
/// On file not found error returns an empty Vec as if nothing had happened. This is because we
/// do "atomic writes" and file removals are expected to be atomic, but reads don't synchronize on
/// writes, so while iterating it's possible that recursive pin is removed.
async fn read_recursively_pinned(path: PathBuf, cid: Cid) -> Result<(Cid, Vec<Cid>), Error> {
    // our fancy format is a Vec<Cid> as json
    let mut path = pin_path(path, &cid);
    path.set_extension("recursive");
    let contents = match tokio::fs::read(path).await {
        Ok(vec) => vec,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // per method comment, return empty Vec; the pins may have seemed to be present earlier
            // but no longer are.
            return Ok((cid, Vec::new()));
        }
        Err(e) => return Err(e.into()),
    };

    let cids: Vec<&str> = serde_json::from_slice(&contents)?;

    // returning a stream which is updated 8kB at time or such might be better, but this should
    // scale quite up as well.
    let found = cids
        .into_iter()
        .map(Cid::try_from)
        .collect::<Result<Vec<Cid>, _>>()?;

    trace!(cid = %cid, count = found.len(), "read indirect pins");
    Ok((cid, found))
}

async fn read_direct_or_recursive(mut block_path: PathBuf) -> Result<Option<PinMode>, Error> {
    tokio::task::spawn_blocking(move || Ok(sync_read_direct_or_recursive(&mut block_path))).await?
}

fn sync_read_direct_or_recursive(block_path: &mut PathBuf) -> Option<PinMode> {
    // important to first check the recursive then only the direct; the latter might be a left over
    for (ext, mode) in &[
        ("recursive", PinMode::Recursive),
        ("direct", PinMode::Direct),
    ] {
        block_path.set_extension(ext);
        // Path::is_file calls fstat and coerces errors to false; this might be enough, as
        // we are holding the lock
        if block_path.is_file() {
            return Some(*mode);
        }
    }
    None
}

fn sync_write_recursive_pin(
    file: std::fs::File,
    count: usize,
    cids: impl Iterator<Item = String>,
) -> Result<(), Error> {
    use serde::{ser::SerializeSeq, Serializer};
    use std::io::{BufWriter, Write};
    let writer = BufWriter::new(file);

    let mut serializer = serde_json::ser::Serializer::new(writer);

    let mut seq = serializer.serialize_seq(Some(count))?;
    for cid in cids {
        seq.serialize_element(&cid)?;
    }
    seq.end()?;

    let mut writer = serializer.into_inner();
    writer.flush()?;

    let file = writer.into_inner()?;
    file.sync_all()?;
    Ok(())
}
