//! Persistent fs backed repo
use crate::error::Error;
use crate::repo::{BlockPut, BlockStore};
use async_trait::async_trait;
use bitswap::Block;
use cid::Cid;
use core::convert::TryFrom;
use std::io::Read;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use tracing_futures::Instrument;

use tokio::fs;
use tokio::sync::broadcast;

use super::{
    BlockRm, BlockRmError, Column, DataStore, PinKind, PinMode, PinStore, References, RepoCid,
};
use std::hash::Hash;

use futures::future::Either;
use futures::stream::{empty, StreamExt, TryStreamExt};
use std::collections::{HashMap, HashSet};

// mod flatfs;
type ArcMutexMap<A, B> = Arc<Mutex<HashMap<A, B>>>;

/// FsDataStore which uses the filesystem as a lockable key-value store. Maintains a similar to
/// blockstore sharded two level storage. Direct have empty files, recursive pins record all of
/// their indirect descendants. Pin files are separated by their file extensions.
///
/// When modifying, per cid locking is used to hold up the invariants.
#[derive(Debug)]
pub struct FsDataStore {
    /// The base directory under which we have a sharded directory structure, and the individual
    /// blocks are stored under the shard. See unixfs/examples/cat.rs for read example.
    path: PathBuf,

    /// Start with simple, conservative solution, allows concurrent queries but single writer.
    /// FIXME: this needs to become semaphore for owned permits; do "dirty reads" as there should
    /// not be any chance of seeing partial writes, that is, under modern linux filesystems.
    lock: tokio::sync::RwLock<()>,

    /// Not really needed
    written_bytes: AtomicU64,
}

// FIXME: need to do
//  - bring back the canonicalized version, or just force base32? should work with that as well
//  - block rm test failure
//  - refs local test failure

impl FsDataStore {
    async fn list_pinfiles(
        &self,
    ) -> impl futures::stream::Stream<Item = Result<(Cid, PinMode), Error>> + 'static {
        let stream = match tokio::fs::read_dir(self.path.clone()).await {
            Ok(st) => Either::Left(st),
            // make this into a stream which will only yield the initial error
            Err(e) => Either::Right(futures::stream::once(futures::future::ready(Err(e)))),
        };

        stream
            .and_then(|d| async move {
                // map over the shard directories
                Ok(if d.file_type().await?.is_dir() {
                    Either::Left(fs::read_dir(d.path()).await?)
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
            return Some(mode.clone());
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

#[async_trait]
impl DataStore for FsDataStore {
    fn new(mut root: PathBuf) -> Self {
        root.push("pins");
        FsDataStore {
            path: root,
            lock: Default::default(),
            written_bytes: Default::default(),
        }
    }

    async fn init(&self) -> Result<(), Error> {
        tokio::fs::create_dir_all(&self.path).await?;
        Ok(())
    }

    async fn open(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn contains(&self, _col: Column, _key: &[u8]) -> Result<bool, Error> {
        Err(anyhow::anyhow!("not implemented"))
    }

    async fn get(&self, _col: Column, _key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        Err(anyhow::anyhow!("not implemented"))
    }

    async fn put(&self, _col: Column, _key: &[u8], _value: &[u8]) -> Result<(), Error> {
        Err(anyhow::anyhow!("not implemented"))
    }

    async fn remove(&self, _col: Column, _key: &[u8]) -> Result<(), Error> {
        Err(anyhow::anyhow!("not implemented"))
    }

    async fn wipe(&self) {
        todo!()
    }
}

#[async_trait]
impl PinStore for FsDataStore {
    async fn is_pinned(&self, cid: &Cid) -> Result<bool, Error> {
        let _g = self.lock.read().await;

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

        while let Some(recursive) = st.try_next().await? {
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
        let _g = self.lock.write().await;

        let mut path = pin_path(self.path.clone(), target);

        let span = tracing::Span::current();

        tokio::task::spawn_blocking(move || {
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

        // FIXME: this needs to become OwnedWriteGuard so that we can move it to the task
        // otherwise this future might be dropped, but the task will continue
        // this is not supported; perhaps until that we are "best effort"
        //
        // Owned semaphore permits are available; maybe synchronize writes with that?
        let _g = self.lock.write().await;

        let mut path = pin_path(self.path.clone(), target);

        let span = tracing::Span::current();

        tokio::task::spawn_blocking(move || {
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
        let _g = self.lock.write().await;

        let mut path = pin_path(self.path.clone(), target);

        let span = tracing::Span::current();

        tokio::task::spawn_blocking(move || {
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
        let _g = self.lock.write().await;

        let mut path = pin_path(self.path.clone(), target);

        let span = tracing::Span::current();

        tokio::task::spawn_blocking(move || {
            let _entered = span.enter();

            path.set_extension("direct");

            match std::fs::remove_file(&path) {
                Ok(_) => trace!("direct pin removed"),
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
                    Ok(())
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // we may have removed only the direct pin, but in the `pin rm` sense this is
                    // still a success
                    Ok(())
                }
                Err(e) => Err(e.into()),
            }
        })
        .await??;

        Ok(())
    }

    async fn list(
        &self,
        requirement: Option<PinMode>,
    ) -> futures::stream::BoxStream<'static, Result<(Cid, PinMode), Error>> {
        // FIXME: needs a lock, or does it?
        let cids = self.list_pinfiles().await;

        let path = self.path.clone();

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

            let collect_recursive_for_indirect = requirement.as_ref().map(|r| *r == PinMode::Indirect).unwrap_or(true);

            futures::pin_mut!(cids);

            while let Some((cid, mode)) = cids.try_next().await? {

                let matches = requirement.as_ref().map(|r| *r == mode).unwrap_or(true);

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

            while let Some((_, next_batch)) = recursive.try_next().await? {
                for indirect in next_batch {
                    if returned.insert(indirect.clone()) {
                        yield (indirect, PinMode::Indirect);
                    }
                }

                trace!(unique = returned.len(), "completed batch of indirect");
            }
        };

        st.in_current_span().boxed()
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

        let (mut response, mut remaining) = if check_direct {
            // find the recursive and direct ones by just seeing if the files exist
            let base = self.path.clone();
            tokio::task::spawn_blocking(move || {
                for (i, cid) in ids.into_iter().enumerate() {
                    let mut path = pin_path(base.clone(), &cid);

                    if let Some(mode) = sync_read_direct_or_recursive(&mut path) {
                        if searched_suffix.as_ref().map(|m| *m == mode).unwrap_or(true) {
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

            'out: while let Some((referring, references)) = recursives.try_next().await? {
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

#[derive(Debug)]
pub struct FsBlockStore {
    /// The base directory under which we have a sharded directory structure, and the individual
    /// blocks are stored under the shard. See unixfs/examples/cat.rs for read example.
    path: PathBuf,

    /// Synchronize concurrent reads and writes to the same Cid.
    /// If the write ever happens, the message sent will be Ok(()), on failure it'll be an Err(()).
    /// Since this is a broadcast channel, the late arriving receiver might not get any messages.
    writes: ArcMutexMap<RepoCid, broadcast::Sender<Result<(), ()>>>,

    /// Initially used to demonstrate a bug, not really needed anymore. Could be used as a basis
    /// for periodic synching to disk to know much space we have used.
    written_bytes: AtomicU64,
}

struct RemoveOnDrop<K: Eq + Hash, V>(ArcMutexMap<K, V>, Option<K>);

impl<K: Eq + Hash, V> Drop for RemoveOnDrop<K, V> {
    fn drop(&mut self) {
        if let Some(key) = self.1.take() {
            let mut g = self.0.lock().unwrap();
            g.remove(&key);
        }
    }
}

/// When synchronizing to a possible ongoing write through `FsBlockStore::writes` these are the
/// possible outcomes.
#[derive(Debug)]
enum WriteCompletion {
    KnownGood,
    NotObserved,
    KnownBad,
    NotOngoing,
}

impl FsBlockStore {
    /// Returns the same Cid in either case. Ok variant is returned in case it is suspected the
    /// write completed successfully or there was never any write ongoing. Err variant is returned
    /// if it's known that the write failed.
    async fn write_completion(&self, cid: &Cid) -> WriteCompletion {
        use std::collections::hash_map::Entry;

        let mut rx = match self
            .writes
            .lock()
            .expect("cannot support poisoned")
            .entry(RepoCid(cid.clone()))
        {
            Entry::Occupied(oe) => oe.get().subscribe(),
            Entry::Vacant(_) => return WriteCompletion::NotOngoing,
        };

        trace!("awaiting concurrent write to completion");

        match rx.recv().await {
            Ok(Ok(())) => WriteCompletion::KnownGood,
            Err(broadcast::RecvError::Closed) => WriteCompletion::NotObserved,
            Ok(Err(_)) => WriteCompletion::KnownBad,
            Err(broadcast::RecvError::Lagged(_)) => {
                unreachable!("sending at most one message to the channel with capacity of one")
            }
        }
    }
}

#[async_trait]
impl BlockStore for FsBlockStore {
    fn new(path: PathBuf) -> Self {
        FsBlockStore {
            path,
            //cids: Default::default(),
            writes: Arc::new(Mutex::new(HashMap::with_capacity(8))),
            written_bytes: Default::default(),
        }
    }

    async fn init(&self) -> Result<(), Error> {
        fs::create_dir_all(self.path.clone()).await?;
        Ok(())
    }

    async fn open(&self) -> Result<(), Error> {
        // TODO: we probably want to cache the space usage?
        Ok(())
    }

    async fn contains(&self, cid: &Cid) -> Result<bool, Error> {
        let path = block_path(self.path.clone(), cid);

        // why doesn't this synchronize with the rest? Not sure if there is any use for this method
        // actually. When does it matter if a block exists, except for testing.

        let metadata = match fs::metadata(path).await {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(e.into()),
        };

        Ok(metadata.is_file())
    }

    async fn get(&self, cid: &Cid) -> Result<Option<Block>, Error> {
        let span = tracing::trace_span!("get block", cid = %cid);

        async move {
            if let WriteCompletion::KnownBad = self.write_completion(cid).await {
                return Ok(None);
            }

            let path = block_path(self.path.clone(), cid);

            let cid = cid.to_owned();

            // probably best to do everything in the blocking thread if we are to issue multiple
            // syscalls
            tokio::task::spawn_blocking(move || {
                let mut file = match std::fs::File::open(path) {
                    Ok(file) => file,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                    Err(e) => {
                        return Err(e.into());
                    }
                };

                let len = file.metadata()?.len();

                let mut data = Vec::with_capacity(len as usize);
                file.read_to_end(&mut data)?;
                let block = Block::new(data.into_boxed_slice(), cid);
                Ok(Some(block))
            })
            .await?
        }
        .instrument(span)
        .await
    }

    async fn put(&self, block: Block) -> Result<(Cid, BlockPut), Error> {
        use std::collections::hash_map::Entry;

        let span = tracing::trace_span!("put block", cid = %block.cid());

        let target_path = block_path(self.path.clone(), &block.cid());
        let cid = block.cid;
        let data = block.data;

        let inner_span = span.clone();

        async move {
            // why synchronize here? because when we lose the race we cant know if there was someone
            // else interested in writing this block or not

            // FIXME: allowing only the creator to cleanup means this is not forget safe. the forget
            // doesn't result in memory unsafety but it will deadlock any other access to the cid.
            let (tx, mut rx) = {
                let mut g = self.writes.lock().expect("cant support poisoned");

                match g.entry(RepoCid(cid.to_owned())) {
                    Entry::Occupied(oe) => {
                        // someone is already writing this, nice
                        trace!("joining in on another already writing the block");
                        (oe.get().clone(), oe.get().subscribe())
                    }
                    Entry::Vacant(ve) => {
                        // we might be the first, or then the block exists already
                        let (tx, rx) = broadcast::channel(1);
                        ve.insert(tx.clone());
                        (tx, rx)
                    }
                }
            };

            // create this in case the winner is dropped while awaiting
            let cleanup = RemoveOnDrop(self.writes.clone(), Some(RepoCid(cid.to_owned())));

            let span = tracing::Span::current();

            // launch a blocking task for the filesystem mutation.
            // `tx` is moved into the task but `rx` stays in the async context.
            let je = tokio::task::spawn_blocking(move || {
                let _entered = span.enter();
                // pick winning writer with filesystem and create_new; this error will be the 1st
                // nested level

                // this is blocking context, use this instead of instrument
                let _entered = inner_span.enter();

                let sharded = target_path
                    .parent()
                    .expect("we already have at least the shard parent");

                // FIXME: missing fsync on directories; for example after winning the race we could
                // fsync the parent and parent.parent
                std::fs::create_dir_all(sharded)?;

                let target = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&target_path)?;

                let temp_path = target_path.with_extension("tmp");

                match write_through_tempfile(target, &target_path, temp_path, &data) {
                    Ok(()) => {
                        let _ = tx
                            .send(Ok(()))
                            .expect("this cannot fail as we have at least one receiver on stack");

                        trace!("successfully wrote the block");
                        Ok::<_, std::io::Error>(Ok(data.len()))
                    }
                    Err(e) => {
                        match std::fs::remove_file(&target_path) {
                            Ok(_) => debug!("removed partially written {:?}", target_path),
                            Err(removal) => warn!(
                                "failed to remove partially written {:?}: {}",
                                target_path, removal
                            ),
                        }
                        let _ = tx
                            .send(Err(()))
                            .expect("this cannot fail as we have at least one receiver on stack");
                        Ok(Err(e))
                    }
                }
            })
            .await;

            // this is quite unfortunate but can't think of a way which would handle cleanup in drop
            // and not waste much effort.
            drop(cleanup);

            match je {
                Ok(Ok(Ok(written))) => {
                    drop(rx);

                    self.written_bytes
                        .fetch_add(written as u64, Ordering::SeqCst);

                    Ok((cid, BlockPut::NewBlock))
                }
                Ok(Ok(Err(e))) => {
                    // write failed but hopefully the target was removed
                    // no point in trying to remove it now
                    // ignore if no one is listening
                    Err(Error::new(e))
                }
                Ok(Err(_)) => {
                    // At least the following cases:
                    // - the block existed already
                    // - the block is being written to and we should await for this to complete
                    // - readonly or full filesystem prevents file creation

                    let message = match rx.recv().await {
                        Ok(message) => {
                            trace!("synchronized with writer, write outcome: {:?}", message);
                            message
                        }
                        Err(broadcast::RecvError::Closed) => {
                            // there was never any write intention by any party, and we may have just
                            // closed the last sender above, or we were late for the one message.
                            Ok(())
                        }
                        Err(broadcast::RecvError::Lagged(_)) => {
                            unreachable!("broadcast channel should only be messaged once here")
                        }
                    };

                    drop(rx);

                    if message.is_err() {
                        // could loop, however if one write failed, the next should probably
                        // fail as well (e.g. out of disk space)
                        Err(anyhow::anyhow!("other concurrent write failed"))
                    } else {
                        Ok((cid.to_owned(), BlockPut::Existed))
                    }
                }
                Err(e) => {
                    // blocking task panicked or the runtime is going down, but we don't know
                    // if the thread has stopped or not (like not)
                    Err(e.into())
                }
            }
        }
        .instrument(span)
        .await
    }

    async fn remove(&self, cid: &Cid) -> Result<Result<BlockRm, BlockRmError>, Error> {
        let path = block_path(self.path.clone(), cid);

        let span = trace_span!("remove block", cid = %cid);

        match self.write_completion(cid).instrument(span).await {
            WriteCompletion::KnownBad => Ok(Err(BlockRmError::NotFound(cid.to_owned()))),
            completion => {
                trace!(cid = %cid, completion = ?completion, "removing block after synchronizing");
                match fs::remove_file(path).await {
                    // FIXME: not sure if theres any point in taking cid ownership here?
                    Ok(()) => Ok(Ok(BlockRm::Removed(cid.to_owned()))),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        Ok(Err(BlockRmError::NotFound(cid.to_owned())))
                    }
                    Err(e) => Err(e.into()),
                }
            }
        }
    }

    async fn list(&self) -> Result<Vec<Cid>, Error> {
        use futures::future::{ready, Either};
        use futures::stream::{empty, TryStreamExt};

        let span = tracing::trace_span!("listing blocks");

        async move {
            let stream = fs::read_dir(self.path.clone()).await?;

            // FIXME: written as a stream to make the Vec be BoxStream<'static, Cid>
            let vec = stream
                .and_then(|d| async move {
                    // map over the shard directories
                    Ok(if d.file_type().await?.is_dir() {
                        Either::Left(fs::read_dir(d.path()).await?)
                    } else {
                        Either::Right(empty())
                    })
                })
                // flatten each
                .try_flatten()
                // convert the paths ending in ".data" into cid
                .try_filter_map(|d| {
                    let name = d.file_name();
                    let path: &std::path::Path = name.as_ref();

                    ready(if path.extension() != Some("data".as_ref()) {
                        Ok(None)
                    } else {
                        let maybe_cid = filestem_to_block_cid(path.file_stem());
                        Ok(maybe_cid)
                    })
                })
                .try_collect::<Vec<_>>()
                .await?;

            Ok(vec)
        }
        .instrument(span)
        .await
    }

    async fn wipe(&self) {
        unimplemented!("wipe")
    }
}

fn filestem_to_block_cid(file_stem: Option<&std::ffi::OsStr>) -> Option<Cid> {
    file_stem.and_then(|stem| stem.to_str()).and_then(|s| {
        // this isn't an interchangeable way to store esp. cidv0
        let cid = Cid::try_from(s);

        // it's very unlikely that we'd hit a valid file with "data" extension
        // which we did write so I'd say wrapping the Cid parsing error as
        // std::io::Error is highly unnecessary. if someone wants to
        // *keep* ".data" ending files in the block store we shouldn't
        // die over it.
        //
        // if we could, we would do a log_once here, if we could easily
        // do such thing. like a inode based global probabilistic
        // hashset.
        //
        // FIXME: add test

        cid.ok()
    })
}

fn filestem_to_pin_cid(file_stem: Option<&std::ffi::OsStr>) -> Option<Cid> {
    file_stem.and_then(|stem| stem.to_str()).and_then(|s| {
        // this isn't an interchangeable way to store esp. cidv0
        let bytes = multibase::Base::Base32Lower.decode(s).ok()?;
        let cid = Cid::try_from(bytes);

        // it's very unlikely that we'd hit a valid file with "data" extension
        // which we did write so I'd say wrapping the Cid parsing error as
        // std::io::Error is highly unnecessary. if someone wants to
        // *keep* ".data" ending files in the block store we shouldn't
        // die over it.
        //
        // if we could, we would do a log_once here, if we could easily
        // do such thing. like a inode based global probabilistic
        // hashset.
        //
        // FIXME: add test

        cid.ok()
    })
}

fn block_path(mut base: PathBuf, cid: &Cid) -> PathBuf {
    // this is ascii always, and wasteful until we can drop the cid for multihash ... which is
    // probably soon, we just need turn /refs/local to use /pin/list.
    let mut file = if cid.version() == cid::Version::V1 {
        cid.to_string()
    } else {
        Cid::new_v1(cid.codec(), cid.hash().to_owned()).to_string()
    };

    // second-to-last/2
    let start = file.len() - 3;

    let shard = &file[start..start + 2];
    assert_eq!(file[start + 2..].len(), 1);
    base.push(shard);

    // not sure why set extension instead
    file.push_str(".data");
    base.push(file);
    base
}

/// Same as `block_path` except it doesn't canonicalize the cid to later version.
fn pin_path(mut base: PathBuf, cid: &Cid) -> PathBuf {
    // it might be illegal to to render cidv0 as base32
    let mut file: String = multibase::Base::Base32Lower.encode(cid.to_bytes());

    // second-to-last/2
    let start = file.len() - 3;

    let shard = &file[start..start + 2];
    assert_eq!(file[start + 2..].len(), 1);
    base.push(shard);

    // not sure why set extension instead
    file.push_str(".data");
    base.push(file);
    base
}

fn write_through_tempfile(
    target: std::fs::File,
    target_path: impl AsRef<std::path::Path>,
    temp_path: impl AsRef<std::path::Path>,
    data: &[u8],
) -> Result<(), std::io::Error> {
    use std::io::Write;

    let mut temp = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temp_path)?;

    temp.write_all(&*data)?;
    temp.flush()?;

    // safe default
    temp.sync_all()?;

    drop(temp);
    drop(target);

    std::fs::rename(temp_path, target_path)?;

    // FIXME: there should be a directory fsync here as well

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitswap::Block;
    use cid::{Cid, Codec};
    use hex_literal::hex;
    use multihash::Sha2_256;
    use std::env::temp_dir;
    use std::sync::Arc;

    #[tokio::test(max_threads = 1)]
    async fn test_fs_blockstore() {
        let mut tmp = temp_dir();
        tmp.push("blockstore1");
        std::fs::remove_dir_all(tmp.clone()).ok();
        let store = FsBlockStore::new(tmp.clone());

        let data = b"1".to_vec().into_boxed_slice();
        let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&data));
        let block = Block::new(data, cid.clone());

        store.init().await.unwrap();
        store.open().await.unwrap();

        let contains = store.contains(&cid).await.unwrap();
        assert_eq!(contains, false);
        let get = store.get(&cid).await.unwrap();
        assert_eq!(get, None);
        if store.remove(&cid).await.unwrap().is_ok() {
            panic!("block should not be found")
        }

        let put = store.put(block.clone()).await.unwrap();
        assert_eq!(put.0, cid.to_owned());
        let contains = store.contains(&cid);
        assert_eq!(contains.await.unwrap(), true);
        let get = store.get(&cid);
        assert_eq!(get.await.unwrap(), Some(block.clone()));

        store.remove(&cid).await.unwrap().unwrap();
        let contains = store.contains(&cid);
        assert_eq!(contains.await.unwrap(), false);
        let get = store.get(&cid);
        assert_eq!(get.await.unwrap(), None);

        std::fs::remove_dir_all(tmp).ok();
    }

    #[tokio::test(max_threads = 1)]
    async fn test_fs_blockstore_open() {
        let mut tmp = temp_dir();
        tmp.push("blockstore2");
        std::fs::remove_dir_all(&tmp).ok();

        let data = b"1".to_vec().into_boxed_slice();
        let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&data));
        let block = Block::new(data, cid);

        let block_store = FsBlockStore::new(tmp.clone());
        block_store.init().await.unwrap();
        block_store.open().await.unwrap();

        assert!(!block_store.contains(block.cid()).await.unwrap());
        block_store.put(block.clone()).await.unwrap();

        let block_store = FsBlockStore::new(tmp.clone());
        block_store.open().await.unwrap();
        assert!(block_store.contains(block.cid()).await.unwrap());
        assert_eq!(block_store.get(block.cid()).await.unwrap().unwrap(), block);

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[tokio::test(max_threads = 1)]
    async fn test_fs_blockstore_list() {
        let mut tmp = temp_dir();
        tmp.push("blockstore_list");
        std::fs::remove_dir_all(&tmp).ok();

        let block_store = FsBlockStore::new(tmp.clone());
        block_store.init().await.unwrap();
        block_store.open().await.unwrap();

        for data in &[b"1", b"2", b"3"] {
            let data_slice = data.to_vec().into_boxed_slice();
            let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&data_slice));
            let block = Block::new(data_slice, cid);
            block_store.put(block.clone()).await.unwrap();
        }

        let cids = block_store.list().await.unwrap();
        assert_eq!(cids.len(), 3);
        for cid in cids.iter() {
            assert!(block_store.contains(cid).await.unwrap());
        }
    }

    #[tokio::test(max_threads = 1)]
    async fn race_to_insert_new() {
        // FIXME: why not tempdir?
        let mut tmp = temp_dir();
        tmp.push("race_to_insert_new");
        std::fs::remove_dir_all(&tmp).ok();

        let single = FsBlockStore::new(tmp.clone());
        single.init().await.unwrap();

        let single = Arc::new(single);

        let cid = Cid::try_from("QmRgutAxd8t7oGkSm4wmeuByG6M51wcTso6cubDdQtuEfL").unwrap();
        let data = hex!("0a0d08021207666f6f6261720a1807");

        let block = Block {
            cid,
            data: data.into(),
        };

        let count = 10;

        let (writes, existing) = race_to_insert_scenario(count, block, &single).await;

        let single = Arc::try_unwrap(single).unwrap();
        assert_eq!(single.written_bytes.into_inner(), 15);

        assert_eq!(writes, 1);
        assert_eq!(existing, count - 1);
    }

    #[tokio::test(max_threads = 1)]
    async fn race_to_insert_with_existing() {
        // FIXME: why not tempdir?
        let mut tmp = temp_dir();
        tmp.push("race_to_insert_existing");
        std::fs::remove_dir_all(&tmp).ok();

        let single = FsBlockStore::new(tmp.clone());
        single.init().await.unwrap();

        let single = Arc::new(single);

        let cid = Cid::try_from("QmRgutAxd8t7oGkSm4wmeuByG6M51wcTso6cubDdQtuEfL").unwrap();
        let data = hex!("0a0d08021207666f6f6261720a1807");

        let block = Block {
            cid,
            data: data.into(),
        };

        single.put(block.clone()).await.unwrap();

        assert_eq!(single.written_bytes.load(Ordering::SeqCst), 15);

        let count = 10;

        let (writes, existing) = race_to_insert_scenario(count, block, &single).await;

        let single = Arc::try_unwrap(single).unwrap();
        assert_eq!(single.written_bytes.into_inner(), 15);

        assert_eq!(writes, 0);
        assert_eq!(existing, count);
    }

    async fn race_to_insert_scenario(
        count: usize,
        block: Block,
        blockstore: &Arc<FsBlockStore>,
    ) -> (usize, usize) {
        let barrier = Arc::new(tokio::sync::Barrier::new(count));

        let join_handles = (0..count)
            .map(|_| {
                tokio::spawn({
                    let bs = Arc::clone(&blockstore);
                    let barrier = Arc::clone(&barrier);
                    let block = block.clone();
                    async move {
                        barrier.wait().await;
                        bs.put(block).await
                    }
                })
            })
            .collect::<Vec<_>>();

        let mut writes = 0usize;
        let mut existing = 0usize;

        for jh in join_handles {
            let res = jh.await;

            match res {
                Ok(Ok((_, BlockPut::NewBlock))) => writes += 1,
                Ok(Ok((_, BlockPut::Existed))) => existing += 1,
                Ok(Err(e)) => println!("joinhandle err: {}", e),
                _ => unreachable!("join error"),
            }
        }

        (writes, existing)
    }

    #[tokio::test(max_threads = 1)]
    async fn remove() {
        // FIXME: why not tempdir?
        let mut tmp = temp_dir();
        tmp.push("remove");
        std::fs::remove_dir_all(&tmp).ok();

        let single = FsBlockStore::new(tmp.clone());

        single.init().await.unwrap();

        let cid = Cid::try_from("QmRgutAxd8t7oGkSm4wmeuByG6M51wcTso6cubDdQtuEfL").unwrap();
        let data = hex!("0a0d08021207666f6f6261720a1807");

        let block = Block {
            cid: cid.clone(),
            data: data.into(),
        };

        assert_eq!(single.list().await.unwrap().len(), 0);

        single.put(block).await.unwrap();

        // compare the multihash since we store the block named as cidv1
        assert_eq!(single.list().await.unwrap()[0].hash(), cid.hash());

        single.remove(&cid).await.unwrap().unwrap();
        assert_eq!(single.list().await.unwrap().len(), 0);
    }
}
