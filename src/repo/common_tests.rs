///! "Interface" tests for pin store, maybe more later
use crate::repo::DataStore;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;

pub struct DSTestContext<T> {
    #[allow(dead_code)]
    tempdir: TempDir,
    datastore: Arc<T>,
}

impl<T: DataStore> DSTestContext<T> {
    /// Create the test context which holds the DataStore inside an Arc and deletes the temporary
    /// directory on drop.
    pub async fn with<F>(factory: F) -> Self
    where
        F: FnOnce(PathBuf) -> T,
    {
        let tempdir = TempDir::new().expect("tempdir creation failed");
        let p = tempdir.path().to_owned();
        let ds = factory(p);

        ds.init().await.unwrap();
        ds.open().await.unwrap();

        DSTestContext {
            tempdir,
            datastore: Arc::new(ds),
        }
    }
}

impl<T: DataStore> std::ops::Deref for DSTestContext<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &*self.datastore
    }
}

/// Generates the "common interface" tests for PinStore implementations as a given module using a
/// types factory method. When adding tests, it might be easier to write them against the one
/// implementation and only then move them here; the compiler errors seem to point at the
/// `#[tokio::test]` attribute and the error needs to be guessed.
#[macro_export]
macro_rules! pinstore_interface_tests {
    ($module_name:ident, $factory:expr) => {
        #[cfg(test)]
        mod $module_name {

            use crate::repo::common_tests::DSTestContext;
            use crate::repo::{DataStore, PinKind, PinMode, PinStore};
            use cid::Cid;
            use futures::{StreamExt, TryStreamExt};
            use std::collections::HashMap;
            use std::convert::TryFrom;

            #[tokio::test]
            async fn pin_direct_twice_is_good() {
                let repo = DSTestContext::with($factory).await;

                let empty =
                    Cid::try_from("QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH").unwrap();

                assert_eq!(
                    repo.is_pinned(&empty).await.unwrap(),
                    false,
                    "initially unpinned"
                );
                repo.insert_direct_pin(&empty).await.unwrap();
                assert_eq!(
                    repo.is_pinned(&empty).await.unwrap(),
                    true,
                    "must be pinned following direct pin"
                );
                repo.insert_direct_pin(&empty)
                    .await
                    .expect("rewriting existing direct pin as direct should be noop");
                assert_eq!(
                    repo.is_pinned(&empty).await.unwrap(),
                    true,
                    "must be pinned following two direct pins"
                );
            }

            #[tokio::test]
            async fn cannot_recursively_unpin_unpinned() {
                let repo = DSTestContext::with($factory).await;
                // root/nested/deeper: QmX5S2xLu32K6WxWnyLeChQFbDHy79ULV9feJYH2Hy9bgp
                let empty =
                    Cid::try_from("QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH").unwrap();

                // the only pin we can try removing without first querying is direct, as shown in
                // `cannot_unpin_indirect`.

                let e = repo
                    .remove_recursive_pin(&empty, futures::stream::empty().boxed())
                    .await
                    .unwrap_err();

                // FIXME: go-ipfs errors on the actual path
                assert_eq!(e.to_string(), "not pinned or pinned indirectly");
            }

            #[tokio::test]
            async fn cannot_unpin_indirect() {
                let repo = DSTestContext::with($factory).await;
                // root/nested/deeper: QmX5S2xLu32K6WxWnyLeChQFbDHy79ULV9feJYH2Hy9bgp
                let root = Cid::try_from("QmX5S2xLu32K6WxWnyLeChQFbDHy79ULV9feJYH2Hy9bgp").unwrap();
                let empty =
                    Cid::try_from("QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH").unwrap();

                // first refs

                repo.insert_recursive_pin(
                    &root,
                    futures::stream::iter(vec![Ok(empty.clone())]).boxed(),
                )
                .await
                .unwrap();

                // should panic because the caller must not attempt this because:

                let (_, kind) = repo
                    .query(vec![empty.clone()], None)
                    .await
                    .unwrap()
                    .into_iter()
                    .next()
                    .unwrap();

                // mem based uses "canonicalized" cids and fs uses them raw
                match kind {
                    PinKind::IndirectFrom(v0_or_v1)
                        if v0_or_v1.hash() == root.hash() && v0_or_v1.codec() == root.codec() => {}
                    x => unreachable!("{:?}", x),
                }

                // this makes the "remove direct" invalid, as the direct pin must not be removed while
                // recursively pinned

                let e = repo.remove_direct_pin(&empty).await.unwrap_err();

                // FIXME: go-ipfs errors on the actual path
                assert_eq!(e.to_string(), "not pinned or pinned indirectly");
            }

            #[tokio::test]
            async fn can_pin_direct_as_recursive() {
                // the other way around doesn't work
                let repo = DSTestContext::with($factory).await;
                //
                // root/nested/deeper: QmX5S2xLu32K6WxWnyLeChQFbDHy79ULV9feJYH2Hy9bgp
                let root = Cid::try_from("QmX5S2xLu32K6WxWnyLeChQFbDHy79ULV9feJYH2Hy9bgp").unwrap();
                let empty =
                    Cid::try_from("QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH").unwrap();

                repo.insert_direct_pin(&root).await.unwrap();

                let pins = repo.list(None).await.try_collect::<Vec<_>>().await.unwrap();

                assert_eq!(
                    pins,
                    vec![(root.clone(), PinMode::Direct)],
                    "must find direct pin for root"
                );

                // first refs

                repo.insert_recursive_pin(
                    &root,
                    futures::stream::iter(vec![Ok(empty.clone())]).boxed(),
                )
                .await
                .unwrap();

                let mut both = repo
                    .list(None)
                    .await
                    .try_collect::<HashMap<Cid, PinMode>>()
                    .await
                    .unwrap();

                assert_eq!(both.remove(&root), Some(PinMode::Recursive));
                assert_eq!(both.remove(&empty), Some(PinMode::Indirect));

                assert!(both.is_empty(), "{:?}", both);
            }

            #[tokio::test]
            async fn pin_recursive_pins_all_blocks() {
                let repo = DSTestContext::with($factory).await;

                // root/nested/deeper: QmX5S2xLu32K6WxWnyLeChQFbDHy79ULV9feJYH2Hy9bgp
                let root = Cid::try_from("QmX5S2xLu32K6WxWnyLeChQFbDHy79ULV9feJYH2Hy9bgp").unwrap();
                let empty =
                    Cid::try_from("QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH").unwrap();

                // assumed use:
                repo.insert_recursive_pin(
                    &root,
                    futures::stream::iter(vec![Ok(empty.clone())]).boxed(),
                )
                .await
                .unwrap();

                assert!(repo.is_pinned(&root).await.unwrap());
                assert!(repo.is_pinned(&empty).await.unwrap());

                let mut both = repo
                    .list(None)
                    .await
                    .try_collect::<HashMap<Cid, PinMode>>()
                    .await
                    .unwrap();

                assert_eq!(both.remove(&root), Some(PinMode::Recursive));
                assert_eq!(both.remove(&empty), Some(PinMode::Indirect));

                assert!(both.is_empty(), "{:?}", both);
            }

            #[tokio::test]
            async fn indirect_can_be_pinned_directly() {
                let repo = DSTestContext::with($factory).await;

                // root/nested/deeper: QmX5S2xLu32K6WxWnyLeChQFbDHy79ULV9feJYH2Hy9bgp
                let root = Cid::try_from("QmX5S2xLu32K6WxWnyLeChQFbDHy79ULV9feJYH2Hy9bgp").unwrap();
                let empty =
                    Cid::try_from("QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH").unwrap();

                repo.insert_direct_pin(&empty).await.unwrap();

                repo.insert_recursive_pin(
                    &root,
                    futures::stream::iter(vec![Ok(empty.clone())]).boxed(),
                )
                .await
                .unwrap();

                let mut both = repo
                    .list(None)
                    .await
                    .try_collect::<HashMap<Cid, PinMode>>()
                    .await
                    .unwrap();

                assert_eq!(both.remove(&root), Some(PinMode::Recursive));

                // when working on the first round of mem based recursive pinning I had understood
                // this to be a rule. go-ipfs preferes the priority order of Recursive, Direct,
                // Indirect and so does our fs datastore.
                let mode = both.remove(&empty).unwrap();
                assert!(
                    mode == PinMode::Indirect || mode == PinMode::Direct,
                    "{:?}",
                    mode
                );

                assert!(both.is_empty(), "{:?}", both);
            }

            #[tokio::test]
            async fn direct_and_indirect_when_parent_unpinned() {
                let repo = DSTestContext::with($factory).await;

                // root/nested/deeper: QmX5S2xLu32K6WxWnyLeChQFbDHy79ULV9feJYH2Hy9bgp
                let root = Cid::try_from("QmX5S2xLu32K6WxWnyLeChQFbDHy79ULV9feJYH2Hy9bgp").unwrap();
                let empty =
                    Cid::try_from("QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH").unwrap();

                repo.insert_direct_pin(&empty).await.unwrap();

                assert_eq!(
                    repo.query(vec![empty.clone()], None)
                        .await
                        .unwrap()
                        .into_iter()
                        .collect::<Vec<_>>(),
                    vec![(empty.clone(), PinKind::Direct)],
                );

                // first refs

                repo.insert_recursive_pin(
                    &root,
                    futures::stream::iter(vec![Ok(empty.clone())]).boxed(),
                )
                .await
                .unwrap();

                // second refs

                repo.remove_recursive_pin(
                    &root,
                    futures::stream::iter(vec![Ok(empty.clone())]).boxed(),
                )
                .await
                .unwrap();

                let mut one = repo
                    .list(None)
                    .await
                    .try_collect::<HashMap<Cid, PinMode>>()
                    .await
                    .unwrap();

                assert_eq!(one.remove(&empty), Some(PinMode::Direct));

                assert!(one.is_empty(), "{:?}", one);
            }

            #[tokio::test]
            async fn cannot_pin_recursively_pinned_directly() {
                // this is a bit of odd as other ops are additive
                let repo = DSTestContext::with($factory).await;

                let empty =
                    Cid::try_from("QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH").unwrap();

                repo.insert_recursive_pin(&empty, futures::stream::iter(vec![]).boxed())
                    .await
                    .unwrap();

                let e = repo.insert_direct_pin(&empty).await.unwrap_err();

                // go-ipfs puts the cid in front here, not sure if we want to at this level? though in
                // go-ipfs it's different than path resolving
                assert_eq!(e.to_string(), "already pinned recursively");
            }
        }
    };
}
