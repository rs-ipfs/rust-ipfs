use crate::error::Error;
use crate::path::IpfsPath;
use std::str::FromStr;
use tracing_futures::Instrument;

pub async fn resolve(domain: &str) -> Result<IpfsPath, Error> {
    use std::borrow::Cow;
    use trust_dns_resolver::AsyncResolver;

    let span = tracing::trace_span!("dnslink", %domain);

    async move {
        // allow using non fqdn names (using the local search path suffices)
        let searched = Some(Cow::Borrowed(domain));

        let prefix = "_dnslink.";
        let prefixed = if !domain.starts_with(prefix) {
            let mut next = String::with_capacity(domain.len() + prefix.len());
            next.push_str(prefix);
            next.push_str(domain);
            Some(Cow::Owned(next))
        } else {
            None
        };

        let searched = searched.into_iter().chain(prefixed.into_iter());

        // FIXME: this uses caching trust-dns resolver even though it's discarded right away
        // when trust-dns support lands in future libp2p-dns investigate if we could share one, no need
        // to have multiple related caches.
        let resolver = AsyncResolver::tokio_from_system_conf()?;

        // previous implementation searched $domain and _dnslink.$domain concurrently. not sure did
        // `domain` assume fqdn names or not, but local suffices were not being searched on windows at
        // least. they are probably waste of time most of the time.
        for domain in searched.into_iter() {
            let res = match resolver.txt_lookup(&*domain).await {
                Ok(res) => res,
                Err(e) => {
                    tracing::debug!("resolving dnslink of {:?} failed: {}", domain, e);
                    continue;
                }
            };

            let mut paths = res
                .iter()
                .flat_map(|txt| txt.iter())
                .filter_map(|txt| {
                    if txt.starts_with(b"dnslink=") {
                        Some(&txt[b"dnslink=".len()..])
                    } else {
                        None
                    }
                })
                .map(|suffix| {
                    std::str::from_utf8(suffix)
                        .map_err(Error::from)
                        .and_then(|s| IpfsPath::from_str(s))
                });

            if let Some(Ok(x)) = paths.next() {
                tracing::trace!("dnslink found for {:?}", domain);
                return Ok(x);
            }

            tracing::trace!("zero TXT records found for {:?}", domain);
        }

        Err(anyhow::anyhow!("failed to resolve {:?}", domain))
    }
    .instrument(span)
    .await
}

#[cfg(test)]
mod tests {
    use super::resolve;

    #[tokio::test]
    async fn resolve_ipfs_io() {
        tracing_subscriber::fmt::init();
        let res = resolve("ipfs.io").await.unwrap().to_string();
        assert_eq!(res, "/ipns/website.ipfs.io");
    }

    #[tokio::test]
    async fn resolve_website_ipfs_io() {
        let res = resolve("website.ipfs.io").await.unwrap();

        assert!(
            matches!(res.root(), crate::path::PathRoot::Ipld(_)),
            "expected an /ipfs/cid path"
        );
    }
}
