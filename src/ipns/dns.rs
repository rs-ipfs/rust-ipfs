use crate::error::Error;
use crate::path::IpfsPath;
use bytes::Bytes;
use domain::base::iana::Rtype;
use domain::base::{Dname, Question};
use domain::rdata::rfc1035::Txt;
use domain_resolv::{stub::Answer, StubResolver};
use futures::future::{select_ok, SelectOk};
use futures::pin_mut;
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::task::{Context, Poll};
use std::{fmt, io};

#[derive(Debug)]
pub struct DnsLinkError(String);

impl fmt::Display for DnsLinkError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "DNS error: {}", self.0)
    }
}

impl std::error::Error for DnsLinkError {}

type FutureAnswer = Pin<Box<dyn Future<Output = Result<Answer, io::Error>> + Send>>;

pub struct DnsLinkFuture {
    query: SelectOk<FutureAnswer>,
}

impl Future for DnsLinkFuture {
    type Output = Result<IpfsPath, Error>;

    fn poll(self: Pin<&mut Self>, context: &mut Context) -> Poll<Self::Output> {
        let _self = self.get_mut();
        loop {
            let query = &mut _self.query;
            pin_mut!(query);
            match query.poll(context) {
                Poll::Ready(Ok((answer, rest))) => {
                    for record in answer.answer()?.limit_to::<Txt<_>>() {
                        let txt = record?;
                        let bytes: &[u8] = txt.data().as_flat_slice().unwrap_or(b"");
                        let string = String::from_utf8_lossy(&bytes).to_string();
                        if string.starts_with("dnslink=") {
                            let path = IpfsPath::from_str(&string[8..])?;
                            return Poll::Ready(Ok(path));
                        }
                    }
                    if !rest.is_empty() {
                        _self.query = select_ok(rest);
                    } else {
                        return Poll::Ready(Err(
                            DnsLinkError("no DNS records found".to_owned()).into()
                        ));
                    }
                }
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(DnsLinkError(e.to_string()).into())),
            }
        }
    }
}

#[cfg(not(target_os = "windows"))]
fn create_resolver() -> Result<StubResolver, Error> {
    Ok(StubResolver::default())
}

#[cfg(target_os = "windows")]
fn create_resolver() -> Result<StubResolver, Error> {
    use domain_resolv::stub::conf::ResolvConf;
    use std::{collections::HashSet, io::Cursor};

    let mut config = ResolvConf::new();
    let mut name_servers = String::new();

    let mut dns_servers = HashSet::new();
    for adapter in ipconfig::get_adapters()? {
        for dns in adapter.dns_servers() {
            dns_servers.insert(dns.to_owned());
        }
    }

    for dns in &dns_servers {
        name_servers.push_str(&format!("nameserver {}\n", dns));
    }

    let mut name_servers = Cursor::new(name_servers.into_bytes());
    config.parse(&mut name_servers)?;
    config.finalize();

    Ok(StubResolver::from_conf(config))
}

pub async fn resolve(domain: &str) -> Result<IpfsPath, Error> {
    let mut dnslink = "_dnslink.".to_string();
    dnslink.push_str(domain);
    let resolver = create_resolver()?;

    let qname = Dname::<Bytes>::from_str(&domain)?;
    let question = Question::new_in(qname, Rtype::Txt);
    let resolver1 = resolver.clone();
    let query1 = Box::pin(async move { resolver1.query(question).await });

    let qname = Dname::<Bytes>::from_str(&dnslink)?;
    let question = Question::new_in(qname, Rtype::Txt);
    let resolver2 = resolver;
    let query2 = Box::pin(async move { resolver2.query(question).await });

    Ok(DnsLinkFuture {
        query: select_ok(vec![query1 as FutureAnswer, query2]),
    }
    .await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(max_threads = 1)]
    #[ignore]
    async fn test_resolve1() {
        let res = resolve("ipfs.io").await.unwrap().to_string();
        assert_eq!(res, "/ipns/website.ipfs.io");
    }

    #[tokio::test(max_threads = 1)]
    #[ignore]
    async fn test_resolve2() {
        let res = resolve("website.ipfs.io").await.unwrap().to_string();
        assert_eq!(
            res,
            "/ipfs/bafybeiayvrj27f65vbecspbnuavehcb3znvnt2strop2rfbczupudoizya"
        );
    }
}
