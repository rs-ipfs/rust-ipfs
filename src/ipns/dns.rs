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
use std::io;
use std::pin::Pin;
use std::str::FromStr;
use std::task::{Context, Poll};
use thiserror::Error;

#[derive(Debug, Error)]
#[error("no dnslink entry")]
pub struct DnsLinkError;

type FutureAnswer = Pin<Box<dyn Future<Output = Result<Answer, io::Error>>>>;

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
                        return Poll::Ready(Err(DnsLinkError.into()));
                    }
                }
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(_)) => return Poll::Ready(Err(DnsLinkError.into())),
            }
        }
    }
}

pub async fn resolve(domain: &str) -> Result<IpfsPath, Error> {
    let mut dnslink = "_dnslink.".to_string();
    dnslink.push_str(domain);
    let qname = Dname::<Bytes>::from_str(&domain)?;
    let question = Question::new_in(qname, Rtype::Txt);
    let resolver = StubResolver::new();
    let query1 = Box::pin(async move { resolver.query(question).await });

    let qname = Dname::<Bytes>::from_str(&dnslink)?;
    let question = Question::new_in(qname, Rtype::Txt);
    let resolver = StubResolver::new();
    let query2 = Box::pin(async move { resolver.query(question).await });

    Ok(DnsLinkFuture {
        query: select_ok(vec![query1 as FutureAnswer, query2]),
    }
    .await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_resolve1() {
        let res = resolve("ipfs.io").await.unwrap().to_string();
        assert_eq!(res, "/ipns/website.ipfs.io");
    }

    #[tokio::test]
    async fn test_resolve2() {
        let res = resolve("website.ipfs.io").await.unwrap().to_string();
        assert_eq!(res, "/ipfs/bafybeiayvrj27f65vbecspbnuavehcb3znvnt2strop2rfbczupudoizya");
    }
}
