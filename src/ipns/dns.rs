use crate::error::Error;
use crate::path::IpfsPath;
use domain::core::iana::Rtype;
use domain::core::rdata::Txt;
use domain::core::{Dname, Question};
use domain::resolv::stub::resolver::Query;
use domain::resolv::{Resolver, StubResolver};
use futures::compat::{Compat01As03, Future01CompatExt};
use futures::future::{select_ok, SelectOk};
use futures::pin_mut;
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::task::{Context, Poll};

#[derive(Debug, Fail)]
#[fail(display = "no dnslink entry")]
pub struct DnsLinkError;

pub struct DnsLinkFuture {
    query: SelectOk<Compat01As03<Query>>,
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
                    for record in answer.answer()?.limit_to::<Txt>() {
                        let txt = record?;
                        let bytes = txt.data().text();
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
    let qname = Dname::from_str(&domain)?;
    let question = Question::new_in(qname, Rtype::Txt);
    let query1 = StubResolver::new().query(question).compat();

    let qname = Dname::from_str(&dnslink)?;
    let question = Question::new_in(qname, Rtype::Txt);
    let query2 = StubResolver::new().query(question).compat();

    Ok(DnsLinkFuture {
        query: select_ok(vec![query1, query2]),
    }
    .await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::async_test;

    #[test]
    #[ignore]
    fn test_resolve1() {
        async_test(async move {
            let res = resolve("ipfs.io").await.unwrap().to_string();
            assert_eq!(res, "/ipns/website.ipfs.io");
        });
    }

    #[test]
    #[ignore]
    fn test_resolve2() {
        async_test(async move {
            let res = resolve("website.ipfs.io").await.unwrap().to_string();
            // FIXME: perhaps this should just be a path to multihash? otherwise it'll
            // break every time they update the site.
            assert_eq!(res, "/ipfs/QmbV3st6TDZVocp4H2f4KE3tvLP1BEpeRHhZyFL9gD4Ut4");
        });
    }
}
