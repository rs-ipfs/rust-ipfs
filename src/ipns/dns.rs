use crate::error::Error;
use crate::path::IpfsPath;
use domain::core::bits::{Dname, Question};
use domain::core::iana::Rtype;
use domain::core::rdata::Txt;
use domain::resolv::{Resolver, StubResolver};
use domain::resolv::stub::resolver::Query;
use std::future::Future;
use std::pin::Pin;
use std::task::{Poll, Waker};
use std::str::FromStr;
use tokio::prelude::{Async, Future as FutureOld, future::SelectOk, future::select_ok};

#[derive(Debug, Fail)]
#[fail(display = "no dnslink entry")]
pub struct DnsLinkError;

pub struct DnsLinkFuture {
    query: SelectOk<Query>,
}

impl Future for DnsLinkFuture {
    type Output = Result<IpfsPath, Error>;

    fn poll(self: Pin<&mut Self>, _waker: &Waker) -> Poll<Self::Output> {
        let _self = self.get_mut();
        loop {
            let poll = _self.query.poll();
            if poll.is_err() {
                return Poll::Ready(Err(DnsLinkError.into()));
            }
            match poll.unwrap() {
                Async::Ready((answer, rest)) => {
                    for record in answer.answer()?.limit_to::<Txt>() {
                        let txt = record?;
                        let bytes = txt.data().text();
                        let string = String::from_utf8_lossy(&bytes).to_string();
                        if string.starts_with("dnslink=") {
                            let path = IpfsPath::from_str(&string[8..])?;
                            return Poll::Ready(Ok(path));
                        }
                    }
                    if rest.len() > 0 {
                        _self.query = select_ok(rest);
                    } else {
                        return Poll::Ready(Err(DnsLinkError.into()))
                    }
                }
                Async::NotReady => return Poll::Pending,
            }
        }
    }
}

pub fn resolve(domain: &str) -> Result<DnsLinkFuture, Error> {
    let mut dnslink = "_dnslink.".to_string();
    dnslink.push_str(domain);
    let qname = Dname::from_str(&dnslink[9..])?;
    let question = Question::new_in(qname, Rtype::Txt);
    let query1 = StubResolver::new().query(question);

    let qname = Dname::from_str(&dnslink)?;
    let question = Question::new_in(qname, Rtype::Txt);
    let query2 = StubResolver::new().query(question);

    Ok(DnsLinkFuture {
        query: select_ok(vec![query1, query2]),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::future::tokio_run;

    #[test]
    fn test_resolve1() {
        tokio_run(async {
            let res = await!(resolve("ipfs.io").unwrap()).unwrap().to_string();
            assert_eq!(res, "/ipns/website.ipfs.io");
        })
    }

    fn test_resolve2() {
        tokio_run(async {
            let res = await!(resolve("website.ipfs.io").unwrap()).unwrap().to_string();
            assert_eq!(res, "/ipfs/QmYfHCcUQBjyvrLfQ8Cnt2YAEiLDNRqMXAeHndM6fDW8yB");
        })
    }
}
