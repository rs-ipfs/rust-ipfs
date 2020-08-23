//! Ipfs path handling following https://github.com/ipfs/go-path/. This rests under ipfs-http for
//! now until it's ready to be moved to `rust-ipfs`. There is a competing implementation under
//! `libipld` which might do almost the same things, but with different dependencies. This should
//! be moving over to `ipfs` once we have seen that this works for `api/v0/dag/get` as well.
//!
//! Does not allow the root to be anything else than `/ipfs/` or missing at the moment.

/*
#[cfg(test)]
mod tests {
    use super::WalkFailed;
    use super::{resolve_segment, WalkSuccess};
    use cid::Cid;
    use ipfs::{ipld::Ipld, make_ipld, IpfsPath};
    use std::convert::TryFrom;

    // good_paths, good_but_unsupported, bad_paths from https://github.com/ipfs/go-path/blob/master/path_test.go

    fn example_doc_and_a_cid() -> (Ipld, Cid) {
        let cid = Cid::try_from("QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n").unwrap();
        let doc = make_ipld!({
            "nested": {
                "even": [
                    {
                        "more": 5
                    },
                    {
                        "or": "this",
                    },
                    {
                        "or": cid.clone(),
                    },
                    {
                        "5": "or",
                    }
                ],
            }
        });
        (doc, cid)
    }

    #[test]
    fn good_walks_on_ipld() {
        let (example_doc, _) = example_doc_and_a_cid();

        let good_examples = [
            (
                "bafyreielwgy762ox5ndmhx6kpi6go6il3gzahz3ngagb7xw3bj3aazeita/nested/even/0/more",
                Ipld::Integer(5),
            ),
            (
                "bafyreielwgy762ox5ndmhx6kpi6go6il3gzahz3ngagb7xw3bj3aazeita/nested/even/1/or",
                Ipld::from("this"),
            ),
            (
                "bafyreielwgy762ox5ndmhx6kpi6go6il3gzahz3ngagb7xw3bj3aazeita/nested/even/3/5",
                Ipld::from("or"),
            ),
        ];

        for (path, expected) in &good_examples {
            let p = IpfsPath::try_from(*path).unwrap();

            // not really the document cid but it doesn't matter; it just needs to be !dag-pb
            //let doc_cid = p.take_root().unwrap();

            // projection
            assert_eq!(
                walk(example_doc.clone(), &p).map(|r| r.0),
                Ok(WalkSuccess::AtDestination(expected.clone()))
            );
        }
    }

    fn walk(mut doc: Ipld, path: &'_ IpfsPath) -> Result<(WalkSuccess, Vec<&'_ str>), WalkFailed> {
        if path.iter().next().is_none() {
            return Ok((WalkSuccess::AtDestination(doc), path.iter().collect()));
        }

        let current = path.root().cid().unwrap();

        if current.codec() == cid::Codec::DagProtobuf {
            return Err(WalkFailed::UnsupportedWalkOnDagPbIpld);
        }

        let mut iter = path.iter();

        loop {
            let needle = if let Some(needle) = iter.next() {
                needle
            } else {
                return Ok((WalkSuccess::AtDestination(doc), Vec::new()));
            };
            doc = match resolve_segment(needle, doc)? {
                WalkSuccess::AtDestination(ipld) => ipld,
                ret @ WalkSuccess::Link(_, _) => return Ok((ret, iter.collect())),
            };
        }
    }
}
*/
