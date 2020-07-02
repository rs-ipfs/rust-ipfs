use cid::Cid;

use crate::pb::{FlatUnixFs, PBLink, UnixFs, UnixFsType};
use quick_protobuf::{MessageWrite, Writer};
use std::borrow::Cow;
use std::num::NonZeroUsize;

use sha2::{Digest, Sha256};

pub struct FileAdder {
    chunker: Chunker,
    block_buffer: Vec<u8>,
    // the index in the outer vec is the height (0 == leaf, 1 == first links to leafs, 2 == links
    // to first link blocks)
    unflushed_links: Vec<Vec<(Cid, u64, u64)>>,
}

impl std::default::Default for FileAdder {
    fn default() -> Self {
        Self::with_chunker(Chunker::Size(1024 * 256))
    }
}

impl FileAdder {
    pub fn with_chunker(chunker: Chunker) -> Self {
        let hint = chunker.size_hint();
        FileAdder {
            chunker,
            block_buffer: Vec::with_capacity(hint),
            unflushed_links: Default::default(),
        }
    }

    pub fn push(
        &mut self,
        input: &[u8],
    ) -> Result<(impl Iterator<Item = (Cid, Vec<u8>)>, usize), ()> {
        // case 0: full chunk is not ready => empty iterator, full read
        // case 1: full chunk becomes ready, maybe short read => at least one block
        //     1a: not enough links => iterator of one
        //     1b: link block is ready => iterator of two blocks

        let (accepted, ready) = self.chunker.accept(input, &self.block_buffer);
        self.block_buffer.extend_from_slice(accepted);
        let written = accepted.len();

        let (leaf, links) = if !ready {
            // a new block did not become ready, which means we couldn't have gotten a new cid.
            (None, Vec::new())
        } else {
            // a new leaf must be output, as well as possibly a new link block
            let leaf = Some(self.flush_buffered_leaf().unwrap());
            let links = self.flush_buffered_links(NonZeroUsize::new(174).unwrap(), false);

            (leaf, links)
        };

        Ok((leaf.into_iter().chain(links.into_iter()), written))
    }

    pub fn finish(mut self) -> impl Iterator<Item = (Cid, Vec<u8>)> {
        /*

        file    |- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -|
        links#0 |-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|
        links#1 |-------|-------|-------|-------|-------|-------|-------|\   /
        links#2 |-------------------------------|                         ^^^
                                                                    one short

        #finish(...) first iteration:

        file    |- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -|
        links#0 |-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|
        links#1 |-------|-------|-------|-------|-------|-------|-------|==1==|
        links#2 |-------------------------------|==========================2==|

        new blocks #1 and #2

        #finish(...) second iteration:

        file    |- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -|
        links#0 |-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|
        links#1 |-------|-------|-------|-------|-------|-------|-------|--1--|
        links#2 |-------------------------------|--------------------------2--|
        links#3 |==========================================================3==|

        new block #3 (the root block)
        */
        let last_leaf = self.flush_buffered_leaf();
        let root_links = self.flush_buffered_links(NonZeroUsize::new(1).unwrap(), true);
        // should probably error if there is neither?
        last_leaf.into_iter().chain(root_links.into_iter())
    }

    fn flush_buffered_leaf(&mut self) -> Option<(Cid, Vec<u8>)> {
        if self.block_buffer.is_empty() {
            return None;
        }
        let bytes = self.block_buffer.len();

        let inner = FlatUnixFs {
            links: Vec::new(),
            data: UnixFs {
                Type: UnixFsType::File,
                Data: Some(Cow::Borrowed(self.block_buffer.as_slice())),
                filesize: Some(self.block_buffer.len() as u64),
                // no blocksizes as there are no links
                ..Default::default()
            },
        };

        let (cid, vec) = render_and_hash(inner);

        let total_size = vec.len();

        self.block_buffer.clear();

        // leafs always go to the lowest level
        if self.unflushed_links.is_empty() {
            self.unflushed_links.push(Vec::new());
        }

        self.unflushed_links[0].push((cid.clone(), total_size as u64, bytes as u64));

        Some((cid, vec))
    }

    // FIXME: collapse the min_links and all into single type to avoid boolean args
    fn flush_buffered_links(&mut self, min_links: NonZeroUsize, all: bool) -> Vec<(Cid, Vec<u8>)> {
        let mut ret = Vec::new();

        for level in 0.. {
            if !all {
                if self.unflushed_links[level].len() < min_links.get() {
                    break;
                }
            } else {
                if self.unflushed_links[level].len() == 1 {
                    // TODO: combine with above?
                    // we need to break here as otherwise we'd be looping for ever
                    break;
                }
            }

            let mut links = Vec::with_capacity(self.unflushed_links.len());
            let mut blocksizes = Vec::with_capacity(self.unflushed_links.len());

            let mut nested_size = 0;
            let mut nested_total_size = 0;

            for (cid, total_size, block_size) in self.unflushed_links[level].drain(..) {
                links.push(PBLink {
                    Hash: Some(cid.to_bytes().into()),
                    Name: Some("".into()),
                    Tsize: Some(total_size),
                });
                blocksizes.push(block_size);
                nested_total_size += total_size;
                nested_size += block_size;
            }

            let inner = FlatUnixFs {
                links,
                data: UnixFs {
                    Type: UnixFsType::File,
                    filesize: Some(nested_size),
                    blocksizes,
                    ..Default::default()
                },
            };

            let (cid, vec) = render_and_hash(inner);

            if self.unflushed_links.len() <= level + 1 {
                self.unflushed_links.push(Vec::new());
            }

            self.unflushed_links[level + 1].push((
                cid.clone(),
                nested_total_size + vec.len() as u64,
                nested_size,
            ));

            ret.push((cid, vec))
        }

        ret
    }

    #[cfg(test)]
    fn collect_blocks(mut self, all_content: &[u8], mut amt: usize) -> Vec<(Cid, Vec<u8>)> {
        let mut written = 0;
        let mut blocks_received = Vec::new();

        if amt == 0 {
            amt = all_content.len();
        }

        while written < all_content.len() {
            let end = written + (all_content.len() - written).min(amt);
            let slice = &all_content[written..end];

            let (blocks, pushed) = self.push(slice).unwrap();
            blocks_received.extend(blocks);
            written += pushed;
        }

        let last_blocks = self.finish();
        blocks_received.extend(last_blocks);

        blocks_received
    }
}

fn render_and_hash(flat: FlatUnixFs<'_>) -> (Cid, Vec<u8>) {
    // as shown in later dagger we don't really need to render the FlatUnixFs fully; we could
    // either just render a fixed header and continue with the body OR links.
    let mut out = Vec::with_capacity(flat.get_size());
    let mut writer = Writer::new(&mut out);
    flat.write_message(&mut writer)
        .expect("unsure how this could fail");
    let cid = Cid::new_v0(multihash::wrap(
        multihash::Code::Sha2_256,
        &Sha256::digest(&out),
    ))
    .unwrap();
    (cid, out)
}

pub enum Chunker {
    Size(usize),
}

impl std::default::Default for Chunker {
    fn default() -> Self {
        Chunker::Size(256 * 1024)
    }
}

impl Chunker {
    fn accept<'a>(&mut self, input: &'a [u8], buffered: &[u8]) -> (&'a [u8], bool) {
        use Chunker::*;

        match self {
            Size(max) => {
                let l = input.len().min(*max - buffered.len());
                let accepted = &input[..l];
                let ready = buffered.len() + l >= *max;
                (accepted, ready)
            }
        }
    }

    fn size_hint(&self) -> usize {
        use Chunker::*;

        match self {
            Size(max) => *max,
        }
    }
}

#[cfg(test)]
mod tests {

    use super::{Chunker, FileAdder};
    use crate::test_support::FakeBlockstore;
    use cid::Cid;
    use std::convert::TryFrom;

    #[test]
    fn test_size_chunker() {
        assert_eq!(size_chunker_scenario(1, 4, 0), (1, true));
        assert_eq!(size_chunker_scenario(2, 4, 0), (2, true));
        assert_eq!(size_chunker_scenario(2, 1, 0), (1, false));
        assert_eq!(size_chunker_scenario(2, 1, 1), (1, true));
        assert_eq!(size_chunker_scenario(32, 3, 29), (3, true));
        // this took some debugging time:
        assert_eq!(size_chunker_scenario(32, 4, 29), (3, true));
    }

    fn size_chunker_scenario(max: usize, input_len: usize, existing_len: usize) -> (usize, bool) {
        let input = vec![0; input_len];
        let existing = vec![0; existing_len];

        let (accepted, ready) = Chunker::Size(max).accept(&input, &existing);
        (accepted.len(), ready)
    }

    #[test]
    fn favourite_single_block_file() {
        let blocks = FakeBlockstore::with_fixtures();
        // everyones favourite content
        let content = b"foobar\n";

        let mut adder = FileAdder::default();

        {
            let (mut ready_blocks, bytes) = adder.push(content).unwrap();
            assert!(ready_blocks.next().is_none());
            assert_eq!(bytes, content.len());
        }

        // real impl would probably hash this ... except maybe hashing is faster when done inline?
        // or maybe not
        let (_, file_block) = adder
            .finish()
            .next()
            .expect("there must have been the root block");

        assert_eq!(
            blocks.get_by_str("QmRgutAxd8t7oGkSm4wmeuByG6M51wcTso6cubDdQtuEfL"),
            file_block.as_slice()
        );
    }

    #[test]
    fn favourite_multi_block_file() {
        // root should be QmRJHYTNvC3hmd9gJQARxLR1QMEincccBV53bBw524yyq6

        let blocks = FakeBlockstore::with_fixtures();
        let content = b"foobar\n";
        let adder = FileAdder::with_chunker(Chunker::Size(2));

        let blocks_received = adder.collect_blocks(content, 0);

        // the order here is "fo", "ob", "ar", "\n", root block
        // while verifying the root Cid would be *enough* this is easier to eyeball, ... not really
        // that much but ...
        let expected = [
            "QmfVyMoStzTvdnUR7Uotzh82gmL427q9z3xW5Y8fUoszi4",
            "QmdPyW4CWE3QBkgjWfjM5f7Tjb3HukxVuBXZtkqAGwsMnm",
            "QmNhDQpphvMWhdCzP74taRzXDaEfPGq8vWfFRzD7mEgePM",
            "Qmc5m94Gu7z62RC8waSKkZUrCCBJPyHbkpmGzEePxy2oXJ",
            "QmRJHYTNvC3hmd9gJQARxLR1QMEincccBV53bBw524yyq6",
        ]
        .iter()
        .map(|key| {
            let cid = Cid::try_from(*key).unwrap();
            let block = blocks.get_by_str(key).to_vec();
            (cid, block)
        })
        .collect::<Vec<_>>();

        assert_eq!(blocks_received, expected);
    }

    #[test]
    fn three_layers() {
        let content = b"Lorem ipsum dolor sit amet, sit enim montes aliquam. Cras non lorem, \
            rhoncus condimentum, irure et ante. Pulvinar suscipit odio ante, et tellus a enim, \
            wisi ipsum, vel rhoncus eget faucibus varius, luctus turpis nibh vel odio nulla pede.";

        assert!(content.len() > 174 && content.len() < 2 * 174);

        // go-ipfs 0.5 result: QmRQ6NZNUs4JrCT2y7tmCC1wUhjqYuTssB8VXbbN3rMffg, 239 blocks and root
        // root has two links:
        // QmXUcuLGKc8SCMEqG4wgct6NKsSRZQfvB2FCfjDow1PfpB and QmeEn8dxWTzGAFKvyXoLj4oWbh9putL4vSw4uhLXJrSZhs
        // left has 174 links, right has 63 links

        // in future, if we ever add inline Cid generation this test would need to be changed not
        // to use those inline cids or raw leaves
        let adder = FileAdder::with_chunker(Chunker::Size(1));

        let blocks_received = adder.collect_blocks(content, 0);

        assert_eq!(blocks_received.len(), 240);

        assert_eq!(
            blocks_received.last().unwrap().0.to_string(),
            "QmRQ6NZNUs4JrCT2y7tmCC1wUhjqYuTssB8VXbbN3rMffg"
        );
    }

    #[test]
    #[ignore]
    fn three_layers_all_subchunks() {
        let content = b"Lorem ipsum dolor sit amet, sit enim montes aliquam. Cras non lorem, \
            rhoncus condimentum, irure et ante. Pulvinar suscipit odio ante, et tellus a enim, \
            wisi ipsum, vel rhoncus eget faucibus varius, luctus turpis nibh vel odio nulla pede.";

        for amt in 1..32 {
            let adder = FileAdder::with_chunker(Chunker::Size(32));
            let blocks_received = adder.collect_blocks(content, amt);
            assert_eq!(
                blocks_received.last().unwrap().0.to_string(),
                "QmYSLcVQqxKygiq7x9w1XGYxU29EShB8ZemiaQ8GAAw17h",
                "amt: {}",
                amt
            );
        }
    }
}
