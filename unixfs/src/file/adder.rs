use cid::Cid;

use crate::pb::{FlatUnixFs, PBLink, UnixFs, UnixFsType};
use quick_protobuf::{MessageWrite, Writer};
use std::borrow::Cow;
use std::fmt;

use sha2::{Digest, Sha256};

/// File tree builder
pub struct FileAdder {
    chunker: Chunker,
    collector: Collector,
    block_buffer: Vec<u8>,
    // all unflushed links as a flat vec; this is compacted as we grow and need to create a link
    // block for the last N blocks, as decided by the collector
    unflushed_links: Vec<(usize, Cid, u64, u64)>,
}

impl std::default::Default for FileAdder {
    fn default() -> Self {
        Self::with_chunker(Chunker::Size(1024 * 256))
    }
}

impl fmt::Debug for FileAdder {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            fmt,
            "FileAdder {{ chunker: {:?}, block_buffer: {}/{}, unflushed_links: {} }}",
            self.chunker,
            self.block_buffer.len(),
            self.block_buffer.capacity(),
            self.unflushed_links
                .iter()
                .fold(Vec::new(), |mut acc, (depth, ..)| {
                    match acc.last_mut() {
                        Some((other_depth, ctr)) if other_depth == &depth => *ctr += 1,
                        Some(_) | None => {
                            acc.push((depth, 1));
                        }
                    }
                    acc
                })
                .into_iter()
                .map(|(_depth, count)| count.to_string())
                .collect::<Vec<_>>()
                .join("/")
        )
    }
}

impl FileAdder {
    /// Creates `FileAdder` with the given chunker. Typically one could just call
    /// `FileAdder::default()`.
    pub fn with_chunker(chunker: Chunker) -> Self {
        let hint = chunker.size_hint();
        FileAdder {
            chunker,
            collector: Default::default(),
            block_buffer: Vec::with_capacity(hint),
            unflushed_links: Default::default(),
        }
    }

    /// Returns a likely amount of buffering the file adding works the best.
    pub fn size_hint(&self) -> usize {
        self.chunker.size_hint()
    }

    /// Called to push new file bytes into the tree builder.
    ///
    /// Returns the newly created blocks (at most 2) and their respective Cids, and the amount of
    /// `input` consumed.
    pub fn push(
        &mut self,
        input: &[u8],
    ) -> Result<(impl Iterator<Item = (Cid, Vec<u8>)>, usize), ()> {
        // case 0: full chunk is not ready => empty iterator, full read
        // case 1: full chunk becomes ready, maybe short read => at least one block
        //     1a: not enough links => iterator of one
        //     1b: link block is ready => iterator of two blocks

        let (accepted, ready) = self.chunker.accept(input, &self.block_buffer);
        if self.block_buffer.is_empty() && ready {
            // save single copy as the caller is giving us whole chunks.
            //
            // TODO: though, this path does make one question if there is any point in keeping
            // block_buffer and chunker here; perhaps FileAdder should only handle pre-chunked
            // blocks and user takes care of chunking (and buffering)?
            let leaf = Some(
                Self::flush_buffered_leaf(accepted, &mut self.unflushed_links, false)
                    .expect("chunker filled a block, must produce one"),
            );
            self.block_buffer.clear();
            let links = self.flush_buffered_links(false);
            Ok((leaf.into_iter().chain(links.into_iter()), accepted.len()))
        } else {
            // slower path as we manage the buffer.
            self.block_buffer.extend_from_slice(accepted);
            let written = accepted.len();

            let (leaf, links) = if !ready {
                // a new block did not become ready, which means we couldn't have gotten a new cid.
                (None, Vec::new())
            } else {
                // a new leaf must be output, as well as possibly a new link block
                let leaf = Some(
                    Self::flush_buffered_leaf(
                        self.block_buffer.as_slice(),
                        &mut self.unflushed_links,
                        false,
                    )
                    .expect("chunker filled a block, must produce one"),
                );
                self.block_buffer.clear();
                let links = self.flush_buffered_links(false);

                (leaf, links)
            };
            Ok((leaf.into_iter().chain(links.into_iter()), written))
        }
    }

    /// Called after the last [`FileAdder::push`] to finish the tree construction.
    ///
    /// Returns a list of Cids and their respective blocks.
    ///
    /// Note: the API will hopefully evolve to a direction which would not allocate new Vec for
    /// every block in the near-ish future.
    pub fn finish(mut self) -> impl Iterator<Item = (Cid, Vec<u8>)> {
        let last_leaf = Self::flush_buffered_leaf(
            &self.block_buffer.as_slice(),
            &mut self.unflushed_links,
            true,
        );
        let root_links = self.flush_buffered_links(true);
        // should probably error if there is neither?
        last_leaf.into_iter().chain(root_links.into_iter())
    }

    /// Returns `None` when the input is empty but there are links, otherwise a new Cid and a
    /// block.
    fn flush_buffered_leaf(
        input: &[u8],
        unflushed_links: &mut Vec<(usize, Cid, u64, u64)>,
        finishing: bool,
    ) -> Option<(Cid, Vec<u8>)> {
        if input.is_empty() {
            if !finishing || !unflushed_links.is_empty() {
                return None;
            }
        }

        let bytes = input.len();

        // for empty unixfs file the bytes is missing but filesize is present.

        let data = if bytes > 0 {
            Some(Cow::Borrowed(input))
        } else {
            None
        };

        let filesize = Some(bytes as u64);

        let inner = FlatUnixFs {
            links: Vec::new(),
            data: UnixFs {
                Type: UnixFsType::File,
                Data: data,
                filesize,
                // no blocksizes as there are no links
                ..Default::default()
            },
        };

        let (cid, vec) = render_and_hash(&inner);

        let total_size = vec.len();

        unflushed_links.push((0, cid.clone(), total_size as u64, bytes as u64));

        Some((cid, vec))
    }

    fn flush_buffered_links(&mut self, finishing: bool) -> Vec<(Cid, Vec<u8>)> {
        self.collector
            .flush_links(&mut self.unflushed_links, finishing)
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

fn render_and_hash(flat: &FlatUnixFs<'_>) -> (Cid, Vec<u8>) {
    // as shown in later dagger we don't really need to render the FlatUnixFs fully; we could
    // either just render a fixed header and continue with the body OR links, though the links are
    // a bit more complicated.
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

/// Chunker strategy
#[derive(Debug)]
pub enum Chunker {
    /// Size based chunking
    Size(usize),
}

impl std::default::Default for Chunker {
    /// Returns a default chunker which matches go-ipfs 0.6
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

/// Collector or layout strategy
#[derive(Debug)]
pub enum Collector {
    /// Balanced trees.
    Balanced(BalancedCollector),
}

impl std::default::Default for Collector {
    fn default() -> Self {
        Collector::Balanced(Default::default())
    }
}

impl Collector {
    fn flush_links(
        &mut self,
        pending: &mut Vec<(usize, Cid, u64, u64)>,
        finishing: bool,
    ) -> Vec<(Cid, Vec<u8>)> {
        use Collector::*;

        match self {
            Balanced(bc) => bc.flush_links(pending, finishing),
        }
    }
}

/// BalancedCollector creates balanced UnixFs trees, most optimized for random access to different
/// parts of the file. Currently supports only link count threshold or the branching factor.
pub struct BalancedCollector {
    branching_factor: usize,
    // reused between link block generation
    reused_links: Vec<PBLink<'static>>,
    // reused between link block generation
    reused_blocksizes: Vec<u64>,
}

impl fmt::Debug for BalancedCollector {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            fmt,
            "BalancedCollector {{ branching_factor: {} }}",
            self.branching_factor
        )
    }
}

impl std::default::Default for BalancedCollector {
    fn default() -> Self {
        Self::with_branching_factor(174)
    }
}

impl BalancedCollector {
    /// Configure Balanced collector with the given branching factor.
    pub fn with_branching_factor(branching_factor: usize) -> Self {
        assert!(branching_factor > 0);

        Self {
            branching_factor,
            reused_links: Vec::new(),
            reused_blocksizes: Vec::new(),
        }
    }

    fn flush_links(
        &mut self,
        pending: &mut Vec<(usize, Cid, u64, u64)>,
        finishing: bool,
    ) -> Vec<(Cid, Vec<u8>)> {
        /*

        file    |- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -|
        links#0 |-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|
        links#1 |-------|-------|-------|-------|-------|-------|-------|\   /
        links#2 |-------------------------------|                         ^^^
                                                                    one short

        #flush_buffered_links(...) first iterations:

        file    |- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -|
        links#0 |-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|
        links#1 |-------|-------|-------|-------|-------|-------|-------|==1==|
        links#2 |-------------------------------|==========================2==|

        new blocks #1 and #2

        #flush_buffered_links(...) last iteration:

        file    |- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -|
        links#0 |-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|
        links#1 |-------|-------|-------|-------|-------|-------|-------|--1--|
        links#2 |-------------------------------|--------------------------2--|
        links#3 |==========================================================3==|

        new block #3 (the root block)
        */

        let mut ret = Vec::new();

        let mut reused_links = std::mem::take(&mut self.reused_links);
        let mut reused_blocksizes = std::mem::take(&mut self.reused_blocksizes);

        if let Some(need) = self.branching_factor.checked_sub(reused_links.capacity()) {
            reused_links.reserve(need);
        }

        if let Some(need) = self
            .branching_factor
            .checked_sub(reused_blocksizes.capacity())
        {
            reused_blocksizes.reserve(need);
        }

        for level in 0.. {
            if pending.len() == 1 && finishing
                || pending.len() < self.branching_factor && !finishing
            {
                // when there is just a single linking block left and we are finishing, we are
                // done. It might not be part of the `ret` as will be the case with single chunk
                // files for example.
                //
                // normally when not finishing we do nothing if we don't have enough links.
                break;
            }

            // this could be optimized... perhaps by maintaining an index structure?
            let first_at = pending.iter().position(|(lvl, ..)| lvl == &level);

            if let Some(first_at) = first_at {
                let to_compress = pending[first_at..].len();

                if to_compress < self.branching_factor && !finishing {
                    // when not finishing recheck if we have enough work to do as we may have had
                    // earlier higher level links which skipped, but we are still awaiting for a
                    // full link block
                    break;
                }

                reused_links.clear();
                reused_blocksizes.clear();

                let mut nested_size = 0;
                let mut nested_total_size = 0;

                for (depth, cid, total_size, block_size) in pending.drain(first_at..) {
                    assert_eq!(depth, level);

                    reused_links.push(PBLink {
                        Hash: Some(cid.to_bytes().into()),
                        Name: Some("".into()),
                        Tsize: Some(total_size),
                    });
                    reused_blocksizes.push(block_size);
                    nested_total_size += total_size;
                    nested_size += block_size;
                }

                let inner = FlatUnixFs {
                    links: reused_links,
                    data: UnixFs {
                        Type: UnixFsType::File,
                        filesize: Some(nested_size),
                        blocksizes: reused_blocksizes,
                        ..Default::default()
                    },
                };

                let (cid, vec) = render_and_hash(&inner);

                pending.push((
                    level + 1,
                    cid.clone(),
                    nested_total_size + vec.len() as u64,
                    nested_size,
                ));

                ret.push((cid, vec));

                reused_links = inner.links;
                reused_links.clear();

                reused_blocksizes = inner.data.blocksizes;
                reused_blocksizes.clear();
            }
        }

        self.reused_links = reused_links;
        self.reused_blocksizes = reused_blocksizes;

        ret
    }
}

#[cfg(test)]
mod tests {

    use super::{Chunker, FileAdder};
    use crate::test_support::FakeBlockstore;
    use cid::Cid;
    use hex_literal::hex;
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
        //  - QmXUcuLGKc8SCMEqG4wgct6NKsSRZQfvB2FCfjDow1PfpB (174 links)
        //  - QmeEn8dxWTzGAFKvyXoLj4oWbh9putL4vSw4uhLXJrSZhs (63 links)
        //
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

    #[test]
    fn empty_file() {
        let blocks = FileAdder::default().collect_blocks(b"", 0);
        assert_eq!(blocks.len(), 1);
        // 0a == field dag-pb body (unixfs)
        // 04 == dag-pb body len, varint, 4 bytes
        // 08 == field type tag, varint, 1 byte
        // 02 == field type (File)
        // 18 == field data tag, varlen
        // 00 == data length, varint, 1 byte
        assert_eq!(blocks[0].1.as_slice(), &hex!("0a 04 08 02 18 00"));
        assert_eq!(
            blocks[0].0.to_string(),
            "QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH"
        );
    }

    #[test]
    fn full_link_block_and_a_byte() {
        let buf = vec![0u8; 2];

        // this should produce a root with two links
        //             +----------^---+
        //             |              |
        //  |----------------------| |-| <-- link blocks
        //   ^^^^^^^^^^^^^^^^^^^^^^   ^
        //          174 blocks        \--- 1 block

        let mut adder = FileAdder::with_chunker(Chunker::Size(2));
        let mut blocks_count = 0;

        for _ in 0..174 {
            let (blocks, written) = adder.push(buf.as_slice()).unwrap();
            assert_eq!(written, buf.len());

            // do not collect the blocks because this is some 45MB
            blocks_count += blocks.count();
        }

        let (blocks, written) = adder.push(&buf[0..1]).unwrap();
        assert_eq!(written, 1);
        blocks_count += blocks.count();

        let last_blocks = adder.finish().collect::<Vec<_>>();
        blocks_count += last_blocks.len();

        // chunks == 174
        // one link block for 174
        // one is for the single byte block
        // one is a link block for the singular single byte block
        // other is for the root block
        assert_eq!(blocks_count, 174 + 1 + 1 + 1 + 1);

        assert_eq!(
            last_blocks.last().unwrap().0.to_string(),
            "QmcHNWF1d56uCDSfJPA7t9fadZRV9we5HGSTGSmwuqmMP9"
        );
    }
}
