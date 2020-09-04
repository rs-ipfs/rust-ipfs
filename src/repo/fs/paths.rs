use cid::Cid;
use core::convert::TryFrom;
use std::path::PathBuf;

pub fn block_path(mut base: PathBuf, cid: &Cid) -> PathBuf {
    // this is ascii always, and wasteful until we can drop the cid for multihash ... which is
    // probably soon, we just need turn /refs/local to use /pin/list.
    let key = if cid.version() == cid::Version::V1 {
        cid.to_string()
    } else {
        Cid::new_v1(cid.codec(), cid.hash().to_owned()).to_string()
    };

    shard(&mut base, &key);

    base.set_extension("data");
    base
}

pub fn filestem_to_block_cid(file_stem: Option<&std::ffi::OsStr>) -> Option<Cid> {
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

        cid.ok()
    })
}

/// Same as `block_path` except it doesn't canonicalize the cid to later version. The produced
/// filename must be converted back to `Cid` using [`filestem_to_pin_cid`].
pub fn pin_path(mut base: PathBuf, cid: &Cid) -> PathBuf {
    // it might be illegal to to render cidv0 as base32
    let key: String = multibase::Base::Base32Lower.encode(cid.to_bytes());
    shard(&mut base, &key);
    base
}

/// Decodes the file stem produced by [`pin_path`], ignoring errors.
pub fn filestem_to_pin_cid(file_stem: Option<&std::ffi::OsStr>) -> Option<Cid> {
    file_stem.and_then(|stem| stem.to_str()).and_then(|s| {
        // this isn't an interchangeable way to store esp. cidv0
        let bytes = multibase::Base::Base32Lower.decode(s).ok()?;
        let cid = Cid::try_from(bytes);

        // See filestem_to_block_cid for discusison on why the error is ignored
        cid.ok()
    })
}

/// second-to-last/2 sharding, just by taking the two characters from suffix ignoring the last
/// character from an ASCII encoded key string to be prepended as the directory or "shard".
///
/// This is done so that the directories don't get
/// gazillion files in them, which would slow them down. For example, git does this with hex or
/// base16 representation of sha1.
///
/// This function does not care how the key has been encoded, it is enough to have ASCII characters
/// where the shard is selected.
fn shard(path: &mut PathBuf, key: &str) {
    let start = key.len() - 3;
    let shard = &key[start..start + 2];
    assert_eq!(key[start + 2..].len(), 1);
    path.push(shard);
    path.push(key);
}

#[cfg(test)]
mod tests {

    use super::shard;
    use cid::Cid;
    use std::convert::TryFrom;
    use std::path::{Path, PathBuf};

    #[test]
    fn cid_v0_to_pin_and_back() {
        roundtrip_pin_path(
            "QmTEn8ypAkbJXZUXCRHBorwF2jM8uTUW9yRLzrcQouSoD4",
            "some_root",
            "some_root/2w/ciqerskzqa5kvny63dm6byuerdsbkiadrnzlerphz2zdcpdvozuh2wy",
        );
    }

    #[test]
    fn cid_v1_to_pin_and_back() {
        roundtrip_pin_path(
            "bafybeicizfmyaovkw4pnrwpa4kcirzaveabyw4vsixt45mrrhr2xm2d5lm",
            "some_root",
            "some_root/5l/afybeicizfmyaovkw4pnrwpa4kcirzaveabyw4vsixt45mrrhr2xm2d5lm",
        );
    }

    fn roundtrip_pin_path(cid: &str, base: &str, expected_path: &str) {
        let cid = Cid::try_from(cid).unwrap();

        let path = super::pin_path(PathBuf::from(base), &cid);

        assert_eq!(path, Path::new(expected_path));

        let parsed_cid = super::filestem_to_pin_cid(path.file_stem());

        assert_eq!(parsed_cid, Some(cid));
    }

    #[test]
    fn cid_to_block_path() {
        // block_path canonicalizes the path; not sure if there's any point nor does it really
        // match how the locking is done (through the multihash) but ... It's close. not spending
        // time fixing this right now but hopefully moving over to storing just multihashes soon.

        let cid_v0 = "QmTEn8ypAkbJXZUXCRHBorwF2jM8uTUW9yRLzrcQouSoD4";
        let cid_v0 = Cid::try_from(cid_v0).unwrap();
        let cid_v1 = "bafybeicizfmyaovkw4pnrwpa4kcirzaveabyw4vsixt45mrrhr2xm2d5lm";
        let cid_v1 = Cid::try_from(cid_v1).unwrap();

        let base = PathBuf::from("another_root");

        let cid_v0_path = super::block_path(base.clone(), &cid_v0);
        let cid_v1_path = super::block_path(base, &cid_v1);

        assert_eq!(cid_v0_path, cid_v1_path);

        // the blockstore has only one kind of file (plus the temp file) so data extension is
        // included always.
        let expected =
            "another_root/5l/bafybeicizfmyaovkw4pnrwpa4kcirzaveabyw4vsixt45mrrhr2xm2d5lm.data";

        assert_eq!(cid_v1_path, Path::new(expected));
    }

    #[test]
    fn block_path_to_cid() {
        let cid_v1 = "bafybeicizfmyaovkw4pnrwpa4kcirzaveabyw4vsixt45mrrhr2xm2d5lm";
        let cid_v1 = Cid::try_from(cid_v1).unwrap();

        let path =
            "another_root/5l/bafybeicizfmyaovkw4pnrwpa4kcirzaveabyw4vsixt45mrrhr2xm2d5lm.data";
        let path = Path::new(path);

        let parsed = super::filestem_to_block_cid(path.file_stem());

        assert_eq!(parsed, Some(cid_v1));
    }

    #[test]
    fn invalid_block_path_is_silently_ignored() {
        let block_path = Path::new("another_root/ba/foobar.data");
        assert_eq!(super::filestem_to_block_cid(block_path.file_stem()), None);
    }

    #[test]
    fn invalid_pin_path_is_silently_ignored() {
        let pin_path = Path::new("another_root/ca/foocar.recursive");
        assert_eq!(super::filestem_to_block_cid(pin_path.file_stem()), None);
    }

    #[test]
    fn shard_example() {
        let mut path = PathBuf::from("some_root");
        let key = "ABCDEFG";

        shard(&mut path, key);

        let expected = Path::new("some_root/EF/ABCDEFG");
        assert_eq!(path, expected);
    }
}
