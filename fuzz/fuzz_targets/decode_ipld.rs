#![no_main]
use libfuzzer_sys::fuzz_target;
use std::convert::TryFrom;

fuzz_target!(|data: (&str, &[u8])| {
    let _ = fuzz(data.0, data.1);
});

fn fuzz(cid: &str, payload: &[u8]) -> Result<(), ()> {
    let cid = ipfs::Cid::try_from(cid).map_err(drop)?;
    let _ = ipfs::ipld::decode_ipld(&cid, payload).map_err(drop)?;
    Ok(())
}
