use cid::Cid;
use ipfs_unixfs::adder::FileAdder;
use std::fmt;
use std::io::BufRead;
use std::time::Duration;

fn main() {
    // read stdin, maybe produce stdout car?

    let stdin = std::io::stdin();
    let mut stdin = stdin.lock();

    let mut adder = FileAdder::default();
    let mut stats = Stats::default();

    let mut input = 0;

    let start = std::time::Instant::now();

    loop {
        match stdin.fill_buf().unwrap() {
            x if x.is_empty() => {
                let blocks = adder.finish();
                stats.process(blocks);
                break;
            }
            x => {
                let (blocks, consumed) = adder.push(x).expect("no idea what could fail here?");
                stdin.consume(consumed);
                stats.process(blocks);

                input += consumed;
            }
        }
    }

    let process_stats = get_process_stats();

    eprintln!("{}", stats);

    let total = start.elapsed();

    if let Some(process_stats) = process_stats {
        eprint!("{}, ", process_stats);
    }

    eprintln!("total: {:?}", total);

    let megabytes = 1024.0 * 1024.0;

    eprintln!(
        "Input: {} MB/s",
        (input as f64 / megabytes) / total.as_secs_f64()
    );

    eprintln!(
        "Output: {} MB/s",
        (stats.block_bytes as f64 / megabytes) / total.as_secs_f64()
    );
}

struct ProcessStats {
    user_time: Duration,
    system_time: Duration,
    max_rss: i64,
}

impl fmt::Display for ProcessStats {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            fmt,
            "Max RSS: {} KB, utime: {:?}, stime: {:?}",
            self.max_rss, self.user_time, self.system_time
        )
    }
}

#[cfg(unix)]
fn get_process_stats() -> Option<ProcessStats> {
    fn to_duration(tv: libc::timeval) -> Duration {
        assert!(tv.tv_sec >= 0);
        Duration::new(tv.tv_sec as u64, tv.tv_usec as u32)
    }

    let (max_rss, user_time, system_time) = unsafe {
        let mut rusage: libc::rusage = std::mem::zeroed();

        let retval = libc::getrusage(libc::RUSAGE_SELF, &mut rusage as *mut _);

        assert_eq!(retval, 0);

        (rusage.ru_maxrss, rusage.ru_utime, rusage.ru_stime)
    };

    let user_time = to_duration(user_time);
    let system_time = to_duration(system_time);

    Some(ProcessStats {
        user_time,
        system_time,
        max_rss,
    })
}

#[cfg(not(unix))]
fn get_process_stats() -> Option<ProcessStats> {
    None
}

#[derive(Default)]
struct Stats {
    blocks: usize,
    block_bytes: u64,
    last: Option<Cid>,
}

impl fmt::Display for Stats {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.last.as_ref() {
            Some(cid) => write!(
                fmt,
                "{} blocks, {} block bytes, last_cid: {}",
                self.blocks, self.block_bytes, cid
            ),
            None => write!(
                fmt,
                "{} blocks, {} block bytes",
                self.blocks, self.block_bytes
            ),
        }
    }
}

impl Stats {
    fn process<I: Iterator<Item = (Cid, Vec<u8>)>>(&mut self, new_blocks: I) {
        for (cid, block) in new_blocks {
            self.last = Some(cid);
            self.blocks += 1;
            self.block_bytes += block.len() as u64;
        }
    }
}
