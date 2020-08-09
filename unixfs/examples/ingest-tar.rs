use cid::Cid;
use ipfs_unixfs::dir::builder::{BufferingTreeBuilder, TreeOptions};
use ipfs_unixfs::file::adder::FileAdder;
use std::fmt;
use std::io::Read;
use std::time::{Duration, Instant};

fn main() {
    let started = Instant::now();

    let stdin = std::io::stdin();
    let stdin = stdin.lock();

    let mut archive = tar::Archive::new(stdin);
    let entries = archive.entries().unwrap();

    let mut buffer = Vec::new();

    let mut opts = TreeOptions::default();
    opts.wrap_with_directory();
    let mut tree = BufferingTreeBuilder::new(opts);

    for entry in entries {
        let mut entry = entry.unwrap();
        let path = std::str::from_utf8(&*entry.path_bytes())
            .unwrap()
            .to_string(); // need to get rid of this

        if let Some(_link_name) = entry.link_name_bytes() {
            continue;
        }

        if !path.ends_with('/') {
            let mut adder = FileAdder::default();

            // with the std::io::Read it'd be good to read into the fileadder, or read into ...
            // something. trying to acccess the buffer from in side FileAdder does not seem the be the
            // way to go.

            if let Some(needed) = adder.size_hint().checked_sub(buffer.capacity()) {
                buffer.reserve(needed);
            }

            if let Some(mut needed) = adder.size_hint().checked_sub(buffer.len()) {
                let zeros = [0u8; 64];

                while needed > zeros.len() {
                    buffer.extend_from_slice(&zeros[..]);
                    needed -= zeros.len();
                }

                buffer.extend(std::iter::repeat(0).take(needed));
            }

            let mut total_written = 0usize;

            loop {
                match entry.read(&mut buffer[0..]).unwrap() {
                    0 => {
                        let blocks = adder.finish();
                        let (cid, subtotal) = blocks
                            .fold(
                                None,
                                |acc: Option<(Cid, usize)>, (cid, bytes): (Cid, Vec<u8>)| match acc
                                {
                                    Some((_, total)) => Some((cid, total + bytes.len())),
                                    None => Some((cid, bytes.len())),
                                },
                            )
                            .expect("this is probably always present");

                        total_written += subtotal;

                        tree.put_file(&path, cid, total_written as u64).unwrap();
                        break;
                    }
                    n => {
                        let mut read = 0;
                        while read < n {
                            let (blocks, consumed) = adder.push(&buffer[read..n]);
                            read += consumed;
                            total_written += blocks.map(|(_, bytes)| bytes.len()).sum::<usize>();
                        }
                    }
                }
            }
        } else {
            tree.set_metadata(&path[..path.len() - 1], ipfs_unixfs::Metadata::default())
                .unwrap();
        }
    }

    let mut iter = tree.build();

    let mut last: Option<(Cid, u64, usize)> = None;

    while let Some(res) = iter.next_borrowed() {
        let res = res.unwrap();

        match &mut last {
            Some(ref mut s) => {
                s.0 = res.cid.to_owned();
                s.1 = res.total_size;
                s.2 = res.block.len();
            }
            n @ None => {
                *n = Some((res.cid.to_owned(), res.total_size, res.block.len()));
            }
        }
    }

    let last = last.unwrap();

    println!("{} ({} bytes), total: {} bytes", last.0, last.2, last.1);

    let process_stats = get_process_stats(started);

    match process_stats {
        Ok(all) => eprintln!("{}", all),
        Err(wall) => eprintln!("wall_time: {:?}", wall),
    }
}

struct ProcessStats {
    user_time: Duration,
    system_time: Duration,
    max_rss: i64,
    wall_time: Duration,
}

impl fmt::Display for ProcessStats {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            fmt,
            "Max RSS: {} KB, utime: {:?}, stime: {:?}, total: {:?}, wall_time: {:?}",
            self.max_rss,
            self.user_time,
            self.system_time,
            self.user_time + self.system_time,
            self.wall_time,
        )
    }
}

#[cfg(unix)]
fn get_process_stats(started_at: Instant) -> Result<ProcessStats, Duration> {
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
    let wall_time = started_at.elapsed();

    Ok(ProcessStats {
        user_time,
        system_time,
        max_rss,
        wall_time,
    })
}

#[cfg(not(unix))]
fn get_process_stats(started_at: Instant) -> Result<ProcessStats, Duration> {
    Err(started_at.elapsed())
}
