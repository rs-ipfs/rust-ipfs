// The goal of this benchmark was initially to expose a supposed quadratic increase in time when
// adding large files.
//
// The behaviour isn't quadratic, though there is a slowdown observed e.g.:
//
//      size (B):       throughput:
//
//      100             2.06 MBps
//      174             2.07 MBps
//      200             2.00 MBps
//      30000           1.98 MBps
//      30277           1.94 MBps
//      31000           1.98 MBps
//      60000           1.77 MBps
//      60552           1.75 MBps
//      60553           1.74 MBps
//      70000           1.71 MBps
//
//
// And feeding the "add" example (basis for the benchmark below) a 5GB file over stdin:
//
//      0.586 MB in 1.00s or 0.586 MBps
//      0.866 MB in 2.00s or 0.280 MBps
//      1.076 MB in 3.00s or 0.210 MBps
//      1.245 MB in 4.00s or 0.169 MBps
//      1.382 MB in 5.00s or 0.137 MBps
//      1.508 MB in 6.00s or 0.126 MBps
//      1.618 MB in 7.00s or 0.110 MBps
//      1.719 MB in 8.00s or 0.102 MBps
//      1.815 MB in 9.00s or 0.096 MBps
//      1.904 MB in 10.00s or 0.088 MBps
//      1.988 MB in 11.00s or 0.084 MBps
//      2.070 MB in 12.00s or 0.082 MBps
//      2.148 MB in 13.00s or 0.078 MBps
//      2.225 MB in 14.00s or 0.077 MBps
//      2.295 MB in 15.00s or 0.070 MBps
//      2.365 MB in 16.00s or 0.070 MBps
//      2.435 MB in 17.00s or 0.070 MBps
//      2.502 MB in 18.00s or 0.067 MBps
//      2.567 MB in 19.00s or 0.065 MBps
//      2.630 MB in 20.00s or 0.063 MBps
//      2.693 MB in 21.00s or 0.063 MBps
//      2.752 MB in 22.00s or 0.060 MBps
//      2.811 MB in 23.00s or 0.059 MBps
//      2.869 MB in 24.00s or 0.058 MBps

use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use ipfs_unixfs::file::adder::{Chunker, FileAdder};

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("adder");
    group.sample_size(30);

    for size in [175, 30277, 60553].iter() {
        group.sampling_mode(SamplingMode::Flat);
        group.throughput(Throughput::Bytes(*size as u64));

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _size| {
            b.iter(|| run_adder(*size));
        });
    }
}

pub fn run_adder(size: usize) {
    // Setting a small chunker size should exacerbate the issue as the BalanceCollector needs to
    // work harder as a result.
    let chunker = Chunker::Size(1);
    let mut adder = FileAdder::builder().with_chunker(chunker).build();
    let mut total = 0;

    while total < size {
        let (blocks, consumed) = adder.push(&[0]);
        blocks.count();

        total += consumed;
    }
    assert_eq!(total, size);
    adder.finish().count();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
