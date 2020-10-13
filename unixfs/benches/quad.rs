use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use ipfs_unixfs::file::adder::{Chunker, FileAdder};

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("quad");
    group.sample_size(30);

    for size in [
        100, 174, 200, 30000, 30276, 30277, 31000, 60000, 60552, 60553, 70000,
    ]
    .iter()
    {
        group.sampling_mode(SamplingMode::Flat);
        group.throughput(Throughput::Bytes(*size as u64));

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _size| {
            b.iter(|| quadratic_flush(*size));
        });
    }
}

pub fn quadratic_flush(size: usize) {
    // Goal: benchmark FileAdder.push to expose quadratic behavior. The benchmark should be done with
    // different file sizes with a small constant chunk size.
    //
    // Steps:
    //
    // 1. create file (vec of bytes); this should be variable
    // 2. create FileAdder and set chunk size
    // 3. measure time it takes to create blocks

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
