use cid::Cid;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use hash_hasher::HashBuildHasher;
use multihash::Sha2_256;
use std::{
    collections::{hash_map::RandomState, HashMap},
    fmt::Formatter,
    hash::BuildHasher,
    vec::Vec,
};

#[derive(Debug, Clone)]
struct Cids(Vec<Cid>);

const GETS_PER_INSERT: usize = 10;

fn insert_get<H: BuildHasher + Default>(cids: &Cids) {
    let mut h = HashMap::<Cid, usize, H>::default();
    cids.0.iter().enumerate().for_each(|(i, c)| {
        h.insert(c.clone(), i);
    });

    cids.0.iter().enumerate().for_each(|(_, c)| {
        for _ in 0..GETS_PER_INSERT {
            h.get(c).unwrap();
        }
    });
}

impl std::fmt::Display for Cids {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "[{} elems]", self.0.len())
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let sizes: Vec<usize> = vec![10000];
    let cids: Vec<Cids> = sizes
        .iter()
        .map(|size| {
            Cids(
                (0..*size)
                    .into_iter()
                    .map(|i| Cid::new_v0(Sha2_256::digest(&i.to_be_bytes())).unwrap())
                    .collect(),
            )
        })
        .collect();

    let mut group = c.benchmark_group("hashed-map-cid");

    for c in cids {
        group.bench_with_input(BenchmarkId::new("HashMap", c.clone()), &c, |b, c| {
            b.iter(|| insert_get::<RandomState>(c));
        });

        group.bench_with_input(BenchmarkId::new("HashedMap", c.clone()), &c, |b, c| {
            b.iter(|| insert_get::<HashBuildHasher>(c));
        });
    }

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
