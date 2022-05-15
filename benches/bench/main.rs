use std::collections::HashSet;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use phash::{chd::*, value::*, *};
use rand::{distributions::Alphanumeric, prelude::SliceRandom, Rng};

const BENCH_FILE: &'static str = "./bench.bin";
const DEFAULT_LEN: usize = 50_0000;

fn init_data(cfg: CHDGeneratorConfig, len: usize) -> Vec<(String, String)> {
    let mut map = HashSet::new();
    let mut keys = Vec::new();
    let mut rng = rand::thread_rng();
    for _ in 0..len {
        loop {
            let key_len = rng.gen_range(4..20);
            let s: String = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(key_len)
                .map(char::from)
                .collect();
            if map.contains(&s) {
                continue;
            }
            map.insert(s.clone());
            keys.push((s.clone(), s));
            break;
        }
    }
    let mut serializer = PerfectHashMapSerializer::<hasher::CityHash, _, _, _>::new(
        CHDGenerator::from_config(cfg),
        DefaultHashValueWriter::new(),
    );
    let tmp_keys: Vec<(&str, &[u8])> = keys
        .iter()
        .map(|v| (v.0.as_str(), v.1.as_bytes()))
        .collect();
    serializer.write_to_file(&tmp_keys, BENCH_FILE);
    keys
}

fn test_lookup(c: &mut Criterion) {
    let config = CHDGeneratorConfig::default().load_factor(0.5f32);
    let mut keys = init_data(config.clone(), DEFAULT_LEN);
    let mut deserializer = PerfectHashMapDeserializer::<hasher::CityHash, _, _, _>::new(
        CHDReader::new(),
        DefaultHashValueReader::new(),
    );
    let mut rng = rand::thread_rng();
    keys.shuffle(&mut rng);
    let size = deserializer.load_from_mmap_file(BENCH_FILE);
    println!("{:?} size {}KiB", config, size / 1024);

    let mut idx = 0usize;
    c.bench_function("lookup", |b| {
        b.iter(|| unsafe {
            for _ in 0..1000 {
                let (k, _) = &keys.get_unchecked(idx % keys.len());
                let value: &str = std::str::from_utf8_unchecked(deserializer.get(k));
                black_box(value);
                idx += 1;
            }
        })
    });

    std::mem::drop(deserializer);
    std::fs::remove_file(BENCH_FILE).unwrap()
}

fn test_lookup_seq(c: &mut Criterion) {
    let config = CHDGeneratorConfig::default().load_factor(0.5f32);
    let keys = init_data(config.clone(), DEFAULT_LEN);
    let mut deserializer = PerfectHashMapDeserializer::<hasher::CityHash, _, _, _>::new(
        CHDReader::new(),
        DefaultHashValueReader::new(),
    );

    let size = deserializer.load_from_mmap_file(BENCH_FILE);
    println!("{:?} size {}KiB", config, size / 1024);

    let mut idx = 0usize;
    c.bench_function("lookup_seq", |b| {
        b.iter(|| unsafe {
            for _ in 0..1000 {
                let (k, _) = &keys.get_unchecked(idx % keys.len());
                let value: &str = std::str::from_utf8_unchecked(deserializer.get(k));
                black_box(value);
                idx += 1;
            }
        })
    });

    std::mem::drop(deserializer);
    std::fs::remove_file(BENCH_FILE).unwrap()
}

fn test_lookup_minimal(c: &mut Criterion) {
    let config = CHDGeneratorConfig::default().minimal(true);
    let mut keys = init_data(config.clone(), DEFAULT_LEN);
    let mut deserializer = PerfectHashMapDeserializer::<hasher::CityHash, _, _, _>::new(
        CHDReader::new(),
        DefaultHashValueReader::new(),
    );

    let mut rng = rand::thread_rng();
    keys.shuffle(&mut rng);
    let size = deserializer.load_from_mmap_file(BENCH_FILE);
    println!("{:?} size {}KiB", config, size / 1024);

    let mut idx = 0usize;
    c.bench_function("lookup_minimal", |b| {
        b.iter(|| unsafe {
            for _ in 0..1000 {
                let (k, _) = &keys.get_unchecked(idx % keys.len());
                let value: &str = std::str::from_utf8_unchecked(deserializer.get(k));
                black_box(value);
                idx += 1;
            }
        })
    });

    std::mem::drop(deserializer);
    std::fs::remove_file(BENCH_FILE).unwrap();
}

fn test_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("sample-build");
    group.sample_size(10);
    let len = 10000usize;
    group.throughput(criterion::Throughput::Elements(len as u64));

    let config = CHDGeneratorConfig::default();
    group.bench_function("build", |b| {
        b.iter(|| {
            let _ = init_data(config.clone(), len);
            std::fs::remove_file(BENCH_FILE).unwrap();
        })
    });
}

criterion_group! {
    name=benches;
    config=Criterion::default().sample_size(50);
    targets = test_lookup, test_lookup_minimal, test_lookup_seq, test_build
}
criterion_main!(benches);
