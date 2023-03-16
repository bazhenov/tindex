use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};

#[derive(Clone)]
struct PL([u64; 5]);

pub fn range_sum(c: &mut Criterion) {
    let mut g = c.benchmark_group("Range Sum");

    let size = 1024 * 1024;
    let mut a = vec![];
    for i in 0..size {
        a.push(i);
    }
    g.throughput(Throughput::Elements(size));

    g.bench_function("Sum simple", |bench| {
        bench.iter_batched(
            || a.clone(),
            |v| traverse_simple(&v[..]),
            BatchSize::SmallInput,
        );
    });

    g.bench_function("Sum unrolled", |bench| {
        bench.iter_batched(
            || a.clone(),
            |v| traverse_unrolled(&v[..]),
            BatchSize::SmallInput,
        );
    });

    g.bench_function("Sum lib", |bench| {
        bench.iter_batched(
            || a.clone(),
            |v| traverse_lib(&v[..]),
            BatchSize::SmallInput,
        );
    });

    g.finish();
}

#[inline(never)]
fn traverse_simple(input: &[u64]) -> u64 {
    let mut a = 0;
    for i in input {
        a += i;
    }
    a
}

#[inline(never)]
fn traverse_lib(input: &[u64]) -> u64 {
    input.iter().sum()
}

#[inline(never)]
fn traverse_unrolled(input: &[u64]) -> u64 {
    let mut a = 0;
    let mut b = 0;
    let mut c = 0;
    let mut d = 0;
    for i in 0..input.len() / 4 {
        a += input[i];
        b += input[i + 1];
        c += input[i + 2];
        d += input[i + 3];
    }
    a + b + c + d
}

criterion_group!(benches, range_sum);
criterion_main!(benches);
