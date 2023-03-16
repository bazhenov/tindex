use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

pub fn range_sum(c: &mut Criterion) {
    let mut g = c.benchmark_group("Range Sum");

    const KB: u32 = 1024;
    const MB: u32 = 1024 * KB;

    let mut size = 1 * KB;
    while size <= 32 * MB {
        let mut a = Vec::with_capacity(size as usize);
        for i in 0..size {
            a.push(i);
        }
        g.throughput(Throughput::Elements(size as u64));

        g.bench_with_input(BenchmarkId::new("simple", size), &size, |b, _| {
            b.iter(|| traverse_simple(black_box(&a[..])));
        });
        g.bench_with_input(BenchmarkId::new("unrolled", size), &size, |b, _| {
            b.iter(|| traverse_unrolled(black_box(&a[..])));
        });
        g.bench_with_input(BenchmarkId::new("lib", size), &size, |b, _| {
            b.iter(|| traverse_lib(black_box(&a[..])));
        });

        size *= 2;
    }
    g.finish();
}

#[inline(never)]
fn traverse_simple(input: &[u32]) -> u32 {
    let mut a = 0;
    for i in input {
        a += i;
    }
    a
}

#[inline(never)]
fn traverse_lib(input: &[u32]) -> u32 {
    input.iter().sum()
}

#[inline(never)]
fn traverse_unrolled(input: &[u32]) -> u32 {
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
