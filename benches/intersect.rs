use auditorium::{intersect, RangePostingList};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};

pub fn posting_list_intersect(c: &mut Criterion) {
    let mut g = c.benchmark_group("Posting List Intersect");

    g.throughput(Throughput::Elements(1_500_000));
    g.bench_function("Half Intersect", |bench| {
        let a = RangePostingList(0..1_000_000);
        let b = RangePostingList(500_000..1_000_000);
        bench.iter_batched(
            || (a.clone(), b.clone()),
            |(a, b)| black_box(intersect(a, b).count()),
            BatchSize::SmallInput,
        );
    });

    g.throughput(Throughput::Elements(2_000_000));
    g.bench_function("Full Intersect", |bench| {
        let a = RangePostingList(0..1_000_000);
        bench.iter_batched(
            || (a.clone(), a.clone()),
            |(a, b)| black_box(intersect(a, b).count()),
            BatchSize::SmallInput,
        );
    });

    g.throughput(Throughput::Elements(1_000_000));
    g.bench_function("No Intersect", |bench| {
        let a = RangePostingList(0..1_000_000);
        let b = RangePostingList(1_000_000..2_000_000);
        bench.iter_batched(
            || (a.clone(), b.clone()),
            |(a, b)| black_box(intersect(a, b).count()),
            BatchSize::SmallInput,
        );
    });
}

pub fn posting_list_merge(c: &mut Criterion) {
    let mut g = c.benchmark_group("Posting List Merge");
    let a = RangePostingList(0..750_000);
    let b = RangePostingList(250_000..1_000_000);

    g.throughput(Throughput::Elements(a.len() + b.len()));
    g.bench_function("1M", |bench| {
        bench.iter_batched(
            || (a.clone(), b.clone()),
            |(a, b)| black_box(intersect(a, b).count()),
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, posting_list_intersect, posting_list_merge);
criterion_main!(benches);
