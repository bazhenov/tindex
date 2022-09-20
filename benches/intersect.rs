use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use tindex::{exclude, intersect, merge, PostingList, PostingListDecoder, RangePostingList};

pub fn posting_list_intersect(c: &mut Criterion) {
    let mut g = c.benchmark_group("Posting List Intersect");

    g.throughput(Throughput::Elements(1_500));
    g.bench_function("Half Intersect", |bench| {
        let a = RangePostingList::new(0..1_000);
        let b = RangePostingList::new(500..1_000);
        bench.iter_batched(
            || intersect(a.clone().into(), b.clone().into()),
            traverse,
            BatchSize::SmallInput,
        );
    });

    g.throughput(Throughput::Elements(2_000));
    g.bench_function("Full Intersect", |bench| {
        let a = RangePostingList::new(0..1_000);
        bench.iter_batched(
            || intersect(a.clone().into(), a.clone().into()),
            traverse,
            BatchSize::SmallInput,
        );
    });

    g.throughput(Throughput::Elements(1_000));
    g.bench_function("No Intersect", |bench| {
        let a = RangePostingList::new(0..1_000);
        let b = RangePostingList::new(1_000..2_000);
        bench.iter_batched(
            || intersect(a.clone().into(), b.clone().into()),
            traverse,
            BatchSize::SmallInput,
        );
    });
}

pub fn posting_list_exclude(c: &mut Criterion) {
    let mut g = c.benchmark_group("Posting List Exclude");

    g.throughput(Throughput::Elements(1_500));
    g.bench_function("Half Exclude", |bench| {
        let a = RangePostingList::new(0..1_000);
        let b = RangePostingList::new(500..1_000);
        bench.iter_batched(
            || exclude(a.clone().into(), b.clone().into()),
            traverse,
            BatchSize::SmallInput,
        );
    });

    g.throughput(Throughput::Elements(2_000));
    g.bench_function("Full Exclude", |bench| {
        let a = RangePostingList::new(0..1_000);
        bench.iter_batched(
            || exclude(a.clone().into(), a.clone().into()),
            traverse,
            BatchSize::SmallInput,
        );
    });

    g.throughput(Throughput::Elements(1_000));
    g.bench_function("No Exclude", |bench| {
        let a = RangePostingList::new(0..1_000);
        let b = RangePostingList::new(1_000..2_000);
        bench.iter_batched(
            || exclude(a.clone().into(), b.clone().into()),
            traverse,
            BatchSize::SmallInput,
        );
    });
}

pub fn posting_list_merge(c: &mut Criterion) {
    let mut g = c.benchmark_group("Posting List Merge");
    let a = RangePostingList::new(0..750);
    let b = RangePostingList::new(250..1_000);

    g.throughput(Throughput::Elements(a.len() + b.len()));
    g.bench_function("1M", |bench| {
        bench.iter_batched(
            || merge(a.clone().into(), b.clone().into()),
            traverse,
            BatchSize::SmallInput,
        );
    });
}

pub fn posting_list_iterate(c: &mut Criterion) {
    let mut g = c.benchmark_group("iterate");
    let a = RangePostingList::new(0..1000);
    g.throughput(Throughput::Elements(1000));
    g.bench_function("1K8", |bench| {
        bench.iter_batched(
            || a.clone(),
            |mut a| {
                let mut v: [u64; 8] = [0; 8];
                while a.fill_buffer(&mut v).unwrap() > 0 {}
            },
            BatchSize::SmallInput,
        );
    });
}

#[inline]
fn traverse(mut input: PostingList) {
    while input.next().unwrap().is_some() {}
}

criterion_group!(
    benches,
    posting_list_intersect,
    posting_list_merge,
    posting_list_exclude,
    posting_list_iterate,
);
criterion_main!(benches);
