use auditorium::{intersect, RangePostingList};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

pub fn bench_posting_list(c: &mut Criterion) {
    c.bench_function("intersect 1000000", |b| {
        b.iter(|| {
            let a = RangePostingList(0..1_000_000);
            let b = RangePostingList(500_000..1_000_000);
            black_box(intersect(a, b).count())
        })
    });

    c.bench_function("merge 1000000", |b| {
        b.iter(|| {
            let a = RangePostingList(0..750_000);
            let b = RangePostingList(250_000..1_000_000);
            black_box(intersect(a, b).count())
        })
    });
}

criterion_group!(benches, bench_posting_list);
criterion_main!(benches);
