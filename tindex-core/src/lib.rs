#![feature(portable_simd)]
#![feature(stdsimd)]
#![feature(slice_as_chunks)]

use lazy_static::lazy_static;
use std::{
    ops::{AddAssign, Range, Shr, ShrAssign},
    simd::{u64x4, u64x8, usizex4, SimdPartialEq, ToBitMask},
};

pub mod encoding;

mod prelude {
    pub type Result<T> = anyhow::Result<T>;
    pub type IoResult<T> = std::io::Result<T>;
}

lazy_static! {
    static ref MASKS: [usizex4; 16] = [
        usizex4::from([4, 4, 4, 4]), // 0000 - 0
        usizex4::from([0, 4, 4, 4]), // 1000 - 1
        usizex4::from([4, 0, 4, 4]), // 0100 - 2
        usizex4::from([0, 1, 4, 4]), // 1100 - 3
        usizex4::from([4, 4, 0, 4]), // 0010 - 4
        usizex4::from([0, 4, 1, 4]), // 1010 - 5
        usizex4::from([4, 0, 1, 4]), // 0110 - 6
        usizex4::from([0, 1, 2, 4]), // 1110 - 7
        usizex4::from([4, 4, 4, 0]), // 0001 - 8
        usizex4::from([0, 4, 4, 1]), // 1001 - 9
        usizex4::from([4, 0, 4, 1]), // 0101 - 10
        usizex4::from([0, 1, 4, 2]), // 1101 - 11
        usizex4::from([4, 4, 0, 1]), // 0011 - 12
        usizex4::from([0, 4, 1, 2]), // 1011 - 13
        usizex4::from([4, 0, 1, 2]), // 0111 - 14
        usizex4::from([0, 1, 2, 3]), // 1111 - 15
    ];

    static ref LENGTHS: [u8; 16] = [
        0,
        1,
        1,
        2,
        1,
        2,
        2,
        3,
        1,
        2,
        2,
        3,
        2,
        3,
        3,
        4,
    ];
}

pub const NO_DOC: u64 = u64::MAX;
type PlBuffer = [u64];

pub trait PostingListDecoder {
    fn next_batch_advance(&mut self, target: u64, buffer: &mut PlBuffer) -> usize {
        let mut len = self.next_batch(buffer);
        if len == 0 {
            return 0;
        }
        while buffer[len - 1] < target {
            len = self.next_batch(buffer);
            if len == 0 {
                return 0;
            }
        }
        len
    }

    fn next_batch(&mut self, buffer: &mut PlBuffer) -> usize;

    fn to_vec(mut self) -> Vec<u64>
    where
        Self: Sized,
    {
        let mut result = vec![];
        let mut pl = [0; 16];
        loop {
            let len = self.next_batch(&mut pl);
            if len == 0 {
                break;
            }
            result.extend(&pl[0..len]);
        }
        result
    }
}

pub fn intersect(
    a: impl PostingListDecoder + 'static,
    b: impl PostingListDecoder + 'static,
) -> Intersect {
    Intersect {
        a_decoder: Box::new(a),
        b_decoder: Box::new(b),
        a: Buffer::<16>::default(),
        b: Buffer::<16>::default(),
    }
    .into()
}

pub fn merge(a: PostingList, b: PostingList) -> PostingList {
    Merge(a, b).into()
}

pub fn exclude(a: PostingList, b: PostingList) -> PostingList {
    Exclude(a, b).into()
}

pub struct PostingList {
    decoder: Box<dyn PostingListDecoder>,
    buffer: [u64; 16],
    len: usize,
    position: usize,
}

impl<T: PostingListDecoder + 'static> From<T> for PostingList {
    fn from(source: T) -> Self {
        Self {
            decoder: Box::new(source),
            buffer: [0; 16],
            len: 0,
            position: 0,
        }
    }
}

impl PostingList {
    #[allow(clippy::should_implement_trait)]
    #[inline]
    pub fn next(&mut self) -> u64 {
        self.position += 1;
        if !self.ensure_buffer_has_data() {
            return NO_DOC;
        }
        self.buffer[self.position]
    }

    /// Возвращает первый элемент в потоке равный или больший чем переданный `target`
    pub fn advance(&mut self, target: u64) -> u64 {
        let mut current = self.current();
        if current == NO_DOC || current >= target {
            return current;
        }
        if self.buffer[self.len - 1] < target {
            // advancing to the target using decoder advance
            self.len = self.decoder.next_batch_advance(target, &mut self.buffer);
            self.position = 0;
            current = self.current();
        }
        // element already in current buffer
        while current != NO_DOC && current < target {
            current = self.next();
        }
        current
    }

    // fn fill_buffer(&mut self, buffer: &mut [u64; 4]) -> usize {
    //     if self.position + 4 < buffer.len() {
    //         buffer.copy_from_slice(&self.buffer[self.position..self.position + 4]);
    //         self.position += 4;
    //         return 4;
    //     }else if
    //     decoder.next_batch(&mut buffer[..])
    // }

    #[inline]
    pub fn current(&mut self) -> u64 {
        if !self.ensure_buffer_has_data() {
            return NO_DOC;
        }
        self.buffer[self.position]
    }

    #[inline]
    fn ensure_buffer_has_data(&mut self) -> bool {
        if self.position < self.len {
            return true;
        }
        self.len = self.decoder.next_batch(&mut self.buffer);
        self.position = 0;
        self.len > 0
    }
}

pub struct Merge(pub PostingList, pub PostingList);

impl PostingListDecoder for Merge {
    fn next_batch(&mut self, buffer: &mut PlBuffer) -> usize {
        let mut a = self.0.current();
        let mut b = self.1.current();
        let mut i = 0;
        while i < buffer.len() && (a != NO_DOC || b != NO_DOC) {
            while a < b && i < buffer.len() && a != NO_DOC {
                buffer[i] = a;
                i += 1;
                a = self.0.next();
            }
            while b < a && i < buffer.len() && b != NO_DOC {
                buffer[i] = b;
                i += 1;
                b = self.1.next();
            }
            while a == b && a != NO_DOC && b != NO_DOC && i < buffer.len() {
                buffer[i] = b;
                i += 1;
                a = self.0.next();
                b = self.1.next();
            }
        }
        i
    }
}
#[derive(Debug)]
struct Buffer<const N: usize> {
    buffer: [u64; N],
    pos: usize,
    capacity: usize,
}

impl<const N: usize> Default for Buffer<N> {
    fn default() -> Self {
        Self {
            buffer: [0; N],
            pos: 0,
            capacity: 0,
        }
    }
}

impl<const N: usize> Buffer<N> {
    #[inline]
    fn items_left(&self) -> usize {
        self.capacity - self.pos
    }

    #[inline]
    fn refill(&mut self, decoder: &mut dyn PostingListDecoder) -> usize {
        self.refill_advance(0, decoder)
    }

    fn refill_advance(&mut self, target: u64, decoder: &mut dyn PostingListDecoder) -> usize {
        let items_left = self.items_left();
        if items_left > 0 && self.pos > 0 {
            self.buffer.copy_within(self.pos..self.capacity, 0);
        }
        let len = decoder.next_batch_advance(target, &mut self.buffer[items_left..]);
        self.capacity = len + items_left;
        self.pos = 0;
        self.capacity
    }
}

// impl<const N: usize> Index<usize> for Buffer<N> {
//     type Output = u64;

//     fn index(&self, index: usize) -> &Self::Output {
//         &self.buffer[index]
//     }
// }

pub struct Intersect {
    a_decoder: Box<dyn PostingListDecoder>,
    b_decoder: Box<dyn PostingListDecoder>,
    a: Buffer<16>,
    b: Buffer<16>,
}

impl PostingListDecoder for Intersect {
    fn next_batch(&mut self, buffer: &mut PlBuffer) -> usize {
        let mut buffer_pos = 0;

        const LANES: usize = 4;

        if self.a.items_left() < LANES {
            self.a.refill(self.a_decoder.as_mut());
        }
        if self.b.items_left() < LANES {
            self.b.refill(self.b_decoder.as_mut());
        }

        while buffer.len() - buffer_pos >= LANES
            && self.a.items_left() >= LANES
            && self.b.items_left() >= LANES
        {
            let a_simd = u64x4::from_slice(&self.a.buffer[self.a.pos..self.a.pos + LANES]);
            let b_simd = u64x4::from_slice(&self.b.buffer[self.b.pos..self.b.pos + LANES]);
            // dbg!(a_simd);
            // dbg!(b_simd);

            let r0 = b_simd.rotate_lanes_left::<0>();
            let r1 = b_simd.rotate_lanes_left::<1>();
            let r2 = b_simd.rotate_lanes_left::<2>();
            let r3 = b_simd.rotate_lanes_left::<3>();

            let mask =
                a_simd.simd_eq(r0) | a_simd.simd_eq(r1) | a_simd.simd_eq(r2) | a_simd.simd_eq(r3);

            let mask_idx = mask.to_bitmask();
            if mask_idx > 0 {
                let mask = MASKS[mask_idx as usize];
                a_simd.scatter(&mut buffer[buffer_pos..buffer_pos + LANES], mask);
                buffer_pos += mask_idx.count_ones() as usize;
            }

            // dbg!(a_max);
            // dbg!(b_max);

            let a_max = a_simd[LANES - 1];
            let b_max = b_simd[LANES - 1];
            if a_max <= b_max {
                self.a.pos += LANES;
                if self.a.items_left() < LANES {
                    self.a
                        .refill_advance(self.b.buffer[self.b.pos], self.a_decoder.as_mut());
                }
            } else {
                self.b.pos += LANES;
                if self.b.items_left() < LANES {
                    self.b
                        .refill_advance(self.a.buffer[self.a.pos], self.b_decoder.as_mut());
                }
            };
        }

        // dbg!(buffer_pos);

        // dbg!(&self.a);
        // dbg!(&self.b);

        while self.a.items_left() > 0 && self.b.items_left() > 0 && buffer_pos < buffer.len() {
            let a = self.a.buffer[self.a.pos];
            let b = self.b.buffer[self.b.pos];

            if a == b {
                buffer[buffer_pos] = a;
                buffer_pos += 1;
            }
            if a <= b {
                self.a.pos += 1;
            }
            if b <= a {
                self.b.pos += 1;
            }
            if self.a.items_left() == 0 {
                self.a.refill_advance(b, self.a_decoder.as_mut());
            }
            if self.b.items_left() == 0 {
                self.b.refill_advance(a, self.b_decoder.as_mut());
            }
        }

        return buffer_pos;
    }
}

pub struct Exclude(pub PostingList, pub PostingList);

impl PostingListDecoder for Exclude {
    fn next_batch(&mut self, buffer: &mut PlBuffer) -> usize {
        let mut a = self.0.current();
        let mut b = self.1.current();
        let mut i = 0;
        while i < buffer.len() && a != NO_DOC {
            while (a < b || b == NO_DOC) && i < buffer.len() && a != NO_DOC {
                buffer[i] = a;
                i += 1;
                a = self.0.next();
            }
            if b < a {
                b = self.1.advance(a);
            }
            while a == b && a != NO_DOC && b != NO_DOC {
                a = self.0.next();
                b = self.1.next();
            }
        }
        i
    }
}

#[derive(Debug)]
pub struct VecPostingList {
    data: Vec<u64>,
    pos: usize,
}

impl VecPostingList {
    pub fn new(input: &[u64]) -> Self {
        assert!(!input.is_empty(), "Posting list should not be empty");
        assert!(input[0] > 0, "First element should be positive");
        let mut list = Vec::with_capacity(input.len());
        let mut previous = input[0];
        list.push(input[0]);
        for item in &input[1..] {
            assert!(*item > previous, "Items should be increasing");
            list.push(*item);
            previous = *item;
        }
        Self { data: list, pos: 0 }
    }
}

impl PostingListDecoder for VecPostingList {
    fn next_batch(&mut self, buffer: &mut PlBuffer) -> usize {
        if self.pos >= self.data.len() {
            return 0;
        }
        let len = buffer.len().min(self.data.len() - self.pos);
        let src = &self.data[self.pos..self.pos + len];
        buffer[0..len].copy_from_slice(src);
        self.pos += len;
        len
    }
}

#[derive(Clone)]
pub struct RangePostingList {
    next: u64,
    end: u64,
}

impl RangePostingList {
    pub fn new(range: Range<u64>) -> Self {
        if range.start == 0 {
            panic!("Start should be greater than zero");
        }
        if range.start == NO_DOC {
            panic!("Start should be less tahn NO_DOC const");
        }
        let next = range.start;
        let end = range.end;
        Self { next, end }
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> u64 {
        self.end - self.next
    }
}

#[inline]
fn next_batch_scalar(pl: &mut RangePostingList, target: u64, buffer: &mut PlBuffer) -> usize {
    pl.next = pl.next.max(target);
    let start = pl.next;
    if start >= pl.end {
        return 0;
    }
    let range_len = (pl.end - pl.next) as usize;
    let len = range_len.min(buffer.len());
    for i in 0..len {
        buffer[i] = pl.next;
        pl.next += 1;
    }
    len
}

#[inline]
fn next_batch_v2(pl: &mut RangePostingList, target: u64, buffer: &mut PlBuffer) -> usize {
    pl.next = pl.next.max(target);
    if pl.next >= pl.end {
        return 0;
    }

    let range_len = (pl.end - pl.next) as usize;
    let len = range_len.min(buffer.len());

    for (i, item) in buffer[..len].iter_mut().enumerate() {
        *item = pl.next + i as u64;
    }

    pl.next += len as u64;

    len
}

#[inline]
fn next_batch_v3(pl: &mut RangePostingList, target: u64, buffer: &mut PlBuffer) -> usize {
    pl.next = pl.next.max(target);
    if pl.next >= pl.end {
        return 0;
    }

    let range_len = (pl.end - pl.next) as usize;
    let len = range_len.min(buffer.len());

    for chunk in buffer[..len].chunks_mut(16) {
        for (i, item) in chunk.iter_mut().enumerate() {
            *item = pl.next + i as u64;
        }
        pl.next += chunk.len() as u64;
    }

    len
}

#[inline]
fn next_batch_v4(pl: &mut RangePostingList, target: u64, buffer: &mut PlBuffer) -> usize {
    const PROGRESSION: [u64; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
    pl.next = pl.next.max(target);
    if pl.next >= pl.end {
        return 0;
    }

    let range_len = (pl.end - pl.next) as usize;
    let len = range_len.min(buffer.len());

    for chunk in buffer[..len].chunks_mut(PROGRESSION.len()) {
        if chunk.len() == PROGRESSION.len() {
            for (item, offset) in chunk.iter_mut().zip(PROGRESSION) {
                *item = pl.next + offset;
            }
        } else {
            for (item, offset) in chunk.iter_mut().zip(PROGRESSION) {
                *item = pl.next + offset;
            }
        }
        pl.next += chunk.len() as u64;
    }

    len
}

#[inline]
fn next_batch_v5(pl: &mut RangePostingList, target: u64, buffer: &mut PlBuffer) -> usize {
    const PROGRESSION: [u64; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
    pl.next = pl.next.max(target);
    if pl.next >= pl.end {
        return 0;
    }

    let range_len = (pl.end - pl.next) as usize;
    let len = range_len.min(buffer.len());

    let (chunks, remainder) = buffer[..len].as_chunks_mut::<16>();
    for chunk in chunks {
        for (item, offset) in chunk.iter_mut().zip(PROGRESSION) {
            *item = pl.next + offset;
        }
        pl.next += chunk.len() as u64;
    }

    for (item, offset) in remainder.iter_mut().zip(PROGRESSION) {
        *item = pl.next + offset;
    }
    pl.next += remainder.len() as u64;

    len
}

#[inline]
fn next_batch_v6(pl: &mut RangePostingList, target: u64, buffer: &mut PlBuffer) -> usize {
    pl.next = pl.next.max(target);
    if pl.next >= pl.end {
        return 0;
    }

    let range_len = (pl.end - pl.next) as usize;
    let len = range_len.min(buffer.len());

    const PROGRESSION: u64x8 = u64x8::from_array([0, 1, 2, 3, 4, 5, 6, 7]);
    const LANES: usize = PROGRESSION.lanes();
    const STRIDE: usize = PROGRESSION.lanes() * 2;
    let lanes_offset = u64x8::splat(LANES as u64);

    for chunk in buffer[..len].chunks_mut(STRIDE) {
        // This code duplication is required for compiler to vectorize code
        // at the moment slice::as_chunks_mut() producing slower code
        if chunk.len() == STRIDE {
            let v = u64x8::splat(pl.next) + PROGRESSION;
            chunk[0..LANES].copy_from_slice(v.as_array());

            let v = v + lanes_offset;
            chunk[LANES..].copy_from_slice(v.as_array());
        } else {
            for (item, add) in chunk.iter_mut().zip(0..STRIDE) {
                *item = pl.next + add as u64;
            }
        }
        pl.next += chunk.len() as u64;
    }

    len
}

#[inline]
fn next_batch_v7(pl: &mut RangePostingList, target: u64, buffer: &mut PlBuffer) -> usize {
    pl.next = pl.next.max(target);
    if pl.next >= pl.end {
        return 0;
    }

    let range_len = (pl.end - pl.next) as usize;
    let len = range_len.min(buffer.len());

    const PROGRESSION: u64x8 = u64x8::from_array([0, 1, 2, 3, 4, 5, 6, 7]);
    const LANES: usize = PROGRESSION.lanes();
    const STRIDE: usize = PROGRESSION.lanes() * 2;
    let lanes_offset = u64x8::splat(LANES as u64);

    let (chunks, remainder) = buffer[..len].as_chunks_mut::<16>();

    for chunk in chunks {
        let v = u64x8::splat(pl.next) + PROGRESSION;
        chunk[0..LANES].copy_from_slice(v.as_array());

        let v = v + lanes_offset;
        chunk[LANES..].copy_from_slice(v.as_array());

        pl.next += chunk.len() as u64;
    }

    for (item, add) in remainder.iter_mut().zip(0..STRIDE) {
        *item = pl.next + add as u64;
    }

    pl.next += remainder.len() as u64;

    len
}

impl PostingListDecoder for RangePostingList {
    fn next_batch(&mut self, buffer: &mut PlBuffer) -> usize {
        self.next_batch_advance(self.next, buffer)
    }

    fn next_batch_advance(&mut self, target: u64, buffer: &mut PlBuffer) -> usize {
        // 1.65GElem/s
        // next_batch_scalar(self, target, buffer)

        // 1.79GElem/s
        // next_batch_v2(self, target, buffer)

        // 3.04GElem/s
        // next_batch_v3(self, target, buffer)

        // 3.74GElem/s
        // next_batch_v4(self, target, buffer)

        // 3.52GElem/s using
        // next_batch_v5(self, target, buffer)

        // 4.40GElem/s using
        next_batch_v6(self, target, buffer)

        // 4.20GElem/s using
        // next_batch_v7(self, target, buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::prelude::*;
    use rand::Fill;
    use rand::SeedableRng;
    use std::fmt::Debug;
    use std::panic;
    use std::panic::RefUnwindSafe;
    use std::simd::u64x4;
    use std::simd::SimdPartialEq;

    #[test]
    fn check_intersect_simple() {
        let a = RangePostingList::new(1..5);
        let b = RangePostingList::new(2..7);

        let values = intersect(a, b).to_vec();
        assert_eq!(values, vec![2, 3, 4]);
    }

    #[test]
    fn check_merge() {
        let a = RangePostingList::new(1..3);
        let b = RangePostingList::new(2..5);

        let values = Merge(a.into(), b.into()).to_vec();
        assert_eq!(values, vec![1, 2, 3, 4]);
    }

    #[test]
    fn check_exclude() {
        let a = RangePostingList::new(1..6);
        let b = RangePostingList::new(2..4);

        let values = Exclude(a.into(), b.into()).to_vec();
        assert_eq!(values, vec![1, 4, 5]);
    }

    #[test]
    fn check_no_exclude() {
        let a = RangePostingList::new(1..1_000);
        let b = RangePostingList::new(1_000..2_000);

        let values = Exclude(a.into(), b.into()).to_vec();
        assert_eq!(999, values.len());
    }

    #[test]
    fn check_merge_massive() {
        run_seeded_test::<StdRng>(None, |mut rng| {
            for _ in 0..100 {
                let a = random_posting_list(&mut rng);
                let b = random_posting_list(&mut rng);

                let expected = naive_merge(&a.data, &b.data);
                let actual = Merge(a.into(), b.into()).to_vec();

                assert_eq!(actual, expected);
            }
        });
    }

    #[test]
    fn check_intersect_massive() {
        let seed = [
            158, 89, 10, 112, 64, 144, 165, 151, 72, 60, 206, 33, 109, 239, 68, 78, 118, 187, 237,
            203, 159, 240, 236, 12, 175, 97, 49, 240, 63, 199, 149, 83,
        ];
        run_seeded_test::<StdRng>(None, |mut rng| {
            for _ in 0..1000 {
                let a = random_posting_list(&mut rng);
                let b = random_posting_list(&mut rng);

                let expected = naive_intersect(&a.data, &b.data);
                let actual = intersect(a, b).to_vec();

                assert_eq!(actual, expected);
            }
        });
    }

    #[test]
    fn check_exclude_massive() {
        run_seeded_test::<StdRng>(None, |mut rng| {
            for _ in 0..100 {
                let a = random_posting_list(&mut rng);
                let b = random_posting_list(&mut rng);

                let expected = naive_exclude(&a.data, &b.data);
                let actual = Exclude(a.into(), b.into()).to_vec();

                assert_eq!(actual, expected);
            }
        });
    }

    #[test]
    fn range_posting_list_next_advance() {
        let mut t = RangePostingList::new(1..1000);
        let mut buffer = [0; 3];

        assert_eq!(t.next_batch(&mut buffer), 3);
        assert_eq!(buffer, [1, 2, 3]);

        assert_eq!(t.next_batch_advance(10, &mut buffer), 3);
        assert_eq!(buffer, [10, 11, 12]);

        assert_eq!(t.next_batch_advance(998, &mut buffer), 2);
        assert_eq!(buffer[..2], [998, 999]);
    }

    fn naive_merge(a: &[u64], b: &[u64]) -> Vec<u64> {
        let mut union = a
            .iter()
            .cloned()
            .chain(b.iter().cloned())
            .collect::<Vec<_>>();
        union.sort();
        union.dedup();
        union
    }

    fn naive_intersect(a: &[u64], b: &[u64]) -> Vec<u64> {
        a.iter()
            .filter(|i| b.binary_search(i).is_ok())
            .cloned()
            .collect()
    }

    fn naive_exclude(a: &[u64], b: &[u64]) -> Vec<u64> {
        a.iter()
            .filter(|i| b.binary_search(i).is_err())
            .cloned()
            .collect()
    }

    fn run_seeded_test<R: SeedableRng + Rng>(seed: Option<R::Seed>, f: fn(R) -> ())
    where
        R::Seed: Fill + Debug + Copy + RefUnwindSafe,
    {
        let seed_provided = seed.is_some();
        let seed = seed.unwrap_or_else(|| {
            let mut seed = Default::default();
            thread_rng().fill(&mut seed);
            seed
        });

        let result = panic::catch_unwind(|| {
            f(R::from_seed(seed));
        });

        if result.is_err() {
            if seed_provided {
                panic!("Test are failed");
            } else {
                panic!(
                    "Test are failed. Check following seed:\n\n  ==> seed: {:?}\n\n",
                    seed
                );
            }
        }
    }

    fn random_posting_list(rng: &mut impl Rng) -> VecPostingList {
        let size: usize = rng.gen_range(1..50);
        let mut list = Vec::with_capacity(size);

        let mut doc_id = 0;
        for _ in 0..size {
            doc_id += rng.gen_range(1..5);
            list.push(doc_id)
        }
        VecPostingList::new(&list)
    }

    #[test]
    fn check_simd_load_store() {
        let a = u64x4::from([1, 2, 3, 4]);
        let b = u64x4::from([2, 3, 6, 8]);

        let r0 = b.rotate_lanes_left::<0>();
        let r1 = b.rotate_lanes_left::<1>();
        let r2 = b.rotate_lanes_left::<2>();
        let r3 = b.rotate_lanes_left::<3>();

        let mask = a.simd_eq(r0) | a.simd_eq(r1) | a.simd_eq(r2) | a.simd_eq(r3);
        let output = mask.to_array();
        assert_eq!(output, [false, true, true, false]);
    }

    #[test]
    fn check_simd_scatter() {
        let a = u64x4::from([1, 2, 3, 4]);
        let mut output = [0u64; 4];
        let mask = MASKS[10]; // 0101
        a.scatter(&mut output, mask);
        assert_eq!(output, [2, 4, 0, 0]);
    }

    #[test]
    fn check_range_posting_list() {
        for i in 2..100 {
            let pl = RangePostingList::new(1..i);
            let vec = pl.to_vec();
            dbg!(i);
            dbg!(&vec);
            let sum: u64 = vec.iter().sum();

            assert_eq!(sum, (i - 1) * i / 2);
        }
    }
}
