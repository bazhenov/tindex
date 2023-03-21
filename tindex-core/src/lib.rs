#![feature(portable_simd)]
#![feature(stdsimd)]

use lazy_static::lazy_static;
use std::{
    ops::{Index, Range},
    simd::{u64x4, usizex4, SimdPartialEq, ToBitMask},
};

pub mod encoding;

mod prelude {
    pub type Result<T> = anyhow::Result<T>;
    pub type IoResult<T> = std::io::Result<T>;
}

lazy_static! {
    static ref MASKS: [(usize, usizex4); 16] = [
        (0, usizex4::from([4, 4, 4, 4])), // 0000 - 0
        (1, usizex4::from([4, 4, 4, 0])), // 0001 - 1
        (1, usizex4::from([4, 4, 0, 4])), // 0010 - 2
        (2, usizex4::from([4, 4, 0, 1])), // 0011 - 3
        (1, usizex4::from([4, 0, 4, 4])), // 0100 - 4
        (2, usizex4::from([4, 0, 4, 1])), // 0101 - 5
        (2, usizex4::from([4, 0, 1, 4])), // 0110 - 6
        (3, usizex4::from([4, 0, 1, 2])), // 0111 - 7
        (1, usizex4::from([0, 4, 4, 4])), // 1000 - 8
        (2, usizex4::from([0, 4, 4, 1])), // 1001 - 9
        (2, usizex4::from([0, 4, 1, 4])), // 1010 - 10
        (3, usizex4::from([0, 4, 1, 2])), // 1011 - 11
        (2, usizex4::from([0, 1, 4, 4])), // 1100 - 12
        (3, usizex4::from([0, 1, 4, 2])), // 1101 - 13
        (3, usizex4::from([0, 1, 2, 4])), // 1110 - 14
        (4, usizex4::from([0, 1, 2, 3])), // 1111 - 15
    ];

    static ref LENGTHS: [usize; 16] = [
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
        a: Buffer::<32>::default(),
        b: Buffer::<32>::default(),
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

    #[inline]
    fn refill_advance(&mut self, target: u64, decoder: &mut dyn PostingListDecoder) -> usize {
        let items_left = self.items_left();
        if items_left > 0 {
            self.buffer.copy_within(self.pos..self.capacity, 0);
        }
        let len = decoder.next_batch_advance(target, &mut self.buffer[items_left..]);
        self.capacity = len + items_left;
        self.pos = 0;
        if self.capacity < N {
            self.buffer[self.capacity..].fill(0);
        }
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
    a: Buffer<32>,
    b: Buffer<32>,
}

impl PostingListDecoder for Intersect {
    fn next_batch(&mut self, buffer: &mut PlBuffer) -> usize {
        let mut buffer_pos = 0;

        const LANES: usize = 4;

        if self.a.items_left() == 0 {
            self.a.refill(self.a_decoder.as_mut());
        }
        if self.b.items_left() == 0 {
            self.b.refill(self.b_decoder.as_mut());
        }

        while buffer.len() - buffer_pos <= LANES
            && self.a.items_left() >= LANES
            && self.b.items_left() >= LANES
        {
            let a: &[u64; LANES] = self.a.buffer[self.a.pos..self.a.pos + LANES]
                .try_into()
                .unwrap();
            let b: &[u64; LANES] = self.b.buffer[self.b.pos..self.b.pos + LANES]
                .try_into()
                .unwrap();

            // dbg!(a);
            // dbg!(b);

            let a_simd = u64x4::from(*a);
            let b_simd = u64x4::from(*b);
            let r0 = b_simd.rotate_lanes_left::<0>();
            let r1 = b_simd.rotate_lanes_left::<1>();
            let r2 = b_simd.rotate_lanes_left::<2>();
            let r3 = b_simd.rotate_lanes_left::<3>();

            let mask =
                a_simd.simd_eq(r0) | a_simd.simd_eq(r1) | a_simd.simd_eq(r2) | a_simd.simd_eq(r3);

            let mask_idx = reverse4bits(mask.to_bitmask()) as usize;
            // dbg!(&mask.to_array());
            let (len, mask) = MASKS[mask_idx];
            a_simd.scatter(&mut buffer[buffer_pos..buffer_pos + LANES], mask);
            buffer_pos += len;
            // dbg!(len);
            // dbg!(&buffer);
            // dbg!(&mask_idx);
            // dbg!(reverse4bits(mask_idx as u8));

            if a.last().unwrap() < b.last().unwrap() {
                self.a.pos += LANES;
                if self.a.items_left() < LANES {
                    self.a.refill_advance(b[LANES - 1], self.a_decoder.as_mut());
                }
            } else {
                self.b.pos += LANES;
                if self.b.items_left() < LANES {
                    self.b.refill_advance(a[LANES - 1], self.b_decoder.as_mut());
                }
            };
        }

        // dbg!(&buffer);
        // dbg!(self.a_buffer);
        // dbg!((self.a_position, self.a_capacity));
        // dbg!(self.b_buffer);
        // dbg!((self.b_position, self.b_capacity));

        // dbg!(buffer_pos);

        // dbg!(&self.a);
        // dbg!(&self.b);

        while self.a.items_left() > 0 && self.b.items_left() > 0 && buffer_pos < buffer.len() {
            let a = self.a.buffer[self.a.pos];
            let b = self.b.buffer[self.b.pos];

            if a == b {
                buffer[buffer_pos] = a;
                self.a.pos += 1;
                self.b.pos += 1;
                buffer_pos += 1;
            } else if a < b {
                self.a.pos += 1;
            } else {
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

fn reverse4bits(mut b: u8) -> u8 {
    // b = (b & 0xF0) >> 4 | (b & 0x0F) << 4;
    b = (b & 0xCC) >> 2 | (b & 0x33) << 2;
    b = (b & 0xAA) >> 1 | (b & 0x55) << 1;
    return b;
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
    range: Range<u64>,
    next: u64,
}

impl RangePostingList {
    pub fn new(range: Range<u64>) -> Self {
        if range.start == NO_DOC {
            panic!("Start should be greater than zero");
        }
        let next = range.start;
        Self { range, next }
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> u64 {
        self.range.end - self.range.start
    }
}

impl PostingListDecoder for RangePostingList {
    fn next_batch(&mut self, buffer: &mut PlBuffer) -> usize {
        self.next_batch_advance(self.next, buffer)
    }

    fn next_batch_advance(&mut self, target: u64, buffer: &mut PlBuffer) -> usize {
        self.next = self.next.max(target);
        let start = self.next;
        if start >= self.range.end {
            return 0;
        }
        let len = buffer.len().min((self.range.end - start) as usize);
        for (i, item) in buffer[..len].iter_mut().enumerate() {
            *item = start + i as u64;
        }
        self.next += len as u64;
        len
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
            71, 254, 118, 3, 30, 11, 164, 87, 231, 202, 4, 94, 71, 208, 178, 90, 154, 191, 156, 19,
            227, 117, 204, 5, 140, 4, 214, 103, 117, 121, 139, 150,
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
        let (len, mask) = &MASKS[5]; // 0101
        a.scatter(&mut output, *mask);
        assert_eq!(*len, 2);
        assert_eq!(output, [2, 4, 0, 0]);
    }
}
