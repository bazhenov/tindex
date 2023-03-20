#![feature(portable_simd)]

use std::{
    ops::Range,
    simd::{u64x4, usizex4, SimdPartialEq, ToBitMask},
};

pub mod encoding;

mod prelude {
    pub type Result<T> = anyhow::Result<T>;
    pub type IoResult<T> = std::io::Result<T>;
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
    Intersect(Box::new(a), Box::new(b), [0; 8], 0).into()
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

pub struct Intersect(
    Box<dyn PostingListDecoder>,
    Box<dyn PostingListDecoder>,
    pub [u64; 8],
    pub usize,
);

impl PostingListDecoder for Intersect {
    fn next_batch(&mut self, buffer: &mut PlBuffer) -> usize {
        let mut buffer_idx = 0;
        let stash = &mut self.2;
        let stash_idx = &mut self.3;

        let masks: [(usize, usizex4); 16] = [
            (0, usizex4::from([4, 4, 4, 4])), // 0000 - 0
            (1, usizex4::from([0, 4, 4, 4])), // 1000 - 1
            (1, usizex4::from([4, 0, 4, 4])), // 0100 - 2
            (2, usizex4::from([0, 1, 4, 4])), // 1100 - 3
            (1, usizex4::from([4, 4, 0, 4])), // 0010 - 4
            (2, usizex4::from([0, 4, 1, 4])), // 1010 - 5
            (2, usizex4::from([4, 0, 1, 4])), // 0110 - 6
            (3, usizex4::from([0, 1, 2, 4])), // 1110 - 7
            (1, usizex4::from([4, 4, 4, 0])), // 0001 - 8
            (2, usizex4::from([0, 4, 4, 1])), // 1001 - 9
            (2, usizex4::from([4, 0, 4, 1])), // 0101 - 10
            (3, usizex4::from([0, 1, 4, 2])), // 1101 - 11
            (2, usizex4::from([4, 4, 0, 1])), // 0011 - 12
            (3, usizex4::from([0, 4, 1, 2])), // 1011 - 13
            (3, usizex4::from([4, 0, 1, 2])), // 0111 - 14
            (4, usizex4::from([0, 1, 2, 3])), // 1111 - 15
        ];

        let mut a = [0; 4];
        let mut b = [0; 4];

        while *stash_idx > 0 && buffer_idx < buffer.len() {
            buffer[buffer_idx] = stash[*stash_idx];
            *stash_idx -= 1;
            buffer_idx += 1;
        }

        if buffer_idx == buffer.len() {
            return buffer.len();
        }

        // a.fill(0);
        let mut a_read = self.0.next_batch(&mut a);
        // b.fill(0);
        let mut b_read = self.1.next_batch(&mut b);

        while buffer_idx + 4 < buffer.len() && a_read == a.len() && b_read == b.len() {
            let a_simd = u64x4::from(a);
            let b_simd = u64x4::from(b);
            let r0 = b_simd.rotate_lanes_left::<0>();
            let r1 = b_simd.rotate_lanes_left::<1>();
            let r2 = b_simd.rotate_lanes_left::<2>();
            let r3 = b_simd.rotate_lanes_left::<3>();

            let mask =
                a_simd.simd_eq(r0) | a_simd.simd_eq(r1) | a_simd.simd_eq(r2) | a_simd.simd_eq(r3);

            // TODO replace code with pshufb/scatter
            let mask_idx = mask.to_bitmask() as usize;
            let (len, mask) = masks[mask_idx];
            a_simd.scatter(&mut buffer[buffer_idx..buffer_idx + 4], mask);
            buffer_idx += len;
            // let mut match_idx = 0;
            // while buffer_idx < buffer.len() && match_idx < a.len() {
            //     if a[match_idx] == 0 {
            //         break;
            //     }
            //     if matches[match_idx] {
            //         buffer[buffer_idx] = a[match_idx];
            //         buffer_idx += 1;
            //     }
            //     match_idx += 1;
            // }

            // while match_idx < a.len() && match_idx < a.len() {
            //     if a[match_idx] == 0 {
            //         break;
            //     }
            //     if matches[match_idx] {
            //         stash[*stash_idx] = a[match_idx];
            //         *stash_idx += 1;
            //     }
            //     match_idx += 1;
            // }

            if a.last().unwrap() < b.last().unwrap() {
                // a.fill(0);
                a_read = self.0.next_batch(&mut a);
            } else {
                // b.fill(0);
                b_read = self.1.next_batch(&mut b);
            };
        }
        return buffer_idx;
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
        self.next = target;
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
    use std::simd::usizex4;
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
        // let seed = [
        //     38, 249, 210, 60, 25, 139, 148, 27, 43, 55, 72, 59, 118, 36, 26, 1, 237, 60, 144, 244,
        //     23, 87, 77, 245, 98, 164, 9, 25, 117, 242, 86, 74,
        // ];
        run_seeded_test::<StdRng>(None, |mut rng| {
            for _ in 0..100 {
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
        let size: usize = rng.gen_range(1..20);
        let mut list = Vec::with_capacity(size);

        let mut doc_id = 0;
        for _ in 0..size {
            doc_id += rng.gen_range(1..1000);
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
        let masks: [(usize, usizex4); 16] = [
            (0, usizex4::from([4, 4, 4, 4])), // 0000
            (1, usizex4::from([4, 4, 4, 0])), // 0001
            (1, usizex4::from([4, 4, 0, 4])), // 0010
            (2, usizex4::from([4, 4, 0, 1])), // 0011
            (1, usizex4::from([4, 0, 4, 4])), // 0100
            (2, usizex4::from([4, 0, 4, 1])), // 0101
            (2, usizex4::from([4, 0, 1, 4])), // 0110
            (3, usizex4::from([4, 0, 1, 2])), // 0111
            (1, usizex4::from([0, 4, 4, 4])), // 1000
            (2, usizex4::from([0, 4, 4, 1])), // 1001
            (2, usizex4::from([0, 4, 1, 4])), // 1010
            (3, usizex4::from([0, 4, 1, 2])), // 1011
            (2, usizex4::from([0, 1, 4, 4])), // 1100
            (3, usizex4::from([0, 1, 4, 2])), // 1101
            (3, usizex4::from([0, 1, 2, 4])), // 1110
            (4, usizex4::from([0, 1, 2, 3])), // 1111
        ];
        let mut output = [0u64; 4];
        let (len, mask) = &masks[5];
        a.scatter(&mut output, *mask);
        assert_eq!(*len, 2);
        assert_eq!(output, [2, 4, 0, 0]);
    }
}
