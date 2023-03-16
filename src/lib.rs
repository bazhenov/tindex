extern crate pest;
#[macro_use]
extern crate pest_derive;

use encoding::PlainTextDecoder;
use prelude::*;
use std::{ops::Range, path::PathBuf};

pub mod clickhouse;
pub mod encoding;
pub mod mysql;
pub mod query;

pub mod prelude {
    use std::path::PathBuf;
    use thiserror::Error;

    pub type Result<T> = anyhow::Result<T>;
    pub type IoResult<T> = std::io::Result<T>;

    pub use anyhow::Context;
    pub use Error::*;

    pub use log::{debug, error, info, log, trace, warn};

    #[derive(Error, Debug)]
    pub enum Error {
        #[error("Opening index file: {0}")]
        OpeningIndexFile(PathBuf),

        #[error("Query worker panic")]
        QueryWorkerPanic,
    }
}

pub const NO_DOC: u64 = u64::MAX;
type PlBuffer = [u64];

pub trait Index: Send + Sync {
    type Iterator: PostingListDecoder + 'static;

    fn lookup(&self, name: &str) -> Result<Self::Iterator>;
}

pub struct DirectoryIndex(pub PathBuf);

impl Index for DirectoryIndex {
    type Iterator = PlainTextDecoder;

    fn lookup(&self, name: &str) -> Result<Self::Iterator> {
        let path = self.0.join(format!("{}.idx", name));
        PlainTextDecoder::open(&path).context(OpeningIndexFile(path))
    }
}

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
        return len;
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

pub fn intersect(a: PostingList, b: PostingList) -> PostingList {
    Intersect(a, b).into()
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
        return self.len > 0;
    }
}

pub struct Merge(PostingList, PostingList);

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

pub struct Intersect(PostingList, PostingList);

impl PostingListDecoder for Intersect {
    fn next_batch(&mut self, buffer: &mut PlBuffer) -> usize {
        let mut a = self.0.current();
        let mut b = self.1.current();
        let mut i = 0;
        while a != NO_DOC && b != NO_DOC {
            if a < b {
                a = self.0.advance(b);
            }
            if b < a {
                b = self.1.advance(a);
            }
            while a == b && a != NO_DOC && b != NO_DOC {
                buffer[i] = b;
                i += 1;
                a = self.0.next();
                b = self.1.next();
                if i >= buffer.len() {
                    return i;
                }
            }
        }
        i
    }
}

pub struct Exclude(PostingList, PostingList);

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
        for i in 0..len {
            buffer[i] = start + i as u64;
        }
        self.next += len as u64;
        len
    }
}

pub mod config {
    use super::*;
    use cron::Schedule;
    use serde::{de::Error, Deserialize, Deserializer};
    use std::str::FromStr;

    #[derive(Deserialize, PartialEq, Eq, Debug)]
    pub struct Config {
        pub mysql: Option<Vec<mysql::MySqlDatabase>>,
        pub clickhouse: Option<Vec<clickhouse::ClickhouseDatabase>>,
    }

    pub fn schedule_from_string<'de, D>(deserializer: D) -> std::result::Result<Schedule, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = Deserialize::deserialize(deserializer)?;
        Schedule::from_str(&s).map_err(D::Error::custom)
    }

    pub trait Database {
        type Connection: Connection;

        fn connect(&self) -> Result<Self::Connection>;
        fn list_queries(&self) -> &[<Self::Connection as Connection>::Query];
    }

    pub trait Query: Clone {
        fn name(&self) -> &str;
        fn schedule(&self) -> &cron::Schedule;
    }

    pub trait Connection {
        type Query: Query;

        fn name(&self) -> &str;
        fn execute(&mut self, query: &Self::Query) -> Result<Vec<u64>>;
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

    #[test]
    fn check_intersect() -> Result<()> {
        let a = RangePostingList::new(1..5);
        let b = RangePostingList::new(2..7);

        let values = Intersect(a.into(), b.into()).to_vec();
        assert_eq!(values, vec![2, 3, 4]);
        Ok(())
    }

    #[test]
    fn check_merge() -> Result<()> {
        let a = RangePostingList::new(1..3);
        let b = RangePostingList::new(2..5);

        let values = Merge(a.into(), b.into()).to_vec();
        assert_eq!(values, vec![1, 2, 3, 4]);
        Ok(())
    }

    #[test]
    fn check_exclude() -> Result<()> {
        let a = RangePostingList::new(1..6);
        let b = RangePostingList::new(2..4);

        let values = Exclude(a.into(), b.into()).to_vec();
        assert_eq!(values, vec![1, 4, 5]);
        Ok(())
    }

    #[test]
    fn check_no_exclude() -> Result<()> {
        let a = RangePostingList::new(1..1_000);
        let b = RangePostingList::new(1_000..2_000);

        let values = Exclude(a.into(), b.into()).to_vec();
        assert_eq!(999, values.len());
        Ok(())
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
        run_seeded_test::<StdRng>(None, |mut rng| {
            for _ in 0..100 {
                let a = random_posting_list(&mut rng);
                let b = random_posting_list(&mut rng);

                let expected = naive_intersect(&a.data, &b.data);
                let actual = Intersect(a.into(), b.into()).to_vec();

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
        let seed = seed.unwrap_or_else(|| {
            let mut seed = Default::default();
            thread_rng().fill(&mut seed);
            seed
        });

        let result = panic::catch_unwind(|| {
            f(R::from_seed(seed));
        });

        if result.is_err() {
            panic!(
                "Test are failed. Check following seed:\n\n  ==> seed: {:?}\n\n",
                seed
            );
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
}
