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
    pub fn next(&mut self) -> u64 {
        self.position += 1;
        if !self.ensure_buffer_has_data() {
            return NO_DOC;
        }
        self.buffer[self.position]
    }

    /// Возвращает первый элемент в потоке равный или больший чем переданный `target`
    pub fn advance(&mut self, target: u64) -> u64 {
        let current = self.current();
        if current == NO_DOC || current >= target {
            return current;
        }
        loop {
            let doc_id = self.next();
            if doc_id == NO_DOC || doc_id >= target {
                return doc_id;
            }
        }
    }

    pub fn current(&mut self) -> u64 {
        if !self.ensure_buffer_has_data() {
            return NO_DOC;
        }
        self.buffer[self.position]
    }

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
        fn next(intersect: &mut Intersect) -> u64 {
            let mut target = intersect.0.next();
            if target == NO_DOC {
                return NO_DOC;
            }
            loop {
                match intersect.1.advance(target) {
                    NO_DOC => return NO_DOC,
                    candidate if candidate == target => return target,
                    candidate => target = candidate,
                };
                match intersect.0.advance(target) {
                    NO_DOC => return NO_DOC,
                    candidate if candidate == target => return target,
                    candidate => target = candidate,
                };
            }
        }

        for i in 0..buffer.len() {
            let doc_id = next(self);
            if doc_id == NO_DOC {
                return i;
            }
            buffer[i] = doc_id;
        }
        return buffer.len();
    }
}

pub struct Exclude(PostingList, PostingList);

impl PostingListDecoder for Exclude {
    fn next_batch(&mut self, buffer: &mut PlBuffer) -> usize {
        fn next(exclude: &mut Exclude) -> u64 {
            loop {
                let doc_id = exclude.0.next();

                if doc_id == NO_DOC {
                    return NO_DOC;
                }
                if exclude.1.advance(doc_id) != doc_id {
                    return doc_id;
                }
            }
        }

        for i in 0..buffer.len() {
            let doc_id = next(self);
            if doc_id == NO_DOC {
                return i;
            }
            buffer[i] = doc_id;
        }
        return buffer.len();
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
}
