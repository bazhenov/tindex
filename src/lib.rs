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
    fn next(&mut self) -> Result<Option<u64>>;

    fn fill_buffer(&mut self, buffer: &mut [u64]) -> Result<usize> {
        let mut capacity = 0;
        for i in 0..buffer.len() {
            if let Some(v) = self.next()? {
                buffer[i] = v;
                capacity += 1;
            } else {
                break;
            }
        }
        Ok(capacity)
    }

    fn advance(&mut self, target: u64) -> Result<Option<u64>> {
        while let Some(n) = self.next()? {
            if n >= target {
                return Ok(Some(n));
            }
        }
        Ok(None)
    }

    fn to_vec(mut self) -> Result<Vec<u64>>
    where
        Self: Sized,
    {
        let mut result = vec![];
        while let Some(item) = self.next()? {
            result.push(item)
        }
        Ok(result)
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
    buf: [u64; 16],
    pos: usize,
    capacity: usize,
}

impl<T: PostingListDecoder + 'static> From<T> for PostingList {
    fn from(source: T) -> Self {
        Self {
            decoder: Box::new(source),
            buf: Default::default(),
            pos: 0,
            capacity: 0,
        }
    }
}

impl PostingList {
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<u64>> {
        if self.pos >= self.capacity {
            self.fill_buffer()?;
        }
        let pos = self.pos;
        if pos >= self.capacity {
            return Ok(None);
        }

        self.pos += 1;
        Ok(Some(self.buf[pos]))
    }

    /// Возвращает первый элемент в потоке равный или больший чем переданный `target`
    pub fn advance(&mut self, target: u64) -> Result<Option<u64>> {
        if let Some(c) = self.current()? {
            if c >= target {
                return Ok(Some(c));
            }
        }
        if self.pos < self.capacity && self.buf[self.pos] < target {
            while let Some(n) = self.next()? {
                if n >= target {
                    return Ok(Some(n));
                }
            }
            Ok(None)
        } else {
            self.decoder.advance(target)
        }
    }

    pub fn current(&mut self) -> Result<Option<u64>> {
        if self.pos >= self.capacity {
            self.fill_buffer()?;
        }
        let pos = self.pos;
        if pos >= self.capacity {
            Ok(None)
        } else {
            Ok(Some(self.buf[pos]))
        }
    }

    fn fill_buffer(&mut self) -> Result<()> {
        self.capacity = self.decoder.fill_buffer(&mut self.buf)?;
        self.pos = 0;
        Ok(())
    }
}

pub struct Merge(PostingList, PostingList);

impl PostingListDecoder for Merge {
    fn next(&mut self) -> Result<Option<u64>> {
        let a = self.0.current()?;
        let b = self.1.current()?;
        if let Some((a, b)) = a.zip(b) {
            if a <= b {
                self.0.next()?;
            }
            if b <= a {
                self.1.next()?;
            }
            return Ok(Some(a.min(b)));
        }
        if let Some(a) = self.0.current()? {
            self.0.next()?;
            return Ok(Some(a));
        }
        if let Some(b) = self.1.current()? {
            self.1.next()?;
            return Ok(Some(b));
        }
        Ok(None)
    }
}

pub struct Intersect(PostingList, PostingList);

impl PostingListDecoder for Intersect {
    fn next(&mut self) -> Result<Option<u64>> {
        if let Some(mut target) = self.0.next()? {
            loop {
                match self.1.advance(target)? {
                    Some(v) if v == target => return Ok(Some(target)),
                    Some(v) => target = v,
                    None => return Ok(None),
                }

                match self.0.advance(target)? {
                    Some(v) if v == target => return Ok(Some(target)),
                    Some(v) => target = v,
                    None => return Ok(None),
                }
            }
        }
        Ok(None)
    }
}

pub struct Exclude(PostingList, PostingList);

impl PostingListDecoder for Exclude {
    fn next(&mut self) -> Result<Option<u64>> {
        while let Some(a) = self.0.next()? {
            if self.1.advance(a)? == Some(a) {
                continue;
            }
            return Ok(Some(a));
        }
        Ok(None)
    }
}

#[derive(Clone)]
pub struct RangePostingList {
    start: u64,
    end: u64,
    pos: u64,
}

impl RangePostingList {
    pub fn new(range: Range<u64>) -> Self {
        Self {
            start: range.start,
            end: range.end,
            pos: range.start,
        }
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> u64 {
        self.end - self.start
    }
}

impl PostingListDecoder for RangePostingList {
    fn next(&mut self) -> Result<Option<u64>> {
        if self.pos >= self.end {
            Ok(None)
        } else {
            let pos = self.pos;
            self.pos += 1;
            Ok(Some(pos))
        }
    }

    fn fill_buffer(&mut self, buffer: &mut [u64]) -> Result<usize> {
        let len = buffer.len();

        let mut idx = 0;
        while len > idx + 4 && self.end > self.pos + 4 {
            buffer[idx] = self.pos;
            idx += 1;
            self.pos += 1;

            buffer[idx] = self.pos;
            idx += 1;
            self.pos += 1;

            buffer[idx] = self.pos;
            idx += 1;
            self.pos += 1;

            buffer[idx] = self.pos;
            idx += 1;
            self.pos += 1;
        }

        for _ in idx..len {
            if self.pos < self.end {
                buffer[idx] = self.pos;
                idx += 1;
                self.pos += 1;
            } else {
                break;
            }
        }

        Ok(idx)
    }

    fn advance(&mut self, target: u64) -> Result<Option<u64>> {
        self.pos = target + 1;
        if target < self.end {
            Ok(Some(target))
        } else {
            Ok(None)
        }
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

    impl Into<Vec<u64>> for PostingList {
        fn into(mut self) -> Vec<u64> {
            let mut result = vec![];
            while let Ok(Some(v)) = self.next() {
                result.push(v);
            }
            result
        }
    }

    #[test]
    fn check_iterate() -> Result<()> {
        let a = RangePostingList::new(0..4);
        let list: PostingList = a.into();
        let vec: Vec<u64> = list.into();

        assert_eq!(vec, vec![0, 1, 2, 3]);
        Ok(())
    }

    #[test]
    fn check_intersect() -> Result<()> {
        let a = RangePostingList::new(0..5);
        let b = RangePostingList::new(2..7);

        let values = Intersect(a.into(), b.into()).to_vec()?;
        assert_eq!(values, vec![2, 3, 4]);
        Ok(())
    }

    #[test]
    fn check_merge() -> Result<()> {
        let a = RangePostingList::new(0..3);
        let b = RangePostingList::new(2..5);

        let values = Merge(a.into(), b.into()).to_vec()?;
        assert_eq!(values, vec![0, 1, 2, 3, 4]);
        Ok(())
    }

    #[test]
    fn check_exclude() -> Result<()> {
        let a = RangePostingList::new(0..6);
        let b = RangePostingList::new(2..4);

        let values = Exclude(a.into(), b.into()).to_vec()?;
        assert_eq!(values, vec![0, 1, 4, 5]);
        Ok(())
    }
}
