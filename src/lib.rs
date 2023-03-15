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

pub const NO_DOC: u64 = 0;

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
    fn next(&mut self) -> u64;

    fn to_vec(mut self) -> Vec<u64>
    where
        Self: Sized,
    {
        let mut result = vec![];
        loop {
            let doc_id = self.next();
            if doc_id == NO_DOC {
                break;
            }
            result.push(doc_id);
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

pub struct PostingList(Box<dyn PostingListDecoder>, Option<u64>);

impl<T: PostingListDecoder + 'static> From<T> for PostingList {
    fn from(source: T) -> Self {
        Self(Box::new(source), None)
    }
}

impl PostingList {
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> u64 {
        let doc_id = self.0.next();
        self.1 = Some(doc_id);
        doc_id
    }

    /// Возвращает первый элемент в потоке равный или больший чем переданный `target`
    pub fn advance(&mut self, target: u64) -> u64 {
        if let Some(c) = self.1 {
            if c >= target {
                return c;
            }
        }
        loop {
            let doc_id = self.next();
            if doc_id == NO_DOC {
                self.1 = None;
                return NO_DOC;
            }
            if doc_id >= target {
                self.1 = Some(doc_id);
                return doc_id;
            }
        }
    }

    pub fn current(&mut self) -> u64 {
        if self.1.is_none() {
            self.1 = Some(self.next());
        }
        self.1.unwrap()
    }
}

pub struct Merge(PostingList, PostingList);

impl PostingListDecoder for Merge {
    fn next(&mut self) -> u64 {
        let a = self.0.current();
        let b = self.1.current();
        match (a, b) {
            (NO_DOC, NO_DOC) => NO_DOC,
            (a, NO_DOC) => {
                self.0.next();
                a
            }
            (NO_DOC, b) => {
                self.1.next();
                b
            }
            (a, b) => {
                if a <= b {
                    self.0.next();
                }
                if b <= a {
                    self.1.next();
                }
                a.min(b)
            }
        }
    }
}

pub struct Intersect(PostingList, PostingList);

impl PostingListDecoder for Intersect {
    fn next(&mut self) -> u64 {
        let mut target = self.0.next();
        if target == NO_DOC {
            return NO_DOC;
        }
        loop {
            let advance = self.1.advance(target);
            match advance {
                NO_DOC => return NO_DOC,
                candidate if candidate == target => return target,
                candidate => target = candidate,
            };
            match self.0.advance(target) {
                NO_DOC => return NO_DOC,
                candidate if candidate == target => return target,
                candidate => target = candidate,
            };
        }
    }
}

pub struct Exclude(PostingList, PostingList);

impl PostingListDecoder for Exclude {
    fn next(&mut self) -> u64 {
        loop {
            let doc_id = self.0.next();
            if doc_id == NO_DOC {
                return NO_DOC;
            }
            if self.1.advance(doc_id) != doc_id {
                return doc_id;
            }
        }
    }
}

#[derive(Clone)]
pub struct RangePostingList(Range<u64>);

impl RangePostingList {
    pub fn new(range: Range<u64>) -> Self {
        if range.start == NO_DOC {
            panic!("Start should be greater than zero");
        }
        Self(range)
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> u64 {
        self.0.end - self.0.start
    }
}

impl PostingListDecoder for RangePostingList {
    fn next(&mut self) -> u64 {
        self.0.next().unwrap_or(NO_DOC)
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
