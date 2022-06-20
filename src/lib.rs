extern crate pest;
#[macro_use]
extern crate pest_derive;
#[macro_use]
extern crate rocket;

use encoding::PlainTextDecoder;
use prelude::*;
use std::{
    cmp::Ordering,
    fs::File,
    io::BufReader,
    ops::Range,
    path::{Path, PathBuf},
};

pub mod encoding;
pub mod indexer;
pub mod query;
pub mod serve;

pub mod prelude {
    use std::path::PathBuf;
    use thiserror::Error;

    pub type Result<T> = anyhow::Result<T>;
    pub type IoResult<T> = std::io::Result<T>;

    pub use anyhow::Context;
    pub use Error::*;

    #[derive(Error, Debug)]
    pub enum Error {
        #[error("Opening index file: {0}")]
        OpeningIndexFile(PathBuf),

        #[error("Parsing query: '{0}'")]
        ParsingQuery(String),
    }
}

pub trait Index: Send + Sync {
    type Iterator: PostingList + 'static;

    fn lookup(&self, name: &str) -> Result<Self::Iterator>;
}

pub struct DirectoryIndex(PathBuf);

impl AsRef<DirectoryIndex> for &DirectoryIndex {
    fn as_ref(&self) -> &DirectoryIndex {
        self
    }
}

impl<T: AsRef<Path>> From<T> for DirectoryIndex {
    fn from(input: T) -> Self {
        Self(input.as_ref().to_path_buf())
    }
}

impl Index for DirectoryIndex {
    type Iterator = PlainTextDecoder;
    fn lookup(&self, name: &str) -> Result<Self::Iterator> {
        let path = self.0.join(format!("{}.idx", name));
        let file = File::open(&path).context(OpeningIndexFile(path))?;

        Ok(PlainTextDecoder(BufReader::new(file)))
    }
}

pub trait PostingList {
    fn next(&mut self) -> Result<Option<u64>>;

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

pub fn intersect<A, B>(a: A, b: B) -> impl PostingList
where
    A: PostingList + 'static,
    B: PostingList + 'static,
{
    let a: Box<dyn PostingList> = Box::new(a);
    let b: Box<dyn PostingList> = Box::new(b);

    Intersect(
        PositionedPostingList(a, None),
        PositionedPostingList(b, None),
    )
}

pub fn merge<A, B>(a: A, b: B) -> impl PostingList
where
    A: PostingList + 'static,
    B: PostingList + 'static,
{
    let a: Box<dyn PostingList> = Box::new(a);
    let b: Box<dyn PostingList> = Box::new(b);
    Merge(
        PositionedPostingList(a, None),
        PositionedPostingList(b, None),
    )
}

pub fn exclude<A, B>(a: A, b: B) -> impl PostingList
where
    A: PostingList + 'static,
    B: PostingList + 'static,
{
    let a: Box<dyn PostingList> = Box::new(a);
    let b: Box<dyn PostingList> = Box::new(b);
    Exclude(
        PositionedPostingList(a, None),
        PositionedPostingList(b, None),
    )
}

struct PositionedPostingList(Box<dyn PostingList>, Option<u64>);

impl PositionedPostingList {
    fn next(&mut self) -> Result<Option<u64>> {
        self.1 = self.0.next()?;
        Ok(self.1)
    }

    fn advance(&mut self, target: u64) -> Result<Option<u64>> {
        if let Some(c) = self.current()? {
            if c >= target {
                return Ok(Some(c));
            }
        }
        while let Some(n) = self.next()? {
            if n >= target {
                break;
            }
        }
        self.current()
    }

    fn current(&mut self) -> Result<Option<u64>> {
        if self.1.is_none() {
            self.1 = self.next()?;
        }
        Ok(self.1)
    }
}

pub struct Merge(PositionedPostingList, PositionedPostingList);

impl Merge {
    pub fn new(a: Box<dyn PostingList>, b: Box<dyn PostingList>) -> Self {
        Self(
            PositionedPostingList(a, None),
            PositionedPostingList(b, None),
        )
    }
}

impl PostingList for Merge {
    fn next(&mut self) -> Result<Option<u64>> {
        match (self.0.current()?, self.1.current()?) {
            (Some(a), Some(b)) => match a.cmp(&b) {
                Ordering::Equal => {
                    self.0.next()?;
                    self.1.next()?;
                    Ok(Some(a))
                }
                Ordering::Less => {
                    self.0.next()?;
                    Ok(Some(a))
                }
                Ordering::Greater => {
                    self.1.next()?;
                    Ok(Some(b))
                }
            },
            (Some(a), None) => {
                self.0.next()?;
                Ok(Some(a))
            }
            (None, Some(b)) => {
                self.1.next()?;
                Ok(Some(b))
            }
            (None, None) => Ok(None),
        }
    }
}

pub struct Intersect(PositionedPostingList, PositionedPostingList);

impl Intersect {
    pub fn new(a: Box<dyn PostingList>, b: Box<dyn PostingList>) -> Self {
        Self(
            PositionedPostingList(a, None),
            PositionedPostingList(b, None),
        )
    }
}

impl PostingList for Intersect {
    fn next(&mut self) -> Result<Option<u64>> {
        while let (Some(a), Some(b)) = (self.0.current()?, self.1.current()?) {
            match a.cmp(&b) {
                Ordering::Less => self.0.advance(b)?,
                Ordering::Greater => self.1.advance(a)?,
                Ordering::Equal => {
                    self.0.next()?;
                    self.1.next()?;
                    return Ok(Some(a));
                }
            };
        }
        Ok(None)
    }
}

pub struct Exclude(PositionedPostingList, PositionedPostingList);

impl Exclude {
    pub fn new(a: Box<dyn PostingList>, b: Box<dyn PostingList>) -> Self {
        Self(
            PositionedPostingList(a, None),
            PositionedPostingList(b, None),
        )
    }
}

impl PostingList for Exclude {
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
pub struct RangePostingList(pub Range<u64>);

impl RangePostingList {
    pub fn len(&self) -> u64 {
        self.0.end - self.0.start
    }
}

impl PostingList for RangePostingList {
    fn next(&mut self) -> Result<Option<u64>> {
        Ok(self.0.next())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_intersect() -> Result<()> {
        let a = RangePostingList(0..5);
        let b = RangePostingList(2..7);

        let values = intersect(a, b).to_vec()?;
        assert_eq!(values, vec![2, 3, 4]);
        Ok(())
    }

    #[test]
    fn check_merge() -> Result<()> {
        let a = RangePostingList(0..3);
        let b = RangePostingList(2..5);

        let values = merge(a, b).to_vec()?;
        assert_eq!(values, vec![0, 1, 2, 3, 4]);
        Ok(())
    }

    #[test]
    fn check_exclude() -> Result<()> {
        let a = RangePostingList(0..6);
        let b = RangePostingList(2..4);

        let values = exclude(a, b).to_vec()?;
        assert_eq!(values, vec![0, 1, 4, 5]);
        Ok(())
    }
}
