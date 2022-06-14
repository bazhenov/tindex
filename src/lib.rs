extern crate pest;
#[macro_use]
extern crate pest_derive;

use prelude::*;
use std::{cmp::Ordering, ops::Range};

pub mod encoding;
pub mod query;

pub mod prelude {
    pub type Result<T> = anyhow::Result<T>;
    pub type IoResult<T> = std::io::Result<T>;
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

pub struct Merge(PositionedPostingList, PositionedPostingList);

struct PositionedPostingList(Box<dyn PostingList>, Option<u64>);

impl PositionedPostingList {
    fn next(&mut self) -> Result<Option<u64>> {
        if self.1.is_some() {
            Ok(self.1.take())
        } else {
            self.1 = self.0.next()?;
            Ok(self.1)
        }
    }

    fn current(&mut self) -> Result<Option<u64>> {
        if self.1.is_none() {
            self.1 = self.next()?;
        }
        Ok(self.1)
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

impl PostingList for Intersect {
    fn next(&mut self) -> Result<Option<u64>> {
        while let (Some(a), Some(b)) = (self.0.current()?, self.1.current()?) {
            match a.cmp(&b) {
                Ordering::Less => self.0.next()?,
                Ordering::Greater => self.1.next()?,
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

impl PostingList for Exclude {
    fn next(&mut self) -> Result<Option<u64>> {
        while let Some(a) = self.0.current()? {
            if let Some(b) = self.1.current()? {
                match a.cmp(&b) {
                    Ordering::Less => {
                        self.0.next()?;
                        return Ok(Some(a));
                    }
                    Ordering::Greater => {
                        self.1.next()?;
                    }
                    Ordering::Equal => {
                        self.0.next()?;
                        self.1.next()?;
                    }
                };
            } else {
                return self.0.next();
            }
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
