use prelude::*;
use std::{cmp::Ordering, iter::Peekable, ops::Range};

pub mod encoding;

pub mod prelude {
    pub type Result<T> = anyhow::Result<T>;
    pub type IoResult<T> = std::io::Result<T>;
}

pub trait PostingList: Iterator<Item = u64> {
    fn to_vec(mut self) -> Result<Vec<u64>>
    where
        Self: Sized,
    {
        let mut result = vec![];
        while let Some(item) = self.next() {
            result.push(item)
        }
        Ok(result)
    }
}
impl<T: Iterator<Item = u64>> PostingList for T {}

pub fn intersect<A, B>(a: A, b: B) -> impl PostingList
where
    A: PostingList + 'static,
    B: PostingList + 'static,
{
    let a: Box<dyn PostingList> = Box::new(a);
    let b: Box<dyn PostingList> = Box::new(b);

    Intersect(a.peekable(), b.peekable())
}

pub fn merge<A, B>(a: A, b: B) -> impl PostingList
where
    A: PostingList + 'static,
    B: PostingList + 'static,
{
    let a: Box<dyn PostingList> = Box::new(a);
    let b: Box<dyn PostingList> = Box::new(b);
    Merge(a.peekable(), b.peekable())
}

pub fn exclude<A, B>(a: A, b: B) -> impl PostingList
where
    A: PostingList + 'static,
    B: PostingList + 'static,
{
    let a: Box<dyn PostingList> = Box::new(a);
    let b: Box<dyn PostingList> = Box::new(b);
    Exclude(a.peekable(), b.peekable())
}

pub struct Merge(
    Peekable<Box<dyn PostingList>>,
    Peekable<Box<dyn PostingList>>,
);

impl Iterator for Merge {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        match (self.0.peek().cloned(), self.1.peek().cloned()) {
            (Some(a), Some(b)) => match a.cmp(&b) {
                Ordering::Equal => {
                    self.0.next();
                    self.1.next();
                    Some(a)
                }
                Ordering::Less => {
                    self.0.next();
                    Some(a)
                }
                Ordering::Greater => {
                    self.1.next();
                    Some(b)
                }
            },
            (Some(a), None) => {
                self.0.next();
                Some(a)
            }
            (None, Some(b)) => {
                self.1.next();
                Some(b)
            }
            (None, None) => None,
        }
    }
}

pub struct Intersect(
    Peekable<Box<dyn PostingList>>,
    Peekable<Box<dyn PostingList>>,
);

impl Iterator for Intersect {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        while let (Some(a), Some(b)) = (self.0.peek(), self.1.peek()) {
            match a.cmp(b) {
                Ordering::Less => self.0.next(),
                Ordering::Greater => self.1.next(),
                Ordering::Equal => {
                    let value = *a;
                    self.0.next();
                    self.1.next();
                    return Some(value);
                }
            };
        }
        None
    }
}

pub struct Exclude(
    Peekable<Box<dyn PostingList>>,
    Peekable<Box<dyn PostingList>>,
);

impl Iterator for Exclude {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        while let Some(a) = self.0.peek() {
            if let Some(b) = self.1.peek() {
                match a.cmp(b) {
                    Ordering::Less => {
                        let value = *a;
                        self.0.next();
                        return Some(value);
                    }
                    Ordering::Greater => {
                        self.1.next();
                    }
                    Ordering::Equal => {
                        self.0.next();
                        self.1.next();
                    }
                };
            } else {
                return self.0.next();
            }
        }
        None
    }
}

#[derive(Clone)]
pub struct RangePostingList(pub Range<u64>);

impl RangePostingList {
    pub fn len(&self) -> u64 {
        self.0.end - self.0.start
    }
}

impl Iterator for RangePostingList {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        self.0.next()
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
