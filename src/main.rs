use std::{cmp::Ordering, iter::Peekable, ops::Range};

fn main() {
    println!("Hello, world!");
}

trait PostingList: Iterator<Item = u64> {}
impl<T: Iterator<Item = u64>> PostingList for T {}

fn intersect<A, B>(a: A, b: B) -> Intersect
where
    A: PostingList + 'static,
    B: PostingList + 'static,
{
    let a: Box<dyn PostingList> = Box::new(a);
    let b: Box<dyn PostingList> = Box::new(b);

    Intersect(a.peekable(), b.peekable())
}

fn merge<A, B>(a: A, b: B) -> Merge
where
    A: PostingList + 'static,
    B: PostingList + 'static,
{
    let a: Box<dyn PostingList> = Box::new(a);
    let b: Box<dyn PostingList> = Box::new(b);
    Merge(a.peekable(), b.peekable())
}

struct Merge(
    Peekable<Box<dyn PostingList>>,
    Peekable<Box<dyn PostingList>>,
);

impl Iterator for Merge {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        match (self.0.peek().cloned(), self.1.peek().cloned()) {
            (Some(a), Some(b)) => {
                if a == b {
                    self.0.next();
                    self.1.next();
                    Some(a)
                } else if a < b {
                    self.0.next();
                    Some(a)
                } else {
                    self.1.next();
                    Some(b)
                }
            }
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

struct Intersect(
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
        return None;
    }
}

struct RangePostingList(Range<u64>);

impl Iterator for RangePostingList {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        self.0.next()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn check_intersect() {
        let a = RangePostingList(0..5);
        let b = RangePostingList(2..7);

        let i = intersect(a, b);
        let values = i.collect::<Vec<_>>();
        assert_eq!(values, vec![2, 3, 4]);
    }

    #[test]
    fn check_merge() {
        let a = RangePostingList(0..3);
        let b = RangePostingList(2..5);

        let i = merge(a, b);
        let values = i.collect::<Vec<_>>();
        assert_eq!(values, vec![0, 1, 2, 3, 4]);
    }
}
