use std::ops::Range;

fn main() {
    println!("Hello, world!");
}

trait PostingList: Iterator<Item = u64> {
    fn next(&mut self) -> Option<u64>;

    fn intersect<B>(self, b: B) -> Intersect
    where
        Self: Sized + 'static,
        B: PostingList + 'static,
    {
        Intersect {
            a: Box::new(self),
            b: Box::new(b),
        }
    }

    fn merge<B>(self, b: B) -> Merge
    where
        Self: Sized + 'static,
        B: PostingList + 'static,
    {
        Merge {
            a: Peekable::new(Box::new(self)),
            b: Peekable::new(Box::new(b)),
        }
    }
}

struct Intersect {
    a: Box<dyn PostingList>,
    b: Box<dyn PostingList>,
}

struct Peekable<T>(T, Option<u64>);

impl<T: AsMut<dyn PostingList>> Peekable<T> {
    fn new(mut inner: T) -> Self {
        let value = PostingList::next(inner.as_mut());
        Self(inner, value)
    }

    fn value(&self) -> Option<u64> {
        self.1
    }

    fn move_next(&mut self) -> Option<u64> {
        self.1 = PostingList::next(self.0.as_mut());
        self.1
    }
}

struct Merge {
    a: Peekable<Box<dyn PostingList>>,
    b: Peekable<Box<dyn PostingList>>,
}

impl PostingList for Merge {
    fn next(&mut self) -> Option<u64> {
        match (self.a.value(), self.b.value()) {
            (Some(a), Some(b)) => {
                if a == b {
                    self.a.move_next();
                    self.b.move_next();
                    Some(a)
                } else if a < b {
                    self.a.move_next();
                    Some(a)
                } else {
                    self.b.move_next();
                    Some(b)
                }
            }
            (Some(a), None) => {
                self.a.move_next();
                Some(a)
            }
            (None, Some(b)) => {
                self.b.move_next();
                Some(b)
            }
            (None, None) => None,
        }
    }
}

impl PostingList for Intersect {
    fn next(&mut self) -> Option<u64> {
        let mut a_value = None;
        let mut b_value = None;
        loop {
            let a = a_value.take().or_else(|| self.a.next());
            let b = b_value.take().or_else(|| self.b.next());

            match (a, b) {
                (Some(a), Some(b)) => {
                    if a == b {
                        return Some(a);
                    } else if a > b {
                        a_value.replace(a);
                    } else {
                        b_value.replace(b);
                    }
                }
                (_, _) => return None,
            }
        }
    }
}

fn range(r: Range<u64>) -> impl PostingList {
    RangePostingList(r)
}

struct RangePostingList(Range<u64>);

impl PostingList for RangePostingList {
    fn next(&mut self) -> Option<u64> {
        self.0.next()
    }
}

macro_rules! impl_Iterator {
    (for $($t:ty),+) => {
        $(impl Iterator for $t {
            type Item = u64;
            fn next(&mut self) -> Option<Self::Item> {
                <Self as PostingList>::next(self)
            }
        })*
    }
}

impl_Iterator!(for Merge, Intersect, RangePostingList);

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn check_intersect() {
        let a = range(0..5);
        let b = range(2..7);

        let i = a.intersect(b);
        let values = i.collect::<Vec<_>>();
        assert_eq!(values, vec![2, 3, 4]);
    }

    #[test]
    fn check_merge() {
        let a = range(0..3);
        let b = range(2..5);

        let i = a.merge(b);
        let values = i.collect::<Vec<_>>();
        assert_eq!(values, vec![0, 1, 2, 3, 4]);
    }
}
