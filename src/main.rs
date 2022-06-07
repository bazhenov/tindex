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
            a: Box::new(self),
            b: Box::new(b),
            a_value: None,
            b_value: None,
        }
    }
}

struct Merge {
    a: Box<dyn PostingList>,
    b: Box<dyn PostingList>,
    a_value: Option<u64>,
    b_value: Option<u64>
}

struct Intersect {
    a: Box<dyn PostingList>,
    b: Box<dyn PostingList>,
}

impl Iterator for Merge {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        <Self as PostingList>::next(self)
    }
}

impl Iterator for Intersect {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        <Self as PostingList>::next(self)
    }
}

impl Iterator for RangePostingList {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        <Self as PostingList>::next(self)
    }
}

impl PostingList for Merge {
    fn next(&mut self) -> Option<u64> {
        loop {
            let a = self.a_value.take().or_else(|| self.a.next());
            let b = self.b_value.take().or_else(|| self.b.next());

            match (a, b) {
                (Some(a), Some(b)) => {
                    if a == b {
                        return Some(a);
                    } else if a < b {
                        self.b_value.replace(b);
                        return Some(a);
                    } else {
                        self.a_value.replace(a);
                        return Some(b);
                    }
                }
                (Some(a), None) => return Some(a),
                (None, Some(b)) => return Some(b),
                (None, None) => return None,
            }
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
