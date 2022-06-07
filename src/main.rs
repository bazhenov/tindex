use std::ops::Range;

fn main() {
    println!("Hello, world!");
}

trait PostingList {
    fn next(&mut self) -> Option<u64>;

    fn intersect<B>(self, b: B) -> Intersect
    where
        Self: Sized + 'static,
        B: PostingList + 'static,
    {
        Intersect {
            a: Box::new(self),
            b: Box::new(b),
            a_value: None,
            b_value: None,
        }
    }
}

struct Intersect {
    a: Box<dyn PostingList>,
    b: Box<dyn PostingList>,

    a_value: Option<u64>,
    b_value: Option<u64>,
}

impl PostingList for Intersect {
    fn next(&mut self) -> Option<u64> {
        loop {
            let a = self.a_value.take().or_else(|| self.a.next());
            let b = self.b_value.take().or_else(|| self.b.next());

            match (a, b) {
                (Some(a), Some(b)) => {
                    if a == b {
                        return Some(a)
                    } else if a > b {
                        self.a_value = Some(a)
                    } else {
                        self.b_value = Some(b)
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
    fn check_iter() {
        let a = range(0..5);
        let b = range(2..7);

        let mut i = a.intersect(b);
        assert_eq!(i.next(), Some(2));
        assert_eq!(i.next(), Some(3));
        assert_eq!(i.next(), Some(4));
        assert_eq!(i.next(), None);
    }
}
