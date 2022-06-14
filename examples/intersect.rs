use auditorium::{encoding::PlainTextDecoder, intersect, prelude::*, PostingList};
use std::env::args;

fn main() -> Result<()> {
    let input = args().nth(1).unwrap();

    let a = PlainTextDecoder::open(&input)?;
    let b = PlainTextDecoder::open(&input)?;

    let mut intersect = intersect(a, b);
    while let Some(v) = intersect.next()? {
        println!("{}", v);
    }

    Ok(())
}
