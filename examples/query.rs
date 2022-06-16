use auditorium::{prelude::*, query::parse_query};
use auditorium::{DirectoryIndex, Index};
use std::env::args;

fn main() -> Result<()> {
    let input = args().nth(1).unwrap();
    let postings: Box<dyn Index> = Box::new(DirectoryIndex::from("."));

    let mut list = parse_query(&input, postings).context(ParsingQuery(input.to_string()))?;
    while let Some(id) = list.next()? {
        println!("{}", id);
    }

    Ok(())
}
