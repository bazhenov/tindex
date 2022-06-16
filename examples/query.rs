use auditorium::DirectoryIndex;
use auditorium::{prelude::*, query::parse_query};
use std::env::args;

fn main() -> Result<()> {
    let input = args().nth(1).unwrap();
    let postings = &DirectoryIndex::from(".");

    let mut list = parse_query(&input, postings).context(ParsingQuery(input.to_string()))?;
    while let Some(id) = list.next()? {
        println!("{}", id);
    }

    Ok(())
}
