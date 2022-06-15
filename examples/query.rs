use auditorium::prelude::*;
use auditorium::query::PostingLists;
use std::{env::args, path::PathBuf};

fn main() -> Result<()> {
    let input = args().nth(1).unwrap();
    let postings = PostingLists(PathBuf::from("."));

    let mut list = postings.parse_query(&input)?;
    while let Some(id) = list.next()? {
        println!("{}", id);
    }

    Ok(())
}
