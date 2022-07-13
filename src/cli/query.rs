use crate::prelude::*;
use auditorium::{query::parse_query, DirectoryIndex};
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
pub struct Opts {
    path: PathBuf,
    query: String,
}

pub async fn main(opts: Opts) -> Result<()> {
    let index = DirectoryIndex(opts.path);

    let query = opts.query;
    let mut list = parse_query(&query, &index)?;
    while let Some(id) = list.next()? {
        println!("{}", id);
    }
    Ok(())
}
