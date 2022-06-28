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

    let mut list = parse_query(&opts.query, &index)?;
    while let Some(id) = list.next()? {
        println!("{}", id);
    }
    Ok(())
}
