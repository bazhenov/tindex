use crate::prelude::*;
use tindex::{query::parse_query, DirectoryIndex};
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[clap(about = "Running query over index")]
pub struct Opts {
    /// path to an index
    path: PathBuf,

    /// query to run (eg. "crit1 & crit2")
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
