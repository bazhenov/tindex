use crate::prelude::*;
use clap::Parser;
use std::path::PathBuf;
use tindex::{query::parse_query, DirectoryIndex, NO_DOC};

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
    loop {
        let doc_id = list.next();
        if doc_id == NO_DOC {
            break;
        }
        println!("{}", doc_id);
    }
    Ok(())
}
