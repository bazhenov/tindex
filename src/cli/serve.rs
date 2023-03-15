use clap::Parser;
use rocket::{get, http::Status, routes, State};
use std::{ops::Deref, path::PathBuf};
use tindex::{prelude::*, query::parse_query, DirectoryIndex, NO_DOC};

#[derive(Parser, Debug)]
#[clap(about = "Run REST API HTTP-server for a given index")]
pub struct Opts {
    /// path to an index
    path: PathBuf,
}

type HttpResult<T> = std::result::Result<T, Status>;

mod app {
    use super::DirectoryIndex;

    pub type Index = DirectoryIndex;
}

pub async fn main(opts: Opts) -> Result<()> {
    let index = DirectoryIndex(opts.path);

    let _ = rocket::build()
        .mount("/", routes![search, check])
        .manage(index)
        .launch()
        .await?;
    Ok(())
}

#[get("/search?<query>")]
fn search(query: &str, index: &State<app::Index>) -> HttpResult<String> {
    let index = index.deref();
    let mut list = parse_query(query, index).map_err(|_| Status::BadRequest)?;
    let mut result = String::new();
    loop {
        let doc_id = list.next();
        if doc_id == NO_DOC {
            break;
        }
        result.push_str(&format!("{}\n", doc_id));
    }
    Ok(result)
}

#[get("/check?<query>&<id>")]
fn check(query: &str, id: u64, index: &State<app::Index>) -> HttpResult<&'static str> {
    let index = index.deref();
    let mut list = parse_query(query, index).map_err(|_| Status::BadRequest)?;

    if list.advance(id) == id {
        Ok("true")
    } else {
        Ok("false")
    }
}
