use tindex::{prelude::*, query::parse_query, DirectoryIndex};
use clap::Parser;
use rocket::{get, http::Status, routes, State};
use std::{cmp::Ordering, ops::Deref, path::PathBuf};

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
    while let Some(id) = list.next().map_err(|_| Status::InternalServerError)? {
        result.push_str(&format!("{}\n", id));
    }
    Ok(result)
}

#[get("/check?<query>&<id>")]
fn check(query: &str, id: u64, index: &State<app::Index>) -> HttpResult<&'static str> {
    let index = index.deref();
    let mut list = parse_query(query, index).map_err(|_| Status::BadRequest)?;

    while let Some(next) = list.next().map_err(|_| Status::InternalServerError)? {
        match next.cmp(&id) {
            Ordering::Less => continue,
            Ordering::Equal => return Ok("true"),
            Ordering::Greater => return Ok("false"),
        }
    }
    Ok("false")
}
