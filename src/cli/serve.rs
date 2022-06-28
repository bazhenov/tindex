use auditorium::{prelude::*, query::parse_query, DirectoryIndex};
use clap::Parser;
use rocket::{get, http::Status, routes, State};
use std::{ops::Deref, path::PathBuf};

#[derive(Parser, Debug)]
pub struct Opts {
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
        .mount("/", routes![index])
        .manage(index)
        .launch()
        .await?;
    Ok(())
}

#[get("/?<query>")]
fn index(query: &str, index: &State<app::Index>) -> HttpResult<String> {
    let index = index.deref();
    let mut list = parse_query(query, index).map_err(|_| Status::BadRequest)?;
    let mut result = String::new();
    while let Some(id) = list.next().map_err(|_| Status::InternalServerError)? {
        result.push_str(&format!("{}\n", id));
    }
    Ok(result)
}
