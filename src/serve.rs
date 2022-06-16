use crate::{prelude::*, query::parse_query, DirectoryIndex, Index};
use clap::Parser;
use rocket::{http::Status, State};
use std::{ops::Deref, path::PathBuf, sync::Mutex};

#[derive(Parser, Debug)]
pub struct ServeOpts {
    path: PathBuf,
}

type HttpResult<T> = std::result::Result<T, Status>;

struct IndexState(Mutex<Box<dyn Index>>);

pub async fn main(opts: ServeOpts) -> Result<()> {
    let index = Box::new(DirectoryIndex(opts.path));

    let _ = rocket::build()
        .mount("/", routes![index])
        .manage(IndexState(Mutex::new(index)))
        .launch()
        .await?;
    Ok(())
}

#[get("/?<query>")]
fn index(query: &str, index: &State<IndexState>) -> HttpResult<String> {
    let index = index.0.lock().unwrap();
    let mut list = parse_query(query, index.deref()).map_err(|_| Status::BadRequest)?;
    let mut result = String::new();
    while let Some(id) = list.next().map_err(|_| Status::InternalServerError)? {
        result.push_str(&format!("{}\n", id));
    }
    return Ok(result);
}
