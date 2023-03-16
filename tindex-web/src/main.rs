mod cli;

use clap::Parser;
use dotenv::dotenv;
use tindex_core::prelude::*;
extern crate rocket;

pub mod clickhouse;
pub mod mysql;
pub mod query;

pub mod prelude {
    use std::path::PathBuf;
    use thiserror::Error;

    pub type Result<T> = anyhow::Result<T>;
    pub type IoResult<T> = std::io::Result<T>;

    pub use anyhow::Context;
    pub use Error::*;

    pub use log::{debug, error, info, log, trace, warn};

    #[derive(Error, Debug)]
    pub enum Error {
        #[error("Opening index file: {0}")]
        OpeningIndexFile(PathBuf),

        #[error("Query worker panic")]
        QueryWorkerPanic,
    }
}

pub mod config {
    use super::*;
    use cron::Schedule;
    use serde::{de::Error, Deserialize, Deserializer};
    use std::str::FromStr;

    #[derive(Deserialize, PartialEq, Eq, Debug)]
    pub struct Config {
        pub mysql: Option<Vec<mysql::MySqlDatabase>>,
        pub clickhouse: Option<Vec<clickhouse::ClickhouseDatabase>>,
    }

    pub fn schedule_from_string<'de, D>(deserializer: D) -> std::result::Result<Schedule, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = Deserialize::deserialize(deserializer)?;
        Schedule::from_str(&s).map_err(D::Error::custom)
    }

    pub trait Database {
        type Connection: Connection;

        fn connect(&self) -> Result<Self::Connection>;
        fn list_queries(&self) -> &[<Self::Connection as Connection>::Query];
    }

    pub trait Query: Clone {
        fn name(&self) -> &str;
        fn schedule(&self) -> &cron::Schedule;
    }

    pub trait Connection {
        type Query: Query;

        fn name(&self) -> &str;
        fn execute(&mut self, query: &Self::Query) -> Result<Vec<u64>>;
    }
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(subcommand)]
    action: Subcommand,
}

#[derive(Parser, Debug)]
enum Subcommand {
    Serve(cli::serve::Opts),
    Index(cli::indexer::IndexOpts),
    Update(cli::indexer::UpdateOpts),
    Query(cli::query::Opts),
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    env_logger::init();

    match Args::parse().action {
        Subcommand::Index(opts) => cli::indexer::do_index(opts)?,
        Subcommand::Update(opts) => cli::indexer::do_update(opts)?,
        Subcommand::Serve(opts) => cli::serve::main(opts).await?,
        Subcommand::Query(opts) => cli::query::main(opts).await?,
    }
    Ok(())
}
