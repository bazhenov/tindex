mod cli;

use clap::Parser;
use dotenv::dotenv;
pub use tindex::prelude;
use tindex::prelude::*;
extern crate rocket;

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
