use auditorium::{indexer, prelude::*, serve};
use clap::Parser;
use dotenv::dotenv;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(subcommand)]
    action: Subcommand,
}

#[derive(Parser, Debug)]
enum Subcommand {
    Serve(serve::Opts),
    Index(indexer::Opts),
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    match Args::parse().action {
        Subcommand::Index(opts) => indexer::main(opts).await?,
        Subcommand::Serve(opts) => serve::main(opts).await?,
    }
    Ok(())
}
