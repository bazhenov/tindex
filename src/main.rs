use auditorium::{
    indexer,
    prelude::*,
    serve::{self, ServeOpts},
};
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
    Serve(ServeOpts),
    Index,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    match Args::parse().action {
        Subcommand::Index => indexer::main().await?,
        Subcommand::Serve(opts) => serve::main(opts).await?,
    }
    Ok(())
}
