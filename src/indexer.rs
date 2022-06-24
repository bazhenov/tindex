use crate::{
    encoding::{Encoder, PlainTextEncoder},
    indexer::mysql::MySqlSource,
    prelude::*,
};
use clap::Parser;
use futures::{Stream, TryStreamExt};
use std::{env, fs::File, path::PathBuf, pin::Pin};

#[derive(Parser, Debug)]
pub struct Opts {
    path: PathBuf,
}

type U64Stream<'a> = Pin<Box<dyn Stream<Item = Result<u64>> + 'a>>;

pub async fn main(opts: Opts) -> Result<()> {
    let url = env::var("DB_URL")?;
    println!("Connecting...");
    let mut db = MySqlSource::new(&url).await?;

    let queries = vec![Query {
        name: "sss".to_string(),
        query: "select id from register_user where adddate > NOW() - INTERVAL 1 DAY".to_string(),
    }];

    for q in queries {
        println!("Querying {}...", q.name);

        let path = opts.path.join(q.name).with_extension("idx");
        let file = File::create(path)?;

        let rows = db.execute(&q.query)?;
        write(rows, PlainTextEncoder(file)).await?;
    }

    Ok(())
}

async fn write(mut rows: U64Stream<'_>, mut sink: impl Encoder) -> Result<()> {
    while let Some(id) = rows.try_next().await? {
        sink.write(id)?;
    }
    Ok(())
}

struct Query {
    name: String,
    query: String,
}

trait Source<'a> {
    type RecordIterator: Stream<Item = Result<u64>>;

    fn execute(&'a mut self, query: &'a str) -> Result<Self::RecordIterator>;
}

mod mysql {
    use super::*;
    use futures::StreamExt;
    use sqlx::{mysql::MySqlRow, MySql, MySqlPool, Pool, Row};

    pub struct MySqlSource(Pool<MySql>);

    impl MySqlSource {
        pub async fn new(url: &str) -> Result<Self> {
            Ok(Self(MySqlPool::connect(&url).await?))
        }
    }

    impl<'a> Source<'a> for MySqlSource {
        type RecordIterator = U64Stream<'a>;

        fn execute(&'a mut self, query: &'a str) -> Result<Self::RecordIterator> {
            let rows = sqlx::query(query).fetch(&self.0);
            Ok(Box::pin(rows.map(read_record)))
        }
    }

    fn read_record(input: std::result::Result<MySqlRow, sqlx::Error>) -> Result<u64> {
        let id = input?.try_get::<i32, _>(0)?;
        Ok(u64::try_from(id)?)
    }
}
