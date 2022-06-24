use crate::{
    encoding::{Encoder, PlainTextEncoder},
    indexer::mysql::MySqlSource,
    prelude::*,
};
use futures::{Stream, TryStreamExt};
use std::{env, fs::File};

pub async fn main() -> Result<()> {
    let url = env::var("DB_URL")?;
    println!("Connecting...");
    let mut slave = MySqlSource::new(&url).await?;

    println!("Querying...");
    let query = "select id from register_user where adddate > NOW() - INTERVAL 1 DAY";
    let mut rows = slave.execute(query)?;

    let path = "./test.data";
    let mut encoder = PlainTextEncoder(File::create(path)?);

    while let Some(id) = rows.try_next().await? {
        encoder.write(id)?;
    }

    Ok(())
}

trait Source<'a> {
    type RecordIterator: Stream<Item = Result<u64>>;

    fn execute(&'a mut self, query: &'a str) -> Result<Self::RecordIterator>;
}

mod mysql {
    use super::*;
    use futures::{Stream, StreamExt};
    use sqlx::{mysql::MySqlRow, MySql, MySqlPool, Pool, Row};
    use std::pin::Pin;

    pub struct MySqlSource(Pool<MySql>);

    impl MySqlSource {
        pub async fn new(url: &str) -> Result<Self> {
            Ok(Self(MySqlPool::connect(&url).await?))
        }
    }

    impl<'a> Source<'a> for MySqlSource {
        type RecordIterator = Pin<Box<dyn Stream<Item = Result<u64>> + 'a>>;

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
