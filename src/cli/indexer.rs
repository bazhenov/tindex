use auditorium::{
    encoding::{Encoder, PlainTextEncoder},
    prelude::*,
};
use clap::Parser;
use futures::{Stream, StreamExt};
use mysql::MySqlSource;
use std::{env, fs::File, path::PathBuf, pin::Pin};

#[derive(Parser, Debug)]
pub struct Opts {
    path: PathBuf,
}

type U64Stream<'a> = Pin<Box<dyn Stream<Item = Result<u64>> + 'a>>;

pub async fn main(opts: Opts) -> Result<()> {
    let url = env::var("DB_URL")?;
    info!("Connecting...");
    let mut db = MySqlSource::new(&url).await?;

    let queries = vec![
        Query {
            name: "new_users_1_day".to_string(),
            query: "select id from register_user where adddate > NOW() - INTERVAL 1 DAY"
                .to_string(),
        },
        Query {
            name: "bulletin_owners_1_day".to_string(),
            query: "select DISTINCT reg_user_id from bulletins where date_created > NOW() - INTERVAL 1 DAY"
                .to_string(),
        },
    ];

    for q in queries {
        info!("Querying {}...", q.name);

        let path = opts.path.join(q.name).with_extension("idx");
        let file = File::create(path)?;

        let rows = db.execute(&q.query)?;
        let rows = sort(rows).await?;
        write(rows, PlainTextEncoder(file))?;
    }

    Ok(())
}

async fn sort(rows: U64Stream<'_>) -> Result<Vec<u64>> {
    let mut vec = rows
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    vec.sort();
    Ok(vec)
}

fn write(rows: impl IntoIterator<Item = u64>, mut sink: impl Encoder) -> Result<()> {
    for id in rows {
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
