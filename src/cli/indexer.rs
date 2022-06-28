use auditorium::{
    config::Config,
    encoding::{Encoder, PlainTextEncoder},
    prelude::*,
};
use clap::Parser;
use futures::{Stream, StreamExt};
use mysql::MySqlSource;
use std::{env, fs::File, path::PathBuf, pin::Pin};

#[derive(Parser, Debug)]
pub struct Opts {
    #[clap(long, default_value = "config.yaml")]
    config: PathBuf,
    path: PathBuf,
}

type U64Stream<'a> = Pin<Box<dyn Stream<Item = Result<u64>> + 'a>>;

pub async fn main(opts: Opts) -> Result<()> {
    let config = read_config(&opts.config).context(ReadingConfigFile(opts.config))?;
    for mysql in config.mysql {
        info!("Connecting mysql source {}...", mysql.name);
        let var_name = format!("{}_MYSQL_URL", mysql.name.to_uppercase());
        let url = env::var(var_name)?;
        let mut db = MySqlSource::new(&url).await?;

        for q in mysql.queries {
            info!("Querying {}...", q.name);

            let path = opts.path.join(q.name).with_extension("idx");
            let file = File::create(path)?;

            let rows = db.execute(&q.sql)?;
            let rows = sort(rows).await?;
            write(rows, PlainTextEncoder(file))?;
        }
    }

    Ok(())
}

fn read_config(path: &PathBuf) -> Result<Config> {
    let file = File::open(path)?;
    let config = serde_yaml::from_reader(file)?;
    Ok(config)
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

trait Source<'a> {
    type RecordIterator: Stream<Item = Result<u64>>;

    fn execute(&'a mut self, query: &'a str) -> Result<Self::RecordIterator>;
}

/// Код позволяющий системе читать данные из MySQL
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
