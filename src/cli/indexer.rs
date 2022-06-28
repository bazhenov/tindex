use auditorium::{
    config::{Config, MySqlQuery, MySqlServer},
    encoding::{Encoder, PlainTextEncoder},
    prelude::*,
};
use chrono::Utc;
use clap::Parser;
use futures::{Stream, StreamExt};
use std::{env, fs::File, path::PathBuf, pin::Pin, sync::Arc};
use tokio::time::{sleep_until, Instant};

use self::mysql::MySqlSource;

#[derive(Parser, Debug)]
pub struct Opts {
    #[clap(long, default_value = "config.yaml")]
    config: PathBuf,
    path: PathBuf,
}

type U64Stream<'a> = Pin<Box<dyn Stream<Item = Result<u64>> + Send + 'a>>;

pub async fn main(opts: Opts) -> Result<()> {
    let config = read_config(&opts.config).context(ReadingConfigFile(opts.config.clone()))?;
    for mysql in config.mysql {
        let db = mysql::connect(&mysql)
            .await
            .context(ConnectingSource(mysql.name.clone()))?;
        let db = Arc::new(db);

        let mut handles = vec![];
        for q in mysql.queries {
            let path = opts.path.join(&q.name).with_extension("idx");
            let db = db.clone();
            handles.push(tokio::spawn(query_worker(db, q, path)));
        }

        for h in handles {
            h.await??;
        }
    }

    Ok(())
}

async fn query_worker(db: Arc<MySqlSource>, q: MySqlQuery, path: PathBuf) -> Result<()> {
    loop {
        info!("Querying {}...", &q.name);
        let rows = db.execute(&q.sql)?;
        let rows = sort(rows).await?;
        let file = File::create(&path)?;
        write(rows, PlainTextEncoder(file))?;

        let next_execution = q.schedule.upcoming(Utc).take(1).next().unwrap();
        let duration = next_execution - Utc::now();

        info!("Next execution of {} on {}", &q.name, next_execution);
        sleep_until(Instant::now() + duration.to_std()?).await;
    }
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

    fn execute<'s>(&'s self, query: &'a str) -> Result<Self::RecordIterator>
    where
        's: 'a;
}

/// Код позволяющий системе читать данные из MySQL
mod mysql {
    use super::*;
    use futures::StreamExt;
    use sqlx::{
        mysql::{MySqlPoolOptions, MySqlRow},
        MySql, Pool, Row,
    };

    pub async fn connect(mysql: &MySqlServer) -> Result<MySqlSource> {
        trace!("Connecting mysql source {}...", mysql.name);
        let var_name = format!("{}_MYSQL_URL", mysql.name.to_uppercase());
        let url = env::var(var_name)?;
        let pool = MySqlPoolOptions::new()
            .max_connections(mysql.max_connections.unwrap_or(1))
            .connect(&url)
            .await?;
        Ok(MySqlSource(pool))
    }

    pub struct MySqlSource(Pool<MySql>);

    impl<'a> Source<'a> for MySqlSource {
        type RecordIterator = U64Stream<'a>;

        fn execute<'s>(&'s self, query: &'a str) -> Result<Self::RecordIterator>
        where
            's: 'a,
        {
            let rows = sqlx::query(query).fetch(&self.0);
            Ok(Box::pin(rows.map(read_record)))
        }
    }

    fn read_record(input: std::result::Result<MySqlRow, sqlx::Error>) -> Result<u64> {
        let id = input?.try_get::<i32, _>(0)?;
        Ok(u64::try_from(id)?)
    }
}
