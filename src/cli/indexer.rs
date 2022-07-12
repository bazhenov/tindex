use auditorium::{
    config::{Config, MySqlQuery, MySqlServer},
    encoding::{Encoder, PlainTextEncoder},
    prelude::*,
};
use chrono::{DateTime, Utc};
use clap::Parser;
use cron::Schedule;
use std::{
    collections::BinaryHeap,
    env,
    fs::File,
    path::PathBuf,
    thread::{self, sleep},
    time::Duration,
};

#[derive(Parser, Debug)]
pub struct Opts {
    #[clap(long, default_value = "config.yaml")]
    config: PathBuf,
    path: PathBuf,
}

pub fn main(opts: Opts) -> Result<()> {
    let config = read_config(&opts.config).context(ReadingConfig(opts.config))?;

    let mut handles = vec![];
    for mysql in config.mysql {
        let db = mysql::connect(&mysql).context(ConnectingSource(mysql.name.clone()))?;

        let path = opts.path.clone();
        let handle = thread::spawn(move || db_worker(db, mysql.queries, path));
        handles.push(handle);
    }

    for h in handles {
        h.join()
            .map_err(|_| QueryWorkerPanic)?
            .context(QueryWorkerFailed)?;
    }

    Ok(())
}

fn db_worker<DB: Database>(mut db: DB, queries: Vec<DB::Query>, path: PathBuf) -> Result<()> {
    let mut heap = BinaryHeap::new();

    for q in queries {
        ScheduledQuery::schedule_next(q, &mut heap)
    }

    // Извлекаем самый ближайший запланированный запрос
    while let Some(ScheduledQuery(time, q)) = heap.pop() {
        sleep_until(time);

        info!("Querying {} {} {}...", db.name(), q.name(), time);
        let path = path.join(q.name()).with_extension("idx");

        let mut ids = db.execute(&q)?;
        ids.sort_unstable();
        let file = File::create(&path)?;
        write(ids, PlainTextEncoder(file))?;

        ScheduledQuery::schedule_next(q, &mut heap);
    }

    Ok(())
}

fn sleep_until(time: DateTime<Utc>) {
    while Utc::now() < time {
        sleep(Duration::from_secs(1));
    }
}

/// Запланированное выполнение запроса
///
/// Содержит запрос и время когда этот запрос по плану должен быть выполнен.
struct ScheduledQuery<Q>(DateTime<Utc>, Q);

impl<Q: NamedQuery> ScheduledQuery<Q> {
    fn schedule_next(q: Q, heap: &mut BinaryHeap<ScheduledQuery<Q>>) {
        if let Some(next_time) = q.schedule().upcoming(Utc).next() {
            heap.push(Self(next_time, q))
        }
    }
}

impl<Q> Eq for ScheduledQuery<Q> {}

impl<Q> Ord for ScheduledQuery<Q> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0).reverse()
    }
}

impl<Q> PartialOrd for ScheduledQuery<Q> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<Q> PartialEq for ScheduledQuery<Q> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

fn read_config(path: &PathBuf) -> Result<Config> {
    let file = File::open(path)?;
    let config = serde_yaml::from_reader(file)?;
    Ok(config)
}

fn write(rows: impl IntoIterator<Item = u64>, mut sink: impl Encoder) -> Result<()> {
    for id in rows {
        sink.write(id)?;
    }
    Ok(())
}

trait NamedQuery {
    fn name(&self) -> &str;

    fn schedule(&self) -> &Schedule;
}

trait Database {
    type Query: NamedQuery;

    fn name(&self) -> &str;

    fn execute(&mut self, query: &Self::Query) -> Result<Vec<u64>>;
}

/// Код позволяющий системе читать данные из MySQL
mod mysql {
    use super::*;
    use ::mysql::{prelude::Queryable, Conn, Opts};

    pub fn connect(mysql: &MySqlServer) -> Result<MySqlSource> {
        trace!("Connecting mysql source {}...", mysql.name);
        let var_name = format!("{}_MYSQL_URL", mysql.name.to_uppercase());
        let url = env::var(var_name)?;
        let conn = Conn::new(Opts::from_url(&url)?)?;
        Ok(MySqlSource(mysql.name.to_owned(), conn))
    }

    pub struct MySqlSource(String, Conn);

    impl NamedQuery for MySqlQuery {
        fn name(&self) -> &str {
            &self.name
        }

        fn schedule(&self) -> &Schedule {
            &self.schedule
        }
    }

    impl Database for MySqlSource {
        type Query = MySqlQuery;

        fn name(&self) -> &str {
            &self.0
        }
        fn execute(&mut self, query: &MySqlQuery) -> Result<Vec<u64>> {
            Ok(self.1.exec_map(&query.sql, (), |id| id)?)
        }
    }
}
