use auditorium::{
    config::{Config, MySqlQuery, MySqlServer},
    encoding::{Encoder, PlainTextEncoder},
    prelude::*,
};
use clap::Parser;
use std::{
    env,
    fs::File,
    path::PathBuf,
    thread::{self},
};

#[derive(Parser, Debug)]
pub struct Opts {
    #[clap(long, default_value = "config.yaml")]
    config: PathBuf,
    path: PathBuf,
}

pub fn main(opts: Opts) -> Result<()> {
    let config = read_config(&opts.config).context(ReadingConfigFile(opts.config.clone()))?;

    let mut handles = vec![];
    for mysql in config.mysql {
        let db = mysql::connect(&mysql).context(ConnectingSource(mysql.name.clone()))?;

        let path = opts.path.clone();
        let name = mysql.name.clone();
        let handle = thread::spawn(move || query_worker(name, db, mysql.queries, path));
        handles.push(handle);
    }

    for h in handles {
        h.join().unwrap().context("Ooops")?;
    }

    Ok(())
}

fn query_worker<T: Source>(
    name: String,
    mut db: T,
    queries: Vec<T::Query>,
    path: PathBuf,
) -> Result<()> {
    for q in &queries {
        let path = path.join(q.name()).with_extension("idx");
        info!("Querying {} {}...", &name, q.name());
        let mut ids = db.execute(q)?;
        ids.sort_unstable();
        let file = File::create(&path)?;
        write(ids, PlainTextEncoder(file))?;
    }
    Ok(())
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
}

trait Source {
    type Query: NamedQuery;

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
        Ok(MySqlSource(conn))
    }

    pub struct MySqlSource(Conn);

    impl NamedQuery for MySqlQuery {
        fn name(&self) -> &str {
            &self.name
        }
    }

    impl Source for MySqlSource {
        type Query = MySqlQuery;

        fn execute(&mut self, query: &MySqlQuery) -> Result<Vec<u64>> {
            Ok(self.0.exec_map(&query.sql, (), |id| id)?)
        }
    }
}
