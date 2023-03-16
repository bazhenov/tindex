use crate::{
    config::{Config, Connection, Database, Query},
    prelude::*,
};
use chrono::{DateTime, Utc};
use clap::Parser;
use fn_error_context::context;
use std::{
    collections::{BinaryHeap, HashSet, LinkedList},
    fs::File,
    path::{Path, PathBuf},
    thread::{self, sleep, JoinHandle},
    time::Duration,
};
use tindex_core::encoding::{Encoder, PlainTextEncoder};

#[derive(Parser, Debug)]
#[clap(about = "Run indexation for all queries in a config")]
pub struct IndexOpts {
    #[clap(long, default_value = "config.yaml")]
    config: PathBuf,

    /// path to an index
    path: PathBuf,
}

/// Запускает цикл обновления всех запросов в соответствии с расписанием
pub fn do_index(opts: IndexOpts) -> Result<()> {
    let config = read_config(&opts.config)?;

    let mut handles = LinkedList::new();
    handles.extend(start_workers(config.mysql, &opts));
    handles.extend(start_workers(config.clickhouse, &opts));
    wait_for_all_workers(handles)
}

/// Запускает потоки (по одному на БД) и возвращает из [`JoinHandle`]'ы
fn start_workers<DB: Database + Send + 'static>(
    databases: Option<Vec<DB>>,
    opts: &IndexOpts,
) -> Vec<JoinHandle<Result<()>>> {
    let mut handles = vec![];
    let databases = databases.unwrap_or_default();
    for db in databases {
        let path = opts.path.clone();
        let handle = thread::spawn(move || db_worker(db, path));
        handles.push(handle);
    }
    handles
}

/// Блокирует выполнение до тех пор пока все потоки не будут выполнены или один из них не вернет отказ
///
/// В стандартной библотеке нет метода для блокировки до момента когда будет завершен один [`JoinHandle`] из набора.
/// Поэтому, этот метод через polling пытается определить какие из handle уже завершены, после чего проверяет их
/// результат и снимает их с мониторинга.
fn wait_for_all_workers<T>(mut handles: LinkedList<JoinHandle<Result<T>>>) -> Result<()> {
    while !handles.is_empty() {
        for _ in 0..handles.len() {
            // так как мы итерируемся строго по длине списка, в голове списка всегда есть элемент
            let handle = handles.pop_front().unwrap();

            if handle.is_finished() {
                handle
                    .join()
                    .map_err(|_| QueryWorkerPanic)?
                    .context("Query worker failed")?;
            } else {
                handles.push_back(handle);
            }
        }
        sleep(Duration::from_millis(100));
    }
    Ok(())
}

#[derive(Parser, Debug)]
#[clap(about = "Updates single query in an index")]
pub struct UpdateOpts {
    #[clap(long, default_value = "config.yaml")]
    config: PathBuf,

    /// path to an index
    path: PathBuf,

    /// names of queries to update
    queries: Vec<String>,
}

/// Обновляет указанные запросы по имени
pub fn do_update(opts: UpdateOpts) -> Result<()> {
    let config = read_config(&opts.config)?;

    let mut query_names = HashSet::new();
    query_names.extend(opts.queries);

    for mysql in &config.mysql.unwrap_or_default() {
        run_queries(mysql, &query_names, &opts.path)?;
    }

    for clickhouse in &config.clickhouse.unwrap_or_default() {
        run_queries(clickhouse, &query_names, &opts.path)?;
    }

    Ok(())
}

fn run_queries(db: &impl Database, query_names: &HashSet<String>, path: &Path) -> Result<()> {
    let queries = db
        .list_queries()
        .iter()
        .filter(|q| query_names.contains(q.name()))
        .collect::<Vec<_>>();
    if !queries.is_empty() {
        let mut conn = db.connect()?;
        for query in queries {
            run_query(&mut conn, query, path)?;
        }
    }
    Ok(())
}

fn db_worker<DB: Database>(d: DB, path: PathBuf) -> Result<()> {
    let mut heap = BinaryHeap::new();

    for q in d.list_queries() {
        schedule_next(q.to_owned(), &mut heap)
    }

    let mut conn = d.connect()?;
    // Извлекаем самый ближайший запланированный запрос
    while let Some(ScheduledQuery(time, q)) = heap.pop() {
        sleep_until(time);
        run_query(&mut conn, &q, &path)?;
        schedule_next(q, &mut heap);
    }

    Ok(())
}

fn schedule_next<Q: Query>(q: Q, heap: &mut BinaryHeap<ScheduledQuery<Q>>) {
    if let Some(next_time) = q.schedule().upcoming(Utc).next() {
        info!("Query {} next execution is {}", q.name(), next_time);
        heap.push(ScheduledQuery(next_time, q))
    }
}

#[context("Processing query {} on database {}", query.name(), db.name())]
fn run_query<C: Connection>(db: &mut C, query: &C::Query, path: &Path) -> Result<()> {
    info!("Query run (name: {}, db: {})", db.name(), query.name());
    let path = path.join(query.name()).with_extension("idx");

    let mut ids = db.execute(query)?;
    let size = ids.len();
    ids.sort_unstable();
    let file = File::create(path)?;
    write(ids, PlainTextEncoder(file))?;
    info!(
        "Query finished (name: {}, records: {})...",
        query.name(),
        size
    );
    Ok(())
}

fn sleep_until(time: DateTime<Utc>) {
    while Utc::now() < time {
        sleep(Duration::from_secs(1));
    }
}

/// Запланированное выполнение запроса
///
/// Содержит запрос и время когда этот запрос по плану должен быть выполнен. Эту
/// структуру можно использовать совместно с `BinaryHeap` для определения какой запрос должен быть выполнен
/// в первую очередь
struct ScheduledQuery<Q>(DateTime<Utc>, Q);

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

#[context("Reading config: {}", path.display())]
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
