//! Код работы с конфигурацией в YAML-формате
use crate::{
    config::{Connection, Database, Query},
    prelude::*,
};
use ::mysql::{prelude::Queryable, Conn, Opts};
use cron::Schedule;
use fn_error_context::context;
use serde::{de::Error, Deserialize, Deserializer};
use std::{env, str::FromStr};

#[derive(Deserialize, PartialEq, Eq, Debug)]
pub struct Config {
    pub mysql: Vec<MySqlServer>,
}

#[derive(Deserialize, PartialEq, Eq, Debug)]
pub struct MySqlServer {
    pub name: String,
    pub queries: Vec<MySqlQuery>,
}

impl Database for MySqlServer {
    type Connection = MySqlSource;

    fn connect(&self) -> Result<MySqlSource> {
        connect(self)
    }

    fn list_queries(&self) -> &[MySqlQuery] {
        &self.queries[..]
    }
}

pub struct MySqlSource(String, Conn);

impl Connection for MySqlSource {
    type Query = MySqlQuery;

    fn name(&self) -> &str {
        &self.0
    }
    fn execute(&mut self, query: &MySqlQuery) -> Result<Vec<u64>> {
        Ok(self.1.exec_map(&query.sql, (), |id| id)?)
    }
}

#[derive(Deserialize, PartialEq, Eq, Debug)]
pub struct MySqlQuery {
    pub name: String,
    #[serde(deserialize_with = "schedule_from_string")]
    pub schedule: Schedule,
    pub sql: String,
}

impl Query for MySqlQuery {
    fn name(&self) -> &str {
        &self.name
    }

    fn schedule(&self) -> &Schedule {
        &self.schedule
    }
}

fn schedule_from_string<'de, D>(deserializer: D) -> std::result::Result<Schedule, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    Schedule::from_str(&s).map_err(D::Error::custom)
}

#[context("Connecting to MySQL: {}", mysql.name)]
pub fn connect(mysql: &MySqlServer) -> Result<MySqlSource> {
    trace!("Connecting mysql source {}...", mysql.name);
    let var_name = format!("{}_MYSQL_URL", mysql.name.to_uppercase());
    let url = env::var(var_name)?;
    let conn = Conn::new(Opts::from_url(&url)?)?;
    Ok(MySqlSource(mysql.name.to_owned(), conn))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_yaml() -> Result<()> {
        let config: Config = serde_yaml::from_str(
            r#"
            mysql:
                - name: slave
                  queries:
                    - name: bulletin_1_week
                      schedule: "0   30   9,12,15     1,15       May-Aug  Mon,Wed,Fri  2018/2"
                      sql: SELECT 1
                - name: users
                  queries:
                    - name: user_stat
                      schedule: "0 0 * * * *"
                      sql: SELECT 2
            "#,
        )?;
        let expected = Config {
            mysql: vec![
                MySqlServer {
                    name: "slave".to_string(),
                    queries: vec![MySqlQuery {
                        name: "bulletin_1_week".to_string(),
                        schedule: Schedule::from_str(
                            "0   30   9,12,15     1,15       May-Aug  Mon,Wed,Fri  2018/2",
                        )?,
                        sql: "SELECT 1".to_string(),
                    }],
                },
                MySqlServer {
                    name: "users".to_string(),
                    queries: vec![MySqlQuery {
                        name: "user_stat".to_string(),
                        schedule: Schedule::from_str("0 0 * * * *")?,
                        sql: "SELECT 2".to_string(),
                    }],
                },
            ],
        };
        assert_eq!(config, expected);
        Ok(())
    }
}
