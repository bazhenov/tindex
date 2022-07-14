use crate::{
    config::{self, Connection, Database, Query},
    prelude::*,
};
use ::mysql::{prelude::Queryable, Conn, Opts};
use cron::Schedule;
use fn_error_context::context;
use mysql::OptsBuilder;
use serde::Deserialize;
use std::env;

#[derive(Deserialize, PartialEq, Eq, Debug)]
pub struct MySqlDatabase {
    name: String,
    url: String,
    queries: Vec<MySqlQuery>,
}

impl Database for MySqlDatabase {
    type Connection = MySqlConnection;

    #[context("Connecting to MySQL: {}", self.name)]
    fn connect(&self) -> Result<Self::Connection> {
        let mut opts = OptsBuilder::from_opts(Opts::from_url(&self.url)?);

        let user_var_name = format!("MYSQL_{}_USER", self.name.to_uppercase());
        if let Ok(user) = env::var(user_var_name) {
            opts = opts.user(Some(user));
        }

        let pass_var_name = format!("MYSQL_{}_PASSWORD", self.name.to_uppercase());
        if let Ok(password) = env::var(pass_var_name) {
            opts = opts.pass(Some(password));
        }

        let conn = Conn::new(opts)?;
        Ok(MySqlConnection(self.name.to_owned(), conn))
    }

    fn list_queries(&self) -> &[MySqlQuery] {
        &self.queries[..]
    }
}

pub struct MySqlConnection(String, Conn);

impl Connection for MySqlConnection {
    type Query = MySqlQuery;

    fn name(&self) -> &str {
        &self.0
    }

    fn execute(&mut self, query: &MySqlQuery) -> Result<Vec<u64>> {
        Ok(self.1.exec_map(&query.sql, (), |id| id)?)
    }
}

#[derive(Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct MySqlQuery {
    name: String,
    #[serde(deserialize_with = "config::schedule_from_string")]
    schedule: Schedule,
    sql: String,
}

impl Query for MySqlQuery {
    fn name(&self) -> &str {
        &self.name
    }

    fn schedule(&self) -> &Schedule {
        &self.schedule
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn read_yaml() -> Result<()> {
        let config: MySqlDatabase = serde_yaml::from_str(
            r#"
            name: slave
            url: mysql://
            queries:
            - name: bulletin_1_week
              schedule: "0 30 9,12,15 1,15 May-Aug Mon,Wed,Fri 2018/2"
              sql: SELECT 1
            "#,
        )?;
        let expected = MySqlDatabase {
            name: "slave".to_string(),
            url: "mysql://".to_string(),
            queries: vec![MySqlQuery {
                name: "bulletin_1_week".to_string(),
                schedule: Schedule::from_str("0 30 9,12,15 1,15 May-Aug Mon,Wed,Fri 2018/2")?,
                sql: "SELECT 1".to_string(),
            }],
        };
        assert_eq!(config, expected);
        Ok(())
    }
}
