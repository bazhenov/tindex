use std::env;

use crate::config::{self, Connection};
use crate::config::{Database, Query};
use crate::prelude::*;
use clickhouse::Client;
use cron::Schedule;
use serde::Deserialize;

#[derive(Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct ClickhouseDatabase {
    name: String,
    url: String,
    queries: Vec<ClickhouseQuery>,
}

impl Database for ClickhouseDatabase {
    type Connection = ClickhouseConnection;

    fn connect(&self) -> Result<Self::Connection> {
        let mut client = Client::default().with_url(&self.url);

        let user_var_name = format!("CLICKHOUSE_{}_USER", self.name.to_uppercase());
        if let Ok(user) = env::var(user_var_name) {
            client = client.with_user(user);
        }

        let pass_var_name = format!("CLICKHOUSE_{}_PASSWORD", self.name.to_uppercase());
        if let Ok(password) = env::var(pass_var_name) {
            client = client.with_password(password);
        }

        Ok(ClickhouseConnection(self.name.clone(), client))
    }

    fn list_queries(&self) -> &[ClickhouseQuery] {
        &self.queries[..]
    }
}

pub struct ClickhouseConnection(String, Client);

impl Connection for ClickhouseConnection {
    type Query = ClickhouseQuery;

    fn name(&self) -> &str {
        &self.0
    }

    fn execute(&mut self, query: &Self::Query) -> Result<Vec<u64>> {
        let result = self.1.query(&query.sql).fetch_all::<u64>();

        Ok(futures::executor::block_on(result)?)
    }
}

#[derive(Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct ClickhouseQuery {
    pub name: String,
    #[serde(deserialize_with = "config::schedule_from_string")]
    pub schedule: Schedule,
    pub sql: String,
}

impl Query for ClickhouseQuery {
    fn name(&self) -> &str {
        &self.name
    }

    fn schedule(&self) -> &cron::Schedule {
        &self.schedule
    }
}
