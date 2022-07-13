//! Код работы с конфигурацией в YAML-формате
use std::str::FromStr;

use cron::Schedule;
use serde::{de::Error, Deserialize, Deserializer};

#[derive(Deserialize, PartialEq, Eq, Debug)]
pub struct Config {
    pub mysql: Vec<MySqlServer>,
}

#[derive(Deserialize, PartialEq, Eq, Debug)]
pub struct MySqlServer {
    pub name: String,
    pub queries: Vec<MySqlQuery>,
}

#[derive(Deserialize, PartialEq, Eq, Debug)]
pub struct MySqlQuery {
    pub name: String,
    #[serde(deserialize_with = "schedule_from_string")]
    pub schedule: Schedule,
    pub sql: String,
}

fn schedule_from_string<'de, D>(deserializer: D) -> Result<Schedule, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    Schedule::from_str(&s).map_err(D::Error::custom)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;

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
