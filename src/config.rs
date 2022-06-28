//! Код работы с конфигурацией в YAML-формате
use serde::Deserialize;

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
    pub sql: String,
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
                      sql: SELECT 1
                - name: users
                  queries:
                    - name: user_stat
                      sql: SELECT 2
            "#,
        )?;
        let expected = Config {
            mysql: vec![
                MySqlServer {
                    name: "slave".to_string(),
                    queries: vec![MySqlQuery {
                        name: "bulletin_1_week".to_string(),
                        sql: "SELECT 1".to_string(),
                    }],
                },
                MySqlServer {
                    name: "users".to_string(),
                    queries: vec![MySqlQuery {
                        name: "user_stat".to_string(),
                        sql: "SELECT 2".to_string(),
                    }],
                },
            ],
        };
        assert_eq!(config, expected);
        Ok(())
    }
}
