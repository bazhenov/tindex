use crate::{
    encoding::{Encoder, PlainTextEncoder},
    prelude::*,
};
use futures::TryStreamExt;
use sqlx::{MySqlPool, Row};
use std::{env, fs::File};

pub async fn main() -> Result<()> {
    let url = env::var("DB_URL")?;
    println!("Connecting...");
    let pool = MySqlPool::connect(&url).await?;

    println!("Querying...");
    let mut rows =
        sqlx::query("select id from register_user where adddate > NOW() - INTERVAL 1 DAY")
            .fetch(&pool);

    let mut encoder = PlainTextEncoder(File::create("./test.data")?);

    while let Some(row) = rows.try_next().await? {
        let id = row.try_get::<i32, _>(0)?;
        let id: u64 = u64::try_from(id)?;
        encoder.write(id)?;
    }

    Ok(())
}
