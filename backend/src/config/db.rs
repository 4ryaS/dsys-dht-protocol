use sqlx::{PgPool, Pool, Postgres};
use std::env;
use dotenv::dotenv;
use sqlx::postgres::PgPoolOptions;

pub async fn create_pool() -> PgPool {
    dotenv().ok();
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL is not set");
    PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("Failed to create database pool")
}