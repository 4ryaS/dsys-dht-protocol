use sqlx::{PgPool, Pool, Postgres};
use std::env;
use dotenv::dotenv;

pub async fn create_pool() -> PgPool {
    dotenv().ok();
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    PgPool::connect(&database_url).await.expect("Failed to connect to database")
}