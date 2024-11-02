use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, sqlx::FromRow)]
pub struct Node {
    pub id: i32,
    pub address: String,
    pub port: i32
}