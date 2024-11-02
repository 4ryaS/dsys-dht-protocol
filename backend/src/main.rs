use actix_web::{web, App, HttpServer, Responder};
use sqlx::{PgPool};
mod config {pub mod db;}

async fn index() -> impl Responder {
    "DHT Application"
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let pool = config::db::create_pool().await;
    HttpServer::new(|| {
        App::new()
            .route("/", web::get().to(index))
            // sharing pool across requests
            .app_data(web::Data::new(pool.clone()))
    })
    .bind("localhost:5000")?
    .run()
    .await
}