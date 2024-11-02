// use actix_web::{web, App, HttpServer, Responder};
// use sqlx::{PgPool};
// mod config {pub mod db;}

// async fn index() -> impl Responder {
//     "DHT Application"
// }

// #[actix_web::main]
// async fn main() -> std::io::Result<()> {
//     let pool = config::db::create_pool().await;
//     HttpServer::new(move || {
//         App::new()
//             .route("/", web::get().to(index))
//             // sharing pool across requests
//             .app_data(web::Data::new(pool.clone()))
//     })
//     .bind("localhost:5000")?
//     .run()
//     .await
// }

use actix::Actor;
mod nodes;
use nodes::{Node, JoinMessage, StabilizeMessage, FixFingersMessage, LookupMessage};

#[actix_web::main]
async fn main() {
    let node1 = Node::new(1, "127.0.0.1".to_string(), 8080).start();
    let node2 = Node::new(2, "127.0.0.1".to_string(), 8081).start();
    let node3 = Node::new(3, "127.0.0.1".to_string(), 8082).start();

    // Simulate joining and stabilizing
    node1.send(JoinMessage { node_id: 2 }).await.unwrap();
    node2.send(StabilizeMessage).await.unwrap();
    node3.send(JoinMessage { node_id: 3 }).await.unwrap();
    node3.send(StabilizeMessage).await.unwrap();

    // Perform a lookup to see if nodes route correctly
    let lookup_result = node1.send(LookupMessage { key: 3 }).await.unwrap();
    println!("Lookup result for key 3: {:?}", lookup_result);
}