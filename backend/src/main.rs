use actix_web::{web, App, HttpServer, HttpResponse, Responder};
use sqlx::PgPool;
use actix::prelude::*;
use std::time::Duration;
use serde::Deserialize;
mod ws_handler;
use ws_handler::{BroadcastMessage, MyWebSocket, ws_route};
use std::sync::{Arc, Mutex};
use std::collections::HashSet;
use actix::Addr;
use actix_web::middleware::Logger;
use std::env;
use env_logger;
use std::collections::HashMap;

type Clients = Arc<Mutex<HashSet<Addr<MyWebSocket>>>>;

mod config { pub mod db; }
mod nodes;
use nodes::node_actor::{Node, JoinMessage, StabilizeMessage, FixFingersMessage, InsertKeyValue, GetKeyValue, DeleteKeyValue, ReplicateData, HealthCheck, TransferData, KeyValue, NodeRecord};



#[derive(Deserialize)]
struct KeyValuePayload {
    value: String,
}

// // Handler for the index route to verify the server is running
// async fn index() -> impl Responder {
//     HttpResponse::Ok().body("DHT Application is running")
// }

// // Handler to add a key-value pair to the DHT
// // async fn add_key(
// //     pool: web::Data<PgPool>,
// //     node: web::Data<Addr<Node>>,
// //     clients: web::Data<Clients>,
// //     path: web::Path<i32>,
// //     payload: web::Json<KeyValuePayload>,
// // ) -> impl Responder {
// //     let key = path.into_inner();
// //     let value = payload.into_inner().value;

// //     // Send InsertKeyValue message to the node
// //     match node.send(InsertKeyValue { key, value }).await {
// //         Ok(Ok(())) => {
// //             // Notify all connected WebSocket clients
// //             let msg = BroadcastMessage(format!("Key {} added to DHT", key));
// //             for client in clients.lock().unwrap().iter() {
// //                 client.do_send(msg.clone());
// //             }


// //             HttpResponse::Ok().json(format!("Key {} added to DHT", key))
// //         }
// //         Ok(Err(e)) => HttpResponse::InternalServerError().body(format!("Failed to add key: {:?}", e)),
// //         Err(_) => HttpResponse::InternalServerError().body("Failed to communicate with the node"),
// //     }
// // }
// async fn add_key(
//     pool: web::Data<PgPool>,
//     clients: web::Data<Arc<Mutex<HashSet<Addr<MyWebSocket>>>>>, // Correctly typed `clients`
//     path: web::Path<i32>,
//     payload: web::Json<KeyValuePayload>,
// ) -> HttpResponse {
//     let key = path.into_inner();
//     let value = payload.into_inner().value;

//     // Database insertion example
//     match sqlx::query!(
//         "INSERT INTO key_values (key, value) VALUES ($1, $2) ON CONFLICT DO NOTHING",
//         key as i32,
//         value
//     )
//     .execute(pool.get_ref())
//     .await
//     {
//         Ok(_) => {
//             // Notify all connected WebSocket clients
//             let msg = BroadcastMessage(format!("Key {} added to DHT", key));
//             for client in clients.lock().unwrap().iter() {
//                 client.do_send(msg.clone());
//             }
//             HttpResponse::Ok().json(format!("Key {} added to DHT", key))
//         },
//         Err(e) => {
//             eprintln!("Database error: {:?}", e);
//             HttpResponse::InternalServerError().body("Error adding key to DHT")
//         }
//     }
// }


// // Handler to get a value from the DHT by key
// async fn get_key(
//     pool: web::Data<PgPool>,
//     node: web::Data<Addr<Node>>,
//     path: web::Path<i32>,
// ) -> impl Responder {
//     let key = path.into_inner();

//     // Send GetKeyValue message to the node
//     match node.send(GetKeyValue { key }).await {
//         Ok(Ok(Some(value))) => HttpResponse::Ok().json(value),
//         Ok(Ok(None)) => HttpResponse::NotFound().body(format!("Key {} not found", key)),
//         Ok(Err(e)) => HttpResponse::InternalServerError().body(format!("Failed to retrieve key: {:?}", e)),
//         Err(_) => HttpResponse::InternalServerError().body("Failed to communicate with the node"),
//     }
// }

// // Handler to list all nodes in the DHT
// async fn list_nodes(pool: web::Data<PgPool>) -> impl Responder {
//     // Query the database for active nodes
//     let result = sqlx::query!("SELECT id, address, port FROM nodes")
//         .fetch_all(pool.get_ref())
//         .await;

//     match result {
//         Ok(nodes) => {
//             let response: Vec<_> = nodes.iter().map(|n| {
//                 format!("Node ID: {}, Address: {}, Port: {}", n.id, n.address, n.port)
//             }).collect();
//             HttpResponse::Ok().json(response)
//         }
//         Err(e) => HttpResponse::InternalServerError().body(format!("Failed to retrieve nodes: {:?}", e)),
//     }
// }

// #[actix_web::main]
// async fn main() -> std::io::Result<()> {
//     env::set_var("RUST_LOG", "actix_web=debug");
//     env_logger::init();
//     // Initialize the database pool
//     let pool = config::db::create_pool().await;
//     let clients: Clients = Arc::new(Mutex::new(HashSet::new()));

//     // Initialize nodes and start them as Actix actors
//     let node1 = Node::new(1, "127.0.0.1".to_string(), 5080, pool.clone()).start();
//     let node2 = Node::new(2, "127.0.0.1".to_string(), 5081, pool.clone()).start();
//     let node3 = Node::new(3, "127.0.0.1".to_string(), 5082, pool.clone()).start();

//     // Start health checks and finger table updates
//     node1.do_send(HealthCheck);
//     node2.do_send(HealthCheck);
//     node3.do_send(HealthCheck);
//     node1.do_send(FingerTableUpdate);
//     node2.do_send(FingerTableUpdate);
//     node3.do_send(FingerTableUpdate);

//     // Set up HTTP server with routes
//     HttpServer::new(move || {
//         App::new()
//             .wrap(Logger::default()) // Enable request logging
//             .app_data(web::Data::new(pool.clone()))
//             .app_data(web::Data::new(clients.clone()))
//             .app_data(web::Data::new(node1.clone())) // Provide node to handlers
//             .route("/", web::get().to(index))
//             .route("/add/{key}", web::post().to(add_key))
//             .route("/get/{key}", web::get().to(get_key))
//             .route("/nodes", web::get().to(list_nodes))
//             .route("/ws/", web::get().to(ws_route)) // WebSocket route
//     })
//     .bind("127.0.0.1:5080")?
//     .run()
//     .await
// }
type NodesMap = Arc<Mutex<HashMap<i32, Addr<Node>>>>;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let pool = config::db::create_pool().await;
    let clients: Clients = Arc::new(Mutex::new(HashSet::new()));
    env::set_var("RUST_LOG", "actix_web=debug");
    env_logger::init();

    // Create an instance of the Node actor
    let node = Node::new(1, "127.0.0.1".to_string(), 5080, pool.clone()).start();
    let node2 = Node::new(2, "127.0.0.1".to_string(), 5081, pool.clone()).start();
    let node3 = Node::new(3, "127.0.0.1".to_string(), 5082, pool.clone()).start();

    // Map of node_id to Node actor address
    let nodes_map = Arc::new(Mutex::new(HashMap::new()));;
    nodes_map.lock().unwrap().insert(1, node.clone());
    nodes_map.lock().unwrap().insert(2, node2.clone());
    nodes_map.lock().unwrap().insert(3, node3.clone());
    println!("{:?}", nodes_map.lock().unwrap());


    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(pool.clone()))
            .app_data(web::Data::new(clients.clone()))
            .app_data(web::Data::new(node.clone()))
            .app_data(web::Data::new(node2.clone()))
            .app_data(web::Data::new(node3.clone()))
            .app_data(web::Data::new(nodes_map.clone()))
            .route("/join/{node_id}", web::post().to(join_node))
            .route("/stabilize/{node_id}", web::post().to(stabilize_node))
            .route("/fix_fingers/{node_id}", web::post().to(fix_fingers))
            .route("/nodes", web::get().to(list_nodes))
            .route("/add/{key}", web::post().to(add_key))
            .route("/get/{key}", web::get().to(get_key))
            .route("/delete/{key}", web::delete().to(delete_key))
            .route("/replicate", web::post().to(replicate_data))
            .route("/health_check/{node_id}", web::post().to(health_check))
            .route("/ws/", web::get().to(ws_handler::ws_route))
    })
    .bind("127.0.0.1:5080")?
    .run()
    .await
}

async fn join_node(
    nodes_map: web::Data<Arc<Mutex<HashMap<i32, Addr<Node>>>>>,
    path: web::Path<i32>,
) -> impl Responder {
    let node_id = path.into_inner();

    // Retrieve the appropriate Node actor for the given node_id
    if let Some(node) = nodes_map.lock().unwrap().get(&node_id) {
        node.send(JoinMessage { node_id }).await.unwrap();
        HttpResponse::Ok().body(format!("Node {} joined", node_id))
    } else {
        HttpResponse::NotFound().body(format!("Node {} not found", node_id))
    }
}


async fn stabilize_node(node: web::Data<Addr<Node>>, path: web::Path<i32>) -> impl Responder {
    node.send(StabilizeMessage).await.unwrap();
    HttpResponse::Ok().body("Node stabilized")
}

async fn fix_fingers(node: web::Data<Addr<Node>>, path: web::Path<i32>) -> impl Responder {
    node.send(FixFingersMessage).await.unwrap();
    HttpResponse::Ok().body("Finger table fixed")
}

async fn list_nodes(pool: web::Data<PgPool>) -> impl Responder {
    let result = sqlx::query_as!(
        NodeRecord,
        "SELECT id, address, port, predecessor FROM nodes"
    )
    .fetch_all(pool.get_ref())
    .await;

    match result {
        Ok(nodes) => HttpResponse::Ok().json(nodes),
        Err(_) => HttpResponse::InternalServerError().body("Failed to retrieve nodes"),
    }
}


async fn add_key(
    node: web::Data<Addr<Node>>,
    path: web::Path<i32>,
    payload: web::Json<KeyValuePayload>,
) -> impl Responder {
    let key = path.into_inner();
    let value = payload.value.clone();

    // Send the InsertKeyValue message to the Node actor
    let result = node.send(InsertKeyValue { key, value }).await;
    match result {
        Ok(Ok(())) => HttpResponse::Ok().json("Key added"),
        Ok(Err(e)) => {
            eprintln!("Database error: {:?}", e);
            HttpResponse::InternalServerError().body("Failed to add key to database")
        }
        Err(_) => HttpResponse::InternalServerError().body("Failed to communicate with the node"),
    }
}


async fn get_key(node: web::Data<Addr<Node>>, path: web::Path<i32>) -> impl Responder {
    let key = path.into_inner();
    let result = node.send(GetKeyValue { key }).await;
    match result {
        Ok(Ok(Some(value))) => HttpResponse::Ok().json(value),
        Ok(Ok(None)) => HttpResponse::NotFound().body("Key not found"),
        _ => HttpResponse::InternalServerError().body("Failed to retrieve key"),
    }
}

async fn delete_key(node: web::Data<Addr<Node>>, path: web::Path<i32>) -> impl Responder {
    let key = path.into_inner();
    let result = node.send(DeleteKeyValue { key }).await;
    match result {
        Ok(Ok(())) => HttpResponse::Ok().body("Key deleted"),
        _ => HttpResponse::InternalServerError().body("Failed to delete key"),
    }
}

async fn replicate_data(node: web::Data<Addr<Node>>, payload: web::Json<ReplicateData>) -> impl Responder {
    let result = node.send(payload.into_inner()).await;
    match result {
        Ok(Ok(())) => HttpResponse::Ok().body("Data replicated"),
        _ => HttpResponse::InternalServerError().body("Replication failed"),
    }
}

async fn health_check(node: web::Data<Addr<Node>>, path: web::Path<i32>) -> impl Responder {
    node.send(HealthCheck).await.unwrap();
    HttpResponse::Ok().body("Health check complete")
}

